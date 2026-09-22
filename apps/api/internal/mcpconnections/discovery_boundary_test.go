package mcpconnections

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// saveToken is save() with a credential chosen by the test: a short one is
// still a credential, and is still replaced everywhere it appears.
func (f *fixture) saveToken(endpoint, token string) Connection {
	f.t.Helper()
	status, b := f.api("POST", "/api/mcp-connections", owner, Input{Name: "Short credential fixture", Endpoint: endpoint, Enabled: true, BearerToken: token})
	if status != 200 {
		f.t.Fatalf("save %d %s", status, b)
	}
	var c Connection
	json.Unmarshal(b, &c)
	return c
}

// A definition that fits the page budget before the result is transformed can
// exceed the durable bound afterwards: every occurrence of a configured value
// becomes "[redacted]". The page decision has to be made on what is actually
// stored — otherwise the continuation advances past schemas that were never
// delivered, and nothing in the result says so.
func TestDiscoveryMeasuresDefinitionsAsPersisted(t *testing.T) {
	f := setup(t)
	remote := mcp.NewServer(&mcp.Implementation{Name: "growth", Version: "1"}, nil)
	call := func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "called"}}}, nil
	}
	// 24 KiB of the connection's own credential echoed back: ~24 KiB measured
	// before the traversal, ~120 KiB once each occurrence is redacted.
	remote.AddTool(&mcp.Tool{Name: "aaa_grows", Description: strings.Repeat("ab", 12<<10), InputSchema: map[string]any{"type": "object"}}, call)
	want := map[string]int{"aaa_grows": 0}
	for i := 0; i < 12; i++ {
		name := fmt.Sprintf("tool_%02d", i)
		want[name] = 0
		remote.AddTool(paddedTool(1200, name), call)
	}
	server := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return remote }, &mcp.StreamableHTTPOptions{JSONResponse: true}))
	defer server.Close()
	c := f.saveToken(server.URL, "ab")

	cursor := ""
	grew := map[string]any{}
	for page := 1; ; page++ {
		if page > 12 {
			t.Fatal("discovery did not terminate")
		}
		request := map[string]any{}
		if cursor != "" {
			request["cursor"] = cursor
		}
		result := f.listTools(c, request)
		if raw, _ := json.Marshal(result); len(raw) > resultBoundBytes {
			t.Fatalf("page %d stored %d bytes", page, len(raw))
		}
		if result["result_omitted"] == true {
			t.Fatalf("page %d was decided before the growth that omitted it: %+v", page, result)
		}
		for _, entry := range result["tools"].([]any) {
			tool, _ := entry.(map[string]any)
			name, _ := tool["name"].(string)
			want[name]++
		}
		for _, entry := range asList(result["tools_omitted"]) {
			omission, _ := entry.(map[string]any)
			name, _ := omission["name"].(string)
			want[name]++
			grew[name] = omission
		}
		cursor, _ = result["next_cursor"].(string)
		if cursor == "" {
			break
		}
	}
	for name, delivered := range want {
		if delivered != 1 {
			t.Fatalf("%s was delivered or named %d times; the continuation skipped it", name, delivered)
		}
	}
	omission, named := grew["aaa_grows"].(map[string]any)
	if !named {
		t.Fatalf("the definition that cannot be stored was not named: %v", grew)
	}
	if size, _ := omission["bytes"].(float64); int(size) <= resultBoundBytes {
		t.Fatalf("omission reports %v bytes, which is the size before it was measured as stored", omission["bytes"])
	}
}

func asList(v any) []any {
	list, _ := v.([]any)
	return list
}

// Even many secret-bearing names must be explicitly accounted for in bounded
// omission pages. They cannot be offered as redacted callable definitions,
// and the continuation must neither skip nor stall on them.
func TestDiscoverySecretBearingNamesAreOmittedAndPagingContinues(t *testing.T) {
	f := setup(t)
	remote := mcp.NewServer(&mcp.Implementation{Name: "wide-names", Version: "1"}, nil)
	call := func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "called"}}}, nil
	}
	// Names expand under redaction and exceed the display-name bound.
	want := map[string]int{}
	// Count the safe stable prefix, not a hypothetical callable alias.
	for i := 0; i < 60; i++ {
		name := fmt.Sprintf("t%02d_%s", i, strings.Repeat("ab", 100))
		want[fmt.Sprintf("t%02d", i)] = 0
		remote.AddTool(&mcp.Tool{Name: name, Description: strings.Repeat("d", 1000), InputSchema: map[string]any{"type": "object"}}, call)
	}
	server := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return remote }, &mcp.StreamableHTTPOptions{JSONResponse: true}))
	defer server.Close()
	c := f.saveToken(server.URL, "ab")

	cursor := ""
	for page := 1; ; page++ {
		if page > 12 {
			t.Fatal("discovery did not terminate")
		}
		request := map[string]any{}
		if cursor != "" {
			request["cursor"] = cursor
		}
		result := f.listTools(c, request)
		if raw, _ := json.Marshal(result); len(raw) > resultBoundBytes {
			t.Fatalf("page %d stored %d bytes", page, len(raw))
		}
		if result["result_omitted"] == true {
			t.Fatalf("page %d dropped schemas that fit: %+v", page, result)
		}
		if len(result["tools"].([]any)) != 0 {
			t.Fatal("secret-bearing names offered as callable tools")
		}
		for _, entry := range asList(result["tools_omitted"]) {
			omission := entry.(map[string]any)
			name := omission["name"].(string)
			if !strings.Contains(omission["reason"].(string), "protected") {
				t.Fatal(omission)
			}
			want[strings.SplitN(name, "_", 2)[0]]++
		}
		cursor, _ = result["next_cursor"].(string)
		if cursor == "" {
			break
		}
		if result["remaining_on_page"] == nil {
			t.Fatalf("page %d continues without saying how much is left: %+v", page, result)
		}
	}
	for name, delivered := range want {
		if delivered != 1 {
			t.Fatalf("%s was accounted for %d times; the continuation did not match the page", name, delivered)
		}
	}
}

// If anything still pushes a discovery result past the durable bound after the
// page was settled, the continuation must be withdrawn: a cursor past schemas
// that were never delivered would skip them with nothing saying so.
func TestOversizedDiscoveryResultWithdrawsItsContinuation(t *testing.T) {
	out := boundResult(map[string]any{
		"dispatched":        false,
		"protocol_version":  "2025-06-18",
		"tools":             []any{map[string]any{"name": "big", "description": strings.Repeat("d", 80<<10)}},
		"next_cursor":       discoveryCursorPrefix + "eyJvIjo5fQ",
		"remaining_on_page": float64(4),
		"next_names":        []any{"waiting"},
	})
	if out["result_omitted"] != true || out["repeat_request"] != true {
		t.Fatalf("an undeliverable page did not say so: %+v", out)
	}
	if _, present := out[cursorKey]; present {
		t.Fatalf("a continuation survived past schemas that were never delivered: %+v", out)
	}
	if _, present := out["remaining_on_page"]; present {
		t.Fatalf("page position survived a page that was not delivered: %+v", out)
	}
	// A call result keeps its own continuation-free metadata untouched.
	out = boundResult(map[string]any{
		"dispatched":  true,
		"outcome":     "returned",
		"call_result": map[string]any{"content": strings.Repeat("c", 80<<10)},
	})
	if out["result_omitted"] != true || out["repeat_request"] != nil || out["outcome"] != "returned" {
		t.Fatalf("a call result was treated as a page: %+v", out)
	}
}

// The transformation's boundary, stated where it can be checked: a result's own
// shape and its minted cursor come back untouched, while the very same
// characters in remote content are still transformed.
func TestPersistKeepsThisPackagesOwnShapeAndCursor(t *testing.T) {
	cursor := mustCursor(t, discoveryCursor{Page: 2, Offset: 17, Digest: "8e31258425a1", Prefix: "def"})
	for _, secret := range append(strings.Split("sumi.tools2eyJ_", ""), "sumi.tools", "eyJ", cursor) {
		out := persist(map[string]any{
			cursorKey:     cursor,
			"tools":       []any{map[string]any{"name": "echo", "remote_key": "echo of " + secret}},
			"call_result": map[string]any{"structuredContent": map[string]any{secret + "_key": "value"}},
			"elsewhere":   cursor,
		}, []string{secret})
		if out[cursorKey] != cursor {
			t.Fatalf("a configured value of %q rewrote the cursor: %v", secret, out[cursorKey])
		}
		for _, key := range []string{cursorKey, "tools", "call_result", "elsewhere"} {
			if _, present := out[key]; !present {
				t.Fatalf("a configured value of %q renamed the result key %q: %+v", secret, key, out)
			}
		}
		if strings.Contains(cursor, secret) && out["elsewhere"] == cursor {
			t.Fatalf("the same string as a remote value escaped redaction for %q", secret)
		}
		remote, _ := out["call_result"].(map[string]any)["structuredContent"].(map[string]any)
		if _, untouched := remote[secret+"_key"]; untouched {
			t.Fatalf("a remote key kept %q: %+v", secret, remote)
		}
	}
}
