package mcpconnections

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

// paddedTool is a tool whose complete definition is about 2*size bytes, so a
// realistic collection of them exceeds what one durable job result can hold.
func paddedTool(size int, name string) *mcp.Tool {
	return &mcp.Tool{Name: name, Description: strings.Repeat("d", size), InputSchema: map[string]any{
		"type":       "object",
		"properties": map[string]any{"value": map[string]any{"type": "string", "description": strings.Repeat("s", size)}},
	}}
}

func discoveryServer(t *testing.T, f *fixture, options *mcp.ServerOptions, tools ...*mcp.Tool) (*mcp.Server, Connection) {
	t.Helper()
	remote := mcp.NewServer(&mcp.Implementation{Name: "discovery", Version: "1"}, options)
	for _, tool := range tools {
		remote.AddTool(tool, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "called"}}}, nil
		})
	}
	server := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return remote }, &mcp.StreamableHTTPOptions{JSONResponse: true}))
	t.Cleanup(server.Close)
	return remote, f.save(server.URL)
}

// listTools runs one real discovery request through the secretary and returns
// the durable result. The leading child drains the previous job's completion
// notification, as a real secretary would before deciding to page again.
func (f *fixture) listTools(c Connection, request map[string]any) map[string]any {
	f.t.Helper()
	f.child("null")
	request["connection_id"] = c.ID
	f.coreCall("mcp.list_tools", request)
	f.tick()
	job := f.job()
	if job.Status != "done" {
		f.t.Fatalf("list_tools %+v", job)
	}
	return job.Result
}

// deliveredTools checks that every definition in a page is whole, and returns
// the names in the order the page delivered them.
func deliveredTools(t *testing.T, result map[string]any, size int) []string {
	t.Helper()
	raw, ok := result["tools"].([]any)
	if !ok {
		t.Fatalf("no tools in %v", result)
	}
	names := []string{}
	for _, entry := range raw {
		tool, _ := entry.(map[string]any)
		name, _ := tool["name"].(string)
		description, _ := tool["description"].(string)
		schema, _ := tool["inputSchema"].(map[string]any)
		properties, _ := schema["properties"].(map[string]any)
		value, _ := properties["value"].(map[string]any)
		inner, _ := value["description"].(string)
		if len(description) != size || len(inner) != size {
			t.Fatalf("tool %q was shortened: description %d, schema %d, want %d", name, len(description), len(inner), size)
		}
		names = append(names, name)
	}
	return names
}

// A server that pages by its own judgement, or not at all, must still be fully
// discoverable: pages carry whole definitions, and advancing the cursor reaches
// every tool exactly once.
func TestDiscoveryDeliversEveryToolCompleteAcrossPages(t *testing.T) {
	for _, upstream := range []struct {
		label   string
		options *mcp.ServerOptions
	}{
		{"server_does_not_paginate", nil},
		{"server_paginates_by_seven", &mcp.ServerOptions{PageSize: 7}},
	} {
		t.Run(upstream.label, func(t *testing.T) {
			f := setup(t)
			want := map[string]int{}
			tools := []*mcp.Tool{}
			for i := 0; i < 30; i++ {
				name := fmt.Sprintf("tool_%02d", i)
				want[name] = 0
				tools = append(tools, paddedTool(1200, name))
			}
			_, c := discoveryServer(t, f, upstream.options, tools...)
			cursor := ""
			pages := 0
			for {
				pages++
				if pages > 12 {
					t.Fatal("discovery did not terminate")
				}
				request := map[string]any{}
				if cursor != "" {
					request["cursor"] = cursor
				}
				result := f.listTools(c, request)
				if result["tools_omitted"] != nil {
					t.Fatalf("nothing in this collection is indivisible: %v", result["tools_omitted"])
				}
				for _, name := range deliveredTools(t, result, 1200) {
					if _, expected := want[name]; !expected {
						t.Fatalf("unknown tool %q", name)
					}
					want[name]++
				}
				cursor, _ = result["next_cursor"].(string)
				if cursor == "" {
					break
				}
				if waiting, ok := result["next_names"].([]any); ok && len(waiting) == 0 {
					t.Fatal("empty next_names alongside a continuation cursor")
				}
			}
			if pages < 2 {
				t.Fatalf("30 bulky tools reported as one page (%d)", pages)
			}
			for name, delivered := range want {
				if delivered != 1 {
					t.Fatalf("%s delivered %d times across %d pages", name, delivered, pages)
				}
			}
			// A tool the secretary only learned about on a later page is callable.
			f.child("null")
			f.coreCall("mcp.call", map[string]any{"connection_id": c.ID, "name": "tool_29", "arguments": map[string]any{"value": "x"}})
			f.tick()
			if job := f.job(); job.Status != "done" {
				t.Fatalf("tool from a later page not callable %+v", job)
			}
		})
	}
}

// One tool can be larger than any page. Naming it (and its size) keeps the rest
// of the collection reachable without inviting a guessed call.
func TestDiscoveryNamesIndivisibleToolsAndKeepsPagingPastThem(t *testing.T) {
	f := setup(t)
	_, c := discoveryServer(t, f, nil, paddedTool(100, "small_a"), paddedTool(30<<10, "giant"), paddedTool(100, "small_b"))
	result := f.listTools(c, map[string]any{})
	omitted, _ := result["tools_omitted"].([]any)
	if len(omitted) != 1 {
		t.Fatalf("indivisible tool not reported %v", result)
	}
	entry, _ := omitted[0].(map[string]any)
	size, _ := entry["bytes"].(float64)
	if entry["name"] != "giant" || int(size) <= toolPageBudget || !strings.Contains(fmt.Sprint(entry["reason"]), "will not shorten") {
		t.Fatalf("omission is not self-explaining %v", entry)
	}
	if names := deliveredTools(t, result, 100); len(names) != 2 || names[0] != "small_a" || names[1] != "small_b" {
		t.Fatalf("cursor stalled on the indivisible tool: %v", names)
	}
	if result["next_cursor"] != "" {
		t.Fatalf("collection is finished but a cursor remains %v", result["next_cursor"])
	}
	// Asking for the giant by name says the same thing rather than half a schema.
	result = f.listTools(c, map[string]any{"names": []any{"giant"}})
	if len(deliveredTools(t, result, 30<<10)) != 0 || result["tools_omitted"] == nil {
		t.Fatalf("indivisible tool returned by name %v", result)
	}
}

// Once a name is known, the secretary can fetch exactly that definition instead
// of paging to reach it; absence is reported as absence.
func TestDiscoveryByNameReturnsExactlyTheRequestedDefinitions(t *testing.T) {
	f := setup(t)
	tools := []*mcp.Tool{}
	for i := 0; i < 20; i++ {
		tools = append(tools, paddedTool(1200, fmt.Sprintf("tool_%02d", i)))
	}
	_, c := discoveryServer(t, f, &mcp.ServerOptions{PageSize: 3}, tools...)
	result := f.listTools(c, map[string]any{"names": []any{"tool_19", "tool_00", "absent_tool"}})
	names := deliveredTools(t, result, 1200)
	if len(names) != 2 || names[0] != "tool_19" || names[1] != "tool_00" {
		t.Fatalf("requested order not preserved: %v", names)
	}
	missing, _ := result["names_not_found"].([]any)
	if len(missing) != 1 || missing[0] != "absent_tool" {
		t.Fatalf("absent name not reported %v", result)
	}
	if result["next_cursor"] != "" {
		t.Fatalf("by-name discovery is not a page %v", result["next_cursor"])
	}
}

// A changed upstream page must not make a tool disappear between requests.
func TestDiscoveryRestartsAPageThatChangedUnderTheCursor(t *testing.T) {
	f := setup(t)
	want := map[string]bool{}
	tools := []*mcp.Tool{}
	for i := 0; i < 24; i++ {
		name := fmt.Sprintf("tool_%02d", i)
		want[name] = false
		tools = append(tools, paddedTool(1200, name))
	}
	remote, c := discoveryServer(t, f, nil, tools...)
	first := f.listTools(c, map[string]any{})
	for _, name := range deliveredTools(t, first, 1200) {
		want[name] = true
	}
	cursor, _ := first["next_cursor"].(string)
	if cursor == "" {
		t.Fatalf("expected a continuation cursor %v", first)
	}
	// The collection changes while the secretary holds a cursor into it.
	want["aaa_added_between_pages"] = false
	remote.AddTool(paddedTool(1200, "aaa_added_between_pages"), func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "called"}}}, nil
	})
	second := f.listTools(c, map[string]any{"cursor": cursor})
	if second["page_changed"] != true {
		t.Fatalf("changed page not reported %v", second)
	}
	names := deliveredTools(t, second, 1200)
	if len(names) == 0 || names[0] != "aaa_added_between_pages" {
		t.Fatalf("resumed mid-page after the page changed: %v", names)
	}
	for _, name := range names {
		want[name] = true
	}
	cursor, _ = second["next_cursor"].(string)
	for pages := 0; cursor != ""; pages++ {
		if pages > 12 {
			t.Fatal("discovery did not terminate after the change")
		}
		result := f.listTools(c, map[string]any{"cursor": cursor})
		for _, name := range deliveredTools(t, result, 1200) {
			want[name] = true
		}
		cursor, _ = result["next_cursor"].(string)
	}
	for name, delivered := range want {
		if !delivered {
			t.Fatalf("%s was never delivered", name)
		}
	}
}

// Discovery request shape is decided before a job is parked, deterministically
// and without reading any remote state.
func TestDiscoveryRequestShapeIsRejectedBeforeParking(t *testing.T) {
	f := setup(t)
	_, c := discoveryServer(t, f, nil, paddedTool(100, "only"))
	effect := f.store.Effects()["mcp.list_tools"]
	corrupt := discoveryCursorPrefix + base64.RawURLEncoding.EncodeToString([]byte(`{"o":-3}`))
	for label, request := range map[string]map[string]any{
		"unreadable cursor":      {"connection_id": c.ID, "cursor": discoveryCursorPrefix + "!!not-base64"},
		"negative offset":        {"connection_id": c.ID, "cursor": corrupt},
		"empty names":            {"connection_id": c.ID, "names": []any{}},
		"repeated name":          {"connection_id": c.ID, "names": []any{"a", "a"}},
		"names and cursor":       {"connection_id": c.ID, "names": []any{"a"}, "cursor": mustCursor(t, discoveryCursor{Page: 1})},
		"name of the wrong type": {"connection_id": c.ID, "names": []any{7}},
		"overlong name":          {"connection_id": c.ID, "names": []any{strings.Repeat("n", 257)}},
		// A cursor is this package's own. A server's cursor, or anything else
		// pasted in, addresses nothing here and is refused rather than sent on.
		"a server's own cursor": {"connection_id": c.ID, "cursor": "opaque-server-cursor"},
		"page past the scan bound": {"connection_id": c.ID,
			"cursor": discoveryCursorPrefix + base64.RawURLEncoding.EncodeToString([]byte(`{"p":32}`))},
	} {
		if e := effect.Validate(request); !errors.Is(e, agentstate.ErrBadRequest) {
			t.Fatalf("%s admitted: %v", label, e)
		}
	}
	for label, request := range map[string]map[string]any{
		"no cursor":       {"connection_id": c.ID},
		"selected names":  {"connection_id": c.ID, "names": []any{"only"}},
		"our own cursor":  {"connection_id": c.ID, "cursor": mustCursor(t, discoveryCursor{Page: 1, Offset: 3, Digest: "abc"})},
		"absent selector": {"connection_id": c.ID, "names": nil},
	} {
		if e := effect.Validate(request); e != nil {
			t.Fatalf("%s refused: %v", label, e)
		}
	}
	// The same refusal holds where the job would be written.
	ctx := context.Background()
	tx, e := f.store.pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	if _, e := effect.Apply(ctx, tx, persona, "discovery-shape:"+uuid.NewString()+":tool:0", map[string]any{"connection_id": c.ID, "cursor": corrupt}); !errors.Is(e, agentstate.ErrBadRequest) {
		t.Fatalf("corrupt cursor parked a job: %v", e)
	}
}

func mustCursor(t *testing.T, c discoveryCursor) string {
	t.Helper()
	out, e := encodeDiscoveryCursor(c)
	if e != nil {
		t.Fatal(e)
	}
	return out
}

func TestDiscoveryCursorRoundTripAndBounds(t *testing.T) {
	for _, c := range []discoveryCursor{{}, {Page: 5}, {Offset: 9}, {Page: 31, Offset: 9, Digest: "0123456789ab"}} {
		text := mustCursor(t, c)
		back, e := decodeDiscoveryCursor(text)
		if e != nil || back != c {
			t.Fatalf("%v round-tripped to %v (%v)", c, back, e)
		}
		if len(text) > 2048 {
			t.Fatalf("cursor exceeds admission bound: %d", len(text))
		}
	}
	// A cursor holds no remote bytes at all: nothing a server says can ride
	// inside it past redaction, which is what lets the traversal skip it.
	text := mustCursor(t, discoveryCursor{Page: 3, Offset: 7, Digest: "0123456789ab"})
	raw, e := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(text, discoveryCursorPrefix))
	if e != nil {
		t.Fatal(e)
	}
	var fields map[string]any
	if e := json.Unmarshal(raw, &fields); e != nil {
		t.Fatal(e)
	}
	for key := range fields {
		if key != "p" && key != "o" && key != "d" {
			t.Fatalf("cursor carries %q, which this package does not mint", key)
		}
	}
	// A page number outside the walk, and anything not minted here, address
	// nothing.
	if _, e := encodeDiscoveryCursor(discoveryCursor{Page: discoveryScanPages}); e == nil {
		t.Fatal("a page past the scan bound was minted")
	}
	if _, e := decodeDiscoveryCursor("opaque-server-cursor"); e == nil {
		t.Fatal("a cursor from somewhere else was accepted")
	}
}

// toolOfExactSize builds a tool whose marshalled definition is exactly target
// bytes, so the page-budget boundary itself can be tested rather than assumed.
func toolOfExactSize(t *testing.T, target int) *mcp.Tool {
	t.Helper()
	tool := &mcp.Tool{Name: "exact", Description: "d", InputSchema: map[string]any{"type": "object"}}
	raw, e := json.Marshal(tool)
	if e != nil || target <= len(raw) {
		t.Fatalf("cannot build a %d byte tool (%v)", target, e)
	}
	tool.Description = strings.Repeat("d", target-len(raw)+1)
	if raw, _ = json.Marshal(tool); len(raw) != target {
		t.Fatalf("built %d bytes, wanted %d", len(raw), target)
	}
	return tool
}

// Every page must consume at least its first tool. One that neither fits nor
// counts as indivisible would hand back the cursor it arrived with, stalling
// the whole collection at that tool.
func TestPackToolsAlwaysConsumesTheFirstTool(t *testing.T) {
	for _, size := range []int{toolPageBudget - 1, toolPageBudget, toolPageBudget + 1} {
		plan := packTools([]*mcp.Tool{toolOfExactSize(t, size), paddedTool(10, "next")}, nil)
		if plan.consumed == 0 || len(plan.packed)+len(plan.omitted) == 0 {
			t.Fatalf("a %d byte tool stalled the page: packed %d, omitted %d, consumed %d", size, len(plan.packed), len(plan.omitted), plan.consumed)
		}
	}
	// The same boundary, measured as stored: a definition that only exceeds the
	// budget once a configured value is redacted is indivisible too.
	fits := toolOfExactSize(t, toolPageBudget-1)
	if plan := packTools([]*mcp.Tool{fits}, nil); len(plan.packed) != 1 {
		t.Fatalf("a tool inside the budget was not packed: %+v", plan)
	}
	if plan := packTools([]*mcp.Tool{fits}, []string{"d"}); len(plan.packed) != 0 || len(plan.omitted) != 1 || plan.consumed != 1 {
		t.Fatalf("growth under redaction was not measured: %+v", plan)
	}
}

// The by-name scan is bounded exactly as the call path's lookup is, and says
// so: "not found" must not claim more than the scan actually looked at.
func TestDiscoveryByNameReportsItsOwnScanBound(t *testing.T) {
	f := setup(t)
	tools := []*mcp.Tool{}
	for i := 0; i < discoveryScanPages+8; i++ {
		tools = append(tools, paddedTool(10, fmt.Sprintf("tool_%02d", i)))
	}
	_, c := discoveryServer(t, f, &mcp.ServerOptions{PageSize: 1}, tools...)
	last := fmt.Sprintf("tool_%02d", discoveryScanPages+7)
	result := f.listTools(c, map[string]any{"names": []any{last}})
	missing, _ := result["names_not_found"].([]any)
	if len(missing) != 1 || missing[0] != last || result["scan_truncated"] != true {
		t.Fatalf("a bounded scan reported as an exhaustive one: %v", result)
	}
	// Within the bound the same request finds its tool and says nothing about
	// truncation.
	result = f.listTools(c, map[string]any{"names": []any{"tool_05"}})
	if names := deliveredTools(t, result, 10); len(names) != 1 || names[0] != "tool_05" || result["scan_truncated"] != nil {
		t.Fatalf("in-bound lookup: %v", result)
	}
}
