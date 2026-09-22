//go:build linux

package mcpconnections

// Regression cases adapted from the independent PageSize:4 backward-shift
// and stdio name-callability probes.

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

// localCallJob is localJob with an explicit tool name, so a probe can call
// the name the secretary actually saw (post-redaction) instead of the raw
// upstream name.
func localCallJob(t *testing.T, s *Store, core *agentstate.Server, id, toolName string, args map[string]any) agentstate.Job {
	t.Helper()
	tx, e := s.pool.Begin(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(context.Background())
	req := map[string]any{"connection_id": id, "name": toolName, "arguments": args}
	out, e := s.Effects()["mcp.call"].Apply(context.Background(), tx, persona, "probe:"+uuid.NewString()+":tool:0", req)
	if e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(context.Background()); e != nil {
		t.Fatal(e)
	}
	j, e := core.Store().GetJob(context.Background(), persona, out["job"].(map[string]any)["job_id"].(string))
	if e != nil {
		t.Fatal(e)
	}
	return j
}

// A cursor minted against one connection is accepted syntactically on
// another: it carries no connection binding. The probe checks the behaviour
// is honest (B's own tools, page_changed signalled) rather than a silent
// wrong-page delivery or a cross-connection leak.
func TestDiscoveryForeignCursorRestartsOnRequestedConnection(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	toolsA := []*mcp.Tool{}
	for i := 0; i < 20; i++ {
		toolsA = append(toolsA, paddedTool(1200, fmt.Sprintf("alpha_%02d", i)))
	}
	toolsB := []*mcp.Tool{}
	for i := 0; i < 20; i++ {
		toolsB = append(toolsB, paddedTool(1200, fmt.Sprintf("beta_%02d", i)))
	}
	_, connA := discoveryServer(t, f, nil, toolsA...)
	_, connB := discoveryServer(t, f, nil, toolsB...)

	first := f.listTools(connA, map[string]any{})
	cursor, _ := first["next_cursor"].(string)
	if cursor == "" {
		t.Fatalf("no continuation from A's first page: %v", first)
	}
	second := f.listTools(connB, map[string]any{"cursor": cursor})
	if second["page_changed"] != true {
		t.Fatalf("foreign cursor not flagged: %v", second)
	}
	for _, entry := range second["tools"].([]any) {
		name, _ := entry.(map[string]any)["name"].(string)
		if !strings.HasPrefix(name, "beta_") {
			t.Fatalf("foreign cursor delivered another connection's tool: %q", name)
		}
	}
	// Delivery restarts at B's page 0, not mid-list.
	names := []string{}
	for _, entry := range second["tools"].([]any) {
		names = append(names, entry.(map[string]any)["name"].(string))
	}
	if len(names) == 0 || names[0] != "beta_00" {
		t.Fatalf("foreign cursor did not restart at the page start: %v", names)
	}
	_ = ctx
}

// Removing an earlier tool moves an unseen tool backward across a consumed
// upstream boundary. The continuation must signal change and make it reachable.
func TestDiscoveryBackwardShiftRestartsFromFirstPage(t *testing.T) {
	f := setup(t)
	tools := []*mcp.Tool{}
	for i := 0; i < 12; i++ {
		tools = append(tools, paddedTool(200, fmt.Sprintf("tool_%02d", i)))
	}
	remote, c := discoveryServer(t, f, &mcp.ServerOptions{PageSize: 4}, tools...)
	first := f.listTools(c, map[string]any{})
	delivered := map[string]bool{}
	omittedSeen := false
	for _, name := range deliveredNames(first) {
		delivered[name] = true
	}
	cursor, _ := first["next_cursor"].(string)
	if cursor == "" {
		t.Fatalf("expected a continuation: %v", first)
	}
	// Remove a tool from page 0, so page 1's first tool slides back into
	// page 0 and every later page's contents shift by one.
	remote.RemoveTools("tool_00")
	sawPageChanged := false
	for pages := 0; cursor != ""; pages++ {
		if pages > 12 {
			t.Fatal("discovery did not terminate")
		}
		result := f.listTools(c, map[string]any{"cursor": cursor})
		if result["page_changed"] == true {
			sawPageChanged = true
		}
		if result["tools_omitted"] != nil {
			omittedSeen = true
		}
		for _, name := range deliveredNames(result) {
			delivered[name] = true
		}
		cursor, _ = result["next_cursor"].(string)
	}
	if !sawPageChanged {
		t.Fatal("the mutation was never signalled")
	}
	if !delivered["tool_04"] || omittedSeen {
		t.Fatalf("backward-shifted tool must remain reachable: tool_04=%v omissions=%v", delivered["tool_04"], omittedSeen)
	}
	for i := 5; i < 12; i++ {
		if !delivered[fmt.Sprintf("tool_%02d", i)] {
			t.Fatalf("tool_%02d lost", i)
		}
	}
}

func deliveredNames(result map[string]any) []string {
	names := []string{}
	for _, entry := range result["tools"].([]any) {
		names = append(names, entry.(map[string]any)["name"].(string))
	}
	return names
}

// Compile-time guard so this file fails loudly if the fixture helpers move.
var _ = httptest.NewServer
