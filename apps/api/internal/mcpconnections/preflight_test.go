package mcpconnections

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

func TestMCPInvalidElevatedCallDoesNotAskForApproval(t *testing.T) {
	id := "0198f0f4-9b72-7000-8000-000000000808"
	cases := []struct {
		name, tool string
		request    map[string]any
		valid      bool
	}{
		{"missing connection", "mcp.call", map[string]any{"name": "echo", "arguments": map[string]any{}}, false},
		{"invalid connection", "mcp.list_tools", map[string]any{"connection_id": "not-a-uuid"}, false},
		{"missing name", "mcp.call", map[string]any{"connection_id": id, "arguments": map[string]any{}}, false},
		{"nonobject arguments", "mcp.call", map[string]any{"connection_id": id, "name": "echo", "arguments": "oops"}, false},
		{"oversize arguments", "mcp.call", map[string]any{"connection_id": id, "name": "echo", "arguments": map[string]any{"body": strings.Repeat("x", 33<<10)}}, false},
		{"oversize cursor", "mcp.list_tools", map[string]any{"connection_id": id, "cursor": strings.Repeat("x", 2049)}, false},
		{"valid explicit elevated", "mcp.call", map[string]any{"connection_id": id, "name": "echo", "arguments": map[string]any{}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := setup(t)
			ctx := context.Background()
			core := f.core.Store()
			lease, err := core.AcquireWriter(ctx, persona, "preflight-test", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = core.SubmitInput(ctx, &agentstate.Input{PersonaID: persona, InputID: "preflight-input", Kind: "message", Payload: map[string]any{"text": "Please use the connected tool"}, ActorKind: "human", ActorID: owner}); err != nil {
				t.Fatal(err)
			}
			if _, err = core.LoadTurn(ctx, persona, lease.Generation, "preflight-turn", 20); err != nil {
				t.Fatal(err)
			}
			call := agentstate.PlanCall{Tool: tc.tool, Route: "elevated", Request: tc.request}
			if _, _, err = core.SavePlan(ctx, persona, "preflight-turn", lease.Generation, 0, agentstate.Decision{Calls: []agentstate.PlanCall{call}}); err != nil {
				t.Fatal(err)
			}
			op, approval, _, err := core.ClaimOperation(ctx, persona, "preflight-turn", lease.Generation, "preflight-op", tc.tool, 0, tc.request)
			if err != nil {
				t.Fatal(err)
			}
			if tc.valid {
				if op.Status != "awaiting_approval" || approval == nil {
					t.Fatalf("valid explicit elevated call did not request approval: %+v %+v", op, approval)
				}
			} else {
				if op.Status != "failed" || approval != nil {
					t.Fatalf("malformed call asked a person to approve it: status=%s approval=%t", op.Status, approval != nil)
				}
				var count int
				if err := f.store.pool.QueryRow(ctx, `SELECT count(*) FROM core_tool_approvals WHERE persona_id=$1`, persona).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 0 {
					t.Fatalf("malformed call created %d approval rows", count)
				}
				again, approval, _, err := core.ClaimOperation(ctx, persona, "preflight-turn", lease.Generation, "preflight-retry", tc.tool, 0, tc.request)
				if err != nil || again.Status != "failed" || approval != nil {
					t.Fatalf("invalid-call replay changed result: %+v %+v %v", again, approval, err)
				}
			}
		})
	}
}
