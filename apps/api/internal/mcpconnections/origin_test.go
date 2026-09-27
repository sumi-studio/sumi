package mcpconnections

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

// Real persisted plans/operations mint the jobs; notifications use the ordinary
// terminal job path. No hand-crafted created_by or origin metadata is inserted.
func TestToolJobOriginNotifications(t *testing.T) {
	for _, scenario := range []struct {
		status   string
		finished bool
	}{
		{"done", false}, {"done", true}, {"cancelled", false}, {"lost", false},
	} {
		t.Run(fmt.Sprintf("%s_finished_%v", scenario.status, scenario.finished), func(t *testing.T) {
			f := setup(t)
			ctx := context.Background()

			remote := mcp.NewServer(&mcp.Implementation{Name: "origin", Version: "1"}, nil)
			remote.AddTool(&mcp.Tool{Name: "noop", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "ok"}}}, nil
			})
			server := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return remote }, &mcp.StreamableHTTPOptions{JSONResponse: true}))
			defer server.Close()
			connection := f.save(server.URL)
			pool := f.store.pool
			calls := []agentstate.PlanCall{
				{Tool: "mcp.call", Route: "normal", Request: map[string]any{"connection_id": connection.ID, "name": "noop", "arguments": map[string]any{}}},
				{Tool: "mcp.list_tools", Route: "normal", Request: map[string]any{"connection_id": connection.ID}},
			}

			core := f.core.Store()
			inputID := "request:tool:7:" + scenario.status
			requestText := "Check the shared request and keep its exact origin"
			lease, e := core.AcquireWriter(ctx, persona, "origin-test", time.Minute)
			if e != nil {
				t.Fatal(e)
			}
			if _, _, e = core.SubmitInput(ctx, &agentstate.Input{PersonaID: persona, InputID: inputID, Kind: "message", Payload: map[string]any{"text": requestText}}); e != nil {
				t.Fatal(e)
			}
			if _, e = core.LoadTurn(ctx, persona, lease.Generation, "origin-turn", 10); e != nil {
				t.Fatal(e)
			}
			if _, _, e = core.SavePlan(ctx, persona, "origin-turn", lease.Generation, 0, agentstate.Decision{Calls: calls}); e != nil {
				t.Fatal(e)
			}
			ids := []string{}
			for i, call := range calls {
				op, _, fresh, e := core.ClaimOperation(ctx, persona, "origin-turn", lease.Generation, fmt.Sprintf("operation-%d", i), call.Tool, i, call.Request)
				if e != nil || !fresh {
					t.Fatalf("claim: %+v %v", op, e)
				}
				job, ok := op.Response["job"].(map[string]any)
				want := fmt.Sprintf("op:%s:%d", inputID, i)
				if !ok || job["job_id"] != want {
					t.Fatalf("identity %+v want %s", op.Response, want)
				}
				ids = append(ids, want)
			}
			if scenario.finished {
				if _, e := core.CommitTurn(ctx, persona, "origin-turn", lease.Generation, agentstate.CommitRequest{Outcome: "complete"}); e != nil {
					t.Fatal(e)
				}
			}
			for _, id := range ids {
				switch scenario.status {
				case "done":
					f.tick()
				case "cancelled":
					if _, e := core.CancelJob(ctx, persona, id); e != nil {
						t.Fatal(e)
					}
				}
			}
			if scenario.status == "lost" {
				jobs, _, e := core.ClaimJobs(ctx, persona, "lost-runner", []string{"mcp"}, time.Minute, 2, "*")
				if e != nil || len(jobs) != 2 {
					t.Fatalf("lost claims: %+v %v", jobs, e)
				}
				if _, e := pool.Exec(ctx, `UPDATE core_jobs SET claim_expires_at=now()-interval '1 second' WHERE persona_id=$1`, persona); e != nil {
					t.Fatal(e)
				}
				if _, e := core.SweepExpiredJobs(ctx, []string{"mcp"}, 64); e != nil {
					t.Fatal(e)
				}
				again, _, e := core.ClaimJobs(ctx, persona, "next-runner", []string{"mcp"}, time.Minute, 2, "*")
				if e != nil || len(again) != 0 {
					t.Fatalf("uncertain job replayed %+v %v", again, e)
				}
			}
			for _, id := range ids {
				var payload map[string]any
				if e := pool.QueryRow(ctx, `SELECT payload FROM core_inputs WHERE persona_id=$1 AND input_id=$2`, persona, "job:"+id).Scan(&payload); e != nil {
					t.Fatal(e)
				}
				if payload["origin_input_id"] != inputID || payload["origin_request"] != requestText || payload["origin_in_progress"] != !scenario.finished || payload["status"] != scenario.status {
					t.Fatalf("notification: %+v", payload)
				}
				text, _ := payload["text"].(string)
				if !strings.Contains(text, requestText) || strings.Contains(text, "not finished yet") != !scenario.finished {
					t.Fatalf("model-visible notification %q", text)
				}
				t.Logf("notification: %+v", payload)
			}
			if !scenario.finished {
				for i, call := range calls {
					op, _, fresh, e := core.ClaimOperation(ctx, persona, "origin-turn", lease.Generation, fmt.Sprintf("replay-%d", i), call.Tool, i, call.Request)
					if e != nil || fresh || op.Response["job"].(map[string]any)["job_id"] != ids[i] {
						t.Fatalf("replay: %+v fresh=%v %v", op, fresh, e)
					}
				}
			}
			var count int
			if e := pool.QueryRow(ctx, `SELECT count(*) FROM core_jobs WHERE persona_id=$1`, persona).Scan(&count); e != nil || count != 2 {
				t.Fatalf("replay minted jobs: %d %v", count, e)
			}
		})
	}
}
