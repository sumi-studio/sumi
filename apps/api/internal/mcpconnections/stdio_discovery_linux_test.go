//go:build linux

package mcpconnections

import (
	"context"
	"strings"
	"testing"
)

// An ordinary Local configuration carries short values. `DEBUG=1` is not a
// credential, but every configured value is a redaction target, so the
// traversal that removes credentials also rewrites any string containing "1".
// A continuation cursor must survive that: if the cursor this package minted
// does not come back intact, every page after the first is unreachable.
//
// Note what this deliberately does not assert. Under DEBUG=1 the remote tool
// *names* come back rewritten too ("bulk_01" is delivered as
// "bulk_0[redacted]"), which is the product's existing redaction policy, not
// something discovery decides — see evidence/short-configured-value-corrupts-
// remote-names.log and the limits section of HANDBACK.md. This test therefore
// counts complete definitions and their integrity, which is what paging owns.
func TestLocalStdioDiscoveryCursorSurvivesOrdinaryConfiguredValues(t *testing.T) {
	// Several ordinary values, because which characters a cursor happens to
	// contain must not be what keeps discovery working.
	for _, value := range []string{"1", "2", "yes"} {
		t.Run("DEBUG="+value, func(t *testing.T) { discoveryUnderConfiguredValue(t, value) })
	}
}

func discoveryUnderConfiguredValue(t *testing.T, value string) {
	s, core, cfg := localTestStore(t)
	ctx := context.Background()
	cfg.Env["MCP_BULK_TOOLS"] = "30"
	cfg.Env["DEBUG"] = value
	c, e := s.SaveLocal(ctx, "", cfg)
	if e != nil {
		t.Fatal(e)
	}
	runner := NewRunner(s, core.Store())
	seen := map[string]int{}
	bulk := 0
	request := map[string]any{}
	for page := 1; ; page++ {
		if page > 8 {
			t.Fatal("discovery did not terminate")
		}
		job := localJob(t, s, core, c.ID, "list_tools", request)
		if e := runner.Tick(ctx); e != nil {
			t.Fatal(e)
		}
		job, e = core.Store().GetJob(ctx, persona, job.JobID)
		if e != nil {
			t.Fatal(e)
		}
		if job.Status != "done" || job.Result["result_omitted"] == true {
			t.Fatalf("page %d against a real Local server: %s %+v", page, job.Status, job.Result)
		}
		tools, ok := job.Result["tools"].([]any)
		if !ok {
			t.Fatalf("page %d delivered no tools: %+v", page, job.Result)
		}
		for _, entry := range tools {
			tool, _ := entry.(map[string]any)
			name, _ := tool["name"].(string)
			seen[name]++
			if !strings.HasPrefix(name, "bulk_") {
				continue
			}
			bulk++
			if description, _ := tool["description"].(string); len(description) != 1200 {
				t.Fatalf("%s was shortened to %d bytes", name, len(description))
			}
		}
		cursor, _ := job.Result["next_cursor"].(string)
		if cursor == "" {
			break
		}
		if !strings.HasPrefix(cursor, discoveryCursorPrefix) {
			t.Fatalf("page %d emitted a cursor this package cannot read back: %q", page, cursor)
		}
		if page == 1 && len(seen) == 0 {
			t.Fatal("a continuation cursor with nothing delivered")
		}
		request = map[string]any{"cursor": cursor}
	}
	if bulk != 30 || seen["remember_label"] != 1 {
		t.Fatalf("delivered %d of 30 bulk definitions, remember_label %d times", bulk, seen["remember_label"])
	}
	for name, count := range seen {
		if count != 1 {
			t.Fatalf("%s delivered %d times", name, count)
		}
	}
}
