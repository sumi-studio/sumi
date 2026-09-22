//go:build linux

package mcpconnections

import (
	"context"
	"strings"
	"testing"
)

// Ordinary configuration must preserve identifiers, schema vocabulary, and
// protocol metadata, including when those strings contain short values.
func TestLocalStdioDiscoveryCursorSurvivesOrdinaryConfiguredValues(t *testing.T) {
	// Several ordinary values, because which characters a cursor happens to
	// contain must not be what keeps discovery working.
	for _, value := range []string{"1", "2", "yes", "object", "string"} {
		t.Run("DEBUG="+value, func(t *testing.T) { discoveryUnderConfiguredValue(t, value) })
	}
}

func discoveryUnderConfiguredValue(t *testing.T, value string) {
	s, core, cfg := localTestStore(t)
	ctx := context.Background()
	cfg.Env["MCP_BULK_TOOLS"] = "30"
	cfg.Env["DEBUG"] = value
	cfg.Env["PATH"] = "/usr/bin:/bin"
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
		if job.Result["protocol_version"] != "2025-06-18" {
			t.Fatalf("protocol version changed: %v", job.Result["protocol_version"])
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
			input := tool["inputSchema"].(map[string]any)
			property := input["properties"].(map[string]any)["value"].(map[string]any)
			if input["type"] != "object" || property["type"] != "string" {
				t.Fatalf("schema changed: %v", input)
			}
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
	if seen["bulk_01"] != 1 {
		t.Fatalf("ordinary config changed bulk_01: %v", seen)
	}
	job := localCallJob(t, s, core, c.ID, "bulk_01", map[string]any{"value": "x"})
	if e := runner.Tick(ctx); e != nil {
		t.Fatal(e)
	}
	job, _ = core.Store().GetJob(ctx, persona, job.JobID)
	if job.Status != "done" {
		t.Fatalf("delivered name not callable: %+v", job)
	}

}
