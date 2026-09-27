//go:build linux

package mcpconnections

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestLocalPrivateSelectorsRejectMistakes(t *testing.T) {
	s, _, original := localTestStore(t)
	for name, change := range map[string]func(*LocalInput){
		"unknown env":   func(c *LocalInput) { c.PrivateEnv = []string{"TYPO"} },
		"duplicate env": func(c *LocalInput) { c.PrivateEnv = []string{"SERVER_SECRET", "SERVER_SECRET"} },
		"negative arg":  func(c *LocalInput) { c.PrivateArgs = []int{-1} },
		"past args":     func(c *LocalInput) { c.PrivateArgs = []int{len(c.Args)} },
		"duplicate arg": func(c *LocalInput) { c.PrivateArgs = []int{0, 0} },
		"https selectors": func(c *LocalInput) {
			*c = LocalInput{Name: "remote", Transport: "https", Endpoint: "https://example.com/mcp", PrivateEnv: []string{"TOKEN"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := original
			change(&cfg)
			if _, e := s.SaveLocal(context.Background(), "", cfg); !errors.Is(e, ErrInvalid) {
				t.Fatalf("invalid selector saved: %v", e)
			}
		})
	}
}

func TestLocalExplicitPrivateValuesScrubResultsErrorsAndNotifications(t *testing.T) {
	s, core, cfg := localTestStore(t)
	cfg.Args = append(cfg.Args, "-test.timeout=45s")
	cfg.PrivateArgs = []int{1}
	cfg.Env["MCP_ECHO_ARGS"] = "yes"
	cfg.Env["DEBUG"] = "1"
	cfg.Env["SERVER_SECRET"] = "private-" + strings.Repeat("x", 600)
	c, e := s.SaveLocal(context.Background(), "", cfg)
	if e != nil {
		t.Fatal(e)
	}
	runner := NewRunner(s, core.Store())
	for _, label := range []string{"echo", "tool-failed", "protocol-error"} {
		job := localJob(t, s, core, c.ID, "call", map[string]any{"label": label})
		if e := runner.Tick(context.Background()); e != nil {
			t.Fatal(e)
		}
		job, _ = core.Store().GetJob(context.Background(), persona, job.JobID)
		raw, _ := json.Marshal(job)
		for _, secret := range []string{cfg.Env["SERVER_SECRET"], cfg.Args[1], strings.Repeat("x", 64)} {
			if strings.Contains(string(raw), secret) {
				t.Fatalf("private value leaked into job: %s", raw)
			}
		}
		if !strings.Contains(string(raw), "[redacted]") {
			t.Fatalf("echo absent: %s", raw)
		}
		if label == "echo" {
			if job.Status != "done" || !strings.Contains(string(raw), cfg.Args[0]) || !strings.Contains(string(raw), "DEBUG=1") || job.Result["notifications"] == nil {
				t.Fatalf("ordinary values or progress lost: %s", raw)
			}
		} else if label == "protocol-error" {
			if job.Status != "failed" || job.Result["error_detail"] == nil || job.Result["outcome"] != "indeterminate" {
				t.Fatalf("protocol error lost: %s", raw)
			}
		} else if job.Status != "failed" || job.Result["outcome"] != "returned" {
			t.Fatalf("tool error lost: %s", raw)
		}
	}
}

func TestLocalSecretBearingDefinitionUnavailableWithoutLeakingName(t *testing.T) {
	s, core, cfg := localTestStore(t)
	cfg.Env["MCP_PROTECTED_TOOL"] = "yes"
	c, e := s.SaveLocal(context.Background(), "", cfg)
	if e != nil {
		t.Fatal(e)
	}
	runner := NewRunner(s, core.Store())
	name := "private_" + cfg.Env["SERVER_SECRET"]
	for _, request := range []map[string]any{{}, {"names": []any{name}}} {
		job := localJob(t, s, core, c.ID, "list_tools", request)
		if e := runner.Tick(context.Background()); e != nil {
			t.Fatal(e)
		}
		job, _ = core.Store().GetJob(context.Background(), persona, job.JobID)
		raw, _ := json.Marshal(job.Result)
		if strings.Contains(string(raw), cfg.Env["SERVER_SECRET"]) || !strings.Contains(string(raw), "protected configuration value") {
			t.Fatalf("secret-bearing definition not explicitly withheld: %s", raw)
		}
		for _, tool := range asList(job.Result["tools"]) {
			if strings.HasPrefix(tool.(map[string]any)["name"].(string), "private_") {
				t.Fatal("redacted name presented as callable")
			}
		}
	}
	// Even a caller that knew the raw name cannot dispatch this unavailable definition.
	job := localCallJob(t, s, core, c.ID, name, map[string]any{})
	if e := runner.Tick(context.Background()); e != nil {
		t.Fatal(e)
	}
	job, _ = core.Store().GetJob(context.Background(), persona, job.JobID)
	if job.Status != "failed" || job.Result["dispatched"] != false || job.Error == nil || !strings.Contains(*job.Error, "protected") {
		t.Fatalf("unsafe definition dispatched: %+v", job)
	}
	if strings.Contains(*job.Error, cfg.Env["SERVER_SECRET"]) {
		t.Fatal("private name in refusal")
	}
}

func TestProtectedSchemaKeysAndEscapedValuesAreUnavailable(t *testing.T) {
	for _, value := range []any{
		map[string]any{"type": "object", "properties": map[string]any{"private-token": map[string]any{"type": "string"}}},
		map[string]any{"type": "object", "description": "secret\"with\\escapes"},
	} {
		tool := &mcp.Tool{Name: "safe_name", InputSchema: value}
		plan := packTools([]*mcp.Tool{tool}, []string{"private-token", "secret\"with\\escapes"})
		if len(plan.packed) != 0 || len(plan.omitted) != 1 {
			t.Fatalf("private schema offered: %+v", plan)
		}
	}
	if got := safeToolName(strings.Repeat("a", 250)+"private-token", []string{"private-token"}); strings.Contains(got, "private") {
		t.Fatalf("truncated credential fragment: %s", got)
	}
}
