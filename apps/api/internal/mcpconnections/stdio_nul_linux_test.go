//go:build linux

package mcpconnections

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// A Local server can be configured with no arguments and no environment, so
// there is nothing to redact — and its answer may still contain a NUL that
// jsonb cannot store. Refusing to record a call that really ran, and reporting
// it as an indeterminate loss, is the worst available outcome for a mutation.
func TestLocalStdioNULResultWithoutConfiguredSecrets(t *testing.T) {
	s, core, cfg := localTestStore(t)
	ctx := context.Background()
	// A shim carries what the connection itself does not: the stored grant has
	// only an absolute command, an empty argument list and an empty environment.
	shim := filepath.Join(t.TempDir(), "local-server.sh")
	script := "#!/bin/sh\nSUMI_MCP_STDIO_HELPER=yes exec " + strconv.Quote(os.Args[0]) + " -test.run='^TestLocalStdioServerHelper$'\n"
	if e := os.WriteFile(shim, []byte(script), 0o700); e != nil {
		t.Fatal(e)
	}
	cfg.Command, cfg.Args, cfg.Env = shim, nil, nil
	c, e := s.SaveLocal(ctx, "", cfg)
	if e != nil {
		t.Fatal(e)
	}
	job := localJob(t, s, core, c.ID, "call", map[string]any{"label": "nul-bytes"})
	if e = NewRunner(s, core.Store()).Tick(ctx); e != nil {
		t.Fatal(e)
	}
	job, e = core.Store().GetJob(ctx, persona, job.JobID)
	if e != nil {
		t.Fatal(e)
	}
	if job.Status != "done" || job.Result["outcome"] != "returned" || job.Result["output_schema_valid"] != true {
		t.Fatalf("a Local call that really ran was recorded as a loss: %+v", job)
	}
	raw, _ := json.Marshal(job.Result)
	if bytes.Contains(raw, []byte(`\u0000`)) {
		t.Fatalf("NUL survived into the durable result: %s", raw)
	}
	for _, want := range []string{"before�after", "key�in-map", "value�here"} {
		if !bytes.Contains(raw, []byte(want)) {
			t.Fatalf("%q was not normalized in place: %s", want, raw)
		}
	}
	effects, _ := os.ReadFile(filepath.Join(cfg.Cwd, "effects.txt"))
	if string(effects) != "nul-bytes\n" {
		t.Fatalf("the call did not run exactly once: %q", effects)
	}
}
