package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

// TestCarriedInputRunsOnceOnDestinationCore drives the real binary through
// a move and then runs the real TypeScript core host against the destination
// state service. The registration adapter and account transaction are the
// FIXTURE (transfersessiontest); the model is the deterministic mock
// provider, used because the fixture human has no model selection. It needs
// Node 22+ and apps/core, so it runs only with SUMI_TRANSFER_CORE_E2E=1
// (SUMI_TRANSFER_NODE selects the node binary).
func TestCarriedInputRunsOnceOnDestinationCore(t *testing.T) {
	if os.Getenv("SUMI_TRANSFER_CORE_E2E") != "1" {
		t.Skip("set SUMI_TRANSFER_CORE_E2E=1 to run the destination core host")
	}
	node := os.Getenv("SUMI_TRANSFER_NODE")
	if node == "" {
		node = "node"
	}
	evidence := os.Getenv("SUMI_TRANSFER_EVIDENCE_DIR")
	record := func(name, content string) {
		if evidence != "" {
			_ = os.MkdirAll(evidence, 0o755)
			_ = os.WriteFile(filepath.Join(evidence, name), []byte(content), 0o644)
		}
	}
	c := setupMove(t)
	ctx := c.ctx

	bin := filepath.Join(t.TempDir(), "sumi-local-move")
	if out, err := exec.Command("go", "build", "-buildvcs=false", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	uid := "fixture-core-" + c.pid[24:]
	sid, moveURL := c.newSession(uid)
	grant := moveURL[strings.Index(moveURL, "#grant=")+7:]
	cli := func(stdin string, args ...string) (int, string) {
		cmd := exec.CommandContext(ctx, bin, args...)
		cmd.Env = append(os.Environ(), "SUMI_LOCAL_HOME="+c.home,
			"SUMI_DB_URL="+c.local.pool.Config().ConnString(), "SUMI_PERSONA_ID="+c.pid)
		cmd.Stdin = strings.NewReader(stdin)
		out, err := cmd.CombinedOutput()
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return ee.ExitCode(), string(out)
		}
		if err != nil {
			t.Fatalf("run %v: %v", args, err)
		}
		return 0, string(out)
	}

	code, out := cli(moveURL+"\n", "start", "--wait", "0")
	record("cli-start.txt", out)
	if code != exitPending || strings.Contains(out, grant) || !strings.Contains(out, "waiting for the registration") {
		t.Fatalf("start: exit %d\n%s", code, out)
	}
	c.provision(uid, sid)
	code, out = cli("", "resume")
	record("cli-resume.txt", out)
	if code != exitDone || !strings.Contains(out, "moved to Sumi Cloud") {
		t.Fatalf("resume: exit %d\n%s", code, out)
	}
	code, out = cli("", "status")
	record("cli-status.txt", out)
	if code != exitDone || !strings.Contains(out, "Local authority transferred") || !strings.Contains(out, "not carried: files") {
		t.Fatalf("status: exit %d\n%s", code, out)
	}

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.srv.URL+"/internal/core/personas",
		strings.NewReader(`{"persona_id":"`+c.pid+`"}`))
	req.Header.Set("Authorization", "Bearer "+cloudAdmin)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(res.Body)
	res.Body.Close()
	var persona struct {
		Created bool   `json:"created"`
		Token   string `json:"persona_token"`
	}
	if err := json.Unmarshal(raw, &persona); err != nil || persona.Created || persona.Token == "" {
		t.Fatalf("cloud persona token: %d %s", res.StatusCode, raw)
	}

	runCore := func(name string) {
		cmd := exec.CommandContext(ctx, node, "src/host/local.ts", "--once")
		cmd.Dir = filepath.Join("..", "..", "..", "core")
		cmd.Env = append(os.Environ(), "SUMI_STATE_URL="+c.srv.URL, "SUMI_PERSONA_ID="+c.pid,
			"SUMI_PERSONA_TOKEN="+persona.Token, "SUMI_MODEL_PROVIDER=mock", "SUMI_HOLDER_ID=cloud-core",
			"SUMI_LEASE_TTL_MS=4000", "SUMI_ONCE_IDLE_MS=1500")
		var buf bytes.Buffer
		cmd.Stdout, cmd.Stderr = &buf, &buf
		done := make(chan error, 1)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		go func() { done <- cmd.Wait() }()
		select {
		case err := <-done:
			record(name, buf.String())
			if err != nil {
				t.Fatalf("%s: %v\n%s", name, err, buf.String())
			}
		case <-time.After(120 * time.Second):
			_ = cmd.Process.Kill()
			t.Fatalf("%s timed out\n%s", name, buf.String())
		}
	}
	counts := func(p placement) (string, int, int) {
		var status string
		var turns, completed int
		if err := p.pool.QueryRow(ctx, `SELECT
			(SELECT status FROM core_inputs WHERE persona_id = $1 AND input_id = 'in-carried'),
			(SELECT count(*) FROM core_turns WHERE persona_id = $1 AND input_id = 'in-carried'),
			(SELECT count(*) FROM core_outbox WHERE persona_id = $1 AND kind = 'turn_completed')`, c.pid).
			Scan(&status, &turns, &completed); err != nil {
			t.Fatal(err)
		}
		return status, turns, completed
	}

	if s, turns, completed := counts(c.dest); s != "queued" || turns != 0 || completed != 0 {
		t.Fatalf("before the destination core: %s %d %d", s, turns, completed)
	}
	runCore("core-run-1.txt")
	if s, turns, completed := counts(c.dest); s != "done" || turns != 1 || completed != 1 {
		t.Fatalf("after the destination core: input %s, turns %d, turn_completed %d", s, turns, completed)
	}
	runCore("core-run-2.txt")
	if s, turns, completed := counts(c.dest); s != "done" || turns != 1 || completed != 1 {
		t.Fatalf("after a second core run: input %s, turns %d, turn_completed %d", s, turns, completed)
	}
	if s, turns, _ := counts(c.local); s != "queued" || turns != 0 {
		t.Fatalf("the Local copy processed the carried input: %s %d", s, turns)
	}
	if _, err := c.local.state.AcquireWriter(ctx, c.pid, "local-core", time.Minute); !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("local writer after the move: %v", err)
	}
	var reply string
	_ = c.dest.pool.QueryRow(ctx, `SELECT payload::text FROM core_outbox WHERE persona_id = $1 AND kind = 'turn_completed'`, c.pid).Scan(&reply)
	record("destination-turn-completed.json", reply)
}
