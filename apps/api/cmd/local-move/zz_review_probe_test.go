package main

// Reviewer probes (independent review transfer-session-independent-a).
// Instrumentation only. No product code is changed.

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The progress lock is an flock on <home>/move/lock: a second mutating
// command is refused while one runs; status must still work.
func TestProbeLockBlocksConcurrentCommands(t *testing.T) {
	c := setupMove(t)
	uid := "probe-lock-" + c.pid[24:]
	_, moveURL := c.newSession(uid)

	// Record a move first so status has something to report.
	m0, _ := c.mover()
	if code := m0.Start(c.ctx, moveURL); code != exitPending {
		t.Fatalf("start: %d", code)
	}
	m1, _ := c.mover()
	release, err := m1.lock()
	if err != nil {
		t.Fatal(err)
	}
	m2, out2 := c.mover()
	if code := m2.Resume(c.ctx); code != exitError || !strings.Contains(out2.String(), "already running") {
		t.Fatalf("resume under a held lock: %d %s", code, out2)
	}
	// Status does not take the lock up front.
	m3, out3 := c.mover()
	if code := m3.Status(c.ctx); code != exitDone {
		t.Fatalf("status under a held lock: %d %s", code, out3)
	}
	release()
	// After release the same command runs.
	m4, _ := c.mover()
	if code := m4.Resume(c.ctx); code != exitPending {
		t.Fatalf("resume after lock release: %d", code)
	}
}

// An unreadable state file is reported and kept — never overwritten.
func TestProbeCorruptStateFile(t *testing.T) {
	c := setupMove(t)
	dir := filepath.Join(c.home, "move")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	sp := filepath.Join(dir, "state.json")
	if err := os.WriteFile(sp, []byte("{ not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	m, out := c.mover()
	if code := m.Resume(c.ctx); code != exitError || !strings.Contains(out.String(), "unreadable") || !strings.Contains(out.String(), "kept as-is") {
		t.Fatalf("resume on corrupt state: %d %s", code, out)
	}
	raw, _ := os.ReadFile(sp)
	if string(raw) != "{ not json" {
		t.Fatal("corrupt state file was overwritten")
	}
}

// The move directory is 0700 and the state file holding the grant is 0600;
// the grant never reaches command output.
func TestProbeStateFileHygiene(t *testing.T) {
	c := setupMove(t)
	uid := "probe-hygiene-" + c.pid[24:]
	_, moveURL := c.newSession(uid)
	grant := moveURL[strings.Index(moveURL, "#grant=")+7:]

	m, out := c.mover()
	if code := m.Start(c.ctx, moveURL); code != exitPending {
		t.Fatalf("start: %d %s", code, out)
	}
	if strings.Contains(out.String(), grant) {
		t.Fatal("grant appeared in command output")
	}
	di, err := os.Stat(filepath.Join(c.home, "move"))
	if err != nil || di.Mode().Perm() != 0o700 {
		t.Fatalf("move dir mode: %v %v", di, err)
	}
	sp := filepath.Join(c.home, "move", "state.json")
	fi, err := os.Stat(sp)
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("state file mode: %v %v", fi, err)
	}
	// The grant is in the state file only.
	raw, _ := os.ReadFile(sp)
	if !strings.Contains(string(raw), grant) {
		t.Fatal("grant missing from state file")
	}
}

// A release build (no movefailpoint tag) ignores SUMI_LOCAL_MOVE_FAILPOINT.
func TestProbeReleaseBinaryIgnoresFailpointEnv(t *testing.T) {
	c := setupMove(t)
	bin := filepath.Join(t.TempDir(), "sumi-local-move")
	if out, err := exec.Command("go", "build", "-buildvcs=false", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	uid := "probe-release-" + c.pid[24:]
	sid, moveURL := c.newSession(uid)
	if r := c.run(bin, nil, moveURL+"\n", "start", "--wait", "0"); r.code != exitPending {
		t.Fatalf("start: %+v", r)
	}
	c.provision(uid, sid)
	r := c.run(bin, []string{"SUMI_LOCAL_MOVE_FAILPOINT=after-complete"}, "", "resume")
	if r.killed || r.code != exitDone || !strings.Contains(r.out, "moved to Sumi Cloud") {
		t.Fatalf("release build honoured the failpoint env: %+v", r)
	}
}

// start with a foreign session id while a move is in progress is refused;
// re-entering the same URL resumes it.
func TestProbeStartGuardsTheRecordedMove(t *testing.T) {
	c := setupMove(t)
	uid := "probe-guard-" + c.pid[24:]
	_, moveURL := c.newSession(uid)
	m, out := c.mover()
	if code := m.Start(c.ctx, moveURL); code != exitPending {
		t.Fatalf("start: %d %s", code, out)
	}
	// A different session URL is refused.
	sid2, url2 := c.newSession(uid + "-other")
	_ = sid2
	m2, out2 := c.mover()
	if code := m2.Start(c.ctx, url2); code != exitError || !strings.Contains(out2.String(), "still in progress") {
		t.Fatalf("start of a second move: %d %s", code, out2)
	}
	// Re-entering the same URL drives the same move, not a new one.
	m3, _ := c.mover()
	if code := m3.Start(c.ctx, moveURL); code != exitPending {
		t.Fatalf("re-start of the same move: %d", code)
	}
	// And status sees a single recorded move.
	if o, ok := c.recordedOutcome(); !ok || o != "" {
		t.Fatalf("unexpected recorded outcome %q %v", o, ok)
	}
}
