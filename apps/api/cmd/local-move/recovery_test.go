package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/portable"
	"github.com/sumi-studio/sumi/apps/api/internal/transfersession"
)

type answer struct {
	MoveURL   string                `json:"move_url"`
	SessionID string                `json:"session_id"`
	Session   *transfersession.View `json:"session"`
	view      transfersession.View
	raw       string
}

// registrant calls a registering-browser route over HTTP under a FIXTURE
// flow for uid (transfersessiontest.Proof stands in for the auth adapter).
func (c *cloud) registrant(uid, path string, extra map[string]string) (int, answer, error) {
	flow, nonce := uuid.NewString(), uuid.NewString()
	c.proof.Add(flow, nonce, uid, time.Minute)
	body := map[string]string{"flow_id": flow, "nonce": nonce}
	for k, v := range extra {
		body[k] = v
	}
	res, err := http.Post(c.srv.URL+transfersession.RoutePrefix+path, "application/json", jsonBody(body))
	if err != nil {
		return 0, answer{}, err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return res.StatusCode, answer{}, err
	}
	a := answer{raw: string(raw)}
	_ = json.Unmarshal(raw, &a)
	if a.Session != nil {
		a.view = *a.Session
	} else {
		_ = json.Unmarshal(raw, &a.view)
	}
	return res.StatusCode, a, nil
}

func (c *cloud) mustRegistrant(uid, path string, extra map[string]string, want int) answer {
	c.t.Helper()
	code, a, err := c.registrant(uid, path, extra)
	if err != nil || code != want {
		c.t.Fatalf("POST %s: %d %v %s (want %d)", path, code, err, a.raw, want)
	}
	return a
}

var expectUnused = map[string]string{"expect_status": "awaiting_bundle"}

// TestLostMoveURLIsRecoveredFromTheOpenSession composes the registrant
// routes after a create whose answer (the only copy of the grant) was lost.
// A retried create never cancels; the person's explicit "new move URL"
// choice is a cancel guarded by the status they were shown.
func TestLostMoveURLIsRecoveredFromTheOpenSession(t *testing.T) {
	t.Run("the answer was lost before anyone saw the URL", func(t *testing.T) {
		c := setupMove(t)
		uid := "fixture-lostcreate-" + c.pid[24:]
		var lost atomic.Bool
		c.setIntercept(func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method != http.MethodPost || r.URL.Path != transfersession.RoutePrefix+"/sessions" || !lost.CompareAndSwap(false, true) {
				return false
			}
			c.mux.ServeHTTP(httptest.NewRecorder(), r) // the session commits
			panic(http.ErrAbortHandler)                // and its answer never arrives
		})
		if code, a, err := c.registrant(uid, "/sessions", nil); err == nil {
			t.Fatalf("the create answer arrived: %d %s", code, a.raw)
		}
		c.setIntercept(nil)
		var first string
		if err := c.dest.pool.QueryRow(c.ctx, `SELECT session_id FROM transfer_sessions
			WHERE claim_subject = $1 AND status = 'awaiting_bundle'`, uid).Scan(&first); err != nil {
			t.Fatalf("the lost create did not commit: %v", err)
		}

		// Retrying names the open session and its status, never a grant, and
		// changes nothing however often it repeats.
		for i := 0; i < 2; i++ {
			dup := c.mustRegistrant(uid, "/sessions", nil, http.StatusConflict)
			if dup.SessionID != first || dup.view.Status != "awaiting_bundle" || dup.view.Arrival != nil ||
				dup.view.Retired || dup.MoveURL != "" || strings.Contains(dup.raw, "grant") || strings.Contains(dup.raw, "proof") {
				t.Fatalf("retry %d: %s", i, dup.raw)
			}
		}
		if s := c.sessionStatus(first); s != "awaiting_bundle" {
			t.Fatalf("a retried create changed the session: %s", s)
		}

		// The person chose "get a new move URL".
		if a := c.mustRegistrant(uid, "/sessions/"+first+"/cancel", expectUnused, http.StatusOK); a.view.Status != "cancelled" {
			t.Fatalf("replace: %s", a.raw)
		}
		second := c.mustRegistrant(uid, "/sessions", nil, http.StatusCreated)
		if second.view.SessionID == first || second.MoveURL == "" {
			t.Fatalf("new session: %s", second.raw)
		}
		m, out := c.mover()
		expect(t, m.Start(c.ctx, second.MoveURL), exitPending, out, "waiting for the registration")
		c.provision(uid, second.view.SessionID)
		expect(t, m.Resume(c.ctx), exitDone, out, "moved to Sumi Cloud")
		if l, d := authority(t, c.local, c.pid), authority(t, c.dest, c.pid); l != "transferred" || d != "active" {
			t.Fatalf("after the move: local %s, cloud %s", l, d)
		}
		if _, err := c.dest.svc.Status(c.ctx, "import", first); !errors.Is(err, portable.ErrTransferNotFound) || c.sessionStatus(first) != "cancelled" {
			t.Fatalf("the replaced session: %v %s", err, c.sessionStatus(first))
		}
	})

	t.Run("the URL was handed out and sealed but not uploaded", func(t *testing.T) {
		c := setupMove(t)
		uid := "fixture-handedout-" + c.pid[24:]
		first := c.mustRegistrant(uid, "/sessions", nil, http.StatusCreated)
		sid1, grant1 := first.view.SessionID, first.MoveURL[strings.Index(first.MoveURL, "#grant=")+7:]
		c.setIntercept(failUploads)
		m, out := c.mover()
		expect(t, m.Start(c.ctx, first.MoveURL), exitPending, out, "stays sealed")
		c.setIntercept(nil)

		// The browser lost the page. Cloud cannot see a seal that never
		// uploaded, so this looks exactly like the unused case.
		if dup := c.mustRegistrant(uid, "/sessions", nil, http.StatusConflict); dup.SessionID != sid1 || dup.view.Status != "awaiting_bundle" {
			t.Fatalf("retry: %s", dup.raw)
		}
		c.mustRegistrant(uid, "/sessions/"+sid1+"/cancel", expectUnused, http.StatusOK)
		second := c.mustRegistrant(uid, "/sessions", nil, http.StatusCreated)

		// The sealed source still holds the first grant: it earns the
		// retirement proof and its secretary is active again before it can
		// start the new move.
		expect(t, m.Start(c.ctx, second.MoveURL), exitError, out, "still in progress")
		expect(t, m.Resume(c.ctx), exitDone, out, "active on Local again")
		if a := authority(t, c.local, c.pid); a != "active" {
			t.Fatalf("local after the replaced session: %s", a)
		}
		if v, err := c.sessions.Status(c.ctx, sid1, grant1); err != nil || v.Status != "cancelled" || !v.Retired || v.RetireProof == "" {
			t.Fatalf("the first grant's receipt: %+v %v", v, err)
		}
		expect(t, m.Start(c.ctx, second.MoveURL), exitPending, out, "waiting for the registration")
		c.provision(uid, second.view.SessionID)
		expect(t, m.Resume(c.ctx), exitDone, out, "moved to Sumi Cloud")
		if l, d := authority(t, c.local, c.pid), authority(t, c.dest, c.pid); l != "transferred" || d != "active" {
			t.Fatalf("after the move: local %s, cloud %s", l, d)
		}
	})

	t.Run("the secretary arrived, so nothing is replaced", func(t *testing.T) {
		c := setupMove(t)
		uid := "fixture-arrived-" + c.pid[24:]
		first := c.mustRegistrant(uid, "/sessions", nil, http.StatusCreated)
		sid := first.view.SessionID
		m, out := c.mover()
		expect(t, m.Start(c.ctx, first.MoveURL), exitPending, out, "waiting for the registration")

		dup := c.mustRegistrant(uid, "/sessions", nil, http.StatusConflict)
		if dup.SessionID != sid || dup.view.Status != "staged" || dup.view.Arrival == nil || dup.view.Arrival.PersonaID != c.pid {
			t.Fatalf("retry after arrival: %s", dup.raw)
		}
		refused := c.mustRegistrant(uid, "/sessions/"+sid+"/cancel", expectUnused, http.StatusConflict)
		if refused.view.Status != "staged" || c.sessionStatus(sid) != "staged" || authority(t, c.dest, c.pid) != "staged" {
			t.Fatalf("a guarded replace touched an arrived secretary: %s", refused.raw)
		}
		// Registration continues without the grant.
		c.provision(uid, sid)
		expect(t, m.Resume(c.ctx), exitDone, out, "moved to Sumi Cloud")
	})

	t.Run("an import that committed before the session caught up counts as arrived", func(t *testing.T) {
		c := setupMove(t)
		uid := "fixture-uncaught-" + c.pid[24:]
		sid, _ := c.newSession(uid)
		dest, err := c.dest.svc.PlacementID(c.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.local.svc.Seal(c.ctx, c.pid, sid, dest); err != nil {
			t.Fatal(err)
		}
		var bundle bytes.Buffer
		if _, err := c.local.svc.Export(c.ctx, c.pid, sid, &bundle); err != nil {
			t.Fatal(err)
		}
		// The upload's import committed; the handler died before the session update.
		if _, _, err := c.dest.svc.Import(c.ctx, &bundle, nil, false); err != nil {
			t.Fatal(err)
		}
		if s := c.sessionStatus(sid); s != "awaiting_bundle" {
			t.Fatalf("precondition: %s", s)
		}
		refused := c.mustRegistrant(uid, "/sessions/"+sid+"/cancel", expectUnused, http.StatusConflict)
		if refused.view.Status != "staged" || authority(t, c.dest, c.pid) != "staged" {
			t.Fatalf("a guarded replace discarded a committed import: %s", refused.raw)
		}
	})
}

// buildFailpointMove builds the real command with the movefailpoint tag.
func buildFailpointMove(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "sumi-local-move")
	if out, err := exec.Command("go", "build", "-tags", "movefailpoint", "-buildvcs=false", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return bin
}

type runResult struct {
	code   int
	killed bool
	out    string
}

// run executes the command binary as its own process against this Local
// placement; env adds variables such as a failpoint.
func (c *cloud) run(bin string, env []string, stdin string, args ...string) runResult {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.ctx, 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = append(os.Environ(), "SUMI_LOCAL_HOME="+c.home,
		"SUMI_DB_URL="+c.local.pool.Config().ConnString(), "SUMI_PERSONA_ID="+c.pid)
	cmd.Env = append(cmd.Env, env...)
	cmd.Stdin = strings.NewReader(stdin)
	out, err := cmd.CombinedOutput()
	r := runResult{out: string(out)}
	var ee *exec.ExitError
	switch {
	case errors.As(err, &ee):
		ws, _ := ee.Sys().(syscall.WaitStatus)
		r.code, r.killed = ee.ExitCode(), ws.Signaled() && ws.Signal() == syscall.SIGKILL
	case err != nil:
		c.t.Fatalf("run %v: %v", args, err)
	}
	if ctx.Err() != nil {
		c.t.Fatalf("run %v did not finish:\n%s", args, out)
	}
	return r
}

// recordedOutcome reads the command's own durable record.
func (c *cloud) recordedOutcome() (string, bool) {
	c.t.Helper()
	raw, err := os.ReadFile(filepath.Join(c.home, "move", "state.json"))
	if errors.Is(err, os.ErrNotExist) {
		return "", false
	}
	var st moveState
	if err != nil || json.Unmarshal(raw, &st) != nil {
		c.t.Fatalf("state file: %v %s", err, raw)
	}
	return st.Outcome, true
}

// source reads the Local ledger and the Local copy of the carried work
// straight from the source database.
func (c *cloud) source(sid string) (exports int, ledger, authorityNow, input string, turns int) {
	c.t.Helper()
	if err := c.local.pool.QueryRow(c.ctx, `SELECT
		(SELECT count(*) FROM core_transfers WHERE direction = 'export'),
		(SELECT status FROM core_transfers WHERE direction = 'export' AND transfer_id = $2),
		(SELECT authority FROM core_personas WHERE persona_id = $1),
		(SELECT status FROM core_inputs WHERE persona_id = $1 AND input_id = 'in-carried'),
		(SELECT count(*) FROM core_turns WHERE persona_id = $1)`, c.pid, sid).
		Scan(&exports, &ledger, &authorityNow, &input, &turns); err != nil {
		c.t.Fatal(err)
	}
	return
}

// stopAtFailpoint starts a real move, provisions it with the FIXTURE account
// transaction, and runs resume in a process killed at the failpoint.
func stopAtFailpoint(t *testing.T, failpoint string) (c *cloud, bin, sid, moveURL string) {
	t.Helper()
	c = setupMove(t)
	bin = buildFailpointMove(t)
	uid := "fixture-stop-" + c.pid[24:]
	sid, moveURL = c.newSession(uid)
	if r := c.run(bin, nil, moveURL+"\n", "start", "--wait", "0"); r.code != exitPending || !strings.Contains(r.out, "waiting for the registration") {
		t.Fatalf("start: %+v", r)
	}
	c.provision(uid, sid)
	r := c.run(bin, []string{"SUMI_LOCAL_MOVE_FAILPOINT=" + failpoint}, "", "resume")
	if !r.killed || strings.Contains(r.out, "moved to Sumi Cloud") {
		t.Fatalf("resume at %s was not stopped there: %+v", failpoint, r)
	}
	return c, bin, sid, moveURL
}

func cloudDark(w http.ResponseWriter, _ *http.Request) bool {
	w.WriteHeader(http.StatusServiceUnavailable)
	return true
}

// TestCommandStoppedAroundComplete kills the real command process at the
// source Complete transaction and checks the next invocation reaches the
// right branch from what the database committed.
func TestCommandStoppedAroundComplete(t *testing.T) {
	checkUntouched := func(t *testing.T, c *cloud, sid string, outs ...string) {
		t.Helper()
		exports, ledger, auth, input, turns := c.source(sid)
		if exports != 1 || ledger != "completed" || auth != "transferred" || input != "queued" || turns != 0 {
			t.Fatalf("source after recovery: exports %d, ledger %s, authority %s, input %s, turns %d", exports, ledger, auth, input, turns)
		}
		if _, err := c.local.state.AcquireWriter(c.ctx, c.pid, "local-core", time.Minute); !errors.Is(err, agentstate.ErrPersonaInactive) {
			t.Fatalf("local writer after the move: %v", err)
		}
		for _, o := range outs {
			if strings.Contains(o, "Sealing") || strings.Contains(o, "Uploading") {
				t.Fatalf("recovery sealed or uploaded again:\n%s", o)
			}
		}
	}

	t.Run("after the commit, resumed while Cloud is unreachable", func(t *testing.T) {
		c, bin, sid, moveURL := stopAtFailpoint(t, "after-complete")
		grant := moveURL[strings.Index(moveURL, "#grant=")+7:]
		// Committed: the source transaction. Not written: the command's outcome.
		if exports, ledger, auth, _, _ := c.source(sid); exports != 1 || ledger != "completed" || auth != "transferred" {
			t.Fatalf("after the stop: exports %d, ledger %s, authority %s", exports, ledger, auth)
		}
		if o, ok := c.recordedOutcome(); !ok || o != "" {
			t.Fatalf("the stopped process recorded %q (%t)", o, ok)
		}
		c.setIntercept(cloudDark)
		resumed := c.run(bin, nil, "", "resume")
		if resumed.code != exitDone || !strings.Contains(resumed.out, "moved to Sumi Cloud") {
			t.Fatalf("resume: %+v", resumed)
		}
		if o, _ := c.recordedOutcome(); o != "transferred" {
			t.Fatalf("outcome after resume: %q", o)
		}
		status := c.run(bin, nil, "", "status")
		if status.code != exitDone || !strings.Contains(status.out, "Outcome: transferred") || !strings.Contains(status.out, "Local authority transferred") {
			t.Fatalf("status: %+v", status)
		}
		checkUntouched(t, c, sid, resumed.out, status.out)
		for _, o := range []string{resumed.out, status.out} {
			if strings.Contains(o, grant) {
				t.Fatal("the grant was printed")
			}
		}
	})

	t.Run("after the commit, status records it and the state file is not needed", func(t *testing.T) {
		c, bin, sid, moveURL := stopAtFailpoint(t, "after-complete")
		c.setIntercept(cloudDark)
		status := c.run(bin, nil, "", "status")
		if status.code != exitDone || !strings.Contains(status.out, "Outcome: transferred") {
			t.Fatalf("status: %+v", status)
		}
		if o, _ := c.recordedOutcome(); o != "transferred" {
			t.Fatalf("status did not record the proven outcome: %q", o)
		}
		// Even without any local record, the same move URL finishes from the
		// ledger instead of sealing again.
		if err := os.RemoveAll(filepath.Join(c.home, "move")); err != nil {
			t.Fatal(err)
		}
		if r := c.run(bin, nil, "", "resume"); r.code != exitUsage || !strings.Contains(r.out, "No move is recorded") {
			t.Fatalf("resume without a record: %+v", r)
		}
		again := c.run(bin, nil, moveURL+"\n", "start")
		if again.code != exitDone || !strings.Contains(again.out, "moved to Sumi Cloud") {
			t.Fatalf("start again: %+v", again)
		}
		if o, _ := c.recordedOutcome(); o != "transferred" {
			t.Fatalf("outcome after start again: %q", o)
		}
		checkUntouched(t, c, sid, status.out, again.out)
	})

	t.Run("before the commit, the next resume completes the same transfer", func(t *testing.T) {
		c, bin, sid, _ := stopAtFailpoint(t, "before-complete")
		if exports, ledger, auth, _, _ := c.source(sid); exports != 1 || ledger != "sealed" || auth != "sealed" {
			t.Fatalf("after the stop: exports %d, ledger %s, authority %s", exports, ledger, auth)
		}
		if s, d := c.sessionStatus(sid), authority(t, c.dest, c.pid); s != "activated" || d != "active" {
			t.Fatalf("cloud after the stop: session %s, authority %s", s, d)
		}
		resumed := c.run(bin, nil, "", "resume")
		if resumed.code != exitDone || !strings.Contains(resumed.out, "moved to Sumi Cloud") {
			t.Fatalf("resume: %+v", resumed)
		}
		checkUntouched(t, c, sid, resumed.out)
	})
}
