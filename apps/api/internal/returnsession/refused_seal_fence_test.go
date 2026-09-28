package returnsession_test

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
	"github.com/sumi-studio/sumi/apps/api/internal/returnsession"
)

// A local-mode return fences the Cloud workspace only for a seal that can
// happen. When the seal is refused because the secretary still has
// unfinished work, that work — and the person — keep a writable workspace
// on Cloud, and the move is retried or cancelled afterwards. A seal that
// is really in flight, or already committed, keeps its fence.

func bindLocal(t *testing.T, h *harness, sid, grant string) (int, []byte) {
	t.Helper()
	return h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sid),
		grant, jsonBody(destMode(t, h.local, h.persona, "absent", "local")))
}

func refusalCode(t *testing.T, raw []byte) string {
	t.Helper()
	var body struct {
		Code string `json:"code"`
	}
	unmarshal(t, raw, &body)
	return body.Code
}

func submitJob(t *testing.T, h *harness, id string) {
	t.Helper()
	if _, _, err := h.cloud.state.SubmitJob(h.ctx, h.persona, id, "subprocess",
		map[string]any{"command": []any{"echo", "hi"}}, "api"); err != nil {
		t.Fatal(err)
	}
}

func finishJob(t *testing.T, h *harness, id string) {
	t.Helper()
	if _, _, err := h.cloud.state.ClaimJobs(h.ctx, h.persona, "runner-1", []string{"subprocess"}, time.Minute, 4, "*"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.cloud.state.CompleteJob(h.ctx, h.persona, id, "runner-1", "done",
		map[string]any{"exit_code": 0.0}, ""); err != nil {
		t.Fatal(err)
	}
}

func fileEpoch(t *testing.T, h *harness, sid string) int64 {
	t.Helper()
	var epoch int64
	if err := h.cloud.pool.QueryRow(h.ctx, `SELECT file_epoch FROM return_sessions WHERE session_id = $1`,
		sid).Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	return epoch
}

// The owner asks for the move while a job still runs: the bind is refused
// before any fence goes up, reads and sweeps never raise one, and once the
// job has finished the same destination seals.
func TestRefusedSealKeepsCloudWorkspaceWritable(t *testing.T) {
	h := setup(t, returnsession.Config{})
	ff := &fakeFiles{}
	h.sessions.SetFileStore(ff)
	submitJob(t, h, "j-running")

	sid, _, grant := h.createMode("local")
	code, raw := bindLocal(t, h, sid, grant)
	if code != http.StatusConflict || refusalCode(t, raw) != "unfinished_work" || !strings.Contains(string(raw), "j-running") {
		t.Fatalf("bind with a running job: %d %s — want 409 unfinished_work naming the job", code, raw)
	}
	for _, c := range ff.callsFor(sid) {
		if strings.HasSuffix(c, "=true") {
			t.Fatalf("a refused bind raised the fence: %v", ff.callsFor(sid))
		}
	}
	if got := authority(t, h.cloud, h.persona); got != "active" {
		t.Fatalf("authority %s", got)
	}
	if got := h.sessionStatus(sid); got != returnsession.StatusAwaitingDestination {
		t.Fatalf("session %s", got)
	}

	// Status reads, the owner's view and sweeps past the admission
	// deadline converge the bound-but-unsealed session — none refreezes.
	mustExec(t, h.cloud.pool, `UPDATE return_sessions SET admit_until = now() - interval '1 minute' WHERE session_id = $1`, sid)
	if code, raw := h.grantReq(http.MethodGet, "/api/secretary-return/sessions/"+sid, grant, nil); code != http.StatusOK {
		t.Fatalf("status: %d %s", code, raw)
	}
	if code, raw := h.ownerReq(http.MethodGet, "/api/secretary-return/session", nil); code != http.StatusOK {
		t.Fatalf("owner view: %d %s", code, raw)
	}
	if _, err := h.sessions.Sweep(h.ctx); err != nil {
		t.Fatal(err)
	}
	if ff.fencedBy(sid) {
		t.Fatalf("a reconcile refroze the refused session: %v", ff.callsFor(sid))
	}
	if got := h.sessionStatus(sid); got != returnsession.StatusAwaitingDestination {
		t.Fatalf("the bound session must stay open for the retry: %s", got)
	}

	// The job finishes on Cloud; the retried bind fences and seals.
	finishJob(t, h, "j-running")
	code, raw = bindLocal(t, h, sid, grant)
	if code != http.StatusOK || asView(t, raw).Status != returnsession.StatusSealed {
		t.Fatalf("retry after the job finished: %d %s", code, raw)
	}
	if !ff.fencedBy(sid) {
		t.Fatalf("the sealed copy window is not fenced: %v", ff.callsFor(sid))
	}
}

// The owner gives up instead: the never-sealed session closes and the
// secretary stays active with its workspace unfenced.
func TestRefusedSealThenCancelLeavesSecretaryActive(t *testing.T) {
	h := setup(t, returnsession.Config{})
	ff := &fakeFiles{}
	h.sessions.SetFileStore(ff)
	submitJob(t, h, "j-running")
	sid, _, grant := h.createMode("local")
	if code, raw := bindLocal(t, h, sid, grant); code == http.StatusOK {
		t.Fatalf("bind with a running job sealed: %s", raw)
	}
	if code, raw := h.ownerReq(http.MethodPost, fmt.Sprintf("/api/secretary-return/sessions/%s/cancel", sid), nil); code != http.StatusOK {
		t.Fatalf("owner cancel: %d %s", code, raw)
	}
	if _, err := h.sessions.Sweep(h.ctx); err != nil {
		t.Fatal(err)
	}
	if got := h.sessionStatus(sid); got != returnsession.StatusCancelled {
		t.Fatalf("session %s", got)
	}
	if ff.fencedBy(sid) {
		t.Fatalf("cancelled session left a fence: %v", ff.callsFor(sid))
	}
	if got := authority(t, h.cloud, h.persona); got != "active" {
		t.Fatalf("authority %s", got)
	}
}

// Refusals that only show up once the fence is up — a live terminal, a
// fence that could not be confirmed, a job admitted between the early
// check and the seal — take the fence down again before answering.
func TestSealRefusedAfterFenceReleasesIt(t *testing.T) {
	t.Run("live terminal session", func(t *testing.T) {
		h := setup(t, returnsession.Config{})
		ff := &fakeFiles{}
		h.sessions.SetFileStore(ff)
		procs := newFakeProcs()
		h.sessions.SetTerminalProcesses(procs)
		term := newID(t)
		termRow(t, h, term, "active")
		procs.addOp(h.persona, term, false)

		sid, _, grant := h.createMode("local")
		code, raw := bindLocal(t, h, sid, grant)
		if code != http.StatusConflict || refusalCode(t, raw) != "terminal_sessions_open" {
			t.Fatalf("bind: %d %s", code, raw)
		}
		assertRaisedThenReleased(t, h, ff, sid)
	})
	t.Run("fence not confirmed", func(t *testing.T) {
		h := setup(t, returnsession.Config{})
		ff := &fakeFiles{}
		h.sessions.SetFileStore(ff)
		ff.set(errors.New("file effects still settling"), nil)

		sid, _, grant := h.createMode("local")
		if code, raw := bindLocal(t, h, sid, grant); code == http.StatusOK {
			t.Fatalf("bind sealed without a confirmed fence: %s", raw)
		}
		assertRaisedThenReleased(t, h, ff, sid)

		// The file service recovers; the retry fences and seals.
		ff.set(nil, nil)
		if code, raw := bindLocal(t, h, sid, grant); code != http.StatusOK {
			t.Fatalf("retry: %d %s", code, raw)
		}
		if !ff.fencedBy(sid) || authority(t, h.cloud, h.persona) != "sealed" {
			t.Fatalf("retry did not seal behind the fence: %v", ff.callsFor(sid))
		}
	})
	t.Run("job admitted behind the early check", func(t *testing.T) {
		h := setup(t, returnsession.Config{})
		ff := &fakeFiles{}
		h.sessions.SetFileStore(ff)
		sid, _, grant := h.createMode("local")
		ff.set(nil, func(owner string) {
			if owner != sid {
				return
			}
			// Runs on the server's goroutine: report, never FailNow.
			if _, _, err := h.cloud.state.SubmitJob(h.ctx, h.persona, "j-late", "subprocess",
				map[string]any{"command": []any{"echo", "hi"}}, "api"); err != nil {
				t.Errorf("late job: %v", err)
			}
		})
		code, raw := bindLocal(t, h, sid, grant)
		if code != http.StatusConflict || refusalCode(t, raw) != "unfinished_work" || !strings.Contains(string(raw), "j-late") {
			t.Fatalf("bind: %d %s — the seal must refuse the late job", code, raw)
		}
		ff.set(nil, nil)
		assertRaisedThenReleased(t, h, ff, sid)
	})
}

func assertRaisedThenReleased(t *testing.T, h *harness, ff *fakeFiles, sid string) {
	t.Helper()
	calls := ff.callsFor(sid)
	raised := false
	for _, c := range calls {
		if strings.HasSuffix(c, "=true") {
			raised = true
		}
	}
	if !raised {
		t.Fatalf("the fence never went up, so this did not exercise the release: %v", calls)
	}
	if ff.fencedBy(sid) {
		t.Fatalf("the refused seal left the fence up: %v", calls)
	}
	if got := authority(t, h.cloud, h.persona); got != "active" {
		t.Fatalf("authority %s", got)
	}
	var exported bool
	if err := h.cloud.pool.QueryRow(h.ctx, `SELECT EXISTS(SELECT 1 FROM core_transfers
		WHERE direction = 'export' AND transfer_id = $1)`, sid).Scan(&exported); err != nil || exported {
		t.Fatalf("export exists after a refused seal: %v %v", exported, err)
	}
}

// While a seal attempt holds its fence, concurrent status reads, owner
// views and sweeps must not take it down, and a cancel waits for the
// attempt's outcome. The attempt then seals behind its fence.
func TestInFlightSealKeepsItsFence(t *testing.T) {
	h := setup(t, returnsession.Config{})
	ff := &fakeFiles{}
	h.sessions.SetFileStore(ff)
	sid, _, grant := h.createMode("local")

	entered := make(chan struct{})
	hold := make(chan struct{})
	var once sync.Once
	ff.set(nil, func(owner string) {
		// Only the bind's own fence call parks; the post-seal converge
		// re-asserts the same fence and passes straight through.
		if owner == sid {
			once.Do(func() {
				close(entered)
				<-hold
			})
		}
	})
	type result struct {
		code int
		raw  []byte
	}
	bindDone := make(chan result, 1)
	go func() {
		code, raw := bindLocal(t, h, sid, grant)
		bindDone <- result{code, raw}
	}()
	select {
	case <-entered:
	case <-time.After(15 * time.Second):
		t.Fatal("the bind never raised its fence")
	}
	ff.reset()

	// Readers converge without blocking behind the attempt and without
	// releasing its fence.
	readDone := make(chan error, 1)
	go func() {
		if code, raw := h.grantReq(http.MethodGet, "/api/secretary-return/sessions/"+sid, grant, nil); code != http.StatusOK {
			readDone <- fmt.Errorf("status: %d %s", code, raw)
			return
		}
		if code, raw := h.ownerReq(http.MethodGet, "/api/secretary-return/session", nil); code != http.StatusOK {
			readDone <- fmt.Errorf("owner view: %d %s", code, raw)
			return
		}
		_, err := h.sessions.Sweep(h.ctx)
		readDone <- err
	}()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("status reads blocked behind the in-flight seal")
	}
	for _, c := range ff.callsFor(sid) {
		if strings.HasSuffix(c, "=false") {
			t.Fatalf("a reader released the fence of a seal in flight: %v", ff.callsFor(sid))
		}
	}

	cancelDone := make(chan result, 1)
	go func() {
		code, raw := h.ownerReq(http.MethodPost, fmt.Sprintf("/api/secretary-return/sessions/%s/cancel", sid), nil)
		cancelDone <- result{code, raw}
	}()
	select {
	case r := <-cancelDone:
		t.Fatalf("the cancel did not wait for the in-flight seal: %d %s", r.code, r.raw)
	case <-time.After(300 * time.Millisecond):
	}

	close(hold)
	if r := <-bindDone; r.code != http.StatusOK {
		t.Fatalf("bind: %d %s", r.code, r.raw)
	}
	if r := <-cancelDone; r.code != http.StatusOK {
		t.Fatalf("cancel: %d %s", r.code, r.raw)
	}
	if got := h.sessionStatus(sid); got != returnsession.StatusCancelling {
		t.Fatalf("session %s (the seal committed; the cancel is a request)", got)
	}
	if _, err := h.sessions.Sweep(h.ctx); err != nil {
		t.Fatal(err)
	}
	for _, c := range ff.callsFor(sid) {
		if strings.HasSuffix(c, "=false") {
			t.Fatalf("the committed copy window lost its fence: %v", ff.callsFor(sid))
		}
	}
	if got := authority(t, h.cloud, h.persona); got != "sealed" {
		t.Fatalf("authority %s", got)
	}
}

// A barrier stranded by a crash between the fence and the seal's
// rollback comes down through the sweep alone — but never while the
// session row lock says a seal attempt is running.
func TestStrandedFenceReleasedOnlyWithoutSealInFlight(t *testing.T) {
	h := setup(t, returnsession.Config{})
	ff := &fakeFiles{}
	h.sessions.SetFileStore(ff)
	submitJob(t, h, "j-running")
	sid, _, grant := h.createMode("local")
	if code, raw := bindLocal(t, h, sid, grant); code == http.StatusOK {
		t.Fatalf("bind sealed: %s", raw)
	}
	scope, err := fileaccess.ScopeForPersona(h.persona)
	if err != nil {
		t.Fatal(err)
	}
	// The crashed attempt's fence, still standing.
	if err := ff.SetScopeFrozen(h.ctx, scope, sid, fileEpoch(t, h, sid), "return "+sid, true); err != nil {
		t.Fatal(err)
	}

	// A seal attempt (simulated) holds the session row lock: the sweep
	// must leave the fence alone.
	tx, err := h.cloud.pool.Begin(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(h.ctx, `SELECT 1 FROM return_sessions WHERE session_id = $1 FOR UPDATE`, sid); err != nil {
		t.Fatal(err)
	}
	if _, err := h.sessions.Sweep(h.ctx); err != nil {
		t.Fatal(err)
	}
	if !ff.fencedBy(sid) {
		t.Fatalf("the sweep released a fence while the session lock was held: %v", ff.callsFor(sid))
	}
	if err := tx.Rollback(h.ctx); err != nil {
		t.Fatal(err)
	}

	// Nobody reads the session; the sweep alone releases the barrier.
	if _, err := h.sessions.Sweep(h.ctx); err != nil {
		t.Fatal(err)
	}
	if ff.fencedBy(sid) {
		t.Fatalf("the stranded fence survived the sweep: %v", ff.callsFor(sid))
	}
	if got := h.sessionStatus(sid); got != returnsession.StatusAwaitingDestination {
		t.Fatalf("session %s", got)
	}
}

// A committed export is never thawed: not by a reconcile that finds the
// seal's status write missing, not by a re-bind whose fence call fails.
func TestCommittedSealKeepsFence(t *testing.T) {
	h := setup(t, returnsession.Config{})
	ff := &fakeFiles{}
	h.sessions.SetFileStore(ff)
	sid, _, grant := h.createMode("local")
	if code, raw := bindLocal(t, h, sid, grant); code != http.StatusOK {
		t.Fatalf("bind: %d %s", code, raw)
	}
	ff.reset()

	// The seal committed but its status write was lost.
	mustExec(t, h.cloud.pool, `UPDATE return_sessions SET status = 'awaiting_destination' WHERE session_id = $1`, sid)
	if _, err := h.sessions.Sweep(h.ctx); err != nil {
		t.Fatal(err)
	}
	if got := h.sessionStatus(sid); got != returnsession.StatusSealed {
		t.Fatalf("session %s", got)
	}

	// A replayed bind whose fence call fails refuses — and keeps the fence.
	ff.set(errors.New("filesvc unreachable"), nil)
	if code, raw := bindLocal(t, h, sid, grant); code == http.StatusOK {
		t.Fatalf("replay with a failing fence answered ok: %s", raw)
	}
	for _, c := range ff.callsFor(sid) {
		if strings.HasSuffix(c, "=false") {
			t.Fatalf("a committed seal's fence was released: %v", ff.callsFor(sid))
		}
	}
	if got := authority(t, h.cloud, h.persona); got != "sealed" {
		t.Fatalf("authority %s", got)
	}
}
