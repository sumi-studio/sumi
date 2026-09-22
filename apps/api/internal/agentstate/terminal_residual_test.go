package agentstate

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// Residual-repair regressions (real Postgres) for the two root
// counterexamples:
//
//	TFIN-05 residual  a zero-width journal-loss marker must reach a
//	                  reader whose byte cursor is ALREADY at the
//	                  marker's position (including 0) exactly once:
//	                  byte progress and loss-notification progress are
//	                  separate dimensions — the reader echoes
//	                  event_cursor to consume each marker once.
//	TFIN-04/07 resid  an old pump's cached 'intended' row must never
//	                  gain delivery authority after a same-runner
//	                  replacement steals the claim: the steal bumps
//	                  the epoch and re-stamps 'intended' rows (the
//	                  stale dequeue fails); a 'dequeued' row becomes
//	                  'unknown' and is never re-served.

func TestResidualLossMarkerVisibleOnceAtCursor(t *testing.T) {
	for _, label := range []string{"zero", "caught-up"} {
		t.Run(label, func(t *testing.T) {
			s := newTerminalStore(t)
			ctx := context.Background()
			pa := pid(t)
			mustPersona(t, s, pa)
			sess := mustTerminalSession(t, s, pa, "sh")
			c := mustClaimTerminal(t, s, pa, "runner-a", time.Minute)

			at := int64(0)
			if label == "caught-up" {
				at = 10
				if _, err := s.AppendTerminalOutput(ctx, pa, sess.SessionID, "runner-a", c.Epoch,
					[]TerminalOutputChunk{{Kind: "data", Base: 0, Data: []byte("0123456789")}}); err != nil {
					t.Fatalf("append data: %v", err)
				}
			}
			if _, err := s.AppendTerminalOutput(ctx, pa, sess.SessionID, "runner-a", c.Epoch,
				[]TerminalOutputChunk{{Kind: "gap", Base: at, GapTo: &at}}); err != nil {
				t.Fatalf("append marker: %v", err)
			}

			// A reader already AT the marker's position must still see
			// the event once — the root counterexample failed here.
			got, err := s.ReadTerminalOutput(ctx, pa, sess.SessionID, at, 0, 32)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			var saw bool
			for _, x := range got.Chunks {
				if x.Kind == "gap" {
					saw = true
				}
			}
			if !saw {
				t.Fatalf("loss at current cursor %d is invisible: chunks=%+v", at, got.Chunks)
			}
			if got.NextCursor != at {
				t.Fatalf("marker moved byte cursor: next=%d want %d", got.NextCursor, at)
			}
			if got.EventCursor == 0 {
				t.Fatal("marker did not advance the event cursor")
			}

			// Exactly once: the same reader echoes event_cursor and the
			// marker is gone — idle repeated polls never re-serve it.
			for i := 0; i < 2; i++ {
				again, err := s.ReadTerminalOutput(ctx, pa, sess.SessionID, got.NextCursor, got.EventCursor, 32)
				if err != nil {
					t.Fatalf("repeat read %d: %v", i, err)
				}
				for _, x := range again.Chunks {
					if x.Kind == "gap" {
						t.Fatalf("consumed marker re-served on poll %d: %+v", i, again.Chunks)
					}
				}
			}

			// Later output after the boundary still flows under the
			// same cursor pair.
			if _, err := s.AppendTerminalOutput(ctx, pa, sess.SessionID, "runner-a", c.Epoch,
				[]TerminalOutputChunk{{Kind: "data", Base: at, Data: []byte("post")}}); err != nil {
				t.Fatalf("append post: %v", err)
			}
			post, err := s.ReadTerminalOutput(ctx, pa, sess.SessionID, got.NextCursor, got.EventCursor, 32)
			if err != nil {
				t.Fatalf("post read: %v", err)
			}
			if len(post.Chunks) != 1 || post.Chunks[0].Kind != "data" || string(post.Chunks[0].Data) != "post" {
				t.Fatalf("post read chunks = %+v", post.Chunks)
			}

			// Reconnect contract: a fresh reader (event_cursor omitted)
			// is shown every marker once — it has consumed none.
			fresh, err := s.ReadTerminalOutput(ctx, pa, sess.SessionID, at, 0, 32)
			if err != nil {
				t.Fatalf("fresh read: %v", err)
			}
			var markerCount int
			for _, x := range fresh.Chunks {
				if x.Kind == "gap" {
					markerCount++
				}
			}
			if markerCount != 1 {
				t.Fatalf("fresh reader saw %d markers, want 1", markerCount)
			}
		})
	}
}

func TestResidualLossMarkerToolReadOnce(t *testing.T) {
	s := newTerminalStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	sess := mustTerminalSession(t, s, pa, "sh")
	c := mustClaimTerminal(t, s, pa, "runner-a", time.Minute)

	if _, err := s.AppendTerminalOutput(ctx, pa, sess.SessionID, "runner-a", c.Epoch,
		[]TerminalOutputChunk{{Kind: "data", Base: 0, Data: []byte("before-")}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	at := int64(7)
	if _, err := s.AppendTerminalOutput(ctx, pa, sess.SessionID, "runner-a", c.Epoch,
		[]TerminalOutputChunk{{Kind: "gap", Base: at, GapTo: &at}}); err != nil {
		t.Fatalf("append marker: %v", err)
	}

	read := func(cursor, eventCursor int64) map[string]any {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		resp, err := s.internalTerminalTool(ctx, tx, pa, "terminal.read", map[string]any{
			"session_id": sess.SessionID, "cursor": float64(cursor), "event_cursor": float64(eventCursor),
		})
		if err != nil {
			t.Fatalf("terminal.read: %v", err)
		}
		return resp
	}

	// Caught-up reader: the marker is a notification, not bytes.
	r1 := read(7, 0)
	if c1, _ := r1["content"].(string); !strings.Contains(c1, "output may be missing at byte 7") {
		t.Fatalf("tool did not surface the caught-up boundary: %q", c1)
	}
	ec, _ := r1["event_cursor"].(int64)
	if ec == 0 {
		t.Fatal("tool did not return event_cursor")
	}
	// Consumed: echoing event_cursor suppresses the marker.
	r2 := read(7, ec)
	if c2, _ := r2["content"].(string); strings.Contains(c2, "missing") {
		t.Fatalf("tool re-served consumed marker: %q", c2)
	}
}

// TestResidualStealFencesCachedIntended is the root counterexample's
// fetch-before-dequeue window under the repaired contract: the old
// pump fetched the row, its lock connection died, a replacement
// incarnation of the same RunnerID claimed — the cached row must
// never gain delivery authority under the stale identity.
func TestResidualStealFencesCachedIntended(t *testing.T) {
	s := newTerminalStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	sess := mustTerminalSession(t, s, pa, "sh")
	c := mustClaimTerminal(t, s, pa, "runner-a#old", time.Minute)

	old, err := s.TryAcquireTerminalRunnerLock(ctx, "runner-a")
	if err != nil || old == nil {
		t.Fatalf("old lock %v %v", old, err)
	}
	defer old.Release(ctx)
	in, err := s.SubmitTerminalInput(ctx, pa, sess.SessionID, "human", "stdin",
		map[string]any{"data": "one effect"})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	fetched, err := s.PendingTerminalInputs(ctx, pa, sess.SessionID, "runner-a#old", c.Epoch)
	if err != nil || len(fetched) != 1 {
		t.Fatalf("old pending %+v %v", fetched, err)
	}

	// The old pump's dedicated lock connection dies; its pool
	// connections and pump goroutine survive — the replacement
	// acquires the same logical runner and adopts first.
	var terminated bool
	if err := s.pool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, old.PID()).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("kill lock conn: %v %v", terminated, err)
	}
	fresh, err := s.TryAcquireTerminalRunnerLock(ctx, "runner-a")
	if err != nil || fresh == nil {
		t.Fatalf("replacement lock %v %v", fresh, err)
	}
	defer fresh.Release(ctx)

	// The replacement steals: epoch bump + claim re-stamp in one
	// transaction — under its own incarnation.
	stolen, _, err := s.ClaimTerminalSessions(ctx, pa, "runner-a#new", "cloud", time.Minute, 4)
	if err != nil {
		t.Fatalf("steal claim: %v", err)
	}
	if len(stolen) != 1 || stolen[0].Epoch != c.Epoch+1 {
		t.Fatalf("steal result = %+v, want epoch %d", stolen, c.Epoch+1)
	}

	// The stale pump's cached fetch cannot dequeue: its (claim, epoch)
	// is gone. Exactly one dequeue grant exists — the replacement's.
	if _, err := s.ReportTerminalInputDisposition(ctx, pa, sess.SessionID, in.InputID,
		"runner-a#old", c.Epoch, "dequeued", nil); !errors.Is(err, ErrTerminalNotClaimed) {
		t.Fatalf("stale dequeue err = %v, want ErrTerminalNotClaimed", err)
	}
	pend, err := s.PendingTerminalInputs(ctx, pa, sess.SessionID, "runner-a#new", stolen[0].Epoch)
	if err != nil || len(pend) != 1 {
		t.Fatalf("replacement pending %+v %v", pend, err)
	}
	if _, err := s.ReportTerminalInputDisposition(ctx, pa, sess.SessionID, in.InputID,
		"runner-a#new", stolen[0].Epoch, "dequeued", nil); err != nil {
		t.Fatalf("replacement dequeue: %v", err)
	}
	if _, err := s.ReportTerminalInputDisposition(ctx, pa, sess.SessionID, in.InputID,
		"runner-a#new", stolen[0].Epoch, "written", nil); err != nil {
		t.Fatalf("replacement written: %v", err)
	}
}

// TestResidualStealDequeuedNeverResent is the already-dequeued
// window: the row left 'dequeued' when the claim is stolen becomes
// 'unknown' atomically — indeterminate, never re-served, and the
// stale pump can no longer disposition it.
func TestResidualStealDequeuedNeverResent(t *testing.T) {
	s := newTerminalStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	sess := mustTerminalSession(t, s, pa, "sh")
	c := mustClaimTerminal(t, s, pa, "runner-a#old", time.Minute)

	in, err := s.SubmitTerminalInput(ctx, pa, sess.SessionID, "human", "stdin",
		map[string]any{"data": "maybe delivered"})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.ReportTerminalInputDisposition(ctx, pa, sess.SessionID, in.InputID,
		"runner-a#old", c.Epoch, "dequeued", nil); err != nil {
		t.Fatalf("old dequeue: %v", err)
	}

	// Replacement incarnation steals before the old pump dispositions
	// the dequeue outcome.
	stolen, _, err := s.ClaimTerminalSessions(ctx, pa, "runner-a#new", "cloud", time.Minute, 4)
	if err != nil || len(stolen) != 1 {
		t.Fatalf("steal: %+v %v", stolen, err)
	}

	// The row is 'unknown' — the steal certified indeterminacy; the
	// replacement never re-serves it.
	pend, err := s.PendingTerminalInputs(ctx, pa, sess.SessionID, "runner-a#new", stolen[0].Epoch)
	if err != nil {
		t.Fatalf("replacement pending: %v", err)
	}
	for _, p := range pend {
		if p.InputID == in.InputID {
			t.Fatal("stolen-claim dequeued row was re-served to the replacement")
		}
	}
	rows, err := s.ListTerminalInputs(ctx, pa, sess.SessionID, 0, 16)
	if err != nil || len(rows) != 1 || rows[0].Status != "unknown" {
		t.Fatalf("ledger = %+v, want one unknown row", rows)
	}
	// The stale pump's late disposition is fenced too.
	if _, err := s.ReportTerminalInputDisposition(ctx, pa, sess.SessionID, in.InputID,
		"runner-a#old", c.Epoch, "written", nil); !errors.Is(err, ErrTerminalNotClaimed) {
		t.Fatalf("stale written err = %v, want ErrTerminalNotClaimed", err)
	}
}

// TestResidualStealPreservesOperationIdentity: the steal bumps epoch
// and claim identity but never clears operation_id — the replacement
// attaches to the same deterministic op.
func TestResidualStealPreservesOperationIdentity(t *testing.T) {
	s := newTerminalStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	sess := mustTerminalSession(t, s, pa, "sh")
	c := mustClaimTerminal(t, s, pa, "runner-a#old", time.Minute)
	if _, err := s.ReportTerminalStatus(ctx, pa, sess.SessionID, "runner-a#old", c.Epoch,
		"active", "", nil, "", "op-keep-1"); err != nil {
		t.Fatalf("active report: %v", err)
	}
	stolen, _, err := s.ClaimTerminalSessions(ctx, pa, "runner-a#new", "cloud", time.Minute, 4)
	if err != nil || len(stolen) != 1 {
		t.Fatalf("steal: %+v %v", stolen, err)
	}
	if stolen[0].OperationID != "op-keep-1" {
		t.Fatalf("steal lost operation identity: %+v", stolen[0])
	}
	// A plain (non-incarnation) claim of the same logical runner is
	// also a stale incarnation — restart paths that predate the
	// incarnation contract still steal correctly.
	stolen2, _, err := s.ClaimTerminalSessions(ctx, pa, "runner-a", "cloud", time.Minute, 4)
	if err != nil || len(stolen2) != 1 {
		t.Fatalf("plain-id steal: %+v %v", stolen2, err)
	}
	if stolen2[0].Epoch != stolen[0].Epoch+1 {
		t.Fatalf("plain-id steal epoch = %d, want %d", stolen2[0].Epoch, stolen[0].Epoch+1)
	}
}

// TestResidualRunnerLockPingReleaseRace exercises concurrent Ping and
// Release under -race: the lock's connection must never be probed
// while it is being returned to the pool.
func TestResidualRunnerLockPingReleaseRace(t *testing.T) {
	s := newTerminalStore(t)
	ctx := context.Background()
	for i := 0; i < 8; i++ {
		lock, err := s.TryAcquireTerminalRunnerLock(ctx, "runner-race")
		if err != nil || lock == nil {
			t.Fatalf("acquire %d: %v %v", i, lock, err)
		}
		var wg sync.WaitGroup
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				pc, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
				lock.Ping(pc)
				cancel()
				lock.PID()
			}()
		}
		lock.Release(ctx)
		wg.Wait()
		if lock.Ping(ctx) {
			t.Fatalf("lock %d still pingable after release", i)
		}
	}
}

// TREV2-02: expired claims held by a *different* logical runner must
// still surface the persona for discovery — the sweep inside
// ClaimTerminalSessions converts them so a renamed or dead runner can
// never strand a session. Live foreign leases are never stolen, and
// the backend boundary keeps foreign backends untouched. One persona
// per fixture keeps ClaimTerminalSessions deterministic.
func TestResidualForeignExpiredClaimSweptAndReclaimed(t *testing.T) {
	s := newTerminalStore(t)
	ctx := context.Background()
	newPA := func() string {
		pa := pid(t)
		mustPersona(t, s, pa)
		return pa
	}
	expire := func(sid string) {
		t.Helper()
		if _, err := s.pool.Exec(ctx, `
			UPDATE core_terminal_sessions
			SET claim_expires_at = now() - interval '1 second'
			WHERE session_id = $1`, sid); err != nil {
			t.Fatalf("expire: %v", err)
		}
	}
	get := func(pa, sid string) TerminalSession {
		t.Helper()
		sess, err := s.GetTerminalSession(ctx, pa, sid)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		return sess
	}

	// A) claimed/active session under an EXPIRED foreign claim —
	//    including an orphaned 'dequeued' input that must end
	//    'unknown', never re-served.
	paA := newPA()
	sessA := mustTerminalSession(t, s, paA, "a")
	claimA := mustClaimTerminal(t, s, paA, "foreign-runner#f1", time.Minute)
	in, err := s.SubmitTerminalInput(ctx, paA, sessA.SessionID, "human", "stdin",
		map[string]any{"data": "in flight"})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.ReportTerminalInputDisposition(ctx, paA, sessA.SessionID, in.InputID,
		"foreign-runner#f1", claimA.Epoch, "dequeued", nil); err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	expire(sessA.SessionID)

	// B) 'ending' under an expired foreign claim — recovery must not
	//    resurrect it; the new owner finishes the physical stop.
	paB := newPA()
	sessB := mustTerminalSession(t, s, paB, "b")
	mustClaimTerminal(t, s, paB, "foreign-runner#f2", time.Minute)
	if _, err := s.CloseTerminalSession(ctx, paB, sessB.SessionID, "close while foreign"); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := get(paB, sessB.SessionID); got.Status != "ending" || got.ClaimedBy == "" {
		t.Fatalf("setup: B = %+v, want ending with foreign claim", got)
	}
	expire(sessB.SessionID)

	// C) LIVE foreign lease — must never be surfaced or stolen.
	paC := newPA()
	sessC := mustTerminalSession(t, s, paC, "c")
	live := mustClaimTerminal(t, s, paC, "foreign-runner#f3", time.Hour)
	if live.ClaimedBy == "" {
		t.Fatal("setup: C unclaimed")
	}

	// D) backend boundary: an expired foreign claim on another
	//    backend is not this runner's to sweep or adopt.
	paD := newPA()
	sessD := mustTerminalSession(t, s, paD, "d")
	mustClaimTerminal(t, s, paD, "foreign-runner#f4", time.Minute)
	if _, err := s.pool.Exec(ctx,
		`UPDATE core_terminal_sessions SET backend = 'local' WHERE session_id = $1`,
		sessD.SessionID); err != nil {
		t.Fatalf("rebackend: %v", err)
	}
	expire(sessD.SessionID)

	// Discovery surfaces A and B purely on their expired foreign
	// claims — no new session needed. C (live lease) and D (foreign
	// backend) stay invisible to this runner.
	runnable, err := s.RunnableTerminalPersonas(ctx, "my-runner", "cloud", 64)
	if err != nil {
		t.Fatalf("runnable: %v", err)
	}
	got := map[string]bool{}
	for _, p := range runnable {
		got[p] = true
	}
	if !got[paA] || !got[paB] {
		t.Fatalf("expired foreign claims not runnable: %v", runnable)
	}
	if got[paC] {
		t.Fatal("live foreign lease surfaced for another runner")
	}
	if got[paD] {
		t.Fatal("cross-backend expired claim surfaced for cloud runner")
	}

	// The claim pass sweeps (dequeued→unknown, claims cleared) and
	// adopts: A resumes, B keeps 'ending' for physical stop.
	claimedA, _, err := s.ClaimTerminalSessions(ctx, paA, "my-runner#i1", "cloud", time.Minute, 4)
	if err != nil || len(claimedA) != 1 {
		t.Fatalf("claim A: %v %v", claimedA, err)
	}
	a := claimedA[0]
	if a.SessionID != sessA.SessionID || a.ClaimedBy != "my-runner#i1" || a.Status != "claimed" {
		t.Fatalf("A = %+v, want claimed by my-runner#i1", a)
	}
	claimedB, _, err := s.ClaimTerminalSessions(ctx, paB, "my-runner#i1", "cloud", time.Minute, 4)
	if err != nil || len(claimedB) != 1 || claimedB[0].Status != "ending" {
		t.Fatalf("B = %+v, want adopted ending for physical stop", claimedB)
	}

	// C: nothing expired — the claim pass must leave the live foreign
	//    lease entirely alone.
	claimedC, _, err := s.ClaimTerminalSessions(ctx, paC, "my-runner#i1", "cloud", time.Minute, 4)
	if err != nil {
		t.Fatalf("claim C: %v", err)
	}
	if len(claimedC) != 0 {
		t.Fatalf("live foreign claim stolen: %+v", claimedC)
	}
	if c := get(paC, sessC.SessionID); c.ClaimedBy != "foreign-runner#f3" {
		t.Fatalf("live foreign claim disturbed: %+v", c)
	}

	// The sweep converted the orphaned dequeue to 'unknown' —
	// indeterminate, never 'failed', never re-served.
	rows, err := s.ListTerminalInputs(ctx, paA, sessA.SessionID, 0, 16)
	if err != nil || len(rows) != 1 || rows[0].Status != "unknown" {
		t.Fatalf("ledger = %+v, want one unknown row", rows)
	}
	pend, err := s.PendingTerminalInputs(ctx, paA, sessA.SessionID, "my-runner#i1", a.Epoch)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	for _, p := range pend {
		if p.InputID == in.InputID {
			t.Fatal("unknown row re-served to the reclaiming runner")
		}
	}

	// D): a cloud-scoped claim sweep must not touch the local-backend
	//     session's expired foreign claim.
	if _, _, err := s.ClaimTerminalSessions(ctx, paD, "my-runner#i1", "cloud", time.Minute, 4); err != nil {
		t.Fatalf("claim D: %v", err)
	}
	if d := get(paD, sessD.SessionID); d.Status != "claimed" || d.ClaimedBy != "foreign-runner#f4" {
		t.Fatalf("cross-backend session disturbed: %+v", d)
	}
	// The local backend's own runner sweeps and reclaims it.
	claimedD, _, err := s.ClaimTerminalSessions(ctx, paD, "local-runner#l1", "local", time.Minute, 4)
	if err != nil || len(claimedD) != 1 || claimedD[0].ClaimedBy != "local-runner#l1" {
		t.Fatalf("local reclaim = %+v, %v", claimedD, err)
	}
}

// TREV2-03: declaring a default backend never declares it served —
// sessions stamped with an unserved backend refuse at admission.
func TestResidualDefaultBackendIsNotAvailability(t *testing.T) {
	s := newTerminalStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	s.SetDefaultTerminalBackend("local") // name only — no runner
	if _, err := s.CreateTerminalSession(ctx, pa, "sh", "human", "test"); !errors.Is(err, ErrTerminalBackend) {
		t.Fatalf("unserved default backend admitted a session: %v", err)
	}
	// Once a runner proves 'local' live, admission opens.
	s.SetTerminalBackendAvailable("local")
	if _, err := s.CreateTerminalSession(ctx, pa, "sh", "human", "test"); err != nil {
		t.Fatalf("served default backend refused: %v", err)
	}
}
