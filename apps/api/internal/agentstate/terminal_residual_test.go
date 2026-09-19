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
