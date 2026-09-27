package agentstate

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// Independent-review repair regressions (real Postgres):
//   B-F1/TFIN-01  close-during-launch stays 'ending' and a stale
//                 'active' report can neither resurrect it nor lose
//                 the operation identity.
//   TFIN-02       close on interrupted/dead-claim sessions keeps a
//                 reclaimable physical stop — never a certified 'ended'.
//   B-F3          'dequeued' inputs end 'unknown', never 'failed' or
//                 'expired'; 'intended' rows end 'expired'.
//   TFIN-04       ResolveDequeuedTerminalInputs marks adopted orphans
//                 'unknown' under the live claim.
//   B-F2/TFIN-05  an output gap is consumed exactly once by the cursor
//                 on both read paths; a zero-width journal-loss marker
//                 names its boundary without inventing byte ranges.

func TestRepairCloseDuringLaunchKeepsEndingAndOperation(t *testing.T) {
	s := newTerminalStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	sess := mustTerminalSession(t, s, pa, "sh")
	claimed := mustClaimTerminal(t, s, pa, "runner-a", time.Minute)

	// The person closes while the runner's launch is still in flight.
	closed, err := s.CloseTerminalSession(ctx, pa, sess.SessionID, "closed by the person")
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	if closed.Status != "ending" {
		t.Fatalf("close during launch status = %q, want ending", closed.Status)
	}

	// The runner's delayed 'active' report must not resurrect the
	// session — but the operation identity it carries is still
	// recorded so termination targets the real op.
	reported, err := s.ReportTerminalStatus(ctx, pa, sess.SessionID, "runner-a", claimed.Epoch,
		"active", "", nil, "", "op-launch-1")
	if err != nil {
		t.Fatalf("stale active report: %v", err)
	}
	if reported.Status != "ending" {
		t.Fatalf("active report resurrected ending session: %+v", reported)
	}
	after, err := s.GetTerminalSession(ctx, pa, sess.SessionID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if after.Status != "ending" || after.OperationID != "op-launch-1" {
		t.Fatalf("session = %+v, want ending with operation_id op-launch-1", after)
	}

	// The runner observes 'ending', terminates the op, and reports the
	// real outcome — only then does the session end.
	ended, err := s.ReportTerminalStatus(ctx, pa, sess.SessionID, "runner-a", claimed.Epoch,
		"ended", "closed", nil, "", "op-launch-1")
	if err != nil || ended.Status != "ended" {
		t.Fatalf("terminal report = %+v err=%v", ended, err)
	}
}

func TestRepairCloseInterruptedKeepsReclaimableStop(t *testing.T) {
	s := newTerminalStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	sess := mustTerminalSession(t, s, pa, "sh")

	// Claim; the pump dequeues an input under the live claim, then the
	// runner dies — the delivery outcome is indeterminate (B's
	// expired-path counterexample).
	c1 := mustClaimTerminal(t, s, pa, "runner-a", 50*time.Millisecond)
	in, err := s.SubmitTerminalInput(ctx, pa, sess.SessionID, "human", "stdin",
		map[string]any{"data": "rm important\n"})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.ReportTerminalInputDisposition(ctx, pa, sess.SessionID,
		in.InputID, "runner-a", c1.Epoch, "dequeued", nil); err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	// The lease lapses and the sweep marks the session 'interrupted' —
	// its provisioner op may still be physically live.
	time.Sleep(80 * time.Millisecond)
	swept, err := s.SweepExpiredTerminalClaims(ctx, pa)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	var interrupted bool
	for _, t0 := range swept {
		if t0.SessionID == sess.SessionID && t0.Status == "interrupted" {
			interrupted = true
		}
	}
	if !interrupted {
		t.Fatalf("sweep did not interrupt session: %+v", swept)
	}

	// Close must NOT certify a stop that never ran: 'ending' with the
	// claim cleared is reclaimable — a later claimant performs the
	// physical stop.
	closed, err := s.CloseTerminalSession(ctx, pa, sess.SessionID, "closed")
	if err != nil {
		t.Fatalf("close interrupted: %v", err)
	}
	if closed.Status != "ending" {
		t.Fatalf("close on interrupted status = %q, want ending", closed.Status)
	}
	reclaimed, _, err := s.ClaimTerminalSessions(ctx, pa, "runner-b", "cloud", time.Minute, 4)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	var re *TerminalSession
	for i := range reclaimed {
		if reclaimed[i].SessionID == sess.SessionID && reclaimed[i].Status == "ending" {
			re = &reclaimed[i]
		}
	}
	if re == nil {
		t.Fatalf("ending session was not reclaimable: %+v", reclaimed)
	}

	// The reclaiming runner performs the physical stop and reports the
	// real end — only then do the input rows resolve: the dequeued
	// row is 'unknown' (maybe delivered), never 'expired'.
	ended, err := s.ReportTerminalStatus(ctx, pa, sess.SessionID, "runner-b", re.Epoch,
		"ended", "closed", nil, "", "op-stopped")
	if err != nil || ended.Status != "ended" {
		t.Fatalf("reclaimed end: %+v err=%v", ended, err)
	}
	inputs, err := s.ListTerminalInputs(ctx, pa, sess.SessionID, 0, 10)
	if err != nil || len(inputs) != 1 || inputs[0].Status != "unknown" {
		t.Fatalf("dequeued input at end = %+v err=%v, want unknown", inputs, err)
	}
}

func TestRepairEndMarksDequeuedUnknownNeverFailed(t *testing.T) {
	s := newTerminalStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	sess := mustTerminalSession(t, s, pa, "sh")
	claimed := mustClaimTerminal(t, s, pa, "runner-a", time.Minute)

	in1, err := s.SubmitTerminalInput(ctx, pa, sess.SessionID, "human", "stdin",
		map[string]any{"data": "echo one\n"})
	if err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	in2, err := s.SubmitTerminalInput(ctx, pa, sess.SessionID, "human", "stdin",
		map[string]any{"data": "echo two\n"})
	if err != nil {
		t.Fatalf("submit 2: %v", err)
	}
	// in2 is dequeued by the pump (delivery outcome unknown); in1
	// stays 'intended'.
	if _, err := s.ReportTerminalInputDisposition(ctx, pa, sess.SessionID, in2.InputID,
		"runner-a", claimed.Epoch, "dequeued", nil); err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	ended, err := s.ReportTerminalStatus(ctx, pa, sess.SessionID, "runner-a", claimed.Epoch,
		"ended", "closed", nil, "", "op-1")
	if err != nil || ended.Status != "ended" {
		t.Fatalf("end: %+v err=%v", ended, err)
	}
	rows, err := s.ListTerminalInputs(ctx, pa, sess.SessionID, 0, 16)
	if err != nil {
		t.Fatalf("list inputs: %v", err)
	}
	status := map[string]string{}
	for _, in := range rows {
		status[in.InputID] = in.Status
	}
	if status[in1.InputID] != "expired" {
		t.Fatalf("intended input status = %q, want expired", status[in1.InputID])
	}
	// 'dequeued' is indeterminate — maybe delivered — so 'unknown',
	// never a definite 'failed'/'expired' and never resent.
	if status[in2.InputID] != "unknown" {
		t.Fatalf("dequeued input status = %q, want unknown", status[in2.InputID])
	}
}

func TestRepairResolveDequeuedOnAdoption(t *testing.T) {
	s := newTerminalStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	sess := mustTerminalSession(t, s, pa, "sh")
	claimed := mustClaimTerminal(t, s, pa, "runner-a", time.Minute)

	in, err := s.SubmitTerminalInput(ctx, pa, sess.SessionID, "human", "stdin",
		map[string]any{"data": "echo hi\n"})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.ReportTerminalInputDisposition(ctx, pa, sess.SessionID, in.InputID,
		"runner-a", claimed.Epoch, "dequeued", nil); err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	// Same-epoch adoption resolves the orphan 'dequeued' row to
	// 'unknown' under the live claim — once.
	n, err := s.ResolveDequeuedTerminalInputs(ctx, pa, sess.SessionID, "runner-a", claimed.Epoch)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if n != 1 {
		t.Fatalf("resolved %d rows, want 1", n)
	}
	if n, err = s.ResolveDequeuedTerminalInputs(ctx, pa, sess.SessionID, "runner-a", claimed.Epoch); err != nil || n != 0 {
		t.Fatalf("second resolve n=%d err=%v, want 0", n, err)
	}
	rows, err := s.ListTerminalInputs(ctx, pa, sess.SessionID, 0, 16)
	if err != nil || len(rows) != 1 || rows[0].Status != "unknown" {
		t.Fatalf("ledger = %+v err=%v, want one unknown row", rows, err)
	}
	// A foreign runner/epoch cannot resolve under this claim.
	if _, err := s.ResolveDequeuedTerminalInputs(ctx, pa, sess.SessionID, "runner-b", claimed.Epoch); !errors.Is(err, ErrTerminalNotClaimed) {
		t.Fatalf("foreign resolve err = %v, want ErrTerminalNotClaimed", err)
	}
}

func TestRepairGapConsumedOnceOnBothReadPaths(t *testing.T) {
	s := newTerminalStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	sess := mustTerminalSession(t, s, pa, "sh")
	claimed := mustClaimTerminal(t, s, pa, "runner-a", time.Minute)

	gapTo := int64(200)
	markerAt := int64(300)
	chunks := []TerminalOutputChunk{
		{Kind: "gap", Base: 100, GapTo: &gapTo}, // nonzero compaction gap
		{Kind: "data", Base: 200, Data: []byte("after-gap ")},
		{Kind: "gap", Base: 300, GapTo: &markerAt}, // zero-width loss boundary
		{Kind: "data", Base: 300, Data: []byte("tail")},
	}
	if _, err := s.AppendTerminalOutput(ctx, pa, sess.SessionID, "runner-a", claimed.Epoch, chunks); err != nil {
		t.Fatalf("append: %v", err)
	}

	// REST path — a reader behind the gap sees the marker once, then
	// the cursor moves past it and the gap is never re-served.
	r1, err := s.ReadTerminalOutput(ctx, pa, sess.SessionID, 0, 0, 32)
	if err != nil {
		t.Fatalf("read 1: %v", err)
	}
	if len(r1.Chunks) != 4 || r1.Chunks[0].Kind != "gap" || *r1.Chunks[0].GapTo != 200 {
		t.Fatalf("read 1 chunks = %+v", r1.Chunks)
	}
	if r1.NextCursor != 304 {
		t.Fatalf("read 1 next = %d, want 304", r1.NextCursor)
	}
	// Inside the gap: cursor 150 sees the gap row (its end is ahead).
	rIn, err := s.ReadTerminalOutput(ctx, pa, sess.SessionID, 150, 0, 32)
	if err != nil || len(rIn.Chunks) != 4 || rIn.Chunks[0].Kind != "gap" {
		t.Fatalf("inside-gap read = %+v err=%v", rIn.Chunks, err)
	}
	// At/past the gap end: the spent gap row is never re-served.
	rAt, err := s.ReadTerminalOutput(ctx, pa, sess.SessionID, 200, 0, 32)
	if err != nil {
		t.Fatalf("at-gap read: %v", err)
	}
	for _, c := range rAt.Chunks {
		if c.Kind == "gap" && c.Base == 100 {
			t.Fatalf("spent gap re-served at cursor 200: %+v", rAt.Chunks)
		}
	}
	rPast, err := s.ReadTerminalOutput(ctx, pa, sess.SessionID, r1.NextCursor, r1.EventCursor, 32)
	if err != nil || len(rPast.Chunks) != 0 {
		t.Fatalf("past read = %+v err=%v, want empty", rPast.Chunks, err)
	}

	// Secretary path — the same cursor semantics through
	// terminal.read: marker text names the boundary honestly and the
	// gap is consumed once.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := s.internalTerminalTool(ctx, tx, pa, "terminal.read", map[string]any{
		"session_id": sess.SessionID, "cursor": float64(0),
	})
	_ = tx.Rollback(ctx)
	if err != nil {
		t.Fatalf("terminal.read: %v", err)
	}
	content, _ := resp["content"].(string)
	if !strings.Contains(content, "bytes 100–200") {
		t.Fatalf("tool content missing nonzero gap marker: %q", content)
	}
	if !strings.Contains(content, "output may be missing at byte 300") {
		t.Fatalf("tool content missing journal-loss marker: %q", content)
	}
	if !strings.Contains(content, "after-gap ") || !strings.HasSuffix(content, "tail") {
		t.Fatalf("tool content missing post-gap data: %q", content)
	}
	if nc, _ := resp["next_cursor"].(int64); nc != 304 {
		t.Fatalf("tool next_cursor = %v, want 304", resp["next_cursor"])
	}
	ec, _ := resp["event_cursor"].(int64)
	if ec == 0 {
		t.Fatal("tool did not return event_cursor")
	}

	tx2, _ := s.pool.Begin(ctx)
	resp2, err := s.internalTerminalTool(ctx, tx2, pa, "terminal.read", map[string]any{
		"session_id": sess.SessionID, "cursor": float64(304), "event_cursor": float64(ec),
	})
	_ = tx2.Rollback(ctx)
	if err != nil {
		t.Fatalf("terminal.read past: %v", err)
	}
	if c2, _ := resp2["content"].(string); strings.Contains(c2, "lost") || strings.Contains(c2, "missing") {
		t.Fatalf("tool re-served a consumed gap: %q", c2)
	}
}

func TestRepairRunnerLockFencesSecondDriver(t *testing.T) {
	s := newTerminalStore(t)
	ctx := context.Background()

	// One live driver per runner identity: the second acquire fails
	// fast while the first holds the lock, and takes over once the
	// first releases (the restart/deploy overlap contract).
	lock, err := s.TryAcquireTerminalRunnerLock(ctx, "runner-a")
	if err != nil || lock == nil {
		t.Fatalf("first acquire lock=%v err=%v, want held", lock, err)
	}
	second, err := s.TryAcquireTerminalRunnerLock(ctx, "runner-a")
	if err != nil {
		t.Fatalf("second acquire err: %v", err)
	}
	if second != nil {
		t.Fatal("second driver acquired the runner lock concurrently")
	}
	lock.Release(ctx)
	third, err := s.TryAcquireTerminalRunnerLock(ctx, "runner-a")
	if err != nil || third == nil {
		t.Fatalf("post-release acquire lock=%v err=%v, want held", third, err)
	}
	defer third.Release(ctx)
	if !third.Ping(ctx) {
		t.Fatal("held lock failed liveness ping")
	}
}
