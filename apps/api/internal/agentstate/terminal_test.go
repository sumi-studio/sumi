package agentstate

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Terminal store behavior on real Postgres: claim-epoch fencing, the
// durable input ledger, ending-survives-sweep, control holds,
// capacity, and scrollback gaps. These are the state-machine
// guarantees the runner and browser layers rely on.

func newTerminalStore(t *testing.T) *Store {
	t.Helper()
	s, _ := newStore(t)
	s.SetDefaultTerminalBackend("cloud")
	s.SetTerminalBackendAvailable("cloud")
	return s
}

func mustTerminalSession(t *testing.T, s *Store, pa, name string) TerminalSession {
	t.Helper()
	sess, err := s.CreateTerminalSession(context.Background(), pa, name, "human", "test")
	if err != nil {
		t.Fatalf("create terminal session: %v", err)
	}
	return sess
}

func mustClaimTerminal(t *testing.T, s *Store, pa, runner string, lease time.Duration) TerminalSession {
	t.Helper()
	claimed, _, err := s.ClaimTerminalSessions(context.Background(), pa, runner, "cloud", lease, 1)
	if err != nil {
		t.Fatalf("claim terminal: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d sessions, want 1", len(claimed))
	}
	return claimed[0]
}

func TestTerminalCreateRequiresLiveBackend(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	// No backend declared: the request is refused honestly rather
	// than queueing a session no runner will ever claim.
	if _, err := s.CreateTerminalSession(ctx, pa, "sh", "human", "test"); !errors.Is(err, ErrTerminalBackend) {
		t.Fatalf("create without backend err = %v, want ErrTerminalBackend", err)
	}
	s.SetTerminalBackendAvailable("cloud")
	s.SetDefaultTerminalBackend("cloud")
	if _, err := s.CreateTerminalSession(ctx, pa, "sh", "human", "test"); err != nil {
		t.Fatalf("create with backend: %v", err)
	}
	// Wrong persona cannot even see the session exists.
	other := pid(t)
	mustPersona(t, s, other)
	if _, err := s.GetTerminalSession(ctx, other, "01900000-0000-7000-8000-000000000000"); !errors.Is(err, ErrTerminalNotFound) {
		t.Fatalf("foreign get err = %v, want ErrTerminalNotFound", err)
	}
}

func TestTerminalClaimEpochFencing(t *testing.T) {
	s := newTerminalStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	sess := mustTerminalSession(t, s, pa, "sh")

	claimed := mustClaimTerminal(t, s, pa, "runner-a", time.Minute)
	if claimed.Status != "claimed" || claimed.Epoch != sess.Epoch+1 || claimed.ClaimedBy != "runner-a" {
		t.Fatalf("claim = %+v", claimed)
	}
	if _, err := s.HeartbeatTerminalSession(ctx, pa, sess.SessionID, "runner-a", claimed.Epoch, time.Minute); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	// Stale epoch and foreign runner are both fenced.
	if _, err := s.HeartbeatTerminalSession(ctx, pa, sess.SessionID, "runner-a", claimed.Epoch-1, time.Minute); !errors.Is(err, ErrTerminalNotClaimed) {
		t.Fatalf("stale epoch heartbeat err = %v", err)
	}
	if _, err := s.HeartbeatTerminalSession(ctx, pa, sess.SessionID, "runner-b", claimed.Epoch, time.Minute); !errors.Is(err, ErrTerminalNotClaimed) {
		t.Fatalf("foreign runner heartbeat err = %v", err)
	}
	in, err := s.SubmitTerminalInput(ctx, pa, sess.SessionID, "agent", "stdin", map[string]any{"data": "ls\n"})
	if err != nil {
		t.Fatalf("submit input: %v", err)
	}
	if _, err := s.ReportTerminalInputDisposition(ctx, pa, sess.SessionID, in.InputID, "runner-b", claimed.Epoch, "written", nil); !errors.Is(err, ErrTerminalNotClaimed) {
		t.Fatalf("foreign disposition err = %v", err)
	}
	if _, err := s.ReportTerminalStatus(ctx, pa, sess.SessionID, "runner-b", claimed.Epoch, "ended", "x", nil, "", "op"); !errors.Is(err, ErrTerminalNotClaimed) {
		t.Fatalf("foreign status report err = %v", err)
	}
}

func TestTerminalInputLedgerEpochFencing(t *testing.T) {
	s := newTerminalStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	sess := mustTerminalSession(t, s, pa, "sh")

	// Input queues while 'requested' — the ledger is durable; the
	// first claim restamps it to the claim epoch and delivers.
	early, err := s.SubmitTerminalInput(ctx, pa, sess.SessionID, "human", "stdin", map[string]any{"data": "echo early\n"})
	if err != nil {
		t.Fatalf("input on requested session: %v", err)
	}
	if early.Status != "intended" {
		t.Fatalf("early input status = %q, want intended", early.Status)
	}

	c1 := mustClaimTerminal(t, s, pa, "runner-a", 60*time.Millisecond)
	pending, err := s.PendingTerminalInputs(ctx, pa, sess.SessionID, "runner-a", c1.Epoch)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 1 || pending[0].SessionEpoch != c1.Epoch {
		t.Fatalf("pending after claim = %+v, want early input restamped to epoch %d", pending, c1.Epoch)
	}
	// Dequeue one input under epoch 1 — it may have been delivered.
	late, err := s.SubmitTerminalInput(ctx, pa, sess.SessionID, "agent", "stdin", map[string]any{"data": "echo late\n"})
	if err != nil {
		t.Fatalf("second input: %v", err)
	}
	if _, err := s.ReportTerminalInputDisposition(ctx, pa, sess.SessionID, late.InputID, "runner-a", c1.Epoch, "dequeued", nil); err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	// Claim dies: sweep → interrupted. The 'dequeued' row turns
	// 'unknown' (never resent); a still-'intended' row would restamp.
	time.Sleep(80 * time.Millisecond)
	swept, err := s.SweepExpiredTerminalClaims(ctx, pa)
	if err != nil || len(swept) != 1 || swept[0].Status != "interrupted" {
		t.Fatalf("sweep = %+v err=%v, want one interrupted", swept, err)
	}
	c2 := mustClaimTerminal(t, s, pa, "runner-b", time.Minute)
	if c2.Epoch <= c1.Epoch {
		t.Fatalf("reclaim epoch %d not after %d", c2.Epoch, c1.Epoch)
	}
	pending2, err := s.PendingTerminalInputs(ctx, pa, sess.SessionID, "runner-b", c2.Epoch)
	if err != nil {
		t.Fatalf("pending2: %v", err)
	}
	for _, in := range pending2 {
		if in.InputID == late.InputID {
			t.Fatalf("dequeued input %s re-served to new claimant — possible duplicate delivery", in.InputID)
		}
	}
	// The old epoch is fenced: runner-a can no longer disposition.
	if _, err := s.ReportTerminalInputDisposition(ctx, pa, sess.SessionID, late.InputID, "runner-a", c1.Epoch, "written", nil); !errors.Is(err, ErrTerminalNotClaimed) {
		t.Fatalf("stale disposition err = %v, want ErrTerminalNotClaimed", err)
	}
}

// TAPI-04: the input ledger's written/failed/unknown outcomes are
// readable by both clients — ListTerminalInputs backs the person's
// GET /terminal/inputs route, and 'terminal.inputs' is the secretary's
// internal tool. Neither resends or mutates: reads are pure observation.
func TestTerminalInputOutcomeLedgerReads(t *testing.T) {
	s, pool := newStore(t)
	s.SetDefaultTerminalBackend("cloud")
	s.SetTerminalBackendAvailable("cloud")
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	sess := mustTerminalSession(t, s, pa, "sh")

	failed, err := s.SubmitTerminalInput(ctx, pa, sess.SessionID, "agent", "signal", map[string]any{"signal": "INT"})
	if err != nil {
		t.Fatalf("submit signal: %v", err)
	}
	uncertain, err := s.SubmitTerminalInput(ctx, pa, sess.SessionID, "human", "stdin", map[string]any{"data": "rm -rf build\n"})
	if err != nil {
		t.Fatalf("submit stdin: %v", err)
	}

	c1 := mustClaimTerminal(t, s, pa, "runner-a", 60*time.Millisecond)
	// A definite physical failure is reported as such.
	if _, err := s.ReportTerminalInputDisposition(ctx, pa, sess.SessionID, failed.InputID, "runner-a", c1.Epoch,
		"failed", map[string]any{"reason": "no foreground process group"}); err != nil {
		t.Fatalf("failed disposition: %v", err)
	}
	// A dequeued-but-undelivered input becomes 'unknown' on reclaim.
	if _, err := s.ReportTerminalInputDisposition(ctx, pa, sess.SessionID, uncertain.InputID, "runner-a", c1.Epoch, "dequeued", nil); err != nil {
		t.Fatalf("dequeue disposition: %v", err)
	}
	time.Sleep(80 * time.Millisecond)
	if _, err := s.SweepExpiredTerminalClaims(ctx, pa); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	c2 := mustClaimTerminal(t, s, pa, "runner-b", time.Minute)

	// Person's read path: ListTerminalInputs.
	rows, err := s.ListTerminalInputs(ctx, pa, sess.SessionID, 0, 0)
	if err != nil {
		t.Fatalf("list inputs: %v", err)
	}
	if len(rows) != 2 || rows[0].Status != "failed" || rows[1].Status != "unknown" {
		t.Fatalf("ledger rows = %+v, want [failed unknown]", rows)
	}
	if rows[0].InputID != failed.InputID || rows[1].InputID != uncertain.InputID {
		t.Fatalf("ledger order = %+v", rows)
	}
	if rows[0].Detail["reason"] != "no foreground process group" {
		t.Fatalf("failure detail lost: %v", rows[0].Detail)
	}
	// after_seq is an incremental cursor.
	rows, err = s.ListTerminalInputs(ctx, pa, sess.SessionID, rows[0].Seq, 0)
	if err != nil || len(rows) != 1 || rows[0].InputID != uncertain.InputID {
		t.Fatalf("after_seq read = %+v err=%v", rows, err)
	}
	// 'unknown' is never re-served to the new claimant.
	pending, err := s.PendingTerminalInputs(ctx, pa, sess.SessionID, "runner-b", c2.Epoch)
	if err != nil {
		t.Fatalf("pending after reclaim: %v", err)
	}
	for _, in := range pending {
		if in.InputID == uncertain.InputID {
			t.Fatalf("unknown input re-queued: %+v", in)
		}
	}

	// Secretary's read path: the 'terminal.inputs' internal tool.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := s.internalTerminalTool(ctx, tx, pa, "terminal.inputs", map[string]any{
		"session_id": sess.SessionID, "after_seq": float64(1),
	})
	_ = tx.Rollback(ctx)
	if err != nil {
		t.Fatalf("terminal.inputs tool: %v", err)
	}
	toolRows, _ := resp["inputs"].([]TerminalInput)
	if len(toolRows) != 1 || toolRows[0].Status != "unknown" {
		t.Fatalf("tool inputs = %+v, want the unknown row", toolRows)
	}
	if _, ok := resp["session"].(TerminalSession); !ok {
		t.Fatalf("tool response missing session: %v", resp)
	}
	// The tool refuses a session the persona does not own.
	other := pid(t)
	mustPersona(t, s, other)
	tx2, _ := pool.Begin(ctx)
	_, err = s.internalTerminalTool(ctx, tx2, other, "terminal.inputs", map[string]any{"session_id": sess.SessionID})
	_ = tx2.Rollback(ctx)
	if err == nil {
		t.Fatal("foreign persona read another persona's input ledger")
	}
}

func TestTerminalEndingSurvivesSweep(t *testing.T) {
	s := newTerminalStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	sess := mustTerminalSession(t, s, pa, "sh")

	mustClaimTerminal(t, s, pa, "runner-a", 60*time.Millisecond)
	closed, err := s.CloseTerminalSession(ctx, pa, sess.SessionID, "closed by the person")
	if err != nil || closed.Status != "ending" {
		t.Fatalf("close = %+v err=%v, want ending", closed, err)
	}
	// The claim dies mid-close. The close intent must survive:
	// sweeping to 'interrupted' would resurrect the session on
	// reclaim and the container would run forever.
	time.Sleep(80 * time.Millisecond)
	swept, err := s.SweepExpiredTerminalClaims(ctx, pa)
	if err != nil || len(swept) != 1 {
		t.Fatalf("sweep = %+v err=%v", swept, err)
	}
	if swept[0].Status != "ending" || swept[0].ClaimedBy != "" {
		t.Fatalf("ending sweep = %+v, want status ending with claim cleared", swept[0])
	}
	// Discovery still finds the unclaimed ending session.
	personas, err := s.RunnableTerminalPersonas(ctx, "runner-b", "cloud", 10)
	if err != nil || len(personas) != 1 || personas[0] != pa {
		t.Fatalf("runnable personas = %v err=%v", personas, err)
	}
	// Reclaim keeps 'ending'; the runner goes straight to the stop.
	c2 := mustClaimTerminal(t, s, pa, "runner-b", time.Minute)
	if c2.Status != "ending" {
		t.Fatalf("reclaimed status = %q, want ending", c2.Status)
	}
	// Input refuses while ending — nothing will deliver it.
	if _, err := s.SubmitTerminalInput(ctx, pa, sess.SessionID, "human", "stdin", map[string]any{"data": "x\n"}); !errors.Is(err, ErrTerminalNotLive) {
		t.Fatalf("input on ending err = %v, want ErrTerminalNotLive", err)
	}
	// The reclaiming runner reports the terminal outcome.
	code := 0
	done, err := s.ReportTerminalStatus(ctx, pa, sess.SessionID, "runner-b", c2.Epoch, "ended", "closed", &code, "", "op-1")
	if err != nil || done.Status != "ended" {
		t.Fatalf("report ended = %+v err=%v", done, err)
	}
	if _, err := s.SubmitTerminalInput(ctx, pa, sess.SessionID, "human", "stdin", map[string]any{"data": "x\n"}); !errors.Is(err, ErrTerminalEnded) {
		t.Fatalf("input on ended err = %v, want ErrTerminalEnded", err)
	}
}

func TestTerminalControlHolds(t *testing.T) {
	s := newTerminalStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	sess := mustTerminalSession(t, s, pa, "sh")

	if _, err := s.SetTerminalControl(ctx, pa, sess.SessionID, true, time.Minute); err != nil {
		t.Fatalf("hold control: %v", err)
	}
	// Agent input is refused at admission — not queued and dropped.
	if _, err := s.SubmitTerminalInput(ctx, pa, sess.SessionID, "agent", "stdin", map[string]any{"data": "x\n"}); !errors.Is(err, ErrTerminalControl) {
		t.Fatalf("agent input under hold err = %v, want ErrTerminalControl", err)
	}
	// Human input is unaffected.
	if _, err := s.SubmitTerminalInput(ctx, pa, sess.SessionID, "human", "stdin", map[string]any{"data": "x\n"}); err != nil {
		t.Fatalf("human input under hold: %v", err)
	}
	if _, err := s.SetTerminalControl(ctx, pa, sess.SessionID, false, 0); err != nil {
		t.Fatalf("release control: %v", err)
	}
	if _, err := s.SubmitTerminalInput(ctx, pa, sess.SessionID, "agent", "stdin", map[string]any{"data": "y\n"}); err != nil {
		t.Fatalf("agent input after release: %v", err)
	}
}

func TestTerminalCapacityAndEndedRefusals(t *testing.T) {
	s := newTerminalStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	var first TerminalSession
	for i := 0; i < terminalMaxSessions; i++ {
		sess := mustTerminalSession(t, s, pa, "sh")
		if i == 0 {
			first = sess
		}
	}
	if _, err := s.CreateTerminalSession(ctx, pa, "sh", "human", "test"); !errors.Is(err, ErrTerminalCapacity) {
		t.Fatalf("capacity err = %v, want ErrTerminalCapacity", err)
	}
	// An unclaimed close is a definite end — nothing runs to stop.
	ended, err := s.CloseTerminalSession(ctx, pa, first.SessionID, "closed")
	if err != nil || ended.Status != "ended" {
		t.Fatalf("close unclaimed = %+v err=%v, want ended", ended, err)
	}
	if _, err := s.SubmitTerminalInput(ctx, pa, first.SessionID, "human", "stdin", map[string]any{"data": "x\n"}); !errors.Is(err, ErrTerminalEnded) {
		t.Fatalf("input on ended err = %v", err)
	}
	if _, err := s.SetTerminalControl(ctx, pa, first.SessionID, true, time.Minute); !errors.Is(err, ErrTerminalEnded) {
		t.Fatalf("control on ended err = %v", err)
	}
	// The end notification is enqueued exactly once — a secretary
	// that was not watching still learns the outcome.
	var kind string
	err = s.pool.QueryRow(ctx,
		`SELECT kind FROM core_inputs WHERE persona_id = $1::uuidv7 AND input_id = $2`,
		pa, "terminal:"+first.SessionID).Scan(&kind)
	if err != nil || kind != "terminal_ended" {
		t.Fatalf("terminal_ended notification kind=%q err=%v", kind, err)
	}
	// A second end attempt must not duplicate the notification.
	if _, err := s.CloseTerminalSession(ctx, pa, first.SessionID, "again"); err != nil {
		t.Fatalf("re-close ended: %v", err)
	}
	var n int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM core_inputs WHERE persona_id = $1::uuidv7 AND input_id = $2`,
		pa, "terminal:"+first.SessionID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("notification count = %d err=%v, want exactly 1", n, err)
	}
}

func TestTerminalOutputAppendDedupAndGap(t *testing.T) {
	s := newTerminalStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	sess := mustTerminalSession(t, s, pa, "sh")
	c1 := mustClaimTerminal(t, s, pa, "runner-a", time.Minute)

	gapTo := int64(100)
	chunks := []TerminalOutputChunk{
		{Kind: "gap", Base: 0, GapTo: &gapTo},
		{Kind: "data", Base: 100, Data: []byte("hello")},
	}
	if _, err := s.AppendTerminalOutput(ctx, pa, sess.SessionID, "runner-a", c1.Epoch, chunks); err != nil {
		t.Fatalf("append output: %v", err)
	}
	// Replayed drain dedupes on (session_id, base, kind) — a crash
	// between drain and commit can never double-append.
	if _, err := s.AppendTerminalOutput(ctx, pa, sess.SessionID, "runner-a", c1.Epoch, chunks); err != nil {
		t.Fatalf("replay append: %v", err)
	}
	var n int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM core_terminal_output WHERE session_id = $1::uuidv7`,
		sess.SessionID).Scan(&n); err != nil || n != 2 {
		t.Fatalf("output rows = %d err=%v, want 2 (deduped)", n, err)
	}
	read, err := s.ReadTerminalOutput(ctx, pa, sess.SessionID, 0, 0, 32)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if len(read.Chunks) != 2 || read.Chunks[0].Kind != "gap" || *read.Chunks[0].GapTo != 100 {
		t.Fatalf("read chunks = %+v", read.Chunks)
	}
	// A stale runner cannot append under the old fencing.
	if _, err := s.AppendTerminalOutput(ctx, pa, sess.SessionID, "runner-b", c1.Epoch, chunks); !errors.Is(err, ErrTerminalNotClaimed) {
		t.Fatalf("foreign append err = %v, want ErrTerminalNotClaimed", err)
	}
	// Lost verdict publishes honestly and fences the row.
	lost, err := s.ReportTerminalStatus(ctx, pa, sess.SessionID, "runner-a", c1.Epoch, "lost", "container outcome indeterminate", nil, "", "op-1")
	if err != nil || lost.Status != "lost" {
		t.Fatalf("lost report = %+v err=%v", lost, err)
	}
}

// The launch fence serializes a delayed terminal StartProcess against
// the return seal's persona lock: an admitted start that arrives while
// the seal holds FOR NO KEY UPDATE waits, then observes the committed
// non-active authority and is fenced — it never reaches the runtime.
func TestTerminalLaunchFenceBlocksBehindSeal(t *testing.T) {
	s, pool := newStore(t)
	s.SetDefaultTerminalBackend("cloud")
	s.SetTerminalBackendAvailable("cloud")
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)

	// Active persona: the fence runs the launch body.
	ran := false
	if err := s.WithTerminalLaunchFence(ctx, pa, func(context.Context) error {
		ran = true
		return nil
	}); err != nil || !ran {
		t.Fatalf("active fence ran=%v err=%v", ran, err)
	}
	// fn errors propagate and the lock releases.
	want := errors.New("boom")
	if err := s.WithTerminalLaunchFence(ctx, pa, func(context.Context) error { return want }); !errors.Is(err, want) {
		t.Fatalf("fn error = %v", err)
	}

	// Stand in for the seal transaction: persona row FOR NO KEY UPDATE.
	lockTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lockTx.Rollback(ctx)
	if _, err := lockTx.Exec(ctx,
		`SELECT 1 FROM core_personas WHERE persona_id = $1 FOR NO KEY UPDATE`, pa); err != nil {
		t.Fatalf("persona lock: %v", err)
	}

	type result struct {
		ran bool
		err error
	}
	done := make(chan result, 1)
	go func() {
		ran := false
		err := s.WithTerminalLaunchFence(ctx, pa, func(context.Context) error {
			ran = true
			return nil
		})
		done <- result{ran, err}
	}()

	// The fence must wait on the lock, not read past it.
	select {
	case r := <-done:
		t.Fatalf("launch fence completed while the seal lock was held: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}

	if _, err := lockTx.Exec(ctx,
		`UPDATE core_personas SET authority = 'sealed' WHERE persona_id = $1`, pa); err != nil {
		t.Fatalf("seal update: %v", err)
	}
	if err := lockTx.Commit(ctx); err != nil {
		t.Fatalf("seal commit: %v", err)
	}

	select {
	case r := <-done:
		if !errors.Is(r.err, ErrTerminalLaunchFenced) {
			t.Fatalf("fence behind committed seal = %v, want ErrTerminalLaunchFenced", r.err)
		}
		if r.ran {
			t.Fatal("launch body ran on a sealed persona")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("launch fence never unblocked after the seal committed")
	}

	// A cancelled move restores 'active' and the fence passes again —
	// queued sessions stay launchable.
	if _, err := pool.Exec(ctx,
		`UPDATE core_personas SET authority = 'active' WHERE persona_id = $1`, pa); err != nil {
		t.Fatal(err)
	}
	ran = false
	if err := s.WithTerminalLaunchFence(ctx, pa, func(context.Context) error {
		ran = true
		return nil
	}); err != nil || !ran {
		t.Fatalf("post-abort fence ran=%v err=%v", ran, err)
	}
}
