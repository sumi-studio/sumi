//go:build integration

// Real-Postgres driver repair tests (independent review findings):
//
//	B-F4  two driver processes sharing a stable RunnerID must never
//	      drive the same epoch concurrently: the runner advisory lock
//	      makes the second fail visibly and fast; a restart safely
//	      takes over and input is delivered exactly once; a killed
//	      lock connection makes the stale driver stop (watchdog) so
//	      takeover is safe.
//	TFIN-08 an adopted 'ending' session with no journaled op is
//	      tombstone-cancelled — never launched just to be killed.
package termexec

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

func newStore(t *testing.T) *agentstate.Store {
	t.Helper()
	pool := testdb.Create(t)
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	s := agentstate.NewStore(pool)
	s.SetDefaultTerminalBackend("cloud")
	return s
}

func pid(t *testing.T) string {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid: %v", err)
	}
	return id.String()
}

// fakeProc scripts ProcessAPI. It records launches, cancels (including
// the tombstone flag) and stdin deliveries so tests can prove "never
// launched" and "delivered exactly once" across driver takeovers.
type fakeProc struct {
	mu          sync.Mutex
	ops         map[string]*runtimeprovision.ProcessOperation
	startCalls  int
	cancelCalls []runtimeprovision.ProcessLookupRequest
	writes      int
	notFound    bool // ProcessStatus answers ErrProcessNotFound
	// writeGate, when non-nil, blocks WriteProcessInput after the
	// write is counted — the deterministic in-flight delivery window
	// for the lock-loss interleaving tests.
	writeGate chan struct{}
	// readFunc scripts interactive output reads; nil returns EOF.
	readFunc func(offset int64) runtimeprovision.ProcessOutput
}

func (f *fakeProc) op(req runtimeprovision.ProcessLookupRequest) (*runtimeprovision.ProcessOperation, bool) {
	o, ok := f.ops[req.OperationID]
	return o, ok
}

func (f *fakeProc) StartProcess(_ context.Context, r runtimeprovision.ProcessStartRequest) (runtimeprovision.ProcessOperation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.startCalls++
	opID := runtimeprovision.ProcessOperationID(r.PersonalityAgentID, r.OriginatingToolCallID)
	if o, ok := f.ops[opID]; ok {
		return *o, nil // idempotent re-attach, like the real service
	}
	o := &runtimeprovision.ProcessOperation{
		OperationID: opID, PersonalityAgentID: r.PersonalityAgentID,
		OriginatingToolCallID: r.OriginatingToolCallID, State: runtimeprovision.ProcessRunning,
	}
	f.ops[opID] = o
	return *o, nil
}

func (f *fakeProc) ProcessStatus(_ context.Context, r runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.notFound {
		return runtimeprovision.ProcessOperation{}, runtimeprovision.ErrProcessNotFound
	}
	if o, ok := f.op(r); ok {
		return *o, nil
	}
	return runtimeprovision.ProcessOperation{}, runtimeprovision.ErrProcessNotFound
}

func (f *fakeProc) ReadProcessOutput(_ context.Context, r runtimeprovision.ProcessOutputRequest) (runtimeprovision.ProcessOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.readFunc != nil {
		out := f.readFunc(r.Offset)
		out.OperationID = r.OperationID
		return out, nil
	}
	return runtimeprovision.ProcessOutput{OperationID: r.OperationID, Offset: r.Offset, NextOffset: r.Offset, EOF: true}, nil
}

func (f *fakeProc) CancelProcess(_ context.Context, r runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelCalls = append(f.cancelCalls, r)
	if o, ok := f.op(r); ok {
		o.State = runtimeprovision.ProcessCancelled
		o.Quiesced = true
		return *o, nil
	}
	tomb := &runtimeprovision.ProcessOperation{
		OperationID: r.OperationID, PersonalityAgentID: r.PersonalityAgentID,
		State: runtimeprovision.ProcessCancelled, Tombstone: true, Quiesced: true,
	}
	f.ops[r.OperationID] = tomb
	return *tomb, nil
}

func (f *fakeProc) WriteProcessInput(_ context.Context, r runtimeprovision.ProcessInputRequest) (runtimeprovision.ProcessInputReceipt, error) {
	f.mu.Lock()
	f.writes++
	gate := f.writeGate
	f.mu.Unlock()
	if gate != nil {
		<-gate
	}
	return runtimeprovision.ProcessInputReceipt{Delivered: true}, nil
}

func (f *fakeProc) ResizeProcess(_ context.Context, r runtimeprovision.ProcessResizeRequest) (runtimeprovision.ProcessOperation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if o, ok := f.op(r.ProcessLookupRequest); ok {
		return *o, nil
	}
	return runtimeprovision.ProcessOperation{}, runtimeprovision.ErrProcessNotFound
}

func (f *fakeProc) SignalProcess(_ context.Context, r runtimeprovision.ProcessSignalRequest) (runtimeprovision.ProcessInputReceipt, error) {
	return runtimeprovision.ProcessInputReceipt{Delivered: true}, nil
}

func testConfig(logs *sync.Map, key string) Config {
	return Config{
		RunnerID: "runner-x", Backend: "cloud",
		Lease: 5 * time.Second, Interval: 60 * time.Millisecond,
		PollInterval: 40 * time.Millisecond, HeartbeatEvery: 2,
		UnknownWait: 2 * time.Second, CallTimeout: 2 * time.Second,
		Logf: func(format string, args ...any) {
			logs.Store(key+time.Now().String(), fmt.Sprintf(format, args...))
		},
	}
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func sessionStatus(t *testing.T, s *agentstate.Store, pa, sid string) string {
	t.Helper()
	sess, err := s.GetTerminalSession(context.Background(), pa, sid)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	return sess.Status
}

// B-F4: a second driver process with the same stable RunnerID never
// double-pumps — it stands by (visibly) instead of driving, and takes
// over when the first exits. Input is delivered exactly once.
func TestDriverSameRunnerIDConcurrencyFence(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	if _, _, err := s.EnsurePersona(ctx, pa, nil, ""); err != nil {
		t.Fatalf("persona: %v", err)
	}
	sess, err := s.CreateTerminalSession(ctx, pa, "sh", "human", "test")
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	claimed, _, err := s.ClaimTerminalSessions(ctx, pa, "runner-x", "cloud", time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v", err)
	}

	var logs sync.Map
	cfg := testConfig(&logs, "d1:")
	fake1 := &fakeProc{ops: map[string]*runtimeprovision.ProcessOperation{}}
	d1 := New(s, fake1, nil, cfg)
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan struct{})
	go func() { d1.Run(ctx1); close(done1) }()

	// Driver 1 adopts the claimed session and launches first — only
	// then is the lock-vs-runner ordering deterministic.
	waitFor(t, 5*time.Second, "d1 launch", func() bool {
		fake1.mu.Lock()
		defer fake1.mu.Unlock()
		return fake1.startCalls == 1
	})

	// Driver 2 — a second process with the SAME runner identity —
	// must not drive concurrently: it stands by for ownership, and
	// logging once then silently going driverless is not acceptable.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	fake2 := &fakeProc{ops: map[string]*runtimeprovision.ProcessOperation{}}
	d2 := New(s, fake2, nil, testConfig(&logs, "d2:"))
	d2done := make(chan struct{})
	go func() { d2.Run(ctx2); close(d2done) }()
	defer func() { cancel2(); <-d2done }()
	time.Sleep(400 * time.Millisecond)
	var standby bool
	logs.Range(func(_, v any) bool {
		if strings.Contains(v.(string), "standing by for ownership") {
			standby = true
		}
		return !standby
	})
	if !standby {
		t.Fatal("second driver produced no visible standby report")
	}
	select {
	case <-d2done:
		t.Fatal("second driver exited instead of standing by for ownership")
	default:
	}
	if fake2.startCalls != 0 {
		t.Fatalf("second driver launched %d ops while locked out", fake2.startCalls)
	}

	waitFor(t, 5*time.Second, "session active", func() bool {
		return sessionStatus(t, s, pa, sess.SessionID) == "active"
	})

	// One stdin input is delivered exactly once.
	if _, err := s.SubmitTerminalInput(ctx, pa, sess.SessionID, "human", "stdin",
		map[string]any{"data": "echo hi\n"}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	waitFor(t, 5*time.Second, "d1 delivery", func() bool {
		fake1.mu.Lock()
		defer fake1.mu.Unlock()
		return fake1.writes == 1
	})
	waitFor(t, 5*time.Second, "input written", func() bool {
		rows, err := s.ListTerminalInputs(ctx, pa, sess.SessionID, 0, 8)
		return err == nil && len(rows) == 1 && rows[0].Status == "written"
	})

	// Restart/deploy overlap: d1 exits cleanly, the standing-by d2
	// acquires ownership and adopts — under a NEW incarnation, so the
	// written input is never re-delivered.
	cancel1()
	select {
	case <-done1:
	case <-time.After(5 * time.Second):
		t.Fatal("d1 did not stop")
	}
	waitFor(t, 8*time.Second, "d2 takeover", func() bool {
		fake2.mu.Lock()
		defer fake2.mu.Unlock()
		return fake2.startCalls == 1 // re-attach of the same op
	})
	// Settle past a few pump ticks, then prove no second delivery.
	time.Sleep(300 * time.Millisecond)
	fake2.mu.Lock()
	w2 := fake2.writes
	fake2.mu.Unlock()
	if w2 != 0 {
		t.Fatalf("takeover driver re-delivered stdin %d times", w2)
	}
}

// B-F4 lock-loss half: when the lock connection dies under a live
// driver, the watchdog stops the drive term (pumps unwind, sweeping
// halts), then the same driver re-acquires and resumes under a NEW
// incarnation — recovery without a peer, never silent abandonment.
func TestDriverLockConnectionLossStopsPumps(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	if _, _, err := s.EnsurePersona(ctx, pa, nil, ""); err != nil {
		t.Fatalf("persona: %v", err)
	}
	if _, err := s.CreateTerminalSession(ctx, pa, "sh", "human", "test"); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, _, err := s.ClaimTerminalSessions(ctx, pa, "runner-x", "cloud", time.Minute, 1); err != nil {
		t.Fatalf("claim: %v", err)
	}

	var logs sync.Map
	fake1 := &fakeProc{ops: map[string]*runtimeprovision.ProcessOperation{}}
	d1 := New(s, fake1, nil, testConfig(&logs, "d1:"))
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan struct{})
	go func() { d1.Run(ctx1); close(done1) }()
	defer func() { cancel1(); <-done1 }()
	waitFor(t, 5*time.Second, "d1 launch", func() bool {
		fake1.mu.Lock()
		defer fake1.mu.Unlock()
		return fake1.startCalls == 1
	})

	// Kill the advisory-lock connection at the database: the lock is
	// gone while the process survives — the exact window the watchdog
	// exists for. This test DB is isolated; only the lock holder can
	// be matched.
	pool := testdb.Create(t)
	if _, err := pool.Exec(ctx, `
		SELECT pg_terminate_backend(pid) FROM pg_locks
		WHERE locktype = 'advisory'`); err != nil {
		t.Fatalf("terminate lock backend: %v", err)
	}

	// The watchdog notices within its tick bound and stops the term.
	waitFor(t, 15*time.Second, "watchdog stop", func() bool {
		var saw bool
		logs.Range(func(_, v any) bool {
			if strings.Contains(v.(string), "runner lock connection lost") {
				saw = true
			}
			return !saw
		})
		return saw
	})

	// The driver recovers on its own: it re-acquires the free lock
	// and re-adopts the session under a new incarnation — the epoch
	// bump + claim re-stamp is exactly what fences the stale term's
	// in-flight work. A second launch is the re-attach of the same op.
	waitFor(t, 10*time.Second, "d1 re-adoption", func() bool {
		fake1.mu.Lock()
		defer fake1.mu.Unlock()
		return fake1.startCalls >= 2
	})
	select {
	case <-done1:
		t.Fatal("driver exited instead of recovering ownership")
	default:
	}
}

// TFIN-08: an adopted 'ending' session whose op never journaled is
// tombstone-cancelled — the deterministic "never existed" fence — and
// never launched just to be killed.
func TestDriverAdoptEndingTombstonesNeverLaunches(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	if _, _, err := s.EnsurePersona(ctx, pa, nil, ""); err != nil {
		t.Fatalf("persona: %v", err)
	}
	sess, err := s.CreateTerminalSession(ctx, pa, "sh", "human", "test")
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, _, err := s.ClaimTerminalSessions(ctx, pa, "runner-x", "cloud", time.Minute, 1); err != nil {
		t.Fatalf("claim: %v", err)
	}
	closed, err := s.CloseTerminalSession(ctx, pa, sess.SessionID, "closed by the person")
	if err != nil || closed.Status != "ending" {
		t.Fatalf("close = %+v err=%v", closed, err)
	}

	var logs sync.Map
	fake := &fakeProc{ops: map[string]*runtimeprovision.ProcessOperation{}, notFound: true}
	d := New(s, fake, nil, testConfig(&logs, "d:"))
	ctx1, cancel1 := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { d.Run(ctx1); close(done) }()
	defer func() { cancel1(); <-done }()

	waitFor(t, 5*time.Second, "tombstone cancel", func() bool {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return len(fake.cancelCalls) > 0
	})
	fake.mu.Lock()
	if fake.startCalls != 0 {
		t.Fatalf("ending session launched %d ops just to kill them", fake.startCalls)
	}
	if !fake.cancelCalls[0].TombstoneIfAbsent {
		t.Fatal("ending-session cancel did not request the tombstone fence")
	}
	fake.mu.Unlock()
	waitFor(t, 5*time.Second, "session ended", func() bool {
		return sessionStatus(t, s, pa, sess.SessionID) == "ended"
	})
}

// TFIN-05: a journaled provisioner loss boundary (journal rotation /
// vanish) reaches the API scrollback as an explicit zero-width gap
// marker — never silent contiguous output and never an invented byte
// range. This is the composed path: provisioner read reports the
// boundary, drainOutput appends the marker, both read surfaces serve
// it exactly once.
func TestDriverJournaledLossReachesScrollback(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	if _, _, err := s.EnsurePersona(ctx, pa, nil, ""); err != nil {
		t.Fatalf("persona: %v", err)
	}
	sess, err := s.CreateTerminalSession(ctx, pa, "sh", "human", "test")
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, _, err := s.ClaimTerminalSessions(ctx, pa, "runner-x", "cloud", time.Minute, 1); err != nil {
		t.Fatalf("claim: %v", err)
	}

	// Script the provisioner's interactive read: "before" occupies
	// [0,6), a journaled loss boundary sits at 6 (the emitted bytes
	// that were lost have unknown size), then "after" resumes at 6.
	var logs sync.Map
	fake := &fakeProc{
		ops: map[string]*runtimeprovision.ProcessOperation{},
		readFunc: func(offset int64) runtimeprovision.ProcessOutput {
			if offset >= 12 {
				return runtimeprovision.ProcessOutput{Offset: offset, NextOffset: offset, EOF: true}
			}
			return runtimeprovision.ProcessOutput{
				Offset: offset, NextOffset: 12,
				Content: "beforeafter",
				EOF:     true,
				Gaps:    []runtimeprovision.ProcessOutputGap{{At: 6, Note: "journal vanished"}},
			}
		},
	}
	d := New(s, fake, nil, testConfig(&logs, "d:"))
	ctx1, cancel1 := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { d.Run(ctx1); close(done) }()
	defer func() { cancel1(); <-done }()

	waitFor(t, 5*time.Second, "session active", func() bool {
		return sessionStatus(t, s, pa, sess.SessionID) == "active"
	})
	waitFor(t, 5*time.Second, "loss marker in scrollback", func() bool {
		read, err := s.ReadTerminalOutput(ctx, pa, sess.SessionID, 0, 0, 32)
		if err != nil {
			return false
		}
		for _, c := range read.Chunks {
			if c.Kind == "gap" && c.Base == 6 && c.GapTo != nil && *c.GapTo == 6 {
				return true
			}
		}
		return false
	})
	read, err := s.ReadTerminalOutput(ctx, pa, sess.SessionID, 0, 0, 32)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var data, markers int
	for _, c := range read.Chunks {
		if c.Kind == "data" {
			data++
		}
		if c.Kind == "gap" {
			markers++
		}
	}
	if markers != 1 {
		t.Fatalf("scrollback has %d gap markers, want exactly 1: %+v", markers, read.Chunks)
	}
	// Past the boundary the marker is consumed — a re-read at the
	// next cursor serves nothing.
	again, err := s.ReadTerminalOutput(ctx, pa, sess.SessionID, read.NextCursor, read.EventCursor, 32)
	if err != nil {
		t.Fatalf("re-read: %v", err)
	}
	for _, c := range again.Chunks {
		if c.Kind == "gap" {
			t.Fatalf("loss marker re-served past its boundary: %+v", again.Chunks)
		}
	}
}

// TFIN-04/07 residual — the root counterexample at actual-driver
// level: the old pump has dequeued an input and its transport write
// is in flight when the lock connection dies; the replacement steals
// the claim under a new incarnation. The in-flight row becomes
// 'unknown' atomically — the stale pump's late disposition is fenced
// and the byte is never re-delivered. (Honest bound: the write may
// have landed — indeterminate — so the ledger says 'unknown' rather
// than claiming either outcome.)
func TestDriverLockLossStealFencesInflightDelivery(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	if _, _, err := s.EnsurePersona(ctx, pa, nil, ""); err != nil {
		t.Fatalf("persona: %v", err)
	}
	sess, err := s.CreateTerminalSession(ctx, pa, "sh", "human", "test")
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, _, err := s.ClaimTerminalSessions(ctx, pa, "runner-x", "cloud", time.Minute, 1); err != nil {
		t.Fatalf("claim: %v", err)
	}

	var logs sync.Map
	gate := make(chan struct{})
	fake1 := &fakeProc{ops: map[string]*runtimeprovision.ProcessOperation{}, writeGate: gate}
	d1 := New(s, fake1, nil, testConfig(&logs, "d1:"))
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan struct{})
	go func() { d1.Run(ctx1); close(done1) }()
	defer func() { cancel1(); <-done1 }()
	waitFor(t, 5*time.Second, "d1 launch", func() bool {
		fake1.mu.Lock()
		defer fake1.mu.Unlock()
		return fake1.startCalls == 1
	})
	waitFor(t, 5*time.Second, "session active", func() bool {
		return sessionStatus(t, s, pa, sess.SessionID) == "active"
	})

	// The old pump dequeues the input and blocks inside the transport
	// write — the deterministic in-flight window.
	if _, err := s.SubmitTerminalInput(ctx, pa, sess.SessionID, "human", "stdin",
		map[string]any{"data": "one effect\n"}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	waitFor(t, 5*time.Second, "d1 in-flight write", func() bool {
		fake1.mu.Lock()
		defer fake1.mu.Unlock()
		return fake1.writes == 1
	})
	waitFor(t, 5*time.Second, "row dequeued", func() bool {
		rows, err := s.ListTerminalInputs(ctx, pa, sess.SessionID, 0, 8)
		return err == nil && len(rows) == 1 && rows[0].Status == "dequeued"
	})

	// Kill only d1's lock connection — pool conns and the pump stay
	// alive. The replacement acquires the lock and steals the claim.
	pool := testdb.Create(t)
	if _, err := pool.Exec(ctx, `
		SELECT pg_terminate_backend(pid) FROM pg_locks
		WHERE locktype = 'advisory'`); err != nil {
		t.Fatalf("terminate lock backend: %v", err)
	}
	fake2 := &fakeProc{ops: map[string]*runtimeprovision.ProcessOperation{}}
	d2 := New(s, fake2, nil, testConfig(&logs, "d2:"))
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan struct{})
	go func() { d2.Run(ctx2); close(done2) }()
	defer func() { cancel2(); <-done2 }()
	waitFor(t, 8*time.Second, "d2 takeover", func() bool {
		fake2.mu.Lock()
		defer fake2.mu.Unlock()
		return fake2.startCalls == 1
	})

	// The steal already resolved the in-flight row: 'unknown'.
	waitFor(t, 5*time.Second, "row unknown", func() bool {
		rows, err := s.ListTerminalInputs(ctx, pa, sess.SessionID, 0, 8)
		return err == nil && len(rows) == 1 && rows[0].Status == "unknown"
	})

	// Release the stale transport write — it may land, but the ledger
	// must not re-deliver it under the new incarnation.
	close(gate)
	time.Sleep(500 * time.Millisecond)
	fake1.mu.Lock()
	w1 := fake1.writes
	fake1.mu.Unlock()
	fake2.mu.Lock()
	w2 := fake2.writes
	fake2.mu.Unlock()
	if w1+w2 != 1 {
		t.Fatalf("input delivered %d times total (old=%d new=%d), want exactly one transport attempt", w1+w2, w1, w2)
	}
	rows, err := s.ListTerminalInputs(ctx, pa, sess.SessionID, 0, 8)
	if err != nil || len(rows) != 1 || rows[0].Status != "unknown" {
		t.Fatalf("ledger = %+v, want one unknown row", rows)
	}
}

// Shutdown lifecycle: Run must not release the advisory lock before
// the watchdog and every pump have stopped and joined — a release
// issued early hands ownership to a replacement while stale pumps
// still settle.
func TestDriverShutdownReleasesLockAfterPumps(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	if _, _, err := s.EnsurePersona(ctx, pa, nil, ""); err != nil {
		t.Fatalf("persona: %v", err)
	}
	sess, err := s.CreateTerminalSession(ctx, pa, "sh", "human", "test")
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, _, err := s.ClaimTerminalSessions(ctx, pa, "runner-x", "cloud", time.Minute, 1); err != nil {
		t.Fatalf("claim: %v", err)
	}

	var logs sync.Map
	gate := make(chan struct{})
	fake := &fakeProc{ops: map[string]*runtimeprovision.ProcessOperation{}, writeGate: gate}
	d := New(s, fake, nil, testConfig(&logs, "d:"))
	ctx1, cancel1 := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { d.Run(ctx1); close(done) }()
	waitFor(t, 5*time.Second, "d launch", func() bool {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return fake.startCalls == 1
	})
	waitFor(t, 5*time.Second, "session active", func() bool {
		return sessionStatus(t, s, pa, sess.SessionID) == "active"
	})

	// Park a pump mid-delivery, then cancel: Run must join the pump
	// before returning — and must not release the lock while it waits.
	if _, err := s.SubmitTerminalInput(ctx, pa, sess.SessionID, "human", "stdin",
		map[string]any{"data": "park\n"}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	waitFor(t, 5*time.Second, "in-flight write", func() bool {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return fake.writes == 1
	})
	cancel1()
	select {
	case <-done:
		t.Fatal("Run returned while a pump was still in flight")
	case <-time.After(300 * time.Millisecond):
	}
	if probe, err := s.TryAcquireTerminalRunnerLock(ctx, "runner-x"); err != nil || probe != nil {
		t.Fatalf("lock acquirable while Run still joining pumps: %v %v", probe, err)
	}
	close(gate)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after pumps settled")
	}
	// After Run returns the lock is free for the next owner.
	probe, err := s.TryAcquireTerminalRunnerLock(ctx, "runner-x")
	if err != nil || probe == nil {
		t.Fatalf("post-shutdown acquire lock=%v err=%v, want held", probe, err)
	}
	defer probe.Release(ctx)
}
