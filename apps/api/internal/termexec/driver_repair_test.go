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
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
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
	s.SetTerminalBackendAvailable("cloud")
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

func (f *fakeProc) ReleaseProcessTombstone(_ context.Context, r runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.op(r)
	if !ok {
		return runtimeprovision.ProcessOperation{}, runtimeprovision.ErrProcessNotFound
	}
	if !o.Tombstone {
		return runtimeprovision.ProcessOperation{}, runtimeprovision.ErrConflict
	}
	op := *o
	delete(f.ops, r.OperationID)
	return op, nil
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

// ------------------------------------------------------------------
// TREV2-01: transport ambiguity → 'unknown', never resent.
//
// wireProvisioner serves the real runtimeprovision HTTP protocol on a
// unix socket, so these tests exercise the actual Client's transport,
// marshalling and error decoding — not a stubbed error value. The
// "drop" mode applies the effect server-side and then destroys the
// connection before the response is read: the exact window where a
// definite 'failed' would invite a duplicate resend.
// ------------------------------------------------------------------

type wireProvisioner struct {
	sock string
	mu   sync.Mutex
	// modes[endpoint] ∈ "ok" | "drop" | "conflict" | "operation_failed"
	// | "resize_unsupported" | "not_interactive" | "garbage"
	modes   map[string]string
	effects map[string]int
}

func startWireProvisioner(t *testing.T) *wireProvisioner {
	t.Helper()
	dir := t.TempDir()
	wp := &wireProvisioner{sock: dir + "/prov.sock", modes: map[string]string{}, effects: map[string]int{}}
	ln, err := net.Listen("unix", wp.sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mux := http.NewServeMux()
	reply := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		b, _ := json.Marshal(v)
		_, _ = w.Write(b)
	}
	fail := func(w http.ResponseWriter, status int, code, msg string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		fmt.Fprintf(w, `{"code":%q,"message":%q}`, code, msg)
	}
	op := func(r *http.Request) runtimeprovision.ProcessOperation {
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		paid, _ := req["personality_agent_id"].(string)
		return runtimeprovision.ProcessOperation{
			OperationID:        runtimeprovision.ProcessOperationID(paid, "term:composed-wire"),
			PersonalityAgentID: paid,
			Executable:         "/bin/bash",
			State:              runtimeprovision.ProcessRunning,
			Interactive:        true, TTY: true,
		}
	}
	effect := func(endpoint string, w http.ResponseWriter) bool {
		wp.mu.Lock()
		defer wp.mu.Unlock()
		switch wp.modes[endpoint] {
		case "conflict":
			fail(w, 409, "conflict", "operation is terminal")
		case "operation_failed":
			fail(w, 502, "operation_failed", "internal fault after dispatch")
		case "resize_unsupported":
			fail(w, 502, "resize_unsupported", "daemon cannot resize")
		case "not_interactive":
			fail(w, 409, "not_interactive", "operation is not interactive")
		case "garbage":
			w.WriteHeader(502)
			_, _ = w.Write([]byte("<html>proxy failure</html>"))
		case "drop":
			// The effect is applied HERE — the same point a real
			// provisioner completes sink.Write before forming the
			// receipt — then the response never arrives.
			wp.effects[endpoint]++
			if hj, ok := w.(http.Hijacker); ok {
				conn, _, herr := hj.Hijack()
				if herr == nil {
					_ = conn.Close()
				}
			}
		default:
			wp.effects[endpoint]++
			return true
		}
		return false
	}
	mux.HandleFunc("/v1/process/start", func(w http.ResponseWriter, r *http.Request) {
		reply(w, op(r))
	})
	mux.HandleFunc("/v1/process/status", func(w http.ResponseWriter, r *http.Request) {
		reply(w, op(r))
	})
	mux.HandleFunc("/v1/process/cancel", func(w http.ResponseWriter, r *http.Request) {
		o := op(r)
		o.State = runtimeprovision.ProcessSucceeded
		reply(w, o)
	})
	mux.HandleFunc("/v1/process/output", func(w http.ResponseWriter, r *http.Request) {
		var req runtimeprovision.ProcessOutputRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		reply(w, runtimeprovision.ProcessOutput{
			OperationID: req.OperationID, Stream: "stdout",
			Offset: req.Offset, NextOffset: req.Offset, EOF: true,
		})
	})
	mux.HandleFunc("/v1/process/input", func(w http.ResponseWriter, r *http.Request) {
		var req runtimeprovision.ProcessInputRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if effect("/v1/process/input", w) {
			reply(w, runtimeprovision.ProcessInputReceipt{Delivered: true})
		}
	})
	mux.HandleFunc("/v1/process/signal", func(w http.ResponseWriter, r *http.Request) {
		var req runtimeprovision.ProcessSignalRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if effect("/v1/process/signal", w) {
			reply(w, runtimeprovision.ProcessInputReceipt{Delivered: true})
		}
	})
	mux.HandleFunc("/v1/process/resize", func(w http.ResponseWriter, r *http.Request) {
		if effect("/v1/process/resize", w) {
			reply(w, op(r))
		}
	})
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close(); _ = ln.Close() })
	return wp
}

func (wp *wireProvisioner) set(endpoint, mode string) {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	wp.modes[endpoint] = mode
}

func (wp *wireProvisioner) count(endpoint string) int {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	return wp.effects[endpoint]
}

func inputStatus(t *testing.T, s *agentstate.Store, pa, sid string, seq int64) string {
	t.Helper()
	rows, err := s.ListTerminalInputs(context.Background(), pa, sid, seq-1, 10)
	if err != nil {
		t.Fatalf("list inputs: %v", err)
	}
	for _, r := range rows {
		if r.Seq == seq {
			return r.Status
		}
	}
	return ""
}

// The decisive TREV2-01 schedule: the provisioner applies the stdin
// effect, then the response is destroyed before the client reads it.
// The durable ledger must say 'unknown' (never auto-resent), not
// 'failed' — the byte ran in the shell and 'failed' invites a
// duplicate. Controls: explicit pre-effect refusals stay 'failed';
// generic/undecodable server failures are ambiguous → 'unknown'.
func TestDriverTransportLossRecordsUnknown(t *testing.T) {
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
	wp := startWireProvisioner(t)
	client, err := runtimeprovision.NewUnixClient(wp.sock)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	var logs sync.Map
	d := New(s, client, nil, testConfig(&logs, "d:"))
	ctx1, cancel1 := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { d.Run(ctx1); close(done) }()
	defer func() { cancel1(); <-done }()

	waitFor(t, 10*time.Second, "session active", func() bool {
		return sessionStatus(t, s, pa, sess.SessionID) == "active"
	})

	submit := func(kind string, payload map[string]any) int64 {
		t.Helper()
		in, err := s.SubmitTerminalInput(ctx, pa, sess.SessionID, "human", kind, payload)
		if err != nil {
			t.Fatalf("submit %s: %v", kind, err)
		}
		return in.Seq
	}
	waitStatus := func(seq int64, want string) {
		t.Helper()
		waitFor(t, 8*time.Second, fmt.Sprintf("input %d → %s", seq, want), func() bool {
			return inputStatus(t, s, pa, sess.SessionID, seq) == want
		})
	}

	// 1. Effect applied + response destroyed → 'unknown', and the
	//    input is never re-delivered even after several pump ticks.
	wp.set("/v1/process/input", "drop")
	seq := submit("stdin", map[string]any{"data": "echo hi\n"})
	waitStatus(seq, "unknown")
	if wp.count("/v1/process/input") != 1 {
		t.Fatalf("input effects = %d, want exactly 1 (effect applied once)", wp.count("/v1/process/input"))
	}
	time.Sleep(400 * time.Millisecond) // several 50ms pump ticks
	if got := wp.count("/v1/process/input"); got != 1 {
		t.Fatalf("input re-delivered after 'unknown': effects = %d", got)
	}

	// 2. Explicit pre-effect refusal → 'failed' (safe to resend).
	wp.set("/v1/process/input", "conflict")
	seq = submit("stdin", map[string]any{"data": "echo no\n"})
	waitStatus(seq, "failed")

	// 3. Generic structured server failure → ambiguous → 'unknown'.
	wp.set("/v1/process/input", "operation_failed")
	seq = submit("stdin", map[string]any{"data": "echo maybe\n"})
	waitStatus(seq, "unknown")

	// 4. Undecodable server reply → 'unknown'.
	wp.set("/v1/process/input", "garbage")
	seq = submit("stdin", map[string]any{"data": "echo html\n"})
	waitStatus(seq, "unknown")

	// 5. Same contract on the signal path: effect + lost answer.
	wp.set("/v1/process/signal", "drop")
	seq = submit("signal", map[string]any{"signal": "TERM"})
	waitStatus(seq, "unknown")
	if wp.count("/v1/process/signal") != 1 {
		t.Fatalf("signal effects = %d, want 1", wp.count("/v1/process/signal"))
	}

	// 6. Resize definite refusal → 'failed'.
	wp.set("/v1/process/resize", "resize_unsupported")
	seq = submit("resize", map[string]any{"cols": 120.0, "rows": 40.0})
	waitStatus(seq, "failed")

	// 7. Delivery still lands 'written' on a clean response.
	wp.set("/v1/process/input", "ok")
	seq = submit("stdin", map[string]any{"data": "echo ok\n"})
	waitStatus(seq, "written")
}

// Client-side decode of an unknown structured code must not collapse
// into a definite sentinel: the driver treats it as ambiguous.
func TestClientStructuredUnknownCodeStaysAmbiguous(t *testing.T) {
	wp := startWireProvisioner(t)
	wp.set("/v1/process/input", "operation_failed")
	client, err := runtimeprovision.NewUnixClient(wp.sock)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	pa := pid(t)
	opID := runtimeprovision.ProcessOperationID(pa, "call-x")
	_, err = client.WriteProcessInput(context.Background(), runtimeprovision.ProcessInputRequest{
		ProcessLookupRequest: runtimeprovision.ProcessLookupRequest{
			PersonalityAgentID: pa, OperationID: opID,
		},
		Data: []byte("x"),
	})
	if err == nil {
		t.Fatal("expected error")
	}
	for _, sentinel := range []error{
		runtimeprovision.ErrProcessNotFound, runtimeprovision.ErrProcessBusy,
		runtimeprovision.ErrInvalidProcessRequest, runtimeprovision.ErrConflict,
		runtimeprovision.ErrProcessWorkspace, runtimeprovision.ErrProcessNotInteractive,
		runtimeprovision.ErrProcessResizeUnsupported,
	} {
		if errors.Is(err, sentinel) {
			t.Fatalf("ambiguous server failure collapsed into %v", sentinel)
		}
	}
	// A structured refusal DOES map to its sentinel — the contract
	// nonterminal consumers already rely on.
	wp.set("/v1/process/input", "not_interactive")
	_, err = client.WriteProcessInput(context.Background(), runtimeprovision.ProcessInputRequest{
		ProcessLookupRequest: runtimeprovision.ProcessLookupRequest{
			PersonalityAgentID: pa, OperationID: opID,
		},
		Data: []byte("x"),
	})
	if !errors.Is(err, runtimeprovision.ErrProcessNotInteractive) {
		t.Fatalf("not_interactive did not map to sentinel: %v", err)
	}
}

// TREV2-05: a RunnerID containing '#' would blur the incarnation
// prefix boundary — the driver refuses visibly instead of driving
// under a corrupt identity. Literal starts_with matching makes
// LIKE metacharacters inert, but the name rule is enforced at run.
func TestDriverInvalidRunnerIDRefusesVisibly(t *testing.T) {
	s := newStore(t)
	var logs sync.Map
	for _, bad := range []string{"a#b", "runner%all", "with space", "runner/x"} {
		fake := &fakeProc{ops: map[string]*runtimeprovision.ProcessOperation{}}
		cfg := testConfig(&logs, "d:")
		cfg.RunnerID = bad
		d := New(s, fake, nil, cfg)
		done := make(chan struct{})
		go func() { d.Run(context.Background()); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("RunnerID %q: Run did not refuse", bad)
		}
		fake.mu.Lock()
		if fake.startCalls != 0 {
			t.Fatalf("RunnerID %q launched ops despite refusal", bad)
		}
		fake.mu.Unlock()
	}
	var refused bool
	logs.Range(func(_, v any) bool {
		if strings.Contains(v.(string), "refusing to run") {
			refused = true
		}
		return true
	})
	if !refused {
		t.Fatal("no visible refusal logged for invalid RunnerID")
	}
}

// TREV2-06: a zero-width loss marker sitting at the drain frontier is
// emitted once — the pump's emittedGaps high-water stops the
// every-tick doomed re-insert — while a genuinely new boundary still
// reaches the scrollback.
func TestDriverLossMarkerEmitsOnceNotEveryTick(t *testing.T) {
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
	var mu sync.Mutex
	secondGap := false
	fake := &fakeProc{
		ops: map[string]*runtimeprovision.ProcessOperation{},
		readFunc: func(offset int64) runtimeprovision.ProcessOutput {
			mu.Lock()
			defer mu.Unlock()
			gaps := []runtimeprovision.ProcessOutputGap{{At: 6, Note: "journal rotated"}}
			if secondGap {
				gaps = append(gaps,
					runtimeprovision.ProcessOutputGap{At: 6, Note: "journal vanished"},
					runtimeprovision.ProcessOutputGap{At: 9, Note: "journal rotated again"})
			}
			return runtimeprovision.ProcessOutput{
				Offset: offset, NextOffset: 12, Content: "beforeafter",
				EOF: true, Gaps: gaps,
			}
		},
	}
	var logs sync.Map
	d := New(s, fake, nil, testConfig(&logs, "d:"))
	ctx1, cancel1 := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { d.Run(ctx1); close(done) }()
	defer func() { cancel1(); <-done }()

	waitFor(t, 5*time.Second, "session active", func() bool {
		return sessionStatus(t, s, pa, sess.SessionID) == "active"
	})
	waitFor(t, 5*time.Second, "loss marker emitted", func() bool {
		d.mu.Lock()
		p := d.pumps[sess.SessionID]
		d.mu.Unlock()
		return p != nil && p.emittedGapCount() == 1
	})
	// Settle several ticks: the marker must not be re-appended — the
	// emitted-set, not another dead insert, is the frontier state.
	time.Sleep(300 * time.Millisecond)
	count := func() int {
		read, err := s.ReadTerminalOutput(ctx, pa, sess.SessionID, 0, 0, 64)
		if err != nil {
			t.Fatalf("read output: %v", err)
		}
		n := 0
		for _, c := range read.Chunks {
			if c.Kind == "gap" && c.Base == 6 {
				n++
			}
		}
		return n
	}
	if got := count(); got != 1 {
		t.Fatalf("loss markers in scrollback = %d, want exactly 1", got)
	}
	// A genuinely new boundary is a distinct event — it must still be
	// emitted. The same-position repeat (different note) is attempted
	// once and absorbed by the store's (session, base, kind) dedupe:
	// a marker at position 6 already states the only reader-visible
	// fact a zero-width boundary can carry, so the scrollback gains
	// exactly one new row — at position 9.
	mu.Lock()
	secondGap = true
	mu.Unlock()
	waitFor(t, 5*time.Second, "second boundary emitted", func() bool {
		read, err := s.ReadTerminalOutput(ctx, pa, sess.SessionID, 0, 0, 64)
		if err != nil {
			t.Fatalf("read output: %v", err)
		}
		var at9 bool
		for _, c := range read.Chunks {
			if c.Kind == "gap" && c.Base == 9 {
				at9 = true
			}
		}
		return at9
	})
	if got := count(); got != 1 {
		t.Fatalf("same-position markers = %d, want 1 (positional dedupe)", got)
	}
	d.mu.Lock()
	p := d.pumps[sess.SessionID]
	d.mu.Unlock()
	emitted := p.emittedGapCount()
	if emitted < 2 {
		t.Fatalf("emittedGaps = %d, want the new boundary recorded", emitted)
	}
}

// ------------------------------------------------------------------
// Joined delayed-start fence: real agentstate store + real
// runtimeprovision service + real unix transport. This is the exact
// RWC-01 schedule root reproduced with a fake proc API — here the
// provisioner is real, so "nothing journaled" is physical evidence.

// fenceBackend is a real ProcessBackend that counts launches without
// Docker: the provisioner journal is the boundary this repair fences.
type fenceBackend struct {
	mu       sync.Mutex
	launches int
	obs      runtimeprovision.ProcessObservation
}

func (b *fenceBackend) Prepare(_ context.Context, r runtimeprovision.PrepareRequest) (runtimeprovision.PreparedEpoch, error) {
	return runtimeprovision.PreparedEpoch{PersonalityAgentID: r.PersonalityAgentID, Generation: 1, RPCBootNonce: "t", OpaquePreparedHandle: "h"}, nil
}
func (b *fenceBackend) Activate(context.Context, runtimeprovision.ActivateRequest) error { return nil }
func (b *fenceBackend) Abort(context.Context, runtimeprovision.PreparedEpoch) (runtimeprovision.Inspection, error) {
	return runtimeprovision.Inspection{}, nil
}
func (b *fenceBackend) Inspect(context.Context, string) (runtimeprovision.Inspection, error) {
	return runtimeprovision.Inspection{}, nil
}
func (b *fenceBackend) Stop(context.Context, runtimeprovision.PreparedEpoch) (runtimeprovision.Inspection, error) {
	return runtimeprovision.Inspection{}, nil
}
func (b *fenceBackend) Reconcile(context.Context, runtimeprovision.ReconcileRequest) (runtimeprovision.Inspection, error) {
	return runtimeprovision.Inspection{}, nil
}
func (b *fenceBackend) LaunchProcess(context.Context, runtimeprovision.ProcessOperation) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.launches++
	b.obs = runtimeprovision.ProcessObservation{Exists: true, Running: true, StartedAt: time.Now().UTC()}
	return nil
}
func (b *fenceBackend) InspectProcess(context.Context, runtimeprovision.ProcessOperation) (runtimeprovision.ProcessObservation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.obs, nil
}
func (b *fenceBackend) StopProcess(context.Context, runtimeprovision.ProcessOperation) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.obs.Running = false
	b.obs.ExitCode = 137
	return nil
}
func (b *fenceBackend) launchCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.launches
}

// gateScope parks runSession inside EnsureScope: the terminal row is
// 'claimed' but StartProcess has not reached the provisioner — exactly
// the admitted-but-delayed start the seal must fence.
type gateScope struct {
	entered chan struct{}
	release chan struct{}
}

func (g *gateScope) EnsureScope(context.Context, string) error {
	select {
	case g.entered <- struct{}{}:
	default:
	}
	<-g.release
	return nil
}

// newJoinedTerminalStack wires the real store, a real
// runtimeprovision.Service over a real unix socket, and a counting
// ProcessBackend. The files-scope check binary is a stub that echoes
// the canonical scope path — workspace resolution is not the boundary
// under test (the journey proves the real FUSE mount separately); the
// journal/fence ordering is.
func newJoinedTerminalStack(t *testing.T, personaID string) (*agentstate.Store, *pgxpool.Pool, *fenceBackend, *runtimeprovision.Client, context.CancelFunc) {
	t.Helper()
	pool := testdb.Create(t)
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	s := agentstate.NewStore(pool)
	s.SetDefaultTerminalBackend("cloud")
	s.SetTerminalBackendAvailable("cloud")

	scopeName, err := fileaccess.ScopeForPersona(personaID)
	if err != nil {
		t.Fatalf("scope name: %v", err)
	}
	mnt := t.TempDir() + "/mnt"
	scopeDir := filepath.Join(mnt, scopeName)
	if err := os.MkdirAll(scopeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	checkBin := t.TempDir() + "/sumi-files-check"
	if err := os.WriteFile(checkBin, []byte(
		"#!/bin/sh\nfor last; do :; done\necho \""+mnt+"/$last\"\n",
	), 0o755); err != nil {
		t.Fatal(err)
	}

	backend := &fenceBackend{}
	svc, err := runtimeprovision.NewService(backend, runtimeprovision.ServiceConfig{
		StateDirectory: t.TempDir() + "/prov",
		Files: runtimeprovision.FilesEnvironment{
			Mountpoint: mnt, VolumeUUID: "vol-test", CheckPath: checkBin,
		},
	})
	if err != nil {
		t.Fatalf("provisioner service: %v", err)
	}
	sock := t.TempDir() + "/prov.sock"
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	handler, err := runtimeprovision.NewHandler(svc)
	if err != nil {
		t.Fatalf("provisioner handler: %v", err)
	}
	srv := &http.Server{Handler: handler}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close(); ln.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	go svc.RunProcessObserver(ctx)
	client, err := runtimeprovision.NewUnixClient(sock)
	if err != nil {
		cancel()
		t.Fatalf("provisioner client: %v", err)
	}
	return s, pool, backend, client, cancel
}

// RWC-01 schedule, joined: claim commits, runSession is parked before
// StartProcess, the return seals, then the delayed start fires. The
// launch fence must block the provisioner call entirely — nothing may
// be journaled for the sealed persona. When the move is cancelled
// (authority back to 'active'), the still-claimed session must start
// normally on the next reclaim.
func TestDriverDelayedStartFencedBySealAndRecovers(t *testing.T) {
	ctx := context.Background()
	pa := pid(t)
	s, pool, backend, client, stopObs := newJoinedTerminalStack(t, pa)
	defer stopObs()
	if _, _, err := s.EnsurePersona(ctx, pa, nil, ""); err != nil {
		t.Fatal(err)
	}
	sess, err := s.CreateTerminalSession(ctx, pa, "late", "human", "test")
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	opID := runtimeprovision.ProcessOperationID(pa, "term:"+sess.SessionID)

	var logs sync.Map
	scope := &gateScope{entered: make(chan struct{}, 1), release: make(chan struct{})}
	d := New(s, client, scope, testConfig(&logs, "d:"))
	dctx, dcancel := context.WithCancel(context.Background())
	ddone := make(chan struct{})
	go func() { d.Run(dctx); close(ddone) }()
	defer func() { dcancel(); <-ddone }()
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		t.Logf("final session status: %s", sessionStatus(t, s, pa, sess.SessionID))
		var lines []string
		logs.Range(func(_, v any) bool { lines = append(lines, v.(string)); return true })
		sort.Strings(lines)
		for _, l := range lines {
			t.Log(l)
		}
	})

	waitFor(t, 15*time.Second, "session claimed", func() bool {
		return sessionStatus(t, s, pa, sess.SessionID) == "claimed"
	})
	<-scope.entered // runSession is between claim-commit and StartProcess

	// The return seal commits while the admitted start is parked.
	if _, err := pool.Exec(ctx,
		`UPDATE core_personas SET authority = 'sealed' WHERE persona_id = $1`, pa); err != nil {
		t.Fatal(err)
	}
	close(scope.release)

	// The fenced start must never journal on the provisioner: the op
	// stays absent (not even a tombstone — the fence refused before the
	// runtime boundary) and no launch is attempted.
	waitFor(t, 10*time.Second, "op absent on provisioner", func() bool {
		_, err := client.ProcessStatus(ctx, runtimeprovision.ProcessLookupRequest{
			PersonalityAgentID: pa, OperationID: opID,
		})
		return errors.Is(err, runtimeprovision.ErrProcessNotFound)
	})
	// Settle past an observer tick to be sure no launch hides behind timing.
	time.Sleep(1500 * time.Millisecond)
	if n := backend.launchCount(); n != 0 {
		t.Fatalf("fenced persona launched a process: %d", n)
	}

	// Cancelled move: authority back to 'active'. The claimed row
	// lapses, the driver reclaims it, the launch fence passes, and the
	// operation journals + launches normally.
	if _, err := pool.Exec(ctx,
		`UPDATE core_personas SET authority = 'active' WHERE persona_id = $1`, pa); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 20*time.Second, "op journaled non-tombstone", func() bool {
		op, err := client.ProcessStatus(ctx, runtimeprovision.ProcessLookupRequest{
			PersonalityAgentID: pa, OperationID: opID,
		})
		return err == nil && !op.Tombstone
	})
	waitFor(t, 10*time.Second, "one launch", func() bool { return backend.launchCount() == 1 })
}

// Recoverability of the gate's tombstone: a return plants
// TombstoneIfAbsent for a limbo session, then the move is cancelled
// before commit — the next launch under active authority must release
// the never-launched tombstone and start, not be fenced forever.
func TestDriverStartReleasesTombstoneOnActivePersona(t *testing.T) {
	ctx := context.Background()
	pa := pid(t)
	s, _, backend, client, stopObs := newJoinedTerminalStack(t, pa)
	defer stopObs()
	if _, _, err := s.EnsurePersona(ctx, pa, nil, ""); err != nil {
		t.Fatal(err)
	}
	sess, err := s.CreateTerminalSession(ctx, pa, "fenced", "human", "test")
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	opID := runtimeprovision.ProcessOperationID(pa, "term:"+sess.SessionID)

	// Simulate the gate's fence from an aborted return: a durable
	// never-launched tombstone under the session's deterministic id.
	if _, err := client.CancelProcess(ctx, runtimeprovision.ProcessLookupRequest{
		PersonalityAgentID:    pa,
		OperationID:           opID,
		OriginatingToolCallID: "term:" + sess.SessionID,
		TombstoneIfAbsent:     true,
	}); err != nil {
		t.Fatalf("plant tombstone: %v", err)
	}

	var logs sync.Map
	d := New(s, client, nil, testConfig(&logs, "d:"))
	dctx, dcancel := context.WithCancel(context.Background())
	ddone := make(chan struct{})
	go func() { d.Run(dctx); close(ddone) }()
	defer func() { dcancel(); <-ddone }()

	waitFor(t, 20*time.Second, "op journaled non-tombstone", func() bool {
		op, err := client.ProcessStatus(ctx, runtimeprovision.ProcessLookupRequest{
			PersonalityAgentID: pa, OperationID: opID,
		})
		return err == nil && !op.Tombstone
	})
	waitFor(t, 10*time.Second, "one launch", func() bool { return backend.launchCount() == 1 })
	waitFor(t, 10*time.Second, "session active", func() bool {
		return sessionStatus(t, s, pa, sess.SessionID) == "active"
	})
}
