//go:build linux

package localterminal

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
	"github.com/sumi-studio/sumi/apps/api/internal/termexec"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

type closeAcceptanceScope struct {
	reached, resume chan struct{}
	once            sync.Once
}

func (s *closeAcceptanceScope) EnsureScope(ctx context.Context, _ string) error {
	s.once.Do(func() { close(s.reached) })
	select {
	case <-s.resume:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type closeAcceptanceProcess struct {
	ProcessBackend
	starts, releases atomic.Int64
	tombstoneSeen    chan struct{}
	allowRelease     chan struct{}
	launched         chan runtimeprovision.ProcessOperation
	allowReturn      chan struct{}
}

func (p *closeAcceptanceProcess) StartProcess(ctx context.Context, r runtimeprovision.ProcessStartRequest) (runtimeprovision.ProcessOperation, error) {
	p.starts.Add(1)
	op, e := p.ProcessBackend.StartProcess(ctx, r)
	if e != nil {
		return op, e
	}
	if op.Tombstone && p.tombstoneSeen != nil {
		close(p.tombstoneSeen)
		select {
		case <-p.allowRelease:
		case <-ctx.Done():
			return op, ctx.Err()
		}
	} else if !op.Tombstone && op.StartedAt != nil && p.launched != nil {
		select {
		case p.launched <- op:
		case <-ctx.Done():
			return op, ctx.Err()
		}
		select {
		case <-p.allowReturn:
		case <-ctx.Done():
			return op, ctx.Err()
		}
	}
	return op, nil
}
func (p *closeAcceptanceProcess) ReleaseProcessTombstone(ctx context.Context, r runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error) {
	p.releases.Add(1)
	return p.ProcessBackend.ReleaseProcessTombstone(ctx, r)
}
func closeAcceptanceFixture(t *testing.T) (*pgxpool.Pool, *agentstate.Store, ProcessBackend, agentstate.TerminalSession, runtimeprovision.ProcessLookupRequest) {
	t.Helper()
	pool := testdb.Create(t)
	ctx := context.Background()
	if e := db.Migrate(ctx, pool); e != nil {
		t.Fatal(e)
	}
	store := agentstate.NewStore(pool)
	store.SetDefaultTerminalBackend("local")
	store.SetTerminalBackendAvailable("local")
	if _, _, e := store.EnsurePersona(ctx, testPersona, nil, "Local close acceptance"); e != nil {
		t.Fatal(e)
	}
	session, e := store.CreateTerminalSession(ctx, testPersona, "close ordering", "human", "test")
	if e != nil {
		t.Fatal(e)
	}
	backend, _ := newTestBackend(t, 0)
	look := runtimeprovision.ProcessLookupRequest{PersonalityAgentID: testPersona, OperationID: runtimeprovision.ProcessOperationID(testPersona, "term:"+session.SessionID), TombstoneIfAbsent: true}
	if _, e = backend.CancelProcess(ctx, look); e != nil {
		t.Fatal(e)
	}
	return pool, store, backend, session, look
}
func runCloseAcceptanceDriver(t *testing.T, store *agentstate.Store, process ProcessBackend, scope termexec.ScopeEnsurer, refused chan struct{}) func() {
	t.Helper()
	cfg := termexec.Config{RunnerID: "local-close-acceptance", Backend: "local", Shell: "/bin/bash", ShellArgs: []string{"--noprofile", "--norc", "-i"}, Interval: 20 * time.Millisecond, PollInterval: 20 * time.Millisecond}
	if refused != nil {
		cfg.Logf = func(_ string, args ...any) {
			for _, arg := range args {
				if err, ok := arg.(error); ok && errors.Is(err, agentstate.ErrTerminalLaunchFenced) {
					select {
					case refused <- struct{}{}:
					default:
					}
				}
			}
		}
	}
	driver := termexec.New(store, process, scope, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); driver.Run(ctx) }()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("driver shutdown timed out")
		}
	}
	t.Cleanup(stop)
	return stop
}
func awaitCloseAcceptance(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out:", what)
	}
}

func TestLocalCloseCommittedBeforeLaunchFenceDoesNotStartPTY(t *testing.T) {
	_, store, backend, session, look := closeAcceptanceFixture(t)
	process := &closeAcceptanceProcess{ProcessBackend: backend}
	scope := &closeAcceptanceScope{reached: make(chan struct{}), resume: make(chan struct{})}
	refused := make(chan struct{}, 1)
	stop := runCloseAcceptanceDriver(t, store, process, scope, refused)
	awaitCloseAcceptance(t, scope.reached, "pre-fence scope hook")
	closed, e := store.CloseTerminalSession(context.Background(), testPersona, session.SessionID, "close before launch fence")
	if e != nil || closed.Status != "ending" {
		t.Fatal("close did not commit", closed, e)
	}
	close(scope.resume)
	awaitCloseAcceptance(t, refused, "driver attempted and refused fenced launch")
	stop()
	if process.starts.Load() != 0 || process.releases.Load() != 0 {
		t.Fatal("closed session reached runtime start/release", process.starts.Load(), process.releases.Load())
	}
	op, e := backend.ProcessStatus(context.Background(), look)
	if e != nil || !op.Tombstone || op.StartedAt != nil || !op.Quiesced {
		t.Fatal("never-launched fence changed", op, e)
	}
	// A fresh driver incarnation adopts ending and completes physical disposition
	// through cancellation/status, not by releasing the tombstone to launch.
	runCloseAcceptanceDriver(t, store, process, nil, nil)
	eventually(t, "ending adoption completes", func() bool {
		s, e := store.GetTerminalSession(context.Background(), testPersona, session.SessionID)
		return e == nil && s.Status == "ended"
	})
	if process.starts.Load() != 0 || process.releases.Load() != 0 {
		t.Fatal("ending adoption launched", process.starts.Load(), process.releases.Load())
	}
	t.Log("close committed while scope hook paused before launch fence; driver refusal observed, zero Start/Release calls, durable never-launched fence retained, ending adoption completes")
}

func TestLocalLaunchFenceHoldsCloseThroughTombstoneRetryAndPTYStart(t *testing.T) {
	pool, store, backend, session, look := closeAcceptanceFixture(t)
	process := &closeAcceptanceProcess{ProcessBackend: backend, tombstoneSeen: make(chan struct{}), allowRelease: make(chan struct{}), launched: make(chan runtimeprovision.ProcessOperation, 1), allowReturn: make(chan struct{})}
	runCloseAcceptanceDriver(t, store, process, nil, nil)
	awaitCloseAcceptance(t, process.tombstoneSeen, "real tombstone response inside launch fence")
	type closeResult struct {
		session agentstate.TerminalSession
		err     error
	}
	closing, closeCancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer closeCancel()
	closed := make(chan closeResult, 1)
	go func() {
		s, e := store.CloseTerminalSession(closing, testPersona, session.SessionID, "close waits for admitted launch")
		closed <- closeResult{s, e}
	}()
	// Observe the real DB lock wait instead of inferring ordering from a sleep
	// or from the time a buffered launch event happens to be consumed.
	eventually(t, "Close is waiting on the session row lock", func() bool {
		select {
		case result := <-closed:
			t.Fatalf("Close committed before held launch window returned: %+v", result)
		default:
		}
		var waiting bool
		e := pool.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%core_terminal_sessions%' AND cardinality(pg_blocking_pids(pid))>0)`).Scan(&waiting)
		if e != nil {
			t.Fatal(e)
		}
		return waiting
	})
	close(process.allowRelease)
	var launched runtimeprovision.ProcessOperation
	select {
	case launched = <-process.launched:
	case <-time.After(5 * time.Second):
		t.Fatal("released tombstone did not launch actual PTY")
	}
	if launched.Tombstone || launched.StartedAt == nil || launched.State != runtimeprovision.ProcessRunning {
		t.Fatal("not a physical launch", launched)
	}
	// The real PTY already exists, but its response is still inside the locked
	// launch window. Prove actual useful I/O before allowing Close to commit.
	write(t, backend, launched, "stty -echo; printf '\\nlaunch-before-close\\n'\n")
	output(t, backend, launched, "\r\nlaunch-before-close\r\n")
	select {
	case result := <-closed:
		t.Fatalf("Close crossed held real-PTY response: %+v", result)
	default:
	}
	stored, e := store.GetTerminalSession(context.Background(), testPersona, session.SessionID)
	if e != nil || stored.Status != "claimed" {
		t.Fatal("close became visible while launch window held", stored, e)
	}
	close(process.allowReturn)
	select {
	case result := <-closed:
		if result.err != nil || result.session.Status != "ending" {
			t.Fatal("close after launch window", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not proceed after launch window returned")
	}
	eventually(t, "closed shell's observed physical exit", func() bool {
		op, e := backend.ProcessStatus(context.Background(), look)
		return e == nil && op.State.Terminal() && op.ExitCode != nil
	})
	eventually(t, "shared session ended", func() bool {
		s, e := store.GetTerminalSession(context.Background(), testPersona, session.SessionID)
		return e == nil && s.Status == "ended"
	})
	ended, e := backend.ProcessStatus(context.Background(), look)
	if e != nil || ended.Tombstone || ended.Quiesced {
		t.Fatal("closed real process lost honest history", ended, e)
	}
	if process.starts.Load() != 2 || process.releases.Load() != 1 {
		t.Fatal("unexpected replay/release count", process.starts.Load(), process.releases.Load())
	}
	if _, e = backend.ReleaseProcessTombstone(context.Background(), look); !errors.Is(e, runtimeprovision.ErrConflict) {
		t.Fatal("closed real history became releasable", e)
	}
	t.Log("actual PG lock wait spans tombstone release/retry and real PTY I/O; Close commits only after launch response/fence return, then shell physically exits and session ends; no physical-quiescence claim")
}
