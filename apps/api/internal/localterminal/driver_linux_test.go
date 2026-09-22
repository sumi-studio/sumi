//go:build linux

package localterminal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
	"github.com/sumi-studio/sumi/apps/api/internal/termexec"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

type lostWriteReply struct {
	ProcessBackend
	lose  atomic.Bool
	calls atomic.Int64
}

func (b *lostWriteReply) WriteProcessInput(ctx context.Context, req runtimeprovision.ProcessInputRequest) (runtimeprovision.ProcessInputReceipt, error) {
	b.calls.Add(1)
	receipt, err := b.ProcessBackend.WriteProcessInput(ctx, req)
	if err == nil && receipt.Delivered && b.lose.Swap(false) {
		return runtimeprovision.ProcessInputReceipt{}, errors.New("fixture: reply lost after real PTY write")
	}
	return receipt, err
}
func eventually(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out:", what)
}
func TestDriverRestartKeepsPTYAndDoesNotReplayUnknownWrite(t *testing.T) {
	pool := testdb.Create(t)
	ctx := context.Background()
	if e := db.Migrate(ctx, pool); e != nil {
		t.Fatal(e)
	}
	store := agentstate.NewStore(pool)
	store.SetDefaultTerminalBackend("local")
	store.SetTerminalBackendAvailable("local")
	if _, _, e := store.EnsurePersona(ctx, testPersona, nil, "Local"); e != nil {
		t.Fatal(e)
	}
	backend, cfg := newTestBackend(t, 0)
	transport := &lostWriteReply{ProcessBackend: backend}
	run := func() func() {
		driver := termexec.New(store, transport, nil, termexec.Config{RunnerID: "local-restart-fixture", Backend: "local", Shell: "/bin/bash", ShellArgs: []string{"--noprofile", "--norc", "-i"}, Interval: 20 * time.Millisecond, PollInterval: 20 * time.Millisecond})
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() { defer close(done); driver.Run(runCtx) }()
		return func() {
			cancel()
			select {
			case <-done:
			case <-time.After(8 * time.Second):
				t.Fatal("driver stop hung")
			}
		}
	}
	stop := run()
	stopped := false
	defer func() {
		if !stopped {
			stop()
		}
	}()
	session, e := store.CreateTerminalSession(ctx, testPersona, "restart", "human", "test")
	if e != nil {
		t.Fatal(e)
	}
	eventually(t, "active", func() bool {
		s, e := store.GetTerminalSession(ctx, testPersona, session.SessionID)
		return e == nil && s.Status == "active"
	})
	submit := func(data string) agentstate.TerminalInput {
		in, e := store.SubmitTerminalInput(ctx, testPersona, session.SessionID, "human", "stdin", map[string]any{"data": data})
		if e != nil {
			t.Fatal(e)
		}
		return in
	}
	transport.lose.Store(true)
	unknown := submit("stty -echo; RETAINED=yes; printf x >> exactly-once\n")
	eventually(t, "effect and unknown disposition", func() bool {
		raw, _ := os.ReadFile(filepath.Join(cfg.WorkspaceRoot, testPersona, "exactly-once"))
		inputs, e := store.ListTerminalInputs(ctx, testPersona, session.SessionID, 0, 100)
		for _, in := range inputs {
			if in.InputID == unknown.InputID {
				return e == nil && in.Status == "unknown" && string(raw) == "x"
			}
		}
		return false
	})
	before, e := store.GetTerminalSession(ctx, testPersona, session.SessionID)
	if e != nil {
		t.Fatal(e)
	}
	stop()
	stopped = true
	stop = run()
	stopped = false
	eventually(t, "new claim epoch", func() bool {
		s, e := store.GetTerminalSession(ctx, testPersona, session.SessionID)
		return e == nil && s.Status == "active" && s.Epoch > before.Epoch
	})
	submit("printf '%s' \"$RETAINED\" > retained-after-driver-restart\n")
	eventually(t, "same shell retained variable", func() bool {
		raw, _ := os.ReadFile(filepath.Join(cfg.WorkspaceRoot, testPersona, "retained-after-driver-restart"))
		return string(raw) == "yes"
	})
	raw, e := os.ReadFile(filepath.Join(cfg.WorkspaceRoot, testPersona, "exactly-once"))
	if e != nil || string(raw) != "x" || transport.calls.Load() != 2 {
		t.Fatal("unknown input replayed", string(raw), transport.calls.Load(), e)
	}
	t.Log("actual PostgreSQL driver epoch takeover + same live PTY; lost post-write reply remains unknown and original file effect occurs once")
}

func TestDriverReleasesDurableLocalFenceAndLaunchesPTY(t *testing.T) {
	pool := testdb.Create(t)
	ctx := context.Background()
	if e := db.Migrate(ctx, pool); e != nil {
		t.Fatal(e)
	}
	store := agentstate.NewStore(pool)
	store.SetDefaultTerminalBackend("local")
	store.SetTerminalBackendAvailable("local")
	if _, _, e := store.EnsurePersona(ctx, testPersona, nil, "Local"); e != nil {
		t.Fatal(e)
	}
	session, e := store.CreateTerminalSession(ctx, testPersona, "recovered Local terminal", "human", "test")
	if e != nil {
		t.Fatal(e)
	}
	first, cfg := newTestBackend(t, 0)
	look := runtimeprovision.ProcessLookupRequest{PersonalityAgentID: testPersona, OperationID: runtimeprovision.ProcessOperationID(testPersona, "term:"+session.SessionID), TombstoneIfAbsent: true}
	if _, e = first.CancelProcess(ctx, look); e != nil {
		t.Fatal(e)
	}
	first.Close()
	backend, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer backend.Close()
	driver := termexec.New(store, backend, nil, termexec.Config{RunnerID: "local-durable-fence", Backend: "local", Shell: "/bin/bash", ShellArgs: []string{"--noprofile", "--norc", "-i"}, Interval: 20 * time.Millisecond, PollInterval: 20 * time.Millisecond})
	run, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); driver.Run(run) }()
	defer func() { cancel(); <-done }()
	eventually(t, "released fence and active session", func() bool {
		got, e := store.GetTerminalSession(ctx, testPersona, session.SessionID)
		return e == nil && got.Status == "active"
	})
	op, e := backend.ProcessStatus(ctx, look)
	if e != nil || op.Tombstone || op.StartedAt == nil || !op.OutputAttached {
		t.Fatal("driver did not launch real PTY", op, e)
	}
	if _, e = store.SubmitTerminalInput(ctx, testPersona, session.SessionID, "human", "stdin", map[string]any{"data": "test -t 0 && printf recovered-once > release-driver.txt\n"}); e != nil {
		t.Fatal(e)
	}
	eventually(t, "released PTY writes actual persistent workspace", func() bool {
		b, _ := os.ReadFile(filepath.Join(cfg.WorkspaceRoot, testPersona, "release-driver.txt"))
		return string(b) == "recovered-once"
	})
	if _, e = store.CloseTerminalSession(ctx, testPersona, session.SessionID, "close recovered session"); e != nil {
		t.Fatal(e)
	}
	eventually(t, "recovered terminal closes normally", func() bool {
		got, e := store.GetTerminalSession(ctx, testPersona, session.SessionID)
		return e == nil && got.Status == "ended"
	})
	if _, e = backend.ReleaseProcessTombstone(ctx, look); !errors.Is(e, runtimeprovision.ErrConflict) {
		t.Fatal("driver's real closed session became releasable", e)
	}
	t.Log("real PG driver releases restart-persisted never-launched Local tombstone; same session acquires actual PTY, writes workspace, and closes without erasing execution history")
}
