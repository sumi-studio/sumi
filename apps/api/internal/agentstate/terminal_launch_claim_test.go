package agentstate

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTerminalLaunchFenceRejectsClosedOrStaleClaim(t *testing.T) {
	for _, kind := range []string{"closed", "expired", "successor", "wrong runner", "wrong epoch"} {
		t.Run(kind, func(t *testing.T) {
			s := newTerminalStore(t)
			ctx := context.Background()
			pa := pid(t)
			mustPersona(t, s, pa)
			mustTerminalSession(t, s, pa, "shell")
			old := mustClaimTerminal(t, s, pa, "runner#old", time.Minute)
			runner, epoch := old.ClaimedBy, old.Epoch
			switch kind {
			case "closed":
				if _, err := s.CloseTerminalSession(ctx, pa, old.SessionID, "closed"); err != nil {
					t.Fatal(err)
				}
			case "expired":
				if _, err := s.pool.Exec(ctx, `UPDATE core_terminal_sessions SET claim_expires_at=now()-interval '1 second' WHERE session_id=$1`, old.SessionID); err != nil {
					t.Fatal(err)
				}
			case "successor":
				next := mustClaimTerminal(t, s, pa, "runner#new", time.Minute)
				if next.Epoch <= old.Epoch {
					t.Fatal("successor did not advance the epoch")
				}
			case "wrong runner":
				runner = "foreign-runner"
			case "wrong epoch":
				epoch--
			}
			ran := false
			err := s.WithTerminalLaunchFence(ctx, pa, old.SessionID, runner, epoch, func(context.Context) error { ran = true; return nil })
			if !errors.Is(err, ErrTerminalLaunchFenced) || ran {
				t.Fatalf("stale launch reached effect: ran=%v err=%v", ran, err)
			}
		})
	}
}

func TestTerminalLaunchFenceSerializesClose(t *testing.T) {
	s := newTerminalStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pa := pid(t)
	mustPersona(t, s, pa)
	mustTerminalSession(t, s, pa, "shell")
	claimed := mustClaimTerminal(t, s, pa, "runner", time.Minute)
	entered, release := make(chan struct{}), make(chan struct{})
	launched := make(chan error, 1)
	go func() {
		launched <- s.WithTerminalLaunchFence(ctx, pa, claimed.SessionID, claimed.ClaimedBy, claimed.Epoch, func(c context.Context) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-c.Done():
				return c.Err()
			}
		})
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	closed := make(chan error, 1)
	go func() {
		_, err := s.CloseTerminalSession(ctx, pa, claimed.SessionID, "close during launch")
		closed <- err
	}()
	select {
	case err := <-closed:
		t.Fatalf("close committed inside launch window: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	close(release)
	if err := <-launched; err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	ran := false
	err := s.WithTerminalLaunchFence(ctx, pa, claimed.SessionID, claimed.ClaimedBy, claimed.Epoch, func(context.Context) error { ran = true; return nil })
	if !errors.Is(err, ErrTerminalLaunchFenced) || ran {
		t.Fatalf("post-close retry reached effect: ran=%v err=%v", ran, err)
	}
}
