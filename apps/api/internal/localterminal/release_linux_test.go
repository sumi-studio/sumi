//go:build linux

package localterminal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
)

func TestTombstoneReleaseDurableAndRefenceable(t *testing.T) {
	b, cfg := newTestBackend(t, 0)
	ctx := context.Background()
	req := shellRequest("release-and-refence")
	look := runtimeprovision.ProcessLookupRequest{PersonalityAgentID: testPersona, OperationID: runtimeprovision.ProcessOperationID(testPersona, req.OriginatingToolCallID), TombstoneIfAbsent: true}
	if _, e := b.ReleaseProcessTombstone(ctx, look); !errors.Is(e, runtimeprovision.ErrProcessNotFound) {
		t.Fatal("absent release", e)
	}
	if _, e := b.CancelProcess(ctx, look); e != nil {
		t.Fatal(e)
	}
	wrong := look
	wrong.PersonalityAgentID = "0198f0f4-9b72-7000-8000-000000000322"
	if _, e := b.ReleaseProcessTombstone(ctx, wrong); !errors.Is(e, runtimeprovision.ErrProcessNotFound) {
		t.Fatal("foreign release", e)
	}
	wrong = look
	wrong.OperationID = runtimeprovision.ProcessOperationID(testPersona, "other-op")
	if _, e := b.ReleaseProcessTombstone(ctx, wrong); !errors.Is(e, runtimeprovision.ErrProcessNotFound) {
		t.Fatal("wrong operation release", e)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, e := b.ReleaseProcessTombstone(cancelled, look); !errors.Is(e, context.Canceled) {
		t.Fatal("cancelled release", e)
	}
	if replay, e := b.StartProcess(ctx, req); e != nil || !replay.Tombstone {
		t.Fatal("fence disappeared", replay, e)
	}
	b.Close()
	reopened, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	if op, e := reopened.ReleaseProcessTombstone(ctx, look); e != nil || !op.Tombstone || !op.Quiesced {
		t.Fatal("persisted release", op, e)
	}
	if _, e := os.Stat(filepath.Join(cfg.JournalRoot, look.OperationID+".json")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("journal retained", e)
	}
	reopened.Close()
	next, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer next.Close()
	if _, e := next.ProcessStatus(ctx, look); !errors.Is(e, runtimeprovision.ErrProcessNotFound) {
		t.Fatal("release did not survive restart", e)
	}
	// A subsequent cancel must erect a fresh durable fence, not inherit an unlock.
	if _, e := next.CancelProcess(ctx, look); e != nil {
		t.Fatal(e)
	}
	if replay, e := next.StartProcess(ctx, req); e != nil || !replay.Tombstone {
		t.Fatal("new cancel did not fence", replay, e)
	}
	if _, e := next.ReleaseProcessTombstone(ctx, look); e != nil {
		t.Fatal(e)
	}
	op, e := next.StartProcess(ctx, req)
	if e != nil || op.Tombstone {
		t.Fatal("real start after release", op, e)
	}
	write(t, next, op, "stty -echo; printf durable-release > released.txt; printf '\\nreleased-pty\\n'\n")
	output(t, next, op, "\r\nreleased-pty\r\n")
	if _, e := next.ReleaseProcessTombstone(ctx, look); !errors.Is(e, runtimeprovision.ErrConflict) {
		t.Fatal("live PTY was released", e)
	}
	write(t, next, op, "exit 7\n")
	ended := waitEnded(t, next, op)
	if _, e := next.ReleaseProcessTombstone(ctx, look); !errors.Is(e, runtimeprovision.ErrConflict) {
		t.Fatal("ended PTY history was erased", e)
	}
	if replay, e := next.StartProcess(ctx, req); e != nil || replay.State != ended.State || replay.Tombstone {
		t.Fatal("ended PTY resurrected", replay, e)
	}
	raw, e := os.ReadFile(filepath.Join(cfg.WorkspaceRoot, testPersona, "released.txt"))
	if e != nil || string(raw) != "durable-release" {
		t.Fatal("workspace", string(raw), e)
	}
}

func TestReleaseIOFailureRetainsFenceAndClosedOwnerCannotMutate(t *testing.T) {
	b, cfg := newTestBackend(t, 0)
	ctx := context.Background()
	req := shellRequest("release-failure")
	look := runtimeprovision.ProcessLookupRequest{PersonalityAgentID: testPersona, OperationID: runtimeprovision.ProcessOperationID(testPersona, req.OriginatingToolCallID), TombstoneIfAbsent: true}
	if _, e := b.CancelProcess(ctx, look); e != nil {
		t.Fatal(e)
	}
	journal := filepath.Join(cfg.JournalRoot, look.OperationID+".json")
	// Force a real unlink error without depending on root/non-root permissions.
	if e := os.Rename(journal, journal+".saved"); e != nil {
		t.Fatal(e)
	}
	if e := os.Mkdir(journal, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(journal, "block"), nil, 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := b.ReleaseProcessTombstone(ctx, look); e == nil {
		t.Fatal("unlink unexpectedly succeeded")
	}
	if replay, e := b.StartProcess(ctx, req); e != nil || !replay.Tombstone {
		t.Fatal("failed release forgot fence", replay, e)
	}
	if e := os.RemoveAll(journal); e != nil {
		t.Fatal(e)
	}
	if e := os.Rename(journal+".saved", journal); e != nil {
		t.Fatal(e)
	}
	b.Close()
	if _, e := b.ReleaseProcessTombstone(ctx, look); e == nil {
		t.Fatal("closed journal owner released fence")
	}
	if _, e := b.CancelProcess(ctx, look); e == nil {
		t.Fatal("closed journal owner rewrote fence")
	}
	reopened, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	if replay, e := reopened.StartProcess(ctx, req); e != nil || !replay.Tombstone {
		t.Fatal("failed release lost durable fence", replay, e)
	}
}

func TestReleasedFenceNeverErasesCancelledRealLaunch(t *testing.T) {
	b, _ := newTestBackend(t, 0)
	ctx := context.Background()
	req := shellRequest("cancelled-real-launch")
	op, e := b.StartProcess(ctx, req)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = b.CancelProcess(ctx, lookup(op)); e != nil {
		t.Fatal(e)
	}
	ended := waitEnded(t, b, op)
	if ended.State != runtimeprovision.ProcessCancelled || ended.Tombstone || ended.Quiesced {
		t.Fatal("cancelled actual process", ended)
	}
	if _, e = b.ReleaseProcessTombstone(ctx, lookup(op)); !errors.Is(e, runtimeprovision.ErrConflict) {
		t.Fatal("cancelled real execution erased", e)
	}
	if replay, e := b.StartProcess(ctx, req); e != nil || replay.State != runtimeprovision.ProcessCancelled || replay.Tombstone {
		t.Fatal("cancelled real execution resurrected", replay, e)
	}
}
