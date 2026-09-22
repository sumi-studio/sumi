package runtimeprovision

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The cancel fence: CancelProcess with TombstoneIfAbsent journals a durable
// terminal record for an operation that never arrived. Once it exists, a
// delayed StartProcess replays 'cancelled' instead of launching — the
// 'absent' verdict a job driver publishes is then mechanically true, not a
// timeout guess. The fence must survive a service restart (it is a journal
// record, not memory) and stay out of the pending-completions feed (no
// command channel awaits it).
func TestProcessCancelTombstoneFencesAbsentOp(t *testing.T) {
	ctx := context.Background()
	b := &processTestBackend{fakeBackend: newFakeBackend()}
	directory := t.TempDir() + "/state"
	s, err := NewService(b, ServiceConfig{StateDirectory: directory})
	if err != nil {
		t.Fatal(err)
	}
	paid := uuid.NewString()
	opID := ProcessOperationID(paid, "job:never-arrived")

	// Default behavior is unchanged: a plain cancel of a missing op is
	// still 'not found' — tombstoning is opt-in per request.
	if _, err := s.CancelProcess(ctx, ProcessLookupRequest{PersonalityAgentID: paid, OperationID: opID}); !errors.Is(err, ErrProcessNotFound) {
		t.Fatalf("plain cancel of a missing op must stay not-found: %v", err)
	}

	fence, err := s.CancelProcess(ctx, ProcessLookupRequest{
		PersonalityAgentID:    paid,
		OperationID:           opID,
		OriginatingToolCallID: "job:never-arrived",
		TombstoneIfAbsent:     true,
	})
	if err != nil {
		t.Fatalf("tombstone cancel: %v", err)
	}
	if fence.State != ProcessCancelled || !fence.Tombstone {
		t.Fatalf("fence must be a cancelled tombstone: %+v", fence)
	}

	// A delayed start for the same operation replays the fence — it does
	// not launch, does not create a second record, and answers cancelled.
	op, err := s.StartProcess(ctx, ProcessStartRequest{
		PersonalityAgentID:    paid,
		OriginatingToolCallID: "job:never-arrived",
		Executable:            "/bin/sh", Args: []string{"-c", "echo late"},
	})
	if err != nil {
		t.Fatalf("late start must replay the fence, got %v", err)
	}
	if op.State != ProcessCancelled || !op.Tombstone {
		t.Fatalf("late start must return the tombstone: %+v", op)
	}
	s.observeProcesses(ctx)
	if b.launches != 0 {
		t.Fatalf("a fenced operation must never launch: %d", b.launches)
	}

	// A different tool call produces a different operation ID — fences
	// are per-operation, never persona-wide.
	other, err := s.StartProcess(ctx, ProcessStartRequest{
		PersonalityAgentID:    paid,
		OriginatingToolCallID: "job:other-work",
		Executable:            "/bin/true",
	})
	if err != nil || other.State != ProcessAccepted {
		t.Fatalf("unrelated work must still launch: %+v %v", other, err)
	}

	// Fence records never enter the completions feed — no command channel
	// awaits them.
	pending, err := s.PendingProcessCompletions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pending {
		if p.Tombstone || p.OperationID == opID {
			t.Fatalf("tombstone must not be a pending completion: %+v", p)
		}
	}

	// Restart: the journal reloads, the fence still holds. The only
	// launch ever is the legitimate 'other-work' op.
	s, err = NewService(b, ServiceConfig{StateDirectory: directory})
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.StartProcess(ctx, ProcessStartRequest{
		PersonalityAgentID:    paid,
		OriginatingToolCallID: "job:never-arrived",
		Executable:            "/bin/sh", Args: []string{"-c", "echo late"},
	})
	if err != nil || again.State != ProcessCancelled || !again.Tombstone {
		t.Fatalf("restarted service must still replay the fence: %+v %v", again, err)
	}
	s.observeProcesses(ctx)
	if b.launches != 1 {
		t.Fatalf("only the unfenced op may launch, never the tombstone: %d", b.launches)
	}
}

// The delayed-start window is real: a files-scope request resolves its
// canonical workspace BEFORE journaling, so a request can be 'in flight
// but invisible' for as long as the mount check takes. This test holds a
// start inside that window with a gated check, lands the cancel fence,
// then releases the check — the released start must replay the tombstone
// and never launch. That ordering is the causal proof a terminal 'absent'
// verdict cannot be followed by a late launch.
func TestProcessTombstoneFencesDelayedStart(t *testing.T) {
	ctx := context.Background()
	b := &processTestBackend{fakeBackend: newFakeBackend()}
	pu, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	paid := pu.String()
	mount := t.TempDir()
	scope := strings.ToLower(strings.ReplaceAll(paid, "-", ""))
	if err := os.MkdirAll(filepath.Join(mount, scope), 0777); err != nil {
		t.Fatal(err)
	}
	// Gated check: writes .entered once invoked, then waits for .release
	// before answering — a stand-in for a slow canonical mount check.
	checkPath := filepath.Join(mount, "fixture-files-check")
	checkScript := `#!/bin/sh
if [ "$1" = "--wait" ]; then shift 2; fi
mnt="$1"; uuid="$2"; scope="$3"
touch "$mnt/.entered"
i=0
while [ ! -f "$mnt/.release" ] && [ $i -lt 400 ]; do sleep 0.05; i=$((i+1)); done
[ -f "$mnt/.release" ] || { echo "refused: gate_timeout" >&2; exit 2; }
dir="$mnt/$scope"
[ -d "$dir" ] || { echo "refused: scope_missing: $scope" >&2; exit 2; }
printf '%s\n' "$(cd "$dir" && pwd -P)"
`
	if err := os.WriteFile(checkPath, []byte(checkScript), 0755); err != nil {
		t.Fatal(err)
	}
	s, err := NewService(b, ServiceConfig{
		StateDirectory: t.TempDir() + "/state",
		Files: FilesEnvironment{
			Mountpoint: mount, VolumeUUID: "5a1b72bc-b9ec-4a72-813b-2f4dc4cd6d07",
			CheckPath: checkPath, CheckWaitSeconds: 30,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	opID := ProcessOperationID(paid, "job:delayed")

	type startResult struct {
		op  ProcessOperation
		err error
	}
	started := make(chan startResult, 1)
	go func() {
		op, err := s.StartProcess(ctx, ProcessStartRequest{
			PersonalityAgentID:    paid,
			OriginatingToolCallID: "job:delayed",
			Executable:            "/bin/sh", Args: []string{"-c", "echo should-never-run"},
			Workspace: "files-scope",
		})
		started <- startResult{op, err}
	}()

	// Wait until the request is provably inside its pre-journal window.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(mount, ".entered")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("start never reached the workspace check")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The fence lands while the start is still resolving: the tombstone
	// is now the operation's durable truth.
	fence, err := s.CancelProcess(ctx, ProcessLookupRequest{
		PersonalityAgentID:    paid,
		OperationID:           opID,
		OriginatingToolCallID: "job:delayed",
		TombstoneIfAbsent:     true,
	})
	if err != nil || !fence.Tombstone {
		t.Fatalf("fence: %+v %v", fence, err)
	}

	// Release the check — the delayed start resolves successfully, then
	// must replay the tombstone instead of journaling a launch.
	if err := os.WriteFile(filepath.Join(mount, ".release"), []byte("go"), 0644); err != nil {
		t.Fatal(err)
	}
	res := <-started
	if res.err != nil {
		t.Fatalf("released start must answer the tombstone, got %v", res.err)
	}
	if res.op.State != ProcessCancelled || !res.op.Tombstone {
		t.Fatalf("released start must replay the fence: %+v", res.op)
	}
	s.observeProcesses(ctx)
	if b.launches != 0 {
		t.Fatalf("no launch may follow a landed fence: %d", b.launches)
	}
}

// A start whose caller already gave up must not publish an operation: the
// context is honored once more before the journal write (the point of no
// return for the operation's existence).
func TestProcessStartHonorsClientCancellation(t *testing.T) {
	b := &processTestBackend{fakeBackend: newFakeBackend()}
	s, err := NewService(b, ServiceConfig{StateDirectory: t.TempDir() + "/state"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = s.StartProcess(ctx, ProcessStartRequest{
		PersonalityAgentID:    uuid.NewString(),
		OriginatingToolCallID: "job:dead-client",
		Executable:            "/bin/true",
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("dead client must not journal an operation: %v", err)
	}
	s.observeProcesses(context.Background())
	if b.launches != 0 {
		t.Fatalf("dead client's operation must never exist: %d launches", b.launches)
	}
}

// The journal section itself is part of the pre-journal window: a caller
// that expires while parked on the store mutex — or on a record mutex in
// the busy-scan — must still be turned away at the save boundary. Here an
// existing operation's record lock is held so the start blocks inside the
// journal section; its context is cancelled mid-wait; the save-boundary
// recheck must keep the expired caller from publishing a zombie record.
func TestProcessStartCancelledDuringJournalWait(t *testing.T) {
	b := &processTestBackend{fakeBackend: newFakeBackend()}
	s, err := NewService(b, ServiceConfig{StateDirectory: t.TempDir() + "/state"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// An existing non-terminal op whose record lock can gate the scan.
	p1 := uuid.NewString()
	op1, err := s.StartProcess(ctx, ProcessStartRequest{
		PersonalityAgentID:    p1,
		OriginatingToolCallID: "job:holder",
		Executable:            "/bin/true",
	})
	if err != nil {
		t.Fatalf("holder start: %v", err)
	}
	rec := s.processes.records[op1.OperationID]
	if rec == nil {
		t.Fatal("holder record missing")
	}

	p2 := uuid.NewString()
	op2 := ProcessOperationID(p2, "job:expired-waiter")
	rec.mu.Lock() // the waiter's busy-scan parks here, inside s.mu
	ctx2, cancel2 := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := s.StartProcess(ctx2, ProcessStartRequest{
			PersonalityAgentID:    p2,
			OriginatingToolCallID: "job:expired-waiter",
			Executable:            "/bin/true",
		})
		done <- err
	}()
	time.Sleep(100 * time.Millisecond) // let the waiter reach the scan
	cancel2()                          // client dies mid-journal-wait
	rec.mu.Unlock()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("expired waiter must not journal: %v", err)
	}
	if _, err := s.ProcessStatus(ctx, ProcessLookupRequest{
		PersonalityAgentID: p2, OperationID: op2,
	}); !errors.Is(err, ErrProcessNotFound) {
		t.Fatalf("expired waiter must leave no record: %v", err)
	}
}

// The recoverable half of the cancel fence: a caller that has
// re-authorized the launch (the termexec driver inside the persona
// launch fence, after a cancelled return) releases the tombstone and
// the same deterministic operation id launches normally. Only a
// never-launched tombstone is releasable — live ops, finished ops and
// ops that attempted a launch keep their history.
func TestReleaseProcessTombstoneRestoresLaunch(t *testing.T) {
	ctx := context.Background()
	b := &processTestBackend{fakeBackend: newFakeBackend()}
	directory := t.TempDir() + "/state"
	s, err := NewService(b, ServiceConfig{StateDirectory: directory})
	if err != nil {
		t.Fatal(err)
	}
	paid := uuid.NewString()
	opID := ProcessOperationID(paid, "term:released")

	// Absent: nothing to release.
	if _, err := s.ReleaseProcessTombstone(ctx, ProcessLookupRequest{
		PersonalityAgentID: paid, OperationID: opID,
	}); !errors.Is(err, ErrProcessNotFound) {
		t.Fatalf("release of absent op = %v, want not-found", err)
	}

	// Plant the fence exactly as the return gate does.
	if _, err := s.CancelProcess(ctx, ProcessLookupRequest{
		PersonalityAgentID: paid, OperationID: opID,
		OriginatingToolCallID: "term:released", TombstoneIfAbsent: true,
	}); err != nil {
		t.Fatalf("plant tombstone: %v", err)
	}
	if _, err := s.StartProcess(ctx, ProcessStartRequest{
		PersonalityAgentID: paid, OriginatingToolCallID: "term:released",
		Executable: "/bin/sh",
	}); err != nil {
		t.Fatalf("fenced replay: %v", err)
	}

	// Release under re-authorization: the fence is gone, a real start
	// journals and launches.
	if _, err := s.ReleaseProcessTombstone(ctx, ProcessLookupRequest{
		PersonalityAgentID: paid, OperationID: opID,
	}); err != nil {
		t.Fatalf("release tombstone: %v", err)
	}
	op, err := s.StartProcess(ctx, ProcessStartRequest{
		PersonalityAgentID: paid, OriginatingToolCallID: "term:released",
		Executable: "/bin/sh",
	})
	if err != nil || op.Tombstone || op.State != ProcessAccepted {
		t.Fatalf("start after release: %+v %v", op, err)
	}
	s.observeProcesses(ctx)
	if b.launches != 1 {
		t.Fatalf("released operation must launch: %d", b.launches)
	}

	// A live record is never releasable.
	if _, err := s.ReleaseProcessTombstone(ctx, ProcessLookupRequest{
		PersonalityAgentID: paid, OperationID: opID,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("release of live op = %v, want conflict", err)
	}

	// The release is durable: a restarted service has no record and a
	// replayed start would journal fresh — but the live op already
	// exists, so the restart sees the real record instead.
	s2, err := NewService(b, ServiceConfig{StateDirectory: directory})
	if err != nil {
		t.Fatal(err)
	}
	after, err := s2.ProcessStatus(ctx, ProcessLookupRequest{
		PersonalityAgentID: paid, OperationID: opID,
	})
	if err != nil || after.Tombstone {
		t.Fatalf("restarted service lost the launched op: %+v %v", after, err)
	}
}

// A failed journal removal must not strand the fence: the durable
// tombstone file is deleted before the in-memory record is unlinked, so
// a filesystem error leaves the release retriable and the delayed-start
// fence still enforced — never a record-less tombstone the next
// StartProcess could silently overwrite into a launch.
func TestReleaseProcessTombstoneRetainsFenceOnRemoveFailure(t *testing.T) {
	ctx := context.Background()
	b := &processTestBackend{fakeBackend: newFakeBackend()}
	directory := t.TempDir() + "/state"
	s, err := NewService(b, ServiceConfig{StateDirectory: directory})
	if err != nil {
		t.Fatal(err)
	}
	paid := uuid.NewString()
	opID := ProcessOperationID(paid, "term:stuck-fence")
	look := ProcessLookupRequest{PersonalityAgentID: paid, OperationID: opID}

	if _, err := s.CancelProcess(ctx, ProcessLookupRequest{
		PersonalityAgentID: paid, OperationID: opID,
		OriginatingToolCallID: "term:stuck-fence", TombstoneIfAbsent: true,
	}); err != nil {
		t.Fatalf("plant tombstone: %v", err)
	}

	// Force os.Remove to fail: replace the journal file with a non-empty
	// directory of the same name (ENOTEMPTY — works under any uid).
	journal := filepath.Join(directory, "processes", opID+".json")
	if err := os.Remove(journal); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(journal, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(journal, "occupant"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	if _, err := s.ReleaseProcessTombstone(ctx, look); err == nil {
		t.Fatal("release must fail while the journal file cannot be removed")
	}
	// The fence is retained in memory: the op is still reported as a
	// tombstone, a retried release reaches the same failure (not
	// not-found), and a delayed start still replays 'cancelled'.
	op, err := s.ProcessStatus(ctx, look)
	if err != nil || !op.Tombstone || op.State != ProcessCancelled {
		t.Fatalf("tombstone record lost on failed release: %+v %v", op, err)
	}
	if _, err := s.ReleaseProcessTombstone(ctx, look); err == nil || errors.Is(err, ErrProcessNotFound) {
		t.Fatalf("retried release must stay retriable, got %v", err)
	}
	replayed, err := s.StartProcess(ctx, ProcessStartRequest{
		PersonalityAgentID: paid, OriginatingToolCallID: "term:stuck-fence",
		Executable: "/bin/sh",
	})
	if err != nil || !replayed.Tombstone || replayed.State != ProcessCancelled {
		t.Fatalf("delayed start must still replay the fence: %+v %v", replayed, err)
	}
	s.observeProcesses(ctx)
	if b.launches != 0 {
		t.Fatalf("no launch may follow a failed release: %d", b.launches)
	}

	// Clear the obstacle: the retried release now removes the journal
	// (os.Remove empties the directory tree only for files — remove the
	// occupant first, then the directory, then let the release run).
	if err := os.RemoveAll(journal); err != nil {
		t.Fatal(err)
	}
	released, err := s.ReleaseProcessTombstone(ctx, look)
	if err != nil {
		t.Fatalf("release must succeed once the journal removes: %v", err)
	}
	if !released.Tombstone || released.State != ProcessCancelled {
		t.Fatalf("release returns the tombstone it retired: %+v", released)
	}
	op, err = s.StartProcess(ctx, ProcessStartRequest{
		PersonalityAgentID: paid, OriginatingToolCallID: "term:stuck-fence",
		Executable: "/bin/sh",
	})
	if err != nil || op.Tombstone || op.State != ProcessAccepted {
		t.Fatalf("start after a healed release must journal fresh: %+v %v", op, err)
	}
	s.observeProcesses(ctx)
	if b.launches != 1 {
		t.Fatalf("the re-authorized launch must run exactly once: %d", b.launches)
	}
}

// A tombstone planted and then released must fence again if a new cut
// arrives: release is not one-shot unlock, the next TombstoneIfAbsent
// cancel re-establishes it.
func TestReleaseProcessTombstoneRefences(t *testing.T) {
	ctx := context.Background()
	b := &processTestBackend{fakeBackend: newFakeBackend()}
	s, err := NewService(b, ServiceConfig{StateDirectory: t.TempDir() + "/state"})
	if err != nil {
		t.Fatal(err)
	}
	paid := uuid.NewString()
	opID := ProcessOperationID(paid, "term:refence")
	look := ProcessLookupRequest{PersonalityAgentID: paid, OperationID: opID}

	if _, err := s.CancelProcess(ctx, ProcessLookupRequest{
		PersonalityAgentID: paid, OperationID: opID,
		OriginatingToolCallID: "term:refence", TombstoneIfAbsent: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReleaseProcessTombstone(ctx, look); err != nil {
		t.Fatal(err)
	}
	// Re-fence (a second seal): late starts replay cancelled again.
	if _, err := s.CancelProcess(ctx, ProcessLookupRequest{
		PersonalityAgentID: paid, OperationID: opID,
		OriginatingToolCallID: "term:refence", TombstoneIfAbsent: true,
	}); err != nil {
		t.Fatal(err)
	}
	op, err := s.StartProcess(ctx, ProcessStartRequest{
		PersonalityAgentID: paid, OriginatingToolCallID: "term:refence",
		Executable: "/bin/sh",
	})
	if err != nil || op.State != ProcessCancelled || !op.Tombstone {
		t.Fatalf("re-fenced start: %+v %v", op, err)
	}
	s.observeProcesses(ctx)
	if b.launches != 0 {
		t.Fatalf("re-fenced op launched: %d", b.launches)
	}
}
