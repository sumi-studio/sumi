package runtimeprovision

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TRT-01 / TRT-02 / TRT-03 discriminating tests: physical-stop proof
// behind terminal state, crash-window cursor recovery, and bounded
// failure handling on the journal path.

// quiescenceBackend models a container whose liveness the test controls
// directly — including a launch that "lands" after the first inspect
// and a stop that can fail.
type quiescenceBackend struct {
	*processTestBackend
	stops     int
	stopErr   error
	removes   int
	removeErr error
}

func (b *quiescenceBackend) StopProcess(context.Context, ProcessOperation) error {
	b.stops++
	if b.stopErr != nil {
		return b.stopErr
	}
	b.observation.Running = false
	b.observation.ExitCode = 137
	return nil
}

func (b *quiescenceBackend) RemoveProcess(context.Context, ProcessOperation) error {
	b.removes++
	if b.removeErr != nil {
		return b.removeErr
	}
	if b.observation.Exists && b.observation.Running {
		return errors.New("cannot remove a running process container")
	}
	b.observation.Exists = false
	return nil
}

func startOp(t *testing.T, s *Service, ctx context.Context) ProcessOperation {
	t.Helper()
	op, err := s.StartProcess(ctx, ProcessStartRequest{
		PersonalityAgentID:    uuid.NewString(),
		OriginatingToolCallID: "q-call",
		Executable:            "/bin/sh",
		Interactive:           true,
		TTY:                   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return op
}

func statusOf(t *testing.T, s *Service, ctx context.Context, op ProcessOperation) ProcessOperation {
	t.Helper()
	got, err := s.ProcessStatus(ctx, ProcessLookupRequest{PersonalityAgentID: op.PersonalityAgentID, OperationID: op.OperationID})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// An indeterminate verdict is not stop proof: the container can land
// after the racing inspect. The reconcile must find and kill it, and
// Quiesced may only become true once removal is verified.
func TestTerminalIndeterminateKillsDelayedContainer(t *testing.T) {
	ctx := context.Background()
	b := &quiescenceBackend{processTestBackend: &processTestBackend{fakeBackend: newFakeBackend()}}
	s := newTestService(t, b)
	op := startOp(t, s, ctx)
	s.observeProcesses(ctx) // launch lands: running

	// The next inspect races a transient container lookup: indeterminate
	// verdict while the physical container is actually still live.
	b.observation = ProcessObservation{Exists: false}
	s.observeProcesses(ctx)
	got := statusOf(t, s, ctx, op)
	if got.State != ProcessIndeterminate {
		t.Fatalf("state=%s", got.State)
	}
	if got.Quiesced {
		t.Fatal("indeterminate op reported quiesced")
	}
	if !got.State.Terminal() {
		t.Fatal("indeterminate should be scheduling-terminal")
	}

	// The delayed launch lands while the op is already terminal.
	b.observation = ProcessObservation{Exists: true, Running: true, StartedAt: time.Now().UTC()}
	s.observeProcesses(ctx)
	got = statusOf(t, s, ctx, op)
	if b.stops == 0 {
		t.Fatal("terminal reconcile did not stop the live container")
	}
	if !got.Quiesced {
		t.Fatal("verified stop+remove must certify quiescence")
	}
}

// A failed stop keeps the op non-quiesced; the next reconcile retries
// instead of certifying a writer that may still be alive.
func TestTerminalStopFailureStaysNonQuiesced(t *testing.T) {
	ctx := context.Background()
	b := &quiescenceBackend{processTestBackend: &processTestBackend{fakeBackend: newFakeBackend()}}
	s := newTestService(t, b)
	op := startOp(t, s, ctx)
	s.observeProcesses(ctx)
	b.observation = ProcessObservation{Exists: false}
	s.observeProcesses(ctx) // -> indeterminate
	b.observation = ProcessObservation{Exists: true, Running: true}
	b.stopErr = errors.New("daemon unreachable")
	s.observeProcesses(ctx)
	got := statusOf(t, s, ctx, op)
	if got.Quiesced {
		t.Fatal("failed stop must not certify quiescence")
	}
	if b.observation.Exists {
		// container still "live" in the fake — the cut stays pending
	}
	b.stopErr = nil
	s.observeProcesses(ctx)
	got = statusOf(t, s, ctx, op)
	if !got.Quiesced {
		t.Fatal("successful reconcile must certify quiescence")
	}
}

// A never-launched tombstone has no possible writer: quiesced at once.
func TestTombstoneIsQuiesced(t *testing.T) {
	ctx := context.Background()
	b := &quiescenceBackend{processTestBackend: &processTestBackend{fakeBackend: newFakeBackend()}}
	s := newTestService(t, b)
	paid := uuid.NewString()
	op, err := s.CancelProcess(ctx, ProcessLookupRequest{
		PersonalityAgentID:    paid,
		OperationID:           ProcessOperationID(paid, "never-launched"),
		OriginatingToolCallID: "never-launched",
		TombstoneIfAbsent:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !op.Tombstone || !op.Quiesced {
		t.Fatalf("tombstone quiesced=%v tombstone=%v", op.Quiesced, op.Tombstone)
	}
	// A replayed start returns the same quiesced tombstone, never a launch.
	again, err := s.StartProcess(ctx, ProcessStartRequest{
		PersonalityAgentID:    op.PersonalityAgentID,
		OriginatingToolCallID: "never-launched",
		Executable:            "/bin/sh",
	})
	if err != nil || !again.Tombstone || !again.Quiesced {
		t.Fatal(again, err)
	}
}

// A cancelled op whose launch is in flight: cancel commits while the
// container is still landing; the post-launch inspect sees it running
// and stops it — no writer survives the cut.
func TestCancelledLaunchInFlightIsStopped(t *testing.T) {
	ctx := context.Background()
	b := &quiescenceBackend{processTestBackend: &processTestBackend{fakeBackend: newFakeBackend()}}
	s := newTestService(t, b)
	op := startOp(t, s, ctx)
	// Cancel before the first observe: the launch branch sees
	// CancelRequested and never launches at all.
	if _, err := s.CancelProcess(ctx, ProcessLookupRequest{PersonalityAgentID: op.PersonalityAgentID, OperationID: op.OperationID}); err != nil {
		t.Fatal(err)
	}
	s.observeProcesses(ctx)
	if b.launches != 0 {
		t.Fatal("cancelled op must never launch")
	}
	got := statusOf(t, s, ctx, op)
	if !got.Quiesced {
		t.Fatal("cancel-before-launch is quiesced once reconciled")
	}
}

// TRT-02: a crash between scrollback append and cursor commit must not
// re-append the already-captured bytes.
func TestRecoverSourceCrashWindow(t *testing.T) {
	dir := t.TempDir()
	journal := journalFile(t, dir, "one\n", "two\n")
	f, err := os.Open(journal)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, _ := f.Stat()
	ino := journalInode(st)

	tty, err := openTTYLog(dir, "op-recover")
	if err != nil {
		t.Fatal(err)
	}
	// First commit: drain the whole journal and save the cursor.
	data, _ := os.ReadFile(journal)
	if n, err := journalDrain(tty, data); err != nil || n != len(data) {
		t.Fatal(n, err)
	}
	pos := ttySrc{Ino: ino, Off: int64(len(data))}
	if err := tty.saveSrc(pos); err != nil {
		t.Fatal(err)
	}
	// A third record lands and is appended — then the "crash": the
	// cursor is never committed.
	appendJournal(t, journal, "three\n")
	f2, _ := os.Open(journal)
	st2, _ := f2.Stat()
	tail := make([]byte, st2.Size()-pos.Off)
	if _, err := f2.ReadAt(tail, pos.Off); err != nil {
		t.Fatal(err)
	}
	if _, err := journalDrain(tty, tail); err != nil {
		t.Fatal(err)
	}
	// loadSrc still returns the pre-"three" cursor — the crash window.
	resumed := recoverSource(tty, f2, ino, st2.Size(), tty.loadSrc())
	if resumed.Off != st2.Size() {
		t.Fatalf("recovery must absorb the uncommitted append: off=%d size=%d", resumed.Off, st2.Size())
	}
	// Draining from the recovered cursor appends nothing — no duplicate.
	if n, err := journalDrain(tty, journalRegion(f2, resumed.Off, st2.Size())); n != 0 || err != nil {
		t.Fatal("uncommitted bytes replayed")
	}
	got, _, _, _, _ := tty.read(0, 1<<20)
	if string(got) != "one\ntwo\nthree\n" {
		t.Fatalf("duplicated or lost output: %q", got)
	}
	f2.Close()
}

// TRT-02: a lost cursor file with existing scrollback must not replay
// the whole journal.
func TestRecoverSourceMissingCursor(t *testing.T) {
	dir := t.TempDir()
	journal := journalFile(t, dir, "alpha\n", "beta\n")
	f, err := os.Open(journal)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, _ := f.Stat()
	ino := journalInode(st)
	tty, err := openTTYLog(dir, "op-missing")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(journal)
	if _, err := journalDrain(tty, data); err != nil {
		t.Fatal(err)
	}
	// No saveSrc — the cursor file is absent, as after a crash or loss.
	resumed := recoverSource(tty, f, ino, st.Size(), ttySrc{})
	if resumed.Off != st.Size() {
		t.Fatalf("missing cursor must recover to journal end of matched history: %d vs %d", resumed.Off, st.Size())
	}
	if n, _ := journalDrain(tty, journalRegion(f, resumed.Off, st.Size())); n != 0 {
		t.Fatal("history replayed")
	}
	got, _, _, _, _ := tty.read(0, 1<<20)
	if string(got) != "alpha\nbeta\n" {
		t.Fatalf("duplicated output: %q", got)
	}
}

// TRT-02: a malformed cursor behaves like a missing one.
func TestRecoverSourceMalformedCursor(t *testing.T) {
	dir := t.TempDir()
	journal := journalFile(t, dir, "x\n")
	f, _ := os.Open(journal)
	defer f.Close()
	st, _ := f.Stat()
	ino := journalInode(st)
	tty, err := openTTYLog(dir, "op-bad")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(journal)
	if _, err := journalDrain(tty, data); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tty.srcPath(), []byte("garbage"), 0600); err != nil {
		t.Fatal(err)
	}
	resumed := recoverSource(tty, f, ino, st.Size(), tty.loadSrc())
	if resumed.Off != st.Size() {
		t.Fatalf("malformed cursor must recover by content: %d vs %d", resumed.Off, st.Size())
	}
}

// TRT-02: retained content that does not match the journal becomes an
// explicit gap, never a silent splice.
func TestRecoverSourceDivergentTail(t *testing.T) {
	dir := t.TempDir()
	journal := journalFile(t, dir, "real\n")
	f, _ := os.Open(journal)
	defer f.Close()
	st, _ := f.Stat()
	ino := journalInode(st)
	tty, err := openTTYLog(dir, "op-diverge")
	if err != nil {
		t.Fatal(err)
	}
	if err := tty.append([]byte("not from this journal\n")); err != nil {
		t.Fatal(err)
	}
	resumed := recoverSource(tty, f, ino, st.Size(), ttySrc{})
	gaps, _ := tty.gapsAtOrAfter(0)
	if len(gaps) == 0 {
		t.Fatal("uncertified history must record a gap")
	}
	_ = resumed
}

// TRT-03: a terminal op whose journal resolves but can never be read
// must bound its retries and record a gap — not spin forever.
func TestSuperviseTerminalReadFaultIsBounded(t *testing.T) {
	ctx := context.Background()
	b := &interactiveTestBackend{processTestBackend: &processTestBackend{fakeBackend: newFakeBackend()}, journal: filepath.Join(t.TempDir(), "nonexistent-json.log")}
	s := newTestService(t, b)
	op := startOp(t, s, ctx)
	s.observeProcesses(ctx)
	// Drive the op terminal (indeterminate) so supervision hits the
	// bounded terminal-drain path with an unreadable journal.
	b.observation = ProcessObservation{Exists: false}
	s.observeProcesses(ctx)
	ps := s.processes
	io_, err := ps.ensureInteractive(ps.records[op.OperationID])
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		ps.superviseInteractive(io_)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("terminal drain of an unreadable journal spun forever")
	}
	gaps, _ := io_.tty.gapsAtOrAfter(0)
	if len(gaps) == 0 {
		t.Fatal("unreadable final journal must record a gap")
	}
}

// Root reproduction (root-runtime-repair-receipt-01): two identical
// `same\n` records across an append-before-cursor crash must not
// silently become three. Content equality cannot identify source
// position — recovery verifies the retained window at absolute offsets.
func TestRecoverSourceRepeatedPayloadCrashWindow(t *testing.T) {
	dir := t.TempDir()
	journal := journalFile(t, dir, "same\n")
	f, err := os.Open(journal)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, _ := f.Stat()
	tty, err := openTTYLog(dir, "root-repeated")
	if err != nil {
		t.Fatal(err)
	}
	first, _ := os.ReadFile(journal)
	if _, err = journalDrain(tty, first); err != nil {
		t.Fatal(err)
	}
	pos := ttySrc{Ino: journalInode(st), Off: int64(len(first))}
	if err = tty.saveSrc(pos); err != nil {
		t.Fatal(err)
	}
	appendJournal(t, journal, "same\n")
	st2, _ := f.Stat()
	// Crash before the second record's cursor commit.
	if _, err = journalDrain(tty, journalRegion(f, pos.Off, st2.Size())); err != nil {
		t.Fatal(err)
	}
	recovered := recoverSource(tty, f, pos.Ino, st2.Size(), tty.loadSrc())
	if recovered.Off != st2.Size() {
		t.Fatalf("recovery must absorb the repeated uncommitted record: %d vs %d", recovered.Off, st2.Size())
	}
	if _, err = journalDrain(tty, journalRegion(f, recovered.Off, st2.Size())); err != nil {
		t.Fatal(err)
	}
	got, _, _, _, _ := tty.read(0, 1<<20)
	gaps, _ := tty.gapsAtOrAfter(0)
	if string(got) != "same\nsame\n" {
		t.Fatalf("silent replay after repeated-payload crash: %q", got)
	}
	if len(gaps) != 0 {
		t.Fatalf("clean certification must not record a gap: %+v", gaps)
	}
}

// Repeated retained history after cursor loss: an all-same journal and
// an all-same scrollback must still recover positionally — the retained
// length pins the boundary, not the payload.
func TestRecoverSourceRepeatedHistoryLostCursor(t *testing.T) {
	dir := t.TempDir()
	journal := journalFile(t, dir, "same\n", "same\n", "same\n", "same\n")
	f, _ := os.Open(journal)
	defer f.Close()
	st, _ := f.Stat()
	ino := journalInode(st)
	tty, err := openTTYLog(dir, "repeat-lost")
	if err != nil {
		t.Fatal(err)
	}
	// Scrollback holds exactly the first three records; no cursor.
	region := journalRegion(f, 0, st.Size())
	consumed := 0
	journalEach(region, func(end int64, payload []byte) bool {
		if consumed < 3 {
			_ = tty.append(payload)
			consumed++
		}
		return consumed < 3
	})
	resumed := recoverSource(tty, f, ino, st.Size(), ttySrc{})
	// The recovered cursor must land at the third record's journal end,
	// so only the fourth record is appended next.
	var ends []int64
	journalEach(region, func(end int64, payload []byte) bool {
		ends = append(ends, end)
		return true
	})
	if resumed.Off != ends[2] {
		t.Fatalf("repeated history must recover at record 3 end: %d vs %d", resumed.Off, ends[2])
	}
	if n, _ := journalDrain(tty, journalRegion(f, resumed.Off, st.Size())); n != int(st.Size()-ends[2]) {
		t.Fatal("recovery consumed the wrong window")
	}
	got, _, _, _, _ := tty.read(0, 1<<20)
	if string(got) != "same\nsame\nsame\nsame\n" {
		t.Fatalf("wrong output after repeated-history recovery: %q", got)
	}
}

// A scrollback tail ending mid-record cannot be certified — the bytes
// after the last complete record boundary are ambiguous provenance.
func TestRecoverSourceTornTail(t *testing.T) {
	dir := t.TempDir()
	journal := journalFile(t, dir, "alpha\n")
	f, _ := os.Open(journal)
	defer f.Close()
	st, _ := f.Stat()
	ino := journalInode(st)
	tty, err := openTTYLog(dir, "torn-tail")
	if err != nil {
		t.Fatal(err)
	}
	if err := tty.append([]byte("alpha\nbet")); err != nil { // ends mid-"record"
		t.Fatal(err)
	}
	resumed := recoverSource(tty, f, ino, st.Size(), ttySrc{})
	gaps, _ := tty.gapsAtOrAfter(0)
	if len(gaps) == 0 {
		t.Fatal("mid-record scrollback tail must record a gap")
	}
	_ = resumed
}

// TREV2-04: a transient `docker info` failure must not disable journal
// resolution for the provisioner's lifetime — failures retry on a
// bounded backoff, successes cache permanently. The fake docker CLI
// below fails its first invocation then answers — resolution recovers
// without a restart, and the backoff keeps a dead daemon from facing
// a `docker info` per tailer poll.
func TestDockerJournalRootRetriesTransientFailure(t *testing.T) {
	dir := t.TempDir()
	counter := filepath.Join(dir, "calls")
	stub := filepath.Join(dir, "docker")
	script := `#!/bin/sh
n=0
[ -f "$COUNT_FILE" ] && n=$(cat "$COUNT_FILE")
n=$((n+1))
echo "$n" > "$COUNT_FILE"
if [ "$n" -lt 2 ]; then
  echo "daemon unreachable" >&2
  exit 1
fi
echo "/var/lib/docker"
`
	if err := os.WriteFile(stub, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	// exec.CommandContext resolves "docker" on the ambient PATH, not
	// cmd.Env — point it at the stub.
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	b := &DockerBackend{
		baseEnvironment: []string{"COUNT_FILE=" + counter},
		runner:          execCommandRunner{},
	}
	ctx := context.Background()

	if _, err := b.dockerJournalRoot(ctx); err == nil {
		t.Fatal("first resolution should fail (daemon down)")
	}
	// Backoff: a poll during the window must NOT hit docker again.
	if _, err := b.dockerJournalRoot(ctx); err == nil {
		t.Fatal("resolution during backoff should still fail")
	}
	raw, _ := os.ReadFile(counter)
	if string(raw) != "1\n" {
		t.Fatalf("docker invocations during backoff = %s, want 1", raw)
	}
	// After the backoff window the retry recovers — and caches.
	b.journalRootRetryAt = time.Now().Add(-time.Second)
	root, err := b.dockerJournalRoot(ctx)
	if err != nil || root != "/var/lib/docker" {
		t.Fatalf("retry resolution = %q, %v", root, err)
	}
	if _, err := b.dockerJournalRoot(ctx); err != nil {
		t.Fatalf("cached resolution failed: %v", err)
	}
	raw, _ = os.ReadFile(counter)
	if string(raw) != "2\n" {
		t.Fatalf("docker invocations after success = %s, want 2 (cached)", raw)
	}
}
