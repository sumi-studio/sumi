package runtimeprovision

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Interactive-operation tests: the journal tailer's durable resume
// (no duplicate scrollback, explicit gap on rotation), torn-write
// safety, input delivery receipts, and signal/resize plumbing — all
// without Docker, using a backend that serves a real journal file.

type interactiveTestBackend struct {
	*processTestBackend
	journal string
	jErr    error
	sink    *processSink
	sinkErr error
	signals []string
	resizes [][2]int
}

func (b *interactiveTestBackend) ProcessJournalPath(context.Context, ProcessOperation) (string, error) {
	return b.journal, b.jErr
}
func (b *interactiveTestBackend) OpenProcessInput(context.Context, ProcessOperation) (*processSink, error) {
	if b.sinkErr != nil {
		return nil, b.sinkErr
	}
	return b.sink, nil
}
func (b *interactiveTestBackend) SignalProcess(_ context.Context, _ ProcessOperation, sig string) error {
	b.signals = append(b.signals, sig)
	return nil
}
func (b *interactiveTestBackend) ResizeProcess(_ context.Context, _ ProcessOperation, cols, rows int) error {
	b.resizes = append(b.resizes, [2]int{cols, rows})
	return nil
}

// scriptedSink is a WriteCloser that can fail after N bytes to
// simulate a mid-stream attach death.
type scriptedSink struct {
	bytes.Buffer
	failAfter int
	written   int
	closed    bool
}

func (w *scriptedSink) Write(p []byte) (int, error) {
	if w.failAfter >= 0 && w.written+len(p) > w.failAfter {
		n := w.failAfter - w.written
		if n < 0 {
			n = 0
		}
		_, _ = w.Buffer.Write(p[:n])
		w.written += n
		return n, errors.New("attach stream died")
	}
	w.written += len(p)
	return w.Buffer.Write(p)
}
func (w *scriptedSink) Close() error { w.closed = true; return nil }

func interactiveOp(t *testing.T) ProcessOperation {
	t.Helper()
	return ProcessOperation{
		OperationID:        fmt.Sprintf("op-%s", uuid.NewString()),
		PersonalityAgentID: uuid.NewString(),
		State:              ProcessRunning,
		Interactive:        true,
		TTY:                true,
	}
}

func journalFile(t *testing.T, dir string, lines ...string) string {
	t.Helper()
	p := filepath.Join(dir, "journal.json.log")
	var buf bytes.Buffer
	for _, l := range lines {
		fmt.Fprintf(&buf, `{"log":%s,"stream":"stdout","time":"2026-09-19T00:00:00Z"}`+"\n", mustJSON(t, l))
	}
	if err := os.WriteFile(p, buf.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	return p
}

func mustJSON(t *testing.T, s string) string {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func appendJournal(t *testing.T, path, s string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fmt.Fprintf(f, `{"log":%s,"stream":"stdout","time":"2026-09-19T00:00:01Z"}`+"\n", mustJSON(t, s))
}

func newInteractiveIO(t *testing.T, s *processStore, op ProcessOperation) *interactiveIO {
	t.Helper()
	tty, err := openTTYLog(s.directory, op.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	return &interactiveIO{op: op, tty: tty, done: make(chan struct{})}
}

func readAllTTY(t *testing.T, tty *ttyLog) string {
	t.Helper()
	data, _, _, _, err := tty.read(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestProcessStartInteractiveRequiresTTY(t *testing.T) {
	r := ProcessStartRequest{
		PersonalityAgentID:    uuid.NewString(),
		OriginatingToolCallID: "call-1",
		Executable:            "/bin/bash",
		Interactive:           true,
	}
	if err := r.Validate(); !errors.Is(err, ErrInvalidProcessRequest) {
		t.Fatalf("interactive without tty err = %v, want ErrInvalidProcessRequest", err)
	}
	r.TTY = true
	if err := r.Validate(); err != nil {
		t.Fatalf("interactive with tty: %v", err)
	}
}

// The durable-source guarantee: a pump that dies and is recreated
// resumes at the committed journal offset — no emitted byte replays
// into duplicate scrollback.
func TestInteractiveJournalResumeNoDuplicate(t *testing.T) {
	dir := t.TempDir()
	journal := journalFile(t, dir, "first\r\n", "second\r\n")
	b := &interactiveTestBackend{processTestBackend: &processTestBackend{fakeBackend: newFakeBackend()}, journal: journal}
	s, err := newProcessStore(dir+"/state", b)
	if err != nil {
		t.Fatal(err)
	}
	op := interactiveOp(t)

	io1 := newInteractiveIO(t, s, op)
	if !s.pumpJournal(io1, journal, false) {
		t.Fatal("first drain should complete for a terminal op")
	}
	if got := readAllTTY(t, io1.tty); got != "first\r\nsecond\r\n" {
		t.Fatalf("tty = %q", got)
	}

	// Emit more, then a NEW supervisor (the restart case) resumes.
	appendJournal(t, journal, "third\r\n")
	io2 := newInteractiveIO(t, s, op)
	if !s.pumpJournal(io2, journal, false) {
		t.Fatal("resume drain should complete")
	}
	got := readAllTTY(t, io2.tty)
	if got != "first\r\nsecond\r\nthird\r\n" {
		t.Fatalf("resumed tty = %q — bytes must appear exactly once", got)
	}
}

// Journal rotation or truncation: the tailer cannot recover bytes
// between the committed offset and the old file's end — it records an
// explicit gap rather than skipping or replaying.
func TestInteractiveJournalRotationGap(t *testing.T) {
	dir := t.TempDir()
	journal := journalFile(t, dir, "old\r\n")
	b := &interactiveTestBackend{processTestBackend: &processTestBackend{fakeBackend: newFakeBackend()}, journal: journal}
	s, err := newProcessStore(dir+"/state", b)
	if err != nil {
		t.Fatal(err)
	}
	op := interactiveOp(t)
	io_ := newInteractiveIO(t, s, op)
	if !s.pumpJournal(io_, journal, false) {
		t.Fatal("first drain")
	}

	// Rotate: replace the file (new inode) with fresh content.
	rotated := filepath.Join(dir, "journal.json.log.1")
	if err := os.Rename(journal, rotated); err != nil {
		t.Fatal(err)
	}
	fresh := journalFile(t, dir, "new-era\r\n")
	if !s.pumpJournal(io_, fresh, false) {
		t.Fatal("post-rotation drain")
	}
	gaps, err := io_.tty.gapsAtOrAfter(0)
	if err != nil || len(gaps) != 1 {
		t.Fatalf("gaps = %v err=%v, want exactly one rotation gap", gaps, err)
	}
	if got := readAllTTY(t, io_.tty); got != "old\r\nnew-era\r\n" {
		t.Fatalf("tty = %q — retained bytes plus new era, no replay", got)
	}
}

// A torn trailing journal record is never committed at a partial
// offset — it completes on the next pass exactly once.
func TestInteractiveJournalPartialLine(t *testing.T) {
	dir := t.TempDir()
	journal := journalFile(t, dir, "complete\r\n")
	f, err := os.OpenFile(journal, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"log":"par`); err != nil {
		t.Fatal(err)
	}
	f.Close()

	b := &interactiveTestBackend{processTestBackend: &processTestBackend{fakeBackend: newFakeBackend()}, journal: journal}
	s, err := newProcessStore(dir+"/state", b)
	if err != nil {
		t.Fatal(err)
	}
	op := interactiveOp(t)
	io_ := newInteractiveIO(t, s, op)
	if !s.pumpJournal(io_, journal, false) {
		t.Fatal("drain")
	}
	if got := readAllTTY(t, io_.tty); got != "complete\r\n" {
		t.Fatalf("tty = %q, torn record must not be committed", got)
	}
	// Finish the torn record; the resume commits it once.
	f2, err := os.OpenFile(journal, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f2.WriteString(`tial\r\n","stream":"stdout","time":"2026-09-19T00:00:02Z"}` + "\n"); err != nil {
		t.Fatal(err)
	}
	f2.Close()
	io2 := newInteractiveIO(t, s, op)
	if !s.pumpJournal(io2, journal, false) {
		t.Fatal("second drain")
	}
	if got := readAllTTY(t, io2.tty); got != "complete\r\npartial\r\n" {
		t.Fatalf("tty = %q", got)
	}
}

// Input delivery receipts: a closed attach proves nothing was sent;
// a full write is delivered; a mid-write death is indeterminate —
// the caller's honest 'unknown', never a resent duplicate.
func TestInteractiveInputReceipts(t *testing.T) {
	op := interactiveOp(t)
	io_ := &interactiveIO{op: op, done: make(chan struct{})}
	ctx := context.Background()

	// Attach never opens → provably nothing sent.
	closed := &interactiveTestBackend{sinkErr: ErrAttachUnavailable}
	if r := io_.writeInput(ctx, closed, []byte("ls\n"), false); r.Delivered || r.Indeterminate {
		t.Fatalf("closed-attach receipt = %+v, want not-delivered definite", r)
	}

	// Full write → delivered.
	good := &scriptedSink{failAfter: -1}
	b := &interactiveTestBackend{sink: &processSink{WriteCloser: good}}
	if r := io_.writeInput(ctx, b, []byte("ls\n"), false); !r.Delivered {
		t.Fatalf("full write receipt = %+v, want delivered", r)
	}
	if good.String() != "ls\n" {
		t.Fatalf("sink got %q", good.String())
	}

	// Mid-write death → indeterminate, and the reattach is NOT
	// retried blindly (the prefix may already be typed).
	io2 := &interactiveIO{op: op, done: make(chan struct{})}
	partial := &scriptedSink{failAfter: 2}
	b2 := &interactiveTestBackend{sink: &processSink{WriteCloser: partial}}
	if r := io2.writeInput(ctx, b2, []byte("long-input\n"), false); !r.Indeterminate {
		t.Fatalf("mid-write receipt = %+v, want indeterminate", r)
	}
	if partial.String() != "lo" {
		t.Fatalf("sink prefix = %q", partial.String())
	}
}

// EOF on a TTY is the real line-discipline EOT — the byte the
// foreground reader sees as end-of-input.
func TestInteractiveEOFIsEOTByte(t *testing.T) {
	op := interactiveOp(t)
	sink := &scriptedSink{failAfter: -1}
	b := &interactiveTestBackend{sink: &processSink{WriteCloser: sink}}
	io_ := &interactiveIO{op: op, done: make(chan struct{})}
	r := io_.writeInput(context.Background(), b, nil, true)
	if !r.Delivered {
		t.Fatalf("eof receipt = %+v", r)
	}
	if !bytes.Equal(sink.Bytes(), []byte{0x04}) {
		t.Fatalf("eof bytes = %x, want 04 (EOT)", sink.Bytes())
	}
}

// Signal and resize ride the backend's verified foreground-group
// delivery; the service returns delivery evidence, not just a bare
// error.
func TestInteractiveSignalAndResizeService(t *testing.T) {
	b := &interactiveTestBackend{processTestBackend: &processTestBackend{fakeBackend: newFakeBackend()}}
	svc := newTestService(t, b)
	ctx := context.Background()
	op, err := svc.StartProcess(ctx, ProcessStartRequest{
		PersonalityAgentID:    uuid.NewString(),
		OriginatingToolCallID: "term-test",
		Executable:            "/bin/bash",
		Interactive:           true,
		TTY:                   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	lookup := ProcessLookupRequest{PersonalityAgentID: op.PersonalityAgentID, OperationID: op.OperationID}
	receipt, err := svc.SignalProcess(ctx, ProcessSignalRequest{ProcessLookupRequest: lookup, Signal: "TERM"})
	if err != nil || !receipt.Delivered {
		t.Fatalf("signal receipt = %+v err=%v", receipt, err)
	}
	if len(b.signals) != 1 || b.signals[0] != "TERM" {
		t.Fatalf("backend signals = %v", b.signals)
	}
	if _, err := svc.SignalProcess(ctx, ProcessSignalRequest{ProcessLookupRequest: lookup, Signal: "SEGV"}); err == nil {
		t.Fatal("unallowlisted signal must be refused")
	}
	if _, err := svc.ResizeProcess(ctx, ProcessResizeRequest{ProcessLookupRequest: lookup, Cols: 120, Rows: 40}); err != nil {
		t.Fatalf("resize: %v", err)
	}
	if len(b.resizes) != 1 || b.resizes[0] != [2]int{120, 40} {
		t.Fatalf("resizes = %v", b.resizes)
	}
	// A non-interactive op cannot be signalled through this path.
	op2, err := svc.StartProcess(ctx, ProcessStartRequest{
		PersonalityAgentID:    op.PersonalityAgentID,
		OriginatingToolCallID: "batch-test",
		Executable:            "true",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.SignalProcess(ctx, ProcessSignalRequest{
		ProcessLookupRequest: ProcessLookupRequest{PersonalityAgentID: op2.PersonalityAgentID, OperationID: op2.OperationID},
		Signal:               "TERM",
	}); !errors.Is(err, ErrProcessNotInteractive) {
		t.Fatalf("batch signal err = %v, want ErrProcessNotInteractive", err)
	}
}

// The supervisor drains a terminal op's journal to stability — final
// bytes written around the exit are captured, not abandoned.
func TestInteractiveTerminalDrain(t *testing.T) {
	dir := t.TempDir()
	journal := journalFile(t, dir, "early\r\n")
	b := &interactiveTestBackend{processTestBackend: &processTestBackend{fakeBackend: newFakeBackend()}, journal: journal}
	s, err := newProcessStore(dir+"/state", b)
	if err != nil {
		t.Fatal(err)
	}
	op := interactiveOp(t)
	io_ := newInteractiveIO(t, s, op)
	rec := &processRecord{mu: &sync.Mutex{}, Operation: op}
	s.mu.Lock()
	s.records[op.OperationID] = rec
	s.mu.Unlock()

	done := make(chan bool, 1)
	go func() { done <- s.pumpJournal(io_, journal, true) }()
	// Late bytes arrive while the pump thinks the op is still live;
	// then the op goes terminal and the drain captures them.
	time.Sleep(200 * time.Millisecond)
	appendJournal(t, journal, "last-gasp\r\n")
	rec.mu.Lock()
	rec.Operation.State = ProcessSucceeded
	rec.mu.Unlock()
	select {
	case drained := <-done:
		if !drained {
			t.Fatal("terminal drain should report complete")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("terminal drain did not finish")
	}
	if got := readAllTTY(t, io_.tty); got != "early\r\nlast-gasp\r\n" {
		t.Fatalf("tty = %q", got)
	}
}

// waitTTY polls the durable tty log until want appears or the deadline
// passes, returning everything captured so far.
func waitTTY(t *testing.T, tty *ttyLog, want string, d time.Duration) string {
	t.Helper()
	return waitTTYCount(t, tty, want, 1, d)
}

// waitTTYCount is waitTTY requiring n occurrences — echoed input and
// command output often share the marker text, so execution proofs
// wait for the count the transcript actually produces.
func waitTTYCount(t *testing.T, tty *ttyLog, want string, n int, d time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		data, _, _, _, err := tty.read(0, 1<<20)
		if err == nil && strings.Count(string(data), want) >= n {
			return string(data)
		}
		time.Sleep(200 * time.Millisecond)
	}
	data, _, _, _, _ := tty.read(0, 1<<20)
	return string(data)
}

// TestInteractiveDockerE2E exercises the real interactive path against
// the host daemon: one bash PTY container, real `docker attach` stdin,
// the real json-file journal tailer, daemon-API resize, the host-side
// foreground-process-group signal path, named-workspace persistence, and
// physical stop on cancel. It only runs when SUMI_TEST_DOCKER_E2E=1 in a
// fixture that mounts the docker socket, the docker data root, and the
// host pid namespace (the signal path resolves host /proc entries).
func TestInteractiveDockerE2E(t *testing.T) {
	if os.Getenv("SUMI_TEST_DOCKER_E2E") != "1" {
		t.Skip("set SUMI_TEST_DOCKER_E2E=1 with docker socket, data-root and host-pid access")
	}
	tag := os.Getenv("SUMI_TEST_DOCKER_JOB_TAG")
	if tag == "" {
		tag = "899a7cdf1defdf9ce09d76cccaea48bf5f58a20d"
	}
	docker := func(args ...string) string {
		cmd := exec.Command("docker", args...)
		cmd.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "DOCKER_HOST=unix:///var/run/docker.sock"}
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
		}
		return strings.TrimSpace(string(out))
	}
	persona := uuid.NewString()
	agent32 := strings.ReplaceAll(persona, "-", "")
	volume := "sumi-" + agent32 + "_workspace"
	project := "sumi-" + agent32
	docker("volume", "create", "--label", "com.docker.compose.project="+project, "--label", "com.docker.compose.volume=workspace", volume)
	// Production seeds a fresh workspace volume through the agent
	// container's ordinary mount: the image's /workspace is drwxrwxrwx,
	// and only that seed makes it writable for the hardened uid-10002
	// ops container (which mounts volume-nocopy). Emulate the seed so
	// the fixture measures the deployed path, not a bare root-owned
	// volume the real product never hands to a session.
	docker("run", "--rm", "-v", volume+":/workspace", "--entrypoint", "true", "ghcr.io/sumi-studio/sumi-job:"+tag)
	t.Cleanup(func() {
		c := exec.Command("docker", "volume", "rm", "-f", volume)
		c.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "DOCKER_HOST=unix:///var/run/docker.sock"}
		_ = c.Run()
	})
	backend := &DockerBackend{
		baseEnvironment: []string{"PATH=/usr/local/bin:/usr/bin:/bin", "DOCKER_HOST=unix:///var/run/docker.sock", "SUMI_JOB_IMAGE_TAG=" + tag},
		runner:          execCommandRunner{},
	}
	stateDir := t.TempDir() + "/state"
	s, err := newProcessStore(stateDir, backend)
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{backend: backend, processes: s}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go service.RunProcessObserver(ctx)
	op, err := service.StartProcess(ctx, ProcessStartRequest{
		PersonalityAgentID:    persona,
		OriginatingToolCallID: "e2e-" + uuid.NewString(),
		Executable:            "/bin/bash",
		Interactive:           true,
		TTY:                   true,
		Image:                 "job",
		TimeoutSeconds:        600,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	container := "sumi-process-" + op.OperationID
	t.Cleanup(func() {
		c := exec.Command("docker", "rm", "-f", container)
		c.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "DOCKER_HOST=unix:///var/run/docker.sock"}
		_ = c.Run()
	})
	lookup := ProcessLookupRequest{PersonalityAgentID: persona, OperationID: op.OperationID}
	write := func(data string) {
		t.Helper()
		rc, err := service.WriteProcessInput(ctx, ProcessInputRequest{ProcessLookupRequest: lookup, Data: []byte(data)})
		if err != nil || !rc.Delivered {
			t.Fatalf("write %q: receipt=%+v err=%v", data, rc, err)
		}
	}
	// Wait for the observer to create+start the container.
	running := false
	for i := 0; i < 100 && !running; i++ {
		c := exec.Command("docker", "inspect", "--format", "{{.State.Running}}", container)
		c.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "DOCKER_HOST=unix:///var/run/docker.sock"}
		out, err := c.Output()
		running = err == nil && strings.TrimSpace(string(out)) == "true"
		time.Sleep(300 * time.Millisecond)
	}
	if !running {
		t.Fatalf("session container %s never started", container)
	}
	write("\n")
	var io_ *interactiveIO
	for i := 0; i < 50 && io_ == nil; i++ {
		io_ = s.interactiveIOFor(op.OperationID)
		time.Sleep(200 * time.Millisecond)
	}
	if io_ == nil {
		t.Fatal("interactive supervisor never attached")
	}
	marker := "E2E-MARK-" + uuid.NewString()[:8]
	write("echo " + marker + "\n")
	// The marker appears once as echoed input and again as command
	// output — two occurrences prove the line executed, not just that
	// the keystrokes reached the PTY.
	if out := waitTTYCount(t, io_.tty, marker, 2, 30*time.Second); strings.Count(out, marker) < 2 {
		jp, jerr := backend.ProcessJournalPath(ctx, op)
		raw, _ := os.ReadFile(jp)
		if len(raw) > 4096 {
			raw = raw[len(raw)-4096:]
		}
		t.Logf("journal path=%s err=%v\njournal tail:\n%s", jp, jerr, raw)
		s.mu.Lock()
		rec := s.records[op.OperationID]
		s.mu.Unlock()
		st := rec.Operation.State
		rec.mu.Unlock()
		var stacks bytes.Buffer
		_ = pprof.Lookup("goroutine").WriteTo(&stacks, 2)
		t.Logf("op state=%s tty=%q\ngoroutines:\n%s", st, out, stacks.String())
		t.Fatalf("marker never reached tty log; captured %q", out)
	}
	// Real workspace persistence inside the session's named volume.
	// "RC=0" can only come from the executed command — the echoed
	// input line carries the literal "$?".
	write("echo persist-ok > /workspace/e2e-file; echo RC=$?\n")
	if out := waitTTY(t, io_.tty, "RC=0", 15*time.Second); !strings.Contains(out, "RC=0") {
		t.Fatalf("workspace write never executed; captured %q", out)
	}
	// Daemon-API resize, verified through the PTY's own stty.
	if _, err := service.ResizeProcess(ctx, ProcessResizeRequest{ProcessLookupRequest: lookup, Cols: 132, Rows: 43}); err != nil {
		t.Fatalf("resize: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	write("stty size\n")
	if out := waitTTY(t, io_.tty, "43 132", 15*time.Second); !strings.Contains(out, "43 132") {
		t.Fatalf("resize not reflected in tty; captured %q", out)
	}
	// Foreground-process-group signal: sleep becomes the foreground job;
	// TERM must kill it without killing the shell.
	write("sleep 300\n")
	time.Sleep(1500 * time.Millisecond)
	rc, err := service.SignalProcess(ctx, ProcessSignalRequest{ProcessLookupRequest: lookup, Signal: "TERM"})
	if err != nil || !rc.Delivered {
		t.Fatalf("signal: receipt=%+v err=%v", rc, err)
	}
	// "SIG-OK-143" is the executed output (128+SIGTERM): the echoed
	// input carries the literal "$?", so the number proves both that
	// sleep died by the signal and that the shell survived.
	write("echo SIG-OK-$?\n")
	if out := waitTTY(t, io_.tty, "SIG-OK-143", 15*time.Second); !strings.Contains(out, "SIG-OK-143") {
		t.Fatalf("shell died or signal missed foreground; captured %q", out)
	}
	// Provisioner restart: end this store's supervision, then a second
	// store on the same state dir reloads the journaled op and resumes
	// the journal tailer at the committed source cursor. Prior
	// scrollback must not duplicate, and input reattaches on demand.
	s.stopInteractive(io_)
	time.Sleep(500 * time.Millisecond)
	s2, err := newProcessStore(stateDir, backend)
	if err != nil {
		t.Fatalf("restart reload: %v", err)
	}
	service2 := &Service{backend: backend, processes: s2}
	go service2.RunProcessObserver(ctx)
	var io2 *interactiveIO
	for i := 0; i < 50 && io2 == nil; i++ {
		io2 = s2.interactiveIOFor(op.OperationID)
		time.Sleep(200 * time.Millisecond)
	}
	if io2 == nil {
		t.Fatal("restart did not reattach output supervision")
	}
	marker2 := "E2E-AFTER-" + uuid.NewString()[:8]
	rc2, err := service2.WriteProcessInput(ctx, ProcessInputRequest{ProcessLookupRequest: lookup, Data: []byte("echo " + marker2 + "\n")})
	if err != nil || !rc2.Delivered {
		t.Fatalf("post-restart write: receipt=%+v err=%v", rc2, err)
	}
	out := waitTTYCount(t, io2.tty, marker2, 2, 30*time.Second)
	if strings.Count(out, marker2) < 2 {
		t.Fatalf("post-restart marker never executed; captured %q", out)
	}
	if strings.Count(out, marker) != 2 {
		t.Fatalf("scrollback duplicated or lost across restart: marker count %d in %q", strings.Count(out, marker), out)
	}
	service = service2
	// Physical stop: cancel kills the exact container.
	if _, err := service.CancelProcess(ctx, lookup); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	stopped := false
	for i := 0; i < 100 && !stopped; i++ {
		stopped = docker("inspect", "--format", "{{.State.Running}}", container) == "false"
		time.Sleep(300 * time.Millisecond)
	}
	if !stopped {
		t.Fatal("container still running after cancel — no physical stop")
	}
	// The workspace volume outlives the session container.
	out = docker("run", "--rm", "-v", volume+":/w", "--entrypoint", "cat", "ghcr.io/sumi-studio/sumi-job:"+tag, "/w/e2e-file")
	if out != "persist-ok" {
		t.Fatalf("workspace file lost after session end: %q", out)
	}
}

// TestInteractiveCapacitySplit verifies the interactive capacity budget
// is separate from the one-shot batch budget: a persona holding
// terminal sessions is refused at 4 and the store at 8, but those same
// sessions never consume the persona's single batch slot.
func TestInteractiveCapacitySplit(t *testing.T) {
	b := &interactiveTestBackend{processTestBackend: &processTestBackend{fakeBackend: newFakeBackend()}}
	s, err := newProcessStore(t.TempDir()+"/state", b)
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{backend: b, processes: s}
	persona := uuid.NewString()
	seed := func(personaID string, interactive bool, n int) {
		s.mu.Lock()
		defer s.mu.Unlock()
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("%032x", len(s.records)+1)
			s.records[id] = &processRecord{mu: &sync.Mutex{}, Operation: ProcessOperation{
				OperationID:        id,
				PersonalityAgentID: personaID,
				State:              ProcessRunning,
				Interactive:        interactive,
			}}
		}
	}
	start := func(personaID string, interactive bool) error {
		_, err := service.StartProcess(context.Background(), ProcessStartRequest{
			PersonalityAgentID:    personaID,
			OriginatingToolCallID: "cap-" + uuid.NewString(),
			Executable:            "/bin/true",
			Interactive:           interactive,
			TTY:                   interactive,
			TimeoutSeconds:        60,
		})
		return err
	}
	// Four live terminal sessions refuse the persona's fifth, while the
	// batch slot stays free — a held terminal must not starve one-shot
	// jobs.
	seed(persona, true, 4)
	if err := start(persona, true); !errors.Is(err, ErrProcessBusy) {
		t.Fatalf("5th interactive for one persona: %v", err)
	}
	if err := start(persona, false); err != nil {
		t.Fatalf("batch slot starved by interactive sessions: %v", err)
	}
	// Eight live sessions store-wide refuse a ninth persona's session.
	other := []string{uuid.NewString(), uuid.NewString(), uuid.NewString()}
	seed(other[0], true, 4)
	seed(other[1], true, 4)
	if err := start(other[2], true); !errors.Is(err, ErrProcessBusy) {
		t.Fatalf("9th interactive store-wide: %v", err)
	}
}
