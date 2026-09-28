package runtimeprovision

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// blockingStream stands in for the attach; tests feed bytes through delivered().
type blockingStream struct{ closed chan struct{} }

func (b *blockingStream) Read([]byte) (int, error) { <-b.closed; return 0, io.EOF }
func (b *blockingStream) Close() error {
	select {
	case <-b.closed:
	default:
		close(b.closed)
	}
	return nil
}

func liveIO(t *testing.T) *interactiveIO {
	t.Helper()
	tty, err := openTTYLog(t.TempDir(), "op-live")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tty.close() })
	io_ := &interactiveIO{tty: tty, done: make(chan struct{})}
	io_.startLive(&blockingStream{closed: make(chan struct{})})
	if !io_.live.active() {
		t.Fatal("live tail did not start on a fresh ttylog")
	}
	return io_
}

// stopSupervisorAtCleanup ends a service-started live op and waits for its
// supervisor goroutine to return. Otherwise it keeps committing ttylog
// cursors under the state directory while that TempDir is being removed
// ("TempDir RemoveAll cleanup: directory not empty").
func stopSupervisorAtCleanup(t *testing.T, svc *Service, opID string, io_ *interactiveIO) {
	t.Helper()
	if io_.supervised == nil {
		t.Fatal("no interactive supervisor")
	}
	t.Cleanup(func() {
		svc.processes.mu.Lock()
		rec := svc.processes.records[opID]
		svc.processes.mu.Unlock()
		rec.mu.Lock()
		rec.Operation.State = ProcessSucceeded
		rec.mu.Unlock()
		svc.processes.stopInteractive(io_)
		select {
		case <-io_.supervised:
		case <-time.After(5 * time.Second):
			t.Error("interactive supervisor did not stop")
		}
	})
}

func mustRead(t *testing.T, io_ *interactiveIO, off int64) string {
	t.Helper()
	b, ok := io_.live.read(off, 1<<20)
	if !ok {
		t.Fatalf("live read at %d refused", off)
	}
	return string(b)
}

// The prompt (no newline) is served from the live tail; once the
// journal commits the same bytes the tail advances without serving
// them twice.
func TestLiveTailServesUncommittedPromptThenYieldsToJournal(t *testing.T) {
	io_ := liveIO(t)
	io_.live.delivered([]byte("user@c:~$ "))
	if got := mustRead(t, io_, 0); got != "user@c:~$ " {
		t.Fatalf("live prompt = %q", got)
	}
	io_.live.delivered([]byte("ls\r\n"))
	if err := io_.tty.append([]byte("user@c:~$ ls\r\n")); err != nil {
		t.Fatal(err)
	}
	if !io_.live.active() {
		t.Fatal("matching journal commit switched the tail off")
	}
	if got := mustRead(t, io_, 14); got != "" {
		t.Fatalf("nothing is pending after the commit, got %q", got)
	}
	if _, ok := io_.live.read(3, 10); ok {
		t.Fatal("offsets below the durable end must come from the ttylog")
	}
}

// The journal can commit bytes before this reader receives them; the
// later live bytes are verified, not served again.
func TestLiveTailJournalAheadOfStream(t *testing.T) {
	io_ := liveIO(t)
	if err := io_.tty.append([]byte("line1\r\n")); err != nil {
		t.Fatal(err)
	}
	io_.live.delivered([]byte("line1\r\n$ "))
	if !io_.live.active() {
		t.Fatal("tail switched off on a consistent stream")
	}
	if got := mustRead(t, io_, 7); got != "$ " {
		t.Fatalf("pending after verify = %q", got)
	}
}

// Divergent content, a journaled gap, and an unbounded backlog all end
// the tail — readers fall back to the durable journal only.
func TestLiveTailSwitchesOffOnDivergenceGapOrBacklog(t *testing.T) {
	io_ := liveIO(t)
	io_.live.delivered([]byte("abc"))
	if err := io_.tty.append([]byte("abX\n")); err != nil {
		t.Fatal(err)
	}
	if io_.live.active() {
		t.Fatal("divergent journal bytes did not switch the tail off")
	}
	// Live bytes may already have been served: the divergence is
	// journaled as an explicit loss boundary.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if ev, _ := io_.tty.gapsAtOrAfter(0); len(ev) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("divergence left no gap marker")
		}
		time.Sleep(10 * time.Millisecond)
	}

	io_ = liveIO(t)
	io_.live.delivered([]byte("x"))
	if err := io_.tty.recordGap("rotated"); err != nil {
		t.Fatal(err)
	}
	if io_.live.active() {
		t.Fatal("a gap did not switch the tail off")
	}

	io_ = liveIO(t)
	io_.live.delivered(bytes.Repeat([]byte("y"), liveTailMax+1))
	if io_.live.active() {
		t.Fatal("an unbounded backlog did not switch the tail off")
	}
}

// A ttylog that already recorded a gap is not a raw-offset prefix of a
// new stream: the tail must not start.
func TestLiveTailRefusesGappedTTYLog(t *testing.T) {
	tty, err := openTTYLog(t.TempDir(), "op-gapped")
	if err != nil {
		t.Fatal(err)
	}
	defer tty.close()
	_ = tty.recordGap("earlier loss")
	io_ := &interactiveIO{tty: tty, done: make(chan struct{})}
	s := &blockingStream{closed: make(chan struct{})}
	io_.startLive(s)
	if io_.live.active() {
		t.Fatal("tail started on a gapped ttylog")
	}
	select {
	case <-s.closed:
	default:
		t.Fatal("refused stream was not closed")
	}
}

// Served text never ends inside a UTF-8 sequence.
func TestLiveTailHoldsTornRune(t *testing.T) {
	io_ := liveIO(t)
	io_.live.delivered([]byte("す\xe3\x81"))
	if got := mustRead(t, io_, 0); got != "す" {
		t.Fatalf("torn rune served: %q", got)
	}
	io_.live.delivered([]byte{0xbf})
	if got := mustRead(t, io_, 3); got != "み" {
		t.Fatalf("completed rune = %q", got)
	}
}

// The bridge's own log never reaches the job's stdout/stderr (the
// person's terminal), but a bridge that never listens still warns there.
func TestJobEgressPreludeKeepsBridgeDiagnosticsOffTheTerminal(t *testing.T) {
	for _, want := range []string{
		"setsid /usr/local/bin/sumi-egress-bridge </dev/null >>/tmp/sumi-egress-bridge.log 2>&1 &",
		"sumi-egress: bridge did not start; outbound network disabled",
		"sumi-egress: bridge unavailable; outbound network disabled",
	} {
		if !strings.Contains(jobEgressPrelude, want) {
			t.Fatalf("prelude missing %q:\n%s", want, jobEgressPrelude)
		}
	}
}

// Through ReadProcessOutput: a reader served the live prompt keeps its
// cursor; if the tail then stops (for example a provisioner restart),
// reads past the durable end return nothing — never the durable bytes
// before the cursor again — and continue exactly once the journal
// commits the line.
func TestReadProcessOutputLiveTailThenJournalNoDuplicate(t *testing.T) {
	dir := t.TempDir()
	b := &interactiveTestBackend{processTestBackend: &processTestBackend{}, journal: journalFile(t, dir)}
	svc := newTestService(t, b)
	ctx := context.Background()
	op, err := svc.StartProcess(ctx, ProcessStartRequest{
		PersonalityAgentID:    uuid.NewString(),
		OriginatingToolCallID: "term-live",
		Executable:            "/bin/bash",
		Interactive:           true,
		TTY:                   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	lookup := ProcessLookupRequest{PersonalityAgentID: op.PersonalityAgentID, OperationID: op.OperationID}
	if _, err := svc.ProcessStatus(ctx, lookup); err != nil {
		t.Fatal(err)
	}
	io_ := svc.processes.interactiveIOFor(op.OperationID)
	if io_ == nil {
		t.Fatal("no interactive supervisor")
	}
	stopSupervisorAtCleanup(t, svc, op.OperationID, io_)
	io_.startLive(&blockingStream{closed: make(chan struct{})})
	io_.live.delivered([]byte("a$ "))
	read := func(off int64) ProcessOutput {
		t.Helper()
		out, err := svc.ReadProcessOutput(ctx, ProcessOutputRequest{ProcessLookupRequest: lookup, Stream: "stdout", Offset: off, Limit: 1 << 10})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	if out := read(0); out.Content != "a$ " || out.NextOffset != 3 {
		t.Fatalf("live prompt read = %q next %d", out.Content, out.NextOffset)
	}
	if st, _ := svc.ProcessStatus(ctx, lookup); !st.OutputAttached {
		t.Fatal("output_attached false while the live tail serves output")
	}
	io_.live.off()
	if out := read(3); out.Content != "" || out.NextOffset != 3 || out.EOF {
		t.Fatalf("reader ahead of the journal got %q next %d eof %v", out.Content, out.NextOffset, out.EOF)
	}
	appendJournal(t, b.journal, "a$ ls\r\n")
	deadline := time.Now().Add(5 * time.Second)
	for {
		if out := read(3); out.Content == "ls\r\n" && out.NextOffset == 7 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("journal continuation never served; ttylog %q", readAllTTY(t, io_.tty))
		}
		time.Sleep(50 * time.Millisecond)
	}
}
