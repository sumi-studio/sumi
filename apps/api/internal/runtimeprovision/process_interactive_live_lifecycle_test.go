package runtimeprovision

import (
	"bytes"
	"context"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

type finiteLiveStream struct {
	io.Reader
	closed atomic.Bool
}

func (s *finiteLiveStream) Close() error { s.closed.Store(true); return nil }

func TestLiveTailStreamEndReleasesConnectionAndFallsBack(t *testing.T) {
	tty, err := openTTYLog(t.TempDir(), "stream-end")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tty.close() })
	io_ := &interactiveIO{tty: tty, done: make(chan struct{})}
	t.Cleanup(io_.live.off)
	stream := &finiteLiveStream{Reader: strings.NewReader("prompt$ ")}
	io_.startLive(stream)
	deadline := time.Now().Add(time.Second)
	for io_.live.active() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if io_.live.active() || !stream.closed.Load() {
		t.Fatalf("ended stream retained: active=%v closed=%v", io_.live.active(), stream.closed.Load())
	}
	if err := tty.append([]byte("prompt$ output\n")); err != nil {
		t.Fatal(err)
	}
	data, _, _, _, err := tty.read(8, 1024)
	if err != nil || string(data) != "output\n" {
		t.Fatalf("journal continuation = %q, %v", data, err)
	}
}

func TestStopInteractiveReleasesLiveConnection(t *testing.T) {
	io_ := liveIO(t)
	t.Cleanup(io_.live.off)
	stream := io_.live.rc.(*blockingStream)
	(&processStore{}).stopInteractive(io_)
	select {
	case <-stream.closed:
	default:
		t.Fatal("stopping terminal supervision retained its live output connection")
	}
	if io_.live.active() {
		t.Fatal("stopped terminal still reports active live output")
	}
}

func TestStoppedInteractiveRefusesLateLiveConnection(t *testing.T) {
	io_ := liveIO(t)
	t.Cleanup(io_.live.off)
	(&processStore{}).stopInteractive(io_)
	stream := &finiteLiveStream{Reader: strings.NewReader("late prompt$ ")}
	io_.startLive(stream)
	if io_.live.active() || !stream.closed.Load() {
		t.Fatal("late attach resurrected stopped terminal output")
	}
}

func TestLiveTailBoundsJournalAheadOfStalledStream(t *testing.T) {
	io_ := liveIO(t)
	t.Cleanup(io_.live.off)
	data := bytes.Repeat([]byte("x"), liveTailMax+1)
	if err := io_.tty.append(data); err != nil {
		t.Fatal(err)
	}
	if io_.live.active() || len(io_.live.verify) != 0 {
		t.Fatalf("stalled live stream retained unbounded journal prefix: active=%v retained=%d", io_.live.active(), len(io_.live.verify))
	}
	got, _, _, _, err := io_.tty.read(0, len(data))
	if err != nil || !bytes.Equal(got, data) {
		t.Fatal("falling back lost the durable journal")
	}
}

func TestProcessAttachHandshakeHonorsCancellation(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "docker.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	backend := &DockerBackend{baseEnvironment: []string{"DOCKER_HOST=unix://" + socket}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		conn, _, err := backend.attachStream(ctx, ProcessOperation{OperationID: "op-cancel"}, "stream=1&stdout=1", "test attach")
		if conn != nil {
			_ = conn.Close()
		}
		result <- err
	}()
	server, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	// The daemon accepted the connection but never sends upgrade headers.
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("cancelled handshake succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled output attach can keep process launch blocked")
	}
}

func TestLiveTailLossMarkerReachesReaderAlreadyAhead(t *testing.T) {
	backend := &interactiveTestBackend{processTestBackend: &processTestBackend{}, journal: journalFile(t, t.TempDir())}
	svc := newTestService(t, backend)
	ctx := context.Background()
	op, err := svc.StartProcess(ctx, ProcessStartRequest{
		PersonalityAgentID: uuid.NewString(), OriginatingToolCallID: "live-gap",
		Executable: "/bin/bash", Interactive: true, TTY: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	lookup := ProcessLookupRequest{PersonalityAgentID: op.PersonalityAgentID, OperationID: op.OperationID}
	if _, err := svc.ProcessStatus(ctx, lookup); err != nil {
		t.Fatal(err)
	}
	io_ := svc.processes.interactiveIOFor(op.OperationID)
	stopSupervisorAtCleanup(t, svc, op.OperationID, io_)
	io_.startLive(&blockingStream{closed: make(chan struct{})})
	t.Cleanup(io_.live.off)
	io_.live.delivered([]byte("abcdef"))
	read := func(offset int64) ProcessOutput {
		t.Helper()
		out, err := svc.ReadProcessOutput(ctx, ProcessOutputRequest{ProcessLookupRequest: lookup, Stream: "stdout", Offset: offset, Limit: 1024})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	if out := read(0); out.Content != "abcdef" || out.NextOffset != 6 {
		t.Fatalf("initial live output = %+v", out)
	}
	// A short conflicting journal record creates its loss marker behind
	// the reader, which has already persisted six live bytes.
	if err := io_.tty.append([]byte("X\n")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		gaps, _ := io_.tty.gapsAtOrAfter(0)
		if len(gaps) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no durable loss marker")
		}
		time.Sleep(time.Millisecond)
	}
	hasDivergence := func(out ProcessOutput) bool {
		for _, gap := range out.Gaps {
			if strings.Contains(gap.Note, "live terminal output disagreed") {
				return true
			}
		}
		return false
	}
	if out := read(6); !hasDivergence(out) {
		t.Fatalf("reader ahead silently lost divergence marker: %+v", out)
	}
	if err := io_.tty.append([]byte("later output\n")); err != nil {
		t.Fatal(err)
	}
	if out := read(6); !hasDivergence(out) {
		t.Fatalf("marker disappeared when the journal caught up: %+v", out)
	}
}
