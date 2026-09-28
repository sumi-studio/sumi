package runtimeprovision

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestProcessImagePinForms(t *testing.T) {
	rev := strings.Repeat("a", 40)
	id := "sha256:" + strings.Repeat("b", 64)
	for _, c := range []struct {
		pin, ref, want string
		ok             bool
	}{
		{pin: rev, ref: processImageRepository + ":" + rev, ok: true},
		{pin: id, ref: id, want: id, ok: true},
		{pin: ""},
		{pin: "latest"},
		{pin: rev[:12]},
		{pin: "sha256:" + strings.Repeat("b", 12)},
		{pin: strings.ToUpper(rev)},
	} {
		ref, want, err := processImagePin([]string{"PATH=/bin", "SUMI_JOB_IMAGE_TAG=" + c.pin})
		if c.ok != (err == nil) || ref != c.ref || want != c.want {
			t.Errorf("pin %q: ref=%q want=%q err=%v", c.pin, ref, want, err)
		}
	}
	if _, _, err := processImagePin(nil); err == nil || !strings.Contains(err.Error(), "unset") {
		t.Fatalf("unset pin: %v", err)
	}
}

// The 2026-09-28 hosted defect: an image-ID pin the backend refused. The
// refusal happens before any Docker call, so it must be a typed
// never-started error, not an unconfirmed launch.
func TestLaunchProcessBadPinIsNotStarted(t *testing.T) {
	b, _ := NewDockerBackend(DockerBackendConfig{BaseEnvironment: []string{"PATH=/nonexistent", "SUMI_JOB_IMAGE_TAG=latest"}})
	err := b.LaunchProcess(context.Background(), ProcessOperation{OperationID: "x", WorkspaceBind: "/w", FilesVolumeUUID: uuid.NewString()})
	var notStarted *ProcessNotStartedError
	if !errors.As(err, &notStarted) || !strings.Contains(err.Error(), "SUMI_JOB_IMAGE_TAG") {
		t.Fatalf("got %v", err)
	}
	err = b.LaunchProcess(context.Background(), ProcessOperation{OperationID: "x"})
	if !errors.As(err, &notStarted) || !errors.Is(err, ErrProcessWorkspace) {
		t.Fatalf("missing workspace: %v", err)
	}
}

type notStartedBackend struct {
	launches, inspects int
}

func (b *notStartedBackend) LaunchProcess(context.Context, ProcessOperation) error {
	b.launches++
	return processNotStarted(errors.New("job image pin is invalid"))
}
func (b *notStartedBackend) InspectProcess(context.Context, ProcessOperation) (ProcessObservation, error) {
	b.inspects++
	return ProcessObservation{}, nil
}
func (b *notStartedBackend) StopProcess(context.Context, ProcessOperation) error { return nil }

// A never-started launch ends as a definite failure carrying its cause —
// not "indeterminate … container unavailable" — is quiesced at once, and
// is never launched again (including after a provisioner restart).
func TestObserveNeverStartedLaunchFailsWithCause(t *testing.T) {
	ctx := context.Background()
	for _, interactive := range []bool{false, true} {
		b := &notStartedBackend{}
		dir := t.TempDir() + "/state"
		s, err := NewService(b, ServiceConfig{StateDirectory: dir})
		if err != nil {
			t.Fatal(err)
		}
		req := ProcessStartRequest{PersonalityAgentID: uuid.NewString(), OriginatingToolCallID: "term:s1", Executable: "/bin/bash", TimeoutSeconds: 600, Interactive: interactive, TTY: interactive}
		op, err := s.StartProcess(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		s.observeProcesses(ctx)
		s.observeProcesses(ctx)
		lookup := ProcessLookupRequest{PersonalityAgentID: req.PersonalityAgentID, OperationID: op.OperationID}
		got, err := s.ProcessStatus(ctx, lookup)
		if err != nil {
			t.Fatal(err)
		}
		if got.State != ProcessFailed || !got.NotStarted || !got.Quiesced || got.StartedAt != nil ||
			got.Error != "process was not started: job image pin is invalid" {
			t.Fatalf("interactive=%v: %+v", interactive, got)
		}
		if b.launches != 1 || b.inspects != 0 {
			t.Fatalf("launches=%d inspects=%d", b.launches, b.inspects)
		}
		s, err = NewService(b, ServiceConfig{StateDirectory: dir})
		if err != nil {
			t.Fatal(err)
		}
		s.observeProcesses(ctx)
		if again, err := s.StartProcess(ctx, req); err != nil || again.State != ProcessFailed || !again.NotStarted {
			t.Fatalf("replay after restart: %+v %v", again, err)
		}
		s.observeProcesses(ctx)
		if b.launches != 1 {
			t.Fatalf("relaunched after restart: %d", b.launches)
		}
	}
}

// fakeDockerCLI puts a scripted `docker` first on PATH for the backend.
func fakeDockerCLI(t *testing.T, script string) []string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte("#!/bin/sh\n"+script+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	path := dir + ":/usr/bin:/bin"
	t.Setenv("PATH", path) // exec resolves "docker" through the test's own PATH
	return []string{"PATH=" + path, "SUMI_JOB_IMAGE_TAG=" + strings.Repeat("a", 40)}
}

// Only Docker's own "No such image" answer is reported as an absent
// image; a check that failed or did not finish says so instead. All three
// remain definite never-started refusals (nothing was created).
func TestResolveProcessImageDistinguishesAbsentFromFailedCheck(t *testing.T) {
	for _, c := range []struct {
		name, script, want string
		cancel             bool
	}{
		{name: "absent", script: `echo "Error response from daemon: No such image: $5" >&2; exit 1`, want: "is not present locally"},
		{name: "daemon down", script: `echo "Cannot connect to the Docker daemon at unix:///var/run/docker.sock" >&2; exit 1`, want: "could not be checked"},
		{name: "cancelled", script: `exec sleep 5`, want: "job image check did not complete", cancel: true},
	} {
		b, _ := NewDockerBackend(DockerBackendConfig{BaseEnvironment: fakeDockerCLI(t, c.script)})
		ctx, cancel := context.WithCancel(context.Background())
		if c.cancel {
			go func() { time.Sleep(100 * time.Millisecond); cancel() }()
		}
		_, err := b.ResolveProcessImage(ctx)
		cancel()
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v", c.name, err)
		}
		if c.name != "absent" && strings.Contains(fmt.Sprint(err), "not present") {
			t.Errorf("%s claims absence: %v", c.name, err)
		}
		if c.cancel {
			continue
		}
		lerr := b.LaunchProcess(context.Background(), ProcessOperation{OperationID: "x", WorkspaceBind: "/w", FilesVolumeUUID: uuid.NewString()})
		var notStarted *ProcessNotStartedError
		if !errors.As(lerr, &notStarted) || !strings.Contains(lerr.Error(), c.want) {
			t.Errorf("%s: launch %v", c.name, lerr)
		}
	}
}

type notStartedInteractiveBackend struct{ notStartedBackend }

func (b *notStartedInteractiveBackend) ProcessJournalPath(context.Context, ProcessOperation) (string, error) {
	return "", errors.New("no such container")
}
func (b *notStartedInteractiveBackend) OpenProcessInput(context.Context, ProcessOperation) (*processSink, error) {
	return nil, errors.New("no such container")
}
func (b *notStartedInteractiveBackend) SignalProcess(context.Context, ProcessOperation, string) error {
	return errors.New("no such container")
}
func (b *notStartedInteractiveBackend) ResizeProcess(context.Context, ProcessOperation, int, int) error {
	return errors.New("no such container")
}

// The driver's first output read can start a supervisor before the
// observer's launch is refused. That supervisor must end without writing
// a "final output journal could not be resolved" loss boundary: nothing
// ran, so no output was lost.
func TestNeverStartedInteractiveRecordsNoOutputLoss(t *testing.T) {
	ctx := context.Background()
	s, err := NewService(&notStartedInteractiveBackend{}, ServiceConfig{StateDirectory: t.TempDir() + "/state"})
	if err != nil {
		t.Fatal(err)
	}
	req := ProcessStartRequest{PersonalityAgentID: uuid.NewString(), OriginatingToolCallID: "term:s1", Executable: "/bin/bash", TimeoutSeconds: 600, Interactive: true, TTY: true}
	op, err := s.StartProcess(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	lookup := ProcessLookupRequest{PersonalityAgentID: req.PersonalityAgentID, OperationID: op.OperationID}
	if _, err := s.ReadProcessOutput(ctx, ProcessOutputRequest{ProcessLookupRequest: lookup, Stream: "stdout"}); err != nil {
		t.Fatal(err)
	}
	s.observeProcesses(ctx)
	// Past the supervisor's terminal miss bound (20 × 150ms).
	time.Sleep(journalMissBound*journalPollInterval + 500*time.Millisecond)
	out, err := s.ReadProcessOutput(ctx, ProcessOutputRequest{ProcessLookupRequest: lookup, Stream: "stdout"})
	if err != nil {
		t.Fatal(err)
	}
	st, _ := s.ProcessStatus(ctx, lookup)
	if !st.NotStarted || len(out.Gaps) != 0 || out.Gap || out.NextOffset != 0 {
		t.Fatalf("status=%+v output=%+v", st, out)
	}
}
