package runtimeprovision

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type processTestBackend struct {
	*fakeBackend
	launches    int
	observation ProcessObservation
}

func (b *processTestBackend) LaunchProcess(context.Context, ProcessOperation) error {
	b.launches++
	b.observation = ProcessObservation{Exists: true, Running: true, StartedAt: time.Now().UTC()}
	return nil
}
func (b *processTestBackend) InspectProcess(context.Context, ProcessOperation) (ProcessObservation, error) {
	return b.observation, nil
}
func (b *processTestBackend) StopProcess(context.Context, ProcessOperation) error {
	b.observation.Running = false
	b.observation.ExitCode = 137
	return nil
}
func TestProcessDurableReceiptAndNoReplay(t *testing.T) {
	ctx := context.Background()
	b := &processTestBackend{fakeBackend: newFakeBackend()}
	directory := t.TempDir() + "/state"
	s, err := NewService(b, ServiceConfig{StateDirectory: directory})
	if err != nil {
		t.Fatal(err)
	}
	request := ProcessStartRequest{PersonalityAgentID: uuid.NewString(), OriginatingToolCallID: "call-one", Executable: "/bin/sh", Args: []string{"-c", "echo done"}}
	op, err := s.StartProcess(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.StartProcess(ctx, request)
	if err != nil || again.OperationID != op.OperationID {
		t.Fatal(again, err)
	}
	s.observeProcesses(ctx)
	if b.launches != 1 {
		t.Fatal(b.launches)
	}
	request.Args = []string{"-c", "echo twice"}
	if _, err = s.StartProcess(ctx, request); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	s, err = NewService(b, ServiceConfig{StateDirectory: directory})
	if err != nil {
		t.Fatal(err)
	}
	b.observation.Running = false
	b.observation.Stdout = []byte("done\n")
	s.observeProcesses(ctx)
	if b.launches != 1 {
		t.Fatal("replayed")
	}
	pending, err := s.PendingProcessCompletions(ctx)
	if err != nil || len(pending) != 1 || pending[0].State != ProcessSucceeded {
		t.Fatal(pending, err)
	}
	receipt := ProcessCompletionReceipt{PersonalityAgentID: op.PersonalityAgentID, OperationID: op.OperationID, EventID: op.EventID, CommandID: uuid.NewString(), CommandSeq: 3}
	if err = s.AcknowledgeProcessCompletion(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	if err = s.AcknowledgeProcessCompletion(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	receipt.CommandSeq++
	if err = s.AcknowledgeProcessCompletion(ctx, receipt); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	s, err = NewService(b, ServiceConfig{StateDirectory: directory})
	if err != nil {
		t.Fatal(err)
	}
	pending, err = s.PendingProcessCompletions(ctx)
	if err != nil || len(pending) != 0 {
		t.Fatal(pending, err)
	}
	output, err := s.ReadProcessOutput(ctx, ProcessOutputRequest{ProcessLookupRequest: ProcessLookupRequest{op.PersonalityAgentID, op.OperationID}, Stream: "stdout"})
	if err != nil || output.Content != "done\n" || !output.EOF {
		t.Fatal(output, err)
	}
}
func TestProcessMissingContainerNeverReexecutes(t *testing.T) {
	ctx := context.Background()
	b := &processTestBackend{fakeBackend: newFakeBackend()}
	s := newTestService(t, b)
	op, err := s.StartProcess(ctx, ProcessStartRequest{PersonalityAgentID: uuid.NewString(), OriginatingToolCallID: "call", Executable: "true"})
	if err != nil {
		t.Fatal(err)
	}
	s.observeProcesses(ctx)
	b.observation = ProcessObservation{}
	s.observeProcesses(ctx)
	s.observeProcesses(ctx)
	got, err := s.ProcessStatus(ctx, ProcessLookupRequest{op.PersonalityAgentID, op.OperationID})
	if err != nil || got.State != ProcessIndeterminate || b.launches != 1 {
		t.Fatal(got, err, b.launches)
	}
}
func TestProcessCancelBeforeLaunch(t *testing.T) {
	ctx := context.Background()
	b := &processTestBackend{fakeBackend: newFakeBackend()}
	s := newTestService(t, b)
	op, err := s.StartProcess(ctx, ProcessStartRequest{PersonalityAgentID: uuid.NewString(), OriginatingToolCallID: "call", Executable: "true"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.CancelProcess(ctx, ProcessLookupRequest{op.PersonalityAgentID, op.OperationID})
	if err != nil {
		t.Fatal(err)
	}
	s.observeProcesses(ctx)
	got, _ := s.ProcessStatus(ctx, ProcessLookupRequest{op.PersonalityAgentID, op.OperationID})
	if got.State != ProcessCancelled || b.launches != 0 {
		t.Fatal(got, b.launches)
	}
}
func TestProcessRequestBounds(t *testing.T) {
	r := ProcessStartRequest{PersonalityAgentID: uuid.NewString(), OriginatingToolCallID: "call", Executable: "sh"}
	for _, cwd := range []string{"../x", "/workspace", "a/../b", "a//b"} {
		r.Cwd = cwd
		if r.Validate() == nil {
			t.Fatal(cwd)
		}
	}
	r.Cwd = "."
	r.Args = make([]string, 129)
	if r.Validate() == nil {
		t.Fatal("args bound")
	}
}

func TestProcessOutputBeyondEOFDoesNotRewind(t *testing.T) {
	ctx := context.Background()
	b := &processTestBackend{fakeBackend: newFakeBackend()}
	s := newTestService(t, b)
	op, err := s.StartProcess(ctx, ProcessStartRequest{PersonalityAgentID: uuid.NewString(), OriginatingToolCallID: "eof", Executable: "true"})
	if err != nil {
		t.Fatal(err)
	}
	s.observeProcesses(ctx)
	b.observation.Running = false
	b.observation.Stdout = []byte("abc")
	s.observeProcesses(ctx)
	output, err := s.ReadProcessOutput(ctx, ProcessOutputRequest{ProcessLookupRequest: ProcessLookupRequest{op.PersonalityAgentID, op.OperationID}, Stream: "stdout", Offset: 100})
	if err != nil || output.NextOffset != 100 || output.Content != "" || !output.EOF {
		t.Fatal(output, err)
	}
}

type blockedProcessBackend struct {
	*processTestBackend
	entered chan struct{}
	release chan struct{}
}

func (b *blockedProcessBackend) InspectProcess(ctx context.Context, o ProcessOperation) (ProcessObservation, error) {
	close(b.entered)
	select {
	case <-b.release:
		return b.processTestBackend.InspectProcess(ctx, o)
	case <-ctx.Done():
		return ProcessObservation{}, ctx.Err()
	}
}
func TestProcessBlockedInspectionDoesNotBlockOtherOperations(t *testing.T) {
	ctx := context.Background()
	b := &blockedProcessBackend{processTestBackend: &processTestBackend{fakeBackend: newFakeBackend()}, entered: make(chan struct{}), release: make(chan struct{})}
	s := newTestService(t, b)
	op, err := s.StartProcess(ctx, ProcessStartRequest{PersonalityAgentID: uuid.NewString(), OriginatingToolCallID: "blocked", Executable: "true"})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { s.observeProcesses(ctx); close(done) }()
	<-b.entered
	completed := make(chan error, 1)
	go func() {
		_, err := s.StartProcess(ctx, ProcessStartRequest{PersonalityAgentID: uuid.NewString(), OriginatingToolCallID: "other", Executable: "true"})
		if err == nil {
			_, err = s.CancelProcess(ctx, ProcessLookupRequest{op.PersonalityAgentID, op.OperationID})
		}
		completed <- err
	}()
	select {
	case err := <-completed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(b.release)
		t.Fatal("backend I/O blocked unrelated start/cancellation")
	}
	close(b.release)
	<-done
	op, err = s.ProcessStatus(ctx, ProcessLookupRequest{op.PersonalityAgentID, op.OperationID})
	if err != nil {
		t.Fatal(err)
	}
	s.processes.mu.Lock()
	r := s.processes.records[op.OperationID]
	s.processes.mu.Unlock()
	r.mu.Lock()
	cancelled := r.CancelRequested
	r.mu.Unlock()
	if !cancelled {
		t.Fatal("lost concurrent cancellation")
	}
}

func TestProcessFailedCancelPersistenceDoesNotCancel(t *testing.T) {
	ctx := context.Background()
	b := &processTestBackend{fakeBackend: newFakeBackend()}
	s := newTestService(t, b)
	op, err := s.StartProcess(ctx, ProcessStartRequest{PersonalityAgentID: uuid.NewString(), OriginatingToolCallID: "save-failure", Executable: "true"})
	if err != nil {
		t.Fatal(err)
	}
	originalDirectory := s.processes.directory
	s.processes.directory = originalDirectory + "/missing"
	if _, err = s.CancelProcess(ctx, ProcessLookupRequest{op.PersonalityAgentID, op.OperationID}); err == nil {
		t.Fatal("expected persistence failure")
	}
	s.processes.directory = originalDirectory
	s.observeProcesses(ctx)
	got, _ := s.ProcessStatus(ctx, ProcessLookupRequest{op.PersonalityAgentID, op.OperationID})
	if got.State != ProcessRunning || b.launches != 1 {
		t.Fatal(got, b.launches)
	}
}

func TestProcessLogsFailureDoesNotPreventCancellation(t *testing.T) {
	directory := t.TempDir()
	calls := filepath.Join(directory, "calls")
	op := ProcessOperation{OperationID: strings.Repeat("a", 64), PersonalityAgentID: uuid.NewString()}
	script := `#!/bin/sh
printf '%s\n' "$1" >> '` + calls + `'
case "$1" in
 container) echo abc ;;
 inspect) echo '{"Config":{"Labels":{"sumi.operation_id":"` + op.OperationID + `","sumi.personality_agent_id":"` + op.PersonalityAgentID + `"}},"State":{"Running":true,"ExitCode":0,"StartedAt":"2026-09-09T00:00:00Z","FinishedAt":"0001-01-01T00:00:00Z"}}' | while IFS= read -r row; do printf '[%s]' "$row"; done ;;
 logs) exec /bin/sleep 10 ;;
 kill) exit 0 ;;
 *) exit 1 ;;
esac
`
	if err := os.WriteFile(filepath.Join(directory, "docker"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory)
	b := &DockerBackend{baseEnvironment: []string{"PATH=" + directory}, processLogTimeout: 10 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	observation, err := b.InspectProcess(ctx, op)
	if err != nil || !observation.Running || !observation.OutputIncomplete || !observation.StdoutTruncated || !observation.StderrTruncated {
		t.Fatal(observation, err)
	}
	if err = b.StopProcess(ctx, op); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(raw), "logs\n") != 1 || !strings.Contains(string(raw), "kill\n") {
		t.Fatal(string(raw))
	}
}

func TestProcessPendingCompletionBatchIsFair(t *testing.T) {
	ctx := context.Background()
	b := &processTestBackend{fakeBackend: newFakeBackend()}
	s := newTestService(t, b)
	for i := 0; i < 20; i++ {
		op, err := s.StartProcess(ctx, ProcessStartRequest{PersonalityAgentID: uuid.NewString(), OriginatingToolCallID: "completion", Executable: "true"})
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.CancelProcess(ctx, ProcessLookupRequest{op.PersonalityAgentID, op.OperationID})
		if err != nil {
			t.Fatal(err)
		}
		s.observeProcesses(ctx)
	}
	first, err := s.PendingProcessCompletions(ctx)
	if err != nil || len(first) != 4 {
		t.Fatal(first, err)
	}
	seen := map[string]bool{}
	for _, op := range first {
		seen[op.OperationID] = true
	}
	for i := 0; i < 4; i++ {
		batch, err := s.PendingProcessCompletions(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, op := range batch {
			seen[op.OperationID] = true
		}
	}
	if len(seen) != 20 {
		t.Fatal("starved completions", len(seen))
	}
}
func TestProcessSmallUTF8PagesAdvanceWithoutCorruption(t *testing.T) {
	ctx := context.Background()
	b := &processTestBackend{fakeBackend: newFakeBackend()}
	s := newTestService(t, b)
	op, err := s.StartProcess(ctx, ProcessStartRequest{PersonalityAgentID: uuid.NewString(), OriginatingToolCallID: "utf8", Executable: "true"})
	if err != nil {
		t.Fatal(err)
	}
	s.observeProcesses(ctx)
	b.observation.Running = false
	b.observation.Stdout = []byte("日本語")
	s.observeProcesses(ctx)
	result := ""
	offset := int64(0)
	for i := 0; i < 3; i++ {
		page, err := s.ReadProcessOutput(ctx, ProcessOutputRequest{ProcessLookupRequest: ProcessLookupRequest{op.PersonalityAgentID, op.OperationID}, Stream: "stdout", Offset: offset, Limit: 4})
		if err != nil || page.NextOffset <= offset {
			t.Fatal(page, err)
		}
		result += page.Content
		offset = page.NextOffset
	}
	if result != "日本語" {
		t.Fatal(result)
	}
}

func TestProcessEscapedCompletionBatchFitsProtocol(t *testing.T) {
	ctx := context.Background()
	b := &processTestBackend{fakeBackend: newFakeBackend()}
	s := newTestService(t, b)
	for i := 0; i < 5; i++ {
		op, err := s.StartProcess(ctx, ProcessStartRequest{PersonalityAgentID: uuid.NewString(), OriginatingToolCallID: strings.Repeat("\x01", 1024), Executable: "true", Args: []string{strings.Repeat("\x01", (32<<10)-4)}})
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.CancelProcess(ctx, ProcessLookupRequest{op.PersonalityAgentID, op.OperationID})
		if err != nil {
			t.Fatal(err)
		}
		s.observeProcesses(ctx)
	}
	batch, err := s.PendingProcessCompletions(ctx)
	if err != nil || len(batch) != 4 {
		t.Fatal(len(batch), err)
	}
	wire, err := json.Marshal(batch)
	if err != nil || len(wire) >= maxRequestBytes {
		t.Fatal(len(wire), err)
	}
	seen := map[string]bool{}
	for _, op := range batch {
		seen[op.OperationID] = true
	}
	batch, err = s.PendingProcessCompletions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range batch {
		seen[op.OperationID] = true
	}
	if len(seen) != 5 {
		t.Fatal("large completion starved")
	}
}

type removingProcessTestBackend struct {
	*processTestBackend
	removed int
}

func (b *removingProcessTestBackend) RemoveProcess(context.Context, ProcessOperation) error {
	b.removed++
	return nil
}

func TestProcessTerminalOutputIsLazyAndSurvivesMetadataRewrites(t *testing.T) {
	ctx := context.Background()
	backend := &removingProcessTestBackend{processTestBackend: &processTestBackend{fakeBackend: newFakeBackend()}}
	directory := t.TempDir() + "/state"
	open := func() *Service {
		t.Helper()
		s, err := NewService(backend, ServiceConfig{StateDirectory: directory})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	s := open()
	operations := []ProcessOperation{}
	for i := 0; i < 4; i++ {
		op, err := s.StartProcess(ctx, ProcessStartRequest{PersonalityAgentID: uuid.NewString(), OriginatingToolCallID: "large-output", Executable: "true"})
		if err != nil {
			t.Fatal(err)
		}
		s.observeProcesses(ctx)
		backend.observation.Running = false
		backend.observation.Stdout = []byte(strings.Repeat("o", processOutputLimit))
		backend.observation.Stderr = []byte(strings.Repeat("e", processOutputLimit))
		s.observeProcesses(ctx)
		operations = append(operations, op)
	}
	assertUnloaded := func() {
		t.Helper()
		for _, r := range s.processes.records {
			if !r.Operation.State.terminal() || !r.outputUnloaded || r.Stdout != nil || r.Stderr != nil {
				t.Fatal("terminal output retained in memory", r.Operation.OperationID)
			}
		}
	}
	assertUnloaded()
	s = open()
	assertUnloaded()
	for _, op := range operations {
		receipt := ProcessCompletionReceipt{PersonalityAgentID: op.PersonalityAgentID, OperationID: op.OperationID, EventID: op.EventID, CommandID: uuid.NewString(), CommandSeq: 1}
		if err := s.AcknowledgeProcessCompletion(ctx, receipt); err != nil {
			t.Fatal(err)
		}
	}
	assertUnloaded()
	s.observeProcesses(ctx)
	assertUnloaded()
	if backend.removed != len(operations) {
		t.Fatal("expected each durable terminal container removed", backend.removed)
	}
	s = open()
	assertUnloaded()
	for _, op := range operations {
		for stream, want := range map[string]string{"stdout": "oooo", "stderr": "eeee"} {
			page, err := s.ReadProcessOutput(ctx, ProcessOutputRequest{ProcessLookupRequest: ProcessLookupRequest{op.PersonalityAgentID, op.OperationID}, Stream: stream, Offset: processOutputLimit - 4, Limit: 4})
			if err != nil || page.Content != want || !page.EOF || page.NextOffset != processOutputLimit {
				t.Fatal(page, err)
			}
		}
	}
	assertUnloaded()
	pending, err := s.PendingProcessCompletions(ctx)
	if err != nil || len(pending) != 0 {
		t.Fatal(pending, err)
	}
}

func TestProcessUnchangedObservationDoesNotRewriteJournal(t *testing.T) {
	ctx := context.Background()
	backend := &processTestBackend{fakeBackend: newFakeBackend()}
	s := newTestService(t, backend)
	op, err := s.StartProcess(ctx, ProcessStartRequest{PersonalityAgentID: uuid.NewString(), OriginatingToolCallID: "unchanged", Executable: "true"})
	if err != nil {
		t.Fatal(err)
	}
	s.observeProcesses(ctx)
	backend.observation.Stdout = []byte(strings.Repeat("x", processOutputLimit))
	s.observeProcesses(ctx)
	journal := filepath.Join(s.processes.directory, op.OperationID+".json")
	old := time.Unix(100, 0)
	if err = os.Chtimes(journal, old, old); err != nil {
		t.Fatal(err)
	}
	s.observeProcesses(ctx)
	info, err := os.Stat(journal)
	if err != nil || !info.ModTime().Equal(old) {
		t.Fatal("unchanged observation rewrote journal", info, err)
	}
}
