package runtimeprovision

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"time"
	"unicode/utf8"
)

// ProcessBackend is separate from generation lifecycle: stopping a PA never
// removes the process container or its durable receipt.
type ProcessBackend interface {
	LaunchProcess(context.Context, ProcessOperation) error
	InspectProcess(context.Context, ProcessOperation) (ProcessObservation, error)
	StopProcess(context.Context, ProcessOperation) error
}
type ProcessObservation struct {
	OutputIncomplete                 bool
	Exists, Running                  bool
	ExitCode                         int
	StartedAt, FinishedAt            time.Time
	Stdout, Stderr                   []byte
	StdoutTruncated, StderrTruncated bool
}
type processRecord struct {
	mu             *sync.Mutex
	outputUnloaded bool

	Operation        ProcessOperation          `json:"operation"`
	ContainerRemoved bool                      `json:"container_removed"`
	LaunchAttempted  bool                      `json:"launch_attempted"`
	CancelRequested  bool                      `json:"cancel_requested"`
	Receipt          *ProcessCompletionReceipt `json:"receipt,omitempty"`
	Stdout           []byte                    `json:"stdout"`
	Stderr           []byte                    `json:"stderr"`
}
type processStore struct {
	mu               sync.Mutex
	directory        string
	backend          ProcessBackend
	records          map[string]*processRecord
	completionCursor string
}

func newProcessStore(directory string, backend ProcessBackend) (*processStore, error) {
	s := &processStore{directory: filepath.Join(directory, "processes"), backend: backend, records: map[string]*processRecord{}}
	if err := os.MkdirAll(s.directory, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(s.directory)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, errors.New("process journal must be a private directory")
	}
	entries, err := os.ReadDir(s.directory)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".json" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(s.directory, e.Name()))
		if err != nil {
			return nil, err
		}
		var r processRecord
		if err = json.Unmarshal(b, &r); err != nil {
			return nil, fmt.Errorf("read process journal %s: %w", e.Name(), err)
		}
		if e.Name() != r.Operation.OperationID+".json" {
			return nil, errors.New("invalid process journal name")
		}
		r.mu = &sync.Mutex{}
		r.releaseTerminalOutput()
		s.records[r.Operation.OperationID] = &r
	}
	return s, nil
}

// Terminal records keep metadata only in RAM. The existing journal still
// contains the complete bounded output; metadata rewrites must rehydrate it.
func (r *processRecord) releaseTerminalOutput() {
	if r.Operation.State.terminal() {
		r.Stdout = nil
		r.Stderr = nil
		r.outputUnloaded = true
	}
}
func (s *processStore) output(r *processRecord) ([]byte, []byte, error) {
	if !r.outputUnloaded {
		return r.Stdout, r.Stderr, nil
	}
	raw, err := os.ReadFile(filepath.Join(s.directory, r.Operation.OperationID+".json"))
	if err != nil {
		return nil, nil, err
	}
	var stored processRecord
	if err = json.Unmarshal(raw, &stored); err != nil {
		return nil, nil, err
	}
	if stored.Operation.OperationID != r.Operation.OperationID || int64(len(stored.Stdout)) != r.Operation.StdoutBytes || int64(len(stored.Stderr)) != r.Operation.StderrBytes {
		return nil, nil, errors.New("process journal output does not match durable metadata")
	}
	return stored.Stdout, stored.Stderr, nil
}
func (s *processStore) save(r *processRecord) error {
	persisted := *r
	if r.outputUnloaded {
		var err error
		persisted.Stdout, persisted.Stderr, err = s.output(r)
		if err != nil {
			return err
		}
	}
	b, err := json.Marshal(&persisted)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(s.directory, ".process-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(name, filepath.Join(s.directory, r.Operation.OperationID+".json")); err != nil {
		return err
	}
	d, err := os.Open(s.directory)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func (service *Service) StartProcess(ctx context.Context, request ProcessStartRequest) (ProcessOperation, error) {
	if err := request.Validate(); err != nil {
		return ProcessOperation{}, err
	}
	request = request.canonical()
	s := service.processes
	if s == nil {
		return ProcessOperation{}, errors.New("process backend unavailable")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id := processID(request)
	if r := s.records[id]; r != nil {
		r.mu.Lock()
		defer r.mu.Unlock()
		o := r.Operation
		if o.Executable != request.Executable || !reflect.DeepEqual(o.Args, request.Args) || o.Cwd != request.Cwd || o.TimeoutSeconds != request.TimeoutSeconds {
			return ProcessOperation{}, ErrConflict
		}
		return o, nil
	}
	active := 0
	for _, r := range s.records {
		r.mu.Lock()
		o := r.Operation
		r.mu.Unlock()
		if !o.State.terminal() {
			active++
			if o.PersonalityAgentID == request.PersonalityAgentID {
				return ProcessOperation{}, ErrProcessBusy
			}
		}
	}
	if active >= 4 {
		return ProcessOperation{}, ErrProcessBusy
	}
	event, err := uuid.NewV7()
	if err != nil {
		return ProcessOperation{}, err
	}
	r := &processRecord{mu: &sync.Mutex{}, Operation: ProcessOperation{OperationID: id, PersonalityAgentID: request.PersonalityAgentID, OriginatingToolCallID: request.OriginatingToolCallID, Executable: request.Executable, Args: request.Args, Cwd: request.Cwd, TimeoutSeconds: request.TimeoutSeconds, State: ProcessAccepted, EventID: event.String(), OccurredAt: time.Now().UTC()}}
	if err = s.save(r); err != nil {
		return ProcessOperation{}, err
	}
	s.records[id] = r
	return r.Operation, nil
}
func (service *Service) ProcessStatus(ctx context.Context, r ProcessLookupRequest) (ProcessOperation, error) {
	if err := r.Validate(); err != nil {
		return ProcessOperation{}, err
	}
	s := service.processes
	if s == nil {
		return ProcessOperation{}, ErrProcessNotFound
	}
	s.mu.Lock()
	record := s.records[r.OperationID]
	s.mu.Unlock()
	if record != nil {
		record.mu.Lock()
		defer record.mu.Unlock()
	}
	if record == nil || record.Operation.PersonalityAgentID != r.PersonalityAgentID {
		return ProcessOperation{}, ErrProcessNotFound
	}
	return record.Operation, nil
}
func (service *Service) ReadProcessOutput(ctx context.Context, r ProcessOutputRequest) (ProcessOutput, error) {
	if err := r.Validate(); err != nil {
		return ProcessOutput{}, err
	}
	s := service.processes
	if s == nil {
		return ProcessOutput{}, ErrProcessNotFound
	}
	s.mu.Lock()
	record := s.records[r.OperationID]
	s.mu.Unlock()
	if record != nil {
		record.mu.Lock()
		defer record.mu.Unlock()
	}
	if record == nil || record.Operation.PersonalityAgentID != r.PersonalityAgentID {
		return ProcessOutput{}, ErrProcessNotFound
	}
	stdout, stderr, err := s.output(record)
	if err != nil {
		return ProcessOutput{}, err
	}
	b, truncated := stdout, record.Operation.StdoutTruncated
	if r.Stream == "stderr" {
		b, truncated = stderr, record.Operation.StderrTruncated
	}
	if r.Limit == 0 {
		r.Limit = 16 << 10
	}
	start := r.Offset
	if start > int64(len(b)) {
		start = int64(len(b))
	}
	end := start + int64(r.Limit)
	if end > int64(len(b)) {
		end = int64(len(b))
	}
	// Keep sequential UTF-8 pages on code-point boundaries. Arbitrary offsets
	// inside a code point are lossy text reads, but never exceed the byte limit.
	if end < int64(len(b)) {
		for end > start && !utf8.RuneStart(b[end]) {
			end--
		}
		if end == start && start < int64(len(b)) {
			_, size := utf8.DecodeRune(b[start:])
			end = start + int64(size)
		}
	}
	nextOffset := end
	if nextOffset < r.Offset {
		nextOffset = r.Offset
	}
	return ProcessOutput{OperationID: r.OperationID, Stream: r.Stream, Offset: r.Offset, NextOffset: nextOffset, Content: string(b[start:end]), EOF: record.Operation.State.terminal() && end == int64(len(b)), Truncated: truncated}, nil
}
func (service *Service) CancelProcess(ctx context.Context, r ProcessLookupRequest) (ProcessOperation, error) {
	if err := r.Validate(); err != nil {
		return ProcessOperation{}, err
	}
	s := service.processes
	if s == nil {
		return ProcessOperation{}, ErrProcessNotFound
	}
	s.mu.Lock()
	record := s.records[r.OperationID]
	s.mu.Unlock()
	if record != nil {
		record.mu.Lock()
		defer record.mu.Unlock()
	}
	if record == nil || record.Operation.PersonalityAgentID != r.PersonalityAgentID {
		return ProcessOperation{}, ErrProcessNotFound
	}
	if record.Operation.State.terminal() {
		return record.Operation, nil
	}
	next := *record
	next.CancelRequested = true
	if err := s.save(&next); err != nil {
		return ProcessOperation{}, err
	}
	*record = next
	return record.Operation, nil
}
func (service *Service) PendingProcessCompletions(ctx context.Context) ([]ProcessOperation, error) {
	result := []ProcessOperation{}
	s := service.processes
	if s == nil {
		return result, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.records {
		r.mu.Lock()
		if r.Operation.State.terminal() && r.Receipt == nil {
			result = append(result, r.Operation)
		}
		r.mu.Unlock()
	}
	sort.Slice(result, func(i, j int) bool { return result[i].OperationID < result[j].OperationID })
	if len(result) > 0 {
		start := sort.Search(len(result), func(i int) bool { return result[i].OperationID > s.completionCursor })
		if start == len(result) {
			start = 0
		}
		count := len(result)
		// Four maximally JSON-escaped valid operations fit the 1 MiB
		// protocol response cap (32 KiB argv plus bounded metadata each).
		if count > 4 {
			count = 4
		}
		batch := make([]ProcessOperation, 0, count)
		for i := 0; i < count; i++ {
			batch = append(batch, result[(start+i)%len(result)])
		}
		s.completionCursor = batch[len(batch)-1].OperationID
		result = batch
	}
	return result, nil
}
func (service *Service) AcknowledgeProcessCompletion(ctx context.Context, r ProcessCompletionReceipt) error {
	if err := (ProcessLookupRequest{r.PersonalityAgentID, r.OperationID}).Validate(); err != nil {
		return err
	}
	if _, err := uuid.Parse(r.CommandID); err != nil || r.CommandSeq == 0 {
		return ErrInvalidProcessRequest
	}
	s := service.processes
	if s == nil {
		return ErrProcessNotFound
	}
	s.mu.Lock()
	record := s.records[r.OperationID]
	s.mu.Unlock()
	if record != nil {
		record.mu.Lock()
		defer record.mu.Unlock()
	}
	if record == nil || record.Operation.PersonalityAgentID != r.PersonalityAgentID {
		return ErrProcessNotFound
	}
	if !record.Operation.State.terminal() || record.Operation.EventID != r.EventID {
		return ErrConflict
	}
	if record.Receipt != nil {
		if *record.Receipt == r {
			return nil
		}
		return ErrConflict
	}
	next := *record
	next.Receipt = &r
	if err := s.save(&next); err != nil {
		return err
	}
	*record = next
	return nil
}

// RunProcessObserver belongs to the provisioner lifetime, never a client request.
// Docker containers continue across observer shutdown and are inspected on restart.
func (service *Service) RunProcessObserver(ctx context.Context) {
	if service.processes == nil {
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		service.observeProcesses(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (service *Service) observeProcesses(ctx context.Context) {
	s := service.processes
	s.mu.Lock()
	records := make([]*processRecord, 0, len(s.records))
	for _, r := range s.records {
		records = append(records, r)
	}
	s.mu.Unlock()
	for _, r := range records {
		if ctx.Err() != nil {
			return
		}
		bounded, cancel := context.WithTimeout(ctx, 20*time.Second)
		s.observe(bounded, r)
		cancel()
	}
}

// commitProcess publishes only durably stored state and preserves cancellation
// accepted while Docker inspection was in flight.
func (s *processStore) commitProcess(original, next *processRecord) error {
	original.mu.Lock()
	defer original.mu.Unlock()
	next.CancelRequested = original.CancelRequested
	next.Receipt = original.Receipt
	if next.Operation.State.terminal() && next.CancelRequested {
		next.Operation.State = ProcessCancelled
	}
	if reflect.DeepEqual(original, next) {
		return nil
	}
	if err := s.save(next); err != nil {
		return err
	}
	next.releaseTerminalOutput()
	*original = *next
	return nil
}
func terminalProcess(r *processRecord, state ProcessState, message string) {
	now := time.Now().UTC()
	if r.Operation.FinishedAt != nil {
		now = *r.Operation.FinishedAt
	}
	r.Operation.State = state
	r.Operation.FinishedAt = &now
	r.Operation.Error = message
}
func (s *processStore) observe(ctx context.Context, original *processRecord) {
	original.mu.Lock()
	if original.Operation.State.terminal() {
		next := *original
		original.mu.Unlock()
		if next.ContainerRemoved {
			return
		}
		if remover, ok := s.backend.(interface {
			RemoveProcess(context.Context, ProcessOperation) error
		}); ok {
			if remover.RemoveProcess(ctx, next.Operation) == nil {
				next.ContainerRemoved = true
				_ = s.commitProcess(original, &next)
			}
		}
		return
	}
	next := *original
	launch := !next.LaunchAttempted
	if launch {
		if next.CancelRequested || time.Since(next.Operation.OccurredAt) > time.Duration(next.Operation.TimeoutSeconds)*time.Second {
			if next.CancelRequested {
				terminalProcess(&next, ProcessCancelled, "")
			} else {
				terminalProcess(&next, ProcessFailed, "process deadline expired before launch")
			}
			if s.save(&next) == nil {
				next.releaseTerminalOutput()
				*original = next
			}
			original.mu.Unlock()
			return
		}
		next.LaunchAttempted = true
		if s.save(&next) != nil {
			original.mu.Unlock()
			return
		}
		*original = next
	}
	original.mu.Unlock()
	// No record or store mutex is held during Docker I/O.
	if launch {
		if err := s.backend.LaunchProcess(ctx, next.Operation); err != nil {
			next.Operation.Error = "process launch could not be confirmed"
		}
	}
	observation, err := s.backend.InspectProcess(ctx, next.Operation)
	if err != nil {
		return
	}
	if !observation.Exists {
		terminalProcess(&next, ProcessIndeterminate, "process container unavailable; operation will not be repeated")
		_ = s.commitProcess(original, &next)
		return
	}
	if !observation.StartedAt.IsZero() {
		next.Operation.StartedAt = &observation.StartedAt
		if next.Operation.Error == "process launch could not be confirmed" {
			next.Operation.Error = ""
		}
	}
	if !observation.OutputIncomplete {
		next.Stdout = observation.Stdout
		next.Stderr = observation.Stderr
	}
	next.Operation.StdoutBytes = int64(len(next.Stdout))
	next.Operation.StderrBytes = int64(len(next.Stderr))
	next.Operation.StdoutTruncated = next.Operation.StdoutTruncated || observation.StdoutTruncated
	next.Operation.StderrTruncated = next.Operation.StderrTruncated || observation.StderrTruncated
	original.mu.Lock()
	next.CancelRequested = original.CancelRequested
	original.mu.Unlock()
	if observation.Running {
		next.Operation.State = ProcessRunning
		if next.CancelRequested || time.Since(next.Operation.OccurredAt) > time.Duration(next.Operation.TimeoutSeconds)*time.Second {
			if err := s.backend.StopProcess(ctx, next.Operation); err != nil {
				return
			}
			if !next.CancelRequested {
				next.Operation.Error = "process timeout exceeded"
			}
		}
		_ = s.commitProcess(original, &next)
		return
	}
	next.Operation.ExitCode = &observation.ExitCode
	if !observation.FinishedAt.IsZero() {
		next.Operation.FinishedAt = &observation.FinishedAt
	}
	state := ProcessSucceeded
	if observation.ExitCode != 0 || next.Operation.Error != "" {
		state = ProcessFailed
	}
	if observation.StartedAt.IsZero() {
		state = ProcessIndeterminate
	}
	terminalProcess(&next, state, next.Operation.Error)
	_ = s.commitProcess(original, &next)
}
