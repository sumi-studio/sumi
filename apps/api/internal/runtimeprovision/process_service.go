package runtimeprovision

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
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
	// interactive is the live supervisor for an interactive op —
	// runtime state only, never journaled. Its durable half (ttylog)
	// lives on disk and is reopened on demand.
	interactive *interactiveIO `json:"-"`

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
	// reverify re-checks a journaled operation's external launch
	// preconditions at the deferred launch point. The canonical workspace
	// bind is verified when the operation is accepted, but the mount can
	// change before the observer launches the container — a bind resolved
	// minutes ago proves nothing about the mount now. Failure holds the
	// operation (no LaunchAttempted) until the deadline path bounds it.
	reverify func(context.Context, ProcessOperation) error
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
	// Reattach output supervision for interactive ops that outlived a
	// provisioner restart. The containers belong to the daemon and are
	// still running; the first reattach journals an explicit gap for
	// the restart window.
	for _, r := range s.records {
		if r.Operation.Interactive && !r.Operation.State.terminal() {
			if _, err := s.ensureInteractive(r); err != nil {
				return nil, err
			}
		}
	}
	return s, nil
}

// Terminal records keep metadata only in RAM. The existing journal still
// contains the complete bounded output; metadata rewrites must rehydrate it.
// Interactive ops never populate Stdout/Stderr — their output is the
// durable ttylog — so there is nothing to release, and marking them
// unloaded would make every later save fail the integrity check that
// compares StdoutBytes (the ttylog total) against the empty snapshot.
func (r *processRecord) releaseTerminalOutput() {
	if r.Operation.State.terminal() && !r.Operation.Interactive {
		r.Stdout = nil
		r.Stderr = nil
		r.outputUnloaded = true
	}
}

// status returns the operation with computed, non-journalled evidence
// filled in: Quiesced certifies no physical writer remains for a
// terminal op (nothing launched, or the container was verifiably
// removed). Call with r.mu held or on a record that is not yet shared.
func (r *processRecord) status() ProcessOperation {
	o := r.Operation
	o.Quiesced = o.State.terminal() && (!r.LaunchAttempted || r.ContainerRemoved)
	return o
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
	id := processID(request)
	// The durable journal answers a replay first: an operation that already
	// exists returns its recorded state even when the canonical mount is
	// unreachable — results must survive environment loss, not depend on it.
	s.mu.Lock()
	r := s.records[id]
	s.mu.Unlock()
	if r != nil {
		o, err := replayProcessOperation(r, request)
		return o, err
	}
	// A canonical-scope launch resolves its verified bind before the record
	// is journalled so the workspace evidence travels with the operation and
	// the backend never has to ask where it came from. Failure here means no
	// record: a files-scope request on an unconfigured, unmounted, or
	// misbound volume refuses honestly instead of silently falling back to
	// the legacy shared volume.
	var workspaceBind, workspaceUUID string
	if request.Workspace == "files-scope" {
		var err error
		workspaceBind, workspaceUUID, err = service.resolveProcessWorkspace(ctx, request.PersonalityAgentID)
		if err != nil {
			return ProcessOperation{}, err
		}
	}
	// The journal write below is this operation's point of no return. A
	// request whose caller already gave up (client deadline/disconnect —
	// the exact delayed-response window the driver's fence protects
	// against) must not publish a record nobody is watching.
	if err := ctx.Err(); err != nil {
		return ProcessOperation{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// A racer resolved the same operation while the workspace check ran.
	if r := s.records[id]; r != nil {
		return replayProcessOperation(r, request)
	}
	// Interactive operations get their own capacity budget: a terminal
	// session held for hours must never starve the one-shot job slot,
	// and a burst of jobs must not leave a persona unable to open a
	// terminal. Batch caps are unchanged (1 per persona, 4 total);
	// interactive caps sit beside them (4 per persona, 8 total).
	active, interactiveActive := 0, 0
	samePersonaActive, samePersonaInteractive := 0, 0
	for _, r := range s.records {
		r.mu.Lock()
		o := r.Operation
		r.mu.Unlock()
		if o.State.terminal() {
			continue
		}
		if o.Interactive {
			interactiveActive++
			if o.PersonalityAgentID == request.PersonalityAgentID {
				samePersonaInteractive++
			}
			continue
		}
		active++
		if o.PersonalityAgentID == request.PersonalityAgentID {
			samePersonaActive++
		}
	}
	if request.Interactive {
		if samePersonaInteractive >= 4 || interactiveActive >= 8 {
			return ProcessOperation{}, ErrProcessBusy
		}
	} else if samePersonaActive >= 1 || active >= 4 {
		return ProcessOperation{}, ErrProcessBusy
	}
	event, err := uuid.NewV7()
	if err != nil {
		return ProcessOperation{}, err
	}
	// The save below is the publication boundary — the last point where
	// a caller that expired while waiting on s.mu or a record mutex can
	// still be turned away. After this check only a save error prevents
	// the journal write, and a journaled op is fenced work, not a zombie.
	if err := ctx.Err(); err != nil {
		return ProcessOperation{}, err
	}
	record := &processRecord{mu: &sync.Mutex{}, Operation: ProcessOperation{OperationID: id, PersonalityAgentID: request.PersonalityAgentID, OriginatingToolCallID: request.OriginatingToolCallID, Executable: request.Executable, Args: request.Args, Cwd: request.Cwd, TimeoutSeconds: request.TimeoutSeconds, Env: request.Env, Image: request.Image, WorkspaceBind: workspaceBind, FilesVolumeUUID: workspaceUUID, Interactive: request.Interactive, TTY: request.TTY, State: ProcessAccepted, EventID: event.String(), OccurredAt: time.Now().UTC()}}
	if err = s.save(record); err != nil {
		return ProcessOperation{}, err
	}
	s.records[id] = record
	return record.status(), nil
}

// replayProcessOperation compares a journalled operation to a repeated
// request: identical requests replay the recorded operation; any divergence
// in the identity-bearing fields is a conflict, never a second launch. The
// caller may hold s.mu — record locking follows the same ordering the busy
// scan uses.
func replayProcessOperation(r *processRecord, request ProcessStartRequest) (ProcessOperation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	o := r.Operation
	// A tombstone is the cancel fence: the operation was cancelled before
	// it ever journaled a launch, so the only honest answer to a late
	// start is the recorded cancellation — never a launch. The
	// deterministic operation ID already binds this record to the
	// (persona, tool call) the request names.
	if o.Tombstone {
		return r.status(), nil
	}
	if o.Executable != request.Executable || !reflect.DeepEqual(o.Args, request.Args) || o.Cwd != request.Cwd || o.TimeoutSeconds != request.TimeoutSeconds ||
		!maps.Equal(o.Env, request.Env) || o.Image != request.Image || (o.WorkspaceBind != "") != (request.Workspace == "files-scope") ||
		o.Interactive != request.Interactive || o.TTY != request.TTY {
		return ProcessOperation{}, ErrConflict
	}
	return r.status(), nil
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
	if record == nil {
		return ProcessOperation{}, ErrProcessNotFound
	}
	record.mu.Lock()
	op := record.status()
	record.mu.Unlock()
	if op.PersonalityAgentID != r.PersonalityAgentID {
		return ProcessOperation{}, ErrProcessNotFound
	}
	if op.Interactive {
		// Interactive cursors come from the durable ttylog, not the
		// batch output snapshot. ensureInteractive takes the store
		// mutex, so it must run after record.mu is released (the
		// established order is store → record).
		if io_, err := s.ensureInteractive(record); err == nil {
			base, total := io_.tty.stats()
			op.StdoutBase = base
			op.StdoutBytes = total
			op.OutputAttached = io_.attached.Load()
		}
	}
	return op, nil
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
	if record == nil {
		return ProcessOutput{}, ErrProcessNotFound
	}
	record.mu.Lock()
	operation := record.Operation
	record.mu.Unlock()
	if operation.PersonalityAgentID != r.PersonalityAgentID {
		return ProcessOutput{}, ErrProcessNotFound
	}
	if r.Limit == 0 {
		r.Limit = 16 << 10
	}
	if operation.Interactive {
		// Interactive streams are served from the durable ttylog with
		// absolute offsets: a TTY op has one merged stream on "stdout";
		// stderr reads return empty so callers learn there is no
		// second stream rather than receiving duplicated content.
		if r.Stream != "stdout" {
			return ProcessOutput{OperationID: r.OperationID, Stream: r.Stream, Offset: r.Offset, NextOffset: r.Offset, EOF: operation.State.terminal()}, nil
		}
		io_, err := s.ensureInteractive(record)
		if err != nil {
			return ProcessOutput{}, err
		}
		data, base, next, gap, err := io_.tty.read(r.Offset, r.Limit)
		if err != nil {
			return ProcessOutput{}, err
		}
		_, total := io_.tty.stats()
		out := ProcessOutput{
			OperationID: r.OperationID, Stream: r.Stream, Offset: r.Offset,
			NextOffset: next, Content: string(data),
			EOF:        operation.State.terminal() && next >= total,
			Truncated:  gap,
			BaseOffset: base, Gap: gap,
		}
		// Journaled loss boundaries (rotation, vanished journal,
		// uncertified resume) must reach readers too — otherwise a
		// rotation under load is a silent hole in the scrollback.
		// Return every boundary at or ahead of the requested offset;
		// the caller emits an explicit gap marker when its cursor
		// reaches each one. Unknown-size loss stays a boundary, never
		// an invented byte range.
		if events, err := io_.tty.gapsAtOrAfter(r.Offset); err == nil {
			for _, ev := range events {
				out.Gaps = append(out.Gaps, ProcessOutputGap{At: ev.At, Note: ev.Note})
			}
		}
		return out, nil
	}
	record.mu.Lock()
	defer record.mu.Unlock()
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
	if record == nil {
		if !r.TombstoneIfAbsent {
			s.mu.Unlock()
			return ProcessOperation{}, ErrProcessNotFound
		}
		// The cancel fence: journal a terminal cancellation for an
		// operation that never arrived. This check-and-create runs under
		// the same store mutex as StartProcess's journal write, so the
		// order is deterministic — whichever lands first wins. A start
		// still resolving its workspace replays the tombstone; a start
		// that already journaled is found by the normal cancel path
		// below. After this write, 'the operation never existed' is a
		// publishable fact, not a timeout guess.
		event, err := uuid.NewV7()
		if err != nil {
			s.mu.Unlock()
			return ProcessOperation{}, err
		}
		now := time.Now().UTC()
		rec := &processRecord{mu: &sync.Mutex{}, Operation: ProcessOperation{
			OperationID:           r.OperationID,
			PersonalityAgentID:    r.PersonalityAgentID,
			OriginatingToolCallID: r.OriginatingToolCallID,
			State:                 ProcessCancelled,
			Tombstone:             true,
			EventID:               event.String(),
			OccurredAt:            now,
			FinishedAt:            &now,
			Error:                 "cancelled before the operation was journaled",
		}}
		if err := s.save(rec); err != nil {
			s.mu.Unlock()
			return ProcessOperation{}, err
		}
		rec.releaseTerminalOutput()
		s.records[r.OperationID] = rec
		s.mu.Unlock()
		return rec.status(), nil
	}
	s.mu.Unlock()
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.Operation.PersonalityAgentID != r.PersonalityAgentID {
		return ProcessOperation{}, ErrProcessNotFound
	}
	if record.Operation.State.terminal() {
		return record.status(), nil
	}
	next := *record
	next.CancelRequested = true
	if err := s.save(&next); err != nil {
		return ProcessOperation{}, err
	}
	*record = next
	return record.status(), nil
}

// ReleaseProcessTombstone lifts a cancel fence that was planted before
// the operation ever journaled a launch. The tombstone exists to make a
// delayed StartProcess replay 'cancelled'; it is released only by a
// caller that has independently re-authorized the launch (the termexec
// driver does so inside the persona's launch fence, where the persona
// authority is proven 'active' under the row lock — i.e. the move that
// planted the fence was cancelled). A record that is absent, was never
// fenced, or actually attempted a launch is never deleted: releasing
// anything else would rewrite real operation history.
func (service *Service) ReleaseProcessTombstone(ctx context.Context, r ProcessLookupRequest) (ProcessOperation, error) {
	if err := r.Validate(); err != nil {
		return ProcessOperation{}, err
	}
	s := service.processes
	if s == nil {
		return ProcessOperation{}, ErrProcessNotFound
	}
	s.mu.Lock()
	record := s.records[r.OperationID]
	if record != nil {
		record.mu.Lock()
	}
	if record == nil {
		s.mu.Unlock()
		return ProcessOperation{}, ErrProcessNotFound
	}
	op := record.status()
	if op.PersonalityAgentID != r.PersonalityAgentID {
		record.mu.Unlock()
		s.mu.Unlock()
		return ProcessOperation{}, ErrProcessNotFound
	}
	if !op.Tombstone || !op.State.terminal() || record.LaunchAttempted {
		record.mu.Unlock()
		s.mu.Unlock()
		return ProcessOperation{}, fmt.Errorf("%w: operation %s is not a never-launched tombstone", ErrConflict, r.OperationID)
	}
	// Remove the journal file first, while the store mutex and the record
	// lock are both held: a racing StartProcess for the same id cannot
	// interleave a fresh journal between the file delete and the map
	// delete. On failure the in-memory fence is retained — the release
	// stays retriable and a delayed start still replays the tombstone
	// rather than launching on a fence that outlived its record.
	err := s.removeLocked(record)
	if err != nil {
		record.mu.Unlock()
		s.mu.Unlock()
		return ProcessOperation{}, err
	}
	delete(s.records, r.OperationID)
	record.mu.Unlock()
	s.mu.Unlock()
	return op, nil
}

// removeLocked deletes a record's journal file and fsyncs the directory.
// The caller holds s.mu and the record's mutex.
func (s *processStore) removeLocked(r *processRecord) error {
	name := filepath.Join(s.directory, r.Operation.OperationID+".json")
	if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	d, err := os.Open(s.directory)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
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
		// Tombstones are cancel fences, not executions — no command
		// channel awaits their outcome, so they never enter the pending
		// feed (which would otherwise retain them forever).
		if r.Operation.State.terminal() && r.Receipt == nil && !r.Operation.Tombstone {
			result = append(result, r.status())
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
	if err := (ProcessLookupRequest{PersonalityAgentID: r.PersonalityAgentID, OperationID: r.OperationID}).Validate(); err != nil {
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

// assignRecord writes the mutable record state back, preserving the
// mutex pointer: a whole-struct copy would write the mu field too, and
// any goroutine loading r.mu to acquire it would race that write.
func assignRecord(original, next *processRecord) {
	original.outputUnloaded = next.outputUnloaded
	original.interactive = next.interactive
	original.Operation = next.Operation
	original.ContainerRemoved = next.ContainerRemoved
	original.LaunchAttempted = next.LaunchAttempted
	original.CancelRequested = next.CancelRequested
	original.Receipt = next.Receipt
	original.Stdout = next.Stdout
	original.Stderr = next.Stderr
}

// commitProcess publishes only durably stored state and preserves cancellation
// accepted while Docker inspection was in flight.
func (s *processStore) commitProcess(original, next *processRecord) error {
	original.mu.Lock()
	defer original.mu.Unlock()
	next.CancelRequested = original.CancelRequested
	next.Receipt = original.Receipt
	// interactive is runtime state, not journaled: the snapshot taken
	// before Docker I/O predates any supervisor a late attach created,
	// so the live pointer is carried across the commit — writing back
	// a stale nil would orphan one pump and let the next attach start
	// a second writer on the same tty log.
	next.interactive = original.interactive
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
	assignRecord(original, next)
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
		// A terminal verdict is not physical-stop proof: a launch may have
		// landed after the last inspect, or an earlier inspect may have
		// raced a still-starting container. StopProcess is a no-op when
		// the container is absent or already stopped, so it is safe to
		// issue unconditionally before removal. ContainerRemoved commits
		// only when the operation container is verifiably gone; while a
		// stop or remove keeps failing the op stays non-quiesced and the
		// reconcile retries each tick instead of certifying a live writer.
		_ = s.backend.StopProcess(ctx, next.Operation)
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
				next.interactive = original.interactive
				assignRecord(original, &next)
			}
			original.mu.Unlock()
			return
		}
		// The deferred launch re-verifies the recorded workspace: a mount
		// that changed or died since acceptance must not receive a bind.
		// Hold the operation unlaunched — the deadline branch bounds it.
		if s.reverify != nil {
			if err := s.reverify(ctx, next.Operation); err != nil {
				original.mu.Unlock()
				return
			}
		}
		next.LaunchAttempted = true
		if s.save(&next) != nil {
			original.mu.Unlock()
			return
		}
		next.interactive = original.interactive
		assignRecord(original, &next)
	}
	original.mu.Unlock()
	// No record or store mutex is held during Docker I/O.
	if launch {
		if err := s.backend.LaunchProcess(ctx, next.Operation); err != nil {
			next.Operation.Error = "process launch could not be confirmed"
		}
		if next.Operation.Interactive {
			// Start output supervision regardless of launch outcome —
			// the pump's own reattach loop tolerates a container that
			// is not up yet, and stopping it only happens on a
			// terminal state.
			if io_, err := s.ensureInteractive(original); err == nil {
				_ = io_
			}
		}
	}
	observation, err := s.backend.InspectProcess(ctx, next.Operation)
	if err != nil {
		return
	}
	if !observation.Exists {
		terminalProcess(&next, ProcessIndeterminate, "process container unavailable; operation will not be repeated")
		_ = s.commitProcess(original, &next)
		if io_ := s.interactiveIOFor(next.Operation.OperationID); io_ != nil {
			s.stopInteractive(io_)
		}
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
	if next.Operation.Interactive {
		// The output child exits with the container and the pump's
		// copy drains the last bytes; give the drain a short window
		// before snapshotting cursors and ending supervision.
		if io_ := s.interactiveIOFor(next.Operation.OperationID); io_ != nil {
			time.Sleep(300 * time.Millisecond)
			base, total := io_.tty.stats()
			original.mu.Lock()
			original.Operation.StdoutBase = base
			original.Operation.StdoutBytes = total
			_ = s.save(original)
			original.mu.Unlock()
			s.stopInteractive(io_)
		}
	}
}
