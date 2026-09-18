package runtimeprovision

// Service-side supervision for interactive operations.
//
// Each live interactive operation owns one interactiveIO: a durable
// ttylog fed by a journal tailer, and a lazily attached input sink
// (`docker attach --no-stdout --no-stderr`). The tailer reads the
// daemon's own json-file journal at a byte offset persisted beside
// the scrollback — a provisioner restart resumes exactly where the
// committed offset left off, so no emitted byte is replayed into
// duplicate scrollback and a rotated/truncated journal is journaled
// as an explicit gap rather than silently skipped.
//
// Provisioner restart: the input-attach child dies with the process
// (Pdeathsig), store reload recreates interactiveIO for every
// non-terminal interactive record, and the tailer resumes at the
// persisted offset. The container itself belongs to the daemon and
// keeps running throughout — reclaim is reattachment to the exact
// same container.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// journalPollInterval is the tailer's idle cadence; journalReadChunk
// bounds one poll's read; journalStablePolls is the consecutive
// no-growth polls after which a terminal operation is declared fully
// drained; journalMissBound bounds terminal-state retries when the
// journal cannot be resolved at all.
const (
	journalPollInterval = 150 * time.Millisecond
	journalReadChunk    = 256 << 10
	journalStablePolls  = 4
	journalMissBound    = 20
)

// InteractiveProcessBackend is the backend contract beyond the batch
// ProcessBackend: journal resolution, a supervised input stream,
// resize, and foreground-group signal delivery. The fake backend in
// tests implements the same surface so session logic is exercisable
// without Docker.
type InteractiveProcessBackend interface {
	ProcessBackend
	ProcessJournalPath(ctx context.Context, o ProcessOperation) (string, error)
	OpenProcessInput(ctx context.Context, o ProcessOperation) (*processSink, error)
	SignalProcess(ctx context.Context, o ProcessOperation, signal string) error
	ResizeProcess(ctx context.Context, o ProcessOperation, cols, rows int) error
}

type interactiveIO struct {
	op   ProcessOperation
	tty  *ttyLog
	done chan struct{}

	// attached reports whether the journal pump currently has the
	// daemon journal open and is appending records. False on a live op
	// means output is degraded even though input may still be accepted.
	attached atomic.Bool

	mu     sync.Mutex
	sink   *processSink
	closed bool
}

// interactiveBackend resolves the extended backend or fails the call
// honestly — a deployment whose backend cannot serve interactive ops
// must not pretend the bytes landed.
func (s *processStore) interactiveBackend() (InteractiveProcessBackend, error) {
	ib, ok := s.backend.(InteractiveProcessBackend)
	if !ok {
		return nil, errors.New("process backend does not support interactive operations")
	}
	return ib, nil
}

// ensureInteractive returns the op's supervisor, creating it (and its
// durable ttylog) on first use. Callers hold no locks.
func (s *processStore) ensureInteractive(r *processRecord) (*interactiveIO, error) {
	// interactive is guarded by the record mutex: commits replace the
	// record under that lock and must see the live supervisor, not a
	// stale snapshot. Callers therefore never hold record.mu here.
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.interactive != nil {
		return r.interactive, nil
	}
	tty, err := openTTYLog(s.directory, r.Operation.OperationID)
	if err != nil {
		return nil, err
	}
	io_ := &interactiveIO{op: r.Operation, tty: tty, done: make(chan struct{})}
	r.interactive = io_
	if !r.Operation.State.terminal() {
		go s.superviseInteractive(io_)
	}
	return io_, nil
}

// interactiveIOFor returns the op's existing supervisor without
// creating one, for read paths that must not start pumps.
func (s *processStore) interactiveIOFor(opID string) *interactiveIO {
	s.mu.Lock()
	r := s.records[opID]
	s.mu.Unlock()
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.interactive
}

// superviseInteractive owns output capture for one operation. It
// resolves the container's journal and tails it; when the operation
// goes terminal it drains until the journal stops growing, then
// returns. An unresolvable journal while the op is live retries
// forever; after terminal it is bounded — the op is already an
// outcome, and an explicit gap is recorded if the tail is lost.
func (s *processStore) superviseInteractive(io_ *interactiveIO) {
	ib, err := s.interactiveBackend()
	if err != nil {
		return
	}
	ctx := context.Background()
	misses := 0
	for {
		alive := !s.interactiveTerminal(io_.op.OperationID)
		path, err := ib.ProcessJournalPath(ctx, io_.op)
		if err != nil {
			if !alive {
				misses++
				if misses >= journalMissBound {
					_ = io_.tty.recordGap("final output journal could not be resolved")
					return
				}
			}
			if !s.waitInteractive(io_, journalPollInterval) && alive {
				return
			}
			continue
		}
		if s.pumpJournal(io_, path, alive) && !alive {
			return
		}
		if !alive {
			// The journal resolved but the tail faulted (open/stat/
			// append). Bounded like a resolve miss: the op is terminal,
			// so an explicit gap beats spinning forever.
			misses++
			if misses >= journalMissBound {
				_ = io_.tty.recordGap("final output journal could not be read")
				return
			}
			continue
		}
		misses = 0
		if !s.waitInteractive(io_, journalPollInterval) {
			return
		}
	}
}

// pumpJournal tails one resolved journal file until it rotates, the
// operation is declared drained, or an I/O fault asks the caller to
// reopen. Returns true only when a terminal operation's journal was
// fully drained. The durable source position guarantees a reattach —
// whether stream death or provisioner restart — resumes at the byte
// after the last committed record, verified against the retained
// scrollback so an uncommitted append cannot silently duplicate.
func (s *processStore) pumpJournal(io_ *interactiveIO, path string, alive bool) (drained bool) {
	defer func() {
		if !drained {
			io_.attached.Store(false)
		}
	}()
	pos := io_.tty.loadSrc()
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return false
	}
	ino := journalInode(st)
	if pos.Ino != 0 && (pos.Ino != ino || pos.Off > st.Size()) {
		// Rotated or truncated: bytes between the committed offset
		// and the old file's end are unrecoverable.
		_ = io_.tty.recordGap("container output journal rotated or truncated; bytes emitted in the window are lost")
		pos = ttySrc{Ino: ino, Off: 0}
	} else {
		// Verify the committed cursor against the retained scrollback:
		// a crash between append and cursor commit, or a lost cursor
		// file, must not silently re-append captured output.
		pos = recoverSource(io_.tty, f, ino, st.Size(), pos)
	}
	if pos.Ino == 0 {
		pos.Ino = ino
	}
	io_.attached.Store(true)
	stable := 0
	for {
		st, err := f.Stat()
		if err != nil {
			return false
		}
		avail := st.Size() - pos.Off
		if avail > 0 {
			want := avail
			if want > journalReadChunk {
				want = journalReadChunk
			}
			buf := make([]byte, want)
			m, rerr := f.ReadAt(buf, pos.Off)
			if rerr != nil && rerr != io.EOF {
				return false
			}
			n, derr := journalDrain(io_.tty, buf[:m])
			if derr != nil {
				return false
			}
			if n > 0 {
				pos.Off += int64(n)
				if serr := io_.tty.saveSrc(pos); serr != nil {
					return false
				}
				stable = 0
			} else {
				stable++
			}
		} else {
			stable++
		}
		if !alive && stable >= journalStablePolls {
			return true
		}
		if nst, serr := os.Stat(path); serr != nil || journalInode(nst) != pos.Ino {
			if !alive {
				// Journal gone or replaced after the op ended — the
				// remaining tail is lost; mark it and stop.
				_ = io_.tty.recordGap("container output journal vanished before drain completed")
				return true
			}
			return false
		}
		select {
		case <-io_.done:
			// done closes when the operation went terminal; fall
			// through to the drain instead of abandoning the tail.
		case <-time.After(journalPollInterval):
		}
		alive = !s.interactiveTerminal(io_.op.OperationID)
	}
}

// journalDrain consumes complete journal records from buf, appending
// their payloads to the tty log, and returns the committed byte
// count. A trailing partial line stays unconsumed — torn journal
// writes never corrupt the resume offset.
func journalDrain(tty *ttyLog, buf []byte) (int, error) {
	consumed := 0
	for len(buf) > 0 {
		i := bytes.IndexByte(buf, '\n')
		if i < 0 {
			break
		}
		line := buf[:i+1]
		var e journalEntry
		if jerr := json.Unmarshal(bytes.TrimSpace(line), &e); jerr == nil && e.Log != "" {
			if aerr := tty.append([]byte(e.Log)); aerr != nil {
				return consumed, aerr
			}
		}
		consumed += i + 1
		buf = buf[i+1:]
	}
	return consumed, nil
}

// waitInteractive sleeps between tail passes and reports whether
// supervision should continue.
func (s *processStore) waitInteractive(io_ *interactiveIO, d time.Duration) bool {
	select {
	case <-io_.done:
		return false
	case <-time.After(d):
		return true
	}
}

func (s *processStore) interactiveTerminal(opID string) bool {
	s.mu.Lock()
	r := s.records[opID]
	s.mu.Unlock()
	if r == nil {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.Operation.State.terminal()
}

// stopInteractive ends supervision. Called after the operation goes
// terminal; the tailer drains the journal's final bytes before its
// loop observes done, so ordering stays: last committed byte, then
// shutdown.
func (s *processStore) stopInteractive(io_ *interactiveIO) {
	io_.mu.Lock()
	defer io_.mu.Unlock()
	if !io_.closed {
		io_.closed = true
		close(io_.done)
		if io_.sink != nil {
			_ = io_.sink.Close()
			io_.sink = nil
		}
	}
}

// interactiveRecord resolves a persona-owned interactive operation for
// an input/resize/signal call, or fails the call with the honest
// contract error.
func (service *Service) interactiveRecord(r ProcessLookupRequest) (*processStore, *processRecord, error) {
	if err := r.Validate(); err != nil {
		return nil, nil, err
	}
	s := service.processes
	if s == nil {
		return nil, nil, ErrProcessNotFound
	}
	s.mu.Lock()
	record := s.records[r.OperationID]
	s.mu.Unlock()
	if record == nil {
		return nil, nil, ErrProcessNotFound
	}
	record.mu.Lock()
	op := record.Operation
	record.mu.Unlock()
	if op.PersonalityAgentID != r.PersonalityAgentID {
		return nil, nil, ErrProcessNotFound
	}
	if !op.Interactive {
		return nil, nil, ErrProcessNotInteractive
	}
	if op.State.terminal() {
		return nil, nil, ErrConflict
	}
	return s, record, nil
}

// WriteProcessInput delivers input to the interactive container and
// returns the delivery evidence. Delivery is a real stream write to
// the container's stdin, not a queued intent: the receipt says
// delivered / not-delivered / indeterminate, and the caller (the
// session driver) decides the durable input disposition from it.
func (service *Service) WriteProcessInput(ctx context.Context, r ProcessInputRequest) (ProcessInputReceipt, error) {
	if err := r.Validate(); err != nil {
		return ProcessInputReceipt{}, err
	}
	s, record, err := service.interactiveRecord(r.ProcessLookupRequest)
	if err != nil {
		return ProcessInputReceipt{}, err
	}
	ib, err := s.interactiveBackend()
	if err != nil {
		return ProcessInputReceipt{}, err
	}
	io_, err := s.ensureInteractive(record)
	if err != nil {
		return ProcessInputReceipt{}, err
	}
	return io_.writeInput(ctx, ib, r.Data, r.EOF), nil
}

// SignalProcess delivers one allowlisted signal to the session's
// current foreground process group — the same target the terminal's
// line discipline would hit for a keystroke signal — and returns the
// delivery evidence. Delivery is a verified host-side group signal,
// not a `docker kill` to PID 1.
func (service *Service) SignalProcess(ctx context.Context, r ProcessSignalRequest) (ProcessInputReceipt, error) {
	if err := r.Validate(); err != nil {
		return ProcessInputReceipt{}, err
	}
	s, _, err := service.interactiveRecord(r.ProcessLookupRequest)
	if err != nil {
		return ProcessInputReceipt{}, err
	}
	ib, err := s.interactiveBackend()
	if err != nil {
		return ProcessInputReceipt{}, err
	}
	record, err := func() (*processRecord, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if rec := s.records[r.OperationID]; rec != nil {
			return rec, nil
		}
		return nil, ErrProcessNotFound
	}()
	if err != nil {
		return ProcessInputReceipt{}, err
	}
	record.mu.Lock()
	op := record.Operation
	record.mu.Unlock()
	if err := ib.SignalProcess(ctx, op, r.Signal); err != nil {
		return ProcessInputReceipt{}, err
	}
	return ProcessInputReceipt{Delivered: true, Detail: "signal delivered to session foreground"}, nil
}

// ResizeProcess sets the TTY winsize. A backend or daemon that cannot
// resize reports ErrProcessResizeUnsupported — the caller surfaces
// the typed failure rather than pretending the resize landed.
func (service *Service) ResizeProcess(ctx context.Context, r ProcessResizeRequest) (ProcessOperation, error) {
	if err := r.Validate(); err != nil {
		return ProcessOperation{}, err
	}
	s, record, err := service.interactiveRecord(r.ProcessLookupRequest)
	if err != nil {
		return ProcessOperation{}, err
	}
	ib, err := s.interactiveBackend()
	if err != nil {
		return ProcessOperation{}, err
	}
	record.mu.Lock()
	op := record.Operation
	record.mu.Unlock()
	if !op.TTY {
		return ProcessOperation{}, fmt.Errorf("%w: operation has no tty", ErrProcessNotInteractive)
	}
	if err := ib.ResizeProcess(ctx, op, r.Cols, r.Rows); err != nil {
		return ProcessOperation{}, err
	}
	return op, nil
}

// writeInput delivers bytes (or an EOF) to the container's input and
// returns the delivery evidence. An attach that never opened proves
// non-delivery; a mid-stream failure is indeterminate — the prefix may
// already have taken effect.
func (io_ *interactiveIO) writeInput(ctx context.Context, ib InteractiveProcessBackend, data []byte, eof bool) ProcessInputReceipt {
	if eof {
		if io_.op.TTY {
			// EOT through the line discipline is the real EOF the
			// foreground reader sees.
			data = []byte{0x04}
		} else {
			return ProcessInputReceipt{Delivered: false, Detail: "eof unsupported without tty"}
		}
	}
	io_.mu.Lock()
	defer io_.mu.Unlock()
	if io_.closed {
		return ProcessInputReceipt{Delivered: false, Detail: "session input closed"}
	}
	for attempt := 0; attempt < 2; attempt++ {
		if io_.sink == nil {
			sink, err := ib.OpenProcessInput(ctx, io_.op)
			if err != nil {
				// The stream never opened: nothing was sent.
				return ProcessInputReceipt{Delivered: false, Detail: "input attach unavailable"}
			}
			io_.sink = sink
		}
		n, err := io_.sink.Write(data)
		if err == nil && n == len(data) {
			return ProcessInputReceipt{Delivered: true}
		}
		// The stream died mid-write or short-wrote: the container may
		// have received a prefix. Reattach and report indeterminate —
		// the caller decides the honest outcome, a blind retry would
		// duplicate keystrokes.
		_ = io_.sink.Close()
		io_.sink = nil
		if n > 0 || attempt == 1 {
			return ProcessInputReceipt{Indeterminate: true, Detail: "input stream lost mid-write"}
		}
	}
	return ProcessInputReceipt{Indeterminate: true, Detail: "input stream lost mid-write"}
}
