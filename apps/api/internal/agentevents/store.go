package agentevents

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// CommandStore is a durable, append-only per-personality-agent command log. It
// implements CommandAppender and exposes read-only introspection helpers for
// verification; it does not wire the WebSocket CommandSource seam (that is
// intentionally isolated for T17/T26).
//
// Each personality agent has one JSON Lines command log. Seq allocation, append, sync
// and rollback are serialized by an advisory flock on the log file so multiple
// API processes sharing SUMI_COMMAND_LOG_DIR cannot allocate duplicate seqs,
// interleave writes, or roll back another process's committed record. In
// addition, a process-level mutex protects the in-memory state map and a
// per-personality-agent mutex protects each cached state without serializing
// unrelated flock waits or scans. Command bytes are fsynced to the log before
// success is returned. After any restart, OpenCommandStore re-reads the logs
// and reconstructs the per-personality-agent next seq and idempotency maps, so
// restart preserves the log and allocation continuity.
type CommandStore struct {
	mu              sync.Mutex
	dir             string
	states          map[string]*personalityAgentState
	idempotencyLock *os.File
	// idempotencyGuard serializes keyed appends that share this store's one
	// flock file description. flock alone does not exclude goroutines using
	// the same open file description.
	idempotencyGuard chan struct{}
	closed           bool
}

// fileHandle abstracts the per-personality-agent log file so tests can inject
// deterministic failures without changing production call sites.
type fileHandle interface {
	io.Seeker
	io.Reader
	io.Writer
	Sync() error
	Truncate(size int64) error
	Close() error
	// Fd returns the underlying file descriptor for cross-process flock.
	Fd() uintptr
}

type personalityAgentState struct {
	mu          sync.Mutex
	path        string
	file        fileHandle
	nextSeq     uint64
	commands    []CommandEnvelope
	bySeq       map[uint64]int
	byCommandID map[string]int
	byKey       map[string]int // idempotency key -> commands index
	fileSize    int64          // end offset observed at last scan
	poisoned    bool
	poisonErr   error
	closed      bool
}

func lockMutexContext(ctx context.Context, mu *sync.Mutex) error {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !mu.TryLock() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	return nil
}

// flockContext is cancellation-aware and retries EINTR. All durable log
// operations use it so a blocked cross-process lock cannot hold a global
// process mutex or outlive the caller's connection.
func flockContext(ctx context.Context, fd uintptr, mode int) error {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		err := syscall.Flock(int(fd), mode|syscall.LOCK_NB)
		if err == nil {
			return nil
		}
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// unlockFile releases the advisory flock. Errors are ignored because the fd may
// already be closed by poisonLocked, which also releases the kernel lock.
func unlockFile(f fileHandle) error {
	if f == nil {
		return nil
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return nil
}

// LogRecord is the on-disk representation of a command. It embeds the public
// CommandEnvelope so the wire-compatible fields are written unchanged, and adds
// storage-only metadata (idempotency_key) that is kept out of the public API.
type LogRecord struct {
	CommandEnvelope
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

// UnmarshalJSON makes durable corruption fail closed: JSON's default decoder
// accepts duplicate fields, which would otherwise let a later key silently
// rewrite a command record during recovery.
func (r *LogRecord) UnmarshalJSON(data []byte) error {
	if err := checkDuplicateKeys(data); err != nil {
		return fmt.Errorf("command log record json: %w", err)
	}
	type rawRecord struct {
		Seq                *uint64               `json:"seq"`
		CommandID          *string               `json:"command_id"`
		PersonalityAgentID *string               `json:"personality_agent_id"`
		Provenance         *DirectChatProvenance `json:"provenance"`
		Command            json.RawMessage       `json:"command"`
		IdempotencyKey     string                `json:"idempotency_key"`
	}
	var raw rawRecord
	if err := unmarshalStrict(data, &raw); err != nil {
		return err
	}
	if raw.Seq == nil || raw.CommandID == nil || raw.PersonalityAgentID == nil || raw.Provenance == nil || len(raw.Command) == 0 {
		return errors.New("seq, command_id, personality_agent_id, provenance, and command are required")
	}
	if *raw.Seq > maxJSONSafeInteger {
		return fmt.Errorf("seq %d exceeds JSON-safe integer range", *raw.Seq)
	}
	if !canonicalUUIDRegexp.MatchString(*raw.CommandID) {
		return errors.New("command_id must be a canonical UUID")
	}
	if err := ValidatePersonalityAgentID(*raw.PersonalityAgentID); err != nil {
		return err
	}
	if *raw.PersonalityAgentID != raw.Provenance.PersonalityAgentID {
		return errors.New("persisted command target does not match provenance target")
	}
	if err := ValidateCommand(raw.Command); err != nil {
		return fmt.Errorf("invalid persisted command: %w", err)
	}
	*r = LogRecord{CommandEnvelope: CommandEnvelope{
		Seq:                *raw.Seq,
		CommandID:          *raw.CommandID,
		PersonalityAgentID: *raw.PersonalityAgentID,
		Provenance:         *raw.Provenance,
		Command:            raw.Command,
	}, IdempotencyKey: raw.IdempotencyKey}
	return nil
}

// ErrSeqExhausted is returned by CommandStore.Append when the next allocated
// sequence number would exceed the JSON-safe integer boundary exposed to clients.
var ErrSeqExhausted = errors.New("command sequence number exhausted")

// OpenCommandStore opens or creates the command log under dir.
func OpenCommandStore(dir string) (*CommandStore, error) {
	if dir == "" {
		return nil, errors.New("command log directory is required")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve command log dir: %w", err)
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return nil, fmt.Errorf("create command log dir: %w", err)
	}

	info, err := os.Lstat(abs)
	if err != nil {
		return nil, fmt.Errorf("stat command log dir: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("command log directory must not be a symlink")
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("command log path is not a directory")
	}

	s := &CommandStore{
		dir:              abs,
		states:           make(map[string]*personalityAgentState),
		idempotencyGuard: make(chan struct{}, 1),
	}
	idempotencyLockPath := filepath.Join(abs, ".idempotency.lock")
	idempotencyLock, err := os.OpenFile(idempotencyLockPath, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open global idempotency lock: %w", err)
	}
	s.idempotencyLock = idempotencyLock

	matches, err := filepath.Glob(filepath.Join(abs, "commands-*.jsonl"))
	if err != nil {
		_ = idempotencyLock.Close()
		return nil, fmt.Errorf("scan command log dir: %w", err)
	}
	sort.Strings(matches)
	for _, path := range matches {
		personalityAgentID, err := personalityAgentIDFromPath(path)
		if err != nil {
			_ = idempotencyLock.Close()
			return nil, fmt.Errorf("invalid command log file %q: %w", path, err)
		}
		st := newPersonalityAgentState(path)
		if err := s.loadStateLocked(context.Background(), st, personalityAgentID); err != nil {
			_ = idempotencyLock.Close()
			return nil, fmt.Errorf("load command log %q: %w", path, err)
		}
		s.states[personalityAgentID] = st
	}
	return s, nil
}

// Close flushes and closes all open log files.
func (s *CommandStore) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	states := make([]*personalityAgentState, 0, len(s.states))
	for _, st := range s.states {
		states = append(states, st)
	}
	s.mu.Unlock()

	var firstErr error
	for _, st := range states {
		st.mu.Lock()
		if st.file != nil {
			if err := st.file.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			st.file = nil
		}
		st.closed = true
		st.mu.Unlock()
	}
	// A keyed append holds this guard from before its flock acquisition until
	// after its per-PAID append completes. Taking it here keeps the shared flock
	// descriptor alive for every in-flight keyed path.
	s.idempotencyGuard <- struct{}{}
	if s.idempotencyLock != nil {
		if err := s.idempotencyLock.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.idempotencyLock = nil
	}
	<-s.idempotencyGuard
	return firstErr
}

func (s *CommandStore) acquireIdempotencyGuard(ctx context.Context) error {
	select {
	case s.idempotencyGuard <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *CommandStore) releaseIdempotencyGuard() {
	<-s.idempotencyGuard
}

// poisonLocked marks a personality-agent state as unusable and closes its file so
// no later append can reuse a seq or continue from uncertain bytes.
func (s *CommandStore) poisonLocked(st *personalityAgentState, reason error) {
	if st.file != nil {
		_ = st.file.Close()
		st.file = nil
	}
	st.poisoned = true
	st.poisonErr = reason
}

// rollbackLocked attempts to truncate the log back to offset and fsync. If the
// rollback cannot be durably confirmed, it poisons the personality-agent state.
func (s *CommandStore) rollbackLocked(st *personalityAgentState, offset int64, origErr error) error {
	var truncErr, syncErr error
	if st.file != nil {
		truncErr = st.file.Truncate(offset)
		syncErr = st.file.Sync()
	}
	if truncErr != nil || syncErr != nil {
		reason := fmt.Errorf("append failure %v; rollback could not be confirmed (truncate=%v, sync=%v)", origErr, truncErr, syncErr)
		s.poisonLocked(st, reason)
		return reason
	}
	return nil
}

// Append durably appends a validated command to the personality agent's log.
// If idempotencyKey is non-empty and a command was previously accepted with
// the same key and identical authenticated envelope, the existing
// CommandEnvelope is returned without allocating a second seq. A changed
// target, tenant, actor, source, or command is a conflict.
func (s *CommandStore) Append(ctx context.Context, provenance DirectChatProvenance, idempotencyKey string, command json.RawMessage) (CommandEnvelope, error) {
	if err := ctx.Err(); err != nil {
		return CommandEnvelope{}, err
	}
	if err := ValidateCommand(command); err != nil {
		return CommandEnvelope{}, fmt.Errorf("validate command before append: %w", err)
	}

	if idempotencyKey != "" {
		if err := s.acquireIdempotencyGuard(ctx); err != nil {
			return CommandEnvelope{}, fmt.Errorf("lock in-process global idempotency index: %w", err)
		}
		defer s.releaseIdempotencyGuard()

		if err := lockMutexContext(ctx, &s.mu); err != nil {
			return CommandEnvelope{}, err
		}
		if s.closed || s.idempotencyLock == nil {
			s.mu.Unlock()
			return CommandEnvelope{}, errors.New("command store is closed")
		}
		idempotencyLock := s.idempotencyLock
		s.mu.Unlock()

		if err := flockContext(ctx, idempotencyLock.Fd(), syscall.LOCK_EX); err != nil {
			return CommandEnvelope{}, fmt.Errorf("lock global idempotency index: %w", err)
		}
		defer func() { _ = syscall.Flock(int(idempotencyLock.Fd()), syscall.LOCK_UN) }()

		existing, found, err := s.findIdempotencyRecord(ctx, idempotencyKey)
		if err != nil {
			return CommandEnvelope{}, err
		}
		if found {
			if existing.PersonalityAgentID == provenance.PersonalityAgentID &&
				existing.Provenance == provenance &&
				string(existing.Command) == string(command) {
				return existing, nil
			}
			return CommandEnvelope{}, fmt.Errorf("idempotency key %q reused with different command: %w", idempotencyKey, errIdempotencyConflict)
		}
	}

	if err := provenance.Validate(); err != nil {
		return CommandEnvelope{}, fmt.Errorf("validate provenance before append: %w", err)
	}
	personalityAgentID := provenance.PersonalityAgentID

	st, err := s.lockPersonalityAgent(ctx, personalityAgentID)
	if err != nil {
		return CommandEnvelope{}, err
	}
	defer st.mu.Unlock()
	if st.poisoned {
		return CommandEnvelope{}, fmt.Errorf("command log for %q is poisoned: %w", personalityAgentID, st.poisonErr)
	}

	if err := flockContext(ctx, st.file.Fd(), syscall.LOCK_EX); err != nil {
		return CommandEnvelope{}, fmt.Errorf("lock command log for %q: %w", personalityAgentID, err)
	}
	defer func() { _ = unlockFile(st.file) }()

	if err := s.refreshStateLocked(ctx, st, personalityAgentID); err != nil {
		return CommandEnvelope{}, err
	}

	if st.nextSeq > maxJSONSafeInteger {
		return CommandEnvelope{}, ErrSeqExhausted
	}

	commandID, err := newCommandID()
	if err != nil {
		return CommandEnvelope{}, fmt.Errorf("generate command_id: %w", err)
	}

	seq := st.nextSeq
	env := CommandEnvelope{
		Seq:                seq,
		CommandID:          commandID,
		PersonalityAgentID: personalityAgentID,
		Provenance:         provenance,
		Command:            command,
	}

	offset := st.fileSize

	record := LogRecord{CommandEnvelope: env}
	if idempotencyKey != "" {
		record.IdempotencyKey = idempotencyKey
	}

	line, err := json.Marshal(record)
	if err != nil {
		return CommandEnvelope{}, fmt.Errorf("marshal command log: %w", err)
	}

	data := append(line, '\n')
	written, writeErr := st.file.Write(data)
	if writeErr != nil || written != len(data) {
		var opErr error
		if writeErr != nil {
			opErr = fmt.Errorf("write command log: %w", writeErr)
		} else {
			opErr = fmt.Errorf("short write to command log: wrote %d of %d bytes", written, len(data))
		}
		if rbErr := s.rollbackLocked(st, offset, opErr); rbErr != nil {
			return CommandEnvelope{}, rbErr
		}
		return CommandEnvelope{}, opErr
	}

	if syncErr := st.file.Sync(); syncErr != nil {
		opErr := fmt.Errorf("sync command log: %w", syncErr)
		if rbErr := s.rollbackLocked(st, offset, opErr); rbErr != nil {
			return CommandEnvelope{}, rbErr
		}
		return CommandEnvelope{}, opErr
	}

	st.nextSeq = seq + 1
	idx := len(st.commands)
	st.commands = append(st.commands, env)
	st.bySeq[seq] = idx
	st.byCommandID[commandID] = idx
	if idempotencyKey != "" {
		st.byKey[idempotencyKey] = idx
	}
	st.fileSize = offset + int64(len(data))

	return env, nil
}

// findIdempotencyRecord scans only records that contain storage-authored
// idempotency metadata. Every keyed writer holds idempotencyLock, so those
// records are stable even while unrelated unkeyed logs append concurrently.
func (s *CommandStore) findIdempotencyRecord(ctx context.Context, idempotencyKey string) (CommandEnvelope, bool, error) {
	matches, err := filepath.Glob(filepath.Join(s.dir, "commands-*.jsonl"))
	if err != nil {
		return CommandEnvelope{}, false, err
	}
	sort.Strings(matches)
	for _, path := range matches {
		if err := ctx.Err(); err != nil {
			return CommandEnvelope{}, false, err
		}
		file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return CommandEnvelope{}, false, fmt.Errorf("open command log for global idempotency scan: %w", err)
		}
		reader := bufio.NewReader(file)
		for {
			line, readErr := reader.ReadBytes('\n')
			if bytes.Contains(line, []byte(`"idempotency_key"`)) {
				var record LogRecord
				if err := json.Unmarshal(bytes.TrimSpace(line), &record); err != nil {
					_ = file.Close()
					return CommandEnvelope{}, false, fmt.Errorf("decode keyed record during global idempotency scan: %w", err)
				}
				if record.IdempotencyKey == idempotencyKey {
					_ = file.Close()
					return record.CommandEnvelope, true, nil
				}
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				_ = file.Close()
				return CommandEnvelope{}, false, readErr
			}
		}
		if err := file.Close(); err != nil {
			return CommandEnvelope{}, false, err
		}
	}
	return CommandEnvelope{}, false, nil
}

// NextCommandSeq returns the next seq that would be allocated for personalityAgentID.
func (s *CommandStore) NextCommandSeq(ctx context.Context, personalityAgentID string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	st, err := s.lockPersonalityAgent(ctx, personalityAgentID)
	if err != nil {
		return 0, err
	}
	defer st.mu.Unlock()
	if st.poisoned {
		return 0, fmt.Errorf("command log for %q is poisoned: %w", personalityAgentID, st.poisonErr)
	}
	if err := flockContext(ctx, st.file.Fd(), syscall.LOCK_EX); err != nil {
		return 0, fmt.Errorf("lock command log for %q: %w", personalityAgentID, err)
	}
	defer func() { _ = unlockFile(st.file) }()
	if err := s.refreshStateLocked(ctx, st, personalityAgentID); err != nil {
		return 0, err
	}
	return st.nextSeq, nil
}

// FirstCommandSeq returns the first durable seq for personalityAgentID. If the log
// is empty, it returns 1 so catch-up can use it as the lower bound.
func (s *CommandStore) FirstCommandSeq(ctx context.Context, personalityAgentID string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	st, err := s.lockPersonalityAgent(ctx, personalityAgentID)
	if err != nil {
		return 0, err
	}
	defer st.mu.Unlock()
	if st.poisoned {
		return 0, fmt.Errorf("command log for %q is poisoned: %w", personalityAgentID, st.poisonErr)
	}
	if err := flockContext(ctx, st.file.Fd(), syscall.LOCK_EX); err != nil {
		return 0, fmt.Errorf("lock command log for %q: %w", personalityAgentID, err)
	}
	defer func() { _ = unlockFile(st.file) }()
	if err := s.refreshStateLocked(ctx, st, personalityAgentID); err != nil {
		return 0, err
	}
	if len(st.commands) == 0 {
		return 1, nil
	}
	return st.commands[0].Seq, nil
}

// HasCommands distinguishes an empty log from a log whose first retained
// sequence happens to be one. Reconnecting agents with durable progress must
// never be silently stranded by a lost command log.
func (s *CommandStore) HasCommands(ctx context.Context, personalityAgentID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	st, err := s.lockPersonalityAgent(ctx, personalityAgentID)
	if err != nil {
		return false, err
	}
	defer st.mu.Unlock()
	if st.poisoned {
		return false, fmt.Errorf("command log for %q is poisoned: %w", personalityAgentID, st.poisonErr)
	}
	if err := flockContext(ctx, st.file.Fd(), syscall.LOCK_EX); err != nil {
		return false, fmt.Errorf("lock command log for %q: %w", personalityAgentID, err)
	}
	defer func() { _ = unlockFile(st.file) }()
	if err := s.refreshStateLocked(ctx, st, personalityAgentID); err != nil {
		return false, err
	}
	return len(st.commands) != 0, nil
}

// GetCommand returns a single command by exact seq. It is preferred over
// CatchUp(seq) when the caller needs exactly one command.
func (s *CommandStore) GetCommand(ctx context.Context, personalityAgentID string, seq uint64) (CommandEnvelope, bool, error) {
	if err := ctx.Err(); err != nil {
		return CommandEnvelope{}, false, err
	}
	st, err := s.lockPersonalityAgent(ctx, personalityAgentID)
	if err != nil {
		return CommandEnvelope{}, false, err
	}
	defer st.mu.Unlock()
	if st.poisoned {
		return CommandEnvelope{}, false, fmt.Errorf("command log for %q is poisoned: %w", personalityAgentID, st.poisonErr)
	}
	if err := flockContext(ctx, st.file.Fd(), syscall.LOCK_EX); err != nil {
		return CommandEnvelope{}, false, fmt.Errorf("lock command log for %q: %w", personalityAgentID, err)
	}
	defer func() { _ = unlockFile(st.file) }()
	if err := s.refreshStateLocked(ctx, st, personalityAgentID); err != nil {
		return CommandEnvelope{}, false, err
	}
	idx, ok := st.bySeq[seq]
	if !ok {
		return CommandEnvelope{}, false, nil
	}
	return st.commands[idx], true, nil
}

// CatchUp returns commands for personalityAgentID with seq >= fromSeq in order.
func (s *CommandStore) CatchUp(ctx context.Context, personalityAgentID string, fromSeq uint64) ([]CommandEnvelope, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	st, err := s.lockPersonalityAgent(ctx, personalityAgentID)
	if err != nil {
		return nil, err
	}
	defer st.mu.Unlock()
	if st.poisoned {
		return nil, fmt.Errorf("command log for %q is poisoned: %w", personalityAgentID, st.poisonErr)
	}
	if err := flockContext(ctx, st.file.Fd(), syscall.LOCK_EX); err != nil {
		return nil, fmt.Errorf("lock command log for %q: %w", personalityAgentID, err)
	}
	defer func() { _ = unlockFile(st.file) }()
	if err := s.refreshStateLocked(ctx, st, personalityAgentID); err != nil {
		return nil, err
	}
	i := sort.Search(len(st.commands), func(i int) bool { return st.commands[i].Seq >= fromSeq })
	out := make([]CommandEnvelope, len(st.commands)-i)
	copy(out, st.commands[i:])
	return out, nil
}

type commandLogSnapshot struct {
	commands []CommandEnvelope
	nextSeq  uint64
}

// commandSnapshot returns an immutable view of the committed command prefix.
// refreshStateLocked always swaps fresh backing arrays, so later rescans and
// appends cannot mutate a view that is being folded into an ACK cursor.
func (s *CommandStore) commandSnapshot(ctx context.Context, personalityAgentID string) (commandLogSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return commandLogSnapshot{}, err
	}
	st, err := s.lockPersonalityAgent(ctx, personalityAgentID)
	if err != nil {
		return commandLogSnapshot{}, err
	}
	defer st.mu.Unlock()
	if st.poisoned {
		return commandLogSnapshot{}, fmt.Errorf("command log for %q is poisoned: %w", personalityAgentID, st.poisonErr)
	}
	if err := flockContext(ctx, st.file.Fd(), syscall.LOCK_EX); err != nil {
		return commandLogSnapshot{}, fmt.Errorf("lock command log for %q: %w", personalityAgentID, err)
	}
	defer func() { _ = unlockFile(st.file) }()
	if err := s.refreshStateLocked(ctx, st, personalityAgentID); err != nil {
		return commandLogSnapshot{}, err
	}
	commands := st.commands[:len(st.commands):len(st.commands)]
	return commandLogSnapshot{commands: commands, nextSeq: st.nextSeq}, nil
}

// lockPersonalityAgent returns with the per-personality-agent mutex held. The store
// mutex protects only lifecycle and map membership and is released before a
// flock wait or disk scan, allowing unrelated personality agents to progress.
func (s *CommandStore) lockPersonalityAgent(ctx context.Context, personalityAgentID string) (*personalityAgentState, error) {
	if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
		return nil, err
	}
	if err := lockMutexContext(ctx, &s.mu); err != nil {
		return nil, err
	}
	if s.closed {
		s.mu.Unlock()
		return nil, errors.New("command store is closed")
	}
	st := s.states[personalityAgentID]
	if st == nil {
		st = newPersonalityAgentState(commandLogPath(s.dir, personalityAgentID))
		s.states[personalityAgentID] = st
	}
	s.mu.Unlock()

	if err := lockMutexContext(ctx, &st.mu); err != nil {
		return nil, err
	}
	if st.closed {
		st.mu.Unlock()
		return nil, errors.New("command store is closed")
	}
	if st.file == nil && !st.poisoned {
		if err := s.loadStateLocked(ctx, st, personalityAgentID); err != nil {
			st.mu.Unlock()
			return nil, err
		}
	}
	return st, nil
}

// isIncompleteJSONError reports whether err indicates the JSON parser ran off
// the end of the input, as opposed to a syntactically complete but invalid
// value. This lets loadFileLocked truncate only provably partial tails.
func isIncompleteJSONError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "unexpected end of JSON input") ||
		strings.Contains(msg, "unexpected EOF")
}

// scanLogLocked reads the log from the current offset and populates st. It
// truncates an incomplete final tail and repairs a missing trailing newline.
// The caller must exclusively own st and hold an exclusive flock on st.file.
func (s *CommandStore) scanLogLocked(ctx context.Context, st *personalityAgentState, personalityAgentID string) error {
	if _, err := st.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek command log for %q: %w", personalityAgentID, err)
	}

	r := bufio.NewReader(st.file)
	offset := int64(0)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		lineStart := offset
		line, readErr := r.ReadBytes('\n')
		if len(line) > 0 {
			offset += int64(len(line))
		}

		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			if readErr == io.EOF {
				break
			}
			continue
		}

		var rec LogRecord
		if err := json.Unmarshal(trimmed, &rec); err != nil {
			// A final record without a terminating newline that provably ends
			// mid-value is an incomplete tail; truncate it and recover. Any
			// other parse error on the final line, or any error on an earlier
			// line, is a malformed complete record and must fail closed.
			if readErr == io.EOF && isIncompleteJSONError(err) {
				if truncErr := st.file.Truncate(lineStart); truncErr != nil {
					return fmt.Errorf("truncate partial tail for %q: %w", personalityAgentID, truncErr)
				}
				if syncErr := st.file.Sync(); syncErr != nil {
					return fmt.Errorf("sync after truncating partial tail for %q: %w", personalityAgentID, syncErr)
				}
				offset = lineStart
				break
			}
			if readErr == io.EOF {
				return fmt.Errorf("decode command log for %q: final record is malformed but complete: %w", personalityAgentID, err)
			}
			return fmt.Errorf("decode command log for %q: %w", personalityAgentID, err)
		}

		if rec.Seq == 0 && rec.CommandID == "" && len(rec.Command) == 0 {
			return fmt.Errorf("empty command record in log for %q", personalityAgentID)
		}
		if rec.Seq > maxJSONSafeInteger {
			return fmt.Errorf("command log for %q contains seq %d exceeding JSON-safe integer range", personalityAgentID, rec.Seq)
		}
		if existing, ok := st.bySeq[rec.Seq]; ok {
			return fmt.Errorf("duplicate seq %d in log for %q (existing %d)", rec.Seq, personalityAgentID, existing)
		}
		expectedSeq := uint64(len(st.commands) + 1)
		if rec.Seq != expectedSeq {
			return fmt.Errorf("command log for %q has non-contiguous seq: got %d, want %d", personalityAgentID, rec.Seq, expectedSeq)
		}

		idx := len(st.commands)
		st.commands = append(st.commands, rec.CommandEnvelope)
		st.nextSeq = rec.Seq + 1
		st.bySeq[rec.Seq] = idx
		if _, ok := st.byCommandID[rec.CommandID]; ok {
			return fmt.Errorf("duplicate command_id %q in log for %q", rec.CommandID, personalityAgentID)
		}
		st.byCommandID[rec.CommandID] = idx
		if rec.IdempotencyKey != "" {
			if _, ok := st.byKey[rec.IdempotencyKey]; ok {
				return fmt.Errorf("duplicate idempotency key %q in log for %q", rec.IdempotencyKey, personalityAgentID)
			}
			st.byKey[rec.IdempotencyKey] = idx
		}

		if readErr == io.EOF {
			// A valid final record may be missing its trailing JSONL delimiter
			// (crash between the write and the appended newline, or a file
			// seeded by a test). Repair it now, durably, so the next append
			// cannot concatenate records into an unparseable `}{` boundary.
			if len(line) > 0 && line[len(line)-1] != '\n' {
				if _, werr := st.file.Write([]byte{'\n'}); werr != nil {
					return fmt.Errorf("repair missing trailing newline for %q: %w", personalityAgentID, werr)
				}
				if syncErr := st.file.Sync(); syncErr != nil {
					return fmt.Errorf("sync repaired trailing newline for %q: %w", personalityAgentID, syncErr)
				}
				offset += 1
			}
			break
		}
		if readErr != nil {
			return fmt.Errorf("read command log for %q: %w", personalityAgentID, readErr)
		}
	}

	size, err := st.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("seek end of command log for %q: %w", personalityAgentID, err)
	}
	st.fileSize = size
	return nil
}

// refreshStateLocked resets the in-memory state for st and rescans the log from
// disk under the existing exclusive flock. A conservative rescan is used rather
// than a file-size short-circuit so that a same-size truncate/rewrite by
// another process cannot leave stale nextSeq or idempotency maps.
func (s *CommandStore) refreshStateLocked(ctx context.Context, st *personalityAgentState, personalityAgentID string) error {
	if _, err := st.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek command log for %q: %w", personalityAgentID, err)
	}

	fresh := newPersonalityAgentState(st.path)
	fresh.file = st.file
	if err := s.scanLogLocked(ctx, fresh, personalityAgentID); err != nil {
		return err
	}
	st.nextSeq = fresh.nextSeq
	st.commands = fresh.commands
	st.bySeq = fresh.bySeq
	st.byCommandID = fresh.byCommandID
	st.byKey = fresh.byKey
	st.fileSize = fresh.fileSize
	return nil
}

func newPersonalityAgentState(path string) *personalityAgentState {
	return &personalityAgentState{
		path: path, nextSeq: 1, bySeq: make(map[uint64]int),
		byCommandID: make(map[string]int), byKey: make(map[string]int),
	}
}

// loadStateLocked initializes st while its per-personality-agent mutex is held.
func (s *CommandStore) loadStateLocked(ctx context.Context, st *personalityAgentState, personalityAgentID string) error {
	info, err := os.Lstat(st.path)
	if err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("command log for %q is a symlink", personalityAgentID)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("command log for %q is not a regular file", personalityAgentID)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat command log for %q: %w", personalityAgentID, err)
	}

	file, err := os.OpenFile(st.path, os.O_CREATE|os.O_RDWR|os.O_APPEND|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("open command log for %q: %w", personalityAgentID, err)
	}
	if err := flockContext(ctx, file.Fd(), syscall.LOCK_EX); err != nil {
		_ = file.Close()
		return fmt.Errorf("lock command log for %q: %w", personalityAgentID, err)
	}
	fresh := newPersonalityAgentState(st.path)
	fresh.file = file
	if err := s.scanLogLocked(ctx, fresh, personalityAgentID); err != nil {
		_ = unlockFile(file)
		_ = file.Close()
		return err
	}

	if err := unlockFile(file); err != nil {
		_ = file.Close()
		return fmt.Errorf("unlock command log for %q: %w", personalityAgentID, err)
	}
	st.file = file
	st.nextSeq = fresh.nextSeq
	st.commands = fresh.commands
	st.bySeq = fresh.bySeq
	st.byCommandID = fresh.byCommandID
	st.byKey = fresh.byKey
	st.fileSize = fresh.fileSize
	return nil
}

func commandLogPath(dir, personalityAgentID string) string {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(personalityAgentID))
	return filepath.Join(dir, "commands-"+encoded+".jsonl")
}

func personalityAgentIDFromPath(path string) (string, error) {
	base := filepath.Base(path)
	const prefix = "commands-"
	const suffix = ".jsonl"
	if !filepath.IsLocal(base) || len(base) <= len(prefix)+len(suffix) || !strings.HasPrefix(base, prefix) || !strings.HasSuffix(base, suffix) {
		return "", fmt.Errorf("unexpected command log filename")
	}
	encoded := base[len(prefix) : len(base)-len(suffix)]
	b, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("decode personality agent id: %w", err)
	}
	personalityAgentID := string(b)
	if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
		return "", err
	}
	return personalityAgentID, nil
}

func newCommandID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	// Version 4.
	b[6] = (b[6] & 0x0f) | 0x40
	// Variant 10.
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
