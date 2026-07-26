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
)

// CommandStore is a durable, append-only per-conversation command log. It
// implements CommandAppender and exposes read-only introspection helpers for
// verification; it does not wire the WebSocket CommandSource seam (that is
// intentionally isolated for T17/T26).
//
// Each conversation has its own JSON Lines log. Seq allocation is protected
// by a process-level mutex and command bytes are fsynced to the log before
// success is returned. After a process restart, OpenCommandStore re-reads the
// logs and reconstructs the per-conversation next seq and idempotency maps,
// so server restart preserves the log and allocation continuity.
type CommandStore struct {
	mu     sync.Mutex
	dir    string
	states map[string]*conversationState
	closed bool
}

// fileHandle abstracts the per-conversation log file so tests can inject
// deterministic failures without changing production call sites.
type fileHandle interface {
	io.Seeker
	io.Reader
	io.Writer
	Sync() error
	Truncate(size int64) error
	Close() error
}

type conversationState struct {
	path        string
	file        fileHandle
	nextSeq     uint64
	commands    []CommandEnvelope
	bySeq       map[uint64]int
	byCommandID map[string]int
	byKey       map[string]int // idempotency key -> commands index
	poisoned    bool
	poisonErr   error
}

// LogRecord is the on-disk representation of a command. It embeds the public
// CommandEnvelope so the wire-compatible fields are written unchanged, and adds
// storage-only metadata (idempotency_key) that is kept out of the public API.
type LogRecord struct {
	CommandEnvelope
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

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
		dir:    abs,
		states: make(map[string]*conversationState),
	}

	matches, err := filepath.Glob(filepath.Join(abs, "commands-*.jsonl"))
	if err != nil {
		return nil, fmt.Errorf("scan command log dir: %w", err)
	}
	sort.Strings(matches)
	for _, path := range matches {
		conversationID, err := conversationIDFromPath(path)
		if err != nil {
			return nil, fmt.Errorf("invalid command log file %q: %w", path, err)
		}
		if _, err := s.loadFileLocked(conversationID, path); err != nil {
			return nil, fmt.Errorf("load command log %q: %w", path, err)
		}
	}
	return s, nil
}

// Close flushes and closes all open log files.
func (s *CommandStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var firstErr error
	for _, st := range s.states {
		if st.file != nil {
			if err := st.file.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	s.states = nil
	return firstErr
}

// poisonLocked marks a conversation state as unusable and closes its file so
// no later append can reuse a seq or continue from uncertain bytes.
func (s *CommandStore) poisonLocked(st *conversationState, reason error) {
	if st.file != nil {
		_ = st.file.Close()
		st.file = nil
	}
	st.poisoned = true
	st.poisonErr = reason
}

// rollbackLocked attempts to truncate the log back to offset and fsync. If the
// rollback cannot be durably confirmed, it poisons the conversation state.
func (s *CommandStore) rollbackLocked(st *conversationState, offset int64, origErr error) error {
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

// Append durably appends a validated command to the conversation log.
// If idempotencyKey is non-empty and a command was previously accepted with
// the same key and identical command bytes, the existing CommandEnvelope is
// returned without allocating a second seq. A different command body for the
// same key is a conflict and returns an error.
func (s *CommandStore) Append(ctx context.Context, conversationID string, idempotencyKey string, command json.RawMessage) (CommandEnvelope, error) {
	if err := ctx.Err(); err != nil {
		return CommandEnvelope{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return CommandEnvelope{}, errors.New("command store is closed")
	}

	st, err := s.ensureStateLocked(conversationID)
	if err != nil {
		return CommandEnvelope{}, err
	}
	if st.poisoned {
		return CommandEnvelope{}, fmt.Errorf("command log for %q is poisoned: %w", conversationID, st.poisonErr)
	}

	if idempotencyKey != "" {
		if idx, ok := st.byKey[idempotencyKey]; ok {
			existing := st.commands[idx]
			if string(existing.Command) == string(command) {
				return existing, nil
			}
			return CommandEnvelope{}, fmt.Errorf("idempotency key %q reused with different command: %w", idempotencyKey, errIdempotencyConflict)
		}
	}

	commandID, err := newCommandID()
	if err != nil {
		return CommandEnvelope{}, fmt.Errorf("generate command_id: %w", err)
	}

	seq := st.nextSeq
	env := CommandEnvelope{
		Seq:       seq,
		CommandID: commandID,
		Command:   command,
	}

	offset, err := st.file.Seek(0, io.SeekEnd)
	if err != nil {
		return CommandEnvelope{}, fmt.Errorf("seek command log: %w", err)
	}

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

	return env, nil
}

// NextCommandSeq returns the next seq that would be allocated for conversationID.
func (s *CommandStore) NextCommandSeq(ctx context.Context, conversationID string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, errors.New("command store is closed")
	}
	st, err := s.ensureStateLocked(conversationID)
	if err != nil {
		return 0, err
	}
	if st.poisoned {
		return 0, fmt.Errorf("command log for %q is poisoned: %w", conversationID, st.poisonErr)
	}
	return st.nextSeq, nil
}

// FirstCommandSeq returns the first durable seq for conversationID. If the log
// is empty, it returns 1 so catch-up can use it as the lower bound.
func (s *CommandStore) FirstCommandSeq(ctx context.Context, conversationID string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, errors.New("command store is closed")
	}
	st, err := s.ensureStateLocked(conversationID)
	if err != nil {
		return 0, err
	}
	if st.poisoned {
		return 0, fmt.Errorf("command log for %q is poisoned: %w", conversationID, st.poisonErr)
	}
	if len(st.commands) == 0 {
		return 1, nil
	}
	return st.commands[0].Seq, nil
}

// CatchUp returns commands for conversationID with seq >= fromSeq in order.
func (s *CommandStore) CatchUp(ctx context.Context, conversationID string, fromSeq uint64) ([]CommandEnvelope, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("command store is closed")
	}
	st, err := s.ensureStateLocked(conversationID)
	if err != nil {
		return nil, err
	}
	if st.poisoned {
		return nil, fmt.Errorf("command log for %q is poisoned: %w", conversationID, st.poisonErr)
	}
	i := sort.Search(len(st.commands), func(i int) bool { return st.commands[i].Seq >= fromSeq })
	out := make([]CommandEnvelope, len(st.commands)-i)
	copy(out, st.commands[i:])
	return out, nil
}

func (s *CommandStore) ensureStateLocked(conversationID string) (*conversationState, error) {
	if st, ok := s.states[conversationID]; ok {
		return st, nil
	}
	path := commandLogPath(s.dir, conversationID)
	return s.loadFileLocked(conversationID, path)
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

func (s *CommandStore) loadFileLocked(conversationID, path string) (*conversationState, error) {
	info, err := os.Lstat(path)
	if err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("command log for %q is a symlink", conversationID)
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("command log for %q is not a regular file", conversationID)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("stat command log for %q: %w", conversationID, err)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open command log for %q: %w", conversationID, err)
	}

	st := &conversationState{
		path:        path,
		file:        file,
		nextSeq:     1,
		bySeq:       make(map[uint64]int),
		byCommandID: make(map[string]int),
		byKey:       make(map[string]int),
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("seek command log for %q: %w", conversationID, err)
	}

	r := bufio.NewReader(file)
	offset := int64(0)
	for {
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
				if truncErr := file.Truncate(lineStart); truncErr != nil {
					_ = file.Close()
					return nil, fmt.Errorf("truncate partial tail for %q: %w", conversationID, truncErr)
				}
				if syncErr := file.Sync(); syncErr != nil {
					_ = file.Close()
					return nil, fmt.Errorf("sync after truncating partial tail for %q: %w", conversationID, syncErr)
				}
				break
			}
			_ = file.Close()
			if readErr == io.EOF {
				return nil, fmt.Errorf("decode command log for %q: final record is malformed but complete: %w", conversationID, err)
			}
			return nil, fmt.Errorf("decode command log for %q: %w", conversationID, err)
		}

		if rec.Seq == 0 && rec.CommandID == "" && len(rec.Command) == 0 {
			_ = file.Close()
			return nil, fmt.Errorf("empty command record in log for %q", conversationID)
		}

		idx := len(st.commands)
		st.commands = append(st.commands, rec.CommandEnvelope)
		if rec.Seq >= st.nextSeq {
			st.nextSeq = rec.Seq + 1
		}
		if existing, ok := st.bySeq[rec.Seq]; ok {
			_ = file.Close()
			return nil, fmt.Errorf("duplicate seq %d in log for %q (existing %d)", rec.Seq, conversationID, existing)
		}
		st.bySeq[rec.Seq] = idx
		if _, ok := st.byCommandID[rec.CommandID]; ok {
			_ = file.Close()
			return nil, fmt.Errorf("duplicate command_id %q in log for %q", rec.CommandID, conversationID)
		}
		st.byCommandID[rec.CommandID] = idx
		if rec.IdempotencyKey != "" {
			if _, ok := st.byKey[rec.IdempotencyKey]; ok {
				_ = file.Close()
				return nil, fmt.Errorf("duplicate idempotency key %q in log for %q", rec.IdempotencyKey, conversationID)
			}
			st.byKey[rec.IdempotencyKey] = idx
		}

		if readErr == io.EOF {
			// A valid final record may be missing its trailing JSONL delimiter
			// (crash between the write and the appended newline, or a file
			// seeded by a test). Repair it now, durably, so the next append
			// cannot concatenate records into an unparseable `}{` boundary.
			if len(line) > 0 && line[len(line)-1] != '\n' {
				if _, werr := file.Write([]byte{'\n'}); werr != nil {
					_ = file.Close()
					return nil, fmt.Errorf("repair missing trailing newline for %q: %w", conversationID, werr)
				}
				if syncErr := file.Sync(); syncErr != nil {
					_ = file.Close()
					return nil, fmt.Errorf("sync repaired trailing newline for %q: %w", conversationID, syncErr)
				}
			}
			break
		}
		if readErr != nil {
			_ = file.Close()
			return nil, fmt.Errorf("read command log for %q: %w", conversationID, readErr)
		}
	}

	// Move the file offset back to the end for future appends.
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("seek end of command log for %q: %w", conversationID, err)
	}

	s.states[conversationID] = st
	return st, nil
}

func commandLogPath(dir, conversationID string) string {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(conversationID))
	return filepath.Join(dir, "commands-"+encoded+".jsonl")
}

func conversationIDFromPath(path string) (string, error) {
	base := filepath.Base(path)
	const prefix = "commands-"
	const suffix = ".jsonl"
	if !filepath.IsLocal(base) || len(base) <= len(prefix)+len(suffix) || !strings.HasPrefix(base, prefix) || !strings.HasSuffix(base, suffix) {
		return "", fmt.Errorf("unexpected command log filename")
	}
	encoded := base[len(prefix) : len(base)-len(suffix)]
	b, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("decode conversation id: %w", err)
	}
	return string(b), nil
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
