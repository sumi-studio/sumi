package agentevents

import (
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

type conversationState struct {
	path        string
	file        *os.File
	nextSeq     uint64
	commands    []CommandEnvelope
	bySeq       map[uint64]int
	byCommandID map[string]int
	byKey       map[string]int // idempotency key -> commands index
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

	line, err := json.Marshal(env)
	if err != nil {
		return CommandEnvelope{}, fmt.Errorf("marshal command envelope: %w", err)
	}

	if _, err := st.file.Write(append(line, '\n')); err != nil {
		return CommandEnvelope{}, fmt.Errorf("write command log: %w", err)
	}
	if err := st.file.Sync(); err != nil {
		return CommandEnvelope{}, fmt.Errorf("sync command log: %w", err)
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
	return st.nextSeq, nil
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

func (s *CommandStore) loadFileLocked(conversationID, path string) (*conversationState, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
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

	dec := json.NewDecoder(file)
	for {
		var env CommandEnvelope
		if err := dec.Decode(&env); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			_ = file.Close()
			return nil, fmt.Errorf("decode command log for %q: %w", conversationID, err)
		}
		if env.Seq == 0 && env.CommandID == "" && len(env.Command) == 0 {
			_ = file.Close()
			return nil, fmt.Errorf("empty command record in log for %q", conversationID)
		}
		idx := len(st.commands)
		st.commands = append(st.commands, env)
		if env.Seq >= st.nextSeq {
			st.nextSeq = env.Seq + 1
		}
		if existing, ok := st.bySeq[env.Seq]; ok {
			_ = file.Close()
			return nil, fmt.Errorf("duplicate seq %d in log for %q (existing %d)", env.Seq, conversationID, existing)
		}
		st.bySeq[env.Seq] = idx
		if _, ok := st.byCommandID[env.CommandID]; ok {
			_ = file.Close()
			return nil, fmt.Errorf("duplicate command_id %q in log for %q", env.CommandID, conversationID)
		}
		st.byCommandID[env.CommandID] = idx
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
