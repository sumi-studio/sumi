package agentevents

// DurableGateway is the production adapter for the T28 API boundary. T26
// publishes one atomically-written state file per agent (generation + stable
// hydration receipt identity); commands, ACKs, and agent events are persisted
// here rather than being represented by cmd/server placeholders.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

type DurableGateway struct {
	dir      string
	commands *CommandStore
	mu       sync.Mutex

	// PollInterval bounds the polling interval used by WaitFor and Live.
	// A zero value uses the safe default (50ms).
	PollInterval time.Duration
	// MaxConversationTails and MaxAckTail bound process memory without changing
	// durable replay. Zero values use conservative defaults.
	MaxConversationTails int
	MaxAckTail           int

	tails   map[string]*conversationLogState
	clock   uint64
	newFile func(string, int, os.FileMode) (durableFileHandle, error)
}

type conversationLogState struct {
	eventSeq  uint64
	eventSize int64
	acks      map[uint64]CommandAck
	ackOrder  []ackCacheEntry
	ackSize   int64
	lastUsed  uint64
}

type ackCacheEntry struct {
	seq uint64
	ack CommandAck
}

type runtimeState struct {
	Generation               uint64  `json:"generation"`
	HydrationReceiptIdentity *string `json:"hydration_receipt_identity"`
	present                  bool
}

type durableEventRecord struct {
	Seq   uint64   `json:"seq"`
	Event Envelope `json:"event"`
}

// UnmarshalJSON makes durable event-log recovery fail-closed on duplicate keys,
// unknown fields, trailing data, and sequence values outside the JSON-safe range.
func (r *durableEventRecord) UnmarshalJSON(data []byte) error {
	if err := checkDuplicateKeys(data); err != nil {
		return fmt.Errorf("durable event record json: %w", err)
	}
	type raw struct {
		Seq   *uint64   `json:"seq"`
		Event *Envelope `json:"event"`
	}
	var v raw
	if err := unmarshalStrict(data, &v); err != nil {
		return err
	}
	if v.Seq == nil || v.Event == nil {
		return errors.New("durable event record requires seq and event")
	}
	if *v.Seq > maxJSONSafeInteger {
		return fmt.Errorf("durable event record seq %d exceeds JSON-safe integer range", *v.Seq)
	}
	*r = durableEventRecord{Seq: *v.Seq, Event: *v.Event}
	return nil
}

// durableFileHandle abstracts the per-conversation log file so tests can
// inject deterministic write/sync/truncate failures without changing
// production call sites.
type durableFileHandle interface {
	io.Seeker
	io.Reader
	io.Writer
	Sync() error
	Truncate(size int64) error
	Close() error
	Fd() uintptr
}

func OpenDurableGateway(dir string, commands *CommandStore) (*DurableGateway, error) {
	if dir == "" {
		return nil, errors.New("gateway runtime state directory is required")
	}
	if commands == nil {
		return nil, errors.New("command store is required")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve gateway runtime state directory: %w", err)
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return nil, fmt.Errorf("create gateway runtime state directory: %w", err)
	}
	info, err := os.Lstat(abs)
	if err != nil {
		return nil, fmt.Errorf("inspect gateway runtime state directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("gateway runtime state path must not be a symlink")
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("gateway runtime state path %q is not a directory", abs)
	}
	return &DurableGateway{
		dir:                  abs,
		commands:             commands,
		PollInterval:         50 * time.Millisecond,
		MaxConversationTails: 128,
		MaxAckTail:           256,
		tails:                make(map[string]*conversationLogState),
		newFile: func(name string, flag int, perm os.FileMode) (durableFileHandle, error) {
			return os.OpenFile(name, flag, perm)
		},
	}, nil
}

func (g *DurableGateway) pollInterval() time.Duration {
	if g.PollInterval > 0 {
		return g.PollInterval
	}
	return 50 * time.Millisecond
}

func (g *DurableGateway) maxConversationTails() int {
	if g.MaxConversationTails > 0 {
		return g.MaxConversationTails
	}
	return 128
}

func (g *DurableGateway) maxAckTail() int {
	if g.MaxAckTail > 0 {
		return g.MaxAckTail
	}
	return 256
}

func (g *DurableGateway) VerifyGeneration(ctx context.Context, agentID string, generation uint64) error {
	state, err := g.state(ctx, agentID)
	if err != nil {
		return err
	}
	if !state.present {
		return errors.New("durable runtime state is absent")
	}
	if state.Generation != generation {
		return fmt.Errorf("stale generation: got %d, current %d", generation, state.Generation)
	}
	return nil
}

func (g *DurableGateway) WaitFor(ctx context.Context, claims TokenClaims, generation uint64) error {
	ticker := time.NewTicker(g.pollInterval())
	defer ticker.Stop()
	for {
		state, err := g.state(ctx, claims.AgentID)
		if err != nil {
			return err
		}
		if !state.present {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				continue
			}
		}
		if state.Generation != generation {
			return fmt.Errorf("hydration generation changed: got %d, current %d", generation, state.Generation)
		}
		if state.HydrationReceiptIdentity != nil {
			if *state.HydrationReceiptIdentity == "" {
				return errors.New("hydration receipt identity must not be empty")
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (g *DurableGateway) FirstCommandSeq(ctx context.Context, claims TokenClaims) (uint64, error) {
	return g.commands.FirstCommandSeq(ctx, claims.ConversationID)
}

func (g *DurableGateway) HasCommands(ctx context.Context, claims TokenClaims) (bool, error) {
	return g.commands.HasCommands(ctx, claims.ConversationID)
}

func (g *DurableGateway) CatchUp(ctx context.Context, claims TokenClaims, fromSeq uint64) ([]CommandEnvelope, error) {
	return g.commands.CatchUp(ctx, claims.ConversationID, fromSeq)
}

// Live polls the durable command log starting at fromSeq and streams each
// command in order. The first poll reads from fromSeq, so a command appended
// concurrently with this call is not lost. next advances only after a command
// is successfully sent, and the next poll continues from that point.
func (g *DurableGateway) Live(ctx context.Context, claims TokenClaims, fromSeq uint64) (<-chan CommandEnvelope, <-chan error, error) {
	next := fromSeq
	out := make(chan CommandEnvelope, 16)
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		defer close(out)
		ticker := time.NewTicker(g.pollInterval())
		defer ticker.Stop()
		for {
			commands, err := g.commands.CatchUp(ctx, claims.ConversationID, next)
			if err != nil {
				select {
				case errCh <- fmt.Errorf("command catch-up: %w", err):
				case <-ctx.Done():
				}
				return
			}
			for _, command := range commands {
				select {
				case out <- command:
					next = command.Seq + 1
				case <-ctx.Done():
					return
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return out, errCh, nil
}

func (g *DurableGateway) ApplyAck(ctx context.Context, claims TokenClaims, ack CommandAck) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateCommandAck(ack); err != nil {
		return err
	}
	cmd, found, err := g.commands.GetCommand(ctx, claims.ConversationID, ack.Seq)
	if err != nil {
		return fmt.Errorf("load acknowledged command: %w", err)
	}
	if !found || cmd.CommandID != ack.CommandID {
		return fmt.Errorf(
			"ack does not match durable command log: seq=%d command_id=%q",
			ack.Seq,
			ack.CommandID,
		)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.appendCommandAckLocked(claims.ConversationID, ack)
}

func (g *DurableGateway) Receive(ctx context.Context, claims TokenClaims, envelope Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateEnvelope(envelope); err != nil {
		return err
	}
	if envelope.Seq == nil { // volatile frames are deliberately not part of replay.
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.appendDurableEventLocked(
		claims.ConversationID,
		durableEventRecord{Seq: *envelope.Seq, Event: envelope},
	)
}

func (g *DurableGateway) LastReceivedEventSeq(ctx context.Context, claims TokenClaims) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	st := g.stateFor(claims.ConversationID)
	path := g.eventPath(claims.ConversationID)
	file, err := g.newFile(path, os.O_RDWR, 0o600)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer file.Close()
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		return 0, fmt.Errorf("lock durable event log for read: %w", err)
	}
	defer func() { _ = unlockDurableFile(file) }()
	if err := g.refreshEventTailLocked(file, st); err != nil {
		return 0, err
	}
	return st.eventSeq, nil
}

func (g *DurableGateway) stateFor(conversationID string) *conversationLogState {
	g.clock++
	st, ok := g.tails[conversationID]
	if ok {
		st.lastUsed = g.clock
		return st
	}
	st = &conversationLogState{acks: make(map[uint64]CommandAck), lastUsed: g.clock}
	g.tails[conversationID] = st
	g.evictInactiveTailsLocked(conversationID)
	return st
}

func (g *DurableGateway) evictInactiveTailsLocked(activeConversationID string) {
	for len(g.tails) > g.maxConversationTails() {
		var evictID string
		var oldest uint64
		for conversationID, state := range g.tails {
			if conversationID == activeConversationID {
				continue
			}
			if evictID == "" || state.lastUsed < oldest || (state.lastUsed == oldest && conversationID < evictID) {
				evictID, oldest = conversationID, state.lastUsed
			}
		}
		if evictID == "" {
			return
		}
		delete(g.tails, evictID)
	}
}

func (g *DurableGateway) rememberAckLocked(st *conversationLogState, ack CommandAck) {
	st.acks[ack.Seq] = ack
	st.ackOrder = append(st.ackOrder, ackCacheEntry{seq: ack.Seq, ack: ack})
	for len(st.acks) > g.maxAckTail() && len(st.ackOrder) > 0 {
		oldest := st.ackOrder[0]
		st.ackOrder = st.ackOrder[1:]
		if current, ok := st.acks[oldest.seq]; ok && commandAckEqual(current, oldest.ack) {
			delete(st.acks, oldest.seq)
		}
	}
}

func commandAckEqual(left, right CommandAck) bool {
	return left.Seq == right.Seq && left.CommandID == right.CommandID && left.Status == right.Status && stringPointerEqual(left.RejectReason, right.RejectReason)
}

func (g *DurableGateway) state(ctx context.Context, agentID string) (runtimeState, error) {
	if err := ctx.Err(); err != nil {
		return runtimeState{}, err
	}
	path := g.statePath(agentID)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return runtimeState{}, nil
	}
	if err != nil {
		return runtimeState{}, fmt.Errorf("inspect durable runtime state: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return runtimeState{}, errors.New("invalid durable runtime state")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return runtimeState{}, err
	}
	var state runtimeState
	if err := unmarshalStrict(raw, &state); err != nil {
		return runtimeState{}, fmt.Errorf("decode durable runtime state: %w", err)
	}
	if state.HydrationReceiptIdentity != nil && *state.HydrationReceiptIdentity == "" {
		return runtimeState{}, errors.New("hydration receipt identity must not be empty")
	}
	if state.Generation > maxJSONSafeInteger {
		return runtimeState{}, fmt.Errorf("runtime generation %d exceeds JSON-safe integer range", state.Generation)
	}
	state.present = true
	return state, nil
}

func (g *DurableGateway) appendDurableEventLocked(conversationID string, record durableEventRecord) error {
	st := g.stateFor(conversationID)
	path := g.eventPath(conversationID)
	file, err := g.newFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock durable event log for append: %w", err)
	}
	defer func() { _ = unlockDurableFile(file) }()

	if err := g.refreshEventTailLocked(file, st); err != nil {
		return err
	}
	if record.Seq != st.eventSeq+1 {
		return fmt.Errorf("event seq is not contiguous: got %d, want %d", record.Seq, st.eventSeq+1)
	}
	line, err := json.Marshal(record)
	if err != nil {
		return err
	}
	data := append(line, '\n')

	preWriteOffset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	written, writeErr := file.Write(data)
	if writeErr != nil || written != len(data) {
		var opErr error
		if writeErr != nil {
			opErr = fmt.Errorf("write durable event log: %w", writeErr)
		} else {
			opErr = fmt.Errorf("short write to durable event log: wrote %d of %d bytes", written, len(data))
		}
		if rbErr := rollbackDurableFile(file, preWriteOffset, opErr); rbErr != nil {
			return rbErr
		}
		return opErr
	}
	if syncErr := file.Sync(); syncErr != nil {
		opErr := fmt.Errorf("sync durable event log: %w", syncErr)
		if rbErr := rollbackDurableFile(file, preWriteOffset, opErr); rbErr != nil {
			return rbErr
		}
		return opErr
	}

	st.eventSeq = record.Seq
	st.eventSize = preWriteOffset + int64(len(data))
	return nil
}

func (g *DurableGateway) appendCommandAckLocked(conversationID string, ack CommandAck) error {
	st := g.stateFor(conversationID)
	path := g.ackPath(conversationID)
	file, err := g.newFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock durable ack log for append: %w", err)
	}
	defer func() { _ = unlockDurableFile(file) }()

	if err := g.refreshAckTailLocked(file, st); err != nil {
		return err
	}

	previous, ok := st.acks[ack.Seq]
	if !ok {
		previous, ok, err = findAckLocked(file, ack.Seq)
		if err != nil {
			return err
		}
	}
	if ok {
		if previous.Seq != ack.Seq || previous.CommandID != ack.CommandID {
			return fmt.Errorf("durable ack log contains mismatched seq/command_id correlation")
		}
		if previous.Status == ack.Status && stringPointerEqual(previous.RejectReason, ack.RejectReason) {
			return nil
		}
		if previous.Status != "received" {
			return fmt.Errorf(
				"command ack is already terminal: seq=%d command_id=%q status=%q",
				ack.Seq,
				ack.CommandID,
				previous.Status,
			)
		}
		if ack.Status == "received" {
			return fmt.Errorf("conflicting duplicate received ack")
		}
	}

	line, err := json.Marshal(ack)
	if err != nil {
		return err
	}
	data := append(line, '\n')

	preWriteOffset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	written, writeErr := file.Write(data)
	if writeErr != nil || written != len(data) {
		var opErr error
		if writeErr != nil {
			opErr = fmt.Errorf("write durable ack log: %w", writeErr)
		} else {
			opErr = fmt.Errorf("short write to durable ack log: wrote %d of %d bytes", written, len(data))
		}
		if rbErr := rollbackDurableFile(file, preWriteOffset, opErr); rbErr != nil {
			return rbErr
		}
		return opErr
	}
	if syncErr := file.Sync(); syncErr != nil {
		opErr := fmt.Errorf("sync durable ack log: %w", syncErr)
		if rbErr := rollbackDurableFile(file, preWriteOffset, opErr); rbErr != nil {
			return rbErr
		}
		return opErr
	}

	g.rememberAckLocked(st, ack)
	st.ackSize = preWriteOffset + int64(len(data))
	return nil
}

func (g *DurableGateway) refreshEventTailLocked(file durableFileHandle, st *conversationLogState) error {
	size, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("seek durable event log: %w", err)
	}
	if size == st.eventSize {
		return nil
	}
	if size < st.eventSize {
		st.eventSeq = 0
		st.eventSize = 0
	}
	start := st.eventSize
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return fmt.Errorf("seek durable event log for tail refresh: %w", err)
	}

	r := bufio.NewReader(file)
	offset := start
	last := st.eventSeq
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
			if readErr != nil {
				return fmt.Errorf("read durable event log: %w", readErr)
			}
			continue
		}

		var existing durableEventRecord
		if err := json.Unmarshal(trimmed, &existing); err != nil {
			if readErr == io.EOF && isIncompleteJSONError(err) {
				if truncErr := file.Truncate(lineStart); truncErr != nil {
					return fmt.Errorf("truncate partial durable event tail: %w", truncErr)
				}
				if syncErr := file.Sync(); syncErr != nil {
					return fmt.Errorf("sync after truncating partial durable event tail: %w", syncErr)
				}
				offset = lineStart
				break
			}
			if readErr == io.EOF {
				return fmt.Errorf("decode durable event log: final record is malformed but complete: %w", err)
			}
			return fmt.Errorf("decode durable event log: %w", err)
		}
		if existing.Seq != last+1 {
			return fmt.Errorf("durable event log is non-contiguous: got %d after %d", existing.Seq, last)
		}
		last = existing.Seq

		if readErr == io.EOF {
			if len(line) > 0 && line[len(line)-1] != '\n' {
				if _, werr := file.Write([]byte{'\n'}); werr != nil {
					return fmt.Errorf("repair missing trailing newline in durable event log: %w", werr)
				}
				if syncErr := file.Sync(); syncErr != nil {
					return fmt.Errorf("sync repaired durable event log trailing newline: %w", syncErr)
				}
				offset += 1
			}
			break
		}
		if readErr != nil {
			return fmt.Errorf("read durable event log: %w", readErr)
		}
	}

	st.eventSeq = last
	st.eventSize = offset
	return nil
}

func (g *DurableGateway) refreshAckTailLocked(file durableFileHandle, st *conversationLogState) error {
	size, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("seek durable ack log: %w", err)
	}
	if size == st.ackSize {
		return nil
	}
	if size < st.ackSize {
		st.acks = make(map[uint64]CommandAck)
		st.ackOrder = nil
		st.ackSize = 0
	}
	start := st.ackSize
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return fmt.Errorf("seek durable ack log for tail refresh: %w", err)
	}

	r := bufio.NewReader(file)
	offset := start
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
			if readErr != nil {
				return fmt.Errorf("read durable ack log: %w", readErr)
			}
			continue
		}

		var existing CommandAck
		if err := json.Unmarshal(trimmed, &existing); err != nil {
			if readErr == io.EOF && isIncompleteJSONError(err) {
				if truncErr := file.Truncate(lineStart); truncErr != nil {
					return fmt.Errorf("truncate partial durable ack tail: %w", truncErr)
				}
				if syncErr := file.Sync(); syncErr != nil {
					return fmt.Errorf("sync after truncating partial durable ack tail: %w", syncErr)
				}
				offset = lineStart
				break
			}
			if readErr == io.EOF {
				return fmt.Errorf("decode durable ack log: final record is malformed but complete: %w", err)
			}
			return fmt.Errorf("decode durable ack log: %w", err)
		}
		g.rememberAckLocked(st, existing)

		if readErr == io.EOF {
			if len(line) > 0 && line[len(line)-1] != '\n' {
				if _, werr := file.Write([]byte{'\n'}); werr != nil {
					return fmt.Errorf("repair missing trailing newline in durable ack log: %w", werr)
				}
				if syncErr := file.Sync(); syncErr != nil {
					return fmt.Errorf("sync repaired durable ack log trailing newline: %w", syncErr)
				}
				offset += 1
			}
			break
		}
		if readErr != nil {
			return fmt.Errorf("read durable ack log: %w", readErr)
		}
	}

	st.ackSize = offset
	return nil
}

// findAckLocked reloads an evicted ACK entry from its durable log. The cache
// may only retain a bounded tail, but terminal ACK transitions remain valid
// regardless of conversation age or process lifetime.
func findAckLocked(file durableFileHandle, seq uint64) (CommandAck, bool, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return CommandAck{}, false, fmt.Errorf("seek durable ack log for lookup: %w", err)
	}
	r := bufio.NewReader(file)
	var found CommandAck
	for {
		line, err := r.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			var candidate CommandAck
			if decodeErr := json.Unmarshal(bytes.TrimSpace(line), &candidate); decodeErr != nil {
				return CommandAck{}, false, fmt.Errorf("decode durable ack log for lookup: %w", decodeErr)
			}
			if candidate.Seq == seq {
				found = candidate
			}
		}
		if errors.Is(err, io.EOF) {
			return found, found.CommandID != "", nil
		}
		if err != nil {
			return CommandAck{}, false, fmt.Errorf("read durable ack log for lookup: %w", err)
		}
	}
}

func (g *DurableGateway) publishRuntimeState(agentID string, state runtimeState) error {
	if state.Generation > maxJSONSafeInteger {
		return fmt.Errorf("runtime generation %d exceeds JSON-safe integer range", state.Generation)
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return writeFileAtomic(g.statePath(agentID), raw, 0o600)
}

func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary file for atomic write: %w", err)
	}
	tmpPath := tmp.Name()
	removeTmp := true
	defer func() {
		if removeTmp {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(perm); err != nil {
		return fmt.Errorf("set temporary file permissions: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("write temporary file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync temporary file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary file: %w", err)
	}
	removeTmp = false
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("publish file atomically: %w", err)
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open runtime-state parent directory after rename: %w", err)
	}
	defer dirFile.Close()
	if err := dirFile.Sync(); err != nil {
		return fmt.Errorf("sync runtime-state parent directory after rename: %w", err)
	}
	return nil
}

func unlockDurableFile(f durableFileHandle) error {
	if f == nil {
		return nil
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return nil
}

func rollbackDurableFile(f durableFileHandle, offset int64, origErr error) error {
	var truncErr, syncErr error
	if f != nil {
		truncErr = f.Truncate(offset)
		syncErr = f.Sync()
	}
	if truncErr != nil || syncErr != nil {
		return fmt.Errorf("append failure %v; rollback could not be confirmed (truncate=%v, sync=%v)", origErr, truncErr, syncErr)
	}
	return nil
}

func stringPointerEqual(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func (g *DurableGateway) statePath(agentID string) string {
	return filepath.Join(g.dir, "runtime-"+safeFileID(agentID)+".json")
}
func (g *DurableGateway) eventPath(conversationID string) string {
	return filepath.Join(g.dir, "events-"+safeFileID(conversationID)+".jsonl")
}
func (g *DurableGateway) ackPath(conversationID string) string {
	return filepath.Join(g.dir, "acks-"+safeFileID(conversationID)+".jsonl")
}
func safeFileID(value string) string { return base64.RawURLEncoding.EncodeToString([]byte(value)) }
