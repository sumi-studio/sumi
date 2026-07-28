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
	"hash/crc32"
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

	tails map[string]*conversationLogState
	// browserSubscribers carry volatile frames only. Durable replay always
	// reads the event log, so disconnecting a slow browser cannot lose durable
	// history or grow this process without bound.
	browserSubscribers    map[string]map[uint64]chan Envelope
	nextBrowserSubscriber uint64
	clock                 uint64
	newFile               func(string, int, os.FileMode) (durableFileHandle, error)

	// stateMu protects run-in-flight and pending-approval state derived from
	// durable events. Readers also take mu first so they cannot observe the
	// interval after an event is durably appended but before its derived state
	// is updated.
	stateMu          sync.RWMutex
	stateRebuilt     map[string]bool
	runInFlight      map[string]bool
	pendingApprovals map[string]map[string]bool
}

type conversationLogState struct {
	eventSeq  uint64
	eventSize int64
	eventCRC  uint32
	acks      map[uint64]CommandAck
	ackOrder  []ackCacheEntry
	ackSize   int64
	ackCRC    uint32
	lastUsed  uint64
}

type ackCacheEntry struct {
	seq uint64
	ack CommandAck
}

// crc32OfFilePrefix returns the CRC-32/IEEE checksum of the first `size` bytes
// of `file`. It re-seeks to the start and streams the prefix so it works for
// large logs without loading them into memory.
func crc32OfFilePrefix(file io.ReadSeeker, size int64) (uint32, error) {
	if size <= 0 {
		return 0, nil
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	h := crc32.New(crc32.IEEETable)
	if _, err := io.CopyN(h, file, size); err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}

func updateCRC(crc uint32, data []byte) uint32 {
	return crc32.Update(crc, crc32.IEEETable, data)
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
	if err := validateEnvelope(*v.Event); err != nil {
		return fmt.Errorf("durable event record event: %w", err)
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
		browserSubscribers:   make(map[string]map[uint64]chan Envelope),
		stateRebuilt:         make(map[string]bool),
		runInFlight:          make(map[string]bool),
		pendingApprovals:     make(map[string]map[string]bool),
		newFile: func(name string, flag int, perm os.FileMode) (durableFileHandle, error) {
			return os.OpenFile(name, flag|syscall.O_NOFOLLOW, perm)
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
	if envelope.ConversationID != claims.ConversationID {
		return fmt.Errorf(
			"event conversation_id %q does not match token claim %q",
			envelope.ConversationID,
			claims.ConversationID,
		)
	}
	if envelope.Seq == nil { // volatile frames are deliberately not part of replay.
		g.mu.Lock()
		g.publishVolatileLocked(claims.ConversationID, envelope)
		g.mu.Unlock()
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.appendDurableEventLocked(
		claims.ConversationID,
		durableEventRecord{Seq: *envelope.Seq, Event: envelope},
	); err != nil {
		return err
	}
	g.updateConversationStateLocked(claims.ConversationID, envelope.Event)
	return nil
}

// EventCatchUp returns the durable event suffix after lastConsumedSeq. It
// verifies the complete retained log on every read, so a gap or corrupt record
// fails closed rather than being silently skipped during browser reconnect.
func (g *DurableGateway) EventCatchUp(ctx context.Context, conversationID string, lastConsumedSeq uint64) ([]Envelope, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if lastConsumedSeq > maxJSONSafeInteger {
		return nil, fmt.Errorf("browser event cursor %d exceeds JSON-safe integer range", lastConsumedSeq)
	}
	path := g.eventPath(conversationID)
	file, err := g.newFile(path, os.O_RDONLY, 0o600)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_SH); err != nil {
		return nil, fmt.Errorf("lock durable event log for browser replay: %w", err)
	}
	defer func() { _ = unlockDurableFile(file) }()

	r := bufio.NewReader(file)
	var previous uint64
	var out []Envelope
	for {
		line, readErr := r.ReadBytes('\n')
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) != 0 {
			var record durableEventRecord
			if err := json.Unmarshal(trimmed, &record); err != nil {
				return nil, fmt.Errorf("decode durable event log for browser replay: %w", err)
			}
			if record.Seq != previous+1 {
				return nil, fmt.Errorf("durable event log is non-contiguous: got %d after %d", record.Seq, previous)
			}
			if record.Event.Seq == nil || *record.Event.Seq != record.Seq {
				return nil, fmt.Errorf("durable event record seq mismatch: outer %d, inner %v", record.Seq, record.Event.Seq)
			}
			if record.Event.ConversationID != conversationID {
				return nil, fmt.Errorf("durable event record conversation mismatch: got %q, want %q", record.Event.ConversationID, conversationID)
			}
			previous = record.Seq
			if record.Seq > lastConsumedSeq {
				out = append(out, record.Event)
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("read durable event log for browser replay: %w", readErr)
		}
	}
	if lastConsumedSeq > previous {
		return nil, fmt.Errorf("browser event cursor %d is ahead of durable tail %d", lastConsumedSeq, previous)
	}
	return out, nil
}

// SubscribeBrowserVolatile registers one bounded live-only receiver. A slow
// consumer is disconnected rather than buffering unbounded volatile deltas.
func (g *DurableGateway) SubscribeBrowserVolatile(conversationID string) (<-chan Envelope, func()) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.nextBrowserSubscriber++
	id := g.nextBrowserSubscriber
	out := make(chan Envelope, 64)
	if g.browserSubscribers[conversationID] == nil {
		g.browserSubscribers[conversationID] = make(map[uint64]chan Envelope)
	}
	g.browserSubscribers[conversationID][id] = out
	var once sync.Once
	return out, func() {
		once.Do(func() {
			g.mu.Lock()
			defer g.mu.Unlock()
			g.removeBrowserSubscriberLocked(conversationID, id)
		})
	}
}

func (g *DurableGateway) publishVolatileLocked(conversationID string, envelope Envelope) {
	for id, subscriber := range g.browserSubscribers[conversationID] {
		select {
		case subscriber <- envelope:
		default:
			// The receiver is no longer a safe live stream. Removing and closing
			// it makes its writer fail closed; durable replay remains available on
			// reconnect.
			g.removeBrowserSubscriberLocked(conversationID, id)
		}
	}
}

func (g *DurableGateway) removeBrowserSubscriberLocked(conversationID string, id uint64) {
	subscribers := g.browserSubscribers[conversationID]
	if subscriber, ok := subscribers[id]; ok {
		delete(subscribers, id)
		close(subscriber)
	}
	if len(subscribers) == 0 {
		delete(g.browserSubscribers, conversationID)
	}
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
	if state.Generation > maxProcessGeneration {
		return runtimeState{}, fmt.Errorf("runtime generation %d exceeds process generation range", state.Generation)
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
	if record.Event.Seq == nil || *record.Event.Seq != record.Seq {
		return fmt.Errorf("durable event record seq mismatch: outer %d, inner %v", record.Seq, record.Event.Seq)
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
	st.eventCRC = updateCRC(st.eventCRC, data)
	return nil
}

func (g *DurableGateway) updateConversationStateLocked(conversationID string, event json.RawMessage) {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()
	g.applyEventStateLocked(conversationID, event)
}

func (g *DurableGateway) applyEventStateLocked(conversationID string, event json.RawMessage) {
	type eventHead struct {
		Type      string `json:"type"`
		RequestID string `json:"request_id"`
		Request   struct {
			ID string `json:"id"`
		} `json:"request"`
	}
	var head eventHead
	if err := json.Unmarshal(event, &head); err != nil {
		return
	}

	switch head.Type {
	case "agent_start":
		// One run spans turn replacement during hard/soft steering as well as
		// assistant message, tool, and continuation boundaries. Only AgentEnd
		// closes the abort-admission window.
		g.runInFlight[conversationID] = true
	case "agent_end":
		g.runInFlight[conversationID] = false
	case "approval_requested":
		if head.Request.ID == "" {
			return
		}
		if g.pendingApprovals[conversationID] == nil {
			g.pendingApprovals[conversationID] = make(map[string]bool)
		}
		g.pendingApprovals[conversationID][head.Request.ID] = true
	case "approval_resolved":
		if head.RequestID == "" {
			return
		}
		if g.pendingApprovals[conversationID] != nil {
			delete(g.pendingApprovals[conversationID], head.RequestID)
			if len(g.pendingApprovals[conversationID]) == 0 {
				delete(g.pendingApprovals, conversationID)
			}
		}
	}
}

// EnsureConversationStateRebuilt reconstructs the in-flight and pending-approval
// command guard state for conversationID from the durable event log. It is called
// by the browser WebSocket before command admission begins so that guards remain
// authoritative across API process restarts. If the durable log is corrupt,
// non-contiguous, or otherwise unreadable, reconstruction returns an error and
// the caller must fail closed rather than admitting commands.
func (g *DurableGateway) EnsureConversationStateRebuilt(ctx context.Context, conversationID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.stateRebuilt[conversationID] {
		return nil
	}

	events, err := g.EventCatchUp(ctx, conversationID, 0)
	if err != nil {
		return fmt.Errorf("rebuild conversation state: %w", err)
	}

	g.stateMu.Lock()
	for _, envelope := range events {
		g.applyEventStateLocked(conversationID, envelope.Event)
	}
	g.stateRebuilt[conversationID] = true
	g.stateMu.Unlock()

	return nil
}

// IsRunInFlight reports whether a durable agent_start has not yet been closed
// by agent_end. It is used by the browser command guard to reject meaningless
// aborts without closing the window during tool execution, continuation calls,
// or hard/soft-steer turn replacement.
func (g *DurableGateway) IsRunInFlight(conversationID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stateMu.RLock()
	defer g.stateMu.RUnlock()
	return g.runInFlight[conversationID]
}

// IsApprovalPending reports whether an approval with requestID is still awaiting
// a decision in the conversation. It is used by the browser WebSocket to reject
// approval_decision commands for unknown or already-resolved requests.
func (g *DurableGateway) IsApprovalPending(conversationID, requestID string) bool {
	if requestID == "" {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stateMu.RLock()
	defer g.stateMu.RUnlock()
	return g.pendingApprovals[conversationID] != nil && g.pendingApprovals[conversationID][requestID]
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
	st.ackCRC = updateCRC(st.ackCRC, data)
	return nil
}

func (g *DurableGateway) refreshEventTailLocked(file durableFileHandle, st *conversationLogState) error {
	size, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("seek durable event log: %w", err)
	}
	if size == st.eventSize {
		// File size is unchanged, but another process may have truncated and
		// rewritten to the same length. Verify the cached CRC before trusting
		// the in-memory tail state.
		crc, err := crc32OfFilePrefix(file, size)
		if err != nil {
			return fmt.Errorf("checksum durable event log prefix: %w", err)
		}
		if crc == st.eventCRC {
			return nil
		}
		st.eventSeq = 0
		st.eventSize = 0
		st.eventCRC = 0
	} else if size < st.eventSize {
		st.eventSeq = 0
		st.eventSize = 0
		st.eventCRC = 0
	}
	if st.eventSize > 0 && size > st.eventSize {
		// Before scanning an appended tail, confirm the existing prefix has
		// not been rewritten underneath us.
		prefixCRC, err := crc32OfFilePrefix(file, st.eventSize)
		if err != nil {
			return fmt.Errorf("checksum durable event log prefix: %w", err)
		}
		if prefixCRC != st.eventCRC {
			st.eventSeq = 0
			st.eventSize = 0
			st.eventCRC = 0
		}
	}

	start := st.eventSize
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return fmt.Errorf("seek durable event log for tail refresh: %w", err)
	}

	r := bufio.NewReader(file)
	offset := start
	last := st.eventSeq
	crc := st.eventCRC
	for {
		lineStart := offset
		line, readErr := r.ReadBytes('\n')
		if len(line) > 0 {
			offset += int64(len(line))
			crc = updateCRC(crc, line)
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
				crc, err = crc32OfFilePrefix(file, lineStart)
				if err != nil {
					return fmt.Errorf("checksum truncated durable event log: %w", err)
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
				crc = updateCRC(crc, []byte{'\n'})
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
	st.eventCRC = crc
	return nil
}

func (g *DurableGateway) refreshAckTailLocked(file durableFileHandle, st *conversationLogState) error {
	size, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("seek durable ack log: %w", err)
	}
	if size == st.ackSize {
		crc, err := crc32OfFilePrefix(file, size)
		if err != nil {
			return fmt.Errorf("checksum durable ack log prefix: %w", err)
		}
		if crc == st.ackCRC {
			return nil
		}
		st.acks = make(map[uint64]CommandAck)
		st.ackOrder = nil
		st.ackSize = 0
		st.ackCRC = 0
	} else if size < st.ackSize {
		st.acks = make(map[uint64]CommandAck)
		st.ackOrder = nil
		st.ackSize = 0
		st.ackCRC = 0
	}
	if st.ackSize > 0 && size > st.ackSize {
		prefixCRC, err := crc32OfFilePrefix(file, st.ackSize)
		if err != nil {
			return fmt.Errorf("checksum durable ack log prefix: %w", err)
		}
		if prefixCRC != st.ackCRC {
			st.acks = make(map[uint64]CommandAck)
			st.ackOrder = nil
			st.ackSize = 0
			st.ackCRC = 0
		}
	}

	start := st.ackSize
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return fmt.Errorf("seek durable ack log for tail refresh: %w", err)
	}

	r := bufio.NewReader(file)
	offset := start
	crc := st.ackCRC
	for {
		lineStart := offset
		line, readErr := r.ReadBytes('\n')
		if len(line) > 0 {
			offset += int64(len(line))
			crc = updateCRC(crc, line)
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
				crc, err = crc32OfFilePrefix(file, lineStart)
				if err != nil {
					return fmt.Errorf("checksum truncated durable ack log: %w", err)
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
				crc = updateCRC(crc, []byte{'\n'})
				offset += 1
			}
			break
		}
		if readErr != nil {
			return fmt.Errorf("read durable ack log: %w", readErr)
		}
	}

	st.ackSize = offset
	st.ackCRC = crc
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
	if state.Generation > maxProcessGeneration {
		return fmt.Errorf("runtime generation %d exceeds process generation range", state.Generation)
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
