package agentevents

// DurableGateway is the production adapter for the T28 API boundary. T26
// publishes one atomically-written state file per agent (generation + stable
// hydration receipt identity); commands, ACKs, and agent events are persisted
// here rather than being represented by cmd/server placeholders.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
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

	// localControlIntegrityKey is installed exactly once by the explicitly
	// enabled local control server. A separate mutex keeps state verification
	// race-free without coupling it to the gateway's event-log lock.
	localControlIntegrityMu  sync.RWMutex
	localControlIntegrityKey []byte
	localControlOwners       map[string]struct{}

	// PollInterval bounds the polling interval used by WaitFor and Live.
	// A zero value uses the safe default (50ms).
	PollInterval time.Duration
	// MaxPersonalityAgentTails and MaxAckTail bound process memory without changing
	// durable replay. Zero values use conservative defaults.
	MaxPersonalityAgentTails int
	MaxAckTail               int

	tails map[string]*personalityAgentLogState
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

type personalityAgentLogState struct {
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

const maxNextCommandAckStateBytes = 16 << 20

type compactAckState byte

const (
	ackStateAbsent compactAckState = iota
	ackStateReceived
	ackStateTerminal
)

type compactAckStates []byte

func newCompactAckStates(commandCount int) (compactAckStates, error) {
	if commandCount < 0 || commandCount > maxNextCommandAckStateBytes*4 {
		return nil, fmt.Errorf(
			"command ACK cursor for %d commands exceeds %d-byte budget",
			commandCount,
			maxNextCommandAckStateBytes,
		)
	}
	byteCount := (commandCount + 3) / 4
	return make(compactAckStates, byteCount), nil
}

func (s compactAckStates) get(index int) compactAckState {
	shift := uint((index % 4) * 2)
	return compactAckState((s[index/4] >> shift) & 0x3)
}

func (s compactAckStates) set(index int, state compactAckState) {
	shift := uint((index % 4) * 2)
	mask := byte(0x3 << shift)
	s[index/4] = (s[index/4] &^ mask) | byte(state)<<shift
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
	// LocalControl is present only when the explicitly enabled local/CI
	// control plane owns this state file. The browser fixture's direct
	// PublishRuntimeState helper deliberately leaves it nil.
	LocalControl *localControlDurableState `json:"local_control,omitempty"`
	present      bool
}

type connectionLeaseState struct {
	Version    uint64                      `json:"version"`
	Generation uint64                      `json:"generation"`
	Sequence   uint64                      `json:"sequence"`
	LeaseID    string                      `json:"lease_id,omitempty"`
	Active     bool                        `json:"active"`
	Integrity  *localControlStateIntegrity `json:"integrity,omitempty"`
	present    bool
}

const (
	connectionLeaseStateVersion  = uint64(1)
	maxConnectionLeaseStateBytes = 4096
	connectionLeaseIDBytes       = 32
)

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

// durableFileHandle abstracts the per-personality-agent log file so tests can
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
		dir:                      abs,
		commands:                 commands,
		PollInterval:             50 * time.Millisecond,
		MaxPersonalityAgentTails: 128,
		MaxAckTail:               256,
		tails:                    make(map[string]*personalityAgentLogState),
		browserSubscribers:       make(map[string]map[uint64]chan Envelope),
		stateRebuilt:             make(map[string]bool),
		runInFlight:              make(map[string]bool),
		pendingApprovals:         make(map[string]map[string]bool),
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

func (g *DurableGateway) maxPersonalityAgentTails() int {
	if g.MaxPersonalityAgentTails > 0 {
		return g.MaxPersonalityAgentTails
	}
	return 128
}

func (g *DurableGateway) maxAckTail() int {
	if g.MaxAckTail > 0 {
		return g.MaxAckTail
	}
	return 256
}

func (g *DurableGateway) VerifyGeneration(ctx context.Context, personalityAgentID string, generation uint64) error {
	state, err := g.state(ctx, personalityAgentID)
	if err != nil {
		return err
	}
	return verifyRuntimeGeneration(state, generation)
}

// ClaimConnectionLease is the file-backed PAID-global ownership boundary.
// Every API replica serving agent WebSockets must point at the same
// POSIX-flock-coherent runtime directory; a per-replica directory cannot
// provide cross-process single ownership.
func (g *DurableGateway) ClaimConnectionLease(
	ctx context.Context,
	claims TokenClaims,
) (ConnectionLease, error) {
	if err := ctx.Err(); err != nil {
		return ConnectionLease{}, err
	}
	if err := ValidatePersonalityAgentID(claims.PersonalityAgentID); err != nil {
		return ConnectionLease{}, err
	}
	lock, err := openLocalControlLock(g.localControlLockPath(claims.PersonalityAgentID))
	if err != nil {
		return ConnectionLease{}, err
	}
	defer lock.Close()
	if err := flockContext(ctx, lock.Fd(), syscall.LOCK_EX); err != nil {
		return ConnectionLease{}, fmt.Errorf("lock connection lease claim: %w", err)
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()

	state, err := g.state(ctx, claims.PersonalityAgentID)
	if err != nil {
		return ConnectionLease{}, err
	}
	if err := verifyRuntimeGeneration(state, claims.Generation); err != nil {
		return ConnectionLease{}, err
	}
	previous, err := g.connectionLeaseState(claims.PersonalityAgentID)
	if err != nil {
		return ConnectionLease{}, err
	}
	if previous.Sequence >= maxJSONSafeInteger {
		return ConnectionLease{}, errors.New("connection lease sequence exhausted")
	}
	leaseIDRaw := make([]byte, connectionLeaseIDBytes)
	if _, err := rand.Read(leaseIDRaw); err != nil {
		return ConnectionLease{}, fmt.Errorf("generate connection lease identity: %w", err)
	}
	lease := ConnectionLease{
		Generation: claims.Generation,
		Sequence:   previous.Sequence + 1,
		ID:         base64.RawURLEncoding.EncodeToString(leaseIDRaw),
	}
	record := connectionLeaseState{
		Version:    connectionLeaseStateVersion,
		Generation: lease.Generation,
		Sequence:   lease.Sequence,
		LeaseID:    lease.ID,
		Active:     true,
	}
	if err := g.writeConnectionLeaseState(claims.PersonalityAgentID, record); err != nil {
		return ConnectionLease{}, err
	}
	return lease, nil
}

func (g *DurableGateway) ValidateConnectionLease(
	ctx context.Context,
	claims TokenClaims,
	lease ConnectionLease,
) error {
	return g.WithConnectionLease(ctx, claims, lease, func() error { return nil })
}

func (g *DurableGateway) WithConnectionLease(
	ctx context.Context,
	claims TokenClaims,
	lease ConnectionLease,
	call func() error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidatePersonalityAgentID(claims.PersonalityAgentID); err != nil {
		return err
	}
	if lease.Generation != claims.Generation {
		return errConnectionEpochRevoked
	}
	lock, err := openLocalControlLock(g.localControlLockPath(claims.PersonalityAgentID))
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := flockContext(ctx, lock.Fd(), syscall.LOCK_SH); err != nil {
		return fmt.Errorf("lock connection lease validation: %w", err)
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()
	if err := g.validateConnectionLeaseLocked(ctx, claims, lease); err != nil {
		return err
	}
	return call()
}

func (g *DurableGateway) ReleaseConnectionLease(
	ctx context.Context,
	claims TokenClaims,
	lease ConnectionLease,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidatePersonalityAgentID(claims.PersonalityAgentID); err != nil {
		return err
	}
	if lease.Generation != claims.Generation {
		return errConnectionEpochRevoked
	}
	lock, err := openLocalControlLock(g.localControlLockPath(claims.PersonalityAgentID))
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := flockContext(ctx, lock.Fd(), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock connection lease release: %w", err)
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()
	record, err := g.connectionLeaseState(claims.PersonalityAgentID)
	if err != nil {
		return err
	}
	if !connectionLeaseMatches(record, lease) {
		return nil
	}
	record.Active = false
	record.LeaseID = ""
	return g.writeConnectionLeaseState(claims.PersonalityAgentID, record)
}

func (g *DurableGateway) validateConnectionLeaseLocked(
	ctx context.Context,
	claims TokenClaims,
	lease ConnectionLease,
) error {
	state, err := g.state(ctx, claims.PersonalityAgentID)
	if err != nil {
		return err
	}
	if err := verifyRuntimeGeneration(state, claims.Generation); err != nil {
		return err
	}
	record, err := g.connectionLeaseState(claims.PersonalityAgentID)
	if err != nil {
		return err
	}
	if !connectionLeaseMatches(record, lease) ||
		lease.Generation != claims.Generation {
		return errConnectionEpochRevoked
	}
	return nil
}

func connectionLeaseMatches(record connectionLeaseState, lease ConnectionLease) bool {
	return record.present &&
		record.Active &&
		record.Generation == lease.Generation &&
		record.Sequence == lease.Sequence &&
		record.LeaseID == lease.ID &&
		lease.ID != ""
}

func verifyRuntimeGeneration(state runtimeState, generation uint64) error {
	if !state.present {
		return errors.New("durable runtime state is absent")
	}
	if state.Generation != generation {
		return fmt.Errorf("stale generation: got %d, current %d", generation, state.Generation)
	}
	return nil
}

// IsPersonalityAgentReady reports the authoritative Ready latch for the one
// global runtime identity. Tenant is intentionally absent from this key.
func (g *DurableGateway) IsPersonalityAgentReady(ctx context.Context, personalityAgentID string) (bool, error) {
	state, err := g.state(ctx, personalityAgentID)
	if err != nil {
		return false, err
	}
	return state.present && state.HydrationReceiptIdentity != nil, nil
}

func (g *DurableGateway) WaitFor(ctx context.Context, claims TokenClaims, generation uint64) error {
	ticker := time.NewTicker(g.pollInterval())
	defer ticker.Stop()
	for {
		state, err := g.state(ctx, claims.PersonalityAgentID)
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
	return g.commands.FirstCommandSeq(ctx, claims.PersonalityAgentID)
}

func (g *DurableGateway) HasCommands(ctx context.Context, claims TokenClaims) (bool, error) {
	return g.commands.HasCommands(ctx, claims.PersonalityAgentID)
}

// NextCommandSeq returns the earliest command without a durable terminal ACK.
// It is the sole replay cursor authority: agent hello state may bound it, but
// can never advance it past an ACK the API has not durably recorded.
func (g *DurableGateway) NextCommandSeq(ctx context.Context, claims TokenClaims) (uint64, error) {
	file, err := g.newFile(g.ackPath(claims.PersonalityAgentID), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	if err := flockContext(ctx, file.Fd(), syscall.LOCK_EX); err != nil {
		return 0, fmt.Errorf("lock durable ack log for replay cursor: %w", err)
	}
	defer func() { _ = unlockDurableFile(file) }()

	// Take the command view after excluding ACK appenders. Any command appended
	// after this snapshot cannot acquire an ACK until this scan releases the
	// per-personality-agent file lock, so returning snapshot.nextSeq is conservative.
	snapshot, err := g.commands.commandSnapshot(ctx, claims.PersonalityAgentID)
	if err != nil {
		return 0, err
	}
	states, err := newCompactAckStates(len(snapshot.commands))
	if err != nil {
		return 0, err
	}
	if err := scanAckCursor(ctx, file, snapshot, states); err != nil {
		return 0, err
	}
	if len(snapshot.commands) == 0 {
		return snapshot.nextSeq, nil
	}
	for index, command := range snapshot.commands {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if states.get(index) != ackStateTerminal {
			return command.Seq, nil
		}
	}
	if snapshot.nextSeq > maxJSONSafeInteger {
		return 0, fmt.Errorf("next_command_seq would exceed JSON-safe integer range")
	}
	return snapshot.nextSeq, nil
}

func (g *DurableGateway) CatchUp(ctx context.Context, claims TokenClaims, fromSeq uint64) ([]CommandEnvelope, error) {
	return g.commands.CatchUp(ctx, claims.PersonalityAgentID, fromSeq)
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
			commands, err := g.commands.CatchUp(ctx, claims.PersonalityAgentID, next)
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
	if ack.PersonalityAgentID != claims.PersonalityAgentID {
		return errors.New("command ACK target does not match token claim")
	}
	cmd, found, err := g.commands.GetCommand(ctx, claims.PersonalityAgentID, ack.Seq)
	if err != nil {
		return fmt.Errorf("load acknowledged command: %w", err)
	}
	if !found || cmd.CommandID != ack.CommandID || cmd.PersonalityAgentID != ack.PersonalityAgentID {
		return fmt.Errorf(
			"ack does not match durable command log: seq=%d command_id=%q",
			ack.Seq,
			ack.CommandID,
		)
	}
	// Reject an already-stale caller before opening (and potentially creating)
	// its ACK log. appendCommandAck repeats this fence after acquiring the ACK
	// and cache locks, which closes the check-to-commit race.
	if err := g.withCurrentGeneration(ctx, claims, func() error { return nil }); err != nil {
		return err
	}
	return g.appendCommandAck(ctx, claims, ack)
}

func (g *DurableGateway) Receive(ctx context.Context, claims TokenClaims, envelope Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateEnvelope(envelope); err != nil {
		return err
	}
	if envelope.PersonalityAgentID != claims.PersonalityAgentID {
		return fmt.Errorf(
			"event personality_agent_id %q does not match token claim %q",
			envelope.PersonalityAgentID,
			claims.PersonalityAgentID,
		)
	}
	if err := lockMutexContext(ctx, &g.mu); err != nil {
		return err
	}
	defer g.mu.Unlock()
	return g.withCurrentGeneration(ctx, claims, func() error {
		if envelope.Seq == nil { // volatile frames are deliberately not part of replay.
			g.publishVolatileLocked(claims.PersonalityAgentID, envelope)
			return nil
		}
		if err := g.appendDurableEventLocked(
			ctx,
			claims.PersonalityAgentID,
			durableEventRecord{Seq: *envelope.Seq, Event: envelope},
		); err != nil {
			return err
		}
		g.updateAgentSessionStateLocked(claims.PersonalityAgentID, envelope.Event)
		return nil
	})
}

// withCurrentGeneration serializes the authoritative generation check and the
// complete synchronous side effect with runtime-state publication for this
// personality agent. A rollover that has committed therefore excludes all
// subsequent work from the prior generation, while a side effect already
// admitted must finish before that rollover can commit.
func (g *DurableGateway) withCurrentGeneration(
	ctx context.Context,
	claims TokenClaims,
	call func() error,
) error {
	lock, err := openLocalControlLock(g.localControlLockPath(claims.PersonalityAgentID))
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := flockContext(ctx, lock.Fd(), syscall.LOCK_SH); err != nil {
		return fmt.Errorf("lock runtime generation for side effect: %w", err)
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()
	if err := ctx.Err(); err != nil {
		return err
	}
	state, err := g.state(ctx, claims.PersonalityAgentID)
	if err != nil {
		return err
	}
	if err := verifyRuntimeGeneration(state, claims.Generation); err != nil {
		return err
	}
	record, err := g.connectionLeaseState(claims.PersonalityAgentID)
	if err != nil {
		return err
	}
	if record.Active {
		lease, ok := connectionLeaseFromContext(ctx)
		if !ok || !connectionLeaseMatches(record, lease) ||
			lease.Generation != claims.Generation {
			return errConnectionEpochRevoked
		}
	}
	return call()
}

// EventCatchUp returns the durable event suffix after lastConsumedSeq. It
// verifies the complete retained log on every read, so a gap or corrupt record
// fails closed rather than being silently skipped during browser reconnect.
func (g *DurableGateway) EventCatchUp(ctx context.Context, personalityAgentID string, lastConsumedSeq uint64) ([]Envelope, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
		return nil, err
	}
	if lastConsumedSeq > maxJSONSafeInteger {
		return nil, fmt.Errorf("browser event cursor %d exceeds JSON-safe integer range", lastConsumedSeq)
	}
	path := g.eventPath(personalityAgentID)
	file, err := g.newFile(path, os.O_RDONLY, 0o600)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if err := flockContext(ctx, file.Fd(), syscall.LOCK_SH); err != nil {
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
			if record.Event.PersonalityAgentID != personalityAgentID {
				return nil, fmt.Errorf("durable event record personality agent mismatch: got %q, want %q", record.Event.PersonalityAgentID, personalityAgentID)
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
func (g *DurableGateway) SubscribeBrowserVolatile(personalityAgentID string) (<-chan Envelope, func()) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.nextBrowserSubscriber++
	id := g.nextBrowserSubscriber
	out := make(chan Envelope, 64)
	if g.browserSubscribers[personalityAgentID] == nil {
		g.browserSubscribers[personalityAgentID] = make(map[uint64]chan Envelope)
	}
	g.browserSubscribers[personalityAgentID][id] = out
	var once sync.Once
	return out, func() {
		once.Do(func() {
			g.mu.Lock()
			defer g.mu.Unlock()
			g.removeBrowserSubscriberLocked(personalityAgentID, id)
		})
	}
}

func (g *DurableGateway) publishVolatileLocked(personalityAgentID string, envelope Envelope) {
	for id, subscriber := range g.browserSubscribers[personalityAgentID] {
		select {
		case subscriber <- envelope:
		default:
			// The receiver is no longer a safe live stream. Removing and closing
			// it makes its writer fail closed; durable replay remains available on
			// reconnect.
			g.removeBrowserSubscriberLocked(personalityAgentID, id)
		}
	}
}

func (g *DurableGateway) removeBrowserSubscriberLocked(personalityAgentID string, id uint64) {
	subscribers := g.browserSubscribers[personalityAgentID]
	if subscriber, ok := subscribers[id]; ok {
		delete(subscribers, id)
		close(subscriber)
	}
	if len(subscribers) == 0 {
		delete(g.browserSubscribers, personalityAgentID)
	}
}

func (g *DurableGateway) LastReceivedEventSeq(ctx context.Context, claims TokenClaims) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := lockMutexContext(ctx, &g.mu); err != nil {
		return 0, err
	}
	defer g.mu.Unlock()
	st := g.stateFor(claims.PersonalityAgentID)
	path := g.eventPath(claims.PersonalityAgentID)
	file, err := g.newFile(path, os.O_RDWR, 0o600)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer file.Close()
	if err := flockContext(ctx, file.Fd(), syscall.LOCK_EX); err != nil {
		return 0, fmt.Errorf("lock durable event log for read: %w", err)
	}
	defer func() { _ = unlockDurableFile(file) }()
	if err := g.refreshEventTailLocked(file, st); err != nil {
		return 0, err
	}
	return st.eventSeq, nil
}

func (g *DurableGateway) stateFor(personalityAgentID string) *personalityAgentLogState {
	g.clock++
	st, ok := g.tails[personalityAgentID]
	if ok {
		st.lastUsed = g.clock
		return st
	}
	st = &personalityAgentLogState{acks: make(map[uint64]CommandAck), lastUsed: g.clock}
	g.tails[personalityAgentID] = st
	g.evictInactiveTailsLocked(personalityAgentID)
	return st
}

func (g *DurableGateway) evictInactiveTailsLocked(activePersonalityAgentID string) {
	for len(g.tails) > g.maxPersonalityAgentTails() {
		var evictID string
		var oldest uint64
		for personalityAgentID, state := range g.tails {
			if personalityAgentID == activePersonalityAgentID {
				continue
			}
			if evictID == "" || state.lastUsed < oldest || (state.lastUsed == oldest && personalityAgentID < evictID) {
				evictID, oldest = personalityAgentID, state.lastUsed
			}
		}
		if evictID == "" {
			return
		}
		delete(g.tails, evictID)
	}
}

func (g *DurableGateway) rememberAckLocked(st *personalityAgentLogState, ack CommandAck) {
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
	return left.Seq == right.Seq &&
		left.CommandID == right.CommandID &&
		left.PersonalityAgentID == right.PersonalityAgentID &&
		left.Status == right.Status &&
		stringPointerEqual(left.RejectReason, right.RejectReason)
}

func (g *DurableGateway) state(ctx context.Context, personalityAgentID string) (runtimeState, error) {
	if err := ctx.Err(); err != nil {
		return runtimeState{}, err
	}
	if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
		return runtimeState{}, err
	}
	path := g.statePath(personalityAgentID)
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if errors.Is(err, os.ErrNotExist) {
		return runtimeState{}, nil
	}
	if err != nil {
		return runtimeState{}, fmt.Errorf("open durable runtime state: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return runtimeState{}, fmt.Errorf("inspect durable runtime state: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return runtimeState{}, errors.New("invalid durable runtime state")
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxLocalControlDurableStateBytes+1))
	if err != nil {
		return runtimeState{}, err
	}
	if len(raw) > maxLocalControlDurableStateBytes {
		return runtimeState{}, errors.New("durable runtime state exceeds maximum allowed size")
	}
	if err := checkDuplicateKeys(raw); err != nil {
		return runtimeState{}, fmt.Errorf("decode durable runtime state: %w", err)
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
	if err := g.verifyLocalControlRuntimeStateIntegrity(state); err != nil {
		return runtimeState{}, fmt.Errorf("decode durable runtime state: %w", err)
	}
	if err := validateLocalControlRuntimeState(personalityAgentID, state); err != nil {
		return runtimeState{}, fmt.Errorf("decode durable runtime state: %w", err)
	}
	state.present = true
	return state, nil
}

// connectionLeaseState reads the lease record while the caller holds the
// PAID runtime lock. The record contains only an opaque random lease identity,
// generation, and monotonic sequence; tenant/user identity is never persisted.
func (g *DurableGateway) connectionLeaseState(personalityAgentID string) (connectionLeaseState, error) {
	path := g.connectionLeasePath(personalityAgentID)
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if errors.Is(err, os.ErrNotExist) {
		return connectionLeaseState{}, nil
	}
	if err != nil {
		return connectionLeaseState{}, fmt.Errorf("open connection lease state: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return connectionLeaseState{}, fmt.Errorf("inspect connection lease state: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return connectionLeaseState{}, errors.New("invalid connection lease state")
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxConnectionLeaseStateBytes+1))
	if err != nil {
		return connectionLeaseState{}, fmt.Errorf("read connection lease state: %w", err)
	}
	if len(raw) > maxConnectionLeaseStateBytes {
		return connectionLeaseState{}, errors.New("connection lease state exceeds maximum allowed size")
	}
	if err := checkDuplicateKeys(raw); err != nil {
		return connectionLeaseState{}, fmt.Errorf("decode connection lease state: %w", err)
	}
	var record connectionLeaseState
	if err := unmarshalStrict(raw, &record); err != nil {
		return connectionLeaseState{}, fmt.Errorf("decode connection lease state: %w", err)
	}
	if record.Version != connectionLeaseStateVersion ||
		record.Generation > maxProcessGeneration ||
		record.Sequence == 0 ||
		record.Sequence > maxJSONSafeInteger {
		return connectionLeaseState{}, errors.New("invalid connection lease state")
	}
	if record.Active {
		decoded, err := base64.RawURLEncoding.DecodeString(record.LeaseID)
		if err != nil || len(decoded) != connectionLeaseIDBytes {
			return connectionLeaseState{}, errors.New("invalid connection lease identity")
		}
	} else if record.LeaseID != "" {
		return connectionLeaseState{}, errors.New("inactive connection lease must not retain identity")
	}
	if err := g.verifyConnectionLeaseIntegrity(personalityAgentID, record); err != nil {
		return connectionLeaseState{}, err
	}
	record.present = true
	return record, nil
}

func (g *DurableGateway) writeConnectionLeaseState(
	personalityAgentID string,
	record connectionLeaseState,
) error {
	if err := g.signConnectionLeaseState(personalityAgentID, &record); err != nil {
		return err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode connection lease state: %w", err)
	}
	if err := writeFileAtomic(g.connectionLeasePath(personalityAgentID), raw, 0o600); err != nil {
		return fmt.Errorf("persist connection lease state: %w", err)
	}
	return nil
}

func (g *DurableGateway) signConnectionLeaseState(
	personalityAgentID string,
	record *connectionLeaseState,
) error {
	if !g.localControlOwns(personalityAgentID) {
		record.Integrity = nil
		return nil
	}
	key, ok := g.localControlIntegrityKeySnapshot()
	if !ok {
		return errors.New("local control connection lease integrity key is unavailable")
	}
	mac, err := connectionLeaseStateMAC(key, personalityAgentID, *record)
	if err != nil {
		return err
	}
	record.Integrity = &localControlStateIntegrity{
		Version: localControlIntegrityVersion,
		MAC:     hex.EncodeToString(mac),
	}
	return nil
}

func (g *DurableGateway) verifyConnectionLeaseIntegrity(
	personalityAgentID string,
	record connectionLeaseState,
) error {
	if record.Integrity == nil {
		if g.localControlOwns(personalityAgentID) {
			return errors.New("local control connection lease integrity is missing")
		}
		return nil
	}
	key, ok := g.localControlIntegrityKeySnapshot()
	if !ok {
		return errors.New("local control connection lease integrity key is unavailable")
	}
	if record.Integrity.Version != localControlIntegrityVersion {
		return errors.New("invalid local control connection lease integrity version")
	}
	actual, err := hex.DecodeString(record.Integrity.MAC)
	if err != nil || len(actual) != sha256.Size {
		return errors.New("invalid local control connection lease integrity")
	}
	expected, err := connectionLeaseStateMAC(key, personalityAgentID, record)
	if err != nil {
		return err
	}
	if !hmac.Equal(actual, expected) {
		return errors.New("invalid local control connection lease integrity")
	}
	return nil
}

func connectionLeaseStateMAC(
	key []byte,
	personalityAgentID string,
	record connectionLeaseState,
) ([]byte, error) {
	record.Integrity = nil
	record.present = false
	raw, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("encode connection lease integrity payload: %w", err)
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte("sumi-connection-lease-state/v1\x00"))
	_, _ = mac.Write([]byte(personalityAgentID))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write(raw)
	return mac.Sum(nil), nil
}

func (g *DurableGateway) appendDurableEventLocked(
	ctx context.Context,
	personalityAgentID string,
	record durableEventRecord,
) error {
	st := g.stateFor(personalityAgentID)
	path := g.eventPath(personalityAgentID)
	file, err := g.newFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := flockContext(ctx, file.Fd(), syscall.LOCK_EX); err != nil {
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

func (g *DurableGateway) updateAgentSessionStateLocked(personalityAgentID string, event json.RawMessage) {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()
	g.applyEventStateLocked(personalityAgentID, event)
}

func (g *DurableGateway) applyEventStateLocked(personalityAgentID string, event json.RawMessage) {
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
		g.runInFlight[personalityAgentID] = true
	case "agent_end":
		g.runInFlight[personalityAgentID] = false
	case "approval_requested":
		if head.Request.ID == "" {
			return
		}
		if g.pendingApprovals[personalityAgentID] == nil {
			g.pendingApprovals[personalityAgentID] = make(map[string]bool)
		}
		g.pendingApprovals[personalityAgentID][head.Request.ID] = true
	case "approval_resolved":
		if head.RequestID == "" {
			return
		}
		if g.pendingApprovals[personalityAgentID] != nil {
			delete(g.pendingApprovals[personalityAgentID], head.RequestID)
			if len(g.pendingApprovals[personalityAgentID]) == 0 {
				delete(g.pendingApprovals, personalityAgentID)
			}
		}
	}
}

// EnsureAgentSessionStateRebuilt reconstructs the in-flight and pending-approval
// command guard state for personalityAgentID from the durable event log. It is called
// by the browser WebSocket before command admission begins so that guards remain
// authoritative across API process restarts. If the durable log is corrupt,
// non-contiguous, or otherwise unreadable, reconstruction returns an error and
// the caller must fail closed rather than admitting commands.
func (g *DurableGateway) EnsureAgentSessionStateRebuilt(ctx context.Context, personalityAgentID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.stateRebuilt[personalityAgentID] {
		return nil
	}

	events, err := g.EventCatchUp(ctx, personalityAgentID, 0)
	if err != nil {
		return fmt.Errorf("rebuild agent session state: %w", err)
	}

	g.stateMu.Lock()
	for _, envelope := range events {
		g.applyEventStateLocked(personalityAgentID, envelope.Event)
	}
	g.stateRebuilt[personalityAgentID] = true
	g.stateMu.Unlock()

	return nil
}

// IsRunInFlight reports whether a durable agent_start has not yet been closed
// by agent_end. It is used by the browser command guard to reject meaningless
// aborts without closing the window during tool execution, continuation calls,
// or hard/soft-steer turn replacement.
func (g *DurableGateway) IsRunInFlight(personalityAgentID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stateMu.RLock()
	defer g.stateMu.RUnlock()
	return g.runInFlight[personalityAgentID]
}

// IsApprovalPending reports whether an approval with requestID is still awaiting
// a decision in the agent session. It is used by the browser WebSocket to reject
// approval_decision commands for unknown or already-resolved requests.
func (g *DurableGateway) IsApprovalPending(personalityAgentID, requestID string) bool {
	if requestID == "" {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stateMu.RLock()
	defer g.stateMu.RUnlock()
	return g.pendingApprovals[personalityAgentID] != nil && g.pendingApprovals[personalityAgentID][requestID]
}

func (g *DurableGateway) appendCommandAck(
	ctx context.Context,
	claims TokenClaims,
	ack CommandAck,
) error {
	path := g.ackPath(claims.PersonalityAgentID)
	file, err := g.newFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	// Lock only this ACK log while waiting for I/O. The process-wide cache lock
	// is acquired afterwards, so unrelated personality agents remain independent.
	if err := flockContext(ctx, file.Fd(), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock durable ack log for append: %w", err)
	}
	defer func() { _ = unlockDurableFile(file) }()
	if err := lockMutexContext(ctx, &g.mu); err != nil {
		return err
	}
	defer g.mu.Unlock()
	return g.withCurrentGeneration(ctx, claims, func() error {
		st := g.stateFor(claims.PersonalityAgentID)

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
	})
}

func (g *DurableGateway) refreshEventTailLocked(file durableFileHandle, st *personalityAgentLogState) error {
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

func (g *DurableGateway) refreshAckTailLocked(file durableFileHandle, st *personalityAgentLogState) error {
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

// scanAckCursor validates and folds the ACK log in one pass using two bits per
// durable command. It deliberately does not populate the bounded ACK tail:
// NextCommandSeq needs one restart-safe cursor calculation, not another
// unbounded history cache.
func scanAckCursor(ctx context.Context, file durableFileHandle, snapshot commandLogSnapshot, states compactAckStates) error {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek durable ack log for replay cursor: %w", err)
	}
	r := bufio.NewReader(file)
	var offset int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		lineStart := offset
		line, readErr := r.ReadBytes('\n')
		offset += int64(len(line))
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) != 0 {
			var ack CommandAck
			if err := json.Unmarshal(trimmed, &ack); err != nil {
				if readErr == io.EOF && isIncompleteJSONError(err) {
					if truncErr := file.Truncate(lineStart); truncErr != nil {
						return fmt.Errorf("truncate partial durable ack tail: %w", truncErr)
					}
					if syncErr := file.Sync(); syncErr != nil {
						return fmt.Errorf("sync after truncating partial durable ack tail: %w", syncErr)
					}
					return nil
				}
				return fmt.Errorf("decode durable ack log for replay cursor: %w", err)
			}
			if err := foldAckCursorRecord(snapshot, states, ack); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			if len(line) > 0 && line[len(line)-1] != '\n' {
				if _, err := file.Seek(0, io.SeekEnd); err != nil {
					return fmt.Errorf("seek durable ack log end for newline repair: %w", err)
				}
				if _, err := file.Write([]byte{'\n'}); err != nil {
					return fmt.Errorf("repair missing trailing newline in durable ack log: %w", err)
				}
				if err := file.Sync(); err != nil {
					return fmt.Errorf("sync repaired durable ack log trailing newline: %w", err)
				}
			}
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("read durable ack log for replay cursor: %w", readErr)
		}
	}
}

// foldAckCursorRecord owns validation at the compact-state transition
// boundary. The decoder also validates, but no alternate construction path can
// make an unknown status terminal merely by bypassing it.
func foldAckCursorRecord(snapshot commandLogSnapshot, states compactAckStates, ack CommandAck) error {
	if err := validateCommandAck(ack); err != nil {
		return fmt.Errorf("validate durable ack log for replay cursor: %w", err)
	}
	if len(snapshot.commands) == 0 {
		return fmt.Errorf("durable ACK seq %d has no matching command", ack.Seq)
	}
	firstSeq := snapshot.commands[0].Seq
	if ack.Seq < firstSeq {
		return fmt.Errorf("durable ACK seq %d precedes command log", ack.Seq)
	}
	delta := ack.Seq - firstSeq
	if delta >= uint64(len(snapshot.commands)) {
		return fmt.Errorf("durable ACK seq %d has no matching command", ack.Seq)
	}
	index := int(delta)
	command := snapshot.commands[index]
	if command.Seq != ack.Seq || command.CommandID != ack.CommandID {
		return fmt.Errorf("durable ACK identity mismatch at seq %d: command_id=%q ack_command_id=%q", ack.Seq, command.CommandID, ack.CommandID)
	}
	switch states.get(index) {
	case ackStateAbsent:
		if ack.Status == "received" {
			states.set(index, ackStateReceived)
		} else {
			states.set(index, ackStateTerminal)
		}
	case ackStateReceived:
		if ack.Status == "received" {
			return fmt.Errorf("durable ACK seq %d repeats received transition", ack.Seq)
		}
		states.set(index, ackStateTerminal)
	case ackStateTerminal:
		return fmt.Errorf("durable ACK seq %d has multiple terminal transitions", ack.Seq)
	default:
		return fmt.Errorf("durable ACK seq %d has invalid compact state", ack.Seq)
	}
	return nil
}

// findAckLocked reloads an evicted ACK entry from its durable log. The cache
// may only retain a bounded tail, but terminal ACK transitions remain valid
// regardless of personality-agent age or process lifetime.
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

func (g *DurableGateway) publishRuntimeState(personalityAgentID string, state runtimeState) error {
	if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
		return err
	}
	if g.localControlOwns(personalityAgentID) {
		return errors.New("direct runtime state publication is disabled while local control owns the registry")
	}
	if state.Generation > maxProcessGeneration {
		return fmt.Errorf("runtime generation %d exceeds process generation range", state.Generation)
	}
	lock, err := openLocalControlLock(g.localControlLockPath(personalityAgentID))
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := flockContext(context.Background(), lock.Fd(), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock runtime state publication: %w", err)
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return writeFileAtomic(g.statePath(personalityAgentID), raw, 0o600)
}

// PublishRuntimeState atomically publishes the global generation and Ready
// latch for one personality agent. A nil receipt means NotReady.
func (g *DurableGateway) PublishRuntimeState(personalityAgentID string, generation uint64, hydrationReceiptIdentity *string) error {
	if generation > maxProcessGeneration {
		return fmt.Errorf("runtime generation %d exceeds process generation range", generation)
	}
	if hydrationReceiptIdentity != nil && *hydrationReceiptIdentity == "" {
		return errors.New("hydration receipt identity must not be empty")
	}
	return g.publishRuntimeState(personalityAgentID, runtimeState{
		Generation:               generation,
		HydrationReceiptIdentity: hydrationReceiptIdentity,
	})
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

func (g *DurableGateway) statePath(personalityAgentID string) string {
	return filepath.Join(g.dir, "runtime-"+safeFileID(personalityAgentID)+".json")
}

func (g *DurableGateway) connectionLeasePath(personalityAgentID string) string {
	return filepath.Join(g.dir, "connection-"+safeFileID(personalityAgentID)+".json")
}
func (g *DurableGateway) eventPath(personalityAgentID string) string {
	return filepath.Join(g.dir, "events-"+safeFileID(personalityAgentID)+".jsonl")
}
func (g *DurableGateway) ackPath(personalityAgentID string) string {
	return filepath.Join(g.dir, "acks-"+safeFileID(personalityAgentID)+".jsonl")
}
func safeFileID(value string) string { return base64.RawURLEncoding.EncodeToString([]byte(value)) }
