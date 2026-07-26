package agentevents

// DurableGateway is the production adapter for the T28 API boundary. T26
// publishes one atomically-written state file per agent (generation + stable
// hydration receipt identity); commands, ACKs, and agent events are persisted here rather than
// being represented by cmd/server placeholders.

import (
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
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("gateway runtime state path must be a real directory")
	}
	return &DurableGateway{dir: abs, commands: commands}, nil
}

func (g *DurableGateway) VerifyGeneration(ctx context.Context, agentID string, generation uint64) error {
	state, err := g.state(ctx, agentID)
	if err != nil {
		return err
	}
	if !state.present {
		return nil
	}
	if state.Generation != generation {
		return fmt.Errorf("stale generation: got %d, current %d", generation, state.Generation)
	}
	return nil
}

func (g *DurableGateway) WaitFor(ctx context.Context, claims TokenClaims, generation uint64) error {
	ticker := time.NewTicker(50 * time.Millisecond)
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

func (g *DurableGateway) CatchUp(ctx context.Context, claims TokenClaims, fromSeq uint64) ([]CommandEnvelope, error) {
	return g.commands.CatchUp(ctx, claims.ConversationID, fromSeq)
}

func (g *DurableGateway) Live(ctx context.Context, claims TokenClaims, fromSeq uint64) (<-chan CommandEnvelope, error) {
	next := fromSeq
	out := make(chan CommandEnvelope, 16)
	go func() {
		defer close(out)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			commands, err := g.commands.CatchUp(ctx, claims.ConversationID, next)
			if err != nil {
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
	return out, nil
}

func (g *DurableGateway) ApplyAck(ctx context.Context, claims TokenClaims, ack CommandAck) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateCommandAck(ack); err != nil {
		return err
	}
	commands, err := g.commands.CatchUp(ctx, claims.ConversationID, ack.Seq)
	if err != nil {
		return fmt.Errorf("load acknowledged command: %w", err)
	}
	if len(commands) == 0 || commands[0].Seq != ack.Seq || commands[0].CommandID != ack.CommandID {
		return fmt.Errorf(
			"ack does not match durable command log: seq=%d command_id=%q",
			ack.Seq,
			ack.CommandID,
		)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.appendCommandAckLocked(g.ackPath(claims.ConversationID), ack)
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
		g.eventPath(claims.ConversationID),
		durableEventRecord{Seq: *envelope.Seq, Event: envelope},
	)
}

func (g *DurableGateway) LastReceivedEventSeq(ctx context.Context, claims TokenClaims) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.lastEventSeqLocked(claims.ConversationID)
}

func (g *DurableGateway) lastEventSeqLocked(conversationID string) (uint64, error) {
	file, err := os.Open(g.eventPath(conversationID))
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer file.Close()
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_SH); err != nil {
		return 0, fmt.Errorf("lock durable event log for read: %w", err)
	}
	defer func() { _ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN) }()
	decoder := json.NewDecoder(file)
	var last uint64
	for {
		var record durableEventRecord
		if err := decoder.Decode(&record); errors.Is(err, io.EOF) {
			return last, nil
		} else if err != nil {
			return 0, fmt.Errorf("decode durable event log: %w", err)
		}
		if record.Seq != last+1 {
			return 0, fmt.Errorf("durable event log is non-contiguous: got %d after %d", record.Seq, last)
		}
		last = record.Seq
	}
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
	state.present = true
	return state, nil
}

func (g *DurableGateway) appendDurableEventLocked(path string, record durableEventRecord) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock durable event log for append: %w", err)
	}
	defer func() { _ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN) }()

	decoder := json.NewDecoder(file)
	var last uint64
	for {
		var existing durableEventRecord
		if err := decoder.Decode(&existing); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("decode durable event log: %w", err)
		}
		if existing.Seq != last+1 {
			return fmt.Errorf("durable event log is non-contiguous: got %d after %d", existing.Seq, last)
		}
		last = existing.Seq
	}
	if record.Seq != last+1 {
		return fmt.Errorf("event seq is not contiguous: got %d, want %d", record.Seq, last+1)
	}
	line, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	if _, err := file.Write(append(line, '\n')); err != nil {
		return err
	}
	return file.Sync()
}

func (g *DurableGateway) appendCommandAckLocked(path string, ack CommandAck) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock durable ack log for append: %w", err)
	}
	defer func() { _ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN) }()

	decoder := json.NewDecoder(file)
	var previous *CommandAck
	for {
		var existing CommandAck
		if err := decoder.Decode(&existing); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("decode durable ack log: %w", err)
		}
		if existing.Seq == ack.Seq || existing.CommandID == ack.CommandID {
			if existing.Seq != ack.Seq || existing.CommandID != ack.CommandID {
				return fmt.Errorf("durable ack log contains mismatched seq/command_id correlation")
			}
			existingCopy := existing
			previous = &existingCopy
		}
	}
	if previous != nil {
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
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	if _, err := file.Write(append(line, '\n')); err != nil {
		return err
	}
	return file.Sync()
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
