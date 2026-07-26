package agentevents

// DurableGateway is the production adapter for the T28 API boundary. T26
// publishes one atomically-written state file per agent (generation + ready
// latch); commands, ACKs, and agent events are persisted here rather than
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
	"time"
)

type DurableGateway struct {
	dir      string
	commands *CommandStore
	mu       sync.Mutex
}

type runtimeState struct {
	Generation uint64 `json:"generation"`
	Ready      bool   `json:"ready"`
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
		if state.Generation != generation {
			return fmt.Errorf("hydration generation changed: got %d, current %d", generation, state.Generation)
		}
		if state.Ready {
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
	return g.appendJSONLine(g.ackPath(claims.ConversationID), ack)
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
	last, err := g.lastEventSeqLocked(claims.ConversationID)
	if err != nil {
		return err
	}
	if *envelope.Seq != last+1 {
		return fmt.Errorf("event seq is not contiguous: got %d, want %d", *envelope.Seq, last+1)
	}
	return g.appendJSONLineLocked(g.eventPath(claims.ConversationID), durableEventRecord{Seq: *envelope.Seq, Event: envelope})
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
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return runtimeState{}, errors.New("missing or invalid durable runtime state")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return runtimeState{}, err
	}
	var state runtimeState
	if err := unmarshalStrict(raw, &state); err != nil {
		return runtimeState{}, fmt.Errorf("decode durable runtime state: %w", err)
	}
	return state, nil
}

func (g *DurableGateway) appendJSONLine(path string, value any) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.appendJSONLineLocked(path, value)
}

func (g *DurableGateway) appendJSONLineLocked(path string, value any) error {
	line, err := json.Marshal(value)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write(append(line, '\n')); err != nil {
		return err
	}
	return file.Sync()
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
