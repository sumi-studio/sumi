package store

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type TombstoneStatus string

const (
	Requested     TombstoneStatus = "requested"
	Fenced        TombstoneStatus = "fenced"
	LivePurged    TombstoneStatus = "live_purged"
	BackupExpired TombstoneStatus = "backup_expired"
)

type TombstoneScope string

const (
	ConversationScope TombstoneScope = "conversation"
	AgentScope        TombstoneScope = "agent"
)

type Tombstone struct {
	ID                        string          `json:"id"`
	TenantID                  string          `json:"tenant_id"`
	AgentID                   string          `json:"agent_id"`
	ConversationID            string          `json:"conversation_id,omitempty"`
	ReplacementConversationID string          `json:"replacement_conversation_id,omitempty"`
	CommandID                 string          `json:"command_id,omitempty"`
	CommandSeq                *int64          `json:"command_seq,omitempty"`
	Scope                     TombstoneScope  `json:"scope"`
	Status                    TombstoneStatus `json:"status"`
	FencedGeneration          *int64          `json:"fenced_generation,omitempty"`
	GenerationLeaseID         string          `json:"generation_lease_id,omitempty"`
	GenerationFenceID         string          `json:"generation_fence_id,omitempty"`
	RequestedAt               time.Time       `json:"requested_at"`
	PurgeAfter                time.Time       `json:"purge_after"`
}

type persistedTombstones struct {
	Version    int                   `json:"version"`
	Tombstones map[string]*Tombstone `json:"tombstones"`
}

// TombstoneStore is the control-plane authority. Its file must be mounted
// outside every deletion-target agent volume; writes use fsync + atomic rename
// so process restart cannot turn an acknowledged CAS into memory-only state.
type TombstoneStore struct {
	mu         sync.Mutex
	path       string
	tombstones map[string]*Tombstone
}

func OpenTombstoneStore(path string) (*TombstoneStore, error) {
	if path == "" || !filepath.IsAbs(path) {
		return nil, errors.New("control-plane tombstone path must be absolute")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create control-plane state directory: %w", err)
	}
	s := &TombstoneStore{path: path, tombstones: make(map[string]*Tombstone)}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := s.persistLocked(); err != nil {
			return nil, err
		}
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read tombstone store: %w", err)
	}
	var persisted persistedTombstones
	if err := json.Unmarshal(data, &persisted); err != nil {
		return nil, fmt.Errorf("decode tombstone store: %w", err)
	}
	if persisted.Version != 1 || persisted.Tombstones == nil {
		return nil, errors.New("unsupported or incomplete tombstone store")
	}
	for id, t := range persisted.Tombstones {
		if t == nil || t.ID != id {
			return nil, errors.New("invalid tombstone identity in persistent store")
		}
		if err := validateTombstone(t); err != nil {
			return nil, fmt.Errorf("invalid persisted tombstone %s: %w", id, err)
		}
	}
	s.tombstones = persisted.Tombstones
	return s, nil
}

func (s *TombstoneStore) Create(
	tenantID, agentID, conversationID, replacementConversationID, commandID string,
	commandSeq *int64,
	scope TombstoneScope,
	purgeAfter time.Time,
) (*Tombstone, error) {
	t := &Tombstone{
		TenantID:                  tenantID,
		AgentID:                   agentID,
		ConversationID:            conversationID,
		ReplacementConversationID: replacementConversationID,
		CommandID:                 commandID,
		CommandSeq:                commandSeq,
		Scope:                     scope,
		Status:                    Requested,
		RequestedAt:               time.Now().UTC(),
		PurgeAfter:                purgeAfter.UTC(),
	}
	if err := validateTombstone(t); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.tombstones {
		sameCommand := commandID != "" && existing.TenantID == tenantID &&
			existing.AgentID == agentID && existing.CommandID == commandID
		sameConversation := scope == ConversationScope &&
			existing.TenantID == tenantID && existing.AgentID == agentID &&
			existing.Scope == ConversationScope && existing.ConversationID == conversationID
		if !sameCommand && !sameConversation {
			continue
		}
		if sameResetIdentity(existing, t) {
			return cloneTombstone(existing), nil
		}
		return nil, errors.New("conflicting conversation reset tombstone already exists")
	}
	id, err := newTombstoneID()
	if err != nil {
		return nil, err
	}
	t.ID = id
	s.tombstones[id] = t
	if err := s.persistLocked(); err != nil {
		delete(s.tombstones, id)
		return nil, err
	}
	return cloneTombstone(t), nil
}

func (s *TombstoneStore) RecordFence(
	id string,
	generation int64,
	leaseID, fenceID string,
) (*Tombstone, error) {
	if generation < 0 || leaseID == "" || fenceID == "" {
		return nil, errors.New("nonnegative generation and nonempty lease/fence ids are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tombstones[id]
	if !ok {
		return nil, fmt.Errorf("tombstone %s not found", id)
	}
	if t.FencedGeneration != nil {
		if *t.FencedGeneration == generation &&
			t.GenerationLeaseID == leaseID && t.GenerationFenceID == fenceID {
			return cloneTombstone(t), nil
		}
		return nil, fmt.Errorf("conflicting generation fence replay for tombstone %s", id)
	}
	if t.Status != Requested {
		return nil, fmt.Errorf("cannot attach generation fence in status %s", t.Status)
	}
	t.FencedGeneration = int64Pointer(generation)
	t.GenerationLeaseID = leaseID
	t.GenerationFenceID = fenceID
	if err := s.persistLocked(); err != nil {
		t.FencedGeneration = nil
		t.GenerationLeaseID = ""
		t.GenerationFenceID = ""
		return nil, err
	}
	return cloneTombstone(t), nil
}

func (s *TombstoneStore) Advance(id string, from, to TombstoneStatus) (*Tombstone, error) {
	if err := validateTransition(from, to); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tombstones[id]
	if !ok {
		return nil, fmt.Errorf("tombstone %s not found", id)
	}
	if t.Status == to {
		return cloneTombstone(t), nil
	}
	if t.Status != from {
		return nil, fmt.Errorf("tombstone %s status is %s, expected %s", id, t.Status, from)
	}
	if from == Requested && to == Fenced &&
		(t.FencedGeneration == nil || t.GenerationLeaseID == "" || t.GenerationFenceID == "") {
		return nil, errors.New("cannot mark tombstone fenced without generation proof")
	}
	previous := t.Status
	t.Status = to
	if err := s.persistLocked(); err != nil {
		t.Status = previous
		return nil, err
	}
	return cloneTombstone(t), nil
}

func (s *TombstoneStore) Get(id string) (*Tombstone, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tombstones[id]
	if !ok {
		return nil, fmt.Errorf("tombstone %s not found", id)
	}
	return cloneTombstone(t), nil
}

func (s *TombstoneStore) ListForAgent(tenantID, agentID string) []*Tombstone {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Tombstone, 0)
	for _, t := range s.tombstones {
		if t.TenantID == tenantID && t.AgentID == agentID {
			out = append(out, cloneTombstone(t))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RequestedAt.Equal(out[j].RequestedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].RequestedAt.Before(out[j].RequestedAt)
	})
	return out
}

func (s *TombstoneStore) FindByCommand(tenantID, agentID, commandID string) (*Tombstone, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.tombstones {
		if t.TenantID == tenantID && t.AgentID == agentID && t.CommandID == commandID {
			return cloneTombstone(t), nil
		}
	}
	return nil, fmt.Errorf("tombstone command %q not found", commandID)
}

func (s *TombstoneStore) persistLocked() error {
	payload, err := json.Marshal(persistedTombstones{Version: 1, Tombstones: s.tombstones})
	if err != nil {
		return fmt.Errorf("encode tombstone store: %w", err)
	}
	dir := filepath.Dir(s.path)
	temp, err := os.CreateTemp(dir, ".tombstones-*.tmp")
	if err != nil {
		return fmt.Errorf("create tombstone temp file: %w", err)
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(payload); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempName, s.path); err != nil {
		return fmt.Errorf("publish tombstone store: %w", err)
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func validateTombstone(t *Tombstone) error {
	if t.TenantID == "" || t.AgentID == "" {
		return errors.New("tenant_id and agent_id are required")
	}
	switch t.Scope {
	case ConversationScope:
		if t.ConversationID == "" || t.ReplacementConversationID == "" ||
			t.ConversationID == t.ReplacementConversationID ||
			t.CommandID == "" || t.CommandSeq == nil || *t.CommandSeq < 0 {
			return errors.New("conversation scope requires exact old/new conversation and command identity")
		}
	case AgentScope:
		if t.ConversationID != "" || t.ReplacementConversationID != "" ||
			t.CommandID != "" || t.CommandSeq != nil {
			return errors.New("agent scope must not contain conversation reset identity")
		}
	default:
		return fmt.Errorf("unknown tombstone scope %q", t.Scope)
	}
	if !validStatus(t.Status) {
		return fmt.Errorf("unknown tombstone status %q", t.Status)
	}
	fenceAbsent := t.FencedGeneration == nil && t.GenerationLeaseID == "" && t.GenerationFenceID == ""
	fenceComplete := t.FencedGeneration != nil && *t.FencedGeneration >= 0 &&
		t.GenerationLeaseID != "" && t.GenerationFenceID != ""
	if !fenceAbsent && !fenceComplete {
		return errors.New("generation fence identity must be wholly absent or present")
	}
	if t.Status != Requested && !fenceComplete {
		return errors.New("post-requested tombstone is missing generation fence proof")
	}
	return nil
}

func validateTransition(from, to TombstoneStatus) error {
	if !validStatus(from) || !validStatus(to) {
		return errors.New("unknown tombstone transition status")
	}
	if from == to {
		return nil
	}
	allowed := map[TombstoneStatus]TombstoneStatus{
		Requested:  Fenced,
		Fenced:     LivePurged,
		LivePurged: BackupExpired,
	}
	if allowed[from] != to {
		return fmt.Errorf("invalid transition %s -> %s", from, to)
	}
	return nil
}

func validStatus(status TombstoneStatus) bool {
	return status == Requested || status == Fenced ||
		status == LivePurged || status == BackupExpired
}

func sameResetIdentity(left, right *Tombstone) bool {
	if left.TenantID != right.TenantID || left.AgentID != right.AgentID ||
		left.ConversationID != right.ConversationID ||
		left.ReplacementConversationID != right.ReplacementConversationID ||
		left.CommandID != right.CommandID || left.Scope != right.Scope {
		return false
	}
	if left.CommandSeq == nil || right.CommandSeq == nil {
		return left.CommandSeq == nil && right.CommandSeq == nil
	}
	return *left.CommandSeq == *right.CommandSeq
}

func cloneTombstone(t *Tombstone) *Tombstone {
	cloned := *t
	if t.CommandSeq != nil {
		cloned.CommandSeq = int64Pointer(*t.CommandSeq)
	}
	if t.FencedGeneration != nil {
		cloned.FencedGeneration = int64Pointer(*t.FencedGeneration)
	}
	return &cloned
}

func int64Pointer(value int64) *int64 {
	return &value
}

func newTombstoneID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate tombstone id: %w", err)
	}
	return "tmb-" + hex.EncodeToString(bytes[:]), nil
}
