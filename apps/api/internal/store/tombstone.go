package store

import (
	"fmt"
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
	ID             string          `json:"id"`
	TenantID       string          `json:"tenant_id"`
	AgentID        string          `json:"agent_id"`
	ConversationID string          `json:"conversation_id,omitempty"`
	Scope          TombstoneScope  `json:"scope"`
	Status         TombstoneStatus `json:"status"`
	RequestedAt    time.Time       `json:"requested_at"`
	PurgeAfter     time.Time       `json:"purge_after"`
}

type TombstoneStore struct {
	mu         sync.Mutex
	tombstones map[string]*Tombstone
}

func NewTombstoneStore() *TombstoneStore {
	return &TombstoneStore{tombstones: make(map[string]*Tombstone)}
}

func (s *TombstoneStore) Create(tenantID, agentID, conversationID string, scope TombstoneScope, purgeAfter time.Time) (*Tombstone, error) {
	if tenantID == "" || agentID == "" {
		return nil, fmt.Errorf("tenant_id and agent_id are required")
	}
	if scope == ConversationScope && conversationID == "" {
		return nil, fmt.Errorf("conversation_id is required for conversation scope")
	}
	if scope == AgentScope && conversationID != "" {
		return nil, fmt.Errorf("conversation_id must be empty for agent scope")
	}
	t := &Tombstone{
		ID:             fmt.Sprintf("tmb-%d", time.Now().UnixNano()),
		TenantID:       tenantID,
		AgentID:        agentID,
		ConversationID: conversationID,
		Scope:          scope,
		Status:         Requested,
		RequestedAt:    time.Now().UTC(),
		PurgeAfter:     purgeAfter,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tombstones[t.ID] = t
	return t, nil
}

func (s *TombstoneStore) Advance(id string, from, to TombstoneStatus) (*Tombstone, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tombstones[id]
	if !ok {
		return nil, fmt.Errorf("tombstone %s not found", id)
	}
	if t.Status != from {
		return nil, fmt.Errorf("tombstone %s status is %s, expected %s", id, t.Status, from)
	}
	allowed := map[TombstoneStatus]TombstoneStatus{
		Requested:     Fenced,
		Fenced:        LivePurged,
		LivePurged:    BackupExpired,
		BackupExpired: "",
	}
	if to == from {
		return t, nil
	}
	if allowed[t.Status] != to {
		return nil, fmt.Errorf("invalid transition %s -> %s", t.Status, to)
	}
	t.Status = to
	return t, nil
}

func (s *TombstoneStore) Get(id string) (*Tombstone, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tombstones[id]
	if !ok {
		return nil, fmt.Errorf("tombstone %s not found", id)
	}
	return t, nil
}

func (s *TombstoneStore) ListForAgent(tenantID, agentID string) []*Tombstone {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Tombstone
	for _, t := range s.tombstones {
		if t.TenantID == tenantID && t.AgentID == agentID {
			out = append(out, t)
		}
	}
	return out
}
