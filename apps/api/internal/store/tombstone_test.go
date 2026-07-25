package store

import (
	"testing"
	"time"
)

func TestTombstoneStateMachine(t *testing.T) {
	s := NewTombstoneStore()
	tmb, err := s.Create("tenant-1", "agent-1", "conv-1", ConversationScope, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("create tombstone: %v", err)
	}
	if tmb.Status != Requested {
		t.Fatalf("expected requested, got %s", tmb.Status)
	}

	if _, err := s.Advance(tmb.ID, Requested, Fenced); err != nil {
		t.Fatalf("advance requested->fenced: %v", err)
	}

	if _, err := s.Advance(tmb.ID, Fenced, BackupExpired); err == nil {
		t.Fatalf("expected skip error")
	}

	if _, err := s.Advance(tmb.ID, Fenced, LivePurged); err != nil {
		t.Fatalf("advance fenced->live_purged: %v", err)
	}
}

func TestAgentScopeRequiresNoConversation(t *testing.T) {
	s := NewTombstoneStore()
	if _, err := s.Create("tenant-1", "agent-1", "", AgentScope, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("agent scope without conversation: %v", err)
	}
	if _, err := s.Create("tenant-1", "agent-1", "conv-1", AgentScope, time.Now().Add(time.Hour)); err == nil {
		t.Fatalf("expected error for agent scope with conversation_id")
	}
}

func TestTombstoneReverseTransitionIsRejected(t *testing.T) {
	s := NewTombstoneStore()
	tmb, err := s.Create("tenant-1", "agent-1", "conv-1", ConversationScope, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("create tombstone: %v", err)
	}
	if _, err := s.Advance(tmb.ID, Requested, Fenced); err != nil {
		t.Fatalf("advance requested->fenced: %v", err)
	}
	if _, err := s.Advance(tmb.ID, Fenced, Requested); err == nil {
		t.Fatalf("expected reverse transition error")
	}
}

func TestTombstoneListForAgentPreservesOtherConversations(t *testing.T) {
	s := NewTombstoneStore()
	if _, err := s.Create("tenant-1", "agent-1", "conv-1", ConversationScope, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("create conv-1 tombstone: %v", err)
	}
	if _, err := s.Create("tenant-1", "agent-1", "conv-2", ConversationScope, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("create conv-2 tombstone: %v", err)
	}
	if _, err := s.Create("tenant-2", "agent-1", "conv-3", ConversationScope, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("create tenant-2 tombstone: %v", err)
	}

	agentTombstones := s.ListForAgent("tenant-1", "agent-1")
	if len(agentTombstones) != 2 {
		t.Fatalf("expected 2 tombstones for tenant-1/agent-1, got %d", len(agentTombstones))
	}
}

func TestTombstoneAdvanceRequiresMatchingFromState(t *testing.T) {
	s := NewTombstoneStore()
	tmb, err := s.Create("tenant-1", "agent-1", "conv-1", ConversationScope, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("create tombstone: %v", err)
	}
	if _, err := s.Advance(tmb.ID, Fenced, LivePurged); err == nil {
		t.Fatalf("expected advance from wrong state to fail")
	}
}
