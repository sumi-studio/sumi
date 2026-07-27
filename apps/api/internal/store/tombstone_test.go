package store

import (
	"path/filepath"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *TombstoneStore {
	t.Helper()
	store, err := OpenTombstoneStore(filepath.Join(t.TempDir(), "control-plane", "tombstones.json"))
	if err != nil {
		t.Fatalf("open tombstone store: %v", err)
	}
	return store
}

func resetRequest(t *testing.T, s *TombstoneStore, commandID string) *Tombstone {
	t.Helper()
	seq := int64(1)
	tmb, err := s.Create(
		"tenant-1", "agent-1", "conv-1", "conv-2", commandID, &seq,
		ConversationScope, time.Now().Add(time.Hour),
	)
	if err != nil {
		t.Fatalf("create tombstone: %v", err)
	}
	return tmb
}

func TestTombstoneStateMachineRequiresFenceAndRejectsSkip(t *testing.T) {
	s := openTestStore(t)
	tmb := resetRequest(t, s, "command-1")
	if _, err := s.Advance(tmb.ID, Requested, Fenced); err == nil {
		t.Fatal("expected missing generation fence to reject requested->fenced")
	}
	if _, err := s.RecordFence(tmb.ID, 7, "lease-7", "fence-7"); err != nil {
		t.Fatalf("record generation fence: %v", err)
	}
	if _, err := s.Advance(tmb.ID, Requested, Fenced); err != nil {
		t.Fatalf("advance requested->fenced: %v", err)
	}
	if _, err := s.Advance(tmb.ID, Requested, Fenced); err != nil {
		t.Fatalf("idempotent CAS replay: %v", err)
	}
	if _, err := s.Advance(tmb.ID, Fenced, BackupExpired); err == nil {
		t.Fatal("expected skipped transition error")
	}
	if _, err := s.Advance(tmb.ID, Fenced, LivePurged); err != nil {
		t.Fatalf("advance fenced->live_purged: %v", err)
	}
	if _, err := s.Advance(tmb.ID, LivePurged, Fenced); err == nil {
		t.Fatal("expected reverse transition error")
	}
}

func TestTombstoneRejectsUnknownScopeStatusAndIdentityMismatch(t *testing.T) {
	s := openTestStore(t)
	seq := int64(1)
	for name, scope := range map[string]TombstoneScope{
		"empty":   "",
		"unknown": "workspace",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := s.Create(
				"tenant-1", "agent-1", "conv-1", "conv-2", "command-"+name, &seq,
				scope, time.Now().Add(time.Hour),
			); err == nil {
				t.Fatal("expected unknown scope rejection")
			}
		})
	}
	if _, err := s.Create(
		"tenant-1", "agent-1", "conv-1", "conv-1", "command-same", &seq,
		ConversationScope, time.Now().Add(time.Hour),
	); err == nil {
		t.Fatal("expected identical old/new conversation rejection")
	}
	if _, err := s.Advance("missing", TombstoneStatus("unknown"), Requested); err == nil {
		t.Fatal("expected unknown status rejection")
	}
}

func TestPersistentAuthoritySurvivesReopenAndCASReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outside-agent-volume", "tombstones.json")
	first, err := OpenTombstoneStore(path)
	if err != nil {
		t.Fatal(err)
	}
	tmb := resetRequest(t, first, "command-persistent")
	if _, err := first.RecordFence(tmb.ID, 9, "lease-9", "fence-9"); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Advance(tmb.ID, Requested, Fenced); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenTombstoneStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, err := reopened.Get(tmb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != Fenced || got.FencedGeneration == nil || *got.FencedGeneration != 9 {
		t.Fatalf("persistent fence receipt lost: %+v", got)
	}
	if _, err := reopened.RecordFence(tmb.ID, 9, "lease-9", "fence-9"); err != nil {
		t.Fatalf("exact fence replay must be idempotent: %v", err)
	}
	if _, err := reopened.RecordFence(tmb.ID, 9, "lease-other", "fence-9"); err == nil {
		t.Fatal("expected conflicting fence replay rejection")
	}
}

func TestCreateIsIdempotentOnlyForExactResetIdentity(t *testing.T) {
	s := openTestStore(t)
	first := resetRequest(t, s, "command-replay")
	second := resetRequest(t, s, "command-replay")
	if first.ID != second.ID {
		t.Fatalf("exact replay minted two tombstones: %s != %s", first.ID, second.ID)
	}
	seq := int64(2)
	if _, err := s.Create(
		"tenant-1", "agent-1", "conv-1", "conv-other", "command-other", &seq,
		ConversationScope, time.Now().Add(time.Hour),
	); err == nil {
		t.Fatal("expected conflicting reset for same old conversation to fail")
	}
}

func TestListForAgentDoesNotCrossTenantBoundary(t *testing.T) {
	s := openTestStore(t)
	resetRequest(t, s, "command-1")
	seq := int64(1)
	if _, err := s.Create(
		"tenant-2", "agent-1", "conv-1", "conv-2", "command-2", &seq,
		ConversationScope, time.Now().Add(time.Hour),
	); err != nil {
		t.Fatal(err)
	}
	if got := len(s.ListForAgent("tenant-1", "agent-1")); got != 1 {
		t.Fatalf("expected one isolated tenant tombstone, got %d", got)
	}
}
