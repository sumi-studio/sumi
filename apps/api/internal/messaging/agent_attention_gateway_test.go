package messaging

import (
	"encoding/json"
	"testing"
	"time"
)

func TestAgentAttentionGatewayPreservesSourceAndReminderOrigin(t *testing.T) {
	const id = "018f47a2-9b3c-7def-8abc-0123456789ab"
	at := time.Date(2026, 9, 8, 21, 0, 0, 123, time.FixedZone("source", 9*3600))
	event := AgentAttentionEvent{EventID: id, Kind: AgentAttentionMention, PersonalityAgentID: id, WorkspaceID: id, InstallationID: id, AuthorityEpoch: 1,
		Actor: AgentAttentionActor{Kind: "human", ID: "actual-author", DisplayName: "Actual author"}, Place: AgentAttentionPlace{ID: id, Kind: "channel", Name: "Shared"},
		MessageID: id, MessageRevision: 1, MessageSeq: 1, OccurredAt: at, Content: "original\ntext"}
	adapter := &AgentAttentionGateway{TenantID: "runtime-tenant"}
	p, command, err := adapter.input(event)
	if err != nil {
		t.Fatal(err)
	}
	if p.Actor.PrincipalID != event.Actor.ID || p.TenantID != "runtime-tenant" || p.Source.OccurredAt != "2026-09-08T12:00:00.000000123Z" {
		t.Fatalf("source mapping: %+v", p)
	}
	var body struct{ Type, Content string }
	if err := json.Unmarshal(command, &body); err != nil {
		t.Fatal(err)
	}
	if body.Type != "external_event" || body.Content != event.Content {
		t.Fatal("source text changed")
	}
	due := at.Add(time.Hour)
	event.Kind = AgentAttentionReminder
	event.MarkerID = id
	event.DueAt = &due
	if _, _, err := adapter.input(event); err == nil {
		t.Fatal("Human reminder delegated as PA experience")
	}
	event.Actor = AgentAttentionActor{Kind: "personality_agent", ID: id}
	event.Content = "my reminder note"
	p, _, err = adapter.input(event)
	if err != nil {
		t.Fatal(err)
	}
	if p.Actor.PrincipalID != id || p.Source.DueAt != "2026-09-08T13:00:00.000000123Z" {
		t.Fatal("reminder origin or due time changed")
	}
	event.AuthorityEpoch = -1
	if _, _, err := adapter.input(event); err == nil {
		t.Fatal("negative epoch accepted")
	}
}
