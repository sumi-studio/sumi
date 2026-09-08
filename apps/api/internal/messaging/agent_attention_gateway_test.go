package messaging

import (
	"context"
	"encoding/json"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
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

func TestAgentAttentionPersistsActualGatewayUUIDv4Receipt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	msg := w.send(t, ctx, ch.PlaceID, w.humanB, "@Kuro 本番のコマンドIDを確認")
	var payload []byte
	if err := w.store.pool.QueryRow(ctx, "SELECT payload FROM agent_attention_deliveries WHERE message_id=$1", msg.MessageID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var event AgentAttentionEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		t.Fatal(err)
	}
	adapter := &AgentAttentionGateway{TenantID: "runtime-tenant"}
	provenance, command, err := adapter.input(event)
	if err != nil {
		t.Fatal(err)
	}
	commands, err := agentevents.OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer commands.Close()
	key := "attention:" + event.PersonalityAgentID + ":" + event.EventID
	admitted, err := commands.Append(ctx, provenance, key, command)
	if err != nil {
		t.Fatal(err)
	}
	if admitted.CommandID[14] != '4' || event.EventID[14] != '7' {
		t.Fatal("fixture no longer covers independent UUIDv4 command and UUIDv7 source identities")
	}
	tx, err := w.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := commitAttentionReceipt(ctx, tx, event.EventID, AgentAttentionReceipt{CommandID: admitted.CommandID, Seq: admitted.Seq}); err != nil {
		t.Fatal(err)
	}
	var stored string
	var seq int64
	if err := w.store.pool.QueryRow(ctx, "SELECT admitted_command_id::text,admitted_command_seq FROM agent_attention_deliveries WHERE event_id=$1", event.EventID).Scan(&stored, &seq); err != nil || stored != admitted.CommandID || uint64(seq) != admitted.Seq {
		t.Fatalf("receipt not preserved: %v", err)
	}
	repeated, found, err := commands.Lookup(ctx, provenance, key, command)
	if err != nil || !found || repeated.CommandID != stored {
		t.Fatal("durable admission cannot reconcile after database ack")
	}
	// Only receipt identity changes type; Messaging event IDs remain UUIDv7.
	var domain string
	if err := w.store.pool.QueryRow(ctx, "SELECT domain_name FROM information_schema.columns WHERE table_name='agent_attention_deliveries' AND column_name='event_id'").Scan(&domain); err != nil || domain != "uuidv7" {
		t.Fatal("Messaging source identity constraint changed")
	}
}
