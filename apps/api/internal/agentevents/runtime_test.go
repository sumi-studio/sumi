package agentevents

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func openRuntimeGateway(t *testing.T) *DurableGateway {
	t.Helper()
	store, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gateway, err := OpenDurableGateway(t.TempDir(), store)
	if err != nil {
		t.Fatal(err)
	}
	return gateway
}

func TestDurableGatewayMissingStateWaitsUntilReceiptIsPublished(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const agentID = "agent-1"
	const generation = uint64(7)

	if err := gateway.VerifyGeneration(context.Background(), agentID, generation); err != nil {
		t.Fatalf("missing state must not reject the generation before publication: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- gateway.WaitFor(ctx, TokenClaims{AgentID: agentID}, generation)
	}()

	if err := os.WriteFile(
		gateway.statePath(agentID),
		[]byte(`{"generation":7,"hydration_receipt_identity":null}`),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		t.Fatalf("not-ready state released the latch: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	if err := os.WriteFile(
		gateway.statePath(agentID),
		[]byte(`{"generation":7,"hydration_receipt_identity":"receipt-7"}`),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("ready state did not release the latch: %v", err)
	}
}

func TestDurableGatewayRejectsLegacyReadyBoolean(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const agentID = "agent-1"
	if err := os.WriteFile(
		gateway.statePath(agentID),
		[]byte(`{"generation":7,"ready":true}`),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.state(context.Background(), agentID); err == nil {
		t.Fatal("legacy ready boolean must fail strict runtime-state decoding")
	}
}

func TestDurableGatewayRejectsEmptyReceiptIdentity(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const agentID = "agent-1"
	if err := os.WriteFile(
		gateway.statePath(agentID),
		[]byte(`{"generation":7,"hydration_receipt_identity":""}`),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.state(context.Background(), agentID); err == nil {
		t.Fatal("empty hydration receipt identity must fail closed")
	}
}

func TestDurableGatewaySerializesEventSequenceAcrossInstances(t *testing.T) {
	commandStore, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer commandStore.Close()
	runtimeDir := t.TempDir()
	first, err := OpenDurableGateway(runtimeDir, commandStore)
	if err != nil {
		t.Fatal(err)
	}
	second, err := OpenDurableGateway(runtimeDir, commandStore)
	if err != nil {
		t.Fatal(err)
	}
	claims := TokenClaims{ConversationID: "conversation-1"}
	seq := uint64(1)
	event := Envelope{
		Seq:            &seq,
		ConversationID: claims.ConversationID,
		Event:          json.RawMessage(`{"type":"agent_start"}`),
	}
	results := make(chan error, 2)
	go func() { results <- first.Receive(context.Background(), claims, event) }()
	go func() { results <- second.Receive(context.Background(), claims, event) }()
	var successes int
	for range 2 {
		if err := <-results; err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("exactly one cross-process seq=1 append must succeed, got %d", successes)
	}

	seq = 2
	event.Seq = &seq
	if err := second.Receive(context.Background(), claims, event); err != nil {
		t.Fatalf("seq=2 append after contention failed: %v", err)
	}
	last, err := first.LastReceivedEventSeq(context.Background(), claims)
	if err != nil {
		t.Fatal(err)
	}
	if last != 2 {
		t.Fatalf("expected last event seq 2, got %d", last)
	}
}

func TestDurableGatewayCorrelatesAndDeduplicatesCommandAcks(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := TokenClaims{ConversationID: "conversation-1"}
	command, err := gateway.commands.Append(
		context.Background(),
		claims.ConversationID,
		"",
		json.RawMessage(`{"type":"user_message","text":"hello","attachments":[]}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	wrong := CommandAck{
		Seq:       command.Seq,
		CommandID: "00000000-0000-4000-8000-000000000001",
		Status:    "received",
	}
	if err := gateway.ApplyAck(context.Background(), claims, wrong); err == nil {
		t.Fatal("ack with a mismatched command_id must be rejected")
	}

	received := CommandAck{Seq: command.Seq, CommandID: command.CommandID, Status: "received"}
	if err := gateway.ApplyAck(context.Background(), claims, received); err != nil {
		t.Fatal(err)
	}
	if err := gateway.ApplyAck(context.Background(), claims, received); err != nil {
		t.Fatalf("exact duplicate ack must be idempotent: %v", err)
	}
	applied := CommandAck{Seq: command.Seq, CommandID: command.CommandID, Status: "applied"}
	if err := gateway.ApplyAck(context.Background(), claims, applied); err != nil {
		t.Fatal(err)
	}
	reason := "schema_violation"
	rejected := CommandAck{
		Seq:          command.Seq,
		CommandID:    command.CommandID,
		Status:       "rejected",
		RejectReason: &reason,
	}
	if err := gateway.ApplyAck(context.Background(), claims, rejected); err == nil {
		t.Fatal("terminal applied ack must reject a conflicting terminal ack")
	}
	raw, err := os.ReadFile(gateway.ackPath(claims.ConversationID))
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(raw), "\n"); lines != 2 {
		t.Fatalf("expected received+applied records without duplicates, got %d lines", lines)
	}
}
