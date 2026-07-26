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
	gateway.PollInterval = 5 * time.Millisecond
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

	if err := gateway.publishRuntimeState(agentID, runtimeState{
		Generation:               7,
		HydrationReceiptIdentity: nil,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		t.Fatalf("not-ready state released the latch: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	receipt := "receipt-7"
	if err := gateway.publishRuntimeState(agentID, runtimeState{
		Generation:               7,
		HydrationReceiptIdentity: &receipt,
	}); err != nil {
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
	empty := ""
	if err := gateway.publishRuntimeState(agentID, runtimeState{
		Generation:               7,
		HydrationReceiptIdentity: &empty,
	}); err != nil {
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
	first.PollInterval = 5 * time.Millisecond
	second.PollInterval = 5 * time.Millisecond
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

func failingOpener(ff *failingFile) func(string, int, os.FileMode) (durableFileHandle, error) {
	return func(name string, flag int, perm os.FileMode) (durableFileHandle, error) {
		f, err := os.OpenFile(name, flag, perm)
		if err != nil {
			return nil, err
		}
		ff.File = f
		return ff, nil
	}
}

func realOpener() func(string, int, os.FileMode) (durableFileHandle, error) {
	return func(name string, flag int, perm os.FileMode) (durableFileHandle, error) {
		return os.OpenFile(name, flag, perm)
	}
}

func TestDurableGatewayEventAppendRollsBackOnWriteFailure(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := TokenClaims{ConversationID: "conversation-1"}
	seq := uint64(1)
	event := Envelope{
		Seq:            &seq,
		ConversationID: claims.ConversationID,
		Event:          json.RawMessage(`{"type":"agent_start"}`),
	}

	ff := &failingFile{failWriteOn: 1}
	gateway.newFile = failingOpener(ff)
	if err := gateway.Receive(context.Background(), claims, event); err == nil {
		t.Fatal("expected write failure")
	}

	gateway.newFile = realOpener()
	last, err := gateway.LastReceivedEventSeq(context.Background(), claims)
	if err != nil {
		t.Fatal(err)
	}
	if last != 0 {
		t.Fatalf("expected empty log after rollback, got seq %d", last)
	}
	if err := gateway.Receive(context.Background(), claims, event); err != nil {
		t.Fatalf("retry after rollback failed: %v", err)
	}
	last, err = gateway.LastReceivedEventSeq(context.Background(), claims)
	if err != nil {
		t.Fatal(err)
	}
	if last != 1 {
		t.Fatalf("expected seq 1, got %d", last)
	}
}

func TestDurableGatewayEventAppendRollsBackOnSyncFailure(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := TokenClaims{ConversationID: "conversation-1"}
	seq := uint64(1)
	event := Envelope{
		Seq:            &seq,
		ConversationID: claims.ConversationID,
		Event:          json.RawMessage(`{"type":"agent_start"}`),
	}

	ff := &failingFile{failSyncOn: 1}
	gateway.newFile = failingOpener(ff)
	if err := gateway.Receive(context.Background(), claims, event); err == nil {
		t.Fatal("expected sync failure")
	}

	gateway.newFile = realOpener()
	if err := gateway.Receive(context.Background(), claims, event); err != nil {
		t.Fatalf("retry after rollback failed: %v", err)
	}
	last, err := gateway.LastReceivedEventSeq(context.Background(), claims)
	if err != nil {
		t.Fatal(err)
	}
	if last != 1 {
		t.Fatalf("expected seq 1, got %d", last)
	}
}

func TestDurableGatewayEventRecoversFromIncompleteFinalRecord(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := TokenClaims{ConversationID: "conversation-1"}
	seq := uint64(1)
	event := Envelope{
		Seq:            &seq,
		ConversationID: claims.ConversationID,
		Event:          json.RawMessage(`{"type":"agent_start"}`),
	}
	if err := gateway.Receive(context.Background(), claims, event); err != nil {
		t.Fatal(err)
	}
	path := gateway.eventPath(claims.ConversationID)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// Truncate into the final JSON record (not just the trailing newline) so
	// the next append must truncate the partial tail and recover.
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatal(err)
	}
	if err := gateway.Receive(context.Background(), claims, event); err != nil {
		t.Fatalf("recovery append failed: %v", err)
	}
	last, err := gateway.LastReceivedEventSeq(context.Background(), claims)
	if err != nil {
		t.Fatal(err)
	}
	if last != 1 {
		t.Fatalf("expected seq 1 after recovery, got %d", last)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(raw), "\n"); lines != 1 {
		t.Fatalf("expected one complete line, got %q", raw)
	}
}

func TestDurableGatewayAckAppendRollsBackOnWriteFailure(t *testing.T) {
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
	ack := CommandAck{Seq: command.Seq, CommandID: command.CommandID, Status: "received"}

	ff := &failingFile{failWriteOn: 1}
	gateway.newFile = failingOpener(ff)
	if err := gateway.ApplyAck(context.Background(), claims, ack); err == nil {
		t.Fatal("expected write failure")
	}

	gateway.newFile = realOpener()
	if err := gateway.ApplyAck(context.Background(), claims, ack); err != nil {
		t.Fatalf("retry after rollback failed: %v", err)
	}
	raw, err := os.ReadFile(gateway.ackPath(claims.ConversationID))
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(raw), "\n"); lines != 1 {
		t.Fatalf("expected one ack line, got %q", raw)
	}
}

func TestDurableGatewayAckAppendRollsBackOnSyncFailure(t *testing.T) {
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
	ack := CommandAck{Seq: command.Seq, CommandID: command.CommandID, Status: "received"}

	ff := &failingFile{failSyncOn: 1}
	gateway.newFile = failingOpener(ff)
	if err := gateway.ApplyAck(context.Background(), claims, ack); err == nil {
		t.Fatal("expected sync failure")
	}

	gateway.newFile = realOpener()
	if err := gateway.ApplyAck(context.Background(), claims, ack); err != nil {
		t.Fatalf("retry after rollback failed: %v", err)
	}
	raw, err := os.ReadFile(gateway.ackPath(claims.ConversationID))
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(raw), "\n"); lines != 1 {
		t.Fatalf("expected one ack line, got %q", raw)
	}
}

func TestDurableGatewayAckRecoversFromIncompleteFinalRecord(t *testing.T) {
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
	ack := CommandAck{Seq: command.Seq, CommandID: command.CommandID, Status: "received"}
	if err := gateway.ApplyAck(context.Background(), claims, ack); err != nil {
		t.Fatal(err)
	}
	path := gateway.ackPath(claims.ConversationID)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatal(err)
	}
	if err := gateway.ApplyAck(context.Background(), claims, ack); err != nil {
		t.Fatalf("recovery ack failed: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(raw), "\n"); lines != 1 {
		t.Fatalf("expected one complete ack line, got %q", raw)
	}
}
