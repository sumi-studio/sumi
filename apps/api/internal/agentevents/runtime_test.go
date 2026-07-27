package agentevents

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

func openGatewayAt(t *testing.T, storeDir, runtimeDir string) (*CommandStore, *DurableGateway, error) {
	t.Helper()
	store, err := OpenCommandStore(storeDir)
	if err != nil {
		return nil, nil, err
	}
	gateway, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		_ = store.Close()
		return nil, nil, err
	}
	gateway.PollInterval = 5 * time.Millisecond
	return store, gateway, nil
}

func TestDurableGatewayMissingStateFailsGenerationVerificationUntilPublished(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const agentID = "agent-1"
	const generation = uint64(7)

	if err := gateway.VerifyGeneration(context.Background(), agentID, generation); err == nil {
		t.Fatal("missing runtime state must fail generation verification")
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
	if err := gateway.VerifyGeneration(context.Background(), agentID, generation); err != nil {
		t.Fatalf("published matching runtime state rejected generation: %v", err)
	}
	if err := gateway.VerifyGeneration(context.Background(), agentID, generation+1); err == nil {
		t.Fatal("mismatched runtime generation must fail verification")
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

func TestDurableGatewayRuntimeStateGenerationBoundary(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const agentID = "agent-1"

	if err := gateway.publishRuntimeState(agentID, runtimeState{Generation: maxProcessGeneration}); err != nil {
		t.Fatalf("publish max process generation failed: %v", err)
	}
	st, err := gateway.state(context.Background(), agentID)
	if err != nil || st.Generation != maxProcessGeneration {
		t.Fatalf("max generation must be accepted in recovery: err=%v gen=%d", err, st.Generation)
	}

	if err := gateway.publishRuntimeState(agentID, runtimeState{Generation: maxProcessGeneration + 1}); err == nil {
		t.Fatal("publish must reject generation max+1")
	}

	raw, err := json.Marshal(runtimeState{Generation: maxProcessGeneration + 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(gateway.statePath(agentID), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.state(context.Background(), agentID); err == nil {
		t.Fatal("recovery must reject generation max+1")
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

func TestDurableGatewayReceiveRejectsConversationClaimMismatch(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := TokenClaims{ConversationID: "conversation-1"}
	seq := uint64(1)
	err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:            &seq,
		ConversationID: "conversation-2",
		Event:          json.RawMessage(`{"type":"agent_start"}`),
	})
	if err == nil || !strings.Contains(err.Error(), "does not match token claim") {
		t.Fatalf("expected conversation claim mismatch, got %v", err)
	}
	last, err := gateway.LastReceivedEventSeq(context.Background(), claims)
	if err != nil {
		t.Fatal(err)
	}
	if last != 0 {
		t.Fatalf("mismatched event must not be persisted, got seq %d", last)
	}
}

func TestDurableGatewayDetectsSameSizeEventReplacement(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := TokenClaims{ConversationID: "conversation-1"}
	seq := uint64(1)
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:            &seq,
		ConversationID: claims.ConversationID,
		Event:          json.RawMessage(`{"type":"agent_start"}`),
	}); err != nil {
		t.Fatal(err)
	}

	path := gateway.eventPath(claims.ConversationID)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	replaced := bytes.Replace(original, []byte("agent_start"), []byte("agent_stXrt"), 1)
	if len(replaced) != len(original) || bytes.Equal(replaced, original) {
		t.Fatal("test replacement must change content without changing file size")
	}
	if err := os.WriteFile(path, replaced, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.LastReceivedEventSeq(context.Background(), claims); err == nil {
		t.Fatal("same-size corrupt event replacement must invalidate the cached tail")
	}
}

func TestDurableGatewayDetectsSameSizeAckReplacement(t *testing.T) {
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
	received := CommandAck{Seq: command.Seq, CommandID: command.CommandID, Status: "received"}
	if err := gateway.ApplyAck(context.Background(), claims, received); err != nil {
		t.Fatal(err)
	}

	path := gateway.ackPath(claims.ConversationID)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	replaced := bytes.Replace(original, []byte("received"), []byte("receivXd"), 1)
	if len(replaced) != len(original) || bytes.Equal(replaced, original) {
		t.Fatal("test replacement must change content without changing file size")
	}
	if err := os.WriteFile(path, replaced, 0o600); err != nil {
		t.Fatal(err)
	}
	applied := CommandAck{Seq: command.Seq, CommandID: command.CommandID, Status: "applied"}
	if err := gateway.ApplyAck(context.Background(), claims, applied); err == nil {
		t.Fatal("same-size corrupt ack replacement must invalidate the cached tail")
	}
}

func TestDurableGatewayEvictsInactiveTailsAndReloadsDurableState(t *testing.T) {
	gateway := openRuntimeGateway(t)
	gateway.MaxConversationTails = 2
	gateway.MaxAckTail = 1

	for _, conversationID := range []string{"conversation-1", "conversation-2", "conversation-3"} {
		claims := TokenClaims{ConversationID: conversationID}
		seq := uint64(1)
		if err := gateway.Receive(context.Background(), claims, Envelope{
			Seq:            &seq,
			ConversationID: conversationID,
			Event:          json.RawMessage(`{"type":"agent_start"}`),
		}); err != nil {
			t.Fatal(err)
		}
	}
	gateway.mu.Lock()
	if len(gateway.tails) > gateway.MaxConversationTails {
		gateway.mu.Unlock()
		t.Fatalf("retained %d conversation tails, limit is %d", len(gateway.tails), gateway.MaxConversationTails)
	}
	gateway.mu.Unlock()

	claims := TokenClaims{ConversationID: "conversation-1"}
	last, err := gateway.LastReceivedEventSeq(context.Background(), claims)
	if err != nil || last != 1 {
		t.Fatalf("evicted event tail did not reload: last=%d err=%v", last, err)
	}
	seq := uint64(2)
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:            &seq,
		ConversationID: claims.ConversationID,
		Event:          json.RawMessage(`{"type":"agent_end"}`),
	}); err != nil {
		t.Fatalf("event append after reload: %v", err)
	}

	for i := 0; i < 2; i++ {
		if _, err := gateway.commands.Append(context.Background(), claims.ConversationID, "", json.RawMessage(`{"type":"user_message","text":"hello","attachments":[]}`)); err != nil {
			t.Fatal(err)
		}
	}
	commands, err := gateway.commands.CatchUp(context.Background(), claims.ConversationID, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, command := range commands {
		if err := gateway.ApplyAck(context.Background(), claims, CommandAck{Seq: command.Seq, CommandID: command.CommandID, Status: "received"}); err != nil {
			t.Fatal(err)
		}
	}
	// seq=1 is outside the one-entry cache, but its durable received ACK must
	// still allow the one legal terminal transition.
	first := commands[0]
	if err := gateway.ApplyAck(context.Background(), claims, CommandAck{Seq: first.Seq, CommandID: first.CommandID, Status: "applied"}); err != nil {
		t.Fatalf("evicted ACK did not reload for terminal transition: %v", err)
	}
	gateway.mu.Lock()
	ackEntries := len(gateway.stateFor(claims.ConversationID).acks)
	gateway.mu.Unlock()
	if ackEntries > gateway.MaxAckTail {
		t.Fatalf("retained %d ACK cache entries, limit is %d", ackEntries, gateway.MaxAckTail)
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
	last, err := gateway.LastReceivedEventSeq(context.Background(), claims)
	if err != nil {
		t.Fatal(err)
	}
	if last != 0 {
		t.Fatalf("expected empty log after sync rollback, got seq %d", last)
	}

	gateway.newFile = realOpener()
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

func TestDurableGatewayEventLogRejectsCorruptButCompleteRecords(t *testing.T) {
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

	corrupt := []string{
		`{"seq":2,"event":{"seq":2,"conversation_id":"conversation-1","event":{"type":"agent_end"}},"seq":2}\n`,
		`{"seq":2,"event":{"seq":2,"conversation_id":"conversation-1","event":{"type":"agent_end"}},"extra":true}\n`,
		`{"seq":9007199254740992,"event":{"seq":9007199254740992,"conversation_id":"conversation-1","event":{"type":"agent_end"}}}\n`,
	}
	for _, line := range corrupt {
		if err := os.WriteFile(gateway.eventPath(claims.ConversationID), append([]byte(nil), []byte(line)...), 0o600); err != nil {
			t.Fatal(err)
		}
		seq = 2
		event.Seq = &seq
		event.Event = json.RawMessage(`{"type":"agent_end"}`)
		if err := gateway.Receive(context.Background(), claims, event); err == nil {
			t.Fatalf("corrupt event log line must be rejected: %s", line)
		}
	}
}

func TestDurableGatewayAckLogRejectsCorruptButCompleteRecords(t *testing.T) {
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

	corrupt := []string{
		fmt.Sprintf(`{"seq":%d,"command_id":"%s","status":"applied","command_id":"%s"}`+"\n", command.Seq, command.CommandID, command.CommandID),
		fmt.Sprintf(`{"seq":%d,"command_id":"%s","status":"applied","extra":true}`+"\n", command.Seq, command.CommandID),
		`{"seq":9007199254740992,"command_id":"00000000-0000-4000-8000-000000000001","status":"applied"}` + "\n",
	}
	for _, line := range corrupt {
		if err := os.WriteFile(gateway.ackPath(claims.ConversationID), []byte(line), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := gateway.ApplyAck(context.Background(), claims, CommandAck{Seq: command.Seq, CommandID: command.CommandID, Status: "applied"}); err == nil {
			t.Fatalf("corrupt ack log line must be rejected: %s", line)
		}
	}
}

func TestDurableGatewayAckLogRejectsCorruptRecordOnFindAckLookup(t *testing.T) {
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

	// Evict the tail so findAckLocked must read the durable log.
	gateway.mu.Lock()
	gateway.tails = make(map[string]*conversationLogState)
	gateway.mu.Unlock()

	corrupt := fmt.Sprintf(`{"seq":%d,"command_id":"%s","status":"received","status":"received"}`+"\n", command.Seq, command.CommandID)
	if err := os.WriteFile(gateway.ackPath(claims.ConversationID), []byte(corrupt), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := gateway.ApplyAck(context.Background(), claims, CommandAck{Seq: command.Seq, CommandID: command.CommandID, Status: "applied"}); err == nil {
		t.Fatal("findAckLocked must reject corrupt ack log")
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

func TestDurableGatewayLogsRejectSymlinkTargets(t *testing.T) {
	tests := []struct {
		name string
		path func(*DurableGateway, string) string
		run  func(*testing.T, *DurableGateway, TokenClaims) error
	}{
		{
			name: "event",
			path: func(g *DurableGateway, conversationID string) string {
				return g.eventPath(conversationID)
			},
			run: func(t *testing.T, g *DurableGateway, claims TokenClaims) error {
				t.Helper()
				seq := uint64(1)
				return g.Receive(context.Background(), claims, Envelope{
					Seq:            &seq,
					ConversationID: claims.ConversationID,
					Event:          json.RawMessage(`{"type":"agent_start"}`),
				})
			},
		},
		{
			name: "ack",
			path: func(g *DurableGateway, conversationID string) string {
				return g.ackPath(conversationID)
			},
			run: func(t *testing.T, g *DurableGateway, claims TokenClaims) error {
				t.Helper()
				command, err := g.commands.Append(
					context.Background(),
					claims.ConversationID,
					"",
					json.RawMessage(`{"type":"abort"}`),
				)
				if err != nil {
					t.Fatal(err)
				}
				return g.ApplyAck(context.Background(), claims, CommandAck{
					Seq:       command.Seq,
					CommandID: command.CommandID,
					Status:    "received",
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gateway := openRuntimeGateway(t)
			claims := TokenClaims{ConversationID: "conversation-1"}
			target := filepath.Join(t.TempDir(), "redirected.log")
			const original = "sentinel"
			if err := os.WriteFile(target, []byte(original), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, test.path(gateway, claims.ConversationID)); err != nil {
				t.Fatal(err)
			}
			if err := test.run(t, gateway, claims); err == nil {
				t.Fatal("expected symlink-backed durable log to be rejected")
			}
			raw, err := os.ReadFile(target)
			if err != nil {
				t.Fatal(err)
			}
			if string(raw) != original {
				t.Fatalf("symlink target changed: %q", raw)
			}
		})
	}
}

func TestDurableGatewayLiveDoesNotLoseConcurrentAppend(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := TokenClaims{ConversationID: "conversation-1"}
	raw := json.RawMessage(`{"type":"abort"}`)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	live, errs, err := gateway.Live(ctx, claims, 1)
	if err != nil {
		t.Fatalf("live: %v", err)
	}

	// Append a command after Live has returned its channels. Because Live starts
	// polling from fromSeq, the next tick must deliver it rather than advance
	// past it.
	env, err := gateway.commands.Append(ctx, claims.ConversationID, "key-1", raw)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	select {
	case cmd := <-live:
		if cmd.Seq != env.Seq {
			t.Fatalf("expected seq %d, got %d", env.Seq, cmd.Seq)
		}
	case err := <-errs:
		t.Fatalf("live error: %v", err)
	case <-ctx.Done():
		t.Fatal("timeout waiting for live command")
	}
}

func TestDurableGatewayEventCatchUpRejectsCorruptRecords(t *testing.T) {
	conversationID := "conversation-1"

	cases := []struct {
		name     string
		contents []byte
		wantErr  string
	}{
		{
			name:     "outer/inner seq mismatch",
			contents: []byte(`{"seq":1,"event":{"seq":2,"conversation_id":"conversation-1","event":{"type":"agent_start"}}}` + "\n"),
			wantErr:  "seq mismatch",
		},
		{
			name:     "conversation mismatch",
			contents: []byte(`{"seq":1,"event":{"seq":1,"conversation_id":"conversation-2","event":{"type":"agent_start"}}}` + "\n"),
			wantErr:  "conversation mismatch",
		},
		{
			name:     "volatile event with seq",
			contents: []byte(`{"seq":1,"event":{"seq":1,"conversation_id":"conversation-1","event":{"type":"message_update"}}}` + "\n"),
			wantErr:  "volatile event",
		},
		{
			name:     "durable event missing inner seq",
			contents: []byte(`{"seq":1,"event":{"conversation_id":"conversation-1","event":{"type":"agent_start"}}}` + "\n"),
			wantErr:  "requires seq",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := openRuntimeGateway(t)
			if err := os.WriteFile(g.eventPath(conversationID), tc.contents, 0o600); err != nil {
				t.Fatalf("write corrupt log: %v", err)
			}
			_, err := g.EventCatchUp(context.Background(), conversationID, 0)
			if err == nil {
				t.Fatal("expected corrupt event log to be rejected")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestDurableGatewayEventCatchUpAcceptsValidRecords(t *testing.T) {
	gateway := openRuntimeGateway(t)
	conversationID := "conversation-1"
	claims := TokenClaims{ConversationID: conversationID}

	seq := uint64(1)
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:            &seq,
		ConversationID: conversationID,
		Event:          json.RawMessage(`{"type":"agent_start"}`),
	}); err != nil {
		t.Fatalf("receive event: %v", err)
	}

	caught, err := gateway.EventCatchUp(context.Background(), conversationID, 0)
	if err != nil {
		t.Fatalf("catch-up: %v", err)
	}
	if len(caught) != 1 {
		t.Fatalf("expected 1 event, got %d", len(caught))
	}
	if caught[0].ConversationID != conversationID {
		t.Fatalf("expected conversation %q, got %q", conversationID, caught[0].ConversationID)
	}
	if caught[0].Seq == nil || *caught[0].Seq != 1 {
		t.Fatalf("expected seq 1, got %v", caught[0].Seq)
	}
}

func TestDurableGatewayAppendRejectsInnerOuterSeqMismatch(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const conversationID = "conversation-1"
	inner := uint64(2)
	err := gateway.appendDurableEventLocked(conversationID, durableEventRecord{
		Seq: 1,
		Event: Envelope{
			Seq:            &inner,
			ConversationID: conversationID,
			Event:          json.RawMessage(`{"type":"agent_start"}`),
		},
	})
	if err == nil {
		t.Fatal("expected inner/outer seq mismatch to be rejected")
	}
	if !strings.Contains(err.Error(), "seq mismatch") {
		t.Fatalf("expected seq mismatch error, got %v", err)
	}

	err = gateway.appendDurableEventLocked(conversationID, durableEventRecord{
		Seq: 1,
		Event: Envelope{
			Seq:            nil,
			ConversationID: conversationID,
			Event:          json.RawMessage(`{"type":"agent_start"}`),
		},
	})
	if err == nil {
		t.Fatal("expected missing inner seq to be rejected")
	}
	if !strings.Contains(err.Error(), "seq mismatch") {
		t.Fatalf("expected seq mismatch error, got %v", err)
	}
}

func TestDurableGatewayReconstructsCommandGuardStateAcrossRestart(t *testing.T) {
	tmp := t.TempDir()
	storeDir := filepath.Join(tmp, "commands")
	runtimeDir := filepath.Join(tmp, "runtime")

	store, gateway, err := openGatewayAt(t, storeDir, runtimeDir)
	if err != nil {
		t.Fatalf("open first gateway: %v", err)
	}

	const conversationID = "conversation-1"
	claims := TokenClaims{TenantID: "tenant-1", AgentID: "agent-1", ConversationID: conversationID, Generation: 1}

	seq := uint64(1)
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:            &seq,
		ConversationID: conversationID,
		Event:          json.RawMessage(`{"type":"message_start","message_id":"00000000-0000-4000-8000-000000000001","message":{"role":"user","content":[{"type":"text","text":"ok"}],"timestamp":"2026-07-28T00:00:00Z"}}`),
	}); err != nil {
		t.Fatalf("receive message_start: %v", err)
	}

	seq = 2
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:            &seq,
		ConversationID: conversationID,
		Event:          json.RawMessage(`{"type":"approval_requested","request":{"id":"request-1","tool_call_id":"call-1","tool_name":"read_file","action":{"reviewable":"read"},"args_summary":"read"}}`),
	}); err != nil {
		t.Fatalf("receive approval_requested: %v", err)
	}

	seq = 3
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:            &seq,
		ConversationID: conversationID,
		Event:          json.RawMessage(`{"type":"approval_requested","request":{"id":"request-2","tool_call_id":"call-2","tool_name":"read_file","action":{"reviewable":"read"},"args_summary":"read"}}`),
	}); err != nil {
		t.Fatalf("receive second approval_requested: %v", err)
	}

	seq = 4
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:            &seq,
		ConversationID: conversationID,
		Event:          json.RawMessage(`{"type":"approval_resolved","request_id":"request-2","resolution":{"decision":{"type":"approve_once"}}}`),
	}); err != nil {
		t.Fatalf("receive approval_resolved: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close command store: %v", err)
	}

	store, gateway, err = openGatewayAt(t, storeDir, runtimeDir)
	if err != nil {
		t.Fatalf("reopen gateway: %v", err)
	}
	defer store.Close()

	// Before reconstruction the guard state must appear empty. This is safe
	// only because EnsureConversationStateRebuilt is invoked before command
	// admission in the browser WebSocket path.
	if gateway.IsAssistantTurnInFlight(conversationID) {
		t.Fatal("expected no in-flight turn before state rebuild")
	}
	if gateway.IsApprovalPending(conversationID, "request-1") {
		t.Fatal("expected no pending approvals before state rebuild")
	}

	if err := gateway.EnsureConversationStateRebuilt(context.Background(), conversationID); err != nil {
		t.Fatalf("rebuild conversation state: %v", err)
	}

	if !gateway.IsAssistantTurnInFlight(conversationID) {
		t.Fatal("expected in-flight turn after state rebuild")
	}
	if !gateway.IsApprovalPending(conversationID, "request-1") {
		t.Fatal("expected request-1 to be pending after state rebuild")
	}
	if gateway.IsApprovalPending(conversationID, "request-2") {
		t.Fatal("expected resolved request-2 not to be pending")
	}
	if gateway.IsApprovalPending(conversationID, "request-unknown") {
		t.Fatal("expected unknown request not to be pending")
	}
}

func TestDurableGatewayReconstructionFailsClosedOnCorruptState(t *testing.T) {
	tmp := t.TempDir()
	storeDir := filepath.Join(tmp, "commands")
	runtimeDir := filepath.Join(tmp, "runtime")

	store, gateway, err := openGatewayAt(t, storeDir, runtimeDir)
	if err != nil {
		t.Fatalf("open gateway: %v", err)
	}

	const conversationID = "conversation-1"
	// A non-contiguous durable event log must fail reconstruction rather than
	// defaulting to an empty "no turn / no approval" state.
	if err := os.WriteFile(
		gateway.eventPath(conversationID),
		[]byte(`{"seq":2,"event":{"seq":2,"conversation_id":"conversation-1","event":{"type":"agent_start"}}}`+"\n"),
		0o600,
	); err != nil {
		t.Fatalf("write corrupt event log: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close command store: %v", err)
	}

	store, gateway, err = openGatewayAt(t, storeDir, runtimeDir)
	if err != nil {
		t.Fatalf("reopen gateway: %v", err)
	}
	defer store.Close()

	if err := gateway.EnsureConversationStateRebuilt(context.Background(), conversationID); err == nil {
		t.Fatal("expected corrupt durable state to fail reconstruction")
	} else if !strings.Contains(err.Error(), "non-contiguous") {
		t.Fatalf("expected non-contiguous error, got %v", err)
	}

	// The guard must remain closed after failed reconstruction; it must not
	// silently default to an empty state that would admit abort or
	// approval_decision commands.
	if gateway.IsAssistantTurnInFlight(conversationID) {
		t.Fatal("expected in-flight flag to remain false after failed reconstruction")
	}
	if gateway.IsApprovalPending(conversationID, "request-1") {
		t.Fatal("expected pending approvals to remain empty after failed reconstruction")
	}
}
