package agentevents

import (
	"context"
	"encoding/json"
	"fmt"
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

	if err := gateway.publishRuntimeState(agentID, runtimeState{Generation: maxJSONSafeInteger}); err != nil {
		t.Fatalf("publish max JSON-safe generation failed: %v", err)
	}
	st, err := gateway.state(context.Background(), agentID)
	if err != nil || st.Generation != maxJSONSafeInteger {
		t.Fatalf("max generation must be accepted in recovery: err=%v gen=%d", err, st.Generation)
	}

	if err := gateway.publishRuntimeState(agentID, runtimeState{Generation: maxJSONSafeInteger + 1}); err == nil {
		t.Fatal("publish must reject generation max+1")
	}

	raw, err := json.Marshal(runtimeState{Generation: maxJSONSafeInteger + 1})
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
