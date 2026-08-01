package messaging

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMentionInChannelIssuesCandidateWithUnreadRange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)

	// Plain channel talk wakes nobody (v0 default: mentionとDMは起こす、
	// それ以外は溜める).
	w.send(t, ctx, ch.PlaceID, w.humanA, "誰宛でもない話") // seq 1
	pending, err := w.store.PendingCandidates(ctx, w.agent.ID, 0, 10)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("plain talk must not issue candidates, got %+v", pending)
	}

	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "@Kuro 様子どう？") // seq 2
	pending, err = w.store.PendingCandidates(ctx, w.agent.ID, 0, 10)
	if err != nil {
		t.Fatalf("pending after mention: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("candidates = %d, want 1", len(pending))
	}
	c := pending[0]
	if c.CandidateSeq != 1 || c.TriggerReason != TriggerMention || c.Urgency != UrgencyNormal {
		t.Fatalf("candidate = %+v", c)
	}
	if c.MessageID != msg.MessageID || c.MessageSeq != 2 || c.Place.PlaceID != ch.PlaceID {
		t.Fatalf("candidate ref = %+v", c)
	}
	// Never read the place: unread runs from seq 1 to the delivered message.
	if c.UnreadFrom != 1 || c.UnreadTo != 2 {
		t.Fatalf("unread range = [%d, %d], want [1, 2]", c.UnreadFrom, c.UnreadTo)
	}
	if c.Author != w.humanA || len(c.Addressees) != 1 || c.Addressees[0] != w.agent {
		t.Fatalf("actor/addressees = %+v / %+v", c.Author, c.Addressees)
	}
}

func TestDMIssuesCandidateAndMentionWinsOverDM(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)
	dm, err := w.store.EnsureDM(ctx, w.humanA, w.agent)
	if err != nil {
		t.Fatalf("ensure dm: %v", err)
	}

	w.send(t, ctx, dm.PlaceID, w.humanA, "そこにいる？")
	pending, err := w.store.PendingCandidates(ctx, w.agent.ID, 0, 10)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 1 || pending[0].TriggerReason != TriggerDM {
		t.Fatalf("dm candidate = %+v", pending)
	}

	// A mention inside the dm is one candidate with the more specific trigger.
	w.send(t, ctx, dm.PlaceID, w.humanA, "@Kuro 急ぎで")
	pending, err = w.store.PendingCandidates(ctx, w.agent.ID, 1, 10)
	if err != nil {
		t.Fatalf("pending after mention: %v", err)
	}
	if len(pending) != 1 || pending[0].TriggerReason != TriggerMention {
		t.Fatalf("mention-in-dm candidate = %+v", pending)
	}

	// The agent's own dm reply never wakes the agent.
	if _, _, err := w.store.AppendMessage(ctx, AppendInput{
		PlaceID: dm.PlaceID, Author: w.agent, Content: "います", ClientNonce: "agent-reply-1",
	}); err != nil {
		t.Fatalf("agent reply: %v", err)
	}
	pending, err = w.store.PendingCandidates(ctx, w.agent.ID, 2, 10)
	if err != nil {
		t.Fatalf("pending after own reply: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("own reply must not self-wake, got %+v", pending)
	}
}

func TestCandidateIssuanceIsIdempotentWithSend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)

	in := AppendInput{
		PlaceID: ch.PlaceID, Author: w.humanA,
		Content: "@Kuro 一度だけ起こす", ClientNonce: "wake-once",
	}
	if _, _, err := w.store.AppendMessage(ctx, in); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, _, err := w.store.AppendMessage(ctx, in); err != nil {
		t.Fatalf("retry: %v", err)
	}
	pending, err := w.store.PendingCandidates(ctx, w.agent.ID, 0, 10)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("retried send must not duplicate candidates: %+v", pending)
	}
}

func TestAckCursorIsMonotonicAndBounded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	w.send(t, ctx, ch.PlaceID, w.humanA, "@Kuro 一件目")
	w.send(t, ctx, ch.PlaceID, w.humanB, "@Kuro 二件目")

	// Redelivery reads everything after the cursor.
	pending, err := w.store.PendingCandidates(ctx, w.agent.ID, 0, 10)
	if err != nil || len(pending) != 2 {
		t.Fatalf("pending = %+v err=%v", pending, err)
	}
	if err := w.store.AckCandidates(ctx, w.agent.ID, 1); err != nil {
		t.Fatalf("ack 1: %v", err)
	}
	pending, err = w.store.PendingCandidates(ctx, w.agent.ID, 1, 10)
	if err != nil || len(pending) != 1 || pending[0].CandidateSeq != 2 {
		t.Fatalf("pending after ack = %+v err=%v", pending, err)
	}

	// A stale ack (old generation replay) cannot rewind the cursor.
	if err := w.store.AckCandidates(ctx, w.agent.ID, 2); err != nil {
		t.Fatalf("ack 2: %v", err)
	}
	if err := w.store.AckCandidates(ctx, w.agent.ID, 1); err != nil {
		t.Fatalf("stale ack: %v", err)
	}
	acked, err := w.store.AckedCandidateSeq(ctx, w.agent.ID)
	if err != nil || acked != 2 {
		t.Fatalf("acked = %d err=%v, want 2", acked, err)
	}

	// Acking what was never issued is rejected.
	if err := w.store.AckCandidates(ctx, w.agent.ID, 99); !errors.Is(err, ErrSeqBeyondLatest) {
		t.Fatalf("ack beyond issued: got %v, want ErrSeqBeyondLatest", err)
	}
}

func TestReadThroughSupersedesCandidates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	w.send(t, ctx, ch.PlaceID, w.humanA, "@Kuro 一件目") // seq 1 → candidate 1
	w.send(t, ctx, ch.PlaceID, w.humanA, "間の話")       // seq 2
	w.send(t, ctx, ch.PlaceID, w.humanA, "@Kuro 二件目") // seq 3 → candidate 2

	// The agent reads through seq 1 (durable admission): candidate 1 is
	// superseded, candidate 2 still pending.
	if err := w.store.ReadThrough(ctx, ch.PlaceID, w.agent, 1); err != nil {
		t.Fatalf("read through: %v", err)
	}
	pending, err := w.store.PendingCandidates(ctx, w.agent.ID, 0, 10)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 1 || pending[0].MessageSeq != 3 {
		t.Fatalf("pending after read = %+v", pending)
	}

	// A human reading the same place resolves nothing for the agent.
	if err := w.store.ReadThrough(ctx, ch.PlaceID, w.humanB, 3); err != nil {
		t.Fatalf("human read through: %v", err)
	}
	pending, err = w.store.PendingCandidates(ctx, w.agent.ID, 0, 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("agent candidates must survive another's read: %+v err=%v", pending, err)
	}

	// Reading to the end clears the inbox; the unread projection agrees.
	if err := w.store.ReadThrough(ctx, ch.PlaceID, w.agent, 3); err != nil {
		t.Fatalf("read through 3: %v", err)
	}
	pending, err = w.store.PendingCandidates(ctx, w.agent.ID, 0, 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending after full read = %+v err=%v", pending, err)
	}
}
