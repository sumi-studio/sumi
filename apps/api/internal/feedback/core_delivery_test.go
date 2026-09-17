package feedback

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

// The core delivery admits a feedback event as one durable core input — the
// persona is ensured, the outbox row finishes 'admitted', and the input
// carries the event's provenance and reply hint. No runtime is prepared.
func TestCoreAttentionDeliveryQueuesDurableInput(t *testing.T) {
	w := fixture(t)
	s := New(w.pool, []participant.Ref{w.dev, w.pa})
	ctx := context.Background()
	thread, err := s.Create(ctx, w.dev, "通知の相談", "Channel does not notify", uuid.NewString(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var eventID string
	if err := w.pool.QueryRow(ctx,
		`SELECT payload->>'EventID' FROM feedback_attention_outbox WHERE recipient_paid=$1`,
		w.pa.ID).Scan(&eventID); err != nil {
		t.Fatalf("outbox row for recipient PA: %v", err)
	}

	core := agentstate.NewStore(w.pool)
	d := &CoreAttentionDelivery{Core: core, Pool: w.pool}
	if err := s.DeliverAttention(ctx, d, 10); err != nil {
		t.Fatalf("deliver attention through core: %v", err)
	}

	var finished bool
	var outcome string
	if err := w.pool.QueryRow(ctx,
		`SELECT finished_at IS NOT NULL, outcome FROM feedback_attention_outbox WHERE recipient_paid=$1`,
		w.pa.ID).Scan(&finished, &outcome); err != nil {
		t.Fatal(err)
	}
	if !finished || outcome != "admitted" {
		t.Fatalf("outbox not finished as admitted: finished=%v outcome=%q", finished, outcome)
	}
	input, _, err := core.GetInput(ctx, w.pa.ID, "feedback:"+eventID)
	if err != nil {
		t.Fatalf("durable core input: %v", err)
	}
	if input.Kind != "feedback_created" || input.SourceSurface != "feedback" ||
		input.ThreadID != thread.ID || input.ActorKind != string(participant.KindHuman) ||
		input.ActorID != w.dev.ID || input.Attention != "reply" {
		t.Fatalf("core input contract fields wrong: %+v", input)
	}
	if text, _ := input.Payload["text"].(string); text != "Channel does not notify" {
		t.Fatalf("core input payload lost the body: %+v", input.Payload)
	}
	state, err := core.PersonaState(ctx, w.pa.ID)
	if err != nil || state.Persona.Authority != "active" {
		t.Fatalf("persona not ensured for the queued input: %+v %v", state, err)
	}
	if state.QueuedInputs != 1 {
		t.Fatalf("expected exactly one queued input, got %d", state.QueuedInputs)
	}
}

// A status change is delivered as observe — it informs the secretary without
// asking for a reply — and a re-delivery reconciles to the same input.
func TestCoreAttentionDeliveryStatusEventObserves(t *testing.T) {
	w := fixture(t)
	s := New(w.pool, []participant.Ref{w.dev, w.pa})
	ctx := context.Background()
	thread, err := s.Create(ctx, w.dev, "通知の相談", "Body", uuid.NewString(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Status(ctx, w.dev, thread.ID, "resolved", 1); err != nil {
		t.Fatal(err)
	}
	core := agentstate.NewStore(w.pool)
	d := &CoreAttentionDelivery{Core: core, Pool: w.pool}
	if err := s.DeliverAttention(ctx, d, 10); err != nil {
		t.Fatalf("deliver attention through core: %v", err)
	}
	var statusEventID string
	if err := w.pool.QueryRow(ctx,
		`SELECT payload->>'EventID' FROM feedback_attention_outbox
		 WHERE recipient_paid=$1 AND payload->>'Kind'='feedback_status'`,
		w.pa.ID).Scan(&statusEventID); err != nil {
		t.Fatalf("status outbox row: %v", err)
	}
	input, _, err := core.GetInput(ctx, w.pa.ID, "feedback:"+statusEventID)
	if err != nil {
		t.Fatalf("status core input: %v", err)
	}
	if input.Kind != "feedback_status" || input.Attention != "observe" {
		t.Fatalf("status event must arrive as observe: kind=%q attention=%q", input.Kind, input.Attention)
	}
	// A replayed delivery must reconcile to the stored input, not conflict.
	var raw []byte
	if err := w.pool.QueryRow(ctx,
		`SELECT payload FROM feedback_attention_outbox WHERE recipient_paid=$1 AND payload->>'EventID'=$2`,
		w.pa.ID, statusEventID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var replayed AttentionEvent
	if err := json.Unmarshal(raw, &replayed); err != nil {
		t.Fatal(err)
	}
	if found, err := d.Lookup(ctx, "unused", replayed); err != nil || !found {
		t.Fatalf("replayed lookup must find the stored input: found=%v err=%v", found, err)
	}
}

// A persona that transferred off this placement can never accept the event
// again: the delivery finishes 'suppressed' instead of retrying forever.
func TestCoreAttentionDeliveryTransferredPersonaSuppresses(t *testing.T) {
	w := fixture(t)
	s := New(w.pool, []participant.Ref{w.dev, w.pa})
	ctx := context.Background()
	if _, err := s.Create(ctx, w.dev, "通知の相談", "Body", uuid.NewString(), nil); err != nil {
		t.Fatal(err)
	}
	core := agentstate.NewStore(w.pool)
	d := &CoreAttentionDelivery{Core: core, Pool: w.pool}
	// EnsurePersona happens inside Prepare; transfer the persona first so the
	// first attempt surfaces the terminal failure.
	if _, _, err := core.EnsurePersona(ctx, w.pa.ID, nil, "pa"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.pool.Exec(ctx,
		`UPDATE core_personas SET authority='transferred' WHERE persona_id=$1`, w.pa.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.DeliverAttention(ctx, d, 10); err != nil {
		t.Fatalf("suppressed delivery must not error the drain: %v", err)
	}
	var finished bool
	var outcome string
	if err := w.pool.QueryRow(ctx,
		`SELECT finished_at IS NOT NULL, outcome FROM feedback_attention_outbox WHERE recipient_paid=$1`,
		w.pa.ID).Scan(&finished, &outcome); err != nil {
		t.Fatal(err)
	}
	if !finished || outcome != "suppressed" {
		t.Fatalf("transferred persona must finish suppressed: finished=%v outcome=%q", finished, outcome)
	}
}
