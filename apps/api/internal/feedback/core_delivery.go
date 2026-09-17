package feedback

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

// CoreAttentionDelivery admits feedback attention events into the shared
// TypeScript secretary core — the same intake Messaging attention uses on a
// core-enabled placement: one persona per PersonalityAgent, one durable input
// per event. Admission idempotency lives in (persona, input_id), so a retried
// delivery and a lost outbox finish reconcile to exactly one queued input.
// The core's own lease/claim contract then delivers the input to the woken
// host; no legacy runtime is prepared, held, or spawned.
type CoreAttentionDelivery struct {
	Core *agentstate.Store
	// Pool resolves the persona's owning Human and display name for
	// EnsurePersona — the same agents lookup the messaging adapter runs.
	Pool *pgxpool.Pool
}

var _ AttentionDelivery = (*CoreAttentionDelivery)(nil)

// Prepare ensures the persona exists so an admitted input has a queue to
// land in while no runtime is running. There is no runtime to hold — a cold
// secretary acquires the writer lease when it next runs — so the release is
// a no-op.
func (d *CoreAttentionDelivery) Prepare(ctx context.Context, paID string) (func(), error) {
	if d == nil || d.Core == nil || d.Pool == nil {
		return nil, errors.New("core feedback delivery is unavailable")
	}
	var humanID *string
	var hid, displayName string
	err := d.Pool.QueryRow(ctx,
		"SELECT human_id::text, display_name FROM agents WHERE personality_agent_id = $1",
		paID).Scan(&hid, &displayName)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// The outbox row exists only for a participant-checked recipient, so
		// the agent record is expected; its absence must not block admission.
	case err != nil:
		return nil, fmt.Errorf("resolve agent for persona: %w", err)
	default:
		humanID = &hid
	}
	if _, _, err := d.Core.EnsurePersona(ctx, paID, humanID, displayName); err != nil {
		if errors.Is(err, agentstate.ErrPersonaInactive) {
			// A persona transferred off this placement can never accept
			// input here again — the delivery is terminally undeliverable,
			// not retryable.
			st, stateErr := d.Core.PersonaState(ctx, paID)
			if stateErr == nil && st.Persona.Authority == "transferred" {
				return nil, fmt.Errorf("%w: %v", ErrDeliverySuppressed, err)
			}
		}
		return nil, err
	}
	return func() {}, nil
}

func (d *CoreAttentionDelivery) Lookup(ctx context.Context, _ string, e AttentionEvent) (bool, error) {
	if d == nil || d.Core == nil {
		return false, errors.New("core feedback delivery is unavailable")
	}
	input, _, err := d.Core.GetInput(ctx, e.PersonalityAgentID, coreInputID(e))
	if errors.Is(err, agentstate.ErrInputNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// A receipt may only stand for an input that actually carries this event;
	// the same input id bound to different content is a conflict, not a
	// delivery — and it can never become this event, so it is terminal.
	if !coreInputMatches(e, input) {
		return false, fmt.Errorf("%w: the durable input under %s already carries different content",
			ErrDeliverySuppressed, input.InputID)
	}
	return true, nil
}

func (d *CoreAttentionDelivery) Admit(ctx context.Context, _ string, e AttentionEvent) error {
	if d == nil || d.Core == nil {
		return errors.New("core feedback delivery is unavailable")
	}
	_, _, err := d.Core.SubmitInput(ctx, coreInputFromEvent(e))
	switch {
	case errors.Is(err, agentstate.ErrTurnConflict):
		// A row under this input id with different content was admitted
		// between Lookup and Submit. It can never become this event.
		return fmt.Errorf("%w: the durable input under %s already carries different content",
			ErrDeliverySuppressed, coreInputID(e))
	case errors.Is(err, agentstate.ErrPersonaInactive):
		// 'transferred' is terminal: the persona can never accept input on
		// this placement again. 'sealed' and 'staged' can still abort back
		// to active, so those deliveries stay pending and retryable.
		st, stateErr := d.Core.PersonaState(ctx, e.PersonalityAgentID)
		if stateErr == nil && st.Persona.Authority == "transferred" {
			return fmt.Errorf("%w: %v", ErrDeliverySuppressed, err)
		}
	}
	return err
}

func coreInputID(e AttentionEvent) string {
	return "feedback:" + e.EventID
}

// coreInputFromEvent freezes the attention event into a durable core input:
// actor, thread, title, revision, and the source's own occurrence time travel
// as contract fields and payload provenance so the journal keeps who/where/
// when without presenting it as a generic human message.
func coreInputFromEvent(e AttentionEvent) *agentstate.Input {
	actorID := e.Actor.Participant.HumanID
	actorKind := string(e.Actor.Participant.Kind)
	if e.Actor.Participant.Kind == participant.KindPersonalityAgent {
		actorID = e.Actor.Participant.PersonalityAgentID
	}
	payload := map[string]any{
		"event_id":   e.EventID,
		"event_kind": e.Kind,
		"thread_id":  e.ThreadID,
		"title":      e.Title,
		"revision":   e.Revision,
		"actor": map[string]any{
			"kind":         actorKind,
			"id":           actorID,
			"display_name": e.Actor.DisplayName,
		},
	}
	if e.Body != "" {
		payload["text"] = e.Body
	}
	attention := "reply"
	if e.Kind == "feedback_status" {
		// A status flip informs the secretary; it never asks for a reply.
		attention = "observe"
	}
	occurredAt := e.OccurredAt
	return &agentstate.Input{
		PersonaID:     e.PersonalityAgentID,
		InputID:       coreInputID(e),
		Kind:          e.Kind,
		Payload:       payload,
		ActorKind:     actorKind,
		ActorID:       actorID,
		SourceSurface: "feedback",
		ThreadID:      e.ThreadID,
		OccurredAt:    &occurredAt,
		Attention:     attention,
	}
}

// coreInputMatches decides whether the durable input under this event's id
// really is this event — the contract fields and payload must match what
// SubmitInput would have written. PG stores timestamptz at microsecond
// precision and jsonb numbers without a Go type, so time and payload
// comparisons run at the stored precision and the canonical JSON form.
func coreInputMatches(e AttentionEvent, input agentstate.Input) bool {
	expected := coreInputFromEvent(e)
	if input.Kind != expected.Kind || input.ActorKind != expected.ActorKind ||
		input.ActorID != expected.ActorID || input.SourceSurface != expected.SourceSurface ||
		input.ThreadID != expected.ThreadID || input.Attention != expected.Attention {
		return false
	}
	if expected.OccurredAt == nil {
		if input.OccurredAt != nil {
			return false
		}
	} else if input.OccurredAt == nil ||
		input.OccurredAt.UnixMicro() != expected.OccurredAt.UnixMicro() {
		return false
	}
	want, err := json.Marshal(expected.Payload)
	if err != nil {
		return false
	}
	got, err := json.Marshal(input.Payload)
	if err != nil {
		return false
	}
	// Marshaled maps are canonical (keys sorted), so a jsonb-round-tripped
	// int64 equals the original.
	return bytes.Equal(want, got)
}
