package agentstate

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// parkHumanApproval provisions a human-owned persona in an existing store and
// parks its first turn behind one gated call — the same shape the secretary
// core leaves when a plan asks for consent.
func parkHumanApproval(t *testing.T, s *Store, pool *pgxpool.Pool, humanID, pa string, call PlanCall) *ToolApproval {
	t.Helper()
	ctx := context.Background()
	if _, _, err := s.EnsurePersona(ctx, pa, &humanID, "secretary"); err != nil {
		t.Fatalf("ensure persona: %v", err)
	}
	lease, err := s.AcquireWriter(ctx, pa, "h1", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{
			"text":         "please",
			"workspace_id": pid(t),
			"actor":        map[string]any{"display_name": "Yohaku"},
			"place":        map[string]any{"id": pid(t), "kind": "channel", "name": "general"},
		}, ActorKind: "human", ActorID: humanID, SourceSurface: "messaging"}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	mustPlan(t, s, pa, "t-1", lease.Generation, call)
	op, a, fresh, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"t-1:op:0", call.Tool, 0, call.Request)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if op.Status != "awaiting_approval" || a == nil || !fresh {
		t.Fatalf("gated claim: op=%+v approval=%+v fresh=%v", op, a, fresh)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-1", lease.Generation, CommitRequest{
		Outcome: "await",
		Events: []EventInput{{Kind: "approval_requested",
			Payload: map[string]any{"approval_id": a.ApprovalID}}},
	}); err != nil {
		t.Fatalf("await commit: %v", err)
	}
	return a
}

func TestListHumanApprovalsScopesToBoundHuman(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	humanA := mustHuman(t, pool)
	humanB := mustHuman(t, pool)
	paA, paB := pid(t), pid(t)
	send := PlanCall{Tool: "message.send", Route: "elevated",
		Request: map[string]any{"text": "hello"}}

	aA := parkHumanApproval(t, s, pool, humanA, paA, send)
	parkHumanApproval(t, s, pool, humanB, paB, send)

	got, err := s.ListHumanApprovals(ctx, humanA)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 1 || got[0].ApprovalID != aA.ApprovalID {
		t.Fatalf("human A inbox = %+v, want only its own pending approval", got)
	}
	one := got[0]
	if one.Status != "pending" || one.Tool != "message.send" || one.Route != "elevated" {
		t.Fatalf("pending row = %+v", one)
	}
	if one.SecretaryName == "" {
		t.Fatal("inbox row carries no secretary name")
	}
	if one.Input == nil || one.Input.Text != "please" ||
		one.Input.Place == nil || one.Input.Place.Name != "general" ||
		one.Input.ActorName != "Yohaku" {
		t.Fatalf("input provenance = %+v", one.Input)
	}

	// The other human's inbox never names A's approval.
	gotB, err := s.ListHumanApprovals(ctx, humanB)
	if err != nil {
		t.Fatalf("list B: %v", err)
	}
	if len(gotB) != 1 || gotB[0].PersonaID != paB {
		t.Fatalf("human B inbox = %+v", gotB)
	}
	if _, err := s.ListHumanApprovals(ctx, ""); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("empty human err = %v", err)
	}

	// Resolving moves the row to the resolved tail of the same inbox.
	if _, err := s.ResolveApproval(ctx, paA, aA.ApprovalID, ApprovalDecision{
		Decision: "deny_once", DecisionID: "d-1",
		DecidedByKind: "human", DecidedByID: humanA,
	}); err != nil {
		t.Fatalf("deny: %v", err)
	}
	got, err = s.ListHumanApprovals(ctx, humanA)
	if err != nil {
		t.Fatalf("list after deny: %v", err)
	}
	if len(got) != 1 || got[0].Status != "denied" || got[0].DecidedByID == nil ||
		*got[0].DecidedByID != humanA {
		t.Fatalf("resolved row = %+v", got)
	}
}

// A persona sealed for transfer takes no new decisions here, so its parked
// grant must stop presenting as actionable — while a row the human already
// decided stays in history. The durable pending row itself is untouched: it
// travels with the persona's portable state to the destination.
func TestListHumanApprovalsSealedPersona(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	human := mustHuman(t, pool)
	pa, paSealed := pid(t), pid(t)
	send := PlanCall{Tool: "message.send", Route: "elevated",
		Request: map[string]any{"text": "hello"}}

	a := parkHumanApproval(t, s, pool, human, pa, send)
	sealed := parkHumanApproval(t, s, pool, human, paSealed, send)

	// One row resolves before the seal: history survives it.
	if _, err := s.ResolveApproval(ctx, pa, a.ApprovalID, ApprovalDecision{
		Decision: "deny_once", DecisionID: "d-1",
		DecidedByKind: "human", DecidedByID: human,
	}); err != nil {
		t.Fatalf("deny: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE core_personas SET authority = 'sealed' WHERE persona_id = $1`,
		paSealed); err != nil {
		t.Fatalf("seal fixture: %v", err)
	}

	got, err := s.ListHumanApprovals(ctx, human)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 1 || got[0].ApprovalID != a.ApprovalID || got[0].Status != "denied" {
		t.Fatalf("post-seal inbox = %+v, want only the resolved row", got)
	}
	for _, row := range got {
		if row.Status == "pending" {
			t.Fatalf("sealed persona's grant still listed actionable: %+v", row)
		}
	}

	// A stale card's decision is refused; the pending row is preserved for
	// the destination, not mutated here.
	if _, err := s.ResolveApproval(ctx, paSealed, sealed.ApprovalID, ApprovalDecision{
		Decision: "approve_once", DecisionID: "d-2",
		DecidedByKind: "human", DecidedByID: human,
	}); !errors.Is(err, ErrPersonaInactive) {
		t.Fatalf("sealed decision err = %v, want ErrPersonaInactive", err)
	}
	current, err := s.ApprovalByID(ctx, sealed.ApprovalID)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if current.Status != "pending" || current.Decision != nil {
		t.Fatalf("sealed approval mutated: %+v", current)
	}
}

func TestApprovalByIDAndPersonaHuman(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	human := mustHuman(t, pool)
	pa := pid(t)
	a := parkHumanApproval(t, s, pool, human, pa, PlanCall{
		Tool: "message.send", Route: "elevated",
		Request: map[string]any{"text": "x"},
	})

	got, err := s.ApprovalByID(ctx, a.ApprovalID)
	if err != nil || got.PersonaID != pa || got.Tool != "message.send" {
		t.Fatalf("lookup: %+v err=%v", got, err)
	}
	if _, err := s.ApprovalByID(ctx, pid(t)); !errors.Is(err, ErrApprovalNotFound) {
		t.Fatalf("unknown approval err = %v", err)
	}

	owner, err := s.PersonaHumanID(ctx, pa)
	if err != nil || owner != human {
		t.Fatalf("persona human = %q err=%v", owner, err)
	}
	if owner, err := s.PersonaHumanID(ctx, pid(t)); err != nil || owner != "" {
		t.Fatalf("unknown persona human = %q err=%v", owner, err)
	}
}

// TestApprovalsChangedHook pins the post-commit fanout contract the
// notification adapter depends on: parking once, a recorded decision, and an
// identical replay each fire exactly once with the persona id — while a
// conflicting decision that never commits fires nothing.
func TestApprovalsChangedHook(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	human := mustHuman(t, pool)
	pa := pid(t)

	var calls []string
	s.ApprovalsChanged = func(_ context.Context, personaID string) {
		calls = append(calls, personaID)
	}
	a := parkHumanApproval(t, s, pool, human, pa, PlanCall{
		Tool: "message.send", Route: "elevated",
		Request: map[string]any{"text": "x"},
	})
	if len(calls) != 1 || calls[0] != pa {
		t.Fatalf("park notifications = %v", calls)
	}

	decision := ApprovalDecision{Decision: "approve_once", DecisionID: "d-1",
		DecidedByKind: "human", DecidedByID: human}
	if _, err := s.ResolveApproval(ctx, pa, a.ApprovalID, decision); err != nil {
		t.Fatalf("approve: %v", err)
	}
	if len(calls) != 2 {
		t.Fatalf("decision notifications = %v", calls)
	}
	if _, err := s.ResolveApproval(ctx, pa, a.ApprovalID, decision); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(calls) != 3 {
		t.Fatalf("replay notifications = %v", calls)
	}
	if _, err := s.ResolveApproval(ctx, pa, a.ApprovalID, ApprovalDecision{
		Decision: "deny_once", DecisionID: "d-2",
		DecidedByKind: "human", DecidedByID: human,
	}); !errors.Is(err, ErrApprovalConflict) {
		t.Fatalf("conflict err = %v", err)
	}
	if len(calls) != 3 {
		t.Fatalf("conflict notified anyway: %v", calls)
	}
}
