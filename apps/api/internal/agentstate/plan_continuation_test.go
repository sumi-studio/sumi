package agentstate

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// Provider continuation (opaque encrypted reasoning) is stored with its
// round, returned unchanged, part of the identical-resave comparison, and
// never required.
func TestPlanContinuationRoundTrip(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10); err != nil {
		t.Fatal(err)
	}
	enc := "gAAAA-opaque/+=bytes"
	cont := json.RawMessage(`{"scope":"chatgpt:abc","output":[{"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":"` + enc + `"},{"type":"function_call","id":"fc_1","call_id":"call-0"}]}`)
	call := PlanCall{Tool: "journal.note", Route: "normal", Request: map[string]any{"text": "n"}}
	dec := Decision{Text: "", Calls: []PlanCall{call}, Usage: map[string]any{}, Continuation: cont}
	p, created, err := s.SavePlan(ctx, pa, "t-1", lease.Generation, 0, dec)
	if err != nil || !created {
		t.Fatalf("save: created=%v err=%v", created, err)
	}
	var got struct {
		Output []map[string]any `json:"output"`
	}
	if err := json.Unmarshal(p.Plan[0].Continuation, &got); err != nil || got.Output[0]["encrypted_content"] != enc {
		t.Fatalf("continuation not returned unchanged: %s err=%v", p.Plan[0].Continuation, err)
	}
	// Identical resave (lost response) replays; a different continuation
	// at the recorded round is a conflicting decision.
	if _, created, err := s.SavePlan(ctx, pa, "t-1", lease.Generation, 0, dec); err != nil || created {
		t.Fatalf("identical resave: created=%v err=%v", created, err)
	}
	other := dec
	other.Continuation = json.RawMessage(`{"scope":"chatgpt:abc","output":[]}`)
	if _, _, err := s.SavePlan(ctx, pa, "t-1", lease.Generation, 0, other); !errors.Is(err, ErrTurnConflict) {
		t.Fatalf("different continuation err = %v, want ErrTurnConflict", err)
	}
	load, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10)
	if err != nil || load.Plan == nil || len(load.Plan.Plan[0].Continuation) == 0 {
		t.Fatalf("load plan continuation: %+v err=%v", load.Plan, err)
	}
	// A round without continuation stores none; a non-object is refused.
	p, _, err = s.SavePlan(ctx, pa, "t-1", lease.Generation, 1, Decision{Text: "done", Calls: []PlanCall{}})
	if err != nil || len(p.Plan) != 2 || p.Plan[1].Continuation != nil {
		t.Fatalf("round without continuation: %+v err=%v", p.Plan, err)
	}
	b, _ := json.Marshal(p.Plan[1])
	var raw map[string]any
	_ = json.Unmarshal(b, &raw)
	if _, present := raw["continuation"]; present {
		t.Fatalf("absent continuation serialized: %s", b)
	}
	if _, _, err := s.SavePlan(ctx, pa, "t-1", lease.Generation, 2, Decision{Text: "x", Calls: []PlanCall{},
		Continuation: json.RawMessage(`"not-an-object"`)}); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("non-object continuation err = %v, want ErrBadRequest", err)
	}
}
