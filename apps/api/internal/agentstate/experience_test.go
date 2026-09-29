package agentstate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// A request that requeues after a transient failure journals what it had
// already done — its input, the round's text, a sent message and its
// receipt — so another input served meanwhile sees it. The attempt that
// resumes the request replays its plan and commits the same records again;
// the store keeps the first of each, and only the new ones land.
func TestCommitKeepsEachExperienceOnceAcrossAttempts(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	l, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	for _, id := range []string{"in-1", "in-2"} {
		if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: id, Kind: "message",
			Payload: map[string]any{"text": id}}); err != nil {
			t.Fatalf("submit %s: %v", id, err)
		}
	}
	receipt := func(id string) EventInput {
		return EventInput{Kind: "input_received", Payload: map[string]any{"input_id": id}}
	}
	experience := func(id string) []EventInput {
		return []EventInput{
			receipt(id),
			{Kind: "assistant_message", Payload: map[string]any{"text": "Posting.", "round": 0}},
			{Kind: "tool_call", Payload: map[string]any{"tool": "messaging.send", "call_id": "repeatable",
				"request": map[string]any{"content": id}, "route": "normal", "round": 0, "call_index": 0}},
			{Kind: "tool_result", Payload: map[string]any{"tool": "messaging.send", "call_id": "repeatable",
				"call_index": 0, "response": map[string]any{"message_id": "m-" + id}}},
		}
	}

	// in-1, attempt 1: sends, then the next model call fails transiently.
	if ld, err := s.LoadTurn(ctx, pa, l.Generation, "t-1", 50); err != nil || ld.Input == nil || ld.Input.InputID != "in-1" {
		t.Fatalf("load in-1: %+v err=%v", ld, err)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-1", l.Generation, CommitRequest{
		Outcome:   "fail",
		Retryable: true,
		Error:     "model: upstream 503",
		Events: append(experience("in-1"),
			EventInput{Kind: "turn_paused", Payload: map[string]any{"reason": "retry"}}),
	}); err != nil {
		t.Fatalf("paused commit: %v", err)
	}

	// in-2 is served while in-1 backs off; its context carries in-1's send.
	// Its own call at the same plan position is a different experience.
	ld2, err := s.LoadTurn(ctx, pa, l.Generation, "t-2", 50)
	if err != nil || ld2.Input == nil || ld2.Input.InputID != "in-2" {
		t.Fatalf("load in-2: %+v err=%v", ld2, err)
	}
	sawSend := false
	for _, e := range ld2.Context {
		if e.Kind == "tool_call" && e.Payload["request"].(map[string]any)["content"] == "in-1" {
			sawSend = true
		}
	}
	if !sawSend {
		t.Fatalf("in-2's context lacks in-1's send: %+v", ld2.Context)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-2", l.Generation, CommitRequest{
		Outcome: "complete",
		Events: append(experience("in-2"),
			EventInput{Kind: "assistant_message", Payload: map[string]any{"text": "Done.", "round": 1}}),
		Output: map[string]any{"text": "Done."},
	}); err != nil {
		t.Fatalf("in-2 commit: %v", err)
	}

	// in-1, attempt 2: the backoff elapses and the replayed plan commits
	// everything it experienced, old and new.
	if _, err := pool.Exec(ctx,
		`UPDATE core_inputs SET not_before = NULL WHERE persona_id = $1 AND input_id = 'in-1'`, pa); err != nil {
		t.Fatalf("elapse backoff: %v", err)
	}
	ld3, err := s.LoadTurn(ctx, pa, l.Generation, "t-3", 50)
	if err != nil || ld3.Input == nil || ld3.Input.InputID != "in-1" {
		t.Fatalf("load in-1 again: %+v err=%v", ld3, err)
	}
	// in-1's earlier records are the turn's own: they are not in its
	// context, so the resumed turn presents them once, as its plan.
	for _, e := range ld3.Context {
		if e.TurnID == "t-1" {
			t.Fatalf("resumed context repeats its own earlier record: %+v", e)
		}
	}
	final := CommitRequest{
		Outcome: "complete",
		Events: append(experience("in-1"),
			EventInput{Kind: "assistant_message", Payload: map[string]any{"text": "Posted.", "round": 1}}),
		Output: map[string]any{"text": "Posted."},
	}
	if _, err := s.CommitTurn(ctx, pa, "t-3", l.Generation, final); err != nil {
		t.Fatalf("resumed commit: %v", err)
	}
	// An identical replay of the lost-response commit still compares as
	// sent (commit_request keeps the request, not the deduplicated list).
	if _, err := s.CommitTurn(ctx, pa, "t-3", l.Generation, final); err != nil {
		t.Fatalf("identical replay: %v", err)
	}

	rows, err := pool.Query(ctx,
		`SELECT turn_id, kind, COALESCE(payload->>'text', payload->>'call_index', payload->>'input_id', payload->>'reason')
		 FROM core_events WHERE persona_id = $1 ORDER BY seq`, pa)
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	var got []string
	for rows.Next() {
		var turn, kind, key string
		if err := rows.Scan(&turn, &kind, &key); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, turn+" "+kind+" "+key)
	}
	rows.Close()
	want := []string{
		"t-1 input_received in-1",
		"t-1 assistant_message Posting.",
		"t-1 tool_call 0",
		"t-1 tool_result 0",
		"t-1 turn_paused retry",
		"t-2 input_received in-2",
		"t-2 assistant_message Posting.",
		"t-2 tool_call 0",
		"t-2 tool_result 0",
		"t-2 assistant_message Done.",
		"t-3 assistant_message Posted.",
	}
	if len(got) != len(want) {
		t.Fatalf("journal =\n%v\nwant\n%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("journal =\n%v\nwant\n%v", got, want)
		}
	}
	if n := unlinkedReceipts(t, pool, pa); n != 0 {
		t.Fatalf("unlinked receipts = %d", n)
	}
}

func TestExperienceKey(t *testing.T) {
	cases := []struct {
		kind    string
		payload map[string]any
		want    string
	}{
		{"assistant_message", map[string]any{"round": float64(2)}, "assistant_message:2"},
		{"assistant_message", map[string]any{"text": "no round"}, ""},
		{"tool_call", map[string]any{"call_index": float64(0)}, "tool_call:0"},
		{"tool_result", map[string]any{"call_index": 3}, "tool_result:3"},
		{"tool_result", map[string]any{"call_index": 1.5}, ""},
		{"tool_result", map[string]any{"call_id": "only-an-id"}, ""},
		{"approval_requested", map[string]any{"approval_id": "a-1"}, "approval_requested:a-1"},
		{"turn_paused", map[string]any{"reason": "retry"}, ""},
		{"note", map[string]any{"text": "x"}, ""},
	}
	for _, c := range cases {
		got, ok := experienceKey(c.kind, c.payload)
		if (c.want == "") == ok || got != c.want {
			t.Errorf("experienceKey(%s, %v) = %q,%v want %q", c.kind, c.payload, got, ok, c.want)
		}
	}
}

// sendEffect is a delegated Messaging-like effect that counts how often it
// actually ran; a request naming recipient "nobody" is rejected the way a
// deterministic Messaging refusal is (400).
func sendEffect(t *testing.T, s *Store) *int32 {
	t.Helper()
	var applied int32
	if err := s.RegisterEffect("messaging.send", ToolEffect{
		Apply: func(_ context.Context, _ pgx.Tx, _, _ string, req map[string]any) (map[string]any, error) {
			if req["to"] == "nobody" {
				return nil, fmt.Errorf("%w: messaging.send: unknown recipient", ErrBadRequest)
			}
			n := atomic.AddInt32(&applied, 1)
			return map[string]any{"message_id": fmt.Sprintf("m-%d", n)}, nil
		},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	return &applied
}

// journalLines is the persona's journal as "turn kind key" lines, in seq
// order — the key being the record's input, text, call index or reason.
func journalLines(t *testing.T, pool *pgxpool.Pool, pa string) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT turn_id, kind, COALESCE(payload->>'input_id', payload->>'text', payload->>'call_index', payload->>'reason', '')
		 FROM core_events WHERE persona_id = $1 ORDER BY seq`, pa)
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var turn, kind, key string
		if err := rows.Scan(&turn, &kind, &key); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, turn+" "+kind+" "+key)
	}
	return got
}

func contextLines(evs []Event) []string {
	var out []string
	for _, e := range evs {
		key := ""
		for _, k := range []string{"input_id", "text", "call_index", "reason"} {
			if v, ok := e.Payload[k]; ok && v != nil {
				key = fmt.Sprint(v)
				break
			}
		}
		out = append(out, e.TurnID+" "+e.Kind+" "+key)
	}
	return out
}

func wantLines(t *testing.T, what string, got, want []string) {
	t.Helper()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("%s =\n  %s\nwant\n  %s", what, strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

// A host stops after a Messaging send committed and before the turn did.
// An older input — parked on an approval, decided while the host was down —
// is served first after the restart: it sees the send, its receipt and that
// the request stopped there. The interrupted input then resumes, replays
// the stored receipt without sending again, and its commit adds only what
// was not journaled yet.
func TestInterruptedSendVisibleToOlderInputServedFirst(t *testing.T) {
	approve := PlanCall{Tool: "message.send", Route: "elevated", Request: map[string]any{"text": "承認済みの連絡"}}
	f := newApprovalFixture(t, approve)
	applied := sendEffect(t, f.s)
	ctx := context.Background()

	a := f.park(t, approve)
	if _, err := f.s.CommitTurn(ctx, f.pa, "t-1", f.gen, CommitRequest{Outcome: "await",
		Events: []EventInput{{Kind: "approval_requested", Payload: map[string]any{"approval_id": a.ApprovalID}}}}); err != nil {
		t.Fatalf("await commit: %v", err)
	}

	if _, _, err := f.s.SubmitInput(ctx, &Input{PersonaID: f.pa, InputID: "in-2", Kind: "message",
		Payload: map[string]any{"text": "tell Aoi"}, ActorKind: "human", ActorID: f.human}); err != nil {
		t.Fatalf("submit in-2: %v", err)
	}
	if ld, err := f.s.LoadTurn(ctx, f.pa, f.gen, "t-2", 50); err != nil || ld.Input == nil || ld.Input.InputID != "in-2" {
		t.Fatalf("load in-2: %+v err=%v", ld, err)
	}
	send := PlanCall{CallID: "call_send", Tool: "messaging.send", Route: "normal",
		Request: map[string]any{"to": "aoi", "content": "明日の会議は15時からです"}}
	mustPlan(t, f.s, f.pa, "t-2", f.gen, send)
	op, _, fresh, err := f.s.ClaimOperation(ctx, f.pa, "t-2", f.gen, "t-2:op:0", send.Tool, 0, send.Request)
	if err != nil || !fresh || op.Status != "done" || *applied != 1 {
		t.Fatalf("send: %+v fresh=%v applied=%d err=%v", op, fresh, *applied, err)
	}
	// The host dies here: no commit for t-2. The human decides meanwhile.
	if _, err := f.s.ResolveApproval(ctx, f.pa, a.ApprovalID, f.decision("approve_once", "d-1")); err != nil {
		t.Fatalf("approve: %v", err)
	}

	lease, err := f.s.AcquireWriter(ctx, f.pa, "h1", time.Minute)
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	f.gen = lease.Generation
	if rec, err := f.s.Recover(ctx, f.pa, f.gen); err != nil || len(rec.InterruptedTurns) != 1 {
		t.Fatalf("recover: %+v err=%v", rec, err)
	}
	// The fenced writer cannot add to the journal any more.
	if _, _, _, err := f.s.ClaimOperation(ctx, f.pa, "t-2", f.gen-1, "t-2:op:0", send.Tool, 0, send.Request); err == nil {
		t.Fatal("fenced claim succeeded")
	}

	ld, err := f.s.LoadTurn(ctx, f.pa, f.gen, "t-3", 50)
	if err != nil || ld.Input == nil || ld.Input.InputID != "in-1" {
		t.Fatalf("older input not served first: %+v err=%v", ld.Input, err)
	}
	// in-1's own records are its plan, not its context.
	wantLines(t, "in-1's context", contextLines(ld.Context), []string{
		"t-2 input_received in-2",
		"t-2 assistant_message reply",
		"t-2 tool_call 0",
		"t-2 tool_result 0",
		"t-2 turn_paused interrupted",
	})
	for _, e := range ld.Context {
		switch e.Kind {
		case "tool_call":
			if e.Payload["call_id"] != "call_send" || e.Payload["route"] != "normal" ||
				e.Payload["request"].(map[string]any)["content"] != "明日の会議は15時からです" {
				t.Fatalf("recorded call = %+v", e.Payload)
			}
		case "tool_result":
			if e.Payload["response"].(map[string]any)["message_id"] != "m-1" || e.Payload["call_id"] != "call_send" {
				t.Fatalf("recorded receipt = %+v", e.Payload)
			}
		}
	}
	if _, _, fresh := f.claim(t, "t-3", 0, approve); !fresh {
		t.Fatal("approved send did not run")
	}
	if _, err := f.s.CommitTurn(ctx, f.pa, "t-3", f.gen, CommitRequest{Outcome: "complete",
		Events: []EventInput{{Kind: "assistant_message", Payload: map[string]any{"text": "sent it", "round": 1}}},
		Output: map[string]any{"text": "sent it"}}); err != nil {
		t.Fatalf("in-1 commit: %v", err)
	}

	ld4, err := f.s.LoadTurn(ctx, f.pa, f.gen, "t-4", 50)
	if err != nil || ld4.Input == nil || ld4.Input.InputID != "in-2" || ld4.Plan == nil {
		t.Fatalf("resume in-2: %+v err=%v", ld4.Input, err)
	}
	for _, e := range ld4.Context {
		if e.TurnID == "t-2" {
			t.Fatalf("resumed context repeats its own record: %+v", e)
		}
	}
	op2, _, fresh2, err := f.s.ClaimOperation(ctx, f.pa, "t-4", f.gen, "t-4:op:0", send.Tool, 0, send.Request)
	if err != nil || fresh2 || op2.Status != "done" || op2.Response["message_id"] != "m-1" || *applied != 1 {
		t.Fatalf("replayed send: %+v fresh=%v applied=%d err=%v", op2, fresh2, *applied, err)
	}
	if _, err := f.s.CommitTurn(ctx, f.pa, "t-4", f.gen, CommitRequest{Outcome: "complete",
		Events: []EventInput{
			{Kind: "input_received", Payload: map[string]any{"input_id": "in-2"}},
			{Kind: "assistant_message", Payload: map[string]any{"text": "reply", "round": 0}},
			{Kind: "tool_call", Payload: map[string]any{"tool": send.Tool, "call_id": "call_send",
				"request": send.Request, "route": "normal", "round": 0, "call_index": 0}},
			{Kind: "tool_result", Payload: map[string]any{"tool": send.Tool, "call_id": "call_send",
				"call_index": 0, "response": op2.Response, "replayed": true}},
			{Kind: "assistant_message", Payload: map[string]any{"text": "Aoiに送りました", "round": 1}},
		},
		Output: map[string]any{"text": "Aoiに送りました"}}); err != nil {
		t.Fatalf("in-2 commit: %v", err)
	}
	wantLines(t, "journal", journalLines(t, f.pool, f.pa), []string{
		"t-1 input_received in-1",
		"t-1 assistant_message reply",
		"t-1 approval_requested 0",
		"t-2 input_received in-2",
		"t-2 assistant_message reply",
		"t-2 tool_call 0",
		"t-2 tool_result 0",
		"t-1 approval_decided in-1",
		"t-2 turn_paused interrupted",
		"t-3 tool_call 0",
		"t-3 tool_result 0",
		"t-3 assistant_message sent it",
		"t-4 assistant_message Aoiに送りました",
	})
	if *applied != 1 || f.outboxCount(t, "secretary_message") != 1 {
		t.Fatalf("effects: messaging=%d secretary_message=%d", *applied, f.outboxCount(t, "secretary_message"))
	}
	if n := unlinkedReceipts(t, f.pool, f.pa); n != 0 {
		t.Fatalf("unlinked receipts = %d", n)
	}
}

// A request that ends in a terminal failure — the Core's attempt cap after
// an interrupted attempt, or a recurring internal error in the attempt that
// sent — commits only its input and the failure marker; the send it made is
// already journaled at its claim, so what comes after sees it.
func TestTerminalFailureKeepsCompletedSend(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	applied := sendEffect(t, s)
	l, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	for _, id := range []string{"in-1", "in-2", "in-3"} {
		if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: id, Kind: "message",
			Payload: map[string]any{"text": id}}); err != nil {
			t.Fatalf("submit %s: %v", id, err)
		}
	}
	send := func(content string) PlanCall {
		return PlanCall{CallID: "c", Tool: "messaging.send", Route: "normal",
			Request: map[string]any{"to": "aoi", "content": content}}
	}
	// What the Core commits for a terminal failure whose turn's records
	// are not in hand: the input and the failure marker (secretary.ts
	// attempt cap and recurring-error paths).
	failed := func(inputID string) CommitRequest {
		return CommitRequest{Outcome: "fail", Error: "terminal", Events: []EventInput{
			{Kind: "input_received", Payload: map[string]any{"input_id": inputID}},
			{Kind: "turn_failed", Payload: map[string]any{"error_kind": nil}},
		}}
	}
	claimSend := func(turn string, gen int64, c PlanCall) {
		t.Helper()
		if _, err := s.LoadTurn(ctx, pa, gen, turn, 50); err != nil {
			t.Fatalf("load %s: %v", turn, err)
		}
		mustPlan(t, s, pa, turn, gen, c)
		if op, _, _, err := s.ClaimOperation(ctx, pa, turn, gen, turn+":op:0", c.Tool, 0, c.Request); err != nil || op.Status != "done" {
			t.Fatalf("send %s: %+v err=%v", turn, op, err)
		}
	}

	// in-1: sends, the host dies; the resumed attempt is over the cap.
	claimSend("t-1", l.Generation, send("one"))
	l2, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	if _, err := s.Recover(ctx, pa, l2.Generation); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if ld, err := s.LoadTurn(ctx, pa, l2.Generation, "t-2", 50); err != nil || ld.Input.InputID != "in-1" {
		t.Fatalf("resume in-1: %+v err=%v", ld.Input, err)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-2", l2.Generation, failed("in-1")); err != nil {
		t.Fatalf("cap commit: %v", err)
	}

	// in-2: sends, then fails terminally in the same attempt.
	claimSend("t-3", l2.Generation, send("two"))
	if _, err := s.CommitTurn(ctx, pa, "t-3", l2.Generation, failed("in-2")); err != nil {
		t.Fatalf("poison commit: %v", err)
	}

	ld, err := s.LoadTurn(ctx, pa, l2.Generation, "t-4", 50)
	if err != nil || ld.Input.InputID != "in-3" {
		t.Fatalf("load in-3: %+v err=%v", ld.Input, err)
	}
	wantLines(t, "in-3's context", contextLines(ld.Context), []string{
		"t-1 input_received in-1",
		"t-1 assistant_message reply",
		"t-1 tool_call 0",
		"t-1 tool_result 0",
		"t-1 turn_paused interrupted",
		"t-2 turn_failed ",
		"t-3 input_received in-2",
		"t-3 assistant_message reply",
		"t-3 tool_call 0",
		"t-3 tool_result 0",
		"t-3 turn_failed ",
	})
	for _, id := range []string{"in-1", "in-2"} {
		in, _, err := s.GetInput(ctx, pa, id)
		if err != nil || in.Status != "done" {
			t.Fatalf("%s: %+v err=%v", id, in, err)
		}
	}
	if *applied != 2 {
		t.Fatalf("messaging applied %d times, want 2", *applied)
	}
}

// A deterministic rejection is the call's result: journaled when it happens,
// ahead of the later calls of the same round, so source order holds; a
// fenced writer's rejection journals nothing.
func TestRejectedClaimJournaledInOrder(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	applied := sendEffect(t, s)
	l, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "tell both"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, l.Generation, "t-1", 50); err != nil {
		t.Fatalf("load: %v", err)
	}
	bad := PlanCall{CallID: "a", Tool: "messaging.send", Route: "normal", Request: map[string]any{"to": "nobody", "content": "x"}}
	good := PlanCall{CallID: "b", Tool: "messaging.send", Route: "normal", Request: map[string]any{"to": "aoi", "content": "y"}}
	mustPlan(t, s, pa, "t-1", l.Generation, bad, good)

	if _, _, _, err := s.ClaimOperation(ctx, pa, "t-1", l.Generation-1, "t-1:op:0", bad.Tool, 0, bad.Request); err == nil {
		t.Fatal("stale generation claimed")
	}
	if _, _, _, err := s.ClaimOperation(ctx, pa, "t-1", l.Generation, "t-1:op:0", bad.Tool, 0, bad.Request); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("rejection err = %v", err)
	}
	// The retried rejection (a lost response) is still one record.
	if _, _, _, err := s.ClaimOperation(ctx, pa, "t-1", l.Generation, "t-1:op:0", bad.Tool, 0, bad.Request); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("second rejection err = %v", err)
	}
	if op, _, _, err := s.ClaimOperation(ctx, pa, "t-1", l.Generation, "t-1:op:1", good.Tool, 1, good.Request); err != nil || op.Status != "done" {
		t.Fatalf("good send: %+v err=%v", op, err)
	}
	rejection := "messaging.send: unknown recipient"
	if _, err := s.CommitTurn(ctx, pa, "t-1", l.Generation, CommitRequest{Outcome: "complete",
		Events: []EventInput{
			{Kind: "input_received", Payload: map[string]any{"input_id": "in-1"}},
			{Kind: "assistant_message", Payload: map[string]any{"text": "reply", "round": 0}},
			{Kind: "tool_call", Payload: map[string]any{"tool": bad.Tool, "call_id": "a", "request": bad.Request, "route": "normal", "round": 0, "call_index": 0}},
			{Kind: "tool_result", Payload: map[string]any{"tool": bad.Tool, "call_id": "a", "call_index": 0, "error": rejection}},
			{Kind: "tool_call", Payload: map[string]any{"tool": good.Tool, "call_id": "b", "request": good.Request, "route": "normal", "round": 0, "call_index": 1}},
			{Kind: "tool_result", Payload: map[string]any{"tool": good.Tool, "call_id": "b", "call_index": 1, "response": map[string]any{"message_id": "m-1"}}},
			{Kind: "assistant_message", Payload: map[string]any{"text": "one failed", "round": 1}},
		},
		Output: map[string]any{"text": "one failed"}}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	wantLines(t, "journal", journalLines(t, pool, pa), []string{
		"t-1 input_received in-1",
		"t-1 assistant_message reply",
		"t-1 tool_call 0",
		"t-1 tool_result 0",
		"t-1 tool_call 1",
		"t-1 tool_result 1",
		"t-1 assistant_message one failed",
	})
	var got string
	if err := pool.QueryRow(ctx, `SELECT payload->>'error' FROM core_events
		WHERE persona_id = $1 AND kind = 'tool_result' AND payload->>'call_index' = '0'`, pa).Scan(&got); err != nil ||
		!strings.Contains(got, rejection) {
		t.Fatalf("journaled rejection = %q err=%v", got, err)
	}
	if *applied != 1 {
		t.Fatalf("applied = %d", *applied)
	}
}

// An operation a turn finalizes after claiming it running (an external
// executor's receipt, or the Core's unknown-outcome receipt for a call a
// stopped attempt left running) is journaled with that receipt once.
func TestCompletedOperationJournaled(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	sendEffect(t, s)
	l, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "go"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, l.Generation, "t-1", 50); err != nil {
		t.Fatalf("load: %v", err)
	}
	c := PlanCall{CallID: "c", Tool: "messaging.send", Route: "normal", Request: map[string]any{"to": "aoi", "content": "z"}}
	mustPlan(t, s, pa, "t-1", l.Generation, c)
	// No store path leaves an operation running across a commit; seed one
	// the way an external executor's claim would have.
	if _, err := pool.Exec(ctx, `
		INSERT INTO core_operations (persona_id, operation_id, turn_id, tool, idempotency_key, request, status, claimed_generation)
		VALUES ($1, 'op-x', 't-1', $2, 'in-1:tool:0', $3, 'running', $4)`,
		pa, c.Tool, c.Request, l.Generation); err != nil {
		t.Fatalf("seed running op: %v", err)
	}
	unknown := map[string]any{"error": "uncompleted operation: whether its effect took place is unknown"}
	for range 2 {
		if op, err := s.CompleteOperation(ctx, pa, "op-x", l.Generation, unknown, true); err != nil || op.Status != "failed" {
			t.Fatalf("complete: %+v err=%v", op, err)
		}
	}
	wantLines(t, "journal", journalLines(t, pool, pa), []string{
		"t-1 input_received in-1",
		"t-1 assistant_message reply",
		"t-1 tool_call 0",
		"t-1 tool_result 0",
	})
}

// A claim journals its input first, so the store's receipt is usually the
// one that stands: it carries what the Core's receipt would (secretary.ts
// inputReceivedEvent), typed the same way.
func TestClaimReceiptCarriesCoreProvenance(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	sendEffect(t, s)
	l, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "terminal", Payload: map[string]any{
		"text": "exited", "session_id": "ts-1", "status": "ended", "end_reason": "exit", "exit_code": 2,
		"exit_signal": 9, "workspace_id": "w-1", "message_revision": 1.5, "attachments": []any{map[string]any{"name": "log"}},
		"actor": map[string]any{"display_name": "Aoi"}, "place": map[string]any{"name": "dev", "kind": 3},
	}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, l.Generation, "t-1", 50); err != nil {
		t.Fatalf("load: %v", err)
	}
	c := PlanCall{CallID: "c", Tool: "messaging.send", Route: "normal", Request: map[string]any{"to": "aoi", "content": "z"}}
	mustPlan(t, s, pa, "t-1", l.Generation, c)
	if _, _, _, err := s.ClaimOperation(ctx, pa, "t-1", l.Generation, "t-1:op:0", c.Tool, 0, c.Request); err != nil {
		t.Fatalf("claim: %v", err)
	}
	var p map[string]any
	if err := pool.QueryRow(ctx, `SELECT payload FROM core_events WHERE persona_id = $1 AND kind = 'input_received'`, pa).Scan(&p); err != nil {
		t.Fatalf("receipt: %v", err)
	}
	want := map[string]any{
		"session_id": "ts-1", "status": "ended", "end_reason": "exit", "exit_code": float64(2),
		"exit_signal": nil, "workspace_id": "w-1", "message_revision": nil,
		"actor_display": "Aoi", "place_name": "dev", "place_kind": nil,
		"attachments": []any{map[string]any{"name": "log"}}, "message_id": nil, "attempt": float64(1),
	}
	for k, v := range want {
		got, ok := p[k]
		if !ok || fmt.Sprint(got) != fmt.Sprint(v) {
			t.Errorf("receipt %s = %v (present %v), want %v", k, got, ok, v)
		}
	}
}
