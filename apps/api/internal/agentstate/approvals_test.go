package agentstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/modelconnections"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

func mustHuman(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	h := pid(t)
	if _, err := pool.Exec(context.Background(), `INSERT INTO humans (human_id) VALUES ($1)`, h); err != nil {
		t.Fatalf("insert human: %v", err)
	}
	return h
}

type approvalFixture struct {
	s     *Store
	pool  *pgxpool.Pool
	pa    string
	human string
	gen   int64
}

// newApprovalFixture provisions a persona owned by a human, submits one
// input, opens its first turn t-1, and records a one-call plan.
func newApprovalFixture(t *testing.T, call PlanCall) *approvalFixture {
	t.Helper()
	s, pool := newStore(t)
	ctx := context.Background()
	f := &approvalFixture{s: s, pool: pool, pa: pid(t)}
	f.human = mustHuman(t, pool)
	if _, _, err := s.EnsurePersona(ctx, f.pa, &f.human, "secretary"); err != nil {
		t.Fatalf("ensure persona: %v", err)
	}
	lease, err := s.AcquireWriter(ctx, f.pa, "h1", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	f.gen = lease.Generation
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: f.pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "please"}, ActorKind: "human", ActorID: f.human}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, f.pa, f.gen, "t-1", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	mustPlan(t, s, f.pa, "t-1", f.gen, call)
	return f
}

func (f *approvalFixture) claim(t *testing.T, turnID string, index int, call PlanCall) (Operation, *ToolApproval, bool) {
	t.Helper()
	op, a, fresh, err := f.s.ClaimOperation(context.Background(), f.pa, turnID, f.gen,
		turnID+":op:"+string(rune('0'+index)), call.Tool, index, call.Request)
	if err != nil {
		t.Fatalf("claim %s[%d]: %v", turnID, index, err)
	}
	return op, a, fresh
}

func (f *approvalFixture) park(t *testing.T, call PlanCall) *ToolApproval {
	t.Helper()
	op, a, fresh := f.claim(t, "t-1", 0, call)
	if op.Status != "awaiting_approval" || a == nil || a.Status != "pending" || !fresh {
		t.Fatalf("gated claim: op=%+v approval=%+v fresh=%v", op, a, fresh)
	}
	if _, err := f.s.CommitTurn(context.Background(), f.pa, "t-1", f.gen, CommitRequest{
		Outcome: "await",
		Events: []EventInput{{Kind: "approval_requested",
			Payload: map[string]any{"approval_id": a.ApprovalID}}},
	}); err != nil {
		t.Fatalf("await commit: %v", err)
	}
	return a
}

// restart simulates the secretary process dying and a new one taking the
// persona: a new generation, recovery, and the next turn t-2.
func (f *approvalFixture) restart(t *testing.T) LoadResult {
	t.Helper()
	ctx := context.Background()
	lease, err := f.s.AcquireWriter(ctx, f.pa, "h1", time.Minute)
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	f.gen = lease.Generation
	if _, err := f.s.Recover(ctx, f.pa, f.gen); err != nil {
		t.Fatalf("recover: %v", err)
	}
	load, err := f.s.LoadTurn(ctx, f.pa, f.gen, "t-2", 10)
	if err != nil {
		t.Fatalf("load after restart: %v", err)
	}
	return load
}

func (f *approvalFixture) outboxCount(t *testing.T, kind string) int {
	t.Helper()
	entries, err := f.s.Outbox(context.Background(), f.pa, 0, 100)
	if err != nil {
		t.Fatalf("outbox: %v", err)
	}
	n := 0
	for _, e := range entries {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

func (f *approvalFixture) inputStatus(t *testing.T) string {
	t.Helper()
	in, _, err := f.s.GetInput(context.Background(), f.pa, "in-1")
	if err != nil {
		t.Fatalf("get input: %v", err)
	}
	return in.Status
}

func (f *approvalFixture) decision(decision, id string) ApprovalDecision {
	return ApprovalDecision{Decision: decision, DecisionID: id, DecidedByKind: "human", DecidedByID: f.human}
}

func TestApprovedSendRunsOnceAfterRestart(t *testing.T) {
	send := PlanCall{Tool: "message.send", Route: "elevated", Request: map[string]any{"text": "会議を15時に移します"}}
	f := newApprovalFixture(t, send)
	ctx := context.Background()

	// Only an explicit elevated call asks the human; a normal message.send
	// would be a structured block (TestNormalSendIsBlocked, below).
	op, a, fresh := f.claim(t, "t-1", 0, send)
	if op.Status != "awaiting_approval" || a == nil || a.RequiredBy != "route" || a.Route != "elevated" || !fresh {
		t.Fatalf("gated claim: op=%+v approval=%+v fresh=%v", op, a, fresh)
	}
	// A lost claim response re-finds the same request instead of minting one.
	if _, again, fresh2 := f.claim(t, "t-1", 0, send); again == nil || again.ApprovalID != a.ApprovalID || fresh2 {
		t.Fatalf("replayed gated claim: approval=%+v fresh=%v", again, fresh2)
	}
	if _, err := f.s.CommitTurn(ctx, f.pa, "t-1", f.gen, CommitRequest{Outcome: "await",
		Events: []EventInput{{Kind: "approval_requested", Payload: map[string]any{"approval_id": a.ApprovalID}}}}); err != nil {
		t.Fatalf("await commit: %v", err)
	}
	if got := f.inputStatus(t); got != "waiting" {
		t.Fatalf("input status = %s, want waiting", got)
	}
	st, err := f.s.PersonaState(ctx, f.pa)
	if err != nil || st.PendingApprovals != 1 || st.WaitingInputs != 1 || st.RunningTurn != nil {
		t.Fatalf("state while waiting: %+v err=%v", st, err)
	}
	if f.outboxCount(t, "approval_requested") != 1 || f.outboxCount(t, "secretary_message") != 0 {
		t.Fatal("waiting must surface one request and send nothing")
	}
	// A waiting input is not claimable: nothing retries behind the human.
	if load, err := f.s.LoadTurn(ctx, f.pa, f.gen, "t-idle", 10); err != nil || load.Turn != nil {
		t.Fatalf("waiting input was claimed: %+v err=%v", load.Turn, err)
	}

	// Only the persona's own human decides, and must be named.
	if _, err := f.s.ResolveApproval(ctx, f.pa, a.ApprovalID, ApprovalDecision{Decision: "approve_once",
		DecisionID: "d-x", DecidedByKind: "human", DecidedByID: pid(t)}); !errors.Is(err, ErrApprovalForbidden) {
		t.Fatalf("other human err = %v, want ErrApprovalForbidden", err)
	}
	if _, err := f.s.ResolveApproval(ctx, f.pa, a.ApprovalID, ApprovalDecision{Decision: "approve_once",
		DecisionID: "d-x", DecidedByKind: "agent", DecidedByID: f.human}); !errors.Is(err, ErrApprovalDecidedBy) {
		t.Fatalf("agent decider err = %v, want ErrApprovalDecidedBy", err)
	}
	got, err := f.s.ResolveApproval(ctx, f.pa, a.ApprovalID, f.decision("approve_once", "d-1"))
	if err != nil || got.Status != "approved" || got.Provenance == nil || *got.Provenance != "agent_own_with_human_consent" {
		t.Fatalf("approve: %+v err=%v", got, err)
	}
	if _, err := f.s.ResolveApproval(ctx, f.pa, a.ApprovalID, f.decision("approve_once", "d-1")); err != nil {
		t.Fatalf("identical decision replay: %v", err)
	}
	if _, err := f.s.ResolveApproval(ctx, f.pa, a.ApprovalID, f.decision("deny_once", "d-2")); !errors.Is(err, ErrApprovalConflict) {
		t.Fatalf("deny after approve err = %v, want ErrApprovalConflict", err)
	}
	if got := f.inputStatus(t); got != "queued" {
		t.Fatalf("input after approval = %s, want queued", got)
	}

	load := f.restart(t)
	if load.Turn == nil || load.Plan == nil {
		t.Fatalf("resume turn not loaded: %+v", load)
	}
	// Waiting on a person is not a spent attempt.
	if load.Turn.Attempt != 1 {
		t.Fatalf("resumed attempt = %d, want 1", load.Turn.Attempt)
	}
	op3, a3, fresh3 := f.claim(t, "t-2", 0, send)
	if op3.Status != "done" || !fresh3 || a3 == nil || a3.ConsumedAt == nil {
		t.Fatalf("approved claim: op=%+v approval=%+v fresh=%v", op3, a3, fresh3)
	}
	if op4, _, fresh4 := f.claim(t, "t-2", 0, send); op4.Status != "done" || fresh4 {
		t.Fatalf("approved replay: op=%+v fresh=%v", op4, fresh4)
	}
	if n := f.outboxCount(t, "secretary_message"); n != 1 {
		t.Fatalf("secretary_message count = %d, want exactly 1", n)
	}
	if _, err := f.s.CommitTurn(ctx, f.pa, "t-2", f.gen, CommitRequest{Outcome: "complete",
		Output: map[string]any{"text": "sent"}}); err != nil {
		t.Fatalf("complete: %v", err)
	}
}

func TestDeniedSendStaysDenied(t *testing.T) {
	send := PlanCall{Tool: "message.send", Route: "elevated", Request: map[string]any{"text": "全員に送信"}}
	f := newApprovalFixture(t, send)
	ctx := context.Background()
	a := f.park(t, send)
	got, err := f.s.ResolveApproval(ctx, f.pa, a.ApprovalID, f.decision("deny_once", "d-1"))
	if err != nil || got.Status != "denied" || got.Provenance != nil {
		t.Fatalf("deny: %+v err=%v", got, err)
	}
	if _, err := f.s.ResolveApproval(ctx, f.pa, a.ApprovalID, f.decision("approve_once", "d-2")); !errors.Is(err, ErrApprovalConflict) {
		t.Fatalf("approve after deny err = %v, want ErrApprovalConflict", err)
	}
	f.restart(t)
	op, replayed, _ := f.claim(t, "t-2", 0, send)
	if op.Status != "failed" || op.Response["error"] != "denied" || replayed == nil || replayed.Status != "denied" {
		t.Fatalf("denied claim: op=%+v approval=%+v", op, replayed)
	}
	if n := f.outboxCount(t, "secretary_message"); n != 0 {
		t.Fatalf("denied send delivered %d messages", n)
	}
}

func TestElevatedDenialBlocksIdenticalNormalCall(t *testing.T) {
	ctx := context.Background()
	private := map[string]any{"text": "private detail"}
	elevated := PlanCall{Tool: "journal.note", Route: "elevated", Request: private}
	f := newApprovalFixture(t, elevated)
	a := f.park(t, elevated)
	if a.RequiredBy != "route" || a.Route != "elevated" {
		t.Fatalf("elevated approval: %+v", a)
	}
	if _, err := f.s.ResolveApproval(ctx, f.pa, a.ApprovalID, f.decision("deny_once", "d-1")); err != nil {
		t.Fatalf("deny: %v", err)
	}
	f.restart(t)
	if op, _, _ := f.claim(t, "t-2", 0, elevated); op.Status != "failed" {
		t.Fatalf("denied elevated replay: %+v", op)
	}
	// The model re-proposes the identical call on its own authority.
	normal := PlanCall{Tool: "journal.note", Route: "normal", Request: private}
	if _, _, err := f.s.SavePlan(ctx, f.pa, "t-2", f.gen, 1, Decision{Text: "again", Calls: []PlanCall{normal}}); err != nil {
		t.Fatalf("save round 1: %v", err)
	}
	op, denial, fresh := f.claim(t, "t-2", 1, normal)
	if op.Status != "failed" || op.Response["error"] != "denied" || denial == nil || denial.ApprovalID != a.ApprovalID || !fresh {
		t.Fatalf("identical normal call: op=%+v approval=%+v fresh=%v", op, denial, fresh)
	}
	// A different note is within the agent's own authority and runs.
	other := PlanCall{Tool: "journal.note", Route: "normal", Request: map[string]any{"text": "public detail"}}
	if _, _, err := f.s.SavePlan(ctx, f.pa, "t-2", f.gen, 2, Decision{Text: "other", Calls: []PlanCall{other}}); err != nil {
		t.Fatalf("save round 2: %v", err)
	}
	if op, _, _ := f.claim(t, "t-2", 2, other); op.Status != "done" {
		t.Fatalf("different normal call: %+v", op)
	}
	evs, err := f.s.Events(ctx, f.pa, 0, 100)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	for _, e := range evs {
		if e.Kind == "note" && e.Payload["text"] == "private detail" {
			t.Fatal("denied note was written")
		}
	}
}

func TestDecisionBeforeParkCommitRequeues(t *testing.T) {
	ctx := context.Background()
	send := PlanCall{Tool: "message.send", Route: "elevated", Request: map[string]any{"text": "quick"}}
	f := newApprovalFixture(t, send)
	_, a, _ := f.claim(t, "t-1", 0, send)
	// The human answers before the secretary's await commit lands.
	if _, err := f.s.ResolveApproval(ctx, f.pa, a.ApprovalID, f.decision("approve_once", "d-1")); err != nil {
		t.Fatalf("early approve: %v", err)
	}
	if _, err := f.s.CommitTurn(ctx, f.pa, "t-1", f.gen, CommitRequest{Outcome: "await"}); err != nil {
		t.Fatalf("await commit: %v", err)
	}
	if got := f.inputStatus(t); got != "queued" {
		t.Fatalf("input = %s, want queued (decision already landed)", got)
	}
	if n := f.outboxCount(t, "approval_requested"); n != 0 {
		t.Fatalf("already-decided request surfaced %d times", n)
	}
}

func TestCrashWhilePendingKeepsOneRequest(t *testing.T) {
	ctx := context.Background()
	send := PlanCall{Tool: "message.send", Route: "elevated", Request: map[string]any{"text": "after crash"}}
	f := newApprovalFixture(t, send)
	_, a, _ := f.claim(t, "t-1", 0, send)
	// Crash before the await commit: the turn is still running.
	load := f.restart(t)
	if load.Turn == nil {
		t.Fatal("recovered input was not reloaded")
	}
	op, again, fresh := f.claim(t, "t-2", 0, send)
	if op.Status != "awaiting_approval" || again == nil || again.ApprovalID != a.ApprovalID || fresh {
		t.Fatalf("post-crash claim: op=%+v approval=%+v fresh=%v", op, again, fresh)
	}
	list, err := f.s.ListApprovals(ctx, f.pa, "")
	if err != nil || len(list) != 1 {
		t.Fatalf("approvals after crash: %d err=%v", len(list), err)
	}
}

func TestSavePlanRequiresRoute(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	for _, route := range []string{"", "sideways"} {
		_, _, err := s.SavePlan(ctx, pa, "t-1", lease.Generation, 0, Decision{Text: "r",
			Calls: []PlanCall{{Tool: "journal.note", Route: route, Request: map[string]any{"text": "x"}}}})
		if !errors.Is(err, ErrBadRequest) {
			t.Fatalf("route %q err = %v, want ErrBadRequest", route, err)
		}
	}
}

func TestApprovalDecisionRoute(t *testing.T) {
	send := PlanCall{Tool: "message.send", Route: "elevated", Request: map[string]any{"text": "via http"}}
	f := newApprovalFixture(t, send)
	srv := NewServer(f.pool, testAdminSecret)
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)
	a := f.park(t, send)
	base := "/internal/core/personas/" + f.pa + "/approvals"
	ptoken := srv.PersonaToken(f.pa)

	rec := do(t, mux, "GET", base+"?status=pending", ptoken, "")
	var list struct {
		Approvals []ToolApproval `json:"approvals"`
	}
	if rec.Code != 200 || json.Unmarshal(rec.Body.Bytes(), &list) != nil || len(list.Approvals) != 1 {
		t.Fatalf("list pending: %d %s", rec.Code, rec.Body)
	}
	if rec := do(t, mux, "GET", base+"/"+a.ApprovalID, ptoken, ""); rec.Code != 200 {
		t.Fatalf("get approval: %d %s", rec.Code, rec.Body)
	}
	body := `{"decision":"approve_once","decision_id":"d-1","decided_by_kind":"human","decided_by_id":"` + f.human + `"}`
	// The secretary's own token cannot consent for its human.
	if rec := do(t, mux, "POST", base+"/"+a.ApprovalID+"/decision", ptoken, body); rec.Code != 401 {
		t.Fatalf("persona-token decision status = %d", rec.Code)
	}
	wrong := `{"decision":"approve_once","decision_id":"d-1","decided_by_kind":"human","decided_by_id":"` + pid(t) + `"}`
	if rec := do(t, mux, "POST", base+"/"+a.ApprovalID+"/decision", testAdminSecret, wrong); rec.Code != 403 {
		t.Fatalf("other-human decision status = %d %s", rec.Code, rec.Body)
	}
	rec = do(t, mux, "POST", base+"/"+a.ApprovalID+"/decision", testAdminSecret, body)
	var decided struct {
		Approval ToolApproval `json:"approval"`
	}
	if rec.Code != 200 || json.Unmarshal(rec.Body.Bytes(), &decided) != nil || decided.Approval.Status != "approved" {
		t.Fatalf("decision: %d %s", rec.Code, rec.Body)
	}
	if rec := do(t, mux, "POST", base+"/"+a.ApprovalID+"/decision", testAdminSecret,
		`{"decision":"deny_once","decision_id":"d-2","decided_by_kind":"human","decided_by_id":"`+f.human+`"}`); rec.Code != 409 {
		t.Fatalf("conflicting decision status = %d", rec.Code)
	}
}

func TestNormalSendIsBlockedNotParked(t *testing.T) {
	// ADR 0013 §2: the Normal route never produces a human prompt. A
	// normal message.send is recorded as a structured block — durable,
	// replayed identically — with no approval row and no outbox ask.
	ctx := context.Background()
	send := PlanCall{Tool: "message.send", Route: "normal", Request: map[string]any{"text": "直接送信"}}
	f := newApprovalFixture(t, send)
	op, a, fresh := f.claim(t, "t-1", 0, send)
	if op.Status != "failed" || a != nil || !fresh {
		t.Fatalf("normal send: op=%+v approval=%+v fresh=%v", op, a, fresh)
	}
	if op.Response["blocked"] != "elevated_route_required" {
		t.Fatalf("block response = %+v", op.Response)
	}
	if n := f.outboxCount(t, "approval_requested"); n != 0 {
		t.Fatal("normal-route send asked the human")
	}
	if list, err := f.s.ListApprovals(ctx, f.pa, ""); err != nil || len(list) != 0 {
		t.Fatalf("approvals = %d err=%v", len(list), err)
	}
	// The block is durable: a replayed claim returns the same record.
	op2, a2, fresh2 := f.claim(t, "t-1", 0, send)
	if op2.Status != "failed" || a2 != nil || fresh2 {
		t.Fatalf("blocked replay: op=%+v approval=%+v fresh=%v", op2, a2, fresh2)
	}
	if f.outboxCount(t, "secretary_message") != 0 {
		t.Fatal("blocked send was delivered")
	}
}

func TestGatedCallWithInvalidArgsNeverParks(t *testing.T) {
	// A call that can never execute is not asked of the human and leaves
	// no approved/unconsumed grant stranded.
	ctx := context.Background()
	bad := PlanCall{Tool: "message.send", Route: "elevated", Request: map[string]any{}}
	f := newApprovalFixture(t, bad)
	op, a, fresh := f.claim(t, "t-1", 0, bad)
	if op.Status != "failed" || a != nil || !fresh {
		t.Fatalf("invalid gated claim: op=%+v approval=%+v fresh=%v", op, a, fresh)
	}
	if !strings.Contains(fmt.Sprint(op.Response["error"]), "message.send requires text") {
		t.Fatalf("invalid response = %+v", op.Response)
	}
	if list, err := f.s.ListApprovals(ctx, f.pa, ""); err != nil || len(list) != 0 {
		t.Fatalf("approvals = %d err=%v", len(list), err)
	}
}

func TestEmptyDecisionIDRejected(t *testing.T) {
	send := PlanCall{Tool: "message.send", Route: "elevated", Request: map[string]any{"text": "x"}}
	f := newApprovalFixture(t, send)
	a := f.park(t, send)
	if _, err := f.s.ResolveApproval(context.Background(), f.pa, a.ApprovalID,
		f.decision("approve_once", "")); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("empty decision_id err = %v, want ErrBadRequest", err)
	}
	// The approval is still pending and decidable.
	got, err := f.s.ResolveApproval(context.Background(), f.pa, a.ApprovalID,
		f.decision("approve_once", "d-1"))
	if err != nil || got.Status != "approved" {
		t.Fatalf("approve after empty-id rejection: %+v err=%v", got, err)
	}
}

func TestApprovalWaitTimeIsRecorded(t *testing.T) {
	// The human's thinking time accumulates durably on the input so the
	// core can exclude it from the provider retry window (F4).
	ctx := context.Background()
	send := PlanCall{Tool: "message.send", Route: "elevated", Request: map[string]any{"text": "long wait"}}
	f := newApprovalFixture(t, send)
	a := f.park(t, send)
	// Backdate the wait start so the decision lands "hours" later —
	// deterministic, no sleeping.
	if _, err := f.pool.Exec(ctx,
		`UPDATE core_inputs SET waiting_since = now() - interval '2 hours'
		 WHERE persona_id = $1 AND input_id = 'in-1'`, f.pa); err != nil {
		t.Fatalf("backdate wait: %v", err)
	}
	if _, err := f.s.ResolveApproval(ctx, f.pa, a.ApprovalID, f.decision("approve_once", "d-1")); err != nil {
		t.Fatalf("approve: %v", err)
	}
	in, _, err := f.s.GetInput(ctx, f.pa, "in-1")
	if err != nil {
		t.Fatalf("get input: %v", err)
	}
	if in.WaitedMs < int64(2*time.Hour/time.Millisecond) || in.WaitingSince != nil {
		t.Fatalf("waited_ms = %d waiting_since = %v", in.WaitedMs, in.WaitingSince)
	}
	// The accumulated wait survives a restart — it is column state, not a
	// process memory.
	f.restart(t)
	in2, _, err := f.s.GetInput(ctx, f.pa, "in-1")
	if err != nil || in2.WaitedMs != in.WaitedMs {
		t.Fatalf("waited_ms after restart = %d err=%v", in2.WaitedMs, err)
	}
}

func TestModelBindingFollowsSelection(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Create(t)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	conns, err := modelconnections.New(pool, key)
	if err != nil {
		t.Fatalf("connections: %v", err)
	}
	srv := NewServer(pool, testAdminSecret)
	srv.SetModelConnections(conns)
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	human := mustHuman(t, pool)
	pa := pid(t)
	if _, _, err := srv.store.EnsurePersona(ctx, pa, &human, "secretary"); err != nil {
		t.Fatalf("persona: %v", err)
	}
	get := func(token string) (int, ModelBinding) {
		t.Helper()
		rec := do(t, mux, "GET", "/internal/core/personas/"+pa+"/model", token, "")
		var b ModelBinding
		if rec.Code == 200 {
			if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil {
				t.Fatalf("decode binding: %v", err)
			}
		}
		return rec.Code, b
	}
	token := srv.PersonaToken(pa)

	if _, b := get(token); b.Selection != "unset" {
		t.Fatalf("no selection = %+v", b)
	}
	apiKey := "sk-binding-test"
	conn, err := conns.Save(ctx, human, "", modelconnections.Input{Name: "Work", Preset: "openai-chat",
		BaseURL: "https://api.example.com/v1", Model: "model-a", APIKey: &apiKey,
		ExtraHeaders: map[string]string{"X-Gateway-Session": "gw-9"}})
	if err != nil {
		t.Fatalf("save connection: %v", err)
	}
	connID := conn.ID
	if err := conns.Select(ctx, human, modelconnections.Selection{Kind: "api", ConnectionID: connID}); err != nil {
		t.Fatalf("select: %v", err)
	}
	_, b := get(token)
	if b.Selection != "api" || b.Connection == nil || b.Connection.ID != connID || b.Connection.Model != "model-a" ||
		b.Connection.Preset != "openai-chat" || b.Connection.Version == "" || b.APIKey != apiKey || !b.CredentialAvailable {
		t.Fatalf("api binding = %+v", b)
	}
	if b.Connection.ExtraHeaders["X-Gateway-Session"] != "gw-9" {
		t.Fatalf("binding dropped the connection's extra headers = %+v", b.Connection)
	}

	// Without the credential key the selection still names the connection
	// but carries no key or headers — the core must fail rather than pick
	// another.
	srv.SetModelConnections(modelconnections.MetadataOnly(pool))
	if _, b := get(token); b.Selection != "api" || b.Connection == nil || b.APIKey != "" ||
		b.CredentialAvailable || b.Reason == "" || len(b.Connection.ExtraHeaders) != 0 {
		t.Fatalf("metadata-only binding = %+v", b)
	}
	srv.SetModelConnections(conns)

	if err := conns.Select(ctx, human, modelconnections.Selection{Kind: "none"}); err != nil {
		t.Fatalf("select none: %v", err)
	}
	if _, b := get(token); b.Selection != "none" || b.Connection != nil || b.APIKey != "" {
		t.Fatalf("none binding = %+v", b)
	}

	// Another persona's token cannot read this persona's key.
	pb := pid(t)
	if _, _, err := srv.store.EnsurePersona(ctx, pb, &human, "other"); err != nil {
		t.Fatalf("persona b: %v", err)
	}
	if code, _ := get(srv.PersonaToken(pb)); code != 401 {
		t.Fatalf("cross-persona binding status = %d", code)
	}
}

// TestDenialMatchShapes documents the exact re-issue surface a denial covers
// (f37: the two final reviews disagreed on whether route or call position
// changes the match). Verified on real PG:
//   - The durable denial match is (persona, input, tool, request) on the
//     non-gated claim path — request is jsonb semantic equality, so key order
//     is irrelevant — with route and call_index not part of the key.
//   - A same-index replay returns the finalized failed op as-is, on either
//     route (the op record itself is the receipt).
//   - A normal-route re-proposal of the identical call at any index is
//     finalized failed with the stored denial — no second prompt, no bypass.
//   - An elevated re-proposal at a different index is a NEW decision
//     instance under deny_once: it parks a fresh pending approval and the
//     human is asked again. Nothing runs without new consent.
//   - A new input proposing the identical call is likewise a new instance —
//     the denial is input-scoped, never a standing policy.
//   - A different request on the same input is unaffected.
func TestDenialMatchShapes(t *testing.T) {
	ctx := context.Background()
	private := map[string]any{"text": "private detail", "channel": "all"}
	elevated := PlanCall{Tool: "journal.note", Route: "elevated", Request: private}
	f := newApprovalFixture(t, elevated)
	a := f.park(t, elevated)
	if _, err := f.s.ResolveApproval(ctx, f.pa, a.ApprovalID, f.decision("deny_once", "d-1")); err != nil {
		t.Fatalf("deny: %v", err)
	}
	f.restart(t)

	// Same input, same index: the finalized failed op replays as-is.
	if op, got, fresh := f.claim(t, "t-2", 0, elevated); op.Status != "failed" ||
		op.Response["error"] != "denied" || got == nil || got.ApprovalID != a.ApprovalID || fresh {
		t.Fatalf("same-index replay: op=%+v approval=%+v fresh=%v", op, got, fresh)
	}

	// Same input, different index, elevated route: a NEW plan proposal is a
	// fresh decision instance — deny_once is one-shot, so the call parks a
	// brand-new pending approval and re-asks the human. It is not a bypass:
	// nothing executes without fresh consent. The denial record itself is
	// only consulted on the non-gated path and by same-index op replay.
	reElevated := PlanCall{Tool: "journal.note", Route: "elevated", Request: private}
	if _, _, err := f.s.SavePlan(ctx, f.pa, "t-2", f.gen, 1,
		Decision{Text: "retry elevated", Calls: []PlanCall{reElevated}}); err != nil {
		t.Fatalf("save elevated retry: %v", err)
	}
	op, a2e, fresh := f.claim(t, "t-2", 1, reElevated)
	if op.Status != "awaiting_approval" || a2e == nil || a2e.Status != "pending" ||
		a2e.ApprovalID == a.ApprovalID || !fresh {
		t.Fatalf("elevated different-index: op=%+v approval=%+v fresh=%v", op, a2e, fresh)
	}
	// Denying the re-asked call again keeps the normal-route bypass closed.
	if _, err := f.s.ResolveApproval(ctx, f.pa, a2e.ApprovalID, f.decision("deny_once", "d-2")); err != nil {
		t.Fatalf("second deny: %v", err)
	}

	// Same input, normal route: the identical request stays refused — a
	// denial cannot be bypassed by lowering the route.
	reNormal := PlanCall{Tool: "journal.note", Route: "normal", Request: private}
	if _, _, err := f.s.SavePlan(ctx, f.pa, "t-2", f.gen, 2,
		Decision{Text: "retry normal", Calls: []PlanCall{reNormal}}); err != nil {
		t.Fatalf("save normal retry: %v", err)
	}
	if op, got, _ := f.claim(t, "t-2", 2, reNormal); op.Status != "failed" ||
		op.Response["error"] != "denied" || got == nil || got.ApprovalID != a.ApprovalID {
		t.Fatalf("normal same-request: op=%+v approval=%+v", op, got)
	}

	// Same input, semantically identical request under a different key
	// order: jsonb equality makes it the same denied call.
	reordered := PlanCall{Tool: "journal.note", Route: "normal",
		Request: map[string]any{"channel": "all", "text": "private detail"}}
	if _, _, err := f.s.SavePlan(ctx, f.pa, "t-2", f.gen, 3,
		Decision{Text: "retry reordered", Calls: []PlanCall{reordered}}); err != nil {
		t.Fatalf("save reordered retry: %v", err)
	}
	if op, got, _ := f.claim(t, "t-2", 3, reordered); op.Status != "failed" ||
		op.Response["error"] != "denied" || got == nil || got.ApprovalID != a.ApprovalID {
		t.Fatalf("reordered same-request: op=%+v approval=%+v", op, got)
	}

	// Same input, a genuinely different request: not the denied call, so
	// the agent's own authority runs it.
	other := PlanCall{Tool: "journal.note", Route: "normal",
		Request: map[string]any{"text": "different note"}}
	if _, _, err := f.s.SavePlan(ctx, f.pa, "t-2", f.gen, 4,
		Decision{Text: "other", Calls: []PlanCall{other}}); err != nil {
		t.Fatalf("save other: %v", err)
	}
	if op, _, _ := f.claim(t, "t-2", 4, other); op.Status != "done" {
		t.Fatalf("different request: %+v", op)
	}

	// A new input proposing the identical denied call is not covered: the
	// denial is scoped to the input the human refused. The elevated call
	// parks a brand-new pending approval rather than replaying the denial.
	if _, err := f.s.CommitTurn(ctx, f.pa, "t-2", f.gen,
		CommitRequest{Outcome: "complete", Output: map[string]any{"text": "done"}}); err != nil {
		t.Fatalf("complete t-2: %v", err)
	}
	if _, _, err := f.s.SubmitInput(ctx, &Input{PersonaID: f.pa, InputID: "in-2",
		Kind: "message", Payload: map[string]any{"text": "again"},
		ActorKind: "human", ActorID: f.human}); err != nil {
		t.Fatalf("submit in-2: %v", err)
	}
	if _, err := f.s.LoadTurn(ctx, f.pa, f.gen, "t-3", 10); err != nil {
		t.Fatalf("load t-3: %v", err)
	}
	mustPlan(t, f.s, f.pa, "t-3", f.gen, elevated)
	op, a2, fresh := f.claim(t, "t-3", 0, elevated)
	if op.Status != "awaiting_approval" || a2 == nil || a2.Status != "pending" ||
		a2.ApprovalID == a.ApprovalID || !fresh {
		t.Fatalf("new input re-issue: op=%+v approval=%+v fresh=%v", op, a2, fresh)
	}
	if a2.InputID != "in-2" {
		t.Fatalf("new approval input = %s, want in-2", a2.InputID)
	}
}

// TestApprovedGrantWithoutEffectSettlesFailed covers f44: a gated tool that
// is registered but has no in-store effect must not consume the grant and
// leave the operation parked awaiting_approval forever. The grant is spent
// and the operation settles as an honest deterministic failure instead.
// toolAuthority is a package-level registry — the phantom tool is added for
// the test and removed on cleanup.
func TestApprovedGrantWithoutEffectSettlesFailed(t *testing.T) {
	toolAuthority["phantom.gated"] = struct {
		internal         bool
		requiresApproval bool
		elevatedOnly     bool
	}{internal: true, elevatedOnly: true}
	t.Cleanup(func() { delete(toolAuthority, "phantom.gated") })

	ctx := context.Background()
	call := PlanCall{Tool: "phantom.gated", Route: "elevated",
		Request: map[string]any{"text": "anything"}}
	f := newApprovalFixture(t, call)
	a := f.park(t, call)
	if _, err := f.s.ResolveApproval(ctx, f.pa, a.ApprovalID, f.decision("approve_once", "d-1")); err != nil {
		t.Fatalf("approve: %v", err)
	}
	f.restart(t)
	op, grant, fresh := f.claim(t, "t-2", 0, call)
	if op.Status != "failed" || grant == nil || grant.ConsumedAt == nil || !fresh {
		t.Fatalf("phantom effect claim: op=%+v grant=%+v fresh=%v", op, grant, fresh)
	}
	if errStr, _ := op.Response["error"].(string); !strings.Contains(errStr, "no registered effect") {
		t.Fatalf("response = %+v, want a no-effect deterministic failure", op.Response)
	}
	// Replays return the finalized failure; the grant is never re-armed.
	op2, grant2, fresh2 := f.claim(t, "t-2", 0, call)
	if op2.Status != "failed" || fresh2 || grant2 == nil || grant2.ConsumedAt == nil {
		t.Fatalf("replay: op=%+v grant=%+v fresh=%v", op2, grant2, fresh2)
	}
}
