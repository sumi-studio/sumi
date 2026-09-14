package messaging

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

// coreApprovalWorld wires the real core store and Messaging store onto one
// database so the browser-facing adapter tests exercise the same authority
// path production does: session cookie → claims → persona's bound human.
type coreApprovalWorld struct {
	world
	core   *agentstate.Store
	server *CoreApprovalsServer
	ts     *httptest.Server
}

func newCoreApprovalWorld(t *testing.T, ctx context.Context) *coreApprovalWorld {
	t.Helper()
	w := newWorld(t, ctx)
	for _, participant := range []ParticipantRef{w.humanA, w.humanB, w.agent} {
		if err := w.store.seedDefaultWorkspaceFixture(ctx, participant); err != nil {
			t.Fatalf("prepare default test Workspace: %v", err)
		}
	}
	core := agentstate.NewStore(w.store.core.pool)
	server := &CoreApprovalsServer{
		Core:           core,
		Messaging:      w.store.core,
		Hub:            NewHub(w.store.core),
		Sessions:       stubSessions{},
		AllowedOrigins: []string{testOrigin},
	}
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return &coreApprovalWorld{world: w, core: core, server: server, ts: ts}
}

// parkAgentApproval drives the secretary's core state to a pending approval:
// the persona bound to humanA submits an input, plans one elevated send, and
// the claim parks behind consent.
func (cw *coreApprovalWorld) parkAgentApproval(t *testing.T, ctx context.Context) *agentstate.ToolApproval {
	t.Helper()
	pa := cw.agent.ID
	human := cw.humanA.ID
	if _, _, err := cw.core.EnsurePersona(ctx, pa, &human, "Kuro"); err != nil {
		t.Fatalf("ensure persona: %v", err)
	}
	lease, err := cw.core.AcquireWriter(ctx, pa, "h1", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := cw.core.SubmitInput(ctx, &agentstate.Input{
		PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{
			"text":         "みんなにお知らせして",
			"workspace_id": DefaultWorkspaceID,
			"actor":        map[string]any{"kind": "human", "id": human, "display_name": "Yohaku"},
			"place":        map[string]any{"id": DefaultGeneralChannelID, "kind": "channel", "name": "general"},
		},
		ActorKind: "human", ActorID: human, SourceSurface: "messaging",
		ThreadID: DefaultGeneralChannelID,
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := cw.core.LoadTurn(ctx, pa, lease.Generation, "t-1", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	call := agentstate.PlanCall{Tool: "message.send", Route: "elevated",
		Request: map[string]any{"text": "会議は15時です"}}
	if _, created, err := cw.core.SavePlan(ctx, pa, "t-1", lease.Generation, 0,
		agentstate.Decision{Text: "reply", Calls: []agentstate.PlanCall{call}}); err != nil || !created {
		t.Fatalf("plan: created=%v err=%v", created, err)
	}
	op, a, fresh, err := cw.core.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"t-1:op:0", call.Tool, 0, call.Request)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if op.Status != "awaiting_approval" || a == nil || a.Status != "pending" || !fresh {
		t.Fatalf("gated claim: op=%+v approval=%+v fresh=%v", op, a, fresh)
	}
	if _, err := cw.core.CommitTurn(ctx, pa, "t-1", lease.Generation, agentstate.CommitRequest{
		Outcome: "await",
		Events: []agentstate.EventInput{{Kind: "approval_requested",
			Payload: map[string]any{"approval_id": a.ApprovalID}}},
	}); err != nil {
		t.Fatalf("await commit: %v", err)
	}
	return a
}

// approvalRequest issues a browser-shaped call: session cookie plus the
// allowed origin on unsafe methods.
func approvalRequest(t *testing.T, ts *httptest.Server, method, path, cookie string, body string) *http.Response {
	t.Helper()
	var r *http.Request
	var err error
	if body == "" {
		r, err = http.NewRequest(method, ts.URL+path, nil)
	} else {
		r, err = http.NewRequest(method, ts.URL+path, strings.NewReader(body))
	}
	if err != nil {
		t.Fatal(err)
	}
	if cookie != "" {
		r.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: cookie})
	}
	if method != http.MethodGet {
		r.Header.Set("Origin", testOrigin)
		if body != "" {
			r.Header.Set("Content-Type", "application/json")
		}
	}
	resp, err := ts.Client().Do(r)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

func readJSON(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return out
}

func TestCoreApprovalsInboxRequiresSession(t *testing.T) {
	ctx := context.Background()
	cw := newCoreApprovalWorld(t, ctx)
	cw.parkAgentApproval(t, ctx)

	for _, tc := range []struct {
		name   string
		cookie string
		want   int
	}{
		{"no cookie", "", http.StatusUnauthorized},
		{"revoked session", "revoked:" + cw.humanA.ID, http.StatusUnauthorized},
		{"stranger human", cw.humanB.ID, http.StatusOK},
		{"bound human", cw.humanA.ID, http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := approvalRequest(t, cw.ts, http.MethodGet, "/me/approvals", tc.cookie, "")
			body := readJSON(t, resp)
			if resp.StatusCode != tc.want {
				t.Fatalf("status = %d body=%v", resp.StatusCode, body)
			}
		})
	}

	// A human who is not the persona's owner sees an empty inbox, never the
	// parked grant.
	resp := approvalRequest(t, cw.ts, http.MethodGet, "/me/approvals", cw.humanB.ID, "")
	body := readJSON(t, resp)
	if got, _ := body["approvals"].([]any); len(got) != 0 {
		t.Fatalf("humanB inbox leaked %d approvals", len(got))
	}

	resp = approvalRequest(t, cw.ts, http.MethodGet, "/me/approvals", cw.humanA.ID, "")
	body = readJSON(t, resp)
	list, _ := body["approvals"].([]any)
	if len(list) != 1 {
		t.Fatalf("humanA inbox = %v", body)
	}
	row, _ := list[0].(map[string]any)
	if row["status"] != "pending" || row["tool"] != "message.send" ||
		row["route"] != "elevated" || row["secretary_name"] != "Kuro" {
		t.Fatalf("inbox row = %v", row)
	}
	input, _ := row["input"].(map[string]any)
	place, _ := input["place"].(map[string]any)
	if input["text"] != "みんなにお知らせして" || place["name"] != "general" ||
		input["actor_name"] != "Yohaku" {
		t.Fatalf("input provenance = %v", input)
	}
	if _, ok := row["action_digest"].(string); !ok {
		t.Fatal("inbox row carries no action digest")
	}
}

func TestCoreApprovalDecisionAuthority(t *testing.T) {
	ctx := context.Background()
	cw := newCoreApprovalWorld(t, ctx)
	a := cw.parkAgentApproval(t, ctx)
	path := "/me/approvals/" + a.ApprovalID + "/decision"

	// The decision body never names the decider — actor fields are rejected.
	resp := approvalRequest(t, cw.ts, http.MethodPost, path, cw.humanA.ID,
		`{"decision":"approve_once","decision_id":"d-x","actor_id":"`+cw.humanB.ID+`"}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("actor-supplied body status = %d", resp.StatusCode)
	}
	_ = readJSON(t, resp)

	// Origin enforcement on unsafe methods.
	req, _ := http.NewRequest(http.MethodPost, cw.ts.URL+path,
		strings.NewReader(`{"decision":"approve_once","decision_id":"d-x"}`))
	req.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: cw.humanA.ID})
	req.Header.Set("Origin", "https://evil.example")
	resp, err := cw.ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin status = %d", resp.StatusCode)
	}
	_ = readJSON(t, resp)

	// A copied approval id in another human's session cannot act.
	resp = approvalRequest(t, cw.ts, http.MethodPost, path, cw.humanB.ID,
		`{"decision":"deny_once","decision_id":"d-b"}`)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("other human status = %d body=%v", resp.StatusCode, readJSON(t, resp))
	} else {
		_ = readJSON(t, resp)
	}

	// The bound human approves once.
	resp = approvalRequest(t, cw.ts, http.MethodPost, path, cw.humanA.ID,
		`{"decision":"approve_once","decision_id":"d-1"}`)
	body := readJSON(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("approve status = %d body=%v", resp.StatusCode, body)
	}
	got, _ := body["approval"].(map[string]any)
	if got["status"] != "approved" || got["decided_by_id"] != cw.humanA.ID {
		t.Fatalf("approved row = %v", got)
	}

	// Identical replay returns the stored decision — no double execution.
	resp = approvalRequest(t, cw.ts, http.MethodPost, path, cw.humanA.ID,
		`{"decision":"approve_once","decision_id":"d-1"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("replay status = %d", resp.StatusCode)
	}
	_ = readJSON(t, resp)

	// A conflicting decision on the resolved approval is refused.
	resp = approvalRequest(t, cw.ts, http.MethodPost, path, cw.humanA.ID,
		`{"decision":"deny_once","decision_id":"d-2"}`)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("conflict status = %d", resp.StatusCode)
	}
	_ = readJSON(t, resp)

	// An approval id that exists for no persona is not found.
	resp = approvalRequest(t, cw.ts, http.MethodPost,
		"/me/approvals/"+a.ApprovalID[:len(a.ApprovalID)-2]+"ff/decision",
		cw.humanA.ID, `{"decision":"approve_once","decision_id":"d-3"}`)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown approval status = %d", resp.StatusCode)
	}
	_ = readJSON(t, resp)
}

// The parked→decided nudge reaches only the deciding human's live Messaging
// subscribers — other members of the same Workspace see nothing.
func TestCoreApprovalChangedFanout(t *testing.T) {
	ctx := context.Background()
	cw := newCoreApprovalWorld(t, ctx)
	a := cw.parkAgentApproval(t, ctx)

	humanAStore := cw.store.mustScopeForActor(t, ctx, cw.humanA)
	humanBStore := cw.store.mustScopeForActor(t, ctx, cw.humanB)
	subA := cw.server.Hub.subscribe(humanAStore)
	defer cw.server.Hub.unsubscribe(subA)
	subB := cw.server.Hub.subscribe(humanBStore)
	defer cw.server.Hub.unsubscribe(subB)

	cw.server.NotifyChanged(ctx, cw.agent.ID)

	select {
	case frame := <-subA.send:
		if !strings.Contains(string(frame.payload), EventCoreApprovalChanged) {
			t.Fatalf("humanA frame = %s", frame.payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("humanA never received the approval nudge")
	}
	select {
	case frame := <-subB.send:
		t.Fatalf("humanB received another human's approval nudge: %s", frame.payload)
	case <-time.After(200 * time.Millisecond):
	}

	// A decision commit fires the same nudge so other tabs converge.
	if _, err := cw.core.ResolveApproval(ctx, cw.agent.ID, a.ApprovalID, agentstate.ApprovalDecision{
		Decision: "deny_once", DecisionID: "d-1",
		DecidedByKind: "human", DecidedByID: cw.humanA.ID,
	}); err != nil {
		t.Fatalf("deny: %v", err)
	}
	cw.server.NotifyChanged(ctx, cw.agent.ID)
	select {
	case frame := <-subA.send:
		if !strings.Contains(string(frame.payload), EventCoreApprovalChanged) {
			t.Fatalf("resolve frame = %s", frame.payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("humanA never received the decision nudge")
	}
}
