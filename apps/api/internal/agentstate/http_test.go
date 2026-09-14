package agentstate

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

const testAdminSecret = "test-admin-secret-0123456789abcdef"

func newHTTPServer(t *testing.T) (*Server, *http.ServeMux) {
	t.Helper()
	pool := testdb.Create(t)
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	srv := NewServer(pool, testAdminSecret)
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)
	return srv, mux
}

func do(t *testing.T, mux *http.ServeMux, method, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func TestScopedCapabilityAuth(t *testing.T) {
	srv, mux := newHTTPServer(t)

	pa, pb := pid(t), pid(t)
	// Persona routes reject missing auth.
	if rec := do(t, mux, "GET", "/internal/core/personas/"+pa+"/state", "", ""); rec.Code != 401 {
		t.Fatalf("no-auth status = %d", rec.Code)
	}
	// Persona provisioning requires the admin secret.
	if rec := do(t, mux, "POST", "/internal/core/personas", "wrong", `{"persona_id":"`+pa+`"}`); rec.Code != 401 {
		t.Fatalf("bad-admin status = %d", rec.Code)
	}
	rec := do(t, mux, "POST", "/internal/core/personas", testAdminSecret, `{"persona_id":"`+pa+`","display_name":"A"}`)
	if rec.Code != 201 {
		t.Fatalf("create status = %d body=%s", rec.Code, rec.Body)
	}
	var created struct {
		Persona      Persona `json:"persona"`
		Created      bool    `json:"created"`
		PersonaToken string  `json:"persona_token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !created.Created || created.PersonaToken == "" {
		t.Fatalf("create: %+v", created)
	}
	// Idempotent re-provision returns the same token.
	rec = do(t, mux, "POST", "/internal/core/personas", testAdminSecret, `{"persona_id":"`+pa+`"}`)
	if rec.Code != 200 {
		t.Fatalf("re-create status = %d", rec.Code)
	}
	var again struct {
		PersonaToken string `json:"persona_token"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &again)
	if again.PersonaToken != created.PersonaToken {
		t.Fatalf("persona token not stable")
	}

	// Persona A's token reads A, writes A, and cannot touch persona B.
	tokenA := created.PersonaToken
	if rec := do(t, mux, "GET", "/internal/core/personas/"+pa+"/state", tokenA, ""); rec.Code != 200 {
		t.Fatalf("own-scope read = %d", rec.Code)
	}
	do(t, mux, "POST", "/internal/core/personas", testAdminSecret, `{"persona_id":"`+pb+`"}`)
	if rec := do(t, mux, "GET", "/internal/core/personas/"+pb+"/state", tokenA, ""); rec.Code != 401 {
		t.Fatalf("cross-persona read = %d, want 401", rec.Code)
	}
	if rec := do(t, mux, "POST", "/internal/core/personas/"+pb+"/inputs", tokenA,
		`{"input_id":"i","kind":"message","payload":{"text":"x"}}`); rec.Code != 401 {
		t.Fatalf("cross-persona write = %d, want 401", rec.Code)
	}
	if rec := do(t, mux, "POST", "/internal/core/personas", tokenA, `{"persona_id":"`+pid(t)+`"}`); rec.Code != 401 {
		t.Fatalf("persona token must not provision: %d", rec.Code)
	}
	// Derived token must equal the server's own derivation.
	if srv.PersonaToken(pa) != tokenA {
		t.Fatalf("derivation mismatch")
	}
}

func TestHTTPInputToCommitFlow(t *testing.T) {
	_, mux := newHTTPServer(t)
	pa := pid(t)
	rec := do(t, mux, "POST", "/internal/core/personas", testAdminSecret, `{"persona_id":"`+pa+`"}`)
	var created struct {
		PersonaToken string `json:"persona_token"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &created)
	tok := created.PersonaToken

	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/writer/acquire", tok,
		`{"holder_id":"h1","ttl_ms":60000}`)
	if rec.Code != 200 {
		t.Fatalf("acquire: %d %s", rec.Code, rec.Body)
	}
	var lease WriterLease
	_ = json.Unmarshal(rec.Body.Bytes(), &lease)

	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/inputs", tok,
		`{"input_id":"i1","kind":"message","payload":{"text":"hi"},"actor_kind":"human","actor_id":"h-1","attention":"reply"}`)
	if rec.Code != 201 {
		t.Fatalf("submit: %d %s", rec.Code, rec.Body)
	}
	// Identical resubmit replays the stored row.
	if rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/inputs", tok,
		`{"input_id":"i1","kind":"message","payload":{"text":"hi"},"actor_kind":"human","actor_id":"h-1","attention":"reply"}`); rec.Code != 200 {
		t.Fatalf("identical resubmit: %d %s", rec.Code, rec.Body)
	}
	// A different request under the same input_id conflicts — it must not
	// be answered with the first request's receipt.
	if rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/inputs", tok,
		`{"input_id":"i1","kind":"message","payload":{"text":"different"},"actor_kind":"human","actor_id":"h-1","attention":"reply"}`); rec.Code != 409 {
		t.Fatalf("divergent resubmit: %d, want 409", rec.Code)
	}
	// The sched: prefix is reserved for dispatch-generated wake inputs.
	if rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/inputs", tok,
		`{"input_id":"sched:x","kind":"message","payload":{}}`); rec.Code != 400 {
		t.Fatalf("sched: input: %d, want 400", rec.Code)
	}

	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/turns/load", tok,
		`{"generation":`+itoa(lease.Generation)+`,"context_limit":10}`)
	if rec.Code != 200 {
		t.Fatalf("load: %d %s", rec.Code, rec.Body)
	}
	var load LoadResult
	_ = json.Unmarshal(rec.Body.Bytes(), &load)
	if load.Turn == nil || load.Input == nil || load.Input.InputID != "i1" {
		t.Fatalf("load: %+v", load)
	}

	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/turns/"+load.Turn.TurnID+"/commit", tok,
		`{"generation":`+itoa(lease.Generation)+`,"outcome":"complete",
		  "events":[{"kind":"assistant_message","payload":{"text":"hey"}}],
		  "output":{"text":"hey"},"usage":{"input_tokens":1}}`)
	if rec.Code != 200 {
		t.Fatalf("commit: %d %s", rec.Code, rec.Body)
	}

	rec = do(t, mux, "GET", "/internal/core/personas/"+pa+"/inputs/i1", tok, "")
	var got struct {
		Input Input `json:"input"`
		Turn  *Turn `json:"turn"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	if got.Input.Status != "done" || got.Turn == nil || got.Turn.Status != "done" {
		t.Fatalf("get input: %+v", got)
	}
	rec = do(t, mux, "GET", "/internal/core/personas/"+pa+"/outbox", tok, "")
	if !strings.Contains(rec.Body.String(), "turn_completed") {
		t.Fatalf("outbox: %s", rec.Body)
	}
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

func TestHTTPBoundaryValidation(t *testing.T) {
	_, mux := newHTTPServer(t)
	pa := pid(t)
	rec := do(t, mux, "POST", "/internal/core/personas", testAdminSecret, `{"persona_id":"`+pa+`"}`)
	var created struct {
		PersonaToken string `json:"persona_token"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &created)
	tok := created.PersonaToken

	// Malformed persona id in path is a client error, not a DB 500.
	if rec := do(t, mux, "GET", "/internal/core/personas/not-a-uuid/state", testAdminSecret, ""); rec.Code != 400 {
		t.Fatalf("bad persona path: %d, want 400", rec.Code)
	}
	// Missing generation on mutation routes is 400, not a fencing 409.
	for _, path := range []string{"/turns/load", "/writer/renew", "/writer/release", "/recover"} {
		if rec := do(t, mux, "POST", "/internal/core/personas/"+pa+path, tok, `{}`); rec.Code != 400 {
			t.Fatalf("%s without generation: %d, want 400", path, rec.Code)
		}
	}
	// Missing required fields on input/operation submit.
	if rec := do(t, mux, "POST", "/internal/core/personas/"+pa+"/inputs", tok, `{"kind":"message"}`); rec.Code != 400 {
		t.Fatalf("input missing fields: %d, want 400", rec.Code)
	}
	if rec := do(t, mux, "POST", "/internal/core/personas/"+pa+"/operations/claim", tok,
		`{"generation":1,"tool":"journal.note"}`); rec.Code != 400 {
		t.Fatalf("claim missing fields: %d, want 400", rec.Code)
	}
}

func TestHTTPPlanAndClaimBoundary(t *testing.T) {
	_, mux := newHTTPServer(t)
	pa := pid(t)
	rec := do(t, mux, "POST", "/internal/core/personas", testAdminSecret, `{"persona_id":"`+pa+`"}`)
	var created struct {
		PersonaToken string `json:"persona_token"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &created)
	tok := created.PersonaToken

	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/writer/acquire", tok,
		`{"holder_id":"h1","ttl_ms":60000}`)
	var lease WriterLease
	_ = json.Unmarshal(rec.Body.Bytes(), &lease)
	gen := itoa(lease.Generation)
	do(t, mux, "POST", "/internal/core/personas/"+pa+"/inputs", tok,
		`{"input_id":"i1","kind":"message","payload":{"text":"hi"}}`)
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/turns/load", tok,
		`{"generation":`+gen+`}`)
	var load LoadResult
	_ = json.Unmarshal(rec.Body.Bytes(), &load)
	if load.Plan != nil {
		t.Fatalf("fresh input should have no plan: %+v", load.Plan)
	}
	turnID := load.Turn.TurnID

	// Missing calls array → 400.
	if rec := do(t, mux, "POST", "/internal/core/personas/"+pa+"/turns/plan", tok,
		`{"generation":`+gen+`,"turn_id":"`+turnID+`","round":0,"text":"hi"}`); rec.Code != 400 {
		t.Fatalf("plan without calls: %d, want 400", rec.Code)
	}
	// Save the decision.
	planBody := `{"generation":` + gen + `,"turn_id":"` + turnID + `","round":0,"text":"noted",
		"calls":[{"tool":"journal.note","request":{"text":"keep me"}}]}`
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/turns/plan", tok, planBody)
	if rec.Code != 200 {
		t.Fatalf("save plan: %d %s", rec.Code, rec.Body)
	}
	var saved struct {
		Plan    TurnPlan `json:"plan"`
		Created bool     `json:"created"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &saved)
	if !saved.Created || saved.Plan.InputID != "i1" || len(saved.Plan.Plan) != 1 || saved.Plan.Plan[0].Text != "noted" {
		t.Fatalf("saved plan: %+v", saved)
	}
	// Identical resave replays.
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/turns/plan", tok, planBody)
	var resaved struct {
		Created bool `json:"created"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &resaved)
	if rec.Code != 200 || resaved.Created {
		t.Fatalf("identical resave: %d created=%v", rec.Code, resaved.Created)
	}
	// Divergent resave conflicts.
	if rec := do(t, mux, "POST", "/internal/core/personas/"+pa+"/turns/plan", tok,
		`{"generation":`+gen+`,"turn_id":"`+turnID+`","round":0,"text":"other","calls":[]}`); rec.Code != 409 {
		t.Fatalf("divergent plan: %d, want 409", rec.Code)
	}
	// Load replay surfaces the plan.
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/turns/load", tok, `{"generation":`+gen+`}`)
	_ = json.Unmarshal(rec.Body.Bytes(), &load)
	if load.Plan == nil || len(load.Plan.Plan) != 1 || len(load.Plan.Plan[0].Calls) != 1 {
		t.Fatalf("load replay plan: %+v", load.Plan)
	}

	// Claim without call_index → 400; a spoofed legacy idempotency_key is
	// an unknown field → 400, never an identity.
	if rec := do(t, mux, "POST", "/internal/core/personas/"+pa+"/operations/claim", tok,
		`{"generation":`+gen+`,"operation_id":"o1","turn_id":"`+turnID+`","tool":"journal.note","request":{"text":"keep me"}}`); rec.Code != 400 {
		t.Fatalf("claim without call_index: %d, want 400", rec.Code)
	}
	if rec := do(t, mux, "POST", "/internal/core/personas/"+pa+"/operations/claim", tok,
		`{"generation":`+gen+`,"operation_id":"o1","turn_id":"`+turnID+`","tool":"journal.note","call_index":0,"idempotency_key":"spoofed","request":{"text":"keep me"}}`); rec.Code != 400 {
		t.Fatalf("claim with legacy key: %d, want 400 (unknown field)", rec.Code)
	}
	// On-plan claim executes.
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/operations/claim", tok,
		`{"generation":`+gen+`,"operation_id":"o1","turn_id":"`+turnID+`","tool":"journal.note","call_index":0,"request":{"text":"keep me"}}`)
	if rec.Code != 200 {
		t.Fatalf("on-plan claim: %d %s", rec.Code, rec.Body)
	}
	var claimed struct {
		Operation Operation `json:"operation"`
		Fresh     bool      `json:"fresh"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &claimed)
	if !claimed.Fresh || claimed.Operation.IdempotencyKey != "i1:tool:0" {
		t.Fatalf("claim: %+v", claimed)
	}
	// Same position under a different operation_id replays the receipt.
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/operations/claim", tok,
		`{"generation":`+gen+`,"operation_id":"o2","turn_id":"`+turnID+`","tool":"journal.note","call_index":0,"request":{"text":"keep me"}}`)
	_ = json.Unmarshal(rec.Body.Bytes(), &claimed)
	if rec.Code != 200 || claimed.Fresh || claimed.Operation.OperationID != "o1" {
		t.Fatalf("replay claim: %d %+v", rec.Code, claimed)
	}
	// Off-plan position → 409.
	if rec := do(t, mux, "POST", "/internal/core/personas/"+pa+"/operations/claim", tok,
		`{"generation":`+gen+`,"operation_id":"o3","turn_id":"`+turnID+`","tool":"journal.note","call_index":1,"request":{"text":"x"}}`); rec.Code != 409 {
		t.Fatalf("off-plan claim: %d, want 409", rec.Code)
	}
}

// CR3-B1 over HTTP: deterministic bad tool data is a 400, never a
// transient-looking 500 — so the secretary records a tool error and the
// input resolves instead of blocking the queue.
func TestHTTPDeterministicToolData400(t *testing.T) {
	_, mux := newHTTPServer(t)
	pa := pid(t)
	rec := do(t, mux, "POST", "/internal/core/personas", testAdminSecret, `{"persona_id":"`+pa+`"}`)
	var created struct {
		PersonaToken string `json:"persona_token"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &created)
	tok := created.PersonaToken

	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/writer/acquire", tok,
		`{"holder_id":"h1","ttl_ms":60000}`)
	var lease WriterLease
	_ = json.Unmarshal(rec.Body.Bytes(), &lease)
	gen := itoa(lease.Generation)
	do(t, mux, "POST", "/internal/core/personas/"+pa+"/inputs", tok,
		`{"input_id":"i1","kind":"message","payload":{"text":"hi"}}`)
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/turns/load", tok,
		`{"generation":`+gen+`}`)
	var load LoadResult
	_ = json.Unmarshal(rec.Body.Bytes(), &load)
	turnID := load.Turn.TurnID

	// A NUL-bearing decision → 400 at savePlan, the first boundary.
	if rec := do(t, mux, "POST", "/internal/core/personas/"+pa+"/turns/plan", tok,
		`{"generation":`+gen+`,"turn_id":"`+turnID+`","round":0,"text":"a\u0000b","calls":[]}`); rec.Code != 400 {
		t.Fatalf("NUL plan: %d, want 400", rec.Code)
	}
	// A clean plan still saves after the rejection.
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/turns/plan", tok,
		`{"generation":`+gen+`,"turn_id":"`+turnID+`","round":0,"text":"scheduling",
		"calls":[{"tool":"schedule.set","request":{"wake_at":"2030-01-01T00:00:00Z","miss_policy":"bogus"}}]}`)
	if rec.Code != 200 {
		t.Fatalf("save plan: %d %s", rec.Code, rec.Body)
	}
	// Invalid enum value → 400 at claim, not a constraint 500.
	if rec := do(t, mux, "POST", "/internal/core/personas/"+pa+"/operations/claim", tok,
		`{"generation":`+gen+`,"operation_id":"o1","turn_id":"`+turnID+`","tool":"schedule.set","call_index":0,"request":{"wake_at":"2030-01-01T00:00:00Z","miss_policy":"bogus"}}`); rec.Code != 400 {
		t.Fatalf("bogus miss_policy claim: %d, want 400", rec.Code)
	}
	// NUL request → 400 before plan binding.
	if rec := do(t, mux, "POST", "/internal/core/personas/"+pa+"/operations/claim", tok,
		`{"generation":`+gen+`,"operation_id":"o2","turn_id":"`+turnID+`","tool":"journal.note","call_index":0,"request":{"text":"x\u0000y"}}`); rec.Code != 400 {
		t.Fatalf("NUL claim: %d, want 400", rec.Code)
	}
}

// TestHTTPCommitOverBodyLimit pins the boundary the commit fallback
// relies on: a commit body over maxBody is rejected 400 ("read body"),
// deterministically — a giant error text can never be persisted
// verbatim and must be bounded client-side.
func TestHTTPCommitOverBodyLimit(t *testing.T) {
	_, mux := newHTTPServer(t)
	pa := pid(t)
	rec := do(t, mux, "POST", "/internal/core/personas", testAdminSecret, `{"persona_id":"`+pa+`"}`)
	var created struct {
		PersonaToken string `json:"persona_token"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &created)
	tok := created.PersonaToken

	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/writer/acquire", tok,
		`{"holder_id":"h1","ttl_ms":60000}`)
	var lease WriterLease
	_ = json.Unmarshal(rec.Body.Bytes(), &lease)
	gen := itoa(lease.Generation)
	do(t, mux, "POST", "/internal/core/personas/"+pa+"/inputs", tok,
		`{"input_id":"i1","kind":"message","payload":{"text":"hi"}}`)
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/turns/load", tok,
		`{"generation":`+gen+`}`)
	var load LoadResult
	_ = json.Unmarshal(rec.Body.Bytes(), &load)

	// ~1.2 MB of error text — over the 1 MiB body limit.
	huge := strings.Repeat("x", 1_200_000)
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/turns/"+load.Turn.TurnID+"/commit", tok,
		`{"generation":`+gen+`,"outcome":"fail","retryable":true,"error":"`+huge+`"}`)
	if rec.Code != 400 {
		t.Fatalf("oversized commit: %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "read body") {
		t.Fatalf("oversized commit body: %s", rec.Body)
	}
	// The turn is untouched — the rejection leaves it running so a
	// bounded retry can still finalize it.
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/turns/"+load.Turn.TurnID+"/commit", tok,
		`{"generation":`+gen+`,"outcome":"fail","retryable":false,"error":"bounded: too large to store"}`)
	if rec.Code != 200 {
		t.Fatalf("bounded commit after rejection: %d %s", rec.Code, rec.Body)
	}
}
