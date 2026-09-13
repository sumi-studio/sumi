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
