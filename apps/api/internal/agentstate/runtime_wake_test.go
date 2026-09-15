package agentstate

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testRuntimeToken = "runtime-credential-for-tests-0123456789"
const testWakeToken = "wake-credential-for-tests-0123456789abc"

func TestRuntimeTokenReachesPersonaRoutesButNotAdminRoutes(t *testing.T) {
	srv, mux := newHTTPServer(t)
	if err := srv.SetRuntimeToken(testAdminSecret); err == nil {
		t.Fatal("a runtime token equal to the admin secret must be refused")
	}
	if err := srv.SetRuntimeToken("short"); err == nil {
		t.Fatal("a short runtime token must be refused")
	}
	pa, pb := pid(t), pid(t)
	for _, p := range []string{pa, pb} {
		if rec := do(t, mux, "POST", "/internal/core/personas", testAdminSecret, `{"persona_id":"`+p+`"}`); rec.Code != 201 {
			t.Fatalf("create persona: %d %s", rec.Code, rec.Body)
		}
	}
	// Not configured yet: the runtime token is just a wrong bearer.
	if rec := do(t, mux, "GET", "/internal/core/personas/"+pa+"/state", testRuntimeToken, ""); rec.Code != 401 {
		t.Fatalf("unconfigured runtime token status = %d", rec.Code)
	}
	if err := srv.SetRuntimeToken(testRuntimeToken); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{pa, pb} {
		if rec := do(t, mux, "GET", "/internal/core/personas/"+p+"/state", testRuntimeToken, ""); rec.Code != 200 {
			t.Fatalf("runtime token on persona route status = %d", rec.Code)
		}
	}
	admin := []struct{ method, path, body string }{
		{"POST", "/internal/core/personas", `{"persona_id":"` + pid(t) + `"}`},
		{"POST", "/internal/core/personas/" + pa + "/bind", `{"human_id":"` + pid(t) + `"}`},
		{"POST", "/internal/core/personas/" + pa + "/approvals/appr-1/decision", `{"decision":"approve_once","decision_id":"d","decided_by_kind":"human","decided_by_id":"` + pid(t) + `"}`},
		{"DELETE", "/internal/core/personas/" + pa + "/model/intent", ""},
	}
	for _, r := range admin {
		if rec := do(t, mux, r.method, r.path, testRuntimeToken, r.body); rec.Code != 401 {
			t.Fatalf("runtime token on admin route %s %s status = %d", r.method, r.path, rec.Code)
		}
	}
}

func TestRuntimeWakerFromEnv(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	if w, err := RuntimeWakerFromEnv(nil, env(nil)); w != nil || err != nil {
		t.Fatalf("unconfigured = %v, %v", w, err)
	}
	bad := []map[string]string{
		{RuntimeWakeURLEnv: "https://core.example"},
		{RuntimeWakeTokenEnv: testWakeToken},
		{RuntimeWakeURLEnv: "ftp://core.example", RuntimeWakeTokenEnv: testWakeToken},
		{RuntimeWakeURLEnv: "core.example", RuntimeWakeTokenEnv: testWakeToken},
		{RuntimeWakeURLEnv: "https://core.example", RuntimeWakeTokenEnv: "short"},
	}
	for _, m := range bad {
		if _, err := RuntimeWakerFromEnv(nil, env(m)); err == nil {
			t.Fatalf("config %v accepted", m)
		}
	}
	w, err := RuntimeWakerFromEnv(nil, env(map[string]string{RuntimeWakeURLEnv: "https://core.example/", RuntimeWakeTokenEnv: testWakeToken}))
	if err != nil || w.Target() != "https://core.example" {
		t.Fatalf("valid config = %v, %v", w, err)
	}
}

type wakeRecorder struct {
	mu     sync.Mutex
	calls  []string
	status atomic.Int32
}

func (r *wakeRecorder) handler(t *testing.T) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Bearer "+testWakeToken {
			t.Errorf("wake without the configured bearer")
		}
		r.mu.Lock()
		r.calls = append(r.calls, req.Method+" "+req.URL.Path)
		r.mu.Unlock()
		w.WriteHeader(int(r.status.Load()))
	})
}

func (r *wakeRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func TestRuntimeWakerWakesPendingWorkWithoutLiveWriter(t *testing.T) {
	srv, mux := newHTTPServer(t)
	rec := &wakeRecorder{}
	rec.status.Store(200)
	host := httptest.NewServer(rec.handler(t))
	defer host.Close()
	waker, err := NewRuntimeWaker(srv.Store(), host.URL, testWakeToken)
	if err != nil {
		t.Fatal(err)
	}
	waker.MinGap = 200 * time.Millisecond
	ctx := context.Background()

	busy, idle := pid(t), pid(t)
	for _, p := range []string{busy, idle} {
		if r := do(t, mux, "POST", "/internal/core/personas", testAdminSecret, `{"persona_id":"`+p+`"}`); r.Code != 201 {
			t.Fatalf("create persona: %d", r.Code)
		}
	}
	if n := waker.Sweep(ctx); n != 0 {
		t.Fatalf("personas without work were woken: %d", n)
	}
	submit := func(id string) {
		t.Helper()
		body := `{"input_id":"` + id + `","kind":"message","payload":{"text":"hi"}}`
		if r := do(t, mux, "POST", "/internal/core/personas/"+busy+"/inputs", testAdminSecret, body); r.Code != 201 {
			t.Fatalf("submit: %d %s", r.Code, r.Body)
		}
	}
	submit("in-1")
	if n := waker.Sweep(ctx); n != 1 || rec.calls[0] != "POST /personas/"+busy+"/wake" {
		t.Fatalf("first sweep sent %d: %v", n, rec.calls)
	}
	// Unchanged work inside the gap is not re-woken.
	if n := waker.Sweep(ctx); n != 0 {
		t.Fatalf("re-woken inside the gap: %d", n)
	}
	// A live writer owns the work: no wake while its lease lasts.
	r := do(t, mux, "POST", "/internal/core/personas/"+busy+"/writer/acquire", testAdminSecret, `{"holder_id":"h","ttl_ms":30000}`)
	if r.Code != 200 {
		t.Fatalf("acquire: %d %s", r.Code, r.Body)
	}
	var lease WriterLease
	if err := json.Unmarshal(r.Body.Bytes(), &lease); err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	if n := waker.Sweep(ctx); n != 0 {
		t.Fatalf("woken while a writer holds the lease: %d", n)
	}
	// The writer stops with work still queued: woken again at once, even
	// though the pending work itself did not change.
	body := `{"holder_id":"h","generation":` + jsonInt(lease.Generation) + `}`
	if r := do(t, mux, "POST", "/internal/core/personas/"+busy+"/writer/release", testAdminSecret, body); r.Code != 200 {
		t.Fatalf("release: %d %s", r.Code, r.Body)
	}
	if n := waker.Sweep(ctx); n != 1 {
		t.Fatalf("not woken after the writer released: %d", n)
	}

	// An unreachable/failing host is retried with a doubling gap.
	rec.status.Store(503)
	submit("in-2")
	before := rec.count()
	if n := waker.Sweep(ctx); n != 0 || rec.count() != before+1 {
		t.Fatalf("failing wake: sent %d, calls %d", n, rec.count()-before)
	}
	time.Sleep(250 * time.Millisecond)
	waker.Sweep(ctx) // gap 200ms elapsed: retry, gap doubles to 400ms
	time.Sleep(250 * time.Millisecond)
	waker.Sweep(ctx) // inside the doubled gap: skipped
	if got := rec.count() - before; got != 2 {
		t.Fatalf("failing wake attempts = %d, want 2 (one retry, then backoff)", got)
	}
	rec.status.Store(200)
	time.Sleep(200 * time.Millisecond)
	if n := waker.Sweep(ctx); n != 1 {
		t.Fatalf("recovered host not woken: %d", n)
	}
	for _, c := range rec.calls {
		if strings.Contains(c, idle) {
			t.Fatalf("idle persona woken: %v", rec.calls)
		}
	}
}

func jsonInt(v int64) string {
	b, _ := json.Marshal(v)
	return string(b)
}
