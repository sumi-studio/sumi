package agentstate

import (
	"context"
	"encoding/json"
	"errors"
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
		// Userinfo would log the password via Target() — refused outright.
		{RuntimeWakeURLEnv: "https://ops:s3cret-pass@core.example/base/", RuntimeWakeTokenEnv: testWakeToken},
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

// More awaiting personas than the per-sweep candidate limit must not starve
// the later ones: personas already woken inside their re-wake gap sort
// behind eligible candidates, so each sweep reaches work that has never
// been woken instead of re-listing the same stalled prefix.
func TestRuntimeWakerDoesNotStarveLaterPersonas(t *testing.T) {
	srv, mux := newHTTPServer(t)
	rec := &wakeRecorder{}
	rec.status.Store(200)
	host := httptest.NewServer(rec.handler(t))
	defer host.Close()
	waker, err := NewRuntimeWaker(srv.Store(), host.URL, testWakeToken)
	if err != nil {
		t.Fatal(err)
	}
	// Stalled personas stay deferred for the whole test.
	waker.MinGap = time.Hour
	waker.MaxGap = time.Hour
	ctx := context.Background()

	const stalled = 205 // over the 200-row sweep batch
	woken := map[string]int{}
	countWakes := func() {
		t.Helper()
		rec.mu.Lock()
		defer rec.mu.Unlock()
		for _, c := range rec.calls {
			woken[strings.TrimSuffix(strings.TrimPrefix(c, "POST /personas/"), "/wake")]++
		}
		rec.calls = nil
	}
	mk := func() string {
		t.Helper()
		p := pid(t)
		if r := do(t, mux, "POST", "/internal/core/personas", testAdminSecret, `{"persona_id":"`+p+`"}`); r.Code != 201 {
			t.Fatalf("create persona: %d %s", r.Code, r.Body)
		}
		body := `{"input_id":"in-` + p + `","kind":"message","payload":{"text":"hi"}}`
		if r := do(t, mux, "POST", "/internal/core/personas/"+p+"/inputs", testAdminSecret, body); r.Code != 201 {
			t.Fatalf("submit: %d %s", r.Code, r.Body)
		}
		return p
	}
	for range stalled {
		mk()
	}
	if n := waker.Sweep(ctx); n != 200 {
		t.Fatalf("first sweep sent %d, want the 200-row batch", n)
	}
	countWakes()
	if n := waker.Sweep(ctx); n != 5 {
		t.Fatalf("second sweep sent %d, want the 5 remaining personas", n)
	}
	countWakes()
	if len(woken) != stalled {
		t.Fatalf("personas woken = %d, want %d", len(woken), stalled)
	}
	// Every persona was woken exactly once; the deferred prefix was skipped,
	// not re-woken.
	for p, n := range woken {
		if n != 1 {
			t.Fatalf("persona %s woken %d times", p, n)
		}
	}
	if n := waker.Sweep(ctx); n != 0 {
		t.Fatalf("stalled personas re-woken inside their gap: %d", n)
	}
	countWakes()
	// A persona created after all the stalled ones is woken on the very
	// next sweep — the tail of the candidate list is reachable.
	fresh := mk()
	if n := waker.Sweep(ctx); n != 1 {
		t.Fatalf("new persona behind stalled work sent %d, want 1", n)
	}
	countWakes()
	if woken[fresh] != 1 {
		t.Fatalf("newest persona was not woken: %v", woken[fresh])
	}
}

// The disputed starvation window: when a full candidate batch stays
// in-flight longer than MaxGap, every mark expires before the next sweep —
// nothing is deferred and the same oldest prefix can fill the bounded
// result forever. Scaled proportionally (300 permanently pending personas,
// 40ms wake latency, a 150ms MaxGap under a ~1s batch) so the condition
// actually holds; the 1h gap in the test above masks it.
func TestRuntimeWakerRotatesFairlyUnderSlowSweeps(t *testing.T) {
	srv, mux := newHTTPServer(t)
	rec := &wakeRecorder{}
	rec.status.Store(200)
	// Each wake occupies a concurrency slot long enough that a full batch
	// (~200/8 * 40ms = 1s) outlasts MaxGap.
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Bearer "+testWakeToken {
			t.Errorf("wake without the configured bearer")
		}
		rec.mu.Lock()
		rec.calls = append(rec.calls, req.Method+" "+req.URL.Path)
		rec.mu.Unlock()
		time.Sleep(40 * time.Millisecond)
		w.WriteHeader(int(rec.status.Load()))
	}))
	defer host.Close()
	waker, err := NewRuntimeWaker(srv.Store(), host.URL, testWakeToken)
	if err != nil {
		t.Fatal(err)
	}
	waker.MinGap = 30 * time.Millisecond
	waker.MaxGap = 150 * time.Millisecond
	ctx := context.Background()

	const stalled = 300 // over the 200-row sweep batch
	var last string
	for range stalled {
		p := pid(t)
		if r := do(t, mux, "POST", "/internal/core/personas", testAdminSecret, `{"persona_id":"`+p+`"}`); r.Code != 201 {
			t.Fatalf("create persona: %d %s", r.Code, r.Body)
		}
		body := `{"input_id":"in-` + p + `","kind":"message","payload":{"text":"hi"}}`
		if r := do(t, mux, "POST", "/internal/core/personas/"+p+"/inputs", testAdminSecret, body); r.Code != 201 {
			t.Fatalf("submit: %d %s", r.Code, r.Body)
		}
		last = p
	}
	// Each sweep's batch outlives every mark's gap. A fair selection still
	// reaches all 300 personas; a selection that keeps preferring the
	// oldest 200 never reaches the last 100.
	for range 4 {
		waker.Sweep(ctx)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	woken := map[string]int{}
	for _, c := range rec.calls {
		woken[strings.TrimSuffix(strings.TrimPrefix(c, "POST /personas/"), "/wake")]++
	}
	if woken[last] == 0 {
		t.Fatalf("newest persona never woken across 4 slow sweeps; %d of %d personas were reached", len(woken), stalled)
	}
	if len(woken) != stalled {
		t.Fatalf("only %d of %d stalled personas were ever selected", len(woken), stalled)
	}
}

func jsonInt(v int64) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// NextWorkFor is the runtime's half of the sweep: a sleeping core host arms
// its own wake from it. Each kind of recorded work yields its due time, and
// with no live writer "due now" matches exactly what the sweep selects.
func TestNextWorkForMatchesRecordedWork(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	near := func(got *time.Time, want time.Time, label string) {
		t.Helper()
		if got == nil || got.Sub(want).Abs() > 2*time.Second {
			t.Fatalf("%s: next_work_at %v, want ~%v", label, got, want)
		}
	}
	nextFor := func(pa string) NextWork {
		t.Helper()
		nw, err := s.NextWorkFor(ctx, pa)
		if err != nil {
			t.Fatalf("next work: %v", err)
		}
		if nw.NextWorkAt != nil && nw.NextWorkAt.Before(nw.Now) {
			t.Fatalf("next_work_at %v before now %v", nw.NextWorkAt, nw.Now)
		}
		return nw
	}
	swept := func(pa string) bool {
		t.Helper()
		awaiting, err := s.PersonasAwaitingRuntime(ctx, 1000, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range awaiting {
			if a.PersonaID == pa {
				return true
			}
		}
		return false
	}
	agrees := func(pa, label string) {
		t.Helper()
		nw := nextFor(pa)
		due := nw.NextWorkAt != nil && !nw.NextWorkAt.After(nw.Now)
		if due != swept(pa) {
			t.Fatalf("%s: next-work due=%v but sweep selected=%v", label, due, !due)
		}
	}
	submit := func(pa, id string) {
		t.Helper()
		in := &Input{PersonaID: pa, InputID: id, Kind: "message",
			Payload: map[string]any{"text": "hi"}, ActorKind: "human", ActorID: "h-1",
			SourceSurface: "dev", Attention: "reply"}
		if _, _, err := s.SubmitInput(ctx, in); err != nil {
			t.Fatalf("submit: %v", err)
		}
	}
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	// Missing persona: the coded absence Core retires on.
	if _, err := s.NextWorkFor(ctx, pid(t)); !errors.Is(err, ErrPersonaNotFound) {
		t.Fatalf("missing persona err = %v", err)
	}

	// Nothing recorded: sleep.
	pa := pid(t)
	mustPersona(t, s, pa)
	if nw := nextFor(pa); nw.NextWorkAt != nil {
		t.Fatalf("idle persona next_work_at = %v", nw.NextWorkAt)
	}
	agrees(pa, "idle")

	// A far schedule is the next wake; a waiting input is not work.
	exec(`INSERT INTO core_schedules (persona_id, schedule_id, wake_at, payload, status)
		VALUES ($1, 'later', now() + interval '3 hours', '{}'::jsonb, 'pending')`, pa)
	submit(pa, "in-wait")
	exec(`UPDATE core_inputs SET status = 'waiting' WHERE persona_id = $1 AND input_id = 'in-wait'`, pa)
	near(nextFor(pa).NextWorkAt, time.Now().Add(3*time.Hour), "far schedule")
	agrees(pa, "far schedule")

	// A queued input backing off until not_before comes first.
	submit(pa, "in-backoff")
	exec(`UPDATE core_inputs SET not_before = now() + interval '10 minutes' WHERE persona_id = $1 AND input_id = 'in-backoff'`, pa)
	near(nextFor(pa).NextWorkAt, time.Now().Add(10*time.Minute), "not_before")
	agrees(pa, "not_before")

	// A ready input is due now.
	submit(pa, "in-ready")
	near(nextFor(pa).NextWorkAt, time.Now(), "ready input")
	agrees(pa, "ready input")
	exec(`UPDATE core_inputs SET status = 'done' WHERE persona_id = $1 AND input_id IN ('in-ready', 'in-backoff')`, pa)

	// An overdue schedule is due now, never in the past.
	exec(`UPDATE core_schedules SET wake_at = now() - interval '1 hour' WHERE persona_id = $1`, pa)
	near(nextFor(pa).NextWorkAt, time.Now(), "overdue schedule")
	agrees(pa, "overdue schedule")
	exec(`UPDATE core_schedules SET status = 'fired' WHERE persona_id = $1`, pa)

	// An input left claimed by a live writer: due when its lease expires;
	// once the writer is gone, due now (recovery requeues it).
	submit(pa, "in-claimed")
	lease, err := s.AcquireWriter(ctx, pa, "h", 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	exec(`UPDATE core_inputs SET status = 'claimed', claimed_generation = $2 WHERE persona_id = $1 AND input_id = 'in-claimed'`, pa, lease.Generation)
	near(nextFor(pa).NextWorkAt, lease.ExpiresAt, "claimed under live lease")
	if swept(pa) {
		t.Fatal("sweep selected a persona whose writer is live")
	}
	if err := s.ReleaseWriter(ctx, pa, "h", lease.Generation); err != nil {
		t.Fatal(err)
	}
	near(nextFor(pa).NextWorkAt, time.Now(), "claimed, writer gone")
	agrees(pa, "claimed, writer gone")

	// An inactive persona has no work here (the sweep skips it too).
	exec(`UPDATE core_personas SET authority = 'sealed' WHERE persona_id = $1`, pa)
	if nw := nextFor(pa); nw.NextWorkAt != nil {
		t.Fatalf("sealed persona next_work_at = %v", nw.NextWorkAt)
	}
	agrees(pa, "sealed")
}

func TestNextWorkRouteAndInactiveCode(t *testing.T) {
	srv, mux := newHTTPServer(t)
	pa := pid(t)
	if r := do(t, mux, "POST", "/internal/core/personas", testAdminSecret, `{"persona_id":"`+pa+`"}`); r.Code != 201 {
		t.Fatalf("create: %d", r.Code)
	}
	r := do(t, mux, "GET", "/internal/core/personas/"+pa+"/next-work", testAdminSecret, "")
	if r.Code != 200 || !strings.Contains(r.Body.String(), `"next_work_at":null`) || !strings.Contains(r.Body.String(), `"now":`) {
		t.Fatalf("idle next-work: %d %s", r.Code, r.Body)
	}
	if r := do(t, mux, "GET", "/internal/core/personas/"+pa+"/next-work", "", ""); r.Code != 401 {
		t.Fatalf("unauthenticated next-work: %d", r.Code)
	}
	if r := do(t, mux, "GET", "/internal/core/personas/"+pid(t)+"/next-work", testAdminSecret, ""); r.Code != 404 || !strings.Contains(r.Body.String(), `"code":"persona_not_found"`) {
		t.Fatalf("missing persona next-work: %d %s", r.Code, r.Body)
	}
	if _, err := srv.Store().pool.Exec(context.Background(), `UPDATE core_personas SET authority = 'transferred' WHERE persona_id = $1`, pa); err != nil {
		t.Fatal(err)
	}
	r = do(t, mux, "POST", "/internal/core/personas/"+pa+"/writer/acquire", testAdminSecret, `{"holder_id":"h","ttl_ms":30000}`)
	if r.Code != 409 || !strings.Contains(r.Body.String(), `"code":"persona_inactive"`) {
		t.Fatalf("inactive acquire: %d %s", r.Code, r.Body)
	}
}

// Memory work is the runtime's own schedule, so the sweep leaves it alone
// while it is on time. Only work overdue past MemoryRescueGrace with no live
// writer — the signature of a lost wake — is rescued. The reactivation case:
// a persona transferred away while a chunk waited (its runtime disarmed on
// persona_inactive), then made active again with no input or schedule.
func TestRuntimeSweepRescuesOnlyOverdueMemory(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	selected := func(pa string) *AwaitingRuntime {
		t.Helper()
		awaiting, err := s.PersonasAwaitingRuntime(ctx, 1000, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range awaiting {
			if a.PersonaID == pa {
				return &a
			}
		}
		return nil
	}
	pa := pid(t)
	mustPersona(t, s, pa)
	exec(`INSERT INTO core_memory_chunks (persona_id, chunk_seq, first_seq, last_seq, est_tokens, status)
		VALUES ($1, 1, 1, 10, 12000, 'sealed')`, pa)
	if selected(pa) != nil {
		t.Fatal("freshly sealed chunk rescued: the runtime plans its own wake")
	}
	if nw, err := s.NextWorkFor(ctx, pa); err != nil || nw.NextWorkAt != nil {
		t.Fatalf("memory is not next-work: %+v %v", nw, err)
	}

	// Transferred away while the chunk waits: never swept.
	exec(`UPDATE core_memory_chunks SET created_at = now() - interval '2 hours' WHERE persona_id = $1`, pa)
	exec(`UPDATE core_personas SET authority = 'transferred' WHERE persona_id = $1`, pa)
	if selected(pa) != nil {
		t.Fatal("inactive persona swept")
	}
	// An unsnapshotted target cannot make progress on an alarm.
	exec(`UPDATE core_personas SET authority = 'active' WHERE persona_id = $1`, pa)
	if selected(pa) != nil {
		t.Fatal("sealed target without live snapshot was repeatedly rescued")
	}
	// A saved branch can resume without a new main conversation.
	exec(`UPDATE core_memory_chunks SET status='preparing' WHERE persona_id=$1`, pa)
	exec(`INSERT INTO core_memory_branches (persona_id,chunk_seq,snapshot,status,updated_at) VALUES ($1,1,'{}','running',now()-interval '2 hours')`, pa)
	// Active again, no input or schedule: the saved work is rescued.
	exec(`UPDATE core_personas SET authority = 'active' WHERE persona_id = $1`, pa)
	a := selected(pa)
	if a == nil || !a.MemoryOnly {
		t.Fatalf("reactivated persona's overdue memory not rescued: %+v", a)
	}
	// A live writer is working on it: no rescue.
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if selected(pa) != nil {
		t.Fatal("rescued under a live writer")
	}
	if err := s.ReleaseWriter(ctx, pa, "h", lease.Generation); err != nil {
		t.Fatal(err)
	}
	// A retry backoff set just now is on schedule again, however old the chunk.
	exec(`UPDATE core_memory_branches SET status='paused', retry_at = now() + interval '1 minute' WHERE persona_id = $1`, pa)
	if selected(pa) != nil {
		t.Fatal("chunk inside its retry backoff rescued")
	}
	exec(`UPDATE core_memory_branches SET retry_at = now() - interval '20 minutes' WHERE persona_id = $1`, pa)
	if selected(pa) == nil {
		t.Fatal("retry overdue past the grace not rescued (e.g. a planned wake the platform gave up on)")
	}
	// A branch left 'preparing' by a writer that is gone.
	exec(`UPDATE core_memory_branches SET retry_at=NULL WHERE persona_id=$1`, pa)
	if selected(pa) != nil {
		t.Fatal("indefinitely paused branch was rescued")
	}
	exec(`UPDATE core_memory_branches SET status='running', updated_at=now() WHERE persona_id=$1`, pa)
	if selected(pa) != nil {
		t.Fatal("recent preparation rescued")
	}
	exec(`UPDATE core_memory_branches SET updated_at = now() - interval '20 minutes' WHERE persona_id = $1`, pa)
	if a := selected(pa); a == nil || !a.MemoryOnly {
		t.Fatalf("orphaned preparation not rescued: %+v", a)
	}
	// Other work makes it an ordinary wake.
	in := &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "hi"}, ActorKind: "human", ActorID: "h-1",
		SourceSurface: "dev", Attention: "reply"}
	if _, _, err := s.SubmitInput(ctx, in); err != nil {
		t.Fatal(err)
	}
	if a := selected(pa); a == nil || a.MemoryOnly {
		t.Fatalf("input + memory: %+v", a)
	}
}

// A memory-only rescue is re-sent no sooner than MemoryGap (its runtime may
// be resting on a model-unavailable shelf the database cannot see), while
// new ordinary work still wakes at once.
func TestRuntimeWakerPacesMemoryRescue(t *testing.T) {
	srv, _ := newHTTPServer(t)
	rec := &wakeRecorder{}
	rec.status.Store(200)
	host := httptest.NewServer(rec.handler(t))
	defer host.Close()
	waker, err := NewRuntimeWaker(srv.Store(), host.URL, testWakeToken)
	if err != nil {
		t.Fatal(err)
	}
	waker.MinGap = 50 * time.Millisecond
	ctx := context.Background()
	s := srv.Store()
	pa := pid(t)
	mustPersona(t, s, pa)
	if _, err := s.pool.Exec(ctx, `INSERT INTO core_memory_chunks (persona_id, chunk_seq, first_seq, last_seq, est_tokens, status, created_at)
		VALUES ($1, 1, 1, 10, 12000, 'preparing', now() - interval '1 hour')`, pa); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO core_memory_branches (persona_id,chunk_seq,snapshot,status,updated_at) VALUES ($1,1,'{}','running',now()-interval '1 hour')`, pa); err != nil {
		t.Fatal(err)
	}
	if n := waker.Sweep(ctx); n != 1 {
		t.Fatalf("rescue wake sent %d", n)
	}
	time.Sleep(120 * time.Millisecond) // past MinGap, far inside MemoryGap
	if n := waker.Sweep(ctx); n != 0 {
		t.Fatalf("memory-only rescue repeated inside MemoryGap: %d", n)
	}
	in := &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "hi"}, ActorKind: "human", ActorID: "h-1",
		SourceSurface: "dev", Attention: "reply"}
	if _, _, err := s.SubmitInput(ctx, in); err != nil {
		t.Fatal(err)
	}
	if n := waker.Sweep(ctx); n != 1 {
		t.Fatalf("new input not woken promptly: %d", n)
	}
}
