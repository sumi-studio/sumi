package agentstate

// Tests for the job-scoped file capability: claim-authority gating, the
// durable admit→settle ledger, keyed-resend reconciliation after lost
// responses and dead connections, cancellation denial, and cross-persona
// scope confinement. Upstream is a scripted JobFileService — the real
// filesvc adapter is exercised separately; here the point is that the
// ledger tells the truth under every fault the service can produce.

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeJobFiles scripts the upstream port. receipts models filesvc's
// durable keyed ledger: an op that "committed upstream but whose response
// was lost" is a stored receipt the resend finds.
type fakeJobFiles struct {
	mu       sync.Mutex
	receipts map[string]int64 // opID → version (the service's durable receipts)
	// behavior hooks; nil = default success
	mutate func(op, scope, path, ifVersion, opID string, body []byte) (version int64, replayed bool, err error)
	read   func(op, scope, path string, q url.Values) (map[string]any, error)
	calls  []string
}

func (f *fakeJobFiles) ScopeForPersona(personaID string) (string, error) {
	return strings.ToLower(strings.ReplaceAll(personaID, "-", "")), nil
}

func (f *fakeJobFiles) MutateOp(ctx context.Context, op, scope, path, ifVersion, opID string, body []byte) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, op+":"+path)
	if f.mutate != nil {
		return f.mutate(op, scope, path, ifVersion, opID, body)
	}
	if f.receipts == nil {
		f.receipts = map[string]int64{}
	}
	if v, ok := f.receipts[opID]; ok {
		return v, true, nil // receipt replay: committed before, not re-run
	}
	v := int64(len(f.receipts) + 1)
	f.receipts[opID] = v
	return v, false, nil
}

func (f *fakeJobFiles) ReadOp(ctx context.Context, op, scope, path string, q url.Values) (map[string]any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, op+":"+path)
	if f.read != nil {
		return f.read(op, scope, path, q)
	}
	return map[string]any{"op": op, "path": path}, nil
}

func (f *fakeJobFiles) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// claimedScriptJob submits a script job and claims it for runner-1.
func claimedScriptJob(t *testing.T, s *Store, pa, jobID string, lease time.Duration) {
	t.Helper()
	ctx := context.Background()
	req := map[string]any{"code": "export async function run(input, sumi) { return input }"}
	if _, _, err := s.SubmitJob(ctx, pa, jobID, "script", req, "api"); err != nil {
		t.Fatalf("submit script job: %v", err)
	}
	claimed, _, err := s.ClaimJobs(ctx, pa, "runner-1", []string{"script"}, lease, 4, "")
	found := false
	for _, j := range claimed {
		if j.JobID == jobID {
			found = true
		}
	}
	if err != nil || !found {
		t.Fatalf("claim %s: %v %+v", jobID, err, claimed)
	}
}

func TestScriptJobValidation(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)

	// Missing/empty/oversized code and bad limits are deterministic 400s —
	// never a queued job a runner would have to refuse.
	for _, req := range []map[string]any{
		{},
		{"code": ""},
		{"code": 42},
		{"code": strings.Repeat("x", scriptCodeMaxBytes+1)},
		{"code": "export async function run(){}", "limits": "x"},
		{"code": "export async function run(){}", "limits": map[string]any{"cpu_seconds": 0.0}},
		{"code": "export async function run(){}", "limits": map[string]any{"cpu_seconds": 601.0}},
		{"code": "export async function run(){}", "limits": map[string]any{"cpu_seconds": 1.5}},
		{"code": "export async function run(){}", "limits": map[string]any{"bogus": 1.0}},
		{"code": "export async function run(){}", "input": strings.Repeat("x", scriptInputMaxBytes)},
	} {
		if _, _, err := s.SubmitJob(ctx, pa, "bad-"+pid(t), "script", req, "api"); !errors.Is(err, ErrBadRequest) {
			t.Fatalf("request %+v err=%v, want ErrBadRequest", req, err)
		}
	}
	// A bounded spec persists.
	j, created, err := s.SubmitJob(ctx, pa, "j-ok", "script",
		map[string]any{"code": "export async function run(input, sumi){return input}",
			"input":  map[string]any{"a": 1.0},
			"limits": map[string]any{"cpu_seconds": 5.0, "wall_ms": 30000.0, "memory_mib": 128.0}}, "api")
	if err != nil || !created || j.Kind != "script" || j.Status != "queued" {
		t.Fatalf("submit script job: %+v created=%v err=%v", j, created, err)
	}
	// An unknown kind is still refused.
	if _, _, err := s.SubmitJob(ctx, pa, "j-x", "wasm", map[string]any{}, "api"); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("unknown kind err=%v, want ErrBadRequest", err)
	}
}

func TestScriptStartTool(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "sum the ledger"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	req := map[string]any{"code": "export async function run(input, sumi) { return input.total }", "input": map[string]any{"total": 41.0}}
	mustPlan(t, s, pa, "t-1", lease.Generation, PlanCall{Tool: "script.start", Route: "normal", Request: req})

	op, _, fresh, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation, "op-1", "script.start", 0, req)
	if err != nil || !fresh || op.Status != "done" {
		t.Fatalf("claim script.start: %+v fresh=%v err=%v", op, fresh, err)
	}
	jobMap, ok := op.Response["job"].(map[string]any)
	if !ok || jobMap["job_id"] != "op:in-1:0" || jobMap["status"] != "queued" {
		t.Fatalf("script.start response: %+v", op.Response)
	}
	j, err := s.GetJob(ctx, pa, "op:in-1:0")
	if err != nil || j.Kind != "script" || j.Status != "queued" {
		t.Fatalf("script job: %+v err=%v", j, err)
	}
	// The tool request IS the job request — a model cannot smuggle a
	// subprocess spec through script.start (the kind is fixed server-side).
	if _, ok := j.Request["code"].(string); !ok {
		t.Fatalf("script request missing code: %+v", j.Request)
	}
	// A malformed script call is a deterministic tool error: plan a
	// codeless call on a fresh input so the plan binding is honest, then
	// claim — validation refuses before any job row exists.
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-2", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit in-2: %v", err)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-1", lease.Generation, CommitRequest{Outcome: "complete"}); err != nil {
		t.Fatalf("commit t-1: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-2", 10); err != nil {
		t.Fatalf("load t-2: %v", err)
	}
	mustPlan(t, s, pa, "t-2", lease.Generation,
		PlanCall{Tool: "script.start", Route: "normal", Request: map[string]any{"input": 1.0}})
	if _, _, _, err := s.ClaimOperation(ctx, pa, "t-2", lease.Generation,
		"op-bad", "script.start", 0, map[string]any{"input": 1.0}); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("codeless script.start err=%v, want ErrBadRequest", err)
	}
}

func TestJobFileAuthorityGate(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	scope, err := (&fakeJobFiles{}).ScopeForPersona(pa)
	if err != nil {
		t.Fatal(err)
	}

	claimedScriptJob(t, s, pa, "j-1", time.Minute)

	// A queued job admits nothing. (Submitted after j-1's claim so the
	// claim pass leaves it queued.)
	if _, _, err := s.SubmitJob(ctx, pa, "j-q", "script",
		map[string]any{"code": "export async function run(){}"}, "api"); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckJobFileAuthority(ctx, pa, "j-q", "runner-1"); !errors.Is(err, ErrJobFileOpDenied) {
		t.Fatalf("queued authority err=%v, want denied", err)
	}

	// The claiming runner may act; another runner id and a read on the
	// wrong job cannot.
	if err := s.CheckJobFileAuthority(ctx, pa, "j-1", "runner-1"); err != nil {
		t.Fatalf("runner authority: %v", err)
	}
	if err := s.CheckJobFileAuthority(ctx, pa, "j-1", "runner-2"); !errors.Is(err, ErrJobFileOpDenied) {
		t.Fatalf("wrong runner err=%v, want denied", err)
	}
	if err := s.CheckJobFileAuthority(ctx, pa, "ghost", "runner-1"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("missing job err=%v, want not found", err)
	}

	// Admission records scope/path/canonical request durably.
	o, err := s.AdmitJobFileOp(ctx, pa, "j-1", "runner-1", "write", scope, "notes/a.txt", "none", []byte("hi"))
	if err != nil || o.Status != "admitted" || o.OpSeq != 1 || o.OpID == "" || o.Scope != scope {
		t.Fatalf("admit: %+v err=%v", o, err)
	}
	if o.Request["path"] != "notes/a.txt" || o.Request["if_version"] != "none" || o.Request["body_bytes"] != float64(2) {
		t.Fatalf("canonical request: %+v", o.Request)
	}

	// cancel_requested denies NEW operations — the corrected gate.
	if _, err := s.CancelJob(ctx, pa, "j-1"); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckJobFileAuthority(ctx, pa, "j-1", "runner-1"); !errors.Is(err, ErrJobFileOpDenied) {
		t.Fatalf("cancel_requested authority err=%v, want denied", err)
	}
	if _, err := s.AdmitJobFileOp(ctx, pa, "j-1", "runner-1", "write", scope, "notes/b.txt", "", nil); !errors.Is(err, ErrJobFileOpDenied) {
		t.Fatalf("cancel_requested admit err=%v, want denied", err)
	}
	// The op admitted BEFORE the cancel stays reconcilable.
	o, pending, err := s.JobFileOpForResolve(ctx, pa, "j-1", o.OpID)
	if err != nil || !pending || o.Status != "admitted" {
		t.Fatalf("admitted op must stay resolvable after cancel: %+v pending=%v err=%v", o, pending, err)
	}

	// Terminal completion denies too; the ledger keeps its pending truth.
	if _, err := s.CompleteJob(ctx, pa, "j-1", "runner-1", "cancelled", map[string]any{}, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckJobFileAuthority(ctx, pa, "j-1", "runner-1"); !errors.Is(err, ErrJobFileOpDenied) {
		t.Fatalf("terminal authority err=%v, want denied", err)
	}
	if _, pending, err := s.JobFileOpForResolve(ctx, pa, "j-1", o.OpID); err != nil || !pending {
		t.Fatalf("pending survives terminal: pending=%v err=%v", pending, err)
	}

	// Claim expiry (the kind-blind sweep path) denies the same way.
	claimedScriptJob(t, s, pa, "j-2", 30*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	if _, _, err := s.ClaimJobs(ctx, pa, "other-runner", []string{"subprocess"}, time.Minute, 1, "*"); err != nil {
		t.Fatal(err)
	}
	j2, err := s.GetJob(ctx, pa, "j-2")
	if err != nil || j2.Status != "lost" {
		t.Fatalf("competing-kind sweep: %+v err=%v", j2, err)
	}
	if err := s.CheckJobFileAuthority(ctx, pa, "j-2", "runner-1"); !errors.Is(err, ErrJobFileOpDenied) {
		t.Fatalf("lost job authority err=%v, want denied", err)
	}
}

func TestJobFileOpsHTTPLifecycle(t *testing.T) {
	srv, mux := newHTTPServer(t)
	fake := &fakeJobFiles{receipts: map[string]int64{}}
	srv.SetJobFileService(fake)

	pa, tok := newPersonaToken(t, mux)
	claimedScriptJob(t, srv.store, pa, "j-1", time.Minute)

	base := "/internal/core/personas/" + pa + "/jobs/j-1/files/"

	// Read op authorized under the claim.
	rec := do(t, mux, "POST", base+"stat", tok, `{"runner_id":"runner-1","path":"a.txt"}`)
	if rec.Code != 200 {
		t.Fatalf("stat: %d %s", rec.Code, rec.Body)
	}

	// A mutating op: admitted → upstream settled in one call.
	rec = do(t, mux, "POST", base+"write", tok,
		`{"runner_id":"runner-1","path":"a.txt","if_version":"none","data_base64":"aGk="}`)
	if rec.Code != 200 {
		t.Fatalf("write: %d %s", rec.Code, rec.Body)
	}
	var wr struct {
		Op JobFileOp `json:"op"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &wr)
	if wr.Op.Status != "settled" || wr.Op.Result["version"] != float64(1) {
		t.Fatalf("write op: %+v", wr.Op)
	}

	// The ledger is visible to the persona.
	rec = do(t, mux, "GET", base+"ops", tok, "")
	var listing struct {
		Ops     []JobFileOp `json:"ops"`
		Pending int         `json:"pending"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &listing)
	if len(listing.Ops) != 1 || listing.Pending != 0 {
		t.Fatalf("ops listing: %+v", listing)
	}

	// Cross-persona denial: another persona's token cannot touch the job.
	_, otherTok := newPersonaToken(t, mux)
	if rec := do(t, mux, "POST", base+"write", otherTok,
		`{"runner_id":"runner-1","path":"a.txt","data_base64":"eA=="}`); rec.Code != 401 {
		t.Fatalf("cross-persona write: %d, want 401", rec.Code)
	}
	// Another runner id on the same job is a claim denial, not a scope issue.
	if rec := do(t, mux, "POST", base+"write", tok,
		`{"runner_id":"runner-2","path":"a.txt","data_base64":"eA=="}`); rec.Code != 409 {
		t.Fatalf("wrong-runner write: %d, want 409", rec.Code)
	}
	if fake.callCount() != 2 {
		t.Fatalf("upstream calls after denials: %d, want 2", fake.callCount())
	}
}

func TestJobFileOpLostResponseResolves(t *testing.T) {
	srv, mux := newHTTPServer(t)
	fake := &fakeJobFiles{receipts: map[string]int64{}}
	// The upstream commits (stores the receipt) but the response is lost —
	// the transport error path. The resend must find the receipt, not re-run.
	fake.mutate = func(op, scope, path, ifVersion, opID string, body []byte) (int64, bool, error) {
		if v, ok := fake.receipts[opID]; ok {
			return v, true, nil
		}
		fake.receipts[opID] = 7
		return 0, false, errors.New("connection reset by peer")
	}
	srv.SetJobFileService(fake)

	pa, tok := newPersonaToken(t, mux)
	claimedScriptJob(t, srv.store, pa, "j-1", time.Minute)
	base := "/internal/core/personas/" + pa + "/jobs/j-1/files/"

	rec := do(t, mux, "POST", base+"write", tok,
		`{"runner_id":"runner-1","path":"ledger.csv","data_base64":"MSwy"}`)
	if rec.Code != 200 {
		t.Fatalf("write: %d %s", rec.Code, rec.Body)
	}
	var wr struct {
		Op JobFileOp `json:"op"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &wr)
	if wr.Op.Status != "unknown" {
		t.Fatalf("lost response must record unknown, got %+v", wr.Op)
	}

	// Resolve: the keyed resend finds the durable receipt — settled, and
	// the receipt says the effect committed (replayed), truthfully.
	rec = do(t, mux, "POST", base+"ops/"+wr.Op.OpID+"/resolve", tok, `{}`)
	_ = json.Unmarshal(rec.Body.Bytes(), &wr)
	if rec.Code != 200 || wr.Op.Status != "settled" || wr.Op.Result["replayed"] != true || wr.Op.Result["version"] != float64(7) {
		t.Fatalf("resolve after lost response: %d %+v", rec.Code, wr.Op)
	}
	// Idempotent resolve replays the stored row.
	rec = do(t, mux, "POST", base+"ops/"+wr.Op.OpID+"/resolve", tok, `{}`)
	_ = json.Unmarshal(rec.Body.Bytes(), &wr)
	if wr.Op.Status != "settled" {
		t.Fatalf("resolve replay: %+v", wr.Op)
	}
}

func TestJobFileOpAdmissionSurvivesUpstream(t *testing.T) {
	// The delayed-commit case: the admitted row must exist durably while
	// the upstream call is still in flight — a crash here leaves 'admitted',
	// never a silently-dropped operation.
	srv, _ := newHTTPServer(t)
	pa := pid(t)
	ctx := context.Background()
	mustPersona(t, srv.store, pa)
	claimedScriptJob(t, srv.store, pa, "j-1", time.Minute)

	fake := &fakeJobFiles{receipts: map[string]int64{}}
	srv.SetJobFileService(fake)
	scope, _ := fake.ScopeForPersona(pa)

	// Admit directly (as the handler does), then never record — the
	// process-died-mid-request case. The row stays admitted.
	o, err := srv.store.AdmitJobFileOp(ctx, pa, "j-1", "runner-1", "write", scope, "pending.txt", "", []byte("x"))
	if err != nil || o.Status != "admitted" {
		t.Fatalf("admit: %+v err=%v", o, err)
	}
	ops, err := srv.store.ListJobFileOps(ctx, pa, "j-1", true, 10)
	if err != nil || len(ops) != 1 || ops[0].Status != "admitted" {
		t.Fatalf("pending list: %+v err=%v", ops, err)
	}
	// Resolve executes it once under the same key — fresh execution now
	// (the original call never reached the service), still exactly-once.
	ver, replayed, err := fake.MutateOp(ctx, o.Op, o.Scope, o.Path, "", o.OpID, o.Body)
	if err != nil || replayed {
		t.Fatalf("first upstream call: ver=%d replayed=%v err=%v", ver, replayed, err)
	}
	o, err = srv.store.RecordJobFileOp(ctx, pa, "j-1", o.OpID, "settled", map[string]any{"version": ver, "replayed": replayed}, "")
	if err != nil || o.Status != "settled" {
		t.Fatalf("record: %+v err=%v", o, err)
	}
	// A delayed second attempt under the same key replays — no double effect.
	ver2, replayed2, err := fake.MutateOp(ctx, o.Op, o.Scope, o.Path, "", o.OpID, o.Body)
	if err != nil || !replayed2 || ver2 != ver {
		t.Fatalf("resend must replay receipt: ver=%d replayed=%v err=%v", ver2, replayed2, err)
	}
}
