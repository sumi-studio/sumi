package agentstate

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func subReq(cmd ...string) map[string]any {
	args := make([]any, len(cmd))
	for i, c := range cmd {
		args[i] = c
	}
	return map[string]any{"command": args}
}

// A job submits queued, replays identically on resend, and rejects a
// divergent replay — the same idempotency contract inputs carry.
func TestJobSubmitReplayAndConflict(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)

	j, created, err := s.SubmitJob(ctx, pa, "j-1", "subprocess", subReq("echo", "hi"), "api")
	if err != nil || !created || j.Status != "queued" {
		t.Fatalf("submit: %+v created=%v err=%v", j, created, err)
	}
	got, err := s.GetJob(ctx, pa, "j-1")
	if err != nil || got.JobID != "j-1" || got.Status != "queued" {
		t.Fatalf("get: %+v err=%v", got, err)
	}
	// Identical resend replays the stored row — lost response path.
	j2, created2, err := s.SubmitJob(ctx, pa, "j-1", "subprocess", subReq("echo", "hi"), "api")
	if err != nil || created2 || j2.JobID != "j-1" {
		t.Fatalf("replay submit: %+v created=%v err=%v", j2, created2, err)
	}
	// Divergent resend conflicts.
	if _, _, err = s.SubmitJob(ctx, pa, "j-1", "subprocess", subReq("rm", "-rf"), "api"); !errors.Is(err, ErrJobConflict) {
		t.Fatalf("divergent submit err = %v, want ErrJobConflict", err)
	}
	// Unknown persona / unknown job.
	if _, _, err = s.SubmitJob(ctx, pid(t), "j-1", "subprocess", subReq("echo"), "api"); !errors.Is(err, ErrPersonaNotFound) {
		t.Fatalf("unknown persona err = %v, want ErrPersonaNotFound", err)
	}
	if _, err = s.GetJob(ctx, pa, "nope"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("unknown job err = %v, want ErrJobNotFound", err)
	}
}

// Validation is deterministic at the boundary: bad specs are 400s, never a
// queued job no runner can execute, and reserved namespaces are refused.
func TestJobValidation(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)

	bad := []struct {
		jobID string
		kind  string
		req   map[string]any
	}{
		{"j-x", "mystery", map[string]any{}},
		{"j-x", "subprocess", map[string]any{}},                    // no command
		{"j-x", "subprocess", map[string]any{"command": []any{}}},  // empty command
		{"j-x", "subprocess", map[string]any{"command": []any{1}}}, // non-string arg
		{"j-x", "subprocess", map[string]any{"command": []any{"echo"}, "timeout_ms": -5}},
		{"j-x", "subprocess", map[string]any{"command": []any{"echo"}, "timeout_ms": 1.5}},
		{"j-x", "subprocess", map[string]any{"command": []any{"echo"}, "cwd": 7}},
		{"j-x", "subprocess", map[string]any{"command": []any{"echo"}, "env": map[string]any{"A": 1}}},
		{"op:in-1:0", "subprocess", subReq("echo")}, // reserved tool-derived prefix
		{"claim", "subprocess", subReq("echo")},     // reserved route word
		{strings.Repeat("x", 300), "subprocess", subReq("echo")},
	}
	for i, b := range bad {
		if _, _, err := s.SubmitJob(ctx, pa, b.jobID, b.kind, b.req, "api"); !errors.Is(err, ErrBadRequest) {
			t.Fatalf("bad[%d] err = %v, want ErrBadRequest", i, err)
		}
	}
	// The job: input prefix is reserved for terminal notifications.
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "job:x",
		Kind: "message", Payload: map[string]any{}}); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("job: input err = %v, want ErrBadRequest", err)
	}
}

// Full happy path: claim → heartbeat → complete stores the result and queues
// exactly one notification input; an identical complete replays the record.
func TestJobClaimHeartbeatComplete(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	if _, _, err := s.SubmitJob(ctx, pa, "j-1", "subprocess", subReq("sleep", "0"), "api"); err != nil {
		t.Fatalf("submit: %v", err)
	}

	claimed, swept, err := s.ClaimJobs(ctx, pa, "runner-1", []string{"subprocess"}, time.Minute, 4)
	if err != nil || len(claimed) != 1 || len(swept) != 0 {
		t.Fatalf("claim: claimed=%+v swept=%+v err=%v", claimed, swept, err)
	}
	j := claimed[0]
	if j.Status != "running" || j.ClaimedBy == nil || *j.ClaimedBy != "runner-1" || j.StartedAt == nil {
		t.Fatalf("claimed job: %+v", j)
	}
	// A second runner sees nothing to claim.
	claimed2, _, err := s.ClaimJobs(ctx, pa, "runner-2", []string{"subprocess"}, time.Minute, 4)
	if err != nil || len(claimed2) != 0 {
		t.Fatalf("second claim: %+v err=%v", claimed2, err)
	}

	// Heartbeat extends the claim and reports current status.
	hb, err := s.HeartbeatJob(ctx, pa, "j-1", "runner-1", time.Minute)
	if err != nil || hb.Status != "running" {
		t.Fatalf("heartbeat: %+v err=%v", hb, err)
	}
	// A different runner's heartbeat is refused but returns the stored row.
	if _, err := s.HeartbeatJob(ctx, pa, "j-1", "runner-2", time.Minute); !errors.Is(err, ErrJobNotClaimed) {
		t.Fatalf("foreign heartbeat err = %v, want ErrJobNotClaimed", err)
	}
	// A different runner cannot complete.
	if _, err := s.CompleteJob(ctx, pa, "j-1", "runner-2", "done", map[string]any{"exit_code": 0.0}, ""); !errors.Is(err, ErrJobNotClaimed) {
		t.Fatalf("foreign complete err = %v, want ErrJobNotClaimed", err)
	}

	res := map[string]any{"exit_code": 0.0, "stdout": "hi\n", "stderr": "", "truncated": false}
	done, err := s.CompleteJob(ctx, pa, "j-1", "runner-1", "done", res, "")
	if err != nil || done.Status != "done" || done.FinishedAt == nil || done.NotifiedAt == nil {
		t.Fatalf("complete: %+v err=%v", done, err)
	}
	// The terminal notification entered the ordinary input stream exactly
	// once, under the reserved job: prefix.
	note, _, err := s.GetInput(ctx, pa, "job:j-1")
	if err != nil || note.Status != "queued" || note.Kind != "job_completed" {
		t.Fatalf("notification input: %+v err=%v", note, err)
	}
	if note.Payload["job_id"] != "j-1" || note.Payload["status"] != "done" {
		t.Fatalf("notification payload: %+v", note.Payload)
	}
	// Identical replay (lost response) returns the stored row — the same
	// transaction guarantee means no second notification can exist.
	replay, err := s.CompleteJob(ctx, pa, "j-1", "runner-1", "done", res, "")
	if err != nil || replay.Status != "done" {
		t.Fatalf("replay complete: %+v err=%v", replay, err)
	}
	// Divergent replay conflicts.
	if _, err := s.CompleteJob(ctx, pa, "j-1", "runner-1", "failed", res, "boom"); !errors.Is(err, ErrJobConflict) {
		t.Fatalf("divergent complete err = %v, want ErrJobConflict", err)
	}
	// A complete on a job that was never claimed is refused.
	if _, _, err := s.SubmitJob(ctx, pa, "j-2", "subprocess", subReq("echo"), "api"); err != nil {
		t.Fatalf("submit j-2: %v", err)
	}
	if _, err := s.CompleteJob(ctx, pa, "j-2", "runner-1", "done", res, ""); !errors.Is(err, ErrJobNotClaimed) {
		t.Fatalf("unclaimed complete err = %v, want ErrJobNotClaimed", err)
	}
}

// Queued cancel is terminal with a notification; running cancel is a request
// the owning runner observes via heartbeat and settles by completing.
func TestJobCancelSemantics(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)

	// Queued → cancelled, notified, never ran.
	if _, _, err := s.SubmitJob(ctx, pa, "j-q", "subprocess", subReq("sleep", "9"), "api"); err != nil {
		t.Fatalf("submit: %v", err)
	}
	j, err := s.CancelJob(ctx, pa, "j-q")
	if err != nil || j.Status != "cancelled" || j.CancelRequestedAt == nil || j.FinishedAt == nil {
		t.Fatalf("queued cancel: %+v err=%v", j, err)
	}
	if _, _, err := s.GetInput(ctx, pa, "job:j-q"); err != nil {
		t.Fatalf("cancel notification missing: %v", err)
	}
	// Nothing remains to claim.
	claimed, _, err := s.ClaimJobs(ctx, pa, "r", []string{"subprocess"}, time.Minute, 4)
	if err != nil || len(claimed) != 0 {
		t.Fatalf("claim after cancel: %+v err=%v", claimed, err)
	}

	// Running → cancel_requested; the runner sees it on heartbeat and then
	// reports the real terminal outcome.
	if _, _, err := s.SubmitJob(ctx, pa, "j-r", "subprocess", subReq("sleep", "9"), "api"); err != nil {
		t.Fatalf("submit j-r: %v", err)
	}
	if _, _, err := s.ClaimJobs(ctx, pa, "runner-1", []string{"subprocess"}, time.Minute, 4); err != nil {
		t.Fatalf("claim j-r: %v", err)
	}
	j, err = s.CancelJob(ctx, pa, "j-r")
	if err != nil || j.Status != "cancel_requested" || j.FinishedAt != nil {
		t.Fatalf("running cancel: %+v err=%v", j, err)
	}
	hb, err := s.HeartbeatJob(ctx, pa, "j-r", "runner-1", time.Minute)
	if err != nil || hb.Status != "cancel_requested" {
		t.Fatalf("heartbeat sees cancel: %+v err=%v", hb, err)
	}
	// The owning runner reports what actually happened: the process was
	// killed → cancelled. cancel_requested_at stays as evidence.
	done, err := s.CompleteJob(ctx, pa, "j-r", "runner-1", "cancelled",
		map[string]any{"exit_code": nil, "signal": "SIGTERM"}, "")
	if err != nil || done.Status != "cancelled" || done.CancelRequestedAt == nil {
		t.Fatalf("cancelled complete: %+v err=%v", done, err)
	}
	// Late-completion honesty: if the command had already exited before the
	// cancel landed, the runner reports 'done' and that real outcome stands.
	if _, _, err := s.SubmitJob(ctx, pa, "j-late", "subprocess", subReq("echo", "x"), "api"); err != nil {
		t.Fatalf("submit j-late: %v", err)
	}
	if _, _, err := s.ClaimJobs(ctx, pa, "runner-1", []string{"subprocess"}, time.Minute, 4); err != nil {
		t.Fatalf("claim j-late: %v", err)
	}
	if _, err := s.CancelJob(ctx, pa, "j-late"); err != nil {
		t.Fatalf("cancel j-late: %v", err)
	}
	late, err := s.CompleteJob(ctx, pa, "j-late", "runner-1", "done",
		map[string]any{"exit_code": 0.0, "stdout": "x\n"}, "")
	if err != nil || late.Status != "done" || late.CancelRequestedAt == nil {
		t.Fatalf("late complete: %+v err=%v", late, err)
	}
	// Cancelling a terminal job is an idempotent replay.
	again, err := s.CancelJob(ctx, pa, "j-late")
	if err != nil || again.Status != "done" {
		t.Fatalf("cancel after done: %+v err=%v", again, err)
	}
}

// An expired runner claim sweeps to 'lost' — indeterminate outcome, never
// re-queued — and the owner is notified once. A subsequent complete by the
// dead runner conflicts.
func TestJobClaimExpiryLost(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	if _, _, err := s.SubmitJob(ctx, pa, "j-1", "subprocess", subReq("sleep", "9"), "api"); err != nil {
		t.Fatalf("submit: %v", err)
	}
	// Tiny lease so the claim expires.
	if _, _, err := s.ClaimJobs(ctx, pa, "runner-1", []string{"subprocess"}, 20*time.Millisecond, 4); err != nil {
		t.Fatalf("claim: %v", err)
	}
	time.Sleep(40 * time.Millisecond)

	// The next claim pass (any runner) sweeps the expired job to lost.
	claimed, swept, err := s.ClaimJobs(ctx, pa, "runner-2", []string{"subprocess"}, time.Minute, 4)
	if err != nil || len(claimed) != 0 || len(swept) != 1 || swept[0].Status != "lost" {
		t.Fatalf("sweep: claimed=%+v swept=%+v err=%v", claimed, swept, err)
	}
	j, err := s.GetJob(ctx, pa, "j-1")
	if err != nil || j.Status != "lost" || j.Error == nil || j.FinishedAt == nil {
		t.Fatalf("lost job: %+v err=%v", j, err)
	}
	if _, _, err := s.GetInput(ctx, pa, "job:j-1"); err != nil {
		t.Fatalf("lost notification missing: %v", err)
	}
	// The dead runner's late completion is a conflict — the stored 'lost'
	// verdict stands; nothing re-executes.
	if _, err := s.CompleteJob(ctx, pa, "j-1", "runner-1", "done",
		map[string]any{"exit_code": 0.0}, ""); !errors.Is(err, ErrJobConflict) {
		t.Fatalf("late complete after lost err = %v, want ErrJobConflict", err)
	}
	// Heartbeat from the dead runner reports the stored terminal row.
	hb, err := s.HeartbeatJob(ctx, pa, "j-1", "runner-1", time.Minute)
	if !errors.Is(err, ErrJobNotClaimed) || hb.Status != "lost" {
		t.Fatalf("dead-runner heartbeat: %+v err=%v", hb, err)
	}
	// No re-execution: the job never returns to 'queued' on later passes.
	claimed, swept, err = s.ClaimJobs(ctx, pa, "runner-2", []string{"subprocess"}, time.Minute, 4)
	if err != nil || len(claimed) != 0 || len(swept) != 0 {
		t.Fatalf("second sweep: claimed=%+v swept=%+v", claimed, swept)
	}
}

// job.start as a state-internal tool: the job row is minted inside the plan-
// bound claim transaction, with a server-derived job_id, so a replayed claim
// can never create a second job.
func TestJobStartTool(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "run tests"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	req := map[string]any{"command": []any{"echo", "hello"}, "timeout_ms": 5000.0}
	mustPlan(t, s, pa, "t-1", lease.Generation, PlanCall{Tool: "job.start", Request: req})

	op, fresh, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation, "op-1", "job.start", 0, req)
	if err != nil || !fresh || op.Status != "done" {
		t.Fatalf("claim job.start: %+v fresh=%v err=%v", op, fresh, err)
	}
	jobMap, ok := op.Response["job"].(map[string]any)
	if !ok || jobMap["job_id"] != "op:in-1:0" || jobMap["status"] != "queued" {
		t.Fatalf("job.start response: %+v", op.Response)
	}
	// The durable job exists with the derived id.
	j, err := s.GetJob(ctx, pa, "op:in-1:0")
	if err != nil || j.Status != "queued" || j.Kind != "subprocess" {
		t.Fatalf("tool job: %+v err=%v", j, err)
	}
	// Replayed claim replays the operation — no second job, no second insert.
	op2, fresh2, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation, "op-2", "job.start", 0, req)
	if err != nil || fresh2 || op2.OperationID != "op-1" {
		t.Fatalf("replay claim: %+v fresh=%v err=%v", op2, fresh2, err)
	}
	jobs, err := s.ListJobs(ctx, pa, nil, 10)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs: %+v err=%v", jobs, err)
	}
	// job.status/job.cancel run through the same plan-bound mechanism;
	// TestJobStatusCancelTools exercises them on their own input (one
	// immutable plan per input).
	_ = op2
}

// job.status and job.cancel as tools, exercised through a second input so
// each plan stays immutable.
func TestJobStatusCancelTools(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitJob(ctx, pa, "j-1", "subprocess", subReq("sleep", "9"), "api"); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-2", Kind: "message",
		Payload: map[string]any{"text": "check job"}}); err != nil {
		t.Fatalf("submit in-2: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-2", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	mustPlan(t, s, pa, "t-2", lease.Generation,
		PlanCall{Tool: "job.status", Request: map[string]any{"job_id": "j-1"}},
		PlanCall{Tool: "job.cancel", Request: map[string]any{"job_id": "j-1"}})

	op, fresh, err := s.ClaimOperation(ctx, pa, "t-2", lease.Generation,
		"op-s", "job.status", 0, map[string]any{"job_id": "j-1"})
	if err != nil || !fresh || op.Status != "done" {
		t.Fatalf("job.status claim: %+v err=%v", op, err)
	}
	jm, _ := op.Response["job"].(map[string]any)
	if jm["job_id"] != "j-1" || jm["status"] != "queued" {
		t.Fatalf("job.status response: %+v", op.Response)
	}
	op, fresh, err = s.ClaimOperation(ctx, pa, "t-2", lease.Generation,
		"op-c", "job.cancel", 1, map[string]any{"job_id": "j-1"})
	if err != nil || !fresh || op.Status != "done" {
		t.Fatalf("job.cancel claim: %+v err=%v", op, err)
	}
	jm, _ = op.Response["job"].(map[string]any)
	if jm["status"] != "cancelled" {
		t.Fatalf("job.cancel response: %+v", op.Response)
	}
	// Status of a nonexistent job is a deterministic tool error (400-class),
	// which the secretary records rather than stalling on. Finish t-2 first —
	// LoadTurn re-attaches a still-running turn instead of beginning t-3.
	if _, err := s.CommitTurn(ctx, pa, "t-2", lease.Generation, CommitRequest{Outcome: "complete"}); err != nil {
		t.Fatalf("commit t-2: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-3", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit in-3: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-3", 10); err != nil {
		t.Fatalf("load t-3: %v", err)
	}
	mustPlan(t, s, pa, "t-3", lease.Generation,
		PlanCall{Tool: "job.status", Request: map[string]any{"job_id": "ghost"}})
	if _, _, err := s.ClaimOperation(ctx, pa, "t-3", lease.Generation,
		"op-g", "job.status", 0, map[string]any{"job_id": "ghost"}); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("ghost job.status err = %v, want ErrBadRequest", err)
	}
}

// The notification input is claimed by the secretary through the ordinary
// LoadTurn path — the result reaches the same continuing secretary, not a
// side channel.
func TestJobNotificationEntersInputStream(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	if _, _, err := s.SubmitJob(ctx, pa, "j-1", "subprocess", subReq("echo", "hi"), "api"); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, _, err := s.ClaimJobs(ctx, pa, "runner-1", []string{"subprocess"}, time.Minute, 4); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := s.CompleteJob(ctx, pa, "j-1", "runner-1", "done",
		map[string]any{"exit_code": 0.0, "stdout": "hi\n"}, ""); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// A (restarted) secretary acquires the writer lease and its next
	// LoadTurn claims the notification — exactly once.
	lease, err := s.AcquireWriter(ctx, pa, "h2", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	load, err := s.LoadTurn(ctx, pa, lease.Generation, "t-9", 10)
	if err != nil || load.Input == nil || load.Input.InputID != "job:j-1" || load.Input.Kind != "job_completed" {
		t.Fatalf("notification load: %+v err=%v", load, err)
	}
	if load.Input.Payload["job_id"] != "j-1" || load.Input.Payload["status"] != "done" {
		t.Fatalf("notification payload: %+v", load.Input.Payload)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-9", lease.Generation, CommitRequest{Outcome: "complete"}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// Consumed: the next load does not re-deliver the same notification.
	load2, err := s.LoadTurn(ctx, pa, lease.Generation, "t-10", 10)
	if err != nil {
		t.Fatalf("second load: %v", err)
	}
	if load2.Input != nil && load2.Input.InputID == "job:j-1" {
		t.Fatalf("notification delivered twice: %+v", load2.Input)
	}
}

// --- HTTP layer -----------------------------------------------------------

func newPersonaToken(t *testing.T, mux *http.ServeMux) (string, string) {
	t.Helper()
	pa := pid(t)
	rec := do(t, mux, "POST", "/internal/core/personas", testAdminSecret,
		`{"persona_id":"`+pa+`"}`)
	if rec.Code != 201 {
		t.Fatalf("provision: %d %s", rec.Code, rec.Body)
	}
	var created struct {
		PersonaToken string `json:"persona_token"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &created)
	return pa, created.PersonaToken
}

// The job routes over HTTP: submit → claim → heartbeat → complete → the
// notification input; plus auth, replay, and conflict shapes.
func TestHTTPJobLifecycle(t *testing.T) {
	_, mux := newHTTPServer(t)
	pa, tok := newPersonaToken(t, mux)

	// Auth: no token and another persona's token are refused.
	if rec := do(t, mux, "POST", "/internal/core/personas/"+pa+"/jobs", "", `{"job_id":"j-1","kind":"subprocess","request":{"command":["echo"]}}`); rec.Code != 401 {
		t.Fatalf("no-auth submit: %d, want 401", rec.Code)
	}
	_, otherTok := newPersonaToken(t, mux)
	if rec := do(t, mux, "POST", "/internal/core/personas/"+pa+"/jobs", otherTok, `{"job_id":"j-1","kind":"subprocess","request":{"command":["echo"]}}`); rec.Code != 401 {
		t.Fatalf("cross-persona submit: %d, want 401", rec.Code)
	}

	rec := do(t, mux, "POST", "/internal/core/personas/"+pa+"/jobs", tok,
		`{"job_id":"j-1","kind":"subprocess","request":{"command":["echo","hi"],"timeout_ms":30000}}`)
	if rec.Code != 201 {
		t.Fatalf("submit: %d %s", rec.Code, rec.Body)
	}
	var submitted struct {
		Job     Job  `json:"job"`
		Created bool `json:"created"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &submitted)
	if !submitted.Created || submitted.Job.Status != "queued" {
		t.Fatalf("submitted: %+v", submitted)
	}
	// Identical resend → 200 replay; divergent → 409.
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/jobs", tok,
		`{"job_id":"j-1","kind":"subprocess","request":{"command":["echo","hi"],"timeout_ms":30000}}`)
	if rec.Code != 200 {
		t.Fatalf("replay submit: %d", rec.Code)
	}
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/jobs", tok,
		`{"job_id":"j-1","kind":"subprocess","request":{"command":["echo","other"]}}`)
	if rec.Code != 409 {
		t.Fatalf("divergent submit: %d, want 409", rec.Code)
	}
	// Bad spec → 400.
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/jobs", tok,
		`{"job_id":"j-bad","kind":"subprocess","request":{"command":[]}}`)
	if rec.Code != 400 {
		t.Fatalf("bad spec: %d, want 400", rec.Code)
	}

	// Claim.
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/jobs/claim", tok,
		`{"runner_id":"r-1","kinds":["subprocess"],"lease_ms":60000,"limit":4}`)
	if rec.Code != 200 {
		t.Fatalf("claim: %d %s", rec.Code, rec.Body)
	}
	var claimed struct {
		Claimed []Job `json:"claimed"`
		Swept   []Job `json:"swept"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &claimed)
	if len(claimed.Claimed) != 1 || claimed.Claimed[0].JobID != "j-1" {
		t.Fatalf("claimed: %+v", claimed)
	}

	// Heartbeat returns current status.
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/jobs/j-1/heartbeat", tok,
		`{"runner_id":"r-1","lease_ms":60000}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"running"`) {
		t.Fatalf("heartbeat: %d %s", rec.Code, rec.Body)
	}
	// Foreign runner heartbeat → 409 carrying the stored row.
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/jobs/j-1/heartbeat", tok,
		`{"runner_id":"r-2","lease_ms":60000}`)
	if rec.Code != 409 || !strings.Contains(rec.Body.String(), `"job"`) {
		t.Fatalf("foreign heartbeat: %d %s", rec.Code, rec.Body)
	}

	// Get + list.
	rec = do(t, mux, "GET", "/internal/core/personas/"+pa+"/jobs/j-1", tok, "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"running"`) {
		t.Fatalf("get: %d %s", rec.Code, rec.Body)
	}
	rec = do(t, mux, "GET", "/internal/core/personas/"+pa+"/jobs?status=running", tok, "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "j-1") {
		t.Fatalf("list: %d %s", rec.Code, rec.Body)
	}
	rec = do(t, mux, "GET", "/internal/core/personas/"+pa+"/jobs?status=queued", tok, "")
	if rec.Code != 200 || strings.Contains(rec.Body.String(), "j-1") {
		t.Fatalf("list filter: %d %s", rec.Code, rec.Body)
	}

	// Complete → done, notification input queued.
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/jobs/j-1/complete", tok,
		`{"runner_id":"r-1","status":"done","result":{"exit_code":0,"stdout":"hi\n","stderr":"","truncated":false}}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"done"`) {
		t.Fatalf("complete: %d %s", rec.Code, rec.Body)
	}
	// Identical replay → 200 stored row.
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/jobs/j-1/complete", tok,
		`{"runner_id":"r-1","status":"done","result":{"exit_code":0,"stdout":"hi\n","stderr":"","truncated":false}}`)
	if rec.Code != 200 {
		t.Fatalf("replay complete: %d %s", rec.Code, rec.Body)
	}
	// Divergent replay → 409 carrying the stored row.
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/jobs/j-1/complete", tok,
		`{"runner_id":"r-1","status":"failed","result":{"exit_code":1},"error":"boom"}`)
	if rec.Code != 409 || !strings.Contains(rec.Body.String(), `"done"`) {
		t.Fatalf("divergent complete: %d %s", rec.Code, rec.Body)
	}
	// The notification input is queued for the secretary.
	rec = do(t, mux, "GET", "/internal/core/personas/"+pa+"/inputs/job:j-1", tok, "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "job_completed") {
		t.Fatalf("notification input: %d %s", rec.Code, rec.Body)
	}
}

// Cancel over HTTP: queued → cancelled with notification; validation shapes.
func TestHTTPJobCancelAndValidation(t *testing.T) {
	_, mux := newHTTPServer(t)
	pa, tok := newPersonaToken(t, mux)

	do(t, mux, "POST", "/internal/core/personas/"+pa+"/jobs", tok,
		`{"job_id":"j-1","kind":"subprocess","request":{"command":["sleep","9"]}}`)
	rec := do(t, mux, "POST", "/internal/core/personas/"+pa+"/jobs/j-1/cancel", tok, `{}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"cancelled"`) {
		t.Fatalf("queued cancel: %d %s", rec.Code, rec.Body)
	}
	rec = do(t, mux, "GET", "/internal/core/personas/"+pa+"/inputs/job:j-1", tok, "")
	if rec.Code != 200 {
		t.Fatalf("cancel notification: %d %s", rec.Code, rec.Body)
	}
	// Unknown job → 404; malformed request bodies → 400.
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/jobs/ghost/cancel", tok, `{}`)
	if rec.Code != 404 {
		t.Fatalf("ghost cancel: %d, want 404", rec.Code)
	}
	rec = do(t, mux, "GET", "/internal/core/personas/"+pa+"/jobs/ghost", tok, "")
	if rec.Code != 404 {
		t.Fatalf("ghost get: %d, want 404", rec.Code)
	}
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/jobs/claim", tok,
		`{"runner_id":"r-1","kinds":[],"lease_ms":60000}`)
	if rec.Code != 400 {
		t.Fatalf("claim without kinds: %d, want 400", rec.Code)
	}
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/jobs/j-1/complete", tok,
		`{"runner_id":"r-1"}`)
	if rec.Code != 400 {
		t.Fatalf("complete without status: %d, want 400", rec.Code)
	}
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/jobs/j-1/complete", tok,
		`{"runner_id":"r-1","status":"running"}`)
	if rec.Code != 400 {
		t.Fatalf("complete bad status: %d, want 400", rec.Code)
	}
	// A job_id literally named "claim" can never be addressed → 400.
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/jobs", tok,
		`{"job_id":"claim","kind":"subprocess","request":{"command":["echo"]}}`)
	if rec.Code != 400 {
		t.Fatalf("reserved job_id: %d, want 400", rec.Code)
	}
	// Unknown fields are rejected like the rest of the surface.
	rec = do(t, mux, "POST", "/internal/core/personas/"+pa+"/jobs", tok,
		`{"job_id":"j-2","kind":"subprocess","request":{"command":["echo"]},"runner_id":"spoof"}`)
	if rec.Code != 400 {
		t.Fatalf("submit with unknown field: %d, want 400", rec.Code)
	}
}
