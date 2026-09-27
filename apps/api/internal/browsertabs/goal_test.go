package browsertabs

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

func (f *fixture) tool(name string, req map[string]any) (map[string]any, error) {
	f.t.Helper()
	ctx := context.Background()
	tx, e := f.store.Pool.Begin(ctx)
	if e != nil {
		f.t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	out, e := f.store.Effects()[name].Apply(ctx, tx, persona, uuid.NewString()+":tool:0", req)
	if e == nil {
		if e = tx.Commit(ctx); e != nil {
			f.t.Fatal(e)
		}
	}
	return out, e
}

func (f *fixture) layer(id string) string {
	f.t.Helper()
	out, e := f.tool("browser.tabs", map[string]any{})
	if e != nil {
		f.t.Fatal(e)
	}
	for _, tab := range out["tabs"].([]map[string]any) {
		if tab["attachment_id"] == id {
			return tab["operation_layers"].(map[string]any)["jev"].(string)
		}
	}
	f.t.Fatal("tab not listed")
	return ""
}

func goalReq(id string) map[string]any {
	return map[string]any{"attachment_id": id, "goal": "Sign up and save", "inputs": map[string]any{"email": "ada@example.test", "password": "pw"}, "private_inputs": []any{"password"}, "max_steps": float64(5)}
}

// Jev availability is host-declared per poll; a goal is refused (not queued,
// no substitute model) until the host declares it, and never on a read-only grant.
func TestGoalAdmissionFollowsDeclaredOperationLayer(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	a, token := f.attach(true)
	if _, e := f.store.Claim(ctx, a.ID, token, false); e != nil {
		t.Fatal(e)
	}
	if got := f.layer(a.ID); got != "not_configured" {
		t.Fatal(got)
	}
	_, e := f.tool("browser.goal", goalReq(a.ID))
	if !errors.Is(e, agentstate.ErrBadRequest) || !errors.Is(e, ErrJevUnavailable) {
		t.Fatalf("goal without Jev: %v", e)
	}
	var jobs int
	f.store.Pool.QueryRow(ctx, `SELECT count(*) FROM core_jobs WHERE persona_id=$1`, persona).Scan(&jobs)
	if jobs != 0 {
		t.Fatal("refused goal was queued")
	}
	if _, e := f.store.Claim(ctx, a.ID, token, true); e != nil {
		t.Fatal(e)
	}
	if got := f.layer(a.ID); got != "available" {
		t.Fatal(got)
	}
	out, e := f.tool("browser.goal", goalReq(a.ID))
	if e != nil {
		t.Fatal(e)
	}
	id := out["job"].(map[string]any)["job_id"].(string)
	j, _ := f.core.Store().GetJob(ctx, persona, id)
	if j.Request["method"] != "goal" || j.Request["goal"] != "Sign up and save" || j.Request["inputs"].(map[string]any)["email"] != "ada@example.test" {
		t.Fatalf("goal request %v", j.Request)
	}
	r, rt := f.attach(false)
	f.store.Claim(ctx, r.ID, rt, true)
	if _, e := f.tool("browser.goal", goalReq(r.ID)); !errors.Is(e, ErrUnavailable) {
		t.Fatal("read-only grant accepted a goal", e)
	}
	for name, req := range map[string]map[string]any{
		"empty goal":      {"attachment_id": a.ID, "goal": "  "},
		"bad input name":  {"attachment_id": a.ID, "goal": "x", "inputs": map[string]any{"Email": "v"}},
		"private unknown": {"attachment_id": a.ID, "goal": "x", "private_inputs": []any{"nope"}},
		"steps":           {"attachment_id": a.ID, "goal": "x", "max_steps": float64(41)},
		"too many":        {"attachment_id": a.ID, "goal": "x", "inputs": manyInputs(17)},
	} {
		if e := validateRequest("goal", req); !errors.Is(e, agentstate.ErrBadRequest) {
			t.Fatal(name, e)
		}
	}
}

func manyInputs(n int) map[string]any {
	out := map[string]any{}
	for i := 0; i < n; i++ {
		out["in_"+strings.Repeat("x", i+1)] = "v"
	}
	return out
}

// Progress renews the claim, shows the host's progress to job.status while
// running, and reports cancellation so the host stops before the next action.
// The terminal receipt replaces progress and notifies exactly once.
func TestGoalProgressCancelAndReceipt(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	a, token := f.attach(true)
	f.store.Claim(ctx, a.ID, token, true)
	out, e := f.tool("browser.goal", goalReq(a.ID))
	if e != nil {
		t.Fatal(e)
	}
	job := out["job"].(map[string]any)["job_id"].(string)
	claimed, e := f.store.Claim(ctx, a.ID, token, true)
	if e != nil || claimed == nil || claimed.JobID != job {
		t.Fatal(claimed, e)
	}
	// The goal holds the tab: a direct observation waits for it.
	observe := f.enqueue(a, "observe")
	if next, _ := f.store.Claim(ctx, a.ID, token, true); next != nil {
		t.Fatal("direct observation raced a running goal", next.JobID)
	}
	before := *claimed.ClaimExpiresAt
	f.store.Pool.Exec(ctx, `UPDATE core_jobs SET claim_expires_at=now()+interval '2 seconds' WHERE job_id=$1`, job)
	status, e := f.store.Progress(ctx, a.ID, token, job, map[string]any{"phase": "acting", "step": float64(1)})
	if e != nil || status != "running" {
		t.Fatal(status, e)
	}
	j, _ := f.core.Store().GetJob(ctx, persona, job)
	if !j.ClaimExpiresAt.After(before.Add(-1)) || j.Result["progress"].(map[string]any)["phase"] != "acting" {
		t.Fatalf("progress not visible/renewed: %v %v", j.ClaimExpiresAt, j.Result)
	}
	if _, e = f.core.Store().CancelJob(ctx, persona, job); e != nil {
		t.Fatal(e)
	}
	status, e = f.store.Progress(ctx, a.ID, token, job, map[string]any{"phase": "acting", "step": float64(2)})
	if e != nil || status != "cancel_requested" {
		t.Fatal("cancel not reported to host", status, e)
	}
	result := map[string]any{"dispatched": true, "outcome": "returned", "value": map[string]any{"operation_layer": "jev", "goal_outcome": "cancelled"}}
	for i := 0; i < 2; i++ {
		if _, e = f.store.Complete(ctx, a.ID, token, job, "cancelled", result, ""); e != nil {
			t.Fatal("cancelled receipt", e)
		}
	}
	j, _ = f.core.Store().GetJob(ctx, persona, job)
	if j.Status != "cancelled" || j.Result["progress"] != nil || j.CancelRequestedAt == nil {
		t.Fatalf("terminal goal %s %v", j.Status, j.Result)
	}
	var notes int
	f.store.Pool.QueryRow(ctx, `SELECT count(*) FROM core_inputs WHERE persona_id=$1 AND input_id=$2`, persona, "job:"+job).Scan(&notes)
	if notes != 1 {
		t.Fatal("notifications", notes)
	}
	if _, e = f.store.Progress(ctx, a.ID, token, job, map[string]any{}); !errors.Is(e, agentstate.ErrJobNotClaimed) {
		t.Fatal("progress after terminal", e)
	}
	next, e := f.store.Claim(ctx, a.ID, token, true)
	if e != nil || next == nil || next.JobID != observe {
		t.Fatal("tab slot not released after goal", next, e)
	}
}

// Revocation stops a running goal at its next admission while its receipt is
// still accepted; progress is bounded, applies only to goals, and cannot keep
// a goal alive past its budget or after it was swept lost.
func TestGoalProgressRevocationBoundsAndLoss(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	a, token := f.attach(true)
	f.store.Claim(ctx, a.ID, token, true)
	out, _ := f.tool("browser.goal", goalReq(a.ID))
	job := out["job"].(map[string]any)["job_id"].(string)
	f.store.Claim(ctx, a.ID, token, true)
	for name, p := range map[string]map[string]any{
		"oversized": {"text": strings.Repeat("x", 17<<10)},
		"nul":       {"title": "a\x00b"},
	} {
		if _, e := f.store.Progress(ctx, a.ID, token, job, p); !errors.Is(e, ErrInvalid) {
			t.Fatal(name, e)
		}
	}
	status, _ := f.api("POST", "/api/browser-host/tabs/"+a.ID+"/progress", "wrong-token", map[string]any{"job_id": job, "progress": map[string]any{}})
	if status != 403 {
		t.Fatal("unauthenticated progress", status)
	}
	f.store.Pool.Exec(ctx, `UPDATE core_jobs SET started_at=now()-interval '7 minutes' WHERE job_id=$1`, job)
	status, _ = f.api("POST", "/api/browser-host/tabs/"+a.ID+"/progress", token, map[string]any{"job_id": job, "progress": map[string]any{}})
	if status != 409 {
		t.Fatal("over-budget goal renewed", status)
	}
	f.store.Pool.Exec(ctx, `UPDATE core_jobs SET started_at=now() WHERE job_id=$1`, job)
	if e := f.store.Revoke(ctx, owner, a.ID); e != nil {
		t.Fatal(e)
	}
	status, _ = f.api("POST", "/api/browser-host/tabs/"+a.ID+"/progress", token, map[string]any{"job_id": job, "progress": map[string]any{}})
	if status != 403 {
		t.Fatal("revoked grant renewed goal", status)
	}
	receipt := map[string]any{"dispatched": true, "outcome": "returned", "code": "grant_revoked", "value": map[string]any{"goal_outcome": "revoked"}}
	if _, e := f.store.Complete(ctx, a.ID, token, job, "failed", receipt, "Browser goal stopped"); e != nil {
		t.Fatal("receipt after revocation", e)
	}

	// Progress on a direct observation is refused; a lost goal is not revived.
	c, ct := f.attach(true)
	f.store.Claim(ctx, c.ID, ct, true)
	obs := f.enqueue(c, "observe")
	f.store.Claim(ctx, c.ID, ct, true)
	if _, e := f.store.Progress(ctx, c.ID, ct, obs, map[string]any{}); !errors.Is(e, agentstate.ErrJobNotClaimed) {
		t.Fatal("progress renewed a direct job", e)
	}
	f.store.Complete(ctx, c.ID, ct, obs, "done", map[string]any{"dispatched": true, "outcome": "returned"}, "")
	out, _ = f.tool("browser.goal", goalReq(c.ID))
	lost := out["job"].(map[string]any)["job_id"].(string)
	f.store.Claim(ctx, c.ID, ct, true)
	f.store.Pool.Exec(ctx, `UPDATE core_jobs SET claim_expires_at=now()-interval '1 second' WHERE job_id=$1`, lost)
	if e := f.store.Sweep(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e := f.store.Progress(ctx, c.ID, ct, lost, map[string]any{}); !errors.Is(e, agentstate.ErrJobNotClaimed) {
		t.Fatal("lost goal revived", e)
	}
	late := map[string]any{"dispatched": true, "outcome": "returned", "value": map[string]any{"goal_outcome": "claim_lost"}}
	if j, e := f.store.Complete(ctx, c.ID, ct, lost, "failed", late, "Browser goal stopped"); e != nil || j.Status != "lost" {
		t.Fatal("late goal receipt", j.Status, e)
	}
}

// Renewal and completion belong to the claiming host only; availability is a
// current declaration, not a sticky flag; cancellation is not renewed.
func TestGoalClaimOwnershipAndCurrentAvailability(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	a, token := f.attach(true)
	b, btoken := f.attach(true)
	f.store.Claim(ctx, a.ID, token, true)
	f.store.Claim(ctx, b.ID, btoken, true)
	out, e := f.tool("browser.goal", goalReq(a.ID))
	if e != nil {
		t.Fatal(e)
	}
	job := out["job"].(map[string]any)["job_id"].(string)
	if c, _ := f.store.Claim(ctx, a.ID, token, true); c == nil || c.JobID != job {
		t.Fatal("claim", c)
	}
	// Another live host of the same persona cannot renew or complete it,
	// and a token only authenticates its own attachment.
	if _, e = f.store.Progress(ctx, b.ID, btoken, job, map[string]any{}); !errors.Is(e, agentstate.ErrJobNotClaimed) {
		t.Fatal("other host renewed the goal", e)
	}
	if _, e = f.store.Complete(ctx, b.ID, btoken, job, "done", map[string]any{"dispatched": false, "outcome": "not_dispatched"}, ""); !errors.Is(e, ErrUnavailable) {
		t.Fatal("other host completed the goal", e)
	}
	if _, e = f.store.Progress(ctx, b.ID, token, job, map[string]any{}); !errors.Is(e, ErrUnavailable) {
		t.Fatal("token crossed attachments", e)
	}
	// Cancellation is reported but no longer renews the claim.
	f.store.Pool.Exec(ctx, `UPDATE core_jobs SET claim_expires_at=now()+interval '2 seconds' WHERE job_id=$1`, job)
	if _, e = f.core.Store().CancelJob(ctx, persona, job); e != nil {
		t.Fatal(e)
	}
	status, e := f.store.Progress(ctx, a.ID, token, job, map[string]any{"phase": "deciding"})
	if e != nil || status != "cancel_requested" {
		t.Fatal(status, e)
	}
	var renewed bool
	f.store.Pool.QueryRow(ctx, `SELECT claim_expires_at>now()+interval '5 seconds' FROM core_jobs WHERE job_id=$1`, job).Scan(&renewed)
	if renewed {
		t.Fatal("cancel_requested goal was renewed")
	}
	// A host that withdraws Jev (e.g. its key was rejected) or stops polling
	// is not reported or admitted as Jev-available.
	if _, e = f.store.Claim(ctx, b.ID, btoken, false); e != nil {
		t.Fatal(e)
	}
	if got := f.layer(b.ID); got != "not_configured" {
		t.Fatal("withdrawn declaration", got)
	}
	f.store.Claim(ctx, b.ID, btoken, true)
	f.store.Pool.Exec(ctx, `UPDATE browser_tab_attachments SET last_seen_at=now()-interval '2 minutes' WHERE attachment_id=$1`, b.ID)
	if got := f.layer(b.ID); got != "host_offline" {
		t.Fatal("stale declaration reported", got)
	}
	if _, e = f.tool("browser.goal", goalReq(b.ID)); !errors.Is(e, ErrUnavailable) {
		t.Fatal("goal admitted for an offline host", e)
	}
}
