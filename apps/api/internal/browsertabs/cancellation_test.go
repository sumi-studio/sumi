package browsertabs

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

// Exercise the existing model-visible job.cancel tool, not a browser-only
// cancellation bypass. The actual Secretary persists the call and its receipt.
func (f *fixture) cancelThroughCore(id string) agentstate.Job {
	f.t.Helper()
	ctx := context.Background()
	_, _, e := f.core.Store().SubmitInput(ctx, &agentstate.Input{PersonaID: persona, InputID: uuid.NewString(), Kind: "message", Payload: map[string]any{"text": "Cancel this browser job"}, ActorKind: "human", ActorID: owner, SourceSurface: "browser-cancel-test", Attention: "reply"})
	if e != nil {
		f.t.Fatal(e)
	}
	script, e := filepath.Abs("../../../core/scripts/browser-cancel-child.mjs")
	if e != nil {
		f.t.Fatal(e)
	}
	cmd := exec.Command("node", script)
	cmd.Env = append(os.Environ(), "BROWSER_TEST_URL="+f.url, "BROWSER_TEST_PERSONA="+persona, "BROWSER_TEST_TOKEN="+f.core.PersonaToken(persona), "BROWSER_TEST_JOB="+id)
	out, e := cmd.CombinedOutput()
	if e != nil {
		f.t.Fatalf("Secretary job.cancel: %v\n%s", e, out)
	}
	f.t.Logf("Secretary cancellation receipt: %s", out)
	j, e := f.core.Store().GetJob(ctx, persona, id)
	if e != nil {
		f.t.Fatal(e)
	}
	return j
}
func (f *fixture) hostCompletion(id, token, jobID string, result map[string]any) int {
	f.t.Helper()
	status, b := f.api("POST", "/api/browser-host/tabs/"+id+"/complete", token, map[string]any{"job_id": jobID, "status": "done", "result": result, "error": ""})
	if status != 200 && status != 409 {
		f.t.Fatalf("completion status %d %s", status, b)
	}
	return status
}
func (f *fixture) assertOneNotification(id string) {
	f.t.Helper()
	var count int
	if e := f.store.Pool.QueryRow(context.Background(), `SELECT count(*) FROM core_inputs WHERE persona_id=$1 AND input_id=$2`, persona, "job:"+id).Scan(&count); e != nil || count != 1 {
		f.t.Fatalf("notification count %d: %v", count, e)
	}
}
func (f *fixture) claimExpected(a Attachment, token, want string) *agentstate.Job {
	f.t.Helper()
	j, e := f.store.Claim(context.Background(), a.ID, token)
	if e != nil || j == nil || j.JobID != want {
		f.t.Fatalf("claim expected %s: %+v %v", want, j, e)
	}
	return j
}

func TestBrowserCancellationBeforeAndAfterDispatch(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	a, token := f.attach(true)
	if _, e := f.store.Claim(ctx, a.ID, token); e != nil {
		t.Fatal(e)
	}
	t.Run("queued_cancel_is_never_dispatched_and_does_not_block_next", func(t *testing.T) {
		childFixture := *f
		childFixture.t = t
		f := &childFixture
		cancelled := f.enqueue(a, "act")
		later := f.enqueue(a, "observe")
		j := f.cancelThroughCore(cancelled)
		if j.Status != "cancelled" || j.ClaimedBy != nil || j.StartedAt != nil || j.FinishedAt == nil || j.CancelRequestedAt == nil || j.Result != nil {
			t.Fatalf("not an honest never-claimed cancel: %+v", j)
		}
		f.assertOneNotification(cancelled)
		f.claimExpected(a, token, later)
		if s := f.hostCompletion(a.ID, token, later, map[string]any{"dispatched": true, "outcome": "returned"}); s != 200 {
			t.Fatal(s)
		}
		if repeat, e := f.store.Claim(ctx, a.ID, token); e != nil || repeat != nil {
			t.Fatal("cancelled command replayed", repeat, e)
		}
	})
	t.Run("cancel_after_dispatch_keeps_actual_result_and_receipt_replay", func(t *testing.T) {
		childFixture := *f
		childFixture.t = t
		f := &childFixture
		id := f.enqueue(a, "act")
		f.claimExpected(a, token, id)
		j := f.cancelThroughCore(id)
		if j.Status != "cancel_requested" || j.FinishedAt != nil || j.ClaimedBy == nil {
			t.Fatalf("dispatch was incorrectly called undone: %+v", j)
		}
		result := map[string]any{"dispatched": true, "outcome": "returned", "value": map[string]any{"status": "dispatched"}}
		// Model a client forgetting the first successful response; API must accept
		// the identical receipt after cancellation without creating a second event.
		if s := f.hostCompletion(a.ID, token, id, result); s != 200 {
			t.Fatal(s)
		}
		j = f.cancelThroughCore(id)
		if j.Status != "done" || j.Result["dispatched"] != true || j.CancelRequestedAt == nil {
			t.Fatal(j)
		}
		if s := f.hostCompletion(a.ID, token, id, result); s != 200 {
			t.Fatal(s)
		}
		f.assertOneNotification(id)
		later := f.enqueue(a, "observe")
		f.claimExpected(a, token, later)
		f.hostCompletion(a.ID, token, later, map[string]any{"dispatched": true, "outcome": "returned"})
	})
	t.Run("cancelled_dispatched_job_with_lost_result_never_requeues", func(t *testing.T) {
		childFixture := *f
		childFixture.t = t
		f := &childFixture
		id := f.enqueue(a, "act")
		f.claimExpected(a, token, id)
		f.cancelThroughCore(id)
		// Expire the owned fixture row deterministically; do not sleep near a lease.
		if _, e := f.store.Pool.Exec(ctx, `UPDATE core_jobs SET claim_expires_at=now()-interval '1 second' WHERE persona_id=$1 AND job_id=$2`, persona, id); e != nil {
			t.Fatal(e)
		}
		if e := f.store.Sweep(ctx); e != nil {
			t.Fatal(e)
		}
		j, e := f.core.Store().GetJob(ctx, persona, id)
		if e != nil || j.Status != "lost" || j.CancelRequestedAt == nil || j.Result["reason"] != "claim_expired" {
			t.Fatal(j, e)
		}
		if value, present := j.Result["dispatched"]; present && value == false {
			t.Fatal("unknown execution was falsely reported never dispatched")
		}
		f.assertOneNotification(id)
		if s := f.hostCompletion(a.ID, token, id, map[string]any{"dispatched": true, "outcome": "returned"}); s != 409 {
			t.Fatalf("late result must preserve lost verdict: %d", s)
		}
		later := f.enqueue(a, "observe")
		f.claimExpected(a, token, later)
		f.hostCompletion(a.ID, token, later, map[string]any{"dispatched": true, "outcome": "returned"})
		if repeat, e := f.store.Claim(ctx, a.ID, token); e != nil || repeat != nil {
			t.Fatal("expired/cancelled command replayed", repeat, e)
		}
	})
}
