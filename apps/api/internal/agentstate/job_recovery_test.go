//go:build integration

// Real-Postgres coverage for the job-runner shared seam added for the
// Linux-on-demand backend (M09 follow-up): kind-filtered discovery and the
// original-owner observed-outcome enrichment on 'lost' jobs.

package agentstate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestPersonasWithRunnableJobs_KindFilteredAndActiveOnly(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	pidA := pid(t)
	mustPersona(t, s, pidA)
	pidB := pid(t)
	mustPersona(t, s, pidB)
	pidC := pid(t)
	mustPersona(t, s, pidC)

	// A: queued subprocess job. B: queued job of a foreign kind (inserted
	// directly — "script" is not a registered kind yet, but the discovery
	// seam must filter on kind regardless of the admission table). C:
	// running subprocess job (already claimed — the runnable query must
	// not re-offer it).
	if _, _, err := s.SubmitJob(ctx, pidA, "j-a1", "subprocess", subReq("echo", "hi"), "assistant"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO core_jobs
		(persona_id, job_id, kind, request, status, created_by)
		VALUES ($1, 'j-b1', 'script', '{"source":"x"}', 'queued', 'assistant')`, pidB); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.SubmitJob(ctx, pidC, "j-c1", "subprocess", subReq("echo", "hi"), "assistant"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ClaimJobs(ctx, pidC, "runner-x", []string{"subprocess"}, 2*time.Minute, 4, "*"); err != nil {
		t.Fatal(err)
	}

	got, err := s.PersonasWithRunnableJobs(ctx, []string{"subprocess"}, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != pidA {
		t.Fatalf("expected only %s runnable for subprocess, got %v", pidA, got)
	}

	// script kind sees B.
	got, err = s.PersonasWithRunnableJobs(ctx, []string{"script"}, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != pidB {
		t.Fatalf("expected only %s runnable for script, got %v", pidB, got)
	}

	// A sealed persona's queued job is never offered.
	if _, _, err := s.SubmitJob(ctx, pidC, "j-c2", "subprocess", subReq("echo", "hi"), "assistant"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE core_personas SET authority='sealed' WHERE persona_id=$1`, pidC); err != nil {
		t.Fatal(err)
	}
	got, err = s.PersonasWithRunnableJobs(ctx, []string{"subprocess"}, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range got {
		if g == pidC {
			t.Fatalf("sealed persona %s must not be offered", pidC)
		}
	}
}

func TestPersonasWithClaimedJobs_StatesAndOwner(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)

	if _, _, err := s.SubmitJob(ctx, pa, "jc-1", "subprocess", subReq("echo", "a"), "assistant"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ClaimJobs(ctx, pa, "jobexec-docker", []string{"subprocess"}, 2*time.Minute, 4, "*"); err != nil {
		t.Fatal(err)
	}

	// running claim → discovered.
	got, err := s.PersonasWithClaimedJobs(ctx, "jobexec-docker", []string{"subprocess"}, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != pa {
		t.Fatalf("claimed persona missing: %v", got)
	}
	// wrong runner → not discovered.
	got, err = s.PersonasWithClaimedJobs(ctx, "script-runner", []string{"subprocess"}, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("foreign runner must not see the claim: %v", got)
	}
	// wrong kind → not discovered.
	got, err = s.PersonasWithClaimedJobs(ctx, "jobexec-docker", []string{"script"}, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("kind filter failed: %v", got)
	}

	// Terminal jobs drop out of reconciliation.
	if _, err := s.CompleteJob(ctx, pa, "jc-1", "jobexec-docker", "done", map[string]any{"exit_code": 0}, ""); err != nil {
		t.Fatal(err)
	}
	got, err = s.PersonasWithClaimedJobs(ctx, "jobexec-docker", []string{"subprocess"}, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("terminal job must leave reconciliation: %v", got)
	}
}

func TestAttachLostOutcome_OwnerEnriches(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)

	if _, _, err := s.SubmitJob(ctx, pa, "jl-1", "subprocess", subReq("make", "target"), "assistant"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ClaimJobs(ctx, pa, "jobexec-docker", []string{"subprocess"}, time.Millisecond, 4, "*"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	// Kind-blind sweep, as a foreign runner's claim pass would do.
	if _, swept, err := s.ClaimJobs(ctx, pa, "script-runner", []string{"script"}, 2*time.Minute, 4, "*"); err != nil {
		t.Fatal(err)
	} else if len(swept) != 1 {
		t.Fatalf("expected sweep of expired claim, got %v", swept)
	}

	lost, err := s.GetJob(ctx, pa, "jl-1")
	if err != nil {
		t.Fatal(err)
	}
	if lost.Status != "lost" {
		t.Fatalf("precondition: status %s", lost.Status)
	}
	if lost.NotifiedAt == nil {
		t.Fatal("precondition: lost transition must have notified")
	}

	outcome := map[string]any{
		"state": "succeeded", "exit_code": 0, "stdout": "built ok\n",
		"stderr": "", "stdout_truncated": false, "stderr_truncated": false,
		"duration_ms": 412, "finished_at": "2026-09-18T10:00:00Z",
	}
	got, err := s.AttachLostOutcome(ctx, pa, "jl-1", "jobexec-docker", outcome)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "lost" {
		t.Fatalf("verdict must stay lost, got %s", got.Status)
	}
	attached, ok := got.Result["observed_outcome"].(map[string]any)
	if !ok {
		t.Fatalf("observed_outcome missing: %#v", got.Result)
	}
	if attached["exit_code"] != 0.0 {
		t.Fatalf("exit_code mismatch: %#v", attached["exit_code"])
	}
	if attached["stdout"] != "built ok\n" {
		t.Fatalf("stdout mismatch: %#v", attached["stdout"])
	}
	// The sweep marker survives alongside the outcome.
	if got.Result["reason"] != "claim_expired" {
		t.Fatalf("sweep marker lost: %#v", got.Result)
	}
	// Identical re-attach replays.
	got2, err := s.AttachLostOutcome(ctx, pa, "jl-1", "jobexec-docker", outcome)
	if err != nil {
		t.Fatalf("idempotent replay must succeed: %v", err)
	}
	if got2.Status != "lost" {
		t.Fatalf("replay changed status: %s", got2.Status)
	}
	// Divergent re-attach refuses; stored outcome stays.
	divergent := map[string]any{"state": "succeeded", "exit_code": 7}
	if _, err := s.AttachLostOutcome(ctx, pa, "jl-1", "jobexec-docker", divergent); !errors.Is(err, ErrJobConflict) {
		t.Fatalf("divergent attach must conflict, got %v", err)
	}
	fresh, err := s.GetJob(ctx, pa, "jl-1")
	if err != nil {
		t.Fatal(err)
	}
	attached, _ = fresh.Result["observed_outcome"].(map[string]any)
	if attached["stdout"] != "built ok\n" {
		t.Fatalf("divergent attach must not replace: %#v", attached)
	}

	// Wrong owner refuses and does not attach.
	pa2 := pid(t)
	mustPersona(t, s, pa2)
	if _, _, err := s.SubmitJob(ctx, pa2, "jl-2", "subprocess", subReq("x"), "assistant"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ClaimJobs(ctx, pa2, "jobexec-docker", []string{"subprocess"}, time.Millisecond, 4, "*"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, _, err := s.ClaimJobs(ctx, pa2, "other", []string{"subprocess"}, time.Minute, 4, "*"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AttachLostOutcome(ctx, pa2, "jl-2", "other-runner", outcome); !errors.Is(err, ErrJobNotClaimed) {
		t.Fatalf("wrong owner must refuse, got %v", err)
	}
	fresh2, err := s.GetJob(ctx, pa2, "jl-2")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := fresh2.Result["observed_outcome"]; ok {
		t.Fatal("wrong-owner attach must not write")
	}

	// Non-lost status refuses.
	if _, _, err := s.SubmitJob(ctx, pa, "jl-3", "subprocess", subReq("y"), "assistant"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ClaimJobs(ctx, pa, "jobexec-docker", []string{"subprocess"}, time.Minute, 4, "*"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteJob(ctx, pa, "jl-3", "jobexec-docker", "done", map[string]any{"exit_code": 0}, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AttachLostOutcome(ctx, pa, "jl-3", "jobexec-docker", outcome); !errors.Is(err, ErrJobConflict) {
		t.Fatalf("attach to non-lost job must conflict, got %v", err)
	}
	fresh3, err := s.GetJob(ctx, pa, "jl-3")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := fresh3.Result["observed_outcome"]; ok {
		t.Fatal("observed_outcome must not appear on done job")
	}
}

// Admission must refuse every subprocess spec that could never map to a
// launchable process request — the bounds mirror the process contract so an
// admitted job is never claimed and then failed on overflow.
func TestSubmitJobSubprocessBounds(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)

	bad := []struct {
		name string
		req  map[string]any
	}{
		{"too many argv", map[string]any{"command": func() []any {
			c := make([]any, 130)
			for i := range c {
				c[i] = "x"
			}
			return c
		}()}},
		{"argv payload", map[string]any{"command": []any{"e", strings.Repeat("a", 33<<10)}}},
		{"executable too long", map[string]any{"command": []any{strings.Repeat("e", 1025)}}},
		{"NUL in argv", map[string]any{"command": []any{"e", "a\x00b"}}},
		{"absolute cwd", map[string]any{"command": []any{"e"}, "cwd": "/etc"}},
		{"escaping cwd", map[string]any{"command": []any{"e"}, "cwd": "../outside"}},
		{"unclean cwd", map[string]any{"command": []any{"e"}, "cwd": "./a/../b"}},
		{"too many env", map[string]any{"command": []any{"e"}, "env": func() map[string]any {
			m := map[string]any{}
			for i := 0; i < 33; i++ {
				m[fmt.Sprintf("K%d", i)] = "v"
			}
			return m
		}()}},
		{"bad env name", map[string]any{"command": []any{"e"}, "env": map[string]any{"9BAD": "v"}}},
		{"reserved env", map[string]any{"command": []any{"e"}, "env": map[string]any{"PATH": "/evil"}}},
		{"reserved env HOME", map[string]any{"command": []any{"e"}, "env": map[string]any{"HOME": "/x"}}},
		{"env value bound", map[string]any{"command": []any{"e"}, "env": map[string]any{"K": strings.Repeat("v", 4097)}}},
		{"env payload", map[string]any{"command": []any{"e"}, "env": map[string]any{"A": strings.Repeat("v", 4096), "B": strings.Repeat("v", 4096), "C": "x"}}},
	}
	for i, tc := range bad {
		if _, _, err := s.SubmitJob(ctx, pa, fmt.Sprintf("bound-%d", i), "subprocess", tc.req, "assistant"); !errors.Is(err, ErrBadRequest) {
			t.Fatalf("%s: admission must refuse, got %v", tc.name, err)
		}
	}

	// The maximum legal spec still admits.
	maxEnv := map[string]any{}
	for i := 0; i < 32; i++ {
		maxEnv[fmt.Sprintf("JOB_VAR_%02d", i)] = "v"
	}
	if _, _, err := s.SubmitJob(ctx, pa, "bound-max", "subprocess", map[string]any{
		"command":    []any{"/bin/sh", "-c", "true"},
		"cwd":        "a/b/c",
		"timeout_ms": float64(3_600_000),
		"env":        maxEnv,
	}, "assistant"); err != nil {
		t.Fatalf("maximum legal spec must admit: %v", err)
	}
}

// Job-level unresolved discovery: an old unresolved job must surface even
// behind hundreds of newer resolved-lost rows and mixed-kind/foreign-owner
// records — the filtering happens in SQL, not in a newest-first window.
func TestClaimJobsNeedingAttention_FiltersAndFairCursor(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)

	// The one unresolved job this runner must find: claimed, running.
	if _, _, err := s.SubmitJob(ctx, pa, "old-unresolved", "subprocess", subReq("echo", "x"), "assistant"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ClaimJobs(ctx, pa, "jobexec-docker", []string{"subprocess"}, 2*time.Minute, 4, "*"); err != nil {
		t.Fatal(err)
	}

	// 250 newer resolved-lost rows (observed_outcome attached), a
	// foreign-kind live claim, and a foreign-runner live claim — all of
	// which would flood a newest-first per-persona window ahead of the
	// unresolved row.
	for i := 0; i < 250; i++ {
		if _, err := s.pool.Exec(ctx, `INSERT INTO core_jobs
			(persona_id, job_id, kind, request, status, claimed_by, result, created_by, finished_at)
			VALUES ($1, $2, 'subprocess', '{}', 'lost', 'jobexec-docker',
			        '{"reason":"claim_expired","observed_outcome":{"state":"succeeded"}}', 'assistant', now())`,
			pa, fmt.Sprintf("resolved-%04d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO core_jobs
		(persona_id, job_id, kind, request, status, claimed_by, created_by)
		VALUES ($1, 'foreign-kind', 'script', '{}', 'running', 'jobexec-docker', 'assistant'),
		       ($1, 'foreign-owner', 'subprocess', '{}', 'running', 'other-runner', 'assistant')`, pa); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO core_jobs
		(persona_id, job_id, kind, request, status, claimed_by, result, created_by)
		VALUES ($1, 'resolved-nooutcome-foreign', 'subprocess', '{}', 'lost', 'other-runner',
		        '{"reason":"claim_expired"}', 'assistant')`, pa); err != nil {
		t.Fatal(err)
	}

	// Page through with a small bound: every page is kind/owner/resolution
	// filtered, so the unresolved row cannot be masked — it must appear in
	// the very first page, and the set must contain ONLY it.
	got, err := s.ClaimJobsNeedingAttention(ctx, "jobexec-docker", []string{"subprocess"}, 8, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].JobID != "old-unresolved" {
		t.Fatalf("unresolved discovery must surface exactly the live job, got %v", jobIDs(got))
	}
	// Kind filtering is symmetric: asking for 'script' surfaces the
	// script-kind claim we own — the same seam serves the scripts runner —
	// while subprocess pages never see it. Foreign runners see nothing.
	if got, err := s.ClaimJobsNeedingAttention(ctx, "jobexec-docker", []string{"script"}, 8, "", ""); err != nil || len(got) != 1 || got[0].JobID != "foreign-kind" {
		t.Fatalf("script-kind claim must be discoverable to its owner: %v %v", jobIDs(got), err)
	}
	if got, err := s.ClaimJobsNeedingAttention(ctx, "other-runner", []string{"subprocess"}, 8, "", ""); err != nil || len(got) != 2 {
		t.Fatalf("other-runner must see only its own unresolved work: %v %v", jobIDs(got), err)
	}
	if got, err := s.ClaimJobsNeedingAttention(ctx, "nobody", []string{"subprocess"}, 8, "", ""); err != nil || len(got) != 0 {
		t.Fatalf("owner filter failed: %v %v", jobIDs(got), err)
	}
}

// A lost row whose observed outcome is attached leaves the unresolved set:
// fully resolved work stops generating backend reads by construction.
func TestClaimJobsNeedingAttention_ResolvedLostExitsDiscovery(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)

	if _, _, err := s.SubmitJob(ctx, pa, "jl-x", "subprocess", subReq("x"), "assistant"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ClaimJobs(ctx, pa, "jobexec-docker", []string{"subprocess"}, time.Millisecond, 4, "*"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, _, err := s.ClaimJobs(ctx, pa, "sweeper", []string{"subprocess"}, time.Minute, 4, "*"); err != nil {
		t.Fatal(err)
	}
	got, err := s.ClaimJobsNeedingAttention(ctx, "jobexec-docker", []string{"subprocess"}, 8, "", "")
	if err != nil || len(got) != 1 {
		t.Fatalf("unresolved lost row must be discovered: %v %v", jobIDs(got), err)
	}
	if _, err := s.AttachLostOutcome(ctx, pa, "jl-x", "jobexec-docker",
		map[string]any{"state": "succeeded", "exit_code": 0}); err != nil {
		t.Fatal(err)
	}
	got, err = s.ClaimJobsNeedingAttention(ctx, "jobexec-docker", []string{"subprocess"}, 8, "", "")
	if err != nil || len(got) != 0 {
		t.Fatalf("resolved lost row must exit discovery: %v %v", jobIDs(got), err)
	}
}

// The fair cursor walks the whole unresolved set over bounded pages.
func TestClaimJobsNeedingAttention_CursorWalksAll(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	pb := pid(t)
	mustPersona(t, s, pb)

	for i := 0; i < 5; i++ {
		p := pa
		if i%2 == 1 {
			p = pb
		}
		if _, _, err := s.SubmitJob(ctx, p, fmt.Sprintf("j-%d", i), "subprocess", subReq("x"), "assistant"); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.ClaimJobs(ctx, p, "jobexec-docker", []string{"subprocess"}, 2*time.Minute, 4, "*"); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	afterP, afterJ := "", ""
	for i := 0; i < 8; i++ {
		page, err := s.ClaimJobsNeedingAttention(ctx, "jobexec-docker", []string{"subprocess"}, 2, afterP, afterJ)
		if err != nil {
			t.Fatal(err)
		}
		for _, j := range page {
			seen[j.JobID] = true
		}
		if len(page) < 2 {
			break
		}
		afterP, afterJ = page[len(page)-1].PersonaID, page[len(page)-1].JobID
	}
	if len(seen) != 5 {
		t.Fatalf("cursor must walk all 5 unresolved jobs, saw %v", seen)
	}
}

// Runnable discovery is job-granular with the same fair cursor.
func TestRunnableJobs_PagesAndFilters(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)

	for i := 0; i < 3; i++ {
		if _, _, err := s.SubmitJob(ctx, pa, fmt.Sprintf("q-%d", i), "subprocess", subReq("x"), "assistant"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO core_jobs
		(persona_id, job_id, kind, request, status, created_by)
		VALUES ($1, 'foreign', 'script', '{}', 'queued', 'assistant')`, pa); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	afterP, afterJ := "", ""
	for i := 0; i < 6; i++ {
		page, err := s.RunnableJobs(ctx, []string{"subprocess"}, 2, afterP, afterJ)
		if err != nil {
			t.Fatal(err)
		}
		for _, j := range page {
			if j.Status != "queued" {
				t.Fatalf("runnable discovery returned non-queued %s", j.JobID)
			}
			seen[j.JobID] = true
		}
		if len(page) < 2 {
			break
		}
		afterP, afterJ = page[len(page)-1].PersonaID, page[len(page)-1].JobID
	}
	if len(seen) != 3 || seen["foreign"] {
		t.Fatalf("runnable walk must return exactly the 3 subprocess jobs: %v", seen)
	}
}

// Claim routing: cloud-stamped work is unreachable to local claimants and
// vice versa; '*' claims either side. Unstamped requests claim as local.
func TestClaimJobs_BackendRouting(t *testing.T) {
	s, _ := newStore(t)
	// A cloud-routed submission presumes a live Cloud runner; the routing
	// test declares one the way post-probe wiring does.
	s.SetJobBackendAvailable("cloud")
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)

	submit := func(id string, req map[string]any) {
		t.Helper()
		if _, _, err := s.SubmitJob(ctx, pa, id, "subprocess", req, "assistant"); err != nil {
			t.Fatal(err)
		}
	}
	submit("local-job", subReq("echo", "l"))
	cloudReq := subReq("echo", "c")
	cloudReq["backend"] = "cloud"
	submit("cloud-job", cloudReq)
	explicitLocal := subReq("echo", "el")
	explicitLocal["backend"] = "local"
	submit("explicit-local", explicitLocal)

	// A local claim pass takes unstamped + explicit-local, never cloud.
	claimed, _, err := s.ClaimJobs(ctx, pa, "local-runner", []string{"subprocess"}, 2*time.Minute, 8, "")
	if err != nil {
		t.Fatal(err)
	}
	ids := jobIDs(claimed)
	if len(ids) != 2 || !contains(ids, "local-job") || !contains(ids, "explicit-local") {
		t.Fatalf("local claim pass must take only local work: %v", ids)
	}
	// A cloud claim pass takes only cloud-stamped work.
	claimed, _, err = s.ClaimJobs(ctx, pa, "jobexec-docker", []string{"subprocess"}, 2*time.Minute, 8, "cloud")
	if err != nil {
		t.Fatal(err)
	}
	if ids = jobIDs(claimed); len(ids) != 1 || ids[0] != "cloud-job" {
		t.Fatalf("cloud claim pass must take only cloud work: %v", ids)
	}
	// '*' claims anything queued — for reconcilers that don't route.
	submit("star-job", subReq("echo", "s"))
	claimed, _, err = s.ClaimJobs(ctx, pa, "star", []string{"subprocess"}, 2*time.Minute, 8, "*")
	if err != nil {
		t.Fatal(err)
	}
	if ids = jobIDs(claimed); len(ids) != 1 || ids[0] != "star-job" {
		t.Fatalf("wildcard claim pass: %v", ids)
	}
	// Invalid backend values are rejected at admission.
	if _, _, err := s.SubmitJob(ctx, pa, "bad-backend", "subprocess",
		map[string]any{"command": []any{"x"}, "backend": "neptune"}, "assistant"); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("backend enum must be enforced: %v", err)
	}
}

// The deployment default stamps new submissions; explicit values win;
// resubmitting the unstamped request replays against the stamped row.
func TestSubmitJob_DefaultBackendStamp(t *testing.T) {
	s, _ := newStore(t)
	s.SetDefaultJobBackend("cloud")
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)

	j, created, err := s.SubmitJob(ctx, pa, "stamped", "subprocess", subReq("echo", "x"), "assistant")
	if err != nil || !created {
		t.Fatalf("submit: %v %v", j, err)
	}
	if j.Request["backend"] != "cloud" {
		t.Fatalf("default backend must be stamped: %#v", j.Request)
	}
	// The same unstamped request replays — the stamp is part of identity.
	j2, created2, err := s.SubmitJob(ctx, pa, "stamped", "subprocess", subReq("echo", "x"), "assistant")
	if err != nil || created2 {
		t.Fatalf("replay must succeed: %v %v", j2, err)
	}
	// Explicit backend is never overridden.
	l, _, err := s.SubmitJob(ctx, pa, "explicit", "subprocess",
		map[string]any{"command": []any{"x"}, "backend": "local"}, "assistant")
	if err != nil {
		t.Fatal(err)
	}
	if l.Request["backend"] != "local" {
		t.Fatalf("explicit backend must win: %#v", l.Request)
	}
}

// An explicit cloud-routed submission is refused while no Cloud runner is
// wired — queuing it would be a silent forever-wait. Local/unstamped work
// is never gated; once the backend is declared live the same request is
// admitted.
func TestSubmitJob_CloudRefusedWithoutRunner(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)

	cloud := subReq("echo", "x")
	cloud["backend"] = "cloud"
	if _, _, err := s.SubmitJob(ctx, pa, "cloud-job", "subprocess", cloud, "assistant"); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("cloud request without a live runner must be refused: %v", err)
	}
	// Unstamped and explicit-local submissions are unaffected.
	if _, _, err := s.SubmitJob(ctx, pa, "unstamped", "subprocess", subReq("echo", "x"), "assistant"); err != nil {
		t.Fatalf("unstamped submit must not be gated: %v", err)
	}
	local := subReq("echo", "x")
	local["backend"] = "local"
	if _, _, err := s.SubmitJob(ctx, pa, "local-job", "subprocess", local, "assistant"); err != nil {
		t.Fatalf("local submit must not be gated: %v", err)
	}
	// Post-probe wiring declares the backend: the identical request admits.
	s.SetJobBackendAvailable("cloud")
	if _, _, err := s.SubmitJob(ctx, pa, "cloud-job", "subprocess", cloud, "assistant"); err != nil {
		t.Fatalf("cloud request must admit once the runner is live: %v", err)
	}
}

// Durable wait bookkeeping: the cause's first-observed time survives in
// result.runner_wait across reads, resets on cause change, and clears.
func TestNoteWaitLifecycle(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)

	if _, _, err := s.SubmitJob(ctx, pa, "jw", "subprocess", subReq("x"), "assistant"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ClaimJobs(ctx, pa, "jobexec-docker", []string{"subprocess"}, 2*time.Minute, 4, "*"); err != nil {
		t.Fatal(err)
	}
	j, err := s.NoteWait(ctx, pa, "jw", "jobexec-docker", 2*time.Minute, "unknown")
	if err != nil {
		t.Fatal(err)
	}
	cause, since, ok := JobWait(j)
	if !ok || cause != "unknown" {
		t.Fatalf("wait must be recorded: %#v", j.Result)
	}
	// Same cause keeps the original since.
	j2, err := s.NoteWait(ctx, pa, "jw", "jobexec-docker", 2*time.Minute, "unknown")
	if err != nil {
		t.Fatal(err)
	}
	_, since2, _ := JobWait(j2)
	if !since2.Equal(since) {
		t.Fatalf("same-cause wait must keep since: %v → %v", since, since2)
	}
	// Cause change resets the clock.
	j3, err := s.NoteWait(ctx, pa, "jw", "jobexec-docker", 2*time.Minute, "busy")
	if err != nil {
		t.Fatal(err)
	}
	cause3, since3, _ := JobWait(j3)
	if cause3 != "busy" || !since3.After(since) && !since3.Equal(since) {
		t.Fatalf("cause change must record new wait: %v %v", cause3, since3)
	}
	if err := s.ClearWait(ctx, pa, "jw", "jobexec-docker"); err != nil {
		t.Fatal(err)
	}
	fresh, err := s.GetJob(ctx, pa, "jw")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := JobWait(fresh); ok {
		t.Fatalf("cleared wait must be gone: %#v", fresh.Result)
	}
	// A completed job replaces result wholesale — wait bookkeeping cannot
	// leak into the terminal record.
	if _, err := s.CompleteJob(ctx, pa, "jw", "jobexec-docker", "done", map[string]any{"exit_code": 0}, ""); err != nil {
		t.Fatal(err)
	}
	done, _ := s.GetJob(ctx, pa, "jw")
	if _, ok := done.Result["runner_wait"]; ok {
		t.Fatal("terminal result must not carry wait bookkeeping")
	}
}

func jobIDs(jobs []Job) []string {
	out := make([]string, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, j.JobID)
	}
	return out
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// Combined-deployment routing: the two backends share one store, so kind
// and backend must keep their work separated. The cloud default applies
// to subprocess only — a script job must stay unstamped and claimable by
// the local scripts runner even when the deployment default is 'cloud',
// and an explicit non-local backend on a script names no claimant.
func TestSubmitJob_ScriptRoutingCombinedDeployment(t *testing.T) {
	s, _ := newStore(t)
	// The combined deployment: cloud runner live, default backend cloud.
	s.SetJobBackendAvailable("cloud")
	s.SetDefaultJobBackend("cloud")
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	scriptReq := map[string]any{"code": "export function run() { return 1 }"}

	// An unstamped script job must NOT inherit the cloud default — no
	// runner claims script+cloud, so stamping would strand it.
	j, _, err := s.SubmitJob(ctx, pa, "sc-default", "script", scriptReq, "assistant")
	if err != nil {
		t.Fatal(err)
	}
	if b, ok := j.Request["backend"]; ok {
		t.Fatalf("script request must stay unstamped under a cloud default: %v", b)
	}
	// An explicit non-local backend names no script claimant — refused.
	if _, _, err := s.SubmitJob(ctx, pa, "sc-cloud", "script",
		map[string]any{"code": "x", "backend": "cloud"}, "assistant"); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("script+cloud must be refused at admission: %v", err)
	}
	// Explicit local is honored.
	lj, _, err := s.SubmitJob(ctx, pa, "sc-local", "script",
		map[string]any{"code": "x", "backend": "local"}, "assistant")
	if err != nil {
		t.Fatal(err)
	}
	if lj.Request["backend"] != "local" {
		t.Fatalf("explicit local must be preserved: %#v", lj.Request)
	}
	// A subprocess job on the same deployment still takes the cloud stamp.
	pj, _, err := s.SubmitJob(ctx, pa, "sub-default", "subprocess", subReq("echo", "x"), "assistant")
	if err != nil {
		t.Fatal(err)
	}
	if pj.Request["backend"] != "cloud" {
		t.Fatalf("subprocess must take the cloud default: %#v", pj.Request)
	}

	// The scripts runner (kind script, local predicate — the baseline
	// client sends no backend) claims both script jobs and no subprocess.
	claimed, _, err := s.ClaimJobs(ctx, pa, "script-runner", []string{"script"}, 2*time.Minute, 8, "")
	if err != nil {
		t.Fatal(err)
	}
	ids := jobIDs(claimed)
	if len(ids) != 2 || !contains(ids, "sc-default") || !contains(ids, "sc-local") {
		t.Fatalf("script claim pass must take exactly the script jobs: %v", ids)
	}
	// The cloud runner sees only its kind+backend; the script jobs were
	// already claimed, the subprocess goes to jobexec.
	claimed, _, err = s.ClaimJobs(ctx, pa, "jobexec-docker", []string{"subprocess"}, 2*time.Minute, 8, "cloud")
	if err != nil {
		t.Fatal(err)
	}
	if ids = jobIDs(claimed); len(ids) != 1 || ids[0] != "sub-default" {
		t.Fatalf("cloud claim pass must take only the cloud subprocess job: %v", ids)
	}
	// A script claim pass never takes subprocess work, and vice versa:
	// submit both kinds again and claim with crossed kinds.
	if _, _, err := s.SubmitJob(ctx, pa, "sc2", "script", scriptReq, "assistant"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.SubmitJob(ctx, pa, "sub2", "subprocess", subReq("echo", "y"), "assistant"); err != nil {
		t.Fatal(err)
	}
	claimed, _, err = s.ClaimJobs(ctx, pa, "jobexec-docker", []string{"subprocess"}, 2*time.Minute, 8, "cloud")
	if err != nil {
		t.Fatal(err)
	}
	if ids = jobIDs(claimed); len(ids) != 1 || ids[0] != "sub2" {
		t.Fatalf("subprocess claim must not see script work: %v", ids)
	}
	claimed, _, err = s.ClaimJobs(ctx, pa, "script-runner", []string{"script"}, 2*time.Minute, 8, "")
	if err != nil {
		t.Fatal(err)
	}
	if ids = jobIDs(claimed); len(ids) != 1 || ids[0] != "sc2" {
		t.Fatalf("script claim must not see subprocess work: %v", ids)
	}
}
