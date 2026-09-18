package portable

// Repairs for the return-lifecycle review findings F353–F355:
//
//   F353 — retiring a staged return must not cascade away the placement's
//          own history (jobs, usage facts/reservations, call state). The
//          incoming carried state dies with the cancelled return; the
//          persona row returns to the surrendered state the completed
//          export still owns — never reactivated here while the source
//          resumes.
//   F354 — Seal/Complete replays of a finished export resolve to the
//          recorded ledger truth even after a reclaim rebinds the
//          persona's transfer_id hold.
//   F355 — the supersedes lineage assertion replays like the other
//          admission parameters: a recorded reclaim answers a different
//          non-empty assertion with a conflict; an absent-slot staging
//          recorded no lineage and accepts any assertion.

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

// localHistory seeds placement-local records that never travel: a finished
// job, a priced usage fact and a settled reservation — the install's own
// operational and billing history for this persona.
func localHistory(t *testing.T, p placement, personaID string) {
	t.Helper()
	mustExec(t, p, `INSERT INTO core_jobs
		(persona_id, job_id, kind, request, status, finished_at)
		VALUES ($1, 'job-hist-1', 'subprocess', '{}'::jsonb, 'done', now())`, personaID)
	mustExec(t, p, `INSERT INTO usage_facts
		(persona_id, fact_id, kind, phase, funding_kind, funding_id, funding, status,
		 input_tokens, output_tokens, cost_minor, currency, cost_basis)
		VALUES ($1, 'fact-hist-1', 'model_call', 'turn', 'sumi', 'acct-1', '{"provider":"x"}'::jsonb,
		 'reported', 100, 50, 12, 'usd', 'configured_rates')`, personaID)
	mustExec(t, p, `INSERT INTO usage_reservations
		(persona_id, fact_id, kind, phase, funding_kind, funding_id, funding,
		 reserved_minor, generation, status, settled_at)
		VALUES ($1, 'fact-hist-1', 'model_call', 'turn', 'sumi', 'acct-1', '{"provider":"x"}'::jsonb,
		 20, 1, 'settled', now())`, personaID)
}

func assertLocalHistory(t *testing.T, p placement, personaID string, want int64) {
	t.Helper()
	if n := countRows(t, p, `SELECT count(*) FROM core_jobs WHERE persona_id = $1`, personaID); n != want {
		t.Fatalf("core_jobs rows: %d, want %d", n, want)
	}
	if n := countRows(t, p, `SELECT count(*) FROM usage_facts WHERE persona_id = $1`, personaID); n != want {
		t.Fatalf("usage_facts rows: %d, want %d", n, want)
	}
	if n := countRows(t, p, `SELECT count(*) FROM usage_reservations WHERE persona_id = $1`, personaID); n != want {
		t.Fatalf("usage_reservations rows: %d, want %d", n, want)
	}
}

// F353: cancelling a staged return preserves the placement's own history.
// The persona row survives as the surrendered copy of the still-completed
// forward export — 'transferred', held by that export, carried rows gone —
// while jobs, usage facts and reservations stay attached. The source's
// abort restores its authority (one live copy only), the retired transfer
// is closed forever, and a fresh return under the same lineage reclaims
// the slot cleanly.
func TestRetireReclaimPreservesPlacementLocalHistory(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	localHistory(t, local, pid)

	moveOut(t, local, cloud, pid, "out-1001", "", nil)
	submit(t, cloud, pid, "cloud-input", "cloud era")
	home := sendHome(t, cloud, local, pid, "home-1001")
	if _, created, err := local.svc.ImportReturning(ctx, bytes.NewReader(home), nil, false, "out-1001"); err != nil || !created {
		t.Fatalf("reclaim: created=%v err=%v", created, err)
	}
	assertLocalHistory(t, local, pid, 1)

	// Cancel the staged return.
	retired := must(local.svc.Retire(ctx, pid, "home-1001", placementID(t, local), bundleHeader(t, home).TransferKey))
	if retired.RetireProof == "" || retired.Status != "retired" {
		t.Fatalf("retire receipt %+v", retired)
	}

	// The placement's own history survives: the persona row is restored to
	// the surrendered state the completed export owns — not deleted.
	assertLocalHistory(t, local, pid, 1)
	if a := authority(t, local, pid); a != "transferred" {
		t.Fatalf("authority after retired return %s, want transferred", a)
	}
	var held *string
	if err := local.pool.QueryRow(ctx,
		`SELECT transfer_id FROM core_personas WHERE persona_id = $1`, pid).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if held == nil || *held != "out-1001" {
		t.Fatalf("restored hold %v, want out-1001", held)
	}
	// The incoming carried state died with the cancelled return.
	if n := countRows(t, local, `SELECT count(*) FROM core_inputs WHERE persona_id = $1`, pid); n != 0 {
		t.Fatalf("carried inputs after retired return: %d", n)
	}
	if n := countRows(t, local, `SELECT count(*) FROM core_writer_leases WHERE persona_id = $1`, pid); n != 0 {
		t.Fatalf("staged lease left after retired return: %d", n)
	}
	// Nothing reactivates here: the surrendered shell admits no input and
	// seals nothing while the source resumes.
	if _, _, err := local.state.SubmitInput(ctx, &agentstate.Input{
		PersonaID: pid, InputID: "late", Kind: "message",
		Payload: map[string]any{"text": "x"}, ActorKind: "human", ActorID: "owner", SourceSurface: "test",
	}); !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("input on surrendered shell: %v, want inactive", err)
	}
	if _, err := local.svc.Seal(ctx, pid, "escape-01", placementID(t, cloud)); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("seal of surrendered shell: %v, want conflict", err)
	}

	// The source's abort restores its authority — exactly one live copy.
	must(cloud.svc.Abort(ctx, pid, "home-1001", retired.RetireProof))
	if a := authority(t, cloud, pid); a != "active" {
		t.Fatalf("cloud authority after abort %s", a)
	}
	// Retire replay returns the same receipt.
	if again := must(local.svc.Retire(ctx, pid, "home-1001", placementID(t, local), "")); again.RetireProof != retired.RetireProof {
		t.Fatalf("retire replay produced %+v vs %+v", again, retired)
	}
	// The retired transfer can never stage again.
	if _, _, err := local.svc.ImportReturning(ctx, bytes.NewReader(home), nil, false, "out-1001"); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("re-import of retired return: %v, want conflict", err)
	}
	// The completed forward export still stands as lineage.
	if exp := must(local.svc.Status(ctx, "export", "out-1001")); exp.Status != "completed" {
		t.Fatalf("forward export after retired return: %s", exp.Status)
	}

	// A fresh return under the same lineage reclaims the slot cleanly.
	home2 := sendHome(t, cloud, local, pid, "home-1002")
	if _, created, err := local.svc.ImportReturning(ctx, bytes.NewReader(home2), nil, false, "out-1001"); err != nil || !created {
		t.Fatalf("second reclaim: created=%v err=%v", created, err)
	}
	assertLocalHistory(t, local, pid, 1)
	act := must(local.svc.Activate(ctx, pid, "home-1002"))
	must(cloud.svc.Complete(ctx, pid, "home-1002", act.ActivateProof))
	if a := authority(t, local, pid); a != "active" {
		t.Fatalf("authority after second return %s", a)
	}
	submit(t, local, pid, "back-1", "home again")
}

// F354: after a reclaim rebinds the persona's hold to the inbound
// transfer, replays of the finished export resolve to the recorded ledger
// truth — Seal returns the recorded receipt and Complete returns the
// recorded completed receipt — instead of a false "not held" conflict. A
// different presented proof is a mismatch, never a silent accept.
func TestExportReplayAfterRebind(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	moveOut(t, local, cloud, pid, "out-2001", "", nil)

	home := sendHome(t, cloud, local, pid, "home-2001")
	if _, created, err := local.svc.ImportReturning(ctx, bytes.NewReader(home), nil, false, "out-2001"); err != nil || !created {
		t.Fatalf("reclaim: created=%v err=%v", created, err)
	}
	// The persona's hold is now the inbound transfer; the completed
	// export's terminal truth still answers its retries.
	completed := must(local.svc.Status(ctx, "export", "out-2001"))
	if completed.Status != "completed" {
		t.Fatalf("export ledger %s", completed.Status)
	}
	sealReplay, err := local.svc.Seal(ctx, pid, "out-2001", placementID(t, cloud))
	if err != nil || sealReplay.Status != "completed" {
		t.Fatalf("seal replay after rebind: %+v err=%v", sealReplay, err)
	}
	completeReplay, err := local.svc.Complete(ctx, pid, "out-2001", completed.ActivateProof)
	if err != nil || completeReplay.Status != "completed" || completeReplay.ActivateProof != completed.ActivateProof {
		t.Fatalf("complete replay after rebind: %+v err=%v", completeReplay, err)
	}
	// A different presented proof is a mismatch, not a retry.
	if _, err := local.svc.Complete(ctx, pid, "out-2001", completed.RetireProof+"00"); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("complete replay with wrong proof: %v, want conflict", err)
	}
	// The newer hold is untouched: the staged return still stands.
	if a := authority(t, local, pid); a != "staged" {
		t.Fatalf("authority after export replays %s, want staged", a)
	}
	// Seal replay naming a different destination is still a conflict.
	if _, err := local.svc.Seal(ctx, pid, "out-2001", newID(t)); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("seal replay with wrong destination: %v, want conflict", err)
	}
}

// F354 companion case: an aborted export is dead — its transfer id can
// never be reused or completed, so Seal and Complete answer a conflict,
// while the ledger still reports the recorded 'aborted' truth.
func TestExportReplayAfterAbort(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)

	must(local.svc.Seal(ctx, pid, "out-3001", placementID(t, cloud)))
	bundle, _ := exportBytes(t, local, pid, "out-3001")
	// Tombstone the destination side, abort the export.
	retired := must(cloud.svc.Retire(ctx, pid, "out-3001", placementID(t, cloud), bundleHeader(t, bundle).TransferKey))
	must(local.svc.Abort(ctx, pid, "out-3001", retired.RetireProof))
	if a := authority(t, local, pid); a != "active" {
		t.Fatalf("authority after abort %s", a)
	}
	// The aborted id stays closed: both entry points refuse, and Status
	// reports the recorded terminal truth.
	if _, err := local.svc.Seal(ctx, pid, "out-3001", placementID(t, cloud)); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("seal replay of aborted export: %v, want conflict", err)
	}
	if _, err := local.svc.Complete(ctx, pid, "out-3001", ""); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("complete of aborted export: %v, want conflict", err)
	}
	if rec := must(local.svc.Status(ctx, "export", "out-3001")); rec.Status != "aborted" {
		t.Fatalf("aborted export ledger: %s", rec.Status)
	}
}

// F355: the lineage assertion replays like the other admission
// parameters. A staged reclaim answers a different non-empty supersedes
// with a conflict; the same supersedes, or an ordinary Import retry that
// asserts nothing, returns the recorded receipt. An absent-slot staging
// recorded no lineage, so any assertion on its replay is consistent.
func TestReturnReplaySupersedesContract(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	moveOut(t, local, cloud, pid, "out-4001", "", nil)

	home := sendHome(t, cloud, local, pid, "home-4001")
	staged, created, err := local.svc.ImportReturning(ctx, bytes.NewReader(home), nil, false, "out-4001")
	if err != nil || !created || staged.Supersedes != "out-4001" {
		t.Fatalf("reclaim: %+v created=%v err=%v", staged, created, err)
	}
	// Same lineage: the recorded receipt, not a second import.
	again, created, err := local.svc.ImportReturning(ctx, bytes.NewReader(home), nil, false, "out-4001")
	if err != nil || created || again.Supersedes != "out-4001" {
		t.Fatalf("reclaim replay: %+v created=%v err=%v", again, created, err)
	}
	// A different lineage assertion conflicts — the caller learns the
	// transfer was staged under a different surrender.
	if _, _, err := local.svc.ImportReturning(ctx, bytes.NewReader(home), nil, false, "out-9999"); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("reclaim replay with wrong supersedes: %v, want conflict", err)
	}
	// An ordinary Import retry asserts nothing and gets the truthful
	// recorded receipt.
	plain, created, err := local.svc.Import(ctx, bytes.NewReader(home), nil, false)
	if err != nil || created || plain.Supersedes != "out-4001" {
		t.Fatalf("ordinary import replay of reclaim: %+v created=%v err=%v", plain, created, err)
	}

	// Absent-slot staging recorded no lineage: the assertion was vacuous
	// and remains so on replay.
	fresh := newPlacement(t)
	pid2 := newID(t)
	liveSecretary(t, local, pid2)
	must(local.svc.Seal(ctx, pid2, "out-4002", placementID(t, fresh)))
	bundle2, _ := exportBytes(t, local, pid2, "out-4002")
	rec, created, err := fresh.svc.ImportReturning(ctx, bytes.NewReader(bundle2), nil, false, "out-4001")
	if err != nil || !created || rec.Supersedes != "" {
		t.Fatalf("absent-slot ImportReturning: %+v created=%v err=%v", rec, created, err)
	}
	// Replay with a different assertion is consistent — nothing was
	// reclaimed, so there is no recorded lineage to contradict.
	if _, created, err := fresh.svc.ImportReturning(ctx, bytes.NewReader(bundle2), nil, false, "anything"); err != nil || created {
		t.Fatalf("absent-slot replay with different supersedes: created=%v err=%v", created, err)
	}
}

// The low-level binding contract (reviewer B F5 clarification): a reclaim
// binds the destination's human only when the caller passes one — a nil
// human_id leaves the persona unbound, exactly like an ordinary import.
// Nothing infers or preserves the surrendered copy's old binding; the
// later user-facing receiver must pass the authenticated destination
// human.
func TestReturnReclaimHumanBindingContract(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	human := newID(t)
	mustExec(t, local, `INSERT INTO humans (human_id) VALUES ($1)`, human)
	must(drop(local.state.EnsurePersona(ctx, pid, &human, "secretary")))
	submit(t, local, pid, "in-1", "hello")

	moveOut(t, local, cloud, pid, "out-5001", "", nil)
	home := sendHome(t, cloud, local, pid, "home-5001")

	// nil: explicitly unbound — the destination binds nothing.
	if _, created, err := local.svc.ImportReturning(ctx, bytes.NewReader(home), nil, false, "out-5001"); err != nil || !created {
		t.Fatalf("reclaim: created=%v err=%v", created, err)
	}
	var bound *string
	if err := local.pool.QueryRow(ctx,
		`SELECT human_id FROM core_personas WHERE persona_id = $1`, pid).Scan(&bound); err != nil {
		t.Fatal(err)
	}
	if bound != nil {
		t.Fatalf("nil human_id reclaim bound the persona to %v", bound)
	}

	// Close the staged leg (retire + source abort), then stage a second
	// return with an explicit human — it binds exactly as passed.
	retired := must(local.svc.Retire(ctx, pid, "home-5001", placementID(t, local), bundleHeader(t, home).TransferKey))
	must(cloud.svc.Abort(ctx, pid, "home-5001", retired.RetireProof))
	human2 := newID(t)
	mustExec(t, local, `INSERT INTO humans (human_id) VALUES ($1)`, human2)
	home2 := sendHome(t, cloud, local, pid, "home-5002")
	if _, created, err := local.svc.ImportReturning(ctx, bytes.NewReader(home2), &human2, false, "out-5001"); err != nil || !created {
		t.Fatalf("bound reclaim: created=%v err=%v", created, err)
	}
	if err := local.pool.QueryRow(ctx,
		`SELECT human_id FROM core_personas WHERE persona_id = $1`, pid).Scan(&bound); err != nil {
		t.Fatal(err)
	}
	if bound == nil || *bound != human2 {
		t.Fatalf("explicit human_id reclaim bound %v, want %s", bound, human2)
	}
}
