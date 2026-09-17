package portable

// Return/reclaim: the same individual comes back to a placement that still
// holds its surrendered copy — or to one whose slot was never occupied.
// These tests pin the contract: only a provably transferred frozen copy may
// be replaced, the replace is atomic, transfer lineage and placement-local
// history survive, and no path can leave two writers or resurrect stale
// work.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

// moveOut runs the forward transfer to completion: local seals, exports,
// cloud imports and activates, local completes. supersedes names the
// completed export that surrendered the destination's copy — "" when the
// destination has never held the persona.
func moveOut(t *testing.T, local, cloud placement, pid, transferID, supersedes string, humanID *string) {
	t.Helper()
	ctx := context.Background()
	must(local.svc.Seal(ctx, pid, transferID, placementID(t, cloud)))
	bundle, _ := exportBytes(t, local, pid, transferID)
	var created bool
	var err error
	if supersedes == "" {
		_, created, err = cloud.svc.Import(ctx, bytes.NewReader(bundle), humanID, false)
	} else {
		_, created, err = cloud.svc.ImportReturning(ctx, bytes.NewReader(bundle), humanID, false, supersedes)
	}
	if err != nil || !created {
		t.Fatalf("destination import: created=%v err=%v", created, err)
	}
	act := must(cloud.svc.Activate(ctx, pid, transferID))
	must(local.svc.Complete(ctx, pid, transferID, act.ActivateProof))
}

// sendHome seals the continuation on cloud addressed to local and returns
// the exported bundle without importing it.
func sendHome(t *testing.T, cloud, local placement, pid, transferID string) []byte {
	t.Helper()
	ctx := context.Background()
	must(cloud.svc.Seal(ctx, pid, transferID, placementID(t, local)))
	bundle, _ := exportBytes(t, cloud, pid, transferID)
	return bundle
}

// syntheticBundle is a minimal structurally-valid bundle: a real header
// addressed to the destination plus one persona row and a trailer. It is
// enough to exercise every refusal decided before row streaming; a caller
// that passes the precondition still needs real exported content.
func syntheticBundle(t *testing.T, personaID, destinationID, transferID string) []byte {
	t.Helper()
	hdr, err := json.Marshal(Header{
		Record:        "header",
		Format:        FormatName,
		FormatVersion: FormatVersion,
		TransferID:    transferID,
		PersonaID:     personaID,
		DestinationID: destinationID,
		TransferKey:   strings.Repeat("ab", 32),
		SealedAt:      time.Now().UTC(),
		Sections:      []SectionRef{{Name: CoreSection, Contract: CoreContract}},
		Cut:           Cut{GenerationHighWater: 1},
		Secrets:       SecretsNone,
	})
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	h := sha256.New()
	buf.Write(append(hdr, '\n'))
	h.Write(append(hdr, '\n'))
	row := fmt.Sprintf(`{"record":"row","section":"core","table":"core_personas","data":{"persona_id":%q,"display_name":"s","created_at":"2026-01-01T00:00:00Z","model_intent":null}}`, personaID)
	buf.WriteString(row + "\n")
	h.Write([]byte(row + "\n"))
	trailer, err := json.Marshal(Trailer{Record: "trailer", Rows: map[string]int64{"core_personas": 1}, ContentSHA256: hex.EncodeToString(h.Sum(nil))})
	if err != nil {
		t.Fatal(err)
	}
	buf.Write(append(trailer, '\n'))
	return buf.Bytes()
}

func inputIDs(t *testing.T, p placement, personaID string) []string {
	t.Helper()
	rows, err := p.pool.Query(context.Background(),
		`SELECT input_id FROM core_inputs WHERE persona_id = $1 ORDER BY admission_seq`, personaID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

func countRows(t *testing.T, p placement, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := p.pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// The same secretary goes Local → Cloud → Local and keeps working on the
// original placement: the reclaimed slot carries the Cloud-era
// continuation, the completed forward export survives as lineage, the
// placement-local job record is preserved, and the first writer after
// activation continues work.
func TestReturnReclaimsTransferredCopy(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)

	// A placement-local record that never travels: a finished job row for
	// this persona. Reclaim must not take it away with the frozen copy.
	mustExec(t, local, `INSERT INTO core_jobs
		(persona_id, job_id, kind, request, status, finished_at)
		VALUES ($1, 'job-keep-1', 'subprocess', '{}'::jsonb, 'done', now())`, pid)

	moveOut(t, local, cloud, pid, "out-0001", "", nil)
	if a := authority(t, local, pid); a != "transferred" {
		t.Fatalf("local authority %s", a)
	}
	// Cloud-era continuation: a new admitted input and a completed turn.
	submit(t, cloud, pid, "in-cloud-1", "settled in at the cloud")
	gen := must(cloud.state.AcquireWriter(ctx, pid, "cloud-core", time.Minute)).Generation
	must(cloud.state.Recover(ctx, pid, gen))
	load := must(cloud.state.LoadTurn(ctx, pid, gen, "turn-c1", 50))
	if load.Input == nil {
		t.Fatal("cloud turn claimed nothing")
	}
	must(cloud.state.CommitTurn(ctx, pid, "turn-c1", gen, agentstate.CommitRequest{
		Outcome: "complete",
		Events:  []agentstate.EventInput{{Kind: "assistant_message", Payload: map[string]any{"text": "hi from cloud"}}},
		Output:  map[string]any{"text": "hi from cloud"},
	}))

	// Ordinary Import still refuses the occupied slot — the caller must
	// explicitly choose the return operation.
	bundle := sendHome(t, cloud, local, pid, "home-0001")
	if _, _, err := local.svc.Import(ctx, bytes.NewReader(bundle), nil, false); !errors.Is(err, ErrPersonaExists) {
		t.Fatalf("ordinary import over surrendered copy: %v, want ErrPersonaExists", err)
	}

	staged, created, err := local.svc.ImportReturning(ctx, bytes.NewReader(bundle), nil, false, "out-0001")
	if err != nil || !created {
		t.Fatalf("returning import: created=%v err=%v", created, err)
	}
	if staged.Status != "staged" || staged.Supersedes != "out-0001" {
		t.Fatalf("reclaim receipt %+v", staged)
	}
	if a := authority(t, local, pid); a != "staged" {
		t.Fatalf("reclaimed authority %s", a)
	}
	// The carried rowset is the Cloud continuation: the cloud-era input and
	// turn are present and the pre-move inputs are still there in order.
	ids := inputIDs(t, local, pid)
	for _, want := range []string{"in-1", "in-2", "in-3", "in-cloud-1"} {
		found := false
		for _, id := range ids {
			if id == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("reclaimed inputs %v lack %s", ids, want)
		}
	}
	// Lineage: the completed forward export and the new import both stand.
	exp := must(local.svc.Status(ctx, "export", "out-0001"))
	if exp.Status != "completed" {
		t.Fatalf("forward export ledger %s", exp.Status)
	}
	imp := must(local.svc.Status(ctx, "import", "home-0001"))
	if imp.Supersedes != "out-0001" || imp.Status != "staged" {
		t.Fatalf("import ledger %+v", imp)
	}
	// The placement-local job survived the replace.
	if n := countRows(t, local, `SELECT count(*) FROM core_jobs WHERE persona_id = $1`, pid); n != 1 {
		t.Fatalf("placement-local job rows after reclaim: %d", n)
	}
	// No writer can run while staged.
	if _, err := local.state.AcquireWriter(ctx, pid, "local-core", time.Minute); !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("writer on staged reclaim: %v, want inactive", err)
	}
	act := must(local.svc.Activate(ctx, pid, "home-0001"))
	if a := authority(t, local, pid); a != "active" {
		t.Fatalf("authority after activate %s", a)
	}
	// The source completes on the real proof; the persona is transferred
	// there — never two live placements.
	must(cloud.svc.Complete(ctx, pid, "home-0001", act.ActivateProof))
	if a := authority(t, cloud, pid); a != "transferred" {
		t.Fatalf("cloud authority %s", a)
	}
	// The returned secretary runs here again, above the Cloud epoch.
	gen2 := must(local.state.AcquireWriter(ctx, pid, "local-core", time.Minute)).Generation
	if gen2 <= 1 {
		t.Fatalf("generation after return %d", gen2)
	}
	submit(t, local, pid, "in-back-1", "good to be home")
}

// A destination whose slot was never occupied accepts the return bundle
// exactly like an ordinary import — no lineage is needed for an empty slot.
func TestReturnIntoAbsentSlot(t *testing.T) {
	ctx := context.Background()
	local, fresh := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)

	must(local.svc.Seal(ctx, pid, "out-0002", placementID(t, fresh)))
	bundle, _ := exportBytes(t, local, pid, "out-0002")

	rec, created, err := fresh.svc.ImportReturning(ctx, bytes.NewReader(bundle), nil, false, "")
	if err != nil || !created || rec.Status != "staged" || rec.Supersedes != "" {
		t.Fatalf("absent-slot return: %+v created=%v err=%v", rec, created, err)
	}
	// Replay is the recorded receipt, not a second import.
	rec2, created2, err := fresh.svc.ImportReturning(ctx, bytes.NewReader(bundle), nil, false, "out-0002")
	if err != nil || created2 || rec2.Status != "staged" {
		t.Fatalf("absent-slot return replay: %+v created=%v err=%v", rec2, created2, err)
	}
}

// The reclaim precondition refuses everything that is not the surrendered
// copy of the named completed export: active, sealed or staged authority, a
// hold by a different transfer, or lineage naming another persona's export.
func TestReturnRefusesNonSurrenderedCopy(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)

	// Active: the secretary is live here — never overwritten. The synthetic
	// bundle is addressed to local for the live persona; the precondition
	// refuses before a single row is read.
	must(local.svc.Seal(ctx, pid, "out-live", placementID(t, cloud)))
	outBundle, _ := exportBytes(t, local, pid, "out-live")
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(outBundle), nil, false)))
	act := must(cloud.svc.Activate(ctx, pid, "out-live"))
	must(local.svc.Complete(ctx, pid, "out-live", act.ActivateProof))

	// Bring it home once so the persona is active on local again — and close
	// the leg on cloud so its copy is transferred, not stuck sealed.
	home := sendHome(t, cloud, local, pid, "home-live")
	must(drop(local.svc.ImportReturning(ctx, bytes.NewReader(home), nil, false, "out-live")))
	actHome := must(local.svc.Activate(ctx, pid, "home-live"))
	must(cloud.svc.Complete(ctx, pid, "home-live", actHome.ActivateProof))
	if a := authority(t, local, pid); a != "active" {
		t.Fatalf("authority after return %s", a)
	}
	liveBundle := syntheticBundle(t, pid, placementID(t, local), "fake-home")
	if _, _, err := local.svc.ImportReturning(ctx, bytes.NewReader(liveBundle), nil, false, "out-live"); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("reclaim over active persona: %v, want conflict", err)
	}
	if _, _, err := local.svc.Import(ctx, bytes.NewReader(liveBundle), nil, false); !errors.Is(err, ErrPersonaExists) {
		t.Fatalf("import over active persona: %v, want exists", err)
	}

	// Sealed: a live outbound transfer — the copy is still owed activation
	// evidence and cannot be replaced by an inbound bundle.
	must(local.svc.Seal(ctx, pid, "out-pending", placementID(t, cloud)))
	if _, _, err := local.svc.ImportReturning(ctx, bytes.NewReader(liveBundle), nil, false, "out-pending"); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("reclaim over sealed persona: %v, want conflict", err)
	}

	// Complete that outbound leg so the copy becomes 'transferred'. Cloud
	// holds its own surrendered copy now, so its import is a reclaim under
	// the home-live lineage — the symmetric case.
	bundleOut, _ := exportBytes(t, local, pid, "out-pending")
	must(drop(cloud.svc.ImportReturning(ctx, bytes.NewReader(bundleOut), nil, false, "home-live")))
	act = must(cloud.svc.Activate(ctx, pid, "out-pending"))
	must(local.svc.Complete(ctx, pid, "out-pending", act.ActivateProof))

	// Wrong supersedes: the caller must name the actual surrendering
	// transfer, not a guess or another transfer's id.
	home2 := sendHome(t, cloud, local, pid, "home-0003")
	if _, _, err := local.svc.ImportReturning(ctx, bytes.NewReader(home2), nil, false, "not-the-export"); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("reclaim under wrong supersedes: %v, want conflict", err)
	}
	// A completed export of a *different* persona is not this persona's
	// surrender: the held transfer id does not match the assertion.
	other := newID(t)
	liveSecretary(t, local, other)
	moveOut(t, local, cloud, other, "out-other", "", nil)
	if _, _, err := local.svc.ImportReturning(ctx, bytes.NewReader(home2), nil, false, "out-other"); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("reclaim under another persona's export: %v, want conflict", err)
	}
	// The real reclaim succeeds only with the true lineage.
	if _, created, err := local.svc.ImportReturning(ctx, bytes.NewReader(home2), nil, false, "out-pending"); err != nil || !created {
		t.Fatalf("legitimate reclaim: created=%v err=%v", created, err)
	}
	if a := authority(t, local, pid); a != "staged" {
		t.Fatalf("reclaimed authority %s", a)
	}
	// Staged: while the return is in flight, a different inbound bundle for
	// the same persona is refused — the slot is not surrendered again.
	other2 := syntheticBundle(t, pid, placementID(t, local), "fake-home-2")
	if _, _, err := local.svc.ImportReturning(ctx, bytes.NewReader(other2), nil, false, "out-pending"); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("second inbound over staged reclaim: %v, want conflict", err)
	}
}

// A failed bundle — truncated mid-stream — rolls the reclaim back to the
// untouched frozen copy: authority, rows, and no import ledger row.
func TestReturnRollbackOnBadBundle(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	moveOut(t, local, cloud, pid, "out-0004", "", nil)
	submit(t, cloud, pid, "in-cloud", "cloud era")

	home := sendHome(t, cloud, local, pid, "home-0004")
	// Cut the stream mid-rows: no trailer reaches the importer.
	truncated := home[:len(home)/2]
	if _, _, err := local.svc.ImportReturning(ctx, bytes.NewReader(truncated), nil, false, "out-0004"); !errors.Is(err, ErrBadBundle) {
		t.Fatalf("truncated reclaim: %v, want ErrBadBundle", err)
	}
	if a := authority(t, local, pid); a != "transferred" {
		t.Fatalf("authority after failed reclaim %s, want transferred", a)
	}
	// The frozen copy's rows are intact: its inputs are exactly the pre-move
	// set, and no import ledger row was recorded.
	ids := inputIDs(t, local, pid)
	for _, id := range ids {
		if id == "in-cloud" {
			t.Fatal("failed reclaim left a cloud-era input behind")
		}
	}
	if len(ids) != 3 {
		t.Fatalf("frozen copy inputs %v", ids)
	}
	if _, err := local.svc.Status(ctx, "import", "home-0004"); !errors.Is(err, ErrTransferNotFound) {
		t.Fatalf("ledger after failed reclaim: %v, want not found", err)
	}
	// The intact frozen copy still answers a correct retry.
	if _, created, err := local.svc.ImportReturning(ctx, bytes.NewReader(home), nil, false, "out-0004"); err != nil || !created {
		t.Fatalf("reclaim retry after rollback: created=%v err=%v", created, err)
	}
}

// A retired return deletes the staged copy — the surrendered copy is gone
// with it — and the source's abort restores its authority. The retired
// ledger row then refuses the same bundle forever.
func TestReturnRetireAfterReclaim(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	moveOut(t, local, cloud, pid, "out-0005", "", nil)

	home := sendHome(t, cloud, local, pid, "home-0005")
	if _, created, err := local.svc.ImportReturning(ctx, bytes.NewReader(home), nil, false, "out-0005"); err != nil || !created {
		t.Fatalf("reclaim: created=%v err=%v", created, err)
	}
	retired := must(local.svc.Retire(ctx, pid, "home-0005", placementID(t, local), bundleHeader(t, home).TransferKey))
	if retired.RetireProof == "" {
		t.Fatal("retire produced no proof")
	}
	if _, err := local.state.PersonaState(ctx, pid); !errors.Is(err, agentstate.ErrPersonaNotFound) {
		t.Fatalf("persona after retire: %v", err)
	}
	// The source's abort restores the cloud copy.
	must(cloud.svc.Abort(ctx, pid, "home-0005", retired.RetireProof))
	if a := authority(t, cloud, pid); a != "active" {
		t.Fatalf("cloud authority after abort %s", a)
	}
	// The retired transfer is dead: the same bundle can never stage again.
	if _, _, err := local.svc.ImportReturning(ctx, bytes.NewReader(home), nil, false, "out-0005"); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("re-import of a retired return: %v, want conflict", err)
	}
	// The completed forward export still stands as lineage.
	if exp := must(local.svc.Status(ctx, "export", "out-0005")); exp.Status != "completed" {
		t.Fatalf("forward export after retire: %s", exp.Status)
	}
}

// Concurrent reclaims of the same bundle converge on one committed staging
// and one stable receipt; a different bundle racing the same slot is
// refused once the winner staged it.
func TestReturnConcurrency(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	moveOut(t, local, cloud, pid, "out-0006", "", nil)

	home := sendHome(t, cloud, local, pid, "home-0006")

	const n = 8
	recs := make([]Receipt, n)
	created := make([]bool, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			recs[i], created[i], errs[i] = local.svc.ImportReturning(
				ctx, bytes.NewReader(home), nil, false, "out-0006")
		}(i)
	}
	wg.Wait()
	won := 0
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent reclaim %d: %v", i, err)
		}
		if created[i] {
			won++
		}
		if recs[i].Status != "staged" || recs[i].ContentSHA256 != recs[0].ContentSHA256 ||
			recs[i].Supersedes != "out-0006" {
			t.Fatalf("concurrent reclaim %d diverged: %+v", i, recs[i])
		}
	}
	if won != 1 {
		t.Fatalf("created count %d, want exactly 1", won)
	}
	if a := authority(t, local, pid); a != "staged" {
		t.Fatalf("authority after race %s", a)
	}
	// A different transfer id for the same persona cannot barge in once the
	// slot is staged.
	other := syntheticBundle(t, pid, placementID(t, local), "fake-race")
	if _, _, err := local.svc.ImportReturning(ctx, bytes.NewReader(other), nil, false, "out-0006"); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("different bundle over staged slot: %v, want conflict", err)
	}
}

// Repeat cycles: the secretary can leave and return more than once — each
// return reclaims under its own supersedes lineage and the ledger keeps the
// whole chain.
func TestReturnRepeatCycles(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)

	prev := ""
	for cycle := 1; cycle <= 2; cycle++ {
		out := fmt.Sprintf("cycle%d-out", cycle)
		home := fmt.Sprintf("cycle%d-home", cycle)
		// The destination's surrendered copy was made by the previous leg's
		// export home — that completed export is the reclaim's lineage.
		moveOut(t, local, cloud, pid, out, prev, nil)
		submit(t, cloud, pid, fmt.Sprintf("cloud-input-%d", cycle), "cloud era")
		bundle := sendHome(t, cloud, local, pid, home)
		if _, created, err := local.svc.ImportReturning(ctx, bytes.NewReader(bundle), nil, false, out); err != nil || !created {
			t.Fatalf("cycle %d reclaim: created=%v err=%v", cycle, created, err)
		}
		act := must(local.svc.Activate(ctx, pid, home))
		must(cloud.svc.Complete(ctx, pid, home, act.ActivateProof))
		if a := authority(t, local, pid); a != "active" {
			t.Fatalf("cycle %d local authority %s", cycle, a)
		}
		submit(t, local, pid, fmt.Sprintf("local-input-%d", cycle), "back home")
		prev = home
	}
	// Every leg of the chain is still recorded.
	for _, id := range []string{"cycle1-out", "cycle2-out"} {
		if exp := must(local.svc.Status(ctx, "export", id)); exp.Status != "completed" {
			t.Fatalf("export %s: %s", id, exp.Status)
		}
	}
	for _, id := range []string{"cycle1-home", "cycle2-home"} {
		if imp := must(local.svc.Status(ctx, "import", id)); imp.Status != "activated" {
			t.Fatalf("import %s: %s", id, imp.Status)
		}
	}
	// All inputs from every era are present.
	ids := inputIDs(t, local, pid)
	for _, want := range []string{"in-1", "in-2", "in-3", "cloud-input-1", "local-input-1", "cloud-input-2", "local-input-2"} {
		found := false
		for _, id := range ids {
			if id == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("after two cycles inputs %v lack %s", ids, want)
		}
	}
}

// Reclaim carries the persona's model intent unchanged — the Cloud-era
// snapshotted selection survives as non-secret intent on the returned copy,
// still owed to the destination's own rebinding.
func TestReturnPreservesModelIntent(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	human := newID(t)
	mustExec(t, local, `INSERT INTO humans (human_id) VALUES ($1)`, human)
	must(drop(local.state.EnsurePersona(ctx, pid, &human, "secretary")))
	submit(t, local, pid, "in-1", "hello")
	mustExec(t, local, `INSERT INTO model_connection_selections (human_id, kind) VALUES ($1, 'none')`, human)

	moveOut(t, local, cloud, pid, "out-0007", "", nil)
	bundle := sendHome(t, cloud, local, pid, "home-0007")
	if _, created, err := local.svc.ImportReturning(ctx, bytes.NewReader(bundle), nil, false, "out-0007"); err != nil || !created {
		t.Fatalf("reclaim: created=%v err=%v", created, err)
	}
	var intent []byte
	if err := local.pool.QueryRow(ctx,
		`SELECT model_intent FROM core_personas WHERE persona_id = $1`, pid).Scan(&intent); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(intent, []byte(`"kind"`)) || !bytes.Contains(intent, []byte(`"none"`)) {
		t.Fatalf("reclaimed model_intent %s, want the carried none intent", intent)
	}
}
