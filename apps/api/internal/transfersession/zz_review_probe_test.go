package transfersession_test

// Reviewer probes (independent review transfer-session-independent-a).
// Instrumentation only: these tests exercise contract boundaries the
// author's suite does not cover directly. No product code is changed.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sumi-studio/sumi/apps/api/internal/portable"
	"github.com/sumi-studio/sumi/apps/api/internal/transfersession"
)

// F233 / A-F1 (repaired). An unlinked (active=false) credential still names
// an existing account: credentials is UNIQUE (provider, external_subject) and
// 0002's trigger refuses rebinding, so koseki re-activates the row only for
// the human that already owns it. Create must refuse it, and the registrant's
// secretary must never be sealed for a move that could not complete.
//
// The original probe recorded the defect: Create returned 201, the secretary
// sealed and staged, and only the cancel → retire → abort path brought it
// back. That original is preserved in the report directory.
func TestProbeInactiveCredentialAdmission(t *testing.T) {
	h := setup(t, transfersession.Config{})
	uid := "review-inactive-" + h.pid[24:]
	oldHuman := newID(t)
	if _, err := h.cloud.pool.Exec(h.ctx, `INSERT INTO humans (human_id) VALUES ($1)`, oldHuman); err != nil {
		t.Fatal(err)
	}
	if _, err := h.cloud.pool.Exec(h.ctx, `INSERT INTO credentials
		(provider, external_subject, human_id, active, unlinked_at)
		VALUES ('firebase', $1, $2, false, now())`, uid, oldHuman); err != nil {
		t.Fatal(err)
	}

	code, raw := h.request("POST", "/api/secretary-transfer/sessions", "", jsonBody(h.flow(uid, time.Minute)))
	if code != http.StatusConflict || !strings.Contains(string(raw), "already has an account") {
		t.Fatalf("create for unlinked credential: %d %s", code, raw)
	}
	var rows int
	if err := h.cloud.pool.QueryRow(h.ctx,
		`SELECT count(*) FROM transfer_sessions WHERE claim_subject = $1`, uid).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("a session was recorded for a credential that already has an account: %d", rows)
	}
	// Nothing left Local: no seal, no export ledger row, still answering.
	var exports int
	if err := h.local.pool.QueryRow(h.ctx,
		`SELECT count(*) FROM core_transfers WHERE direction = 'export' AND persona_id = $1`, h.pid).Scan(&exports); err != nil {
		t.Fatal(err)
	}
	if exports != 0 {
		t.Fatalf("the secretary was sealed for a session that should never have opened: %d", exports)
	}
	if got := authority(t, h.local, h.pid); got != "active" {
		t.Fatalf("local authority: %s", got)
	}

	// A linked credential is refused the same way; a credential with no row
	// at all still opens a session, so the refusal is ownership, not caution.
	if _, err := h.cloud.pool.Exec(h.ctx,
		`UPDATE credentials SET active = true, unlinked_at = NULL WHERE external_subject = $1`, uid); err != nil {
		t.Fatal(err)
	}
	if code, raw := h.request("POST", "/api/secretary-transfer/sessions", "", jsonBody(h.flow(uid, time.Minute))); code != http.StatusConflict {
		t.Fatalf("create for linked credential: %d %s", code, raw)
	}
	h.create("review-fresh-" + h.pid[24:])
}

// F233 race. A credential that acquires an account after Create is caught at
// the last moment before any Local authority moves: taking the move URL
// re-checks ownership, so the secretary is never sealed for a session whose
// account transaction could not bind the credential.
func TestProbeAccountAppearsBetweenCreateAndSeal(t *testing.T) {
	h := setup(t, transfersession.Config{})
	uid := "review-raceacct-" + h.pid[24:]
	sid, grant := h.create(uid)

	human := newID(t)
	if _, err := h.cloud.pool.Exec(h.ctx, `INSERT INTO humans (human_id) VALUES ($1)`, human); err != nil {
		t.Fatal(err)
	}
	if _, err := h.cloud.pool.Exec(h.ctx, `INSERT INTO credentials
		(provider, external_subject, human_id) VALUES ('firebase', $1, $2)`, uid, human); err != nil {
		t.Fatal(err)
	}

	code, v := h.bind(sid, grant)
	if code != http.StatusConflict {
		t.Fatalf("bind after the credential acquired an account: %d %+v", code, v)
	}
	if got := authority(t, h.local, h.pid); got != "active" {
		t.Fatalf("local authority after the refused bind: %s", got)
	}
	if got := h.dbStatus(sid); got != transfersession.StatusAwaitingBundle {
		t.Fatalf("session: %s", got)
	}
	// The registrant can still close the session; nothing was sealed, so
	// there is no proof to collect and no secretary to bring back.
	code, raw := h.request("POST", "/api/secretary-transfer/sessions/"+sid+"/cancel", "",
		jsonBody(withFlow(h.flow(uid, time.Minute), map[string]string{"expect_status": "awaiting_bundle"})))
	if code != http.StatusOK {
		t.Fatalf("subject cancel: %d %s", code, raw)
	}
}

// Duplicate Create under concurrency must issue exactly one grant.
func TestProbeConcurrentCreate(t *testing.T) {
	h := setup(t, transfersession.Config{})
	uid := "review-race-" + h.pid[24:]
	flow := h.flow(uid, time.Minute)

	const n = 8
	codes := make([]int, n)
	bodies := make([][]byte, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i], bodies[i] = h.request("POST", "/api/secretary-transfer/sessions", "", jsonBody(flow))
		}(i)
	}
	wg.Wait()
	created, conflicts := 0, 0
	openID := ""
	for i := 0; i < n; i++ {
		switch codes[i] {
		case http.StatusCreated:
			created++
			openID = asView(t, bodies[i]).SessionID
		case http.StatusConflict:
			conflicts++
			var env struct {
				SessionID string `json:"session_id"`
			}
			decodeRaw(t, bodies[i], &env)
			if openID != "" && env.SessionID != openID {
				t.Fatalf("conflicting create named a different session %s vs %s", env.SessionID, openID)
			}
			openID = env.SessionID
		default:
			t.Fatalf("create %d: %d %s", i, codes[i], bodies[i])
		}
	}
	if created != 1 || conflicts != n-1 {
		t.Fatalf("created=%d conflicts=%d", created, conflicts)
	}
	var rows int
	if err := h.cloud.pool.QueryRow(h.ctx,
		`SELECT count(*) FROM transfer_sessions WHERE claim_subject = $1`, uid).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("sessions for subject: %d", rows)
	}
}

// F234 / A-F2 (repaired). The same move URL pasted into two Sumi Local
// installs. Taking the session is what decides, and it happens before either
// secretary is sealed: the second placement is refused while its secretary is
// still active and answering, and the destination refuses a bundle from any
// placement but the one that took the URL.
//
// The original probe recorded the defect: both placements sealed, the first
// upload won, and the loser stayed sealed for good with no proof that could
// ever end its seal. That original is preserved in the report directory.
func TestProbeSecondPlacementSubstitution(t *testing.T) {
	h := setup(t, transfersession.Config{})
	other := newPlacement(t, true)
	otherPID := newID(t)
	if _, _, err := other.state.EnsurePersona(h.ctx, otherPID, nil, "other secretary"); err != nil {
		t.Fatal(err)
	}
	otherPlacement, err := other.svc.PlacementID(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	uid := "review-subst-" + h.pid[24:]
	sid, grant := h.create(uid)
	dest := mustPlacement(t, h)

	// A takes the URL first.
	if code, v := h.bind(sid, grant); code != http.StatusOK || v.Source == nil || v.Source.PersonaID != h.pid {
		t.Fatalf("A bind: %d %+v", code, v)
	}
	// B pastes the same URL. It is refused before it seals anything, and the
	// answer names the placement that holds the session.
	code, vb := h.bindAs(sid, grant, transfersession.Source{PlacementID: otherPlacement, PersonaID: otherPID})
	if code != http.StatusConflict {
		t.Fatalf("B bind: %d %+v", code, vb)
	}
	if vb.Source == nil || vb.Source.PersonaID != h.pid || vb.Source.PlacementID == otherPlacement {
		t.Fatalf("B was not told which placement holds the session: %+v", vb.Source)
	}
	if got := authority(t, other, otherPID); got != "active" {
		t.Fatalf("B was affected by the refused bind: %s", got)
	}
	var otherExports int
	if err := other.pool.QueryRow(h.ctx,
		`SELECT count(*) FROM core_transfers WHERE direction = 'export' AND persona_id = $1`, otherPID).Scan(&otherExports); err != nil {
		t.Fatal(err)
	}
	if otherExports != 0 {
		t.Fatalf("B sealed for a URL it does not hold: %d export rows", otherExports)
	}
	// A's bind stays idempotent under retries and restarts.
	for i := 0; i < 3; i++ {
		if code, _ := h.bind(sid, grant); code != http.StatusOK {
			t.Fatalf("A re-bind %d: %d", i, code)
		}
	}

	// A seals, uploads and completes exactly once.
	if _, err := h.local.svc.Seal(h.ctx, h.pid, sid, dest); err != nil {
		t.Fatal(err)
	}
	var bufA bytes.Buffer
	if _, err := h.local.svc.Export(h.ctx, h.pid, sid, &bufA); err != nil {
		t.Fatal(err)
	}
	if code, v := h.upload(sid, grant, bytes.NewReader(bufA.Bytes())); code != http.StatusCreated || v.Status != transfersession.StatusStaged {
		t.Fatalf("A upload: %d %s", code, v.Status)
	}

	// Out-of-protocol: B seals anyway (no shipped client does this — the
	// command takes the URL before it seals). The destination still refuses
	// its bundle before reading a row, and says why.
	if _, err := other.svc.Seal(h.ctx, otherPID, sid, dest); err != nil {
		t.Fatal(err)
	}
	var bufB bytes.Buffer
	if _, err := other.svc.Export(h.ctx, otherPID, sid, &bufB); err != nil {
		t.Fatal(err)
	}
	code, raw := h.request("PUT", "/api/secretary-transfer/sessions/"+sid+"/bundle", grant, bytes.NewReader(bufB.Bytes()))
	if code != http.StatusConflict || !strings.Contains(string(raw), "accepts secretary "+h.pid) {
		t.Fatalf("B upload: %d %s", code, raw)
	}
	// B's mistaken cancel cannot foreclose the transfer A is moving.
	var hdrB portable.Header
	if err := json.Unmarshal(bufB.Bytes()[:bytes.IndexByte(bufB.Bytes(), '\n')], &hdrB); err != nil {
		t.Fatal(err)
	}
	code, raw = h.request("POST", "/api/secretary-transfer/sessions/"+sid+"/cancel", grant,
		jsonBody(map[string]string{"persona_id": otherPID, "transfer_key": hdrB.TransferKey}))
	if code != http.StatusConflict {
		t.Fatalf("B cancel: %d %s", code, raw)
	}
	if got := h.dbStatus(sid); got != transfersession.StatusStaged {
		t.Fatalf("session after B's cancel: %s", got)
	}

	// The account commits for A's persona; A is active on Cloud exactly once
	// and transferred on Local.
	if _, _, err := h.provision(uid, sid, nil); err != nil {
		t.Fatal(err)
	}
	gv, err := h.sessions.Status(h.ctx, sid, grant)
	if err != nil || gv.Status != transfersession.StatusActivated || gv.ActivateProof == "" {
		t.Fatalf("activated view: %+v %v", gv, err)
	}
	if _, err := h.local.svc.Complete(h.ctx, h.pid, sid, gv.ActivateProof); err != nil {
		t.Fatalf("A complete: %v", err)
	}
	if l, c := authority(t, h.local, h.pid), authority(t, h.cloud, h.pid); l != "transferred" || c != "active" {
		t.Fatalf("A after completion: local %s, cloud %s", l, c)
	}
}

// Claim/provision boundaries the author's tests touch only indirectly.
func TestProbeClaimBoundaries(t *testing.T) {
	h := setup(t, transfersession.Config{})
	uid := "review-claim-" + h.pid[24:]
	sid, grant := h.create(uid)
	subj := transfersession.Subject{Provider: "firebase", Subject: uid}

	t.Run("awaiting session cannot be claimed", func(t *testing.T) {
		tx, err := h.cloud.pool.Begin(h.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(h.ctx)
		_, err = transfersession.ClaimInTx(h.ctx, tx, sid, subj)
		if !errors.Is(err, transfersession.ErrConflict) {
			t.Fatalf("claim on awaiting: %v", err)
		}
	})

	if code, v := h.upload(sid, grant, bytes.NewReader(h.seal(sid, grant))); code != http.StatusCreated {
		t.Fatalf("upload: %d %v", code, v)
	}

	t.Run("wrong subject gets ErrNotFound", func(t *testing.T) {
		tx, _ := h.cloud.pool.Begin(h.ctx)
		defer tx.Rollback(h.ctx)
		_, err := transfersession.ClaimInTx(h.ctx, tx, sid,
			transfersession.Subject{Provider: "firebase", Subject: "someone-else"})
		if !errors.Is(err, transfersession.ErrNotFound) {
			t.Fatalf("wrong-subject claim: %v", err)
		}
	})

	t.Run("provision without bound credential fails and rolls back", func(t *testing.T) {
		var humansBefore, humansAfter int
		_ = h.cloud.pool.QueryRow(h.ctx, `SELECT count(*) FROM humans`).Scan(&humansBefore)
		tx, _ := h.cloud.pool.Begin(h.ctx)
		claim, err := transfersession.ClaimInTx(h.ctx, tx, sid, subj)
		if err != nil {
			t.Fatal(err)
		}
		humanID := newID(t)
		if _, err := tx.Exec(h.ctx, `INSERT INTO humans (human_id) VALUES ($1)`, humanID); err != nil {
			t.Fatal(err)
		}
		// No credential row: the account tx "forgot" to bind it.
		err = transfersession.ProvisionInTx(h.ctx, tx, claim, humanID)
		if !errors.Is(err, transfersession.ErrBadRequest) {
			t.Fatalf("provision without bound credential: %v", err)
		}
		tx.Rollback(h.ctx)
		_ = h.cloud.pool.QueryRow(h.ctx, `SELECT count(*) FROM humans`).Scan(&humansAfter)
		if humansAfter != humansBefore {
			t.Fatal("a failed provision left a minted human")
		}
		if got := h.dbStatus(sid); got != transfersession.StatusStaged {
			t.Fatalf("session after failed provision: %s", got)
		}
	})

	t.Run("credential bound to another human cannot provision", func(t *testing.T) {
		tx, _ := h.cloud.pool.Begin(h.ctx)
		claim, err := transfersession.ClaimInTx(h.ctx, tx, sid, subj)
		if err != nil {
			t.Fatal(err)
		}
		humanA, humanB := newID(t), newID(t)
		for _, hid := range []string{humanA, humanB} {
			if _, err := tx.Exec(h.ctx, `INSERT INTO humans (human_id) VALUES ($1)`, hid); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := tx.Exec(h.ctx, `INSERT INTO credentials (provider, external_subject, human_id)
			VALUES ('firebase', $1, $2)`, uid, humanA); err != nil {
			t.Fatal(err)
		}
		err = transfersession.ProvisionInTx(h.ctx, tx, claim, humanB)
		if !errors.Is(err, transfersession.ErrBadRequest) {
			t.Fatalf("provision binding another human's credential: %v", err)
		}
		tx.Rollback(h.ctx)
	})

	t.Run("claim deadline expiry is enforced", func(t *testing.T) {
		if _, err := h.cloud.pool.Exec(h.ctx,
			`UPDATE transfer_sessions SET claim_until = now() - interval '1 second' WHERE session_id = $1`, sid); err != nil {
			t.Fatal(err)
		}
		tx, _ := h.cloud.pool.Begin(h.ctx)
		defer tx.Rollback(h.ctx)
		_, err := transfersession.ClaimInTx(h.ctx, tx, sid, subj)
		if !errors.Is(err, transfersession.ErrExpired) {
			t.Fatalf("claim past deadline: %v", err)
		}
	})
}

// A failed account transaction leaves the staged secretary claimable.
func TestProbeFailedAccountTxLeavesStaged(t *testing.T) {
	h := setup(t, transfersession.Config{})
	uid := "review-tx-" + h.pid[24:]
	sid, grant := h.create(uid)
	if code, _ := h.upload(sid, grant, bytes.NewReader(h.seal(sid, grant))); code != http.StatusCreated {
		t.Fatal("upload")
	}
	_, _, err := h.provision(uid, sid,
		func(tx pgx.Tx) error { return fmt.Errorf("injected downstream failure") })
	if err == nil {
		t.Fatal("account tx should have failed")
	}
	if got := h.dbStatus(sid); got != transfersession.StatusStaged {
		t.Fatalf("session after rolled-back account tx: %s", got)
	}
	// The staged import still belongs to the session and is claimable.
	humanID, personaID, err := h.provision(uid, sid, nil)
	if err != nil {
		t.Fatalf("retry provision: %v", err)
	}
	if personaID != h.pid || humanID == "" {
		t.Fatalf("carried persona %s human %s, want %s", personaID, humanID, h.pid)
	}
	// A second claim on the now-provisioned session is refused.
	tx, _ := h.cloud.pool.Begin(h.ctx)
	defer tx.Rollback(h.ctx)
	_, err = transfersession.ClaimInTx(h.ctx, tx, sid,
		transfersession.Subject{Provider: "firebase", Subject: uid})
	if !errors.Is(err, transfersession.ErrConflict) {
		t.Fatalf("claim on provisioned: %v", err)
	}
}

// Malformed and misaddressed uploads must not poison the session.
func TestProbeBadUploadsDoNotPoison(t *testing.T) {
	h := setup(t, transfersession.Config{})
	uid := "review-bad-" + h.pid[24:]
	sid, grant := h.create(uid)

	if code, _ := h.upload(sid, grant, strings.NewReader("definitely not ndjson\n")); code != http.StatusUnprocessableEntity {
		t.Fatalf("garbage upload: %d", code)
	}
	if code, _ := h.upload(sid, grant, strings.NewReader(`{"transfer_id":"`+newID(t)+`"}`+"\n")); code != http.StatusUnprocessableEntity {
		t.Fatalf("foreign-transfer upload: %d", code)
	}
	// A bundle from a placement that does not hold this move URL is refused
	// before a row is read, and named as such. The seal has to happen on a
	// second Local placement: a source's export ledger allows one seal per
	// transfer_id, so sealing a throwaway persona for sid on h.local would
	// consume the id and refuse h.pid's real seal afterwards.
	if code, v := h.bind(sid, grant); code != http.StatusOK {
		t.Fatalf("bind: %d %+v", code, v)
	}
	other := newPlacement(t, true)
	throwPID := newID(t)
	if _, _, err := other.state.EnsurePersona(h.ctx, throwPID, nil, "throwaway"); err != nil {
		t.Fatal(err)
	}
	if _, err := other.svc.Seal(h.ctx, throwPID, sid, newID(t)); err != nil {
		t.Fatal(err)
	}
	var wrong bytes.Buffer
	if _, err := other.svc.Export(h.ctx, throwPID, sid, &wrong); err != nil {
		t.Fatal(err)
	}
	if code, raw := h.request("PUT", "/api/secretary-transfer/sessions/"+sid+"/bundle", grant, bytes.NewReader(wrong.Bytes())); code != http.StatusConflict ||
		!strings.Contains(string(raw), "accepts secretary "+h.pid) {
		t.Fatalf("foreign-placement upload: %d %s", code, raw)
	}
	// A bundle from the placement that does hold its session, sealed for a
	// destination that is not this one, is still refused by the import.
	sid2, grant2 := h.create("review-bad2-" + h.pid[24:])
	throw2PID := newID(t)
	if _, _, err := other.state.EnsurePersona(h.ctx, throw2PID, nil, "throwaway 2"); err != nil {
		t.Fatal(err)
	}
	otherPlacement, err := other.svc.PlacementID(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if code, v := h.bindAs(sid2, grant2, transfersession.Source{PlacementID: otherPlacement, PersonaID: throw2PID}); code != http.StatusOK {
		t.Fatalf("bind sid2: %d %+v", code, v)
	}
	if _, err := other.svc.Seal(h.ctx, throw2PID, sid2, newID(t)); err != nil {
		t.Fatal(err)
	}
	var misaddressed bytes.Buffer
	if _, err := other.svc.Export(h.ctx, throw2PID, sid2, &misaddressed); err != nil {
		t.Fatal(err)
	}
	if code, raw := h.request("PUT", "/api/secretary-transfer/sessions/"+sid2+"/bundle", grant2, bytes.NewReader(misaddressed.Bytes())); code != http.StatusUnprocessableEntity {
		t.Fatalf("misaddressed upload: %d %s", code, raw)
	}
	if got := h.dbStatus(sid); got != transfersession.StatusAwaitingBundle {
		t.Fatalf("session after refused uploads: %s", got)
	}
	// The real bundle is still admitted.
	code, v := h.upload(sid, grant, bytes.NewReader(h.seal(sid, grant)))
	if code != http.StatusCreated || v.Status != transfersession.StatusStaged {
		t.Fatalf("valid upload after refusals: %d %s", code, v.Status)
	}
}

// Proof visibility: registrant views never carry proofs; grant views do.
// Unknown session and wrong grant are indistinguishable (401).
func TestProbeProofVisibilityAndAuthz(t *testing.T) {
	h := setup(t, transfersession.Config{})
	uid := "review-vis-" + h.pid[24:]
	sid, grant := h.create(uid)

	for _, tc := range []struct{ name, sid, grant string }{
		{"malformed grant", sid, "not-a-grant"},
		{"valid-format wrong grant", sid, strings.Repeat("A", 43)},
		{"unknown session", newID(t), grant},
	} {
		code, _ := h.request("GET", "/api/secretary-transfer/sessions/"+tc.sid, tc.grant, nil)
		if code != http.StatusUnauthorized {
			t.Fatalf("%s: %d", tc.name, code)
		}
	}
	if code, _ := h.request("GET", "/api/secretary-transfer/sessions/"+sid, "", nil); code != http.StatusUnauthorized {
		t.Fatalf("no auth: %d", code)
	}
	// A different credential cannot even see the session exists.
	code, _ := h.request("POST", "/api/secretary-transfer/registrant/session", "",
		jsonBody(h.flow("someone-else-"+h.pid[24:], time.Minute)))
	if code != http.StatusNotFound {
		t.Fatalf("foreign registrant view: %d", code)
	}

	// Drive to activation.
	if code, _ := h.upload(sid, grant, bytes.NewReader(h.seal(sid, grant))); code != http.StatusCreated {
		t.Fatal("upload")
	}
	if _, _, err := h.provision(uid, sid, nil); err != nil {
		t.Fatal(err)
	}
	gv, err := h.sessions.Status(h.ctx, sid, grant)
	if err != nil || gv.Status != transfersession.StatusActivated || gv.ActivateProof == "" {
		t.Fatalf("grant view: %+v %v", gv, err)
	}
	rv, err := h.sessions.ForSubject(h.ctx, transfersession.Subject{Provider: "firebase", Subject: uid})
	if err != nil {
		t.Fatal(err)
	}
	if rv.ActivateProof != "" || rv.RetireProof != "" {
		t.Fatal("registrant view leaked a transfer proof")
	}
	if rv.Arrival == nil || rv.Arrival.PersonaID != h.pid {
		t.Fatalf("registrant view missing arrival: %+v", rv)
	}
}

// expect_status guards on the registrant cancel.
func TestProbeSubjectCancelExpectations(t *testing.T) {
	h := setup(t, transfersession.Config{})
	uid := "review-expect-" + h.pid[24:]

	t.Run("staged expectation on an awaiting session cancels nothing", func(t *testing.T) {
		sid, _ := h.create(uid)
		code, raw := h.request("POST", "/api/secretary-transfer/sessions/"+sid+"/cancel", "",
			jsonBody(withFlow(h.flow(uid, time.Minute), map[string]string{"expect_status": "staged"})))
		if code != http.StatusConflict {
			t.Fatalf("expect staged on awaiting: %d %s", code, raw)
		}
		if got := h.dbStatus(sid); got != transfersession.StatusAwaitingBundle {
			t.Fatalf("session wrongly changed: %s", got)
		}
	})

	t.Run("invalid expectation is rejected", func(t *testing.T) {
		uidB := uid + "-b"
		sid, _ := h.create(uidB)
		code, _ := h.request("POST", "/api/secretary-transfer/sessions/"+sid+"/cancel", "",
			jsonBody(withFlow(h.flow(uidB, time.Minute), map[string]string{"expect_status": "activated"})))
		if code != http.StatusBadRequest {
			t.Fatalf("invalid expect_status: %d", code)
		}
	})

	t.Run("grant cancel requires persona and key together", func(t *testing.T) {
		uidC := uid + "-c"
		sid, grant := h.create(uidC)
		code, _ := h.request("POST", "/api/secretary-transfer/sessions/"+sid+"/cancel", grant,
			jsonBody(map[string]string{"persona_id": h.pid}))
		if code != http.StatusBadRequest {
			t.Fatalf("persona without key: %d", code)
		}
	})

	t.Run("cancelling a closed session is a no-op", func(t *testing.T) {
		uidD := uid + "-d"
		sid, _ := h.create(uidD)
		for i := 0; i < 2; i++ {
			code, _ := h.request("POST", "/api/secretary-transfer/sessions/"+sid+"/cancel", "",
				jsonBody(h.flow(uidD, time.Minute)))
			if code != http.StatusOK {
				t.Fatalf("cancel %d: %d", i, code)
			}
		}
		if got := h.dbStatus(sid); got != transfersession.StatusCancelled {
			t.Fatalf("session: %s", got)
		}
	})
}

// An expired admission leaves no import row; the sealed source still gets
// its retirement proof through the tombstone path.
func TestProbeExpiredAdmissionLeavesNoOrphan(t *testing.T) {
	h := setup(t, transfersession.Config{AdmitTTL: 60 * time.Millisecond})
	uid := "review-exp-" + h.pid[24:]
	sid, grant := h.create(uid)
	if _, err := h.local.svc.Seal(h.ctx, h.pid, sid, mustPlacement(t, h)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	var buf bytes.Buffer
	if _, err := h.local.svc.Export(h.ctx, h.pid, sid, &buf); err != nil {
		t.Fatal(err)
	}
	code, v := h.upload(sid, grant, bytes.NewReader(buf.Bytes()))
	if code != http.StatusGone || v.Status != transfersession.StatusExpired {
		t.Fatalf("upload after admit deadline: %d %s", code, v.Status)
	}
	var rows int
	if err := h.cloud.pool.QueryRow(h.ctx,
		`SELECT count(*) FROM core_transfers WHERE direction='import' AND transfer_id=$1`, sid).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatal("an import row exists for a session that never admitted a bundle")
	}
	// The sealed source asks for retirement with its persona+key; a
	// tombstone is written and the proof unseals it.
	key := header(t, buf.Bytes()).TransferKey
	gv, err := h.sessions.CancelByGrant(h.ctx, sid, grant, h.pid, key)
	if err != nil || gv.RetireProof == "" {
		t.Fatalf("grant cancel for tombstone: %+v %v", gv, err)
	}
	if _, err := h.local.svc.Abort(h.ctx, h.pid, sid, gv.RetireProof); err != nil {
		t.Fatalf("abort with tombstone proof: %v", err)
	}
	if got := authority(t, h.local, h.pid); got != "active" {
		t.Fatalf("authority after tombstone abort: %s", got)
	}
}

// A committed import whose session-row promotion never ran is promoted by
// Sweep, and a guarded awaiting_bundle cancel cannot pass it.
func TestProbeSweepPromotesCommittedImport(t *testing.T) {
	h := setup(t, transfersession.Config{})
	uid := "review-gap-" + h.pid[24:]
	sid, grant := h.create(uid)
	bundle := h.seal(sid, grant)
	// Stage the import directly, bypassing the session admission path:
	// the same state a crash between Import commit and session update leaves.
	dst := portable.NewService(h.cloud.pool)
	if _, _, err := dst.Import(h.ctx, bytes.NewReader(bundle), nil, false); err != nil {
		t.Fatal(err)
	}
	if got := h.dbStatus(sid); got != transfersession.StatusAwaitingBundle {
		t.Fatalf("precondition: %s", got)
	}
	if n, err := h.sessions.Sweep(h.ctx); err != nil || n == 0 {
		t.Fatalf("sweep: %d %v", n, err)
	}
	if got := h.dbStatus(sid); got != transfersession.StatusStaged {
		t.Fatalf("session after sweep: %s", got)
	}
	// The guarded cancel sees the arrival even though the row lagged.
	code, _ := h.request("POST", "/api/secretary-transfer/sessions/"+sid+"/cancel", "",
		jsonBody(withFlow(h.flow(uid, time.Minute), map[string]string{"expect_status": "awaiting_bundle"})))
	if code != http.StatusConflict {
		t.Fatalf("guarded cancel passed an arrived secretary: %d", code)
	}
}

// helpers

func decodeRaw(t *testing.T, raw []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
}

func withFlow(flow map[string]string, extra map[string]string) map[string]string {
	for k, v := range extra {
		flow[k] = v
	}
	return flow
}

func mustPlacement(t *testing.T, h *harness) string {
	t.Helper()
	id, err := h.cloud.svc.PlacementID(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
