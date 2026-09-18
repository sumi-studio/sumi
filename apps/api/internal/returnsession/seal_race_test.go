package returnsession_test

// Regression tests for the bind→seal window: once the destination binding
// commits, admission has happened — a cancel or the admission deadline may
// never write a terminal status that would strand a sealing persona.

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/returnsession"
)

// TestCancelDuringSealResolves: the owner cancels while the bind's seal is
// blocked on the persona row lock. The cancel cannot race the seal — the
// bind holds the session row lock across it, so the cancel waits, sees the
// committed seal, and marks cancelling. The destination's retire proof
// then resolves the session honestly.
func TestCancelDuringSealResolves(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, _, grant := h.create()

	// Hold the persona row lock portable.Seal takes so the bind's seal
	// blocks after the binding commits — the window an owner cancel has.
	conn, err := h.cloud.pool.Acquire(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	btx, err := conn.Begin(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := btx.Exec(h.ctx,
		`SELECT authority FROM core_personas WHERE persona_id = $1 FOR NO KEY UPDATE`,
		h.persona); err != nil {
		t.Fatal(err)
	}

	d := dest(t, h.local, newID(t), "absent")
	type result struct {
		code int
		raw  []byte
	}
	done := make(chan result, 1)
	go func() {
		code, raw := h.grantReq(http.MethodPost,
			fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID),
			grant, jsonBody(d))
		done <- result{code, raw}
	}()

	// Wait for the binding commit; the seal is now blocked mid-flight.
	deadline := time.Now().Add(15 * time.Second)
	for {
		var bound bool
		if err := h.cloud.pool.QueryRow(h.ctx,
			`SELECT destination_bound_at IS NOT NULL FROM return_sessions WHERE session_id = $1`,
			sessionID).Scan(&bound); err != nil {
			t.Fatal(err)
		}
		if bound {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the binding never committed")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The owner cancel must wait behind the session row lock the in-flight
	// seal holds — that lock is the fence, so this request can only see
	// the seal's outcome, never a halfway state.
	cancelDone := make(chan result, 1)
	go func() {
		code, raw := h.ownerReq(http.MethodPost,
			fmt.Sprintf("/api/secretary-return/sessions/%s/cancel", sessionID), nil)
		cancelDone <- result{code, raw}
	}()
	select {
	case <-cancelDone:
		t.Fatal("the owner cancel did not wait behind the in-flight seal")
	case <-time.After(300 * time.Millisecond):
	}

	// Release the lock; the in-flight seal commits, then the queued cancel
	// sees the sealed row and marks cancelling — never terminal.
	if err := btx.Commit(h.ctx); err != nil {
		t.Fatal(err)
	}
	conn.Release()
	res := <-done
	if res.code != http.StatusOK {
		t.Fatalf("bind: %d %s", res.code, res.raw)
	}
	cres := <-cancelDone
	if cres.code != http.StatusOK {
		t.Fatalf("owner cancel: %d %s", cres.code, cres.raw)
	}
	if got := authority(t, h.cloud, h.persona); got != "sealed" {
		t.Fatalf("authority %s (the seal committed)", got)
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusCancelling {
		t.Fatalf("session %s (want cancelling)", got)
	}

	// The destination tombstones the never-arrived transfer and reports
	// its proof; the source unseals on it — no strand.
	code, raw := h.grantReq(http.MethodGet,
		fmt.Sprintf("/api/secretary-return/sessions/%s", sessionID), grant, nil)
	if code != http.StatusOK {
		t.Fatalf("status for transfer key: %d %s", code, raw)
	}
	v := asView(t, raw)
	if v.TransferKey == "" {
		t.Fatal("the cancelling view carries no transfer key for the tombstone")
	}
	rec, err := h.local.svc.Retire(h.ctx, h.persona, sessionID, mustPlacement(t, h.local), v.TransferKey)
	if err != nil {
		t.Fatalf("tombstone: %v", err)
	}
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/retired", sessionID),
		grant, jsonBody(map[string]string{"retire_proof": rec.RetireProof}))
	if code != http.StatusOK {
		t.Fatalf("retired report: %d %s", code, raw)
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusAborted {
		t.Fatalf("session %s (want aborted)", got)
	}
	if got := authority(t, h.cloud, h.persona); got != "active" {
		t.Fatalf("authority %s (the source unsealed)", got)
	}
}

// TestExpiredBoundSessionStaysResumable: a bound-but-unsealed session (the
// state a crash between the binding commit and the seal leaves) does not
// expire — admission already happened. The retried bind seals, and the
// transfer completes normally.
func TestExpiredBoundSessionStaysResumable(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, _, grant := h.create()

	d := dest(t, h.local, newID(t), "absent")
	mustExec(t, h.cloud.pool, `UPDATE return_sessions
		SET destination_placement_id = $2, destination_persona_id = $3,
		    destination_slot_state = $4, destination_bound_at = now(), updated_at = now()
		WHERE session_id = $1`, sessionID, d.PlacementID, d.PersonaID, d.SlotState)
	mustExec(t, h.cloud.pool,
		`UPDATE return_sessions SET admit_until = now() - interval '1 second' WHERE session_id = $1`, sessionID)

	if err := h.sessions.Reconcile(h.ctx, sessionID); err != nil {
		t.Fatal(err)
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusAwaitingDestination {
		t.Fatalf("session %s — a bound session must not expire", got)
	}

	// The retry seals: the deadline bounds admission, not the seal.
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID),
		grant, jsonBody(d))
	if code != http.StatusOK {
		t.Fatalf("re-bind: %d %s", code, raw)
	}
	if got := authority(t, h.cloud, h.persona); got != "sealed" {
		t.Fatalf("authority %s", got)
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusSealed {
		t.Fatalf("session %s", got)
	}
}

// TestBoundNeverSealedCancelResolves: the binding committed, the process
// died before the seal, and the owner cancelled. The seal gate only runs
// while the session is awaiting/sealed, so cancelling is a durable
// exclusion — no seal can ever start, the destination provably holds
// nothing, and the next reconciled read closes the session cancelled
// with no proof. Authority never moved.
func TestBoundNeverSealedCancelResolves(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, _, grant := h.create()

	d := dest(t, h.local, newID(t), "absent")
	mustExec(t, h.cloud.pool, `UPDATE return_sessions
		SET destination_placement_id = $2, destination_persona_id = $3,
		    destination_slot_state = $4, destination_bound_at = now(), updated_at = now()
		WHERE session_id = $1`, sessionID, d.PlacementID, d.PersonaID, d.SlotState)

	// The cancel's own reconcile resolves it: no export exists and none
	// can ever start (the seal gate only runs on awaiting/sealed), so the
	// answer is cancelled outright — nothing moved, no proof needed.
	code, raw := h.ownerReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/cancel", sessionID), nil)
	if code != http.StatusOK {
		t.Fatalf("cancel: %d %s", code, raw)
	}
	if got := asView(t, raw).Status; got != returnsession.StatusCancelled {
		t.Fatalf("cancel answer %s (want cancelled — nothing ever sealed)", got)
	}
	if got := authority(t, h.cloud, h.persona); got != "active" {
		t.Fatalf("authority %s (still the source's)", got)
	}
	var exports int
	if err := h.cloud.pool.QueryRow(h.ctx, `SELECT count(*) FROM core_transfers
		WHERE direction = 'export' AND transfer_id = $1`, sessionID).Scan(&exports); err != nil {
		t.Fatal(err)
	}
	if exports != 0 {
		t.Fatalf("an export appeared on a cancelled session: %d", exports)
	}

	// A re-bind after the session closed must be refused — the fence
	// that keeps a seal from ever committing after cancelled.
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID),
		grant, jsonBody(d))
	if code != http.StatusGone {
		t.Fatalf("re-bind after cancelled: %d %s (want 410)", code, raw)
	}
	if err := h.cloud.pool.QueryRow(h.ctx, `SELECT count(*) FROM core_transfers
		WHERE direction = 'export' AND transfer_id = $1`, sessionID).Scan(&exports); err != nil {
		t.Fatal(err)
	}
	if exports != 0 {
		t.Fatalf("a seal committed after the session closed: %d exports", exports)
	}
	if got := authority(t, h.cloud, h.persona); got != "active" {
		t.Fatalf("authority %s (still the source's)", got)
	}
}

// TestSealRefusedWhenCancelLandsFirst is the reverse interleaving of the
// in-flight-seal race: the owner cancel commits between the binding and
// the seal attempt, so when the bind reaches the seal gate the session
// is already cancelling. The seal is refused under the session row
// lock — no export appears, the session resolves cancelled, authority
// never left active.
func TestSealRefusedWhenCancelLandsFirst(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, _, grant := h.create()

	d := dest(t, h.local, newID(t), "absent")
	mustExec(t, h.cloud.pool, `UPDATE return_sessions
		SET destination_placement_id = $2, destination_persona_id = $3,
		    destination_slot_state = $4, destination_bound_at = now(), updated_at = now()
		WHERE session_id = $1`, sessionID, d.PlacementID, d.PersonaID, d.SlotState)
	// The cancel answer already resolves the never-sealed session:
	// cancelling was written, reconcile found no export, cancelled it is.
	code, raw := h.ownerReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/cancel", sessionID), nil)
	if code != http.StatusOK {
		t.Fatalf("cancel: %d %s", code, raw)
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusCancelled {
		t.Fatalf("session %s (want cancelled)", got)
	}

	// The retried bind must be refused — no seal may commit on a session
	// that already closed. This is the fence the whole invariant rests on.
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID),
		grant, jsonBody(d))
	if code != http.StatusGone {
		t.Fatalf("re-bind on a cancelled session: %d %s (want 410)", code, raw)
	}
	var exports int
	if err := h.cloud.pool.QueryRow(h.ctx, `SELECT count(*) FROM core_transfers
		WHERE direction = 'export' AND transfer_id = $1`, sessionID).Scan(&exports); err != nil {
		t.Fatal(err)
	}
	if exports != 0 {
		t.Fatalf("a seal committed on a cancelling session: %d exports", exports)
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusCancelled {
		t.Fatalf("session %s (want cancelled)", got)
	}
	if got := authority(t, h.cloud, h.persona); got != "active" {
		t.Fatalf("authority %s (nothing moved)", got)
	}
}

// TestRetireProofBeforeSealCommitRefused: a retire proof delivered while
// the session is still awaiting — the seal has not committed — is
// meaningless and refused, because the destination provably holds
// nothing. Once the seal does commit the ordinary proof path resolves:
// cancel → cancelling → real retire proof → aborted.
func TestRetireProofBeforeSealCommitRefused(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, _, grant := h.create()

	d := dest(t, h.local, newID(t), "absent")
	mustExec(t, h.cloud.pool, `UPDATE return_sessions
		SET destination_placement_id = $2, destination_persona_id = $3,
		    destination_slot_state = $4, destination_bound_at = now(), updated_at = now()
		WHERE session_id = $1`, sessionID, d.PlacementID, d.PersonaID, d.SlotState)

	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/retired", sessionID),
		grant, jsonBody(map[string]string{"retire_proof": "proof-before-seal"}))
	if code != http.StatusGone {
		t.Fatalf("retire proof on an awaiting session: %d %s (want 410)", code, raw)
	}
	if got := authority(t, h.cloud, h.persona); got != "active" {
		t.Fatalf("authority %s", got)
	}

	// The seal still commits on the retried bind; then the normal
	// cancel → retire-proof path resolves the session aborted.
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID),
		grant, jsonBody(d))
	if code != http.StatusOK {
		t.Fatalf("re-bind: %d %s", code, raw)
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusSealed {
		t.Fatalf("session %s (want sealed)", got)
	}
	code, raw = h.ownerReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/cancel", sessionID), nil)
	if code != http.StatusOK {
		t.Fatalf("cancel: %d %s", code, raw)
	}
	code, raw = h.grantReq(http.MethodGet,
		fmt.Sprintf("/api/secretary-return/sessions/%s", sessionID), grant, nil)
	if code != http.StatusOK {
		t.Fatalf("status: %d %s", code, raw)
	}
	v := asView(t, raw)
	rec, err := h.local.svc.Retire(h.ctx, h.persona, sessionID, mustPlacement(t, h.local), v.TransferKey)
	if err != nil {
		t.Fatalf("tombstone: %v", err)
	}
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/retired", sessionID),
		grant, jsonBody(map[string]string{"retire_proof": rec.RetireProof}))
	if code != http.StatusOK {
		t.Fatalf("retired report: %d %s", code, raw)
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusAborted {
		t.Fatalf("session %s (want aborted)", got)
	}
	if got := authority(t, h.cloud, h.persona); got != "active" {
		t.Fatalf("authority %s (the source unsealed)", got)
	}
}

// TestPrefixedPublicBaseRefused: a public base URL carrying a path prefix
// would mint return URLs the Local command can never accept — the receiver
// anchors session paths at RoutePrefix. Refuse it at construction instead.
func TestPrefixedPublicBaseRefused(t *testing.T) {
	h := setup(t, returnsession.Config{})
	if _, err := returnsession.NewServer(h.sessions, h.proof, h.srv.URL+"/prefix"); err == nil {
		t.Fatal("a path-prefixed public base was accepted")
	}
	if _, err := returnsession.NewServer(h.sessions, h.proof, h.srv.URL); err != nil {
		t.Fatalf("a clean origin was refused: %v", err)
	}
}

// sessionStatus reads the session row's status directly.
func (h *harness) sessionStatus(sessionID string) string {
	h.t.Helper()
	var s string
	if err := h.cloud.pool.QueryRow(h.ctx,
		`SELECT status FROM return_sessions WHERE session_id = $1`, sessionID).Scan(&s); err != nil {
		h.t.Fatal(err)
	}
	return s
}
