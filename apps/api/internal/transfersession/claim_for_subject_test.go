package transfersession_test

// ClaimForSubjectInTx is the account transaction's entry: the verified
// credential subject — not a request body — selects the open session. These
// tests exercise its answers directly: no session, still awaiting, the
// interrupted-import promotion, the staged claim, and the terminal
// refusals.

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/portable"
	"github.com/sumi-studio/sumi/apps/api/internal/transfersession"
)

// freshPersona gives each subtest its own Local secretary: a persona sealed
// by an earlier move can never be sealed again.
func freshPersona(t *testing.T, h *harness) string {
	t.Helper()
	pid := newID(t)
	if _, _, err := h.local.state.EnsurePersona(context.Background(), pid, nil, "Local secretary"); err != nil {
		t.Fatal(err)
	}
	return pid
}

// stageAs runs bind→seal→export→upload for a specific local persona.
func (h *harness) stageAs(uid, pid string) (string, string) {
	t := h.t
	sid, grant := h.create(uid)
	own, err := h.local.svc.PlacementID(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if code, v := h.bindAs(sid, grant, transfersession.Source{PlacementID: own, PersonaID: pid}); code != 200 {
		t.Fatalf("bind source: %d %+v", code, v)
	}
	dest, err := h.cloud.svc.PlacementID(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.local.svc.Seal(h.ctx, pid, sid, dest); err != nil {
		t.Fatalf("seal: %v", err)
	}
	var buf bytes.Buffer
	if _, err := h.local.svc.Export(h.ctx, pid, sid, &buf); err != nil {
		t.Fatalf("export: %v", err)
	}
	if code, v := h.upload(sid, grant, &buf); code != 201 || v.Status != "staged" {
		t.Fatalf("upload: %d %+v", code, v)
	}
	return sid, grant
}

// sealAs binds and seals one local persona for the session, returning the
// exported bundle without uploading it.
func sealAs(h *harness, sessionID, grant, pid string) []byte {
	t := h.t
	own, err := h.local.svc.PlacementID(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if code, v := h.bindAs(sessionID, grant, transfersession.Source{PlacementID: own, PersonaID: pid}); code != 200 {
		t.Fatalf("bind source: %d %+v", code, v)
	}
	dest, err := h.cloud.svc.PlacementID(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.local.svc.Seal(h.ctx, pid, sessionID, dest); err != nil {
		t.Fatalf("seal: %v", err)
	}
	var buf bytes.Buffer
	if _, err := h.local.svc.Export(h.ctx, pid, sessionID, &buf); err != nil {
		t.Fatalf("export: %v", err)
	}
	return buf.Bytes()
}

func claimForSubject(t *testing.T, h *harness, uid string) (transfersession.Claim, bool, error) {
	t.Helper()
	tx, err := h.cloud.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	return h.sessions.ClaimForSubjectInTx(context.Background(), tx,
		transfersession.Subject{Provider: transfersession.ProviderFirebase, Subject: uid})
}

func TestClaimForSubjectAnswers(t *testing.T) {
	h := setup(t, transfersession.Config{})

	t.Run("no session means a fresh secretary", func(t *testing.T) {
		_, ok, err := claimForSubject(t, h, uidFor(t, "none"))
		if err != nil || ok {
			t.Fatalf("claim without a session: ok=%v err=%v", ok, err)
		}
	})

	t.Run("awaiting bundle answers pending, never a claim", func(t *testing.T) {
		uid := uidFor(t, "awaiting")
		h.create(uid)
		if _, _, err := claimForSubject(t, h, uid); !errors.Is(err, transfersession.ErrPending) {
			t.Fatalf("awaiting claim: %v", err)
		}
	})

	t.Run("a staged session claims the carried persona", func(t *testing.T) {
		uid := uidFor(t, "staged")
		pid := freshPersona(t, h)
		sid, _ := h.stageAs(uid, pid)
		claim, ok, err := claimForSubject(t, h, uid)
		if err != nil || !ok {
			t.Fatalf("staged claim: ok=%v err=%v", ok, err)
		}
		if claim.SessionID != sid || claim.PersonaID != pid {
			t.Fatalf("claim %+v, want session %s persona %s", claim, sid, pid)
		}
	})

	t.Run("a past-deadline session expires on consult, without a sweep", func(t *testing.T) {
		uid := uidFor(t, "deadline")
		sid, _ := h.create(uid)
		if _, err := h.cloud.pool.Exec(h.ctx,
			`UPDATE transfer_sessions SET admit_until = now() - interval '1 second' WHERE session_id = $1`,
			sid); err != nil {
			t.Fatal(err)
		}
		// The consult itself reaches the deadline result: the row expires
		// under the claim's row lock instead of answering pending until a
		// later sweep notices it. Commit the account transaction so the
		// expiry lands the way provisionFromFlow's commit does.
		tx, err := h.cloud.pool.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, ok, err := h.sessions.ClaimForSubjectInTx(context.Background(), tx,
			transfersession.Subject{Provider: transfersession.ProviderFirebase, Subject: uid}); err != nil || ok {
			t.Fatalf("past-deadline claim: ok=%v err=%v", ok, err)
		}
		if err := tx.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := h.dbStatus(sid); got != transfersession.StatusExpired {
			t.Fatalf("after consult: %s", got)
		}
	})

	t.Run("the deadline expiry rolls back with a failed account transaction", func(t *testing.T) {
		uid := uidFor(t, "deadline-rollback")
		sid, _ := h.create(uid)
		if _, err := h.cloud.pool.Exec(h.ctx,
			`UPDATE transfer_sessions SET admit_until = now() - interval '1 second' WHERE session_id = $1`,
			sid); err != nil {
			t.Fatal(err)
		}
		// The expiry is part of the account transaction: when the account
		// creation aborts, the session must not be left expired by a tx
		// that never committed.
		tx, err := h.cloud.pool.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, ok, err := h.sessions.ClaimForSubjectInTx(context.Background(), tx,
			transfersession.Subject{Provider: transfersession.ProviderFirebase, Subject: uid}); err != nil || ok {
			t.Fatalf("past-deadline claim: ok=%v err=%v", ok, err)
		}
		if err := tx.Rollback(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := h.dbStatus(sid); got != transfersession.StatusAwaitingBundle {
			t.Fatalf("after rollback: %s", got)
		}
	})

	t.Run("a past-deadline interrupted import still promotes and claims", func(t *testing.T) {
		uid := uidFor(t, "interrupted-late")
		pid := freshPersona(t, h)
		sid, grant := h.create(uid)
		bundle := sealAs(h, sid, grant, pid)
		// The import committed and the promotion was lost, then admit_until
		// passed. The committed staged import must still be recovered — the
		// deadline must never expire what upload already committed.
		if _, _, err := portable.NewService(h.cloud.pool).Import(h.ctx, bytes.NewReader(bundle), nil, false); err != nil {
			t.Fatal(err)
		}
		if _, err := h.cloud.pool.Exec(h.ctx,
			`UPDATE transfer_sessions SET admit_until = now() - interval '1 second' WHERE session_id = $1`,
			sid); err != nil {
			t.Fatal(err)
		}
		claim, ok, err := claimForSubject(t, h, uid)
		if err != nil || !ok {
			t.Fatalf("late promoting claim: ok=%v err=%v", ok, err)
		}
		if claim.SessionID != sid || claim.PersonaID != pid {
			t.Fatalf("claim %+v, want session %s persona %s", claim, sid, pid)
		}
	})

	t.Run("an interrupted import is promoted and claimed", func(t *testing.T) {
		uid := uidFor(t, "interrupted")
		pid := freshPersona(t, h)
		sid, grant := h.create(uid)
		bundle := sealAs(h, sid, grant, pid)
		// Import committed but the session promotion did not: the crash gap.
		if _, _, err := portable.NewService(h.cloud.pool).Import(h.ctx, bytes.NewReader(bundle), nil, false); err != nil {
			t.Fatal(err)
		}
		if got := h.dbStatus(sid); got != transfersession.StatusAwaitingBundle {
			t.Fatalf("precondition: %s", got)
		}
		claim, ok, err := claimForSubject(t, h, uid)
		if err != nil || !ok {
			t.Fatalf("promoting claim: ok=%v err=%v", ok, err)
		}
		if claim.SessionID != sid || claim.PersonaID != pid {
			t.Fatalf("claim %+v, want session %s persona %s", claim, sid, pid)
		}
	})

	t.Run("provisioned and cancelled sessions refuse", func(t *testing.T) {
		uid := uidFor(t, "provisioned")
		pid := freshPersona(t, h)
		sid, _ := h.stageAs(uid, pid)
		if _, _, err := h.provision(uid, sid, nil); err != nil {
			t.Fatalf("provision: %v", err)
		}
		if _, _, err := claimForSubject(t, h, uid); !errors.Is(err, transfersession.ErrConflict) {
			t.Fatalf("provisioned claim: %v", err)
		}

		uid = uidFor(t, "cancelled")
		sid, _ = h.create(uid)
		if code, _ := h.request("POST", "/api/secretary-transfer/sessions/"+sid+"/cancel", "",
			jsonBody(h.flow(uid, time.Minute))); code != 200 {
			t.Fatalf("cancel: %d", code)
		}
		// A cancelled session leaves no open claim: the registration mints a
		// fresh secretary instead of reviving the closed move.
		if _, ok, err := claimForSubject(t, h, uid); err != nil || ok {
			t.Fatalf("cancelled claim: ok=%v err=%v", ok, err)
		}
	})

	t.Run("another credential's session is invisible", func(t *testing.T) {
		uid := uidFor(t, "owner")
		h.stageAs(uid, freshPersona(t, h))
		other := uidFor(t, "stranger")
		if _, ok, err := claimForSubject(t, h, other); err != nil || ok {
			t.Fatalf("stranger claim: ok=%v err=%v", ok, err)
		}
	})
}

// The claimed persona binds inside the account transaction and commits the
// activation obligation; a sweep afterwards finishes activation — the same
// path a restarted process takes through Service.Run.
func TestClaimForSubjectProvisionsAndSweeps(t *testing.T) {
	h := setup(t, transfersession.Config{})
	uid := uidFor(t, "sweep")
	pid := freshPersona(t, h)
	sid, _ := h.stageAs(uid, pid)
	if _, _, err := h.provision(uid, sid, nil); err != nil {
		t.Fatalf("provision: %v", err)
	}
	if got := h.dbStatus(sid); got != transfersession.StatusProvisioned {
		t.Fatalf("after provisioning: %s", got)
	}
	if n, err := h.sessions.Sweep(h.ctx); err != nil || n == 0 {
		t.Fatalf("sweep: %d %v", n, err)
	}
	if got := h.dbStatus(sid); got != transfersession.StatusActivated {
		t.Fatalf("after sweep: %s", got)
	}
	if got := authority(t, h.cloud, pid); got != "active" {
		t.Fatalf("cloud authority: %s", got)
	}
}
