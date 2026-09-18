package returnsession_test

// Liveness regressions for the seal gate (F383): the session row lock that
// fences seal against cancel must never be held on one pooled connection
// while the seal waits for another — portable.Activate documents the same
// contract ("a transaction that holds the lock cannot wait on the pool").
// portable.SealTx joins bindAndSeal's transaction, so these tests pin the
// pool to a single usable connection and require the seal to complete.

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/returnsession"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

// livenessSetup is probeSetup's shape: one placement, one persona, the
// session service under the explicit test-only fixture policy — on a pool
// of exactly maxConns connections.
func livenessSetup(t *testing.T, maxConns int32) (*pgxpool.Pool, *returnsession.Service, string, string) {
	t.Helper()
	ctx := context.Background()
	pool := testdb.CreateWithMaxConns(t, maxConns)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	human, pid := newID(t), newID(t)
	mustExec(t, pool, `INSERT INTO humans (human_id) VALUES ($1)`, human)
	if _, _, err := agentstate.NewStore(pool).EnsurePersona(ctx, pid, nil, "sec"); err != nil {
		t.Fatal(err)
	}
	mustExec(t, pool, `UPDATE core_personas SET human_id = $1 WHERE persona_id = $2`, human, pid)
	svc := returnsession.New(pool, returnsession.Config{FilePolicy: returnsession.FilePolicyFixture})
	return pool, svc, human, pid
}

func rowStatus(t *testing.T, pool *pgxpool.Pool, sessionID string) string {
	t.Helper()
	var s string
	if err := pool.QueryRow(context.Background(),
		`SELECT status FROM return_sessions WHERE session_id = $1`, sessionID).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

// MaxConns=1: the seal gate and the seal itself must run on the one
// connection. Before SealTx this deadlocked deterministically — the row
// lock's connection was the pool. The deadline is a test backstop, not the
// mechanism: a healthy seal completes in milliseconds.
func TestSealSucceedsAtPoolOne(t *testing.T) {
	pool, svc, human, pid := livenessSetup(t, 1)
	ctx := context.Background()
	created, _, err := svc.Create(ctx, returnsession.Owner{HumanID: human, PersonaID: pid}, "cloud")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := svc.BindDestination(dctx, created.View.SessionID, created.Grant,
		returnsession.Destination{PlacementID: newID(t), PersonaID: pid, SlotState: "absent", FileMode: "cloud"}); err != nil {
		t.Fatalf("bind on a single-connection pool: %v", err)
	}
	if got := authority(t, placement{pool: pool}, pid); got != "sealed" {
		t.Fatalf("authority %s", got)
	}
	if got := rowStatus(t, pool, created.View.SessionID); got != returnsession.StatusSealed {
		t.Fatalf("session %s", got)
	}
}

// MaxConns=2 with one ordinary connection held — the shape an in-flight
// export stream or any other request's transaction produces in production.
// The seal still completes on the one free connection, and a same-session
// cancel afterward resolves to cancelling (proof-owed), not a wedge.
func TestSealSucceedsWithOneHeldConn(t *testing.T) {
	pool, svc, human, pid := livenessSetup(t, 2)
	ctx := context.Background()
	created, _, err := svc.Create(ctx, returnsession.Owner{HumanID: human, PersonaID: pid}, "cloud")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	dest := returnsession.Destination{PlacementID: newID(t), PersonaID: pid, SlotState: "absent", FileMode: "cloud"}

	// One unrelated held connection — the pool has exactly one left.
	held, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	_, err = svc.BindDestination(dctx, created.View.SessionID, created.Grant, dest)
	cancel()
	held.Release()
	if err != nil {
		t.Fatalf("bind with one held connection: %v", err)
	}
	if got := authority(t, placement{pool: pool}, pid); got != "sealed" {
		t.Fatalf("authority %s", got)
	}
	if got := rowStatus(t, pool, created.View.SessionID); got != returnsession.StatusSealed {
		t.Fatalf("session %s", got)
	}

	// A replayed bind on the sealed session is a clean replay on the same
	// single-connection path.
	held, err = pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dctx2, cancel2 := context.WithTimeout(ctx, 30*time.Second)
	_, err = svc.BindDestination(dctx2, created.View.SessionID, created.Grant, dest)
	cancel2()
	held.Release()
	if err != nil {
		t.Fatalf("replay bind with one held connection: %v", err)
	}

	// Same-session cancel after the seal commits resolves to cancelling —
	// the proof-owed state — on the same small pool.
	if _, err := svc.CancelByOwner(ctx, created.View.SessionID,
		returnsession.Owner{HumanID: human, PersonaID: pid}); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if got := rowStatus(t, pool, created.View.SessionID); got != returnsession.StatusCancelling {
		t.Fatalf("session %s after cancel", got)
	}
}

// TestSweepResolvesBoundCancellingNeverSealed (F386 / A3): the crash window
// between the cancel's commit and its reconcile — bound, cancelling, no
// export — used to be invisible to Sweep, so the row only resolved on an
// interactive read. The new no-export selection is only a work list:
// resolveNeverSealedCancel re-decides under the session row lock, so an
// in-flight seal holding that lock is never misjudged by the snapshot.
func TestSweepResolvesBoundCancellingNeverSealed(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, _, grant := h.create()
	d := dest(t, h.local, newID(t), "absent")
	// The crash window: the cancel committed bound+cancelling and the
	// process stopped before reconciledView — no export exists.
	mustExec(t, h.cloud.pool, `UPDATE return_sessions
		SET destination_placement_id = $2, destination_persona_id = $3,
		    destination_slot_state = $4, destination_bound_at = now(),
		    status = 'cancelling', updated_at = now()
		WHERE session_id = $1`, sessionID, d.PlacementID, d.PersonaID, d.SlotState)

	n, err := h.sessions.Sweep(h.ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n == 0 {
		t.Fatal("sweep did not select the bound never-sealed cancelling session")
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusCancelled {
		t.Fatalf("session %s after sweep (want cancelled — nothing ever sealed)", got)
	}
	if got := authority(t, h.cloud, h.persona); got != "active" {
		t.Fatalf("authority %s (still the source's)", got)
	}
	// The seal fence is intact: a re-bind after the swept close is refused.
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID),
		grant, jsonBody(d))
	if code != http.StatusGone {
		t.Fatalf("re-bind after swept cancelled: %d %s (want 410)", code, raw)
	}
	var exports int
	if err := h.cloud.pool.QueryRow(h.ctx, `SELECT count(*) FROM core_transfers
		WHERE direction = 'export' AND transfer_id = $1`, sessionID).Scan(&exports); err != nil || exports != 0 {
		t.Fatalf("a seal committed after the session closed: %d exports", exports)
	}
}

// TestSweepKeepsCancellingAwaitingProof: a cancelling session that DOES
// have a sealed export must not be resolved by the sweep — it owes a real
// destination retirement proof, and only that proof may unseal the source.
func TestSweepKeepsCancellingAwaitingProof(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, _, grant := h.create()
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID),
		grant, jsonBody(dest(t, h.local, h.persona, "absent")))
	if code != http.StatusOK {
		t.Fatalf("bind: %d %s", code, raw)
	}
	code, raw = h.ownerReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/cancel", sessionID), nil)
	if code != http.StatusOK {
		t.Fatalf("cancel: %d %s", code, raw)
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusCancelling {
		t.Fatalf("session %s after cancel", got)
	}

	if _, err := h.sessions.Sweep(h.ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusCancelling {
		t.Fatalf("sweep resolved a proof-owed cancel: %s", got)
	}
	if got := authority(t, h.cloud, h.persona); got != "sealed" {
		t.Fatalf("the sweep unsealed the source without a proof: %s", got)
	}
}
