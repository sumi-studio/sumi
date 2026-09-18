package returnsession_test

// Liveness regressions for the seal gate (F383): the session row lock that
// fences seal against cancel must never be held on one pooled connection
// while the seal waits for another — portable.Activate documents the same
// contract ("a transaction that holds the lock cannot wait on the pool").
// portable.SealTx joins bindAndSeal's transaction, so these tests pin the
// pool to a single usable connection and require the seal to complete.

import (
	"context"
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
	created, _, err := svc.Create(ctx, returnsession.Owner{HumanID: human, PersonaID: pid})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := svc.BindDestination(dctx, created.View.SessionID, created.Grant,
		returnsession.Destination{PlacementID: newID(t), PersonaID: pid, SlotState: "absent"}); err != nil {
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
	created, _, err := svc.Create(ctx, returnsession.Owner{HumanID: human, PersonaID: pid})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	dest := returnsession.Destination{PlacementID: newID(t), PersonaID: pid, SlotState: "absent"}

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
