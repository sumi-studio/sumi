package portable

// F266: Activate used to ask the pool for a second connection inside the
// transaction that holds the transfer's advisory lock. On a shared pool
// (MaxConns 10, internal/db) at saturation the lock holder waited on the
// pool while everyone else waited on the lock — owed activation never
// committed. These tests pin the contract at the connection boundary: with
// a single usable connection, and with more callers than connections,
// activation still converges on exactly one committed outcome and one
// stable proof.

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// holdConns checks out all but leave pool connections, the state a shared
// pool reaches near saturation.
func holdConns(t *testing.T, p placement, leave int) []*pgxpool.Conn {
	t.Helper()
	max := int(p.pool.Config().MaxConns)
	held := make([]*pgxpool.Conn, 0, max-leave)
	for i := 0; i < max-leave; i++ {
		c, err := p.pool.Acquire(context.Background())
		if err != nil {
			t.Fatalf("hold conn %d: %v", i, err)
		}
		held = append(held, c)
	}
	return held
}

func releaseAll(held []*pgxpool.Conn) {
	for _, c := range held {
		c.Release()
	}
}

// A staged transfer must activate with exactly one usable pool connection —
// the whole operation has to fit inside the one conn its transaction holds.
func TestActivateConvergesOnOneUsableConnection(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	must(local.svc.Seal(ctx, pid, "move-pool-1", must(cloud.svc.PlacementID(ctx))))
	bundle, _ := exportBytes(t, local, pid, "move-pool-1")
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false)))

	held := holdConns(t, cloud, 1)
	defer releaseAll(held)

	actx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	rec, err := cloud.svc.Activate(actx, pid, "move-pool-1")
	if err != nil {
		t.Fatalf("activation on one usable connection: %v", err)
	}
	if rec.Status != "activated" || rec.ActivateProof == "" {
		t.Fatalf("activation under one usable connection: %+v", rec)
	}
	if a := authority(t, cloud, pid); a != "active" {
		t.Fatalf("destination authority: %s", a)
	}

	releaseAll(held)
	held = nil
	// A second activation returns the committed proof — no second
	// activation is minted.
	again := must(cloud.svc.Activate(ctx, pid, "move-pool-1"))
	if again.ActivateProof != rec.ActivateProof {
		t.Fatalf("activation proof changed: %s -> %s", rec.ActivateProof, again.ActivateProof)
	}
}

// More concurrent activators than the pool has connections must still
// converge: the advisory lock serializes them, each one sees the committed
// activation, and every caller is returned the same proof.
func TestConcurrentActivatesConvergeAbovePoolSize(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	must(local.svc.Seal(ctx, pid, "move-pool-2", must(cloud.svc.PlacementID(ctx))))
	bundle, _ := exportBytes(t, local, pid, "move-pool-2")
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false)))

	n := int(cloud.pool.Config().MaxConns) + 6
	actx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	recs := make([]Receipt, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			recs[i], errs[i] = cloud.svc.Activate(actx, pid, "move-pool-2")
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("concurrent activates did not converge")
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("activate %d: %v", i, err)
		}
	}
	for i, rec := range recs {
		if rec.Status != "activated" || rec.ActivateProof != recs[0].ActivateProof {
			t.Fatalf("activate %d saw a different outcome: %+v vs %+v", i, rec, recs[0])
		}
	}
	if a := authority(t, cloud, pid); a != "active" {
		t.Fatalf("destination authority: %s", a)
	}
}
