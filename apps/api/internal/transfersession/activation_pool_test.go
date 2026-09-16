package transfersession_test

// F266 regression coverage at the session layer: a provisioned session owes
// activation, and Reconcile is the path that pays it. That obligation must
// finish near pool saturation — one usable connection must be enough — and
// more concurrent reconcilers than the pool has connections must converge on
// exactly one activation and one stable proof. Before the repair, portable's
// Activate acquired a second pool connection while its transaction held the
// transfer's advisory lock, so either of these shapes deadlocked the pool.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sumi-studio/sumi/apps/api/internal/transfersession"
)

// With every pool connection but one held, the owed activation still
// commits: the reconcile path must never need a second connection while it
// holds the transfer's advisory lock.
func TestReconcileConvergesOnOneUsableConnection(t *testing.T) {
	h := setup(t, transfersession.Config{})
	uid := uidFor(t, "oneconn")
	sid, grant, _ := h.stage(uid)
	if _, _, err := h.provision(uid, sid, nil); err != nil {
		t.Fatalf("provision: %v", err)
	}

	max := int(h.cloud.pool.Config().MaxConns)
	held := make([]*pgxpool.Conn, 0, max-1)
	for i := 0; i < max-1; i++ {
		c, err := h.cloud.pool.Acquire(h.ctx)
		if err != nil {
			t.Fatalf("hold conn %d: %v", i, err)
		}
		held = append(held, c)
	}
	defer func() {
		for _, c := range held {
			c.Release()
		}
	}()

	ctx, cancel := context.WithTimeout(h.ctx, 15*time.Second)
	defer cancel()
	if err := h.sessions.Reconcile(ctx, sid); err != nil {
		t.Fatalf("reconcile on one usable connection: %v", err)
	}
	if got := h.dbStatus(sid); got != transfersession.StatusActivated {
		t.Fatalf("session after reconcile under saturation: %s", got)
	}
	if a := authority(t, h.cloud, h.pid); a != "active" {
		t.Fatalf("destination authority: %s", a)
	}

	for _, c := range held {
		c.Release()
	}
	held = nil
	v := h.view(sid, grant)
	if v.Status != transfersession.StatusActivated || v.ActivateProof == "" {
		t.Fatalf("view after reconcile under saturation: %+v", v)
	}
}

// Concurrent reconcilers above the pool size converge on exactly one
// activation; the carried proof is stable across all of them.
func TestConcurrentReconcileAbovePoolSizeActivatesOnce(t *testing.T) {
	h := setup(t, transfersession.Config{})
	uid := uidFor(t, "abovlpool")
	sid, grant, _ := h.stage(uid)
	if _, _, err := h.provision(uid, sid, nil); err != nil {
		t.Fatalf("provision: %v", err)
	}

	n := int(h.cloud.pool.Config().MaxConns) + 6
	ctx, cancel := context.WithTimeout(h.ctx, 30*time.Second)
	defer cancel()
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = h.sessions.Reconcile(ctx, sid)
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("concurrent reconciles did not converge")
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}

	v := h.view(sid, grant)
	if v.Status != transfersession.StatusActivated || v.ActivateProof == "" {
		t.Fatalf("after concurrent reconcile: %+v", v)
	}
	// The activation proof is stable: no second activation minted another.
	first := v.ActivateProof
	if err := h.sessions.Reconcile(h.ctx, sid); err != nil {
		t.Fatal(err)
	}
	if again := h.view(sid, grant).ActivateProof; again != first {
		t.Fatalf("activation proof changed: %s -> %s", first, again)
	}
}
