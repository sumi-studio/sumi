package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

type pendingTestGateway struct {
	busy  map[string]bool
	calls map[string]int
	fail  map[string]bool
}

func (g *pendingTestGateway) HasPendingRuntimeWork(ctx context.Context, id string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	g.calls[id]++
	if g.fail[id] {
		return false, errors.New("unreadable durable ACK")
	}
	return g.busy[id], nil
}

type pendingTestManager struct {
	running map[string]bool
	calls   map[string]int
	denied  map[string]bool
}

func (m *pendingTestManager) Running(id string) bool { return m.running[id] }
func (m *pendingTestManager) EnsureRunning(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.calls[id]++
	if m.denied[id] {
		return errors.New("current activation is disabled")
	}
	m.running[id] = true
	return nil
}

func TestPendingWorkRestartsColdRuntimeWithoutBrowserAndPreservesEligibility(t *testing.T) {
	now := time.Now()
	registry := warmTestRegistry{ids: []string{"cold", "idle", "unemployed", "disabled", "corrupt"}, ineligible: map[string]bool{"unemployed": true}}
	gateway := &pendingTestGateway{busy: map[string]bool{"cold": true, "unemployed": true, "disabled": true}, calls: map[string]int{}, fail: map[string]bool{"corrupt": true}}
	manager := &pendingTestManager{running: map[string]bool{}, calls: map[string]int{}, denied: map[string]bool{"disabled": true}}
	worker := pendingWorkReconciler{registry: registry, gateway: gateway, manager: manager, now: func() time.Time { return now }, timeout: time.Second, retryBase: time.Second, retryMax: 8 * time.Second, stableAfter: 10 * time.Second}
	if err := worker.reconcile(context.Background()); err == nil {
		t.Fatal("disabled/unreadable paths must report failed recovery")
	}
	if !manager.running["cold"] || manager.calls["cold"] != 1 {
		t.Fatal("durable cold work was not restored")
	}
	if manager.calls["idle"] != 0 || manager.calls["unemployed"] != 0 || gateway.calls["unemployed"] != 0 || manager.calls["corrupt"] != 0 {
		t.Fatalf("non-work or ineligible runtime admitted: %v", manager.calls)
	}
	if manager.running["disabled"] {
		t.Fatal("disabled activation was bypassed")
	}
	if err := worker.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if manager.calls["cold"] != 1 || manager.calls["disabled"] != 1 {
		t.Fatal("live runtime touched or disabled runtime hot-looped")
	}
	// A new reconciler (API restart) derives the remaining obligation from the
	// durable gateway again; it does not depend on its lost process-local map.
	manager.running["cold"] = false
	fresh := worker
	fresh.attempts = nil
	_ = fresh.reconcile(context.Background())
	if manager.calls["cold"] != 2 {
		t.Fatal("API restart lost accepted work")
	}
}

func TestPendingWorkCrashLoopBacksOffAndStopsWhenWorkIsCompletedOrCancelled(t *testing.T) {
	now := time.Now()
	gateway := &pendingTestGateway{busy: map[string]bool{"cold": true}, calls: map[string]int{}, fail: map[string]bool{}}
	manager := &pendingTestManager{running: map[string]bool{}, calls: map[string]int{}, denied: map[string]bool{}}
	worker := pendingWorkReconciler{registry: warmTestRegistry{ids: []string{"cold"}}, gateway: gateway, manager: manager, now: func() time.Time { return now }, timeout: time.Second, retryBase: time.Second, retryMax: 8 * time.Second, stableAfter: 10 * time.Second}
	if err := worker.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.running["cold"] = false // successful start followed by an immediate crash
	now = now.Add(500 * time.Millisecond)
	_ = worker.reconcile(context.Background())
	if manager.calls["cold"] != 1 {
		t.Fatal("crash caused an immediate restart loop")
	}
	now = now.Add(500 * time.Millisecond)
	_ = worker.reconcile(context.Background())
	if manager.calls["cold"] != 2 {
		t.Fatal("due crash recovery was not retried")
	}
	manager.running["cold"] = false
	now = now.Add(time.Second)
	_ = worker.reconcile(context.Background())
	if manager.calls["cold"] != 2 {
		t.Fatal("success followed by crash incorrectly reset retry backoff")
	}
	now = now.Add(time.Second)
	gateway.busy["cold"] = false // terminal ACK/AgentEnd, including explicit cancellation
	_ = worker.reconcile(context.Background())
	if manager.calls["cold"] != 2 || len(worker.attempts) != 0 {
		t.Fatal("completed work revived a stopped runtime")
	}
	gateway.busy["cold"] = true
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := worker.reconcile(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled reconciliation: %v", err)
	}
	if manager.calls["cold"] != 2 {
		t.Fatal("API shutdown restarted work")
	}
}
