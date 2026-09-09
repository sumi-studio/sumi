package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type warmTestRegistry struct {
	ids        []string
	ineligible map[string]bool
}

func (r warmTestRegistry) ListAgents(context.Context) ([]string, error) { return r.ids, nil }
func (r warmTestRegistry) CurrentEmployer(_ context.Context, id string) (string, string, error) {
	if r.ineligible[id] {
		return "", "", pgx.ErrNoRows
	}
	return "human", "employer", nil
}

type warmTestManager struct {
	mu        sync.Mutex
	calls     map[string]int
	block     string
	failFirst string
	notify    chan string
}

func (m *warmTestManager) ReconcileWarm(ctx context.Context, id string) error {
	m.mu.Lock()
	m.calls[id]++
	attempt := m.calls[id]
	m.mu.Unlock()
	if m.notify != nil {
		m.notify <- id
	}
	if id == m.block {
		<-ctx.Done()
		return ctx.Err()
	}
	if id == m.failFirst && attempt == 1 {
		return errors.New("temporary startup failure")
	}
	return nil
}

func TestWarmReconciliationBoundsEachAgentAndRechecksEligibility(t *testing.T) {
	registry := warmTestRegistry{ids: []string{"unemployed", "deleted", "slow", "failed", "healthy"}, ineligible: map[string]bool{"unemployed": true, "deleted": true}}
	manager := &warmTestManager{calls: make(map[string]int), block: "slow", failFirst: "failed"}
	for pass := 0; pass < 2; pass++ {
		if err := reconcileWarmAgents(context.Background(), registry, manager, 5*time.Millisecond); err == nil {
			t.Fatal("expected per-agent failures")
		}
	}
	if manager.calls["unemployed"] != 0 || manager.calls["deleted"] != 0 {
		t.Fatal("ineligible agent admitted")
	}
	for _, id := range []string{"slow", "failed", "healthy"} {
		if manager.calls[id] != 2 {
			t.Fatalf("%s did not progress across failed passes: %v", id, manager.calls)
		}
	}
}

func TestWarmReconciliationStartsImmediatelyRetriesAndStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := warmTestRegistry{ids: []string{"warm"}}
	manager := &warmTestManager{calls: make(map[string]int), failFirst: "warm", notify: make(chan string, 16)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		runWarmReconciliation(ctx, registry, manager, 20*time.Millisecond, time.Second)
	}()
	for attempt := 0; attempt < 2; attempt++ {
		select {
		case <-manager.notify:
		case <-time.After(time.Second):
			t.Fatal("startup/retry did not run")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reconciliation did not stop")
	}
	manager.mu.Lock()
	before := manager.calls["warm"]
	manager.mu.Unlock()
	if err := reconcileWarmAgents(ctx, registry, manager, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled pass: %v", err)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.calls["warm"] != before {
		t.Fatal("cancelled pass started a runtime")
	}
}
