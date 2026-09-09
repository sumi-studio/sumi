package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

type warmAgentRegistry interface {
	ListAgents(context.Context) ([]string, error)
	CurrentEmployer(context.Context, string) (string, string, error)
}

type warmRuntimeManager interface {
	ReconcileWarm(context.Context, string) error
}

// Warmth is existing durable desired presence. The process-local pass has no
// work queue: failures are retried from current registry state on the next tick.
func reconcileWarmAgents(ctx context.Context, registry warmAgentRegistry, manager warmRuntimeManager, timeout time.Duration) error {
	listCtx, cancel := context.WithTimeout(ctx, timeout)
	ids, err := registry.ListAgents(listCtx)
	cancel()
	if err != nil {
		return err
	}
	var failures []error
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(failures, err)...)
		}
		itemCtx, cancel := context.WithTimeout(ctx, timeout)
		// Registration alone is insufficient: an agent without a current
		// employer must not be autonomously started. Normal activation still
		// rechecks its provider and local-runtime authorization boundaries.
		_, _, err := registry.CurrentEmployer(itemCtx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			cancel()
			continue
		}
		if err == nil {
			err = manager.ReconcileWarm(itemCtx, id)
		}
		cancel()
		if err != nil {
			failures = append(failures, fmt.Errorf("reconcile warm agent %s: %w", id, err))
		}
	}
	return errors.Join(failures...)
}

func runWarmReconciliation(ctx context.Context, registry warmAgentRegistry, manager warmRuntimeManager, interval, timeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		if err := reconcileWarmAgents(ctx, registry, manager, timeout); err != nil && ctx.Err() == nil {
			log.Printf("spawn: warm reconciliation failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (a *application) startWarmReconciliation() {
	if a.spawnManager == nil || a.database == nil {
		return
	}
	a.attentionWorkers.Add(1)
	go func() {
		defer a.attentionWorkers.Done()
		runWarmReconciliation(a.backgroundCtx, koseki.New(a.database.Pool), a.spawnManager, 30*time.Second, 30*time.Second)
	}()
}
