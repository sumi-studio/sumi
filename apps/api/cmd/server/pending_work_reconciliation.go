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

type pendingWorkGateway interface {
	HasPendingRuntimeWork(context.Context, string) (bool, error)
}

type pendingWorkManager interface {
	Running(string) bool
	EnsureRunning(context.Context, string) error
}

type pendingRestartAttempt struct {
	failures uint
	next     time.Time
	started  time.Time
}

// Accepted commands and unclosed runs are durable work, independently of the
// person's cold/warm cost setting. A received ACK is deliberately not terminal:
// this also protects a deferred notification until its Session timer delivers it.
// This reconciler never marks a command complete or replays an effect itself.
type pendingWorkReconciler struct {
	registry    warmAgentRegistry
	gateway     pendingWorkGateway
	manager     pendingWorkManager
	attempts    map[string]pendingRestartAttempt
	now         func() time.Time
	timeout     time.Duration
	retryBase   time.Duration
	retryMax    time.Duration
	stableAfter time.Duration
}

func (r *pendingWorkReconciler) reconcile(ctx context.Context) error {
	listCtx, cancel := context.WithTimeout(ctx, r.timeout)
	ids, err := r.registry.ListAgents(listCtx)
	cancel()
	if err != nil {
		return err
	}
	if r.attempts == nil {
		r.attempts = make(map[string]pendingRestartAttempt)
	}
	known := make(map[string]bool, len(ids))
	var failures []error
	for _, id := range ids {
		known[id] = true
		if err := ctx.Err(); err != nil {
			return errors.Join(append(failures, err)...)
		}
		now := r.now()
		attempt := r.attempts[id]
		if r.manager.Running(id) {
			if !attempt.started.IsZero() && now.Sub(attempt.started) >= r.stableAfter {
				delete(r.attempts, id)
			}
			continue // Do not record synthetic activity or prevent real idle stop.
		}
		if now.Before(attempt.next) {
			continue
		}
		itemCtx, cancel := context.WithTimeout(ctx, r.timeout)
		_, _, err := r.registry.CurrentEmployer(itemCtx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			cancel()
			delete(r.attempts, id)
			continue
		}
		var pending bool
		if err == nil {
			// This read does not admit commands or change durable state.
			// EnsureRunning owns current authorization and generation fencing.
			pending, err = r.gateway.HasPendingRuntimeWork(itemCtx, id)
		}
		if err == nil && !pending {
			cancel()
			delete(r.attempts, id)
			continue
		}
		if err == nil {
			err = r.manager.EnsureRunning(itemCtx, id)
		}
		cancel()
		attempt.failures++
		delay := r.retryBase
		for n := uint(1); n < attempt.failures && delay < r.retryMax; n++ {
			if delay > r.retryMax/2 {
				delay = r.retryMax
			} else {
				delay *= 2
			}
		}
		if delay > r.retryMax {
			delay = r.retryMax
		}
		attempt.next = now.Add(delay)
		if err == nil {
			attempt.started = now
		} else {
			attempt.started = time.Time{}
		}
		r.attempts[id] = attempt
		if err != nil {
			failures = append(failures, fmt.Errorf("resume accepted work for %s: %w", id, err))
		}
	}
	for id := range r.attempts {
		if !known[id] {
			delete(r.attempts, id)
		}
	}
	return errors.Join(failures...)
}

func (r *pendingWorkReconciler) run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		if err := r.reconcile(ctx); err != nil && ctx.Err() == nil {
			// Provider/admission errors can carry connection detail.
			log.Print("spawn: unfinished-work reconciliation failed; durable work will retry")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (a *application) startPendingWorkReconciliation() {
	if a.spawnManager == nil || a.database == nil || a.pendingWorkGateway == nil {
		return
	}
	worker := &pendingWorkReconciler{
		registry: koseki.New(a.database.Pool), gateway: a.pendingWorkGateway, manager: a.spawnManager,
		now: time.Now, timeout: 30 * time.Second, retryBase: 30 * time.Second, retryMax: 10 * time.Minute, stableAfter: 2 * time.Minute,
	}
	a.attentionWorkers.Add(1)
	go func() { defer a.attentionWorkers.Done(); worker.run(a.backgroundCtx, 30*time.Second) }()
}
