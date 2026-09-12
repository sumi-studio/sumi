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

type runtimeRecoveryManager interface {
	Running(string) bool
	RestoreRunning(context.Context, string) error
}

// Recover every employed live runtime, including cold runtimes with work that
// has not yet reached the API event stream. Missing runtimes remain absent.
func recoverExistingRuntimes(ctx context.Context, registry warmAgentRegistry, manager runtimeRecoveryManager, timeout time.Duration) error {
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
		if manager.Running(id) {
			continue
		}
		itemCtx, cancel := context.WithTimeout(ctx, timeout)
		_, _, err := registry.CurrentEmployer(itemCtx, id)
		if err == nil {
			err = manager.RestoreRunning(itemCtx, id)
		}
		cancel()
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			failures = append(failures, fmt.Errorf("recover runtime %s: %w", id, err))
		}
	}
	return errors.Join(failures...)
}

func (a *application) startRuntimeRecovery() {
	if a.spawnManager == nil || a.database == nil {
		return
	}
	a.attentionWorkers.Add(1)
	go func() {
		defer a.attentionWorkers.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			if err := recoverExistingRuntimes(a.backgroundCtx, koseki.New(a.database.Pool), a.spawnManager, 30*time.Second); err != nil && a.backgroundCtx.Err() == nil {
				log.Printf("spawn: API runtime recovery unavailable; preserving existing compute: %v", err)
			}
			select {
			case <-a.backgroundCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}
