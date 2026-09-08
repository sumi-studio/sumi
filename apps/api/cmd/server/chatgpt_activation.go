package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

type chatGPTActivationManager interface {
	StopIfIdle(string) (bool, error)
	EnsureRunning(context.Context, string) error
}
type chatGPTActivationEmployer interface {
	AgentForHuman(context.Context, string) (string, error)
	AuthorizeCurrentHumanEmployer(context.Context, string, string, func() error) error
}

// Coalesce repeated selections by Human, without one goroutine per change.
// The connection store is the durable latest selection; this queue only nudges
// already-running processes to pick it up once their current work is finished.
type chatGPTActivationWorker struct {
	mu        sync.Mutex
	pending   map[string]uint64
	revision  uint64
	wake      chan struct{}
	employers chatGPTActivationEmployer
	manager   chatGPTActivationManager
}

func newChatGPTActivationWorker(employers chatGPTActivationEmployer) *chatGPTActivationWorker {
	return &chatGPTActivationWorker{pending: make(map[string]uint64), wake: make(chan struct{}, 1), employers: employers}
}
func (w *chatGPTActivationWorker) enqueue(human string) {
	w.mu.Lock()
	w.revision++
	w.pending[human] = w.revision
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
}
func (w *chatGPTActivationWorker) apply(ctx context.Context, human string) (bool, error) {
	pa, err := w.employers.AgentForHuman(ctx, human)
	if errors.Is(err, pgx.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	// Validate ownership, then release its lease before process teardown.
	// Teardown fences local runtime credentials; holding the employer lease
	// there would invert the authenticated credential route's lock order.
	err = w.employers.AuthorizeCurrentHumanEmployer(ctx, human, pa, func() error { return nil })
	if errors.Is(err, koseki.ErrNotCurrentEmployer) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	ready, err := w.manager.StopIfIdle(pa)
	if err != nil || !ready {
		return false, err
	}
	// Activation itself derives and rechecks the current employer again.
	return true, w.manager.EnsureRunning(ctx, pa)
}
func (w *chatGPTActivationWorker) run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.wake:
		case <-ticker.C:
		}
		w.mu.Lock()
		batch := make(map[string]uint64, len(w.pending))
		for human, revision := range w.pending {
			batch[human] = revision
		}
		w.mu.Unlock()
		for human, revision := range batch {
			if ctx.Err() != nil {
				return
			}
			itemCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			complete, err := w.apply(itemCtx, human)
			cancel()
			if complete && err == nil {
				w.mu.Lock()
				if w.pending[human] == revision {
					delete(w.pending, human)
				}
				w.mu.Unlock()
			}
		}
	}
}

func (a *application) startChatGPTActivation() {
	if a.chatGPTActivation == nil || a.chatGPTActivation.manager == nil {
		return
	}
	a.attentionWorkers.Add(1)
	go func() { defer a.attentionWorkers.Done(); a.chatGPTActivation.run(a.backgroundCtx) }()
}
