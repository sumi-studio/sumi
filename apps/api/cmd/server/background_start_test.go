package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/messaging"
)

// backgroundTestApplication has side-effect loops that count their passes:
// the attention delivery and feedback cleanup, which check for cancellation
// first, and startEager's worker, which makes one pass before it could.
func backgroundTestApplication() (*application, *atomic.Int64) {
	ctx, cancel := context.WithCancel(context.Background())
	var passes atomic.Int64
	return &application{
		backgroundCtx:  ctx,
		stopBackground: cancel,
		deliverAttention: func(context.Context) (messaging.AgentAttentionDeliveryStats, error) {
			passes.Add(1)
			return messaging.AgentAttentionDeliveryStats{}, nil
		},
		cleanupFeedbackAttachments: func(context.Context) error {
			passes.Add(1)
			return nil
		},
	}, &passes
}

func startEager(app *application, passes *atomic.Int64) {
	app.attentionWorkers.Add(1)
	go func() { defer app.attentionWorkers.Done(); passes.Add(1) }()
}

// N-4: the lease is lost after the application was built but before run
// registered the stop hook and started the workers. The hook closes the
// application at once, and no worker starts.
func TestLeaseLostBeforeBackgroundStartStartsNothing(t *testing.T) {
	app, passes := backgroundTestApplication()
	gate := newStartupGate()
	gate.stop(errors.New("lease lost"))
	gate.onStop(func() { _ = app.Close() })
	started := app.startBackground(func() {
		app.startAgentAttention()
		app.startFeedbackAttention()
		startEager(app, passes)
	})
	if started {
		t.Fatal("background work started after the application was closed")
	}
	time.Sleep(50 * time.Millisecond)
	if n := passes.Load(); n != 0 {
		t.Fatalf("a closed application ran %d side-effect passes", n)
	}
	if code, status, _ := gateStatus(t, gate); code != 503 || status.Error != "api_stopping" {
		t.Fatalf("gate = %d %+v", code, status)
	}
}

// N-4: the lease is lost between two individual starts. Close waits until
// the starts are done, then stops every worker; nothing runs after Close
// returns and nothing starts afterwards.
func TestLeaseLostBetweenBackgroundStartsStopsEveryWorker(t *testing.T) {
	app, passes := backgroundTestApplication()
	gate := newStartupGate()
	gate.onStop(func() { _ = app.Close() })
	closed := make(chan struct{})
	started := app.startBackground(func() {
		app.startAgentAttention()
		go func() { gate.stop(errors.New("lease lost")); close(closed) }()
		select {
		case <-closed:
			t.Error("Close finished while workers were still being started")
		case <-time.After(100 * time.Millisecond):
		}
		app.startFeedbackAttention()
	})
	if !started {
		t.Fatal("start was refused before Close began")
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish after the starts")
	}
	after := passes.Load()
	time.Sleep(50 * time.Millisecond)
	if n := passes.Load(); n != after {
		t.Fatalf("workers ran %d passes after Close returned", n-after)
	}
	if app.startBackground(func() { app.startAgentAttention() }) {
		t.Fatal("a start after Close was accepted")
	}
	time.Sleep(50 * time.Millisecond)
	if n := passes.Load(); n != after {
		t.Fatalf("a start after Close ran %d passes", n-after)
	}
}
