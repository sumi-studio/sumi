package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/messaging"
)

func TestAgentAttentionRetriesBatchFailureAndCancelsInFlight(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan int, 3)
	finished := make(chan struct{})
	calls := 0
	go func() {
		defer close(finished)
		runAgentAttention(ctx, func(ctx context.Context) (messaging.AgentAttentionDeliveryStats, error) {
			calls++
			entered <- calls
			if calls == 1 {
				return messaging.AgentAttentionDeliveryStats{}, errors.New("temporary failure")
			}
			<-ctx.Done()
			return messaging.AgentAttentionDeliveryStats{}, ctx.Err()
		}, time.Millisecond)
	}()
	for expected := 1; expected <= 2; expected++ {
		select {
		case got := <-entered:
			if got != expected {
				t.Fatalf("call %d, want %d", got, expected)
			}
		case <-time.After(time.Second):
			t.Fatal("delivery did not run/retry")
		}
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("delivery ignored cancellation")
	}
	if calls != 2 {
		t.Fatalf("unexpected extra delivery: %d", calls)
	}
}

func TestApplicationCloseJoinsAttentionWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	released := make(chan struct{})
	app := &application{backgroundCtx: ctx, stopBackground: cancel,
		deliverAttention: func(ctx context.Context) (messaging.AgentAttentionDeliveryStats, error) {
			close(started)
			<-ctx.Done()
			close(released)
			return messaging.AgentAttentionDeliveryStats{}, ctx.Err()
		},
	}
	app.startAgentAttention()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	if err := app.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-released:
	default:
		t.Fatal("Close returned before delivery released resources")
	}
	if err := app.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestUnconfiguredAttentionDoesNotStartWorker(t *testing.T) {
	app := &application{}
	app.startAgentAttention()
	if err := app.Close(); err != nil {
		t.Fatal(err)
	}
}
