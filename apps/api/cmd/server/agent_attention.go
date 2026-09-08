package main

import (
	"context"
	"log"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/messaging"
)

// Polling owns delivery independently of browser connections. The outbox keeps
// per-source retry deadlines, receipts and cancellation decisions durably.
func runAgentAttention(ctx context.Context, deliver func(context.Context) (messaging.AgentAttentionDeliveryStats, error), interval time.Duration) {
	for {
		if ctx.Err() != nil {
			return
		}
		_, err := deliver(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			// Delivery errors can include source data; keep process logs generic.
			log.Print("messaging attention: delivery batch failed; pending events will retry")
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (a *application) startAgentAttention() {
	if a.deliverAttention == nil {
		return
	}
	a.attentionWorkers.Add(1)
	go func() {
		defer a.attentionWorkers.Done()
		runAgentAttention(a.backgroundCtx, a.deliverAttention, time.Second)
	}()
}
