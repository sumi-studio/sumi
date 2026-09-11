package main

import (
	"log"
	"time"
)

func (a *application) startFeedbackAttention() {
	if a.deliverFeedbackAttention == nil {
		return
	}
	a.attentionWorkers.Add(1)
	go func() {
		defer a.attentionWorkers.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			if a.backgroundCtx.Err() != nil {
				return
			}
			err := a.deliverFeedbackAttention(a.backgroundCtx)
			if err != nil && a.backgroundCtx.Err() == nil {
				log.Print("feedback attention: delivery batch failed; pending events will retry")
			}
			select {
			case <-a.backgroundCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}
