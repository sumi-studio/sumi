package main

import (
	"log"
	"os"
	"strings"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/processoperations"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
)

func processOperationsFromEnv(control *agentevents.LocalControlServer) (*processoperations.Server, error) {
	socket := strings.TrimSpace(os.Getenv("SUMI_RUNTIME_PROVISIONER_SOCKET"))
	if control == nil || socket == "" {
		return nil, nil
	}
	client, err := runtimeprovision.NewUnixClient(socket)
	if err != nil {
		return nil, err
	}
	s := &processoperations.Server{Backend: client}
	if err := s.RegisterLocalControlRoutes(control); err != nil {
		return nil, err
	}
	return s, nil
}

func (a *application) startProcessAttention() {
	if a.processOperations == nil || a.processOperations.Delivery == nil {
		return
	}
	a.attentionWorkers.Add(1)
	go func() {
		defer a.attentionWorkers.Done()
		for a.backgroundCtx.Err() == nil {
			if err := a.processOperations.DeliverPending(a.backgroundCtx); err != nil && a.backgroundCtx.Err() == nil {
				// Process output and source data must not leak into server logs.
				log.Print("process completion delivery failed; durable results will retry")
			}
			timer := time.NewTimer(time.Second)
			select {
			case <-a.backgroundCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
}
