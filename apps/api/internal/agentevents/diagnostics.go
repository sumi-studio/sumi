package agentevents

import (
	"context"
	"strconv"
	"syscall"
)

// RuntimeObservation contains only operational state. It excludes runtime
// credentials, lease handles, hydration identities and conversation content.
// Fields are observations, not evidence granting any runtime authority.
type RuntimeObservation struct {
	Generation      string `json:"generation,omitempty"`
	Ready           bool   `json:"ready"`
	ReadinessReason string `json:"readiness_reason"`
	RunInFlight     *bool  `json:"run_in_flight,omitempty"`
}

func (g *DurableGateway) DiagnosticObservation(ctx context.Context, personalityAgentID string) (RuntimeObservation, error) {
	var result RuntimeObservation
	if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
		return result, err
	}
	lock, err := g.openRuntimeLock(personalityAgentID)
	if err != nil {
		return result, err
	}
	defer lock.Close()
	if err = flockContext(ctx, lock.Fd(), syscall.LOCK_SH); err != nil {
		return result, err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	state, err := g.state(ctx, personalityAgentID)
	if err != nil {
		return result, err
	}
	// Release the runtime lease before acquiring the gateway event mutex.
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); err != nil {
		return result, err
	}
	if state.present {
		result.Generation = strconv.FormatUint(state.Generation, 10)
	}
	result.Ready = state.present && state.HydrationReceiptIdentity != nil
	result.ReadinessReason = "unknown"
	if result.Ready {
		result.ReadinessReason = "ready"
	} else if state.LocalControl != nil {
		switch state.LocalControl.Reason {
		case LocalRuntimeStartup:
			result.ReadinessReason = "rehydrating"
		case LocalRuntimeShutdown:
			result.ReadinessReason = "stopped"
		}
	}
	// Do not replay an unbounded conversation just to report diagnostics. A
	// missing field honestly records that this process has not observed runs.
	if g.mu.TryLock() {
		g.stateMu.RLock()
		if g.stateRebuilt[personalityAgentID] {
			running := g.runInFlight[personalityAgentID]
			result.RunInFlight = &running
		}
		g.stateMu.RUnlock()
		g.mu.Unlock()
	}
	return result, nil
}
