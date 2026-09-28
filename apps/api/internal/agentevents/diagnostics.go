package agentevents

import (
	"context"
)

// RuntimeObservation reports the browser journal's locally observed run state.
type RuntimeObservation struct {
	Ready           bool   `json:"ready"`
	ReadinessReason string `json:"readiness_reason"`
	RunInFlight     *bool  `json:"run_in_flight,omitempty"`
}

func (g *BrowserJournal) DiagnosticObservation(ctx context.Context, personalityAgentID string) (RuntimeObservation, error) {
	var result RuntimeObservation
	if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
		return result, err
	}
	result.Ready = true
	result.ReadinessReason = "ready"
	// Do not replay an unbounded conversation just to report diagnostics. A
	// missing field honestly records that this process has not observed runs.
	if g.mu.TryLock() {
		g.stateMu.RLock()
		if st, ok := g.tails[personalityAgentID]; ok && st.tailObserved {
			running := g.runInFlight[personalityAgentID]
			result.RunInFlight = &running
		}
		g.stateMu.RUnlock()
		g.mu.Unlock()
	}
	return result, nil
}
