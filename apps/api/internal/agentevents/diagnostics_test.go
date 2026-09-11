package agentevents

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestRuntimeDiagnosticObservationPreservesUnknownAndExcludesAuthority(t *testing.T) {
	g := openRuntimeGateway(t)
	const paid = "018f47a2-9b3c-7def-8abc-0123456789ab"
	ctx := context.Background()
	observation, err := g.DiagnosticObservation(ctx, paid)
	if err != nil || observation.Ready || observation.Generation != "" || observation.RunInFlight != nil {
		t.Fatal(observation, err)
	}
	receipt := "private-hydration-receipt"
	if err = g.PublishRuntimeState(paid, 7, &receipt); err != nil {
		t.Fatal(err)
	}
	observation, err = g.DiagnosticObservation(ctx, paid)
	if err != nil || !observation.Ready || observation.Generation != "7" || observation.ReadinessReason != "ready" {
		t.Fatal(observation, err)
	}
	raw, err := json.Marshal(observation)
	if err != nil || strings.Contains(string(raw), receipt) || strings.Contains(string(raw), "lease") {
		t.Fatal("authority material leaked")
	}
	if _, err = g.DiagnosticObservation(ctx, "../foreign"); err == nil {
		t.Fatal("invalid PA accepted")
	}
}
