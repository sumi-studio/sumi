package messaging

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

func statusOf(t *testing.T, statuses []ParticipantStatus, who ParticipantRef) (ParticipantStatus, bool) {
	t.Helper()
	for _, status := range statuses {
		if status.Participant == who {
			return status, true
		}
	}
	return ParticipantStatus{}, false
}

func TestStatusIsReplacedInPlaceAndExpiresAtReadTime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)

	if _, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "取り込み中", nil); err != nil {
		t.Fatalf("set status: %v", err)
	}
	statuses, err := w.store.StatusesVisibleTo(ctx, w.humanB)
	if err != nil {
		t.Fatalf("list statuses: %v", err)
	}
	got, ok := statusOf(t, statuses, w.humanA)
	if !ok || got.Status != StatusBusy || got.Note != "取り込み中" || got.ExpiresAt != nil {
		t.Fatalf("status = %+v (found %v)", got, ok)
	}

	// Setting again replaces the one row rather than accumulating history.
	if _, err := w.store.SetStatus(ctx, w.humanA, StatusAway, "", nil); err != nil {
		t.Fatalf("replace status: %v", err)
	}
	statuses, err = w.store.StatusesVisibleTo(ctx, w.humanB)
	if err != nil {
		t.Fatalf("list statuses after replace: %v", err)
	}
	got, _ = statusOf(t, statuses, w.humanA)
	if got.Status != StatusAway || got.Note != "" {
		t.Fatalf("replaced status = %+v", got)
	}
	if n := len(statuses); n != 1 {
		t.Fatalf("statuses = %d, want exactly one row per participant", n)
	}

	// An already-expired status is simply not reported: expiry is a read-time
	// filter, so no sweeper can disagree with what readers see.
	past := time.Now().Add(-time.Minute)
	if _, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "会議中", &past); err != nil {
		t.Fatalf("set expiring status: %v", err)
	}
	statuses, err = w.store.StatusesVisibleTo(ctx, w.humanB)
	if err != nil {
		t.Fatalf("list statuses after expiry: %v", err)
	}
	if _, ok := statusOf(t, statuses, w.humanA); ok {
		t.Fatalf("expired status must not be reported, got %+v", statuses)
	}
	// The owner does not get to see their own expired status either.
	statuses, err = w.store.StatusesVisibleTo(ctx, w.humanA)
	if err != nil {
		t.Fatalf("list own statuses: %v", err)
	}
	if _, ok := statusOf(t, statuses, w.humanA); ok {
		t.Fatalf("expired own status must not be reported, got %+v", statuses)
	}

	// Only the three declared values exist; anything else fails closed.
	if _, err := w.store.SetStatus(ctx, w.humanA, "invisible", "", nil); err == nil {
		t.Fatal("unknown status must be rejected")
	}
}

func TestStatusVisibilityIsBoundToExactWorkspace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)
	if _, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "", nil); err != nil {
		t.Fatalf("set status: %v", err)
	}
	statuses, err := w.store.StatusesVisibleTo(ctx, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := statusOf(t, statuses, w.humanA); !ok {
		t.Fatalf("shared exact Workspace did not expose status: %+v", statuses)
	}
	strangerID, err := koseki.New(w.store.core.pool).MintHuman(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stranger := Human(strangerID)
	if _, err := w.store.createWorkspace(ctx, "isolated", stranger); err != nil {
		t.Fatal(err)
	}
	statuses, err = w.store.StatusesVisibleTo(ctx, stranger)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := statusOf(t, statuses, w.humanA); ok {
		t.Fatalf("status leaked across exact Workspace: %+v", statuses)
	}
}

func TestStatusOverHTTPPublishesToParticipantScopedSubscribers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newWSWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)

	conn := dialWS(t, ts, w.humanB.ID, nil)
	resp, body := call(t, ts, http.MethodPut, "/messaging/status", w.humanA.ID,
		map[string]any{"status": "busy", "note": "取り込み中"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("set status: status %d body %v", resp.StatusCode, body)
	}
	if body["status"] != "busy" || body["note"] != "取り込み中" || body["expires_at"] != nil {
		t.Fatalf("status body = %v", body)
	}

	// status_updated carries no place: it is scoped to the participant, and the
	// subscriber receives it because they share a workspace.
	frame := readFrame(t, conn)
	event := frame["event"].(map[string]any)
	if event["type"] != EventStatusUpdated {
		t.Fatalf("event = %v", event)
	}
	if _, hasPlace := event["place_id"]; hasPlace {
		t.Fatalf("status event must not claim a place: %v", event)
	}
	status := event["status"].(map[string]any)
	if status["status"] != "busy" ||
		status["participant"].(map[string]any)["human_id"] != w.humanA.ID {
		t.Fatalf("status event payload = %v", status)
	}

	// Bootstrap reports the current value, since the event never replays.
	resp, body = call(t, ts, http.MethodGet, "/messaging/bootstrap", w.humanB.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bootstrap: status %d", resp.StatusCode)
	}
	statuses := body["statuses"].([]any)
	if len(statuses) != 1 || statuses[0].(map[string]any)["status"] != "busy" {
		t.Fatalf("bootstrap statuses = %v", statuses)
	}

	// Nobody sets anybody else's: the value comes from the session only, and a
	// value outside the vocabulary is rejected before the store.
	resp, _ = call(t, ts, http.MethodPut, "/messaging/status", w.humanA.ID,
		map[string]any{"status": "invisible"})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown status: status %d, want 400", resp.StatusCode)
	}
	resp, _ = call(t, ts, http.MethodPut, "/messaging/status", w.humanA.ID,
		map[string]any{"status": "busy", "participant": map[string]any{"kind": "human", "human_id": w.humanB.ID}})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("participant field: status %d, want 400", resp.StatusCode)
	}
}

func TestLocalStatusSetsTheAgentsOwnAttentionState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)
	server := NewServer(w.store.core, nil)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}

	status, body := callLocal(t, ctx, server.localStatus, LocalStatusPath, map[string]any{
		"status": "busy", "note": "別の対応中です", "expires_in_minutes": 45,
	}, authorization)
	if status != http.StatusOK {
		t.Fatalf("agent status: status %d body %v", status, body)
	}
	declared := body["status"].(map[string]any)
	participant := declared["participant"].(map[string]any)
	if participant["kind"] != "personality_agent" || participant["personality_agent_id"] != w.agent.ID {
		t.Fatalf("agent status participant = %v", participant)
	}
	if declared["status"] != "busy" || declared["note"] != "別の対応中です" {
		t.Fatalf("agent status = %v", declared)
	}
	if declared["expires_at"] == nil {
		t.Fatalf("a relative expiry must resolve to an instant: %v", declared)
	}

	// Explicit membership, not the Human-Agent relation, makes the status
	// visible through the same store the UI reads.
	w.workspaceWithChannel(t, ctx)
	statuses, err := w.store.StatusesVisibleTo(ctx, w.humanA)
	if err != nil {
		t.Fatalf("list statuses: %v", err)
	}
	if got, ok := statusOf(t, statuses, w.agent); !ok || got.Status != StatusBusy {
		t.Fatalf("human view of the agent status = %+v (found %v)", got, ok)
	}

	// Same vocabulary, same bounds as the human lane.
	status, _ = callLocal(t, ctx, server.localStatus, LocalStatusPath, map[string]any{
		"status": "invisible",
	}, authorization)
	if status != http.StatusBadRequest {
		t.Fatalf("unknown agent status: %d, want 400", status)
	}
}
