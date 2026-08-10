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

	// An already-expired status is never reported as itself: expiry is resolved
	// at read time, so no sweeper can disagree with what readers see. Since
	// 0017 the lapse restores what the participant had said before (here the
	// away above) instead of erasing them — see
	// TestTemporaryStatusLapsesBackToWhatWasSaidBefore.
	past := time.Now().Add(-time.Minute)
	if _, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "会議中", &past); err != nil {
		t.Fatalf("set expiring status: %v", err)
	}
	for _, viewer := range []ParticipantRef{w.humanB, w.humanA} {
		statuses, err = w.store.StatusesVisibleTo(ctx, viewer)
		if err != nil {
			t.Fatalf("list statuses after expiry: %v", err)
		}
		got, _ := statusOf(t, statuses, w.humanA)
		if got.Status == StatusBusy || got.Note == "会議中" {
			t.Fatalf("expired status must not be reported, got %+v", statuses)
		}
	}

	// Only the three declared values exist; anything else fails closed.
	if _, err := w.store.SetStatus(ctx, w.humanA, "invisible", "", nil); err == nil {
		t.Fatal("unknown status must be rejected")
	}
}

func TestStatusIsVisibleOnlyThroughASharedWorkspaceOrPlace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	strangerID, err := koseki.New(w.store.pool).MintHuman(ctx)
	if err != nil {
		t.Fatalf("mint stranger: %v", err)
	}
	stranger := Human(strangerID)

	if _, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "", nil); err != nil {
		t.Fatalf("set status: %v", err)
	}

	// Before any shared membership nobody but the owner sees it.
	visible, err := w.store.ParticipantVisible(ctx, w.humanB, w.humanA)
	if err != nil {
		t.Fatalf("visibility before workspace: %v", err)
	}
	if visible {
		t.Fatal("an unrelated participant must not see a self-declared status")
	}
	own, err := w.store.ParticipantVisible(ctx, w.humanA, w.humanA)
	if err != nil || !own {
		t.Fatalf("own visibility = %v (%v)", own, err)
	}

	w.workspaceWithChannel(t, ctx)
	visible, err = w.store.ParticipantVisible(ctx, w.humanB, w.humanA)
	if err != nil || !visible {
		t.Fatalf("shared workspace visibility = %v (%v)", visible, err)
	}

	// Someone in no shared workspace and no shared place still sees nothing.
	statuses, err := w.store.StatusesVisibleTo(ctx, stranger)
	if err != nil {
		t.Fatalf("stranger statuses: %v", err)
	}
	if _, ok := statusOf(t, statuses, w.humanA); ok {
		t.Fatalf("stranger must see no status, got %+v", statuses)
	}
	// bootstrap and the live fan-out share this basis, so they cannot disagree.
	for _, viewer := range []ParticipantRef{w.humanB, w.agent} {
		statuses, err := w.store.StatusesVisibleTo(ctx, viewer)
		if err != nil {
			t.Fatalf("statuses for %s: %v", viewer.Key(), err)
		}
		if _, ok := statusOf(t, statuses, w.humanA); !ok {
			t.Fatalf("%s should see the status, got %+v", viewer.Key(), statuses)
		}
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
	server := NewServer(w.store, nil)
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

	// The human sees the agent's status through the same store the UI reads.
	if err := w.store.EnsureDefaultWorkspaceMembership(ctx, w.humanA); err != nil {
		t.Fatalf("admit human: %v", err)
	}
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

// A temporary status lapses back to what the participant had already said —
// the platform never quietly replaces someone's own words with a default.
func TestTemporaryStatusLapsesBackToWhatWasSaidBefore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)

	// A lasting state, then a temporary one on top of it.
	if _, err := w.store.SetStatus(ctx, w.humanA, StatusAway, "外出中", nil); err != nil {
		t.Fatalf("set lasting status: %v", err)
	}
	soon := time.Now().Add(time.Hour)
	temporary, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "会議中", &soon)
	if err != nil {
		t.Fatalf("set temporary status: %v", err)
	}
	if temporary.BaseStatus != StatusAway || temporary.BaseNote != "外出中" {
		t.Fatalf("temporary = %+v, want the lasting state remembered", temporary)
	}
	statuses, err := w.store.StatusesVisibleTo(ctx, w.humanB)
	if err != nil {
		t.Fatalf("list statuses: %v", err)
	}
	got, _ := statusOf(t, statuses, w.humanA)
	if got.Status != StatusBusy || got.ExpiresAt == nil || got.BaseStatus != StatusAway {
		t.Fatalf("visible while it holds = %+v", got)
	}

	// Once it has lapsed, readers see the earlier state — before any sweep.
	past := time.Now().Add(-time.Minute)
	if _, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "会議中", &past); err != nil {
		t.Fatalf("set lapsed status: %v", err)
	}
	statuses, err = w.store.StatusesVisibleTo(ctx, w.humanB)
	if err != nil {
		t.Fatalf("list statuses after lapse: %v", err)
	}
	got, ok := statusOf(t, statuses, w.humanA)
	if !ok || got.Status != StatusAway || got.Note != "外出中" || got.ExpiresAt != nil {
		t.Fatalf("visible after lapse = %+v (found %v), want the earlier state", got, ok)
	}

	// The sweep makes that same answer durable and reports what changed, so
	// the socket can announce it. It never disagrees with the read-time answer.
	expired, err := w.store.ExpireStatuses(ctx)
	if err != nil {
		t.Fatalf("expire statuses: %v", err)
	}
	if len(expired) != 1 || expired[0].Participant != w.humanA ||
		expired[0].Status != StatusAway || expired[0].Note != "外出中" {
		t.Fatalf("expired = %+v", expired)
	}
	statuses, err = w.store.StatusesVisibleTo(ctx, w.humanB)
	if err != nil {
		t.Fatalf("list statuses after sweep: %v", err)
	}
	got, _ = statusOf(t, statuses, w.humanA)
	if got.Status != StatusAway || got.ExpiresAt != nil || got.BaseStatus != "" {
		t.Fatalf("after sweep = %+v, want a plain lasting status", got)
	}
	// A second sweep has nothing to do: the lapse already happened.
	if again, err := w.store.ExpireStatuses(ctx); err != nil || len(again) != 0 {
		t.Fatalf("second sweep = %+v, %v", again, err)
	}
}

// A temporary status with nothing behind it ends the declaration rather than
// leaving a stale one: saying nothing is not the same as saying「対応可能」.
func TestTemporaryStatusWithNoEarlierStateClearsOnLapse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)

	past := time.Now().Add(-time.Minute)
	declared, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "集中中", &past)
	if err != nil {
		t.Fatalf("set temporary status: %v", err)
	}
	if declared.BaseStatus != "" {
		t.Fatalf("declared = %+v, want no earlier state to return to", declared)
	}
	statuses, err := w.store.StatusesVisibleTo(ctx, w.humanB)
	if err != nil {
		t.Fatalf("list statuses: %v", err)
	}
	if _, ok := statusOf(t, statuses, w.humanA); ok {
		t.Fatalf("statuses = %+v, want the lapsed declaration not reported", statuses)
	}

	expired, err := w.store.ExpireStatuses(ctx)
	if err != nil {
		t.Fatalf("expire statuses: %v", err)
	}
	if len(expired) != 1 || expired[0].Status != "" {
		t.Fatalf("expired = %+v, want one cleared declaration", expired)
	}
	statuses, err = w.store.StatusesVisibleTo(ctx, w.humanB)
	if err != nil {
		t.Fatalf("list statuses after sweep: %v", err)
	}
	if len(statuses) != 0 {
		t.Fatalf("statuses after sweep = %+v, want none", statuses)
	}
}

// Stacking two temporary statuses keeps the lasting one as the base: the state
// the participant actually chose to hold cannot be buried by two short ones.
func TestStackedTemporaryStatusesKeepTheLastingBase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)

	if _, err := w.store.SetStatus(ctx, w.humanA, StatusAway, "外出中", nil); err != nil {
		t.Fatalf("set lasting status: %v", err)
	}
	hour := time.Now().Add(time.Hour)
	if _, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "会議中", &hour); err != nil {
		t.Fatalf("set first temporary status: %v", err)
	}
	second, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "別件", &hour)
	if err != nil {
		t.Fatalf("set second temporary status: %v", err)
	}
	if second.BaseStatus != StatusAway || second.BaseNote != "外出中" {
		t.Fatalf("second = %+v, want the lasting state still underneath", second)
	}

	// Declaring a lasting state again forgets the base: the new words are the
	// whole truth.
	lasting, err := w.store.SetStatus(ctx, w.humanA, StatusAvailable, "", nil)
	if err != nil {
		t.Fatalf("set lasting status again: %v", err)
	}
	if lasting.BaseStatus != "" || lasting.ExpiresAt != nil {
		t.Fatalf("lasting = %+v, want nothing pending behind it", lasting)
	}
}
