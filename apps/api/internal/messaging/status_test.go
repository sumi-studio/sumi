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

	// An expired temporary status is resolved at read time, so no sweeper can
	// disagree with what readers see. With a lasting declaration underneath,
	// what is reported is that declaration — not「対応可能」and not nothing.
	past := time.Now().Add(-time.Minute)
	if _, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "会議中", &past); err != nil {
		t.Fatalf("set expiring status: %v", err)
	}
	statuses, err = w.store.StatusesVisibleTo(ctx, w.humanB)
	if err != nil {
		t.Fatalf("list statuses after expiry: %v", err)
	}
	got, ok = statusOf(t, statuses, w.humanA)
	if !ok || got.Status != StatusAway || got.Note != "" || got.ExpiresAt != nil {
		t.Fatalf("lapsed status = %+v (found %v), want the lasting away", got, ok)
	}

	// With nothing behind it, the lapse ends the declaration outright.
	if _, err := w.store.SetStatus(ctx, w.humanB, StatusBusy, "会議中", &past); err != nil {
		t.Fatalf("set expiring status without a base: %v", err)
	}
	statuses, err = w.store.StatusesVisibleTo(ctx, w.humanA)
	if err != nil {
		t.Fatalf("list own statuses: %v", err)
	}
	if _, ok := statusOf(t, statuses, w.humanB); ok {
		t.Fatalf("a lapsed status with no base must not be reported, got %+v", statuses)
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

// A timed status is a promise about a return, not just about an end: it has to
// say what it returns to, decide that at declaration time, and hold that answer
// even if another timed status is stacked on top of it.
func TestTimedStatusLapsesBackToTheDeclarationUnderneath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)

	if _, err := w.store.SetStatus(ctx, w.humanA, StatusAway, "在宅です", nil); err != nil {
		t.Fatalf("set lasting status: %v", err)
	}
	soon := time.Now().Add(time.Hour)
	declared, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "会議中", &soon)
	if err != nil {
		t.Fatalf("set timed status: %v", err)
	}
	// The declaration itself already names the return, so a screen can say
	// 「期限が来たら『離席中』に戻ります」before the hour is up.
	if declared.BaseStatus != StatusAway || declared.BaseNote != "在宅です" {
		t.Fatalf("timed status = %+v, want the lasting away recorded as its base", declared)
	}

	// Stacking a second timed status keeps the lasting declaration underneath.
	stacked, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "電話中", &soon)
	if err != nil {
		t.Fatalf("stack timed status: %v", err)
	}
	if stacked.BaseStatus != StatusAway || stacked.BaseNote != "在宅です" {
		t.Fatalf("stacked status = %+v, want the base to survive", stacked)
	}

	// A lasting declaration has nothing to return to.
	lasting, err := w.store.SetStatus(ctx, w.humanA, StatusAvailable, "", nil)
	if err != nil {
		t.Fatalf("set lasting status: %v", err)
	}
	if lasting.BaseStatus != "" || lasting.ExpiresAt != nil {
		t.Fatalf("lasting status = %+v, want no base", lasting)
	}
}

// The sweep only makes the read-time answer durable and announces it. It must
// not invent a state nobody declared, and it must have somewhere to announce to.
func TestExpireStatusesRestoresTheBaseAndClearsWhatHasNone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, _ := w.workspaceWithChannel(t, ctx)

	past := time.Now().Add(-time.Minute)
	if _, err := w.store.SetStatus(ctx, w.humanA, StatusAway, "在宅です", nil); err != nil {
		t.Fatalf("set lasting status: %v", err)
	}
	if _, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "会議中", &past); err != nil {
		t.Fatalf("set lapsed status with a base: %v", err)
	}
	if _, err := w.store.SetStatus(ctx, w.humanB, StatusBusy, "会議中", &past); err != nil {
		t.Fatalf("set lapsed status without a base: %v", err)
	}

	expiries, err := w.store.core.ExpireStatuses(ctx)
	if err != nil {
		t.Fatalf("expire statuses: %v", err)
	}
	if len(expiries) != 2 {
		t.Fatalf("expiries = %d, want both lapsed rows", len(expiries))
	}
	for _, expiry := range expiries {
		switch expiry.Status.Participant {
		case w.humanA:
			if expiry.Status.Status != StatusAway || expiry.Status.Note != "在宅です" {
				t.Fatalf("restored status = %+v", expiry.Status)
			}
		case w.humanB:
			// An empty status is how the sweep says「もう何も言っていない」.
			if expiry.Status.Status != "" {
				t.Fatalf("cleared status = %+v", expiry.Status)
			}
		default:
			t.Fatalf("unexpected expiry subject %+v", expiry.Status.Participant)
		}
		// The announcement has an address to go to, and it is the Messaging
		// installation of a Workspace they are actually in.
		if len(expiry.Scopes) != 1 || expiry.Scopes[0].WorkspaceID != workspace.WorkspaceID {
			t.Fatalf("expiry scopes = %+v", expiry.Scopes)
		}
	}

	// Sweeping is idempotent: the second pass finds nothing left to lapse.
	again, err := w.store.core.ExpireStatuses(ctx)
	if err != nil {
		t.Fatalf("second expire: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("second sweep = %+v, want nothing", again)
	}

	// And the durable rows now agree with what readers were already told.
	statuses, err := w.store.StatusesVisibleTo(ctx, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := statusOf(t, statuses, w.humanA); !ok || got.Status != StatusAway || got.ExpiresAt != nil {
		t.Fatalf("status after sweep = %+v (found %v)", got, ok)
	}
	if _, ok := statusOf(t, statuses, w.humanB); ok {
		t.Fatalf("cleared status must be gone, got %+v", statuses)
	}
}
