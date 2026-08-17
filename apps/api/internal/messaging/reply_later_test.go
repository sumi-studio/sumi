package messaging

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

func TestReplyLaterIsIdempotentAndOnlyItsOwnerResolvesIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "あとで読みます")
	remindAt := time.Now().Add(30 * time.Minute)

	marker, created, err := w.store.CreateReplyLater(ctx, ch.PlaceID, msg.MessageID, w.humanB, "", remindAt)
	if err != nil {
		t.Fatalf("create marker: %v", err)
	}
	if !created || marker.Note != DefaultReplyLaterNote || marker.Participant != w.humanB {
		t.Fatalf("created marker = %+v (created %v)", marker, created)
	}

	// Tapping again is the same promise, not a second one.
	again, created, err := w.store.CreateReplyLater(ctx, ch.PlaceID, msg.MessageID, w.humanB, "別の言葉", remindAt.Add(time.Hour))
	if err != nil {
		t.Fatalf("repeat marker: %v", err)
	}
	if created || again.MarkerID != marker.MarkerID || again.Note != DefaultReplyLaterNote {
		t.Fatalf("repeat marker = %+v (created %v)", again, created)
	}

	// A second participant's promise on the same message is its own marker.
	other, created, err := w.store.CreateReplyLater(ctx, ch.PlaceID, msg.MessageID, w.agent, "対応します", remindAt)
	if err != nil || !created || other.MarkerID == marker.MarkerID {
		t.Fatalf("agent marker = %+v (created %v, err %v)", other, created, err)
	}

	// Resolving is 本人のみ; anyone else's attempt is indistinguishable from a
	// marker that does not exist.
	if _, err := w.store.ResolveReplyLater(ctx, marker.MarkerID, w.humanA); !errors.Is(err, ErrMarkerNotFound) {
		t.Fatalf("foreign resolve = %v, want ErrMarkerNotFound", err)
	}
	if _, err := w.store.ResolveReplyLater(ctx, newUUIDv7(), w.humanB); !errors.Is(err, ErrMarkerNotFound) {
		t.Fatalf("unknown resolve = %v, want ErrMarkerNotFound", err)
	}
	resolved, err := w.store.ResolveReplyLater(ctx, marker.MarkerID, w.humanB)
	if err != nil || !resolved.Resolved || resolved.PlaceID != ch.PlaceID {
		t.Fatalf("resolve = %+v (%v)", resolved, err)
	}
	// Resolving twice is idempotent, and the kept promise leaves the open list.
	if _, err := w.store.ResolveReplyLater(ctx, marker.MarkerID, w.humanB); err != nil {
		t.Fatalf("repeat resolve: %v", err)
	}
	open, err := w.store.ReplyLaterMarkersFor(ctx, w.humanA)
	if err != nil {
		t.Fatalf("list markers: %v", err)
	}
	if len(open) != 1 || open[0].MarkerID != other.MarkerID {
		t.Fatalf("open markers = %+v", open)
	}

	// The slot freed by resolving accepts a fresh promise on the same message.
	fresh, created, err := w.store.CreateReplyLater(ctx, ch.PlaceID, msg.MessageID, w.humanB, "", remindAt)
	if err != nil || !created || fresh.MarkerID == marker.MarkerID {
		t.Fatalf("post-resolve marker = %+v (created %v, err %v)", fresh, created, err)
	}
}

func TestReplyLaterRefusesInvisiblePlacesAndTombstones(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "消えます")
	remindAt := time.Now().Add(time.Hour)

	// A place the actor cannot see is reported as missing, never as forbidden.
	dm, _, err := w.store.EnsureDM(ctx, w.humanA, w.agent)
	if err != nil {
		t.Fatalf("ensure dm: %v", err)
	}
	private := w.send(t, ctx, dm.PlaceID, w.humanA, "二人だけの話")
	if _, _, err := w.store.CreateReplyLater(ctx, dm.PlaceID, private.MessageID, w.humanB, "", remindAt); !errors.Is(err, ErrPlaceNotFound) {
		t.Fatalf("invisible place = %v, want ErrPlaceNotFound", err)
	}
	if _, _, err := w.store.CreateReplyLater(ctx, ch.PlaceID, newUUIDv7(), w.humanB, "", remindAt); !errors.Is(err, ErrMessageNotFound) {
		t.Fatalf("unknown message = %v, want ErrMessageNotFound", err)
	}
	if _, _, err := w.store.CreateReplyLater(ctx, ch.PlaceID, msg.MessageID, w.humanB, "", time.Time{}); err == nil {
		t.Fatal("a promise without a reminder time must be rejected")
	}

	if _, err := w.store.DeleteMessage(ctx, ch.PlaceID, msg.MessageID, w.humanA); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, _, err := w.store.CreateReplyLater(ctx, ch.PlaceID, msg.MessageID, w.humanB, "", remindAt); !errors.Is(err, ErrMessageDeleted) {
		t.Fatalf("tombstone = %v, want ErrMessageDeleted", err)
	}
}

func TestThreadReplyLaterSurvivesBootstrapForNonparticipant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)

	owner := w.store.mustScope(t, ctx, DefaultWorkspaceID, w.humanA)
	thread, _, err := owner.CreateThread(ctx, DefaultGeneralChannelID, "あとで返信", "", "thread-reply-later-1")
	if err != nil {
		t.Fatalf("create thread: %v", err)
	}
	message := w.send(t, ctx, thread.Place.PlaceID, w.humanA, "この枝をあとで見ます")

	viewer := w.store.mustScope(t, ctx, DefaultWorkspaceID, w.humanB)
	if _, err := viewer.ThreadFor(ctx, thread.Place.PlaceID); err != nil {
		t.Fatalf("open thread as workspace member: %v", err)
	}
	if threads, err := viewer.ThreadsFor(ctx); err != nil || len(threads) != 0 {
		t.Fatalf("opened thread made viewer a participant: threads=%+v err=%v", threads, err)
	}

	path := "/messaging/places/" + thread.Place.PlaceID + "/messages/" + message.MessageID + "/reply-later"
	resp, body := call(t, ts, http.MethodPost, path, w.humanB.ID, map[string]any{
		"remind_at": time.Now().Add(30 * time.Minute).UTC().Format(time.RFC3339Nano),
	})
	if resp.StatusCode != http.StatusCreated || body["created"] != true {
		t.Fatalf("create thread reply-later: status %d body %v", resp.StatusCode, body)
	}
	markerID := body["marker"].(map[string]any)["marker_id"]

	resp, body = call(t, ts, http.MethodGet, "/messaging/bootstrap", w.humanB.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bootstrap after thread reply-later: status %d body %v", resp.StatusCode, body)
	}
	markers := body["reply_later_markers"].([]any)
	if len(markers) != 1 || markers[0].(map[string]any)["marker_id"] != markerID {
		t.Fatalf("bootstrap markers = %v, want durable thread marker %v", markers, markerID)
	}
	place := markers[0].(map[string]any)["place"].(map[string]any)
	if place["thread_id"] != thread.Place.PlaceID {
		t.Fatalf("bootstrap marker place = %v, want thread %s", place, thread.Place.PlaceID)
	}

	for _, raw := range body["unread_summaries"].([]any) {
		summary := raw.(map[string]any)
		if summary["place"].(map[string]any)["thread_id"] == thread.Place.PlaceID {
			if summary["unread_count"] != float64(1) || summary["mention_count"] != float64(0) {
				t.Fatalf("thread unread summary = %v", summary)
			}
			return
		}
	}
	t.Fatalf("bootstrap unread summaries omitted visible thread: %v", body["unread_summaries"])
}

func TestReplyLaterRemindAtNeverLeavesTheOwnersWire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newWSWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	msg := w.send(t, ctx, ch.PlaceID, w.humanB, "至急の相談")

	owner := dialWS(t, ts, w.humanA.ID, nil)
	bystander := dialWS(t, ts, w.humanB.ID, nil)

	path := "/messaging/places/" + ch.PlaceID + "/messages/" + msg.MessageID + "/reply-later"
	remindAt := time.Now().Add(30 * time.Minute).UTC().Format(time.RFC3339Nano)
	resp, body := call(t, ts, http.MethodPost, path, w.humanA.ID, map[string]any{"remind_at": remindAt})
	if resp.StatusCode != http.StatusCreated || body["created"] != true {
		t.Fatalf("create marker: status %d body %v", resp.StatusCode, body)
	}
	created := body["marker"].(map[string]any)
	if created["remind_at"] == nil {
		t.Fatalf("the owner's own response must carry remind_at: %v", created)
	}
	if created["note"] != DefaultReplyLaterNote || created["resolved"] != false {
		t.Fatalf("marker = %v", created)
	}
	markerID := created["marker_id"].(string)

	// The owner's live copy keeps the private schedule…
	ownerEvent := readFrame(t, owner)["event"].(map[string]any)
	if ownerEvent["type"] != EventReplyLaterCreated {
		t.Fatalf("owner event = %v", ownerEvent)
	}
	if ownerEvent["marker"].(map[string]any)["remind_at"] == nil {
		t.Fatalf("owner event lost remind_at: %v", ownerEvent)
	}
	// …and everyone else learns the promise without the time behind it.
	otherEvent := readFrame(t, bystander)["event"].(map[string]any)
	otherMarker := otherEvent["marker"].(map[string]any)
	if otherEvent["type"] != EventReplyLaterCreated || otherMarker["marker_id"] != markerID {
		t.Fatalf("bystander event = %v", otherEvent)
	}
	if _, leaked := otherMarker["remind_at"]; leaked {
		t.Fatalf("remind_at must not ride another participant's wire: %v", otherMarker)
	}
	if otherMarker["note"] != DefaultReplyLaterNote {
		t.Fatalf("the promise itself is public: %v", otherMarker)
	}

	// The same split holds in bootstrap, the other place a marker is serialized.
	resp, body = call(t, ts, http.MethodGet, "/messaging/bootstrap", w.humanB.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bystander bootstrap: status %d", resp.StatusCode)
	}
	markers := body["reply_later_markers"].([]any)
	if len(markers) != 1 {
		t.Fatalf("bystander markers = %v", markers)
	}
	if _, leaked := markers[0].(map[string]any)["remind_at"]; leaked {
		t.Fatalf("bootstrap leaked remind_at: %v", markers[0])
	}
	resp, body = call(t, ts, http.MethodGet, "/messaging/bootstrap", w.humanA.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("owner bootstrap: status %d", resp.StatusCode)
	}
	markers = body["reply_later_markers"].([]any)
	if len(markers) != 1 || markers[0].(map[string]any)["remind_at"] == nil {
		t.Fatalf("owner bootstrap markers = %v", markers)
	}

	// A repeat tap is the same promise: 200 instead of 201, no second event.
	resp, body = call(t, ts, http.MethodPost, path, w.humanA.ID, map[string]any{"remind_at": remindAt})
	if resp.StatusCode != http.StatusOK || body["created"] != false {
		t.Fatalf("repeat tap: status %d body %v", resp.StatusCode, body)
	}

	// Resolving travels as an identifier only — nothing private to restate.
	resp, body = call(t, ts, http.MethodPost, "/messaging/reply-later/"+markerID+"/resolve", w.humanA.ID, map[string]any{})
	if resp.StatusCode != http.StatusOK || body["marker"].(map[string]any)["resolved"] != true {
		t.Fatalf("resolve: status %d body %v", resp.StatusCode, body)
	}
	resolvedEvent := readFrame(t, bystander)["event"].(map[string]any)
	if resolvedEvent["type"] != EventReplyLaterResolved || resolvedEvent["marker_id"] != markerID {
		t.Fatalf("resolved event = %v", resolvedEvent)
	}
	if _, hasMarker := resolvedEvent["marker"]; hasMarker {
		t.Fatalf("resolved event must carry only the identifier: %v", resolvedEvent)
	}

	// Someone else's marker is 404, not 403.
	resp, _ = call(t, ts, http.MethodPost, "/messaging/reply-later/"+markerID+"/resolve", w.humanB.ID, map[string]any{})
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("foreign resolve: status %d, want 404", resp.StatusCode)
	}
	resp, _ = call(t, ts, http.MethodPost, path, w.humanA.ID, map[string]any{})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing remind_at: status %d, want 400", resp.StatusCode)
	}
}

func TestLocalReplyLaterAndResolveUseTheSharedStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	server := NewServer(w.store.core, nil)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}
	_, channel := w.workspaceWithChannel(t, ctx)
	msg := w.send(t, ctx, channel.PlaceID, w.humanA, "手が空いたら見てください")

	status, body := callLocal(t, ctx, server.localReplyLater, LocalReplyLaterPath, map[string]any{
		"place_id": channel.PlaceID, "message_id": msg.MessageID,
		"note": "他の対応中です。後で返信します", "remind_in_minutes": 45,
	}, authorization)
	if status != http.StatusCreated || body["created"] != true {
		t.Fatalf("agent marker: status %d body %v", status, body)
	}
	marker := body["marker"].(map[string]any)
	participant := marker["participant"].(map[string]any)
	if participant["kind"] != "personality_agent" || participant["personality_agent_id"] != w.agent.ID {
		t.Fatalf("agent marker participant = %v", marker)
	}
	// The agent owns this marker, so its own response carries the schedule.
	if marker["remind_at"] == nil || marker["note"] != "他の対応中です。後で返信します" {
		t.Fatalf("agent marker = %v", marker)
	}
	markerID := marker["marker_id"].(string)

	// The human sees the promise through the store the web UI reads, with the
	// agent's private reminder time withheld.
	markers, err := w.store.ReplyLaterMarkersFor(ctx, w.humanA)
	if err != nil {
		t.Fatalf("list markers: %v", err)
	}
	if len(markers) != 1 || markers[0].MarkerID != markerID {
		t.Fatalf("human view = %+v", markers)
	}
	if wire := replyLaterToWire(markers[0], w.humanA); wire.RemindAt != nil {
		t.Fatalf("human wire leaked the agent's remind_at: %+v", wire)
	}

	// The default reminder applies when the agent names no time.
	other := w.send(t, ctx, channel.PlaceID, w.humanA, "こちらもいつか")
	status, body = callLocal(t, ctx, server.localReplyLater, LocalReplyLaterPath, map[string]any{
		"place_id": channel.PlaceID, "message_id": other.MessageID,
	}, authorization)
	if status != http.StatusCreated || body["marker"].(map[string]any)["note"] != DefaultReplyLaterNote {
		t.Fatalf("default marker: status %d body %v", status, body)
	}

	status, body = callLocal(t, ctx, server.localReplyLaterResolve, LocalReplyLaterResolvePath,
		map[string]any{"marker_id": markerID}, authorization)
	if status != http.StatusOK || body["marker"].(map[string]any)["resolved"] != true {
		t.Fatalf("agent resolve: status %d body %v", status, body)
	}

	// Another participant's marker is missing, not forbidden — the same answer
	// the human lane gives.
	humanMarker, _, err := w.store.CreateReplyLater(
		ctx, channel.PlaceID, other.MessageID, w.humanA, "", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("human marker: %v", err)
	}
	status, _ = callLocal(t, ctx, server.localReplyLaterResolve, LocalReplyLaterResolvePath,
		map[string]any{"marker_id": humanMarker.MarkerID}, authorization)
	if status != http.StatusNotFound {
		t.Fatalf("agent resolving the human's marker: status %d, want 404", status)
	}
}

func TestLocalOverviewRestoresReplyLaterMarkerAfterServerReconstruction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}
	_, channel := w.workspaceWithChannel(t, ctx)
	message := w.send(t, ctx, channel.PlaceID, w.humanA, "再起動後に対応してください")

	beforeRestart := NewServer(w.store.core, nil)
	status, body := callLocal(t, ctx, beforeRestart.localReplyLater, LocalReplyLaterPath, map[string]any{
		"place_id": channel.PlaceID, "message_id": message.MessageID,
		"note": "再起動後も覚えておく", "remind_in_minutes": 30,
	}, authorization)
	if status != http.StatusCreated {
		t.Fatalf("create marker before reconstruction: status %d body %v", status, body)
	}
	markerID := body["marker"].(map[string]any)["marker_id"].(string)

	// The overview is the reconstruction boundary for the agent adapter. A new
	// server instance sharing only the durable Store must return the marker with
	// the same owner-private schedule that the Human bootstrap returns.
	afterRestart := NewServer(w.store.core, nil)
	status, body = callLocal(t, ctx, afterRestart.localOverview, LocalOverviewPath,
		map[string]any{}, authorization)
	if status != http.StatusOK {
		t.Fatalf("overview after reconstruction: status %d body %v", status, body)
	}
	markers, ok := body["reply_later_markers"].([]any)
	if !ok || len(markers) != 1 {
		t.Fatalf("overview markers = %#v, want the durable marker", body["reply_later_markers"])
	}
	marker := markers[0].(map[string]any)
	if marker["marker_id"] != markerID || marker["note"] != "再起動後も覚えておく" || marker["remind_at"] == nil {
		t.Fatalf("overview marker = %v, want the agent's complete own marker", marker)
	}

	status, body = callLocal(t, ctx, afterRestart.localReplyLaterResolve,
		LocalReplyLaterResolvePath, map[string]any{"marker_id": markerID}, authorization)
	if status != http.StatusOK || body["marker"].(map[string]any)["resolved"] != true {
		t.Fatalf("resolve reconstructed marker: status %d body %v", status, body)
	}
	open, err := w.store.ReplyLaterMarkersFor(ctx, w.agent)
	if err != nil {
		t.Fatalf("list markers after resolve: %v", err)
	}
	if len(open) != 0 {
		t.Fatalf("resolved marker remained open: %+v", open)
	}
}
