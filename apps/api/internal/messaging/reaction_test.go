package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

// callLocal invokes one local-control handler directly, the way the PAID-bound
// Unix socket transport would after authorizing the lease.
func callLocal(
	t *testing.T,
	ctx context.Context,
	handler func(http.ResponseWriter, *http.Request, agentevents.LocalRuntimeAuthorization),
	path string,
	body map[string]any,
	authorization agentevents.LocalRuntimeAuthorization,
) (int, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal local request: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw)).WithContext(ctx)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler(response, request, authorization)
	var decoded map[string]any
	_ = json.Unmarshal(response.Body.Bytes(), &decoded)
	return response.Code, decoded
}

func TestSetReactionIsIdempotentAndAggregates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "リアクションして")

	// First desired-state add.
	got, err := w.store.SetReaction(ctx, ch.PlaceID, msg.MessageID, w.humanB, "👍", true)
	if err != nil {
		t.Fatalf("first set: %v", err)
	}
	if !got.Reacted || len(got.Reactions) != 1 || got.Reactions[0].Emoji != "👍" ||
		len(got.Reactions[0].Participants) != 1 || got.Reactions[0].Participants[0] != w.humanB {
		t.Fatalf("first set state = reacted %v, reactions %+v", got.Reacted, got.Reactions)
	}
	// Repeating the same call cannot invert the already-committed intent.
	got, err = w.store.SetReaction(ctx, ch.PlaceID, msg.MessageID, w.humanB, "👍", true)
	if err != nil || len(got.Reactions) != 1 || len(got.Reactions[0].Participants) != 1 {
		t.Fatalf("idempotent add = result %+v, err %v", got, err)
	}

	// The agent joins the same emoji through the identical path; participants
	// aggregate in reaction order.
	got, err = w.store.SetReaction(ctx, ch.PlaceID, msg.MessageID, w.agent, "👍", true)
	if err != nil {
		t.Fatalf("agent set: %v", err)
	}
	if !got.Reacted || len(got.Reactions) != 1 || len(got.Reactions[0].Participants) != 2 ||
		got.Reactions[0].Participants[0] != w.humanB || got.Reactions[0].Participants[1] != w.agent {
		t.Fatalf("aggregate state = %+v", got.Reactions)
	}

	// A different emoji becomes a second summary, ordered by first reaction.
	got, err = w.store.SetReaction(ctx, ch.PlaceID, msg.MessageID, w.humanA, "🎉", true)
	if err != nil {
		t.Fatalf("second emoji: %v", err)
	}
	if len(got.Reactions) != 2 || got.Reactions[0].Emoji != "👍" || got.Reactions[1].Emoji != "🎉" {
		t.Fatalf("summary order = %+v", got.Reactions)
	}

	// Desired false removes only the actor's own reaction.
	got, err = w.store.SetReaction(ctx, ch.PlaceID, msg.MessageID, w.humanB, "👍", false)
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	if got.Reacted || len(got.Reactions) != 2 || len(got.Reactions[0].Participants) != 1 ||
		got.Reactions[0].Participants[0] != w.agent {
		t.Fatalf("after removal = reacted %v, reactions %+v", got.Reacted, got.Reactions)
	}
	got, err = w.store.SetReaction(ctx, ch.PlaceID, msg.MessageID, w.humanB, "👍", false)
	if err != nil || len(got.Reactions) != 2 || len(got.Reactions[0].Participants) != 1 {
		t.Fatalf("idempotent remove = result %+v, err %v", got, err)
	}

	// Removing the last participant removes the summary.
	got, err = w.store.SetReaction(ctx, ch.PlaceID, msg.MessageID, w.agent, "👍", false)
	if err != nil {
		t.Fatalf("final removal: %v", err)
	}
	if len(got.Reactions) != 1 || got.Reactions[0].Emoji != "🎉" {
		t.Fatalf("after final removal = %+v", got.Reactions)
	}

	// History carries the aggregated reactions.
	history, err := w.store.History(ctx, ch.PlaceID, w.humanA, HistoryOptions{})
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) != 1 || len(history[0].Reactions) != 1 || history[0].Reactions[0].Emoji != "🎉" {
		t.Fatalf("history reactions = %+v", history[0].Reactions)
	}
}

func TestSetReactionAuthorizationAndTombstones(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "本文")

	// A stranger is not told the place exists.
	stranger := Human("018f3f8d-7b2c-7a10-8f9e-00000000ab99")
	if _, err := w.store.SetReaction(ctx, ch.PlaceID, msg.MessageID, stranger, "👍", true); !errors.Is(err, ErrPlaceNotFound) {
		t.Fatalf("stranger set: got %v, want ErrPlaceNotFound", err)
	}

	// Emoji shape is bounded.
	for _, emoji := range []string{"", "a b", "\n", string(make([]rune, MaxReactionEmojiChars+1))} {
		if _, err := w.store.SetReaction(ctx, ch.PlaceID, msg.MessageID, w.humanB, emoji, true); err == nil {
			t.Fatalf("emoji %q must be rejected", emoji)
		}
	}

	// An unknown message in a visible place is reported as missing.
	if _, err := w.store.SetReaction(ctx, ch.PlaceID, newUUIDv7(), w.humanB, "👍", true); !errors.Is(err, ErrMessageNotFound) {
		t.Fatalf("missing message: got %v, want ErrMessageNotFound", err)
	}

	// A tombstone rejects reactions, and deletion clears existing ones.
	if _, err := w.store.SetReaction(ctx, ch.PlaceID, msg.MessageID, w.humanB, "👍", true); err != nil {
		t.Fatalf("react before delete: %v", err)
	}
	deleted, err := w.store.DeleteMessage(ctx, ch.PlaceID, msg.MessageID, w.humanA)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if len(deleted.Reactions) != 0 {
		t.Fatalf("tombstone reactions = %+v", deleted.Reactions)
	}
	if _, err := w.store.SetReaction(ctx, ch.PlaceID, msg.MessageID, w.humanB, "👍", true); !errors.Is(err, ErrMessageDeleted) {
		t.Fatalf("react to tombstone: got %v, want ErrMessageDeleted", err)
	}
	history, err := w.store.History(ctx, ch.PlaceID, w.humanA, HistoryOptions{})
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) != 1 || len(history[0].Reactions) != 0 {
		t.Fatalf("tombstone history reactions = %+v", history[0].Reactions)
	}
}

func TestSetReactionRollsBackWhenSnapshotConstructionFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "snapshot failure")

	_, err := w.store.setReaction(
		ctx, ch.PlaceID, msg.MessageID, w.humanB, "👍", true,
		func(context.Context, pgx.Tx, string) ([]ReactionSummary, error) {
			return nil, errors.New("induced snapshot failure")
		},
	)
	if err == nil {
		t.Fatal("set reaction succeeded despite snapshot failure")
	}
	history, err := w.store.History(ctx, ch.PlaceID, w.humanA, HistoryOptions{})
	if err != nil {
		t.Fatalf("history after rollback: %v", err)
	}
	if len(history) != 1 || len(history[0].Reactions) != 0 {
		t.Fatalf("reaction mutation escaped rollback: %+v", history)
	}
}

func TestEditKeepsReactionsOnTheReturnedMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "編集前")
	if _, err := w.store.SetReaction(ctx, ch.PlaceID, msg.MessageID, w.humanB, "👀", true); err != nil {
		t.Fatalf("react: %v", err)
	}
	edited, err := w.store.EditMessage(ctx, ch.PlaceID, msg.MessageID, w.humanA, "編集後")
	if err != nil {
		t.Fatalf("edit: %v", err)
	}
	// The edited event replaces the message wholesale on live clients, so the
	// reaction state must ride along.
	if len(edited.Reactions) != 1 || edited.Reactions[0].Emoji != "👀" {
		t.Fatalf("edited reactions = %+v", edited.Reactions)
	}
}

func TestReactionDesiredStateOverHTTPReachesWSSubscribersAndCatchUp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newWSWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "リアクション対象")

	conn := dialWS(t, ts, w.humanB.ID, nil)
	path := "/messaging/places/" + ch.PlaceID + "/messages/" + msg.MessageID + "/reactions"
	resp, body := call(t, ts, http.MethodPost, path, w.humanA.ID, map[string]any{
		"emoji": "👍", "reacted": true,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("set reaction: status %d body %v", resp.StatusCode, body)
	}
	if body["reacted"] != true {
		t.Fatalf("set reaction body = %v", body)
	}
	if body["message_id"] != msg.MessageID {
		t.Fatalf("reaction message id = %v, want %s", body["message_id"], msg.MessageID)
	}
	reactions := body["reactions"].([]any)
	if len(reactions) != 1 || reactions[0].(map[string]any)["emoji"] != "👍" {
		t.Fatalf("set reactions = %v", reactions)
	}

	// Live fan-out: the other participant sees the durable event.
	frame := readFrame(t, conn)
	if frame["type"] != "event" {
		t.Fatalf("frame = %v", frame)
	}
	event := frame["event"].(map[string]any)
	if event["type"] != EventReactionUpdated {
		t.Fatalf("event = %v", event)
	}
	if _, ok := event["message"]; ok {
		t.Fatalf("reaction event carried unrelated message fields: %v", event)
	}
	update := event["reaction"].(map[string]any)
	if update["message_id"] != msg.MessageID {
		t.Fatalf("reaction event message id = %v", update["message_id"])
	}
	eventReactions := update["reactions"].([]any)
	participants := eventReactions[0].(map[string]any)["participants"].([]any)
	if len(participants) != 1 || participants[0].(map[string]any)["human_id"] != w.humanA.ID {
		t.Fatalf("event participants = %v", participants)
	}

	// Bad emoji fails closed before the store.
	resp, _ = call(t, ts, http.MethodPost, path, w.humanA.ID, map[string]any{
		"emoji": "", "reacted": true,
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("empty emoji: status %d, want 400", resp.StatusCode)
	}
	resp, _ = call(t, ts, http.MethodPost, path, w.humanA.ID, map[string]any{"emoji": "👍"})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing desired state: status %d, want 400", resp.StatusCode)
	}

	// Reconnect catch-up replays the message with its current reactions.
	replay := dialWS(t, ts, w.humanB.ID, map[string]int64{ch.PlaceID: 0})
	frame = readFrame(t, replay)
	if frame["type"] != "event" {
		t.Fatalf("replay frame = %v", frame)
	}
	replayed := frame["event"].(map[string]any)["message"].(map[string]any)
	replayedReactions := replayed["reactions"].([]any)
	if len(replayedReactions) != 1 || replayedReactions[0].(map[string]any)["emoji"] != "👍" {
		t.Fatalf("replayed reactions = %v", replayedReactions)
	}

	resp, body = call(t, ts, http.MethodPost, path, w.humanA.ID, map[string]any{
		"emoji": "👍", "reacted": false,
	})
	if resp.StatusCode != http.StatusOK || body["reacted"] != false {
		t.Fatalf("clear reaction: status %d body %v", resp.StatusCode, body)
	}
	frame = readFrame(t, conn)
	cleared := frame["event"].(map[string]any)["reaction"].(map[string]any)
	if got, ok := cleared["reactions"].([]any); !ok || len(got) != 0 {
		t.Fatalf("cleared reactions = %v", cleared["reactions"])
	}
}

func TestConcurrentReactionPublishesFollowCommittedSnapshots(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "ordered reactions")
	hub := NewHub(w.store)
	server := NewServer(w.store, nil)
	server.Hub = hub
	sub := hub.subscribe(w.humanA)
	sub.markVisible(ch.PlaceID, true)
	t.Cleanup(func() { hub.unsubscribe(sub) })

	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, actor := range []ParticipantRef{w.humanA, w.humanB} {
		actor := actor
		go func() {
			<-start
			_, err := server.setReaction(
				ctx, ch.PlaceID, msg.MessageID, actor, "👍", true,
			)
			errs <- err
		}()
	}
	close(start)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent set reaction: %v", err)
		}
	}

	for wantParticipants := 1; wantParticipants <= 2; wantParticipants++ {
		select {
		case raw := <-sub.send:
			var frame struct {
				Event Event `json:"event"`
			}
			if err := json.Unmarshal(raw, &frame); err != nil {
				t.Fatalf("decode reaction event: %v", err)
			}
			if frame.Event.Reaction == nil || len(frame.Event.Reaction.Reactions) != 1 {
				t.Fatalf("reaction event = %+v", frame.Event)
			}
			if got := len(frame.Event.Reaction.Reactions[0].Participants); got != wantParticipants {
				t.Fatalf("published participant count = %d, want %d", got, wantParticipants)
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for reaction event")
		}
	}
}

func TestReactionPublishLockDoesNotSerializeDifferentMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	first := w.send(t, ctx, ch.PlaceID, w.humanA, "first")
	second := w.send(t, ctx, ch.PlaceID, w.humanA, "second")
	server := NewServer(w.store, nil)

	unlockFirst := server.lockReactionPublish(first.MessageID)
	defer unlockFirst()
	result := make(chan error, 1)
	go func() {
		_, err := server.setReaction(
			ctx, ch.PlaceID, second.MessageID, w.humanB, "👍", true,
		)
		result <- err
	}()

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("different-message reaction: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("different message was blocked by unrelated reaction lane")
	}
}

func TestLocalReactStatesDesiredAgentReaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	server := NewServer(w.store, nil)
	if err := w.store.EnsureDefaultWorkspaceMembership(ctx, w.humanA); err != nil {
		t.Fatalf("admit human: %v", err)
	}
	msg := w.send(t, ctx, DefaultGeneralChannelID, w.humanA, "generalの発言")

	react := func(emoji string, reacted bool) (int, map[string]any) {
		t.Helper()
		return callLocal(t, ctx, server.localReact, LocalReactPath, map[string]any{
			"place_id": DefaultGeneralChannelID, "message_id": msg.MessageID,
			"emoji": emoji, "reacted": reacted,
		}, agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID})
	}

	status, body := react("🎉", true)
	if status != http.StatusOK || body["reacted"] != true {
		t.Fatalf("agent react: status %d body %v", status, body)
	}
	reactions := body["reactions"].([]any)
	participant := reactions[0].(map[string]any)["participants"].([]any)[0].(map[string]any)
	if participant["kind"] != "personality_agent" || participant["personality_agent_id"] != w.agent.ID {
		t.Fatalf("agent reaction participant = %v", participant)
	}

	// Repeating true is idempotent; false is the explicit removal intent.
	status, body = react("🎉", true)
	if status != http.StatusOK || body["reacted"] != true || len(body["reactions"].([]any)) != 1 {
		t.Fatalf("agent repeated react: status %d body %v", status, body)
	}
	status, body = react("🎉", false)
	if status != http.StatusOK || body["reacted"] != false {
		t.Fatalf("agent un-react: status %d body %v", status, body)
	}
	if n := len(body["reactions"].([]any)); n != 0 {
		t.Fatalf("reactions after un-react = %d, want 0", n)
	}
}
