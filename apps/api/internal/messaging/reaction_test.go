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

func TestToggleReactionFlipsAndAggregates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "リアクションして")

	// First toggle adds.
	got, reacted, err := w.store.ToggleReaction(ctx, ch.PlaceID, msg.MessageID, w.humanB, "👍")
	if err != nil {
		t.Fatalf("first toggle: %v", err)
	}
	if !reacted || len(got.Reactions) != 1 || got.Reactions[0].Emoji != "👍" ||
		len(got.Reactions[0].Participants) != 1 || got.Reactions[0].Participants[0] != w.humanB {
		t.Fatalf("first toggle state = reacted %v, reactions %+v", reacted, got.Reactions)
	}

	// The agent joins the same emoji through the identical path; participants
	// aggregate in reaction order.
	got, reacted, err = w.store.ToggleReaction(ctx, ch.PlaceID, msg.MessageID, w.agent, "👍")
	if err != nil {
		t.Fatalf("agent toggle: %v", err)
	}
	if !reacted || len(got.Reactions) != 1 || len(got.Reactions[0].Participants) != 2 ||
		got.Reactions[0].Participants[0] != w.humanB || got.Reactions[0].Participants[1] != w.agent {
		t.Fatalf("aggregate state = %+v", got.Reactions)
	}

	// A different emoji becomes a second summary, ordered by first reaction.
	got, _, err = w.store.ToggleReaction(ctx, ch.PlaceID, msg.MessageID, w.humanA, "🎉")
	if err != nil {
		t.Fatalf("second emoji: %v", err)
	}
	if len(got.Reactions) != 2 || got.Reactions[0].Emoji != "👍" || got.Reactions[1].Emoji != "🎉" {
		t.Fatalf("summary order = %+v", got.Reactions)
	}

	// Second toggle removes only the actor's own reaction.
	got, reacted, err = w.store.ToggleReaction(ctx, ch.PlaceID, msg.MessageID, w.humanB, "👍")
	if err != nil {
		t.Fatalf("remove toggle: %v", err)
	}
	if reacted || len(got.Reactions) != 2 || len(got.Reactions[0].Participants) != 1 ||
		got.Reactions[0].Participants[0] != w.agent {
		t.Fatalf("after removal = reacted %v, reactions %+v", reacted, got.Reactions)
	}

	// Removing the last participant removes the summary.
	got, _, err = w.store.ToggleReaction(ctx, ch.PlaceID, msg.MessageID, w.agent, "👍")
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

func TestToggleReactionAuthorizationAndTombstones(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "本文")

	// A stranger is not told the place exists.
	stranger := Human("018f3f8d-7b2c-7a10-8f9e-00000000ab99")
	if _, _, err := w.store.ToggleReaction(ctx, ch.PlaceID, msg.MessageID, stranger, "👍"); !errors.Is(err, ErrPlaceNotFound) {
		t.Fatalf("stranger toggle: got %v, want ErrPlaceNotFound", err)
	}

	// Emoji shape is bounded.
	for _, emoji := range []string{"", "a b", "\n", string(make([]rune, MaxReactionEmojiChars+1))} {
		if _, _, err := w.store.ToggleReaction(ctx, ch.PlaceID, msg.MessageID, w.humanB, emoji); err == nil {
			t.Fatalf("emoji %q must be rejected", emoji)
		}
	}

	// An unknown message in a visible place is reported as missing.
	if _, _, err := w.store.ToggleReaction(ctx, ch.PlaceID, newUUIDv7(), w.humanB, "👍"); !errors.Is(err, ErrMessageNotFound) {
		t.Fatalf("missing message: got %v, want ErrMessageNotFound", err)
	}

	// A tombstone rejects reactions, and deletion clears existing ones.
	if _, _, err := w.store.ToggleReaction(ctx, ch.PlaceID, msg.MessageID, w.humanB, "👍"); err != nil {
		t.Fatalf("react before delete: %v", err)
	}
	deleted, err := w.store.DeleteMessage(ctx, ch.PlaceID, msg.MessageID, w.humanA)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if len(deleted.Reactions) != 0 {
		t.Fatalf("tombstone reactions = %+v", deleted.Reactions)
	}
	if _, _, err := w.store.ToggleReaction(ctx, ch.PlaceID, msg.MessageID, w.humanB, "👍"); !errors.Is(err, ErrMessageDeleted) {
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

func TestEditKeepsReactionsOnTheReturnedMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "編集前")
	if _, _, err := w.store.ToggleReaction(ctx, ch.PlaceID, msg.MessageID, w.humanB, "👀"); err != nil {
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

func TestReactionToggleOverHTTPReachesWSSubscribersAndCatchUp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newWSWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "リアクション対象")

	conn := dialWS(t, ts, w.humanB.ID, nil)
	path := "/messaging/places/" + ch.PlaceID + "/messages/" + msg.MessageID + "/reactions"
	resp, body := call(t, ts, http.MethodPost, path, w.humanA.ID, map[string]any{"emoji": "👍"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("toggle: status %d body %v", resp.StatusCode, body)
	}
	if body["reacted"] != true {
		t.Fatalf("toggle body = %v", body)
	}
	reactions := body["message"].(map[string]any)["reactions"].([]any)
	if len(reactions) != 1 || reactions[0].(map[string]any)["emoji"] != "👍" {
		t.Fatalf("toggle reactions = %v", reactions)
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
	// The event is a partial update: identity plus reactions, never a whole
	// message. A full message would carry the pre-lock content and roll back a
	// concurrent edit that committed after this toggle released the row lock.
	if _, ok := event["message"]; ok {
		t.Fatalf("reaction event must not carry a message: %v", event)
	}
	update := event["reaction"].(map[string]any)
	if update["message_id"] != msg.MessageID {
		t.Fatalf("reaction message_id = %v, want %v", update["message_id"], msg.MessageID)
	}
	eventReactions := update["reactions"].([]any)
	participants := eventReactions[0].(map[string]any)["participants"].([]any)
	if len(participants) != 1 || participants[0].(map[string]any)["human_id"] != w.humanA.ID {
		t.Fatalf("event participants = %v", participants)
	}

	// Bad emoji fails closed before the store.
	resp, _ = call(t, ts, http.MethodPost, path, w.humanA.ID, map[string]any{"emoji": ""})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("empty emoji: status %d, want 400", resp.StatusCode)
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

	// Removing the last reaction publishes an empty set rather than omitting
	// the field, so a client can tell "cleared" from "unchanged".
	resp, _ = call(t, ts, http.MethodPost, path, w.humanA.ID, map[string]any{"emoji": "👍"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("untoggle: status %d", resp.StatusCode)
	}
	frame = readFrame(t, conn)
	cleared := frame["event"].(map[string]any)["reaction"].(map[string]any)
	if got, ok := cleared["reactions"].([]any); !ok || len(got) != 0 {
		t.Fatalf("cleared reactions = %v", cleared["reactions"])
	}
}

func TestLocalReactTogglesForTheAgent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	server := NewServer(w.store, nil)
	if err := w.store.EnsureDefaultWorkspaceMembership(ctx, w.humanA); err != nil {
		t.Fatalf("admit human: %v", err)
	}
	msg := w.send(t, ctx, DefaultGeneralChannelID, w.humanA, "generalの発言")

	react := func(emoji string) (int, map[string]any) {
		t.Helper()
		return callLocal(t, ctx, server.localReact, LocalReactPath, map[string]any{
			"place_id": DefaultGeneralChannelID, "message_id": msg.MessageID, "emoji": emoji,
		}, agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID})
	}

	status, body := react("🎉")
	if status != http.StatusOK || body["reacted"] != true {
		t.Fatalf("agent react: status %d body %v", status, body)
	}
	reactions := body["message"].(map[string]any)["reactions"].([]any)
	participant := reactions[0].(map[string]any)["participants"].([]any)[0].(map[string]any)
	if participant["kind"] != "personality_agent" || participant["personality_agent_id"] != w.agent.ID {
		t.Fatalf("agent reaction participant = %v", participant)
	}

	// The identical toggle removes it again.
	status, body = react("🎉")
	if status != http.StatusOK || body["reacted"] != false {
		t.Fatalf("agent un-react: status %d body %v", status, body)
	}
	if n := len(body["message"].(map[string]any)["reactions"].([]any)); n != 0 {
		t.Fatalf("reactions after un-react = %d, want 0", n)
	}
}
