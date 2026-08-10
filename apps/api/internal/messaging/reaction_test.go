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
	resp, body := call(t, ts, http.MethodPost, path, w.humanA.ID, map[string]any{"emoji": "👍", "client_nonce": "human-reaction-1"})
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
	resp, _ = call(t, ts, http.MethodPost, path, w.humanA.ID, map[string]any{"emoji": "", "client_nonce": "human-reaction-invalid"})
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
	resp, _ = call(t, ts, http.MethodPost, path, w.humanA.ID, map[string]any{"emoji": "👍", "client_nonce": "human-reaction-2"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("untoggle: status %d", resp.StatusCode)
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
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "concurrent reactions")
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
			_, _, err := server.toggleReaction(ctx, ch.PlaceID, msg.MessageID, actor, "👍", "concurrent-"+actor.Key())
			errs <- err
		}()
	}
	close(start)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent toggle: %v", err)
		}
	}

	// Whichever actor wins the lock is the one-participant snapshot. The second
	// commit must publish the complete two-participant snapshot last.
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

func TestLocalReactTogglesForTheAgent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	server := NewServer(w.store, nil)
	if err := w.store.EnsureDefaultWorkspaceMembership(ctx, w.humanA); err != nil {
		t.Fatalf("admit human: %v", err)
	}
	msg := w.send(t, ctx, DefaultGeneralChannelID, w.humanA, "generalの発言")

	react := func(emoji, clientNonce string) (int, map[string]any) {
		t.Helper()
		return callLocal(t, ctx, server.localReact, LocalReactPath, map[string]any{
			"place_id": DefaultGeneralChannelID, "message_id": msg.MessageID, "emoji": emoji,
			"client_nonce": clientNonce,
		}, agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID})
	}

	status, body := react("🎉", "agent-reaction-add")
	if status != http.StatusOK || body["reacted"] != true {
		t.Fatalf("agent react: status %d body %v", status, body)
	}
	reactions := body["message"].(map[string]any)["reactions"].([]any)
	participant := reactions[0].(map[string]any)["participants"].([]any)[0].(map[string]any)
	if participant["kind"] != "personality_agent" || participant["personality_agent_id"] != w.agent.ID {
		t.Fatalf("agent reaction participant = %v", participant)
	}
	status, body = react("🎉", "agent-reaction-add")
	if status != http.StatusOK || body["reacted"] != true || len(body["message"].(map[string]any)["reactions"].([]any)) != 1 {
		t.Fatalf("agent reaction replay changed state: status %d body %v", status, body)
	}

	// A fresh gesture removes it again.
	status, body = react("🎉", "agent-reaction-remove")
	if status != http.StatusOK || body["reacted"] != false {
		t.Fatalf("agent un-react: status %d body %v", status, body)
	}
	if n := len(body["message"].(map[string]any)["reactions"].([]any)); n != 0 {
		t.Fatalf("reactions after un-react = %d, want 0", n)
	}
}

func TestToggleReactionIdempotentReplayAndConcurrentDuplicate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "retry-safe reaction")

	first, reacted, err := w.store.ToggleReactionIdempotent(
		ctx, ch.PlaceID, msg.MessageID, w.humanB, "👍", "gesture-add")
	if err != nil || !reacted || len(first.Reactions) != 1 {
		t.Fatalf("first idempotent toggle: reacted=%v reactions=%+v err=%v", reacted, first.Reactions, err)
	}
	replayed, replayedReacted, err := w.store.ToggleReactionIdempotent(
		ctx, ch.PlaceID, msg.MessageID, w.humanB, "👍", "gesture-add")
	if err != nil || !replayedReacted || len(replayed.Reactions) != 1 {
		t.Fatalf("replayed toggle changed result: reacted=%v reactions=%+v err=%v", replayedReacted, replayed.Reactions, err)
	}

	start := make(chan struct{})
	type result struct {
		message Message
		reacted bool
		err     error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			<-start
			message, reacted, err := w.store.ToggleReactionIdempotent(
				ctx, ch.PlaceID, msg.MessageID, w.humanB, "👍", "gesture-remove")
			results <- result{message: message, reacted: reacted, err: err}
		}()
	}
	close(start)
	for range 2 {
		got := <-results
		if got.err != nil || got.reacted || len(got.message.Reactions) != 0 {
			t.Fatalf("concurrent duplicate did not converge: reacted=%v reactions=%+v err=%v", got.reacted, got.message.Reactions, got.err)
		}
	}

	if _, _, err := w.store.ToggleReactionIdempotent(
		ctx, ch.PlaceID, msg.MessageID, w.humanB, "🎉", "gesture-remove"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("nonce reuse for another mutation: got %v, want ErrIdempotencyConflict", err)
	}
	var reactions, mutations int
	if err := w.store.pool.QueryRow(ctx,
		"SELECT count(*) FROM message_reactions WHERE message_id = $1", msg.MessageID).Scan(&reactions); err != nil {
		t.Fatal(err)
	}
	if err := w.store.pool.QueryRow(ctx,
		"SELECT count(*) FROM message_reaction_mutations WHERE message_id = $1", msg.MessageID).Scan(&mutations); err != nil {
		t.Fatal(err)
	}
	if reactions != 0 || mutations != 2 {
		t.Fatalf("durable reaction state: reactions=%d mutations=%d, want 0/2", reactions, mutations)
	}
}

func TestToggleReactionNonceConflictAcrossMessageRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	first := w.send(t, ctx, ch.PlaceID, w.humanA, "first target")
	second := w.send(t, ctx, ch.PlaceID, w.humanA, "second target")

	type result struct {
		messageID string
		err       error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for _, messageID := range []string{first.MessageID, second.MessageID} {
		messageID := messageID
		go func() {
			<-start
			_, _, err := w.store.ToggleReactionIdempotent(
				ctx, ch.PlaceID, messageID, w.humanB, "👍", "same-gesture")
			results <- result{messageID: messageID, err: err}
		}()
	}
	close(start)
	var succeeded, conflicted int
	for range 2 {
		got := <-results
		switch {
		case got.err == nil:
			succeeded++
		case errors.Is(got.err, ErrIdempotencyConflict):
			conflicted++
		default:
			t.Fatalf("toggle %s: unexpected error %v", got.messageID, got.err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("cross-message nonce race: succeeded=%d conflicted=%d, want 1/1", succeeded, conflicted)
	}
	var reactions, mutations int
	if err := w.store.pool.QueryRow(ctx,
		"SELECT count(*) FROM message_reactions WHERE message_id = ANY($1)",
		[]string{first.MessageID, second.MessageID}).Scan(&reactions); err != nil {
		t.Fatal(err)
	}
	if err := w.store.pool.QueryRow(ctx,
		"SELECT count(*) FROM message_reaction_mutations WHERE client_nonce = 'same-gesture'").Scan(&mutations); err != nil {
		t.Fatal(err)
	}
	if reactions != 1 || mutations != 1 {
		t.Fatalf("cross-message durable state: reactions=%d mutations=%d, want 1/1", reactions, mutations)
	}
}

func TestReactionNonceConflictMapsToRESTAndLocalControl409(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newWSWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	first := w.send(t, ctx, ch.PlaceID, w.humanA, "first REST target")
	second := w.send(t, ctx, ch.PlaceID, w.humanA, "second REST target")

	restPath := func(messageID string) string {
		return "/messaging/places/" + ch.PlaceID + "/messages/" + messageID + "/reactions"
	}
	request := map[string]any{"emoji": "👍", "client_nonce": "rest-conflict"}
	if response, body := call(t, ts, http.MethodPost, restPath(first.MessageID), w.humanB.ID, request); response.StatusCode != http.StatusOK {
		t.Fatalf("first REST reaction: status=%d body=%v", response.StatusCode, body)
	}
	response, body := call(t, ts, http.MethodPost, restPath(second.MessageID), w.humanB.ID, request)
	if response.StatusCode != http.StatusConflict || body["error"] != "idempotency_conflict" {
		t.Fatalf("REST nonce conflict: status=%d body=%v", response.StatusCode, body)
	}

	server := NewServer(w.store, nil)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}
	localRequest := func(messageID string) (int, map[string]any) {
		return callLocal(t, ctx, server.localReact, LocalReactPath, map[string]any{
			"place_id": ch.PlaceID, "message_id": messageID,
			"emoji": "🎉", "client_nonce": "local-conflict",
		}, authorization)
	}
	if status, localBody := localRequest(first.MessageID); status != http.StatusOK {
		t.Fatalf("first local reaction: status=%d body=%v", status, localBody)
	}
	status, localBody := localRequest(second.MessageID)
	if status != http.StatusConflict || localBody["error"] != "idempotency_conflict" {
		t.Fatalf("local nonce conflict: status=%d body=%v", status, localBody)
	}
}
