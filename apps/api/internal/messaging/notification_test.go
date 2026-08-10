package messaging

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

// mintHuman adds one more Human to the world and names them. Notification
// tests need a third participant: an author, someone who is called, and
// someone who is deliberately not.
func (w world) mintHuman(t *testing.T, ctx context.Context, name string) ParticipantRef {
	t.Helper()
	id, err := koseki.New(w.store.pool).MintHuman(ctx)
	if err != nil {
		t.Fatalf("mint human %s: %v", name, err)
	}
	if _, err := w.store.pool.Exec(ctx,
		"UPDATE humans SET display_name = $1 WHERE human_id = $2", name, id); err != nil {
		t.Fatalf("name human %s: %v", name, err)
	}
	return Human(id)
}

func reasonFor(t *testing.T, decisions []NotificationDecision, participant ParticipantRef) string {
	t.Helper()
	for _, decision := range decisions {
		if decision.Participant == participant {
			return decision.Reason
		}
	}
	return ""
}

func TestNotificationDecisionsFollowPriorityAndMuteSuppresses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	ws, ch := w.workspaceWithChannel(t, ctx)

	// A participant who has never touched their settings hears everything, and
	// the author is never called for their own words.
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "今日のデプロイの件です")
	decisions, err := w.store.NotificationDecisionsFor(ctx, ch, msg)
	if err != nil {
		t.Fatalf("decisions: %v", err)
	}
	if len(decisions) != 2 {
		t.Fatalf("default decisions = %+v, want humanB and the agent", decisions)
	}
	if reasonFor(t, decisions, w.humanB) != NotifyReasonAll {
		t.Fatalf("humanB reason = %q, want all", reasonFor(t, decisions, w.humanB))
	}
	if reasonFor(t, decisions, w.humanA) != "" {
		t.Fatalf("the author must not be called for their own message: %+v", decisions)
	}

	// mentions: everything else in the channel goes quiet, a mention does not.
	if _, err := w.store.SetNotificationSetting(ctx, w.humanB, NotifyLevelMentions, nil, nil); err != nil {
		t.Fatalf("humanB mentions: %v", err)
	}
	decisions, err = w.store.NotificationDecisionsFor(ctx, ch, msg)
	if err != nil {
		t.Fatalf("decisions after mentions: %v", err)
	}
	if reasonFor(t, decisions, w.humanB) != "" {
		t.Fatalf("mentions level must not be called by ambient chatter: %+v", decisions)
	}
	mention := w.send(t, ctx, ch.PlaceID, w.humanA, "@Haru 確認をお願いします")
	decisions, err = w.store.NotificationDecisionsFor(ctx, ch, mention)
	if err != nil {
		t.Fatalf("decisions for mention: %v", err)
	}
	if reasonFor(t, decisions, w.humanB) != NotifyReasonMention {
		t.Fatalf("mention reason = %q, want mention", reasonFor(t, decisions, w.humanB))
	}

	// keyword: a word someone asked to be called for outranks "all" as the
	// explanation, and reaches them even at the mentions level.
	if _, err := w.store.SetNotificationSetting(
		ctx, w.humanB, NotifyLevelMentions, nil, []string{"デプロイ"}); err != nil {
		t.Fatalf("humanB keywords: %v", err)
	}
	decisions, err = w.store.NotificationDecisionsFor(ctx, ch, msg)
	if err != nil {
		t.Fatalf("decisions for keyword: %v", err)
	}
	if reasonFor(t, decisions, w.humanB) != NotifyReasonKeyword {
		t.Fatalf("keyword reason = %q, want keyword", reasonFor(t, decisions, w.humanB))
	}
	// A mention still outranks the keyword it also contains.
	both := w.send(t, ctx, ch.PlaceID, w.humanA, "@Haru デプロイお願いします")
	decisions, err = w.store.NotificationDecisionsFor(ctx, ch, both)
	if err != nil {
		t.Fatalf("decisions for mention+keyword: %v", err)
	}
	if reasonFor(t, decisions, w.humanB) != NotifyReasonMention {
		t.Fatalf("mention must outrank keyword: %+v", decisions)
	}

	// mute silences the place completely — including the mention and the
	// keyword. Silencing a place means silence, not "unless someone insists".
	muted := []PlaceNotifyLevel{{PlaceID: ch.PlaceID, Level: NotifyLevelMute}}
	if _, err := w.store.SetNotificationSetting(
		ctx, w.humanB, NotifyLevelAll, muted, []string{"デプロイ"}); err != nil {
		t.Fatalf("humanB mute: %v", err)
	}
	for _, message := range []Message{msg, mention, both} {
		decisions, err = w.store.NotificationDecisionsFor(ctx, ch, message)
		if err != nil {
			t.Fatalf("decisions while muted: %v", err)
		}
		if reasonFor(t, decisions, w.humanB) != "" {
			t.Fatalf("mute must suppress every reason: %+v", decisions)
		}
	}
	// The mute is scoped to that one place: a second channel still calls them.
	other, err := w.store.CreateChannel(ctx, ws.WorkspaceID, "random", "", w.humanA)
	if err != nil {
		t.Fatalf("create second channel: %v", err)
	}
	elsewhere := w.send(t, ctx, other.PlaceID, w.humanA, "こちらは通常運転です")
	decisions, err = w.store.NotificationDecisionsFor(ctx, other, elsewhere)
	if err != nil {
		t.Fatalf("decisions elsewhere: %v", err)
	}
	if reasonFor(t, decisions, w.humanB) != NotifyReasonAll {
		t.Fatalf("a muted place must not silence another: %+v", decisions)
	}
}

func TestNotificationDecisionsTreatDMsAsTheirOwnReason(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	if _, err := w.store.CreateWorkspace(ctx, "sumi-dev", w.humanA); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := w.store.EnsureDefaultWorkspaceMembership(ctx, w.humanA); err != nil {
		t.Fatalf("admit humanA: %v", err)
	}
	if err := w.store.EnsureDefaultWorkspaceMembership(ctx, w.humanB); err != nil {
		t.Fatalf("admit humanB: %v", err)
	}
	dm, _, err := w.store.EnsureDM(ctx, w.humanA, w.humanB)
	if err != nil {
		t.Fatalf("ensure dm: %v", err)
	}

	// Someone who silenced every channel is still reachable in a dm: a direct
	// message is addressed to them by construction.
	if _, err := w.store.SetNotificationSetting(ctx, w.humanB, NotifyLevelMentions, nil, nil); err != nil {
		t.Fatalf("humanB mentions: %v", err)
	}
	msg := w.send(t, ctx, dm.PlaceID, w.humanA, "少しだけ相談があります")
	decisions, err := w.store.NotificationDecisionsFor(ctx, dm, msg)
	if err != nil {
		t.Fatalf("dm decisions: %v", err)
	}
	if len(decisions) != 1 || decisions[0].Participant != w.humanB || decisions[0].Reason != NotifyReasonDM {
		t.Fatalf("dm decisions = %+v, want humanB via dm", decisions)
	}

	// Muting the dm still silences it: 受信側が最後の決定権を持つ。
	muted := []PlaceNotifyLevel{{PlaceID: dm.PlaceID, Level: NotifyLevelMute}}
	if _, err := w.store.SetNotificationSetting(ctx, w.humanB, NotifyLevelAll, muted, nil); err != nil {
		t.Fatalf("humanB mutes the dm: %v", err)
	}
	decisions, err = w.store.NotificationDecisionsFor(ctx, dm, msg)
	if err != nil {
		t.Fatalf("muted dm decisions: %v", err)
	}
	if len(decisions) != 0 {
		t.Fatalf("muted dm decisions = %+v, want none", decisions)
	}
}

func TestNotificationIntentsAreIssuedAtomicallyAtAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)

	if _, err := w.store.SetNotificationSetting(ctx, w.humanB, NotifyLevelMentions, nil, nil); err != nil {
		t.Fatalf("humanB mentions: %v", err)
	}
	if _, err := w.store.SetNotificationSetting(ctx, w.agent, NotifyLevelMute, nil, nil); err != nil {
		t.Fatalf("agent mute: %v", err)
	}
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "@Haru 確認をお願いします")

	intents, err := w.store.NotificationIntentsForMessage(ctx, msg.MessageID)
	if err != nil {
		t.Fatalf("load committed intents: %v", err)
	}
	if len(intents) != 1 || intents[0].Participant != w.humanB || intents[0].Reason != NotifyReasonMention {
		t.Fatalf("committed intents = %+v, want humanB via mention", intents)
	}

	// A later preference change cannot rewrite what was issued with the
	// message. Live delivery therefore uses the admission-time authority and
	// setting snapshot instead of re-evaluating mutable state after commit.
	if _, err := w.store.SetNotificationSetting(ctx, w.humanB, NotifyLevelMute, nil, nil); err != nil {
		t.Fatalf("humanB mute after admission: %v", err)
	}
	intents, err = w.store.NotificationIntentsForMessage(ctx, msg.MessageID)
	if err != nil {
		t.Fatalf("reload committed intents: %v", err)
	}
	if len(intents) != 1 || intents[0].Reason != NotifyReasonMention {
		t.Fatalf("later setting change rewrote committed intents: %+v", intents)
	}
}

func TestNotificationIntentIssuanceFailureRollsBackMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)

	// This isolated test database deliberately removes the outbox to prove that
	// a message cannot commit without its required intent issuance step.
	if _, err := w.store.pool.Exec(ctx, "DROP TABLE message_notification_intents"); err != nil {
		t.Fatalf("drop intent table: %v", err)
	}
	if _, _, err := w.store.AppendMessage(ctx, AppendInput{
		PlaceID: ch.PlaceID, Author: w.humanA, Content: "commitしてはいけない",
		ClientNonce: "intent-issuance-failure",
	}); err == nil {
		t.Fatal("append succeeded without its notification intent outbox")
	}
	var messages int
	if err := w.store.pool.QueryRow(ctx,
		"SELECT count(*) FROM messages WHERE place_id = $1", ch.PlaceID).Scan(&messages); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if messages != 0 {
		t.Fatalf("messages = %d, want rollback", messages)
	}
	var lastSeq int64
	if err := w.store.pool.QueryRow(ctx,
		"SELECT last_seq FROM places WHERE place_id = $1", ch.PlaceID).Scan(&lastSeq); err != nil {
		t.Fatalf("load place seq: %v", err)
	}
	if lastSeq != 0 {
		t.Fatalf("last_seq = %d, want rollback to 0", lastSeq)
	}
}

func TestNotificationSettingRoundTripsAndStaysItsOwners(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)

	// A participant who never spoke has a full default, not a missing resource.
	resp, body := call(t, ts, http.MethodGet, "/messaging/notification-settings", w.humanA.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get setting: status %d", resp.StatusCode)
	}
	if body["defaults"].(map[string]any)["level"] != NotifyLevelAll {
		t.Fatalf("default setting = %v", body)
	}
	if len(body["per_place"].([]any)) != 0 || len(body["keywords"].([]any)) != 0 {
		t.Fatalf("default setting = %v", body)
	}
	if body["owner"].(map[string]any)["human_id"] != w.humanA.ID {
		t.Fatalf("setting owner = %v", body["owner"])
	}

	update := map[string]any{
		"defaults": map[string]any{"level": NotifyLevelMentions},
		"per_place": []any{map[string]any{
			"place": map[string]any{"kind": "channel", "channel_id": ch.PlaceID},
			"level": NotifyLevelMute,
		}},
		// Blank and duplicate keywords are the same request said twice.
		"keywords": []any{"デプロイ", " ", "デプロイ", "Kuro"},
	}
	resp, body = call(t, ts, http.MethodPut, "/messaging/notification-settings", w.humanA.ID, update)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("put setting: status %d body %v", resp.StatusCode, body)
	}
	if got := body["keywords"].([]any); len(got) != 2 || got[0] != "デプロイ" || got[1] != "Kuro" {
		t.Fatalf("keywords = %v", got)
	}
	perPlace := body["per_place"].([]any)
	if len(perPlace) != 1 {
		t.Fatalf("per_place = %v", perPlace)
	}
	entry := perPlace[0].(map[string]any)
	if entry["level"] != NotifyLevelMute ||
		entry["place"].(map[string]any)["channel_id"] != ch.PlaceID {
		t.Fatalf("per_place entry = %v", entry)
	}

	// bootstrap carries the same value, so the first paint already knows which
	// places are muted.
	resp, body = call(t, ts, http.MethodGet, "/messaging/bootstrap", w.humanA.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bootstrap: status %d", resp.StatusCode)
	}
	setting := body["notification_setting"].(map[string]any)
	if setting["defaults"].(map[string]any)["level"] != NotifyLevelMentions ||
		len(setting["per_place"].([]any)) != 1 {
		t.Fatalf("bootstrap setting = %v", setting)
	}

	// Nobody else's setting moved: this resource is 本人のもの.
	resp, body = call(t, ts, http.MethodGet, "/messaging/notification-settings", w.humanB.ID, nil)
	if resp.StatusCode != http.StatusOK ||
		body["defaults"].(map[string]any)["level"] != NotifyLevelAll ||
		len(body["per_place"].([]any)) != 0 {
		t.Fatalf("humanB setting = %v (status %d)", body, resp.StatusCode)
	}

	// An unknown level is a bad request…
	resp, _ = call(t, ts, http.MethodPut, "/messaging/notification-settings", w.humanA.ID,
		map[string]any{"defaults": map[string]any{"level": "loud"}})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid level: status %d, want 400", resp.StatusCode)
	}
	// …and a place the caller cannot see is missing, never forbidden: the
	// setting route must not confirm places across the membership boundary.
	if err := w.store.EnsureDefaultWorkspaceMembership(ctx, w.humanB); err != nil {
		t.Fatalf("admit humanB: %v", err)
	}
	private, _, err := w.store.EnsureDM(ctx, w.humanB, w.agent)
	if err != nil {
		t.Fatalf("ensure private dm: %v", err)
	}
	resp, _ = call(t, ts, http.MethodPut, "/messaging/notification-settings", w.humanA.ID, map[string]any{
		"defaults": map[string]any{"level": NotifyLevelAll},
		"per_place": []any{map[string]any{
			"place": map[string]any{"kind": "dm", "dm_id": private.PlaceID},
			"level": NotifyLevelMute,
		}},
	})
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("foreign place: status %d, want 404", resp.StatusCode)
	}
	// The rejected write changed nothing.
	stored, err := w.store.NotificationSettingFor(ctx, w.humanA)
	if err != nil {
		t.Fatalf("reload setting: %v", err)
	}
	if stored.Default() != NotifyLevelMentions || len(stored.PerPlace) != 1 {
		t.Fatalf("rejected write leaked through: %+v", stored)
	}
}

func TestMessageCreatedCarriesNotifyOnlyOnTheCalledRecipientsWire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newWSWorld(t, ctx)
	workspace, ch := w.workspaceWithChannel(t, ctx)
	bystander := w.mintHuman(t, ctx, "Nagi")
	if err := w.store.AddWorkspaceMember(ctx, workspace.WorkspaceID, bystander, RoleMember); err != nil {
		t.Fatalf("admit bystander: %v", err)
	}
	// Haru is called by name; Nagi asked to hear only mentions and is not one.
	for _, participant := range []ParticipantRef{w.humanB, bystander} {
		if _, err := w.store.SetNotificationSetting(ctx, participant, NotifyLevelMentions, nil, nil); err != nil {
			t.Fatalf("set %s: %v", participant.Key(), err)
		}
	}

	author := dialWS(t, ts, w.humanA.ID, nil)
	called := dialWS(t, ts, w.humanB.ID, nil)
	quiet := dialWS(t, ts, bystander.ID, nil)

	resp, _ := call(t, ts, http.MethodPost, "/messaging/places/"+ch.PlaceID+"/messages", w.humanA.ID,
		map[string]any{"content": "@Haru 例の件お願いします", "client_nonce": "nonce-notify-1"})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("send: status %d", resp.StatusCode)
	}

	calledEvent := readFrame(t, called)["event"].(map[string]any)
	if calledEvent["type"] != EventMessageCreated {
		t.Fatalf("called event = %v", calledEvent)
	}
	notify, ok := calledEvent["notify"].(map[string]any)
	if !ok || notify["reason"] != NotifyReasonMention {
		t.Fatalf("called wire must explain the call: %v", calledEvent)
	}

	// Everyone else receives the same message with no claim that they were
	// called. The absence of notify is the answer, not a missing field.
	quietEvent := readFrame(t, quiet)["event"].(map[string]any)
	if quietEvent["type"] != EventMessageCreated ||
		quietEvent["message"].(map[string]any)["message_id"] !=
			calledEvent["message"].(map[string]any)["message_id"] {
		t.Fatalf("bystander event = %v", quietEvent)
	}
	if _, leaked := quietEvent["notify"]; leaked {
		t.Fatalf("notify must not ride an uncalled participant's wire: %v", quietEvent)
	}
	// The author is not notified of their own message either.
	authorEvent := readFrame(t, author)["event"].(map[string]any)
	if _, leaked := authorEvent["notify"]; leaked {
		t.Fatalf("the author must not be called for their own message: %v", authorEvent)
	}
}

func TestPublishMessageCreatedAuthorizesAllIntentVariantsOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "全員に一度ずつ")
	authorizer := &countingHubAuthorizer{store: w.store}
	hub := newHub(authorizer)
	subs := []*subscriber{
		hub.subscribe(w.humanA),
		hub.subscribe(w.humanB),
		hub.subscribe(w.agent),
	}
	for _, sub := range subs {
		defer hub.unsubscribe(sub)
	}

	publishMessageCreated(ctx, w.store, hub, ch, msg)

	if authorizer.placeCalls != 1 {
		t.Fatalf("message variant authorization queries = %d, want one", authorizer.placeCalls)
	}
	for _, sub := range subs {
		if got := len(sub.send); got != 1 {
			t.Fatalf("subscriber %s received %d message variants, want one", sub.viewer.Key(), got)
		}
	}
}

func TestLocalNotificationSettingsUseTheSharedStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	server := NewServer(w.store, nil)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}

	// An empty request is a read: the agent's default, same as a Human's.
	status, body := callLocal(t, ctx, server.localNotificationSettings,
		LocalNotificationSettingsPath, map[string]any{}, authorization)
	if status != http.StatusOK {
		t.Fatalf("read setting: status %d body %v", status, body)
	}
	setting := body["setting"].(map[string]any)
	if setting["defaults"].(map[string]any)["level"] != NotifyLevelAll {
		t.Fatalf("agent default = %v", setting)
	}
	owner := setting["owner"].(map[string]any)
	if owner["kind"] != "personality_agent" || owner["personality_agent_id"] != w.agent.ID {
		t.Fatalf("agent setting owner = %v", owner)
	}

	// Naming one preference changes only that one; the rest survives.
	status, body = callLocal(t, ctx, server.localNotificationSettings, LocalNotificationSettingsPath,
		map[string]any{"keywords": []any{"リリース"}}, authorization)
	if status != http.StatusOK {
		t.Fatalf("set keywords: status %d body %v", status, body)
	}
	status, body = callLocal(t, ctx, server.localNotificationSettings, LocalNotificationSettingsPath,
		map[string]any{"defaults_level": NotifyLevelMentions}, authorization)
	if status != http.StatusOK {
		t.Fatalf("set defaults: status %d body %v", status, body)
	}
	setting = body["setting"].(map[string]any)
	if setting["defaults"].(map[string]any)["level"] != NotifyLevelMentions {
		t.Fatalf("agent defaults = %v", setting)
	}
	if keywords := setting["keywords"].([]any); len(keywords) != 1 || keywords[0] != "リリース" {
		t.Fatalf("naming one field must not erase the others: %v", setting)
	}

	// The same store the human UI reads answers with the agent's own setting,
	// and the keyword it named reaches it through the shared evaluator.
	stored, err := w.store.NotificationSettingFor(ctx, w.agent)
	if err != nil {
		t.Fatalf("reload agent setting: %v", err)
	}
	if stored.Default() != NotifyLevelMentions || len(stored.Keywords) != 1 {
		t.Fatalf("agent stored setting = %+v", stored)
	}
	if err := w.store.EnsureDefaultWorkspaceMembership(ctx, w.humanA); err != nil {
		t.Fatalf("admit humanA: %v", err)
	}
	general, err := w.store.PlaceFor(ctx, DefaultGeneralChannelID, w.humanA)
	if err != nil {
		t.Fatalf("load general: %v", err)
	}
	msg := w.send(t, ctx, general.PlaceID, w.humanA, "今夜のリリースについて")
	decisions, err := w.store.NotificationDecisionsFor(ctx, general, msg)
	if err != nil {
		t.Fatalf("decisions: %v", err)
	}
	if reasonFor(t, decisions, w.agent) != NotifyReasonKeyword {
		t.Fatalf("agent reason = %q, want keyword", reasonFor(t, decisions, w.agent))
	}

	// An unknown level is a bad request on this lane too.
	status, _ = callLocal(t, ctx, server.localNotificationSettings, LocalNotificationSettingsPath,
		map[string]any{"defaults_level": "loud"}, authorization)
	if status != http.StatusBadRequest {
		t.Fatalf("invalid level: status %d, want 400", status)
	}
	if !errors.Is(ValidateNotifyLevel("loud"), ErrInvalidNotificationSetting) {
		t.Fatal("an unknown level must be a request error, not an internal one")
	}
}
