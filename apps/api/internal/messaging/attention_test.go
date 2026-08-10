package messaging

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

// queue runs the same fan-out the transports run after a commit, so these tests
// exercise the real path from「呼ぶと決めた」to「候補が積まれた」rather than a
// hand-written insert.
func (w world) queue(t *testing.T, ctx context.Context, place Place, msg Message) {
	t.Helper()
	decisions, err := w.store.NotificationDecisionsFor(ctx, place, msg)
	if err != nil {
		t.Fatalf("decisions: %v", err)
	}
	w.store.recordAttentionCandidates(ctx, place, msg, decisions)
}

func TestMentioningAnAgentQueuesACandidateItCanTakeIn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	server := NewServer(w.store, nil)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}

	mention := w.send(t, ctx, ch.PlaceID, w.humanA, "@Kuro（Yohaku） この件おねがいします")
	w.queue(t, ctx, ch, mention)

	status, body := callLocal(t, ctx, server.localAttention, LocalAttentionPath,
		map[string]any{}, authorization)
	if status != http.StatusOK {
		t.Fatalf("poll attention: status %d body %v", status, body)
	}
	candidates, _ := body["candidates"].([]any)
	if len(candidates) != 1 {
		t.Fatalf("candidates = %v, want the mention", body)
	}
	first, _ := candidates[0].(map[string]any)
	if first["reason"] != NotifyReasonMention {
		t.Fatalf("reason = %v, want the server's own verdict", first["reason"])
	}
	if first["message_seq"] != float64(mention.Seq) {
		t.Fatalf("message_seq = %v, want %d", first["message_seq"], mention.Seq)
	}
	// 候補は message ref であって本文の注入ではない（凍結契約 v1）。
	if _, leaked := first["content"]; leaked {
		t.Fatalf("candidate carried the message body: %v", first)
	}
	place, _ := first["place"].(map[string]any)
	if place["channel_id"] != ch.PlaceID {
		t.Fatalf("place = %v, want the channel the agent must open", place)
	}

	// ack すれば消える。二度目の poll では出てこない。
	seq := int64(first["candidate_seq"].(float64))
	status, body = callLocal(t, ctx, server.localAttention, LocalAttentionPath,
		map[string]any{"consume_through": seq}, authorization)
	if status != http.StatusOK {
		t.Fatalf("consume: status %d body %v", status, body)
	}
	if body["consumed"] != float64(1) {
		t.Fatalf("consumed = %v, want 1", body["consumed"])
	}
	if remaining, _ := body["candidates"].([]any); len(remaining) != 0 {
		t.Fatalf("candidates after ack = %v, want none", remaining)
	}
	// ack は冪等。同じ cursor をもう一度渡しても、何も二重に起きない。
	_, body = callLocal(t, ctx, server.localAttention, LocalAttentionPath,
		map[string]any{"consume_through": seq}, authorization)
	if body["consumed"] != float64(0) {
		t.Fatalf("re-ack consumed = %v, want 0", body["consumed"])
	}
}

func TestAttentionFollowsTheAgentsOwnNotificationSetting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	server := NewServer(w.store, nil)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}

	// agent は人間と同じ resource を、同じ意味で持つ。mute にした place の
	// 雑談で起こされないことが「同型」の中身である。
	status, body := callLocal(t, ctx, server.localNotificationSettings, LocalNotificationSettingsPath,
		map[string]any{"defaults_level": NotifyLevelMentions}, authorization)
	if status != http.StatusOK {
		t.Fatalf("set agent notification setting: status %d body %v", status, body)
	}

	chatter := w.send(t, ctx, ch.PlaceID, w.humanA, "今日はいい天気ですね")
	w.queue(t, ctx, ch, chatter)
	_, body = callLocal(t, ctx, server.localAttention, LocalAttentionPath, map[string]any{}, authorization)
	if candidates, _ := body["candidates"].([]any); len(candidates) != 0 {
		t.Fatalf("mentions level was woken by ambient chatter: %v", candidates)
	}

	called := w.send(t, ctx, ch.PlaceID, w.humanA, "@Kuro（Yohaku） 見てもらえますか")
	w.queue(t, ctx, ch, called)
	_, body = callLocal(t, ctx, server.localAttention, LocalAttentionPath, map[string]any{}, authorization)
	candidates, _ := body["candidates"].([]any)
	if len(candidates) != 1 {
		t.Fatalf("a mention must still get through: %v", body)
	}
}

func TestCandidateSeqIsPerAgentAndMonotonic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)

	for _, content := range []string{"ひとつめ", "ふたつめ", "みっつめ"} {
		w.queue(t, ctx, ch, w.send(t, ctx, ch.PlaceID, w.humanA, content))
	}
	candidates, err := w.store.PendingAttentionCandidates(ctx, w.agent, 0)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(candidates) != 3 {
		t.Fatalf("candidates = %d, want 3", len(candidates))
	}
	for i, candidate := range candidates {
		// place の seq とは別軸の、agent ごとの目盛り（凍結契約 v1 §2）。
		if candidate.CandidateSeq != int64(i+1) {
			t.Fatalf("candidate_seq[%d] = %d, want %d", i, candidate.CandidateSeq, i+1)
		}
	}
	latest, err := w.store.LatestAttentionSeq(ctx, w.agent)
	if err != nil {
		t.Fatalf("latest seq: %v", err)
	}
	if latest != 3 {
		t.Fatalf("latest attention seq = %d, want 3", latest)
	}
}

func TestReadingThroughSupersedesCandidatesInsteadOfWakingTwice(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)

	first := w.send(t, ctx, ch.PlaceID, w.humanA, "@Kuro（Yohaku） ひとつめ")
	w.queue(t, ctx, ch, first)
	second := w.send(t, ctx, ch.PlaceID, w.humanA, "@Kuro（Yohaku） ふたつめ")
	w.queue(t, ctx, ch, second)

	// 本人が place を読んだ。読んだところまでは「もう見た」ので、それで
	// もう一度起こす理由は無い（凍結契約 v1「read_through との連動」）。
	if err := w.store.ReadThrough(ctx, ch.PlaceID, w.agent, first.Seq); err != nil {
		t.Fatalf("read through: %v", err)
	}
	candidates, err := w.store.PendingAttentionCandidates(ctx, w.agent, 0)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(candidates) != 1 || candidates[0].MessageSeq != second.Seq {
		t.Fatalf("candidates = %+v, want only the still-unread one", candidates)
	}
}

func TestQueuingTheSameMessageTwiceCallsTheAgentOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)

	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "@Kuro（Yohaku） おねがいします")
	w.queue(t, ctx, ch, msg)
	w.queue(t, ctx, ch, msg)

	candidates, err := w.store.PendingAttentionCandidates(ctx, w.agent, 0)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidates = %d, want one call per message", len(candidates))
	}
}

func TestAgentSearchSeesOnlyWhatItCouldOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	server := NewServer(w.store, nil)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}

	w.send(t, ctx, ch.PlaceID, w.humanA, "デプロイの手順をまとめました")
	w.send(t, ctx, ch.PlaceID, w.humanB, "今日のお昼はカレーでした")

	status, body := callLocal(t, ctx, server.localSearch, LocalSearchPath,
		map[string]any{"query": "デプロイ"}, authorization)
	if status != http.StatusOK {
		t.Fatalf("agent search: status %d body %v", status, body)
	}
	results, _ := body["results"].([]any)
	if len(results) != 1 {
		t.Fatalf("results = %v, want the one matching message", body)
	}
	hit, _ := results[0].(map[string]any)
	if snippet, _ := hit["snippet"].(string); snippet == "" {
		t.Fatalf("search hit carried no snippet: %v", hit)
	}

	// 空の query は検索ではない。人間の UI と同じく 400 で断る。
	status, _ = callLocal(t, ctx, server.localSearch, LocalSearchPath,
		map[string]any{"query": "   "}, authorization)
	if status != http.StatusBadRequest {
		t.Fatalf("empty agent query status = %d, want 400", status)
	}

	// 見られない place を名指しした検索は「無い」と答える——存在を漏らさない。
	dm, _, err := w.store.EnsureDM(ctx, w.humanA, w.humanB)
	if err != nil {
		t.Fatalf("ensure dm: %v", err)
	}
	status, _ = callLocal(t, ctx, server.localSearch, LocalSearchPath,
		map[string]any{"query": "カレー", "place_id": dm.PlaceID}, authorization)
	if status != http.StatusNotFound {
		t.Fatalf("agent search into an invisible place status = %d, want 404", status)
	}
}
