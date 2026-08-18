package messaging

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

// poll runs the agent's own local-control call, so these tests exercise the
// real path from「呼ぶと決めた」（message と同じ transaction で確定した intent）
// to「本人が候補として受け取った」rather than a hand-written insert.
func (w world) poll(
	t *testing.T, ctx context.Context, server *Server, body map[string]any,
) map[string]any {
	t.Helper()
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}
	status, decoded := callLocal(t, ctx, server.localAttention, LocalAttentionPath, body, authorization)
	if status != http.StatusOK {
		t.Fatalf("poll attention: status %d body %v", status, decoded)
	}
	return decoded
}

func candidateList(t *testing.T, body map[string]any) []map[string]any {
	t.Helper()
	raw, _ := body["candidates"].([]any)
	out := make([]map[string]any, 0, len(raw))
	for _, entry := range raw {
		item, ok := entry.(map[string]any)
		if !ok {
			t.Fatalf("candidate is not an object: %v", entry)
		}
		out = append(out, item)
	}
	return out
}

func TestMentioningAnAgentQueuesACandidateItCanTakeIn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	server := NewServer(w.store.core, nil)

	mention := w.send(t, ctx, ch.PlaceID, w.humanA, "@Kuro（Yohaku） この件おねがいします")

	body := w.poll(t, ctx, server, map[string]any{})
	candidates := candidateList(t, body)
	if len(candidates) != 1 {
		t.Fatalf("candidates = %v, want the mention", body)
	}
	first := candidates[0]
	if first["reason"] != NotifyReasonMention {
		t.Fatalf("reason = %v, want the server's own verdict", first["reason"])
	}
	if first["message_seq"] != float64(mention.Seq) {
		t.Fatalf("message_seq = %v, want %d", first["message_seq"], mention.Seq)
	}
	// 候補は message ref であって本文の注入ではない（凍結契約 v1）。
	for _, leaked := range []string{"content", "author", "message_id"} {
		if _, present := first[leaked]; present {
			t.Fatalf("candidate carried %q: %v", leaked, first)
		}
	}
	place, _ := first["place"].(map[string]any)
	if place["channel_id"] != ch.PlaceID {
		t.Fatalf("place = %v, want the channel the agent must open", place)
	}

	// ack すれば消える。二度目の poll では出てこない。
	seq := int64(first["candidate_seq"].(float64))
	body = w.poll(t, ctx, server, map[string]any{"consume_through": seq})
	if body["consumed"] != float64(1) {
		t.Fatalf("consumed = %v, want 1", body["consumed"])
	}
	if remaining := candidateList(t, body); len(remaining) != 0 {
		t.Fatalf("candidates after ack = %v, want none", remaining)
	}
	// ack は冪等。同じ cursor をもう一度渡しても、何も二重に起きない。
	body = w.poll(t, ctx, server, map[string]any{"consume_through": seq})
	if body["consumed"] != float64(0) {
		t.Fatalf("re-ack consumed = %v, want 0", body["consumed"])
	}
	if body["latest_seq"] != float64(seq) {
		t.Fatalf("latest_seq = %v, want the highest number ever issued (%d)", body["latest_seq"], seq)
	}
}

func TestAttentionFollowsTheAgentsOwnNotificationSetting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	server := NewServer(w.store.core, nil)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}

	// agent は人間と同じ resource を、同じ意味で持つ。mentions にした本人が
	// 雑談で起こされないことが「同型」の中身である。
	status, body := callLocal(t, ctx, server.localNotificationSettings, LocalNotificationSettingsPath,
		map[string]any{"defaults_level": NotifyLevelMentions}, authorization)
	if status != http.StatusOK {
		t.Fatalf("set agent notification setting: status %d body %v", status, body)
	}

	w.send(t, ctx, ch.PlaceID, w.humanA, "今日はいい天気ですね")
	if candidates := candidateList(t, w.poll(t, ctx, server, map[string]any{})); len(candidates) != 0 {
		t.Fatalf("mentions level was woken by ambient chatter: %v", candidates)
	}

	w.send(t, ctx, ch.PlaceID, w.humanA, "@Kuro（Yohaku） 見てもらえますか")
	if candidates := candidateList(t, w.poll(t, ctx, server, map[string]any{})); len(candidates) != 1 {
		t.Fatalf("a mention must still get through: %v", candidates)
	}
}

func TestCandidateSeqIsPerAgentAndMonotonic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	server := NewServer(w.store.core, nil)

	for _, content := range []string{"ひとつめ", "ふたつめ"} {
		w.send(t, ctx, ch.PlaceID, w.humanA, content)
	}
	body := w.poll(t, ctx, server, map[string]any{})
	candidates := candidateList(t, body)
	if len(candidates) != 2 {
		t.Fatalf("candidates = %d, want 2", len(candidates))
	}
	for i, candidate := range candidates {
		// place の seq とは別軸の、agent ごとの目盛り（凍結契約 v1 §2）。
		if candidate["candidate_seq"] != float64(i+1) {
			t.Fatalf("candidate_seq[%d] = %v, want %d", i, candidate["candidate_seq"], i+1)
		}
	}

	// 番号は「本人が受け取った順」に振られる。後から届いたものは、既に配った
	// どの番号よりも後ろに並ぶ——cursor が未読を飛び越さない、ということ。
	w.send(t, ctx, ch.PlaceID, w.humanA, "みっつめ")
	body = w.poll(t, ctx, server, map[string]any{})
	candidates = candidateList(t, body)
	if len(candidates) != 3 || candidates[2]["candidate_seq"] != float64(3) {
		t.Fatalf("later arrivals are numbered after the earlier ones: %v", candidates)
	}
	if body["latest_seq"] != float64(3) {
		t.Fatalf("latest attention seq = %v, want 3", body["latest_seq"])
	}

	// 番号の軸は agent であって place ではない。別の place の呼びかけも同じ
	// 目盛りの続きになる。
	dm, _, err := w.store.EnsureDM(ctx, w.humanA, w.agent)
	if err != nil {
		t.Fatalf("ensure dm: %v", err)
	}
	w.send(t, ctx, dm.PlaceID, w.humanA, "こちらもおねがい")
	candidates = candidateList(t, w.poll(t, ctx, server, map[string]any{}))
	if len(candidates) != 4 || candidates[3]["candidate_seq"] != float64(4) {
		t.Fatalf("a second place restarted the numbering: %v", candidates)
	}
}

func TestLatestSeqNeverRunsAheadOfWhatThePollActuallyHandedOver(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	server := NewServer(w.store.core, nil)

	for _, content := range []string{"ひとつめ", "ふたつめ", "みっつめ"} {
		w.send(t, ctx, ch.PlaceID, w.humanA, content)
	}

	// limit で切った poll。採番は三つとも済むが、本人が受け取ったのは一つ。
	body := w.poll(t, ctx, server, map[string]any{"limit": 1})
	candidates := candidateList(t, body)
	if len(candidates) != 1 || candidates[0]["candidate_seq"] != float64(1) {
		t.Fatalf("limited poll = %v, want only the first candidate", candidates)
	}
	// latest_seq は「配られたところまで」。ここが採番済みの最大（3）になると、
	// 素直に ack した本人が、見ていない二つを永久に落とす。
	if body["latest_seq"] != float64(1) {
		t.Fatalf("latest_seq = %v, want 1 — the last candidate actually handed over", body["latest_seq"])
	}

	// 本人はその latest_seq を信じて ack する。残りは残っていなければならない。
	body = w.poll(t, ctx, server, map[string]any{"consume_through": body["latest_seq"], "limit": 1})
	if body["consumed"] != float64(1) {
		t.Fatalf("consumed = %v, want the single acked candidate", body["consumed"])
	}
	candidates = candidateList(t, body)
	if len(candidates) != 1 || candidates[0]["candidate_seq"] != float64(2) {
		t.Fatalf("after acking the first, the poll must offer the second: %v", candidates)
	}
	if body["latest_seq"] != float64(2) {
		t.Fatalf("latest_seq = %v, want 2", body["latest_seq"])
	}

	// 最後まで取り込むと、返すものが無くなる。そのときの latest_seq は
	// ack 済みの最大——後戻りしないが、未配布を含みもしない。
	body = w.poll(t, ctx, server, map[string]any{"consume_through": float64(2)})
	candidates = candidateList(t, body)
	if len(candidates) != 1 || candidates[0]["candidate_seq"] != float64(3) {
		t.Fatalf("the third candidate must still be waiting: %v", candidates)
	}
	body = w.poll(t, ctx, server, map[string]any{"consume_through": float64(3)})
	if len(candidateList(t, body)) != 0 {
		t.Fatalf("everything was acked, yet the poll still offers candidates: %v", body)
	}
	if body["latest_seq"] != float64(3) {
		t.Fatalf("empty poll latest_seq = %v, want the acked high-water mark 3", body["latest_seq"])
	}
}

func TestReadingThroughSupersedesCandidatesInsteadOfWakingTwice(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	server := NewServer(w.store.core, nil)

	first := w.send(t, ctx, ch.PlaceID, w.humanA, "@Kuro（Yohaku） ひとつめ")
	second := w.send(t, ctx, ch.PlaceID, w.humanA, "@Kuro（Yohaku） ふたつめ")

	// 本人が place を読んだ。読んだところまでは「もう見た」ので、それで
	// もう一度起こす理由は無い（凍結契約 v1「read_through との連動」）。
	if err := w.store.ReadThrough(ctx, ch.PlaceID, w.agent, first.Seq); err != nil {
		t.Fatalf("read through: %v", err)
	}
	candidates := candidateList(t, w.poll(t, ctx, server, map[string]any{}))
	if len(candidates) != 1 || candidates[0]["message_seq"] != float64(second.Seq) {
		t.Fatalf("candidates = %+v, want only the still-unread one", candidates)
	}
}

func TestAnAgentNeverSeesAnotherParticipantsCandidates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	server := NewServer(w.store.core, nil)

	// 人間宛の呼びかけは agent の inbox に現れない。ここが漏れると、
	// 「誰が呼ばれたか」が本人以外に見えることになる。
	w.send(t, ctx, ch.PlaceID, w.humanA, "@Haru（Yohaku） おねがい")
	if candidates := candidateList(t, w.poll(t, ctx, server, map[string]any{})); len(candidates) != 1 {
		// channel の既定は all なので agent 自身にも一件は来る。
		t.Fatalf("candidates = %v, want only the agent's own", candidates)
	}

	// 人間が同じ経路を叩いても候補は持てない（対応物は push 購読である）。
	scoped := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanA)
	if _, err := scoped.PollAttentionCandidates(ctx, 0, 0); err == nil {
		t.Fatal("a Human polled an attention inbox")
	}
}

func TestAgentSearchSeesOnlyWhatItCouldOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	server := NewServer(w.store.core, nil)
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
