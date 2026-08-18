package messaging

import (
	"context"
	"encoding/json"
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
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID, TenantID: "tenant-1"}
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
	workspace, ch := w.workspaceWithChannel(t, ctx)
	server := NewServer(w.store.core, nil)

	mention := w.send(t, ctx, ch.PlaceID, w.humanA, "@Kuro（Yohaku） この件おねがいします")

	body := w.poll(t, ctx, server, map[string]any{})
	candidates := candidateList(t, body)
	if len(candidates) != 1 {
		t.Fatalf("candidates = %v, want the mention", body)
	}
	first := candidates[0]
	if first["kind"] != "attention_candidate" {
		t.Fatalf("kind = %v, want attention_candidate", first["kind"])
	}
	provenance, _ := first["provenance"].(map[string]any)
	if provenance["version"] != float64(1) || provenance["tenant_id"] != "tenant-1" ||
		provenance["personality_agent_id"] != w.agent.ID {
		t.Fatalf("provenance header = %v", provenance)
	}
	source, _ := provenance["source"].(map[string]any)
	if source["surface"] != "messaging" || source["workspace_id"] != workspace.WorkspaceID {
		t.Fatalf("provenance source = %v", source)
	}
	place, _ := source["place"].(map[string]any)
	if place["channel_id"] != ch.PlaceID {
		t.Fatalf("provenance place = %v, want the channel the agent must open", place)
	}
	delivery, _ := source["delivery"].(map[string]any)
	if delivery["message_id"] != mention.MessageID || delivery["seq"] != float64(mention.Seq) ||
		delivery["trigger_reason"] != "mention" || delivery["urgency"] != UrgencyNormal {
		t.Fatalf("provenance delivery = %v", delivery)
	}
	actor, _ := provenance["actor"].(map[string]any)
	if actor["kind"] != "human" || actor["human_id"] != w.humanA.ID {
		t.Fatalf("provenance actor = %v", actor)
	}
	unreadRange, _ := first["unread_range"].(map[string]any)
	if unreadRange["place_seq_from"] != float64(1) || unreadRange["place_seq_to"] != float64(mention.Seq) {
		t.Fatalf("unread_range = %v", unreadRange)
	}
	// 候補は message ref を provenance に持つが、本文を注入しない。
	for _, leaked := range []string{"content", "place", "message_seq", "reason", "message_id", "author"} {
		if _, present := first[leaked]; present {
			t.Fatalf("candidate carried %q: %v", leaked, first)
		}
	}
	if _, present := delivery["content"]; present {
		t.Fatalf("provenance delivery leaked content: %v", delivery)
	}
	// The actual local-control response—not merely the fixture—must decode as
	// the frozen contract's Go type.
	wireJSON, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("marshal attention candidate: %v", err)
	}
	var frozen agentevents.AttentionCandidate
	if err := json.Unmarshal(wireJSON, &frozen); err != nil {
		t.Fatalf("decode frozen attention candidate: %v", err)
	}
	if err := frozen.Provenance.Validate(); err != nil {
		t.Fatalf("validate frozen candidate provenance: %v", err)
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

func TestCandidateSeqIsPerAgentAcrossWorkspacesAndMonotonic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	firstWorkspace, firstChannel := w.workspaceWithChannel(t, ctx)
	secondWorkspace, secondChannel := w.workspaceWithChannel(t, ctx)
	w.send(t, ctx, firstChannel.PlaceID, w.humanA, "最初の Workspace です")
	w.send(t, ctx, secondChannel.PlaceID, w.humanA, "次の Workspace です")

	firstInbox, err := w.store.mustScope(t, ctx, firstWorkspace.WorkspaceID, w.agent).
		PollAttentionCandidates(ctx, 0, 1)
	if err != nil {
		t.Fatalf("poll first workspace: %v", err)
	}
	secondInbox, err := w.store.mustScope(t, ctx, secondWorkspace.WorkspaceID, w.agent).
		PollAttentionCandidates(ctx, 0, 1)
	if err != nil {
		t.Fatalf("poll second workspace: %v", err)
	}
	if len(firstInbox.Candidates) != 1 || len(secondInbox.Candidates) != 1 {
		t.Fatalf("candidates = first %v, second %v", firstInbox.Candidates, secondInbox.Candidates)
	}
	if firstInbox.Candidates[0].CandidateSeq != 1 || secondInbox.Candidates[0].CandidateSeq != 2 {
		t.Fatalf("candidate sequences = first %d, second %d; want the one agent-wide sequence 1, 2",
			firstInbox.Candidates[0].CandidateSeq, secondInbox.Candidates[0].CandidateSeq)
	}
	if secondInbox.LatestSeq != 2 {
		t.Fatalf("second Workspace delivery high-water = %d, want 2", secondInbox.LatestSeq)
	}
	if firstInbox.LatestSeq != 1 {
		t.Fatalf("first Workspace delivery high-water = %d, want 1", firstInbox.LatestSeq)
	}
}

func TestAttentionAckCannotConsumeAnotherWorkspaceAfterLostResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	firstWorkspace, firstChannel := w.workspaceWithChannel(t, ctx)
	secondWorkspace, secondChannel := w.workspaceWithChannel(t, ctx)
	w.send(t, ctx, firstChannel.PlaceID, w.humanA, "A だけの呼びかけ")
	w.send(t, ctx, secondChannel.PlaceID, w.humanA, "B だけの呼びかけ")

	firstScope := w.store.mustScope(t, ctx, firstWorkspace.WorkspaceID, w.agent)
	secondScope := w.store.mustScope(t, ctx, secondWorkspace.WorkspaceID, w.agent)
	// A の poll は DB commit の後に応答を落とした、とする。候補を返した事実だけを
	// 残し、caller は cursor を受け取らなかったため ack しない。
	firstInbox, err := firstScope.PollAttentionCandidates(ctx, 0, 1)
	if err != nil || len(firstInbox.Candidates) != 1 || firstInbox.Candidates[0].CandidateSeq != 1 {
		t.Fatalf("first poll = %+v, %v; want A candidate 1", firstInbox, err)
	}

	secondInbox, err := secondScope.PollAttentionCandidates(ctx, 0, 1)
	if err != nil || len(secondInbox.Candidates) != 1 || secondInbox.Candidates[0].CandidateSeq != 2 {
		t.Fatalf("second poll = %+v, %v; want B candidate 2", secondInbox, err)
	}
	// B が自身の latest_seq を ack しても、A が配布した候補には触れない。
	acked, err := secondScope.PollAttentionCandidates(ctx, secondInbox.LatestSeq, 1)
	if err != nil || acked.Consumed != 1 {
		t.Fatalf("ack second Workspace = %+v, %v; want one consumed", acked, err)
	}

	firstRetry, err := firstScope.PollAttentionCandidates(ctx, 0, 1)
	if err != nil {
		t.Fatalf("retry first Workspace: %v", err)
	}
	if len(firstRetry.Candidates) != 1 || firstRetry.Candidates[0].CandidateSeq != 1 {
		t.Fatalf("A candidate was consumed by B ack: %+v", firstRetry)
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

	// limit で切った poll。採番も配布枠に限るので、本人が受け取ったのは一つだけ。
	body := w.poll(t, ctx, server, map[string]any{"limit": 1})
	candidates := candidateList(t, body)
	if len(candidates) != 1 || candidates[0]["candidate_seq"] != float64(1) {
		t.Fatalf("limited poll = %v, want only the first candidate", candidates)
	}
	// latest_seq は「配られたところまで」。ここが未配布まで先行すると、素直に
	// ack した本人が、見ていない二つを永久に落とす。
	if body["latest_seq"] != float64(1) {
		t.Fatalf("latest_seq = %v, want 1 — the last candidate actually handed over", body["latest_seq"])
	}

	// 任意に大きい cursor でも server は配布済み高水位へ clamp する。残りは
	// 消えず、次の poll で返さなければならない。
	body = w.poll(t, ctx, server, map[string]any{"consume_through": 200, "limit": 1})
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
	if len(candidates) != 1 {
		t.Fatalf("candidates = %+v, want only the still-unread one", candidates)
	}
	provenance, _ := candidates[0]["provenance"].(map[string]any)
	source, _ := provenance["source"].(map[string]any)
	delivery, _ := source["delivery"].(map[string]any)
	if delivery["seq"] != float64(second.Seq) {
		t.Fatalf("candidates = %+v, want only the still-unread one", candidates)
	}
}

func TestAlreadyReadIntentIsNotNumbered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, ch := w.workspaceWithChannel(t, ctx)
	message := w.send(t, ctx, ch.PlaceID, w.humanA, "@Kuro（Yohaku） もう読みました")
	if err := w.store.ReadThrough(ctx, ch.PlaceID, w.agent, message.Seq); err != nil {
		t.Fatalf("read through before first poll: %v", err)
	}

	inbox, err := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent).
		PollAttentionCandidates(ctx, 0, 1)
	if err != nil {
		t.Fatalf("poll after read through: %v", err)
	}
	if len(inbox.Candidates) != 0 || inbox.LatestSeq != 0 {
		t.Fatalf("already-read message was offered or sequenced: %+v", inbox)
	}
	var candidates int
	if err := w.store.core.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM attention_candidates WHERE agent_id = $1 AND message_id = $2`,
		w.agent.ID, message.MessageID).Scan(&candidates); err != nil {
		t.Fatalf("count candidates: %v", err)
	}
	if candidates != 0 {
		t.Fatalf("already-read message was numbered: %d rows", candidates)
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
