package messaging

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

// askPoll sends a message carrying a poll and returns it.
func (w world) askPoll(t *testing.T, ctx context.Context, placeID string, author ParticipantRef, in PollInput) Message {
	t.Helper()
	msg, created, err := w.store.AppendMessage(ctx, AppendInput{
		PlaceID: placeID, Author: author,
		ClientNonce: fmt.Sprintf("poll-%s-%d", author.Key(), time.Now().UnixNano()),
		Poll:        &in,
	})
	if err != nil {
		t.Fatalf("ask poll: %v", err)
	}
	if !created || msg.Poll == nil {
		t.Fatalf("poll did not commit with its message: created=%v poll=%v", created, msg.Poll)
	}
	return msg
}

func voterKeys(option PollOption) []string {
	keys := make([]string, len(option.Voters))
	for i, voter := range option.Voters {
		keys[i] = voter.Key()
	}
	return keys
}

func TestPollTravelsWithItsMessageAndCountsVotes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)

	// A message may be a bare question: no text, only the poll.
	asked := w.askPoll(t, ctx, ch.PlaceID, w.humanA, PollInput{
		Question: "リリースはいつにしますか？",
		Options:  []string{"今日", "明日", "来週"},
	})
	if asked.Content != "" || len(asked.Poll.Options) != 3 {
		t.Fatalf("poll-only message = %+v", asked.Poll)
	}
	// 並びは述べた順のまま。選択肢のidはサーバーが採番する。
	if asked.Poll.Options[0].Text != "今日" || asked.Poll.Options[2].Text != "来週" {
		t.Fatalf("options lost their order: %+v", asked.Poll.Options)
	}
	if asked.Poll.Options[0].OptionID == "" {
		t.Fatal("option identity must be minted by the server")
	}

	// 読み返した履歴にも投票が乗る（別に取りに行く必要がない）。
	page, err := w.store.History(ctx, ch.PlaceID, w.humanB, HistoryOptions{Limit: 10})
	if err != nil {
		t.Fatalf("read history: %v", err)
	}
	if len(page) != 1 || page[0].Poll == nil || page[0].Poll.Question != "リリースはいつにしますか？" {
		t.Fatalf("poll missing from history: %+v", page)
	}

	today := asked.Poll.Options[0].OptionID
	tomorrow := asked.Poll.Options[1].OptionID

	voted, err := w.store.VotePoll(ctx, ch.PlaceID, asked.MessageID, w.humanB, []string{today})
	if err != nil {
		t.Fatalf("vote: %v", err)
	}
	if got := voterKeys(voted.Poll.Options[0]); len(got) != 1 || got[0] != w.humanB.Key() {
		t.Fatalf("vote did not land: %v", got)
	}

	// human と personality_agent は同じ形で投票する。
	agentVoted, err := w.store.VotePoll(ctx, ch.PlaceID, asked.MessageID, w.agent, []string{today})
	if err != nil {
		t.Fatalf("agent vote: %v", err)
	}
	if len(agentVoted.Poll.Options[0].Voters) != 2 {
		t.Fatalf("agent vote did not join the tally: %+v", agentVoted.Poll.Options[0])
	}

	// 気が変わる＝置き換え。単一選択では前の票が残らない。
	moved, err := w.store.VotePoll(ctx, ch.PlaceID, asked.MessageID, w.humanB, []string{tomorrow})
	if err != nil {
		t.Fatalf("change vote: %v", err)
	}
	if got := voterKeys(moved.Poll.Options[0]); len(got) != 1 || got[0] != w.agent.Key() {
		t.Fatalf("old vote survived the change: %v", got)
	}
	if got := voterKeys(moved.Poll.Options[1]); len(got) != 1 || got[0] != w.humanB.Key() {
		t.Fatalf("new vote did not land: %v", got)
	}

	// 取り消しは同じ呼び出し（空配列）。別の道具にはしない。
	withdrawn, err := w.store.VotePoll(ctx, ch.PlaceID, asked.MessageID, w.humanB, nil)
	if err != nil {
		t.Fatalf("withdraw: %v", err)
	}
	if len(withdrawn.Poll.Options[1].Voters) != 0 {
		t.Fatalf("withdrawal left a vote: %+v", withdrawn.Poll.Options[1])
	}

	// 単一選択に複数入れようとしたらサーバーが拒む（UIだけの制約にしない）。
	if _, err := w.store.VotePoll(
		ctx, ch.PlaceID, asked.MessageID, w.humanB, []string{today, tomorrow},
	); !errors.Is(err, ErrPollSingleChoice) {
		t.Fatalf("single-choice poll accepted two options: %v", err)
	}

	// 他のメッセージの選択肢idでは投票できない。
	other := w.askPoll(t, ctx, ch.PlaceID, w.humanA, PollInput{
		Question: "昼はどこ？", Options: []string{"社食", "外"},
	})
	if _, err := w.store.VotePoll(
		ctx, ch.PlaceID, asked.MessageID, w.humanB,
		[]string{other.Poll.Options[0].OptionID},
	); !errors.Is(err, ErrPollOptionNotFound) {
		t.Fatalf("a foreign option was accepted: %v", err)
	}

	// 投票を持たない発言に投票はできない。
	plain := w.send(t, ctx, ch.PlaceID, w.humanA, "ただの発言")
	if _, err := w.store.VotePoll(
		ctx, ch.PlaceID, plain.MessageID, w.humanB, []string{today},
	); !errors.Is(err, ErrPollNotFound) {
		t.Fatalf("voted on a message with no poll: %v", err)
	}
}

func TestMultiChoicePollKeepsEveryChoiceAndClosesOnTime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)

	asked := w.askPoll(t, ctx, ch.PlaceID, w.humanA, PollInput{
		Question: "参加できる日は？", AllowMulti: true,
		Options: []string{"月", "火", "水"},
	})
	ids := []string{
		asked.Poll.Options[0].OptionID,
		asked.Poll.Options[2].OptionID,
	}
	voted, err := w.store.VotePoll(ctx, ch.PlaceID, asked.MessageID, w.humanB, ids)
	if err != nil {
		t.Fatalf("multi vote: %v", err)
	}
	if len(voted.Poll.Options[0].Voters) != 1 || len(voted.Poll.Options[1].Voters) != 0 ||
		len(voted.Poll.Options[2].Voters) != 1 {
		t.Fatalf("multi vote landed wrong: %+v", voted.Poll.Options)
	}
	// 置き換えなので、後から述べ直した分だけが残る。
	narrowed, err := w.store.VotePoll(
		ctx, ch.PlaceID, asked.MessageID, w.humanB, ids[1:])
	if err != nil {
		t.Fatalf("narrow vote: %v", err)
	}
	if len(narrowed.Poll.Options[0].Voters) != 0 || len(narrowed.Poll.Options[2].Voters) != 1 {
		t.Fatalf("restated choice did not replace the old one: %+v", narrowed.Poll.Options)
	}

	// 締切を過ぎたら結果だけ。あとから直せる投票は当時の記録にならない。
	closesAt := time.Now().Add(time.Hour)
	timed := w.askPoll(t, ctx, ch.PlaceID, w.humanA, PollInput{
		Question: "続けますか？", Options: []string{"はい", "いいえ"},
		ClosesAt: &closesAt,
	})
	if timed.Poll.ClosesAt == nil {
		t.Fatal("deadline did not persist")
	}
	if _, err := w.store.pool.Exec(ctx,
		"UPDATE message_polls SET closes_at = now() - interval '1 minute' WHERE message_id = $1",
		timed.MessageID); err != nil {
		t.Fatalf("age the deadline: %v", err)
	}
	if _, err := w.store.VotePoll(
		ctx, ch.PlaceID, timed.MessageID, w.humanB,
		[]string{timed.Poll.Options[0].OptionID},
	); !errors.Is(err, ErrPollClosed) {
		t.Fatalf("closed poll accepted a vote: %v", err)
	}
	// 締め切っても結果は残る。
	page, err := w.store.History(ctx, ch.PlaceID, w.humanB, HistoryOptions{Limit: 10})
	if err != nil {
		t.Fatalf("read history: %v", err)
	}
	for _, msg := range page {
		if msg.MessageID == timed.MessageID && msg.Poll == nil {
			t.Fatal("a closed poll must still show its result")
		}
	}
}

func TestPollValidationAndTombstone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)

	past := time.Now().Add(-time.Minute)
	for name, in := range map[string]PollInput{
		"no question":      {Question: "  ", Options: []string{"a", "b"}},
		"one option":       {Question: "?", Options: []string{"a"}},
		"same options":     {Question: "?", Options: []string{"a", " a "}},
		"blank option":     {Question: "?", Options: []string{"a", "  "}},
		"deadline in past": {Question: "?", Options: []string{"a", "b"}, ClosesAt: &past},
	} {
		poll := in
		if _, _, err := w.store.AppendMessage(ctx, AppendInput{
			PlaceID: ch.PlaceID, Author: w.humanA,
			ClientNonce: "bad-" + name, Poll: &poll,
		}); !errors.Is(err, ErrInvalidPoll) {
			t.Fatalf("%s: expected ErrInvalidPoll, got %v", name, err)
		}
	}

	// 発言が消えれば、問いも票も一緒に消える。事実とseqだけが残る。
	asked := w.askPoll(t, ctx, ch.PlaceID, w.humanA, PollInput{
		Question: "消えますか？", Options: []string{"はい", "いいえ"},
	})
	if _, err := w.store.VotePoll(
		ctx, ch.PlaceID, asked.MessageID, w.humanB,
		[]string{asked.Poll.Options[0].OptionID}); err != nil {
		t.Fatalf("vote: %v", err)
	}
	deleted, err := w.store.DeleteMessage(ctx, ch.PlaceID, asked.MessageID, w.humanA)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if deleted.Poll != nil || !deleted.Deleted {
		t.Fatalf("tombstone kept the poll: %+v", deleted)
	}
	var votes int
	if err := w.store.pool.QueryRow(ctx,
		"SELECT count(*) FROM message_poll_votes WHERE message_id = $1",
		asked.MessageID).Scan(&votes); err != nil {
		t.Fatalf("count votes: %v", err)
	}
	if votes != 0 {
		t.Fatalf("a vanished question kept %d votes", votes)
	}
}

// TestLocalPollOpsMatchWhatTheScreenOffers pins the AX symmetry: asking a
// question of the room and answering one are the same two acts a human has in
// the UI, reached over the agent's local-control lane and backed by the same
// store calls (AX: UIだけにある操作を作らない).
func TestLocalPollOpsMatchWhatTheScreenOffers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	server := NewServer(w.store, nil)
	if err := w.store.EnsureDefaultWorkspaceMembership(ctx, w.humanA); err != nil {
		t.Fatalf("admit human: %v", err)
	}
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}

	status, body := callLocal(t, ctx, server.localCreatePoll, LocalCreatePollPath, map[string]any{
		"place_id": DefaultGeneralChannelID, "question": "リリースはいつ？",
		"options": []string{"今日", "明日"}, "client_nonce": "agent-poll-1",
		"closes_in_minutes": 60,
	}, authorization)
	if status != http.StatusCreated {
		t.Fatalf("agent create poll: status %d body %v", status, body)
	}
	messageID, _ := body["message_id"].(string)
	poll, ok := body["message"].(map[string]any)["poll"].(map[string]any)
	if !ok || poll["question"] != "リリースはいつ？" {
		t.Fatalf("poll did not ride the created message: %v", body["message"])
	}
	if poll["closes_at"] == nil {
		t.Fatal("relative minutes must become a deadline on the server's clock")
	}
	options := poll["options"].([]any)
	if len(options) != 2 {
		t.Fatalf("options = %v", options)
	}
	optionID := options[0].(map[string]any)["option_id"].(string)

	// The same client_nonce is the same question, not a second one.
	status, _ = callLocal(t, ctx, server.localCreatePoll, LocalCreatePollPath, map[string]any{
		"place_id": DefaultGeneralChannelID, "question": "リリースはいつ？",
		"options": []string{"今日", "明日"}, "client_nonce": "agent-poll-1",
	}, authorization)
	if status != http.StatusOK {
		t.Fatalf("idempotent re-ask: status %d", status)
	}

	// A human answers the agent's question over the same store call the REST
	// route uses, and the agent answers its own.
	if _, err := w.store.VotePoll(
		ctx, DefaultGeneralChannelID, messageID, w.humanA, []string{optionID}); err != nil {
		t.Fatalf("human vote: %v", err)
	}
	status, body = callLocal(t, ctx, server.localVotePoll, LocalVotePollPath, map[string]any{
		"place_id": DefaultGeneralChannelID, "message_id": messageID,
		"option_ids": []string{optionID},
	}, authorization)
	if status != http.StatusOK {
		t.Fatalf("agent vote: status %d body %v", status, body)
	}
	voters := body["message"].(map[string]any)["poll"].(map[string]any)["options"].([]any)[0].(map[string]any)["voters"].([]any)
	if len(voters) != 2 {
		t.Fatalf("human and agent votes must share one tally: %v", voters)
	}

	// An empty list is a withdrawal, over this lane too.
	status, body = callLocal(t, ctx, server.localVotePoll, LocalVotePollPath, map[string]any{
		"place_id": DefaultGeneralChannelID, "message_id": messageID,
		"option_ids": []string{},
	}, authorization)
	if status != http.StatusOK {
		t.Fatalf("agent withdraw: status %d body %v", status, body)
	}
	voters = body["message"].(map[string]any)["poll"].(map[string]any)["options"].([]any)[0].(map[string]any)["voters"].([]any)
	if len(voters) != 1 {
		t.Fatalf("withdrawal took someone else's vote: %v", voters)
	}

	// A malformed question is refused before it reaches the room.
	status, _ = callLocal(t, ctx, server.localCreatePoll, LocalCreatePollPath, map[string]any{
		"place_id": DefaultGeneralChannelID, "question": "?",
		"options": []string{"ひとつ"}, "client_nonce": "agent-poll-2",
	}, authorization)
	if status != http.StatusBadRequest {
		t.Fatalf("one-option poll: status %d, want 400", status)
	}
}

// TestPollOverHTTPRidesTheOrdinarySend covers the browser's whole path: the
// poll is stated on the same send that carries the message, and the vote route
// restates the viewer's choice. Sending the poll and dropping it on the way to
// the store would leave a question no one can see.
func TestPollOverHTTPRidesTheOrdinarySend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)
	if err := w.store.EnsureDefaultWorkspaceMembership(ctx, w.humanA); err != nil {
		t.Fatalf("admit human A: %v", err)
	}
	if err := w.store.EnsureDefaultWorkspaceMembership(ctx, w.humanB); err != nil {
		t.Fatalf("admit human B: %v", err)
	}
	messages := "/messaging/places/" + DefaultGeneralChannelID + "/messages"

	// A poll-only send: no text, only the question.
	resp, body := call(t, ts, http.MethodPost, messages, w.humanA.ID, map[string]any{
		"client_nonce": "web-poll-1",
		"poll": map[string]any{
			"question": "リリースはいつ？", "allow_multi": false,
			"options": []string{"今日", "明日"},
		},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("send poll: status %d body %v", resp.StatusCode, body)
	}
	messageID := body["message_id"].(string)
	history, err := w.store.History(ctx, DefaultGeneralChannelID, w.humanA, HistoryOptions{})
	if err != nil || len(history) != 1 || history[0].Poll == nil {
		t.Fatalf("poll was dropped between request and store: %#v, err %v", history, err)
	}
	poll := history[0].Poll
	if poll.Question != "リリースはいつ？" || poll.AllowMulti {
		t.Fatalf("poll = %#v", poll)
	}
	today := poll.Options[0].OptionID
	tomorrow := poll.Options[1].OptionID

	// An ordinary message carries no poll key at all.
	_, plain := call(t, ts, http.MethodPost, messages, w.humanA.ID, map[string]any{
		"content": "ただの発言", "client_nonce": "web-plain-1",
	})
	plainID, _ := plain["message_id"].(string)
	history, err = w.store.History(ctx, DefaultGeneralChannelID, w.humanA, HistoryOptions{})
	if err != nil {
		t.Fatalf("history after plain message: %v", err)
	}
	for _, message := range history {
		if message.MessageID == plainID && message.Poll != nil {
			t.Fatal("a message that asks nothing must not carry a poll")
		}
	}

	vote := "/messaging/places/" + DefaultGeneralChannelID + "/messages/" + messageID + "/poll/vote"
	resp, body = call(t, ts, http.MethodPost, vote, w.humanB.ID, map[string]any{
		"option_ids": []string{today},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("vote: status %d body %v", resp.StatusCode, body)
	}
	voted := body["message"].(map[string]any)["poll"].(map[string]any)["options"].([]any)
	voters := voted[0].(map[string]any)["voters"].([]any)
	if len(voters) != 1 || voters[0].(map[string]any)["human_id"] != w.humanB.ID {
		t.Fatalf("vote did not land on the wire: %v", voters)
	}

	// A single-choice poll refuses two options at the route, not only in the UI.
	resp, body = call(t, ts, http.MethodPost, vote, w.humanB.ID, map[string]any{
		"option_ids": []string{today, tomorrow},
	})
	if resp.StatusCode != http.StatusBadRequest || body["error"] != "poll_single_choice" {
		t.Fatalf("two options on a single-choice poll: status %d body %v", resp.StatusCode, body)
	}

	// A malformed poll is refused before a message is committed.
	resp, body = call(t, ts, http.MethodPost, messages, w.humanA.ID, map[string]any{
		"client_nonce": "web-poll-2",
		"poll":         map[string]any{"question": "?", "options": []string{"ひとつ"}},
	})
	if resp.StatusCode != http.StatusBadRequest || body["error"] != "invalid_poll" {
		t.Fatalf("one-option poll: status %d body %v", resp.StatusCode, body)
	}

	// Voting on a message with no poll is a 404, not a silent success.
	noPoll := "/messaging/places/" + DefaultGeneralChannelID + "/messages/" +
		plain["message_id"].(string) + "/poll/vote"
	resp, body = call(t, ts, http.MethodPost, noPoll, w.humanB.ID, map[string]any{
		"option_ids": []string{today},
	})
	if resp.StatusCode != http.StatusNotFound || body["error"] != "poll_not_found" {
		t.Fatalf("vote on a plain message: status %d body %v", resp.StatusCode, body)
	}
}
