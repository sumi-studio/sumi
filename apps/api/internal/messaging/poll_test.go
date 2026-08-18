package messaging

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestPollLifecycleUsesMessageTransactionAndWholeVoteReplacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	a := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	b := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	closesAt := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
	input := AppendInput{
		PlaceID: channel.PlaceID, Content: "候補を決めます", ClientNonce: "poll-once",
		Poll: &PollInput{Question: "いつ？", Options: []string{"今日", "明日", "来週"}, ClosesAt: &closesAt},
	}
	message, created, err := a.AppendMessage(ctx, input)
	if err != nil || !created {
		t.Fatalf("create poll: created=%v err=%v", created, err)
	}
	if message.Poll == nil || len(message.Poll.Options) != 3 {
		t.Fatalf("created poll = %+v", message.Poll)
	}
	for _, option := range message.Poll.Options {
		if option.OptionID == "" {
			t.Fatal("server omitted an option id")
		}
	}
	again, created, err := a.AppendMessage(ctx, input)
	if err != nil || created || again.MessageID != message.MessageID {
		t.Fatalf("idempotent poll retry = %+v created=%v err=%v", again, created, err)
	}

	first := message.Poll.Options[0].OptionID
	second := message.Poll.Options[1].OptionID
	voted, err := b.VotePoll(ctx, channel.PlaceID, message.MessageID, []string{first})
	if err != nil || voted.Poll.Revision != 1 || len(voted.Poll.Options[0].Voters) != 1 {
		t.Fatalf("first vote = %+v err=%v", voted.Poll, err)
	}
	if _, err := b.VotePoll(ctx, channel.PlaceID, message.MessageID, []string{first, second}); !errors.Is(err, ErrPollSingleChoice) {
		t.Fatalf("single-choice multi vote error = %v", err)
	}
	voted, err = b.VotePoll(ctx, channel.PlaceID, message.MessageID, []string{second})
	if err != nil || voted.Poll.Revision != 2 || len(voted.Poll.Options[0].Voters) != 0 || len(voted.Poll.Options[1].Voters) != 1 {
		t.Fatalf("replacement vote = %+v err=%v", voted.Poll, err)
	}
	voted, err = b.VotePoll(ctx, channel.PlaceID, message.MessageID, nil)
	if err != nil || voted.Poll.Revision != 3 {
		t.Fatalf("withdraw vote: %v", err)
	}
	for _, option := range voted.Poll.Options {
		if len(option.Voters) != 0 {
			t.Fatalf("withdraw left voters: %+v", voted.Poll)
		}
	}
}

func TestPollRetryAfterClosingTimeReturnsOriginalReceipt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	a := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	closesAt := time.Now().Add(100 * time.Millisecond).UTC().Truncate(time.Microsecond)
	input := AppendInput{
		PlaceID: channel.PlaceID, ClientNonce: "retry-after-close",
		Poll: &PollInput{Question: "締切後も再試行できる？", Options: []string{"はい", "いいえ"}, ClosesAt: &closesAt},
	}
	created, didCreate, err := a.AppendMessage(ctx, input)
	if err != nil || !didCreate {
		t.Fatalf("create poll: created=%v err=%v", didCreate, err)
	}
	time.Sleep(time.Until(closesAt) + 20*time.Millisecond)
	replayed, didCreate, err := a.AppendMessage(ctx, input)
	if err != nil || didCreate || replayed.MessageID != created.MessageID {
		t.Fatalf("retry after close = %+v created=%v err=%v", replayed, didCreate, err)
	}
}

func TestPollInputRejectsNULText(t *testing.T) {
	for name, input := range map[string]PollInput{
		"question": {Question: "bad\x00question", Options: []string{"A", "B"}},
		"option":   {Question: "question", Options: []string{"bad\x00option", "B"}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := input.Validate(time.Now()); !errors.Is(err, ErrInvalidPoll) {
				t.Fatalf("Validate() error = %v, want invalid poll", err)
			}
		})
	}
}

func TestPollDeadlineProjectionPreservationAndTombstone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	a := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	closesAt := time.Now().Add(150 * time.Millisecond)
	message, _, err := a.AppendMessage(ctx, AppendInput{
		PlaceID: channel.PlaceID, ClientNonce: "closing-poll",
		Poll: &PollInput{Question: "締切？", Options: []string{"はい", "いいえ"}, AllowMulti: true, ClosesAt: &closesAt},
	})
	if err != nil {
		t.Fatal(err)
	}
	edited, err := a.EditMessage(ctx, channel.PlaceID, message.MessageID, "編集後も残る")
	if err != nil || edited.Poll == nil {
		t.Fatalf("edit lost poll: %+v err=%v", edited, err)
	}
	if _, _, err := a.ToggleReactionIdempotent(ctx, channel.PlaceID, message.MessageID, "👍", "poll-reaction"); err != nil {
		t.Fatalf("react: %v", err)
	}
	history, err := a.History(ctx, channel.PlaceID, HistoryOptions{})
	if err != nil || len(history) != 1 || history[0].Poll == nil {
		t.Fatalf("reaction path lost poll: %+v err=%v", history, err)
	}
	time.Sleep(time.Until(closesAt) + 20*time.Millisecond)
	if _, err := a.VotePoll(ctx, channel.PlaceID, message.MessageID, []string{message.Poll.Options[0].OptionID}); !errors.Is(err, ErrPollClosed) {
		t.Fatalf("closed poll vote error = %v", err)
	}
	tombstone, err := a.DeleteMessage(ctx, channel.PlaceID, message.MessageID)
	if err != nil || !tombstone.Deleted || tombstone.Poll != nil {
		t.Fatalf("poll tombstone = %+v err=%v", tombstone, err)
	}
	history, err = a.History(ctx, channel.PlaceID, HistoryOptions{})
	if err != nil || len(history) != 1 || history[0].Poll != nil {
		t.Fatalf("reloaded tombstone = %+v err=%v", history, err)
	}
}

func TestPollHTTPCreateAndVoteReturnWholeMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, server := newTestServer(t, ctx)
	path := "/messaging/places/" + DefaultGeneralChannelID + "/messages"
	resp, body := call(t, server, http.MethodPost, path, w.humanA.ID, map[string]any{
		"content": "", "client_nonce": "http-poll", "poll": map[string]any{
			"question": "どちら？", "options": []string{"A", "B"}, "allow_multi": false,
		},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status %d body %v", resp.StatusCode, body)
	}
	messageID := body["message_id"].(string)
	resp, body = call(t, server, http.MethodGet, path, w.humanA.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("open status %d body %v", resp.StatusCode, body)
	}
	messages := body["messages"].([]any)
	message := messages[len(messages)-1].(map[string]any)
	poll := message["poll"].(map[string]any)
	options := poll["options"].([]any)
	optionID := options[0].(map[string]any)["option_id"].(string)
	votePath := path + "/" + messageID + "/poll/vote"
	resp, body = call(t, server, http.MethodPost, votePath, w.humanB.ID, map[string]any{
		"option_ids": []string{optionID},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("vote status %d body %v", resp.StatusCode, body)
	}
	voted := body["message"].(map[string]any)["poll"].(map[string]any)
	if revision, ok := voted["revision"].(float64); !ok || revision != 1 {
		t.Fatalf("vote revision = %#v", voted["revision"])
	}
	voters := voted["options"].([]any)[0].(map[string]any)["voters"].([]any)
	if len(voters) != 1 {
		t.Fatalf("visible voters = %v", voters)
	}
	resp, body = call(t, server, http.MethodPost, path, w.humanA.ID, map[string]any{
		"content": "", "client_nonce": "nul-poll", "poll": map[string]any{
			"question": "bad\x00question", "options": []string{"A", "B"},
		},
	})
	if resp.StatusCode != http.StatusBadRequest || body["error"] != "invalid_poll" {
		t.Fatalf("NUL poll status %d body %v", resp.StatusCode, body)
	}
}
