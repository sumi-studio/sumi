package messaging

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestThreadsAreWorkspaceVisibleButBootstrapParticipationScoped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	origin := w.send(t, ctx, channel.PlaceID, w.humanA, "認証の話")
	a := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	thread, err := a.CreateThread(ctx, channel.PlaceID, "認証リダイレクト", origin.MessageID)
	if err != nil {
		t.Fatalf("create thread: %v", err)
	}
	if _, err := a.CreateThread(ctx, channel.PlaceID, "duplicate", origin.MessageID); !errors.Is(err, ErrThreadExists) {
		t.Fatalf("duplicate origin error = %v", err)
	}
	b := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	if _, err := b.ThreadFor(ctx, thread.Place.PlaceID); err != nil {
		t.Fatalf("workspace member cannot open thread: %v", err)
	}
	if got, err := b.ThreadsFor(ctx); err != nil || len(got) != 0 {
		t.Fatalf("nonparticipant bootstrap threads = %+v, err %v", got, err)
	}
	message := w.send(t, ctx, thread.Place.PlaceID, w.humanA, "@Haru この枝を見てください")
	if got, err := b.ThreadsFor(ctx); err != nil || len(got) != 1 {
		t.Fatalf("mentioned participant threads = %+v, err %v", got, err)
	}
	decisions, err := a.NotificationDecisionsFor(ctx, thread.Place, message)
	if err != nil {
		t.Fatalf("thread notifications: %v", err)
	}
	if reasonFor(t, decisions, w.humanB) != NotifyReasonMention || reasonFor(t, decisions, w.agent) != "" {
		t.Fatalf("thread decisions = %+v", decisions)
	}
}

func TestThreadSearchUsesParticipationProjection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	a := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	thread, err := a.CreateThread(ctx, channel.PlaceID, "検索", "")
	if err != nil {
		t.Fatal(err)
	}
	w.send(t, ctx, thread.Place.PlaceID, w.humanA, "枝だけの合言葉")
	b := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	results, err := b.SearchMessages(ctx, "合言葉", SearchOptions{})
	if err != nil || len(results) != 0 {
		t.Fatalf("global search = %+v, %v", results, err)
	}
	w.send(t, ctx, thread.Place.PlaceID, w.humanA, "@Haru 参加してください")
	results, err = b.SearchMessages(ctx, "合言葉", SearchOptions{})
	if err != nil || len(results) != 1 {
		t.Fatalf("participant search = %+v, %v", results, err)
	}
}

func TestThreadHTTPRoutesReturnParentRelation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, server := newTestServer(t, ctx)
	resp, body := call(t, server, http.MethodPost,
		"/messaging/places/"+DefaultGeneralChannelID+"/threads", w.humanA.ID,
		map[string]any{"name": "HTTP thread", "parent_message_id": ""})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status %d body %v", resp.StatusCode, body)
	}
	threadID, _ := body["thread_id"].(string)
	resp, body = call(t, server, http.MethodGet, "/messaging/places/"+threadID, w.humanB.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("open status %d body %v", resp.StatusCode, body)
	}
	thread, ok := body["thread"].(map[string]any)
	if !ok || thread["thread_id"] != threadID {
		t.Fatalf("thread relation = %v", body)
	}
}
