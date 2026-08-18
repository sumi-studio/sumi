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
	thread, created, err := a.CreateThread(ctx, channel.PlaceID, "認証リダイレクト", origin.MessageID, "thread-origin-1")
	if err != nil {
		t.Fatalf("create thread: %v", err)
	}
	if !created {
		t.Fatal("first thread creation was replayed")
	}
	if _, _, err := a.CreateThread(ctx, channel.PlaceID, "duplicate", origin.MessageID, "thread-origin-2"); !errors.Is(err, ErrThreadExists) {
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

func TestEditingThreadMessageAdmitsNewMention(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	a := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	b := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	thread, _, err := a.CreateThread(ctx, channel.PlaceID, "編集で参加", "", "thread-edit-mention-1")
	if err != nil {
		t.Fatalf("create thread: %v", err)
	}
	message := w.send(t, ctx, thread.Place.PlaceID, w.humanA, "あとで追記します")
	if threads, err := b.ThreadsFor(ctx); err != nil || len(threads) != 0 {
		t.Fatalf("mentioned member started as nonparticipant: threads=%+v err=%v", threads, err)
	}

	if _, err := a.EditMessage(ctx, thread.Place.PlaceID, message.MessageID, "@Haru この件もお願いします"); err != nil {
		t.Fatalf("edit thread message: %v", err)
	}
	threads, err := b.ThreadsFor(ctx)
	if err != nil || len(threads) != 1 {
		t.Fatalf("edited mention did not admit participant: threads=%+v err=%v", threads, err)
	}
	if got := threads[0].Participants; len(got) != 2 || got[0] != w.humanA || got[1] != w.humanB {
		t.Fatalf("thread participants = %+v, want author and edited mention", got)
	}
}

func TestThreadRejectsDeletedOriginMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	owner := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	origin := w.send(t, ctx, channel.PlaceID, w.humanA, "削除する起点")
	if _, err := owner.DeleteMessage(ctx, channel.PlaceID, origin.MessageID); err != nil {
		t.Fatalf("delete origin: %v", err)
	}
	if _, _, err := owner.CreateThread(ctx, channel.PlaceID, "削除済み起点", origin.MessageID, "deleted-origin-1"); !errors.Is(err, ErrMessageNotFound) {
		t.Fatalf("create from deleted origin: got %v, want ErrMessageNotFound", err)
	}
}

func TestNonparticipantThreadReadMarkerSurvivesBootstrap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)

	owner := w.store.mustScope(t, ctx, DefaultWorkspaceID, w.humanA)
	thread, _, err := owner.CreateThread(ctx, DefaultGeneralChannelID, "閲覧だけ", "", "thread-viewer-1")
	if err != nil {
		t.Fatalf("create thread: %v", err)
	}
	message := w.send(t, ctx, thread.Place.PlaceID, w.humanA, "既読を残します")
	viewer := w.store.mustScope(t, ctx, DefaultWorkspaceID, w.humanB)
	if threads, err := viewer.ThreadsFor(ctx); err != nil || len(threads) != 0 {
		t.Fatalf("viewer started as a nonparticipant: threads=%+v err=%v", threads, err)
	}

	resp, body := call(t, ts, http.MethodPut,
		"/messaging/places/"+thread.Place.PlaceID+"/read-through", w.humanB.ID,
		map[string]any{"seq": message.Seq})
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("read-through: status %d body %v", resp.StatusCode, body)
	}
	if threads, err := viewer.ThreadsFor(ctx); err != nil || len(threads) != 0 {
		t.Fatalf("read marker made viewer a participant: threads=%+v err=%v", threads, err)
	}

	resp, body = call(t, ts, http.MethodGet, "/messaging/bootstrap", w.humanB.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bootstrap after read-through: status %d body %v", resp.StatusCode, body)
	}
	for _, raw := range body["unread_summaries"].([]any) {
		summary := raw.(map[string]any)
		if summary["place"].(map[string]any)["thread_id"] == thread.Place.PlaceID {
			if summary["unread_count"] != float64(0) || summary["latest_seq"] != float64(message.Seq) {
				t.Fatalf("thread unread summary after bootstrap = %v", summary)
			}
			return
		}
	}
	t.Fatalf("bootstrap unread summaries omitted visible thread: %v", body["unread_summaries"])
}

func TestConcurrentThreadParticipantAdmissionIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorldWithMaxConns(t, ctx, 10)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	owner := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	thread, _, err := owner.CreateThread(ctx, channel.PlaceID, "同時参加", "", "thread-admission-1")
	if err != nil {
		t.Fatalf("create thread: %v", err)
	}

	lookup, err := w.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	membership, err := w.workspaces.ActiveMembershipInTx(ctx, lookup, workspace.WorkspaceID, w.humanB)
	if err != nil {
		_ = lookup.Rollback(ctx)
		t.Fatal(err)
	}
	if err := lookup.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			tx, err := w.store.pool.Begin(ctx)
			if err != nil {
				results <- err
				return
			}
			defer func() { _ = tx.Rollback(context.Background()) }()
			<-start
			if err := admitPlaceTenure(ctx, tx, thread.Place.PlaceID, membership, 1); err != nil {
				results <- err
				return
			}
			results <- tx.Commit(ctx)
		}()
	}
	close(start)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("concurrent thread admission: %v", err)
		}
	}

	var count int
	if err := w.store.pool.QueryRow(ctx, `
		SELECT count(*) FROM place_members
		WHERE place_id = $1 AND workspace_member_id = $2 AND left_at IS NULL`,
		thread.Place.PlaceID, membership.WorkspaceMemberID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("active participant tenures = %d, want 1", count)
	}
}

func TestThreadSearchIncludesWorkspaceVisibleNonparticipantWhenPlaceIsScoped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	a := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	thread, _, err := a.CreateThread(ctx, channel.PlaceID, "検索", "", "thread-search-1")
	if err != nil {
		t.Fatal(err)
	}
	w.send(t, ctx, thread.Place.PlaceID, w.humanA, "枝だけの合言葉")
	b := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	results, err := b.SearchMessages(ctx, "合言葉", SearchOptions{PlaceID: thread.Place.PlaceID})
	if err != nil || len(results) != 1 {
		t.Fatalf("nonparticipant scoped search = %+v, %v", results, err)
	}
}

func TestThreadHTTPRoutesReturnParentRelation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, server := newTestServer(t, ctx)
	resp, body := call(t, server, http.MethodPost,
		"/messaging/places/"+DefaultGeneralChannelID+"/threads", w.humanA.ID,
		map[string]any{"name": "HTTP thread", "parent_message_id": "", "client_nonce": "thread-http-1"})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status %d body %v", resp.StatusCode, body)
	}
	threadID, _ := body["thread_id"].(string)
	replayed, replayBody := call(t, server, http.MethodPost,
		"/messaging/places/"+DefaultGeneralChannelID+"/threads", w.humanA.ID,
		map[string]any{"name": "HTTP thread", "parent_message_id": "", "client_nonce": "thread-http-1"})
	if replayed.StatusCode != http.StatusOK || replayBody["thread_id"] != threadID {
		t.Fatalf("replayed create status %d body %v, want existing %q", replayed.StatusCode, replayBody, threadID)
	}
	resp, body = call(t, server, http.MethodGet, "/messaging/places/"+threadID, w.humanB.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("open status %d body %v", resp.StatusCode, body)
	}
	thread, ok := body["thread"].(map[string]any)
	if !ok || thread["thread_id"] != threadID {
		t.Fatalf("thread relation = %v", body)
	}
}

func TestThreadHTTPRejectsNULName(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, server := newTestServer(t, ctx)

	resp, body := call(t, server, http.MethodPost,
		"/messaging/places/"+DefaultGeneralChannelID+"/threads", w.humanA.ID,
		map[string]any{"name": "bad\x00thread", "client_nonce": "thread-nul-http"})
	if resp.StatusCode != http.StatusBadRequest || body["error"] != "invalid_name" {
		t.Fatalf("NUL thread name = %d %v, want 400 invalid_name", resp.StatusCode, body)
	}
}
