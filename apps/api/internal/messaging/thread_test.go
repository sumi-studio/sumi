package messaging

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
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

// A viewer who only reads a thread keeps a durable read marker without
// becoming a participant, and an unjoined thread stays out of their ledger:
// bootstrap projects the places they hold, not every place they could open.
func TestNonparticipantThreadReadMarkerPersistsOutsideBootstrap(t *testing.T) {
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
	// The marker is durable and is what the thread's own screen shows.
	if seq, err := viewer.ReadMarker(ctx, thread.Place.PlaceID); err != nil || seq != message.Seq {
		t.Fatalf("nonparticipant read marker = %d (err %v), want %d", seq, err, message.Seq)
	}

	resp, body = call(t, ts, http.MethodGet, "/messaging/bootstrap", w.humanB.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bootstrap after read-through: status %d body %v", resp.StatusCode, body)
	}
	for _, raw := range body["unread_summaries"].([]any) {
		summary := raw.(map[string]any)
		if summary["place"].(map[string]any)["thread_id"] == thread.Place.PlaceID {
			t.Fatalf("bootstrap projected an unjoined thread: %v", summary)
		}
	}
	if threads, ok := body["threads"].([]any); ok && len(threads) != 0 {
		t.Fatalf("bootstrap threads for a nonparticipant = %v", threads)
	}
}

// A Workspace holds more threads than one reconnect handshake may carry, and
// anyone who can post can make another one. If bootstrap projected all of them
// the client would send one cursor per thread, the handshake would be refused
// outright, and the same bootstrap would rebuild the same rejected ledger on
// every reload — live delivery would stop permanently.
func TestManyUnjoinedThreadsStayOutOfBootstrapAndTheHandshake(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	w, ts := newWSWorld(t, ctx)
	owner := w.store.mustScope(t, ctx, DefaultWorkspaceID, w.humanA)
	joined, _, err := owner.CreateThread(ctx, DefaultGeneralChannelID, "参加している枝", "", "thread-handshake-joined")
	if err != nil {
		t.Fatalf("create joined thread: %v", err)
	}
	ids := make([]string, 0, maxHelloCursors+16)
	for range cap(ids) {
		ids = append(ids, newUUIDv7())
	}
	if _, err := w.store.pool.Exec(ctx, `
		INSERT INTO places (place_id, kind, workspace_id, name, parent_place_id)
		SELECT id, 'thread', $2, 'unjoined-' || ordinality, $3
		FROM unnest($1::text[]) WITH ORDINALITY AS listed(id, ordinality)`,
		ids, DefaultWorkspaceID, DefaultGeneralChannelID); err != nil {
		t.Fatalf("seed unjoined threads: %v", err)
	}

	resp, body := call(t, ts, http.MethodGet, "/messaging/bootstrap", w.humanB.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bootstrap: status %d body %v", resp.StatusCode, body)
	}
	// Build the handshake exactly as the browser client does: one cursor per
	// place bootstrap summarized.
	cursors := map[string]int64{}
	for _, raw := range body["unread_summaries"].([]any) {
		place := raw.(map[string]any)["place"].(map[string]any)
		for _, key := range []string{"channel_id", "dm_id", "thread_id"} {
			if id, ok := place[key].(string); ok && id != "" {
				cursors[id] = int64(raw.(map[string]any)["latest_seq"].(float64))
			}
		}
	}
	if _, projected := cursors[joined.Place.PlaceID]; projected {
		t.Fatal("bootstrap projected a thread the viewer never joined")
	}
	if len(cursors) > maxHelloCursors {
		t.Fatalf("bootstrap projected %d places, over the %d the handshake accepts",
			len(cursors), maxHelloCursors)
	}
	// dialWS fails the test unless the handshake is accepted.
	dialWS(t, ts, w.humanB.ID, cursors)

	// Participation, not Workspace visibility, is what carries a thread.
	resp, body = call(t, ts, http.MethodGet, "/messaging/bootstrap", w.humanA.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("owner bootstrap: status %d body %v", resp.StatusCode, body)
	}
	found := false
	for _, raw := range body["unread_summaries"].([]any) {
		place := raw.(map[string]any)["place"].(map[string]any)
		if place["thread_id"] == joined.Place.PlaceID {
			found = true
		}
	}
	if !found {
		t.Fatal("bootstrap omitted the thread its viewer participates in")
	}
}

// Live thread traffic belongs to its participants. A Workspace member who
// opens a thread they never joined still sees it arrive while it is on screen,
// and stops receiving it when they leave — reading is not joining.
func TestThreadLiveDeliveryIsParticipationScopedUntilTheThreadIsOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w, ts := newWSWorld(t, ctx)
	owner := w.store.mustScope(t, ctx, DefaultWorkspaceID, w.humanA)
	thread, _, err := owner.CreateThread(ctx, DefaultGeneralChannelID, "参加者の枝", "", "thread-live-1")
	if err != nil {
		t.Fatalf("create thread: %v", err)
	}
	participant := dialWS(t, ts, w.humanA.ID, nil)
	viewer := dialWS(t, ts, w.humanB.ID, nil)

	post := func(content, nonce string) {
		t.Helper()
		resp, body := call(t, ts, http.MethodPost,
			"/messaging/places/"+thread.Place.PlaceID+"/messages", w.humanA.ID,
			map[string]any{"content": content, "client_nonce": nonce})
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("post %q: status %d body %v", content, resp.StatusCode, body)
		}
	}
	expectMessage := func(conn *websocket.Conn, content string) {
		t.Helper()
		frame := readFrame(t, conn)
		event, _ := frame["event"].(map[string]any)
		message, _ := event["message"].(map[string]any)
		if frame["type"] != "event" || message["content"] != content {
			t.Fatalf("expected %q, got %v", content, frame)
		}
	}
	// Frames reach one subscriber in the order they were queued, and each post
	// has already fanned out by the time it responds. So the first frame that
	// answers the declaration is proof that nothing was delivered before it — a
	// read timeout would prove the same thing but permanently break the socket.
	// The declaration says the client already holds everything committed so far,
	// which leaves the replay empty and caught_up first.
	declareOpenHoldingAll := func(conn *websocket.Conn, placeID string) {
		t.Helper()
		place, err := owner.PlaceFor(ctx, placeID)
		if err != nil {
			t.Fatalf("read place %s: %v", placeID, err)
		}
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := conn.WriteJSON(map[string]any{
			"type": "open", "place_id": placeID, "since": place.LastSeq,
		}); err != nil {
			t.Fatalf("declare open place: %v", err)
		}
		if frame := readFrame(t, conn); frame["type"] != "caught_up" || frame["place_id"] != placeID {
			t.Fatalf("first frame after open = %v, want caught_up for %s", frame, placeID)
		}
		if frame := readFrame(t, conn); frame["type"] != "open_ack" || frame["place_id"] != placeID {
			t.Fatalf("frame after replay = %v, want open_ack for %s", frame, placeID)
		}
	}

	post("参加者だけに届く", "thread-live-nonce-1")
	expectMessage(participant, "参加者だけに届く")
	declareOpenHoldingAll(viewer, thread.Place.PlaceID)

	post("開いている間は届く", "thread-live-nonce-2")
	expectMessage(participant, "開いている間は届く")
	expectMessage(viewer, "開いている間は届く")

	_ = viewer.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := viewer.WriteJSON(map[string]any{"type": "close", "place_id": thread.Place.PlaceID}); err != nil {
		t.Fatalf("close place: %v", err)
	}
	post("閉じたら届かない", "thread-live-nonce-3")
	expectMessage(participant, "閉じたら届かない")
	declareOpenHoldingAll(viewer, DefaultGeneralChannelID)

	// Reading never admitted the viewer as a participant.
	viewerStore := w.store.mustScope(t, ctx, DefaultWorkspaceID, w.humanB)
	if threads, err := viewerStore.ThreadsFor(ctx); err != nil || len(threads) != 0 {
		t.Fatalf("watching admitted a participant: threads=%+v err=%v", threads, err)
	}
}

// One thread summary must describe one moment. Its message count and its
// participant list used to come from two statements, so a message that admitted
// its author could be counted before that author appeared — a state that never
// existed in the database.
func TestThreadSummaryProjectsOneMoment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	owner := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	viewer := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	joined := func(thread Thread) bool {
		for _, participant := range thread.Participants {
			if participant == w.humanB {
				return true
			}
		}
		return false
	}
	for round := range 20 {
		nonce := fmt.Sprintf("thread-snapshot-%d", round)
		thread, _, err := owner.CreateThread(ctx, channel.PlaceID, nonce, "", nonce)
		if err != nil {
			t.Fatalf("create thread: %v", err)
		}
		w.send(t, ctx, thread.Place.PlaceID, w.humanA, "起点")
		written := make(chan error, 1)
		go func() {
			_, _, err := w.store.AppendMessage(ctx, AppendInput{
				PlaceID: thread.Place.PlaceID, Author: w.humanB,
				Content: "参加と同時の一通", ClientNonce: nonce + "-reply",
			})
			written <- err
		}()
		for {
			summary, err := viewer.ThreadFor(ctx, thread.Place.PlaceID)
			if err != nil {
				t.Fatalf("read thread summary: %v", err)
			}
			if (summary.MessageCount == 2) != joined(summary) {
				t.Fatalf("summary showed a moment that never existed: count=%d participants=%+v",
					summary.MessageCount, summary.Participants)
			}
			select {
			case err := <-written:
				if err != nil {
					t.Fatalf("concurrent reply: %v", err)
				}
				final, err := viewer.ThreadFor(ctx, thread.Place.PlaceID)
				if err != nil {
					t.Fatalf("read committed summary: %v", err)
				}
				if final.MessageCount != 2 || !joined(final) {
					t.Fatalf("committed summary = count %d participants %+v",
						final.MessageCount, final.Participants)
				}
			default:
				continue
			}
			break
		}
	}
}

// A store failure is not an answer about existence. Reporting it as not-found
// makes a client mark a thread it can see as permanently gone.
func TestThreadReadReportsStoreFailureInsteadOfNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	owner := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	thread, _, err := owner.CreateThread(ctx, channel.PlaceID, "障害中", "", "thread-failure-1")
	if err != nil {
		t.Fatalf("create thread: %v", err)
	}
	if _, err := w.store.pool.Exec(ctx, "ALTER TABLE places RENAME TO places_unavailable"); err != nil {
		t.Fatalf("inject store failure: %v", err)
	}
	_, err = owner.ThreadFor(ctx, thread.Place.PlaceID)
	if err == nil || errors.Is(err, ErrPlaceNotFound) {
		t.Fatalf("thread read during a store failure = %v, want the failure itself", err)
	}
	if _, err := w.store.pool.Exec(ctx, "ALTER TABLE places_unavailable RENAME TO places"); err != nil {
		t.Fatalf("restore store: %v", err)
	}
	if _, err := owner.ThreadFor(ctx, thread.Place.PlaceID); err != nil {
		t.Fatalf("thread read after recovery: %v", err)
	}
	if _, err := owner.ThreadFor(ctx, channel.PlaceID); !errors.Is(err, ErrPlaceNotFound) {
		t.Fatalf("non-thread place = %v, want ErrPlaceNotFound", err)
	}
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

// A cursor says where a client stopped reading, not what it is entitled to
// read. A thread is visible Workspace-wide, so a viewer who once opened one
// keeps a working cursor for it; replaying on that alone would push a
// background conversation back into someone who never joined it. Replay
// follows the same line as live delivery — participation, or an open
// declaration on this connection — and the deferred half arrives after the
// open frame, never before it.
func TestThreadCatchUpFollowsParticipationNotTheClientsCursor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w, ts := newWSWorld(t, ctx)
	owner := w.store.mustScope(t, ctx, DefaultWorkspaceID, w.humanA)
	thread, _, err := owner.CreateThread(ctx, DefaultGeneralChannelID, "背景の枝", "", "thread-catchup-1")
	if err != nil {
		t.Fatalf("create thread: %v", err)
	}
	for i, content := range []string{"先に置いておく", "二通目"} {
		resp, body := call(t, ts, http.MethodPost,
			"/messaging/places/"+thread.Place.PlaceID+"/messages", w.humanA.ID,
			map[string]any{"content": content, "client_nonce": fmt.Sprintf("thread-catchup-post-%d", i)})
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("post %q: status %d body %v", content, resp.StatusCode, body)
		}
	}

	expectMessage := func(conn *websocket.Conn, content string) {
		t.Helper()
		frame := readFrame(t, conn)
		event, _ := frame["event"].(map[string]any)
		message, _ := event["message"].(map[string]any)
		if frame["type"] != "event" || message["content"] != content {
			t.Fatalf("expected replayed %q, got %v", content, frame)
		}
	}
	expectCaughtUp := func(conn *websocket.Conn, placeID string) {
		t.Helper()
		if frame := readFrame(t, conn); frame["type"] != "caught_up" || frame["place_id"] != placeID {
			t.Fatalf("expected caught_up for %s, got %v", placeID, frame)
		}
	}
	declareOpen := func(conn *websocket.Conn, placeID string, since int64) {
		t.Helper()
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := conn.WriteJSON(map[string]any{
			"type": "open", "place_id": placeID, "since": since,
		}); err != nil {
			t.Fatalf("declare open place: %v", err)
		}
	}
	expectOpenAck := func(conn *websocket.Conn, placeID string) {
		t.Helper()
		if frame := readFrame(t, conn); frame["type"] != "open_ack" || frame["place_id"] != placeID {
			t.Fatalf("expected open_ack for %s, got %v", placeID, frame)
		}
	}

	// The participant holds the thread, so the handshake alone replays it.
	participant := dialWS(t, ts, w.humanA.ID, map[string]int64{thread.Place.PlaceID: 0})
	expectMessage(participant, "先に置いておく")
	expectMessage(participant, "二通目")
	expectCaughtUp(participant, thread.Place.PlaceID)

	// The viewer claims the same cursor without ever having joined. Opening a
	// different place proves the handshake replayed nothing for the thread:
	// its acknowledgement is the first frame after hello_ack.
	viewer := dialWS(t, ts, w.humanB.ID, map[string]int64{thread.Place.PlaceID: 0})
	channel, err := owner.PlaceFor(ctx, DefaultGeneralChannelID)
	if err != nil {
		t.Fatalf("read channel place: %v", err)
	}
	declareOpen(viewer, DefaultGeneralChannelID, channel.LastSeq)
	expectCaughtUp(viewer, DefaultGeneralChannelID)
	expectOpenAck(viewer, DefaultGeneralChannelID)

	// Declaring the thread open is what finally admits the deferred replay, and
	// it lands before the acknowledgement like any other cursor's.
	declareOpen(viewer, thread.Place.PlaceID, 0)
	expectMessage(viewer, "先に置いておく")
	expectMessage(viewer, "二通目")
	expectCaughtUp(viewer, thread.Place.PlaceID)
	expectOpenAck(viewer, thread.Place.PlaceID)

	// Replay is delivery, not admission: the viewer is still not a participant.
	viewerStore := w.store.mustScope(t, ctx, DefaultWorkspaceID, w.humanB)
	if threads, err := viewerStore.ThreadsFor(ctx); err != nil || len(threads) != 0 {
		t.Fatalf("catch-up admitted a participant: threads=%+v err=%v", threads, err)
	}
}

// A screen is opened after its history has been fetched, so the declaration and
// the fetch cannot be made simultaneous: whatever commits between them is in
// neither. It is not in the page the client already holds, and it is not live,
// because until the frame arrives the connection has not said it is looking at
// the thread. The open frame therefore carries how far the client holds, and
// the server replays from there before acknowledging.
func TestThreadOpenReplaysWhatCommittedWhileTheDeclarationWasInFlight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w, ts := newWSWorld(t, ctx)
	owner := w.store.mustScope(t, ctx, DefaultWorkspaceID, w.humanA)
	thread, _, err := owner.CreateThread(ctx, DefaultGeneralChannelID, "行き違う枝", "", "thread-open-gap-1")
	if err != nil {
		t.Fatalf("create thread: %v", err)
	}
	post := func(content, nonce string) {
		t.Helper()
		resp, body := call(t, ts, http.MethodPost,
			"/messaging/places/"+thread.Place.PlaceID+"/messages", w.humanA.ID,
			map[string]any{"content": content, "client_nonce": nonce})
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("post %q: status %d body %v", content, resp.StatusCode, body)
		}
	}
	expectMessage := func(conn *websocket.Conn, content string) {
		t.Helper()
		frame := readFrame(t, conn)
		event, _ := frame["event"].(map[string]any)
		message, _ := event["message"].(map[string]any)
		if frame["type"] != "event" || message["content"] != content {
			t.Fatalf("expected %q, got %v", content, frame)
		}
	}

	post("画面が読み込んだ分", "thread-open-gap-post-1")
	// The screen fetched its history here: this is everything the client holds.
	held, err := owner.PlaceFor(ctx, thread.Place.PlaceID)
	if err != nil {
		t.Fatalf("read thread place: %v", err)
	}

	// The socket is already up and carries no cursor for a thread this viewer
	// never joined, so the handshake deferred nothing to flush.
	viewer := dialWS(t, ts, w.humanB.ID, nil)
	post("取得と宣言の隙間", "thread-open-gap-post-2")

	_ = viewer.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := viewer.WriteJSON(map[string]any{
		"type": "open", "place_id": thread.Place.PlaceID, "since": held.LastSeq,
	}); err != nil {
		t.Fatalf("declare open place: %v", err)
	}

	// The gap arrives by replay, before the acknowledgement. Frames reach one
	// subscriber in the order they were queued, so this is also proof that the
	// page the client already holds was not replayed on top of it.
	expectMessage(viewer, "取得と宣言の隙間")
	if frame := readFrame(t, viewer); frame["type"] != "caught_up" || frame["place_id"] != thread.Place.PlaceID {
		t.Fatalf("expected caught_up for the thread, got %v", frame)
	}
	if frame := readFrame(t, viewer); frame["type"] != "open_ack" || frame["place_id"] != thread.Place.PlaceID {
		t.Fatalf("expected open_ack after the replay, got %v", frame)
	}

	// Live delivery starts at the declaration, so replay and live overlap
	// rather than leaving a second gap behind the first.
	post("開いてからのlive", "thread-open-gap-post-3")
	expectMessage(viewer, "開いてからのlive")
}
