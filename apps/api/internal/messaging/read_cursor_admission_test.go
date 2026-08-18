package messaging

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

func TestOpenSnapshotStaysCoherentAcrossConcurrentAppendAndCursorAdvance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	scoped := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
	first := w.send(t, ctx, channel.PlaceID, w.humanA, "first")
	if err := w.store.ReadThrough(ctx, channel.PlaceID, w.agent, first.Seq); err != nil {
		t.Fatalf("establish initial Agent cursor: %v", err)
	}

	// Mirror OpenSnapshot through its first authorized read, then keep that
	// real PostgreSQL snapshot open while another transaction appends and
	// advances this exact viewer's cursor.
	reader, err := scoped.Store.beginOpenSnapshot(ctx)
	if err != nil {
		t.Fatalf("begin reader snapshot: %v", err)
	}
	defer func() { _ = reader.Rollback(ctx) }()
	membership, err := scoped.authorizeSnapshotInTx(ctx, reader)
	if err != nil {
		t.Fatalf("authorize exact reader scope: %v", err)
	}
	place, err := scoped.loadScopedPlace(ctx, reader, channel.PlaceID)
	if err != nil {
		t.Fatalf("authorize reader snapshot: %v", err)
	}
	access, err := scoped.placeAccessAfterAuthorization(ctx, reader, place, w.agent)
	if err != nil {
		t.Fatalf("authorize reader place tenure: %v", err)
	}
	var isolation, readOnly string
	if err := reader.QueryRow(ctx,
		"SELECT current_setting('transaction_isolation'), current_setting('transaction_read_only')").
		Scan(&isolation, &readOnly); err != nil {
		t.Fatalf("inspect reader snapshot: %v", err)
	}
	if isolation != "repeatable read" || readOnly != "on" {
		t.Fatalf("reader mode = %q/%q, want repeatable read/on", isolation, readOnly)
	}

	second := w.send(t, ctx, channel.PlaceID, w.humanA, "second")
	if err := w.store.ReadThrough(ctx, channel.PlaceID, w.agent, second.Seq); err != nil {
		t.Fatalf("advance concurrent Agent cursor: %v", err)
	}

	old, err := scoped.openSnapshotFromPlace(ctx, reader, membership.WorkspaceMemberID, place, access, HistoryOptions{Limit: 10})
	if err != nil {
		t.Fatalf("finish reader snapshot: %v", err)
	}
	if old.Place.LastSeq != first.Seq || old.LastReadSeq != first.Seq ||
		len(old.Messages) != 1 || old.Messages[0].MessageID != first.MessageID {
		t.Fatalf("reader mixed old and new commits: %+v", old)
	}
	if err := reader.Commit(ctx); err != nil {
		t.Fatalf("commit reader snapshot: %v", err)
	}

	// A new screen sees the later commit coherently as well. Either side of the
	// race is valid; mixing latest/history/cursor from both sides is not.
	fresh, err := scoped.OpenSnapshot(ctx, channel.PlaceID, HistoryOptions{Limit: 10})
	if err != nil {
		t.Fatalf("open fresh snapshot: %v", err)
	}
	if fresh.Place.LastSeq != second.Seq || fresh.LastReadSeq != second.Seq ||
		len(fresh.Messages) != 2 || fresh.Messages[1].MessageID != second.MessageID {
		t.Fatalf("fresh snapshot missed the concurrent commit: %+v", fresh)
	}
}

func TestOpenSnapshotKeepsThreadSummaryInTheSameSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	scoped := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
	thread, _, err := scoped.CreateThread(ctx, channel.PlaceID, "snapshot summary", "", "thread-snapshot-summary")
	if err != nil {
		t.Fatalf("create thread: %v", err)
	}
	first := w.send(t, ctx, thread.Place.PlaceID, w.humanA, "first")

	// Hold the exact REPEATABLE READ transaction OpenSnapshot uses, then append
	// after its place read. The summary must remain on this old snapshot rather
	// than leaking the later append through a second transaction.
	reader, err := scoped.Store.beginOpenSnapshot(ctx)
	if err != nil {
		t.Fatalf("begin reader snapshot: %v", err)
	}
	defer func() { _ = reader.Rollback(ctx) }()
	membership, err := scoped.authorizeSnapshotInTx(ctx, reader)
	if err != nil {
		t.Fatalf("authorize reader snapshot: %v", err)
	}
	place, err := scoped.loadScopedPlace(ctx, reader, thread.Place.PlaceID)
	if err != nil {
		t.Fatalf("load thread place: %v", err)
	}
	access, err := scoped.placeAccessAfterAuthorization(ctx, reader, place, w.agent)
	if err != nil {
		t.Fatalf("authorize thread place: %v", err)
	}

	second := w.send(t, ctx, thread.Place.PlaceID, w.humanA, "second")
	snapshot, err := scoped.openSnapshotFromPlace(ctx, reader, membership.WorkspaceMemberID, place, access, HistoryOptions{Limit: 10})
	if err != nil {
		t.Fatalf("finish reader snapshot: %v", err)
	}
	if snapshot.Thread == nil || snapshot.Place.LastSeq != first.Seq ||
		snapshot.Thread.Place.LastSeq != first.Seq || snapshot.Thread.MessageCount != 1 ||
		len(snapshot.Messages) != 1 || snapshot.Messages[0].MessageID != first.MessageID {
		t.Fatalf("thread screen mixed old and new commits: %+v", snapshot)
	}
	if snapshot.Thread.Place.LastSeq == second.Seq {
		t.Fatalf("thread summary leaked concurrent append: %+v", snapshot.Thread)
	}
	if err := reader.Commit(ctx); err != nil {
		t.Fatalf("commit reader snapshot: %v", err)
	}
}

func TestLocalAgentOpenCarriesExactCursorForContiguousAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	server := NewServer(w.store.core, nil)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}
	workspace, channel := w.workspaceWithChannel(t, ctx)
	scoped := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
	scope := map[string]any{
		"workspace_id":    workspace.WorkspaceID,
		"installation_id": scoped.Scope.InstallationID,
		"authority_epoch": strconv.FormatInt(scoped.Scope.AuthorityEpoch, 10),
	}

	messages := make([]Message, 25)
	for i := range messages {
		messages[i] = w.send(t, ctx, channel.PlaceID, w.humanA, fmt.Sprintf("message %02d", i+1))
	}
	// Tombstones still occupy their sequence in a contiguous admitted page.
	if _, err := w.store.DeleteMessage(ctx, channel.PlaceID, messages[2].MessageID, w.humanA); err != nil {
		t.Fatalf("delete message 3: %v", err)
	}
	// Prove the local wire is scoped to the authenticated Agent, not its Human
	// owner or another visible participant.
	if err := w.store.ReadThrough(ctx, channel.PlaceID, w.humanA, 25); err != nil {
		t.Fatalf("advance Human cursor: %v", err)
	}

	open := func(beforeSeq int64) map[string]any {
		t.Helper()
		request := map[string]any{
			"place_id": channel.PlaceID, "before_seq": beforeSeq, "limit": 10,
		}
		for key, value := range scope {
			request[key] = value
		}
		status, body := callLocal(t, ctx, server.localOpen, LocalOpenPath, request, authorization)
		if status != http.StatusOK {
			t.Fatalf("open before %d: status %d body %v", beforeSeq, status, body)
		}
		return body
	}
	readThrough := func(seq int64) {
		t.Helper()
		request := map[string]any{"place_id": channel.PlaceID, "seq": seq}
		for key, value := range scope {
			request[key] = value
		}
		status, body := callLocal(t, ctx, server.localReadThrough, LocalReadThroughPath,
			request, authorization)
		if status != http.StatusOK {
			t.Fatalf("read through %d: status %d body %v", seq, status, body)
		}
	}
	assertPage := func(body map[string]any, wantLastRead int64, wantFirst, wantLast int64) {
		t.Helper()
		if got := int64(body["last_read_seq"].(float64)); got != wantLastRead {
			t.Fatalf("last_read_seq = %d, want %d (body %v)", got, wantLastRead, body)
		}
		rows := body["messages"].([]any)
		if got := int64(rows[0].(map[string]any)["seq"].(float64)); got != wantFirst {
			t.Fatalf("first seq = %d, want %d", got, wantFirst)
		}
		if got := int64(rows[len(rows)-1].(map[string]any)["seq"].(float64)); got != wantLast {
			t.Fatalf("last seq = %d, want %d", got, wantLast)
		}
	}
	assertAgentReadState := func(wantLastRead, wantUnread int64) {
		t.Helper()
		lastRead, err := w.store.ReadMarker(ctx, channel.PlaceID, w.agent)
		if err != nil {
			t.Fatalf("read Agent cursor: %v", err)
		}
		if lastRead != wantLastRead {
			t.Fatalf("Agent cursor = %d, want %d", lastRead, wantLastRead)
		}
		summaries, err := w.store.UnreadSummaries(ctx, w.agent)
		if err != nil {
			t.Fatalf("read Agent unread summaries: %v", err)
		}
		for _, summary := range summaries {
			if summary.Place.PlaceID == channel.PlaceID {
				if summary.UnreadCount != wantUnread {
					t.Fatalf("Agent unread = %d, want %d", summary.UnreadCount, wantUnread)
				}
				return
			}
		}
		t.Fatalf("Agent has no unread summary for %s", channel.PlaceID)
	}

	// The latest and middle pages both have a gap after cursor 0. Merely opening
	// them is read-only: the 24 non-tombstoned unread messages remain eligible
	// for attention until a durable ToolResult admits a contiguous prefix.
	assertPage(open(0), 0, 16, 25)
	assertAgentReadState(0, 24)
	assertPage(open(16), 0, 6, 15)
	assertAgentReadState(0, 24)

	// Paging to the oldest page exposes 1..5, including tombstone seq 3. The
	// tool may acknowledge exactly that prefix after its result is durable.
	assertPage(open(6), 0, 1, 5)
	assertAgentReadState(0, 24)
	readThrough(5)
	assertAgentReadState(5, 20)

	// Re-open successive pages from the now-durable cursor. Each response names
	// the new exact cursor and admits only the next contiguous prefix.
	assertPage(open(16), 5, 6, 15)
	assertAgentReadState(5, 20)
	readThrough(15)
	assertAgentReadState(15, 10)
	assertPage(open(0), 15, 16, 25)
	assertAgentReadState(15, 10)
	readThrough(25)
	assertAgentReadState(25, 0)
}
