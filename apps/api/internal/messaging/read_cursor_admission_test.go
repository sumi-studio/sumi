package messaging

import (
	"context"
	"testing"
	"time"
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
