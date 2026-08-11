package messaging

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

func TestLocalAgentOpenCarriesExactCursorForContiguousAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	server := NewServer(w.store, nil)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}
	_, channel := w.workspaceWithChannel(t, ctx)

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
		status, body := callLocal(t, ctx, server.localOpen, LocalOpenPath, map[string]any{
			"place_id": channel.PlaceID, "before_seq": beforeSeq, "limit": 10,
		}, authorization)
		if status != http.StatusOK {
			t.Fatalf("open before %d: status %d body %v", beforeSeq, status, body)
		}
		return body
	}
	readThrough := func(seq int64) {
		t.Helper()
		status, body := callLocal(t, ctx, server.localReadThrough, LocalReadThroughPath,
			map[string]any{"place_id": channel.PlaceID, "seq": seq}, authorization)
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
