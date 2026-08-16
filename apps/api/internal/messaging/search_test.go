package messaging

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestScopedSearchMatchesJapaneseAndBoundsSnippet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	w.send(t, ctx, channel.PlaceID, w.humanA, "おはようございます")
	target := w.send(t, ctx, channel.PlaceID, w.humanB, "今日の予定を共有します")
	long := strings.Repeat("あ", 100) + "検索の的" + strings.Repeat("い", 100)
	w.send(t, ctx, channel.PlaceID, w.humanA, long)
	doomed := w.send(t, ctx, channel.PlaceID, w.humanA, "この秘密は消えます")
	if _, err := w.store.DeleteMessage(ctx, channel.PlaceID, doomed.MessageID, w.humanA); err != nil {
		t.Fatalf("delete search fixture: %v", err)
	}

	search := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	results, err := search.SearchMessages(ctx, "予定", SearchOptions{})
	if err != nil {
		t.Fatalf("search Japanese substring: %v", err)
	}
	if len(results) != 1 || results[0].Message.MessageID != target.MessageID {
		t.Fatalf("Japanese results = %+v, want %s", results, target.MessageID)
	}
	results, err = search.SearchMessages(ctx, "検索の的", SearchOptions{})
	if err != nil {
		t.Fatalf("search clipped snippet: %v", err)
	}
	if len(results) != 1 || !strings.Contains(results[0].Snippet, "検索の的") {
		t.Fatalf("snippet = %+v, want match", results)
	}
	if got, max := len([]rune(results[0].Snippet)), 2*searchSnippetRadius+len([]rune("検索の的"))+2; got > max {
		t.Fatalf("snippet is %d runes, want at most %d", got, max)
	}
	if !strings.HasPrefix(results[0].Snippet, "…") || !strings.HasSuffix(results[0].Snippet, "…") {
		t.Fatalf("snippet %q should elide both sides", results[0].Snippet)
	}
	results, err = search.SearchMessages(ctx, "秘密", SearchOptions{})
	if err != nil {
		t.Fatalf("search tombstone: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("deleted message appeared in search: %+v", results)
	}
	results, err = search.SearchMessages(ctx, "%", SearchOptions{})
	if err != nil {
		t.Fatalf("search literal percent: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("literal percent matched as a wildcard: %+v", results)
	}
}

func TestScopedSearchEnforcesPlaceTenureAndScope(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	w.send(t, ctx, channel.PlaceID, w.humanA, "共有の会議メモ")
	dm, _, err := w.store.EnsureDM(ctx, w.humanA, w.agent)
	if err != nil {
		t.Fatalf("ensure dm: %v", err)
	}
	w.send(t, ctx, dm.PlaceID, w.humanA, "ひみつの会議メモ")

	viewerB := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	results, err := viewerB.SearchMessages(ctx, "会議メモ", SearchOptions{})
	if err != nil {
		t.Fatalf("search visible places: %v", err)
	}
	if len(results) != 1 || results[0].Place.PlaceID != channel.PlaceID {
		t.Fatalf("viewer B results = %+v, want channel only", results)
	}
	if _, err := viewerB.SearchMessages(ctx, "会議メモ", SearchOptions{PlaceID: dm.PlaceID}); !errors.Is(err, ErrPlaceNotFound) {
		t.Fatalf("invisible place search error = %v, want ErrPlaceNotFound", err)
	}

	group, err := w.store.CreateGroupDM(ctx, w.humanA, []ParticipantRef{w.humanB, w.agent})
	if err != nil {
		t.Fatalf("create group DM: %v", err)
	}
	w.send(t, ctx, group.PlaceID, w.humanA, "引継ぎ前の機密メモ")
	w.send(t, ctx, group.PlaceID, w.humanA, "引継ぎ前の機密メモ その2")
	visible := w.send(t, ctx, group.PlaceID, w.humanA, "引継ぎ後の機密メモ")
	if _, err := w.store.pool.Exec(ctx, `
		UPDATE place_members SET visible_from_seq = $1
		WHERE workspace_id = $2 AND place_id = $3
		  AND member_kind = $4 AND member_id = $5 AND left_at IS NULL`,
		visible.Seq, workspace.WorkspaceID, group.PlaceID, w.humanB.Kind, w.humanB.ID,
	); err != nil {
		t.Fatalf("set group DM tenure boundary: %v", err)
	}
	results, err = viewerB.SearchMessages(ctx, "機密メモ", SearchOptions{PlaceID: group.PlaceID})
	if err != nil {
		t.Fatalf("search group DM tenure: %v", err)
	}
	if len(results) != 1 || results[0].Message.MessageID != visible.MessageID {
		t.Fatalf("tenure-scoped results = %+v, want %s", results, visible.MessageID)
	}
}

func TestScopedSearchLimitAndHTTPProjection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, server := newTestServer(t, ctx)
	channel := Place{PlaceID: DefaultGeneralChannelID, Kind: PlaceChannel, WorkspaceID: DefaultWorkspaceID}
	for _, content := range []string{"りんごの話", "りんごとみかん", "りんご狩り"} {
		w.send(t, ctx, channel.PlaceID, w.humanA, content)
	}

	resp, body := call(t, server, http.MethodGet,
		"/messaging/search?q="+url.QueryEscape("りんご")+"&limit=2", w.humanA.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("search HTTP status %d body %v", resp.StatusCode, body)
	}
	results, ok := body["results"].([]any)
	if !ok || len(results) != 2 {
		t.Fatalf("limited HTTP results = %v, want 2", body)
	}
	hit := results[0].(map[string]any)
	if _, exists := hit["content"]; exists {
		t.Fatalf("search result leaked full content: %v", hit)
	}
	if _, exists := hit["snippet"]; !exists {
		t.Fatalf("search result omitted snippet: %v", hit)
	}

	for _, path := range []string{
		"/messaging/search?q=",
		"/messaging/search?q=" + url.QueryEscape("りんご") + "&limit=0",
	} {
		resp, body = call(t, server, http.MethodGet, path, w.humanA.ID, nil)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s status %d body %v, want 400", path, resp.StatusCode, body)
		}
	}

	dm, _, err := w.store.EnsureDM(ctx, w.humanA, w.agent)
	if err != nil {
		t.Fatalf("ensure private search fixture: %v", err)
	}
	w.send(t, ctx, dm.PlaceID, w.humanA, "ひみつのりんご")
	resp, body = call(t, server, http.MethodGet,
		"/messaging/search?q="+url.QueryEscape("りんご")+"&place_id="+dm.PlaceID,
		w.humanB.ID, nil)
	if resp.StatusCode != http.StatusNotFound || body["error"] != "not_found" {
		t.Fatalf("invisible place HTTP search status %d body %v, want 404 not_found", resp.StatusCode, body)
	}
}

func TestScopedSearchCapsLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	for i := 0; i <= MaxSearchLimit; i++ {
		w.send(t, ctx, channel.PlaceID, w.humanA, "limit cap fixture")
	}
	search := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	results, err := search.SearchMessages(ctx, "fixture", SearchOptions{Limit: MaxSearchLimit + 1})
	if err != nil {
		t.Fatalf("capped search: %v", err)
	}
	if len(results) != MaxSearchLimit {
		t.Fatalf("capped search returned %d results, want %d", len(results), MaxSearchLimit)
	}
}
