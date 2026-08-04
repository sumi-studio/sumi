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

func TestSearchMatchesJapaneseSubstringsAndRanks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	w.send(t, ctx, ch.PlaceID, w.humanA, "おはようございます")
	target := w.send(t, ctx, ch.PlaceID, w.humanB, "今日の予定を共有します")
	w.send(t, ctx, ch.PlaceID, w.agent, "Deploy DONE")

	results, err := w.store.SearchMessages(ctx, w.humanA, "予定", SearchOptions{})
	if err != nil {
		t.Fatalf("search 予定: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("search 予定: %d results, want 1", len(results))
	}
	hit := results[0]
	if hit.Message.MessageID != target.MessageID || hit.Message.Seq != target.Seq {
		t.Fatalf("search 予定: hit %+v, want message %s seq %d", hit.Message, target.MessageID, target.Seq)
	}
	if hit.Place.PlaceID != ch.PlaceID || hit.Place.Kind != PlaceChannel {
		t.Fatalf("search 予定: place %+v, want channel %s", hit.Place, ch.PlaceID)
	}
	if !strings.Contains(hit.Snippet, "予定") {
		t.Fatalf("search 予定: snippet %q does not contain the query", hit.Snippet)
	}
	if hit.Message.Author != w.humanB {
		t.Fatalf("search 予定: author %+v, want %+v", hit.Message.Author, w.humanB)
	}
	if hit.Message.CreatedAt.IsZero() {
		t.Fatal("search 予定: created_at is zero")
	}

	// ILIKE is case-insensitive for ASCII too.
	results, err = w.store.SearchMessages(ctx, w.humanA, "deploy", SearchOptions{})
	if err != nil {
		t.Fatalf("search deploy: %v", err)
	}
	if len(results) != 1 || results[0].Message.Content != "Deploy DONE" {
		t.Fatalf("search deploy: results %+v, want the Deploy DONE message", results)
	}

	// No match is an empty result, not an error.
	results, err = w.store.SearchMessages(ctx, w.humanA, "ぜんぜん違う話", SearchOptions{})
	if err != nil {
		t.Fatalf("search miss: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("search miss: %d results, want 0", len(results))
	}

	// LIKE metacharacters are matched literally, never as wildcards.
	results, err = w.store.SearchMessages(ctx, w.humanA, "%", SearchOptions{})
	if err != nil {
		t.Fatalf("search %%: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("search %%: %d results, want 0 (literal percent)", len(results))
	}
}

func TestSearchSkipsTombstonesAndClipsSnippets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	doomed := w.send(t, ctx, ch.PlaceID, w.humanA, "この秘密はすぐ消えます")
	long := "前置きが続きます。" + strings.Repeat("あ", 200) + "ここが検索の的です。" + strings.Repeat("い", 200)
	w.send(t, ctx, ch.PlaceID, w.humanA, long)

	if _, err := w.store.DeleteMessage(ctx, ch.PlaceID, doomed.MessageID, w.humanA); err != nil {
		t.Fatalf("delete: %v", err)
	}
	results, err := w.store.SearchMessages(ctx, w.humanA, "秘密", SearchOptions{})
	if err != nil {
		t.Fatalf("search after delete: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("search after delete: %d results, want 0 (tombstones are invisible)", len(results))
	}

	results, err = w.store.SearchMessages(ctx, w.humanA, "検索の的", SearchOptions{})
	if err != nil {
		t.Fatalf("search long: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("search long: %d results, want 1", len(results))
	}
	snippet := results[0].Snippet
	if !strings.Contains(snippet, "検索の的") {
		t.Fatalf("snippet %q does not contain the match", snippet)
	}
	if got, limit := len([]rune(snippet)), 2*searchSnippetRadius+len([]rune("検索の的"))+2; got > limit {
		t.Fatalf("snippet is %d runes, want at most %d", got, limit)
	}
	if !strings.HasPrefix(snippet, "…") || !strings.HasSuffix(snippet, "…") {
		t.Fatalf("snippet %q should be elided on both sides", snippet)
	}
}

func TestSearchEnforcesVisibilityAndPlaceScope(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	w.send(t, ctx, ch.PlaceID, w.humanA, "共有の会議メモ")

	// A dm between humanA and the agent is invisible to humanB.
	dm, _, err := w.store.EnsureDM(ctx, w.humanA, w.agent)
	if err != nil {
		t.Fatalf("ensure dm: %v", err)
	}
	w.send(t, ctx, dm.PlaceID, w.humanA, "ひみつの会議メモ")

	results, err := w.store.SearchMessages(ctx, w.humanB, "会議メモ", SearchOptions{})
	if err != nil {
		t.Fatalf("search as humanB: %v", err)
	}
	if len(results) != 1 || results[0].Place.PlaceID != ch.PlaceID {
		t.Fatalf("humanB results %+v, want only the channel hit", results)
	}
	results, err = w.store.SearchMessages(ctx, w.humanA, "会議メモ", SearchOptions{})
	if err != nil {
		t.Fatalf("search as humanA: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("humanA results %d, want 2 (channel and dm)", len(results))
	}

	// place_id scoping: only hits from that place.
	results, err = w.store.SearchMessages(ctx, w.humanA, "会議メモ", SearchOptions{PlaceID: dm.PlaceID})
	if err != nil {
		t.Fatalf("search dm scope: %v", err)
	}
	if len(results) != 1 || results[0].Place.PlaceID != dm.PlaceID {
		t.Fatalf("dm-scoped results %+v, want only the dm hit", results)
	}

	// A place the viewer cannot see is not found, never an empty result.
	if _, err := w.store.SearchMessages(ctx, w.humanB, "会議メモ", SearchOptions{PlaceID: dm.PlaceID}); !errors.Is(err, ErrPlaceNotFound) {
		t.Fatalf("humanB dm-scoped search: err %v, want ErrPlaceNotFound", err)
	}

	// The agent participant searches through the identical path.
	results, err = w.store.SearchMessages(ctx, w.agent, "ひみつ", SearchOptions{})
	if err != nil {
		t.Fatalf("search as agent: %v", err)
	}
	if len(results) != 1 || results[0].Place.PlaceID != dm.PlaceID {
		t.Fatalf("agent results %+v, want the dm hit", results)
	}
}

func TestSearchValidatesQueryAndLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	for _, content := range []string{"りんごの話", "りんごとみかん", "りんご狩り"} {
		w.send(t, ctx, ch.PlaceID, w.humanA, content)
	}

	if _, err := w.store.SearchMessages(ctx, w.humanA, "   ", SearchOptions{}); err == nil {
		t.Fatal("blank query: want an error")
	}
	if _, err := w.store.SearchMessages(ctx, w.humanA, strings.Repeat("あ", 100), SearchOptions{}); err == nil {
		t.Fatal("oversized query: want an error")
	}
	results, err := w.store.SearchMessages(ctx, w.humanA, "りんご", SearchOptions{Limit: 2})
	if err != nil {
		t.Fatalf("limited search: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("limited search: %d results, want 2", len(results))
	}
}

func TestSearchOverHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	target := w.send(t, ctx, ch.PlaceID, w.humanB, "明日の予定はこちらです")
	dm, _, err := w.store.EnsureDM(ctx, w.humanA, w.agent)
	if err != nil {
		t.Fatalf("ensure dm: %v", err)
	}
	w.send(t, ctx, dm.PlaceID, w.humanA, "ひみつの予定")

	resp, body := call(t, ts, http.MethodGet,
		"/messaging/search?q="+url.QueryEscape("予定"), w.humanA.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("search: status %d body %v", resp.StatusCode, body)
	}
	results := body["results"].([]any)
	if len(results) != 2 {
		t.Fatalf("search: %d results, want 2", len(results))
	}

	// humanB does not see the dm hit, and the wire shape carries the
	// permalink identity plus snippet/author/created_at.
	resp, body = call(t, ts, http.MethodGet,
		"/messaging/search?q="+url.QueryEscape("予定"), w.humanB.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("search as humanB: status %d body %v", resp.StatusCode, body)
	}
	results = body["results"].([]any)
	if len(results) != 1 {
		t.Fatalf("search as humanB: %d results, want 1", len(results))
	}
	hit := results[0].(map[string]any)
	if hit["message_id"] != target.MessageID {
		t.Fatalf("hit message_id = %v, want %v", hit["message_id"], target.MessageID)
	}
	if int64(hit["seq"].(float64)) != target.Seq {
		t.Fatalf("hit seq = %v, want %d", hit["seq"], target.Seq)
	}
	place := hit["place"].(map[string]any)
	if place["kind"] != "channel" || place["channel_id"] != ch.PlaceID {
		t.Fatalf("hit place = %v", place)
	}
	author := hit["author"].(map[string]any)
	if author["kind"] != "human" || author["human_id"] != w.humanB.ID {
		t.Fatalf("hit author = %v", author)
	}
	if !strings.Contains(hit["snippet"].(string), "予定") {
		t.Fatalf("hit snippet = %v", hit["snippet"])
	}
	if _, ok := hit["created_at"].(string); !ok {
		t.Fatalf("hit created_at = %v", hit["created_at"])
	}

	// place_id scoping over HTTP; an invisible place is 404.
	resp, body = call(t, ts, http.MethodGet,
		"/messaging/search?q="+url.QueryEscape("予定")+"&place_id="+dm.PlaceID, w.humanA.ID, nil)
	if resp.StatusCode != http.StatusOK || len(body["results"].([]any)) != 1 {
		t.Fatalf("dm-scoped search: status %d body %v", resp.StatusCode, body)
	}
	resp, _ = call(t, ts, http.MethodGet,
		"/messaging/search?q="+url.QueryEscape("予定")+"&place_id="+dm.PlaceID, w.humanB.ID, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("invisible place_id: status %d, want 404", resp.StatusCode)
	}

	// Request validation fails closed.
	resp, _ = call(t, ts, http.MethodGet, "/messaging/search?q=", w.humanA.ID, nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("empty q: status %d, want 400", resp.StatusCode)
	}
	resp, _ = call(t, ts, http.MethodGet,
		"/messaging/search?q="+url.QueryEscape("予定")+"&limit=0", w.humanA.ID, nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("limit=0: status %d, want 400", resp.StatusCode)
	}
	resp, _ = call(t, ts, http.MethodGet, "/messaging/search?q="+url.QueryEscape("予定"), "", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no session: status %d, want 401", resp.StatusCode)
	}
}
