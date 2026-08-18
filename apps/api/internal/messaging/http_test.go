package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

const testOrigin = "https://app.sumi.test"

// stubSessions maps the cookie value directly to the session's HumanID so
// tests act as different humans by switching cookies. Empty cookie values are
// rejected like an invalid signature would be.
type stubSessions struct{}

func (stubSessions) VerifySession(_ context.Context, cookie string) (agentevents.UserSessionClaims, error) {
	if cookie == "" || strings.HasPrefix(cookie, "revoked:") {
		return agentevents.UserSessionClaims{}, fmt.Errorf("invalid session")
	}
	return agentevents.UserSessionClaims{TenantID: "tenant-1", UserID: cookie}, nil
}

func (stubSessions) AuthorizeSession(ctx context.Context, claims agentevents.UserSessionClaims, op func() error) error {
	return op()
}

func newTestServer(t *testing.T, ctx context.Context) (world, *httptest.Server) {
	t.Helper()
	w := newWorld(t, ctx)
	for _, participant := range []ParticipantRef{w.humanA, w.humanB, w.agent} {
		if err := w.store.seedDefaultWorkspaceFixture(ctx, participant); err != nil {
			t.Fatalf("prepare default test Workspace: %v", err)
		}
	}
	server := NewServer(w.store.core, stubSessions{})
	server.AllowedOrigins = []string{testOrigin}
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	testStoresByServer.Store(ts.URL, w.store)
	t.Cleanup(ts.Close)
	return w, ts
}

// call issues a request as the given participant (via the stub cookie).
func call(t *testing.T, ts *httptest.Server, method, path, cookie string, body any) (*http.Response, map[string]any) {
	t.Helper()
	requestBody := map[string]any{}
	if supplied, ok := body.(map[string]any); ok {
		for key, value := range supplied {
			requestBody[key] = value
		}
	}
	if cookie != "" && !strings.HasPrefix(cookie, "revoked:") {
		store, ok := testStoreForParticipant(cookie)
		if !ok {
			store, ok = testStoreForServer(ts.URL)
		}
		if ok {
			if scoped, err := store.fixtureScopeForRequest(context.Background(), Human(cookie), path, requestBody); err == nil {
				parsed, parseErr := url.Parse(path)
				if parseErr != nil {
					t.Fatalf("parse request path: %v", parseErr)
				}
				query := parsed.Query()
				query.Set("workspace_id", scoped.Scope.WorkspaceID)
				query.Set("installation_id", scoped.Scope.InstallationID)
				query.Set("authority_epoch", strconv.FormatInt(scoped.Scope.AuthorityEpoch, 10))
				parsed.RawQuery = query.Encode()
				path = parsed.String()
			}
		}
	}
	var reader *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(requestBody)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, ts.URL+path, reader)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Origin", testOrigin)
	if cookie != "" {
		req.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: cookie})
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	var decoded map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&decoded)
	return resp, decoded
}

func TestMessagingRoutesFailClosedOnOriginAndSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)

	// Browser GET fetches may omit Origin and remain valid with a session.
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/messaging/bootstrap", nil)
	req.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: w.humanA.ID})
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("GET without exact scope: status %d, want 400", resp.StatusCode)
	}

	// Unsafe methods still fail closed before request-body or resource checks.
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/messaging/places/"+newUUIDv7()+"/messages", strings.NewReader(`{}`))
	req.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: w.humanA.ID})
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unsafe request: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("unsafe request without origin: status %d, want 403", resp.StatusCode)
	}

	// No cookie.
	resp, _ = call(t, ts, http.MethodGet, "/messaging/bootstrap", "", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("missing cookie: status %d, want 401", resp.StatusCode)
	}

	// Rejected session.
	resp, _ = call(t, ts, http.MethodGet, "/messaging/bootstrap", "revoked:"+w.humanA.ID, nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("revoked session: status %d, want 401", resp.StatusCode)
	}
}

func TestMessagingJSONUnknownFieldsReturnInvalidJSON(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)

	// JSON request bodies share decodeJSON. Verify representative creation,
	// send, edit, and reaction endpoints use its strict unknown-field result
	// instead of introducing endpoint-specific input-error classifications.
	for _, test := range []struct {
		name   string
		method string
		path   string
		body   map[string]any
	}{
		{
			name:   "create channel",
			method: http.MethodPost,
			path:   "/messaging/channels",
			body:   map[string]any{"name": "design", "unknown": true},
		},
		{
			name:   "send",
			method: http.MethodPost,
			path:   "/messaging/places/" + ch.PlaceID + "/messages",
			body:   map[string]any{"content": "hello", "client_nonce": "unknown-send", "unknown": true},
		},
		{
			name:   "edit",
			method: http.MethodPatch,
			path:   "/messaging/places/" + ch.PlaceID + "/messages/" + newUUIDv7(),
			body:   map[string]any{"content": "hello", "unknown": true},
		},
		{
			name:   "reaction",
			method: http.MethodPost,
			path:   "/messaging/places/" + ch.PlaceID + "/messages/" + newUUIDv7() + "/reactions",
			body:   map[string]any{"emoji": "👍", "client_nonce": "unknown-reaction", "unknown": true},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			resp, body := call(t, ts, test.method, test.path, w.humanA.ID, test.body)
			if resp.StatusCode != http.StatusBadRequest || body["error"] != "invalid_json" {
				t.Fatalf("unknown field: status %d body %v", resp.StatusCode, body)
			}
		})
	}
}

func TestMessagingJSONTrailingGarbageReturnsInvalidJSONBeforeMutation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	path := scopedPath(t, ts, w.humanA.ID, "/messaging/places/"+ch.PlaceID+"/messages")

	// This is one valid object plus an invalid top-level suffix. It must not
	// reach the send mutation (and therefore must not allocate a sequence).
	req, err := http.NewRequest(http.MethodPost, ts.URL+path,
		strings.NewReader(`{"content":"must not send","client_nonce":"trailing-json"}]`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Origin", testOrigin)
	req.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: w.humanA.ID})
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusBadRequest || body["error"] != "invalid_json" {
		t.Fatalf("trailing JSON: status %d body %v", resp.StatusCode, body)
	}
	history, err := w.store.History(ctx, ch.PlaceID, w.humanA, HistoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 0 {
		t.Fatalf("trailing JSON reached mutation: %+v", history)
	}
}

func TestEditPatchRejectsStaleRevision(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "編集前")
	path := "/messaging/places/" + ch.PlaceID + "/messages/" + msg.MessageID

	response, _ := call(t, ts, http.MethodPatch, path, w.humanA.ID, map[string]any{
		"content": "先に保存された本文", "revision": 1,
	})
	if response.StatusCode != http.StatusOK {
		t.Fatalf("first PATCH: status %d, want 200", response.StatusCode)
	}
	response, body := call(t, ts, http.MethodPatch, path, w.humanA.ID, map[string]any{
		"content": "古い書きかけ", "revision": 1,
	})
	if response.StatusCode != http.StatusConflict || body["error"] != "edit_conflict" {
		t.Fatalf("stale PATCH: status %d body %v, want 409 edit_conflict", response.StatusCode, body)
	}
}

func TestBootstrapProjectsPlacesMembersAndUnread(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	if _, _, err := w.store.EnsureDM(ctx, w.humanA, w.agent); err != nil {
		t.Fatalf("ensure dm: %v", err)
	}
	w.send(t, ctx, ch.PlaceID, w.humanB, "@Yohaku 見て")

	resp, body := call(t, ts, http.MethodGet, "/messaging/bootstrap", w.humanA.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bootstrap: status %d body %v", resp.StatusCode, body)
	}
	self := body["self"].(map[string]any)
	if self["kind"] != "human" || self["human_id"] != w.humanA.ID {
		t.Fatalf("self = %v", self)
	}
	if n := len(body["workspaces"].([]any)); n != 1 {
		t.Fatalf("workspaces = %d, want the explicit workspace", n)
	}
	if n := len(body["channels"].([]any)); n != 1 {
		t.Fatalf("channels = %d, want the explicit channel", n)
	}
	dms := body["dms"].([]any)
	if len(dms) != 1 || len(dms[0].(map[string]any)["participants"].([]any)) != 2 {
		t.Fatalf("dms = %v", dms)
	}
	// Everyone in the workspace appears once with a display name.
	members := body["members"].([]any)
	names := map[string]bool{}
	for _, m := range members {
		names[m.(map[string]any)["display_name"].(string)] = true
	}
	if !names["Yohaku"] || !names["Haru"] || !names["Kuro"] {
		t.Fatalf("members missing display names: %v", members)
	}
	// The channel has one unread mention for the viewer.
	found := false
	for _, u := range body["unread_summaries"].([]any) {
		sum := u.(map[string]any)
		place := sum["place"].(map[string]any)
		if place["kind"] == "channel" && place["channel_id"] == ch.PlaceID {
			found = true
			if sum["unread_count"].(float64) != 1 || sum["mention_count"].(float64) != 1 {
				t.Fatalf("channel summary = %v", sum)
			}
		}
	}
	if !found {
		t.Fatalf("channel summary missing: %v", body["unread_summaries"])
	}
}

func TestSendIsIdempotentAcrossRetriesOverHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)

	path := "/messaging/places/" + ch.PlaceID + "/messages"
	send := map[string]any{"content": "@Kuro（Yohaku） 様子どう？", "client_nonce": "nonce-1", "urgency": "urgent"}
	resp, body := call(t, ts, http.MethodPost, path, w.humanA.ID, send)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("send: status %d body %v", resp.StatusCode, body)
	}
	firstID := body["message_id"].(string)
	if body["client_nonce"] != "nonce-1" || body["created"] != true || len(body) != 4 {
		t.Fatalf("create receipt = %v", body)
	}
	history, err := w.store.History(ctx, ch.PlaceID, w.humanA, HistoryOptions{})
	if err != nil || len(history) != 1 {
		t.Fatalf("history after send = %#v, err %v", history, err)
	}
	if history[0].Urgency != "urgent" || len(history[0].Mentions) != 1 {
		t.Fatalf("stored message = %#v", history[0])
	}

	// Retry with the same nonce: 200, same identity, no second message.
	resp, body = call(t, ts, http.MethodPost, path, w.humanA.ID, send)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("retry: status %d, want 200", resp.StatusCode)
	}
	if body["message_id"] != firstID || body["seq"].(float64) != 1 ||
		body["client_nonce"] != "nonce-1" || body["created"] != false || len(body) != 4 {
		t.Fatalf("retry receipt = %v", body)
	}

	// A stranger is not told the place exists.
	stranger := "018f3f8d-7b2c-7a10-8f9e-00000000ab99"
	resp, body = call(t, ts, http.MethodPost, path, stranger, send)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("stranger send: status %d body %v", resp.StatusCode, body)
	}
}

func TestReadThroughAndHistoryOverHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	for i := 1; i <= 3; i++ {
		w.send(t, ctx, ch.PlaceID, w.humanB, fmt.Sprintf("メッセージ %d", i))
	}

	resp, _ := call(t, ts, http.MethodPut, "/messaging/places/"+ch.PlaceID+"/read-through",
		w.humanA.ID, map[string]any{"seq": 2})
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("read-through: status %d", resp.StatusCode)
	}
	resp, body := call(t, ts, http.MethodPut, "/messaging/places/"+ch.PlaceID+"/read-through",
		w.humanA.ID, map[string]any{"seq": 99})
	if resp.StatusCode != http.StatusBadRequest || body["error"] != "seq_beyond_latest" {
		t.Fatalf("read beyond latest: status %d body %v", resp.StatusCode, body)
	}

	resp, body = call(t, ts, http.MethodGet,
		"/messaging/places/"+ch.PlaceID+"/messages?limit=2&before_seq=3", w.humanA.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("history: status %d", resp.StatusCode)
	}
	messages := body["messages"].([]any)
	if len(messages) != 2 ||
		messages[0].(map[string]any)["seq"].(float64) != 1 ||
		messages[1].(map[string]any)["seq"].(float64) != 2 {
		t.Fatalf("history page = %v", messages)
	}
}

func TestEditAndDeleteMapAuthorizationOverHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	msg := w.send(t, ctx, ch.PlaceID, w.humanB, "元の本文")

	base := "/messaging/places/" + ch.PlaceID + "/messages/" + msg.MessageID
	resp, body := call(t, ts, http.MethodPatch, base, w.humanA.ID, map[string]any{"content": "書き換え"})
	if resp.StatusCode != http.StatusForbidden || body["error"] != "not_author" {
		t.Fatalf("non-author edit: status %d body %v", resp.StatusCode, body)
	}
	resp, body = call(t, ts, http.MethodPatch, base, w.humanB.ID, map[string]any{"content": "本人の編集"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("author edit: status %d body %v", resp.StatusCode, body)
	}
	if body["message"].(map[string]any)["content"] != "本人の編集" {
		t.Fatalf("edited message = %v", body["message"])
	}

	// Owner (humanA) deletes another's message in a channel.
	resp, _ = call(t, ts, http.MethodDelete, base, w.humanA.ID, nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("owner delete: status %d", resp.StatusCode)
	}
	// Editing the tombstone conflicts.
	resp, body = call(t, ts, http.MethodPatch, base, w.humanB.ID, map[string]any{"content": "復活"})
	if resp.StatusCode != http.StatusConflict || body["error"] != "message_deleted" {
		t.Fatalf("edit tombstone: status %d body %v", resp.StatusCode, body)
	}
}

func TestDMEndpointsUseReachability(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)
	w.workspaceWithChannel(t, ctx)

	resp, body := call(t, ts, http.MethodPost, "/messaging/dms", w.humanA.ID,
		map[string]any{"participant": map[string]any{"kind": "personality_agent", "personality_agent_id": w.agent.ID}})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ensure dm: status %d body %v", resp.StatusCode, body)
	}
	dmID := body["dm_id"].(string)
	resp, body = call(t, ts, http.MethodPost, "/messaging/dms", w.humanA.ID,
		map[string]any{"participant": map[string]any{"kind": "personality_agent", "personality_agent_id": w.agent.ID}})
	if resp.StatusCode != http.StatusOK || body["dm_id"] != dmID {
		t.Fatalf("ensure dm again: status %d body %v", resp.StatusCode, body)
	}

	// Unknown participant kinds fail closed at the wire.
	resp, body = call(t, ts, http.MethodPost, "/messaging/dms", w.humanA.ID,
		map[string]any{"participant": map[string]any{"kind": "app", "human_id": w.humanB.ID}})
	if resp.StatusCode != http.StatusBadRequest || body["error"] != "invalid_participant" {
		t.Fatalf("unknown kind: status %d body %v", resp.StatusCode, body)
	}

	// A human outside every shared workspace is unreachable.
	stranger := "018f3f8d-7b2c-7a10-8f9e-00000000ab99"
	resp, body = call(t, ts, http.MethodPost, "/messaging/dms", stranger,
		map[string]any{"participant": map[string]any{"kind": "human", "human_id": w.humanA.ID}})
	if resp.StatusCode != http.StatusNotFound || body["error"] != "not_found" {
		t.Fatalf("unreachable dm: status %d body %v", resp.StatusCode, body)
	}
}

func TestChannelTopicPatchOverHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)
	ws, _ := w.workspaceWithChannel(t, ctx)

	resp, body := call(t, ts, http.MethodPost, "/messaging/channels", w.humanA.ID,
		map[string]any{"workspace_id": ws.WorkspaceID, "name": "design", "topic": "デザインの話"})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create channel: status %d body %v", resp.StatusCode, body)
	}
	channelID := body["channel_id"].(string)

	// Channel management requires the Workspace-owned app capability.
	resp, body = call(t, ts, http.MethodPatch, "/messaging/places/"+channelID, w.humanB.ID,
		map[string]any{"topic": "レビュー予約はこちら"})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("member patch topic: status %d body %v", resp.StatusCode, body)
	}
	resp, body = call(t, ts, http.MethodPatch, "/messaging/places/"+channelID, w.humanA.ID,
		map[string]any{"topic": "レビュー予約はこちら"})
	if resp.StatusCode != http.StatusOK || body["topic"] != "レビュー予約はこちら" {
		t.Fatalf("owner patch topic: status %d body %v", resp.StatusCode, body)
	}

	// The change is durable and projected by bootstrap.
	resp, body = call(t, ts, http.MethodGet, "/messaging/bootstrap", w.humanA.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bootstrap: status %d", resp.StatusCode)
	}
	found := false
	for _, entry := range body["channels"].([]any) {
		channel := entry.(map[string]any)
		if channel["channel_id"] == channelID {
			found = true
			if channel["topic"] != "レビュー予約はこちら" {
				t.Fatalf("bootstrap topic = %v", channel["topic"])
			}
		}
	}
	if !found {
		t.Fatalf("created channel missing from bootstrap: %v", body["channels"])
	}

	// A non-member cannot even learn the channel exists.
	outsider := "018f3f8d-7b2c-7a10-8f9e-00000000ab99"
	resp, body = call(t, ts, http.MethodPatch, "/messaging/places/"+channelID, outsider,
		map[string]any{"topic": "見えないはず"})
	if resp.StatusCode != http.StatusNotFound || body["error"] != "not_found" {
		t.Fatalf("outsider patch: status %d body %v", resp.StatusCode, body)
	}

	// A dm has no topic.
	dm, _, err := w.store.EnsureDM(ctx, w.humanA, w.agent)
	if err != nil {
		t.Fatalf("ensure dm: %v", err)
	}
	resp, body = call(t, ts, http.MethodPatch, "/messaging/places/"+dm.PlaceID, w.humanA.ID,
		map[string]any{"topic": "トピックなし"})
	if resp.StatusCode != http.StatusBadRequest || body["error"] != "not_a_channel" {
		t.Fatalf("dm patch: status %d body %v", resp.StatusCode, body)
	}

	// Oversized topics fail closed before touching the store.
	resp, body = call(t, ts, http.MethodPatch, "/messaging/places/"+channelID,
		w.humanA.ID, map[string]any{"topic": strings.Repeat("あ", 1000)})
	if resp.StatusCode != http.StatusBadRequest || body["error"] != "invalid_topic" {
		t.Fatalf("oversized topic: status %d body %v", resp.StatusCode, body)
	}
}
