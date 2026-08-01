package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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
	return agentevents.UserSessionClaims{TenantID: "tenant-1", HumanID: cookie}, nil
}

func (stubSessions) AuthorizeSession(ctx context.Context, claims agentevents.UserSessionClaims, op func() error) error {
	return op()
}

func newTestServer(t *testing.T, ctx context.Context) (world, *httptest.Server) {
	t.Helper()
	w := newWorld(t, ctx)
	server := NewServer(w.store, stubSessions{})
	server.AllowedOrigins = []string{testOrigin}
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return w, ts
}

// call issues a request as the given participant (via the stub cookie).
func call(t *testing.T, ts *httptest.Server, method, path, cookie string, body any) (*http.Response, map[string]any) {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
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

	// No Origin header: rejected before anything else.
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/messaging/bootstrap", nil)
	req.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: w.humanA.ID})
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("missing origin: status %d, want 403", resp.StatusCode)
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

func TestBootstrapProjectsPlacesMembersAndUnread(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	if _, err := w.store.EnsureDM(ctx, w.humanA, w.agent); err != nil {
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
		t.Fatalf("workspaces = %d, want 1", n)
	}
	if n := len(body["channels"].([]any)); n != 1 {
		t.Fatalf("channels = %d, want 1", n)
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
	send := map[string]any{"content": "@Kuro 様子どう？", "client_nonce": "nonce-1", "urgency": "urgent"}
	resp, body := call(t, ts, http.MethodPost, path, w.humanA.ID, send)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("send: status %d body %v", resp.StatusCode, body)
	}
	msg := body["message"].(map[string]any)
	if msg["urgency"] != "urgent" || len(msg["mentions"].([]any)) != 1 {
		t.Fatalf("message = %v", msg)
	}
	firstID := body["message_id"].(string)

	// Retry with the same nonce: 200, same identity, no second message.
	resp, body = call(t, ts, http.MethodPost, path, w.humanA.ID, send)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("retry: status %d, want 200", resp.StatusCode)
	}
	if body["message_id"] != firstID || body["seq"].(float64) != 1 {
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
	if resp.StatusCode != http.StatusForbidden || body["error"] != "not_reachable" {
		t.Fatalf("unreachable dm: status %d body %v", resp.StatusCode, body)
	}
}
