package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

type unusedFirebaseVerifier struct{}

func (unusedFirebaseVerifier) VerifyIDToken(context.Context, string) (agentevents.FirebaseIdentity, error) {
	return agentevents.FirebaseIdentity{}, fmt.Errorf("not used")
}

// wsWorld wires the REST server and WS server onto the same store and hub,
// like cmd/server does.
func newWSWorld(t *testing.T, ctx context.Context) (world, *httptest.Server) {
	t.Helper()
	w := newWorld(t, ctx)
	hub := NewHub(w.store)
	rest := NewServer(w.store, stubSessions{})
	rest.AllowedOrigins = []string{testOrigin}
	rest.Hub = hub
	ws := NewWSServer(w.store, stubSessions{}, hub)
	ws.AllowedOrigins = []string{testOrigin}
	mux := http.NewServeMux()
	rest.RegisterRoutes(mux)
	mux.Handle("GET /messaging/ws", ws)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return w, ts
}

func dialWS(t *testing.T, ts *httptest.Server, cookie string, cursors map[string]int64) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/messaging/ws"
	header := http.Header{}
	header.Set("Origin", testOrigin)
	header.Set("Cookie", agentevents.BrowserSessionCookie+"="+cookie)
	conn, resp, err := websocket.DefaultDialer.Dial(url, header)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("dial ws: %v (status %d)", err, status)
	}
	t.Cleanup(func() { conn.Close() })
	if cursors == nil {
		cursors = map[string]int64{}
	}
	if err := conn.WriteJSON(map[string]any{"type": "hello", "cursors": cursors}); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	frame := readFrame(t, conn)
	if frame["type"] != "hello_ack" {
		t.Fatalf("expected hello_ack, got %v", frame)
	}
	return conn
}

func readFrame(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var frame map[string]any
	if err := conn.ReadJSON(&frame); err != nil {
		t.Fatalf("read frame: %v", err)
	}
	return frame
}

func TestWSRejectsBadOriginAndSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newWSWorld(t, ctx)
	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/messaging/ws"

	// Wrong origin.
	header := http.Header{}
	header.Set("Origin", "https://evil.example")
	header.Set("Cookie", agentevents.BrowserSessionCookie+"="+w.humanA.ID)
	if _, resp, err := websocket.DefaultDialer.Dial(url, header); err == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("wrong origin must be rejected with 403, got err=%v", err)
	}
	// Missing cookie.
	header = http.Header{}
	header.Set("Origin", testOrigin)
	if _, resp, err := websocket.DefaultDialer.Dial(url, header); err == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("missing cookie must be rejected with 401, got err=%v", err)
	}
}

func TestLogoutClosesMessagingSocketAndRevocationFencesCachedHubEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const (
		humanID = "01913f5e-7b8a-7abc-8def-0123456789ab"
		agentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
		placeID = "cached-place"
	)

	commandStore, err := agentevents.OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = commandStore.Close() })
	revocations, err := agentevents.OpenDurableGateway(t.TempDir(), commandStore)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := agentevents.NewHMACUserSessionVerifier(
		[]byte("0123456789abcdef0123456789abcdef"),
		agentevents.DefaultBrowserAudience(),
		revocations,
	)
	if err != nil {
		t.Fatal(err)
	}
	claims := agentevents.UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             humanID,
		PersonalityAgentID: agentID,
	}
	binding, err := agentevents.NewStaticIdentityBindingResolver("unused", claims)
	if err != nil {
		t.Fatal(err)
	}
	auth, err := agentevents.NewBrowserAuthServer(
		unusedFirebaseVerifier{}, binding, sessions, []string{testOrigin}, false,
	)
	if err != nil {
		t.Fatal(err)
	}

	hub := NewHub(nil)
	ws := NewWSServer(nil, sessions, hub)
	ws.AllowedOrigins = []string{testOrigin}
	auth.Connections = ws
	mux := http.NewServeMux()
	auth.RegisterRoutes(mux)
	mux.Handle("GET /messaging/ws", ws)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	issueAndPrime := func(t *testing.T) (string, *websocket.Conn) {
		t.Helper()
		cookie, issueErr := sessions.IssueSession(ctx, claims, time.Minute)
		if issueErr != nil {
			t.Fatal(issueErr)
		}
		conn := dialWS(t, ts, cookie, nil)
		hub.mu.Lock()
		for sub := range hub.subscribers {
			sub.markVisible(placeID, true)
		}
		hub.mu.Unlock()
		return cookie, conn
	}

	t.Run("logout eagerly closes the registered socket", func(t *testing.T) {
		cookie, conn := issueAndPrime(t)
		csrfRequest, _ := http.NewRequest(http.MethodGet, ts.URL+"/auth/csrf", nil)
		csrfRequest.Header.Set("Origin", testOrigin)
		csrfResponse, requestErr := http.DefaultClient.Do(csrfRequest)
		if requestErr != nil {
			t.Fatal(requestErr)
		}
		var csrfBody struct {
			Token string `json:"csrf_token"`
		}
		if err := json.NewDecoder(csrfResponse.Body).Decode(&csrfBody); err != nil {
			t.Fatal(err)
		}
		_ = csrfResponse.Body.Close()
		var csrfCookie *http.Cookie
		for _, candidate := range csrfResponse.Cookies() {
			if candidate.Name == agentevents.BrowserCSRFCookie {
				csrfCookie = candidate
			}
		}
		if csrfCookie == nil {
			t.Fatal("missing CSRF cookie")
		}
		logoutRequest, _ := http.NewRequest(http.MethodPost, ts.URL+"/auth/logout", nil)
		logoutRequest.Header.Set("Origin", testOrigin)
		logoutRequest.Header.Set("X-CSRF-Token", csrfBody.Token)
		logoutRequest.AddCookie(csrfCookie)
		logoutRequest.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: cookie})
		logoutResponse, requestErr := http.DefaultClient.Do(logoutRequest)
		if requestErr != nil {
			t.Fatal(requestErr)
		}
		_ = logoutResponse.Body.Close()
		if logoutResponse.StatusCode != http.StatusNoContent {
			t.Fatalf("logout status = %d", logoutResponse.StatusCode)
		}
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		if _, _, err := conn.ReadMessage(); err == nil {
			t.Fatal("revoked messaging socket remained readable")
		}
	})

	t.Run("shared revocation blocks a queued event despite cached visibility", func(t *testing.T) {
		cookie, conn := issueAndPrime(t)
		if _, err := sessions.RevokeSession(ctx, cookie); err != nil {
			t.Fatal(err)
		}
		hub.Publish(ctx, Event{Type: EventTyping, PlaceID: placeID})
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		if _, _, err := conn.ReadMessage(); err == nil {
			t.Fatal("revoked session received a positive-cached hub event")
		}
	})
}

func TestWSSendReceiptAndFanOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newWSWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)

	sender := dialWS(t, ts, w.humanA.ID, nil)
	receiver := dialWS(t, ts, w.humanB.ID, nil)

	if err := sender.WriteJSON(map[string]any{
		"type": "send", "place_id": ch.PlaceID,
		"content": "@Kuro（Yohaku） WSからこんにちは", "client_nonce": "ws-nonce-1",
	}); err != nil {
		t.Fatalf("write send: %v", err)
	}

	// Sender gets the receipt, then the fan-out event.
	receipt := readFrame(t, sender)
	if receipt["type"] != "receipt" || receipt["client_nonce"] != "ws-nonce-1" ||
		receipt["seq"].(float64) != 1 || receipt["created"] != true {
		t.Fatalf("receipt = %v", receipt)
	}
	senderEvent := readFrame(t, sender)
	if senderEvent["type"] != "event" {
		t.Fatalf("sender event = %v", senderEvent)
	}

	// The other member sees message_created with resolved mentions.
	frame := readFrame(t, receiver)
	if frame["type"] != "event" {
		t.Fatalf("receiver frame = %v", frame)
	}
	event := frame["event"].(map[string]any)
	if event["type"] != EventMessageCreated || event["place_id"] != ch.PlaceID {
		t.Fatalf("event = %v", event)
	}
	msg := event["message"].(map[string]any)
	if msg["content"] != "@Kuro（Yohaku） WSからこんにちは" || len(msg["mentions"].([]any)) != 1 {
		t.Fatalf("message = %v", msg)
	}

	// A retry gets the original receipt and no second fan-out.
	if err := sender.WriteJSON(map[string]any{
		"type": "send", "place_id": ch.PlaceID,
		"content": "@Kuro（Yohaku） WSからこんにちは", "client_nonce": "ws-nonce-1",
	}); err != nil {
		t.Fatalf("write retry: %v", err)
	}
	retry := readFrame(t, sender)
	if retry["type"] != "receipt" || retry["created"] != false || retry["seq"].(float64) != 1 {
		t.Fatalf("retry receipt = %v", retry)
	}
}

func TestWSCatchUpReplaysFromCursor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newWSWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	for i := 1; i <= 4; i++ {
		w.send(t, ctx, ch.PlaceID, w.humanB, fmt.Sprintf("メッセージ %d", i))
	}

	conn := dialWS(t, ts, w.humanA.ID, map[string]int64{ch.PlaceID: 2})
	var seqs []float64
	for {
		frame := readFrame(t, conn)
		switch frame["type"] {
		case "event":
			event := frame["event"].(map[string]any)
			seqs = append(seqs, event["message"].(map[string]any)["seq"].(float64))
		case "caught_up":
			if frame["place_id"] != ch.PlaceID || frame["latest_seq"].(float64) != 4 {
				t.Fatalf("caught_up = %v", frame)
			}
			if len(seqs) != 2 || seqs[0] != 3 || seqs[1] != 4 {
				t.Fatalf("replayed seqs = %v, want [3 4]", seqs)
			}
			return
		default:
			t.Fatalf("unexpected frame %v", frame)
		}
	}
}

func TestWSDeliveryFollowsPlaceVisibility(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newWSWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)
	dm, err := w.store.EnsureDM(ctx, w.humanA, w.agent)
	if err != nil {
		t.Fatalf("ensure dm: %v", err)
	}

	// humanB is in the workspace but not in the dm.
	outsider := dialWS(t, ts, w.humanB.ID, nil)
	insider := dialWS(t, ts, w.humanA.ID, nil)

	if err := insider.WriteJSON(map[string]any{
		"type": "send", "place_id": dm.PlaceID,
		"content": "二人だけの話", "client_nonce": "dm-nonce-1",
	}); err != nil {
		t.Fatalf("write send: %v", err)
	}
	if frame := readFrame(t, insider); frame["type"] != "receipt" {
		t.Fatalf("insider receipt = %v", frame)
	}
	if frame := readFrame(t, insider); frame["type"] != "event" {
		t.Fatalf("insider event = %v", frame)
	}

	// The outsider must not receive the dm event. Prove the socket is still
	// live and ordered by sending something the outsider can see afterwards.
	_, ch2 := w.workspaceWithChannel(t, ctx)
	if err := insider.WriteJSON(map[string]any{
		"type": "send", "place_id": ch2.PlaceID,
		"content": "全員向け", "client_nonce": "ch-nonce-1",
	}); err != nil {
		t.Fatalf("write channel send: %v", err)
	}
	frame := readFrame(t, outsider)
	if frame["type"] != "event" {
		t.Fatalf("outsider frame = %v", frame)
	}
	event := frame["event"].(map[string]any)
	if event["place_id"] != ch2.PlaceID {
		t.Fatalf("outsider must only see the channel event, got %v", event)
	}
}

func TestRESTSendReachesWSSubscribers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newWSWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)

	conn := dialWS(t, ts, w.humanB.ID, nil)
	resp, _ := call(t, ts, http.MethodPost, "/messaging/places/"+ch.PlaceID+"/messages",
		w.humanA.ID, map[string]any{"content": "RESTから", "client_nonce": "rest-nonce-1"})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("rest send: status %d", resp.StatusCode)
	}
	frame := readFrame(t, conn)
	if frame["type"] != "event" {
		t.Fatalf("frame = %v", frame)
	}
	event := frame["event"].(map[string]any)
	if event["type"] != EventMessageCreated ||
		event["message"].(map[string]any)["content"] != "RESTから" {
		t.Fatalf("event = %v", event)
	}
}

func TestWSTypingIsVolatileAndScoped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newWSWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)

	typist := dialWS(t, ts, w.humanA.ID, nil)
	watcher := dialWS(t, ts, w.humanB.ID, nil)

	if err := typist.WriteJSON(map[string]any{"type": "typing", "place_id": ch.PlaceID}); err != nil {
		t.Fatalf("write typing: %v", err)
	}
	frame := readFrame(t, watcher)
	event := frame["event"].(map[string]any)
	if event["type"] != EventTyping || event["actor"].(map[string]any)["human_id"] != w.humanA.ID {
		t.Fatalf("typing event = %v", event)
	}
}
