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
	if err := w.store.seedDefaultWorkspaceFixture(ctx, w.humanA); err != nil {
		t.Fatal(err)
	}
	if err := w.store.seedDefaultWorkspaceFixture(ctx, w.humanB); err != nil {
		t.Fatal(err)
	}
	if err := w.store.seedDefaultWorkspaceFixture(ctx, w.agent); err != nil {
		t.Fatal(err)
	}
	hub := NewHub(w.store.core)
	rest := NewServer(w.store.core, stubSessions{})
	rest.AllowedOrigins = []string{testOrigin}
	rest.Hub = hub
	ws := NewWSServer(w.store.core, stubSessions{}, hub)
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
	if store, ok := testStoreForParticipant(cookie); ok {
		if scoped, err := store.scopeForActor(context.Background(), Human(cookie)); err == nil {
			url += "?workspace_id=" + scoped.Scope.WorkspaceID + "&installation_id=" + scoped.Scope.InstallationID
		}
	} else if store, ok := testStoreForServer(ts.URL); ok {
		if actor, actorOK := testActorForServer(ts.URL); actorOK {
			if scoped, err := store.scopeForActor(context.Background(), actor); err == nil {
				url += "?workspace_id=" + scoped.Scope.WorkspaceID + "&installation_id=" + scoped.Scope.InstallationID
			}
		}
	}
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
	w := newWorld(t, ctx)
	_, place := w.workspaceWithChannel(t, ctx)
	humanID, agentID, placeID := w.humanA.ID, w.agent.ID, place.PlaceID

	commandStore, err := agentevents.OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = commandStore.Close() })
	revocations, err := agentevents.OpenDurableGateway(privateRuntimeDir(t), commandStore)
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

	hub := NewHub(w.store.core)
	ws := NewWSServer(w.store.core, sessions, hub)
	ws.AllowedOrigins = []string{testOrigin}
	auth.Connections = ws
	mux := http.NewServeMux()
	auth.RegisterRoutes(mux)
	mux.Handle("GET /messaging/ws", ws)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	testStoresByServer.Store(ts.URL, w.store)
	testActorsByServer.Store(ts.URL, w.humanA)

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
		store := w.store.mustScopeForPlace(t, ctx, placeID, w.humanA)
		_ = hub.PublishScoped(ctx, store, Event{Type: EventTyping, PlaceID: placeID})
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
	_, channel := w.workspaceWithChannel(t, ctx)
	dm, _, err := w.store.EnsureDM(ctx, w.humanA, w.agent)
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
	if err := insider.WriteJSON(map[string]any{
		"type": "send", "place_id": channel.PlaceID,
		"content": "全員向け", "client_nonce": "ch-nonce-1",
	}); err != nil {
		t.Fatalf("write channel send: %v", err)
	}
	frame := readFrame(t, outsider)
	if frame["type"] != "event" {
		t.Fatalf("outsider frame = %v", frame)
	}
	event := frame["event"].(map[string]any)
	if event["place_id"] != channel.PlaceID {
		t.Fatalf("outsider must only see the channel event, got %v", event)
	}
}

func TestHubReauthorizesWarmedVisibilityAfterMembershipRemoval(t *testing.T) {
	for _, test := range []struct {
		name        string
		participant func(world) ParticipantRef
	}{
		{name: "human", participant: func(w world) ParticipantRef { return w.humanB }},
		{name: "personality_agent", participant: func(w world) ParticipantRef { return w.agent }},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			w := newWorld(t, ctx)
			ws, ch := w.workspaceWithChannel(t, ctx)
			participant := test.participant(w)
			hub := NewHub(w.store.core)
			sub := hub.subscribe(w.store.mustScopeForPlace(t, ctx, ch.PlaceID, participant))
			defer hub.unsubscribe(sub)

			audience, err := w.store.ActiveParticipantsForPlace(ctx, ch.PlaceID)
			if err != nil {
				t.Fatalf("load initial audience: %v", err)
			}
			if _, visible := audience[participant]; !visible {
				t.Fatal("member was not initially authorized")
			}
			sub.markVisible(ch.PlaceID, true)
			if cached, known := sub.visibility(ch.PlaceID); !known || !cached {
				t.Fatal("positive visibility observation was not warmed")
			}

			msg := w.send(t, ctx, ch.PlaceID, w.humanA, "revocation後に漏らしてはいけない")
			wire := messageToWire(ch, msg)
			if err := w.store.RemoveWorkspaceMember(ctx, ws.WorkspaceID, participant); err != nil {
				t.Fatalf("remove member: %v", err)
			}
			// This is the broad fallback frame (no OnlyFor). Even though the
			// subscriber once had a positive cache entry, current membership must
			// fence the full message body after the removal commit.
			_ = hub.PublishScoped(ctx, w.store.mustScope(t, ctx, ws.WorkspaceID, w.humanA), Event{
				Type: EventMessageCreated, PlaceID: ch.PlaceID, Message: &wire,
			})
			if got := len(sub.send); got != 0 {
				t.Fatalf("removed %s received %d queued content frames", participant.Key(), got)
			}
		})
	}
}

type countingHubAuthorizer struct {
	placeCalls       int
	participantCalls int
	audience         map[ParticipantRef]struct{}
	store            *testMessagingStore
}

func (a *countingHubAuthorizer) withLiveAudience(
	ctx context.Context,
	scope Scope,
	boundary liveBoundary,
	requireActor bool,
	deliver func(map[ParticipantRef]struct{}) error,
) error {
	if boundary.placeID != "" {
		a.placeCalls++
	} else {
		a.participantCalls++
	}
	if a.store != nil {
		return a.store.core.withLiveAudience(ctx, scope, boundary, requireActor, deliver)
	}
	return deliver(a.audience)
}

func TestHubBatchesAuthorizationAndVariantFanout(t *testing.T) {
	const subscriberCount = 500
	authorizer := &countingHubAuthorizer{audience: map[ParticipantRef]struct{}{}}
	hub := newHub(authorizer)
	subs := make([]*subscriber, 0, subscriberCount)
	for i := 0; i < subscriberCount; i++ {
		participant := Human(fmt.Sprintf("participant-%d", i))
		authorizer.audience[participant] = struct{}{}
		subs = append(subs, hub.subscribe(participant))
	}
	events := make([]Event, 0, subscriberCount+1)
	notified := make([]ParticipantRef, 0, subscriberCount)
	for _, sub := range subs {
		participant := sub.viewer
		notified = append(notified, participant)
		events = append(events, Event{
			Type: EventMessageCreated, PlaceID: "place", OnlyFor: &participant,
		})
	}
	events = append(events, Event{
		Type: EventMessageCreated, PlaceID: "place", ExceptFor: notified,
	})
	hub.PublishVariants(context.Background(), events)
	if authorizer.placeCalls != 1 {
		t.Fatalf("place authorization queries = %d, want one batched lookup", authorizer.placeCalls)
	}
	for _, sub := range subs {
		if got := len(sub.send); got != 1 {
			t.Fatalf("subscriber %s received %d variants, want exactly one", sub.viewer.Key(), got)
		}
	}

	subject := Human("subject")
	hub.Publish(context.Background(), Event{Type: EventStatusUpdated, Subject: &subject})
	if authorizer.participantCalls != 1 {
		t.Fatalf("participant authorization queries = %d, want one batched lookup", authorizer.participantCalls)
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

func TestPlaceLifecycleEventsReachWSSubscribers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newWSWorld(t, ctx)
	ws, _ := w.workspaceWithChannel(t, ctx)

	observer := dialWS(t, ts, w.humanB.ID, nil)

	// Channel creation reaches every workspace member's live socket.
	resp, created := call(t, ts, http.MethodPost, "/messaging/channels", w.humanA.ID,
		map[string]any{"workspace_id": ws.WorkspaceID, "name": "dev", "topic": "開発の相談"})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create channel: status %d body %v", resp.StatusCode, created)
	}
	channelID := created["channel_id"].(string)
	frame := readFrame(t, observer)
	event := frame["event"].(map[string]any)
	if event["type"] != EventPlaceCreated || event["place_id"] != channelID ||
		event["channel"].(map[string]any)["name"] != "dev" {
		t.Fatalf("place_created frame = %v", frame)
	}

	// A dm the observer is not part of stays invisible; the group dm that does
	// include the observer arrives next. Ordering on one socket proves the dm
	// event was skipped, not merely delayed.
	resp, _ = call(t, ts, http.MethodPost, "/messaging/dms", w.humanA.ID,
		map[string]any{"participant": map[string]any{"kind": "personality_agent", "personality_agent_id": w.agent.ID}})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ensure dm: status %d", resp.StatusCode)
	}
	resp, groupDM := call(t, ts, http.MethodPost, "/messaging/group-dms", w.humanA.ID,
		map[string]any{"participants": []any{
			map[string]any{"kind": "human", "human_id": w.humanB.ID},
			map[string]any{"kind": "personality_agent", "personality_agent_id": w.agent.ID},
		}})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create group dm: status %d body %v", resp.StatusCode, groupDM)
	}
	frame = readFrame(t, observer)
	event = frame["event"].(map[string]any)
	if event["type"] != EventPlaceCreated || event["place_id"] != groupDM["dm_id"] ||
		event["dm"].(map[string]any)["kind"] != PlaceGroupDM {
		t.Fatalf("group dm frame = %v", frame)
	}

	// Topic edits fan out as place_updated.
	resp, _ = call(t, ts, http.MethodPatch, "/messaging/places/"+channelID, w.humanA.ID,
		map[string]any{"topic": "レビューはこちら"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("patch topic: status %d", resp.StatusCode)
	}
	frame = readFrame(t, observer)
	event = frame["event"].(map[string]any)
	if event["type"] != EventPlaceUpdated || event["place_id"] != channelID ||
		event["channel"].(map[string]any)["topic"] != "レビューはこちら" {
		t.Fatalf("place_updated frame = %v", frame)
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
