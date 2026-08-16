package messaging

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

const (
	testLiveKitKey    = "sumi-local"
	testLiveKitSecret = "a-secret-long-enough-for-hs256-signing"
)

func testLiveKit() LiveKitConfig {
	return LiveKitConfig{
		URL: "ws://127.0.0.1:7880", APIKey: testLiveKitKey, APISecret: testLiveKitSecret,
	}
}

func TestCallAccessTokenCarriesOnlyTheAuthenticatedParticipantGrant(t *testing.T) {
	now := time.Unix(1_780_000_000, 0)
	if CallTokenTTL != 5*time.Minute {
		t.Fatalf("CallTokenTTL = %s, want a five-minute join credential", CallTokenTTL)
	}
	token, err := testLiveKit().accessToken("place-1", "human:abc", "Yohaku", now, CallTokenTTL)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := verifyJWT(token, testLiveKitSecret)
	if err != nil {
		t.Fatal(err)
	}
	var claims livekitClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatal(err)
	}
	if claims.Issuer != testLiveKitKey || claims.Subject != "human:abc" || claims.Name != "Yohaku" {
		t.Fatalf("identity claims = %+v", claims)
	}
	if claims.Video.Room != "place-1" || !claims.Video.RoomJoin ||
		!claims.Video.CanPublish || !claims.Video.CanSubscribe || !claims.Video.CanPublishData {
		t.Fatalf("video grant = %+v", claims.Video)
	}
	if claims.Expiry != now.Add(CallTokenTTL).Unix() {
		t.Fatalf("expiry = %d", claims.Expiry)
	}
	if _, err := verifyJWT(token, "wrong-secret"); err == nil {
		t.Fatal("token verified with another secret")
	}
}

func signCallWebhook(t *testing.T, body []byte, now time.Time) string {
	t.Helper()
	digest := sha256.Sum256(body)
	token, err := signJWT(livekitClaims{
		Issuer: testLiveKitKey, NotBefore: now.Unix(), Expiry: now.Add(time.Hour).Unix(),
		SHA256: base64.StdEncoding.EncodeToString(digest[:]),
	}, testLiveKitSecret)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func TestCallWebhookRejectsSignaturesAndRebuildsState(t *testing.T) {
	now := time.Unix(1_780_000_000, 0)
	service := &CallService{
		Server: &Server{}, LiveKit: testLiveKit(), Registry: NewCallRegistry(),
		Now: func() time.Time { return now },
	}
	mux := http.NewServeMux()
	service.RegisterRoutes(mux)
	body := []byte(`{"event":"participant_joined","room":{"name":"place-1"},"participant":{"identity":"human:01900000-0000-7000-8000-0000000000aa"}}`)

	for name, authorization := range map[string]string{
		"unsigned":          "Bearer not-a-token",
		"wrong body digest": "Bearer " + signCallWebhook(t, []byte(`{}`), now),
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/messaging/livekit/webhook", strings.NewReader(string(body)))
			request.Header.Set("Authorization", authorization)
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, request)
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
	if _, ok := service.Registry.snapshot("place-1"); ok {
		t.Fatal("rejected webhook changed state")
	}

	for _, authorization := range []string{
		// livekit-server v1.8 sends the signed JWT directly, while accepting
		// Bearer keeps compatibility with SDKs and reverse proxies that add it.
		signCallWebhook(t, body, now),
		"Bearer " + signCallWebhook(t, body, now),
	} {
		request := httptest.NewRequest(http.MethodPost, "/messaging/livekit/webhook", strings.NewReader(string(body)))
		request.Header.Set("Authorization", authorization)
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)
		if response.Code != http.StatusNoContent {
			t.Fatalf("signed status=%d body=%s", response.Code, response.Body.String())
		}
	}
	state, ok := service.Registry.snapshot("place-1")
	if !ok || !state.Active || len(state.Participants) != 1 ||
		state.Participants[0].Participant.Key() != "human:01900000-0000-7000-8000-0000000000aa" {
		t.Fatalf("rebuilt state = %+v", state)
	}
}

func TestCallRegistryFoldsRoomParticipantAndScreenEvents(t *testing.T) {
	now := time.Unix(1_780_000_000, 0)
	registry := NewCallRegistry()
	alice := Human("01900000-0000-7000-8000-0000000000aa")
	bob := Human("01900000-0000-7000-8000-0000000000bb")
	registry.open("place-1", now)
	registry.join("place-1", alice, now)
	state := registry.join("place-1", bob, now.Add(time.Second))
	state = registry.join("place-1", bob, now.Add(2*time.Second))
	if len(state.Participants) != 2 {
		t.Fatalf("duplicate webhook created %d participants", len(state.Participants))
	}
	if _, changed := registry.setScreenShare("place-1", bob, true); !changed {
		t.Fatal("screen-share publication did not change state")
	}
	state, _ = registry.leave("place-1", alice)
	if !state.Active || len(state.Participants) != 1 || !state.Participants[0].ScreenShare {
		t.Fatalf("participant fold = %+v", state)
	}
	state = registry.close("place-1")
	if state.Active {
		t.Fatal("finished room remained active")
	}
}

func scopedCallURL(base, path string, scope Scope) string {
	query := url.Values{
		"workspace_id":    {scope.WorkspaceID},
		"installation_id": {scope.InstallationID},
		"authority_epoch": {strconv.FormatInt(scope.AuthorityEpoch, 10)},
	}
	return base + path + "?" + query.Encode()
}

func issueCallToken(t *testing.T, serverURL, placeID, cookie string, scope Scope) (*http.Response, map[string]any) {
	t.Helper()
	request, err := http.NewRequest(
		http.MethodPost,
		scopedCallURL(serverURL, "/messaging/places/"+placeID+"/call/token", scope),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Origin", testOrigin)
	request.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: cookie})
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var body map[string]any
	_ = json.NewDecoder(response.Body).Decode(&body)
	return response, body
}

func TestCallTokenAdmissionTracksCurrentMessagingAuthority(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	fixture := newScopedContractFixture(t, ctx, w, "calls", w.humanB)
	ownerScope := fixture.scope(t, w, w.humanA)
	memberScope := fixture.scope(t, w, w.humanB)
	channel, err := ownerScope.CreateChannel(ctx, "通話", "", true)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(w.store.core, stubSessions{})
	server.AllowedOrigins = []string{testOrigin}
	server.Hub = NewHub(w.store.core)
	calls := NewCallService(server, testLiveKit())
	server.Calls = calls
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	calls.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	response, body := issueCallToken(t, ts.URL, channel.PlaceID, w.humanB.ID, memberScope.Scope)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("authorized status=%d body=%v", response.StatusCode, body)
	}

	payload, err := verifyJWT(body["token"].(string), testLiveKitSecret)
	if err != nil {
		t.Fatal(err)
	}
	var claims livekitClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatal(err)
	}
	if claims.Subject != w.humanB.Key() || body["identity"] != w.humanB.Key() {
		t.Fatalf("transport actor was not authoritative: claims=%+v body=%v", claims, body)
	}

	nonVoice, err := ownerScope.CreateChannel(ctx, "general", "", false)
	if err != nil {
		t.Fatal(err)
	}
	response, body = issueCallToken(t, ts.URL, nonVoice.PlaceID, w.humanB.ID, memberScope.Scope)
	if response.StatusCode != http.StatusForbidden || body["error"] != "forbidden" {
		t.Fatalf("non-voice channel token status=%d body=%v", response.StatusCode, body)
	}

	if err := w.store.RemoveWorkspaceMember(ctx, fixture.workspace.WorkspaceID, w.humanB); err != nil {
		t.Fatal(err)
	}
	response, body = issueCallToken(t, ts.URL, channel.PlaceID, w.humanB.ID, memberScope.Scope)
	if response.StatusCode != http.StatusNotFound || body["error"] != "not_found" {
		t.Fatalf("membership loss status=%d body=%v, want opaque 404", response.StatusCode, body)
	}

	response, body = issueCallToken(t, ts.URL, channel.PlaceID, "revoked:"+w.humanA.ID, ownerScope.Scope)
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expired session status=%d body=%v", response.StatusCode, body)
	}

	if _, err := w.apps.SetEnabledByID(ctx, fixture.installation.InstallationID, w.humanA, false); err != nil {
		t.Fatal(err)
	}
	response, body = issueCallToken(t, ts.URL, channel.PlaceID, w.humanA.ID, ownerScope.Scope)
	if response.StatusCode == http.StatusOK {
		t.Fatalf("disabled installation issued token: %v", body)
	}
	reenabled, err := w.apps.SetEnabledByID(ctx, fixture.installation.InstallationID, w.humanA, true)
	if err != nil {
		t.Fatal(err)
	}
	response, body = issueCallToken(t, ts.URL, channel.PlaceID, w.humanA.ID, ownerScope.Scope)
	if response.StatusCode != http.StatusNotFound || body["error"] != "installation_not_found" {
		t.Fatalf("stale epoch status=%d body=%v", response.StatusCode, body)
	}
	current, err := w.store.core.Scoped(Scope{
		WorkspaceID: fixture.workspace.WorkspaceID, InstallationID: reenabled.InstallationID,
		AuthorityEpoch: reenabled.AuthorityEpoch, Actor: w.humanA,
	})
	if err != nil {
		t.Fatal(err)
	}
	response, body = issueCallToken(t, ts.URL, channel.PlaceID, w.humanA.ID, current.Scope)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("current epoch status=%d body=%v", response.StatusCode, body)
	}
}

type stubLiveKitRoomService struct {
	rooms              []liveKitRoom
	participants       map[string][]liveKitParticipant
	err                error
	onListParticipants func(string)
	removed            *[][2]string
}

func (s stubLiveKitRoomService) ListRooms(context.Context) ([]liveKitRoom, error) {
	return s.rooms, s.err
}

func (s stubLiveKitRoomService) ListParticipants(_ context.Context, room string) ([]liveKitParticipant, error) {
	if s.onListParticipants != nil {
		s.onListParticipants(room)
	}
	return s.participants[room], s.err
}

func (s stubLiveKitRoomService) RemoveParticipant(_ context.Context, room, identity string) error {
	if s.removed != nil {
		*s.removed = append(*s.removed, [2]string{room, identity})
	}
	return s.err
}

func TestCallRegistryRebuildsFromLiveKitRoomServiceAfterRestart(t *testing.T) {
	service := &CallService{
		Registry: NewCallRegistry(),
		Now:      func() time.Time { return time.Unix(1_780_000_100, 0) },
		RoomService: stubLiveKitRoomService{
			rooms: []liveKitRoom{{Name: "place-1", CreatedAt: 1_780_000_000}},
			participants: map[string][]liveKitParticipant{
				"place-1": {{
					Identity: "human:01900000-0000-7000-8000-0000000000aa", JoinedAt: 1_780_000_001,
					Tracks: []struct {
						Source string `json:"source"`
					}{{Source: "SCREEN_SHARE"}},
				}},
			},
		},
	}
	if err := service.rebuildRegistry(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, ok := service.Registry.snapshot("place-1")
	if !ok || !state.Active || state.StartedAt.Unix() != 1_780_000_000 || len(state.Participants) != 1 ||
		!state.Participants[0].ScreenShare || state.Participants[0].JoinedAt.Unix() != 1_780_000_001 {
		t.Fatalf("rebuilt state = %+v", state)
	}
}

func TestCallRegistryRebuildPreservesWebhookAppliedDuringSnapshot(t *testing.T) {
	now := time.Unix(1_780_000_100, 0)
	registry := NewCallRegistry()
	webhookParticipant := Human("01900000-0000-7000-8000-0000000000bb")
	service := &CallService{
		Registry: registry,
		Now:      func() time.Time { return now },
		RoomService: stubLiveKitRoomService{
			rooms: []liveKitRoom{{Name: "place-1", CreatedAt: 1_780_000_000}},
			participants: map[string][]liveKitParticipant{
				"place-1": {{Identity: "human:01900000-0000-7000-8000-0000000000aa", JoinedAt: 1_780_000_001}},
			},
			onListParticipants: func(string) {
				// This is the previously unsafe interleaving: RoomService has
				// already been read, then LiveKit delivers a newer webhook.
				registry.join("place-1", webhookParticipant, now)
			},
		},
	}
	if err := service.rebuildRegistry(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, ok := registry.snapshot("place-1")
	if !ok || len(state.Participants) != 1 || state.Participants[0].Participant != webhookParticipant {
		t.Fatalf("newer webhook state was overwritten by snapshot: %+v", state)
	}
}

func TestCallServiceRemovesClosedWorkspaceMemberFromLiveKitRooms(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	fixture := newScopedContractFixture(t, ctx, w, "call-removal", w.humanB)
	owner := fixture.scope(t, w, w.humanA)
	channel, err := owner.CreateChannel(ctx, "通話", "", true)
	if err != nil {
		t.Fatal(err)
	}
	removed := [][2]string{}
	service := &CallService{
		Server:   NewServer(w.store.core, nil),
		Registry: NewCallRegistry(),
		RoomService: stubLiveKitRoomService{
			rooms: []liveKitRoom{{Name: channel.PlaceID}, {Name: "01900000-0000-7000-8000-0000000000ff"}},
			participants: map[string][]liveKitParticipant{
				channel.PlaceID: {{Identity: w.humanB.Key()}},
			},
			removed: &removed,
		},
	}
	if err := service.RemoveWorkspaceParticipant(ctx, fixture.workspace.WorkspaceID, w.humanB); err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0] != [2]string{channel.PlaceID, w.humanB.Key()} {
		t.Fatalf("RemoveParticipant calls = %+v", removed)
	}
}

func TestCallRegistryRebuildUsesLiveKitTwirpRoomService(t *testing.T) {
	now := time.Unix(1_780_000_000, 0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			t.Fatal("RoomService request was unsigned")
		}
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		payload, err := verifyJWT(token, testLiveKitSecret)
		if err != nil {
			t.Fatal(err)
		}
		var claims livekitClaims
		if err := json.Unmarshal(payload, &claims); err != nil {
			t.Fatal(err)
		}
		switch r.URL.Path {
		case "/twirp/livekit.RoomService/ListRooms":
			if !claims.Video.RoomList || claims.Video.RoomAdmin || claims.Video.Room != "" {
				t.Fatalf("ListRooms grant = %+v", claims.Video)
			}
			_, _ = w.Write([]byte(`{"rooms":[{"name":"place-1","creationTime":1780000000}]}`))
		case "/twirp/livekit.RoomService/ListParticipants":
			if claims.Video.RoomList || !claims.Video.RoomAdmin || claims.Video.Room != "place-1" {
				t.Fatalf("ListParticipants grant = %+v", claims.Video)
			}
			_, _ = w.Write([]byte(`{"participants":[{"identity":"human:01900000-0000-7000-8000-0000000000aa","joinedAt":1780000001}]}`))
		default:
			t.Fatalf("unexpected RoomService path %s", r.URL.Path)
		}
	}))
	defer server.Close()
	service := NewCallService(nil, LiveKitConfig{
		URL: strings.Replace(server.URL, "http://", "ws://", 1), APIKey: testLiveKitKey, APISecret: testLiveKitSecret,
	})
	service.Now = func() time.Time { return now }
	if err := service.rebuildRegistry(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state, ok := service.Registry.snapshot("place-1"); !ok || len(state.Participants) != 1 {
		t.Fatalf("twirp rebuilt state = %+v", state)
	}
}

func TestCallTokenInvisiblePrivatePlaceIs404Not403(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	fixture := newScopedContractFixture(t, ctx, w, "private-call", w.humanB, w.agent)
	owner := fixture.scope(t, w, w.humanA)
	dm, _, err := owner.EnsureDM(ctx, w.agent)
	if err != nil {
		t.Fatal(err)
	}
	viewer := fixture.scope(t, w, w.humanB)
	server := NewServer(w.store.core, stubSessions{})
	server.AllowedOrigins = []string{testOrigin}
	calls := NewCallService(server, testLiveKit())
	mux := http.NewServeMux()
	calls.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()
	response, body := issueCallToken(t, ts.URL, dm.PlaceID, w.humanB.ID, viewer.Scope)
	if response.StatusCode != http.StatusNotFound || body["error"] != "not_found" {
		t.Fatalf("invisible DM status=%d body=%v", response.StatusCode, body)
	}
}

func TestCallStateReadFiltersInvisiblePlaces(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	fixture := newScopedContractFixture(t, ctx, w, "state", w.humanB, w.agent)
	owner := fixture.scope(t, w, w.humanA)
	channel, err := owner.CreateChannel(ctx, "general", "", false)
	if err != nil {
		t.Fatal(err)
	}
	dm, _, err := owner.EnsureDM(ctx, w.agent)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(w.store.core, stubSessions{})
	server.AllowedOrigins = []string{testOrigin}
	calls := NewCallService(server, testLiveKit())
	calls.RoomService = stubLiveKitRoomService{
		rooms: []liveKitRoom{
			{Name: channel.PlaceID, CreatedAt: time.Now().Unix()},
			{Name: dm.PlaceID, CreatedAt: time.Now().Unix()},
		},
		participants: map[string][]liveKitParticipant{
			channel.PlaceID: {{Identity: w.humanA.Key(), JoinedAt: time.Now().Unix()}},
			dm.PlaceID:      {{Identity: w.agent.Key(), JoinedAt: time.Now().Unix()}},
		},
	}
	visible, err := calls.visibleCalls(ctx, fixture.scope(t, w, w.humanB))
	if err != nil {
		t.Fatal(err)
	}
	if len(visible) != 1 || visible[0].Place.ChannelID != channel.PlaceID {
		t.Fatalf("visible calls = %+v", visible)
	}
}

func TestCallWebhookStatePublishesAsVolatileScopedEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	fixture := newScopedContractFixture(t, ctx, w, "call-live", w.humanB)
	owner := fixture.scope(t, w, w.humanA)
	viewer := fixture.scope(t, w, w.humanB)
	channel, err := owner.CreateChannel(ctx, "作業通話", "", true)
	if err != nil {
		t.Fatal(err)
	}
	hub := NewHub(w.store.core)
	server := NewServer(w.store.core, stubSessions{})
	server.Hub = hub
	calls := NewCallService(server, testLiveKit())
	subscriber := hub.subscribe(viewer)
	defer hub.unsubscribe(subscriber)

	event := livekitWebhookEvent{Event: "participant_joined"}
	event.Room.Name = channel.PlaceID
	event.Participant.Identity = w.humanA.Key()
	calls.applyWebhook(ctx, event)
	select {
	case frame := <-subscriber.send:
		var envelope struct {
			Type  string `json:"type"`
			Event struct {
				Type string         `json:"type"`
				Call map[string]any `json:"call"`
			} `json:"event"`
		}
		if err := json.Unmarshal(frame.payload, &envelope); err != nil {
			t.Fatal(err)
		}
		if envelope.Type != "event" || envelope.Event.Type != EventCallState || envelope.Event.Call == nil {
			t.Fatalf("event = %s", frame.payload)
		}
		if _, durableSeq := envelope.Event.Call["seq"]; durableSeq {
			t.Fatalf("volatile call state acquired a seq: %s", frame.payload)
		}
	case <-ctx.Done():
		t.Fatal("call_state was not published")
	}
}
