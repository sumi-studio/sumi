package messaging

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const (
	testLiveKitKey    = "sumi-local"
	testLiveKitSecret = "a-secret-long-enough-for-hs256-signing"
)

func testLiveKit() LiveKitConfig {
	return LiveKitConfig{URL: "ws://127.0.0.1:7880", APIKey: testLiveKitKey, APISecret: testLiveKitSecret}
}

func TestAccessTokenCarriesTheRoomGrantAndVerifies(t *testing.T) {
	config := testLiveKit()
	now := time.Unix(1_780_000_000, 0)
	token, err := config.accessToken("place-1", "human:abc", "Yohaku", now, CallTokenTTL)
	if err != nil {
		t.Fatalf("mint access token: %v", err)
	}
	payload, err := verifyJWT(token, testLiveKitSecret)
	if err != nil {
		t.Fatalf("verify own token: %v", err)
	}
	var claims livekitClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatalf("decode claims: %v", err)
	}
	if claims.Issuer != testLiveKitKey || claims.Subject != "human:abc" || claims.Name != "Yohaku" {
		t.Fatalf("unexpected identity claims: %+v", claims)
	}
	if claims.Video.Room != "place-1" || !claims.Video.RoomJoin ||
		!claims.Video.CanPublish || !claims.Video.CanSubscribe {
		t.Fatalf("unexpected video grant: %+v", claims.Video)
	}
	if claims.Expiry != now.Add(CallTokenTTL).Unix() {
		t.Fatalf("expiry = %d, want %d", claims.Expiry, now.Add(CallTokenTTL).Unix())
	}
	// A token signed with any other secret must not pass.
	if _, err := verifyJWT(token, "another-secret"); err == nil {
		t.Fatal("token verified under the wrong secret")
	}
}

func TestAccessTokenRefusesAnUnconfiguredDeployment(t *testing.T) {
	var empty LiveKitConfig
	if _, err := empty.accessToken("place-1", "human:abc", "", time.Now(), CallTokenTTL); err == nil {
		t.Fatal("unconfigured LiveKit minted a token")
	}
}

// signWebhook builds the delivery LiveKit would make: a JWT whose sha256 claim
// pins the exact body.
func signWebhook(t *testing.T, body []byte, secret string, now time.Time) string {
	t.Helper()
	digest := sha256.Sum256(body)
	token, err := signJWT(livekitClaims{
		Issuer:    testLiveKitKey,
		NotBefore: now.Unix(),
		Expiry:    now.Add(time.Hour).Unix(),
		SHA256:    base64.StdEncoding.EncodeToString(digest[:]),
	}, secret)
	if err != nil {
		t.Fatalf("sign webhook token: %v", err)
	}
	return token
}

func TestWebhookTokenBindsToItsOwnBody(t *testing.T) {
	config := testLiveKit()
	now := time.Unix(1_780_000_000, 0)
	body := []byte(`{"event":"participant_joined"}`)
	token := signWebhook(t, body, testLiveKitSecret, now)

	if err := config.verifyWebhookToken(token, body, now); err != nil {
		t.Fatalf("verify webhook: %v", err)
	}
	// The same token must not authenticate a different body: replaying a
	// signed envelope around swapped events is exactly what the digest stops.
	if err := config.verifyWebhookToken(token, []byte(`{"event":"room_finished"}`), now); err == nil {
		t.Fatal("webhook token accepted a body it did not cover")
	}
	if err := config.verifyWebhookToken(signWebhook(t, body, "wrong-secret", now), body, now); err == nil {
		t.Fatal("webhook token signed with the wrong secret was accepted")
	}
	if err := config.verifyWebhookToken(token, body, now.Add(2*time.Hour)); err == nil {
		t.Fatal("expired webhook token was accepted")
	}
}

func TestUnsignedTokenIsRejected(t *testing.T) {
	// `alg: none` with an empty signature is the classic JWT bypass.
	header := base64URL([]byte(`{"alg":"none","typ":"JWT"}`))
	payload := base64URL([]byte(`{"iss":"` + testLiveKitKey + `"}`))
	if _, err := verifyJWT(header+"."+payload+".", testLiveKitSecret); err == nil {
		t.Fatal("alg=none token verified")
	}
}

func TestParticipantIdentityRoundTripsTheSharedKey(t *testing.T) {
	human := Human("01900000-0000-7000-8000-0000000000aa")
	parsed, err := participantFromIdentity(human.Key())
	if err != nil {
		t.Fatalf("parse human identity: %v", err)
	}
	if parsed != human {
		t.Fatalf("parsed = %+v, want %+v", parsed, human)
	}
	for _, identity := range []string{"", "human", "human:not-a-uuid", "person:01900000-0000-7000-8000-0000000000aa"} {
		if _, err := participantFromIdentity(identity); err == nil {
			t.Fatalf("identity %q was accepted", identity)
		}
	}
}

func TestRegistryTracksWhoIsInTheCall(t *testing.T) {
	registry := NewCallRegistry()
	now := time.Unix(1_780_000_000, 0)
	alice := Human("01900000-0000-7000-8000-0000000000aa")
	bob := Human("01900000-0000-7000-8000-0000000000bb")

	if _, ok := registry.snapshot("place-1"); ok {
		t.Fatal("a place with no room reported a call")
	}
	registry.open("place-1", now)
	state := registry.join("place-1", alice, now)
	if !state.Active || len(state.Participants) != 1 {
		t.Fatalf("after one join: %+v", state)
	}
	// A duplicated join (LiveKit re-delivering after a reconnect) must not
	// double the person on the tiles.
	state = registry.join("place-1", alice, now.Add(time.Second))
	if len(state.Participants) != 1 {
		t.Fatalf("duplicate join produced %d participants", len(state.Participants))
	}
	state = registry.join("place-1", bob, now.Add(2*time.Second))
	if len(state.Participants) != 2 || state.Participants[0].Participant != alice {
		t.Fatalf("join order lost: %+v", state.Participants)
	}
	if _, changed := registry.setScreenShare("place-1", bob, true); !changed {
		t.Fatal("screen share was not recorded")
	}
	if _, changed := registry.setScreenShare("place-1", bob, true); changed {
		t.Fatal("unchanged screen share announced a change")
	}
	state, _ = registry.leave("place-1", alice)
	if len(state.Participants) != 1 || state.Participants[0].Participant != bob {
		t.Fatalf("after leave: %+v", state.Participants)
	}
	// The room stays open for whoever is still in it.
	if !state.Active {
		t.Fatal("room closed while a participant remained")
	}
	state = registry.close("place-1")
	if state.Active || len(state.Participants) != 0 {
		t.Fatalf("closed room still reports a call: %+v", state)
	}
	if _, ok := registry.snapshot("place-1"); ok {
		t.Fatal("closed room is still registered")
	}
}

func TestRegistrySnapshotDoesNotAliasLiveState(t *testing.T) {
	registry := NewCallRegistry()
	now := time.Unix(1_780_000_000, 0)
	alice := Human("01900000-0000-7000-8000-0000000000aa")
	registry.join("place-1", alice, now)
	state, _ := registry.snapshot("place-1")
	state.Participants[0].ScreenShare = true
	fresh, _ := registry.snapshot("place-1")
	if fresh.Participants[0].ScreenShare {
		t.Fatal("snapshot shared the registry's participant slice")
	}
}

func TestWebhookRouteRejectsAForgedDelivery(t *testing.T) {
	service := &CallService{
		Server:   &Server{},
		LiveKit:  testLiveKit(),
		Registry: NewCallRegistry(),
		Now:      func() time.Time { return time.Unix(1_780_000_000, 0) },
	}
	mux := http.NewServeMux()
	service.RegisterRoutes(mux)

	body := `{"event":"participant_joined","room":{"name":"place-1"},"participant":{"identity":"human:01900000-0000-7000-8000-0000000000aa"}}`
	request := httptest.NewRequest(http.MethodPost, "/messaging/livekit/webhook", strings.NewReader(body))
	request.Header.Set("Authorization", "not-a-token")
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("forged webhook status = %d, body = %s", response.Code, response.Body.String())
	}
	if _, ok := service.Registry.snapshot("place-1"); ok {
		t.Fatal("a forged webhook changed the call state")
	}
}

func TestWebhookRouteAppliesASignedDelivery(t *testing.T) {
	now := time.Unix(1_780_000_000, 0)
	service := &CallService{
		// Hub is nil, so publishing is a no-op and the registry is the only
		// observable effect. That is exactly what this test is about.
		Server:   &Server{},
		LiveKit:  testLiveKit(),
		Registry: NewCallRegistry(),
		Now:      func() time.Time { return now },
	}
	mux := http.NewServeMux()
	service.RegisterRoutes(mux)

	body := []byte(`{"event":"participant_joined","room":{"name":"place-1"},"participant":{"identity":"human:01900000-0000-7000-8000-0000000000aa"}}`)
	request := httptest.NewRequest(http.MethodPost, "/messaging/livekit/webhook", strings.NewReader(string(body)))
	request.Header.Set("Authorization", "Bearer "+signWebhook(t, body, testLiveKitSecret, now))
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("signed webhook status = %d, body = %s", response.Code, response.Body.String())
	}
	state, ok := service.Registry.snapshot("place-1")
	if !ok || len(state.Participants) != 1 {
		t.Fatalf("signed webhook did not register the participant: %+v", state)
	}
}

func TestCallRoutesFailClosedWithoutLiveKit(t *testing.T) {
	service := &CallService{Server: &Server{}, Registry: NewCallRegistry()}
	mux := http.NewServeMux()
	service.RegisterRoutes(mux)
	request := httptest.NewRequest(http.MethodPost, "/messaging/livekit/webhook", strings.NewReader("{}"))
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured webhook status = %d", response.Code)
	}
}

// TestVoiceChannelIsAChannelThatAlsoCarriesText pins the ADR 0012 decision that
// a voice channel is an attribute of a channel, not a separate kind: it keeps
// the timeline, the seq and the unread projection every other channel has.
func TestVoiceChannelIsAChannelThatAlsoCarriesText(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	ws, err := w.store.CreateWorkspace(ctx, "sumi-dev", w.humanA)
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := w.store.AddWorkspaceMember(ctx, ws.WorkspaceID, w.humanB, RoleMember); err != nil {
		t.Fatalf("add member: %v", err)
	}

	voice, err := w.store.CreateChannel(ctx, ws.WorkspaceID, "作業通話", "", w.humanA, true)
	if err != nil {
		t.Fatalf("create voice channel: %v", err)
	}
	if !voice.Voice || voice.Kind != PlaceChannel {
		t.Fatalf("created place = %+v, want a voice channel", voice)
	}
	text, err := w.store.CreateChannel(ctx, ws.WorkspaceID, "general", "", w.humanA, false)
	if err != nil {
		t.Fatalf("create text channel: %v", err)
	}
	if text.Voice {
		t.Fatal("a channel created without voice must not be a voice channel")
	}

	// Reloading must preserve the flag, and the wire must carry it.
	reloaded, err := w.store.PlaceFor(ctx, voice.PlaceID, w.humanA)
	if err != nil {
		t.Fatalf("reload voice channel: %v", err)
	}
	if !reloaded.Voice {
		t.Fatal("voice was not persisted")
	}
	if !channelToWire(reloaded).Voice {
		t.Fatal("voice did not reach the wire")
	}

	// It is still a place where people write, and the unread projection still
	// covers it — that is the whole point of not making it a separate kind.
	w.send(t, ctx, voice.PlaceID, w.humanB, "先に入ってます")
	summaries, err := w.store.UnreadSummaries(ctx, w.humanA)
	if err != nil {
		t.Fatalf("unread summaries: %v", err)
	}
	found := false
	for _, summary := range summaries {
		if summary.Place.PlaceID != voice.PlaceID {
			continue
		}
		found = true
		if !summary.Place.Voice {
			t.Fatal("unread summary lost the voice flag")
		}
		if summary.UnreadCount != 1 {
			t.Fatalf("unread count = %d, want 1", summary.UnreadCount)
		}
	}
	if !found {
		t.Fatal("the voice channel is missing from the unread summaries")
	}
}

// TestVoiceIsRefusedOutsideChannels keeps dm/group_dm from becoming permanent
// voice rooms: they can hold a call, but they are not a place people join.
func TestVoiceIsRefusedOutsideChannels(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	ws, err := w.store.CreateWorkspace(ctx, "sumi-dev", w.humanA)
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := w.store.AddWorkspaceMember(ctx, ws.WorkspaceID, w.humanB, RoleMember); err != nil {
		t.Fatalf("add member: %v", err)
	}
	dm, _, err := w.store.EnsureDM(ctx, w.humanA, w.humanB)
	if err != nil {
		t.Fatalf("ensure dm: %v", err)
	}
	if _, err := w.store.pool.Exec(ctx,
		"UPDATE places SET voice = true WHERE place_id = $1", dm.PlaceID); err == nil {
		t.Fatal("a dm was allowed to become a voice channel")
	}
}
