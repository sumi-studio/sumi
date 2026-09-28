package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	firebaseauth "firebase.google.com/go/v4/auth"
	"github.com/gorilla/websocket"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/directchat"
	"github.com/sumi-studio/sumi/apps/api/internal/messaging"
)

var testTokenSecret = []byte("test-secret-32bytes-long-string!!")
var testSessionSecret = []byte("browser-session-secret-32-bytes!!")

const testLocalControlPAID = "0198f0f4-9b72-7000-8000-000000000001"
const testBrowserOrigin = "https://web.example"
const testDirectChatInstallationID = "018f47a2-9b3c-7def-8abc-0123456789ac"
const testDirectChatAuthorityEpoch int64 = 1

type allowDirectChatAuthorizer struct{}

func TestApplicationCloseCancelsBackgroundWorkers(t *testing.T) {
	backgroundCtx, stopBackground := context.WithCancel(context.Background())
	app := &application{
		backgroundCtx:  backgroundCtx,
		stopBackground: stopBackground,
	}

	if err := app.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-backgroundCtx.Done():
	default:
		t.Fatal("application close left its background worker context running")
	}
}

func (allowDirectChatAuthorizer) AuthorizeDirectChat(
	_ context.Context,
	_, _, installationID string,
	authorityEpoch int64,
) error {
	if installationID != testDirectChatInstallationID ||
		authorityEpoch != testDirectChatAuthorityEpoch {
		return agentevents.ErrDirectChatAuthorizationDenied
	}
	return nil
}

func newAuthorizedTestRouter(t *testing.T) (*http.ServeMux, error) {
	t.Helper()
	app, err := newApplicationFromEnv()
	if err != nil {
		return nil, err
	}
	app.browser.SetAuthorizer(allowDirectChatAuthorizer{})
	t.Cleanup(func() { _ = app.Close() })
	return app.publicMux, nil
}

type testTokenClaims struct {
	TenantID           string `json:"tenant_id"`
	PersonalityAgentID string `json:"personality_agent_id"`
	Generation         uint64 `json:"generation"`
	Exp                int64  `json:"exp"`
	Aud                string `json:"aud"`
}

type testSessionClaims struct {
	TenantID           string `json:"tenant_id"`
	UserID             string `json:"user_id"`
	PersonalityAgentID string `json:"personality_agent_id"`
	Iat                int64  `json:"iat"`
	Exp                int64  `json:"exp"`
	Aud                string `json:"aud"`
	SID                string `json:"sid"`
}

type testCommandReceipt struct {
	IdempotencyKey string `json:"idempotency_key"`
	CommandID      string `json:"command_id"`
	Seq            uint64 `json:"seq"`
}

type readyingDirectChatSpawner struct {
	gateway *agentevents.BrowserJournal
}

func (*readyingDirectChatSpawner) Touch(string) {}

type fakeFirebaseIDTokenClient struct {
	token *firebaseauth.Token
	err   error
	calls int
}

func (f *fakeFirebaseIDTokenClient) VerifyIDTokenAndCheckRevoked(
	_ context.Context,
	_ string,
) (*firebaseauth.Token, error) {
	f.calls++
	return f.token, f.err
}

func signTestToken(t *testing.T, secret []byte, claims testTokenClaims) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claimsBytes, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	claimsPart := base64.RawURLEncoding.EncodeToString(claimsBytes)
	signingInput := header + "." + claimsPart
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signingInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signingInput + "." + sig
}

func signTestSession(t *testing.T, secret []byte, claims testSessionClaims) string {
	t.Helper()
	if claims.Iat == 0 && claims.Exp != 0 {
		claims.Iat = claims.Exp - int64(time.Hour/time.Second)
	}
	issuer, err := agentevents.NewHMACBrowserSessionIssuer(secret, claims.Aud)
	if err != nil {
		t.Fatalf("construct browser session issuer: %v", err)
	}
	session, err := issuer.IssueSession(
		context.Background(),
		agentevents.UserSessionClaims{
			TenantID:           claims.TenantID,
			UserID:             claims.UserID,
			PersonalityAgentID: claims.PersonalityAgentID,
		},
		time.Duration(claims.Exp-claims.Iat)*time.Second,
	)
	if err != nil {
		t.Fatalf("issue browser session: %v", err)
	}
	return session
}

func setTokenSecret(t *testing.T) {
	t.Helper()
	t.Setenv("SUMI_BROWSER_EVENT_DIR", privateRuntimeDir(t))
}

// An explicit core backend with no core state service must not silently keep
// the legacy runtime and call it core: startup fails closed instead.
func TestDirectChatCoreBackendWithoutCoreStateServiceFails(t *testing.T) {
	setTokenSecret(t)
	setSessionSecret(t)
	t.Setenv("SUMI_COMMAND_LOG_DIR", t.TempDir())
	_, err := newApplicationFromEnv()
	if err == nil || !strings.Contains(err.Error(), "Sumi requires the core state service") {
		t.Fatalf("explicit core backend without the core state service must fail clearly, got %v", err)
	}
}

func setSessionSecret(t *testing.T) {
	t.Helper()
	t.Setenv("SUMI_BROWSER_SESSION_SECRET", base64.StdEncoding.EncodeToString(testSessionSecret))
	t.Setenv("SUMI_BROWSER_SESSION_AUDIENCE", agentevents.DefaultBrowserAudience())
	t.Setenv("SUMI_BROWSER_WS_ALLOWED_ORIGINS", testBrowserOrigin)
	t.Setenv("SUMI_BROWSER_EVENT_DIR", privateRuntimeDir(t))
}

func testBrowserSessionRevocationStore(
	t *testing.T,
) agentevents.BrowserSessionRevocationStore {
	t.Helper()
	store, err := agentevents.OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gateway, err := agentevents.OpenBrowserJournal(privateRuntimeDir(t), store)
	if err != nil {
		t.Fatal(err)
	}
	return gateway
}

func postAuthorized(t *testing.T, serverURL, personalityAgentID string, body []byte) *http.Response {
	t.Helper()
	token := signTestToken(t, testTokenSecret, testTokenClaims{
		TenantID:           "tenant-1",
		PersonalityAgentID: personalityAgentID,
		Generation:         7,
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                "sumi:agent:events",
	})
	req, err := http.NewRequest(http.MethodPost, serverURL+"/direct-chat/commands?installation_id="+testDirectChatInstallationID+"&authority_epoch=1", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "test-key")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Origin", testBrowserOrigin)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	return resp
}

func postWithSessionCookie(t *testing.T, serverURL, personalityAgentID string, body []byte) *http.Response {
	return postWithSessionCookieAndKey(t, serverURL, personalityAgentID, "test-key", body)
}

func postWithSessionCookieAndKey(t *testing.T, serverURL, personalityAgentID, idempotencyKey string, body []byte) *http.Response {
	t.Helper()
	session := signTestSession(t, testSessionSecret, testSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: personalityAgentID,
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                agentevents.DefaultBrowserAudience(),
	})
	req, err := http.NewRequest(http.MethodPost, serverURL+"/direct-chat/commands?installation_id="+testDirectChatInstallationID+"&authority_epoch=1", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempotencyKey)
	req.Header.Set("Origin", testBrowserOrigin)
	req.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: session})
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	return resp
}

func TestBrowserSessionConfigurationRejectsEveryPartialGroup(t *testing.T) {
	clearBrowserConfiguration(t)
	for _, tc := range []struct {
		name  string
		env   string
		value string
	}{
		{name: "secret only", env: "SUMI_BROWSER_SESSION_SECRET", value: base64.StdEncoding.EncodeToString(testSessionSecret)},
		{name: "audience only", env: "SUMI_BROWSER_SESSION_AUDIENCE", value: agentevents.DefaultBrowserAudience()},
		{name: "origins only", env: "SUMI_BROWSER_WS_ALLOWED_ORIGINS", value: testBrowserOrigin},
		{name: "auth dependency only", env: "SUMI_AUTH_FIREBASE_PROJECT_ID", value: "firebase-user"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearBrowserConfiguration(t)
			t.Setenv(tc.env, tc.value)
			if _, _, err := browserSessionConfigFromEnv(
				testBrowserSessionRevocationStore(t),
			); err == nil {
				t.Fatal("partial browser-session configuration did not fail startup")
			}
		})
	}

	t.Run("complete group", func(t *testing.T) {
		clearBrowserConfiguration(t)
		t.Setenv("SUMI_BROWSER_SESSION_SECRET", base64.StdEncoding.EncodeToString(testSessionSecret))
		t.Setenv("SUMI_BROWSER_SESSION_AUDIENCE", agentevents.DefaultBrowserAudience())
		t.Setenv("SUMI_BROWSER_WS_ALLOWED_ORIGINS", testBrowserOrigin)
		sessions, origins, err := browserSessionConfigFromEnv(
			testBrowserSessionRevocationStore(t),
		)
		if err != nil {
			t.Fatal(err)
		}
		if sessions == nil || len(origins) != 1 || origins[0] != testBrowserOrigin {
			t.Fatalf("unexpected complete browser config: sessions=%v origins=%v", sessions, origins)
		}
	})
}

func clearBrowserConfiguration(t *testing.T) {
	t.Helper()
	for _, name := range append([]string{
		"SUMI_BROWSER_SESSION_SECRET",
		"SUMI_BROWSER_SESSION_AUDIENCE",
		"SUMI_BROWSER_WS_ALLOWED_ORIGINS",
	}, browserAuthEnvironmentNames...) {
		t.Setenv(name, "")
	}
}

func TestApplicationCloseOwnsAndDrainsHijackedBrowserSocketsBeforeStoreClose(t *testing.T) {
	store, err := agentevents.OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := agentevents.OpenBrowserJournal(privateRuntimeDir(t), store)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	sessions, err := agentevents.NewHMACUserSessionVerifier(
		testSessionSecret,
		"",
		testBrowserSessionRevocationStore(t),
	)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	browser := agentevents.NewBrowserServer(sessions, runtime, runtime)
	browser.AllowedOrigins = []string{testBrowserOrigin}
	browser.SetAuthorizer(allowDirectChatAuthorizer{})
	browser.SetLifecycleFence(directchat.NewLifecycleFence())
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", browser)
	server := httptest.NewServer(mux)
	defer server.Close()

	session, err := sessions.IssueSession(context.Background(), agentevents.UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}, time.Minute)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	wsURL := strings.Replace(server.URL, "http", "ws", 1) + "/direct-chat/ws?installation_id=" + testDirectChatInstallationID + "&authority_epoch=1"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{
		"Origin": {testBrowserOrigin},
		"Cookie": {agentevents.BrowserSessionCookie + "=" + session},
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.WriteJSON(map[string]any{"type": "hello", "last_event_seq": 0}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for browser.ConnectionStats().Active != 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if browser.ConnectionStats().Active != 1 {
		_ = store.Close()
		t.Fatal("browser socket was not retained by the gateway")
	}
	// Active means the hijacked handler owns the connection; it does not mean
	// the hello response has reached the client. Consume that response before
	// shutdown so the next successful read cannot be a frame queued before
	// Close.
	conn.SetReadDeadline(time.Now().Add(time.Second))
	var status struct {
		Type   string `json:"type"`
		Status string `json:"status"`
	}
	if err := conn.ReadJSON(&status); err != nil {
		_ = store.Close()
		t.Fatalf("read browser status before shutdown: %v", err)
	}
	if status.Type != "direct_chat_status" || status.Status != "ready" {
		_ = store.Close()
		t.Fatalf("browser status before shutdown = %+v", status)
	}

	app := &application{store: store, browser: browser}
	if err := app.Close(); err != nil {
		t.Fatalf("application close: %v", err)
	}
	if stats := browser.ConnectionStats(); stats.Active != 0 {
		t.Fatalf("application close returned before browser drain: %+v", stats)
	}
	conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("hijacked browser socket remained open after application close")
	} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatalf("application shutdown did not close hijacked socket: %v", err)
	}
	if err := app.Close(); err != nil {
		t.Fatalf("idempotent application close: %v", err)
	}
}

func TestFirebaseAdminVerifierChecksRevocationAndReturnsTenant(t *testing.T) {
	client := &fakeFirebaseIDTokenClient{token: &firebaseauth.Token{
		UID:      "firebase-user",
		AuthTime: time.Now().Add(-time.Minute).Unix(),
		IssuedAt: time.Now().Add(-30 * time.Second).Unix(),
		Claims: map[string]interface{}{
			"email":          "Human@Example.com",
			"email_verified": true,
			"name":           "Verified Human",
		},
		Firebase: firebaseauth.FirebaseInfo{
			Tenant: "firebase-tenant", SignInProvider: "github.com",
			Identities: map[string]interface{}{"github.com": []interface{}{"github-subject"}},
		},
	}}
	verifier := &firebaseAdminIDTokenVerifier{client: client}
	identity, err := verifier.VerifyIDToken(context.Background(), "id-token")
	if err != nil {
		t.Fatal(err)
	}
	if client.calls != 1 {
		t.Fatalf("revocation-aware verification calls = %d, want 1", client.calls)
	}
	if identity.UID != "firebase-user" || identity.TenantID != "firebase-tenant" {
		t.Fatalf("unexpected identity: %+v", identity)
	}
	if identity.Email != "Human@Example.com" || identity.DisplayName != "Verified Human" || !identity.EmailVerified ||
		identity.SignInProvider != "github.com" ||
		len(identity.ProviderSubjects["github.com"]) != 1 ||
		identity.ProviderSubjects["github.com"][0] != "github-subject" || identity.AuthTime.IsZero() || identity.IssuedAt.IsZero() ||
		identity.IssuedAt.Unix() != client.token.IssuedAt {
		t.Fatalf("verified proof claims were not preserved: %+v", identity)
	}
}

func TestLiveKitConfigIsOptionalButNeverPartial(t *testing.T) {
	for _, name := range []string{"SUMI_LIVEKIT_URL", "SUMI_LIVEKIT_API_URL", "SUMI_LIVEKIT_API_KEY", "SUMI_LIVEKIT_API_SECRET"} {
		t.Setenv(name, "")
	}
	if _, enabled, err := liveKitConfigFromEnv(); err != nil || enabled {
		t.Fatalf("empty config: enabled=%v err=%v", enabled, err)
	}
	t.Setenv("SUMI_LIVEKIT_URL", "wss://calls.sumi.test")
	t.Setenv("SUMI_LIVEKIT_API_KEY", "key")
	if _, _, err := liveKitConfigFromEnv(); err == nil {
		t.Fatal("partial config was accepted")
	}
	t.Setenv("SUMI_LIVEKIT_API_SECRET", "secret")
	config, enabled, err := liveKitConfigFromEnv()
	if err != nil || !enabled || config.URL != "wss://calls.sumi.test" {
		t.Fatalf("complete config: %+v enabled=%v err=%v", config, enabled, err)
	}
	t.Setenv("SUMI_LIVEKIT_URL", "https://calls.sumi.test")
	if _, _, err := liveKitConfigFromEnv(); err == nil {
		t.Fatal("non-WebSocket signalling URL was accepted")
	}
	t.Setenv("SUMI_LIVEKIT_URL", "wss://calls.sumi.test")
	t.Setenv("SUMI_LIVEKIT_API_URL", "ws://calls.sumi.test")
	if _, _, err := liveKitConfigFromEnv(); err == nil {
		t.Fatal("non-HTTP RoomService URL was accepted")
	}
}

func TestAuthSessionTTLFromEnvIsShortAndBounded(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Setenv("SUMI_AUTH_SESSION_TTL", "")
		got, err := authSessionTTLFromEnv()
		if err != nil || got != 15*time.Minute {
			t.Fatalf("got %v, %v", got, err)
		}
	})
	t.Run("configured", func(t *testing.T) {
		t.Setenv("SUMI_AUTH_SESSION_TTL", "20m")
		got, err := authSessionTTLFromEnv()
		if err != nil || got != 20*time.Minute {
			t.Fatalf("got %v, %v", got, err)
		}
	})
	t.Run("overlong", func(t *testing.T) {
		t.Setenv("SUMI_AUTH_SESSION_TTL", "61m")
		if _, err := authSessionTTLFromEnv(); err == nil {
			t.Fatal("expected overlong session TTL to fail")
		}
	})
	t.Run("too short", func(t *testing.T) {
		t.Setenv("SUMI_AUTH_SESSION_TTL", "30s")
		if _, err := authSessionTTLFromEnv(); err == nil {
			t.Fatal("expected sub-minute session TTL to fail")
		}
	})
}

func TestNewRouter_RequiresCommandLogDir(t *testing.T) {
	t.Setenv("SUMI_COMMAND_LOG_DIR", "")
	_, err := newRouter()
	if err == nil {
		t.Fatal("expected newRouter to fail without SUMI_COMMAND_LOG_DIR")
	}
	if !strings.Contains(err.Error(), "SUMI_COMMAND_LOG_DIR") {
		t.Fatalf("expected error to mention SUMI_COMMAND_LOG_DIR, got %v", err)
	}
}

func TestNewRouter_RequiresAgentRuntimeStateDir(t *testing.T) {
	t.Setenv("SUMI_COMMAND_LOG_DIR", t.TempDir())
	t.Setenv("SUMI_BROWSER_EVENT_DIR", "")
	_, err := newRouter()
	if err == nil || !strings.Contains(err.Error(), "SUMI_BROWSER_EVENT_DIR") {
		t.Fatalf("expected explicit runtime-state directory error, got %v", err)
	}
}

func socketInode(t *testing.T, path string) (uint64, uint64) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("socket stat unavailable")
	}
	return stat.Dev, stat.Ino
}

func assertUnixSocketIsLive(t *testing.T, path string) {
	t.Helper()
	connection, err := net.DialTimeout("unix", path, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("socket %s is not live: %v", path, err)
	}
	_ = connection.Close()
}

func TestPublicListenAddressFromEnv(t *testing.T) {
	tests := []struct {
		name     string
		public   string
		loopback string
		want     string
		wantErr  bool
	}{
		{name: "default", want: ":8080"},
		{name: "legacy loopback", loopback: "127.0.0.1:4321", want: "127.0.0.1:4321"},
		{name: "literal IPv4", public: "100.116.25.99:8080", want: "100.116.25.99:8080"},
		{name: "literal IPv6 canonicalizes", public: "[2001:0db8:0:0:0:0:0:1]:8080", want: "[2001:db8::1]:8080"},
		{name: "loopback literal allowed", public: "127.0.0.1:4321", want: "127.0.0.1:4321"},
		{name: "wildcard IPv4", public: "0.0.0.0:4321", wantErr: true},
		{name: "wildcard IPv6", public: "[::]:4321", wantErr: true},
		{name: "hostname", public: "localhost:4321", wantErr: true},
		{name: "multicast IPv4", public: "224.0.0.1:4321", wantErr: true},
		{name: "multicast IPv6", public: "[ff02::1]:4321", wantErr: true},
		{name: "missing port", public: "100.116.25.99", wantErr: true},
		{name: "zero port", public: "100.116.25.99:0", wantErr: true},
		{name: "non-numeric port", public: "100.116.25.99:not-a-port", wantErr: true},
		{name: "signed port", public: "100.116.25.99:+8080", wantErr: true},
		{name: "both listener environments", public: "100.116.25.99:8080", loopback: "127.0.0.1:4321", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("SUMI_PUBLIC_LISTEN", tt.public)
			t.Setenv("SUMI_PUBLIC_LOOPBACK_LISTEN", tt.loopback)
			got, err := publicListenAddressFromEnv("8080")
			if tt.wantErr {
				if err == nil {
					t.Fatalf("public listener address %q / %q was accepted", tt.public, tt.loopback)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("address=%q err=%v, want address=%q", got, err, tt.want)
			}
		})
	}
}

func TestPublicLiteralListenAddressBindsLoopback(t *testing.T) {
	reserve, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve local loopback port: %v", err)
	}
	address := reserve.Addr().String()
	if err := reserve.Close(); err != nil {
		t.Fatalf("release local loopback port: %v", err)
	}

	t.Setenv("SUMI_PUBLIC_LISTEN", address)
	t.Setenv("SUMI_PUBLIC_LOOPBACK_LISTEN", "")
	configured, err := publicListenAddressFromEnv("8080")
	if err != nil {
		t.Fatalf("read public literal listener: %v", err)
	}
	listener, err := net.Listen("tcp", configured)
	if err != nil {
		t.Fatalf("bind configured public literal listener %q: %v", configured, err)
	}
	defer listener.Close()
}

func TestConfigureMessagingAttachmentsRequiresWholeStoreCaps(t *testing.T) {
	all := map[string]string{
		messagingAttachmentRootEnv:             filepath.Join(t.TempDir(), "attachments"),
		messagingAttachmentWorkspaceBytesEnv:   "20971520",
		messagingAttachmentWorkspaceObjectsEnv: "10",
		messagingAttachmentTotalBytesEnv:       "41943040",
		messagingAttachmentTotalObjectsEnv:     "20",
	}
	for name := range all {
		t.Setenv(name, "")
	}
	store := messaging.New(nil, nil, nil)
	if err := configureMessagingAttachmentsFromEnv(store); err != nil {
		t.Fatalf("all absent must leave attachments disabled: %v", err)
	}
	if store.AttachmentsEnabled() {
		t.Fatal("all-absent attachment configuration enabled storage")
	}
	for name, value := range all {
		t.Setenv(name, value)
	}
	if err := configureMessagingAttachmentsFromEnv(messaging.New(nil, nil, nil)); err != nil {
		t.Fatalf("complete attachment cap configuration: %v", err)
	}
	for missing := range all {
		for name, value := range all {
			t.Setenv(name, value)
		}
		t.Setenv(missing, "")
		if err := configureMessagingAttachmentsFromEnv(messaging.New(nil, nil, nil)); err == nil {
			t.Fatalf("configuration missing %s was accepted", missing)
		}
	}
}
