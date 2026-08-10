package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	firebaseauth "firebase.google.com/go/v4/auth"
	"github.com/gorilla/websocket"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/handler"
	"github.com/sumi-studio/sumi/apps/api/internal/messaging"
)

var testTokenSecret = []byte("test-secret-32bytes-long-string!!")
var testSessionSecret = []byte("browser-session-secret-32-bytes!!")

const testLocalControlPAID = "0198f0f4-9b72-7000-8000-000000000001"
const testBrowserOrigin = "https://web.example"

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
	gateway *agentevents.DurableGateway
}

func (s *readyingDirectChatSpawner) EnsureRunning(_ context.Context, personalityAgentID string) error {
	receipt := "ready-" + personalityAgentID
	return s.gateway.PublishRuntimeState(personalityAgentID, 1, &receipt)
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
	t.Setenv("SUMI_AGENT_TOKEN_SECRET", base64.StdEncoding.EncodeToString(testTokenSecret))
	t.Setenv("SUMI_AGENT_RUNTIME_STATE_DIR", t.TempDir())
}

func setSessionSecret(t *testing.T) {
	t.Helper()
	t.Setenv("SUMI_BROWSER_SESSION_SECRET", base64.StdEncoding.EncodeToString(testSessionSecret))
	t.Setenv("SUMI_BROWSER_SESSION_AUDIENCE", agentevents.DefaultBrowserAudience())
	t.Setenv("SUMI_BROWSER_WS_ALLOWED_ORIGINS", testBrowserOrigin)
	t.Setenv("SUMI_AGENT_RUNTIME_STATE_DIR", t.TempDir())
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
	gateway, err := agentevents.OpenDurableGateway(t.TempDir(), store)
	if err != nil {
		t.Fatal(err)
	}
	return gateway
}

func setReadyRouterState(t *testing.T, personalityAgentID string) {
	t.Helper()
	commandDir := t.TempDir()
	runtimeDir := t.TempDir()
	t.Setenv("SUMI_COMMAND_LOG_DIR", commandDir)
	t.Setenv("SUMI_AGENT_RUNTIME_STATE_DIR", runtimeDir)
	store, err := agentevents.OpenCommandStore(commandDir)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := agentevents.OpenDurableGateway(runtimeDir, store)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	receipt := "router-test-ready"
	if err := gateway.PublishRuntimeState(personalityAgentID, 7, &receipt); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
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
	req, err := http.NewRequest(http.MethodPost, serverURL+"/direct-chat/commands", bytes.NewReader(body))
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
	req, err := http.NewRequest(http.MethodPost, serverURL+"/direct-chat/commands", bytes.NewReader(body))
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

func TestTokenVerifierFromEnvMalformed(t *testing.T) {
	t.Setenv("SUMI_AGENT_TOKEN_SECRET", "not-valid-base64!!!")
	_, err := tokenVerifierFromEnv()
	if err == nil {
		t.Fatal("expected malformed secret to be rejected")
	}
}

func TestAllowedOriginsFromEnv(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		t.Setenv("SUMI_AGENT_WS_ALLOWED_ORIGINS", "")
		if got := allowedOriginsFromEnv(); got != nil {
			t.Fatalf("expected nil, got %v", got)
		}
	})
	t.Run("single", func(t *testing.T) {
		t.Setenv("SUMI_AGENT_WS_ALLOWED_ORIGINS", "https://app.example.com")
		got := allowedOriginsFromEnv()
		if len(got) != 1 || got[0] != "https://app.example.com" {
			t.Fatalf("unexpected origins: %v", got)
		}
	})
	t.Run("comma-separated-trimmed", func(t *testing.T) {
		t.Setenv("SUMI_AGENT_WS_ALLOWED_ORIGINS", " https://a.example , https://b.example ")
		got := allowedOriginsFromEnv()
		want := []string{"https://a.example", "https://b.example"}
		if len(got) != len(want) || strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
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
		{name: "auth dependency only", env: "SUMI_AUTH_FIREBASE_UID", value: "firebase-user"},
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

func TestBrowserAuthDisabledWithoutExplicitFirebaseUID(t *testing.T) {
	t.Setenv("SUMI_AUTH_FIREBASE_UID", "")
	server, enabled, err := browserAuthServerFromEnv(
		context.Background(),
		nil,
		[]string{testBrowserOrigin},
	)
	if err != nil {
		t.Fatalf("disabled auth: %v", err)
	}
	if enabled || server != nil {
		t.Fatal("auth routes must remain disabled without explicit Firebase UID binding")
	}
}

func TestApplicationCloseOwnsAndDrainsHijackedBrowserSocketsBeforeStoreClose(t *testing.T) {
	store, err := agentevents.OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := agentevents.OpenDurableGateway(t.TempDir(), store)
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
	wsURL := strings.Replace(server.URL, "http", "ws", 1) + "/direct-chat/ws"
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

func TestBrowserAuthOptionalOnlyConfigurationFailsClosed(t *testing.T) {
	t.Setenv("SUMI_AUTH_FIREBASE_UID", "")
	t.Setenv("SUMI_AUTH_SESSION_TTL", "20m")
	if _, _, err := browserAuthServerFromEnv(
		context.Background(),
		nil,
		[]string{testBrowserOrigin},
	); err == nil || !strings.Contains(err.Error(), "SUMI_AUTH_FIREBASE_UID") {
		t.Fatalf("partial SUMI_AUTH_* configuration did not fail startup: %v", err)
	}
}

func TestBrowserAuthPartialConfigurationFailsClosed(t *testing.T) {
	t.Setenv("SUMI_AUTH_FIREBASE_UID", "firebase-user")
	t.Setenv("SUMI_AUTH_TENANT_ID", "")
	t.Setenv("SUMI_AUTH_USER_ID", "")
	t.Setenv("SUMI_AUTH_PERSONALITY_AGENT_ID", "")
	sessions, err := agentevents.NewHMACUserSessionVerifier(
		testSessionSecret,
		"",
		testBrowserSessionRevocationStore(t),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := browserAuthServerFromEnv(
		context.Background(),
		sessions,
		[]string{testBrowserOrigin},
	); err == nil {
		t.Fatal("partial Firebase binding must fail startup")
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
	t.Setenv("SUMI_AGENT_RUNTIME_STATE_DIR", "")
	_, err := newRouter()
	if err == nil || !strings.Contains(err.Error(), "SUMI_AGENT_RUNTIME_STATE_DIR") {
		t.Fatalf("expected explicit runtime-state directory error, got %v", err)
	}
}

func TestHealthAndReadinessAreSeparateContracts(t *testing.T) {
	t.Setenv("SUMI_COMMAND_LOG_DIR", t.TempDir())
	t.Setenv("SUMI_AGENT_RUNTIME_STATE_DIR", t.TempDir())
	mux, err := newRouter()
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		path       string
		wantStatus int
		wantBody   string
	}{
		{path: "/health", wantStatus: http.StatusOK, wantBody: `"status":"alive"`},
		{path: "/ready", wantStatus: http.StatusOK, wantBody: `"status":"ready"`},
	} {
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, test.path, nil))
		if recorder.Code != test.wantStatus || !strings.Contains(recorder.Body.String(), test.wantBody) {
			t.Fatalf("%s response=%d body=%s", test.path, recorder.Code, recorder.Body.String())
		}
		if recorder.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s omitted no-store", test.path)
		}
	}
}

func TestPublicAPIResponsesAreNeverCacheable(t *testing.T) {
	handler := noStoreAPIResponses(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Add("Set-Cookie", "first=1; Secure")
		response.Header().Add("Set-Cookie", "second=2; Secure")
		response.WriteHeader(http.StatusUnauthorized)
	}))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/auth/session", nil))
	if recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("API response is cacheable: %v", recorder.Header())
	}
	if cookies := recorder.Header().Values("Set-Cookie"); len(cookies) != 2 {
		t.Fatalf("no-store middleware collapsed Set-Cookie: %v", cookies)
	}
}

func TestReadinessDetectsLostWritableDirectory(t *testing.T) {
	commandRoot := t.TempDir()
	runtimeRoot := t.TempDir()
	t.Setenv("SUMI_COMMAND_LOG_DIR", commandRoot)
	t.Setenv("SUMI_AGENT_RUNTIME_STATE_DIR", runtimeRoot)
	app, err := newApplicationFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	if err := os.RemoveAll(runtimeRoot); err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	app.publicMux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), `"runtime_state":"failed"`) {
		t.Fatalf("lost runtime root was not reported: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestReadinessChecksConfiguredProvisionerSocket(t *testing.T) {
	socketRoot := t.TempDir()
	socketPath := filepath.Join(socketRoot, "provisioner.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	t.Setenv("SUMI_RUNTIME_PROVISIONER_SOCKET", socketPath)
	checks := readinessChecks(nil)
	var provisioner handler.ReadinessCheck
	for _, check := range checks {
		if check.Name == "runtime_provisioner" {
			provisioner = check
			break
		}
	}
	if provisioner.Check == nil {
		t.Fatal("configured provisioner check is absent")
	}
	if err := provisioner.Check(context.Background()); err != nil {
		t.Fatalf("live provisioner socket rejected: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if err := provisioner.Check(context.Background()); err == nil {
		t.Fatal("closed provisioner socket reported ready")
	}
}

func TestNewRouter_RegistersCommandRouteWithBrowserSession(t *testing.T) {
	setSessionSecret(t)
	setReadyRouterState(t, "018f47a2-9b3c-7def-8abc-0123456789ab")
	mux, err := newRouter()
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(mux)
	defer server.Close()

	body := []byte(`{"type":"user_message","text":"hello","attachments":[]}`)
	resp := postWithSessionCookie(t, server.URL, "018f47a2-9b3c-7def-8abc-0123456789ab", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	var env testCommandReceipt
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatal(err)
	}
	if env.Seq != 1 {
		t.Fatalf("expected seq 1, got %d", env.Seq)
	}
	if env.CommandID == "" {
		t.Fatal("expected command_id")
	}
}

func TestNewRouter_CommandRouteRejectsUnavailableWithoutDurableAppend(t *testing.T) {
	setSessionSecret(t)
	commandDir := t.TempDir()
	t.Setenv("SUMI_COMMAND_LOG_DIR", commandDir)
	mux, err := newRouter()
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(mux)
	defer server.Close()

	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	body := []byte(`{"type":"user_message","text":"not ready","attachments":[]}`)
	resp := postWithSessionCookie(t, server.URL, personalityAgentID, body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}
	var rejection struct {
		Error        string `json:"error"`
		RejectReason string `json:"reject_reason"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rejection); err != nil {
		t.Fatal(err)
	}
	if rejection.Error != "unavailable" || rejection.RejectReason != "unavailable" {
		t.Fatalf("unexpected unavailable rejection: %+v", rejection)
	}

	observer, err := agentevents.OpenCommandStore(commandDir)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	if hasCommands, err := observer.HasCommands(context.Background(), personalityAgentID); err != nil || hasCommands {
		t.Fatalf("unavailable HTTP command reached durable log: hasCommands=%v err=%v", hasCommands, err)
	}
}

func TestDirectCommandLazySpawnIsolatesThreePAIDLogsAndRejectsTargetInjection(t *testing.T) {
	setSessionSecret(t)
	commandDir := t.TempDir()
	t.Setenv("SUMI_COMMAND_LOG_DIR", commandDir)
	app, err := newApplicationFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	app.browser.SetSpawner(&readyingDirectChatSpawner{gateway: app.browser.Events})
	server := httptest.NewServer(app.publicMux)
	defer server.Close()

	paids := []string{
		"0198f0f4-9b72-7000-8000-000000000001",
		"0198f0f4-9b72-7000-8000-000000000002",
		"0198f0f4-9b72-7000-8000-000000000003",
	}
	for index, paid := range paids {
		body := []byte(fmt.Sprintf(`{"type":"user_message","text":"own-%d","attachments":[]}`, index))
		response := postWithSessionCookieAndKey(t, server.URL, paid, fmt.Sprintf("own-%d", index), body)
		if response.StatusCode != http.StatusCreated {
			var rejection map[string]any
			_ = json.NewDecoder(response.Body).Decode(&rejection)
			response.Body.Close()
			t.Fatalf("own command for %s status=%d rejection=%v", paid, response.StatusCode, rejection)
		}
		response.Body.Close()
	}

	injected := []byte(fmt.Sprintf(
		`{"type":"user_message","text":"cross-target","attachments":[],"personality_agent_id":%q}`,
		paids[1],
	))
	response := postWithSessionCookieAndKey(t, server.URL, paids[0], "cross-injection", injected)
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("cross-PAID injection status=%d, want 400", response.StatusCode)
	}
	var rejection struct {
		RejectReason string `json:"reject_reason"`
	}
	if err := json.NewDecoder(response.Body).Decode(&rejection); err != nil {
		t.Fatal(err)
	}
	if rejection.RejectReason != "schema_violation" {
		t.Fatalf("cross-PAID injection rejection=%q", rejection.RejectReason)
	}

	observer, err := agentevents.OpenCommandStore(commandDir)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	for _, paid := range paids {
		next, err := observer.NextCommandSeq(context.Background(), paid)
		if err != nil {
			t.Fatal(err)
		}
		if next != 2 {
			t.Fatalf("PAID %s next sequence=%d, want exactly one isolated command", paid, next)
		}
	}
}

func TestNewRouter_CommandRouteRejectsAgentToken(t *testing.T) {
	setTokenSecret(t)
	setSessionSecret(t)
	t.Setenv("SUMI_COMMAND_LOG_DIR", t.TempDir())
	mux, err := newRouter()
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(mux)
	defer server.Close()

	body := []byte(`{"type":"user_message","text":"hello","attachments":[]}`)
	resp := postAuthorized(t, server.URL, "018f47a2-9b3c-7def-8abc-0123456789ab", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for agent bearer token, got %d", resp.StatusCode)
	}
}

func TestNewRouter_CommandRouteRejectsOversized(t *testing.T) {
	setSessionSecret(t)
	t.Setenv("SUMI_COMMAND_LOG_DIR", t.TempDir())
	mux, err := newRouter()
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(mux)
	defer server.Close()

	text := strings.Repeat("x", 1024*1024+1)
	body := []byte(`{"type":"user_message","text":"` + text + `","attachments":[]}`)
	resp := postWithSessionCookie(t, server.URL, "018f47a2-9b3c-7def-8abc-0123456789ab", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestNewRouter_CommandRouteIdempotency(t *testing.T) {
	setSessionSecret(t)
	setReadyRouterState(t, "018f47a2-9b3c-7def-8abc-0123456789ab")
	mux, err := newRouter()
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(mux)
	defer server.Close()

	body := []byte(`{"type":"user_message","text":"idem","attachments":[]}`)

	req1, err := http.NewRequest(http.MethodPost, server.URL+"/direct-chat/commands", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set("Origin", testBrowserOrigin)
	req1.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: signTestSession(t, testSessionSecret, testSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                agentevents.DefaultBrowserAudience(),
	})})
	req1.Header.Set("Idempotency-Key", "idem-key-1")

	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	if resp1.StatusCode != http.StatusCreated {
		t.Fatalf("expected first response 201, got %d", resp1.StatusCode)
	}
	var env1 testCommandReceipt
	if err := json.NewDecoder(resp1.Body).Decode(&env1); err != nil {
		t.Fatal(err)
	}
	resp1.Body.Close()
	if env1.Seq == 0 || env1.CommandID == "" {
		t.Fatalf("expected non-empty first command envelope, got %+v", env1)
	}

	req2, err := http.NewRequest(http.MethodPost, server.URL+"/direct-chat/commands", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Origin", testBrowserOrigin)
	req2.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: signTestSession(t, testSessionSecret, testSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                agentevents.DefaultBrowserAudience(),
	})})
	req2.Header.Set("Idempotency-Key", "idem-key-1")

	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	if resp2.StatusCode != http.StatusCreated {
		t.Fatalf("expected second response 201, got %d", resp2.StatusCode)
	}
	var env2 testCommandReceipt
	if err := json.NewDecoder(resp2.Body).Decode(&env2); err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()

	if env1.Seq != env2.Seq || env1.CommandID != env2.CommandID {
		t.Fatalf("idempotency key did not return the same command: %+v vs %+v", env1, env2)
	}
}

func TestNewRouter_LocalControlRoutesAreAbsentFromPublicMux(t *testing.T) {
	t.Setenv("SUMI_LOCAL_CONTROL_ENABLED", "0")
	t.Setenv("SUMI_COMMAND_LOG_DIR", t.TempDir())
	t.Setenv("SUMI_AGENT_RUNTIME_STATE_DIR", t.TempDir())
	mux, err := newRouter()
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(
		http.MethodPost,
		agentevents.LocalRuntimeStatePublishPath,
		strings.NewReader(`{}`),
	)
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("disabled local control route: got %d, want 404", recorder.Code)
	}

	t.Setenv("SUMI_LOCAL_CONTROL_ENABLED", "1")
	t.Setenv("SUMI_LOCAL_CONTROL_BEARER", "server-fixture-control-bearer-32-bytes-minimum")
	t.Setenv("SUMI_LOCAL_CONTROL_TENANT_ID", "tenant-fixture")
	t.Setenv("SUMI_LOCAL_CONTROL_PERSONALITY_AGENT_ID", testLocalControlPAID)
	t.Setenv("SUMI_LOCAL_CONTROL_GENERATION", "7")
	t.Setenv("SUMI_LOCAL_CONTROL_RPC_BOOT_NONCE", "boot-fixture")
	t.Setenv("SUMI_LOCAL_CONTROL_AUDIENCE", "sumi:agent:events")
	t.Setenv("SUMI_LOCAL_CONTROL_DELIVERY_AUTHORIZATION", "raw")
	t.Setenv("SUMI_LOCAL_CONTROL_LOOPBACK_LISTEN", "127.0.0.1:0")
	t.Setenv("SUMI_AGENT_TOKEN_SECRET", base64.StdEncoding.EncodeToString(testTokenSecret))
	commandDir := t.TempDir()
	runtimeDir := t.TempDir()
	t.Setenv("SUMI_COMMAND_LOG_DIR", commandDir)
	t.Setenv("SUMI_AGENT_RUNTIME_STATE_DIR", runtimeDir)
	app, err := newApplicationFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	publicRequest := httptest.NewRequest(
		http.MethodPost,
		agentevents.LocalRuntimeStatePublishPath,
		strings.NewReader(`{}`),
	)
	publicRecorder := httptest.NewRecorder()
	app.publicMux.ServeHTTP(publicRecorder, publicRequest)
	if publicRecorder.Code != http.StatusNotFound {
		t.Fatalf("enabled local control route leaked onto public mux: got %d, want 404", publicRecorder.Code)
	}

	server := httptest.NewServer(app.localMux)
	defer server.Close()

	publication := []byte(`{
		"publication_id":"startup-fixture",
		"personality_agent_id":"0198f0f4-9b72-7000-8000-000000000001",
		"generation":7,
		"rpc_boot_nonce":"boot-fixture",
		"expected_revision":null,
		"state":"not_ready",
		"hydration_receipt_identity":null,
		"reason":"startup"
	}`)
	req, err := http.NewRequest(
		http.MethodPost,
		server.URL+agentevents.LocalRuntimeStatePublishPath,
		bytes.NewReader(publication),
	)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer server-fixture-control-bearer-32-bytes-minimum")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("explicitly enabled local control route: got %d, want 200", resp.StatusCode)
	}

	// A local API process restart can be provisioned with the next exact
	// runtime epoch while reusing the same durable PAID-keyed registry.
	server.Close()
	if err := app.Close(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SUMI_LOCAL_CONTROL_BEARER", "server-fixture-next-control-bearer-32-bytes-minimum")
	t.Setenv("SUMI_LOCAL_CONTROL_GENERATION", "8")
	t.Setenv("SUMI_LOCAL_CONTROL_RPC_BOOT_NONCE", "boot-fixture-next")
	app, err = newApplicationFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	restartedServer := httptest.NewServer(app.localMux)
	defer restartedServer.Close()
	rollover := []byte(`{
		"publication_id":"startup-fixture-next",
		"personality_agent_id":"0198f0f4-9b72-7000-8000-000000000001",
		"generation":8,
		"rpc_boot_nonce":"boot-fixture-next",
		"expected_revision":null,
		"state":"not_ready",
		"hydration_receipt_identity":null,
		"reason":"startup"
	}`)
	req, err = http.NewRequest(
		http.MethodPost,
		restartedServer.URL+agentevents.LocalRuntimeStatePublishPath,
		bytes.NewReader(rollover),
	)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer server-fixture-next-control-bearer-32-bytes-minimum")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("higher generation after API restart: got %d, want 200", resp.StatusCode)
	}
	var ack struct {
		Generation uint64 `json:"generation"`
		Revision   uint64 `json:"revision"`
		State      string `json:"state"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&ack); err != nil {
		t.Fatal(err)
	}
	if ack.Generation != 8 || ack.Revision != 2 || ack.State != "not_ready" {
		t.Fatalf("restart rollover ack mismatch: %+v", ack)
	}
}

func setCompleteLocalControlEnv(t *testing.T) {
	t.Helper()
	t.Setenv("SUMI_LOCAL_CONTROL_ENABLED", "1")
	t.Setenv("SUMI_LOCAL_CONTROL_BEARER", "server-fixture-control-bearer-32-bytes-minimum")
	t.Setenv("SUMI_LOCAL_CONTROL_TENANT_ID", "tenant-fixture")
	t.Setenv("SUMI_LOCAL_CONTROL_PERSONALITY_AGENT_ID", "0198f0f4-9b72-7000-8000-000000000001")
	t.Setenv("SUMI_LOCAL_CONTROL_GENERATION", "7")
	t.Setenv("SUMI_LOCAL_CONTROL_RPC_BOOT_NONCE", "boot-fixture")
	t.Setenv("SUMI_LOCAL_CONTROL_AUDIENCE", "sumi:agent:events")
	t.Setenv("SUMI_LOCAL_CONTROL_DELIVERY_AUTHORIZATION", "raw")
	t.Setenv("SUMI_AGENT_TOKEN_SECRET", base64.StdEncoding.EncodeToString(testTokenSecret))
	t.Setenv("SUMI_COMMAND_LOG_DIR", t.TempDir())
	t.Setenv("SUMI_AGENT_RUNTIME_STATE_DIR", t.TempDir())
}

func TestRegisterMessagingCallRoutesSeparatesPublicStateFromLocalControlCapability(t *testing.T) {
	localMux := func(t *testing.T, server *messaging.Server) *http.ServeMux {
		t.Helper()
		store, err := agentevents.OpenCommandStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		gateway, err := agentevents.OpenDurableGateway(t.TempDir(), store)
		if err != nil {
			t.Fatal(err)
		}
		control, err := agentevents.NewLocalControlServer(gateway, testTokenSecret, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := server.RegisterLocalControlRoutes(control); err != nil {
			t.Fatal(err)
		}
		mux := http.NewServeMux()
		if err := control.RegisterRoutes(mux); err != nil {
			t.Fatal(err)
		}
		return mux
	}

	for _, test := range []struct {
		name              string
		livekit           messaging.LiveKitConfig
		wantLocalCallPath bool
	}{
		{name: "unconfigured"},
		{
			name: "configured",
			livekit: messaging.LiveKitConfig{
				URL:       "wss://livekit.example",
				APIKey:    "key",
				APISecret: "secret",
			},
			wantLocalCallPath: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := messaging.NewServer(nil, nil)
			publicMux := http.NewServeMux()
			configured := registerMessagingCallRoutes(publicMux, server, test.livekit)
			if configured != test.wantLocalCallPath {
				t.Fatalf("configured = %v, want %v", configured, test.wantLocalCallPath)
			}

			_, publicPattern := publicMux.Handler(
				httptest.NewRequest(http.MethodGet, "/messaging/calls", nil),
			)
			if publicPattern != "GET /messaging/calls" {
				t.Fatalf("public call-state route pattern = %q, want mounted GET route", publicPattern)
			}

			_, localPattern := localMux(t, server).Handler(
				httptest.NewRequest(http.MethodPost, messaging.LocalCallStatePath, nil),
			)
			if test.wantLocalCallPath && localPattern != "POST "+messaging.LocalCallStatePath {
				t.Fatalf("configured local-control call-state route pattern = %q, want mounted POST route", localPattern)
			}
			if !test.wantLocalCallPath && localPattern != "" {
				t.Fatalf("unconfigured local-control call-state route pattern = %q, want absent", localPattern)
			}
		})
	}
}

func trustedSocketParent(t *testing.T) string {
	t.Helper()
	parent := t.TempDir()
	if err := os.Chmod(parent, localControlParentMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(parent, os.Geteuid(), os.Getegid()); err != nil {
		t.Fatal(err)
	}
	return parent
}

func trustedShortSocketParent(t *testing.T) string {
	t.Helper()
	parent, err := os.MkdirTemp("", "su-p-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(parent) })
	if err := os.Chmod(parent, localControlParentMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(parent, os.Geteuid(), os.Getegid()); err != nil {
		t.Fatal(err)
	}
	return parent
}

func replaceTrustedSocketParent(t *testing.T, parent string, gid int) string {
	t.Helper()
	movedParent := parent + "-pinned"
	if err := os.Rename(parent, movedParent); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(movedParent) })
	if err := os.Mkdir(parent, localControlParentMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(parent, os.Geteuid(), gid); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, localControlParentMode); err != nil {
		t.Fatal(err)
	}
	return movedParent
}

func TestUnixLocalControlRoundTripRequiresBearerAndNeverUsesPublicMux(t *testing.T) {
	setCompleteLocalControlEnv(t)
	parent := trustedSocketParent(t)
	socketPath := filepath.Join(parent, "control.sock")
	t.Setenv("SUMI_LOCAL_CONTROL_UNIX_SOCKET", socketPath)
	t.Setenv("SUMI_LOCAL_CONTROL_SOCKET_GID", strconv.Itoa(os.Getegid()))

	app, err := newApplicationFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	listener, err := app.localListener.listen()
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: app.localListener.handler(app.localMux)}
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()
	t.Cleanup(func() {
		_ = server.Shutdown(context.Background())
		<-serveDone
	})

	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", socketPath)
		},
		DisableKeepAlives: true,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   2 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	t.Cleanup(transport.CloseIdleConnections)

	publication := []byte(`{
		"publication_id":"uds-startup",
		"personality_agent_id":"0198f0f4-9b72-7000-8000-000000000001",
		"generation":7,
		"rpc_boot_nonce":"boot-fixture",
		"expected_revision":null,
		"state":"not_ready",
		"hydration_receipt_identity":null,
		"reason":"startup"
	}`)
	post := func(bearer string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(
			http.MethodPost,
			"http://local-control.invalid"+agentevents.LocalRuntimeStatePublishPath,
			bytes.NewReader(publication),
		)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return response
	}

	unauthorized := post("wrong-bearer")
	unauthorized.Body.Close()
	if unauthorized.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong bearer: got %d, want 401", unauthorized.StatusCode)
	}
	authorized := post("server-fixture-control-bearer-32-bytes-minimum")
	authorized.Body.Close()
	if authorized.StatusCode != http.StatusOK {
		t.Fatalf("UDS publication: got %d, want 200", authorized.StatusCode)
	}

	publicRequest := httptest.NewRequest(
		http.MethodPost,
		agentevents.LocalRuntimeStatePublishPath,
		bytes.NewReader(publication),
	)
	publicRequest.Header.Set("Authorization", "Bearer server-fixture-control-bearer-32-bytes-minimum")
	publicRecorder := httptest.NewRecorder()
	app.publicMux.ServeHTTP(publicRecorder, publicRequest)
	if publicRecorder.Code != http.StatusNotFound {
		t.Fatalf("public local-control route: got %d, want 404", publicRecorder.Code)
	}

	socketInfo, err := os.Lstat(socketPath)
	if err != nil {
		t.Fatal(err)
	}
	if socketInfo.Mode().Perm() != localControlSocketMode {
		t.Fatalf("socket mode: got %04o, want %04o", socketInfo.Mode().Perm(), localControlSocketMode)
	}
}

func TestUnixLocalControlTrustChecksFailClosed(t *testing.T) {
	gid := os.Getegid()
	t.Run("wrong parent mode", func(t *testing.T) {
		parent := t.TempDir()
		socketPath := filepath.Join(parent, "control.sock")
		if _, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID); err == nil ||
			!strings.Contains(err.Error(), "parent mode") {
			t.Fatalf("wrong parent mode was accepted: %v", err)
		}
	})

	t.Run("symlink parent", func(t *testing.T) {
		parent, err := os.MkdirTemp("", "su-a-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(parent) })
		if err := os.Chmod(parent, localControlParentMode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chown(parent, os.Geteuid(), gid); err != nil {
			t.Fatal(err)
		}
		linkRoot, err := os.MkdirTemp("", "su-l-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(linkRoot) })
		link := filepath.Join(linkRoot, "p")
		if err := os.Symlink(parent, link); err != nil {
			t.Fatal(err)
		}
		if _, err := listenTrustedUnixSocket(filepath.Join(link, "control.sock"), gid, testLocalControlPAID); err == nil ||
			(!strings.Contains(err.Error(), "symlink") && !strings.Contains(err.Error(), "real directory")) {
			t.Fatalf("symlink parent was accepted: %v", err)
		}
	})

	t.Run("non-socket stale target", func(t *testing.T) {
		parent := trustedSocketParent(t)
		socketPath := filepath.Join(parent, "control.sock")
		if err := os.WriteFile(socketPath, []byte("not a socket"), localControlSocketMode); err != nil {
			t.Fatal(err)
		}
		if _, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID); err == nil ||
			!strings.Contains(err.Error(), "not a Unix socket") {
			t.Fatalf("non-socket target was accepted: %v", err)
		}
		if _, err := os.Lstat(socketPath); err != nil {
			t.Fatalf("untrusted target was removed: %v", err)
		}
	})

	t.Run("symlink ownership lock", func(t *testing.T) {
		parent := trustedSocketParent(t)
		socketPath := filepath.Join(parent, "control.sock")
		target := filepath.Join(parent, "unrelated")
		if err := os.WriteFile(target, []byte("do not touch"), localControlLockMode); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, socketPath+".owner.lock"); err != nil {
			t.Fatal(err)
		}
		if _, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID); err == nil ||
			!strings.Contains(err.Error(), "ownership lock") {
			t.Fatalf("symlink ownership lock was accepted: %v", err)
		}
		body, err := os.ReadFile(target)
		if err != nil {
			t.Fatal(err)
		}
		if string(body) != "do not touch" {
			t.Fatal("symlink target was modified")
		}
	})

	t.Run("wrong-mode ownership lock", func(t *testing.T) {
		parent := trustedSocketParent(t)
		socketPath := filepath.Join(parent, "control.sock")
		lockPath := socketPath + ".owner.lock"
		if err := os.WriteFile(lockPath, nil, 0o640); err != nil {
			t.Fatal(err)
		}
		if err := os.Chown(lockPath, os.Geteuid(), gid); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(lockPath, 0o640); err != nil {
			t.Fatal(err)
		}
		if _, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID); err == nil ||
			!strings.Contains(err.Error(), "neither complete") {
			t.Fatalf("wrong-mode ownership lock was accepted: %v", err)
		}
	})

	t.Run("hardlinked ownership lock", func(t *testing.T) {
		parent := trustedSocketParent(t)
		socketPath := filepath.Join(parent, "control.sock")
		lockPath := socketPath + ".owner.lock"
		if err := os.WriteFile(lockPath, nil, localControlLockMode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chown(lockPath, os.Geteuid(), gid); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(lockPath, filepath.Join(parent, "second-lock-link")); err != nil {
			t.Fatal(err)
		}
		if _, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID); err == nil ||
			!strings.Contains(err.Error(), "neither complete") {
			t.Fatalf("hardlinked ownership lock was accepted: %v", err)
		}
	})

	t.Run("arbitrary initializer residue", func(t *testing.T) {
		parent := trustedShortSocketParent(t)
		socketPath := filepath.Join(parent, "control.sock")
		residue := socketPath + ".owner.lock.init-arbitrary"
		if err := os.WriteFile(residue, []byte("unrelated-content"), localControlLockMode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chown(residue, os.Geteuid(), gid); err != nil {
			t.Fatal(err)
		}
		if listener, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID); err == nil {
			_ = listener.Close()
			t.Fatal("arbitrary initializer residue was accepted")
		} else if !strings.Contains(err.Error(), "untrusted") {
			t.Fatalf("unexpected arbitrary initializer residue error: %v", err)
		}
		body, err := os.ReadFile(residue)
		if err != nil {
			t.Fatalf("arbitrary initializer residue was removed: %v", err)
		}
		if string(body) != "unrelated-content" {
			t.Fatal("arbitrary initializer residue was modified")
		}
	})

	t.Run("wrong-mode stale socket", func(t *testing.T) {
		parent := trustedSocketParent(t)
		socketPath := filepath.Join(parent, "control.sock")
		stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
		if err != nil {
			t.Fatal(err)
		}
		stale.SetUnlinkOnClose(false)
		if err := os.Chown(socketPath, os.Geteuid(), gid); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(socketPath, 0o640); err != nil {
			t.Fatal(err)
		}
		if err := stale.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID); err == nil ||
			!strings.Contains(err.Error(), "socket mode") {
			t.Fatalf("wrong-mode stale socket was accepted: %v", err)
		}
	})

	t.Run("live socket", func(t *testing.T) {
		parent := trustedSocketParent(t)
		socketPath := filepath.Join(parent, "control.sock")
		live, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
		if err != nil {
			t.Fatal(err)
		}
		defer live.Close()
		if err := os.Chown(socketPath, os.Geteuid(), gid); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(socketPath, localControlSocketMode); err != nil {
			t.Fatal(err)
		}
		if _, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID); err == nil ||
			!strings.Contains(err.Error(), "live local control socket") {
			t.Fatalf("live socket was replaced: %v", err)
		}
	})

	t.Run("hardlinked socket", func(t *testing.T) {
		parent := trustedSocketParent(t)
		socketPath := filepath.Join(parent, "control.sock")
		stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
		if err != nil {
			t.Fatal(err)
		}
		stale.SetUnlinkOnClose(false)
		if err := os.Chown(socketPath, os.Geteuid(), gid); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(socketPath, localControlSocketMode); err != nil {
			t.Fatal(err)
		}
		if err := stale.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(socketPath, filepath.Join(parent, "second-link.sock")); err != nil {
			t.Fatal(err)
		}
		if _, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID); err == nil ||
			!strings.Contains(err.Error(), "link count") {
			t.Fatalf("hardlinked socket was accepted: %v", err)
		}
	})

	t.Run("trusted stale socket", func(t *testing.T) {
		parent := trustedSocketParent(t)
		socketPath := filepath.Join(parent, "control.sock")
		stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
		if err != nil {
			t.Fatal(err)
		}
		stale.SetUnlinkOnClose(false)
		if err := os.Chown(socketPath, os.Geteuid(), gid); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(socketPath, localControlSocketMode); err != nil {
			t.Fatal(err)
		}
		if err := stale.Close(); err != nil {
			t.Fatal(err)
		}
		replacement, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID)
		if err != nil {
			t.Fatalf("trusted stale socket was not recovered: %v", err)
		}
		if err := replacement.Close(); err != nil {
			t.Fatal(err)
		}
	})
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

func bindTrustedTestSocket(t *testing.T, path string, gid int) *net.UnixListener {
	t.Helper()
	listener, err := net.ListenUnix(
		"unix",
		&net.UnixAddr{Name: path, Net: "unix"},
	)
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(false)
	if err := os.Chown(path, os.Geteuid(), gid); err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	if err := os.Chmod(path, localControlSocketMode); err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	return listener
}

func assertUnixSocketIsLive(t *testing.T, path string) {
	t.Helper()
	connection, err := net.DialTimeout("unix", path, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("socket %s is not live: %v", path, err)
	}
	_ = connection.Close()
}

func TestUnixListenerOwnershipLockPreventsReplicaEvictionAndLateUnlink(t *testing.T) {
	parent := trustedSocketParent(t)
	socketPath := filepath.Join(parent, "control.sock")
	gid := os.Getegid()

	first, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID)
	if err != nil {
		t.Fatal(err)
	}
	firstDev, firstIno := socketInode(t, socketPath)

	secondAttempt, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID)
	if err == nil {
		_ = secondAttempt.Close()
		t.Fatal("second replica acquired the listener ownership lock")
	}
	if !strings.Contains(err.Error(), "ownership lock is already held") {
		t.Fatalf("unexpected second replica error: %v", err)
	}
	gotDev, gotIno := socketInode(t, socketPath)
	if gotDev != firstDev || gotIno != firstIno {
		t.Fatal("second replica replaced the first listener socket")
	}

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("first listener did not remove its owned socket: %v", err)
	}

	survivor, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID)
	if err != nil {
		t.Fatal(err)
	}
	survivorDev, survivorIno := socketInode(t, socketPath)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	gotDev, gotIno = socketInode(t, socketPath)
	if gotDev != survivorDev || gotIno != survivorIno {
		t.Fatal("late close from the first replica unlinked its successor")
	}
	if err := survivor.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := listenTrustedUnixSocket(socketPath, gid, "0198f0f4-9b72-7000-8000-000000000099"); err == nil ||
		!strings.Contains(err.Error(), "neither complete") {
		t.Fatalf("persistent listener lock accepted a different PAID: %v", err)
	}
}

func TestUnixListenerCloseNeverUnlinksAReplacementInode(t *testing.T) {
	parent := trustedSocketParent(t)
	socketPath := filepath.Join(parent, "control.sock")
	gid := os.Getegid()
	owned, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID)
	if err != nil {
		t.Fatal(err)
	}
	if err := owned.listener.Close(); err != nil {
		t.Fatal(err)
	}
	keptOldInode := filepath.Join(parent, "old-inode.sock")
	if err := os.Link(socketPath, keptOldInode); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(socketPath); err != nil {
		t.Fatal(err)
	}
	replacement, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if err := os.Chown(socketPath, os.Geteuid(), gid); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socketPath, localControlSocketMode); err != nil {
		t.Fatal(err)
	}
	replacementDev, replacementIno := socketInode(t, socketPath)

	err = owned.Close()
	if err == nil || !strings.Contains(err.Error(), "no longer owned") {
		t.Fatalf("close did not report replacement inode: %v", err)
	}
	gotDev, gotIno := socketInode(t, socketPath)
	if gotDev != replacementDev || gotIno != replacementIno {
		t.Fatal("listener close unlinked the replacement socket")
	}
}

func TestUnixListenerOwnershipSerializesConcurrentStaleRecovery(t *testing.T) {
	parent := trustedSocketParent(t)
	socketPath := filepath.Join(parent, "control.sock")
	gid := os.Getegid()
	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	stale.SetUnlinkOnClose(false)
	if err := os.Chown(socketPath, os.Geteuid(), gid); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socketPath, localControlSocketMode); err != nil {
		t.Fatal(err)
	}
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}

	type result struct {
		listener *ownedUnixListener
		err      error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for range 2 {
		go func() {
			<-start
			listener, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID)
			results <- result{listener: listener, err: err}
		}()
	}
	close(start)
	first := <-results
	second := <-results
	successes := 0
	var winner *ownedUnixListener
	for _, candidate := range []result{first, second} {
		if candidate.err == nil {
			successes++
			winner = candidate.listener
			continue
		}
		if !strings.Contains(candidate.err.Error(), "ownership lock is already held") &&
			!strings.Contains(candidate.err.Error(), "bootstrap lock is already held") {
			t.Fatalf("unexpected concurrent stale-recovery error: %v", candidate.err)
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent stale recovery produced %d successful listeners, want 1", successes)
	}
	socketInode(t, socketPath)
	if err := winner.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestUnixListenerOwnershipLockIsProcessIndependent(t *testing.T) {
	const childMarker = "SUMI_TEST_LOCAL_CONTROL_LOCK_CHILD"
	if os.Getenv(childMarker) == "1" {
		socketPath := os.Getenv("SUMI_TEST_LOCAL_CONTROL_LOCK_SOCKET")
		gid, err := strconv.Atoi(os.Getenv("SUMI_TEST_LOCAL_CONTROL_LOCK_GID"))
		if err != nil {
			t.Fatal(err)
		}
		listener, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID)
		if err == nil {
			_ = listener.Close()
			t.Fatal("child process acquired a lock held by the parent process")
		}
		if !strings.Contains(err.Error(), "ownership lock is already held") {
			t.Fatalf("child received unexpected ownership error: %v", err)
		}
		return
	}

	parent := trustedSocketParent(t)
	socketPath := filepath.Join(parent, "control.sock")
	gid := os.Getegid()
	listener, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	dev, ino := socketInode(t, socketPath)

	command := exec.Command(os.Args[0], "-test.run=^TestUnixListenerOwnershipLockIsProcessIndependent$")
	command.Env = append(
		os.Environ(),
		childMarker+"=1",
		"SUMI_TEST_LOCAL_CONTROL_LOCK_SOCKET="+socketPath,
		"SUMI_TEST_LOCAL_CONTROL_LOCK_GID="+strconv.Itoa(gid),
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("child lock probe failed: %v\n%s", err, output)
	}
	gotDev, gotIno := socketInode(t, socketPath)
	if gotDev != dev || gotIno != ino {
		t.Fatal("child process replaced the parent listener socket")
	}
}

func TestUnixListenerCrashResidueConverges(t *testing.T) {
	const childMarker = "SUMI_TEST_LOCAL_CONTROL_CRASH_CHILD"
	if os.Getenv(childMarker) == "1" {
		socketPath := os.Getenv("SUMI_TEST_LOCAL_CONTROL_CRASH_SOCKET")
		gid, err := strconv.Atoi(os.Getenv("SUMI_TEST_LOCAL_CONTROL_CRASH_GID"))
		if err != nil {
			t.Fatal(err)
		}
		if listener, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID); err != nil {
			t.Fatalf("crash child failed before failpoint: %v", err)
		} else {
			_ = listener.Close()
			t.Fatal("crash failpoint did not terminate child")
		}
		return
	}

	for _, failpoint := range []string{
		"lock-create-before-metadata",
		"lock-metadata-before-binding",
		"lock-binding-before-fsync",
		"socket-bind-before-metadata",
	} {
		t.Run(failpoint, func(t *testing.T) {
			parent := trustedSocketParent(t)
			socketPath := filepath.Join(parent, "control.sock")
			gid := os.Getegid()
			command := exec.Command(os.Args[0], "-test.run=^TestUnixListenerCrashResidueConverges$")
			command.Env = append(
				os.Environ(),
				childMarker+"=1",
				"SUMI_TEST_LOCAL_CONTROL_CRASH_SOCKET="+socketPath,
				"SUMI_TEST_LOCAL_CONTROL_CRASH_GID="+strconv.Itoa(gid),
				"SUMI_TEST_LOCAL_CONTROL_CRASH_FAILPOINT="+failpoint,
			)
			output, err := command.CombinedOutput()
			exitError, ok := err.(*exec.ExitError)
			if !ok {
				t.Fatalf("crash child was not killed: %v\n%s", err, output)
			}
			status, ok := exitError.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatalf("crash child exit was not SIGKILL: %v\n%s", err, output)
			}
			lockPath := socketPath + ".owner.lock"
			if strings.HasPrefix(failpoint, "lock-") {
				if _, err := os.Lstat(lockPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("partially initialized lock became visible: %v", err)
				}
			} else {
				content, err := os.ReadFile(lockPath)
				if err != nil {
					t.Fatalf("socket failpoint lost initialized ownership lock: %v", err)
				}
				if !bytes.Contains(content, []byte("personality_agent_id="+testLocalControlPAID)) {
					t.Fatal("published ownership lock lacks exact PAID binding")
				}
			}

			restarted, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID)
			if err != nil {
				t.Fatalf("restart did not recover %s residue: %v", failpoint, err)
			}
			dev, ino := socketInode(t, socketPath)
			competing, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID)
			if err == nil {
				_ = competing.Close()
				t.Fatal("competing replica acquired listener after crash recovery")
			}
			gotDev, gotIno := socketInode(t, socketPath)
			if gotDev != dev || gotIno != ino {
				t.Fatal("competing replica removed the recovered live socket")
			}
			if err := restarted.Close(); err != nil {
				t.Fatal(err)
			}

			entries, err := os.ReadDir(parent)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if strings.Contains(entry.Name(), ".owner.lock.init-") {
					t.Fatalf("initialization residue was not cleaned: %s", entry.Name())
				}
			}
		})
	}
}

func TestUnixListenerParentReplacementDuringLockPublicationFailsClosed(t *testing.T) {
	parent := trustedShortSocketParent(t)
	socketPath := filepath.Join(parent, "control.sock")
	gid := os.Getegid()
	var movedParent string
	hooks := &localControlListenerTestHooks{
		afterLockPublication: func() {
			movedParent = replaceTrustedSocketParent(t, parent, gid)
		},
	}

	listener, err := listenTrustedUnixSocketWithHooks(
		socketPath,
		gid,
		testLocalControlPAID,
		hooks,
	)
	if err == nil {
		_ = listener.Close()
		t.Fatal("listener returned after its configured parent was replaced")
	}
	if !strings.Contains(err.Error(), "no longer names the pinned trusted directory") {
		t.Fatalf("unexpected parent replacement error: %v", err)
	}
	if _, err := os.Lstat(socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement parent received a socket: %v", err)
	}
	if _, err := os.Lstat(socketPath + ".owner.lock"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement parent received an ownership lock: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(movedParent, "control.sock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pinned parent received a socket after publication race: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(movedParent, "control.sock.owner.lock")); err != nil {
		t.Fatalf("atomic ownership lock was not published in the pinned parent: %v", err)
	}
}

func TestUnixListenerParentReplacementBeforeReturnCleansOnlyPinnedSocket(t *testing.T) {
	parent := trustedShortSocketParent(t)
	socketPath := filepath.Join(parent, "control.sock")
	gid := os.Getegid()
	var movedParent string
	var replacement *net.UnixListener
	var replacementDev uint64
	var replacementIno uint64
	hooks := &localControlListenerTestHooks{
		beforeListenerReturn: func() {
			movedParent = replaceTrustedSocketParent(t, parent, gid)
			var err error
			replacement, err = net.ListenUnix(
				"unix",
				&net.UnixAddr{Name: socketPath, Net: "unix"},
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Chown(socketPath, os.Geteuid(), gid); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(socketPath, localControlSocketMode); err != nil {
				t.Fatal(err)
			}
			replacementDev, replacementIno = socketInode(t, socketPath)
		},
	}

	listener, err := listenTrustedUnixSocketWithHooks(
		socketPath,
		gid,
		testLocalControlPAID,
		hooks,
	)
	if listener != nil {
		_ = listener.Close()
		t.Fatal("listener returned after its configured parent was replaced")
	}
	if replacement == nil {
		t.Fatal("parent replacement hook did not install its live socket")
	}
	defer replacement.Close()
	if err == nil || !strings.Contains(err.Error(), "no longer names the pinned trusted directory") {
		t.Fatalf("unexpected parent replacement error: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(movedParent, "control.sock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("owned socket was not removed from the pinned parent: %v", err)
	}
	gotDev, gotIno := socketInode(t, socketPath)
	if gotDev != replacementDev || gotIno != replacementIno {
		t.Fatal("cleanup removed or replaced the non-owned socket in the configured replacement parent")
	}
}

func TestUnixListenerLockPublicationNeverReplacesNoncooperatingDestination(t *testing.T) {
	parent := trustedShortSocketParent(t)
	socketPath := filepath.Join(parent, "control.sock")
	lockPath := socketPath + ".owner.lock"
	gid := os.Getegid()
	const installed = "same-uid-noncooperating-destination"
	var installedDev uint64
	var installedIno uint64
	hooks := &localControlListenerTestHooks{
		beforeLockPublication: func() {
			if err := os.WriteFile(lockPath, []byte(installed), localControlLockMode); err != nil {
				t.Fatal(err)
			}
			if err := os.Chown(lockPath, os.Geteuid(), gid); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(lockPath, localControlLockMode); err != nil {
				t.Fatal(err)
			}
			info, err := os.Lstat(lockPath)
			if err != nil {
				t.Fatal(err)
			}
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !ok {
				t.Fatal("destination installer inode is unavailable")
			}
			installedDev, installedIno = stat.Dev, stat.Ino
		},
	}

	listener, err := listenTrustedUnixSocketWithHooks(
		socketPath,
		gid,
		testLocalControlPAID,
		hooks,
	)
	if err == nil {
		_ = listener.Close()
		t.Fatal("no-replace lock publication overwrote a competing destination")
	}
	if !strings.Contains(err.Error(), "appeared during atomic publication") {
		t.Fatalf("unexpected no-replace publication error: %v", err)
	}
	content, err := os.ReadFile(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != installed {
		t.Fatalf("competing destination content changed: %q", content)
	}
	info, err := os.Lstat(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Dev != installedDev || stat.Ino != installedIno {
		t.Fatal("competing destination inode was replaced")
	}
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".owner.lock.init-") {
			t.Fatalf("failed no-replace publication left initializer residue: %s", entry.Name())
		}
	}
	if _, err := os.Lstat(socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed no-replace publication created a socket: %v", err)
	}
}

func TestUnixListenerStaleQuarantineRestoresLiveReplacement(t *testing.T) {
	parent := trustedShortSocketParent(t)
	socketPath := filepath.Join(parent, "control.sock")
	gid := os.Getegid()
	stale := bindTrustedTestSocket(t, socketPath, gid)
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}

	var replacement *net.UnixListener
	var replacementDev uint64
	var replacementIno uint64
	hooks := &localControlListenerTestHooks{
		beforeSocketQuarantine: func() {
			if err := os.Remove(socketPath); err != nil {
				t.Fatal(err)
			}
			replacement = bindTrustedTestSocket(t, socketPath, gid)
			replacementDev, replacementIno = socketInode(t, socketPath)
		},
	}
	listener, err := listenTrustedUnixSocketWithHooks(
		socketPath,
		gid,
		testLocalControlPAID,
		hooks,
	)
	if listener != nil {
		_ = listener.Close()
		t.Fatal("stale cleanup accepted a socket rebound after validation")
	}
	if replacement == nil {
		t.Fatal("replacement hook did not install a live socket")
	}
	defer func() {
		_ = replacement.Close()
		_ = os.Remove(socketPath)
	}()
	if err == nil || !strings.Contains(err.Error(), "replacement restored without overwrite") {
		t.Fatalf("unexpected stale quarantine error: %v", err)
	}
	gotDev, gotIno := socketInode(t, socketPath)
	if gotDev != replacementDev || gotIno != replacementIno {
		t.Fatal("stale quarantine removed or replaced the live rebound socket")
	}
	assertUnixSocketIsLive(t, socketPath)
}

func TestUnixListenerCloseQuarantinePreservesBothLiveSocketsOnRestoreConflict(t *testing.T) {
	parent := trustedShortSocketParent(t)
	socketPath := filepath.Join(parent, "control.sock")
	gid := os.Getegid()
	var firstReplacement *net.UnixListener
	var secondReplacement *net.UnixListener
	var firstDev uint64
	var firstIno uint64
	var secondDev uint64
	var secondIno uint64
	var quarantinePath string
	hooks := &localControlListenerTestHooks{
		beforeSocketQuarantine: func() {
			if err := os.Remove(socketPath); err != nil {
				t.Fatal(err)
			}
			firstReplacement = bindTrustedTestSocket(t, socketPath, gid)
			firstDev, firstIno = socketInode(t, socketPath)
		},
		afterSocketQuarantine: func(quarantineName string) {
			quarantinePath = filepath.Join(parent, quarantineName)
			secondReplacement = bindTrustedTestSocket(t, socketPath, gid)
			secondDev, secondIno = socketInode(t, socketPath)
		},
	}
	owned, err := listenTrustedUnixSocketWithHooks(
		socketPath,
		gid,
		testLocalControlPAID,
		hooks,
	)
	if err != nil {
		t.Fatal(err)
	}
	err = owned.Close()
	if firstReplacement == nil || secondReplacement == nil || quarantinePath == "" {
		t.Fatal("close quarantine hooks did not install both live replacements")
	}
	defer func() {
		_ = firstReplacement.Close()
		_ = secondReplacement.Close()
		_ = os.Remove(quarantinePath)
		_ = os.Remove(socketPath)
	}()
	if err == nil || !strings.Contains(err.Error(), "both preserved") {
		t.Fatalf("unexpected close quarantine conflict error: %v", err)
	}
	gotDev, gotIno := socketInode(t, quarantinePath)
	if gotDev != firstDev || gotIno != firstIno {
		t.Fatal("close quarantine deleted the first rebound socket")
	}
	gotDev, gotIno = socketInode(t, socketPath)
	if gotDev != secondDev || gotIno != secondIno {
		t.Fatal("close quarantine overwrote the newly published socket")
	}
	assertUnixSocketIsLive(t, quarantinePath)
	assertUnixSocketIsLive(t, socketPath)
}

func TestUnixListenerRejectsAncestorSymlinkEvenWhenItResolvesToPinnedParent(t *testing.T) {
	root, err := os.MkdirTemp("", "su-r-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	ancestor := filepath.Join(root, "ancestor")
	parent := filepath.Join(ancestor, "parent")
	if err := os.MkdirAll(parent, localControlParentMode); err != nil {
		t.Fatal(err)
	}
	gid := os.Getegid()
	if err := os.Chown(parent, os.Geteuid(), gid); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, localControlParentMode); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(parent, "control.sock")
	parentInfo, err := os.Lstat(parent)
	if err != nil {
		t.Fatal(err)
	}
	expectedParent, ok := parentInfo.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("configured parent inode is unavailable")
	}
	movedAncestor := filepath.Join(root, "ancestor-pinned")
	hooks := &localControlListenerTestHooks{
		afterLockPublication: func() {
			if err := os.Rename(ancestor, movedAncestor); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(movedAncestor, ancestor); err != nil {
				t.Fatal(err)
			}
		},
	}
	listener, err := listenTrustedUnixSocketWithHooks(
		socketPath,
		gid,
		testLocalControlPAID,
		hooks,
	)
	if listener != nil {
		_ = listener.Close()
		t.Fatal("listener accepted an ancestor symlink back to the pinned parent")
	}
	if err == nil || !strings.Contains(err.Error(), "without symlinks") {
		t.Fatalf("unexpected same-inode ancestor symlink error: %v", err)
	}
	movedParent := filepath.Join(movedAncestor, "parent")
	if _, err := os.Lstat(filepath.Join(movedParent, "control.sock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ancestor symlink race published a socket in the pinned parent: %v", err)
	}

	if err := os.Remove(ancestor); err != nil {
		t.Fatal(err)
	}
	replacementAncestor := filepath.Join(root, "ancestor-replacement")
	replacementParent := filepath.Join(replacementAncestor, "parent")
	if err := os.MkdirAll(replacementParent, localControlParentMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(replacementParent, os.Geteuid(), gid); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(replacementParent, localControlParentMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(replacementAncestor, ancestor); err != nil {
		t.Fatal(err)
	}
	if err := validateConfiguredParentIdentity(parent, *expectedParent, gid); err == nil ||
		!strings.Contains(err.Error(), "without symlinks") {
		t.Fatalf("retargeted ancestor symlink was accepted: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(replacementParent, "control.sock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retargeted ancestor received a socket: %v", err)
	}
}

func TestUnixListenerRecoversAuthenticatedLegacyPartialPublishedLock(t *testing.T) {
	parent := trustedSocketParent(t)
	socketPath := filepath.Join(parent, "control.sock")
	lockPath := socketPath + ".owner.lock"
	gid := os.Getegid()
	if err := os.WriteFile(lockPath, nil, localControlLockMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(lockPath, os.Geteuid(), gid); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(lockPath, localControlLockMode); err != nil {
		t.Fatal(err)
	}

	listener, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID)
	if err != nil {
		t.Fatalf("restart rejected authenticated legacy partial lock: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(content, []byte("personality_agent_id="+testLocalControlPAID)) {
		t.Fatal("recovered lock was not republished with the exact PAID binding")
	}
}

func TestUnixListenerNeverTreatsLivePrivateSocketAsCrashResidue(t *testing.T) {
	parent := trustedSocketParent(t)
	socketPath := filepath.Join(parent, "control.sock")
	gid := os.Getegid()

	initializer, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID)
	if err != nil {
		t.Fatal(err)
	}
	if err := initializer.Close(); err != nil {
		t.Fatal(err)
	}
	live, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	if err := os.Chown(socketPath, os.Geteuid(), gid); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		t.Fatal(err)
	}
	dev, ino := socketInode(t, socketPath)

	if listener, err := listenTrustedUnixSocket(socketPath, gid, testLocalControlPAID); err == nil {
		_ = listener.Close()
		t.Fatal("restart replaced a live private initialization socket")
	} else if !strings.Contains(err.Error(), "live local control socket") {
		t.Fatalf("unexpected live private socket error: %v", err)
	}
	gotDev, gotIno := socketInode(t, socketPath)
	if gotDev != dev || gotIno != ino {
		t.Fatal("restart removed a live private initialization socket")
	}
}

func TestLocalControlTransportRequiresExactlyOneExplicitSelection(t *testing.T) {
	t.Setenv("SUMI_LOCAL_CONTROL_UNIX_SOCKET", "")
	t.Setenv("SUMI_LOCAL_CONTROL_LOOPBACK_LISTEN", "")
	if _, err := localControlListenerFromEnv(true); err == nil {
		t.Fatal("enabled local control accepted neither transport")
	}
	t.Setenv("SUMI_LOCAL_CONTROL_UNIX_SOCKET", "/run/sumi/local-control/control.sock")
	t.Setenv("SUMI_LOCAL_CONTROL_LOOPBACK_LISTEN", "127.0.0.1:4321")
	if _, err := localControlListenerFromEnv(true); err == nil {
		t.Fatal("enabled local control accepted both transports")
	}
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

func TestLocalControlServerFromEnvRejectsPartialOrAmbiguousEnablement(t *testing.T) {
	store, err := agentevents.OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	gateway, err := agentevents.OpenDurableGateway(t.TempDir(), store)
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv("SUMI_LOCAL_CONTROL_ENABLED", "true")
	if _, _, err := localControlServerFromEnv(gateway); err == nil {
		t.Fatal("ambiguous local control enablement was accepted")
	}

	t.Setenv("SUMI_LOCAL_CONTROL_ENABLED", "1")
	t.Setenv("SUMI_LOCAL_CONTROL_BEARER", "")
	if _, _, err := localControlServerFromEnv(gateway); err == nil ||
		!strings.Contains(err.Error(), "SUMI_LOCAL_CONTROL_BEARER") {
		t.Fatalf("partial local control config was not rejected precisely: %v", err)
	}

	t.Setenv("SUMI_LOCAL_CONTROL_BEARER", "server-fixture-control-bearer-32-bytes-minimum")
	t.Setenv("SUMI_LOCAL_CONTROL_TENANT_ID", "tenant-fixture")
	t.Setenv("SUMI_LOCAL_CONTROL_PERSONALITY_AGENT_ID", "0198f0f4-9b72-7000-8000-000000000001")
	t.Setenv("SUMI_LOCAL_CONTROL_GENERATION", "7")
	t.Setenv("SUMI_LOCAL_CONTROL_RPC_BOOT_NONCE", "boot-fixture")
	t.Setenv("SUMI_LOCAL_CONTROL_DELIVERY_AUTHORIZATION", "raw")
	t.Setenv("SUMI_AGENT_TOKEN_SECRET", base64.StdEncoding.EncodeToString(testTokenSecret))
	t.Setenv("SUMI_AGENT_TOKEN_AUDIENCE", "sumi:agent:events")
	t.Setenv("SUMI_LOCAL_CONTROL_AUDIENCE", "wrong-audience")
	if _, _, err := localControlServerFromEnv(gateway); err == nil ||
		!strings.Contains(err.Error(), "must match") {
		t.Fatalf("issuer/verifier audience split was accepted: %v", err)
	}

	t.Setenv("SUMI_LOCAL_CONTROL_AUDIENCE", "sumi:agent:events")
	t.Setenv("SUMI_LOCAL_CONTROL_BEARER", string(testTokenSecret))
	if _, _, err := localControlServerFromEnv(gateway); err == nil {
		t.Fatal("decoded agent token secret was accepted as the local control bearer")
	} else if strings.Contains(err.Error(), string(testTokenSecret)) {
		t.Fatal("secret-separation error exposed the credential value")
	}
}

func TestLocalControlServerFromEnvMigratesPreviousSigningSecret(t *testing.T) {
	store, err := agentevents.OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runtimeDir := t.TempDir()
	oldGateway, err := agentevents.OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}

	const (
		personalityAgentID = "0198f0f4-9b72-7000-8000-000000000001"
		bearer             = "server-rotation-control-bearer-32-bytes-minimum"
	)
	oldSecret := []byte("server-old-signing-secret-32-bytes-minimum")
	currentSecret := []byte("server-new-signing-secret-32-bytes-minimum")
	t.Setenv("SUMI_LOCAL_CONTROL_ENABLED", "1")
	t.Setenv("SUMI_LOCAL_CONTROL_BEARER", bearer)
	t.Setenv("SUMI_LOCAL_CONTROL_TENANT_ID", "tenant-fixture")
	t.Setenv("SUMI_LOCAL_CONTROL_PERSONALITY_AGENT_ID", personalityAgentID)
	t.Setenv("SUMI_LOCAL_CONTROL_GENERATION", "7")
	t.Setenv("SUMI_LOCAL_CONTROL_RPC_BOOT_NONCE", "boot-fixture")
	t.Setenv("SUMI_LOCAL_CONTROL_AUDIENCE", "sumi:agent:events")
	t.Setenv("SUMI_LOCAL_CONTROL_DELIVERY_AUTHORIZATION", "raw")
	t.Setenv("SUMI_AGENT_TOKEN_AUDIENCE", "sumi:agent:events")
	t.Setenv("SUMI_AGENT_TOKEN_SECRET", base64.StdEncoding.EncodeToString(oldSecret))
	t.Setenv(localControlPreviousSigningSecretsEnv, "")

	oldControl, enabled, err := localControlServerFromEnv(oldGateway)
	if err != nil {
		t.Fatal(err)
	}
	if !enabled {
		t.Fatal("complete local-control environment did not enable the server")
	}
	mux := http.NewServeMux()
	if err := oldControl.RegisterRoutes(mux); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(mux)
	publication, err := json.Marshal(agentevents.LocalRuntimeStatePublication{
		PublicationID:      "startup-before-server-secret-rotation",
		PersonalityAgentID: personalityAgentID,
		Generation:         7,
		RPCBootNonce:       "boot-fixture",
		State:              agentevents.LocalRuntimeNotReady,
		Reason:             agentevents.LocalRuntimeStartup,
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(
		http.MethodPost,
		server.URL+agentevents.LocalRuntimeStatePublishPath,
		bytes.NewReader(publication),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+bearer)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	server.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("seed local-control state: got %d, want 200", response.StatusCode)
	}

	claims := agentevents.TokenClaims{
		PersonalityAgentID: personalityAgentID,
		Generation:         7,
	}
	lease, err := oldGateway.ClaimConnectionLease(context.Background(), claims)
	if err != nil {
		t.Fatal(err)
	}

	rotatedGateway, err := agentevents.OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("SUMI_AGENT_TOKEN_SECRET", base64.StdEncoding.EncodeToString(currentSecret))
	t.Setenv(
		localControlPreviousSigningSecretsEnv,
		base64.StdEncoding.EncodeToString(oldSecret),
	)
	if _, enabled, err := localControlServerFromEnv(rotatedGateway); err != nil {
		t.Fatalf("server bootstrap did not accept the bounded previous signing secret: %v", err)
	} else if !enabled {
		t.Fatal("rotation bootstrap unexpectedly disabled local control")
	}

	currentOnly, err := agentevents.OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(localControlPreviousSigningSecretsEnv, "")
	if _, _, err := localControlServerFromEnv(currentOnly); err != nil {
		t.Fatalf("current-only bootstrap rejected migrated durable state: %v", err)
	}
	if err := currentOnly.ValidateConnectionLease(context.Background(), claims, lease); err != nil {
		t.Fatalf("server bootstrap rotation changed lease authority: %v", err)
	}
}

func TestLocalControlPreviousSigningSecretsFromEnvIsBoundedAndStrict(t *testing.T) {
	first := []byte("first-previous-server-signing-secret-32-bytes")
	second := []byte("second-previous-server-signing-secret-32-bytes")
	t.Setenv(
		localControlPreviousSigningSecretsEnv,
		base64.StdEncoding.EncodeToString(first)+", "+
			base64.StdEncoding.EncodeToString(second),
	)
	secrets, err := localControlPreviousSigningSecretsFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if len(secrets) != 2 || !bytes.Equal(secrets[0], first) || !bytes.Equal(secrets[1], second) {
		t.Fatalf("unexpected previous signing secrets: %d", len(secrets))
	}

	t.Setenv(
		localControlPreviousSigningSecretsEnv,
		base64.StdEncoding.EncodeToString(first)+",",
	)
	if _, err := localControlPreviousSigningSecretsFromEnv(); err == nil {
		t.Fatal("empty previous signing-secret entry was accepted")
	}
	t.Setenv(localControlPreviousSigningSecretsEnv, "not-base64")
	if _, err := localControlPreviousSigningSecretsFromEnv(); err == nil {
		t.Fatal("malformed previous signing secret was accepted")
	}
	t.Setenv(
		localControlPreviousSigningSecretsEnv,
		strings.Join([]string{
			base64.StdEncoding.EncodeToString(first),
			base64.StdEncoding.EncodeToString(second),
			base64.StdEncoding.EncodeToString(first),
		}, ","),
	)
	if _, err := localControlPreviousSigningSecretsFromEnv(); err == nil {
		t.Fatal("unbounded previous signing-secret set was accepted")
	}
}
