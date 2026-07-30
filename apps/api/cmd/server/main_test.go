package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

var testTokenSecret = []byte("test-secret-32bytes-long-string!!")
var testSessionSecret = []byte("browser-session-secret-32-bytes!!")

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
	Exp                int64  `json:"exp"`
	Aud                string `json:"aud"`
}

type testCommandReceipt struct {
	IdempotencyKey string `json:"idempotency_key"`
	CommandID      string `json:"command_id"`
	Seq            uint64 `json:"seq"`
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

func setTokenSecret(t *testing.T) {
	t.Helper()
	t.Setenv("SUMI_AGENT_TOKEN_SECRET", base64.StdEncoding.EncodeToString(testTokenSecret))
	t.Setenv("SUMI_AGENT_RUNTIME_STATE_DIR", t.TempDir())
}

func setSessionSecret(t *testing.T) {
	t.Helper()
	t.Setenv("SUMI_BROWSER_SESSION_SECRET", base64.StdEncoding.EncodeToString(testSessionSecret))
	t.Setenv("SUMI_BROWSER_SESSION_AUDIENCE", agentevents.DefaultBrowserAudience())
	t.Setenv("SUMI_AGENT_RUNTIME_STATE_DIR", t.TempDir())
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
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	return resp
}

func postWithSessionCookie(t *testing.T, serverURL, personalityAgentID string, body []byte) *http.Response {
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
	req.Header.Set("Idempotency-Key", "test-key")
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

func TestNewRouter_RegistersCommandRouteWithBrowserSession(t *testing.T) {
	setSessionSecret(t)
	t.Setenv("SUMI_COMMAND_LOG_DIR", t.TempDir())
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
	t.Setenv("SUMI_COMMAND_LOG_DIR", t.TempDir())
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

func TestNewRouter_LocalControlRoutesRequireExplicitCompleteFixtureConfig(t *testing.T) {
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
	t.Setenv("SUMI_LOCAL_CONTROL_PERSONALITY_AGENT_ID", "0198f0f4-9b72-7000-8000-000000000001")
	t.Setenv("SUMI_LOCAL_CONTROL_GENERATION", "7")
	t.Setenv("SUMI_LOCAL_CONTROL_RPC_BOOT_NONCE", "boot-fixture")
	t.Setenv("SUMI_LOCAL_CONTROL_AUDIENCE", "sumi:agent:events")
	t.Setenv("SUMI_LOCAL_CONTROL_DELIVERY_AUTHORIZATION", "raw")
	t.Setenv("SUMI_AGENT_TOKEN_SECRET", base64.StdEncoding.EncodeToString(testTokenSecret))
	commandDir := t.TempDir()
	runtimeDir := t.TempDir()
	t.Setenv("SUMI_COMMAND_LOG_DIR", commandDir)
	t.Setenv("SUMI_AGENT_RUNTIME_STATE_DIR", runtimeDir)
	mux, err = newRouter()
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(mux)
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
	t.Setenv("SUMI_LOCAL_CONTROL_BEARER", "server-fixture-next-control-bearer-32-bytes-minimum")
	t.Setenv("SUMI_LOCAL_CONTROL_GENERATION", "8")
	t.Setenv("SUMI_LOCAL_CONTROL_RPC_BOOT_NONCE", "boot-fixture-next")
	mux, err = newRouter()
	if err != nil {
		t.Fatal(err)
	}
	restartedServer := httptest.NewServer(mux)
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
