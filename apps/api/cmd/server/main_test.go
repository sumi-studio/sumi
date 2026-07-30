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
	"strconv"
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
	t.Setenv("SUMI_LOCAL_CONTROL_PERSONALITY_AGENT_ID", "0198f0f4-9b72-7000-8000-000000000001")
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
		if _, err := listenTrustedUnixSocket(socketPath, gid); err == nil ||
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
		if _, err := listenTrustedUnixSocket(filepath.Join(link, "control.sock"), gid); err == nil ||
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
		if _, err := listenTrustedUnixSocket(socketPath, gid); err == nil ||
			!strings.Contains(err.Error(), "not a Unix socket") {
			t.Fatalf("non-socket target was accepted: %v", err)
		}
		if _, err := os.Lstat(socketPath); err != nil {
			t.Fatalf("untrusted target was removed: %v", err)
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
		if err := os.Chmod(socketPath, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := stale.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := listenTrustedUnixSocket(socketPath, gid); err == nil ||
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
		if _, err := listenTrustedUnixSocket(socketPath, gid); err == nil ||
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
		if _, err := listenTrustedUnixSocket(socketPath, gid); err == nil ||
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
		replacement, err := listenTrustedUnixSocket(socketPath, gid)
		if err != nil {
			t.Fatalf("trusted stale socket was not recovered: %v", err)
		}
		if err := replacement.Close(); err != nil {
			t.Fatal(err)
		}
	})
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
