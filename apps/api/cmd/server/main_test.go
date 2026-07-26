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

type testTokenClaims struct {
	TenantID       string `json:"tenant_id"`
	AgentID        string `json:"agent_id"`
	ConversationID string `json:"conversation_id"`
	Generation     uint64 `json:"generation"`
	Exp            int64  `json:"exp"`
	Aud            string `json:"aud"`
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

func setTokenSecret(t *testing.T) {
	t.Helper()
	t.Setenv("SUMI_AGENT_TOKEN_SECRET", base64.StdEncoding.EncodeToString(testTokenSecret))
	t.Setenv("SUMI_AGENT_RUNTIME_STATE_DIR", t.TempDir())
}

func postAuthorized(t *testing.T, serverURL, conversationID string, body []byte) *http.Response {
	t.Helper()
	token := signTestToken(t, testTokenSecret, testTokenClaims{
		TenantID:       "tenant-1",
		AgentID:        "agent-1",
		ConversationID: conversationID,
		Generation:     7,
		Exp:            time.Now().Add(time.Hour).Unix(),
		Aud:            "sumi:agent:events",
	})
	req, err := http.NewRequest(http.MethodPost, serverURL+"/conversations/"+conversationID+"/commands", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
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
}

func TestNewRouter_RegistersCommandRoute(t *testing.T) {
	setTokenSecret(t)
	t.Setenv("SUMI_COMMAND_LOG_DIR", t.TempDir())
	mux, err := newRouter()
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(mux)
	defer server.Close()

	body := []byte(`{"type":"user_message","text":"hello","attachments":[]}`)
	resp := postAuthorized(t, server.URL, "c-1", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	var env agentevents.CommandEnvelope
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

func TestNewRouter_CommandRouteRejectsOversized(t *testing.T) {
	setTokenSecret(t)
	t.Setenv("SUMI_COMMAND_LOG_DIR", t.TempDir())
	mux, err := newRouter()
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(mux)
	defer server.Close()

	text := strings.Repeat("x", 1024*1024+1)
	body := []byte(`{"type":"user_message","text":"` + text + `","attachments":[]}`)
	resp := postAuthorized(t, server.URL, "c-1", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestNewRouter_CommandRouteIdempotency(t *testing.T) {
	setTokenSecret(t)
	t.Setenv("SUMI_COMMAND_LOG_DIR", t.TempDir())
	mux, err := newRouter()
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(mux)
	defer server.Close()

	body := []byte(`{"type":"user_message","text":"idem","attachments":[]}`)

	req1, err := http.NewRequest(http.MethodPost, server.URL+"/conversations/c-1/commands", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set("Authorization", "Bearer "+signTestToken(t, testTokenSecret, testTokenClaims{
		TenantID:       "tenant-1",
		AgentID:        "agent-1",
		ConversationID: "c-1",
		Generation:     7,
		Exp:            time.Now().Add(time.Hour).Unix(),
		Aud:            "sumi:agent:events",
	}))
	req1.Header.Set("Idempotency-Key", "idem-key-1")

	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	if resp1.StatusCode != http.StatusCreated {
		t.Fatalf("expected first response 201, got %d", resp1.StatusCode)
	}
	var env1 agentevents.CommandEnvelope
	if err := json.NewDecoder(resp1.Body).Decode(&env1); err != nil {
		t.Fatal(err)
	}
	resp1.Body.Close()
	if env1.Seq == 0 || env1.CommandID == "" {
		t.Fatalf("expected non-empty first command envelope, got %+v", env1)
	}

	req2, err := http.NewRequest(http.MethodPost, server.URL+"/conversations/c-1/commands", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Authorization", "Bearer "+signTestToken(t, testTokenSecret, testTokenClaims{
		TenantID:       "tenant-1",
		AgentID:        "agent-1",
		ConversationID: "c-1",
		Generation:     7,
		Exp:            time.Now().Add(time.Hour).Unix(),
		Aud:            "sumi:agent:events",
	}))
	req2.Header.Set("Idempotency-Key", "idem-key-1")

	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	if resp2.StatusCode != http.StatusCreated {
		t.Fatalf("expected second response 201, got %d", resp2.StatusCode)
	}
	var env2 agentevents.CommandEnvelope
	if err := json.NewDecoder(resp2.Body).Decode(&env2); err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()

	if env1.Seq != env2.Seq || env1.CommandID != env2.CommandID {
		t.Fatalf("idempotency key did not return the same command: %+v vs %+v", env1, env2)
	}
}
