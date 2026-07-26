package agentevents

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

var testSecret = []byte("test-secret-32bytes-long-string!!")

func TestHMACTokenVerifierAcceptsValidToken(t *testing.T) {
	v, err := NewHMACTokenVerifier(testSecret, "")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	token := signTestToken(t, testSecret, tokenClaims{
		TenantID:       "tenant-1",
		AgentID:        "agent-1",
		ConversationID: "conversation-1",
		Generation:     7,
		Exp:            time.Now().Add(time.Hour).Unix(),
		Aud:            defaultAgentAudience,
	})

	claims, err := v.Verify(context.Background(), token)
	if err != nil {
		t.Fatalf("verify valid token: %v", err)
	}
	if claims.TenantID != "tenant-1" || claims.AgentID != "agent-1" || claims.ConversationID != "conversation-1" || claims.Generation != 7 {
		t.Fatalf("unexpected claims: %+v", claims)
	}
}

func TestHMACTokenVerifierRejectsExpiredToken(t *testing.T) {
	v, err := NewHMACTokenVerifier(testSecret, "")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	token := signTestToken(t, testSecret, tokenClaims{
		TenantID:       "tenant-1",
		AgentID:        "agent-1",
		ConversationID: "conversation-1",
		Generation:     7,
		Exp:            time.Now().Add(-time.Hour).Unix(),
		Aud:            defaultAgentAudience,
	})

	if _, err := v.Verify(context.Background(), token); err == nil {
		t.Fatal("expected expired token to be rejected")
	}
}

func TestHMACTokenVerifierRejectsTokenAtExpiryBoundary(t *testing.T) {
	v, err := NewHMACTokenVerifier(testSecret, "")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	token := signTestToken(t, testSecret, tokenClaims{
		TenantID:       "tenant-1",
		AgentID:        "agent-1",
		ConversationID: "conversation-1",
		Generation:     7,
		Exp:            time.Now().Unix(),
		Aud:            defaultAgentAudience,
	})

	if _, err := v.Verify(context.Background(), token); err == nil {
		t.Fatal("expected token at its expiry boundary to be rejected")
	}
}

func TestHMACTokenVerifierRejectsWrongAudience(t *testing.T) {
	v, err := NewHMACTokenVerifier(testSecret, "sumi:agent:events")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	token := signTestToken(t, testSecret, tokenClaims{
		TenantID:       "tenant-1",
		AgentID:        "agent-1",
		ConversationID: "conversation-1",
		Generation:     7,
		Exp:            time.Now().Add(time.Hour).Unix(),
		Aud:            "other-audience",
	})

	if _, err := v.Verify(context.Background(), token); err == nil {
		t.Fatal("expected wrong audience to be rejected")
	}
}

func TestHMACTokenVerifierRejectsTamperedSignature(t *testing.T) {
	v, err := NewHMACTokenVerifier(testSecret, "")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	token := signTestToken(t, testSecret, tokenClaims{
		TenantID:       "tenant-1",
		AgentID:        "agent-1",
		ConversationID: "conversation-1",
		Generation:     7,
		Exp:            time.Now().Add(time.Hour).Unix(),
		Aud:            defaultAgentAudience,
	})

	token = token + "x"
	if _, err := v.Verify(context.Background(), token); err == nil {
		t.Fatal("expected tampered token to be rejected")
	}
}

func TestHMACTokenVerifierRejectsAlgNone(t *testing.T) {
	v, err := NewHMACTokenVerifier(testSecret, "")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	claims := base64.RawURLEncoding.EncodeToString(mustJSON(t, tokenClaims{
		TenantID:       "tenant-1",
		AgentID:        "agent-1",
		ConversationID: "conversation-1",
		Generation:     7,
		Exp:            time.Now().Add(time.Hour).Unix(),
		Aud:            defaultAgentAudience,
	}))
	token := header + "." + claims + "."

	if _, err := v.Verify(context.Background(), token); err == nil {
		t.Fatal("expected alg=none token to be rejected")
	}
}

func TestHMACTokenVerifierRejectsMissingClaims(t *testing.T) {
	v, err := NewHMACTokenVerifier(testSecret, "")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	token := signTestToken(t, testSecret, tokenClaims{
		TenantID:       "",
		AgentID:        "agent-1",
		ConversationID: "conversation-1",
		Generation:     7,
		Exp:            time.Now().Add(time.Hour).Unix(),
		Aud:            defaultAgentAudience,
	})
	if _, err := v.Verify(context.Background(), token); err == nil {
		t.Fatal("expected missing tenant_id to be rejected")
	}

	token = signTestToken(t, testSecret, tokenClaims{
		TenantID:       "tenant-1",
		AgentID:        "agent-1",
		ConversationID: "conversation-1",
		Generation:     7,
		Exp:            0,
		Aud:            defaultAgentAudience,
	})
	if _, err := v.Verify(context.Background(), token); err == nil {
		t.Fatal("expected missing exp to be rejected")
	}
}

func TestHMACTokenVerifierRejectsShortSecret(t *testing.T) {
	if _, err := NewHMACTokenVerifier([]byte("short"), ""); err == nil {
		t.Fatal("expected short secret to be rejected")
	}
}

func signTestToken(t *testing.T, secret []byte, claims tokenClaims) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString(mustJSON(t, claims))
	signingInput := header + "." + payload
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signingInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signingInput + "." + sig
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	return b
}

func TestDecodeBase64URLAcceptsPaddedInput(t *testing.T) {
	// Ensure tokens with standard base64 padding are accepted defensively.
	padded := base64.URLEncoding.EncodeToString([]byte("hello world"))
	if !strings.Contains(padded, "=") {
		t.Fatal("expected padded base64url output")
	}
	b, err := decodeBase64URL(padded)
	if err != nil {
		t.Fatalf("decode padded: %v", err)
	}
	if string(b) != "hello world" {
		t.Fatalf("unexpected decoded value: %s", b)
	}
}
