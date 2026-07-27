package agentevents

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
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

func TestHMACTokenVerifierProcessGenerationBoundary(t *testing.T) {
	v, err := NewHMACTokenVerifier(testSecret, "")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	valid := signTestToken(t, testSecret, tokenClaims{
		TenantID:       "tenant-1",
		AgentID:        "agent-1",
		ConversationID: "conversation-1",
		Generation:     maxProcessGeneration,
		Exp:            time.Now().Add(time.Hour).Unix(),
		Aud:            defaultAgentAudience,
	})
	if _, err := v.Verify(context.Background(), valid); err != nil {
		t.Fatalf("max process generation must be accepted: %v", err)
	}

	invalid := signTestToken(t, testSecret, tokenClaims{
		TenantID:       "tenant-1",
		AgentID:        "agent-1",
		ConversationID: "conversation-1",
		Generation:     maxProcessGeneration + 1,
		Exp:            time.Now().Add(time.Hour).Unix(),
		Aud:            defaultAgentAudience,
	})
	if _, err := v.Verify(context.Background(), invalid); err == nil {
		t.Fatal("generation max+1 must be rejected before hello emission")
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

func TestHMACTokenVerifierAcceptsPaddedSignature(t *testing.T) {
	v, err := NewHMACTokenVerifier(testSecret, "")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	token := signTestToken(t, testSecret, tokenClaims{
		TenantID:       "tenant-1",
		AgentID:        "agent-1",
		ConversationID: "conversation-1",
		Generation:     7,
		Exp:            time.Now().Add(time.Minute).Unix(),
		Aud:            defaultAgentAudience,
	})
	parts := strings.Split(token, ".")
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	parts[2] = base64.URLEncoding.EncodeToString(signature)
	if _, err := v.Verify(context.Background(), strings.Join(parts, ".")); err != nil {
		t.Fatalf("padded signature must verify: %v", err)
	}
}

func TestHMACTokenVerifierRejectsDuplicateKeys(t *testing.T) {
	v, err := NewHMACTokenVerifier(testSecret, "")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","alg":"HS256","typ":"JWT"}`))
	claims := base64.RawURLEncoding.EncodeToString(mustJSON(t, tokenClaims{
		TenantID:       "tenant-1",
		AgentID:        "agent-1",
		ConversationID: "conversation-1",
		Generation:     7,
		Exp:            time.Now().Add(time.Hour).Unix(),
		Aud:            defaultAgentAudience,
	}))
	token := signTokenWithParts(t, testSecret, header, claims)
	if _, err := v.Verify(context.Background(), token); err == nil {
		t.Fatal("expected duplicate keys in header to be rejected")
	}

	header = base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claims = base64.RawURLEncoding.EncodeToString([]byte(`{"tenant_id":"tenant-1","tenant_id":"tenant-1","agent_id":"agent-1","conversation_id":"conversation-1","generation":7,"exp":` + fmt.Sprintf("%d", time.Now().Add(time.Hour).Unix()) + `,"aud":"` + defaultAgentAudience + `"}`))
	token = signTokenWithParts(t, testSecret, header, claims)
	if _, err := v.Verify(context.Background(), token); err == nil {
		t.Fatal("expected duplicate keys in claims to be rejected")
	}
}

func TestHMACTokenVerifierRejectsUnknownFieldsAndWrongTyp(t *testing.T) {
	v, err := NewHMACTokenVerifier(testSecret, "")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT","extra":"field"}`))
	claims := base64.RawURLEncoding.EncodeToString(mustJSON(t, tokenClaims{
		TenantID:       "tenant-1",
		AgentID:        "agent-1",
		ConversationID: "conversation-1",
		Generation:     7,
		Exp:            time.Now().Add(time.Hour).Unix(),
		Aud:            defaultAgentAudience,
	}))
	token := signTokenWithParts(t, testSecret, header, claims)
	if _, err := v.Verify(context.Background(), token); err == nil {
		t.Fatal("expected unknown header field to be rejected")
	}

	header = base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"token"}`))
	token = signTokenWithParts(t, testSecret, header, claims)
	if _, err := v.Verify(context.Background(), token); err == nil {
		t.Fatal("expected wrong typ to be rejected")
	}
}

func signTokenWithParts(t *testing.T, secret []byte, header, claims string) string {
	t.Helper()
	signingInput := header + "." + claims
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signingInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signingInput + "." + sig
}

func TestHMACTokenVerifierSignatureCheckedBeforeClaims(t *testing.T) {
	v, err := NewHMACTokenVerifier(testSecret, "")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	// A valid token with expired claims must reach claim handling and fail
	// with a token-expired error.
	expired := signTestToken(t, testSecret, tokenClaims{
		TenantID:       "tenant-1",
		AgentID:        "agent-1",
		ConversationID: "conversation-1",
		Generation:     7,
		Exp:            time.Now().Add(-time.Hour).Unix(),
		Aud:            defaultAgentAudience,
	})
	_, verifyErr := v.Verify(context.Background(), expired)
	if verifyErr == nil {
		t.Fatal("expected expired token to be rejected")
	}
	if !strings.Contains(verifyErr.Error(), "token expired") {
		t.Fatalf("expected expired error, got %v", verifyErr)
	}

	// Tamper with the signature; the same expired claims must now fail with a
	// signature error, not an expired error, proving claims are not parsed first.
	tampered := expired + "x"
	_, verifyErr = v.Verify(context.Background(), tampered)
	if verifyErr == nil {
		t.Fatal("expected tampered token to be rejected")
	}
	if !strings.Contains(verifyErr.Error(), "invalid token signature") {
		t.Fatalf("expected signature error before claim handling, got %v", verifyErr)
	}

	// A token with a completely invalid/missing signature segment must also
	// fail with a signature error without reaching claim handling.
	parts := strings.Split(expired, ".")
	emptySig := parts[0] + "." + parts[1] + "."
	_, verifyErr = v.Verify(context.Background(), emptySig)
	if verifyErr == nil {
		t.Fatal("expected empty signature to be rejected")
	}
	if !strings.Contains(verifyErr.Error(), "invalid token signature") {
		t.Fatalf("expected signature error for empty signature, got %v", verifyErr)
	}

	// A token with a malformed header/claim encoding but a matching signature
	// must not be rejected by signature and then fail at header parsing.
	malformedPayload := base64.RawURLEncoding.EncodeToString([]byte(`not json`))
	malformedHeader := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	signingInput := malformedHeader + "." + malformedPayload
	mac := hmac.New(sha256.New, testSecret)
	mac.Write([]byte(signingInput))
	malformedSig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	malformedToken := signingInput + "." + malformedSig
	_, verifyErr = v.Verify(context.Background(), malformedToken)
	if verifyErr == nil {
		t.Fatal("expected malformed token to be rejected")
	}
	if !strings.Contains(verifyErr.Error(), "parse token claims") && !strings.Contains(verifyErr.Error(), "parse token header") {
		t.Fatalf("expected parsing error after valid signature, got %v", verifyErr)
	}
}
