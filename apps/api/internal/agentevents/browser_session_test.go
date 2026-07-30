package agentevents

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"
)

var testSessionSecret = []byte("browser-session-secret-32-bytes!!")

type testSessionClaims struct {
	TenantID           string `json:"tenant_id"`
	UserID             string `json:"user_id"`
	PersonalityAgentID string `json:"personality_agent_id"`
	Exp                int64  `json:"exp"`
	Aud                string `json:"aud"`
}

func signTestSession(t *testing.T, secret []byte, claims testSessionClaims) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString(mustJSON(t, claims))
	signingInput := header + "." + payload
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signingInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signingInput + "." + sig
}

func TestHMACUserSessionVerifierAcceptsValidSession(t *testing.T) {
	v, err := NewHMACUserSessionVerifier(testSessionSecret, "")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	session := signTestSession(t, testSessionSecret, testSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                defaultBrowserAudience,
	})

	claims, err := v.VerifySession(context.Background(), session)
	if err != nil {
		t.Fatalf("verify valid session: %v", err)
	}
	if claims.TenantID != "tenant-1" || claims.UserID != "user-1" || claims.PersonalityAgentID != "018f47a2-9b3c-7def-8abc-0123456789ab" {
		t.Fatalf("unexpected claims: %+v", claims)
	}
}

func TestHMACUserSessionVerifierIssuesItsOwnVerifiableSession(t *testing.T) {
	v, err := NewHMACUserSessionVerifier(testSessionSecret, "")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	want := UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}
	session, err := v.IssueSession(context.Background(), want, 5*time.Minute)
	if err != nil {
		t.Fatalf("issue session: %v", err)
	}
	got, err := v.VerifySession(context.Background(), session)
	if err != nil {
		t.Fatalf("verify issued session: %v", err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	if _, err := v.IssueSession(context.Background(), want, time.Hour+time.Second); err == nil {
		t.Fatal("expected overlong session TTL to be rejected")
	}
}

func TestHMACUserSessionVerifierRejectsWrongAudience(t *testing.T) {
	v, err := NewHMACUserSessionVerifier(testSessionSecret, "sumi:web:conversation")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	session := signTestSession(t, testSessionSecret, testSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                "other-audience",
	})

	if _, err := v.VerifySession(context.Background(), session); err == nil {
		t.Fatal("expected wrong audience session to be rejected")
	}
}

func TestHMACUserSessionVerifierRejectsDuplicateKeysUnknownFieldsAndWrongTyp(t *testing.T) {
	v, err := NewHMACUserSessionVerifier(testSessionSecret, "")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	cases := []struct {
		name   string
		header string
		claims string
	}{
		{
			name:   "duplicate header keys",
			header: `{"alg":"HS256","alg":"HS256","typ":"JWT"}`,
			claims: `{"tenant_id":"tenant-1","user_id":"user-1","personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","exp":` + fmt.Sprintf("%d", time.Now().Add(time.Hour).Unix()) + `,"aud":"` + defaultBrowserAudience + `"}`,
		},
		{
			name:   "duplicate claims keys",
			header: `{"alg":"HS256","typ":"JWT"}`,
			claims: `{"tenant_id":"tenant-1","tenant_id":"tenant-1","user_id":"user-1","personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","exp":` + fmt.Sprintf("%d", time.Now().Add(time.Hour).Unix()) + `,"aud":"` + defaultBrowserAudience + `"}`,
		},
		{
			name:   "unknown header field",
			header: `{"alg":"HS256","typ":"JWT","extra":"field"}`,
			claims: `{"tenant_id":"tenant-1","user_id":"user-1","personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","exp":` + fmt.Sprintf("%d", time.Now().Add(time.Hour).Unix()) + `,"aud":"` + defaultBrowserAudience + `"}`,
		},
		{
			name:   "wrong typ",
			header: `{"alg":"HS256","typ":"session"}`,
			claims: `{"tenant_id":"tenant-1","user_id":"user-1","personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","exp":` + fmt.Sprintf("%d", time.Now().Add(time.Hour).Unix()) + `,"aud":"` + defaultBrowserAudience + `"}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			header := base64.RawURLEncoding.EncodeToString([]byte(tc.header))
			claims := base64.RawURLEncoding.EncodeToString([]byte(tc.claims))
			signingInput := header + "." + claims
			mac := hmac.New(sha256.New, testSessionSecret)
			mac.Write([]byte(signingInput))
			sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
			session := signingInput + "." + sig

			if _, err := v.VerifySession(context.Background(), session); err == nil {
				t.Fatal("expected malformed session to be rejected")
			}
		})
	}
}

func TestHMACUserSessionVerifierRejectsTamperedSignature(t *testing.T) {
	v, err := NewHMACUserSessionVerifier(testSessionSecret, "")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	session := signTestSession(t, testSessionSecret, testSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                defaultBrowserAudience,
	})
	session = session + "x"

	if _, err := v.VerifySession(context.Background(), session); err == nil {
		t.Fatal("expected tampered session to be rejected")
	}
}

func TestHMACUserSessionVerifierRejectsPaddedInput(t *testing.T) {
	// browser sessions use raw base64url; padded signatures should be trimmed
	// defensively before HMAC comparison.
	v, err := NewHMACUserSessionVerifier(testSessionSecret, "")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	session := signTestSession(t, testSessionSecret, testSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                defaultBrowserAudience,
	})
	parts := strings.Split(session, ".")
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	parts[2] = base64.URLEncoding.EncodeToString(sig)
	session = strings.Join(parts, ".")

	if _, err := v.VerifySession(context.Background(), session); err != nil {
		t.Fatalf("padded signature must verify: %v", err)
	}
}
