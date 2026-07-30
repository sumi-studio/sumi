package agentevents

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
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
	Iat                int64  `json:"iat"`
	Exp                int64  `json:"exp"`
	Aud                string `json:"aud"`
	SID                string `json:"sid"`
}

func signTestSession(t *testing.T, secret []byte, claims testSessionClaims) string {
	t.Helper()
	if claims.Iat == 0 && claims.Exp != 0 {
		claims.Iat = claims.Exp - int64(maxBrowserSessionTTL/time.Second)
	}
	if claims.SID == "" {
		claims.SID = base64.RawURLEncoding.EncodeToString(make([]byte, browserSessionIDBytes))
	}
	return signRawTestSession(t, secret, claims)
}

func signRawTestSession(t *testing.T, secret []byte, claims testSessionClaims) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString(mustJSON(t, claims))
	signingInput := header + "." + payload
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signingInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signingInput + "." + sig
}

func TestHMACUserSessionVerifierRequiresBoundedLifecycleClaims(t *testing.T) {
	v, err := NewHMACUserSessionVerifier(testSessionSecret, "")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 31, 1, 0, 0, 0, time.UTC)
	v.now = func() time.Time { return now }
	valid := testSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		Iat:                now.Add(-time.Minute).Unix(),
		Exp:                now.Add(time.Minute).Unix(),
		Aud:                defaultBrowserAudience,
		SID:                base64.RawURLEncoding.EncodeToString(make([]byte, browserSessionIDBytes)),
	}

	cases := []struct {
		name   string
		mutate func(*testSessionClaims)
	}{
		{name: "missing iat", mutate: func(c *testSessionClaims) { c.Iat = 0 }},
		{name: "future iat", mutate: func(c *testSessionClaims) { c.Iat = now.Add(time.Second).Unix() }},
		{name: "nonpositive lifetime", mutate: func(c *testSessionClaims) { c.Exp = c.Iat }},
		{name: "overlong lifetime", mutate: func(c *testSessionClaims) { c.Exp = c.Iat + int64(maxBrowserSessionTTL/time.Second) + 1 }},
		{name: "expired", mutate: func(c *testSessionClaims) { c.Exp = now.Unix() }},
		{name: "missing sid", mutate: func(c *testSessionClaims) { c.SID = "" }},
		{name: "short sid", mutate: func(c *testSessionClaims) {
			c.SID = base64.RawURLEncoding.EncodeToString(make([]byte, browserSessionIDBytes-1))
		}},
		{name: "padded sid", mutate: func(c *testSessionClaims) {
			c.SID = base64.URLEncoding.EncodeToString(make([]byte, browserSessionIDBytes))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claims := valid
			tc.mutate(&claims)
			if _, err := v.VerifySession(context.Background(), signRawTestSession(t, testSessionSecret, claims)); err == nil {
				t.Fatal("expected lifecycle claims to be rejected")
			}
		})
	}
}

func TestHMACUserSessionVerifierRevocationIsAnAdmissionBarrier(t *testing.T) {
	v, err := NewHMACUserSessionVerifier(testSessionSecret, "")
	if err != nil {
		t.Fatal(err)
	}
	session, err := v.IssueSession(context.Background(), UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := v.VerifySession(context.Background(), session)
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	authorized := make(chan error, 1)
	go func() {
		authorized <- v.AuthorizeSession(context.Background(), claims, func() error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started
	revoked := make(chan error, 1)
	go func() {
		_, err := v.RevokeSession(context.Background(), session)
		revoked <- err
	}()
	select {
	case err := <-revoked:
		t.Fatalf("revocation crossed an in-flight admission: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	if err := <-authorized; err != nil {
		t.Fatalf("authorized operation: %v", err)
	}
	if err := <-revoked; err != nil {
		t.Fatalf("revoke session: %v", err)
	}
	if _, err := v.VerifySession(context.Background(), session); err == nil {
		t.Fatal("revoked session still verified")
	}
	called := false
	if err := v.AuthorizeSession(context.Background(), claims, func() error {
		called = true
		return nil
	}); err == nil || called {
		t.Fatal("revoked session admitted a new operation")
	}
}

func TestHMACUserSessionVerifierBoundsAndReclaimsRevocations(t *testing.T) {
	v, err := NewHMACUserSessionVerifier(testSessionSecret, "")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 31, 1, 0, 0, 0, time.UTC)
	v.now = func() time.Time { return now }
	v.maxRevoked = 1
	claims := UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}
	first, err := v.IssueSession(context.Background(), claims, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	second, err := v.IssueSession(context.Background(), claims, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.RevokeSession(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := v.RevokeSession(context.Background(), second); !errors.Is(err, errRevocationCapacity) {
		t.Fatalf("got %v, want revocation capacity error", err)
	}
	now = now.Add(time.Minute + time.Second)
	if _, err := v.RevokeSession(context.Background(), second); err != nil {
		t.Fatalf("expired revocation was not reclaimed: %v", err)
	}
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
	if got.TenantID != want.TenantID ||
		got.UserID != want.UserID ||
		got.PersonalityAgentID != want.PersonalityAgentID {
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
