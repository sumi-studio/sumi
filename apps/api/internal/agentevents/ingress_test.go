package agentevents

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
)

var testIngressSecret = []byte("test-ingress-session-secret-32!")

const testBrowserOrigin = "https://web.example"

type fakeCommandAppender struct {
	mu      sync.Mutex
	calls   []appendCall
	nextSeq uint64
}

type appendCall struct {
	PersonalityAgentID string
	Provenance         DirectChatProvenance
	IdempotencyKey     string
	Command            json.RawMessage
}

type fakeTokenVerifier struct {
	mu                 sync.Mutex
	personalityAgentID string
	reject             bool
	err                error
}

func (f *fakeTokenVerifier) setReject(reject bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reject = reject
}

func (f *fakeTokenVerifier) Verify(ctx context.Context, token string) (TokenClaims, error) {
	f.mu.Lock()
	reject := f.reject
	err := f.err
	personalityAgentID := f.personalityAgentID
	f.mu.Unlock()

	if reject {
		return TokenClaims{}, fmt.Errorf("rejected")
	}
	if err != nil {
		return TokenClaims{}, err
	}
	conv := personalityAgentID
	if conv == "" {
		conv = "018f47a2-9b3c-7def-8abc-0123456789ab"
	}
	return TokenClaims{
		TenantID:           "tenant-1",
		PersonalityAgentID: conv,
		Generation:         7,
	}, nil
}

type fakeSessionVerifier struct {
	mu                 sync.Mutex
	personalityAgentID string
	reject             bool
	err                error
}

func (f *fakeSessionVerifier) VerifySession(ctx context.Context, cookie string) (UserSessionClaims, error) {
	f.mu.Lock()
	reject := f.reject
	err := f.err
	personalityAgentID := f.personalityAgentID
	f.mu.Unlock()

	if reject {
		return UserSessionClaims{}, fmt.Errorf("rejected")
	}
	if err != nil {
		return UserSessionClaims{}, err
	}
	conv := personalityAgentID
	if conv == "" {
		conv = "018f47a2-9b3c-7def-8abc-0123456789ab"
	}
	return UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: conv,
	}, nil
}

func (f *fakeSessionVerifier) setReject(reject bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reject = reject
}

func signTestIngressSession(personalityAgentID string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claims, _ := json.Marshal(map[string]any{
		"tenant_id":            "tenant-1",
		"user_id":              "user-1",
		"personality_agent_id": personalityAgentID,
		"exp":                  1893456000,
		"aud":                  defaultBrowserAudience,
	})
	claimsPart := base64.RawURLEncoding.EncodeToString(claims)
	signingInput := header + "." + claimsPart
	mac := hmac.New(sha256.New, testIngressSecret)
	mac.Write([]byte(signingInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signingInput + "." + sig
}

type errorReadCloser struct{}

func (errorReadCloser) Read([]byte) (int, error) {
	return 0, errors.New("body read failed")
}

func (errorReadCloser) Close() error {
	return nil
}

func (f *fakeCommandAppender) Append(ctx context.Context, provenance DirectChatProvenance, idempotencyKey string, command json.RawMessage) (CommandEnvelope, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextSeq++
	f.calls = append(f.calls, appendCall{
		PersonalityAgentID: provenance.PersonalityAgentID,
		Provenance:         provenance,
		IdempotencyKey:     idempotencyKey,
		Command:            command,
	})
	return CommandEnvelope{
		Seq:                f.nextSeq,
		CommandID:          fmt.Sprintf("00000000-0000-4000-8000-%012d", f.nextSeq),
		PersonalityAgentID: provenance.PersonalityAgentID,
		Provenance:         provenance,
		Command:            command,
	}, nil
}

func (f *fakeCommandAppender) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func newTestIngress(t *testing.T) (*UserCommandIngress, *fakeCommandAppender) {
	t.Helper()
	appender := &fakeCommandAppender{}
	verifier := &fakeSessionVerifier{personalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab"}
	ingress, err := NewUserCommandIngress(appender, verifier)
	if err != nil {
		t.Fatalf("new ingress: %v", err)
	}
	ingress.AllowedOrigins = []string{testBrowserOrigin}
	return ingress, appender
}

func postWithSessionCookie(t *testing.T, url string, body []byte, personalityAgentID string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "test-key")
	req.Header.Set("Origin", testBrowserOrigin)
	req.AddCookie(&http.Cookie{Name: BrowserSessionCookie, Value: signTestIngressSession(personalityAgentID)})
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	return resp
}

func postWithAuthorization(t *testing.T, url string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer agent-token")
	req.Header.Set("Origin", testBrowserOrigin)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	return resp
}

func newCommandMux(ingress *UserCommandIngress) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("POST /direct-chat/commands", ingress)
	return mux
}

func TestUserCommandIngress_ValidRequestAllocatesSeq(t *testing.T) {
	ingress, appender := newTestIngress(t)
	server := httptest.NewServer(newCommandMux(ingress))
	defer server.Close()

	body := []byte(`{"type":"user_message","text":"hi","attachments":[]}`)
	resp := postWithSessionCookie(t, server.URL+"/direct-chat/commands", body, "018f47a2-9b3c-7def-8abc-0123456789ab")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
	if appender.callCount() != 1 {
		t.Fatalf("expected 1 append call, got %d", appender.callCount())
	}
	wantProvenance := testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab")
	if got := appender.calls[0].Provenance; got != wantProvenance {
		t.Fatalf("server-authored provenance mismatch: got %+v want %+v", got, wantProvenance)
	}

	var env browserCommandReceipt
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if env.Seq != 1 {
		t.Fatalf("expected seq 1, got %d", env.Seq)
	}
	if env.CommandID == "" {
		t.Fatal("expected non-empty command_id")
	}
}

func TestUserCommandIngress_BrowserOriginPolicyPrecedesSessionAndBody(t *testing.T) {
	ingress, appender := newTestIngress(t)
	body := `{"type":"user_message","text":"hi","attachments":[]}`

	for _, tc := range []struct {
		name       string
		origin     string
		wantStatus int
	}{
		{name: "allowed", origin: testBrowserOrigin, wantStatus: http.StatusUnauthorized},
		{name: "disallowed", origin: "https://evil.example", wantStatus: http.StatusForbidden},
		{name: "missing", wantStatus: http.StatusForbidden},
		{name: "not exact", origin: testBrowserOrigin + ".evil.example", wantStatus: http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/direct-chat/commands", strings.NewReader(body))
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			recorder := httptest.NewRecorder()

			ingress.ServeHTTP(recorder, req)

			if recorder.Code != tc.wantStatus {
				t.Fatalf("got status %d, want %d", recorder.Code, tc.wantStatus)
			}
			if appender.callCount() != 0 {
				t.Fatalf("origin/session rejection appended %d commands", appender.callCount())
			}
		})
	}

	t.Run("duplicated header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/direct-chat/commands", strings.NewReader(body))
		req.Header["Origin"] = []string{testBrowserOrigin, testBrowserOrigin}
		recorder := httptest.NewRecorder()

		ingress.ServeHTTP(recorder, req)

		if recorder.Code != http.StatusForbidden {
			t.Fatalf("got status %d, want %d", recorder.Code, http.StatusForbidden)
		}
		if appender.callCount() != 0 {
			t.Fatalf("ambiguous origin appended %d commands", appender.callCount())
		}
	})
}

func TestUserCommandIngress_OversizedRejectedWithoutAllocatingSeq(t *testing.T) {
	ingress, appender := newTestIngress(t)
	server := httptest.NewServer(newCommandMux(ingress))
	defer server.Close()

	// Build a command whose raw body is just over 1 MiB.
	// The text alone is at the limit; the JSON wrapper pushes the total over.
	padding := strings.Repeat("x", MaxUserCommandBytes)
	body := []byte(fmt.Sprintf(`{"type":"user_message","text":"%s","attachments":[]}`, padding))
	if len(body) <= MaxUserCommandBytes {
		t.Fatalf("test fixture not oversized: %d bytes", len(body))
	}

	resp := postWithSessionCookie(t, server.URL+"/direct-chat/commands", body, "018f47a2-9b3c-7def-8abc-0123456789ab")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
	if appender.callCount() != 0 {
		t.Fatalf("expected 0 append calls for oversized, got %d", appender.callCount())
	}
	assertRejectReason(t, resp.Body, RejectOversized)

	// A subsequent valid request must receive the first contiguous sequence.
	valid := []byte(`{"type":"user_message","text":"hi","attachments":[]}`)
	resp2 := postWithSessionCookie(t, server.URL+"/direct-chat/commands", valid, "018f47a2-9b3c-7def-8abc-0123456789ab")
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 after reject, got %d", resp2.StatusCode)
	}
	var env browserCommandReceipt
	if err := json.NewDecoder(resp2.Body).Decode(&env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.Seq != 1 {
		t.Fatalf("expected first valid seq to be 1 after rejection, got %d", env.Seq)
	}
	if appender.callCount() != 1 {
		t.Fatalf("expected 1 total append call, got %d", appender.callCount())
	}
}

func TestUserCommandIngress_NonEmptyAttachmentsRejected(t *testing.T) {
	ingress, appender := newTestIngress(t)
	server := httptest.NewServer(newCommandMux(ingress))
	defer server.Close()

	body := []byte(`{"type":"user_message","text":"hi","attachments":[{"name":"x"}]}`)
	resp := postWithSessionCookie(t, server.URL+"/direct-chat/commands", body, "018f47a2-9b3c-7def-8abc-0123456789ab")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
	if appender.callCount() != 0 {
		t.Fatalf("expected 0 append calls, got %d", appender.callCount())
	}
	assertRejectReason(t, resp.Body, RejectAttachmentsNotEmpty)

	valid := []byte(`{"type":"user_message","text":"hi","attachments":[]}`)
	resp2 := postWithSessionCookie(t, server.URL+"/direct-chat/commands", valid, "018f47a2-9b3c-7def-8abc-0123456789ab")
	defer resp2.Body.Close()
	var env browserCommandReceipt
	if err := json.NewDecoder(resp2.Body).Decode(&env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.Seq != 1 {
		t.Fatalf("expected first valid seq 1, got %d", env.Seq)
	}
}

func TestUserCommandIngress_MalformedAndUnknownRejected(t *testing.T) {
	ingress, appender := newTestIngress(t)
	server := httptest.NewServer(newCommandMux(ingress))
	defer server.Close()

	cases := []struct {
		name   string
		body   string
		reason RejectReason
	}{
		{
			name:   "invalid JSON",
			body:   `{"type":"user_message","text":"hi","attachments":[]`,
			reason: RejectSchemaViolation,
		},
		{
			name:   "missing text",
			body:   `{"type":"user_message","attachments":[]}`,
			reason: RejectSchemaViolation,
		},
		{
			name:   "browser target override",
			body:   `{"type":"user_message","text":"hi","attachments":[],"personality_agent_id":"018f47a2-9b3c-7def-9abc-0123456789ac"}`,
			reason: RejectSchemaViolation,
		},
		{
			name:   "browser provenance override",
			body:   `{"type":"user_message","text":"hi","attachments":[],"provenance":{"version":1}}`,
			reason: RejectSchemaViolation,
		},
		{
			name:   "text not string",
			body:   `{"type":"user_message","text":123,"attachments":[]}`,
			reason: RejectSchemaViolation,
		},
		{
			name:   "unknown command type",
			body:   `{"type":"abort","text":"","attachments":[]}`,
			reason: RejectUnknownCommand,
		},
		{
			name:   "extra field",
			body:   `{"type":"user_message","text":"hi","attachments":[],"extra":1}`,
			reason: RejectSchemaViolation,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := postWithSessionCookie(t, server.URL+"/direct-chat/commands", []byte(tc.body), "018f47a2-9b3c-7def-8abc-0123456789ab")
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", resp.StatusCode)
			}
			if appender.callCount() != 0 {
				t.Fatalf("expected 0 append calls, got %d", appender.callCount())
			}
			assertRejectReason(t, resp.Body, tc.reason)
		})
	}
}

func TestUserCommandIngressRequiresNonemptyIdempotencyKey(t *testing.T) {
	ingress, appender := newTestIngress(t)
	req := httptest.NewRequest(http.MethodPost, "/direct-chat/commands", strings.NewReader(`{"type":"user_message","text":"hi","attachments":[]}`))
	req.Header.Set("Origin", testBrowserOrigin)
	req.AddCookie(&http.Cookie{Name: BrowserSessionCookie, Value: signTestIngressSession("018f47a2-9b3c-7def-8abc-0123456789ab")})
	recorder := httptest.NewRecorder()
	ingress.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusBadRequest || appender.callCount() != 0 {
		t.Fatalf("missing idempotency key: status=%d appends=%d", recorder.Code, appender.callCount())
	}
	assertRejectReason(t, recorder.Body, RejectSchemaViolation)
}

func TestUserCommandIngress_InvalidUTF8RejectedWithoutAllocatingSeq(t *testing.T) {
	ingress, appender := newTestIngress(t)
	server := httptest.NewServer(newCommandMux(ingress))
	defer server.Close()

	body := append(
		[]byte(`{"type":"user_message","text":"`),
		0xff,
	)
	body = append(body, []byte(`","attachments":[]}`)...)
	resp := postWithSessionCookie(t, server.URL+"/direct-chat/commands", body, "018f47a2-9b3c-7def-8abc-0123456789ab")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
	if appender.callCount() != 0 {
		t.Fatalf("expected 0 append calls, got %d", appender.callCount())
	}
	assertRejectReason(t, resp.Body, RejectSchemaViolation)
}

func TestUserCommandIngress_BodyReadFailureIsNotMisclassifiedAsOversized(t *testing.T) {
	ingress, appender := newTestIngress(t)
	req := httptest.NewRequest(
		http.MethodPost,
		"/direct-chat/commands",
		nil,
	)
	req.SetPathValue("personality_agent_id", "018f47a2-9b3c-7def-8abc-0123456789ab")
	req.Header.Set("Origin", testBrowserOrigin)
	req.AddCookie(&http.Cookie{Name: BrowserSessionCookie, Value: signTestIngressSession("018f47a2-9b3c-7def-8abc-0123456789ab")})
	req.Body = errorReadCloser{}
	recorder := httptest.NewRecorder()

	ingress.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recorder.Code)
	}
	if appender.callCount() != 0 {
		t.Fatalf("expected 0 append calls, got %d", appender.callCount())
	}
	assertRejectReason(t, recorder.Body, RejectSchemaViolation)
}

func TestUserCommandIngress_MultipleValidRequestsAreContiguous(t *testing.T) {
	ingress, appender := newTestIngress(t)
	server := httptest.NewServer(newCommandMux(ingress))
	defer server.Close()

	for i := 1; i <= 3; i++ {
		body := []byte(fmt.Sprintf(`{"type":"user_message","text":"msg %d","attachments":[]}`, i))
		resp := postWithSessionCookie(t, server.URL+"/direct-chat/commands", body, "018f47a2-9b3c-7def-8abc-0123456789ab")
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("expected 201 for request %d, got %d", i, resp.StatusCode)
		}
		var env browserCommandReceipt
		if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
			t.Fatalf("decode %d: %v", i, err)
		}
		resp.Body.Close()
		if env.Seq != uint64(i) {
			t.Fatalf("expected seq %d, got %d", i, env.Seq)
		}
	}
	if appender.callCount() != 3 {
		t.Fatalf("expected 3 append calls, got %d", appender.callCount())
	}
}

func TestUserCommandIngress_RequiresSessionCookie(t *testing.T) {
	appender := &fakeCommandAppender{}
	ingress, err := NewUserCommandIngress(appender, nil)
	if err != nil {
		t.Fatal(err)
	}
	ingress.AllowedOrigins = []string{testBrowserOrigin}
	mux := http.NewServeMux()
	mux.Handle("POST /direct-chat/commands", ingress)
	server := httptest.NewServer(mux)
	defer server.Close()

	body := []byte(`{"type":"user_message","text":"hi","attachments":[]}`)
	resp := postWithAuthorization(t, server.URL+"/direct-chat/commands", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing session cookie, got %d", resp.StatusCode)
	}
	if appender.callCount() != 0 {
		t.Fatalf("expected 0 append calls, got %d", appender.callCount())
	}
}

func TestUserCommandIngress_AgentBearerTokenCannotInjectUserCommand(t *testing.T) {
	appender := &fakeCommandAppender{}
	verifier := &fakeSessionVerifier{personalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab"}
	ingress, err := NewUserCommandIngress(appender, verifier)
	if err != nil {
		t.Fatal(err)
	}
	ingress.AllowedOrigins = []string{testBrowserOrigin}
	mux := http.NewServeMux()
	mux.Handle("POST /direct-chat/commands", ingress)
	server := httptest.NewServer(mux)
	defer server.Close()

	body := []byte(`{"type":"user_message","text":"hi","attachments":[]}`)
	resp := postWithAuthorization(t, server.URL+"/direct-chat/commands", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for agent bearer token, got %d", resp.StatusCode)
	}
	if appender.callCount() != 0 {
		t.Fatalf("expected 0 append calls, got %d", appender.callCount())
	}
}

func TestUserCommandIngress_TargetComesOnlyFromVerifiedSession(t *testing.T) {
	appender := &fakeCommandAppender{}
	verifier := &fakeSessionVerifier{personalityAgentID: "018f47a2-9b3c-7def-9abc-0123456789ac"}
	ingress, err := NewUserCommandIngress(appender, verifier)
	if err != nil {
		t.Fatal(err)
	}
	ingress.AllowedOrigins = []string{testBrowserOrigin}
	mux := http.NewServeMux()
	mux.Handle("POST /direct-chat/commands", ingress)
	server := httptest.NewServer(mux)
	defer server.Close()

	body := []byte(`{"type":"user_message","text":"hi","attachments":[]}`)
	resp := postWithSessionCookie(t, server.URL+"/direct-chat/commands", body, "018f47a2-9b3c-7def-8abc-0123456789ab")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected targetless route to use the verified session target, got %d", resp.StatusCode)
	}
	if appender.callCount() != 1 || appender.calls[0].PersonalityAgentID != "018f47a2-9b3c-7def-9abc-0123456789ac" {
		t.Fatalf("expected one append to the signed target, got %+v", appender.calls)
	}

	verifier.setReject(true)
	resp2 := postWithSessionCookie(t, server.URL+"/direct-chat/commands", body, "018f47a2-9b3c-7def-8abc-0123456789ab")
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for rejected session, got %d", resp2.StatusCode)
	}
}

func TestUserCommandIngress_NilAppenderFailsClosed(t *testing.T) {
	if _, err := NewUserCommandIngress(nil, nil); err == nil {
		t.Fatal("expected nil appender to fail closed")
	}
}

func TestUserCommandIngress_TrailingDataRejectedWithoutAllocatingSeq(t *testing.T) {
	ingress, appender := newTestIngress(t)
	server := httptest.NewServer(newCommandMux(ingress))
	defer server.Close()

	body := []byte(`{"type":"user_message","text":"hi","attachments":[]} trailing`)
	resp := postWithSessionCookie(t, server.URL+"/direct-chat/commands", body, "018f47a2-9b3c-7def-8abc-0123456789ab")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
	if appender.callCount() != 0 {
		t.Fatalf("invalid body must not append, got %d calls", appender.callCount())
	}
	assertRejectReason(t, resp.Body, RejectSchemaViolation)
}

func TestUserCommandIngress_OversizedIdempotencyKeyRejected(t *testing.T) {
	ingress, appender := newTestIngress(t)
	server := httptest.NewServer(newCommandMux(ingress))
	defer server.Close()

	body := []byte(`{"type":"user_message","text":"hi","attachments":[]}`)
	req, err := http.NewRequest(http.MethodPost, server.URL+"/direct-chat/commands", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", testBrowserOrigin)
	req.AddCookie(&http.Cookie{Name: BrowserSessionCookie, Value: signTestIngressSession("018f47a2-9b3c-7def-8abc-0123456789ab")})
	req.Header.Set("Idempotency-Key", strings.Repeat("x", MaxIdempotencyKeyBytes+1))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
	if appender.callCount() != 0 {
		t.Fatalf("expected 0 append calls, got %d", appender.callCount())
	}
	assertRejectReason(t, resp.Body, RejectOversized)
}

func TestUserCommandIngress_RejectsNonContiguousPersistedSeq(t *testing.T) {
	dir := t.TempDir()
	conv := "018f47a2-9b3c-7def-8abc-012345678970"

	// A persisted jump must be rejected at startup: CatchUp assumes a
	// contiguous durable log rather than a sparse sorted sequence.
	seed := testLogRecord(maxJSONSafeInteger, "00000000-0000-4000-8000-000000000000", json.RawMessage(`{"type":"user_message","text":"seed","attachments":[]}`), conv)
	seedLine, _ := json.Marshal(seed)
	path := commandLogPath(dir, conv)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(seedLine, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := OpenCommandStore(dir); err == nil || !strings.Contains(err.Error(), "non-contiguous") {
		t.Fatalf("expected non-contiguous log rejection, got %v", err)
	}
}

func assertRejectReason(t *testing.T, r io.Reader, want RejectReason) {
	t.Helper()
	var got struct {
		Error        string `json:"error"`
		RejectReason string `json:"reject_reason"`
	}
	if err := json.NewDecoder(r).Decode(&got); err != nil {
		t.Fatalf("decode rejection: %v", err)
	}
	if got.RejectReason != string(want) {
		t.Fatalf("expected reject_reason %q, got %q", want, got.RejectReason)
	}
}
