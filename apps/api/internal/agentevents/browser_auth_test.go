package agentevents

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const browserAuthTestOrigin = "https://web.example"

type fakeFirebaseVerifier struct {
	identity FirebaseIdentity
	err      error
	calls    int
}

func (f *fakeFirebaseVerifier) VerifyIDToken(
	_ context.Context,
	_ string,
) (FirebaseIdentity, error) {
	f.calls++
	return f.identity, f.err
}

type fakeBindingResolver struct {
	claims UserSessionClaims
	err    error
	calls  int
}

func (f *fakeBindingResolver) ResolveIdentity(
	_ context.Context,
	_ FirebaseIdentity,
) (UserSessionClaims, error) {
	f.calls++
	return f.claims, f.err
}

func newTestBrowserAuthServer(
	t *testing.T,
	firebase *fakeFirebaseVerifier,
	bindings *fakeBindingResolver,
) (*BrowserAuthServer, *HMACUserSessionVerifier) {
	t.Helper()
	sessions, err := NewHMACUserSessionVerifier(testSessionSecret, "")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewBrowserAuthServer(
		firebase,
		bindings,
		sessions,
		[]string{browserAuthTestOrigin},
		true,
	)
	if err != nil {
		t.Fatal(err)
	}
	server.SessionTTL = 5 * time.Minute
	return server, sessions
}

func obtainCSRF(t *testing.T, server *BrowserAuthServer) (string, *http.Cookie) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/auth/csrf", nil)
	req.Header.Set("Origin", browserAuthTestOrigin)
	recorder := httptest.NewRecorder()
	server.serveCSRF(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("obtain CSRF: status %d body %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Token string `json:"csrf_token"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	cookies := recorder.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("expected one CSRF cookie, got %d", len(cookies))
	}
	if cookies[0].Name != BrowserCSRFCookie ||
		cookies[0].HttpOnly ||
		!cookies[0].Secure ||
		cookies[0].SameSite != http.SameSiteLaxMode ||
		cookies[0].Path != "/auth" ||
		cookies[0].Domain != "" {
		t.Fatalf("unexpected CSRF cookie: %+v", cookies[0])
	}
	return response.Token, cookies[0]
}

func TestBrowserAuthExchangesVerifiedIdentityForOpaqueSession(t *testing.T) {
	firebase := &fakeFirebaseVerifier{identity: FirebaseIdentity{UID: "firebase-user"}}
	bindings := &fakeBindingResolver{claims: UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}}
	server, sessions := newTestBrowserAuthServer(t, firebase, bindings)
	csrf, csrfCookie := obtainCSRF(t, server)

	req := httptest.NewRequest(
		http.MethodPost,
		"/auth/session",
		strings.NewReader(`{"id_token":"verified-by-fake"}`),
	)
	req.Header.Set("Origin", browserAuthTestOrigin)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-CSRF-Token", csrf)
	req.AddCookie(csrfCookie)
	recorder := httptest.NewRecorder()
	server.serveSessionExchange(recorder, req)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("exchange: status %d body %s", recorder.Code, recorder.Body.String())
	}

	cookies := recorder.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("expected one session cookie, got %d", len(cookies))
	}
	sessionCookie := cookies[0]
	if sessionCookie.Name != BrowserSessionCookie ||
		!sessionCookie.HttpOnly ||
		!sessionCookie.Secure ||
		sessionCookie.SameSite != http.SameSiteLaxMode ||
		sessionCookie.Path != "/" ||
		sessionCookie.Domain != "" {
		t.Fatalf("unexpected session cookie: %+v", sessionCookie)
	}
	claims, err := sessions.VerifySession(context.Background(), sessionCookie.Value)
	if err != nil {
		t.Fatalf("verify session cookie: %v", err)
	}
	if claims != bindings.claims {
		t.Fatalf("got %+v, want %+v", claims, bindings.claims)
	}

	statusReq := httptest.NewRequest(http.MethodGet, "/auth/session", nil)
	statusReq.Header.Set("Origin", browserAuthTestOrigin)
	statusReq.AddCookie(sessionCookie)
	statusRecorder := httptest.NewRecorder()
	server.serveSessionStatus(statusRecorder, statusReq)
	body := statusRecorder.Body.String()
	if statusRecorder.Code != http.StatusOK ||
		!strings.Contains(body, `"authenticated":true`) ||
		!strings.Contains(body, `"id":"user-1"`) {
		t.Fatalf("unexpected session status: %d %s", statusRecorder.Code, body)
	}
	if strings.Contains(body, "tenant-1") ||
		strings.Contains(body, bindings.claims.PersonalityAgentID) ||
		strings.Contains(body, "firebase-user") {
		t.Fatalf("session status leaked authorization binding: %s", body)
	}
}

func TestBrowserAuthRejectsOriginBeforeAuthAndBody(t *testing.T) {
	firebase := &fakeFirebaseVerifier{identity: FirebaseIdentity{UID: "firebase-user"}}
	bindings := &fakeBindingResolver{}
	server, _ := newTestBrowserAuthServer(t, firebase, bindings)

	req := httptest.NewRequest(http.MethodPost, "/auth/session", strings.NewReader("{"))
	req.Header.Set("Origin", "https://evil.example")
	recorder := httptest.NewRecorder()
	server.serveSessionExchange(recorder, req)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("got %d, want 403", recorder.Code)
	}
	if firebase.calls != 0 || bindings.calls != 0 {
		t.Fatal("origin rejection must precede token verification and binding")
	}
}

func TestBrowserAuthSafeGETAllowsMissingOriginButRejectsWrongOrigin(t *testing.T) {
	firebase := &fakeFirebaseVerifier{}
	bindings := &fakeBindingResolver{}
	server, _ := newTestBrowserAuthServer(t, firebase, bindings)

	missingOrigin := httptest.NewRequest(http.MethodGet, "/auth/csrf", nil)
	missingRecorder := httptest.NewRecorder()
	server.serveCSRF(missingRecorder, missingOrigin)
	if missingRecorder.Code != http.StatusOK {
		t.Fatalf("same-origin-style GET without Origin got %d", missingRecorder.Code)
	}

	wrongOrigin := httptest.NewRequest(http.MethodGet, "/auth/session", nil)
	wrongOrigin.Header.Set("Origin", "https://evil.example")
	wrongRecorder := httptest.NewRecorder()
	server.serveSessionStatus(wrongRecorder, wrongOrigin)
	if wrongRecorder.Code != http.StatusForbidden {
		t.Fatalf("cross-origin GET got %d, want 403", wrongRecorder.Code)
	}
}

func TestBrowserAuthRequiresUniqueMatchingCSRF(t *testing.T) {
	firebase := &fakeFirebaseVerifier{identity: FirebaseIdentity{UID: "firebase-user"}}
	bindings := &fakeBindingResolver{}
	server, _ := newTestBrowserAuthServer(t, firebase, bindings)
	csrf, cookie := obtainCSRF(t, server)

	cases := []struct {
		name   string
		header []string
		cookie []*http.Cookie
	}{
		{name: "missing"},
		{name: "mismatch", header: []string{csrf}, cookie: []*http.Cookie{{Name: BrowserCSRFCookie, Value: strings.Repeat("A", len(csrf))}}},
		{name: "duplicate header", header: []string{csrf, csrf}, cookie: []*http.Cookie{cookie}},
		{name: "duplicate cookie", header: []string{csrf}, cookie: []*http.Cookie{cookie, cookie}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/auth/session", strings.NewReader(`{"id_token":"token"}`))
			req.Header.Set("Origin", browserAuthTestOrigin)
			req.Header["X-CSRF-Token"] = tc.header
			for _, item := range tc.cookie {
				req.AddCookie(item)
			}
			recorder := httptest.NewRecorder()
			server.serveSessionExchange(recorder, req)
			if recorder.Code != http.StatusForbidden {
				t.Fatalf("got %d, want 403", recorder.Code)
			}
		})
	}
	if firebase.calls != 0 {
		t.Fatal("CSRF rejection must precede Firebase verification")
	}
}

func TestBrowserAuthFailsClosedForInvalidTokenAndUnboundIdentity(t *testing.T) {
	validClaims := UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}
	cases := []struct {
		name        string
		firebaseErr error
		bindingErr  error
		wantStatus  int
	}{
		{name: "invalid Firebase token", firebaseErr: errors.New("invalid"), wantStatus: http.StatusUnauthorized},
		{name: "unbound Firebase identity", bindingErr: errors.New("not bound"), wantStatus: http.StatusForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			firebase := &fakeFirebaseVerifier{
				identity: FirebaseIdentity{UID: "firebase-user"},
				err:      tc.firebaseErr,
			}
			bindings := &fakeBindingResolver{claims: validClaims, err: tc.bindingErr}
			server, _ := newTestBrowserAuthServer(t, firebase, bindings)
			csrf, cookie := obtainCSRF(t, server)
			req := httptest.NewRequest(http.MethodPost, "/auth/session", strings.NewReader(`{"id_token":"token"}`))
			req.Header.Set("Origin", browserAuthTestOrigin)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-CSRF-Token", csrf)
			req.AddCookie(cookie)
			recorder := httptest.NewRecorder()
			server.serveSessionExchange(recorder, req)
			if recorder.Code != tc.wantStatus {
				t.Fatalf("got %d, want %d", recorder.Code, tc.wantStatus)
			}
			if strings.Contains(recorder.Body.String(), "firebase-user") {
				t.Fatal("error response leaked external identity")
			}
		})
	}
}

func TestBrowserAuthRejectsMalformedOrOversizedExchangeBeforeVerification(t *testing.T) {
	firebase := &fakeFirebaseVerifier{identity: FirebaseIdentity{UID: "firebase-user"}}
	bindings := &fakeBindingResolver{}
	server, _ := newTestBrowserAuthServer(t, firebase, bindings)

	cases := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "duplicate ID token",
			body:       `{"id_token":"one","id_token":"two"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unknown field",
			body:       `{"id_token":"one","target":"client-authored"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "oversized body",
			body:       `{"id_token":"` + strings.Repeat("x", maxAuthRequestBytes) + `"}`,
			wantStatus: http.StatusRequestEntityTooLarge,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			csrf, cookie := obtainCSRF(t, server)
			req := httptest.NewRequest(http.MethodPost, "/auth/session", strings.NewReader(tc.body))
			req.Header.Set("Origin", browserAuthTestOrigin)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-CSRF-Token", csrf)
			req.AddCookie(cookie)
			recorder := httptest.NewRecorder()
			server.serveSessionExchange(recorder, req)
			if recorder.Code != tc.wantStatus {
				t.Fatalf("got %d, want %d", recorder.Code, tc.wantStatus)
			}
		})
	}
	if firebase.calls != 0 || bindings.calls != 0 {
		t.Fatal("malformed exchange must not reach verification or identity binding")
	}
}

func TestBrowserAuthLogoutClearsSessionAndCSRF(t *testing.T) {
	firebase := &fakeFirebaseVerifier{}
	bindings := &fakeBindingResolver{}
	server, _ := newTestBrowserAuthServer(t, firebase, bindings)
	csrf, cookie := obtainCSRF(t, server)

	req := httptest.NewRequest(http.MethodPost, "/auth/logout", nil)
	req.Header.Set("Origin", browserAuthTestOrigin)
	req.Header.Set("X-CSRF-Token", csrf)
	req.AddCookie(cookie)
	recorder := httptest.NewRecorder()
	server.serveLogout(recorder, req)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("got %d, want 204", recorder.Code)
	}
	cookies := recorder.Result().Cookies()
	if len(cookies) != 2 {
		t.Fatalf("expected two clearing cookies, got %d", len(cookies))
	}
	for _, cleared := range cookies {
		if cleared.MaxAge >= 0 {
			t.Fatalf("cookie was not cleared: %+v", cleared)
		}
	}
}

func TestStaticIdentityBindingResolverAllowsOnlyConfiguredUID(t *testing.T) {
	claims := UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}
	resolver, err := NewStaticIdentityBindingResolver("allowed-uid", claims)
	if err != nil {
		t.Fatal(err)
	}
	got, err := resolver.ResolveIdentity(context.Background(), FirebaseIdentity{UID: "allowed-uid"})
	if err != nil || got != claims {
		t.Fatalf("resolve allowed UID: %+v, %v", got, err)
	}
	if _, err := resolver.ResolveIdentity(context.Background(), FirebaseIdentity{UID: "other-uid"}); err == nil {
		t.Fatal("expected all unconfigured UIDs to be denied")
	}
}
