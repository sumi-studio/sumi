package agentevents

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

const browserAuthTestOrigin = "https://web.example"

type fakeFirebaseVerifier struct {
	identity FirebaseIdentity
	err      error
	calls    int
}

type barrierFirebaseVerifier struct {
	mu    sync.Mutex
	count int
	ready chan struct{}
}

func (f *barrierFirebaseVerifier) VerifyIDToken(
	ctx context.Context,
	_ string,
) (FirebaseIdentity, error) {
	f.mu.Lock()
	f.count++
	if f.count == 2 {
		close(f.ready)
	}
	f.mu.Unlock()
	select {
	case <-ctx.Done():
		return FirebaseIdentity{}, ctx.Err()
	case <-f.ready:
		return FirebaseIdentity{UID: "firebase-user"}, nil
	case <-time.After(2 * time.Second):
		return FirebaseIdentity{}, errors.New("timed out waiting for concurrent Firebase verification")
	}
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

type fakeBrowserConnectionCloser struct {
	sessionIDs []string
}

func (f *fakeBrowserConnectionCloser) CloseBrowserSession(sessionID string) {
	f.sessionIDs = append(f.sessionIDs, sessionID)
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
	sessions, err := NewHMACUserSessionVerifier(testSessionSecret, "", newTestBrowserSessionRevocationStore())
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
	if claims.TenantID != bindings.claims.TenantID ||
		claims.UserID != bindings.claims.UserID ||
		claims.PersonalityAgentID != bindings.claims.PersonalityAgentID {
		t.Fatalf("got %+v, want %+v", claims, bindings.claims)
	}

	statusReq := httptest.NewRequest(http.MethodGet, "/auth/session", nil)
	statusReq.Header.Set("Origin", browserAuthTestOrigin)
	statusReq.AddCookie(sessionCookie)
	statusRecorder := httptest.NewRecorder()
	server.serveSessionStatus(statusRecorder, statusReq)
	body := statusRecorder.Body.String()
	if statusRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected session status: %d %s", statusRecorder.Code, body)
	}
	var status struct {
		Authenticated      bool   `json:"authenticated"`
		AuthorityBindingID string `json:"authority_binding_id"`
		User               struct {
			ID string `json:"id"`
		} `json:"user"`
	}
	if err := json.Unmarshal(statusRecorder.Body.Bytes(), &status); err != nil {
		t.Fatalf("decode session status: %v", err)
	}
	if !status.Authenticated ||
		status.User.ID != "user-1" ||
		status.AuthorityBindingID != claims.authorityBindingID ||
		!validBrowserAuthorityBindingID(status.AuthorityBindingID) {
		t.Fatalf("unexpected session status: %+v", status)
	}
	if strings.Contains(body, "tenant-1") ||
		strings.Contains(body, bindings.claims.PersonalityAgentID) ||
		strings.Contains(body, "firebase-user") ||
		strings.Contains(body, claims.sessionID) {
		t.Fatalf("session status leaked authorization binding: %s", body)
	}

	repeatedStatusReq := httptest.NewRequest(http.MethodGet, "/auth/session", nil)
	repeatedStatusReq.Header.Set("Origin", browserAuthTestOrigin)
	repeatedStatusReq.AddCookie(sessionCookie)
	repeatedStatusRecorder := httptest.NewRecorder()
	server.serveSessionStatus(repeatedStatusRecorder, repeatedStatusReq)
	var repeatedStatus struct {
		AuthorityBindingID string `json:"authority_binding_id"`
	}
	if err := json.Unmarshal(repeatedStatusRecorder.Body.Bytes(), &repeatedStatus); err != nil {
		t.Fatalf("decode repeated session status: %v", err)
	}
	if repeatedStatusRecorder.Code != http.StatusOK ||
		repeatedStatus.AuthorityBindingID != status.AuthorityBindingID {
		t.Fatalf(
			"same session status changed authority binding: %d %+v",
			repeatedStatusRecorder.Code,
			repeatedStatus,
		)
	}
}

func TestBrowserAuthExchangeReplacesPreFenceCookieWithV2Session(t *testing.T) {
	firebase := &fakeFirebaseVerifier{
		identity: FirebaseIdentity{UID: "firebase-user"},
	}
	bindings := &fakeBindingResolver{claims: UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}}
	server, sessions := newTestBrowserAuthServer(t, firebase, bindings)
	now := time.Now().UTC().Truncate(time.Second)
	preFence := signPreFenceTestSession(
		t,
		testSessionSecret,
		testSessionClaims{
			TenantID:           bindings.claims.TenantID,
			UserID:             bindings.claims.UserID,
			PersonalityAgentID: bindings.claims.PersonalityAgentID,
			Iat:                now.Add(-time.Minute).Unix(),
			Exp:                now.Add(time.Minute).Unix(),
			Aud:                defaultBrowserAudience,
		},
	)
	if _, err := sessions.VerifySession(
		context.Background(),
		preFence,
	); err == nil || !strings.Contains(err.Error(), "invalid browser session signature") {
		t.Fatalf("pre-fence cookie verification = %v, want signature rejection", err)
	}

	csrf, csrfCookie := obtainCSRF(t, server)
	request := httptest.NewRequest(
		http.MethodPost,
		"/auth/session",
		strings.NewReader(`{"id_token":"verified-by-fake"}`),
	)
	request.Header.Set("Origin", browserAuthTestOrigin)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-CSRF-Token", csrf)
	request.AddCookie(csrfCookie)
	request.AddCookie(&http.Cookie{
		Name:  BrowserSessionCookie,
		Value: preFence,
	})
	recorder := httptest.NewRecorder()
	server.serveSessionExchange(recorder, request)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf(
			"exchange pre-fence cookie: status %d body %s",
			recorder.Code,
			recorder.Body.String(),
		)
	}
	cookies := recorder.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Name != BrowserSessionCookie {
		t.Fatalf("replacement cookies = %+v, want one browser session", cookies)
	}
	if cookies[0].Value == preFence {
		t.Fatal("Firebase exchange returned the pre-fence credential")
	}
	replacement, err := sessions.VerifySession(
		context.Background(),
		cookies[0].Value,
	)
	if err != nil {
		t.Fatalf("Firebase exchange did not issue a valid v2 cookie: %v", err)
	}
	if replacement.TenantID != bindings.claims.TenantID ||
		replacement.UserID != bindings.claims.UserID ||
		replacement.PersonalityAgentID != bindings.claims.PersonalityAgentID {
		t.Fatalf("replacement claims = %+v, want %+v", replacement, bindings.claims)
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

func TestBrowserAuthLogoutRevokesSessionAndClosesOnlyItsConnections(t *testing.T) {
	firebase := &fakeFirebaseVerifier{}
	bindings := &fakeBindingResolver{}
	server, sessions := newTestBrowserAuthServer(t, firebase, bindings)
	closer := &fakeBrowserConnectionCloser{}
	server.Connections = closer
	session, err := sessions.IssueSession(context.Background(), UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := sessions.VerifySession(context.Background(), session)
	if err != nil {
		t.Fatal(err)
	}
	csrf, csrfCookie := obtainCSRF(t, server)

	req := httptest.NewRequest(http.MethodPost, "/auth/logout", nil)
	req.Header.Set("Origin", browserAuthTestOrigin)
	req.Header.Set("X-CSRF-Token", csrf)
	req.AddCookie(csrfCookie)
	req.AddCookie(&http.Cookie{Name: BrowserSessionCookie, Value: session})
	recorder := httptest.NewRecorder()
	server.serveLogout(recorder, req)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("got %d, want 204", recorder.Code)
	}
	if _, err := sessions.VerifySession(context.Background(), session); err == nil {
		t.Fatal("logout left the session valid")
	}
	if len(closer.sessionIDs) != 1 || closer.sessionIDs[0] != claims.sessionID {
		t.Fatalf("closed sessions %v, want %q", closer.sessionIDs, claims.sessionID)
	}
}

func TestBrowserAuthLogoutRetainsValidCookieWhenDurableRevocationIsUnavailable(t *testing.T) {
	store, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	gateway, err := OpenDurableGateway(t.TempDir(), store)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := NewHMACUserSessionVerifier(
		testSessionSecret,
		"",
		gateway,
	)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewBrowserAuthServer(
		&fakeFirebaseVerifier{},
		&fakeBindingResolver{},
		sessions,
		[]string{browserAuthTestOrigin},
		true,
	)
	if err != nil {
		t.Fatal(err)
	}
	closer := &fakeBrowserConnectionCloser{}
	server.Connections = closer
	session, err := sessions.IssueSession(
		context.Background(),
		UserSessionClaims{
			TenantID:           "tenant-1",
			UserID:             "user-1",
			PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		},
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	logoutSession := func() *httptest.ResponseRecorder {
		t.Helper()
		csrf, csrfCookie := obtainCSRF(t, server)
		logout := httptest.NewRequest(http.MethodPost, "/auth/logout", nil)
		logout.Header.Set("Origin", browserAuthTestOrigin)
		logout.Header.Set("X-CSRF-Token", csrf)
		logout.AddCookie(csrfCookie)
		logout.AddCookie(&http.Cookie{
			Name:  BrowserSessionCookie,
			Value: session,
		})
		recorder := httptest.NewRecorder()
		server.serveLogout(recorder, logout)
		return recorder
	}
	assertRetained := func(label string, recorder *httptest.ResponseRecorder) {
		t.Helper()
		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s logout status = %d, want 503", label, recorder.Code)
		}
		if cookies := recorder.Result().Cookies(); len(cookies) != 0 {
			t.Fatalf("%s logout mutated browser credentials: %+v", label, cookies)
		}
		if len(closer.sessionIDs) != 0 {
			t.Fatalf("%s logout closed sessions: %v", label, closer.sessionIDs)
		}
	}
	if err := os.WriteFile(
		gateway.browserSessionRevocationPath(),
		[]byte(`{"version":1,"entries":`),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	assertRetained("corrupt-state", logoutSession())
	if err := os.Remove(gateway.browserSessionRevocationPath()); err != nil {
		t.Fatal(err)
	}
	if _, err := sessions.VerifySession(context.Background(), session); err != nil {
		t.Fatalf("valid signed cookie did not recover with durable store: %v", err)
	}

	gateway.writeAtomic = func(string, []byte, os.FileMode) error {
		return errors.New("injected revocation write failure")
	}
	assertRetained("write-failed", logoutSession())
	gateway.writeAtomic = writeFileAtomic
	if _, err := sessions.VerifySession(context.Background(), session); err != nil {
		t.Fatalf("failed durable update invalidated signed cookie: %v", err)
	}
}

func TestBrowserAuthReplacementRetiresOldSessionBeforePublishingNewAuthority(t *testing.T) {
	firebase := &fakeFirebaseVerifier{identity: FirebaseIdentity{UID: "firebase-user"}}
	bindings := &fakeBindingResolver{claims: UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}}
	server, sessions := newTestBrowserAuthServer(t, firebase, bindings)
	closer := &fakeBrowserConnectionCloser{}
	server.Connections = closer
	first, err := sessions.IssueSession(context.Background(), bindings.claims, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	firstClaims, err := sessions.VerifySession(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	csrf, csrfCookie := obtainCSRF(t, server)
	exchange := httptest.NewRequest(http.MethodPost, "/auth/session", strings.NewReader(`{"id_token":"replacement"}`))
	exchange.Header.Set("Origin", browserAuthTestOrigin)
	exchange.Header.Set("Content-Type", "application/json")
	exchange.Header.Set("X-CSRF-Token", csrf)
	exchange.AddCookie(csrfCookie)
	exchange.AddCookie(&http.Cookie{Name: BrowserSessionCookie, Value: first})
	exchangeRecorder := httptest.NewRecorder()
	server.serveSessionExchange(exchangeRecorder, exchange)
	if exchangeRecorder.Code != http.StatusNoContent {
		t.Fatalf("replacement exchange: %d %s", exchangeRecorder.Code, exchangeRecorder.Body.String())
	}
	if _, err := sessions.VerifySession(context.Background(), first); err == nil {
		t.Fatal("replacement left the old session authoritative")
	}
	if len(closer.sessionIDs) != 1 || closer.sessionIDs[0] != firstClaims.sessionID {
		t.Fatalf("replacement closed sessions %v, want old SID %q", closer.sessionIDs, firstClaims.sessionID)
	}
	replacementCookies := exchangeRecorder.Result().Cookies()
	if len(replacementCookies) != 1 {
		t.Fatalf("replacement cookies = %d, want 1", len(replacementCookies))
	}
	second := replacementCookies[0].Value
	secondClaims, err := sessions.VerifySession(context.Background(), second)
	if err != nil {
		t.Fatalf("replacement session is invalid: %v", err)
	}
	if secondClaims.authorityBindingID != firstClaims.authorityBindingID {
		t.Fatalf(
			"same authority binding changed across replacement: %q != %q",
			secondClaims.authorityBindingID,
			firstClaims.authorityBindingID,
		)
	}
	replayedExchange := httptest.NewRequest(http.MethodPost, "/auth/session", strings.NewReader(`{"id_token":"concurrent-replay"}`))
	replayedExchange.Header.Set("Origin", browserAuthTestOrigin)
	replayedExchange.Header.Set("Content-Type", "application/json")
	replayedExchange.Header.Set("X-CSRF-Token", csrf)
	replayedExchange.AddCookie(csrfCookie)
	replayedExchange.AddCookie(&http.Cookie{Name: BrowserSessionCookie, Value: first})
	replayedRecorder := httptest.NewRecorder()
	server.serveSessionExchange(replayedRecorder, replayedExchange)
	if replayedRecorder.Code != http.StatusServiceUnavailable || len(replayedRecorder.Result().Cookies()) != 0 {
		t.Fatalf("retired replacement credential minted another session: %d %+v", replayedRecorder.Code, replayedRecorder.Result().Cookies())
	}
	if _, err := sessions.VerifySession(context.Background(), second); err != nil {
		t.Fatalf("replayed old credential invalidated replacement: %v", err)
	}

	logout := httptest.NewRequest(http.MethodPost, "/auth/logout", nil)
	logout.Header.Set("Origin", browserAuthTestOrigin)
	logout.Header.Set("X-CSRF-Token", csrf)
	logout.AddCookie(csrfCookie)
	logout.AddCookie(&http.Cookie{Name: BrowserSessionCookie, Value: second})
	logoutRecorder := httptest.NewRecorder()
	server.serveLogout(logoutRecorder, logout)
	if logoutRecorder.Code != http.StatusNoContent {
		t.Fatalf("replacement logout: %d %s", logoutRecorder.Code, logoutRecorder.Body.String())
	}
	if _, err := sessions.VerifySession(context.Background(), second); err == nil {
		t.Fatal("logout left replacement session authoritative")
	}
	if len(closer.sessionIDs) != 2 || closer.sessionIDs[1] != secondClaims.sessionID {
		t.Fatalf("replacement lifecycle closed sessions %v", closer.sessionIDs)
	}
}

func TestBrowserAuthReplacementIsSingleUseAcrossGateways(t *testing.T) {
	commandDir := t.TempDir()
	runtimeDir := t.TempDir()
	store, err := OpenCommandStore(commandDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	firstGateway, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	secondGateway, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	firstSessions, err := NewHMACUserSessionVerifier(
		testSessionSecret,
		"",
		firstGateway,
	)
	if err != nil {
		t.Fatal(err)
	}
	secondSessions, err := NewHMACUserSessionVerifier(
		testSessionSecret,
		"",
		secondGateway,
	)
	if err != nil {
		t.Fatal(err)
	}
	claims := UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}
	oldSession, err := firstSessions.IssueSession(
		context.Background(),
		claims,
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	firebase := &barrierFirebaseVerifier{ready: make(chan struct{})}
	servers := make([]*BrowserAuthServer, 2)
	for index, sessions := range []browserSessionManager{
		firstSessions,
		secondSessions,
	} {
		servers[index], err = NewBrowserAuthServer(
			firebase,
			&fakeBindingResolver{claims: claims},
			sessions,
			[]string{browserAuthTestOrigin},
			true,
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	recorders := make([]*httptest.ResponseRecorder, 2)
	requests := make([]*http.Request, 2)
	for index, server := range servers {
		csrf, csrfCookie := obtainCSRF(t, server)
		request := httptest.NewRequest(
			http.MethodPost,
			"/auth/session",
			strings.NewReader(`{"id_token":"replacement"}`),
		)
		request.Header.Set("Origin", browserAuthTestOrigin)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-CSRF-Token", csrf)
		request.AddCookie(csrfCookie)
		request.AddCookie(&http.Cookie{
			Name:  BrowserSessionCookie,
			Value: oldSession,
		})
		requests[index] = request
		recorders[index] = httptest.NewRecorder()
	}

	var wait sync.WaitGroup
	for index := range servers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			servers[index].serveSessionExchange(recorders[index], requests[index])
		}()
	}
	wait.Wait()

	successes := 0
	failures := 0
	var replacement string
	for _, recorder := range recorders {
		switch recorder.Code {
		case http.StatusNoContent:
			successes++
			cookies := recorder.Result().Cookies()
			if len(cookies) != 1 {
				t.Fatalf("successful replacement cookies = %+v", cookies)
			}
			replacement = cookies[0].Value
		case http.StatusServiceUnavailable:
			failures++
			if len(recorder.Result().Cookies()) != 0 {
				t.Fatal("replayed replacement published a cookie")
			}
		default:
			t.Fatalf("replacement status = %d", recorder.Code)
		}
	}
	if successes != 1 || failures != 1 {
		t.Fatalf("replacement outcomes: successes=%d failures=%d", successes, failures)
	}
	if _, err := secondSessions.VerifySession(
		context.Background(),
		replacement,
	); err != nil {
		t.Fatalf("winning replacement is invalid: %v", err)
	}
	if _, err := firstSessions.VerifySession(
		context.Background(),
		oldSession,
	); !errors.Is(err, errBrowserSessionRevoked) {
		t.Fatalf("old replacement credential = %v, want revoked", err)
	}
	if _, err := firstSessions.RevokeSession(
		context.Background(),
		oldSession,
	); err != nil {
		t.Fatalf("logout-style replay must remain idempotent: %v", err)
	}
}

func TestBrowserAuthReplacementFailsClosedWhenOldSessionCannotBeRetired(t *testing.T) {
	firebase := &fakeFirebaseVerifier{identity: FirebaseIdentity{UID: "firebase-user"}}
	bindings := &fakeBindingResolver{claims: UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}}
	server, sessions := newTestBrowserAuthServer(t, firebase, bindings)
	sessions.revocations.(*testBrowserSessionRevocationStore).max = 0
	first, err := sessions.IssueSession(context.Background(), bindings.claims, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	csrf, csrfCookie := obtainCSRF(t, server)
	exchange := httptest.NewRequest(http.MethodPost, "/auth/session", strings.NewReader(`{"id_token":"replacement"}`))
	exchange.Header.Set("Origin", browserAuthTestOrigin)
	exchange.Header.Set("Content-Type", "application/json")
	exchange.Header.Set("X-CSRF-Token", csrf)
	exchange.AddCookie(csrfCookie)
	exchange.AddCookie(&http.Cookie{Name: BrowserSessionCookie, Value: first})
	recorder := httptest.NewRecorder()
	server.serveSessionExchange(recorder, exchange)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", recorder.Code)
	}
	if len(recorder.Result().Cookies()) != 0 {
		t.Fatal("replacement published a cookie after retirement failed")
	}
	if _, err := sessions.VerifySession(context.Background(), first); err != nil {
		t.Fatalf("failed retirement unexpectedly invalidated old session: %v", err)
	}
}

func TestBrowserAuthDuplicateCookieLogoutRevokesEveryVerifiableSession(t *testing.T) {
	server, sessions := newTestBrowserAuthServer(t, &fakeFirebaseVerifier{}, &fakeBindingResolver{})
	closer := &fakeBrowserConnectionCloser{}
	server.Connections = closer
	claims := UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}
	first, err := sessions.IssueSession(context.Background(), claims, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	second, err := sessions.IssueSession(context.Background(), claims, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	csrf, csrfCookie := obtainCSRF(t, server)
	logout := httptest.NewRequest(http.MethodPost, "/auth/logout", nil)
	logout.Header.Set("Origin", browserAuthTestOrigin)
	logout.Header.Set("X-CSRF-Token", csrf)
	logout.AddCookie(csrfCookie)
	logout.AddCookie(&http.Cookie{Name: BrowserSessionCookie, Value: "malformed"})
	logout.AddCookie(&http.Cookie{Name: BrowserSessionCookie, Value: first})
	logout.AddCookie(&http.Cookie{Name: BrowserSessionCookie, Value: second})
	recorder := httptest.NewRecorder()
	server.serveLogout(recorder, logout)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("duplicate-cookie logout: %d %s", recorder.Code, recorder.Body.String())
	}
	for _, signed := range []string{first, second} {
		if _, err := sessions.VerifySession(context.Background(), signed); err == nil {
			t.Fatal("duplicate-cookie logout left a verifiable session authoritative")
		}
	}
	if len(closer.sessionIDs) != 2 {
		t.Fatalf("closed sessions = %v, want two", closer.sessionIDs)
	}
	cleared := recorder.Result().Cookies()
	if len(cleared) != 2 || cleared[0].Name != BrowserSessionCookie || cleared[0].MaxAge >= 0 {
		t.Fatalf("logout did not clear authoritative cookie: %+v", cleared)
	}
}

func TestBrowserAuthRejectsDuplicateSessionCookies(t *testing.T) {
	firebase := &fakeFirebaseVerifier{identity: FirebaseIdentity{UID: "firebase-user"}}
	bindings := &fakeBindingResolver{}
	server, _ := newTestBrowserAuthServer(t, firebase, bindings)
	csrf, csrfCookie := obtainCSRF(t, server)
	duplicate := &http.Cookie{Name: BrowserSessionCookie, Value: "one"}

	for _, tc := range []struct {
		name   string
		method string
		path   string
		call   func(http.ResponseWriter, *http.Request)
	}{
		{name: "exchange", method: http.MethodPost, path: "/auth/session", call: server.serveSessionExchange},
		{name: "status", method: http.MethodGet, path: "/auth/session", call: server.serveSessionStatus},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(`{"id_token":"token"}`))
			req.Header.Set("Origin", browserAuthTestOrigin)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-CSRF-Token", csrf)
			req.AddCookie(csrfCookie)
			req.AddCookie(duplicate)
			req.AddCookie(duplicate)
			recorder := httptest.NewRecorder()
			tc.call(recorder, req)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("got %d, want 400", recorder.Code)
			}
		})
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
	if _, err := resolver.ResolveIdentity(context.Background(), FirebaseIdentity{UID: "allowed-uid", TenantID: "tenant-auth"}); err == nil {
		t.Fatal("tenant token must be denied without an explicit Firebase tenant binding")
	}
	tenantResolver, err := NewStaticIdentityBindingResolverForTenant("allowed-uid", "tenant-auth", claims)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tenantResolver.ResolveIdentity(context.Background(), FirebaseIdentity{UID: "allowed-uid"}); err == nil {
		t.Fatal("non-tenant token must not satisfy an explicit Firebase tenant binding")
	}
	if _, err := tenantResolver.ResolveIdentity(context.Background(), FirebaseIdentity{UID: "allowed-uid", TenantID: "tenant-auth"}); err != nil {
		t.Fatalf("explicit tenant binding rejected: %v", err)
	}
}
