package agentevents

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeEnrollmentInvites struct {
	issued    int
	inspected int
	issuer    string
}

func (f *fakeEnrollmentInvites) IssueEnrollmentInvite(_ context.Context, issuer, email string, _ time.Duration) (EnrollmentInvitation, string, error) {
	f.issued++
	f.issuer = issuer
	return EnrollmentInvitation{ID: "test-invite", Email: email}, "test-token", nil
}
func (f *fakeEnrollmentInvites) ListEnrollmentInvites(context.Context) ([]EnrollmentInvitation, error) {
	return []EnrollmentInvitation{{ID: "test-invite"}}, nil
}
func (f *fakeEnrollmentInvites) RevokeEnrollmentInvite(context.Context, string) error { return nil }
func (f *fakeEnrollmentInvites) InspectEnrollmentInvite(context.Context, string) (EnrollmentInvitation, error) {
	f.inspected++
	return EnrollmentInvitation{ExpiresAt: time.Now().Add(time.Hour)}, nil
}
func TestEnrollmentAdminAndCSRFBoundary(t *testing.T) {
	s, sessions := newTestBrowserAuthServer(t, &fakeFirebaseVerifier{}, &fakeBindingResolver{})
	store := &fakeEnrollmentInvites{}
	s.EnrollmentInvitations = store
	id := "0198f0f4-9b72-7000-8000-000000000010"
	agent := "0198f0f4-9b72-7000-8000-000000000011"
	cookie, err := sessions.IssueSession(context.Background(), UserSessionClaims{TenantID: "local", UserID: id, PersonalityAgentID: agent}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	csrf, csrfCookie := obtainCSRF(t, s)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)
	request := func(authorized, withCSRF bool, origin string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/auth/invitations", strings.NewReader(`{}`))
		r.Header.Set("Origin", origin)
		r.Header.Set("Content-Type", "application/json")
		if authorized {
			r.AddCookie(&http.Cookie{Name: BrowserSessionCookie, Value: cookie})
		}
		if withCSRF {
			r.AddCookie(csrfCookie)
			r.Header.Set("X-CSRF-Token", csrf)
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		return w
	}
	if got := request(false, true, browserAuthTestOrigin).Code; got != 401 {
		t.Fatalf("unauth=%d", got)
	}
	if got := request(true, true, browserAuthTestOrigin).Code; got != 403 {
		t.Fatalf("nonadmin=%d", got)
	}
	s.EnrollmentAdmins = map[string]bool{id: true}
	if got := request(true, false, browserAuthTestOrigin).Code; got != 403 {
		t.Fatalf("no csrf=%d", got)
	}
	if got := request(true, true, "https://other.invalid").Code; got != 403 {
		t.Fatalf("wrong origin=%d", got)
	}
	if store.issued != 0 {
		t.Fatal("rejected requests reached issuance")
	}
	w := request(true, true, browserAuthTestOrigin)
	if w.Code != 201 || store.issued != 1 || store.issuer != id {
		t.Fatalf("authorized issuance=%d", w.Code)
	}
	r := httptest.NewRequest("GET", "/auth/invitations", nil)
	r.AddCookie(&http.Cookie{Name: BrowserSessionCookie, Value: cookie})
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != 200 || strings.Contains(w.Body.String(), "test-token") {
		t.Fatalf("list leaks token or fails: %d", w.Code)
	}
}
func TestAuthAllocationBudgetBeforeStoreAndForwardedHeaders(t *testing.T) {
	s, _ := newTestBrowserAuthServer(t, &fakeFirebaseVerifier{}, &fakeBindingResolver{})
	store := &fakeEnrollmentInvites{}
	s.EnrollmentInvitations = store
	csrf, cookie := obtainCSRF(t, s)
	for i := 0; i < 61; i++ {
		r := httptest.NewRequest("POST", "/auth/invitations/inspect", strings.NewReader(`{"token":"opaque"}`))
		r.RemoteAddr = "192.0.2.1:8000"
		r.Header.Set("Origin", browserAuthTestOrigin)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-CSRF-Token", csrf)
		r.Header.Set("X-Forwarded-For", time.Now().String())
		r.AddCookie(cookie)
		w := httptest.NewRecorder()
		s.serveInspectEnrollmentInvite(w, r)
		want := 200
		if i == 60 {
			want = 429
		}
		if w.Code != want {
			t.Fatalf("request%d=%d want%d", i, w.Code, want)
		}
	}
	if store.inspected != 60 {
		t.Fatalf("store calls=%d", store.inspected)
	}
	var limiter authAllocationLimiter
	now := time.Now()
	for i := 0; i < 300; i++ {
		if !limiter.allow(time.Unix(int64(i), 0).String(), now) {
			t.Fatal("budget rejected early")
		}
	}
	if limiter.allow("another", now) || len(limiter.peers) > 300 {
		t.Fatal("unbounded allocation")
	}
	if !limiter.allow("another", now.Add(time.Minute)) {
		t.Fatal("budget did not recover")
	}
}
