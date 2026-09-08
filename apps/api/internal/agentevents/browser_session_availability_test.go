package agentevents

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestBrowserSessionStatusDistinguishesUnavailableFromSignedOut(t *testing.T) {
	server, sessions := newTestBrowserAuthServer(t, &fakeFirebaseVerifier{}, &fakeBindingResolver{})
	cookie, err := sessions.IssueSession(context.Background(), UserSessionClaims{
		TenantID: "tenant-1", UserID: "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	store := sessions.revocations.(*testBrowserSessionRevocationStore)
	request := func(ctx context.Context, value string, wantCode int, wantAuthenticated bool) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/auth/session", nil).WithContext(ctx)
		req.Header.Set("Origin", browserAuthTestOrigin)
		req.AddCookie(&http.Cookie{Name: BrowserSessionCookie, Value: value})
		recorder := httptest.NewRecorder()
		server.serveSessionStatus(recorder, req)
		if recorder.Code != wantCode {
			t.Fatalf("status = %d, want %d; body %s", recorder.Code, wantCode, recorder.Body.String())
		}
		if len(recorder.Result().Cookies()) != 0 {
			t.Fatal("status read changed browser cookie")
		}
		var body map[string]any
		if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if wantCode == http.StatusOK {
			if body["authenticated"] != wantAuthenticated {
				t.Fatalf("unexpected auth status: %v", body)
			}
		} else if _, exists := body["authenticated"]; exists {
			t.Fatalf("unavailability claimed an authentication result: %v", body)
		}
	}
	request(context.Background(), cookie, http.StatusOK, true)
	for _, failure := range []error{
		&os.PathError{Op: "read", Path: "revocations", Err: syscall.EIO},
		context.DeadlineExceeded,
		errors.New("browser session revocation state integrity failure"),
	} {
		store.checkErr = failure
		request(context.Background(), cookie, http.StatusServiceUnavailable, false)
		store.checkErr = nil
		request(context.Background(), cookie, http.StatusOK, true)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request(ctx, cookie, http.StatusServiceUnavailable, false)
	request(context.Background(), "malformed", http.StatusOK, false)
	originalNow := sessions.now
	sessions.now = func() time.Time { return originalNow().Add(2 * time.Minute) }
	request(context.Background(), cookie, http.StatusOK, false)
	sessions.now = originalNow
	if _, err := sessions.RevokeSession(context.Background(), cookie); err != nil {
		t.Fatal(err)
	}
	request(context.Background(), cookie, http.StatusOK, false)
}
