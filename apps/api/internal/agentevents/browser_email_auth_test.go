package agentevents

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeEmailAuthController struct {
	err            error
	verifyCalls    int
	completeCalls  int
	inspectSession *UserSessionClaims
}

func (f *fakeEmailAuthController) VerifyEmailCode(context.Context, VerifyEmailCodeRequest, *UserSessionClaims) (EmailProofResult, error) {
	f.verifyCalls++
	if f.err != nil {
		return EmailProofResult{}, f.err
	}
	return EmailProofResult{FlowID: "flow-id", Intent: "sign_in", CustomToken: "custom-token"}, nil
}

func (f *fakeEmailAuthController) ResendEmailCode(context.Context, EmailFlowRequest) (EmailChallengeResult, error) {
	return EmailChallengeResult{FlowID: "flow-id"}, f.err
}

func (f *fakeEmailAuthController) EmailFlowStatus(context.Context, EmailFlowRequest) (EmailChallengeResult, error) {
	return EmailChallengeResult{FlowID: "flow-id", FlowStatus: "pending"}, f.err
}

func (f *fakeEmailAuthController) InspectEmailLink(_ context.Context, _ InspectEmailLinkRequest, session *UserSessionClaims) (EmailLinkInspectionResult, error) {
	f.inspectSession = session
	return EmailLinkInspectionResult{FlowID: "flow-id", State: "usable", Session: "none"}, f.err
}

func (f *fakeEmailAuthController) CompleteEmailLink(context.Context, CompleteEmailLinkRequest, *UserSessionClaims) (EmailProofResult, error) {
	f.completeCalls++
	return EmailProofResult{FlowID: "flow-id", Intent: "sign_in", CustomToken: "custom-token"}, f.err
}

func newEmailAuthTestServer(t *testing.T, controller *fakeEmailAuthController) (*BrowserAuthServer, *http.ServeMux, *HMACUserSessionVerifier) {
	t.Helper()
	server, sessions := newTestBrowserAuthServer(t, &fakeFirebaseVerifier{}, &fakeBindingResolver{})
	server.Flows = &fakeAuthFlowController{}
	server.EmailFlows = controller
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	return server, mux, sessions
}

func postEmailAuth(t *testing.T, server *BrowserAuthServer, mux *http.ServeMux, path, body string, cookies ...*http.Cookie) *httptest.ResponseRecorder {
	t.Helper()
	csrf, csrfCookie, epochCookie := obtainCSRF(t, server)
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Origin", browserAuthTestOrigin)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-CSRF-Token", csrf)
	req.AddCookie(csrfCookie)
	req.AddCookie(epochCookie)
	for _, cookie := range cookies {
		req.AddCookie(cookie)
	}
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, req)
	return recorder
}

const emailLinkTestBody = `{"challenge_id":"0198f0f4-9b72-7000-8000-000000000030","token":"token","nonce":"nonce","adopt":true}`

func TestEmailAuthRoutesMapSemanticErrorsWithoutSession(t *testing.T) {
	retryAt := time.Now().Add(90 * time.Second)
	for _, tc := range []struct {
		name   string
		err    error
		status int
		body   []string
	}{
		{"mismatch", &BrowserEmailCodeMismatchError{AttemptsRemaining: 3}, 422, []string{`"error":"code_mismatch"`, `"attempts_remaining":3`}},
		{"format", &BrowserEmailCodeMismatchError{AttemptsRemaining: -1}, 422, []string{`"error":"code_mismatch"`}},
		{"limited", &BrowserEmailSendLimitedError{RetryAt: retryAt}, 429, []string{`"error":"email_send_limited"`, `"retry_at":`}},
		{"locked", ErrBrowserEmailCodeLocked, 409, []string{`"error":"code_locked"`}},
		{"superseded", ErrBrowserEmailSuperseded, 409, []string{`"error":"email_superseded"`}},
		{"expired", ErrBrowserEmailExpired, 410, []string{`"error":"email_expired"`}},
		{"continued", ErrBrowserEmailContinuedElsewhere, 409, []string{`"error":"continued_in_other_browser"`}},
		{"unverified", ErrBrowserEmailUnverifiedAccount, 409, []string{`"error":"email_unverified_account"`}},
		{"unverified providers", &BrowserEmailUnverifiedAccountError{SignInProviders: []string{"github.com"}}, 409, []string{`"error":"email_unverified_account"`, `"sign_in_providers":["github.com"]`}},
		{"unverified no providers", &BrowserEmailUnverifiedAccountError{}, 409, []string{`"sign_in_providers":[]`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, mux, _ := newEmailAuthTestServer(t, &fakeEmailAuthController{err: tc.err})
			recorder := postEmailAuth(t, server, mux, "/auth/email/verify", `{"flow_id":"flow-id","nonce":"nonce","code":"123456"}`)
			if recorder.Code != tc.status {
				t.Fatalf("status %d body %s", recorder.Code, recorder.Body.String())
			}
			for _, want := range tc.body {
				if !strings.Contains(recorder.Body.String(), want) {
					t.Fatalf("body %s missing %s", recorder.Body.String(), want)
				}
			}
			if tc.name == "format" && strings.Contains(recorder.Body.String(), "attempts_remaining") {
				t.Fatalf("uncounted format error reported attempts: %s", recorder.Body.String())
			}
			if tc.name == "limited" && recorder.Header().Get("Retry-After") == "" {
				t.Fatal("send limit without Retry-After")
			}
			if len(recorder.Result().Cookies()) != 0 {
				t.Fatal("email proof route issued a cookie")
			}
		})
	}

	server, mux, _ := newEmailAuthTestServer(t, &fakeEmailAuthController{})
	ok := postEmailAuth(t, server, mux, "/auth/email/verify", `{"flow_id":"flow-id","nonce":"nonce","code":"123456"}`)
	if ok.Code != http.StatusOK || !strings.Contains(ok.Body.String(), `"custom_token":"custom-token"`) {
		t.Fatalf("verify success: %d %s", ok.Code, ok.Body.String())
	}
	for _, cookie := range ok.Result().Cookies() {
		if cookie.Name == BrowserSessionCookie {
			t.Fatal("mailbox proof issued a Sumi session before flow resolution")
		}
	}
}

func TestEmailLinkCompletionPassesSessionThroughAndInspectSeesIt(t *testing.T) {
	controller := &fakeEmailAuthController{}
	server, mux, sessions := newEmailAuthTestServer(t, controller)
	claims := UserSessionClaims{TenantID: "local", UserID: "0198f0f4-9b72-7000-8000-000000000010", PersonalityAgentID: "0198f0f4-9b72-7000-8000-000000000011"}
	session, err := sessions.IssueSession(context.Background(), claims, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	sessionCookie := &http.Cookie{Name: BrowserSessionCookie, Value: session}

	inspected := postEmailAuth(t, server, mux, "/auth/email/link/inspect", `{"challenge_id":"0198f0f4-9b72-7000-8000-000000000030","token":"token"}`, sessionCookie)
	if inspected.Code != http.StatusOK || controller.inspectSession == nil || controller.inspectSession.UserID != claims.UserID {
		t.Fatalf("inspect with session: %d %s %+v", inspected.Code, inspected.Body.String(), controller.inspectSession)
	}
	// Completion beside a live session is now allowed through: the proof is
	// only mailbox evidence, and resolve decides same-account vs. explicit
	// switch through session admission.
	completed := postEmailAuth(t, server, mux, "/auth/email/link/complete", emailLinkTestBody, sessionCookie)
	if completed.Code != http.StatusOK || controller.completeCalls != 1 {
		t.Fatalf("complete beside session: %d %s calls=%d", completed.Code, completed.Body.String(), controller.completeCalls)
	}
	if _, err := sessions.VerifySession(context.Background(), session); err != nil {
		t.Fatalf("completion changed the active session: %v", err)
	}
	completed = postEmailAuth(t, server, mux, "/auth/email/link/complete", emailLinkTestBody)
	if completed.Code != http.StatusOK || controller.completeCalls != 2 {
		t.Fatalf("complete without session: %d %s", completed.Code, completed.Body.String())
	}
}

func TestEmailAuthRoutesRequireCSRFAndStayAbsentWhenDisabled(t *testing.T) {
	controller := &fakeEmailAuthController{}
	_, mux, _ := newEmailAuthTestServer(t, controller)
	req := httptest.NewRequest(http.MethodPost, "/auth/email/verify", strings.NewReader(`{"flow_id":"flow-id","nonce":"nonce","code":"123456"}`))
	req.Header.Set("Origin", browserAuthTestOrigin)
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, req)
	if recorder.Code == http.StatusOK || controller.verifyCalls != 0 {
		t.Fatalf("verify without CSRF: %d calls=%d", recorder.Code, controller.verifyCalls)
	}

	server, _ := newTestBrowserAuthServer(t, &fakeFirebaseVerifier{}, &fakeBindingResolver{})
	server.Flows = &fakeAuthFlowController{}
	disabled := http.NewServeMux()
	server.RegisterRoutes(disabled)
	recorder = postEmailAuth(t, server, disabled, "/auth/email/verify", `{"flow_id":"flow-id","nonce":"nonce","code":"123456"}`)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("email routes registered without email sign-in: %d", recorder.Code)
	}
}
