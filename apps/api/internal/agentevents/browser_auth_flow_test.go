package agentevents

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeAuthFlowController struct {
	startResult           BrowserAuthFlowResult
	resolveResult         BrowserAuthFlowResult
	confirmResult         BrowserAuthFlowResult
	providerStatusResult  ProviderOperationStatusResult
	providerStatusErr     error
	providerStatusCalls   int
	providerStatusClaims  UserSessionClaims
	providerStatusRequest ProviderOperationStatusRequest
}

func (f *fakeAuthFlowController) Start(context.Context, StartBrowserAuthFlowRequest) (BrowserAuthFlowResult, error) {
	return f.startResult, nil
}
func (f *fakeAuthFlowController) Resolve(context.Context, ResolveBrowserAuthFlowRequest, FirebaseIdentity) (BrowserAuthFlowResult, error) {
	return f.resolveResult, nil
}
func (f *fakeAuthFlowController) Confirm(context.Context, ConfirmBrowserAuthFlowRequest) (BrowserAuthFlowResult, error) {
	return f.confirmResult, nil
}
func (f *fakeAuthFlowController) Status(context.Context, ConfirmBrowserAuthFlowRequest) (BrowserAuthFlowResult, error) {
	return f.confirmResult, nil
}
func (f *fakeAuthFlowController) StartProviderOperation(context.Context, UserSessionClaims, StartProviderOperationRequest, FirebaseIdentity) (ProviderOperationResult, error) {
	return ProviderOperationResult{}, nil
}
func (f *fakeAuthFlowController) CompleteProviderOperation(context.Context, UserSessionClaims, CompleteProviderOperationRequest, FirebaseIdentity) (ProviderOperationResult, error) {
	return ProviderOperationResult{}, nil
}
func (f *fakeAuthFlowController) FailProviderOperation(context.Context, UserSessionClaims, FailProviderOperationRequest) (ProviderOperationResult, error) {
	return ProviderOperationResult{}, nil
}
func (f *fakeAuthFlowController) StatusProviderOperation(_ context.Context, claims UserSessionClaims, request ProviderOperationStatusRequest) (ProviderOperationStatusResult, error) {
	f.providerStatusCalls++
	f.providerStatusClaims = claims
	f.providerStatusRequest = request
	return f.providerStatusResult, f.providerStatusErr
}

type trackingProviderStatusSessions struct {
	*HMACUserSessionVerifier
	mutations int
}

func (s *trackingProviderStatusSessions) IssueSession(ctx context.Context, claims UserSessionClaims, ttl time.Duration) (string, error) {
	s.mutations++
	return s.HMACUserSessionVerifier.IssueSession(ctx, claims, ttl)
}

func (s *trackingProviderStatusSessions) RotateSession(ctx context.Context, signedCookie string, claims UserSessionClaims, ttl time.Duration) (UserSessionClaims, string, bool, error) {
	s.mutations++
	return s.HMACUserSessionVerifier.RotateSession(ctx, signedCookie, claims, ttl)
}

func (s *trackingProviderStatusSessions) RevokeSession(ctx context.Context, signedCookie string) (UserSessionClaims, error) {
	s.mutations++
	return s.HMACUserSessionVerifier.RevokeSession(ctx, signedCookie)
}

func (s *trackingProviderStatusSessions) RevokeSessionForLogout(ctx context.Context, signedCookie string) (UserSessionClaims, bool, error) {
	s.mutations++
	return s.HMACUserSessionVerifier.RevokeSessionForLogout(ctx, signedCookie)
}

func postFlowJSON(t *testing.T, server *BrowserAuthServer, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	csrf, cookie := obtainCSRF(t, server)
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Origin", browserAuthTestOrigin)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-CSRF-Token", csrf)
	req.AddCookie(cookie)
	recorder := httptest.NewRecorder()
	switch path {
	case "/auth/flows":
		server.serveStartAuthFlow(recorder, req)
	case "/auth/flows/resolve":
		server.serveResolveAuthFlow(recorder, req)
	case "/auth/flows/confirm":
		server.serveConfirmAuthFlow(recorder, req)
	}
	return recorder
}

func TestBrowserAuthFlowRoutesExposeSemanticOutcomes(t *testing.T) {
	firebase := &fakeFirebaseVerifier{identity: FirebaseIdentity{
		UID: "firebase-user", Email: "human@example.com", EmailVerified: true,
		SignInProvider: "password", AuthTime: time.Now(),
	}}
	bindings := &fakeBindingResolver{}
	server, sessions := newTestBrowserAuthServer(t, firebase, bindings)
	controller := &fakeAuthFlowController{
		startResult:   BrowserAuthFlowResult{FlowID: "flow-id", Outcome: "proof_required", ExpiresAt: time.Now().Add(time.Minute)},
		resolveResult: BrowserAuthFlowResult{FlowID: "flow-id", Outcome: "confirmation_required", NextAction: "create_account"},
		confirmResult: BrowserAuthFlowResult{FlowID: "flow-id", Outcome: "account_created", Claims: UserSessionClaims{
			TenantID: "local", UserID: "0198f0f4-9b72-7000-8000-000000000010", PersonalityAgentID: "0198f0f4-9b72-7000-8000-000000000011",
		}},
	}
	server.Flows = controller

	started := postFlowJSON(t, server, "/auth/flows", `{"intent":"sign_up","provider":"email_link","email":"human@example.com","continuation":"/direct-chat","nonce":"abc"}`)
	if started.Code != http.StatusCreated || !strings.Contains(started.Body.String(), `"outcome":"proof_required"`) {
		t.Fatalf("start: %d %s", started.Code, started.Body.String())
	}

	resolved := postFlowJSON(t, server, "/auth/flows/resolve", `{"flow_id":"flow-id","nonce":"abc","id_token":"token"}`)
	if resolved.Code != http.StatusOK || !strings.Contains(resolved.Body.String(), `"next_action":"create_account"`) {
		t.Fatalf("resolve: %d %s", resolved.Code, resolved.Body.String())
	}
	if len(resolved.Result().Cookies()) != 0 {
		t.Fatal("confirmation-required response issued a session")
	}

	confirmed := postFlowJSON(t, server, "/auth/flows/confirm", `{"flow_id":"flow-id","nonce":"abc","action":"create_account"}`)
	if confirmed.Code != http.StatusOK || !strings.Contains(confirmed.Body.String(), `"outcome":"account_created"`) {
		t.Fatalf("confirm: %d %s", confirmed.Code, confirmed.Body.String())
	}
	var sessionCookie *http.Cookie
	for _, cookie := range confirmed.Result().Cookies() {
		if cookie.Name == BrowserSessionCookie {
			sessionCookie = cookie
		}
	}
	if sessionCookie == nil {
		t.Fatal("terminal outcome did not issue session")
	}
	claims, err := sessions.VerifySession(context.Background(), sessionCookie.Value)
	if err != nil || claims.UserID != controller.confirmResult.Claims.UserID {
		t.Fatalf("session claims: %+v %v", claims, err)
	}

	encoded, _ := json.Marshal(controller.confirmResult)
	if strings.Contains(string(encoded), controller.confirmResult.Claims.PersonalityAgentID) {
		t.Fatal("semantic response leaked session claims")
	}
}

func TestRegisterRoutesRemovesSilentLegacyExchangeWhenFlowsEnabled(t *testing.T) {
	server, _ := newTestBrowserAuthServer(t, &fakeFirebaseVerifier{}, &fakeBindingResolver{})
	server.Flows = &fakeAuthFlowController{}
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	legacy := httptest.NewRequest(http.MethodPost, "/auth/session", nil)
	legacyRecorder := httptest.NewRecorder()
	mux.ServeHTTP(legacyRecorder, legacy)
	if legacyRecorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("legacy auto-registration exchange remains registered: %d", legacyRecorder.Code)
	}
}

func TestProviderOperationStatusRecoversWithoutSessionOrFirebaseSideEffects(t *testing.T) {
	createdAt := time.Date(2026, 8, 1, 6, 0, 0, 250_000_000, time.UTC)
	completedAt := createdAt.Add(2 * time.Second)
	controller := &fakeAuthFlowController{providerStatusResult: ProviderOperationStatusResult{
		OperationID: "0198f0f4-9b72-7000-8000-000000000020",
		Provider:    "github.com", Operation: "link", Status: "completed",
		Outcome: "provider_linked", CreatedAt: createdAt,
		CompletionTokenNotBefore: createdAt.Truncate(time.Second).Add(time.Second),
		ExpiresAt:                createdAt.Add(10 * time.Minute), CompletedAt: &completedAt,
		NoticeRequired: true,
	}}
	firebase := &fakeFirebaseVerifier{}
	server, sessions := newTestBrowserAuthServer(t, firebase, &fakeBindingResolver{})
	claims := UserSessionClaims{
		TenantID: "local", UserID: "0198f0f4-9b72-7000-8000-000000000010",
		PersonalityAgentID: "0198f0f4-9b72-7000-8000-000000000011",
	}
	session, err := sessions.IssueSession(context.Background(), claims, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	trackedSessions := &trackingProviderStatusSessions{HMACUserSessionVerifier: sessions}
	server.Sessions = trackedSessions
	server.Flows = controller

	requestStatus := func() *httptest.ResponseRecorder {
		t.Helper()
		csrf, csrfCookie := obtainCSRF(t, server)
		request := httptest.NewRequest(http.MethodPost, "/auth/providers/operations/status", strings.NewReader(
			`{"operation_id":"0198f0f4-9b72-7000-8000-000000000020","nonce":"nonce-value"}`))
		request.Header.Set("Origin", browserAuthTestOrigin)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-CSRF-Token", csrf)
		request.AddCookie(csrfCookie)
		request.AddCookie(&http.Cookie{Name: BrowserSessionCookie, Value: session})
		recorder := httptest.NewRecorder()
		server.serveProviderOperationStatus(recorder, request)
		return recorder
	}

	first := requestStatus()
	second := requestStatus()
	if first.Code != http.StatusOK || second.Code != http.StatusOK {
		t.Fatalf("status responses: %d %s / %d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
	}
	if first.Body.String() != second.Body.String() {
		t.Fatalf("repeated status changed:\n%s\n%s", first.Body.String(), second.Body.String())
	}
	if cookies := append(first.Result().Cookies(), second.Result().Cookies()...); len(cookies) != 0 {
		t.Fatalf("status issued cookies: %+v", cookies)
	}
	if trackedSessions.mutations != 0 {
		t.Fatalf("status mutated session lifecycle %d times", trackedSessions.mutations)
	}
	if firebase.calls != 0 {
		t.Fatalf("status verified Firebase token %d times", firebase.calls)
	}
	if controller.providerStatusCalls != 2 || controller.providerStatusClaims.UserID != claims.UserID ||
		controller.providerStatusRequest.Nonce != "nonce-value" {
		t.Fatalf("controller invocation: calls=%d claims=%+v request=%+v", controller.providerStatusCalls, controller.providerStatusClaims, controller.providerStatusRequest)
	}
	if _, err := sessions.VerifySession(context.Background(), session); err != nil {
		t.Fatalf("status invalidated original session: %v", err)
	}
	body := first.Body.String()
	for _, secret := range []string{claims.UserID, claims.PersonalityAgentID, "firebase", "id_token"} {
		if strings.Contains(body, secret) {
			t.Fatalf("status leaked %q: %s", secret, body)
		}
	}
	var decoded ProviderOperationStatusResult
	if err := json.Unmarshal(first.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.OperationID != controller.providerStatusResult.OperationID || decoded.Status != "completed" ||
		decoded.Outcome != "provider_linked" || decoded.CompletedAt == nil || !decoded.NoticeRequired {
		t.Fatalf("status payload: %+v", decoded)
	}
}

func TestProviderOperationStatusRequiresOriginCSRFAndSession(t *testing.T) {
	server, sessions := newTestBrowserAuthServer(t, &fakeFirebaseVerifier{}, &fakeBindingResolver{})
	controller := &fakeAuthFlowController{}
	server.Flows = controller
	session, err := sessions.IssueSession(context.Background(), UserSessionClaims{
		TenantID: "local", UserID: "0198f0f4-9b72-7000-8000-000000000010",
		PersonalityAgentID: "0198f0f4-9b72-7000-8000-000000000011",
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	csrf, csrfCookie := obtainCSRF(t, server)
	body := `{"operation_id":"0198f0f4-9b72-7000-8000-000000000020","nonce":"nonce-value"}`
	tests := []struct {
		name       string
		origin     string
		csrfHeader string
		csrfCookie bool
		session    bool
		want       int
	}{
		{name: "wrong origin", origin: "https://evil.example", csrfHeader: csrf, csrfCookie: true, session: true, want: http.StatusForbidden},
		{name: "missing csrf", origin: browserAuthTestOrigin, session: true, want: http.StatusForbidden},
		{name: "missing session", origin: browserAuthTestOrigin, csrfHeader: csrf, csrfCookie: true, want: http.StatusUnauthorized},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/auth/providers/operations/status", strings.NewReader(body))
			request.Header.Set("Origin", test.origin)
			request.Header.Set("Content-Type", "application/json")
			if test.csrfHeader != "" {
				request.Header.Set("X-CSRF-Token", test.csrfHeader)
			}
			if test.csrfCookie {
				request.AddCookie(csrfCookie)
			}
			if test.session {
				request.AddCookie(&http.Cookie{Name: BrowserSessionCookie, Value: session})
			}
			recorder := httptest.NewRecorder()
			server.serveProviderOperationStatus(recorder, request)
			if recorder.Code != test.want {
				t.Fatalf("status=%d body=%s, want %d", recorder.Code, recorder.Body.String(), test.want)
			}
		})
	}
	if controller.providerStatusCalls != 0 {
		t.Fatalf("rejected request reached controller %d times", controller.providerStatusCalls)
	}
}

func TestProviderOperationStatusUsesStrictTwoFieldJSON(t *testing.T) {
	server, sessions := newTestBrowserAuthServer(t, &fakeFirebaseVerifier{}, &fakeBindingResolver{})
	controller := &fakeAuthFlowController{}
	server.Flows = controller
	session, err := sessions.IssueSession(context.Background(), UserSessionClaims{
		TenantID: "local", UserID: "0198f0f4-9b72-7000-8000-000000000010",
		PersonalityAgentID: "0198f0f4-9b72-7000-8000-000000000011",
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name        string
		body        string
		contentType string
		want        int
	}{
		{name: "unknown id token", body: `{"operation_id":"op","nonce":"nonce","id_token":"forbidden"}`, contentType: "application/json", want: http.StatusBadRequest},
		{name: "duplicate field", body: `{"operation_id":"one","operation_id":"two","nonce":"nonce"}`, contentType: "application/json", want: http.StatusBadRequest},
		{name: "missing nonce", body: `{"operation_id":"op"}`, contentType: "application/json", want: http.StatusBadRequest},
		{name: "non json", body: `{"operation_id":"op","nonce":"nonce"}`, contentType: "text/plain", want: http.StatusUnsupportedMediaType},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			csrf, csrfCookie := obtainCSRF(t, server)
			request := httptest.NewRequest(http.MethodPost, "/auth/providers/operations/status", strings.NewReader(test.body))
			request.Header.Set("Origin", browserAuthTestOrigin)
			request.Header.Set("Content-Type", test.contentType)
			request.Header.Set("X-CSRF-Token", csrf)
			request.AddCookie(csrfCookie)
			request.AddCookie(&http.Cookie{Name: BrowserSessionCookie, Value: session})
			recorder := httptest.NewRecorder()
			server.serveProviderOperationStatus(recorder, request)
			if recorder.Code != test.want {
				t.Fatalf("status=%d body=%s, want %d", recorder.Code, recorder.Body.String(), test.want)
			}
		})
	}
	if controller.providerStatusCalls != 0 {
		t.Fatalf("invalid JSON reached controller %d times", controller.providerStatusCalls)
	}
}

func TestProviderOperationStatusRouteExistsOnlyInFlowMode(t *testing.T) {
	server, _ := newTestBrowserAuthServer(t, &fakeFirebaseVerifier{}, &fakeBindingResolver{})
	path := "/auth/providers/operations/status"

	legacyMux := http.NewServeMux()
	server.RegisterRoutes(legacyMux)
	legacyRecorder := httptest.NewRecorder()
	legacyMux.ServeHTTP(legacyRecorder, httptest.NewRequest(http.MethodPost, path, nil))
	if legacyRecorder.Code != http.StatusNotFound {
		t.Fatalf("status route registered outside flow mode: %d", legacyRecorder.Code)
	}

	server.Flows = &fakeAuthFlowController{}
	flowMux := http.NewServeMux()
	server.RegisterRoutes(flowMux)
	flowRecorder := httptest.NewRecorder()
	flowMux.ServeHTTP(flowRecorder, httptest.NewRequest(http.MethodPost, path, nil))
	if flowRecorder.Code != http.StatusForbidden {
		t.Fatalf("status route missing in flow mode: %d", flowRecorder.Code)
	}
}
