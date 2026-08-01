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
	startResult   BrowserAuthFlowResult
	resolveResult BrowserAuthFlowResult
	confirmResult BrowserAuthFlowResult
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
