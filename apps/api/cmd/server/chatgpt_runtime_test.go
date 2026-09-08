package main

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/chatgpt"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
)

type chatGPTTestStore struct {
	status                      chatgpt.Status
	human, connection, rejected string
	calls                       int
}

func (s *chatGPTTestStore) Status(_ context.Context, human string) (chatgpt.Status, error) {
	s.human = human
	return s.status, nil
}
func (s *chatGPTTestStore) ResolveAccess(_ context.Context, human, connection, rejected string, _ chatgpt.RefreshFunc) (chatgpt.Access, error) {
	s.calls++
	s.human = human
	s.connection = connection
	s.rejected = rejected
	return chatgpt.Access{Token: "synthetic-access", AccountID: "account", ConnectionID: connection, Selection: chatgpt.Selection{Model: "gpt-6-astra", Effort: "medium"}}, nil
}

type chatGPTTestEmployer struct {
	kind   string
	denied bool
	pa     string
}

func (e *chatGPTTestEmployer) CurrentEmployer(_ context.Context, pa string) (string, string, error) {
	e.pa = pa
	return e.kind, "current-human", nil
}
func (e *chatGPTTestEmployer) AuthorizeCurrentHumanEmployer(_ context.Context, human, pa string, effect func() error) error {
	if e.denied || human != "current-human" || pa != e.pa {
		return errors.New("employment changed")
	}
	return effect()
}
func TestChatGPTRuntimeSelectionPreservesReviewerAndDefaultBoundaries(t *testing.T) {
	store := &chatGPTTestStore{status: chatgpt.Status{Connected: true, ConnectionID: "connection", AccountID: "actual-account", Selection: chatgpt.Selection{Model: "gpt-6-astra", Effort: "medium"}}}
	employer := &chatGPTTestEmployer{kind: "human"}
	runtime := &chatGPTRuntime{connections: store, employers: employer}
	base := runtimeprovision.ActivationConfig{ModelPreset: "kimi-k3", ProviderAPIKey: "default-key", ExecutionReviewerAPIKey: "reviewer-key", ExecutionReviewerModelPreset: "kimi-k3"}
	got, err := runtime.activation(context.Background(), "pa", base)
	if err != nil || got.ModelPreset != "chatgpt-responses" || got.ModelAccountScope != "actual-account" || got.ChatGPTConnectionID != "connection" || got.ProviderAPIKey != "" || got.ExecutionReviewerAPIKey != base.ExecutionReviewerAPIKey || got.ExecutionReviewerModelPreset != base.ExecutionReviewerModelPreset {
		t.Fatalf("incorrect native activation boundary: %v", err)
	}
	store.status = chatgpt.Status{}
	got, err = runtime.activation(context.Background(), "pa", base)
	if err != nil || got.ModelPreset != base.ModelPreset || got.ProviderAPIKey != base.ProviderAPIKey {
		t.Fatal("disconnected next-start default changed")
	}
	employer.kind = "workspace"
	got, err = runtime.activation(context.Background(), "pa", base)
	if err != nil || got.ModelPreset != base.ModelPreset {
		t.Fatal("workspace default changed")
	}
	employer.kind = "human"
	employer.denied = true
	if _, err = runtime.activation(context.Background(), "pa", base); err == nil {
		t.Fatal("changed employment admitted")
	}
}
func TestChatGPTRuntimeAccessUsesAuthorizedPAAndRejectsCallerIdentity(t *testing.T) {
	store := &chatGPTTestStore{}
	employer := &chatGPTTestEmployer{kind: "human"}
	runtime := &chatGPTRuntime{connections: store, employers: employer}
	request := httptest.NewRequest("POST", chatGPTAccessPath, strings.NewReader(`{"connection_id":"connection","rejected_access_token":"rejected"}`))
	response := httptest.NewRecorder()
	runtime.access(response, request, agentevents.LocalRuntimeAuthorization{PersonalityAgentID: "authenticated-pa"})
	if response.Code != 200 || store.human != "current-human" || employer.pa != "authenticated-pa" || store.connection != "connection" || store.rejected != "rejected" || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("credential authority or response contract failed")
	}
	for _, body := range []string{`{"connection_id":"connection","human_id":"other"}`, `{"connection_id":"one","connection_id":"two"}`} {
		response = httptest.NewRecorder()
		runtime.access(response, httptest.NewRequest("POST", chatGPTAccessPath, strings.NewReader(body)), agentevents.LocalRuntimeAuthorization{PersonalityAgentID: "authenticated-pa"})
		if response.Code != 400 {
			t.Fatal("forged/ambiguous request accepted")
		}
	}
	employer.denied = true
	response = httptest.NewRecorder()
	runtime.access(response, httptest.NewRequest("POST", chatGPTAccessPath, strings.NewReader(`{"connection_id":"connection"}`)), agentevents.LocalRuntimeAuthorization{PersonalityAgentID: "authenticated-pa"})
	if response.Code != 503 || store.calls != 1 || strings.Contains(response.Body.String(), "synthetic-access") {
		t.Fatal("employment denial leaked or resolved credentials")
	}
}

func TestChatGPTBrowserIdentityRevalidatesSessionAtAsyncCommit(t *testing.T) {
	sessions := &profileSessionAuthorizer{claims: agentevents.UserSessionClaims{UserID: "human"}, authorize: true}
	authenticate := chatGPTBrowserIdentity(sessions, []string{testBrowserOrigin})
	identity, err := authenticate(profileRequest(`{}`))
	if err != nil || identity.HumanID != "human" {
		t.Fatal("valid authenticated mutation rejected")
	}
	sessions.authorize = false
	called := false
	if identity.Authorize(context.Background(), func(context.Context) error { called = true; return nil }) == nil || called {
		t.Fatal("revoked initiating session committed")
	}
	if _, err = authenticate(profileReadRequest()); err == nil {
		t.Fatal("revoked GET accepted")
	}
	sessions.authorize = true
	request := profileRequest(`{}`)
	request.Header.Del("X-CSRF-Token")
	if _, err = authenticate(request); err == nil {
		t.Fatal("mutation without CSRF accepted")
	}
	request = profileReadRequest()
	request.Header.Set("Origin", "https://foreign.invalid")
	if _, err = authenticate(request); err == nil {
		t.Fatal("foreign-origin read accepted")
	}
	request = profileReadRequest()
	if _, err = authenticate(request); err != nil {
		t.Fatal("same-origin browser GET without Origin rejected")
	}
}
