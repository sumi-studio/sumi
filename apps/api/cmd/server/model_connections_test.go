package main

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/modelconnections"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
	"net/http/httptest"
	"strings"
	"testing"
)

type userConnectionFixture struct {
	selected modelconnections.Selection
	exists   bool
	human    string
}

func (s *userConnectionFixture) Selected(_ context.Context, h string) (modelconnections.Selection, bool, error) {
	s.human = h
	return s.selected, s.exists, nil
}
func (s *userConnectionFixture) Metadata(_ context.Context, h, id string) (modelconnections.Access, error) {
	if h != "current-human" || id != "mine" {
		return modelconnections.Access{}, errors.New("wrong owner")
	}
	return modelconnections.Access{Connection: modelconnections.Connection{ID: id, Preset: "openai-chat", Model: "my-model", BaseURL: "https://api.example/v1"}, APIKey: "my-key", Version: "version"}, nil
}
func TestUserModelActivationHonorsChoiceAndEmployer(t *testing.T) {
	ctx := context.Background()
	store := &userConnectionFixture{selected: modelconnections.Selection{Kind: "api", ConnectionID: "mine"}, exists: true}
	employer := &chatGPTTestEmployer{kind: "human"}
	resolve := userModelActivation(store, employer, nil, nil)
	base := runtimeprovision.ActivationConfig{ProviderAPIKey: "operator", ModelPreset: "chatgpt-responses", ChatGPTConnectionID: "old", ModelReasoningEffort: "medium", ExecutionReviewerAPIKey: "reviewer"}
	got, err := resolve(ctx, "pa", base)
	if err != nil || got.ProviderAPIKey != "" || got.APIConnectionID != "mine" || got.APIConnectionHumanID != "current-human" || got.APIConnectionVersion != "version" || got.ModelBaseURL != "https://api.example/v1" || !got.ModelPublicEndpoint || got.ChatGPTConnectionID != "" || got.ModelReasoningEffort != "" || got.ExecutionReviewerAPIKey != "reviewer" {
		t.Fatal("BYOK configuration", err)
	}
	store.selected = modelconnections.Selection{Kind: "none"}
	if _, err = resolve(ctx, "pa", base); err == nil {
		t.Fatal("none used operator credential")
	}
	store.selected = modelconnections.Selection{Kind: "chatgpt"}
	if _, err = resolve(ctx, "pa", base); err == nil {
		t.Fatal("disconnected ChatGPT used operator credential")
	}
	store.exists = false
	got, err = resolve(ctx, "pa", base)
	if err != nil || got.ProviderAPIKey != "operator" {
		t.Fatal("existing default changed", err)
	}
	employer.denied = true
	if _, err = resolve(ctx, "pa", base); err == nil {
		t.Fatal("employer revocation ignored")
	}
}

func TestNoModelSelectionStopsAtIdleWithoutRestartLoop(t *testing.T) {
	employer := &activationTestEmployer{}
	manager := &activationTestManager{employer: employer}
	worker := newChatGPTActivationWorker(employer)
	worker.manager = manager
	worker.shouldStart = func(context.Context, string) (bool, error) { return false, nil }
	complete, err := worker.apply(context.Background(), "human")
	if err != nil || complete || manager.starts != 0 {
		t.Fatal("busy work was interrupted")
	}
	manager.idle = true
	complete, err = worker.apply(context.Background(), "human")
	if err != nil || !complete || manager.starts != 0 {
		t.Fatal("none restarted or kept retrying", err)
	}
}

func (s *userConnectionFixture) Resolve(ctx context.Context, h, id string) (modelconnections.Access, error) {
	return s.Metadata(ctx, h, id)
}
func TestUserAPIAccessRechecksOwnerSelectionAndVersion(t *testing.T) {
	store := &userConnectionFixture{selected: modelconnections.Selection{Kind: "api", ConnectionID: "mine"}, exists: true}
	employer := &chatGPTTestEmployer{kind: "human", pa: "pa"}
	handler := userAPIAccess(store, employer)
	request := func(body string, pa string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		handler(w, httptest.NewRequest("POST", userAPIAccessPath, strings.NewReader(body)), agentevents.LocalRuntimeAuthorization{PersonalityAgentID: pa})
		return w
	}
	body := `{"human_id":"current-human","connection_id":"mine","version":"version"}`
	if w := request(body, "pa"); w.Code != 200 || !strings.Contains(w.Body.String(), "my-key") || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("access unavailable")
	}
	for _, payload := range []string{strings.Replace(body, "current-human", "other", 1), strings.Replace(body, `"version":"version"`, `"version":"stale"`, 1), strings.Replace(body, "mine", "other", 1)} {
		if w := request(payload, "pa"); w.Code == 200 || strings.Contains(w.Body.String(), "my-key") {
			t.Fatal("foreign or stale access")
		}
	}
	if w := request(body, "other-pa"); w.Code == 200 {
		t.Fatal("different personality agent used connection")
	}
	employer.denied = true
	if w := request(body, "pa"); w.Code == 200 || strings.Contains(w.Body.String(), "my-key") {
		t.Fatal("key survived employer transfer")
	}
	employer.denied = false
	store.selected = modelconnections.Selection{Kind: "none"}
	if w := request(body, "pa"); w.Code == 200 {
		t.Fatal("key survived selection revocation")
	}
}

func TestMissingEncryptionKeyKeepsSelectionStore(t *testing.T) {
	t.Setenv("SUMI_MODEL_CONNECTION_KEY", "")
	store, err := modelConnectionStoreFromEnv(&pgxpool.Pool{})
	if err != nil || store == nil {
		t.Fatal("missing encryption key disables persisted selection checks", err)
	}
}
func TestChatGPTAccessStopsAfterExplicitSelectionChanges(t *testing.T) {
	store := &chatGPTTestStore{}
	selections := &userConnectionFixture{exists: true, selected: modelconnections.Selection{Kind: "chatgpt"}}
	runtime := &chatGPTRuntime{connections: store, employers: &chatGPTTestEmployer{kind: "human"}, selection: selections}
	access := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		runtime.access(w, httptest.NewRequest("POST", chatGPTAccessPath, strings.NewReader(`{"connection_id":"connection"}`)), agentevents.LocalRuntimeAuthorization{PersonalityAgentID: "pa"})
		return w
	}
	if access().Code != 200 {
		t.Fatal("selected ChatGPT unavailable")
	}
	for _, kind := range []string{"none", "api"} {
		selections.selected.Kind = kind
		if w := access(); w.Code == 200 || store.calls != 1 || strings.Contains(w.Body.String(), "synthetic-access") {
			t.Fatalf("old ChatGPT credentials survived selection %s", kind)
		}
	}
	selections.exists = false
	if access().Code != 200 || store.calls != 2 {
		t.Fatal("unselected legacy ChatGPT access changed")
	}
}
