package main

import (
	"context"
	"github.com/sumi-studio/sumi/apps/api/internal/chatgpt"
	"github.com/sumi-studio/sumi/apps/api/internal/modelconnections"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
	"testing"
)

func TestReflexSelectionPreservesTheUsersResolvedConnection(t *testing.T) {
	base := runtimeprovision.ActivationConfig{ReflexModelID: "small-model", ReflexReasoningEffort: "low", ProviderAPIKey: "operator"}
	employer := &chatGPTTestEmployer{kind: "human"}
	api := &userConnectionFixture{selected: modelconnections.Selection{Kind: "api", ConnectionID: "mine"}, exists: true}
	got, err := userModelActivation(api, employer, nil, nil)(context.Background(), "pa", base)
	if err != nil || got.APIConnectionHumanID != "current-human" || got.APIConnectionID != "mine" || got.ProviderAPIKey != "" || got.ReflexModelID != base.ReflexModelID || got.ReflexReasoningEffort != base.ReflexReasoningEffort {
		t.Fatal("reflex changed user API connection or lost selection", err)
	}
	native := &chatGPTRuntime{connections: &chatGPTTestStore{status: chatgpt.Status{Connected: true, ConnectionID: "connection", AccountID: "actual-account", Selection: chatgpt.Selection{Model: "gpt-6-astra", Effort: "medium"}}}, employers: employer}
	got, err = native.activation(context.Background(), "pa", base)
	if err != nil || got.ChatGPTConnectionID != "connection" || got.ModelAccountScope != "actual-account" || got.ProviderAPIKey != "" || got.ModelID != "gpt-6-astra" || got.ModelReasoningEffort != "medium" || got.ReflexModelID != base.ReflexModelID || got.ReflexReasoningEffort != "low" {
		t.Fatal("reflex changed ChatGPT account/parent selection", err)
	}
	if base.ProviderAPIKey != "operator" || base.ReflexModelID != "small-model" {
		t.Fatal("mutated activation input")
	}
}
