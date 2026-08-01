package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

func TestProviderUnlinkRequiresOtherRecentMethodAndVerifiedCompletion(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store := koseki.New(pool)
	registered, err := store.AutoRegister(ctx, "firebase", "unlink-uid")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BindCredential(ctx, "github.com", "github-subject", registered.HumanID); err != nil {
		t.Fatal(err)
	}
	controller := newKosekiAuthFlowController(store, "local")
	now := time.Now().UTC()
	controller.clock = func() time.Time { return now }
	claims := agentevents.UserSessionClaims{UserID: registered.HumanID, PersonalityAgentID: registered.AgentID, TenantID: "local"}
	request := agentevents.StartProviderOperationRequest{Provider: "github.com", Operation: "unlink", DecisionPath: "notice_action", Nonce: controllerNonce(t), IDToken: "verified"}

	stale := agentevents.FirebaseIdentity{UID: "unlink-uid", AuthTime: now.Add(-6 * time.Minute), SignInProvider: "password", ProviderSubjects: map[string][]string{"password": {"human@example.com"}, "github.com": {"github-subject"}}}
	if _, err := controller.StartProviderOperation(ctx, claims, request, stale); !errors.Is(err, agentevents.ErrBrowserAuthRecentReauth) {
		t.Fatalf("stale reauth: %v", err)
	}

	sameMethod := stale
	sameMethod.AuthTime = now
	sameMethod.SignInProvider = "github.com"
	if _, err := controller.StartProviderOperation(ctx, claims, request, sameMethod); !errors.Is(err, agentevents.ErrBrowserAuthRecentReauth) {
		t.Fatalf("same-method reauth: %v", err)
	}

	lastMethod := stale
	lastMethod.AuthTime = now
	lastMethod.ProviderSubjects = map[string][]string{"github.com": {"github-subject"}}
	if _, err := controller.StartProviderOperation(ctx, claims, request, lastMethod); !errors.Is(err, agentevents.ErrBrowserAuthLastMethod) {
		t.Fatalf("last method: %v", err)
	}

	request.Nonce = controllerNonce(t)
	fresh := stale
	fresh.AuthTime = now
	started, err := controller.StartProviderOperation(ctx, claims, request, fresh)
	if err != nil {
		t.Fatal(err)
	}
	if started.ClientOperation != "firebase_unlink_provider" {
		t.Fatalf("client operation: %+v", started)
	}

	// A refreshed token that still contains the provider cannot claim success.
	complete := agentevents.CompleteProviderOperationRequest{OperationID: started.OperationID, Nonce: request.Nonce, IDToken: "refreshed"}
	if _, err := controller.CompleteProviderOperation(ctx, claims, complete, fresh); !errors.Is(err, agentevents.ErrBrowserAuthFlowProof) {
		t.Fatalf("provider still present: %v", err)
	}

	refreshed := fresh
	refreshed.ProviderSubjects = map[string][]string{"password": {"human@example.com"}}
	result, err := controller.CompleteProviderOperation(ctx, claims, complete, refreshed)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "provider_unlinked" || !result.NoticeRequired {
		t.Fatalf("completion: %+v", result)
	}
}

func controllerNonce(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}
