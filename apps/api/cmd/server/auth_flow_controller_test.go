package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"reflect"
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
	if started.CreatedAt.IsZero() || started.CompletionTokenNotBefore.Before(started.CreatedAt) {
		t.Fatalf("operation token boundary: %+v", started)
	}

	// A refreshed token that still contains the provider cannot claim success.
	complete := agentevents.CompleteProviderOperationRequest{OperationID: started.OperationID, Nonce: request.Nonce, IDToken: "refreshed"}
	fresh.IssuedAt = started.CompletionTokenNotBefore
	if _, err := controller.CompleteProviderOperation(ctx, claims, complete, fresh); !errors.Is(err, agentevents.ErrBrowserAuthFlowProof) {
		t.Fatalf("provider still present: %v", err)
	}

	refreshed := fresh
	refreshed.ProviderSubjects = map[string][]string{"password": {"human@example.com"}}
	refreshed.IssuedAt = started.CompletionTokenNotBefore.Add(-time.Second)
	if _, err := controller.CompleteProviderOperation(ctx, claims, complete, refreshed); !errors.Is(err, agentevents.ErrBrowserAuthFlowProof) {
		t.Fatalf("stale pre-operation unlink snapshot: %v", err)
	}

	refreshed.IssuedAt = started.CompletionTokenNotBefore
	result, err := controller.CompleteProviderOperation(ctx, claims, complete, refreshed)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "provider_unlinked" || !result.NoticeRequired {
		t.Fatalf("completion: %+v", result)
	}
}

func TestProviderLinkRejectsPreOperationTokenAndAcceptsForcedRefresh(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store := koseki.New(pool)
	registered, err := store.AutoRegister(ctx, "firebase", "link-uid")
	if err != nil {
		t.Fatal(err)
	}
	controller := newKosekiAuthFlowController(store, "local")
	claims := agentevents.UserSessionClaims{UserID: registered.HumanID, PersonalityAgentID: registered.AgentID, TenantID: "local"}
	request := agentevents.StartProviderOperationRequest{
		Provider: "github.com", Operation: "link", DecisionPath: "same_email_recovery",
		Nonce: controllerNonce(t), IDToken: "before-link",
	}
	initial := agentevents.FirebaseIdentity{UID: "link-uid", IssuedAt: time.Now().Add(-time.Minute)}
	started, err := controller.StartProviderOperation(ctx, claims, request, initial)
	if err != nil {
		t.Fatal(err)
	}
	complete := agentevents.CompleteProviderOperationRequest{OperationID: started.OperationID, Nonce: request.Nonce, IDToken: "completion"}
	linkedSnapshot := agentevents.FirebaseIdentity{
		UID: "link-uid", SignInProvider: "github.com",
		ProviderSubjects: map[string][]string{"github.com": {"new-github-subject"}},
		IssuedAt:         started.CompletionTokenNotBefore.Add(-time.Second),
	}
	if _, err := controller.CompleteProviderOperation(ctx, claims, complete, linkedSnapshot); !errors.Is(err, agentevents.ErrBrowserAuthFlowProof) {
		t.Fatalf("stale pre-operation link snapshot: %v", err)
	}

	linkedSnapshot.IssuedAt = started.CompletionTokenNotBefore
	result, err := controller.CompleteProviderOperation(ctx, claims, complete, linkedSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "provider_linked" || !result.NoticeRequired {
		t.Fatalf("forced-refresh completion: %+v", result)
	}
}

func TestCompletionTokenNotBeforeHandlesFirebaseSecondPrecision(t *testing.T) {
	exact := time.Date(2026, 8, 1, 12, 0, 0, 0, time.FixedZone("offset", 9*60*60))
	if got := completionTokenNotBefore(exact); !got.Equal(exact.UTC()) {
		t.Fatalf("exact second: got %s want %s", got, exact.UTC())
	}
	fractional := exact.Add(250 * time.Millisecond)
	want := exact.UTC().Add(time.Second)
	if got := completionTokenNotBefore(fractional); !got.Equal(want) {
		t.Fatalf("fractional second: got %s want %s", got, want)
	}
}

func TestProviderOperationStatusMapsDurableSemanticOutcomes(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store := koseki.New(pool)
	owner, err := store.AutoRegister(ctx, "firebase", "status-controller-owner")
	if err != nil {
		t.Fatal(err)
	}
	other, err := store.AutoRegister(ctx, "firebase", "status-controller-other")
	if err != nil {
		t.Fatal(err)
	}
	controller := newKosekiAuthFlowController(store, "local")
	claims := agentevents.UserSessionClaims{TenantID: "local", UserID: owner.HumanID, PersonalityAgentID: owner.AgentID}

	pendingNonce := controllerNonce(t)
	pending, err := store.BeginProviderOperation(ctx, owner.HumanID, "status-controller-owner", "github.com", "link", "account_settings", pendingNonce)
	if err != nil {
		t.Fatal(err)
	}
	pendingResult, err := controller.StatusProviderOperation(ctx, claims, agentevents.ProviderOperationStatusRequest{OperationID: pending.OperationID, Nonce: pendingNonce})
	if err != nil {
		t.Fatal(err)
	}
	if pendingResult.Provider != "github.com" || pendingResult.Operation != "link" || pendingResult.Status != "pending" ||
		pendingResult.Outcome != "client_operation_required" || pendingResult.ClientOperation != "firebase_link_with_credential" ||
		pendingResult.NoticeRequired || pendingResult.CompletedAt != nil {
		t.Fatalf("pending result: %+v", pendingResult)
	}
	pendingUnlinkNonce := controllerNonce(t)
	pendingUnlink, err := store.BeginProviderOperation(ctx, owner.HumanID, "status-controller-owner", "google.com", "unlink", "account_settings", pendingUnlinkNonce)
	if err != nil {
		t.Fatal(err)
	}
	pendingUnlinkResult, err := controller.StatusProviderOperation(ctx, claims, agentevents.ProviderOperationStatusRequest{OperationID: pendingUnlink.OperationID, Nonce: pendingUnlinkNonce})
	if err != nil || pendingUnlinkResult.ClientOperation != "firebase_unlink_provider" {
		t.Fatalf("pending unlink result: %+v %v", pendingUnlinkResult, err)
	}
	if _, err := controller.StatusProviderOperation(ctx, claims, agentevents.ProviderOperationStatusRequest{OperationID: pending.OperationID, Nonce: controllerNonce(t)}); !errors.Is(err, agentevents.ErrBrowserAuthFlowInvalid) {
		t.Fatalf("wrong nonce: %v", err)
	}
	otherClaims := agentevents.UserSessionClaims{TenantID: "local", UserID: other.HumanID, PersonalityAgentID: other.AgentID}
	if _, err := controller.StatusProviderOperation(ctx, otherClaims, agentevents.ProviderOperationStatusRequest{OperationID: pending.OperationID, Nonce: pendingNonce}); !errors.Is(err, agentevents.ErrBrowserAuthFlowProof) {
		t.Fatalf("wrong Human: %v", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE provider_operations SET expires_at=now()-interval '1 second' WHERE operation_id=$1", pending.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.StatusProviderOperation(ctx, claims, agentevents.ProviderOperationStatusRequest{OperationID: pending.OperationID, Nonce: pendingNonce}); !errors.Is(err, agentevents.ErrBrowserAuthFlowExpired) {
		t.Fatalf("expired pending: %v", err)
	}

	linkNonce := controllerNonce(t)
	link, err := store.BeginProviderOperation(ctx, owner.HumanID, "status-controller-owner", "github.com", "link", "same_email_recovery", linkNonce)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteProviderLink(ctx, link.OperationID, linkNonce, "status-controller-owner", "status-controller-github"); err != nil {
		t.Fatal(err)
	}
	linked, err := controller.StatusProviderOperation(ctx, claims, agentevents.ProviderOperationStatusRequest{OperationID: link.OperationID, Nonce: linkNonce})
	if err != nil || linked.Outcome != "provider_linked" || linked.Status != "completed" || !linked.NoticeRequired || linked.CompletedAt == nil {
		t.Fatalf("linked result: %+v %v", linked, err)
	}
	repeatedLinked, err := controller.StatusProviderOperation(ctx, claims, agentevents.ProviderOperationStatusRequest{OperationID: link.OperationID, Nonce: linkNonce})
	if err != nil || !reflect.DeepEqual(linked, repeatedLinked) {
		t.Fatalf("repeated linked result: %+v / %+v %v", linked, repeatedLinked, err)
	}

	alreadyNonce := controllerNonce(t)
	already, err := store.BeginProviderOperation(ctx, owner.HumanID, "status-controller-owner", "github.com", "link", "notice_action", alreadyNonce)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteProviderLink(ctx, already.OperationID, alreadyNonce, "status-controller-owner", "status-controller-github"); err != nil {
		t.Fatal(err)
	}
	alreadyLinked, err := controller.StatusProviderOperation(ctx, claims, agentevents.ProviderOperationStatusRequest{OperationID: already.OperationID, Nonce: alreadyNonce})
	if err != nil || alreadyLinked.Outcome != "provider_already_linked" || alreadyLinked.NoticeRequired {
		t.Fatalf("already-linked result: %+v %v", alreadyLinked, err)
	}

	unlinkNonce := controllerNonce(t)
	unlink, err := store.BeginProviderOperation(ctx, owner.HumanID, "status-controller-owner", "github.com", "unlink", "account_settings", unlinkNonce)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteProviderUnlink(ctx, unlink.OperationID, unlinkNonce, "status-controller-owner", "status-controller-github"); err != nil {
		t.Fatal(err)
	}
	unlinked, err := controller.StatusProviderOperation(ctx, claims, agentevents.ProviderOperationStatusRequest{OperationID: unlink.OperationID, Nonce: unlinkNonce})
	if err != nil || unlinked.Outcome != "provider_unlinked" || !unlinked.NoticeRequired {
		t.Fatalf("unlinked result: %+v %v", unlinked, err)
	}

	failNonce := controllerNonce(t)
	failedOperation, err := store.BeginProviderOperation(ctx, owner.HumanID, "status-controller-owner", "google.com", "link", "provider_sign_in", failNonce)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.FailProviderOperation(ctx, failedOperation.OperationID, failNonce, "firebase_operation_failed"); err != nil {
		t.Fatal(err)
	}
	failed, err := controller.StatusProviderOperation(ctx, claims, agentevents.ProviderOperationStatusRequest{OperationID: failedOperation.OperationID, Nonce: failNonce})
	if err != nil || failed.Status != "failed" || failed.Outcome != "firebase_operation_failed" || failed.NoticeRequired {
		t.Fatalf("failed result: %+v %v", failed, err)
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
