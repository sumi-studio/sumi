package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

type fakeFirebaseProviderLifecycle struct {
	mu            sync.Mutex
	accounts      map[string]firebaseProviderAccount
	getErrors     map[int]error
	deleteErr     error
	leaveProvider bool
	getCalls      int
	deleteCalls   int
}

func (f *fakeFirebaseProviderLifecycle) ProviderAccount(_ context.Context, uid string) (firebaseProviderAccount, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	if err := f.getErrors[f.getCalls]; err != nil {
		return firebaseProviderAccount{}, err
	}
	account, ok := f.accounts[uid]
	if !ok {
		return firebaseProviderAccount{}, errors.New("missing Firebase account")
	}
	copy := account
	copy.ProviderSubjects = make(map[string]string, len(account.ProviderSubjects))
	for provider, subject := range account.ProviderSubjects {
		copy.ProviderSubjects[provider] = subject
	}
	return copy, nil
}

func (f *fakeFirebaseProviderLifecycle) DeleteProvider(_ context.Context, uid, provider string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls++
	if !f.leaveProvider {
		account := f.accounts[uid]
		delete(account.ProviderSubjects, provider)
		f.accounts[uid] = account
	}
	return f.deleteErr
}

func TestProviderUnlinkIsBackendOwnedAndCountsOnlyProvedMethods(t *testing.T) {
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
	providers := &fakeFirebaseProviderLifecycle{accounts: map[string]firebaseProviderAccount{
		"unlink-uid": {
			UID: "unlink-uid", EmailProvider: true,
			ProviderSubjects: map[string]string{"github.com": "github-subject", "facebook.com": "unsupported"},
		},
	}}
	controller := newKosekiAuthFlowController(store, "local", providers)
	now := time.Now().UTC()
	controller.clock = func() time.Time { return now }
	claims := agentevents.UserSessionClaims{UserID: registered.HumanID, PersonalityAgentID: registered.AgentID, TenantID: "local"}
	request := agentevents.StartProviderOperationRequest{Provider: "github.com", Operation: "unlink", DecisionPath: "notice_action", Nonce: controllerNonce(t), IDToken: "verified"}

	stale := agentevents.FirebaseIdentity{
		UID: "unlink-uid", AuthTime: now.Add(-6 * time.Minute), SignInProvider: "password",
		Email: "human@example.com", EmailVerified: true,
		ProviderSubjects: map[string][]string{"email": {"human@example.com"}, "github.com": {"github-subject"}},
	}
	if _, err := controller.StartProviderOperation(ctx, claims, request, stale); !errors.Is(err, agentevents.ErrBrowserAuthRecentReauth) {
		t.Fatalf("stale reauth: %v", err)
	}

	sameMethod := stale
	sameMethod.AuthTime = now
	sameMethod.SignInProvider = "github.com"
	if _, err := controller.StartProviderOperation(ctx, claims, request, sameMethod); !errors.Is(err, agentevents.ErrBrowserAuthRecentReauth) {
		t.Fatalf("same-method reauth: %v", err)
	}

	profileOnly := stale
	profileOnly.AuthTime = now
	profileOnly.SignInProvider = "google.com"
	profileOnly.ProviderSubjects = map[string][]string{"google.com": {"stale-google-subject"}, "github.com": {"github-subject"}}
	if _, err := controller.StartProviderOperation(ctx, claims, request, profileOnly); !errors.Is(err, agentevents.ErrBrowserAuthLastMethod) {
		t.Fatalf("profile email, Firebase anchor, and unsupported provider counted as methods: %v", err)
	}
	if providers.deleteCalls != 0 {
		t.Fatalf("last-method guard performed %d Admin deletes", providers.deleteCalls)
	}

	normalized, err := koseki.NormalizeEmail("human@example.com")
	if err != nil {
		t.Fatal(err)
	}
	proofNonce := controllerNonce(t)
	proof, err := store.StartAuthFlow(ctx, koseki.StartAuthFlowRequest{
		Intent: koseki.IntentSignIn, Channel: koseki.ChannelEmailLink,
		ExpectedProvider: "password", NormalizedEmail: normalized,
		Continuation: "/direct-chat", Nonce: proofNonce, TTL: 10 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ResolveAuthProof(ctx, proof.FlowID, proofNonce, koseki.VerifiedIdentity{
		FirebaseUID: "unlink-uid", NormalizedEmail: normalized, EmailVerified: true, SignInProvider: "password",
	}); err != nil {
		t.Fatal(err)
	}
	request.Nonce = controllerNonce(t)
	result, err := controller.StartProviderOperation(ctx, claims, request, profileOnly)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "provider_unlinked" || result.ClientOperation != "" || !result.NoticeRequired || providers.deleteCalls != 1 {
		t.Fatalf("backend unlink: result=%+v deletes=%d", result, providers.deleteCalls)
	}
}

func TestProviderUnlinkReconcilesAmbiguousAdminSuccessAndSameNonceRetry(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store := koseki.New(pool)
	registered, err := store.AutoRegister(ctx, "firebase", "ambiguous-unlink-uid")
	if err != nil {
		t.Fatal(err)
	}
	for provider, subject := range map[string]string{"google.com": "google-subject", "github.com": "github-subject"} {
		if err := store.BindCredential(ctx, provider, subject, registered.HumanID); err != nil {
			t.Fatal(err)
		}
	}
	providers := &fakeFirebaseProviderLifecycle{
		accounts: map[string]firebaseProviderAccount{"ambiguous-unlink-uid": {
			UID: "ambiguous-unlink-uid", ProviderSubjects: map[string]string{"google.com": "google-subject", "github.com": "github-subject"},
		}},
		deleteErr: errors.New("lost Admin response"),
	}
	controller := newKosekiAuthFlowController(store, "local", providers)
	controller.clock = func() time.Time { return time.Now().UTC() }
	claims := agentevents.UserSessionClaims{UserID: registered.HumanID, PersonalityAgentID: registered.AgentID, TenantID: "local"}
	request := agentevents.StartProviderOperationRequest{Provider: "github.com", Operation: "unlink", DecisionPath: "account_settings", Nonce: controllerNonce(t)}
	identity := agentevents.FirebaseIdentity{
		UID: "ambiguous-unlink-uid", AuthTime: time.Now().UTC(), SignInProvider: "google.com",
		ProviderSubjects: map[string][]string{"google.com": {"google-subject"}, "github.com": {"github-subject"}},
	}
	first, err := controller.StartProviderOperation(ctx, claims, request, identity)
	if err != nil || first.Outcome != "provider_unlinked" {
		t.Fatalf("ambiguous Admin success: %+v %v", first, err)
	}
	second, err := controller.StartProviderOperation(ctx, claims, request, identity)
	if err != nil || !reflect.DeepEqual(first, second) || providers.deleteCalls != 1 {
		t.Fatalf("same-nonce retry: first=%+v second=%+v err=%v deletes=%d", first, second, err, providers.deleteCalls)
	}
	status, err := controller.StatusProviderOperation(ctx, claims, agentevents.ProviderOperationStatusRequest{OperationID: first.OperationID, Nonce: request.Nonce})
	if err != nil || status.Outcome != "provider_unlinked" || status.Status != "completed" {
		t.Fatalf("terminal recovery: %+v %v", status, err)
	}
}

func TestProviderUnlinkKeepsFenceUntilIndeterminatePostcheckReconciles(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store := koseki.New(pool)
	registered, err := store.AutoRegister(ctx, "firebase", "postcheck-unlink-uid")
	if err != nil {
		t.Fatal(err)
	}
	for provider, subject := range map[string]string{"google.com": "google-subject", "github.com": "github-subject"} {
		if err := store.BindCredential(ctx, provider, subject, registered.HumanID); err != nil {
			t.Fatal(err)
		}
	}
	providers := &fakeFirebaseProviderLifecycle{
		accounts: map[string]firebaseProviderAccount{"postcheck-unlink-uid": {
			UID: "postcheck-unlink-uid", ProviderSubjects: map[string]string{"google.com": "google-subject", "github.com": "github-subject"},
		}},
		getErrors: map[int]error{2: errors.New("indeterminate postcheck")},
	}
	controller := newKosekiAuthFlowController(store, "local", providers)
	now := time.Now().UTC()
	controller.clock = func() time.Time { return now }
	claims := agentevents.UserSessionClaims{UserID: registered.HumanID, PersonalityAgentID: registered.AgentID, TenantID: "local"}
	request := agentevents.StartProviderOperationRequest{Provider: "github.com", Operation: "unlink", DecisionPath: "account_settings", Nonce: controllerNonce(t)}
	identity := agentevents.FirebaseIdentity{UID: "postcheck-unlink-uid", AuthTime: now, SignInProvider: "google.com", ProviderSubjects: map[string][]string{"google.com": {"google-subject"}}}
	if _, err := controller.StartProviderOperation(ctx, claims, request, identity); !errors.Is(err, agentevents.ErrBrowserAuthProviderUnavailable) {
		t.Fatalf("indeterminate postcheck: %v", err)
	}
	var operationID string
	if err := pool.QueryRow(ctx, "SELECT operation_id FROM provider_operations WHERE firebase_uid=$1 AND status='pending'", "postcheck-unlink-uid").Scan(&operationID); err != nil {
		t.Fatalf("pending fence: %v", err)
	}
	pending, err := controller.StatusProviderOperation(ctx, claims, agentevents.ProviderOperationStatusRequest{OperationID: operationID, Nonce: request.Nonce})
	if err != nil || pending.Outcome != "provider_operation_pending" || pending.ClientOperation != "" {
		t.Fatalf("pending recovery: %+v %v", pending, err)
	}
	recovered, err := controller.StartProviderOperation(ctx, claims, request, identity)
	if err != nil || recovered.OperationID != operationID || recovered.Outcome != "provider_unlinked" || providers.deleteCalls != 1 {
		t.Fatalf("postcheck recovery: %+v %v deletes=%d", recovered, err, providers.deleteCalls)
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
	controller := newKosekiAuthFlowController(store, "local", nil)
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
	controller := newKosekiAuthFlowController(store, "local", nil)
	claims := agentevents.UserSessionClaims{TenantID: "local", UserID: owner.HumanID, PersonalityAgentID: owner.AgentID}
	otherClaims := agentevents.UserSessionClaims{TenantID: "local", UserID: other.HumanID, PersonalityAgentID: other.AgentID}

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
	pendingUnlink, err := store.BeginProviderOperation(ctx, other.HumanID, "status-controller-other", "google.com", "unlink", "account_settings", pendingUnlinkNonce)
	if err != nil {
		t.Fatal(err)
	}
	pendingUnlinkResult, err := controller.StatusProviderOperation(ctx, otherClaims, agentevents.ProviderOperationStatusRequest{OperationID: pendingUnlink.OperationID, Nonce: pendingUnlinkNonce})
	if err != nil || pendingUnlinkResult.Outcome != "provider_operation_pending" || pendingUnlinkResult.ClientOperation != "" {
		t.Fatalf("pending unlink result: %+v %v", pendingUnlinkResult, err)
	}
	if _, err := controller.CompleteProviderOperation(ctx, otherClaims, agentevents.CompleteProviderOperationRequest{
		OperationID: pendingUnlink.OperationID, Nonce: pendingUnlinkNonce,
	}, agentevents.FirebaseIdentity{UID: "status-controller-other"}); !errors.Is(err, agentevents.ErrBrowserAuthFlowInvalid) {
		t.Fatalf("browser completed backend unlink: %v", err)
	}
	if _, err := controller.FailProviderOperation(ctx, otherClaims, agentevents.FailProviderOperationRequest{
		OperationID: pendingUnlink.OperationID, Nonce: pendingUnlinkNonce, Outcome: "cancelled",
	}); !errors.Is(err, agentevents.ErrBrowserAuthFlowInvalid) {
		t.Fatalf("browser released backend unlink fence: %v", err)
	}
	if _, err := store.FailProviderOperation(ctx, pendingUnlink.OperationID, pendingUnlinkNonce, "cancelled"); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.StatusProviderOperation(ctx, claims, agentevents.ProviderOperationStatusRequest{OperationID: pending.OperationID, Nonce: controllerNonce(t)}); !errors.Is(err, agentevents.ErrBrowserAuthFlowInvalid) {
		t.Fatalf("wrong nonce: %v", err)
	}
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
