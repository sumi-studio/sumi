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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/transfersession"
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
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	registered, err := store.AutoRegister(ctx, "firebase", "unlink-uid")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BindCredential(ctx, "github.com", "github-subject", registered.HumanID); err != nil {
		t.Fatal(err)
	}
	providers := &fakeFirebaseProviderLifecycle{accounts: map[string]firebaseProviderAccount{
		"unlink-uid": {
			UID: "unlink-uid", EmailVerified: true,
			ProviderSubjects: map[string]string{"github.com": "github-subject", "facebook.com": "unsupported"},
		},
	}}
	controller := newKosekiAuthFlowController(store, "local", providers)
	// Proved email counts as a method only while the email channel is enabled.
	controller.email = &emailCodeController{}
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
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
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
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
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

func TestOrphanedProviderUnlinkIsSettledFromLiveStateInsteadOfFencingForever(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	const uid = "orphan-controller-uid"
	registered, err := store.AutoRegister(ctx, "firebase", uid)
	if err != nil {
		t.Fatal(err)
	}
	for provider, subject := range map[string]string{"google.com": "google-subject", "github.com": "github-subject"} {
		if err := store.BindCredential(ctx, provider, subject, registered.HumanID); err != nil {
			t.Fatal(err)
		}
	}
	providers := &fakeFirebaseProviderLifecycle{accounts: map[string]firebaseProviderAccount{uid: {
		UID: uid, ProviderSubjects: map[string]string{"google.com": "google-subject", "github.com": "github-subject"},
	}}}
	controller := newKosekiAuthFlowController(store, "local", providers)
	now := time.Now().UTC()
	controller.clock = func() time.Time { return now }
	claims := agentevents.UserSessionClaims{UserID: registered.HumanID, PersonalityAgentID: registered.AgentID, TenantID: "local"}
	identity := agentevents.FirebaseIdentity{UID: uid, AuthTime: now, SignInProvider: "google.com", ProviderSubjects: map[string][]string{"google.com": {"google-subject"}}}
	expire := func(interval string) {
		t.Helper()
		if _, err := pool.Exec(ctx, "UPDATE provider_operations SET expires_at=now()-$2::interval WHERE firebase_uid=$1 AND status='pending'", uid, interval); err != nil {
			t.Fatal(err)
		}
	}

	// An unlink whose remote delete may or may not have happened, and whose
	// browser record (the nonce) is gone.
	lost, err := store.BeginProviderOperation(ctx, registered.HumanID, uid, "github.com", "unlink", "account_settings", controllerNonce(t))
	if err != nil {
		t.Fatal(err)
	}
	linkRequest := agentevents.StartProviderOperationRequest{Provider: "github.com", Operation: "link", DecisionPath: "account_settings", Nonce: controllerNonce(t)}
	if _, err := controller.StartProviderOperation(ctx, claims, linkRequest, identity); !errors.Is(err, agentevents.ErrBrowserAuthProviderPending) {
		t.Fatalf("live unlink did not fence: %v", err)
	}
	expire("1 second")
	if _, err := controller.StartProviderOperation(ctx, claims, linkRequest, identity); !errors.Is(err, agentevents.ErrBrowserAuthProviderPending) {
		t.Fatalf("present provider inside settle grace released its fence: %v", err)
	}

	// The late remote delete becomes visible and is reconciled at once.
	providers.mu.Lock()
	delete(providers.accounts[uid].ProviderSubjects, "github.com")
	providers.mu.Unlock()
	started, err := controller.StartProviderOperation(ctx, claims, linkRequest, identity)
	if err != nil || started.Outcome != "client_operation_required" {
		t.Fatalf("new change after settled orphan: %+v %v", started, err)
	}
	var lostStatus, lostOutcome string
	if err := pool.QueryRow(ctx, "SELECT status, terminal_outcome FROM provider_operations WHERE operation_id=$1", lost.OperationID).Scan(&lostStatus, &lostOutcome); err != nil ||
		lostStatus != "completed" || lostOutcome != "unlinked" {
		t.Fatalf("orphan settlement: %s %s %v", lostStatus, lostOutcome, err)
	}
	if providers.deleteCalls != 0 {
		t.Fatalf("settlement issued %d Admin deletes", providers.deleteCalls)
	}

	// A nonce holder replaying an expired unlink inside the settle grace sees
	// the honest "still settling" answer — the server has not yet decided the
	// remote outcome and never re-runs the original delete through the replay.
	if _, err := controller.FailProviderOperation(ctx, claims, agentevents.FailProviderOperationRequest{OperationID: started.OperationID, Nonce: linkRequest.Nonce, Outcome: "cancelled"}); err != nil {
		t.Fatal(err)
	}
	unlinkRequest := agentevents.StartProviderOperationRequest{Provider: "google.com", Operation: "unlink", DecisionPath: "account_settings", Nonce: controllerNonce(t)}
	held, err := store.BeginProviderOperation(ctx, registered.HumanID, uid, "google.com", "unlink", "account_settings", unlinkRequest.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	identity.SignInProvider = "password"
	identity.Email, identity.EmailVerified = "human@example.com", true
	identity.ProviderSubjects = map[string][]string{"email": {"human@example.com"}}
	expire("1 second")
	if _, err := controller.StartProviderOperation(ctx, claims, unlinkRequest, identity); !errors.Is(err, agentevents.ErrBrowserAuthProviderPending) {
		t.Fatalf("expired unlink inside grace: %v", err)
	}
	if providers.deleteCalls != 0 {
		t.Fatalf("inside-grace settle issued %d Admin deletes", providers.deleteCalls)
	}

	// The user relinked GitHub from another browser while the Google orphan
	// was fencing: the account now carries a different GitHub subject. Past
	// the grace the unlink's recorded subject is still linked remotely and
	// other methods remain, so the server drives one bounded reconcile delete
	// of its own: the original intent is fulfilled without touching the
	// relinked identity, the operation completes, the fence releases.
	providers.mu.Lock()
	providers.accounts[uid].ProviderSubjects["github.com"] = "github-relinked-subject"
	providers.mu.Unlock()
	expire("3 minutes")
	replayed, err := controller.StartProviderOperation(ctx, claims, unlinkRequest, identity)
	if err != nil || replayed.OperationID != held.OperationID || replayed.Outcome != "provider_unlinked" {
		t.Fatalf("expired unlink reconcile: %+v %v", replayed, err)
	}
	status, err := controller.StatusProviderOperation(ctx, claims, agentevents.ProviderOperationStatusRequest{OperationID: held.OperationID, Nonce: unlinkRequest.Nonce})
	if err != nil || status.Status != "completed" || status.Outcome != "provider_unlinked" {
		t.Fatalf("reconciled unlink status: %+v %v", status, err)
	}
	if providers.deleteCalls != 1 {
		t.Fatalf("reconcile issued %d Admin deletes", providers.deleteCalls)
	}
	if subject, err := store.ActiveProviderSubject(ctx, registered.HumanID, "google.com"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("reconciled credential still active: %q %v", subject, err)
	}
}

func TestOrphanedProviderUnlinkReconcileDeleteFailureAndSubjectDrift(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	const uid = "orphan-reconcile-uid"
	registered, err := store.AutoRegister(ctx, "firebase", uid)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BindCredential(ctx, "github.com", "github-subject", registered.HumanID); err != nil {
		t.Fatal(err)
	}
	if err := store.BindCredential(ctx, "google.com", "google-subject", registered.HumanID); err != nil {
		t.Fatal(err)
	}
	providers := &fakeFirebaseProviderLifecycle{
		accounts: map[string]firebaseProviderAccount{uid: {
			UID: uid, ProviderSubjects: map[string]string{
				"github.com": "github-subject", "google.com": "google-subject",
			},
		}},
		// The remote delete cannot confirm removal: the provider stays.
		leaveProvider: true,
	}
	controller := newKosekiAuthFlowController(store, "local", providers)
	controller.clock = func() time.Time { return time.Now().UTC() }
	claims := agentevents.UserSessionClaims{UserID: registered.HumanID, PersonalityAgentID: registered.AgentID, TenantID: "local"}
	expire := func(interval string) {
		t.Helper()
		if _, err := pool.Exec(ctx, "UPDATE provider_operations SET expires_at=now()-$2::interval WHERE firebase_uid=$1 AND status='pending'", uid, interval); err != nil {
			t.Fatal(err)
		}
	}

	orphan, err := store.BeginProviderOperation(ctx, registered.HumanID, uid, "github.com", "unlink", "account_settings", controllerNonce(t))
	if err != nil {
		t.Fatal(err)
	}
	expire("3 minutes")
	linkRequest := agentevents.StartProviderOperationRequest{Provider: "google.com", Operation: "link", DecisionPath: "account_settings", Nonce: controllerNonce(t)}
	// The reconcile delete is issued, but the postcheck still shows the
	// provider: the operation ends as expired, the fence releases, and the
	// credential survives — nothing was removed remotely.
	started, err := controller.StartProviderOperation(ctx, claims, linkRequest, agentevents.FirebaseIdentity{UID: uid})
	if err != nil {
		t.Fatalf("new change after expired orphan: %v", err)
	}
	var status, outcome string
	if err := pool.QueryRow(ctx, "SELECT status, terminal_outcome FROM provider_operations WHERE operation_id=$1", orphan.OperationID).Scan(&status, &outcome); err != nil ||
		status != "failed" || outcome != "expired" {
		t.Fatalf("unconfirmable reconcile: %s %s %v", status, outcome, err)
	}
	if providers.deleteCalls != 1 {
		t.Fatalf("reconcile issued %d Admin deletes", providers.deleteCalls)
	}
	if subject, err := store.ActiveProviderSubject(ctx, registered.HumanID, "github.com"); err != nil || subject != "github-subject" {
		t.Fatalf("credential disabled although provider survived: %q %v", subject, err)
	}

	// The recorded subject is gone but a different identity now occupies the
	// provider remotely: the unlink's target is still removed, so it
	// completes — and disables the stale credential — without another delete.
	if _, err := store.FailProviderOperation(ctx, started.OperationID, linkRequest.Nonce, "cancelled"); err != nil {
		t.Fatal(err)
	}
	second, err := store.BeginProviderOperation(ctx, registered.HumanID, uid, "github.com", "unlink", "account_settings", controllerNonce(t))
	if err != nil {
		t.Fatal(err)
	}
	expire("3 minutes")
	providers.mu.Lock()
	providers.accounts[uid].ProviderSubjects["github.com"] = "github-other-subject"
	providers.mu.Unlock()
	next := agentevents.StartProviderOperationRequest{Provider: "google.com", Operation: "link", DecisionPath: "account_settings", Nonce: controllerNonce(t)}
	if _, err := controller.StartProviderOperation(ctx, claims, next, agentevents.FirebaseIdentity{UID: uid}); err != nil {
		t.Fatalf("new change after drifted orphan: %v", err)
	}
	if err := pool.QueryRow(ctx, "SELECT status, terminal_outcome FROM provider_operations WHERE operation_id=$1", second.OperationID).Scan(&status, &outcome); err != nil ||
		status != "completed" || outcome != "unlinked" {
		t.Fatalf("subject-drift reconcile: %s %s %v", status, outcome, err)
	}
	if providers.deleteCalls != 1 {
		t.Fatalf("subject drift issued %d extra Admin deletes", providers.deleteCalls-1)
	}
	if subject, err := store.ActiveProviderSubject(ctx, registered.HumanID, "github.com"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("stale credential survived subject drift: %q %v", subject, err)
	}
}

func TestOrphanedProviderUnlinkOnLastMethodFailsWithoutServerDelete(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	const uid = "orphan-last-method-uid"
	registered, err := store.AutoRegister(ctx, "firebase", uid)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BindCredential(ctx, "github.com", "github-subject", registered.HumanID); err != nil {
		t.Fatal(err)
	}
	providers := &fakeFirebaseProviderLifecycle{accounts: map[string]firebaseProviderAccount{uid: {
		UID: uid, ProviderSubjects: map[string]string{"github.com": "github-subject"},
	}}}
	controller := newKosekiAuthFlowController(store, "local", providers)
	claims := agentevents.UserSessionClaims{UserID: registered.HumanID, PersonalityAgentID: registered.AgentID, TenantID: "local"}

	// An orphaned unlink whose target is the account's only usable sign-in
	// method: consent captured at start does not outlive the methods that
	// made it safe, so the server never issues a delete that would strand
	// the account. The orphan ends as last_login_method, the fence releases,
	// and the credential survives.
	orphan, err := store.BeginProviderOperation(ctx, registered.HumanID, uid, "github.com", "unlink", "account_settings", controllerNonce(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "UPDATE provider_operations SET expires_at=now()-interval '3 minutes' WHERE operation_id=$1", orphan.OperationID); err != nil {
		t.Fatal(err)
	}
	linkRequest := agentevents.StartProviderOperationRequest{Provider: "google.com", Operation: "link", DecisionPath: "account_settings", Nonce: controllerNonce(t)}
	started, err := controller.StartProviderOperation(ctx, claims, linkRequest, agentevents.FirebaseIdentity{UID: uid})
	if err != nil || started.Outcome != "client_operation_required" {
		t.Fatalf("new change after last-method orphan: %+v %v", started, err)
	}
	var status, outcome string
	if err := pool.QueryRow(ctx, "SELECT status, terminal_outcome FROM provider_operations WHERE operation_id=$1", orphan.OperationID).Scan(&status, &outcome); err != nil ||
		status != "failed" || outcome != "last_login_method" {
		t.Fatalf("last-method settle: %s %s %v", status, outcome, err)
	}
	if providers.deleteCalls != 0 {
		t.Fatalf("last-method settle issued %d Admin deletes", providers.deleteCalls)
	}
	if subject, err := store.ActiveProviderSubject(ctx, registered.HumanID, "github.com"); err != nil || subject != "github-subject" {
		t.Fatalf("last-method credential was disabled: %q %v", subject, err)
	}
	// The lost nonce is not the link nonce: status stays nonce-bound.
	if _, err := controller.StatusProviderOperation(ctx, claims, agentevents.ProviderOperationStatusRequest{OperationID: orphan.OperationID, Nonce: linkRequest.Nonce}); !errors.Is(err, agentevents.ErrBrowserAuthFlowInvalid) {
		t.Fatalf("orphan status leaked to another nonce: %v", err)
	}
}

func TestProviderMethodsReadLiveAccountAndCountOnlyProvedEmail(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	const uid = "methods-controller-uid"
	registered, err := store.AutoRegister(ctx, "firebase", uid)
	if err != nil {
		t.Fatal(err)
	}
	// A provider is listed only while the remote account carries the exact
	// subject an active credential binds.
	if err := store.BindCredential(ctx, "github.com", "github-subject", registered.HumanID); err != nil {
		t.Fatal(err)
	}
	providers := &fakeFirebaseProviderLifecycle{accounts: map[string]firebaseProviderAccount{uid: {
		UID: uid, EmailVerified: true,
		ProviderSubjects: map[string]string{"github.com": "github-subject", "facebook.com": "unsupported"},
	}}}
	claims := agentevents.UserSessionClaims{UserID: registered.HumanID, PersonalityAgentID: registered.AgentID, TenantID: "local"}

	if _, err := newKosekiAuthFlowController(store, "local", nil).ProviderMethods(ctx, claims); !errors.Is(err, agentevents.ErrBrowserAuthProviderUnavailable) {
		t.Fatalf("methods without Admin authority: %v", err)
	}
	controller := newKosekiAuthFlowController(store, "local", providers)
	controller.email = &emailCodeController{}
	methods, err := controller.ProviderMethods(ctx, claims)
	if err != nil || !reflect.DeepEqual(methods.Providers, []string{"github.com"}) || methods.Email {
		t.Fatalf("unproved email counted: %+v %v", methods, err)
	}
	normalized, err := koseki.NormalizeEmail("methods@example.com")
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
		FirebaseUID: uid, NormalizedEmail: normalized, EmailVerified: true, SignInProvider: "password",
	}); err != nil {
		t.Fatal(err)
	}
	methods, err = controller.ProviderMethods(ctx, claims)
	if err != nil || !reflect.DeepEqual(methods.Providers, []string{"github.com"}) || !methods.Email {
		t.Fatalf("proved email not counted: %+v %v", methods, err)
	}
	// Another browser's removal is visible immediately, and the credential it
	// stranded is retired by the same read.
	providers.mu.Lock()
	delete(providers.accounts[uid].ProviderSubjects, "github.com")
	providers.mu.Unlock()
	methods, err = controller.ProviderMethods(ctx, claims)
	if err != nil || len(methods.Providers) != 0 || !methods.Email {
		t.Fatalf("removed provider still listed: %+v %v", methods, err)
	}
	if subject, err := store.ActiveProviderSubject(ctx, registered.HumanID, "github.com"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("stranded credential not healed: %q %v", subject, err)
	}

	// A remote identity no credential binds is not a usable method and is not
	// listed; a pending unlink suppresses the heal while the saga owns the
	// credential's transition.
	providers.mu.Lock()
	providers.accounts[uid].ProviderSubjects["github.com"] = "github-subject-2"
	providers.mu.Unlock()
	if err := store.BindCredential(ctx, "github.com", "github-subject-2", registered.HumanID); err != nil {
		t.Fatal(err)
	}
	methods, err = controller.ProviderMethods(ctx, claims)
	if err != nil || !reflect.DeepEqual(methods.Providers, []string{"github.com"}) {
		t.Fatalf("rebound provider not listed: %+v %v", methods, err)
	}
	if _, err := store.BeginProviderOperation(ctx, registered.HumanID, uid, "github.com", "unlink", "account_settings", controllerNonce(t)); err != nil {
		t.Fatal(err)
	}
	providers.mu.Lock()
	delete(providers.accounts[uid].ProviderSubjects, "github.com")
	providers.mu.Unlock()
	methods, err = controller.ProviderMethods(ctx, claims)
	if err != nil || len(methods.Providers) != 0 {
		t.Fatalf("mid-flight unlink listed: %+v %v", methods, err)
	}
	if subject, err := store.ActiveProviderSubject(ctx, registered.HumanID, "github.com"); err != nil || subject != "github-subject-2" {
		t.Fatalf("heal raced a pending unlink: %q %v", subject, err)
	}
}

func TestProviderLinkRejectsPreOperationTokenAndAcceptsForcedRefresh(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
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
	replayed, err := controller.StartProviderOperation(ctx, claims, request, initial)
	if err != nil {
		t.Fatalf("terminal same-nonce start: %v", err)
	}
	if replayed.OperationID != started.OperationID || replayed.Outcome != "provider_linked" ||
		replayed.ClientOperation != "" || !replayed.NoticeRequired {
		t.Fatalf("terminal same-nonce start reissued browser mutation: %+v", replayed)
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
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
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

// Without a configured sender the OAuth providers and the registration choice
// keep working; only the email channel itself is off.
func TestProviderRegistrationAndMethodsWithoutEmailSender(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	// The transfer choice gate is independent of the sender: wire the service
	// so the sign-up below must still park at create-account confirmation.
	store.Transfers = transfersession.New(pool, transfersession.Config{})
	providers := &fakeFirebaseProviderLifecycle{accounts: map[string]firebaseProviderAccount{}}
	controller := newKosekiAuthFlowController(store, "local", providers)
	// controller.email stays nil — the sender is not configured.

	// email_code refuses honestly instead of starting a flow that can never
	// deliver.
	if _, err := controller.Start(ctx, agentevents.StartBrowserAuthFlowRequest{
		Intent: "sign_in", Provider: "email_code", Email: "nobody@example.com",
		Continuation: "/direct-chat", Nonce: controllerNonce(t),
	}); !errors.Is(err, agentevents.ErrBrowserEmailUnavailable) {
		t.Fatalf("email start without sender: %v", err)
	}

	// An invited OAuth sign-up reaches the same create-account choice and
	// provisions on confirm — nothing consults the email surface.
	issuer := uuid.Must(uuid.NewV7()).String()
	if _, err := pool.Exec(ctx,
		`INSERT INTO humans(human_id,display_name) VALUES($1,'no-sender issuer')`, issuer); err != nil {
		t.Fatalf("issuer: %v", err)
	}
	normalized, err := koseki.NormalizeEmail("oauth-signup@example.com")
	if err != nil {
		t.Fatal(err)
	}
	_, inviteToken, err := store.IssueEnrollmentInvite(ctx, issuer, normalized, time.Hour)
	if err != nil {
		t.Fatalf("issue invite: %v", err)
	}
	nonce := controllerNonce(t)
	started, err := controller.Start(ctx, agentevents.StartBrowserAuthFlowRequest{
		InviteToken: inviteToken, Intent: "sign_up", Provider: "google.com",
		Continuation: "/direct-chat", Nonce: nonce,
	})
	if err != nil || started.Outcome != "proof_required" || started.FlowID == "" {
		t.Fatalf("provider start without sender: %+v %v", started, err)
	}
	resolved, err := controller.Resolve(ctx, agentevents.ResolveBrowserAuthFlowRequest{
		FlowID: started.FlowID, Nonce: nonce,
	}, agentevents.FirebaseIdentity{
		UID: "oauth-signup-uid", SignInProvider: "google.com",
		Email: "oauth-signup@example.com", EmailVerified: true,
		ProviderSubjects: map[string][]string{"google.com": {"google-subject"}},
		IssuedAt:         time.Now(),
	})
	if err != nil || resolved.Outcome != "confirmation_required" || resolved.NextAction != "create_account" {
		t.Fatalf("provider resolve without sender: %+v %v", resolved, err)
	}
	confirmed, err := controller.Confirm(ctx, agentevents.ConfirmBrowserAuthFlowRequest{
		FlowID: started.FlowID, Nonce: nonce, Action: "create_account",
	})
	if err != nil || confirmed.Outcome != "account_created" {
		t.Fatalf("provider confirm without sender: %+v %v", confirmed, err)
	}

	// An account whose live Firebase profile carries a verified address — with
	// a completed email proof — reports no usable email method while the
	// channel is off, so neither settings nor the unlink guard count a method
	// that cannot sign in.
	const uid = "verified-email-uid"
	registered, err := store.AutoRegister(ctx, "firebase", uid)
	if err != nil {
		t.Fatal(err)
	}
	providers.accounts[uid] = firebaseProviderAccount{
		UID: uid, EmailVerified: true, ProviderSubjects: map[string]string{},
	}
	proofNonce := controllerNonce(t)
	proofNormalized, err := koseki.NormalizeEmail("password-provider@example.com")
	if err != nil {
		t.Fatal(err)
	}
	proof, err := store.StartAuthFlow(ctx, koseki.StartAuthFlowRequest{
		Intent: koseki.IntentSignIn, Channel: koseki.ChannelEmailLink,
		ExpectedProvider: "password", NormalizedEmail: proofNormalized,
		Continuation: "/direct-chat", Nonce: proofNonce, TTL: 10 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ResolveAuthProof(ctx, proof.FlowID, proofNonce, koseki.VerifiedIdentity{
		FirebaseUID: uid, NormalizedEmail: proofNormalized, EmailVerified: true, SignInProvider: "password",
	}); err != nil {
		t.Fatal(err)
	}
	claims := agentevents.UserSessionClaims{UserID: registered.HumanID, PersonalityAgentID: registered.AgentID, TenantID: "local"}
	methods, err := controller.ProviderMethods(ctx, claims)
	if err != nil || methods.Email || len(methods.Providers) != 0 {
		t.Fatalf("proved email counted while sender absent: %+v %v", methods, err)
	}
}
