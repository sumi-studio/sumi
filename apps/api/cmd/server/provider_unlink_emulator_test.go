package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	firebase "firebase.google.com/go/v4"
	firebaseauth "firebase.google.com/go/v4/auth"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

func firebaseProviderEmulatorClient(t *testing.T) *firebaseauth.Client {
	t.Helper()
	host := strings.TrimSpace(os.Getenv("SUMI_TEST_FIREBASE_AUTH_EMULATOR_HOST"))
	if host == "" {
		t.Skip("SUMI_TEST_FIREBASE_AUTH_EMULATOR_HOST not set; skipping Firebase Auth emulator integration test")
	}
	t.Setenv("FIREBASE_AUTH_EMULATOR_HOST", host)
	projectID := strings.TrimSpace(os.Getenv("SUMI_TEST_FIREBASE_PROJECT_ID"))
	if projectID == "" {
		projectID = "sumi-studio"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	app, err := firebase.NewApp(ctx, &firebase.Config{ProjectID: projectID})
	if err != nil {
		t.Fatalf("initialize Firebase emulator app: %v", err)
	}
	client, err := app.Auth(ctx)
	if err != nil {
		t.Fatalf("initialize Firebase emulator Auth client: %v", err)
	}
	return client
}

func firebaseEmulatorID(t *testing.T, prefix string) string {
	t.Helper()
	raw := make([]byte, 8)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return prefix + "-" + hex.EncodeToString(raw)
}

func createFirebaseEmulatorUser(t *testing.T, client *firebaseauth.Client, uid string, providers map[string]string) string {
	t.Helper()
	email := uid + "@example.test"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := client.CreateUser(ctx, (&firebaseauth.UserToCreate{}).UID(uid).Email(email).EmailVerified(true)); err != nil {
		t.Fatalf("create Firebase emulator user: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := client.DeleteUser(cleanupCtx, uid); err != nil {
			t.Logf("delete Firebase emulator user %s: %v", uid, err)
		}
	})
	for provider, subject := range providers {
		if _, err := client.UpdateUser(ctx, uid, (&firebaseauth.UserToUpdate{}).ProviderToLink(&firebaseauth.UserProvider{
			ProviderID: provider, UID: subject, Email: email,
		})); err != nil {
			t.Fatalf("link Firebase emulator provider %s: %v", provider, err)
		}
	}
	return email
}

func completeEmailLinkProof(t *testing.T, ctx context.Context, store *koseki.Store, uid, email string) {
	t.Helper()
	normalized, err := koseki.NormalizeEmail(email)
	if err != nil {
		t.Fatal(err)
	}
	nonce := controllerNonce(t)
	flow, err := store.StartAuthFlow(ctx, koseki.StartAuthFlowRequest{
		Intent: koseki.IntentSignIn, Channel: koseki.ChannelEmailLink,
		ExpectedProvider: "password", NormalizedEmail: normalized,
		Continuation: "/direct-chat", Nonce: nonce, TTL: 10 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, koseki.VerifiedIdentity{
		FirebaseUID: uid, NormalizedEmail: normalized, EmailVerified: true, SignInProvider: "password",
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFirebaseEmulatorUnlinkGuardRequiresLiveEmailFamilyAndSumiProof(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	client := firebaseProviderEmulatorClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store := koseki.New(pool)
	uid := firebaseEmulatorID(t, "unlink-guard")
	email := createFirebaseEmulatorUser(t, client, uid, map[string]string{
		"google.com": "google-subject", "github.com": "github-subject", "facebook.com": "unsupported-subject",
	})
	registered, err := store.AutoRegister(ctx, "firebase", uid)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BindCredential(ctx, "github.com", "github-subject", registered.HumanID); err != nil {
		t.Fatal(err)
	}
	lifecycle := &firebaseAdminProviderLifecycle{client: client}
	// Model a recent Google reauthentication snapshot whose provider was then
	// removed. The live precheck, not the token's stale identity map, controls
	// the last-method decision.
	if err := lifecycle.DeleteProvider(ctx, uid, "google.com"); err != nil {
		t.Fatal(err)
	}
	controller := newKosekiAuthFlowController(store, "local", lifecycle)
	now := time.Now().UTC()
	controller.clock = func() time.Time { return now }
	claims := agentevents.UserSessionClaims{TenantID: "local", UserID: registered.HumanID, PersonalityAgentID: registered.AgentID}
	identity := agentevents.FirebaseIdentity{
		UID: uid, AuthTime: now, SignInProvider: "google.com",
		ProviderSubjects: map[string][]string{"google.com": {"google-subject"}},
	}
	request := agentevents.StartProviderOperationRequest{
		Provider: "github.com", Operation: "unlink", DecisionPath: "account_settings", Nonce: controllerNonce(t),
	}
	if _, err := controller.StartProviderOperation(ctx, claims, request, identity); !errors.Is(err, agentevents.ErrBrowserAuthLastMethod) {
		t.Fatalf("profile email, Firebase UID, or unsupported provider counted: %v", err)
	}

	completeEmailLinkProof(t, ctx, store, uid, email)
	request.Nonce = controllerNonce(t)
	if _, err := controller.StartProviderOperation(ctx, claims, request, identity); !errors.Is(err, agentevents.ErrBrowserAuthLastMethod) {
		t.Fatalf("Sumi proof counted without live Firebase email family: %v", err)
	}
	if _, err := client.UpdateUser(ctx, uid, (&firebaseauth.UserToUpdate{}).Password("emulator-password-123")); err != nil {
		t.Fatalf("add Firebase email/password family: %v", err)
	}
	account, err := lifecycle.ProviderAccount(ctx, uid)
	if err != nil || !account.EmailProvider {
		t.Fatalf("live Firebase email family: %+v %v", account, err)
	}
	request.Nonce = controllerNonce(t)
	result, err := controller.StartProviderOperation(ctx, claims, request, identity)
	if err != nil || result.Outcome != "provider_unlinked" || result.ClientOperation != "" {
		t.Fatalf("backend-owned unlink with both proofs: %+v %v", result, err)
	}
	account, err = lifecycle.ProviderAccount(ctx, uid)
	if err != nil || account.ProviderSubjects["github.com"] != "" {
		t.Fatalf("Firebase provider remained after terminal result: %+v %v", account, err)
	}
}

func TestFirebaseEmulatorUnlinkReconcilesRemoteAppliedDatabaseLost(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	client := firebaseProviderEmulatorClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store := koseki.New(pool)
	uid := firebaseEmulatorID(t, "unlink-reconcile")
	createFirebaseEmulatorUser(t, client, uid, map[string]string{
		"google.com": "google-subject", "github.com": "github-subject",
	})
	registered, err := store.AutoRegister(ctx, "firebase", uid)
	if err != nil {
		t.Fatal(err)
	}
	for provider, subject := range map[string]string{"google.com": "google-subject", "github.com": "github-subject"} {
		if err := store.BindCredential(ctx, provider, subject, registered.HumanID); err != nil {
			t.Fatal(err)
		}
	}
	nonce := controllerNonce(t)
	pending, err := store.BeginProviderOperation(ctx, registered.HumanID, uid, "github.com", "unlink", "account_settings", nonce)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := &firebaseAdminProviderLifecycle{client: client}
	if err := lifecycle.DeleteProvider(ctx, uid, "github.com"); err != nil {
		t.Fatalf("apply remote mutation: %v", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE provider_operations SET expires_at=now()-interval '1 second' WHERE operation_id=$1", pending.OperationID); err != nil {
		t.Fatal(err)
	}

	controller := newKosekiAuthFlowController(store, "local", lifecycle)
	request := agentevents.StartProviderOperationRequest{
		Provider: "github.com", Operation: "unlink", DecisionPath: "account_settings", Nonce: nonce,
	}
	identity := agentevents.FirebaseIdentity{
		UID: uid, AuthTime: time.Now().UTC(), SignInProvider: "google.com",
		ProviderSubjects: map[string][]string{"google.com": {"google-subject"}},
	}
	claims := agentevents.UserSessionClaims{TenantID: "local", UserID: registered.HumanID, PersonalityAgentID: registered.AgentID}
	pendingStatus, err := controller.StatusProviderOperation(ctx, claims, agentevents.ProviderOperationStatusRequest{OperationID: pending.OperationID, Nonce: nonce})
	if err != nil || pendingStatus.Status != "pending" || pendingStatus.Outcome != "provider_operation_pending" {
		t.Fatalf("expired durable saga status: %+v %v", pendingStatus, err)
	}
	result, err := controller.StartProviderOperation(ctx, claims, request, identity)
	if err != nil || result.OperationID != pending.OperationID || result.Outcome != "provider_unlinked" {
		t.Fatalf("remote-applied/DB-lost reconciliation: %+v %v", result, err)
	}
	if _, err := store.ActiveProviderSubject(ctx, registered.HumanID, "github.com"); err == nil {
		t.Fatal("local credential remained active after reconciliation")
	}
	status, err := controller.StatusProviderOperation(ctx, claims, agentevents.ProviderOperationStatusRequest{OperationID: pending.OperationID, Nonce: nonce})
	if err != nil || status.Status != "completed" || status.Outcome != "provider_unlinked" {
		t.Fatalf("terminal status: %+v %v", status, err)
	}
}

func TestFirebaseEmulatorConcurrentUnlinksNeverRemoveLastSupportedMethod(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	client := firebaseProviderEmulatorClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store := koseki.New(pool)
	uid := firebaseEmulatorID(t, "unlink-race")
	createFirebaseEmulatorUser(t, client, uid, map[string]string{
		"google.com": "google-subject", "github.com": "github-subject",
	})
	registered, err := store.AutoRegister(ctx, "firebase", uid)
	if err != nil {
		t.Fatal(err)
	}
	for provider, subject := range map[string]string{"google.com": "google-subject", "github.com": "github-subject"} {
		if err := store.BindCredential(ctx, provider, subject, registered.HumanID); err != nil {
			t.Fatal(err)
		}
	}
	lifecycle := &firebaseAdminProviderLifecycle{client: client}
	controller := newKosekiAuthFlowController(store, "local", lifecycle)
	now := time.Now().UTC()
	controller.clock = func() time.Time { return now }
	claims := agentevents.UserSessionClaims{TenantID: "local", UserID: registered.HumanID, PersonalityAgentID: registered.AgentID}
	type unlinkAttempt struct {
		provider string
		identity agentevents.FirebaseIdentity
		nonce    string
	}
	attempts := []unlinkAttempt{
		{provider: "google.com", nonce: controllerNonce(t), identity: agentevents.FirebaseIdentity{
			UID: uid, AuthTime: now, SignInProvider: "github.com", ProviderSubjects: map[string][]string{"github.com": {"github-subject"}},
		}},
		{provider: "github.com", nonce: controllerNonce(t), identity: agentevents.FirebaseIdentity{
			UID: uid, AuthTime: now, SignInProvider: "google.com", ProviderSubjects: map[string][]string{"google.com": {"google-subject"}},
		}},
	}
	type attemptResult struct {
		result agentevents.ProviderOperationResult
		err    error
	}
	start := make(chan struct{})
	results := make(chan attemptResult, len(attempts))
	var wg sync.WaitGroup
	for _, attempt := range attempts {
		attempt := attempt
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, err := controller.StartProviderOperation(ctx, claims, agentevents.StartProviderOperationRequest{
				Provider: attempt.provider, Operation: "unlink", DecisionPath: "account_settings", Nonce: attempt.nonce,
			}, attempt.identity)
			results <- attemptResult{result: result, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	succeeded := 0
	for attempt := range results {
		switch {
		case attempt.err == nil && attempt.result.Outcome == "provider_unlinked":
			succeeded++
		case errors.Is(attempt.err, agentevents.ErrBrowserAuthProviderPending), errors.Is(attempt.err, agentevents.ErrBrowserAuthLastMethod):
		default:
			t.Fatalf("unexpected concurrent result: %+v %v", attempt.result, attempt.err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("successful concurrent unlinks=%d, want 1", succeeded)
	}
	account, err := lifecycle.ProviderAccount(ctx, uid)
	if err != nil || supportedProviderMethodCount(account) != 1 {
		t.Fatalf("live supported methods after race: %+v %v", account, err)
	}
	var activeSupported int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM credentials
		WHERE human_id=$1 AND active AND provider IN ('google.com','github.com')`, registered.HumanID).Scan(&activeSupported); err != nil {
		t.Fatal(err)
	}
	if activeSupported != 1 {
		t.Fatalf("active supported DB methods after race=%d, want 1", activeSupported)
	}
}
