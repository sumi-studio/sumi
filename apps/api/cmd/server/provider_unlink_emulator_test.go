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
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
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
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
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

func TestFirebaseEmulatorOrphanedUnlinkSettlesFromLiveAccountWithoutNonce(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	client := firebaseProviderEmulatorClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	uid := firebaseEmulatorID(t, "unlink-orphan")
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
	claims := agentevents.UserSessionClaims{TenantID: "local", UserID: registered.HumanID, PersonalityAgentID: registered.AgentID}
	identity := agentevents.FirebaseIdentity{
		UID: uid, AuthTime: time.Now().UTC(), SignInProvider: "google.com",
		ProviderSubjects: map[string][]string{"google.com": {"google-subject"}},
	}
	orphanUnlink := func(provider string) string {
		t.Helper()
		operation, err := store.BeginProviderOperation(ctx, registered.HumanID, uid, provider, "unlink", "account_settings", controllerNonce(t))
		if err != nil {
			t.Fatal(err)
		}
		return operation.OperationID
	}
	expire := func(operationID, interval string) {
		t.Helper()
		if _, err := pool.Exec(ctx, "UPDATE provider_operations SET expires_at=now()-$2::interval WHERE operation_id=$1", operationID, interval); err != nil {
			t.Fatal(err)
		}
	}
	outcome := func(operationID string) string {
		t.Helper()
		var status, terminal string
		if err := pool.QueryRow(ctx, "SELECT status, COALESCE(terminal_outcome, '') FROM provider_operations WHERE operation_id=$1", operationID).Scan(&status, &terminal); err != nil {
			t.Fatal(err)
		}
		return status + "/" + terminal
	}

	// The remote delete never happened and the browser record is gone.
	untouched := orphanUnlink("github.com")
	expire(untouched, "1 second")
	link := agentevents.StartProviderOperationRequest{Provider: "google.com", Operation: "link", DecisionPath: "account_settings", Nonce: controllerNonce(t)}
	if _, err := controller.StartProviderOperation(ctx, claims, link, identity); !errors.Is(err, agentevents.ErrBrowserAuthProviderPending) {
		t.Fatalf("present provider inside grace released its fence: %v", err)
	}
	// Past the grace the server cannot know whether the original delete is
	// still in flight, so it does not declare the remote final: it drives one
	// bounded reconcile delete of the recorded subject itself. The emulator
	// applies it — the orphan completes as unlinked and the fence releases.
	expire(untouched, "3 minutes")
	started, err := controller.StartProviderOperation(ctx, claims, link, identity)
	if err != nil || started.Outcome != "client_operation_required" || outcome(untouched) != "completed/unlinked" {
		t.Fatalf("untouched orphan reconcile: %+v %v %s", started, err, outcome(untouched))
	}
	account, err := lifecycle.ProviderAccount(ctx, uid)
	if err != nil {
		t.Fatalf("read remote account: %v", err)
	}
	if _, stillLinked := account.ProviderSubjects["github.com"]; stillLinked {
		t.Fatalf("reconcile delete did not remove the provider: %+v", account)
	}
	if _, err := store.ActiveProviderSubject(ctx, registered.HumanID, "github.com"); err == nil {
		t.Fatal("credential stayed active after reconciled remote removal")
	}
	if _, err := controller.FailProviderOperation(ctx, claims, agentevents.FailProviderOperationRequest{OperationID: started.OperationID, Nonce: link.Nonce, Outcome: "cancelled"}); err != nil {
		t.Fatal(err)
	}

	// The remote delete landed but its reply and browser record were lost —
	// a second principal, since the emulator cannot re-link the removed
	// provider to the same user through Admin. Credentials are globally
	// unique on (provider, subject), so this principal gets its own.
	uid2 := firebaseEmulatorID(t, "unlink-orphan-applied")
	createFirebaseEmulatorUser(t, client, uid2, map[string]string{
		"google.com": "google-subject-2", "github.com": "github-subject-2",
	})
	registered2, err := store.AutoRegister(ctx, "firebase", uid2)
	if err != nil {
		t.Fatal(err)
	}
	for provider, subject := range map[string]string{"google.com": "google-subject-2", "github.com": "github-subject-2"} {
		if err := store.BindCredential(ctx, provider, subject, registered2.HumanID); err != nil {
			t.Fatal(err)
		}
	}
	claims2 := agentevents.UserSessionClaims{TenantID: "local", UserID: registered2.HumanID, PersonalityAgentID: registered2.AgentID}
	identity2 := agentevents.FirebaseIdentity{
		UID: uid2, AuthTime: time.Now().UTC(), SignInProvider: "google.com",
		ProviderSubjects: map[string][]string{"google.com": {"google-subject-2"}},
	}
	applied, err := store.BeginProviderOperation(ctx, registered2.HumanID, uid2, "github.com", "unlink", "account_settings", controllerNonce(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.DeleteProvider(ctx, uid2, "github.com"); err != nil {
		t.Fatal(err)
	}
	expire(applied.OperationID, "1 second")
	link2 := agentevents.StartProviderOperationRequest{Provider: "google.com", Operation: "link", DecisionPath: "account_settings", Nonce: controllerNonce(t)}
	started, err = controller.StartProviderOperation(ctx, claims2, link2, identity2)
	if err != nil || started.Outcome != "client_operation_required" || outcome(applied.OperationID) != "completed/unlinked" {
		t.Fatalf("applied orphan settlement: %+v %v %s", started, err, outcome(applied.OperationID))
	}
	if _, err := store.ActiveProviderSubject(ctx, registered2.HumanID, "github.com"); err == nil {
		t.Fatal("credential stayed active after settled remote removal")
	}
	methods, err := controller.ProviderMethods(ctx, claims2)
	if err != nil || len(methods.Providers) != 1 || methods.Providers[0] != "google.com" {
		t.Fatalf("live methods after settlement: %+v %v", methods, err)
	}
}

func TestFirebaseEmulatorLinkNonceReplayCannotEscapePendingUnlinkFence(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	client := firebaseProviderEmulatorClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	uid := firebaseEmulatorID(t, "link-replay-fence")
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

	terminalNonce := controllerNonce(t)
	terminal, err := store.BeginProviderOperation(ctx, registered.HumanID, uid, "google.com", "link", "account_settings", terminalNonce)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteProviderLink(ctx, terminal.OperationID, terminalNonce, uid, "google-subject"); err != nil {
		t.Fatal(err)
	}
	expiredNonce := controllerNonce(t)
	expired, err := store.BeginProviderOperation(ctx, registered.HumanID, uid, "google.com", "link", "notice_action", expiredNonce)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "UPDATE provider_operations SET expires_at=now()-interval '1 second' WHERE operation_id=$1", expired.OperationID); err != nil {
		t.Fatal(err)
	}
	unlinkNonce := controllerNonce(t)
	pendingUnlink, err := store.BeginProviderOperation(ctx, registered.HumanID, uid, "github.com", "unlink", "account_settings", unlinkNonce)
	if err != nil {
		t.Fatal(err)
	}

	lifecycle := &firebaseAdminProviderLifecycle{client: client}
	controller := newKosekiAuthFlowController(store, "local", lifecycle)
	claims := agentevents.UserSessionClaims{TenantID: "local", UserID: registered.HumanID, PersonalityAgentID: registered.AgentID}
	identity := agentevents.FirebaseIdentity{UID: uid}
	recovered, err := controller.StartProviderOperation(ctx, claims, agentevents.StartProviderOperationRequest{
		Provider: "google.com", Operation: "link", DecisionPath: "account_settings", Nonce: terminalNonce,
	}, identity)
	if err != nil || recovered.OperationID != terminal.OperationID || recovered.Outcome != "provider_already_linked" || recovered.ClientOperation != "" {
		t.Fatalf("terminal link replay: %+v %v", recovered, err)
	}
	if _, err := controller.StartProviderOperation(ctx, claims, agentevents.StartProviderOperationRequest{
		Provider: "google.com", Operation: "link", DecisionPath: "notice_action", Nonce: expiredNonce,
	}, identity); !errors.Is(err, agentevents.ErrBrowserAuthFlowExpired) {
		t.Fatalf("expired link replay: %v", err)
	}
	pending, err := controller.StatusProviderOperation(ctx, claims, agentevents.ProviderOperationStatusRequest{
		OperationID: pendingUnlink.OperationID, Nonce: unlinkNonce,
	})
	if err != nil || pending.Status != "pending" || pending.Outcome != "provider_operation_pending" || pending.ClientOperation != "" {
		t.Fatalf("unlink fence after replays: %+v %v", pending, err)
	}
	account, err := lifecycle.ProviderAccount(ctx, uid)
	if err != nil || account.ProviderSubjects["google.com"] != "google-subject" || account.ProviderSubjects["github.com"] != "github-subject" {
		t.Fatalf("Firebase account changed during replay: %+v %v", account, err)
	}
}

func TestFirebaseEmulatorConcurrentUnlinksNeverRemoveLastSupportedMethod(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	client := firebaseProviderEmulatorClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
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
