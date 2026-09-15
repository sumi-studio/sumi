package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	firebaseauth "firebase.google.com/go/v4/auth"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

var emulatorEmailChallengeKey = &koseki.EmailChallengeKey{KeyID: "emulator-email/v1", Key: []byte("emulator-email-key-0123456789abcdef")}

type emailCodeEmulatorHarness struct {
	client     *firebaseauth.Client
	pool       *pgxpool.Pool
	store      *koseki.Store
	controller *kosekiAuthFlowController
	principals *flakyEmailPrincipalClient
}

// flakyEmailPrincipalClient fails selected Admin calls once to model a
// Firebase boundary failure after the mailbox proof committed.
type flakyEmailPrincipalClient struct {
	*firebaseauth.Client
	mu                sync.Mutex
	failCustomToken   int
	customTokenCalls  int
	createUserCalls   int
	deletedPrincipals []string
}

func (f *flakyEmailPrincipalClient) CustomToken(ctx context.Context, uid string) (string, error) {
	f.mu.Lock()
	f.customTokenCalls++
	fail := f.failCustomToken > 0
	if fail {
		f.failCustomToken--
	}
	f.mu.Unlock()
	if fail {
		return "", errors.New("signBlob unavailable")
	}
	return f.Client.CustomToken(ctx, uid)
}

func (f *flakyEmailPrincipalClient) CreateUser(ctx context.Context, user *firebaseauth.UserToCreate) (*firebaseauth.UserRecord, error) {
	f.mu.Lock()
	f.createUserCalls++
	f.mu.Unlock()
	return f.Client.CreateUser(ctx, user)
}

func (f *flakyEmailPrincipalClient) DeleteUser(ctx context.Context, uid string) error {
	f.mu.Lock()
	f.deletedPrincipals = append(f.deletedPrincipals, uid)
	f.mu.Unlock()
	return f.Client.DeleteUser(ctx, uid)
}

func newEmailCodeEmulatorHarness(t *testing.T) *emailCodeEmulatorHarness {
	t.Helper()
	pool := kosekiResolverTestPool(t)
	client := firebaseProviderEmulatorClient(t)
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	store.EmailChallengeKey = emulatorEmailChallengeKey
	principals := &flakyEmailPrincipalClient{Client: client}
	controller := newKosekiAuthFlowController(store, "local", &firebaseAdminProviderLifecycle{client: client})
	controller.email = &emailCodeController{store: store, firebase: principals}
	return &emailCodeEmulatorHarness{client: client, pool: pool, store: store, controller: controller, principals: principals}
}

func (h *emailCodeEmulatorHarness) start(t *testing.T, ctx context.Context, email string) (agentevents.BrowserAuthFlowResult, string, string) {
	t.Helper()
	nonce := controllerNonce(t)
	started, err := h.controller.Start(ctx, agentevents.StartBrowserAuthFlowRequest{
		Intent: "sign_in", Provider: "email_code", Email: email, Continuation: "/direct-chat", Nonce: nonce,
	})
	if err != nil || started.EmailChallenge == nil {
		t.Fatalf("start email code: %+v %v", started, err)
	}
	_, state, err := h.store.EmailFlowStatus(ctx, started.FlowID, nonce)
	if err != nil {
		t.Fatal(err)
	}
	code, _, err := emulatorEmailChallengeKey.EmailChallengeSecrets(state.ChallengeID)
	if err != nil {
		t.Fatal(err)
	}
	return started, nonce, code
}

// exchangeCustomToken performs the browser's signInWithCustomToken against the
// emulator and verifies the resulting ID token with the production verifier.
func exchangeCustomToken(t *testing.T, ctx context.Context, client *firebaseauth.Client, customToken string) agentevents.FirebaseIdentity {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"token": customToken, "returnSecureToken": true})
	resp, err := http.Post("http://"+os.Getenv("FIREBASE_AUTH_EMULATOR_HOST")+"/identitytoolkit.googleapis.com/v1/accounts:signInWithCustomToken?key=emulator",
		"application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		IDToken string `json:"idToken"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || out.IDToken == "" {
		t.Fatalf("custom token exchange: status=%d %v", resp.StatusCode, err)
	}
	identity, err := (&firebaseAdminIDTokenVerifier{client: client}).VerifyIDToken(ctx, out.IDToken)
	if err != nil {
		t.Fatalf("verify custom-token ID token: %v", err)
	}
	return identity
}

func emulatorUserCountByEmail(t *testing.T, ctx context.Context, client *firebaseauth.Client, email string) (*firebaseauth.UserRecord, int) {
	t.Helper()
	var match *firebaseauth.UserRecord
	count := 0
	iter := client.Users(ctx, "")
	for {
		user, err := iter.Next()
		if err != nil {
			break
		}
		if user.Email == email {
			count++
			match = user.UserRecord
		}
	}
	return match, count
}

func TestFirebaseEmulatorEmailCodeSignsInExistingPrincipalWithCustomClaims(t *testing.T) {
	h := newEmailCodeEmulatorHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	uid := firebaseEmulatorID(t, "email-code-existing")
	email := createFirebaseEmulatorUser(t, h.client, uid, map[string]string{"google.com": "google-" + uid})
	registered, err := h.store.AutoRegister(ctx, "firebase", uid)
	if err != nil {
		t.Fatal(err)
	}

	started, nonce, code := h.start(t, ctx, email)
	proof, err := h.controller.email.VerifyEmailCode(ctx, agentevents.VerifyEmailCodeRequest{FlowID: started.FlowID, Nonce: nonce, Code: code})
	if err != nil || proof.CustomToken == "" {
		t.Fatalf("verify: %+v %v", proof, err)
	}
	// A dropped Firebase exchange: the same flow authority gets a new token.
	retry, err := h.controller.email.VerifyEmailCode(ctx, agentevents.VerifyEmailCodeRequest{FlowID: started.FlowID, Nonce: nonce})
	if err != nil || retry.CustomToken == "" {
		t.Fatalf("retry after proof: %+v %v", retry, err)
	}
	identity := exchangeCustomToken(t, ctx, h.client, retry.CustomToken)
	if identity.UID != uid || identity.SignInProvider != "custom" || !identity.EmailVerified || identity.Email != email ||
		len(identity.ProviderSubjects["google.com"]) != 1 || identity.IssuedAt.IsZero() {
		t.Fatalf("custom-token identity: %+v", identity)
	}
	result, err := h.controller.Resolve(ctx, agentevents.ResolveBrowserAuthFlowRequest{FlowID: started.FlowID, Nonce: nonce}, identity)
	if err != nil || result.Outcome != koseki.OutcomeSignedIn || result.Claims.UserID != registered.HumanID {
		t.Fatalf("resolve: %+v %v", result, err)
	}
	// The resolve response was lost: inside the replay window the same flow
	// authority gets a token for the same UID and signs in the same Human.
	replayProof, err := h.controller.email.VerifyEmailCode(ctx, agentevents.VerifyEmailCodeRequest{FlowID: started.FlowID, Nonce: nonce, Code: code})
	if err != nil || replayProof.CustomToken == "" {
		t.Fatalf("verify replay after sign-in: %+v %v", replayProof, err)
	}
	replayed, err := h.controller.Resolve(ctx, agentevents.ResolveBrowserAuthFlowRequest{FlowID: started.FlowID, Nonce: nonce},
		exchangeCustomToken(t, ctx, h.client, replayProof.CustomToken))
	if err != nil || replayed.Outcome != koseki.OutcomeSignedIn || replayed.Claims.UserID != registered.HumanID ||
		replayed.Claims.PersonalityAgentID != result.Claims.PersonalityAgentID {
		t.Fatalf("resolve replay: %+v %v", replayed, err)
	}
	if _, err := h.pool.Exec(ctx, `UPDATE auth_flows SET completed_at=completed_at-make_interval(secs => $2) WHERE flow_id=$1`,
		started.FlowID, koseki.EmailCompletionReplayWindow.Seconds()+1); err != nil {
		t.Fatal(err)
	}
	minted := h.principals.customTokenCalls
	if _, err := h.controller.email.VerifyEmailCode(ctx, agentevents.VerifyEmailCodeRequest{FlowID: started.FlowID, Nonce: nonce, Code: code}); !errors.Is(err, agentevents.ErrBrowserAuthFlowConsumed) {
		t.Fatalf("second use after sign-in: %v", err)
	}
	if _, err := h.controller.Resolve(ctx, agentevents.ResolveBrowserAuthFlowRequest{FlowID: started.FlowID, Nonce: nonce}, identity); !errors.Is(err, agentevents.ErrBrowserAuthFlowConsumed) {
		t.Fatalf("resolve after replay window: %v", err)
	}
	if h.principals.customTokenCalls != minted {
		t.Fatalf("second use minted custom tokens: %d -> %d", minted, h.principals.customTokenCalls)
	}
	status, err := h.controller.Status(ctx, agentevents.ConfirmBrowserAuthFlowRequest{FlowID: started.FlowID, Nonce: nonce})
	if err != nil || status.Outcome != koseki.OutcomeSignedIn {
		t.Fatalf("completed retry recovers through status: %+v %v", status, err)
	}
	if h.principals.createUserCalls != 0 {
		t.Fatalf("existing verified principal created a user: %d", h.principals.createUserCalls)
	}
}

func TestFirebaseEmulatorEmailCodeConcurrentRetriesCreateOnePrincipal(t *testing.T) {
	h := newEmailCodeEmulatorHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	email := firebaseEmulatorID(t, "email-code-new") + "@example.test"
	started, nonce, code := h.start(t, ctx, email)
	h.principals.failCustomToken = 1

	const workers = 6
	var wg sync.WaitGroup
	tokens := make(chan string, workers)
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			proof, err := h.controller.email.VerifyEmailCode(ctx, agentevents.VerifyEmailCodeRequest{FlowID: started.FlowID, Nonce: nonce, Code: code})
			if err != nil {
				errs <- err
				return
			}
			tokens <- proof.CustomToken
		}()
	}
	wg.Wait()
	close(tokens)
	close(errs)
	unavailable := 0
	for err := range errs {
		if !errors.Is(err, agentevents.ErrBrowserAuthProviderUnavailable) {
			t.Fatalf("unexpected concurrent verify error: %v", err)
		}
		unavailable++
	}
	if unavailable != 1 {
		t.Fatalf("injected signing failures observed = %d", unavailable)
	}
	uids := map[string]bool{}
	for token := range tokens {
		uids[exchangeCustomToken(t, ctx, h.client, token).UID] = true
	}
	record, count := emulatorUserCountByEmail(t, ctx, h.client, email)
	if count != 1 || len(uids) != 1 || !uids[record.UID] || !record.EmailVerified {
		t.Fatalf("principals for proved address: count=%d uids=%v record=%+v", count, uids, record)
	}
	t.Cleanup(func() { _ = h.client.DeleteUser(context.Background(), record.UID) })

	// The failed signing call is an ordinary retry of the same authority.
	proof, err := h.controller.email.VerifyEmailCode(ctx, agentevents.VerifyEmailCodeRequest{FlowID: started.FlowID, Nonce: nonce})
	if err != nil {
		t.Fatal(err)
	}
	identity := exchangeCustomToken(t, ctx, h.client, proof.CustomToken)
	if identity.UID != record.UID {
		t.Fatalf("retry granted a different UID: %s != %s", identity.UID, record.UID)
	}
	// Unknown sign-in without invitation keeps the existing enrollment rule.
	if _, err := h.controller.Resolve(ctx, agentevents.ResolveBrowserAuthFlowRequest{FlowID: started.FlowID, Nonce: nonce}, identity); !errors.Is(err, agentevents.ErrBrowserEnrollmentInvite) {
		t.Fatalf("uninvited unknown email-code sign-in: %v", err)
	}
}

func TestFirebaseEmulatorEmailCodeReplacesUnboundUnverifiedPrincipal(t *testing.T) {
	h := newEmailCodeEmulatorHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	attackerUID := firebaseEmulatorID(t, "email-code-squatter")
	email := attackerUID + "@example.test"
	if _, err := h.client.CreateUser(ctx, (&firebaseauth.UserToCreate{}).UID(attackerUID).Email(email).Password("squatter-password-1")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.client.DeleteUser(context.Background(), attackerUID) })

	started, nonce, code := h.start(t, ctx, email)
	proof, err := h.controller.email.VerifyEmailCode(ctx, agentevents.VerifyEmailCodeRequest{FlowID: started.FlowID, Nonce: nonce, Code: code})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	identity := exchangeCustomToken(t, ctx, h.client, proof.CustomToken)
	t.Cleanup(func() { _ = h.client.DeleteUser(context.Background(), identity.UID) })
	if identity.UID == attackerUID || !identity.EmailVerified || len(h.principals.deletedPrincipals) != 1 {
		t.Fatalf("unverified principal survived mailbox proof: %+v deleted=%v", identity, h.principals.deletedPrincipals)
	}
	record, err := h.client.GetUser(ctx, identity.UID)
	if err != nil || len(record.ProviderUserInfo) != 0 {
		t.Fatalf("replacement principal carries sign-in methods: %+v %v", record, err)
	}
	body := []byte(`{"email":"` + email + `","password":"squatter-password-1","returnSecureToken":true}`)
	resp, err := http.Post("http://"+os.Getenv("FIREBASE_AUTH_EMULATOR_HOST")+"/identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=emulator", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatal("pre-created password still signs in after mailbox proof")
	}
}

func TestFirebaseEmulatorEmailCodeFailsClosedForBoundUnverifiedPrincipal(t *testing.T) {
	h := newEmailCodeEmulatorHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	uid := firebaseEmulatorID(t, "email-code-bound-unverified")
	email := uid + "@example.test"
	if _, err := h.client.CreateUser(ctx, (&firebaseauth.UserToCreate{}).UID(uid).Email(email)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.client.DeleteUser(context.Background(), uid) })
	if _, err := h.store.AutoRegister(ctx, "firebase", uid); err != nil {
		t.Fatal(err)
	}
	started, nonce, code := h.start(t, ctx, email)
	var unverified *agentevents.BrowserEmailUnverifiedAccountError
	_, err := h.controller.email.VerifyEmailCode(ctx, agentevents.VerifyEmailCodeRequest{FlowID: started.FlowID, Nonce: nonce, Code: code})
	if !errors.Is(err, agentevents.ErrBrowserEmailUnverifiedAccount) || !errors.As(err, &unverified) || len(unverified.SignInProviders) != 0 {
		t.Fatalf("bound unverified principal without providers: %v", err)
	}
	if _, err := h.client.GetUser(ctx, uid); err != nil || len(h.principals.deletedPrincipals) != 0 {
		t.Fatalf("bound principal was deleted: %v %v", err, h.principals.deletedPrincipals)
	}

	// A GitHub-first account: GitHub is not a trusted email verifier for
	// Firebase, so the address stays unverified. The refusal names the linked
	// GitHub method, which still signs in to the same UID.
	githubUID := firebaseEmulatorID(t, "email-code-github-first")
	githubEmail := githubUID + "@example.test"
	if _, err := h.client.CreateUser(ctx, (&firebaseauth.UserToCreate{}).UID(githubUID).Email(githubEmail)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.client.DeleteUser(context.Background(), githubUID) })
	if _, err := h.client.UpdateUser(ctx, githubUID, (&firebaseauth.UserToUpdate{}).ProviderToLink(&firebaseauth.UserProvider{
		ProviderID: "github.com", UID: "github-" + githubUID, Email: githubEmail,
	})); err != nil {
		t.Fatal(err)
	}
	if record, err := h.client.GetUser(ctx, githubUID); err != nil || record.EmailVerified {
		t.Fatalf("GitHub-first principal setup: %+v %v", record, err)
	}
	if _, err := h.store.AutoRegister(ctx, "firebase", githubUID); err != nil {
		t.Fatal(err)
	}
	started, nonce, code = h.start(t, ctx, githubEmail)
	_, err = h.controller.email.VerifyEmailCode(ctx, agentevents.VerifyEmailCodeRequest{FlowID: started.FlowID, Nonce: nonce, Code: code})
	if !errors.As(err, &unverified) || len(unverified.SignInProviders) != 1 || unverified.SignInProviders[0] != "github.com" {
		t.Fatalf("GitHub-first account refusal: %v %+v", err, unverified)
	}
}

func TestFirebaseEmulatorEmailCodeExpiredProofMintsNoCustomToken(t *testing.T) {
	h := newEmailCodeEmulatorHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	uid := firebaseEmulatorID(t, "email-code-expiring")
	email := createFirebaseEmulatorUser(t, h.client, uid, nil)
	if _, err := h.store.AutoRegister(ctx, "firebase", uid); err != nil {
		t.Fatal(err)
	}
	started, nonce, code := h.start(t, ctx, email)
	if _, err := h.controller.email.VerifyEmailCode(ctx, agentevents.VerifyEmailCodeRequest{FlowID: started.FlowID, Nonce: nonce, Code: code}); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if _, err := h.controller.email.VerifyEmailCode(ctx, agentevents.VerifyEmailCodeRequest{FlowID: started.FlowID, Nonce: nonce}); err != nil {
		t.Fatalf("retry before expiry: %v", err)
	}
	minted := h.principals.customTokenCalls
	if minted != 2 {
		t.Fatalf("custom tokens before expiry = %d", minted)
	}
	if _, err := h.pool.Exec(ctx, `UPDATE auth_flows SET created_at=created_at-interval '1 hour',
		expires_at=clock_timestamp()-interval '1 second' WHERE flow_id=$1`, started.FlowID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := h.controller.email.VerifyEmailCode(ctx, agentevents.VerifyEmailCodeRequest{FlowID: started.FlowID, Nonce: nonce}); !errors.Is(err, agentevents.ErrBrowserAuthFlowExpired) {
			t.Fatalf("retry after flow expiry: %v", err)
		}
	}
	if h.principals.customTokenCalls != minted {
		t.Fatalf("expired proof minted custom tokens: %d -> %d", minted, h.principals.customTokenCalls)
	}
}

func TestFirebaseEmulatorEmailLinkAdoptionAndSessionRelation(t *testing.T) {
	h := newEmailCodeEmulatorHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	uid := firebaseEmulatorID(t, "email-link-adopt")
	email := createFirebaseEmulatorUser(t, h.client, uid, nil)
	registered, err := h.store.AutoRegister(ctx, "firebase", uid)
	if err != nil {
		t.Fatal(err)
	}
	other, err := h.store.AutoRegister(ctx, "firebase", firebaseEmulatorID(t, "other-human"))
	if err != nil {
		t.Fatal(err)
	}
	started, pwaNonce, _ := h.start(t, ctx, email)
	_, state, err := h.store.EmailFlowStatus(ctx, started.FlowID, pwaNonce)
	if err != nil {
		t.Fatal(err)
	}
	_, token, _ := emulatorEmailChallengeKey.EmailChallengeSecrets(state.ChallengeID)
	link := agentevents.InspectEmailLinkRequest{ChallengeID: state.ChallengeID, Token: token}
	for session, want := range map[*agentevents.UserSessionClaims]string{
		nil: "none",
		{TenantID: "local", UserID: registered.HumanID}: "same_account",
		{TenantID: "local", UserID: other.HumanID}:      "other_account",
	} {
		inspection, err := h.controller.email.InspectEmailLink(ctx, link, session)
		if err != nil || inspection.Session != want || inspection.State != "usable" || inspection.SameBrowser {
			t.Fatalf("inspect session want %s: %+v %v", want, inspection, err)
		}
	}
	browserNonce := controllerNonce(t)
	complete := agentevents.CompleteEmailLinkRequest{ChallengeID: state.ChallengeID, Token: token, Nonce: browserNonce}
	if _, err := h.controller.email.CompleteEmailLink(ctx, complete); !errors.Is(err, agentevents.ErrBrowserEmailAdoptionRequired) {
		t.Fatalf("other browser without choice: %v", err)
	}
	complete.Adopt = true
	proof, err := h.controller.email.CompleteEmailLink(ctx, complete)
	if err != nil {
		t.Fatal(err)
	}
	identity := exchangeCustomToken(t, ctx, h.client, proof.CustomToken)
	if _, err := h.controller.Resolve(ctx, agentevents.ResolveBrowserAuthFlowRequest{FlowID: started.FlowID, Nonce: pwaNonce}, identity); !errors.Is(err, agentevents.ErrBrowserEmailContinuedElsewhere) {
		t.Fatalf("original browser resolved adopted flow: %v", err)
	}
	result, err := h.controller.Resolve(ctx, agentevents.ResolveBrowserAuthFlowRequest{FlowID: started.FlowID, Nonce: browserNonce}, identity)
	if err != nil || result.Claims.UserID != registered.HumanID {
		t.Fatalf("adopting browser resolve: %+v %v", result, err)
	}
	if _, err := h.controller.Status(ctx, agentevents.ConfirmBrowserAuthFlowRequest{FlowID: started.FlowID, Nonce: pwaNonce}); !errors.Is(err, agentevents.ErrBrowserEmailContinuedElsewhere) {
		t.Fatalf("original browser status: %v", err)
	}
}

func TestFirebaseEmulatorEmailCodeCountsAsUnlinkMethodOnlyWhenEnabled(t *testing.T) {
	h := newEmailCodeEmulatorHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	uid := firebaseEmulatorID(t, "email-code-unlink")
	email := createFirebaseEmulatorUser(t, h.client, uid, map[string]string{"github.com": "github-" + uid})
	registered, err := h.store.AutoRegister(ctx, "firebase", uid)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.store.BindCredential(ctx, "github.com", "github-"+uid, registered.HumanID); err != nil {
		t.Fatal(err)
	}
	started, nonce, code := h.start(t, ctx, email)
	proof, err := h.controller.email.VerifyEmailCode(ctx, agentevents.VerifyEmailCodeRequest{FlowID: started.FlowID, Nonce: nonce, Code: code})
	if err != nil {
		t.Fatal(err)
	}
	identity := exchangeCustomToken(t, ctx, h.client, proof.CustomToken)
	if _, err := h.controller.Resolve(ctx, agentevents.ResolveBrowserAuthFlowRequest{FlowID: started.FlowID, Nonce: nonce}, identity); err != nil {
		t.Fatal(err)
	}
	claims := agentevents.UserSessionClaims{TenantID: "local", UserID: registered.HumanID, PersonalityAgentID: registered.AgentID}
	identity.AuthTime = time.Now()
	request := agentevents.StartProviderOperationRequest{Provider: "github.com", Operation: "unlink", DecisionPath: "account_settings", Nonce: controllerNonce(t)}

	disabled := newKosekiAuthFlowController(h.store, "local", &firebaseAdminProviderLifecycle{client: h.client})
	if _, err := disabled.StartProviderOperation(ctx, claims, request, identity); !errors.Is(err, agentevents.ErrBrowserAuthLastMethod) {
		t.Fatalf("verified address counted while email-code sign-in is disabled: %v", err)
	}
	request.Nonce = controllerNonce(t)
	result, err := h.controller.StartProviderOperation(ctx, claims, request, identity)
	if err != nil || result.Outcome != "provider_unlinked" {
		t.Fatalf("unlink with email-code method and recent custom reauth: %+v %v", result, err)
	}
}
