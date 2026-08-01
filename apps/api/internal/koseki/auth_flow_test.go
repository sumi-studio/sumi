package koseki

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

func authFlowStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	pool := testdb.Create(t)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return New(pool), ctx
}

func testNonce(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func startEmailFlow(t *testing.T, ctx context.Context, store *Store, intent AuthIntent, email, nonce string) AuthFlow {
	t.Helper()
	normalized, err := NormalizeEmail(email)
	if err != nil {
		t.Fatal(err)
	}
	flow, err := store.StartAuthFlow(ctx, StartAuthFlowRequest{
		Intent: intent, Channel: ChannelEmailLink, ExpectedProvider: "password",
		NormalizedEmail: normalized, Continuation: "/direct-chat", Nonce: nonce,
		TTL: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("start flow: %v", err)
	}
	return flow
}

func emailProof(uid, email string) VerifiedIdentity {
	normalized, _ := NormalizeEmail(email)
	return VerifiedIdentity{FirebaseUID: uid, NormalizedEmail: normalized, EmailVerified: true, SignInProvider: "password"}
}

func TestAuthFlowFourIntentExistenceCombinations(t *testing.T) {
	t.Run("sign in existing signs in", func(t *testing.T) {
		store, ctx := authFlowStore(t)
		registered, err := store.AutoRegister(ctx, "firebase", "known-sign-in")
		if err != nil {
			t.Fatal(err)
		}
		nonce := testNonce(t)
		flow := startEmailFlow(t, ctx, store, IntentSignIn, "Person@Example.com", nonce)
		result, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, emailProof("known-sign-in", "person@example.com"))
		if err != nil {
			t.Fatal(err)
		}
		if result.TerminalOutcome != OutcomeSignedIn || result.HumanID != registered.HumanID || result.AgentID != registered.AgentID {
			t.Fatalf("unexpected result: %+v", result)
		}
	})

	t.Run("sign up unknown creates once", func(t *testing.T) {
		store, ctx := authFlowStore(t)
		nonce := testNonce(t)
		flow := startEmailFlow(t, ctx, store, IntentSignUp, "new@example.com", nonce)
		result, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, emailProof("new-sign-up", "new@example.com"))
		if err != nil {
			t.Fatal(err)
		}
		if result.TerminalOutcome != OutcomeAccountCreated || result.HumanID == "" || result.AgentID == "" {
			t.Fatalf("unexpected result: %+v", result)
		}
		if got, err := store.AgentForHuman(ctx, result.HumanID); err != nil || got != result.AgentID {
			t.Fatalf("Secretary: %q %v", got, err)
		}
		if _, err := store.AgentWrappingKey(ctx, result.AgentID); err != nil {
			t.Fatalf("wrapping key: %v", err)
		}
	})

	t.Run("sign in unknown requires create confirmation", func(t *testing.T) {
		store, ctx := authFlowStore(t)
		nonce := testNonce(t)
		flow := startEmailFlow(t, ctx, store, IntentSignIn, "unknown@example.com", nonce)
		pending, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, emailProof("unknown-sign-in", "unknown@example.com"))
		if err != nil {
			t.Fatal(err)
		}
		if pending.Status != "confirmation_required" || pending.ConfirmationAction != ActionCreateAccount {
			t.Fatalf("unexpected pending: %+v", pending)
		}
		assertRegistryCounts(t, ctx, store, 0, 0)
		result, err := store.ConfirmAuthFlow(ctx, flow.FlowID, nonce, ActionCreateAccount)
		if err != nil {
			t.Fatal(err)
		}
		if result.TerminalOutcome != OutcomeAccountCreated {
			t.Fatalf("unexpected result: %+v", result)
		}
		assertRegistryCounts(t, ctx, store, 1, 1)
	})

	t.Run("sign up existing requires sign in confirmation", func(t *testing.T) {
		store, ctx := authFlowStore(t)
		registered, err := store.AutoRegister(ctx, "firebase", "known-sign-up")
		if err != nil {
			t.Fatal(err)
		}
		nonce := testNonce(t)
		flow := startEmailFlow(t, ctx, store, IntentSignUp, "known@example.com", nonce)
		pending, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, emailProof("known-sign-up", "known@example.com"))
		if err != nil {
			t.Fatal(err)
		}
		if pending.Status != "confirmation_required" || pending.ConfirmationAction != ActionSignIn {
			t.Fatalf("unexpected pending: %+v", pending)
		}
		result, err := store.ConfirmAuthFlow(ctx, flow.FlowID, nonce, ActionSignIn)
		if err != nil {
			t.Fatal(err)
		}
		if result.TerminalOutcome != OutcomeSignedIn || result.HumanID != registered.HumanID {
			t.Fatalf("unexpected result: %+v", result)
		}
		assertRegistryCounts(t, ctx, store, 1, 1)
	})
}

func assertRegistryCounts(t *testing.T, ctx context.Context, store *Store, humans, agents int) {
	t.Helper()
	var gotHumans, gotAgents int
	if err := store.pool.QueryRow(ctx, "SELECT count(*) FROM humans").Scan(&gotHumans); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, "SELECT count(*) FROM agents").Scan(&gotAgents); err != nil {
		t.Fatal(err)
	}
	if gotHumans != humans || gotAgents != agents {
		t.Fatalf("registry counts humans=%d agents=%d, want %d/%d", gotHumans, gotAgents, humans, agents)
	}
}

func TestAuthFlowRejectsMismatchReplayAndChangedIdempotency(t *testing.T) {
	store, ctx := authFlowStore(t)
	nonce := testNonce(t)
	flow := startEmailFlow(t, ctx, store, IntentSignUp, "bound@example.com", nonce)

	if _, err := store.ResolveAuthProof(ctx, flow.FlowID, testNonce(t), emailProof("uid", "bound@example.com")); !errors.Is(err, ErrInvalidAuthFlow) {
		t.Fatalf("wrong nonce: %v", err)
	}
	if _, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, emailProof("uid", "other@example.com")); !errors.Is(err, ErrAuthProofMismatch) {
		t.Fatalf("wrong email: %v", err)
	}
	if _, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, VerifiedIdentity{FirebaseUID: "uid", NormalizedEmail: "bound@example.com", SignInProvider: "password"}); !errors.Is(err, ErrAuthProofMismatch) {
		t.Fatalf("unverified email: %v", err)
	}
	assertRegistryCounts(t, ctx, store, 0, 0)

	result, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, emailProof("uid", "bound@example.com"))
	if err != nil || result.TerminalOutcome != OutcomeAccountCreated {
		t.Fatalf("complete: %+v %v", result, err)
	}
	if _, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, emailProof("uid", "bound@example.com")); !errors.Is(err, ErrAuthFlowConsumed) {
		t.Fatalf("replay: %v", err)
	}
	status, err := store.AuthFlowStatus(ctx, flow.FlowID, nonce)
	if err != nil || status.TerminalOutcome != OutcomeAccountCreated || status.HumanID != result.HumanID {
		t.Fatalf("terminal status recovery: %+v %v", status, err)
	}
	assertRegistryCounts(t, ctx, store, 1, 1)

	same, err := store.StartAuthFlow(ctx, StartAuthFlowRequest{Intent: IntentSignUp, Channel: ChannelEmailLink, ExpectedProvider: "password", NormalizedEmail: "bound@example.com", Continuation: "/direct-chat", Nonce: nonce, TTL: 10 * time.Minute})
	if err != nil || same.FlowID != flow.FlowID {
		t.Fatalf("idempotent start: %+v %v", same, err)
	}
	_, err = store.StartAuthFlow(ctx, StartAuthFlowRequest{Intent: IntentSignIn, Channel: ChannelEmailLink, ExpectedProvider: "password", NormalizedEmail: "bound@example.com", Continuation: "/direct-chat", Nonce: nonce, TTL: 10 * time.Minute})
	if !errors.Is(err, ErrInvalidAuthFlow) {
		t.Fatalf("changed nonce semantics: %v", err)
	}
}

func TestExpiredFlowAndWrongConfirmationNeverProvision(t *testing.T) {
	store, ctx := authFlowStore(t)
	nonce := testNonce(t)
	flow := startEmailFlow(t, ctx, store, IntentSignIn, "expired@example.com", nonce)
	if _, err := store.pool.Exec(ctx, "UPDATE auth_flows SET expires_at=now()-interval '1 second' WHERE flow_id=$1", flow.FlowID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, emailProof("expired", "expired@example.com")); !errors.Is(err, ErrAuthFlowExpired) {
		t.Fatalf("expired proof: %v", err)
	}
	assertRegistryCounts(t, ctx, store, 0, 0)

	nonce = testNonce(t)
	flow = startEmailFlow(t, ctx, store, IntentSignIn, "confirm@example.com", nonce)
	if _, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, emailProof("confirm", "confirm@example.com")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ConfirmAuthFlow(ctx, flow.FlowID, nonce, ActionSignIn); !errors.Is(err, ErrConfirmation) {
		t.Fatalf("wrong confirmation: %v", err)
	}
	assertRegistryCounts(t, ctx, store, 0, 0)
}

func TestConcurrentSignUpFlowsCreateOnlyOneHuman(t *testing.T) {
	store, ctx := authFlowStore(t)
	const uid = "racing-firebase-uid"
	type item struct {
		flow  AuthFlow
		nonce string
	}
	items := make([]item, 2)
	for i := range items {
		items[i].nonce = testNonce(t)
		items[i].flow = startEmailFlow(t, ctx, store, IntentSignUp, fmt.Sprintf("race%d@example.com", i), items[i].nonce)
	}
	var wg sync.WaitGroup
	results := make(chan AuthFlow, 2)
	errs := make(chan error, 2)
	for i := range items {
		wg.Add(1)
		go func(item item, email string) {
			defer wg.Done()
			result, err := store.ResolveAuthProof(ctx, item.flow.FlowID, item.nonce, emailProof(uid, email))
			results <- result
			errs <- err
		}(items[i], fmt.Sprintf("race%d@example.com", i))
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("race resolve: %v", err)
		}
	}
	created, confirmation := 0, 0
	for result := range results {
		if result.TerminalOutcome == OutcomeAccountCreated {
			created++
		}
		if result.ConfirmationAction == ActionSignIn {
			confirmation++
		}
	}
	if created != 1 || confirmation != 1 {
		t.Fatalf("created=%d confirmation=%d", created, confirmation)
	}
	assertRegistryCounts(t, ctx, store, 1, 1)
}

func TestProviderLifecycleIsAuditedAndHistoricalBindingCannotMove(t *testing.T) {
	store, ctx := authFlowStore(t)
	registered, err := store.AutoRegister(ctx, "firebase", "provider-owner")
	if err != nil {
		t.Fatal(err)
	}
	linkNonce := testNonce(t)
	link, err := store.BeginProviderOperation(ctx, registered.HumanID, "provider-owner", "github.com", "link", "same_email_recovery", linkNonce)
	if err != nil {
		t.Fatal(err)
	}
	event, err := store.CompleteProviderLink(ctx, link.OperationID, linkNonce, "provider-owner", "github-subject")
	if err != nil {
		t.Fatal(err)
	}
	if event.EventType != "provider_linked" || event.TerminalOutcome != "linked" {
		t.Fatalf("link event: %+v", event)
	}

	other, err := store.AutoRegister(ctx, "firebase", "other-owner")
	if err != nil {
		t.Fatal(err)
	}
	otherNonce := testNonce(t)
	otherLink, err := store.BeginProviderOperation(ctx, other.HumanID, "other-owner", "github.com", "link", "provider_sign_in", otherNonce)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteProviderLink(ctx, otherLink.OperationID, otherNonce, "other-owner", "github-subject"); !errors.Is(err, ErrCredentialAlreadyBound) {
		t.Fatalf("cross-Human link: %v", err)
	}

	unlinkNonce := testNonce(t)
	unlink, err := store.BeginProviderOperation(ctx, registered.HumanID, "provider-owner", "github.com", "unlink", "notice_action", unlinkNonce)
	if err != nil {
		t.Fatal(err)
	}
	event, err = store.CompleteProviderUnlink(ctx, unlink.OperationID, unlinkNonce, "provider-owner", "github-subject")
	if err != nil {
		t.Fatal(err)
	}
	if event.EventType != "provider_unlinked" || event.TerminalOutcome != "unlinked" {
		t.Fatalf("unlink event: %+v", event)
	}
	var active bool
	if err := store.pool.QueryRow(ctx, "SELECT active FROM credentials WHERE provider='github.com' AND external_subject='github-subject'").Scan(&active); err != nil || active {
		t.Fatalf("historical credential active=%v err=%v", active, err)
	}
	if _, err := store.pool.Exec(ctx, "DELETE FROM credentials WHERE provider='github.com' AND external_subject='github-subject'"); err == nil {
		t.Fatal("historical credential deletion succeeded")
	}
	if _, err := store.pool.Exec(ctx, "UPDATE credential_security_events SET terminal_outcome='tampered' WHERE event_id=$1", event.EventID); err == nil {
		t.Fatal("security event mutation succeeded")
	}
	if _, err := store.CompleteProviderUnlink(ctx, unlink.OperationID, unlinkNonce, "provider-owner", "github-subject"); !errors.Is(err, ErrAuthFlowConsumed) {
		t.Fatalf("unlink replay: %v", err)
	}
}

func TestProviderAuthFlowBindsSubjectWithoutUsingEmail(t *testing.T) {
	store, ctx := authFlowStore(t)
	nonce := testNonce(t)
	flow, err := store.StartAuthFlow(ctx, StartAuthFlowRequest{
		Intent: IntentSignUp, Channel: ChannelProvider, ExpectedProvider: "github.com",
		Continuation: "/direct-chat", Nonce: nonce, TTL: 10 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, VerifiedIdentity{
		FirebaseUID: "provider-new-uid", SignInProvider: "github.com", ProviderSubject: "provider-new-subject",
		NormalizedEmail: "same-as-someone@example.com", EmailVerified: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.TerminalOutcome != OutcomeAccountCreated {
		t.Fatalf("result: %+v", result)
	}
	resolved, err := store.ResolveCredential(ctx, "github.com", "provider-new-subject")
	if err != nil || resolved != result.HumanID {
		t.Fatalf("provider binding: %q %v", resolved, err)
	}
	var events int
	if err := store.pool.QueryRow(ctx, "SELECT count(*) FROM credential_security_events WHERE human_id=$1 AND event_type='provider_linked'", result.HumanID).Scan(&events); err != nil || events != 1 {
		t.Fatalf("security events=%d err=%v", events, err)
	}
}
