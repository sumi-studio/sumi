package koseki

import (
	"context"
	"errors"
	"testing"
	"time"
)

// seedProviderHuman provisions the registry rows a signed-up account would
// have: Human, Secretary, and the Firebase + provider credentials a provider
// sign-in flow resolves by.
func seedProviderHuman(t *testing.T, ctx context.Context, store *Store, firebaseUID, provider, subject string) (string, string) {
	t.Helper()
	humanID, err := store.MintHuman(ctx)
	if err != nil {
		t.Fatal(err)
	}
	agentID, err := store.MintSecretary(ctx, humanID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `INSERT INTO credentials (provider, external_subject, human_id)
		VALUES ('firebase', $1, $2), ($3, $4, $2)`, firebaseUID, humanID, provider, subject); err != nil {
		t.Fatal(err)
	}
	return humanID, agentID
}

func providerProof(uid, provider, subject string) VerifiedIdentity {
	return VerifiedIdentity{FirebaseUID: uid, SignInProvider: provider, ProviderSubject: subject}
}

func startProviderFlow(t *testing.T, ctx context.Context, store *Store, intent AuthIntent, provider, nonce string) AuthFlow {
	t.Helper()
	flow, err := store.StartAuthFlow(ctx, StartAuthFlowRequest{
		InviteToken: enrollmentTestToken(t, ctx, store, nonce), Intent: intent,
		Channel: ChannelProvider, ExpectedProvider: provider,
		Continuation: "/direct-chat", Nonce: nonce, TTL: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("start provider flow: %v", err)
	}
	return flow
}

func TestProviderCompletionReplayRecoversTheSameOutcome(t *testing.T) {
	store, ctx := authFlowStore(t)
	humanID, agentID := seedProviderHuman(t, ctx, store, "uid-a", "google.com", "subject-a")
	nonce := testNonce(t)
	flow := startProviderFlow(t, ctx, store, IntentSignIn, "google.com", nonce)

	result, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, providerProof("uid-a", "google.com", "subject-a"))
	if err != nil || result.TerminalOutcome != OutcomeSignedIn || result.HumanID != humanID {
		t.Fatalf("resolve: %+v %v", result, err)
	}
	eventsBefore := countRows(t, ctx, store, `SELECT count(*) FROM credential_security_events WHERE human_id=$1`, humanID)

	// The same authority's resolve replays the recorded outcome — the path a
	// consented switch retry or a lost response takes.
	replayed, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, providerProof("uid-a", "google.com", "subject-a"))
	if err != nil || replayed.TerminalOutcome != OutcomeSignedIn || replayed.HumanID != humanID || replayed.AgentID != agentID {
		t.Fatalf("resolve replay: %+v %v", replayed, err)
	}
	if n := countRows(t, ctx, store, `SELECT count(*) FROM credential_security_events WHERE human_id=$1`, humanID); n != eventsBefore {
		t.Fatalf("replay recorded a second credential event: %d -> %d", eventsBefore, n)
	}
	if n := countRows(t, ctx, store, `SELECT count(*) FROM credentials WHERE provider='google.com' AND external_subject='subject-a'`); n != 1 {
		t.Fatalf("replay duplicated the provider credential: %d", n)
	}

	for name, identity := range map[string]VerifiedIdentity{
		"other uid":      providerProof("uid-b", "google.com", "subject-a"),
		"other subject":  providerProof("uid-a", "google.com", "subject-b"),
		"other provider": providerProof("uid-a", "github.com", "subject-a"),
		"empty subject":  {FirebaseUID: "uid-a", SignInProvider: "google.com"},
	} {
		if _, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, identity); !errors.Is(err, ErrAuthProofMismatch) {
			t.Fatalf("replay with %s accepted: %v", name, err)
		}
	}
	if _, err := store.ResolveAuthProof(ctx, flow.FlowID, testNonce(t), providerProof("uid-a", "google.com", "subject-a")); !errors.Is(err, ErrInvalidAuthFlow) {
		t.Fatalf("replay with another nonce: %v", err)
	}

	// A confirm repeats only the action that committed the outcome.
	if _, err := store.ConfirmAuthFlow(ctx, flow.FlowID, nonce, ActionCreateAccount); !errors.Is(err, ErrAuthFlowConsumed) {
		t.Fatalf("confirm replay with the wrong action: %v", err)
	}
	confirmed, err := store.ConfirmAuthFlow(ctx, flow.FlowID, nonce, ActionSignIn)
	if err != nil || confirmed.TerminalOutcome != OutcomeSignedIn || confirmed.HumanID != humanID {
		t.Fatalf("same-action confirm replay: %+v %v", confirmed, err)
	}
	assertRegistryCounts(t, ctx, store, 1, 1)
}

func TestProviderConfirmedCompletionReplaysOnce(t *testing.T) {
	store, ctx := authFlowStore(t)
	humanID, _ := seedProviderHuman(t, ctx, store, "uid-c", "github.com", "subject-c")
	nonce := testNonce(t)
	flow := startProviderFlow(t, ctx, store, IntentSignUp, "github.com", nonce)

	pending, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, providerProof("uid-c", "github.com", "subject-c"))
	if err != nil || pending.Status != "confirmation_required" || pending.ConfirmationAction != ActionSignIn {
		t.Fatalf("resolve: %+v %v", pending, err)
	}
	confirmed, err := store.ConfirmAuthFlow(ctx, flow.FlowID, nonce, ActionSignIn)
	if err != nil || confirmed.TerminalOutcome != OutcomeSignedIn || confirmed.HumanID != humanID {
		t.Fatalf("confirm: %+v %v", confirmed, err)
	}

	// A lost confirm response recovers through either endpoint.
	reConfirmed, err := store.ConfirmAuthFlow(ctx, flow.FlowID, nonce, ActionSignIn)
	if err != nil || reConfirmed.TerminalOutcome != OutcomeSignedIn || reConfirmed.HumanID != humanID {
		t.Fatalf("confirm replay: %+v %v", reConfirmed, err)
	}
	replayed, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, providerProof("uid-c", "github.com", "subject-c"))
	if err != nil || replayed.TerminalOutcome != OutcomeSignedIn || replayed.HumanID != humanID {
		t.Fatalf("resolve replay of confirmed flow: %+v %v", replayed, err)
	}
	if _, err := store.ConfirmAuthFlow(ctx, flow.FlowID, nonce, ActionCreateAccount); !errors.Is(err, ErrAuthFlowConsumed) {
		t.Fatalf("confirm replay with the wrong action: %v", err)
	}
	assertRegistryCounts(t, ctx, store, 1, 1)
}

func TestProviderCompletionReplayIsBounded(t *testing.T) {
	store, ctx := authFlowStore(t)
	seedProviderHuman(t, ctx, store, "uid-w", "google.com", "subject-w")
	consumedInvitations := func() int {
		return countRows(t, ctx, store, `SELECT count(*) FROM enrollment_invites WHERE consumed_at IS NOT NULL`)
	}

	// Account creation replays the same Human and consumes the invite once.
	nonce := testNonce(t)
	flow := startProviderFlow(t, ctx, store, IntentSignUp, "google.com", nonce)
	created, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, providerProof("uid-new", "google.com", "subject-new"))
	if err != nil || created.TerminalOutcome != OutcomeAccountCreated {
		t.Fatalf("create: %+v %v", created, err)
	}
	for i := 0; i < 2; i++ {
		replayed, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, providerProof("uid-new", "google.com", "subject-new"))
		if err != nil || replayed.TerminalOutcome != OutcomeAccountCreated || replayed.HumanID != created.HumanID {
			t.Fatalf("creation replay %d: %+v %v", i, replayed, err)
		}
	}
	if n := consumedInvitations(); n != 1 {
		t.Fatalf("creation replay consumed another invitation: %d", n)
	}
	assertRegistryCounts(t, ctx, store, 2, 2)

	// A closed flow refuses replay even inside the window.
	nonce = testNonce(t)
	closed := startProviderFlow(t, ctx, store, IntentSignIn, "google.com", nonce)
	if _, err := store.ResolveAuthProof(ctx, closed.FlowID, nonce, providerProof("uid-w", "google.com", "subject-w")); err != nil {
		t.Fatal(err)
	}
	if err := store.CloseAuthFlows(ctx, []string{closed.FlowID}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ResolveAuthProof(ctx, closed.FlowID, nonce, providerProof("uid-w", "google.com", "subject-w")); !errors.Is(err, ErrAuthFlowConsumed) {
		t.Fatalf("closed flow replay: %v", err)
	}
	if _, err := store.ConfirmAuthFlow(ctx, closed.FlowID, nonce, ActionSignIn); !errors.Is(err, ErrAuthFlowConsumed) {
		t.Fatalf("closed flow confirm replay: %v", err)
	}

	// Outside the window the same authority's use is consumed.
	nonce = testNonce(t)
	expired := startProviderFlow(t, ctx, store, IntentSignIn, "google.com", nonce)
	if _, err := store.ResolveAuthProof(ctx, expired.FlowID, nonce, providerProof("uid-w", "google.com", "subject-w")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE auth_flows SET completed_at=completed_at-make_interval(secs => $2) WHERE flow_id=$1`,
		expired.FlowID, EmailCompletionReplayWindow.Seconds()+1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ResolveAuthProof(ctx, expired.FlowID, nonce, providerProof("uid-w", "google.com", "subject-w")); !errors.Is(err, ErrAuthFlowConsumed) {
		t.Fatalf("replay outside the window: %v", err)
	}
	if _, err := store.ConfirmAuthFlow(ctx, expired.FlowID, nonce, ActionSignIn); !errors.Is(err, ErrAuthFlowConsumed) {
		t.Fatalf("confirm replay outside the window: %v", err)
	}
}
