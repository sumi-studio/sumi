package koseki

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"testing"
)

func testEpochHash(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// Once a flow is closed — logout, discard, or an account switch retiring its
// issuer — every proof, resolve, replay, and status path reports it as
// consumed. Nothing it could mint may ever mint again.
func TestClosedAuthFlowRefusesProofResolveAndStatus(t *testing.T) {
	store, ctx := emailCodeStore(t)
	nonce := testNonce(t)
	flow, state := startCodeFlow(t, ctx, store, IntentSignIn, "closed@example.com", nonce, false)
	code, token := challengeSecrets(t, store, state.ChallengeID)

	if err := store.CloseAuthFlows(ctx, []string{flow.FlowID}); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := store.VerifyEmailCode(ctx, flow.FlowID, nonce, code); !errors.Is(err, ErrAuthFlowConsumed) {
		t.Fatalf("verify on closed flow: %v", err)
	}
	if _, err := store.CompleteEmailLink(ctx, state.ChallengeID, token, nonce, false, ""); !errors.Is(err, ErrAuthFlowConsumed) {
		t.Fatalf("link complete on closed flow: %v", err)
	}
	if _, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, customTokenProof("firebase-uid")); !errors.Is(err, ErrAuthFlowConsumed) {
		t.Fatalf("resolve on closed flow: %v", err)
	}
	if _, err := store.ConfirmAuthFlow(ctx, flow.FlowID, nonce, "continue"); !errors.Is(err, ErrAuthFlowConsumed) {
		t.Fatalf("confirm on closed flow: %v", err)
	}
	if _, err := store.AuthFlowStatus(ctx, flow.FlowID, nonce); !errors.Is(err, ErrAuthFlowConsumed) {
		t.Fatalf("status on closed flow: %v", err)
	}
	inspection, err := store.InspectEmailLink(ctx, state.ChallengeID, token, nonce)
	if err != nil || inspection.State != "consumed" {
		t.Fatalf("link inspection on closed flow: %+v %v", inspection, err)
	}
	// Closing is idempotent so logout and discard retries stay safe.
	if err := store.CloseAuthFlows(ctx, []string{flow.FlowID}); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}
}

// A proof that commits after logout already ran cannot complete: the epoch
// barrier in the session store refuses issuance, and this layer reports the
// mirrored closure.
func TestClosedAuthFlowRefusesLateProof(t *testing.T) {
	store, ctx := emailCodeStore(t)
	nonce := testNonce(t)
	flow, state := startCodeFlow(t, ctx, store, IntentSignIn, "late@example.com", nonce, false)
	code, _ := challengeSecrets(t, store, state.ChallengeID)

	// Prove the mailbox first, then close — the proved replay path is
	// refused alongside fresh proofs.
	if _, err := store.VerifyEmailCode(ctx, flow.FlowID, nonce, code); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if err := store.CloseAuthFlows(ctx, []string{flow.FlowID}); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := store.VerifyEmailCode(ctx, flow.FlowID, nonce, code); !errors.Is(err, ErrAuthFlowConsumed) {
		t.Fatalf("proved replay on closed flow: %v", err)
	}
	if _, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, customTokenProof("firebase-uid")); !errors.Is(err, ErrAuthFlowConsumed) {
		t.Fatalf("resolve on closed flow: %v", err)
	}
}

// BrowserFlowRefForNonce is the discard endpoint's authentication: the nonce
// that owns a flow may read its closure-facing view; nobody else may.
func TestBrowserFlowRefForNonceAuthenticatesAuthority(t *testing.T) {
	store, ctx := emailCodeStore(t)
	nonce := testNonce(t)
	flow, _ := startCodeFlow(t, ctx, store, IntentSignIn, "ref@example.com", nonce, false)
	if _, err := store.pool.Exec(ctx, `UPDATE auth_flows SET browser_epoch_hash=$2 WHERE flow_id=$1`,
		flow.FlowID, testEpochHash("epoch-x")); err != nil {
		t.Fatal(err)
	}

	ref, err := store.BrowserFlowRefForNonce(ctx, flow.FlowID, nonce)
	if err != nil || ref.FlowID != flow.FlowID || ref.EpochHash != testEpochHash("epoch-x") || ref.ClosedAt != nil {
		t.Fatalf("ref: %+v %v", ref, err)
	}
	if _, err := store.BrowserFlowRefForNonce(ctx, flow.FlowID, testNonce(t)); !errors.Is(err, ErrAuthProofMismatch) {
		t.Fatalf("wrong nonce: %v", err)
	}
	if _, err := store.BrowserFlowRefForNonce(ctx, newUUIDv7(), nonce); !errors.Is(err, ErrInvalidAuthFlow) {
		t.Fatalf("unknown flow: %v", err)
	}
}

// OpenBrowserFlows enumerates an epoch's flows that could still issue a
// session — logout's enumeration before it closes the epoch barrier.
func TestOpenBrowserFlowsListsOnlyLiveFlows(t *testing.T) {
	store, ctx := emailCodeStore(t)
	epochA, epochB := testEpochHash("epoch-a"), testEpochHash("epoch-b")

	start := func(email string, intent AuthIntent, invite bool) (AuthFlow, string) {
		nonce := testNonce(t)
		flow, _ := startCodeFlow(t, ctx, store, intent, email, nonce, invite)
		if _, err := store.pool.Exec(ctx, `UPDATE auth_flows SET browser_epoch_hash=$2 WHERE flow_id=$1`,
			flow.FlowID, epochA); err != nil {
			t.Fatal(err)
		}
		return flow, nonce
	}
	open, _ := start("open@example.com", IntentSignIn, false)
	completed, completedNonce := start("done@example.com", IntentSignUp, true)
	closed, _ := start("shut@example.com", IntentSignIn, false)

	// Complete one flow and close another.
	_, completedState, err := store.EmailFlowStatus(ctx, completed.FlowID, completedNonce)
	if err != nil {
		t.Fatal(err)
	}
	code, _ := challengeSecrets(t, store, completedState.ChallengeID)
	if _, err := store.VerifyEmailCode(ctx, completed.FlowID, completedNonce, code); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if _, err := store.BindEmailProofUID(ctx, completed.FlowID, completedNonce, "done-uid"); err != nil {
		t.Fatalf("bind uid: %v", err)
	}
	resolved, err := store.ResolveAuthProof(ctx, completed.FlowID, completedNonce, customTokenProof("done-uid"))
	if err != nil || resolved.Status != "completed" {
		t.Fatalf("resolve: %+v %v", resolved, err)
	}
	if err := store.CloseAuthFlows(ctx, []string{closed.FlowID}); err != nil {
		t.Fatal(err)
	}
	// A flow on another epoch is never listed.
	otherNonce := testNonce(t)
	other, _ := startCodeFlow(t, ctx, store, IntentSignIn, "other@example.com", otherNonce, false)
	if _, err := store.pool.Exec(ctx, `UPDATE auth_flows SET browser_epoch_hash=$2 WHERE flow_id=$1`,
		other.FlowID, epochB); err != nil {
		t.Fatal(err)
	}

	refs, err := store.OpenBrowserFlows(ctx, epochA)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	// The still-pending flow and the completed-but-replayable flow are both
	// enumerated: each could still issue a session, so logout must close them.
	// The already-closed flow and the other epoch's flow are excluded.
	var got []string
	for _, ref := range refs {
		got = append(got, ref.FlowID)
		if ref.EpochHash != epochA {
			t.Fatalf("ref epoch: %+v", ref)
		}
	}
	want := map[string]bool{open.FlowID: true, completed.FlowID: true}
	if len(got) != len(want) {
		t.Fatalf("open flows = %v, want %v", got, want)
	}
	for _, flowID := range got {
		if !want[flowID] {
			t.Fatalf("unexpected flow %q in %v", flowID, got)
		}
	}
}

// Terminal results carry the resolved Human so the browser can refuse a
// silent identity switch instead of trusting whatever session arrived.
func TestAuthFlowTerminalResultsCarryHumanID(t *testing.T) {
	store, ctx := emailCodeStore(t)
	nonce := testNonce(t)
	flow, state := startCodeFlow(t, ctx, store, IntentSignUp, "human@example.com", nonce, true)
	code, _ := challengeSecrets(t, store, state.ChallengeID)
	if _, err := store.VerifyEmailCode(ctx, flow.FlowID, nonce, code); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if _, err := store.BindEmailProofUID(ctx, flow.FlowID, nonce, "human-uid"); err != nil {
		t.Fatalf("bind uid: %v", err)
	}
	resolved, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, customTokenProof("human-uid"))
	if err != nil || resolved.Status != "completed" || resolved.HumanID == "" {
		t.Fatalf("resolve: %+v %v", resolved, err)
	}
}
