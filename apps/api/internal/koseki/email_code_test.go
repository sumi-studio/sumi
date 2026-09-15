package koseki

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

var testEmailChallengeKey = &EmailChallengeKey{KeyID: "test-email/v1", Key: []byte("0123456789abcdef0123456789abcdef")}

func emailCodeStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	store, ctx := authFlowStore(t)
	store.EmailChallengeKey = testEmailChallengeKey
	return store, ctx
}

func startCodeFlow(t *testing.T, ctx context.Context, store *Store, intent AuthIntent, email, nonce string, invite bool) (AuthFlow, EmailChallengeState) {
	t.Helper()
	flow, state, err := startCodeFlowErr(t, ctx, store, intent, email, nonce, invite)
	if err != nil {
		t.Fatalf("start email code flow: %v", err)
	}
	return flow, state
}

func startCodeFlowErr(t *testing.T, ctx context.Context, store *Store, intent AuthIntent, email, nonce string, invite bool) (AuthFlow, EmailChallengeState, error) {
	t.Helper()
	normalized, err := NormalizeEmail(email)
	if err != nil {
		t.Fatal(err)
	}
	token := ""
	if invite {
		token = enrollmentTestToken(t, ctx, store, nonce)
	}
	return store.StartEmailCodeFlow(ctx, StartAuthFlowRequest{
		InviteToken: token, Intent: intent, Channel: ChannelEmailCode, ExpectedProvider: EmailCodeSignInProvider,
		NormalizedEmail: normalized, Continuation: "/direct-chat", Nonce: nonce, TTL: MaxFlowTTL,
	})
}

func challengeSecrets(t *testing.T, store *Store, challengeID string) (string, string) {
	t.Helper()
	code, token, err := store.EmailChallengeKey.EmailChallengeSecrets(challengeID)
	if err != nil {
		t.Fatal(err)
	}
	return code, token
}

func otherCode(code string) string {
	n, _ := strconv.Atoi(code)
	return fmt.Sprintf("%06d", (n+1)%1_000_000)
}

// allowResend moves the flow's send history outside the resend interval.
func allowResend(t *testing.T, ctx context.Context, store *Store, flowID string) {
	t.Helper()
	if _, err := store.pool.Exec(ctx, `UPDATE auth_email_deliveries d SET created_at=d.created_at-interval '1 minute'
		FROM auth_email_challenges c WHERE c.challenge_id=d.challenge_id AND c.flow_id=$1`, flowID); err != nil {
		t.Fatal(err)
	}
}

func customTokenProof(uid string) VerifiedIdentity {
	return VerifiedIdentity{FirebaseUID: uid, SignInProvider: EmailCodeSignInProvider, IssuedAt: time.Now()}
}

func countRows(t *testing.T, ctx context.Context, store *Store, query string, args ...any) int {
	t.Helper()
	var n int
	if err := store.pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestEmailCodeStartCommitsChallengeWithSendIntentAndRetriesIdempotently(t *testing.T) {
	store, ctx := emailCodeStore(t)
	nonce := testNonce(t)
	flow, state := startCodeFlow(t, ctx, store, IntentSignIn, "Person@Example.com", nonce, false)
	if flow.Channel != ChannelEmailCode || state.ChallengeID == "" || state.AttemptsRemaining != EmailCodeMaxAttempts ||
		state.DeliveryStatus != "pending" || !state.ResendAvailableAt.After(state.DeliveryCreatedAt) {
		t.Fatalf("start: %+v %+v", flow, state)
	}
	again, againState := startCodeFlow(t, ctx, store, IntentSignIn, "person@example.com", nonce, false)
	if again.FlowID != flow.FlowID || againState.ChallengeID != state.ChallengeID {
		t.Fatalf("retry changed flow: %+v %+v", again, againState)
	}
	if n := countRows(t, ctx, store, `SELECT count(*) FROM auth_email_deliveries`); n != 1 {
		t.Fatalf("retry queued another email: %d", n)
	}
	if _, err := store.StartAuthFlow(ctx, StartAuthFlowRequest{
		Intent: IntentSignIn, Channel: ChannelEmailCode, ExpectedProvider: EmailCodeSignInProvider,
		NormalizedEmail: "person@example.com", Continuation: "/", Nonce: testNonce(t), TTL: MaxFlowTTL,
	}); !errors.Is(err, ErrInvalidAuthFlow) {
		t.Fatalf("email-code flow started without challenge: %v", err)
	}

	// A failing send-intent insert rolls back the flow and challenge too.
	failing := testNonce(t)
	if _, err := store.pool.Exec(ctx, `ALTER TABLE auth_email_deliveries ADD CONSTRAINT test_block_send CHECK (normalized_email <> 'blocked@example.com')`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := startCodeFlowErr(t, ctx, store, IntentSignIn, "blocked@example.com", failing, false); err == nil {
		t.Fatal("start succeeded without send intent")
	}
	if n := countRows(t, ctx, store, `SELECT count(*) FROM auth_flows WHERE normalized_email='blocked@example.com'`); n != 0 {
		t.Fatalf("flow committed without send intent: %d", n)
	}
	if n := countRows(t, ctx, store, `SELECT count(*) FROM auth_email_challenges WHERE normalized_email='blocked@example.com'`); n != 0 {
		t.Fatalf("challenge committed without send intent: %d", n)
	}
}

func TestEmailCodeWrongGuessesAreCommittedAndLockOnlyThatCode(t *testing.T) {
	store, ctx := emailCodeStore(t)
	nonce := testNonce(t)
	flow, state := startCodeFlow(t, ctx, store, IntentSignIn, "guess@example.com", nonce, false)
	code, token := challengeSecrets(t, store, state.ChallengeID)

	var mismatch *EmailCodeMismatchError
	if _, err := store.VerifyEmailCode(ctx, flow.FlowID, nonce, "12ab56"); !errors.As(err, &mismatch) || mismatch.AttemptsRemaining != -1 {
		t.Fatalf("format-invalid code: %v", err)
	}
	if _, err := store.VerifyEmailCode(ctx, flow.FlowID, testNonce(t), code); !errors.Is(err, ErrInvalidAuthFlow) {
		t.Fatalf("correct code without flow authority: %v", err)
	}
	for want := EmailCodeMaxAttempts - 1; want >= 0; want-- {
		_, err := store.VerifyEmailCode(ctx, flow.FlowID, nonce, otherCode(code))
		if !errors.As(err, &mismatch) || mismatch.AttemptsRemaining != want {
			t.Fatalf("wrong guess want remaining %d: %v", want, err)
		}
	}
	if n := countRows(t, ctx, store, `SELECT failed_code_attempts FROM auth_email_challenges WHERE challenge_id=$1`, state.ChallengeID); n != EmailCodeMaxAttempts {
		t.Fatalf("committed attempts = %d", n)
	}
	if _, err := store.VerifyEmailCode(ctx, flow.FlowID, nonce, code); !errors.Is(err, ErrEmailCodeLocked) {
		t.Fatalf("correct code after lock: %v", err)
	}
	_, status, err := store.EmailFlowStatus(ctx, flow.FlowID, nonce)
	if err != nil || status.AttemptsRemaining != 0 {
		t.Fatalf("status after lock: %+v %v", status, err)
	}

	// Guessing another person's flow requires that flow's nonce; the other
	// flow's code is unaffected.
	otherNonce := testNonce(t)
	otherFlow, otherState := startCodeFlow(t, ctx, store, IntentSignIn, "guess@example.com", otherNonce, false)
	otherCodeValue, _ := challengeSecrets(t, store, otherState.ChallengeID)
	if _, err := store.VerifyEmailCode(ctx, otherFlow.FlowID, otherNonce, otherCodeValue); err != nil {
		t.Fatalf("independent flow code: %v", err)
	}

	// The link in the locked email still proves the mailbox.
	proof, err := store.CompleteEmailLink(ctx, state.ChallengeID, token, nonce, false, "")
	if err != nil || proof.Method != "link" {
		t.Fatalf("link after code lock: %+v %v", proof, err)
	}
}

func TestEmailCodeResendReusesLiveCodeOrSupersedesIt(t *testing.T) {
	store, ctx := emailCodeStore(t)
	nonce := testNonce(t)
	flow, first := startCodeFlow(t, ctx, store, IntentSignIn, "resend@example.com", nonce, false)
	firstCode, firstToken := challengeSecrets(t, store, first.ChallengeID)

	var limited *EmailSendLimitedError
	if _, err := store.ResendEmailChallenge(ctx, flow.FlowID, nonce); !errors.As(err, &limited) ||
		!limited.RetryAt.Equal(first.ResendAvailableAt) {
		t.Fatalf("immediate resend: %v", err)
	}
	allowResend(t, ctx, store, flow.FlowID)
	reused, err := store.ResendEmailChallenge(ctx, flow.FlowID, nonce)
	if err != nil || reused.ChallengeID != first.ChallengeID {
		t.Fatalf("resend with time left must repeat the same code: %+v %v", reused, err)
	}

	// Near expiry, resend rotates. Out-of-order older emails then say so.
	if _, err := store.pool.Exec(ctx, `UPDATE auth_email_challenges SET expires_at=clock_timestamp()+interval '2 minutes' WHERE challenge_id=$1`, first.ChallengeID); err != nil {
		t.Fatal(err)
	}
	allowResend(t, ctx, store, flow.FlowID)
	rotated, err := store.ResendEmailChallenge(ctx, flow.FlowID, nonce)
	if err != nil || rotated.ChallengeID == first.ChallengeID || rotated.AttemptsRemaining != EmailCodeMaxAttempts {
		t.Fatalf("rotation: %+v %v", rotated, err)
	}
	if n := countRows(t, ctx, store, `SELECT count(*) FROM auth_email_deliveries WHERE challenge_id=$1 AND status='pending'`, first.ChallengeID); n != 0 {
		t.Fatalf("superseded challenge still queued to send: %d", n)
	}
	newCode, _ := challengeSecrets(t, store, rotated.ChallengeID)
	if newCode != firstCode {
		if _, err := store.VerifyEmailCode(ctx, flow.FlowID, nonce, firstCode); !errors.Is(err, ErrEmailChallengeSuperseded) {
			t.Fatalf("old code: %v", err)
		}
		if n := countRows(t, ctx, store, `SELECT failed_code_attempts FROM auth_email_challenges WHERE challenge_id=$1`, rotated.ChallengeID); n != 0 {
			t.Fatalf("old code counted as a guess: %d", n)
		}
	}
	inspection, err := store.InspectEmailLink(ctx, first.ChallengeID, firstToken, nonce)
	if err != nil || inspection.State != "superseded" {
		t.Fatalf("old link inspection: %+v %v", inspection, err)
	}
	if _, err := store.CompleteEmailLink(ctx, first.ChallengeID, firstToken, nonce, false, ""); !errors.Is(err, ErrEmailChallengeSuperseded) {
		t.Fatalf("old link complete: %v", err)
	}
	if proof, err := store.VerifyEmailCode(ctx, flow.FlowID, nonce, newCode); err != nil || proof.Method != "code" {
		t.Fatalf("new code: %+v %v", proof, err)
	}
	if _, err := store.ResendEmailChallenge(ctx, flow.FlowID, nonce); !errors.Is(err, ErrEmailChallengeConsumed) {
		t.Fatalf("resend after proof: %v", err)
	}
}

func TestEmailSendLimitIsPerAddressHonestAndSerialized(t *testing.T) {
	store, ctx := emailCodeStore(t)
	const attempts = EmailSendHourlyLimit + 3
	var wg sync.WaitGroup
	results := make(chan error, attempts)
	nonces := make([]string, attempts)
	for i := range nonces {
		nonces[i] = testNonce(t)
	}
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(nonce string) {
			defer wg.Done()
			_, _, err := startCodeFlowErr(t, ctx, store, IntentSignIn, "flood@example.com", nonce, false)
			results <- err
		}(nonces[i])
	}
	wg.Wait()
	close(results)
	succeeded, limitedCount := 0, 0
	var retryAt time.Time
	for err := range results {
		var limited *EmailSendLimitedError
		switch {
		case err == nil:
			succeeded++
		case errors.As(err, &limited):
			limitedCount++
			retryAt = limited.RetryAt
		default:
			t.Fatalf("unexpected start error: %v", err)
		}
	}
	if succeeded != EmailSendHourlyLimit || limitedCount != attempts-EmailSendHourlyLimit {
		t.Fatalf("concurrent sends succeeded=%d limited=%d", succeeded, limitedCount)
	}
	var oldest time.Time
	if err := store.pool.QueryRow(ctx, `SELECT min(created_at) FROM auth_email_deliveries WHERE normalized_email='flood@example.com'`).Scan(&oldest); err != nil {
		t.Fatal(err)
	}
	if !retryAt.Equal(oldest.Add(time.Hour)) {
		t.Fatalf("retry_at %s, want %s", retryAt, oldest.Add(time.Hour))
	}
	if n := countRows(t, ctx, store, `SELECT count(*) FROM auth_flows WHERE normalized_email='flood@example.com'`); n != EmailSendHourlyLimit {
		t.Fatalf("limited start committed a flow: %d", n)
	}
	if _, _, err := startCodeFlowErr(t, ctx, store, IntentSignIn, "other@example.com", testNonce(t), false); err != nil {
		t.Fatalf("limit leaked to another address: %v", err)
	}

	// Already delivered codes keep working while new sends are paused, and a
	// used challenge returns its budget.
	var flowID, challengeID string
	if err := store.pool.QueryRow(ctx, `SELECT f.flow_id, c.challenge_id FROM auth_flows f JOIN auth_email_challenges c USING (flow_id)
		WHERE f.normalized_email='flood@example.com' ORDER BY c.created_at LIMIT 1`).Scan(&flowID, &challengeID); err != nil {
		t.Fatal(err)
	}
	var usedNonce string
	for _, nonce := range nonces {
		if _, _, err := store.EmailFlowStatus(ctx, flowID, nonce); err == nil {
			usedNonce = nonce
		}
	}
	code, _ := challengeSecrets(t, store, challengeID)
	if _, err := store.VerifyEmailCode(ctx, flowID, usedNonce, code); err != nil {
		t.Fatalf("delivered code during send pause: %v", err)
	}
	if _, _, err := startCodeFlowErr(t, ctx, store, IntentSignIn, "flood@example.com", testNonce(t), false); err != nil {
		t.Fatalf("consumed challenge still counted: %v", err)
	}
}

func TestEmailProofRetriesKeepAuthorityAndSecondUseIsDistinct(t *testing.T) {
	store, ctx := emailCodeStore(t)
	registered, err := store.AutoRegister(ctx, "firebase", "email-code-existing")
	if err != nil {
		t.Fatal(err)
	}
	nonce := testNonce(t)
	flow, state := startCodeFlow(t, ctx, store, IntentSignIn, "existing@example.com", nonce, false)
	code, _ := challengeSecrets(t, store, state.ChallengeID)
	proof, err := store.VerifyEmailCode(ctx, flow.FlowID, nonce, code)
	if err != nil || proof.UID != "" || proof.ProvedAt.IsZero() {
		t.Fatalf("verify: %+v %v", proof, err)
	}
	// Lost response: the same authority retries without a code.
	retry, err := store.VerifyEmailCode(ctx, flow.FlowID, nonce, "")
	if err != nil || !retry.ProvedAt.Equal(proof.ProvedAt) {
		t.Fatalf("retry after proof: %+v %v", retry, err)
	}
	if _, err := store.VerifyEmailCode(ctx, flow.FlowID, testNonce(t), code); !errors.Is(err, ErrInvalidAuthFlow) {
		t.Fatalf("other authority reused proved code: %v", err)
	}
	if _, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, customTokenProof("email-code-existing")); !errors.Is(err, ErrAuthProofMismatch) {
		t.Fatalf("resolve before UID binding: %v", err)
	}
	bound, err := store.BindEmailProofUID(ctx, flow.FlowID, nonce, "email-code-existing")
	if err != nil || bound.UID != "email-code-existing" || bound.UIDBoundAt.IsZero() {
		t.Fatalf("bind: %+v %v", bound, err)
	}
	if _, err := store.BindEmailProofUID(ctx, flow.FlowID, nonce, "email-code-existing"); err != nil {
		t.Fatalf("same UID bind retry: %v", err)
	}
	if _, err := store.BindEmailProofUID(ctx, flow.FlowID, nonce, "different-uid"); !errors.Is(err, ErrAuthProofMismatch) {
		t.Fatalf("bind moved to a different UID: %v", err)
	}
	// Failed custom-token exchange: the retry returns the bound UID.
	if retry, err = store.VerifyEmailCode(ctx, flow.FlowID, nonce, ""); err != nil || retry.UID != "email-code-existing" {
		t.Fatalf("retry after bind: %+v %v", retry, err)
	}

	for name, identity := range map[string]VerifiedIdentity{
		"other uid":      customTokenProof("different-uid"),
		"password token": {FirebaseUID: "email-code-existing", SignInProvider: "password", IssuedAt: time.Now(), EmailVerified: true, NormalizedEmail: "existing@example.com"},
		"stale token":    {FirebaseUID: "email-code-existing", SignInProvider: EmailCodeSignInProvider, IssuedAt: bound.UIDBoundAt.Add(-2 * time.Minute)},
		"no iat":         {FirebaseUID: "email-code-existing", SignInProvider: EmailCodeSignInProvider},
	} {
		if _, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, identity); !errors.Is(err, ErrAuthProofMismatch) {
			t.Fatalf("%s accepted: %v", name, err)
		}
	}
	result, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, customTokenProof("email-code-existing"))
	if err != nil || result.TerminalOutcome != OutcomeSignedIn || result.HumanID != registered.HumanID {
		t.Fatalf("resolve: %+v %v", result, err)
	}
	// A lost completion response: inside the replay window the same authority
	// retries without a code and resolves the same principal to the same Human.
	if replay, err := store.VerifyEmailCode(ctx, flow.FlowID, nonce, ""); err != nil || replay.UID != "email-code-existing" || replay.Status != "completed" {
		t.Fatalf("verify replay after completion: %+v %v", replay, err)
	}
	if _, err := store.BindEmailProofUID(ctx, flow.FlowID, nonce, "email-code-existing"); err != nil {
		t.Fatalf("bind replay after completion: %v", err)
	}
	if _, err := store.BindEmailProofUID(ctx, flow.FlowID, nonce, "different-uid"); !errors.Is(err, ErrAuthProofMismatch) {
		t.Fatalf("bind replay moved to a different UID: %v", err)
	}
	replayed, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, customTokenProof("email-code-existing"))
	if err != nil || replayed.TerminalOutcome != OutcomeSignedIn || replayed.HumanID != registered.HumanID || replayed.AgentID != result.AgentID {
		t.Fatalf("resolve replay: %+v %v", replayed, err)
	}
	for name, identity := range map[string]VerifiedIdentity{
		"other uid":      customTokenProof("different-uid"),
		"password token": {FirebaseUID: "email-code-existing", SignInProvider: "password", IssuedAt: time.Now(), EmailVerified: true, NormalizedEmail: "existing@example.com"},
		"stale token":    {FirebaseUID: "email-code-existing", SignInProvider: EmailCodeSignInProvider, IssuedAt: bound.UIDBoundAt.Add(-2 * time.Minute)},
	} {
		if _, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, identity); !errors.Is(err, ErrAuthProofMismatch) {
			t.Fatalf("replay with %s accepted: %v", name, err)
		}
	}
	if _, err := store.ConfirmAuthFlow(ctx, flow.FlowID, nonce, ActionSignIn); !errors.Is(err, ErrAuthFlowConsumed) {
		t.Fatalf("confirmation replay: %v", err)
	}
	if _, err := store.VerifyEmailCode(ctx, flow.FlowID, testNonce(t), code); !errors.Is(err, ErrInvalidAuthFlow) {
		t.Fatalf("other authority replayed completion: %v", err)
	}
	if n := countRows(t, ctx, store, `SELECT count(*) FROM humans`); n != 1 {
		t.Fatalf("replay created Humans: %d", n)
	}
	// After the window the same authority's use is a distinct second use.
	if _, err := store.pool.Exec(ctx, `UPDATE auth_flows SET completed_at=completed_at-make_interval(secs => $2) WHERE flow_id=$1`,
		flow.FlowID, EmailCompletionReplayWindow.Seconds()+1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.VerifyEmailCode(ctx, flow.FlowID, nonce, code); !errors.Is(err, ErrAuthFlowConsumed) {
		t.Fatalf("second use after completion: %v", err)
	}
	if _, err := store.BindEmailProofUID(ctx, flow.FlowID, nonce, "email-code-existing"); !errors.Is(err, ErrAuthFlowConsumed) {
		t.Fatalf("bind after completion: %v", err)
	}
	if _, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, customTokenProof("email-code-existing")); !errors.Is(err, ErrAuthFlowConsumed) {
		t.Fatalf("resolve after replay window: %v", err)
	}
	status, err := store.AuthFlowStatus(ctx, flow.FlowID, nonce)
	if err != nil || status.TerminalOutcome != OutcomeSignedIn {
		t.Fatalf("status recovery: %+v %v", status, err)
	}
	proved, err := store.HasCompletedEmailLinkProof(ctx, registered.HumanID, "email-code-existing")
	if err != nil || !proved {
		t.Fatalf("completed email-code proof not counted as email method: %v %v", proved, err)
	}
}

func TestEmailCodeKeepsIntentConfirmationAndInvitationSemantics(t *testing.T) {
	prove := func(t *testing.T, ctx context.Context, store *Store, flow AuthFlow, state EmailChallengeState, nonce, uid string) (AuthFlow, error) {
		t.Helper()
		code, _ := challengeSecrets(t, store, state.ChallengeID)
		if _, err := store.VerifyEmailCode(ctx, flow.FlowID, nonce, code); err != nil {
			t.Fatal(err)
		}
		if _, err := store.BindEmailProofUID(ctx, flow.FlowID, nonce, uid); err != nil {
			t.Fatal(err)
		}
		return store.ResolveAuthProof(ctx, flow.FlowID, nonce, customTokenProof(uid))
	}
	t.Run("sign in unknown requires invitation and create confirmation", func(t *testing.T) {
		store, ctx := emailCodeStore(t)
		nonce := testNonce(t)
		flow, state := startCodeFlow(t, ctx, store, IntentSignIn, "nobody@example.com", nonce, false)
		if _, err := prove(t, ctx, store, flow, state, nonce, "code-unknown"); !errors.Is(err, ErrEnrollmentInvite) {
			t.Fatalf("uninvited unknown sign-in: %v", err)
		}
		nonce = testNonce(t)
		flow, state = startCodeFlow(t, ctx, store, IntentSignIn, "invited@example.com", nonce, true)
		pending, err := prove(t, ctx, store, flow, state, nonce, "code-invited")
		if err != nil || pending.Status != "confirmation_required" || pending.ConfirmationAction != ActionCreateAccount {
			t.Fatalf("pending: %+v %v", pending, err)
		}
		assertRegistryCounts(t, ctx, store, 0, 0)
		result, err := store.ConfirmAuthFlow(ctx, flow.FlowID, nonce, ActionCreateAccount)
		if err != nil || result.TerminalOutcome != OutcomeAccountCreated {
			t.Fatalf("confirm: %+v %v", result, err)
		}
		assertRegistryCounts(t, ctx, store, 1, 1)
	})
	t.Run("sign up existing requires sign in confirmation", func(t *testing.T) {
		store, ctx := emailCodeStore(t)
		registered, err := store.AutoRegister(ctx, "firebase", "code-known")
		if err != nil {
			t.Fatal(err)
		}
		nonce := testNonce(t)
		flow, state := startCodeFlow(t, ctx, store, IntentSignUp, "known@example.com", nonce, true)
		pending, err := prove(t, ctx, store, flow, state, nonce, "code-known")
		if err != nil || pending.ConfirmationAction != ActionSignIn {
			t.Fatalf("pending: %+v %v", pending, err)
		}
		result, err := store.ConfirmAuthFlow(ctx, flow.FlowID, nonce, ActionSignIn)
		if err != nil || result.TerminalOutcome != OutcomeSignedIn || result.HumanID != registered.HumanID {
			t.Fatalf("confirm: %+v %v", result, err)
		}
	})
	t.Run("sign up requires invitation and email-bound invitation checks proved address", func(t *testing.T) {
		store, ctx := emailCodeStore(t)
		if _, _, err := startCodeFlowErr(t, ctx, store, IntentSignUp, "new@example.com", testNonce(t), false); !errors.Is(err, ErrEnrollmentInvite) {
			t.Fatalf("uninvited sign-up: %v", err)
		}
		nonce := testNonce(t)
		token := enrollmentTestToken(t, ctx, store, nonce)
		if _, err := store.pool.Exec(ctx, `UPDATE enrollment_invites SET email='bound@example.com' WHERE token_hash=$1`, mustEnrollmentHash(t, token)); err != nil {
			t.Fatal(err)
		}
		flow, state, err := store.StartEmailCodeFlow(ctx, StartAuthFlowRequest{
			InviteToken: token, Intent: IntentSignUp, Channel: ChannelEmailCode, ExpectedProvider: EmailCodeSignInProvider,
			NormalizedEmail: "other@example.com", Continuation: "/", Nonce: nonce, TTL: MaxFlowTTL,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := prove(t, ctx, store, flow, state, nonce, "code-wrong-invitee"); err == nil {
			t.Fatal("email-bound invitation accepted a different proved address")
		}
		assertRegistryCounts(t, ctx, store, 0, 0)
	})
}

func mustEnrollmentHash(t *testing.T, token string) []byte {
	t.Helper()
	hash, err := enrollmentTokenHash(token)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

func TestEmailLinkInspectionDoesNotConsumeAndOtherBrowserMustAdopt(t *testing.T) {
	store, ctx := emailCodeStore(t)
	pwaNonce := testNonce(t)
	flow, state := startCodeFlow(t, ctx, store, IntentSignIn, "link@example.com", pwaNonce, false)
	code, token := challengeSecrets(t, store, state.ChallengeID)

	for _, nonce := range []string{"", testNonce(t), pwaNonce} {
		inspection, err := store.InspectEmailLink(ctx, state.ChallengeID, token, nonce)
		if err != nil || inspection.State != "usable" || inspection.FlowID != flow.FlowID || inspection.SameBrowser != (nonce == pwaNonce) {
			t.Fatalf("inspect nonce=%q: %+v %v", nonce, inspection, err)
		}
	}
	// The last base64url character carries 4 bits, so it can already be "A".
	tampered := token[:42] + "A"
	if tampered == token {
		tampered = token[:42] + "E"
	}
	if _, err := store.InspectEmailLink(ctx, state.ChallengeID, tampered, ""); !errors.Is(err, ErrEmailLinkInvalid) {
		t.Fatalf("tampered token: %v", err)
	}
	browserNonce := testNonce(t)
	if _, err := store.CompleteEmailLink(ctx, state.ChallengeID, token, browserNonce, false, ""); !errors.Is(err, ErrEmailLinkAdoptionRequired) {
		t.Fatalf("other browser without explicit choice: %v", err)
	}
	// Scanning and a declined continuation left the PWA code usable.
	if proof, err := store.VerifyEmailCode(ctx, flow.FlowID, pwaNonce, code); err != nil || proof.Method != "code" {
		t.Fatalf("code after link inspection: %+v %v", proof, err)
	}
	if _, err := store.CompleteEmailLink(ctx, state.ChallengeID, token, browserNonce, true, ""); !errors.Is(err, ErrEmailChallengeConsumed) {
		t.Fatalf("adopt after code proof: %v", err)
	}

	pwaNonce = testNonce(t)
	flow, state = startCodeFlow(t, ctx, store, IntentSignIn, "adopt@example.com", pwaNonce, false)
	code, token = challengeSecrets(t, store, state.ChallengeID)
	browserNonce = testNonce(t)
	proof, err := store.CompleteEmailLink(ctx, state.ChallengeID, token, browserNonce, true, "")
	if err != nil || proof.Method != "link" || proof.FlowID != flow.FlowID {
		t.Fatalf("adopt: %+v %v", proof, err)
	}
	if _, err := store.CompleteEmailLink(ctx, state.ChallengeID, token, browserNonce, false, ""); err != nil {
		t.Fatalf("adopting browser retry: %v", err)
	}
	if _, err := store.VerifyEmailCode(ctx, flow.FlowID, pwaNonce, code); !errors.Is(err, ErrEmailFlowContinuedElsewhere) {
		t.Fatalf("original code after adoption: %v", err)
	}
	if _, _, err := store.EmailFlowStatus(ctx, flow.FlowID, pwaNonce); !errors.Is(err, ErrEmailFlowContinuedElsewhere) {
		t.Fatalf("original status: %v", err)
	}
	if _, err := store.AuthFlowStatus(ctx, flow.FlowID, pwaNonce); !errors.Is(err, ErrEmailFlowContinuedElsewhere) {
		t.Fatalf("original auth status: %v", err)
	}
	if _, err := store.CompleteEmailLink(ctx, state.ChallengeID, token, testNonce(t), true, ""); !errors.Is(err, ErrEmailChallengeConsumed) {
		t.Fatalf("third browser adoption: %v", err)
	}
	if inspection, err := store.InspectEmailLink(ctx, state.ChallengeID, token, browserNonce); err != nil || inspection.State != "proved_here" {
		t.Fatalf("adopter inspection: %+v %v", inspection, err)
	}
	if inspection, err := store.InspectEmailLink(ctx, state.ChallengeID, token, pwaNonce); err != nil || inspection.State != "consumed" {
		t.Fatalf("original inspection: %+v %v", inspection, err)
	}
	if _, err := store.BindEmailProofUID(ctx, flow.FlowID, browserNonce, "adopted-uid"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AutoRegister(ctx, "firebase", "adopted-uid"); err != nil {
		t.Fatal(err)
	}
	if result, err := store.ResolveAuthProof(ctx, flow.FlowID, browserNonce, customTokenProof("adopted-uid")); err != nil || result.TerminalOutcome != OutcomeSignedIn {
		t.Fatalf("adopter resolve: %+v %v", result, err)
	}
	if inspection, err := store.InspectEmailLink(ctx, state.ChallengeID, token, ""); err != nil || inspection.State != "completed" {
		t.Fatalf("completed inspection: %+v %v", inspection, err)
	}
}

func TestEmailCodeAndLinkRaceYieldsExactlyOneAuthority(t *testing.T) {
	store, ctx := emailCodeStore(t)
	for i := 0; i < 8; i++ {
		pwaNonce, browserNonce := testNonce(t), testNonce(t)
		flow, state := startCodeFlow(t, ctx, store, IntentSignIn, fmt.Sprintf("race%d@example.com", i), pwaNonce, false)
		code, token := challengeSecrets(t, store, state.ChallengeID)
		var codeErr, linkErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _, codeErr = store.VerifyEmailCode(ctx, flow.FlowID, pwaNonce, code) }()
		go func() {
			defer wg.Done()
			_, linkErr = store.CompleteEmailLink(ctx, state.ChallengeID, token, browserNonce, true, "")
		}()
		wg.Wait()
		switch {
		case codeErr == nil && errors.Is(linkErr, ErrEmailChallengeConsumed):
			if _, err := store.BindEmailProofUID(ctx, flow.FlowID, browserNonce, "x"); !errors.Is(err, ErrInvalidAuthFlow) {
				t.Fatalf("losing link gained authority: %v", err)
			}
		case linkErr == nil && errors.Is(codeErr, ErrEmailFlowContinuedElsewhere):
			if _, err := store.BindEmailProofUID(ctx, flow.FlowID, pwaNonce, "x"); !errors.Is(err, ErrEmailFlowContinuedElsewhere) {
				t.Fatalf("losing code kept authority: %v", err)
			}
		default:
			t.Fatalf("race %d: code=%v link=%v", i, codeErr, linkErr)
		}
		if n := countRows(t, ctx, store, `SELECT count(*) FROM auth_email_challenges WHERE flow_id=$1 AND consumed_at IS NOT NULL`, flow.FlowID); n != 1 {
			t.Fatalf("consumed challenges = %d", n)
		}
	}
}

func TestEmailChallengeExpiryAndKeyRotationRecoverByResend(t *testing.T) {
	store, ctx := emailCodeStore(t)
	nonce := testNonce(t)
	flow, state := startCodeFlow(t, ctx, store, IntentSignIn, "expiry@example.com", nonce, false)
	code, token := challengeSecrets(t, store, state.ChallengeID)
	if _, err := store.pool.Exec(ctx, `UPDATE auth_email_challenges SET created_at=created_at-interval '20 minutes',
		expires_at=clock_timestamp()-interval '1 second' WHERE challenge_id=$1`, state.ChallengeID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.VerifyEmailCode(ctx, flow.FlowID, nonce, code); !errors.Is(err, ErrEmailChallengeExpired) {
		t.Fatalf("expired code: %v", err)
	}
	if inspection, err := store.InspectEmailLink(ctx, state.ChallengeID, token, ""); err != nil || inspection.State != "expired" {
		t.Fatalf("expired inspection: %+v %v", inspection, err)
	}
	if _, err := store.CompleteEmailLink(ctx, state.ChallengeID, token, nonce, false, ""); !errors.Is(err, ErrEmailChallengeExpired) {
		t.Fatalf("expired link: %v", err)
	}
	allowResend(t, ctx, store, flow.FlowID)
	fresh, err := store.ResendEmailChallenge(ctx, flow.FlowID, nonce)
	if err != nil || fresh.ChallengeID == state.ChallengeID {
		t.Fatalf("resend after expiry: %+v %v", fresh, err)
	}

	rotated := *store
	rotated.EmailChallengeKey = &EmailChallengeKey{KeyID: "test-email/v2", Key: []byte("fedcba9876543210fedcba9876543210")}
	freshCode, freshToken := challengeSecrets(t, store, fresh.ChallengeID)
	if _, err := rotated.VerifyEmailCode(ctx, flow.FlowID, nonce, freshCode); !errors.Is(err, ErrEmailChallengeExpired) {
		t.Fatalf("code under rotated key: %v", err)
	}
	if _, err := rotated.InspectEmailLink(ctx, fresh.ChallengeID, freshToken, ""); !errors.Is(err, ErrEmailLinkInvalid) {
		t.Fatalf("link under rotated key: %v", err)
	}
	allowResend(t, ctx, store, flow.FlowID)
	next, err := rotated.ResendEmailChallenge(ctx, flow.FlowID, nonce)
	if err != nil || next.ChallengeID == fresh.ChallengeID {
		t.Fatalf("resend under rotated key: %+v %v", next, err)
	}
	nextCode, _ := challengeSecrets(t, &rotated, next.ChallengeID)
	if _, err := rotated.VerifyEmailCode(ctx, flow.FlowID, nonce, nextCode); err != nil {
		t.Fatalf("rotated-key code: %v", err)
	}

	expiredNonce := testNonce(t)
	expiredFlow, _ := startCodeFlow(t, ctx, store, IntentSignIn, "flow-expiry@example.com", expiredNonce, false)
	if _, err := store.pool.Exec(ctx, `UPDATE auth_flows SET created_at=created_at-interval '1 hour', expires_at=clock_timestamp()-interval '1 second' WHERE flow_id=$1`, expiredFlow.FlowID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.VerifyEmailCode(ctx, expiredFlow.FlowID, expiredNonce, "000000"); !errors.Is(err, ErrAuthFlowExpired) {
		t.Fatalf("expired flow: %v", err)
	}
	allowResend(t, ctx, store, expiredFlow.FlowID)
	if _, err := store.ResendEmailChallenge(ctx, expiredFlow.FlowID, expiredNonce); !errors.Is(err, ErrAuthFlowExpired) {
		t.Fatalf("resend on expired flow: %v", err)
	}
}

func TestEmailDeliveryOutboxRetriesFailsAndCancelsInactiveIntents(t *testing.T) {
	store, ctx := emailCodeStore(t)
	nonce := testNonce(t)
	flow, state := startCodeFlow(t, ctx, store, IntentSignIn, "outbox@example.com", nonce, false)
	claimed, err := store.ClaimEmailDeliveries(ctx, 10, time.Minute)
	if err != nil || len(claimed) != 1 || claimed[0].Attempts != 1 || claimed[0].ChallengeID != state.ChallengeID {
		t.Fatalf("claim: %+v %v", claimed, err)
	}
	if again, err := store.ClaimEmailDeliveries(ctx, 10, time.Minute); err != nil || len(again) != 0 {
		t.Fatalf("leased intent claimed twice: %+v %v", again, err)
	}
	_, status, err := store.EmailFlowStatus(ctx, flow.FlowID, nonce)
	if err != nil || status.DeliveryStatus != "pending" {
		t.Fatalf("sending status: %+v %v", status, err)
	}
	if err := store.FinishEmailDelivery(ctx, claimed[0].DeliveryID, claimed[0].Attempts, errors.New("smtp 451"), false, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.FinishEmailDelivery(ctx, claimed[0].DeliveryID, claimed[0].Attempts, nil, false, time.Time{}); !errors.Is(err, errEmailDeliveryLeaseSuperseded) {
		t.Fatalf("stale attempt result recorded: %v", err)
	}
	retried, err := store.ClaimEmailDeliveries(ctx, 10, time.Minute)
	if err != nil || len(retried) != 1 || retried[0].Attempts != 2 {
		t.Fatalf("retry claim: %+v %v", retried, err)
	}
	// Lease expiry reclaims the same intent (at-least-once).
	if _, err := store.pool.Exec(ctx, `UPDATE auth_email_deliveries SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE delivery_id=$1`, retried[0].DeliveryID); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := store.ClaimEmailDeliveries(ctx, 10, time.Minute)
	if err != nil || len(reclaimed) != 1 || reclaimed[0].Attempts != 3 {
		t.Fatalf("lease reclaim: %+v %v", reclaimed, err)
	}
	if err := store.FinishEmailDelivery(ctx, reclaimed[0].DeliveryID, 2, nil, false, time.Time{}); !errors.Is(err, errEmailDeliveryLeaseSuperseded) {
		t.Fatalf("expired lease result recorded: %v", err)
	}
	if err := store.FinishEmailDelivery(ctx, reclaimed[0].DeliveryID, 3, errors.New("rejected recipient"), true, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, status, err = store.EmailFlowStatus(ctx, flow.FlowID, nonce); err != nil || status.DeliveryStatus != "failed" {
		t.Fatalf("failed status: %+v %v", status, err)
	}
	var class string
	if err := store.pool.QueryRow(ctx, `SELECT failure_class FROM auth_email_deliveries WHERE delivery_id=$1`, reclaimed[0].DeliveryID).Scan(&class); err != nil || class != "permanent" {
		t.Fatalf("failure class %q %v", class, err)
	}

	// A failed delivery is recoverable by resend, which reuses the live code.
	allowResend(t, ctx, store, flow.FlowID)
	resent, err := store.ResendEmailChallenge(ctx, flow.FlowID, nonce)
	if err != nil || resent.ChallengeID != state.ChallengeID || resent.DeliveryStatus != "pending" {
		t.Fatalf("resend after failed delivery: %+v %v", resent, err)
	}
	code, _ := challengeSecrets(t, store, state.ChallengeID)
	if _, err := store.VerifyEmailCode(ctx, flow.FlowID, nonce, code); err != nil {
		t.Fatal(err)
	}
	if claimed, err := store.ClaimEmailDeliveries(ctx, 10, time.Minute); err != nil || len(claimed) != 0 {
		t.Fatalf("intent for consumed challenge was sent: %+v %v", claimed, err)
	}
	if n := countRows(t, ctx, store, `SELECT count(*) FROM auth_email_deliveries WHERE challenge_id=$1 AND status='cancelled'`, state.ChallengeID); n != 1 {
		t.Fatalf("cancelled intents = %d", n)
	}

	// The final attempt whose lease expired without a result becomes failed.
	exhaustedNonce := testNonce(t)
	startCodeFlow(t, ctx, store, IntentSignIn, "exhausted@example.com", exhaustedNonce, false)
	if _, err := store.pool.Exec(ctx, `UPDATE auth_email_deliveries SET status='sending', attempts=$1,
		lease_expires_at=clock_timestamp()-interval '1 second' WHERE normalized_email='exhausted@example.com'`, EmailDeliveryMaxAttempts); err != nil {
		t.Fatal(err)
	}
	if claimed, err := store.ClaimEmailDeliveries(ctx, 10, time.Minute); err != nil || len(claimed) != 0 {
		t.Fatalf("exhausted intent claimed: %+v %v", claimed, err)
	}
	if n := countRows(t, ctx, store, `SELECT count(*) FROM auth_email_deliveries WHERE normalized_email='exhausted@example.com' AND failure_class='retries_exhausted'`); n != 1 {
		t.Fatalf("exhausted intents = %d", n)
	}
}

func TestDeleteUnboundFirebasePrincipalHoldsCredentialLock(t *testing.T) {
	store, ctx := emailCodeStore(t)
	if _, err := store.AutoRegister(ctx, "firebase", "bound-unverified"); err != nil {
		t.Fatal(err)
	}
	called := false
	if err := store.DeleteUnboundFirebasePrincipal(ctx, "bound-unverified", func(context.Context) error { called = true; return nil }); !errors.Is(err, ErrCredentialAlreadyBound) || called {
		t.Fatalf("bound principal deletion: called=%v %v", called, err)
	}

	nonce := testNonce(t)
	flow, err := store.StartAuthFlow(ctx, StartAuthFlowRequest{
		InviteToken: enrollmentTestToken(t, ctx, store, nonce), Intent: IntentSignIn, Channel: ChannelProvider,
		ExpectedProvider: "github.com", Continuation: "/", Nonce: nonce, TTL: MaxFlowTTL,
	})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, VerifiedIdentity{
		FirebaseUID: "attacker-principal", SignInProvider: "github.com", ProviderSubject: "attacker",
	})
	if err != nil || pending.Status != "confirmation_required" {
		t.Fatalf("pending attacker flow: %+v %v", pending, err)
	}
	failure := errors.New("admin unavailable")
	if err := store.DeleteUnboundFirebasePrincipal(ctx, "attacker-principal", func(context.Context) error { return failure }); !errors.Is(err, failure) {
		t.Fatalf("remove failure: %v", err)
	}
	if _, err := store.AuthFlowStatus(ctx, flow.FlowID, nonce); err != nil {
		t.Fatalf("failed deletion expired flow: %v", err)
	}
	if err := store.DeleteUnboundFirebasePrincipal(ctx, "attacker-principal", func(context.Context) error { called = true; return nil }); err != nil || !called {
		t.Fatalf("unbound deletion: called=%v %v", called, err)
	}
	if _, err := store.ConfirmAuthFlow(ctx, flow.FlowID, nonce, ActionCreateAccount); !errors.Is(err, ErrAuthFlowExpired) {
		t.Fatalf("confirmation for deleted principal: %v", err)
	}
}

func TestEmailCompletionReplayGrantsNoSecondAccountOrInvitation(t *testing.T) {
	store, ctx := emailCodeStore(t)
	proveAndBind := func(flow AuthFlow, state EmailChallengeState, nonce, uid string) {
		t.Helper()
		code, _ := challengeSecrets(t, store, state.ChallengeID)
		if _, err := store.VerifyEmailCode(ctx, flow.FlowID, nonce, code); err != nil {
			t.Fatal(err)
		}
		if _, err := store.BindEmailProofUID(ctx, flow.FlowID, nonce, uid); err != nil {
			t.Fatal(err)
		}
	}
	consumedInvitations := func() int {
		return countRows(t, ctx, store, `SELECT count(*) FROM enrollment_invites WHERE consumed_at IS NOT NULL`)
	}

	// Direct creation through an invited sign-up.
	nonce := testNonce(t)
	flow, state := startCodeFlow(t, ctx, store, IntentSignUp, "replay-new@example.com", nonce, true)
	proveAndBind(flow, state, nonce, "replay-new")
	created, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, customTokenProof("replay-new"))
	if err != nil || created.TerminalOutcome != OutcomeAccountCreated {
		t.Fatalf("create: %+v %v", created, err)
	}
	for i := 0; i < 2; i++ {
		replayed, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, customTokenProof("replay-new"))
		if err != nil || replayed.TerminalOutcome != OutcomeAccountCreated || replayed.HumanID != created.HumanID || replayed.AgentID != created.AgentID {
			t.Fatalf("creation replay %d: %+v %v", i, replayed, err)
		}
	}
	assertRegistryCounts(t, ctx, store, 1, 1)
	if n := consumedInvitations(); n != 1 {
		t.Fatalf("consumed invitations after creation replay: %d", n)
	}

	// Creation through confirmation: the confirmation itself stays consumed,
	// and the resolve replay returns the same Human.
	nonce = testNonce(t)
	flow, state = startCodeFlow(t, ctx, store, IntentSignIn, "replay-invited@example.com", nonce, true)
	proveAndBind(flow, state, nonce, "replay-invited")
	if pending, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, customTokenProof("replay-invited")); err != nil || pending.Status != "confirmation_required" {
		t.Fatalf("pending: %+v %v", pending, err)
	}
	confirmed, err := store.ConfirmAuthFlow(ctx, flow.FlowID, nonce, ActionCreateAccount)
	if err != nil || confirmed.TerminalOutcome != OutcomeAccountCreated {
		t.Fatalf("confirm: %+v %v", confirmed, err)
	}
	if _, err := store.ConfirmAuthFlow(ctx, flow.FlowID, nonce, ActionCreateAccount); !errors.Is(err, ErrAuthFlowConsumed) {
		t.Fatalf("second confirmation: %v", err)
	}
	replayed, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, customTokenProof("replay-invited"))
	if err != nil || replayed.TerminalOutcome != OutcomeAccountCreated || replayed.HumanID != confirmed.HumanID {
		t.Fatalf("confirmed creation replay: %+v %v", replayed, err)
	}
	assertRegistryCounts(t, ctx, store, 2, 2)
	if n := consumedInvitations(); n != 2 {
		t.Fatalf("consumed invitations after confirmation replay: %d", n)
	}

	// A principal that no longer resolves to that Human ends the replay.
	if _, err := store.pool.Exec(ctx, `UPDATE credentials SET active=false, unlinked_at=now()
		WHERE provider='firebase' AND external_subject='replay-invited'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, customTokenProof("replay-invited")); !errors.Is(err, ErrAuthProofMismatch) {
		t.Fatalf("replay for an inactive credential: %v", err)
	}
}

func TestProvedEmailFlowRetriesEndAtFlowExpiry(t *testing.T) {
	store, ctx := emailCodeStore(t)
	nonce := testNonce(t)
	flow, state := startCodeFlow(t, ctx, store, IntentSignIn, "expiring@example.com", nonce, false)
	code, token := challengeSecrets(t, store, state.ChallengeID)
	if _, err := store.VerifyEmailCode(ctx, flow.FlowID, nonce, code); err != nil {
		t.Fatal(err)
	}
	if _, err := store.BindEmailProofUID(ctx, flow.FlowID, nonce, "expiring-uid"); err != nil {
		t.Fatal(err)
	}
	// Before expiry the proof is an ordinary retry by code or by link.
	if _, err := store.VerifyEmailCode(ctx, flow.FlowID, nonce, ""); err != nil {
		t.Fatalf("code retry before expiry: %v", err)
	}
	if _, err := store.CompleteEmailLink(ctx, state.ChallengeID, token, nonce, false, ""); err != nil {
		t.Fatalf("link retry before expiry: %v", err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE auth_flows SET created_at=created_at-interval '1 hour',
		expires_at=clock_timestamp()-interval '1 second' WHERE flow_id=$1`, flow.FlowID); err != nil {
		t.Fatal(err)
	}
	for name, retry := range map[string]func() error{
		"code":    func() error { _, err := store.VerifyEmailCode(ctx, flow.FlowID, nonce, ""); return err },
		"link":    func() error { _, err := store.CompleteEmailLink(ctx, state.ChallengeID, token, nonce, false, ""); return err },
		"bind":    func() error { _, err := store.BindEmailProofUID(ctx, flow.FlowID, nonce, "expiring-uid"); return err },
		"resolve": func() error { _, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, customTokenProof("expiring-uid")); return err },
	} {
		if err := retry(); !errors.Is(err, ErrAuthFlowExpired) {
			t.Fatalf("%s retry after flow expiry: %v", name, err)
		}
	}
}

func TestEmailSendLimitIsHourlyAndLiveLinksStayUsable(t *testing.T) {
	store, ctx := emailCodeStore(t)
	const email = "hourly@example.com"
	var firstState EmailChallengeState
	// Four hours at the hourly limit: 20 admitted intents inside 24 hours.
	for hour := 0; hour < 4; hour++ {
		for i := 0; i < EmailSendHourlyLimit; i++ {
			_, state := startCodeFlow(t, ctx, store, IntentSignIn, email, testNonce(t), false)
			if hour == 3 && i == 0 {
				firstState = state
			}
		}
		if hour < 3 {
			if _, err := store.pool.Exec(ctx, `UPDATE auth_email_deliveries SET created_at=created_at-interval '61 minutes' WHERE normalized_email=$1`, email); err != nil {
				t.Fatal(err)
			}
		}
	}
	var limited *EmailSendLimitedError
	if _, _, err := startCodeFlowErr(t, ctx, store, IntentSignIn, email, testNonce(t), false); !errors.As(err, &limited) {
		t.Fatalf("sixth intent in one hour: %v", err)
	}
	var oldest time.Time
	if err := store.pool.QueryRow(ctx, `SELECT min(created_at) FROM auth_email_deliveries
		WHERE normalized_email=$1 AND created_at > clock_timestamp()-interval '1 hour'`, email).Scan(&oldest); err != nil {
		t.Fatal(err)
	}
	if !limited.RetryAt.Equal(oldest.Add(time.Hour)) {
		t.Fatalf("retry_at %s, want oldest in the hour + 1h %s", limited.RetryAt, oldest.Add(time.Hour))
	}
	// While sends are paused, a delivered live link still proves the mailbox,
	// including from another browser that adopts the flow.
	_, token := challengeSecrets(t, store, firstState.ChallengeID)
	if proof, err := store.CompleteEmailLink(ctx, firstState.ChallengeID, token, testNonce(t), true, ""); err != nil || proof.Method != "link" {
		t.Fatalf("live link during send pause: %+v %v", proof, err)
	}
	// There is no longer pause than the rolling hour, even after 20 intents
	// in 24 hours.
	if _, err := store.pool.Exec(ctx, `UPDATE auth_email_deliveries SET created_at=created_at-interval '61 minutes' WHERE normalized_email=$1`, email); err != nil {
		t.Fatal(err)
	}
	if n := countRows(t, ctx, store, `SELECT count(*) FROM auth_email_deliveries WHERE normalized_email=$1 AND created_at > clock_timestamp()-interval '24 hours'`, email); n < 19 {
		t.Fatalf("setup intents inside 24 hours: %d", n)
	}
	if _, _, err := startCodeFlowErr(t, ctx, store, IntentSignIn, email, testNonce(t), false); err != nil {
		t.Fatalf("send after the hour with 20 intents in 24 hours: %v", err)
	}
}

func TestNormalizeEmailCodeAcceptsPasteForms(t *testing.T) {
	for raw, want := range map[string]string{"123456": "123456", " 123-456 ": "123456", "１２３４５６": "123456", "123　456": "123456"} {
		if got, ok := NormalizeEmailCode(raw); !ok || got != want {
			t.Fatalf("%q => %q %v", raw, got, ok)
		}
	}
	for _, raw := range []string{"", "12345", "1234567", "12a456", "123.456"} {
		if _, ok := NormalizeEmailCode(raw); ok {
			t.Fatalf("%q accepted", raw)
		}
	}
}
