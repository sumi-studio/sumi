package koseki

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

const enrollmentTestIssuer = "0198f0f4-9b72-7000-8000-000000000099"

func enrollmentIssuer(t *testing.T, ctx context.Context, s *Store) string {
	t.Helper()
	if _, err := s.pool.Exec(ctx, `INSERT INTO humans(human_id,display_name) VALUES($1,'Invitation test operator') ON CONFLICT DO NOTHING`, enrollmentTestIssuer); err != nil {
		t.Fatal(err)
	}
	return enrollmentTestIssuer
}

// Existing auth-flow tests exercise enrollment with an explicit invitation.
func enrollmentTestToken(t *testing.T, ctx context.Context, s *Store, nonce string) string {
	t.Helper()
	hash, err := enrollmentTokenHash(nonce)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.pool.Exec(ctx, `INSERT INTO enrollment_invites(invite_id,token_hash,issued_by,expires_at) VALUES($1,$2,$3,now()+interval '1 day') ON CONFLICT(token_hash) DO NOTHING`, newUUIDv7(), hash, enrollmentIssuer(t, ctx, s))
	if err != nil {
		t.Fatal(err)
	}
	return nonce
}
func inviteFlow(t *testing.T, ctx context.Context, s *Store, token, nonce string, intent AuthIntent) AuthFlow {
	t.Helper()
	f, err := s.StartAuthFlow(ctx, StartAuthFlowRequest{InviteToken: token, Intent: intent, Channel: ChannelProvider, ExpectedProvider: "google.com", Continuation: "/", Nonce: nonce, TTL: time.Minute * 10})
	if err != nil {
		t.Fatal(err)
	}
	return f
}
func inviteProof(uid, email string, verified bool) VerifiedIdentity {
	return VerifiedIdentity{FirebaseUID: uid, SignInProvider: "google.com", ProviderSubject: uid, NormalizedEmail: email, EmailVerified: verified}
}
func TestEnrollmentInvitationBlocksUninvitedCreationAndKeepsSignIn(t *testing.T) {
	s, ctx := authFlowStore(t)
	n := testNonce(t)
	_, err := s.StartAuthFlow(ctx, StartAuthFlowRequest{Intent: IntentSignUp, Channel: ChannelProvider, ExpectedProvider: "google.com", Continuation: "/", Nonce: n, TTL: time.Minute})
	if !errors.Is(err, ErrEnrollmentInvite) {
		t.Fatalf("signup without invitation: %v", err)
	}
	n = testNonce(t)
	f := inviteFlow(t, ctx, s, "", n, IntentSignIn)
	if _, err = s.ResolveAuthProof(ctx, f.FlowID, n, inviteProof("unknown", "", false)); !errors.Is(err, ErrEnrollmentInvite) {
		t.Fatalf("signin signup bypass: %v", err)
	}
	assertRegistryCounts(t, ctx, s, 0, 0)
	reg, err := s.AutoRegister(ctx, "firebase", "known")
	if err != nil {
		t.Fatal(err)
	}
	n = testNonce(t)
	f = inviteFlow(t, ctx, s, "", n, IntentSignIn)
	got, err := s.ResolveAuthProof(ctx, f.FlowID, n, inviteProof("known", "", false))
	if err != nil || got.HumanID != reg.HumanID {
		t.Fatalf("existing signin: %v %+v", err, got)
	}
}
func TestEnrollmentInvitationOneUseConcurrentAndPrivateList(t *testing.T) {
	s, ctx := authFlowStore(t)
	inv, token, err := s.IssueEnrollmentInvite(ctx, enrollmentIssuer(t, ctx, s), "", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(token) != 43 {
		t.Fatalf("token length %d", len(token))
	}
	const count = 5
	flows := make([]AuthFlow, count)
	nonces := make([]string, count)
	for i := range flows {
		nonces[i] = testNonce(t)
		flows[i] = inviteFlow(t, ctx, s, token, nonces[i], IntentSignUp)
	}
	var wg sync.WaitGroup
	results := make(chan error, count)
	for i := range flows {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := s.ResolveAuthProof(ctx, flows[i].FlowID, nonces[i], inviteProof(fmt.Sprintf("race-%d", i), "", false))
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)
	won := 0
	for err := range results {
		if err == nil {
			won++
		} else if !errors.Is(err, ErrEnrollmentInvite) {
			t.Fatal(err)
		}
	}
	if won != 1 {
		t.Fatalf("redemptions=%d", won)
	}
	assertRegistryCounts(t, ctx, s, 1, 1)
	if _, err = s.InspectEnrollmentInvite(ctx, token); !errors.Is(err, ErrEnrollmentInvite) {
		t.Fatalf("used inspect: %v", err)
	}
	list, err := s.ListEnrollmentInvites(ctx)
	if err != nil || len(list) != 1 || list[0].ID != inv.ID || list[0].ConsumedBy == "" {
		t.Fatalf("list %v %+v", err, list)
	}
}
func TestEnrollmentInvitationEmailRevocationAndRollback(t *testing.T) {
	s, ctx := authFlowStore(t)
	issuer := enrollmentIssuer(t, ctx, s)
	inv, token, err := s.IssueEnrollmentInvite(ctx, issuer, "Target@Example.com", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	n := testNonce(t)
	f := inviteFlow(t, ctx, s, token, n, IntentSignUp)
	for _, proof := range []VerifiedIdentity{inviteProof("new", "target@example.com", false), inviteProof("new", "wrong@example.com", true)} {
		if _, err = s.ResolveAuthProof(ctx, f.FlowID, n, proof); !errors.Is(err, ErrEnrollmentInvite) {
			t.Fatalf("email mismatch accepted: %v", err)
		}
	}
	assertRegistryCounts(t, ctx, s, 0, 0)
	// A failure after invitation consumption must leave both account and invite reusable.
	_, err = s.pool.Exec(ctx, `CREATE FUNCTION fail_invited_app() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected install failure'; END $$; CREATE TRIGGER fail_invited_app BEFORE INSERT ON app_installations FOR EACH ROW EXECUTE FUNCTION fail_invited_app()`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ResolveAuthProof(ctx, f.FlowID, n, inviteProof("new", "target@example.com", true)); err == nil {
		t.Fatal("injected failure did not fail")
	}
	assertRegistryCounts(t, ctx, s, 0, 0)
	if _, err = s.InspectEnrollmentInvite(ctx, token); err != nil {
		t.Fatalf("rollback consumed invitation: %v", err)
	}
	if _, err = s.pool.Exec(ctx, `DROP TRIGGER fail_invited_app ON app_installations`); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ResolveAuthProof(ctx, f.FlowID, n, inviteProof("new", "target@example.com", true)); err != nil {
		t.Fatal(err)
	}
	inv, token, err = s.IssueEnrollmentInvite(ctx, issuer, "", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	n = testNonce(t)
	f = inviteFlow(t, ctx, s, token, n, IntentSignUp)
	if err = s.RevokeEnrollmentInvite(ctx, inv.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ResolveAuthProof(ctx, f.FlowID, n, inviteProof("revoked", "", false)); !errors.Is(err, ErrEnrollmentInvite) {
		t.Fatalf("revoked proof: %v", err)
	}
	inv, token, err = s.IssueEnrollmentInvite(ctx, issuer, "", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	n = testNonce(t)
	f = inviteFlow(t, ctx, s, token, n, IntentSignUp)
	if _, err = s.pool.Exec(ctx, `UPDATE enrollment_invites SET expires_at=now()-interval '1 second' WHERE invite_id=$1`, inv.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ResolveAuthProof(ctx, f.FlowID, n, inviteProof("expired", "", false)); !errors.Is(err, ErrEnrollmentInvite) {
		t.Fatalf("expired proof: %v", err)
	}
}

func TestEnrollmentInvitationConfirmationRetainsVerifiedEmailAndChecksRevocation(t *testing.T) {
	s, ctx := authFlowStore(t)
	issuer := enrollmentIssuer(t, ctx, s)
	inv, token, err := s.IssueEnrollmentInvite(ctx, issuer, "target@example.com", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	n := testNonce(t)
	f := inviteFlow(t, ctx, s, token, n, IntentSignIn)
	if _, err = s.ResolveAuthProof(ctx, f.FlowID, n, inviteProof("confirmation", "target@example.com", false)); !errors.Is(err, ErrEnrollmentInvite) {
		t.Fatalf("unverified confirmation: %v", err)
	}
	pending, err := s.ResolveAuthProof(ctx, f.FlowID, n, inviteProof("confirmation", "target@example.com", true))
	if err != nil || pending.ConfirmationAction != ActionCreateAccount {
		t.Fatalf("proof: %v %+v", err, pending)
	}
	if err = s.RevokeEnrollmentInvite(ctx, inv.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ConfirmAuthFlow(ctx, f.FlowID, n, ActionCreateAccount); !errors.Is(err, ErrEnrollmentInvite) {
		t.Fatalf("revoked confirmation: %v", err)
	}
	_, token, err = s.IssueEnrollmentInvite(ctx, issuer, "target@example.com", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	n = testNonce(t)
	f = inviteFlow(t, ctx, s, token, n, IntentSignIn)
	if _, err = s.ResolveAuthProof(ctx, f.FlowID, n, inviteProof("confirmation", "target@example.com", true)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ConfirmAuthFlow(ctx, f.FlowID, n, ActionCreateAccount); err != nil {
		t.Fatalf("verified email lost at confirmation: %v", err)
	}
}

func TestEnrollmentInvitationExpiresWhileWaitingForRowLock(t *testing.T) {
	s, ctx := authFlowStore(t)
	inv, token, err := s.IssueEnrollmentInvite(ctx, enrollmentIssuer(t, ctx, s), "", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	n := testNonce(t)
	f := inviteFlow(t, ctx, s, token, n, IntentSignUp)
	// Publish expiry before acquiring a read-only row lock, so releasing the
	// blocker creates no new tuple version that could incidentally recheck WHERE.
	if _, err = s.pool.Exec(ctx, `UPDATE enrollment_invites SET expires_at=clock_timestamp()+interval '1 second' WHERE invite_id=$1`, inv.ID); err != nil {
		t.Fatal(err)
	}
	blocker, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(ctx)
	if _, err = blocker.Exec(ctx, `SELECT invite_id FROM enrollment_invites WHERE invite_id=$1 FOR UPDATE`, inv.ID); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := s.ResolveAuthProof(ctx, f.FlowID, n, inviteProof("expired-lock", "", false))
		result <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	blocked := false
	for time.Now().Before(deadline) {
		if err = s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%enrollment_invites%')`).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !blocked {
		t.Fatal("redemption never waited for invitation row lock")
	}
	// Database time avoids assumptions about app/database clock skew.
	if _, err = blocker.Exec(ctx, `SELECT pg_sleep(1.1)`); err != nil {
		t.Fatal(err)
	}
	if err = blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-result; !errors.Is(err, ErrEnrollmentInvite) {
		t.Fatalf("post-expiry redemption: %v", err)
	}
	assertRegistryCounts(t, ctx, s, 0, 0)
}

func TestEnrollmentInvitationIssuanceSharesCallerTransaction(t *testing.T) {
	s, ctx := authFlowStore(t)
	issuer := enrollmentIssuer(t, ctx, s)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	_, token, err := s.IssueEnrollmentInviteInTx(ctx, tx, issuer, "", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.InspectEnrollmentInvite(ctx, token); !errors.Is(err, ErrEnrollmentInvite) {
		t.Fatalf("uncommitted invitation visible: %v", err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = s.InspectEnrollmentInvite(ctx, token); !errors.Is(err, ErrEnrollmentInvite) {
		t.Fatalf("rolled back issuance visible: %v", err)
	}
	tx, err = s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	_, token, err = s.IssueEnrollmentInviteInTx(ctx, tx, issuer, "", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = s.InspectEnrollmentInvite(ctx, token); err != nil {
		t.Fatalf("committed issuance missing: %v", err)
	}
}
