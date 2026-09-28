package koseki

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// bootstrapStore is an empty, freshly migrated install.
//
// Until the consolidated schema carries the requested bootstrap DDL
// (reset-bootstrap/SCHEMA-REQUEST.md), apply it here so these tests state
// the target contract. Both statements are no-ops once the schema has it.
func bootstrapStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	return authFlowStore(t)
}

func bootstrapCount(t *testing.T, ctx context.Context, s *Store, query string, args ...any) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestBootstrapInviteOnEmptyInstallCreatesNoHuman(t *testing.T) {
	s, ctx := bootstrapStore(t)
	status, err := s.EnrollmentBootstrapStatus(ctx)
	if err != nil || !status.Open || status.Outstanding != nil || status.Consumed != nil {
		t.Fatalf("empty install status %+v %v", status, err)
	}
	inv, token, err := s.IssueBootstrapEnrollmentInvite(ctx, " First@Example.COM ", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(token) != 43 || inv.Email != "first@example.com" || inv.Superseded != 0 {
		t.Fatalf("invite %+v token length %d", inv, len(token))
	}
	if got := inv.ExpiresAt.Sub(inv.CreatedAt); got < 59*time.Minute || got > 61*time.Minute {
		t.Fatalf("lifetime %s", got)
	}
	if n := bootstrapCount(t, ctx, s, `SELECT count(*) FROM humans`); n != 0 {
		t.Fatalf("bootstrap created %d Human(s)", n)
	}
	if n := bootstrapCount(t, ctx, s, `SELECT count(*) FROM enrollment_invites WHERE issued_by IS NULL AND invite_id=$1`, inv.ID); n != 1 {
		t.Fatal("bootstrap invitation must have no issuing Human")
	}
	// Only the hash is stored; the token is the only way in.
	if n := bootstrapCount(t, ctx, s, `SELECT count(*) FROM enrollment_invites WHERE encode(token_hash,'escape') LIKE '%'||$1||'%'`, token); n != 0 {
		t.Fatal("raw token stored")
	}
	if _, err := s.InspectEnrollmentInvite(ctx, token); err != nil {
		t.Fatalf("issued token not inspectable: %v", err)
	}
	status, err = s.EnrollmentBootstrapStatus(ctx)
	if err != nil || !status.Open || status.Outstanding == nil || status.Outstanding.ID != inv.ID || status.Outstanding.Expired {
		t.Fatalf("status after issue %+v %v", status, err)
	}
}

func TestBootstrapInviteRejectsMisuse(t *testing.T) {
	s, ctx := bootstrapStore(t)
	for _, c := range []struct {
		email string
		ttl   time.Duration
	}{
		{"", time.Hour},                        // an unbound bootstrap token would admit whoever holds it
		{"not-an-email", time.Hour},            // malformed
		{"a@example.com", time.Minute},         // shorter than the minimum
		{"a@example.com", 73 * time.Hour},      // longer than the maximum
		{"a@example.com", -time.Hour},          // nonsense
		{"a@example.com", 30 * 24 * time.Hour}, // product maximum is still too long for bootstrap
	} {
		if _, _, err := s.IssueBootstrapEnrollmentInvite(ctx, c.email, c.ttl); !errors.Is(err, ErrBootstrapInvite) {
			t.Fatalf("%q %s: %v", c.email, c.ttl, err)
		}
	}
	if n := bootstrapCount(t, ctx, s, `SELECT count(*) FROM enrollment_invites`); n != 0 {
		t.Fatalf("misuse left %d invitation(s)", n)
	}
}

// The bootstrap invitation behaves like any email-bound invitation: the
// wrong or unverified email is refused, the right one registers exactly one
// Human, and from then on the bootstrap is closed while ordinary
// Human-issued invitations work.
func TestBootstrapInviteAdmitsOnlyBoundEmailThenCloses(t *testing.T) {
	s, ctx := bootstrapStore(t)
	inv, token, err := s.IssueBootstrapEnrollmentInvite(ctx, "first@example.com", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	n := testNonce(t)
	f := inviteFlow(t, ctx, s, token, n, IntentSignUp)
	for _, proof := range []VerifiedIdentity{
		inviteProof("first-uid", "first@example.com", false),
		inviteProof("first-uid", "other@example.com", true),
	} {
		if _, err := s.ResolveAuthProof(ctx, f.FlowID, n, proof); !errors.Is(err, ErrEnrollmentInvite) {
			t.Fatalf("mismatched proof accepted: %v", err)
		}
	}
	assertRegistryCounts(t, ctx, s, 0, 0)
	got, err := s.ResolveAuthProof(ctx, f.FlowID, n, inviteProof("first-uid", "first@example.com", true))
	if err != nil || got.HumanID == "" {
		t.Fatalf("bound email registration: %v %+v", err, got)
	}
	assertRegistryCounts(t, ctx, s, 1, 1)
	if _, err := s.InspectEnrollmentInvite(ctx, token); !errors.Is(err, ErrEnrollmentInvite) {
		t.Fatalf("consumed bootstrap token still valid: %v", err)
	}

	status, err := s.EnrollmentBootstrapStatus(ctx)
	if err != nil || status.Open || status.Outstanding != nil || status.Consumed == nil ||
		status.Consumed.ID != inv.ID || status.Consumed.ConsumedBy != got.HumanID {
		t.Fatalf("status after registration %+v %v", status, err)
	}
	if _, _, err := s.IssueBootstrapEnrollmentInvite(ctx, "second@example.com", time.Hour); !errors.Is(err, ErrBootstrapClosed) {
		t.Fatalf("bootstrap reopened after registration: %v", err)
	}
	if n := bootstrapCount(t, ctx, s, `SELECT count(*) FROM enrollment_invites WHERE issued_by IS NULL`); n != 1 {
		t.Fatalf("bootstrap invitations %d", n)
	}

	// The admitted Human issues the next invitation through the ordinary path.
	next, nextToken, err := s.IssueEnrollmentInvite(ctx, got.HumanID, "second@example.com", time.Hour)
	if err != nil || next.IssuedBy != got.HumanID {
		t.Fatalf("ordinary invite: %v %+v", err, next)
	}
	n2 := testNonce(t)
	f2 := inviteFlow(t, ctx, s, nextToken, n2, IntentSignUp)
	if _, err := s.ResolveAuthProof(ctx, f2.FlowID, n2, inviteProof("second-uid", "second@example.com", true)); err != nil {
		t.Fatalf("ordinary invited registration: %v", err)
	}
	assertRegistryCounts(t, ctx, s, 2, 2)
	// Invitation-only enrollment is unchanged: no token, no account.
	n3 := testNonce(t)
	if _, err := s.StartAuthFlow(ctx, StartAuthFlowRequest{Intent: IntentSignUp, Channel: ChannelProvider, ExpectedProvider: "google.com", Continuation: "/", Nonce: n3, TTL: time.Minute}); !errors.Is(err, ErrEnrollmentInvite) {
		t.Fatalf("uninvited sign-up: %v", err)
	}
}

// Requires the admin list to skip bootstrap rows (SCHEMA-REQUEST §2): the
// admin surface lists Human-issued invitations and must keep working after
// the install was bootstrapped.
func TestAdminInviteListSurvivesBootstrapRow(t *testing.T) {
	s, ctx := bootstrapStore(t)
	_, token, err := s.IssueBootstrapEnrollmentInvite(ctx, "first@example.com", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	n := testNonce(t)
	f := inviteFlow(t, ctx, s, token, n, IntentSignUp)
	admin, err := s.ResolveAuthProof(ctx, f.FlowID, n, inviteProof("first-uid", "first@example.com", true))
	if err != nil {
		t.Fatal(err)
	}
	issued, _, err := s.IssueEnrollmentInvite(ctx, admin.HumanID, "", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	list, err := s.ListEnrollmentInvites(ctx)
	if err != nil || len(list) != 1 || list[0].ID != issued.ID || list[0].IssuedBy != admin.HumanID {
		t.Fatalf("admin list %v %+v", err, list)
	}
}

func TestBootstrapReissueSupersedesAndExpiryIsHonoured(t *testing.T) {
	s, ctx := bootstrapStore(t)
	first, firstToken, err := s.IssueBootstrapEnrollmentInvite(ctx, "first@example.com", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	second, secondToken, err := s.IssueBootstrapEnrollmentInvite(ctx, "first@example.com", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if second.Superseded != 1 || second.ID == first.ID {
		t.Fatalf("reissue %+v", second)
	}
	if _, err := s.InspectEnrollmentInvite(ctx, firstToken); !errors.Is(err, ErrEnrollmentInvite) {
		t.Fatalf("superseded token still valid: %v", err)
	}
	if _, err := s.InspectEnrollmentInvite(ctx, secondToken); err != nil {
		t.Fatalf("current token: %v", err)
	}

	// An expired bootstrap invitation admits nobody; the operator reissues.
	if _, err := s.pool.Exec(ctx, `UPDATE enrollment_invites SET expires_at = now() - interval '1 second' WHERE invite_id=$1`, second.ID); err != nil {
		t.Fatal(err)
	}
	status, err := s.EnrollmentBootstrapStatus(ctx)
	if err != nil || status.Outstanding == nil || !status.Outstanding.Expired {
		t.Fatalf("expired status %+v %v", status, err)
	}
	n := testNonce(t)
	if _, err := s.StartAuthFlow(ctx, StartAuthFlowRequest{InviteToken: secondToken, Intent: IntentSignUp, Channel: ChannelProvider, ExpectedProvider: "google.com", Continuation: "/", Nonce: n, TTL: time.Minute}); !errors.Is(err, ErrEnrollmentInvite) {
		t.Fatalf("expired token started a sign-up: %v", err)
	}
	third, _, err := s.IssueBootstrapEnrollmentInvite(ctx, "first@example.com", time.Hour)
	if err != nil || third.Superseded != 1 {
		t.Fatalf("reissue after expiry %+v %v", third, err)
	}
	assertRegistryCounts(t, ctx, s, 0, 0)
}

func TestConcurrentBootstrapLeavesOneLiveToken(t *testing.T) {
	s, ctx := bootstrapStore(t)
	const count = 6
	tokens := make(chan string, count)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, token, err := s.IssueBootstrapEnrollmentInvite(ctx, "first@example.com", time.Hour)
			if err != nil {
				errs <- err
				return
			}
			tokens <- token
		}()
	}
	wg.Wait()
	close(tokens)
	close(errs)
	for err := range errs {
		t.Fatalf("serialized bootstrap failed: %v", err)
	}
	live := 0
	for token := range tokens {
		if _, err := s.InspectEnrollmentInvite(ctx, token); err == nil {
			live++
		}
	}
	if live != 1 {
		t.Fatalf("live bootstrap tokens %d, want exactly 1", live)
	}
	if n := bootstrapCount(t, ctx, s, `SELECT count(*) FROM enrollment_invites WHERE issued_by IS NULL AND revoked_at IS NULL`); n != 1 {
		t.Fatalf("unrevoked bootstrap rows %d", n)
	}
}

// A registration that is creating a Human right now decides the race: the
// bootstrap waits for it and is closed if it commits, open if it aborts.
func TestBootstrapWaitsForInFlightHumanCreation(t *testing.T) {
	for _, commit := range []bool{true, false} {
		s, ctx := bootstrapStore(t)
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// A failed assertion must not leave the transaction holding a pool
		// connection: the test database teardown would wait on it forever.
		t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
		if _, err := tx.Exec(ctx, `INSERT INTO humans (human_id, display_name) VALUES ($1, 'In flight')`, newUUIDv7()); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			_, _, err := s.IssueBootstrapEnrollmentInvite(ctx, "first@example.com", time.Hour)
			done <- err
		}()
		select {
		case err := <-done:
			t.Fatalf("commit=%v: bootstrap did not wait for the in-flight Human: %v", commit, err)
		case <-time.After(300 * time.Millisecond):
		}
		if commit {
			err = tx.Commit(ctx)
		} else {
			err = tx.Rollback(ctx)
		}
		if err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if commit && !errors.Is(err, ErrBootstrapClosed) {
				t.Fatalf("after committed Human: %v", err)
			}
			if !commit && err != nil {
				t.Fatalf("after aborted Human: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("commit=%v: bootstrap still blocked", commit)
		}
	}
}
