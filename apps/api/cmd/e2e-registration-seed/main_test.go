package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

func testPool(t *testing.T) (*pgxpool.Pool, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	pool := testdb.Create(t)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool, ctx
}

func testNonce(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func issueInvite(t *testing.T, ctx context.Context, pool *pgxpool.Pool, email string) (string, string) {
	t.Helper()
	issuer := uuid.Must(uuid.NewV7()).String()
	if _, err := pool.Exec(ctx,
		`INSERT INTO humans (human_id, display_name) VALUES ($1, 'test operator')`, issuer); err != nil {
		t.Fatalf("issuer: %v", err)
	}
	invite, token, err := koseki.New(pool).IssueEnrollmentInvite(ctx, issuer, email, 24*time.Hour)
	if err != nil {
		t.Fatalf("issue invite: %v", err)
	}
	return invite.ID, token
}

// startProviderSignup creates a real pending invited provider sign-up flow via
// the production path — the exact row resolve-synthetic is meant to own.
func startProviderSignup(t *testing.T, ctx context.Context, pool *pgxpool.Pool, inviteToken, nonce string) koseki.AuthFlow {
	t.Helper()
	flow, err := koseki.New(pool).StartAuthFlow(ctx, koseki.StartAuthFlowRequest{
		InviteToken: inviteToken, Intent: koseki.IntentSignUp, Channel: koseki.ChannelProvider,
		ExpectedProvider: "google.com", Continuation: "/direct-chat", Nonce: nonce,
		TTL: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("start provider sign-up flow: %v", err)
	}
	return flow
}

// flowState reads the columns the synthetic resolve is allowed to change.
func flowState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, flowID string) (status, action, uid, email string) {
	t.Helper()
	err := pool.QueryRow(ctx, `SELECT status, COALESCE(confirmation_action,''),
		COALESCE(firebase_uid,''), COALESCE(verified_email,'')
		FROM auth_flows WHERE flow_id=$1`, flowID).Scan(&status, &action, &uid, &email)
	if err != nil {
		t.Fatalf("flow state: %v", err)
	}
	return
}

func assertFlowPending(t *testing.T, ctx context.Context, pool *pgxpool.Pool, flowID string) {
	t.Helper()
	status, action, uid, _ := flowState(t, ctx, pool, flowID)
	if status != "pending" || action != "" || uid != "" {
		t.Fatalf("flow mutated despite refusal: status=%q action=%q uid=%q", status, action, uid)
	}
}

func assertNoNonceLeak(t *testing.T, err error, nonces ...string) {
	t.Helper()
	if err == nil {
		return
	}
	for _, n := range nonces {
		if strings.Contains(err.Error(), n) {
			t.Fatalf("error leaks the nonce: %v", err)
		}
	}
}

func TestResolveSyntheticOwnedFlowSucceeds(t *testing.T) {
	pool, ctx := testPool(t)
	_, token := issueInvite(t, ctx, pool, "registrant@example.com")
	nonce := testNonce(t)
	flow := startProviderSignup(t, ctx, pool, token, nonce)

	if err := resolveSynthetic(ctx, pool, flow.FlowID, nonce, "Registrant@Example.com", ""); err != nil {
		t.Fatalf("resolve owned flow: %v", err)
	}
	status, action, uid, email := flowState(t, ctx, pool, flow.FlowID)
	if status != "confirmation_required" || action != "create_account" {
		t.Fatalf("flow = %q/%q, want confirmation_required/create_account", status, action)
	}
	if !strings.HasPrefix(uid, "synthetic-") {
		t.Fatalf("firebase uid %q lacks synthetic marker", uid)
	}
	// verified_email must be the normalized invite email so the real
	// invite-consumption check at confirm still applies.
	if email != "registrant@example.com" {
		t.Fatalf("verified_email = %q, want normalized invite email", email)
	}

	// The production confirm path still works on the bootstrapped flow and
	// consumes the invite for real.
	confirmed, err := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1").ConfirmAuthFlow(ctx, flow.FlowID, nonce, "create_account")
	if err != nil {
		t.Fatalf("production confirm after synthetic resolve: %v", err)
	}
	if confirmed.TerminalOutcome != "account_created" {
		t.Fatalf("confirm outcome = %q, want account_created", confirmed.TerminalOutcome)
	}
	var consumed bool
	if err := pool.QueryRow(ctx, `SELECT consumed_at IS NOT NULL FROM enrollment_invites`).Scan(&consumed); err != nil {
		t.Fatalf("invite state: %v", err)
	}
	if !consumed {
		t.Fatal("invite was not consumed by the real confirm path")
	}
}

func TestResolveSyntheticRejectsWrongNonce(t *testing.T) {
	pool, ctx := testPool(t)
	_, token := issueInvite(t, ctx, pool, "a@example.com")
	nonce := testNonce(t)
	flow := startProviderSignup(t, ctx, pool, token, nonce)

	wrong := testNonce(t)
	err := resolveSynthetic(ctx, pool, flow.FlowID, wrong, "a@example.com", "")
	if err == nil {
		t.Fatal("wrong nonce resolved the flow")
	}
	assertNoNonceLeak(t, err, nonce, wrong)
	assertFlowPending(t, ctx, pool, flow.FlowID)
}

func TestResolveSyntheticRejectsOtherFlowsNonce(t *testing.T) {
	pool, ctx := testPool(t)
	_, tokenA := issueInvite(t, ctx, pool, "a@example.com")
	_, tokenB := issueInvite(t, ctx, pool, "b@example.com")
	nonceA, nonceB := testNonce(t), testNonce(t)
	flowA := startProviderSignup(t, ctx, pool, tokenA, nonceA)
	flowB := startProviderSignup(t, ctx, pool, tokenB, nonceB)

	// Flow B's nonce must not authorize a mutation of flow A.
	err := resolveSynthetic(ctx, pool, flowA.FlowID, nonceB, "a@example.com", "")
	if err == nil {
		t.Fatal("another flow's nonce authorized the mutation")
	}
	assertNoNonceLeak(t, err, nonceA, nonceB)
	assertFlowPending(t, ctx, pool, flowA.FlowID)
	assertFlowPending(t, ctx, pool, flowB.FlowID)
}

func TestResolveSyntheticRejectsUnknownFlow(t *testing.T) {
	pool, ctx := testPool(t)
	nonce := testNonce(t)
	err := resolveSynthetic(ctx, pool, uuid.Must(uuid.NewV7()).String(), nonce, "a@example.com", "")
	if err == nil {
		t.Fatal("unknown flow resolved")
	}
	assertNoNonceLeak(t, err, nonce)
}

func TestResolveSyntheticRejectsWrongExpectedEmail(t *testing.T) {
	pool, ctx := testPool(t)
	_, token := issueInvite(t, ctx, pool, "bound@example.com")
	nonce := testNonce(t)
	flow := startProviderSignup(t, ctx, pool, token, nonce)

	err := resolveSynthetic(ctx, pool, flow.FlowID, nonce, "other@example.com", "")
	if err == nil {
		t.Fatal("mismatched expected email resolved the flow")
	}
	assertFlowPending(t, ctx, pool, flow.FlowID)
}

func TestResolveSyntheticRejectsNonPending(t *testing.T) {
	pool, ctx := testPool(t)
	_, token := issueInvite(t, ctx, pool, "a@example.com")
	nonce := testNonce(t)
	flow := startProviderSignup(t, ctx, pool, token, nonce)

	if err := resolveSynthetic(ctx, pool, flow.FlowID, nonce, "a@example.com", ""); err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	err := resolveSynthetic(ctx, pool, flow.FlowID, nonce, "a@example.com", "")
	if err == nil {
		t.Fatal("second resolve on a non-pending flow succeeded")
	}
	status, _, uid, _ := flowState(t, ctx, pool, flow.FlowID)
	if status != "confirmation_required" || !strings.HasPrefix(uid, "synthetic-") {
		t.Fatalf("flow clobbered: status=%q uid=%q", status, uid)
	}
}

func TestResolveSyntheticRejectsExpiredFlow(t *testing.T) {
	pool, ctx := testPool(t)
	_, token := issueInvite(t, ctx, pool, "a@example.com")
	nonce := testNonce(t)
	flow := startProviderSignup(t, ctx, pool, token, nonce)
	if _, err := pool.Exec(ctx, `UPDATE auth_flows SET expires_at = now() - interval '1 minute' WHERE flow_id=$1`, flow.FlowID); err != nil {
		t.Fatalf("expire flow: %v", err)
	}
	if err := resolveSynthetic(ctx, pool, flow.FlowID, nonce, "a@example.com", ""); err == nil {
		t.Fatal("expired flow resolved")
	}
	assertFlowPending(t, ctx, pool, flow.FlowID)
}

func TestResolveSyntheticRejectsConsumedInvite(t *testing.T) {
	pool, ctx := testPool(t)
	inviteID, token := issueInvite(t, ctx, pool, "a@example.com")
	nonce := testNonce(t)
	flow := startProviderSignup(t, ctx, pool, token, nonce)
	var consumer string
	if err := pool.QueryRow(ctx, `SELECT issued_by::text FROM enrollment_invites WHERE invite_id=$1`, inviteID).Scan(&consumer); err != nil {
		t.Fatalf("consumer lookup: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE enrollment_invites SET consumed_at=now(), consumed_by=$2 WHERE invite_id=$1`, inviteID, consumer); err != nil {
		t.Fatalf("consume invite: %v", err)
	}
	if err := resolveSynthetic(ctx, pool, flow.FlowID, nonce, "a@example.com", ""); err == nil {
		t.Fatal("flow with consumed invite resolved")
	}
	assertFlowPending(t, ctx, pool, flow.FlowID)
}

func TestResolveSyntheticRejectsRevokedInvite(t *testing.T) {
	pool, ctx := testPool(t)
	inviteID, token := issueInvite(t, ctx, pool, "a@example.com")
	nonce := testNonce(t)
	flow := startProviderSignup(t, ctx, pool, token, nonce)
	if _, err := pool.Exec(ctx, `UPDATE enrollment_invites SET revoked_at=now() WHERE invite_id=$1`, inviteID); err != nil {
		t.Fatalf("revoke invite: %v", err)
	}
	if err := resolveSynthetic(ctx, pool, flow.FlowID, nonce, "a@example.com", ""); err == nil {
		t.Fatal("flow with revoked invite resolved")
	}
	assertFlowPending(t, ctx, pool, flow.FlowID)
}

func TestResolveSyntheticRejectsExpiredInvite(t *testing.T) {
	pool, ctx := testPool(t)
	inviteID, token := issueInvite(t, ctx, pool, "a@example.com")
	nonce := testNonce(t)
	flow := startProviderSignup(t, ctx, pool, token, nonce)
	if _, err := pool.Exec(ctx, `UPDATE enrollment_invites SET expires_at=now() - interval '1 minute' WHERE invite_id=$1`, inviteID); err != nil {
		t.Fatalf("expire invite: %v", err)
	}
	if err := resolveSynthetic(ctx, pool, flow.FlowID, nonce, "a@example.com", ""); err == nil {
		t.Fatal("flow with expired invite resolved")
	}
	assertFlowPending(t, ctx, pool, flow.FlowID)
}

func TestResolveSyntheticRejectsSignInIntent(t *testing.T) {
	pool, ctx := testPool(t)
	nonce := testNonce(t)
	flow, err := koseki.New(pool).StartAuthFlow(ctx, koseki.StartAuthFlowRequest{
		Intent: koseki.IntentSignIn, Channel: koseki.ChannelProvider,
		ExpectedProvider: "google.com", Continuation: "/direct-chat", Nonce: nonce,
		TTL: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("start sign-in flow: %v", err)
	}
	if err := resolveSynthetic(ctx, pool, flow.FlowID, nonce, "a@example.com", ""); err == nil {
		t.Fatal("sign-in flow resolved")
	}
	assertFlowPending(t, ctx, pool, flow.FlowID)
}

func TestResolveSyntheticRejectsEmailChannel(t *testing.T) {
	pool, ctx := testPool(t)
	_, token := issueInvite(t, ctx, pool, "a@example.com")
	nonce := testNonce(t)
	flow, err := koseki.New(pool).StartAuthFlow(ctx, koseki.StartAuthFlowRequest{
		InviteToken: token, Intent: koseki.IntentSignUp, Channel: koseki.ChannelEmailLink,
		ExpectedProvider: "password", NormalizedEmail: "a@example.com",
		Continuation: "/direct-chat", Nonce: nonce, TTL: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("start email flow: %v", err)
	}
	if err := resolveSynthetic(ctx, pool, flow.FlowID, nonce, "a@example.com", ""); err == nil {
		t.Fatal("email-channel flow resolved")
	}
	assertFlowPending(t, ctx, pool, flow.FlowID)
}

func TestResolveSyntheticRejectsUninvitedSignup(t *testing.T) {
	pool, ctx := testPool(t)
	nonce := testNonce(t)
	hash, err := ownershipNonceHash(nonce)
	if err != nil {
		t.Fatalf("hash nonce: %v", err)
	}
	flowID := uuid.Must(uuid.NewV7()).String()
	if _, err := pool.Exec(ctx, `INSERT INTO auth_flows
		(flow_id, nonce_hash, intent, channel, expected_provider, continuation, expires_at)
		VALUES ($1,$2,'sign_up','provider','google.com','/direct-chat',now() + interval '10 minutes')`,
		flowID, hash); err != nil {
		t.Fatalf("insert uninvited flow: %v", err)
	}
	if err := resolveSynthetic(ctx, pool, flowID, nonce, "a@example.com", ""); err == nil {
		t.Fatal("uninvited sign-up flow resolved")
	}
	assertFlowPending(t, ctx, pool, flowID)
}

func TestResolveSyntheticConcurrent(t *testing.T) {
	pool, ctx := testPool(t)
	_, token := issueInvite(t, ctx, pool, "a@example.com")
	nonce := testNonce(t)
	flow := startProviderSignup(t, ctx, pool, token, nonce)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = resolveSynthetic(ctx, pool, flow.FlowID, nonce, "a@example.com", "")
		}(i)
	}
	wg.Wait()
	var succeeded, failed int
	for _, err := range errs {
		if err == nil {
			succeeded++
		} else {
			failed++
			assertNoNonceLeak(t, err, nonce)
		}
	}
	if succeeded != 1 || failed != 1 {
		t.Fatalf("concurrent resolves: %d succeeded, %d failed — want exactly 1/1", succeeded, failed)
	}
	status, _, uid, _ := flowState(t, ctx, pool, flow.FlowID)
	if status != "confirmation_required" || !strings.HasPrefix(uid, "synthetic-") {
		t.Fatalf("flow = %q uid=%q after race", status, uid)
	}
}

func TestResolveSyntheticInviteWithoutEmail(t *testing.T) {
	pool, ctx := testPool(t)
	_, token := issueInvite(t, ctx, pool, "")
	nonce := testNonce(t)
	flow := startProviderSignup(t, ctx, pool, token, nonce)

	if err := resolveSynthetic(ctx, pool, flow.FlowID, nonce, "Claimed@Example.com", ""); err != nil {
		t.Fatalf("resolve with email-less invite: %v", err)
	}
	_, _, _, email := flowState(t, ctx, pool, flow.FlowID)
	if email != "claimed@example.com" {
		t.Fatalf("verified_email = %q, want normalized expected email", email)
	}
}

func TestReadNonceFile(t *testing.T) {
	if _, err := readNonceFile(""); err == nil {
		t.Fatal("empty nonce-file path accepted")
	}
	if _, err := readNonceFile(t.TempDir() + "/missing"); err == nil {
		t.Fatal("missing nonce file accepted")
	}
	path := t.TempDir() + "/nonce.txt"
	nonce := testNonce(t)
	if err := writeFile0600(path, nonce+"\n"); err != nil {
		t.Fatalf("write nonce file: %v", err)
	}
	got, err := readNonceFile(path)
	if err != nil || got != nonce {
		t.Fatalf("readNonceFile = %q, %v", got, err)
	}
}

func TestOwnershipNonceHash(t *testing.T) {
	if _, err := ownershipNonceHash("not-a-nonce"); err == nil {
		t.Fatal("invalid nonce accepted")
	}
	if _, err := ownershipNonceHash(""); err == nil {
		t.Fatal("empty nonce accepted")
	}
	// The derivation must match production: sha256 of the 32 decoded bytes.
	nonce := testNonce(t)
	hash, err := ownershipNonceHash(nonce)
	if err != nil {
		t.Fatalf("valid nonce rejected: %v", err)
	}
	if len(hash) != 32 {
		t.Fatalf("hash len = %d, want 32", len(hash))
	}
	// Error text must not echo the nonce.
	if _, err := ownershipNonceHash("zzz"); err != nil && strings.Contains(err.Error(), "zzz") {
		t.Fatalf("error echoes input: %v", err)
	}
}

func TestResolveSyntheticRejectsInvalidNonce(t *testing.T) {
	pool, ctx := testPool(t)
	_, token := issueInvite(t, ctx, pool, "a@example.com")
	nonce := testNonce(t)
	flow := startProviderSignup(t, ctx, pool, token, nonce)
	if err := resolveSynthetic(ctx, pool, flow.FlowID, "definitely-not-valid", "a@example.com", ""); err == nil {
		t.Fatal("malformed nonce resolved the flow")
	}
	assertFlowPending(t, ctx, pool, flow.FlowID)
}

// writeFile0600 mirrors the operator's private-file convention for tests.
func writeFile0600(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}
