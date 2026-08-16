package koseki

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/directchat"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

var uuidv7Re = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
var wrappingKeyRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

func TestWrappingKeyGenerationAndStorageBoundaryRequireCanonicalHex(t *testing.T) {
	first, err := generateWrappingKey()
	if err != nil {
		t.Fatal(err)
	}
	second, err := generateWrappingKey()
	if err != nil {
		t.Fatal(err)
	}
	if !wrappingKeyRe.MatchString(first) || !wrappingKeyRe.MatchString(second) {
		t.Fatal("generated wrapping key is not canonical 64-character lowercase hex")
	}
	if first == second {
		t.Fatal("two generated wrapping keys were equal")
	}
	if got, err := validateStoredWrappingKey(first); err != nil || got != first {
		t.Fatalf("canonical stored wrapping key rejected: %v", err)
	}
	for _, invalid := range []string{
		"short",
		"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"___________________________________________",
	} {
		if _, err := validateStoredWrappingKey(invalid); err == nil {
			t.Fatal("noncanonical stored wrapping key accepted")
		}
	}
}

func TestWrappingKeyIdentityValidation(t *testing.T) {
	for _, valid := range []string{"test-wrapping/v1", "kms:key:2026-08"} {
		if got, err := validateWrappingKeyID(valid); err != nil || got != valid {
			t.Fatalf("valid wrapping key ID rejected: %q %v", valid, err)
		}
	}
	for _, invalid := range []string{"", " padded", "padded ", "line\nbreak", strings.Repeat("x", 256)} {
		if _, err := validateWrappingKeyID(invalid); err == nil {
			t.Fatalf("invalid wrapping key ID accepted: %q", invalid)
		}
	}
}

func TestAgentWrappingKeyFailsClosedWhenHistoricalIDIsUnresolved(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool := connectTestPool(t, ctx)
	store := NewWithWrappingKeyID(pool, "configured-must-not-fallback/v1")
	const humanID = "0198f0f4-9b72-7000-8000-000000000021"
	const agentID = "0198f0f4-9b72-7000-8000-000000000022"
	const key = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, err := pool.Exec(ctx, "INSERT INTO humans (human_id) VALUES ($1)", humanID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		"INSERT INTO agents (personality_agent_id, human_id) VALUES ($1, $2)",
		agentID, humanID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		"INSERT INTO agent_secrets (personality_agent_id, wrapping_key) VALUES ($1, $2)",
		agentID, key); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AgentWrappingKey(ctx, agentID); err == nil {
		t.Fatal("unresolved historical key ID fell back to the configured global ID")
	}
}

func TestNewUUIDv7FormatAndUniqueness(t *testing.T) {
	seen := make(map[string]bool, 1000)
	for i := 0; i < 1000; i++ {
		id := newUUIDv7()
		if !uuidv7Re.MatchString(id) {
			t.Fatalf("minted id is not canonical UUIDv7: %q", id)
		}
		if seen[id] {
			t.Fatalf("duplicate id generated: %q", id)
		}
		seen[id] = true
	}
}

func TestHumanDisplayNameValidationAndExplicitOverride(t *testing.T) {
	if got, err := normalizeHumanDisplayName("  薄明色\nの忘れ路  "); err != nil || got != "薄明色 の忘れ路" {
		t.Fatalf("normalized name = %q, %v", got, err)
	}
	if got, err := normalizeHumanDisplayName("家族\u200d👩"); err != nil || got != "家族\u200d👩" {
		t.Fatalf("ZWJ name = %q, %v", got, err)
	}
	for _, invalid := range []string{"", "\u200d", "\ufe0f", "\u0301", "safe\u202edanger", strings.Repeat("名", MaxHumanDisplayNameRunes+1)} {
		if _, err := normalizeHumanDisplayName(invalid); !errors.Is(err, ErrInvalidDisplayName) {
			t.Fatalf("invalid name accepted: %q, %v", invalid, err)
		}
		if got := initialHumanDisplayName(invalid); got != "" {
			t.Fatalf("invalid provider name persisted: %q => %q", invalid, got)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool := connectTestPool(t, ctx)
	store := NewWithWrappingKeyID(pool, "test-wrapping/v1")
	registration, err := store.AutoRegister(ctx, "firebase", "display-name-owner")
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := store.HumanDisplayName(ctx, registration.HumanID); got != "Sumi" {
		t.Fatalf("new account without provider name = %q", got)
	}
	if got, err := store.UpdateHumanDisplayName(ctx, registration.HumanID, "Sumi"); err != nil || got != "Sumi" {
		t.Fatalf("explicit literal sentinel = %q, %v", got, err)
	}
}

func connectTestPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	pool := testdb.Create(t)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

func TestMintHumanIsUUIDv7AndGloballyUnique(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool := connectTestPool(t, ctx)
	store := NewWithWrappingKeyID(pool, "test-wrapping/v1")

	first, err := store.MintHuman(ctx)
	if err != nil {
		t.Fatalf("mint first human: %v", err)
	}
	if !uuidv7Re.MatchString(first) {
		t.Fatalf("first human id not UUIDv7: %q", first)
	}
	second, err := store.MintHuman(ctx)
	if err != nil {
		t.Fatalf("mint second human: %v", err)
	}
	if first == second {
		t.Fatal("minted human ids are not unique")
	}
	// Global uniqueness is also enforced by the humans primary key: inserting a
	// duplicate must fail.
	_, err = pool.Exec(ctx, "INSERT INTO humans (human_id) VALUES ($1)", first)
	if err == nil {
		t.Fatal("expected duplicate human id insert to fail")
	}
}

func TestMintSecretaryRecordsAgentAndInitialEmployment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool := connectTestPool(t, ctx)
	store := NewWithWrappingKeyID(pool, "test-wrapping/v1")

	humanID, err := store.MintHuman(ctx)
	if err != nil {
		t.Fatalf("mint human: %v", err)
	}
	agentID, err := store.MintSecretary(ctx, humanID)
	if err != nil {
		t.Fatalf("mint secretary: %v", err)
	}
	if !uuidv7Re.MatchString(agentID) {
		t.Fatalf("agent id not UUIDv7: %q", agentID)
	}
	// The agent exists and links back to the Human.
	got, err := store.AgentForHuman(ctx, humanID)
	if err != nil {
		t.Fatalf("agent for human: %v", err)
	}
	if got != agentID {
		t.Fatalf("agent id mismatch: got %q want %q", got, agentID)
	}
	// The initial employment has the Human as active Employer.
	var activeEmployers int
	if err := pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM employments WHERE agent_id = $1 AND ended_at IS NULL",
		agentID).Scan(&activeEmployers); err != nil {
		t.Fatalf("count employments: %v", err)
	}
	if activeEmployers != 1 {
		t.Fatalf("expected 1 active employer, got %d", activeEmployers)
	}
}

func TestEmployerAuthorityLeaseSerializesTransfer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool := connectTestPool(t, ctx)
	lifecycle := directchat.NewLifecycleFence()
	store := NewWithWrappingKeyID(pool, "test-wrapping/v1", lifecycle)
	first, err := store.AutoRegister(ctx, "firebase", "authority-lease-first")
	if err != nil {
		t.Fatalf("auto-register first Human: %v", err)
	}
	second, err := store.AutoRegister(ctx, "firebase", "authority-lease-second")
	if err != nil {
		t.Fatalf("auto-register second Human: %v", err)
	}

	operationStarted := make(chan struct{})
	releaseOperation := make(chan struct{})
	authorized := make(chan error, 1)
	go func() {
		authorized <- store.AuthorizeCurrentHumanEmployer(
			ctx,
			first.HumanID,
			first.AgentID,
			func() error {
				close(operationStarted)
				select {
				case <-releaseOperation:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		)
	}()
	select {
	case <-operationStarted:
	case <-ctx.Done():
		t.Fatalf("Employer-authorized operation did not start: %v", ctx.Err())
	}

	blockedCtx, blockedCancel := context.WithTimeout(ctx, 250*time.Millisecond)
	err = store.TransferEmployment(
		blockedCtx,
		first.AgentID,
		EmployerHuman,
		second.HumanID,
	)
	blockedCancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		close(releaseOperation)
		t.Fatalf("transfer crossed active Employer authority lease: %v", err)
	}
	close(releaseOperation)
	if err := <-authorized; err != nil {
		t.Fatalf("Employer-authorized operation: %v", err)
	}
	if err := store.TransferEmployment(
		ctx,
		first.AgentID,
		EmployerHuman,
		second.HumanID,
	); err != nil {
		t.Fatalf("transfer after authority release: %v", err)
	}
	if err := store.AuthorizeCurrentHumanEmployer(
		ctx,
		first.HumanID,
		first.AgentID,
		func() error { return nil },
	); !errors.Is(err, ErrNotCurrentEmployer) {
		t.Fatalf("former Employer authority after transfer: %v", err)
	}
	if err := store.AuthorizeCurrentHumanEmployer(
		ctx,
		second.HumanID,
		first.AgentID,
		func() error { return nil },
	); err != nil {
		t.Fatalf("successor Employer authority after transfer: %v", err)
	}
}

func TestCredentialBindingRejectsRebindAndDoubleBind(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool := connectTestPool(t, ctx)
	store := NewWithWrappingKeyID(pool, "test-wrapping/v1")

	human1, err := store.MintHuman(ctx)
	if err != nil {
		t.Fatalf("mint human1: %v", err)
	}
	human2, err := store.MintHuman(ctx)
	if err != nil {
		t.Fatalf("mint human2: %v", err)
	}
	if err := store.BindCredential(ctx, "firebase", "uid-aaa", human1); err != nil {
		t.Fatalf("bind credential to human1: %v", err)
	}
	// Double-binding the same credential to a different Human is rejected by the
	// store (unique constraint surfaces as ErrCredentialAlreadyBound).
	err = store.BindCredential(ctx, "firebase", "uid-aaa", human2)
	if !errors.Is(err, ErrCredentialAlreadyBound) {
		t.Fatalf("expected ErrCredentialAlreadyBound, got %v", err)
	}
	// Rebinding via a raw UPDATE is rejected by the database trigger.
	_, err = pool.Exec(ctx,
		"UPDATE credentials SET human_id = $1 WHERE provider='firebase' AND external_subject='uid-aaa'",
		human2)
	if err == nil {
		t.Fatal("expected raw credential rebind to fail via trigger")
	}
	// Resolve returns the original Human.
	got, err := store.ResolveCredential(ctx, "firebase", "uid-aaa")
	if err != nil {
		t.Fatalf("resolve credential: %v", err)
	}
	if got != human1 {
		t.Fatalf("resolved human mismatch: got %q want %q", got, human1)
	}
	// An unbound credential returns pgx.ErrNoRows.
	if _, err := store.ResolveCredential(ctx, "firebase", "uid-unbound"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected pgx.ErrNoRows for unbound credential, got %v", err)
	}
}

func TestResearchConsentRegisterLookupAndRevoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool := connectTestPool(t, ctx)
	store := NewWithWrappingKeyID(pool, "test-wrapping/v1")

	humanID, err := store.MintHuman(ctx)
	if err != nil {
		t.Fatalf("mint human: %v", err)
	}
	active, err := store.ResearchConsentActive(ctx, humanID)
	if err != nil {
		t.Fatalf("consent active before grant: %v", err)
	}
	if active {
		t.Fatal("expected no active consent before grant")
	}
	if err := store.GrantResearchConsent(ctx, humanID); err != nil {
		t.Fatalf("grant consent: %v", err)
	}
	active, err = store.ResearchConsentActive(ctx, humanID)
	if err != nil {
		t.Fatalf("consent active after grant: %v", err)
	}
	if !active {
		t.Fatal("expected active consent after grant")
	}
	// Granting again is a no-op (one active consent per Human).
	if err := store.GrantResearchConsent(ctx, humanID); err != nil {
		t.Fatalf("re-grant consent: %v", err)
	}
	var count int
	if err := pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM research_consents WHERE human_id = $1 AND revoked_at IS NULL",
		humanID).Scan(&count); err != nil {
		t.Fatalf("count active consents: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 active consent, got %d", count)
	}
	if err := store.RevokeResearchConsent(ctx, humanID); err != nil {
		t.Fatalf("revoke consent: %v", err)
	}
	active, err = store.ResearchConsentActive(ctx, humanID)
	if err != nil {
		t.Fatalf("consent active after revoke: %v", err)
	}
	if active {
		t.Fatal("expected no active consent after revoke")
	}
	// Re-granting after revocation opens a new active record.
	if err := store.GrantResearchConsent(ctx, humanID); err != nil {
		t.Fatalf("re-grant after revoke: %v", err)
	}
	active, err = store.ResearchConsentActive(ctx, humanID)
	if err != nil {
		t.Fatalf("consent active after re-grant: %v", err)
	}
	if !active {
		t.Fatal("expected active consent after re-grant")
	}
}
