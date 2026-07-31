package koseki

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

var uuidv7Re = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

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
	store := New(pool)

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
	store := New(pool)

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

func TestCredentialBindingRejectsRebindAndDoubleBind(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool := connectTestPool(t, ctx)
	store := New(pool)

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
	store := New(pool)

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

func TestSetResearchConsentTracksDecision(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool := connectTestPool(t, ctx)
	store := New(pool)

	humanID, err := store.MintHuman(ctx)
	if err != nil {
		t.Fatalf("mint human: %v", err)
	}
	// Before any decision: not decided, not granted.
	decided, granted, err := store.ResearchConsentState(ctx, humanID)
	if err != nil {
		t.Fatalf("state before decision: %v", err)
	}
	if decided || granted {
		t.Fatalf("expected undecided before decision, got decided=%v granted=%v", decided, granted)
	}
	// Decline: decided=true, granted=false.
	if err := store.SetResearchConsent(ctx, humanID, false); err != nil {
		t.Fatalf("decline consent: %v", err)
	}
	decided, granted, err = store.ResearchConsentState(ctx, humanID)
	if err != nil {
		t.Fatalf("state after decline: %v", err)
	}
	if !decided || granted {
		t.Fatalf("expected decided+not granted after decline, got decided=%v granted=%v", decided, granted)
	}
	// Change to grant: decided=true, granted=true.
	if err := store.SetResearchConsent(ctx, humanID, true); err != nil {
		t.Fatalf("grant consent: %v", err)
	}
	decided, granted, err = store.ResearchConsentState(ctx, humanID)
	if err != nil {
		t.Fatalf("state after grant: %v", err)
	}
	if !decided || !granted {
		t.Fatalf("expected decided+granted after grant, got decided=%v granted=%v", decided, granted)
	}
	// Change back to decline: decided=true, granted=false.
	if err := store.SetResearchConsent(ctx, humanID, false); err != nil {
		t.Fatalf("decline consent again: %v", err)
	}
	decided, granted, err = store.ResearchConsentState(ctx, humanID)
	if err != nil {
		t.Fatalf("state after re-decline: %v", err)
	}
	if !decided || granted {
		t.Fatalf("expected decided+not granted after re-decline, got decided=%v granted=%v", decided, granted)
	}
}
