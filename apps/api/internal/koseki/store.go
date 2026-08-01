// Package koseki implements the 戸籍 (identity registry) store for the Sumi
// control plane (ADR 0009). It is the trusted provisioning boundary: the only
// component that mints HumanId and PersonalityAgentId values and binds external
// credentials to Humans. The schema is defined by migration 0002 (package db).
package koseki

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Employer types for the employment ledger.
const (
	EmployerHuman     = "human"
	EmployerWorkspace = "workspace"
)

// Warmth settings (ADR 0010 §4): an Employer cost setting, not a personality
// state. Cold (default) stops the runtime when idle; warm keeps it ready.
const (
	WarmthCold = "cold"
	WarmthWarm = "warm"
)

// Sentinel errors for the credential-binding contract (ADR 0009 §2).
var (
	ErrCredentialAlreadyBound = errors.New("credential is already bound to a Human")
	ErrHumanNotFound          = errors.New("human not found")
)

// Store is the trusted provisioning boundary for the 戸籍. All minting and
// credential binding flows through it; no other component writes the registry.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a Store backed by the given pool. The pool must be connected to a
// database that has had the 戸籍 migrations applied.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// MintHuman mints a fresh, globally unique HumanId (UUIDv7) and records the
// Human in the registry. The returned ID is the canonical lowercase hyphenated
// form.
func (s *Store) MintHuman(ctx context.Context) (string, error) {
	humanID := newUUIDv7()
	_, err := s.pool.Exec(ctx,
		"INSERT INTO humans (human_id) VALUES ($1)", humanID)
	if err != nil {
		return "", fmt.Errorf("mint human: %w", err)
	}
	return humanID, nil
}

// MintSecretary mints a fresh PersonalityAgentId (UUIDv7) for the given Human,
// records the agent, and opens the initial employment with the Human as
// Employer (ADR 0009 §4: personal signup hires the Secretary simultaneously).
// The returned ID is the canonical lowercase hyphenated form.
func (s *Store) MintSecretary(ctx context.Context, humanID string) (string, error) {
	if err := s.humanExists(ctx, humanID); err != nil {
		return "", err
	}
	agentID := newUUIDv7()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin mint secretary: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		"INSERT INTO agents (personality_agent_id, human_id) VALUES ($1, $2)",
		agentID, humanID); err != nil {
		return "", fmt.Errorf("insert agent: %w", err)
	}
	if _, err := tx.Exec(ctx,
		"INSERT INTO employments (agent_id, employer_type, employer_id) VALUES ($1, $2, $3)",
		agentID, EmployerHuman, humanID); err != nil {
		return "", fmt.Errorf("insert initial employment: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit mint secretary: %w", err)
	}
	return agentID, nil
}

// BindCredential permanently binds an external credential (provider +
// externalSubject, e.g. "firebase" + UID) to a Human. A credential may be bound
// to exactly one Human for all time; rebinding to a different Human is rejected
// by the database trigger and by this store's unique-constraint handling.
func (s *Store) BindCredential(ctx context.Context, provider, externalSubject, humanID string) error {
	if err := s.humanExists(ctx, humanID); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx,
		"INSERT INTO credentials (provider, external_subject, human_id) VALUES ($1, $2, $3)",
		provider, externalSubject, humanID)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrCredentialAlreadyBound
		}
		return fmt.Errorf("bind credential: %w", err)
	}
	return nil
}

// ResolveCredential looks up the HumanId bound to an external credential. It
// returns the HumanId and nil error when found, or "" and pgx.ErrNoRows when the
// credential is not bound to any Human.
func (s *Store) ResolveCredential(ctx context.Context, provider, externalSubject string) (string, error) {
	var humanID string
	err := s.pool.QueryRow(ctx,
		"SELECT human_id FROM credentials WHERE provider = $1 AND external_subject = $2",
		provider, externalSubject).Scan(&humanID)
	if err != nil {
		return "", err
	}
	return humanID, nil
}

func (s *Store) FirebaseUIDForHuman(ctx context.Context, humanID string) (string, error) {
	var uid string
	err := s.pool.QueryRow(ctx, `SELECT external_subject FROM credentials
		WHERE provider='firebase' AND human_id=$1 AND active`, humanID).Scan(&uid)
	if err != nil {
		return "", err
	}
	return uid, nil
}

// AgentForHuman returns the PersonalityAgentId of the Human's Secretary, or
// pgx.ErrNoRows when none exists.
func (s *Store) AgentForHuman(ctx context.Context, humanID string) (string, error) {
	var agentID string
	err := s.pool.QueryRow(ctx,
		"SELECT personality_agent_id FROM agents WHERE human_id = $1",
		humanID).Scan(&agentID)
	if err != nil {
		return "", err
	}
	return agentID, nil
}

// CurrentEmployer returns the active Employer of an agent (employer_type,
// employer_id) — the employment row with ended_at IS NULL. It returns
// pgx.ErrNoRows when the agent has no active Employer.
func (s *Store) CurrentEmployer(ctx context.Context, agentID string) (string, string, error) {
	var employerType, employerID string
	err := s.pool.QueryRow(ctx,
		"SELECT employer_type, employer_id FROM employments WHERE agent_id = $1 AND ended_at IS NULL",
		agentID).Scan(&employerType, &employerID)
	if err != nil {
		return "", "", err
	}
	return employerType, employerID, nil
}

// ListAgents returns the PersonalityAgentIds of all agents registered in the
// 戸籍. The control plane uses this to provision runtime authorizations
// dynamically instead of from a single env-configured agent.
func (s *Store) ListAgents(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		"SELECT personality_agent_id FROM agents ORDER BY created_at")
	if err != nil {
		return nil, fmt.Errorf("list agents: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan agent id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate agents: %w", err)
	}
	return ids, nil
}

// AgentWarmth returns the warmth setting (cold/warm) of an agent, or
// pgx.ErrNoRows when the agent is not registered.
func (s *Store) AgentWarmth(ctx context.Context, agentID string) (string, error) {
	var warmth string
	err := s.pool.QueryRow(ctx,
		"SELECT warmth FROM agents WHERE personality_agent_id = $1", agentID).Scan(&warmth)
	if err != nil {
		return "", err
	}
	return warmth, nil
}

// Registration is the result of auto-registering a previously unbound credential
// (ADR 0009 §3): a fresh HumanId, the default Secretary's PersonalityAgentId,
// and the per-agent wrapping key generated at hire time.
type Registration struct {
	HumanID     string
	AgentID     string
	WrappingKey string
}

// AutoRegister performs first-login self-serve signup for an unbound credential:
// it mints a HumanId, hires the default Secretary (with the Human as initial
// Employer), generates and persists a per-agent wrapping key, and binds the
// credential to the new Human — all in one transaction. It returns the
// registration result. If the credential is already bound, the caller should
// use ResolveCredential + AgentForHuman instead; AutoRegister does not check for
// an existing binding (the unique constraint would reject a duplicate).
func (s *Store) AutoRegister(ctx context.Context, provider, externalSubject string) (Registration, error) {
	humanID := newUUIDv7()
	agentID := newUUIDv7()
	wrappingKey, err := generateWrappingKey()
	if err != nil {
		return Registration{}, fmt.Errorf("generate wrapping key: %w", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Registration{}, fmt.Errorf("begin auto-register: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		"INSERT INTO humans (human_id) VALUES ($1)", humanID); err != nil {
		return Registration{}, fmt.Errorf("insert human: %w", err)
	}
	if _, err := tx.Exec(ctx,
		"INSERT INTO agents (personality_agent_id, human_id) VALUES ($1, $2)",
		agentID, humanID); err != nil {
		return Registration{}, fmt.Errorf("insert agent: %w", err)
	}
	if _, err := tx.Exec(ctx,
		"INSERT INTO employments (agent_id, employer_type, employer_id) VALUES ($1, $2, $3)",
		agentID, EmployerHuman, humanID); err != nil {
		return Registration{}, fmt.Errorf("insert initial employment: %w", err)
	}
	if _, err := tx.Exec(ctx,
		"INSERT INTO agent_secrets (personality_agent_id, wrapping_key) VALUES ($1, $2)",
		agentID, wrappingKey); err != nil {
		return Registration{}, fmt.Errorf("insert agent secrets: %w", err)
	}
	if _, err := tx.Exec(ctx,
		"INSERT INTO credentials (provider, external_subject, human_id) VALUES ($1, $2, $3)",
		provider, externalSubject, humanID); err != nil {
		if isUniqueViolation(err) {
			return Registration{}, ErrCredentialAlreadyBound
		}
		return Registration{}, fmt.Errorf("bind credential: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Registration{}, fmt.Errorf("commit auto-register: %w", err)
	}
	return Registration{HumanID: humanID, AgentID: agentID, WrappingKey: wrappingKey}, nil
}

// AgentWrappingKey returns the per-agent wrapping key persisted at registration
// time, or pgx.ErrNoRows when none exists.
func (s *Store) AgentWrappingKey(ctx context.Context, agentID string) (string, error) {
	var key string
	err := s.pool.QueryRow(ctx,
		"SELECT wrapping_key FROM agent_secrets WHERE personality_agent_id = $1",
		agentID).Scan(&key)
	if err != nil {
		return "", err
	}
	return key, nil
}

// generateWrappingKey produces a 32-byte random key, base64-rawurl encoded.
func generateWrappingKey() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// GrantResearchConsent registers an active 研究協力 consent for a Human. If an
// active consent already exists this is a no-op; if a previously revoked consent
// exists, a new active record is opened (ADR 0009 §6).
func (s *Store) GrantResearchConsent(ctx context.Context, humanID string) error {
	if err := s.humanExists(ctx, humanID); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO research_consents (human_id)
		 SELECT $1::text WHERE NOT EXISTS (
		   SELECT 1 FROM research_consents WHERE human_id = $1 AND revoked_at IS NULL
		 )`, humanID)
	if err != nil {
		return fmt.Errorf("grant research consent: %w", err)
	}
	return nil
}

// RevokeResearchConsent revokes the active 研究協力 consent for a Human, if any.
// Revoking when there is no active consent is a no-op.
func (s *Store) RevokeResearchConsent(ctx context.Context, humanID string) error {
	_, err := s.pool.Exec(ctx,
		"UPDATE research_consents SET revoked_at = $2 WHERE human_id = $1 AND revoked_at IS NULL",
		humanID, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("revoke research consent: %w", err)
	}
	return nil
}

// ResearchConsentActive reports whether a Human has an active 研究協力 consent.
func (s *Store) ResearchConsentActive(ctx context.Context, humanID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM research_consents WHERE human_id = $1 AND revoked_at IS NULL)",
		humanID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("query research consent: %w", err)
	}
	return exists, nil
}

func (s *Store) humanExists(ctx context.Context, humanID string) error {
	var exists bool
	err := s.pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM humans WHERE human_id = $1)", humanID).Scan(&exists)
	if err != nil {
		return fmt.Errorf("check human exists: %w", err)
	}
	if !exists {
		return ErrHumanNotFound
	}
	return nil
}

// newUUIDv7 returns a canonical lowercase hyphenated UUIDv7 string.
func newUUIDv7() string {
	id, err := uuid.NewV7()
	if err != nil {
		// uuid.NewV7 only fails when the crypto/rand source fails, which is a
		// fatal process condition. Panic so the caller surfaces it immediately.
		panic(fmt.Sprintf("generate uuidv7: %v", err))
	}
	return id.String()
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505" // unique_violation
	}
	return false
}
