package koseki

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type AuthIntent string

const (
	IntentSignIn AuthIntent = "sign_in"
	IntentSignUp AuthIntent = "sign_up"

	ChannelEmailLink = "email_link"
	ChannelProvider  = "provider"

	ActionCreateAccount = "create_account"
	ActionSignIn        = "sign_in"

	OutcomeSignedIn       = "signed_in"
	OutcomeAccountCreated = "account_created"
)

const (
	MinFlowTTL = time.Minute
	MaxFlowTTL = 30 * time.Minute
)

var (
	ErrInvalidAuthFlow          = errors.New("invalid authentication flow")
	ErrAuthFlowExpired          = errors.New("authentication flow expired")
	ErrAuthFlowConsumed         = errors.New("authentication flow already consumed")
	ErrAuthProofMismatch        = errors.New("verified identity does not match authentication flow")
	ErrConfirmation             = errors.New("invalid authentication confirmation")
	ErrCredentialInactive       = errors.New("credential login method is inactive")
	ErrLastLoginMethod          = errors.New("last usable login method cannot be removed")
	ErrRecentReauth             = errors.New("recent reauthentication through another method is required")
	ErrProviderOperationPending = errors.New("another provider operation is pending")
)

type StartAuthFlowRequest struct {
	Intent           AuthIntent
	Channel          string
	ExpectedProvider string
	NormalizedEmail  string
	Continuation     string
	Nonce            string
	TTL              time.Duration
}

type AuthFlow struct {
	FlowID                  string
	Intent                  AuthIntent
	Channel                 string
	ExpectedProvider        string
	NormalizedEmail         string
	Continuation            string
	Status                  string
	ConfirmationAction      string
	TerminalOutcome         string
	HumanID                 string
	AgentID                 string
	VerifiedProviderSubject string
	ExpiresAt               time.Time
}

type VerifiedIdentity struct {
	FirebaseUID     string
	NormalizedEmail string
	EmailVerified   bool
	SignInProvider  string
	ProviderSubject string
}

// NormalizeEmail canonicalizes the email value used only to bind a magic-link
// flow to its proof. It is never used to locate or merge Humans.
func NormalizeEmail(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 3 || len(raw) > 320 || strings.Count(raw, "@") != 1 {
		return "", errors.New("invalid email")
	}
	for _, r := range raw {
		if r <= 0x20 || r >= 0x7f {
			return "", errors.New("email must use printable ASCII")
		}
	}
	parts := strings.SplitN(raw, "@", 2)
	if parts[0] == "" || parts[1] == "" || strings.HasPrefix(parts[1], ".") ||
		strings.HasSuffix(parts[1], ".") || !strings.Contains(parts[1], ".") {
		return "", errors.New("invalid email")
	}
	return strings.ToLower(raw), nil
}

func validateNonce(raw string) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil || len(decoded) != 32 {
		return nil, ErrInvalidAuthFlow
	}
	digest := sha256.Sum256(decoded)
	return digest[:], nil
}

func validateContinuation(raw string) error {
	if len(raw) == 0 || len(raw) > 2048 {
		return ErrInvalidAuthFlow
	}
	u, err := url.Parse(raw)
	if err != nil || u.IsAbs() || u.Host != "" || !strings.HasPrefix(u.Path, "/") || strings.HasPrefix(u.Path, "//") {
		return ErrInvalidAuthFlow
	}
	return nil
}

func (s *Store) StartAuthFlow(ctx context.Context, request StartAuthFlowRequest) (AuthFlow, error) {
	if request.Intent != IntentSignIn && request.Intent != IntentSignUp {
		return AuthFlow{}, ErrInvalidAuthFlow
	}
	if request.Channel != ChannelEmailLink && request.Channel != ChannelProvider {
		return AuthFlow{}, ErrInvalidAuthFlow
	}
	if request.TTL < MinFlowTTL || request.TTL > MaxFlowTTL || validateContinuation(request.Continuation) != nil {
		return AuthFlow{}, ErrInvalidAuthFlow
	}
	if request.Channel == ChannelEmailLink {
		if request.ExpectedProvider != "password" || request.NormalizedEmail == "" {
			return AuthFlow{}, ErrInvalidAuthFlow
		}
	} else if (request.ExpectedProvider != "google.com" && request.ExpectedProvider != "github.com") || request.NormalizedEmail != "" {
		return AuthFlow{}, ErrInvalidAuthFlow
	}
	nonceHash, err := validateNonce(request.Nonce)
	if err != nil {
		return AuthFlow{}, err
	}
	flowID := newUUIDv7()
	expiresAt := time.Now().UTC().Add(request.TTL)
	var result AuthFlow
	err = s.pool.QueryRow(ctx, `
		INSERT INTO auth_flows
			(flow_id, nonce_hash, intent, channel, expected_provider, normalized_email, continuation, expires_at)
		VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7, $8)
		ON CONFLICT (nonce_hash) DO UPDATE SET nonce_hash = auth_flows.nonce_hash
		RETURNING flow_id, intent, channel, expected_provider, COALESCE(normalized_email, ''),
			continuation, status, COALESCE(confirmation_action, ''),
			COALESCE(terminal_outcome, ''), COALESCE(human_id::text, ''),
			COALESCE(personality_agent_id::text, ''), expires_at`,
		flowID, nonceHash, request.Intent, request.Channel, request.ExpectedProvider,
		request.NormalizedEmail, request.Continuation, expiresAt,
	).Scan(&result.FlowID, &result.Intent, &result.Channel, &result.ExpectedProvider,
		&result.NormalizedEmail, &result.Continuation, &result.Status,
		&result.ConfirmationAction, &result.TerminalOutcome, &result.HumanID,
		&result.AgentID, &result.ExpiresAt)
	if err != nil {
		return AuthFlow{}, fmt.Errorf("start auth flow: %w", err)
	}
	// A nonce is the idempotency identity. Reusing it with changed semantics is
	// rejected instead of accidentally continuing a different flow.
	if result.Intent != request.Intent || result.Channel != request.Channel ||
		result.ExpectedProvider != request.ExpectedProvider ||
		result.NormalizedEmail != request.NormalizedEmail || result.Continuation != request.Continuation {
		return AuthFlow{}, ErrInvalidAuthFlow
	}
	return result, nil
}

func (s *Store) ResolveAuthProof(ctx context.Context, flowID, nonce string, identity VerifiedIdentity) (AuthFlow, error) {
	return s.advanceAuthFlow(ctx, flowID, nonce, identity, "")
}

func (s *Store) ConfirmAuthFlow(ctx context.Context, flowID, nonce, action string) (AuthFlow, error) {
	return s.advanceAuthFlow(ctx, flowID, nonce, VerifiedIdentity{}, action)
}

// AuthFlowStatus recovers the semantic state after an ambiguous network
// result. It never advances the flow or issues a session; a consumed proof
// therefore cannot be replayed as another login.
func (s *Store) AuthFlowStatus(ctx context.Context, flowID, nonce string) (AuthFlow, error) {
	nonceHash, err := validateNonce(nonce)
	if err != nil {
		return AuthFlow{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AuthFlow{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	flow, _, err := scanAuthFlowForUpdate(ctx, tx, flowID, nonceHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return AuthFlow{}, ErrInvalidAuthFlow
	}
	if err != nil {
		return AuthFlow{}, err
	}
	if flow.Status != "completed" && !time.Now().UTC().Before(flow.ExpiresAt) {
		return AuthFlow{}, ErrAuthFlowExpired
	}
	if err := tx.Commit(ctx); err != nil {
		return AuthFlow{}, err
	}
	return flow, nil
}

func (s *Store) advanceAuthFlow(ctx context.Context, flowID, nonce string, identity VerifiedIdentity, action string) (AuthFlow, error) {
	nonceHash, err := validateNonce(nonce)
	if err != nil {
		return AuthFlow{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AuthFlow{}, fmt.Errorf("begin auth flow: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	flow, firebaseUID, err := scanAuthFlowForUpdate(ctx, tx, flowID, nonceHash)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return AuthFlow{}, ErrInvalidAuthFlow
		}
		return AuthFlow{}, err
	}
	if !time.Now().UTC().Before(flow.ExpiresAt) {
		return AuthFlow{}, ErrAuthFlowExpired
	}
	if flow.Status == "completed" {
		return AuthFlow{}, ErrAuthFlowConsumed
	}

	if action == "" {
		if identity.FirebaseUID == "" || len(identity.FirebaseUID) > 128 {
			return AuthFlow{}, ErrAuthProofMismatch
		}
		if flow.Status == "confirmation_required" {
			if firebaseUID != identity.FirebaseUID {
				return AuthFlow{}, ErrAuthProofMismatch
			}
			return flow, tx.Commit(ctx)
		}
		if flow.Channel == ChannelEmailLink {
			if !identity.EmailVerified || identity.SignInProvider != "password" ||
				identity.NormalizedEmail != flow.NormalizedEmail {
				return AuthFlow{}, ErrAuthProofMismatch
			}
		} else if identity.SignInProvider != flow.ExpectedProvider || identity.ProviderSubject == "" || len(identity.ProviderSubject) > 512 {
			return AuthFlow{}, ErrAuthProofMismatch
		}
		firebaseUID = identity.FirebaseUID
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "8:firebase"+firebaseUID); err != nil {
			return AuthFlow{}, fmt.Errorf("lock Firebase credential: %w", err)
		}
		humanID, agentID, exists, err := resolveHumanTx(ctx, tx, firebaseUID)
		if err != nil {
			return AuthFlow{}, err
		}
		switch {
		case flow.Intent == IntentSignIn && exists:
			if flow.Channel == ChannelProvider {
				if err := syncVerifiedProviderTx(ctx, tx, humanID, flow.ExpectedProvider, identity.ProviderSubject, "provider_sign_in"); err != nil {
					return AuthFlow{}, err
				}
			}
			flow, err = completeExistingFlow(ctx, tx, flow, firebaseUID, humanID, agentID, OutcomeSignedIn)
		case flow.Intent == IntentSignUp && !exists:
			flow, err = s.provisionFromFlow(ctx, tx, flow, identity)
		case flow.Intent == IntentSignIn && !exists:
			flow.ConfirmationAction = ActionCreateAccount
			flow.Status = "confirmation_required"
			flow.VerifiedProviderSubject = identity.ProviderSubject
			_, err = tx.Exec(ctx, `UPDATE auth_flows SET status='confirmation_required',
				confirmation_action=$2, firebase_uid=$3, provider_subject=NULLIF($4,''), proved_at=now() WHERE flow_id=$1`,
				flow.FlowID, flow.ConfirmationAction, firebaseUID, identity.ProviderSubject)
		case flow.Intent == IntentSignUp && exists:
			flow.ConfirmationAction = ActionSignIn
			flow.Status = "confirmation_required"
			flow.HumanID, flow.AgentID = humanID, agentID
			flow.VerifiedProviderSubject = identity.ProviderSubject
			_, err = tx.Exec(ctx, `UPDATE auth_flows SET status='confirmation_required',
				confirmation_action=$2, firebase_uid=$3, human_id=$4,
				personality_agent_id=$5, provider_subject=NULLIF($6,''), proved_at=now() WHERE flow_id=$1`,
				flow.FlowID, flow.ConfirmationAction, firebaseUID, humanID, agentID, identity.ProviderSubject)
		}
		if err != nil {
			return AuthFlow{}, err
		}
	} else {
		if flow.Status != "confirmation_required" || action != flow.ConfirmationAction || firebaseUID == "" {
			return AuthFlow{}, ErrConfirmation
		}
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "8:firebase"+firebaseUID); err != nil {
			return AuthFlow{}, fmt.Errorf("lock Firebase credential: %w", err)
		}
		humanID, agentID, exists, err := resolveHumanTx(ctx, tx, firebaseUID)
		if err != nil {
			return AuthFlow{}, err
		}
		if action == ActionCreateAccount {
			if exists {
				return AuthFlow{}, ErrCredentialAlreadyBound
			}
			flow, err = s.provisionFromFlow(ctx, tx, flow, VerifiedIdentity{FirebaseUID: firebaseUID, SignInProvider: flow.ExpectedProvider, ProviderSubject: flow.VerifiedProviderSubject})
		} else {
			if !exists {
				return AuthFlow{}, ErrAuthProofMismatch
			}
			if flow.Channel == ChannelProvider {
				if err := syncVerifiedProviderTx(ctx, tx, humanID, flow.ExpectedProvider, flow.VerifiedProviderSubject, "provider_sign_in"); err != nil {
					return AuthFlow{}, err
				}
			}
			flow, err = completeExistingFlow(ctx, tx, flow, firebaseUID, humanID, agentID, OutcomeSignedIn)
		}
		if err != nil {
			return AuthFlow{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return AuthFlow{}, fmt.Errorf("commit auth flow: %w", err)
	}
	return flow, nil
}

func scanAuthFlowForUpdate(ctx context.Context, tx pgx.Tx, flowID string, nonceHash []byte) (AuthFlow, string, error) {
	var flow AuthFlow
	var firebaseUID string
	err := tx.QueryRow(ctx, `SELECT flow_id, intent, channel, expected_provider,
		COALESCE(normalized_email, ''), continuation, status,
		COALESCE(confirmation_action, ''), COALESCE(terminal_outcome, ''),
		COALESCE(human_id::text, ''), COALESCE(personality_agent_id::text, ''),
		expires_at, COALESCE(firebase_uid, ''), COALESCE(provider_subject, '') FROM auth_flows
		WHERE flow_id=$1 AND nonce_hash=$2 FOR UPDATE`, flowID, nonceHash).Scan(
		&flow.FlowID, &flow.Intent, &flow.Channel, &flow.ExpectedProvider,
		&flow.NormalizedEmail, &flow.Continuation, &flow.Status,
		&flow.ConfirmationAction, &flow.TerminalOutcome, &flow.HumanID,
		&flow.AgentID, &flow.ExpiresAt, &firebaseUID, &flow.VerifiedProviderSubject)
	return flow, firebaseUID, err
}

func resolveHumanTx(ctx context.Context, tx pgx.Tx, firebaseUID string) (string, string, bool, error) {
	var humanID, agentID string
	err := tx.QueryRow(ctx, `SELECT c.human_id, a.personality_agent_id
		FROM credentials c JOIN agents a ON a.human_id=c.human_id
		WHERE c.provider='firebase' AND c.external_subject=$1 AND c.active`, firebaseUID).Scan(&humanID, &agentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, fmt.Errorf("resolve verified credential: %w", err)
	}
	return humanID, agentID, true, nil
}

func completeExistingFlow(ctx context.Context, tx pgx.Tx, flow AuthFlow, uid, humanID, agentID, outcome string) (AuthFlow, error) {
	_, err := tx.Exec(ctx, `UPDATE auth_flows SET status='completed', confirmation_action=NULL,
		firebase_uid=$2, human_id=$3, personality_agent_id=$4, terminal_outcome=$5,
		proved_at=COALESCE(proved_at, now()), completed_at=now() WHERE flow_id=$1`,
		flow.FlowID, uid, humanID, agentID, outcome)
	if err != nil {
		return AuthFlow{}, fmt.Errorf("complete auth flow: %w", err)
	}
	flow.Status, flow.ConfirmationAction, flow.TerminalOutcome = "completed", "", outcome
	flow.HumanID, flow.AgentID = humanID, agentID
	return flow, nil
}

func (s *Store) provisionFromFlow(ctx context.Context, tx pgx.Tx, flow AuthFlow, identity VerifiedIdentity) (AuthFlow, error) {
	wrappingKeyID, err := validateWrappingKeyID(s.wrappingKeyID)
	if err != nil {
		return AuthFlow{}, fmt.Errorf("configured wrapping key ID: %w", err)
	}
	humanID, agentID := newUUIDv7(), newUUIDv7()
	wrappingKey, err := generateWrappingKey()
	if err != nil {
		return AuthFlow{}, err
	}
	statements := []struct {
		query string
		args  []any
	}{
		{"INSERT INTO humans (human_id) VALUES ($1)", []any{humanID}},
		{"INSERT INTO agents (personality_agent_id, human_id) VALUES ($1, $2)", []any{agentID, humanID}},
		{"INSERT INTO employments (agent_id, employer_type, employer_id) VALUES ($1, $2, $3)", []any{agentID, EmployerHuman, humanID}},
		{"INSERT INTO agent_secrets (personality_agent_id, wrapping_key_id, wrapping_key) VALUES ($1, $2, $3)", []any{agentID, wrappingKeyID, wrappingKey}},
		{"INSERT INTO credentials (provider, external_subject, human_id) VALUES ('firebase', $1, $2)", []any{identity.FirebaseUID, humanID}},
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement.query, statement.args...); err != nil {
			if isUniqueViolation(err) {
				return AuthFlow{}, ErrCredentialAlreadyBound
			}
			return AuthFlow{}, fmt.Errorf("provision confirmed Human and Secretary: %w", err)
		}
	}
	if flow.Channel == ChannelProvider {
		if identity.ProviderSubject == "" {
			return AuthFlow{}, ErrAuthProofMismatch
		}
		if _, err := tx.Exec(ctx, "INSERT INTO credentials (provider, external_subject, human_id) VALUES ($1,$2,$3)",
			flow.ExpectedProvider, identity.ProviderSubject, humanID); err != nil {
			if isUniqueViolation(err) {
				return AuthFlow{}, ErrCredentialAlreadyBound
			}
			return AuthFlow{}, fmt.Errorf("bind initial provider credential: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO credential_security_events
			(human_id, provider, event_type, decision_path, terminal_outcome)
			VALUES ($1,$2,'provider_linked','new_account_activation','linked')`, humanID, flow.ExpectedProvider); err != nil {
			return AuthFlow{}, fmt.Errorf("record initial provider link: %w", err)
		}
	}
	return completeExistingFlow(ctx, tx, flow, identity.FirebaseUID, humanID, agentID, OutcomeAccountCreated)
}

func syncVerifiedProviderTx(ctx context.Context, tx pgx.Tx, humanID, provider, subject, decisionPath string) error {
	if subject == "" || len(subject) > 512 {
		return ErrAuthProofMismatch
	}
	var boundHuman string
	var active bool
	err := tx.QueryRow(ctx, "SELECT human_id, active FROM credentials WHERE provider=$1 AND external_subject=$2 FOR UPDATE", provider, subject).Scan(&boundHuman, &active)
	changed := false
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		_, err = tx.Exec(ctx, "INSERT INTO credentials (provider, external_subject, human_id) VALUES ($1,$2,$3)", provider, subject, humanID)
		changed = true
	case err != nil:
		return err
	case boundHuman != humanID:
		return ErrCredentialAlreadyBound
	case !active:
		_, err = tx.Exec(ctx, "UPDATE credentials SET active=true, unlinked_at=NULL WHERE provider=$1 AND external_subject=$2", provider, subject)
		changed = true
	}
	if err != nil {
		return err
	}
	if changed {
		_, err = tx.Exec(ctx, `INSERT INTO credential_security_events
			(human_id, provider, event_type, decision_path, terminal_outcome)
			VALUES ($1,$2,'provider_linked',$3,'linked')`, humanID, provider, decisionPath)
	}
	return err
}
