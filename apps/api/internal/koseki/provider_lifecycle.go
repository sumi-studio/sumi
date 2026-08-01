package koseki

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const ProviderOperationTTL = 10 * time.Minute

type ProviderOperation struct {
	OperationID  string
	HumanID      string
	FirebaseUID  string
	Provider     string
	Operation    string
	Status       string
	DecisionPath string
	ExpiresAt    time.Time
}

type SecurityEvent struct {
	EventID         int64
	OperationID     string
	HumanID         string
	Provider        string
	EventType       string
	DecisionPath    string
	TerminalOutcome string
	OccurredAt      time.Time
}

func (s *Store) BeginProviderOperation(ctx context.Context, humanID, firebaseUID, provider, operation, decisionPath, nonce string) (ProviderOperation, error) {
	if humanID == "" || firebaseUID == "" || len(firebaseUID) > 128 ||
		(provider != "google.com" && provider != "github.com") ||
		(operation != "link" && operation != "unlink") || !validDecisionPath(decisionPath) {
		return ProviderOperation{}, ErrInvalidAuthFlow
	}
	nonceHash, err := validateNonce(nonce)
	if err != nil {
		return ProviderOperation{}, err
	}
	operationID := newUUIDv7()
	expiresAt := time.Now().UTC().Add(ProviderOperationTTL)
	var result ProviderOperation
	err = s.pool.QueryRow(ctx, `INSERT INTO provider_operations
		(operation_id, nonce_hash, human_id, firebase_uid, provider, operation, decision_path, expires_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (nonce_hash) DO UPDATE SET nonce_hash=provider_operations.nonce_hash
		RETURNING operation_id, human_id, firebase_uid, provider, operation, status, decision_path, expires_at`,
		operationID, nonceHash, humanID, firebaseUID, provider, operation, decisionPath, expiresAt).Scan(
		&result.OperationID, &result.HumanID, &result.FirebaseUID, &result.Provider,
		&result.Operation, &result.Status, &result.DecisionPath, &result.ExpiresAt)
	if err != nil {
		return ProviderOperation{}, fmt.Errorf("begin provider operation: %w", err)
	}
	if result.HumanID != humanID || result.FirebaseUID != firebaseUID || result.Provider != provider ||
		result.Operation != operation || result.DecisionPath != decisionPath {
		return ProviderOperation{}, ErrInvalidAuthFlow
	}
	return result, nil
}

func validDecisionPath(path string) bool {
	switch path {
	case "provider_sign_in", "same_email_recovery", "notice_action", "account_settings":
		return true
	default:
		return false
	}
}

func scanProviderOperation(ctx context.Context, tx pgx.Tx, operationID, nonce string) (ProviderOperation, error) {
	nonceHash, err := validateNonce(nonce)
	if err != nil {
		return ProviderOperation{}, err
	}
	var operation ProviderOperation
	err = tx.QueryRow(ctx, `SELECT operation_id, human_id, firebase_uid, provider,
		operation, status, decision_path, expires_at FROM provider_operations
		WHERE operation_id=$1 AND nonce_hash=$2 FOR UPDATE`, operationID, nonceHash).Scan(
		&operation.OperationID, &operation.HumanID, &operation.FirebaseUID,
		&operation.Provider, &operation.Operation, &operation.Status,
		&operation.DecisionPath, &operation.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProviderOperation{}, ErrInvalidAuthFlow
	}
	if err != nil {
		return ProviderOperation{}, fmt.Errorf("read provider operation: %w", err)
	}
	if operation.Status != "pending" {
		return ProviderOperation{}, ErrAuthFlowConsumed
	}
	if !time.Now().UTC().Before(operation.ExpiresAt) {
		return ProviderOperation{}, ErrAuthFlowExpired
	}
	return operation, nil
}

func (s *Store) PendingProviderOperation(ctx context.Context, operationID, nonce string) (ProviderOperation, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ProviderOperation{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	operation, err := scanProviderOperation(ctx, tx, operationID, nonce)
	if err != nil {
		return ProviderOperation{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ProviderOperation{}, err
	}
	return operation, nil
}

func (s *Store) ActiveProviderSubject(ctx context.Context, humanID, provider string) (string, error) {
	var subject string
	err := s.pool.QueryRow(ctx, `SELECT external_subject FROM credentials
		WHERE human_id=$1 AND provider=$2 AND active`, humanID, provider).Scan(&subject)
	return subject, err
}

// CompleteProviderLink records only a provider identity present in a refreshed,
// server-verified token for the already-bound Firebase UID. Provider OAuth
// credentials remain exclusively in the Firebase browser SDK and are never
// accepted or persisted here.
func (s *Store) CompleteProviderLink(ctx context.Context, operationID, nonce, firebaseUID, providerSubject string) (SecurityEvent, error) {
	if providerSubject == "" || len(providerSubject) > 512 {
		return SecurityEvent{}, ErrAuthProofMismatch
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SecurityEvent{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	operation, err := scanProviderOperation(ctx, tx, operationID, nonce)
	if err != nil {
		return SecurityEvent{}, err
	}
	if operation.Operation != "link" || operation.FirebaseUID != firebaseUID {
		return SecurityEvent{}, ErrAuthProofMismatch
	}
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", fmt.Sprintf("%d:%s%s", len(operation.Provider), operation.Provider, providerSubject)); err != nil {
		return SecurityEvent{}, err
	}
	var boundHuman string
	var active bool
	outcome := "linked"
	err = tx.QueryRow(ctx, "SELECT human_id, active FROM credentials WHERE provider=$1 AND external_subject=$2 FOR UPDATE",
		operation.Provider, providerSubject).Scan(&boundHuman, &active)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		_, err = tx.Exec(ctx, "INSERT INTO credentials (provider, external_subject, human_id) VALUES ($1,$2,$3)",
			operation.Provider, providerSubject, operation.HumanID)
	case err != nil:
		return SecurityEvent{}, err
	case boundHuman != operation.HumanID:
		return SecurityEvent{}, ErrCredentialAlreadyBound
	case !active:
		_, err = tx.Exec(ctx, "UPDATE credentials SET active=true, unlinked_at=NULL WHERE provider=$1 AND external_subject=$2",
			operation.Provider, providerSubject)
	default:
		outcome = "already_linked"
	}
	if err != nil {
		return SecurityEvent{}, fmt.Errorf("activate linked credential: %w", err)
	}
	event, err := finishProviderOperation(ctx, tx, operation, "provider_linked", outcome)
	if err != nil {
		return SecurityEvent{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return SecurityEvent{}, err
	}
	return event, nil
}

// CompleteProviderUnlink is called only after the Firebase provider has been
// removed and the resulting refreshed token has been verified by the server.
// The historical binding is disabled, never deleted.
func (s *Store) CompleteProviderUnlink(ctx context.Context, operationID, nonce, firebaseUID, providerSubject string) (SecurityEvent, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SecurityEvent{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	operation, err := scanProviderOperation(ctx, tx, operationID, nonce)
	if err != nil {
		return SecurityEvent{}, err
	}
	if operation.Operation != "unlink" || operation.FirebaseUID != firebaseUID || providerSubject == "" {
		return SecurityEvent{}, ErrAuthProofMismatch
	}
	var activeCount int
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM credentials WHERE human_id=$1 AND active", operation.HumanID).Scan(&activeCount); err != nil {
		return SecurityEvent{}, err
	}
	// The immutable Firebase UID anchor counts as one method; unlink may only
	// disable a provider method while at least one other active method remains.
	if activeCount <= 1 {
		return SecurityEvent{}, ErrLastLoginMethod
	}
	command, err := tx.Exec(ctx, `UPDATE credentials SET active=false, unlinked_at=now()
		WHERE provider=$1 AND external_subject=$2 AND human_id=$3 AND active`,
		operation.Provider, providerSubject, operation.HumanID)
	if err != nil {
		return SecurityEvent{}, fmt.Errorf("disable provider credential: %w", err)
	}
	if command.RowsAffected() != 1 {
		return SecurityEvent{}, ErrAuthProofMismatch
	}
	event, err := finishProviderOperation(ctx, tx, operation, "provider_unlinked", "unlinked")
	if err != nil {
		return SecurityEvent{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return SecurityEvent{}, err
	}
	return event, nil
}

func (s *Store) FailProviderOperation(ctx context.Context, operationID, nonce, terminalOutcome string) (SecurityEvent, error) {
	if terminalOutcome == "" || len(terminalOutcome) > 128 {
		return SecurityEvent{}, ErrInvalidAuthFlow
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SecurityEvent{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	operation, err := scanProviderOperation(ctx, tx, operationID, nonce)
	if err != nil {
		return SecurityEvent{}, err
	}
	eventType := "provider_" + operation.Operation + "_failed"
	event, err := finishProviderOperation(ctx, tx, operation, eventType, terminalOutcome)
	if err != nil {
		return SecurityEvent{}, err
	}
	if _, err := tx.Exec(ctx, "UPDATE provider_operations SET status='failed' WHERE operation_id=$1", operation.OperationID); err != nil {
		return SecurityEvent{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return SecurityEvent{}, err
	}
	return event, nil
}

func finishProviderOperation(ctx context.Context, tx pgx.Tx, operation ProviderOperation, eventType, outcome string) (SecurityEvent, error) {
	var event SecurityEvent
	err := tx.QueryRow(ctx, `INSERT INTO credential_security_events
		(operation_id, human_id, provider, event_type, decision_path, terminal_outcome)
		VALUES ($1,$2,$3,$4,$5,$6)
		RETURNING event_id, operation_id, human_id, provider, event_type, decision_path, terminal_outcome, occurred_at`,
		operation.OperationID, operation.HumanID, operation.Provider, eventType,
		operation.DecisionPath, outcome).Scan(&event.EventID, &event.OperationID,
		&event.HumanID, &event.Provider, &event.EventType, &event.DecisionPath,
		&event.TerminalOutcome, &event.OccurredAt)
	if err != nil {
		return SecurityEvent{}, fmt.Errorf("record credential security event: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE provider_operations SET status='completed', terminal_outcome=$2,
		completed_at=now() WHERE operation_id=$1`, operation.OperationID, outcome); err != nil {
		return SecurityEvent{}, err
	}
	return event, nil
}
