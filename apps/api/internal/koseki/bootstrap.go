package koseki

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// Enrollment bootstrap lets an operator admit the first person into a
// completely empty install. Enrollment stays invitation-only: the bootstrap
// only mints an ordinary, email-bound enrollment invitation, and it exists
// only while no Human exists. The invitation has no issuing Human
// (issued_by IS NULL) instead of a fabricated operator account; every
// product-issued invitation still names its issuer.

var (
	// ErrBootstrapClosed: a Human already exists, so invitations come from an
	// enrollment admin through the product, never from the bootstrap.
	ErrBootstrapClosed = errors.New("enrollment bootstrap is closed: a Human already exists")
	// ErrBootstrapInvite: the request itself is unacceptable (missing or
	// malformed email, lifetime out of range).
	ErrBootstrapInvite = errors.New("bootstrap invitation requires a valid email and a lifetime between 10 minutes and 72 hours")
)

const (
	MinBootstrapInviteTTL     = 10 * time.Minute
	MaxBootstrapInviteTTL     = 72 * time.Hour
	DefaultBootstrapInviteTTL = 24 * time.Hour
)

// BootstrapInvite is an issued bootstrap invitation. Superseded counts the
// earlier unused bootstrap invitations this issuance revoked, so a lost or
// leaked earlier token stops working the moment a new one exists.
type BootstrapInvite struct {
	ID         string
	Email      string
	CreatedAt  time.Time
	ExpiresAt  time.Time
	Superseded int64
}

// IssueBootstrapEnrollmentInvite mints the email-bound first invitation of
// an empty install. The caller must reveal the raw token only after this
// returns (the issuance is committed).
//
// The humans table is locked in SHARE ROW EXCLUSIVE mode for the whole
// transaction. The mode conflicts with itself, which serializes concurrent
// bootstrap runs (the later one supersedes the earlier token), and with the
// ROW EXCLUSIVE lock every Human insert takes, so a registration that is
// consuming a bootstrap invitation right now either commits first — and
// closes the bootstrap — or waits until this issuance commits. Registration
// inserts the Human before it locks the invitation row, and this
// transaction takes the table lock before it touches invitations, so the
// two never wait on each other in opposite orders.
func (s *Store) IssueBootstrapEnrollmentInvite(ctx context.Context, email string, ttl time.Duration) (BootstrapInvite, string, error) {
	var invite BootstrapInvite
	if ttl < MinBootstrapInviteTTL || ttl > MaxBootstrapInviteTTL || email == "" {
		return invite, "", ErrBootstrapInvite
	}
	normalized, err := NormalizeEmail(email)
	if err != nil {
		return invite, "", ErrBootstrapInvite
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return invite, "", err
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	hash, err := enrollmentTokenHash(token)
	if err != nil {
		return invite, "", err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return invite, "", err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `LOCK TABLE humans IN SHARE ROW EXCLUSIVE MODE`); err != nil {
		return invite, "", err
	}
	var populated bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM humans)`).Scan(&populated); err != nil {
		return invite, "", err
	}
	if populated {
		return invite, "", ErrBootstrapClosed
	}
	tag, err := tx.Exec(ctx, `UPDATE enrollment_invites SET revoked_at = clock_timestamp()
		WHERE issued_by IS NULL AND consumed_at IS NULL AND revoked_at IS NULL`)
	if err != nil {
		return invite, "", err
	}
	invite.Superseded = tag.RowsAffected()
	if err := tx.QueryRow(ctx, `INSERT INTO enrollment_invites (invite_id, token_hash, issued_by, email, expires_at)
		VALUES ($1, $2, NULL, $3, clock_timestamp() + $4::interval)
		RETURNING invite_id, email, created_at, expires_at`,
		newUUIDv7(), hash, normalized, ttl.String()).
		Scan(&invite.ID, &invite.Email, &invite.CreatedAt, &invite.ExpiresAt); err != nil {
		return BootstrapInvite{}, "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return BootstrapInvite{}, "", err
	}
	return invite, token, nil
}

// BootstrapStatus is the operator's view of the bootstrap. It never
// includes token material.
type BootstrapStatus struct {
	// Open reports whether a bootstrap invitation may be issued now
	// (no Human exists).
	Open bool `json:"open"`
	// Outstanding is the unused, unrevoked bootstrap invitation, if any.
	// It may already be past its expiry.
	Outstanding *BootstrapInviteState `json:"outstanding,omitempty"`
	// Consumed is the most recent bootstrap invitation that admitted a
	// Human. Its ConsumedBy is the Human to name as the first enrollment
	// admin.
	Consumed *BootstrapInviteState `json:"consumed,omitempty"`
}

type BootstrapInviteState struct {
	ID         string     `json:"id"`
	Email      string     `json:"email"`
	CreatedAt  time.Time  `json:"created_at"`
	ExpiresAt  time.Time  `json:"expires_at"`
	Expired    bool       `json:"expired"`
	ConsumedAt *time.Time `json:"consumed_at,omitempty"`
	ConsumedBy string     `json:"consumed_by,omitempty"`
}

func (s *Store) EnrollmentBootstrapStatus(ctx context.Context) (BootstrapStatus, error) {
	var status BootstrapStatus
	var populated bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM humans)`).Scan(&populated); err != nil {
		return status, err
	}
	status.Open = !populated
	outstanding, err := s.bootstrapInviteState(ctx, `consumed_at IS NULL AND revoked_at IS NULL`)
	if err != nil {
		return status, err
	}
	consumed, err := s.bootstrapInviteState(ctx, `consumed_at IS NOT NULL`)
	if err != nil {
		return status, err
	}
	status.Outstanding, status.Consumed = outstanding, consumed
	return status, nil
}

func (s *Store) bootstrapInviteState(ctx context.Context, condition string) (*BootstrapInviteState, error) {
	var v BootstrapInviteState
	err := s.pool.QueryRow(ctx, `SELECT invite_id, COALESCE(email, ''), created_at, expires_at,
		expires_at <= clock_timestamp(), consumed_at, COALESCE(consumed_by::text, '')
		FROM enrollment_invites WHERE issued_by IS NULL AND `+condition+`
		ORDER BY created_at DESC, invite_id DESC LIMIT 1`).
		Scan(&v.ID, &v.Email, &v.CreatedAt, &v.ExpiresAt, &v.Expired, &v.ConsumedAt, &v.ConsumedBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &v, nil
}
