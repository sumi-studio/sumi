package koseki

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

var ErrEnrollmentInvite = errors.New("a valid enrollment invitation is required")

type EnrollmentInvite struct {
	ID         string     `json:"id"`
	IssuedBy   string     `json:"issued_by"`
	Email      string     `json:"email,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	ExpiresAt  time.Time  `json:"expires_at"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
	ConsumedAt *time.Time `json:"consumed_at,omitempty"`
	ConsumedBy string     `json:"consumed_by,omitempty"`
}

func enrollmentTokenHash(token string) ([]byte, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) != 32 {
		return nil, ErrEnrollmentInvite
	}
	hash := sha256.Sum256(raw)
	return hash[:], nil
}

// IssueEnrollmentInvite is an operator operation. The HTTP boundary authorizes
// the issuer explicitly; this store never sends email or grants Workspace access.
func (s *Store) IssueEnrollmentInvite(ctx context.Context, issuer, email string, ttl time.Duration) (EnrollmentInvite, string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return EnrollmentInvite{}, "", err
	}
	defer tx.Rollback(ctx)
	invite, token, err := s.IssueEnrollmentInviteInTx(ctx, tx, issuer, email, ttl)
	if err != nil {
		return EnrollmentInvite{}, "", err
	}
	if err = tx.Commit(ctx); err != nil {
		return EnrollmentInvite{}, "", err
	}
	return invite, token, nil
}

// IssueEnrollmentInviteInTx permits atomic issuance together with a Workspace
// invitation. The caller must authorize the issuer and commit before revealing
// the raw token. It does not grant membership or consume either invitation.
func (s *Store) IssueEnrollmentInviteInTx(ctx context.Context, tx pgx.Tx, issuer, email string, ttl time.Duration) (EnrollmentInvite, string, error) {
	var invite EnrollmentInvite
	if ttl < time.Minute || ttl > 30*24*time.Hour {
		return invite, "", ErrEnrollmentInvite
	}
	if email != "" {
		var err error
		email, err = NormalizeEmail(email)
		if err != nil {
			return invite, "", ErrEnrollmentInvite
		}
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return invite, "", err
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	hash, _ := enrollmentTokenHash(token)
	err := tx.QueryRow(ctx, `INSERT INTO enrollment_invites(invite_id,token_hash,issued_by,email,expires_at)
 VALUES($1,$2,$3,NULLIF($4,''),clock_timestamp()+$5::interval)
 RETURNING invite_id,issued_by,COALESCE(email,''),created_at,expires_at`, newUUIDv7(), hash, issuer, email, ttl.String()).Scan(&invite.ID, &invite.IssuedBy, &invite.Email, &invite.CreatedAt, &invite.ExpiresAt)
	return invite, token, err
}

func (s *Store) ListEnrollmentInvites(ctx context.Context) ([]EnrollmentInvite, error) {
	rows, err := s.pool.Query(ctx, `SELECT invite_id,issued_by,COALESCE(email,''),created_at,expires_at,revoked_at,consumed_at,COALESCE(consumed_by::text,'') FROM enrollment_invites ORDER BY created_at DESC,invite_id DESC LIMIT 100`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []EnrollmentInvite{}
	for rows.Next() {
		var v EnrollmentInvite
		if err := rows.Scan(&v.ID, &v.IssuedBy, &v.Email, &v.CreatedAt, &v.ExpiresAt, &v.RevokedAt, &v.ConsumedAt, &v.ConsumedBy); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *Store) RevokeEnrollmentInvite(ctx context.Context, id string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	linked, err := s.lockEnrollmentWorkspace(ctx, tx, id, false)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE enrollment_invites SET revoked_at=COALESCE(revoked_at,clock_timestamp()) WHERE invite_id=$1 AND consumed_at IS NULL`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 && linked == "" {
		return ErrEnrollmentInvite
	}
	if linked != "" {
		if _, err = tx.Exec(ctx, `UPDATE workspace_invites SET revoked_at=COALESCE(revoked_at,clock_timestamp()) WHERE invite_id=$1 AND redeemed_at IS NULL`, linked); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
func (s *Store) InspectEnrollmentInvite(ctx context.Context, token string) (EnrollmentInvite, error) {
	var v EnrollmentInvite
	hash, err := enrollmentTokenHash(token)
	if err != nil {
		return v, err
	}
	err = s.pool.QueryRow(ctx, `SELECT invite_id,expires_at FROM enrollment_invites WHERE token_hash=$1 AND revoked_at IS NULL AND consumed_at IS NULL AND expires_at>now()`, hash).Scan(&v.ID, &v.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrEnrollmentInvite
	}
	return v, err
}
func (s *Store) checkEnrollmentInviteProof(ctx context.Context, tx pgx.Tx, flow AuthFlow, identity VerifiedIdentity) error {
	if flow.EnrollmentInviteID == "" {
		return ErrEnrollmentInvite
	}
	if _, err := s.lockEnrollmentWorkspace(ctx, tx, flow.EnrollmentInviteID, true); err != nil {
		return err
	}
	var email string
	var expires time.Time
	err := tx.QueryRow(ctx, `SELECT COALESCE(email,''),expires_at FROM enrollment_invites WHERE invite_id=$1 AND revoked_at IS NULL AND consumed_at IS NULL FOR UPDATE`, flow.EnrollmentInviteID).Scan(&email, &expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrEnrollmentInvite
	}
	if err != nil {
		return err
	}
	// Evaluate database time after acquiring the row lock: it may have waited
	// past expiry even if the invitation was live when the statement began.
	var live bool
	if err := tx.QueryRow(ctx, "SELECT $1::timestamptz>clock_timestamp()", expires).Scan(&live); err != nil {
		return err
	}
	if !live {
		return ErrEnrollmentInvite
	}
	if email != "" && (!identity.EmailVerified || identity.NormalizedEmail != email) {
		return ErrEnrollmentInvite
	}
	return nil
}
func (s *Store) consumeEnrollmentInvite(ctx context.Context, tx pgx.Tx, flow AuthFlow, identity VerifiedIdentity, humanID string) error {
	if err := s.checkEnrollmentInviteProof(ctx, tx, flow, identity); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE enrollment_invites SET consumed_at=clock_timestamp(),consumed_by=$2 WHERE invite_id=$1 AND expires_at>clock_timestamp()`, flow.EnrollmentInviteID, humanID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrEnrollmentInvite
	}
	// Reservation identifies the one recipient but creates no Workspace member.
	_, err = tx.Exec(ctx, `UPDATE workspace_invites SET reserved_human_id=$2 WHERE invite_id=(SELECT workspace_invite_id FROM enrollment_invites WHERE invite_id=$1)`, flow.EnrollmentInviteID, humanID)
	return err
}
