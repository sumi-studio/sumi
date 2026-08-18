package messaging

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// MaxAttachmentBytes bounds one uploaded file (20 MiB), matching the CHECK on
// message_attachments.size_bytes and message_attachment_uploads.declared_bytes.
const MaxAttachmentBytes int64 = 20 << 20

// MaxAttachmentsPerMessage bounds how many attachments one send may carry.
const MaxAttachmentsPerMessage = 10

// MaxUnboundDraftBytes bounds the outstanding (reserved + finalized unbound)
// bytes one uploader may hold in one place: exactly one full message.
const MaxUnboundDraftBytes = int64(MaxAttachmentsPerMessage) * MaxAttachmentBytes

// MaxAttachmentFilenameBytes matches the schema CHECK on filename.
const MaxAttachmentFilenameBytes = 255

// DefaultAttachmentReservationTTL is how long an upload may stay reserved
// without finalizing. The upload route allows 130 seconds for a 20 MiB body;
// the reservation outlives it so a slow finalization never races expiry.
const DefaultAttachmentReservationTTL = 10 * time.Minute

// DefaultUnboundAttachmentTTL is how long a finalized upload that never became
// part of a message is kept before its bytes are reclaimed.
const DefaultUnboundAttachmentTTL = 24 * time.Hour

// attachmentStagingLeaseTTL is longer than the bounded HTTP upload window but
// finite. It is a durable single-stager lease, not an authority lease.
const attachmentStagingLeaseTTL = 3 * time.Minute

// Attachment blob states. 'deleting' is the durable deletion outbox.
const (
	AttachmentBlobStored   = "stored"
	AttachmentBlobDeleting = "deleting"
	AttachmentBlobDeleted  = "deleted"
)

// Attachment sentinels. ErrAttachmentNotFound doubles as the authorization
// failure: absent, foreign, hidden, tombstoned, and stale-scope targets are all
// reported identically so existence never leaks.
var (
	ErrAttachmentNotFound       = errors.New("attachment not found")
	ErrAttachmentTooLarge       = errors.New("attachment exceeds the size limit")
	ErrAttachmentEmpty          = errors.New("attachment has no bytes")
	ErrAttachmentNonce          = errors.New("attachment client nonce must be 1..128 bytes")
	ErrTooManyAttachments       = errors.New("too many attachments for one message")
	ErrAttachmentQuotaExceeded  = errors.New("workspace attachment quota exceeded")
	ErrAttachmentDraftLimit     = errors.New("too many outstanding unsent attachments in this place")
	ErrAttachmentUploadConflict = errors.New("attachment upload nonce was already used with different content")
	ErrAttachmentUploadExpired  = errors.New("attachment upload reservation expired")
	// Retired is terminal for a nonce whose historical attachment was deleted.
	// The logical upload identity remains durable; it never masquerades as a
	// ready receipt and never attempts to reuse a historical attachment id.
	ErrAttachmentUploadRetired    = errors.New("attachment upload receipt was retired")
	ErrAttachmentUploadInProgress = errors.New("attachment upload is already staging")
	ErrAttachmentSizeMismatch     = errors.New("attachment body size differs from the declared size")
	ErrAttachmentsUnavailable     = errors.New("attachments are not configured")
)

// AttachmentPolicy is the operator-owned attachment configuration. Every
// byte and object cap is mandatory: a partial policy fails closed rather than
// silently leaving either a Workspace or the whole API-owned blob root
// unlimited.
type AttachmentPolicy struct {
	WorkspaceQuotaBytes   int64
	WorkspaceQuotaObjects int64
	TotalQuotaBytes       int64
	TotalQuotaObjects     int64
	ReservationTTL        time.Duration
	UnboundTTL            time.Duration
}

func (p AttachmentPolicy) normalized() (AttachmentPolicy, error) {
	if p.WorkspaceQuotaBytes <= 0 {
		return p, errors.New("attachment workspace quota must be a positive byte count")
	}
	if p.WorkspaceQuotaBytes < MaxAttachmentBytes {
		return p, fmt.Errorf("attachment workspace quota must be at least %d bytes", MaxAttachmentBytes)
	}
	if p.WorkspaceQuotaObjects <= 0 {
		return p, errors.New("attachment workspace object quota must be positive")
	}
	if p.TotalQuotaBytes <= 0 {
		return p, errors.New("attachment total quota must be a positive byte count")
	}
	if p.TotalQuotaObjects <= 0 {
		return p, errors.New("attachment total object quota must be positive")
	}
	if p.TotalQuotaBytes < p.WorkspaceQuotaBytes {
		return p, errors.New("attachment total byte quota must cover one workspace quota")
	}
	if p.TotalQuotaObjects < p.WorkspaceQuotaObjects {
		return p, errors.New("attachment total object quota must cover one workspace quota")
	}
	if p.ReservationTTL <= 0 {
		p.ReservationTTL = DefaultAttachmentReservationTTL
	}
	if p.UnboundTTL <= 0 {
		p.UnboundTTL = DefaultUnboundAttachmentTTL
	}
	return p, nil
}

// ConfigureAttachments enables attachment storage on the store. Without it
// every attachment operation reports ErrAttachmentsUnavailable.
func (s *Store) ConfigureAttachments(blobs AttachmentBlobs, policy AttachmentPolicy) error {
	if blobs == nil {
		return errors.New("attachment blob storage is required")
	}
	normalized, err := policy.normalized()
	if err != nil {
		return err
	}
	s.blobs = blobs
	s.attachmentPolicy = normalized
	return nil
}

// AttachmentsEnabled reports whether the store can accept and serve bytes.
func (s *Store) AttachmentsEnabled() bool {
	return s != nil && s.blobs != nil
}

// AttachmentBlobStore exposes the configured byte store for lifecycle tooling.
func (s *Store) AttachmentBlobStore() AttachmentBlobs { return s.blobs }

// AttachmentPolicyInEffect reports the normalized configuration.
func (s *Store) AttachmentPolicyInEffect() AttachmentPolicy { return s.attachmentPolicy }

// Attachment is one durable uploaded file. It is minted only after its bytes
// are durable and becomes part of a message when its uploader sends one
// carrying it.
type Attachment struct {
	AttachmentID string
	WorkspaceID  string
	PlaceID      string
	MessageID    string // empty while unbound
	Uploader     ParticipantRef
	ClientNonce  string
	Filename     string
	MIME         string
	SizeBytes    int64
	SHA256       []byte
	Position     int
	BlobState    string
	CreatedAt    time.Time
	BoundAt      *time.Time
}

// SHA256Hex renders the digest for wire projection.
func (a Attachment) SHA256Hex() string { return hex.EncodeToString(a.SHA256) }

// AttachmentUploadReservation is the durable quota reservation for one
// in-flight upload. UploadID is the attachment identity the upload will take.
type AttachmentUploadReservation struct {
	UploadID      string
	PlaceID       string
	ClientNonce   string
	DeclaredBytes int64
	ExpiresAt     time.Time
	StageToken    string
}

// AttachmentUploadReceipt is the outcome of an upload preflight: either the
// nonce already finalized (Existing) or the caller may stage bytes for the
// returned reservation.
type AttachmentUploadReceipt struct {
	Existing    *Attachment
	Reservation *AttachmentUploadReservation
}

// StagedAttachment is one uploaded body that is durable in a private staging
// file but not yet published under its attachment identity.
type StagedAttachment struct {
	UploadID   string
	Filename   string
	MIME       string
	Size       int64
	SHA256     []byte
	StageToken string
	// Handle is opaque to the store; the blob backend interprets it.
	Handle StagedBlob
}

func validateAttachmentNonce(clientNonce string) error {
	if !clientNonceValid(clientNonce) {
		return ErrAttachmentNonce
	}
	return nil
}

func validAttachmentID(id string) bool {
	parsed, err := uuid.Parse(id)
	if err != nil {
		return false
	}
	// The uuidv7 domain accepts canonical lowercase hyphenated v7 only.
	return parsed.Version() == 7 && parsed.String() == id
}

func (s *ScopedStore) requireAttachments() error {
	if s == nil || s.Store == nil || !s.Store.AttachmentsEnabled() {
		return ErrAttachmentsUnavailable
	}
	return nil
}

// ReserveAttachmentUpload runs the exact-scope preflight for one upload: it
// authorizes the actor's current Workspace/installation/place authority,
// resolves the per-file nonce receipt, and reserves bytes plus one object
// against the whole-store and Workspace ledgers and the uploader's per-place
// draft budget. No body byte may be read before this returns a reservation.
//
// Every multi-row attachment mutation follows this single lock order:
// whole-store usage -> Workspace usage -> uploader/place reservation or
// attachment row. Reconciliation uses the same order; never acquire a usage
// lock after an upload/attachment row lock.
func (s *ScopedStore) ReserveAttachmentUpload(ctx context.Context, placeID, clientNonce string, declaredBytes int64) (AttachmentUploadReceipt, error) {
	if err := s.requireAttachments(); err != nil {
		return AttachmentUploadReceipt{}, err
	}
	if err := validateAttachmentNonce(clientNonce); err != nil {
		return AttachmentUploadReceipt{}, err
	}
	if declaredBytes <= 0 {
		return AttachmentUploadReceipt{}, ErrAttachmentEmpty
	}
	if declaredBytes > MaxAttachmentBytes {
		return AttachmentUploadReceipt{}, ErrAttachmentTooLarge
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AttachmentUploadReceipt{}, fmt.Errorf("begin attachment reservation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeMutationInTx(ctx, tx); err != nil {
		return AttachmentUploadReceipt{}, err
	}
	place, err := s.loadScopedPlace(ctx, tx, placeID)
	if err != nil {
		return AttachmentUploadReceipt{}, err
	}
	if _, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor); err != nil {
		return AttachmentUploadReceipt{}, err
	}
	// Finalized receipt first: it outlives the reservation row.
	if existing, found, err := s.attachmentByNonceInTx(ctx, tx, placeID, clientNonce); err != nil {
		return AttachmentUploadReceipt{}, err
	} else if found {
		if existing.SizeBytes != declaredBytes {
			return AttachmentUploadReceipt{}, ErrAttachmentUploadConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return AttachmentUploadReceipt{}, fmt.Errorf("commit attachment receipt read: %w", err)
		}
		return AttachmentUploadReceipt{Existing: &existing}, nil
	}
	// The two usage rows serialize reservations before the uploader/place row is
	// locked, so byte and object limits are decided against one durable total.
	usage, err := lockAttachmentUsage(ctx, tx, s.Scope.WorkspaceID)
	if err != nil {
		return AttachmentUploadReceipt{}, err
	}
	now := time.Now()
	var (
		uploadID       string
		state          string
		declared       int64
		expiresAt      time.Time
		stageToken     string
		stageExpiresAt *time.Time
	)
	err = tx.QueryRow(ctx, `
		SELECT upload_id, state, declared_bytes, expires_at,
		       COALESCE(staging_token::text, ''), staging_expires_at
		FROM message_attachment_uploads
		WHERE workspace_id = $1 AND place_id = $2
		  AND uploader_kind = $3 AND uploader_id = $4 AND client_nonce = $5
		FOR UPDATE`,
		s.Scope.WorkspaceID, placeID, s.Scope.Actor.Kind, s.Scope.Actor.ID, clientNonce,
	).Scan(&uploadID, &state, &declared, &expiresAt, &stageToken, &stageExpiresAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		uploadID = ""
	case err != nil:
		return AttachmentUploadReceipt{}, fmt.Errorf("load attachment reservation: %w", err)
	}
	policy := s.Store.attachmentPolicy
	if uploadID != "" {
		switch state {
		case "finalized":
			// A concurrent attempt finalized between the receipt lookup and the
			// row lock; a fresh statement now sees its committed attachment.
			existing, found, err := s.attachmentByNonceInTx(ctx, tx, placeID, clientNonce)
			if err != nil {
				return AttachmentUploadReceipt{}, err
			}
			if !found {
				// The nonce's historical attachment was tombstoned/deleted. Never
				// return a ready receipt for bytes no longer present; callers must
				// begin a new upload identity rather than bind a dead receipt.
				return AttachmentUploadReceipt{}, ErrAttachmentUploadRetired
			}
			if existing.SizeBytes != declaredBytes {
				return AttachmentUploadReceipt{}, ErrAttachmentUploadConflict
			}
			if err := tx.Commit(ctx); err != nil {
				return AttachmentUploadReceipt{}, fmt.Errorf("commit attachment receipt read: %w", err)
			}
			return AttachmentUploadReceipt{Existing: &existing}, nil
		case "reserved":
			if declared != declaredBytes {
				return AttachmentUploadReceipt{}, ErrAttachmentUploadConflict
			}
			if stageExpiresAt != nil && stageExpiresAt.After(now) {
				return AttachmentUploadReceipt{}, ErrAttachmentUploadInProgress
			}
			if exists, err := s.Store.blobs.StagingExists(uploadID); err != nil {
				return AttachmentUploadReceipt{}, err
			} else if exists {
				return AttachmentUploadReceipt{}, ErrAttachmentUploadInProgress
			}
			// A live reservation is resumed under the current exact authority;
			// the ledger already holds its bytes. One fresh staging lease is the
			// only authority to create the deterministic staging artifact.
			expiresAt = now.Add(policy.ReservationTTL)
			stageToken = newUUIDv7()
			stageDeadline := now.Add(attachmentStagingLeaseTTL)
			if _, err := tx.Exec(ctx, `
				UPDATE message_attachment_uploads
				SET installation_id = $2, authority_epoch = $3, expires_at = $4,
				    staging_token = $5, staging_expires_at = $6
				WHERE upload_id = $1`,
				uploadID, s.Scope.InstallationID, s.Scope.AuthorityEpoch, expiresAt,
				stageToken, stageDeadline); err != nil {
				return AttachmentUploadReceipt{}, fmt.Errorf("refresh attachment reservation: %w", err)
			}
			if err := tx.Commit(ctx); err != nil {
				return AttachmentUploadReceipt{}, fmt.Errorf("commit attachment reservation refresh: %w", err)
			}
			return AttachmentUploadReceipt{Reservation: &AttachmentUploadReservation{
				UploadID: uploadID, PlaceID: placeID, ClientNonce: clientNonce,
				DeclaredBytes: declaredBytes, ExpiresAt: expiresAt, StageToken: stageToken,
			}}, nil
		case "released":
			if declared != declaredBytes {
				return AttachmentUploadReceipt{}, ErrAttachmentUploadConflict
			}
			// Re-reserve below under the same upload identity.
		default:
			return AttachmentUploadReceipt{}, fmt.Errorf("unknown attachment reservation state %q", state)
		}
	}
	// Budgets are checked before any ledger mutation.
	if usage.WorkspaceBytes+declaredBytes > policy.WorkspaceQuotaBytes ||
		usage.WorkspaceObjects+1 > policy.WorkspaceQuotaObjects ||
		usage.TotalBytes+declaredBytes > policy.TotalQuotaBytes ||
		usage.TotalObjects+1 > policy.TotalQuotaObjects {
		return AttachmentUploadReceipt{}, ErrAttachmentQuotaExceeded
	}
	outstandingCount, outstandingBytes, err := s.outstandingDraftsInTx(ctx, tx, placeID, now)
	if err != nil {
		return AttachmentUploadReceipt{}, err
	}
	if outstandingCount+1 > MaxAttachmentsPerMessage || outstandingBytes+declaredBytes > MaxUnboundDraftBytes {
		return AttachmentUploadReceipt{}, ErrAttachmentDraftLimit
	}
	expiresAt = now.Add(policy.ReservationTTL)
	stageToken = newUUIDv7()
	stageDeadline := now.Add(attachmentStagingLeaseTTL)
	if uploadID == "" {
		uploadID = newUUIDv7()
		if _, err := tx.Exec(ctx, `
			INSERT INTO message_attachment_uploads
				(upload_id, workspace_id, place_id, uploader_kind, uploader_id, client_nonce,
				 installation_id, authority_epoch, declared_bytes, state, expires_at, staging_token, staging_expires_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'reserved', $10, $11, $12)`,
			uploadID, s.Scope.WorkspaceID, placeID, s.Scope.Actor.Kind, s.Scope.Actor.ID,
			clientNonce, s.Scope.InstallationID, s.Scope.AuthorityEpoch, declaredBytes, expiresAt,
			stageToken, stageDeadline,
		); err != nil {
			return AttachmentUploadReceipt{}, fmt.Errorf("insert attachment reservation: %w", err)
		}
	} else if _, err := tx.Exec(ctx, `
		UPDATE message_attachment_uploads
		SET state = 'reserved', settled_at = NULL, installation_id = $2,
		    authority_epoch = $3, expires_at = $4, staging_token = $5, staging_expires_at = $6
		WHERE upload_id = $1`,
		uploadID, s.Scope.InstallationID, s.Scope.AuthorityEpoch, expiresAt, stageToken, stageDeadline); err != nil {
		return AttachmentUploadReceipt{}, fmt.Errorf("re-reserve attachment upload: %w", err)
	}
	if err := adjustAttachmentUsage(ctx, tx, s.Scope.WorkspaceID, declaredBytes, 1); err != nil {
		return AttachmentUploadReceipt{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return AttachmentUploadReceipt{}, fmt.Errorf("commit attachment reservation: %w", err)
	}
	return AttachmentUploadReceipt{Reservation: &AttachmentUploadReservation{
		UploadID: uploadID, PlaceID: placeID, ClientNonce: clientNonce,
		DeclaredBytes: declaredBytes, ExpiresAt: expiresAt, StageToken: stageToken,
	}}, nil
}

// AbandonAttachmentStaging clears only this caller's durable staging claim.
// It never releases quota: the reservation remains accountable until
// finalization or the reconciler confirms both deterministic staging and any
// published artifact are gone.
func (s *ScopedStore) AbandonAttachmentStaging(ctx context.Context, reservation AttachmentUploadReservation) error {
	if s == nil || s.Store == nil || reservation.StageToken == "" {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin attachment staging abandonment: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := lockAttachmentUsage(ctx, tx, s.Scope.WorkspaceID); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE message_attachment_uploads
		SET staging_token = NULL, staging_expires_at = NULL
		WHERE upload_id = $1 AND workspace_id = $2 AND place_id = $3
		  AND state = 'reserved' AND staging_token = $4`,
		reservation.UploadID, s.Scope.WorkspaceID, reservation.PlaceID, reservation.StageToken)
	if err != nil {
		return fmt.Errorf("clear attachment staging claim: %w", err)
	}
	if tag.RowsAffected() > 0 {
		if err := s.Store.blobs.DiscardStaging(reservation.UploadID); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit attachment staging abandonment: %w", err)
	}
	return nil
}

type attachmentUsage struct {
	TotalBytes, TotalObjects         int64
	WorkspaceBytes, WorkspaceObjects int64
}

// lockAttachmentUsage creates and locks the global store row before the
// Workspace row. Callers must take these locks before any uploader/place
// reservation or attachment row they will mutate.
func lockAttachmentUsage(ctx context.Context, tx pgx.Tx, workspaceID string) (attachmentUsage, error) {
	if _, err := tx.Exec(ctx, `
		INSERT INTO message_attachment_store_usage (singleton) VALUES (true)
		ON CONFLICT (singleton) DO NOTHING`); err != nil {
		return attachmentUsage{}, fmt.Errorf("ensure attachment store usage row: %w", err)
	}
	var usage attachmentUsage
	if err := tx.QueryRow(ctx, `
		SELECT used_bytes, object_count FROM message_attachment_store_usage
		WHERE singleton = true FOR UPDATE`).Scan(&usage.TotalBytes, &usage.TotalObjects); err != nil {
		return attachmentUsage{}, fmt.Errorf("lock attachment store usage: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO message_attachment_quotas (workspace_id) VALUES ($1)
		ON CONFLICT (workspace_id) DO NOTHING`, workspaceID); err != nil {
		return attachmentUsage{}, fmt.Errorf("ensure attachment quota row: %w", err)
	}
	if err := tx.QueryRow(ctx, `
		SELECT used_bytes, object_count FROM message_attachment_quotas WHERE workspace_id = $1 FOR UPDATE`,
		workspaceID).Scan(&usage.WorkspaceBytes, &usage.WorkspaceObjects); err != nil {
		return attachmentUsage{}, fmt.Errorf("lock attachment quota: %w", err)
	}
	return usage, nil
}

func adjustAttachmentUsage(ctx context.Context, tx pgx.Tx, workspaceID string, byteDelta, objectDelta int64) error {
	tag, err := tx.Exec(ctx, `
		UPDATE message_attachment_store_usage
		SET used_bytes = used_bytes + $1, object_count = object_count + $2, updated_at = now()
		WHERE singleton = true`, byteDelta, objectDelta)
	if err != nil {
		return fmt.Errorf("adjust attachment store usage by %d bytes/%d objects: %w", byteDelta, objectDelta, err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("attachment store usage row is missing")
	}
	tag, err = tx.Exec(ctx, `
		UPDATE message_attachment_quotas
		SET used_bytes = used_bytes + $2, object_count = object_count + $3, updated_at = now()
		WHERE workspace_id = $1`, workspaceID, byteDelta, objectDelta)
	if err != nil {
		return fmt.Errorf("adjust attachment quota by %d bytes/%d objects: %w", byteDelta, objectDelta, err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("attachment quota row is missing")
	}
	return nil
}

// outstandingDraftsInTx counts the uploader's live reservations plus finalized
// unbound attachments in one place: the budget one full message may hold.
func (s *ScopedStore) outstandingDraftsInTx(ctx context.Context, tx pgx.Tx, placeID string, now time.Time) (int64, int64, error) {
	var count, bytes int64
	if err := tx.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM message_attachment_uploads u
			 WHERE u.workspace_id = $1 AND u.place_id = $2
			   AND u.uploader_kind = $3 AND u.uploader_id = $4
			   AND u.state = 'reserved' AND u.expires_at > $5)
			+
			(SELECT count(*) FROM message_attachments a
			 WHERE a.workspace_id = $1 AND a.place_id = $2
			   AND a.uploader_kind = $3 AND a.uploader_id = $4
			   AND a.message_id IS NULL AND a.blob_state = 'stored'),
			COALESCE((SELECT sum(u.declared_bytes) FROM message_attachment_uploads u
			 WHERE u.workspace_id = $1 AND u.place_id = $2
			   AND u.uploader_kind = $3 AND u.uploader_id = $4
			   AND u.state = 'reserved' AND u.expires_at > $5), 0)
			+
			COALESCE((SELECT sum(a.size_bytes) FROM message_attachments a
			 WHERE a.workspace_id = $1 AND a.place_id = $2
			   AND a.uploader_kind = $3 AND a.uploader_id = $4
			   AND a.message_id IS NULL AND a.blob_state = 'stored'), 0)`,
		s.Scope.WorkspaceID, placeID, s.Scope.Actor.Kind, s.Scope.Actor.ID, now,
	).Scan(&count, &bytes); err != nil {
		return 0, 0, fmt.Errorf("count outstanding attachment drafts: %w", err)
	}
	return count, bytes, nil
}

const attachmentColumns = `attachment_id, workspace_id, place_id, message_id, uploader_kind, uploader_id,
	client_nonce, filename, mime, size_bytes, sha256, position, blob_state, created_at, bound_at`

func scanAttachment(row pgx.Row) (Attachment, error) {
	var (
		att       Attachment
		messageID *string
		kind      string
	)
	if err := row.Scan(&att.AttachmentID, &att.WorkspaceID, &att.PlaceID, &messageID, &kind, &att.Uploader.ID,
		&att.ClientNonce, &att.Filename, &att.MIME, &att.SizeBytes, &att.SHA256, &att.Position,
		&att.BlobState, &att.CreatedAt, &att.BoundAt); err != nil {
		return Attachment{}, err
	}
	att.Uploader.Kind = ParticipantKind(kind)
	if messageID != nil {
		att.MessageID = *messageID
	}
	return att, nil
}

func (s *ScopedStore) attachmentByNonceInTx(ctx context.Context, q querier, placeID, clientNonce string) (Attachment, bool, error) {
	att, err := scanAttachment(q.QueryRow(ctx, `
		SELECT `+attachmentColumns+` FROM message_attachments
		WHERE workspace_id = $1 AND place_id = $2
		  AND uploader_kind = $3 AND uploader_id = $4 AND client_nonce = $5
		  AND blob_state = 'stored'`,
		s.Scope.WorkspaceID, placeID, s.Scope.Actor.Kind, s.Scope.Actor.ID, clientNonce))
	if errors.Is(err, pgx.ErrNoRows) {
		return Attachment{}, false, nil
	}
	if err != nil {
		return Attachment{}, false, fmt.Errorf("load attachment by nonce: %w", err)
	}
	return att, true, nil
}

// AttachmentUploadReceiptByNonce re-reads the durable receipt for a nonce
// after an ambiguous finalization outcome.
func (s *ScopedStore) AttachmentUploadReceiptByNonce(ctx context.Context, placeID, clientNonce string) (Attachment, bool, error) {
	if err := s.requireAttachments(); err != nil {
		return Attachment{}, false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Attachment{}, false, fmt.Errorf("begin attachment receipt read: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeInTx(ctx, tx); err != nil {
		return Attachment{}, false, err
	}
	att, found, err := s.attachmentByNonceInTx(ctx, tx, placeID, clientNonce)
	if err != nil {
		return Attachment{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Attachment{}, false, fmt.Errorf("commit attachment receipt read: %w", err)
	}
	return att, found, nil
}

// attachmentFinalizeNotCommittedError marks failures that provably happened
// before the finalize transaction could commit. Anything else is ambiguous.
type attachmentFinalizeNotCommittedError struct{ err error }

func (e *attachmentFinalizeNotCommittedError) Error() string { return e.err.Error() }
func (e *attachmentFinalizeNotCommittedError) Unwrap() error { return e.err }

// AttachmentFinalizeDefinitelyNotCommitted reports whether a FinalizeAttachmentUpload
// error proves that no metadata was committed.
func AttachmentFinalizeDefinitelyNotCommitted(err error) bool {
	var marked *attachmentFinalizeNotCommittedError
	if errors.As(err, &marked) {
		return true
	}
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError)
}

func finalizeNotCommitted(err error) error {
	return &attachmentFinalizeNotCommittedError{err: err}
}

// FinalizeAttachmentUpload reacquires exact authority and publishes one staged
// body under its reserved identity. The reservation row lock serializes every
// attempt for the same nonce; the blob rename and the metadata insert happen
// under that lock so an orphaned artifact from an earlier ambiguous attempt is
// provably ownerless before it is replaced. On a definite failure the staged
// bytes are discarded here; on an ambiguous commit outcome the published blob
// is retained for the reconciler and the caller may re-read the receipt.
func (s *ScopedStore) FinalizeAttachmentUpload(ctx context.Context, placeID string, staged StagedAttachment) (attachment Attachment, created bool, retErr error) {
	if err := s.requireAttachments(); err != nil {
		return Attachment{}, false, err
	}
	blobs := s.Store.blobs
	discard := func(err error) (Attachment, bool, error) {
		_ = blobs.Discard(staged.Handle)
		return Attachment{}, false, err
	}
	if !validAttachmentID(staged.UploadID) {
		return discard(finalizeNotCommitted(errors.New("attachment upload id must be a canonical UUIDv7")))
	}
	if !validAttachmentID(staged.StageToken) {
		return discard(finalizeNotCommitted(ErrAttachmentUploadExpired))
	}
	filename := strings.TrimSpace(staged.Filename)
	if filename == "" || len(filename) > MaxAttachmentFilenameBytes {
		return discard(finalizeNotCommitted(fmt.Errorf("filename must be 1..%d bytes", MaxAttachmentFilenameBytes)))
	}
	if staged.MIME == "" || len(staged.MIME) > 255 {
		return discard(finalizeNotCommitted(errors.New("mime must be 1..255 bytes")))
	}
	if staged.Size <= 0 {
		return discard(finalizeNotCommitted(ErrAttachmentEmpty))
	}
	if staged.Size > MaxAttachmentBytes {
		return discard(finalizeNotCommitted(ErrAttachmentTooLarge))
	}
	if len(staged.SHA256) != sha256.Size {
		return discard(finalizeNotCommitted(errors.New("attachment digest must be sha256")))
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return discard(finalizeNotCommitted(fmt.Errorf("begin attachment finalize: %w", err)))
	}
	reservationLocked := false
	// Register cleanup before rollback so the rollback runs first. A definite
	// failed finalize must not leave a live single-stager claim behind; the
	// token match prevents an old attempt from clearing a newer claimant.
	defer func() {
		if reservationLocked && retErr != nil && AttachmentFinalizeDefinitelyNotCommitted(retErr) {
			_ = s.AbandonAttachmentStaging(context.WithoutCancel(ctx), AttachmentUploadReservation{
				UploadID: staged.UploadID, PlaceID: placeID, StageToken: staged.StageToken,
			})
		}
	}()
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeMutationInTx(ctx, tx); err != nil {
		return discard(finalizeNotCommitted(err))
	}
	place, err := s.loadScopedPlace(ctx, tx, placeID)
	if err != nil {
		return discard(finalizeNotCommitted(err))
	}
	if _, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor); err != nil {
		return discard(finalizeNotCommitted(err))
	}
	// Finalization does not change quota totals (its reservation already owns
	// them), but it still takes the usage rows before its reservation lock so
	// it cannot invert reconciliation's global -> Workspace -> upload order.
	if _, err := lockAttachmentUsage(ctx, tx, s.Scope.WorkspaceID); err != nil {
		return discard(finalizeNotCommitted(err))
	}
	var (
		uploadID         string
		state            string
		declared         int64
		installation     string
		epoch            int64
		expiresAt        time.Time
		clientNonce      string
		stagingToken     string
		stagingExpiresAt *time.Time
	)
	err = tx.QueryRow(ctx, `
		SELECT upload_id, state, declared_bytes, installation_id, authority_epoch, expires_at, client_nonce,
		       COALESCE(staging_token::text, ''), staging_expires_at
		FROM message_attachment_uploads
		WHERE upload_id = $1 AND workspace_id = $2 AND place_id = $3
		  AND uploader_kind = $4 AND uploader_id = $5
		FOR UPDATE`,
		staged.UploadID, s.Scope.WorkspaceID, placeID, s.Scope.Actor.Kind, s.Scope.Actor.ID,
	).Scan(&uploadID, &state, &declared, &installation, &epoch, &expiresAt, &clientNonce, &stagingToken, &stagingExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return discard(finalizeNotCommitted(ErrAttachmentUploadExpired))
	}
	if err != nil {
		return discard(finalizeNotCommitted(fmt.Errorf("lock attachment reservation: %w", err)))
	}
	reservationLocked = true
	switch state {
	case "finalized":
		existing, found, err := s.attachmentByNonceInTx(ctx, tx, placeID, clientNonce)
		if err != nil {
			return discard(finalizeNotCommitted(err))
		}
		if !found {
			return discard(finalizeNotCommitted(ErrAttachmentUploadRetired))
		}
		if err := tx.Commit(ctx); err != nil {
			return discard(finalizeNotCommitted(fmt.Errorf("commit attachment receipt read: %w", err)))
		}
		_ = blobs.Discard(staged.Handle)
		return existing, false, nil
	case "released":
		return discard(finalizeNotCommitted(ErrAttachmentUploadExpired))
	case "reserved":
	default:
		return discard(finalizeNotCommitted(fmt.Errorf("unknown attachment reservation state %q", state)))
	}
	if !expiresAt.After(time.Now()) {
		return discard(finalizeNotCommitted(ErrAttachmentUploadExpired))
	}
	if installation != s.Scope.InstallationID || epoch != s.Scope.AuthorityEpoch {
		// The reservation was fenced to a different installation authority
		// than the one finalizing; the caller must preflight again.
		return discard(finalizeNotCommitted(ErrAttachmentUploadExpired))
	}
	if stagingToken != staged.StageToken || stagingExpiresAt == nil || !stagingExpiresAt.After(time.Now()) {
		return discard(finalizeNotCommitted(ErrAttachmentUploadExpired))
	}
	if declared != staged.Size {
		return discard(finalizeNotCommitted(ErrAttachmentSizeMismatch))
	}
	// Publish the bytes under the reservation lock. A leftover blob at the
	// final path can only be an artifact of an earlier attempt whose metadata
	// never committed (state is still 'reserved' under this lock), so
	// replacing it is safe.
	if err := blobs.Commit(staged.Handle); err != nil {
		return discard(finalizeNotCommitted(fmt.Errorf("publish attachment blob: %w", err)))
	}
	att := Attachment{
		AttachmentID: uploadID, WorkspaceID: s.Scope.WorkspaceID, PlaceID: placeID,
		Uploader: s.Scope.Actor, ClientNonce: clientNonce, Filename: filename,
		MIME: staged.MIME, SizeBytes: staged.Size, SHA256: staged.SHA256,
		BlobState: AttachmentBlobStored,
	}
	if err := tx.QueryRow(ctx, `
		INSERT INTO message_attachments
			(attachment_id, workspace_id, place_id, uploader_kind, uploader_id, client_nonce,
			 filename, mime, size_bytes, sha256)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING created_at`,
		att.AttachmentID, att.WorkspaceID, att.PlaceID, att.Uploader.Kind, att.Uploader.ID,
		att.ClientNonce, att.Filename, att.MIME, att.SizeBytes, att.SHA256,
	).Scan(&att.CreatedAt); err != nil {
		_ = blobs.Remove(uploadID)
		return Attachment{}, false, fmt.Errorf("insert attachment: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE message_attachment_uploads
		SET state = 'finalized', attachment_id = $2, settled_at = now(),
		    staging_token = NULL, staging_expires_at = NULL
		WHERE upload_id = $1`, uploadID, uploadID); err != nil {
		_ = blobs.Remove(uploadID)
		return Attachment{}, false, fmt.Errorf("finalize attachment reservation: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		// The commit outcome is unknown: the blob stays published for the
		// reconciler; if the row committed it is exactly right.
		return Attachment{}, false, fmt.Errorf("commit attachment finalize (outcome indeterminate): %w", err)
	}
	return att, true, nil
}

// AttachmentForViewer authorizes one attachment read against the current
// exact scope and place visibility. Rules:
//   - a bound attachment is readable by everyone who currently sees its
//     message (active place tenure, seq at or after their visible_from);
//   - an unbound attachment is readable by its uploader alone;
//   - a tombstoned message, a deleted blob, a foreign Workspace, or a stale
//     scope all report ErrAttachmentNotFound.
func (s *ScopedStore) AttachmentForViewer(ctx context.Context, attachmentID string) (Attachment, error) {
	if err := s.requireAttachments(); err != nil {
		return Attachment{}, err
	}
	if !validAttachmentID(attachmentID) {
		return Attachment{}, ErrAttachmentNotFound
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Attachment{}, fmt.Errorf("begin attachment read: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeInTx(ctx, tx); err != nil {
		return Attachment{}, err
	}
	att, err := s.attachmentForViewerInTx(ctx, tx, attachmentID)
	if err != nil {
		return Attachment{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Attachment{}, fmt.Errorf("commit attachment read: %w", err)
	}
	return att, nil
}

func (s *ScopedStore) attachmentForViewerInTx(ctx context.Context, q querier, attachmentID string) (Attachment, error) {
	att, err := scanAttachment(q.QueryRow(ctx, `
		SELECT `+attachmentColumns+` FROM message_attachments
		WHERE workspace_id = $1 AND attachment_id = $2`, s.Scope.WorkspaceID, attachmentID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Attachment{}, ErrAttachmentNotFound
	}
	if err != nil {
		return Attachment{}, fmt.Errorf("load attachment: %w", err)
	}
	if att.BlobState != AttachmentBlobStored {
		return Attachment{}, ErrAttachmentNotFound
	}
	place, err := s.loadScopedPlace(ctx, q, att.PlaceID)
	if err != nil {
		return Attachment{}, ErrAttachmentNotFound
	}
	access, err := s.placeAccessAfterAuthorization(ctx, q, place, s.Scope.Actor)
	if err != nil {
		return Attachment{}, ErrAttachmentNotFound
	}
	if att.MessageID == "" {
		if att.Uploader != s.Scope.Actor {
			return Attachment{}, ErrAttachmentNotFound
		}
		return att, nil
	}
	var seq int64
	var deleted bool
	err = q.QueryRow(ctx, `
		SELECT seq, deleted_at IS NOT NULL FROM messages
		WHERE workspace_id = $1 AND place_id = $2 AND message_id = $3`,
		s.Scope.WorkspaceID, att.PlaceID, att.MessageID).Scan(&seq, &deleted)
	if err != nil {
		return Attachment{}, ErrAttachmentNotFound
	}
	if deleted || seq < access.VisibleFromSeq {
		return Attachment{}, ErrAttachmentNotFound
	}
	return att, nil
}

// bindAttachmentsInTx binds the author's own finalized, unbound attachments to
// a freshly inserted message in the sender's order. One UPDATE carries the
// whole rule: the row must exist in this Workspace and place, be unbound,
// have stored bytes, and have been uploaded by this author. Anything else is
// ErrAttachmentNotFound and rolls the message back with it.
func (s *ScopedStore) bindAttachmentsInTx(ctx context.Context, tx pgx.Tx, placeID, messageID string, ids []string) ([]Attachment, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	if len(ids) > MaxAttachmentsPerMessage {
		return nil, ErrTooManyAttachments
	}
	seen := make(map[string]struct{}, len(ids))
	out := make([]Attachment, 0, len(ids))
	for position, id := range ids {
		if !validAttachmentID(id) {
			return nil, ErrAttachmentNotFound
		}
		if _, dup := seen[id]; dup {
			return nil, ErrAttachmentNotFound
		}
		seen[id] = struct{}{}
		att, err := scanAttachment(tx.QueryRow(ctx, `
			UPDATE message_attachments
			SET message_id = $1, position = $2, bound_at = now()
			WHERE attachment_id = $3 AND workspace_id = $4 AND place_id = $5
			  AND uploader_kind = $6 AND uploader_id = $7
			  AND message_id IS NULL AND blob_state = 'stored'
			RETURNING `+attachmentColumns,
			messageID, position, id, s.Scope.WorkspaceID, placeID,
			s.Scope.Actor.Kind, s.Scope.Actor.ID))
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrAttachmentNotFound
		}
		if err != nil {
			return nil, fmt.Errorf("bind attachment: %w", err)
		}
		out = append(out, att)
	}
	return out, nil
}

// attachAttachmentsWith projects each live message's ordered attachments.
// Tombstones carry nothing; their rows stay only as the record.
func attachAttachmentsWith(ctx context.Context, q querier, messages []Message) error {
	ids := make([]string, 0, len(messages))
	index := make(map[string]int, len(messages))
	for i, m := range messages {
		if m.Deleted {
			continue
		}
		ids = append(ids, m.MessageID)
		index[m.MessageID] = i
	}
	if len(ids) == 0 {
		return nil
	}
	rows, err := q.Query(ctx, `
		SELECT `+attachmentColumns+` FROM message_attachments
		WHERE message_id = ANY($1) AND blob_state <> 'deleted'
		ORDER BY message_id, position, attachment_id`, ids)
	if err != nil {
		return fmt.Errorf("query attachments: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		att, err := scanAttachment(rows)
		if err != nil {
			return fmt.Errorf("scan attachment: %w", err)
		}
		i := index[att.MessageID]
		messages[i].Attachments = append(messages[i].Attachments, att)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate attachments: %w", err)
	}
	return nil
}

// enqueueAttachmentDeletionsInTx moves every stored blob of a message into the
// durable deletion outbox. Bytes are removed asynchronously; the rows remain.
func enqueueAttachmentDeletionsInTx(ctx context.Context, tx pgx.Tx, workspaceID, messageID string) error {
	if _, err := tx.Exec(ctx, `
		UPDATE message_attachments SET blob_state = 'deleting'
		WHERE workspace_id = $1 AND message_id = $2 AND blob_state = 'stored'`,
		workspaceID, messageID); err != nil {
		return fmt.Errorf("enqueue attachment deletions: %w", err)
	}
	return nil
}

// messageRequestDigest is the canonical identity of one send request. Nonce
// replay compares it so a changed request under the same nonce is a conflict.
// Attachment identities are server-minted and one-to-one with immutable
// manifests, so listing them in order fixes the manifests as well.
func messageRequestDigest(content, urgency, replyTo string, attachmentIDs []string) []byte {
	ids := attachmentIDs
	if ids == nil {
		ids = []string{}
	}
	canonical, err := json.Marshal(struct {
		Content     string   `json:"content"`
		Urgency     string   `json:"urgency"`
		ReplyTo     string   `json:"reply_to"`
		Attachments []string `json:"attachments"`
	}{Content: content, Urgency: urgency, ReplyTo: replyTo, Attachments: ids})
	if err != nil {
		panic(fmt.Sprintf("marshal message request digest: %v", err))
	}
	sum := sha256.Sum256(append([]byte("sumi-messaging-request-v1\x00"), canonical...))
	return sum[:]
}
