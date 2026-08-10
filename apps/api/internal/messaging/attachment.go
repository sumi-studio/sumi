package messaging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// MaxAttachmentBytes bounds one uploaded file (20 MiB), matching the CHECK on
// message_attachments.size_bytes.
const MaxAttachmentBytes int64 = 20 << 20

// MaxAttachmentsPerMessage bounds how many attachments one send may carry.
const MaxAttachmentsPerMessage = 10

// MaxAttachmentFilenameBytes matches the schema CHECK on filename.
const MaxAttachmentFilenameBytes = 255

// MaxAttachmentAltRunes matches the schema CHECK on alt: the description is a
// sentence about the file, not a second message.
const MaxAttachmentAltRunes = 1000

// AttachmentDraftLease is the server-side lifetime of an uploaded attachment
// that is still held by a live composer. Active browser sessions renew the
// lease; message/profile publication makes the blob durable independently of
// it. The lease is deliberately measured in days so ordinary browser sleep is
// not mistaken for abandonment.
const AttachmentDraftLease = 7 * 24 * time.Hour

// AttachmentOrphanGrace keeps a blob written just before its metadata row from
// being mistaken for crash debris by the filesystem reconciler.
const AttachmentOrphanGrace = 24 * time.Hour

// AttachmentSweepInterval is how often the reclaimer advances one bounded
// metadata/filesystem batch.
const AttachmentSweepInterval = time.Hour

// MaxAttachmentDraftRenewal bounds one heartbeat request. The Web client uses
// smaller batches; this remains an application-level resource bound for every
// caller.
const MaxAttachmentDraftRenewal = 100

// Attachment sentinels. ErrAttachmentNotFound doubles as the authorization
// failure: an attachment the caller may not see, may not bind, or that never
// existed are all reported identically, so existence never leaks.
var (
	ErrAttachmentNotFound = errors.New("attachment not found")
	ErrAttachmentTooLarge = errors.New("attachment exceeds the size limit")
	ErrAttachmentEmpty    = errors.New("attachment has no bytes")
	ErrTooManyAttachments = errors.New("too many attachments for one message")
	// ErrAttachmentAlreadySent is a draft edit arriving after the attachment
	// became part of a message. What was sent is what was sent.
	ErrAttachmentAlreadySent = errors.New("attachment is already part of a message")
)

// Attachment is one uploaded file. It is minted unbound (MessageID empty) and
// becomes part of a message when its uploader sends one carrying it.
type Attachment struct {
	AttachmentID string
	MessageID    string // empty while unbound
	Uploader     ParticipantRef
	Filename     string
	MIME         string
	SizeBytes    int64
	// Spoiler hides the content behind a reveal on the receiving side. It is
	// the sender's declaration about the file, so it travels with the file and
	// not with the message text.
	Spoiler bool
	// Alt describes the content for someone who cannot or should not see it
	// yet — a screen reader, or a PersonalityAgent reading the timeline.
	Alt       string
	CreatedAt time.Time
	// DraftExpiresAt is meaningful only while MessageID is empty. It is not
	// delivered with a sent message; it exists so active composers can keep
	// their private upload alive without making abandoned uploads immortal.
	DraftExpiresAt time.Time
}

// AttachmentDraftPatch edits an attachment that has not been sent yet. A nil
// field is「触らない」; a non-nil one is the new value.
type AttachmentDraftPatch struct {
	Filename *string
	Alt      *string
	Spoiler  *bool
}

// NewAttachmentID mints the identity of an upload. The caller writes the bytes
// under this id first and records the metadata with CreateAttachment after, so
// a row never points at a blob that does not exist.
func NewAttachmentID() string { return newUUIDv7() }

// CreateAttachment records an uploaded file's metadata. The bytes are the
// caller's responsibility (AttachmentBlobs); this row is the durable identity
// and the visibility record.
func (s *Store) CreateAttachment(ctx context.Context, attachmentID string, uploader ParticipantRef, filename, mime string, sizeBytes int64) (Attachment, error) {
	if !validAttachmentID(attachmentID) {
		return Attachment{}, fmt.Errorf("attachment id must be a canonical UUIDv7")
	}
	if err := uploader.Validate(); err != nil {
		return Attachment{}, err
	}
	if err := s.participantExists(ctx, uploader); err != nil {
		return Attachment{}, err
	}
	filename = strings.TrimSpace(filename)
	if filename == "" || len(filename) > MaxAttachmentFilenameBytes {
		return Attachment{}, fmt.Errorf("filename must be 1..%d bytes", MaxAttachmentFilenameBytes)
	}
	if mime == "" || len(mime) > 255 {
		return Attachment{}, fmt.Errorf("mime must be 1..255 bytes")
	}
	if sizeBytes <= 0 {
		return Attachment{}, ErrAttachmentEmpty
	}
	if sizeBytes > MaxAttachmentBytes {
		return Attachment{}, ErrAttachmentTooLarge
	}
	att := Attachment{
		AttachmentID:   attachmentID,
		Uploader:       uploader,
		Filename:       filename,
		MIME:           mime,
		SizeBytes:      sizeBytes,
		DraftExpiresAt: time.Now().Add(AttachmentDraftLease),
	}
	if err := s.pool.QueryRow(ctx,
		`INSERT INTO message_attachments
		   (attachment_id, uploader_kind, uploader_id, filename, mime, size_bytes, draft_expires_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 RETURNING created_at, draft_expires_at`,
		att.AttachmentID, uploader.Kind, uploader.ID,
		att.Filename, att.MIME, att.SizeBytes, att.DraftExpiresAt).
		Scan(&att.CreatedAt, &att.DraftExpiresAt); err != nil {
		return Attachment{}, fmt.Errorf("insert attachment: %w", err)
	}
	return att, nil
}

// AttachmentForViewer loads an attachment the viewer is allowed to read. The
// rule lives here, not in the transport, so REST, WebSocket, and the local
// control lane (agent tools, #209) cannot diverge:
//   - a bound attachment is readable by everyone who can see its message's
//     place (the same canAccess rule that governs the message itself);
//   - an unbound attachment used as somebody's avatar or header is readable by
//     everyone who can see that participant (a face on the member list is meant
//     to be seen);
//   - any other unbound attachment is readable by its uploader alone;
//   - a tombstoned message delivers nothing, its uploader included.
func (s *Store) AttachmentForViewer(ctx context.Context, attachmentID string, viewer ParticipantRef) (Attachment, error) {
	if err := viewer.Validate(); err != nil {
		return Attachment{}, err
	}
	if !validAttachmentID(attachmentID) {
		return Attachment{}, ErrAttachmentNotFound
	}
	var (
		att       Attachment
		messageID *string
		kind      string
		placeID   *string
		deletedAt *time.Time
	)
	err := s.pool.QueryRow(ctx,
		`SELECT a.attachment_id, a.message_id, a.uploader_kind, a.uploader_id,
		        a.filename, a.mime, a.size_bytes, a.spoiler, a.alt, a.created_at,
		        a.draft_expires_at,
		        m.place_id, m.deleted_at
		 FROM message_attachments a
		 LEFT JOIN messages m ON m.message_id = a.message_id
		 WHERE a.attachment_id = $1`, attachmentID).
		Scan(&att.AttachmentID, &messageID, &kind, &att.Uploader.ID,
			&att.Filename, &att.MIME, &att.SizeBytes, &att.Spoiler, &att.Alt,
			&att.CreatedAt, &att.DraftExpiresAt, &placeID, &deletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Attachment{}, ErrAttachmentNotFound
	}
	if err != nil {
		return Attachment{}, fmt.Errorf("load attachment: %w", err)
	}
	att.Uploader.Kind = ParticipantKind(kind)
	if messageID == nil || placeID == nil {
		if att.Uploader == viewer {
			return att, nil
		}
		profileImage, err := s.attachmentIsProfileImage(ctx, attachmentID, viewer)
		if err != nil {
			return Attachment{}, err
		}
		if !profileImage {
			return Attachment{}, ErrAttachmentNotFound
		}
		return att, nil
	}
	if deletedAt != nil {
		return Attachment{}, ErrAttachmentNotFound
	}
	att.MessageID = *messageID
	place, err := s.loadPlace(ctx, s.pool, *placeID)
	if err != nil {
		if errors.Is(err, ErrPlaceNotFound) {
			return Attachment{}, ErrAttachmentNotFound
		}
		return Attachment{}, err
	}
	visible, err := s.canAccess(ctx, s.pool, place, viewer)
	if err != nil {
		return Attachment{}, err
	}
	if !visible {
		return Attachment{}, ErrAttachmentNotFound
	}
	return att, nil
}

// UpdateDraftAttachment edits an upload that has not been sent yet: its display
// name, its description, and whether it arrives hidden behind a spoiler. The
// window is deliberately narrow — the uploader's own attachment, still unbound
// — because these are things you decide *before* pressing send. Once the
// attachment is part of a message, what the recipients saw is what was sent,
// so the edit is refused rather than silently rewriting history.
//
// An attachment belonging to someone else is reported as missing, never as
// forbidden, matching AttachmentForViewer: existence never leaks.
func (s *Store) UpdateDraftAttachment(ctx context.Context, attachmentID string, uploader ParticipantRef, patch AttachmentDraftPatch) (Attachment, error) {
	if err := uploader.Validate(); err != nil {
		return Attachment{}, err
	}
	if !validAttachmentID(attachmentID) {
		return Attachment{}, ErrAttachmentNotFound
	}
	if patch.Filename != nil {
		name := strings.TrimSpace(*patch.Filename)
		if name == "" || len(name) > MaxAttachmentFilenameBytes {
			return Attachment{}, fmt.Errorf("filename must be 1..%d bytes", MaxAttachmentFilenameBytes)
		}
		patch.Filename = &name
	}
	if patch.Alt != nil && utf8.RuneCountInString(*patch.Alt) > MaxAttachmentAltRunes {
		return Attachment{}, fmt.Errorf("alt must be at most %d characters", MaxAttachmentAltRunes)
	}
	var (
		att       Attachment
		messageID *string
		kind      string
	)
	// COALESCE keeps「触らない」honest: a nil field reads back the stored value
	// in the same statement that writes the others.
	expiresAt := time.Now().Add(AttachmentDraftLease)
	err := s.pool.QueryRow(ctx,
		`UPDATE message_attachments
		    SET filename = COALESCE($4, filename),
		        alt      = COALESCE($5, alt),
		        spoiler  = COALESCE($6, spoiler),
		        draft_expires_at = $7
		  WHERE attachment_id = $1 AND uploader_kind = $2 AND uploader_id = $3
		    AND message_id IS NULL
		 RETURNING attachment_id, message_id, uploader_kind, uploader_id,
		           filename, mime, size_bytes, spoiler, alt, created_at,
		           draft_expires_at`,
		attachmentID, uploader.Kind, uploader.ID,
		patch.Filename, patch.Alt, patch.Spoiler, expiresAt).
		Scan(&att.AttachmentID, &messageID, &kind, &att.Uploader.ID,
			&att.Filename, &att.MIME, &att.SizeBytes, &att.Spoiler, &att.Alt,
			&att.CreatedAt, &att.DraftExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		// Either it is not ours / does not exist, or it has already been sent.
		// Tell those apart only for our own already-sent upload: the person
		// deserves to know why the edit did not land on their own file.
		existing, loadErr := s.AttachmentForViewer(ctx, attachmentID, uploader)
		if loadErr == nil && existing.Uploader == uploader && existing.MessageID != "" {
			return Attachment{}, ErrAttachmentAlreadySent
		}
		return Attachment{}, ErrAttachmentNotFound
	}
	if err != nil {
		return Attachment{}, fmt.Errorf("update attachment: %w", err)
	}
	att.Uploader.Kind = ParticipantKind(kind)
	return att, nil
}

// RenewDraftAttachments extends the lease for a bounded set of the uploader's
// still-unbound attachments. This is application-owned draft maintenance, not
// a new authorization decision: the uploader can renew only the same private
// rows they can already edit and bind. A missing, sent, profile-owned, or
// foreign id makes the heartbeat fail without revealing which case occurred.
func (s *Store) RenewDraftAttachments(ctx context.Context, uploader ParticipantRef, ids []string, now time.Time) error {
	if err := uploader.Validate(); err != nil {
		return err
	}
	if len(ids) == 0 || len(ids) > MaxAttachmentDraftRenewal {
		return ErrAttachmentNotFound
	}
	unique := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if !validAttachmentID(id) {
			return ErrAttachmentNotFound
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	sort.Strings(unique)
	expiresAt := now.Add(AttachmentDraftLease)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin renew attachment drafts: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx,
		`SELECT a.attachment_id FROM message_attachments a
		  WHERE a.attachment_id = ANY($1)
		    AND a.uploader_kind = $2 AND a.uploader_id = $3
		    AND a.message_id IS NULL
		    AND NOT EXISTS (
		      SELECT 1 FROM participant_profiles p
		      WHERE p.avatar_attachment_id = a.attachment_id
		         OR p.banner_attachment_id = a.attachment_id)
		  ORDER BY a.attachment_id
		  FOR UPDATE`, unique, uploader.Kind, uploader.ID)
	if err != nil {
		return fmt.Errorf("lock attachment drafts for renewal: %w", err)
	}
	defer rows.Close()
	locked := 0
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("scan attachment draft for renewal: %w", err)
		}
		locked++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate attachment drafts for renewal: %w", err)
	}
	rows.Close()
	if locked != len(unique) {
		return ErrAttachmentNotFound
	}
	if _, err := tx.Exec(ctx,
		`UPDATE message_attachments
		 SET draft_expires_at = $2
		 WHERE attachment_id = ANY($1)`, unique, expiresAt); err != nil {
		return fmt.Errorf("renew attachment drafts: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit attachment draft renewal: %w", err)
	}
	return nil
}

// bindAttachments binds the author's own unbound attachments to a message in
// the send transaction. A single UPDATE carries the whole rule: the row must
// exist, must still be unbound, must not already be somebody's profile image,
// and must have been uploaded by this author. Anything else — someone else's
// attachment, an already-sent one, an avatar, a made-up id — is
// ErrAttachmentNotFound.
//
// Excluding profile images keeps the two lifetimes apart: an avatar that became
// a message attachment would vanish from every member list the moment that
// message was deleted.
func bindAttachments(ctx context.Context, tx pgx.Tx, messageID string, author ParticipantRef, ids []string) ([]Attachment, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	if len(ids) > MaxAttachmentsPerMessage {
		return nil, ErrTooManyAttachments
	}
	// Lock every candidate in a stable order before the first UPDATE. A profile
	// replacement locks the same row before publishing it in
	// participant_profiles. The UPDATE below is intentionally a later statement:
	// under READ COMMITTED it receives a fresh snapshot after any waited-on
	// profile transaction commits, so NOT EXISTS cannot approve the row from the
	// stale snapshot with which this bind began.
	ordered := append([]string(nil), ids...)
	sort.Strings(ordered)
	previous := ""
	for _, id := range ordered {
		if !validAttachmentID(id) {
			return nil, ErrAttachmentNotFound
		}
		if id == previous {
			continue
		}
		previous = id
		var lockedID string
		err := tx.QueryRow(ctx,
			`SELECT attachment_id FROM message_attachments
			 WHERE attachment_id = $1 AND message_id IS NULL
			   AND uploader_kind = $2 AND uploader_id = $3
			 FOR UPDATE`, id, author.Kind, author.ID).Scan(&lockedID)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrAttachmentNotFound
		}
		if err != nil {
			return nil, fmt.Errorf("lock attachment for binding: %w", err)
		}
	}
	out := make([]Attachment, 0, len(ids))
	for position, id := range ids {
		att := Attachment{AttachmentID: id, MessageID: messageID, Uploader: author}
		// position is the sender's order, not the upload order: uploads run in
		// parallel and finish out of order, so created_at cannot stand in for it.
		err := tx.QueryRow(ctx,
			`UPDATE message_attachments SET message_id = $1, position = $5
			 WHERE attachment_id = $2 AND message_id IS NULL
			   AND uploader_kind = $3 AND uploader_id = $4
			   AND NOT EXISTS (
			     SELECT 1 FROM participant_profiles p
			     WHERE p.avatar_attachment_id = $2 OR p.banner_attachment_id = $2)
			 RETURNING filename, mime, size_bytes, spoiler, alt, created_at`,
			messageID, id, author.Kind, author.ID, position).
			Scan(&att.Filename, &att.MIME, &att.SizeBytes, &att.Spoiler, &att.Alt, &att.CreatedAt)
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

// attachAttachments loads attachment rows for the given messages in one query,
// mirroring attachMentions.
func (s *Store) attachAttachments(ctx context.Context, messages []Message) error {
	if len(messages) == 0 {
		return nil
	}
	ids := make([]string, 0, len(messages))
	index := make(map[string]int, len(messages))
	for i, m := range messages {
		// A tombstone carries nothing; its rows stay only as the record.
		if m.Deleted {
			continue
		}
		ids = append(ids, m.MessageID)
		index[m.MessageID] = i
	}
	if len(ids) == 0 {
		return nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT message_id, attachment_id, uploader_kind, uploader_id,
		        filename, mime, size_bytes, spoiler, alt, created_at
		 FROM message_attachments
		 WHERE message_id = ANY($1)
		 ORDER BY message_id, position, attachment_id`, ids)
	if err != nil {
		return fmt.Errorf("query attachments: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			messageID string
			att       Attachment
			kind      string
		)
		if err := rows.Scan(&messageID, &att.AttachmentID, &kind, &att.Uploader.ID,
			&att.Filename, &att.MIME, &att.SizeBytes, &att.Spoiler, &att.Alt,
			&att.CreatedAt); err != nil {
			return fmt.Errorf("scan attachment: %w", err)
		}
		att.MessageID = messageID
		att.Uploader.Kind = ParticipantKind(kind)
		i := index[messageID]
		messages[i].Attachments = append(messages[i].Attachments, att)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate attachments: %w", err)
	}
	return nil
}

// --- reclaiming uploads that never became messages or profile images ---

// attachmentSweepBatch bounds one pass so a large backlog is worked off over
// several runs instead of one long transaction.
const attachmentSweepBatch = 500

// ExpiredUnboundAttachments lists uploads whose renewable draft lease expired
// and that belong to neither a message nor a participant profile. Profile
// images intentionally remain unbound for their whole lifetime and therefore
// are not abandoned uploads.
func (s *Store) ExpiredUnboundAttachments(ctx context.Context, cutoff time.Time, limit int) ([]string, error) {
	if limit <= 0 {
		limit = attachmentSweepBatch
	}
	rows, err := s.pool.Query(ctx,
		`SELECT a.attachment_id FROM message_attachments a
		 WHERE a.message_id IS NULL AND a.draft_expires_at < $1
		   AND NOT EXISTS (
		     SELECT 1 FROM participant_profiles p
		     WHERE p.avatar_attachment_id = a.attachment_id
		        OR p.banner_attachment_id = a.attachment_id)
		 ORDER BY a.draft_expires_at, a.attachment_id
		 LIMIT $2`, cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("query expired attachments: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan expired attachment: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate expired attachments: %w", err)
	}
	return ids, nil
}

// DeleteUnboundAttachments removes metadata for ids that are still unused and
// returns the rows it actually deleted, so the caller removes only the blobs it
// just orphaned. Candidate rows are locked first in stable order and deleted by
// a later statement. A racing message bind or profile publication therefore
// chooses exactly one winner: if it commits first the fresh delete snapshot
// preserves the row; if deletion commits first, the other mutation sees no row.
func (s *Store) DeleteExpiredUnboundAttachments(ctx context.Context, ids []string, cutoff time.Time) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	ordered := append([]string(nil), ids...)
	sort.Strings(ordered)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin delete unbound attachments: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx,
		`SELECT attachment_id FROM message_attachments
		 WHERE attachment_id = ANY($1) AND message_id IS NULL
		   AND draft_expires_at < $2
		 ORDER BY attachment_id
		 FOR UPDATE`, ordered, cutoff)
	if err != nil {
		return nil, fmt.Errorf("lock unbound attachments: %w", err)
	}
	locked := make([]string, 0, len(ordered))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan locked attachment: %w", err)
		}
		locked = append(locked, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate locked attachments: %w", err)
	}
	rows.Close()

	deleted := make([]string, 0, len(locked))
	if len(locked) > 0 {
		rows, err = tx.Query(ctx,
			`DELETE FROM message_attachments a
			 WHERE a.attachment_id = ANY($1) AND a.message_id IS NULL
			   AND a.draft_expires_at < $2
			   AND NOT EXISTS (
			     SELECT 1 FROM participant_profiles p
			     WHERE p.avatar_attachment_id = a.attachment_id
			        OR p.banner_attachment_id = a.attachment_id)
			 RETURNING a.attachment_id`, locked, cutoff)
		if err != nil {
			return nil, fmt.Errorf("delete unbound attachments: %w", err)
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan deleted attachment: %w", err)
			}
			deleted = append(deleted, id)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("iterate deleted attachments: %w", err)
		}
		rows.Close()
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit delete unbound attachments: %w", err)
	}
	return deleted, nil
}

// KnownAttachmentIDs reports which ids still have metadata. Complete blobs
// absent from this set are orphans left by a crash before metadata insertion.
func (s *Store) KnownAttachmentIDs(ctx context.Context, ids []string) (map[string]struct{}, error) {
	known := make(map[string]struct{}, len(ids))
	if len(ids) == 0 {
		return known, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT attachment_id FROM message_attachments WHERE attachment_id = ANY($1)`, ids)
	if err != nil {
		return nil, fmt.Errorf("query known attachments: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan known attachment: %w", err)
		}
		known[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate known attachments: %w", err)
	}
	return known, nil
}

// AttachmentSweep reports what one reclamation pass reclaimed.
type AttachmentSweep struct {
	Expired  int // expired unused rows whose blobs were removed
	Orphaned int // old complete blobs with no metadata row
}

// SweepAttachments reclaims uploads that belong to neither messages nor
// participant profiles. Metadata is deleted before its blob; a crash between
// those operations leaves an orphan blob, which the second reconciliation pass
// removes. Bound rows and profile-image rows are durable records and survive.
func (s *Server) SweepAttachments(ctx context.Context, orphanGrace time.Duration) (AttachmentSweep, error) {
	var swept AttachmentSweep
	if s.Attachments == nil || s.Store == nil {
		return swept, nil
	}
	if orphanGrace <= 0 {
		orphanGrace = AttachmentOrphanGrace
	}
	now := time.Now()

	expired, err := s.Store.ExpiredUnboundAttachments(ctx, now, attachmentSweepBatch)
	if err != nil {
		return swept, err
	}
	deleted, err := s.Store.DeleteExpiredUnboundAttachments(ctx, expired, now)
	if err != nil {
		return swept, err
	}
	for _, id := range deleted {
		if err := s.Attachments.Remove(id); err != nil {
			return swept, fmt.Errorf("remove expired attachment %s: %w", id, err)
		}
		swept.Expired++
	}

	page, err := s.Attachments.Sweep(ctx, now.Add(-orphanGrace), attachmentSweepBatch)
	if err != nil {
		return swept, err
	}
	candidates := page.Candidates
	known, err := s.Store.KnownAttachmentIDs(ctx, candidates)
	if err != nil {
		return swept, err
	}
	for _, id := range candidates {
		if _, ok := known[id]; ok {
			continue
		}
		if err := s.Attachments.Remove(id); err != nil {
			return swept, fmt.Errorf("remove orphaned attachment %s: %w", id, err)
		}
		swept.Orphaned++
	}
	return swept, nil
}

// RunAttachmentSweeper reclaims unused uploads on a timer until ctx is done.
// A failing pass is logged, not fatal: the next pass sees the same backlog.
func (s *Server) RunAttachmentSweeper(ctx context.Context, orphanGrace, interval time.Duration) {
	if s.Attachments == nil || s.Store == nil {
		return
	}
	if interval <= 0 {
		interval = AttachmentSweepInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		swept, err := s.SweepAttachments(ctx, orphanGrace)
		switch {
		case err != nil && ctx.Err() == nil:
			log.Printf("messaging attachment sweep: %v", err)
		case swept.Expired > 0 || swept.Orphaned > 0:
			log.Printf("messaging attachment sweep reclaimed %d unused, %d orphaned",
				swept.Expired, swept.Orphaned)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func validAttachmentID(id string) bool {
	parsed, err := uuid.Parse(id)
	if err != nil {
		return false
	}
	// The uuidv7 domain accepts canonical lowercase hyphenated v7 only; reject
	// anything else here so a malformed id is a clean not-found instead of a
	// database type error.
	return parsed.Version() == 7 && parsed.String() == id
}

// --- blob storage ---

// AttachmentBlobs stores attachment bytes outside the database. Local disk is
// the v0 implementation; an object store slots in behind the same interface.
type AttachmentBlobs interface {
	// Put streams r into the blob for id, refusing more than MaxAttachmentBytes
	// with ErrAttachmentTooLarge, and returns the stored size.
	Put(id string, r io.Reader) (int64, error)
	// Open returns the stored bytes for range-capable delivery.
	Open(id string) (io.ReadSeekCloser, error)
	// Remove deletes the blob. Used to undo a write whose metadata row failed.
	Remove(id string) error
	// Sweep advances a context-cancellable, bounded filesystem scan. It removes
	// storage-internal temp debris older than cutoff and returns complete old
	// blob ids for metadata reconciliation. The age boundary keeps an ordinary
	// Put-before-row write from being mistaken for an orphan.
	Sweep(ctx context.Context, cutoff time.Time, maxEntries int) (BlobSweepPage, error)
}

// BlobSweepPage is one bounded step through the on-disk blob namespace.
type BlobSweepPage struct {
	Candidates    []string
	Visited       int
	CycleComplete bool
}

// DiskAttachments keeps attachment bytes under Root, sharded by the first two
// byte-pairs of the attachment id so no directory grows unbounded:
// <root>/01/90/0190....bin. Ids are validated UUIDv7 strings, so no path
// component is ever caller-controlled.
type DiskAttachments struct {
	Root string

	sweepMu     sync.Mutex
	sweepFrames []diskAttachmentSweepFrame
}

// diskAttachmentSweepFrame retains a directory offset between bounded sweeper
// passes. The blob layout is exactly two shard directories deep, so at most
// three handles are alive and total traversal work is amortized over time.
type diskAttachmentSweepFrame struct {
	dir   *os.File
	path  string
	depth int
}

// NewDiskAttachments returns a disk blob store rooted at root, creating it.
func NewDiskAttachments(root string) (*DiskAttachments, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, fmt.Errorf("attachment root must not be empty")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create attachment root: %w", err)
	}
	return &DiskAttachments{Root: root}, nil
}

func (d *DiskAttachments) path(id string) (string, error) {
	if !validAttachmentID(id) {
		return "", ErrAttachmentNotFound
	}
	return filepath.Join(d.Root, id[0:2], id[2:4], id+".bin"), nil
}

func (d *DiskAttachments) Put(id string, r io.Reader) (int64, error) {
	path, err := d.path(id)
	if err != nil {
		return 0, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return 0, fmt.Errorf("create attachment directory: %w", err)
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".upload-*")
	if err != nil {
		return 0, fmt.Errorf("create attachment temp file: %w", err)
	}
	tempName := temp.Name()
	defer func() {
		temp.Close()
		_ = os.Remove(tempName)
	}()
	// One byte past the limit is enough to know the upload is oversized.
	size, err := io.Copy(temp, io.LimitReader(r, MaxAttachmentBytes+1))
	if err != nil {
		return 0, fmt.Errorf("write attachment: %w", err)
	}
	if size > MaxAttachmentBytes {
		return 0, ErrAttachmentTooLarge
	}
	if size == 0 {
		return 0, ErrAttachmentEmpty
	}
	if err := temp.Sync(); err != nil {
		return 0, fmt.Errorf("sync attachment: %w", err)
	}
	if err := temp.Close(); err != nil {
		return 0, fmt.Errorf("close attachment: %w", err)
	}
	if err := os.Chmod(tempName, 0o600); err != nil {
		return 0, fmt.Errorf("chmod attachment: %w", err)
	}
	if err := os.Rename(tempName, path); err != nil {
		return 0, fmt.Errorf("commit attachment: %w", err)
	}
	return size, nil
}

func (d *DiskAttachments) Open(id string) (io.ReadSeekCloser, error) {
	path, err := d.path(id)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrAttachmentNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("open attachment: %w", err)
	}
	return file, nil
}

func (d *DiskAttachments) Remove(id string) error {
	path, err := d.path(id)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove attachment: %w", err)
	}
	return nil
}

func (d *DiskAttachments) Sweep(ctx context.Context, cutoff time.Time, maxEntries int) (BlobSweepPage, error) {
	var page BlobSweepPage
	if maxEntries <= 0 {
		maxEntries = attachmentSweepBatch
	}
	d.sweepMu.Lock()
	defer d.sweepMu.Unlock()

	if err := ctx.Err(); err != nil {
		d.resetSweepLocked()
		return page, err
	}
	if len(d.sweepFrames) == 0 {
		root, err := os.Open(d.Root)
		if errors.Is(err, os.ErrNotExist) {
			page.CycleComplete = true
			return page, nil
		}
		if err != nil {
			return page, fmt.Errorf("open attachment root for sweep: %w", err)
		}
		d.sweepFrames = append(d.sweepFrames, diskAttachmentSweepFrame{
			dir: root, path: d.Root,
		})
	}

	page.Candidates = make([]string, 0, maxEntries)
	for page.Visited < maxEntries {
		if err := ctx.Err(); err != nil {
			d.resetSweepLocked()
			return BlobSweepPage{}, err
		}
		frame := &d.sweepFrames[len(d.sweepFrames)-1]
		entries, err := frame.dir.ReadDir(1)
		if errors.Is(err, io.EOF) {
			_ = frame.dir.Close()
			d.sweepFrames = d.sweepFrames[:len(d.sweepFrames)-1]
			if len(d.sweepFrames) == 0 {
				page.CycleComplete = true
				return page, nil
			}
			continue
		}
		if err != nil {
			d.resetSweepLocked()
			return BlobSweepPage{}, fmt.Errorf("read attachment directory for sweep: %w", err)
		}
		page.Visited++
		entry := entries[0]
		path := filepath.Join(frame.path, entry.Name())

		if entry.IsDir() {
			if frame.depth >= 2 || !validAttachmentShard(entry.Name()) {
				continue
			}
			child, err := os.Open(path)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				d.resetSweepLocked()
				return BlobSweepPage{}, fmt.Errorf("open attachment shard for sweep: %w", err)
			}
			d.sweepFrames = append(d.sweepFrames, diskAttachmentSweepFrame{
				dir: child, path: path, depth: frame.depth + 1,
			})
			continue
		}

		info, err := entry.Info()
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			d.resetSweepLocked()
			return BlobSweepPage{}, fmt.Errorf("inspect attachment entry for sweep: %w", err)
		}
		if !info.Mode().IsRegular() {
			continue
		}
		if !info.ModTime().Before(cutoff) {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, ".upload-") {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				d.resetSweepLocked()
				return BlobSweepPage{}, fmt.Errorf("remove abandoned upload: %w", err)
			}
			continue
		}
		if frame.depth != 2 {
			continue
		}
		id := strings.TrimSuffix(name, ".bin")
		if id == name || !validAttachmentID(id) {
			continue
		}
		expected, err := d.path(id)
		if err != nil || filepath.Clean(expected) != filepath.Clean(path) {
			continue
		}
		page.Candidates = append(page.Candidates, id)
	}
	return page, nil
}

func validAttachmentShard(name string) bool {
	if len(name) != 2 {
		return false
	}
	for _, char := range name {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			return false
		}
	}
	return true
}

func (d *DiskAttachments) resetSweepLocked() {
	for i := range d.sweepFrames {
		_ = d.sweepFrames[i].dir.Close()
	}
	d.sweepFrames = nil
}
