package messaging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

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

// Attachment sentinels. ErrAttachmentNotFound doubles as the authorization
// failure: an attachment the caller may not see, may not bind, or that never
// existed are all reported identically, so existence never leaks.
var (
	ErrAttachmentNotFound = errors.New("attachment not found")
	ErrAttachmentTooLarge = errors.New("attachment exceeds the size limit")
	ErrAttachmentEmpty    = errors.New("attachment has no bytes")
	ErrTooManyAttachments = errors.New("too many attachments for one message")
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
	CreatedAt    time.Time
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
		AttachmentID: attachmentID,
		Uploader:     uploader,
		Filename:     filename,
		MIME:         mime,
		SizeBytes:    sizeBytes,
	}
	if err := s.pool.QueryRow(ctx,
		`INSERT INTO message_attachments
		   (attachment_id, uploader_kind, uploader_id, filename, mime, size_bytes)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING created_at`,
		att.AttachmentID, uploader.Kind, uploader.ID,
		att.Filename, att.MIME, att.SizeBytes).Scan(&att.CreatedAt); err != nil {
		return Attachment{}, fmt.Errorf("insert attachment: %w", err)
	}
	return att, nil
}

// AttachmentForViewer loads an attachment the viewer is allowed to read. The
// rule lives here, not in the transport, so REST, WebSocket, and the local
// control lane (agent tools, #209) cannot diverge:
//   - a bound attachment is readable by everyone who can see its message's
//     place (the same canAccess rule that governs the message itself);
//   - an unbound attachment is readable by its uploader alone;
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
		        a.filename, a.mime, a.size_bytes, a.created_at, m.place_id, m.deleted_at
		 FROM message_attachments a
		 LEFT JOIN messages m ON m.message_id = a.message_id
		 WHERE a.attachment_id = $1`, attachmentID).
		Scan(&att.AttachmentID, &messageID, &kind, &att.Uploader.ID,
			&att.Filename, &att.MIME, &att.SizeBytes, &att.CreatedAt, &placeID, &deletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Attachment{}, ErrAttachmentNotFound
	}
	if err != nil {
		return Attachment{}, fmt.Errorf("load attachment: %w", err)
	}
	att.Uploader.Kind = ParticipantKind(kind)
	if messageID == nil || placeID == nil {
		if att.Uploader != viewer {
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

// bindAttachments binds the author's own unbound attachments to a message in
// the send transaction. A single UPDATE carries the whole rule: the row must
// exist, must still be unbound, and must have been uploaded by this author.
// Anything else — someone else's attachment, an already-sent one, a made-up id
// — is ErrAttachmentNotFound.
func bindAttachments(ctx context.Context, tx pgx.Tx, messageID string, author ParticipantRef, ids []string) ([]Attachment, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	if len(ids) > MaxAttachmentsPerMessage {
		return nil, ErrTooManyAttachments
	}
	out := make([]Attachment, 0, len(ids))
	for _, id := range ids {
		if !validAttachmentID(id) {
			return nil, ErrAttachmentNotFound
		}
		att := Attachment{AttachmentID: id, MessageID: messageID, Uploader: author}
		err := tx.QueryRow(ctx,
			`UPDATE message_attachments SET message_id = $1
			 WHERE attachment_id = $2 AND message_id IS NULL
			   AND uploader_kind = $3 AND uploader_id = $4
			 RETURNING filename, mime, size_bytes, created_at`,
			messageID, id, author.Kind, author.ID).
			Scan(&att.Filename, &att.MIME, &att.SizeBytes, &att.CreatedAt)
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
		        filename, mime, size_bytes, created_at
		 FROM message_attachments
		 WHERE message_id = ANY($1)
		 ORDER BY message_id, created_at, attachment_id`, ids)
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
			&att.Filename, &att.MIME, &att.SizeBytes, &att.CreatedAt); err != nil {
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
}

// DiskAttachments keeps attachment bytes under Root, sharded by the first two
// byte-pairs of the attachment id so no directory grows unbounded:
// <root>/01/90/0190....bin. Ids are validated UUIDv7 strings, so no path
// component is ever caller-controlled.
type DiskAttachments struct {
	Root string
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
