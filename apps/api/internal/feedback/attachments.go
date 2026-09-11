package feedback

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"path"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

const MaxAttachmentBytes = 20 << 20
const MaxAttachments = 5
const maxAttachmentStorageBytes = 1 << 30
const maxAttachmentTransfers = 4

var ErrAttachmentTooLarge = errors.New("attachment_too_large")
var ErrAttachmentType = errors.New("unsupported_attachment_type")
var ErrAttachmentQuota = errors.New("attachment_quota_exceeded")
var ErrAttachmentBusy = errors.New("attachment_transfer_busy")

// Share one bound across incoming and outgoing media before either operation
// materializes bytes. Admission is immediate; excess callers retain no body or
// database blob in an application queue. Each Server owns its own capacity.
func (s *Server) acquireAttachmentTransfer() (func(), error) {
	s.attachmentTransferOnce.Do(func() { s.attachmentTransfers = make(chan struct{}, maxAttachmentTransfers) })
	select {
	case s.attachmentTransfers <- struct{}{}:
		return func() { <-s.attachmentTransfers }, nil
	default:
		return nil, ErrAttachmentBusy
	}
}

type Attachment struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	MIMEType string `json:"mime_type"`
	Size     int    `json:"size"`
	URL      string `json:"url"`
}

func attachmentMIME(data []byte) string {
	switch value := http.DetectContentType(data); value {
	case "image/png", "image/jpeg", "image/webp", "video/webm", "video/mp4":
		return value
	default:
		return ""
	}
}

func attachmentName(name string) string {
	name = path.Base(strings.ReplaceAll(name, "\\", "/"))
	name = strings.TrimSpace(strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || r == '\u2028' || r == '\u2029' {
			return -1
		}
		return r
	}, name))
	for len(name) > 255 {
		_, n := utf8.DecodeLastRuneInString(name)
		name = name[:len(name)-n]
	}
	if name == "" || name == "." || name == "/" {
		return "attachment"
	}
	return name
}

// CleanupAttachments reclaims only expired, unbound Feedback uploads.
func (s *Store) CleanupAttachments(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM feedback_attachments WHERE thread_id IS NULL AND expires_at<=now()`)
	return err
}

func (s *Store) UploadAttachment(ctx context.Context, actor participant.Ref, name string, data []byte) (Attachment, error) {
	var result Attachment
	if len(data) > MaxAttachmentBytes {
		return result, ErrAttachmentTooLarge
	}
	if len(data) == 0 {
		return result, ErrInvalid
	}
	mediaType := attachmentMIME(data)
	if mediaType == "" {
		return result, ErrAttachmentType
	}
	tx, err := s.begin(ctx, actor)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	// Serialize quota reservation and insertion, including concurrent authors.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('feedback-attachment-quota',0))`); err != nil {
		return result, err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM feedback_attachments WHERE thread_id IS NULL AND expires_at<=now()`); err != nil {
		return result, err
	}
	var total, staged, objects, stagedObjects int64
	err = tx.QueryRow(ctx, `SELECT COALESCE(sum(octet_length(content)),0),COALESCE(sum(octet_length(content)) FILTER (WHERE author_key=$1 AND thread_id IS NULL),0),count(*),count(*) FILTER (WHERE author_key=$1 AND thread_id IS NULL) FROM feedback_attachments`, actor.Key()).Scan(&total, &staged, &objects, &stagedObjects)
	if err != nil {
		return result, err
	}
	if total+int64(len(data)) > maxAttachmentStorageBytes || staged+int64(len(data)) > MaxAttachmentBytes*MaxAttachments || objects >= 10000 || stagedObjects >= 20 {
		return result, ErrAttachmentQuota
	}
	result = Attachment{ID: newID(), Name: attachmentName(name), MIMEType: mediaType, Size: len(data)}
	result.URL = "/feedback/attachments/" + result.ID
	_, err = tx.Exec(ctx, `INSERT INTO feedback_attachments(attachment_id,author_key,name,mime_type,content) VALUES($1,$2,$3,$4,$5)`, result.ID, actor.Key(), result.Name, result.MIMEType, data)
	if err != nil {
		return Attachment{}, err
	}
	return result, tx.Commit(ctx)
}

func validAttachmentIDs(ids []string) bool {
	if len(ids) > MaxAttachments {
		return false
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if !validID(id, 7) || seen[id] {
			return false
		}
		seen[id] = true
	}
	return true
}

func bindAttachments(ctx context.Context, tx pgx.Tx, actor participant.Ref, threadID string, ids []string) ([]Attachment, error) {
	result := []Attachment{}
	for index, id := range ids {
		var a Attachment
		err := tx.QueryRow(ctx, `UPDATE feedback_attachments SET thread_id=$3,position=$4 WHERE attachment_id=$1 AND author_key=$2 AND thread_id IS NULL AND expires_at>now() RETURNING attachment_id,name,mime_type,octet_length(content)`, id, actor.Key(), threadID, index).Scan(&a.ID, &a.Name, &a.MIMEType, &a.Size)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		if err != nil {
			return nil, err
		}
		a.URL = "/feedback/attachments/" + a.ID
		result = append(result, a)
	}
	return result, nil
}

func threadAttachments(ctx context.Context, tx pgx.Tx, threadID string) ([]Attachment, error) {
	result := []Attachment{}
	rows, err := tx.Query(ctx, `SELECT attachment_id,name,mime_type,octet_length(content) FROM feedback_attachments WHERE thread_id=$1 ORDER BY position`, threadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var a Attachment
		if err = rows.Scan(&a.ID, &a.Name, &a.MIMEType, &a.Size); err != nil {
			return nil, err
		}
		a.URL = "/feedback/attachments/" + a.ID
		result = append(result, a)
	}
	return result, rows.Err()
}

func (s *Store) ReadAttachment(ctx context.Context, actor participant.Ref, id string) (Attachment, []byte, error) {
	var a Attachment
	if !validID(id, 7) {
		return a, nil, ErrNotFound
	}
	tx, err := s.begin(ctx, actor)
	if err != nil {
		return a, nil, err
	}
	defer tx.Rollback(ctx)
	var data []byte
	err = tx.QueryRow(ctx, `SELECT a.attachment_id,a.name,a.mime_type,octet_length(a.content),a.content FROM feedback_attachments a LEFT JOIN feedback_threads t ON t.thread_id=a.thread_id WHERE a.attachment_id=$1 AND ((a.thread_id IS NULL AND a.author_key=$2 AND a.expires_at>now()) OR (a.thread_id IS NOT NULL AND (t.author_key=$2 OR $3)))`, id, actor.Key(), s.isRecipient(actor)).Scan(&a.ID, &a.Name, &a.MIMEType, &a.Size, &data)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, nil, ErrNotFound
	}
	if err != nil {
		return a, nil, err
	}
	a.URL = "/feedback/attachments/" + a.ID
	return a, data, tx.Commit(ctx)
}

func (s *Server) uploadAttachment(w http.ResponseWriter, r *http.Request, actor participant.Ref) (any, error) {
	// Reject disabled/uninstalled authors before reading a potentially large
	// body. UploadAttachment rechecks under its write transaction after the
	// transfer, so this early check does not outlive installation authority.
	tx, err := s.Store.begin(r.Context(), actor)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(r.Context()); err != nil {
		return nil, err
	}
	r.Body = http.MaxBytesReader(w, r.Body, MaxAttachmentBytes+(64<<10))
	reader, err := r.MultipartReader()
	if err != nil {
		return nil, ErrInvalid
	}
	part, err := reader.NextPart()
	if err != nil || part.FormName() != "file" || part.FileName() == "" {
		return nil, ErrInvalid
	}
	data, err := io.ReadAll(io.LimitReader(part, MaxAttachmentBytes+1))
	if len(data) > MaxAttachmentBytes {
		return nil, ErrAttachmentTooLarge
	}
	if err != nil {
		return nil, ErrInvalid
	}
	if _, err = reader.NextPart(); err != io.EOF {
		return nil, ErrInvalid
	}
	a, err := s.Store.UploadAttachment(r.Context(), actor, part.FileName(), data)
	return map[string]Attachment{"attachment": a}, err
}

func (s *Server) downloadAttachment(w http.ResponseWriter, r *http.Request, actor participant.Ref) (any, error) {
	a, data, err := s.Store.ReadAttachment(r.Context(), actor, r.PathValue("attachment_id"))
	if err != nil {
		return nil, err
	}
	w.Header().Set("Content-Type", a.MIMEType)
	w.Header().Set("Content-Disposition", mime.FormatMediaType("inline", map[string]string{"filename": a.Name}))
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; sandbox")
	w.Header().Set("Cross-Origin-Resource-Policy", "same-origin")
	w.Header().Set("Referrer-Policy", "no-referrer")
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(data))
	return nil, nil
}
