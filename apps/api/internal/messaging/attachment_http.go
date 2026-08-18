package messaging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

// Upload request headers. The body is the raw file; every piece of metadata
// travels in headers so authorization, receipt lookup, and quota reservation
// finish before any body byte is read.
const (
	// AttachmentUploadNonceHeader carries the application-owned per-file retry
	// identity.
	AttachmentUploadNonceHeader = "Idempotency-Key"
	// AttachmentUploadFilenameHeader carries the RFC 3986 percent-encoded UTF-8
	// display filename. It is never used as a storage path.
	AttachmentUploadFilenameHeader = "X-Sumi-Attachment-Filename"
)

// The public server's global timeouts are intentionally strict for ordinary
// API calls. A legal 20 MiB upload needs a wider but still finite connection
// read/write window, set only for this route after preflight succeeds.
const attachmentUploadTimeout = 130 * time.Second

// After an ambiguous finalize outcome the durable receipt is probed briefly
// from an independent context before the caller gives up.
const (
	attachmentReceiptRecoveryWindow = 500 * time.Millisecond
	attachmentReceiptRecoveryPoll   = 10 * time.Millisecond
)

// inlineImageMIMEs are the only types served with `Content-Disposition:
// inline`. Everything else — including image/svg+xml, which is a scriptable
// document — is delivered as a download.
var inlineImageMIMEs = map[string]bool{
	"image/png":  true,
	"image/jpeg": true,
	"image/gif":  true,
	"image/webp": true,
}

// InlineImageMIME reports whether a stored MIME may render inline.
func InlineImageMIME(value string) bool { return inlineImageMIMEs[value] }

var (
	errAttachmentUploadDeadline = errors.New("attachment upload deadline unavailable")
	errAttachmentLengthRequired = errors.New("attachment upload requires an exact Content-Length")
	errAttachmentFilename       = errors.New("attachment filename header is invalid")
)

type attachmentWire struct {
	AttachmentID string `json:"attachment_id"`
	Filename     string `json:"filename"`
	MIME         string `json:"mime"`
	SizeBytes    int64  `json:"size_bytes"`
	SHA256       string `json:"sha256"`
	Position     int    `json:"position"`
	// Spoiler and Alt are the sender's declarations about the file. They ride
	// on every delivery — REST, WebSocket, and the agent's local control lane
	// — so a PersonalityAgent reading a timeline learns「これはネタバレ画像だ」
	// exactly as a human's screen does.
	Spoiler bool   `json:"spoiler"`
	Alt     string `json:"alt"`
}

type attachmentUploadWire struct {
	Attachment attachmentWire `json:"attachment"`
	Created    bool           `json:"created"`
}

func attachmentToWire(a Attachment) attachmentWire {
	return attachmentWire{
		AttachmentID: a.AttachmentID,
		Filename:     a.Filename,
		MIME:         a.MIME,
		SizeBytes:    a.SizeBytes,
		SHA256:       a.SHA256Hex(),
		Position:     a.Position,
		Spoiler:      a.Spoiler,
		Alt:          a.Alt,
	}
}

func attachmentsToWire(attachments []Attachment) []attachmentWire {
	out := make([]attachmentWire, len(attachments))
	for i, a := range attachments {
		out[i] = attachmentToWire(a)
	}
	return out
}

// attachmentUploadRequest is everything the transport must resolve before the
// store may reserve quota.
type attachmentUploadRequest struct {
	placeID      string
	clientNonce  string
	filename     string
	declaredMIME string
	declaredSize int64
}

func parseAttachmentUploadRequest(r *http.Request, placeID string) (attachmentUploadRequest, error) {
	req := attachmentUploadRequest{placeID: placeID}
	req.clientNonce = r.Header.Get(AttachmentUploadNonceHeader)
	if err := validateAttachmentNonce(req.clientNonce); err != nil {
		return req, err
	}
	// Chunked or unknown lengths cannot be reserved before the body.
	lengths := r.Header.Values("Content-Length")
	if r.ContentLength < 0 || len(lengths) != 1 {
		return req, errAttachmentLengthRequired
	}
	if r.ContentLength == 0 {
		return req, ErrAttachmentEmpty
	}
	if r.ContentLength > MaxAttachmentBytes {
		return req, ErrAttachmentTooLarge
	}
	req.declaredSize = r.ContentLength
	encoded := r.Header.Get(AttachmentUploadFilenameHeader)
	decoded, err := url.PathUnescape(encoded)
	if err != nil || !utf8.ValidString(decoded) {
		return req, errAttachmentFilename
	}
	req.filename = sanitizeAttachmentFilename(decoded)
	req.declaredMIME = r.Header.Get("Content-Type")
	return req, nil
}

func setAttachmentUploadDeadlines(w http.ResponseWriter) error {
	controller := http.NewResponseController(w)
	deadline := time.Now().Add(attachmentUploadTimeout)
	if err := controller.SetReadDeadline(deadline); err != nil {
		return fmt.Errorf("%w: set read deadline: %v", errAttachmentUploadDeadline, err)
	}
	if err := controller.SetWriteDeadline(deadline); err != nil {
		return fmt.Errorf("%w: set write deadline: %v", errAttachmentUploadDeadline, err)
	}
	return nil
}

// attachmentUploadAdmission is a transport-supplied authority boundary around
// the two short metadata phases of an upload. The browser route supplies the
// session admission lease; the PA route supplies the exact runtime-epoch
// admission. Neither is held while body bytes stream.
type attachmentUploadAdmission func(func() error) (admitted bool, err error)

// uploadAttachment is the one upload state machine shared by every transport:
//
//  1. reserve: exact scope, place, nonce receipt, and quota inside admission;
//  2. stage: stream the body to a private, synced staging file without any
//     authority lease held;
//  3. finalize: reacquire admission, revalidate exact scope and the reservation,
//     publish the blob, and record metadata in one transaction.
//
// It returns admitted=false only when the transport's admission refused to
// run the phase (session logout, runtime replacement); in that case nothing
// durable was written by that phase and any staged bytes are discarded.
func uploadAttachment(
	ctx context.Context,
	store *ScopedStore,
	req attachmentUploadRequest,
	admit attachmentUploadAdmission,
	beforeBody func() error,
	body io.Reader,
) (Attachment, bool, bool, error) {
	if store == nil || !store.Store.AttachmentsEnabled() {
		return Attachment{}, false, true, ErrAttachmentsUnavailable
	}
	var receipt AttachmentUploadReceipt
	admitted, err := admit(func() error {
		var reserveErr error
		receipt, reserveErr = store.ReserveAttachmentUpload(ctx, req.placeID, req.clientNonce, req.declaredSize)
		return reserveErr
	})
	if !admitted {
		return Attachment{}, false, false, nil
	}
	if err != nil {
		return Attachment{}, false, true, err
	}
	if receipt.Existing != nil {
		return *receipt.Existing, false, true, nil
	}
	reservation := receipt.Reservation
	if beforeBody != nil {
		if err := beforeBody(); err != nil {
			_ = store.AbandonAttachmentStaging(context.WithoutCancel(ctx), *reservation)
			return Attachment{}, false, true, err
		}
	}
	blob, err := store.Store.blobs.Stage(reservation.UploadID, body, req.declaredSize)
	if err != nil {
		_ = store.AbandonAttachmentStaging(context.WithoutCancel(ctx), *reservation)
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return Attachment{}, false, true, ErrAttachmentTooLarge
		}
		return Attachment{}, false, true, err
	}
	staged := StagedAttachment{
		UploadID:   reservation.UploadID,
		Filename:   req.filename,
		MIME:       resolveAttachmentMIME(req.declaredMIME, blob.Head),
		Size:       blob.Size,
		SHA256:     blob.SHA256,
		StageToken: reservation.StageToken,
		Handle:     blob,
	}
	var (
		attachment Attachment
		created    bool
	)
	finalAdmitted, finalErr := admit(func() error {
		var finalizeErr error
		attachment, created, finalizeErr = store.FinalizeAttachmentUpload(ctx, req.placeID, staged)
		return finalizeErr
	})
	if !finalAdmitted {
		_ = store.Store.blobs.Discard(blob)
		_ = store.AbandonAttachmentStaging(context.WithoutCancel(ctx), *reservation)
		return Attachment{}, false, false, nil
	}
	if finalErr == nil {
		return attachment, created, true, nil
	}
	if AttachmentFinalizeDefinitelyNotCommitted(finalErr) {
		_ = store.AbandonAttachmentStaging(context.WithoutCancel(ctx), *reservation)
		return Attachment{}, false, true, finalErr
	}
	// The commit outcome is unknown. Probe the durable receipt briefly; if it
	// surfaces, the published blob is exactly right. Otherwise the blob stays
	// for the reconciler and the caller sees the ambiguity.
	if recovered, found := recoverAttachmentReceipt(ctx, store, req.placeID, req.clientNonce); found {
		return recovered, recovered.AttachmentID == reservation.UploadID, true, nil
	}
	return Attachment{}, false, true, finalErr
}

func recoverAttachmentReceipt(ctx context.Context, store *ScopedStore, placeID, clientNonce string) (Attachment, bool) {
	recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), attachmentReceiptRecoveryWindow)
	defer cancel()
	for {
		att, found, err := store.AttachmentUploadReceiptByNonce(recoveryCtx, placeID, clientNonce)
		if err == nil && found {
			return att, true
		}
		select {
		case <-recoveryCtx.Done():
			return Attachment{}, false
		case <-time.After(attachmentReceiptRecoveryPoll):
		}
	}
}

// writeAttachmentUploadError maps upload-phase failures to transport codes.
// Anything it does not recognise falls through to writeStoreError.
func writeAttachmentUploadError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errAttachmentUploadDeadline):
		writeError(w, http.StatusServiceUnavailable, "upload_deadline_unavailable")
	case errors.Is(err, errAttachmentLengthRequired):
		writeError(w, http.StatusLengthRequired, "length_required")
	case errors.Is(err, errAttachmentFilename):
		writeError(w, http.StatusBadRequest, "invalid_filename")
	case errors.Is(err, ErrAttachmentNonce):
		writeError(w, http.StatusBadRequest, "invalid_client_nonce")
	case errors.Is(err, ErrAttachmentEmpty):
		writeError(w, http.StatusBadRequest, "attachment_empty")
	case errors.Is(err, ErrAttachmentUploadInProgress):
		writeError(w, http.StatusConflict, "attachment_upload_in_progress")
	default:
		writeStoreError(w, err)
	}
}

// serveUploadAttachment accepts one raw file body and returns its identity.
// The upload is not yet part of any message: it is bound when the uploader
// sends a message listing it.
func (s *Server) serveUploadAttachment(w http.ResponseWriter, r *http.Request) {
	_, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	store := scopedStoreForRequest(r)
	if store == nil || !store.Store.AttachmentsEnabled() {
		writeError(w, http.StatusServiceUnavailable, "attachments_unavailable")
		return
	}
	req, err := parseAttachmentUploadRequest(r, r.PathValue("place_id"))
	if err != nil {
		writeAttachmentUploadError(w, err)
		return
	}
	body := http.MaxBytesReader(w, r.Body, MaxAttachmentBytes)
	att, created, admitted, err := uploadAttachment(
		r.Context(), store, req,
		func(op func() error) (bool, error) {
			return s.authorizeSessionMutation(r.Context(), claims, op)
		},
		func() error { return setAttachmentUploadDeadlines(w) },
		body,
	)
	if err != nil {
		writeAttachmentUploadError(w, err)
		return
	}
	if !admitted {
		writeError(w, http.StatusUnauthorized, "invalid_session")
		return
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
	}
	writeJSON(w, status, attachmentUploadWire{attachmentToWire(att), created})
}

// serveUpdateAttachment edits an upload before it is sent: display name,
// description, and whether it arrives covered. Only the uploader's own
// still-unbound attachment can be edited; after send, what the recipients saw
// stands.
//
// An absent field is「触らない」, the same shape as the notification settings
// lane, so naming one preference never silently resets the others.
func (s *Server) serveUpdateAttachment(w http.ResponseWriter, r *http.Request) {
	_, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	store := scopedStoreForRequest(r)
	if store == nil || !store.Store.AttachmentsEnabled() {
		writeError(w, http.StatusServiceUnavailable, "attachments_unavailable")
		return
	}
	var req struct {
		Filename *string `json:"filename"`
		Alt      *string `json:"alt"`
		Spoiler  *bool   `json:"spoiler"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Filename == nil && req.Alt == nil && req.Spoiler == nil {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if req.Filename != nil {
		name := sanitizeAttachmentFilename(*req.Filename)
		req.Filename = &name
	}
	if req.Alt != nil {
		alt := sanitizeAttachmentAlt(*req.Alt)
		if utf8.RuneCountInString(alt) > MaxAttachmentAltRunes {
			writeError(w, http.StatusBadRequest, "invalid_request")
			return
		}
		req.Alt = &alt
	}
	var att Attachment
	done, err := s.mutate(w, r, claims, func() error {
		var updateErr error
		att, updateErr = store.UpdateDraftAttachment(r.Context(), r.PathValue("attachment_id"),
			AttachmentDraftPatch{Filename: req.Filename, Alt: req.Alt, Spoiler: req.Spoiler})
		return updateErr
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, attachmentToWire(att))
}

// sanitizeAttachmentAlt keeps a one-paragraph description: no control
// characters, newlines included. Length is bounded by the caller afterwards,
// on the sanitized value.
func sanitizeAttachmentAlt(alt string) string {
	return strings.TrimSpace(strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, alt))
}

// authorizeSessionMutation is mutate without transport output: it runs op
// under the session's durable admission lease and reports whether the lease
// was granted at all.
func (s *Server) authorizeSessionMutation(ctx context.Context, claims agentevents.UserSessionClaims, op func() error) (bool, error) {
	called := false
	err := s.Sessions.AuthorizeSession(ctx, claims, func() error {
		called = true
		return op()
	})
	if !called {
		return false, nil
	}
	return true, err
}

// serveAttachment delivers the bytes to a viewer the store says may read
// them. Every response is nosniff, sandboxed, and uncacheable; only known-safe
// image types render inline, so an uploaded document can never execute in the
// app's origin.
func (s *Server) serveAttachment(w http.ResponseWriter, r *http.Request) {
	_, _, ok := s.viewer(w, r)
	if !ok {
		return
	}
	store := scopedStoreForRequest(r)
	if store == nil || !store.Store.AttachmentsEnabled() {
		writeError(w, http.StatusServiceUnavailable, "attachments_unavailable")
		return
	}
	att, err := store.AttachmentForViewer(r.Context(), r.PathValue("attachment_id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	blob, err := store.Store.blobs.Open(att.AttachmentID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	defer blob.Close()
	writeAttachmentHeaders(w.Header(), att)
	http.ServeContent(w, r, "", att.CreatedAt, blob)
}

// writeAttachmentHeaders sets the safe delivery headers shared by every byte
// response.
func writeAttachmentHeaders(header http.Header, att Attachment) {
	disposition := "attachment"
	contentType := "application/octet-stream"
	if inlineImageMIMEs[att.MIME] {
		disposition = "inline"
		contentType = att.MIME
	}
	header.Set("Content-Type", contentType)
	header.Set("Content-Disposition",
		mime.FormatMediaType(disposition, map[string]string{"filename": att.Filename}))
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("Content-Security-Policy", "default-src 'none'; sandbox")
	header.Set("Cross-Origin-Resource-Policy", "same-origin")
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("Cache-Control", "private, no-store")
	header.Set("X-Sumi-Attachment-Id", att.AttachmentID)
	header.Set("X-Sumi-Attachment-Mime", att.MIME)
	header.Set("X-Sumi-Attachment-Size", strconv.FormatInt(att.SizeBytes, 10))
	header.Set("X-Sumi-Attachment-Sha256", att.SHA256Hex())
	header.Set(AttachmentUploadFilenameHeader, url.PathEscape(att.Filename))
}

// sanitizeAttachmentFilename keeps a display name only: no directories, no
// control characters, bounded length. It is never used as a storage path.
func sanitizeAttachmentFilename(name string) string {
	name = strings.ReplaceAll(name, "\\", "/")
	name = path.Base(strings.TrimSpace(name))
	name = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, name)
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == ".." || name == "/" {
		return "file"
	}
	for len(name) > MaxAttachmentFilenameBytes {
		_, size := utf8.DecodeLastRuneInString(name)
		name = name[:len(name)-size]
	}
	if name == "" {
		return "file"
	}
	return name
}

// resolveAttachmentMIME decides the stored type from the bytes first and the
// client's claim second. Bytes that sniff as a supported image are that image;
// a claimed image whose bytes disagree is demoted to an opaque download, so a
// document can never be delivered under an inline image type.
func resolveAttachmentMIME(declared string, head []byte) string {
	sniffed := normalizeMediaType(http.DetectContentType(head))
	if inlineImageMIMEs[sniffed] {
		return sniffed
	}
	claimed := normalizeMediaType(declared)
	if claimed == "" || inlineImageMIMEs[claimed] || strings.HasPrefix(claimed, "image/") {
		return "application/octet-stream"
	}
	return claimed
}

func normalizeMediaType(value string) string {
	parsed, _, err := mime.ParseMediaType(strings.TrimSpace(value))
	if err != nil {
		return ""
	}
	parsed = strings.ToLower(parsed)
	if len(parsed) > 255 {
		return ""
	}
	return parsed
}
