package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

// pngBytes is a byte string http.DetectContentType classifies as image/png:
// the PNG signature is all the sniffer reads.
var pngBytes = append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte("pixel"), 8)...)

func newAttachmentServer(t *testing.T, ctx context.Context) (world, *httptest.Server) {
	w, _, ts := newAttachmentServerWithServer(t, ctx)
	return w, ts
}

func newAttachmentServerWithServer(t *testing.T, ctx context.Context) (world, *Server, *httptest.Server) {
	t.Helper()
	w := newWorld(t, ctx)
	blobs, err := NewDiskAttachments(t.TempDir())
	if err != nil {
		t.Fatalf("disk attachments: %v", err)
	}
	server := NewServer(w.store, stubSessions{})
	server.AllowedOrigins = []string{testOrigin}
	server.Attachments = blobs
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return w, server, ts
}

// upload posts one multipart file as the given participant.
func upload(t *testing.T, ts *httptest.Server, cookie, filename, contentType string, content []byte) (*http.Response, map[string]any) {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	header := make(map[string][]string)
	header["Content-Disposition"] = []string{
		fmt.Sprintf(`form-data; name="file"; filename=%q`, filename),
	}
	header["Content-Type"] = []string{contentType}
	part, err := writer.CreatePart(header)
	if err != nil {
		t.Fatalf("create part: %v", err)
	}
	if _, err := part.Write(content); err != nil {
		t.Fatalf("write part: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/messaging/attachments", &body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Origin", testOrigin)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: cookie})
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	var decoded map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&decoded)
	return resp, decoded
}

// fetchAttachment issues the authenticated download as the given participant.
func fetchAttachment(t *testing.T, ts *httptest.Server, cookie, attachmentID string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/messaging/attachments/"+attachmentID, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if cookie != "" {
		req.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: cookie})
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp, payload
}

func attachmentID(t *testing.T, body map[string]any) string {
	t.Helper()
	id, _ := body["attachment_id"].(string)
	if id == "" {
		t.Fatalf("upload response carries no attachment_id: %v", body)
	}
	return id
}

// TestAttachmentUploadBindAndVisibility walks the whole life of an upload: it
// is private to its uploader while unbound, only its uploader may bind it to a
// message, and once bound it inherits the message place's visibility.
func TestAttachmentUploadBindAndVisibility(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w, ts := newAttachmentServer(t, ctx)

	// A workspace both humans share, and one only humanA can see.
	shared, sharedChannel := w.workspaceWithChannel(t, ctx)
	_ = shared
	private, err := w.store.CreateWorkspace(ctx, "private", w.humanA)
	if err != nil {
		t.Fatalf("create private workspace: %v", err)
	}
	privateChannel, err := w.store.CreateChannel(ctx, private.WorkspaceID, "solo", "", w.humanA, false)
	if err != nil {
		t.Fatalf("create private channel: %v", err)
	}

	resp, body := upload(t, ts, w.humanA.ID, "shot.png", "image/png", pngBytes)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("upload status = %d, want 201 (%v)", resp.StatusCode, body)
	}
	if body["mime"] != "image/png" {
		t.Fatalf("stored mime = %v, want image/png", body["mime"])
	}
	if body["filename"] != "shot.png" {
		t.Fatalf("filename = %v, want shot.png", body["filename"])
	}
	if size, _ := body["size"].(float64); int(size) != len(pngBytes) {
		t.Fatalf("size = %v, want %d", body["size"], len(pngBytes))
	}
	id := attachmentID(t, body)

	// Unbound: the uploader may read it, nobody else may learn it exists.
	if resp, _ := fetchAttachment(t, ts, w.humanA.ID, id); resp.StatusCode != http.StatusOK {
		t.Fatalf("uploader fetch of unbound attachment = %d, want 200", resp.StatusCode)
	}
	if resp, _ := fetchAttachment(t, ts, w.humanB.ID, id); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("stranger fetch of unbound attachment = %d, want 404", resp.StatusCode)
	}
	if resp, _ := fetchAttachment(t, ts, "", id); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous fetch = %d, want 401", resp.StatusCode)
	}

	// Only the uploader may bind it: humanB cannot send humanA's upload.
	resp, body = call(t, ts, http.MethodPost,
		"/messaging/places/"+sharedChannel.PlaceID+"/messages", w.humanB.ID,
		map[string]any{"content": "stolen", "client_nonce": "n-steal", "attachments": []string{id}})
	if resp.StatusCode != http.StatusNotFound || body["error"] != "attachment_not_found" {
		t.Fatalf("binding another participant's attachment = %d %v, want 404 attachment_not_found",
			resp.StatusCode, body)
	}

	// The uploader sends it into the place only they can see.
	resp, body = call(t, ts, http.MethodPost,
		"/messaging/places/"+privateChannel.PlaceID+"/messages", w.humanA.ID,
		map[string]any{"content": "", "client_nonce": "n-1", "attachments": []string{id}})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("send with attachment = %d %v, want 201", resp.StatusCode, body)
	}
	history, err := w.store.History(ctx, privateChannel.PlaceID, w.humanA, HistoryOptions{})
	if err != nil || len(history) != 1 || len(history[0].Attachments) != 1 {
		t.Fatalf("stored attachment message = %#v, err %v", history, err)
	}
	first := history[0].Attachments[0]
	if first.AttachmentID != id || first.MIME != "image/png" {
		t.Fatalf("stored attachment = %#v", first)
	}

	// Bound: readable by the place's members only.
	resp, payload := fetchAttachment(t, ts, w.humanA.ID, id)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("member fetch = %d, want 200", resp.StatusCode)
	}
	if !bytes.Equal(payload, pngBytes) {
		t.Fatalf("served bytes differ from the uploaded bytes")
	}
	if got := resp.Header.Get("Content-Type"); got != "image/png" {
		t.Fatalf("Content-Type = %q, want image/png", got)
	}
	if got := resp.Header.Get("Content-Disposition"); !strings.HasPrefix(got, "inline") {
		t.Fatalf("Content-Disposition = %q, want inline for an image", got)
	}
	if got := resp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("X-Content-Type-Options = %q, want nosniff", got)
	}
	if got := resp.Header.Get("Content-Security-Policy"); got == "" {
		t.Fatalf("attachment response carries no Content-Security-Policy")
	}
	if resp, _ := fetchAttachment(t, ts, w.humanB.ID, id); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("non-member fetch of a bound attachment = %d, want 404", resp.StatusCode)
	}

	// The same attachment cannot be sent twice.
	resp, body = call(t, ts, http.MethodPost,
		"/messaging/places/"+privateChannel.PlaceID+"/messages", w.humanA.ID,
		map[string]any{"content": "again", "client_nonce": "n-2", "attachments": []string{id}})
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("rebinding a sent attachment = %d %v, want 404", resp.StatusCode, body)
	}

	// History carries the attachment for a viewer who can see the place.
	resp, body = call(t, ts, http.MethodGet,
		"/messaging/places/"+privateChannel.PlaceID+"/messages", w.humanA.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("history = %d, want 200", resp.StatusCode)
	}
	messages, _ := body["messages"].([]any)
	if len(messages) != 1 {
		t.Fatalf("history carries %d messages, want 1", len(messages))
	}
	historic, _ := messages[0].(map[string]any)
	if got, _ := historic["attachments"].([]any); len(got) != 1 {
		t.Fatalf("history message carries %d attachments, want 1", len(got))
	}
}

// TestAttachmentDeletedMessageStopsServing shows the tombstone rule: deleting a
// message stops delivering the bytes it carried.
func TestAttachmentDeletedMessageStopsServing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w, ts := newAttachmentServer(t, ctx)
	_, channel := w.workspaceWithChannel(t, ctx)

	_, body := upload(t, ts, w.humanA.ID, "shot.png", "image/png", pngBytes)
	id := attachmentID(t, body)
	resp, body := call(t, ts, http.MethodPost,
		"/messaging/places/"+channel.PlaceID+"/messages", w.humanA.ID,
		map[string]any{"content": "見て", "client_nonce": "n-1", "attachments": []string{id}})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("send = %d %v, want 201", resp.StatusCode, body)
	}
	messageID, _ := body["message_id"].(string)
	if resp, _ := fetchAttachment(t, ts, w.humanB.ID, id); resp.StatusCode != http.StatusOK {
		t.Fatalf("member fetch before delete = %d, want 200", resp.StatusCode)
	}

	resp, _ = call(t, ts, http.MethodDelete,
		"/messaging/places/"+channel.PlaceID+"/messages/"+messageID, w.humanA.ID, nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete = %d, want 204", resp.StatusCode)
	}
	for _, cookie := range []string{w.humanA.ID, w.humanB.ID} {
		if resp, _ := fetchAttachment(t, ts, cookie, id); resp.StatusCode != http.StatusNotFound {
			t.Fatalf("fetch after delete as %s = %d, want 404", cookie, resp.StatusCode)
		}
	}
}

// TestAttachmentUploadLimits covers the wire bounds and the type rules: a file
// over the size limit is refused, a non-image is a download, and bytes that
// disagree with a claimed image type are demoted to an opaque download.
func TestAttachmentUploadLimits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w, ts := newAttachmentServer(t, ctx)

	oversize := bytes.Repeat([]byte("A"), int(MaxAttachmentBytes)+1)
	resp, body := upload(t, ts, w.humanA.ID, "big.bin", "application/octet-stream", oversize)
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize upload = %d %v, want 413", resp.StatusCode, body)
	}

	resp, body = upload(t, ts, w.humanA.ID, "notes.pdf", "application/pdf", []byte("%PDF-1.7 body"))
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("pdf upload = %d %v, want 201", resp.StatusCode, body)
	}
	resp, _ = fetchAttachment(t, ts, w.humanA.ID, attachmentID(t, body))
	if got := resp.Header.Get("Content-Disposition"); !strings.HasPrefix(got, "attachment") {
		t.Fatalf("Content-Disposition = %q, want attachment for a non-image", got)
	}
	if got := resp.Header.Get("Content-Type"); got != "application/octet-stream" {
		t.Fatalf("Content-Type = %q, want application/octet-stream", got)
	}
	if got := resp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("X-Content-Type-Options = %q, want nosniff", got)
	}

	// A document claiming to be an image must never come back as one.
	resp, body = upload(t, ts, w.humanA.ID, "../../evil.png", "image/png",
		[]byte("<html><script>alert(1)</script></html>"))
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("spoofed image upload = %d %v, want 201", resp.StatusCode, body)
	}
	if body["mime"] != "application/octet-stream" {
		t.Fatalf("spoofed image stored as %v, want application/octet-stream", body["mime"])
	}
	if body["filename"] != "evil.png" {
		t.Fatalf("filename = %v, want the path stripped to evil.png", body["filename"])
	}
	resp, _ = fetchAttachment(t, ts, w.humanA.ID, attachmentID(t, body))
	if got := resp.Header.Get("Content-Disposition"); !strings.HasPrefix(got, "attachment") {
		t.Fatalf("Content-Disposition = %q, want attachment for spoofed bytes", got)
	}

	// A message with neither content nor attachments stays refused.
	_, channel := w.workspaceWithChannel(t, ctx)
	resp, body = call(t, ts, http.MethodPost,
		"/messaging/places/"+channel.PlaceID+"/messages", w.humanA.ID,
		map[string]any{"content": "", "client_nonce": "n-empty"})
	if resp.StatusCode != http.StatusBadRequest || body["error"] != "invalid_content" {
		t.Fatalf("empty send = %d %v, want 400 invalid_content", resp.StatusCode, body)
	}
}

// TestAttachmentDraftSpoilerAndAlt covers the window in which「送る前」の編集
// is allowed: the uploader's own still-unbound attachment takes a new display
// name, a description and the spoiler flag, an absent field is left alone, and
// the moment the attachment becomes part of a message the edit is refused.
// Both declarations then travel with every delivery of that message.
func TestAttachmentDraftSpoilerAndAlt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w, ts := newAttachmentServer(t, ctx)
	_, channel := w.workspaceWithChannel(t, ctx)

	resp, body := upload(t, ts, w.humanA.ID, "shot.png", "image/png", pngBytes)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("upload = %d %v, want 201", resp.StatusCode, body)
	}
	// A fresh upload hides nothing and describes nothing.
	if body["spoiler"] != false || body["alt"] != "" {
		t.Fatalf("fresh upload = %v, want spoiler false and empty alt", body)
	}
	id := attachmentID(t, body)
	path := "/messaging/attachments/" + id

	// Nobody else can edit it — and learns nothing about its existence.
	resp, body = call(t, ts, http.MethodPatch, path, w.humanB.ID,
		map[string]any{"spoiler": true})
	if resp.StatusCode != http.StatusNotFound || body["error"] != "attachment_not_found" {
		t.Fatalf("stranger patch = %d %v, want 404 attachment_not_found", resp.StatusCode, body)
	}

	// The uploader marks it as a spoiler and describes it.
	resp, body = call(t, ts, http.MethodPatch, path, w.humanA.ID,
		map[string]any{"spoiler": true, "alt": "結末の一枚"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("patch = %d %v, want 200", resp.StatusCode, body)
	}
	if body["spoiler"] != true || body["alt"] != "結末の一枚" {
		t.Fatalf("patched attachment = %v", body)
	}
	if body["filename"] != "shot.png" {
		t.Fatalf("filename changed without being named: %v", body)
	}

	// An absent field is「触らない」: renaming keeps the spoiler and the alt.
	resp, body = call(t, ts, http.MethodPatch, path, w.humanA.ID,
		map[string]any{"filename": "ending.png"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("rename = %d %v, want 200", resp.StatusCode, body)
	}
	if body["filename"] != "ending.png" || body["spoiler"] != true || body["alt"] != "結末の一枚" {
		t.Fatalf("rename dropped the other fields: %v", body)
	}

	// Naming nothing at all is not an edit.
	resp, body = call(t, ts, http.MethodPatch, path, w.humanA.ID, map[string]any{})
	if resp.StatusCode != http.StatusBadRequest || body["error"] != "invalid_request" {
		t.Fatalf("empty patch = %d %v, want 400 invalid_request", resp.StatusCode, body)
	}

	// Sent: the declarations ride along with the message.
	resp, body = call(t, ts, http.MethodPost,
		"/messaging/places/"+channel.PlaceID+"/messages", w.humanA.ID,
		map[string]any{"content": "見る?", "client_nonce": "n-spoiler", "attachments": []string{id}})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("send = %d %v, want 201", resp.StatusCode, body)
	}
	history, err := w.store.History(ctx, channel.PlaceID, w.humanA, HistoryOptions{})
	if err != nil || len(history) != 1 || len(history[0].Attachments) != 1 {
		t.Fatalf("stored declaration message = %#v, err %v", history, err)
	}
	sent := history[0].Attachments[0]
	if !sent.Spoiler || sent.Alt != "結末の一枚" || sent.Filename != "ending.png" {
		t.Fatalf("stored attachment declarations = %#v", sent)
	}

	// The recipient's own read of the timeline carries them too.
	resp, body = call(t, ts, http.MethodGet,
		"/messaging/places/"+channel.PlaceID+"/messages", w.humanB.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("history = %d %v, want 200", resp.StatusCode, body)
	}
	messages, _ := body["messages"].([]any)
	if len(messages) == 0 {
		t.Fatalf("history carries no messages: %v", body)
	}
	received, _ := messages[len(messages)-1].(map[string]any)
	receivedAttachments, _ := received["attachments"].([]any)
	if len(receivedAttachments) != 1 {
		t.Fatalf("received message carries %d attachments, want 1", len(receivedAttachments))
	}
	seen, _ := receivedAttachments[0].(map[string]any)
	if seen["spoiler"] != true || seen["alt"] != "結末の一枚" {
		t.Fatalf("received attachment wire = %v", seen)
	}

	// After send the edit window is closed. What was seen was seen.
	resp, body = call(t, ts, http.MethodPatch, path, w.humanA.ID,
		map[string]any{"spoiler": false})
	if resp.StatusCode != http.StatusConflict || body["error"] != "attachment_already_sent" {
		t.Fatalf("patch after send = %d %v, want 409 attachment_already_sent", resp.StatusCode, body)
	}
}

func TestActiveAttachmentDraftLeaseRenewsBeforeReclamation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w, server, ts := newAttachmentServerWithServer(t, ctx)
	blobs := server.Attachments.(*DiskAttachments)

	_, body := upload(t, ts, w.humanA.ID, "long-lived.png", "image/png", pngBytes)
	id := attachmentID(t, body)
	_, foreignBody := upload(t, ts, w.humanB.ID, "foreign.png", "image/png", pngBytes)
	foreignID := attachmentID(t, foreignBody)
	aged := time.Now().Add(-48 * time.Hour)
	backdateAttachment(t, ctx, w, blobs, id, aged)

	resp, body := call(t, ts, http.MethodPost, "/messaging/attachments:renew", w.humanB.ID,
		map[string]any{"attachment_ids": []string{id}})
	if resp.StatusCode != http.StatusNotFound || body["error"] != "attachment_not_found" {
		t.Fatalf("foreign renewal = %d %v, want 404 attachment_not_found", resp.StatusCode, body)
	}
	resp, body = call(t, ts, http.MethodPost, "/messaging/attachments:renew", w.humanA.ID,
		map[string]any{"attachment_ids": []string{id, foreignID}})
	if resp.StatusCode != http.StatusNotFound || body["error"] != "attachment_not_found" {
		t.Fatalf("mixed renewal = %d %v, want atomic 404", resp.StatusCode, body)
	}
	var stillExpired time.Time
	if err := w.store.pool.QueryRow(ctx,
		`SELECT draft_expires_at FROM message_attachments WHERE attachment_id = $1`, id).
		Scan(&stillExpired); err != nil {
		t.Fatalf("load draft after rejected mixed renewal: %v", err)
	}
	if stillExpired.Sub(aged).Abs() > time.Millisecond {
		t.Fatalf("mixed renewal partially changed owned draft: got %s, want %s", stillExpired, aged)
	}

	before := time.Now()
	resp, body = call(t, ts, http.MethodPost, "/messaging/attachments:renew", w.humanA.ID,
		map[string]any{"attachment_ids": []string{id}})
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("owner renewal = %d %v, want 204", resp.StatusCode, body)
	}
	var expiresAt time.Time
	if err := w.store.pool.QueryRow(ctx,
		`SELECT draft_expires_at FROM message_attachments WHERE attachment_id = $1`, id).
		Scan(&expiresAt); err != nil {
		t.Fatalf("load renewed draft lease: %v", err)
	}
	if expiresAt.Before(before.Add(AttachmentDraftLease - time.Minute)) {
		t.Fatalf("renewed draft expires at %s, want roughly one full lease after %s", expiresAt, before)
	}

	swept, err := server.SweepAttachments(ctx, AttachmentOrphanGrace)
	if err != nil || swept.Expired != 0 || !blobExists(t, blobs, id) {
		t.Fatalf("active draft sweep = %+v, error %v, blob present %v", swept, err, blobExists(t, blobs, id))
	}

	if _, err := w.store.pool.Exec(ctx,
		`UPDATE message_attachments SET draft_expires_at = $1 WHERE attachment_id = $2`, aged, id); err != nil {
		t.Fatalf("expire renewed draft: %v", err)
	}
	swept, err = server.SweepAttachments(ctx, AttachmentOrphanGrace)
	if err != nil || swept.Expired != 1 || blobExists(t, blobs, id) {
		t.Fatalf("expired draft sweep = %+v, error %v, blob present %v", swept, err, blobExists(t, blobs, id))
	}
}

func TestAttachmentDraftRenewalSerializesWithReclamation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w, server, ts := newAttachmentServerWithServer(t, ctx)
	blobs := server.Attachments.(*DiskAttachments)

	_, body := upload(t, ts, w.humanA.ID, "renew-race.png", "image/png", pngBytes)
	id := attachmentID(t, body)
	backdateAttachment(t, ctx, w, blobs, id, time.Now().Add(-48*time.Hour))

	const gateKey int32 = 24502
	if _, err := w.store.pool.Exec(ctx, `
		CREATE FUNCTION gate_attachment_draft_renew() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.draft_expires_at > OLD.draft_expires_at THEN
				PERFORM pg_advisory_xact_lock(24502);
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER gate_attachment_draft_renew
		BEFORE UPDATE OF draft_expires_at ON message_attachments
		FOR EACH ROW EXECUTE FUNCTION gate_attachment_draft_renew()`); err != nil {
		t.Fatalf("install draft renewal gate: %v", err)
	}
	release := holdProfileTestGate(t, ctx, w, gateKey)

	renewDone := make(chan error, 1)
	go func() {
		renewDone <- w.store.RenewDraftAttachments(ctx, w.humanA, []string{id}, time.Now())
	}()
	waitForProfileTestGate(t, ctx, w, gateKey)

	type sweepResult struct {
		sweep AttachmentSweep
		err   error
	}
	sweepDone := make(chan sweepResult, 1)
	go func() {
		sweep, err := server.SweepAttachments(ctx, AttachmentOrphanGrace)
		sweepDone <- sweepResult{sweep: sweep, err: err}
	}()

	deadline := time.Now().Add(5 * time.Second)
	waiting := false
	for !waiting && time.Now().Before(deadline) {
		if err := w.store.pool.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_locks l
			JOIN pg_stat_activity a ON a.pid = l.pid
			WHERE a.datname = current_database() AND NOT l.granted
			  AND l.locktype = 'transactionid'
		)`).Scan(&waiting); err != nil {
			t.Fatalf("inspect sweep/renew waiter: %v", err)
		}
		if !waiting {
			time.Sleep(5 * time.Millisecond)
		}
	}
	if !waiting {
		t.Fatal("attachment sweep did not wait for the in-flight draft renewal")
	}

	release()
	if err := <-renewDone; err != nil {
		t.Fatalf("renew draft: %v", err)
	}
	result := <-sweepDone
	if result.err != nil || result.sweep != (AttachmentSweep{}) {
		t.Fatalf("racing renewal sweep = %+v, error %v, want renewed draft preserved",
			result.sweep, result.err)
	}
	if !blobExists(t, blobs, id) {
		t.Fatal("racing sweep removed the renewed draft blob")
	}
}

func TestSanitizeAttachmentAlt(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"  結末の一枚  ", "結末の一枚"},
		{"二行の\n説明", "二行の 説明"},
		{"tab\tここ", "tab ここ"},
		{"", ""},
	} {
		if got := sanitizeAttachmentAlt(tc.in); got != tc.want {
			t.Fatalf("sanitizeAttachmentAlt(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestAttachmentSendOrderPersistsTheSenderSelection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w, ts := newAttachmentServer(t, ctx)
	_, channel := w.workspaceWithChannel(t, ctx)

	// Upload completion order is deliberately the reverse of send selection.
	byName := make(map[string]string, 3)
	for _, name := range []string{"third.png", "second.png", "first.png"} {
		resp, body := upload(t, ts, w.humanA.ID, name, "image/png", pngBytes)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("upload %s = %d %v, want 201", name, resp.StatusCode, body)
		}
		byName[name] = attachmentID(t, body)
	}
	want := []string{"first.png", "second.png", "third.png"}
	ids := make([]string, len(want))
	for i, name := range want {
		ids[i] = byName[name]
	}

	resp, receipt := call(t, ts, http.MethodPost,
		"/messaging/places/"+channel.PlaceID+"/messages", w.humanA.ID,
		map[string]any{"content": "three shots", "client_nonce": "n-order", "attachments": ids})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("send = %d %v, want 201", resp.StatusCode, receipt)
	}
	if _, hasMessage := receipt["message"]; hasMessage {
		t.Fatalf("send returned a full message instead of the compact receipt: %v", receipt)
	}

	history, err := w.store.History(ctx, channel.PlaceID, w.humanA, HistoryOptions{})
	if err != nil || len(history) != 1 {
		t.Fatalf("stored history = %#v, error %v", history, err)
	}
	got := make([]string, len(history[0].Attachments))
	for i, attachment := range history[0].Attachments {
		got[i] = attachment.Filename
	}
	if len(got) != len(want) {
		t.Fatalf("stored attachment order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("stored attachment order = %v, want %v", got, want)
		}
	}
}

func backdateAttachment(t *testing.T, ctx context.Context, w world, blobs *DiskAttachments, id string, when time.Time) {
	t.Helper()
	if _, err := w.store.pool.Exec(ctx,
		`UPDATE message_attachments
		 SET created_at = $1, draft_expires_at = $1
		 WHERE attachment_id = $2`, when, id); err != nil {
		t.Fatalf("backdate attachment row %s: %v", id, err)
	}
	backdateBlob(t, blobs, id, when)
}

func backdateBlob(t *testing.T, blobs *DiskAttachments, id string, when time.Time) {
	t.Helper()
	path, err := blobs.path(id)
	if err != nil {
		t.Fatalf("blob path %s: %v", id, err)
	}
	if err := os.Chtimes(path, when, when); err != nil {
		t.Fatalf("backdate blob %s: %v", id, err)
	}
}

func blobExists(t *testing.T, blobs *DiskAttachments, id string) bool {
	t.Helper()
	path, err := blobs.path(id)
	if err != nil {
		t.Fatalf("blob path %s: %v", id, err)
	}
	_, err = os.Stat(path)
	if err == nil {
		return true
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat blob %s: %v", id, err)
	}
	return false
}

func TestAttachmentSweepReclaimsOnlyExpiredUnusedUploads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w, server, ts := newAttachmentServerWithServer(t, ctx)
	blobs := server.Attachments.(*DiskAttachments)
	_, channel := w.workspaceWithChannel(t, ctx)

	_, body := upload(t, ts, w.humanA.ID, "sent.png", "image/png", pngBytes)
	sentID := attachmentID(t, body)
	resp, receipt := call(t, ts, http.MethodPost,
		"/messaging/places/"+channel.PlaceID+"/messages", w.humanA.ID,
		map[string]any{"content": "", "client_nonce": "n-sent", "attachments": []string{sentID}})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("send = %d %v, want 201", resp.StatusCode, receipt)
	}

	_, body = upload(t, ts, w.humanA.ID, "abandoned.png", "image/png", pngBytes)
	abandonedID := attachmentID(t, body)
	_, body = upload(t, ts, w.humanA.ID, "fresh.png", "image/png", pngBytes)
	freshID := attachmentID(t, body)
	_, body = upload(t, ts, w.humanA.ID, "avatar.png", "image/png", pngBytes)
	profileID := attachmentID(t, body)
	if _, err := w.store.SetProfile(ctx, w.humanA, "Yohaku", "", profileID, ""); err != nil {
		t.Fatalf("set profile image: %v", err)
	}

	orphanID := NewAttachmentID()
	if _, err := blobs.Put(orphanID, bytes.NewReader(pngBytes)); err != nil {
		t.Fatalf("put orphan blob: %v", err)
	}
	tempPath := filepath.Join(blobs.Root, ".upload-dead")
	if err := os.WriteFile(tempPath, pngBytes, 0o600); err != nil {
		t.Fatalf("write stale temp: %v", err)
	}

	aged := time.Now().Add(-48 * time.Hour)
	backdateAttachment(t, ctx, w, blobs, sentID, aged)
	backdateAttachment(t, ctx, w, blobs, abandonedID, aged)
	backdateAttachment(t, ctx, w, blobs, profileID, aged)
	backdateBlob(t, blobs, orphanID, aged)
	if err := os.Chtimes(tempPath, aged, aged); err != nil {
		t.Fatalf("backdate temp: %v", err)
	}

	swept, err := server.SweepAttachments(ctx, AttachmentOrphanGrace)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if swept.Expired != 1 || swept.Orphaned != 1 {
		t.Fatalf("sweep = %+v, want one expired and one orphaned", swept)
	}
	if blobExists(t, blobs, abandonedID) || blobExists(t, blobs, orphanID) {
		t.Fatal("sweep retained an abandoned or orphaned blob")
	}
	if _, err := os.Stat(tempPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale temp survived sweep: %v", err)
	}
	if resp, _ := fetchAttachment(t, ts, w.humanA.ID, abandonedID); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expired upload fetch = %d, want 404", resp.StatusCode)
	}
	for label, id := range map[string]string{
		"sent": sentID, "fresh": freshID, "profile": profileID,
	} {
		if resp, _ := fetchAttachment(t, ts, w.humanA.ID, id); resp.StatusCode != http.StatusOK {
			t.Fatalf("%s attachment fetch after sweep = %d, want 200", label, resp.StatusCode)
		}
	}
	if resp, _ := fetchAttachment(t, ts, w.humanB.ID, profileID); resp.StatusCode != http.StatusOK {
		t.Fatalf("visible profile image fetch after sweep = %d, want 200", resp.StatusCode)
	}

	swept, err = server.SweepAttachments(ctx, AttachmentOrphanGrace)
	if err != nil || swept != (AttachmentSweep{}) {
		t.Fatalf("second sweep = %+v, error %v, want no work", swept, err)
	}
}

func TestAttachmentSweepSerializesWithProfilePublication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w, server, ts := newAttachmentServerWithServer(t, ctx)
	blobs := server.Attachments.(*DiskAttachments)

	_, body := upload(t, ts, w.humanA.ID, "racing-avatar.png", "image/png", pngBytes)
	profileAttachmentID := attachmentID(t, body)
	backdateAttachment(t, ctx, w, blobs, profileAttachmentID, time.Now().Add(-48*time.Hour))

	const gateKey int32 = 24501
	if _, err := w.store.pool.Exec(ctx, `
		CREATE FUNCTION gate_sweep_profile_publish() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.tagline = 'profile-beats-sweep' THEN
				PERFORM pg_advisory_xact_lock(24501);
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER gate_sweep_profile_publish
		BEFORE INSERT OR UPDATE ON participant_profiles
		FOR EACH ROW EXECUTE FUNCTION gate_sweep_profile_publish()`); err != nil {
		t.Fatalf("install sweep/profile gate: %v", err)
	}
	release := holdProfileTestGate(t, ctx, w, gateKey)

	profileDone := make(chan profileResult, 1)
	go func() {
		profile, err := w.store.SetProfile(
			ctx, w.humanA, "Yohaku", "profile-beats-sweep", profileAttachmentID, "",
		)
		profileDone <- profileResult{profile: profile, err: err}
	}()
	waitForProfileTestGate(t, ctx, w, gateKey)

	type sweepResult struct {
		sweep AttachmentSweep
		err   error
	}
	sweepDone := make(chan sweepResult, 1)
	go func() {
		sweep, err := server.SweepAttachments(ctx, AttachmentOrphanGrace)
		sweepDone <- sweepResult{sweep: sweep, err: err}
	}()

	// The reclaimer selected the row while the profile reference was still
	// uncommitted, then blocked on the attachment lock held by SetProfile.
	deadline := time.Now().Add(5 * time.Second)
	waiting := false
	for !waiting && time.Now().Before(deadline) {
		if err := w.store.pool.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_locks l
			JOIN pg_stat_activity a ON a.pid = l.pid
			WHERE a.datname = current_database() AND NOT l.granted
			  AND l.locktype = 'transactionid'
		)`).Scan(&waiting); err != nil {
			t.Fatalf("inspect sweep attachment waiter: %v", err)
		}
		if !waiting {
			time.Sleep(5 * time.Millisecond)
		}
	}
	if !waiting {
		t.Fatal("attachment sweep did not wait for the profile publication")
	}

	release()
	profileResult := <-profileDone
	if profileResult.err != nil || profileResult.profile.AvatarAttachmentID != profileAttachmentID {
		t.Fatalf("profile publication = %#v, error %v", profileResult.profile, profileResult.err)
	}
	result := <-sweepDone
	if result.err != nil || result.sweep != (AttachmentSweep{}) {
		t.Fatalf("racing sweep = %+v, error %v, want profile image preserved", result.sweep, result.err)
	}
	if !blobExists(t, blobs, profileAttachmentID) {
		t.Fatal("racing sweep removed the committed profile image blob")
	}
}

func TestDiskAttachmentSweepIsBoundedResumableAndLocationSafe(t *testing.T) {
	blobs, err := NewDiskAttachments(t.TempDir())
	if err != nil {
		t.Fatalf("new disk attachments: %v", err)
	}
	old := time.Now().Add(-48 * time.Hour)
	cutoff := time.Now().Add(-24 * time.Hour)

	orphanID := NewAttachmentID()
	if _, err := blobs.Put(orphanID, bytes.NewReader(pngBytes)); err != nil {
		t.Fatalf("put orphan: %v", err)
	}
	backdateBlob(t, blobs, orphanID, old)

	misplacedID := NewAttachmentID()
	misplacedPath := filepath.Join(blobs.Root, misplacedID+".bin")
	if err := os.WriteFile(misplacedPath, pngBytes, 0o600); err != nil {
		t.Fatalf("write misplaced blob: %v", err)
	}
	if err := os.Chtimes(misplacedPath, old, old); err != nil {
		t.Fatalf("backdate misplaced blob: %v", err)
	}
	staleTemp := filepath.Join(blobs.Root, ".upload-stale")
	freshTemp := filepath.Join(blobs.Root, ".upload-fresh")
	for _, path := range []string{staleTemp, freshTemp} {
		if err := os.WriteFile(path, pngBytes, 0o600); err != nil {
			t.Fatalf("write temp %s: %v", path, err)
		}
	}
	if err := os.Chtimes(staleTemp, old, old); err != nil {
		t.Fatalf("backdate stale temp: %v", err)
	}

	seen := map[string]bool{}
	complete := false
	for pass := 0; pass < 32 && !complete; pass++ {
		page, err := blobs.Sweep(context.Background(), cutoff, 1)
		if err != nil {
			t.Fatalf("bounded sweep pass %d: %v", pass, err)
		}
		if page.Visited > 1 {
			t.Fatalf("bounded sweep visited %d entries with budget 1", page.Visited)
		}
		for _, id := range page.Candidates {
			seen[id] = true
		}
		complete = page.CycleComplete
	}
	if !complete {
		t.Fatal("bounded sweep did not complete one small filesystem cycle")
	}
	if !seen[orphanID] || seen[misplacedID] {
		t.Fatalf("sweep candidates = %v, want canonical %s only", seen, orphanID)
	}
	if _, err := os.Stat(staleTemp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale temp survived: %v", err)
	}
	if _, err := os.Stat(freshTemp); err != nil {
		t.Fatalf("fresh temp was removed: %v", err)
	}
	if _, err := os.Stat(misplacedPath); err != nil {
		t.Fatalf("misplaced blob was mutated: %v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := blobs.Sweep(canceled, cutoff, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled sweep error = %v, want context.Canceled", err)
	}

	lateID := NewAttachmentID()
	if _, err := blobs.Put(lateID, bytes.NewReader(pngBytes)); err != nil {
		t.Fatalf("put late orphan: %v", err)
	}
	backdateBlob(t, blobs, lateID, old)
	foundLate := false
	for pass := 0; pass < 64; pass++ {
		page, err := blobs.Sweep(context.Background(), cutoff, 1)
		if err != nil {
			t.Fatalf("restarted sweep pass %d: %v", pass, err)
		}
		for _, id := range page.Candidates {
			foundLate = foundLate || id == lateID
		}
		if page.CycleComplete {
			break
		}
	}
	if !foundLate {
		t.Fatal("a sweep after cancellation/reset did not discover a later orphan")
	}
}
