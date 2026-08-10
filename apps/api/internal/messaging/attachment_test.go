package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

// pngBytes is a byte string http.DetectContentType classifies as image/png:
// the PNG signature is all the sniffer reads.
var pngBytes = append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte("pixel"), 8)...)

func newAttachmentServer(t *testing.T, ctx context.Context) (world, *httptest.Server) {
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
	return w, ts
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
