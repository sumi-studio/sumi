package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

func newAttachmentTestServer(t *testing.T, ctx context.Context) (attachmentFixture, *httptest.Server) {
	t.Helper()
	f := newAttachmentWorld(t, ctx, AttachmentPolicy{})
	for _, participant := range []ParticipantRef{f.humanA, f.humanB, f.agent} {
		if err := f.store.seedDefaultWorkspaceFixture(ctx, participant); err != nil {
			t.Fatalf("prepare default test Workspace: %v", err)
		}
	}
	server := NewServer(f.store.core, stubSessions{})
	server.AllowedOrigins = []string{testOrigin}
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	testStoresByServer.Store(ts.URL, f.store)
	t.Cleanup(ts.Close)
	return f, ts
}

func scopedPath(t *testing.T, ts *httptest.Server, actor string, path string) string {
	t.Helper()
	store, ok := testStoreForServer(ts.URL)
	if !ok {
		t.Fatal("no fixture store")
	}
	scoped, err := store.fixtureScopeForRequest(context.Background(), Human(actor), path, map[string]any{})
	if err != nil {
		t.Fatalf("fixture scope: %v", err)
	}
	parsed, _ := url.Parse(path)
	query := parsed.Query()
	query.Set("workspace_id", scoped.Scope.WorkspaceID)
	query.Set("installation_id", scoped.Scope.InstallationID)
	query.Set("authority_epoch", strconv.FormatInt(scoped.Scope.AuthorityEpoch, 10))
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func rawUpload(t *testing.T, ts *httptest.Server, actor, placeID, nonce, filename, contentType string, body []byte, mutate func(*http.Request)) (*http.Response, map[string]any) {
	t.Helper()
	path := scopedPath(t, ts, actor, "/messaging/places/"+placeID+"/attachments")
	req, err := http.NewRequest(http.MethodPost, ts.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Origin", testOrigin)
	req.Header.Set(AttachmentUploadNonceHeader, nonce)
	req.Header.Set(AttachmentUploadFilenameHeader, url.PathEscape(filename))
	req.Header.Set("Content-Type", contentType)
	req.ContentLength = int64(len(body))
	req.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: actor})
	if mutate != nil {
		mutate(req)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var decoded map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&decoded)
	return resp, decoded
}

func rawDownload(t *testing.T, ts *httptest.Server, actor, attachmentID string) (*http.Response, []byte) {
	t.Helper()
	path := scopedPath(t, ts, actor, "/messaging/attachments/"+attachmentID)
	req, _ := http.NewRequest(http.MethodGet, ts.URL+path, nil)
	req.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: actor})
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp, body
}

func TestAttachmentHTTPUploadSendAndSafeDownload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f, ts := newAttachmentTestServer(t, ctx)
	_, channel := f.workspaceWithChannel(t, ctx)
	// The default fixture Workspace also exists; make sure the channel we use
	// is the one the fixture scope resolves for the path.
	image := append(append([]byte{}, pngHeader...), bytes.Repeat([]byte{1}, 64)...)
	svg := []byte(`<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>`)

	resp, body := rawUpload(t, ts, f.humanA.ID, channel.PlaceID, "u-1", "写真 1.png", "image/png", image, nil)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("upload: %d %v", resp.StatusCode, body)
	}
	att := body["attachment"].(map[string]any)
	imageID := att["attachment_id"].(string)
	if att["mime"] != "image/png" || att["filename"] != "写真 1.png" || att["size_bytes"].(float64) != float64(len(image)) {
		t.Fatalf("receipt: %v", att)
	}
	// Retry: same nonce, no new row, 200.
	resp, body = rawUpload(t, ts, f.humanA.ID, channel.PlaceID, "u-1", "写真 1.png", "image/png", image, nil)
	if resp.StatusCode != http.StatusOK || body["created"] != false || body["attachment"].(map[string]any)["attachment_id"] != imageID {
		t.Fatalf("retry: %d %v", resp.StatusCode, body)
	}
	// SVG claims image but is a document: stored as opaque bytes.
	resp, body = rawUpload(t, ts, f.humanA.ID, channel.PlaceID, "u-2", "../../evil.svg", "image/svg+xml", svg, nil)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("svg upload: %d %v", resp.StatusCode, body)
	}
	svgAtt := body["attachment"].(map[string]any)
	svgID := svgAtt["attachment_id"].(string)
	if svgAtt["mime"] != "application/octet-stream" || svgAtt["filename"] != "evil.svg" {
		t.Fatalf("svg receipt: %v", svgAtt)
	}
	// Transport shape failures never reserve or store anything.
	for name, mutate := range map[string]func(*http.Request){
		"missing nonce":  func(r *http.Request) { r.Header.Del(AttachmentUploadNonceHeader) },
		"chunked body":   func(r *http.Request) { r.ContentLength = -1 },
		"bad filename":   func(r *http.Request) { r.Header.Set(AttachmentUploadFilenameHeader, "%zz") },
		"empty body":     func(r *http.Request) { r.Body = io.NopCloser(bytes.NewReader(nil)); r.ContentLength = 0 },
		"origin missing": func(r *http.Request) { r.Header.Del("Origin") },
	} {
		resp, body := rawUpload(t, ts, f.humanA.ID, channel.PlaceID, "u-shape-"+name, "x.txt", "text/plain", []byte("x"), mutate)
		if resp.StatusCode/100 != 4 {
			t.Fatalf("%s: %d %v", name, resp.StatusCode, body)
		}
	}
	// A body shorter than Content-Length (connection closed early) is a size
	// mismatch and leaves no staged bytes or attachment row behind. Go's
	// client refuses to send such a request, so speak HTTP/1.1 by hand.
	shortPath := scopedPath(t, ts, f.humanA.ID, "/messaging/places/"+channel.PlaceID+"/attachments")
	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.WriteString(conn, "POST "+shortPath+" HTTP/1.1\r\nHost: x\r\nOrigin: "+testOrigin+
		"\r\nCookie: "+agentevents.BrowserSessionCookie+"="+f.humanA.ID+
		"\r\n"+AttachmentUploadNonceHeader+": u-short\r\n"+AttachmentUploadFilenameHeader+": short.txt\r\n"+
		"Content-Type: text/plain\r\nContent-Length: 3\r\n\r\nab")
	_ = conn.(*net.TCPConn).CloseWrite()
	shortResponse, _ := io.ReadAll(conn)
	conn.Close()
	if bytes.Contains(shortResponse, []byte("201 Created")) {
		t.Fatalf("short body accepted: %s", shortResponse)
	}
	var shortRows int
	if err := f.store.core.pool.QueryRow(ctx, "SELECT count(*) FROM message_attachments WHERE client_nonce='u-short'").Scan(&shortRows); err != nil || shortRows != 0 {
		t.Fatalf("short body left a row: %d %v", shortRows, err)
	}
	if entries, _ := filepath.Glob(filepath.Join(f.root, "*", "*", ".staging-*")); len(entries) != 0 {
		t.Fatalf("staging debris: %v", entries)
	}
	if used := f.usedBytes(t, ctx, channel.WorkspaceID); used != int64(len(image)+len(svg)) {
		// The short-body reservation stays charged until it expires and the
		// reconciler releases it; that is the documented durable-ledger rule.
		if used != int64(len(image)+len(svg)+3) {
			t.Fatalf("ledger: %d", used)
		}
	}

	// Send text + attachments, then attachment-only, through the same route.
	resp, body = call(t, ts, http.MethodPost, "/messaging/places/"+channel.PlaceID+"/messages", f.humanA.ID, map[string]any{
		"content": "", "client_nonce": "s-1", "attachments": []string{svgID, imageID},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("attachment-only send: %d %v", resp.StatusCode, body)
	}
	resp, body = call(t, ts, http.MethodPost, "/messaging/places/"+channel.PlaceID+"/messages", f.humanA.ID, map[string]any{
		"content": "", "client_nonce": "s-2",
	})
	if resp.StatusCode != http.StatusBadRequest || body["error"] != "invalid_content" {
		t.Fatalf("empty send: %d %v", resp.StatusCode, body)
	}
	resp, body = call(t, ts, http.MethodPost, "/messaging/places/"+channel.PlaceID+"/messages", f.humanA.ID, map[string]any{
		"content": "hi", "client_nonce": "s-3", "attachments": []string{imageID},
	})
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("rebind through HTTP: %d %v", resp.StatusCode, body)
	}
	// History for another member projects ordered attachments.
	resp, body = call(t, ts, http.MethodGet, "/messaging/places/"+channel.PlaceID+"/messages", f.humanB.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("history: %d %v", resp.StatusCode, body)
	}
	messages := body["messages"].([]any)
	last := messages[len(messages)-1].(map[string]any)
	projected := last["attachments"].([]any)
	if len(projected) != 2 || projected[0].(map[string]any)["attachment_id"] != svgID || projected[1].(map[string]any)["position"].(float64) != 1 {
		t.Fatalf("history attachments: %v", projected)
	}

	// Download: image inline with the exact MIME, document as an opaque
	// attachment; both nosniff, sandboxed, no-store.
	resp, got := rawDownload(t, ts, f.humanB.ID, imageID)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(got, image) {
		t.Fatalf("image download: %d", resp.StatusCode)
	}
	for header, want := range map[string]string{
		"Content-Type":            "image/png",
		"X-Content-Type-Options":  "nosniff",
		"Content-Security-Policy": "default-src 'none'; sandbox",
		"Cache-Control":           "private, no-store",
		"Referrer-Policy":         "no-referrer",
	} {
		if resp.Header.Get(header) != want {
			t.Fatalf("%s: %q", header, resp.Header.Get(header))
		}
	}
	if disposition := resp.Header.Get("Content-Disposition"); len(disposition) < 6 || disposition[:6] != "inline" {
		t.Fatalf("image disposition %q", disposition)
	}
	resp, got = rawDownload(t, ts, f.humanB.ID, svgID)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(got, svg) {
		t.Fatalf("svg download: %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != "application/octet-stream" || resp.Header.Get("Content-Disposition")[:10] != "attachment" {
		t.Fatalf("svg headers: %v", resp.Header)
	}
	// Absent, foreign-scope, and malformed ids collapse to the same 404.
	for _, id := range []string{"0190aaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa", "nope", imageID + "x"} {
		resp, _ := rawDownload(t, ts, f.humanB.ID, id)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("download %q: %d", id, resp.StatusCode)
		}
	}
	// A real registered Human without membership sees the same 404 as a
	// missing id; the fixture scope is intentionally present so this proves
	// attachment authorization rather than a missing session/scope shortcut.
	strangerID, err := koseki.New(f.store.core.pool).MintHuman(ctx)
	if err != nil {
		t.Fatal(err)
	}
	resp, _ = rawDownload(t, ts, strangerID, imageID)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("stranger download: %d", resp.StatusCode)
	}
	// Session-less download is 401, never a hint about existence.
	req, _ := http.NewRequest(http.MethodGet, ts.URL+scopedPath(t, ts, f.humanB.ID, "/messaging/attachments/"+imageID), nil)
	noSession, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	noSession.Body.Close()
	if noSession.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no session: %d", noSession.StatusCode)
	}
}

func TestAttachmentHTTPTombstonedNonceSurvivesReceiptGC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f, ts := newAttachmentTestServer(t, ctx)
	workspace, channel := f.workspaceWithChannel(t, ctx)
	data := []byte("permanent nonce identity")

	resp, body := rawUpload(t, ts, f.humanA.ID, channel.PlaceID, "retired-after-gc", "doc.txt", "text/plain", data, nil)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("initial upload: %d %v", resp.StatusCode, body)
	}
	attachmentID := body["attachment"].(map[string]any)["attachment_id"].(string)
	sender := f.store.mustScope(t, ctx, workspace.WorkspaceID, f.humanA)
	msg, _, err := sender.AppendMessage(ctx, AppendInput{
		PlaceID: channel.PlaceID, Content: "doc", ClientNonce: "retire-message", AttachmentIDs: []string{attachmentID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.DeleteMessage(ctx, channel.PlaceID, msg.MessageID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.core.ReconcileAttachments(ctx); err != nil {
		t.Fatal(err)
	}
	// Force the receipt beyond its normal TTL and make the next reconciliation
	// exercise receipt GC after the attachment has been tombstoned and its blob
	// removed. The history row must retain the permanent nonce identity.
	f.store.core.attachmentPolicy.UnboundTTL = time.Millisecond
	if _, err := f.store.core.pool.Exec(ctx, `
		UPDATE message_attachment_uploads
		SET settled_at = NOW() - INTERVAL '1 hour'
		WHERE attachment_id = $1`, attachmentID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.core.ReconcileAttachments(ctx); err != nil {
		t.Fatal(err)
	}
	var receiptRows int
	if err := f.store.core.pool.QueryRow(ctx, "SELECT count(*) FROM message_attachment_uploads WHERE attachment_id = $1", attachmentID).Scan(&receiptRows); err != nil {
		t.Fatal(err)
	}
	if receiptRows != 1 {
		t.Fatalf("receipt GC removed historical nonce identity: %d rows", receiptRows)
	}
	beforeBytes, beforeObjects := f.totalUsage(t, ctx)
	beforeStaging, err := filepath.Glob(filepath.Join(f.root, "*", "*", ".staging-*"))
	if err != nil {
		t.Fatal(err)
	}

	resp, body = rawUpload(t, ts, f.humanA.ID, channel.PlaceID, "retired-after-gc", "doc.txt", "text/plain", data, nil)
	if resp.StatusCode != http.StatusGone || body["error"] != "attachment_upload_retired" {
		t.Fatalf("retry after receipt GC: %d %v", resp.StatusCode, body)
	}
	afterBytes, afterObjects := f.totalUsage(t, ctx)
	afterStaging, err := filepath.Glob(filepath.Join(f.root, "*", "*", ".staging-*"))
	if err != nil {
		t.Fatal(err)
	}
	if afterBytes != beforeBytes || afterObjects != beforeObjects {
		t.Fatalf("retired retry changed quota from %d/%d to %d/%d", beforeBytes, beforeObjects, afterBytes, afterObjects)
	}
	if len(afterStaging) != len(beforeStaging) {
		t.Fatalf("retired retry changed staging artifacts from %v to %v", beforeStaging, afterStaging)
	}
}

func TestAttachmentHTTPWithoutStorageFailsClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)
	_, channel := w.workspaceWithChannel(t, ctx)
	resp, body := rawUpload(t, ts, w.humanA.ID, channel.PlaceID, "n", "x.txt", "text/plain", []byte("x"), nil)
	if resp.StatusCode != http.StatusServiceUnavailable || body["error"] != "attachments_unavailable" {
		t.Fatalf("upload without storage: %d %v", resp.StatusCode, body)
	}
	resp, _ = rawDownload(t, ts, w.humanA.ID, "0190aaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("download without storage: %d", resp.StatusCode)
	}
	resp, body = call(t, ts, http.MethodPost, "/messaging/places/"+channel.PlaceID+"/messages", w.humanA.ID, map[string]any{
		"content": "", "client_nonce": "s", "attachments": []string{"0190aaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa"},
	})
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("send with unknown attachment: %d %v", resp.StatusCode, body)
	}
}

// TestLocalUploadReleasesLeaseAndReadmitsExactEpoch proves the PA lane's
// staged upload: the initial runtime lease is released before the body streams
// (a replacement can proceed while bytes are in flight) and the finalization
// readmits only the exact epoch that authenticated the request. A replaced
// epoch fails closed with nothing durable and no staged bytes.
func TestLocalUploadReleasesLeaseAndReadmitsExactEpoch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newAttachmentWorld(t, ctx, AttachmentPolicy{})
	workspace, channel := f.workspaceWithChannel(t, ctx)
	scoped := f.store.mustScope(t, ctx, workspace.WorkspaceID, f.agent)
	messagingServer := NewServer(f.store.core, nil)

	commandStore, err := agentevents.OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = commandStore.Close() })
	gateway, err := agentevents.OpenDurableGateway(privateRuntimeDir(t), commandStore)
	if err != nil {
		t.Fatal(err)
	}
	authorization := agentevents.LocalRuntimeAuthorization{
		BearerToken: "attachment-upload-bearer-generation-one", TenantID: "attachment-upload",
		PersonalityAgentID: f.agent.ID, Generation: 1, RPCBootNonce: "attachment-boot-1",
		Audience: agentevents.DefaultAgentAudience(), DeliveryAuthorization: agentevents.LocalDeliveryRaw,
	}
	control, err := agentevents.NewLocalControlServer(gateway, []byte("attachment-upload-secret-at-least-32-bytes-long"),
		[]agentevents.LocalRuntimeAuthorization{authorization})
	if err != nil {
		t.Fatal(err)
	}
	if err := messagingServer.RegisterLocalControlRoutes(control); err != nil {
		t.Fatal(err)
	}
	handler, err := control.HandlerForLocalRuntime(f.agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(t.TempDir(), "lc.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: handler}
	go func() { _ = httpServer.Serve(listener) }()
	t.Cleanup(func() { _ = httpServer.Close() })
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", socketPath)
	}}}

	newUpload := func(nonce string, body io.Reader, size int64, bearer string) *http.Request {
		req, err := http.NewRequest(http.MethodPost, "http://local"+LocalUploadAttachmentPath(channel.PlaceID), body)
		if err != nil {
			t.Fatal(err)
		}
		req.ContentLength = size
		req.Header.Set("Authorization", "Bearer "+bearer)
		req.Header.Set(LocalScopeWorkspaceHeader, scoped.Scope.WorkspaceID)
		req.Header.Set(LocalScopeInstallationHeader, scoped.Scope.InstallationID)
		req.Header.Set(LocalScopeAuthorityEpochHeader, strconv.FormatInt(scoped.Scope.AuthorityEpoch, 10))
		req.Header.Set(AttachmentUploadNonceHeader, nonce)
		req.Header.Set(AttachmentUploadFilenameHeader, url.PathEscape("report.txt"))
		req.Header.Set("Content-Type", "text/plain")
		return req
	}
	// Happy path.
	data := []byte("agent-sourced bytes")
	resp, err := client.Do(newUpload("a-1", bytes.NewReader(data), int64(len(data)), authorization.BearerToken))
	if err != nil {
		t.Fatal(err)
	}
	var receipt struct {
		Attachment attachmentWire `json:"attachment"`
		Created    bool           `json:"created"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&receipt); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated || !receipt.Created || receipt.Attachment.SizeBytes != int64(len(data)) {
		t.Fatalf("local upload: %d %+v", resp.StatusCode, receipt)
	}
	// Send it through the local write route, then read it back through the
	// local attachment route bound to the exact place/message.
	body, _ := json.Marshal(map[string]any{
		"workspace_id": scoped.Scope.WorkspaceID, "installation_id": scoped.Scope.InstallationID,
		"authority_epoch": strconv.FormatInt(scoped.Scope.AuthorityEpoch, 10),
		"place_id":        channel.PlaceID, "content": "", "client_nonce": "w-1",
		"attachments": []string{receipt.Attachment.AttachmentID},
	})
	writeReq, _ := http.NewRequest(http.MethodPost, "http://local"+LocalWritePath, bytes.NewReader(body))
	writeReq.Header.Set("Authorization", "Bearer "+authorization.BearerToken)
	writeReq.Header.Set("Content-Type", "application/json")
	writeResp, err := client.Do(writeReq)
	if err != nil {
		t.Fatal(err)
	}
	var written struct {
		MessageID string `json:"message_id"`
	}
	_ = json.NewDecoder(writeResp.Body).Decode(&written)
	writeResp.Body.Close()
	if writeResp.StatusCode != http.StatusCreated || written.MessageID == "" {
		t.Fatalf("local write with attachment: %d", writeResp.StatusCode)
	}
	fetch := func(placeID, messageID, attachmentID string) (*http.Response, []byte) {
		body, _ := json.Marshal(map[string]any{
			"workspace_id": scoped.Scope.WorkspaceID, "installation_id": scoped.Scope.InstallationID,
			"authority_epoch": strconv.FormatInt(scoped.Scope.AuthorityEpoch, 10),
			"place_id":        placeID, "message_id": messageID, "attachment_id": attachmentID,
		})
		req, _ := http.NewRequest(http.MethodPost, "http://local"+LocalAttachmentPath, bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+authorization.BearerToken)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		got, _ := io.ReadAll(resp.Body)
		return resp, got
	}
	resp, got := fetch(channel.PlaceID, written.MessageID, receipt.Attachment.AttachmentID)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) || resp.Header.Get("X-Sumi-Attachment-Mime") != "text/plain" {
		t.Fatalf("local fetch: %d %q", resp.StatusCode, got)
	}
	// A wrong message binding is not-found, never a hint.
	resp, _ = fetch(channel.PlaceID, newUUIDv7(), receipt.Attachment.AttachmentID)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("mismatched binding: %d", resp.StatusCode)
	}

	// Replacement during body streaming: the body is held open until the
	// authorization epoch is replaced; finalization must then fail closed.
	pipeReader, pipeWriter := io.Pipe()
	replaced := agentevents.LocalRuntimeAuthorization{
		BearerToken: "attachment-upload-bearer-generation-two", TenantID: "attachment-upload",
		PersonalityAgentID: f.agent.ID, Generation: 2, RPCBootNonce: "attachment-boot-2",
		Audience: agentevents.DefaultAgentAudience(), DeliveryAuthorization: agentevents.LocalDeliveryRaw,
	}
	payload := bytes.Repeat([]byte{7}, 4096)
	var wg sync.WaitGroup
	wg.Add(1)
	var stagedResp *http.Response
	var stagedErr error
	go func() {
		defer wg.Done()
		stagedResp, stagedErr = client.Do(newUpload("a-2", pipeReader, int64(len(payload)), authorization.BearerToken))
	}()
	// Write half, then replace the epoch. Replacement must not block on the
	// in-flight body: the initial lease was released before body reads.
	if _, err := pipeWriter.Write(payload[:2048]); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	installDone := make(chan error, 1)
	go func() {
		installDone <- control.InstallLocalRuntimeAuthorization(ctx, replaced)
	}()
	select {
	case err := <-installDone:
		if err != nil {
			t.Fatalf("replace authorization while body in flight: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("authorization replacement blocked behind an in-flight upload body")
	}
	if _, err := pipeWriter.Write(payload[2048:]); err != nil {
		t.Fatal(err)
	}
	pipeWriter.Close()
	wg.Wait()
	if stagedErr != nil {
		t.Fatal(stagedErr)
	}
	stagedResp.Body.Close()
	if stagedResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("finalize under replaced epoch: %d", stagedResp.StatusCode)
	}
	var rows int
	if err := f.store.core.pool.QueryRow(ctx, "SELECT count(*) FROM message_attachments WHERE client_nonce='a-2'").Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("replaced epoch left a durable row: %d %v", rows, err)
	}
	if entries, _ := filepath.Glob(filepath.Join(f.root, "*", "*", ".staging-*")); len(entries) != 0 {
		t.Fatalf("staging debris after replaced epoch: %v", entries)
	}
	if entries, _ := filepath.Glob(filepath.Join(f.root, "*", "*", "*.bin")); len(entries) != 1 {
		t.Fatalf("published blobs: %v", entries)
	}
	// The stale bearer no longer authenticates at all.
	resp, err = client.Do(newUpload("a-3", bytes.NewReader(data), int64(len(data)), authorization.BearerToken))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("stale bearer: %d", resp.StatusCode)
	}
	if !errors.Is(ctx.Err(), nil) {
		t.Fatal(ctx.Err())
	}
}

// TestAttachmentDraftSpoilerAltAndEditWindow covers「送る前だけ」の編集:
// the uploader's own still-unbound attachment takes a new display name, a
// description, and the spoiler flag; an unnamed field is left alone; a
// stranger learns nothing; and the moment the attachment becomes part of a
// message the edit is refused. Both declarations then travel with every
// delivery of that message.
func TestAttachmentDraftSpoilerAltAndEditWindow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f, ts := newAttachmentTestServer(t, ctx)
	workspace, channel := f.workspaceWithChannel(t, ctx)
	image := append(append([]byte{}, pngHeader...), bytes.Repeat([]byte{2}, 64)...)

	resp, body := rawUpload(t, ts, f.humanA.ID, channel.PlaceID, "e-1", "shot.png", "image/png", image, nil)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("upload: %d %v", resp.StatusCode, body)
	}
	att := body["attachment"].(map[string]any)
	id := att["attachment_id"].(string)
	// A fresh upload hides nothing and describes nothing.
	if att["spoiler"] != false || att["alt"] != "" {
		t.Fatalf("fresh upload: %v", att)
	}
	path := "/messaging/attachments/" + id
	scope := map[string]any{"workspace_id": workspace.WorkspaceID}
	patch := func(actor string, fields map[string]any) (*http.Response, map[string]any) {
		t.Helper()
		merged := map[string]any{}
		for key, value := range scope {
			merged[key] = value
		}
		for key, value := range fields {
			merged[key] = value
		}
		return call(t, ts, http.MethodPatch, path, actor, merged)
	}

	// Nobody else may edit it, and learns nothing about its existence.
	resp, body = patch(f.humanB.ID, map[string]any{"spoiler": true})
	if resp.StatusCode != http.StatusNotFound || body["error"] != "not_found" {
		t.Fatalf("stranger patch: %d %v", resp.StatusCode, body)
	}

	// The uploader covers it and describes it. Control characters in the
	// description collapse to spaces: it is one paragraph, not a message.
	resp, body = patch(f.humanA.ID, map[string]any{"spoiler": true, "alt": " 結末の\n一枚 "})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("patch: %d %v", resp.StatusCode, body)
	}
	if body["spoiler"] != true || body["alt"] != "結末の 一枚" || body["filename"] != "shot.png" {
		t.Fatalf("patched attachment: %v", body)
	}

	// An unnamed field is「触らない」: renaming keeps the spoiler and the alt.
	resp, body = patch(f.humanA.ID, map[string]any{"filename": "ending.png"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("rename: %d %v", resp.StatusCode, body)
	}
	if body["filename"] != "ending.png" || body["spoiler"] != true || body["alt"] != "結末の 一枚" {
		t.Fatalf("rename dropped the other fields: %v", body)
	}

	// Naming nothing at all is not an edit, and an oversized description is
	// refused rather than silently truncated.
	resp, body = patch(f.humanA.ID, nil)
	if resp.StatusCode != http.StatusBadRequest || body["error"] != "invalid_request" {
		t.Fatalf("empty patch: %d %v", resp.StatusCode, body)
	}
	resp, body = patch(f.humanA.ID, map[string]any{"alt": strings.Repeat("あ", MaxAttachmentAltRunes+1)})
	if resp.StatusCode != http.StatusBadRequest || body["error"] != "invalid_request" {
		t.Fatalf("oversized alt: %d %v", resp.StatusCode, body)
	}

	// Sent: the declarations ride along with the message.
	resp, body = call(t, ts, http.MethodPost, "/messaging/places/"+channel.PlaceID+"/messages", f.humanA.ID,
		map[string]any{"content": "見る?", "client_nonce": "e-send", "attachments": []string{id}})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("send: %d %v", resp.StatusCode, body)
	}

	// The recipient's own read of the timeline carries them too.
	resp, body = call(t, ts, http.MethodGet, "/messaging/places/"+channel.PlaceID+"/messages", f.humanB.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("history: %d %v", resp.StatusCode, body)
	}
	messages := body["messages"].([]any)
	received := messages[len(messages)-1].(map[string]any)["attachments"].([]any)
	if len(received) != 1 {
		t.Fatalf("received attachments: %v", received)
	}
	seen := received[0].(map[string]any)
	if seen["spoiler"] != true || seen["alt"] != "結末の 一枚" || seen["filename"] != "ending.png" {
		t.Fatalf("received attachment wire: %v", seen)
	}

	// After send the window is closed. What was seen was seen.
	resp, body = patch(f.humanA.ID, map[string]any{"spoiler": false})
	if resp.StatusCode != http.StatusConflict || body["error"] != "attachment_already_sent" {
		t.Fatalf("patch after send: %d %v", resp.StatusCode, body)
	}
	var stillCovered bool
	if err := f.store.core.pool.QueryRow(ctx,
		"SELECT spoiler FROM message_attachments WHERE attachment_id=$1", id).Scan(&stillCovered); err != nil {
		t.Fatal(err)
	}
	if !stillCovered {
		t.Fatal("a refused edit still changed the durable row")
	}
}

func TestSanitizeAttachmentAlt(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"  結末の一枚  ", "結末の一枚"},
		{"二行の\n説明", "二行の 説明"},
		{"tab\tここ", "tab ここ"},
		{"\x00\x7f", ""},
		{"before\u0085after", "before after"},
		{"before\u2028after\u2029end", "before after end"},
		{"before\u202eafter\u200bend", "before after end"},
		{"", ""},
	} {
		if got := sanitizeAttachmentAlt(tc.in); got != tc.want {
			t.Fatalf("sanitizeAttachmentAlt(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestSanitizeAttachmentFilenameRemovesForbiddenDisplayCharacters(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"before\u0085after.txt", "beforeafter.txt"},
		{"before\u2028after\u2029end.txt", "beforeafterend.txt"},
		{"before\u202eafter\u200bend.txt", "beforeafterend.txt"},
	} {
		if got := sanitizeAttachmentFilename(tc.in); got != tc.want {
			t.Fatalf("sanitizeAttachmentFilename(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
