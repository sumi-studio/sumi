package feedback

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

var pngEvidence = []byte("\x89PNG\r\n\x1a\nfeedback screenshot")

type stalledAttachmentBody struct {
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (b *stalledAttachmentBody) Read(_ []byte) (int, error) {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return 0, io.EOF
}

type observedAttachmentBody struct{ reads int }

func (b *observedAttachmentBody) Read(_ []byte) (int, error) { b.reads++; return 0, io.EOF }

func TestAttachmentTransferLimitPrecedesBodyAndBlobReadsAndReleasesCapacity(t *testing.T) {
	w := fixture(t)
	s := &Server{Store: w.s, Sessions: sessions{}, AllowedOrigins: []string{"https://sumi.test"}}
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)
	request := func(method string, body io.Reader) *httptest.ResponseRecorder {
		url := "/feedback/attachments"
		if method == "GET" {
			url += "/" + newID()
		}
		r := httptest.NewRequest(method, url, body)
		r.Header.Set("Origin", "https://sumi.test")
		r.Header.Set("Content-Type", "multipart/form-data; boundary=feedback-transfer-test")
		r.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: w.human.ID})
		res := httptest.NewRecorder()
		mux.ServeHTTP(res, r)
		return res
	}
	releaseBodies := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseBodies) }) }
	defer release()
	finished := make(chan int, maxAttachmentTransfers)
	for i := 0; i < maxAttachmentTransfers; i++ {
		body := &stalledAttachmentBody{entered: make(chan struct{}), release: releaseBodies}
		go func() { finished <- request("POST", body).Code }()
		select {
		case <-body.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("transfer did not reach admitted body read")
		}
	}
	extraBody := &observedAttachmentBody{}
	if res := request("POST", extraBody); res.Code != 503 || !strings.Contains(res.Body.String(), "attachment_transfer_busy") || extraBody.reads != 0 {
		t.Fatal("busy upload read body", res.Code, extraBody.reads, res.Body.String())
	}
	if res := request("GET", nil); res.Code != 503 {
		t.Fatal("download did not share upload slots", res.Code)
	}
	// A second server is independent of the first server's occupied capacity.
	otherRelease, err := (&Server{}).acquireAttachmentTransfer()
	if err != nil {
		t.Fatal("transfer capacity coupled across servers", err)
	}
	otherRelease()
	release()
	for i := 0; i < maxAttachmentTransfers; i++ {
		select {
		case code := <-finished:
			if code != 400 {
				t.Fatal("incomplete multipart response", code)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("failed transfer retained slot")
		}
	}
	if res := request("GET", nil); res.Code != 404 {
		t.Fatal("completed transfers did not release capacity", res.Code)
	}
	s.Sessions = sessions{revoked: true}
	extraBody = &observedAttachmentBody{}
	if res := request("POST", extraBody); res.Code != 401 || extraBody.reads != 0 {
		t.Fatal("revoked session body read", res.Code, extraBody.reads)
	}
}

func TestAttachmentContentBoundary(t *testing.T) {
	for _, test := range []struct {
		data []byte
		want string
	}{
		{pngEvidence, "image/png"},
		{[]byte("\xff\xd8\xffimage"), "image/jpeg"},
		{[]byte("RIFF\x00\x00\x00\x00WEBPVP8 "), "image/webp"},
		{[]byte("\x1a\x45\xdf\xa3video"), "video/webm"},
		{[]byte("\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00mp42isom"), "video/mp4"},
		{[]byte(`<svg onload="alert(1)"/>`), ""},
		{[]byte("<html>document</html>"), ""},
	} {
		if got := attachmentMIME(test.data); got != test.want {
			t.Errorf("MIME %q, want %q", got, test.want)
		}
	}
	if got := attachmentName("../../screen\u202e\n.png"); got != "screen.png" {
		t.Fatal(got)
	}
	if got := attachmentName(strings.Repeat("写", 100)); len(got) > 255 {
		t.Fatal("name not bounded")
	}
	if validAttachmentIDs([]string{newID(), "bad"}) {
		t.Fatal("invalid attachment identity accepted")
	}
	id := newID()
	if validAttachmentIDs([]string{id, id}) {
		t.Fatal("duplicate attachment identity accepted")
	}
	if _, err := (&Store{}).UploadAttachment(context.Background(), participant.Ref{}, "too-large.png", make([]byte, MaxAttachmentBytes+1)); !errors.Is(err, ErrAttachmentTooLarge) {
		t.Fatal("oversize file accepted", err)
	}
	if _, err := (&Store{}).UploadAttachment(context.Background(), participant.Ref{}, "fake.png", []byte("<svg/>")); !errors.Is(err, ErrAttachmentType) {
		t.Fatal("filename spoofed media type", err)
	}
}

func TestAttachmentStagingQuotaDoesNotBlockOtherAuthors(t *testing.T) {
	w := fixture(t)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if _, err := w.s.UploadAttachment(ctx, w.human, "screen.png", pngEvidence); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.s.UploadAttachment(ctx, w.human, "screen.png", pngEvidence); !errors.Is(err, ErrAttachmentQuota) {
		t.Fatal("staged object quota not enforced", err)
	}
	if _, err := w.s.UploadAttachment(ctx, w.stranger, "screen.png", pngEvidence); err != nil {
		t.Fatal("other author blocked", err)
	}
	if _, err := w.pool.Exec(ctx, `UPDATE feedback_attachments SET expires_at=now()-interval '1 hour' WHERE author_key=$1`, w.human.Key()); err != nil {
		t.Fatal(err)
	}
	if _, err := w.s.UploadAttachment(ctx, w.human, "screen.png", pngEvidence); err != nil {
		t.Fatal("expired quota not reclaimed", err)
	}
}

func TestAttachmentOwnershipAtomicBindingRetryAndExpiry(t *testing.T) {
	w := fixture(t)
	ctx := context.Background()
	upload := func(actor participant.Ref) Attachment {
		t.Helper()
		a, err := w.s.UploadAttachment(ctx, actor, "screen.png", pngEvidence)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	a := upload(w.human)
	for _, actor := range []participant.Ref{w.stranger, w.dev, w.pa} {
		if _, _, err := w.s.ReadAttachment(ctx, actor, a.ID); !errors.Is(err, ErrNotFound) {
			t.Fatal("staged upload leaked", err)
		}
	}
	if _, data, err := w.s.ReadAttachment(ctx, w.human, a.ID); err != nil || !bytes.Equal(data, pngEvidence) {
		t.Fatal(err)
	}
	foreign := upload(w.stranger)
	if _, err := w.s.Create(ctx, w.human, "test", "body", uuid.NewString(), nil, a.ID, foreign.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("foreign upload bound", err)
	}
	var count int
	if err := w.pool.QueryRow(ctx, `SELECT count(*) FROM feedback_threads`).Scan(&count); err != nil || count != 0 {
		t.Fatal("partial thread committed", count, err)
	}
	var unbound bool
	if err := w.pool.QueryRow(ctx, `SELECT thread_id IS NULL FROM feedback_attachments WHERE attachment_id=$1`, a.ID).Scan(&unbound); err != nil || !unbound {
		t.Fatal("partial binding committed", err)
	}
	nonce := uuid.NewString()
	thread, err := w.s.Create(ctx, w.human, "test", "body", nonce, nil, a.ID)
	if err != nil || len(thread.Attachments) != 1 {
		t.Fatal(thread, err)
	}
	retry, err := w.s.Create(ctx, w.human, "test", "body", nonce, nil, a.ID)
	if err != nil {
		t.Fatal("retry failed", err)
	}
	// Compare the complete response contract. Database timestamps and a JSON
	// receipt can represent the same UTC instant with different Go Locations.
	originalJSON, err := json.Marshal(thread)
	if err != nil {
		t.Fatal(err)
	}
	retryJSON, err := json.Marshal(retry)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(originalJSON, retryJSON) {
		t.Fatalf("retry changed original response: original=%s retry=%s", originalJSON, retryJSON)
	}
	if _, err = w.s.Create(ctx, w.human, "test", "body", nonce, nil); !errors.Is(err, ErrRequest) {
		t.Fatal("retry accepted changed attachments", err)
	}
	if _, err = w.s.Create(ctx, w.human, "test", "body", uuid.NewString(), nil, a.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("bound attachment reused", err)
	}
	if _, _, err = w.s.ReadAttachment(ctx, w.dev, a.ID); err != nil {
		t.Fatal("recipient cannot see sent attachment", err)
	}
	if _, _, err = w.s.ReadAttachment(ctx, w.stranger, a.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("stranger saw sent attachment", err)
	}
	detail, err := w.s.Open(ctx, w.dev, thread.ID, "")
	if err != nil || !reflect.DeepEqual(detail.Thread.Attachments, thread.Attachments) {
		t.Fatal("detail lost attachments", err)
	}
	if _, err = w.pool.Exec(ctx, `UPDATE feedback_attachments SET expires_at=now()-interval '1 hour'`); err != nil {
		t.Fatal(err)
	}
	if _, err = w.s.Create(ctx, w.stranger, "expired", "body", uuid.NewString(), nil, foreign.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("expired upload bound", err)
	}
	if err = w.s.CleanupAttachments(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err = w.s.ReadAttachment(ctx, w.dev, a.ID); err != nil {
		t.Fatal("cleanup deleted bound evidence", err)
	}
	if err = w.pool.QueryRow(ctx, `SELECT count(*) FROM feedback_attachments`).Scan(&count); err != nil || count != 1 {
		t.Fatal("expired orphan retained", count, err)
	}
}

func TestAttachmentBrowserAdmissionAndDelivery(t *testing.T) {
	w := fixture(t)
	s := &Server{Store: w.s, Sessions: sessions{}, AllowedOrigins: []string{"https://sumi.test"}}
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)
	upload := func(origin string) *httptest.ResponseRecorder {
		var body bytes.Buffer
		form := multipart.NewWriter(&body)
		part, err := form.CreateFormFile("file", "screen.png")
		if err != nil {
			t.Fatal(err)
		}
		if _, err = part.Write(pngEvidence); err != nil {
			t.Fatal(err)
		}
		if err = form.Close(); err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest("POST", "/feedback/attachments", &body)
		r.Header.Set("Content-Type", form.FormDataContentType())
		r.Header.Set("Origin", origin)
		r.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: w.human.ID})
		res := httptest.NewRecorder()
		mux.ServeHTTP(res, r)
		return res
	}
	if res := upload("https://evil.test"); res.Code != 403 {
		t.Fatal("foreign origin upload", res.Code)
	}
	s.Sessions = sessions{revoked: true}
	if res := upload("https://sumi.test"); res.Code != 401 {
		t.Fatal("revoked upload", res.Code)
	}
	s.Sessions = sessions{}
	res := upload("https://sumi.test")
	if res.Code != 200 {
		t.Fatal(res.Code, res.Body.String())
	}
	var response struct {
		Attachment Attachment `json:"attachment"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", response.Attachment.URL, nil)
	r.Header.Set("Range", "bytes=0-7")
	r.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: w.human.ID})
	res = httptest.NewRecorder()
	mux.ServeHTTP(res, r)
	if res.Code != 206 || !bytes.Equal(res.Body.Bytes(), pngEvidence[:8]) || res.Header().Get("X-Content-Type-Options") != "nosniff" || res.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatal("invalid private media delivery", res.Code, res.Header())
	}
}
