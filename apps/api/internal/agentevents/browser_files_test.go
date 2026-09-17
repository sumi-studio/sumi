package agentevents

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

const filesPA = "018f47a2-9b3c-7def-8abc-0123456789ab"
const filesScope = "018f47a29b3c7def8abc0123456789ab"

// fakeFileBackend records what the route asked the transport for and answers
// with a canned upstream response — the route's job is deriving the right
// scope and forwarding a whitelisted request, so the fake asserts exactly
// that boundary.
type fakeFileBackend struct {
	scope    string
	op       string
	method   string
	query    url.Values
	ifVer    string
	opKey    string
	body     []byte
	fail     bool
	response *http.Response
}

func (f *fakeFileBackend) ProxyOp(_ context.Context, scope, op, method string, query url.Values, headers http.Header, body io.Reader) (*http.Response, error) {
	f.scope, f.op, f.method = scope, op, method
	f.query = query
	f.ifVer = headers.Get("If-Version")
	f.opKey = headers.Get("X-Idempotency-Key")
	if body != nil {
		f.body, _ = io.ReadAll(body)
	}
	if f.fail {
		return nil, fmt.Errorf("dial: connection refused")
	}
	return f.response, nil
}

func cannedFileResponse(status int, headers map[string]string, body string) *http.Response {
	resp := &http.Response{
		StatusCode: status,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
	for k, v := range headers {
		resp.Header.Set(k, v)
	}
	return resp
}

func newFileBrowserServer(t *testing.T, backend *fakeFileBackend) *BrowserServer {
	t.Helper()
	s := newAuthorizedBrowserServer(&fakeSessionVerifier{personalityAgentID: filesPA}, nil, nil)
	s.Files = backend
	return s
}

func fileRequest(t *testing.T, s *BrowserServer, method, target string, headers map[string]string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, target, body)
	r.AddCookie(&http.Cookie{Name: BrowserSessionCookie, Value: "fixture"})
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	mux := http.NewServeMux()
	s.RegisterFileRoutes(mux)
	mux.ServeHTTP(w, r)
	return w
}

const fileScopeQuery = "installation_id=" + testDirectChatInstallationID + "&authority_epoch=1"

func TestBrowserFilesScopeDerivedFromSession(t *testing.T) {
	backend := &fakeFileBackend{response: cannedFileResponse(200, map[string]string{"Content-Type": "application/json"}, `{"entries":[],"next_cursor":""}`)}
	s := newFileBrowserServer(t, backend)
	w := fileRequest(t, s, http.MethodGet, "/files/list?"+fileScopeQuery+"&path=sub", nil, nil)
	if w.Code != 200 {
		t.Fatalf("list: %d %s", w.Code, w.Body.String())
	}
	if backend.scope != filesScope || backend.op != "list" || backend.method != http.MethodGet {
		t.Fatalf("backend saw scope=%q op=%q method=%q", backend.scope, backend.op, backend.method)
	}
	if backend.query.Get("path") != "sub" {
		t.Fatalf("path not forwarded: %v", backend.query)
	}
	// Authority params must not leak upstream.
	if backend.query.Get("installation_id") != "" || backend.query.Get("authority_epoch") != "" {
		t.Fatalf("authority params leaked upstream: %v", backend.query)
	}
}

func TestBrowserFilesCallerCannotChooseScope(t *testing.T) {
	backend := &fakeFileBackend{response: cannedFileResponse(200, nil, "{}")}
	s := newFileBrowserServer(t, backend)
	// Every scope-shaping attempt is ignored: the backend must still see the
	// session-derived scope and nothing else.
	for _, extra := range []string{
		"&scope=00000000000000000000000000000000",
		"&persona_id=00000000-0000-7000-8000-000000000000",
		"&paid=00000000-0000-7000-8000-000000000000",
		"&scope=",
	} {
		backend.scope = ""
		w := fileRequest(t, s, http.MethodGet, "/files/stat?"+fileScopeQuery+"&path=a"+extra, nil, nil)
		if w.Code != 200 {
			t.Fatalf("%s: %d %s", extra, w.Code, w.Body.String())
		}
		if backend.scope != filesScope {
			t.Fatalf("%s: backend saw scope %q", extra, backend.scope)
		}
	}
}

func TestBrowserFilesAuthBoundary(t *testing.T) {
	backend := &fakeFileBackend{response: cannedFileResponse(200, nil, "{}")}
	s := newFileBrowserServer(t, backend)
	// No cookie -> 401.
	r := httptest.NewRequest(http.MethodGet, "/files/list?"+fileScopeQuery, nil)
	w := httptest.NewRecorder()
	mux := http.NewServeMux()
	s.RegisterFileRoutes(mux)
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("no cookie: %d", w.Code)
	}
	// Malformed scope params -> 400 invalid_scope; wrong epoch -> 403.
	for _, tc := range []struct {
		target string
		want   int
	}{
		{"/files/list", 400},
		{"/files/list?installation_id=" + testDirectChatInstallationID, 400},
		{"/files/list?authority_epoch=1", 400},
		{"/files/list?installation_id=not-a-uuid&authority_epoch=1", 400},
		{"/files/list?installation_id=" + testDirectChatInstallationID + "&authority_epoch=0", 400},
		{"/files/list?installation_id=" + testDirectChatInstallationID + "&authority_epoch=1&authority_epoch=1", 400},
		{"/files/list?installation_id=" + testDirectChatInstallationID + "&authority_epoch=2", 403},
	} {
		backend.scope = ""
		w = fileRequest(t, s, http.MethodGet, tc.target, nil, nil)
		if w.Code != tc.want {
			t.Fatalf("%s: got %d want %d", tc.target, w.Code, tc.want)
		}
		if backend.scope != "" {
			t.Fatalf("%s reached the file backend", tc.target)
		}
	}
	// Revoked session -> 401/403 (verifier rejects).
	s.Sessions.(*fakeSessionVerifier).setReject(true)
	w = fileRequest(t, s, http.MethodGet, "/files/list?"+fileScopeQuery, nil, nil)
	if w.Code == 200 {
		t.Fatal("revoked session still read files")
	}
	s.Sessions.(*fakeSessionVerifier).setReject(false)
}

func TestBrowserFilesWriteAndReadForward(t *testing.T) {
	backend := &fakeFileBackend{response: cannedFileResponse(200, map[string]string{
		"Content-Type": "application/json",
	}, `{"version":7}`)}
	s := newFileBrowserServer(t, backend)

	// Write without If-Version is refused at the route.
	w := fileRequest(t, s, http.MethodPut, "/files/write?"+fileScopeQuery+"&path=a.txt", nil, strings.NewReader("data"))
	if w.Code != 400 {
		t.Fatalf("missing If-Version: %d", w.Code)
	}
	w = fileRequest(t, s, http.MethodPut, "/files/write?"+fileScopeQuery+"&path=a.txt",
		map[string]string{"If-Version": "none"}, strings.NewReader("data"))
	if w.Code != 200 || backend.ifVer != "none" || string(backend.body) != "data" {
		t.Fatalf("write forward: %d ifv=%q body=%q", w.Code, backend.ifVer, backend.body)
	}

	// Read preserves the filesvc version/change headers.
	backend.response = cannedFileResponse(200, map[string]string{
		"Content-Type": "application/octet-stream", "X-File-Version": "7", "X-External-Change": "true",
	}, "bytes")
	w = fileRequest(t, s, http.MethodGet, "/files/read?"+fileScopeQuery+"&path=a.txt&offset=3&len=9", nil, nil)
	if w.Code != 200 || w.Header().Get("X-File-Version") != "7" || w.Header().Get("X-External-Change") != "true" || w.Body.String() != "bytes" {
		t.Fatalf("read forward: %d %v %q", w.Code, w.Header(), w.Body.String())
	}
	if backend.query.Get("offset") != "3" || backend.query.Get("len") != "9" {
		t.Fatalf("read range not forwarded: %v", backend.query)
	}
}

func TestBrowserFilesTraversalAndServiceErrorsPassThrough(t *testing.T) {
	// Traversal is forwarded verbatim; filesvc's authoritative check answers.
	backend := &fakeFileBackend{response: cannedFileResponse(400, map[string]string{"Content-Type": "application/json"}, `{"error":"escapes scope","code":"bad_path"}`)}
	s := newFileBrowserServer(t, backend)
	w := fileRequest(t, s, http.MethodGet, "/files/read?"+fileScopeQuery+"&path="+url.QueryEscape("../escape"), nil, nil)
	if w.Code != 400 || !strings.Contains(w.Body.String(), "bad_path") {
		t.Fatalf("traversal must surface filesvc's 400: %d %s", w.Code, w.Body.String())
	}
	if backend.query.Get("path") != "../escape" {
		t.Fatalf("path must be forwarded verbatim for filesvc to judge: %q", backend.query.Get("path"))
	}
	// Encoded traversal arrives decoded-but-verbatim too.
	backend.scope = ""
	w = fileRequest(t, s, http.MethodGet, "/files/read?"+fileScopeQuery+"&path=%2e%2e%2f%2e%2e%2fetc", nil, nil)
	if backend.query.Get("path") != "../../etc" {
		t.Fatalf("encoded traversal mangled: %q", backend.query.Get("path"))
	}
	// A filesvc outage is a truthful 502, never an auth error.
	backend.fail = true
	w = fileRequest(t, s, http.MethodGet, "/files/list?"+fileScopeQuery, nil, nil)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("upstream outage: %d want 502", w.Code)
	}
}

func TestBrowserFilesUnconfiguredFailsClosed(t *testing.T) {
	s := newAuthorizedBrowserServer(&fakeSessionVerifier{personalityAgentID: filesPA}, nil, nil)
	// Routes unregistered when Files is nil in production wiring; if a caller
	// still registers them, they fail closed.
	s.RegisterFileRoutes(http.NewServeMux())
	backend := s.Files
	if backend != nil {
		t.Fatal("unconfigured backend present")
	}
	w := fileRequest(t, s, http.MethodGet, "/files/list?"+fileScopeQuery, nil, nil)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil Files must fail closed: %d", w.Code)
	}
}

func TestBrowserFilesRemoveForwardsIfVersion(t *testing.T) {
	backend := &fakeFileBackend{response: cannedFileResponse(200, nil, `{"removed":true}`)}
	s := newFileBrowserServer(t, backend)
	w := fileRequest(t, s, http.MethodDelete, "/files/remove?"+fileScopeQuery+"&path=a.txt",
		map[string]string{"If-Version": "7"}, nil)
	if w.Code != 200 || backend.op != "remove" || backend.method != http.MethodDelete || backend.ifVer != "7" {
		t.Fatalf("remove forward: %d %v", w.Code, backend)
	}
}

// A browser-chosen operation key is namespaced under "br:" upstream so it
// can never collide with the Core ledger's derived identity, and malformed
// or oversized keys are refused at the route rather than forwarded.
func TestBrowserFilesIdempotencyKeyNamespaced(t *testing.T) {
	backend := &fakeFileBackend{response: cannedFileResponse(200, nil, `{"version":1}`)}
	s := newFileBrowserServer(t, backend)
	w := fileRequest(t, s, http.MethodPut, "/files/write?"+fileScopeQuery+"&path=k.txt",
		map[string]string{"If-Version": "none", "X-Idempotency-Key": "my-key"},
		strings.NewReader("data"))
	if w.Code != 200 || backend.opKey != "br:my-key" {
		t.Fatalf("namespaced key forward: %d key=%q", w.Code, backend.opKey)
	}
	// No key → no upstream header.
	backend.opKey = "unset"
	w = fileRequest(t, s, http.MethodPut, "/files/write?"+fileScopeQuery+"&path=k.txt",
		map[string]string{"If-Version": "none"}, strings.NewReader("data"))
	if w.Code != 200 || backend.opKey != "" {
		t.Fatalf("unkeyed write must not forward a key: %d key=%q", w.Code, backend.opKey)
	}
	// Bad keys are refused without touching the backend.
	backend.opKey = "unset"
	backend.scope = "unset"
	w = fileRequest(t, s, http.MethodPut, "/files/write?"+fileScopeQuery+"&path=k.txt",
		map[string]string{"If-Version": "none", "X-Idempotency-Key": "bad key with spaces"},
		strings.NewReader("data"))
	if w.Code != http.StatusBadRequest || backend.scope == filesScope {
		t.Fatalf("malformed key must be refused at the route: %d", w.Code)
	}
	// Reads never carry a key even if the client sends one.
	backend.opKey = "unset"
	w = fileRequest(t, s, http.MethodGet, "/files/read?"+fileScopeQuery+"&path=k.txt",
		map[string]string{"X-Idempotency-Key": "sneaky"}, nil)
	if backend.opKey != "" {
		t.Fatalf("read must not forward a key, got %q", backend.opKey)
	}
}
