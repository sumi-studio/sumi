package agentevents

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/directchat"
	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
)

// Real-filesvc browser-route e2e. Skipped unless FILEACCESS_E2E_URL and
// FILEACCESS_E2E_TOKEN are set (fixture provides both). Session verification
// runs the real HMAC verifier+revocation-store path; the direct-chat
// authorizer is the package's allow-double keyed on the test installation.
const e2eFilesPA = "019a0000-0000-7000-8000-0000000000a1"
const e2eFilesPAOther = "019a0000-0000-7000-8000-0000000000b2"
const e2eFilesScopeB = "019a00000000700080000000000000b2"

var e2eFilesSecret = []byte("e2e-browser-session-secret-32-bytes!")

func e2eFileServer(t *testing.T) (*HMACUserSessionVerifier, *http.ServeMux) {
	t.Helper()
	rawURL, token := os.Getenv("FILEACCESS_E2E_URL"), os.Getenv("FILEACCESS_E2E_TOKEN")
	if rawURL == "" || token == "" {
		t.Skip("FILEACCESS_E2E_URL/FILEACCESS_E2E_TOKEN unset; skipping real-filesvc e2e")
	}
	revocations := newTestBrowserSessionRevocationStore()
	verifier, err := NewHMACUserSessionVerifier(e2eFilesSecret, "", revocations)
	if err != nil {
		t.Fatal(err)
	}
	client, err := fileaccess.NewClient(rawURL, token)
	if err != nil {
		t.Fatal(err)
	}
	s := NewBrowserServer(verifier, nil, nil)
	s.Authorizer = allowDirectChatAuthorizer{}
	s.SetLifecycleFence(directchat.NewLifecycleFence())
	s.Files = client
	mux := http.NewServeMux()
	s.RegisterFileRoutes(mux)
	return verifier, mux
}

func e2eCookie(t *testing.T, v *HMACUserSessionVerifier, paid string) *http.Cookie {
	t.Helper()
	signed, err := v.IssueSession(context.Background(), UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-" + paid[len(paid)-4:],
		PersonalityAgentID: paid,
	}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return &http.Cookie{Name: BrowserSessionCookie, Value: signed}
}

func e2eDo(t *testing.T, mux *http.ServeMux, cookie *http.Cookie, method, target string, headers map[string]string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, target, body)
	r.AddCookie(cookie)
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	return w
}

func TestE2EBrowserFilesRealFilesvc(t *testing.T) {
	verifier, mux := e2eFileServer(t)
	cookieA := e2eCookie(t, verifier, e2eFilesPA)
	cookieB := e2eCookie(t, verifier, e2eFilesPAOther)
	q := "installation_id=" + testDirectChatInstallationID + "&authority_epoch=1"

	// Best-effort pre-clean so a crashed earlier run cannot wedge the
	// create-only write.
	for _, p := range []string{"e2e-web/hello.txt", "e2e-web"} {
		e2eDo(t, mux, cookieA, http.MethodDelete, "/files/remove?"+q+"&path="+url.QueryEscape(p),
			map[string]string{"If-Version": "any"}, nil)
	}

	// mkdir + write as person A.
	w := e2eDo(t, mux, cookieA, http.MethodPost, "/files/mkdir?"+q, map[string]string{"Content-Type": "application/json"}, strings.NewReader(`{"path":"e2e-web"}`))
	if w.Code != 200 {
		t.Fatalf("mkdir: %d %s", w.Code, w.Body.String())
	}
	w = e2eDo(t, mux, cookieA, http.MethodPut, "/files/write?"+q+"&path="+url.QueryEscape("e2e-web/hello.txt"),
		map[string]string{"If-Version": "none"}, strings.NewReader("from person A"))
	if w.Code != 200 {
		t.Fatalf("write: %d %s", w.Code, w.Body.String())
	}
	var wr struct {
		Version int64 `json:"version"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &wr); err != nil || wr.Version < 1 {
		t.Fatalf("write response missing version: %s", w.Body.String())
	}

	// Read back + version headers survive the proxy.
	w = e2eDo(t, mux, cookieA, http.MethodGet, "/files/read?"+q+"&path="+url.QueryEscape("e2e-web/hello.txt"), nil, nil)
	if w.Code != 200 || w.Body.String() != "from person A" {
		t.Fatalf("read: %d %q", w.Code, w.Body.String())
	}
	if w.Header().Get("X-File-Version") == "" {
		t.Fatal("X-File-Version not proxied")
	}

	// Another signed-in person: same request shape, different session —
	// the derived scope differs, so A's file is simply absent (404),
	// never visible.
	w = e2eDo(t, mux, cookieB, http.MethodGet, "/files/read?"+q+"&path="+url.QueryEscape("e2e-web/hello.txt"), nil, nil)
	if w.Code != 404 {
		t.Fatalf("person B read of A's file must 404 (foreign scope), got %d %s", w.Code, w.Body.String())
	}
	// And no client parameter can steer A's session into B's scope.
	w = e2eDo(t, mux, cookieA, http.MethodGet, "/files/read?"+q+"&path="+url.QueryEscape("e2e-web/hello.txt")+"&scope="+e2eFilesScopeB, nil, nil)
	if w.Code != 200 || w.Body.String() != "from person A" {
		t.Fatalf("scope param must be ignored: %d %q", w.Code, w.Body.String())
	}

	// Real traversal: filesvc answers 403 escape_denied through the proxy —
	// its openat2-beneath check is the authority.
	w = e2eDo(t, mux, cookieA, http.MethodGet, "/files/read?"+q+"&path=%2e%2e%2f..%2fescape.txt", nil, nil)
	if w.Code != 403 && w.Code != 400 {
		t.Fatalf("encoded traversal must be refused, got %d %s", w.Code, w.Body.String())
	}

	// A CAS write at the minted version succeeds; replaying that same version
	// after the file moved on is a real 409, not a silent overwrite.
	w = e2eDo(t, mux, cookieA, http.MethodPut, "/files/write?"+q+"&path="+url.QueryEscape("e2e-web/hello.txt"),
		map[string]string{"If-Version": strconv.FormatInt(wr.Version, 10)}, strings.NewReader("v2"))
	if w.Code != 200 {
		t.Fatalf("CAS overwrite: %d %s", w.Code, w.Body.String())
	}
	w = e2eDo(t, mux, cookieA, http.MethodPut, "/files/write?"+q+"&path="+url.QueryEscape("e2e-web/hello.txt"),
		map[string]string{"If-Version": strconv.FormatInt(wr.Version, 10)}, strings.NewReader("stale write"))
	if w.Code != 409 {
		t.Fatalf("stale CAS: %d %s", w.Code, w.Body.String())
	}

	// The shared-scope proof: the Core effect for the same persona reads the
	// file the person just wrote, writes its own reply, and the person's
	// session reads that reply — one canonical scope, both faces.
	fx := fileaccess.FileEffects(mustE2EClient(t))
	out, err := fx[fileaccess.ToolRead].Apply(context.Background(), nil, e2eFilesPA, "e2e:tool:0",
		map[string]any{"path": "e2e-web/hello.txt"})
	if err != nil {
		t.Fatalf("secretary read of person write: %v", err)
	}
	if out["content_text"] != "v2" {
		t.Fatalf("secretary saw %v", out)
	}
	if _, err = fx[fileaccess.ToolWrite].Apply(context.Background(), nil, e2eFilesPA, "e2e:tool:1",
		map[string]any{"path": "e2e-web/reply.txt", "content_text": "from secretary"}); err != nil {
		t.Fatalf("secretary write: %v", err)
	}
	w = e2eDo(t, mux, cookieA, http.MethodGet, "/files/read?"+q+"&path="+url.QueryEscape("e2e-web/reply.txt"), nil, nil)
	if w.Code != 200 || w.Body.String() != "from secretary" {
		t.Fatalf("person read of secretary write: %d %q", w.Code, w.Body.String())
	}

	// Cleanup through the same route.
	for _, p := range []string{"e2e-web/hello.txt", "e2e-web/reply.txt", "e2e-web"} {
		w = e2eDo(t, mux, cookieA, http.MethodDelete, "/files/remove?"+q+"&path="+url.QueryEscape(p),
			map[string]string{"If-Version": "any"}, nil)
		if w.Code != 200 {
			t.Fatalf("cleanup %s: %d %s", p, w.Code, w.Body.String())
		}
	}
}

func mustE2EClient(t *testing.T) *fileaccess.Client {
	t.Helper()
	c, err := fileaccess.NewClient(os.Getenv("FILEACCESS_E2E_URL"), os.Getenv("FILEACCESS_E2E_TOKEN"))
	if err != nil {
		t.Fatal(err)
	}
	return c
}
