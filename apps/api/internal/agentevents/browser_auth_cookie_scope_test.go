package agentevents

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestBrowserCSRFJarCoversApplicationRoutesAndMigratesLegacyCookie(t *testing.T) {
	server, _ := newTestBrowserAuthServer(t, &fakeFirebaseVerifier{}, &fakeBindingResolver{})
	jar, _ := cookiejar.New(nil)
	origin, _ := url.Parse(browserAuthTestOrigin + "/auth/csrf")
	old := base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("a", csrfTokenBytes)))
	// A safe refresh removes legacy path cookies; writes still reject duplicates.
	jar.SetCookies(origin, []*http.Cookie{{Name: BrowserCSRFCookie, Value: old, Path: "/auth", Secure: true}, {Name: BrowserCSRFCookie, Value: old, Path: "/", Secure: true}})
	request := httptest.NewRequest(http.MethodGet, origin.String(), nil)
	for _, c := range jar.Cookies(origin) {
		request.AddCookie(c)
	}
	response := httptest.NewRecorder()
	server.serveCSRF(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("refresh: %d", response.Code)
	}
	jar.SetCookies(origin, response.Result().Cookies())
	var body struct {
		Token string `json:"csrf_token"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/auth/flows", "/api/model-connections/api", "/api/model-connections/chatgpt/login", "/feedback/threads", "/messaging/channels"} {
		endpoint, _ := url.Parse(browserAuthTestOrigin + path)
		req := httptest.NewRequest(http.MethodPost, endpoint.String(), nil)
		req.Header.Set("X-CSRF-Token", body.Token)
		for _, c := range jar.Cookies(endpoint) {
			req.AddCookie(c)
		}
		if len(req.CookiesNamed(BrowserCSRFCookie)) != 1 || !BrowserCSRFValid(req) {
			t.Fatalf("CSRF cookie missing/duplicated for %s", path)
		}
		req.Header.Set("X-CSRF-Token", old)
		if BrowserCSRFValid(req) {
			t.Fatalf("mismatch accepted for %s", path)
		}
		req.Header.Set("X-CSRF-Token", body.Token)
		req.AddCookie(&http.Cookie{Name: BrowserCSRFCookie, Value: body.Token})
		if BrowserCSRFValid(req) {
			t.Fatalf("duplicates accepted for %s", path)
		}
	}
	endpoint, _ := url.Parse(browserAuthTestOrigin + "/auth/logout")
	req := httptest.NewRequest(http.MethodPost, endpoint.String(), nil)
	req.Header.Set("Origin", browserAuthTestOrigin)
	req.Header.Set("X-CSRF-Token", body.Token)
	for _, c := range jar.Cookies(endpoint) {
		req.AddCookie(c)
	}
	logout := httptest.NewRecorder()
	server.serveLogout(logout, req)
	if logout.Code != http.StatusNoContent {
		t.Fatalf("logout: %d", logout.Code)
	}
	jar.SetCookies(endpoint, logout.Result().Cookies())
	for _, path := range []string{"/auth/flows", "/api/model-connections/api"} {
		u, _ := url.Parse(browserAuthTestOrigin + path)
		if len(jar.Cookies(u)) != 0 {
			t.Fatalf("cookie survived logout for %s", path)
		}
	}
}

func TestBrowserCSRFLogoutExpiresLegacyPathWithoutRefresh(t *testing.T) {
	server, _ := newTestBrowserAuthServer(t, &fakeFirebaseVerifier{}, &fakeBindingResolver{})
	jar, _ := cookiejar.New(nil)
	u, _ := url.Parse(browserAuthTestOrigin + "/auth/logout")
	token := base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("b", csrfTokenBytes)))
	jar.SetCookies(u, []*http.Cookie{{Name: BrowserCSRFCookie, Value: token, Path: "/auth", Secure: true}})
	req := httptest.NewRequest(http.MethodPost, u.String(), nil)
	req.Header.Set("Origin", browserAuthTestOrigin)
	req.Header.Set("X-CSRF-Token", token)
	for _, c := range jar.Cookies(u) {
		req.AddCookie(c)
	}
	out := httptest.NewRecorder()
	server.serveLogout(out, req)
	if out.Code != 204 {
		t.Fatalf("logout: %d", out.Code)
	}
	jar.SetCookies(u, out.Result().Cookies())
	if len(jar.Cookies(u)) != 0 {
		t.Fatal("legacy cookie survived logout")
	}
}
