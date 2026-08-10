package agentevents

import (
	"crypto/subtle"
	"net/http"
)

// BrowserOriginAllowed applies the browser-facing exact-origin policy shared
// by every browser-authenticated surface (direct chat and messaging). It is
// exported so other surface packages enforce the identical policy instead of
// re-deriving it.
func BrowserOriginAllowed(r *http.Request, allowedOrigins []string) bool {
	return browserOriginAllowed(r, allowedOrigins)
}

// BrowserCSRFValid validates the double-submit token used by /auth mutations.
// It is shared with adjacent authenticated browser surfaces so they cannot
// accidentally weaken the auth boundary.
func BrowserCSRFValid(r *http.Request) bool {
	headers := r.Header.Values("X-CSRF-Token")
	cookies := r.CookiesNamed(BrowserCSRFCookie)
	if len(headers) != 1 || len(cookies) != 1 {
		return false
	}
	headerToken, cookieToken := headers[0], cookies[0].Value
	return validCSRFToken(headerToken) && validCSRFToken(cookieToken) &&
		subtle.ConstantTimeCompare([]byte(headerToken), []byte(cookieToken)) == 1
}

// browserOriginAllowed applies the browser-facing exact-origin policy shared by
// direct-chat HTTP and WebSocket entry points. Missing, duplicated, empty, and
// non-exact Origin values fail closed. An empty allowlist accepts nothing.
func browserOriginAllowed(r *http.Request, allowedOrigins []string) bool {
	origins := r.Header.Values("Origin")
	if len(origins) != 1 || origins[0] == "" {
		return false
	}
	for _, allowed := range allowedOrigins {
		if origins[0] == allowed {
			return true
		}
	}
	return false
}
