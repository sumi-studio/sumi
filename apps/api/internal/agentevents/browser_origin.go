package agentevents

import "net/http"

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
