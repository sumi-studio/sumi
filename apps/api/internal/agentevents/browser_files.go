package agentevents

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// FileBackend is the narrow transport the browser file routes proxy
// through. The implementation (internal/fileaccess.Client) holds the
// internal filesvc credential; the routes only ever pass a server-derived
// scope, a whitelisted op, and a filtered parameter set.
type FileBackend interface {
	ProxyOp(ctx context.Context, scope, op, method string, query url.Values, headers http.Header, body io.Reader) (*http.Response, error)
}

// browserFileWriteMaxBytes bounds a browser write body independent of
// filesvc's own 256 MiB ceiling — the API should not stream arbitrary sizes
// through itself for this first slice.
const browserFileWriteMaxBytes = 32 << 20

// browserFileParams whitelists which client query parameters each op may
// forward to filesvc. installation_id/authority_epoch are consumed by
// authorization and deliberately never forwarded.
var browserFileParams = map[string]map[string]bool{
	"stat":   {"path": true},
	"list":   {"path": true, "limit": true, "cursor": true},
	"read":   {"path": true, "offset": true, "len": true},
	"write":  {"path": true},
	"remove": {"path": true},
}

// RegisterFileRoutes mounts the person-facing file routes. It is called only
// when a FileBackend is configured, so an unconfigured deployment exposes no
// file surface at all — discovery and executability agree by construction.
func (s *BrowserServer) RegisterFileRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /files/list", func(w http.ResponseWriter, r *http.Request) { s.serveFileOp(w, r, "list") })
	mux.HandleFunc("GET /files/stat", func(w http.ResponseWriter, r *http.Request) { s.serveFileOp(w, r, "stat") })
	mux.HandleFunc("GET /files/read", func(w http.ResponseWriter, r *http.Request) { s.serveFileOp(w, r, "read") })
	mux.HandleFunc("PUT /files/write", func(w http.ResponseWriter, r *http.Request) { s.serveFileOp(w, r, "write") })
	mux.HandleFunc("POST /files/mkdir", func(w http.ResponseWriter, r *http.Request) { s.serveFileOp(w, r, "mkdir") })
	mux.HandleFunc("DELETE /files/remove", func(w http.ResponseWriter, r *http.Request) { s.serveFileOp(w, r, "remove") })
}

// serveFileOp shares the exact session, Employer and AppInstallation
// boundary with history and the socket — and adds the lifecycle fence and
// authority-epoch check, so a revoked installation or a stale epoch cannot
// reach filesvc even for reads.
func (s *BrowserServer) serveFileOp(w http.ResponseWriter, r *http.Request, op string) {
	w.Header().Set("Cache-Control", "no-store")
	if s.Sessions == nil || s.Files == nil || s.Authorizer == nil || s.LifecycleFence == nil {
		http.Error(w, "files unavailable", http.StatusServiceUnavailable)
		return
	}
	if len(r.Header.Values("Origin")) > 0 && !s.checkOrigin(r) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return
	}
	cookie, err := uniqueBrowserSessionCookie(r)
	if err != nil {
		http.Error(w, "invalid session", http.StatusUnauthorized)
		return
	}
	claims, err := s.Sessions.VerifySession(r.Context(), cookie.Value)
	if err != nil {
		http.Error(w, "invalid session", http.StatusUnauthorized)
		return
	}
	scope, err := directChatScopeFromRequest(r)
	if err != nil {
		writeDirectChatInvalidScope(w)
		return
	}
	paid := claims.PersonalityAgentID
	fileScope, err := fileScopeForPAID(paid)
	if err != nil {
		http.Error(w, "files unavailable", http.StatusServiceUnavailable)
		return
	}
	upstreamQuery, upstreamHeaders, body, err := s.fileRequestParts(w, r, op)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	reachedFiles := false
	err = s.authorizeBrowserOperation(r.Context(), claims, scope, func() error {
		reachedFiles = true
		resp, uerr := s.Files.ProxyOp(r.Context(), fileScope, op, r.Method, upstreamQuery, upstreamHeaders, body)
		if uerr != nil {
			return uerr
		}
		defer resp.Body.Close()
		copyFileResponse(w, resp)
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrDirectChatAuthorizationUnavailable) {
			http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
			return
		}
		if reachedFiles {
			// filesvc was unreachable or its transport failed — a truthful
			// outage, not an authorization refusal.
			http.Error(w, "files unavailable", http.StatusBadGateway)
			return
		}
		http.Error(w, "not authorized", http.StatusForbidden)
	}
}

// fileRequestParts extracts the whitelisted upstream inputs for one op.
func (s *BrowserServer) fileRequestParts(w http.ResponseWriter, r *http.Request, op string) (url.Values, http.Header, io.Reader, error) {
	in := r.URL.Query()
	out := url.Values{}
	for k := range browserFileParams[op] {
		if vs, present := in[k]; present {
			if len(vs) != 1 {
				return nil, nil, nil, fmt.Errorf("duplicate %s", k)
			}
			if k == "path" {
				if err := checkBrowserFilePath(vs[0]); err != nil {
					return nil, nil, nil, err
				}
			}
			out[k] = vs
		}
	}
	if op != "list" && op != "mkdir" && out.Get("path") == "" {
		return nil, nil, nil, errors.New("path is required")
	}
	var headers http.Header
	var body io.Reader
	switch op {
	case "write", "remove":
		iv := r.Header.Get("If-Version")
		if iv == "" {
			return nil, nil, nil, errors.New("If-Version header required (any|none|<n>)")
		}
		headers = http.Header{"If-Version": {iv}}
		if op == "write" {
			// Bound before streaming: a request body larger than the slice
			// cap is rejected before the proxy opens the upstream stream.
			if r.ContentLength > browserFileWriteMaxBytes {
				return nil, nil, nil, fmt.Errorf("body exceeds %d bytes", browserFileWriteMaxBytes)
			}
			body = http.MaxBytesReader(w, r.Body, browserFileWriteMaxBytes)
		}
	case "mkdir":
		// mkdir's path travels in the JSON body, not the query; bound it and
		// pass through verbatim — filesvc validates the shape.
		body = http.MaxBytesReader(w, r.Body, 4096)
		headers = http.Header{"Content-Type": {"application/json"}}
	}
	return out, headers, body, nil
}

// checkBrowserFilePath is a cheap outer bound; filesvc's openat2-beneath
// resolution remains the authoritative traversal/escape check.
func checkBrowserFilePath(p string) error {
	if len(p) > 1024 {
		return errors.New("path too long")
	}
	return nil
}

// fileScopeForPAID derives the filesvc scope from the verified session's
// PAID — compact lower-hex UUID. A PAID that does not map is a server-side
// integrity failure, never a client error.
func fileScopeForPAID(paid string) (string, error) {
	scope := strings.ToLower(strings.ReplaceAll(paid, "-", ""))
	if len(scope) != 32 {
		return "", fmt.Errorf("paid does not map to a file scope")
	}
	for _, c := range scope {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", fmt.Errorf("paid does not map to a file scope")
		}
	}
	return scope, nil
}

// copyFileResponse streams the upstream response through, preserving the
// filesvc headers a client needs to do CAS and detect external change.
func copyFileResponse(w http.ResponseWriter, resp *http.Response) {
	h := w.Header()
	if v := resp.Header.Get("Content-Type"); v != "" {
		h.Set("Content-Type", v)
	}
	for _, k := range []string{"X-File-Version", "X-External-Change"} {
		if v := resp.Header.Get(k); v != "" {
			h.Set(k, v)
		}
	}
	if n := resp.Header.Get("Content-Length"); n != "" {
		h.Set("Content-Length", n)
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}
