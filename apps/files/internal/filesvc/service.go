package filesvc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// VersionStore is the persistence surface the service needs — *Store satisfies
// it against real PG; tests substitute a fake.
type VersionStore interface {
	WithWrite(ctx context.Context, scope, path, op string, iv IfVersion, fn func() (FileInfo, error)) (int64, FileInfo, error)
	Rename(ctx context.Context, scope, from, to string, iv IfVersion, fn func() (FileInfo, error)) (int64, FileInfo, error)
	Remove(ctx context.Context, scope, path string, iv IfVersion, fn func() error) error
	ObservedVersion(ctx context.Context, scope, path string) (int64, string, error)
	Changes(ctx context.Context, scope string, since int64, limit int) ([]Event, error)
}

// Service implements the scoped file API over one canonical namespace root.
// See contracts/files-api.yaml for the wire contract.
type Service struct {
	root   *posixRoot
	store  VersionStore
	tokens map[string]map[string]bool // token -> allowed scopes ("*" = all)
}

// RequireMount makes every file op fail with 503 while the namespace root is
// not a live mountpoint. Without it a dead mount leaves the bare directory
// writable/readable and files silently land on the wrong filesystem.
func (s *Service) RequireMount() { s.root.requireMount = true }

func New(root *posixRoot, store VersionStore, tokens map[string]map[string]bool) *Service {
	return &Service{root: root, store: store, tokens: tokens}
}

// NewAt resolves and validates the namespace root, then builds the service.
func NewAt(root string, store VersionStore, tokens map[string]map[string]bool) (*Service, error) {
	r, err := newRoot(root)
	if err != nil {
		return nil, err
	}
	return &Service{root: r, store: store, tokens: tokens}, nil
}

func (s *Service) authorized(r *http.Request, scope string) bool {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		return false
	}
	tok := strings.TrimPrefix(auth, "Bearer ")
	scopes, ok := s.tokens[tok]
	if !ok {
		return false
	}
	return scopes["*"] || scopes[scope]
}

type errBody struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(errBody{Error: msg, Code: code})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("content-type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.Write([]byte("ok"))
		return
	}
	// /v1/files/{scope}/{op}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 4 || parts[0] != "v1" || parts[1] != "files" {
		writeErr(w, 404, "not_found", "unknown route")
		return
	}
	scope, op := parts[2], parts[3]
	if scope == "" || strings.Contains(scope, "..") || strings.ContainsAny(scope, "\\") {
		writeErr(w, 400, "bad_scope", "invalid scope")
		return
	}
	if !s.authorized(r, scope) {
		writeErr(w, 403, "forbidden", "token does not grant this scope")
		return
	}
	if s.root.requireMount && op != "changes" && !s.root.mounted() {
		writeErr(w, 503, "mount_unavailable",
			"canonical namespace root is not mounted")
		return
	}
	q := r.URL.Query()
	path := q.Get("path")
	switch {
	case op == "stat" && r.Method == "GET":
		s.handleStat(w, r, scope, path)
	case op == "list" && r.Method == "GET":
		s.handleList(w, r, scope, path, q)
	case op == "read" && r.Method == "GET":
		s.handleRead(w, r, scope, path, q)
	case op == "write" && r.Method == "PUT":
		s.handleWrite(w, r, scope, path)
	case op == "rename" && r.Method == "POST":
		s.handleRename(w, r, scope)
	case op == "mkdir" && r.Method == "POST":
		s.handleMkdir(w, r, scope)
	case op == "remove" && r.Method == "DELETE":
		s.handleRemove(w, r, scope, path, q)
	case op == "changes" && r.Method == "GET":
		s.handleChanges(w, r, scope, q)
	default:
		writeErr(w, 404, "not_found", "unknown op or method")
	}
}

func parseIfVersion(h string) (IfVersion, error) {
	switch h {
	case "any":
		return IfVersion{Mode: "any"}, nil
	case "none":
		return IfVersion{Mode: "none"}, nil
	case "":
		return IfVersion{}, errors.New("missing If-Version header (any|none|<n>)")
	default:
		n, err := strconv.ParseInt(h, 10, 64)
		if err != nil {
			return IfVersion{}, fmt.Errorf("bad If-Version %q", h)
		}
		return IfVersion{Mode: "eq", Version: n}, nil
	}
}

func (s *Service) handleStat(w http.ResponseWriter, r *http.Request, scope, path string) {
	info, err := s.root.stat(scope, path)
	if err != nil {
		s.mapErr(w, err)
		return
	}
	ver, recordedFP, _ := s.store.ObservedVersion(r.Context(), scope, path)
	changed := ver > 0 && recordedFP != "" && recordedFP != info.Fingerprint
	writeJSON(w, map[string]any{
		"kind":            info.Kind,
		"size":            info.Size,
		"mtime_ns":        info.MtimeNS,
		"version":         ver,
		"fingerprint":     info.Fingerprint,
		"external_change": changed,
	})
}

func (s *Service) handleList(w http.ResponseWriter, r *http.Request, scope, path string, q url.Values) {
	limit := 200
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}
	entries, next, err := s.root.list(scope, path, limit, q.Get("cursor"))
	if err != nil {
		s.mapErr(w, err)
		return
	}
	writeJSON(w, map[string]any{"entries": entries, "next_cursor": next})
}

func (s *Service) handleRead(w http.ResponseWriter, r *http.Request, scope, path string, q url.Values) {
	var off, length int64
	if v := q.Get("offset"); v != "" {
		off, _ = strconv.ParseInt(v, 10, 64)
	}
	if v := q.Get("len"); v != "" {
		length, _ = strconv.ParseInt(v, 10, 64)
	}
	data, info, err := s.root.read(scope, path, off, length)
	if err != nil {
		s.mapErr(w, err)
		return
	}
	ver, recordedFP, _ := s.store.ObservedVersion(r.Context(), scope, path)
	changed := ver > 0 && recordedFP != "" && recordedFP != info.Fingerprint
	w.Header().Set("X-File-Version", strconv.FormatInt(ver, 10))
	if changed {
		w.Header().Set("X-External-Change", "true")
	}
	w.Header().Set("content-type", "application/octet-stream")
	w.Write(data)
}

func (s *Service) handleWrite(w http.ResponseWriter, r *http.Request, scope, path string) {
	iv, err := parseIfVersion(r.Header.Get("If-Version"))
	if err != nil {
		writeErr(w, 400, "bad_if_version", err.Error())
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 256<<20))
	if err != nil {
		writeErr(w, 413, "too_large", "body exceeds service ceiling")
		return
	}
	ver, _, err := s.store.WithWrite(r.Context(), scope, path, "write", iv,
		func() (FileInfo, error) {
			if iv.Mode == "none" {
				if _, serr := s.root.stat(scope, path); serr == nil {
					return FileInfo{}, ErrConflict
				} else if !errors.Is(serr, ErrNotFound) {
					return FileInfo{}, serr
				}
			}
			return s.root.atomicWrite(scope, path, body)
		})
	if err != nil {
		s.mapErr(w, err)
		return
	}
	writeJSON(w, map[string]any{"version": ver})
}

type renameReq struct {
	From      string `json:"from"`
	To        string `json:"to"`
	IfVersion any    `json:"if_version"`
}

func (s *Service) handleRename(w http.ResponseWriter, r *http.Request, scope string) {
	var rr renameReq
	if err := json.NewDecoder(r.Body).Decode(&rr); err != nil {
		writeErr(w, 400, "bad_request", err.Error())
		return
	}
	iv, err := parseIfVersion(fmt.Sprint(rr.IfVersion))
	if err != nil {
		iv = IfVersion{Mode: "any"}
	}
	ver, _, err := s.store.Rename(r.Context(), scope, rr.From, rr.To, iv,
		func() (FileInfo, error) {
			return s.root.rename(scope, rr.From, rr.To)
		})
	if err != nil {
		s.mapErr(w, err)
		return
	}
	writeJSON(w, map[string]any{"version": ver})
}

func (s *Service) handleMkdir(w http.ResponseWriter, r *http.Request, scope string) {
	var body struct {
		Path string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "bad_request", err.Error())
		return
	}
	ver, _, err := s.store.WithWrite(r.Context(), scope, body.Path, "mkdir",
		IfVersion{Mode: "any"},
		func() (FileInfo, error) {
			return s.root.mkdir(scope, body.Path)
		})
	if err != nil {
		s.mapErr(w, err)
		return
	}
	writeJSON(w, map[string]any{"version": ver})
}

func (s *Service) handleRemove(w http.ResponseWriter, r *http.Request, scope, path string, q url.Values) {
	iv, err := parseIfVersion(r.Header.Get("If-Version"))
	if err != nil {
		writeErr(w, 400, "bad_if_version", err.Error())
		return
	}
	err = s.store.Remove(r.Context(), scope, path, iv, func() error {
		return s.root.remove(scope, path)
	})
	if err != nil {
		s.mapErr(w, err)
		return
	}
	writeJSON(w, map[string]any{"removed": true})
}

func (s *Service) handleChanges(w http.ResponseWriter, r *http.Request, scope string, q url.Values) {
	var since int64
	if v := q.Get("since"); v != "" {
		since, _ = strconv.ParseInt(v, 10, 64)
	}
	limit := 500
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 5000 {
			limit = n
		}
	}
	evs, err := s.store.Changes(r.Context(), scope, since, limit)
	if err != nil {
		s.mapErr(w, err)
		return
	}
	writeJSON(w, map[string]any{"events": evs})
}

func (s *Service) mapErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrConflict):
		writeErr(w, 409, "version_conflict", err.Error())
	case errors.Is(err, ErrNoSuchFile), errors.Is(err, ErrNotFound):
		writeErr(w, 404, "not_found", err.Error())
	case errors.Is(err, ErrEscape):
		writeErr(w, 403, "escape_denied", err.Error())
	case errors.Is(err, ErrIsDir), errors.Is(err, ErrNotDir):
		writeErr(w, 400, "wrong_kind", err.Error())
	default:
		writeErr(w, 500, "internal", err.Error())
	}
}
