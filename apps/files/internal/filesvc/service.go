package filesvc

import (
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// VersionStore is the persistence surface the service needs — *Store satisfies
// it against real PG; tests substitute a fake.
type VersionStore interface {
	// The fn callbacks report whether the filesystem commit point was
	// reached — an error with committed=true means "landed but
	// unobserved" and the intent must be kept for reconciliation, not
	// dropped as a rejection (F-RA-5/f120).
	WithWrite(ctx context.Context, scope, path, op string, iv IfVersion, expectSHA string, probe FPProbe, fn func(intent) (FileInfo, bool, error)) (int64, FileInfo, error)
	Rename(ctx context.Context, scope, from, to string, iv IfVersion, casProbe, fromProbe FPProbe, fn func(intent) (FileInfo, bool, error)) (int64, FileInfo, error)
	Remove(ctx context.Context, scope, path string, iv IfVersion, probe FPProbe, fn func(intent) (bool, error)) error
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
	r.sweepStaging()
	return &Service{root: r, store: store, tokens: tokens}, nil
}

// probe returns a filesystem probe the store calls under the row lock.
// It reports the live FileInfo (kind + fingerprint) so the store can
// record source-kind evidence for later settlement; ErrNotFound means
// "absent", any other error is unverifiable. The fingerprint is of the
// object AT the path (lstat — a symlink itself, not its target) because
// it records what an effect may displace.
func (s *Service) probe(scope, path string) FPProbe {
	return func() (FileInfo, bool, error) {
		info, err := s.root.lstat(scope, path)
		if errors.Is(err, ErrNotFound) || errors.Is(err, ErrNotDir) {
			return FileInfo{}, false, nil
		}
		if err != nil {
			return FileInfo{}, false, err
		}
		return info, true, nil
	}
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
		// A deposed store (writer lock lost) can serve nothing mutable;
		// report it so supervision/routing sees the fenced state instead
		// of a healthy-looking zombie that 503s every write (f107).
		if d, ok := s.store.(interface{ Deposed() bool }); ok && d.Deposed() {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("deposed"))
			return
		}
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
	if s.root.requireMount && op != "changes" {
		if err := s.root.checkMount(); err != nil {
			if errors.Is(err, ErrMountPolicy) {
				writeErr(w, 503, "mount_policy",
					"canonical mount does not satisfy the freshness policy")
			} else {
				writeErr(w, 503, "mount_unavailable",
					"canonical namespace root is not mounted")
			}
			return
		}
	}
	q := r.URL.Query()
	// Every caller-supplied path is normalized once at the boundary into
	// the canonical form the store uses for keys and intents: "./x",
	// "/x", and "a//b" name the same logical filesystem object (f121).
	path, perr := relPath(q.Get("path"))
	if perr != nil {
		s.mapErr(w, perr)
		return
	}
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
	// A store error must not degrade to version:0/external_change:false —
	// that would report "clean" exactly when the record cannot be checked.
	ver, recordedFP, verr := s.store.ObservedVersion(r.Context(), scope, path)
	if verr != nil {
		writeErr(w, 503, "store_unavailable",
			"version store unavailable; safe to retry")
		return
	}
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
	var off, length int64 = 0, -1
	bad := false
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err != nil || n < 0 {
			bad = true
		} else {
			off = n
		}
	}
	if v := q.Get("len"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err != nil || n < 0 {
			bad = true
		} else {
			length = n
		}
	}
	if bad {
		writeErr(w, 400, "bad_range", "offset/len must be non-negative integers")
		return
	}
	f, info, err := s.root.open(scope, path, off)
	if err != nil {
		s.mapErr(w, err)
		return
	}
	defer f.Close()
	// Clamp to what exists; bound the copy to the file size so a huge len
	// can never drive allocation or over-read.
	n := info.Size - off
	if n < 0 {
		n = 0
	}
	if length >= 0 && length < n {
		n = length
	}
	ver, recordedFP, verr := s.store.ObservedVersion(r.Context(), scope, path)
	if verr != nil {
		writeErr(w, 503, "store_unavailable",
			"version store unavailable; safe to retry")
		return
	}
	changed := ver > 0 && recordedFP != "" && recordedFP != info.Fingerprint
	w.Header().Set("X-File-Version", strconv.FormatInt(ver, 10))
	if changed {
		w.Header().Set("X-External-Change", "true")
	}
	w.Header().Set("content-type", "application/octet-stream")
	w.Header().Set("content-length", strconv.FormatInt(n, 10))
	// If the backend wedges mid-read (object store down), close the file
	// under the copy so the request cannot hang holding the socket forever.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-r.Context().Done():
			f.Close()
		case <-done:
		}
	}()
	io.CopyN(w, f, n)
}

func (s *Service) handleWrite(w http.ResponseWriter, r *http.Request, scope, path string) {
	if path == "" || path == "/" {
		writeErr(w, 400, "bad_path", "write requires a file path")
		return
	}
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
	exclusive := iv.Mode == "none" || (iv.Mode == "eq" && iv.Version == 0)
	sum := sha256.Sum256(body)
	ver, _, err := s.store.WithWrite(r.Context(), scope, path, "write", iv,
		hex.EncodeToString(sum[:]),
		s.probe(scope, path),
		func(it intent) (FileInfo, bool, error) {
			return s.root.atomicWrite(scope, path, body, exclusive, it)
		})
	if err != nil {
		s.mapErr(w, err)
		return
	}
	writeJSON(w, map[string]any{"version": ver})
}

type renameReq struct {
	From      string          `json:"from"`
	To        string          `json:"to"`
	IfVersion json.RawMessage `json:"if_version"`
}

// parseIfVersionJSON accepts a JSON string ("any"|"none"|"<n>") or a JSON
// integer. Anything else — absent, null, float, bool — is a 400. Rename must
// never silently degrade a missing or malformed condition into "any".
func parseIfVersionJSON(raw json.RawMessage) (IfVersion, error) {
	if len(raw) == 0 {
		return IfVersion{}, errors.New("if_version is required (any|none|<n>)")
	}
	var str string
	if err := json.Unmarshal(raw, &str); err == nil {
		return parseIfVersion(str)
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		return IfVersion{Mode: "eq", Version: n}, nil
	}
	return IfVersion{}, errors.New("bad if_version")
}

func (s *Service) handleRename(w http.ResponseWriter, r *http.Request, scope string) {
	var rr renameReq
	if err := json.NewDecoder(r.Body).Decode(&rr); err != nil {
		writeErr(w, 400, "bad_request", err.Error())
		return
	}
	iv, err := parseIfVersionJSON(rr.IfVersion)
	if err != nil {
		writeErr(w, 400, "bad_if_version", err.Error())
		return
	}
	from, ferr := relPath(rr.From)
	to, terr := relPath(rr.To)
	if ferr != nil {
		s.mapErr(w, ferr)
		return
	}
	if terr != nil {
		s.mapErr(w, terr)
		return
	}
	// none / eq 0 = "destination must not exist" — enforced atomically by
	// renameat2(RENAME_NOREPLACE), not just by the version row.
	noReplace := iv.Mode == "none" || (iv.Mode == "eq" && iv.Version == 0)
	ver, _, err := s.store.Rename(r.Context(), scope, from, to, iv,
		s.probe(scope, to), s.probe(scope, from),
		func(it intent) (FileInfo, bool, error) {
			return s.root.rename(scope, from, to, noReplace, it)
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
	mpath, merr := relPath(body.Path)
	if merr != nil {
		s.mapErr(w, merr)
		return
	}
	ver, _, err := s.store.WithWrite(r.Context(), scope, mpath, "mkdir",
		IfVersion{Mode: "any"}, "dir",
		s.probe(scope, mpath),
		func(it intent) (FileInfo, bool, error) {
			return s.root.mkdir(scope, mpath)
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
	err = s.store.Remove(r.Context(), scope, path, iv, s.probe(scope, path),
		func(it intent) (bool, error) {
			return s.root.remove(scope, path, it)
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

// isStoreErr classifies database faults: deadlines, connection errors,
// severed pooled connections, and network errors all mean "the version
// store could not answer" — 503 store_unavailable, not a 500 (f58).
func isStoreErr(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	var ce *pgconn.ConnectError
	if errors.As(err, &ce) {
		return true
	}
	// pgx v5.7.5 reports a severed pooled conn as an unexported
	// connLockError("conn closed") — no sentinel exists, so match text.
	if strings.Contains(err.Error(), "conn closed") {
		return true
	}
	var pge *pgconn.PgError
	if errors.As(err, &pge) {
		// SQLSTATE class 08 = connection exception; 57P* = server-side
		// shutdown/drop/idle-timeout operator intervention.
		return strings.HasPrefix(pge.Code, "08") ||
			strings.HasPrefix(pge.Code, "57P")
	}
	return false
}

// StartReconciler runs the intent-journal reconciler against the real
// filesystem: at startup, after any apply-failure, and periodically. A
// mutation whose fs commit outlived its DB apply converges instead of
// diverging. No-op for stores without a journal (test fakes).
func (s *Service) StartReconciler(ctx context.Context) {
	st, ok := s.store.(interface {
		SetReconcile(StatFn, HashFn, func() error)
		SetReconcileView(func(context.Context) (ReconView, error))
		ReconcileLoop(context.Context)
	})
	if !ok {
		return
	}
	// Ops-side probes (declare-time evidence hashing); the pass-pinned
	// view below is what judgments are made under.
	st.SetReconcile(s.root.stat, s.root.hash, nil)
	// Each pass acquires one view: under require_mount the mount check runs
	// first, then the root descriptor is pinned for the whole pass, so an
	// unmount mid-drain answers ENOTCONN on the dead mount rather than
	// ENOENT on the bare directory left behind — "unverifiable", never
	// "absent" (f102/F-RA-1). Without require_mount the root is an
	// ordinary directory: pinning still binds the pass to that root inode
	// (a renamed-away root cannot masquerade as absence).
	st.SetReconcileView(func(ctx context.Context) (ReconView, error) {
		return s.root.pin(s.root.requireMount)
	})
	go st.ReconcileLoop(ctx)
}

func (s *Service) mapErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrExternalChange):
		writeErr(w, 409, "external_change",
			"path changed outside the service since the declared version")
	case errors.Is(err, ErrConflict):
		writeErr(w, 409, "version_conflict", err.Error())
	case errors.Is(err, ErrNoSuchFile), errors.Is(err, ErrNotFound):
		writeErr(w, 404, "not_found", "path not found")
	case errors.Is(err, ErrEscape):
		writeErr(w, 403, "escape_denied", "path escapes scope root")
	case errors.Is(err, ErrReserved):
		writeErr(w, 400, "reserved_name", "name is reserved for service staging")
	case errors.Is(err, ErrIsDir), errors.Is(err, ErrNotDir), errors.Is(err, ErrWrongKind):
		writeErr(w, 400, "wrong_kind", err.Error())
	case errors.Is(err, ErrAccess):
		writeErr(w, 403, "permission_denied", "filesystem denied the operation")
	case errors.Is(err, ErrNotEmpty):
		writeErr(w, 409, "dir_not_empty", err.Error())
	case errors.Is(err, ErrUnsettled):
		// The error message carries the unsettled path and, when the
		// filesystem could not be observed at all, the recorded stall
		// cause — uncertainty is reported, not smoothed over (f119).
		writeErr(w, 503, "pending_settlement", err.Error())
	case errors.Is(err, ErrUnavailable):
		writeErr(w, 503, "unavailable", "operation timed out; safe to retry")
	case isStoreErr(err):
		// All store faults share one retryable class — a PG outage must not
		// look like an internal bug (f58).
		writeErr(w, 503, "store_unavailable",
			"version store unavailable; safe to retry")
	default:
		// Never leak host paths or syscall details in error bodies.
		writeErr(w, 500, "internal", "internal error")
	}
}
