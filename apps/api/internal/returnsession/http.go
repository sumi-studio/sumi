package returnsession

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
	"github.com/sumi-studio/sumi/apps/api/internal/portable"
)

// RoutePrefix is where the return-session routes live. A return URL is the
// session resource itself plus a fragment carrying the grant:
//
//	https://<api>/api/secretary-return/sessions/<session_id>#grant=<grant>
//
// The fragment is never sent in a request, so the grant stays out of access
// logs; the Local command presents it as a bearer token.
const RoutePrefix = "/api/secretary-return"

// OwnerProof is the trusted authentication adapter the account owner
// supplies. It returns the human and secretary a live, verified browser
// session owns for this request — never ids taken from the request body —
// and an error for an unproven, expired or revoked session.
type OwnerProof interface {
	OwnerClaims(ctx context.Context, r *http.Request) (Owner, error)
}

type Server struct {
	svc   *Service
	proof OwnerProof
	base  string
	logf  func(string, ...any)
	files *fileaccess.Client
}

// NewServer builds the routes over svc. publicBaseURL is the dedicated
// origin the Local command reaches this API at; return URLs are built from
// it, never from request headers. A path prefix is refused rather than
// minted: the receiver validates session paths anchored at RoutePrefix, so
// a prefixed base would produce URLs the Local command can never accept.
func NewServer(svc *Service, proof OwnerProof, publicBaseURL string) (*Server, error) {
	if proof == nil {
		return nil, errors.New("returnsession: an owner proof adapter is required")
	}
	u, err := url.Parse(publicBaseURL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		(u.Path != "" && u.Path != "/") ||
		!(u.Scheme == "https" || (u.Scheme == "http" && loopback(u.Hostname()))) {
		return nil, fmt.Errorf("returnsession: public base URL must be an https origin (or http on loopback) without path, query or fragment — the API needs its own origin, not a path prefix")
	}
	return &Server{svc: svc, proof: proof, base: strings.TrimRight(publicBaseURL, "/"), logf: log.Printf}, nil
}

// loopback reports a literal loopback host, the only place a return URL may
// use plain http.
func loopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST "+RoutePrefix+"/sessions", s.create)
	mux.HandleFunc("GET "+RoutePrefix+"/session", s.ownerSession)
	mux.HandleFunc("GET "+RoutePrefix+"/sessions/{session}", s.status)
	mux.HandleFunc("POST "+RoutePrefix+"/sessions/{session}/destination", s.destination)
	mux.HandleFunc("GET "+RoutePrefix+"/sessions/{session}/bundle", s.bundle)
	mux.HandleFunc("POST "+RoutePrefix+"/sessions/{session}/activated", s.activated)
	mux.HandleFunc("POST "+RoutePrefix+"/sessions/{session}/retired", s.retired)
	mux.HandleFunc("POST "+RoutePrefix+"/sessions/{session}/cancel", s.cancel)
	mux.HandleFunc("POST "+RoutePrefix+"/sessions/{session}/files-credential", s.filesCredential)
	mux.HandleFunc("GET "+RoutePrefix+"/sessions/{session}/files/{op}", s.grantFileOp)
	mux.HandleFunc("POST "+RoutePrefix+"/sessions/{session}/capture", s.captureEnsure)
	mux.HandleFunc("GET "+RoutePrefix+"/sessions/{session}/capture", s.captureView)
	mux.HandleFunc("DELETE "+RoutePrefix+"/sessions/{session}/capture", s.captureRelease)
	mux.HandleFunc("POST "+RoutePrefix+"/sessions/{session}/capture/retake", s.captureRetake)
	mux.HandleFunc("GET "+RoutePrefix+"/sessions/{session}/capture/entries", s.captureEntries)
	mux.HandleFunc("GET "+RoutePrefix+"/sessions/{session}/capture/read", s.captureRead)
	mux.HandleFunc("POST "+RoutePrefix+"/files/revoke", s.revokeFiles)
}

// FileRoutePrefix is where the scoped storage proxy mounts: file
// operations on a transferred persona's Cloud working store, authorized
// by the destination's minted storage credential — never by a scope taken
// from the request.
const FileRoutePrefix = "/api/secretary-files"

// SetFiles wires the file service client both file surfaces need: the
// grant's copy-read route (local mode) and the storage-credential proxy
// (cloud mode). Without it RegisterFileProxy mounts nothing.
func (s *Server) SetFiles(c *fileaccess.Client) { s.files = c }

// RegisterFileProxy mounts the durable storage-credential surface. Every
// request under FileRoutePrefix is a filesvc op: the bearer token
// resolves to its stored scope, which must equal the URL's scope — a
// token can never name a scope it was not minted for. Each method gets
// an explicit METHOD-prefixed registration: the edge route-parity
// contract inspects every mount and refuses methodless patterns.
func (s *Server) RegisterFileProxy(mux *http.ServeMux) {
	if s.files == nil {
		return
	}
	mux.HandleFunc("GET "+FileRoutePrefix+"/v1/files/{scope}/{op}", s.fileProxy)
	mux.HandleFunc("PUT "+FileRoutePrefix+"/v1/files/{scope}/{op}", s.fileProxy)
	mux.HandleFunc("POST "+FileRoutePrefix+"/v1/files/{scope}/{op}", s.fileProxy)
	mux.HandleFunc("DELETE "+FileRoutePrefix+"/v1/files/{scope}/{op}", s.fileProxy)
}

// tokenOps is the operation set a scoped storage credential may issue —
// the same allowlist the human and secretary file surfaces use. Service
// administration (freeze, changes) is never delegable.
var tokenOps = map[string]bool{
	"stat": true, "list": true, "read": true,
	"write": true, "mkdir": true, "remove": true,
}

// grantReadOps is what the mover's copy may issue under its return grant —
// enumeration and reads only; the copy never writes the source.
var grantReadOps = map[string]bool{"stat": true, "list": true, "read": true}

// ReturnURL is what the owner is shown for the Local command.
func (s *Server) ReturnURL(sessionID, grant string) string {
	return s.base + RoutePrefix + "/sessions/" + sessionID + "#grant=" + grant
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "malformed request body")
		return false
	}
	return true
}

func bearer(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		return "", false
	}
	return strings.TrimPrefix(h, "Bearer "), true
}

func (s *Server) owner(w http.ResponseWriter, r *http.Request) (Owner, bool) {
	o, err := s.proof.OwnerClaims(r.Context(), r)
	if err != nil || !o.valid() {
		writeError(w, http.StatusUnauthorized, "a signed-in Sumi Cloud session is required")
		return Owner{}, false
	}
	return o, true
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	o, ok := s.owner(w, r)
	if !ok {
		return
	}
	var body struct {
		FileMode string `json:"file_mode"`
	}
	if !decode(w, r, &body) {
		return
	}
	created, open, err := s.svc.Create(r.Context(), o, body.FileMode)
	if errors.Is(err, ErrOpenSession) {
		// The open session's status is what the owner decides a lost return
		// URL from; the grant is never repeated.
		v, verr := s.svc.reconciledView(r.Context(), open, false)
		if verr != nil {
			s.writeErr(w, verr, nil)
			return
		}
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "session_id": open, "session": v})
		return
	}
	if err != nil {
		s.writeErr(w, err, nil)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"return_url": s.ReturnURL(created.View.SessionID, created.Grant),
		"session":    created.View,
	})
}

func (s *Server) ownerSession(w http.ResponseWriter, r *http.Request) {
	o, ok := s.owner(w, r)
	if !ok {
		return
	}
	v, err := s.svc.ForOwner(r.Context(), o)
	if err != nil {
		s.writeErr(w, err, nil)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	grant, ok := bearer(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, ErrGrant.Error())
		return
	}
	v, err := s.svc.Status(r.Context(), r.PathValue("session"), grant)
	if err != nil {
		s.writeErr(w, err, &v)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

// destination takes the return URL for one Local placement and seals the
// source for it. A second placement is answered 409 while this secretary is
// still active here.
func (s *Server) destination(w http.ResponseWriter, r *http.Request) {
	grant, ok := bearer(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, ErrGrant.Error())
		return
	}
	var body Destination
	if !decode(w, r, &body) {
		return
	}
	v, err := s.svc.BindDestination(r.Context(), r.PathValue("session"), grant, body)
	if err != nil {
		s.writeErr(w, err, &v)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (s *Server) bundle(w http.ResponseWriter, r *http.Request) {
	grant, ok := bearer(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, ErrGrant.Error())
		return
	}
	v, err := s.svc.Download(r.Context(), r.PathValue("session"), grant, &startedWriter{w: w})
	if err != nil {
		s.writeErr(w, err, &v)
		return
	}
	// Download already wrote the view-independent bundle stream; nothing
	// left to answer.
}

// startedWriter sets the bundle content type on first write so a refusal
// before any byte can still answer JSON.
type startedWriter struct {
	w       http.ResponseWriter
	started bool
}

func (sw *startedWriter) Write(p []byte) (int, error) {
	if !sw.started {
		sw.started = true
		sw.w.Header().Set("Content-Type", "application/x-ndjson")
		sw.w.Header().Set("Cache-Control", "no-store")
	}
	return sw.w.Write(p)
}

type proofBody struct {
	Proof string `json:"proof"`
}

func (s *Server) activated(w http.ResponseWriter, r *http.Request) {
	grant, ok := bearer(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, ErrGrant.Error())
		return
	}
	var body struct {
		ActivateProof string `json:"activate_proof"`
	}
	if !decode(w, r, &body) {
		return
	}
	v, err := s.svc.ReportActivated(r.Context(), r.PathValue("session"), grant, body.ActivateProof)
	if err != nil {
		s.writeErr(w, err, &v)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (s *Server) retired(w http.ResponseWriter, r *http.Request) {
	grant, ok := bearer(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, ErrGrant.Error())
		return
	}
	var body struct {
		RetireProof string `json:"retire_proof"`
	}
	if !decode(w, r, &body) {
		return
	}
	v, err := s.svc.ReportRetired(r.Context(), r.PathValue("session"), grant, body.RetireProof)
	if err != nil {
		s.writeErr(w, err, &v)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("session")
	if grant, ok := bearer(r); ok {
		v, err := s.svc.CancelByGrant(r.Context(), id, grant)
		if err != nil {
			s.writeErr(w, err, &v)
			return
		}
		writeJSON(w, http.StatusOK, v)
		return
	}
	o, ok := s.owner(w, r)
	if !ok {
		return
	}
	v, err := s.svc.CancelByOwner(r.Context(), id, o)
	if err != nil {
		s.writeErr(w, err, &v)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// writeErr answers err, with the session view when one was produced so a
// refused report or a closed download still tells the caller where it
// stands.
func (s *Server) writeErr(w http.ResponseWriter, err error, v *View) {
	var pgErr *pgconn.PgError
	code := http.StatusInternalServerError
	switch {
	case errors.Is(err, ErrGrant):
		code = http.StatusUnauthorized
	case errors.Is(err, ErrNotFound):
		code = http.StatusNotFound
	case errors.Is(err, ErrNoSecretary), errors.Is(err, ErrNotOwned):
		code = http.StatusForbidden
	case errors.Is(err, ErrClosed), errors.Is(err, ErrExpired):
		code = http.StatusGone
	case errors.Is(err, ErrConflict), errors.Is(err, ErrOpenSession),
		errors.Is(err, ErrDestBound), errors.Is(err, ErrFilePolicyUndecided),
		errors.Is(err, ErrTerminalSessionsOpen), errors.Is(err, ErrTerminalQuiescePending),
		errors.Is(err, ErrScopeChanged), errors.Is(err, ErrCaptureReplaced),
		errors.Is(err, portable.ErrTransferConflict),
		errors.Is(err, portable.ErrPersonaExists), errors.Is(err, portable.ErrUnresolvedOperations):
		code = http.StatusConflict
	case errors.Is(err, ErrCaptureUnconfigured):
		code = http.StatusServiceUnavailable
	case errors.Is(err, ErrBadRequest), errors.Is(err, portable.ErrBadRequest), errors.Is(err, portable.ErrMissingProof):
		code = http.StatusBadRequest
	case errors.As(err, &pgErr) && (pgErr.Code == "23505" || pgErr.Code == "40P01" || pgErr.Code == "40001"):
		code = http.StatusConflict
	}
	msg := err.Error()
	// A filesvc verdict travels with its own status and message — a 422
	// refusal or 503 pending is a real upstream answer, not an internal
	// fault to be hidden behind 500.
	var se *fileaccess.ServiceError
	if errors.As(err, &se) && se.Status >= 400 && se.Status < 600 {
		code = se.Status
		if se.Message != "" {
			msg = se.Message
		}
	}
	if code == http.StatusInternalServerError {
		if s.logf != nil {
			s.logf("return session: %v", err)
		}
		msg = "internal error"
	}
	body := map[string]any{"error": msg}
	if errors.Is(err, ErrCaptureReplaced) {
		body["code"] = "capture_replaced"
	}
	if errors.Is(err, ErrTerminalSessionsOpen) {
		body["code"] = "terminal_sessions_open"
	}
	if errors.Is(err, ErrTerminalQuiescePending) {
		body["code"] = "terminal_quiesce_pending"
	}
	if errors.Is(err, ErrFilePolicyUndecided) {
		// A machine-readable marker: the web client renders the undecided
		// file-policy state from this instead of matching message text.
		body["code"] = "file_policy_undecided"
	}
	if v != nil && v.SessionID != "" {
		body["session"] = v
	}
	writeJSON(w, code, body)
}

// filesCredential mints (or rotates) the destination's scoped storage
// credential under a cloud-mode session's lineage — a real mutation, so
// POST. The token is returned once; the store keeps only its hash.
func (s *Server) filesCredential(w http.ResponseWriter, r *http.Request) {
	grant, ok := bearer(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, ErrGrant.Error())
		return
	}
	cred, err := s.svc.MintFileCredential(r.Context(), r.PathValue("session"), grant)
	if err != nil {
		s.writeErr(w, err, nil)
		return
	}
	writeJSON(w, http.StatusCreated, cred)
}

// grantFileOp is the mover's copy-read surface: stat/list/read on the
// sealed source workspace under the return grant. It exists only for
// local-mode sessions and only while sealed — the copy is the reason the
// grant reaches file content at all; the record-bundle grant alone never
// authorized file bytes.
func (s *Server) grantFileOp(w http.ResponseWriter, r *http.Request) {
	grant, ok := bearer(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, ErrGrant.Error())
		return
	}
	if s.files == nil {
		writeError(w, http.StatusServiceUnavailable, "file service is not configured")
		return
	}
	op := r.PathValue("op")
	if !grantReadOps[op] {
		writeError(w, http.StatusNotFound, "unknown file op")
		return
	}
	scope, err := s.svc.AuthorizeFileRead(r.Context(), r.PathValue("session"), grant)
	if err != nil {
		s.writeErr(w, err, nil)
		return
	}
	s.proxyOp(w, r, scope, op)
}

// revokeFiles is the owner's storage-credential revocation: every active
// file token for their secretary dies. Files are untouched — this ends
// access, not data.
func (s *Server) revokeFiles(w http.ResponseWriter, r *http.Request) {
	o, ok := s.owner(w, r)
	if !ok {
		return
	}
	n, err := s.svc.RevokePersonaFileTokens(r.Context(), o)
	if err != nil {
		s.writeErr(w, err, nil)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"revoked": n})
}

// fileProxy authorizes a presented storage credential and forwards the
// filesvc op under the service's internal credential. The URL's scope
// must equal the token's recorded scope — a token cannot widen itself.
func (s *Server) fileProxy(w http.ResponseWriter, r *http.Request) {
	scope, op := r.PathValue("scope"), r.PathValue("op")
	token, ok := bearer(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, ErrFileToken.Error())
		return
	}
	if !tokenOps[op] {
		writeError(w, http.StatusNotFound, "unknown file op")
		return
	}
	_, tokenScope, err := s.svc.AuthorizeFileToken(r.Context(), token)
	if err != nil {
		writeError(w, http.StatusUnauthorized, ErrFileToken.Error())
		return
	}
	if tokenScope != scope {
		writeError(w, http.StatusForbidden, "credential does not grant this scope")
		return
	}
	s.proxyOp(w, r, scope, op)
}

// proxyOp forwards one file operation upstream and streams the answer
// back, preserving the filesvc result (status, version and external-change
// headers, body) so the caller sees the store's own verdicts.
func (s *Server) proxyOp(w http.ResponseWriter, r *http.Request, scope, op string) {
	headers := http.Header{}
	for _, h := range []string{"If-Version", "X-Idempotency-Key", "Content-Type"} {
		if v := r.Header.Get(h); v != "" {
			headers.Set(h, v)
		}
	}
	resp, err := s.files.ProxyOp(r.Context(), scope, op, r.Method, r.URL.Query(), headers, r.Body)
	if err != nil {
		if s.logf != nil {
			s.logf("return file proxy %s %s: %v", scope, op, err)
		}
		writeError(w, http.StatusBadGateway, "file service unreachable; safe to retry")
		return
	}
	defer resp.Body.Close()
	for _, h := range []string{"Content-Type", "Content-Length", "X-File-Version", "X-External-Change"} {
		if v := resp.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// --- immutable capture surface --------------------------------------
//
// The local-mode copy binds the session to one durable manifest
// association; owner and epoch are derived from the authorized session
// row, never the request. Reads are bound to the persisted capture_id —
// the grant can never name a capture.

// captureEnsure is POST /capture: bind (idempotently) the session's
// capture association. A lost response or mover restart re-POSTs and
// gets the SAME binding — never a second manifest.
func (s *Server) captureEnsure(w http.ResponseWriter, r *http.Request) {
	grant, ok := bearer(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, ErrGrant.Error())
		return
	}
	b, err := s.svc.EnsureCapture(r.Context(), r.PathValue("session"), grant)
	if err != nil {
		s.writeErr(w, err, nil)
		return
	}
	writeJSON(w, http.StatusOK, b)
}

// captureView is GET /capture: the persisted association for recovery,
// 404 while unbound.
func (s *Server) captureView(w http.ResponseWriter, r *http.Request) {
	grant, ok := bearer(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, ErrGrant.Error())
		return
	}
	b, err := s.svc.CaptureView(r.Context(), r.PathValue("session"), grant)
	if err != nil {
		s.writeErr(w, err, nil)
		return
	}
	writeJSON(w, http.StatusOK, b)
}

// captureRetake is POST /capture/retake with {"expected_scope_id"}: a
// fresh coherent manifest when the bound capture loses a required
// object. A stale expectation answers the current binding; a changed
// scope identity is a refusal.
func (s *Server) captureRetake(w http.ResponseWriter, r *http.Request) {
	grant, ok := bearer(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, ErrGrant.Error())
		return
	}
	var body struct {
		ExpectedScopeID   string `json:"expected_scope_id"`
		ExpectedCaptureID string `json:"expected_capture_id"`
	}
	if !decode(w, r, &body) {
		return
	}
	b, err := s.svc.RetakeCapture(r.Context(), r.PathValue("session"), grant, body.ExpectedScopeID, body.ExpectedCaptureID)
	if err != nil {
		s.writeErr(w, err, nil)
		return
	}
	writeJSON(w, http.StatusOK, b)
}

// captureRelease is DELETE /capture: drop the binding and free the
// reservation. Idempotent.
func (s *Server) captureRelease(w http.ResponseWriter, r *http.Request) {
	grant, ok := bearer(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, ErrGrant.Error())
		return
	}
	if err := s.svc.ReleaseCapture(r.Context(), r.PathValue("session"), grant,
		r.URL.Query().Get("expected_capture_id")); err != nil {
		s.writeErr(w, err, nil)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"released": true})
}

// captureEntries proxies one manifest page of the PERSISTED binding.
func (s *Server) captureEntries(w http.ResponseWriter, r *http.Request) {
	grant, ok := bearer(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, ErrGrant.Error())
		return
	}
	if s.files == nil {
		writeError(w, http.StatusServiceUnavailable, "file service is not configured")
		return
	}
	q := r.URL.Query()
	cursor, err := strconv.ParseInt(q.Get("cursor"), 10, 64)
	if err != nil || cursor < -1 {
		writeError(w, http.StatusBadRequest, "cursor must be an integer >= -1")
		return
	}
	limit := 500
	if v := q.Get("limit"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 1 || n > 1000 {
			writeError(w, http.StatusBadRequest, "limit must be 1..1000")
			return
		}
		limit = int(n)
	}
	id, owner, epoch, err := s.svc.AuthorizeCaptureEntries(r.Context(), r.PathValue("session"), grant,
		q.Get("expected_capture_id"))
	if err != nil {
		s.writeErr(w, err, nil)
		return
	}
	resp, err := s.files.CaptureEntriesRaw(r.Context(), id, owner, epoch, cursor, limit)
	s.streamCapture(w, resp, err)
}

// captureRead proxies captured row bytes of the PERSISTED binding —
// seq/offset/len choose a range inside the bound manifest only.
func (s *Server) captureRead(w http.ResponseWriter, r *http.Request) {
	grant, ok := bearer(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, ErrGrant.Error())
		return
	}
	if s.files == nil {
		writeError(w, http.StatusServiceUnavailable, "file service is not configured")
		return
	}
	q := r.URL.Query()
	seq, err := strconv.ParseInt(q.Get("seq"), 10, 64)
	if err != nil || seq < 0 {
		writeError(w, http.StatusBadRequest, "seq is required")
		return
	}
	var off, n int64
	if v := q.Get("offset"); v != "" {
		if off, err = strconv.ParseInt(v, 10, 64); err != nil || off < 0 {
			writeError(w, http.StatusBadRequest, "offset must be a non-negative integer")
			return
		}
	}
	if v := q.Get("len"); v != "" {
		if n, err = strconv.ParseInt(v, 10, 64); err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "len must be a non-negative integer")
			return
		}
	}
	id, owner, epoch, err := s.svc.AuthorizeCaptureRead(r.Context(), r.PathValue("session"), grant,
		q.Get("expected_capture_id"))
	if err != nil {
		s.writeErr(w, err, nil)
		return
	}
	resp, err := s.files.CaptureRead(r.Context(), id, owner, epoch, seq, off, n)
	s.streamCapture(w, resp, err)
}

// streamCapture forwards the upstream capture verdict verbatim —
// status, content headers and body — so a 503 capture_pending or a
// truncated stream reaches the mover exactly as the service emitted it.
func (s *Server) streamCapture(w http.ResponseWriter, resp *http.Response, err error) {
	if err != nil {
		if s.logf != nil {
			s.logf("return capture proxy: %v", err)
		}
		writeError(w, http.StatusBadGateway, "file service unreachable; safe to retry")
		return
	}
	defer resp.Body.Close()
	for _, h := range []string{"Content-Type", "Content-Length"} {
		if v := resp.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}
