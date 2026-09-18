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
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

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
}

// NewServer builds the routes over svc. publicBaseURL is the origin (and
// any path prefix) the Local command reaches this API at; return URLs are
// built from it, never from request headers.
func NewServer(svc *Service, proof OwnerProof, publicBaseURL string) (*Server, error) {
	if proof == nil {
		return nil, errors.New("returnsession: an owner proof adapter is required")
	}
	u, err := url.Parse(publicBaseURL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		!(u.Scheme == "https" || (u.Scheme == "http" && loopback(u.Hostname()))) {
		return nil, fmt.Errorf("returnsession: public base URL must be https (or http on loopback) without query or fragment")
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
}

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
	created, open, err := s.svc.Create(r.Context(), o)
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
		errors.Is(err, portable.ErrTransferConflict),
		errors.Is(err, portable.ErrPersonaExists), errors.Is(err, portable.ErrUnresolvedOperations):
		code = http.StatusConflict
	case errors.Is(err, ErrBadRequest), errors.Is(err, portable.ErrBadRequest), errors.Is(err, portable.ErrMissingProof):
		code = http.StatusBadRequest
	case errors.As(err, &pgErr) && (pgErr.Code == "23505" || pgErr.Code == "40P01" || pgErr.Code == "40001"):
		code = http.StatusConflict
	}
	msg := err.Error()
	if code == http.StatusInternalServerError {
		if s.logf != nil {
			s.logf("return session: %v", err)
		}
		msg = "internal error"
	}
	body := map[string]any{"error": msg}
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
