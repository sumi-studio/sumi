package transfersession

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

// RoutePrefix is where the session routes live. A move URL is the session
// resource itself plus a fragment carrying the grant:
//
//	https://<api>/api/secretary-transfer/sessions/<session_id>#grant=<grant>
//
// The fragment is never sent in a request, so the grant stays out of access
// logs; the Local command presents it as a bearer token.
const RoutePrefix = "/api/secretary-transfer"

// RegistrantProof is the trusted authentication adapter the account owner
// supplies. It returns the credential a live, verified registration flow
// proved for this request — never a subject taken from the request body or
// an unrelated ambient session — and an error for an unproven, expired,
// closed or consumed flow. The request is passed so the adapter can apply
// the browser binding its flow rules require.
type RegistrantProof interface {
	RegistrantSubject(ctx context.Context, r *http.Request, flowID, nonce string) (Subject, error)
}

type Server struct {
	svc       *Service
	proof     RegistrantProof
	base      string
	maxBundle int64
	logf      func(string, ...any)
}

// NewServer builds the routes over svc. publicBaseURL is the origin (and any
// path prefix) the Local command reaches this API at; move URLs are built
// from it, never from request headers.
func NewServer(svc *Service, proof RegistrantProof, publicBaseURL string) (*Server, error) {
	if proof == nil {
		return nil, errors.New("transfersession: a registrant proof adapter is required")
	}
	u, err := url.Parse(publicBaseURL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		!(u.Scheme == "https" || (u.Scheme == "http" && IsLoopbackHost(u.Hostname()))) {
		return nil, fmt.Errorf("transfersession: public base URL must be https (or http on loopback) without query or fragment")
	}
	return &Server{svc: svc, proof: proof, base: strings.TrimRight(publicBaseURL, "/"), maxBundle: 1 << 30, logf: log.Printf}, nil
}

// IsLoopbackHost reports a literal loopback host, the only place a move URL
// may use plain http.
func IsLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST "+RoutePrefix+"/sessions", s.create)
	mux.HandleFunc("POST "+RoutePrefix+"/registrant/session", s.registrant)
	mux.HandleFunc("GET "+RoutePrefix+"/sessions/{session}", s.status)
	mux.HandleFunc("PUT "+RoutePrefix+"/sessions/{session}/bundle", s.bundle)
	mux.HandleFunc("POST "+RoutePrefix+"/sessions/{session}/cancel", s.cancel)
}

// MoveURL is what the registering browser shows for the Local command.
func (s *Server) MoveURL(sessionID, grant string) string {
	return s.base + RoutePrefix + "/sessions/" + sessionID + "#grant=" + grant
}

type flowBody struct {
	FlowID string `json:"flow_id"`
	Nonce  string `json:"nonce"`
}

func (s *Server) subject(w http.ResponseWriter, r *http.Request, fb flowBody) (Subject, bool) {
	if fb.FlowID == "" || fb.Nonce == "" {
		writeError(w, http.StatusUnauthorized, "a verified registration flow is required")
		return Subject{}, false
	}
	subj, err := s.proof.RegistrantSubject(r.Context(), r, fb.FlowID, fb.Nonce)
	if err != nil || !subj.valid() {
		writeError(w, http.StatusUnauthorized, "the registration flow is not a live verified proof")
		return Subject{}, false
	}
	return subj, true
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

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var fb flowBody
	if !decode(w, r, &fb) {
		return
	}
	subj, ok := s.subject(w, r, fb)
	if !ok {
		return
	}
	created, open, err := s.svc.Create(r.Context(), subj)
	if errors.Is(err, ErrOpenSession) {
		// The open session's status is what the registrant decides a lost
		// move URL from; the grant is never repeated.
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
		"move_url": s.MoveURL(created.View.SessionID, created.Grant),
		"session":  created.View,
	})
}

func (s *Server) registrant(w http.ResponseWriter, r *http.Request) {
	var fb flowBody
	if !decode(w, r, &fb) {
		return
	}
	subj, ok := s.subject(w, r, fb)
	if !ok {
		return
	}
	v, err := s.svc.ForSubject(r.Context(), subj)
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
		s.writeErr(w, err, nil)
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
	v, created, err := s.svc.Upload(r.Context(), r.PathValue("session"), grant, http.MaxBytesReader(w, r.Body, s.maxBundle))
	if err != nil {
		s.writeErr(w, err, &v)
		return
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
	}
	writeJSON(w, code, v)
}

func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("session")
	if grant, ok := bearer(r); ok {
		var body struct {
			PersonaID   string `json:"persona_id"`
			TransferKey string `json:"transfer_key"`
		}
		if !decode(w, r, &body) {
			return
		}
		v, err := s.svc.CancelByGrant(r.Context(), id, grant, body.PersonaID, body.TransferKey)
		if err != nil {
			s.writeErr(w, err, &v)
			return
		}
		writeJSON(w, http.StatusOK, v)
		return
	}
	var body struct {
		flowBody
		ExpectStatus string `json:"expect_status"`
	}
	if !decode(w, r, &body) {
		return
	}
	subj, ok := s.subject(w, r, body.flowBody)
	if !ok {
		return
	}
	v, err := s.svc.CancelBySubject(r.Context(), id, subj, body.ExpectStatus)
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
// refused cancel or a closed upload still tells the caller where it stands.
func (s *Server) writeErr(w http.ResponseWriter, err error, v *View) {
	var maxErr *http.MaxBytesError
	var pgErr *pgconn.PgError
	code := http.StatusInternalServerError
	switch {
	case errors.As(err, &maxErr):
		code = http.StatusRequestEntityTooLarge
	case errors.Is(err, ErrGrant):
		code = http.StatusUnauthorized
	case errors.Is(err, ErrNotFound):
		code = http.StatusNotFound
	case errors.Is(err, ErrClosed), errors.Is(err, ErrExpired):
		code = http.StatusGone
	case errors.Is(err, ErrConflict), errors.Is(err, ErrAccountExists),
		errors.Is(err, portable.ErrTransferConflict), errors.Is(err, portable.ErrPersonaExists):
		code = http.StatusConflict
	case errors.Is(err, ErrBadRequest), errors.Is(err, portable.ErrBadRequest), errors.Is(err, portable.ErrMissingProof):
		code = http.StatusBadRequest
	case errors.Is(err, portable.ErrBadBundle), errors.Is(err, portable.ErrIntegrity), errors.Is(err, portable.ErrNotPortable):
		code = http.StatusUnprocessableEntity
	case errors.As(err, &pgErr) && (pgErr.Code == "40P01" || pgErr.Code == "40001"):
		code = http.StatusConflict
	}
	msg := err.Error()
	if code == http.StatusInternalServerError {
		if s.logf != nil {
			s.logf("transfer session: %v", err)
		}
		msg = "internal error"
	}
	body := map[string]any{"error": msg}
	if v != nil && v.SessionID != "" {
		body["session"] = v
	}
	writeJSON(w, code, body)
}
