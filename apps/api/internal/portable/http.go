package portable

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Server exposes transfer steps under /internal/core. Every route requires
// the admin/service secret: a persona capability token must never be able to
// seal, export, replace or activate a persona. Binding a transfer to the
// authenticated account that consented to it is the account flow's job
// (M21); these routes are the state-service boundary that flow calls.
type Server struct {
	svc       *Service
	secret    []byte
	maxBundle int64
}

func NewServer(pool *pgxpool.Pool, adminSecret string) *Server {
	return &Server{svc: NewService(pool), secret: []byte(adminSecret), maxBundle: 1 << 30}
}

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	const p = "/internal/core/personas/{persona}/transfers/{transfer}"
	mux.HandleFunc("GET /internal/core/placement", s.placement)
	mux.HandleFunc("POST "+p+"/seal", s.seal)
	mux.HandleFunc("GET "+p+"/bundle", s.bundle)
	mux.HandleFunc("POST "+p+"/complete", s.complete)
	mux.HandleFunc("POST "+p+"/abort", s.abort)
	mux.HandleFunc("POST "+p+"/activate", s.step((*Service).Activate))
	mux.HandleFunc("POST "+p+"/retire", s.retire)
	mux.HandleFunc("POST /internal/core/transfers/import", s.importBundle)
	mux.HandleFunc("GET /internal/core/transfers/{direction}/{transfer}", s.status)
}

func (s *Server) admin(w http.ResponseWriter, r *http.Request) bool {
	const prefix = "Bearer "
	h := r.Header.Get("Authorization")
	if len(h) > len(prefix) && strings.HasPrefix(h, prefix) &&
		subtle.ConstantTimeCompare([]byte(h[len(prefix):]), s.secret) == 1 {
		return true
	}
	writeError(w, http.StatusUnauthorized, "unauthorized")
	return false
}

// body decodes an optional JSON object. An empty body decodes as the zero
// value so a bare POST stays meaningful where a step takes no parameters.
func body[T any](w http.ResponseWriter, r *http.Request, v *T) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "request body must be a JSON object")
		return false
	}
	return true
}

func (s *Server) step(fn func(*Service, context.Context, string, string) (Receipt, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.admin(w, r) {
			return
		}
		rec, err := fn(s.svc, r.Context(), r.PathValue("persona"), r.PathValue("transfer"))
		if err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, rec)
	}
}

func (s *Server) placement(w http.ResponseWriter, r *http.Request) {
	if !s.admin(w, r) {
		return
	}
	id, err := s.svc.PlacementID(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"placement_id": id})
}

func (s *Server) seal(w http.ResponseWriter, r *http.Request) {
	if !s.admin(w, r) {
		return
	}
	var b struct {
		DestinationID string `json:"destination_id"`
	}
	if !body(w, r, &b) {
		return
	}
	rec, err := s.svc.Seal(r.Context(), r.PathValue("persona"), r.PathValue("transfer"), b.DestinationID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

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

func (s *Server) bundle(w http.ResponseWriter, r *http.Request) {
	if !s.admin(w, r) {
		return
	}
	sw := &startedWriter{w: w}
	_, err := s.svc.Export(r.Context(), r.PathValue("persona"), r.PathValue("transfer"), sw)
	if err != nil && !sw.started {
		writeErr(w, err)
	}
	// After the first byte a failure can only end the stream early; without
	// its trailer the bundle is refused by every importer.
}

func (s *Server) complete(w http.ResponseWriter, r *http.Request) {
	if !s.admin(w, r) {
		return
	}
	var b struct {
		ActivateProof string `json:"activate_proof"`
	}
	if !body(w, r, &b) {
		return
	}
	rec, err := s.svc.Complete(r.Context(), r.PathValue("persona"), r.PathValue("transfer"), b.ActivateProof)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (s *Server) abort(w http.ResponseWriter, r *http.Request) {
	if !s.admin(w, r) {
		return
	}
	var b struct {
		RetireProof string `json:"retire_proof"`
		Force       bool   `json:"force"`
	}
	if !body(w, r, &b) {
		return
	}
	rec, err := s.svc.Abort(r.Context(), r.PathValue("persona"), r.PathValue("transfer"), b.RetireProof, b.Force)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (s *Server) retire(w http.ResponseWriter, r *http.Request) {
	if !s.admin(w, r) {
		return
	}
	var b struct {
		TransferKey string `json:"transfer_key"`
	}
	if !body(w, r, &b) {
		return
	}
	rec, err := s.svc.Retire(r.Context(), r.PathValue("persona"), r.PathValue("transfer"), b.TransferKey)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (s *Server) importBundle(w http.ResponseWriter, r *http.Request) {
	if !s.admin(w, r) {
		return
	}
	var humanID *string
	if v := r.URL.Query().Get("human_id"); v != "" {
		humanID = &v
	}
	rec, created, err := s.svc.Import(r.Context(), http.MaxBytesReader(w, r.Body, s.maxBundle), humanID)
	if err != nil {
		writeErr(w, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, rec)
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	if !s.admin(w, r) {
		return
	}
	rec, err := s.svc.Status(r.Context(), r.PathValue("direction"), r.PathValue("transfer"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rec)
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

func writeErr(w http.ResponseWriter, err error) {
	var maxErr *http.MaxBytesError
	var pgErr *pgconn.PgError
	switch {
	case errors.As(err, &maxErr):
		writeError(w, http.StatusRequestEntityTooLarge, "bundle exceeds the accepted size")
	case errors.Is(err, ErrPersonaNotFound), errors.Is(err, ErrTransferNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrMissingProof):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrTransferConflict), errors.Is(err, ErrPersonaExists), errors.Is(err, ErrUnresolvedOperations):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, ErrBadBundle), errors.Is(err, ErrIntegrity), errors.Is(err, ErrNotPortable):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, ErrBadRequest):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.As(err, &pgErr) && (pgErr.Code == "23505" || pgErr.Code == "40P01" || pgErr.Code == "40001"):
		writeError(w, http.StatusConflict, "a concurrent transfer step won; repeat the call to read its result")
	case errors.As(err, &pgErr) && pgErr.Code == "23503":
		writeError(w, http.StatusUnprocessableEntity, "a referenced row does not exist (for example human_id)")
	default:
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}
