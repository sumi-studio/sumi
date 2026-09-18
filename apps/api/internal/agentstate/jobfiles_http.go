package agentstate

// HTTP surface for the job-scoped file capability (see jobfiles.go). The
// caller is the job's claiming runner authenticating with its persona
// token; the filesvc credential stays server-side and every scope is
// derived from the verified persona — nothing caller-supplied crosses
// into a filesvc request.
//
// The upstream call is a port (JobFileService): agentstate defines the
// interface so it never imports the filesvc client (which already imports
// this package for ToolEffect). fileaccess adapts *Client to it at wiring.

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// JobFileSvcError is a non-2xx answer from the file service, surfaced
// through the port so the ledger can classify determinate refusals,
// the service's own uncertain-outcome verdicts, and plain unavailability.
type JobFileSvcError struct {
	Status  int
	Code    string
	Message string
}

func (e *JobFileSvcError) Error() string {
	return "file service " + strconv.Itoa(e.Status) + " " + e.Code + ": " + e.Message
}

// JobFileService is the upstream port the capability calls: one bounded,
// allowlisted filesvc operation under an already-derived scope. The
// mutating call carries the op's durable identity as the service's
// idempotency key, so a resend is the reconciliation mechanism.
type JobFileService interface {
	// ReadOp runs a non-mutating op (stat|list|read) and returns the
	// upstream result payload.
	ReadOp(ctx context.Context, op, scope, path string, q url.Values) (map[string]any, error)
	// MutateOp runs a mutating op (write|mkdir|remove) under the durable
	// op identity. replayed reports the service's receipt answer.
	MutateOp(ctx context.Context, op, scope, path, ifVersion, opID string, body []byte) (version int64, replayed bool, err error)
	// ScopeForPersona derives the canonical filesvc scope for the verified
	// persona — the only scope these routes may ever use.
	ScopeForPersona(personaID string) (string, error)
}

// jobFileOpTimeout bounds the upstream filesvc call for one operation. A
// timeout leaves the op 'unknown' — the ledger, not the clock, is the
// authority on whether the effect committed.
const jobFileOpTimeout = 20 * time.Second

// SetJobFileService wires the upstream port for the job file capability.
// Nil leaves the routes answering 503 — the capability is honestly absent,
// not half-present.
func (s *Server) SetJobFileService(svc JobFileService) {
	s.jobFiles = svc
}

// jobFileOpRequest is the runner's call envelope. Path/if_version describe
// the operation; data_base64 carries the write body (bounded at decode);
// cursor/offset/len/limit page read ops.
type jobFileOpRequest struct {
	RunnerID  string `json:"runner_id"`
	Path      string `json:"path"`
	IfVersion string `json:"if_version"`
	DataB64   string `json:"data_base64"`
	Cursor    string `json:"cursor"`
	Offset    int64  `json:"offset"`
	Len       int64  `json:"len"`
	Limit     int64  `json:"limit"`
}

func (s *Server) jobFileOp(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	op := r.PathValue("op")
	mutating, allowed := jobFileOpAllowed(op)
	if !allowed {
		writeError(w, http.StatusBadRequest, "file op must be stat, list, read, write, mkdir, or remove")
		return
	}
	if s.jobFiles == nil {
		writeError(w, http.StatusServiceUnavailable, "file capability is not configured")
		return
	}
	var req jobFileOpRequest
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	if req.RunnerID == "" {
		writeError(w, http.StatusBadRequest, "runner_id required")
		return
	}
	if err := validJobFilePath(req.Path); err != nil {
		storeError(w, err)
		return
	}
	if mutating {
		s.jobFileMutation(w, r, personaID, op, req)
		return
	}
	s.jobFileRead(w, r, personaID, op, req)
}

// jobFileRead authorizes a read op under the live claim and proxies it —
// no ledger row, because a read carries no effect to settle.
func (s *Server) jobFileRead(w http.ResponseWriter, r *http.Request, personaID, op string, req jobFileOpRequest) {
	jobID := r.PathValue("job")
	if err := s.store.CheckJobFileAuthority(r.Context(), personaID, jobID, req.RunnerID); err != nil {
		jobFileError(w, err)
		return
	}
	scope, err := s.jobFiles.ScopeForPersona(personaID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	q := url.Values{}
	if req.Cursor != "" {
		q.Set("cursor", req.Cursor)
	}
	if req.Offset > 0 {
		q.Set("offset", strconv.FormatInt(req.Offset, 10))
	}
	// A script read page is bounded; larger files page through offset.
	length := req.Len
	if length < 0 || length > 64<<10 {
		length = 64 << 10
	}
	q.Set("len", strconv.FormatInt(length, 10))
	limit := req.Limit
	if limit <= 0 || limit > 200 {
		limit = 200
	}
	q.Set("limit", strconv.FormatInt(limit, 10))
	ctx, cancel := context.WithTimeout(r.Context(), jobFileOpTimeout)
	defer cancel()
	out, err := s.jobFiles.ReadOp(ctx, op, scope, req.Path, q)
	if err != nil {
		jobFileUpstreamError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"result": out})
}

// jobFileMutation admits, executes, and records one mutating operation.
// The ledger row commits BEFORE the upstream call (see jobfiles.go), so a
// crash anywhere after admission leaves a resolvable pending record, not
// an unaccounted effect.
func (s *Server) jobFileMutation(w http.ResponseWriter, r *http.Request, personaID, op string, req jobFileOpRequest) {
	jobID := r.PathValue("job")
	var body []byte
	if req.DataB64 != "" {
		b, err := base64.StdEncoding.DecodeString(req.DataB64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "data_base64 must be valid base64")
			return
		}
		body = b
	}
	scope, err := s.jobFiles.ScopeForPersona(personaID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	o, err := s.store.AdmitJobFileOp(r.Context(), personaID, jobID, req.RunnerID,
		op, scope, req.Path, req.IfVersion, body)
	if err != nil {
		jobFileError(w, err)
		return
	}
	s.runJobFileOp(w, r, o, body)
}

// runJobFileOp performs the upstream call for an admitted op and records
// the outcome. It is shared by the admission path and the resolve path —
// the keyed resend is the same call under the same identity.
func (s *Server) runJobFileOp(w http.ResponseWriter, r *http.Request, o JobFileOp, body []byte) {
	ctx, cancel := context.WithTimeout(r.Context(), jobFileOpTimeout)
	defer cancel()
	ifVersion, _ := o.Request["if_version"].(string)
	status, result, opErr := classifyJobFileMutation(
		s.jobFiles.MutateOp(ctx, o.Op, o.Scope, o.Path, ifVersion, o.OpID, body))
	o, err := s.store.RecordJobFileOp(r.Context(), o.PersonaID, o.JobID, o.OpID, status, result, opErr)
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"op": o})
}

// classifyJobFileMutation maps the upstream answer to a ledger status:
// settled (executed or receipt replayed), refused (determinate 4xx — no
// effect), diverged (the service's own outcome_uncertain/idempotency
// verdict — preserved, not retried), unknown (transport/5xx — the resend
// decides later).
func classifyJobFileMutation(version int64, replayed bool, err error) (status string, result map[string]any, opErr string) {
	if err == nil {
		return "settled", map[string]any{"version": version, "replayed": replayed}, ""
	}
	var se *JobFileSvcError
	if errors.As(err, &se) {
		result = map[string]any{"code": se.Code, "status": se.Status}
		if se.Code == "outcome_uncertain" || se.Code == "idempotency_conflict" {
			// filesvc's own verdict that the effect cannot be confirmed —
			// preserved as diverged, never silently retried.
			return "diverged", result, se.Message
		}
		if se.Status >= 400 && se.Status < 500 {
			return "refused", result, se.Message
		}
		return "unknown", result, se.Message
	}
	return "unknown", nil, err.Error()
}

// listJobFileOps exposes the ledger: the secretary's truth about which of
// a job's file effects are recorded, settled, refused, or still unknown.
func (s *Server) listJobFileOps(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	pending := r.URL.Query().Get("pending") == "true"
	ops, err := s.store.ListJobFileOps(r.Context(), personaID, r.PathValue("job"), pending, 500)
	if err != nil {
		storeError(w, err)
		return
	}
	n, err := s.store.PendingJobFileOps(r.Context(), personaID, r.PathValue("job"))
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ops": ops, "pending": n})
}

// resolveJobFileOp reconciles one unresolved op by resending its stored
// request under the same durable key. Any persona-scoped caller may ask —
// the answer is the service's receipt, not the caller's say-so.
func (s *Server) resolveJobFileOp(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	if s.jobFiles == nil {
		writeError(w, http.StatusServiceUnavailable, "file capability is not configured")
		return
	}
	o, pending, err := s.store.JobFileOpForResolve(r.Context(), personaID,
		r.PathValue("job"), r.PathValue("opid"))
	if err != nil {
		storeError(w, err)
		return
	}
	if !pending {
		writeJSON(w, http.StatusOK, map[string]any{"op": o})
		return
	}
	s.runJobFileOp(w, r, o, o.Body)
}

func jobFileError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrJobNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrJobFileOpDenied):
		writeError(w, http.StatusConflict, err.Error())
	default:
		storeError(w, err)
	}
}

func jobFileUpstreamError(w http.ResponseWriter, err error) {
	var se *JobFileSvcError
	if errors.As(err, &se) && se.Status >= 400 && se.Status < 500 {
		writeJSON(w, se.Status, map[string]any{"error": se.Message, "code": se.Code})
		return
	}
	writeError(w, http.StatusBadGateway, "file service unavailable: "+err.Error())
}
