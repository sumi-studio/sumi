package agentstate

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/modelconnections"
)

// uuidv7Re matches the uuidv7 domain: malformed persona ids in the path are
// a client error (400), not a database domain violation (500).
var uuidv7Re = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// Server exposes the persona-scoped state contract over HTTP.
//
// Two credential scopes exist, both deliberately explicit:
//   - the admin/service secret (SUMI_CORE_STATE_TOKEN) provisions personas and
//     may act on any persona — a developer/operator credential, not proof of
//     cross-persona authorization;
//   - a persona capability token authorizes exactly one persona's routes.
//     It is derived as HMAC-SHA256(adminSecret, "persona:"+personaID), so a
//     token for one persona can never authenticate for another, and nothing
//     per-persona needs storing. The real multi-user binding (koseki identity
//     → persona) lands with the auth-flow milestone; this proves the shape.
type Server struct {
	store   *Store
	secret  []byte
	maxBody int64
	conns   *modelconnections.Store
}

func NewServer(pool *pgxpool.Pool, adminSecret string) *Server {
	return &Server{store: NewStore(pool), secret: []byte(adminSecret), maxBody: 1 << 20}
}

// SetModelConnections wires the user model-connection store so
// GET .../model can resolve the persona's explicit selection. Without it
// the route reports "unset" rather than guessing at a provider.
func (s *Server) SetModelConnections(conns *modelconnections.Store) {
	s.conns = conns
}

// PersonaToken derives the scoped capability for one persona.
func (s *Server) PersonaToken(personaID string) string {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte("persona:" + personaID))
	return "core_" + hex.EncodeToString(mac.Sum(nil))
}

func (s *Server) authorized(r *http.Request, personaID string) bool {
	token, ok := bearerToken(r.Header.Get("Authorization"))
	if !ok {
		return false
	}
	if subtle.ConstantTimeCompare([]byte(token), s.secret) == 1 {
		return true
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(s.PersonaToken(personaID))) == 1
}

func bearerToken(header string) (string, bool) {
	const prefix = "Bearer "
	if len(header) <= len(prefix) || !strings.HasPrefix(header, prefix) {
		return "", false
	}
	return header[len(prefix):], true
}

func (s *Server) adminOnly(r *http.Request) bool {
	token, ok := bearerToken(r.Header.Get("Authorization"))
	return ok && subtle.ConstantTimeCompare([]byte(token), s.secret) == 1
}

// RegisterRoutes mounts the contract on mux. The caller decides where the
// surface lives (public API mux behind env config, or the standalone
// state-dev binary).
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /internal/core/personas", s.createPersona)
	mux.HandleFunc("GET /internal/core/personas/{persona}/state", s.personaState)
	mux.HandleFunc("POST /internal/core/personas/{persona}/inputs", s.submitInput)
	mux.HandleFunc("GET /internal/core/personas/{persona}/inputs/{input}", s.getInput)
	mux.HandleFunc("POST /internal/core/personas/{persona}/writer/acquire", s.acquireWriter)
	mux.HandleFunc("POST /internal/core/personas/{persona}/writer/renew", s.renewWriter)
	mux.HandleFunc("POST /internal/core/personas/{persona}/writer/release", s.releaseWriter)
	mux.HandleFunc("POST /internal/core/personas/{persona}/recover", s.recover)
	mux.HandleFunc("POST /internal/core/personas/{persona}/turns/load", s.loadTurn)
	mux.HandleFunc("POST /internal/core/personas/{persona}/turns/plan", s.savePlan)
	mux.HandleFunc("POST /internal/core/personas/{persona}/turns/{turn}/commit", s.commitTurn)
	mux.HandleFunc("GET /internal/core/personas/{persona}/events", s.events)
	mux.HandleFunc("POST /internal/core/personas/{persona}/operations/claim", s.claimOperation)
	mux.HandleFunc("POST /internal/core/personas/{persona}/operations/{operation}/complete", s.completeOperation)
	mux.HandleFunc("POST /internal/core/personas/{persona}/schedules/dispatch", s.dispatchSchedules)
	mux.HandleFunc("GET /internal/core/personas/{persona}/outbox", s.outbox)
	mux.HandleFunc("GET /internal/core/personas/{persona}/approvals", s.listApprovals)
	mux.HandleFunc("GET /internal/core/personas/{persona}/approvals/{approval}", s.getApproval)
	mux.HandleFunc("POST /internal/core/personas/{persona}/approvals/{approval}/decision", s.decideApproval)
	mux.HandleFunc("GET /internal/core/personas/{persona}/model", s.modelBinding)
}

func (s *Server) scope(w http.ResponseWriter, r *http.Request) (string, bool) {
	personaID := r.PathValue("persona")
	if !uuidv7Re.MatchString(personaID) {
		writeError(w, http.StatusBadRequest, "persona must be a uuidv7")
		return "", false
	}
	if !s.authorized(r, personaID) {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return "", false
	}
	return personaID, true
}

// requireGen enforces that mutation bodies carry a positive writer
// generation; a missing one is a client error, not a fencing failure.
func requireGen(w http.ResponseWriter, generation int64) bool {
	if generation <= 0 {
		writeError(w, http.StatusBadRequest, "generation required")
		return false
	}
	return true
}

func decode(w http.ResponseWriter, r *http.Request, v any, maxBody int64) bool {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		writeError(w, http.StatusBadRequest, "read body")
		return false
	}
	if len(body) == 0 {
		return true
	}
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return false
	}
	return true
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

func storeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrPersonaNotFound), errors.Is(err, ErrInputNotFound),
		errors.Is(err, ErrTurnNotFound), errors.Is(err, ErrOpNotFound),
		errors.Is(err, ErrApprovalNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrWriterHeld), errors.Is(err, ErrGenerationFence), errors.Is(err, ErrTurnConflict),
		errors.Is(err, ErrApprovalConflict):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, ErrBadRequest), errors.Is(err, ErrUnknownTool), errors.Is(err, ErrApprovalDecidedBy):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrApprovalForbidden):
		writeError(w, http.StatusForbidden, err.Error())
	case isDataError(err):
		// Deterministic data errors (class 22, 23514) can never succeed on
		// retry; report them as 400, not a transient-looking 500.
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func isDataError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) &&
		(strings.HasPrefix(pgErr.Code, "22") || pgErr.Code == "23514")
}

func (s *Server) createPersona(w http.ResponseWriter, r *http.Request) {
	if !s.adminOnly(r) {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req struct {
		PersonaID   string  `json:"persona_id"`
		HumanID     *string `json:"human_id"`
		DisplayName string  `json:"display_name"`
	}
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	if !uuidv7Re.MatchString(req.PersonaID) {
		writeError(w, http.StatusBadRequest, "persona_id must be a uuidv7")
		return
	}
	if req.HumanID != nil && !uuidv7Re.MatchString(*req.HumanID) {
		writeError(w, http.StatusBadRequest, "human_id must be a uuidv7")
		return
	}
	p, created, err := s.store.EnsurePersona(r.Context(), req.PersonaID, req.HumanID, req.DisplayName)
	if err != nil {
		storeError(w, err)
		return
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
	}
	writeJSON(w, status, map[string]any{
		"persona":       p,
		"created":       created,
		"persona_token": s.PersonaToken(p.PersonaID),
	})
}

func (s *Server) personaState(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	st, err := s.store.PersonaState(r.Context(), personaID)
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) submitInput(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	var req struct {
		InputID       string         `json:"input_id"`
		Kind          string         `json:"kind"`
		Payload       map[string]any `json:"payload"`
		ActorKind     string         `json:"actor_kind"`
		ActorID       string         `json:"actor_id"`
		SourceSurface string         `json:"source_surface"`
		ThreadID      string         `json:"thread_id"`
		OccurredAt    *time.Time     `json:"occurred_at"`
		Attention     string         `json:"attention"`
	}
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	if req.InputID == "" || req.Kind == "" || req.Payload == nil {
		writeError(w, http.StatusBadRequest, "input_id, kind, payload required")
		return
	}
	if req.Attention == "" {
		req.Attention = "reply"
	}
	in, created, err := s.store.SubmitInput(r.Context(), &Input{
		PersonaID:     personaID,
		InputID:       req.InputID,
		Kind:          req.Kind,
		Payload:       req.Payload,
		ActorKind:     req.ActorKind,
		ActorID:       req.ActorID,
		SourceSurface: req.SourceSurface,
		ThreadID:      req.ThreadID,
		OccurredAt:    req.OccurredAt,
		Attention:     req.Attention,
	})
	if err != nil {
		storeError(w, err)
		return
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
	}
	writeJSON(w, status, map[string]any{"input": in, "created": created})
}

func (s *Server) getInput(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	in, turn, err := s.store.GetInput(r.Context(), personaID, r.PathValue("input"))
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"input": in, "turn": turn})
}

func (s *Server) acquireWriter(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	var req struct {
		HolderID string `json:"holder_id"`
		TTLms    int64  `json:"ttl_ms"`
	}
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	if req.HolderID == "" || req.TTLms <= 0 {
		writeError(w, http.StatusBadRequest, "holder_id and positive ttl_ms required")
		return
	}
	lease, err := s.store.AcquireWriter(r.Context(), personaID, req.HolderID, time.Duration(req.TTLms)*time.Millisecond)
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, lease)
}

func (s *Server) renewWriter(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	var req struct {
		HolderID   string `json:"holder_id"`
		Generation int64  `json:"generation"`
		TTLms      int64  `json:"ttl_ms"`
	}
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	if req.HolderID == "" || req.TTLms <= 0 {
		writeError(w, http.StatusBadRequest, "holder_id and positive ttl_ms required")
		return
	}
	if !requireGen(w, req.Generation) {
		return
	}
	lease, err := s.store.RenewWriter(r.Context(), personaID, req.HolderID, req.Generation, time.Duration(req.TTLms)*time.Millisecond)
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, lease)
}

func (s *Server) releaseWriter(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	var req struct {
		HolderID   string `json:"holder_id"`
		Generation int64  `json:"generation"`
	}
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	if req.HolderID == "" {
		writeError(w, http.StatusBadRequest, "holder_id required")
		return
	}
	if !requireGen(w, req.Generation) {
		return
	}
	if err := s.store.ReleaseWriter(r.Context(), personaID, req.HolderID, req.Generation); err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"released": true})
}

func (s *Server) recover(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	var req struct {
		Generation int64 `json:"generation"`
	}
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	if !requireGen(w, req.Generation) {
		return
	}
	res, err := s.store.Recover(r.Context(), personaID, req.Generation)
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) loadTurn(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	var req struct {
		Generation   int64  `json:"generation"`
		TurnID       string `json:"turn_id"`
		ContextLimit int    `json:"context_limit"`
	}
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	if !requireGen(w, req.Generation) {
		return
	}
	res, err := s.store.LoadTurn(r.Context(), personaID, req.Generation, req.TurnID, req.ContextLimit)
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) commitTurn(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	var req struct {
		Generation int64 `json:"generation"`
		CommitRequest
	}
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	if !requireGen(w, req.Generation) {
		return
	}
	t, err := s.store.CommitTurn(r.Context(), personaID, r.PathValue("turn"), req.Generation, req.CommitRequest)
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"turn": t})
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	var after int64
	if raw := r.URL.Query().Get("after_seq"); raw != "" {
		after, _ = strconv.ParseInt(raw, 10, 64)
	}
	var limit int
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, _ = strconv.Atoi(raw)
	}
	evs, err := s.store.Events(r.Context(), personaID, after, limit)
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": evs})
}

// savePlan records one round of the model's decisions for the turn's input
// before any of that round's effects execute — the durable F1 boundary.
// The plan is an append-only list of rounds: identical resaves of a
// recorded round replay the stored plan; a conflicting decision at a
// recorded position or a skipped round conflicts.
func (s *Server) savePlan(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	var req struct {
		Generation int64          `json:"generation"`
		TurnID     string         `json:"turn_id"`
		Round      *int64         `json:"round"`
		Text       string         `json:"text"`
		Calls      *[]PlanCall    `json:"calls"`
		Usage      map[string]any `json:"usage"`
	}
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	if req.TurnID == "" || req.Calls == nil || req.Round == nil || *req.Round < 0 {
		writeError(w, http.StatusBadRequest, "turn_id, round (>= 0), and calls (array) required")
		return
	}
	if !requireGen(w, req.Generation) {
		return
	}
	plan, created, err := s.store.SavePlan(r.Context(), personaID, req.TurnID, req.Generation, *req.Round, Decision{
		Text:  req.Text,
		Calls: *req.Calls,
		Usage: req.Usage,
	})
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"plan": plan, "created": created})
}

func (s *Server) claimOperation(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	var req struct {
		Generation  int64          `json:"generation"`
		OperationID string         `json:"operation_id"`
		TurnID      string         `json:"turn_id"`
		Tool        string         `json:"tool"`
		CallIndex   *int           `json:"call_index"`
		Request     map[string]any `json:"request"`
	}
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	// No caller idempotency_key: effect identity is derived server-side from
	// the turn's input and call_index, so a supplied legacy key can never
	// mint a second effect for the same planned call.
	if req.OperationID == "" || req.TurnID == "" || req.Tool == "" || req.CallIndex == nil {
		writeError(w, http.StatusBadRequest, "operation_id, turn_id, tool, call_index required")
		return
	}
	if !requireGen(w, req.Generation) {
		return
	}
	op, approval, fresh, err := s.store.ClaimOperation(r.Context(), personaID, req.TurnID, req.Generation,
		req.OperationID, req.Tool, *req.CallIndex, req.Request)
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"operation": op, "approval": approval, "fresh": fresh})
}

func (s *Server) completeOperation(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	var req struct {
		Generation int64          `json:"generation"`
		Response   map[string]any `json:"response"`
		Failed     bool           `json:"failed"`
	}
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	if !requireGen(w, req.Generation) {
		return
	}
	op, err := s.store.CompleteOperation(r.Context(), personaID, r.PathValue("operation"), req.Generation, req.Response, req.Failed)
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"operation": op})
}

func (s *Server) dispatchSchedules(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	var req struct {
		Generation int64      `json:"generation"`
		Now        *time.Time `json:"now"`
		Limit      int        `json:"limit"`
	}
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	now := time.Now()
	if req.Now != nil {
		now = *req.Now
	}
	if !requireGen(w, req.Generation) {
		return
	}
	fired, err := s.store.DispatchDueSchedules(r.Context(), personaID, req.Generation, now, req.Limit)
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"fired": fired})
}

func (s *Server) outbox(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	var after int64
	if raw := r.URL.Query().Get("after_seq"); raw != "" {
		after, _ = strconv.ParseInt(raw, 10, 64)
	}
	var limit int
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, _ = strconv.Atoi(raw)
	}
	entries, err := s.store.Outbox(r.Context(), personaID, after, limit)
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"outbox": entries})
}
