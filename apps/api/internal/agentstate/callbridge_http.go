package agentstate

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

// Persona-scoped bridge routes for the per-placement call media runner. Like
// the jobs surface, these are deliberately not writer-generation gated: a
// session claim is its own authority and outlives writer restarts.

type callClaimRequest struct {
	RunnerID string `json:"runner_id"`
	LeaseMs  int64  `json:"lease_ms"`
	Limit    int    `json:"limit"`
}

type callHeartbeatRequest struct {
	RunnerID string `json:"runner_id"`
	Epoch    int64  `json:"epoch"`
	LeaseMs  int64  `json:"lease_ms"`
}

type callTicketRequest struct {
	RunnerID string `json:"runner_id"`
	Epoch    int64  `json:"epoch"`
}

type callStatusRequest struct {
	RunnerID string `json:"runner_id"`
	Epoch    int64  `json:"epoch"`
	Status   string `json:"status"`
	Reason   string `json:"reason"`
}

type callUtteranceRequest struct {
	RunnerID string         `json:"runner_id"`
	Epoch    int64          `json:"epoch"`
	Status   string         `json:"status"`
	Detail   map[string]any `json:"detail"`
}

func (s *Server) requireCallBridge(w http.ResponseWriter) CallBridge {
	if s.callBridge == nil {
		writeError(w, http.StatusServiceUnavailable, "call bridge is not configured")
		return nil
	}
	return s.callBridge
}

func (s *Server) claimCallSessions(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	bridge := s.requireCallBridge(w)
	if bridge == nil {
		return
	}
	var req callClaimRequest
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	sessions, err := bridge.ClaimCallSessions(r.Context(), personaID,
		strings.TrimSpace(req.RunnerID), time.Duration(req.LeaseMs)*time.Millisecond, req.Limit)
	if err != nil {
		s.callError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sessions": sessions})
}

func (s *Server) heartbeatCallSession(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	bridge := s.requireCallBridge(w)
	if bridge == nil {
		return
	}
	var req callHeartbeatRequest
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	session, err := bridge.HeartbeatCallSession(r.Context(), personaID,
		r.PathValue("session"), strings.TrimSpace(req.RunnerID), req.Epoch,
		time.Duration(req.LeaseMs)*time.Millisecond)
	if err != nil {
		s.callError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"session": session})
}

func (s *Server) callSessionTicket(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	bridge := s.requireCallBridge(w)
	if bridge == nil {
		return
	}
	var req callTicketRequest
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	ticket, err := bridge.CallSessionTicket(r.Context(), personaID,
		r.PathValue("session"), strings.TrimSpace(req.RunnerID), req.Epoch)
	if err != nil {
		s.callError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ticket": ticket})
}

func (s *Server) reportCallSessionStatus(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	bridge := s.requireCallBridge(w)
	if bridge == nil {
		return
	}
	var req callStatusRequest
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	session, err := bridge.ReportCallSessionStatus(r.Context(), personaID,
		r.PathValue("session"), strings.TrimSpace(req.RunnerID), req.Epoch,
		req.Status, req.Reason)
	if err != nil {
		s.callError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"session": session})
}

func (s *Server) pendingCallUtterances(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	bridge := s.requireCallBridge(w)
	if bridge == nil {
		return
	}
	utterances, err := bridge.PendingCallUtterances(r.Context(), personaID,
		r.PathValue("session"), strings.TrimSpace(r.URL.Query().Get("runner_id")),
		parseInt64Query(r, "epoch"))
	if err != nil {
		s.callError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"utterances": utterances})
}

func (s *Server) reportCallUtterance(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	bridge := s.requireCallBridge(w)
	if bridge == nil {
		return
	}
	var req callUtteranceRequest
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	utterance, err := bridge.ReportUtteranceDisposition(r.Context(), personaID,
		r.PathValue("session"), r.PathValue("utterance"),
		strings.TrimSpace(req.RunnerID), req.Epoch, req.Status, req.Detail)
	if err != nil {
		s.callError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"utterance": utterance})
}

// callError maps bridge errors onto the same shape the rest of the surface
// uses; claim-loss and session-lifecycle rejections are conflicts the runner
// treats as stop signals rather than retries.
func (s *Server) callError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrBadRequest):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrPersonaNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	default:
		msg := err.Error()
		switch {
		case strings.Contains(msg, "claim"), strings.Contains(msg, "not live"),
			strings.Contains(msg, "terminal"):
			writeError(w, http.StatusConflict, msg)
		case strings.Contains(msg, "not found"):
			writeError(w, http.StatusNotFound, msg)
		default:
			writeError(w, http.StatusInternalServerError, msg)
		}
	}
}

func parseInt64Query(r *http.Request, key string) int64 {
	var value int64
	if raw := r.URL.Query().Get(key); raw != "" {
		if err := json.Unmarshal([]byte(raw), &value); err != nil {
			return 0
		}
	}
	return value
}
