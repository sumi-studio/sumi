package agentstate

// Runner-facing terminal routes — persona-token scoped like jobs and
// call sessions: the runner's claim is its own authority, deliberately
// not writer-generation gated. Every mutation carries (runner_id,
// epoch) so a stale claimant can never write after its lease lapsed.

import (
	"errors"
	"fmt"
	"net/http"
	"time"
)

// claimTerminalSessions is the runner's periodic call: sweeps expired
// claims to 'interrupted' and claims up to limit requested/interrupted
// sessions for this runner's backend.
func (s *Server) claimTerminalSessions(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	var req struct {
		RunnerID string `json:"runner_id"`
		Backend  string `json:"backend"`
		LeaseMs  int64  `json:"lease_ms"`
		Limit    int    `json:"limit"`
	}
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	if req.RunnerID == "" || req.LeaseMs <= 0 {
		writeError(w, http.StatusBadRequest, "runner_id and positive lease_ms required")
		return
	}
	claimed, interrupted, err := s.store.ClaimTerminalSessions(r.Context(), personaID,
		req.RunnerID, req.Backend, time.Duration(req.LeaseMs)*time.Millisecond, req.Limit)
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"claimed": claimed, "interrupted": interrupted})
}

func (s *Server) heartbeatTerminalSession(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	var req struct {
		RunnerID string `json:"runner_id"`
		Epoch    int64  `json:"epoch"`
		LeaseMs  int64  `json:"lease_ms"`
	}
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	if req.RunnerID == "" || req.LeaseMs <= 0 {
		writeError(w, http.StatusBadRequest, "runner_id and positive lease_ms required")
		return
	}
	t, err := s.store.HeartbeatTerminalSession(r.Context(), personaID, r.PathValue("session"),
		req.RunnerID, req.Epoch, time.Duration(req.LeaseMs)*time.Millisecond)
	if err != nil {
		if errors.Is(err, ErrTerminalNotClaimed) {
			writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "session": t})
			return
		}
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"session": t})
}

func (s *Server) pendingTerminalInputs(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	runnerID := r.URL.Query().Get("runner_id")
	var epoch int64
	if e := r.URL.Query().Get("epoch"); e != "" {
		if _, err := fmt.Sscanf(e, "%d", &epoch); err != nil {
			writeError(w, http.StatusBadRequest, "invalid epoch")
			return
		}
	}
	if runnerID == "" || epoch <= 0 {
		writeError(w, http.StatusBadRequest, "runner_id and positive epoch required")
		return
	}
	inputs, err := s.store.PendingTerminalInputs(r.Context(), personaID, r.PathValue("session"), runnerID, epoch)
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"inputs": inputs})
}

func (s *Server) reportTerminalInputDisposition(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	var req struct {
		RunnerID string         `json:"runner_id"`
		Epoch    int64          `json:"epoch"`
		Status   string         `json:"status"`
		Detail   map[string]any `json:"detail"`
	}
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	if req.RunnerID == "" || req.Epoch <= 0 || req.Status == "" {
		writeError(w, http.StatusBadRequest, "runner_id, positive epoch and status required")
		return
	}
	in, err := s.store.ReportTerminalInputDisposition(r.Context(), personaID, r.PathValue("session"),
		r.PathValue("input"), req.RunnerID, req.Epoch, req.Status, req.Detail)
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"input": in})
}

func (s *Server) appendTerminalOutput(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	var req struct {
		RunnerID string                `json:"runner_id"`
		Epoch    int64                 `json:"epoch"`
		Chunks   []TerminalOutputChunk `json:"chunks"`
	}
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	if req.RunnerID == "" || req.Epoch <= 0 {
		writeError(w, http.StatusBadRequest, "runner_id and positive epoch required")
		return
	}
	if len(req.Chunks) == 0 || len(req.Chunks) > 256 {
		writeError(w, http.StatusBadRequest, "chunks must be 1..256")
		return
	}
	t, err := s.store.AppendTerminalOutput(r.Context(), personaID, r.PathValue("session"),
		req.RunnerID, req.Epoch, req.Chunks)
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"session": t})
}

// reportTerminalStatus is the runner's lifecycle report: 'active'
// binds the provisioner operation; 'ended'/'lost' perform the
// terminal transition and its notification in one transaction.
func (s *Server) reportTerminalStatus(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	var req struct {
		RunnerID    string `json:"runner_id"`
		Epoch       int64  `json:"epoch"`
		Status      string `json:"status"`
		Reason      string `json:"reason"`
		ExitCode    *int   `json:"exit_code"`
		ExitSignal  string `json:"exit_signal"`
		OperationID string `json:"operation_id"`
	}
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	if req.RunnerID == "" || req.Epoch <= 0 || req.Status == "" {
		writeError(w, http.StatusBadRequest, "runner_id, positive epoch and status required")
		return
	}
	t, err := s.store.ReportTerminalStatus(r.Context(), personaID, r.PathValue("session"),
		req.RunnerID, req.Epoch, req.Status, req.Reason, req.ExitCode, req.ExitSignal, req.OperationID)
	if err != nil {
		if errors.Is(err, ErrTerminalNotClaimed) {
			writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "session": t})
			return
		}
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"session": t})
}

// sweepTerminalSessions is the standalone recovery route — same
// reasoning as jobs/sweep-expired: a quiet persona's lapsed claim
// should not wait for new work to become reclaimable.
func (s *Server) sweepTerminalSessions(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	out, err := s.store.SweepExpiredTerminalClaims(r.Context(), personaID)
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"interrupted": out})
}
