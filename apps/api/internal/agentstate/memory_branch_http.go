package agentstate

import (
	"encoding/json"
	"net/http"
	"strconv"
)

func (s *Server) claimMemoryBranch(w http.ResponseWriter, r *http.Request) {
	persona, ok := s.scope(w, r)
	if !ok {
		return
	}
	var req struct {
		Generation int64           `json:"generation"`
		Snapshot   *BranchSnapshot `json:"snapshot"`
		Policy     json.RawMessage `json:"policy"`
	}
	if !decode(w, r, &req, 64<<20) || !requireGen(w, req.Generation) {
		return
	}
	b, err := s.store.ClaimMemoryBranch(r.Context(), persona, req.Generation, req.Snapshot, req.Policy)
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, b)
}
func (s *Server) saveMemoryBranch(w http.ResponseWriter, r *http.Request) {
	persona, ok := s.scope(w, r)
	if !ok {
		return
	}
	seq, err := strconv.ParseInt(r.PathValue("chunk"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "chunk must be an integer")
		return
	}
	var req struct {
		Generation int64           `json:"generation"`
		Revision   int64           `json:"revision"`
		State      json.RawMessage `json:"state"`
	}
	if !decode(w, r, &req, 64<<20) || !requireGen(w, req.Generation) {
		return
	}
	b, err := s.store.SaveMemoryBranch(r.Context(), persona, req.Generation, seq, req.Revision, req.State)
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, b)
}
