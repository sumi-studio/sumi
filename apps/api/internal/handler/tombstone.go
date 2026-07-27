package handler

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/store"
)

type TombstoneRequest struct {
	TenantID                  string               `json:"tenant_id"`
	AgentID                   string               `json:"agent_id"`
	ConversationID            string               `json:"conversation_id"`
	ReplacementConversationID string               `json:"replacement_conversation_id"`
	CommandID                 string               `json:"command_id"`
	CommandSeq                *int64               `json:"command_seq"`
	Scope                     store.TombstoneScope `json:"scope"`
	PurgeAfter                time.Time            `json:"purge_after"`
}

type TombstoneAdvanceRequest struct {
	From store.TombstoneStatus `json:"from"`
	To   store.TombstoneStatus `json:"to"`
}

type TombstoneFenceRequest struct {
	Generation int64  `json:"generation"`
	LeaseID    string `json:"lease_id"`
	FenceID    string `json:"fence_id"`
}

func RegisterTombstoneRoutes(mux *http.ServeMux, s *store.TombstoneStore) {
	mux.HandleFunc("POST /tombstones", func(w http.ResponseWriter, r *http.Request) {
		var req TombstoneRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		tmb, err := s.Create(
			req.TenantID,
			req.AgentID,
			req.ConversationID,
			req.ReplacementConversationID,
			req.CommandID,
			req.CommandSeq,
			req.Scope,
			req.PurgeAfter,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(tmb)
	})

	mux.HandleFunc("GET /tombstones/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		tmb, err := s.Get(id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(tmb)
	})

	mux.HandleFunc("POST /tombstones/{id}/advance", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		var req TombstoneAdvanceRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		tmb, err := s.Advance(id, req.From, req.To)
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		json.NewEncoder(w).Encode(tmb)
	})

	mux.HandleFunc("POST /tombstones/{id}/generation-fence", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		var req TombstoneFenceRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		tmb, err := s.RecordFence(id, req.Generation, req.LeaseID, req.FenceID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		json.NewEncoder(w).Encode(tmb)
	})
}
