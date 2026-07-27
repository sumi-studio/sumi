package handler

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/store"
)

// RuntimeIdentity is issued by the control plane to one agent runtime.  The
// internal lifecycle API never trusts tenant/agent values supplied in JSON.
type RuntimeIdentity struct {
	TenantID string
	AgentID  string
	Token    string
}

func RuntimeIdentityFromEnv() (RuntimeIdentity, bool) {
	i := RuntimeIdentity{os.Getenv("SUMI_CONTROL_PLANE_TENANT_ID"), os.Getenv("SUMI_CONTROL_PLANE_AGENT_ID"), os.Getenv("SUMI_CONTROL_PLANE_RUNTIME_TOKEN")}
	return i, i.TenantID != "" && i.AgentID != "" && i.Token != ""
}

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
	identity, ok := RuntimeIdentityFromEnv()
	RegisterAuthenticatedTombstoneRoutes(mux, s, identity, ok)
}

// RegisterAuthenticatedTombstoneRoutes is deliberately internal-only. User
// lifecycle APIs must authenticate the user and mint a supervisor request;
// they must not call these agent-runtime routes with caller-selected scope.
func RegisterAuthenticatedTombstoneRoutes(mux *http.ServeMux, s *store.TombstoneStore, identity RuntimeIdentity, configured bool) {
	authorize := func(w http.ResponseWriter, r *http.Request) bool {
		if !configured {
			http.Error(w, "internal lifecycle identity is not configured", http.StatusServiceUnavailable)
			return false
		}
		if r.Header.Get("X-Sumi-Internal-Principal") != "agent-runtime" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+identity.Token)) != 1 {
			http.Error(w, "unauthenticated internal lifecycle request", http.StatusUnauthorized)
			return false
		}
		return true
	}
	owned := func(w http.ResponseWriter, t *store.Tombstone) bool {
		if t.TenantID != identity.TenantID || t.AgentID != identity.AgentID {
			http.Error(w, "tombstone does not belong to authenticated runtime", http.StatusForbidden)
			return false
		}
		return true
	}
	mux.HandleFunc("POST /internal/agent/tombstones", func(w http.ResponseWriter, r *http.Request) {
		if !authorize(w, r) {
			return
		}
		var req TombstoneRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.TenantID != identity.TenantID || req.AgentID != identity.AgentID {
			http.Error(w, "request identity differs from authenticated runtime", http.StatusForbidden)
			return
		}
		if req.Scope != store.ConversationScope {
			http.Error(w, "agent runtime may only request conversation-reset tombstones", http.StatusForbidden)
			return
		}
		t, err := s.Create(req.TenantID, req.AgentID, req.ConversationID, req.ReplacementConversationID, req.CommandID, req.CommandSeq, req.Scope, req.PurgeAfter)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(t)
	})
	mux.HandleFunc("GET /internal/agent/tombstones", func(w http.ResponseWriter, r *http.Request) {
		if !authorize(w, r) {
			return
		}
		_ = json.NewEncoder(w).Encode(s.ListForAgent(identity.TenantID, identity.AgentID))
	})
	mux.HandleFunc("GET /internal/agent/tombstones/by-command/{command_id}", func(w http.ResponseWriter, r *http.Request) {
		if !authorize(w, r) {
			return
		}
		t, err := s.FindByCommand(identity.TenantID, identity.AgentID, r.PathValue("command_id"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(t)
	})
	mux.HandleFunc("GET /internal/agent/tombstones/{id}", func(w http.ResponseWriter, r *http.Request) {
		if !authorize(w, r) {
			return
		}
		t, err := s.Get(r.PathValue("id"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		if !owned(w, t) {
			return
		}
		_ = json.NewEncoder(w).Encode(t)
	})
	mux.HandleFunc("POST /internal/agent/tombstones/{id}/advance", func(w http.ResponseWriter, r *http.Request) {
		if !authorize(w, r) {
			return
		}
		id := r.PathValue("id")
		t, err := s.Get(id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		if !owned(w, t) {
			return
		}
		var req TombstoneAdvanceRequest
		if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		t, err = s.Advance(id, req.From, req.To)
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		_ = json.NewEncoder(w).Encode(t)
	})
	mux.HandleFunc("POST /internal/agent/tombstones/{id}/generation-fence", func(w http.ResponseWriter, r *http.Request) {
		if !authorize(w, r) {
			return
		}
		id := r.PathValue("id")
		t, err := s.Get(id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		if !owned(w, t) {
			return
		}
		var req TombstoneFenceRequest
		if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		t, err = s.RecordFence(id, req.Generation, req.LeaseID, req.FenceID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		_ = json.NewEncoder(w).Encode(t)
	})
}
