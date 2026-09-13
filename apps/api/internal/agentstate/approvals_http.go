package agentstate

import (
	"net/http"

	"github.com/sumi-studio/sumi/apps/api/internal/modelconnections"
)

// Approval list/read follow the persona scope — the core itself may need to
// observe its pending approvals — while a decision is admin-only: the route
// is the authenticated channel the host application uses to deliver the
// human's one-shot decision, not a capability the persona token holds.

func (s *Server) listApprovals(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	approvals, err := s.store.ListApprovals(r.Context(), personaID, r.URL.Query().Get("status"))
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"approvals": approvals})
}

func (s *Server) getApproval(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	a, err := s.store.GetApproval(r.Context(), personaID, r.PathValue("approval"))
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"approval": a})
}

func (s *Server) decideApproval(w http.ResponseWriter, r *http.Request) {
	personaID := r.PathValue("persona")
	if !uuidv7Re.MatchString(personaID) {
		writeError(w, http.StatusBadRequest, "persona must be a uuidv7")
		return
	}
	if !s.adminOnly(r) {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req ApprovalDecision
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	a, err := s.store.ResolveApproval(r.Context(), personaID, r.PathValue("approval"), req)
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"approval": a})
}

// ModelBinding is the persona's resolved model connection state for the
// core: the authoritative user selection plus, for an API connection, the
// connection metadata and — only when the credential store is armed — the
// decrypted key. The wire shape never substitutes a different model,
// preset, or endpoint: when the selection cannot be honored the binding
// says so and the core must fail rather than fall back.
type ModelBinding struct {
	Selection string `json:"selection"` // unset | none | api | chatgpt
	// Connection carries non-secret metadata for an api selection.
	Connection          *ModelConnectionBinding `json:"connection,omitempty"`
	APIKey              string                  `json:"api_key,omitempty"`
	CredentialAvailable bool                    `json:"credential_available"`
	Reason              string                  `json:"reason,omitempty"`
}

type ModelConnectionBinding struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Preset  string `json:"preset"`
	BaseURL string `json:"base_url"`
	Model   string `json:"model"`
	Version string `json:"version"`
}

// modelBinding resolves the persona's human's explicit model selection.
// The selection row is authoritative: it is re-read inside this call, and
// the returned identity (connection id + version) is exactly what the core
// must use — never a fallback.
func (s *Server) modelBinding(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	persona, err := s.store.persona(r.Context(), personaID)
	if err != nil {
		storeError(w, err)
		return
	}
	if s.conns == nil || persona.HumanID == nil {
		writeJSON(w, http.StatusOK, ModelBinding{Selection: "unset"})
		return
	}
	human := *persona.HumanID
	sel, exists, err := s.conns.Selected(r.Context(), human)
	if err != nil {
		storeError(w, err)
		return
	}
	if !exists {
		writeJSON(w, http.StatusOK, ModelBinding{Selection: "unset"})
		return
	}
	switch sel.Kind {
	case "none":
		writeJSON(w, http.StatusOK, ModelBinding{Selection: "none"})
	case "chatgpt":
		// The ChatGPT token is not served to the TS core in this slice —
		// the binding reports the selection honestly and the core fails.
		writeJSON(w, http.StatusOK, ModelBinding{
			Selection: "chatgpt",
			Reason:    "chatgpt connections are not yet served by the TypeScript core",
		})
	case "api":
		meta, err := s.conns.Describe(r.Context(), human, sel.ConnectionID)
		if err != nil {
			if err == modelconnections.ErrNotFound {
				writeJSON(w, http.StatusOK, ModelBinding{
					Selection: "api",
					Reason:    "selected API connection no longer exists",
				})
				return
			}
			storeError(w, err)
			return
		}
		binding := ModelBinding{
			Selection: "api",
			Connection: &ModelConnectionBinding{
				ID:      meta.Connection.ID,
				Name:    meta.Connection.Name,
				Preset:  meta.Connection.Preset,
				BaseURL: meta.Connection.BaseURL,
				Model:   meta.Connection.Model,
				Version: meta.Version,
			},
		}
		if s.conns.CredentialsAvailable() {
			access, err := s.conns.Resolve(r.Context(), human, sel.ConnectionID)
			if err != nil {
				storeError(w, err)
				return
			}
			binding.APIKey = access.APIKey
			binding.CredentialAvailable = true
		} else {
			binding.Reason = "credential unavailable: state service has no model-connection key"
		}
		writeJSON(w, http.StatusOK, binding)
	default:
		writeJSON(w, http.StatusOK, ModelBinding{Selection: sel.Kind, Reason: "unknown selection kind"})
	}
}
