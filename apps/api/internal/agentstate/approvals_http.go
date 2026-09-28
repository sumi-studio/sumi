package agentstate

import (
	"encoding/json"
	"errors"
	"fmt"
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
	// unset | none | api | needs_rebinding
	Selection string `json:"selection"`
	// Intent echoes the carried model_intent when the persona arrived by
	// transfer: non-secret preference the destination must satisfy.
	Intent              json.RawMessage         `json:"intent,omitempty"`
	Connection          *ModelConnectionBinding `json:"connection,omitempty"`
	APIKey              string                  `json:"api_key,omitempty"`
	CredentialAvailable bool                    `json:"credential_available"`
	// CredentialState classifies an unavailable subscription credential
	// for the surfaces that render it: "reconnect_required" (the person
	// must sign in to ChatGPT again) or "disabled" (this server does not
	// offer subscription connections). Empty otherwise.
	CredentialState string `json:"credential_state,omitempty"`
	Reason          string `json:"reason,omitempty"`
}

type ModelConnectionBinding struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Preset  string `json:"preset"`
	BaseURL string `json:"base_url"`
	Model   string `json:"model"`
	Version string `json:"version"`
	// MaxOutputTokens is the connection's requested output bound
	// (non-secret metadata). Nil means the adapter's protocol default.
	MaxOutputTokens *int `json:"max_output_tokens,omitempty"`
	// ExtraHeaders are the connection's sealed per-request headers, sent
	// only with the credential material (armed store) — never in
	// metadata-only responses and never persisted into core state.
	ExtraHeaders map[string]string `json:"extra_headers,omitempty"`
	// AccountID is the ChatGPT account a subscription access token belongs
	// to (sent as ChatGPT-Account-ID). Only with credential material.
	AccountID string `json:"account_id,omitempty"`
	// ReasoningEffort is the connection's requested effort (subscription
	// connections); empty = adapter default.
	ReasoningEffort string `json:"reasoning_effort,omitempty"`
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
	s.writeModelBinding(w, r, personaID, "", "")
}

// refreshModelCredential is the core's report that the provider rejected
// the access token it was given (HTTP 401 before any output). It answers
// with the persona's current binding, exactly as GET .../model would, but
// for a subscription connection it first refreshes the grant unless the
// rejected token (named by its SHA-256, never sent back) was already
// replaced by a concurrent call. The rejection applies only to the
// connection the core names: a selection that changed meanwhile is simply
// returned as the new binding.
func (s *Server) refreshModelCredential(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	var req struct {
		ConnectionID string `json:"connection_id"`
		Rejected     string `json:"rejected_token_sha256"`
	}
	if !decode(w, r, &req, s.maxBody) {
		return
	}
	if req.ConnectionID == "" || len(req.Rejected) != 64 {
		writeError(w, http.StatusBadRequest, "connection_id and rejected_token_sha256 are required")
		return
	}
	s.writeModelBinding(w, r, personaID, req.ConnectionID, req.Rejected)
}

func (s *Server) writeModelBinding(w http.ResponseWriter, r *http.Request, personaID, rejectedConnection, rejectedDigest string) {
	persona, err := s.store.persona(r.Context(), personaID)
	if err != nil {
		storeError(w, err)
		return
	}
	// A carried model intent gates everything below: the destination must
	// satisfy it — a bound human selecting a connection of the same kind —
	// before any model may run. 'unset' is never the answer for a persona
	// that had a selection: silently falling back to an environment
	// default would run a model the user did not choose, or run a model
	// where they chose 'none'.
	var intentKind string
	if len(persona.ModelIntent) > 0 {
		var intent struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(persona.ModelIntent, &intent); err == nil {
			intentKind = intent.Kind
		}
	}
	rebind := func(reason string) {
		writeJSON(w, http.StatusOK, ModelBinding{
			Selection: "needs_rebinding",
			Intent:    persona.ModelIntent,
			Reason:    reason,
		})
	}
	if s.conns == nil || persona.HumanID == nil {
		if intentKind != "" {
			rebind(fmt.Sprintf("carried model intent '%s' needs a bound human's matching selection on this placement; bind a human and select a '%s' connection, or DELETE .../model/intent to start fresh",
				intentKind, intentKind))
			return
		}
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
		if intentKind != "" {
			rebind(fmt.Sprintf("carried model intent '%s' has no selection on this placement; select a '%s' connection for the bound human, or DELETE .../model/intent to start fresh",
				intentKind, intentKind))
			return
		}
		writeJSON(w, http.StatusOK, ModelBinding{Selection: "unset"})
		return
	}
	if intentKind != "" && sel.Kind != intentKind {
		rebind(fmt.Sprintf("carried model intent '%s' does not match the bound human's selection '%s'; select a '%s' connection, or DELETE .../model/intent to start fresh",
			intentKind, sel.Kind, intentKind))
		return
	}
	switch sel.Kind {
	case "none":
		writeJSON(w, http.StatusOK, ModelBinding{Selection: "none"})
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
				ID:              meta.Connection.ID,
				Name:            meta.Connection.Name,
				Preset:          meta.Connection.Preset,
				BaseURL:         meta.Connection.BaseURL,
				Model:           meta.Connection.Model,
				Version:         meta.Version,
				MaxOutputTokens: meta.Connection.MaxOutputTokens,
				ReasoningEffort: meta.Connection.ReasoningEffort,
			},
		}
		if meta.Connection.Preset == modelconnections.ChatGPTPreset {
			rejected := ""
			if rejectedConnection == meta.Connection.ID {
				rejected = rejectedDigest
			}
			s.writeChatGPTBinding(w, r, human, binding, rejected)
			return
		}
		if s.conns.CredentialsAvailable() {
			access, err := s.conns.Resolve(r.Context(), human, sel.ConnectionID)
			if err != nil {
				storeError(w, err)
				return
			}
			binding.APIKey = access.APIKey
			binding.Connection.ExtraHeaders = access.ExtraHeaders
			binding.CredentialAvailable = true
		} else {
			binding.Reason = "credential unavailable: state service has no model-connection key"
		}
		writeJSON(w, http.StatusOK, binding)
	default:
		writeJSON(w, http.StatusOK, ModelBinding{Selection: sel.Kind, Reason: "unknown selection kind"})
	}
}

// writeChatGPTBinding resolves a subscription connection's access token,
// refreshing under the connection's row lock when needed. A grant that can
// no longer be refreshed is a definite, user-fixable unavailability
// (reconnect), never a fallback; a transient refresh failure is a 503 the
// core retries like a provider outage.
func (s *Server) writeChatGPTBinding(w http.ResponseWriter, r *http.Request, human string, binding ModelBinding, rejectedDigest string) {
	access, err := s.conns.ResolveChatGPT(r.Context(), human, binding.Connection.ID, rejectedDigest)
	switch {
	case err == nil:
		binding.APIKey = access.AccessToken
		binding.Connection.AccountID = access.AccountID
		binding.Connection.Version = access.Version
		binding.CredentialAvailable = true
	case errors.Is(err, modelconnections.ErrReconnectRequired):
		binding.CredentialState = "reconnect_required"
		binding.Reason = "the ChatGPT sign-in for this connection has expired or was revoked; reconnect ChatGPT in AI connection settings"
	case errors.Is(err, modelconnections.ErrChatGPTDisabled):
		binding.CredentialState = "disabled"
		binding.Reason = "ChatGPT subscription connections are not enabled on this server"
	case errors.Is(err, modelconnections.ErrNotFound):
		binding.Connection = nil
		binding.Reason = "selected API connection no longer exists"
	case errors.Is(err, modelconnections.ErrRefreshFailed):
		writeError(w, http.StatusServiceUnavailable, "ChatGPT credential refresh failed; retry")
		return
	default:
		storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, binding)
}
