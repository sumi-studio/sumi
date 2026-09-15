package messaging

import (
	"context"
	"errors"
	"net/http"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

// EventCoreApprovalChanged is the live nudge that a secretary's approval set
// changed for the connection's own Human — a parked call asking for a
// decision or a recorded one. It carries no payload: the durable
// /me/approvals inbox is the record, the event only tells the client to
// re-read it. Delivery is subject-scoped and OnlyFor the deciding human, so
// no other Workspace member ever sees that a secretary is waiting.
const EventCoreApprovalChanged = "core_approval_changed"

// CoreApprovalsServer is the authenticated browser surface for the shared
// TypeScript core's durable tool approvals. It answers "what does my
// secretary want to run" (GET /me/approvals) and records the human's
// one-shot decision (POST /me/approvals/{approval}/decision).
//
// The deciding identity is always derived server-side from the verified
// browser session — the request body carries only the decision vocabulary
// and its idempotency id, and the persona's bound human is enforced again
// inside Store.ResolveApproval, so a copied approval id or a stale session
// cannot act on another account's secretary.
type CoreApprovalsServer struct {
	Core           *agentstate.Store
	Messaging      *Store
	Hub            *Hub
	Sessions       agentevents.UserSessionAuthorizer
	AllowedOrigins []string
}

// RegisterRoutes mounts the human-scoped approval inbox on the public mux.
// A nil Core or Sessions fails closed on each request rather than at mount,
// matching the other browser surfaces.
func (s *CoreApprovalsServer) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /me/approvals", s.serveList)
	mux.HandleFunc("POST /me/approvals/{approval}/decision", s.serveDecision)
}

// session authenticates the browser lane: an exact-origin check for unsafe
// methods, exactly one signed session cookie, and a verified session whose
// human is the acting identity.
func (s *CoreApprovalsServer) session(w http.ResponseWriter, r *http.Request) (agentevents.UserSessionClaims, bool) {
	var none agentevents.UserSessionClaims
	if r.Method != http.MethodGet && !agentevents.BrowserOriginAllowed(r, s.AllowedOrigins) {
		writeError(w, http.StatusForbidden, "origin_not_allowed")
		return none, false
	}
	cookies := r.CookiesNamed(agentevents.BrowserSessionCookie)
	switch {
	case len(cookies) > 1:
		writeError(w, http.StatusBadRequest, "duplicate_session_cookies")
		return none, false
	case len(cookies) == 0 || s.Sessions == nil:
		writeError(w, http.StatusUnauthorized, "missing_session")
		return none, false
	}
	claims, err := s.Sessions.VerifySession(r.Context(), cookies[0].Value)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid_session")
		return none, false
	}
	if s.Core == nil {
		writeError(w, http.StatusServiceUnavailable, "approvals_unavailable")
		return none, false
	}
	return claims, true
}

func (s *CoreApprovalsServer) serveList(w http.ResponseWriter, r *http.Request) {
	// Another person's approvals must never sit in a shared cache.
	w.Header().Set("Cache-Control", "no-store")
	claims, ok := s.session(w, r)
	if !ok {
		return
	}
	approvals, err := s.Core.ListHumanApprovals(r.Context(), claims.UserID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list approvals")
		return
	}
	// `human` echoes the session human the rows belong to: the client tags its
	// projection with it so data loaded under one account can never be
	// rendered under another, even for a single committed render.
	writeJSON(w, http.StatusOK, map[string]any{
		"approvals": approvals,
		"human":     claims.UserID,
	})
}

// approvalDecisionBody is the whole browser decision vocabulary: which
// one-shot decision and its idempotency id. The decider is the session —
// actor fields sent by the browser are rejected as unknown, never trusted.
type approvalDecisionBody struct {
	Decision   string `json:"decision"`
	DecisionID string `json:"decision_id"`
}

func (s *CoreApprovalsServer) serveDecision(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	claims, ok := s.session(w, r)
	if !ok {
		return
	}
	approvalID := r.PathValue("approval")
	var body approvalDecisionBody
	if !decodeJSON(w, r, &body) {
		return
	}
	record, err := s.Core.ApprovalByID(r.Context(), approvalID)
	if err != nil {
		s.writeCoreError(w, err)
		return
	}
	called := false
	err = s.Sessions.AuthorizeSession(r.Context(), claims, func() error {
		called = true
		_, resolveErr := s.Core.ResolveApproval(r.Context(), record.PersonaID, approvalID, agentstate.ApprovalDecision{
			Decision:      body.Decision,
			DecisionID:    body.DecisionID,
			DecidedByKind: "human",
			DecidedByID:   claims.UserID,
		})
		return resolveErr
	})
	if !called {
		writeError(w, http.StatusUnauthorized, "invalid_session")
		return
	}
	if err != nil {
		s.writeCoreError(w, err)
		return
	}
	// Answer with the same enriched projection the inbox list returns, so the
	// just-resolved card keeps its secretary name and input provenance instead
	// of degrading to the bare grant until the next refresh.
	enriched, err := s.Core.HumanApprovalByID(r.Context(), claims.UserID, approvalID)
	if err != nil {
		s.writeCoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"approval": enriched})
}

// writeCoreError maps the core decision refusal to a stable wire code the
// client can render without parsing Go error text.
func (s *CoreApprovalsServer) writeCoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, agentstate.ErrApprovalNotFound):
		writeError(w, http.StatusNotFound, "approval_not_found")
	case errors.Is(err, agentstate.ErrApprovalForbidden):
		writeError(w, http.StatusForbidden, "forbidden")
	case errors.Is(err, agentstate.ErrPersonaInactive):
		// The persona's authority moved (seal/stage/transfer): this placement
		// can never take the decision, so the refusal is terminal, not a
		// retryable conflict — the parked grant travels with the persona.
		writeError(w, http.StatusConflict, "persona_inactive")
	case errors.Is(err, agentstate.ErrApprovalConflict):
		writeError(w, http.StatusConflict, "approval_conflict")
	case errors.Is(err, agentstate.ErrPersonaNotFound):
		writeError(w, http.StatusConflict, "persona_not_found")
	case errors.Is(err, agentstate.ErrBadRequest),
		errors.Is(err, agentstate.ErrApprovalDecidedBy):
		writeError(w, http.StatusBadRequest, "invalid_decision")
	default:
		writeError(w, http.StatusInternalServerError, "approval decision")
	}
}

// NotifyChanged publishes the live approval nudge for the persona's bound
// human over the existing Messaging socket: one subject-scoped event per
// Workspace the human actively belongs to, OnlyFor that human. A human with
// no Workspace membership has no socket to reach; the inbox re-read on next
// open covers them. Best-effort by construction — callers ignore failure.
func (s *CoreApprovalsServer) NotifyChanged(ctx context.Context, personaID string) {
	if s == nil || s.Core == nil || s.Messaging == nil || s.Hub == nil {
		return
	}
	humanID, err := s.Core.PersonaHumanID(ctx, personaID)
	if err != nil || humanID == "" {
		return
	}
	s.NotifyHuman(ctx, humanID)
}

// NotifyHuman fans the nudge to every live Workspace scope the human
// participates in — wherever their Messaging socket currently lives.
func (s *CoreApprovalsServer) NotifyHuman(ctx context.Context, humanID string) {
	rows, err := s.Messaging.pool.Query(ctx, `
		SELECT workspace_id::text FROM workspace_members
		WHERE member_kind = $1 AND member_id = $2 AND left_at IS NULL`,
		string(KindHuman), humanID)
	if err != nil {
		return
	}
	var workspaceIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil {
			workspaceIDs = append(workspaceIDs, id)
		}
	}
	rows.Close()
	for _, workspaceID := range workspaceIDs {
		scope, err := s.messagingScope(ctx, workspaceID)
		if err != nil {
			continue
		}
		human := Human(humanID)
		_ = s.Hub.PublishSystemScoped(ctx, scope, Event{
			Type:    EventCoreApprovalChanged,
			Subject: &human,
			OnlyFor: &human,
		})
	}
}

// messagingScope resolves the Workspace's current enabled Messaging
// installation the same way the core send path does — the live authority
// epoch, never a frozen one.
func (s *CoreApprovalsServer) messagingScope(ctx context.Context, workspaceID string) (Scope, error) {
	var installationID string
	var epoch int64
	err := s.Messaging.pool.QueryRow(ctx, `
		SELECT installation_id::text, authority_epoch
		FROM app_installations
		WHERE owner_kind = 'workspace' AND owner_id = $1
		  AND app_id = $2 AND enabled
		ORDER BY installed_at, installation_id LIMIT 1`,
		workspaceID, MessagingAppID).Scan(&installationID, &epoch)
	if err != nil {
		return Scope{}, err
	}
	return Scope{
		WorkspaceID: workspaceID, InstallationID: installationID,
		AuthorityEpoch: epoch,
	}, nil
}
