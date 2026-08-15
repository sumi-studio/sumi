package workspace

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
	"github.com/sumi-studio/sumi/apps/api/internal/directchat"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

const maxControlPlaneRequestBytes = 64 * 1024

type Server struct {
	Store          *Store
	Apps           *applicationapps.Store
	Sessions       agentevents.UserSessionAuthorizer
	AllowedOrigins []string
}

func NewServer(store *Store, apps *applicationapps.Store, sessions agentevents.UserSessionAuthorizer) *Server {
	return &Server{Store: store, Apps: apps, Sessions: sessions}
}

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /workspaces", s.serveWorkspaces)
	mux.HandleFunc("POST /workspaces", s.serveCreateWorkspace)
	mux.HandleFunc("GET /workspaces/{workspace_id}", s.serveWorkspace)
	mux.HandleFunc("PATCH /workspaces/{workspace_id}", s.serveUpdateWorkspace)
	mux.HandleFunc("PUT /workspaces/{workspace_id}/owner", s.serveTransferWorkspaceOwnership)
	mux.HandleFunc("GET /workspaces/{workspace_id}/members", s.serveMembers)
	mux.HandleFunc("DELETE /workspaces/{workspace_id}/membership", s.serveLeaveWorkspace)
	mux.HandleFunc("DELETE /workspaces/{workspace_id}/members/{workspace_member_id}", s.serveRemoveMember)
	mux.HandleFunc("POST /workspaces/{workspace_id}/invites", s.serveCreateInvite)
	mux.HandleFunc("GET /workspaces/{workspace_id}/invites", s.serveInvites)
	mux.HandleFunc("DELETE /workspaces/{workspace_id}/invites/{invite_id}", s.serveRevokeInvite)
	mux.HandleFunc("GET /workspace-invites/preview", s.servePreviewInvite)
	mux.HandleFunc("POST /workspace-invites/redeem", s.serveRedeemInvite)
	mux.HandleFunc("GET /workspaces/{workspace_id}/roles", s.serveRoles)
	mux.HandleFunc("POST /workspaces/{workspace_id}/roles", s.serveCreateRole)
	mux.HandleFunc("PATCH /workspaces/{workspace_id}/roles/{role_id}", s.serveUpdateRole)
	mux.HandleFunc("DELETE /workspaces/{workspace_id}/roles/{role_id}", s.serveDeleteRole)
	mux.HandleFunc("PUT /workspaces/{workspace_id}/members/{workspace_member_id}/roles", s.serveSetMemberRoles)
	mux.HandleFunc("GET /apps/catalog", s.serveAppCatalog)
	mux.HandleFunc("GET /app-installations", s.serveAppInstallations)
	mux.HandleFunc("POST /app-installations", s.serveInstallApp)
	mux.HandleFunc("PUT /app-installations/{installation_id}/state", s.serveSetAppEnabled)
	mux.HandleFunc("DELETE /app-installations/{installation_id}", s.serveUninstallApp)
}

type participantWire struct {
	Kind               string `json:"kind"`
	HumanID            string `json:"human_id,omitempty"`
	PersonalityAgentID string `json:"personality_agent_id,omitempty"`
}

func participantToWire(ref participant.Ref) participantWire {
	wire := participantWire{Kind: string(ref.Kind)}
	if ref.Kind == participant.KindHuman {
		wire.HumanID = ref.ID
	} else {
		wire.PersonalityAgentID = ref.ID
	}
	return wire
}

func (wire participantWire) ref() (participant.Ref, error) {
	var ref participant.Ref
	switch participant.Kind(wire.Kind) {
	case participant.KindHuman:
		if wire.PersonalityAgentID != "" {
			return participant.Ref{}, errors.New("Human participant contains PersonalityAgentId")
		}
		ref = participant.Human(wire.HumanID)
	case participant.KindPersonalityAgent:
		if wire.HumanID != "" {
			return participant.Ref{}, errors.New("PersonalityAgent participant contains HumanId")
		}
		ref = participant.PersonalityAgent(wire.PersonalityAgentID)
	default:
		return participant.Ref{}, errors.New("unknown participant kind")
	}
	if err := ref.Validate(); err != nil {
		return participant.Ref{}, err
	}
	return ref, nil
}

type workspaceWire struct {
	WorkspaceID            string    `json:"workspace_id"`
	Name                   string    `json:"name"`
	OwnerWorkspaceMemberID string    `json:"owner_workspace_member_id"`
	CreatedAt              time.Time `json:"created_at"`
}

func workspaceToWire(item Workspace) workspaceWire {
	return workspaceWire{
		WorkspaceID: item.WorkspaceID, Name: item.Name,
		OwnerWorkspaceMemberID: item.OwnerWorkspaceMemberID,
		CreatedAt:              item.CreatedAt,
	}
}

type membershipWire struct {
	WorkspaceMemberID string          `json:"workspace_member_id"`
	WorkspaceID       string          `json:"workspace_id"`
	Participant       participantWire `json:"participant"`
	DisplayName       string          `json:"display_name"`
	Owner             bool            `json:"owner"`
	RoleIDs           []string        `json:"role_ids"`
	JoinedAt          time.Time       `json:"joined_at"`
	LeftAt            *time.Time      `json:"left_at"`
}

func membershipToWire(item Membership) membershipWire {
	roleIDs := item.RoleIDs
	if roleIDs == nil {
		roleIDs = []string{}
	}
	displayName := item.DisplayName
	if strings.TrimSpace(displayName) == "" {
		displayName = participantDisplayNameFallback(item.Participant)
	}
	return membershipWire{
		WorkspaceMemberID: item.WorkspaceMemberID, WorkspaceID: item.WorkspaceID,
		Participant: participantToWire(item.Participant), DisplayName: displayName,
		Owner:   item.Owner,
		RoleIDs: roleIDs, JoinedAt: item.JoinedAt, LeftAt: item.LeftAt,
	}
}

type roleWire struct {
	RoleID      string    `json:"role_id"`
	WorkspaceID string    `json:"workspace_id"`
	Name        string    `json:"name"`
	Color       string    `json:"color,omitempty"`
	Position    int       `json:"position"`
	Permissions []string  `json:"permissions"`
	CreatedAt   time.Time `json:"created_at"`
}

func roleToWire(item Role) roleWire {
	return roleWire{
		RoleID: item.RoleID, WorkspaceID: item.WorkspaceID, Name: item.Name,
		Color: item.Color, Position: item.Position, Permissions: item.CapabilityRefs(),
		CreatedAt: item.CreatedAt,
	}
}

type inviteWire struct {
	InviteID    string    `json:"invite_id"`
	WorkspaceID string    `json:"workspace_id"`
	Code        string    `json:"code"`
	ExpiresAt   time.Time `json:"expires_at"`
	CreatedAt   time.Time `json:"created_at"`
}

func inviteToWire(item Invite) inviteWire {
	return inviteWire{
		InviteID: item.InviteID, WorkspaceID: item.WorkspaceID, Code: item.Code,
		ExpiresAt: item.ExpiresAt, CreatedAt: item.CreatedAt,
	}
}

type inviteRecordWire struct {
	InviteID    string    `json:"invite_id"`
	WorkspaceID string    `json:"workspace_id"`
	ExpiresAt   time.Time `json:"expires_at"`
	CreatedAt   time.Time `json:"created_at"`
}

func inviteRecordToWire(item InviteRecord) inviteRecordWire {
	return inviteRecordWire{
		InviteID: item.InviteID, WorkspaceID: item.WorkspaceID,
		ExpiresAt: item.ExpiresAt, CreatedAt: item.CreatedAt,
	}
}

type invitePreviewWire struct {
	WorkspaceID   string    `json:"workspace_id"`
	WorkspaceName string    `json:"workspace_name"`
	ExpiresAt     time.Time `json:"expires_at"`
}

func invitePreviewToWire(item InvitePreview) invitePreviewWire {
	return invitePreviewWire{
		WorkspaceID: item.WorkspaceID, WorkspaceName: item.WorkspaceName,
		ExpiresAt: item.ExpiresAt,
	}
}

type appOwnerWire struct {
	Kind        string           `json:"kind"`
	WorkspaceID string           `json:"workspace_id,omitempty"`
	Participant *participantWire `json:"participant,omitempty"`
}

func appOwnerToWire(owner applicationapps.OwnerRef) appOwnerWire {
	wire := appOwnerWire{Kind: string(owner.Kind)}
	switch owner.Kind {
	case applicationapps.OwnerWorkspace:
		wire.WorkspaceID = owner.WorkspaceID
	case applicationapps.OwnerParticipant:
		participant := participantToWire(owner.ParticipantRef)
		wire.Participant = &participant
	}
	return wire
}

func (wire appOwnerWire) ref() (applicationapps.OwnerRef, error) {
	owner := applicationapps.OwnerRef{Kind: applicationapps.OwnerKind(wire.Kind)}
	switch owner.Kind {
	case applicationapps.OwnerWorkspace:
		if wire.Participant != nil {
			return applicationapps.OwnerRef{}, errors.New("Workspace app owner contains ParticipantRef")
		}
		owner.WorkspaceID = wire.WorkspaceID
	case applicationapps.OwnerParticipant:
		if wire.WorkspaceID != "" || wire.Participant == nil {
			return applicationapps.OwnerRef{}, errors.New("Participant app owner has an invalid shape")
		}
		ref, err := wire.Participant.ref()
		if err != nil {
			return applicationapps.OwnerRef{}, err
		}
		owner.ParticipantRef = ref
	default:
		return applicationapps.OwnerRef{}, errors.New("unknown app owner kind")
	}
	if err := owner.Validate(); err != nil {
		return applicationapps.OwnerRef{}, err
	}
	return owner, nil
}

type appDescriptorWire struct {
	AppID                     string                        `json:"app_id"`
	DisplayName               string                        `json:"display_name"`
	WorkspaceOwnerAllowed     bool                          `json:"workspace_owner_allowed"`
	ParticipantOwnerAllowed   bool                          `json:"participant_owner_allowed"`
	WorkspaceRoleCapabilities []workspaceRoleCapabilityWire `json:"workspace_role_capabilities"`
}

type workspaceRoleCapabilityWire struct {
	Ref   string `json:"ref"`
	Label string `json:"label"`
}

func descriptorToWire(item applicationapps.Descriptor) appDescriptorWire {
	capabilities := make([]workspaceRoleCapabilityWire, len(item.WorkspaceRoleCapabilities))
	for i, capability := range item.WorkspaceRoleCapabilities {
		capabilities[i] = workspaceRoleCapabilityWire{Ref: capability.Ref, Label: capability.Label}
	}
	return appDescriptorWire{
		AppID: item.AppID, DisplayName: item.DisplayName,
		WorkspaceOwnerAllowed:     item.WorkspaceOwnerAllowed,
		ParticipantOwnerAllowed:   item.ParticipantOwnerAllowed,
		WorkspaceRoleCapabilities: capabilities,
	}
}

type appInstallationWire struct {
	InstallationID string       `json:"installation_id"`
	Owner          appOwnerWire `json:"owner"`
	AppID          string       `json:"app_id"`
	State          string       `json:"state"`
	AuthorityEpoch string       `json:"authority_epoch"`
	InstalledAt    time.Time    `json:"installed_at"`
	UpdatedAt      time.Time    `json:"updated_at"`
}

func installationToWire(item applicationapps.Installation) appInstallationWire {
	return appInstallationWire{
		InstallationID: item.InstallationID, Owner: appOwnerToWire(item.Owner),
		AppID: item.AppID, State: string(item.State),
		AuthorityEpoch: strconv.FormatInt(item.AuthorityEpoch, 10), InstalledAt: item.InstalledAt,
		UpdatedAt: item.UpdatedAt,
	}
}

func appOwnerFromQuery(r *http.Request) (applicationapps.OwnerRef, error) {
	kind := applicationapps.OwnerKind(r.URL.Query().Get("owner_kind"))
	ownerID := r.URL.Query().Get("owner_id")
	owner := applicationapps.OwnerRef{Kind: kind}
	switch kind {
	case applicationapps.OwnerWorkspace:
		if r.URL.Query().Get("participant_kind") != "" {
			return applicationapps.OwnerRef{}, errors.New("Workspace owner contains participant_kind")
		}
		owner.WorkspaceID = ownerID
	case applicationapps.OwnerParticipant:
		owner.ParticipantRef = participant.Ref{
			Kind: participant.Kind(r.URL.Query().Get("participant_kind")), ID: ownerID,
		}
	}
	if err := owner.Validate(); err != nil {
		return applicationapps.OwnerRef{}, err
	}
	return owner, nil
}

func (s *Server) browserActor(w http.ResponseWriter, r *http.Request) (participant.Ref, agentevents.UserSessionClaims, bool) {
	var none agentevents.UserSessionClaims
	if r.Method != http.MethodGet && !agentevents.BrowserOriginAllowed(r, s.AllowedOrigins) {
		writeAPIError(w, http.StatusForbidden, "origin_not_allowed")
		return participant.Ref{}, none, false
	}
	cookies := r.CookiesNamed(agentevents.BrowserSessionCookie)
	switch {
	case len(cookies) > 1:
		writeAPIError(w, http.StatusBadRequest, "duplicate_session_cookies")
		return participant.Ref{}, none, false
	case len(cookies) == 0 || s.Sessions == nil:
		writeAPIError(w, http.StatusUnauthorized, "missing_session")
		return participant.Ref{}, none, false
	}
	claims, err := s.Sessions.VerifySession(r.Context(), cookies[0].Value)
	if err != nil {
		writeAPIError(w, http.StatusUnauthorized, "invalid_session")
		return participant.Ref{}, none, false
	}
	actor := participant.Human(claims.UserID)
	if err := actor.Validate(); err != nil {
		writeAPIError(w, http.StatusUnauthorized, "invalid_session")
		return participant.Ref{}, none, false
	}
	return actor, claims, true
}

func (s *Server) browserMutation(w http.ResponseWriter, r *http.Request, claims agentevents.UserSessionClaims, operation func() error) (bool, error) {
	called := false
	err := s.Sessions.AuthorizeSession(r.Context(), claims, func() error {
		called = true
		return operation()
	})
	if !called {
		writeAPIError(w, http.StatusUnauthorized, "invalid_session")
		return false, nil
	}
	return true, err
}

func decodeStrictJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxControlPlaneRequestBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeAPIError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, struct {
		Error string `json:"error"`
	}{Error: code})
}

func writeDomainError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrRoleNotFound),
		errors.Is(err, ErrInviteUnavailable),
		errors.Is(err, applicationapps.ErrAppNotFound),
		errors.Is(err, applicationapps.ErrInstallationNotFound):
		writeAPIError(w, http.StatusNotFound, "not_found")
	case errors.Is(err, ErrForbidden),
		errors.Is(err, applicationapps.ErrForbidden):
		writeAPIError(w, http.StatusForbidden, "forbidden")
	case errors.Is(err, ErrOwnerProtected):
		writeAPIError(w, http.StatusForbidden, "owner_protected")
	case errors.Is(err, ErrMemberNotFound):
		writeAPIError(w, http.StatusNotFound, "membership_not_active")
	case errors.Is(err, ErrLastAdministrator):
		writeAPIError(w, http.StatusConflict, "last_administrator")
	case errors.Is(err, ErrAlreadyMember), errors.Is(err, ErrRoleNameTaken),
		errors.Is(err, applicationapps.ErrAlreadyInstalled):
		writeAPIError(w, http.StatusConflict, "conflict")
	case errors.Is(err, applicationapps.ErrAuthorityEpochStale):
		writeAPIError(w, http.StatusConflict, "stale_authority")
	case errors.Is(err, ErrInvalidName), errors.Is(err, ErrInvalidColor),
		errors.Is(err, ErrInvalidPosition), errors.Is(err, ErrInvalidPermission),
		errors.Is(err, ErrInvalidInvite), errors.Is(err, ErrInvalidWorkspaceListCursor),
		errors.Is(err, applicationapps.ErrOwnerKindUnsupported):
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
	case errors.Is(err, directchat.ErrLifecycleFenceUnavailable):
		writeAPIError(w, http.StatusServiceUnavailable, "unavailable")
	default:
		writeAPIError(w, http.StatusInternalServerError, "internal_error")
	}
}

func parsePositiveInt(value string) (int, error) {
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		return 0, fmt.Errorf("positive integer required")
	}
	return parsed, nil
}
