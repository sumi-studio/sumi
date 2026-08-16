package workspace

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

const (
	workspaceListCursorVersion      = byte(2)
	workspaceListCursorPayloadBytes = 1 + 8 + 16
	workspaceListCursorMACBytes     = sha256.Size
	workspaceListCursorBytes        = workspaceListCursorPayloadBytes + workspaceListCursorMACBytes
	// Raw base64url of the fixed 57-byte payload and MAC is exactly 76 bytes.
	workspaceListCursorEncodedBytes = 76
	localWorkspaceListResponseBytes = 64 * 1024
	workspaceListCursorDomain       = "sumi-workspace-list-cursor-v2\x00"

	workspaceInvitationListCursorVersion      = byte(1)
	workspaceInvitationListCursorPayloadBytes = 1 + 8 + 16
	workspaceInvitationListCursorMACBytes     = sha256.Size
	workspaceInvitationListCursorBytes        = workspaceInvitationListCursorPayloadBytes + workspaceInvitationListCursorMACBytes
	workspaceInvitationListCursorEncodedBytes = 76
	workspaceInvitationListCursorDomain       = "sumi-workspace-invitation-list-cursor-v1\x00"
)

const (
	LocalWorkspacesPath       = "/local-control/v1/workspace:list"
	LocalWorkspaceCreatePath  = "/local-control/v1/workspace:create"
	LocalWorkspaceGetPath     = "/local-control/v1/workspace:get"
	LocalWorkspaceUpdatePath  = "/local-control/v1/workspace:update"
	LocalWorkspaceOwnerPath   = "/local-control/v1/workspace:transfer-owner"
	LocalMembersPath          = "/local-control/v1/workspace:members"
	LocalLeavePath            = "/local-control/v1/workspace:leave"
	LocalRemoveMemberPath     = "/local-control/v1/workspace:remove-member"
	LocalInviteCreatePath     = "/local-control/v1/workspace:invite-create"
	LocalInvitesPath          = "/local-control/v1/workspace:invites"
	LocalInvitePreviewPath    = "/local-control/v1/workspace:invite-preview"
	LocalInviteRedeemPath     = "/local-control/v1/workspace:invite-redeem"
	LocalInviteRevokePath     = "/local-control/v1/workspace:invite-revoke"
	LocalInvitationListPath   = "/local-control/v1/workspace:invitation-list"
	LocalInvitationAcceptPath = "/local-control/v1/workspace:invitation-accept"
	LocalRolesPath            = "/local-control/v1/workspace:roles"
	LocalRoleCreatePath       = "/local-control/v1/workspace:role-create"
	LocalRoleUpdatePath       = "/local-control/v1/workspace:role-update"
	LocalRoleDeletePath       = "/local-control/v1/workspace:role-delete"
	LocalMemberRolesPath      = "/local-control/v1/workspace:member-roles"
	LocalAppCatalogPath       = "/local-control/v1/apps:catalog"
	LocalAppInstallationsPath = "/local-control/v1/apps:installations"
	LocalAppResolvePath       = "/local-control/v1/apps:resolve-enabled"
	LocalAppInstallPath       = "/local-control/v1/apps:install"
	LocalAppSetEnabledPath    = "/local-control/v1/apps:set-enabled"
	LocalAppUninstallPath     = "/local-control/v1/apps:uninstall"
)

// RegisterLocalControlRoutes exposes the same Workspace and app-lifecycle
// domain operations as the browser transport. The only difference is actor
// provenance: the local-control bearer supplies one generation-fenced PAID.
func (s *Server) RegisterLocalControlRoutes(control *agentevents.LocalControlServer) error {
	routes := []struct {
		pattern string
		handler agentevents.LocalAuthorizedHandler
	}{
		{"POST " + LocalWorkspacesPath, s.localWorkspaces},
		{"POST " + LocalWorkspaceCreatePath, s.localCreateWorkspace},
		{"POST " + LocalWorkspaceGetPath, s.localWorkspace},
		{"POST " + LocalWorkspaceUpdatePath, s.localUpdateWorkspace},
		{"POST " + LocalWorkspaceOwnerPath, s.localTransferWorkspaceOwnership},
		{"POST " + LocalMembersPath, s.localMembers},
		{"POST " + LocalLeavePath, s.localLeave},
		{"POST " + LocalRemoveMemberPath, s.localRemoveMember},
		{"POST " + LocalInviteCreatePath, s.localCreateInvite},
		{"POST " + LocalInvitesPath, s.localInvites},
		{"POST " + LocalInvitePreviewPath, s.localPreviewInvite},
		{"POST " + LocalInviteRedeemPath, s.localRedeemInvite},
		{"POST " + LocalInviteRevokePath, s.localRevokeInvite},
		{"POST " + LocalInvitationListPath, s.localInvitationList},
		{"POST " + LocalInvitationAcceptPath, s.localInvitationAccept},
		{"POST " + LocalRolesPath, s.localRoles},
		{"POST " + LocalRoleCreatePath, s.localCreateRole},
		{"POST " + LocalRoleUpdatePath, s.localUpdateRole},
		{"POST " + LocalRoleDeletePath, s.localDeleteRole},
		{"POST " + LocalMemberRolesPath, s.localSetMemberRoles},
		{"POST " + LocalAppCatalogPath, s.localAppCatalog},
		{"POST " + LocalAppInstallationsPath, s.localAppInstallations},
		{"POST " + LocalAppResolvePath, s.localResolveEnabledApp},
		{"POST " + LocalAppInstallPath, s.localInstallApp},
		{"POST " + LocalAppSetEnabledPath, s.localSetAppEnabled},
		{"POST " + LocalAppUninstallPath, s.localUninstallApp},
	}
	for _, route := range routes {
		if err := control.RegisterAuthorizedRoute(route.pattern, route.handler); err != nil {
			return err
		}
	}
	return nil
}

func localActor(authorization agentevents.LocalRuntimeAuthorization) participant.Ref {
	return participant.PersonalityAgent(authorization.PersonalityAgentID)
}

func (s *Server) localWorkspaces(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		// RawMessage distinguishes an omitted cursor from JSON null. The public
		// contract accepts only an optional string, never a nullable field.
		Cursor json.RawMessage `json:"cursor"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	var after *workspaceListCursorPosition
	if request.Cursor != nil {
		var rawCursor string
		if err := json.Unmarshal(request.Cursor, &rawCursor); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_request")
			return
		}
		var err error
		after, err = decodeWorkspaceListCursor(rawCursor, authorization)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_request")
			return
		}
	}
	page, err := s.Store.workspacePageFor(r.Context(), localActor(authorization), after)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	wires := make([]workspaceWire, len(page.Items))
	for i, item := range page.Items {
		wires[i] = workspaceToWire(item.Workspace)
	}
	var nextCursor *string
	if page.HasMore {
		encoded, encodeErr := encodeWorkspaceListCursor(
			page.Items[len(page.Items)-1].Position,
			authorization,
		)
		if encodeErr != nil {
			writeDomainError(w, encodeErr)
			return
		}
		nextCursor = &encoded
	}
	writeJSON(w, http.StatusOK, struct {
		Workspaces []workspaceWire `json:"workspaces"`
		NextCursor *string         `json:"next_cursor,omitempty"`
	}{Workspaces: wires, NextCursor: nextCursor})
}

func encodeWorkspaceListCursor(
	position workspaceListCursorPosition,
	authorization agentevents.LocalRuntimeAuthorization,
) (string, error) {
	if len(authorization.BearerToken) < 32 || !isCanonicalUUIDv7(position.WorkspaceID) {
		return "", ErrInvalidWorkspaceListCursor
	}
	workspaceID, err := uuid.Parse(position.WorkspaceID)
	if err != nil {
		return "", ErrInvalidWorkspaceListCursor
	}
	payload := make([]byte, workspaceListCursorPayloadBytes)
	payload[0] = workspaceListCursorVersion
	// Bytes 1:9 are reserved and authenticated as zero. Retaining the original
	// fixed payload width preserves the 76-character wire bound while version 2
	// rejects cursors issued by the superseded membership-tenure ordering.
	copy(payload[9:], workspaceID[:])

	mac := workspaceListCursorMAC(authorization, payload)
	wire := make([]byte, 0, workspaceListCursorBytes)
	wire = append(wire, payload...)
	wire = append(wire, mac...)
	return base64.RawURLEncoding.EncodeToString(wire), nil
}

func decodeWorkspaceListCursor(
	raw string,
	authorization agentevents.LocalRuntimeAuthorization,
) (*workspaceListCursorPosition, error) {
	if len(authorization.BearerToken) < 32 || len(raw) != workspaceListCursorEncodedBytes {
		return nil, ErrInvalidWorkspaceListCursor
	}
	wire, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil || len(wire) != workspaceListCursorBytes {
		return nil, ErrInvalidWorkspaceListCursor
	}
	payload := wire[:workspaceListCursorPayloadBytes]
	wantMAC := workspaceListCursorMAC(authorization, payload)
	if !hmac.Equal(wire[workspaceListCursorPayloadBytes:], wantMAC) {
		return nil, ErrInvalidWorkspaceListCursor
	}
	if payload[0] != workspaceListCursorVersion {
		return nil, ErrInvalidWorkspaceListCursor
	}
	for _, reserved := range payload[1:9] {
		if reserved != 0 {
			return nil, ErrInvalidWorkspaceListCursor
		}
	}
	workspaceID, err := uuid.FromBytes(payload[9:])
	if err != nil || workspaceID.Version() != 7 || workspaceID.Variant() != uuid.RFC4122 {
		return nil, ErrInvalidWorkspaceListCursor
	}
	return &workspaceListCursorPosition{
		WorkspaceID: workspaceID.String(),
	}, nil
}

func workspaceListCursorMAC(
	authorization agentevents.LocalRuntimeAuthorization,
	payload []byte,
) []byte {
	mac := hmac.New(sha256.New, []byte(authorization.BearerToken))
	_, _ = mac.Write([]byte(workspaceListCursorDomain))
	_, _ = mac.Write([]byte(authorization.PersonalityAgentID))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}

type targetedInvitationWire struct {
	InvitationID  string `json:"invitation_id"`
	WorkspaceID   string `json:"workspace_id"`
	WorkspaceName string `json:"workspace_name"`
	ExpiresAt     string `json:"expires_at"`
	CreatedAt     string `json:"created_at"`
}

func targetedInvitationToWire(item TargetedInvitation) targetedInvitationWire {
	return targetedInvitationWire{
		InvitationID:  item.InvitationID,
		WorkspaceID:   item.WorkspaceID,
		WorkspaceName: item.WorkspaceName,
		ExpiresAt:     item.ExpiresAt.UTC().Format(time.RFC3339Nano),
		CreatedAt:     item.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func (s *Server) localInvitationList(
	w http.ResponseWriter,
	r *http.Request,
	authorization agentevents.LocalRuntimeAuthorization,
) {
	var request struct {
		// JSON null is not an omitted cursor and must not gain a second meaning.
		Cursor json.RawMessage `json:"cursor"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	var after *workspaceInvitationListCursorPosition
	if request.Cursor != nil {
		var rawCursor string
		if err := json.Unmarshal(request.Cursor, &rawCursor); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_request")
			return
		}
		var err error
		after, err = decodeWorkspaceInvitationListCursor(rawCursor, authorization)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_request")
			return
		}
	}
	page, err := s.Store.targetedInvitationPageFor(
		r.Context(),
		localActor(authorization),
		after,
	)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	items := make([]targetedInvitationWire, len(page.Items))
	for i, item := range page.Items {
		items[i] = targetedInvitationToWire(item.Invitation)
	}
	var nextCursor *string
	if page.HasMore {
		encoded, encodeErr := encodeWorkspaceInvitationListCursor(
			page.Items[len(page.Items)-1].Position,
			authorization,
		)
		if encodeErr != nil {
			writeDomainError(w, encodeErr)
			return
		}
		nextCursor = &encoded
	}
	writeJSON(w, http.StatusOK, struct {
		Invitations []targetedInvitationWire `json:"invitations"`
		NextCursor  *string                  `json:"next_cursor,omitempty"`
	}{Invitations: items, NextCursor: nextCursor})
}

func encodeWorkspaceInvitationListCursor(
	position workspaceInvitationListCursorPosition,
	authorization agentevents.LocalRuntimeAuthorization,
) (string, error) {
	if len(authorization.BearerToken) < 32 || !isCanonicalUUIDv7(position.InvitationID) {
		return "", ErrInvalidWorkspaceInvitationListCursor
	}
	invitationID, err := uuid.Parse(position.InvitationID)
	if err != nil {
		return "", ErrInvalidWorkspaceInvitationListCursor
	}
	payload := make([]byte, workspaceInvitationListCursorPayloadBytes)
	payload[0] = workspaceInvitationListCursorVersion
	copy(payload[9:], invitationID[:])

	mac := workspaceInvitationListCursorMAC(authorization, payload)
	wire := make([]byte, 0, workspaceInvitationListCursorBytes)
	wire = append(wire, payload...)
	wire = append(wire, mac...)
	return base64.RawURLEncoding.EncodeToString(wire), nil
}

func decodeWorkspaceInvitationListCursor(
	raw string,
	authorization agentevents.LocalRuntimeAuthorization,
) (*workspaceInvitationListCursorPosition, error) {
	if len(authorization.BearerToken) < 32 || len(raw) != workspaceInvitationListCursorEncodedBytes {
		return nil, ErrInvalidWorkspaceInvitationListCursor
	}
	wire, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil || len(wire) != workspaceInvitationListCursorBytes {
		return nil, ErrInvalidWorkspaceInvitationListCursor
	}
	payload := wire[:workspaceInvitationListCursorPayloadBytes]
	wantMAC := workspaceInvitationListCursorMAC(authorization, payload)
	if !hmac.Equal(wire[workspaceInvitationListCursorPayloadBytes:], wantMAC) {
		return nil, ErrInvalidWorkspaceInvitationListCursor
	}
	if payload[0] != workspaceInvitationListCursorVersion {
		return nil, ErrInvalidWorkspaceInvitationListCursor
	}
	for _, reserved := range payload[1:9] {
		if reserved != 0 {
			return nil, ErrInvalidWorkspaceInvitationListCursor
		}
	}
	invitationID, err := uuid.FromBytes(payload[9:])
	if err != nil || invitationID.Version() != 7 || invitationID.Variant() != uuid.RFC4122 {
		return nil, ErrInvalidWorkspaceInvitationListCursor
	}
	return &workspaceInvitationListCursorPosition{
		InvitationID: invitationID.String(),
	}, nil
}

func workspaceInvitationListCursorMAC(
	authorization agentevents.LocalRuntimeAuthorization,
	payload []byte,
) []byte {
	mac := hmac.New(sha256.New, []byte(authorization.BearerToken))
	_, _ = mac.Write([]byte(workspaceInvitationListCursorDomain))
	_, _ = mac.Write([]byte(authorization.PersonalityAgentID))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}

func (s *Server) localInvitationAccept(
	w http.ResponseWriter,
	r *http.Request,
	authorization agentevents.LocalRuntimeAuthorization,
) {
	var request struct {
		InvitationID string `json:"invitation_id"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	membership, err := s.Store.AcceptTargetedInvitation(
		r.Context(),
		request.InvitationID,
		localActor(authorization),
	)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, membershipToWire(membership))
}

func (s *Server) localCreateWorkspace(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		Name string `json:"name"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	created, err := s.Store.CreateWorkspace(r.Context(), request.Name, localActor(authorization))
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, workspaceToWire(created))
}

func (s *Server) localWorkspace(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request workspaceIDRequest
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	item, err := s.Store.WorkspaceFor(r.Context(), request.WorkspaceID, localActor(authorization))
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, workspaceToWire(item))
}

func (s *Server) localUpdateWorkspace(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		WorkspaceID string `json:"workspace_id"`
		Name        string `json:"name"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	item, err := s.Store.UpdateName(r.Context(), request.WorkspaceID,
		localActor(authorization), request.Name)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, workspaceToWire(item))
}

func (s *Server) localTransferWorkspaceOwnership(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request membershipIDRequest
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	item, err := s.Store.TransferOwnership(r.Context(), request.WorkspaceID,
		request.WorkspaceMemberID, localActor(authorization))
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, workspaceToWire(item))
}

func (s *Server) localMembers(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request workspaceIDRequest
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	members, err := s.Store.Members(r.Context(), request.WorkspaceID, localActor(authorization))
	if err != nil {
		writeDomainError(w, err)
		return
	}
	wires := make([]membershipWire, len(members))
	for i, member := range members {
		wires[i] = membershipToWire(member)
	}
	writeJSON(w, http.StatusOK, struct {
		Members []membershipWire `json:"members"`
	}{Members: wires})
}

func (s *Server) localLeave(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request workspaceIDRequest
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	if err := s.Store.Leave(r.Context(), request.WorkspaceID, localActor(authorization)); err != nil {
		writeDomainError(w, err)
		return
	}
	s.notifyMembershipClosed(r.Context(), request.WorkspaceID, localActor(authorization))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) localRemoveMember(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request membershipIDRequest
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	target, err := s.Store.RemoveMemberWithTarget(r.Context(), request.WorkspaceID,
		request.WorkspaceMemberID, localActor(authorization))
	if err != nil {
		writeDomainError(w, err)
		return
	}
	s.notifyMembershipClosed(r.Context(), request.WorkspaceID, target.Participant)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) localCreateInvite(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request workspaceIDRequest
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	invite, err := s.Store.CreateInvite(r.Context(), request.WorkspaceID, localActor(authorization))
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, inviteToWire(invite))
}

func (s *Server) localInvites(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request workspaceIDRequest
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	items, err := s.Store.Invites(r.Context(), request.WorkspaceID, localActor(authorization))
	if err != nil {
		writeDomainError(w, err)
		return
	}
	wires := make([]inviteRecordWire, len(items))
	for i, item := range items {
		wires[i] = inviteRecordToWire(item)
	}
	writeJSON(w, http.StatusOK, struct {
		Invites []inviteRecordWire `json:"invites"`
	}{Invites: wires})
}

func (s *Server) localPreviewInvite(w http.ResponseWriter, r *http.Request, _ agentevents.LocalRuntimeAuthorization) {
	var request struct {
		Code string `json:"code"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	if request.Code == "" || len(request.Code) > 128 {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	preview, err := s.Store.PreviewInvite(r.Context(), request.Code)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, invitePreviewToWire(preview))
}

func (s *Server) localRedeemInvite(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		Code string `json:"code"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	if request.Code == "" || len(request.Code) > 128 {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	membership, err := s.Store.RedeemInvite(r.Context(), request.Code, localActor(authorization))
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, membershipToWire(membership))
}

func (s *Server) localRevokeInvite(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		WorkspaceID string `json:"workspace_id"`
		InviteID    string `json:"invite_id"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	if err := s.Store.RevokeInvite(r.Context(), request.WorkspaceID,
		request.InviteID, localActor(authorization)); err != nil {
		writeDomainError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) localRoles(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request workspaceIDRequest
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	roles, err := s.Store.Roles(r.Context(), request.WorkspaceID, localActor(authorization))
	if err != nil {
		writeDomainError(w, err)
		return
	}
	wires := make([]roleWire, len(roles))
	for i, role := range roles {
		wires[i] = roleToWire(role)
	}
	writeJSON(w, http.StatusOK, struct {
		Roles []roleWire `json:"roles"`
	}{Roles: wires})
}

func (s *Server) localCreateRole(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		WorkspaceID string `json:"workspace_id"`
		roleMutationRequest
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	if request.Permissions == nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	role, err := s.Store.CreateRoleWithPosition(r.Context(), request.WorkspaceID,
		localActor(authorization), request.Name, request.Color,
		permissionMap(*request.Permissions), request.Position.pointer())
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, roleToWire(role))
}

func (s *Server) localUpdateRole(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		WorkspaceID string `json:"workspace_id"`
		RoleID      string `json:"role_id"`
		roleMutationRequest
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	if request.Permissions == nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	role, err := s.Store.UpdateRoleWithPosition(r.Context(), request.WorkspaceID, request.RoleID,
		localActor(authorization), request.Name, request.Color,
		permissionMap(*request.Permissions), request.Position.pointer())
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, roleToWire(role))
}

func (s *Server) localDeleteRole(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		WorkspaceID string `json:"workspace_id"`
		RoleID      string `json:"role_id"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	if err := s.Store.DeleteRole(r.Context(), request.WorkspaceID, request.RoleID,
		localActor(authorization)); err != nil {
		writeDomainError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) localSetMemberRoles(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		WorkspaceID       string    `json:"workspace_id"`
		WorkspaceMemberID string    `json:"workspace_member_id"`
		RoleIDs           *[]string `json:"role_ids"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	if request.RoleIDs == nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	roleIDs, err := s.Store.SetMembershipRoles(r.Context(), request.WorkspaceID,
		request.WorkspaceMemberID, localActor(authorization), *request.RoleIDs)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	if roleIDs == nil {
		roleIDs = []string{}
	}
	writeJSON(w, http.StatusOK, struct {
		RoleIDs []string `json:"role_ids"`
	}{RoleIDs: roleIDs})
}

func (s *Server) localAppCatalog(w http.ResponseWriter, r *http.Request, _ agentevents.LocalRuntimeAuthorization) {
	var request struct{}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	descriptors, err := s.Apps.Catalog(r.Context())
	if err != nil {
		writeDomainError(w, err)
		return
	}
	wires := make([]appDescriptorWire, len(descriptors))
	for i, descriptor := range descriptors {
		wires[i] = descriptorToWire(descriptor)
	}
	writeJSON(w, http.StatusOK, struct {
		Apps []appDescriptorWire `json:"apps"`
	}{Apps: wires})
}

func (s *Server) localAppInstallations(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		Owner appOwnerWire `json:"owner"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	owner, err := request.Owner.ref()
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	installations, err := s.Apps.Installations(r.Context(), owner, localActor(authorization))
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeInstallationList(w, installations)
}

// localResolveEnabledApp is the app-lifecycle-owned bind seam used by trusted
// app adapters. The model selects a Workspace, while the adapter supplies its
// own fixed app identity. The authenticated local-control authorization is the
// only source of actor identity; the response returns the exact Workspace,
// installation, and lifecycle epoch while this read neither installs nor
// enables an app.
func (s *Server) localResolveEnabledApp(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		WorkspaceID string `json:"workspace_id"`
		AppID       string `json:"app_id"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	if s.Apps == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "apps_unavailable")
		return
	}
	owner := applicationapps.WorkspaceOwner(request.WorkspaceID)
	if owner.Validate() != nil || request.AppID == "" || len(request.AppID) > 128 {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	installation, err := s.Apps.ResolveEnabledInstallation(
		r.Context(), owner, localActor(authorization), request.AppID,
	)
	if err != nil {
		switch {
		case errors.Is(err, applicationapps.ErrForbidden), errors.Is(err, ErrForbidden):
			writeAPIError(w, http.StatusForbidden, "forbidden")
		case errors.Is(err, ErrNotFound), errors.Is(err, ErrMemberNotFound):
			// Workspace reads deliberately conceal whether the Workspace or
			// membership is absent. Preserve that boundary at this resolver.
			writeAPIError(w, http.StatusNotFound, "not_found")
		case errors.Is(err, applicationapps.ErrInstallationNotFound):
			writeAPIError(w, http.StatusNotFound, "installation_not_found")
		case errors.Is(err, applicationapps.ErrAppDisabled):
			writeAPIError(w, http.StatusConflict, "app_disabled")
		default:
			writeAPIError(w, http.StatusServiceUnavailable, "apps_unavailable")
		}
		return
	}
	writeJSON(w, http.StatusOK, struct {
		WorkspaceID    string `json:"workspace_id"`
		InstallationID string `json:"installation_id"`
		AuthorityEpoch string `json:"authority_epoch"`
	}{
		WorkspaceID: request.WorkspaceID, InstallationID: installation.InstallationID,
		AuthorityEpoch: strconv.FormatInt(installation.AuthorityEpoch, 10),
	})
}

func (s *Server) localInstallApp(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		Owner       appOwnerWire    `json:"owner"`
		AppID       string          `json:"app_id"`
		OperationID json.RawMessage `json:"operation_id"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	owner, err := request.Owner.ref()
	operationID, present, valid := optionalNonEmptyString(request.OperationID)
	if err != nil || request.AppID == "" || !present || !valid || applicationapps.ValidateInstallOperationID(operationID) != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	installation, err := s.Apps.InstallAtOperation(r.Context(), owner,
		localActor(authorization), request.AppID, operationID)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, installationToWire(installation))
}

func (s *Server) localSetAppEnabled(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		InstallationID string `json:"installation_id"`
		Enabled        *bool  `json:"enabled"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	if request.InstallationID == "" || request.Enabled == nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	installation, err := s.Apps.SetEnabledByID(r.Context(), request.InstallationID,
		localActor(authorization), *request.Enabled)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, installationToWire(installation))
}

func (s *Server) localUninstallApp(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		InstallationID string `json:"installation_id"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	if request.InstallationID == "" {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if err := s.Apps.UninstallByID(r.Context(), request.InstallationID,
		localActor(authorization)); err != nil {
		writeDomainError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type workspaceIDRequest struct {
	WorkspaceID string `json:"workspace_id"`
}

type membershipIDRequest struct {
	WorkspaceID       string `json:"workspace_id"`
	WorkspaceMemberID string `json:"workspace_member_id"`
}
