package workspace

import (
	"net/http"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

const (
	LocalWorkspacesPath       = "/local-control/v1/workspace:list"
	LocalWorkspaceCreatePath  = "/local-control/v1/workspace:create"
	LocalWorkspaceGetPath     = "/local-control/v1/workspace:get"
	LocalWorkspaceUpdatePath  = "/local-control/v1/workspace:update"
	LocalMembersPath          = "/local-control/v1/workspace:members"
	LocalLeavePath            = "/local-control/v1/workspace:leave"
	LocalRemoveMemberPath     = "/local-control/v1/workspace:remove-member"
	LocalInviteCreatePath     = "/local-control/v1/workspace:invite-create"
	LocalInviteRedeemPath     = "/local-control/v1/workspace:invite-redeem"
	LocalInviteRevokePath     = "/local-control/v1/workspace:invite-revoke"
	LocalRolesPath            = "/local-control/v1/workspace:roles"
	LocalRoleCreatePath       = "/local-control/v1/workspace:role-create"
	LocalRoleUpdatePath       = "/local-control/v1/workspace:role-update"
	LocalRoleDeletePath       = "/local-control/v1/workspace:role-delete"
	LocalMemberRolesPath      = "/local-control/v1/workspace:member-roles"
	LocalAppCatalogPath       = "/local-control/v1/apps:catalog"
	LocalAppInstallationsPath = "/local-control/v1/apps:installations"
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
		{"POST " + LocalMembersPath, s.localMembers},
		{"POST " + LocalLeavePath, s.localLeave},
		{"POST " + LocalRemoveMemberPath, s.localRemoveMember},
		{"POST " + LocalInviteCreatePath, s.localCreateInvite},
		{"POST " + LocalInviteRedeemPath, s.localRedeemInvite},
		{"POST " + LocalInviteRevokePath, s.localRevokeInvite},
		{"POST " + LocalRolesPath, s.localRoles},
		{"POST " + LocalRoleCreatePath, s.localCreateRole},
		{"POST " + LocalRoleUpdatePath, s.localUpdateRole},
		{"POST " + LocalRoleDeletePath, s.localDeleteRole},
		{"POST " + LocalMemberRolesPath, s.localSetMemberRoles},
		{"POST " + LocalAppCatalogPath, s.localAppCatalog},
		{"POST " + LocalAppInstallationsPath, s.localAppInstallations},
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
	var request struct{}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	items, err := s.Store.WorkspacesFor(r.Context(), localActor(authorization))
	if err != nil {
		writeDomainError(w, err)
		return
	}
	wires := make([]workspaceWire, len(items))
	for i, item := range items {
		wires[i] = workspaceToWire(item)
	}
	writeJSON(w, http.StatusOK, struct {
		Workspaces []workspaceWire `json:"workspaces"`
	}{Workspaces: wires})
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
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) localRemoveMember(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request membershipIDRequest
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	if err := s.Store.RemoveMember(r.Context(), request.WorkspaceID,
		request.WorkspaceMemberID, localActor(authorization)); err != nil {
		writeDomainError(w, err)
		return
	}
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

func (s *Server) localRedeemInvite(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		Code string `json:"code"`
	}
	if !decodeStrictJSON(w, r, &request) {
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
	role, err := s.Store.CreateRole(r.Context(), request.WorkspaceID,
		localActor(authorization), request.Name, request.Color,
		permissionMap(request.Permissions))
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
	role, err := s.Store.UpdateRole(r.Context(), request.WorkspaceID, request.RoleID,
		localActor(authorization), request.Name, request.Color,
		permissionMap(request.Permissions))
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
		WorkspaceID       string   `json:"workspace_id"`
		WorkspaceMemberID string   `json:"workspace_member_id"`
		RoleIDs           []string `json:"role_ids"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	roleIDs, err := s.Store.SetMembershipRoles(r.Context(), request.WorkspaceID,
		request.WorkspaceMemberID, localActor(authorization), request.RoleIDs)
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

func (s *Server) localInstallApp(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		Owner appOwnerWire `json:"owner"`
		AppID string       `json:"app_id"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	owner, err := request.Owner.ref()
	if err != nil || request.AppID == "" {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	installation, err := s.Apps.Install(r.Context(), owner,
		localActor(authorization), request.AppID)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, installationToWire(installation))
}

func (s *Server) localSetAppEnabled(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		InstallationID string `json:"installation_id"`
		Enabled        bool   `json:"enabled"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	if request.InstallationID == "" {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	installation, err := s.Apps.SetEnabledByID(r.Context(), request.InstallationID,
		localActor(authorization), request.Enabled)
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
