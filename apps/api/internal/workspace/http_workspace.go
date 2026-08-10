package workspace

import (
	"net/http"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

func (s *Server) serveWorkspaces(w http.ResponseWriter, r *http.Request) {
	actor, _, ok := s.browserActor(w, r)
	if !ok {
		return
	}
	items, err := s.Store.WorkspacesFor(r.Context(), actor)
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

func (s *Server) serveCreateWorkspace(w http.ResponseWriter, r *http.Request) {
	actor, claims, ok := s.browserActor(w, r)
	if !ok {
		return
	}
	var request struct {
		Name string `json:"name"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	var created Workspace
	done, err := s.browserMutation(w, r, claims, func() error {
		var createErr error
		created, createErr = s.Store.CreateWorkspace(r.Context(), request.Name, actor)
		return createErr
	})
	if !done {
		return
	}
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, workspaceToWire(created))
}

func (s *Server) serveWorkspace(w http.ResponseWriter, r *http.Request) {
	actor, _, ok := s.browserActor(w, r)
	if !ok {
		return
	}
	item, err := s.Store.WorkspaceFor(r.Context(), r.PathValue("workspace_id"), actor)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, workspaceToWire(item))
}

func (s *Server) serveUpdateWorkspace(w http.ResponseWriter, r *http.Request) {
	actor, claims, ok := s.browserActor(w, r)
	if !ok {
		return
	}
	var request struct {
		Name string `json:"name"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	var updated Workspace
	done, err := s.browserMutation(w, r, claims, func() error {
		var updateErr error
		updated, updateErr = s.Store.UpdateName(
			r.Context(), r.PathValue("workspace_id"), actor, request.Name,
		)
		return updateErr
	})
	if !done {
		return
	}
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, workspaceToWire(updated))
}

func (s *Server) serveMembers(w http.ResponseWriter, r *http.Request) {
	actor, _, ok := s.browserActor(w, r)
	if !ok {
		return
	}
	members, err := s.Store.Members(r.Context(), r.PathValue("workspace_id"), actor)
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

func (s *Server) serveLeaveWorkspace(w http.ResponseWriter, r *http.Request) {
	actor, claims, ok := s.browserActor(w, r)
	if !ok {
		return
	}
	done, err := s.browserMutation(w, r, claims, func() error {
		return s.Store.Leave(r.Context(), r.PathValue("workspace_id"), actor)
	})
	if !done {
		return
	}
	if err != nil {
		writeDomainError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) serveRemoveMember(w http.ResponseWriter, r *http.Request) {
	actor, claims, ok := s.browserActor(w, r)
	if !ok {
		return
	}
	done, err := s.browserMutation(w, r, claims, func() error {
		return s.Store.RemoveMember(r.Context(), r.PathValue("workspace_id"),
			r.PathValue("workspace_member_id"), actor)
	})
	if !done {
		return
	}
	if err != nil {
		writeDomainError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) serveCreateInvite(w http.ResponseWriter, r *http.Request) {
	actor, claims, ok := s.browserActor(w, r)
	if !ok {
		return
	}
	var request struct{}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	var invite Invite
	done, err := s.browserMutation(w, r, claims, func() error {
		var inviteErr error
		invite, inviteErr = s.Store.CreateInvite(
			r.Context(), r.PathValue("workspace_id"), actor,
		)
		return inviteErr
	})
	if !done {
		return
	}
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, inviteToWire(invite))
}

func (s *Server) serveRevokeInvite(w http.ResponseWriter, r *http.Request) {
	actor, claims, ok := s.browserActor(w, r)
	if !ok {
		return
	}
	done, err := s.browserMutation(w, r, claims, func() error {
		return s.Store.RevokeInvite(r.Context(), r.PathValue("workspace_id"),
			r.PathValue("invite_id"), actor)
	})
	if !done {
		return
	}
	if err != nil {
		writeDomainError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) servePreviewInvite(w http.ResponseWriter, r *http.Request) {
	code := r.URL.Query().Get("code")
	if code == "" || len(code) > 128 {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	preview, err := s.Store.PreviewInvite(r.Context(), code)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, invitePreviewToWire(preview))
}

func (s *Server) serveRedeemInvite(w http.ResponseWriter, r *http.Request) {
	actor, claims, ok := s.browserActor(w, r)
	if !ok {
		return
	}
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
	var membership Membership
	done, err := s.browserMutation(w, r, claims, func() error {
		var redeemErr error
		membership, redeemErr = s.Store.RedeemInvite(r.Context(), request.Code, actor)
		return redeemErr
	})
	if !done {
		return
	}
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, membershipToWire(membership))
}

func (s *Server) serveRoles(w http.ResponseWriter, r *http.Request) {
	actor, _, ok := s.browserActor(w, r)
	if !ok {
		return
	}
	roles, err := s.Store.Roles(r.Context(), r.PathValue("workspace_id"), actor)
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

type roleMutationRequest struct {
	Name        string    `json:"name"`
	Color       string    `json:"color,omitempty"`
	Position    *int      `json:"position,omitempty"`
	Permissions *[]string `json:"permissions"`
}

func permissionMap(keys []string) map[string]bool {
	permissions := make(map[string]bool, len(keys))
	for _, key := range keys {
		permissions[key] = true
	}
	return permissions
}

func (s *Server) serveCreateRole(w http.ResponseWriter, r *http.Request) {
	actor, claims, request, ok := s.roleMutationAdmission(w, r)
	if !ok {
		return
	}
	var role Role
	done, err := s.browserMutation(w, r, claims, func() error {
		var roleErr error
		role, roleErr = s.Store.CreateRoleWithPosition(r.Context(), r.PathValue("workspace_id"),
			actor, request.Name, request.Color, permissionMap(*request.Permissions), request.Position)
		return roleErr
	})
	if !done {
		return
	}
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, roleToWire(role))
}

func (s *Server) serveUpdateRole(w http.ResponseWriter, r *http.Request) {
	actor, claims, request, ok := s.roleMutationAdmission(w, r)
	if !ok {
		return
	}
	var role Role
	done, err := s.browserMutation(w, r, claims, func() error {
		var roleErr error
		role, roleErr = s.Store.UpdateRoleWithPosition(r.Context(), r.PathValue("workspace_id"),
			r.PathValue("role_id"), actor, request.Name, request.Color,
			permissionMap(*request.Permissions), request.Position)
		return roleErr
	})
	if !done {
		return
	}
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, roleToWire(role))
}

func (s *Server) roleMutationAdmission(w http.ResponseWriter, r *http.Request) (participant.Ref, agentevents.UserSessionClaims, roleMutationRequest, bool) {
	actor, claims, ok := s.browserActor(w, r)
	if !ok {
		return participant.Ref{}, agentevents.UserSessionClaims{}, roleMutationRequest{}, false
	}
	var request roleMutationRequest
	if !decodeStrictJSON(w, r, &request) {
		return participant.Ref{}, agentevents.UserSessionClaims{}, roleMutationRequest{}, false
	}
	if request.Permissions == nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return participant.Ref{}, agentevents.UserSessionClaims{}, roleMutationRequest{}, false
	}
	return actor, claims, request, true
}

func (s *Server) serveDeleteRole(w http.ResponseWriter, r *http.Request) {
	actor, claims, ok := s.browserActor(w, r)
	if !ok {
		return
	}
	done, err := s.browserMutation(w, r, claims, func() error {
		return s.Store.DeleteRole(r.Context(), r.PathValue("workspace_id"),
			r.PathValue("role_id"), actor)
	})
	if !done {
		return
	}
	if err != nil {
		writeDomainError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) serveSetMemberRoles(w http.ResponseWriter, r *http.Request) {
	actor, claims, ok := s.browserActor(w, r)
	if !ok {
		return
	}
	var request struct {
		RoleIDs *[]string `json:"role_ids"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	if request.RoleIDs == nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	var stored []string
	done, err := s.browserMutation(w, r, claims, func() error {
		var setErr error
		stored, setErr = s.Store.SetMembershipRoles(r.Context(),
			r.PathValue("workspace_id"), r.PathValue("workspace_member_id"),
			actor, *request.RoleIDs)
		return setErr
	})
	if !done {
		return
	}
	if err != nil {
		writeDomainError(w, err)
		return
	}
	if stored == nil {
		stored = []string{}
	}
	writeJSON(w, http.StatusOK, struct {
		RoleIDs []string `json:"role_ids"`
	}{RoleIDs: stored})
}
