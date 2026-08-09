package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

// admitAll puts everyone in the shared Workspace, where the migration's seeded
// Admin/Member roles live.
func (w world) admitAll(t *testing.T, ctx context.Context) {
	t.Helper()
	for _, participant := range []ParticipantRef{w.humanA, w.humanB, w.agent} {
		if err := w.store.EnsureDefaultWorkspaceMembership(ctx, participant); err != nil {
			t.Fatalf("admit %s: %v", participant.Key(), err)
		}
	}
}

// localHandler is one agent-lane endpoint, driven the way the local control
// server drives it: identity comes from the generation-fenced lease, never
// from the request body.
type localHandler func(http.ResponseWriter, *http.Request, agentevents.LocalRuntimeAuthorization)

func (w world) callAgent(t *testing.T, ctx context.Context, handler localHandler, path string, body any) (int, map[string]any) {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("encode %s request: %v", path, err)
	}
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(encoded)).WithContext(ctx)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler(response, request, agentevents.LocalRuntimeAuthorization{
		PersonalityAgentID: w.agent.ID,
	})
	decoded := map[string]any{}
	if response.Body.Len() > 0 {
		if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
			t.Fatalf("decode %s response %q: %v", path, response.Body.String(), err)
		}
	}
	return response.Code, decoded
}

func TestSeededRolesDefineAdminAndPlainMember(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.admitAll(t, ctx)

	// The migration seeds exactly two roles into the shared Workspace. In a
	// live deployment it also grants Admin to every Human who already existed;
	// a test database mints its Humans after migrating, so the founding-admin
	// bootstrap is what hands out the first Admin here (covered separately).
	roles, err := w.store.Roles(ctx, DefaultWorkspaceID, w.humanA)
	if err != nil {
		t.Fatalf("list roles: %v", err)
	}
	byName := map[string]Role{}
	for _, role := range roles {
		byName[role.Name] = role
	}
	admin, ok := byName["Admin"]
	if !ok || admin.RoleID != DefaultAdminRoleID {
		t.Fatalf("seeded roles = %#v", roles)
	}
	for _, permission := range knownPermissions {
		if !admin.Permissions[permission] {
			t.Fatalf("Admin lacks %s: %#v", permission, admin.Permissions)
		}
	}
	member, ok := byName["Member"]
	if !ok || member.RoleID != DefaultMemberRoleID || len(member.Permissions) != 0 {
		t.Fatalf("seeded Member = %#v", member)
	}

	// Holding the seeded Admin role is what grants everything — the same role
	// works for a PersonalityAgent, because permission is about the role, not
	// about the kind of participant.
	if _, err := w.store.SetParticipantRoles(ctx, DefaultWorkspaceID, w.humanA, w.agent, []string{DefaultAdminRoleID}); err != nil {
		t.Fatalf("grant Admin to agent: %v", err)
	}
	granted, err := w.store.PermissionsFor(ctx, DefaultWorkspaceID, w.agent)
	if err != nil {
		t.Fatalf("agent permissions: %v", err)
	}
	for _, permission := range knownPermissions {
		if !granted.Can(permission) {
			t.Fatalf("agent with Admin lacks %s: %#v", permission, granted)
		}
	}

	// A member holding no role holds no permission.
	plain, err := w.store.PermissionsFor(ctx, DefaultWorkspaceID, w.humanB)
	if err != nil {
		t.Fatalf("plain member permissions: %v", err)
	}
	if len(plain) != 0 {
		t.Fatalf("plain member permissions = %#v, want none", plain)
	}
}

func TestChannelAdministrationRequiresManageChannels(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.admitAll(t, ctx)

	// An Admin human may create and edit channels.
	channel, err := w.store.CreateChannel(ctx, DefaultWorkspaceID, "dev", "開発", w.humanA, false)
	if err != nil {
		t.Fatalf("admin creates channel: %v", err)
	}
	if _, err := w.store.UpdateChannelTopic(ctx, channel.PlaceID, "開発の相談", w.humanA); err != nil {
		t.Fatalf("admin edits topic: %v", err)
	}

	// The agent is a member with no permissions: refused, not hidden. It can
	// see the place, so pretending it is missing would be a lie.
	if _, err := w.store.CreateChannel(ctx, DefaultWorkspaceID, "secret", "", w.agent, false); !errors.Is(err, ErrForbidden) {
		t.Fatalf("agent creates channel: error = %v, want ErrForbidden", err)
	}
	if _, err := w.store.UpdateChannelTopic(ctx, channel.PlaceID, "書き換え", w.agent); !errors.Is(err, ErrForbidden) {
		t.Fatalf("agent edits topic: error = %v, want ErrForbidden", err)
	}

	// The identical rule applies to a Human once their roles are taken away.
	if _, err := w.store.SetParticipantRoles(ctx, DefaultWorkspaceID, w.humanA, w.humanB, nil); err != nil {
		t.Fatalf("clear B's roles: %v", err)
	}
	if _, err := w.store.CreateChannel(ctx, DefaultWorkspaceID, "b-channel", "", w.humanB, false); !errors.Is(err, ErrForbidden) {
		t.Fatalf("demoted human creates channel: error = %v, want ErrForbidden", err)
	}
}

func TestRoleLifecycleAndAssignmentAreGatedSeparately(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.admitAll(t, ctx)

	role, err := w.store.CreateRole(ctx, DefaultWorkspaceID, w.humanA, "開発", "#3366ff",
		map[string]bool{PermManageChannels: true, "invented": true})
	if err != nil {
		t.Fatalf("create role: %v", err)
	}
	// An unknown permission key is dropped rather than stored: a later reader
	// can never be surprised by a permission this build does not enforce.
	if _, ok := role.Permissions["invented"]; ok {
		t.Fatalf("role permissions = %#v, want the unknown key dropped", role.Permissions)
	}
	if !role.Permissions[PermManageChannels] {
		t.Fatalf("role permissions = %#v", role.Permissions)
	}

	if _, err := w.store.CreateRole(ctx, DefaultWorkspaceID, w.humanA, "開発", "", nil); !errors.Is(err, ErrRoleNameTaken) {
		t.Fatalf("duplicate role name: error = %v, want ErrRoleNameTaken", err)
	}
	if _, err := w.store.CreateRole(ctx, DefaultWorkspaceID, w.humanA, "色", "red", nil); !errors.Is(err, ErrInvalidRoleColor) {
		t.Fatalf("non-hex colour: error = %v, want ErrInvalidRoleColor", err)
	}
	if _, err := w.store.CreateRole(ctx, DefaultWorkspaceID, w.humanA, "", "", nil); !errors.Is(err, ErrInvalidRoleName) {
		t.Fatalf("blank role name: error = %v, want ErrInvalidRoleName", err)
	}

	// Granting the role gives the agent exactly the permission it carries.
	if _, err := w.store.SetParticipantRoles(ctx, DefaultWorkspaceID, w.humanA, w.agent, []string{role.RoleID}); err != nil {
		t.Fatalf("grant role to agent: %v", err)
	}
	granted, err := w.store.PermissionsFor(ctx, DefaultWorkspaceID, w.agent)
	if err != nil {
		t.Fatalf("agent permissions: %v", err)
	}
	if !granted.Can(PermManageChannels) || granted.Can(PermManageRoles) {
		t.Fatalf("agent permissions after grant = %#v", granted)
	}
	// The agent may now administer channels through the identical store path a
	// Human uses — the permission is about the role, not the kind.
	if _, err := w.store.CreateChannel(ctx, DefaultWorkspaceID, "agent-made", "", w.agent, false); err != nil {
		t.Fatalf("agent with manage_channels creates channel: %v", err)
	}
	// but still not roles or members.
	if _, err := w.store.CreateRole(ctx, DefaultWorkspaceID, w.agent, "自称管理者", "", nil); !errors.Is(err, ErrForbidden) {
		t.Fatalf("agent creates role: error = %v, want ErrForbidden", err)
	}
	if _, err := w.store.SetParticipantRoles(ctx, DefaultWorkspaceID, w.agent, w.agent, []string{DefaultAdminRoleID}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("agent grants itself Admin: error = %v, want ErrForbidden", err)
	}

	// A role id from another workspace cannot be granted here.
	other, err := w.store.CreateWorkspace(ctx, "別の場所", w.humanA)
	if err != nil {
		t.Fatalf("create other workspace: %v", err)
	}
	foreign, err := w.store.CreateRole(ctx, other.WorkspaceID, w.humanA, "よそのロール", "", nil)
	if err != nil {
		t.Fatalf("create foreign role: %v", err)
	}
	if _, err := w.store.SetParticipantRoles(ctx, DefaultWorkspaceID, w.humanA, w.agent, []string{foreign.RoleID}); !errors.Is(err, ErrRoleNotFound) {
		t.Fatalf("foreign role grant: error = %v, want ErrRoleNotFound", err)
	}

	// Deleting a role withdraws it from everyone holding it.
	if err := w.store.DeleteRole(ctx, DefaultWorkspaceID, role.RoleID, w.humanA); err != nil {
		t.Fatalf("delete role: %v", err)
	}
	granted, err = w.store.PermissionsFor(ctx, DefaultWorkspaceID, w.agent)
	if err != nil {
		t.Fatalf("agent permissions after delete: %v", err)
	}
	if granted.Can(PermManageChannels) {
		t.Fatalf("agent kept a deleted role's permission: %#v", granted)
	}
	if err := w.store.DeleteRole(ctx, DefaultWorkspaceID, role.RoleID, w.humanA); !errors.Is(err, ErrRoleNotFound) {
		t.Fatalf("deleting twice: error = %v, want ErrRoleNotFound", err)
	}
}

func TestRoleRoutesRefuseWithoutPermissionAndBootstrapCarriesThem(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)
	w.admitAll(t, ctx)

	rolesPath := "/messaging/workspaces/" + DefaultWorkspaceID + "/roles"

	// Bootstrap tells the client what it may do, so the UI can gate its own
	// entries from the first paint instead of guessing.
	resp, body := call(t, ts, http.MethodGet, "/messaging/bootstrap", w.humanA.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bootstrap: status %d", resp.StatusCode)
	}
	permissions, _ := body["permissions"].(map[string]any)
	if permissions[PermManageRoles] != true {
		t.Fatalf("bootstrap permissions = %v", body["permissions"])
	}
	if roles, _ := body["roles"].([]any); len(roles) != 2 {
		t.Fatalf("bootstrap roles = %v, want the two seeded roles", body["roles"])
	}

	// Reading roles is open to members; the seeded Admin may also write.
	resp, _ = call(t, ts, http.MethodGet, rolesPath, w.humanB.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("member reads roles: status %d", resp.StatusCode)
	}
	resp, created := call(t, ts, http.MethodPost, rolesPath, w.humanA.ID, map[string]any{
		"name": "設計", "color": "#aa3366",
		"permissions": map[string]bool{PermManageChannels: true},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("admin creates role: status %d body %v", resp.StatusCode, created)
	}
	roleID, _ := created["role_id"].(string)

	// Take B's roles away, then check every administrative route refuses B
	// with a clear 403 rather than a pretend-missing resource.
	memberPath := "/messaging/workspaces/" + DefaultWorkspaceID +
		"/members/human/" + w.humanB.ID + "/roles"
	resp, _ = call(t, ts, http.MethodPut, memberPath, w.humanA.ID, map[string]any{
		"role_ids": []string{},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("admin clears B's roles: status %d", resp.StatusCode)
	}

	refusals := []struct {
		method string
		path   string
		body   any
	}{
		{http.MethodPost, rolesPath, map[string]any{"name": "勝手に", "permissions": map[string]bool{}}},
		{http.MethodPatch, rolesPath + "/" + roleID, map[string]any{"name": "改名", "permissions": map[string]bool{}}},
		{http.MethodDelete, rolesPath + "/" + roleID, nil},
		{http.MethodPut, memberPath, map[string]any{"role_ids": []string{DefaultAdminRoleID}}},
		{http.MethodPost, "/messaging/channels", map[string]any{
			"workspace_id": DefaultWorkspaceID, "name": "勝手なチャンネル",
		}},
	}
	for _, refusal := range refusals {
		resp, decoded := call(t, ts, refusal.method, refusal.path, w.humanB.ID, refusal.body)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("%s %s without permission: status %d body %v",
				refusal.method, refusal.path, resp.StatusCode, decoded)
		}
		if decoded["error"] != "forbidden" {
			t.Fatalf("%s %s error = %v, want a clear refusal", refusal.method, refusal.path, decoded["error"])
		}
	}

	// B's own bootstrap now reports no administrative permission.
	_, body = call(t, ts, http.MethodGet, "/messaging/bootstrap", w.humanB.ID, nil)
	permissions, _ = body["permissions"].(map[string]any)
	if permissions[PermManageRoles] == true || permissions[PermManageChannels] == true {
		t.Fatalf("demoted bootstrap permissions = %v", body["permissions"])
	}
}

// Every administrative gesture the settings screen offers exists on the agent
// lane too (AX 同型). What separates the two participants is the permission
// they hold, not the operations their surface has.
func TestAgentLaneAdministersRolesUnderTheSamePermission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	server := NewServer(w.store, nil)
	w.admitAll(t, ctx)

	// Seed one role through the human lane so the refusals below have a real
	// role to aim at; the agent still holds nothing.
	seeded, err := w.store.CreateRole(ctx, DefaultWorkspaceID, w.humanA, "編集者", "",
		map[string]bool{PermManageChannels: true})
	if err != nil {
		t.Fatalf("seed role: %v", err)
	}
	channel, err := w.store.CreateChannel(ctx, DefaultWorkspaceID, "general", "", w.humanA, false)
	if err != nil {
		t.Fatalf("seed channel: %v", err)
	}

	refusals := []struct {
		name    string
		handler localHandler
		path    string
		body    map[string]any
	}{
		{"create", server.localCreateRole, LocalRoleCreatePath,
			map[string]any{"name": "自称管理者"}},
		{"update", server.localUpdateRole, LocalRoleUpdatePath,
			map[string]any{"role_id": seeded.RoleID, "name": "改名"}},
		{"delete", server.localDeleteRole, LocalRoleDeletePath,
			map[string]any{"role_id": seeded.RoleID}},
		{"assign", server.localSetMemberRoles, LocalMemberRolesPath,
			map[string]any{"member_kind": "personality_agent", "member_id": w.agent.ID,
				"role_ids": []string{DefaultAdminRoleID}}},
		{"channel", server.localCreateChannel, LocalCreateChannelPath,
			map[string]any{"name": "勝手なチャンネル"}},
		{"topic", server.localSetChannelTopic, LocalChannelTopicPath,
			map[string]any{"place_id": channel.PlaceID, "topic": "勝手な書き換え"}},
	}
	for _, refusal := range refusals {
		status, decoded := w.callAgent(t, ctx, refusal.handler, refusal.path, refusal.body)
		if status != http.StatusForbidden || decoded["error"] != "forbidden" {
			t.Fatalf("%s without permission: status %d body %v", refusal.name, status, decoded)
		}
	}

	// Give the agent the seeded Admin role, and the identical calls go through.
	if _, err := w.store.SetParticipantRoles(ctx, DefaultWorkspaceID, w.humanA, w.agent,
		[]string{DefaultAdminRoleID}); err != nil {
		t.Fatalf("grant Admin to agent: %v", err)
	}

	// The agent lane names permissions in a list; an unknown name is dropped
	// by the same store rule the browser's boolean map meets.
	status, created := w.callAgent(t, ctx, server.localCreateRole, LocalRoleCreatePath,
		map[string]any{"name": "開発", "color": "#3366ff",
			"permissions": []string{PermManageChannels, "invented"}})
	if status != http.StatusCreated {
		t.Fatalf("agent creates role: status %d body %v", status, created)
	}
	role, _ := created["role"].(map[string]any)
	permissions, _ := role["permissions"].(map[string]any)
	if permissions[PermManageChannels] != true || len(permissions) != 1 {
		t.Fatalf("created role permissions = %v", role["permissions"])
	}
	roleID, _ := role["role_id"].(string)

	// Granting it to a Human works the same way round: the member is named in
	// the ParticipantRef grammar, not by a kind-specific route.
	status, assigned := w.callAgent(t, ctx, server.localSetMemberRoles, LocalMemberRolesPath,
		map[string]any{"member_kind": "human", "member_id": w.humanB.ID,
			"role_ids": []string{roleID}})
	if status != http.StatusOK {
		t.Fatalf("agent assigns role: status %d body %v", status, assigned)
	}
	granted, err := w.store.PermissionsFor(ctx, DefaultWorkspaceID, w.humanB)
	if err != nil {
		t.Fatalf("humanB permissions: %v", err)
	}
	if !granted.Can(PermManageChannels) {
		t.Fatalf("humanB permissions = %#v, want the granted role's", granted)
	}

	// update_role replaces the permission set rather than adding to it.
	status, updated := w.callAgent(t, ctx, server.localUpdateRole, LocalRoleUpdatePath,
		map[string]any{"role_id": roleID, "name": "設計"})
	if status != http.StatusOK {
		t.Fatalf("agent updates role: status %d body %v", status, updated)
	}
	granted, err = w.store.PermissionsFor(ctx, DefaultWorkspaceID, w.humanB)
	if err != nil {
		t.Fatalf("humanB permissions after update: %v", err)
	}
	if granted.Can(PermManageChannels) {
		t.Fatalf("humanB kept a permission the role no longer carries: %#v", granted)
	}

	status, deleted := w.callAgent(t, ctx, server.localDeleteRole, LocalRoleDeletePath,
		map[string]any{"role_id": roleID})
	if status != http.StatusOK || deleted["deleted"] != true {
		t.Fatalf("agent deletes role: status %d body %v", status, deleted)
	}
	assignments, err := w.store.RoleAssignments(ctx, DefaultWorkspaceID, w.humanA)
	if err != nil {
		t.Fatalf("role assignments: %v", err)
	}
	for _, ids := range assignments {
		for _, id := range ids {
			if id == roleID {
				t.Fatalf("deleted role is still held: %v", assignments)
			}
		}
	}

	// manage_channels is a permission an agent can actually exercise: the
	// sidebar's「＋」and the header's topic line both have an action here.
	status, madeChannel := w.callAgent(t, ctx, server.localCreateChannel, LocalCreateChannelPath,
		map[string]any{"name": "agent-made", "topic": "agentが立てた場所"})
	if status != http.StatusCreated {
		t.Fatalf("agent creates channel: status %d body %v", status, madeChannel)
	}
	status, retopiced := w.callAgent(t, ctx, server.localSetChannelTopic, LocalChannelTopicPath,
		map[string]any{"place_id": channel.PlaceID, "topic": "レビュー予約はこちら"})
	if status != http.StatusOK {
		t.Fatalf("agent sets topic: status %d body %v", status, retopiced)
	}
	editedChannel, _ := retopiced["channel"].(map[string]any)
	if editedChannel["topic"] != "レビュー予約はこちら" {
		t.Fatalf("topic after agent edit = %v", retopiced["channel"])
	}

	// A member named with a kind the model does not have is a bad request,
	// not a silent no-op.
	status, decoded := w.callAgent(t, ctx, server.localSetMemberRoles, LocalMemberRolesPath,
		map[string]any{"member_kind": "bot", "member_id": w.humanB.ID, "role_ids": []string{}})
	if status != http.StatusBadRequest || decoded["error"] != "invalid_participant" {
		t.Fatalf("unknown member kind: status %d body %v", status, decoded)
	}
}

func TestLocalRolesIsReadableByTheAgentAndGrantsNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	server := NewServer(w.store, nil)
	w.admitAll(t, ctx)

	request := httptest.NewRequest(http.MethodPost, LocalRolesPath, strings.NewReader(`{}`)).WithContext(ctx)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.localRoles(response, request, agentevents.LocalRuntimeAuthorization{
		PersonalityAgentID: w.agent.ID,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("local roles: status %d body %s", response.Code, response.Body.String())
	}
	var decoded struct {
		WorkspaceID string          `json:"workspace_id"`
		Roles       []roleWire      `json:"roles"`
		Members     []memberWire    `json:"members"`
		Permissions map[string]bool `json:"permissions"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode local roles: %v", err)
	}
	if decoded.WorkspaceID != DefaultWorkspaceID || len(decoded.Roles) != 2 {
		t.Fatalf("local roles = %#v", decoded)
	}
	if len(decoded.Members) == 0 {
		t.Fatalf("local roles members = %#v, want the member list", decoded.Members)
	}
	// The agent can read who administers the place, and holds nothing itself.
	if decoded.Permissions[PermManageRoles] || decoded.Permissions[PermManageMembers] {
		t.Fatalf("agent permissions = %#v, want none", decoded.Permissions)
	}
}

func TestFoundingAdminOnlyFiresWhileNobodyAdministers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)

	// A database migrated before its first Human existed has no seeded Admin,
	// so the first arrival must be able to administer the workspace — a
	// deployment nobody can add a channel to is not a usable state.
	if _, err := w.store.pool.Exec(ctx, "DELETE FROM participant_roles"); err != nil {
		t.Fatalf("clear seeded grants: %v", err)
	}
	if err := w.store.EnsureDefaultWorkspaceMembership(ctx, w.humanA); err != nil {
		t.Fatalf("admit first human: %v", err)
	}
	granted, err := w.store.PermissionsFor(ctx, DefaultWorkspaceID, w.humanA)
	if err != nil {
		t.Fatalf("first human permissions: %v", err)
	}
	if !granted.Can(PermManageRoles) {
		t.Fatalf("first human permissions = %#v, want Admin", granted)
	}

	// The second arrival is an ordinary member: the rule is a bootstrap, not
	// a policy of admitting everyone as an administrator.
	if err := w.store.EnsureDefaultWorkspaceMembership(ctx, w.humanB); err != nil {
		t.Fatalf("admit second human: %v", err)
	}
	granted, err = w.store.PermissionsFor(ctx, DefaultWorkspaceID, w.humanB)
	if err != nil {
		t.Fatalf("second human permissions: %v", err)
	}
	if granted.Can(PermManageRoles) || granted.Can(PermManageChannels) {
		t.Fatalf("second human permissions = %#v, want none", granted)
	}
}

func TestRoleMutationsCannotExceedTheActorsEffectiveAuthority(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.admitAll(t, ctx)

	roleManager, err := w.store.CreateRole(ctx, DefaultWorkspaceID, w.humanA, "Role manager", "",
		map[string]bool{PermManageRoles: true})
	if err != nil {
		t.Fatalf("create role manager: %v", err)
	}
	memberManager, err := w.store.CreateRole(ctx, DefaultWorkspaceID, w.humanA, "Member manager", "",
		map[string]bool{PermManageMembers: true})
	if err != nil {
		t.Fatalf("create member manager: %v", err)
	}
	stronger, err := w.store.CreateRole(ctx, DefaultWorkspaceID, w.humanA, "Stronger", "",
		map[string]bool{
			PermManageRoles: true, PermManageMembers: true, PermManageChannels: true,
		})
	if err != nil {
		t.Fatalf("create stronger role: %v", err)
	}
	if _, err := w.store.SetParticipantRoles(ctx, DefaultWorkspaceID, w.humanA, w.humanB,
		[]string{roleManager.RoleID}); err != nil {
		t.Fatalf("grant role manager: %v", err)
	}
	if _, err := w.store.SetParticipantRoles(ctx, DefaultWorkspaceID, w.humanA, w.agent,
		[]string{memberManager.RoleID}); err != nil {
		t.Fatalf("grant member manager: %v", err)
	}

	if _, err := w.store.CreateRole(ctx, DefaultWorkspaceID, w.humanB, "Escalation", "",
		map[string]bool{PermManageChannels: true}); !errors.Is(err, ErrForbidden) {
		t.Errorf("role manager creates a permission it lacks: error = %v, want ErrForbidden", err)
	}
	if _, err := w.store.UpdateRole(ctx, DefaultWorkspaceID, roleManager.RoleID, w.humanB,
		"Role manager", "", map[string]bool{
			PermManageRoles: true, PermManageChannels: true,
		}); !errors.Is(err, ErrForbidden) {
		t.Errorf("role manager adds a permission to its own role: error = %v, want ErrForbidden", err)
	}
	if err := w.store.DeleteRole(ctx, DefaultWorkspaceID, stronger.RoleID, w.humanB); !errors.Is(err, ErrForbidden) {
		t.Errorf("role manager deletes a stronger role: error = %v, want ErrForbidden", err)
	}
	if _, err := w.store.SetParticipantRoles(ctx, DefaultWorkspaceID, w.agent, w.agent,
		[]string{DefaultAdminRoleID}); !errors.Is(err, ErrForbidden) {
		t.Errorf("member manager grants itself Admin: error = %v, want ErrForbidden", err)
	}
	if _, err := w.store.SetParticipantRoles(ctx, DefaultWorkspaceID, w.agent, w.humanA, nil); !errors.Is(err, ErrForbidden) {
		t.Errorf("member manager strips a stronger role: error = %v, want ErrForbidden", err)
	}
}

func TestRoleAuthorityCeilingIsSharedByHumanAndAgentLanes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)
	w.admitAll(t, ctx)

	memberManager, err := w.store.CreateRole(ctx, DefaultWorkspaceID, w.humanA, "Member manager", "",
		map[string]bool{PermManageMembers: true})
	if err != nil {
		t.Fatalf("create member manager: %v", err)
	}
	for _, participant := range []ParticipantRef{w.humanB, w.agent} {
		if _, err := w.store.SetParticipantRoles(ctx, DefaultWorkspaceID, w.humanA, participant,
			[]string{memberManager.RoleID}); err != nil {
			t.Fatalf("grant member manager to %s: %v", participant.Key(), err)
		}
	}

	memberPath := "/messaging/workspaces/" + DefaultWorkspaceID +
		"/members/human/" + w.humanB.ID + "/roles"
	response, body := call(t, ts, http.MethodPut, memberPath, w.humanB.ID, map[string]any{
		"role_ids": []string{DefaultAdminRoleID},
	})
	if response.StatusCode != http.StatusForbidden || body["error"] != "forbidden" {
		t.Fatalf("Human self-escalation: status %d body %v", response.StatusCode, body)
	}

	server := NewServer(w.store, nil)
	status, body := w.callAgent(t, ctx, server.localSetMemberRoles, LocalMemberRolesPath,
		map[string]any{
			"member_kind": "personality_agent", "member_id": w.agent.ID,
			"role_ids": []string{DefaultAdminRoleID},
		})
	if status != http.StatusForbidden || body["error"] != "forbidden" {
		t.Fatalf("agent self-escalation: status %d body %v", status, body)
	}
}

func TestRoleMutationsPreserveARecoverableWorkspaceAdministrator(t *testing.T) {
	t.Run("seed role cannot be weakened or deleted", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newWorld(t, ctx)
		w.admitAll(t, ctx)

		backup, err := w.store.CreateRole(ctx, DefaultWorkspaceID, w.humanA, "Backup admin", "",
			map[string]bool{
				PermManageChannels: true, PermManageRoles: true,
				PermManageMembers: true, PermMentionAll: true,
			})
		if err != nil {
			t.Fatalf("create backup admin: %v", err)
		}
		if _, err := w.store.SetParticipantRoles(ctx, DefaultWorkspaceID, w.humanA, w.humanB,
			[]string{backup.RoleID}); err != nil {
			t.Fatalf("grant backup admin: %v", err)
		}

		if _, err := w.store.UpdateRole(ctx, DefaultWorkspaceID, DefaultAdminRoleID, w.humanA,
			"Admin", "", map[string]bool{PermManageRoles: true}); !errors.Is(err, ErrForbidden) {
			t.Errorf("weaken seeded Admin: error = %v, want ErrForbidden", err)
		}
		if err := w.store.DeleteRole(ctx, DefaultWorkspaceID, DefaultAdminRoleID, w.humanA); !errors.Is(err, ErrForbidden) {
			t.Errorf("delete seeded Admin: error = %v, want ErrForbidden", err)
		}
	})

	t.Run("last effective administrator cannot be removed", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newWorld(t, ctx)
		w.admitAll(t, ctx)

		customAdmin, err := w.store.CreateRole(ctx, DefaultWorkspaceID, w.humanA, "Custom admin", "",
			map[string]bool{
				PermManageChannels: true, PermManageRoles: true,
				PermManageMembers: true, PermMentionAll: true,
			})
		if err != nil {
			t.Fatalf("create custom admin: %v", err)
		}
		if _, err := w.store.SetParticipantRoles(ctx, DefaultWorkspaceID, w.humanA, w.humanA,
			[]string{customAdmin.RoleID}); err != nil {
			t.Fatalf("replace seeded Admin with custom Admin: %v", err)
		}

		if _, err := w.store.UpdateRole(ctx, DefaultWorkspaceID, customAdmin.RoleID, w.humanA,
			"Custom admin", "", map[string]bool{PermManageChannels: true}); !errors.Is(err, ErrForbidden) {
			t.Errorf("remove manage_roles from the last admin: error = %v, want ErrForbidden", err)
		}
		if err := w.store.DeleteRole(ctx, DefaultWorkspaceID, customAdmin.RoleID, w.humanA); !errors.Is(err, ErrForbidden) {
			t.Errorf("delete the last admin role: error = %v, want ErrForbidden", err)
		}
		if _, err := w.store.SetParticipantRoles(ctx, DefaultWorkspaceID, w.humanA, w.humanA, nil); !errors.Is(err, ErrForbidden) {
			t.Errorf("remove the last admin assignment: error = %v, want ErrForbidden", err)
		}
	})

	t.Run("one admin may hand administration to another", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newWorld(t, ctx)
		w.admitAll(t, ctx)
		if _, err := w.store.SetParticipantRoles(ctx, DefaultWorkspaceID, w.humanA, w.humanB,
			[]string{DefaultAdminRoleID}); err != nil {
			t.Fatalf("grant second admin: %v", err)
		}
		if _, err := w.store.SetParticipantRoles(ctx, DefaultWorkspaceID, w.humanA, w.humanA, nil); err != nil {
			t.Fatalf("first admin hands off administration: %v", err)
		}
		granted, err := w.store.PermissionsFor(ctx, DefaultWorkspaceID, w.humanB)
		if err != nil {
			t.Fatalf("second admin permissions: %v", err)
		}
		if !granted.Can(PermManageRoles) {
			t.Fatalf("second admin permissions = %#v, want manage_roles", granted)
		}
	})

	t.Run("concurrent admins cannot both remove themselves", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newWorld(t, ctx)
		w.admitAll(t, ctx)
		if _, err := w.store.SetParticipantRoles(ctx, DefaultWorkspaceID, w.humanA, w.humanB,
			[]string{DefaultAdminRoleID}); err != nil {
			t.Fatalf("grant second admin: %v", err)
		}

		start := make(chan struct{})
		errs := make(chan error, 2)
		for _, participant := range []ParticipantRef{w.humanA, w.humanB} {
			participant := participant
			go func() {
				<-start
				_, err := w.store.SetParticipantRoles(ctx, DefaultWorkspaceID, participant, participant, nil)
				errs <- err
			}()
		}
		close(start)
		var succeeded, refused int
		for range 2 {
			switch err := <-errs; {
			case err == nil:
				succeeded++
			case errors.Is(err, ErrForbidden):
				refused++
			default:
				t.Fatalf("concurrent admin removal: %v", err)
			}
		}
		if succeeded != 1 || refused != 1 {
			t.Fatalf("concurrent admin removals: succeeded=%d refused=%d, want 1/1", succeeded, refused)
		}
	})
}

func TestFoundingAdminDoesNotPretendAMissingSeedWasGranted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.admitAll(t, ctx)
	if _, err := w.store.pool.Exec(ctx, "DELETE FROM participant_roles"); err != nil {
		t.Fatalf("clear seeded grants: %v", err)
	}
	if _, err := w.store.pool.Exec(ctx,
		"DELETE FROM workspace_roles WHERE role_id = $1", DefaultAdminRoleID); err != nil {
		t.Fatalf("remove seeded Admin role: %v", err)
	}
	if err := w.store.EnsureFoundingAdmin(ctx, w.humanA); !errors.Is(err, ErrRoleNotFound) {
		t.Fatalf("founding admin with missing seed: error = %v, want ErrRoleNotFound", err)
	}
}

func TestFoundingAdminIgnoresAssignmentsHeldOnlyByFormerMembers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.admitAll(t, ctx)
	if err := w.store.RemoveWorkspaceMember(ctx, DefaultWorkspaceID, w.humanA); err != nil {
		t.Fatalf("remove original admin: %v", err)
	}
	if err := w.store.EnsureFoundingAdmin(ctx, w.humanB); err != nil {
		t.Fatalf("grant replacement founding admin: %v", err)
	}
	granted, err := w.store.PermissionsFor(ctx, DefaultWorkspaceID, w.humanB)
	if err != nil {
		t.Fatalf("replacement admin permissions: %v", err)
	}
	if !granted.Can(PermManageRoles) {
		t.Fatalf("replacement admin permissions = %#v, want manage_roles", granted)
	}
}

func TestConcurrentFoundingAdminBootstrapGrantsExactlyOneHuman(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.admitAll(t, ctx)

	participants := []ParticipantRef{w.humanA, w.humanB}
	registry := koseki.New(w.store.pool)
	for range 30 {
		humanID, err := registry.MintHuman(ctx)
		if err != nil {
			t.Fatalf("mint concurrent founder: %v", err)
		}
		participant := Human(humanID)
		if err := w.store.AddWorkspaceMember(ctx, DefaultWorkspaceID, participant, RoleMember); err != nil {
			t.Fatalf("admit concurrent founder: %v", err)
		}
		participants = append(participants, participant)
	}
	if _, err := w.store.pool.Exec(ctx, "DELETE FROM participant_roles"); err != nil {
		t.Fatalf("clear seeded grants: %v", err)
	}

	start := make(chan struct{})
	errs := make(chan error, len(participants))
	var ready sync.WaitGroup
	ready.Add(len(participants))
	for _, participant := range participants {
		participant := participant
		go func() {
			ready.Done()
			<-start
			errs <- w.store.EnsureFoundingAdmin(ctx, participant)
		}()
	}
	ready.Wait()
	close(start)
	for range participants {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent founding admin: %v", err)
		}
	}

	var admins int
	if err := w.store.pool.QueryRow(ctx,
		`SELECT count(DISTINCT (pr.member_kind, pr.member_id))
		 FROM participant_roles pr
		 JOIN workspace_roles wr ON wr.role_id = pr.role_id
		 JOIN workspace_members wm
		   ON wm.workspace_id = wr.workspace_id
		  AND wm.member_kind = pr.member_kind AND wm.member_id = pr.member_id
		  AND wm.left_at IS NULL
		 WHERE wr.workspace_id = $1
		   AND COALESCE((wr.permissions ->> 'manage_roles')::boolean, false)`,
		DefaultWorkspaceID).Scan(&admins); err != nil {
		t.Fatalf("count founding admins: %v", err)
	}
	if admins != 1 {
		t.Fatalf("concurrent founding admins = %d, want exactly 1", admins)
	}
}
