package messaging

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

// The four permission keys of the pre-launch model. They are deliberately few:
// a permission nobody enforces is a promise the product does not keep.
const (
	// PermManageChannels covers creating, editing, duplicating and deleting
	// channels.
	PermManageChannels = "manage_channels"
	PermManageRoles    = "manage_roles"
	PermManageMembers  = "manage_members"
	// PermMentionAll is the @everyone placeholder. Nothing enforces it yet; it
	// exists so the role editor already speaks the word the product will need.
	PermMentionAll = "mention_all"
)

// DefaultAdminRoleID and DefaultMemberRoleID are the stable identities the
// migration seeds into the shared MVP Workspace, matching DefaultWorkspaceID.
const (
	DefaultAdminRoleID  = "01900000-0000-7000-8000-000000000003"
	DefaultMemberRoleID = "01900000-0000-7000-8000-000000000004"
)

// MaxRoleNameChars matches the schema CHECK on workspace_roles.name.
const MaxRoleNameChars = 60

// Role sentinels.
var (
	ErrRoleNotFound     = errors.New("role not found")
	ErrInvalidRoleName  = errors.New("role name must be 1..60 characters")
	ErrInvalidRoleColor = errors.New("role color must be #rrggbb in lowercase")
	ErrRoleNameTaken    = errors.New("a role with that name already exists")
)

var roleColorPattern = regexp.MustCompile(`^#[0-9a-f]{6}$`)

// knownPermissions is the closed set. An unknown key on the wire is dropped
// rather than stored, so a future reader can never be surprised by a permission
// this build does not understand (fail-closed).
var knownPermissions = []string{
	PermManageChannels, PermManageRoles, PermManageMembers, PermMentionAll,
}

// Role is one bundle of permissions inside a workspace.
type Role struct {
	RoleID      string
	WorkspaceID string
	Name        string
	Color       string // empty means "no colour"
	Position    int
	Permissions map[string]bool
}

// PermissionSet is what one participant may do in one workspace: the union of
// their roles' permissions.
type PermissionSet map[string]bool

// Can reports whether the set grants the permission. A nil set grants nothing.
func (p PermissionSet) Can(permission string) bool { return p[permission] }

// normalizePermissions keeps only the keys this build enforces, and only the
// true ones — a stored false and an absent key mean the same thing.
func normalizePermissions(raw map[string]bool) map[string]bool {
	out := map[string]bool{}
	for _, key := range knownPermissions {
		if raw[key] {
			out[key] = true
		}
	}
	return out
}

func validateRoleName(name string) error {
	length := utf8.RuneCountInString(name)
	if length < 1 || length > MaxRoleNameChars {
		return ErrInvalidRoleName
	}
	return nil
}

func validateRoleColor(color string) error {
	if color == "" || roleColorPattern.MatchString(color) {
		return nil
	}
	return ErrInvalidRoleColor
}

// PermissionsFor resolves what a participant may do in a workspace.
//
// Two sources are unioned. Explicit workspace_roles rows are the model this
// layer introduces. The legacy workspace_members.role of owner/admin also
// grants everything, so a workspace created before roles existed (or created
// by CreateWorkspace in a test) still has somebody who can administer it —
// there is no state in which a workspace has no administrator at all.
//
// A participant who is not an active member of the workspace has no
// permissions, whatever roles happen to point at them.
func (s *Store) PermissionsFor(ctx context.Context, workspaceID string, participant ParticipantRef) (PermissionSet, error) {
	if err := participant.Validate(); err != nil {
		return nil, err
	}
	active, memberRole, err := s.workspaceMembership(ctx, s.pool, workspaceID, participant)
	if err != nil {
		return nil, err
	}
	if !active {
		return PermissionSet{}, nil
	}
	granted := PermissionSet{}
	if memberRole == RoleOwner || memberRole == RoleAdmin {
		for _, key := range knownPermissions {
			granted[key] = true
		}
	}
	rows, err := s.pool.Query(ctx,
		`SELECT wr.permissions FROM participant_roles pr
		 JOIN workspace_roles wr ON wr.role_id = pr.role_id
		 WHERE wr.workspace_id = $1 AND pr.member_kind = $2 AND pr.member_id = $3`,
		workspaceID, participant.Kind, participant.ID)
	if err != nil {
		return nil, fmt.Errorf("query participant permissions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var permissions map[string]bool
		if err := rows.Scan(&permissions); err != nil {
			return nil, fmt.Errorf("scan permissions: %w", err)
		}
		for key, allowed := range normalizePermissions(permissions) {
			if allowed {
				granted[key] = true
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate permissions: %w", err)
	}
	return granted, nil
}

// RequirePermission is the one gate every administrative mutation goes through,
// on the REST lane and the agent lane alike (AX 同型: the rule belongs to the
// store, not to a transport). Missing permission is ErrForbidden — a clear
// refusal, not a pretend-missing resource, because the actor can already see
// the workspace.
func (s *Store) RequirePermission(ctx context.Context, workspaceID string, actor ParticipantRef, permission string) error {
	granted, err := s.PermissionsFor(ctx, workspaceID, actor)
	if err != nil {
		return err
	}
	if !granted.Can(permission) {
		return ErrForbidden
	}
	return nil
}

// PlacePermissionsFor resolves permissions against the workspace that owns a
// place. dm/group_dm places belong to no workspace and carry no roles: nobody
// administers somebody else's conversation.
func (s *Store) PlacePermissionsFor(ctx context.Context, place Place, actor ParticipantRef) (PermissionSet, error) {
	if place.WorkspaceID == "" {
		return PermissionSet{}, nil
	}
	return s.PermissionsFor(ctx, place.WorkspaceID, actor)
}

// Roles lists a workspace's roles for a viewer who is an active member.
// Reading is not gated on manage_roles: a member needs the names and colours to
// read the member list they are already allowed to see.
func (s *Store) Roles(ctx context.Context, workspaceID string, viewer ParticipantRef) ([]Role, error) {
	if err := viewer.Validate(); err != nil {
		return nil, err
	}
	active, _, err := s.workspaceMembership(ctx, s.pool, workspaceID, viewer)
	if err != nil {
		return nil, err
	}
	if !active {
		return nil, ErrWorkspaceNotFound
	}
	rows, err := s.pool.Query(ctx,
		`SELECT role_id, workspace_id, name, color, position, permissions
		 FROM workspace_roles WHERE workspace_id = $1
		 ORDER BY position DESC, name`, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("query roles: %w", err)
	}
	defer rows.Close()
	roles := []Role{}
	for rows.Next() {
		var (
			role        Role
			color       *string
			permissions map[string]bool
		)
		if err := rows.Scan(&role.RoleID, &role.WorkspaceID, &role.Name,
			&color, &role.Position, &permissions); err != nil {
			return nil, fmt.Errorf("scan role: %w", err)
		}
		if color != nil {
			role.Color = *color
		}
		role.Permissions = normalizePermissions(permissions)
		roles = append(roles, role)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate roles: %w", err)
	}
	return roles, nil
}

// RoleAssignments maps each participant holding a role in this workspace to
// their role ids, so the member list can render badges in one round trip.
func (s *Store) RoleAssignments(ctx context.Context, workspaceID string, viewer ParticipantRef) (map[string][]string, error) {
	if err := viewer.Validate(); err != nil {
		return nil, err
	}
	active, _, err := s.workspaceMembership(ctx, s.pool, workspaceID, viewer)
	if err != nil {
		return nil, err
	}
	if !active {
		return nil, ErrWorkspaceNotFound
	}
	rows, err := s.pool.Query(ctx,
		`SELECT pr.member_kind, pr.member_id, pr.role_id
		 FROM participant_roles pr
		 JOIN workspace_roles wr ON wr.role_id = pr.role_id
		 WHERE wr.workspace_id = $1
		 ORDER BY wr.position DESC, wr.name`, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("query role assignments: %w", err)
	}
	defer rows.Close()
	assignments := map[string][]string{}
	for rows.Next() {
		var participant ParticipantRef
		var kind, roleID string
		if err := rows.Scan(&kind, &participant.ID, &roleID); err != nil {
			return nil, fmt.Errorf("scan role assignment: %w", err)
		}
		participant.Kind = ParticipantKind(kind)
		key := participant.Key()
		assignments[key] = append(assignments[key], roleID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate role assignments: %w", err)
	}
	return assignments, nil
}

// CreateRole mints a role. Requires manage_roles.
func (s *Store) CreateRole(ctx context.Context, workspaceID string, actor ParticipantRef, name, color string, permissions map[string]bool) (Role, error) {
	if err := s.RequirePermission(ctx, workspaceID, actor, PermManageRoles); err != nil {
		return Role{}, err
	}
	if err := validateRoleName(name); err != nil {
		return Role{}, err
	}
	if err := validateRoleColor(color); err != nil {
		return Role{}, err
	}
	role := Role{
		RoleID:      newUUIDv7(),
		WorkspaceID: workspaceID,
		Name:        name,
		Color:       color,
		Permissions: normalizePermissions(permissions),
	}
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO workspace_roles (role_id, workspace_id, name, color, position, permissions)
		 VALUES ($1, $2, $3, $4, 0, $5)`,
		role.RoleID, workspaceID, role.Name, nullableColor(color), role.Permissions); err != nil {
		if isUniqueViolation(err) {
			return Role{}, ErrRoleNameTaken
		}
		return Role{}, fmt.Errorf("insert role: %w", err)
	}
	return role, nil
}

// UpdateRole replaces a role's name, colour and permissions. Requires
// manage_roles.
func (s *Store) UpdateRole(ctx context.Context, workspaceID, roleID string, actor ParticipantRef, name, color string, permissions map[string]bool) (Role, error) {
	if err := s.RequirePermission(ctx, workspaceID, actor, PermManageRoles); err != nil {
		return Role{}, err
	}
	if err := validateRoleName(name); err != nil {
		return Role{}, err
	}
	if err := validateRoleColor(color); err != nil {
		return Role{}, err
	}
	role := Role{
		RoleID:      roleID,
		WorkspaceID: workspaceID,
		Name:        name,
		Color:       color,
		Permissions: normalizePermissions(permissions),
	}
	err := s.pool.QueryRow(ctx,
		`UPDATE workspace_roles SET name = $3, color = $4, permissions = $5
		 WHERE role_id = $1 AND workspace_id = $2
		 RETURNING position`,
		roleID, workspaceID, role.Name, nullableColor(color), role.Permissions).Scan(&role.Position)
	if errors.Is(err, pgx.ErrNoRows) {
		return Role{}, ErrRoleNotFound
	}
	if err != nil {
		if isUniqueViolation(err) {
			return Role{}, ErrRoleNameTaken
		}
		return Role{}, fmt.Errorf("update role: %w", err)
	}
	return role, nil
}

// DeleteRole removes a role and every grant of it (ON DELETE CASCADE).
// Requires manage_roles.
func (s *Store) DeleteRole(ctx context.Context, workspaceID, roleID string, actor ParticipantRef) error {
	if err := s.RequirePermission(ctx, workspaceID, actor, PermManageRoles); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx,
		"DELETE FROM workspace_roles WHERE role_id = $1 AND workspace_id = $2",
		roleID, workspaceID)
	if err != nil {
		return fmt.Errorf("delete role: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrRoleNotFound
	}
	return nil
}

// SetParticipantRoles replaces the set of roles one member holds. Requires
// manage_members. Passing an empty list removes every role, which is how a
// participant is returned to plain membership.
func (s *Store) SetParticipantRoles(ctx context.Context, workspaceID string, actor, member ParticipantRef, roleIDs []string) ([]string, error) {
	if err := s.RequirePermission(ctx, workspaceID, actor, PermManageMembers); err != nil {
		return nil, err
	}
	if err := member.Validate(); err != nil {
		return nil, err
	}
	active, _, err := s.workspaceMembership(ctx, s.pool, workspaceID, member)
	if err != nil {
		return nil, err
	}
	if !active {
		return nil, ErrNotAMember
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin set participant roles: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`DELETE FROM participant_roles pr
		 USING workspace_roles wr
		 WHERE wr.role_id = pr.role_id AND wr.workspace_id = $1
		   AND pr.member_kind = $2 AND pr.member_id = $3`,
		workspaceID, member.Kind, member.ID); err != nil {
		return nil, fmt.Errorf("clear participant roles: %w", err)
	}
	stored := make([]string, 0, len(roleIDs))
	for _, roleID := range roleIDs {
		// The role must belong to this workspace: a role id from elsewhere is
		// not found rather than silently granted.
		tag, err := tx.Exec(ctx,
			`INSERT INTO participant_roles (role_id, member_kind, member_id)
			 SELECT $1::uuidv7, $2::text, $3::uuidv7 FROM workspace_roles
			 WHERE role_id = $1::uuidv7 AND workspace_id = $4
			 ON CONFLICT DO NOTHING`,
			roleID, member.Kind, member.ID, workspaceID)
		if err != nil {
			return nil, fmt.Errorf("grant participant role: %w", err)
		}
		if tag.RowsAffected() == 0 {
			var exists bool
			if err := tx.QueryRow(ctx,
				`SELECT EXISTS (SELECT 1 FROM workspace_roles
				  WHERE role_id = $1 AND workspace_id = $2)`,
				roleID, workspaceID).Scan(&exists); err != nil {
				return nil, fmt.Errorf("check role workspace: %w", err)
			}
			if !exists {
				return nil, ErrRoleNotFound
			}
		}
		stored = append(stored, roleID)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit set participant roles: %w", err)
	}
	return stored, nil
}

// EnsureFoundingAdmin gives the arriving Human the Admin role when the shared
// Workspace has nobody who can administer it.
//
// The migration made every Human who already existed an Admin. A deployment
// migrated before its first Human — a fresh development database, for instance
// — would otherwise have a workspace nobody can add a channel to. The rule is
// narrow on purpose: it fires only while no participant holds manage_roles, so
// the second arrival is an ordinary member.
func (s *Store) EnsureFoundingAdmin(ctx context.Context, participant ParticipantRef) error {
	if participant.Kind != KindHuman {
		return nil
	}
	var administered bool
	if err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (
		   SELECT 1 FROM participant_roles pr
		   JOIN workspace_roles wr ON wr.role_id = pr.role_id
		   JOIN workspace_members wm
		     ON wm.workspace_id = wr.workspace_id
		    AND wm.member_kind = pr.member_kind
		    AND wm.member_id = pr.member_id
		    AND wm.left_at IS NULL
		   WHERE wr.workspace_id = $1
		     AND COALESCE((wr.permissions ->> 'manage_roles')::boolean, false))`,
		DefaultWorkspaceID).Scan(&administered); err != nil {
		return fmt.Errorf("check workspace administration: %w", err)
	}
	if administered {
		return nil
	}
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO participant_roles (role_id, member_kind, member_id)
		 SELECT $1::uuidv7, $2::text, $3::uuidv7 FROM workspace_roles
		 WHERE role_id = $1::uuidv7
		 ON CONFLICT DO NOTHING`,
		DefaultAdminRoleID, participant.Kind, participant.ID); err != nil {
		return fmt.Errorf("grant founding admin: %w", err)
	}
	return nil
}

func nullableColor(color string) *string {
	if color == "" {
		return nil
	}
	return &color
}
