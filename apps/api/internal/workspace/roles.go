package workspace

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

const (
	maxRoleNameChars = 60
	maxRolePosition  = 1_000_000
)

var roleColorPattern = regexp.MustCompile(`^#[0-9a-f]{6}$`)

func (s *Store) Roles(ctx context.Context, workspaceID string, actor participant.Ref) ([]Role, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("begin workspace-role read: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := activeMembership(ctx, tx, workspaceID, actor); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT role_id, workspace_id, name, color, position, permissions, created_at
		FROM workspace_roles WHERE workspace_id = $1
		ORDER BY position DESC, name, role_id`, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("list workspace roles: %w", err)
	}
	roles := []Role{}
	for rows.Next() {
		var role Role
		var color *string
		var permissions map[string]bool
		if err := rows.Scan(&role.RoleID, &role.WorkspaceID, &role.Name, &color,
			&role.Position, &permissions, &role.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan workspace role: %w", err)
		}
		if color != nil {
			role.Color = *color
		}
		role.Permissions = normalizePermissions(permissions)
		roles = append(roles, role)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate workspace roles: %w", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit workspace-role read: %w", err)
	}
	return roles, nil
}

func (s *Store) CreateRole(ctx context.Context, workspaceID string, actor participant.Ref, name, color string, permissions map[string]bool) (Role, error) {
	return s.CreateRoleWithPosition(ctx, workspaceID, actor, name, color, permissions, nil)
}

// CreateRoleWithPosition exposes the ordering hint used by both Human and
// Agent transports. A missing position remains compatible with the current Web
// role model and creates the role at position zero.
func (s *Store) CreateRoleWithPosition(ctx context.Context, workspaceID string, actor participant.Ref, name, color string, permissions map[string]bool, position *int) (Role, error) {
	name = strings.TrimSpace(name)
	if err := validateRolePresentation(name, color); err != nil {
		return Role{}, err
	}
	if err := validateRolePosition(position); err != nil {
		return Role{}, err
	}
	normalized, err := validatePermissions(permissions)
	if err != nil {
		return Role{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Role{}, fmt.Errorf("begin create workspace role: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockWorkspace(ctx, tx, workspaceID); err != nil {
		return Role{}, err
	}
	actorPermissions, err := s.permissionsFor(ctx, tx, workspaceID, actor)
	if err != nil {
		return Role{}, err
	}
	if !actorPermissions.Can(PermissionManageRoles) || !permissionsWithin(actorPermissions, normalized) {
		return Role{}, ErrForbidden
	}
	role := Role{
		RoleID: newUUIDv7(), WorkspaceID: workspaceID, Name: name,
		Color: color, Permissions: normalized, CreatedAt: s.now().UTC(),
	}
	if position != nil {
		role.Position = *position
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO workspace_roles
			(role_id, workspace_id, name, color, position, permissions, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING position`, role.RoleID, workspaceID, name, nullableColor(color),
		role.Position, normalized, role.CreatedAt,
	).Scan(&role.Position)
	if err != nil {
		if isUniqueViolation(err) {
			return Role{}, ErrRoleNameTaken
		}
		return Role{}, fmt.Errorf("insert workspace role: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Role{}, fmt.Errorf("commit workspace role: %w", err)
	}
	return role, nil
}

func (s *Store) UpdateRole(ctx context.Context, workspaceID, roleID string, actor participant.Ref, name, color string, permissions map[string]bool) (Role, error) {
	return s.UpdateRoleWithPosition(ctx, workspaceID, roleID, actor, name, color, permissions, nil)
}

// UpdateRoleWithPosition preserves the stored ordering when position is
// omitted and changes it only when the caller explicitly supplies a validated
// value.
func (s *Store) UpdateRoleWithPosition(ctx context.Context, workspaceID, roleID string, actor participant.Ref, name, color string, permissions map[string]bool, position *int) (Role, error) {
	name = strings.TrimSpace(name)
	if err := validateRolePresentation(name, color); err != nil {
		return Role{}, err
	}
	if err := validateRolePosition(position); err != nil {
		return Role{}, err
	}
	normalized, err := validatePermissions(permissions)
	if err != nil {
		return Role{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Role{}, fmt.Errorf("begin update workspace role: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockWorkspace(ctx, tx, workspaceID); err != nil {
		return Role{}, err
	}
	actorPermissions, err := s.permissionsFor(ctx, tx, workspaceID, actor)
	if err != nil {
		return Role{}, err
	}
	if !actorPermissions.Can(PermissionManageRoles) {
		return Role{}, ErrForbidden
	}
	previous, err := roleByID(ctx, tx, workspaceID, roleID)
	if err != nil {
		return Role{}, err
	}
	if !permissionsWithin(actorPermissions, previous.Permissions) ||
		!permissionsWithin(actorPermissions, normalized) {
		return Role{}, ErrForbidden
	}
	previous.Name = name
	previous.Color = color
	previous.Permissions = normalized
	if position != nil {
		previous.Position = *position
	}
	err = tx.QueryRow(ctx, `
		UPDATE workspace_roles
		SET name = $3, color = $4, position = $5, permissions = $6
		WHERE workspace_id = $1 AND role_id = $2
		RETURNING position, created_at`, workspaceID, roleID, name,
		nullableColor(color), previous.Position, normalized,
	).Scan(&previous.Position, &previous.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return Role{}, ErrRoleNameTaken
		}
		return Role{}, fmt.Errorf("update workspace role: %w", err)
	}
	if err := ensureEffectiveAdministrator(ctx, tx, workspaceID); err != nil {
		return Role{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Role{}, fmt.Errorf("commit workspace role update: %w", err)
	}
	return previous, nil
}

func (s *Store) DeleteRole(ctx context.Context, workspaceID, roleID string, actor participant.Ref) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin delete workspace role: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockWorkspace(ctx, tx, workspaceID); err != nil {
		return err
	}
	actorPermissions, err := s.permissionsFor(ctx, tx, workspaceID, actor)
	if err != nil {
		return err
	}
	if !actorPermissions.Can(PermissionManageRoles) {
		return ErrForbidden
	}
	target, err := roleByID(ctx, tx, workspaceID, roleID)
	if err != nil {
		return err
	}
	if !permissionsWithin(actorPermissions, target.Permissions) {
		return ErrForbidden
	}
	if _, err := tx.Exec(ctx,
		"DELETE FROM workspace_roles WHERE workspace_id = $1 AND role_id = $2",
		workspaceID, roleID,
	); err != nil {
		return fmt.Errorf("delete workspace role: %w", err)
	}
	if err := ensureEffectiveAdministrator(ctx, tx, workspaceID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit workspace role deletion: %w", err)
	}
	return nil
}

func (s *Store) SetMembershipRoles(ctx context.Context, workspaceID, membershipID string, actor participant.Ref, roleIDs []string) ([]string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin set workspace member roles: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockWorkspace(ctx, tx, workspaceID); err != nil {
		return nil, err
	}
	actorPermissions, err := s.permissionsFor(ctx, tx, workspaceID, actor)
	if err != nil {
		return nil, err
	}
	if !actorPermissions.Can(PermissionManageMembers) {
		return nil, ErrForbidden
	}
	target, err := membershipByID(ctx, tx, workspaceID, membershipID, true)
	if err != nil {
		return nil, err
	}
	currentRoles, err := rolesForMembership(ctx, tx, workspaceID, membershipID)
	if err != nil {
		return nil, err
	}
	requestedRoles := make(map[string]Role, len(roleIDs))
	stored := make([]string, 0, len(roleIDs))
	for _, roleID := range roleIDs {
		if _, duplicate := requestedRoles[roleID]; duplicate {
			continue
		}
		role, err := roleByID(ctx, tx, workspaceID, roleID)
		if err != nil {
			return nil, err
		}
		requestedRoles[roleID] = role
		stored = append(stored, roleID)
	}
	for roleID, role := range currentRoles {
		if _, unchanged := requestedRoles[roleID]; !unchanged &&
			!permissionsWithin(actorPermissions, role.Permissions) {
			return nil, ErrForbidden
		}
	}
	for roleID, role := range requestedRoles {
		if _, unchanged := currentRoles[roleID]; !unchanged &&
			!permissionsWithin(actorPermissions, role.Permissions) {
			return nil, ErrForbidden
		}
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM workspace_role_assignments
		WHERE workspace_id = $1 AND workspace_member_id = $2`, workspaceID, membershipID); err != nil {
		return nil, fmt.Errorf("clear workspace member roles: %w", err)
	}
	for _, roleID := range stored {
		if _, err := tx.Exec(ctx, `
			INSERT INTO workspace_role_assignments
				(workspace_id, role_id, workspace_member_id)
			VALUES ($1, $2, $3)`, workspaceID, roleID, membershipID); err != nil {
			return nil, fmt.Errorf("assign workspace role: %w", err)
		}
	}
	if err := ensureEffectiveAdministrator(ctx, tx, workspaceID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit workspace member roles: %w", err)
	}
	_ = target // target lookup pins active tenure and prevents stale assignment.
	return stored, nil
}

func roleByID(ctx context.Context, q querier, workspaceID, roleID string) (Role, error) {
	if !isCanonicalUUIDv7(workspaceID) || !isCanonicalUUIDv7(roleID) {
		return Role{}, ErrRoleNotFound
	}
	var role Role
	var color *string
	var permissions map[string]bool
	err := q.QueryRow(ctx, `
		SELECT role_id, workspace_id, name, color, position, permissions, created_at
		FROM workspace_roles WHERE workspace_id = $1 AND role_id = $2`,
		workspaceID, roleID,
	).Scan(&role.RoleID, &role.WorkspaceID, &role.Name, &color,
		&role.Position, &permissions, &role.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Role{}, ErrRoleNotFound
	}
	if err != nil {
		return Role{}, fmt.Errorf("load workspace role: %w", err)
	}
	if color != nil {
		role.Color = *color
	}
	role.Permissions = normalizePermissions(permissions)
	return role, nil
}

func rolesForMembership(ctx context.Context, q querier, workspaceID, membershipID string) (map[string]Role, error) {
	rows, err := q.Query(ctx, `
		SELECT wr.role_id, wr.workspace_id, wr.name, wr.color, wr.position,
		       wr.permissions, wr.created_at
		FROM workspace_role_assignments wra
		JOIN workspace_roles wr
		  ON wr.workspace_id = wra.workspace_id AND wr.role_id = wra.role_id
		WHERE wra.workspace_id = $1 AND wra.workspace_member_id = $2`,
		workspaceID, membershipID)
	if err != nil {
		return nil, fmt.Errorf("query workspace member roles: %w", err)
	}
	defer rows.Close()
	roles := map[string]Role{}
	for rows.Next() {
		var role Role
		var color *string
		var permissions map[string]bool
		if err := rows.Scan(&role.RoleID, &role.WorkspaceID, &role.Name, &color,
			&role.Position, &permissions, &role.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan workspace member role: %w", err)
		}
		if color != nil {
			role.Color = *color
		}
		role.Permissions = normalizePermissions(permissions)
		roles[role.RoleID] = role
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspace member roles: %w", err)
	}
	return roles, nil
}

func validateRolePresentation(name, color string) error {
	if utf8.RuneCountInString(name) < 1 || utf8.RuneCountInString(name) > maxRoleNameChars {
		return ErrInvalidName
	}
	if color != "" && !roleColorPattern.MatchString(color) {
		return ErrInvalidColor
	}
	return nil
}

func validateRolePosition(position *int) error {
	if position != nil && (*position < 0 || *position > maxRolePosition) {
		return ErrInvalidPosition
	}
	return nil
}

func nullableColor(color string) *string {
	if color == "" {
		return nil
	}
	return &color
}
