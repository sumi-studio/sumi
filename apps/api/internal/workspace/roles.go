package workspace

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
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
	for i := range roles {
		roles[i].AppCapabilities, err = appCapabilitiesForRole(
			ctx, tx, roles[i].WorkspaceID, roles[i].RoleID,
		)
		if err != nil {
			return nil, err
		}
	}
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
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Role{}, fmt.Errorf("begin create workspace role: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockWorkspace(ctx, tx, workspaceID); err != nil {
		return Role{}, err
	}
	requested, err := resolveRoleCapabilities(ctx, tx, permissions)
	if err != nil {
		return Role{}, err
	}
	actorMembership, actorPermissions, err := roleMutationAuthority(ctx, tx, workspaceID, actor)
	if err != nil {
		return Role{}, err
	}
	if !actorMembership.Owner &&
		(!actorPermissions.Can(PermissionManageRoles) ||
			!permissionsWithin(actorPermissions, requested.effective())) {
		return Role{}, ErrForbidden
	}
	role := Role{
		RoleID: newUUIDv7(), WorkspaceID: workspaceID, Name: name,
		Color: color, Permissions: requested.platform,
		AppCapabilities: requested.appSet(), CreatedAt: s.now().UTC(),
	}
	if position != nil {
		role.Position = *position
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO workspace_roles
			(role_id, workspace_id, name, color, position, permissions, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING position`, role.RoleID, workspaceID, name, nullableColor(color),
		role.Position, requested.platform, role.CreatedAt,
	).Scan(&role.Position)
	if err != nil {
		if isUniqueViolation(err) {
			return Role{}, ErrRoleNameTaken
		}
		return Role{}, fmt.Errorf("insert workspace role: %w", err)
	}
	if err := replaceRoleAppCapabilities(ctx, tx, role, requested.app); err != nil {
		return Role{}, err
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
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Role{}, fmt.Errorf("begin update workspace role: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockWorkspace(ctx, tx, workspaceID); err != nil {
		return Role{}, err
	}
	previous, err := roleByID(ctx, tx, workspaceID, roleID)
	if err != nil {
		return Role{}, err
	}
	requested, err := resolveRoleCapabilitiesForUpdate(ctx, tx, previous, permissions)
	if err != nil {
		return Role{}, err
	}
	actorMembership, actorPermissions, err := roleMutationAuthority(ctx, tx, workspaceID, actor)
	if err != nil {
		return Role{}, err
	}
	if !actorMembership.Owner &&
		(!actorPermissions.Can(PermissionManageRoles) ||
			!permissionsWithin(actorPermissions, previous.EffectiveCapabilities()) ||
			!permissionsWithin(actorPermissions, requested.effective())) {
		return Role{}, ErrForbidden
	}
	previous.Name = name
	previous.Color = color
	previous.Permissions = requested.platform
	previous.AppCapabilities = requested.appSet()
	if position != nil {
		previous.Position = *position
	}
	err = tx.QueryRow(ctx, `
		UPDATE workspace_roles
		SET name = $3, color = $4, position = $5, permissions = $6
		WHERE workspace_id = $1 AND role_id = $2
		RETURNING position, created_at`, workspaceID, roleID, name,
		nullableColor(color), previous.Position, requested.platform,
	).Scan(&previous.Position, &previous.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return Role{}, ErrRoleNameTaken
		}
		return Role{}, fmt.Errorf("update workspace role: %w", err)
	}
	if err := replaceRoleAppCapabilities(ctx, tx, previous, requested.app); err != nil {
		return Role{}, err
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
	actorMembership, actorPermissions, err := roleMutationAuthority(ctx, tx, workspaceID, actor)
	if err != nil {
		return err
	}
	target, err := roleByID(ctx, tx, workspaceID, roleID)
	if err != nil {
		return err
	}
	if !actorMembership.Owner &&
		(!actorPermissions.Can(PermissionManageRoles) ||
			!permissionsWithin(actorPermissions, target.EffectiveCapabilities())) {
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
	actorMembership, actorPermissions, err := roleMutationAuthority(ctx, tx, workspaceID, actor)
	if err != nil {
		return nil, err
	}
	if !actorMembership.Owner && !actorPermissions.Can(PermissionManageMembers) {
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
			!actorMembership.Owner &&
			!permissionsWithin(actorPermissions, role.EffectiveCapabilities()) {
			return nil, ErrForbidden
		}
	}
	for roleID, role := range requestedRoles {
		if _, unchanged := currentRoles[roleID]; !unchanged &&
			!actorMembership.Owner &&
			!permissionsWithin(actorPermissions, role.EffectiveCapabilities()) {
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
	role.AppCapabilities, err = appCapabilitiesForRole(ctx, q, workspaceID, roleID)
	if err != nil {
		return Role{}, err
	}
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
		rows.Close()
		return nil, fmt.Errorf("iterate workspace member roles: %w", err)
	}
	rows.Close()
	for roleID, role := range roles {
		role.AppCapabilities, err = appCapabilitiesForRole(ctx, q, workspaceID, roleID)
		if err != nil {
			return nil, err
		}
		roles[roleID] = role
	}
	return roles, nil
}

type resolvedAppCapability struct {
	CapabilityID string
	Ref          string
	Effective    bool
}

type resolvedRoleCapabilitySet struct {
	platform PermissionSet
	app      []resolvedAppCapability
}

func (set resolvedRoleCapabilitySet) appSet() AppCapabilitySet {
	out := make(AppCapabilitySet, len(set.app))
	for _, capability := range set.app {
		out[capability.Ref] = capability.Effective
	}
	return out
}

func (set resolvedRoleCapabilitySet) effective() PermissionSet {
	role := Role{Permissions: set.platform, AppCapabilities: set.appSet()}
	return role.EffectiveCapabilities()
}

func resolveRoleCapabilities(ctx context.Context, q querier, input map[string]bool) (resolvedRoleCapabilitySet, error) {
	resolved := resolvedRoleCapabilitySet{platform: normalizePermissions(input)}
	refs, err := requestedAppCapabilityRefs(input)
	if err != nil {
		return resolvedRoleCapabilitySet{}, err
	}
	for _, ref := range refs {
		capability, err := resolveActiveAppCapability(ctx, q, ref)
		if err != nil {
			return resolvedRoleCapabilitySet{}, err
		}
		resolved.app = append(resolved.app, capability)
	}
	return resolved, nil
}

// resolveRoleCapabilitiesForUpdate distinguishes preservation from a new
// grant. A ref already stored on this exact role keeps its catalog identity and
// snapshot even after retirement; removing it and later adding the same
// spelling is a new grant and must resolve through the active catalog. Taking a
// SHARE lock on every preserved catalog identity also keeps its effective state
// stable through the authority checks and commit below.
func resolveRoleCapabilitiesForUpdate(ctx context.Context, q querier, role Role, input map[string]bool) (resolvedRoleCapabilitySet, error) {
	resolved := resolvedRoleCapabilitySet{platform: normalizePermissions(input)}
	existing, err := storedRoleAppCapabilities(ctx, q, role.WorkspaceID, role.RoleID)
	if err != nil {
		return resolvedRoleCapabilitySet{}, err
	}
	refs, err := requestedAppCapabilityRefs(input)
	if err != nil {
		return resolvedRoleCapabilitySet{}, err
	}
	for _, ref := range refs {
		if preserved, ok := existing[ref]; ok {
			resolved.app = append(resolved.app, preserved)
			continue
		}
		active, err := resolveActiveAppCapability(ctx, q, ref)
		if err != nil {
			return resolvedRoleCapabilitySet{}, err
		}
		resolved.app = append(resolved.app, active)
	}
	return resolved, nil
}

func requestedAppCapabilityRefs(input map[string]bool) ([]string, error) {
	refs := make([]string, 0, len(input))
	for ref, allowed := range input {
		if !allowed || isKnownPermission(ref) {
			continue
		}
		if !appCapabilityRefPattern.MatchString(ref) {
			return nil, ErrInvalidPermission
		}
		refs = append(refs, ref)
	}
	sort.Strings(refs)
	return refs, nil
}

func resolveActiveAppCapability(ctx context.Context, q querier, ref string) (resolvedAppCapability, error) {
	var capabilityID string
	err := q.QueryRow(ctx, `
		SELECT capability_id
		FROM app_workspace_role_capabilities
		WHERE capability_ref = $1 AND retired_at IS NULL
		FOR SHARE`, ref).Scan(&capabilityID)
	if errors.Is(err, pgx.ErrNoRows) {
		return resolvedAppCapability{}, ErrInvalidPermission
	}
	if err != nil {
		return resolvedAppCapability{}, fmt.Errorf("resolve app Workspace-role capability: %w", err)
	}
	return resolvedAppCapability{CapabilityID: capabilityID, Ref: ref, Effective: true}, nil
}

func storedRoleAppCapabilities(ctx context.Context, q querier, workspaceID, roleID string) (map[string]resolvedAppCapability, error) {
	rows, err := q.Query(ctx, `
		SELECT role_grant.capability_id, role_grant.capability_ref_snapshot,
		       capability.retired_at IS NULL
		       AND capability.capability_ref = role_grant.capability_ref_snapshot
		FROM workspace_role_app_capability_grants role_grant
		JOIN app_workspace_role_capabilities capability
		  ON capability.capability_id = role_grant.capability_id
		WHERE role_grant.workspace_id = $1 AND role_grant.role_id = $2
		ORDER BY role_grant.capability_ref_snapshot
		FOR SHARE OF capability`, workspaceID, roleID)
	if err != nil {
		return nil, fmt.Errorf("query stored role app capabilities: %w", err)
	}
	defer rows.Close()
	stored := map[string]resolvedAppCapability{}
	for rows.Next() {
		var capability resolvedAppCapability
		if err := rows.Scan(&capability.CapabilityID, &capability.Ref, &capability.Effective); err != nil {
			return nil, fmt.Errorf("scan stored role app capability: %w", err)
		}
		stored[capability.Ref] = capability
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stored role app capabilities: %w", err)
	}
	return stored, nil
}

func replaceRoleAppCapabilities(ctx context.Context, q querier, role Role, capabilities []resolvedAppCapability) error {
	if _, err := q.Exec(ctx, `
		DELETE FROM workspace_role_app_capability_grants
		WHERE workspace_id = $1 AND role_id = $2`, role.WorkspaceID, role.RoleID); err != nil {
		return fmt.Errorf("clear role app capability grants: %w", err)
	}
	for _, capability := range capabilities {
		if _, err := q.Exec(ctx, `
			INSERT INTO workspace_role_app_capability_grants
				(workspace_id, role_id, capability_id, capability_ref_snapshot)
			VALUES ($1, $2, $3, $4)`, role.WorkspaceID, role.RoleID,
			capability.CapabilityID, capability.Ref); err != nil {
			return fmt.Errorf("store role app capability grant: %w", err)
		}
	}
	return nil
}

func appCapabilitiesForRole(ctx context.Context, q querier, workspaceID, roleID string) (AppCapabilitySet, error) {
	rows, err := q.Query(ctx, `
		SELECT role_grant.capability_ref_snapshot,
		       capability.retired_at IS NULL
		       AND capability.capability_ref = role_grant.capability_ref_snapshot
		FROM workspace_role_app_capability_grants role_grant
		JOIN app_workspace_role_capabilities capability
		  ON capability.capability_id = role_grant.capability_id
		WHERE role_grant.workspace_id = $1 AND role_grant.role_id = $2
		ORDER BY role_grant.capability_ref_snapshot`, workspaceID, roleID)
	if err != nil {
		return nil, fmt.Errorf("query role app capabilities: %w", err)
	}
	defer rows.Close()
	capabilities := AppCapabilitySet{}
	for rows.Next() {
		var ref string
		var effective bool
		if err := rows.Scan(&ref, &effective); err != nil {
			return nil, fmt.Errorf("scan role app capability: %w", err)
		}
		capabilities[ref] = effective
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate role app capabilities: %w", err)
	}
	return capabilities, nil
}

func roleMutationAuthority(ctx context.Context, q querier, workspaceID string, actor participant.Ref) (Membership, PermissionSet, error) {
	membership, err := activeMembership(ctx, q, workspaceID, actor)
	if err != nil {
		return Membership{}, nil, err
	}
	permissions, err := permissionsForMembership(ctx, q, membership)
	if err != nil {
		return Membership{}, nil, err
	}
	return membership, permissions, nil
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
