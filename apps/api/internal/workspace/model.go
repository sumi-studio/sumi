// Package workspace owns Sumi Workspace identity, membership tenure, custom
// roles, consent-based admission, and their application-wide authorization.
package workspace

import (
	"errors"
	"sort"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

const (
	PermissionManageWorkspace = "manage_workspace"
	PermissionManageMembers   = "manage_members"
	PermissionManageRoles     = "manage_roles"
	PermissionManageApps      = "manage_apps"
	// Messaging is a consumer of Workspace authority. These permissions remain
	// application-wide vocabulary even though Workspace core does not implement
	// channel or mention behavior itself.
	PermissionManageChannels = "manage_channels"
	PermissionMentionAll     = "mention_all"
)

var knownPermissions = []string{
	PermissionManageWorkspace,
	PermissionManageMembers,
	PermissionManageRoles,
	PermissionManageApps,
	PermissionManageChannels,
	PermissionMentionAll,
}

var administrativePermissions = []string{
	PermissionManageWorkspace,
	PermissionManageMembers,
	PermissionManageRoles,
	PermissionManageApps,
}

var (
	ErrNotFound          = errors.New("workspace not found")
	ErrForbidden         = errors.New("workspace operation forbidden")
	ErrInvalidName       = errors.New("invalid workspace or role name")
	ErrInvalidColor      = errors.New("invalid role color")
	ErrInvalidPosition   = errors.New("invalid role position")
	ErrInvalidPermission = errors.New("invalid workspace permission")
	ErrRoleNotFound      = errors.New("workspace role not found")
	ErrRoleNameTaken     = errors.New("workspace role name is already used")
	ErrMemberNotFound    = errors.New("workspace member not found")
	ErrAlreadyMember     = errors.New("participant is already a workspace member")
	ErrOwnerProtected    = errors.New("workspace owner membership cannot change")
	ErrInviteUnavailable = errors.New("workspace invite is unavailable")
	ErrInvalidInvite     = errors.New("invalid workspace invite settings")
)

type PermissionSet map[string]bool

func (p PermissionSet) Can(permission string) bool { return p[permission] }

func (p PermissionSet) Keys() []string {
	keys := make([]string, 0, len(p))
	for _, permission := range knownPermissions {
		if p[permission] {
			keys = append(keys, permission)
		}
	}
	sort.Strings(keys)
	return keys
}

func allPermissions() PermissionSet {
	permissions := make(PermissionSet, len(knownPermissions))
	for _, permission := range knownPermissions {
		permissions[permission] = true
	}
	return permissions
}

type Workspace struct {
	WorkspaceID            string
	Name                   string
	OwnerWorkspaceMemberID string
	CreatedAt              time.Time
}

type Membership struct {
	WorkspaceMemberID string
	WorkspaceID       string
	Participant       participant.Ref
	Owner             bool
	RoleIDs           []string
	JoinedAt          time.Time
	LeftAt            *time.Time
}

type Role struct {
	RoleID      string
	WorkspaceID string
	Name        string
	Color       string
	Position    int
	Permissions PermissionSet
	CreatedAt   time.Time
}

type Invite struct {
	InviteID    string
	WorkspaceID string
	Code        string
	ExpiresAt   time.Time
	CreatedAt   time.Time
}

// InvitePreview is deliberately smaller than Workspace. Possession of an
// unconsumed invite code reveals only enough information to make an informed
// redemption choice; it is not a Workspace directory or membership view.
type InvitePreview struct {
	WorkspaceID   string
	WorkspaceName string
	ExpiresAt     time.Time
}
