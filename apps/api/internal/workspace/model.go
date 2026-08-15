// Package workspace owns Sumi Workspace identity, membership tenure, custom
// roles, consent-based admission, and their application-wide authorization.
package workspace

import (
	"errors"
	"regexp"
	"sort"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

const (
	PermissionManageWorkspace = "manage_workspace"
	PermissionManageMembers   = "manage_members"
	PermissionManageRoles     = "manage_roles"
	PermissionManageApps      = "manage_apps"
)

var knownPermissions = []string{
	PermissionManageWorkspace,
	PermissionManageMembers,
	PermissionManageRoles,
	PermissionManageApps,
}

// App capability refs are namespaced by their owning app. Workspace validates
// this durable shape and catalog membership but never interprets the final
// app-owned segment.
var appCapabilityRefPattern = regexp.MustCompile(
	`^app\.[a-z][a-z0-9-]{0,63}\.[a-z][a-z0-9_]{0,63}$`,
)

var administrativePermissions = []string{
	PermissionManageWorkspace,
	PermissionManageMembers,
	PermissionManageRoles,
	PermissionManageApps,
}

var (
	ErrNotFound                   = errors.New("workspace not found")
	ErrForbidden                  = errors.New("workspace operation forbidden")
	ErrInvalidName                = errors.New("invalid workspace or role name")
	ErrInvalidColor               = errors.New("invalid role color")
	ErrInvalidPosition            = errors.New("invalid role position")
	ErrInvalidPermission          = errors.New("invalid workspace permission")
	ErrRoleNotFound               = errors.New("workspace role not found")
	ErrRoleNameTaken              = errors.New("workspace role name is already used")
	ErrMemberNotFound             = errors.New("workspace member not found")
	ErrAlreadyMember              = errors.New("participant is already a workspace member")
	ErrOwnerProtected             = errors.New("workspace owner membership cannot change")
	ErrLastAdministrator          = errors.New("workspace must retain an effective administrator")
	ErrInviteUnavailable          = errors.New("workspace invite is unavailable")
	ErrInvalidInvite              = errors.New("invalid workspace invite settings")
	ErrInviteAuthorityUnavailable = errors.New("current-agent invite authority is unavailable")
)

var ErrInvalidWorkspaceListCursor = errors.New("invalid workspace list cursor")
var ErrInvalidWorkspaceInvitationListCursor = errors.New("invalid workspace invitation list cursor")

type PermissionSet map[string]bool

func (p PermissionSet) Can(permission string) bool { return p[permission] }

func (p PermissionSet) Keys() []string {
	keys := make([]string, 0, len(p))
	for permission, allowed := range p {
		if allowed {
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

// AppCapabilitySet preserves every catalog-backed ref stored on a role. The
// bool says whether the exact catalog identity is currently active and may
// contribute authority; false refs remain visible but are fail-closed.
type AppCapabilitySet map[string]bool

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
	// DisplayName is projected from the canonical Human/PersonalityAgent
	// registry. Workspace does not own a second participant-name store.
	DisplayName string
	Owner       bool
	RoleIDs     []string
	JoinedAt    time.Time
	LeftAt      *time.Time
}

type Role struct {
	RoleID      string
	WorkspaceID string
	Name        string
	Color       string
	Position    int
	Permissions PermissionSet
	// AppCapabilities is deliberately separate from Workspace's four platform
	// permissions. Its keys are the display/preservation projection; only true
	// values are effective authorization.
	AppCapabilities AppCapabilitySet
	CreatedAt       time.Time
}

func (r Role) CapabilityRefs() []string {
	refs := r.Permissions.Keys()
	for ref := range r.AppCapabilities {
		refs = append(refs, ref)
	}
	sort.Strings(refs)
	return refs
}

func (r Role) EffectiveCapabilities() PermissionSet {
	effective := make(PermissionSet, len(r.Permissions)+len(r.AppCapabilities))
	for permission, allowed := range r.Permissions {
		if allowed {
			effective[permission] = true
		}
	}
	for ref, active := range r.AppCapabilities {
		if active {
			effective[ref] = true
		}
	}
	return effective
}

type Invite struct {
	InviteID    string
	WorkspaceID string
	Code        string
	ExpiresAt   time.Time
	CreatedAt   time.Time
}

type InviteKind string

const (
	InviteKindShareCode                InviteKind = "share_code"
	InviteKindTargetedPersonalityAgent InviteKind = "targeted_personality_agent"
)

// InviteRecord is the non-secret control-plane projection retained after the
// one-time plaintext code has left the create response.
type InviteRecord struct {
	InviteID    string
	WorkspaceID string
	Kind        InviteKind
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

// TargetedInvitation is the exact, non-secret intent visible to its target
// PersonalityAgent.  It deliberately contains no issuer or target identity:
// the local-control bearer supplies the target and issuer authority is only a
// current admission predicate.
type TargetedInvitation struct {
	InvitationID  string
	WorkspaceID   string
	WorkspaceName string
	ExpiresAt     time.Time
	CreatedAt     time.Time
}
