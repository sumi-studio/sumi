// Package apps owns the canonical app catalog and installation lifecycle.
// App-owned data and authorization remain with each app; an installation is a
// binding, not a cascade root for product data.
package apps

import (
	"errors"
	"fmt"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/canonicalid"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

type OwnerKind string

const (
	OwnerWorkspace   OwnerKind = "workspace"
	OwnerParticipant OwnerKind = "participant"
)

type OwnerRef struct {
	Kind           OwnerKind
	WorkspaceID    string
	ParticipantRef participant.Ref
}

func WorkspaceOwner(workspaceID string) OwnerRef {
	return OwnerRef{Kind: OwnerWorkspace, WorkspaceID: workspaceID}
}

func ParticipantOwner(ref participant.Ref) OwnerRef {
	return OwnerRef{Kind: OwnerParticipant, ParticipantRef: ref}
}

func (o OwnerRef) Participant() (participant.Ref, bool) {
	if o.Kind != OwnerParticipant {
		return participant.Ref{}, false
	}
	return o.ParticipantRef, true
}

func (o OwnerRef) Validate() error {
	switch o.Kind {
	case OwnerWorkspace:
		if o.ParticipantRef != (participant.Ref{}) {
			return errors.New("Workspace app owner contains ParticipantRef")
		}
		if !isCanonicalUUIDv7(o.WorkspaceID) {
			return fmt.Errorf("workspace_id must be a canonical lowercase UUIDv7")
		}
		return nil
	case OwnerParticipant:
		if o.WorkspaceID != "" {
			return errors.New("Participant app owner contains WorkspaceId")
		}
		return o.ParticipantRef.Validate()
	default:
		return fmt.Errorf("unknown app installation owner kind %q", o.Kind)
	}
}

type State string

const (
	StateEnabled  State = "enabled"
	StateDisabled State = "disabled"
)

var (
	ErrForbidden            = errors.New("app lifecycle operation forbidden")
	ErrAppNotFound          = errors.New("app not found")
	ErrOwnerKindUnsupported = errors.New("app does not support this owner kind")
	ErrAlreadyInstalled     = errors.New("app is already installed for this owner")
	ErrInstallationNotFound = errors.New("app installation not found")
	ErrAppDisabled          = errors.New("app installation is disabled")
)

// ValidateInstallationID validates an opaque exact installation address.
func ValidateInstallationID(value string) error {
	if !canonicalid.IsUUIDv7(value) {
		return ErrInstallationNotFound
	}
	return nil
}

type Descriptor struct {
	AppID                     string
	DisplayName               string
	WorkspaceOwnerAllowed     bool
	ParticipantOwnerAllowed   bool
	WorkspaceRoleCapabilities []WorkspaceRoleCapability
}

// WorkspaceRoleCapability is the app-owned vocabulary that may be attached to
// a Workspace role. Risk, tool route, approval, and domain commit semantics do
// not belong in this catalog projection.
type WorkspaceRoleCapability struct {
	Ref   string
	Label string
}

type Installation struct {
	InstallationID string
	Owner          OwnerRef
	AppID          string
	State          State
	AuthorityEpoch int64
	InstalledAt    time.Time
	UpdatedAt      time.Time
}
