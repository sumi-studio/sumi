package messaging

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
	workspacecontrol "github.com/sumi-studio/sumi/apps/api/internal/workspace"
)

const (
	MessagingAppID           = "messaging"
	ManageChannelsCapability = "app.messaging.manage_channels"
)

var ErrInvalidScope = errors.New("invalid messaging scope")

// Scope is the exact application address required by every Messaging entry
// surface. Actor always comes from transport authentication; the two opaque
// IDs select one Workspace-owned Messaging installation.
type Scope struct {
	WorkspaceID    string
	InstallationID string
	Actor          participant.Ref
}

func (s Scope) Validate() error {
	if err := s.Actor.Validate(); err != nil {
		return fmt.Errorf("%w: actor: %v", ErrInvalidScope, err)
	}
	if err := applicationapps.WorkspaceOwner(s.WorkspaceID).Validate(); err != nil {
		return fmt.Errorf("%w: workspace_id", ErrInvalidScope)
	}
	if err := applicationapps.ValidateInstallationID(s.InstallationID); err != nil {
		return fmt.Errorf("%w: installation_id", ErrInvalidScope)
	}
	return nil
}

type WorkspaceAuthority interface {
	WorkspaceFor(context.Context, string, participant.Ref) (workspacecontrol.Workspace, error)
	Members(context.Context, string, participant.Ref) ([]workspacecontrol.Membership, error)
	ActiveMembershipInTx(context.Context, pgx.Tx, string, participant.Ref) (workspacecontrol.Membership, error)
	ActiveMembershipsInTx(context.Context, pgx.Tx, string) ([]workspacecontrol.Membership, error)
	LockAndRequireAppCapability(context.Context, pgx.Tx, string, participant.Ref, string) error
	RequireMembership(context.Context, string, participant.Ref) error
}

type AppAuthority interface {
	RequireEnabledInstallation(context.Context, string, applicationapps.OwnerRef, string) (applicationapps.Installation, error)
	RequireEnabledInstallationInTx(context.Context, pgx.Tx, string, applicationapps.OwnerRef, string) (applicationapps.Installation, error)
	RequireEnabledInstallationInSnapshot(context.Context, pgx.Tx, string, applicationapps.OwnerRef, string) (applicationapps.Installation, error)
}

// ScopedStore is one immutable view of one installed Messaging app. Keeping
// Actor on the receiver prevents a payload from selecting a different author.
type ScopedStore struct {
	*Store
	Scope Scope
}

func (s *Store) Scoped(scope Scope) (*ScopedStore, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	return &ScopedStore{Store: s, Scope: scope}, nil
}

func (s *ScopedStore) authorize(ctx context.Context) error {
	return s.Store.authorizeScope(ctx, s.Scope)
}

func (s *ScopedStore) authorizeInTx(ctx context.Context, tx pgx.Tx) (workspacecontrol.Membership, error) {
	return s.Store.authorizeScopeInTx(ctx, tx, s.Scope)
}

func (s *ScopedStore) authorizeSnapshotInTx(ctx context.Context, tx pgx.Tx) (workspacecontrol.Membership, error) {
	return s.Store.authorizeScopeSnapshotInTx(ctx, tx, s.Scope)
}

func (s *ScopedStore) authorizeManageChannelsInTx(ctx context.Context, tx pgx.Tx) (workspacecontrol.Membership, error) {
	if err := s.Scope.Validate(); err != nil {
		return workspacecontrol.Membership{}, err
	}
	if s.workspaces == nil || s.apps == nil {
		return workspacecontrol.Membership{}, errors.New("messaging authority dependencies are unavailable")
	}
	if err := s.workspaces.LockAndRequireAppCapability(
		ctx, tx, s.Scope.WorkspaceID, s.Scope.Actor, ManageChannelsCapability,
	); err != nil {
		switch {
		case errors.Is(err, workspacecontrol.ErrNotFound):
			return workspacecontrol.Membership{}, ErrPlaceNotFound
		case errors.Is(err, workspacecontrol.ErrForbidden):
			return workspacecontrol.Membership{}, ErrForbidden
		default:
			return workspacecontrol.Membership{}, err
		}
	}
	if _, err := s.apps.RequireEnabledInstallationInTx(
		ctx, tx, s.Scope.InstallationID,
		applicationapps.WorkspaceOwner(s.Scope.WorkspaceID), MessagingAppID,
	); err != nil {
		return workspacecontrol.Membership{}, err
	}
	membership, err := s.workspaces.ActiveMembershipInTx(ctx, tx, s.Scope.WorkspaceID, s.Scope.Actor)
	if err != nil {
		return workspacecontrol.Membership{}, ErrPlaceNotFound
	}
	return membership, nil
}

func (s *Store) authorizeScope(ctx context.Context, scope Scope) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if s.workspaces == nil || s.apps == nil {
		return errors.New("messaging authority dependencies are unavailable")
	}
	if err := s.workspaces.RequireMembership(ctx, scope.WorkspaceID, scope.Actor); err != nil {
		return ErrPlaceNotFound
	}
	_, err := s.apps.RequireEnabledInstallation(
		ctx, scope.InstallationID,
		applicationapps.WorkspaceOwner(scope.WorkspaceID), MessagingAppID,
	)
	return err
}

func (s *Store) authorizeScopeInTx(ctx context.Context, tx pgx.Tx, scope Scope) (workspacecontrol.Membership, error) {
	if err := scope.Validate(); err != nil {
		return workspacecontrol.Membership{}, err
	}
	if s.workspaces == nil || s.apps == nil {
		return workspacecontrol.Membership{}, errors.New("messaging authority dependencies are unavailable")
	}
	if _, err := s.apps.RequireEnabledInstallationInTx(
		ctx, tx, scope.InstallationID,
		applicationapps.WorkspaceOwner(scope.WorkspaceID), MessagingAppID,
	); err != nil {
		return workspacecontrol.Membership{}, err
	}
	membership, err := s.workspaces.ActiveMembershipInTx(ctx, tx, scope.WorkspaceID, scope.Actor)
	if err != nil {
		return workspacecontrol.Membership{}, ErrPlaceNotFound
	}
	return membership, nil
}

func (s *Store) authorizeScopeSnapshotInTx(ctx context.Context, tx pgx.Tx, scope Scope) (workspacecontrol.Membership, error) {
	if err := scope.Validate(); err != nil {
		return workspacecontrol.Membership{}, err
	}
	if s.workspaces == nil || s.apps == nil {
		return workspacecontrol.Membership{}, errors.New("messaging authority dependencies are unavailable")
	}
	if _, err := s.apps.RequireEnabledInstallationInSnapshot(
		ctx, tx, scope.InstallationID,
		applicationapps.WorkspaceOwner(scope.WorkspaceID), MessagingAppID,
	); err != nil {
		return workspacecontrol.Membership{}, err
	}
	membership, err := s.workspaces.ActiveMembershipInTx(ctx, tx, scope.WorkspaceID, scope.Actor)
	if err != nil {
		return workspacecontrol.Membership{}, ErrPlaceNotFound
	}
	return membership, nil
}
