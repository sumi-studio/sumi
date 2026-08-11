package messaging

import (
	"context"
	"errors"
	"fmt"

	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
	workspacecontrol "github.com/sumi-studio/sumi/apps/api/internal/workspace"
)

// withLiveAudience projects one already-committed event to one immutable
// current audience. It deliberately does not require the originating actor to
// remain a member: this is delivery of durable application truth, not a new
// actor operation. The exact Workspace-owned installation and the event's
// Workspace/place boundary are nevertheless fenced through the in-memory
// partition and enqueue. If removal/disable commits first, its new audience
// wins; if delivery acquires the shared lease first, the whole old audience
// snapshot is enqueued before the mutation can commit. Reconnect/cursor replay
// remains the durable recovery path when best-effort delivery loses.
func (s *Store) withLiveAudience(
	ctx context.Context,
	scope Scope,
	boundary liveBoundary,
	requireActor bool,
	deliver func(map[ParticipantRef]struct{}) error,
) error {
	if s == nil || s.workspaces == nil || s.apps == nil || deliver == nil {
		return ErrInvalidScope
	}
	if err := scope.Validate(); err != nil {
		return err
	}
	if boundary.key() == "" {
		return ErrInvalidScope
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin live audience snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	scoped := &ScopedStore{Store: s, Scope: scope}
	if requireActor {
		if _, err := scoped.authorizeMutationInTx(ctx, tx); err != nil {
			return err
		}
	} else {
		if err := s.workspaces.LockSharedInTx(ctx, tx, scope.WorkspaceID); err != nil {
			if errors.Is(err, workspacecontrol.ErrNotFound) {
				return ErrPlaceNotFound
			}
			return err
		}
		if _, err := s.apps.RequireEnabledInstallationInTx(
			ctx,
			tx,
			scope.InstallationID,
			applicationapps.WorkspaceOwner(scope.WorkspaceID),
			MessagingAppID,
		); err != nil {
			return err
		}
	}

	audience := map[ParticipantRef]struct{}{}
	if boundary.placeID != "" {
		place, err := scoped.lockScopedPlace(ctx, tx, boundary.placeID)
		if err != nil {
			return err
		}
		if requireActor {
			if _, err := scoped.placeAccessAfterAuthorization(
				ctx, tx, place, scope.Actor,
			); err != nil {
				return err
			}
		}
		members, err := scoped.activeMembersScoped(ctx, tx, place)
		if err != nil {
			return err
		}
		for _, member := range members {
			audience[member.Participant] = struct{}{}
		}
	} else {
		if !boundary.subjectSet {
			return ErrInvalidScope
		}
		if err := boundary.subject.Validate(); err != nil {
			return ErrInvalidScope
		}
		if _, err := s.workspaces.ActiveMembershipInTx(
			ctx, tx, scope.WorkspaceID, boundary.subject,
		); err != nil {
			return ErrPlaceNotFound
		}
		memberships, err := s.workspaces.ActiveMembershipsInTx(ctx, tx, scope.WorkspaceID)
		if err != nil {
			return err
		}
		for _, membership := range memberships {
			audience[membership.Participant] = struct{}{}
		}
	}
	if err := deliver(audience); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit live audience snapshot: %w", err)
	}
	return nil
}
