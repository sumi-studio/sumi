package messaging

import (
	"context"
	"errors"
	"fmt"

	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
	workspacecontrol "github.com/sumi-studio/sumi/apps/api/internal/workspace"
)

// liveAudience is one immutable current audience for one event boundary.
// Members hold the place itself and always receive the event. Watchers may
// only read it — a Workspace member who opens a thread they never joined —
// and receive it exclusively while that exact place is the one their
// connection has open. Reading therefore never turns a place's traffic into
// the reader's ambient ledger, which is what participation is for.
type liveAudience struct {
	members  map[ParticipantRef]struct{}
	watchers map[ParticipantRef]struct{}
}

// admits answers whether this connection may receive the event now. watching
// is the connection's own open-place declaration; it can widen delivery only
// to a participant the fenced audience already listed as a watcher.
func (a liveAudience) admits(participant ParticipantRef, watching bool) bool {
	if _, member := a.members[participant]; member {
		return true
	}
	if !watching {
		return false
	}
	_, watcher := a.watchers[participant]
	return watcher
}

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
	deliver func(liveAudience) error,
) error {
	if s == nil || s.workspaces == nil || s.apps == nil || deliver == nil {
		return ErrInvalidScope
	}
	if requireActor {
		if err := scope.Validate(); err != nil {
			return err
		}
	} else if err := scope.validateAddress(); err != nil {
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
		if _, err := s.apps.RequireEnabledInstallationEpochInTx(
			ctx,
			tx,
			scope.InstallationID,
			scope.AuthorityEpoch,
			applicationapps.WorkspaceOwner(scope.WorkspaceID),
			MessagingAppID,
		); err != nil {
			return err
		}
	}

	audience := liveAudience{
		members:  map[ParticipantRef]struct{}{},
		watchers: map[ParticipantRef]struct{}{},
	}
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
		if place.Kind == PlaceThread {
			// A thread is projected by participation, not by Workspace
			// visibility. Everyone who may open it can watch it while it is
			// open; only its participants carry it when it is not.
			joined, err := scoped.threadParticipants(ctx, tx, []string{place.PlaceID})
			if err != nil {
				return err
			}
			for _, participant := range joined[place.PlaceID] {
				audience.members[participant] = struct{}{}
			}
			for _, member := range members {
				if _, joined := audience.members[member.Participant]; !joined {
					audience.watchers[member.Participant] = struct{}{}
				}
			}
		} else {
			for _, member := range members {
				audience.members[member.Participant] = struct{}{}
			}
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
			audience.members[membership.Participant] = struct{}{}
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
