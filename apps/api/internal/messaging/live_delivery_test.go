package messaging

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
)

func TestProductionHubRemovalAndFanoutHaveOneWorkspaceBoundary(t *testing.T) {
	t.Run("removal commits before fanout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newWorld(t, ctx)
		workspace, channel := w.workspaceWithChannel(t, ctx)
		ownerStore := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
		recipientStore := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
		if err := recipientStore.ReadThrough(ctx, channel.PlaceID, 0); err != nil {
			t.Fatalf("materialize recipient tenure: %v", err)
		}
		membershipID := activeMembershipID(t, ctx, w, workspace.WorkspaceID, w.agent)
		placeMemberID := activePlaceMembershipID(t, ctx, w, channel.PlaceID, w.agent)

		hub := NewHub(w.store.core)
		recipient := hub.subscribe(recipientStore)
		defer hub.unsubscribe(recipient)

		gate, err := w.store.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = gate.Rollback(context.Background()) }()
		var locked string
		if err := gate.QueryRow(ctx, `
			SELECT place_member_id FROM place_members
			WHERE place_member_id = $1 FOR UPDATE`, placeMemberID).Scan(&locked); err != nil {
			t.Fatalf("lock recipient tenure: %v", err)
		}

		removeDone := make(chan error, 1)
		go func() {
			removeDone <- w.workspaces.RemoveMember(
				ctx, workspace.WorkspaceID, membershipID, w.humanA,
			)
		}()
		waitForBlockedDatabaseSessions(t, ctx, w, 1)

		publishDone := make(chan error, 1)
		go func() {
			publishDone <- hub.PublishScoped(ctx, ownerStore, Event{
				Type: EventTyping, PlaceID: channel.PlaceID,
			})
		}()
		// Removal owns Workspace FOR UPDATE while fanout waits for one shared
		// current-audience lease. It cannot fall back to per-subscriber reads.
		waitForBlockedDatabaseSessions(t, ctx, w, 2)
		if err := gate.Commit(ctx); err != nil {
			t.Fatalf("release removal gate: %v", err)
		}
		if err := receiveError(t, removeDone, "removal before live fanout"); err != nil {
			t.Fatalf("remove recipient: %v", err)
		}
		if err := receiveError(t, publishDone, "live fanout after removal"); err != nil {
			t.Fatalf("publish after removal: %v", err)
		}
		if got := len(recipient.send); got != 0 {
			t.Fatalf("removed recipient received %d live frames", got)
		}
	})

	t.Run("fanout commits before removal", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newWorld(t, ctx)
		workspace, channel := w.workspaceWithChannel(t, ctx)
		ownerStore := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
		recipientStore := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
		if err := recipientStore.ReadThrough(ctx, channel.PlaceID, 0); err != nil {
			t.Fatalf("materialize recipient tenure: %v", err)
		}
		membershipID := activeMembershipID(t, ctx, w, workspace.WorkspaceID, w.agent)

		hub := NewHub(w.store.core)
		recipient := hub.subscribe(recipientStore)
		defer hub.unsubscribe(recipient)

		gate, err := w.store.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = gate.Rollback(context.Background()) }()
		var locked string
		if err := gate.QueryRow(ctx, `
			SELECT place_id FROM places WHERE place_id = $1 FOR UPDATE`,
			channel.PlaceID).Scan(&locked); err != nil {
			t.Fatalf("lock fanout place: %v", err)
		}

		publishDone := make(chan error, 1)
		go func() {
			publishDone <- hub.PublishScoped(ctx, ownerStore, Event{
				Type: EventTyping, PlaceID: channel.PlaceID,
			})
		}()
		waitForBlockedDatabaseSessions(t, ctx, w, 1)
		removeDone := make(chan error, 1)
		go func() {
			removeDone <- w.workspaces.RemoveMember(
				ctx, workspace.WorkspaceID, membershipID, w.humanA,
			)
		}()
		// Fanout owns Workspace FOR SHARE while waiting for the place. Removal
		// must wait; all subscribers are partitioned from the same audience.
		waitForBlockedDatabaseSessions(t, ctx, w, 2)
		if err := gate.Commit(ctx); err != nil {
			t.Fatalf("release fanout gate: %v", err)
		}
		if err := receiveError(t, publishDone, "live fanout before removal"); err != nil {
			t.Fatalf("publish before removal: %v", err)
		}
		if got := len(recipient.send); got != 1 {
			t.Fatalf("pre-removal recipient received %d frames, want one", got)
		}
		if err := receiveError(t, removeDone, "removal after live fanout"); err != nil {
			t.Fatalf("remove recipient: %v", err)
		}
	})
}

func TestProductionHubJoinAndFanoutHaveOneWorkspaceBoundary(t *testing.T) {
	makeDetachedSubscriber := func(
		t *testing.T,
		w world,
		ownerStore *ScopedStore,
		actor ParticipantRef,
	) *ScopedStore {
		t.Helper()
		store, err := w.store.core.Scoped(Scope{
			WorkspaceID:    ownerStore.Scope.WorkspaceID,
			InstallationID: ownerStore.Scope.InstallationID,
			Actor:          actor,
		})
		if err != nil {
			t.Fatalf("construct detached subscriber scope: %v", err)
		}
		return store
	}

	t.Run("join commits before fanout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newWorld(t, ctx)
		workspace, channel := w.workspaceWithChannel(t, ctx)
		ownerStore := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
		if err := w.store.removeWorkspaceMember(ctx, workspace.WorkspaceID, w.agent); err != nil {
			t.Fatalf("detach future recipient: %v", err)
		}
		invite, err := w.workspaces.CreateInvite(ctx, workspace.WorkspaceID, w.humanA)
		if err != nil {
			t.Fatalf("create join invite: %v", err)
		}
		hub := NewHub(w.store.core)
		recipient := hub.subscribe(makeDetachedSubscriber(t, w, ownerStore, w.agent))
		defer hub.unsubscribe(recipient)

		gate, err := w.store.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = gate.Rollback(context.Background()) }()
		var locked string
		if err := gate.QueryRow(ctx, `SELECT invite_id FROM workspace_invites
			WHERE invite_id = $1 FOR UPDATE`, invite.InviteID).Scan(&locked); err != nil {
			t.Fatal(err)
		}
		joinDone := make(chan error, 1)
		go func() {
			_, err := w.workspaces.RedeemInvite(ctx, invite.Code, w.agent)
			joinDone <- err
		}()
		waitForBlockedDatabaseSessions(t, ctx, w, 1)
		publishDone := make(chan error, 1)
		go func() {
			publishDone <- hub.PublishScoped(ctx, ownerStore, Event{
				Type: EventTyping, PlaceID: channel.PlaceID,
			})
		}()
		waitForBlockedDatabaseSessions(t, ctx, w, 2)
		if err := gate.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err := receiveError(t, joinDone, "join before live fanout"); err != nil {
			t.Fatal(err)
		}
		if err := receiveError(t, publishDone, "fanout after join"); err != nil {
			t.Fatal(err)
		}
		if got := len(recipient.send); got != 1 {
			t.Fatalf("newly joined recipient received %d frames, want one", got)
		}
	})

	t.Run("fanout commits before join", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newWorld(t, ctx)
		workspace, channel := w.workspaceWithChannel(t, ctx)
		ownerStore := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
		if err := w.store.removeWorkspaceMember(ctx, workspace.WorkspaceID, w.agent); err != nil {
			t.Fatalf("detach future recipient: %v", err)
		}
		invite, err := w.workspaces.CreateInvite(ctx, workspace.WorkspaceID, w.humanA)
		if err != nil {
			t.Fatalf("create join invite: %v", err)
		}
		hub := NewHub(w.store.core)
		recipient := hub.subscribe(makeDetachedSubscriber(t, w, ownerStore, w.agent))
		defer hub.unsubscribe(recipient)

		gate, err := w.store.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = gate.Rollback(context.Background()) }()
		var locked string
		if err := gate.QueryRow(ctx, `SELECT place_id FROM places
			WHERE place_id = $1 FOR UPDATE`, channel.PlaceID).Scan(&locked); err != nil {
			t.Fatal(err)
		}
		publishDone := make(chan error, 1)
		go func() {
			publishDone <- hub.PublishScoped(ctx, ownerStore, Event{
				Type: EventTyping, PlaceID: channel.PlaceID,
			})
		}()
		waitForBlockedDatabaseSessions(t, ctx, w, 1)
		joinDone := make(chan error, 1)
		go func() {
			_, err := w.workspaces.RedeemInvite(ctx, invite.Code, w.agent)
			joinDone <- err
		}()
		waitForBlockedDatabaseSessions(t, ctx, w, 2)
		if err := gate.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err := receiveError(t, publishDone, "fanout before join"); err != nil {
			t.Fatal(err)
		}
		if got := len(recipient.send); got != 0 {
			t.Fatalf("not-yet-joined recipient received %d frames", got)
		}
		if err := receiveError(t, joinDone, "join after live fanout"); err != nil {
			t.Fatal(err)
		}
	})
}

func TestProductionHubPartitionsSocketsByExactAppAddress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	first := newScopedContractFixture(t, ctx, w, "first", w.humanB)
	second := newScopedContractFixture(t, ctx, w, "second", w.humanB)
	firstStore := first.scope(t, w, w.humanA)
	channel, err := firstStore.CreateChannel(ctx, "general", "")
	if err != nil {
		t.Fatal(err)
	}

	hub := NewHub(w.store.core)
	wrongWorkspace := hub.subscribe(second.scope(t, w, w.humanB))
	defer hub.unsubscribe(wrongWorkspace)
	if err := hub.PublishScoped(ctx, firstStore, Event{
		Type: EventTyping, PlaceID: channel.PlaceID,
	}); err != nil {
		t.Fatal(err)
	}
	if got := len(wrongWorkspace.send); got != 0 {
		t.Fatalf("same participant's other Workspace received %d frames", got)
	}
}

func TestLiveAuthorityLeaseRemovalHasBothCommitOrders(t *testing.T) {
	t.Run("removal first rejects socket effect", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newWorld(t, ctx)
		workspace, channel := w.workspaceWithChannel(t, ctx)
		store := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
		if err := store.ReadThrough(ctx, channel.PlaceID, 0); err != nil {
			t.Fatalf("materialize actor tenure: %v", err)
		}
		membershipID := activeMembershipID(t, ctx, w, workspace.WorkspaceID, w.agent)
		placeMemberID := activePlaceMembershipID(t, ctx, w, channel.PlaceID, w.agent)

		gate, err := w.store.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = gate.Rollback(context.Background()) }()
		var locked string
		if err := gate.QueryRow(ctx, `SELECT place_member_id FROM place_members
			WHERE place_member_id = $1 FOR UPDATE`, placeMemberID).Scan(&locked); err != nil {
			t.Fatal(err)
		}
		removeDone := make(chan error, 1)
		go func() {
			removeDone <- w.workspaces.RemoveMember(
				ctx, workspace.WorkspaceID, membershipID, w.humanA,
			)
		}()
		waitForBlockedDatabaseSessions(t, ctx, w, 1)
		effectCalled := make(chan struct{}, 1)
		leaseDone := make(chan error, 1)
		go func() {
			leaseDone <- store.withLiveAuthorityLease(
				ctx,
				liveBoundary{placeID: channel.PlaceID},
				func() error { effectCalled <- struct{}{}; return nil },
			)
		}()
		waitForBlockedDatabaseSessions(t, ctx, w, 2)
		if err := gate.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err := receiveError(t, removeDone, "remove before socket effect"); err != nil {
			t.Fatal(err)
		}
		if err := receiveError(t, leaseDone, "socket effect after removal"); !errors.Is(err, ErrPlaceNotFound) {
			t.Fatalf("socket effect error = %v, want ErrPlaceNotFound", err)
		}
		select {
		case <-effectCalled:
			t.Fatal("revoked socket effect ran")
		default:
		}
	})

	t.Run("socket effect first delays removal", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newWorld(t, ctx)
		workspace, channel := w.workspaceWithChannel(t, ctx)
		store := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
		if err := store.ReadThrough(ctx, channel.PlaceID, 0); err != nil {
			t.Fatalf("materialize actor tenure: %v", err)
		}
		membershipID := activeMembershipID(t, ctx, w, workspace.WorkspaceID, w.agent)
		effectEntered := make(chan struct{})
		releaseEffect := make(chan struct{})
		leaseDone := make(chan error, 1)
		go func() {
			leaseDone <- store.withLiveAuthorityLease(
				ctx,
				liveBoundary{placeID: channel.PlaceID},
				func() error {
					close(effectEntered)
					<-releaseEffect
					return nil
				},
			)
		}()
		select {
		case <-effectEntered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		removeDone := make(chan error, 1)
		go func() {
			removeDone <- w.workspaces.RemoveMember(
				ctx, workspace.WorkspaceID, membershipID, w.humanA,
			)
		}()
		waitForBlockedDatabaseSessions(t, ctx, w, 1)
		close(releaseEffect)
		if err := receiveError(t, leaseDone, "socket effect before removal"); err != nil {
			t.Fatal(err)
		}
		if err := receiveError(t, removeDone, "remove after socket effect"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("disable first rejects socket effect", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newWorld(t, ctx)
		workspace, channel := w.workspaceWithChannel(t, ctx)
		store := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
		gate, err := w.store.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = gate.Rollback(context.Background()) }()
		var locked string
		if err := gate.QueryRow(ctx, `SELECT installation_id FROM app_installations
			WHERE installation_id = $1 FOR UPDATE`, store.Scope.InstallationID).Scan(&locked); err != nil {
			t.Fatal(err)
		}
		disableDone := make(chan error, 1)
		go func() {
			_, err := w.apps.SetEnabled(
				ctx,
				applicationapps.WorkspaceOwner(workspace.WorkspaceID),
				w.humanA,
				MessagingAppID,
				false,
			)
			disableDone <- err
		}()
		waitForBlockedDatabaseSessions(t, ctx, w, 1)
		effectCalled := make(chan struct{}, 1)
		leaseDone := make(chan error, 1)
		go func() {
			leaseDone <- store.withLiveAuthorityLease(
				ctx,
				liveBoundary{placeID: channel.PlaceID},
				func() error { effectCalled <- struct{}{}; return nil },
			)
		}()
		waitForBlockedDatabaseSessions(t, ctx, w, 2)
		if err := gate.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err := receiveError(t, disableDone, "disable before socket effect"); err != nil {
			t.Fatal(err)
		}
		if err := receiveError(t, leaseDone, "socket effect after disable"); !errors.Is(err, applicationapps.ErrAppDisabled) {
			t.Fatalf("socket effect error = %v, want ErrAppDisabled", err)
		}
		select {
		case <-effectCalled:
			t.Fatal("disabled installation socket effect ran")
		default:
		}
	})

	t.Run("socket effect first delays disable", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newWorld(t, ctx)
		workspace, channel := w.workspaceWithChannel(t, ctx)
		store := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
		effectEntered := make(chan struct{})
		releaseEffect := make(chan struct{})
		leaseDone := make(chan error, 1)
		go func() {
			leaseDone <- store.withLiveAuthorityLease(
				ctx,
				liveBoundary{placeID: channel.PlaceID},
				func() error {
					close(effectEntered)
					<-releaseEffect
					return nil
				},
			)
		}()
		select {
		case <-effectEntered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		disableDone := make(chan error, 1)
		go func() {
			_, err := w.apps.SetEnabled(
				ctx,
				applicationapps.WorkspaceOwner(workspace.WorkspaceID),
				w.humanA,
				MessagingAppID,
				false,
			)
			disableDone <- err
		}()
		waitForBlockedDatabaseSessions(t, ctx, w, 1)
		close(releaseEffect)
		if err := receiveError(t, leaseDone, "socket effect before disable"); err != nil {
			t.Fatal(err)
		}
		if err := receiveError(t, disableDone, "disable after socket effect"); err != nil {
			t.Fatal(err)
		}
	})
}

// controlledSessionAdmission models the same shared-admission/exclusive-
// logout ordering as the durable browser-session gateway while exposing a
// deterministic test rendezvous around WSServer.handleTyping.
type controlledSessionAdmission struct {
	mu      sync.RWMutex
	revoked bool
}

func (s *controlledSessionAdmission) VerifySession(
	context.Context,
	string,
) (agentevents.UserSessionClaims, error) {
	return agentevents.UserSessionClaims{}, nil
}

func (s *controlledSessionAdmission) AuthorizeSession(
	ctx context.Context,
	_ agentevents.UserSessionClaims,
	op func() error,
) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if s.revoked {
		return errors.New("session revoked")
	}
	return op()
}

func (s *controlledSessionAdmission) revoke(effect func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.revoked = true
	if effect != nil {
		effect()
	}
}

func TestTypingHoldsSessionAndWorkspaceAuthorityThroughPublish(t *testing.T) {
	t.Run("removal first suppresses typing", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newWorld(t, ctx)
		workspace, channel := w.workspaceWithChannel(t, ctx)
		actorStore := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
		if err := actorStore.ReadThrough(ctx, channel.PlaceID, 0); err != nil {
			t.Fatalf("materialize actor tenure: %v", err)
		}
		membershipID := activeMembershipID(t, ctx, w, workspace.WorkspaceID, w.agent)
		placeMemberID := activePlaceMembershipID(t, ctx, w, channel.PlaceID, w.agent)
		hub := NewHub(w.store.core)
		receiver := hub.subscribe(w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB))
		defer hub.unsubscribe(receiver)
		server := NewWSServer(w.store.core, &controlledSessionAdmission{}, hub)

		gate, err := w.store.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = gate.Rollback(context.Background()) }()
		var locked string
		if err := gate.QueryRow(ctx, `SELECT place_member_id FROM place_members
			WHERE place_member_id = $1 FOR UPDATE`, placeMemberID).Scan(&locked); err != nil {
			t.Fatal(err)
		}
		removeDone := make(chan error, 1)
		go func() {
			removeDone <- w.workspaces.RemoveMember(
				ctx, workspace.WorkspaceID, membershipID, w.humanA,
			)
		}()
		waitForBlockedDatabaseSessions(t, ctx, w, 1)
		typingDone := make(chan struct{})
		go func() {
			server.handleTyping(ctx, &subscriber{viewer: w.agent, store: actorStore},
				agentevents.UserSessionClaims{}, wsClientFrame{PlaceID: channel.PlaceID})
			close(typingDone)
		}()
		waitForBlockedDatabaseSessions(t, ctx, w, 2)
		if err := gate.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err := receiveError(t, removeDone, "removal before typing"); err != nil {
			t.Fatal(err)
		}
		select {
		case <-typingDone:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if got := len(receiver.send); got != 0 {
			t.Fatalf("revoked actor emitted %d typing frames", got)
		}
	})

	t.Run("typing first delays removal", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newWorld(t, ctx)
		workspace, channel := w.workspaceWithChannel(t, ctx)
		actorStore := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
		if err := actorStore.ReadThrough(ctx, channel.PlaceID, 0); err != nil {
			t.Fatalf("materialize actor tenure: %v", err)
		}
		membershipID := activeMembershipID(t, ctx, w, workspace.WorkspaceID, w.agent)
		hub := NewHub(w.store.core)
		receiver := hub.subscribe(w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB))
		defer hub.unsubscribe(receiver)
		server := NewWSServer(w.store.core, &controlledSessionAdmission{}, hub)

		gate, err := w.store.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = gate.Rollback(context.Background()) }()
		var locked string
		if err := gate.QueryRow(ctx, `SELECT place_id FROM places
			WHERE place_id = $1 FOR UPDATE`, channel.PlaceID).Scan(&locked); err != nil {
			t.Fatal(err)
		}
		typingDone := make(chan struct{})
		go func() {
			server.handleTyping(ctx, &subscriber{viewer: w.agent, store: actorStore},
				agentevents.UserSessionClaims{}, wsClientFrame{PlaceID: channel.PlaceID})
			close(typingDone)
		}()
		waitForBlockedDatabaseSessions(t, ctx, w, 1)
		removeDone := make(chan error, 1)
		go func() {
			removeDone <- w.workspaces.RemoveMember(
				ctx, workspace.WorkspaceID, membershipID, w.humanA,
			)
		}()
		waitForBlockedDatabaseSessions(t, ctx, w, 2)
		if err := gate.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		select {
		case <-typingDone:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if got := len(receiver.send); got != 1 {
			t.Fatalf("pre-removal typing delivered %d frames, want one", got)
		}
		if err := receiveError(t, removeDone, "removal after typing"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("logout first suppresses typing", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newWorld(t, ctx)
		workspace, channel := w.workspaceWithChannel(t, ctx)
		store := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
		hub := NewHub(w.store.core)
		receiver := hub.subscribe(w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB))
		defer hub.unsubscribe(receiver)
		sessions := &controlledSessionAdmission{}
		sessions.revoke(nil)
		server := NewWSServer(w.store.core, sessions, hub)
		server.handleTyping(ctx, &subscriber{viewer: w.humanA, store: store},
			agentevents.UserSessionClaims{}, wsClientFrame{PlaceID: channel.PlaceID})
		if got := len(receiver.send); got != 0 {
			t.Fatalf("post-logout typing delivered %d frames", got)
		}
	})

	t.Run("typing first delays logout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newWorld(t, ctx)
		workspace, channel := w.workspaceWithChannel(t, ctx)
		store := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
		hub := NewHub(w.store.core)
		receiver := hub.subscribe(w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB))
		defer hub.unsubscribe(receiver)
		sessions := &controlledSessionAdmission{}
		server := NewWSServer(w.store.core, sessions, hub)

		gate, err := w.store.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = gate.Rollback(context.Background()) }()
		var locked string
		if err := gate.QueryRow(ctx, `SELECT place_id FROM places
			WHERE place_id = $1 FOR UPDATE`, channel.PlaceID).Scan(&locked); err != nil {
			t.Fatal(err)
		}
		typingDone := make(chan struct{})
		go func() {
			server.handleTyping(ctx, &subscriber{viewer: w.humanA, store: store},
				agentevents.UserSessionClaims{}, wsClientFrame{PlaceID: channel.PlaceID})
			close(typingDone)
		}()
		waitForBlockedDatabaseSessions(t, ctx, w, 1)
		logoutStarted := make(chan struct{})
		logoutDone := make(chan struct{})
		go func() {
			close(logoutStarted)
			sessions.revoke(nil)
			close(logoutDone)
		}()
		<-logoutStarted
		select {
		case <-logoutDone:
			t.Fatal("logout crossed in-flight typing admission")
		default:
		}
		if err := gate.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		select {
		case <-typingDone:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		select {
		case <-logoutDone:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if got := len(receiver.send); got != 1 {
			t.Fatalf("pre-logout typing delivered %d frames, want one", got)
		}
	})
}
