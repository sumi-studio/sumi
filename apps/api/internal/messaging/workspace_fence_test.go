package messaging

import (
	"context"
	"errors"
	"testing"
	"time"

	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
)

type appendOutcome struct {
	message Message
	created bool
	err     error
}

type placeOutcome struct {
	place Place
	err   error
}

func TestManageChannelsLockOrderIsWorkspaceThenInstallationThenAppRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, _ := w.workspaceWithChannel(t, ctx)
	store := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)

	installationGate, err := w.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = installationGate.Rollback(context.Background()) }()
	var lockedInstallation string
	if err := installationGate.QueryRow(ctx, `
		SELECT installation_id FROM app_installations
		WHERE installation_id = $1 FOR UPDATE`, store.Scope.InstallationID,
	).Scan(&lockedInstallation); err != nil {
		t.Fatal(err)
	}

	createDone := make(chan placeOutcome, 1)
	go func() {
		place, err := store.CreateChannel(ctx, "ordered", "", false)
		createDone <- placeOutcome{place: place, err: err}
	}()
	// Channel management must already hold Workspace FOR SHARE when it reaches
	// the exact installation row and blocks behind this gate.
	waitForBlockedDatabaseSessions(t, ctx, w, 1)

	disableDone := make(chan error, 1)
	go func() {
		_, err := w.apps.SetEnabled(
			ctx, applicationapps.WorkspaceOwner(workspace.WorkspaceID),
			w.humanA, MessagingAppID, false,
		)
		disableDone <- err
	}()
	// The lifecycle mutation must wait at Workspace FOR UPDATE rather than
	// overtaking the channel mutation and contending on the installation row.
	waitForBlockedDatabaseSessions(t, ctx, w, 2)

	if err := installationGate.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case outcome := <-createDone:
		if outcome.err != nil || outcome.place.Name != "ordered" {
			t.Fatalf("channel management outcome = %#v, %v", outcome.place, outcome.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("channel management did not complete")
	}
	if err := receiveError(t, disableDone, "disable after channel management"); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceFenceRemovalCommitsBeforeRevokedActorMutation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	actorStore := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)

	// Materialize the channel tenure so removal can be paused after taking the
	// Workspace-exclusive fence but before it closes the exact child tenure.
	if err := actorStore.ReadThrough(ctx, channel.PlaceID, 0); err != nil {
		t.Fatalf("admit actor channel tenure: %v", err)
	}
	membershipID := activeMembershipID(t, ctx, w, workspace.WorkspaceID, w.agent)
	placeMemberID := activePlaceMembershipID(t, ctx, w, channel.PlaceID, w.agent)

	gate, err := w.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = gate.Rollback(context.Background()) }()
	var locked string
	if err := gate.QueryRow(ctx, `
		SELECT place_member_id FROM place_members
		WHERE place_member_id = $1 FOR UPDATE`, placeMemberID).Scan(&locked); err != nil {
		t.Fatalf("lock actor place tenure: %v", err)
	}

	removeDone := make(chan error, 1)
	go func() {
		removeDone <- w.workspaces.RemoveMember(
			ctx, workspace.WorkspaceID, membershipID, w.humanA,
		)
	}()
	waitForBlockedDatabaseSessions(t, ctx, w, 1)

	appendDone := make(chan appendOutcome, 1)
	go func() {
		message, created, err := actorStore.AppendMessage(ctx, AppendInput{
			PlaceID: channel.PlaceID, Content: "must not survive removal",
			ClientNonce: "removal-before-actor-mutation",
		})
		appendDone <- appendOutcome{message: message, created: created, err: err}
	}()
	// Removal is waiting on the child tenure; append is waiting on the shared
	// Workspace fence. This proves both operations reached the intended order.
	waitForBlockedDatabaseSessions(t, ctx, w, 2)

	if err := gate.Commit(ctx); err != nil {
		t.Fatalf("release actor-tenure gate: %v", err)
	}
	if err := receiveError(t, removeDone, "member removal"); err != nil {
		t.Fatalf("member removal: %v", err)
	}
	outcome := receiveAppend(t, appendDone, "revoked actor append")
	if !errors.Is(outcome.err, ErrPlaceNotFound) || outcome.created {
		t.Fatalf("revoked actor append = created %v, err %v; want ErrPlaceNotFound",
			outcome.created, outcome.err)
	}
	var persisted int
	if err := w.store.pool.QueryRow(ctx, `
		SELECT count(*) FROM messages
		WHERE workspace_id = $1 AND client_nonce = $2`,
		workspace.WorkspaceID, "removal-before-actor-mutation").Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if persisted != 0 {
		t.Fatalf("revoked actor persisted %d messages", persisted)
	}
}

func TestWorkspaceFenceRemovalPrecedesNotificationAudienceSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	ownerStore := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	agentStore := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)

	if _, err := agentStore.SetNotificationSetting(ctx, NotifyLevelAll, nil, nil); err != nil {
		t.Fatalf("set recipient notifications: %v", err)
	}
	if err := agentStore.ReadThrough(ctx, channel.PlaceID, 0); err != nil {
		t.Fatalf("admit recipient channel tenure: %v", err)
	}
	membershipID := activeMembershipID(t, ctx, w, workspace.WorkspaceID, w.agent)
	placeMemberID := activePlaceMembershipID(t, ctx, w, channel.PlaceID, w.agent)

	gate, err := w.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = gate.Rollback(context.Background()) }()
	var locked string
	if err := gate.QueryRow(ctx, `
		SELECT place_member_id FROM place_members
		WHERE place_member_id = $1 FOR UPDATE`, placeMemberID).Scan(&locked); err != nil {
		t.Fatalf("lock recipient place tenure: %v", err)
	}

	removeDone := make(chan error, 1)
	go func() {
		removeDone <- w.workspaces.RemoveMember(
			ctx, workspace.WorkspaceID, membershipID, w.humanA,
		)
	}()
	waitForBlockedDatabaseSessions(t, ctx, w, 1)

	appendDone := make(chan appendOutcome, 1)
	go func() {
		message, created, err := ownerStore.AppendMessage(ctx, AppendInput{
			PlaceID: channel.PlaceID, Content: "audience after removal",
			ClientNonce: "removal-before-audience-snapshot",
		})
		appendDone <- appendOutcome{message: message, created: created, err: err}
	}()
	waitForBlockedDatabaseSessions(t, ctx, w, 2)

	if err := gate.Commit(ctx); err != nil {
		t.Fatalf("release recipient-tenure gate: %v", err)
	}
	if err := receiveError(t, removeDone, "recipient removal"); err != nil {
		t.Fatalf("recipient removal: %v", err)
	}
	outcome := receiveAppend(t, appendDone, "append after recipient removal")
	if outcome.err != nil || !outcome.created {
		t.Fatalf("append after recipient removal = created %v, err %v",
			outcome.created, outcome.err)
	}
	intents, err := ownerStore.NotificationIntentsForMessage(ctx, outcome.message.MessageID)
	if err != nil {
		t.Fatalf("load notification intents: %v", err)
	}
	if containsRecipient(intents, w.agent) {
		t.Fatalf("removed recipient remained in committed intent snapshot: %+v", intents)
	}
}

func TestWorkspaceFenceMutationMayCommitBeforeRemoval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	actorStore := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
	if err := actorStore.ReadThrough(ctx, channel.PlaceID, 0); err != nil {
		t.Fatalf("admit actor channel tenure: %v", err)
	}
	membershipID := activeMembershipID(t, ctx, w, workspace.WorkspaceID, w.agent)
	placeMemberID := activePlaceMembershipID(t, ctx, w, channel.PlaceID, w.agent)

	placeGate, err := w.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = placeGate.Rollback(context.Background()) }()
	var lockedPlace string
	if err := placeGate.QueryRow(ctx, `
		SELECT place_id FROM places WHERE place_id = $1 FOR UPDATE`,
		channel.PlaceID).Scan(&lockedPlace); err != nil {
		t.Fatalf("lock append place: %v", err)
	}

	// A second gate keeps removal from completing after append releases the
	// shared Workspace fence. This lets the test observe the valid
	// append-before-removal order without scheduler timing assumptions.
	tenureGate, err := w.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tenureGate.Rollback(context.Background()) }()
	var lockedTenure string
	if err := tenureGate.QueryRow(ctx, `
		SELECT place_member_id FROM place_members
		WHERE place_member_id = $1 FOR UPDATE`, placeMemberID).Scan(&lockedTenure); err != nil {
		t.Fatalf("lock actor tenure: %v", err)
	}

	appendDone := make(chan appendOutcome, 1)
	go func() {
		message, created, err := actorStore.AppendMessage(ctx, AppendInput{
			PlaceID: channel.PlaceID, Content: "commit before removal",
			ClientNonce: "actor-mutation-before-removal",
		})
		appendDone <- appendOutcome{message: message, created: created, err: err}
	}()
	waitForBlockedDatabaseSessions(t, ctx, w, 1)

	removeDone := make(chan error, 1)
	go func() {
		removeDone <- w.workspaces.RemoveMember(
			ctx, workspace.WorkspaceID, membershipID, w.humanA,
		)
	}()
	// Append holds Workspace FOR SHARE while waiting on the place. Removal must
	// therefore wait at Workspace FOR UPDATE rather than revoke it mid-commit.
	waitForBlockedDatabaseSessions(t, ctx, w, 2)

	if err := placeGate.Commit(ctx); err != nil {
		t.Fatalf("release append-place gate: %v", err)
	}
	outcome := receiveAppend(t, appendDone, "append before removal")
	if outcome.err != nil || !outcome.created {
		t.Fatalf("append before removal = created %v, err %v",
			outcome.created, outcome.err)
	}
	waitForBlockedDatabaseSessions(t, ctx, w, 1)
	select {
	case err := <-removeDone:
		t.Fatalf("removal bypassed exact child-tenure gate: %v", err)
	default:
	}
	if err := tenureGate.Commit(ctx); err != nil {
		t.Fatalf("release actor-tenure gate: %v", err)
	}
	if err := receiveError(t, removeDone, "removal after append"); err != nil {
		t.Fatalf("removal after append: %v", err)
	}

	var authorKind, authorID string
	if err := w.store.pool.QueryRow(ctx, `
		SELECT author_kind, author_id FROM messages
		WHERE workspace_id = $1 AND message_id = $2`,
		workspace.WorkspaceID, outcome.message.MessageID).Scan(&authorKind, &authorID); err != nil {
		t.Fatalf("load committed pre-removal message: %v", err)
	}
	if (ParticipantRef{Kind: ParticipantKind(authorKind), ID: authorID}) != w.agent {
		t.Fatalf("pre-removal message author = %s:%s, want %s",
			authorKind, authorID, w.agent.Key())
	}
	if _, _, err := actorStore.AppendMessage(ctx, AppendInput{
		PlaceID: channel.PlaceID, Content: "after removal",
		ClientNonce: "actor-mutation-after-removal",
	}); !errors.Is(err, ErrPlaceNotFound) {
		t.Fatalf("post-removal append error = %v, want ErrPlaceNotFound", err)
	}
}

func TestWorkspaceFenceJoinPrecedesNotificationAudienceSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	ownerStore := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	agentStore := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
	if _, err := agentStore.SetNotificationSetting(ctx, NotifyLevelAll, nil, nil); err != nil {
		t.Fatalf("set future recipient notifications: %v", err)
	}
	if err := w.store.removeWorkspaceMember(ctx, workspace.WorkspaceID, w.agent); err != nil {
		t.Fatalf("remove future recipient: %v", err)
	}
	invite, err := w.workspaces.CreateInvite(ctx, workspace.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatalf("create rejoin invite: %v", err)
	}

	gate, err := w.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = gate.Rollback(context.Background()) }()
	var locked string
	if err := gate.QueryRow(ctx, `
		SELECT invite_id FROM workspace_invites
		WHERE invite_id = $1 FOR UPDATE`, invite.InviteID).Scan(&locked); err != nil {
		t.Fatalf("lock rejoin invite: %v", err)
	}

	joinDone := make(chan error, 1)
	go func() {
		_, err := w.workspaces.RedeemInvite(ctx, invite.Code, w.agent)
		joinDone <- err
	}()
	waitForBlockedDatabaseSessions(t, ctx, w, 1)

	appendDone := make(chan appendOutcome, 1)
	go func() {
		message, created, err := ownerStore.AppendMessage(ctx, AppendInput{
			PlaceID: channel.PlaceID, Content: "audience after join",
			ClientNonce: "join-before-audience-snapshot",
		})
		appendDone <- appendOutcome{message: message, created: created, err: err}
	}()
	// Invite redemption holds Workspace FOR UPDATE while waiting on the invite;
	// append must wait rather than taking an audience snapshot with a phantom
	// omission.
	waitForBlockedDatabaseSessions(t, ctx, w, 2)

	if err := gate.Commit(ctx); err != nil {
		t.Fatalf("release rejoin-invite gate: %v", err)
	}
	if err := receiveError(t, joinDone, "recipient rejoin"); err != nil {
		t.Fatalf("recipient rejoin: %v", err)
	}
	outcome := receiveAppend(t, appendDone, "append after recipient join")
	if outcome.err != nil || !outcome.created {
		t.Fatalf("append after recipient join = created %v, err %v",
			outcome.created, outcome.err)
	}
	intents, err := ownerStore.NotificationIntentsForMessage(ctx, outcome.message.MessageID)
	if err != nil {
		t.Fatalf("load joined recipient intents: %v", err)
	}
	if !containsRecipient(intents, w.agent) {
		t.Fatalf("joined recipient absent from committed intent snapshot: %+v", intents)
	}
}

func TestPlaceFenceAdmissionPrecedesPrivateAudienceSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, _ := w.workspaceWithChannel(t, ctx)
	ownerStore := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	memberStore := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	if _, err := memberStore.SetNotificationSetting(ctx, NotifyLevelAll, nil, nil); err != nil {
		t.Fatalf("set private recipient notifications: %v", err)
	}
	dm, created, err := ownerStore.EnsureDM(ctx, w.humanB)
	if err != nil || !created {
		t.Fatalf("create dm = created %v, err %v", created, err)
	}
	if err := w.store.removeWorkspaceMember(ctx, workspace.WorkspaceID, w.humanB); err != nil {
		t.Fatalf("remove private recipient: %v", err)
	}
	invite, err := w.workspaces.CreateInvite(ctx, workspace.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatalf("create private recipient rejoin invite: %v", err)
	}
	if _, err := w.workspaces.RedeemInvite(ctx, invite.Code, w.humanB); err != nil {
		t.Fatalf("rejoin private recipient: %v", err)
	}
	newMembershipID := activeMembershipID(t, ctx, w, workspace.WorkspaceID, w.humanB)

	// Pause re-admission after EnsureDM has locked the existing DM place. The
	// place_members trigger takes a KEY SHARE lock on the new Workspace tenure,
	// so this row gate leaves the DM place lock held while admission waits.
	gate, err := w.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = gate.Rollback(context.Background()) }()
	var locked string
	if err := gate.QueryRow(ctx, `
		SELECT workspace_member_id FROM workspace_members
		WHERE workspace_member_id = $1 FOR UPDATE`, newMembershipID).Scan(&locked); err != nil {
		t.Fatalf("lock rejoined Workspace tenure: %v", err)
	}

	admissionDone := make(chan error, 1)
	go func() {
		_, _, err := memberStore.EnsureDM(ctx, w.humanA)
		admissionDone <- err
	}()
	waitForBlockedDatabaseSessions(t, ctx, w, 1)

	appendDone := make(chan appendOutcome, 1)
	go func() {
		message, created, err := ownerStore.AppendMessage(ctx, AppendInput{
			PlaceID: dm.PlaceID, Content: "private audience after re-admission",
			ClientNonce: "place-admission-before-audience-snapshot",
		})
		appendDone <- appendOutcome{message: message, created: created, err: err}
	}()
	// EnsureDM holds the existing place FOR UPDATE, so append cannot allocate a
	// sequence or snapshot the private audience until admission commits.
	waitForBlockedDatabaseSessions(t, ctx, w, 2)

	if err := gate.Commit(ctx); err != nil {
		t.Fatalf("release rejoined-tenure gate: %v", err)
	}
	if err := receiveError(t, admissionDone, "private-place re-admission"); err != nil {
		t.Fatalf("private-place re-admission: %v", err)
	}
	outcome := receiveAppend(t, appendDone, "append after private-place admission")
	if outcome.err != nil || !outcome.created {
		t.Fatalf("append after private-place admission = created %v, err %v",
			outcome.created, outcome.err)
	}
	intents, err := ownerStore.NotificationIntentsForMessage(ctx, outcome.message.MessageID)
	if err != nil {
		t.Fatalf("load private notification intents: %v", err)
	}
	if !containsRecipient(intents, w.humanB) {
		t.Fatalf("re-admitted private recipient absent from committed intent snapshot: %+v", intents)
	}
}

func activeMembershipID(
	t *testing.T,
	ctx context.Context,
	w world,
	workspaceID string,
	member ParticipantRef,
) string {
	t.Helper()
	var membershipID string
	if err := w.store.pool.QueryRow(ctx, `
		SELECT workspace_member_id FROM workspace_members
		WHERE workspace_id = $1 AND member_kind = $2 AND member_id = $3
		  AND left_at IS NULL`, workspaceID, member.Kind, member.ID).Scan(&membershipID); err != nil {
		t.Fatalf("load active Workspace membership: %v", err)
	}
	return membershipID
}

func activePlaceMembershipID(
	t *testing.T,
	ctx context.Context,
	w world,
	placeID string,
	member ParticipantRef,
) string {
	t.Helper()
	var placeMemberID string
	if err := w.store.pool.QueryRow(ctx, `
		SELECT place_member_id FROM place_members
		WHERE place_id = $1 AND member_kind = $2 AND member_id = $3
		  AND left_at IS NULL`, placeID, member.Kind, member.ID).Scan(&placeMemberID); err != nil {
		t.Fatalf("load active place membership: %v", err)
	}
	return placeMemberID
}

func waitForBlockedDatabaseSessions(t *testing.T, ctx context.Context, w world, want int) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		select {
		case <-poll.C:
			var blocked int
			if err := w.store.pool.QueryRow(ctx, `
				SELECT count(*) FROM pg_stat_activity
				WHERE datname = current_database()
				  AND pid <> pg_backend_pid()
				  AND wait_event_type = 'Lock'`).Scan(&blocked); err != nil {
				t.Fatalf("observe database lock waits: %v", err)
			}
			if blocked >= want {
				return
			}
		case <-deadline.C:
			var sessions []string
			rows, err := w.store.pool.Query(ctx, `
				SELECT state || ':' || COALESCE(wait_event_type, '-') || ':' || left(query, 120)
				FROM pg_stat_activity
				WHERE datname = current_database() AND pid <> pg_backend_pid()
				ORDER BY pid`)
			if err == nil {
				for rows.Next() {
					var session string
					if rows.Scan(&session) == nil {
						sessions = append(sessions, session)
					}
				}
				rows.Close()
			}
			t.Fatalf("database lock waits never reached %d; sessions=%v", want, sessions)
		case <-ctx.Done():
			t.Fatalf("context ended while waiting for %d database lock waits: %v", want, ctx.Err())
		}
	}
}

func receiveError(t *testing.T, result <-chan error, operation string) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not complete", operation)
		return nil
	}
}

func receiveAppend(t *testing.T, result <-chan appendOutcome, operation string) appendOutcome {
	t.Helper()
	select {
	case outcome := <-result:
		return outcome
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not complete", operation)
		return appendOutcome{}
	}
}

func containsRecipient(intents []NotificationDecision, recipient ParticipantRef) bool {
	for _, intent := range intents {
		if intent.Participant == recipient {
			return true
		}
	}
	return false
}
