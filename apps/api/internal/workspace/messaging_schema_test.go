package workspace

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

func TestMessagingPlaceTenureBindsExactWorkspaceMembership(t *testing.T) {
	w := newTestWorld(t)
	ctx := context.Background()
	created, err := w.store.CreateWorkspace(ctx, "Messaging scope", w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	invite, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	humanBMembership, err := w.store.RedeemInvite(ctx, invite.Code, w.humanB)
	if err != nil {
		t.Fatal(err)
	}

	placeID := newUUIDv7()
	const pairKey = "human:0198f0f4-9b72-7000-8000-000000000101|human:0198f0f4-9b72-7000-8000-000000000102"
	if _, err := w.pool.Exec(ctx, `
		INSERT INTO places (place_id, kind, workspace_id, dm_key)
		VALUES ($1, 'dm', $2, $3)`, placeID, created.WorkspaceID, pairKey); err != nil {
		t.Fatal(err)
	}
	ownerPlaceTenure := newUUIDv7()
	if _, err := w.pool.Exec(ctx, `
		INSERT INTO place_members
			(place_member_id, workspace_id, place_id, workspace_member_id,
			 member_kind, member_id, visible_from_seq)
		VALUES ($1, $2, $3, $4, $5, $6, 1)`, ownerPlaceTenure,
		created.WorkspaceID, placeID, created.OwnerWorkspaceMemberID,
		w.humanA.Kind, w.humanA.ID); err != nil {
		t.Fatal(err)
	}
	humanBPlaceTenure := newUUIDv7()
	if _, err := w.pool.Exec(ctx, `
		INSERT INTO place_members
			(place_member_id, workspace_id, place_id, workspace_member_id,
			 member_kind, member_id, visible_from_seq)
		VALUES ($1, $2, $3, $4, $5, $6, 1)`, humanBPlaceTenure,
		created.WorkspaceID, placeID, humanBMembership.WorkspaceMemberID,
		w.humanB.Kind, w.humanB.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := w.pool.Exec(ctx, `
		UPDATE workspace_members SET left_at = now()
		WHERE workspace_member_id = $1`, humanBMembership.WorkspaceMemberID); err == nil {
		t.Fatal("database allowed parent tenure closure while an active place tenure remained")
	}

	// The stable ParticipantRef cannot be paired with somebody else's tenure.
	if _, err := w.pool.Exec(ctx, `
		INSERT INTO place_members
			(place_member_id, workspace_id, place_id, workspace_member_id,
			 member_kind, member_id, visible_from_seq)
		VALUES ($1, $2, $3, $4, $5, $6, 1)`, newUUIDv7(),
		created.WorkspaceID, placeID, created.OwnerWorkspaceMemberID,
		w.humanB.Kind, w.humanB.ID); err == nil {
		t.Fatal("place tenure accepted a mismatched Workspace membership")
	}

	// The canonical pair key is scoped by Workspace, not globally.
	otherWorkspace, err := w.store.CreateWorkspace(ctx, "Other scope", w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.pool.Exec(ctx, `
		INSERT INTO places (place_id, kind, workspace_id, dm_key)
		VALUES ($1, 'dm', $2, $3)`, newUUIDv7(), otherWorkspace.WorkspaceID, pairKey); err != nil {
		t.Fatalf("same pair in another Workspace: %v", err)
	}
	if _, err := w.pool.Exec(ctx, `
		INSERT INTO places (place_id, kind, workspace_id, dm_key)
		VALUES ($1, 'dm', $2, $3)`, newUUIDv7(), created.WorkspaceID, pairKey); err == nil {
		t.Fatal("same Workspace admitted a duplicate canonical DM pair")
	}

	if _, err := w.pool.Exec(ctx, `
		INSERT INTO read_markers (place_id, workspace_member_id, last_read_seq)
		VALUES ($1, $2, 0)`, placeID, humanBMembership.WorkspaceMemberID); err != nil {
		t.Fatal(err)
	}
	if _, err := w.pool.Exec(ctx, `
		CREATE FUNCTION test_reject_place_tenure_close() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN
			IF NEW.place_member_id = '`+humanBPlaceTenure+`' THEN
				RAISE EXCEPTION 'test place close rejection';
			END IF;
			RETURN NEW;
		END $$;
		CREATE TRIGGER test_reject_place_tenure_close
		BEFORE UPDATE ON place_members FOR EACH ROW
		EXECUTE FUNCTION test_reject_place_tenure_close()`); err != nil {
		t.Fatal(err)
	}
	if err := w.store.RemoveMember(ctx, created.WorkspaceID,
		humanBMembership.WorkspaceMemberID, w.humanA); err == nil {
		t.Fatal("RemoveMember succeeded despite place-tenure closure failure")
	}
	var workspaceClosed, placeClosed bool
	if err := w.pool.QueryRow(ctx, `
		SELECT wm.left_at IS NOT NULL, pm.left_at IS NOT NULL
		FROM workspace_members wm
		JOIN place_members pm ON pm.workspace_member_id = wm.workspace_member_id
		WHERE wm.workspace_member_id = $1 AND pm.place_member_id = $2`,
		humanBMembership.WorkspaceMemberID, humanBPlaceTenure,
	).Scan(&workspaceClosed, &placeClosed); err != nil {
		t.Fatal(err)
	}
	if workspaceClosed || placeClosed {
		t.Fatalf("failed removal partially closed tenures: workspace=%v place=%v",
			workspaceClosed, placeClosed)
	}
	if _, err := w.pool.Exec(ctx, `
		DROP TRIGGER test_reject_place_tenure_close ON place_members;
		DROP FUNCTION test_reject_place_tenure_close()`); err != nil {
		t.Fatal(err)
	}
	if err := w.store.RemoveMember(ctx, created.WorkspaceID,
		humanBMembership.WorkspaceMemberID, w.humanA); err != nil {
		t.Fatal(err)
	}
	var workspaceLeftAt, placeLeftAt *time.Time
	if err := w.pool.QueryRow(ctx, `
		SELECT wm.left_at, pm.left_at
		FROM workspace_members wm
		JOIN place_members pm ON pm.workspace_member_id = wm.workspace_member_id
		WHERE wm.workspace_member_id = $1 AND pm.place_member_id = $2`,
		humanBMembership.WorkspaceMemberID, humanBPlaceTenure,
	).Scan(&workspaceLeftAt, &placeLeftAt); err != nil {
		t.Fatal(err)
	}
	if workspaceLeftAt == nil || placeLeftAt == nil || !workspaceLeftAt.Equal(*placeLeftAt) {
		t.Fatalf("tenure closure timestamps workspace=%v place=%v", workspaceLeftAt, placeLeftAt)
	}
	rejoinInvite, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	rejoined, err := w.store.RedeemInvite(ctx, rejoinInvite.Code, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	if rejoined.WorkspaceMemberID == humanBMembership.WorkspaceMemberID {
		t.Fatal("Workspace rejoin reused a membership tenure")
	}

	// Workspace rejoin alone does not revive the place. The old place tenure
	// was closed atomically with Workspace removal and no new one exists until
	// an explicit Messaging admission inserts it.
	var activeAfterRejoin int
	if err := w.pool.QueryRow(ctx, `
		SELECT count(*) FROM place_members
		WHERE place_id = $1 AND member_kind = $2 AND member_id = $3
		  AND left_at IS NULL`, placeID, w.humanB.Kind, w.humanB.ID,
	).Scan(&activeAfterRejoin); err != nil {
		t.Fatal(err)
	}
	if activeAfterRejoin != 0 {
		t.Fatalf("Workspace rejoin implicitly restored %d place tenures", activeAfterRejoin)
	}
	newPlaceTenure := newUUIDv7()
	if _, err := w.pool.Exec(ctx, `
		INSERT INTO place_members
			(place_member_id, workspace_id, place_id, workspace_member_id,
			 member_kind, member_id, visible_from_seq)
		VALUES ($1, $2, $3, $4, $5, $6, 1)`, newPlaceTenure, created.WorkspaceID,
		placeID, rejoined.WorkspaceMemberID, w.humanB.Kind, w.humanB.ID); err != nil {
		t.Fatalf("explicit 1:1 DM re-admission: %v", err)
	}
	var markerOnNewTenure bool
	if err := w.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM read_markers
			WHERE place_id = $1 AND workspace_member_id = $2
		)`, placeID, rejoined.WorkspaceMemberID).Scan(&markerOnNewTenure); err != nil {
		t.Fatal(err)
	}
	if markerOnNewTenure {
		t.Fatal("read marker leaked across place membership tenures")
	}

	leaveInvite, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	humanCMembership, err := w.store.RedeemInvite(ctx, leaveInvite.Code, w.humanC)
	if err != nil {
		t.Fatal(err)
	}
	groupID := newUUIDv7()
	if _, err := w.pool.Exec(ctx, `
		INSERT INTO places (place_id, kind, workspace_id)
		VALUES ($1, 'group_dm', $2)`, groupID, created.WorkspaceID); err != nil {
		t.Fatal(err)
	}
	humanCPlaceTenure := newUUIDv7()
	if _, err := w.pool.Exec(ctx, `
		INSERT INTO place_members
			(place_member_id, workspace_id, place_id, workspace_member_id,
			 member_kind, member_id, visible_from_seq)
		VALUES ($1, $2, $3, $4, $5, $6, 1)`, humanCPlaceTenure,
		created.WorkspaceID, groupID, humanCMembership.WorkspaceMemberID,
		w.humanC.Kind, w.humanC.ID); err != nil {
		t.Fatal(err)
	}
	if err := w.store.Leave(ctx, created.WorkspaceID, w.humanC); err != nil {
		t.Fatal(err)
	}
	if err := w.pool.QueryRow(ctx, `
		SELECT wm.left_at, pm.left_at
		FROM workspace_members wm
		JOIN place_members pm ON pm.workspace_member_id = wm.workspace_member_id
		WHERE wm.workspace_member_id = $1 AND pm.place_member_id = $2`,
		humanCMembership.WorkspaceMemberID, humanCPlaceTenure,
	).Scan(&workspaceLeftAt, &placeLeftAt); err != nil {
		t.Fatal(err)
	}
	if workspaceLeftAt == nil || placeLeftAt == nil || !workspaceLeftAt.Equal(*placeLeftAt) {
		t.Fatalf("Leave timestamps workspace=%v place=%v", workspaceLeftAt, placeLeftAt)
	}
}

func TestActivePlaceTenureRequiresAndSerializesWithActiveWorkspaceTenure(t *testing.T) {
	w := newTestWorld(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	created, err := w.store.CreateWorkspace(ctx, "tenure locks", w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	join := func(actor participant.Ref) Membership {
		t.Helper()
		invite, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanA)
		if err != nil {
			t.Fatal(err)
		}
		membership, err := w.store.RedeemInvite(ctx, invite.Code, actor)
		if err != nil {
			t.Fatal(err)
		}
		return membership
	}
	first := join(w.humanB)
	second := join(w.humanC)
	placeID := newUUIDv7()
	if _, err := w.pool.Exec(ctx, `
		INSERT INTO places (place_id, kind, workspace_id)
		VALUES ($1, 'group_dm', $2)`, placeID, created.WorkspaceID); err != nil {
		t.Fatal(err)
	}

	// Admission wins the parent-row lock first. RemoveMember must wait, then
	// include the newly committed child in the same-timestamp closure.
	admitTx, err := w.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	firstPlaceMemberID := newUUIDv7()
	if _, err := admitTx.Exec(ctx, `
		INSERT INTO place_members
			(place_member_id, workspace_id, place_id, workspace_member_id,
			 member_kind, member_id, visible_from_seq)
		VALUES ($1, $2, $3, $4, $5, $6, 1)`, firstPlaceMemberID,
		created.WorkspaceID, placeID, first.WorkspaceMemberID,
		w.humanB.Kind, w.humanB.ID); err != nil {
		t.Fatal(err)
	}
	removeResult := make(chan error, 1)
	go func() {
		removeResult <- w.store.RemoveMember(ctx, created.WorkspaceID,
			first.WorkspaceMemberID, w.humanA)
	}()
	select {
	case err := <-removeResult:
		t.Fatalf("parent closure did not wait for racing admission: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := admitTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-removeResult; err != nil {
		t.Fatalf("RemoveMember after admission commit: %v", err)
	}
	var workspaceLeftAt, placeLeftAt *time.Time
	if err := w.pool.QueryRow(ctx, `
		SELECT wm.left_at, pm.left_at
		FROM workspace_members wm
		JOIN place_members pm ON pm.workspace_member_id = wm.workspace_member_id
		WHERE wm.workspace_member_id = $1 AND pm.place_member_id = $2`,
		first.WorkspaceMemberID, firstPlaceMemberID,
	).Scan(&workspaceLeftAt, &placeLeftAt); err != nil {
		t.Fatal(err)
	}
	if workspaceLeftAt == nil || placeLeftAt == nil || !workspaceLeftAt.Equal(*placeLeftAt) {
		t.Fatalf("racing admission closure timestamps workspace=%v place=%v",
			workspaceLeftAt, placeLeftAt)
	}

	// Closure wins the parent FOR UPDATE lock first. A racing child admission
	// waits and then fails because the exact parent tenure is no longer active.
	closeTx, err := w.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var lockedMembershipID string
	if err := closeTx.QueryRow(ctx, `
		SELECT workspace_member_id FROM workspace_members
		WHERE workspace_id = $1 AND workspace_member_id = $2 AND left_at IS NULL
		FOR UPDATE`, created.WorkspaceID, second.WorkspaceMemberID,
	).Scan(&lockedMembershipID); err != nil {
		t.Fatal(err)
	}
	closedAt := time.Now().UTC()
	if _, err := closeTx.Exec(ctx, `
		UPDATE workspace_members SET left_at = $3
		WHERE workspace_id = $1 AND workspace_member_id = $2`,
		created.WorkspaceID, second.WorkspaceMemberID, closedAt); err != nil {
		t.Fatal(err)
	}
	admitResult := make(chan error, 1)
	go func() {
		_, err := w.pool.Exec(ctx, `
			INSERT INTO place_members
				(place_member_id, workspace_id, place_id, workspace_member_id,
				 member_kind, member_id, visible_from_seq)
			VALUES ($1, $2, $3, $4, $5, $6, 1)`, newUUIDv7(),
			created.WorkspaceID, placeID, second.WorkspaceMemberID,
			w.humanC.Kind, w.humanC.ID)
		admitResult <- err
	}()
	select {
	case err := <-admitResult:
		t.Fatalf("racing admission did not wait for parent closure: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := closeTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-admitResult; err == nil {
		t.Fatal("active place tenure bound a concurrently closed Workspace tenure")
	}

	// The same trigger rejects both a direct insert under a closed parent and
	// reopening a previously closed child tenure.
	if _, err := w.pool.Exec(ctx, `
		INSERT INTO place_members
			(place_member_id, workspace_id, place_id, workspace_member_id,
			 member_kind, member_id, visible_from_seq)
		VALUES ($1, $2, $3, $4, $5, $6, 1)`, newUUIDv7(),
		created.WorkspaceID, placeID, second.WorkspaceMemberID,
		w.humanC.Kind, w.humanC.ID); err == nil {
		t.Fatal("active place tenure accepted an already closed Workspace tenure")
	}
	if _, err := w.pool.Exec(ctx, `
		UPDATE place_members SET left_at = NULL WHERE place_member_id = $1`,
		firstPlaceMemberID); err == nil {
		t.Fatal("closed place tenure reopened under a closed Workspace tenure")
	}
}

func TestWorkspaceAndPlaceTenuresShareClosureTimeAfterLaterChildJoin(t *testing.T) {
	w := newTestWorld(t)
	ctx := context.Background()
	created, err := w.store.CreateWorkspace(ctx, "closure clock", w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	invite, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	membership, err := w.store.RedeemInvite(ctx, invite.Code, w.humanB)
	if err != nil {
		t.Fatal(err)
	}

	joinedTimes := []time.Time{
		membership.JoinedAt.Add(5 * time.Minute),
		membership.JoinedAt.Add(10 * time.Minute),
	}
	placeMemberIDs := make([]string, len(joinedTimes))
	for i, joinedAt := range joinedTimes {
		placeID := newUUIDv7()
		if _, err := w.pool.Exec(ctx, `
			INSERT INTO places (place_id, kind, workspace_id, name)
			VALUES ($1, 'channel', $2, $3)`, placeID, created.WorkspaceID,
			fmt.Sprintf("clock-%d", i)); err != nil {
			t.Fatal(err)
		}
		placeMemberIDs[i] = newUUIDv7()
		if _, err := w.pool.Exec(ctx, `
			INSERT INTO place_members
				(place_member_id, workspace_id, place_id, workspace_member_id,
				 member_kind, member_id, visible_from_seq, joined_at)
			VALUES ($1, $2, $3, $4, $5, $6, 1, $7)`, placeMemberIDs[i],
			created.WorkspaceID, placeID, membership.WorkspaceMemberID,
			w.humanB.Kind, w.humanB.ID, joinedAt); err != nil {
			t.Fatal(err)
		}
	}

	// A wildly future application clock must not inflate the closure boundary.
	// The database chooses one clock value and raises it only as far as needed
	// for the latest parent/child join invariant.
	skewedApplicationTime := membership.JoinedAt.AddDate(100, 0, 0)
	w.store.now = func() time.Time { return skewedApplicationTime }
	if err := w.store.RemoveMember(ctx, created.WorkspaceID,
		membership.WorkspaceMemberID, w.humanA); err != nil {
		t.Fatal(err)
	}
	var parentLeftAt time.Time
	if err := w.pool.QueryRow(ctx, `
		SELECT left_at FROM workspace_members
		WHERE workspace_member_id = $1`, membership.WorkspaceMemberID,
	).Scan(&parentLeftAt); err != nil {
		t.Fatal(err)
	}
	wantLeftAt := joinedTimes[1].Truncate(time.Microsecond)
	if !parentLeftAt.Equal(wantLeftAt) {
		t.Fatalf("effective parent closure = %s, want latest join %s (application clock %s)",
			parentLeftAt, wantLeftAt, skewedApplicationTime)
	}
	for _, placeMemberID := range placeMemberIDs {
		var childLeftAt time.Time
		if err := w.pool.QueryRow(ctx, `
			SELECT left_at FROM place_members WHERE place_member_id = $1`,
			placeMemberID).Scan(&childLeftAt); err != nil {
			t.Fatal(err)
		}
		if !childLeftAt.Equal(parentLeftAt) {
			t.Fatalf("child %s closure = %s, parent = %s",
				placeMemberID, childLeftAt, parentLeftAt)
		}
	}
}
