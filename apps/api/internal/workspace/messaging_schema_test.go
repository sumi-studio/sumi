package workspace

import (
	"context"
	"testing"
	"time"
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
		INSERT INTO read_markers (place_id, place_member_id, last_read_seq)
		VALUES ($1, $2, 0)`, placeID, humanBPlaceTenure); err != nil {
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
			WHERE place_id = $1 AND place_member_id = $2
		)`, placeID, newPlaceTenure).Scan(&markerOnNewTenure); err != nil {
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
