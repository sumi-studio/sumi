package workspace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

const (
	testHumanA                  = "0198f0f4-9b72-7000-8000-000000000101"
	testHumanB                  = "0198f0f4-9b72-7000-8000-000000000102"
	testHumanC                  = "0198f0f4-9b72-7000-8000-000000000103"
	testAgentA                  = "0198f0f4-9b72-7000-8000-0000000001a1"
	testAgentB                  = "0198f0f4-9b72-7000-8000-0000000001a2"
	testMessagingManageChannels = "app.messaging.manage_channels"
)

type testWorld struct {
	pool   *pgxpool.Pool
	store  *Store
	humanA participant.Ref
	humanB participant.Ref
	humanC participant.Ref
	agentA participant.Ref
	agentB participant.Ref
}

func newTestWorld(t *testing.T) testWorld {
	t.Helper()
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	for humanID, displayName := range map[string]string{
		testHumanA: "Yohaku", testHumanB: "Haru", testHumanC: "Mio",
	} {
		if _, err := pool.Exec(ctx, "INSERT INTO humans (human_id, display_name) VALUES ($1, $2)", humanID, displayName); err != nil {
			t.Fatalf("insert Human %s: %v", humanID, err)
		}
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO agents (personality_agent_id, human_id, display_name)
		VALUES ($1, $2, 'Kuro'), ($3, $4, 'Shiro')`,
		testAgentA, testHumanA, testAgentB, testHumanB); err != nil {
		t.Fatalf("insert PersonalityAgent: %v", err)
	}
	return testWorld{
		pool: pool, store: New(pool),
		humanA: participant.Human(testHumanA),
		humanB: participant.Human(testHumanB),
		humanC: participant.Human(testHumanC),
		agentA: participant.PersonalityAgent(testAgentA),
		agentB: participant.PersonalityAgent(testAgentB),
	}
}

func TestWorkspaceCreateIsAtomicAndReadsNeverAdmit(t *testing.T) {
	w := newTestWorld(t)
	ctx := context.Background()

	before := tableCounts(t, ctx, w.pool)
	listed, err := w.store.WorkspacesFor(ctx, w.humanA)
	if err != nil || len(listed) != 0 {
		t.Fatalf("empty WorkspacesFor = %#v, %v", listed, err)
	}
	afterRead := tableCounts(t, ctx, w.pool)
	if before != afterRead {
		t.Fatalf("read mutated Workspace state: before=%v after=%v", before, afterRead)
	}

	created, err := w.store.CreateWorkspace(ctx, "Sumi developers", w.humanA)
	if err != nil {
		t.Fatalf("CreateWorkspace: %v", err)
	}
	members, err := w.store.Members(ctx, created.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatalf("Members: %v", err)
	}
	if len(members) != 1 || !members[0].Owner ||
		members[0].WorkspaceMemberID != created.OwnerWorkspaceMemberID ||
		members[0].Participant != w.humanA {
		t.Fatalf("owner membership = %#v", members)
	}
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	exact, err := w.store.ActiveMembershipInTx(ctx, tx, created.WorkspaceID, w.humanA)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("ActiveMembershipInTx: %v", err)
	}
	active, err := w.store.ActiveMembershipsInTx(ctx, tx, created.WorkspaceID)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("ActiveMembershipsInTx: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if exact.WorkspaceMemberID != created.OwnerWorkspaceMemberID ||
		len(active) != 1 || active[0].WorkspaceMemberID != exact.WorkspaceMemberID ||
		active[0].Participant != w.humanA {
		t.Fatalf("exact active tenures exact=%#v active=%#v", exact, active)
	}

	// Make the second half of creation fail inside the transaction. The
	// workspace row inserted before it must roll back too.
	if _, err := w.pool.Exec(ctx, `
		CREATE FUNCTION test_reject_owner_membership() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN
			IF NEW.member_id = '`+testHumanB+`' THEN
				RAISE EXCEPTION 'test membership rejection';
			END IF;
			RETURN NEW;
		END $$;
		CREATE TRIGGER test_reject_owner_membership
		BEFORE INSERT ON workspace_members FOR EACH ROW
		EXECUTE FUNCTION test_reject_owner_membership()`); err != nil {
		t.Fatalf("install atomicity failpoint: %v", err)
	}
	if _, err := w.store.CreateWorkspace(ctx, "must roll back", w.humanB); err == nil {
		t.Fatal("CreateWorkspace succeeded despite owner-membership failure")
	}
	var leaked int
	if err := w.pool.QueryRow(ctx,
		"SELECT count(*) FROM workspaces WHERE name = 'must roll back'",
	).Scan(&leaked); err != nil {
		t.Fatal(err)
	}
	if leaked != 0 {
		t.Fatalf("atomic create leaked %d Workspace rows", leaked)
	}
}

func TestWorkspaceExistenceHidingAndOwnerProtection(t *testing.T) {
	w := newTestWorld(t)
	ctx := context.Background()
	created, err := w.store.CreateWorkspace(ctx, "private", w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	missingID := "0198f0f4-9b72-7000-8000-000000000199"
	_, hiddenErr := w.store.WorkspaceFor(ctx, created.WorkspaceID, w.humanB)
	_, missingErr := w.store.WorkspaceFor(ctx, missingID, w.humanB)
	if !errors.Is(hiddenErr, ErrNotFound) || !errors.Is(missingErr, ErrNotFound) {
		t.Fatalf("existence errors hidden=%v missing=%v", hiddenErr, missingErr)
	}
	if hiddenErr.Error() != missingErr.Error() {
		t.Fatalf("existence disclosure differs: hidden=%q missing=%q", hiddenErr, missingErr)
	}
	if err := w.store.Leave(ctx, created.WorkspaceID, w.humanA); !errors.Is(err, ErrOwnerProtected) {
		t.Fatalf("owner Leave error = %v", err)
	}
	if err := w.store.RemoveMember(ctx, created.WorkspaceID,
		created.OwnerWorkspaceMemberID, w.humanA); !errors.Is(err, ErrOwnerProtected) {
		t.Fatalf("owner RemoveMember error = %v", err)
	}
	if _, err := w.pool.Exec(ctx, `
		UPDATE workspace_members SET left_at = now()
		WHERE workspace_member_id = $1`, created.OwnerWorkspaceMemberID); err == nil {
		t.Fatal("database allowed direct owner-membership closure")
	}
	invite, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	member, err := w.store.RedeemInvite(ctx, invite.Code, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.pool.Exec(ctx, `
		UPDATE workspaces SET owner_workspace_member_id = $2
		WHERE workspace_id = $1`, created.WorkspaceID, member.WorkspaceMemberID); err == nil {
		t.Fatal("database allowed owner transfer without a defined operation")
	}
}

func TestInviteAdmissionIsOpaqueBoundedAndHumanAgentSymmetric(t *testing.T) {
	w := newTestWorld(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	w.store.now = func() time.Time { return now }
	created, err := w.store.CreateWorkspace(ctx, "shared", w.humanA)
	if err != nil {
		t.Fatal(err)
	}

	oneUse, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	if !oneUse.ExpiresAt.Equal(now.Add(24 * time.Hour)) {
		t.Fatalf("default invite expiry = %s", oneUse.ExpiresAt)
	}
	for i := 0; i < 2; i++ {
		preview, err := w.store.PreviewInvite(ctx, oneUse.Code)
		if err != nil {
			t.Fatalf("non-consuming preview %d: %v", i, err)
		}
		if preview.WorkspaceID != created.WorkspaceID ||
			preview.WorkspaceName != "shared" || !preview.ExpiresAt.Equal(oneUse.ExpiresAt) {
			t.Fatalf("minimal invite preview = %#v", preview)
		}
	}
	var storedHash []byte
	if err := w.pool.QueryRow(ctx,
		"SELECT code_hash FROM workspace_invites WHERE invite_id = $1", oneUse.InviteID,
	).Scan(&storedHash); err != nil {
		t.Fatal(err)
	}
	wantHash := sha256.Sum256([]byte(oneUse.Code))
	if !bytes.Equal(storedHash, wantHash[:]) {
		t.Fatal("invite code was not stored as its SHA-256 digest")
	}
	var plaintextColumn bool
	if err := w.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = current_schema()
			  AND table_name = 'workspace_invites' AND column_name = 'code'
		)`).Scan(&plaintextColumn); err != nil {
		t.Fatal(err)
	}
	if plaintextColumn {
		t.Fatal("workspace_invites exposes a plaintext code column")
	}
	humanMembership, err := w.store.RedeemInvite(ctx, oneUse.Code, w.humanB)
	if err != nil {
		t.Fatalf("Human redeem: %v", err)
	}
	if humanMembership.Participant != w.humanB {
		t.Fatalf("Human admission = %#v", humanMembership)
	}
	if _, err := w.store.PreviewInvite(ctx, oneUse.Code); !errors.Is(err, ErrInviteUnavailable) {
		t.Fatalf("consumed invite preview error = %v", err)
	}
	now = now.Add(25 * time.Hour)
	replayed, err := w.store.RedeemInvite(ctx, oneUse.Code, w.humanB)
	if err != nil || replayed.WorkspaceMemberID != humanMembership.WorkspaceMemberID {
		t.Fatalf("same-Human replay = %#v, %v", replayed, err)
	}
	if _, err := w.store.RedeemInvite(ctx, oneUse.Code, w.agentA); !errors.Is(err, ErrInviteUnavailable) {
		t.Fatalf("single-use replay error = %v", err)
	}

	w.store.inviteTTL = time.Minute
	expiring, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	if _, err := w.store.RedeemInvite(ctx, expiring.Code, w.agentA); !errors.Is(err, ErrInviteUnavailable) {
		t.Fatalf("expired invite error = %v", err)
	}

	agentInvite, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	agentMembership, err := w.store.RedeemInvite(ctx, agentInvite.Code, w.agentA)
	if err != nil {
		t.Fatalf("PersonalityAgent redeem: %v", err)
	}
	if agentMembership.Participant != w.agentA {
		t.Fatalf("Agent admission = %#v", agentMembership)
	}
	if len(agentMembership.RoleIDs) != 0 {
		t.Fatalf("invite redemption elevated Agent roles: %#v", agentMembership.RoleIDs)
	}
	var assignedRoles int
	if err := w.pool.QueryRow(ctx, `
		SELECT count(*) FROM workspace_role_assignments
		WHERE workspace_member_id = $1`, agentMembership.WorkspaceMemberID,
	).Scan(&assignedRoles); err != nil {
		t.Fatal(err)
	}
	if assignedRoles != 0 {
		t.Fatalf("invite redemption persisted %d role assignments", assignedRoles)
	}

	// Both actor kinds call the same creation operation and receive the same
	// distinguished-owner invariant.
	agentOwned, err := w.store.CreateWorkspace(ctx, "agent-created", w.agentA)
	if err != nil {
		t.Fatalf("PersonalityAgent CreateWorkspace: %v", err)
	}
	if agentOwned.OwnerWorkspaceMemberID == "" {
		t.Fatal("agent-created Workspace lacks owner membership")
	}
}

func TestInviteReplayReturnsCurrentRoleAndClosedTenureState(t *testing.T) {
	w := newTestWorld(t)
	ctx := context.Background()
	created, err := w.store.CreateWorkspace(ctx, "replay state", w.humanA)
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
	role, err := w.store.CreateRole(ctx, created.WorkspaceID, w.humanA,
		"Channel manager", "", map[string]bool{testMessagingManageChannels: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.SetMembershipRoles(ctx, created.WorkspaceID,
		membership.WorkspaceMemberID, w.humanA, []string{role.RoleID}); err != nil {
		t.Fatal(err)
	}

	replayed, err := w.store.RedeemInvite(ctx, invite.Code, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed.RoleIDs) != 1 || replayed.RoleIDs[0] != role.RoleID || replayed.LeftAt != nil {
		t.Fatalf("role-aware active replay = %#v", replayed)
	}
	if err := w.store.RemoveMember(ctx, created.WorkspaceID,
		membership.WorkspaceMemberID, w.humanA); err != nil {
		t.Fatal(err)
	}
	closedReplay, err := w.store.RedeemInvite(ctx, invite.Code, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	if closedReplay.WorkspaceMemberID != membership.WorkspaceMemberID ||
		closedReplay.LeftAt == nil || len(closedReplay.RoleIDs) != 1 ||
		closedReplay.RoleIDs[0] != role.RoleID {
		t.Fatalf("closed-tenure replay = %#v", closedReplay)
	}
	var active int
	if err := w.pool.QueryRow(ctx, `
		SELECT count(*) FROM workspace_members
		WHERE workspace_id = $1 AND member_kind = $2 AND member_id = $3
		  AND left_at IS NULL`, created.WorkspaceID, w.humanB.Kind, w.humanB.ID,
	).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 0 {
		t.Fatalf("replay reopened %d membership tenures", active)
	}
}

func TestInviteRedemptionRequiresIssuingTenureCurrentAuthority(t *testing.T) {
	w := newTestWorld(t)
	ctx := context.Background()
	created, err := w.store.CreateWorkspace(ctx, "revocable issuer", w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	issuerInvite, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	issuerMembership, err := w.store.RedeemInvite(ctx, issuerInvite.Code, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	issuerRole, err := w.store.CreateRole(ctx, created.WorkspaceID, w.humanA,
		"Inviter", "", map[string]bool{PermissionManageMembers: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.SetMembershipRoles(ctx, created.WorkspaceID,
		issuerMembership.WorkspaceMemberID, w.humanA, []string{issuerRole.RoleID}); err != nil {
		t.Fatal(err)
	}

	issued, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanB)
	if err != nil {
		t.Fatalf("delegated issuer CreateInvite: %v", err)
	}
	revokeCandidate, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanB)
	if err != nil {
		t.Fatalf("delegated issuer second CreateInvite: %v", err)
	}
	completedInvite, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanB)
	if err != nil {
		t.Fatalf("delegated issuer completed CreateInvite: %v", err)
	}
	completedMembership, err := w.store.RedeemInvite(ctx, completedInvite.Code, w.humanC)
	if err != nil {
		t.Fatalf("redeem before issuer revocation: %v", err)
	}
	if _, err := w.store.SetMembershipRoles(ctx, created.WorkspaceID,
		issuerMembership.WorkspaceMemberID, w.humanA, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanB); !errors.Is(err, ErrForbidden) {
		t.Fatalf("CreateInvite after losing manage_members = %v", err)
	}
	if err := w.store.RevokeInvite(ctx, created.WorkspaceID,
		revokeCandidate.InviteID, w.humanB); !errors.Is(err, ErrForbidden) {
		t.Fatalf("RevokeInvite after losing manage_members = %v", err)
	}
	if err := w.store.RevokeInvite(ctx, created.WorkspaceID,
		revokeCandidate.InviteID, w.humanA); err != nil {
		t.Fatalf("owner RevokeInvite: %v", err)
	}
	replayed, err := w.store.RedeemInvite(ctx, completedInvite.Code, w.humanC)
	if err != nil || replayed.WorkspaceMemberID != completedMembership.WorkspaceMemberID {
		t.Fatalf("completed redemption lost idempotence after issuer revocation: %#v, %v", replayed, err)
	}
	if _, err := w.store.RedeemInvite(ctx, issued.Code, w.agentA); !errors.Is(err, ErrInviteUnavailable) {
		t.Fatalf("redeem after issuer lost manage_members = %v", err)
	}

	// Rejoining the same Human creates a new tenure and must not revive an
	// invite issued by the closed tenure, even after the new tenure gains the
	// same authority.
	if err := w.store.RemoveMember(ctx, created.WorkspaceID,
		issuerMembership.WorkspaceMemberID, w.humanA); err != nil {
		t.Fatal(err)
	}
	rejoinInvite, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	rejoined, err := w.store.RedeemInvite(ctx, rejoinInvite.Code, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	if rejoined.WorkspaceMemberID == issuerMembership.WorkspaceMemberID {
		t.Fatal("rejoin reused the prior membership tenure")
	}
	if _, err := w.store.SetMembershipRoles(ctx, created.WorkspaceID,
		rejoined.WorkspaceMemberID, w.humanA, []string{issuerRole.RoleID}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.RedeemInvite(ctx, issued.Code, w.agentA); !errors.Is(err, ErrInviteUnavailable) {
		t.Fatalf("new issuer tenure revived old invite: %v", err)
	}
}

func TestInviteListingRetainsOnlyRevocableNonSecretRecords(t *testing.T) {
	w := newTestWorld(t)
	ctx := context.Background()
	created, err := w.store.CreateWorkspace(ctx, "invite registry", w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	first, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	second, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	listed, err := w.store.Invites(ctx, created.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 2 || !inviteRecordIDs(listed)[first.InviteID] ||
		!inviteRecordIDs(listed)[second.InviteID] {
		t.Fatalf("initial invite records = %#v", listed)
	}

	humanBMembership, err := w.store.RedeemInvite(ctx, first.Code, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	listed, err = w.store.Invites(ctx, created.WorkspaceID, w.humanA)
	if err != nil || len(listed) != 1 || listed[0].InviteID != second.InviteID {
		t.Fatalf("after redemption invite records = %#v, %v", listed, err)
	}
	if err := w.store.RevokeInvite(ctx, created.WorkspaceID, second.InviteID, w.humanA); err != nil {
		t.Fatal(err)
	}
	listed, err = w.store.Invites(ctx, created.WorkspaceID, w.humanA)
	if err != nil || len(listed) != 0 {
		t.Fatalf("after revocation invite records = %#v, %v", listed, err)
	}

	issuerRole, err := w.store.CreateRole(ctx, created.WorkspaceID, w.humanA,
		"Inviter", "", map[string]bool{PermissionManageMembers: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.SetMembershipRoles(ctx, created.WorkspaceID,
		humanBMembership.WorkspaceMemberID, w.humanA, []string{issuerRole.RoleID}); err != nil {
		t.Fatal(err)
	}
	delegated, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	listed, err = w.store.Invites(ctx, created.WorkspaceID, w.humanB)
	if err != nil || len(listed) != 1 || listed[0].InviteID != delegated.InviteID {
		t.Fatalf("delegated invite records = %#v, %v", listed, err)
	}
	if _, err := w.store.SetMembershipRoles(ctx, created.WorkspaceID,
		humanBMembership.WorkspaceMemberID, w.humanA, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.Invites(ctx, created.WorkspaceID, w.humanB); !errors.Is(err, ErrForbidden) {
		t.Fatalf("list after losing manage_members = %v", err)
	}
	listed, err = w.store.Invites(ctx, created.WorkspaceID, w.humanA)
	if err != nil || len(listed) != 0 {
		t.Fatalf("owner saw invalid issuer invite = %#v, %v", listed, err)
	}
}

func inviteRecordIDs(records []InviteRecord) map[string]bool {
	ids := make(map[string]bool, len(records))
	for _, record := range records {
		ids[record.InviteID] = true
	}
	return ids
}

func TestInviteRedemptionSerializesSameAndDifferentActors(t *testing.T) {
	w := newTestWorld(t)
	ctx := context.Background()
	created, err := w.store.CreateWorkspace(ctx, "invite races", w.humanA)
	if err != nil {
		t.Fatal(err)
	}

	sameActorInvite, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	const retries = 8
	var wait sync.WaitGroup
	sameResults := make(chan struct {
		membership Membership
		err        error
	}, retries)
	for range retries {
		wait.Add(1)
		go func() {
			defer wait.Done()
			membership, err := w.store.RedeemInvite(ctx, sameActorInvite.Code, w.humanB)
			sameResults <- struct {
				membership Membership
				err        error
			}{membership, err}
		}()
	}
	wait.Wait()
	close(sameResults)
	var membershipID string
	for result := range sameResults {
		if result.err != nil {
			t.Fatalf("same-actor concurrent retry: %v", result.err)
		}
		if membershipID == "" {
			membershipID = result.membership.WorkspaceMemberID
		}
		if result.membership.WorkspaceMemberID != membershipID {
			t.Fatalf("same actor received multiple memberships: %s vs %s",
				membershipID, result.membership.WorkspaceMemberID)
		}
	}

	differentActorInvite, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	type redemption struct {
		actor      participant.Ref
		membership Membership
		err        error
	}
	differentResults := make(chan redemption, 2)
	for _, actor := range []participant.Ref{w.humanC, w.agentA} {
		wait.Add(1)
		go func(actor participant.Ref) {
			defer wait.Done()
			membership, err := w.store.RedeemInvite(ctx, differentActorInvite.Code, actor)
			differentResults <- redemption{actor: actor, membership: membership, err: err}
		}(actor)
	}
	wait.Wait()
	close(differentResults)
	var winner *redemption
	var loser *redemption
	for result := range differentResults {
		result := result
		if result.err == nil {
			winner = &result
		} else if errors.Is(result.err, ErrInviteUnavailable) {
			loser = &result
		} else {
			t.Fatalf("different-actor race: %v", result.err)
		}
	}
	if winner == nil || loser == nil {
		t.Fatalf("different-actor winner=%#v loser=%#v", winner, loser)
	}
	replayed, err := w.store.RedeemInvite(ctx, differentActorInvite.Code, winner.actor)
	if err != nil || replayed.WorkspaceMemberID != winner.membership.WorkspaceMemberID {
		t.Fatalf("winner replay = %#v, %v", replayed, err)
	}
	if _, err := w.store.RedeemInvite(ctx, differentActorInvite.Code, loser.actor); !errors.Is(err, ErrInviteUnavailable) {
		t.Fatalf("loser replay error = %v", err)
	}
}

func TestRolesBindToTenureAndEnforcePrivilegeCeilingUnderConcurrency(t *testing.T) {
	w := newTestWorld(t)
	ctx := context.Background()
	created, err := w.store.CreateWorkspace(ctx, "roles", w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	join := func(actor participant.Ref) Membership {
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
	humanMembership := join(w.humanB)
	agentMembership := join(w.agentA)

	roleManager, err := w.store.CreateRole(ctx, created.WorkspaceID, w.humanA,
		"Role manager", "", map[string]bool{
			PermissionManageRoles:   true,
			PermissionManageMembers: true,
		})
	if err != nil {
		t.Fatal(err)
	}
	for _, membershipID := range []string{humanMembership.WorkspaceMemberID, agentMembership.WorkspaceMemberID} {
		if _, err := w.store.SetMembershipRoles(ctx, created.WorkspaceID,
			membershipID, w.humanA, []string{roleManager.RoleID}); err != nil {
			t.Fatal(err)
		}
	}

	// Two concurrent actors with manage_roles but without manage_apps must both
	// fail closed; serialization must not turn one rejected proposal into a
	// privilege that lets the other succeed.
	actors := []participant.Ref{w.humanB, w.agentA}
	var wait sync.WaitGroup
	errorsSeen := make(chan error, len(actors))
	for i, actor := range actors {
		wait.Add(1)
		go func(i int, actor participant.Ref) {
			defer wait.Done()
			_, err := w.store.CreateRole(ctx, created.WorkspaceID, actor,
				fmt.Sprintf("escalation-%d", i), "", map[string]bool{
					PermissionManageApps: true,
				})
			errorsSeen <- err
		}(i, actor)
	}
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if !errors.Is(err, ErrForbidden) {
			t.Fatalf("concurrent privilege-ceiling error = %v", err)
		}
	}

	// Leave closes one tenure. Rejoining creates a distinct membership id, and
	// the old tenure's assignment cannot affect it.
	if err := w.store.RemoveMember(ctx, created.WorkspaceID,
		humanMembership.WorkspaceMemberID, w.humanA); err != nil {
		t.Fatal(err)
	}
	rejoined := join(w.humanB)
	if rejoined.WorkspaceMemberID == humanMembership.WorkspaceMemberID {
		t.Fatal("rejoin reused the previous membership tenure")
	}
	permissions, err := w.store.PermissionsFor(ctx, created.WorkspaceID, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	if len(permissions) != 0 {
		t.Fatalf("rejoin resurrected stale roles: %#v", permissions)
	}
	var oldAssignments int
	if err := w.pool.QueryRow(ctx, `
		SELECT count(*) FROM workspace_role_assignments
		WHERE workspace_member_id = $1`, humanMembership.WorkspaceMemberID,
	).Scan(&oldAssignments); err != nil {
		t.Fatal(err)
	}
	if oldAssignments != 1 {
		t.Fatalf("historical tenure assignment count = %d, want 1", oldAssignments)
	}
}

func TestAppRoleCapabilitiesAreCatalogBackedAndPrivilegeBounded(t *testing.T) {
	w := newTestWorld(t)
	ctx := context.Background()
	created, err := w.store.CreateWorkspace(ctx, "capability boundary", w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	invite, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	member, err := w.store.RedeemInvite(ctx, invite.Code, w.humanB)
	if err != nil {
		t.Fatal(err)
	}

	for _, unknown := range []string{
		"manage_channels",
		"app.messaging.mention_all",
		"app.messaging.ManageChannels",
		"app.unknown.manage_channels",
	} {
		if _, err := w.store.CreateRole(ctx, created.WorkspaceID, w.humanA,
			"unknown "+unknown, "", map[string]bool{unknown: true}); !errors.Is(err, ErrInvalidPermission) {
			t.Fatalf("unknown capability %q error = %v", unknown, err)
		}
	}

	roleManager, err := w.store.CreateRole(ctx, created.WorkspaceID, w.humanA,
		"Role manager", "", map[string]bool{
			PermissionManageRoles:   true,
			PermissionManageMembers: true,
		})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.SetMembershipRoles(ctx, created.WorkspaceID,
		member.WorkspaceMemberID, w.humanA, []string{roleManager.RoleID}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.CreateRole(ctx, created.WorkspaceID, w.humanB,
		"escalation", "", map[string]bool{testMessagingManageChannels: true}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("non-owner granted capability above ceiling: %v", err)
	}

	roleManager, err = w.store.UpdateRole(ctx, created.WorkspaceID, roleManager.RoleID,
		w.humanA, "Role and channel manager", "", map[string]bool{
			PermissionManageRoles:       true,
			PermissionManageMembers:     true,
			testMessagingManageChannels: true,
		})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.CreateRole(ctx, created.WorkspaceID, w.humanB,
		"delegated channel manager", "", map[string]bool{
			testMessagingManageChannels: true,
		}); err != nil {
		t.Fatalf("capability holder could not grant within ceiling: %v", err)
	}

	require := func(actor participant.Ref) error {
		t.Helper()
		tx, err := w.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		return w.store.LockAndRequireAppCapability(
			ctx, tx, created.WorkspaceID, actor, testMessagingManageChannels,
		)
	}
	if err := require(w.humanA); err != nil {
		t.Fatalf("owner capability bypass = %v", err)
	}
	if err := require(w.humanB); err != nil {
		t.Fatalf("role capability admission = %v", err)
	}
	if err := require(w.humanC); !errors.Is(err, ErrNotFound) {
		t.Fatalf("non-member crossed owner/capability bypass = %v", err)
	}
}

func TestRetiredAppCapabilityStaysVisibleWithoutResurrectingAuthority(t *testing.T) {
	w := newTestWorld(t)
	ctx := context.Background()
	created, err := w.store.CreateWorkspace(ctx, "retired capability", w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	invite, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	member, err := w.store.RedeemInvite(ctx, invite.Code, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	role, err := w.store.CreateRole(ctx, created.WorkspaceID, w.humanA,
		"Channel manager", "", map[string]bool{testMessagingManageChannels: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.SetMembershipRoles(ctx, created.WorkspaceID,
		member.WorkspaceMemberID, w.humanA, []string{role.RoleID}); err != nil {
		t.Fatal(err)
	}
	var originalCapabilityID, originalSnapshot string
	if err := w.pool.QueryRow(ctx, `
		SELECT capability_id, capability_ref_snapshot
		FROM workspace_role_app_capability_grants
		WHERE workspace_id = $1 AND role_id = $2`,
		created.WorkspaceID, role.RoleID,
	).Scan(&originalCapabilityID, &originalSnapshot); err != nil {
		t.Fatal(err)
	}

	if _, err := w.pool.Exec(ctx, `
		UPDATE app_workspace_role_capabilities SET retired_at = now()
		WHERE capability_ref = $1`, testMessagingManageChannels); err != nil {
		t.Fatal(err)
	}
	if _, err := w.pool.Exec(ctx, `
		UPDATE app_workspace_role_capabilities SET retired_at = NULL
		WHERE capability_ref = $1`, testMessagingManageChannels); err == nil {
		t.Fatal("database allowed a retired capability identity to reactivate")
	}
	roles, err := w.store.Roles(ctx, created.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	if len(roles) != 1 || len(roles[0].CapabilityRefs()) != 1 ||
		roles[0].CapabilityRefs()[0] != testMessagingManageChannels ||
		roles[0].AppCapabilities[testMessagingManageChannels] {
		t.Fatalf("retired capability display/effect projection = %#v", roles)
	}
	permissions, err := w.store.PermissionsFor(ctx, created.WorkspaceID, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	if permissions.Can(testMessagingManageChannels) {
		t.Fatal("retired capability remained effective")
	}

	position := 41
	updated, err := w.store.UpdateRoleWithPosition(ctx, created.WorkspaceID, role.RoleID,
		w.humanA, "Renamed channel manager", "#123456",
		map[string]bool{testMessagingManageChannels: true}, &position)
	if err != nil {
		t.Fatalf("round-trip retired capability during presentation update: %v", err)
	}
	if updated.Name != "Renamed channel manager" || updated.Color != "#123456" ||
		updated.Position != position || updated.AppCapabilities[testMessagingManageChannels] {
		t.Fatalf("retired capability round-trip projection = %#v", updated)
	}
	var preservedCapabilityID, preservedSnapshot string
	if err := w.pool.QueryRow(ctx, `
		SELECT capability_id, capability_ref_snapshot
		FROM workspace_role_app_capability_grants
		WHERE workspace_id = $1 AND role_id = $2`,
		created.WorkspaceID, role.RoleID,
	).Scan(&preservedCapabilityID, &preservedSnapshot); err != nil {
		t.Fatal(err)
	}
	if preservedCapabilityID != originalCapabilityID || preservedSnapshot != originalSnapshot {
		t.Fatalf("retired capability identity changed on round-trip: got (%s, %s), want (%s, %s)",
			preservedCapabilityID, preservedSnapshot, originalCapabilityID, originalSnapshot)
	}
	permissions, err = w.store.PermissionsFor(ctx, created.WorkspaceID, w.humanB)
	if err != nil || permissions.Can(testMessagingManageChannels) {
		t.Fatalf("round-trip resurrected retired authority = %#v, %v", permissions, err)
	}

	blank, err := w.store.CreateRole(ctx, created.WorkspaceID, w.humanA,
		"No app capability", "", map[string]bool{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.UpdateRole(ctx, created.WorkspaceID, blank.RoleID, w.humanA,
		blank.Name, blank.Color, map[string]bool{testMessagingManageChannels: true}); !errors.Is(err, ErrInvalidPermission) {
		t.Fatalf("new grant of retired capability error = %v", err)
	}
	if _, err := w.store.UpdateRole(ctx, created.WorkspaceID, blank.RoleID, w.humanA,
		blank.Name, blank.Color, map[string]bool{"app.messaging.unknown": true}); !errors.Is(err, ErrInvalidPermission) {
		t.Fatalf("new grant of unknown capability error = %v", err)
	}

	const replacementID = "0198f0f4-9b72-7000-8000-0000000008c2"
	if _, err := w.pool.Exec(ctx, `
		INSERT INTO app_workspace_role_capabilities
			(capability_id, app_id, capability_ref, label)
		VALUES ($1, 'messaging', $2, 'Manage channels again')`,
		replacementID, testMessagingManageChannels); err != nil {
		t.Fatal(err)
	}
	permissions, err = w.store.PermissionsFor(ctx, created.WorkspaceID, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	if permissions.Can(testMessagingManageChannels) {
		t.Fatal("same-spelling replacement silently resurrected historical authority")
	}

	tx, err := w.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = w.store.LockAndRequireAppCapability(ctx, tx, created.WorkspaceID,
		w.humanB, testMessagingManageChannels)
	_ = tx.Rollback(ctx)
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("historical holder admitted through replacement capability = %v", err)
	}

	updated, err = w.store.UpdateRole(ctx, created.WorkspaceID, role.RoleID,
		w.humanA, "Still historical", updated.Color, map[string]bool{testMessagingManageChannels: true})
	if err != nil {
		t.Fatal(err)
	}
	if updated.AppCapabilities[testMessagingManageChannels] {
		t.Fatalf("same-spelling replacement resurrected during round-trip: %#v", updated)
	}
	permissions, err = w.store.PermissionsFor(ctx, created.WorkspaceID, w.humanB)
	if err != nil || permissions.Can(testMessagingManageChannels) {
		t.Fatalf("same-spelling round-trip rebound replacement = %#v, %v", permissions, err)
	}

	updated, err = w.store.UpdateRole(ctx, created.WorkspaceID, role.RoleID,
		w.humanA, updated.Name, updated.Color, map[string]bool{})
	if err != nil {
		t.Fatalf("remove retired capability: %v", err)
	}
	updated, err = w.store.UpdateRole(ctx, created.WorkspaceID, role.RoleID,
		w.humanA, updated.Name, updated.Color, map[string]bool{testMessagingManageChannels: true})
	if err != nil {
		t.Fatalf("grant active replacement after explicit removal: %v", err)
	}
	if !updated.AppCapabilities[testMessagingManageChannels] {
		t.Fatalf("new grant did not bind active replacement identity: %#v", updated)
	}
	var reboundCapabilityID string
	if err := w.pool.QueryRow(ctx, `
		SELECT capability_id FROM workspace_role_app_capability_grants
		WHERE workspace_id = $1 AND role_id = $2`,
		created.WorkspaceID, role.RoleID,
	).Scan(&reboundCapabilityID); err != nil {
		t.Fatal(err)
	}
	if reboundCapabilityID != replacementID {
		t.Fatalf("new grant capability identity = %s, want %s", reboundCapabilityID, replacementID)
	}
	permissions, err = w.store.PermissionsFor(ctx, created.WorkspaceID, w.humanB)
	if err != nil || !permissions.Can(testMessagingManageChannels) {
		t.Fatalf("explicit replacement grant = %#v, %v", permissions, err)
	}
}

func TestRoleUpdateResolutionPinsCapabilityLifecycle(t *testing.T) {
	w := newTestWorld(t)
	ctx := context.Background()
	created, err := w.store.CreateWorkspace(ctx, "capability lifecycle lock", w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	role, err := w.store.CreateRole(ctx, created.WorkspaceID, w.humanA,
		"Channel manager", "", map[string]bool{testMessagingManageChannels: true})
	if err != nil {
		t.Fatal(err)
	}

	tx, err := w.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockWorkspace(ctx, tx, created.WorkspaceID); err != nil {
		t.Fatal(err)
	}
	previous, err := roleByID(ctx, tx, created.WorkspaceID, role.RoleID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolveRoleCapabilitiesForUpdate(ctx, tx, previous,
		map[string]bool{testMessagingManageChannels: true}); err != nil {
		t.Fatal(err)
	}

	retireCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	_, err = w.pool.Exec(retireCtx, `
		UPDATE app_workspace_role_capabilities
		SET retired_at = created_at + interval '1 second'
		WHERE capability_ref = $1`, testMessagingManageChannels)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("capability retirement crossed update resolution lock: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := w.pool.Exec(ctx, `
		UPDATE app_workspace_role_capabilities
		SET retired_at = created_at + interval '1 second'
		WHERE capability_ref = $1`, testMessagingManageChannels); err != nil {
		t.Fatalf("retire capability after update transaction ended: %v", err)
	}
}

func tableCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool) [2]int {
	t.Helper()
	var counts [2]int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM workspaces").Scan(&counts[0]); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM workspace_members").Scan(&counts[1]); err != nil {
		t.Fatal(err)
	}
	return counts
}
