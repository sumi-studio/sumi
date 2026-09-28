package workspace

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

func TestTargetedInvitationListAppliesExactTargetAndCurrentAdmissionTruth(t *testing.T) {
	w := newTestWorld(t)
	ctx := context.Background()
	seedHumanEmployer(t, ctx, w, w.humanA, w.agentA)
	seedHumanEmployer(t, ctx, w, w.humanB, w.agentB)
	authority := koseki.New(w.pool)

	issue := func(name string, issuer, target participant.Ref) (Workspace, InviteRecord) {
		t.Helper()
		created, err := w.store.CreateWorkspace(ctx, name, issuer)
		if err != nil {
			t.Fatal(err)
		}
		invitation, wasCreated, err := w.store.CreateCurrentAgentInvite(
			ctx,
			created.WorkspaceID,
			issuer,
			target,
			authority,
		)
		if err != nil || !wasCreated {
			t.Fatalf("issue %s = %#v created=%v err=%v", name, invitation, wasCreated, err)
		}
		return created, invitation
	}

	eligibleWorkspace, eligible := issue("eligible after employment transfer", w.humanA, w.agentA)
	_, wrongTarget := issue("other PersonalityAgent", w.humanB, w.agentB)
	_, expired := issue("expired target", w.humanA, w.agentA)
	if _, err := w.pool.Exec(ctx,
		"UPDATE workspace_invites SET expires_at = now() - interval '1 second' WHERE invite_id = $1",
		expired.InviteID,
	); err != nil {
		t.Fatal(err)
	}
	revokedWorkspace, revoked := issue("revoked target", w.humanA, w.agentA)
	if err := w.store.RevokeInvite(ctx, revokedWorkspace.WorkspaceID, revoked.InviteID, w.humanA); err != nil {
		t.Fatal(err)
	}
	activeWorkspace, active := issue("already active target", w.humanA, w.agentA)
	if _, err := w.pool.Exec(ctx, `
		INSERT INTO workspace_members
			(workspace_member_id, workspace_id, member_kind, member_id)
		VALUES ($1, $2, 'personality_agent', $3)`,
		newUUIDv7(), activeWorkspace.WorkspaceID, w.agentA.ID,
	); err != nil {
		t.Fatal(err)
	}

	delegatedWorkspace, err := w.store.CreateWorkspace(ctx, "issuer authority removed", w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	join, err := w.store.CreateInvite(ctx, delegatedWorkspace.WorkspaceID, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	issuerTenure, err := w.store.RedeemInvite(ctx, join.Code, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := w.store.CreateRole(
		ctx,
		delegatedWorkspace.WorkspaceID,
		w.humanB,
		"Invitation manager",
		"",
		map[string]bool{PermissionManageMembers: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.SetMembershipRoles(
		ctx,
		delegatedWorkspace.WorkspaceID,
		issuerTenure.WorkspaceMemberID,
		w.humanB,
		[]string{manager.RoleID},
	); err != nil {
		t.Fatal(err)
	}
	unauthorized, wasCreated, err := w.store.CreateCurrentAgentInvite(
		ctx,
		delegatedWorkspace.WorkspaceID,
		w.humanA,
		w.agentA,
		authority,
	)
	if err != nil || !wasCreated {
		t.Fatalf("delegated issuance = %#v created=%v err=%v", unauthorized, wasCreated, err)
	}
	if _, err := w.store.SetMembershipRoles(
		ctx,
		delegatedWorkspace.WorkspaceID,
		issuerTenure.WorkspaceMemberID,
		w.humanB,
		nil,
	); err != nil {
		t.Fatal(err)
	}

	// Acceptance and listing depend on the durable invitation, not on a later
	// employment or app-installation snapshot. No app is installed in this
	// fixture, and the Human Employer changes after all target-A issuance.
	if _, err := w.pool.Exec(ctx, `
		UPDATE employments SET ended_at = now()
		WHERE agent_id = $1 AND ended_at IS NULL`, w.agentA.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := w.pool.Exec(ctx, `
		INSERT INTO employments (agent_id, employer_type, employer_id)
		VALUES ($1, 'human', $2)`, w.agentA.ID, w.humanB.ID); err != nil {
		t.Fatal(err)
	}

	page, err := w.store.targetedInvitationPageFor(ctx, w.pool, w.agentA, nil)
	if err != nil {
		t.Fatal(err)
	}
	if page.HasMore || len(page.Items) != 1 {
		t.Fatalf("target-A invitation page = %#v", page)
	}
	listed := page.Items[0].Invitation
	if listed.InvitationID != eligible.InviteID ||
		listed.WorkspaceID != eligibleWorkspace.WorkspaceID ||
		listed.WorkspaceName != eligibleWorkspace.Name {
		t.Fatalf("eligible invitation = %#v, want %#v / %#v", listed, eligible, eligibleWorkspace)
	}
	for _, hidden := range []string{wrongTarget.InviteID, expired.InviteID, revoked.InviteID, active.InviteID, unauthorized.InviteID} {
		if listed.InvitationID == hidden {
			t.Fatalf("ineligible invitation %s was exposed", hidden)
		}
	}
	otherPage, err := w.store.targetedInvitationPageFor(ctx, w.pool, w.agentB, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(otherPage.Items) != 1 || otherPage.Items[0].Invitation.InvitationID != wrongTarget.InviteID {
		t.Fatalf("target-B invitation page = %#v", otherPage)
	}
}

func TestTargetedInvitationAcceptanceIsExactIdempotentAndReturnsClosedTenure(t *testing.T) {
	w := newTestWorld(t)
	ctx := context.Background()
	seedHumanEmployer(t, ctx, w, w.humanA, w.agentA)
	authority := koseki.New(w.pool)
	created, err := w.store.CreateWorkspace(ctx, "accept exact invitation", w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	invitation, wasCreated, err := w.store.CreateCurrentAgentInvite(
		ctx,
		created.WorkspaceID,
		w.humanA,
		w.agentA,
		authority,
	)
	if err != nil || !wasCreated {
		t.Fatalf("issue invitation = %#v created=%v err=%v", invitation, wasCreated, err)
	}

	if _, err := w.store.AcceptTargetedInvitation(ctx, invitation.InviteID, w.agentB); !errors.Is(err, ErrInviteUnavailable) {
		t.Fatalf("wrong target acceptance = %v", err)
	}
	var beforeRedeemedAt *time.Time
	if err := w.pool.QueryRow(ctx,
		"SELECT redeemed_at FROM workspace_invites WHERE invite_id = $1",
		invitation.InviteID,
	).Scan(&beforeRedeemedAt); err != nil {
		t.Fatal(err)
	}
	if beforeRedeemedAt != nil {
		t.Fatal("wrong target consumed invitation")
	}

	// The invitation remains sufficient after the Human Employer changes and
	// with no app installation present.
	if _, err := w.pool.Exec(ctx, `
		UPDATE employments SET ended_at = now()
		WHERE agent_id = $1 AND ended_at IS NULL`, w.agentA.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := w.pool.Exec(ctx, `
		INSERT INTO employments (agent_id, employer_type, employer_id)
		VALUES ($1, 'human', $2)`, w.agentA.ID, w.humanB.ID); err != nil {
		t.Fatal(err)
	}
	membership, err := w.store.AcceptTargetedInvitation(ctx, invitation.InviteID, w.agentA)
	if err != nil {
		t.Fatal(err)
	}
	if membership.WorkspaceID != created.WorkspaceID || membership.Participant != w.agentA ||
		membership.DisplayName != "Kuro" || membership.LeftAt != nil || len(membership.RoleIDs) != 0 {
		t.Fatalf("accepted membership = %#v", membership)
	}
	workspaces, err := w.store.WorkspacesFor(ctx, w.agentA)
	if err != nil {
		t.Fatal(err)
	}
	if len(workspaces) != 1 || workspaces[0].WorkspaceID != created.WorkspaceID {
		t.Fatalf("workspace list after acceptance = %#v", workspaces)
	}
	var pendingForTarget int
	if err := w.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM workspace_invites
		WHERE workspace_id = $1
		  AND invite_kind = 'targeted_personality_agent'
		  AND target_id = $2
		  AND revoked_at IS NULL
		  AND redeemed_at IS NULL`, created.WorkspaceID, w.agentA.ID,
	).Scan(&pendingForTarget); err != nil {
		t.Fatal(err)
	}
	if pendingForTarget != 0 {
		t.Fatalf("acceptance left %d pending invitations for the target", pendingForTarget)
	}

	replayed, err := w.store.AcceptTargetedInvitation(ctx, invitation.InviteID, w.agentA)
	if err != nil || replayed.WorkspaceMemberID != membership.WorkspaceMemberID || replayed.LeftAt != nil {
		t.Fatalf("active acceptance replay = %#v, %v", replayed, err)
	}
	if err := w.store.RemoveMember(ctx, created.WorkspaceID, membership.WorkspaceMemberID, w.humanA); err != nil {
		t.Fatal(err)
	}
	closed, err := w.store.AcceptTargetedInvitation(ctx, invitation.InviteID, w.agentA)
	if err != nil {
		t.Fatal(err)
	}
	if closed.WorkspaceMemberID != membership.WorkspaceMemberID || closed.LeftAt == nil {
		t.Fatalf("closed acceptance replay = %#v", closed)
	}
	workspaces, err = w.store.WorkspacesFor(ctx, w.agentA)
	if err != nil {
		t.Fatal(err)
	}
	if len(workspaces) != 0 {
		t.Fatalf("closed tenure remained in workspace list: %#v", workspaces)
	}
}

func TestTargetedInvitationAcceptanceRejectsCurrentInvalidationWithoutConsumption(t *testing.T) {
	w := newTestWorld(t)
	ctx := context.Background()
	seedHumanEmployer(t, ctx, w, w.humanA, w.agentA)
	authority := koseki.New(w.pool)

	issue := func(name string) (Workspace, InviteRecord) {
		t.Helper()
		created, err := w.store.CreateWorkspace(ctx, name, w.humanA)
		if err != nil {
			t.Fatal(err)
		}
		invitation, wasCreated, err := w.store.CreateCurrentAgentInvite(
			ctx, created.WorkspaceID, w.humanA, w.agentA, authority,
		)
		if err != nil || !wasCreated {
			t.Fatalf("issue %s: created=%v err=%v", name, wasCreated, err)
		}
		return created, invitation
	}
	assertPending := func(invitationID string) {
		t.Helper()
		var redeemedAt, revokedAt *time.Time
		if err := w.pool.QueryRow(ctx, `
			SELECT redeemed_at, revoked_at
			FROM workspace_invites WHERE invite_id = $1`, invitationID,
		).Scan(&redeemedAt, &revokedAt); err != nil {
			t.Fatal(err)
		}
		if redeemedAt != nil || revokedAt != nil {
			t.Fatalf("invitation %s changed: redeemed=%v revoked=%v", invitationID, redeemedAt, revokedAt)
		}
	}

	revokedWorkspace, revoked := issue("revoked acceptance")
	if err := w.store.RevokeInvite(ctx, revokedWorkspace.WorkspaceID, revoked.InviteID, w.humanA); err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.AcceptTargetedInvitation(ctx, revoked.InviteID, w.agentA); !errors.Is(err, ErrInviteUnavailable) {
		t.Fatalf("revoked acceptance = %v", err)
	}

	_, expired := issue("expired acceptance")
	if _, err := w.pool.Exec(ctx,
		"UPDATE workspace_invites SET expires_at = now() - interval '1 second' WHERE invite_id = $1",
		expired.InviteID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.AcceptTargetedInvitation(ctx, expired.InviteID, w.agentA); !errors.Is(err, ErrInviteUnavailable) {
		t.Fatalf("expired acceptance = %v", err)
	}

	activeWorkspace, active := issue("active conflict")
	if _, err := w.pool.Exec(ctx, `
		INSERT INTO workspace_members
			(workspace_member_id, workspace_id, member_kind, member_id)
		VALUES ($1, $2, 'personality_agent', $3)`,
		newUUIDv7(), activeWorkspace.WorkspaceID, w.agentA.ID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.AcceptTargetedInvitation(ctx, active.InviteID, w.agentA); !errors.Is(err, ErrAlreadyMember) {
		t.Fatalf("active membership acceptance = %v", err)
	}
	assertPending(active.InviteID)

	delegatedWorkspace, err := w.store.CreateWorkspace(ctx, "issuer lost permission", w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	join, err := w.store.CreateInvite(ctx, delegatedWorkspace.WorkspaceID, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := w.store.RedeemInvite(ctx, join.Code, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := w.store.CreateRole(
		ctx,
		delegatedWorkspace.WorkspaceID,
		w.humanB,
		"Temporary inviter",
		"",
		map[string]bool{PermissionManageMembers: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.SetMembershipRoles(
		ctx, delegatedWorkspace.WorkspaceID, issuer.WorkspaceMemberID, w.humanB, []string{manager.RoleID},
	); err != nil {
		t.Fatal(err)
	}
	unauthorized, wasCreated, err := w.store.CreateCurrentAgentInvite(
		ctx, delegatedWorkspace.WorkspaceID, w.humanA, w.agentA, authority,
	)
	if err != nil || !wasCreated {
		t.Fatalf("delegated issue: created=%v err=%v", wasCreated, err)
	}
	if _, err := w.store.SetMembershipRoles(
		ctx, delegatedWorkspace.WorkspaceID, issuer.WorkspaceMemberID, w.humanB, nil,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.AcceptTargetedInvitation(ctx, unauthorized.InviteID, w.agentA); !errors.Is(err, ErrInviteUnavailable) {
		t.Fatalf("issuer-authority acceptance = %v", err)
	}
	assertPending(unauthorized.InviteID)

	closedIssuerWorkspace, err := w.store.CreateWorkspace(ctx, "issuer tenure closed", w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	closedIssuerJoin, err := w.store.CreateInvite(ctx, closedIssuerWorkspace.WorkspaceID, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	closedIssuer, err := w.store.RedeemInvite(ctx, closedIssuerJoin.Code, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	closedIssuerRole, err := w.store.CreateRole(
		ctx,
		closedIssuerWorkspace.WorkspaceID,
		w.humanB,
		"Exact-tenure inviter",
		"",
		map[string]bool{PermissionManageMembers: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.SetMembershipRoles(
		ctx,
		closedIssuerWorkspace.WorkspaceID,
		closedIssuer.WorkspaceMemberID,
		w.humanB,
		[]string{closedIssuerRole.RoleID},
	); err != nil {
		t.Fatal(err)
	}
	closedTenureInvitation, wasCreated, err := w.store.CreateCurrentAgentInvite(
		ctx,
		closedIssuerWorkspace.WorkspaceID,
		w.humanA,
		w.agentA,
		authority,
	)
	if err != nil || !wasCreated {
		t.Fatalf("closed-tenure issue: created=%v err=%v", wasCreated, err)
	}
	if err := w.store.RemoveMember(
		ctx,
		closedIssuerWorkspace.WorkspaceID,
		closedIssuer.WorkspaceMemberID,
		w.humanB,
	); err != nil {
		t.Fatal(err)
	}
	rejoin, err := w.store.CreateInvite(ctx, closedIssuerWorkspace.WorkspaceID, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	rejoinedIssuer, err := w.store.RedeemInvite(ctx, rejoin.Code, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.SetMembershipRoles(
		ctx,
		closedIssuerWorkspace.WorkspaceID,
		rejoinedIssuer.WorkspaceMemberID,
		w.humanB,
		[]string{closedIssuerRole.RoleID},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.AcceptTargetedInvitation(
		ctx,
		closedTenureInvitation.InviteID,
		w.agentA,
	); !errors.Is(err, ErrInviteUnavailable) {
		t.Fatalf("successor issuer tenure revived invitation: %v", err)
	}
	assertPending(closedTenureInvitation.InviteID)
}

func TestTargetedInvitationAcceptanceSerializesSameTargetRetries(t *testing.T) {
	w := newTestWorld(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	seedHumanEmployer(t, ctx, w, w.humanA, w.agentA)
	created, err := w.store.CreateWorkspace(ctx, "concurrent targeted acceptance", w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	invitation, wasCreated, err := w.store.CreateCurrentAgentInvite(
		ctx,
		created.WorkspaceID,
		w.humanA,
		w.agentA,
		koseki.New(w.pool),
	)
	if err != nil || !wasCreated {
		t.Fatalf("issue invitation: created=%v err=%v", wasCreated, err)
	}

	const retries = 8
	results := make(chan struct {
		membership Membership
		err        error
	}, retries)
	var wait sync.WaitGroup
	for range retries {
		wait.Add(1)
		go func() {
			defer wait.Done()
			membership, err := w.store.AcceptTargetedInvitation(ctx, invitation.InviteID, w.agentA)
			results <- struct {
				membership Membership
				err        error
			}{membership, err}
		}()
	}
	wait.Wait()
	close(results)

	var membershipID string
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent acceptance = %v", result.err)
		}
		if membershipID == "" {
			membershipID = result.membership.WorkspaceMemberID
		}
		if result.membership.WorkspaceMemberID != membershipID {
			t.Fatalf("multiple tenures returned: %s / %s", membershipID, result.membership.WorkspaceMemberID)
		}
	}
	var active int
	if err := w.pool.QueryRow(ctx, `
		SELECT count(*) FROM workspace_members
		WHERE workspace_id = $1 AND member_kind = 'personality_agent'
		  AND member_id = $2 AND left_at IS NULL`, created.WorkspaceID, w.agentA.ID,
	).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 1 {
		t.Fatalf("active membership tenures = %d, want 1", active)
	}
}

func TestTargetedInvitationAcceptanceSerializesWithRevocation(t *testing.T) {
	w := newTestWorld(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	seedHumanEmployer(t, ctx, w, w.humanA, w.agentA)
	authority := koseki.New(w.pool)

	for attempt := range 6 {
		created, err := w.store.CreateWorkspace(
			ctx,
			fmt.Sprintf("accept-revoke race %d", attempt),
			w.humanA,
		)
		if err != nil {
			t.Fatal(err)
		}
		invitation, wasCreated, err := w.store.CreateCurrentAgentInvite(
			ctx,
			created.WorkspaceID,
			w.humanA,
			w.agentA,
			authority,
		)
		if err != nil || !wasCreated {
			t.Fatalf("issue race invitation: created=%v err=%v", wasCreated, err)
		}

		start := make(chan struct{})
		acceptDone := make(chan struct {
			membership Membership
			err        error
		}, 1)
		revokeDone := make(chan error, 1)
		go func() {
			<-start
			membership, err := w.store.AcceptTargetedInvitation(
				ctx,
				invitation.InviteID,
				w.agentA,
			)
			acceptDone <- struct {
				membership Membership
				err        error
			}{membership, err}
		}()
		go func() {
			<-start
			revokeDone <- w.store.RevokeInvite(
				ctx,
				created.WorkspaceID,
				invitation.InviteID,
				w.humanA,
			)
		}()
		close(start)
		accepted := <-acceptDone
		if err := <-revokeDone; err != nil {
			t.Fatalf("concurrent revocation = %v", err)
		}

		var (
			redeemedMembershipID *string
			redeemedAt           *time.Time
			revokedAt            *time.Time
			membershipCount      int
		)
		if err := w.pool.QueryRow(ctx, `
			SELECT redeemed_workspace_member_id, redeemed_at, revoked_at
			FROM workspace_invites
			WHERE invite_id = $1`, invitation.InviteID,
		).Scan(&redeemedMembershipID, &redeemedAt, &revokedAt); err != nil {
			t.Fatal(err)
		}
		if err := w.pool.QueryRow(ctx, `
			SELECT count(*)
			FROM workspace_members
			WHERE workspace_id = $1
			  AND member_kind = 'personality_agent'
			  AND member_id = $2`, created.WorkspaceID, w.agentA.ID,
		).Scan(&membershipCount); err != nil {
			t.Fatal(err)
		}
		if revokedAt == nil {
			t.Fatal("concurrent revocation did not durably close the invitation")
		}
		if accepted.err == nil {
			if redeemedAt == nil || redeemedMembershipID == nil ||
				*redeemedMembershipID != accepted.membership.WorkspaceMemberID || membershipCount != 1 {
				t.Fatalf(
					"accepted race left partial ledger: result=%#v redeemed=%v/%v memberships=%d",
					accepted.membership,
					redeemedMembershipID,
					redeemedAt,
					membershipCount,
				)
			}
			replayed, err := w.store.AcceptTargetedInvitation(
				ctx,
				invitation.InviteID,
				w.agentA,
			)
			if err != nil || replayed.WorkspaceMemberID != accepted.membership.WorkspaceMemberID {
				t.Fatalf("accepted-then-revoked retry = %#v, %v", replayed, err)
			}
		} else {
			if !errors.Is(accepted.err, ErrInviteUnavailable) {
				t.Fatalf("revocation race acceptance = %v", accepted.err)
			}
			if redeemedAt != nil || redeemedMembershipID != nil || membershipCount != 0 {
				t.Fatalf(
					"revoked race left partial admission: redeemed=%v/%v memberships=%d",
					redeemedMembershipID,
					redeemedAt,
					membershipCount,
				)
			}
		}
	}
}
