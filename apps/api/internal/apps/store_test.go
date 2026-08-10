package apps_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
	"github.com/sumi-studio/sumi/apps/api/internal/workspace"
)

const (
	testHumanOwner  = "0198f0f4-9b72-7000-8000-000000000201"
	testHumanMember = "0198f0f4-9b72-7000-8000-000000000202"
	testAgentOwner  = "0198f0f4-9b72-7000-8000-0000000002a1"
)

type appWorld struct {
	pool       *pgxpool.Pool
	workspaces *workspace.Store
	apps       *applicationapps.Store
	owner      participant.Ref
	member     participant.Ref
	agent      participant.Ref
}

func newAppWorld(t *testing.T) appWorld {
	t.Helper()
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	for _, humanID := range []string{testHumanOwner, testHumanMember} {
		if _, err := pool.Exec(ctx, "INSERT INTO humans (human_id) VALUES ($1)", humanID); err != nil {
			t.Fatalf("insert Human: %v", err)
		}
	}
	if _, err := pool.Exec(ctx,
		"INSERT INTO agents (personality_agent_id, human_id) VALUES ($1, $2)",
		testAgentOwner, testHumanOwner); err != nil {
		t.Fatalf("insert PersonalityAgent: %v", err)
	}
	workspaces := workspace.New(pool)
	return appWorld{
		pool: pool, workspaces: workspaces, apps: applicationapps.New(pool, workspaces),
		owner:  participant.Human(testHumanOwner),
		member: participant.Human(testHumanMember),
		agent:  participant.PersonalityAgent(testAgentOwner),
	}
}

func TestInstallationLifecycleAuthorizesOwnerAndPreservesAppData(t *testing.T) {
	w := newAppWorld(t)
	ctx := context.Background()
	created, err := w.workspaces.CreateWorkspace(ctx, "apps", w.owner)
	if err != nil {
		t.Fatal(err)
	}
	workspaceOwner := applicationapps.WorkspaceOwner(created.WorkspaceID)

	installed, err := w.apps.Install(ctx, workspaceOwner, w.owner, "messaging")
	if err != nil {
		t.Fatalf("install Messaging: %v", err)
	}
	if installed.State != applicationapps.StateEnabled {
		t.Fatalf("initial state = %q", installed.State)
	}
	projected, err := w.apps.RequireEnabledInstallation(ctx,
		installed.InstallationID, workspaceOwner, "messaging")
	if err != nil || projected.InstallationID != installed.InstallationID {
		t.Fatalf("exact enabled projection = %#v, %v", projected, err)
	}
	if _, err := w.apps.SetEnabledByID(ctx, installed.InstallationID, w.owner, false); err != nil {
		t.Fatalf("disable Messaging: %v", err)
	}
	list, err := w.apps.Installations(ctx, workspaceOwner, w.owner)
	if err != nil || len(list) != 1 || list[0].State != applicationapps.StateDisabled {
		t.Fatalf("disabled list = %#v, %v", list, err)
	}
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.apps.RequireEnabledInstallationInTx(ctx, tx,
		installed.InstallationID, workspaceOwner, "messaging"); !errors.Is(err, applicationapps.ErrAppDisabled) {
		_ = tx.Rollback(ctx)
		t.Fatalf("disabled commit admission = %v", err)
	}
	_ = tx.Rollback(ctx)
	if _, err := w.apps.SetEnabledByID(ctx, installed.InstallationID, w.owner, true); err != nil {
		t.Fatalf("re-enable Messaging: %v", err)
	}

	// Messaging data is owned by Messaging and references Workspace, never the
	// installation row. Uninstall must remove only the binding.
	const placeID = "0198f0f4-9b72-7000-8000-0000000002aa"
	if _, err := w.pool.Exec(ctx, `
		INSERT INTO places (place_id, kind, workspace_id, name)
		VALUES ($1, 'channel', $2, 'preserved')`, placeID, created.WorkspaceID); err != nil {
		t.Fatal(err)
	}
	if err := w.apps.UninstallByID(ctx, installed.InstallationID, w.owner); err != nil {
		t.Fatalf("uninstall Messaging: %v", err)
	}
	var preserved bool
	if err := w.pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM places WHERE place_id = $1)", placeID,
	).Scan(&preserved); err != nil {
		t.Fatal(err)
	}
	if !preserved {
		t.Fatal("uninstall deleted app-owned Messaging data")
	}
	if _, err := w.apps.RequireEnabledInstallation(ctx,
		installed.InstallationID, workspaceOwner, "messaging"); !errors.Is(err, applicationapps.ErrInstallationNotFound) {
		t.Fatalf("uninstalled exact id admission = %v", err)
	}
	list, err = w.apps.Installations(ctx, workspaceOwner, w.owner)
	if err != nil || len(list) != 0 {
		t.Fatalf("post-uninstall list = %#v, %v", list, err)
	}
	reinstalled, err := w.apps.Install(ctx, workspaceOwner, w.owner, "messaging")
	if err != nil {
		t.Fatalf("reinstall Messaging: %v", err)
	}
	if reinstalled.InstallationID == installed.InstallationID {
		t.Fatal("reinstall reused installation identity")
	}
	if _, err := w.apps.RequireEnabledInstallation(ctx,
		installed.InstallationID, workspaceOwner, "messaging"); !errors.Is(err, applicationapps.ErrInstallationNotFound) {
		t.Fatalf("stale pre-uninstall id authorized reinstall: %v", err)
	}
	if exact, err := w.apps.RequireEnabledInstallation(ctx,
		reinstalled.InstallationID, workspaceOwner, "messaging"); err != nil || exact.InstallationID != reinstalled.InstallationID {
		t.Fatalf("reinstalled exact admission = %#v, %v", exact, err)
	}
}

func TestExactInstallationAdmissionRejectsOwnerAndAppSubstitution(t *testing.T) {
	w := newAppWorld(t)
	ctx := context.Background()
	first, err := w.workspaces.CreateWorkspace(ctx, "first", w.owner)
	if err != nil {
		t.Fatal(err)
	}
	second, err := w.workspaces.CreateWorkspace(ctx, "second", w.owner)
	if err != nil {
		t.Fatal(err)
	}
	firstOwner := applicationapps.WorkspaceOwner(first.WorkspaceID)
	installed, err := w.apps.Install(ctx, firstOwner, w.owner, "messaging")
	if err != nil {
		t.Fatal(err)
	}
	for name, owner := range map[string]applicationapps.OwnerRef{
		"other Workspace": applicationapps.WorkspaceOwner(second.WorkspaceID),
		"Participant":     applicationapps.ParticipantOwner(w.owner),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := w.apps.RequireEnabledInstallation(ctx,
				installed.InstallationID, owner, "messaging"); !errors.Is(err, applicationapps.ErrInstallationNotFound) {
				t.Fatalf("substituted owner admission = %v", err)
			}
		})
	}
	if _, err := w.apps.RequireEnabledInstallation(ctx,
		installed.InstallationID, firstOwner, "alarm"); !errors.Is(err, applicationapps.ErrInstallationNotFound) {
		t.Fatalf("substituted app admission = %v", err)
	}
}

func TestParticipantOwnerRoundTripPreservesNestedAgentKind(t *testing.T) {
	w := newAppWorld(t)
	ctx := context.Background()
	owner := applicationapps.ParticipantOwner(w.agent)
	installed, err := w.apps.Install(ctx, owner, w.agent, "alarm")
	if err != nil {
		t.Fatal(err)
	}
	if installed.Owner != owner {
		t.Fatalf("installed owner = %#v, want %#v", installed.Owner, owner)
	}
	listed, err := w.apps.Installations(ctx, owner, w.agent)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].Owner != owner {
		t.Fatalf("listed nested Participant owner = %#v", listed)
	}
	disabled, err := w.apps.SetEnabledByID(ctx, installed.InstallationID, w.agent, false)
	if err != nil {
		t.Fatal(err)
	}
	if disabled.Owner != owner || disabled.State != applicationapps.StateDisabled {
		t.Fatalf("disabled nested Participant owner = %#v", disabled)
	}
	if _, err := w.pool.Exec(ctx, `
		UPDATE app_installations SET owner_id = $2
		WHERE installation_id = $1`, installed.InstallationID, w.member.ID); err == nil {
		t.Fatal("database allowed an installation_id to move to another owner")
	}
	if err := w.apps.UninstallByID(ctx, installed.InstallationID, w.agent); err != nil {
		t.Fatal(err)
	}
}

func TestParticipantAndWorkspaceOwnerRulesUseOneLifecycle(t *testing.T) {
	w := newAppWorld(t)
	ctx := context.Background()
	created, err := w.workspaces.CreateWorkspace(ctx, "owner rules", w.owner)
	if err != nil {
		t.Fatal(err)
	}
	invite, err := w.workspaces.CreateInvite(ctx, created.WorkspaceID, w.owner)
	if err != nil {
		t.Fatal(err)
	}
	membership, err := w.workspaces.RedeemInvite(ctx, invite.Code, w.member)
	if err != nil {
		t.Fatal(err)
	}
	workspaceOwner := applicationapps.WorkspaceOwner(created.WorkspaceID)
	if _, err := w.apps.Install(ctx, workspaceOwner, w.member, "messaging"); !errors.Is(err, workspace.ErrForbidden) {
		t.Fatalf("member without manage_apps error = %v", err)
	}
	appManager, err := w.workspaces.CreateRole(ctx, created.WorkspaceID, w.owner,
		"App manager", "", map[string]bool{workspace.PermissionManageApps: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.workspaces.SetMembershipRoles(ctx, created.WorkspaceID,
		membership.WorkspaceMemberID, w.owner, []string{appManager.RoleID}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.apps.Install(ctx, workspaceOwner, w.member, "messaging"); err != nil {
		t.Fatalf("member with manage_apps: %v", err)
	}

	personal := applicationapps.ParticipantOwner(w.member)
	if _, err := w.apps.Install(ctx, personal, w.member, "alarm"); err != nil {
		t.Fatalf("participant installs own Alarm: %v", err)
	}
	if _, err := w.apps.Install(ctx, personal, w.owner, "life-log"); !errors.Is(err, applicationapps.ErrForbidden) {
		t.Fatalf("other Human manages participant owner: %v", err)
	}
	if _, err := w.apps.Install(ctx, personal, w.member, "messaging"); !errors.Is(err, applicationapps.ErrOwnerKindUnsupported) {
		t.Fatalf("participant-scoped Messaging error = %v", err)
	}
	if _, err := w.apps.Install(ctx, workspaceOwner, w.owner, "alarm"); !errors.Is(err, applicationapps.ErrOwnerKindUnsupported) {
		t.Fatalf("workspace-scoped Alarm error = %v", err)
	}
}

func TestConcurrentInstallCreatesOneBinding(t *testing.T) {
	w := newAppWorld(t)
	ctx := context.Background()
	created, err := w.workspaces.CreateWorkspace(ctx, "race", w.owner)
	if err != nil {
		t.Fatal(err)
	}
	owner := applicationapps.WorkspaceOwner(created.WorkspaceID)
	const attempts = 8
	var wait sync.WaitGroup
	results := make(chan error, attempts)
	for range attempts {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := w.apps.Install(ctx, owner, w.owner, "messaging")
			results <- err
		}()
	}
	wait.Wait()
	close(results)
	createdCount := 0
	alreadyCount := 0
	for err := range results {
		switch {
		case err == nil:
			createdCount++
		case errors.Is(err, applicationapps.ErrAlreadyInstalled):
			alreadyCount++
		default:
			t.Fatalf("unexpected concurrent install error: %v", err)
		}
	}
	if createdCount != 1 || alreadyCount != attempts-1 {
		t.Fatalf("concurrent installs created=%d already=%d", createdCount, alreadyCount)
	}
	var rows int
	if err := w.pool.QueryRow(ctx, `
		SELECT count(*) FROM app_installations
		WHERE owner_kind = 'workspace' AND owner_id = $1 AND app_id = 'messaging'`,
		created.WorkspaceID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("installation rows = %d", rows)
	}
}
