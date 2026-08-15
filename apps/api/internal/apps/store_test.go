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
	"github.com/sumi-studio/sumi/apps/api/internal/directchat"
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

func TestDirectChatLifecycleUsesProcessFenceAndOtherAppsDoNot(t *testing.T) {
	w := newAppWorld(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	owner := applicationapps.ParticipantOwner(w.owner)
	withoutFence := applicationapps.New(w.pool, w.workspaces)
	if _, err := withoutFence.Install(ctx, owner, w.owner, directchat.AppID); !errors.Is(err, directchat.ErrLifecycleFenceUnavailable) {
		t.Fatalf("direct-chat install without lifecycle fence = %v", err)
	}

	fence := directchat.NewLifecycleFence()
	store := applicationapps.New(w.pool, w.workspaces, fence)
	releaseOperation, err := fence.AcquireOperation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	type installResult struct {
		installation applicationapps.Installation
		err          error
	}
	directChatDone := make(chan installResult, 1)
	go func() {
		installation, installErr := store.Install(ctx, owner, w.owner, directchat.AppID)
		directChatDone <- installResult{installation: installation, err: installErr}
	}()
	alarmDone := make(chan error, 1)
	go func() {
		_, installErr := store.Install(
			ctx,
			applicationapps.ParticipantOwner(w.member),
			w.member,
			"alarm",
		)
		alarmDone <- installErr
	}()
	select {
	case err := <-alarmDone:
		if err != nil {
			t.Fatalf("unrelated app serialized or failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("unrelated app was serialized behind direct-chat operation")
	}
	select {
	case result := <-directChatDone:
		t.Fatalf("direct-chat install crossed operation fence: %v", result.err)
	case <-time.After(30 * time.Millisecond):
	}
	releaseOperation()
	result := <-directChatDone
	if result.err != nil {
		t.Fatalf("direct-chat install after operation: %v", result.err)
	}

	assertMutationWaits := func(name string, mutate func() error) {
		t.Helper()
		release, acquireErr := fence.AcquireOperation(ctx)
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		done := make(chan error, 1)
		go func() { done <- mutate() }()
		select {
		case err := <-done:
			release()
			t.Fatalf("%s crossed operation fence: %v", name, err)
		case <-time.After(30 * time.Millisecond):
		}
		release()
		if err := <-done; err != nil {
			t.Fatalf("%s after operation: %v", name, err)
		}
	}
	assertMutationWaits("disable", func() error {
		_, err := store.SetEnabledByID(ctx, result.installation.InstallationID, w.owner, false)
		return err
	})
	assertMutationWaits("enable", func() error {
		_, err := store.SetEnabledByID(ctx, result.installation.InstallationID, w.owner, true)
		return err
	})
	assertMutationWaits("uninstall", func() error {
		return store.UninstallByID(ctx, result.installation.InstallationID, w.owner)
	})
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
	if installed.State != applicationapps.StateEnabled || installed.AuthorityEpoch != 1 {
		t.Fatalf("initial lifecycle = %#v", installed)
	}
	projected, err := w.apps.RequireEnabledInstallation(ctx,
		installed.InstallationID, workspaceOwner, "messaging")
	if err != nil || projected.InstallationID != installed.InstallationID ||
		projected.AuthorityEpoch != 1 {
		t.Fatalf("exact enabled projection = %#v, %v", projected, err)
	}
	resolved, err := w.apps.ResolveEnabledInstallation(ctx, workspaceOwner, w.owner, "messaging")
	if err != nil || resolved.InstallationID != installed.InstallationID {
		t.Fatalf("authenticated owner/app resolution = %#v, %v", resolved, err)
	}
	if _, err := w.apps.ResolveEnabledInstallation(ctx, workspaceOwner, w.member,
		"messaging"); !errors.Is(err, workspace.ErrNotFound) {
		t.Fatalf("non-member resolution error = %v", err)
	}
	disabled, err := w.apps.SetEnabledByID(ctx, installed.InstallationID, w.owner, false)
	if err != nil {
		t.Fatalf("disable Messaging: %v", err)
	}
	if disabled.AuthorityEpoch != 2 {
		t.Fatalf("disable authority epoch = %d, want 2", disabled.AuthorityEpoch)
	}
	disabledAgain, err := w.apps.SetEnabledByID(ctx, installed.InstallationID, w.owner, false)
	if err != nil {
		t.Fatalf("idempotent disable Messaging: %v", err)
	}
	if disabledAgain.AuthorityEpoch != 2 {
		t.Fatalf("idempotent disable churned authority epoch to %d", disabledAgain.AuthorityEpoch)
	}
	if _, err := w.apps.SetEnabledByIDAtEpoch(ctx, installed.InstallationID,
		w.owner, false, 1); !errors.Is(err, applicationapps.ErrAuthorityEpochStale) {
		t.Fatalf("stale lifecycle replay = %v, want stale authority", err)
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
	if _, err := w.apps.ResolveEnabledInstallation(ctx, workspaceOwner, w.owner,
		"messaging"); !errors.Is(err, applicationapps.ErrAppDisabled) {
		t.Fatalf("disabled bind-time resolution = %v", err)
	}
	reenabled, err := w.apps.SetEnabledByIDAtEpoch(ctx,
		installed.InstallationID, w.owner, true, 2)
	if err != nil {
		t.Fatalf("re-enable Messaging: %v", err)
	}
	if reenabled.AuthorityEpoch != 2 {
		t.Fatalf("re-enable authority epoch = %d, want 2", reenabled.AuthorityEpoch)
	}
	tx, err = w.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.apps.RequireEnabledInstallationEpochInTx(
		ctx, tx, installed.InstallationID, 1, workspaceOwner, "messaging",
	); !errors.Is(err, applicationapps.ErrInstallationNotFound) {
		_ = tx.Rollback(ctx)
		t.Fatalf("stale authority epoch admission = %v", err)
	}
	if _, err := w.apps.RequireEnabledInstallationEpochInTx(
		ctx, tx, installed.InstallationID, 2, workspaceOwner, "messaging",
	); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("current authority epoch admission = %v", err)
	}
	_ = tx.Rollback(ctx)

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

func TestExactLifecycleReplaysSerializeAtDatabaseBoundaries(t *testing.T) {
	w := newAppWorld(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	created, err := w.workspaces.CreateWorkspace(ctx, "replay", w.owner)
	if err != nil {
		t.Fatal(err)
	}
	owner := applicationapps.WorkspaceOwner(created.WorkspaceID)
	installed, err := w.apps.Install(ctx, owner, w.owner, "messaging")
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		installation applicationapps.Installation
		err          error
	}
	disableResults := make(chan result, 2)
	start := make(chan struct{})
	for range 2 {
		go func() {
			<-start
			item, setErr := w.apps.SetEnabledByIDAtEpoch(
				ctx, installed.InstallationID, w.owner, false, 1,
			)
			disableResults <- result{installation: item, err: setErr}
		}()
	}
	close(start)
	var committed, stale int
	for range 2 {
		item := <-disableResults
		switch {
		case item.err == nil:
			committed++
			if item.installation.State != applicationapps.StateDisabled ||
				item.installation.AuthorityEpoch != 2 {
				t.Fatalf("committed replay = %#v", item.installation)
			}
		case errors.Is(item.err, applicationapps.ErrAuthorityEpochStale):
			stale++
		default:
			t.Fatalf("disable replay = %v", item.err)
		}
	}
	if committed != 1 || stale != 1 {
		t.Fatalf("disable replay outcomes: committed=%d stale=%d", committed, stale)
	}

	// Enabling does not churn the authority epoch. Two exact replays therefore
	// both succeed, but the row lock still makes their shared desired truth
	// stable before either caller can perform its final read.
	enableResults := make(chan result, 2)
	start = make(chan struct{})
	for range 2 {
		go func() {
			<-start
			item, setErr := w.apps.SetEnabledByIDAtEpoch(
				ctx, installed.InstallationID, w.owner, true, 2,
			)
			enableResults <- result{installation: item, err: setErr}
		}()
	}
	close(start)
	var enableUpdatedAt time.Time
	for range 2 {
		item := <-enableResults
		if item.err != nil || item.installation.State != applicationapps.StateEnabled ||
			item.installation.AuthorityEpoch != 2 {
			t.Fatalf("enable replay = %#v, %v", item.installation, item.err)
		}
		if enableUpdatedAt.IsZero() {
			enableUpdatedAt = item.installation.UpdatedAt
		} else if !item.installation.UpdatedAt.Equal(enableUpdatedAt) {
			t.Fatalf("idempotent enable replay churned updated_at: %s != %s",
				item.installation.UpdatedAt, enableUpdatedAt)
		}
	}

	uninstallResults := make(chan error, 2)
	start = make(chan struct{})
	for range 2 {
		go func() {
			<-start
			uninstallResults <- w.apps.UninstallByID(
				ctx, installed.InstallationID, w.owner,
			)
		}()
	}
	close(start)
	var removed, absent int
	for range 2 {
		switch uninstallErr := <-uninstallResults; {
		case uninstallErr == nil:
			removed++
		case errors.Is(uninstallErr, applicationapps.ErrInstallationNotFound):
			absent++
		default:
			t.Fatalf("uninstall replay = %v", uninstallErr)
		}
	}
	if removed != 1 || absent != 1 {
		t.Fatalf("uninstall replay outcomes: removed=%d absent=%d", removed, absent)
	}
}

func TestDurableInstallOperationReceiptSurvivesUninstall(t *testing.T) {
	w := newAppWorld(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	created, err := w.workspaces.CreateWorkspace(ctx, "install receipt", w.owner)
	if err != nil {
		t.Fatal(err)
	}
	owner := applicationapps.WorkspaceOwner(created.WorkspaceID)
	const (
		installedOperation  = "00000000-0000-4000-8000-000000000101"
		conflictOperation   = "00000000-0000-4000-8000-000000000102"
		reinstallOperation  = "00000000-0000-4000-8000-000000000103"
		pendingOperation    = "00000000-0000-4000-8000-000000000104"
		concurrentOperation = "00000000-0000-4000-8000-000000000105"
	)

	installed, err := w.apps.InstallAtOperation(
		ctx, owner, w.owner, "messaging", installedOperation,
	)
	if err != nil {
		t.Fatalf("install at operation: %v", err)
	}
	historical, err := w.apps.InstallAtOperation(
		ctx, owner, w.owner, "messaging", installedOperation,
	)
	if err != nil || historical != installed {
		t.Fatalf("installed receipt replay = %#v, %v; want %#v", historical, err, installed)
	}
	if _, err := w.apps.InstallAtOperation(
		ctx, owner, w.owner, "messaging", conflictOperation,
	); !errors.Is(err, applicationapps.ErrInstallIntentAlreadyInstalled) {
		t.Fatalf("existing install operation = %v", err)
	}

	if err := w.apps.UninstallByID(ctx, installed.InstallationID, w.owner); err != nil {
		t.Fatalf("uninstall installed receipt result: %v", err)
	}
	historical, err = w.apps.InstallAtOperation(
		ctx, owner, w.owner, "messaging", installedOperation,
	)
	if err != nil || historical != installed {
		t.Fatalf("post-uninstall installed receipt = %#v, %v", historical, err)
	}
	if _, err := w.apps.InstallAtOperation(
		ctx, owner, w.owner, "messaging", conflictOperation,
	); !errors.Is(err, applicationapps.ErrInstallIntentAlreadyInstalled) {
		t.Fatalf("post-uninstall conflict receipt = %v", err)
	}
	list, err := w.apps.Installations(ctx, owner, w.owner)
	if err != nil || len(list) != 0 {
		t.Fatalf("receipt replay resurrected installation = %#v, %v", list, err)
	}

	reinstalled, err := w.apps.InstallAtOperation(
		ctx, owner, w.owner, "messaging", reinstallOperation,
	)
	if err != nil {
		t.Fatalf("intentional reinstall: %v", err)
	}
	if reinstalled.InstallationID == installed.InstallationID {
		t.Fatalf("intentional reinstall reused installation id %s", reinstalled.InstallationID)
	}

	personalOwner := applicationapps.ParticipantOwner(w.member)
	const mismatchOperation = "00000000-0000-4000-8000-000000000106"
	if _, err := w.apps.InstallAtOperation(
		ctx, personalOwner, w.member, "alarm", mismatchOperation,
	); err != nil {
		t.Fatalf("seed mismatched operation: %v", err)
	}
	if _, err := w.apps.InstallAtOperation(
		ctx, personalOwner, w.member, "missing-app", mismatchOperation,
	); !errors.Is(err, applicationapps.ErrInstallIntentMismatch) {
		t.Fatalf("operation app mismatch = %v", err)
	}
	if _, err := w.apps.InstallAtOperation(
		ctx, personalOwner, w.member, "life-log", "not-a-uuid",
	); !errors.Is(err, applicationapps.ErrInstallOperationInvalid) {
		t.Fatalf("invalid operation id = %v", err)
	}

	if _, err := w.pool.Exec(ctx, `
		INSERT INTO app_install_operation_receipts
			(owner_kind, owner_id, operation_id, app_id, status, created_at)
		VALUES ('workspace', $1, $2, 'messaging', 'pending', now())`,
		created.WorkspaceID, pendingOperation,
	); err != nil {
		t.Fatalf("seed committed pending receipt: %v", err)
	}
	if _, err := w.apps.InstallAtOperation(
		ctx, owner, w.owner, "messaging", pendingOperation,
	); !errors.Is(err, applicationapps.ErrInstallIntentIncomplete) {
		t.Fatalf("committed pending receipt = %v", err)
	}

	type result struct {
		installation applicationapps.Installation
		err          error
	}
	concurrentOwner := applicationapps.ParticipantOwner(w.owner)
	start := make(chan struct{})
	results := make(chan result, 2)
	for range 2 {
		go func() {
			<-start
			item, installErr := w.apps.InstallAtOperation(
				ctx, concurrentOwner, w.owner, "life-log", concurrentOperation,
			)
			results <- result{installation: item, err: installErr}
		}()
	}
	close(start)
	first := <-results
	second := <-results
	if first.err != nil || second.err != nil || first.installation != second.installation {
		t.Fatalf("concurrent operation replay = %#v/%v and %#v/%v",
			first.installation, first.err, second.installation, second.err)
	}

	var receiptCount int
	if err := w.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM app_install_operation_receipts
		WHERE owner_kind = 'workspace' AND owner_id = $1`,
		created.WorkspaceID,
	).Scan(&receiptCount); err != nil {
		t.Fatal(err)
	}
	if receiptCount != 4 {
		t.Fatalf("workspace receipt count = %d, want 4", receiptCount)
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

func TestCatalogProjectsOnlyAppOwnedWorkspaceRoleCapabilities(t *testing.T) {
	w := newAppWorld(t)
	catalog, err := w.apps.Catalog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog) != 4 {
		t.Fatalf("catalog length = %d, want 4: %#v", len(catalog), catalog)
	}
	for _, descriptor := range catalog {
		switch descriptor.AppID {
		case "messaging":
			if len(descriptor.WorkspaceRoleCapabilities) != 1 {
				t.Fatalf("Messaging capabilities = %#v", descriptor.WorkspaceRoleCapabilities)
			}
			capability := descriptor.WorkspaceRoleCapabilities[0]
			if capability.Ref != "app.messaging.manage_channels" || capability.Label != "Manage channels" {
				t.Fatalf("Messaging capability = %#v", capability)
			}
		case "alarm", "direct-chat", "life-log":
			if descriptor.WorkspaceRoleCapabilities == nil || len(descriptor.WorkspaceRoleCapabilities) != 0 {
				t.Fatalf("%s capabilities = %#v, want a non-nil empty catalog projection",
					descriptor.AppID, descriptor.WorkspaceRoleCapabilities)
			}
		default:
			t.Fatalf("unexpected app descriptor %#v", descriptor)
		}
		for _, capability := range descriptor.WorkspaceRoleCapabilities {
			if capability.Ref == "app.messaging.mention_all" {
				t.Fatal("catalog advertises unimplemented mention_all")
			}
		}
	}
	for _, invalid := range []struct {
		id, appID, ref string
	}{
		{"0198f0f4-9b72-7000-8000-0000000008d1", "messaging", "app.messaging.ManageChannels"},
		{"0198f0f4-9b72-7000-8000-0000000008d2", "alarm", "app.messaging.another_capability"},
	} {
		if _, err := w.pool.Exec(context.Background(), `
			INSERT INTO app_workspace_role_capabilities
				(capability_id, app_id, capability_ref, label)
			VALUES ($1, $2, $3, 'invalid')`, invalid.id, invalid.appID, invalid.ref); err == nil {
			t.Fatalf("catalog admitted invalid capability ref %#v", invalid)
		}
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
