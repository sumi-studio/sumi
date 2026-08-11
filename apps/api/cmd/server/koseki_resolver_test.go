package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
	workspacecontrol "github.com/sumi-studio/sumi/apps/api/internal/workspace"
)

func kosekiResolverTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

func TestKosekiResolverAutoRegistersAndResolves(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resolver := newKosekiIdentityBindingResolver(koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1"), "local", "firebase")
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")

	// First account: auto-registration mints a Human + Secretary.
	first, err := resolver.ResolveIdentity(ctx, agentevents.FirebaseIdentity{
		UID: "firebase-uid-aaa", DisplayName: "  First\nHuman  ",
	})
	if err != nil {
		t.Fatalf("resolve first identity: %v", err)
	}
	if first.TenantID != "local" {
		t.Fatalf("tenant id: got %q want local", first.TenantID)
	}
	if first.UserID == "" || first.PersonalityAgentID == "" {
		t.Fatal("auto-registration must produce a HumanId and PersonalityAgentID")
	}
	if first.UserID == first.PersonalityAgentID {
		t.Fatal("HumanId and PersonalityAgentID must differ")
	}
	if got, err := store.HumanDisplayName(ctx, first.UserID); err != nil || got != "First Human" {
		t.Fatalf("verified initial display name = %q, %v", got, err)
	}
	// Per-agent wrapping key is generated at registration.
	firstKey, err := store.AgentWrappingKey(ctx, first.PersonalityAgentID)
	if err != nil {
		t.Fatalf("wrapping key for first agent: %v", err)
	}
	if firstKey.ID != "test-wrapping/v1" || len(firstKey.Bytes) != 64 {
		t.Fatalf("wrapping key pair mismatch: id=%q bytes=%d", firstKey.ID, len(firstKey.Bytes))
	}

	// Known credential resolves to the same HumanId and agent (no re-registration).
	firstAgain, err := resolver.ResolveIdentity(ctx, agentevents.FirebaseIdentity{UID: "firebase-uid-aaa", DisplayName: "Later Provider Name"})
	if err != nil {
		t.Fatalf("resolve known identity: %v", err)
	}
	if firstAgain.UserID != first.UserID || firstAgain.PersonalityAgentID != first.PersonalityAgentID {
		t.Fatalf("known credential resolved differently: first=%+v again=%+v", first, firstAgain)
	}
	if got, _ := store.HumanDisplayName(ctx, first.UserID); got != "First Human" {
		t.Fatalf("later provider name overwrote initial label: %q", got)
	}

	// Second account: a distinct Human + Secretary, auto-registered.
	second, err := resolver.ResolveIdentity(ctx, agentevents.FirebaseIdentity{UID: "firebase-uid-bbb"})
	if err != nil {
		t.Fatalf("resolve second identity: %v", err)
	}
	if second.UserID == first.UserID || second.PersonalityAgentID == first.PersonalityAgentID {
		t.Fatal("second account must get a distinct HumanId and PersonalityAgentID")
	}
	secondKey, err := store.AgentWrappingKey(ctx, second.PersonalityAgentID)
	if err != nil {
		t.Fatalf("wrapping key for second agent: %v", err)
	}
	if secondKey.ID != "test-wrapping/v1" || len(secondKey.Bytes) != 64 {
		t.Fatalf("second wrapping key pair mismatch: id=%q bytes=%d", secondKey.ID, len(secondKey.Bytes))
	}

	// Each Human has exactly one Secretary that round-trips through the store.
	firstAgent, err := store.AgentForHuman(ctx, first.UserID)
	if err != nil {
		t.Fatalf("agent for first human: %v", err)
	}
	if firstAgent != first.PersonalityAgentID {
		t.Fatalf("agent mismatch: got %q want %q", firstAgent, first.PersonalityAgentID)
	}

	// An unbound credential that is not in the registry returns ErrNoRows from the
	// store (the resolver auto-registers instead, so this checks the lookup path).
	if _, err := store.ResolveCredential(ctx, "firebase", "never-bound"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected ErrNoRows for unbound credential, got %v", err)
	}
}

func TestDirectChatAuthorizerComposesEmployerAndExactParticipantInstallation(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	appStore := applicationapps.New(pool, workspacecontrol.New(pool))
	authorizer := newDirectChatAuthorizer(pool, store, appStore)

	// Two Humans, each with their own Secretary.
	first, err := store.AutoRegister(ctx, "firebase", "uid-employer-1")
	if err != nil {
		t.Fatalf("auto-register first: %v", err)
	}
	second, err := store.AutoRegister(ctx, "firebase", "uid-employer-2")
	if err != nil {
		t.Fatalf("auto-register second: %v", err)
	}
	firstInstallation, err := appStore.Install(
		ctx,
		applicationapps.ParticipantOwner(participant.Human(first.HumanID)),
		participant.Human(first.HumanID),
		"direct-chat",
	)
	if err != nil {
		t.Fatalf("install first direct chat: %v", err)
	}
	secondInstallation, err := appStore.Install(
		ctx,
		applicationapps.ParticipantOwner(participant.Human(second.HumanID)),
		participant.Human(second.HumanID),
		"direct-chat",
	)
	if err != nil {
		t.Fatalf("install second direct chat: %v", err)
	}

	// Each Human is the Employer of their own Secretary: direct chat allowed.
	if err := authorizer.AuthorizeDirectChat(ctx, first.HumanID, first.AgentID, firstInstallation.InstallationID, func() error { return nil }); err != nil {
		t.Fatalf("owner should be authorized for own secretary: %v", err)
	}
	alarmInstallation, err := appStore.Install(
		ctx,
		applicationapps.ParticipantOwner(participant.Human(first.HumanID)),
		participant.Human(first.HumanID),
		"alarm",
	)
	if err != nil {
		t.Fatalf("install alarm: %v", err)
	}
	if err := authorizer.AuthorizeDirectChat(ctx, first.HumanID, first.AgentID, alarmInstallation.InstallationID, func() error { return nil }); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatalf("wrong app installation error = %v", err)
	}
	if _, err := appStore.SetEnabledByID(ctx, firstInstallation.InstallationID, participant.Human(first.HumanID), false); err != nil {
		t.Fatalf("disable first direct chat: %v", err)
	}
	if err := authorizer.AuthorizeDirectChat(ctx, first.HumanID, first.AgentID, firstInstallation.InstallationID, func() error { return nil }); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatalf("disabled direct chat error = %v", err)
	}
	if _, err := appStore.SetEnabledByID(ctx, firstInstallation.InstallationID, participant.Human(first.HumanID), true); err != nil {
		t.Fatalf("re-enable first direct chat: %v", err)
	}
	// A Human is NOT the Employer of another Human's Secretary: rejected.
	if err := authorizer.AuthorizeDirectChat(ctx, second.HumanID, first.AgentID, secondInstallation.InstallationID, func() error { return nil }); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatal("non-employer human must not direct-chat with another's secretary")
	}
	// An exact installation cannot be borrowed across Humans.
	if err := authorizer.AuthorizeDirectChat(ctx, second.HumanID, second.AgentID, firstInstallation.InstallationID, func() error { return nil }); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatalf("wrong Human installation error = %v", err)
	}

	// 異動: transfer the first agent's employment to the second Human. The first
	// Human is no longer the Employer and loses direct-chat access.
	if err := store.TransferEmployment(
		ctx,
		first.AgentID,
		koseki.EmployerHuman,
		second.HumanID,
	); err != nil {
		t.Fatalf("transfer employment: %v", err)
	}
	if err := authorizer.AuthorizeDirectChat(ctx, first.HumanID, first.AgentID, firstInstallation.InstallationID, func() error { return nil }); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatal("former employer must lose direct-chat access after 異動")
	}
	if err := authorizer.AuthorizeDirectChat(ctx, second.HumanID, first.AgentID, secondInstallation.InstallationID, func() error { return nil }); err != nil {
		t.Fatalf("new employer should be authorized after 異動: %v", err)
	}
}

func TestDirectChatAuthorizerSerializesDisableAgainstOperation(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	kosekiStore := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	appStore := applicationapps.New(pool, workspacecontrol.New(pool))
	authorizer := newDirectChatAuthorizer(pool, kosekiStore, appStore)
	registration, err := kosekiStore.AutoRegister(ctx, "firebase", "uid-disable-race")
	if err != nil {
		t.Fatal(err)
	}
	actor := participant.Human(registration.HumanID)
	installation, err := appStore.Install(ctx, applicationapps.ParticipantOwner(actor), actor, "direct-chat")
	if err != nil {
		t.Fatal(err)
	}

	operationEntered := make(chan struct{})
	releaseOperation := make(chan struct{})
	authorizeDone := make(chan error, 1)
	go func() {
		authorizeDone <- authorizer.AuthorizeDirectChat(
			ctx,
			registration.HumanID,
			registration.AgentID,
			installation.InstallationID,
			func() error {
				close(operationEntered)
				<-releaseOperation
				return nil
			},
		)
	}()
	<-operationEntered

	disableStarted := make(chan struct{})
	disableDone := make(chan error, 1)
	go func() {
		close(disableStarted)
		_, err := appStore.SetEnabledByID(ctx, installation.InstallationID, actor, false)
		disableDone <- err
	}()
	<-disableStarted
	select {
	case err := <-disableDone:
		t.Fatalf("disable overtook admitted operation: %v", err)
	case <-time.After(75 * time.Millisecond):
	}
	close(releaseOperation)
	if err := <-authorizeDone; err != nil {
		t.Fatalf("admitted operation failed: %v", err)
	}
	if err := <-disableDone; err != nil {
		t.Fatalf("disable after operation: %v", err)
	}

	operationCalled := false
	if err := authorizer.AuthorizeDirectChat(
		ctx,
		registration.HumanID,
		registration.AgentID,
		installation.InstallationID,
		func() error { operationCalled = true; return nil },
	); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatalf("post-disable authorization error = %v", err)
	}
	if operationCalled {
		t.Fatal("operation ran after disable committed")
	}

	// Exercise the opposite serialization order with a lifecycle transaction
	// already holding the exact installation row. Authorization must wait for
	// that commit, then observe disabled state without entering the operation.
	if _, err := appStore.SetEnabledByID(ctx, installation.InstallationID, actor, true); err != nil {
		t.Fatalf("re-enable before lifecycle-first race: %v", err)
	}
	lifecycleTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lifecycleTx.Rollback(context.Background()) }()
	var lockedInstallationID string
	if err := lifecycleTx.QueryRow(
		ctx,
		"SELECT installation_id FROM app_installations WHERE installation_id = $1 FOR UPDATE",
		installation.InstallationID,
	).Scan(&lockedInstallationID); err != nil {
		t.Fatalf("lock lifecycle row: %v", err)
	}
	if _, err := lifecycleTx.Exec(
		ctx,
		"UPDATE app_installations SET enabled = FALSE, updated_at = NOW() WHERE installation_id = $1",
		installation.InstallationID,
	); err != nil {
		t.Fatalf("stage disable: %v", err)
	}
	lifecycleFirstCalled := false
	lifecycleFirstDone := make(chan error, 1)
	go func() {
		lifecycleFirstDone <- authorizer.AuthorizeDirectChat(
			ctx,
			registration.HumanID,
			registration.AgentID,
			installation.InstallationID,
			func() error { lifecycleFirstCalled = true; return nil },
		)
	}()
	select {
	case err := <-lifecycleFirstDone:
		t.Fatalf("authorization bypassed lifecycle lock: %v", err)
	case <-time.After(75 * time.Millisecond):
	}
	if err := lifecycleTx.Commit(ctx); err != nil {
		t.Fatalf("commit lifecycle-first disable: %v", err)
	}
	if err := <-lifecycleFirstDone; !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatalf("lifecycle-first authorization error = %v", err)
	}
	if lifecycleFirstCalled {
		t.Fatal("operation ran after lifecycle-first disable")
	}
}

func TestDirectChatAuthorizerUninstallReinstallDoesNotReviveStaleInstallation(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	kosekiStore := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	appStore := applicationapps.New(pool, workspacecontrol.New(pool))
	authorizer := newDirectChatAuthorizer(pool, kosekiStore, appStore)
	registration, err := kosekiStore.AutoRegister(ctx, "firebase", "uid-reinstall")
	if err != nil {
		t.Fatal(err)
	}
	actor := participant.Human(registration.HumanID)
	owner := applicationapps.ParticipantOwner(actor)
	oldInstallation, err := appStore.Install(ctx, owner, actor, "direct-chat")
	if err != nil {
		t.Fatal(err)
	}
	if err := appStore.UninstallByID(ctx, oldInstallation.InstallationID, actor); err != nil {
		t.Fatal(err)
	}
	newInstallation, err := appStore.Install(ctx, owner, actor, "direct-chat")
	if err != nil {
		t.Fatal(err)
	}
	if oldInstallation.InstallationID == newInstallation.InstallationID {
		t.Fatal("reinstall reused installation identity")
	}
	if err := authorizer.AuthorizeDirectChat(ctx, registration.HumanID, registration.AgentID, oldInstallation.InstallationID, func() error { return nil }); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatalf("stale installation error = %v", err)
	}
	if err := authorizer.AuthorizeDirectChat(ctx, registration.HumanID, registration.AgentID, newInstallation.InstallationID, func() error { return nil }); err != nil {
		t.Fatalf("new installation rejected: %v", err)
	}
}
