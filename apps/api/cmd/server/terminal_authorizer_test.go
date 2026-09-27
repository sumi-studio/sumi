package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
	"github.com/sumi-studio/sumi/apps/api/internal/directchat"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
	workspacecontrol "github.com/sumi-studio/sumi/apps/api/internal/workspace"
)

// The terminal surface authorizes against the participant-owned
// 'terminal' AppInstallation through the SAME composite snapshot as
// direct chat — Current Employer plus exact enabled installation +
// authority epoch — but the app id is bound server-side to 'terminal'.
// A direct-chat installation can never authorize terminal, and vice
// versa.

func authorizeTerminalWithFence(
	ctx context.Context,
	lifecycle *directchat.LifecycleFence,
	authorizer *directChatAuthorizer,
	humanID,
	agentID,
	installationID string,
	authorityEpoch int64,
	operation func() error,
) error {
	release, err := lifecycle.AcquireOperation(ctx)
	if err != nil {
		return err
	}
	defer release()
	if err := authorizer.AuthorizeTerminal(
		ctx, humanID, agentID, installationID, authorityEpoch,
	); err != nil {
		return err
	}
	return operation()
}

func TestTerminalAuthorizerBindsTerminalInstallationNotDirectChat(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	lifecycle := directchat.NewLifecycleFence()
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1", lifecycle)
	appStore := applicationapps.New(pool, workspacecontrol.New(pool), lifecycle)
	authorizer := newDirectChatAuthorizer(pool, store, appStore)

	first, err := store.AutoRegister(ctx, "firebase", "uid-terminal-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.AutoRegister(ctx, "firebase", "uid-terminal-2")
	if err != nil {
		t.Fatal(err)
	}
	actor := participant.Human(first.HumanID)
	owner := applicationapps.ParticipantOwner(actor)

	// The shared terminal is its own application: the human installs it
	// once and every /terminal/* request carries this exact binding.
	terminal, err := appStore.InstallAtOperation(ctx, owner, actor, "terminal", uuid.NewString())
	if err != nil {
		t.Fatalf("install terminal: %v", err)
	}
	if terminal.AuthorityEpoch != 1 {
		t.Fatalf("fresh terminal epoch = %d, want 1", terminal.AuthorityEpoch)
	}
	// Auto-registration already installed direct-chat for this human.
	directChat, err := appStore.ResolveEnabledInstallation(ctx, owner, actor, "direct-chat")
	if err != nil {
		t.Fatalf("resolve direct-chat: %v", err)
	}

	// Valid terminal installation authorizes the terminal boundary with
	// no direct-chat dependency.
	if err := authorizeTerminalWithFence(ctx, lifecycle, authorizer,
		first.HumanID, first.AgentID, terminal.InstallationID, 1,
		func() error { return nil }); err != nil {
		t.Fatalf("terminal installation rejected: %v", err)
	}
	// The same installation does not authorize direct chat.
	if err := authorizeDirectChatEpochWithFence(ctx, lifecycle, authorizer,
		first.HumanID, first.AgentID, terminal.InstallationID, 1,
		func() error { return nil }); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatalf("terminal installation must not authorize direct chat: %v", err)
	}
	// The direct-chat installation cannot authorize terminal — the bug
	// this boundary exists to fix.
	if err := authorizeTerminalWithFence(ctx, lifecycle, authorizer,
		first.HumanID, first.AgentID, directChat.InstallationID, 1,
		func() error { return nil }); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatalf("direct-chat installation must not authorize terminal: %v", err)
	}
	// Stale epoch refuses.
	if err := authorizeTerminalWithFence(ctx, lifecycle, authorizer,
		first.HumanID, first.AgentID, terminal.InstallationID, 2,
		func() error { return nil }); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatalf("stale epoch authorized: %v", err)
	}
	// Another human cannot borrow the installation id.
	if err := authorizeTerminalWithFence(ctx, lifecycle, authorizer,
		second.HumanID, second.AgentID, terminal.InstallationID, 1,
		func() error { return nil }); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatalf("foreign human authorized with borrowed installation: %v", err)
	}
	// The second human's own terminal installation works — a second
	// participant-owned install is independent.
	secondTerminal, err := appStore.InstallAtOperation(
		ctx,
		applicationapps.ParticipantOwner(participant.Human(second.HumanID)),
		participant.Human(second.HumanID),
		"terminal", uuid.NewString())
	if err != nil {
		t.Fatalf("install second terminal: %v", err)
	}
	if err := authorizeTerminalWithFence(ctx, lifecycle, authorizer,
		second.HumanID, second.AgentID, secondTerminal.InstallationID, 1,
		func() error { return nil }); err != nil {
		t.Fatalf("second terminal installation rejected: %v", err)
	}
	// Wrong persona for a still-current employer: the Employer check
	// binds human→agent, so first's human with second's agent refuses.
	if err := authorizeTerminalWithFence(ctx, lifecycle, authorizer,
		first.HumanID, second.AgentID, terminal.InstallationID, 1,
		func() error { return nil }); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatalf("wrong persona authorized: %v", err)
	}
}

func TestTerminalAuthorizerDisableRevokesAndEpochBumpDoesNotRevive(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	lifecycle := directchat.NewLifecycleFence()
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1", lifecycle)
	appStore := applicationapps.New(pool, workspacecontrol.New(pool), lifecycle)
	authorizer := newDirectChatAuthorizer(pool, store, appStore)
	registration, err := store.AutoRegister(ctx, "firebase", "uid-terminal-disable")
	if err != nil {
		t.Fatal(err)
	}
	actor := participant.Human(registration.HumanID)
	owner := applicationapps.ParticipantOwner(actor)
	terminal, err := appStore.InstallAtOperation(ctx, owner, actor, "terminal", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}

	// Disable → denied.
	if _, err := appStore.SetEnabledByID(ctx, terminal.InstallationID, actor, false); err != nil {
		t.Fatal(err)
	}
	if err := authorizeTerminalWithFence(ctx, lifecycle, authorizer,
		registration.HumanID, registration.AgentID, terminal.InstallationID, 1,
		func() error { return nil }); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatalf("disabled terminal authorized: %v", err)
	}
	// Re-enable bumps the epoch: the pre-disable binding stays dead.
	reEnabled, err := appStore.SetEnabledByID(ctx, terminal.InstallationID, actor, true)
	if err != nil {
		t.Fatal(err)
	}
	if reEnabled.AuthorityEpoch != 2 {
		t.Fatalf("re-enabled epoch = %d, want 2", reEnabled.AuthorityEpoch)
	}
	if err := authorizeTerminalWithFence(ctx, lifecycle, authorizer,
		registration.HumanID, registration.AgentID, terminal.InstallationID, 1,
		func() error { return nil }); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatalf("pre-disable epoch revived: %v", err)
	}
	if err := authorizeTerminalWithFence(ctx, lifecycle, authorizer,
		registration.HumanID, registration.AgentID, terminal.InstallationID, 2,
		func() error { return nil }); err != nil {
		t.Fatalf("current epoch rejected: %v", err)
	}
	// Uninstall → the exact id is gone; a reinstall mints a new identity.
	if err := appStore.UninstallByID(ctx, terminal.InstallationID, actor); err != nil {
		t.Fatal(err)
	}
	if err := authorizeTerminalWithFence(ctx, lifecycle, authorizer,
		registration.HumanID, registration.AgentID, terminal.InstallationID, 2,
		func() error { return nil }); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatalf("uninstalled terminal authorized: %v", err)
	}
	reinstalled, err := appStore.InstallAtOperation(ctx, owner, actor, "terminal", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if reinstalled.InstallationID == terminal.InstallationID {
		t.Fatal("reinstall reused installation identity")
	}
	if err := authorizeTerminalWithFence(ctx, lifecycle, authorizer,
		registration.HumanID, registration.AgentID, terminal.InstallationID, 1,
		func() error { return nil }); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatalf("stale pre-uninstall installation authorized: %v", err)
	}
	if err := authorizeTerminalWithFence(ctx, lifecycle, authorizer,
		registration.HumanID, registration.AgentID, reinstalled.InstallationID, 1,
		func() error { return nil }); err != nil {
		t.Fatalf("reinstalled terminal rejected: %v", err)
	}
}
