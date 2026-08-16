package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/directchat"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
	workspacecontrol "github.com/sumi-studio/sumi/apps/api/internal/workspace"
)

type backendLossCommandAppender struct {
	inner   agentevents.CommandAppender
	started chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (a *backendLossCommandAppender) Append(
	ctx context.Context,
	provenance agentevents.DirectChatProvenance,
	idempotencyKey string,
	command json.RawMessage,
) (agentevents.CommandEnvelope, error) {
	envelope, err := a.inner.Append(ctx, provenance, idempotencyKey, command)
	a.once.Do(func() { close(a.started) })
	select {
	case <-a.release:
	case <-ctx.Done():
		return agentevents.CommandEnvelope{}, ctx.Err()
	}
	return envelope, err
}

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
	lifecycle := directchat.NewLifecycleFence()
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1", lifecycle)
	appStore := applicationapps.New(pool, workspacecontrol.New(pool), lifecycle)
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
	firstInstallation, err := appStore.InstallAtOperation(
		ctx,
		applicationapps.ParticipantOwner(participant.Human(first.HumanID)),
		participant.Human(first.HumanID),
		"direct-chat", uuid.NewString(),
	)
	if err != nil {
		t.Fatalf("install first direct chat: %v", err)
	}
	secondInstallation, err := appStore.InstallAtOperation(
		ctx,
		applicationapps.ParticipantOwner(participant.Human(second.HumanID)),
		participant.Human(second.HumanID),
		"direct-chat", uuid.NewString(),
	)
	if err != nil {
		t.Fatalf("install second direct chat: %v", err)
	}

	// Each Human is the Employer of their own Secretary: direct chat allowed.
	if err := authorizeDirectChatWithFence(ctx, lifecycle, authorizer, first.HumanID, first.AgentID, firstInstallation.InstallationID, func() error { return nil }); err != nil {
		t.Fatalf("owner should be authorized for own secretary: %v", err)
	}
	alarmInstallation, err := appStore.InstallAtOperation(
		ctx,
		applicationapps.ParticipantOwner(participant.Human(first.HumanID)),
		participant.Human(first.HumanID),
		"alarm", uuid.NewString(),
	)
	if err != nil {
		t.Fatalf("install alarm: %v", err)
	}
	if err := authorizeDirectChatWithFence(ctx, lifecycle, authorizer, first.HumanID, first.AgentID, alarmInstallation.InstallationID, func() error { return nil }); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatalf("wrong app installation error = %v", err)
	}
	if _, err := appStore.SetEnabledByID(ctx, firstInstallation.InstallationID, participant.Human(first.HumanID), false); err != nil {
		t.Fatalf("disable first direct chat: %v", err)
	}
	if err := authorizeDirectChatWithFence(ctx, lifecycle, authorizer, first.HumanID, first.AgentID, firstInstallation.InstallationID, func() error { return nil }); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatalf("disabled direct chat error = %v", err)
	}
	if _, err := appStore.SetEnabledByID(ctx, firstInstallation.InstallationID, participant.Human(first.HumanID), true); err != nil {
		t.Fatalf("re-enable first direct chat: %v", err)
	}
	if err := authorizeDirectChatEpochWithFence(
		ctx, lifecycle, authorizer, first.HumanID, first.AgentID,
		firstInstallation.InstallationID, 1, func() error { return nil },
	); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatalf("pre-disable authority epoch revived after re-enable: %v", err)
	}
	if err := authorizeDirectChatEpochWithFence(
		ctx, lifecycle, authorizer, first.HumanID, first.AgentID,
		firstInstallation.InstallationID, 2, func() error { return nil },
	); err != nil {
		t.Fatalf("current authority epoch rejected after re-enable: %v", err)
	}
	// A Human is NOT the Employer of another Human's Secretary: rejected.
	if err := authorizeDirectChatWithFence(ctx, lifecycle, authorizer, second.HumanID, first.AgentID, secondInstallation.InstallationID, func() error { return nil }); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatal("non-employer human must not direct-chat with another's secretary")
	}
	// An exact installation cannot be borrowed across Humans.
	if err := authorizeDirectChatWithFence(ctx, lifecycle, authorizer, second.HumanID, second.AgentID, firstInstallation.InstallationID, func() error { return nil }); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
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
	if err := authorizeDirectChatWithFence(ctx, lifecycle, authorizer, first.HumanID, first.AgentID, firstInstallation.InstallationID, func() error { return nil }); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatal("former employer must lose direct-chat access after 異動")
	}
	if err := authorizeDirectChatWithFence(ctx, lifecycle, authorizer, second.HumanID, first.AgentID, secondInstallation.InstallationID, func() error { return nil }); err != nil {
		t.Fatalf("new employer should be authorized after 異動: %v", err)
	}
}

func TestDirectChatAuthorizerSerializesDisableAgainstOperation(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	lifecycle := directchat.NewLifecycleFence()
	kosekiStore := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1", lifecycle)
	appStore := applicationapps.New(pool, workspacecontrol.New(pool), lifecycle)
	authorizer := newDirectChatAuthorizer(pool, kosekiStore, appStore)
	registration, err := kosekiStore.AutoRegister(ctx, "firebase", "uid-disable-race")
	if err != nil {
		t.Fatal(err)
	}
	actor := participant.Human(registration.HumanID)
	installation, err := appStore.InstallAtOperation(ctx, applicationapps.ParticipantOwner(actor), actor, "direct-chat", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}

	operationEntered := make(chan struct{})
	releaseOperation := make(chan struct{})
	authorizeDone := make(chan error, 1)
	go func() {
		authorizeDone <- authorizeDirectChatWithFence(
			ctx,
			lifecycle,
			authorizer,
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
	if err := authorizeDirectChatWithFence(
		ctx,
		lifecycle,
		authorizer,
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
	releaseLifecycle, err := lifecycle.AcquireMutation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	lifecycleReleased := false
	defer func() {
		if !lifecycleReleased {
			releaseLifecycle()
		}
	}()
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
		"UPDATE app_installations SET enabled = FALSE, authority_epoch = authority_epoch + 1, updated_at = NOW() WHERE installation_id = $1",
		installation.InstallationID,
	); err != nil {
		t.Fatalf("stage disable: %v", err)
	}
	lifecycleFirstCalled := false
	lifecycleFirstDone := make(chan error, 1)
	go func() {
		lifecycleFirstDone <- authorizeDirectChatWithFence(
			ctx,
			lifecycle,
			authorizer,
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
	releaseLifecycle()
	lifecycleReleased = true
	if err := <-lifecycleFirstDone; !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatalf("lifecycle-first authorization error = %v", err)
	}
	if lifecycleFirstCalled {
		t.Fatal("operation ran after lifecycle-first disable")
	}
}

func TestDirectChatProcessFenceSurvivesBackendLossAfterAuthorizationCommit(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	lifecycle := directchat.NewLifecycleFence()
	kosekiStore := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1", lifecycle)
	appStore := applicationapps.New(pool, workspacecontrol.New(pool), lifecycle)
	const authorizationApplicationName = "sumi-direct-chat-auth-backend-loss"
	authorizationConfig, err := pgxpool.ParseConfig(pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	authorizationConfig.MaxConns = 1
	authorizationConfig.MinConns = 1
	if authorizationConfig.ConnConfig.RuntimeParams == nil {
		authorizationConfig.ConnConfig.RuntimeParams = map[string]string{}
	}
	authorizationConfig.ConnConfig.RuntimeParams["application_name"] = authorizationApplicationName
	authorizationPool, err := pgxpool.NewWithConfig(ctx, authorizationConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer authorizationPool.Close()
	if err := authorizationPool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	authorizer := newDirectChatAuthorizer(authorizationPool, kosekiStore, appStore)
	first, err := kosekiStore.AutoRegister(ctx, "firebase", "uid-backend-loss-first")
	if err != nil {
		t.Fatal(err)
	}
	second, err := kosekiStore.AutoRegister(ctx, "firebase", "uid-backend-loss-second")
	if err != nil {
		t.Fatal(err)
	}
	actor := participant.Human(first.HumanID)
	installation, err := appStore.InstallAtOperation(
		ctx,
		applicationapps.ParticipantOwner(actor),
		actor,
		directchat.AppID, uuid.NewString(),
	)
	if err != nil {
		t.Fatal(err)
	}

	commandStore, err := agentevents.OpenCommandStore(privateRuntimeDir(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = commandStore.Close() })
	gateway, err := agentevents.OpenDurableGateway(privateRuntimeDir(t), commandStore)
	if err != nil {
		t.Fatal(err)
	}
	runtimeReceipt := "backend-loss-runtime-ready"
	if err := gateway.PublishRuntimeState(first.AgentID, 1, &runtimeReceipt); err != nil {
		t.Fatal(err)
	}
	sessions, err := agentevents.NewHMACUserSessionVerifier(
		testSessionSecret,
		agentevents.DefaultBrowserAudience(),
		gateway,
	)
	if err != nil {
		t.Fatal(err)
	}
	session, err := sessions.IssueSession(ctx, agentevents.UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             first.HumanID,
		PersonalityAgentID: first.AgentID,
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	releaseEffect := make(chan struct{})
	appender := &backendLossCommandAppender{
		inner: gateway, started: make(chan struct{}), release: releaseEffect,
	}
	ingress, err := agentevents.NewUserCommandIngress(appender, sessions)
	if err != nil {
		t.Fatal(err)
	}
	ingress.AllowedOrigins = []string{testBrowserOrigin}
	ingress.Authorizer = authorizer
	ingress.LifecycleFence = lifecycle
	server := httptest.NewServer(ingress)
	defer server.Close()

	type commandResult struct {
		response *http.Response
		err      error
	}
	commandDone := make(chan commandResult, 1)
	go func() {
		body := bytes.NewBufferString(
			`{"type":"user_message","text":"survive backend loss","attachments":[]}`,
		)
		req, requestErr := http.NewRequestWithContext(
			ctx,
			http.MethodPost,
			fmt.Sprintf(
				"%s/direct-chat/commands?installation_id=%s&authority_epoch=1",
				server.URL,
				installation.InstallationID,
			),
			body,
		)
		if requestErr != nil {
			commandDone <- commandResult{err: requestErr}
			return
		}
		req.Header.Set("Origin", testBrowserOrigin)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "backend-loss-after-auth")
		req.AddCookie(&http.Cookie{
			Name: agentevents.BrowserSessionCookie, Value: session,
		})
		response, requestErr := http.DefaultClient.Do(req)
		commandDone <- commandResult{response: response, err: requestErr}
	}()
	select {
	case <-appender.started:
		// The appender is entered only after the second composite authorization
		// transaction committed. The durable filesystem append has completed,
		// while returning its receipt and the HTTP acceptance are still fenced by
		// the process-lifetime operation permit.
	case <-ctx.Done():
		t.Fatalf("authorized command effect did not start: %v", ctx.Err())
	}

	var backendPID int32
	if err := pool.QueryRow(ctx, `
		SELECT pid
		FROM pg_stat_activity
		WHERE datname = current_database()
		  AND application_name = $1
		  AND state = 'idle'
		ORDER BY backend_start DESC
		LIMIT 1`, authorizationApplicationName).Scan(&backendPID); err != nil {
		t.Fatalf("locate committed authorization backend: %v", err)
	}
	var terminated bool
	if err := pool.QueryRow(ctx, "SELECT pg_terminate_backend($1)", backendPID).Scan(&terminated); err != nil {
		t.Fatalf("terminate authorization backend: %v", err)
	}
	if !terminated {
		t.Fatalf("authorization backend %d was not terminated", backendPID)
	}

	disableDone := make(chan error, 1)
	go func() {
		_, disableErr := appStore.SetEnabledByID(
			ctx,
			installation.InstallationID,
			actor,
			false,
		)
		disableDone <- disableErr
	}()
	transferDone := make(chan error, 1)
	go func() {
		transferDone <- kosekiStore.TransferEmployment(
			ctx,
			first.AgentID,
			koseki.EmployerHuman,
			second.HumanID,
		)
	}()
	for name, done := range map[string]<-chan error{
		"disable":  disableDone,
		"transfer": transferDone,
	} {
		select {
		case mutationErr := <-done:
			close(releaseEffect)
			t.Fatalf("%s committed after PG lease loss but before effect completion: %v", name, mutationErr)
		case <-time.After(75 * time.Millisecond):
		}
	}
	close(releaseEffect)
	result := <-commandDone
	if result.err != nil {
		t.Fatalf("authorized command became ambiguous after backend loss: %v", result.err)
	}
	defer result.response.Body.Close()
	if result.response.StatusCode != http.StatusCreated {
		t.Fatalf("authorized command status after backend loss = %d", result.response.StatusCode)
	}
	var receipt testCommandReceipt
	if err := json.NewDecoder(result.response.Body).Decode(&receipt); err != nil {
		t.Fatalf("decode command acceptance after backend loss: %v", err)
	}
	commands, err := gateway.CatchUp(ctx, agentevents.TokenClaims{
		PersonalityAgentID: first.AgentID,
	}, 1)
	if err != nil {
		t.Fatalf("read durable command after backend loss: %v", err)
	}
	if len(commands) != 1 || commands[0].CommandID != receipt.CommandID ||
		commands[0].Seq != receipt.Seq {
		t.Fatalf("accepted command does not match durable log: receipt=%+v commands=%+v", receipt, commands)
	}
	if err := <-disableDone; err != nil {
		t.Fatalf("disable after effect completion: %v", err)
	}
	if err := <-transferDone; err != nil {
		t.Fatalf("transfer after effect completion: %v", err)
	}
	var enabled bool
	if err := pool.QueryRow(
		ctx,
		"SELECT enabled FROM app_installations WHERE installation_id = $1",
		installation.InstallationID,
	).Scan(&enabled); err != nil {
		t.Fatal(err)
	}
	if enabled {
		t.Fatal("direct-chat installation remained enabled")
	}
	var employerID string
	if err := pool.QueryRow(
		ctx,
		"SELECT employer_id FROM employments WHERE agent_id = $1 AND ended_at IS NULL",
		first.AgentID,
	).Scan(&employerID); err != nil {
		t.Fatal(err)
	}
	if employerID != second.HumanID {
		t.Fatalf("current Employer = %q, want %q", employerID, second.HumanID)
	}
}

func TestDirectChatAuthorizerUninstallReinstallDoesNotReviveStaleInstallation(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	lifecycle := directchat.NewLifecycleFence()
	kosekiStore := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1", lifecycle)
	appStore := applicationapps.New(pool, workspacecontrol.New(pool), lifecycle)
	authorizer := newDirectChatAuthorizer(pool, kosekiStore, appStore)
	registration, err := kosekiStore.AutoRegister(ctx, "firebase", "uid-reinstall")
	if err != nil {
		t.Fatal(err)
	}
	actor := participant.Human(registration.HumanID)
	owner := applicationapps.ParticipantOwner(actor)
	oldInstallation, err := appStore.InstallAtOperation(ctx, owner, actor, "direct-chat", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if err := appStore.UninstallByID(ctx, oldInstallation.InstallationID, actor); err != nil {
		t.Fatal(err)
	}
	newInstallation, err := appStore.InstallAtOperation(ctx, owner, actor, "direct-chat", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if oldInstallation.InstallationID == newInstallation.InstallationID {
		t.Fatal("reinstall reused installation identity")
	}
	if err := authorizeDirectChatWithFence(ctx, lifecycle, authorizer, registration.HumanID, registration.AgentID, oldInstallation.InstallationID, func() error { return nil }); !errors.Is(err, agentevents.ErrDirectChatAuthorizationDenied) {
		t.Fatalf("stale installation error = %v", err)
	}
	if err := authorizeDirectChatWithFence(ctx, lifecycle, authorizer, registration.HumanID, registration.AgentID, newInstallation.InstallationID, func() error { return nil }); err != nil {
		t.Fatalf("new installation rejected: %v", err)
	}
}

func authorizeDirectChatWithFence(
	ctx context.Context,
	lifecycle *directchat.LifecycleFence,
	authorizer *directChatAuthorizer,
	humanID,
	agentID,
	installationID string,
	operation func() error,
) error {
	return authorizeDirectChatEpochWithFence(
		ctx,
		lifecycle,
		authorizer,
		humanID,
		agentID,
		installationID,
		1,
		operation,
	)
}

func authorizeDirectChatEpochWithFence(
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
	if err := authorizer.AuthorizeDirectChat(
		ctx,
		humanID,
		agentID,
		installationID,
		authorityEpoch,
	); err != nil {
		return err
	}
	return operation()
}
