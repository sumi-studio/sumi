package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
	"github.com/sumi-studio/sumi/apps/api/internal/spawn"
)

type continuityResolver struct{}

func (continuityResolver) AgentWrappingKey(context.Context, string) (spawn.WrappingKeyMaterial, error) {
	return provisionedTestWrappingMaterial, nil
}
func (continuityResolver) AgentWarmth(context.Context, string) (string, error) {
	return spawn.WarmthCold, nil
}

type continuityEmployer struct{ paid string }

func (e continuityEmployer) AgentForHuman(context.Context, string) (string, error) {
	return e.paid, nil
}
func (e continuityEmployer) AuthorizeCurrentHumanEmployer(_ context.Context, _, _ string, operation func() error) error {
	return operation()
}

func controlRequest(t *testing.T, socketPath, path, bearer string, body any) (int, []byte) {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Second}
	request, err := http.NewRequest(http.MethodPost, "http://local-control"+path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+bearer)
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	output, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, output
}

func publishContinuityReady(t *testing.T, socketPath string, epoch runtimeprovision.PreparedEpoch, bearer string) {
	t.Helper()
	publication := agentevents.LocalRuntimeStatePublication{
		PublicationID: "continuity-startup", PersonalityAgentID: epoch.PersonalityAgentID,
		Generation: epoch.Generation, RPCBootNonce: epoch.RPCBootNonce,
		State: agentevents.LocalRuntimeNotReady, Reason: agentevents.LocalRuntimeStartup,
	}
	if status, _ := controlRequest(t, socketPath, agentevents.LocalRuntimeStatePublishPath, bearer, publication); status != http.StatusOK {
		t.Fatalf("startup publication status=%d", status)
	}
	revision, receipt := uint64(1), "same-hydration-receipt"
	publication.PublicationID = "continuity-ready"
	publication.ExpectedRevision = &revision
	publication.HydrationReceiptIdentity = &receipt
	publication.State = agentevents.LocalRuntimeReady
	publication.Reason = agentevents.LocalRuntimeHydrated
	if status, _ := controlRequest(t, socketPath, agentevents.LocalRuntimeStatePublishPath, bearer, publication); status != http.StatusOK {
		t.Fatalf("Ready publication status=%d", status)
	}
}

func TestApplicationClosePreservesActiveEpochAndFreshAPIAdoptsIt(t *testing.T) {
	ctx := context.Background()
	recorder := &provisioningRecorder{}
	provisioner := newFakeRuntimeProvisioner(recorder)
	commandDir, runtimeDir := t.TempDir(), privateRuntimeDir(t)
	root, err := os.MkdirTemp("/tmp", "continuity-lc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Chmod(root, 0o750); err != nil {
		t.Fatal(err)
	}
	paid := provisionedTestPAIDs[0]
	socketPath, err := agentevents.LocalControlSocketPath(root, paid)
	if err != nil {
		t.Fatal(err)
	}
	newAPI := func() (*application, *agentevents.LocalControlServer, *agentevents.DurableGateway) {
		store, err := agentevents.OpenCommandStore(commandDir)
		if err != nil {
			t.Fatal(err)
		}
		gateway, err := agentevents.OpenDurableGateway(runtimeDir, store)
		if err != nil {
			t.Fatal(err)
		}
		control, err := agentevents.NewLocalControlServer(gateway, []byte("continuity-signing-secret-at-least-32-bytes"), nil)
		if err != nil {
			t.Fatal(err)
		}
		registry, err := agentevents.NewLocalControlListenerRegistry(control, agentevents.LocalControlListenerRegistryConfig{
			RootDir: root, SocketGID: os.Getegid(),
			OpenListener: func(path string, _ int, _ string) (net.Listener, error) {
				if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
					return nil, err
				}
				listener, err := net.Listen("unix", path)
				if err == nil {
					err = os.Chmod(path, 0o660)
				}
				return listener, err
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		spawner, err := newProvisionedRuntimeSpawner(provisionedRuntimeSpawnerConfig{
			Provisioner: provisioner, Authorizations: control, Listeners: registry,
			Readiness: &fakeRuntimeReadiness{ready: true}, TenantID: "tenant",
			Audience: agentevents.DefaultAgentAudience(), Delivery: agentevents.LocalDeliveryRaw,
			LifecycleTimeout: time.Second, TeardownTimeout: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		manager, err := spawn.New(spawn.Config{Spawner: spawner, Resolver: continuityResolver{}})
		if err != nil {
			t.Fatal(err)
		}
		return &application{spawnManager: manager, localRuntimes: registry, store: store}, control, gateway
	}
	first, _, _ := newAPI()
	if err := first.spawnManager.EnsureRunning(ctx, paid); err != nil {
		t.Fatal(err)
	}
	epoch := provisioner.epochs[paid]
	bearer := provisioner.activations[paid].LocalControlBearer
	publishContinuityReady(t, socketPath, epoch, bearer)
	issue := agentevents.LocalCredentialIssueRequest{RequestID: "before-restart", PersonalityAgentID: paid, Generation: epoch.Generation, RPCBootNonce: epoch.RPCBootNonce, Audience: agentevents.DefaultAgentAudience()}
	if status, _ := controlRequest(t, socketPath, agentevents.LocalCredentialIssuePath, bearer, issue); status != http.StatusOK {
		t.Fatalf("initial credential status=%d", status)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if provisioner.epochs[paid] != epoch || provisioner.stops[paid] != 0 {
		t.Fatal("API close stopped the active runtime")
	}
	boundary := len(recorder.calls)
	second, control, gateway := newAPI()
	defer second.Close()
	// A temporary root outage must leave the existing compute and epoch alone.
	provisioner.inspectErr = errors.New("temporary observation outage")
	if err := second.spawnManager.RestoreRunning(ctx, paid); err == nil {
		t.Fatal("unavailable recovery succeeded")
	}
	if provisioner.stops[paid] != 0 {
		t.Fatal("unavailable recovery stopped work")
	}
	if _, err := os.Stat(socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("recovery exposed a socket before authorization was restored")
	}
	provisioner.inspectErr = nil
	if err := second.spawnManager.RestoreRunning(ctx, paid); err != nil {
		t.Fatal(err)
	}
	if !second.spawnManager.Running(paid) || provisioner.epochs[paid] != epoch {
		t.Fatal("API did not adopt the same active epoch")
	}
	for _, call := range recorder.calls[boundary:] {
		if call == "prepare:"+paid || call == "activate:"+paid || call == "stop:"+paid || call == "reconcile:"+paid {
			t.Fatalf("adoption changed physical lifecycle: %s", call)
		}
	}
	issue.RequestID = "after-restart"
	status, body := controlRequest(t, socketPath, agentevents.LocalCredentialIssuePath, bearer, issue)
	if status != http.StatusOK {
		t.Fatalf("reconnected credential status=%d", status)
	}
	var grant agentevents.LocalCredentialIssueResponse
	if err := json.Unmarshal(body, &grant); err != nil {
		t.Fatal(err)
	}
	if grant.Generation != epoch.Generation || grant.RPCBootNonce != epoch.RPCBootNonce {
		t.Fatal("reconnect changed runtime identity")
	}
	observation, err := gateway.Observe(ctx, agentevents.TokenClaims{PersonalityAgentID: paid, Generation: epoch.Generation}, epoch.Generation)
	if err != nil || !observation.Ready {
		t.Fatalf("recovery lost Ready: %v", err)
	}
	if err := control.FenceLocalRuntimeAuthorization(ctx, paid, epoch.Generation, epoch.RPCBootNonce); err != nil {
		t.Fatal(err)
	}
	if err := control.RecoverLocalRuntimeAuthorization(ctx, agentevents.LocalRuntimeAuthorization{BearerToken: bearer, TenantID: "tenant", PersonalityAgentID: paid, Generation: epoch.Generation, RPCBootNonce: epoch.RPCBootNonce, Audience: agentevents.DefaultAgentAudience(), DeliveryAuthorization: agentevents.LocalDeliveryRaw}); err == nil {
		t.Fatal("recovery revived a terminal epoch")
	}
	t.Run("failed-explicit-stop-survives-API-restart-as-stop-authority", func(t *testing.T) {
		provisioner.mu.Lock()
		provisioner.stopErr = errors.New("temporary teardown failure")
		provisioner.mu.Unlock()
		if err := second.spawnManager.Stop(paid); err == nil {
			t.Fatal("failed physical stop was reported complete")
		}
		if err := second.Close(); err != nil {
			t.Fatal(err)
		}
		third, _, recoveredGateway := newAPI()
		defer third.Close()
		boundary := len(recorder.calls)
		if err := third.spawnManager.RestoreRunning(ctx, paid); err == nil {
			t.Fatal("unavailable terminal cleanup was reported complete")
		}
		if third.spawnManager.Running(paid) || provisioner.epochs[paid] != epoch {
			t.Fatal("failed terminal cleanup revived or replaced compute")
		}
		observation, err := recoveredGateway.Observe(ctx, agentevents.TokenClaims{PersonalityAgentID: paid, Generation: epoch.Generation}, epoch.Generation)
		if err != nil || !observation.TerminalNotReady {
			t.Fatal("prior explicit stop lost its terminal authority")
		}
		provisioner.mu.Lock()
		provisioner.stopErr = nil
		provisioner.mu.Unlock()
		if err := third.spawnManager.RestoreRunning(ctx, paid); err != nil {
			t.Fatal(err)
		}
		if _, exists := provisioner.epochs[paid]; exists || third.spawnManager.Running(paid) || provisioner.stops[paid] != 1 {
			t.Fatal("terminal cleanup did not finish without admission")
		}
		for _, call := range recorder.calls[boundary:] {
			if call == "prepare:"+paid || call == "activate:"+paid {
				t.Fatal("terminal recovery initialized a replacement runtime")
			}
		}
	})
}

func TestRecoveryRebuildsPendingSelectionAndWaitsForWorkToFinish(t *testing.T) {
	ctx := context.Background()
	first, provisioner, _, _, recorder := newProvisioningTestSpawner(t)
	paid := provisionedTestPAIDs[0]
	desired := "model-first"
	resolve := func(_ context.Context, _ string, base runtimeprovision.ActivationConfig) (runtimeprovision.ActivationConfig, error) {
		base.ModelID = desired
		return base, nil
	}
	first.config.ResolveActivation = resolve
	process, err := first.Spawn(ctx, spawn.AgentRuntimeConfig{AgentID: paid, WrappingKey: provisionedTestWrappingMaterial})
	if err != nil {
		t.Fatal(err)
	}
	if err := process.(spawn.DetachedProcess).Detach(); err != nil {
		t.Fatal(err)
	}
	epoch := provisioner.epochs[paid]
	bearer := provisioner.activations[paid].LocalControlBearer
	busy := true
	worker := newChatGPTActivationWorker(continuityEmployer{paid: paid})
	config := first.config
	config.Authorizations = &fakeAuthorizationController{recorder: recorder, current: make(map[string]agentevents.LocalRuntimeAuthorization), fences: make(map[string]int)}
	config.Listeners = &fakeListenerController{recorder: recorder, active: make(map[string]bool)}
	config.OnRecovered = func(context.Context, string) error { worker.enqueue("human"); return nil }
	restarted, err := newProvisionedRuntimeSpawner(config)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := spawn.New(spawn.Config{Spawner: restarted, Resolver: continuityResolver{}, ClaimIdle: func(_ string, claim func() bool) (bool, error) {
		if busy {
			return false, nil
		}
		return claim(), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.DetachAll()
	worker.manager = manager
	worker.configurationCurrent = manager.ConfigurationCurrent
	if err := recoverExistingRuntimes(ctx, warmTestRegistry{ids: provisionedTestPAIDs}, manager, time.Second); err != nil {
		t.Fatal(err)
	}
	if !manager.Running(paid) || len(provisioner.epochs) != 1 || len(worker.pending) != 1 {
		t.Fatal("startup did not recover the cold runtime and rebuild selection work without spawning absent runtimes")
	}
	if done, err := worker.apply(ctx, "human"); err != nil || !done || provisioner.stops[paid] != 0 {
		t.Fatal("unchanged adopted configuration was replaced")
	}
	// The durable desired selection can change before API replacement. The
	// recovered admitted fingerprint still names the running configuration.
	desired = "model-second"
	if done, err := worker.apply(ctx, "human"); err != nil || done || provisioner.stops[paid] != 0 {
		t.Fatal("changed selection interrupted a busy adopted runtime")
	}
	if provisioner.epochs[paid] != epoch || provisioner.activations[paid].LocalControlBearer != bearer {
		t.Fatal("busy selection changed runtime identity or credential")
	}
	busy = false
	if done, err := worker.apply(ctx, "human"); err != nil || !done {
		t.Fatalf("idle selection activation failed: %v", err)
	}
	if provisioner.stops[paid] != 1 || provisioner.epochs[paid].Generation != epoch.Generation+1 || provisioner.activations[paid].ModelID != desired {
		t.Fatal("idle runtime did not apply the pending desired selection exactly once")
	}
	if done, err := worker.apply(ctx, "human"); err != nil || !done || provisioner.stops[paid] != 1 {
		t.Fatal("already applied selection was restarted again")
	}
}
