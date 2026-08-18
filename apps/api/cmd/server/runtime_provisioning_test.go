package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
	"github.com/sumi-studio/sumi/apps/api/internal/spawn"
)

var provisionedTestPAIDs = []string{
	"0198f0f4-9b72-7000-8000-000000000001",
	"0198f0f4-9b72-7000-8000-000000000002",
	"0198f0f4-9b72-7000-8000-000000000003",
}

const (
	provisionedTestWrappingKey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	provisionedTestApprovalKey = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
)

var provisionedTestWrappingMaterial = spawn.WrappingKeyMaterial{
	ID: "wrapping/v1", Bytes: provisionedTestWrappingKey,
}

type provisioningRecorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *provisioningRecorder) add(call string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
}

type fakeRuntimeProvisioner struct {
	recorder        *provisioningRecorder
	mu              sync.Mutex
	nextGeneration  map[string]uint64
	epochs          map[string]runtimeprovision.PreparedEpoch
	aborts          map[string]int
	stops           map[string]int
	activations     map[string]runtimeprovision.ActivationConfig
	reapedThrough   map[string]uint64
	activationErr   error
	mismatchActive  bool
	dropBeforeReady bool
	recovery        map[string]bool
	reconcileReaps  map[string]bool
	omitReapReceipt bool
	inspectErr      error
}

func newFakeRuntimeProvisioner(recorder *provisioningRecorder) *fakeRuntimeProvisioner {
	return &fakeRuntimeProvisioner{
		recorder:       recorder,
		nextGeneration: make(map[string]uint64),
		epochs:         make(map[string]runtimeprovision.PreparedEpoch),
		aborts:         make(map[string]int),
		stops:          make(map[string]int),
		activations:    make(map[string]runtimeprovision.ActivationConfig),
		reapedThrough:  make(map[string]uint64),
		recovery:       make(map[string]bool),
		reconcileReaps: make(map[string]bool),
	}
}

func (p *fakeRuntimeProvisioner) Prepare(_ context.Context, request runtimeprovision.PrepareRequest) (runtimeprovision.PreparedEpoch, error) {
	p.recorder.add("prepare:" + request.PersonalityAgentID)
	p.mu.Lock()
	defer p.mu.Unlock()
	generation := p.nextGeneration[request.PersonalityAgentID]
	p.nextGeneration[request.PersonalityAgentID]++
	epoch := runtimeprovision.PreparedEpoch{
		PersonalityAgentID:   request.PersonalityAgentID,
		Generation:           generation,
		RPCBootNonce:         fmt.Sprintf("nonce-%s-%d", request.PersonalityAgentID, generation),
		OpaquePreparedHandle: fmt.Sprintf("handle-%s-%d", request.PersonalityAgentID, generation),
	}
	p.epochs[request.PersonalityAgentID] = epoch
	return epoch, nil
}

func (p *fakeRuntimeProvisioner) Activate(_ context.Context, request runtimeprovision.ActivateRequest) (runtimeprovision.Inspection, error) {
	p.recorder.add("activate:" + request.PersonalityAgentID)
	p.mu.Lock()
	p.activations[request.PersonalityAgentID] = request.Activation
	p.mu.Unlock()
	if p.activationErr != nil {
		return runtimeprovision.Inspection{}, p.activationErr
	}
	epoch := request.PreparedEpoch
	if p.mismatchActive {
		epoch.RPCBootNonce += "-wrong"
	}
	return runtimeprovision.Inspection{
		PersonalityAgentID: request.PersonalityAgentID,
		Phase:              runtimeprovision.PhaseActive,
		Epoch:              &epoch,
	}, nil
}

func (p *fakeRuntimeProvisioner) Abort(_ context.Context, request runtimeprovision.AbortRequest) (runtimeprovision.Inspection, error) {
	p.recorder.add("abort:" + request.PersonalityAgentID)
	p.mu.Lock()
	defer p.mu.Unlock()
	current, exists := p.epochs[request.PersonalityAgentID]
	if !exists || current != request.PreparedEpoch {
		return runtimeprovision.Inspection{}, runtimeprovision.ErrConflict
	}
	p.aborts[request.PersonalityAgentID]++
	delete(p.epochs, request.PersonalityAgentID)
	return p.reapInspection(request.PreparedEpoch), nil
}

func (p *fakeRuntimeProvisioner) Stop(_ context.Context, request runtimeprovision.StopRequest) (runtimeprovision.Inspection, error) {
	p.recorder.add("stop:" + request.PersonalityAgentID)
	p.mu.Lock()
	defer p.mu.Unlock()
	current, exists := p.epochs[request.PersonalityAgentID]
	if !exists || current != request.PreparedEpoch {
		return runtimeprovision.Inspection{}, runtimeprovision.ErrConflict
	}
	p.stops[request.PersonalityAgentID]++
	delete(p.epochs, request.PersonalityAgentID)
	return p.reapInspection(request.PreparedEpoch), nil
}

func (p *fakeRuntimeProvisioner) reapInspection(epoch runtimeprovision.PreparedEpoch) runtimeprovision.Inspection {
	inspection := runtimeprovision.Inspection{
		PersonalityAgentID: epoch.PersonalityAgentID,
		Phase:              runtimeprovision.PhaseUnknown,
	}
	if !p.omitReapReceipt {
		reaped := epoch.Generation
		inspection.ReapedThroughGeneration = &reaped
		if previous, ok := p.reapedThrough[epoch.PersonalityAgentID]; !ok || reaped > previous {
			p.reapedThrough[epoch.PersonalityAgentID] = reaped
		}
	}
	return inspection
}

func (p *fakeRuntimeProvisioner) Reconcile(_ context.Context, request runtimeprovision.ReconcileRequest) (runtimeprovision.Inspection, error) {
	p.recorder.add("reconcile:" + request.PersonalityAgentID)
	p.mu.Lock()
	defer p.mu.Unlock()
	epoch, exists := p.epochs[request.PersonalityAgentID]
	if !exists {
		inspection := runtimeprovision.Inspection{PersonalityAgentID: request.PersonalityAgentID, Phase: runtimeprovision.PhaseUnknown}
		if reaped, ok := p.reapedThrough[request.PersonalityAgentID]; ok {
			inspection.ReapedThroughGeneration = &reaped
		}
		return inspection, nil
	}
	if p.reconcileReaps[request.PersonalityAgentID] {
		p.recorder.add("reap:" + request.PersonalityAgentID)
		delete(p.epochs, request.PersonalityAgentID)
		delete(p.recovery, request.PersonalityAgentID)
		delete(p.reconcileReaps, request.PersonalityAgentID)
		return p.reapInspection(epoch), nil
	}
	if p.dropBeforeReady {
		delete(p.epochs, request.PersonalityAgentID)
		return runtimeprovision.Inspection{PersonalityAgentID: request.PersonalityAgentID, Phase: runtimeprovision.PhaseUnknown}, nil
	}
	return runtimeprovision.Inspection{
		PersonalityAgentID: request.PersonalityAgentID,
		Phase:              runtimeprovision.PhaseActive,
		Epoch:              &epoch,
	}, nil
}

func (p *fakeRuntimeProvisioner) Inspect(_ context.Context, request runtimeprovision.InspectRequest) (runtimeprovision.Inspection, error) {
	p.recorder.add("inspect:" + request.PersonalityAgentID)
	if p.inspectErr != nil {
		return runtimeprovision.Inspection{}, p.inspectErr
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	epoch, exists := p.epochs[request.PersonalityAgentID]
	if !exists {
		return runtimeprovision.Inspection{PersonalityAgentID: request.PersonalityAgentID, Phase: runtimeprovision.PhaseUnknown}, nil
	}
	if p.dropBeforeReady {
		delete(p.epochs, request.PersonalityAgentID)
		return runtimeprovision.Inspection{PersonalityAgentID: request.PersonalityAgentID, Phase: runtimeprovision.PhaseUnknown}, nil
	}
	phase := runtimeprovision.PhaseActive
	if p.recovery[request.PersonalityAgentID] {
		phase = runtimeprovision.PhaseRecovery
	}
	return runtimeprovision.Inspection{PersonalityAgentID: request.PersonalityAgentID, Phase: phase, Epoch: &epoch}, nil
}

type fakeAuthorizationController struct {
	recorder *provisioningRecorder
	mu       sync.Mutex
	current  map[string]agentevents.LocalRuntimeAuthorization
	fences   map[string]int
}

func (c *fakeAuthorizationController) InstallLocalRuntimeAuthorization(_ context.Context, authorization agentevents.LocalRuntimeAuthorization) error {
	c.recorder.add("authorize:" + authorization.PersonalityAgentID)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current[authorization.PersonalityAgentID] = authorization
	return nil
}

func (c *fakeAuthorizationController) FenceLocalRuntimeAuthorization(_ context.Context, paid string, generation uint64, nonce string) error {
	c.recorder.add("fence:" + paid)
	c.mu.Lock()
	defer c.mu.Unlock()
	if current, ok := c.current[paid]; ok && current.Generation == generation && current.RPCBootNonce == nonce {
		delete(c.current, paid)
	}
	c.fences[paid]++
	return nil
}

type fakeListenerController struct {
	recorder  *provisioningRecorder
	mu        sync.Mutex
	active    map[string]bool
	ensureErr error
}

type fakeRuntimeReadiness struct {
	mu                 sync.Mutex
	ready              bool
	terminal           bool
	err                error
	expectedGeneration *uint64
	onObserve          func()
}

func (r *fakeRuntimeReadiness) Observe(
	_ context.Context,
	claims agentevents.TokenClaims,
	generation uint64,
) (agentevents.HydrationObservation, error) {
	r.mu.Lock()
	ready, terminal, err := r.ready, r.terminal, r.err
	expectedGeneration, onObserve := r.expectedGeneration, r.onObserve
	r.mu.Unlock()
	if claims.Generation != generation {
		return agentevents.HydrationObservation{}, errors.New("claims generation mismatch")
	}
	if expectedGeneration != nil && generation != *expectedGeneration {
		return agentevents.HydrationObservation{}, errors.New("stale readiness generation")
	}
	if onObserve != nil {
		onObserve()
	}
	return agentevents.HydrationObservation{Ready: ready, TerminalNotReady: terminal}, err
}

func (r *fakeRuntimeReadiness) setReady(ready bool) {
	r.mu.Lock()
	r.ready = ready
	r.mu.Unlock()
}

func (l *fakeListenerController) EnsureLocalRuntime(paid string) error {
	l.recorder.add("listen:" + paid)
	if l.ensureErr != nil {
		return l.ensureErr
	}
	l.mu.Lock()
	l.active[paid] = true
	l.mu.Unlock()
	return nil
}

func (l *fakeListenerController) CloseLocalRuntime(_ context.Context, paid string) error {
	l.recorder.add("unlisten:" + paid)
	l.mu.Lock()
	delete(l.active, paid)
	l.mu.Unlock()
	return nil
}

func newProvisioningTestSpawner(t *testing.T) (*provisionedRuntimeSpawner, *fakeRuntimeProvisioner, *fakeAuthorizationController, *fakeListenerController, *provisioningRecorder) {
	t.Helper()
	recorder := &provisioningRecorder{}
	provisioner := newFakeRuntimeProvisioner(recorder)
	authorizations := &fakeAuthorizationController{recorder: recorder, current: make(map[string]agentevents.LocalRuntimeAuthorization), fences: make(map[string]int)}
	listeners := &fakeListenerController{recorder: recorder, active: make(map[string]bool)}
	spawner, err := newProvisionedRuntimeSpawner(provisionedRuntimeSpawnerConfig{
		Provisioner:      provisioner,
		Authorizations:   authorizations,
		Listeners:        listeners,
		Readiness:        &fakeRuntimeReadiness{ready: true},
		TenantID:         "tenant-context",
		Audience:         agentevents.DefaultAgentAudience(),
		Delivery:         agentevents.LocalDeliveryRaw,
		BearerTTL:        time.Hour,
		LifecycleTimeout: time.Second,
		Activation: runtimeprovision.ActivationConfig{
			LocalControlServerUID:         65532,
			LocalControlSocketGID:         20000,
			AgentWrappingKeyID:            "template-must-be-overridden",
			ApprovalSecretDigestKey:       provisionedTestApprovalKey,
			ProviderAPIKey:                "provider-key",
			ExecutionReviewerAPIKey:       "execution-reviewer-key",
			ExecutionReviewerModelPreset:  "kimi-k3",
			EscalationReviewerAPIKey:      "escalation-reviewer-key",
			EscalationReviewerModelPreset: "glm-5.2",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return spawner, provisioner, authorizations, listeners, recorder
}

func TestProvisionedRuntimeSpawnerThreePAIDsAreExactAndIsolatedAcrossRestart(t *testing.T) {
	spawner, provisioner, authorizations, listeners, recorder := newProvisioningTestSpawner(t)
	processes := make(map[string]spawn.Process)
	for _, paid := range provisionedTestPAIDs {
		process, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{
			AgentID:     paid,
			WrappingKey: provisionedTestWrappingMaterial,
			GatewayURL:  "ws://gateway.invalid/agent/ws",
		})
		if err != nil {
			t.Fatalf("spawn %s: %v", paid, err)
		}
		processes[paid] = process
		authorization := authorizations.current[paid]
		epoch := provisioner.epochs[paid]
		if authorization.PersonalityAgentID != paid ||
			authorization.Generation != epoch.Generation ||
			authorization.RPCBootNonce != epoch.RPCBootNonce {
			t.Fatalf("authorization does not match prepared epoch for %s: %#v %#v", paid, authorization, epoch)
		}
		if !listeners.active[paid] {
			t.Fatalf("listener not active for %s", paid)
		}
		activation := provisioner.activations[paid]
		if activation.AgentWrappingKeyID != provisionedTestWrappingMaterial.ID ||
			activation.AgentWrappingKey != provisionedTestWrappingMaterial.Bytes {
			t.Fatalf("activation split the stored wrapping key pair for %s", paid)
		}
		if activation.ReapAttestation != nil {
			t.Fatalf("initial spawn fabricated a reap attestation for %s: %#v", paid, activation.ReapAttestation)
		}
	}
	for _, paid := range provisionedTestPAIDs {
		want := []string{"prepare:" + paid, "authorize:" + paid, "listen:" + paid, "activate:" + paid}
		if !containsOrdered(recorder.calls, want) {
			t.Fatalf("lifecycle order for %s is not prepare/authorize/listen/activate: %v", paid, recorder.calls)
		}
	}

	first := provisionedTestPAIDs[0]
	if err := processes[first].Stop(); err != nil {
		t.Fatal(err)
	}
	if _, exists := authorizations.current[first]; exists || listeners.active[first] {
		t.Fatal("stopped PAID retained authorization or listener")
	}
	for _, paid := range provisionedTestPAIDs[1:] {
		if _, exists := authorizations.current[paid]; !exists || !listeners.active[paid] {
			t.Fatalf("stopping %s disturbed isolated PAID %s", first, paid)
		}
	}
	restarted, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: first, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := provisioner.epochs[first].Generation; got != 1 {
		t.Fatalf("restart generation=%d, want 1", got)
	}
	if attestation := provisioner.activations[first].ReapAttestation; attestation == nil || attestation.ReapedThroughGeneration != 0 {
		t.Fatalf("restart after verified Stop did not consume retained reap receipt: %#v", attestation)
	}
	_ = restarted.Stop()
	for _, paid := range provisionedTestPAIDs[1:] {
		_ = processes[paid].Stop()
	}
}

func TestProvisionedRuntimeSpawnerFailureFencesAndAbortsWithoutActivation(t *testing.T) {
	spawner, provisioner, authorizations, listeners, recorder := newProvisioningTestSpawner(t)
	listeners.ensureErr = errors.New("listener failed")
	paid := provisionedTestPAIDs[0]
	_, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
	})
	if err == nil {
		t.Fatal("expected listener failure")
	}
	if provisioner.aborts[paid] != 1 || authorizations.fences[paid] != 1 {
		t.Fatalf("failed spawn did not fence and abort: aborts=%d fences=%d", provisioner.aborts[paid], authorizations.fences[paid])
	}
	if containsOrdered(recorder.calls, []string{"listen:" + paid, "activate:" + paid}) {
		t.Fatalf("activation ran after listener failure: %v", recorder.calls)
	}
}

func TestProvisionedRuntimeSpawnerAmbiguousActivationFailureRetiresExactEpoch(t *testing.T) {
	spawner, provisioner, authorizations, listeners, recorder := newProvisioningTestSpawner(t)
	provisioner.activationErr = errors.New("activation response lost after commit")
	paid := provisionedTestPAIDs[0]
	_, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
	})
	if err == nil {
		t.Fatal("expected ambiguous activation failure")
	}
	if provisioner.aborts[paid] != 1 || authorizations.fences[paid] != 1 || listeners.active[paid] {
		t.Fatalf("ambiguous activation was not exactly retired: aborts=%d fences=%d listener=%v", provisioner.aborts[paid], authorizations.fences[paid], listeners.active[paid])
	}
	want := []string{"activate:" + paid, "fence:" + paid, "abort:" + paid, "unlisten:" + paid}
	if !containsOrdered(recorder.calls, want) {
		t.Fatalf("ambiguous activation cleanup order mismatch: %v", recorder.calls)
	}
}

func TestProvisionedRuntimeSpawnerRejectsWrongActiveEpochBeforeReplacement(t *testing.T) {
	spawner, provisioner, authorizations, listeners, recorder := newProvisioningTestSpawner(t)
	paid := provisionedTestPAIDs[0]
	provisioner.mismatchActive = true
	_, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
	})
	if err == nil {
		t.Fatal("expected wrong active epoch rejection")
	}
	if provisioner.aborts[paid] != 1 || authorizations.fences[paid] != 1 || listeners.active[paid] {
		t.Fatal("wrong activation response retained old epoch authority")
	}
	provisioner.mismatchActive = false
	replacement, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := provisioner.epochs[paid].Generation; got != 1 {
		t.Fatalf("replacement generation=%d, want 1", got)
	}
	if _, ok := authorizations.current[paid]; !ok || !listeners.active[paid] {
		t.Fatal("exact old-epoch cleanup harmed the replacement")
	}
	if !containsOrdered(recorder.calls, []string{"abort:" + paid, "unlisten:" + paid, "prepare:" + paid}) {
		t.Fatalf("replacement began before exact cleanup: %v", recorder.calls)
	}
	_ = replacement.Stop()
}

func TestProvisionedProcessStaleStopCannotCloseReplacementListener(t *testing.T) {
	spawner, provisioner, authorizations, listeners, _ := newProvisioningTestSpawner(t)
	paid := provisionedTestPAIDs[0]
	process, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
	})
	if err != nil {
		t.Fatal(err)
	}
	old := provisioner.epochs[paid]
	replacement := old
	replacement.Generation++
	replacement.RPCBootNonce = "replacement-nonce"
	replacement.OpaquePreparedHandle = "replacement-handle"
	provisioner.mu.Lock()
	provisioner.epochs[paid] = replacement
	provisioner.mu.Unlock()
	authorizations.mu.Lock()
	authorizations.current[paid] = agentevents.LocalRuntimeAuthorization{
		PersonalityAgentID: paid, Generation: replacement.Generation, RPCBootNonce: replacement.RPCBootNonce,
	}
	authorizations.mu.Unlock()

	if err := process.Stop(); !errors.Is(err, runtimeprovision.ErrConflict) {
		t.Fatalf("stale stop error=%v, want ErrConflict", err)
	}
	if !listeners.active[paid] {
		t.Fatal("stale stop closed the replacement listener")
	}
	if current := authorizations.current[paid]; current.Generation != replacement.Generation || current.RPCBootNonce != replacement.RPCBootNonce {
		t.Fatalf("stale stop fenced replacement authorization: %#v", current)
	}
}

func TestProvisionedProcessMonitorFencesAndReconcilesRecoveryBeforeReturning(t *testing.T) {
	spawner, provisioner, authorizations, listeners, recorder := newProvisioningTestSpawner(t)
	paid := provisionedTestPAIDs[0]
	process, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
	})
	if err != nil {
		t.Fatal(err)
	}
	provisioner.mu.Lock()
	provisioner.recovery[paid] = true
	provisioner.reconcileReaps[paid] = true
	provisioner.mu.Unlock()
	provisioned := process.(*provisionedProcess)
	provisioned.monitorInterval = time.Millisecond

	if err := provisioned.Wait(); err == nil || !strings.Contains(err.Error(), "left its active epoch") {
		t.Fatalf("monitor error = %v, want non-active epoch failure", err)
	}
	if authorizations.fences[paid] != 1 || listeners.active[paid] {
		t.Fatalf("monitor retained local runtime authority: fences=%d listener=%t", authorizations.fences[paid], listeners.active[paid])
	}
	if !containsOrdered(recorder.calls, []string{
		"inspect:" + paid,
		"fence:" + paid,
		"unlisten:" + paid,
		"reconcile:" + paid,
		"reap:" + paid,
	}) {
		t.Fatalf("monitor returned before fenced recovery reconcile: %v", recorder.calls)
	}
}

func TestProvisionedProcessMonitorFencesAndReconcilesAfterInspectError(t *testing.T) {
	spawner, provisioner, authorizations, listeners, recorder := newProvisioningTestSpawner(t)
	paid := provisionedTestPAIDs[0]
	process, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
	})
	if err != nil {
		t.Fatal(err)
	}
	provisioner.mu.Lock()
	provisioner.reconcileReaps[paid] = true
	provisioner.mu.Unlock()
	provisioner.inspectErr = errors.New("inspect unavailable")
	provisioned := process.(*provisionedProcess)
	provisioned.monitorInterval = time.Millisecond

	if err := provisioned.Wait(); err == nil || !strings.Contains(err.Error(), "inspect unavailable") {
		t.Fatalf("monitor error = %v, want inspect failure", err)
	}
	if authorizations.fences[paid] != 1 || listeners.active[paid] {
		t.Fatalf("monitor retained local runtime authority: fences=%d listener=%t", authorizations.fences[paid], listeners.active[paid])
	}
	if !containsOrdered(recorder.calls, []string{
		"inspect:" + paid,
		"fence:" + paid,
		"unlisten:" + paid,
		"reconcile:" + paid,
		"reap:" + paid,
	}) {
		t.Fatalf("monitor inspect failure returned before fenced reconcile: %v", recorder.calls)
	}
}

func TestProvisionedRuntimeSpawnerReconcilesSurvivingActiveEpochBeforeRestart(t *testing.T) {
	spawner, provisioner, _, _, recorder := newProvisioningTestSpawner(t)
	paid := provisionedTestPAIDs[0]
	if _, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
	}); err != nil {
		t.Fatal(err)
	}
	// Do not Stop the first process: this models an API crash. Recreate every
	// API-process-owned controller while retaining only the host provisioner's
	// durable view of the active epoch.
	freshAuthorizations := &fakeAuthorizationController{recorder: recorder, current: make(map[string]agentevents.LocalRuntimeAuthorization), fences: make(map[string]int)}
	freshListeners := &fakeListenerController{recorder: recorder, active: make(map[string]bool)}
	restarted, err := newProvisionedRuntimeSpawner(provisionedRuntimeSpawnerConfig{
		Provisioner: provisioner, Authorizations: freshAuthorizations, Listeners: freshListeners,
		Readiness: &fakeRuntimeReadiness{ready: true},
		TenantID:  "tenant-context", Audience: agentevents.DefaultAgentAudience(), Delivery: agentevents.LocalDeliveryRaw,
		BearerTTL: time.Hour, LifecycleTimeout: time.Second, TeardownTimeout: time.Second,
		Activation: runtimeprovision.ActivationConfig{LocalControlServerUID: 65532, LocalControlSocketGID: 20000, AgentWrappingKeyID: "wrapping/v1", ApprovalSecretDigestKey: provisionedTestApprovalKey, ProviderAPIKey: "provider-key", ExecutionReviewerAPIKey: "execution-reviewer-key", ExecutionReviewerModelPreset: "kimi-k3", EscalationReviewerAPIKey: "escalation-reviewer-key", EscalationReviewerModelPreset: "glm-5.2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
	}); err != nil {
		t.Fatal(err)
	}
	if got := provisioner.epochs[paid].Generation; got != 1 {
		t.Fatalf("reconciled restart generation=%d, want 1", got)
	}
	if provisioner.stops[paid] != 1 || freshAuthorizations.fences[paid] != 1 {
		t.Fatalf("surviving active epoch was not fenced/stopped: stops=%d fences=%d", provisioner.stops[paid], freshAuthorizations.fences[paid])
	}
	activation := provisioner.activations[paid]
	if activation.ReapAttestation == nil ||
		activation.ReapAttestation.PersonalityAgentID != paid ||
		activation.ReapAttestation.EpochGeneration != 1 ||
		activation.ReapAttestation.RPCBootNonce != provisioner.epochs[paid].RPCBootNonce ||
		activation.ReapAttestation.ReapedThroughGeneration != 0 {
		t.Fatalf("replacement activation omitted or misbound verified reap attestation: %#v", activation.ReapAttestation)
	}
	want := []string{
		"inspect:" + paid,
		"fence:" + paid,
		"unlisten:" + paid,
		"reconcile:" + paid,
		"stop:" + paid,
		"prepare:" + paid,
	}
	// Match the second lifecycle suffix rather than the initial reconciliation.
	secondStart := 0
	for i, call := range recorder.calls {
		if call == "activate:"+paid {
			secondStart = i + 1
			break
		}
	}
	if !containsOrdered(recorder.calls[secondStart:], want) {
		t.Fatalf("reconcile ordering mismatch: %v", recorder.calls)
	}
}

func TestProvisionedRuntimeSpawnerFencesBeforeReconcileReapsActiveProjectWithOrphan(t *testing.T) {
	spawner, provisioner, _, _, recorder := newProvisioningTestSpawner(t)
	paid := provisionedTestPAIDs[0]
	if _, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
	}); err != nil {
		t.Fatal(err)
	}
	provisioner.mu.Lock()
	provisioner.reconcileReaps[paid] = true
	provisioner.mu.Unlock()
	if _, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
	}); err != nil {
		t.Fatal(err)
	}
	if !containsOrdered(recorder.calls, []string{
		"inspect:" + paid,
		"fence:" + paid,
		"unlisten:" + paid,
		"reconcile:" + paid,
		"reap:" + paid,
		"prepare:" + paid,
	}) {
		t.Fatalf("active orphan reconcile reaped before local authority was fenced: %v", recorder.calls)
	}
}

func TestProvisionedRuntimeSpawnerFencesBeforeReconcileReapsPartialProject(t *testing.T) {
	spawner, provisioner, _, _, recorder := newProvisioningTestSpawner(t)
	paid := provisionedTestPAIDs[0]
	if _, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
	}); err != nil {
		t.Fatal(err)
	}
	provisioner.mu.Lock()
	provisioner.recovery[paid] = true
	provisioner.reconcileReaps[paid] = true
	provisioner.mu.Unlock()
	if _, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
	}); err != nil {
		t.Fatal(err)
	}
	if !containsOrdered(recorder.calls, []string{
		"inspect:" + paid,
		"fence:" + paid,
		"unlisten:" + paid,
		"reconcile:" + paid,
		"reap:" + paid,
		"prepare:" + paid,
	}) {
		t.Fatalf("partial reconcile reaped before local authority was fenced: %v", recorder.calls)
	}
}

func TestProvisionedRuntimeSpawnerRejectsTeardownWithoutObservedEmptyReceipt(t *testing.T) {
	first, provisioner, _, _, recorder := newProvisioningTestSpawner(t)
	paid := provisionedTestPAIDs[0]
	if _, err := first.Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
	}); err != nil {
		t.Fatal(err)
	}
	provisioner.omitReapReceipt = true
	freshAuthorizations := &fakeAuthorizationController{recorder: recorder, current: make(map[string]agentevents.LocalRuntimeAuthorization), fences: make(map[string]int)}
	freshListeners := &fakeListenerController{recorder: recorder, active: make(map[string]bool)}
	restarted, err := newProvisionedRuntimeSpawner(provisionedRuntimeSpawnerConfig{
		Provisioner: provisioner, Authorizations: freshAuthorizations, Listeners: freshListeners,
		Readiness: &fakeRuntimeReadiness{ready: true}, TenantID: "tenant-context",
		Audience: agentevents.DefaultAgentAudience(), Delivery: agentevents.LocalDeliveryRaw,
		TeardownTimeout: time.Second,
		Activation:      runtimeprovision.ActivationConfig{LocalControlServerUID: 65532, LocalControlSocketGID: 20000, AgentWrappingKeyID: "wrapping/v1", ApprovalSecretDigestKey: provisionedTestApprovalKey, ProviderAPIKey: "provider-key", ExecutionReviewerAPIKey: "execution-reviewer-key", ExecutionReviewerModelPreset: "kimi-k3", EscalationReviewerAPIKey: "escalation-reviewer-key", EscalationReviewerModelPreset: "glm-5.2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = restarted.Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
	})
	if err == nil || !strings.Contains(err.Error(), "observed-empty reap receipt") {
		t.Fatalf("unattested teardown was accepted: %v", err)
	}
	if containsOrdered(recorder.calls, []string{"stop:" + paid, "prepare:" + paid}) {
		t.Fatalf("replacement prepare ran after teardown omitted its proof: %v", recorder.calls)
	}
}

func TestProvisionedRuntimeSpawnerFreshProcessReconcilesThreePAIDs(t *testing.T) {
	first, provisioner, _, _, recorder := newProvisioningTestSpawner(t)
	for _, paid := range provisionedTestPAIDs {
		if _, err := first.Spawn(context.Background(), spawn.AgentRuntimeConfig{
			AgentID: paid, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
		}); err != nil {
			t.Fatal(err)
		}
	}
	freshAuthorizations := &fakeAuthorizationController{recorder: recorder, current: make(map[string]agentevents.LocalRuntimeAuthorization), fences: make(map[string]int)}
	freshListeners := &fakeListenerController{recorder: recorder, active: make(map[string]bool)}
	restarted, err := newProvisionedRuntimeSpawner(provisionedRuntimeSpawnerConfig{
		Provisioner: provisioner, Authorizations: freshAuthorizations, Listeners: freshListeners,
		Readiness: &fakeRuntimeReadiness{ready: true},
		TenantID:  "tenant-context", Audience: agentevents.DefaultAgentAudience(), Delivery: agentevents.LocalDeliveryRaw,
		LifecycleTimeout: time.Second, TeardownTimeout: time.Second,
		Activation: runtimeprovision.ActivationConfig{LocalControlServerUID: 65532, LocalControlSocketGID: 20000, AgentWrappingKeyID: "wrapping/v1", ApprovalSecretDigestKey: provisionedTestApprovalKey, ProviderAPIKey: "provider-key", ExecutionReviewerAPIKey: "execution-reviewer-key", ExecutionReviewerModelPreset: "kimi-k3", EscalationReviewerAPIKey: "escalation-reviewer-key", EscalationReviewerModelPreset: "glm-5.2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, paid := range provisionedTestPAIDs {
		if _, err := restarted.Spawn(context.Background(), spawn.AgentRuntimeConfig{
			AgentID: paid, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
		}); err != nil {
			t.Fatal(err)
		}
		if provisioner.epochs[paid].Generation != 1 || provisioner.stops[paid] != 1 || freshAuthorizations.fences[paid] != 1 || !freshListeners.active[paid] {
			t.Fatalf("fresh-process reconcile failed for %s: epoch=%#v stops=%d fences=%d listener=%v", paid, provisioner.epochs[paid], provisioner.stops[paid], freshAuthorizations.fences[paid], freshListeners.active[paid])
		}
	}
}

func TestProvisionedRuntimeSpawnerReconcileUsesFreshDurableControlPlaneAndListenerRegistry(t *testing.T) {
	recorder := &provisioningRecorder{}
	provisioner := newFakeRuntimeProvisioner(recorder)
	commandDir := t.TempDir()
	runtimeDir := privateRuntimeDir(t)
	root, err := os.MkdirTemp("/tmp", "lc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Chmod(root, 0o750); err != nil {
		t.Fatal(err)
	}
	newProcessSide := func() (*agentevents.CommandStore, *agentevents.LocalControlServer, *agentevents.LocalControlListenerRegistry) {
		store, err := agentevents.OpenCommandStore(commandDir)
		if err != nil {
			t.Fatal(err)
		}
		gateway, err := agentevents.OpenDurableGateway(runtimeDir, store)
		if err != nil {
			t.Fatal(err)
		}
		control, err := agentevents.NewLocalControlServer(gateway, []byte("fresh-process-signing-secret-32-bytes"), nil)
		if err != nil {
			t.Fatal(err)
		}
		registry, err := agentevents.NewLocalControlListenerRegistry(control, agentevents.LocalControlListenerRegistryConfig{
			RootDir: root, SocketGID: os.Getegid(),
			OpenListener: func(socketPath string, _ int, _ string) (net.Listener, error) {
				if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
					return nil, err
				}
				listener, err := net.Listen("unix", socketPath)
				if err == nil {
					err = os.Chmod(socketPath, 0o660)
				}
				return listener, err
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return store, control, registry
	}
	newSpawner := func(control *agentevents.LocalControlServer, registry *agentevents.LocalControlListenerRegistry) *provisionedRuntimeSpawner {
		spawner, err := newProvisionedRuntimeSpawner(provisionedRuntimeSpawnerConfig{
			Provisioner: provisioner, Authorizations: control, Listeners: registry,
			Readiness: &fakeRuntimeReadiness{ready: true},
			TenantID:  "tenant", Audience: agentevents.DefaultAgentAudience(), Delivery: agentevents.LocalDeliveryRaw,
			LifecycleTimeout: time.Second, TeardownTimeout: time.Second,
			Activation: runtimeprovision.ActivationConfig{LocalControlServerUID: uint32(os.Geteuid() + 1), LocalControlSocketGID: uint32(os.Getegid() + 1), AgentWrappingKeyID: "key", ApprovalSecretDigestKey: provisionedTestApprovalKey, ProviderAPIKey: "provider", ExecutionReviewerAPIKey: "execution-reviewer-key", ExecutionReviewerModelPreset: "kimi-k3", EscalationReviewerAPIKey: "escalation-reviewer-key", EscalationReviewerModelPreset: "glm-5.2"},
		})
		if err != nil {
			t.Fatal(err)
		}
		return spawner
	}
	paid := provisionedTestPAIDs[0]
	firstStore, firstControl, firstRegistry := newProcessSide()
	if _, err := newSpawner(firstControl, firstRegistry).Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
	}); err != nil {
		t.Fatal(err)
	}
	if err := firstRegistry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := firstStore.Close(); err != nil {
		t.Fatal(err)
	}

	secondStore, secondControl, secondRegistry := newProcessSide()
	defer secondStore.Close()
	defer secondRegistry.Close(context.Background())
	if _, err := newSpawner(secondControl, secondRegistry).Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
	}); err != nil {
		t.Fatal(err)
	}
	if provisioner.epochs[paid].Generation != 1 || provisioner.stops[paid] != 1 {
		t.Fatalf("fresh process did not reconcile host epoch: epoch=%#v stops=%d", provisioner.epochs[paid], provisioner.stops[paid])
	}
	socketPath, err := agentevents.LocalControlSocketPath(root, paid)
	if err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(filepath.Clean(socketPath)); err != nil || info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("fresh registry did not bind replacement socket: info=%v err=%v", info, err)
	}
}

func TestProvisionedRuntimeSpawnerDoesNotReturnBeforeAuthoritativeReady(t *testing.T) {
	spawner, provisioner, _, _, _ := newProvisioningTestSpawner(t)
	readiness := &fakeRuntimeReadiness{}
	spawner.config.Readiness = readiness
	spawner.config.StartupReadyTimeout = time.Second
	paid := provisionedTestPAIDs[0]
	done := make(chan error, 1)
	go func() {
		_, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{
			AgentID: paid, WrappingKey: provisionedTestWrappingMaterial,
			GatewayURL: "ws://gateway.invalid/agent/ws",
		})
		done <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		provisioner.mu.Lock()
		_, activated := provisioner.activations[paid]
		provisioner.mu.Unlock()
		if activated {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("runtime never reached activation")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-done:
		t.Fatalf("spawn returned before Ready: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	readiness.setReady(true)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("spawn did not return after Ready")
	}
}

func TestProvisionedRuntimeSpawnerSurfacesPreReadyRuntimeExit(t *testing.T) {
	spawner, provisioner, authorizations, listeners, _ := newProvisioningTestSpawner(t)
	spawner.config.Readiness = &fakeRuntimeReadiness{}
	spawner.config.StartupReadyTimeout = time.Second
	provisioner.dropBeforeReady = true
	paid := provisionedTestPAIDs[0]
	_, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial,
		GatewayURL: "ws://gateway.invalid/agent/ws",
	})
	if err == nil || !strings.Contains(err.Error(), "left its exact active epoch before Ready") {
		t.Fatalf("pre-Ready runtime exit was not surfaced: %v", err)
	}
	if authorizations.fences[paid] != 1 || listeners.active[paid] {
		t.Fatalf("pre-Ready failure retained authority: fences=%d listener=%v", authorizations.fences[paid], listeners.active[paid])
	}
}

func TestProvisionedRuntimeSpawnerRejectsReadyFromWrongGeneration(t *testing.T) {
	spawner, _, authorizations, listeners, _ := newProvisioningTestSpawner(t)
	wrongGeneration := uint64(99)
	spawner.config.Readiness = &fakeRuntimeReadiness{ready: true, expectedGeneration: &wrongGeneration}
	paid := provisionedTestPAIDs[0]
	_, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial,
		GatewayURL: "ws://gateway.invalid/agent/ws",
	})
	if err == nil || !strings.Contains(err.Error(), "stale readiness generation") {
		t.Fatalf("wrong-generation Ready satisfied activation: %v", err)
	}
	if authorizations.fences[paid] != 1 || listeners.active[paid] {
		t.Fatal("wrong-generation Ready retained runtime authority")
	}
}

func TestProvisionedRuntimeSpawnerRejectsReadyThenExit(t *testing.T) {
	spawner, provisioner, authorizations, listeners, _ := newProvisioningTestSpawner(t)
	paid := provisionedTestPAIDs[0]
	readiness := &fakeRuntimeReadiness{ready: true}
	readiness.onObserve = func() {
		provisioner.mu.Lock()
		delete(provisioner.epochs, paid)
		provisioner.mu.Unlock()
	}
	spawner.config.Readiness = readiness
	_, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial,
		GatewayURL: "ws://gateway.invalid/agent/ws",
	})
	if err == nil || !strings.Contains(err.Error(), "left its exact active epoch before Ready") {
		t.Fatalf("Ready-then-exit runtime satisfied activation: %v", err)
	}
	if authorizations.fences[paid] != 1 || listeners.active[paid] {
		t.Fatal("Ready-then-exit runtime retained authority")
	}
}

func containsOrdered(haystack, needles []string) bool {
	next := 0
	for _, value := range haystack {
		if next < len(needles) && value == needles[next] {
			next++
		}
	}
	return next == len(needles)
}
