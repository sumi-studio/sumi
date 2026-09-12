package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/sumi-studio/sumi/apps/api/internal/chatgpt"
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
	stopErr         error
	reconcileErr    error
	inspectErr      error
	inspectErrLimit int
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

func (p *fakeRuntimeProvisioner) RecoverLocalControl(_ context.Context, request runtimeprovision.RecoverLocalControlRequest) (runtimeprovision.RecoveredLocalControl, error) {
	p.recorder.add("recover:" + request.PersonalityAgentID)
	p.mu.Lock()
	defer p.mu.Unlock()
	epoch, ok := p.epochs[request.PersonalityAgentID]
	if !ok || epoch != request.PreparedEpoch || p.recovery[request.PersonalityAgentID] || p.reconcileReaps[request.PersonalityAgentID] {
		return runtimeprovision.RecoveredLocalControl{}, runtimeprovision.ErrConflict
	}
	activation := p.activations[request.PersonalityAgentID]
	return runtimeprovision.RecoveredLocalControl{PreparedEpoch: epoch, Bearer: activation.LocalControlBearer, SelectionFingerprint: activation.SelectionFingerprint()}, nil
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
	if p.stopErr != nil {
		return runtimeprovision.Inspection{}, p.stopErr
	}
	p.recorder.add("stop:" + request.PersonalityAgentID)
	p.mu.Lock()
	defer p.mu.Unlock()
	current, exists := p.epochs[request.PersonalityAgentID]
	if !exists || current != request.PreparedEpoch {
		return runtimeprovision.Inspection{}, runtimeprovision.ErrConflict
	}
	if p.recovery[request.PersonalityAgentID] {
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
	if p.reconcileErr != nil {
		return runtimeprovision.Inspection{}, p.reconcileErr
	}
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
	p.mu.Lock()
	if p.inspectErr != nil {
		err := p.inspectErr
		if p.inspectErrLimit > 0 {
			p.inspectErrLimit--
			if p.inspectErrLimit == 0 {
				p.inspectErr = nil
			}
		}
		p.mu.Unlock()
		return runtimeprovision.Inspection{}, err
	}
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
	if p.recovery[request.PersonalityAgentID] || p.reconcileReaps[request.PersonalityAgentID] {
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

func (c *fakeAuthorizationController) RecoverLocalRuntimeAuthorization(ctx context.Context, authorization agentevents.LocalRuntimeAuthorization) error {
	return c.InstallLocalRuntimeAuthorization(ctx, authorization)
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

func TestProvisionedProcessStopReconcilesRecoveryInsteadOfLeavingPartialRuntime(t *testing.T) {
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

	if err := process.Stop(); err != nil {
		t.Fatalf("recovery Stop = %v, want fenced reconciliation", err)
	}
	if _, exists := provisioner.epochs[paid]; exists {
		t.Fatal("recovery Stop left the partial runtime behind")
	}
	if reaped, ok := provisioner.reapedThrough[paid]; !ok || reaped != 0 {
		t.Fatalf("recovery Stop did not return an exact observed-empty receipt: reaped=%d present=%t", reaped, ok)
	}
	if authorizations.fences[paid] != 2 || listeners.active[paid] {
		t.Fatalf("recovery Stop retained local authority: fences=%d listener=%t", authorizations.fences[paid], listeners.active[paid])
	}
	if !containsOrdered(recorder.calls, []string{
		"fence:" + paid,
		"stop:" + paid,
		"inspect:" + paid,
		"fence:" + paid,
		"unlisten:" + paid,
		"reconcile:" + paid,
		"reap:" + paid,
	}) {
		t.Fatalf("recovery Stop did not fence and reconcile before returning: %v", recorder.calls)
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

type monitoringTestProvisioner struct {
	mu sync.Mutex
	*fakeRuntimeProvisioner
	inspect func(context.Context, runtimeprovision.InspectRequest) (runtimeprovision.Inspection, error)
}

func (p *monitoringTestProvisioner) Inspect(ctx context.Context, request runtimeprovision.InspectRequest) (runtimeprovision.Inspection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.inspect(ctx, request)
}

func (p *monitoringTestProvisioner) Stop(ctx context.Context, request runtimeprovision.StopRequest) (runtimeprovision.Inspection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fakeRuntimeProvisioner.Stop(ctx, request)
}

func TestProvisionedProcessMonitorPreservesRuntimeDuringObservationLoss(t *testing.T) {
	for _, recoverObservation := range []bool{true, false} {
		t.Run(fmt.Sprintf("recover=%t", recoverObservation), func(t *testing.T) {
			spawner, provisioner, authorizations, listeners, recorder := newProvisioningTestSpawner(t)
			paid := provisionedTestPAIDs[0]
			process, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{
				AgentID: paid, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
			})
			if err != nil {
				t.Fatal(err)
			}
			p := process.(*provisionedProcess)
			p.monitorInterval = time.Millisecond
			p.timeout = time.Minute
			epoch := p.epoch
			recorder.calls = nil
			observed := make(chan struct{})
			attempts := 0
			p.provisioner = &monitoringTestProvisioner{fakeRuntimeProvisioner: provisioner,
				inspect: func(ctx context.Context, request runtimeprovision.InspectRequest) (runtimeprovision.Inspection, error) {
					attempts++
					if attempts <= 5 {
						return runtimeprovision.Inspection{}, errors.New("private inspection failure")
					}
					if recoverObservation {
						inspection, err := provisioner.Inspect(ctx, request)
						if attempts == 6 {
							close(observed)
						}
						return inspection, err
					}
					if attempts == 6 {
						close(observed)
					}
					<-ctx.Done() // Stop must also interrupt an outstanding observation.
					return runtimeprovision.Inspection{}, ctx.Err()
				},
			}
			waitErr := make(chan error, 1)
			go func() { waitErr <- p.Wait() }()
			t.Cleanup(func() {
				if p.monitorCancel != nil {
					p.monitorCancel()
				}
				_ = p.Stop()
			})
			select {
			case <-observed:
			case err := <-waitErr:
				t.Fatalf("monitor retired a runtime on observation failure: %v", err)
			case <-time.After(time.Second):
				t.Fatal("monitor stopped retrying observation")
			}
			provisioner.mu.Lock()
			currentEpoch, stopped := provisioner.epochs[paid], provisioner.stops[paid]
			provisioner.mu.Unlock()
			if currentEpoch != epoch || stopped != 0 {
				t.Fatal("observation failure replaced or stopped the runtime")
			}
			authorizations.mu.Lock()
			fences := authorizations.fences[paid]
			authorizations.mu.Unlock()
			listeners.mu.Lock()
			listening := listeners.active[paid]
			listeners.mu.Unlock()
			if fences != 0 || !listening {
				t.Fatalf("observation failure removed authority: fences=%d listening=%t", fences, listening)
			}
			recorder.mu.Lock()
			for _, call := range recorder.calls {
				if call == "reconcile:"+paid || call == "stop:"+paid || call == "fence:"+paid {
					recorder.mu.Unlock()
					t.Fatalf("unexpected lifecycle action while observing: %s", call)
				}
			}
			recorder.mu.Unlock()
			stopErr := make(chan error, 1)
			go func() { stopErr <- p.Stop() }()
			select {
			case err := <-stopErr:
				if err != nil {
					t.Fatalf("explicit stop during observation loss = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Stop waited for the inspection timeout instead of canceling observation")
			}
			select {
			case err := <-waitErr:
				if err != nil {
					t.Fatalf("Wait after explicit stop = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("explicit stop did not release the monitor")
			}
		})
	}
}

func TestProvisionedRuntimeSpawnerAdoptsSurvivingActiveEpochAfterRestart(t *testing.T) {
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
		LifecycleTimeout: time.Second, TeardownTimeout: time.Second,
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
	if got := provisioner.epochs[paid].Generation; got != 0 {
		t.Fatalf("adopted generation=%d, want unchanged 0", got)
	}
	if provisioner.stops[paid] != 0 || freshAuthorizations.fences[paid] != 0 {
		t.Fatal("adoption retired the existing runtime")
	}
	if freshAuthorizations.current[paid].BearerToken != provisioner.activations[paid].LocalControlBearer {
		t.Fatal("adoption changed the live bearer")
	}
	if !containsOrdered(recorder.calls, []string{"recover:" + paid, "authorize:" + paid, "listen:" + paid}) {
		t.Fatalf("adoption did not authorize before listening: %v", recorder.calls)
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
	provisioner.recovery[paid] = true
	provisioner.reconcileReaps[paid] = true
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

func TestProvisionedRuntimeSpawnerFreshProcessAdoptsThreePAIDs(t *testing.T) {
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
		if provisioner.epochs[paid].Generation != 0 || provisioner.stops[paid] != 0 || freshAuthorizations.fences[paid] != 0 || !freshListeners.active[paid] {
			t.Fatalf("fresh-process reconcile failed for %s: epoch=%#v stops=%d fences=%d listener=%v", paid, provisioner.epochs[paid], provisioner.stops[paid], freshAuthorizations.fences[paid], freshListeners.active[paid])
		}
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

func TestProvisionedRuntimeSpawnerPreReadyRecoveryReconcilesExactEpochBeforeFailing(t *testing.T) {
	spawner, provisioner, authorizations, listeners, recorder := newProvisioningTestSpawner(t)
	paid := provisionedTestPAIDs[0]
	readiness := &fakeRuntimeReadiness{terminal: true}
	readiness.onObserve = func() {
		provisioner.mu.Lock()
		defer provisioner.mu.Unlock()
		provisioner.recovery[paid] = true
		provisioner.reconcileReaps[paid] = true
	}
	spawner.config.Readiness = readiness

	_, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial,
		GatewayURL: "ws://gateway.invalid/agent/ws",
	})
	if err == nil || !strings.Contains(err.Error(), "terminal NotReady") {
		t.Fatalf("pre-Ready recovery spawn error = %v, want terminal readiness failure", err)
	}
	if _, exists := provisioner.epochs[paid]; exists {
		t.Fatal("pre-Ready recovery left the partial runtime behind")
	}
	if reaped, ok := provisioner.reapedThrough[paid]; !ok || reaped != 0 {
		t.Fatalf("pre-Ready recovery did not produce the exact observed-empty receipt: reaped=%d present=%t", reaped, ok)
	}
	if authorizations.fences[paid] != 2 || listeners.active[paid] {
		t.Fatalf("pre-Ready recovery retained local authority: fences=%d listener=%t", authorizations.fences[paid], listeners.active[paid])
	}
	if !containsOrdered(recorder.calls, []string{
		"fence:" + paid,
		"stop:" + paid,
		"unlisten:" + paid,
		"inspect:" + paid,
		"fence:" + paid,
		"unlisten:" + paid,
		"reconcile:" + paid,
		"reap:" + paid,
	}) {
		t.Fatalf("pre-Ready recovery did not fence and reconcile before returning: %v", recorder.calls)
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

type validatingRuntimeProvisioner struct{ *fakeRuntimeProvisioner }

func (p validatingRuntimeProvisioner) Activate(ctx context.Context, request runtimeprovision.ActivateRequest) (runtimeprovision.Inspection, error) {
	if err := request.Activation.Validate(); err != nil {
		return runtimeprovision.Inspection{}, err
	}
	return p.fakeRuntimeProvisioner.Activate(ctx, request)
}
func TestProvisionedRuntimeSelectsChatGPTWithoutFallbackConversationKey(t *testing.T) {
	for _, tc := range []struct {
		name                string
		connected, reviewer bool
		pass                bool
	}{
		{"connected native", true, true, true},
		{"unconnected API fallback lacks key", false, true, false},
		{"native still requires reviewer", true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spawner, provisioner, _, _, _ := newProvisioningTestSpawner(t)
			spawner.config.Provisioner = validatingRuntimeProvisioner{provisioner}
			spawner.config.Activation.ProviderAPIKey = ""
			if !tc.reviewer {
				spawner.config.Activation.ExecutionReviewerAPIKey = ""
			}
			connection := &chatGPTTestStore{status: chatgpt.Status{Connected: tc.connected, ConnectionID: "connection", AccountID: "account", Selection: chatgpt.Selection{Model: "gpt-6-astra", Effort: "medium"}}}
			native := &chatGPTRuntime{connections: connection, employers: &chatGPTTestEmployer{kind: "human"}}
			spawner.config.ResolveActivation = native.activation
			process, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{AgentID: provisionedTestPAIDs[0], WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws"})
			if (err == nil) != tc.pass {
				t.Fatalf("unexpected selected activation result: %v", err)
			}
			if process != nil {
				if err := process.Stop(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestProvisionedMonitorCleanupFailureCanBeRetriedByStop(t *testing.T) {
	spawner, provisioner, _, _, _ := newProvisioningTestSpawner(t)
	paid := provisionedTestPAIDs[0]
	process, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
	})
	if err != nil {
		t.Fatal(err)
	}
	provisioner.recovery[paid] = true
	provisioner.reconcileReaps[paid] = true
	failure := errors.New("temporary reconcile outage")
	provisioner.reconcileErr = failure
	p := process.(*provisionedProcess)
	p.monitorInterval = time.Millisecond
	if err := p.Wait(); !errors.Is(err, spawn.ErrCleanupIncomplete) || !errors.Is(err, failure) {
		t.Fatalf("wait=%v", err)
	}
	if err := p.Stop(); !errors.Is(err, failure) {
		t.Fatalf("persistent stop=%v", err)
	}
	provisioner.reconcileErr = nil
	if err := p.Stop(); err != nil {
		t.Fatalf("retry stop=%v", err)
	}
	if _, exists := provisioner.epochs[paid]; exists {
		t.Fatal("writer epoch not reaped")
	}
	if err := p.Stop(); err != nil {
		t.Fatalf("idempotent stop=%v", err)
	}
}

func TestProvisionedExplicitStopRetriesAfterPhysicalTeardownFailure(t *testing.T) {
	spawner, provisioner, _, _, _ := newProvisioningTestSpawner(t)
	paid := provisionedTestPAIDs[0]
	process, err := spawner.Spawn(context.Background(), spawn.AgentRuntimeConfig{
		AgentID: paid, WrappingKey: provisionedTestWrappingMaterial, GatewayURL: "ws://gateway.invalid/agent/ws",
	})
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("temporary stop outage")
	provisioner.stopErr = failure
	if err := process.Stop(); !errors.Is(err, failure) {
		t.Fatalf("stop=%v", err)
	}
	if err := process.Wait(); !errors.Is(err, spawn.ErrCleanupIncomplete) {
		t.Fatalf("wait=%v", err)
	}
	provisioner.stopErr = nil
	if err := process.Stop(); err != nil {
		t.Fatalf("retry=%v", err)
	}
	if _, exists := provisioner.epochs[paid]; exists {
		t.Fatal("writer remains")
	}
	if err := process.Wait(); err != nil {
		t.Fatalf("recovered wait=%v", err)
	}
}
