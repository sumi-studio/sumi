package runtimeprovision

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

const (
	testPAID  = "0198f0f4-9b72-7000-8000-000000000001"
	testPAID2 = "0198f0f4-9b72-7000-8000-000000000002"
)

func testActivationConfig() ActivationConfig {
	return ActivationConfig{
		GatewayURL:                      "wss://gateway.invalid",
		LocalControlBearer:              "bearer",
		LocalControlBearerExpiresAtUnix: 2_000_000_000,
		LocalControlServerUID:           65532,
		LocalControlSocketGID:           20000,
		AgentWrappingKey:                "wrapping-key",
		AgentWrappingKeyID:              "wrapping/v1",
		ApprovalSecretDigestKey:         "approval-key",
		ProviderAPIKey:                  "provider-key",
	}
}

type fakeBackend struct {
	mu             sync.Mutex
	state          map[string]Inspection
	nextGeneration map[string]uint64
	prepareCalls   map[string]int
	activateCalls  map[string]int
	abortCalls     map[string]int
	stopCalls      map[string]int
	privateVolumes map[string]bool
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		state:          make(map[string]Inspection),
		nextGeneration: make(map[string]uint64),
		prepareCalls:   make(map[string]int),
		activateCalls:  make(map[string]int),
		abortCalls:     make(map[string]int),
		stopCalls:      make(map[string]int),
		privateVolumes: make(map[string]bool),
	}
}

func (backend *fakeBackend) Prepare(_ context.Context, request PrepareRequest) (PreparedEpoch, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.prepareCalls[request.PersonalityAgentID]++
	generation := backend.nextGeneration[request.PersonalityAgentID]
	backend.nextGeneration[request.PersonalityAgentID] = generation + 1
	epoch := PreparedEpoch{
		PersonalityAgentID:   request.PersonalityAgentID,
		Generation:           generation,
		RPCBootNonce:         fmt.Sprintf("nonce-%d", generation),
		OpaquePreparedHandle: fmt.Sprintf("fake-handle-%d", generation),
	}
	backend.privateVolumes[request.PersonalityAgentID] = true
	backend.state[request.PersonalityAgentID] = Inspection{
		PersonalityAgentID: request.PersonalityAgentID,
		Phase:              PhasePrepared,
		Epoch:              &epoch,
	}
	return epoch, nil
}

func (backend *fakeBackend) Activate(_ context.Context, request ActivateRequest) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	inspection := backend.state[request.PersonalityAgentID]
	if inspection.Epoch == nil || *inspection.Epoch != request.PreparedEpoch {
		return ErrConflict
	}
	backend.activateCalls[request.PersonalityAgentID]++
	inspection.Phase = PhaseActive
	backend.state[request.PersonalityAgentID] = inspection
	return nil
}

func (backend *fakeBackend) Abort(_ context.Context, epoch PreparedEpoch) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.abortCalls[epoch.PersonalityAgentID]++
	backend.state[epoch.PersonalityAgentID] = unknownInspection(epoch.PersonalityAgentID)
	return nil
}

func (backend *fakeBackend) Inspect(_ context.Context, personalityAgentID string) (Inspection, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	inspection, ok := backend.state[personalityAgentID]
	if !ok {
		return unknownInspection(personalityAgentID), nil
	}
	return cloneInspection(inspection), nil
}

func (backend *fakeBackend) Stop(_ context.Context, epoch PreparedEpoch) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.stopCalls[epoch.PersonalityAgentID]++
	backend.state[epoch.PersonalityAgentID] = unknownInspection(epoch.PersonalityAgentID)
	return nil
}

func (backend *fakeBackend) Reconcile(ctx context.Context, personalityAgentID string) (Inspection, error) {
	return backend.Inspect(ctx, personalityAgentID)
}

func cloneInspection(inspection Inspection) Inspection {
	if inspection.Epoch != nil {
		epoch := *inspection.Epoch
		inspection.Epoch = &epoch
	}
	return inspection
}

func TestServiceFakeBackendContract(t *testing.T) {
	backend := newFakeBackend()
	service, err := NewService(backend)
	if err != nil {
		t.Fatal(err)
	}
	request := PrepareRequest{Version: ProtocolVersion, PersonalityAgentID: testPAID, IdempotencyKey: "request-1"}

	const callers = 24
	results := make(chan PreparedEpoch, callers)
	errorsFound := make(chan error, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			epoch, prepareErr := service.Prepare(context.Background(), request)
			results <- epoch
			errorsFound <- prepareErr
		}()
	}
	wait.Wait()
	close(results)
	close(errorsFound)
	var expected PreparedEpoch
	for prepareErr := range errorsFound {
		if prepareErr != nil {
			t.Fatalf("concurrent prepare: %v", prepareErr)
		}
	}
	for epoch := range results {
		if expected.PersonalityAgentID == "" {
			expected = epoch
		}
		if epoch != expected {
			t.Fatalf("prepare results differ: %#v != %#v", epoch, expected)
		}
	}
	if backend.prepareCalls[testPAID] != 1 {
		t.Fatalf("allocator ran %d times, want exactly once", backend.prepareCalls[testPAID])
	}

	activate := ActivateRequest{Version: ProtocolVersion, PreparedEpoch: expected, Activation: testActivationConfig()}
	for range 2 {
		inspection, activateErr := service.Activate(context.Background(), activate)
		if activateErr != nil || inspection.Phase != PhaseActive {
			t.Fatalf("activate: inspection=%#v err=%v", inspection, activateErr)
		}
	}
	if backend.prepareCalls[testPAID] != 1 || backend.activateCalls[testPAID] != 1 {
		t.Fatalf("activate reran backend: prepare=%d activate=%d", backend.prepareCalls[testPAID], backend.activateCalls[testPAID])
	}

	stop := StopRequest{Version: ProtocolVersion, PreparedEpoch: expected}
	for range 2 {
		if _, stopErr := service.Stop(context.Background(), stop); stopErr != nil {
			t.Fatal(stopErr)
		}
	}
	if backend.stopCalls[testPAID] != 1 {
		t.Fatalf("stop called %d times", backend.stopCalls[testPAID])
	}
	if !backend.privateVolumes[testPAID] {
		t.Fatal("ordinary stop removed the personality agent's private volumes")
	}

	next := request
	next.IdempotencyKey = "request-2"
	nextEpoch, err := service.Prepare(context.Background(), next)
	if err != nil {
		t.Fatal(err)
	}
	if nextEpoch.Generation != expected.Generation+1 || backend.prepareCalls[testPAID] != 2 {
		t.Fatalf("new lifecycle did not allocate exactly the next epoch: %#v", nextEpoch)
	}
	abort := AbortRequest{Version: ProtocolVersion, PreparedEpoch: nextEpoch}
	for range 2 {
		if _, abortErr := service.Abort(context.Background(), abort); abortErr != nil {
			t.Fatal(abortErr)
		}
	}
	if backend.abortCalls[testPAID] != 1 {
		t.Fatalf("abort called %d times", backend.abortCalls[testPAID])
	}
}

func TestPrepareRecoversCommittedBackendEpochWithoutAllocatingAgain(t *testing.T) {
	backend := newFakeBackend()
	committed, err := backend.Prepare(context.Background(), PrepareRequest{PersonalityAgentID: testPAID})
	if err != nil {
		t.Fatal(err)
	}
	service, _ := NewService(backend)
	recovered, err := service.Prepare(context.Background(), PrepareRequest{
		Version: ProtocolVersion, PersonalityAgentID: testPAID, IdempotencyKey: "retry-after-daemon-restart",
	})
	if err != nil {
		t.Fatal(err)
	}
	if recovered != committed || backend.prepareCalls[testPAID] != 1 {
		t.Fatalf("recovery allocated again: recovered=%#v committed=%#v calls=%d", recovered, committed, backend.prepareCalls[testPAID])
	}
}

func TestStaleStopCannotTearDownReplacementEpoch(t *testing.T) {
	backend := newFakeBackend()
	service, err := NewService(backend)
	if err != nil {
		t.Fatal(err)
	}
	request := PrepareRequest{
		Version: ProtocolVersion, PersonalityAgentID: testPAID, IdempotencyKey: "epoch-n",
	}
	first, err := service.Prepare(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Activate(context.Background(), ActivateRequest{
		Version: ProtocolVersion, PreparedEpoch: first, Activation: testActivationConfig(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Stop(context.Background(), StopRequest{
		Version: ProtocolVersion, PreparedEpoch: first,
	}); err != nil {
		t.Fatal(err)
	}

	request.IdempotencyKey = "epoch-n-plus-one"
	second, err := service.Prepare(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Activate(context.Background(), ActivateRequest{
		Version: ProtocolVersion, PreparedEpoch: second, Activation: testActivationConfig(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Stop(context.Background(), StopRequest{
		Version: ProtocolVersion, PreparedEpoch: first,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale stop error=%v, want ErrConflict", err)
	}
	inspection, err := service.Inspect(context.Background(), InspectRequest{
		Version: ProtocolVersion, PersonalityAgentID: testPAID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Phase != PhaseActive || inspection.Epoch == nil || *inspection.Epoch != second {
		t.Fatalf("stale stop disturbed replacement: %#v", inspection)
	}
	if got := backend.stopCalls[testPAID]; got != 1 {
		t.Fatalf("stale stop reached backend: stop calls=%d", got)
	}
}

func TestStableNamespaceDependsOnlyOnCanonicalPAID(t *testing.T) {
	first, err := NamespaceFor(testPAID)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := NamespaceFor(testPAID)
	second, _ := NamespaceFor(testPAID2)
	if first != again {
		t.Fatalf("same PAID produced unstable namespace: %#v != %#v", first, again)
	}
	if first.Project == second.Project || first.VolumePrefix == second.VolumePrefix || first.IPCPrefix == second.IPCPrefix {
		t.Fatalf("distinct PAIDs shared a stable namespace: %#v %#v", first, second)
	}
	if first.Project != "sumi-0198f0f49b7270008000000000000001" {
		t.Fatalf("unexpected canonical project: %s", first.Project)
	}
}
