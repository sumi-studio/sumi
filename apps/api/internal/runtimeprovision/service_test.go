package runtimeprovision

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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
		AgentWrappingKey:                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		AgentWrappingKeyID:              "wrapping/v1",
		ApprovalSecretDigestKey:         "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		ProviderAPIKey:                  "provider-key",
		ExecutionReviewerAPIKey:         "execution-reviewer-key",
		ExecutionReviewerModelPreset:    "kimi-k3",
		EscalationReviewerAPIKey:        "escalation-reviewer-key",
		EscalationReviewerModelPreset:   "glm-5.2",
	}
}

func TestActivationConfigRequiresBothReviewerBoundaries(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ActivationConfig)
	}{
		{
			name: "missing execution credential",
			mutate: func(config *ActivationConfig) {
				config.ExecutionReviewerAPIKey = ""
			},
		},
		{
			name: "missing execution preset",
			mutate: func(config *ActivationConfig) {
				config.ExecutionReviewerModelPreset = ""
			},
		},
		{
			name: "missing escalation credential",
			mutate: func(config *ActivationConfig) {
				config.EscalationReviewerAPIKey = ""
			},
		},
		{
			name: "missing escalation preset",
			mutate: func(config *ActivationConfig) {
				config.EscalationReviewerModelPreset = ""
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := testActivationConfig()
			test.mutate(&config)
			if err := config.Validate(); err == nil {
				t.Fatal("incomplete reviewer boundary was accepted")
			}
		})
	}

	config := testActivationConfig()
	if err := config.Validate(); err != nil {
		t.Fatalf("complete reviewer boundaries were rejected: %v", err)
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
	reconcileReaps map[string]uint64
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
		reconcileReaps: make(map[string]uint64),
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

func (backend *fakeBackend) Abort(_ context.Context, epoch PreparedEpoch) (Inspection, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.abortCalls[epoch.PersonalityAgentID]++
	reaped := epoch.Generation
	inspection := unknownInspection(epoch.PersonalityAgentID)
	inspection.ReapedThroughGeneration = &reaped
	backend.state[epoch.PersonalityAgentID] = unknownInspection(epoch.PersonalityAgentID)
	return inspection, nil
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

func (backend *fakeBackend) Stop(_ context.Context, epoch PreparedEpoch) (Inspection, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.stopCalls[epoch.PersonalityAgentID]++
	reaped := epoch.Generation
	inspection := unknownInspection(epoch.PersonalityAgentID)
	inspection.ReapedThroughGeneration = &reaped
	backend.state[epoch.PersonalityAgentID] = unknownInspection(epoch.PersonalityAgentID)
	return inspection, nil
}

func (backend *fakeBackend) Reconcile(ctx context.Context, personalityAgentID string) (Inspection, error) {
	backend.mu.Lock()
	if generation, ok := backend.reconcileReaps[personalityAgentID]; ok {
		backend.mu.Unlock()
		inspection := unknownInspection(personalityAgentID)
		inspection.ReapedThroughGeneration = &generation
		return inspection, nil
	}
	backend.mu.Unlock()
	return backend.Inspect(ctx, personalityAgentID)
}

func cloneInspection(inspection Inspection) Inspection {
	if inspection.Epoch != nil {
		epoch := *inspection.Epoch
		inspection.Epoch = &epoch
	}
	if inspection.ReapedThroughGeneration != nil {
		reaped := *inspection.ReapedThroughGeneration
		inspection.ReapedThroughGeneration = &reaped
	}
	return inspection
}

func newTestService(t *testing.T, backend Backend) *Service {
	t.Helper()
	service, err := NewService(backend, ServiceConfig{StateDirectory: filepath.Join(t.TempDir(), "state")})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestServiceFakeBackendContract(t *testing.T) {
	backend := newFakeBackend()
	service := newTestService(t, backend)
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
		inspection, stopErr := service.Stop(context.Background(), stop)
		if stopErr != nil {
			t.Fatal(stopErr)
		}
		if inspection.ReapedThroughGeneration == nil || *inspection.ReapedThroughGeneration != expected.Generation {
			t.Fatalf("stop did not retain exact reap receipt: %#v", inspection)
		}
	}
	if backend.stopCalls[testPAID] != 1 {
		t.Fatalf("stop called %d times", backend.stopCalls[testPAID])
	}
	if !backend.privateVolumes[testPAID] {
		t.Fatal("ordinary stop removed the personality agent's private volumes")
	}
	reconciled, err := service.Reconcile(context.Background(), ReconcileRequest{
		Version: ProtocolVersion, PersonalityAgentID: testPAID,
	})
	if err != nil || reconciled.ReapedThroughGeneration == nil || *reconciled.ReapedThroughGeneration != expected.Generation {
		t.Fatalf("unknown reconciliation lost highest verified reap: %#v %v", reconciled, err)
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
	service := newTestService(t, backend)
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
	service := newTestService(t, backend)
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

func TestVerifiedStopReapAttestationSurvivesProvisionerRestart(t *testing.T) {
	backend := newFakeBackend()
	stateDirectory := filepath.Join(t.TempDir(), "state")
	first, err := NewService(backend, ServiceConfig{StateDirectory: stateDirectory})
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := first.Prepare(context.Background(), PrepareRequest{
		Version: ProtocolVersion, PersonalityAgentID: testPAID, IdempotencyKey: "first",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Activate(context.Background(), ActivateRequest{
		Version: ProtocolVersion, PreparedEpoch: epoch, Activation: testActivationConfig(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Stop(context.Background(), StopRequest{
		Version: ProtocolVersion, PreparedEpoch: epoch,
	}); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewService(backend, ServiceConfig{StateDirectory: stateDirectory})
	if err != nil {
		t.Fatal(err)
	}
	inspection, err := restarted.Reconcile(context.Background(), ReconcileRequest{
		Version: ProtocolVersion, PersonalityAgentID: testPAID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if inspection.ReapedThroughGeneration == nil || *inspection.ReapedThroughGeneration != epoch.Generation {
		t.Fatalf("restarted provisioner lost verified reap: %#v", inspection)
	}

	raw, err := os.ReadFile(filepath.Join(stateDirectory, reapStateFileName))
	if err != nil {
		t.Fatal(err)
	}
	var document reapStateDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	if len(document.PersonalityAgents) != 1 ||
		document.PersonalityAgents[testPAID].ReapedThroughGeneration == nil ||
		*document.PersonalityAgents[testPAID].ReapedThroughGeneration != epoch.Generation {
		t.Fatalf("durable reap state has the wrong per-PA shape: %s", raw)
	}
}

func TestDurableReapStateRejectsMissingGeneration(t *testing.T) {
	stateDirectory := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(stateDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	malformed := `{"version":1,"personality_agents":{"` + testPAID + `":{}}}`
	if err := os.WriteFile(filepath.Join(stateDirectory, reapStateFileName), []byte(malformed), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewService(newFakeBackend(), ServiceConfig{StateDirectory: stateDirectory}); err == nil || !strings.Contains(err.Error(), "reaped_through_generation is required") {
		t.Fatalf("missing durable reap generation was accepted: %v", err)
	}
}

func TestDurableReapStateAtMaximumSizeRoundTrips(t *testing.T) {
	stateDirectory := filepath.Join(t.TempDir(), "state")
	state, err := newDurableReapState(stateDirectory)
	if err != nil {
		t.Fatal(err)
	}
	fillReapStateToMaximumSize(t, state)
	if err := state.persist(); err != nil {
		t.Fatalf("persist reap state at maximum size: %v", err)
	}
	info, err := os.Stat(state.path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != maxReapStateBytes {
		t.Fatalf("persisted reap state has size %d, want exact limit %d", info.Size(), maxReapStateBytes)
	}

	restarted, err := newDurableReapState(stateDirectory)
	if err != nil {
		t.Fatalf("load reap state at maximum size: %v", err)
	}
	if !reflect.DeepEqual(restarted.entries, state.entries) {
		t.Fatal("maximum-size reap state did not round-trip")
	}
}

func TestDurableReapStateRejectsOversizeBeforePublishing(t *testing.T) {
	stateDirectory := filepath.Join(t.TempDir(), "state")
	state, err := newDurableReapState(stateDirectory)
	if err != nil {
		t.Fatal(err)
	}
	nextPersonalityAgentID := fillReapStateToMaximumSize(t, state)
	if err := state.persist(); err != nil {
		t.Fatalf("persist reap state at maximum size: %v", err)
	}
	if err := state.record(nextPersonalityAgentID, 0); err == nil || !strings.Contains(err.Error(), "would exceed the maximum allowed size") {
		t.Fatalf("oversize reap state write was accepted or unclear: %v", err)
	}
	if _, ok := state.entries[nextPersonalityAgentID]; ok {
		t.Fatal("oversize reap state write remained in memory")
	}
	info, err := os.Stat(state.path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != maxReapStateBytes {
		t.Fatalf("oversize reap state write published %d bytes, want %d", info.Size(), maxReapStateBytes)
	}
	if _, err := newDurableReapState(stateDirectory); err != nil {
		t.Fatalf("state after rejected oversize write no longer loads: %v", err)
	}
}

func fillReapStateToMaximumSize(t *testing.T, state *durableReapState) string {
	t.Helper()
	next := 0
	for {
		personalityAgentID := reapStateTestPersonalityAgentID(next)
		state.entries[personalityAgentID] = 0
		encoded, err := encodeReapStateDocument(reapStateDocumentForEntries(state.entries))
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) > maxReapStateBytes {
			delete(state.entries, personalityAgentID)
			break
		}
		next++
	}
	encoded, err := encodeReapStateDocument(reapStateDocumentForEntries(state.entries))
	if err != nil {
		t.Fatal(err)
	}
	remaining := maxReapStateBytes - len(encoded)
	for index := 0; remaining > 0; index++ {
		extraDigits := min(remaining, 18)
		generation := uint64(1)
		for range extraDigits {
			generation *= 10
		}
		state.entries[reapStateTestPersonalityAgentID(index)] = generation
		remaining -= extraDigits
	}
	encoded, err = encodeReapStateDocument(reapStateDocumentForEntries(state.entries))
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != maxReapStateBytes {
		t.Fatalf("maximum reap state fixture has size %d, want %d", len(encoded), maxReapStateBytes)
	}
	return reapStateTestPersonalityAgentID(next)
}

func reapStateDocumentForEntries(entries map[string]uint64) reapStateDocument {
	document := reapStateDocument{
		Version:           reapStateVersion,
		PersonalityAgents: make(map[string]reapStateAgentRecord, len(entries)),
	}
	for personalityAgentID, generation := range entries {
		reapedThroughGeneration := generation
		document.PersonalityAgents[personalityAgentID] = reapStateAgentRecord{
			ReapedThroughGeneration: &reapedThroughGeneration,
		}
	}
	return document
}

func reapStateTestPersonalityAgentID(index int) string {
	return fmt.Sprintf("00000000-0000-7000-8000-%012x", index)
}

func TestReconcileObservedEmptyReapIsPersistedAcrossRestart(t *testing.T) {
	backend := newFakeBackend()
	stateDirectory := filepath.Join(t.TempDir(), "state")
	backend.reconcileReaps[testPAID] = 7
	first, err := NewService(backend, ServiceConfig{StateDirectory: stateDirectory})
	if err != nil {
		t.Fatal(err)
	}
	inspection, err := first.Reconcile(context.Background(), ReconcileRequest{
		Version: ProtocolVersion, PersonalityAgentID: testPAID,
	})
	if err != nil || inspection.ReapedThroughGeneration == nil || *inspection.ReapedThroughGeneration != 7 {
		t.Fatalf("reconcile cleanup receipt was not accepted: %#v %v", inspection, err)
	}
	delete(backend.reconcileReaps, testPAID)

	restarted, err := NewService(backend, ServiceConfig{StateDirectory: stateDirectory})
	if err != nil {
		t.Fatal(err)
	}
	inspection, err = restarted.Reconcile(context.Background(), ReconcileRequest{
		Version: ProtocolVersion, PersonalityAgentID: testPAID,
	})
	if err != nil || inspection.ReapedThroughGeneration == nil || *inspection.ReapedThroughGeneration != 7 {
		t.Fatalf("reconcile cleanup receipt did not survive restart: %#v %v", inspection, err)
	}
}

func TestDurableReapAttestationNeverLeaksAcrossPersonalityAgents(t *testing.T) {
	backend := newFakeBackend()
	stateDirectory := filepath.Join(t.TempDir(), "state")
	backend.reconcileReaps[testPAID] = 11
	first, err := NewService(backend, ServiceConfig{StateDirectory: stateDirectory})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Reconcile(context.Background(), ReconcileRequest{
		Version: ProtocolVersion, PersonalityAgentID: testPAID,
	}); err != nil {
		t.Fatal(err)
	}
	delete(backend.reconcileReaps, testPAID)

	restarted, err := NewService(backend, ServiceConfig{StateDirectory: stateDirectory})
	if err != nil {
		t.Fatal(err)
	}
	other, err := restarted.Reconcile(context.Background(), ReconcileRequest{
		Version: ProtocolVersion, PersonalityAgentID: testPAID2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if other.ReapedThroughGeneration != nil {
		t.Fatalf("PA %s received PA %s's reap attestation: %#v", testPAID2, testPAID, other)
	}
	original, err := restarted.Reconcile(context.Background(), ReconcileRequest{
		Version: ProtocolVersion, PersonalityAgentID: testPAID,
	})
	if err != nil || original.ReapedThroughGeneration == nil || *original.ReapedThroughGeneration != 11 {
		t.Fatalf("original PA lost its isolated reap attestation: %#v %v", original, err)
	}
}

func TestUnknownRuntimeWithoutDurableReapAttestationStaysUnattested(t *testing.T) {
	service := newTestService(t, newFakeBackend())
	inspection, err := service.Reconcile(context.Background(), ReconcileRequest{
		Version: ProtocolVersion, PersonalityAgentID: testPAID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if inspection.ReapedThroughGeneration != nil {
		t.Fatalf("unknown runtime fabricated a reap attestation: %#v", inspection)
	}
}
