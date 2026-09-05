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
		GatewayURL:                    "wss://gateway.invalid",
		LocalControlBearer:            "bearer",
		LocalControlServerUID:         65532,
		LocalControlSocketGID:         20000,
		AgentWrappingKey:              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		AgentWrappingKeyID:            "wrapping/v1",
		ApprovalSecretDigestKey:       "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		ProviderAPIKey:                "provider-key",
		ExecutionReviewerAPIKey:       "execution-reviewer-key",
		ExecutionReviewerModelPreset:  "kimi-k3",
		EscalationReviewerAPIKey:      "escalation-reviewer-key",
		EscalationReviewerModelPreset: "glm-5.2",
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
	// identitySurvivesReap reproduces the host shape that made every spawn after
	// the first fail. A verified teardown runs `compose down` without removing
	// the allocator's named volume, so the epoch identity written there outlives
	// the containers and the supervisor's next inspection classifies the empty
	// project as PhaseRecovery instead of PhaseUnknown.
	identitySurvivesReap bool
}

// reapedInspection is what the backend reports for a personality agent whose
// containers this backend has just removed.
func (backend *fakeBackend) reapedInspection(epoch PreparedEpoch) Inspection {
	if !backend.identitySurvivesReap {
		return unknownInspection(epoch.PersonalityAgentID)
	}
	retired := epoch
	return Inspection{
		PersonalityAgentID: epoch.PersonalityAgentID,
		Phase:              PhaseRecovery,
		Epoch:              &retired,
	}
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
	backend.state[epoch.PersonalityAgentID] = backend.reapedInspection(epoch)
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
	backend.state[epoch.PersonalityAgentID] = backend.reapedInspection(epoch)
	return inspection, nil
}

func (backend *fakeBackend) Reconcile(ctx context.Context, request ReconcileRequest) (Inspection, error) {
	personalityAgentID := request.PersonalityAgentID
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

func TestDurableReapStateCreatesSearchableSharedParentAndPrivateLeaf(t *testing.T) {
	sharedParent := filepath.Join(t.TempDir(), "runtime-provisioner")
	stateDirectory := filepath.Join(sharedParent, "state")
	if _, err := newDurableReapState(stateDirectory); err != nil {
		t.Fatal(err)
	}
	parentInfo, err := os.Stat(sharedParent)
	if err != nil {
		t.Fatal(err)
	}
	if parentInfo.Mode().Perm() != 0o755 {
		t.Fatalf("shared socket parent mode = %o, want 0755", parentInfo.Mode().Perm())
	}
	stateInfo, err := os.Stat(stateDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if stateInfo.Mode().Perm() != 0o700 {
		t.Fatalf("private reap state directory mode = %o, want 0700", stateInfo.Mode().Perm())
	}
}

func TestDurableReapStateAtMaximumSizeRoundTrips(t *testing.T) {
	stateDirectory := filepath.Join(t.TempDir(), "state")
	state, err := newDurableReapState(stateDirectory)
	if err != nil {
		t.Fatal(err)
	}
	fillReapStateToMaximumSize(t, state)
	if _, err := state.persist(); err != nil {
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
	if _, err := state.persist(); err != nil {
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

func TestDurableReapStateConfirmsPublishedEntryOnDirectorySyncRetry(t *testing.T) {
	stateDirectory := filepath.Join(t.TempDir(), "state")
	state, err := newDurableReapState(stateDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.record(testPAID, 3); err != nil {
		t.Fatalf("record initial reap attestation: %v", err)
	}

	syncFailure := errors.New("injected directory sync failure")
	syncAttempts := 0
	state.syncDirectory = func(*os.File) error {
		syncAttempts++
		if syncAttempts == 1 {
			return syncFailure
		}
		return nil
	}
	if err := state.record(testPAID2, 5); !errors.Is(err, syncFailure) {
		t.Fatalf("post-rename directory sync failure was not returned: %v", err)
	}
	if generation, ok := state.lookup(testPAID2); ok || generation != 0 {
		t.Fatalf("post-rename entry became durable in memory: generation=%d ok=%t", generation, ok)
	}

	restarted, err := newDurableReapState(stateDirectory)
	if err != nil {
		t.Fatalf("published document after directory sync failure no longer loads: %v", err)
	}
	if generation, ok := restarted.lookup(testPAID2); !ok || generation != 5 {
		t.Fatalf("post-rename entry was not present in the published document: generation=%d ok=%t", generation, ok)
	}

	if err := state.record(testPAID2, 5); err != nil {
		t.Fatalf("retry directory sync for published reap attestation: %v", err)
	}
	if syncAttempts != 2 {
		t.Fatalf("directory sync attempts = %d, want retry after failure", syncAttempts)
	}
	if generation, ok := state.lookup(testPAID2); !ok || generation != 5 {
		t.Fatalf("retried post-rename entry did not become durable: generation=%d ok=%t", generation, ok)
	}

	thirdPAID := "0198f0f4-9b72-7000-8000-000000000003"
	if err := state.record(thirdPAID, 7); err != nil {
		t.Fatalf("later reap attestation discarded published entry: %v", err)
	}
	final, err := newDurableReapState(stateDirectory)
	if err != nil {
		t.Fatalf("final reap state does not load: %v", err)
	}
	for personalityAgentID, generation := range map[string]uint64{
		testPAID:  3,
		testPAID2: 5,
		thirdPAID: 7,
	} {
		if actual, ok := final.lookup(personalityAgentID); !ok || actual != generation {
			t.Fatalf("final reap state lost %s: generation=%d ok=%t", personalityAgentID, actual, ok)
		}
	}
}

func fillReapStateToMaximumSize(t *testing.T, state *durableReapState) string {
	t.Helper()
	encodedSize := func() int {
		encoded, err := encodeReapStateDocument(reapStateDocumentForEntries(state.entries))
		if err != nil {
			t.Fatal(err)
		}
		return len(encoded)
	}
	// Re-encoding the whole document once per candidate entry made this fixture
	// quadratic in the ~12k entries a 1 MiB document holds. Every entry has the
	// same canonical PAID width, so measure one entry's cost, jump to a
	// deliberately low estimate, and converge from there.
	empty := encodedSize()
	state.entries[reapStateTestPersonalityAgentID(0)] = 0
	single := encodedSize()
	state.entries[reapStateTestPersonalityAgentID(1)] = 0
	perEntry := encodedSize() - single
	if perEntry <= 0 {
		t.Fatalf("reap state entry cost is not positive: %d", perEntry)
	}
	next := 2
	for index := next; index < (maxReapStateBytes-empty)/perEntry-2; index++ {
		state.entries[reapStateTestPersonalityAgentID(index)] = 0
		next = index + 1
	}
	for {
		personalityAgentID := reapStateTestPersonalityAgentID(next)
		state.entries[personalityAgentID] = 0
		if encodedSize() > maxReapStateBytes {
			delete(state.entries, personalityAgentID)
			break
		}
		next++
	}
	remaining := maxReapStateBytes - encodedSize()
	for index := 0; remaining > 0; index++ {
		extraDigits := min(remaining, 18)
		generation := uint64(1)
		for range extraDigits {
			generation *= 10
		}
		state.entries[reapStateTestPersonalityAgentID(index)] = generation
		remaining -= extraDigits
	}
	if size := encodedSize(); size != maxReapStateBytes {
		t.Fatalf("maximum reap state fixture has size %d, want %d", size, maxReapStateBytes)
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

// A verified stop leaves the allocator's named volume in place, so the host
// keeps answering with the retired epoch identity and the supervisor reports
// the empty project as PhaseRecovery. Before the durable receipt became the
// authority on "already cleaned up", that inspection made Prepare refuse every
// spawn after the first for the same personality agent.
func TestSecondSpawnSucceedsAfterAVerifiedStopLeavesTheAllocatorIdentityBehind(t *testing.T) {
	backend := newFakeBackend()
	backend.identitySurvivesReap = true
	service := newTestService(t, backend)

	first, err := service.Prepare(context.Background(), PrepareRequest{
		Version: ProtocolVersion, PersonalityAgentID: testPAID, IdempotencyKey: "spawn-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Activate(context.Background(), ActivateRequest{
		Version: ProtocolVersion, PreparedEpoch: first, Activation: testActivationConfig(),
	}); err != nil {
		t.Fatal(err)
	}
	stopped, err := service.Stop(context.Background(), StopRequest{
		Version: ProtocolVersion, PreparedEpoch: first,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stopped.ReapedThroughGeneration == nil || *stopped.ReapedThroughGeneration != first.Generation {
		t.Fatalf("stop did not return the exact reap receipt: %#v", stopped)
	}
	if inspection, err := service.Inspect(context.Background(), InspectRequest{
		Version: ProtocolVersion, PersonalityAgentID: testPAID,
	}); err != nil || inspection.Phase != PhaseRecovery {
		t.Fatalf("fixture does not reproduce the surviving identity: %#v %v", inspection, err)
	}

	second, err := service.Prepare(context.Background(), PrepareRequest{
		Version: ProtocolVersion, PersonalityAgentID: testPAID, IdempotencyKey: "spawn-2",
	})
	if err != nil {
		t.Fatalf("second spawn was refused after a verified stop: %v", err)
	}
	if second.Generation != first.Generation+1 || backend.prepareCalls[testPAID] != 2 {
		t.Fatalf("second spawn did not allocate exactly the next epoch: %#v calls=%d",
			second, backend.prepareCalls[testPAID])
	}

	activation := testActivationConfig()
	activation.ReapAttestation = &ReapAttestation{
		PersonalityAgentID:      testPAID,
		EpochGeneration:         second.Generation,
		RPCBootNonce:            second.RPCBootNonce,
		ReapedThroughGeneration: first.Generation,
	}
	inspection, err := service.Activate(context.Background(), ActivateRequest{
		Version: ProtocolVersion, PreparedEpoch: second, Activation: activation,
	})
	if err != nil || inspection.Phase != PhaseActive {
		t.Fatalf("observed reap receipt was refused on activation: %#v %v", inspection, err)
	}
}

// A provisioner that persists the teardown receipt and then restarts before its
// answer reaches the caller must let that caller retry. The allocator volume
// outlives the teardown, so the restarted daemon hydrates the personality agent
// as PhaseRecovery; the durable receipt, not that classification, decides.
func TestTeardownRetryAfterAProvisionerRestartIsIdempotent(t *testing.T) {
	for _, test := range []struct {
		name     string
		teardown func(*Service, PreparedEpoch) (Inspection, error)
		calls    func(*fakeBackend) int
	}{
		{
			name: "stop",
			teardown: func(service *Service, epoch PreparedEpoch) (Inspection, error) {
				return service.Stop(context.Background(), StopRequest{
					Version: ProtocolVersion, PreparedEpoch: epoch,
				})
			},
			calls: func(backend *fakeBackend) int { return backend.stopCalls[testPAID] },
		},
		{
			name: "abort",
			teardown: func(service *Service, epoch PreparedEpoch) (Inspection, error) {
				return service.Abort(context.Background(), AbortRequest{
					Version: ProtocolVersion, PreparedEpoch: epoch,
				})
			},
			calls: func(backend *fakeBackend) int { return backend.abortCalls[testPAID] },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := newFakeBackend()
			backend.identitySurvivesReap = true
			stateDirectory := filepath.Join(t.TempDir(), "state")
			first, err := NewService(backend, ServiceConfig{StateDirectory: stateDirectory})
			if err != nil {
				t.Fatal(err)
			}
			epoch, err := first.Prepare(context.Background(), PrepareRequest{
				Version: ProtocolVersion, PersonalityAgentID: testPAID, IdempotencyKey: "spawn-1",
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := first.Activate(context.Background(), ActivateRequest{
				Version: ProtocolVersion, PreparedEpoch: epoch, Activation: testActivationConfig(),
			}); err != nil {
				t.Fatal(err)
			}
			// The receipt reaches durable state; the answer never reaches the caller.
			if _, err := test.teardown(first, epoch); err != nil {
				t.Fatal(err)
			}
			completed := test.calls(backend)

			restarted, err := NewService(backend, ServiceConfig{StateDirectory: stateDirectory})
			if err != nil {
				t.Fatal(err)
			}
			if inspection, err := restarted.Inspect(context.Background(), InspectRequest{
				Version: ProtocolVersion, PersonalityAgentID: testPAID,
			}); err != nil || inspection.Phase != PhaseRecovery {
				t.Fatalf("fixture does not reproduce the surviving identity: %#v %v", inspection, err)
			}

			retried, err := test.teardown(restarted, epoch)
			if err != nil {
				t.Fatalf("%s retry after a provisioner restart was refused: %v", test.name, err)
			}
			if retried.Phase != PhaseUnknown {
				t.Fatalf("%s retry did not report the personality agent reaped: %#v", test.name, retried)
			}
			if retried.ReapedThroughGeneration == nil || *retried.ReapedThroughGeneration != epoch.Generation {
				t.Fatalf("%s retry lost the durable receipt: %#v", test.name, retried)
			}
			if test.calls(backend) != completed {
				t.Fatalf("%s retry re-ran host teardown: %d then %d", test.name, completed, test.calls(backend))
			}
			// The retry is also idempotent against the restarted daemon's own cache.
			if again, err := test.teardown(restarted, epoch); err != nil ||
				again.ReapedThroughGeneration == nil || *again.ReapedThroughGeneration != epoch.Generation {
				t.Fatalf("second %s retry diverged: %#v %v", test.name, again, err)
			}
		})
	}
}

// A recovery the receipt does not cover is a real one: the host still owns
// processes this daemon never observed leaving, and teardown must stay
// fail-closed until a fenced reconcile.
func TestTeardownStillRefusesAnUncoveredRecovery(t *testing.T) {
	backend := newFakeBackend()
	service := newTestService(t, backend)
	epoch, err := service.Prepare(context.Background(), PrepareRequest{
		Version: ProtocolVersion, PersonalityAgentID: testPAID, IdempotencyKey: "spawn-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Activate(context.Background(), ActivateRequest{
		Version: ProtocolVersion, PreparedEpoch: epoch, Activation: testActivationConfig(),
	}); err != nil {
		t.Fatal(err)
	}
	// The host wrecked the project without this daemon observing an empty one.
	wrecked := epoch
	backend.state[testPAID] = Inspection{
		PersonalityAgentID: testPAID, Phase: PhaseRecovery, Epoch: &wrecked,
	}
	restarted, err := NewService(backend, ServiceConfig{StateDirectory: filepath.Join(t.TempDir(), "state")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Stop(context.Background(), StopRequest{
		Version: ProtocolVersion, PreparedEpoch: epoch,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("stop accepted an unattested recovery: %v", err)
	}
	if _, err := restarted.Abort(context.Background(), AbortRequest{
		Version: ProtocolVersion, PreparedEpoch: epoch,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("abort accepted an unattested recovery: %v", err)
	}
	if backend.stopCalls[testPAID] != 0 || backend.abortCalls[testPAID] != 0 {
		t.Fatal("unattested recovery reached host teardown")
	}
}

// The runtime consumes ReapedThroughGeneration as a host observation. The
// provisioner holds the physical record, so it must recompute the caller's
// claim against that record instead of forwarding whatever the API declares.
func TestActivateRejectsAReapAttestationNoDurableReceiptCovers(t *testing.T) {
	prepareAt := func(t *testing.T, generation uint64) (*Service, *fakeBackend, PreparedEpoch) {
		t.Helper()
		backend := newFakeBackend()
		backend.identitySurvivesReap = true
		service := newTestService(t, backend)
		backend.nextGeneration[testPAID] = generation
		epoch, err := service.Prepare(context.Background(), PrepareRequest{
			Version: ProtocolVersion, PersonalityAgentID: testPAID, IdempotencyKey: "forged",
		})
		if err != nil {
			t.Fatal(err)
		}
		return service, backend, epoch
	}

	t.Run("no durable receipt at all", func(t *testing.T) {
		service, backend, epoch := prepareAt(t, 6)
		activation := testActivationConfig()
		activation.ReapAttestation = &ReapAttestation{
			PersonalityAgentID:      testPAID,
			EpochGeneration:         epoch.Generation,
			RPCBootNonce:            epoch.RPCBootNonce,
			ReapedThroughGeneration: 5,
		}
		_, err := service.Activate(context.Background(), ActivateRequest{
			Version: ProtocolVersion, PreparedEpoch: epoch, Activation: activation,
		})
		if !errors.Is(err, ErrConflict) {
			t.Fatalf("unbacked reap attestation was accepted: %v", err)
		}
		if backend.activateCalls[testPAID] != 0 {
			t.Fatal("unbacked reap attestation reached the backend")
		}
	})

	t.Run("durable receipt behind the claim", func(t *testing.T) {
		backend := newFakeBackend()
		backend.identitySurvivesReap = true
		service := newTestService(t, backend)
		backend.nextGeneration[testPAID] = 3
		retired, err := service.Prepare(context.Background(), PrepareRequest{
			Version: ProtocolVersion, PersonalityAgentID: testPAID, IdempotencyKey: "spawn-3",
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := service.Abort(context.Background(), AbortRequest{
			Version: ProtocolVersion, PreparedEpoch: retired,
		}); err != nil {
			t.Fatal(err)
		}
		// Generations 4 and 5 were allocated on the host without this daemon
		// observing their teardown, so its durable receipt stops at 3.
		backend.nextGeneration[testPAID] = 6
		epoch, err := service.Prepare(context.Background(), PrepareRequest{
			Version: ProtocolVersion, PersonalityAgentID: testPAID, IdempotencyKey: "spawn-6",
		})
		if err != nil {
			t.Fatal(err)
		}
		activation := testActivationConfig()
		activation.ReapAttestation = &ReapAttestation{
			PersonalityAgentID:      testPAID,
			EpochGeneration:         epoch.Generation,
			RPCBootNonce:            epoch.RPCBootNonce,
			ReapedThroughGeneration: 5,
		}
		activate := ActivateRequest{
			Version: ProtocolVersion, PreparedEpoch: epoch, Activation: activation,
		}
		if _, err := service.Activate(context.Background(), activate); !errors.Is(err, ErrConflict) {
			t.Fatalf("over-claimed reap attestation was accepted: %v", err)
		}
		if backend.activateCalls[testPAID] != 0 {
			t.Fatal("over-claimed reap attestation reached the backend")
		}

		activate.Activation.ReapAttestation.ReapedThroughGeneration = retired.Generation
		inspection, err := service.Activate(context.Background(), activate)
		if err != nil || inspection.Phase != PhaseActive {
			t.Fatalf("observed reap receipt was refused on activation: %#v %v", inspection, err)
		}
	})
}
