package runtimeprovision

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

type recordingRunner struct {
	mu      sync.Mutex
	actions []string
	envs    [][]string
	outputs map[string]string
	failed  map[string]bool
}

func (runner *recordingRunner) Run(_ context.Context, _ string, args, environment []string) ([]byte, error) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	action := args[0]
	runner.actions = append(runner.actions, action)
	runner.envs = append(runner.envs, append([]string(nil), environment...))
	if runner.failed[action] {
		return nil, errors.New("secret backend detail")
	}
	return []byte(runner.outputs[action]), nil
}

func TestDockerBackendUsesExplicitPhasesAndCoherentHandle(t *testing.T) {
	runner := &recordingRunner{outputs: map[string]string{
		"prepare":       `{"personality_agent_id":"` + testPAID + `","phase":"prepared","generation":7,"rpc_boot_nonce":"boot-7"}`,
		"inspect-epoch": `{"personality_agent_id":"` + testPAID + `","phase":"prepared","generation":7,"rpc_boot_nonce":"boot-7"}`,
		"reconcile":     `{"personality_agent_id":"` + testPAID + `","phase":"active","generation":7,"rpc_boot_nonce":"boot-7"}`,
	}}
	backend := &DockerBackend{supervisor: "/fake/supervisor", baseEnvironment: []string{"PATH=/usr/bin"}, runner: runner}
	epoch, err := backend.Prepare(context.Background(), PrepareRequest{PersonalityAgentID: testPAID})
	if err != nil {
		t.Fatal(err)
	}
	if epoch.Generation != 7 || epoch.RPCBootNonce != "boot-7" || !strings.HasPrefix(epoch.OpaquePreparedHandle, "docker-v1-") {
		t.Fatalf("unexpected prepared epoch: %#v", epoch)
	}
	inspection, err := backend.Inspect(context.Background(), testPAID)
	if err != nil || inspection.Epoch == nil || inspection.Epoch.OpaquePreparedHandle != epoch.OpaquePreparedHandle {
		t.Fatalf("inspect did not reconstruct the coherent handle: %#v %v", inspection, err)
	}
	activate := ActivateRequest{
		Version:       ProtocolVersion,
		PreparedEpoch: epoch,
		Activation:    testActivationConfig(),
	}
	if err := backend.Activate(context.Background(), activate); err != nil {
		t.Fatal(err)
	}
	if len(runner.actions) != 3 || runner.actions[0] != "prepare" || runner.actions[1] != "inspect-epoch" || runner.actions[2] != "activate" {
		t.Fatalf("unexpected supervisor phase calls: %#v", runner.actions)
	}
	joinedEnvironment := strings.Join(runner.envs[2], "\n")
	for _, expected := range []string{
		"SUMI_PERSONALITY_AGENT_ID=" + testPAID,
		"SUMI_EXPECTED_RPC_GENERATION=7",
		"SUMI_EXPECTED_RPC_NONCE=boot-7",
		"SUMI_GATEWAY_URL=wss://gateway.invalid",
	} {
		if !strings.Contains(joinedEnvironment, expected) {
			t.Fatalf("activation environment omitted %s: %s", expected, joinedEnvironment)
		}
	}
}

func TestDockerBackendRejectsAuthorityEnvironmentOverridesAndRedactsFailure(t *testing.T) {
	runner := &recordingRunner{outputs: map[string]string{}, failed: map[string]bool{"activate": true}}
	backend := &DockerBackend{supervisor: "/fake/supervisor", runner: runner}
	epoch := PreparedEpoch{PersonalityAgentID: testPAID, Generation: 1, RPCBootNonce: "nonce", OpaquePreparedHandle: "handle"}
	if _, err := mergeEnvironment(nil, map[string]string{"LD_PRELOAD": "/tmp/host-injection.so"}, nil, testPAID); err == nil {
		t.Fatal("non-allowlisted host environment override was accepted")
	}
	request := ActivateRequest{PreparedEpoch: epoch, Activation: testActivationConfig()}
	err := backend.Activate(context.Background(), request)
	if err == nil || strings.Contains(err.Error(), "secret backend detail") {
		t.Fatalf("backend failure was not redacted: %v", err)
	}
}

func TestParseSupervisorInspectionRejectsTrailingOutput(t *testing.T) {
	_, err := parseSupervisorInspection([]byte(`{"personality_agent_id":"`+testPAID+`","phase":"unknown"} trailing`), testPAID)
	if err == nil {
		t.Fatal("trailing supervisor output was accepted")
	}
}
