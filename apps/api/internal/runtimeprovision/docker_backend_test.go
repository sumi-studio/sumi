package runtimeprovision

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
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
	if err := backend.Stop(context.Background(), epoch); err != nil {
		t.Fatal(err)
	}
	if len(runner.actions) != 4 || runner.actions[0] != "prepare" || runner.actions[1] != "inspect-epoch" || runner.actions[2] != "activate" || runner.actions[3] != "stop-epoch" {
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
	stopEnvironment := strings.Join(runner.envs[3], "\n")
	for _, expected := range []string{"SUMI_EXPECTED_RPC_GENERATION=7", "SUMI_EXPECTED_RPC_NONCE=boot-7"} {
		if !strings.Contains(stopEnvironment, expected) {
			t.Fatalf("exact stop environment omitted %s: %s", expected, stopEnvironment)
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

func TestExecCommandRunnerCancelsThroughSupervisorTermTrap(t *testing.T) {
	dir := t.TempDir()
	readyPath := filepath.Join(dir, "ready")
	termPath := filepath.Join(dir, "term")
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := (execCommandRunner{terminationGrace: time.Second, pipeWait: 100 * time.Millisecond}).Run(
			ctx,
			"/bin/sh",
			[]string{"-c", `trap 'printf term >"$TERM_PATH"; exit 143' TERM; printf ready >"$READY_PATH"; while :; do sleep 1; done`},
			[]string{"PATH=/usr/bin:/bin", "READY_PATH=" + readyPath, "TERM_PATH=" + termPath},
		)
		result <- err
	}()
	waitForFile(t, readyPath)
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runner error = %v, want context cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not return after graceful cancellation")
	}
	if raw, err := os.ReadFile(termPath); err != nil || string(raw) != "term" {
		t.Fatalf("supervisor TERM trap did not run: value=%q err=%v", raw, err)
	}
}

func TestExecCommandRunnerBoundsInheritedPipeAndKillsProcessGroup(t *testing.T) {
	dir := t.TempDir()
	readyPath := filepath.Join(dir, "ready")
	pidPath := filepath.Join(dir, "pid")
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := (execCommandRunner{terminationGrace: 100 * time.Millisecond, pipeWait: 25 * time.Millisecond}).Run(
			ctx,
			"/bin/sh",
			[]string{"-c", `trap 'exit 143' TERM; (trap '' TERM; while :; do printf held >&2; sleep 1; done) & printf '%s' "$$" >"$PID_PATH"; printf ready >"$READY_PATH"; while :; do sleep 1; done`},
			[]string{"PATH=/usr/bin:/bin", "READY_PATH=" + readyPath, "PID_PATH=" + pidPath},
		)
		result <- err
	}()
	waitForFile(t, readyPath)
	rawPID, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(rawPID))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runner error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runner hung on inherited stderr after cancellation")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(-pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("supervisor process group survived bounded cancellation")
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}
