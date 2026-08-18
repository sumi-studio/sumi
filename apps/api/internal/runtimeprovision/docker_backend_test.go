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
		"stop-epoch":    `{"personality_agent_id":"` + testPAID + `","phase":"unknown","reaped_through_generation":7}`,
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
	activation := testActivationConfig()
	activation.ReapAttestation = &ReapAttestation{
		PersonalityAgentID:      testPAID,
		EpochGeneration:         7,
		RPCBootNonce:            "boot-7",
		ReapedThroughGeneration: 6,
	}
	activate := ActivateRequest{
		Version:       ProtocolVersion,
		PreparedEpoch: epoch,
		Activation:    activation,
	}
	if err := backend.Activate(context.Background(), activate); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Stop(context.Background(), epoch); err != nil {
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
		"SUMI_EXECUTION_REVIEWER_API_KEY=execution-reviewer-key",
		"SUMI_EXECUTION_REVIEWER_MODEL_PRESET=kimi-k3",
		"SUMI_EXECUTION_REVIEWER_MODEL_API_KEY_ENV=SUMI_EXECUTION_REVIEWER_API_KEY",
		"SUMI_ESCALATION_REVIEWER_API_KEY=escalation-reviewer-key",
		"SUMI_ESCALATION_REVIEWER_MODEL_PRESET=glm-5.2",
		"SUMI_ESCALATION_REVIEWER_MODEL_API_KEY_ENV=SUMI_ESCALATION_REVIEWER_API_KEY",
		"SUMI_REAP_ATTESTATION_PERSONALITY_AGENT_ID=" + testPAID,
		"SUMI_REAP_ATTESTATION_EPOCH_GENERATION=7",
		"SUMI_REAP_ATTESTATION_RPC_BOOT_NONCE=boot-7",
		"SUMI_REAPED_THROUGH_GENERATION=6",
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

func TestDockerBackendRejectsTeardownWithoutExactObservedEmptyReceipt(t *testing.T) {
	epoch := PreparedEpoch{PersonalityAgentID: testPAID, Generation: 7, RPCBootNonce: "boot-7", OpaquePreparedHandle: "handle"}
	for name, output := range map[string]string{
		"missing": `{"personality_agent_id":"` + testPAID + `","phase":"unknown"}`,
		"lower":   `{"personality_agent_id":"` + testPAID + `","phase":"unknown","reaped_through_generation":6}`,
	} {
		t.Run(name, func(t *testing.T) {
			runner := &recordingRunner{outputs: map[string]string{"stop-epoch": output}}
			backend := &DockerBackend{supervisor: "/fake/supervisor", runner: runner}
			if _, err := backend.Stop(context.Background(), epoch); err == nil || !strings.Contains(err.Error(), "exact observed-empty") {
				t.Fatalf("invalid teardown output accepted: %v", err)
			}
		})
	}
}

func TestDockerBackendReconcileReturnsObservedEmptyReceipt(t *testing.T) {
	runner := &recordingRunner{outputs: map[string]string{
		"reconcile": `{"personality_agent_id":"` + testPAID + `","phase":"unknown","reaped_through_generation":7}`,
	}}
	backend := &DockerBackend{supervisor: "/fake/supervisor", runner: runner}
	inspection, err := backend.Reconcile(context.Background(), testPAID)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.ReapedThroughGeneration == nil || *inspection.ReapedThroughGeneration != 7 {
		t.Fatalf("reconcile lost the supervisor's observed-empty receipt: %#v", inspection)
	}
}

func TestDockerBackendPassesPinnedAgentImageTagToSupervisor(t *testing.T) {
	const tag = "a1b2c3d4e5f6"
	runner := &recordingRunner{outputs: map[string]string{
		"prepare": `{"personality_agent_id":"` + testPAID + `","phase":"prepared","generation":7,"rpc_boot_nonce":"boot-7"}`,
	}}
	backend := &DockerBackend{
		supervisor:      "/fake/supervisor",
		baseEnvironment: []string{"SUMI_AGENT_IMAGE_TAG=" + tag},
		runner:          runner,
	}
	if _, err := backend.Prepare(context.Background(), PrepareRequest{PersonalityAgentID: testPAID}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(runner.envs[0], "\n"); !strings.Contains(got, "SUMI_AGENT_IMAGE_TAG="+tag) {
		t.Fatalf("supervisor environment = %q, want pinned agent image tag", got)
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

func TestSanitizeSupervisorErrorRedactsReapAttestationNonce(t *testing.T) {
	const nonce = "reap-attestation-rpc-boot-nonce"
	diagnostic := sanitizeSupervisorError(
		"compose activation failed with SUMI_REAP_ATTESTATION_RPC_BOOT_NONCE="+nonce,
		[]string{"SUMI_REAP_ATTESTATION_RPC_BOOT_NONCE=" + nonce},
	)
	if strings.Contains(diagnostic, nonce) {
		t.Fatalf("reap attestation nonce leaked in diagnostic: %s", diagnostic)
	}
	if !strings.Contains(diagnostic, "<redacted:SUMI_REAP_ATTESTATION_RPC_BOOT_NONCE>") {
		t.Fatalf("reap attestation nonce was not marked redacted: %s", diagnostic)
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

func TestExecCommandRunnerHonorsCleanupBoundForDetachedSession(t *testing.T) {
	dir := t.TempDir()
	readyPath := filepath.Join(dir, "ready")
	nestedReadyPath := filepath.Join(dir, "nested-ready")
	cleanedPath := filepath.Join(dir, "cleaned")
	pidPath := filepath.Join(dir, "nested-pid")
	scriptPath := filepath.Join(dir, "supervisor.sh")
	script := `#!/bin/bash
set -eu
printf 'cleanup-bound-ms 600\n' >&3
trap 'sleep 0.2; kill -TERM -- "-${nested}"; wait "${nested}" || true; printf "nested-done %s\n" "${nested}" >&3; printf cleaned >"${CLEANED_PATH}"; exit 143' TERM
setsid /bin/bash -c 'trap "exit 0" TERM; printf ready >"${NESTED_READY_PATH}"; while :; do sleep 1; done' 3>&- &
nested=$!
while [[ ! -f "${NESTED_READY_PATH}" ]]; do
  kill -0 "${nested}" 2>/dev/null || { wait "${nested}"; exit $?; }
  sleep 0.005
done
printf 'nested-start %s\n' "${nested}" >&3
printf '%s' "${nested}" >"${PID_PATH}"
printf ready >"${READY_PATH}"
while :; do sleep 1; done
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := (execCommandRunner{pipeWait: 25 * time.Millisecond}).Run(
			ctx,
			scriptPath,
			nil,
			[]string{
				"PATH=/usr/bin:/bin",
				"READY_PATH=" + readyPath,
				"NESTED_READY_PATH=" + nestedReadyPath,
				"CLEANED_PATH=" + cleanedPath,
				"PID_PATH=" + pidPath,
			},
		)
		result <- err
	}()
	waitForFile(t, readyPath)
	nestedPID := readPID(t, pidPath)
	started := time.Now()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runner error = %v, want context cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not return within the advertised cleanup bound")
	}
	if elapsed := time.Since(started); elapsed < 150*time.Millisecond {
		t.Fatalf("runner returned before slow cleanup completed: %s", elapsed)
	}
	if raw, err := os.ReadFile(cleanedPath); err != nil || string(raw) != "cleaned" {
		t.Fatalf("slow cleanup did not complete: value=%q err=%v", raw, err)
	}
	waitForProcessGroupGone(t, nestedPID, "detached cleanup session")
}

func TestExecCommandRunnerKillsTrackedDetachedSessionAfterCleanupBound(t *testing.T) {
	dir := t.TempDir()
	readyPath := filepath.Join(dir, "ready")
	nestedReadyPath := filepath.Join(dir, "nested-ready")
	pidPath := filepath.Join(dir, "nested-pid")
	scriptPath := filepath.Join(dir, "stuck-supervisor.sh")
	script := `#!/bin/bash
set -eu
printf 'cleanup-bound-ms 100\n' >&3
trap '' TERM
setsid /bin/bash -c 'trap "" TERM; printf ready >"${NESTED_READY_PATH}"; while :; do sleep 1; done' 3>&- &
nested=$!
while [[ ! -f "${NESTED_READY_PATH}" ]]; do
  kill -0 "${nested}" 2>/dev/null || { wait "${nested}"; exit $?; }
  sleep 0.005
done
printf 'nested-start %s\n' "${nested}" >&3
printf '%s' "${nested}" >"${PID_PATH}"
printf ready >"${READY_PATH}"
while :; do sleep 1; done
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := (execCommandRunner{pipeWait: 25 * time.Millisecond}).Run(
			ctx,
			scriptPath,
			nil,
			[]string{
				"PATH=/usr/bin:/bin",
				"READY_PATH=" + readyPath,
				"NESTED_READY_PATH=" + nestedReadyPath,
				"PID_PATH=" + pidPath,
			},
		)
		result <- err
	}()
	waitForFile(t, readyPath)
	nestedPID := readPID(t, pidPath)
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runner error = %v, want context cancellation", err)
		}
		var commandError *supervisorCommandError
		if !errors.As(err, &commandError) || !strings.Contains(commandError.diagnostic, "host lifecycle state is indeterminate and requires reconciliation") {
			t.Fatalf("runner omitted indeterminate-state reconciliation diagnostic: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner exceeded the advertised cleanup bound")
	}
	waitForProcessGroupGone(t, nestedPID, "stuck detached session")
}

func readPID(t *testing.T, path string) int {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

func waitForProcessGroupGone(t *testing.T, pid int, label string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(-pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s survived cancellation", label)
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
