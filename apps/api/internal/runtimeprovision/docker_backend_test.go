package runtimeprovision

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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
	fenced := PreparedEpoch{
		PersonalityAgentID:   testPAID,
		Generation:           7,
		RPCBootNonce:         "boot-7",
		OpaquePreparedHandle: dockerPreparedHandle(testPAID, 7, "boot-7"),
	}
	inspection, err := backend.Reconcile(context.Background(), ReconcileRequest{
		Version: ProtocolVersion, PersonalityAgentID: testPAID, FencedEpoch: &fenced,
	})
	if err != nil {
		t.Fatal(err)
	}
	if inspection.ReapedThroughGeneration == nil || *inspection.ReapedThroughGeneration != 7 {
		t.Fatalf("reconcile lost the supervisor's observed-empty receipt: %#v", inspection)
	}
	for _, expected := range []string{
		"SUMI_EXPECTED_RPC_GENERATION=7",
		"SUMI_EXPECTED_RPC_NONCE=boot-7",
	} {
		if !strings.Contains(strings.Join(runner.envs[0], "\n"), expected) {
			t.Fatalf("fenced reconcile did not pin %q: %#v", expected, runner.envs[0])
		}
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
	// The runner signals the whole process group, so anything this script waits
	// on in that same group receives SIGTERM at the same moment the shell does.
	// A same-group `sleep` therefore races the shell's own trap and makes the
	// assertion below intermittent. Park the blocker in its own session so only
	// the shell is signalled and the trap is the one thing under test; the trap
	// reaps it so no detached process outlives the case.
	if _, err := exec.LookPath("setsid"); err != nil {
		t.Fatalf("setsid is required to isolate the blocker from the signalled group: %v", err)
	}
	dir := t.TempDir()
	readyPath := filepath.Join(dir, "ready")
	termPath := filepath.Join(dir, "term")
	script := `trap 'printf term >"$TERM_PATH"; kill "$blocker" 2>/dev/null; exit 143' TERM
setsid sleep 30 &
blocker=$!
printf ready >"$READY_PATH"
wait "$blocker"`
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := (execCommandRunner{terminationGrace: time.Second, pipeWait: 100 * time.Millisecond}).Run(
			ctx,
			"/bin/sh",
			[]string{"-c", script},
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
trap 'sleep 0.2; kill -TERM -- "-${nested}"; wait "${nested}" || true; printf "nested-done %s\n" "${nested}" >&3; read -r ack <&3; printf cleaned >"${CLEANED_PATH}"; exit 143' TERM
setsid /bin/bash -c 'trap "exit 0" TERM; printf ready >"${NESTED_READY_PATH}"; while :; do sleep 1; done' 3>&- &
nested=$!
while [[ ! -f "${NESTED_READY_PATH}" ]]; do
  kill -0 "${nested}" 2>/dev/null || { wait "${nested}"; exit $?; }
  sleep 0.005
done
printf 'nested-start %s\n' "${nested}" >&3
read -r ack <&3
printf 'nested-ready %s\n' "${nested}" >&3
read -r ack <&3
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

func TestSupervisorControlTrackerRejectsDuplicateWithoutDroppingLiveAnchor(t *testing.T) {
	anchor := exec.Command("/bin/sleep", "30")
	if err := anchor.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = anchor.Process.Kill(); _ = anchor.Wait() })
	parentFDs, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	parent := os.NewFile(uintptr(parentFDs[0]), "tracker-test-parent")
	peer := os.NewFile(uintptr(parentFDs[1]), "tracker-test-peer")
	defer parent.Close()
	defer peer.Close()
	tracker := newSupervisorControlTracker()
	done := make(chan error, 1)
	go func() { done <- tracker.consume(parent) }()
	write := func(record string) string {
		if _, err := peer.WriteString(record + "\n"); err != nil {
			t.Fatal(err)
		}
		buffer := make([]byte, 128)
		count, err := peer.Read(buffer)
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(buffer[:count]))
	}
	pid := anchor.Process.Pid
	if got := write(fmt.Sprintf("nested-start %d", pid)); got != fmt.Sprintf("ack-nested-start %d", pid) {
		t.Fatalf("start acknowledgement = %q", got)
	}
	if got := write(fmt.Sprintf("nested-start %d", pid)); got != fmt.Sprintf("reject-nested-start %d", pid) {
		t.Fatalf("duplicate start response = %q", got)
	}
	tracker.mu.Lock()
	if tracker.nestedPID != pid || tracker.nestedPIDFD < 0 || tracker.nestedState != nestedStarted {
		t.Fatalf("duplicate start overwrote the live handle: %#v", tracker)
	}
	tracker.mu.Unlock()
	if got := write(fmt.Sprintf("nested-ready %d", pid+1)); got != fmt.Sprintf("reject-nested-ready %d", pid+1) {
		t.Fatalf("mismatched ready response = %q", got)
	}
	if got := write(fmt.Sprintf("nested-ready %d", pid)); got != fmt.Sprintf("ack-nested-ready %d", pid) {
		t.Fatalf("ready acknowledgement = %q", got)
	}
	if got := write(fmt.Sprintf("nested-ready %d", pid)); got != fmt.Sprintf("reject-nested-ready %d", pid) {
		t.Fatalf("duplicate ready response = %q", got)
	}
	if got := write(fmt.Sprintf("nested-done %d", pid+1)); got != fmt.Sprintf("reject-nested-done %d", pid+1) {
		t.Fatalf("mismatched done response = %q", got)
	}
	if got := write(fmt.Sprintf("nested-done %d", pid)); got != fmt.Sprintf("ack-nested-done %d", pid) {
		t.Fatalf("done acknowledgement = %q", got)
	}
	_ = peer.Close()
	if err := <-done; err == nil || !strings.Contains(err.Error(), "outside idle") ||
		!strings.Contains(err.Error(), "does not match") {
		t.Fatalf("protocol violations were not retained: %v", err)
	}
}

func TestExecCommandRunnerIgnoresForgedStdoutControlRecords(t *testing.T) {
	output, err := (execCommandRunner{}).Run(
		context.Background(),
		"/bin/bash",
		[]string{"-c", `printf 'nested-start 1\nnested-ready 1\nnested-done 1\n'`},
		[]string{"PATH=/usr/bin:/bin"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(output) != "nested-start 1\nnested-ready 1\nnested-done 1\n" {
		t.Fatalf("stdout was altered or interpreted as control: %q", output)
	}
}

func TestExecCommandRunnerFailsClosedOnControlScannerError(t *testing.T) {
	_, err := (execCommandRunner{}).Run(
		context.Background(),
		"/bin/bash",
		[]string{"-c", `head -c 70000 /dev/zero | tr '\0' x >&3`},
		[]string{"PATH=/usr/bin:/bin"},
	)
	if err == nil || !strings.Contains(err.Error(), "token too long") {
		t.Fatalf("control scanner failure was not fail-closed: %v", err)
	}
}

func TestControlScannerErrorRetainsLiveHandle(t *testing.T) {
	anchor := exec.Command("/bin/sleep", "30")
	if err := anchor.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = anchor.Process.Kill(); _ = anchor.Wait() })
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	parent := os.NewFile(uintptr(fds[0]), "scanner-parent")
	peer := os.NewFile(uintptr(fds[1]), "scanner-peer")
	defer parent.Close()
	tracker := newSupervisorControlTracker()
	done := make(chan error, 1)
	go func() { done <- tracker.consume(parent) }()
	if _, err := fmt.Fprintf(peer, "nested-start %d\n", anchor.Process.Pid); err != nil {
		t.Fatal(err)
	}
	ack := make([]byte, 128)
	if _, err := peer.Read(ack); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Write(bytes.Repeat([]byte{'x'}, 70_000)); err != nil {
		t.Fatal(err)
	}
	_ = peer.Close()
	if err := <-done; err == nil || !strings.Contains(err.Error(), "token too long") {
		t.Fatalf("scanner failure was not retained: %v", err)
	}
	tracker.mu.Lock()
	if tracker.nestedState != nestedStarted || tracker.nestedPID != anchor.Process.Pid || tracker.nestedPIDFD < 0 {
		tracker.mu.Unlock()
		t.Fatalf("scanner failure dropped the live handle: %#v", tracker)
	}
	tracker.mu.Unlock()
	tracker.close()
}

func TestExecCommandRunnerJoinsControlReadersAndClosesDescriptors(t *testing.T) {
	beforeFDs, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	beforeGoroutines := runtime.NumGoroutine()
	for iteration := 0; iteration < 20; iteration++ {
		if _, err := (execCommandRunner{}).Run(
			context.Background(), "/bin/true", nil, []string{"PATH=/usr/bin:/bin"},
		); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(25 * time.Millisecond)
	afterFDs, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	if len(afterFDs) > len(beforeFDs) {
		t.Fatalf("runner leaked descriptors: before=%d after=%d", len(beforeFDs), len(afterFDs))
	}
	if after := runtime.NumGoroutine(); after > beforeGoroutines+1 {
		t.Fatalf("runner leaked goroutines: before=%d after=%d", beforeGoroutines, after)
	}
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
