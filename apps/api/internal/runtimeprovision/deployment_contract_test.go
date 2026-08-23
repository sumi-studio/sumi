package runtimeprovision

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var (
	testComposeAnchorOnce sync.Once
	testComposeAnchorDir  string
	testComposeAnchorPath string
	testComposeAnchorErr  error
)

func composeAnchorBinary(t *testing.T) string {
	t.Helper()
	testComposeAnchorOnce.Do(func() {
		directory, err := os.MkdirTemp("", "sumi-compose-anchor-test-")
		if err != nil {
			testComposeAnchorErr = err
			return
		}
		testComposeAnchorDir = directory
		testComposeAnchorPath = filepath.Join(directory, "sumi-compose-anchor")
		command := exec.Command("go", "build", "-buildvcs=false", "-o", testComposeAnchorPath, "./cmd/compose-anchor")
		command.Dir = filepath.Dir(repositoryFilePath("apps", "api", "go.mod"))
		output, err := command.CombinedOutput()
		if err != nil {
			testComposeAnchorErr = fmt.Errorf("build Compose anchor: %w: %s", err, output)
		}
	})
	if testComposeAnchorErr != nil {
		t.Fatal(testComposeAnchorErr)
	}
	return testComposeAnchorPath
}

func TestMain(testingMain *testing.M) {
	status := testingMain.Run()
	if testComposeAnchorDir != "" {
		_ = os.RemoveAll(testComposeAnchorDir)
	}
	os.Exit(status)
}

func TestSupervisorRecoveryInspectionDecodesAndPinsFencedReconcile(t *testing.T) {
	if _, err := exec.LookPath("unshare"); err != nil {
		t.Skip("unshare is required to isolate the supervisor trust roots")
	}
	if output, err := exec.Command("unshare", "-Urnm", "/bin/true").CombinedOutput(); err != nil {
		t.Skipf("user and mount namespaces are unavailable: %v: %s", err, output)
	}

	testRoot := t.TempDir()
	fakeDocker := filepath.Join(testRoot, "docker")
	fakeStat := filepath.Join(testRoot, "stat")
	fakeDockerScript := `#!/bin/sh
set -eu
case "$*" in
  "compose version")
    ;;
  "ps --all --filter label=com.docker.compose.project="*)
    # Only runtime remains; executor and broker are missing. This is the
    # partial-project shape that must flow to recovery, not a hard fail.
    printf '0123456789ab\truntime\trunning\n'
    ;;
  *"compose.prepare.yaml run --rm --no-deps --pull never --entrypoint /bin/bash allocator"*)
    printf 'SUMI_PERSONALITY_AGENT_ID=%s\nSUMI_RPC_GENERATION=7\nSUMI_RPC_NONCE=recovery-nonce\n' "$SUMI_PERSONALITY_AGENT_ID"
    ;;
  *)
    exit 91
    ;;
esac
`
	if err := os.WriteFile(fakeDocker, []byte(fakeDockerScript), 0o755); err != nil {
		t.Fatal(err)
	}
	fakeStatScript := `#!/bin/sh
if [ "$#" -eq 4 ] && [ "$1" = "-c" ] && [ "$3" = "--" ] && [ "$4" = "/" ]; then
  case "$2" in
    %u) printf '0\n'; exit 0 ;;
    %a) printf '755\n'; exit 0 ;;
  esac
fi
exec /usr/bin/stat "$@"
`
	if err := os.WriteFile(fakeStat, []byte(fakeStatScript), 0o755); err != nil {
		t.Fatal(err)
	}

	supervisor, err := filepath.Abs(repositoryFilePath("deploy", "agent", "supervisor"))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(
		"unshare", "-Urnm", "/bin/bash", "-eu", "-c",
		`mount -t tmpfs -o mode=0755 tmpfs /run; exec "$1" inspect-epoch`,
		"--", supervisor,
	)
	command.Env = []string{
		"PATH=" + testRoot + ":/usr/bin:/bin",
		"SUMI_CONFIG_FILE=/dev/null",
		"SUMI_PERSONALITY_AGENT_ID=" + testPAID,
	}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("real supervisor recovery inspection failed: %v\n%s", err, output)
	}

	runner := &recordingRunner{outputs: map[string]string{
		"inspect-epoch": string(output),
		"reconcile":     `{"personality_agent_id":"` + testPAID + `","phase":"unknown","reaped_through_generation":7}`,
	}}
	backend := &DockerBackend{supervisor: "/fake/supervisor", runner: runner}
	inspection, err := backend.Inspect(context.Background(), testPAID)
	if err != nil {
		t.Fatalf("decode real supervisor recovery response: %v\n%s", err, output)
	}
	if inspection.Phase != PhaseRecovery || inspection.Epoch == nil ||
		inspection.Epoch.Generation != 7 || inspection.Epoch.RPCBootNonce != "recovery-nonce" {
		t.Fatalf("decoded recovery inspection = %#v", inspection)
	}
	if err := inspection.Validate(); err != nil {
		t.Fatalf("decoded recovery inspection did not validate: %v", err)
	}
	if _, err := backend.Reconcile(context.Background(), ReconcileRequest{
		Version: ProtocolVersion, PersonalityAgentID: testPAID, FencedEpoch: inspection.Epoch,
	}); err != nil {
		t.Fatalf("fenced reconcile after decoded recovery inspection: %v", err)
	}
	if len(runner.actions) != 2 || runner.actions[0] != "inspect-epoch" || runner.actions[1] != "reconcile" {
		t.Fatalf("backend actions = %#v, want inspect then fenced reconcile", runner.actions)
	}
	for _, expected := range []string{"SUMI_EXPECTED_RPC_GENERATION=7", "SUMI_EXPECTED_RPC_NONCE=recovery-nonce"} {
		if !strings.Contains(strings.Join(runner.envs[1], "\n"), expected) {
			t.Fatalf("fenced reconcile omitted %q: %#v", expected, runner.envs[1])
		}
	}
}

func TestSupervisorInspectEpochReturnsActiveWithoutPullingAllocatorImage(t *testing.T) {
	if _, err := exec.LookPath("unshare"); err != nil {
		t.Skip("unshare is required to isolate the supervisor trust roots")
	}
	if output, err := exec.Command("unshare", "-Urnm", "/bin/true").CombinedOutput(); err != nil {
		t.Skipf("user and mount namespaces are unavailable: %v: %s", err, output)
	}

	testRoot := t.TempDir()
	fakeDocker := filepath.Join(testRoot, "docker")
	fakeStat := filepath.Join(testRoot, "stat")
	dockerLog := filepath.Join(testRoot, "docker.log")
	fakeDockerScript := `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$SUMI_FAKE_DOCKER_LOG"
case "$*" in
  "compose version")
    ;;
  "ps --all --filter label=com.docker.compose.project="*)
    printf 'aaaaaaaaaaaa\truntime\trunning\n'
    printf 'bbbbbbbbbbbb\texecutor\trunning\n'
    printf 'cccccccccccc\tbroker\trunning\n'
    ;;
  *"compose.prepare.yaml run --rm --no-deps --pull never --entrypoint /bin/bash allocator"*)
    printf 'SUMI_PERSONALITY_AGENT_ID=%s\nSUMI_RPC_GENERATION=7\nSUMI_RPC_NONCE=active-nonce\n' "$SUMI_PERSONALITY_AGENT_ID"
    ;;
  *)
    exit 91
    ;;
esac
`
	if err := os.WriteFile(fakeDocker, []byte(fakeDockerScript), 0o755); err != nil {
		t.Fatal(err)
	}
	fakeStatScript := `#!/bin/sh
if [ "$#" -eq 4 ] && [ "$1" = "-c" ] && [ "$3" = "--" ] && [ "$4" = "/" ]; then
  case "$2" in
    %u) printf '0\n'; exit 0 ;;
    %a) printf '755\n'; exit 0 ;;
  esac
fi
exec /usr/bin/stat "$@"
`
	if err := os.WriteFile(fakeStat, []byte(fakeStatScript), 0o755); err != nil {
		t.Fatal(err)
	}

	supervisor, err := filepath.Abs(repositoryFilePath("deploy", "agent", "supervisor"))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(
		"unshare", "-Urnm", "/bin/bash", "-eu", "-c",
		`mount -t tmpfs -o mode=0755 tmpfs /run; exec "$1" inspect-epoch`,
		"--", supervisor,
	)
	command.Env = []string{
		"PATH=" + testRoot + ":/usr/bin:/bin",
		"SUMI_CONFIG_FILE=/dev/null",
		"SUMI_FAKE_DOCKER_LOG=" + dockerLog,
		"SUMI_PERSONALITY_AGENT_ID=" + testPAID,
	}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("real supervisor active inspection failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), `"phase":"active","generation":7,"rpc_boot_nonce":"active-nonce"`) {
		t.Fatalf("active inspection did not confirm the running epoch: %s", output)
	}
	calls, err := os.ReadFile(dockerLog)
	if err != nil {
		t.Fatal(err)
	}
	// --pull never is the registry-unreachable resilience: epoch identity must
	// not attempt a pull that a DNS/registry gap would turn into a false death.
	if !strings.Contains(string(calls), "compose.prepare.yaml run --rm --no-deps --pull never --entrypoint /bin/bash allocator") {
		t.Fatalf("epoch identity did not use --pull never:\n%s", calls)
	}
}

func TestSupervisorPrepareDoesNotRequireActivationEnvironment(t *testing.T) {
	if _, err := exec.LookPath("unshare"); err != nil {
		t.Skip("unshare is required to isolate the supervisor trust roots")
	}
	if output, err := exec.Command("unshare", "-Urnm", "/bin/true").CombinedOutput(); err != nil {
		t.Skipf("user and mount namespaces are unavailable: %v: %s", err, output)
	}

	testRoot := t.TempDir()
	fakeDocker := filepath.Join(testRoot, "docker")
	fakeStat := filepath.Join(testRoot, "stat")
	dockerLog := filepath.Join(testRoot, "docker.log")
	fakeDockerScript := `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$SUMI_FAKE_DOCKER_LOG"
case "$*" in
  *"compose.prepare.yaml run --rm --no-deps --pull never --entrypoint /bin/bash allocator"*)
    printf 'SUMI_PERSONALITY_AGENT_ID=%s\nSUMI_RPC_GENERATION=7\nSUMI_RPC_NONCE=prepare-phase-nonce\n' "$SUMI_PERSONALITY_AGENT_ID"
    ;;
esac
`
	if err := os.WriteFile(fakeDocker, []byte(fakeDockerScript), 0o755); err != nil {
		t.Fatal(err)
	}
	// The host root uid is unmapped inside a rootless user namespace. Preserve
	// the production assertion for every mutable trust path and report only the
	// immutable namespace root as uid 0 to the supervisor.
	fakeStatScript := `#!/bin/sh
if [ "$#" -eq 4 ] && [ "$1" = "-c" ] && [ "$3" = "--" ] && [ "$4" = "/" ]; then
  case "$2" in
    %u) printf '0\n'; exit 0 ;;
    %a) printf '755\n'; exit 0 ;;
  esac
fi
exec /usr/bin/stat "$@"
`
	if err := os.WriteFile(fakeStat, []byte(fakeStatScript), 0o755); err != nil {
		t.Fatal(err)
	}

	supervisor, err := filepath.Abs(repositoryFilePath("deploy", "agent", "supervisor"))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(
		"unshare", "-Urnm", "/bin/bash", "-eu", "-c",
		`mount -t tmpfs -o mode=0755 tmpfs /run; exec "$1" prepare`,
		"--", supervisor,
	)
	command.Env = []string{
		"PATH=" + testRoot + ":/usr/bin:/bin",
		"SUMI_CONFIG_FILE=/dev/null",
		"SUMI_COMPOSE_ANCHOR=" + composeAnchorBinary(t),
		"SUMI_DEV_ALLOW_APPARMOR_UNCONFINED=true",
		"SUMI_FAKE_DOCKER_LOG=" + dockerLog,
		"SUMI_PERSONALITY_AGENT_ID=" + testPAID,
	}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("real supervisor prepare required activation-only environment: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), `"phase":"prepared"`) {
		t.Fatalf("real supervisor prepare did not return a prepared epoch: %s", output)
	}
	calls, err := os.ReadFile(dockerLog)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"compose version", "compose.lifecycle.yaml down", "compose.prepare.yaml up", "compose.prepare.yaml run"} {
		if !strings.Contains(string(calls), required) {
			t.Fatalf("real supervisor prepare omitted %q:\n%s", required, calls)
		}
	}
}

func TestSupervisorReconcileAttestsPartialProjectOnlyAfterObservedEmpty(t *testing.T) {
	if _, err := exec.LookPath("unshare"); err != nil {
		t.Skip("unshare is required to isolate the supervisor trust roots")
	}
	if output, err := exec.Command("unshare", "-Urnm", "/bin/true").CombinedOutput(); err != nil {
		t.Skipf("user and mount namespaces are unavailable: %v: %s", err, output)
	}

	testRoot := t.TempDir()
	fakeDocker := filepath.Join(testRoot, "docker")
	fakeStat := filepath.Join(testRoot, "stat")
	dockerLog := filepath.Join(testRoot, "docker.log")
	dockerState := filepath.Join(testRoot, "project-down")
	fakeDockerScript := `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$SUMI_FAKE_DOCKER_LOG"
case "$*" in
  "ps --all --filter label=com.docker.compose.project="*)
    [ -e "$SUMI_FAKE_DOCKER_STATE" ] || printf '0123456789ab\truntime\texited\n'
    ;;
  *"compose.lifecycle.yaml down"*)
    : > "$SUMI_FAKE_DOCKER_STATE"
    exit 17
    ;;
  *"compose.prepare.yaml run --rm --no-deps --pull never --entrypoint /bin/bash allocator"*)
    printf 'SUMI_PERSONALITY_AGENT_ID=%s\nSUMI_RPC_GENERATION=7\nSUMI_RPC_NONCE=reconciled-nonce\n' "$SUMI_PERSONALITY_AGENT_ID"
    ;;
esac
`
	if err := os.WriteFile(fakeDocker, []byte(fakeDockerScript), 0o755); err != nil {
		t.Fatal(err)
	}
	fakeStatScript := `#!/bin/sh
if [ "$#" -eq 4 ] && [ "$1" = "-c" ] && [ "$3" = "--" ] && [ "$4" = "/" ]; then
  case "$2" in
    %u) printf '0\n'; exit 0 ;;
    %a) printf '755\n'; exit 0 ;;
  esac
fi
exec /usr/bin/stat "$@"
`
	if err := os.WriteFile(fakeStat, []byte(fakeStatScript), 0o755); err != nil {
		t.Fatal(err)
	}

	supervisor, err := filepath.Abs(repositoryFilePath("deploy", "agent", "supervisor"))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(
		"unshare", "-Urnm", "/bin/bash", "-eu", "-c",
		`mount -t tmpfs -o mode=0755 tmpfs /run; exec "$1" reconcile`,
		"--", supervisor,
	)
	command.Env = []string{
		"PATH=" + testRoot + ":/usr/bin:/bin",
		"SUMI_CONFIG_FILE=/dev/null",
		"SUMI_FAKE_DOCKER_LOG=" + dockerLog,
		"SUMI_FAKE_DOCKER_STATE=" + dockerState,
		"SUMI_PERSONALITY_AGENT_ID=" + testPAID,
		"SUMI_EXPECTED_RPC_GENERATION=7",
		"SUMI_EXPECTED_RPC_NONCE=reconciled-nonce",
	}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("real supervisor reconcile failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), `"phase":"unknown","reaped_through_generation":7`) {
		t.Fatalf("reconcile cleanup omitted the observed-empty receipt: %s", output)
	}
	calls, err := os.ReadFile(dockerLog)
	if err != nil {
		t.Fatal(err)
	}
	down := strings.Index(string(calls), "compose.lifecycle.yaml down")
	emptyObservation := strings.LastIndex(string(calls), "ps --all --filter label=com.docker.compose.project=")
	durableGeneration := strings.LastIndex(string(calls), "compose.prepare.yaml run")
	if down < 0 || emptyObservation <= down || durableGeneration <= emptyObservation {
		t.Fatalf("reconcile did not observe empty before deriving its durable generation:\n%s", calls)
	}
}

func TestSupervisorReconcileDoesNotAttestWhenPartialDownLeavesRenamedContainer(t *testing.T) {
	if _, err := exec.LookPath("unshare"); err != nil {
		t.Skip("unshare is required to isolate the supervisor trust roots")
	}
	if output, err := exec.Command("unshare", "-Urnm", "/bin/true").CombinedOutput(); err != nil {
		t.Skipf("user and mount namespaces are unavailable: %v: %s", err, output)
	}

	testRoot := t.TempDir()
	fakeDocker := filepath.Join(testRoot, "docker")
	fakeStat := filepath.Join(testRoot, "stat")
	dockerLog := filepath.Join(testRoot, "docker.log")
	fakeDockerScript := `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$SUMI_FAKE_DOCKER_LOG"
case "$*" in
  "ps --all --filter label=com.docker.compose.project="*"--format {{.ID}}")
    printf '0123456789ab\n'
    ;;
  "ps --all --filter label=com.docker.compose.project="*)
    # The old service name is not a long-lived role in the current manifest,
    # but it still belongs to this Compose project and remains running after
    # the partially failed down.
    printf '0123456789ab\tretired-executor\trunning\n'
    ;;
  *"compose.lifecycle.yaml down"*)
    exit 17
    ;;
  *"compose.prepare.yaml run --rm --no-deps --pull never --entrypoint /bin/bash allocator"*)
    printf 'SUMI_PERSONALITY_AGENT_ID=%s\nSUMI_RPC_GENERATION=7\nSUMI_RPC_NONCE=renamed-remnant-nonce\n' "$SUMI_PERSONALITY_AGENT_ID"
    ;;
esac
`
	if err := os.WriteFile(fakeDocker, []byte(fakeDockerScript), 0o755); err != nil {
		t.Fatal(err)
	}
	fakeStatScript := `#!/bin/sh
if [ "$#" -eq 4 ] && [ "$1" = "-c" ] && [ "$3" = "--" ] && [ "$4" = "/" ]; then
  case "$2" in
    %u) printf '0\n'; exit 0 ;;
    %a) printf '755\n'; exit 0 ;;
  esac
fi
exec /usr/bin/stat "$@"
`
	if err := os.WriteFile(fakeStat, []byte(fakeStatScript), 0o755); err != nil {
		t.Fatal(err)
	}

	supervisor, err := filepath.Abs(repositoryFilePath("deploy", "agent", "supervisor"))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(
		"unshare", "-Urnm", "/bin/bash", "-eu", "-c",
		`mount -t tmpfs -o mode=0755 tmpfs /run; exec "$1" reconcile`,
		"--", supervisor,
	)
	command.Env = []string{
		"PATH=" + testRoot + ":/usr/bin:/bin",
		"SUMI_CONFIG_FILE=/dev/null",
		"SUMI_FAKE_DOCKER_LOG=" + dockerLog,
		"SUMI_PERSONALITY_AGENT_ID=" + testPAID,
		"SUMI_EXPECTED_RPC_GENERATION=7",
		"SUMI_EXPECTED_RPC_NONCE=renamed-remnant-nonce",
	}
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("reconcile attested a project with a renamed container remnant: %s", output)
	}
	if strings.Contains(string(output), `"reaped_through_generation"`) {
		t.Fatalf("reconcile emitted a reap receipt despite the renamed remnant: %s", output)
	}
	calls, err := os.ReadFile(dockerLog)
	if err != nil {
		t.Fatal(err)
	}
	down := strings.Index(string(calls), "compose.lifecycle.yaml down")
	projectObservation := strings.LastIndex(string(calls), "--format {{.ID}}")
	if down < 0 || projectObservation <= down {
		t.Fatalf("reconcile did not verify every project container after partial down:\n%s", calls)
	}
}

func TestSupervisorReconcileReattestsAlreadyEmptyProjectAfterProvisionerCrash(t *testing.T) {
	if _, err := exec.LookPath("unshare"); err != nil {
		t.Skip("unshare is required to isolate the supervisor trust roots")
	}
	if output, err := exec.Command("unshare", "-Urnm", "/bin/true").CombinedOutput(); err != nil {
		t.Skipf("user and mount namespaces are unavailable: %v: %s", err, output)
	}

	testRoot := t.TempDir()
	fakeDocker := filepath.Join(testRoot, "docker")
	fakeStat := filepath.Join(testRoot, "stat")
	dockerLog := filepath.Join(testRoot, "docker.log")
	fakeDockerScript := `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$SUMI_FAKE_DOCKER_LOG"
case "$*" in
  *"compose.prepare.yaml run --rm --no-deps --pull never --entrypoint /bin/bash allocator"*)
    printf 'SUMI_PERSONALITY_AGENT_ID=%s\nSUMI_RPC_GENERATION=7\nSUMI_RPC_NONCE=recovered-nonce\n' "$SUMI_PERSONALITY_AGENT_ID"
    ;;
esac
`
	if err := os.WriteFile(fakeDocker, []byte(fakeDockerScript), 0o755); err != nil {
		t.Fatal(err)
	}
	fakeStatScript := `#!/bin/sh
if [ "$#" -eq 4 ] && [ "$1" = "-c" ] && [ "$3" = "--" ] && [ "$4" = "/" ]; then
  case "$2" in
    %u) printf '0\n'; exit 0 ;;
    %a) printf '755\n'; exit 0 ;;
  esac
fi
exec /usr/bin/stat "$@"
`
	if err := os.WriteFile(fakeStat, []byte(fakeStatScript), 0o755); err != nil {
		t.Fatal(err)
	}

	supervisor, err := filepath.Abs(repositoryFilePath("deploy", "agent", "supervisor"))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(
		"unshare", "-Urnm", "/bin/bash", "-eu", "-c",
		`mount -t tmpfs -o mode=0755 tmpfs /run; exec "$1" reconcile`,
		"--", supervisor,
	)
	command.Env = []string{
		"PATH=" + testRoot + ":/usr/bin:/bin",
		"SUMI_CONFIG_FILE=/dev/null",
		"SUMI_FAKE_DOCKER_LOG=" + dockerLog,
		"SUMI_PERSONALITY_AGENT_ID=" + testPAID,
		"SUMI_EXPECTED_RPC_GENERATION=7",
		"SUMI_EXPECTED_RPC_NONCE=recovered-nonce",
	}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("reconcile after provisioner crash failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), `"phase":"unknown","reaped_through_generation":7`) {
		t.Fatalf("reconcile after provisioner crash did not re-attest observed-empty project: %s", output)
	}
	calls, err := os.ReadFile(dockerLog)
	if err != nil {
		t.Fatal(err)
	}
	down := strings.Index(string(calls), "compose.lifecycle.yaml down")
	emptyObservation := strings.LastIndex(string(calls), "ps --all --filter label=com.docker.compose.project=")
	durableGeneration := strings.LastIndex(string(calls), "compose.prepare.yaml run")
	if down < 0 || emptyObservation <= down || durableGeneration <= emptyObservation {
		t.Fatalf("reconcile did not verify emptiness before re-attesting the durable generation:\n%s", calls)
	}
}

func TestSupervisorReconcileRemovesAllocatorOnlyProjectBeforeReattesting(t *testing.T) {
	if _, err := exec.LookPath("unshare"); err != nil {
		t.Skip("unshare is required to isolate the supervisor trust roots")
	}
	if output, err := exec.Command("unshare", "-Urnm", "/bin/true").CombinedOutput(); err != nil {
		t.Skipf("user and mount namespaces are unavailable: %v: %s", err, output)
	}

	testRoot := t.TempDir()
	fakeDocker := filepath.Join(testRoot, "docker")
	fakeStat := filepath.Join(testRoot, "stat")
	dockerLog := filepath.Join(testRoot, "docker.log")
	dockerState := filepath.Join(testRoot, "project-down")
	fakeDockerScript := `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$SUMI_FAKE_DOCKER_LOG"
case "$*" in
  "ps --all --filter label=com.docker.compose.project="*)
    [ -e "$SUMI_FAKE_DOCKER_STATE" ] || printf '0123456789ab\tallocator\texited\n'
    ;;
  *"compose.lifecycle.yaml ps --all --quiet runtime"*|*"compose.lifecycle.yaml ps --all --quiet executor"*|*"compose.lifecycle.yaml ps --all --quiet broker"*|*"compose.lifecycle.yaml ps --all --quiet prepare"*)
    ;;
  *"compose.lifecycle.yaml ps --all --quiet allocator"*)
    [ -e "$SUMI_FAKE_DOCKER_STATE" ] || printf '0123456789ab\n'
    ;;
  *"compose.lifecycle.yaml ps --all --quiet"*)
    [ -e "$SUMI_FAKE_DOCKER_STATE" ] || printf '0123456789ab\n'
    ;;
  *"compose.lifecycle.yaml down"*)
    : > "$SUMI_FAKE_DOCKER_STATE"
    exit 17
    ;;
  *"compose.prepare.yaml run --rm --no-deps --pull never --entrypoint /bin/bash allocator"*)
    printf 'SUMI_PERSONALITY_AGENT_ID=%s\nSUMI_RPC_GENERATION=7\nSUMI_RPC_NONCE=allocator-only-nonce\n' "$SUMI_PERSONALITY_AGENT_ID"
    ;;
esac
`
	if err := os.WriteFile(fakeDocker, []byte(fakeDockerScript), 0o755); err != nil {
		t.Fatal(err)
	}
	fakeStatScript := `#!/bin/sh
if [ "$#" -eq 4 ] && [ "$1" = "-c" ] && [ "$3" = "--" ] && [ "$4" = "/" ]; then
  case "$2" in
    %u) printf '0\n'; exit 0 ;;
    %a) printf '755\n'; exit 0 ;;
  esac
fi
exec /usr/bin/stat "$@"
`
	if err := os.WriteFile(fakeStat, []byte(fakeStatScript), 0o755); err != nil {
		t.Fatal(err)
	}

	supervisor, err := filepath.Abs(repositoryFilePath("deploy", "agent", "supervisor"))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(
		"unshare", "-Urnm", "/bin/bash", "-eu", "-c",
		`mount -t tmpfs -o mode=0755 tmpfs /run; exec "$1" reconcile`,
		"--", supervisor,
	)
	command.Env = []string{
		"PATH=" + testRoot + ":/usr/bin:/bin",
		"SUMI_CONFIG_FILE=/dev/null",
		"SUMI_FAKE_DOCKER_LOG=" + dockerLog,
		"SUMI_FAKE_DOCKER_STATE=" + dockerState,
		"SUMI_PERSONALITY_AGENT_ID=" + testPAID,
		"SUMI_EXPECTED_RPC_GENERATION=7",
		"SUMI_EXPECTED_RPC_NONCE=allocator-only-nonce",
	}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("reconcile allocator-only project failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), `"phase":"unknown","reaped_through_generation":7`) {
		t.Fatalf("allocator-only reconcile omitted the observed-empty receipt: %s", output)
	}
	calls, err := os.ReadFile(dockerLog)
	if err != nil {
		t.Fatal(err)
	}
	down := strings.Index(string(calls), "compose.lifecycle.yaml down")
	emptyObservation := strings.LastIndex(string(calls), "ps --all --filter label=com.docker.compose.project=")
	durableGeneration := strings.LastIndex(string(calls), "compose.prepare.yaml run")
	if down < 0 || emptyObservation <= down || durableGeneration <= emptyObservation {
		t.Fatalf("allocator-only reconcile did not tear down and observe empty before re-attesting:\n%s", calls)
	}
}

// A completed one-shot or another project-labelled one-off does not change the
// active epoch: only runtime, executor, and broker define live epoch state.
//
// The Rust fixture for this path binds the host's real /run, so it cannot run
// on a machine with a live control plane. This drives the same supervisor
// branch inside a private mount namespace over a tmpfs /run instead.
func TestSupervisorReconcileKeepsActiveEpochWithOneShotBesideLongLivedRoles(t *testing.T) {
	if _, err := exec.LookPath("unshare"); err != nil {
		t.Skip("unshare is required to isolate the supervisor trust roots")
	}
	if output, err := exec.Command("unshare", "-Urnm", "/bin/true").CombinedOutput(); err != nil {
		t.Skipf("user and mount namespaces are unavailable: %v: %s", err, output)
	}

	fakeDockerScript := `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$SUMI_FAKE_DOCKER_LOG"
case "$*" in
  "compose version")
    ;;
  "ps --all --filter label=com.docker.compose.project="*)
    printf 'aaaaaaaaaaaa\truntime\trunning\n'
    printf 'bbbbbbbbbbbb\texecutor\trunning\n'
    printf 'cccccccccccc\tbroker\trunning\n'
    printf 'dddddddddddd\torphan-one-off\texited\n'
    ;;
  *"compose.prepare.yaml run --rm --no-deps --pull never --entrypoint /bin/bash allocator"*)
    printf 'SUMI_PERSONALITY_AGENT_ID=%s\nSUMI_RPC_GENERATION=7\nSUMI_RPC_NONCE=orphan-nonce\n' "$SUMI_PERSONALITY_AGENT_ID"
    ;;
  *)
    exit 91
    ;;
esac
`
	fakeStatScript := `#!/bin/sh
if [ "$#" -eq 4 ] && [ "$1" = "-c" ] && [ "$3" = "--" ] && [ "$4" = "/" ]; then
  case "$2" in
    %u) printf '0\n'; exit 0 ;;
    %a) printf '755\n'; exit 0 ;;
  esac
fi
exec /usr/bin/stat "$@"
`
	reconcile := func(t *testing.T) ([]byte, string, error) {
		t.Helper()
		testRoot := t.TempDir()
		dockerLog := filepath.Join(testRoot, "docker.log")
		if err := os.WriteFile(filepath.Join(testRoot, "docker"), []byte(fakeDockerScript), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(testRoot, "stat"), []byte(fakeStatScript), 0o755); err != nil {
			t.Fatal(err)
		}
		supervisor, err := filepath.Abs(repositoryFilePath("deploy", "agent", "supervisor"))
		if err != nil {
			t.Fatal(err)
		}
		command := exec.Command(
			"unshare", "-Urnm", "/bin/bash", "-eu", "-c",
			`mount -t tmpfs -o mode=0755 tmpfs /run; exec "$1" reconcile`,
			"--", supervisor,
		)
		command.Env = []string{
			"PATH=" + testRoot + ":/usr/bin:/bin",
			"SUMI_CONFIG_FILE=/dev/null",
			"SUMI_FAKE_DOCKER_LOG=" + dockerLog,
			"SUMI_FAKE_REAPED=" + filepath.Join(testRoot, "reaped"),
			"SUMI_PERSONALITY_AGENT_ID=" + testPAID,
		}
		output, err := command.CombinedOutput()
		calls, readErr := os.ReadFile(dockerLog)
		if readErr != nil && !os.IsNotExist(readErr) {
			t.Fatal(readErr)
		}
		return output, string(calls), err
	}

	t.Run("reconcile preserves the active epoch", func(t *testing.T) {
		output, calls, err := reconcile(t)
		if err != nil {
			t.Fatalf("real supervisor reconcile failed: %v\n%s", err, output)
		}
		if !strings.Contains(string(output), `"phase":"active","generation":7,"rpc_boot_nonce":"orphan-nonce"`) {
			t.Fatalf("reconcile did not preserve the active epoch: %s", output)
		}
		if strings.Contains(calls, "compose.lifecycle.yaml down --remove-orphans") {
			t.Fatalf("active epoch unexpectedly entered destructive reconciliation:\n%s", calls)
		}
	})
}

func TestSupervisorReconcileKeepsFullyRunningProjectActive(t *testing.T) {
	if _, err := exec.LookPath("unshare"); err != nil {
		t.Skip("unshare is required to isolate the supervisor trust roots")
	}
	if output, err := exec.Command("unshare", "-Urnm", "/bin/true").CombinedOutput(); err != nil {
		t.Skipf("user and mount namespaces are unavailable: %v: %s", err, output)
	}

	testRoot := t.TempDir()
	fakeDocker := filepath.Join(testRoot, "docker")
	fakeStat := filepath.Join(testRoot, "stat")
	dockerLog := filepath.Join(testRoot, "docker.log")
	fakeDockerScript := `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$SUMI_FAKE_DOCKER_LOG"
case "$*" in
  "compose version")
    ;;
  *"ps --all --filter label=com.docker.compose.project="*)
    printf 'aaaaaaaaaaaa\truntime\trunning\n'
    printf 'bbbbbbbbbbbb\texecutor\trunning\n'
    printf 'cccccccccccc\tbroker\trunning\n'
    ;;
  *"compose.prepare.yaml run --rm --no-deps --pull never --entrypoint /bin/bash allocator"*)
    printf 'SUMI_PERSONALITY_AGENT_ID=%s\nSUMI_RPC_GENERATION=7\nSUMI_RPC_NONCE=active-nonce\n' "$SUMI_PERSONALITY_AGENT_ID"
    ;;
  *)
    exit 91
    ;;
esac
`
	if err := os.WriteFile(fakeDocker, []byte(fakeDockerScript), 0o755); err != nil {
		t.Fatal(err)
	}
	fakeStatScript := `#!/bin/sh
if [ "$#" -eq 4 ] && [ "$1" = "-c" ] && [ "$3" = "--" ] && [ "$4" = "/" ]; then
  case "$2" in
    %u) printf '0\n'; exit 0 ;;
    %a) printf '755\n'; exit 0 ;;
  esac
fi
exec /usr/bin/stat "$@"
`
	if err := os.WriteFile(fakeStat, []byte(fakeStatScript), 0o755); err != nil {
		t.Fatal(err)
	}

	supervisor, err := filepath.Abs(repositoryFilePath("deploy", "agent", "supervisor"))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(
		"unshare", "-Urnm", "/bin/bash", "-eu", "-c",
		`mount -t tmpfs -o mode=0755 tmpfs /run; exec "$1" reconcile`,
		"--", supervisor,
	)
	command.Env = []string{
		"PATH=" + testRoot + ":/usr/bin:/bin",
		"SUMI_CONFIG_FILE=/dev/null",
		"SUMI_FAKE_DOCKER_LOG=" + dockerLog,
		"SUMI_PERSONALITY_AGENT_ID=" + testPAID,
	}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("reconcile fully running project failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), `"phase":"active","generation":7,"rpc_boot_nonce":"active-nonce"`) {
		t.Fatalf("fully running reconcile did not preserve active attestation: %s", output)
	}
	calls, err := os.ReadFile(dockerLog)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(calls), "compose.lifecycle.yaml down") {
		t.Fatalf("fully running project unexpectedly ran teardown:\n%s", calls)
	}
	if !strings.Contains(string(calls), "ps --all --filter label=com.docker.compose.project=") {
		t.Fatalf("fully running reconcile did not enumerate all project containers:\n%s", calls)
	}
}

func TestSupervisorReconcileAttestationContractPreservesUnknownWithoutDurableEpoch(t *testing.T) {
	supervisor := readDeploymentFile(t, "supervisor")
	reconcileStart := strings.Index(supervisor, "  reconcile)")
	if reconcileStart < 0 {
		t.Fatal("supervisor has no reconcile action")
	}
	reconcile := supervisor[reconcileStart:]
	for _, required := range []string{
		"project_is_empty || fail",
		"if epoch_identity; then",
		`print_reaped_json "${reaped_generation}"`,
		"print_unknown_json",
	} {
		if !strings.Contains(reconcile, required) {
			t.Fatalf("reconcile cleanup omits %q:\n%s", required, reconcile)
		}
	}
	if strings.Index(reconcile, "require_expected_epoch") > strings.Index(reconcile, "lifecycle_compose down") ||
		strings.Index(reconcile, "project_is_empty") > strings.LastIndex(reconcile, "if epoch_identity; then") ||
		strings.LastIndex(reconcile, "epoch_identity") > strings.Index(reconcile, "print_reaped_json") {
		t.Fatalf("reconcile can attest before observed-empty verification and durable generation recovery:\n%s", reconcile)
	}
}

// reapAttestationVariables is the exact wire the runtime consumes as its boot
// reap receipt. Every hop from the provisioner to `sumi-agent` has to carry all
// four or the runtime silently loses the receipt and refuses to reuse durable
// state, which is the symptom this branch exists to remove.
var reapAttestationVariables = []string{
	"SUMI_REAP_ATTESTATION_PERSONALITY_AGENT_ID",
	"SUMI_REAP_ATTESTATION_EPOCH_GENERATION",
	"SUMI_REAP_ATTESTATION_RPC_BOOT_NONCE",
	"SUMI_REAPED_THROUGH_GENERATION",
}

func TestReapAttestationCrossesEveryHopFromProvisionerToRuntime(t *testing.T) {
	attestation := ReapAttestation{
		PersonalityAgentID:      testPAID,
		EpochGeneration:         9,
		RPCBootNonce:            "boot-9",
		ReapedThroughGeneration: 8,
	}
	expected := map[string]string{
		"SUMI_REAP_ATTESTATION_PERSONALITY_AGENT_ID": testPAID,
		"SUMI_REAP_ATTESTATION_EPOCH_GENERATION":     "9",
		"SUMI_REAP_ATTESTATION_RPC_BOOT_NONCE":       "boot-9",
		"SUMI_REAPED_THROUGH_GENERATION":             "8",
	}

	t.Run("provisioner to supervisor", func(t *testing.T) {
		config := testActivationConfig()
		config.ReapAttestation = &attestation
		values := activationEnvironment(config)
		environment, err := mergeEnvironment(nil, values, nil, testPAID)
		if err != nil {
			t.Fatalf("attestation environment was rejected by the supervisor allowlist: %v", err)
		}
		joined := strings.Join(environment, "\n")
		for _, name := range reapAttestationVariables {
			if values[name] != expected[name] {
				t.Fatalf("activation environment set %s=%q, want %q", name, values[name], expected[name])
			}
			if !strings.Contains(joined, name+"="+expected[name]) {
				t.Fatalf("supervisor environment omitted %s:\n%s", name, joined)
			}
		}
	})

	t.Run("supervisor to compose", func(t *testing.T) {
		runtimeEnvironment := composeServiceEnvironmentBlock(t, readDeploymentFile(t, "compose.yaml"), "runtime")
		for _, name := range reapAttestationVariables {
			// Optional pass-through. A required (`:?`) mapping would refuse to
			// start the very first generation, which legitimately has no receipt.
			mapping := "      " + name + ": ${" + name + ":-}"
			if !strings.Contains(runtimeEnvironment, mapping+"\n") {
				t.Fatalf("runtime service does not pass %s through Compose:\n%s", name, runtimeEnvironment)
			}
		}
	})

	t.Run("compose to entrypoint", func(t *testing.T) {
		harness := filepath.Join(t.TempDir(), "forwarding-harness")
		if err := os.WriteFile(harness, []byte(entrypointReapForwardingHarness(t)), 0o700); err != nil {
			t.Fatal(err)
		}
		run := func(t *testing.T, environment []string) []string {
			t.Helper()
			command := exec.Command("/bin/bash", harness)
			command.Env = append([]string{"PATH=/usr/bin:/bin"}, environment...)
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("entrypoint forwarding harness failed: %v\n%s", err, output)
			}
			return strings.Split(strings.TrimSuffix(string(output), "\n"), "\n")
		}

		supplied := make([]string, 0, len(reapAttestationVariables))
		for _, name := range reapAttestationVariables {
			supplied = append(supplied, name+"="+expected[name])
		}
		forwarded := run(t, supplied)
		for _, name := range reapAttestationVariables {
			if !slices.Contains(forwarded, name+"="+expected[name]) {
				t.Fatalf("entrypoint dropped %s before exec: %#v", name, forwarded)
			}
		}

		// Compose maps every one of them to the empty string on a first-ever
		// generation. The entrypoint must omit them rather than hand the runtime
		// an empty attestation field.
		empty := make([]string, 0, len(reapAttestationVariables))
		for _, name := range reapAttestationVariables {
			empty = append(empty, name+"=")
		}
		for _, environment := range [][]string{empty, nil} {
			for _, forwarded := range run(t, environment) {
				if forwarded != "" {
					t.Fatalf("entrypoint forwarded an unattested value %q", forwarded)
				}
			}
		}
	})
}

// composeServiceEnvironmentBlock returns the exact `environment:` block of one
// Compose service without depending on a YAML parser in this module.
func composeServiceEnvironmentBlock(t *testing.T, compose, service string) string {
	t.Helper()
	serviceStart := strings.Index(compose, "\n  "+service+":\n")
	if serviceStart < 0 {
		t.Fatalf("compose file has no %s service", service)
	}
	block := compose[serviceStart+1:]
	environmentStart := strings.Index(block, "\n    environment:\n")
	if environmentStart < 0 {
		t.Fatalf("%s service has no environment block", service)
	}
	block = block[environmentStart+len("\n    environment:\n"):]
	var environment strings.Builder
	for _, line := range strings.Split(block, "\n") {
		if line != "" && !strings.HasPrefix(line, "      ") {
			break
		}
		environment.WriteString(line)
		environment.WriteString("\n")
	}
	return environment.String()
}

// entrypointReapForwardingHarness lifts the real forwarding loop out of
// container-entrypoint so the test executes the shipped text rather than a
// paraphrase of it.
func entrypointReapForwardingHarness(t *testing.T) string {
	t.Helper()
	entrypoint := readDeploymentFile(t, "container-entrypoint")
	start := strings.Index(entrypoint, "    for name in \\\n      "+reapAttestationVariables[0])
	if start < 0 {
		t.Fatal("container-entrypoint has no reap attestation forwarding loop")
	}
	end := strings.Index(entrypoint[start:], "\n    done\n")
	if end < 0 {
		t.Fatal("reap attestation forwarding loop has no terminator")
	}
	return "#!/usr/bin/env bash\nset -euo pipefail\ndeclare -a runtime_environment=()\n" +
		entrypoint[start:start+end+len("\n    done\n")] +
		"printf '%s\\n' \"${runtime_environment[@]+\"${runtime_environment[@]}\"}\"\n"
}

func TestSupervisorRedactsReapAttestationNonceDiagnostics(t *testing.T) {
	supervisor := readDeploymentFile(t, "supervisor")
	start := strings.Index(supervisor, "redact_activation_diagnostic_stream() {")
	if start < 0 {
		t.Fatal("supervisor diagnostic redaction function not found")
	}
	end := strings.Index(supervisor[start:], "\n}\n\nreport_long_lived_role_logs()")
	if end < 0 {
		t.Fatal("supervisor diagnostic redaction function is incomplete")
	}
	function := supervisor[start : start+end+2]
	scriptPath := filepath.Join(t.TempDir(), "redact-diagnostic.sh")
	script := "#!/bin/bash\nset -euo pipefail\n" + function + "\nredact_activation_diagnostic_stream\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	const nonce = "reap-attestation-rpc-boot-nonce"
	command := exec.Command("/bin/bash", scriptPath)
	command.Env = append(os.Environ(), "SUMI_REAP_ATTESTATION_RPC_BOOT_NONCE="+nonce)
	command.Stdin = strings.NewReader("activation failed: " + nonce + "\n")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run supervisor diagnostic redaction: %v\n%s", err, output)
	}
	if strings.Contains(string(output), nonce) {
		t.Fatalf("reap attestation nonce leaked in supervisor diagnostic: %s", output)
	}
	if !strings.Contains(string(output), "<redacted:SUMI_REAP_ATTESTATION_RPC_BOOT_NONCE>") {
		t.Fatalf("supervisor did not mark reap attestation nonce redacted: %s", output)
	}
}

func TestSupervisorPrepareRejectsMissingSetsidBeforeLifecycle(t *testing.T) {
	if _, err := exec.LookPath("unshare"); err != nil {
		t.Skip("unshare is required to isolate the supervisor trust roots")
	}
	if output, err := exec.Command("unshare", "-Urnm", "/bin/true").CombinedOutput(); err != nil {
		t.Skipf("user and mount namespaces are unavailable: %v: %s", err, output)
	}

	testRoot := t.TempDir()
	binDir := filepath.Join(testRoot, "bin")
	if err := os.Mkdir(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"dirname", "readlink", "flock", "install", "mount"} {
		path, err := exec.LookPath(name)
		if err != nil {
			t.Fatalf("find %s: %v", name, err)
		}
		if err := os.Symlink(path, filepath.Join(binDir, name)); err != nil {
			t.Fatalf("link %s: %v", name, err)
		}
	}
	dockerLog := filepath.Join(testRoot, "docker.log")
	fakeDocker := filepath.Join(binDir, "docker")
	if err := os.WriteFile(fakeDocker, []byte(`#!/bin/sh
printf '%s\n' "$*" >> "$SUMI_FAKE_DOCKER_LOG"
`), 0o755); err != nil {
		t.Fatal(err)
	}
	fakeStat := filepath.Join(binDir, "stat")
	if err := os.WriteFile(fakeStat, []byte(`#!/bin/sh
if [ "$#" -eq 4 ] && [ "$1" = "-c" ] && [ "$3" = "--" ] && [ "$4" = "/" ]; then
  case "$2" in
    %u) printf '0\n'; exit 0 ;;
    %a) printf '755\n'; exit 0 ;;
  esac
fi
exec /usr/bin/stat "$@"
`), 0o755); err != nil {
		t.Fatal(err)
	}

	supervisor, err := filepath.Abs(repositoryFilePath("deploy", "agent", "supervisor"))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(
		"unshare", "-Urnm", "/bin/bash", "-eu", "-c",
		`mount -t tmpfs -o mode=0755 tmpfs /run; exec "$1" prepare`,
		"--", supervisor,
	)
	command.Env = []string{
		"PATH=" + binDir,
		"SUMI_CONFIG_FILE=/dev/null",
		"SUMI_FAKE_DOCKER_LOG=" + dockerLog,
		"SUMI_PERSONALITY_AGENT_ID=" + testPAID,
	}
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "setsid is required for supervised Compose launch") {
		t.Fatalf("supervisor did not reject missing setsid before lifecycle: err=%v output=%s", err, output)
	}
	calls, err := os.ReadFile(dockerLog)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(calls), "compose.lifecycle.yaml") {
		t.Fatalf("supervisor reached lifecycle work before rejecting missing setsid:\n%s", calls)
	}
}

func TestDeploymentPrepareGraphCannotStartLongLivedRoles(t *testing.T) {
	prepare := readDeploymentFile(t, "compose.prepare.yaml")
	for _, service := range []string{"runtime", "executor", "broker"} {
		pattern := regexp.MustCompile(`(?m)^  ` + service + `:$`)
		if pattern.MatchString(prepare) {
			t.Fatalf("prepare graph contains long-lived %s service", service)
		}
	}
	if !regexp.MustCompile(`(?m)^  allocator:$`).MatchString(prepare) || !regexp.MustCompile(`(?m)^  prepare:$`).MatchString(prepare) {
		t.Fatal("prepare graph omits allocator or filesystem prepare role")
	}
	supervisor := readDeploymentFile(t, "supervisor")
	prepareStart := strings.Index(supervisor, "  prepare)")
	activateStart := strings.Index(supervisor, "  activate)")
	if prepareStart < 0 || activateStart <= prepareStart {
		t.Fatal("supervisor has no explicit prepare-to-activate phase boundary")
	}
	prepareAction := supervisor[prepareStart:activateStart]
	for _, activationOnly := range []string{
		"validate_local_control_socket",
		"revalidate_local_control_socket",
		"SUMI_LOCAL_CONTROL_SERVER_UID",
		"SUMI_LOCAL_CONTROL_SOCKET_GID",
	} {
		if strings.Contains(prepareAction, activationOnly) {
			t.Fatalf("prepare phase depends on activation-only local-control state %q:\n%s", activationOnly, prepareAction)
		}
	}
}

func TestProvisionerImageInstallsComposeLifetimeAnchor(t *testing.T) {
	dockerfile := readDeploymentFile(t, "../provisioner/Dockerfile")
	for _, required := range []string{
		"go build -o /usr/local/libexec/sumi-compose-anchor ./cmd/compose-anchor",
		"COPY --from=build /usr/local/libexec/sumi-compose-anchor /usr/local/libexec/sumi-compose-anchor",
	} {
		if !strings.Contains(dockerfile, required) {
			t.Fatalf("provisioner image omits Compose lifetime anchor contract %q:\n%s", required, dockerfile)
		}
	}
}

func TestSupervisorAdvertisesCleanupBoundForFullCleanupPath(t *testing.T) {
	const (
		millisecondsPerSecond               = 1000
		composeChildTermAttempts            = 150
		composeChildTermDelayMilliseconds   = 100
		cleanupMaxAttempts                  = 3
		cleanupRetryDelayMilliseconds       = 100
		composeDownPostStopAllowanceSeconds = 5
		downKillGraceSeconds                = 1
		verificationTimeoutSeconds          = 5
		verificationKillGraceSeconds        = 1
		anchorForceGraceSeconds             = 1
		controlAckTimeoutSeconds            = 1
		maximumNestedAnchors                = 7
		controlDrainJoinSeconds             = 1
		schedulingMarginSeconds             = 10
		composeTimeoutSeconds               = 7
	)

	supervisorPath, err := filepath.Abs(repositoryFilePath("deploy", "agent", "supervisor"))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(
		"/bin/bash", "-c", `exec 3>&1; exec "$1" unsupported-action`, "--", supervisorPath,
	)
	command.Env = []string{
		"PATH=/usr/bin:/bin",
		"SUMI_CONFIG_FILE=/dev/null",
		"SUMI_PERSONALITY_AGENT_ID=" + testPAID,
		"SUMI_SUPERVISOR_CONTROL_FD=3",
		"SUMI_COMPOSE_TIMEOUT=" + strconv.Itoa(composeTimeoutSeconds),
	}
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("supervisor accepted an unsupported action: %s", output)
	}
	match := regexp.MustCompile(`(?m)^cleanup-bound-ms ([0-9]+)$`).FindSubmatch(output)
	if match == nil {
		t.Fatalf("supervisor did not advertise a cleanup bound: %s", output)
	}
	got, err := strconv.ParseInt(string(match[1]), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	want := int64(
		composeChildTermAttempts*composeChildTermDelayMilliseconds +
			cleanupMaxAttempts*(composeTimeoutSeconds+composeDownPostStopAllowanceSeconds+downKillGraceSeconds)*millisecondsPerSecond +
			cleanupMaxAttempts*(verificationTimeoutSeconds+verificationKillGraceSeconds)*millisecondsPerSecond +
			cleanupMaxAttempts*2*anchorForceGraceSeconds*millisecondsPerSecond +
			(cleanupMaxAttempts-1)*cleanupRetryDelayMilliseconds +
			maximumNestedAnchors*3*controlAckTimeoutSeconds*millisecondsPerSecond +
			controlDrainJoinSeconds*millisecondsPerSecond +
			schedulingMarginSeconds*millisecondsPerSecond,
	)
	if got != want {
		t.Fatalf("advertised cleanup bound = %dms, want %dms for TERM grace, down retries, verification, retry delays, and margin", got, want)
	}

	compactSupervisor := strings.Join(strings.Fields(readDeploymentFile(t, "supervisor")), " ")
	for _, term := range []string{
		"COMPOSE_CHILD_TERM_ATTEMPTS * COMPOSE_CHILD_TERM_DELAY_MILLISECONDS",
		"SUMI_COMPOSE_TIMEOUT + COMPOSE_DOWN_POST_STOP_ALLOWANCE_SECONDS",
		"CLEANUP_MAX_ATTEMPTS * ( CLEANUP_DOWN_TIMEOUT_SECONDS + CLEANUP_DOWN_KILL_GRACE_SECONDS + CLEANUP_ANCHOR_FORCE_GRACE_SECONDS ) * MILLISECONDS_PER_SECOND",
		"CLEANUP_MAX_ATTEMPTS * ( CLEANUP_VERIFICATION_TIMEOUT_SECONDS + CLEANUP_VERIFICATION_KILL_GRACE_SECONDS + CLEANUP_ANCHOR_FORCE_GRACE_SECONDS ) * MILLISECONDS_PER_SECOND",
		"(CLEANUP_MAX_ATTEMPTS - 1) * CLEANUP_RETRY_DELAY_MILLISECONDS",
		"SUPERVISOR_MAX_NESTED_ANCHORS * 3 * SUPERVISOR_CONTROL_ACK_TIMEOUT_SECONDS * MILLISECONDS_PER_SECOND",
		"SUPERVISOR_CONTROL_DRAIN_JOIN_SECONDS * MILLISECONDS_PER_SECOND",
		"SUPERVISOR_CLEANUP_SCHEDULING_MARGIN_SECONDS * MILLISECONDS_PER_SECOND",
	} {
		if !strings.Contains(compactSupervisor, term) {
			t.Fatalf("supervisor cleanup bound omits %q:\n%s", term, compactSupervisor)
		}
	}
	for _, row := range []string{
		"down --timeout SUMI_COMPOSE_TIMEOUT container-stop grace stop + TERM + force + ACK/join anchored Docker process group",
		"ps --all --filter project-label none query + TERM + force + ACK/join anchored Docker process group",
	} {
		if !strings.Contains(compactSupervisor, row) {
			t.Fatalf("supervisor cleanup group-deadline table omits %q:\n%s", row, compactSupervisor)
		}
	}
	if !strings.Contains(compactSupervisor, "cleanup_down_lifecycle_compose down --remove-orphans --timeout \"${SUMI_COMPOSE_TIMEOUT}\"") {
		t.Fatalf("cleanup down is not subject to the advertised timeout: %s", compactSupervisor)
	}
	for _, required := range []string{
		`output="$(cleanup_verification_docker ps --all`,
		`--filter "label=com.docker.compose.project=${SUMI_COMPOSE_PROJECT}"`,
		`--format '{{.ID}}' 2>/dev/null)"`,
	} {
		if !strings.Contains(compactSupervisor, required) {
			t.Fatalf("direct project-label absence proof is not subject to the advertised timeout; missing %q: %s", required, compactSupervisor)
		}
	}
	for _, required := range []string{
		"normal_deadline_milliseconds=$(( $(monotonic_time_milliseconds) + timeout_seconds * MILLISECONDS_PER_SECOND ))",
		"term_deadline_milliseconds=$(( normal_deadline_milliseconds + kill_grace_seconds * MILLISECONDS_PER_SECOND ))",
		"force_deadline_milliseconds=$(( term_deadline_milliseconds + CLEANUP_ANCHOR_FORCE_GRACE_SECONDS * MILLISECONDS_PER_SECOND ))",
	} {
		if !strings.Contains(compactSupervisor, required) {
			t.Fatalf("supervisor cleanup deadline does not reserve TERM grace from one clock %q:\n%s", required, compactSupervisor)
		}
	}
}

func TestSupervisorCleanupKillsForkedDockerChildAfterLeaderExit(t *testing.T) {
	if _, err := exec.LookPath("unshare"); err != nil {
		t.Skip("unshare is required to isolate the supervisor trust roots")
	}
	if output, err := exec.Command("unshare", "-Urnm", "/bin/true").CombinedOutput(); err != nil {
		t.Skipf("user and mount namespaces are unavailable: %v: %s", err, output)
	}

	testRoot := t.TempDir()
	dockerLog := filepath.Join(testRoot, "docker.log")
	prepareReady := filepath.Join(testRoot, "prepare-ready")
	hangCleanupDown := filepath.Join(testRoot, "hang-cleanup-down")
	cleanupDownStarted := filepath.Join(testRoot, "cleanup-down-started")
	cleanupChildPID := filepath.Join(testRoot, "cleanup-child-pid")
	fakeDocker := filepath.Join(testRoot, "docker")
	fakeDockerScript := `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$SUMI_FAKE_DOCKER_LOG"
case "$*" in
  *"compose.lifecycle.yaml down"*)
    if [ -e "$SUMI_HANG_CLEANUP_DOWN" ]; then
      /bin/sh -c 'trap "" TERM; printf "$$" > "$SUMI_CLEANUP_CHILD_PID"; printf started > "$SUMI_CLEANUP_DOWN_STARTED"; while :; do :; done' &
      exit 0
    fi
    ;;
  *"compose.prepare.yaml up --detach --wait"*)
    printf ready > "$SUMI_PREPARE_READY"
    trap 'exit 143' TERM
    while :; do sleep 1; done
    ;;
esac
`
	if err := os.WriteFile(fakeDocker, []byte(fakeDockerScript), 0o755); err != nil {
		t.Fatal(err)
	}
	fakeStat := filepath.Join(testRoot, "stat")
	fakeStatScript := `#!/bin/sh
if [ "$#" -eq 4 ] && [ "$1" = "-c" ] && [ "$3" = "--" ] && [ "$4" = "/" ]; then
  case "$2" in
    %u) printf '0\n'; exit 0 ;;
    %a) printf '755\n'; exit 0 ;;
  esac
fi
exec /usr/bin/stat "$@"
`
	if err := os.WriteFile(fakeStat, []byte(fakeStatScript), 0o755); err != nil {
		t.Fatal(err)
	}

	supervisor, err := filepath.Abs(repositoryFilePath("deploy", "agent", "supervisor"))
	if err != nil {
		t.Fatal(err)
	}
	environment := []string{
		"PATH=" + testRoot + ":/usr/bin:/bin",
		"SUMI_CONFIG_FILE=/dev/null",
		"SUMI_COMPOSE_ANCHOR=" + composeAnchorBinary(t),
		"SUMI_DEV_ALLOW_APPARMOR_UNCONFINED=true",
		"SUMI_FAKE_DOCKER_LOG=" + dockerLog,
		"SUMI_PERSONALITY_AGENT_ID=" + testPAID,
		"SUMI_PREPARE_READY=" + prepareReady,
		"SUMI_HANG_CLEANUP_DOWN=" + hangCleanupDown,
		"SUMI_CLEANUP_DOWN_STARTED=" + cleanupDownStarted,
		"SUMI_CLEANUP_CHILD_PID=" + cleanupChildPID,
		"SUMI_COMPOSE_TIMEOUT=1",
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := (execCommandRunner{pipeWait: 100 * time.Millisecond}).Run(
			ctx,
			"unshare",
			[]string{"-Urnm", "/bin/bash", "-eu", "-c", `mount -t tmpfs -o mode=0755 tmpfs /run; exec "$1" prepare`, "--", supervisor},
			environment,
		)
		done <- err
	}()
	waitForSupervisorFile(t, prepareReady, 5*time.Second)
	if err := os.WriteFile(hangCleanupDown, []byte("hang"), 0o600); err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	cancel()
	var waitErr error
	select {
	case waitErr = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("supervisor cleanup did not bound a hung docker compose down")
	}
	elapsed := time.Since(started)
	if !errors.Is(waitErr, context.Canceled) {
		t.Fatalf("supervisor cancellation error = %v, want context cancellation", waitErr)
	}
	waitForSupervisorFile(t, cleanupDownStarted, time.Second)
	childPID := readSupervisorPID(t, cleanupChildPID)
	waitForSupervisorProcessGone(t, childPID, 2*time.Second)
	// This tighter assertion makes the fake daemon hang observable while
	// reserving the exact descendant join and control acknowledgement after the
	// eight-second down/TERM/force group budget. The advertised bound also
	// includes retries and conservative host margin.
	if elapsed > 10*time.Second {
		t.Fatalf("forked cleanup child escaped its process-group deadline: cleanup took %s", elapsed)
	}
}

func TestSupervisorCleanupAllowsPostStopComposeFinalization(t *testing.T) {
	if _, err := exec.LookPath("unshare"); err != nil {
		t.Skip("unshare is required to isolate the supervisor trust roots")
	}
	if output, err := exec.Command("unshare", "-Urnm", "/bin/true").CombinedOutput(); err != nil {
		t.Skipf("user and mount namespaces are unavailable: %v: %s", err, output)
	}

	testRoot := t.TempDir()
	dockerLog := filepath.Join(testRoot, "docker.log")
	prepareReady := filepath.Join(testRoot, "prepare-ready")
	delayCleanupDown := filepath.Join(testRoot, "delay-cleanup-down")
	cleanupDownFinished := filepath.Join(testRoot, "cleanup-down-finished")
	fakeDocker := filepath.Join(testRoot, "docker")
	fakeDockerScript := `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$SUMI_FAKE_DOCKER_LOG"
case "$*" in
  *"compose.lifecycle.yaml down"*)
    if [ -e "$SUMI_DELAY_CLEANUP_DOWN" ]; then
      # Model a container using all of Docker's --timeout stop grace before
      # Compose records completion and removes the remaining project state.
      sleep "$SUMI_COMPOSE_TIMEOUT"
      sleep 0.2
      printf finished > "$SUMI_CLEANUP_DOWN_FINISHED"
    fi
    ;;
  *"compose.prepare.yaml up --detach --wait"*)
    printf ready > "$SUMI_PREPARE_READY"
    trap 'exit 143' TERM
    while :; do sleep 1; done
    ;;
esac
`
	if err := os.WriteFile(fakeDocker, []byte(fakeDockerScript), 0o755); err != nil {
		t.Fatal(err)
	}
	fakeStat := filepath.Join(testRoot, "stat")
	fakeStatScript := `#!/bin/sh
if [ "$#" -eq 4 ] && [ "$1" = "-c" ] && [ "$3" = "--" ] && [ "$4" = "/" ]; then
  case "$2" in
    %u) printf '0\n'; exit 0 ;;
    %a) printf '755\n'; exit 0 ;;
  esac
fi
exec /usr/bin/stat "$@"
`
	if err := os.WriteFile(fakeStat, []byte(fakeStatScript), 0o755); err != nil {
		t.Fatal(err)
	}

	supervisor, err := filepath.Abs(repositoryFilePath("deploy", "agent", "supervisor"))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(
		"unshare", "-Urnm", "/bin/bash", "-eu", "-c",
		`mount -t tmpfs -o mode=0755 tmpfs /run; exec "$1" prepare`, "--", supervisor,
	)
	command.Env = []string{
		"PATH=" + testRoot + ":/usr/bin:/bin",
		"SUMI_CONFIG_FILE=/dev/null",
		"SUMI_COMPOSE_ANCHOR=" + composeAnchorBinary(t),
		"SUMI_DEV_ALLOW_APPARMOR_UNCONFINED=true",
		"SUMI_FAKE_DOCKER_LOG=" + dockerLog,
		"SUMI_PERSONALITY_AGENT_ID=" + testPAID,
		"SUMI_PREPARE_READY=" + prepareReady,
		"SUMI_DELAY_CLEANUP_DOWN=" + delayCleanupDown,
		"SUMI_CLEANUP_DOWN_FINISHED=" + cleanupDownFinished,
		"SUMI_COMPOSE_TIMEOUT=1",
	}
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waitForSupervisorFile(t, prepareReady, 5*time.Second)
	if err := os.WriteFile(delayCleanupDown, []byte("delay"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("supervisor prepare unexpectedly succeeded after TERM: %s", output.String())
		}
	case <-time.After(8 * time.Second):
		_ = command.Process.Kill()
		<-done
		t.Fatal("supervisor cleanup did not complete a post-stop compose finalization")
	}
	waitForSupervisorFile(t, cleanupDownFinished, time.Second)
}

func waitForSupervisorFile(t *testing.T, path string, limit time.Duration) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func assertBalancedNestedLifecycle(t *testing.T, output []byte) {
	t.Helper()
	var active string
	var starts int
	var readies int
	ready := false
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || (fields[0] != "nested-start" && fields[0] != "nested-ready" && fields[0] != "nested-done") {
			continue
		}
		if fields[0] == "nested-start" {
			if active != "" {
				t.Fatalf("nested lifecycle overlapped %s with %s: %s", active, fields[1], output)
			}
			active = fields[1]
			ready = false
			starts++
			continue
		}
		if fields[0] == "nested-ready" {
			if active == "" || active != fields[1] || ready {
				t.Fatalf("nested group verification was not ordered for %s while %s was active: %s", fields[1], active, output)
			}
			ready = true
			readies++
			continue
		}
		if active == "" || active != fields[1] {
			t.Fatalf("nested lifecycle completed %s while %s was active: %s", fields[1], active, output)
		}
		active = ""
		ready = false
	}
	if starts == 0 || readies == 0 || active != "" {
		t.Fatalf("nested lifecycle was not balanced and verified: starts=%d readies=%d active=%q output=%s", starts, readies, active, output)
	}
}

func readSupervisorPID(t *testing.T, path string) int {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid <= 0 {
		t.Fatalf("read PID from %s: value=%q err=%v", path, raw, err)
	}
	return pid
}

func waitForSupervisorProcessGone(t *testing.T, pid int, limit time.Duration) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("cleanup Docker child %d survived its process-group deadline", pid)
}

func TestDeploymentActivateCannotRerunAllocatorOrExposeDockerSocket(t *testing.T) {
	supervisor := readDeploymentFile(t, "supervisor")
	activateStart := strings.Index(supervisor, "  activate)")
	abortStart := strings.Index(supervisor, "  abort)")
	if activateStart < 0 || abortStart <= activateStart {
		t.Fatal("supervisor has no explicit activate phase")
	}
	activate := supervisor[activateStart:abortStart]
	if !strings.Contains(activate, "--no-deps") || !strings.Contains(activate, "executor broker runtime") {
		t.Fatalf("activate phase can reach a one-shot allocator dependency:\n%s", activate)
	}
	launchEnvironmentStart := strings.Index(supervisor, "require_launch_environment()")
	launchEnvironmentEnd := strings.Index(supervisor, "\nrequire_paid\n")
	if launchEnvironmentStart < 0 || launchEnvironmentEnd <= launchEnvironmentStart ||
		!strings.Contains(supervisor[launchEnvironmentStart:launchEnvironmentEnd], "validate_local_control_socket") {
		t.Fatal("activation environment validation does not establish the local-control trust snapshot")
	}
	validate := strings.Index(activate, "require_launch_environment")
	launch := strings.Index(activate, `run_tracked_compose "${COMPOSE_FILE}"`)
	firstRevalidation := strings.Index(activate, "revalidate_local_control_socket")
	lastRevalidation := strings.LastIndex(activate, "revalidate_local_control_socket")
	if validate < 0 || firstRevalidation <= validate || launch <= firstRevalidation || lastRevalidation <= launch {
		t.Fatalf("activate does not revalidate local-control trust immediately before and after runtime launch:\n%s", activate)
	}
	for _, file := range []string{"compose.yaml", "compose.prepare.yaml", "compose.lifecycle.yaml"} {
		if strings.Contains(readDeploymentFile(t, file), "docker.sock") {
			t.Fatalf("%s exposes the Docker socket to a container", file)
		}
	}
	stopStart := strings.Index(supervisor, "  stop|down)")
	statusStart := strings.Index(supervisor, "  status|ps)")
	if stopStart < 0 || statusStart <= stopStart || strings.Contains(supervisor[stopStart:statusStart], "--volumes") {
		t.Fatal("ordinary stop can remove PAID-private volumes")
	}
}

func TestLocalControlPlaneGivesDockerOnlyToProvisioner(t *testing.T) {
	compose := readRepositoryFile(t, "deploy", "local", "compose.dev.yaml")
	if count := strings.Count(compose, "/var/run/docker.sock:/var/run/docker.sock"); count != 1 {
		t.Fatalf("local control plane has %d Docker socket mounts, want exactly one", count)
	}
	provisionerStart := strings.Index(compose, "  runtime-provisioner:")
	webStart := strings.Index(compose, "  web:")
	if provisionerStart < 0 || webStart <= provisionerStart ||
		!strings.Contains(compose[provisionerStart:webStart], "/var/run/docker.sock:/var/run/docker.sock") {
		t.Fatal("Docker socket is not confined to runtime-provisioner service")
	}
	apiStart := strings.Index(compose, "  api:")
	if apiStart < 0 || provisionerStart <= apiStart || strings.Contains(compose[apiStart:provisionerStart], "docker.sock") {
		t.Fatal("API service can access Docker")
	}
	for _, forbidden := range []string{"  agent-runtime:", "  agent-executor:", "  agent-broker:"} {
		if strings.Contains(compose, forbidden) {
			t.Fatalf("shared control-plane compose contains static per-agent role %q", forbidden)
		}
	}
}

func TestFullStackProvisionedRolesRetainCanonicalSandboxHardening(t *testing.T) {
	controlPlane := readRepositoryFile(t, "deploy", "local", "compose.dev.yaml")
	agent := readDeploymentFile(t, "compose.yaml")
	if !strings.Contains(controlPlane, "runtime-provisioner:") ||
		!strings.Contains(controlPlane, "- /opt/sumi/deploy/agent/supervisor") {
		t.Fatal("full stack does not route PAID lifecycle through the canonical agent supervisor")
	}
	for _, required := range []string{
		"SUMI_AGENT_GATEWAY_URL: ws://sumi-agent-gateway:8080/agent/ws",
		"name: sumi-control-plane", "external: true", "- sumi-agent-gateway",
		"network_mode: host",
		`command: ["pnpm", "dev", "--port", "5173"]`,
		"SUMI_DEV_HOST: ${SUMI_DEV_BIND_HOST:-127.0.0.1}",
		"SUMI_DEV_API_ORIGIN: http://${SUMI_DEV_BIND_HOST:-127.0.0.1}:8080",
	} {
		if !strings.Contains(controlPlane, required) {
			t.Fatalf("full stack stable control-plane bridge omits %q", required)
		}
	}
	if strings.Contains(controlPlane, "network_mode: service:api") ||
		strings.Contains(controlPlane, `:5173:5173"`) ||
		strings.Contains(controlPlane, `command: ["pnpm", "dev", "--host", "0.0.0.0"`) {
		t.Fatal("web shares the API netns, publishes an unreachable bridge port, or widens its host bind")
	}
	if strings.Contains(agent, "network_mode: container:") ||
		!strings.Contains(agent, "name: ${SUMI_CONTROL_PLANE_NETWORK:-sumi-control-plane}") {
		t.Fatal("provisioned runtime shares the API netns or lacks the stable external bridge")
	}
	anchorStart := strings.Index(agent, "x-long-lived-hardening:")
	servicesStart := strings.Index(agent, "services:")
	if anchorStart < 0 || servicesStart <= anchorStart {
		t.Fatal("agent descriptor omits shared long-lived sandbox hardening")
	}
	anchor := agent[anchorStart:servicesStart]
	for _, required := range []string{
		"read_only: true", "cap_drop: [ALL]", "no-new-privileges:true",
		"seccomp:./seccomp/sidecar.json", "apparmor:${SUMI_DOCKER_APPARMOR_PROFILE:-docker-default}",
	} {
		if !strings.Contains(anchor, required) {
			t.Fatalf("canonical long-lived sandbox omits %q", required)
		}
	}
	if count := strings.Count(agent, "<<: *long-lived-hardening"); count != 3 {
		t.Fatalf("runtime/executor/broker hardening applications=%d, want 3", count)
	}
	executorStart := strings.Index(agent, "  executor:")
	brokerStart := strings.Index(agent, "  broker:")
	if executorStart < 0 || brokerStart <= executorStart {
		t.Fatal("agent descriptor omits executor or broker")
	}
	executor := agent[executorStart:brokerStart]
	for _, required := range []string{
		`user: "10002:10002"`, "network_mode: none", "source: executor-ipc",
		"target: /run/sumi/executor", "source: workspace", "target: /workspace",
		"read_only: true", "nocopy: true",
	} {
		if !strings.Contains(executor, required) {
			t.Fatalf("provisioned executor omits isolation contract %q", required)
		}
	}
	entrypoint := readDeploymentFile(t, "container-entrypoint")
	if !strings.Contains(entrypoint, "readonly EXECUTOR_IPC_GID=10020") {
		t.Fatal("runtime/executor group IPC contract is absent")
	}
	if strings.Count(controlPlane, "stop_grace_period: 2m") != 2 {
		t.Fatal("API and provisioner do not share the bounded teardown grace period")
	}
	stackScript := readRepositoryFile(t, "scripts", "dev", "compose-stack")
	for _, required := range []string{"docker network create --driver bridge --internal", "must be an internal bridge network"} {
		if !strings.Contains(stackScript, required) {
			t.Fatalf("compose-stack does not retain/validate the external control-plane network: missing %q", required)
		}
	}
}

func TestLocalProvisionerForwardsPinnedAgentImageTag(t *testing.T) {
	controlPlane := readRepositoryFile(t, "deploy", "local", "compose.dev.yaml")
	provisionerStart := strings.Index(controlPlane, "  runtime-provisioner:")
	webStart := strings.Index(controlPlane, "  web:")
	if provisionerStart < 0 || webStart <= provisionerStart {
		t.Fatal("local compose omits the runtime provisioner service boundary")
	}
	provisioner := controlPlane[provisionerStart:webStart]
	if !strings.Contains(provisioner, "SUMI_AGENT_IMAGE_TAG: ${SUMI_AGENT_IMAGE_TAG:-latest}") {
		t.Fatal("local runtime provisioner does not forward SUMI_AGENT_IMAGE_TAG")
	}
	for _, required := range []string{
		"DOCKER_CONFIG: /run/sumi/docker-config",
		"- -state-dir",
		"- /run/sumi/runtime-provisioner/state",
		"source: ${SUMI_DOCKER_CONFIG_FILE:?SUMI_DOCKER_CONFIG_FILE is required}",
		"target: /run/sumi/docker-config/config.json",
		"read_only: true",
		"create_host_path: false",
	} {
		if !strings.Contains(provisioner, required) {
			t.Fatalf("local runtime provisioner Docker config handoff omits %q", required)
		}
	}
	if strings.Contains(provisioner, "/root/.docker") {
		t.Fatal("local runtime provisioner mounts a Docker configuration directory")
	}

	agent := readDeploymentFile(t, "compose.yaml")
	if !strings.Contains(agent, "image: ghcr.io/sumi-studio/sumi-agent:${SUMI_AGENT_IMAGE_TAG:-latest}") {
		t.Fatal("agent runtime compose does not interpolate SUMI_AGENT_IMAGE_TAG into the sumi-agent image")
	}
}

func readDeploymentFile(t *testing.T, name string) string {
	t.Helper()
	return readRepositoryFile(t, "deploy", "agent", name)
}

func readRepositoryFile(t *testing.T, parts ...string) string {
	t.Helper()
	path := repositoryFilePath(parts...)
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(contents)
}

func repositoryFilePath(parts ...string) string {
	pathParts := append([]string{"..", "..", "..", ".."}, parts...)
	return filepath.Join(pathParts...)
}
