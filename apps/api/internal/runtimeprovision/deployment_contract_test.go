package runtimeprovision

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

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
  *"compose.prepare.yaml run --rm --no-deps --entrypoint /bin/bash allocator"*)
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
			(cleanupMaxAttempts-1)*cleanupRetryDelayMilliseconds +
			schedulingMarginSeconds*millisecondsPerSecond,
	)
	if got != want {
		t.Fatalf("advertised cleanup bound = %dms, want %dms for TERM grace, down retries, verification, retry delays, and margin", got, want)
	}

	compactSupervisor := strings.Join(strings.Fields(readDeploymentFile(t, "supervisor")), " ")
	for _, term := range []string{
		"COMPOSE_CHILD_TERM_ATTEMPTS * COMPOSE_CHILD_TERM_DELAY_MILLISECONDS",
		"SUMI_COMPOSE_TIMEOUT + COMPOSE_DOWN_POST_STOP_ALLOWANCE_SECONDS",
		"CLEANUP_MAX_ATTEMPTS * ( CLEANUP_DOWN_TIMEOUT_SECONDS + CLEANUP_DOWN_KILL_GRACE_SECONDS ) * MILLISECONDS_PER_SECOND",
		"CLEANUP_MAX_ATTEMPTS * ( CLEANUP_VERIFICATION_TIMEOUT_SECONDS + CLEANUP_VERIFICATION_KILL_GRACE_SECONDS ) * MILLISECONDS_PER_SECOND",
		"(CLEANUP_MAX_ATTEMPTS - 1) * CLEANUP_RETRY_DELAY_MILLISECONDS",
		"SUPERVISOR_CLEANUP_SCHEDULING_MARGIN_SECONDS * MILLISECONDS_PER_SECOND",
	} {
		if !strings.Contains(compactSupervisor, term) {
			t.Fatalf("supervisor cleanup bound omits %q:\n%s", term, compactSupervisor)
		}
	}
	for _, row := range []string{
		"down --timeout SUMI_COMPOSE_TIMEOUT container-stop grace stop grace + post-stop allowance + TERM grace Docker process group (all descendants)",
		"ps --all --quiet none verification timeout + TERM grace Docker process group (all descendants)",
	} {
		if !strings.Contains(compactSupervisor, row) {
			t.Fatalf("supervisor cleanup group-deadline table omits %q:\n%s", row, compactSupervisor)
		}
	}
	if !strings.Contains(compactSupervisor, "cleanup_down_lifecycle_compose down --remove-orphans --timeout \"${SUMI_COMPOSE_TIMEOUT}\"") {
		t.Fatalf("cleanup down is not subject to the advertised timeout: %s", compactSupervisor)
	}
	if !strings.Contains(compactSupervisor, "containers=\"$(cleanup_verification_lifecycle_compose ps --all --quiet 2>/dev/null)\"") {
		t.Fatalf("cleanup emptiness verification is not subject to the advertised timeout: %s", compactSupervisor)
	}
	for _, required := range []string{
		"deadline_milliseconds=$(( $(monotonic_time_milliseconds) + (timeout_seconds + kill_grace_seconds) * MILLISECONDS_PER_SECOND ))",
		"milliseconds_until_cleanup_term \"${deadline_milliseconds}\" \"${kill_grace_seconds}\"",
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
	if _, err := exec.LookPath("perl"); err != nil {
		t.Skip("perl is required to hold a cleanup process group outside a new session")
	}
	if output, err := exec.Command("unshare", "-Urnm", "/bin/true").CombinedOutput(); err != nil {
		t.Skipf("user and mount namespaces are unavailable: %v: %s", err, output)
	}

	testRoot := t.TempDir()
	dockerLog := filepath.Join(testRoot, "docker.log")
	prepareReady := filepath.Join(testRoot, "prepare-ready")
	hangCleanupDown := filepath.Join(testRoot, "hang-cleanup-down")
	forceUnisolatedCleanup := filepath.Join(testRoot, "force-unisolated-cleanup")
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
	fakeSetsid := filepath.Join(testRoot, "setsid")
	fakeSetsidScript := `#!/bin/sh
set -eu
case "$*" in
  *"compose.lifecycle.yaml"*)
    if [ -e "$SUMI_FORCE_UNISOLATED_CLEANUP" ]; then
      # Keep the child process group alive but outside a new session. This
      # makes the supervisor's /proc isolation probe consume its full normal
      # budget before the process-group cleanup path takes over.
      exec /usr/bin/perl -MPOSIX -e 'POSIX::setpgid(0, 0) or die "$!\n"; exec @ARGV' "$@"
    fi
    ;;
esac
exec /usr/bin/setsid "$@"
`
	if err := os.WriteFile(fakeSetsid, []byte(fakeSetsidScript), 0o755); err != nil {
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
	controlRead, controlWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer controlRead.Close()
	command := exec.Command(
		"unshare", "-Urnm", "/bin/bash", "-eu", "-c",
		`mount -t tmpfs -o mode=0755 tmpfs /run; exec "$1" prepare`, "--", supervisor,
	)
	command.Env = []string{
		"PATH=" + testRoot + ":/usr/bin:/bin",
		"SUMI_CONFIG_FILE=/dev/null",
		"SUMI_DEV_ALLOW_APPARMOR_UNCONFINED=true",
		"SUMI_FAKE_DOCKER_LOG=" + dockerLog,
		"SUMI_PERSONALITY_AGENT_ID=" + testPAID,
		"SUMI_PREPARE_READY=" + prepareReady,
		"SUMI_HANG_CLEANUP_DOWN=" + hangCleanupDown,
		"SUMI_FORCE_UNISOLATED_CLEANUP=" + forceUnisolatedCleanup,
		"SUMI_CLEANUP_DOWN_STARTED=" + cleanupDownStarted,
		"SUMI_CLEANUP_CHILD_PID=" + cleanupChildPID,
		"SUMI_SUPERVISOR_CONTROL_FD=3",
		"SUMI_COMPOSE_TIMEOUT=1",
	}
	command.ExtraFiles = []*os.File{controlWrite}
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		_ = controlWrite.Close()
		t.Fatal(err)
	}
	if err := controlWrite.Close(); err != nil {
		t.Fatal(err)
	}
	waitForSupervisorFile(t, prepareReady, 5*time.Second)
	if err := os.WriteFile(hangCleanupDown, []byte("hang"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(forceUnisolatedCleanup, []byte("force"), 0o600); err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	var waitErr error
	select {
	case waitErr = <-done:
	case <-time.After(8 * time.Second):
		_ = command.Process.Kill()
		<-done
		t.Fatal("supervisor cleanup did not bound a hung docker compose down")
	}
	elapsed := time.Since(started)
	if waitErr == nil {
		t.Fatal("supervisor prepare unexpectedly succeeded after TERM")
	}
	waitForSupervisorFile(t, cleanupDownStarted, time.Second)
	childPID := readSupervisorPID(t, cleanupChildPID)
	waitForSupervisorProcessGone(t, childPID, 2*time.Second)
	controlOutput, err := io.ReadAll(controlRead)
	if err != nil {
		t.Fatal(err)
	}
	match := regexp.MustCompile(`(?m)^cleanup-bound-ms ([0-9]+)$`).FindSubmatch(controlOutput)
	if match == nil {
		t.Fatalf("supervisor did not advertise cleanup bound: control=%s output=%s", controlOutput, output.String())
	}
	boundMilliseconds, err := strconv.ParseInt(string(match[1]), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	advertisedBound := time.Duration(boundMilliseconds) * time.Millisecond
	if elapsed > advertisedBound {
		t.Fatalf("cleanup took %s, exceeding advertised bound %s", elapsed, advertisedBound)
	}
	// This tighter assertion makes the fake daemon hang observable. The
	// advertised bound also includes three retries and conservative host margin.
	if elapsed > 8*time.Second {
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
