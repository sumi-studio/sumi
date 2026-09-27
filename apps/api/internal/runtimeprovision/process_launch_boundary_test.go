package runtimeprovision

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Launch-boundary tests: a shim `docker` binary gates the real daemon's
// create/start calls so cancellation and provisioner restart overlap an
// actually-in-flight launch. Real Docker steps are only DELAYED by the
// shim (create/start wait on marker files) — the daemon semantics are
// real: create produces an unstarted container, an uncertain start can
// still start it, and removal wins over a late start.

const dockerShimScript = `#!/bin/sh
sub="$1"
echo "$sub $2" >> "$SHIM_GATE/calls.log"
case "$sub" in
create)
  touch "$SHIM_GATE/create-seen"
  while [ -f "$SHIM_GATE/block-create" ]; do sleep 0.05; done
  ;;
start)
  touch "$SHIM_GATE/start-seen"
  if [ -f "$SHIM_GATE/start-uncertain" ]; then
    %s "$@" &
    sleep 0.2
    exit 1
  fi
  while [ -f "$SHIM_GATE/block-start" ]; do sleep 0.05; done
  ;;
esac
exec %s "$@"
`

// writeDockerShim installs a `docker` shim that delegates to
// `docker.real`, gating create/start on marker files in gate.
func writeDockerShim(t *testing.T, gate string) string {
	t.Helper()
	dir := t.TempDir()
	real, err := exec.LookPath("docker")
	if err != nil {
		t.Skip("docker binary unavailable")
	}
	// The real path is baked in: test helpers and the backend invoke the
	// shim under varying child environments, so PATH cannot carry it.
	script := fmt.Sprintf(dockerShimScript, real, real)
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	return dir
}

func dockerE2EBackend(t *testing.T, extraEnv ...string) (*DockerBackend, string) {
	t.Helper()
	tag := os.Getenv("SUMI_TEST_DOCKER_JOB_TAG")
	if tag == "" {
		tag = "899a7cdf1defdf9ce09d76cccaea48bf5f58a20d"
	}
	env := append([]string{
		"PATH=" + os.Getenv("PATH"),
		"DOCKER_HOST=unix:///var/run/docker.sock",
		"SUMI_JOB_IMAGE_TAG=" + tag,
	}, extraEnv...)
	return &DockerBackend{baseEnvironment: env, runner: execCommandRunner{}}, tag
}

// seedWorkspace creates the labelled persona volume the launcher
// requires and seeds it like the production agent mount.
func seedWorkspace(t *testing.T, persona, tag string) {
	t.Helper()
	docker := func(args ...string) {
		cmd := exec.Command("docker", args...)
		cmd.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "DOCKER_HOST=unix:///var/run/docker.sock"}
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
	agent32 := strings.ReplaceAll(persona, "-", "")
	volume := "sumi-" + agent32 + "_workspace"
	project := "sumi-" + agent32
	docker("volume", "create", "--label", "com.docker.compose.project="+project, "--label", "com.docker.compose.volume=workspace", volume)
	docker("run", "--rm", "-v", volume+":/workspace", "--entrypoint", "true", "ghcr.io/sumi-studio/sumi-job:"+tag)
	t.Cleanup(func() {
		c := exec.Command("docker", "volume", "rm", "-f", volume)
		c.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "DOCKER_HOST=unix:///var/run/docker.sock"}
		_ = c.Run()
		c2 := exec.Command("docker", "rm", "-f", "sumi-process-"+ProcessOperationID(persona, "launch-boundary"))
		c2.Env = c.Env
		_ = c2.Run()
	})
}

func waitFile(t *testing.T, path string, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func waitQuiesced(t *testing.T, s *Service, op ProcessOperation, d time.Duration) ProcessOperation {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		got, err := s.ProcessStatus(context.Background(), ProcessLookupRequest{PersonalityAgentID: op.PersonalityAgentID, OperationID: op.OperationID})
		if err == nil && got.Quiesced {
			return got
		}
		time.Sleep(50 * time.Millisecond)
	}
	got, _ := s.ProcessStatus(context.Background(), ProcessLookupRequest{PersonalityAgentID: op.PersonalityAgentID, OperationID: op.OperationID})
	t.Fatalf("operation never quiesced: state=%s quiesced=%v", got.State, got.Quiesced)
	return got
}

// dockerInspectState returns the real daemon's view of the op container.
func dockerInspectState(t *testing.T, op ProcessOperation) (exists, running bool) {
	t.Helper()
	cmd := exec.Command("docker", "inspect", "--format", "{{.State.Running}}", "sumi-process-"+op.OperationID)
	cmd.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "DOCKER_HOST=unix:///var/run/docker.sock"}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return false, false
	}
	return true, strings.TrimSpace(string(out)) == "true"
}

func boundaryOp(t *testing.T, s *Service, persona string) ProcessOperation {
	t.Helper()
	op, err := s.StartProcess(context.Background(), ProcessStartRequest{
		PersonalityAgentID:    persona,
		OriginatingToolCallID: "launch-boundary",
		Executable:            "/bin/sh",
		Interactive:           true,
		TTY:                   true,
		Image:                 "job",
		TimeoutSeconds:        600,
	})
	if err != nil {
		t.Fatal(err)
	}
	return op
}

// Cancellation committed while `docker create` is in flight: the
// launch completes, the post-launch inspect sees the running container
// plus the committed cancel, and it is killed — no writer survives.
func TestDockerLaunchBoundaryCancelInFlight(t *testing.T) {
	if os.Getenv("SUMI_TEST_DOCKER_E2E") != "1" {
		t.Skip("set SUMI_TEST_DOCKER_E2E=1")
	}
	gate := t.TempDir()
	if err := os.WriteFile(filepath.Join(gate, "block-create"), []byte("1"), 0644); err != nil {
		t.Fatal(err)
	}
	writeDockerShim(t, gate)
	backend, tag := dockerE2EBackend(t, "SHIM_GATE="+gate)
	persona := uuid.NewString()
	seedWorkspace(t, persona, tag)
	ps, err := newProcessStore(t.TempDir()+"/state", backend)
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{backend: backend, processes: ps}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go service.RunProcessObserver(ctx)
	op := boundaryOp(t, service, persona)

	waitFile(t, filepath.Join(gate, "create-seen"), 15*time.Second)
	if _, err := service.CancelProcess(context.Background(), ProcessLookupRequest{PersonalityAgentID: persona, OperationID: op.OperationID}); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(gate, "block-create")); err != nil {
		t.Fatal(err)
	}
	got := waitQuiesced(t, service, op, 30*time.Second)
	if got.State != ProcessCancelled {
		t.Fatalf("expected cancelled, got %s", got.State)
	}
	exists, running := dockerInspectState(t, op)
	if exists && running {
		t.Fatal("a writer survived the in-flight cancel")
	}
}

// An uncertain `docker start` (daemon started the container but the
// CLI reported failure) under a committed cancel: the post-launch
// inspect sees it running and kills it. The error string must not be
// treated as proof nothing started.
func TestDockerLaunchBoundaryUncertainStart(t *testing.T) {
	if os.Getenv("SUMI_TEST_DOCKER_E2E") != "1" {
		t.Skip("set SUMI_TEST_DOCKER_E2E=1")
	}
	gate := t.TempDir()
	for _, m := range []string{"block-create", "start-uncertain"} {
		if err := os.WriteFile(filepath.Join(gate, m), []byte("1"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	writeDockerShim(t, gate)
	backend, tag := dockerE2EBackend(t, "SHIM_GATE="+gate)
	persona := uuid.NewString()
	seedWorkspace(t, persona, tag)
	ps, err := newProcessStore(t.TempDir()+"/state", backend)
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{backend: backend, processes: ps}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go service.RunProcessObserver(ctx)
	op := boundaryOp(t, service, persona)

	waitFile(t, filepath.Join(gate, "create-seen"), 15*time.Second)
	if _, err := service.CancelProcess(context.Background(), ProcessLookupRequest{PersonalityAgentID: persona, OperationID: op.OperationID}); err != nil {
		t.Fatal(err)
	}
	// Release create; the shim's start fires the real start then
	// reports failure — the daemon still started the container.
	if err := os.Remove(filepath.Join(gate, "block-create")); err != nil {
		t.Fatal(err)
	}
	got := waitQuiesced(t, service, op, 30*time.Second)
	if got.State != ProcessCancelled {
		t.Fatalf("expected cancelled, got %s (%s)", got.State, got.Error)
	}
	exists, running := dockerInspectState(t, op)
	if exists && running {
		t.Fatal("an uncertain-start container survived the cut")
	}
}

// Provisioner restart between create and start: LaunchAttempted is
// journaled before the daemon calls, so a second store sees the op
// mid-launch. The shim holds `docker start` until the second store has
// already delivered a terminal verdict — the late start then lands on
// a terminal op and the reconcile must kill the container it created.
func TestDockerLaunchBoundaryRestartDuringStart(t *testing.T) {
	if os.Getenv("SUMI_TEST_DOCKER_E2E") != "1" {
		t.Skip("set SUMI_TEST_DOCKER_E2E=1")
	}
	gate := t.TempDir()
	if err := os.WriteFile(filepath.Join(gate, "block-start"), []byte("1"), 0644); err != nil {
		t.Fatal(err)
	}
	writeDockerShim(t, gate)
	backend, tag := dockerE2EBackend(t, "SHIM_GATE="+gate)
	persona := uuid.NewString()
	seedWorkspace(t, persona, tag)
	stateDir := t.TempDir() + "/state"
	ps1, err := newProcessStore(stateDir, backend)
	if err != nil {
		t.Fatal(err)
	}
	service1 := &Service{backend: backend, processes: ps1}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go service1.RunProcessObserver(ctx)
	op := boundaryOp(t, service1, persona)

	// Wait until the real `docker start` call is in flight, then load a
	// second store on the same journal — the restarted provisioner.
	waitFile(t, filepath.Join(gate, "start-seen"), 20*time.Second)
	ps2, err := newProcessStore(stateDir, backend)
	if err != nil {
		t.Fatal(err)
	}
	service2 := &Service{backend: backend, processes: ps2}

	// The second store's first inspect sees a created-not-started
	// container: indeterminate terminal verdict while `docker start`
	// is still blocked.
	service2.observeProcesses(ctx)
	got, err := service2.ProcessStatus(ctx, ProcessLookupRequest{PersonalityAgentID: persona, OperationID: op.OperationID})
	if err != nil {
		t.Fatal(err)
	}
	if got.State != ProcessIndeterminate || got.Quiesced {
		t.Fatalf("expected non-quiesced indeterminate, got %s quiesced=%v", got.State, got.Quiesced)
	}

	// Release the in-flight start: the container now actually starts
	// AFTER the terminal verdict. The reconcile must find and kill it.
	if err := os.Remove(filepath.Join(gate, "block-start")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		service2.observeProcesses(ctx)
		got, err = service2.ProcessStatus(ctx, ProcessLookupRequest{PersonalityAgentID: persona, OperationID: op.OperationID})
		if err == nil && got.Quiesced {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !got.Quiesced {
		t.Fatalf("terminal op with late-landing start never quiesced: %s", got.State)
	}
	exists, running := dockerInspectState(t, op)
	if exists && running {
		t.Fatal("a writer started after the terminal verdict and survived")
	}
}
