package runtimeprovision

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Real-Docker integration for the canonical files-scope process launch:
// every job op bind-mounts its persona's verified scope directory instead
// of the legacy shared volume. Requires an owned fixture check script
// standing in for sumi-files-check — the fixture enforces the same argv
// contract and the same "prints the resolved scope path" result, while the
// mountpoint itself is a plain directory. Real JuiceFS mount/fstype/UUID
// verification is covered by the files settlement's own acceptance; what
// this test adds is the launch path that depends on it.
//
// Opt-in: SUMI_TEST_PROCESS_DOCKER=1 plus
// SUMI_TEST_JOB_IMAGE_TAG=<40-hex revision of an existing sumi-job image>.
func TestProcessFilesScopeIntegration(t *testing.T) {
	if os.Getenv("SUMI_TEST_PROCESS_DOCKER") != "1" {
		t.Skip("requires explicit owned Docker integration opt-in")
	}
	jobTag := os.Getenv("SUMI_TEST_JOB_IMAGE_TAG")
	if !processImageTag.MatchString(jobTag) {
		t.Fatal("SUMI_TEST_JOB_IMAGE_TAG must name an existing full 40-character sumi-job revision")
	}
	ctx := context.Background()
	newPA := func() string {
		t.Helper()
		u, err := uuid.NewV7()
		if err != nil {
			t.Fatal(err)
		}
		return u.String()
	}
	paidA, paidB := newPA(), newPA()
	scopeName := func(paid string) string {
		return strings.ToLower(strings.ReplaceAll(paid, "-", ""))
	}

	// Owned fixture "mount": scope dirs a container-uid can write, which is
	// the shape the real all-squash JuiceFS mount presents.
	mount := t.TempDir()
	scopeA := filepath.Join(mount, scopeName(paidA))
	scopeB := filepath.Join(mount, scopeName(paidB))
	for _, d := range []string{scopeA, scopeB} {
		if err := os.MkdirAll(d, 0777); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(d, 0777); err != nil {
			t.Fatal(err)
		}
	}
	// A marker in B's scope: A's container must never see it — the bind
	// exposes exactly one scope subtree.
	if err := os.WriteFile(filepath.Join(scopeB, "b-secret.txt"), []byte("B"), 0644); err != nil {
		t.Fatal(err)
	}

	// Fixture files-check: same contract as deploy/files/sumi-files-check —
	// [--wait S] MOUNTPOINT VOLUME_UUID SCOPE, prints the resolved scope
	// path, refuses scope_missing.
	checkPath := filepath.Join(mount, "fixture-files-check")
	checkScript := `#!/bin/sh
wait=0
if [ "$1" = "--wait" ]; then wait="$2"; shift 2; fi
mnt="$1"; uuid="$2"; scope="$3"
[ -d "$mnt" ] || { echo "refused: not_mounted: $mnt" >&2; exit 2; }
[ -n "$uuid" ] || { echo "refused: usage: uuid required" >&2; exit 2; }
dir="$mnt/$scope"
[ -d "$dir" ] || { echo "refused: scope_missing: $scope" >&2; exit 2; }
printf '%s\n' "$(cd "$dir" && pwd -P)"
`
	if err := os.WriteFile(checkPath, []byte(checkScript), 0755); err != nil {
		t.Fatal(err)
	}
	volumeUUID := "5a1b72bc-b9ec-4a72-813b-2f4dc4cd6d07"

	docker := func(args ...string) []byte {
		t.Helper()
		cmd := exec.Command("docker", args...)
		raw, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("docker %s: %v %s", args[0], err, raw)
		}
		return raw
	}
	docker("image", "inspect", "ghcr.io/sumi-studio/sumi-job:"+jobTag)

	environment := []string{
		"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"),
		"SUMI_AGENT_IMAGE_TAG=" + jobTag,
		"SUMI_JOB_IMAGE_TAG=" + jobTag,
	}
	for _, key := range []string{"DOCKER_HOST", "DOCKER_CONFIG"} {
		if v := os.Getenv(key); v != "" {
			environment = append(environment, key+"="+v)
		}
	}
	backend := &DockerBackend{baseEnvironment: environment}
	directory := t.TempDir() + "/state"
	operations := []ProcessOperation{}
	t.Cleanup(func() {
		for _, o := range operations {
			_ = exec.Command("docker", "rm", "-f", processContainer(o)).Run()
		}
	})
	newService := func() *Service {
		t.Helper()
		s, err := NewService(backend, ServiceConfig{
			StateDirectory: directory,
			Files: FilesEnvironment{
				Mountpoint:       mount,
				VolumeUUID:       volumeUUID,
				CheckPath:        checkPath,
				CheckWaitSeconds: 5,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	service := newService()
	wait := func(o ProcessOperation) ProcessOperation {
		t.Helper()
		deadline := time.Now().Add(60 * time.Second)
		for time.Now().Before(deadline) {
			service.observeProcesses(ctx)
			current, err := service.ProcessStatus(ctx, ProcessLookupRequest{PersonalityAgentID: o.PersonalityAgentID, OperationID: o.OperationID})
			if err != nil {
				t.Fatal(err)
			}
			if current.State.terminal() {
				return current
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatal("process did not finish")
		return ProcessOperation{}
	}

	// The build/toolchain flow: write a C source and Makefile into the
	// scope, compile with the pinned job image's real toolchain, run the
	// binary — inside the hardened container, writing only to /workspace.
	buildScript := `set -e
test "$(id -u)" = 10002
printf 'int main(void){return 0;}\n' > main.c
printf 'all: main\nmain: main.c\n\t$(CC) -O2 -o main main.c\n' > Makefile
make
./main
test ! -e /workspace/b-secret.txt
printf '%s' "$JOB_GREETING" > greeting.txt
cat greeting.txt
printf 'build-done\n'`
	op, err := service.StartProcess(ctx, ProcessStartRequest{
		PersonalityAgentID:    paidA,
		OriginatingToolCallID: "job:e2e-build",
		Executable:            "/bin/sh",
		Args:                  []string{"-c", buildScript},
		TimeoutSeconds:        60,
		Env:                   map[string]string{"JOB_GREETING": "hello-from-job"},
		Image:                 "job",
		Workspace:             "files-scope",
	})
	if err != nil {
		t.Fatalf("files-scope launch: %v", err)
	}
	operations = append(operations, op)
	if op.WorkspaceBind != scopeA || op.FilesVolumeUUID != volumeUUID {
		t.Fatalf("op must journal the verified bind: %+v", op)
	}
	// The files binding record was persisted for the persona.
	if binding, bound := service.files.lookup(paidA); !bound || binding.VolumeUUID != volumeUUID {
		t.Fatalf("files binding must be recorded: %v %+v", bound, binding)
	}
	done := wait(op)
	if done.State != ProcessSucceeded {
		t.Fatalf("build job failed: %+v", done)
	}
	// Job-written files land in the canonical scope on the host — they
	// outlive the container by construction.
	for _, f := range []string{"main.c", "Makefile", "main", "greeting.txt"} {
		if _, err := os.Stat(filepath.Join(scopeA, f)); err != nil {
			t.Fatalf("job artifact %s missing from scope: %v", f, err)
		}
	}
	if raw, err := os.ReadFile(filepath.Join(scopeA, "greeting.txt")); err != nil || string(raw) != "hello-from-job" {
		t.Fatalf("env-bound file content: %q %v", raw, err)
	}
	// Container evidence: bind mount of the verified path, volume label.
	raw := docker("inspect", processContainer(done))
	var containers []struct {
		Config struct {
			Labels map[string]string
			Env    []string
		}
		Mounts []struct {
			Type, Source, Destination string
		}
	}
	if err := json.Unmarshal(raw, &containers); err != nil {
		t.Fatal(err)
	}
	c := containers[0]
	if c.Config.Labels["sumi.files_volume_uuid"] != volumeUUID {
		t.Fatalf("volume label missing: %+v", c.Config.Labels)
	}
	if len(c.Mounts) != 1 || c.Mounts[0].Type != "bind" || c.Mounts[0].Destination != "/workspace" {
		t.Fatalf("workspace must be a single bind mount: %+v", c.Mounts)
	}
	foundEnv := false
	for _, v := range c.Config.Env {
		if v == "JOB_GREETING=hello-from-job" {
			foundEnv = true
		}
	}
	if !foundEnv {
		t.Fatalf("bounded env must reach the container: %v", c.Config.Env)
	}

	// Environment reclamation: once the terminal state is committed the
	// container is removed; the scope files persist regardless.
	service.observeProcesses(ctx)
	raw = docker("container", "ls", "-a", "--filter", "name=^/"+processContainer(done)+"$", "--format", "{{.ID}}")
	if strings.TrimSpace(string(raw)) != "" {
		t.Fatal("terminal operation container must be removed after commit")
	}
	if _, err := os.Stat(filepath.Join(scopeA, "greeting.txt")); err != nil {
		t.Fatalf("files must survive environment reclamation: %v", err)
	}

	// Provisioner restart: the journal reloads, the op replays terminal,
	// and nothing relaunches.
	service = newService()
	again, err := service.StartProcess(ctx, ProcessStartRequest{
		PersonalityAgentID:    paidA,
		OriginatingToolCallID: "job:e2e-build",
		Executable:            "/bin/sh",
		Args:                  []string{"-c", buildScript},
		TimeoutSeconds:        60,
		Env:                   map[string]string{"JOB_GREETING": "hello-from-job"},
		Image:                 "job",
		Workspace:             "files-scope",
	})
	if err != nil || again.OperationID != op.OperationID || again.State != ProcessSucceeded {
		t.Fatalf("restart must replay the terminal op: %+v %v", again, err)
	}

	// Isolation + honest refusal: persona C has no scope directory; the
	// launch must fail as a workspace error, not fall back anywhere.
	if _, err := service.StartProcess(ctx, ProcessStartRequest{
		PersonalityAgentID:    newPA(),
		OriginatingToolCallID: "job:e2e-noscope",
		Executable:            "/bin/true",
		TimeoutSeconds:        30,
		Image:                 "job",
		Workspace:             "files-scope",
	}); err == nil || !strings.Contains(err.Error(), "workspace") {
		t.Fatalf("missing scope must refuse as workspace error: %v", err)
	}
	raw = docker("container", "ls", "-a", "--filter", "name=^/sumi-process-", "--format", "{{.Names}}")
	for _, name := range strings.Fields(string(raw)) {
		if strings.Contains(name, "e2e-noscope") {
			t.Fatalf("refused launch left a container: %s", name)
		}
	}

	// A persona-B op sees only B's scope (and its own marker, not A's).
	opB, err := service.StartProcess(ctx, ProcessStartRequest{
		PersonalityAgentID:    paidB,
		OriginatingToolCallID: "job:e2e-isolation",
		Executable:            "/bin/sh",
		Args: []string{"-c", `set -e
test -f /workspace/b-secret.txt
test ! -f /workspace/greeting.txt
printf b-done`},
		TimeoutSeconds: 30,
		Image:          "job",
		Workspace:      "files-scope",
	})
	if err != nil {
		t.Fatalf("B scope launch: %v", err)
	}
	operations = append(operations, opB)
	if done = wait(opB); done.State != ProcessSucceeded {
		t.Fatalf("isolation op failed: %+v", done)
	}
	out, err := service.ReadProcessOutput(ctx, ProcessOutputRequest{
		ProcessLookupRequest: ProcessLookupRequest{PersonalityAgentID: paidB, OperationID: opB.OperationID}, Stream: "stdout"})
	if err != nil || out.Content != "b-done" {
		t.Fatalf("B output: %q %v", out.Content, err)
	}
	t.Logf("verified files-scope launch: toolchain build, scope bind, isolation, env, restart replay, honest refusal, reclamation persistence")
}

// Launch-time re-verification: the workspace check runs again at the
// deferred container launch, not only at acceptance. A mount that "goes
// away" between accept and launch must hold the operation unlaunched —
// no container is created — and a mount that returns launches normally.
// Opt-in: SUMI_TEST_PROCESS_DOCKER=1 + SUMI_TEST_JOB_IMAGE_TAG.
func TestProcessLaunchRechecksWorkspace(t *testing.T) {
	if os.Getenv("SUMI_TEST_PROCESS_DOCKER") != "1" {
		t.Skip("requires explicit owned Docker integration opt-in")
	}
	jobTag := os.Getenv("SUMI_TEST_JOB_IMAGE_TAG")
	if !processImageTag.MatchString(jobTag) {
		t.Fatal("SUMI_TEST_JOB_IMAGE_TAG must name an existing full 40-character sumi-job revision")
	}
	ctx := context.Background()
	paid, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	paidS := paid.String()
	mount := t.TempDir()
	scopeName := strings.ToLower(strings.ReplaceAll(paidS, "-", ""))
	if err := os.MkdirAll(filepath.Join(mount, scopeName), 0777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(mount, scopeName), 0777); err != nil {
		t.Fatal(err)
	}
	// The check refuses while $mnt/.recheck-refuse exists — standing in
	// for a mount that died or was replaced since acceptance.
	checkPath := filepath.Join(mount, "fixture-files-check")
	checkScript := `#!/bin/sh
if [ "$1" = "--wait" ]; then shift 2; fi
mnt="$1"; uuid="$2"; scope="$3"
[ -d "$mnt" ] || { echo "refused: not_mounted: $mnt" >&2; exit 2; }
[ -f "$mnt/.recheck-refuse" ] && { echo "refused: mount_lost: $mnt" >&2; exit 2; }
dir="$mnt/$scope"
[ -d "$dir" ] || { echo "refused: scope_missing: $scope" >&2; exit 2; }
printf '%s\n' "$(cd "$dir" && pwd -P)"
`
	if err := os.WriteFile(checkPath, []byte(checkScript), 0755); err != nil {
		t.Fatal(err)
	}
	environment := []string{
		"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"),
		"SUMI_AGENT_IMAGE_TAG=" + jobTag, "SUMI_JOB_IMAGE_TAG=" + jobTag,
	}
	for _, key := range []string{"DOCKER_HOST", "DOCKER_CONFIG"} {
		if v := os.Getenv(key); v != "" {
			environment = append(environment, key+"="+v)
		}
	}
	backend := &DockerBackend{baseEnvironment: environment}
	service, err := NewService(backend, ServiceConfig{
		StateDirectory: t.TempDir() + "/state",
		Files: FilesEnvironment{
			Mountpoint: mount, VolumeUUID: "5a1b72bc-b9ec-4a72-813b-2f4dc4cd6d07",
			CheckPath: checkPath, CheckWaitSeconds: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	op, err := service.StartProcess(ctx, ProcessStartRequest{
		PersonalityAgentID:    paidS,
		OriginatingToolCallID: "job:e2e-recheck",
		Executable:            "/bin/sh",
		Args:                  []string{"-c", "printf recheck-ok"},
		TimeoutSeconds:        60,
		Image:                 "job",
		Workspace:             "files-scope",
	})
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	defer exec.Command("docker", "rm", "-f", processContainer(op)).Run()

	// The mount "dies" before the deferred launch.
	flag := filepath.Join(mount, ".recheck-refuse")
	if err := os.WriteFile(flag, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		service.observeProcesses(ctx)
		time.Sleep(100 * time.Millisecond)
	}
	current, err := service.ProcessStatus(ctx, ProcessLookupRequest{PersonalityAgentID: paidS, OperationID: op.OperationID})
	if err != nil {
		t.Fatal(err)
	}
	if current.State != ProcessAccepted {
		t.Fatalf("mount loss must hold the op unlaunched, got %s", current.State)
	}
	raw, _ := exec.Command("docker", "container", "ls", "-a",
		"--filter", "name=^/"+processContainer(op)+"$", "--format", "{{.ID}}").Output()
	if strings.TrimSpace(string(raw)) != "" {
		t.Fatal("no container may be created while the mount is unverified")
	}

	// The mount "returns": the next observe re-verifies and launches.
	if err := os.Remove(flag); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) && current.State != ProcessSucceeded {
		service.observeProcesses(ctx)
		current, err = service.ProcessStatus(ctx, ProcessLookupRequest{PersonalityAgentID: paidS, OperationID: op.OperationID})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(150 * time.Millisecond)
	}
	if current.State != ProcessSucceeded {
		t.Fatalf("recovered mount must launch and complete, got %s", current.State)
	}
}
