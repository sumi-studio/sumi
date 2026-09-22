package runtimeprovision

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Real-Docker end-to-end for job egress, gated on SUMI_TEST_DOCKER_E2E=1.
// Requires a job image containing sumi-egress-bridge + pip, tagged with the
// 40-hex revision in SUMI_TEST_DOCKER_JOB_TAG. Creates only owned,
// fixture-labelled resources: one workspace volume per generated persona,
// an owned egress socket dir under the test's temp dir, and the op
// containers (auto-removed by the reconcile loop; swept in cleanup too).
//
// The egress proxy runs as a plain local process on the test's socket —
// the same binary and protocol the job-egress-proxy container serves.

const egressFixtureLabel = "sumi-netdeps-20260922"

func egressE2EBackend(t *testing.T, egressDir string) (*DockerBackend, string) {
	t.Helper()
	tag := os.Getenv("SUMI_TEST_DOCKER_JOB_TAG")
	if tag == "" {
		t.Fatal("SUMI_TEST_DOCKER_JOB_TAG must name a locally built 40-hex job image tag")
	}
	return dockerE2EBackend(t, "SUMI_JOB_EGRESS_DIR="+egressDir)
}

// startEgressProxy compiles sumi-egress-proxy and serves it on a unix
// socket inside dir from an owned fixture container — the same binary and
// protocol the job-egress-proxy deployment serves, run with real network
// so DNS and public dials resolve exactly as they do in production.
func startEgressProxy(t *testing.T, dir string) (socketPath string) {
	t.Helper()
	return startEgressProxyLabelled(t, dir, egressFixtureLabel)
}

func startEgressProxyLabelled(t *testing.T, dir, label string) (socketPath string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	binDir := t.TempDir()
	binary := filepath.Join(binDir, "sumi-egress-proxy")
	build := exec.Command("go", "build", "-buildvcs=false", "-o", binary, "github.com/sumi-studio/sumi/apps/api/cmd/egress-proxy")
	build.Dir = filepath.Dir(filepath.Dir(wd(t))) // apps/api
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build egress proxy: %v\n%s", err, out)
	}
	name := label + "-proxy-" + uuid.NewString()[:8]
	cmd := exec.Command("docker", "run", "-d",
		"--name", name,
		"--label", "sumi.fixture="+label,
		"--label", "sumi.fixture.role=egress-proxy",
		"--mount", "type=bind,src="+binary+",dst=/usr/local/bin/sumi-egress-proxy,ro",
		"--mount", "type=bind,src="+dir+",dst=/sock",
		"--entrypoint", "/usr/local/bin/sumi-egress-proxy",
		"debian:bookworm-slim",
		"-socket", "/sock/proxy.sock", "-socket-mode", "0622")
	cmd.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "DOCKER_HOST=unix:///var/run/docker.sock"}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("start proxy container: %v\n%s", err, out)
	}
	logPath := filepath.Join(dir, "proxy.log")
	t.Cleanup(func() {
		logs := exec.Command("docker", "logs", name)
		logs.Env = cmd.Env
		if out, err := logs.CombinedOutput(); err == nil {
			_ = os.WriteFile(logPath, out, 0o644)
			if keep := os.Getenv("SUMI_TEST_EGRESS_LOG_DIR"); keep != "" {
				_ = os.WriteFile(filepath.Join(keep, name+".log"), out, 0o644)
			}
		}
		rm := exec.Command("docker", "rm", "-f", name)
		rm.Env = cmd.Env
		_ = rm.Run()
	})
	socketPath = filepath.Join(dir, "proxy.sock")
	waitFile(t, socketPath, 15*time.Second)
	t.Logf("egress proxy container %s; log: %s", name, logPath)
	return socketPath
}

func wd(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func egressService(t *testing.T, backend *DockerBackend) *Service {
	t.Helper()
	ps, err := newProcessStore(t.TempDir()+"/state", backend)
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{backend: backend, processes: ps}
	ctx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	go service.RunProcessObserver(ctx)
	return service
}

func egressOp(t *testing.T, s *Service, persona, call, script string) ProcessOperation {
	t.Helper()
	op, err := s.StartProcess(context.Background(), ProcessStartRequest{
		PersonalityAgentID:    persona,
		OriginatingToolCallID: call,
		Executable:            "/bin/bash",
		Args:                  []string{"-c", script},
		Image:                 "job",
		TimeoutSeconds:        300,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c := exec.Command("docker", "rm", "-f", "sumi-process-"+op.OperationID)
		c.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "DOCKER_HOST=unix:///var/run/docker.sock"}
		_ = c.Run()
	})
	return op
}

func egressOutput(t *testing.T, s *Service, op ProcessOperation, stream string) string {
	t.Helper()
	out, err := s.ReadProcessOutput(context.Background(), ProcessOutputRequest{
		ProcessLookupRequest: ProcessLookupRequest{PersonalityAgentID: op.PersonalityAgentID, OperationID: op.OperationID},
		Stream:               stream,
		Limit:                64 << 10,
	})
	if err != nil {
		t.Fatalf("read %s: %v", stream, err)
	}
	return out.Content
}

func seedEgressWorkspace(t *testing.T, persona, tag string) string {
	t.Helper()
	return seedEgressWorkspaceLabelled(t, persona, tag, egressFixtureLabel)
}

func seedEgressWorkspaceLabelled(t *testing.T, persona, tag, label string) string {
	t.Helper()
	agent32 := strings.ReplaceAll(persona, "-", "")
	volume := "sumi-" + agent32 + "_workspace"
	project := "sumi-" + agent32
	docker := func(args ...string) {
		t.Helper()
		cmd := exec.Command("docker", args...)
		cmd.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "DOCKER_HOST=unix:///var/run/docker.sock"}
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
	docker("volume", "create",
		"--label", "com.docker.compose.project="+project,
		"--label", "com.docker.compose.volume=workspace",
		"--label", "sumi.fixture="+label,
		volume)
	t.Cleanup(func() {
		c := exec.Command("docker", "volume", "rm", "-f", volume)
		c.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "DOCKER_HOST=unix:///var/run/docker.sock"}
		_ = c.Run()
	})
	// Volume roots are root-owned; the job uid must own its workspace.
	docker("run", "--rm", "--network", "none", "--user", "0:0",
		"--mount", "type=volume,src="+volume+",dst=/workspace",
		"--entrypoint", "/bin/chown", "ghcr.io/sumi-studio/sumi-job:"+tag, "10002:10002", "/workspace")
	return volume
}

// The packet's real acceptance: install a pinned public dependency through
// the actual launch path, end the environment, reuse the installed
// dependency from a fresh container on the same workspace.
func TestDockerJobEgressEndToEnd(t *testing.T) {
	if os.Getenv("SUMI_TEST_DOCKER_E2E") != "1" {
		t.Skip("set SUMI_TEST_DOCKER_E2E=1")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain needed to build the fixture proxy")
	}
	egressDir := filepath.Join(t.TempDir(), "egress")
	startEgressProxy(t, egressDir)
	backend, tag := egressE2EBackend(t, egressDir)
	persona := uuid.NewString()
	seedEgressWorkspace(t, persona, tag)
	service := egressService(t, backend)

	// op1: sanity — the mount, the env, and the absence of a real NIC.
	op := egressOp(t, service, persona, "egress-sanity",
		`env | grep -i proxy; ls -la /run/sumi/egress; ip link 2>/dev/null || cat /proc/net/dev; printf seed > /workspace/marker.txt`)
	got := waitQuiesced(t, service, op, 60*time.Second)
	stdout := egressOutput(t, service, op, "stdout")
	if got.State != ProcessSucceeded {
		t.Fatalf("sanity op failed: %s stderr=%s", got.State, egressOutput(t, service, op, "stderr"))
	}
	if !strings.Contains(stdout, "HTTP_PROXY=http://127.0.0.1:3128") || !strings.Contains(stdout, "proxy.sock") {
		t.Fatalf("egress wiring absent in job:\n%s", stdout)
	}
	if strings.Contains(stdout, "eth0") {
		t.Fatalf("job unexpectedly has a real NIC:\n%s", stdout)
	}
	// A proxy-bypassed fetch must fail — no direct egress exists.
	op = egressOp(t, service, persona, "egress-no-direct",
		`curl --noproxy '*' --max-time 8 -sS -o /dev/null https://pypi.org/ 2>&1; echo "direct-exit=$?"`)
	got = waitQuiesced(t, service, op, 60*time.Second)
	if got.State != ProcessSucceeded {
		t.Fatalf("no-direct op failed: %s", got.State)
	}
	if out := egressOutput(t, service, op, "stdout"); !strings.Contains(out, "direct-exit=") || strings.Contains(out, "direct-exit=0") {
		t.Fatalf("direct fetch unexpectedly succeeded or vanished:\n%s", out)
	}

	// op2: real pinned public install into the canonical workspace.
	op = egressOp(t, service, persona, "egress-install",
		`python3 -m pip install --user --quiet six==1.16.0 && python3 -c 'import six; print("six", six.__version__)'`)
	got = waitQuiesced(t, service, op, 120*time.Second)
	if got.State != ProcessSucceeded {
		t.Fatalf("pip install failed: state=%s\nstdout=%s\nstderr=%s", got.State,
			egressOutput(t, service, op, "stdout"), egressOutput(t, service, op, "stderr"))
	}
	if out := egressOutput(t, service, op, "stdout"); !strings.Contains(out, "six 1.16.0") {
		t.Fatalf("installed package did not run in the installing job:\n%s", out)
	}
	t.Logf("install op stdout: %s", egressOutput(t, service, op, "stdout"))

	// op3: environment ends and a fresh container is created — the
	// installed dependency persists through the workspace alone.
	op = egressOp(t, service, persona, "egress-reuse",
		`python3 -c 'import six; print("reused", six.__version__)'; test "$(cat /workspace/marker.txt)" = seed && echo marker-intact; git ls-remote https://github.com/octocat/Hello-World HEAD`)
	got = waitQuiesced(t, service, op, 90*time.Second)
	if out := egressOutput(t, service, op, "stdout"); got.State != ProcessSucceeded || !strings.Contains(out, "reused 1.16.0") || !strings.Contains(out, "marker-intact") || !strings.Contains(out, "HEAD") {
		t.Fatalf("fresh container did not reuse workspace install: %s\n%s", got.State, out)
	}
	t.Logf("reuse op stdout: %s", egressOutput(t, service, op, "stdout"))

	// op4: private/unauthorized destination is denied by the proxy —
	// link-local metadata over plain HTTP gets a 403 body; a private
	// CONNECT gets a 403 proxy response; a non-80 http port refuses too.
	op = egressOp(t, service, persona, "egress-deny",
		`echo "--- metadata"; curl -sS --max-time 10 http://169.254.169.254/latest/meta-data; echo; echo "--- private-https"; curl -sv --max-time 10 https://10.255.255.1/ 2>&1 | grep -Ei '403|egress' | head -3; echo "--- bad-port"; curl -sS --max-time 10 http://neverssl.invalid:8080/`+"\n"+`echo done`)
	got = waitQuiesced(t, service, op, 90*time.Second)
	out := egressOutput(t, service, op, "stdout")
	if !strings.Contains(out, "sumi-egress") || !strings.Contains(out, "403") {
		t.Fatalf("private destination was not denied by the proxy:\n%s\nstderr=%s", out, egressOutput(t, service, op, "stderr"))
	}
	t.Logf("deny op stdout: %s", out)

	// op5: a failing install must not damage the terminal/workspace —
	// then an unrelated command still succeeds with earlier files intact.
	op = egressOp(t, service, persona, "egress-fail-install",
		`python3 -m pip install --user --quiet definitely-not-a-real-package-suminetdeps==9.9.9`)
	got = waitQuiesced(t, service, op, 120*time.Second)
	if got.State != ProcessFailed {
		t.Fatalf("bogus install did not fail: %s", got.State)
	}
	op = egressOp(t, service, persona, "egress-after-fail",
		`cat /workspace/marker.txt; python3 -c 'import six; print("still", six.__version__)'`)
	got = waitQuiesced(t, service, op, 60*time.Second)
	out = egressOutput(t, service, op, "stdout")
	if got.State != ProcessSucceeded || !strings.Contains(out, "seed") || !strings.Contains(out, "still 1.16.0") {
		t.Fatalf("workspace damaged by failed install: %s\n%s", got.State, out)
	}
	t.Logf("post-failure op stdout: %s", out)
	t.Logf("egress proxy log kept at %s", filepath.Join(egressDir, "proxy.log"))
}

// The egress socket directory is shared infrastructure: every job gets the
// same mount. This proves the mount does not widen workspace access —
// persona B's job still sees only persona B's volume.
func TestDockerJobEgressForeignWorkspace(t *testing.T) {
	if os.Getenv("SUMI_TEST_DOCKER_E2E") != "1" {
		t.Skip("set SUMI_TEST_DOCKER_E2E=1")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain needed to build the fixture proxy")
	}
	egressDir := filepath.Join(t.TempDir(), "egress")
	startEgressProxy(t, egressDir)
	backend, tag := egressE2EBackend(t, egressDir)
	personaA, personaB := uuid.NewString(), uuid.NewString()
	seedEgressWorkspace(t, personaA, tag)
	seedEgressWorkspace(t, personaB, tag)
	service := egressService(t, backend)

	opA := egressOp(t, service, personaA, "a-seed", `printf secret-a > /workspace/only-a.txt; echo done`)
	if got := waitQuiesced(t, service, opA, 60*time.Second); got.State != ProcessSucceeded {
		t.Fatalf("seed failed: %s", got.State)
	}
	opB := egressOp(t, service, personaB, "b-scan", `ls -la /workspace; ls /run/sumi/egress`)
	got := waitQuiesced(t, service, opB, 60*time.Second)
	out := egressOutput(t, service, opB, "stdout")
	if got.State != ProcessSucceeded || strings.Contains(out, "only-a.txt") {
		t.Fatalf("persona B observed persona A's workspace: %s\n%s", got.State, out)
	}
	if !strings.Contains(out, "proxy.sock") {
		t.Fatalf("egress socket absent in B's job:\n%s", out)
	}
}
