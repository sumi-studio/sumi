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

// Real-Docker interactive-terminal egress e2e, gated on
// SUMI_TEST_DOCKER_E2E=1. Closes the PTY evidence gap: the batch e2e
// proves job egress; this test drives the same operation shape the
// termexec driver launches for a shared Cloud terminal — Interactive +
// TTY + Image "job" + Executable /bin/bash -l — through Service.
// StartProcess, WriteProcessInput, the durable ttylog, CancelProcess and
// a second session container. Everything below the session store is the
// real path; the store/ledger layer above the driver is exercised by
// termexec's own suites and is unchanged by this feature.
//
// Requires a job image containing sumi-egress-bridge + pip, tagged with
// the 40-hex revision in SUMI_TEST_DOCKER_JOB_TAG. Fixtures (proxy
// container, workspace volumes, op containers) carry
// label sumi.fixture=sumi-network-terminal-20260922 and are swept in
// cleanup.

const egressTerminalFixtureLabel = "sumi-network-terminal-20260922"

// terminalSession starts one interactive op with exactly the request
// shape termexec.operationRequest builds for a claimed session
// (driver.go): bash -l on a TTY, image "job", the session-derived call id
// the provisioner turns into the operation id.
func terminalSession(t *testing.T, s *Service, persona, sessionID string) (ProcessOperation, *interactiveIO, func(string)) {
	t.Helper()
	op, err := s.StartProcess(context.Background(), ProcessStartRequest{
		PersonalityAgentID:    persona,
		OriginatingToolCallID: "term:" + sessionID,
		Executable:            "/bin/bash",
		Args:                  []string{"-l"},
		Cwd:                   ".",
		TimeoutSeconds:        600,
		Image:                 "job",
		Interactive:           true,
		TTY:                   true,
	})
	if err != nil {
		t.Fatalf("start terminal session: %v", err)
	}
	container := "sumi-process-" + op.OperationID
	t.Cleanup(func() {
		c := exec.Command("docker", "rm", "-f", container)
		c.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "DOCKER_HOST=unix:///var/run/docker.sock"}
		_ = c.Run()
	})
	// Wait for the observer to create+start the session container.
	running := false
	for i := 0; i < 100 && !running; i++ {
		c := exec.Command("docker", "inspect", "--format", "{{.State.Running}}", container)
		c.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "DOCKER_HOST=unix:///var/run/docker.sock"}
		out, err := c.Output()
		running = err == nil && strings.TrimSpace(string(out)) == "true"
		time.Sleep(300 * time.Millisecond)
	}
	if !running {
		t.Fatalf("session container %s never started", container)
	}
	lookup := ProcessLookupRequest{PersonalityAgentID: persona, OperationID: op.OperationID}
	write := func(data string) {
		t.Helper()
		rc, err := s.WriteProcessInput(context.Background(), ProcessInputRequest{ProcessLookupRequest: lookup, Data: []byte(data)})
		if err != nil || !rc.Delivered {
			t.Fatalf("write %q: receipt=%+v err=%v", data, rc, err)
		}
	}
	write("\n")
	var io_ *interactiveIO
	for i := 0; i < 50 && io_ == nil; i++ {
		io_ = s.processes.interactiveIOFor(op.OperationID)
		time.Sleep(200 * time.Millisecond)
	}
	if io_ == nil {
		t.Fatal("interactive supervisor never attached")
	}
	return op, io_, write
}

// The packet's terminal acceptance: a real shared-terminal PTY session
// installs a pinned public dependency with a normal pip command, a failed
// install leaves the shell usable, and a second real session reuses the
// installed dependency from the same persona workspace volume.
func TestDockerJobEgressInteractiveTerminal(t *testing.T) {
	if os.Getenv("SUMI_TEST_DOCKER_E2E") != "1" {
		t.Skip("set SUMI_TEST_DOCKER_E2E=1")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain needed to build the fixture proxy")
	}
	egressDir := filepath.Join(t.TempDir(), "egress")
	startEgressProxyLabelled(t, egressDir, egressTerminalFixtureLabel)
	backend, tag := egressE2EBackend(t, egressDir)
	persona := uuid.NewString()
	seedEgressWorkspaceLabelled(t, persona, tag, egressTerminalFixtureLabel)
	service := egressService(t, backend)

	op, io_, write := terminalSession(t, service, persona, "sess-"+uuid.NewString()[:8])

	// The session environment: proxy env is live, the socket is mounted,
	// and the container still has no real NIC. The PTY starts at 80
	// columns — widen it and keep markers short so line discipline wraps
	// cannot split a match.
	write("stty cols 200; echo PROXY=$HTTPS_PROXY; test -S /run/sumi/egress/proxy.sock && echo SOCK-OK; awk 'NR>2{print $1}' /proc/net/dev\n")
	out := waitTTY(t, io_.tty, "SOCK-OK", 30*time.Second)
	if !strings.Contains(out, "PROXY=http://127.0.0.1:3128") {
		t.Fatalf("proxy env absent in terminal session:\n%s", out)
	}
	if strings.Contains(out, "eth0") {
		t.Fatalf("terminal session unexpectedly has a real NIC:\n%s", out)
	}

	// A normal pip command typed into the real PTY installs the pinned
	// public package into the persona workspace volume (HOME=/workspace).
	write("pip install --user six==1.16.0 && echo PIP-DONE-$?\n")
	if out := waitTTY(t, io_.tty, "PIP-DONE-0", 120*time.Second); !strings.Contains(out, "PIP-DONE-0") {
		t.Fatalf("pip install did not succeed in the terminal:\n%s", out)
	}
	write("python3 -c 'import six; print(\"SIX-\"+six.__version__)'\n")
	if out := waitTTY(t, io_.tty, "SIX-1.16.0", 30*time.Second); !strings.Contains(out, "SIX-1.16.0") {
		t.Fatalf("installed package not importable in the installing session:\n%s", out)
	}
	// Workspace file for the post-failure and reuse checks.
	write("echo keep > /workspace/term-marker; echo MARKER-$?\n")
	if out := waitTTY(t, io_.tty, "MARKER-0", 15*time.Second); !strings.Contains(out, "MARKER-0") {
		t.Fatalf("workspace write failed:\n%s", out)
	}

	// A private destination is denied through the same session.
	write("curl -sS --max-time 10 http://169.254.169.254/latest/meta-data; echo DENY-RC=$?\n")
	if out := waitTTY(t, io_.tty, "DENY-RC=", 30*time.Second); !strings.Contains(out, "sumi-egress") {
		t.Fatalf("private destination not denied in the terminal:\n%s", out)
	}

	// A failed install must leave this shell and the workspace usable.
	write("pip install --user definitely-not-a-real-package-sumiterm==9.9.9\n")
	if out := waitTTY(t, io_.tty, "No matching distribution", 120*time.Second); !strings.Contains(out, "No matching distribution") {
		t.Fatalf("bogus install did not report its failure:\n%s", out)
	}
	write("echo SHELL-ALIVE-$?; cat /workspace/term-marker\n")
	if out := waitTTY(t, io_.tty, "SHELL-ALIVE-0", 15*time.Second); !strings.Contains(out, "keep") {
		t.Fatalf("shell unusable or workspace damaged after failed install:\n%s", out)
	}

	// The environment ends — the session op is cancelled and its
	// container physically removed.
	if _, err := service.CancelProcess(context.Background(), ProcessLookupRequest{
		PersonalityAgentID: persona, OperationID: op.OperationID,
	}); err != nil {
		t.Fatalf("cancel session: %v", err)
	}
	got := waitQuiesced(t, service, op, 60*time.Second)
	if got.State != ProcessCancelled {
		t.Fatalf("session op did not end cancelled: %s", got.State)
	}

	// A subsequent real terminal session — new container on the same
	// persona workspace volume — reuses the installed dependency.
	op2, io2, write2 := terminalSession(t, service, persona, "sess-"+uuid.NewString()[:8])
	write2("python3 -c 'import six; print(\"REUSED-\"+six.__version__)'; cat /workspace/term-marker\n")
	if out := waitTTY(t, io2.tty, "REUSED-1.16.0", 30*time.Second); !strings.Contains(out, "REUSED-1.16.0") || !strings.Contains(out, "keep") {
		t.Fatalf("second terminal did not reuse the workspace install:\n%s", out)
	}
	if _, err := service.CancelProcess(context.Background(), ProcessLookupRequest{
		PersonalityAgentID: persona, OperationID: op2.OperationID,
	}); err != nil {
		t.Fatalf("cancel second session: %v", err)
	}
	waitQuiesced(t, service, op2, 60*time.Second)
}
