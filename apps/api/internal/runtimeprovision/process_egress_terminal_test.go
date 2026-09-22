package runtimeprovision

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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
// StartProcess, WriteProcessInput, the durable ttylog, SignalProcess,
// CancelProcess and a second session container. Everything below the
// session store is the real path; the store/ledger layer above the
// driver is exercised by termexec's own suites and is unchanged by this
// feature.
//
// Requires a job image containing sumi-egress-bridge + pip, tagged with
// the 40-hex revision in SUMI_TEST_DOCKER_JOB_TAG. Fixtures (proxy
// container, workspace volumes, op containers) carry
// label sumi.fixture=<label> and are swept in cleanup; the label
// defaults to sumi-network-terminal-20260922 and SUMI_TEST_EGRESS_LABEL
// overrides it.
//
// The test process must be able to read the daemon's json-file journals
// — run it where /var/lib/docker is visible (the production provisioner
// container mounts it; the review runner container does the same), and
// with host-pid visibility if SignalProcess assertions are exercised.

// egressTerminalFixtureLabel is this suite's default ownership label;
// SUMI_TEST_EGRESS_LABEL overrides it like the batch suite.
func egressTerminalFixtureLabel() string {
	if l := os.Getenv("SUMI_TEST_EGRESS_LABEL"); l != "" {
		return l
	}
	return "sumi-network-terminal-20260922"
}

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

// bridgeProbe emits a tagged liveness check against the in-container
// bridge listener — it reports the bridge's own liveness, not the
// proxy's (a down proxy is honest degradation, not bridge death). The
// echoed input line contains the tag text too, so callers must match
// the output line specifically: `^tag$` for up, `tag-DOWN` for down —
// via waitTTYLine, which fails on timeout.
func bridgeProbe(tag string) string {
	return `if (exec 3<>/dev/tcp/127.0.0.1/3128) 2>/dev/null; then echo ` + tag + `; exec 3>&- 3<&-; else echo ` + tag + `-DOWN; fi`
}

// waitTTYRe waits for transcript content matching want and fails the
// test on timeout. The plain waitTTY helpers return partial output
// silently on deadline — fine for diagnostics, but a gate that must
// prove a command RAN needs a hard wait.
func waitTTYRe(t *testing.T, tty *ttyLog, want *regexp.Regexp, d time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		data, _, _, _, err := tty.read(0, 1<<20)
		if err == nil && want.Match(data) {
			return string(data)
		}
		time.Sleep(200 * time.Millisecond)
	}
	data, _, _, _, _ := tty.read(0, 1<<20)
	t.Fatalf("tty never matched %s; transcript:\n%s", want, data)
	return ""
}

// waitTTYLine waits for an output line that is exactly `line`. PTY echo
// puts the marker text inside the echoed input line too — only a line
// whose content starts with the marker proves real command output. The
// tty stream separates output lines with bare \r as well as \r\n, and
// readline emits bracketed-paste escapes (\x1b[?2004l) glued to the
// front of the first output line — so anchor on ^ or \r and allow
// leading CSI sequences.
func waitTTYLine(t *testing.T, tty *ttyLog, line string, d time.Duration) string {
	t.Helper()
	return waitTTYRe(t, tty, regexp.MustCompile(`(?m)(?:^|\r)(?:\x1b\[[0-9;?]*[a-zA-Z])*`+regexp.QuoteMeta(line)+`\r?$`), d)
}

// The packet's terminal acceptance plus the review's recovery findings:
// a real shared-terminal PTY session installs a pinned public dependency
// with a normal pip command, idle/foreground Ctrl-C and SignalProcess
// never kill the detached bridge, a job-local loopback server stays
// reachable, a failed install leaves the shell usable, and a second real
// session reuses the installed dependency — CLI included.
func TestDockerJobEgressInteractiveTerminal(t *testing.T) {
	if os.Getenv("SUMI_TEST_DOCKER_E2E") != "1" {
		t.Skip("set SUMI_TEST_DOCKER_E2E=1")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain needed to build the fixture proxy")
	}
	egressDir := filepath.Join(t.TempDir(), "egress")
	startEgressProxyLabelled(t, egressDir, egressTerminalFixtureLabel())
	backend, tag := egressE2EBackend(t, egressDir)
	persona := uuid.NewString()
	seedEgressWorkspaceLabelled(t, persona, tag, egressTerminalFixtureLabel())
	service := egressService(t, backend)

	op, io_, write := terminalSession(t, service, persona, "sess-"+uuid.NewString()[:8])
	lookup := ProcessLookupRequest{PersonalityAgentID: persona, OperationID: op.OperationID}

	// The session environment: proxy env is live with the loopback-only
	// NO_PROXY bypass, the socket is mounted, and the container still has
	// no real NIC. The PTY starts at 80 columns — widen it and keep
	// markers short so line-discipline wraps cannot split a match.
	write("stty cols 200; echo PROXY=$HTTPS_PROXY NO=$NO_PROXY; test -S /run/sumi/egress/proxy.sock && echo SOCK-OK; awk 'NR>2{print $1}' /proc/net/dev\n")
	out := waitTTYLine(t, io_.tty, "SOCK-OK", 30*time.Second)
	if !strings.Contains(out, "PROXY=http://127.0.0.1:3128") {
		t.Fatalf("proxy env absent in terminal session:\n%s", out)
	}
	if !strings.Contains(out, "NO=localhost,127.0.0.1,::1") {
		t.Fatalf("loopback NO_PROXY bypass absent in terminal session:\n%s", out)
	}
	if strings.Contains(out, "eth0") {
		t.Fatalf("terminal session unexpectedly has a real NIC:\n%s", out)
	}

	// f415: the bridge runs detached — own session, own process group,
	// unreachable by foreground-group signals. Prove the process state
	// first (comm is truncated to 15 chars), then the actual signals.
	write("b=; for p in /proc/[0-9]*/comm; do read c < $p; case $c in sumi-egress-bri*) b=${p#/proc/}; b=${b%/comm};; esac; done; awk '{print \"BRIDGE-PGRP-\"$5\"-SID-\"$6}' /proc/$b/stat\n")
	out = waitTTYRe(t, io_.tty, regexp.MustCompile(`BRIDGE-PGRP-(\d+)-SID-(\d+)`), 30*time.Second)
	m := regexp.MustCompile(`BRIDGE-PGRP-(\d+)-SID-(\d+)`).FindStringSubmatch(out)
	if m == nil || m[1] != m[2] {
		t.Fatalf("bridge is not a detached session leader:\n%s", out)
	}
	// Ordinary idle-prompt Ctrl-C: the line discipline signals the
	// foreground group (the shell's) — the bridge must survive. Bash
	// honestly reports 130 for the interrupted empty command line.
	write("\x03")
	time.Sleep(500 * time.Millisecond)
	write("echo IDLE-INT-$?; " + bridgeProbe("BR1") + "\n")
	out = waitTTYLine(t, io_.tty, "IDLE-INT-130", 30*time.Second)
	if out := waitTTYLine(t, io_.tty, "BR1", 30*time.Second); !strings.Contains(out, "IDLE-INT-130") {
		t.Fatalf("idle Ctrl-C killed the bridge or the shell:\n%s", out)
	}
	// Foreground command interruption: ^C must still kill the running
	// command (130 = 128+SIGINT) — not be swallowed — while the bridge
	// survives.
	write("sleep 60\n")
	time.Sleep(1200 * time.Millisecond)
	write("\x03")
	time.Sleep(500 * time.Millisecond)
	write("echo FG-$?; " + bridgeProbe("BR2") + "\n")
	out = waitTTYLine(t, io_.tty, "FG-130", 30*time.Second)
	if out := waitTTYLine(t, io_.tty, "BR2", 30*time.Second); !strings.Contains(out, "FG-130") {
		t.Fatalf("foreground Ctrl-C did not interrupt sleep or killed bridge:\n%s", out)
	}
	// The SignalProcess group path — same delivery the session driver
	// uses for a keystroke signal — at an idle prompt.
	if _, err := service.SignalProcess(context.Background(), ProcessSignalRequest{ProcessLookupRequest: lookup, Signal: "INT"}); err != nil {
		t.Fatalf("SignalProcess: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	write("echo SIG-OK; " + bridgeProbe("BR3") + "\n")
	out = waitTTYLine(t, io_.tty, "SIG-OK", 30*time.Second)
	if out := waitTTYLine(t, io_.tty, "BR3", 30*time.Second); !strings.Contains(out, "SIG-OK") {
		t.Fatalf("SignalProcess killed the bridge or the shell:\n%s", out)
	}

	// f416: a job-local loopback server answers curl with no special
	// flags — the NO_PROXY bypass — while public egress stays proxied.
	write("echo keep > /workspace/term-marker\n")
	write("(python3 -m http.server 8901 --bind 127.0.0.1 >/dev/null 2>&1 &) ; sleep 0.5; curl -s --max-time 5 http://127.0.0.1:8901/term-marker; echo LOOP-$?\n")
	out = waitTTYLine(t, io_.tty, "LOOP-0", 30*time.Second)
	if !strings.Contains(out, "keep") {
		t.Fatalf("job-local loopback server unreachable through normal tools:\n%s", out)
	}

	// A normal pip command typed into the real PTY installs the pinned
	// public packages into the canonical workspace (HOME=/workspace) —
	// one library and one console script.
	write("pip install --user six==1.16.0 chardet==5.2.0 && echo PIP-DONE-$?\n")
	if out := waitTTYLine(t, io_.tty, "PIP-DONE-0", 120*time.Second); !strings.Contains(out, "PIP-DONE-0") {
		t.Fatalf("pip install did not succeed in the terminal:\n%s", out)
	}
	write("python3 -c 'import six; print(\"SIX-\"+six.__version__)'; chardetect --version && echo CLI-OK\n")
	if out := waitTTYLine(t, io_.tty, "SIX-1.16.0", 30*time.Second); !strings.Contains(out, "SIX-1.16.0") {
		t.Fatalf("installed package not importable in the installing session:\n%s", out)
	}
	// f419: the console script is runnable BY NAME in this installing
	// login shell — on a fresh workspace .local/bin did not exist at
	// login, so this is the exact gap review A reproduced. CLI-OK must be
	// a real output line (waitTTYLine), and the version line is checked
	// as output too — echoed input contains neither `chardetect 5.2.0`.
	if out := waitTTYLine(t, io_.tty, "CLI-OK", 30*time.Second); !strings.Contains(out, "chardetect 5.2.0") {
		t.Fatalf("installed console script not on PATH:\n%s", out)
	}

	// A private destination is denied through the same session.
	write("curl -sS --max-time 10 http://169.254.169.254/latest/meta-data; echo DENY-RC=$?\n")
	// The denial body text is output-only — an echoed input line cannot
	// contain it — so it is a strict gate on the refusal arriving.
	if out := waitTTYRe(t, io_.tty, regexp.MustCompile(`sumi-egress: destination`), 30*time.Second); !strings.Contains(out, "DENY-RC=0") {
		t.Fatalf("private destination not denied in the terminal:\n%s", out)
	}

	// A failed install must leave this shell and the workspace usable.
	write("pip install --user definitely-not-a-real-package-sumiterm==9.9.9\n")
	if out := waitTTYRe(t, io_.tty, regexp.MustCompile(`No matching distribution`), 120*time.Second); !strings.Contains(out, "No matching distribution") {
		t.Fatalf("bogus install did not report its failure:\n%s", out)
	}
	write("echo SHELL-ALIVE; cat /workspace/term-marker\n")
	if out := waitTTYLine(t, io_.tty, "SHELL-ALIVE", 15*time.Second); !strings.Contains(out, "keep") {
		t.Fatalf("shell unusable or workspace damaged after failed install:\n%s", out)
	}

	// The environment ends — the session op is cancelled and its
	// container physically removed, bridge with it.
	if _, err := service.CancelProcess(context.Background(), lookup); err != nil {
		t.Fatalf("cancel session: %v", err)
	}
	got := waitQuiesced(t, service, op, 60*time.Second)
	if got.State != ProcessCancelled {
		t.Fatalf("session op did not end cancelled: %s", got.State)
	}
	c := exec.Command("docker", "inspect", "--format", "{{.State.Running}}", "sumi-process-"+op.OperationID)
	c.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "DOCKER_HOST=unix:///var/run/docker.sock"}
	if out, err := c.CombinedOutput(); err == nil {
		t.Fatalf("session container still exists after quiesce: %s", out)
	}

	// A subsequent real terminal session — new container on the same
	// canonical workspace — reuses the installed dependency AND its CLI.
	op2, io2, write2 := terminalSession(t, service, persona, "sess-"+uuid.NewString()[:8])
	write2("stty cols 200; python3 -c 'import six; print(\"REUSED-\"+six.__version__)'; chardetect --version && echo CLI2-OK; cat /workspace/term-marker\n")
	if out := waitTTYLine(t, io2.tty, "CLI2-OK", 30*time.Second); !strings.Contains(out, "REUSED-1.16.0") || !strings.Contains(out, "chardetect 5.2.0") || !strings.Contains(out, "keep") {
		t.Fatalf("second terminal did not reuse the workspace install:\n%s", out)
	}
	if _, err := service.CancelProcess(context.Background(), ProcessLookupRequest{
		PersonalityAgentID: persona, OperationID: op2.OperationID,
	}); err != nil {
		t.Fatalf("cancel second session: %v", err)
	}
	waitQuiesced(t, service, op2, 60*time.Second)
}
