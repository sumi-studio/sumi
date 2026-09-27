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

// Real-Docker first-open experience of a shared Cloud terminal, gated on
// SUMI_TEST_DOCKER_E2E=1 like the egress terminal suite (same fixture
// requirements). Hosted acceptance (2026-09-27) found a fresh terminal
// showing only the egress bridge's startup log line and no prompt until
// the person typed something. The daemon's json-file log copier frames
// output on '\n', so a prompt — or any keystroke echo — without a newline
// never reached the journal the pump reads. This test never sends input
// before asserting the prompt, and reads through ReadProcessOutput with a
// moving cursor exactly like the termexec session pump.
func TestDockerInteractiveTerminalFirstPromptWithoutInput(t *testing.T) {
	if os.Getenv("SUMI_TEST_DOCKER_E2E") != "1" {
		t.Skip("set SUMI_TEST_DOCKER_E2E=1")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain needed to build the fixture proxy")
	}
	label := egressTerminalFixtureLabel()
	egressDir := filepath.Join(t.TempDir(), "egress")
	startEgressProxyLabelled(t, egressDir, label)
	backend, tag := egressE2EBackend(t, egressDir)
	persona := uuid.NewString()
	seedEgressWorkspaceLabelled(t, persona, tag, label)
	service := egressService(t, backend)

	started := time.Now()
	op, err := service.StartProcess(context.Background(), ProcessStartRequest{
		PersonalityAgentID:    persona,
		OriginatingToolCallID: "term:sess-" + uuid.NewString()[:8],
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
	t.Cleanup(func() {
		c := exec.Command("docker", "rm", "-f", "sumi-process-"+op.OperationID)
		c.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "DOCKER_HOST=unix:///var/run/docker.sock"}
		_ = c.Run()
	})
	lookup := ProcessLookupRequest{PersonalityAgentID: persona, OperationID: op.OperationID}

	// The session pump's view: a cursor advanced by NextOffset.
	var cursor int64
	var screen strings.Builder
	pull := func() {
		out, err := service.ReadProcessOutput(context.Background(), ProcessOutputRequest{ProcessLookupRequest: lookup, Stream: "stdout", Offset: cursor, Limit: 64 << 10})
		if err != nil {
			return
		}
		if out.Offset == cursor && out.NextOffset >= cursor {
			screen.WriteString(out.Content)
			cursor = out.NextOffset
		}
	}
	waitScreen := func(want *regexp.Regexp, d time.Duration) bool {
		deadline := time.Now().Add(d)
		for time.Now().Before(deadline) {
			pull()
			if want.MatchString(screen.String()) {
				return true
			}
			time.Sleep(100 * time.Millisecond)
		}
		return false
	}

	// 1. No input at all: the actual interactive prompt must render.
	prompt := regexp.MustCompile(`sumi-executor@[0-9a-f]+:~\$ $`)
	if !waitScreen(prompt, 20*time.Second) {
		t.Fatalf("no shell prompt without input after %s; screen so far: %q", time.Since(started).Round(time.Millisecond), screen.String())
	}
	t.Logf("prompt rendered %s after StartProcess without input", time.Since(started).Round(time.Millisecond))
	// 2. Internal egress-bridge diagnostics are not in the person's shell.
	if strings.Contains(screen.String(), "egress bridge") {
		t.Fatalf("bridge diagnostics printed into the terminal: %q", screen.String())
	}
	// 3. Output health reads attached once the prompt is visible.
	if st, err := service.ProcessStatus(context.Background(), lookup); err != nil || !st.OutputAttached {
		t.Fatalf("output_attached=%v err=%v after the prompt rendered", st.OutputAttached, err)
	}

	// An output line (not the echoed input line): starts at ^ or after a
	// bare \r, possibly behind readline's bracketed-paste CSI.
	outLine := func(line string) *regexp.Regexp {
		return regexp.MustCompile(`(?m)(?:^|\r)(?:\x1b\[[0-9;?]*[a-zA-Z])*` + regexp.QuoteMeta(line) + `\r?$`)
	}
	write := func(data string) {
		t.Helper()
		rc, err := service.WriteProcessInput(context.Background(), ProcessInputRequest{ProcessLookupRequest: lookup, Data: []byte(data)})
		if err != nil || !rc.Delivered {
			t.Fatalf("write %q: receipt=%+v err=%v", data, rc, err)
		}
	}
	// 4. Keystroke echo is visible before Enter (readline echoes each
	// character without a newline).
	write("echo TYPED")
	if !waitScreen(regexp.MustCompile(`\$ echo TYPED$`), 5*time.Second) {
		t.Fatalf("typed characters not echoed before Enter; screen: %q", screen.String())
	}
	// 5. The command runs; login PATH and the egress env are intact.
	write("-$((40+2)); echo \"P=$PATH\"; echo \"X=$HTTPS_PROXY\"\n")
	if !waitScreen(outLine("TYPED-42"), 10*time.Second) ||
		!waitScreen(regexp.MustCompile(`P=/workspace/\.local/bin:`), 5*time.Second) ||
		!waitScreen(regexp.MustCompile(`X=http://127\.0\.0\.1:3128`), 5*time.Second) {
		t.Fatalf("command output missing; screen: %q", screen.String())
	}
	// 6. Ctrl-C still interrupts a foreground command.
	write("sleep 60\n")
	time.Sleep(1200 * time.Millisecond)
	write("\x03")
	time.Sleep(300 * time.Millisecond)
	write("echo FG-$?\n")
	if !waitScreen(outLine("FG-130"), 10*time.Second) {
		t.Fatalf("Ctrl-C did not interrupt sleep; screen: %q", screen.String())
	}
	// 7. The durable scrollback is byte-identical to what the reader was
	// served, once the journal has caught up (the last output ends a line).
	served := screen.String()
	served = served[:strings.LastIndex(served, "\n")+1] // a trailing prompt has no newline yet
	deadline := time.Now().Add(10 * time.Second)
	for {
		io_ := service.processes.interactiveIOFor(op.OperationID)
		if io_ != nil {
			data, _, _, _, _ := io_.tty.read(0, 1<<20)
			if len(data) >= len(served) {
				if string(data[:len(served)]) != served {
					t.Fatalf("durable scrollback diverges from served output:\nserved:  %q\ndurable: %q", served, data)
				}
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("durable scrollback never caught up with served output (%d bytes served)", len(served))
		}
		time.Sleep(200 * time.Millisecond)
	}
}
