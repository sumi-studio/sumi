//go:build linux

package composeanchor

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestAnchorHelperProcess(t *testing.T) {
	if os.Getenv("SUMI_TEST_COMPOSE_ANCHOR") != "1" {
		return
	}
	separator := 0
	for index, arg := range os.Args {
		if arg == "--" {
			separator = index + 1
			break
		}
	}
	if separator == 0 {
		os.Exit(126)
	}
	if err := Run(os.Args[separator:]); err != nil {
		os.Exit(125)
	}
	os.Exit(0)
}

func startTestAnchor(t *testing.T, script string, environment ...string) *exec.Cmd {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=TestAnchorHelperProcess", "--", "/bin/bash", "-p", "-c", script)
	command.Env = append([]string{"PATH=/usr/bin:/bin", "SUMI_TEST_COMPOSE_ANCHOR=1"}, environment...)
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if command.ProcessState != nil && command.ProcessState.Exited() {
			return
		}
		// A failed assertion must not strand a stopped anchor or one of its
		// detached adopted descendants on the host running the test suite.
		_ = command.Process.Signal(syscall.SIGCONT)
		_ = command.Process.Signal(syscall.SIGTERM)
		_ = command.Process.Signal(syscall.SIGUSR2)
		done := make(chan struct{})
		go func() {
			_, _ = command.Process.Wait()
			close(done)
		}()
		select {
		case <-done:
			return
		case <-time.After(time.Second):
		}
		_ = signalContainedDescendants(command.Process.Pid, syscall.SIGKILL)
		_ = command.Process.Kill()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
	})
	return command
}

func waitStopped(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
		if err == nil {
			fields := strings.Fields(string(raw)[strings.LastIndex(string(raw), ") ")+2:])
			if len(fields) > 3 && fields[0] == "T" && fields[2] == strconv.Itoa(pid) && fields[3] == strconv.Itoa(pid) {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("anchor %d was not observably stopped and isolated", pid)
}

func continueAnchor(t *testing.T, command *exec.Cmd) {
	t.Helper()
	waitStopped(t, command.Process.Pid)
	if err := command.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
}

func TestAnchorCannotLoseContinueBeforeStoppedProof(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ran")
	command := startTestAnchor(t, `printf ran >"$MARKER"`, "MARKER="+marker)
	t.Cleanup(func() { _ = command.Process.Kill(); _, _ = command.Process.Wait() })
	time.Sleep(75 * time.Millisecond)
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("anchored command ran before stopped proof: %v", err)
	}
	continueAnchor(t, command)
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestAnchorDelayedSelfStopCannotLoseContinue(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ran")
	command := exec.Command(
		"/bin/bash", "-c", `kill -STOP $$; exec "$@"`, "bash",
		os.Args[0], "-test.run=TestAnchorHelperProcess", "--", "/bin/bash", "-p", "-c", `printf ran >"$MARKER"`,
	)
	command.Env = []string{"PATH=/usr/bin:/bin", "SUMI_TEST_COMPOSE_ANCHOR=1", "MARKER=" + marker}
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = command.Process.Kill(); _, _ = command.Process.Wait() })
	waitStopped(t, command.Process.Pid)
	if err := command.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	time.Sleep(75 * time.Millisecond)
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("early CONT was retained across the delayed self-stop: %v", err)
	}
	waitStopped(t, command.Process.Pid)
	if err := command.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestAnchorWaitsForOrdinaryHelperAfterLeaderExit(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "helper-done")
	command := startTestAnchor(t, `(sleep 0.15; printf done >"$MARKER") & exit 0`, "MARKER="+marker)
	continueAnchor(t, command)
	started := time.Now()
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) < 100*time.Millisecond {
		t.Fatal("anchor exited with the leader while an ordinary helper survived")
	}
	if raw, err := os.ReadFile(marker); err != nil || string(raw) != "done" {
		t.Fatalf("ordinary helper did not finish: %q %v", raw, err)
	}
}

func TestAnchorWaitsForDetachedDescendantAfterLeaderExit(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "detached-done")
	command := startTestAnchor(t, `setsid /bin/bash -c 'sleep 0.15; printf done >"$MARKER"' & exit 0`, "MARKER="+marker)
	continueAnchor(t, command)
	started := time.Now()
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) < 100*time.Millisecond {
		t.Fatal("anchor exited before a detached descendant was reaped")
	}
	if raw, err := os.ReadFile(marker); err != nil || string(raw) != "done" {
		t.Fatalf("detached descendant did not finish: %q %v", raw, err)
	}
}

func TestAnchorForceKillsTermResistantLeaderFirstHelperBeforeExit(t *testing.T) {
	ready := filepath.Join(t.TempDir(), "ready")
	command := startTestAnchor(t, `(trap '' TERM; printf ready >"$READY"; while :; do sleep 1; done) & exit 0`, "READY="+ready)
	continueAnchor(t, command)
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("TERM-resistant helper did not start")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := command.Process.Signal(syscall.SIGUSR2); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("terminated anchor unexpectedly reported success")
	}
	if err := syscall.Kill(-command.Process.Pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("anchor reported done before exact group absence: %v", err)
	}
}

func TestAnchorForceKillsDetachedAdoptedDescendantBeforeExit(t *testing.T) {
	ready := filepath.Join(t.TempDir(), "detached-ready")
	command := startTestAnchor(t, `setsid /bin/bash -c 'trap "" TERM; printf ready >"$READY"; while :; do sleep 1; done' & exit 0`, "READY="+ready)
	continueAnchor(t, command)
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("detached TERM-resistant descendant did not start")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := command.Process.Signal(syscall.SIGUSR2); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("terminated anchor unexpectedly reported success")
	}
}

func TestAnchorPrivilegedBashIgnoresBashEnv(t *testing.T) {
	dir := t.TempDir()
	hook := filepath.Join(dir, "hook")
	marker := filepath.Join(dir, "sourced")
	if err := os.WriteFile(hook, []byte(`printf sourced >"$MARKER"`), 0o600); err != nil {
		t.Fatal(err)
	}
	command := startTestAnchor(t, `:`, "BASH_ENV="+hook, "MARKER="+marker)
	continueAnchor(t, command)
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("privileged inner Bash sourced BASH_ENV: %v", err)
	}
}
