//go:build linux

package localterminal

import (
	"context"
	"encoding/json"
	"errors"
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
)

const testPersona = "0198f0f4-9b72-7000-8000-000000000321"

func newTestBackend(t *testing.T, limit int) (ProcessBackend, Config) {
	t.Helper()
	root := t.TempDir()
	cfg := Config{PersonaID: testPersona, WorkspaceRoot: filepath.Join(root, "workspace"), JournalRoot: filepath.Join(root, "terminals"), OutputLimit: limit}
	b, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { b.Close() })
	return b, cfg
}
func shellRequest(id string) runtimeprovision.ProcessStartRequest {
	return runtimeprovision.ProcessStartRequest{PersonalityAgentID: testPersona, OriginatingToolCallID: id, Executable: "/bin/bash", Args: []string{"--noprofile", "--norc", "-i"}, Cwd: ".", Interactive: true, TTY: true, TimeoutSeconds: 60}
}
func lookup(op runtimeprovision.ProcessOperation) runtimeprovision.ProcessLookupRequest {
	return runtimeprovision.ProcessLookupRequest{PersonalityAgentID: op.PersonalityAgentID, OperationID: op.OperationID}
}
func write(t *testing.T, b ProcessBackend, op runtimeprovision.ProcessOperation, data string) {
	t.Helper()
	r, e := b.WriteProcessInput(context.Background(), runtimeprovision.ProcessInputRequest{ProcessLookupRequest: lookup(op), Data: []byte(data)})
	if e != nil || !r.Delivered {
		t.Fatalf("write %+v %v", r, e)
	}
}
func output(t *testing.T, b ProcessBackend, op runtimeprovision.ProcessOperation, want string) runtimeprovision.ProcessOutput {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var out runtimeprovision.ProcessOutput
	for time.Now().Before(deadline) {
		var e error
		out, e = b.ReadProcessOutput(context.Background(), runtimeprovision.ProcessOutputRequest{ProcessLookupRequest: lookup(op), Stream: "stdout", Limit: 64 << 10})
		if e != nil {
			t.Fatal(e)
		}
		if strings.Contains(out.Content, want) {
			return out
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("output missing %q: %+v", want, out)
	return out
}
func waitEnded(t *testing.T, b ProcessBackend, op runtimeprovision.ProcessOperation) runtimeprovision.ProcessOperation {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, e := b.ProcessStatus(context.Background(), lookup(op))
		if e != nil {
			t.Fatal(e)
		}
		if got.State.Terminal() {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("shell did not end")
	return op
}
func TestActualPTYResizeSignalPersistenceAndReplay(t *testing.T) {
	b, cfg := newTestBackend(t, 4096)
	ctx := context.Background()
	req := shellRequest("shell-one")
	op, e := b.StartProcess(ctx, req)
	if e != nil {
		t.Fatal(e)
	}
	again, e := b.StartProcess(ctx, req)
	if e != nil || again.OperationID != op.OperationID {
		t.Fatal("start replay", again, e)
	}
	write(t, b, op, "stty -echo; SHARED_VALUE=retained; printf 'file-data' > shared.txt\n")
	time.Sleep(50 * time.Millisecond)
	write(t, b, op, "printf '\\n%s\\n' \"$SHARED_VALUE\"; test -t 0 && printf 'actual-tty\\n'\n")
	output(t, b, op, "\r\nretained\r\nactual-tty\r\n")
	if _, e = b.ResizeProcess(ctx, runtimeprovision.ProcessResizeRequest{ProcessLookupRequest: lookup(op), Cols: 101, Rows: 31}); e != nil {
		t.Fatal(e)
	}
	write(t, b, op, "stty size\n")
	output(t, b, op, "31 101\r\n")
	write(t, b, op, "sleep 30\n")
	time.Sleep(100 * time.Millisecond)
	receipt, e := b.SignalProcess(ctx, runtimeprovision.ProcessSignalRequest{ProcessLookupRequest: lookup(op), Signal: "INT"})
	if e != nil || !receipt.Delivered {
		t.Fatal("signal", receipt, e)
	}
	write(t, b, op, "printf 'interrupt-returned\\n'\n")
	output(t, b, op, "interrupt-returned\r\n")
	// The retained window bounds memory and reports skipped output explicitly.
	write(t, b, op, "head -c 8192 /dev/zero | tr '\\0' x; printf '\\nretention-end\\n'\n")
	out := output(t, b, op, "retention-end\r\n")
	if !out.Gap || out.BaseOffset == 0 || len(out.Content) > 4096 {
		t.Fatalf("retention %+v", out)
	}
	wrong := lookup(op)
	wrong.PersonalityAgentID = "0198f0f4-9b72-7000-8000-000000000322"
	if _, e = b.ProcessStatus(ctx, wrong); !errors.Is(e, runtimeprovision.ErrProcessNotFound) {
		t.Fatal("foreign persona", e)
	}
	write(t, b, op, "exit 7\n")
	ended := waitEnded(t, b, op)
	if ended.ExitCode == nil || *ended.ExitCode != 7 || ended.Quiesced {
		t.Fatalf("shell outcome/physical claim %+v", ended)
	}
	if _, e = b.StartProcess(ctx, req); e != nil {
		t.Fatal(e)
	}
	if after, _ := b.ProcessStatus(ctx, lookup(op)); after.State != ended.State {
		t.Fatal("ended shell resurrected")
	}
	next, e := b.StartProcess(ctx, shellRequest("shell-two"))
	if e != nil {
		t.Fatal(e)
	}
	write(t, b, next, "stty -echo; printf '\\n'; cat shared.txt; printf '\\n'\n")
	output(t, b, next, "\r\nfile-data\r\n")
	if raw, e := os.ReadFile(filepath.Join(cfg.WorkspaceRoot, testPersona, "shared.txt")); e != nil || string(raw) != "file-data" {
		t.Fatal("workspace persistence", string(raw), e)
	}
}
func TestCancelBeforeLaunchIsDurableFence(t *testing.T) {
	b, cfg := newTestBackend(t, 0)
	req := shellRequest("cancel-before-start")
	id := runtimeprovision.ProcessOperationID(testPersona, req.OriginatingToolCallID)
	op, e := b.CancelProcess(context.Background(), runtimeprovision.ProcessLookupRequest{PersonalityAgentID: testPersona, OperationID: id, TombstoneIfAbsent: true})
	if e != nil || !op.Tombstone || !op.Quiesced {
		t.Fatal(op, e)
	}
	op, e = b.StartProcess(context.Background(), req)
	if e != nil || !op.Tombstone {
		t.Fatal("late launch not fenced", op, e)
	}
	b.Close()
	reopened, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	op, e = reopened.StartProcess(context.Background(), req)
	if e != nil || !op.Tombstone {
		t.Fatal("restarted fence", op, e)
	}
}
func TestJournalOwnerAndCwdRefusal(t *testing.T) {
	b, cfg := newTestBackend(t, 0)
	if _, e := New(cfg); e == nil {
		t.Fatal("second backend acquired journal")
	}
	os.MkdirAll(filepath.Join(cfg.WorkspaceRoot, testPersona), 0700)
	os.Symlink(t.TempDir(), filepath.Join(cfg.WorkspaceRoot, testPersona, "escape"))
	req := shellRequest("escape")
	req.Cwd = "escape"
	if _, e := b.StartProcess(context.Background(), req); !errors.Is(e, runtimeprovision.ErrProcessWorkspace) {
		t.Fatal("cwd escape", e)
	}
}

// This helper deliberately dies without Close, exactly as a SIGKILLed host
// would. The PTY shell ignores HUP and disconnects its stdio so its survival is
// observable independently from the backend's recovered logical disposition.
func TestHostDeathHelper(t *testing.T) {
	root := os.Getenv("SUMI_LOCAL_TERMINAL_DEATH_HELPER")
	if root == "" {
		return
	}
	cfg := Config{PersonaID: testPersona, WorkspaceRoot: filepath.Join(root, "workspace"), JournalRoot: filepath.Join(root, "terminals")}
	b, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	op, e := b.StartProcess(context.Background(), shellRequest("host-death"))
	if e != nil {
		t.Fatal(e)
	}
	write(t, b, op, "trap '' HUP; printf x >> launched; exec </dev/null >/dev/null 2>&1; echo $$ > survivor.pid; while :; do sleep 0.1; done\n")
	select {}
}

func TestWholeHostDeathDoesNotRelaunchOrKillSurvivor(t *testing.T) {
	root := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHostDeathHelper$")
	cmd.Env = append(os.Environ(), "SUMI_LOCAL_TERMINAL_DEATH_HELPER="+root)
	if e := cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()
	pidPath := filepath.Join(root, "workspace", testPersona, "survivor.pid")
	var pid int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		raw, _ := os.ReadFile(pidPath)
		pid, _ = strconv.Atoi(strings.TrimSpace(string(raw)))
		if pid > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pid == 0 {
		t.Fatal("surviving shell was not ready")
	}
	// Pin the actual fixture process before killing its host; never signal a
	// number rediscovered from persisted state after a possible PID reuse.
	fd, e := unix.PidfdOpen(pid, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer unix.Close(fd)
	defer unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0)
	if e = cmd.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	_ = cmd.Wait()
	cfg := Config{PersonaID: testPersona, WorkspaceRoot: filepath.Join(root, "workspace"), JournalRoot: filepath.Join(root, "terminals")}
	b, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	op, e := b.StartProcess(context.Background(), shellRequest("host-death"))
	if e != nil || op.State != runtimeprovision.ProcessIndeterminate || op.Quiesced || op.OutputAttached || !strings.Contains(op.Error, "not proven stopped") {
		t.Fatal("restart disposition", op, e)
	}
	if _, e := b.ReleaseProcessTombstone(context.Background(), lookup(op)); !errors.Is(e, runtimeprovision.ErrConflict) {
		t.Fatal("host-death history released", e)
	}
	cancelled, e := b.CancelProcess(context.Background(), lookup(op))
	if e != nil || cancelled.Quiesced || cancelled.State != runtimeprovision.ProcessIndeterminate {
		t.Fatal("cancel lost shell", cancelled, e)
	}
	if e = unix.PidfdSendSignal(fd, 0, nil, 0); e != nil {
		t.Fatal("recovery killed surviving physical shell", e)
	}
	raw, e := os.ReadFile(filepath.Join(cfg.WorkspaceRoot, testPersona, "launched"))
	if e != nil || string(raw) != "x" {
		t.Fatal("relaunch happened", string(raw), e)
	}
	out, e := b.ReadProcessOutput(context.Background(), runtimeprovision.ProcessOutputRequest{ProcessLookupRequest: lookup(op), Stream: "stdout"})
	if e != nil || len(out.Gaps) != 1 {
		t.Fatal("restart output loss missing", out, e)
	}
	t.Log("SIGKILLed actual host; original HUP-ignoring physical shell survives; restart returns indeterminate, never relaunches or signals it; fixture cleanup uses pre-death pidfd")
}

func TestCorruptRetainedOutputJournalRefusesRecovery(t *testing.T) {
	b, cfg := newTestBackend(t, 0)
	b.Close()
	req := shellRequest("corrupt-tail")
	id := runtimeprovision.ProcessOperationID(testPersona, req.OriginatingToolCallID)
	rec := record{Operation: runtimeprovision.ProcessOperation{OperationID: id, PersonalityAgentID: testPersona, State: runtimeprovision.ProcessRunning, StdoutBytes: 42, StdoutBase: 0}}
	raw, e := json.Marshal(rec)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(cfg.JournalRoot, id+".json"), raw, 0600); e != nil {
		t.Fatal(e)
	}
	if recovered, e := New(cfg); e == nil {
		recovered.Close()
		t.Fatal("corrupt tail would produce invalid slices")
	}
}
