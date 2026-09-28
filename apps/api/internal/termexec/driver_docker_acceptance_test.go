//go:build integration

package termexec

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

// Real-Docker acceptance of a Cloud terminal through the hosted path:
// Core terminal rows (real Postgres) → termexec driver → provisioner
// wire transport → runtimeprovision.Service → DockerBackend → the pinned
// job image, with a files-scope bind and (optionally) the job egress
// socket. The provisioner half needs the production provisioner's
// privileges, so run the test binary as root inside the provisioner image:
//
//	SUMI_TEST_TERMINAL_DOCKER=1
//	SUMI_TEST_JOB_IMAGE_PIN=<40-hex revision or sha256 image ID>
//	SUMI_TEST_TERMINAL_ROOT=<dir at the same path on host and in the runner>
//	SUMI_DOCKER_JOURNAL_ROOT=<docker data root mounted read-only>
//	SUMI_JOB_EGRESS_DIR=<host dir holding a running egress proxy socket> (optional)
//	--pid host (the signal path reads host /proc)
//
// The person and the secretary write to the same session; both see one
// PTY. A driver restart re-attaches the same shell rather than starting a
// new one, and close physically removes the container.
func TestDockerTerminalHostedPathAcceptance(t *testing.T) {
	if os.Getenv("SUMI_TEST_TERMINAL_DOCKER") != "1" {
		t.Skip("set SUMI_TEST_TERMINAL_DOCKER=1 in a provisioner-privileged runner")
	}
	pin := os.Getenv("SUMI_TEST_JOB_IMAGE_PIN")
	root := os.Getenv("SUMI_TEST_TERMINAL_ROOT")
	if pin == "" || root == "" {
		t.Fatal("SUMI_TEST_JOB_IMAGE_PIN and SUMI_TEST_TERMINAL_ROOT are required")
	}
	ctx := context.Background()
	pa := pid(t)

	pool := testdb.Create(t)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	store := agentstate.NewStore(pool)
	store.SetDefaultTerminalBackend("cloud")
	store.SetTerminalBackendAvailable("cloud")
	if _, _, err := store.EnsurePersona(ctx, pa, nil, ""); err != nil {
		t.Fatal(err)
	}

	// Files scope fixture: the bind source is a host path dockerd
	// resolves, so root must be identical inside and outside the runner.
	run := filepath.Join(root, "run-"+pa[len(pa)-12:])
	scopeName, err := fileaccess.ScopeForPersona(pa)
	if err != nil {
		t.Fatal(err)
	}
	mnt := filepath.Join(run, "mnt")
	scopeDir := filepath.Join(mnt, scopeName)
	if err := os.MkdirAll(scopeDir, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(scopeDir, 0o777); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(run) })
	checkBin := filepath.Join(run, "sumi-files-check")
	if err := os.WriteFile(checkBin, []byte("#!/bin/sh\nfor last; do :; done\necho \""+mnt+"/$last\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	env := []string{"PATH=" + os.Getenv("PATH"), "SUMI_JOB_IMAGE_TAG=" + pin}
	for _, name := range []string{"DOCKER_HOST", "SUMI_DOCKER_JOURNAL_ROOT", "SUMI_JOB_EGRESS_DIR"} {
		if v, ok := os.LookupEnv(name); ok {
			env = append(env, name+"="+v)
		}
	}
	backend, err := runtimeprovision.NewDockerBackend(runtimeprovision.DockerBackendConfig{BaseEnvironment: env})
	if err != nil {
		t.Fatal(err)
	}
	image, err := backend.ResolveProcessImage(ctx)
	if err != nil {
		t.Fatalf("startup image check: %v", err)
	}
	t.Logf("job image pin %s resolved to %s", pin, image)
	svc, err := runtimeprovision.NewService(backend, runtimeprovision.ServiceConfig{
		StateDirectory: filepath.Join(run, "prov"),
		Files:          runtimeprovision.FilesEnvironment{Mountpoint: mnt, VolumeUUID: "vol-acceptance", CheckPath: checkBin},
	})
	if err != nil {
		t.Fatal(err)
	}
	sockDir, err := os.MkdirTemp("", "prov")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "s") // sun_path is ~108 bytes
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := runtimeprovision.NewHandler(svc)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: handler}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close(); ln.Close() })
	obsCtx, stopObs := context.WithCancel(ctx)
	defer stopObs()
	go svc.RunProcessObserver(obsCtx)
	client, err := runtimeprovision.NewUnixClient(sock)
	if err != nil {
		t.Fatal(err)
	}

	var logs sync.Map
	cfg := Config{
		RunnerID: "termexec-acceptance", Backend: "cloud",
		Lease: 10 * time.Second, Interval: 200 * time.Millisecond,
		PollInterval: 100 * time.Millisecond, HeartbeatEvery: 5,
		MaxLifetimeSeconds: 900, UnknownWait: 5 * time.Second, CallTimeout: 10 * time.Second,
		Logf: func(format string, args ...any) {
			logs.Store(time.Now().Format(time.RFC3339Nano), fmt.Sprintf(format, args...))
		},
	}
	startDriver := func() func() {
		d := New(store, client, readyScope{}, cfg)
		dctx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() { d.Run(dctx); close(done) }()
		return func() { cancel(); <-done }
	}
	stopDriver := startDriver()
	defer func() { stopDriver() }()

	sess, err := store.CreateTerminalSession(ctx, pa, "acceptance", "human", "test")
	if err != nil {
		t.Fatal(err)
	}
	sid := sess.SessionID
	opID := runtimeprovision.ProcessOperationID(pa, "term:"+sid)
	container := "sumi-process-" + opID
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", container).Run()
		if t.Failed() {
			logs.Range(func(k, v any) bool { t.Logf("%s %s", k, v); return true })
		}
	})

	// One reader, as the web terminal does: cursor + event cursor.
	var screen strings.Builder
	var cursor, events int64
	var pullErr error
	pull := func() {
		read, err := store.ReadTerminalOutput(ctx, pa, sid, cursor, events, 64)
		pullErr = err
		if err != nil {
			return
		}
		for _, c := range read.Chunks {
			if c.Kind == "data" {
				screen.Write(c.Data)
			}
		}
		cursor, events = read.NextCursor, read.EventCursor
	}
	wait := func(what string, re *regexp.Regexp, d time.Duration) {
		t.Helper()
		deadline := time.Now().Add(d)
		for time.Now().Before(deadline) {
			pull()
			if re.MatchString(screen.String()) {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Logf("reader cursor=%d events=%d err=%v screen tail=%q", cursor, events, pullErr, tail(screen.String(), 200))
		if ins, err := store.ListTerminalInputs(ctx, pa, sid, 0, 100); err == nil {
			for _, in := range ins {
				t.Logf("input seq=%d epoch=%d kind=%s source=%s status=%s", in.Seq, in.SessionEpoch, in.Kind, in.Source, in.Status)
			}
		}
		if sess, err := store.GetTerminalSession(ctx, pa, sid); err == nil {
			t.Logf("session epoch=%d status=%s claimed_by=%v output_bytes=%d", sess.Epoch, sess.Status, sess.ClaimedBy, sess.OutputBytes)
		}
		if op, err := client.ProcessStatus(ctx, runtimeprovision.ProcessLookupRequest{PersonalityAgentID: pa, OperationID: opID}); err == nil {
			t.Logf("op state=%s stdout_bytes=%d attached=%v", op.State, op.StdoutBytes, op.OutputAttached)
		}
		if out, err := client.ReadProcessOutput(ctx, runtimeprovision.ProcessOutputRequest{ProcessLookupRequest: runtimeprovision.ProcessLookupRequest{PersonalityAgentID: pa, OperationID: opID}, Stream: "stdout", Offset: 700, Limit: 64 << 10}); err == nil {
			t.Logf("provisioner output from 700: next=%d eof=%v content=%q", out.NextOffset, out.EOF, out.Content)
		} else {
			t.Logf("provisioner output read: %v", err)
		}
		if out, err := exec.Command("docker", "logs", "--tail", "5", container).CombinedOutput(); err == nil {
			t.Logf("docker logs tail: %q", out)
		}
		t.Fatalf("%s: not seen within %s; status=%s screen=%q", what, d, sessionStatus(t, store, pa, sid), screen.String())
	}
	input := func(source, kind string, payload map[string]any) {
		t.Helper()
		if _, err := store.SubmitTerminalInput(ctx, pa, sid, source, kind, payload); err != nil {
			t.Fatalf("%s %s input: %v", source, kind, err)
		}
	}
	line := func(s string) *regexp.Regexp {
		return regexp.MustCompile(`(?m)(?:^|\r)(?:\x1b\[[0-9;?]*[a-zA-Z])*` + regexp.QuoteMeta(s) + `\r?$`)
	}

	started := time.Now()
	waitFor(t, 60*time.Second, "session active", func() bool { return sessionStatus(t, store, pa, sid) == "active" })
	// 1. A shell prompt appears without any input.
	wait("prompt", regexp.MustCompile(`\$ $`), 30*time.Second)
	t.Logf("prompt visible %s after terminal.open", time.Since(started).Round(time.Millisecond))

	// 2. Person and secretary share one PTY: both inputs land in the
	// same shell, visible in the one output stream.
	input("human", "stdin", map[string]any{"data": "SHARED=from-person; echo H-$((40+2))\n"})
	wait("person command", line("H-42"), 15*time.Second)
	input("agent", "stdin", map[string]any{"data": "echo \"A-$SHARED-$$\"\n"})
	wait("secretary sees person's shell state", regexp.MustCompile(`(?m)A-from-person-[0-9]+\r?$`), 15*time.Second)
	shellPID := regexp.MustCompile(`A-from-person-([0-9]+)`).FindStringSubmatch(screen.String())[1]

	// 3. Colour: xterm-256color terminal, SGR sequences pass through.
	input("agent", "stdin", map[string]any{"data": "echo T=$TERM C=$(tput colors); printf '\\033[31mRED\\033[0m\\n'\n"})
	wait("TERM", line("T=xterm-256color C=256"), 15*time.Second)
	wait("SGR colour", regexp.MustCompile(`\x1b\[31mRED\x1b\[0m`), 15*time.Second)

	// 4. Terminal size follows a resize from the person's window.
	input("human", "resize", map[string]any{"cols": float64(132), "rows": float64(41)})
	input("human", "stdin", map[string]any{"data": "echo SZ=$(stty size)\n"})
	wait("resize", line("SZ=41 132"), 15*time.Second)

	// 5. Ctrl-C as the web terminal sends it (a ^C byte) interrupts the
	// foreground job; the signal input path does the same.
	input("human", "stdin", map[string]any{"data": "sleep 60\n"})
	time.Sleep(1500 * time.Millisecond)
	input("human", "stdin", map[string]any{"data": "\x03"})
	input("human", "stdin", map[string]any{"data": "echo FG-$?\n"})
	wait("Ctrl-C byte", line("FG-130"), 15*time.Second)
	input("agent", "stdin", map[string]any{"data": "sleep 61\n"})
	time.Sleep(1500 * time.Millisecond)
	input("agent", "signal", map[string]any{"signal": "INT"})
	input("agent", "stdin", map[string]any{"data": "echo SIG-$?\n"})
	wait("INT signal", line("SIG-130"), 15*time.Second)

	// 6. Reconnect: a fresh reader (cursor 0) gets the retained scrollback.
	fresh, err := store.ReadTerminalOutput(ctx, pa, sid, 0, 0, 64)
	if err != nil {
		t.Fatal(err)
	}
	var replay strings.Builder
	for _, c := range fresh.Chunks {
		replay.Write(c.Data)
	}
	if !strings.Contains(replay.String(), "H-42") {
		t.Fatalf("fresh reader missing scrollback: %q", replay.String())
	}

	// 7. Driver restart (API redeploy) re-attaches the same shell; it
	// must not start a replacement that silently loses the session.
	stopDriver()
	stopDriver = startDriver()
	input("agent", "stdin", map[string]any{"data": "echo \"R-$SHARED-$$\"\n"})
	wait("same shell after driver restart", line("R-from-person-"+shellPID), 60*time.Second)
	if st := sessionStatus(t, store, pa, sid); st != "active" {
		t.Fatalf("session after restart: %s", st)
	}

	// 8. Close ends the session and physically removes the container.
	if _, err := store.CloseTerminalSession(ctx, pa, sid, "closed"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 60*time.Second, "session ended", func() bool { return sessionStatus(t, store, pa, sid) == "ended" })
	waitFor(t, 60*time.Second, "container removed", func() bool {
		out, _ := exec.Command("docker", "ps", "-aq", "--filter", "name=^"+container+"$").Output()
		return strings.TrimSpace(string(out)) == ""
	})
	final, _ := store.GetTerminalSession(ctx, pa, sid)
	t.Logf("closed: status=%s reason=%s; total %s", final.Status, final.EndReason, time.Since(started).Round(time.Millisecond))
}

func tail(s string, n int) string {
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}
