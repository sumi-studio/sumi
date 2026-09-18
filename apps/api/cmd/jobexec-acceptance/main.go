// jobexec-acceptance is the guarded root-run acceptance helper for the
// Linux-on-demand job path. Root executes it against the real deployment
// after source acceptance; it drives the real components, not fixtures:
//
//	FileScopeEnsurer (real fileaccess client) → deployed filesvc
//	→ owned runtime-provisioner instance (real binary, owned socket/state)
//	→ real sumi-files-check + real JuiceFS mount → real Docker
//	→ file-service readback through the deployed filesvc
//
// Proves: new-persona scope creation, a real launch with output and
// scope-visible artifacts, A/B scope separation, all-squash file
// management from the token side, missing-scope and changed-volume
// refusals, provisioner restart with no repeat launch, cancellation, and
// cleanup of every container it created.
//
// Credentials: the filesvc token is supplied as a FILE PATH and read into
// memory at execution time. The token value is never placed on a command
// line, never logged, and never persisted by this tool. The tool does not
// read or enumerate any other private configuration.
//
// Scope of side effects: filesvc operations touch only the generated
// persona scopes created for this run; Docker operations touch only
// containers named for this run's deterministic operation IDs; the owned
// provisioner instances use owned sockets and state directories under
// --work-dir. No public service is reconfigured, stopped, or probed
// beyond ordinary API requests.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
	"github.com/sumi-studio/sumi/apps/api/internal/jobexec"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
)

const confirmPhrase = "run-real-files-acceptance"

var (
	filesvcURL = flag.String("filesvc-url", "", "deployed filesvc base URL, e.g. http://127.0.0.1:8780")
	tokenFile  = flag.String("filesvc-token-file", "", "path to a file containing the filesvc service token (read into memory; never logged or passed on a command line)")
	provBin    = flag.String("provisioner-bin", "", "path to a runtime-provisioner binary built from this source (e.g. go build -o /tmp/rp ./cmd/runtime-provisioner)")
	filesMount = flag.String("files-mount", "", "canonical files mount root, e.g. /var/lib/sumi-files/mnt")
	filesUUID  = flag.String("files-uuid", "", "expected JuiceFS volume UUID")
	filesCheck = flag.String("files-check", "", "path to the real deploy/files/sumi-files-check")
	jobImage   = flag.String("job-image", "", "full 40-hex sumi-job image revision present in Docker")
	workDir    = flag.String("work-dir", "", "scratch dir for owned sockets/state (default: mktemp)")
	confirm    = flag.String("confirm", "", "must equal "+confirmPhrase+" — the guard that keeps this from running by accident")
)

type harness struct {
	ctx      context.Context
	files    *fileaccess.Client
	proc     *runtimeprovision.Client
	dir      string
	personas []string
	ops      []string // operation IDs whose containers we may clean
	checks   int
	failed   int
	prov     *exec.Cmd
	socket   string
	stateDir string
}

func main() {
	flag.Parse()
	if *confirm != confirmPhrase {
		fmt.Fprintf(os.Stderr, "refusing to run without --confirm %s\n", confirmPhrase)
		os.Exit(2)
	}
	for _, req := range []struct{ name, val string }{
		{"filesvc-url", *filesvcURL}, {"filesvc-token-file", *tokenFile},
		{"provisioner-bin", *provBin}, {"files-mount", *filesMount},
		{"files-uuid", *filesUUID}, {"files-check", *filesCheck}, {"job-image", *jobImage},
	} {
		if req.val == "" {
			fmt.Fprintf(os.Stderr, "missing required --%s\n", req.name)
			os.Exit(2)
		}
	}
	token, err := os.ReadFile(*tokenFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "token file: %v\n", err)
		os.Exit(2)
	}
	fc, err := fileaccess.NewClient(*filesvcURL, strings.TrimSpace(string(token)))
	if err != nil {
		fmt.Fprintf(os.Stderr, "fileaccess client: %v\n", err)
		os.Exit(2)
	}
	dir := *workDir
	if dir == "" {
		// Default is cwd-relative, not /tmp: probe evidence belongs under
		// the report directory the run is invoked from.
		dir, err = filepath.Abs(fmt.Sprintf("jobexec-acceptance-work-%d", time.Now().Unix()))
		if err != nil {
			fmt.Fprintf(os.Stderr, "work dir: %v\n", err)
			os.Exit(2)
		}
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "work dir: %v\n", err)
		os.Exit(2)
	}
	h := &harness{ctx: context.Background(), files: fc, dir: dir}
	h.run()
	// Cleanup runs BEFORE any exit — os.Exit skips deferred calls, and a
	// failed probe must not leave owned provisioners or containers alive.
	summary := h.cleanup()
	fmt.Printf("cleanup: %s\n", summary)
	if h.failed > 0 {
		fmt.Printf("\nACCEPTANCE FAILED: %d of %d checks failed — evidence retained under %s\n", h.failed, h.checks, h.dir)
		os.Exit(1)
	}
	fmt.Printf("\nACCEPTANCE PASSED: %d checks\n", h.checks)
}

// callCtx bounds each backend call — a hung provisioner or filesvc must
// fail a check, never stall the acceptance run (and its cleanup).
func (h *harness) callCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(h.ctx, 30*time.Second)
}

func (h *harness) check(name string, ok bool, detail string) {
	h.checks++
	if ok {
		fmt.Printf("PASS  %s\n", name)
		return
	}
	h.failed++
	fmt.Printf("FAIL  %s: %s\n", name, detail)
}

// startProvisioner spawns an owned provisioner instance: owned socket and
// state dir under --work-dir, real mount/UUID/check, real Docker.
func (h *harness) startProvisioner(sock, state, mount, uuidV string) error {
	h.socket, h.stateDir = sock, state
	cmd := exec.Command(*provBin,
		"-socket", sock, "-state-dir", state,
		"-supervisor", "/bin/true", "-socket-mode", "0600")
	cmd.Env = append(os.Environ(),
		"SUMI_FILES_MOUNTPOINT="+mount,
		"SUMI_FILES_VOLUME_UUID="+uuidV,
		"SUMI_FILES_CHECK="+*filesCheck,
		"SUMI_FILES_CHECK_WAIT_SECONDS=10",
		"SUMI_JOB_IMAGE_TAG="+*jobImage,
	)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	h.prov = cmd
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(sock); err == nil {
			client, err := runtimeprovision.NewUnixClient(sock)
			if err != nil {
				return err
			}
			h.proc = client
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("provisioner socket never appeared")
}

func (h *harness) stopProvisioner(kill bool) {
	if h.prov == nil {
		return
	}
	sig := syscall.SIGTERM
	if kill {
		sig = syscall.SIGKILL
	}
	_ = h.prov.Process.Signal(sig)
	done := make(chan error, 1)
	go func() { done <- h.prov.Wait() }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		_ = h.prov.Process.Kill()
		<-done
	}
	h.prov = nil
}

func (h *harness) waitState(paid, opID string, wantTerminal bool, bound time.Duration) (runtimeprovision.ProcessOperation, error) {
	deadline := time.Now().Add(bound)
	lookup := runtimeprovision.ProcessLookupRequest{PersonalityAgentID: paid, OperationID: opID}
	for time.Now().Before(deadline) {
		callCtx, cancel := h.callCtx()
		op, err := h.proc.ProcessStatus(callCtx, lookup)
		cancel()
		if err != nil {
			return op, err
		}
		if wantTerminal {
			switch op.State {
			case runtimeprovision.ProcessSucceeded, runtimeprovision.ProcessFailed,
				runtimeprovision.ProcessCancelled, runtimeprovision.ProcessIndeterminate:
				return op, nil
			}
		} else if op.State == runtimeprovision.ProcessRunning {
			return op, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return runtimeprovision.ProcessOperation{}, errors.New("state wait timed out")
}

func (h *harness) readAll(paid, opID, stream string) string {
	var b strings.Builder
	var off int64
	// Page bound: a misbehaving server must not spin this loop forever.
	for pages := 0; pages < 64; pages++ {
		callCtx, cancel := h.callCtx()
		out, err := h.proc.ReadProcessOutput(callCtx, runtimeprovision.ProcessOutputRequest{
			ProcessLookupRequest: runtimeprovision.ProcessLookupRequest{PersonalityAgentID: paid, OperationID: opID},
			Stream:               stream, Offset: off, Limit: 64 << 10,
		})
		cancel()
		if err != nil {
			return b.String() + "<read error: " + err.Error() + ">"
		}
		b.WriteString(out.Content)
		off = out.NextOffset
		if out.EOF || out.Content == "" {
			return b.String()
		}
	}
	return b.String() + "<page bound reached>"
}

func scopeOf(paid string) string {
	s, err := fileaccess.ScopeForPersona(paid)
	if err != nil {
		return "<bad persona>"
	}
	return s
}

func (h *harness) run() {
	ensurer := &jobexec.FileScopeEnsurer{Client: h.files}
	paA := uuid.Must(uuid.NewV7()).String()
	paB := uuid.Must(uuid.NewV7()).String()
	paC := uuid.Must(uuid.NewV7()).String() // never ensured — refusal probe
	h.personas = []string{paA, paB, paC}
	scopeA, scopeB := scopeOf(paA), scopeOf(paB)
	fmt.Printf("acceptance: personas A=%s B=%s C=%s (generated, owned)\n", paA, paB, paC)
	fmt.Printf("acceptance: filesvc=%s mount=%s workdir=%s\n", *filesvcURL, *filesMount, h.dir)
	ensure := func(paid string) error {
		callCtx, cancel := h.callCtx()
		defer cancel()
		return ensurer.EnsureScope(callCtx, paid)
	}
	filesStat := func(scope, name string) (fileaccess.StatInfo, error) {
		callCtx, cancel := h.callCtx()
		defer cancel()
		return h.files.Stat(callCtx, scope, name)
	}

	// --- phase 1: real FileScopeEnsurer creates a new persona scope ---
	if err := ensure(paA); err != nil {
		h.check("ensure-scope-A", false, err.Error())
		return
	}
	_, statErr := filesStat(scopeA, "")
	_, diskErr := os.Stat(filepath.Join(*filesMount, scopeA))
	h.check("ensure-scope-A (new persona scope via filesvc)", statErr == nil && diskErr == nil,
		fmt.Sprintf("stat=%v disk=%v", statErr, diskErr))
	if err := ensure(paB); err != nil {
		h.check("ensure-scope-B", false, err.Error())
		return
	}
	_, diskErrB := os.Stat(filepath.Join(*filesMount, scopeB))
	h.check("ensure-scope-B (second persona scope)", diskErrB == nil, fmt.Sprint(diskErrB))
	// Replay idempotency: the second ensure for A must be a no-op.
	err := ensure(paA)
	h.check("ensure-scope-A-replay (idempotent)", err == nil, fmt.Sprint(err))

	// --- phase 2: real provisioner launch on the real mount ---
	err = h.startProvisioner(filepath.Join(h.dir, "prov.sock"), filepath.Join(h.dir, "prov-state"),
		*filesMount, *filesUUID)
	if !h.okCheck("provisioner-start (real mount/check/image)", err) {
		return
	}
	opA := h.startJob(paA, "job:accept-a",
		"/bin/sh", []string{"-c", "printf A-artifact > a.txt; echo job-stdout-marker; id -u"})
	if !h.okCheck("start-A (files-scope launch)", errOf(&opA)) {
		return
	}
	term, werr := h.waitState(paA, opA.OperationID, true, 90*time.Second)
	h.check("job-A terminal", werr == nil && term.State == runtimeprovision.ProcessSucceeded,
		fmt.Sprintf("state=%s err=%v", term.State, werr))
	stdout := h.readAll(paA, opA.OperationID, "stdout")
	h.check("job-A stdout", strings.Contains(stdout, "job-stdout-marker") && strings.Contains(stdout, "10002"),
		"stdout="+stdout)
	// The artifact landed in the canonical scope — visible to the storage
	// authority, not only inside the container.
	readCtx, readCancel := h.callCtx()
	rd, rerr := h.files.Read(readCtx, scopeA, "a.txt", 0, -1)
	readCancel()
	h.check("filesvc readback of job artifact", rerr == nil && string(rd.Body) == "A-artifact",
		fmt.Sprintf("read=%v body=%q", rerr, rd.Body))

	// --- phase 3: A/B separation ---
	opB := h.startJob(paB, "job:accept-b", "/bin/sh", []string{"-c", "ls /workspace; echo B-done"})
	if h.okCheck("start-B", errOf(&opB)) {
		termB, _ := h.waitState(paB, opB.OperationID, true, 90*time.Second)
		outB := h.readAll(paB, opB.OperationID, "stdout")
		h.check("B-scope terminal", termB.State == runtimeprovision.ProcessSucceeded, string(termB.State))
		h.check("B cannot see A's artifact (scope isolation)",
			strings.Contains(outB, "B-done") && !strings.Contains(outB, "a.txt"), "B stdout="+outB)
	}

	// --- phase 4: all-squash — the uid-10002 artifact is managed by filesvc ---
	wCtx, wCancel := h.callCtx()
	ver, werr := h.files.Write(wCtx, scopeA, "svc-note.txt", "none", []byte("token-side"))
	wCancel()
	h.check("filesvc write into job scope", werr == nil, fmt.Sprint(werr))
	st, serr := filesStat(scopeA, "a.txt")
	h.check("filesvc stat of container-written file", serr == nil && st.Kind != "", fmt.Sprint(serr))
	rmCtx, rmCancel := h.callCtx()
	rerr = h.files.Remove(rmCtx, scopeA, "svc-note.txt", "any")
	rmCancel()
	h.check("filesvc remove inside job scope", rerr == nil, fmt.Sprint(rerr))
	_ = ver

	// --- phase 5: missing scope refuses ---
	opC := h.startJob(paC, "job:accept-c", "/bin/sh", []string{"-c", "true"})
	h.check("missing-scope refusal", errors.Is(opC.startErr, runtimeprovision.ErrProcessWorkspace),
		fmt.Sprintf("err=%v", opC.startErr))

	// --- phase 6: changed volume refuses (owned provisioner, real check) ---
	h.stopProvisioner(false)
	err = h.startProvisioner(filepath.Join(h.dir, "prov-wrong.sock"), filepath.Join(h.dir, "prov-state-wrong"),
		*filesMount, "00000000-0000-0000-0000-000000000000")
	if h.okCheck("provisioner-start wrong-uuid", err) {
		opW := h.startJob(paA, "job:accept-wronguuid", "/bin/sh", []string{"-c", "true"})
		h.check("changed-volume refusal", errors.Is(opW.startErr, runtimeprovision.ErrProcessWorkspace),
			fmt.Sprintf("err=%v", opW.startErr))
	}
	h.stopProvisioner(false)

	// --- phase 7: restart — journaled op is not repeated ---
	err = h.startProvisioner(filepath.Join(h.dir, "prov2.sock"), filepath.Join(h.dir, "prov-state2"),
		*filesMount, *filesUUID)
	if !h.okCheck("provisioner-restart", err) {
		return
	}
	opR := h.startJob(paA, "job:accept-restart", "/bin/sh", []string{"-c", "sleep 2; echo restart-survived"})
	if h.okCheck("start-restart-op", errOf(&opR)) {
		_, _ = h.waitState(paA, opR.OperationID, false, 30*time.Second)
		h.stopProvisioner(true) // SIGKILL mid-run — the crash window
		err = h.startProvisioner(filepath.Join(h.dir, "prov3.sock"), filepath.Join(h.dir, "prov-state2"),
			*filesMount, *filesUUID)
		if h.okCheck("provisioner-revive same state", err) {
			termR, rerr := h.waitState(paA, opR.OperationID, true, 90*time.Second)
			h.check("journaled op survives restart", rerr == nil, fmt.Sprint(rerr))
			h.check("restart op completes once", termR.State == runtimeprovision.ProcessSucceeded ||
				termR.State == runtimeprovision.ProcessFailed, string(termR.State))
			entries, _ := os.ReadDir(filepath.Join(h.stateDir, "processes"))
			n := 0
			for _, e := range entries {
				if strings.HasSuffix(e.Name(), ".json") {
					n++
				}
			}
			h.check("exactly one journaled op (no repeat launch)", n == 1, fmt.Sprintf("%d journal records", n))
		}
	}

	// --- phase 8: cancel physically stops the container ---
	opK := h.startJob(paA, "job:accept-cancel", "/bin/sh", []string{"-c", "sleep 60"})
	if h.okCheck("start-cancel-op", errOf(&opK)) {
		_, _ = h.waitState(paA, opK.OperationID, false, 30*time.Second)
		cancelCtx, cancelCancel := h.callCtx()
		_, cerr := h.proc.CancelProcess(cancelCtx, runtimeprovision.ProcessLookupRequest{
			PersonalityAgentID: paA, OperationID: opK.OperationID})
		cancelCancel()
		termK, _ := h.waitState(paA, opK.OperationID, true, 60*time.Second)
		h.check("cancel reaches terminal", cerr == nil &&
			(termK.State == runtimeprovision.ProcessCancelled || termK.State == runtimeprovision.ProcessFailed),
			fmt.Sprintf("cancel=%v state=%s", cerr, termK.State))
		deadline := time.Now().Add(30 * time.Second)
		gone := false
		for time.Now().Before(deadline) && !gone {
			lsCtx, lsCancel := context.WithTimeout(h.ctx, 15*time.Second)
			out, _ := exec.CommandContext(lsCtx, "docker", "container", "ls", "-a",
				"--filter", "name=^/sumi-process-"+opK.OperationID+"$", "--format", "{{.ID}}").Output()
			lsCancel()
			gone = strings.TrimSpace(string(out)) == ""
			time.Sleep(300 * time.Millisecond)
		}
		h.check("cancelled container removed", gone, "container still present")
	}
}

type startedOp struct {
	runtimeprovision.ProcessOperation
	startErr error
}

func errOf(o *startedOp) error { return o.startErr }

func (h *harness) startJob(paid, toolCall, exe string, args []string) startedOp {
	callCtx, cancel := h.callCtx()
	defer cancel()
	op, err := h.proc.StartProcess(callCtx, runtimeprovision.ProcessStartRequest{
		PersonalityAgentID:    paid,
		OriginatingToolCallID: toolCall,
		Executable:            exe,
		Args:                  args,
		TimeoutSeconds:        120,
		Image:                 "job",
		Workspace:             "files-scope",
	})
	if err == nil {
		h.ops = append(h.ops, op.OperationID)
	}
	return startedOp{op, err}
}

func (h *harness) okCheck(name string, err error) bool {
	h.check(name, err == nil, fmt.Sprint(err))
	return err == nil
}

// cleanup removes only what this run created: containers named for our
// operation IDs, and the owned provisioner. Scope directories are persona
// state — left in place and reported, matching product behavior for real
// personas (scopes are never deleted). It returns a recorded summary so a
// failed run still reports what was reaped and what (if anything) refused.
func (h *harness) cleanup() string {
	h.stopProvisioner(false)
	removed, failedRm := 0, []string{}
	for _, opID := range h.ops {
		ctx, cancel := context.WithTimeout(h.ctx, 20*time.Second)
		err := exec.CommandContext(ctx, "docker", "rm", "-f", "sumi-process-"+opID).Run()
		cancel()
		if err == nil {
			removed++
		} else {
			failedRm = append(failedRm, opID)
		}
	}
	parts := []string{fmt.Sprintf("provisioner stopped, %d owned containers removed", removed)}
	if len(failedRm) > 0 {
		parts = append(parts, "FAILED to remove (manual docker rm needed): "+strings.Join(failedRm, ", "))
	}
	if len(h.personas) > 0 {
		parts = append(parts, "leftover persona scopes (product state, safe to remove manually): "+strings.Join(h.personas, ", "))
	}
	return strings.Join(parts, "; ")
}
