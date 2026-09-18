//go:build integration

// Full-stack e2e: real Postgres job ledger → driver → real provisioner
// process service → real Docker, bind-mounted to a fixture canonical scope.
// It proves the whole contract end to end — admission, claim, launch,
// completion with real output, scope persistence, cancellation, and the
// lost-verdict reconcile (orphan stop + observed outcome) — without faking
// any persistence boundary.
//
// Requirements (all owned fixtures, opt-in):
//
//	SUMI_JOBEXEC_E2E=1
//	SUMI_JOBEXEC_E2E_MOUNT — directory shared with the Docker daemon's
//	  filesystem at the same absolute path (bind source for scopes)
//	SUMI_TEST_JOB_IMAGE_TAG — 40-hex revision of a present sumi-job image
//	SUMI_TEST_DB_URL — Postgres the test database helper creates under
//	docker CLI + socket reachable (the harness mounts both)

package jobexec

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

// fixtureEnsurer stands in for the file-service scope creation path: it
// makes the scope directory the way filesvc's mkdir would. The provisioner
// still runs its own mount/scope/binding verification on every launch.
type fixtureEnsurer struct{ mount string }

func (e fixtureEnsurer) EnsureScope(_ context.Context, personaID string) error {
	scope, err := fileaccess.ScopeForPersona(personaID)
	if err != nil {
		return err
	}
	dir := filepath.Join(e.mount, scope)
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return err
	}
	return os.Chmod(dir, 0o777)
}

func TestDriverE2ERealBackend(t *testing.T) {
	if os.Getenv("SUMI_JOBEXEC_E2E") != "1" {
		t.Skip("requires owned real-backend e2e opt-in")
	}
	mount := strings.TrimSpace(os.Getenv("SUMI_JOBEXEC_E2E_MOUNT"))
	if mount == "" {
		t.Fatal("SUMI_JOBEXEC_E2E_MOUNT required")
	}
	jobTag := os.Getenv("SUMI_TEST_JOB_IMAGE_TAG")
	if len(jobTag) != 40 {
		t.Fatal("SUMI_TEST_JOB_IMAGE_TAG must be a full 40-hex revision")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Fixture "files volume": the check script mirrors the real
	// sumi-files-check argv contract; the mount is a shared directory the
	// daemon can bind (the real JuiceFS mount/fstype/UUID verification is
	// the files settlement's own acceptance — see handback).
	checkPath := filepath.Join(mount, "fixture-files-check")
	check := `#!/bin/sh
if [ "$1" = "--wait" ]; then shift 2; fi
mnt="$1"; uuid="$2"; scope="$3"
[ -d "$mnt" ] || { echo "refused: not_mounted: $mnt" >&2; exit 2; }
[ -n "$uuid" ] || { echo "refused: usage" >&2; exit 2; }
dir="$mnt/$scope"
[ -d "$dir" ] || { echo "refused: scope_missing: $scope" >&2; exit 2; }
printf '%s\n' "$(cd "$dir" && pwd -P)"
`
	if err := os.WriteFile(checkPath, []byte(check), 0o755); err != nil {
		t.Fatal(err)
	}
	volumeUUID := "3f6f2d92-8b34-4f1a-9c8e-2c7f1a9b0d44"

	environment := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"),
		"SUMI_AGENT_IMAGE_TAG=" + jobTag, "SUMI_JOB_IMAGE_TAG=" + jobTag}
	for _, key := range []string{"DOCKER_HOST", "DOCKER_CONFIG"} {
		if v := os.Getenv(key); v != "" {
			environment = append(environment, key+"="+v)
		}
	}
	backend, err := runtimeprovision.NewDockerBackend(runtimeprovision.DockerBackendConfig{
		SupervisorPath:  "/bin/true", // process ops never invoke the supervisor
		BaseEnvironment: environment,
	})
	if err != nil {
		t.Fatal(err)
	}
	svc, err := runtimeprovision.NewService(backend, runtimeprovision.ServiceConfig{
		StateDirectory: t.TempDir() + "/state",
		Files: runtimeprovision.FilesEnvironment{
			Mountpoint:       mount,
			VolumeUUID:       volumeUUID,
			CheckPath:        checkPath,
			CheckWaitSeconds: 5,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	go svc.RunProcessObserver(ctx)

	pool := testdb.Create(t)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := agentstate.NewStore(pool)
	store.SetDefaultJobBackend("cloud")
	driver := New(store, svc, fixtureEnsurer{mount: mount}, Config{
		Interval:   300 * time.Millisecond,
		Lease:      2 * time.Minute,
		ClaimLimit: 2,
	})
	driverCtx, stopDriver := context.WithCancel(ctx)
	go driver.Run(driverCtx)

	pa := uuid.Must(uuid.NewV7()).String()
	if _, _, err := store.EnsurePersona(ctx, pa, nil, ""); err != nil {
		t.Fatal(err)
	}
	scopeDir := filepath.Join(mount, strings.ReplaceAll(pa, "-", ""))

	jobStatus := func(id string) agentstate.Job {
		t.Helper()
		j, err := store.GetJob(ctx, pa, id)
		if err != nil {
			t.Fatal(err)
		}
		return j
	}
	waitStatus := func(id string, want ...string) agentstate.Job {
		t.Helper()
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) {
			j := jobStatus(id)
			for _, w := range want {
				if j.Status == w {
					return j
				}
			}
			time.Sleep(300 * time.Millisecond)
		}
		t.Fatalf("job %s never reached %v (now %s)", id, want, jobStatus(id).Status)
		return agentstate.Job{}
	}
	containerNames := func() []string {
		t.Helper()
		raw, err := exec.Command("docker", "container", "ls", "-a",
			"--filter", "name=^/sumi-process-", "--format", "{{.Names}}").Output()
		if err != nil {
			t.Fatal(err)
		}
		return strings.Fields(string(raw))
	}

	// 1. Real command + build flow: gcc/make run in the pinned job image,
	// output lands in the canonical scope, the result is durable.
	if _, _, err := store.SubmitJob(ctx, pa, "e2e-build", "subprocess", map[string]any{
		"command": []any{"/bin/sh", "-c",
			`set -e; printf 'int main(void){return 0;}\n' > m.c; printf 'all: m\nm: m.c\n\t$(CC) -o m m.c\n' > Makefile; make && ./m && printf e2e-built > result.txt; echo build-ok`},
		"env":        map[string]any{"JOB_MARKER": "e2e-marker"},
		"timeout_ms": float64(120000),
	}, "assistant"); err != nil {
		t.Fatal(err)
	}
	j := waitStatus("e2e-build", "done", "failed")
	if j.Status != "done" {
		t.Fatalf("build job failed: %#v", j.Result)
	}
	if j.Result["exit_code"] != 0.0 || !strings.Contains(fmt.Sprint(j.Result["stdout"]), "build-ok") {
		t.Fatalf("unexpected result: %#v", j.Result)
	}
	for _, f := range []string{"m.c", "Makefile", "m", "result.txt"} {
		if _, err := os.Stat(filepath.Join(scopeDir, f)); err != nil {
			t.Fatalf("artifact %s missing from canonical scope: %v", f, err)
		}
	}

	// 2. Cancellation while running: never reports cancelled before the
	// backend is durably stopped and removed.
	if _, _, err := store.SubmitJob(ctx, pa, "e2e-cancel", "subprocess", map[string]any{
		"command": []any{"/bin/sh", "-c", "sleep 60"},
	}, "assistant"); err != nil {
		t.Fatal(err)
	}
	waitStatus("e2e-cancel", "running")
	if _, err := store.CancelJob(ctx, pa, "e2e-cancel"); err != nil {
		t.Fatal(err)
	}
	j = waitStatus("e2e-cancel", "cancelled", "failed")
	if j.Status != "cancelled" {
		t.Fatalf("cancel verdict: %s %#v", j.Status, j.Result)
	}
	cancelOp := "sumi-process-" + operationID(jobStatus("e2e-cancel"))
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		gone := true
		for _, name := range containerNames() {
			if name == cancelOp {
				gone = false
			}
		}
		if gone {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	for _, name := range containerNames() {
		if name == cancelOp {
			t.Fatalf("cancelled job container must be removed: %s", name)
		}
	}

	// 3. Lost-verdict reconcile: the driver's lease expires while it is
	// dead, a foreign claimant sweeps, and on restart the driver stops the
	// orphaned container and attaches the real backend outcome.
	stopDriver()
	time.Sleep(500 * time.Millisecond) // in-flight sweeps settle
	if _, _, err := store.SubmitJob(ctx, pa, "e2e-lost", "subprocess", map[string]any{
		"command": []any{"/bin/sh", "-c", "printf orphan-ran > orphan.txt; sleep 60"},
	}, "assistant"); err != nil {
		t.Fatal(err)
	}
	// The driver launched this op before "dying" — same deterministic op
	// identity the driver derives.
	lostJob := jobStatus("e2e-lost")
	if _, _, err := store.ClaimJobs(ctx, pa, "jobexec-docker", []string{"subprocess"}, 200*time.Millisecond, 1, "*"); err != nil {
		t.Fatal(err)
	}
	req, err := processRequest(lostJob)
	if err != nil {
		t.Fatal(err)
	}
	startOp, err := svc.StartProcess(ctx, req)
	if err != nil {
		t.Fatalf("orphan launch: %v", err)
	}
	// The launch is deferred to the observer loop — wait until the op is
	// actually running so the orphan write is durable before the sweep.
	// A fixed sleep races the deferred launch under load.
	launchDeadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(launchDeadline) {
		op, err := svc.ProcessStatus(ctx, runtimeprovision.ProcessLookupRequest{
			PersonalityAgentID: pa, OperationID: startOp.OperationID})
		if err != nil {
			t.Fatalf("orphan status: %v", err)
		}
		if op.State == runtimeprovision.ProcessRunning {
			break
		}
		switch op.State {
		case runtimeprovision.ProcessSucceeded, runtimeprovision.ProcessFailed,
			runtimeprovision.ProcessCancelled, runtimeprovision.ProcessIndeterminate:
			t.Fatalf("orphan op terminal before launch: %s", op.State)
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(400 * time.Millisecond) // lease expires
	if _, swept, err := store.ClaimJobs(ctx, pa, "foreign-script-runner", []string{"script"}, time.Minute, 4, "*"); err != nil {
		t.Fatal(err)
	} else if len(swept) != 1 || swept[0].JobID != "e2e-lost" {
		t.Fatalf("foreign sweep must mark the expired claim lost: %v", swept)
	}
	// "Restart": a fresh driver over the same store/backend reconciles.
	driver = New(store, svc, fixtureEnsurer{mount: mount}, Config{
		Interval: 300 * time.Millisecond, Lease: 2 * time.Minute, ClaimLimit: 2,
	})
	deadline = time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		driver.SweepOnce(ctx)
		j = jobStatus("e2e-lost")
		if j.Status == "lost" {
			if _, ok := j.Result["observed_outcome"]; ok {
				break
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	j = jobStatus("e2e-lost")
	if j.Status != "lost" {
		t.Fatalf("verdict must stay lost: %s", j.Status)
	}
	outcome, ok := j.Result["observed_outcome"].(map[string]any)
	if !ok {
		t.Fatalf("observed outcome must be attached: %#v", j.Result)
	}
	if outcome["state"] == "" {
		t.Fatalf("outcome must carry the backend state: %#v", outcome)
	}
	// The orphaned container was stopped — a lost verdict is never proof
	// the process is dead, so the reconcile must have killed it.
	lostOp := "sumi-process-" + operationID(j)
	for _, name := range containerNames() {
		if name == lostOp {
			raw, _ := exec.Command("docker", "inspect", "--format", "{{.State.Running}}", name).Output()
			if strings.TrimSpace(string(raw)) == "true" {
				t.Fatalf("orphan container still running after lost reconcile: %s", name)
			}
		}
	}
	// The write the orphan completed before its kill is real scope content.
	if _, err := os.Stat(filepath.Join(scopeDir, "orphan.txt")); err != nil {
		t.Fatalf("orphan's pre-kill write should persist in scope: %v", err)
	}
	t.Logf("e2e verified: build flow, scope persistence, cancellation, lost reconcile + orphan stop + observed outcome")
}

// flakyAPI wraps the real provisioner Service with scriptable failure
// injection. The dropped start response is the interesting case: the real
// call keeps running against the real journal in the background while the
// driver sees a transport error — exactly the lost-response window a
// client timeout produces on the socket transport.
type flakyAPI struct {
	svc       *runtimeprovision.Service
	dropStart int
	statErr   error
	readErr   error
	mu        chan struct{}
}

func newFlaky(svc *runtimeprovision.Service) *flakyAPI {
	return &flakyAPI{svc: svc, mu: make(chan struct{}, 1)}
}

func (f *flakyAPI) StartProcess(ctx context.Context, r runtimeprovision.ProcessStartRequest) (runtimeprovision.ProcessOperation, error) {
	f.mu <- struct{}{}
	drop := f.dropStart
	if drop > 0 {
		f.dropStart--
	}
	<-f.mu
	if drop > 0 {
		// The real request continues against the real journal; only the
		// response is lost.
		go func() {
			bg := context.Background()
			_, _ = f.svc.StartProcess(bg, r)
		}()
		return runtimeprovision.ProcessOperation{}, context.DeadlineExceeded
	}
	return f.svc.StartProcess(ctx, r)
}

func (f *flakyAPI) ProcessStatus(ctx context.Context, r runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error) {
	f.mu <- struct{}{}
	err := f.statErr
	<-f.mu
	if err != nil {
		return runtimeprovision.ProcessOperation{}, err
	}
	return f.svc.ProcessStatus(ctx, r)
}

func (f *flakyAPI) ReadProcessOutput(ctx context.Context, r runtimeprovision.ProcessOutputRequest) (runtimeprovision.ProcessOutput, error) {
	f.mu <- struct{}{}
	err := f.readErr
	<-f.mu
	if err != nil {
		return runtimeprovision.ProcessOutput{}, err
	}
	return f.svc.ReadProcessOutput(ctx, r)
}

func (f *flakyAPI) CancelProcess(ctx context.Context, r runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error) {
	return f.svc.CancelProcess(ctx, r)
}

// Real-backend recovery boundaries: a lost StartProcess response and a
// long status outage both resolve through the real journal — the job is
// never failed on ambiguity and never relaunched.
//
//	SUMI_JOBEXEC_E2E=1, SUMI_JOBEXEC_E2E_MOUNT, SUMI_TEST_JOB_IMAGE_TAG,
//	SUMI_TEST_DB_URL — same fixture contract as TestDriverE2ERealBackend.
func TestDriverE2ERecoveryBoundaries(t *testing.T) {
	if os.Getenv("SUMI_JOBEXEC_E2E") != "1" {
		t.Skip("requires owned real-backend e2e opt-in")
	}
	mount := strings.TrimSpace(os.Getenv("SUMI_JOBEXEC_E2E_MOUNT"))
	if mount == "" {
		t.Fatal("SUMI_JOBEXEC_E2E_MOUNT required")
	}
	jobTag := os.Getenv("SUMI_TEST_JOB_IMAGE_TAG")
	if len(jobTag) != 40 {
		t.Fatal("SUMI_TEST_JOB_IMAGE_TAG must be a full 40-hex revision")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	checkPath := filepath.Join(mount, "fixture-files-check")
	check := `#!/bin/sh
if [ "$1" = "--wait" ]; then shift 2; fi
mnt="$1"; uuid="$2"; scope="$3"
[ -d "$mnt" ] || { echo "refused: not_mounted: $mnt" >&2; exit 2; }
dir="$mnt/$scope"
[ -d "$dir" ] || { echo "refused: scope_missing: $scope" >&2; exit 2; }
printf '%s\n' "$(cd "$dir" && pwd -P)"
`
	if err := os.WriteFile(checkPath, []byte(check), 0o755); err != nil {
		t.Fatal(err)
	}
	environment := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"),
		"SUMI_AGENT_IMAGE_TAG=" + jobTag, "SUMI_JOB_IMAGE_TAG=" + jobTag}
	for _, key := range []string{"DOCKER_HOST", "DOCKER_CONFIG"} {
		if v := os.Getenv(key); v != "" {
			environment = append(environment, key+"="+v)
		}
	}
	backend, err := runtimeprovision.NewDockerBackend(runtimeprovision.DockerBackendConfig{
		SupervisorPath:  "/bin/true",
		BaseEnvironment: environment,
	})
	if err != nil {
		t.Fatal(err)
	}
	stateDir := t.TempDir() + "/state"
	svc, err := runtimeprovision.NewService(backend, runtimeprovision.ServiceConfig{
		StateDirectory: stateDir,
		Files: runtimeprovision.FilesEnvironment{
			Mountpoint:       mount,
			VolumeUUID:       "3f6f2d92-8b34-4f1a-9c8e-2c7f1a9b0d44",
			CheckPath:        checkPath,
			CheckWaitSeconds: 5,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	go svc.RunProcessObserver(ctx)

	pool := testdb.Create(t)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := agentstate.NewStore(pool)
	store.SetDefaultJobBackend("cloud")
	flaky := newFlaky(svc)
	driver := New(store, flaky, fixtureEnsurer{mount: mount}, Config{
		Interval: 250 * time.Millisecond, Lease: 2 * time.Minute, ClaimLimit: 2,
	})
	driverCtx, stopDriver := context.WithCancel(ctx)
	defer stopDriver()
	go driver.Run(driverCtx)

	pa := uuid.Must(uuid.NewV7()).String()
	if _, _, err := store.EnsurePersona(ctx, pa, nil, ""); err != nil {
		t.Fatal(err)
	}
	waitStatus := func(id string, want ...string) agentstate.Job {
		t.Helper()
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) {
			j, err := store.GetJob(ctx, pa, id)
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range want {
				if j.Status == w {
					return j
				}
			}
			time.Sleep(250 * time.Millisecond)
		}
		j, _ := store.GetJob(ctx, pa, id)
		t.Fatalf("job %s never reached %v (now %s)", id, want, j.Status)
		return agentstate.Job{}
	}

	// 1. Lost StartProcess response: the driver sees a transport error but
	// the real request journals and launches. The job must recover through
	// status — never failed, never relaunched (one journal record).
	flaky.mu <- struct{}{}
	flaky.dropStart = 1
	<-flaky.mu
	if _, _, err := store.SubmitJob(ctx, pa, "e2e-lostresp", "subprocess", map[string]any{
		"command": []any{"/bin/sh", "-c", "printf response-recovered"},
	}, "assistant"); err != nil {
		t.Fatal(err)
	}
	j := waitStatus("e2e-lostresp", "done", "failed")
	if j.Status != "done" {
		t.Fatalf("lost start response must recover to done: %s %#v", j.Status, j.Result)
	}
	if got := fmt.Sprint(j.Result["stdout"]); got != "response-recovered" {
		t.Fatalf("real output must survive the lost response: %q", got)
	}
	journalEntries, err := os.ReadDir(filepath.Join(stateDir, "processes"))
	if err != nil {
		t.Fatal(err)
	}
	ops := 0
	for _, e := range journalEntries {
		if strings.HasSuffix(e.Name(), ".json") {
			ops++
		}
	}
	if ops != 1 {
		t.Fatalf("lost response must produce exactly one journaled op, got %d", ops)
	}

	// 2. Long status outage: the op runs to completion while every status
	// call fails. The claim stays heartbeated and the job is still
	// running — never failed on an ambiguous answer — and once the
	// backend answers the real terminal outcome lands.
	if _, _, err := store.SubmitJob(ctx, pa, "e2e-outage", "subprocess", map[string]any{
		"command": []any{"/bin/sh", "-c", "sleep 1; printf outage-ok"},
	}, "assistant"); err != nil {
		t.Fatal(err)
	}
	waitStatus("e2e-outage", "running")
	flaky.mu <- struct{}{}
	flaky.statErr = errors.New("provisioner unreachable")
	<-flaky.mu
	// Hold the outage across several sweeps: job must stay running with
	// the durable unknown-wait recorded, never failed.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		j = jobStatusOf(t, store, pa, "e2e-outage")
		if j.Status == "failed" {
			t.Fatalf("status outage must never fail the job: %#v", j.Result)
		}
		time.Sleep(300 * time.Millisecond)
	}
	j = jobStatusOf(t, store, pa, "e2e-outage")
	if j.Status != "running" {
		t.Fatalf("outage job must still be running, got %s", j.Status)
	}
	cause, _, ok := agentstate.JobWait(j)
	if !ok || cause != "unknown" {
		t.Fatalf("durable unknown wait must be recorded during outage: %#v", j.Result)
	}
	flaky.mu <- struct{}{}
	flaky.statErr = nil
	<-flaky.mu
	j = waitStatus("e2e-outage", "done", "failed")
	if j.Status != "done" || fmt.Sprint(j.Result["stdout"]) != "outage-ok" {
		t.Fatalf("outage must recover the real outcome: %s %#v", j.Status, j.Result)
	}
	t.Logf("e2e verified: lost start response recovery, status outage hold + recovery, single journal record")
}

func jobStatusOf(t *testing.T, store *agentstate.Store, pa, id string) agentstate.Job {
	t.Helper()
	j, err := store.GetJob(context.Background(), pa, id)
	if err != nil {
		t.Fatal(err)
	}
	return j
}
