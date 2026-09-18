//go:build integration

// Real-Postgres driver tests: the driver's claim/monitor/complete cycle
// runs against the production agentstate store; the backend is a fake
// ProcessAPI that records calls and lets each test script the operation
// lifecycle. The real-Docker integration lives in e2e_test.go.

package jobexec

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

func newStore(t *testing.T) *agentstate.Store {
	t.Helper()
	pool := testdb.Create(t)
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	s := agentstate.NewStore(pool)
	// Driver tests run the Cloud claim path: stamp submitted jobs
	// backend:"cloud" the way jobexecFromEnv does for a live driver.
	s.SetDefaultJobBackend("cloud")
	return s
}

func pid(t *testing.T) string {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid: %v", err)
	}
	return id.String()
}

func mustPersona(t *testing.T, s *agentstate.Store, id string) {
	t.Helper()
	if _, _, err := s.EnsurePersona(context.Background(), id, nil, ""); err != nil {
		t.Fatalf("persona: %v", err)
	}
}

func subReq(argv ...string) map[string]any {
	a := make([]any, len(argv))
	for i, s := range argv {
		a[i] = s
	}
	return map[string]any{"command": a}
}

// fakeProc scripts ProcessAPI behavior. ops maps operationID → scripted op;
// statusErr/startErr inject transient failures.
type fakeProc struct {
	mu       sync.Mutex
	ops      map[string]*runtimeprovision.ProcessOperation
	started  []runtimeprovision.ProcessStartRequest
	canceled []string
	outputs  map[string][2]string // opID → stdout,stderr
	startErr error
	statErr  error
	readErr  error
	// startLostN > 0 journals the op but returns a transport error — the
	// lost-response window: the request landed, the answer did not.
	startLostN int
	// startBlock, when non-nil, holds StartProcess inside its pre-journal
	// window — the fake checked the op absent but has not recorded it —
	// modeling the real service's workspace-resolution gap. startEntered
	// is closed when the block begins so tests can land fences mid-flight.
	startBlock, startEntered chan struct{}
	// onStart is invoked after a successful StartProcess; tests use it to
	// advance scripted state or simulate a lost response.
	onStart func(*fakeProc)
	// onAbsent runs inside ProcessStatus when the op is missing — the
	// deterministic stale-read window: a delayed start that journals
	// between the driver's status read and its fence call. It executes
	// under f.mu, so hooks mutate f.ops directly and clear the field to
	// fire once. The status read still reports absent — it genuinely
	// happened before the materialization.
	onAbsent func(f *fakeProc, r runtimeprovision.ProcessLookupRequest)
	// realCancel mirrors the provisioner's exact contract: CancelProcess
	// marks CancelRequested and returns the op still LIVE — the terminal
	// transition lands through observation, not the cancel call itself.
	// Tests that script the fence's returned operation need this fidelity;
	// the default immediate-terminal behavior stays for older tests.
	realCancel bool
}

func newFakeProc() *fakeProc {
	return &fakeProc{ops: map[string]*runtimeprovision.ProcessOperation{}, outputs: map[string][2]string{}}
}

func (f *fakeProc) StartProcess(ctx context.Context, r runtimeprovision.ProcessStartRequest) (runtimeprovision.ProcessOperation, error) {
	id := runtimeprovision.ProcessOperationID(r.PersonalityAgentID, r.OriginatingToolCallID)
	f.mu.Lock()
	if f.startErr != nil {
		f.mu.Unlock()
		return runtimeprovision.ProcessOperation{}, f.startErr
	}
	if op, ok := f.ops[id]; ok {
		f.mu.Unlock()
		return *op, nil
	}
	block, entered := f.startBlock, f.startEntered
	f.mu.Unlock()
	if block != nil {
		// The pre-journal window: the op was checked absent but nothing is
		// recorded yet. A fence landing now must win — mirroring the real
		// journal's serialized create.
		close(entered)
		<-block
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if op, ok := f.ops[id]; ok {
		// A tombstone or a racing create landed while we were in flight.
		return *op, nil
	}
	op := &runtimeprovision.ProcessOperation{
		OperationID:           id,
		PersonalityAgentID:    r.PersonalityAgentID,
		OriginatingToolCallID: r.OriginatingToolCallID,
		Executable:            r.Executable,
		Args:                  r.Args,
		Cwd:                   r.Cwd,
		TimeoutSeconds:        r.TimeoutSeconds,
		Env:                   r.Env,
		Image:                 r.Image,
		WorkspaceBind:         "/mnt/files/" + strings.ReplaceAll(r.PersonalityAgentID, "-", ""),
		FilesVolumeUUID:       "5a1b72bc-b9ec-4a72-813b-2f4dc4cd6d07",
		State:                 runtimeprovision.ProcessRunning,
	}
	f.ops[id] = op
	f.started = append(f.started, r)
	if f.startLostN > 0 {
		f.startLostN--
		return runtimeprovision.ProcessOperation{}, errors.New("transport: response lost")
	}
	if f.onStart != nil {
		cb := f.onStart
		go cb(f)
	}
	return *op, nil
}

func (f *fakeProc) ProcessStatus(ctx context.Context, r runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.statErr != nil {
		return runtimeprovision.ProcessOperation{}, f.statErr
	}
	op, ok := f.ops[r.OperationID]
	if !ok || op.PersonalityAgentID != r.PersonalityAgentID {
		if f.onAbsent != nil {
			f.onAbsent(f, r)
		}
		return runtimeprovision.ProcessOperation{}, runtimeprovision.ErrProcessNotFound
	}
	return *op, nil
}

func (f *fakeProc) ReadProcessOutput(ctx context.Context, r runtimeprovision.ProcessOutputRequest) (runtimeprovision.ProcessOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.readErr != nil {
		return runtimeprovision.ProcessOutput{}, f.readErr
	}
	io := f.outputs[r.OperationID]
	content := io[0]
	if r.Stream == "stderr" {
		content = io[1]
	}
	out := runtimeprovision.ProcessOutput{OperationID: r.OperationID, Stream: r.Stream, EOF: true}
	if r.Limit > 0 && len(content) > r.Limit {
		out.Content = content[:r.Limit]
		out.EOF = false
		return out, nil
	}
	out.Content = content
	return out, nil
}

func (f *fakeProc) CancelProcess(ctx context.Context, r runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	op, ok := f.ops[r.OperationID]
	if !ok {
		if !r.TombstoneIfAbsent {
			return runtimeprovision.ProcessOperation{}, runtimeprovision.ErrProcessNotFound
		}
		// Mirror the real journal's cancel fence: the deterministic op id
		// gets a durable terminal record, and the fake's StartProcess
		// replay (f.ops[id] hit above) returns it — no launch, ever.
		op = &runtimeprovision.ProcessOperation{
			OperationID:           r.OperationID,
			PersonalityAgentID:    r.PersonalityAgentID,
			OriginatingToolCallID: r.OriginatingToolCallID,
			State:                 runtimeprovision.ProcessCancelled,
			Tombstone:             true,
		}
		f.ops[r.OperationID] = op
		f.canceled = append(f.canceled, r.OperationID)
		return *op, nil
	}
	f.canceled = append(f.canceled, r.OperationID)
	switch op.State {
	case runtimeprovision.ProcessAccepted, runtimeprovision.ProcessRunning:
		if f.realCancel {
			// The real journal returns the op still live — the terminal
			// transition is observed, not written by the cancel call.
			// f.canceled above already records that the cancel landed.
			return *op, nil
		}
		op.State = runtimeprovision.ProcessCancelled
		now := time.Now()
		op.FinishedAt = &now
	}
	return *op, nil
}

// finish advances a scripted op to a terminal state with output.
func (f *fakeProc) finish(opID string, state runtimeprovision.ProcessState, exit int, stdout, stderr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	op := f.ops[opID]
	if op == nil {
		return
	}
	now := time.Now()
	started := now.Add(-time.Second)
	op.State = state
	op.ExitCode = &exit
	op.StartedAt = &started
	op.FinishedAt = &now
	f.outputs[opID] = [2]string{stdout, stderr}
}

func (f *fakeProc) setState(opID string, state runtimeprovision.ProcessState, errText string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if op := f.ops[opID]; op != nil {
		op.State = state
		op.Error = errText
		now := time.Now()
		op.FinishedAt = &now
	}
}

func newDriver(s *agentstate.Store, proc ProcessAPI, scope ScopeEnsurer) *Driver {
	return New(s, proc, scope, Config{Lease: 90 * time.Second, Interval: time.Second})
}

// A claimed job launches once, gets monitored, and completes done with the
// real bounded output in the result.
func TestDriverClaimLaunchComplete(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	proc := newFakeProc()
	scope := &countingScope{}
	d := newDriver(s, proc, scope)

	if _, _, err := s.SubmitJob(ctx, pa, "j-run", "subprocess", subReq("make", "hello"), "assistant"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx)

	proc.mu.Lock()
	if len(proc.started) != 1 {
		t.Fatalf("expected one launch, got %d", len(proc.started))
	}
	req := proc.started[0]
	if req.Image != "job" || req.Workspace != "files-scope" {
		t.Fatalf("request must pin job image and files scope: %+v", req)
	}
	if req.Executable != "make" || len(req.Args) != 1 || req.Args[0] != "hello" {
		t.Fatalf("argv mapping: %+v", req)
	}
	opID := runtimeprovision.ProcessOperationID(pa, "job:j-run")
	proc.mu.Unlock()
	if scope.calls != 1 {
		t.Fatalf("scope ensured once before launch, got %d", scope.calls)
	}
	if j, _ := s.GetJob(ctx, pa, "j-run"); j.Status != "running" {
		t.Fatalf("job should be running, got %s", j.Status)
	}

	// Second sweep while running: monitored, no second launch, job alive.
	d.SweepOnce(ctx)
	proc.mu.Lock()
	if len(proc.started) != 1 {
		t.Fatalf("relaunch detected: %d starts", len(proc.started))
	}
	proc.mu.Unlock()

	proc.finish(opID, runtimeprovision.ProcessSucceeded, 0, "built\n", "")
	d.SweepOnce(ctx)
	j, err := s.GetJob(ctx, pa, "j-run")
	if err != nil {
		t.Fatal(err)
	}
	if j.Status != "done" {
		t.Fatalf("job should be done, got %s (err=%v)", j.Status, j.Error)
	}
	if j.Result["stdout"] != "built\n" || j.Result["exit_code"] != 0.0 {
		t.Fatalf("result must carry real output: %#v", j.Result)
	}
	if j.Result["operation_id"] != opID || j.Result["backend"] != "docker-process" {
		t.Fatalf("result must carry backend evidence: %#v", j.Result)
	}
}

// A job that never reaches the backend launches on reconcile; a job whose
// op exists is never re-launched (no silent repeat after ambiguous launch).
func TestDriverRestartNoRelaunch(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	proc := newFakeProc()
	d := newDriver(s, proc, nil)

	if _, _, err := s.SubmitJob(ctx, pa, "j-re", "subprocess", subReq("echo", "x"), "assistant"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx) // claimed + launched
	opID := runtimeprovision.ProcessOperationID(pa, "job:j-re")

	// Driver "restarts": a NEW Driver on the same store must reconcile the
	// claim and NOT launch again.
	d2 := newDriver(s, proc, nil)
	d2.SweepOnce(ctx)
	proc.mu.Lock()
	if len(proc.started) != 1 {
		t.Fatalf("restart must not relaunch: %d starts", len(proc.started))
	}
	proc.mu.Unlock()

	// Kill the journal entry to simulate "start response lost before it
	// ever landed": op absent → the restarted driver launches it (first
	// real launch).
	delete(proc.ops, opID)
	d3 := newDriver(s, proc, nil)
	d3.SweepOnce(ctx)
	proc.mu.Lock()
	if len(proc.started) != 2 {
		t.Fatalf("absent op must launch once: %d starts", len(proc.started))
	}
	proc.mu.Unlock()
}

// Expired-but-unswept claims are revived by heartbeat; a job swept by a
// foreign claimant keeps its lost verdict, gets its orphan cancelled, and
// the real terminal outcome lands under observed_outcome.
func TestDriverLostVerdictAndObservedOutcome(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	proc := newFakeProc()
	d := New(s, proc, nil, Config{Lease: 60 * time.Millisecond})

	if _, _, err := s.SubmitJob(ctx, pa, "j-lost", "subprocess", subReq("sleep", "1"), "assistant"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx) // claim + launch
	opID := runtimeprovision.ProcessOperationID(pa, "job:j-lost")

	// Claim expires — margin must survive a loaded CI box.
	time.Sleep(300 * time.Millisecond)
	// Foreign claimant's claim pass sweeps the expired claim — kind-blind.
	if _, swept, err := s.ClaimJobs(ctx, pa, "script-runner", []string{"script"}, time.Minute, 4, "*"); err != nil {
		t.Fatal(err)
	} else if len(swept) != 1 {
		t.Fatalf("expected foreign sweep, got %v", swept)
	}

	// Backend finished while the claim was swept.
	proc.finish(opID, runtimeprovision.ProcessSucceeded, 0, "real result\n", "")
	d.SweepOnce(ctx)

	j, err := s.GetJob(ctx, pa, "j-lost")
	if err != nil {
		t.Fatal(err)
	}
	if j.Status != "lost" {
		t.Fatalf("verdict must stay lost, got %s", j.Status)
	}
	out, ok := j.Result["observed_outcome"].(map[string]any)
	if !ok {
		t.Fatalf("observed outcome must be attached: %#v", j.Result)
	}
	if out["state"] != "succeeded" || out["stdout"] != "real result\n" {
		t.Fatalf("observed outcome must carry the real terminal state: %#v", out)
	}

	// Orphan reaping: a still-running op on a lost job is cancelled.
	if _, _, err := s.SubmitJob(ctx, pa, "j-orph", "subprocess", subReq("sleep", "99"), "assistant"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx)
	orphID := runtimeprovision.ProcessOperationID(pa, "job:j-orph")
	if _, _, err := s.ClaimJobs(ctx, pa, "other", []string{"subprocess"}, time.Millisecond, 4, "*"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, _, err := s.ClaimJobs(ctx, pa, "other2", []string{"subprocess"}, time.Minute, 4, "*"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx)
	proc.mu.Lock()
	found := false
	for _, id := range proc.canceled {
		if id == orphID {
			found = true
		}
	}
	proc.mu.Unlock()
	if !found {
		t.Fatal("orphaned live op must be cancelled for the lost job")
	}
}

// Cancellation before launch completes cancelled with never_started;
// cancellation while running is relayed and the job completes cancelled
// only after the backend reaches a terminal state.
func TestDriverCancelPaths(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	proc := newFakeProc()
	d := newDriver(s, proc, nil)

	if _, _, err := s.SubmitJob(ctx, pa, "j-c1", "subprocess", subReq("sleep", "60"), "assistant"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx)
	opID := runtimeprovision.ProcessOperationID(pa, "job:j-c1")

	if _, err := s.CancelJob(ctx, pa, "j-c1"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx) // relay cancel to the live op
	proc.mu.Lock()
	if len(proc.canceled) != 1 || proc.canceled[0] != opID {
		t.Fatalf("cancel must reach the op: %v", proc.canceled)
	}
	proc.mu.Unlock()
	// Job stays cancel_requested until the backend is terminal — never a
	// false terminal while the process could still write.
	if j, _ := s.GetJob(ctx, pa, "j-c1"); j.Status != "cancel_requested" {
		t.Fatalf("job must wait for backend terminal, got %s", j.Status)
	}
	proc.setState(opID, runtimeprovision.ProcessCancelled, "")
	d.SweepOnce(ctx)
	if j, _ := s.GetJob(ctx, pa, "j-c1"); j.Status != "cancelled" {
		t.Fatalf("job should be cancelled, got %s", j.Status)
	}
}

// Cancel arriving while the job was claimed but never launched lands the
// tombstone fence and completes 'never started' immediately — the fence,
// not a grace timer, is what makes the verdict publishable. A start that
// arrives afterward replays the tombstone and never executes.
func TestDriverCancelBeforeLaunch(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	proc := newFakeProc()
	proc.startErr = errors.New("provisioner down")
	d := New(s, proc, nil, Config{Lease: 90 * time.Second})

	if _, _, err := s.SubmitJob(ctx, pa, "j-c2", "subprocess", subReq("echo", "x"), "assistant"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx) // claims; launch attempt fails (backend down)
	if _, err := s.CancelJob(ctx, pa, "j-c2"); err != nil {
		t.Fatal(err)
	}
	proc.mu.Lock()
	proc.startErr = nil
	proc.mu.Unlock()
	// One sweep: status says absent → the tombstone fence lands → the
	// never-started verdict commits right away.
	d.SweepOnce(ctx)
	j, _ := s.GetJob(ctx, pa, "j-c2")
	if j.Status != "cancelled" {
		t.Fatalf("cancel-before-launch must complete cancelled behind the fence, got %s", j.Status)
	}
	if j.Result["outcome"] != "never_started" {
		t.Fatalf("result must record never_started: %#v", j.Result)
	}
	// A delayed start arriving after the verdict replays the fence — it
	// records no launch, and nothing executes.
	opID := runtimeprovision.ProcessOperationID(pa, "job:j-c2")
	op, err := proc.StartProcess(ctx, runtimeprovision.ProcessStartRequest{
		PersonalityAgentID:    pa,
		OriginatingToolCallID: "job:j-c2",
		Executable:            "echo", Args: []string{"x"},
	})
	if err != nil || !op.Tombstone || op.OperationID != opID {
		t.Fatalf("late start must replay the tombstone: %+v %v", op, err)
	}
	proc.mu.Lock()
	defer proc.mu.Unlock()
	if len(proc.started) != 0 {
		t.Fatalf("cancelled-before-launch job must never execute: %d starts", len(proc.started))
	}
}

// A deterministic refusal (workspace or invalid request) fails the job
// immediately instead of looping; a busy backend holds the claim until it
// can start.
func TestDriverRefusalAndBusy(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	proc := newFakeProc()
	proc.startErr = runtimeprovision.ErrProcessWorkspace
	d := newDriver(s, proc, nil)

	if _, _, err := s.SubmitJob(ctx, pa, "j-ws", "subprocess", subReq("echo", "x"), "assistant"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx)
	j, _ := s.GetJob(ctx, pa, "j-ws")
	if j.Status != "failed" {
		t.Fatalf("workspace refusal must fail the job, got %s", j.Status)
	}

	// Busy backend holds the claim and starts when capacity returns.
	proc.mu.Lock()
	proc.startErr = runtimeprovision.ErrProcessBusy
	proc.mu.Unlock()
	if _, _, err := s.SubmitJob(ctx, pa, "j-busy", "subprocess", subReq("echo", "x"), "assistant"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx)
	j, _ = s.GetJob(ctx, pa, "j-busy")
	if j.Status != "running" {
		t.Fatalf("busy backend holds the claim running, got %s", j.Status)
	}
	proc.mu.Lock()
	proc.startErr = nil
	proc.mu.Unlock()
	d.SweepOnce(ctx)
	proc.mu.Lock()
	if len(proc.started) != 1 {
		t.Fatalf("job must start once capacity returns: %d", len(proc.started))
	}
	proc.mu.Unlock()
}

// Timeout maps to failed with timed_out evidence.
func TestDriverTimeout(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	proc := newFakeProc()
	d := newDriver(s, proc, nil)

	if _, _, err := s.SubmitJob(ctx, pa, "j-to", "subprocess", subReq("sleep", "999"), "assistant"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx)
	opID := runtimeprovision.ProcessOperationID(pa, "job:j-to")
	proc.setState(opID, runtimeprovision.ProcessFailed, "process timeout exceeded")
	d.SweepOnce(ctx)
	j, _ := s.GetJob(ctx, pa, "j-to")
	if j.Status != "failed" {
		t.Fatalf("timeout must fail, got %s", j.Status)
	}
	if j.Result["timed_out"] != true {
		t.Fatalf("timeout evidence missing: %#v", j.Result)
	}
}

type countingScope struct{ calls int }

func (c *countingScope) EnsureScope(ctx context.Context, personaID string) error {
	c.calls++
	return nil
}

// A StartProcess whose response is lost after the request journaled is
// indeterminate, not a refusal: the job holds durably (runner_wait is in
// the row, not in RAM), the next sweep finds the operation, and the work
// completes — with exactly one launch and no 'failed' detour.
func TestDriverLostStartResponseRecovers(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	proc := newFakeProc()
	proc.startLostN = 1
	d := newDriver(s, proc, nil)

	if _, _, err := s.SubmitJob(ctx, pa, "j-lr", "subprocess", subReq("echo", "x"), "assistant"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx) // claim + start; the response is lost
	j, _ := s.GetJob(ctx, pa, "j-lr")
	if j.Status != "running" {
		t.Fatalf("lost response must hold the claim, got %s", j.Status)
	}
	cause, _, ok := agentstate.JobWait(j)
	if !ok || cause != "unknown" {
		t.Fatalf("indeterminate wait must be recorded durably: %#v", j.Result)
	}
	opID := runtimeprovision.ProcessOperationID(pa, "job:j-lr")
	d.SweepOnce(ctx) // status now finds the journaled op — recovery, no rerun
	proc.mu.Lock()
	starts := len(proc.started)
	proc.mu.Unlock()
	if starts != 1 {
		t.Fatalf("delayed-accepted op must not relaunch: %d starts", starts)
	}
	proc.finish(opID, runtimeprovision.ProcessSucceeded, 0, "landed\n", "")
	d.SweepOnce(ctx)
	j, _ = s.GetJob(ctx, pa, "j-lr")
	if j.Status != "done" || j.Result["stdout"] != "landed\n" {
		t.Fatalf("lost-response job must recover to done: %s %#v", j.Status, j.Result)
	}
	if _, _, ok := agentstate.JobWait(j); ok {
		t.Fatal("terminal result must not carry wait bookkeeping")
	}
}

// A long status outage is not proof of a stopped process: the claim is
// heartbeated while indeterminate and the job recovers when the backend
// answers — never silently failed out of discovery.
func TestDriverStatusOutageHoldsThenRecovers(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	proc := newFakeProc()
	d := newDriver(s, proc, nil)

	if _, _, err := s.SubmitJob(ctx, pa, "j-out", "subprocess", subReq("echo", "x"), "assistant"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx)
	opID := runtimeprovision.ProcessOperationID(pa, "job:j-out")

	proc.mu.Lock()
	proc.statErr = errors.New("provisioner unreachable")
	proc.mu.Unlock()
	for i := 0; i < 3; i++ {
		d.SweepOnce(ctx)
		j, _ := s.GetJob(ctx, pa, "j-out")
		if j.Status != "running" {
			t.Fatalf("outage must never fail the job, got %s", j.Status)
		}
		cause, _, ok := agentstate.JobWait(j)
		if !ok || cause != "unknown" {
			t.Fatalf("durable unknown wait must be recorded: %#v", j.Result)
		}
		if j.ClaimExpiresAt == nil || j.ClaimExpiresAt.Before(time.Now()) {
			t.Fatal("claim must stay heartbeated during the outage")
		}
	}
	proc.mu.Lock()
	proc.statErr = nil
	proc.mu.Unlock()
	proc.finish(opID, runtimeprovision.ProcessSucceeded, 0, "recovered\n", "")
	d.SweepOnce(ctx)
	j, _ := s.GetJob(ctx, pa, "j-out")
	if j.Status != "done" || j.Result["stdout"] != "recovered\n" {
		t.Fatalf("outage must recover, got %s %#v", j.Status, j.Result)
	}
}

// When the indeterminate wait outlives its bound the claim stops renewing
// and the sweep commits 'lost' — where reconcileLost attaches the real
// outcome. The job is never failed on an ambiguous answer.
func TestDriverStatusOutageReleasesToLost(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	proc := newFakeProc()
	d := New(s, proc, nil, Config{
		Lease: 60 * time.Millisecond, UnknownWait: 30 * time.Millisecond,
		Interval: 10 * time.Millisecond,
	})

	if _, _, err := s.SubmitJob(ctx, pa, "j-rel", "subprocess", subReq("sleep", "5"), "assistant"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx) // claim + launch
	opID := runtimeprovision.ProcessOperationID(pa, "job:j-rel")
	proc.mu.Lock()
	proc.statErr = errors.New("provisioner unreachable")
	proc.mu.Unlock()

	d.SweepOnce(ctx) // records the durable unknown wait
	time.Sleep(150 * time.Millisecond)
	d.SweepOnce(ctx) // over bound: released, no heartbeat; released pass sweeps
	j, _ := s.GetJob(ctx, pa, "j-rel")
	if j.Status == "failed" {
		t.Fatal("indeterminate execution must never be failed")
	}
	// The released claim expires and the driver's released-claim pass
	// sweeps it to lost.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && j.Status == "running" {
		d.SweepOnce(ctx)
		j, _ = s.GetJob(ctx, pa, "j-rel")
		time.Sleep(30 * time.Millisecond)
	}
	if j.Status != "lost" {
		t.Fatalf("released claim must sweep to lost, got %s", j.Status)
	}
	// Backend answers again: reconcileLost attaches the real outcome.
	proc.mu.Lock()
	proc.statErr = nil
	proc.mu.Unlock()
	proc.finish(opID, runtimeprovision.ProcessSucceeded, 0, "was alive\n", "")
	d.SweepOnce(ctx)
	j, _ = s.GetJob(ctx, pa, "j-rel")
	out, ok := j.Result["observed_outcome"].(map[string]any)
	if j.Status != "lost" || !ok || out["state"] != "succeeded" {
		t.Fatalf("lost job must carry its real outcome: %s %#v", j.Status, j.Result)
	}
}

// Output that cannot be read yet does not blank the result: the job holds
// (durably), then completes with the real content once reads recover.
func TestDriverOutputOutageThenRecovery(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	proc := newFakeProc()
	d := newDriver(s, proc, nil)

	if _, _, err := s.SubmitJob(ctx, pa, "j-or", "subprocess", subReq("echo", "x"), "assistant"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx)
	opID := runtimeprovision.ProcessOperationID(pa, "job:j-or")
	proc.finish(opID, runtimeprovision.ProcessSucceeded, 0, "real output\n", "")
	proc.mu.Lock()
	proc.readErr = errors.New("journal read unavailable")
	proc.mu.Unlock()
	d.SweepOnce(ctx)
	j, _ := s.GetJob(ctx, pa, "j-or")
	if j.Status != "running" {
		t.Fatalf("unread output must hold, not commit blank: %s %#v", j.Status, j.Result)
	}
	cause, _, ok := agentstate.JobWait(j)
	if !ok || cause != "output" {
		t.Fatalf("output wait must be recorded: %#v", j.Result)
	}
	proc.mu.Lock()
	proc.readErr = nil
	proc.mu.Unlock()
	d.SweepOnce(ctx)
	j, _ = s.GetJob(ctx, pa, "j-or")
	if j.Status != "done" || j.Result["stdout"] != "real output\n" {
		t.Fatalf("recovered read must deliver real output: %s %#v", j.Status, j.Result)
	}
	if _, bad := j.Result["stdout_unavailable"]; bad {
		t.Fatal("successful read must not carry unavailable markers")
	}
}

// Past the output bound the job completes with explicit unavailability —
// bounded, truthful, never stuck, never pretending empty.
func TestDriverOutputUnavailableBound(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	proc := newFakeProc()
	d := New(s, proc, nil, Config{Lease: 90 * time.Second, OutputWait: 5 * time.Millisecond})

	if _, _, err := s.SubmitJob(ctx, pa, "j-ob", "subprocess", subReq("echo", "x"), "assistant"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx)
	opID := runtimeprovision.ProcessOperationID(pa, "job:j-ob")
	proc.finish(opID, runtimeprovision.ProcessSucceeded, 0, "unreachable\n", "")
	proc.mu.Lock()
	proc.readErr = errors.New("journal read down")
	proc.mu.Unlock()
	d.SweepOnce(ctx) // records the output wait
	time.Sleep(15 * time.Millisecond)
	d.SweepOnce(ctx) // over bound: complete degraded
	j, _ := s.GetJob(ctx, pa, "j-ob")
	if j.Status != "done" {
		t.Fatalf("bounded output wait must still complete: %s", j.Status)
	}
	if j.Result["stdout_unavailable"] != true || j.Result["stdout_read_error"] == nil {
		t.Fatalf("unavailable output must be marked, not blanked: %#v", j.Result)
	}
	if j.Result["exit_code"] != 0.0 {
		t.Fatalf("real outcome must survive the degraded result: %#v", j.Result)
	}
}

// A 16–100 KiB stream is a prefix read, not a complete one: truncated is
// set from the read's own bound, independent of the journal's larger cap.
func TestDriverOutputTruncationHonest(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	proc := newFakeProc()
	d := newDriver(s, proc, nil)

	if _, _, err := s.SubmitJob(ctx, pa, "j-big", "subprocess", subReq("yes"), "assistant"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx)
	opID := runtimeprovision.ProcessOperationID(pa, "job:j-big")
	big := strings.Repeat("0123456789abcdef\n", 5000) // ~85 KiB
	proc.finish(opID, runtimeprovision.ProcessSucceeded, 0, big, "")
	d.SweepOnce(ctx)
	j, _ := s.GetJob(ctx, pa, "j-big")
	if j.Status != "done" {
		t.Fatalf("big output must still complete: %s", j.Status)
	}
	if j.Result["stdout_truncated"] != true {
		t.Fatalf("prefix read must be flagged truncated: %#v", j.Result["stdout_truncated"])
	}
	if got := len(fmt.Sprint(j.Result["stdout"])); got != resultBytes {
		t.Fatalf("read bound must be honored: got %d bytes", got)
	}

	// A stream under the bound is complete — truncated stays false.
	if _, _, err := s.SubmitJob(ctx, pa, "j-small", "subprocess", subReq("echo"), "assistant"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx)
	smallID := runtimeprovision.ProcessOperationID(pa, "job:j-small")
	proc.finish(smallID, runtimeprovision.ProcessSucceeded, 0, "small\n", "")
	d.SweepOnce(ctx)
	j, _ = s.GetJob(ctx, pa, "j-small")
	if j.Result["stdout_truncated"] != false {
		t.Fatalf("complete output must not claim truncation: %#v", j.Result["stdout_truncated"])
	}
}

// NUL, control bytes and invalid UTF-8 cannot make the terminal write
// fail: output is sanitized and the sanitization is recorded.
func TestDriverOutputSanitizedAndStorable(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	proc := newFakeProc()
	d := newDriver(s, proc, nil)

	if _, _, err := s.SubmitJob(ctx, pa, "j-nul", "subprocess", subReq("x"), "assistant"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx)
	opID := runtimeprovision.ProcessOperationID(pa, "job:j-nul")
	proc.finish(opID, runtimeprovision.ProcessSucceeded, 0, "a\x00b\x07c\xff\xf0end\n", "")
	d.SweepOnce(ctx)
	j, _ := s.GetJob(ctx, pa, "j-nul")
	if j.Status != "done" {
		t.Fatalf("unsanitizable output must not block completion: %s %v", j.Status, j.Error)
	}
	stdout, _ := j.Result["stdout"].(string)
	if strings.ContainsRune(stdout, 0) || j.Result["stdout_sanitized"] != true {
		t.Fatalf("NUL must be stripped and recorded: %#v", j.Result)
	}
	if !strings.Contains(stdout, "abc") || !strings.HasSuffix(stdout, "end\n") {
		t.Fatalf("sanitized output lost real content: %q", stdout)
	}
}

// A cancel landing while the driver is still inside the slow scope ensure
// must fence the launch: the stale pre-claim row is re-checked before the
// start call is made.
func TestDriverLaunchRecheckBlocksCancelled(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	proc := newFakeProc()
	scope := newGateScope()
	d := newDriver(s, proc, scope)

	if _, _, err := s.SubmitJob(ctx, pa, "j-gc", "subprocess", subReq("echo", "x"), "assistant"); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { d.SweepOnce(ctx); close(done) }()
	<-scope.entered // driver is inside EnsureScope with its stale claim
	if _, err := s.CancelJob(ctx, pa, "j-gc"); err != nil {
		t.Fatal(err)
	}
	close(scope.release)
	<-done
	proc.mu.Lock()
	defer proc.mu.Unlock()
	if len(proc.started) != 0 {
		t.Fatalf("job cancelled during scope ensure must not launch: %d starts", len(proc.started))
	}
}

// The same fence for a foreign sweep: a job claimed away while the driver
// was inside EnsureScope does not launch.
func TestDriverLaunchRecheckBlocksSwept(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	proc := newFakeProc()
	scope := newGateScope()
	d := New(s, proc, scope, Config{Lease: 40 * time.Millisecond, Interval: time.Second})

	if _, _, err := s.SubmitJob(ctx, pa, "j-gs", "subprocess", subReq("echo", "x"), "assistant"); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { d.SweepOnce(ctx); close(done) }()
	<-scope.entered
	time.Sleep(80 * time.Millisecond) // driver's lease expires while it waits
	if _, swept, err := s.ClaimJobs(ctx, pa, "foreign", []string{"subprocess"}, time.Minute, 4, "*"); err != nil {
		t.Fatal(err)
	} else if len(swept) != 1 {
		t.Fatalf("expected the expired claim to sweep: %v", swept)
	}
	close(scope.release)
	<-done
	proc.mu.Lock()
	defer proc.mu.Unlock()
	if len(proc.started) != 0 {
		t.Fatalf("job swept during scope ensure must not launch: %d starts", len(proc.started))
	}
	j, _ := s.GetJob(ctx, pa, "j-gs")
	if j.Status != "lost" {
		t.Fatalf("swept job must stay lost, got %s", j.Status)
	}
}

// The Cloud driver must never take local-intended work: an unstamped job
// (the local side of the routing split) is left for a local runner while a
// cloud-stamped job on the same persona is claimed.
func TestDriverCloudRoutingLeavesLocalWork(t *testing.T) {
	pool := testdb.Create(t)
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	s := agentstate.NewStore(pool)
	// Driver wired (cloud claimed) but no default stamping: unstamped
	// requests stay local, explicit cloud admits because the runner is
	// declared live — the post-probe wiring's SetJobBackendAvailable.
	s.SetJobBackendAvailable("cloud")
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	proc := newFakeProc()
	d := newDriver(s, proc, nil)

	if _, _, err := s.SubmitJob(ctx, pa, "local-j", "subprocess", subReq("echo", "l"), "assistant"); err != nil {
		t.Fatal(err)
	}
	cloudReq := subReq("echo", "c")
	cloudReq["backend"] = "cloud"
	if _, _, err := s.SubmitJob(ctx, pa, "cloud-j", "subprocess", cloudReq, "assistant"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx)
	lj, _ := s.GetJob(ctx, pa, "local-j")
	if lj.Status != "queued" {
		t.Fatalf("local job must be left for a local runner, got %s", lj.Status)
	}
	cj, _ := s.GetJob(ctx, pa, "cloud-j")
	if cj.Status != "running" {
		t.Fatalf("cloud job must be claimed, got %s", cj.Status)
	}
	proc.mu.Lock()
	defer proc.mu.Unlock()
	if len(proc.started) != 1 || proc.started[0].OriginatingToolCallID != "job:cloud-j" {
		t.Fatalf("only the cloud job may launch: %+v", proc.started)
	}
}

type gateScope struct {
	entered chan struct{}
	release chan struct{}
}

func newGateScope() *gateScope {
	return &gateScope{entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *gateScope) EnsureScope(ctx context.Context, personaID string) error {
	close(g.entered)
	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// The late-start boundary, driver side: a StartProcess still inside its
// pre-journal window when the foreign sweep lands is NOT invisible
// forever — it journals after the verdict, is discovered by reconcile,
// and is physically cancelled. The observed outcome then records
// 'cancelled' (it was admitted before the sweep), never 'absent'.
func TestDriverForeignSweepDuringDelayedStart(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	proc := newFakeProc()
	proc.startBlock, proc.startEntered = make(chan struct{}), make(chan struct{})
	d := New(s, proc, nil, Config{Lease: 60 * time.Millisecond})

	if _, _, err := s.SubmitJob(ctx, pa, "j-ds", "subprocess", subReq("sleep", "30"), "assistant"); err != nil {
		t.Fatal(err)
	}
	sweptDone := make(chan struct{})
	go func() { d.SweepOnce(ctx); close(sweptDone) }()
	<-proc.startEntered // the request is provably in flight, unjournaled

	// The claim expires while the start is in flight; a foreign claim
	// pass sweeps the row to 'lost' before the backend journals anything.
	time.Sleep(300 * time.Millisecond)
	if _, swept, err := s.ClaimJobs(ctx, pa, "script-runner", []string{"script"}, time.Minute, 4, "*"); err != nil {
		t.Fatal(err)
	} else if len(swept) != 1 || swept[0].Status != "lost" {
		t.Fatalf("expected the foreign sweep to commit lost: %v", swept)
	}

	// The delayed start now resolves: it journals a live operation AFTER
	// the lost verdict — the exact race the fence exists to close.
	close(proc.startBlock)
	<-sweptDone
	opID := runtimeprovision.ProcessOperationID(pa, "job:j-ds")

	// Reconcile finds the materialized op and cancels it; the outcome
	// lands once the backend reaches terminal.
	d.SweepOnce(ctx)
	proc.mu.Lock()
	cancelled := false
	for _, id := range proc.canceled {
		cancelled = cancelled || id == opID
	}
	proc.mu.Unlock()
	if !cancelled {
		t.Fatal("the late-journaled op must be cancelled behind the lost verdict")
	}
	d.SweepOnce(ctx)
	j, err := s.GetJob(ctx, pa, "j-ds")
	if err != nil {
		t.Fatal(err)
	}
	if j.Status != "lost" {
		t.Fatalf("verdict must stay lost, got %s", j.Status)
	}
	out, ok := j.Result["observed_outcome"].(map[string]any)
	if !ok || out["state"] != "cancelled" {
		t.Fatalf("the late op must be reported cancelled, not absent: %#v", j.Result)
	}
}

// The other ordering: the fence lands first, the delayed start replays it.
// A cancel-requested job whose operation is still absent gets its
// never-started verdict only after the tombstone exists — and a start
// that lands after the verdict is fenced, never executed.
func TestDriverCancelThenDelayedStartFenced(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	proc := newFakeProc()
	proc.startErr = errors.New("provisioner down")
	d := newDriver(s, proc, nil)

	if _, _, err := s.SubmitJob(ctx, pa, "j-df", "subprocess", subReq("sleep", "30"), "assistant"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx) // claim; start fails (backend down) — no op journaled
	if _, err := s.CancelJob(ctx, pa, "j-df"); err != nil {
		t.Fatal(err)
	}
	proc.mu.Lock()
	proc.startErr = nil
	proc.mu.Unlock()
	d.SweepOnce(ctx) // absent → tombstone fence → cancelled/never_started
	j, _ := s.GetJob(ctx, pa, "j-df")
	if j.Status != "cancelled" || j.Result["outcome"] != "never_started" {
		t.Fatalf("cancel must complete behind the fence: %s %#v", j.Status, j.Result)
	}
	// The zombie request lands after the verdict: the fake's replay
	// mirrors the journal — it returns the tombstone and records no start.
	op, err := proc.StartProcess(ctx, runtimeprovision.ProcessStartRequest{
		PersonalityAgentID:    pa,
		OriginatingToolCallID: "job:j-df",
		Executable:            "sleep", Args: []string{"30"},
	})
	if err != nil || !op.Tombstone {
		t.Fatalf("zombie start must replay the fence: %+v %v", op, err)
	}
	proc.mu.Lock()
	defer proc.mu.Unlock()
	if len(proc.started) != 0 {
		t.Fatalf("a fenced operation must never execute: %d starts", len(proc.started))
	}
}

// A lost verdict over an absent operation attaches 'absent' as soon as
// the tombstone lands — no grace wait — because the fence is what makes
// the conclusion mechanically true.
func TestDriverLostAbsentFenceAttaches(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	proc := newFakeProc()
	proc.startErr = errors.New("provisioner down")
	d := New(s, proc, nil, Config{Lease: 60 * time.Millisecond})

	if _, _, err := s.SubmitJob(ctx, pa, "j-la", "subprocess", subReq("echo", "x"), "assistant"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx) // claim; start fails (backend down) — no op journaled
	time.Sleep(300 * time.Millisecond)
	if _, swept, err := s.ClaimJobs(ctx, pa, "other", []string{"script"}, time.Minute, 4, "*"); err != nil {
		t.Fatal(err)
	} else if len(swept) != 1 {
		t.Fatalf("expected foreign sweep: %v", swept)
	}
	proc.mu.Lock()
	proc.startErr = nil
	proc.mu.Unlock()
	// One reconcile: status absent → tombstone fence → absent outcome
	// attached in the same pass.
	d.SweepOnce(ctx)
	j, err := s.GetJob(ctx, pa, "j-la")
	if err != nil {
		t.Fatal(err)
	}
	if j.Status != "lost" {
		t.Fatalf("verdict must stay lost, got %s", j.Status)
	}
	out, ok := j.Result["observed_outcome"].(map[string]any)
	if !ok || out["state"] != "absent" {
		t.Fatalf("fenced-absent outcome must be attached: %#v", j.Result)
	}
	proc.mu.Lock()
	defer proc.mu.Unlock()
	if len(proc.started) != 0 {
		t.Fatalf("absent job must never execute: %d starts", len(proc.started))
	}
}

// The fence's RETURNED operation is the verdict input, not just the call's
// success: a delayed start that journals between the driver's absent status
// read and the cancel lands as a live record — the fence then returns a
// real, running operation with the cancel on it. Completing never_started
// there would erase real work and drop the row from continued reconcile.
// Reproduced deterministically: the fake reports absent (the read is
// genuinely stale) while the op materializes under the same call.
func TestDriverCancelFenceReturnsLiveOp(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	proc := newFakeProc()
	proc.realCancel = true // mirror the journal: cancel returns the op live
	proc.startErr = errors.New("provisioner down")
	d := New(s, proc, nil, Config{Lease: 90 * time.Second})

	if _, _, err := s.SubmitJob(ctx, pa, "j-mat", "subprocess", subReq("echo", "x"), "assistant"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx) // claims; launch fails — no journal record yet
	if _, err := s.CancelJob(ctx, pa, "j-mat"); err != nil {
		t.Fatal(err)
	}
	opID := runtimeprovision.ProcessOperationID(pa, "job:j-mat")
	proc.mu.Lock()
	proc.startErr = nil
	// The delayed start lands between the status read and the fence:
	// ProcessStatus answers absent, then the op exists when CancelProcess
	// runs — the fence returns it LIVE, not a tombstone.
	proc.onAbsent = func(f *fakeProc, r runtimeprovision.ProcessLookupRequest) {
		f.onAbsent = nil
		started := time.Now().Add(-time.Second)
		f.ops[r.OperationID] = &runtimeprovision.ProcessOperation{
			OperationID:           r.OperationID,
			PersonalityAgentID:    r.PersonalityAgentID,
			OriginatingToolCallID: "job:j-mat",
			State:                 runtimeprovision.ProcessRunning,
			StartedAt:             &started,
		}
	}
	proc.mu.Unlock()

	d.SweepOnce(ctx)
	j, _ := s.GetJob(ctx, pa, "j-mat")
	if j.Status != "cancel_requested" {
		t.Fatalf("a live op returned by the fence must keep reconciling — never_started would erase it, got %s %#v", j.Status, j.Result)
	}
	proc.mu.Lock()
	op := proc.ops[opID]
	cancelled := false
	for _, c := range proc.canceled {
		cancelled = cancelled || c == opID
	}
	proc.mu.Unlock()
	if op == nil || op.State != runtimeprovision.ProcessRunning || !cancelled {
		t.Fatalf("materialized op must be live with the cancel on it: %+v canceled=%v", op, cancelled)
	}

	// The op now reaches its terminal state — the verdict is the honest
	// outcome the work produced, not never_started.
	proc.finish(opID, runtimeprovision.ProcessCancelled, 137, "partial", "")
	d.SweepOnce(ctx)
	j, _ = s.GetJob(ctx, pa, "j-mat")
	if j.Status != "cancelled" {
		t.Fatalf("materialized op must resolve to cancelled, got %s", j.Status)
	}
	if j.Result["outcome"] == "never_started" {
		t.Fatalf("never_started erases real work: %#v", j.Result)
	}
}

// Same interleave, but the op already reached a terminal state before the
// fence: the job gets the honest outcome the work produced — never a
// never_started erasure and never a fabricated cancellation.
func TestDriverCancelFenceReturnsTerminalOp(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	proc := newFakeProc()
	proc.startErr = errors.New("provisioner down")
	d := New(s, proc, nil, Config{Lease: 90 * time.Second})

	if _, _, err := s.SubmitJob(ctx, pa, "j-fin", "subprocess", subReq("echo", "x"), "assistant"); err != nil {
		t.Fatal(err)
	}
	d.SweepOnce(ctx) // claims; launch fails — no journal record yet
	if _, err := s.CancelJob(ctx, pa, "j-fin"); err != nil {
		t.Fatal(err)
	}
	proc.mu.Lock()
	proc.startErr = nil
	// The delayed start journaled AND finished before the fence: a real
	// succeeded op comes back — the fence is a no-op on terminal records.
	proc.onAbsent = func(f *fakeProc, r runtimeprovision.ProcessLookupRequest) {
		f.onAbsent = nil
		now := time.Now()
		started := now.Add(-time.Second)
		exit := 0
		f.ops[r.OperationID] = &runtimeprovision.ProcessOperation{
			OperationID:           r.OperationID,
			PersonalityAgentID:    r.PersonalityAgentID,
			OriginatingToolCallID: "job:j-fin",
			State:                 runtimeprovision.ProcessSucceeded,
			ExitCode:              &exit,
			StartedAt:             &started,
			FinishedAt:            &now,
		}
		f.outputs[r.OperationID] = [2]string{"ran-anyway", ""}
	}
	proc.mu.Unlock()

	d.SweepOnce(ctx)
	j, _ := s.GetJob(ctx, pa, "j-fin")
	if j.Status != "done" {
		t.Fatalf("terminal op returned by the fence must publish its honest outcome, got %s %#v", j.Status, j.Result)
	}
	if j.Result["outcome"] == "never_started" {
		t.Fatalf("never_started erases finished work: %#v", j.Result)
	}
	if j.Result["stdout"] != "ran-anyway" {
		t.Fatalf("the real output must survive, got %#v", j.Result["stdout"])
	}
}
