// Package jobexec is the Cloud Linux job runner: it drives core_jobs of
// kind "subprocess" onto the root runtime provisioner's durable process
// service. Each job maps to exactly one deterministic process operation
// (persona × "job:"+job_id), launched as an ephemeral hardened container
// bind-mounted to the persona's canonical verified files scope — Linux is
// started for the command and reclaimed when it ends, while output, result
// and shared files persist independently of the environment.
//
// Recovery contract (see REPAIR-HANDBACK.md): the runner identity is the
// stable string RunnerID, never a PID, so a restarted driver reconciles
// every outstanding claim before it claims anything new — and the
// reconcile set itself is discovered at job granularity, filtered by
// kind/owner/resolution in SQL, so unresolved work is never hidden behind
// newer history. A possibly-launched operation is never re-created: the
// deterministic operation ID is inspected first, and a delayed start that
// is still in flight is fenced by the provisioner's own journal — a second
// StartProcess for the same operation replays it rather than launching
// twice. A job the sweep already marked 'lost' keeps that verdict: the
// driver stops the orphaned environment and records what the backend
// actually did through AttachLostOutcome.
//
// Refusal vs. unknown: only typed definite refusals (invalid request,
// workspace refusal, divergent identity, capacity busy) may complete a job
// failed. A lost StartProcess response, a status outage, or any other
// ambiguous backend error leaves the work unresolved — durably, through
// result.runner_wait — until the backend answers or the bounded wait
// releases the claim to the sweep, where reconcileLost performs the
// physical cleanup the verdict requires.
package jobexec

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
)

// ProcessAPI is the slice of the provisioner process transport the driver
// needs — satisfied by *runtimeprovision.Client and by test fakes.
type ProcessAPI interface {
	StartProcess(context.Context, runtimeprovision.ProcessStartRequest) (runtimeprovision.ProcessOperation, error)
	ProcessStatus(context.Context, runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error)
	ReadProcessOutput(context.Context, runtimeprovision.ProcessOutputRequest) (runtimeprovision.ProcessOutput, error)
	CancelProcess(context.Context, runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error)
}

// ScopeEnsurer verifies the persona's canonical files scope exists through
// the storage authority before a launch. The provisioner refuses a
// files-scope request whose scope is missing, so a nil ensurer degrades the
// driver to "every job fails workspace resolution" — wiring must supply one.
type ScopeEnsurer interface {
	EnsureScope(ctx context.Context, personaID string) error
}

// Wait causes recorded durably under result.runner_wait.
const (
	waitBusy    = "busy"    // definite capacity refusal — bounded, then failed
	waitUnknown = "unknown" // indeterminate execution — bounded, then released
	waitOutput  = "output"  // terminal op whose output cannot be read yet
)

// Config tunes the driver. Zero values pick safe defaults.
type Config struct {
	// RunnerID is the durable claim identity recorded on core_jobs. It must
	// be identical across restarts and unique per deployment — a second
	// driver with the same id would fight over claims, and a different id
	// cannot reconcile this driver's work.
	RunnerID string
	// Kinds this runner claims. Only "subprocess" today; the scripts
	// backend registers its own kind against the same discovery seam.
	Kinds []string
	// Backend is the routing predicate passed to ClaimJobs — "cloud" claims
	// only requests stamped backend:"cloud", leaving unstamped/local work
	// to local runners.
	Backend string
	// Lease is the claim TTL. Heartbeats renew it every sweep; an
	// unrefreshed claim becomes sweepable after Lease.
	Lease time.Duration
	// Interval between sweeps.
	Interval time.Duration
	// PageSize bounds unresolved and runnable rows discovered per sweep.
	PageSize int
	// ClaimLimit bounds new jobs claimed per persona per sweep.
	ClaimLimit int
	// BackendWait bounds a 'busy' wait: the backend affirmatively refused
	// capacity for this long, so the job fails honestly instead of holding
	// a claim while nothing can execute.
	BackendWait time.Duration
	// UnknownWait bounds an indeterminate wait: the backend has not
	// answered definitely for this long, so the driver stops renewing the
	// claim — the sweep's 'lost' verdict and reconcileLost's physical
	// cleanup take over. It never completes the job failed: a dead or
	// unreachable service is not proof of a stopped process.
	UnknownWait time.Duration
	// OutputWait bounds how long a terminal job waits for its output to
	// become readable before completing with explicit *_unavailable fields
	// rather than retrying forever.
	OutputWait time.Duration
	// CallTimeout bounds each process-backend call so a slow or hung
	// transport cannot starve the sweep's heartbeats for everyone else.
	CallTimeout time.Duration
	// Logf receives operational messages; nil discards.
	Logf func(format string, args ...any)
}

func (c Config) normalize() Config {
	if c.RunnerID == "" {
		c.RunnerID = "jobexec-docker"
	}
	if len(c.Kinds) == 0 {
		c.Kinds = []string{"subprocess"}
	}
	if c.Backend == "" {
		c.Backend = "cloud"
	}
	if c.Lease <= 0 {
		c.Lease = 2 * time.Minute
	}
	if c.Interval <= 0 {
		c.Interval = 2 * time.Second
	}
	if c.PageSize <= 0 {
		c.PageSize = 64
	}
	if c.PageSize > 500 {
		c.PageSize = 500
	}
	if c.ClaimLimit <= 0 {
		c.ClaimLimit = 2
	}
	if c.BackendWait <= 0 {
		c.BackendWait = 10 * time.Minute
	}
	if c.UnknownWait <= 0 {
		c.UnknownWait = 10 * time.Minute
	}
	if c.OutputWait <= 0 {
		c.OutputWait = 2 * time.Minute
	}
	if c.CallTimeout <= 0 {
		c.CallTimeout = 15 * time.Second
	}
	if c.Logf == nil {
		c.Logf = func(string, ...any) {}
	}
	return c
}

// Driver is a single in-process reconcile→claim loop. Duplicate monitors
// are structurally impossible: one Driver owns one claim identity and all
// job state lives in core_jobs, so running two Drivers on the same store
// and runner id would just contend — deployments must run exactly one.
type Driver struct {
	store *agentstate.Store
	proc  ProcessAPI
	scope ScopeEnsurer
	cfg   Config

	mu sync.Mutex
	// Job-granular discovery cursors (persona_id, job_id pairs).
	claimedPersona, claimedJob   string
	runnablePersona, runnableJob string
	// released tracks jobs whose claims we stopped renewing after an
	// over-bound indeterminate wait. Their personas get claim passes until
	// the sweep commits 'lost'; restart-loss only delays the transition.
	released map[string]bool
}

func New(store *agentstate.Store, proc ProcessAPI, scope ScopeEnsurer, cfg Config) *Driver {
	return &Driver{
		store: store, proc: proc, scope: scope,
		cfg: cfg.normalize(), released: map[string]bool{},
	}
}

// Run sweeps until ctx ends. The first sweep reconciles before claiming —
// that ordering is the recovery contract, not an optimization.
func (d *Driver) Run(ctx context.Context) {
	d.sweep(ctx)
	tick := time.NewTicker(d.cfg.Interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			d.sweep(ctx)
		}
	}
}

// SweepOnce runs one reconcile+claim pass; tests drive it directly.
func (d *Driver) SweepOnce(ctx context.Context) { d.sweep(ctx) }

// Runner reports the configured claim identity for logging.
func (d *Driver) Runner() string { return d.cfg.RunnerID }

func (d *Driver) sweep(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	// The whole sweep is bounded at half the lease: a hung backend makes
	// every call eat CallTimeout, and without a sweep bound a page of them
	// would starve every other job's heartbeat past expiry. Rows not
	// attended this sweep keep their claims and wait for the next.
	ctx, cancelSweep := context.WithTimeout(ctx, d.cfg.Lease/2)
	defer cancelSweep()
	// Unresolved claimed work is discovered and reconciled first — before
	// any new claim is offered — so an expired lease is revived or
	// completed rather than swept by our own claim pass. Discovery is at
	// job granularity: kind, owner and resolution are filtered in SQL and
	// one bounded page rotates through the whole unresolved set.
	unresolved, err := d.store.ClaimJobsNeedingAttention(ctx, d.cfg.RunnerID, d.cfg.Kinds,
		d.cfg.PageSize, d.claimedPersona, d.claimedJob)
	if err != nil {
		d.cfg.Logf("jobexec: unresolved-job discovery failed: %v", err)
	} else {
		d.advance(&d.claimedPersona, &d.claimedJob, unresolved)
		for _, j := range unresolved {
			d.reconcileJob(ctx, j)
		}
	}
	runnable, err := d.store.RunnableJobs(ctx, d.cfg.Kinds,
		d.cfg.PageSize, d.runnablePersona, d.runnableJob)
	if err != nil {
		d.cfg.Logf("jobexec: runnable-job discovery failed: %v", err)
	} else {
		d.advance(&d.runnablePersona, &d.runnableJob, runnable)
		personas := map[string]struct{}{}
		for _, j := range runnable {
			personas[j.PersonaID] = struct{}{}
		}
		for p := range personas {
			d.claimPersona(ctx, p)
		}
	}
	// Personas carrying released claims get claim passes until their
	// expired leases sweep to 'lost' — the claim pass is the only path to
	// the verdict.
	d.mu.Lock()
	released := map[string]struct{}{}
	for key := range d.released {
		p, _, _ := splitKey(key)
		released[p] = struct{}{}
	}
	d.mu.Unlock()
	for p := range released {
		d.claimPersona(ctx, p)
	}
}

// advance moves a discovery cursor to the last row of the page, or wraps to
// the start when the page came back short (the unresolved set is drained).
func (d *Driver) advance(persona, job *string, page []agentstate.Job) {
	if len(page) == 0 || len(page) < d.cfg.PageSize {
		*persona, *job = "", ""
		return
	}
	last := page[len(page)-1]
	*persona, *job = last.PersonaID, last.JobID
}

func jobKey(j agentstate.Job) string { return j.PersonaID + "/" + j.JobID }

func splitKey(key string) (persona, job string, ok bool) {
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			return key[:i], key[i+1:], true
		}
	}
	return "", "", false
}

func (d *Driver) ownerOf(j agentstate.Job) string {
	if j.ClaimedBy == nil {
		return ""
	}
	return *j.ClaimedBy
}

// opCtx bounds one process-backend call so a hung transport cannot starve
// the sweep. A timed-out call surfaces as 'unknown' — safe, because the
// server-side work continues and the durable journal answers next tick.
func (d *Driver) opCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d.cfg.CallTimeout)
}

// reconcileJob re-drives one unresolved claim before new claims are
// offered: lost jobs get their environment retired and their real outcome
// recorded; live claims get heartbeats, cancel relays, or completion.
func (d *Driver) reconcileJob(ctx context.Context, j agentstate.Job) {
	if j.Status == "lost" {
		d.mu.Lock()
		delete(d.released, jobKey(j))
		d.mu.Unlock()
		d.reconcileLost(ctx, j)
		return
	}
	d.reconcileClaimed(ctx, j)
}

// reconcileClaimed handles one running/cancel_requested job this runner
// claims. The deterministic operation ID decides what the backend knows;
// the job row decides what the owner asked for.
func (d *Driver) reconcileClaimed(ctx context.Context, j agentstate.Job) {
	lookup := processLookup(j)
	opCtx, cancel := d.opCtx(ctx)
	op, err := d.proc.ProcessStatus(opCtx, lookup)
	cancel()
	switch {
	case errors.Is(err, runtimeprovision.ErrProcessNotFound):
		if j.Status == "cancel_requested" {
			// 'No journal record' is not proof no start is in flight —
			// but the cancel fence is. Tombstone-cancel lands a durable
			// terminal record at the deterministic operation ID under the
			// same serialization as StartProcess's journal write. What the
			// fence RETURNS decides the verdict: only a tombstone proves
			// 'never started' — a successful cancel alone does not. A
			// delayed start that journaled between the status read and the
			// fence comes back as a live record (cancel now on it), which
			// ordinary reconcile resolves to its honest terminal outcome.
			fop, ferr := d.fenceOperation(ctx, j)
			if ferr != nil {
				d.backendWait(ctx, j, waitUnknown, ferr)
				return
			}
			switch {
			case fop.Tombstone:
				d.completeJob(ctx, j, "cancelled",
					map[string]any{"backend": "docker-process", "outcome": "never_started"}, "")
			case fop.State == runtimeprovision.ProcessAccepted, fop.State == runtimeprovision.ProcessRunning:
				// Real work materialized mid-fence — the cancel is on it.
				// Its terminal state and truthful result land next sweep;
				// 'never_started' would erase work that exists.
			default:
				// Already-terminal real work reached the journal before
				// the fence — publish the honest outcome it produced.
				d.finishJob(ctx, j, fop)
			}
			return
		}
		d.startBackend(ctx, j)
		return
	case err != nil:
		d.backendWait(ctx, j, waitUnknown, err)
		return
	}
	// The operation exists: renew the claim (this also revives an
	// expired-but-unswept claim — verified against real Postgres) and learn
	// the job's current wishes.
	hj, herr := d.store.HeartbeatJob(ctx, j.PersonaID, j.JobID, d.cfg.RunnerID, d.cfg.Lease)
	if errors.Is(herr, agentstate.ErrJobNotClaimed) {
		// Swept between status read and heartbeat — next tick handles the
		// lost row (stop orphan, record outcome).
		return
	}
	if herr != nil {
		d.cfg.Logf("jobexec: heartbeat %s: %v", j.JobID, herr)
		return
	}
	d.clearWait(ctx, hj)
	if hj.Status == "lost" {
		// Our own heartbeat just swept the expired claim.
		d.reconcileLost(ctx, hj)
		return
	}
	switch op.State {
	case runtimeprovision.ProcessAccepted, runtimeprovision.ProcessRunning:
		if hj.Status == "cancel_requested" {
			opCtx, cancel := d.opCtx(ctx)
			_, cerr := d.proc.CancelProcess(opCtx, lookup)
			cancel()
			if cerr != nil && !errors.Is(cerr, runtimeprovision.ErrProcessNotFound) {
				d.cfg.Logf("jobexec: cancel relay %s: %v", j.JobID, cerr)
			}
		}
		// Otherwise still running — the next tick re-evaluates.
	default:
		// Terminal backend state is the job outcome.
		d.finishJob(ctx, j, op)
	}
}

// fenceOperation lands the durable cancel fence for a job's deterministic
// operation. A live operation is cancel-requested; a missing one gets a
// tombstone record so a delayed StartProcess replays the cancellation
// instead of launching after a terminal verdict was published. The CALLER
// must inspect the returned operation: only a Tombstone proves 'never
// started' or 'observed absent' — a live or terminal return means real
// work exists and the fence merely put the cancel on it.
func (d *Driver) fenceOperation(ctx context.Context, j agentstate.Job) (runtimeprovision.ProcessOperation, error) {
	opCtx, cancel := d.opCtx(ctx)
	op, err := d.proc.CancelProcess(opCtx, runtimeprovision.ProcessLookupRequest{
		PersonalityAgentID:    j.PersonaID,
		OperationID:           operationID(j),
		OriginatingToolCallID: toolCallPrefix + j.JobID,
		TombstoneIfAbsent:     true,
	})
	cancel()
	return op, err
}

// reconcileLost retires the environment behind a committed lost verdict and
// records what the backend actually did. 'lost' is never upgraded to a
// success — the outcome is evidence attached to the verdict, not a rewrite.
func (d *Driver) reconcileLost(ctx context.Context, j agentstate.Job) {
	lookup := processLookup(j)
	opCtx, cancel := d.opCtx(ctx)
	op, err := d.proc.ProcessStatus(opCtx, lookup)
	cancel()
	switch {
	case errors.Is(err, runtimeprovision.ErrProcessNotFound):
		if d.ownerOf(j) != d.cfg.RunnerID {
			return
		}
		// 'Absent' is only publishable behind the fence: until the
		// tombstone lands, a delayed start may still be inside its
		// pre-journal window. Once it lands, serialization guarantees no
		// launch can follow — the outcome attaches immediately, not after
		// a grace guess.
		fop, ferr := d.fenceOperation(ctx, j)
		if ferr != nil {
			d.cfg.Logf("jobexec: lost-job cancel fence %s: %v", j.JobID, ferr)
			return
		}
		if fop.Tombstone {
			d.attachOutcome(ctx, j, map[string]any{
				"state": "absent",
				"note":  "the deterministic operation was fenced before it ever journaled a launch; the job never reached the process service",
			})
			return
		}
		// The operation materialized between the status read and the
		// fence — the cancel is on it now; its terminal state and outcome
		// land next sweep.
		return
	case err != nil:
		d.cfg.Logf("jobexec: lost-job status %s: %v", j.JobID, err)
		return
	}
	switch op.State {
	case runtimeprovision.ProcessAccepted, runtimeprovision.ProcessRunning:
		// A lost verdict does not stop a container by itself: the orphaned
		// environment is killed here, regardless of which claimant swept
		// the claim. The outcome is attached once the kill lands.
		if _, cerr := d.fenceOperation(ctx, j); cerr != nil && !errors.Is(cerr, runtimeprovision.ErrProcessNotFound) {
			d.cfg.Logf("jobexec: orphan cancel %s: %v", j.JobID, cerr)
		}
	default:
		if d.ownerOf(j) == d.cfg.RunnerID {
			d.attachOutcome(ctx, j, d.observedOutcome(ctx, op))
		}
	}
}

// claimPersona offers claims after reconciliation. The claim pass also
// returns jobs it just swept — those are reconciled the same way so their
// environments are retired rather than assumed dead.
func (d *Driver) claimPersona(ctx context.Context, personaID string) {
	claimed, swept, err := d.store.ClaimJobs(ctx, personaID, d.cfg.RunnerID, d.cfg.Kinds,
		d.cfg.Lease, d.cfg.ClaimLimit, d.cfg.Backend)
	if err != nil {
		d.cfg.Logf("jobexec: claim %s: %v", personaID, err)
		return
	}
	for _, j := range swept {
		d.reconcileLost(ctx, j)
	}
	for _, j := range claimed {
		d.startBackend(ctx, j)
	}
}

// startBackend is the single launch path, used for both fresh claims and
// reconcile-discovered jobs that never reached the backend.
func (d *Driver) startBackend(ctx context.Context, j agentstate.Job) {
	if d.scope != nil {
		if err := d.scope.EnsureScope(ctx, j.PersonaID); err != nil {
			d.completeJob(ctx, j, "failed",
				map[string]any{"backend": "docker-process", "failure": "workspace_scope", "detail": err.Error()},
				fmt.Sprintf("canonical files scope unavailable: %v", err))
			return
		}
	}
	// The row we reconciled is stale by the whole scope-ensure window —
	// the slowest step in the launch path. Re-read authority now: a job
	// cancelled, swept, sealed or claimed away while we resolved must not
	// launch behind our back.
	fresh, err := d.store.GetJob(ctx, j.PersonaID, j.JobID)
	if err != nil {
		d.cfg.Logf("jobexec: authority recheck %s: %v — not launching", j.JobID, err)
		return
	}
	if fresh.Status != "running" || d.ownerOf(fresh) != d.cfg.RunnerID {
		return
	}
	req, err := processRequest(fresh)
	if err != nil {
		d.completeJob(ctx, j, "failed",
			map[string]any{"backend": "docker-process", "failure": "invalid_request", "detail": err.Error()},
			err.Error())
		return
	}
	opCtx, cancel := d.opCtx(ctx)
	op, err := d.proc.StartProcess(opCtx, req)
	cancel()
	switch {
	case err == nil:
		d.clearWait(ctx, fresh)
		d.cfg.Logf("jobexec: job %s launched as operation %s", j.JobID, op.OperationID)
	case errors.Is(err, runtimeprovision.ErrProcessBusy):
		// Definite capacity refusal: nothing was accepted — bounded wait,
		// then failed honestly.
		d.backendWait(ctx, j, waitBusy, err)
	case errors.Is(err, runtimeprovision.ErrInvalidProcessRequest),
		errors.Is(err, runtimeprovision.ErrProcessWorkspace),
		errors.Is(err, runtimeprovision.ErrConflict):
		// Deterministic refusal — the request, the workspace, or a
		// divergent op identity will not heal by retrying.
		d.completeJob(ctx, j, "failed",
			map[string]any{"backend": "docker-process", "failure": "refused", "detail": err.Error()},
			err.Error())
	default:
		// Everything else is indeterminate: the request may have been
		// journaled before the response was lost, or may still be
		// resolving. Never fail — never conclude absent — just wait.
		d.backendWait(ctx, j, waitUnknown, err)
	}
}

// backendWait records a durable wait on the job row and applies the
// per-cause bound. 'busy' is definite refusal — after BackendWait the job
// fails honestly. 'unknown' is indeterminate — after UnknownWait the claim
// is no longer renewed so the sweep commits 'lost' and reconcileLost
// performs the physical cleanup; the job itself is never failed on an
// ambiguous answer.
func (d *Driver) backendWait(ctx context.Context, j agentstate.Job, cause string, causeErr error) {
	key := jobKey(j)
	if c, since, ok := agentstate.JobWait(j); ok && c == cause {
		switch cause {
		case waitBusy:
			if time.Since(since) > d.cfg.BackendWait {
				d.completeJob(ctx, j, "failed",
					map[string]any{"backend": "docker-process", "failure": "backend_unavailable", "detail": causeErr.Error()},
					fmt.Sprintf("process backend refused capacity for %s: %v", d.cfg.BackendWait, causeErr))
				return
			}
		case waitUnknown:
			if time.Since(since) > d.cfg.UnknownWait {
				// Stop renewing: the claim expires, the next claim pass
				// commits 'lost', and reconcileLost keeps polling the
				// backend — recovery and cleanup stay possible forever.
				d.mu.Lock()
				d.released[key] = true
				d.mu.Unlock()
				d.cfg.Logf("jobexec: job %s indeterminate for %s — releasing claim to sweep", j.JobID, d.cfg.UnknownWait)
				return
			}
		}
	}
	if _, err := d.store.NoteWait(ctx, j.PersonaID, j.JobID, d.cfg.RunnerID, d.cfg.Lease, cause); err != nil {
		if !errors.Is(err, agentstate.ErrJobNotClaimed) {
			d.cfg.Logf("jobexec: wait record %s: %v", j.JobID, err)
		}
	}
}

// clearWait drops the durable wait bookkeeping once the backend answers
// affirmatively. The released marker goes with it — a revived claim is not
// a released one.
func (d *Driver) clearWait(ctx context.Context, j agentstate.Job) {
	if _, _, ok := agentstate.JobWait(j); !ok {
		return
	}
	if err := d.store.ClearWait(ctx, j.PersonaID, j.JobID, d.cfg.RunnerID); err != nil {
		d.cfg.Logf("jobexec: clear wait %s: %v", j.JobID, err)
	}
	d.mu.Lock()
	delete(d.released, jobKey(j))
	d.mu.Unlock()
}

// finishJob completes a job from a terminal operation. The result carries
// the bounded actual output, not just a pointer to the journal — and if
// the output cannot be read yet the job holds (durably) instead of
// committing blank output. Only after OutputWait does it complete with
// explicit *_unavailable fields: never silent blanking, never stuck.
func (d *Driver) finishJob(ctx context.Context, j agentstate.Job, op runtimeprovision.ProcessOperation) {
	status, jobErr := jobStatusFor(op)
	result, outErr := d.jobResult(ctx, op)
	if outErr != nil {
		cause, since, ok := agentstate.JobWait(j)
		if !(ok && cause == waitOutput && time.Since(since) > d.cfg.OutputWait) {
			// Output may just not be flushed/readable yet — hold and retry.
			if _, err := d.store.NoteWait(ctx, j.PersonaID, j.JobID, d.cfg.RunnerID, d.cfg.Lease, waitOutput); err != nil && !errors.Is(err, agentstate.ErrJobNotClaimed) {
				d.cfg.Logf("jobexec: output wait %s: %v", j.JobID, err)
			}
			return
		}
		d.cfg.Logf("jobexec: job %s output unreadable for %s — completing with unavailable markers", j.JobID, d.cfg.OutputWait)
	}
	_, err := d.store.CompleteJob(ctx, j.PersonaID, j.JobID, d.cfg.RunnerID, status, result, jobErr)
	switch {
	case err == nil:
		return
	case errors.Is(err, agentstate.ErrJobNotClaimed):
		// Swept mid-flight: the row is lost now. Preserve the real outcome
		// under the verdict rather than leaving it in the journal only.
		if fresh, gerr := d.store.GetJob(ctx, j.PersonaID, j.JobID); gerr == nil && fresh.Status == "lost" {
			d.reconcileLost(ctx, fresh)
		}
	case errors.Is(err, agentstate.ErrJobConflict):
		d.cfg.Logf("jobexec: divergent completion recorded for %s — not overwriting", j.JobID)
	default:
		d.cfg.Logf("jobexec: complete %s: %v", j.JobID, err)
	}
}

func (d *Driver) completeJob(ctx context.Context, j agentstate.Job, status string, result map[string]any, jobErr string) {
	if _, err := d.store.CompleteJob(ctx, j.PersonaID, j.JobID, d.cfg.RunnerID, status, result, jobErr); err != nil {
		d.cfg.Logf("jobexec: complete %s as %s: %v", j.JobID, status, err)
	}
}

func (d *Driver) attachOutcome(ctx context.Context, j agentstate.Job, outcome map[string]any) {
	_, err := d.store.AttachLostOutcome(ctx, j.PersonaID, j.JobID, d.cfg.RunnerID, outcome)
	switch {
	case err == nil:
		d.cfg.Logf("jobexec: recorded observed outcome on lost job %s", j.JobID)
	case errors.Is(err, agentstate.ErrJobConflict), errors.Is(err, agentstate.ErrJobNotClaimed):
		// Already attached, or no longer ours — both are stable end states.
	default:
		d.cfg.Logf("jobexec: attach outcome %s: %v", j.JobID, err)
	}
}
