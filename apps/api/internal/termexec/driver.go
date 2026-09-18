// Package termexec claims durable terminal sessions and drives them on
// the root runtime provisioner's interactive process operations.
//
// Same contract shape as jobexec: reconcile-before-claim, durable
// session identity (the provisioner op id is derived from the session
// id, so a restart re-attaches to the exact same container instead of
// launching a second shell), heartbeat-leased claims, and honest
// outcomes — an indeterminate backend answer yields 'lost', never a
// silent re-launch.
//
// Delivery evidence flows through the input ledger: a write whose
// attach died mid-write is 'unknown', never resent, because the byte
// prefix may already have taken effect in the shell.
package termexec

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
)

// ProcessAPI is the provisioner transport surface the driver needs —
// satisfied by *runtimeprovision.Client and by test fakes.
type ProcessAPI interface {
	StartProcess(context.Context, runtimeprovision.ProcessStartRequest) (runtimeprovision.ProcessOperation, error)
	ProcessStatus(context.Context, runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error)
	ReadProcessOutput(context.Context, runtimeprovision.ProcessOutputRequest) (runtimeprovision.ProcessOutput, error)
	CancelProcess(context.Context, runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error)
	WriteProcessInput(context.Context, runtimeprovision.ProcessInputRequest) (runtimeprovision.ProcessInputReceipt, error)
	ResizeProcess(context.Context, runtimeprovision.ProcessResizeRequest) (runtimeprovision.ProcessOperation, error)
	SignalProcess(context.Context, runtimeprovision.ProcessSignalRequest) (runtimeprovision.ProcessInputReceipt, error)
}

// ScopeEnsurer verifies the persona's canonical files scope exists
// through the storage authority before launch — the provisioner
// refuses a files-scope request whose scope is missing.
type ScopeEnsurer interface {
	EnsureScope(ctx context.Context, personaID string) error
}

type Config struct {
	// RunnerID is the durable claim identity on
	// core_terminal_sessions. Identical across restarts, unique per
	// deployment — same rule as the job driver's.
	RunnerID string
	// Backend is the routing predicate — "cloud" claims only
	// cloud-stamped sessions.
	Backend string
	// Lease is the claim TTL, renewed by heartbeats. Terminal leases
	// are short (default 30s): a dead driver stops blocking input fast.
	Lease time.Duration
	// Interval between discovery sweeps.
	Interval time.Duration
	// PollInterval between per-session pump ticks (input dispatch +
	// output drain + heartbeat cadence is this × heartbeatEvery).
	PollInterval time.Duration
	// HeartbeatEvery pump ticks between claim renewals.
	HeartbeatEvery int
	// PageSize bounds sessions discovered per sweep.
	PageSize int
	// ClaimLimit bounds new claims per persona per sweep.
	ClaimLimit int
	// Shell/ShellArgs are the session entrypoint inside the pinned
	// job image — server-owned, never requester-chosen.
	Shell     string
	ShellArgs []string
	// MaxLifetimeSeconds bounds a session; the provisioner enforces it
	// as the op deadline and the end reason surfaces 'timeout'.
	MaxLifetimeSeconds int
	// UnknownWait bounds indeterminate backend answers before the
	// driver stops renewing the claim — the sweep's 'interrupted'
	// verdict and a later reclaim take over.
	UnknownWait time.Duration
	// CallTimeout bounds each backend call.
	CallTimeout time.Duration
	// Logf receives operational messages; nil discards.
	Logf func(format string, args ...any)
}

func (c Config) normalize() Config {
	if c.RunnerID == "" {
		c.RunnerID = "termexec-docker"
	}
	if c.Backend == "" {
		c.Backend = "cloud"
	}
	if c.Lease <= 0 {
		c.Lease = 30 * time.Second
	}
	if c.Interval <= 0 {
		c.Interval = 2 * time.Second
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 350 * time.Millisecond
	}
	if c.HeartbeatEvery <= 0 {
		c.HeartbeatEvery = 20
	}
	if c.PageSize <= 0 {
		c.PageSize = 64
	}
	if c.ClaimLimit <= 0 {
		c.ClaimLimit = 2
	}
	if c.Shell == "" {
		c.Shell = "/bin/bash"
		c.ShellArgs = []string{"-l"}
	}
	if c.MaxLifetimeSeconds <= 0 {
		c.MaxLifetimeSeconds = 8 * 3600
	}
	if c.UnknownWait <= 0 {
		c.UnknownWait = 5 * time.Minute
	}
	if c.CallTimeout <= 0 {
		c.CallTimeout = 15 * time.Second
	}
	if c.Logf == nil {
		c.Logf = func(string, ...any) {}
	}
	return c
}

type Driver struct {
	store *agentstate.Store
	proc  ProcessAPI
	scope ScopeEnsurer
	cfg   Config

	mu     sync.Mutex
	active map[string]context.CancelFunc
}

func New(store *agentstate.Store, proc ProcessAPI, scope ScopeEnsurer, cfg Config) *Driver {
	return &Driver{store: store, proc: proc, scope: scope, cfg: cfg.normalize(), active: map[string]context.CancelFunc{}}
}

// Runner is the claim identity this driver writes into terminal
// sessions — stable across restarts so owned sessions are recovered.
func (d *Driver) Runner() string { return d.cfg.RunnerID }

// OutputAttached reports the runtime's own read-time output health
// for a session's live op: the journal pump is attached and appending
// records. attached=false on a live op means emitted bytes are not
// being captured — the surface must not claim a healthy terminal.
// known=false means the runtime could not answer (or the op already
// ended); callers omit the field rather than guess either way.
func (d *Driver) OutputAttached(ctx context.Context, personaID, sessionID string) (attached, known bool) {
	op, err := d.proc.ProcessStatus(ctx, runtimeprovision.ProcessLookupRequest{
		PersonalityAgentID: personaID,
		OperationID:        runtimeprovision.ProcessOperationID(personaID, "term:"+sessionID),
	})
	if err != nil || op.State.Terminal() {
		return false, false
	}
	return op.OutputAttached, true
}

func (d *Driver) Run(ctx context.Context) {
	d.sweep(ctx)
	tick := time.NewTicker(d.cfg.Interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			d.stopAll()
			return
		case <-tick.C:
			d.sweep(ctx)
		}
	}
}

func (d *Driver) stopAll() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, cancel := range d.active {
		cancel()
	}
}

func (d *Driver) sweep(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	personas, err := d.store.RunnableTerminalPersonas(ctx, d.cfg.RunnerID, d.cfg.Backend, d.cfg.PageSize)
	if err != nil {
		d.cfg.Logf("termexec: discovery failed: %v", err)
		return
	}
	for _, personaID := range personas {
		if ctx.Err() != nil {
			return
		}
		d.sweepPersona(ctx, personaID)
	}
}

// sweepPersona claims new work and resumes sessions this runner
// already owns (a driver restart leaves 'claimed' rows whose leases
// may still be live — resuming under the existing epoch is faster
// than waiting out the sweep).
func (d *Driver) sweepPersona(ctx context.Context, personaID string) {
	claimed, _, err := d.store.ClaimTerminalSessions(ctx, personaID, d.cfg.RunnerID, d.cfg.Backend, d.cfg.Lease, d.cfg.ClaimLimit)
	if err != nil {
		d.cfg.Logf("termexec: claim persona %s: %v", personaID, err)
	}
	for _, t := range claimed {
		d.adopt(t)
	}
	// Sessions already claimed by this runner: resume under the
	// existing epoch (the container survived our restart).
	ours, err := d.store.OwnedTerminalSessions(ctx, personaID, d.cfg.RunnerID)
	if err != nil {
		return
	}
	for _, t := range ours {
		d.adopt(t)
	}
}

func (d *Driver) adopt(t agentstate.TerminalSession) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.active[t.SessionID]; ok {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	d.active[t.SessionID] = cancel
	go d.runSession(ctx, t)
}

func (d *Driver) release(sessionID string) {
	d.mu.Lock()
	delete(d.active, sessionID)
	d.mu.Unlock()
}

// operationRequest builds the launch spec for a session. The
// originating tool call id is the session's own deterministic
// identity — the provisioner derives the same operation id from it,
// so a replayed StartProcess re-attaches rather than relaunches.
func (d *Driver) operationRequest(t agentstate.TerminalSession) runtimeprovision.ProcessStartRequest {
	return runtimeprovision.ProcessStartRequest{
		PersonalityAgentID:    t.PersonaID,
		OriginatingToolCallID: "term:" + t.SessionID,
		Executable:            d.cfg.Shell,
		Args:                  d.cfg.ShellArgs,
		Cwd:                   ".",
		TimeoutSeconds:        d.cfg.MaxLifetimeSeconds,
		Image:                 "job",
		Workspace:             "files-scope",
		Interactive:           true,
		TTY:                   true,
	}
}

// runSession drives one claimed session: launch/attach the provisioner
// op, then pump inputs → container and output → scrollback until the
// session ends, the claim is lost, or the driver is told to stop.
func (d *Driver) runSession(ctx context.Context, session agentstate.TerminalSession) {
	defer d.release(session.SessionID)
	personaID, sessionID := session.PersonaID, session.SessionID
	epoch := session.Epoch
	opID := runtimeprovision.ProcessOperationID(personaID, "term:"+sessionID)

	call := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(ctx, d.cfg.CallTimeout)
	}

	// The files scope must exist before launch — the provisioner
	// refuses the bind otherwise and the session would sit 'claimed'
	// forever. Failure here is indeterminate (scope check or launch
	// may have raced); the claim lapses and a reclaim retries.
	if d.scope != nil {
		c, cancel := call()
		err := d.scope.EnsureScope(c, personaID)
		cancel()
		if err != nil {
			d.cfg.Logf("termexec: session %s scope ensure failed: %v", sessionID, err)
			return
		}
	}
	var op runtimeprovision.ProcessOperation
	{
		c, cancel := call()
		var err error
		op, err = d.proc.StartProcess(c, d.operationRequest(session))
		cancel()
		if err != nil {
			// Busy is a definite capacity answer; everything else is
			// indeterminate — either way the claim lapses and the
			// session becomes reclaimable rather than failed-by-us.
			d.cfg.Logf("termexec: session %s launch: %v", sessionID, err)
			return
		}
	}
	if op.State.Terminal() {
		d.reportTerminal(ctx, session, op)
		return
	}
	pump := &sessionPump{
		d: d, ctx: ctx, sessionID: sessionID, personaID: personaID,
		epoch: epoch, opID: opID, ending: session.Status == "ending",
	}
	if session.Status != "ending" {
		// Bind the live op: the session becomes 'active'. An adopted
		// 'ending' session skips this — it goes straight to terminate.
		if _, err := d.store.ReportTerminalStatus(ctx, personaID, sessionID, d.cfg.RunnerID, epoch,
			"active", "", nil, "", op.OperationID); err != nil {
			return
		}
	}
	pump.run()
}

// sessionPump is one session's delivery loop. It heartbeats the
// claim, dispatches the input ledger in order, drains output into the
// scrollback, and maps the op's terminal state to the session's
// honest end.
type sessionPump struct {
	d         *Driver
	ctx       context.Context
	sessionID string
	personaID string
	epoch     int64
	opID      string

	cursor    int64
	ending    bool
	unknownAt time.Time
	ticks     int
}

func (p *sessionPump) call() (context.Context, context.CancelFunc) {
	return context.WithTimeout(p.ctx, p.d.cfg.CallTimeout)
}

func (p *sessionPump) run() {
	tick := time.NewTicker(p.d.cfg.PollInterval)
	defer tick.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-tick.C:
			if !p.step() {
				return
			}
		}
	}
}

// step is one pump tick. False means the session is done with us:
// terminal reported, claim lost, or the backend stayed indeterminate
// past UnknownWait (claim lapses → 'interrupted' → later reclaim).
func (p *sessionPump) step() bool {
	p.ticks++
	if p.ticks%p.d.cfg.HeartbeatEvery == 0 {
		c, cancel := p.call()
		t, err := p.d.store.HeartbeatTerminalSession(c, p.personaID, p.sessionID,
			p.d.cfg.RunnerID, p.epoch, p.d.cfg.Lease)
		cancel()
		if err != nil {
			if errors.Is(err, agentstate.ErrTerminalNotClaimed) {
				p.d.cfg.Logf("termexec: session %s claim lost", p.sessionID)
				return false
			}
			// Transient store failure: keep working — the lease has
			// margin and the next heartbeat retries.
		} else {
			if t.Status == "ending" {
				p.ending = true
			}
		}
	}
	if !p.dispatchInputs() {
		return false
	}
	p.drainOutput()
	if p.ending {
		p.terminate()
		return false
	}
	c, cancel := p.call()
	op, err := p.d.proc.ProcessStatus(c, runtimeprovision.ProcessLookupRequest{
		PersonalityAgentID: p.personaID, OperationID: p.opID,
	})
	cancel()
	if err != nil {
		return p.boundUnknown()
	}
	if op.State.Terminal() {
		p.drainOutput()
		p.d.reportTerminal(p.ctx, agentstate.TerminalSession{
			PersonaID: p.personaID, SessionID: p.sessionID, Epoch: p.epoch,
		}, op)
		return false
	}
	p.unknownAt = time.Time{}
	return true
}

// boundUnknown enforces the indeterminate-wait bound: after
// UnknownWait the driver stops renewing — the lease lapses, the sweep
// marks 'interrupted', and a reclaim re-attaches when the backend is
// back. It never declares the session dead on a hunch.
func (p *sessionPump) boundUnknown() bool {
	if p.unknownAt.IsZero() {
		p.unknownAt = time.Now()
		return true
	}
	return time.Since(p.unknownAt) < p.d.cfg.UnknownWait
}

// dispatchInputs drains the session's input ledger in seq order.
// Delivery evidence maps to dispositions: delivered→written,
// indeterminate→unknown (never resent), definite backend refusal→
// failed. A stopped loop is not a dropped input — the row keeps its
// state and the next claimant re-serves only provably-undelivered
// work.
func (p *sessionPump) dispatchInputs() bool {
	c, cancel := p.call()
	inputs, err := p.d.store.PendingTerminalInputs(c, p.personaID, p.sessionID, p.d.cfg.RunnerID, p.epoch)
	cancel()
	if err != nil {
		if errors.Is(err, agentstate.ErrTerminalNotClaimed) {
			return false
		}
		return true
	}
	for _, in := range inputs {
		if p.ctx.Err() != nil {
			return false
		}
		// Dequeue first: if the claim is already gone we must not
		// deliver a byte nobody owns the disposition for.
		if err := p.disposition(in.InputID, "dequeued", nil); err != nil {
			return false
		}
		status, detail := p.deliver(in)
		if err := p.disposition(in.InputID, status, detail); err != nil {
			return false
		}
	}
	return true
}

func (p *sessionPump) disposition(inputID, status string, detail map[string]any) error {
	c, cancel := p.call()
	_, err := p.d.store.ReportTerminalInputDisposition(c, p.personaID, p.sessionID,
		inputID, p.d.cfg.RunnerID, p.epoch, status, detail)
	cancel()
	return err
}

// deliver performs one input's backend effect and returns its honest
// disposition.
func (p *sessionPump) deliver(in agentstate.TerminalInput) (string, map[string]any) {
	lookup := runtimeprovision.ProcessLookupRequest{PersonalityAgentID: p.personaID, OperationID: p.opID}
	switch in.Kind {
	case "stdin", "eof":
		var data []byte
		eof := in.Kind == "eof"
		if !eof {
			str, _ := in.Payload["data"].(string)
			data = []byte(str)
		}
		var receipt runtimeprovision.ProcessInputReceipt
		var err error
		for attempt := 0; attempt < 2; attempt++ {
			c, cancel := p.call()
			receipt, err = p.d.proc.WriteProcessInput(c, runtimeprovision.ProcessInputRequest{
				ProcessLookupRequest: lookup, Data: []byte(data), EOF: eof,
			})
			cancel()
			if err != nil {
				break
			}
			if receipt.Delivered || receipt.Indeterminate {
				break
			}
			// Attach never opened → provably nothing sent → one retry
			// is safe; a second failure is a definite failed.
		}
		switch {
		case err != nil:
			return "failed", map[string]any{"error": err.Error()}
		case receipt.Delivered:
			return "written", nil
		case receipt.Indeterminate:
			// Maybe-delivered bytes are never resent.
			return "unknown", map[string]any{"detail": receipt.Detail}
		default:
			return "failed", map[string]any{"detail": receipt.Detail}
		}
	case "resize":
		cols, _ := in.Payload["cols"].(float64)
		rows, _ := in.Payload["rows"].(float64)
		c, cancel := p.call()
		_, err := p.d.proc.ResizeProcess(c, runtimeprovision.ProcessResizeRequest{
			ProcessLookupRequest: lookup, Cols: int(cols), Rows: int(rows),
		})
		cancel()
		if err != nil {
			return "failed", map[string]any{"error": err.Error()}
		}
		return "written", nil
	case "signal":
		sig, _ := in.Payload["signal"].(string)
		c, cancel := p.call()
		receipt, err := p.d.proc.SignalProcess(c, runtimeprovision.ProcessSignalRequest{
			ProcessLookupRequest: lookup, Signal: sig,
		})
		cancel()
		switch {
		case err != nil:
			return "failed", map[string]any{"error": err.Error()}
		case receipt.Delivered:
			return "written", nil
		case receipt.Indeterminate:
			return "unknown", map[string]any{"detail": receipt.Detail}
		default:
			return "failed", map[string]any{"detail": receipt.Detail}
		}
	}
	return "failed", map[string]any{"error": "unknown input kind"}
}

// drainOutput pulls retained output from the provisioner into the
// session scrollback until a short read or EOF. A reported gap becomes
// an explicit gap chunk; replayed drains dedupe on
// (session_id, base, kind) so a crash between drain and commit can
// never double-append.
func (p *sessionPump) drainOutput() {
	for {
		c, cancel := p.call()
		out, err := p.d.proc.ReadProcessOutput(c, runtimeprovision.ProcessOutputRequest{
			ProcessLookupRequest: runtimeprovision.ProcessLookupRequest{
				PersonalityAgentID: p.personaID, OperationID: p.opID,
			},
			Stream: "stdout", Offset: p.cursor, Limit: 64 << 10,
		})
		cancel()
		if err != nil {
			return
		}
		var chunks []agentstate.TerminalOutputChunk
		if out.Gap && out.BaseOffset > p.cursor {
			gapTo := out.BaseOffset
			chunks = append(chunks, agentstate.TerminalOutputChunk{
				Kind: "gap", Base: p.cursor, GapTo: &gapTo,
			})
			p.cursor = out.BaseOffset
		}
		if len(out.Content) > 0 {
			// On a gap the content starts at BaseOffset, not at the
			// requested offset.
			base := out.Offset
			if out.Gap {
				base = out.BaseOffset
			}
			chunks = append(chunks, agentstate.TerminalOutputChunk{
				Kind: "data", Base: base, Data: []byte(out.Content),
			})
			if out.NextOffset > p.cursor {
				p.cursor = out.NextOffset
			}
		}
		if len(chunks) > 0 {
			c2, cancel2 := p.call()
			_, err = p.d.store.AppendTerminalOutput(c2, p.personaID, p.sessionID,
				p.d.cfg.RunnerID, p.epoch, chunks)
			cancel2()
			if err != nil {
				// Roll the cursor back: uncommitted chunks re-drain
				// next tick and dedupe if they partially landed.
				p.cursor = chunks[0].Base
				return
			}
		}
		if out.EOF || len(out.Content) < (64<<10) {
			return
		}
	}
}

// terminate performs an 'ending' session's stop: cancel the op, wait
// for its terminal state, drain the last output, report the end.
func (p *sessionPump) terminate() {
	c, cancel := p.call()
	_, _ = p.d.proc.CancelProcess(c, runtimeprovision.ProcessLookupRequest{
		PersonalityAgentID: p.personaID, OperationID: p.opID,
	})
	cancel()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		c, cancel := p.call()
		op, err := p.d.proc.ProcessStatus(c, runtimeprovision.ProcessLookupRequest{
			PersonalityAgentID: p.personaID, OperationID: p.opID,
		})
		cancel()
		if err == nil && op.State.Terminal() {
			p.drainOutput()
			p.d.reportTerminal(p.ctx, agentstate.TerminalSession{
				PersonaID: p.personaID, SessionID: p.sessionID, Epoch: p.epoch,
			}, op)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	// The cancel could not be confirmed terminal inside the bound —
	// stop renewing; the claim lapses and the sweep's reclaim path
	// takes over rather than a guess being published.
}

// reportTerminal maps the op's terminal state to the session's honest
// end. 'indeterminate' is 'lost', never 'ended' — the container's
// fate is unknown and the row says so.
func (d *Driver) reportTerminal(ctx context.Context, session agentstate.TerminalSession, op runtimeprovision.ProcessOperation) {
	status, reason := "ended", "shell_exit"
	var exitCode *int
	if op.ExitCode != nil {
		exitCode = op.ExitCode
	}
	switch op.State {
	case runtimeprovision.ProcessSucceeded:
		reason = "shell_exit"
	case runtimeprovision.ProcessCancelled:
		reason = "closed"
	case runtimeprovision.ProcessFailed:
		reason = "failed"
		if op.Error != "" && (containsFold(op.Error, "timeout") || containsFold(op.Error, "deadline")) {
			reason = "timeout"
		}
	case runtimeprovision.ProcessIndeterminate:
		status, reason = "lost", "container outcome indeterminate"
		if op.Error != "" {
			reason = op.Error
		}
	}
	if status == "lost" {
		// A 'lost' verdict does not prove the writer stopped. The
		// provisioner's terminal reconcile stops any container that is
		// still physically live and commits ContainerRemoved; cancel is
		// issued here so the returned operation reports the reconcile's
		// current verdict — Quiesced means the writer is proven gone,
		// false means the stop is still pending and the op must not be
		// consumed as a quiescence boundary.
		c2, cancel2 := context.WithTimeout(context.Background(), d.cfg.CallTimeout)
		stopped, cerr := d.proc.CancelProcess(c2, runtimeprovision.ProcessLookupRequest{
			PersonalityAgentID: session.PersonaID, OperationID: op.OperationID,
		})
		cancel2()
		if cerr != nil {
			d.cfg.Logf("termexec: session %s lost-container stop: %v", session.SessionID, cerr)
		} else if !stopped.Quiesced {
			d.cfg.Logf("termexec: session %s lost verdict published with container stop still pending", session.SessionID)
		}
	}
	c, cancel := context.WithTimeout(ctx, d.cfg.CallTimeout)
	_, err := d.store.ReportTerminalStatus(c, session.PersonaID, session.SessionID,
		d.cfg.RunnerID, session.Epoch, status, reason, exitCode, "", op.OperationID)
	cancel()
	if err != nil {
		d.cfg.Logf("termexec: session %s terminal report: %v", session.SessionID, err)
	}
}

func containsFold(s, sub string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(sub))
}
