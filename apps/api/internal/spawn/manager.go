// Package spawn manages per-agent lazy runtime lifecycle for the dev control
// plane (ADR 0010). Agents start on demand (呼びかけ), stop when idle in cold
// mode, and stay running in warm mode. Per-agent state directories, workspaces,
// and wrapping keys are provisioned from the 戸籍.
package spawn

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// Warmth settings mirror the 戸籍 agents.warmth column (ADR 0010 §4).
const (
	WarmthCold = "cold"
	WarmthWarm = "warm"
)

// ErrManagerClosed rejects runtime admission after StopAll begins.
var ErrManagerClosed = errors.New("spawn manager is closed")

// ErrStartShutdownTimeout reports that an in-flight start did not return within
// StopAll's wait bound. Its completion remains fenced and will stop any process
// that arrives after the timeout.
var ErrStartShutdownTimeout = errors.New("timed out waiting for agent starts to stop")

// AgentRuntimeConfig is the per-agent configuration a ProcessSpawner receives.
type AgentRuntimeConfig struct {
	AgentID                   string
	StateDir                  string
	WorkspaceDir              string
	WrappingKey               WrappingKeyMaterial
	Bearer                    string
	Nonce                     string
	Warmth                    string
	Generation                uint64
	GenerationLeaseID         string
	GenerationRecoveryFenceID string
	GatewayURL                string
	ExecutorSocket            string
	LocalControlURL           string
}

// WrappingKeyMaterial is one indivisible per-agent key identity. The ID and
// bytes move together from 戸籍 storage through runtime activation.
type WrappingKeyMaterial struct {
	ID    string
	Bytes string
}

// ErrCleanupIncomplete means Wait ended without proving physical teardown.
// Ordinary process exit errors do not carry this marker.
var ErrCleanupIncomplete = errors.New("runtime cleanup incomplete")

// Lifecycle causes carry only classification; underlying diagnostics can
// contain private configuration and must not be written to process logs.
var ErrRuntimeEpochLost = errors.New("runtime left its active epoch")

// ErrRuntimeDetached ends API observation without claiming physical process exit.
var ErrRuntimeDetached = errors.New("runtime observation detached")

// Process represents a running agent process.
type Process interface {
	Wait() error
	Stop() error
}

// DetachedProcess is owned by an external runtime service. Detach only joins
// this API's observation; Stop remains the explicit physical stop operation.
type DetachedProcess interface {
	Process
	Detach() error
}

type RuntimeRestorer interface {
	Restore(context.Context, AgentRuntimeConfig) (Process, error)
}

// ProcessSpawner starts an agent process for the given config. The context
// bounds startup only; canceling it after Spawn returns must not stop the
// returned process. Runtime lifetime is owned through Process.Stop.
type ProcessSpawner interface {
	Spawn(ctx context.Context, config AgentRuntimeConfig) (Process, error)
}

// AgentResolver looks up the per-agent material the manager needs: the wrapping
// key and warmth setting from the 戸籍.
type AgentResolver interface {
	AgentWrappingKey(ctx context.Context, agentID string) (WrappingKeyMaterial, error)
	AgentWarmth(ctx context.Context, agentID string) (string, error)
}

// Config is the shared spawn configuration independent of individual agents.
type Config struct {
	Spawner         ProcessSpawner
	Resolver        AgentResolver
	StateRoot       string // root directory for per-agent state dirs
	WorkspaceRoot   string // root directory for per-agent workspaces
	GatewayURL      string
	ExecutorSocket  string
	LocalControlURL string
	Generation      uint64
	SharedBearer    string // shared control-plane bearer; per-agent value is derived
	SharedNonce     string
	IdleTimeout     time.Duration // cold-mode idle stop delay; 0 disables auto-stop
	ShutdownTimeout time.Duration // bound for in-flight starts during StopAll; 0 uses 5s
	// ClaimIdle holds the activity source's lock while claim reserves an idle
	// stop. claim only takes Manager.mu; neither callback may stop a process.
	ClaimIdle func(agentID string, claim func() bool) (bool, error)
	Now       func() time.Time
	// SkipAgentIDs are agents already managed externally (e.g. the legacy
	// single-process dev agent). EnsureRunning is a no-op for them.
	SkipAgentIDs []string
}

// Manager owns the per-agent runtime lifecycle.
type Manager struct {
	cfg       Config
	mu        sync.Mutex
	running   map[string]*agentRuntime
	starting  map[string]*startAttempt
	stopping  map[string]*stopAttempt
	closing   bool
	detaching bool
	now       func() time.Time
	idleStop  time.Duration
	stopWait  time.Duration
	skip      map[string]bool
}

type agentRuntime struct {
	process          Process
	cleanupPending   bool
	lastActive       time.Time
	warmth           string
	activityRevision uint64
	admissions       uint64
}

type startAttempt struct {
	done       chan struct{}
	cancel     context.CancelFunc
	err        error
	cleanupErr error
}

type stopAttempt struct {
	done chan struct{}
	err  error
}

// New returns a Manager. A nil Spawner or Resolver is an error.
func New(cfg Config) (*Manager, error) {
	if cfg.Spawner == nil {
		return nil, errors.New("spawn manager requires a ProcessSpawner")
	}
	if cfg.Resolver == nil {
		return nil, errors.New("spawn manager requires an AgentResolver")
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 5 * time.Second
	}
	skip := make(map[string]bool, len(cfg.SkipAgentIDs))
	for _, id := range cfg.SkipAgentIDs {
		skip[id] = true
	}
	return &Manager{
		cfg:      cfg,
		running:  make(map[string]*agentRuntime),
		starting: make(map[string]*startAttempt),
		stopping: make(map[string]*stopAttempt),
		now:      now,
		idleStop: cfg.IdleTimeout,
		stopWait: cfg.ShutdownTimeout,
		skip:     skip,
	}, nil
}

// ReconcileWarm refreshes the persisted cost setting without recording activity.
// Only an absent or cleanup-pending warm runtime needs ordinary lifecycle
// admission. EnsureRunning owns coalescing, fenced cleanup, and shutdown races.
func (m *Manager) ReconcileWarm(ctx context.Context, agentID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	warmth, err := m.cfg.Resolver.AgentWarmth(ctx, agentID)
	if err != nil {
		return fmt.Errorf("resolve warmth for %s: %w", agentID, err)
	}
	m.mu.Lock()
	if m.closing {
		m.mu.Unlock()
		return ErrManagerClosed
	}
	if m.skip[agentID] {
		m.mu.Unlock()
		return nil
	}
	rt := m.running[agentID]
	if rt != nil {
		rt.warmth = warmth
	}
	needsStart := warmth == WarmthWarm && (rt == nil || rt.cleanupPending)
	m.mu.Unlock()
	if !needsStart {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return m.ensureRunning(ctx, agentID, true)
}

// EnsureRunning starts the agent if it is not already running and records the
// call as activity. It is called on 呼びかけ (direct-chat connection). Agents
// in the SkipAgentIDs set are left to their external manager.
func (m *Manager) EnsureRunning(ctx context.Context, agentID string) error {
	return m.ensureRunning(ctx, agentID, false)
}

// RestoreRunning adopts an existing external runtime without creating one.
func (m *Manager) RestoreRunning(ctx context.Context, agentID string) error {
	return m.ensureRuntime(ctx, agentID, false, true)
}

func (m *Manager) ConfigurationCurrent(ctx context.Context, agentID string) (bool, error) {
	if !m.Running(agentID) {
		return false, nil
	}
	if checker, ok := m.cfg.Spawner.(interface {
		ConfigurationCurrent(context.Context, string) (bool, error)
	}); ok {
		return checker.ConfigurationCurrent(ctx, agentID)
	}
	return true, nil
}

func (m *Manager) ensureRunning(ctx context.Context, agentID string, warmOnly bool) error {
	return m.ensureRuntime(ctx, agentID, warmOnly, false)
}

func (m *Manager) ensureRuntime(ctx context.Context, agentID string, warmOnly, restoreOnly bool) error {
	for {
		m.mu.Lock()
		if m.closing {
			m.mu.Unlock()
			return ErrManagerClosed
		}
		if m.skip[agentID] {
			m.mu.Unlock()
			return nil
		}
		if attempt, ok := m.stopping[agentID]; ok {
			m.mu.Unlock()
			select {
			case <-attempt.done:
				if attempt.err != nil {
					return attempt.err
				}
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if rt, ok := m.running[agentID]; ok {
			if rt.cleanupPending {
				m.mu.Unlock()
				if err := m.Stop(agentID); err != nil {
					return err
				}
				continue
			}
			if !warmOnly && !restoreOnly {
				rt.lastActive = m.now()
				rt.activityRevision++
			}
			m.mu.Unlock()
			return nil
		}
		if attempt, ok := m.starting[agentID]; ok {
			m.mu.Unlock()
			select {
			case <-attempt.done:
				if attempt.err != nil {
					return attempt.err
				}
				// A warm-only start can settle without a process when the
				// persisted setting became cold. Demand still needs admission.
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		startContext, cancelStart := context.WithCancel(ctx)
		attempt := &startAttempt{
			done:   make(chan struct{}),
			cancel: cancelStart,
		}
		m.starting[agentID] = attempt
		m.mu.Unlock()

		runtime, err := m.startRuntime(startContext, agentID, warmOnly, restoreOnly)
		cancelStart()
		m.mu.Lock()
		if !m.closing {
			if err == nil && runtime != nil {
				m.running[agentID] = runtime
			}
			attempt.err = err
			delete(m.starting, agentID)
			close(attempt.done)
			m.mu.Unlock()
			if runtime != nil {
				go m.watchRuntime(agentID, runtime)
			}
			return err
		}
		detaching := m.detaching
		m.mu.Unlock()

		// API shutdown won publication. Release this controller's ownership using
		// the chosen detach/stop mode; late completion cannot reopen admission.
		if runtime != nil {
			if stopErr := releaseProcess(runtime.process, detaching); stopErr != nil {
				attempt.cleanupErr = fmt.Errorf("stop late agent %s: %w", agentID, stopErr)
			}
		}
		m.mu.Lock()
		if runtime != nil && attempt.cleanupErr != nil {
			runtime.cleanupPending = true
			m.running[agentID] = runtime
		}
		attempt.err = errors.Join(ErrManagerClosed, attempt.cleanupErr)
		delete(m.starting, agentID)
		close(attempt.done)
		m.mu.Unlock()
		return attempt.err
	}
}

func (m *Manager) startRuntime(ctx context.Context, agentID string, warmOnly, restoreOnly bool) (*agentRuntime, error) {
	warmth, err := m.cfg.Resolver.AgentWarmth(ctx, agentID)
	if err != nil {
		return nil, fmt.Errorf("resolve warmth for %s: %w", agentID, err)
	}
	if warmth == "" {
		warmth = WarmthCold
	}
	if warmOnly && warmth != WarmthWarm {
		return nil, nil
	}
	if restoreOnly {
		restorer, ok := m.cfg.Spawner.(RuntimeRestorer)
		if !ok {
			return nil, nil
		}
		process, err := restorer.Restore(ctx, AgentRuntimeConfig{AgentID: agentID, Warmth: warmth, GatewayURL: m.cfg.GatewayURL})
		if err != nil || process == nil {
			return nil, err
		}
		return &agentRuntime{process: process, lastActive: m.now(), warmth: warmth}, nil
	}
	wrappingKey, err := m.cfg.Resolver.AgentWrappingKey(ctx, agentID)
	if err != nil {
		return nil, fmt.Errorf("resolve wrapping key for %s: %w", agentID, err)
	}
	config := AgentRuntimeConfig{
		AgentID:                   agentID,
		StateDir:                  fmt.Sprintf("%s/%s", m.cfg.StateRoot, agentID),
		WorkspaceDir:              fmt.Sprintf("%s/%s", m.cfg.WorkspaceRoot, agentID),
		WrappingKey:               wrappingKey,
		Bearer:                    deriveCredential(m.cfg.SharedBearer, agentID),
		Nonce:                     deriveCredential(m.cfg.SharedNonce, agentID),
		Warmth:                    warmth,
		Generation:                m.cfg.Generation,
		GenerationLeaseID:         randomOpaqueID(),
		GenerationRecoveryFenceID: randomOpaqueID(),
		GatewayURL:                m.cfg.GatewayURL,
		ExecutorSocket:            m.cfg.ExecutorSocket,
		LocalControlURL:           m.cfg.LocalControlURL,
	}
	process, err := m.cfg.Spawner.Spawn(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("spawn agent %s: %w", agentID, err)
	}
	return &agentRuntime{process: process, lastActive: m.now(), warmth: warmth}, nil
}

// watchRuntime evicts only an exited instance; incomplete cleanup remains owned
// but cannot admit work until a later lifecycle request finishes cleanup.
// A prior epoch may finish after a replacement has already been published.
func (m *Manager) watchRuntime(agentID string, runtime *agentRuntime) {
	err := runtime.process.Wait()
	m.mu.Lock()
	requested := m.closing || m.stopping[agentID] != nil || m.running[agentID] != runtime
	if m.running[agentID] == runtime {
		if errors.Is(err, ErrCleanupIncomplete) {
			runtime.cleanupPending = true
		} else {
			delete(m.running, agentID)
		}
	}
	m.mu.Unlock()
	log.Printf("spawn: runtime ended: agent=%q reason=%s cleanup_incomplete=%t",
		agentID, runtimeEndReason(err, requested), errors.Is(err, ErrCleanupIncomplete))
}

func runtimeEndReason(err error, requested bool) string {
	switch {
	case errors.Is(err, ErrRuntimeDetached):
		return "api_detached"
	case errors.Is(err, ErrRuntimeEpochLost):
		return "active_epoch_lost"
	case errors.Is(err, ErrCleanupIncomplete):
		return "cleanup_incomplete"
	case requested:
		return "requested_stop"
	case err != nil:
		return "process_failed"
	default:
		return "clean_exit"
	}
}

// Touch records activity for a running agent (an open direct-chat connection).
func (m *Manager) Touch(agentID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if rt, ok := m.running[agentID]; ok {
		rt.lastActive = m.now()
		rt.activityRevision++
	}
}

// Running reports whether an agent is currently running.
func (m *Manager) Running(agentID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	rt := m.running[agentID]
	return rt != nil && !rt.cleanupPending
}

// HoldAdmission protects an already-running generation across durable command
// append. If idle reclamation won first, callers must reject before appending.
func (m *Manager) HoldAdmission(agentID string) (func(), error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.skip[agentID] {
		return func() {}, nil
	}
	rt := m.running[agentID]
	if m.closing || rt == nil || rt.cleanupPending || m.stopping[agentID] != nil {
		return nil, errors.New("runtime is not available for admission")
	}
	rt.admissions++
	rt.activityRevision++
	rt.lastActive = m.now()
	var once sync.Once
	return func() {
		once.Do(func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			rt.admissions--
			rt.lastActive = m.now()
			rt.activityRevision++
		})
	}, nil
}

// Stop terminates a running agent.
func (m *Manager) Stop(agentID string) error {
	for {
		m.mu.Lock()
		if attempt, ok := m.stopping[agentID]; ok {
			m.mu.Unlock()
			<-attempt.done
			return attempt.err
		}
		if attempt, ok := m.starting[agentID]; ok {
			m.mu.Unlock()
			<-attempt.done
			continue
		}
		rt, ok := m.running[agentID]
		if !ok {
			m.mu.Unlock()
			return nil
		}
		attempt := &stopAttempt{done: make(chan struct{})}
		m.stopping[agentID] = attempt
		m.mu.Unlock()

		err := rt.process.Stop()
		m.mu.Lock()
		attempt.err = err
		if m.running[agentID] == rt {
			if err == nil {
				delete(m.running, agentID)
			} else {
				rt.cleanupPending = true
			}
		}
		delete(m.stopping, agentID)
		close(attempt.done)
		m.mu.Unlock()
		return err
	}
}

// StopIdleCold reserves each idle stop atomically with runtime activity, then
// stops outside all activity/manager locks. Only successful stops are reported.
func (m *Manager) StopIdleCold() ([]string, error) {
	if m.idleStop <= 0 {
		return nil, nil
	}
	m.mu.Lock()
	type candidate struct {
		runtime  *agentRuntime
		revision uint64
	}
	candidates := make(map[string]candidate)
	for id, rt := range m.running {
		if rt.warmth != WarmthWarm && m.now().Sub(rt.lastActive) >= m.idleStop {
			candidates[id] = candidate{rt, rt.activityRevision}
		}
	}
	m.mu.Unlock()
	var stopped []string
	var failures []error
	for id, selected := range candidates {
		didStop, err := m.stopSelectedIdle(id, selected.runtime, selected.revision, true)
		if err != nil {
			failures = append(failures, err)
		} else if didStop {
			stopped = append(stopped, id)
		}
	}
	return stopped, errors.Join(failures...)
}

// StopIfIdle stops the current runtime only if no work or admission wins its
// idle reservation. It ignores warmth and idle age, for explicit configuration
// changes. True means the runtime stopped successfully or was already absent
// with no start/stop in flight under the manager lock. Busy, closing, externally
// managed, or starting/stopping runtimes return false. EnsureRunning waits for
// a reserved stop; HoldAdmission rejects it.
func (m *Manager) StopIfIdle(agentID string) (bool, error) {
	m.mu.Lock()
	if m.closing || m.skip[agentID] || m.starting[agentID] != nil || m.stopping[agentID] != nil {
		m.mu.Unlock()
		return false, nil
	}
	rt := m.running[agentID]
	if rt == nil {
		m.mu.Unlock()
		return true, nil
	}
	revision := rt.activityRevision
	m.mu.Unlock()
	return m.stopSelectedIdle(agentID, rt, revision, false)
}

func (m *Manager) stopSelectedIdle(id string, rt *agentRuntime, revision uint64, coldOnly bool) (bool, error) {
	var attempt *stopAttempt
	claim := func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.closing || m.running[id] != rt || m.stopping[id] != nil || rt.admissions != 0 || rt.activityRevision != revision {
			return false
		}
		if coldOnly && (rt.warmth == WarmthWarm || m.now().Sub(rt.lastActive) < m.idleStop) {
			return false
		}
		attempt = &stopAttempt{done: make(chan struct{})}
		m.stopping[id] = attempt
		return true
	}
	var claimed bool
	var err error
	if m.cfg.ClaimIdle != nil {
		claimed, err = m.cfg.ClaimIdle(id, claim)
	} else {
		claimed = claim()
	}
	if err != nil {
		return false, fmt.Errorf("inspect idle agent %s: %w", id, err)
	}
	if !claimed {
		return false, nil
	}
	err = rt.process.Stop()
	m.mu.Lock()
	attempt.err = err
	// Failed cleanup stays owned and is unavailable for further admission.
	if m.running[id] == rt {
		if err == nil {
			delete(m.running, id)
		} else {
			rt.cleanupPending = true
		}
	}
	delete(m.stopping, id)
	close(attempt.done)
	m.mu.Unlock()
	if err != nil {
		return false, fmt.Errorf("stop idle agent %s: %w", id, err)
	}
	return true, nil
}

// Warmth returns the warmth setting of a running agent, or "" if not running.
func (m *Manager) Warmth(agentID string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if rt, ok := m.running[agentID]; ok {
		return rt.warmth
	}
	return ""
}

// StopAll permanently closes runtime admission, cancels in-flight starts,
// waits boundedly for them to finish, and stops every published runtime. A
// spawner that returns success after the wait bound is still fenced by the
// closing state: its process is stopped instead of being registered.
func (m *Manager) StopAll() error {
	return m.close(false)
}

// DetachAll closes API admission and joins external runtime observations while
// leaving established work and its durable authorization epoch intact.
func (m *Manager) DetachAll() error {
	return m.close(true)
}

func releaseProcess(process Process, detach bool) error {
	if detach {
		if external, ok := process.(DetachedProcess); ok {
			return external.Detach()
		}
		return errors.New("runtime does not support detachment")
	}
	return process.Stop()
}

func (m *Manager) close(detach bool) error {
	m.mu.Lock()
	m.closing = true
	m.detaching = detach
	ids := make([]string, 0, len(m.running))
	for id := range m.running {
		ids = append(ids, id)
	}
	type pendingStart struct {
		agentID string
		attempt *startAttempt
	}
	starts := make([]pendingStart, 0, len(m.starting))
	for agentID, attempt := range m.starting {
		starts = append(starts, pendingStart{agentID: agentID, attempt: attempt})
	}
	m.mu.Unlock()

	for _, start := range starts {
		start.attempt.cancel()
	}
	var errs []error
	for _, id := range ids {
		var err error
		if detach {
			m.mu.Lock()
			rt := m.running[id]
			m.mu.Unlock()
			if rt != nil {
				err = releaseProcess(rt.process, true)
			}
		} else {
			err = m.Stop(id)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("stop agent %s: %w", id, err))
		}
	}
	if len(starts) == 0 {
		return errors.Join(errs...)
	}
	waitContext, cancelWait := context.WithTimeout(context.Background(), m.stopWait)
	defer cancelWait()
	for _, start := range starts {
		select {
		case <-start.attempt.done:
			if start.attempt.cleanupErr != nil {
				errs = append(errs, start.attempt.cleanupErr)
			}
		case <-waitContext.Done():
			// Prefer completion when it races the wait deadline.
			select {
			case <-start.attempt.done:
				if start.attempt.cleanupErr != nil {
					errs = append(errs, start.attempt.cleanupErr)
				}
				continue
			default:
			}
			errs = append(errs, fmt.Errorf(
				"%w: %s",
				ErrStartShutdownTimeout,
				start.agentID,
			))
			return errors.Join(errs...)
		}
	}
	return errors.Join(errs...)
}

// deriveCredential produces a per-agent value from a shared secret and agent id,
// matching the LocalControlServer derivation in cmd/server.
func deriveCredential(shared, agentID string) string {
	return shared + "/" + agentID
}

// randomOpaqueID produces a 16-byte URL-safe base64 string suitable for the
// agent's process generation lease and recovery fence.
func randomOpaqueID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand.Read panics only on catastrophic failure; returning a
		// zero string would make the agent fail to boot, so panic is safer.
		panic(fmt.Sprintf("crypto/rand.Read failed: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(b)
}
