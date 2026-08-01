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
	WrappingKey               string
	Bearer                    string
	Nonce                     string
	BearerExpiresAtUnix       int64
	Warmth                    string
	Generation                uint64
	GenerationLeaseID         string
	GenerationRecoveryFenceID string
	GatewayURL                string
	ExecutorSocket            string
	LocalControlURL           string
}

// Process represents a running agent process.
type Process interface {
	Wait() error
	Stop() error
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
	AgentWrappingKey(ctx context.Context, agentID string) (string, error)
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
	BearerTTL       time.Duration // lifetime of the local-control bearer; 0 uses 8h
	IdleTimeout     time.Duration // cold-mode idle stop delay; 0 disables auto-stop
	ShutdownTimeout time.Duration // bound for in-flight starts during StopAll; 0 uses 5s
	Now             func() time.Time
	// SkipAgentIDs are agents already managed externally (e.g. the legacy
	// single-process dev agent). EnsureRunning is a no-op for them.
	SkipAgentIDs []string
}

// Manager owns the per-agent runtime lifecycle.
type Manager struct {
	cfg      Config
	mu       sync.Mutex
	running  map[string]*agentRuntime
	starting map[string]*startAttempt
	closing  bool
	now      func() time.Time
	idleStop time.Duration
	stopWait time.Duration
	skip     map[string]bool
}

type agentRuntime struct {
	process    Process
	lastActive time.Time
	warmth     string
}

type startAttempt struct {
	done       chan struct{}
	cancel     context.CancelFunc
	err        error
	cleanupErr error
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
	if cfg.BearerTTL <= 0 {
		cfg.BearerTTL = 8 * time.Hour
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
		now:      now,
		idleStop: cfg.IdleTimeout,
		stopWait: cfg.ShutdownTimeout,
		skip:     skip,
	}, nil
}

// EnsureRunning starts the agent if it is not already running and records the
// call as activity. It is called on 呼びかけ (direct-chat connection). Agents
// in the SkipAgentIDs set are left to their external manager.
func (m *Manager) EnsureRunning(ctx context.Context, agentID string) error {
	m.mu.Lock()
	if m.closing {
		m.mu.Unlock()
		return ErrManagerClosed
	}
	if m.skip[agentID] {
		m.mu.Unlock()
		return nil
	}
	if rt, ok := m.running[agentID]; ok {
		rt.lastActive = m.now()
		m.mu.Unlock()
		return nil
	}
	if attempt, ok := m.starting[agentID]; ok {
		m.mu.Unlock()
		select {
		case <-attempt.done:
			return attempt.err
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

	runtime, err := m.startRuntime(startContext, agentID)
	cancelStart()
	m.mu.Lock()
	if !m.closing {
		if err == nil {
			m.running[agentID] = runtime
		}
		attempt.err = err
		delete(m.starting, agentID)
		close(attempt.done)
		m.mu.Unlock()
		return err
	}
	m.mu.Unlock()

	// StopAll won the publication race. A spawner that returned success after
	// manager cancellation must not escape shutdown or enter the running map.
	if runtime != nil {
		if stopErr := runtime.process.Stop(); stopErr != nil {
			attempt.cleanupErr = fmt.Errorf("stop late agent %s: %w", agentID, stopErr)
		}
	}
	m.mu.Lock()
	attempt.err = errors.Join(ErrManagerClosed, attempt.cleanupErr)
	delete(m.starting, agentID)
	close(attempt.done)
	m.mu.Unlock()
	return attempt.err
}

func (m *Manager) startRuntime(ctx context.Context, agentID string) (*agentRuntime, error) {
	warmth, err := m.cfg.Resolver.AgentWarmth(ctx, agentID)
	if err != nil {
		return nil, fmt.Errorf("resolve warmth for %s: %w", agentID, err)
	}
	if warmth == "" {
		warmth = WarmthCold
	}
	wrappingKey, err := m.cfg.Resolver.AgentWrappingKey(ctx, agentID)
	if err != nil {
		return nil, fmt.Errorf("resolve wrapping key for %s: %w", agentID, err)
	}
	now := m.now()
	config := AgentRuntimeConfig{
		AgentID:                   agentID,
		StateDir:                  fmt.Sprintf("%s/%s", m.cfg.StateRoot, agentID),
		WorkspaceDir:              fmt.Sprintf("%s/%s", m.cfg.WorkspaceRoot, agentID),
		WrappingKey:               wrappingKey,
		Bearer:                    deriveCredential(m.cfg.SharedBearer, agentID),
		Nonce:                     deriveCredential(m.cfg.SharedNonce, agentID),
		BearerExpiresAtUnix:       now.Add(m.cfg.BearerTTL).Unix(),
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

// Touch records activity for a running agent (an open direct-chat connection).
func (m *Manager) Touch(agentID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if rt, ok := m.running[agentID]; ok {
		rt.lastActive = m.now()
	}
}

// Running reports whether an agent is currently running.
func (m *Manager) Running(agentID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.running[agentID]
	return ok
}

// Stop terminates a running agent.
func (m *Manager) Stop(agentID string) error {
	for {
		m.mu.Lock()
		if stop := m.stopping[agentID]; stop != nil {
			m.mu.Unlock()
			<-stop.done
			return stop.err
		}
		if start := m.starting[agentID]; start != nil {
			m.mu.Unlock()
			<-start.done
			continue
		}
		rt, ok := m.running[agentID]
		if !ok {
			m.mu.Unlock()
			return nil
		}
		m.mu.Unlock()
		return m.stopRuntime(agentID, rt)
	}
}

func (m *Manager) stopRuntime(agentID string, runtime *agentRuntime) error {
	m.mu.Lock()
	if current := m.running[agentID]; current != runtime {
		m.mu.Unlock()
		return nil
	}
	if stop := m.stopping[agentID]; stop != nil {
		m.mu.Unlock()
		<-stop.done
		return stop.err
	}
	stop := &agentStop{done: make(chan struct{})}
	m.stopping[agentID] = stop
	m.mu.Unlock()

	err := runtime.process.Stop()
	m.mu.Lock()
	stop.err = err
	if m.running[agentID] == runtime {
		delete(m.running, agentID)
	}
	delete(m.stopping, agentID)
	close(stop.done)
	m.mu.Unlock()
	return err
}

// StopIdleCold stops any running cold-mode agent that has been idle longer than
// the configured IdleTimeout. Warm-mode agents are never stopped. Returns the
// agent ids that were stopped.
func (m *Manager) StopIdleCold() []string {
	if m.idleStop <= 0 {
		return nil
	}
	now := m.now()
	var stopped []string
	m.mu.Lock()
	for agentID, rt := range m.running {
		if rt.warmth == WarmthWarm {
			continue
		}
		if now.Sub(rt.lastActive) >= m.idleStop {
			stopped = append(stopped, agentID)
		}
	}
	m.mu.Unlock()
	for _, agentID := range stopped {
		_ = m.Stop(agentID)
	}
	return stopped
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
	m.mu.Lock()
	m.closing = true
	runtimes := make(map[string]*agentRuntime, len(m.running))
	for id, runtime := range m.running {
		runtimes[id] = runtime
		delete(m.running, id)
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
	for id, runtime := range runtimes {
		if err := runtime.process.Stop(); err != nil {
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
