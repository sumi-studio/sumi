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

// ProcessSpawner starts an agent process for the given config. Implementations
// are pluggable so the lifecycle logic is testable without a real binary.
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
	starting map[string]*agentStart
	stopping map[string]*agentStop
	now      func() time.Time
	idleStop time.Duration
	skip     map[string]bool
}

type agentRuntime struct {
	process    Process
	lastActive time.Time
	warmth     string
}

type agentStart struct {
	done chan struct{}
	err  error
}

type agentStop struct {
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
	if cfg.BearerTTL <= 0 {
		cfg.BearerTTL = 8 * time.Hour
	}
	skip := make(map[string]bool, len(cfg.SkipAgentIDs))
	for _, id := range cfg.SkipAgentIDs {
		skip[id] = true
	}
	return &Manager{
		cfg:      cfg,
		running:  make(map[string]*agentRuntime),
		starting: make(map[string]*agentStart),
		stopping: make(map[string]*agentStop),
		now:      now,
		idleStop: cfg.IdleTimeout,
		skip:     skip,
	}, nil
}

// EnsureRunning starts the agent if it is not already running and records the
// call as activity. It is called on 呼びかけ (direct-chat connection). Agents
// in the SkipAgentIDs set are left to their external manager.
func (m *Manager) EnsureRunning(ctx context.Context, agentID string) error {
	if m.skip[agentID] {
		return nil
	}
	for {
		m.mu.Lock()
		stop := m.stopping[agentID]
		m.mu.Unlock()
		if stop == nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-stop.done:
			if stop.err != nil {
				return stop.err
			}
		}
	}
	m.mu.Lock()
	if stop := m.stopping[agentID]; stop != nil {
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-stop.done:
			if stop.err != nil {
				return stop.err
			}
			return m.EnsureRunning(ctx, agentID)
		}
	}
	if rt, ok := m.running[agentID]; ok {
		rt.lastActive = m.now()
		m.mu.Unlock()
		return nil
	}
	if start, ok := m.starting[agentID]; ok {
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-start.done:
			return start.err
		}
	}
	start := &agentStart{done: make(chan struct{})}
	m.starting[agentID] = start
	m.mu.Unlock()
	finishStart := func(err error) {
		m.mu.Lock()
		start.err = err
		delete(m.starting, agentID)
		close(start.done)
		m.mu.Unlock()
	}

	warmth, err := m.cfg.Resolver.AgentWarmth(ctx, agentID)
	if err != nil {
		err = fmt.Errorf("resolve warmth for %s: %w", agentID, err)
		finishStart(err)
		return err
	}
	if warmth == "" {
		warmth = WarmthCold
	}
	wrappingKey, err := m.cfg.Resolver.AgentWrappingKey(ctx, agentID)
	if err != nil {
		err = fmt.Errorf("resolve wrapping key for %s: %w", agentID, err)
		finishStart(err)
		return err
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
		err = fmt.Errorf("spawn agent %s: %w", agentID, err)
		finishStart(err)
		return err
	}
	m.mu.Lock()
	runtime := &agentRuntime{process: process, lastActive: m.now(), warmth: warmth}
	m.running[agentID] = runtime
	start.err = nil
	delete(m.starting, agentID)
	close(start.done)
	m.mu.Unlock()
	go m.watchProcess(agentID, runtime)
	return nil
}

func (m *Manager) watchProcess(agentID string, runtime *agentRuntime) {
	if err := runtime.process.Wait(); err != nil {
		_ = m.stopRuntime(agentID, runtime)
		return
	}
	m.mu.Lock()
	if m.running[agentID] == runtime {
		delete(m.running, agentID)
	}
	m.mu.Unlock()
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

// StopAll stops every running agent. Used on graceful shutdown.
func (m *Manager) StopAll() error {
	m.mu.Lock()
	ids := make([]string, 0, len(m.running))
	seen := make(map[string]bool, len(m.running)+len(m.starting))
	for id := range m.running {
		ids = append(ids, id)
		seen[id] = true
	}
	for id := range m.starting {
		if !seen[id] {
			ids = append(ids, id)
		}
	}
	m.mu.Unlock()
	errCh := make(chan error, len(ids))
	var wait sync.WaitGroup
	for _, id := range ids {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := m.Stop(id); err != nil {
				errCh <- fmt.Errorf("stop agent %s: %w", id, err)
			}
		}()
	}
	wait.Wait()
	close(errCh)
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
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
