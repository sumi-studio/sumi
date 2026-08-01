package spawn

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeProcess struct {
	mu      sync.Mutex
	stopped bool
	waitErr error
	stopErr error
}

func (p *fakeProcess) Wait() error { return p.waitErr }
func (p *fakeProcess) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopped = true
	return p.stopErr
}

func (p *fakeProcess) isStopped() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stopped
}

type fakeSpawner struct {
	mu        sync.Mutex
	spawns    []AgentRuntimeConfig
	processes map[string]*fakeProcess
}

func newFakeSpawner() *fakeSpawner {
	return &fakeSpawner{processes: make(map[string]*fakeProcess)}
}

func (s *fakeSpawner) Spawn(_ context.Context, config AgentRuntimeConfig) (Process, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := &fakeProcess{}
	s.processes[config.AgentID] = p
	s.spawns = append(s.spawns, config)
	return p, nil
}

func (s *fakeSpawner) config(agentID string) AgentRuntimeConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.spawns {
		if c.AgentID == agentID {
			return c
		}
	}
	return AgentRuntimeConfig{}
}

type blockingSpawner struct {
	mu      sync.Mutex
	spawns  int
	started chan struct{}
	release chan struct{}
}

type lateSuccessSpawner struct {
	started  chan struct{}
	canceled chan struct{}
	release  chan struct{}
	process  *fakeProcess
}

func newLateSuccessSpawner() *lateSuccessSpawner {
	return &lateSuccessSpawner{
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
		release:  make(chan struct{}),
		process:  &fakeProcess{},
	}
}

func (s *lateSuccessSpawner) Spawn(ctx context.Context, _ AgentRuntimeConfig) (Process, error) {
	close(s.started)
	<-ctx.Done()
	close(s.canceled)
	<-s.release
	return s.process, nil
}

func newBlockingSpawner() *blockingSpawner {
	return &blockingSpawner{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *blockingSpawner) Spawn(ctx context.Context, _ AgentRuntimeConfig) (Process, error) {
	s.mu.Lock()
	s.spawns++
	if s.spawns == 1 {
		close(s.started)
	}
	s.mu.Unlock()
	select {
	case <-s.release:
		return &fakeProcess{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *blockingSpawner) spawnCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.spawns
}

type fakeResolver struct {
	keys   map[string]string
	warmth map[string]string
	keyErr error
}

func (r fakeResolver) AgentWrappingKey(_ context.Context, agentID string) (string, error) {
	if r.keyErr != nil {
		return "", r.keyErr
	}
	return r.keys[agentID], nil
}

func (r fakeResolver) AgentWarmth(_ context.Context, agentID string) (string, error) {
	return r.warmth[agentID], nil
}

func TestEnsureRunningSpawnsPerAgent(t *testing.T) {
	spawner := newFakeSpawner()
	now := time.Now()
	mgr, err := New(Config{
		Spawner:       spawner,
		Resolver:      fakeResolver{keys: map[string]string{"a1": "k1", "a2": "k2"}},
		StateRoot:     "/tmp/state",
		WorkspaceRoot: "/tmp/ws",
		SharedBearer:  "bearer",
		SharedNonce:   "nonce",
		Now:           func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	if err := mgr.EnsureRunning(context.Background(), "a1"); err != nil {
		t.Fatalf("ensure a1: %v", err)
	}
	if err := mgr.EnsureRunning(context.Background(), "a2"); err != nil {
		t.Fatalf("ensure a2: %v", err)
	}
	// Second call for a1 is a no-op (already running).
	if err := mgr.EnsureRunning(context.Background(), "a1"); err != nil {
		t.Fatalf("ensure a1 again: %v", err)
	}
	if len(spawner.spawns) != 2 {
		t.Fatalf("expected 2 spawns, got %d", len(spawner.spawns))
	}
	if !mgr.Running("a1") || !mgr.Running("a2") {
		t.Fatal("both agents should be running")
	}
	// Per-agent wrapping key and derived bearer are passed to the spawner.
	c1 := spawner.config("a1")
	if c1.WrappingKey != "k1" {
		t.Fatalf("a1 wrapping key: got %q want k1", c1.WrappingKey)
	}
	if c1.Bearer != "bearer/a1" {
		t.Fatalf("a1 derived bearer: got %q want bearer/a1", c1.Bearer)
	}
}

func TestEnsureRunningCoalescesConcurrentStarts(t *testing.T) {
	spawner := newBlockingSpawner()
	mgr, err := New(Config{
		Spawner:      spawner,
		Resolver:     fakeResolver{keys: map[string]string{"a1": "k1"}},
		SharedBearer: "bearer",
		SharedNonce:  "nonce",
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	leaderResult := make(chan error, 1)
	go func() {
		leaderResult <- mgr.EnsureRunning(context.Background(), "a1")
	}()
	<-spawner.started

	waiterContext, cancelWaiter := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelWaiter()
	if err := mgr.EnsureRunning(waiterContext, "a1"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("concurrent waiter error = %v, want context deadline exceeded", err)
	}
	if got := spawner.spawnCount(); got != 1 {
		t.Fatalf("concurrent EnsureRunning calls spawned %d processes, want 1", got)
	}

	close(spawner.release)
	if err := <-leaderResult; err != nil {
		t.Fatalf("leader EnsureRunning: %v", err)
	}
	if err := mgr.EnsureRunning(context.Background(), "a1"); err != nil {
		t.Fatalf("EnsureRunning after start: %v", err)
	}
	if got := spawner.spawnCount(); got != 1 {
		t.Fatalf("running agent spawned again: got %d starts, want 1", got)
	}
}

func TestStopAllCancelsBlockedStartAndClosesAdmission(t *testing.T) {
	spawner := newBlockingSpawner()
	mgr, err := New(Config{
		Spawner:      spawner,
		Resolver:     fakeResolver{keys: map[string]string{"a1": "k1"}},
		SharedBearer: "bearer",
		SharedNonce:  "nonce",
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	startResult := make(chan error, 1)
	go func() {
		startResult <- mgr.EnsureRunning(context.Background(), "a1")
	}()
	<-spawner.started
	if err := mgr.StopAll(); err != nil {
		t.Fatalf("StopAll: %v", err)
	}
	if err := <-startResult; !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("canceled start error = %v, want ErrManagerClosed", err)
	}
	if err := mgr.EnsureRunning(context.Background(), "a1"); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("post-shutdown start error = %v, want ErrManagerClosed", err)
	}
	if got := spawner.spawnCount(); got != 1 {
		t.Fatalf("shutdown admitted another start: got %d starts, want 1", got)
	}
	if mgr.Running("a1") {
		t.Fatal("canceled start was published after StopAll")
	}
}

func TestStopAllStopsLateSuccessfulProcessAfterWaitTimeout(t *testing.T) {
	spawner := newLateSuccessSpawner()
	mgr, err := New(Config{
		Spawner:         spawner,
		Resolver:        fakeResolver{keys: map[string]string{"a1": "k1"}},
		SharedBearer:    "bearer",
		SharedNonce:     "nonce",
		ShutdownTimeout: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	startResult := make(chan error, 1)
	go func() {
		startResult <- mgr.EnsureRunning(context.Background(), "a1")
	}()
	<-spawner.started
	if err := mgr.StopAll(); !errors.Is(err, ErrStartShutdownTimeout) {
		t.Fatalf("StopAll error = %v, want ErrStartShutdownTimeout", err)
	}
	select {
	case <-spawner.canceled:
	default:
		t.Fatal("StopAll did not cancel the manager-owned start context")
	}
	if err := mgr.EnsureRunning(context.Background(), "a1"); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("post-shutdown start error = %v, want ErrManagerClosed", err)
	}

	close(spawner.release)
	if err := <-startResult; !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("late successful start error = %v, want ErrManagerClosed", err)
	}
	if !spawner.process.isStopped() {
		t.Fatal("late successful process survived StopAll")
	}
	if mgr.Running("a1") {
		t.Fatal("late successful process was registered after StopAll")
	}
	if err := mgr.StopAll(); err != nil {
		t.Fatalf("repeated StopAll after late cleanup: %v", err)
	}
}

func TestColdAgentStopsWhenIdle(t *testing.T) {
	spawner := newFakeSpawner()
	base := time.Now()
	mgr, err := New(Config{
		Spawner:      spawner,
		Resolver:     fakeResolver{keys: map[string]string{"cold": "k"}, warmth: map[string]string{"cold": WarmthCold}},
		SharedBearer: "b",
		SharedNonce:  "n",
		IdleTimeout:  5 * time.Minute,
		Now:          func() time.Time { return base },
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if err := mgr.EnsureRunning(context.Background(), "cold"); err != nil {
		t.Fatalf("ensure cold: %v", err)
	}
	coldProc := spawner.processes["cold"]
	// Advance past the idle timeout without activity.
	base = base.Add(6 * time.Minute)
	stopped := mgr.StopIdleCold()
	if len(stopped) != 1 || stopped[0] != "cold" {
		t.Fatalf("expected cold agent stopped, got %v", stopped)
	}
	if !coldProc.isStopped() {
		t.Fatal("cold agent process should have been stopped")
	}
	if mgr.Running("cold") {
		t.Fatal("cold agent should not be running after idle stop")
	}
}

func TestWarmAgentNeverStopsIdle(t *testing.T) {
	spawner := newFakeSpawner()
	base := time.Now()
	mgr, err := New(Config{
		Spawner:      spawner,
		Resolver:     fakeResolver{keys: map[string]string{"warm": "k"}, warmth: map[string]string{"warm": WarmthWarm}},
		SharedBearer: "b",
		SharedNonce:  "n",
		IdleTimeout:  5 * time.Minute,
		Now:          func() time.Time { return base },
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if err := mgr.EnsureRunning(context.Background(), "warm"); err != nil {
		t.Fatalf("ensure warm: %v", err)
	}
	warmProc := spawner.processes["warm"]
	base = base.Add(time.Hour)
	stopped := mgr.StopIdleCold()
	if len(stopped) != 0 {
		t.Fatalf("warm agent must not be idle-stopped, got %v", stopped)
	}
	if warmProc.isStopped() {
		t.Fatal("warm agent process should still be running")
	}
	if mgr.Warmth("warm") != WarmthWarm {
		t.Fatalf("warmth: got %q want %q", mgr.Warmth("warm"), WarmthWarm)
	}
}

func TestTouchKeepsColdAgentAlive(t *testing.T) {
	spawner := newFakeSpawner()
	base := time.Now()
	mgr, err := New(Config{
		Spawner:      spawner,
		Resolver:     fakeResolver{keys: map[string]string{"cold": "k"}, warmth: map[string]string{"cold": WarmthCold}},
		SharedBearer: "b",
		SharedNonce:  "n",
		IdleTimeout:  5 * time.Minute,
		Now:          func() time.Time { return base },
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if err := mgr.EnsureRunning(context.Background(), "cold"); err != nil {
		t.Fatalf("ensure cold: %v", err)
	}
	// Advance 4 minutes, touch (activity), advance 4 more — total since last
	// touch is 4m which is under the 5m timeout.
	base = base.Add(4 * time.Minute)
	mgr.Touch("cold")
	base = base.Add(4 * time.Minute)
	if stopped := mgr.StopIdleCold(); len(stopped) != 0 {
		t.Fatalf("touched cold agent should not be stopped, got %v", stopped)
	}
	// Advance past the timeout since the last touch.
	base = base.Add(2 * time.Minute)
	if stopped := mgr.StopIdleCold(); len(stopped) != 1 {
		t.Fatalf("cold agent should stop after idle, got %v", stopped)
	}
}
