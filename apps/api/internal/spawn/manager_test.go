package spawn

import (
	"context"
	"sync"
	"testing"
	"time"
)

type fakeProcess struct {
	mu      sync.Mutex
	stopped bool
	waitErr error
	stopErr error
	done    chan struct{}
	once    sync.Once
}

func (p *fakeProcess) Wait() error {
	<-p.done
	return p.waitErr
}
func (p *fakeProcess) Stop() error {
	p.mu.Lock()
	p.stopped = true
	p.mu.Unlock()
	p.once.Do(func() { close(p.done) })
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

type blockingStopProcess struct {
	done        chan struct{}
	stopEntered chan struct{}
	releaseStop chan struct{}
	once        sync.Once
}

func (p *blockingStopProcess) Wait() error {
	<-p.done
	return nil
}

func (p *blockingStopProcess) Stop() error {
	p.once.Do(func() {
		close(p.stopEntered)
		<-p.releaseStop
		close(p.done)
	})
	return nil
}

type replacementRaceSpawner struct {
	mu        sync.Mutex
	first     *blockingStopProcess
	processes []Process
}

type parallelStopProcess struct {
	id      string
	entered chan<- string
	release <-chan struct{}
	done    chan struct{}
	once    sync.Once
}

func (p *parallelStopProcess) Wait() error { <-p.done; return nil }
func (p *parallelStopProcess) Stop() error {
	p.once.Do(func() {
		p.entered <- p.id
		<-p.release
		close(p.done)
	})
	return nil
}

type parallelStopSpawner struct {
	entered chan string
	release chan struct{}
	mu      sync.Mutex
	spawns  int
}

type blockingSpawnSpawner struct {
	entered chan struct{}
	release chan struct{}
	process *fakeProcess
}

func (s *blockingSpawnSpawner) Spawn(_ context.Context, _ AgentRuntimeConfig) (Process, error) {
	close(s.entered)
	<-s.release
	return s.process, nil
}

func (s *parallelStopSpawner) Spawn(_ context.Context, config AgentRuntimeConfig) (Process, error) {
	s.mu.Lock()
	s.spawns++
	s.mu.Unlock()
	return &parallelStopProcess{id: config.AgentID, entered: s.entered, release: s.release, done: make(chan struct{})}, nil
}

func (s *replacementRaceSpawner) Spawn(_ context.Context, _ AgentRuntimeConfig) (Process, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var process Process
	if len(s.processes) == 0 {
		process = s.first
	} else {
		process = &fakeProcess{done: make(chan struct{})}
	}
	s.processes = append(s.processes, process)
	return process, nil
}

func (s *replacementRaceSpawner) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.processes)
}

func newFakeSpawner() *fakeSpawner {
	return &fakeSpawner{processes: make(map[string]*fakeProcess)}
}

func (s *fakeSpawner) Spawn(_ context.Context, config AgentRuntimeConfig) (Process, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := &fakeProcess{done: make(chan struct{})}
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

func TestEnsureRunningSingleflightsConcurrentCallsPerPAID(t *testing.T) {
	spawner := newFakeSpawner()
	mgr, err := New(Config{
		Spawner:       spawner,
		Resolver:      fakeResolver{},
		SharedBearer:  "bearer",
		SharedNonce:   "nonce",
		StateRoot:     t.TempDir(),
		WorkspaceRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	const callers = 32
	const agentID = "singleflight-agent"
	var wait sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			errs <- mgr.EnsureRunning(context.Background(), agentID)
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	spawner.mu.Lock()
	defer spawner.mu.Unlock()
	if got := len(spawner.spawns); got != 1 {
		t.Fatalf("spawned %d processes for one PAID, want 1", got)
	}
}

func TestEnsureRunningWaitsForExactPriorStopBeforeReplacement(t *testing.T) {
	first := &blockingStopProcess{
		done:        make(chan struct{}),
		stopEntered: make(chan struct{}),
		releaseStop: make(chan struct{}),
	}
	spawner := &replacementRaceSpawner{first: first}
	mgr, err := New(Config{
		Spawner:       spawner,
		Resolver:      fakeResolver{},
		StateRoot:     t.TempDir(),
		WorkspaceRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	const paid = "0198f0f4-9b72-7000-8000-000000000001"
	if err := mgr.EnsureRunning(context.Background(), paid); err != nil {
		t.Fatal(err)
	}
	stopDone := make(chan error, 1)
	go func() { stopDone <- mgr.Stop(paid) }()
	<-first.stopEntered

	replacementDone := make(chan error, 1)
	go func() { replacementDone <- mgr.EnsureRunning(context.Background(), paid) }()
	select {
	case err := <-replacementDone:
		t.Fatalf("replacement escaped the prior stop/join: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if got := spawner.count(); got != 1 {
		t.Fatalf("spawned replacement before prior stop completed: %d", got)
	}

	close(first.releaseStop)
	if err := <-stopDone; err != nil {
		t.Fatal(err)
	}
	if err := <-replacementDone; err != nil {
		t.Fatal(err)
	}
	if got := spawner.count(); got != 2 || !mgr.Running(paid) {
		t.Fatalf("replacement did not become the sole running epoch: spawns=%d running=%v", got, mgr.Running(paid))
	}
}

func TestStopAllJoinsThreePAIDsConcurrentlyBeforeRestart(t *testing.T) {
	spawner := &parallelStopSpawner{entered: make(chan string, 3), release: make(chan struct{})}
	mgr, err := New(Config{Spawner: spawner, Resolver: fakeResolver{}})
	if err != nil {
		t.Fatal(err)
	}
	paids := []string{"agent-a", "agent-b", "agent-c"}
	for _, paid := range paids {
		if err := mgr.EnsureRunning(context.Background(), paid); err != nil {
			t.Fatal(err)
		}
	}
	stopped := make(chan error, 1)
	go func() { stopped <- mgr.StopAll() }()
	seen := make(map[string]bool, 3)
	for range paids {
		select {
		case paid := <-spawner.entered:
			seen[paid] = true
		case <-time.After(time.Second):
			t.Fatalf("StopAll serialized or stalled before all PAIDs entered teardown: %v", seen)
		}
	}
	close(spawner.release)
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	for _, paid := range paids {
		if mgr.Running(paid) {
			t.Fatalf("%s remained running after StopAll", paid)
		}
		if err := mgr.EnsureRunning(context.Background(), paid); err != nil {
			t.Fatal(err)
		}
		if !mgr.Running(paid) {
			t.Fatalf("%s did not restart after the joined shutdown", paid)
		}
	}
	spawner.mu.Lock()
	defer spawner.mu.Unlock()
	if spawner.spawns != 6 {
		t.Fatalf("restart spawn count=%d, want 6", spawner.spawns)
	}
}

func TestStopAllJoinsBlockedStartAndStopsCommittedProcess(t *testing.T) {
	process := &fakeProcess{done: make(chan struct{})}
	spawner := &blockingSpawnSpawner{entered: make(chan struct{}), release: make(chan struct{}), process: process}
	mgr, err := New(Config{Spawner: spawner, Resolver: fakeResolver{}})
	if err != nil {
		t.Fatal(err)
	}
	ensureDone := make(chan error, 1)
	go func() { ensureDone <- mgr.EnsureRunning(context.Background(), "starting-agent") }()
	<-spawner.entered
	stopDone := make(chan error, 1)
	go func() { stopDone <- mgr.StopAll() }()
	select {
	case err := <-stopDone:
		t.Fatalf("StopAll returned before in-flight Spawn committed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(spawner.release)
	if err := <-ensureDone; err != nil {
		t.Fatal(err)
	}
	if err := <-stopDone; err != nil {
		t.Fatal(err)
	}
	if !process.isStopped() || mgr.Running("starting-agent") {
		t.Fatalf("process committed after StopAll: stopped=%v running=%v", process.isStopped(), mgr.Running("starting-agent"))
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
