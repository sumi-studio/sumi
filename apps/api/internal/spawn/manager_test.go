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
	done    chan struct{}
	once    sync.Once
}

func (p *fakeProcess) Wait() error {
	if p.done == nil {
		return p.waitErr
	}
	<-p.done
	return p.waitErr
}
func (p *fakeProcess) Stop() error {
	p.mu.Lock()
	p.stopped = true
	p.mu.Unlock()
	if p.done != nil {
		p.once.Do(func() { close(p.done) })
	}
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

type delayedWaitProcess struct {
	exited       chan struct{}
	waitRelease  chan struct{}
	waitReturned chan struct{}
	once         sync.Once
}

func (p *delayedWaitProcess) Wait() error {
	<-p.exited
	<-p.waitRelease
	close(p.waitReturned)
	return nil
}

func (p *delayedWaitProcess) Stop() error {
	p.once.Do(func() { close(p.exited) })
	return nil
}

type sequenceSpawner struct {
	mu        sync.Mutex
	processes []Process
	next      int
}

func (s *sequenceSpawner) Spawn(_ context.Context, _ AgentRuntimeConfig) (Process, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	process := s.processes[s.next]
	s.next++
	return process, nil
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
		return &fakeProcess{done: make(chan struct{})}, nil
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

func (r fakeResolver) AgentWrappingKey(_ context.Context, agentID string) (WrappingKeyMaterial, error) {
	if r.keyErr != nil {
		return WrappingKeyMaterial{}, r.keyErr
	}
	return WrappingKeyMaterial{ID: "test/" + agentID, Bytes: r.keys[agentID]}, nil
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
	if c1.WrappingKey.Bytes != "k1" || c1.WrappingKey.ID != "test/a1" {
		t.Fatalf("a1 wrapping key: got %#v", c1.WrappingKey)
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

func TestExitedRuntimeIsEvictedAndCanRestart(t *testing.T) {
	spawner := newFakeSpawner()
	mgr, err := New(Config{Spawner: spawner, Resolver: fakeResolver{}})
	if err != nil {
		t.Fatal(err)
	}
	const agentID = "crashed-agent"
	if err := mgr.EnsureRunning(context.Background(), agentID); err != nil {
		t.Fatal(err)
	}
	spawner.mu.Lock()
	first := spawner.processes[agentID]
	spawner.mu.Unlock()
	first.once.Do(func() { close(first.done) })

	deadline := time.Now().Add(time.Second)
	for mgr.Running(agentID) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if mgr.Running(agentID) {
		t.Fatal("exited runtime remained cached as running")
	}
	if err := mgr.EnsureRunning(context.Background(), agentID); err != nil {
		t.Fatal(err)
	}
	spawner.mu.Lock()
	spawnCount := len(spawner.spawns)
	spawner.mu.Unlock()
	if spawnCount != 2 || !mgr.Running(agentID) {
		t.Fatalf("crashed runtime was not replaced: spawns=%d running=%v", spawnCount, mgr.Running(agentID))
	}
}

func TestPriorRuntimeWaitCannotEvictReplacement(t *testing.T) {
	first := &delayedWaitProcess{exited: make(chan struct{}), waitRelease: make(chan struct{}), waitReturned: make(chan struct{})}
	second := &fakeProcess{done: make(chan struct{})}
	spawner := &sequenceSpawner{processes: []Process{first, second}}
	mgr, err := New(Config{Spawner: spawner, Resolver: fakeResolver{}})
	if err != nil {
		t.Fatal(err)
	}
	const agentID = "replacement-agent"
	if err := mgr.EnsureRunning(context.Background(), agentID); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Stop(agentID); err != nil {
		t.Fatal(err)
	}
	if err := mgr.EnsureRunning(context.Background(), agentID); err != nil {
		t.Fatal(err)
	}
	close(first.waitRelease)
	<-first.waitReturned
	time.Sleep(10 * time.Millisecond)
	if !mgr.Running(agentID) {
		t.Fatal("prior runtime waiter deleted the replacement")
	}
}

func TestEnsureRunningWaitsForPriorStopJoin(t *testing.T) {
	first := &blockingStopProcess{
		done:        make(chan struct{}),
		stopEntered: make(chan struct{}),
		releaseStop: make(chan struct{}),
	}
	spawner := &replacementRaceSpawner{first: first}
	mgr, err := New(Config{Spawner: spawner, Resolver: fakeResolver{}})
	if err != nil {
		t.Fatal(err)
	}
	const agentID = "serialized-replacement"
	if err := mgr.EnsureRunning(context.Background(), agentID); err != nil {
		t.Fatal(err)
	}
	stopResult := make(chan error, 1)
	go func() { stopResult <- mgr.Stop(agentID) }()
	<-first.stopEntered
	replacementResult := make(chan error, 1)
	go func() { replacementResult <- mgr.EnsureRunning(context.Background(), agentID) }()
	select {
	case err := <-replacementResult:
		t.Fatalf("replacement escaped before prior Stop joined: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	if count := spawner.count(); count != 1 {
		t.Fatalf("spawned replacement before Stop join: count=%d", count)
	}
	close(first.releaseStop)
	if err := <-stopResult; err != nil {
		t.Fatal(err)
	}
	if err := <-replacementResult; err != nil {
		t.Fatal(err)
	}
	if count := spawner.count(); count != 2 || !mgr.Running(agentID) {
		t.Fatalf("replacement did not start after Stop join: count=%d running=%v", count, mgr.Running(agentID))
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
	stopped, err := mgr.StopIdleCold()
	if err != nil {
		t.Fatal(err)
	}
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
	stopped, err := mgr.StopIdleCold()
	if err != nil {
		t.Fatal(err)
	}
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
	if stopped, err := mgr.StopIdleCold(); err != nil || len(stopped) != 0 {
		t.Fatalf("touched cold agent should not be stopped, got %v", stopped)
	}
	// Advance past the timeout since the last touch.
	base = base.Add(2 * time.Minute)
	if stopped, err := mgr.StopIdleCold(); err != nil || len(stopped) != 1 {
		t.Fatalf("cold agent should stop after idle, got %v", stopped)
	}
}

func TestIdleClaimKeepsClosedTabWorkAndRechecksConcurrentActivity(t *testing.T) {
	spawner := newFakeSpawner()
	now := time.Now()
	busy := true
	var beforeClaim func()
	mgr, err := New(Config{
		Spawner: spawner, Resolver: fakeResolver{keys: map[string]string{"agent": "key"}},
		IdleTimeout: time.Minute, Now: func() time.Time { return now },
		ClaimIdle: func(_ string, claim func() bool) (bool, error) {
			if busy {
				return false, nil
			}
			if beforeClaim != nil {
				beforeClaim()
			}
			return claim(), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.EnsureRunning(context.Background(), "agent"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	if stopped, err := mgr.StopIdleCold(); err != nil || len(stopped) != 0 {
		t.Fatalf("busy: %v %v", stopped, err)
	}
	if spawner.processes["agent"].isStopped() {
		t.Fatal("closed-tab active work stopped")
	}
	busy = false
	// Selection has already happened when a new admission touches the manager.
	beforeClaim = func() {
		if err := mgr.EnsureRunning(context.Background(), "agent"); err != nil {
			t.Fatal(err)
		}
	}
	if stopped, err := mgr.StopIdleCold(); err != nil || len(stopped) != 0 {
		t.Fatalf("racing request: %v %v", stopped, err)
	}
	beforeClaim = nil
	now = now.Add(2 * time.Minute)
	if stopped, err := mgr.StopIdleCold(); err != nil || len(stopped) != 1 {
		t.Fatalf("completed idle: %v %v", stopped, err)
	}
}

func TestIdleStopFailureIsReportedNotSuccessful(t *testing.T) {
	spawner := newFakeSpawner()
	now := time.Now()
	mgr, err := New(Config{Spawner: spawner, Resolver: fakeResolver{keys: map[string]string{"agent": "key"}}, IdleTimeout: time.Minute, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.EnsureRunning(context.Background(), "agent"); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("stop failed")
	spawner.processes["agent"].stopErr = failure
	now = now.Add(2 * time.Minute)
	stopped, err := mgr.StopIdleCold()
	if len(stopped) != 0 || !errors.Is(err, failure) {
		t.Fatalf("got %v %v", stopped, err)
	}
}

func TestAdmissionHoldPreventsIdleStopEvenAfterTimeout(t *testing.T) {
	spawner := newFakeSpawner()
	now := time.Now()
	mgr, err := New(Config{Spawner: spawner, Resolver: fakeResolver{keys: map[string]string{"agent": "key"}}, IdleTimeout: time.Minute, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.EnsureRunning(context.Background(), "agent"); err != nil {
		t.Fatal(err)
	}
	release, err := mgr.HoldAdmission("agent")
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	if stopped, err := mgr.StopIdleCold(); err != nil || len(stopped) != 0 {
		t.Fatalf("held admission: %v %v", stopped, err)
	}
	release()
	release() // no underflow on repeated cleanup
	now = now.Add(2 * time.Minute)
	if stopped, err := mgr.StopIdleCold(); err != nil || len(stopped) != 1 {
		t.Fatalf("released admission: %v %v", stopped, err)
	}
}

func TestIdleClaimRejectsAdmissionUntilReplacementGeneration(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "cold", true: "explicit"}[explicit], func(t *testing.T) {
			old := &blockingStopProcess{done: make(chan struct{}), stopEntered: make(chan struct{}), releaseStop: make(chan struct{})}
			fresh := &fakeProcess{done: make(chan struct{})}
			spawner := &sequenceSpawner{processes: []Process{old, fresh}}
			now := time.Now()
			mgr, err := New(Config{Spawner: spawner, Resolver: fakeResolver{keys: map[string]string{"agent": "key"}}, IdleTimeout: time.Minute, Now: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			if err := mgr.EnsureRunning(context.Background(), "agent"); err != nil {
				t.Fatal(err)
			}
			now = now.Add(2 * time.Minute)
			reaped := make(chan error, 1)
			go func() {
				var err error
				if explicit {
					_, err = mgr.StopIfIdle("agent")
				} else {
					_, err = mgr.StopIdleCold()
				}
				reaped <- err
			}()
			<-old.stopEntered
			if release, err := mgr.HoldAdmission("agent"); err == nil {
				release()
				t.Fatal("accepted input into stopping generation")
			}
			ready := make(chan error, 1)
			go func() { ready <- mgr.EnsureRunning(context.Background(), "agent") }()
			select {
			case err := <-ready:
				t.Fatalf("ensure returned before stop completed: %v", err)
			default:
			}
			close(old.releaseStop)
			if err := <-reaped; err != nil {
				t.Fatal(err)
			}
			if err := <-ready; err != nil {
				t.Fatal(err)
			}
			release, err := mgr.HoldAdmission("agent")
			if err != nil {
				t.Fatal(err)
			}
			release()
			if fresh.isStopped() {
				t.Fatal("old idle stop killed replacement")
			}
			if err := mgr.Stop("agent"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStopIfIdleChecksWorkAdmissionAndActivityWithoutAgeOrWarmth(t *testing.T) {
	spawner := newFakeSpawner()
	busy := true
	var beforeClaim func()
	mgr, err := New(Config{
		Spawner: spawner, Resolver: fakeResolver{keys: map[string]string{"agent": "key"}, warmth: map[string]string{"agent": WarmthWarm}},
		ClaimIdle: func(_ string, claim func() bool) (bool, error) {
			if busy {
				return false, nil
			}
			if beforeClaim != nil {
				beforeClaim()
			}
			return claim(), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.EnsureRunning(context.Background(), "agent"); err != nil {
		t.Fatal(err)
	}
	assertSkipped := func() {
		t.Helper()
		if stopped, err := mgr.StopIfIdle("agent"); err != nil || stopped {
			t.Fatalf("expected skip: %v %v", stopped, err)
		}
		if !mgr.Running("agent") {
			t.Fatal("lost live runtime")
		}
	}
	assertSkipped()
	busy = false
	release, err := mgr.HoldAdmission("agent")
	if err != nil {
		t.Fatal(err)
	}
	assertSkipped()
	release()
	beforeClaim = func() { mgr.Touch("agent") }
	assertSkipped()
	beforeClaim = nil
	if stopped, err := mgr.StopIfIdle("agent"); err != nil || !stopped {
		t.Fatalf("idle warm runtime: %v %v", stopped, err)
	}
	if stopped, err := mgr.StopIfIdle("agent"); err != nil || !stopped {
		t.Fatalf("absent runtime: %v %v", stopped, err)
	}
}

func TestStopIfIdleFailureRetainsLiveRuntime(t *testing.T) {
	mgr, err := New(Config{Spawner: newFakeSpawner(), Resolver: fakeResolver{}})
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("still alive")
	process := &fakeProcess{stopErr: failure}
	// No exit signal: a failed stop must keep this exact runtime available for
	// later inspection/cleanup instead of pretending the process disappeared.
	rt := &agentRuntime{process: process}
	mgr.running["agent"] = rt
	if stopped, err := mgr.StopIfIdle("agent"); stopped || !errors.Is(err, failure) {
		t.Fatalf("failed stop: %v %v", stopped, err)
	}
	if mgr.running["agent"] != rt || mgr.stopping["agent"] != nil {
		t.Fatal("failed stop lost runtime or retained reservation")
	}
}

func TestStopIfIdleAbsentRequiresNoLifecycleOperation(t *testing.T) {
	for _, state := range []string{"absent", "starting", "stopping", "closing", "external"} {
		t.Run(state, func(t *testing.T) {
			mgr, err := New(Config{Spawner: newFakeSpawner(), Resolver: fakeResolver{}})
			if err != nil {
				t.Fatal(err)
			}
			switch state {
			case "starting":
				mgr.starting["agent"] = &startAttempt{done: make(chan struct{})}
			case "stopping":
				mgr.stopping["agent"] = &stopAttempt{done: make(chan struct{})}
			case "closing":
				mgr.closing = true
			case "external":
				mgr.skip["agent"] = true
			}
			ready, err := mgr.StopIfIdle("agent")
			if err != nil || ready != (state == "absent") {
				t.Fatalf("ready = %v, error = %v", ready, err)
			}
		})
	}
}
