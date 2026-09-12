package spawn

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type externalTestProcess struct {
	done            chan struct{}
	once            sync.Once
	mu              sync.Mutex
	stops, detaches int
}

func (p *externalTestProcess) Wait() error { <-p.done; return nil }
func (p *externalTestProcess) Stop() error {
	p.mu.Lock()
	p.stops++
	p.mu.Unlock()
	p.once.Do(func() { close(p.done) })
	return nil
}
func (p *externalTestProcess) Detach() error {
	p.mu.Lock()
	p.detaches++
	p.mu.Unlock()
	p.once.Do(func() { close(p.done) })
	return nil
}

type externalTestSpawner struct {
	process          *externalTestProcess
	entered, release chan struct{}
}

func (s *externalTestSpawner) Spawn(context.Context, AgentRuntimeConfig) (Process, error) {
	if s.entered != nil {
		close(s.entered)
		<-s.release
	}
	return s.process, nil
}

func TestDetachAllKeepsExternalComputeAndClosesAdmission(t *testing.T) {
	for _, duringStart := range []bool{false, true} {
		t.Run(map[bool]string{false: "established", true: "late-start"}[duringStart], func(t *testing.T) {
			process := &externalTestProcess{done: make(chan struct{})}
			spawner := &externalTestSpawner{process: process}
			if duringStart {
				spawner.entered = make(chan struct{})
				spawner.release = make(chan struct{})
			}
			manager, err := New(Config{Spawner: spawner, Resolver: fakeResolver{keys: map[string]string{"agent": "key"}}, ShutdownTimeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			started := make(chan error, 1)
			go func() { started <- manager.EnsureRunning(context.Background(), "agent") }()
			if duringStart {
				<-spawner.entered
			} else if err := <-started; err != nil {
				t.Fatal(err)
			}
			detached := make(chan error, 1)
			go func() { detached <- manager.DetachAll() }()
			if duringStart {
				deadline := time.Now().Add(time.Second)
				for {
					manager.mu.Lock()
					closing := manager.closing
					manager.mu.Unlock()
					if closing {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("detach did not close admission")
					}
					time.Sleep(time.Millisecond)
				}
				close(spawner.release)
				if err := <-started; !errors.Is(err, ErrManagerClosed) {
					t.Fatalf("late start error=%v", err)
				}
			}
			if err := <-detached; err != nil {
				t.Fatal(err)
			}
			process.mu.Lock()
			stops, detaches := process.stops, process.detaches
			process.mu.Unlock()
			if stops != 0 || detaches != 1 {
				t.Fatalf("stop=%d detach=%d", stops, detaches)
			}
			if err := manager.EnsureRunning(context.Background(), "agent"); !errors.Is(err, ErrManagerClosed) {
				t.Fatal("closed manager admitted work")
			}
		})
	}
}
