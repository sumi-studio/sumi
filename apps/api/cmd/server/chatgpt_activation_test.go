package main

import (
	"context"
	"errors"
	"testing"

	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

type activationTestEmployer struct {
	denied bool
	held   bool
}

func (*activationTestEmployer) AgentForHuman(context.Context, string) (string, error) {
	return "same-pa", nil
}
func (e *activationTestEmployer) AuthorizeCurrentHumanEmployer(_ context.Context, human, pa string, operation func() error) error {
	if e.denied {
		return koseki.ErrNotCurrentEmployer
	}
	e.held = true
	defer func() { e.held = false }()
	return operation()
}

type activationTestManager struct {
	employer      *activationTestEmployer
	idle          bool
	stopErr       error
	stops, starts int
	started       string
	onStart       func()
}

func (m *activationTestManager) StopIfIdle(string) (bool, error) {
	if m.employer != nil && m.employer.held {
		panic("process stop while employer lease held")
	}
	m.stops++
	return m.idle, m.stopErr
}
func (m *activationTestManager) EnsureRunning(_ context.Context, pa string) error {
	m.starts++
	m.started = pa
	if m.onStart != nil {
		m.onStart()
	}
	return nil
}
func TestChatGPTActivationWaitsForIdleAndKeepsSamePA(t *testing.T) {
	employer := &activationTestEmployer{}
	manager := &activationTestManager{employer: employer}
	worker := newChatGPTActivationWorker(employer)
	worker.manager = manager
	complete, err := worker.apply(context.Background(), "human")
	if err != nil || complete || manager.starts != 0 {
		t.Fatal("busy work interrupted")
	}
	manager.idle = true
	complete, err = worker.apply(context.Background(), "human")
	if err != nil || !complete || manager.starts != 1 || manager.started != "same-pa" {
		t.Fatal("idle selection did not restart same PA")
	}
	manager.stopErr = errors.New("stop failed")
	complete, err = worker.apply(context.Background(), "human")
	if err == nil || complete || manager.starts != 1 {
		t.Fatal("failed stop started new process")
	}
	employer.denied = true
	complete, err = worker.apply(context.Background(), "human")
	if err != nil || !complete || manager.stops != 3 {
		t.Fatal("former employer changed runtime")
	}
}
func TestChatGPTActivationCoalescesSelectionChanges(t *testing.T) {
	worker := newChatGPTActivationWorker(&activationTestEmployer{})
	worker.enqueue("human")
	first := worker.pending["human"]
	worker.enqueue("human")
	if len(worker.pending) != 1 || worker.pending["human"] <= first || len(worker.wake) != 1 {
		t.Fatal("selection updates did not coalesce with new generation")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	worker.run(ctx)
}

func TestChatGPTActivationKeepsNewSelectionArrivingDuringRestart(t *testing.T) {
	worker := newChatGPTActivationWorker(&activationTestEmployer{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := &activationTestManager{idle: true}
	worker.manager = manager
	worker.enqueue("human")
	first := worker.pending["human"]
	manager.onStart = func() { worker.enqueue("human"); cancel() }
	worker.run(ctx)
	if worker.pending["human"] <= first {
		t.Fatal("completed old apply erased newer model selection")
	}
}
