package directchat

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLifecycleFenceOrdersOperationsAndMutations(t *testing.T) {
	fence := NewLifecycleFence()
	releaseOperation, err := fence.AcquireOperation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	mutationEntered := make(chan struct{})
	mutationDone := make(chan struct{})
	go func() {
		releaseMutation, acquireErr := fence.AcquireMutation(context.Background())
		if acquireErr != nil {
			return
		}
		close(mutationEntered)
		releaseMutation()
		close(mutationDone)
	}()
	select {
	case <-mutationEntered:
		t.Fatal("mutation overtook active operation")
	case <-time.After(25 * time.Millisecond):
	}
	releaseOperation()
	select {
	case <-mutationDone:
	case <-time.After(time.Second):
		t.Fatal("mutation did not proceed after operation")
	}
}

func TestLifecycleFenceWaitIsCancellableAndNilFailsClosed(t *testing.T) {
	fence := NewLifecycleFence()
	releaseMutation, err := fence.AcquireMutation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := fence.AcquireOperation(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled acquire error = %v", err)
	}
	releaseMutation()

	var missing *LifecycleFence
	if _, err := missing.AcquireOperation(context.Background()); !errors.Is(err, ErrLifecycleFenceUnavailable) {
		t.Fatalf("nil fence error = %v", err)
	}
}
