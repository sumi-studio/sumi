// Package directchat owns the single-process lifecycle fence shared by the
// Direct Chat transport, app lifecycle, and Employer ledger.
package directchat

import (
	"context"
	"errors"
	"sync"

	"golang.org/x/sync/semaphore"
)

const (
	AppID                  = "direct-chat"
	lifecycleFenceCapacity = int64(1 << 30)
)

var ErrLifecycleFenceUnavailable = errors.New("direct-chat lifecycle fence unavailable")

// LifecycleFence orders effect-capable Direct Chat operations against app and
// Employer lifecycle commits in the current API process. Operations share the
// read side; lifecycle mutations take the write side.
//
// This is intentionally a single-host dogfood boundary. It survives loss of a
// PostgreSQL backend while the effect-capable API process remains alive, and a
// process crash stops that process's effects. Multi-replica deployment requires
// a durable or distributed replacement before enabling more than one API.
type LifecycleFence struct {
	permits *semaphore.Weighted
}

func NewLifecycleFence() *LifecycleFence {
	return &LifecycleFence{permits: semaphore.NewWeighted(lifecycleFenceCapacity)}
}

func (f *LifecycleFence) AcquireOperation(ctx context.Context) (func(), error) {
	return f.acquire(ctx, 1)
}

func (f *LifecycleFence) AcquireMutation(ctx context.Context) (func(), error) {
	return f.acquire(ctx, lifecycleFenceCapacity)
}

func (f *LifecycleFence) acquire(ctx context.Context, permits int64) (func(), error) {
	if f == nil || f.permits == nil {
		return nil, ErrLifecycleFenceUnavailable
	}
	if err := f.permits.Acquire(ctx, permits); err != nil {
		return nil, err
	}
	var once sync.Once
	return func() {
		once.Do(func() { f.permits.Release(permits) })
	}, nil
}
