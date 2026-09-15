package messaging

// Operation admission for the Messaging surface: a coarse, in-memory bound on
// new durable mutations and live WebSocket connections. It deliberately lives
// in the API process — per the messaging boundary contract the authoritative
// per-operation decision belongs here, not at the edge — and it is
// single-process: buckets and leases reset on restart and are not shared
// across replicas. That constraint is recorded in docs/messaging-boundary-contract.md.
//
// Ordering contract: admission is consulted only after the durable nonce
// replay check, so a retried send always reaches its committed receipt even
// while the new-operation bucket is empty. Only a genuinely new durable
// mutation consumes a token.

import (
	"fmt"
	"sync"
	"time"
)

// RateLimitedError reports a refused new operation. RetryAfter is the
// transport-facing retry hint; the operation wrote nothing.
type RateLimitedError struct {
	RetryAfter time.Duration
}

func (e *RateLimitedError) Error() string {
	return fmt.Sprintf("operation rate limited; retry after %s", e.RetryAfter.Round(time.Millisecond))
}

// Dogfood starting thresholds — operational values tuned by observation, not
// a policy ceiling. See the boundary contract for the aggregate keys.
const (
	// New durable message mutations per (Workspace, Installation, actor):
	// 240/min sustained, burst 64 — comfortably above any single scripted
	// burst (search-cap fixtures send ~50), still a hard bound on floods.
	admissionMutationRatePerSecond = 4.0
	admissionMutationBurst         = 64.0
	// Live Messaging WebSockets per (Workspace, Installation, actor) and per
	// actor across scopes.
	admissionWSPerScope = 6
	admissionWSPerActor = 18
	// Buckets idle this long are evicted; the limiter stays bounded.
	admissionBucketIdleTTL = 10 * time.Minute
)

// wsAdmissionRetryHint is the fixed retry hint for a refused connection
// lease: unlike the mutation bucket there is no drain rate to compute from.
const wsAdmissionRetryHint = time.Second

type admissionKey struct {
	workspaceID    string
	installationID string
	actorKey       string
}

func keyForScope(scope Scope) admissionKey {
	return admissionKey{
		workspaceID:    scope.WorkspaceID,
		installationID: scope.InstallationID,
		actorKey:       scope.Actor.Key(),
	}
}

type mutationBucket struct {
	tokens  float64
	touched time.Time
}

// operationAdmission holds all mutable admission state for one API process.
type operationAdmission struct {
	mu      sync.Mutex
	now     func() time.Time
	rate    float64
	burst   float64
	buckets map[admissionKey]*mutationBucket

	wsScopeCap int
	wsActorCap int
	wsByScope  map[admissionKey]int
	wsByActor  map[string]int
}

func newOperationAdmission() *operationAdmission {
	return &operationAdmission{
		now:        time.Now,
		rate:       admissionMutationRatePerSecond,
		burst:      admissionMutationBurst,
		buckets:    map[admissionKey]*mutationBucket{},
		wsScopeCap: admissionWSPerScope,
		wsActorCap: admissionWSPerActor,
		wsByScope:  map[admissionKey]int{},
		wsByActor:  map[string]int{},
	}
}

// admitMutation consumes one new-mutation token for the scope, or returns a
// *RateLimitedError whose RetryAfter is the wait for the next token.
func (a *operationAdmission) admitMutation(scope Scope) error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	key := keyForScope(scope)
	b := a.buckets[key]
	if b == nil {
		b = &mutationBucket{tokens: a.burst, touched: now}
		a.buckets[key] = b
	}
	if elapsed := now.Sub(b.touched); elapsed > 0 {
		b.tokens += elapsed.Seconds() * a.rate
		if b.tokens > a.burst {
			b.tokens = a.burst
		}
	}
	b.touched = now
	a.evictIdleLocked(now)
	if b.tokens < 1 {
		wait := time.Duration((1-b.tokens)/a.rate*float64(time.Second)) + time.Millisecond
		return &RateLimitedError{RetryAfter: wait}
	}
	b.tokens--
	return nil
}

// evictIdleLocked drops buckets untouched for the idle TTL. Called under mu;
// O(bucket count) on each admission, bounded by distinct active scopes.
func (a *operationAdmission) evictIdleLocked(now time.Time) {
	for key, b := range a.buckets {
		if now.Sub(b.touched) > admissionBucketIdleTTL {
			delete(a.buckets, key)
		}
	}
}

// wsConnLease is one live Messaging WebSocket slot. Release is idempotent.
type wsConnLease struct {
	admission *operationAdmission
	key       admissionKey
	actorKey  string
	once      sync.Once
}

// acquireWSLease reserves a live-connection slot for the scope before the
// socket upgrade, or returns a *RateLimitedError when the scope or actor cap
// is full. Nothing is held when it errors.
func (a *operationAdmission) acquireWSLease(scope Scope) (*wsConnLease, error) {
	if a == nil {
		return &wsConnLease{}, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	key := keyForScope(scope)
	if a.wsByScope[key] >= a.wsScopeCap || a.wsByActor[key.actorKey] >= a.wsActorCap {
		return nil, &RateLimitedError{RetryAfter: wsAdmissionRetryHint}
	}
	a.wsByScope[key]++
	a.wsByActor[key.actorKey]++
	return &wsConnLease{admission: a, key: key, actorKey: key.actorKey}, nil
}

// release returns the slot exactly once; every failure and close path may
// call it.
func (l *wsConnLease) release() {
	if l == nil || l.admission == nil {
		return
	}
	l.once.Do(func() {
		l.admission.mu.Lock()
		defer l.admission.mu.Unlock()
		l.admission.wsByScope[l.key]--
		l.admission.wsByActor[l.actorKey]--
		if l.admission.wsByScope[l.key] <= 0 {
			delete(l.admission.wsByScope, l.key)
		}
		if l.admission.wsByActor[l.actorKey] <= 0 {
			delete(l.admission.wsByActor, l.actorKey)
		}
	})
}
