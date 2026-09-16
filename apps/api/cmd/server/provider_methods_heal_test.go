package main

// F312 regression coverage: the ProviderMethods stale-credential heal must
// never retire a binding that committed after the remote snapshot the heal
// decision was based on. The schedules below are deterministic: a barrier
// holds the first remote read while a concurrent relink lands.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

// heldSnapshotProviderLifecycle serves each ProviderAccount call from a
// snapshot captured at call entry. The first call is additionally held open
// until the test releases it, modelling a remote read that began before a
// concurrent relink committed and whose response therefore predates it.
type heldSnapshotProviderLifecycle struct {
	mu       sync.Mutex
	accounts map[string]firebaseProviderAccount
	calls    int
	entered  chan struct{}
	release  chan struct{}
}

func (f *heldSnapshotProviderLifecycle) ProviderAccount(_ context.Context, uid string) (firebaseProviderAccount, error) {
	f.mu.Lock()
	f.calls++
	call := f.calls
	account, ok := f.accounts[uid]
	snapshot := account
	snapshot.ProviderSubjects = make(map[string]string, len(account.ProviderSubjects))
	for provider, subject := range account.ProviderSubjects {
		snapshot.ProviderSubjects[provider] = subject
	}
	f.mu.Unlock()
	if call == 1 && f.entered != nil {
		f.entered <- struct{}{}
		<-f.release
	}
	if !ok {
		return firebaseProviderAccount{}, errors.New("missing Firebase account")
	}
	return snapshot, nil
}

func (f *heldSnapshotProviderLifecycle) setSubjects(uid string, subjects map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	account := f.accounts[uid]
	account.ProviderSubjects = subjects
	f.accounts[uid] = account
}

func (f *heldSnapshotProviderLifecycle) DeleteProvider(_ context.Context, uid, provider string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	account := f.accounts[uid]
	delete(account.ProviderSubjects, provider)
	f.accounts[uid] = account
	return nil
}

// The F312 schedule: the remote read that feeds the heal raced a same-subject
// relink. The credential row must survive — locally AND on the next read.
func TestProviderMethodsHealDoesNotRetireConcurrentRelink(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	const uid = "methods-heal-race-uid"
	registered, err := store.AutoRegister(ctx, "firebase", uid)
	if err != nil {
		t.Fatal(err)
	}
	// The credential row is active throughout: only the remote binding was
	// deleted and relinked (e.g. a zombie delete followed by a relink).
	if err := store.BindCredential(ctx, "github.com", "github-live", registered.HumanID); err != nil {
		t.Fatal(err)
	}
	providers := &heldSnapshotProviderLifecycle{
		accounts: map[string]firebaseProviderAccount{uid: {
			UID: uid, ProviderSubjects: map[string]string{},
		}},
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	controller := newKosekiAuthFlowController(store, "local", providers)
	claims := agentevents.UserSessionClaims{UserID: registered.HumanID, PersonalityAgentID: registered.AgentID, TenantID: "local"}

	type readResult struct {
		methods agentevents.ProviderMethodsResult
		err     error
	}
	readDone := make(chan readResult, 1)
	go func() {
		methods, err := controller.ProviderMethods(ctx, claims)
		readDone <- readResult{methods, err}
	}()

	// The first remote read captured an account without github. While it is
	// still in flight the same-subject relink lands remotely.
	<-providers.entered
	providers.setSubjects(uid, map[string]string{"github.com": "github-live"})
	close(providers.release)

	result := <-readDone
	if result.err != nil {
		t.Fatalf("ProviderMethods: %v", result.err)
	}
	// One response may still reflect the stale snapshot.
	if len(result.methods.Providers) != 0 {
		t.Fatalf("expected stale snapshot to omit github, got %+v", result.methods.Providers)
	}
	// The heal's locked revalidation saw the relink: the binding survives.
	subject, err := store.ActiveProviderSubject(ctx, registered.HumanID, "github.com")
	if err != nil {
		t.Fatalf("freshly relinked credential was retired: %v", err)
	}
	if subject != "github-live" {
		t.Fatalf("unexpected credential subject %q", subject)
	}

	// The next read lists the method — no sticky wrongness.
	second, err := controller.ProviderMethods(ctx, claims)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Providers) != 1 || second.Providers[0] != "github.com" {
		t.Fatalf("second read should list github: %+v", second)
	}
}

// A binding committed while the first remote read was in flight is not a heal
// candidate at all: the local credential read happens before the remote read.
func TestProviderMethodsLocalReadPrecedesRemoteRead(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	const uid = "methods-order-uid"
	registered, err := store.AutoRegister(ctx, "firebase", uid)
	if err != nil {
		t.Fatal(err)
	}
	providers := &heldSnapshotProviderLifecycle{
		accounts: map[string]firebaseProviderAccount{uid: {
			UID: uid, ProviderSubjects: map[string]string{},
		}},
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	controller := newKosekiAuthFlowController(store, "local", providers)
	claims := agentevents.UserSessionClaims{UserID: registered.HumanID, PersonalityAgentID: registered.AgentID, TenantID: "local"}

	readDone := make(chan error, 1)
	go func() {
		_, err := controller.ProviderMethods(ctx, claims)
		readDone <- err
	}()

	// A link commits both halves while the remote read is held: the remote
	// mutation lands first, then the local row activates — the same order
	// CompleteProviderLink observes.
	<-providers.entered
	providers.setSubjects(uid, map[string]string{"github.com": "github-new"})
	if err := store.BindCredential(ctx, "github.com", "github-new", registered.HumanID); err != nil {
		t.Fatal(err)
	}
	close(providers.release)

	if err := <-readDone; err != nil {
		t.Fatalf("ProviderMethods: %v", err)
	}
	subject, err := store.ActiveProviderSubject(ctx, registered.HumanID, "github.com")
	if err != nil {
		t.Fatalf("credential bound mid-read was retired: %v", err)
	}
	if subject != "github-new" {
		t.Fatalf("unexpected credential subject %q", subject)
	}
}

// The heal still retires a binding whose remote identity is genuinely gone:
// the revalidation confirms the absence instead of trusting the first read.
// A same-subject link replay afterwards reactivates the row — recovery works.
func TestProviderMethodsHealStillRetiresConfirmedAbsence(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	const uid = "methods-heal-gone-uid"
	registered, err := store.AutoRegister(ctx, "firebase", uid)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BindCredential(ctx, "github.com", "github-gone", registered.HumanID); err != nil {
		t.Fatal(err)
	}
	providers := &heldSnapshotProviderLifecycle{
		accounts: map[string]firebaseProviderAccount{uid: {
			UID: uid, ProviderSubjects: map[string]string{},
		}},
	}
	controller := newKosekiAuthFlowController(store, "local", providers)
	claims := agentevents.UserSessionClaims{UserID: registered.HumanID, PersonalityAgentID: registered.AgentID, TenantID: "local"}

	methods, err := controller.ProviderMethods(ctx, claims)
	if err != nil {
		t.Fatal(err)
	}
	if len(methods.Providers) != 0 {
		t.Fatalf("expected empty provider list, got %+v", methods.Providers)
	}
	if _, err := store.ActiveProviderSubject(ctx, registered.HumanID, "github.com"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("stale credential should be disabled, err=%v", err)
	}
	if providers.calls != 2 {
		t.Fatalf("expected the heal to re-read the remote once, got %d reads", providers.calls)
	}

	// Recovery: the same subject is linked again — remote mutation first —
	// and the link replay reactivates the disabled row.
	providers.setSubjects(uid, map[string]string{"github.com": "github-gone"})
	linkNonce := controllerNonce(t)
	link, err := store.BeginProviderOperation(ctx, registered.HumanID, uid, "github.com", "link", "account_settings", linkNonce)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteProviderLink(ctx, link.OperationID, linkNonce, uid, "github-gone"); err != nil {
		t.Fatalf("link replay: %v", err)
	}
	third, err := controller.ProviderMethods(ctx, claims)
	if err != nil {
		t.Fatal(err)
	}
	if len(third.Providers) != 1 || third.Providers[0] != "github.com" {
		t.Fatalf("recovered read should list github: %+v", third)
	}
}

// A different remote subject now occupying the provider slot means the
// recorded binding is dead remotely: the revalidation still retires it and
// leaves the replacement identity alone.
func TestProviderMethodsHealRetiresSubjectMismatch(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	const uid = "methods-heal-drift-uid"
	registered, err := store.AutoRegister(ctx, "firebase", uid)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BindCredential(ctx, "github.com", "github-old", registered.HumanID); err != nil {
		t.Fatal(err)
	}
	providers := &heldSnapshotProviderLifecycle{
		accounts: map[string]firebaseProviderAccount{uid: {
			UID: uid, ProviderSubjects: map[string]string{"github.com": "github-other"},
		}},
	}
	controller := newKosekiAuthFlowController(store, "local", providers)
	claims := agentevents.UserSessionClaims{UserID: registered.HumanID, PersonalityAgentID: registered.AgentID, TenantID: "local"}

	methods, err := controller.ProviderMethods(ctx, claims)
	if err != nil {
		t.Fatal(err)
	}
	if len(methods.Providers) != 0 {
		t.Fatalf("subject-mismatched credential must not be listed: %+v", methods.Providers)
	}
	if _, err := store.ActiveProviderSubject(ctx, registered.HumanID, "github.com"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("mismatched credential should be disabled, err=%v", err)
	}
}
