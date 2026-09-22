package filesvc

// PG-backed coverage for the persisted scope mutation barrier
// (file_freeze): the fence a Cloud→Local file copy puts on the source
// scope. The freeze must refuse new mutation admissions while reads
// continue, must be wildcard-credential only, and must be a durable
// row — an unfreeze or a fresh store handle converges on the same
// answer.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func nilProbe() (FileInfo, bool, error) { return FileInfo{}, false, nil }

func okWriteFn(it intent) (FileInfo, bool, error) {
	return FileInfo{Kind: "file", Size: 1}, true, nil
}

func pgService(t *testing.T) (*Service, *Store) {
	t.Helper()
	dsn := pgDSN(t)
	resetTables(t, dsn)
	st := newPGStore(t, dsn, t.TempDir())
	st.SetCutHorizon(0) // tests run the cut immediately; tenure is exercised separately
	svc, err := NewAt(t.TempDir(), st, map[string]map[string]bool{
		"tok-scope": {"ws1": true},
		"tok-all":   {"*": true},
	})
	if err != nil {
		t.Fatal(err)
	}
	return svc, st
}

// The barrier is a persisted row, not a process flag: a store handle
// opened after the freeze commits sees the same answer. (The store
// single-writer-locks its database, so the second handle opens only
// after the first closes.)
func TestScopeFrozenPersistsAcrossHandles(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	ctx := context.Background()

	// One filesystem root per database binding — the store refuses a
	// second handle that names a different root.
	root := t.TempDir()
	st1, err := NewStore(ctx, dsn, root)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	st1.SetCutHorizon(0)
	// The writer lock must release even on failure — a leaked handle
	// blocks every later store open on this database for the handoff
	// timeout.
	defer st1.Close()
	frozen, err := st1.ScopeFrozen(ctx, "ws1")
	if err != nil || frozen {
		t.Fatalf("fresh scope frozen=%v err=%v", frozen, err)
	}
	if err := st1.SetScopeFrozen(ctx, "ws1", "sess-a", 1, "return copy", true); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	st1.Close()

	st2, err := NewStore(ctx, dsn, root)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st2.Close()
	frozen, err = st2.ScopeFrozen(ctx, "ws1")
	if err != nil || !frozen {
		t.Fatalf("reopened handle frozen=%v err=%v (want true)", frozen, err)
	}
	if err := st2.SetScopeFrozen(ctx, "ws1", "sess-a", 1, "", false); err != nil {
		t.Fatalf("unfreeze: %v", err)
	}
	frozen, err = st2.ScopeFrozen(ctx, "ws1")
	if err != nil || frozen {
		t.Fatalf("after unfreeze frozen=%v err=%v", frozen, err)
	}
}

// End-to-end through the service: freeze refuses new writes with the
// typed 409 while reads keep working; unfreeze restores mutations.
// Only the wildcard credential may fence — a scope token cannot fence
// its own scope (or anyone's).
func TestFreezeRefusesMutationsReadsStayOpen(t *testing.T) {
	svc, _ := pgService(t)

	w := req(t, svc, "PUT", "/v1/files/ws1/write?path=a.txt", "tok-scope", "v1",
		map[string]string{"If-Version": "none"})
	if w.Code != 200 {
		t.Fatalf("initial write: %d %s", w.Code, w.Body)
	}

	// Scope-scoped credential cannot fence — the barrier is
	// administrative, not caller self-service.
	w = req(t, svc, "POST", "/v1/files/ws1/freeze?reason=test", "tok-scope", "", nil)
	if w.Code != 403 {
		t.Fatalf("freeze with scope token: %d (want 403)", w.Code)
	}
	w = req(t, svc, "POST", "/v1/files/ws1/freeze?reason=test", "", "", nil)
	if w.Code != 403 && w.Code != 401 {
		t.Fatalf("freeze unauthenticated: %d (want 401/403)", w.Code)
	}
	w = req(t, svc, "POST", "/v1/files/ws1/freeze?reason=return%20copy", "tok-all", "", nil)
	if w.Code != 200 {
		t.Fatalf("freeze with wildcard: %d %s", w.Code, w.Body)
	}

	w = req(t, svc, "PUT", "/v1/files/ws1/write?path=b.txt", "tok-scope", "v2",
		map[string]string{"If-Version": "none"})
	if w.Code != 409 || !strings.Contains(w.Body.String(), "scope_frozen") {
		t.Fatalf("write while frozen: %d %s (want 409 scope_frozen)", w.Code, w.Body)
	}
	w = req(t, svc, "POST", "/v1/files/ws1/mkdir", "tok-scope", `{"path":"d"}`,
		map[string]string{"Content-Type": "application/json"})
	if w.Code != 409 {
		t.Fatalf("mkdir while frozen: %d (want 409)", w.Code)
	}
	w = req(t, svc, "DELETE", "/v1/files/ws1/remove?path=a.txt", "tok-scope", "",
		map[string]string{"If-Version": "any"})
	if w.Code != 409 {
		t.Fatalf("remove while frozen: %d (want 409)", w.Code)
	}
	// Reads are unaffected — the copy itself runs under read authority.
	w = req(t, svc, "GET", "/v1/files/ws1/read?path=a.txt", "tok-scope", "", nil)
	if w.Code != 200 || w.Body.String() != "v1" {
		t.Fatalf("read while frozen: %d %q", w.Code, w.Body)
	}
	w = req(t, svc, "GET", "/v1/files/ws1/list", "tok-scope", "", nil)
	if w.Code != 200 {
		t.Fatalf("list while frozen: %d", w.Code)
	}

	w = req(t, svc, "POST", "/v1/files/ws1/unfreeze", "tok-all", "", nil)
	if w.Code != 200 {
		t.Fatalf("unfreeze: %d %s", w.Code, w.Body)
	}
	w = req(t, svc, "PUT", "/v1/files/ws1/write?path=b.txt", "tok-scope", "v2",
		map[string]string{"If-Version": "none"})
	if w.Code != 200 {
		t.Fatalf("write after unfreeze: %d %s", w.Code, w.Body)
	}
}

// An unfreeze is lineage-scoped: a different owner's unfreeze leaves the
// barrier standing, so a stale or unrelated return can never drop the
// fence a live copy depends on.
func TestUnfreezeClearsOnlyOwningLineage(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	ctx := context.Background()
	st := newPGStore(t, dsn, t.TempDir())
	st.SetCutHorizon(0)
	if err := st.SetScopeFrozen(ctx, "ws1", "sess-live", 2, "return copy", true); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	// A STALE lineage's unfreeze does not touch it — epoch 1 < the
	// barrier's 2, so the release is a no-op, not an error.
	if err := st.SetScopeFrozen(ctx, "ws1", "sess-stale", 1, "", false); err != nil {
		t.Fatalf("foreign unfreeze: %v", err)
	}
	if frozen, err := st.ScopeFrozen(ctx, "ws1"); err != nil || !frozen {
		t.Fatalf("foreign unfreeze cleared the barrier: frozen=%v err=%v", frozen, err)
	}
	// The owning lineage's unfreeze clears exactly it.
	if err := st.SetScopeFrozen(ctx, "ws1", "sess-live", 2, "", false); err != nil {
		t.Fatalf("owner unfreeze: %v", err)
	}
	if frozen, err := st.ScopeFrozen(ctx, "ws1"); err != nil || frozen {
		t.Fatalf("owner unfreeze did not clear: frozen=%v err=%v", frozen, err)
	}
}

// stat exposes the copied-content digest the mover verifies against —
// the copy's per-file integrity check depends on it answering.
func TestStatExposesContentSHA(t *testing.T) {
	svc, _ := pgService(t)
	w := req(t, svc, "PUT", "/v1/files/ws1/write?path=h.txt", "tok-scope", "hashme",
		map[string]string{"If-Version": "none"})
	if w.Code != 200 {
		t.Fatalf("write: %d %s", w.Code, w.Body)
	}
	w = req(t, svc, "GET", "/v1/files/ws1/stat?path=h.txt", "tok-scope", "", nil)
	if w.Code != 200 {
		t.Fatalf("stat: %d %s", w.Code, w.Body)
	}
	body := w.Body.String()
	if !strings.Contains(body, `"content_sha"`) {
		t.Fatalf("stat carries no content_sha: %s", body)
	}
	// sha256("hashme") — the digest the copier recomputes over the bytes.
	if !strings.Contains(body, "02208b9403a87df9f4ed6b2ee2657efaa589026b4cce9accc8e8a5bf3d693c86") {
		t.Fatalf("content_sha mismatch: %s", body)
	}
}

// The freeze is a copy boundary, not just an admission fence: a write
// whose intent committed BEFORE the barrier but whose filesystem effect
// is still in flight must keep the freeze from succeeding — a "frozen"
// scope that can still change is no stable snapshot. The barrier
// commits (new admissions refuse), the drain reports the still-settling
// effect, and only once it lands does the freeze succeed and the copied
// inventory provably include it.
func TestFreezeDrainsAdmittedEffects(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	ctx := context.Background()
	root := t.TempDir()
	st, err := NewStore(ctx, dsn, root)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer st.Close()
	st.SetDrainTimeout(400 * time.Millisecond)
	st.SetCutHorizon(0)

	// Admitted intent, fs effect paused on a gate — the regression
	// window: declare committed, runFs has not landed the bytes.
	release := make(chan struct{})
	inflight := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, _, err := st.WithWrite(ctx, "ws1", "slow.txt", "write",
			IfVersion{Mode: "none"}, "sha-of-bytes", nilProbe,
			func(it intent) (FileInfo, bool, error) {
				close(inflight)
				<-release
				return FileInfo{Kind: "file", Size: 5}, true, nil
			})
		done <- err
	}()
	select {
	case <-inflight:
	case <-time.After(10 * time.Second):
		t.Fatal("fs effect never reached its gate")
	}

	// The barrier commits but the freeze must NOT report success while
	// the admitted effect can still land.
	err = st.SetScopeFrozen(ctx, "ws1", "sess-a", 1, "return copy", true)
	if !errors.Is(err, ErrDrainPending) {
		t.Fatalf("freeze while effect in flight: %v (want ErrDrainPending)", err)
	}
	// …and the barrier is real: the row is committed even though the
	// freeze call itself reported pending. (A competing WithWrite here
	// would queue behind the held one's scope mutex — the row check is
	// the discriminating probe.)
	if frozen, ferr := st.ScopeFrozen(ctx, "ws1"); ferr != nil || !frozen {
		t.Fatalf("barrier after drain-pending: frozen=%v err=%v", frozen, ferr)
	}

	// The admitted effect lands and settles; the freeze then completes —
	// the file is inside the stable inventory, not raced past the cut.
	close(release)
	if werr := <-done; werr != nil {
		t.Fatalf("held write: %v", werr)
	}
	if err := st.SetScopeFrozen(ctx, "ws1", "sess-a", 1, "return copy", true); err != nil {
		t.Fatalf("freeze after drain: %v", err)
	}
	// …and new admissions still refuse until the owner unfreezes.
	if _, _, werr := st.WithWrite(ctx, "ws1", "new.txt", "write",
		IfVersion{Mode: "none"}, "sha", nilProbe, okWriteFn); !errors.Is(werr, ErrFrozen) {
		t.Fatalf("write after freeze: %v (want ErrFrozen)", werr)
	}
}
