package filesvc

// The stable-cut proof under the delayed-boundary adversary: a
// predecessor that is admitted PAST the last pre-syscall check, loses
// its writer lease while parked, and resumes its real mutation after
// the successor requests — or completes — the cut. Neither elapsed
// time nor a process flag bounds that landing; the cut's proof is that
// the public tree is observed identical twice across a real interval
// (the manifest pair), so a landing inside the window is detected and
// the cut stays pending, while a landing after the seal is reported by
// every later observation — never served as the sealed state.

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// cutSeed plants a scope dir with one real file and returns its path.
func cutSeed(t *testing.T, dir, name, content string) string {
	t.Helper()
	if err := os.MkdirAll(dir+"/ws", 0o755); err != nil {
		t.Fatal(err)
	}
	p := dir + "/ws/" + name
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// gatedExchange arms root's exchange seam so the publish syscall parks
// on the gate AFTER every pre-check has passed — the predecessor is
// inside its mutation, only the public syscall remains.
func gatedExchange(root *posixRoot) (entered, release chan struct{}) {
	entered = make(chan struct{})
	release = make(chan struct{})
	root.xchFn = func(oldfd int, old string, newfd int, newName string) error {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return unix.Renameat2(oldfd, old, newfd, newName, unix.RENAME_EXCHANGE)
	}
	return entered, release
}

// manifestEntry returns the cut manifest's entry for path, or nil.
func manifestEntry(m CutManifest, path string) *CutEntry {
	for i := range m.Entries {
		if m.Entries[i].Path == path {
			return &m.Entries[i]
		}
	}
	return nil
}

// A predecessor admitted past the last pre-check loses its lease while
// parked before the publish syscall. The successor's cut judges the
// intent, REAPS its journaled staged source (settleNames moves the
// authored body to a visible recovered-* name — never destroys it) and
// seals. When the predecessor resumes, its real Renameat2 fails ENOENT:
// the staged name it was about to publish no longer exists. This is the
// physical prevention the cut provides for staged mutations — not a
// memory flag, not elapsed time: the publish's own source was moved out
// from under it. The sealed manifest then still equals a fresh
// observation — the tree did not change after seal.
func TestCutSealThenPredecessorLandsLate(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	ctx := context.Background()
	dir := t.TempDir()
	cutSeed(t, dir, "victim.txt", "OLD")

	a := newUnwatchedPGStore(t, dsn, dir) // zombie-capable: no lock watcher
	rootA, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := gatedExchange(rootA)
	wDone := make(chan error, 1)
	go func() {
		_, _, werr := a.WithWrite(ctx, "ws", "victim.txt", "write",
			IfVersion{Mode: "any"}, "", authProbe(rootA, "victim.txt"),
			authWriteFn(rootA, "victim.txt", "NEW"))
		wDone <- werr
	}()
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("predecessor never reached the publish gate")
	}

	// The lease dies mid-mutation; the successor takes ownership and cuts.
	a.releaseWriter()
	b := newPGStore(t, dsn, dir)
	rootB, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	b.SetReconcileView(authPinned(rootB, nil))
	b.SetCutHorizon(0)
	b.SetCutObserveGap(30 * time.Millisecond)
	b.SetDrainTimeout(5 * time.Second)

	if err := b.SetScopeFrozen(ctx, "ws", "sess-b", 2, "return copy", true); err != nil {
		t.Fatalf("successor freeze: %v", err)
	}
	sm, ok, err := b.SealedCutManifest(ctx, "ws")
	if err != nil || !ok {
		t.Fatalf("sealed manifest: ok=%v err=%v", ok, err)
	}
	if manifestEntry(sm, "victim.txt") == nil {
		t.Fatalf("sealed manifest lacks victim.txt: %+v", sm.Entries)
	}

	// The predecessor resumes past the seal — its publish syscall runs
	// for real. The staged name was reaped by the cut's settle, so the
	// exchange has no source: the mutation CANNOT land.
	close(release)
	if werr := <-wDone; werr == nil {
		t.Fatal("resumed predecessor publish succeeded — cut failed to prevent it")
	}
	body, err := os.ReadFile(dir + "/ws/victim.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "OLD" {
		t.Fatalf("post-seal landing changed the copy source: %q", body)
	}
	// The authored bytes were surfaced, not destroyed.
	found := false
	ents, _ := os.ReadDir(dir + "/ws")
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "recovered-") {
			if b2, _ := os.ReadFile(dir + "/ws/" + e.Name()); string(b2) == "NEW" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("reaped authored body was not surfaced as recovered residue")
	}

	// The sealed record is still the truth: a fresh observation equals
	// it exactly — nothing changed the source after seal.
	fm, err := b.CutManifest(ctx, "ws")
	if err != nil {
		t.Fatalf("fresh manifest: %v", err)
	}
	if fm.SHA != sm.SHA {
		t.Fatalf("tree diverged from the sealed manifest after seal: %s != %s",
			fm.SHA, sm.SHA)
	}
}

// A direct-landing foreign effect (no journal — the terminal/peer-writer
// case, or any writer outside the intent protocol) can still land past
// the last pre-check: no staged source exists to reap. The cut's bound
// for those is observational — the manifest pair must disagree while
// the tree is moving and the freeze must stay pending (bounded refusal),
// then seal once the tree is actually still.
func TestCutStaysPendingWhileTreeMoves(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	ctx := context.Background()
	dir := t.TempDir()
	cutSeed(t, dir, "victim.txt", "OLD")

	b := newPGStore(t, dsn, dir)
	rootB, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	b.SetReconcileView(authPinned(rootB, nil))
	b.SetCutHorizon(0)
	b.SetCutObserveGap(300 * time.Millisecond)
	b.SetDrainTimeout(1200 * time.Millisecond)

	// Foreign writes keep landing — invisible to the journal, visible
	// to the manifest pair. Sustained movement past the drain deadline
	// must surface the bounded refusal, never a premature seal.
	stop := make(chan struct{})
	moved := make(chan struct{})
	go func() {
		defer close(moved)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = os.WriteFile(dir+"/ws/foreign.txt", []byte("LATE"+strconv.Itoa(i)), 0o644)
			time.Sleep(120 * time.Millisecond)
		}
	}()
	err = b.SetScopeFrozen(ctx, "ws", "sess-b", 2, "return copy", true)
	close(stop)
	<-moved
	if !errors.Is(err, ErrDrainPending) {
		t.Fatalf("freeze while the tree moved: %v (want ErrDrainPending)", err)
	}

	// Once the tree is actually still, the retry seals and the sealed
	// manifest records the post-landing truth — never a hidden write.
	if err := b.SetScopeFrozen(ctx, "ws", "sess-b", 2, "return copy", true); err != nil {
		t.Fatalf("freeze retry: %v", err)
	}
	sm, ok, err := b.SealedCutManifest(ctx, "ws")
	if err != nil || !ok {
		t.Fatalf("sealed manifest: ok=%v err=%v", ok, err)
	}
	e := manifestEntry(sm, "foreign.txt")
	if e == nil {
		t.Fatalf("sealed manifest lacks the landed foreign write: %+v", sm.Entries)
	}
	live, lerr := rootB.lstat("ws", "foreign.txt")
	if lerr != nil {
		t.Fatal(lerr)
	}
	if e.FP != live.Fingerprint {
		t.Fatalf("manifest fp %s does not match the live object %s", e.FP, live.Fingerprint)
	}
}

// The mover's verify: while the tree is still moving, CutManifest
// refuses with drain-pending — never a racy manifest — and once the
// landing completes it serves the true inventory.
func TestCutManifestRefusesWhileMoving(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	ctx := context.Background()
	dir := t.TempDir()
	cutSeed(t, dir, "victim.txt", "OLD")

	a := newUnwatchedPGStore(t, dsn, dir)
	rootA, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := gatedExchange(rootA)
	wDone := make(chan error, 1)
	go func() {
		_, _, werr := a.WithWrite(ctx, "ws", "victim.txt", "write",
			IfVersion{Mode: "any"}, "", authProbe(rootA, "victim.txt"),
			authWriteFn(rootA, "victim.txt", "NEW"))
		wDone <- werr
	}()
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("predecessor never reached the publish gate")
	}
	a.releaseWriter()
	b := newPGStore(t, dsn, dir)
	rootB, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	b.SetReconcileView(authPinned(rootB, nil))
	b.SetCutObserveGap(400 * time.Millisecond)

	mDone := make(chan error, 1)
	go func() {
		_, merr := b.CutManifest(ctx, "ws")
		mDone <- merr
	}()
	time.Sleep(150 * time.Millisecond)
	close(release)
	if err := <-mDone; !errors.Is(err, ErrDrainPending) {
		t.Fatalf("CutManifest during landing: %v (want ErrDrainPending)", err)
	}
	<-wDone
	m, err := b.CutManifest(ctx, "ws")
	if err != nil {
		t.Fatalf("CutManifest after landing: %v", err)
	}
	if e := manifestEntry(m, "victim.txt"); e == nil {
		t.Fatal("manifest lacks victim.txt")
	}
}

// A sealed scope is inert to every filesvc producer: mutation admission
// refuses, the reconciler's tombstone re-judgment skips it, and the
// orphan sweep cannot surface residue — until a newer lineage releases
// the barrier, after which recovery resumes and surfaces the evidence.
func TestSealedScopeSkipsReconcileAndSweep(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	ctx := context.Background()
	dir := t.TempDir()
	cutSeed(t, dir, "a.txt", "A")

	b := newPGStore(t, dsn, dir)
	rootB, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	b.SetReconcileView(authPinned(rootB, nil))
	b.SetCutHorizon(0)
	if err := b.SetScopeFrozen(ctx, "ws", "sess-b", 2, "return copy", true); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	// Orphan residue appears after the seal: a private name no intent
	// owns. On an unsealed scope the sweep would surface it publicly.
	if err := os.WriteFile(dir+"/ws/"+opStagePrefix+"4242-p-late", []byte("residue"), 0o644); err != nil {
		t.Fatal(err)
	}
	b.Reconcile(ctx)
	if _, err := os.Stat(dir + "/ws/" + opStagePrefix + "4242-p-late"); err != nil {
		t.Fatal("sealed sweep touched the private name")
	}
	ents, _ := os.ReadDir(dir + "/ws")
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "recovered-") {
			t.Fatalf("sealed sweep surfaced residue publicly: %s", e.Name())
		}
	}
	// A newer lineage releases the barrier — recovery resumes.
	if err := b.SetScopeFrozen(ctx, "ws", "sess-c", 3, "", false); err != nil {
		t.Fatalf("newer release: %v", err)
	}
	// The orphan sweep needs two sightings before minting recovery;
	// drive it directly (its Reconcile cadence is 30s).
	view, verr := rootB.pin(false)
	if verr != nil {
		t.Fatal(verr)
	}
	defer view.Close()
	found := false
	for i := 0; i < 6 && !found; i++ {
		b.sweepScopeNames(ctx, "ws", view)
		b.Reconcile(ctx)
		ents, _ = os.ReadDir(dir + "/ws")
		for _, e := range ents {
			if strings.HasPrefix(e.Name(), "recovered-") {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("post-release sweep did not surface the orphaned residue")
	}
}

// Lineage at the mutation boundary: an older epoch cannot assert a
// barrier over a newer one, cannot release a newer one, and cannot
// re-assert after its barrier was released — while a newer lineage
// legitimately releases an older retained barrier (the Local→Cloud
// hand-off).
func TestBarrierLineageOrdering(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	ctx := context.Background()
	st := newPGStore(t, dsn, t.TempDir())
	st.SetCutHorizon(0)

	if err := st.SetScopeFrozen(ctx, "ws", "sess-new", 5, "return copy", true); err != nil {
		t.Fatalf("epoch-5 freeze: %v", err)
	}
	// Stale freeze: refused inside the barrier transaction.
	if err := st.SetScopeFrozen(ctx, "ws", "sess-old", 3, "return copy", true); !errors.Is(err, ErrStaleBarrier) {
		t.Fatalf("stale freeze: %v (want ErrStaleBarrier)", err)
	}
	// Stale unfreeze: a no-op that leaves the newer barrier standing.
	if err := st.SetScopeFrozen(ctx, "ws", "sess-old", 3, "", false); err != nil {
		t.Fatalf("stale unfreeze: %v", err)
	}
	if frozen, _ := st.ScopeFrozen(ctx, "ws"); !frozen {
		t.Fatal("stale unfreeze cleared a newer barrier")
	}
	// Newer release: the legitimate hand-off — clears barrier and seal.
	if err := st.SetScopeFrozen(ctx, "ws", "sess-newer", 7, "", false); err != nil {
		t.Fatalf("newer release: %v", err)
	}
	if frozen, _ := st.ScopeFrozen(ctx, "ws"); frozen {
		t.Fatal("newer release did not clear the barrier")
	}
	// The release tombstone bars re-assertion by the superseded lineage.
	if err := st.SetScopeFrozen(ctx, "ws", "sess-new", 5, "return copy", true); !errors.Is(err, ErrStaleBarrier) {
		t.Fatalf("re-assert after release: %v (want ErrStaleBarrier)", err)
	}
	// An even newer lineage may still assert.
	if err := st.SetScopeFrozen(ctx, "ws", "sess-newest", 8, "return copy", true); err != nil {
		t.Fatalf("newest freeze: %v", err)
	}
}

// The landing fence is a convergence aid, not the proof — a mutation
// parked BEFORE its next landing check is refused when it resumes
// deposed. (A mutation already past the last check — the xchFn gate —
// provably still lands; that window is what the manifest proof covers,
// exercised by the tests above.)
func TestDeposedFenceRefusesResumedLanding(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	ctx := context.Background()
	dir := t.TempDir()
	cutSeed(t, dir, "victim.txt", "OLD")

	a := newUnwatchedPGStore(t, dsn, dir)
	rootA, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Wire the fence to A's own deposed flag — the production wiring.
	rootA.fence = func() error {
		if a.deposed.Load() {
			return ErrUnavailable
		}
		return nil
	}
	// Park the write after its staging file exists but before the
	// publish landing checks — resuming deposed must be refused there.
	entered := make(chan struct{})
	release := make(chan struct{})
	rootA.faultHook = func(tag string) {
		if tag != "write.postCreate" {
			return
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
	}
	wDone := make(chan error, 1)
	go func() {
		_, _, werr := a.WithWrite(ctx, "ws", "victim.txt", "write",
			IfVersion{Mode: "any"}, "", authProbe(rootA, "victim.txt"),
			authWriteFn(rootA, "victim.txt", "NEW"))
		wDone <- werr
	}()
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("predecessor never reached the staging gate")
	}
	a.deposed.Store(true) // noticed deposition while parked mid-composite
	close(release)
	if err := <-wDone; err == nil {
		t.Fatal("fenced predecessor's write succeeded")
	}
	body, _ := os.ReadFile(dir + "/ws/victim.txt")
	if string(body) != "OLD" {
		t.Fatalf("fenced landing published: %q", body)
	}
}
