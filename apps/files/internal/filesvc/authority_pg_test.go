package filesvc

// Discard authority against writers that can still record an object
// (findings 174, 176, 177). Real Store, real PG, real ext4 syscalls
// through the production reconcile view. Test substitutions are named
// where they occur: gates at the named schedule positions, a held row
// lock standing in for a slow apply commit, aged timestamps standing in
// for elapsed time, and — only where stated — composed intent rows or a
// composed delayed move.

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func authProbe(root *posixRoot, path string) FPProbe {
	return func() (FileInfo, bool, error) {
		info, err := root.stat("ws", path)
		if errors.Is(err, ErrNotFound) || errors.Is(err, ErrNotDir) {
			return FileInfo{}, false, nil
		}
		return info, err == nil, err
	}
}

func authWriteFn(root *posixRoot, path, content string) func(intent) (FileInfo, bool, error) {
	return func(it intent) (FileInfo, bool, error) {
		return root.atomicWrite("ws", path, []byte(content), false, it)
	}
}

type authGate struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func newAuthGate() *authGate {
	return &authGate{entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *authGate) hit() { g.once.Do(func() { close(g.entered); <-g.release }) }

func (g *authGate) await(t *testing.T, what string) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(30 * time.Second):
		t.Fatalf("gate never reached: %s", what)
	}
}

// authView interposes on the pass-pinned production view; every call is
// delegated to the real rootView after the optional gate.
type authView struct {
	ReconView
	onHash func(scope, path string)
	onMove func(scope, from, to string)
}

func (v authView) Hash(scope, path string) (string, error) {
	if v.onHash != nil {
		v.onHash(scope, path)
	}
	return v.ReconView.Hash(scope, path)
}

func (v authView) MoveStaged(scope, from, to string) error {
	if v.onMove != nil {
		v.onMove(scope, from, to)
	}
	return v.ReconView.MoveStaged(scope, from, to)
}

func authPinned(root *posixRoot, wrap func(ReconView) ReconView) func(context.Context) (ReconView, error) {
	return func(context.Context) (ReconView, error) {
		v, err := root.pin(false)
		if err != nil {
			return nil, err
		}
		if wrap == nil {
			return v, nil
		}
		return wrap(v), nil
	}
}

// newUnwatchedPGStore is NewStore without the lock watcher. It models the
// window in which an instance has lost its advisory-lock session but its
// watcher has not noticed yet (up to one ping interval plus timeout), so a
// reconcile pass that already entered keeps running. Lock acquisition,
// migration and owner binding are the production methods.
func newUnwatchedPGStore(t *testing.T, dsn, rootID string) *Store {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	s := &Store{pool: pool, dsn: dsn, opTimeout: 30 * time.Second, dbTimeout: 15 * time.Second,
		owner: "inst-unwatched-" + randHex(4), rootID: rootID,
		done: make(chan struct{}), reconcile: make(chan struct{}, 1)}
	if err := s.acquireWriter(ctx); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	if err := s.migrate(ctx); err != nil {
		s.Close()
		t.Fatal(err)
	}
	if err := s.bindRoot(ctx); err != nil {
		s.Close()
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func authMint(t *testing.T, s *Store) int64 {
	t.Helper()
	var v int64
	if err := s.pool.QueryRow(context.Background(),
		`SELECT nextval('file_version_seq')`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func authRow(t *testing.T, s *Store, path string) (int64, string, bool) {
	t.Helper()
	var v int64
	var fp string
	err := s.pool.QueryRow(context.Background(),
		`SELECT version, fp FROM file_version WHERE scope='ws' AND path=$1`, path).Scan(&v, &fp)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, "", false
	}
	if err != nil {
		t.Fatal(err)
	}
	return v, fp, true
}

func authExec(t *testing.T, s *Store, sql string, args ...any) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func authReadOpt(dir, rel string) (string, bool) {
	b, err := os.ReadFile(dir + "/" + rel)
	if err != nil {
		return "", false
	}
	return string(b), true
}

// authHold opens a tx on its own connection. authLockIntent then holds an
// intent row FOR UPDATE inside it, so that intent's apply blocks on its
// claim (DELETE … RETURNING) inside a real apply tx — a deterministic
// stand-in for a slow or contended PG commit.
func authHold(t *testing.T, dsn string) pgx.Tx {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		conn.Close(ctx)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = tx.Rollback(ctx)
		conn.Close(ctx)
	})
	return tx
}

type authObserved struct {
	info FileInfo
	err  error
}

// authHeldWrite runs a real WithWrite whose fs effect publishes and
// observes its object (the post-publish stat the apply will record);
// its apply is then held by tx. Returns the observed object and the
// request's completion channel.
func authHeldWrite(t *testing.T, s *Store, root *posixRoot, tx pgx.Tx, path, content string) (FileInfo, <-chan error) {
	t.Helper()
	observed := make(chan authObserved, 1)
	done := make(chan error, 1)
	go func() {
		_, _, err := s.WithWrite(context.Background(), "ws", path, "write",
			IfVersion{Mode: "any"}, sha(content), authProbe(root, path),
			func(it intent) (FileInfo, bool, error) {
				info, committed, ferr := authWriteFn(root, path, content)(it)
				if ferr == nil {
					_, ferr2 := tx.Exec(context.Background(),
						`SELECT id FROM file_op WHERE id=$1 FOR UPDATE`, it.id)
					observed <- authObserved{info, ferr2}
				} else {
					observed <- authObserved{info, ferr}
				}
				return info, committed, ferr
			})
		done <- err
	}()
	select {
	case o := <-observed:
		if o.err != nil {
			t.Fatalf("held write %s: %v", path, o.err)
		}
		return o.info, done
	case <-time.After(30 * time.Second):
		t.Fatalf("held write %s never published", path)
	}
	return FileInfo{}, done
}

func authWait(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func authSettlersDone(s *Store) bool {
	empty := true
	s.inflight.Range(func(_, _ any) bool { empty = false; return false })
	return empty
}

// authSettle runs ordinary passes after interference has stopped, with the
// tombstone scan and orphan-sweep clocks reset to stand in for their
// intervals elapsing.
func authSettle(t *testing.T, s *Store) {
	t.Helper()
	for i := 0; i < 3; i++ {
		s.lastTombScan.Store(0)
		s.lastStageSweep.Store(0)
		s.Reconcile(context.Background())
	}
}

// authAssertRecovered: the request's observed object holds its public
// name with its bytes, and the version row records exactly that object.
func authAssertRecovered(t *testing.T, s *Store, root *posixRoot, dir, path, content string, observed FileInfo) {
	t.Helper()
	got, ok := authReadOpt(dir, "ws/"+path)
	if !ok || got != content {
		t.Fatalf("%s = %q (present=%v); %q bytes found at %q (empty = destroyed)",
			path, got, ok, content, scanDirFor(t, dir, "ws", []byte(content)))
	}
	live := durFP(t, root, "ws", path)
	if fp3(live) != fp3(observed.Fingerprint) {
		t.Fatalf("%s holds %s, not the request's observed object %s", path, live, observed.Fingerprint)
	}
	if _, fp, found := authRow(t, s, path); !found || fp3(fp) != fp3(live) {
		t.Fatalf("row for %s records %q (found=%v), live object %s", path, fp, found, live)
	}
}

// authUndoParkedTombstone builds 174's precondition through production
// paths only: acknowledged E0 at a.txt, then a write of "H" whose
// verified exchange displaces a racer's D (not the declared E0) and whose
// undo finds a second racer R on the name. errUndoParked leaves D at the
// intent's slot and R at a.txt, and runFs tombstones the intent. The
// racers are executor-side renames injected at the real fault points.
// The tombstone is then aged past the dead-owner grace (still hot).
func authUndoParkedTombstone(t *testing.T, s *Store, root *posixRoot, dir string) (intent, string) {
	t.Helper()
	ctx := context.Background()
	if err := os.MkdirAll(dir+"/ws", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.WithWrite(ctx, "ws", "a.txt", "write", IfVersion{Mode: "any"},
		sha("E0"), authProbe(root, "a.txt"), authWriteFn(root, "a.txt", "E0")); err != nil {
		t.Fatalf("write E0: %v", err)
	}
	racer := func(content string) {
		tmp := dir + "/ws/racer.tmp"
		if err := os.WriteFile(tmp, []byte(content), 0o644); err != nil {
			panic(err)
		}
		if err := os.Rename(tmp, dir+"/ws/a.txt"); err != nil {
			panic(err)
		}
	}
	root.faultHook = func(tag string) {
		switch tag {
		case "write.postCreate":
			racer("D")
		case "write.preUndo":
			racer("R")
		}
	}
	_, _, werr := s.WithWrite(ctx, "ws", "a.txt", "write", IfVersion{Mode: "any"},
		sha("H"), authProbe(root, "a.txt"), authWriteFn(root, "a.txt", "H"))
	root.faultHook = nil
	if !errors.Is(werr, ErrExternalChange) {
		t.Fatalf("undo-parked write = %v, want external_change", werr)
	}
	var it intent
	var resolved bool
	if err := s.pool.QueryRow(ctx,
		`SELECT id, owner, scope, op, path, version, pre_fp, dst_fp, expect_sha, resolved_at IS NOT NULL
		   FROM file_op WHERE expect_sha=$1`, sha("H")).Scan(&it.id, &it.owner, &it.scope,
		&it.op, &it.path, &it.version, &it.preFP, &it.dstFP, &it.expectSHA, &resolved); err != nil {
		t.Fatalf("undo-parked intent: %v", err)
	}
	slot := loadIntentName(t, s, it.id, 0)
	if !resolved {
		t.Fatal("undo-parked intent not tombstoned")
	}
	if got, _ := authReadOpt(dir, "ws/"+slot); got != "D" {
		t.Fatalf("slot = %q, want parked D", got)
	}
	if got, _ := authReadOpt(dir, "ws/a.txt"); got != "R" {
		t.Fatalf("a.txt = %q, want racer R", got)
	}
	authExec(t, s, `UPDATE file_op SET resolved_at = now() - interval '1 minute' WHERE id=$1`, it.id)
	return it, slot
}

// 174, decision-time position. Instance A's tombstone pass has statted
// the slot (D) and is about to hash the public name. A loses its lock
// session (its watcher has not noticed) and B becomes owner. B's client
// writes "H" — the tombstone's expected bytes — and B's fs effect
// publishes and observes O at a.txt; B's apply is still in flight. A
// resumes: before the repair it hashed O as its own bytes, swapped O into
// the slot and unlinked it at the veto; B then acknowledged a row for
// destroyed bytes.
func TestAuthPGCrossOwnerSameContentSwap(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	a := newUnwatchedPGStore(t, dsn, dir)
	authUndoParkedTombstone(t, a, root, dir)

	g := newAuthGate()
	a.SetReconcileView(authPinned(root, func(v ReconView) ReconView {
		return authView{ReconView: v, onHash: func(_, p string) {
			if p == "a.txt" {
				g.hit()
			}
		}}
	}))
	a.lastTombScan.Store(0)
	aDone := make(chan struct{})
	go func() { defer close(aDone); a.Reconcile(ctx) }()
	g.await(t, "A about to hash a.txt")

	a.releaseWriter()
	b := newPGStore(t, dsn, dir)
	b.SetReconcileView(authPinned(root, nil))
	hold := authHold(t, dsn)
	o, bDone := authHeldWrite(t, b, root, hold, "a.txt", "H")

	close(g.release)
	select {
	case <-aDone:
	case <-time.After(30 * time.Second):
		t.Fatal("A's pass did not finish")
	}
	if err := hold.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-bDone; err != nil {
		t.Fatalf("B's write was not acknowledged: %v", err)
	}
	authSettle(t, b)
	authAssertRecovered(t, b, root, dir, "a.txt", "H", o)
}

// 174, veto position. The name already holds unrecorded executor bytes
// equal to the tombstone's expectation, so A's decision-time screen and
// row check pass while A is still owner. A is held just before its swap;
// then A loses the lock, B becomes owner and B's request publishes and
// observes O. A swaps O into the slot and reaches the sealed capture and
// veto with no row yet recording O: only the veto can refuse.
func TestAuthPGCrossOwnerVetoAfterScreen(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	a := newUnwatchedPGStore(t, dsn, dir)
	authUndoParkedTombstone(t, a, root, dir)
	if err := os.WriteFile(dir+"/ws/q.tmp", []byte("H"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(dir+"/ws/q.tmp", dir+"/ws/a.txt"); err != nil {
		t.Fatal(err)
	}

	g := newAuthGate()
	a.SetReconcileView(authPinned(root, func(v ReconView) ReconView {
		return authView{ReconView: v, onMove: func(_, from, _ string) {
			if strings.HasPrefix(from, ".filesv-op-") || strings.Contains(from, "/.filesv-op-") {
				g.hit()
			}
		}}
	}))
	a.lastTombScan.Store(0)
	aDone := make(chan struct{})
	go func() { defer close(aDone); a.Reconcile(ctx) }()
	g.await(t, "A about to move the parked object off its private name")

	a.releaseWriter()
	b := newPGStore(t, dsn, dir)
	b.SetReconcileView(authPinned(root, nil))
	hold := authHold(t, dsn)
	o, bDone := authHeldWrite(t, b, root, hold, "a.txt", "H")

	close(g.release)
	select {
	case <-aDone:
	case <-time.After(30 * time.Second):
		t.Fatal("A's pass did not finish")
	}
	if got, _ := authReadOpt(dir, "ws/a.txt"); got != "H" {
		t.Logf("after A's pass a.txt = %q (A's swap moved B's object off the name)", got)
	}
	if err := hold.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-bDone; err != nil {
		t.Fatalf("B's write was not acknowledged: %v", err)
	}
	authSettle(t, b)
	authAssertRecovered(t, b, root, dir, "a.txt", "H", o)
}

// 177 generalized to a path the discarding intent never declared. Same
// Store: a real write of "T" at s.txt publishes and observes O, its apply
// is held, and the request returns unavailable at opTimeout while its
// settler goroutine keeps settling outside the scope mutex. Composed: C,
// an earlier tombstoned write of the same bytes at r.txt, and a delayed
// move of O into C's namespace (the park move drainSealed issues). The
// real pass must not unlink O; after the apply commits, recovery puts O
// back at s.txt under a coherent row.
func TestAuthPGTimedOutSettlerOtherPath(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	s.SetReconcileView(authPinned(root, nil))
	s.SetOpTimeout(400 * time.Millisecond)
	if err := os.MkdirAll(dir+"/ws", 0o755); err != nil {
		t.Fatal(err)
	}
	vC := authMint(t, s)
	hold := authHold(t, dsn)
	o, bDone := authHeldWrite(t, s, root, hold, "s.txt", "T")
	select {
	case err := <-bDone:
		if !errors.Is(err, ErrUnavailable) {
			t.Fatalf("held write returned %v, want unavailable", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("held write did not time out")
	}

	c := intent{owner: "dead-inst", scope: "ws", op: "write", path: "r.txt",
		version: vC, preFP: "0:0:0:0", expectSHA: sha("T"), at: time.Now().Add(-time.Hour)}
	c.id = insertIntent(t, s, c)
	authExec(t, s, `UPDATE file_op SET resolved_at = now() - interval '1 minute' WHERE id=$1`, c.id)
	v, err := root.pin(false)
	if err != nil {
		t.Fatal(err)
	}
	// A delayed executor-side move parks O under a private name no
	// intent journals — an orphan the sweep must surface, never delete.
	if err := v.MoveStaged("ws", "s.txt", opStagePrefix+strconv.FormatInt(c.id, 10)+"-p-aa01"); err != nil {
		t.Fatalf("delayed park move: %v", err)
	}
	v.Close()

	s.lastTombScan.Store(0)
	s.lastStageSweep.Store(0)
	s.Reconcile(ctx)
	if scanDirFor(t, dir, "ws", []byte("T")) == "" {
		t.Fatal("object observed by an in-flight settler was unlinked while its apply was pending")
	}

	if err := hold.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	authWait(t, "settler apply", func() bool {
		_, fp, found := authRow(t, s, "s.txt")
		return found && fp3(fp) == fp3(o.Fingerprint) && authSettlersDone(s)
	})
	authSettle(t, s)
	// Accepted-semantics change: recovery never moves a public object
	// back by fingerprint. O was orphaned under an unjournaled private
	// name, so it surfaces visibly with a row+event people can
	// read/delete; s.txt's recorded row may show divergence until the
	// surfaced object is moved back by an ordinary operation.
	surfaced := scanDirFor(t, dir, "ws", []byte("T"))
	if surfaced == "" {
		t.Fatal("O was destroyed")
	}
	if !durExists(t, dir, "ws/"+surfaced) || !strings.HasPrefix(surfaced, "recovered-o") &&
		!strings.Contains(surfaced, ".recovered-o") {
		t.Fatalf("O at %q — want a visible recovered-* surface name", surfaced)
	}
}

// 177 as witnessed (CORRECTION-REVIEW TestW4PGPostVetoRowCommit), with
// the assertions inverted to the required outcome. Real Rename q.txt →
// r.txt commits its fs effect; its apply blocks on a destination-subtree
// row lock and the request returns unavailable. Composed: C, a tombstoned
// write at r.txt declared earlier while O held the path (dstFP = O) with
// an apply event while its intent row is retained — the shape a diverged
// roll-forward journals — and a delayed recovery move of O into C's -p-
// namespace. O must survive the pass and return to r.txt once the rename
// settles.
func TestAuthPGWitness177PostVetoRowCommit(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	s.SetReconcileView(authPinned(root, nil))
	s.SetOpTimeout(400 * time.Millisecond)
	const content = "O-acknowledged-by-late-apply"
	if err := os.MkdirAll(dir+"/ws", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/ws/q.txt", []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	vC := authMint(t, s)
	authExec(t, s, `INSERT INTO file_version (scope, path, version, fp, updated)
		VALUES ('ws','r.txt/zzz.txt',1,'old:1:1:1',now())`)
	hold := authHold(t, dsn)
	if _, err := hold.Exec(ctx,
		`SELECT path FROM file_version WHERE scope='ws' AND path='r.txt/zzz.txt' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	renameDone := make(chan error, 1)
	go func() {
		_, _, rerr := s.Rename(ctx, "ws", "q.txt", "r.txt",
			IfVersion{Mode: "any"}, authProbe(root, "r.txt"), authProbe(root, "q.txt"),
			func(it intent) (FileInfo, bool, error) {
				return root.rename("ws", "q.txt", "r.txt", false, it)
			})
		renameDone <- rerr
	}()
	select {
	case rerr := <-renameDone:
		if !errors.Is(rerr, ErrUnavailable) {
			t.Fatalf("rename returned %v, want unavailable", rerr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("rename did not time out")
	}
	o, err := root.stat("ws", "r.txt")
	if err != nil {
		t.Fatalf("rename's fs effect did not land: %v", err)
	}

	c := intent{owner: "dead-inst", scope: "ws", op: "write", path: "r.txt",
		version: vC, preFP: o.Fingerprint, dstFP: o.Fingerprint,
		expectSHA: sha("C-content"), at: time.Now().Add(-time.Hour)}
	c.id = insertIntent(t, s, c)
	insertEvent(t, s, "ws", "r.txt", "", "write", vC)
	if !s.tombstoneIntent(ctx, c) {
		t.Fatal("tombstone C")
	}
	v, err := root.pin(false)
	if err != nil {
		t.Fatal(err)
	}
	// Delayed executor-side move of O under an unjournaled private name
	// in C's namespace — an orphan the sweep must preserve and surface.
	if err := v.MoveStaged("ws", "r.txt", opStagePrefix+strconv.FormatInt(c.id, 10)+"-p-aa01"); err != nil {
		t.Fatalf("delayed park move: %v", err)
	}
	v.Close()

	s.lastTombScan.Store(0)
	s.lastStageSweep.Store(0)
	s.Reconcile(ctx)
	if scanDirFor(t, dir, "ws", []byte(content)) == "" {
		t.Fatal("O unlinked while the rename's apply was pending (177)")
	}

	if err := hold.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	authWait(t, "rename apply", func() bool {
		_, fp, found := authRow(t, s, "r.txt")
		return found && fp3(fp) == fp3(o.Fingerprint) && authSettlersDone(s)
	})
	authSettle(t, s)
	// Accepted-semantics change: O is never moved back onto a public
	// path by fingerprint. It surfaced at a recovered-* name; the row
	// re-keyed onto where the object observably sits keeps the
	// recorded version reachable and deletable.
	surfaced := scanDirFor(t, dir, "ws", []byte(content))
	if surfaced == "" {
		t.Fatal("O was destroyed")
	}
	if !strings.HasPrefix(surfaced, "recovered-o") && !strings.Contains(surfaced, ".recovered-o") {
		t.Fatalf("O at %q — want a visible recovered-* surface name", surfaced)
	}
	if _, _, found := authRow(t, s, surfaced); !found {
		t.Fatalf("no row records the surfaced object at %q", surfaced)
	}
}

// 176. A write intent whose bytes never landed is judged diverged once
// (legitimately journaled), then a later request supersedes the path.
// Re-judging the tombstone must not mint stale events, must not refresh
// resolved_at (which re-arms pending_settlement for the next request),
// and its retained intent must not count as commit evidence.
func TestAuthPGDivergedTombstoneSuperseded(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	s.SetReconcileView(authPinned(root, nil))
	if _, _, err := s.WithWrite(ctx, "ws", "a.txt", "write", IfVersion{Mode: "any"},
		sha("v0"), authProbe(root, "a.txt"), authWriteFn(root, "a.txt", "v0")); err != nil {
		t.Fatalf("write v0: %v", err)
	}
	x := intent{owner: "dead-inst", scope: "ws", op: "write", path: "a.txt",
		version: authMint(t, s), preFP: "0:0:0:0", expectSHA: sha("never-landed"),
		at: time.Now().Add(-time.Hour)}
	x.id = insertIntent(t, s, x)
	s.Reconcile(ctx)
	if v, fp, _ := authRow(t, s, "a.txt"); v != x.version || fp != divergedFP(x.expectSHA) {
		t.Fatalf("first diverged judgment row = (%d,%q)", v, fp)
	}
	authExec(t, s, `UPDATE file_op SET resolved_at = now() - interval '1 minute' WHERE id=$1`, x.id)
	vW, _, err := s.WithWrite(ctx, "ws", "a.txt", "write", IfVersion{Mode: "any"},
		sha("v2"), authProbe(root, "a.txt"), authWriteFn(root, "a.txt", "v2"))
	if err != nil {
		t.Fatalf("successor write: %v", err)
	}
	events := func() int {
		var n int
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM file_event
			WHERE scope='ws' AND path='a.txt' AND op='write' AND version=$1`, x.version).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	resolvedAt := func() time.Time {
		var ts time.Time
		if err := s.pool.QueryRow(ctx, `SELECT resolved_at FROM file_op WHERE id=$1`, x.id).Scan(&ts); err != nil {
			t.Fatal(err)
		}
		return ts
	}
	e0, r0 := events(), resolvedAt()
	for i := 0; i < 3; i++ {
		s.lastTombScan.Store(0)
		s.Reconcile(ctx)
	}
	if e := events(); e != e0 {
		t.Errorf("stale diverged events: %d → %d across re-judgments", e0, e)
	}
	if r := resolvedAt(); !r.Equal(r0) {
		t.Errorf("tombstone resolved_at refreshed: %v → %v", r0, r)
	}
	if _, _, err := s.WithWrite(ctx, "ws", "a.txt", "write", IfVersion{Mode: "any"},
		sha("v3"), authProbe(root, "a.txt"), authWriteFn(root, "a.txt", "v3")); err != nil {
		t.Errorf("next request after re-judgment: %v", err)
	}
	if ok, err := s.intentApplied(ctx, x); err != nil || ok {
		t.Errorf("intentApplied(retained diverged tombstone) = %v, %v — want no commit evidence", ok, err)
	}
	if v, _, _ := authRow(t, s, "a.txt"); v <= vW {
		t.Errorf("row version %d did not advance past successor %d", v, vW)
	}
}

// 176, the in-tx guard. A newer version is recorded before the diverged
// apply's tx runs — as when a successor's apply commits between any
// pre-read and this tx. The apply must write no event and leave the
// tombstone's resolved_at untouched.
func TestAuthPGDivergedApplyAfterSupersede(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	ctx := context.Background()
	s := newPGStore(t, dsn, t.TempDir())
	x := intent{owner: "dead-inst", scope: "ws", op: "write", path: "a.txt",
		version: authMint(t, s), preFP: "0:0:0:0", expectSHA: sha("never-landed"),
		at: time.Now().Add(-time.Hour)}
	x.id = insertIntent(t, s, x)
	authExec(t, s, `UPDATE file_op SET resolved_at = now() - interval '1 minute' WHERE id=$1`, x.id)
	vW := authMint(t, s)
	authExec(t, s, `INSERT INTO file_version (scope, path, version, fp, updated)
		VALUES ('ws','a.txt',$1,'9:9:9:9',now())`, vW)
	var r0 time.Time
	if err := s.pool.QueryRow(ctx, `SELECT resolved_at FROM file_op WHERE id=$1`, x.id).Scan(&r0); err != nil {
		t.Fatal(err)
	}
	err := s.apply(ctx, x, FileInfo{Fingerprint: divergedFP(x.expectSHA)}, "", true)
	t.Logf("diverged apply over a newer row: %v", err)
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM file_event WHERE version=$1`, x.version).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("stale event journaled for superseded diverged apply (%d)", n)
	}
	var r1 time.Time
	if err := s.pool.QueryRow(ctx, `SELECT resolved_at FROM file_op WHERE id=$1`, x.id).Scan(&r1); err != nil {
		t.Fatal(err)
	}
	if !r1.Equal(r0) {
		t.Errorf("resolved_at refreshed %v → %v", r0, r1)
	}
	if v, fp, _ := authRow(t, s, "a.txt"); v != vW || fp != "9:9:9:9" {
		t.Errorf("newer row regressed to (%d,%q)", v, fp)
	}
}

// Authorized discard still progresses on the real crash shape. A dead
// write intent verified-exchanged its bytes onto a.txt — displacing the
// unrecorded executor object it declared — and died after the exchange
// result was journaled but before discarding it. The pass rolls the
// intent forward (apply removes the intent row with its event) and,
// with bound-oid proof that the slot holds the declared displaced
// object, deletes it.
func TestAuthPGRollForwardDiscardsDisplaced(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	s.SetReconcileView(authPinned(root, nil))
	if err := os.MkdirAll(dir+"/ws", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/ws/a.txt", []byte("O0"), 0o644); err != nil {
		t.Fatal(err)
	}
	pre, err := root.stat("ws", "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	x := intent{owner: "dead-inst", scope: "ws", op: "write", path: "a.txt",
		version: authMint(t, s), preFP: pre.Fingerprint, dstFP: pre.Fingerprint,
		expectSHA: sha("v1"), at: time.Now().Add(-time.Hour)}
	x.id = insertIntent(t, s, x)
	slot := opStagePrefix + "o" + strconv.FormatInt(x.id, 10) + "-a0-t"
	// The post-exchange crash state: the object declared at a.txt sits
	// at the private name (moved, keeping its identity), and the
	// write's authored bytes occupy a.txt.
	if err := os.Rename(dir+"/ws/a.txt", dir+"/ws/"+slot); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/ws/a.txt", []byte("v1"), 0o644); err != nil {
		t.Fatal(err)
	}
	authExec(t, s, `UPDATE file_op SET dst_oid=$2, dst_sha=$3, names=$4::jsonb WHERE id=$1`,
		x.id, pre.Oid, sha("O0"),
		mustJSON([]nameRec{{Name: slot, Act: "xch", Src: "a.txt", Res: "ok"}}))

	s.Reconcile(ctx)
	if got, _ := authReadOpt(dir, "ws/a.txt"); got != "v1" {
		t.Fatalf("a.txt = %q, want rolled-forward v1", got)
	}
	if ver, fp, _ := authRow(t, s, "a.txt"); ver != x.version || fp3(fp) != fp3(durFP(t, root, "ws", "a.txt")) {
		t.Fatalf("row (%d,%q) does not record the roll-forward", ver, fp)
	}
	if n := intentCount(t, s); n != 0 {
		t.Fatalf("intent rows left: %d", n)
	}
	if where := scanDirFor(t, dir, "ws", []byte("O0")); where != "" {
		if pre.Oid == "" {
			// Unbound identity on this filesystem: the displaced object
			// must be preserved visibly, never discarded on weak
			// evidence.
			if !strings.Contains(where, "recovered-o") {
				t.Fatalf("unbound displaced object at %q — want a recovered-* surface", where)
			}
			return
		}
		t.Fatalf("authorized displaced object still parked at %q", where)
	}
}
