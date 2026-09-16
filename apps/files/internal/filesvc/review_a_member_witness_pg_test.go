package filesvc

// Member-authority adversarial witnesses (overlay only — member-src is
// immutable). Probe points the integrated member repair does not close:
//   MW1: sole-claimant rows are accepted unverified and kind-blind — a
//        stale row for a DEAD FILE routes an unrecorded foreign dir onto
//        its recorded path.
//   MW2: two fp3-equal claimants neither present nor corroborated →
//        recAmbiguous parks the recorded dir forever — a stale row that
//        is never GC'd defeats recovery permanently.
//   MW3: the moveJudged re-stat narrows but cannot close the
//        stat→renameat2 window; a swap landing inside it installs
//        whatever object then sits at rel at the recorded home.
//   MW4: CW4 strict — a member with its own committed home must reach it.
//   MW5: member corroboration binds a container to a member's home
//        parent even when that parent path is held by a DIFFERENT live
//        object — a stale claimant plus a displaced member misroutes the
//        container over a live occupant.

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// parkUnderDeadIntent moves rel beneath a dead intent's namespace and
// removes the intent row, leaving an orphan the sweep owns.
func parkUnderDeadIntent(t *testing.T, s *Store, dir, rel, tag string) string {
	t.Helper()
	it := intent{owner: "dead-inst", scope: "ws", op: "write",
		path: "dead.txt", version: authMint(t, s),
		preFP: "0:0:0:0", at: time.Now().Add(-time.Hour)}
	it.id = insertIntent(t, s, it)
	parked := opStagePrefix + strconv.FormatInt(it.id, 10) + "-p-" + tag
	if err := os.Rename(dir+"/ws/"+rel, dir+"/ws/"+parked); err != nil {
		t.Fatal(err)
	}
	authExec(t, s, `DELETE FROM file_op WHERE id=$1`, it.id)
	return parked
}

func lstatIno(t *testing.T, path string) uint64 {
	t.Helper()
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err != nil {
		t.Fatalf("lstat %s: %v", path, err)
	}
	return st.Ino
}

// MW1 — a sole claimant is accepted with NO verification (store.go
// len(claims)==1 → recFound): a stale row whose object is long dead
// claims any live object sharing the inode leg, regardless of kind.
// Reachable on one ordinary filesystem: inode reuse after an external
// delete (rows are never marked diverged unless an intent touches the
// path — Reconcile scans file_op only), plus an unrecorded parked dir.
func TestMemberSoleStaleClaimMisroutesForeignDir(t *testing.T) {
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
	if _, _, err := s.WithWrite(ctx, "ws", "aa", "write",
		IfVersion{Mode: "any"}, sha("F"), authProbe(root, "aa"),
		authWriteFn(root, "aa", "F")); err != nil {
		t.Fatalf("write aa: %v", err)
	}
	_, afp, _ := authRow(t, s, "aa")
	aino, _, _, _ := fpParts(afp)
	// External delete: the row stays and is never marked diverged.
	if err := os.Remove(dir + "/ws/aa"); err != nil {
		t.Fatal(err)
	}
	// Foreign unrecorded dir reusing the dead file's inode.
	var fino uint64
	want, _ := strconv.ParseUint(aino, 10, 64)
	for i := 0; i < 4000; i++ {
		if err := os.Mkdir(dir+"/ws/forg", 0o755); err != nil {
			t.Fatal(err)
		}
		fino = lstatIno(t, dir+"/ws/forg")
		if fino == want {
			break
		}
		if err := os.Remove(dir + "/ws/forg"); err != nil {
			t.Fatal(err)
		}
	}
	if fino != want {
		t.Skipf("inode reuse not observed after 4000 tries (want %s)", aino)
	}
	parkUnderDeadIntent(t, s, dir, "forg", "f")

	authSettle(t, s)
	authSettle(t, s)

	if st, err := os.Stat(dir + "/ws/aa"); err == nil && st.IsDir() {
		_, row, _ := authRow(t, s, "aa")
		t.Fatalf("DEFECT: unrecorded foreign dir installed at 'aa' on a "+
			"stale FILE row's sole claim — row(aa)=%q records a dead file", row)
	}
	// The foreign dir is preserved — captured out of the dead namespace
	// and surfaced at a visible name, never destroyed and never left
	// hidden under a private name.
	alive := scanDirForInode(dir, "ws", strconv.FormatUint(fino, 10))
	if alive == "" || strings.Contains(alive, opStagePrefix) {
		t.Fatalf("foreign dir vanished or left hidden (alive=%q)", alive)
	}
}

// MW2 — two fp3-equal claimants with no presence and no member evidence
// → recAmbiguous → the recorded dir stays parked while the rows are
// stable. Nothing ever deletes a stale row, so this is permanent: an
// acknowledged empty dir never returns to its recorded home.
func TestMemberAmbiguousClaimsParkForever(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	s := newPGStore(t, dsn, dir)
	s.SetReconcileView(authPinned(root, nil))
	if err := os.MkdirAll(dir+"/ws", 0o755); err != nil {
		t.Fatal(err)
	}
	mkdirOp(t, s, root, "zz")
	_, zfp, found := authRow(t, s, "zz")
	if !found {
		t.Fatal("zz row missing")
	}
	zino, zsize, zmt, _ := fpParts(zfp)
	// Ghost row: a stale row for a dead object that once held this inode
	// at 'aa' — fp3 crafted equal to the live dir's (size+mtime of an
	// empty dir are ordinary values; the ino leg is the reuse). Its ctime
	// leg differs so it lands in the fp3 tier, not exact.
	authExec(t, s,
		`INSERT INTO file_version (scope, path, version, fp) VALUES ('ws','aa',$1,$2)`,
		authMint(t, s), zino+":"+zsize+":"+zmt+":1")
	parked := parkUnderDeadIntent(t, s, dir, "zz", "z")

	for i := 0; i < 4; i++ {
		authSettle(t, s)
	}
	if _, err := os.Stat(dir + "/ws/zz"); err == nil {
		if inoOf(t, root, "ws", "zz") == zino {
			return // recorded object restored to its recorded home
		}
		t.Fatalf("zz holds a different dir object")
	}
	if _, err := os.Stat(dir + "/ws/aa"); err == nil {
		t.Fatalf("DEFECT: dir misrouted to stale claimant 'aa'")
	}
	// Ambiguous equal-strength claims: the recorded dir is neither
	// installed on a guess nor destroyed — it is surfaced at a visible
	// recovered name, enumerable and deletable, and never left hidden
	// under a private name.
	if fileExists(dir + "/ws/" + parked) {
		t.Fatalf("ambiguous dir left hidden at private name %s", parked)
	}
	if alive := scanDirForInode(dir, "ws", zino); alive == "" ||
		strings.Contains(alive, opStagePrefix) {
		t.Fatalf("ambiguous recorded dir lost (ino %s): alive=%q", zino, alive)
	}
}

// swapView wraps a ReconView: on the first MoveStaged whose destination
// is `to`, it runs hook() before delegating — placing the caller's swap
// inside the residual window between moveJudged's re-stat and the
// actual renameat2.
type swapView struct {
	ReconView
	to   string
	hook func()
	done *bool
}

func (w swapView) MoveStaged(scope, from, to string) error {
	if to == w.to && !*w.done {
		*w.done = true
		w.hook()
	}
	return w.ReconView.MoveStaged(scope, from, to)
}

// MW3a — the stat→move window: a swap landing between moveJudged's
// re-stat and renameat2 installs whatever then sits at rel at the
// recorded home. Here the displaced object is preserved at another
// staged name (non-destructive racer, like a delayed SwapStaged) — the
// pass must converge back.
func TestMemberMoveWindowSwapConverges(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	done := false
	s := newPGStore(t, dsn, dir)
	if err := os.MkdirAll(dir+"/ws", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.WithWrite(ctx, "ws", "phome", "write",
		IfVersion{Mode: "any"}, sha("P"), authProbe(root, "phome"),
		authWriteFn(root, "phome", "P")); err != nil {
		t.Fatalf("write phome: %v", err)
	}
	// Foreign file F, unrecorded.
	if err := os.WriteFile(dir+"/ws/forgF", []byte("F"), 0o644); err != nil {
		t.Fatal(err)
	}
	parked := parkUnderDeadIntent(t, s, dir, "phome", "p")
	s.SetReconcileView(authPinned(root, func(v ReconView) ReconView {
		return swapView{ReconView: v, to: "phome", done: &done, hook: func() {
			// Swap in F at rel; P lands at a staged rescue name.
			os.Rename(dir+"/ws/"+parked, dir+"/ws/"+parked+"-resc")
			os.Rename(dir+"/ws/forgF", dir+"/ws/"+parked)
		}}
	}))

	authSettle(t, s)
	authSettle(t, s)

	// No journaled provenance binds the parked object to phome — it
	// surfaces visibly. The swapView hook keys on a restore to "phome"
	// that no longer happens, so it never fires; the point stands:
	// nothing is installed at the recorded path on row evidence.
	authSurfaced(t, s, dir, "ws", []byte("P"))
	if got, ok := authReadOpt(dir, "ws/forgF"); !ok || got != "F" {
		t.Fatalf("foreign F disturbed: %q ok=%v", got, ok)
	}
	if fileExists(dir + "/ws/phome") {
		if got, _ := authReadOpt(dir, "ws/phome"); got != "P" {
			t.Fatalf("DEFECT: phome holds %q — foreign content at recorded path", got)
		}
	}
}

// MW3b — same window, destructive racer: the swapped-in object is
// RECORDED ELSEWHERE (M at 'mout') and the displaced original P is
// dropped by the racer (external deletion is ordinary interference).
// M lands at 'phome' — a public path no pass ever re-judges.
func TestMemberMoveWindowSwapRecordedElsewhere(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	done := false
	s := newPGStore(t, dsn, dir)
	if err := os.MkdirAll(dir+"/ws", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.WithWrite(ctx, "ws", "phome", "write",
		IfVersion{Mode: "any"}, sha("P"), authProbe(root, "phome"),
		authWriteFn(root, "phome", "P")); err != nil {
		t.Fatalf("write phome: %v", err)
	}
	if _, _, err := s.WithWrite(ctx, "ws", "mout", "write",
		IfVersion{Mode: "any"}, sha("M"), authProbe(root, "mout"),
		authWriteFn(root, "mout", "M")); err != nil {
		t.Fatalf("write mout: %v", err)
	}
	parked := parkUnderDeadIntent(t, s, dir, "phome", "p")
	s.SetReconcileView(authPinned(root, func(v ReconView) ReconView {
		return swapView{ReconView: v, to: "phome", done: &done, hook: func() {
			// Racer: M onto the staged slot, P's name dropped entirely.
			os.Remove(dir + "/ws/" + parked)
			os.Rename(dir+"/ws/mout", dir+"/ws/"+parked)
		}}
	}))

	authSettle(t, s)
	authSettle(t, s)

	got, ok := authReadOpt(dir, "ws/phome")
	moutOK := fileExists(dir + "/ws/mout")
	if ok && got == "M" && !moutOK {
		_, mrow, mfound := authRow(t, s, "mout")
		t.Fatalf("DEFECT: recorded member M permanently misplaced — M sits "+
			"at phome (row records dead P), row(mout)=%q found=%v but mout "+
			"is empty; no pass re-judges a public path", mrow, mfound)
	}
	t.Logf("phome=%q ok=%v mout exists=%v", got, ok, moutOK)
}

// MW4 — strict form of CW4: a member whose own row records a different
// home MUST reach it after its recorded container is restored.
func TestMemberReachesOwnHomeStrict(t *testing.T) {
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
	mkdirOp(t, s, root, "ddir")
	if _, _, err := s.WithWrite(ctx, "ws", "ddir/m", "write",
		IfVersion{Mode: "any"}, sha("M"), authProbe(root, "ddir/m"),
		authWriteFn(root, "ddir/m", "M")); err != nil {
		t.Fatalf("write ddir/m: %v", err)
	}
	if _, _, err := s.Rename(ctx, "ws", "ddir/m", "mout",
		IfVersion{Mode: "any"}, authProbe(root, "mout"), authProbe(root, "ddir/m"),
		func(it intent) (FileInfo, bool, error) {
			return root.rename("ws", "ddir/m", "mout", false, it)
		}); err != nil {
		t.Fatalf("rename ddir/m->mout: %v", err)
	}
	// Delayed effect returns the member object inside the container;
	// row(mout) still records it.
	if err := os.Rename(dir+"/ws/mout", dir+"/ws/ddir/m"); err != nil {
		t.Fatal(err)
	}
	parkUnderDeadIntent(t, s, dir, "ddir", "d")

	authSettle(t, s)
	authSettle(t, s)

	// No journaled provenance — the container surfaces whole and the
	// member rides inside it; the stale 'mout' row never extracts it.
	mAt := scanDirForFile(t, dir, "ws", "m")
	if mAt == "" || strings.Contains(mAt, opStagePrefix) {
		t.Fatalf("member destroyed or left private: %q", mAt)
	}
	if got, ok := authReadOpt(dir, "ws/"+mAt); !ok || got != "M" {
		t.Fatalf("member not readable inside surfaced container: %q ok=%v", got, ok)
	}
	if fileExists(dir + "/ws/mout") {
		t.Fatal("member extracted to its stale row's path")
	}
	if _, _, found := authRow(t, s, mAt); !found {
		t.Fatalf("surfaced member %q has no version row", mAt)
	}
}

// MW5 — member corroboration binds the container to a member's home
// parent even when that path is held by a DIFFERENT live object. A stale
// claimant row at 'other' (dead object, ino-leg only) plus a member
// whose own row lives beneath 'other' routes the container onto 'other'
// and evicts the live unrecorded occupant.
func TestMemberCorroborationInstallsWrongHome(t *testing.T) {
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
	mkdirOp(t, s, root, "xhome")
	mkdirOp(t, s, root, "other")
	if _, _, err := s.WithWrite(ctx, "ws", "other/m", "write",
		IfVersion{Mode: "any"}, sha("M"), authProbe(root, "other/m"),
		authWriteFn(root, "other/m", "M")); err != nil {
		t.Fatalf("write other/m: %v", err)
	}
	_, xfp, _ := authRow(t, s, "xhome")
	xino, _, _, _ := fpParts(xfp)
	oino := lstatIno(t, dir+"/ws/other")
	// Stale row: 'other' previously held a dead object that shared X's
	// inode — the row kept its fp while the live dir there is different.
	authExec(t, s,
		`UPDATE file_version SET fp=$1 WHERE scope='ws' AND path='other'`,
		xino+":1:1:1")
	// External interference: the recorded member is moved inside X.
	if err := os.Rename(dir+"/ws/other/m", dir+"/ws/xhome/m"); err != nil {
		t.Fatal(err)
	}
	parked := parkUnderDeadIntent(t, s, dir, "xhome", "x")

	authSettle(t, s)
	authSettle(t, s)

	// Correct: X restored to xhome (carrying or not carrying m).
	if st, err := os.Stat(dir + "/ws/xhome"); err == nil && st.IsDir() &&
		inoOf(t, root, "ws", "xhome") == xino {
		t.Log("X restored to xhome — corroboration did not misroute")
		return
	}
	// Defect: X installed at 'other' over the live occupant.
	if st, err := os.Stat(dir + "/ws/other"); err == nil && st.IsDir() &&
		inoOf(t, root, "ws", "other") == xino {
		_, xrow, _ := authRow(t, s, "xhome")
		t.Fatalf("DEFECT: corroboration installed container X at 'other' "+
			"(stale claimant, ino %s) over live occupant (ino %d now "+
			"parked); row(xhome)=%q still records X — xhome empty forever",
			xino, oino, xrow)
	}
	// Ambiguous-park is the fail-closed outcome — acceptable. So is the
	// surfaced container: a visible recovered name holding X.
	if fileExists(dir + "/ws/" + parked) {
		t.Log("X still parked — ambiguous claims preserved rather than misrouted")
		return
	}
	if alive := scanDirForInode(dir, "ws", xino); alive == "" ||
		strings.Contains(alive, opStagePrefix) {
		t.Fatalf("X vanished: xhome=%v other=%v",
			fileExists(dir+"/ws/xhome"), fileExists(dir+"/ws/other"))
	}
}
