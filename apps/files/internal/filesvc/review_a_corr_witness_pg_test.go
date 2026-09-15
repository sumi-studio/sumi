package filesvc

// CORRECTION-ROUND adversarial witnesses — review A, candidate 92ccc9d9.
//
// Root concern under test: recordedMatch/sameObjectAt judge directory
// identity by the inode leg alone (no dev, no kind). Two live objects on
// different filesystems sharing an inode number are DIFFERENT objects —
// and a stale row's ino leg can collide with a parked dir after inode
// reuse. The destinations of that judgment are (a) settleDelete's
// surplus-link delete authorization and (b) restore routing. This file
// drives the real capture/veto/unlink machinery with real tmpfs mount
// boundaries inside the scope where a second st_dev is required.
//
// Runs unprivileged only where mounts are unavailable: tests that need a
// second device call tryMount and t.Skip on EPERM. The intended runtime
// is `unshare -rm go test ...` inside the review container.

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func tryMount(t *testing.T, target string) {
	t.Helper()
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount("none", target, "tmpfs", 0, ""); err != nil {
		t.Skipf("no mount privilege (%v) — cross-device witnesses need unshare -rm", err)
	}
	t.Cleanup(func() { _ = unix.Unmount(target, unix.MNT_DETACH) })
}

// inoOf returns the inode leg of a live fingerprint, or "" on fallback.
func inoOf(t *testing.T, root *posixRoot, scope, path string) string {
	t.Helper()
	ino, _, _, ok := fpParts(durFP(t, root, scope, path))
	if !ok {
		t.Fatalf("%s: no inode leg in fingerprint", path)
	}
	return ino
}

// mkdirOp records a real directory through the production mkdir path.
func mkdirOp(t *testing.T, s *Store, root *posixRoot, path string) {
	t.Helper()
	if _, _, err := s.WithWrite(context.Background(), "ws", path, "mkdir",
		IfVersion{Mode: "any"}, "dir", authProbe(root, path),
		func(it intent) (FileInfo, bool, error) {
			return root.mkdir("ws", path)
		}); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
}

// treeHasIno reports whether any name beneath scope dir still resolves
// to the given inode (any device) — i.e. the object still exists.
func treeHasIno(t *testing.T, dir, scope, ino string) bool {
	found := false
	root := dir + "/" + scope
	var walk func(string)
	walk = func(d string) {
		ents, err := os.ReadDir(d)
		if err != nil {
			return
		}
		for _, e := range ents {
			p := d + "/" + e.Name()
			var st unix.Stat_t
			if err := unix.Lstat(p, &st); err != nil {
				continue
			}
			if strconv.FormatUint(st.Ino, 10) == ino {
				found = true
			}
			if e.IsDir() {
				walk(p)
			}
		}
	}
	walk(root)
	return found
}

// devInoOf returns "dev:ino" for a filesystem path — real object identity.
func devInoOf(t *testing.T, path string) string {
	t.Helper()
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err != nil {
		t.Fatalf("lstat %s: %v", path, err)
	}
	return fmt.Sprintf("%d:%d", st.Dev, st.Ino)
}

// treeHasDevIno reports whether the object with this exact dev:ino still
// exists anywhere beneath the scope — distinguishes colliding inodes
// across devices, which is the whole point of the exercise.
func treeHasDevIno(t *testing.T, dir, scope, devino string) bool {
	var devStr, inoStr string
	fmt.Sscanf(devino, "%s", &devStr) // placeholder; parse below
	parts := strings.SplitN(devino, ":", 2)
	devStr, inoStr = parts[0], parts[1]
	found := false
	root := dir + "/" + scope
	var walk func(string)
	walk = func(d string) {
		ents, err := os.ReadDir(d)
		if err != nil {
			return
		}
		for _, e := range ents {
			p := d + "/" + e.Name()
			var st unix.Stat_t
			if err := unix.Lstat(p, &st); err != nil {
				continue
			}
			if strconv.FormatUint(st.Dev, 10) == devStr &&
				strconv.FormatUint(st.Ino, 10) == inoStr {
				found = true
			}
			if e.IsDir() {
				walk(p)
			}
		}
	}
	walk(root)
	return found
}

// corrFixture builds the two-device scope: ext4 scope root plus tmpfs
// mounts at ws/mntA and ws/mntB, and a recorded empty dir X at
// mntB/xd whose inode collides with a recorded dir at mntA/ad<i>.
// Returns the collider path and X's row fp.
func corrFixture(t *testing.T, s *Store, root *posixRoot, dir string) (collider, xfp string) {
	t.Helper()
	if err := os.MkdirAll(dir+"/ws", 0o755); err != nil {
		t.Fatal(err)
	}
	tryMount(t, dir+"/ws/mntA")
	tryMount(t, dir+"/ws/mntB")
	// X: recorded dir on tmpfsB. Fresh tmpfs ino counters are sequential
	// from a low base, so candidate dirs on tmpfsA collide quickly.
	mkdirOp(t, s, root, "mntB/xd")
	_, xfp, found := authRow(t, s, "mntB/xd")
	if !found {
		t.Fatal("mntB/xd row missing")
	}
	xino, _, _, ok := fpParts(xfp)
	if !ok {
		t.Fatalf("row fp %q has no inode leg", xfp)
	}
	for i := 0; i < 500; i++ {
		p := fmt.Sprintf("mntA/ad%d", i)
		mkdirOp(t, s, root, p)
		if inoOf(t, root, "ws", p) == xino {
			return p, xfp
		}
	}
	t.Skipf("no inode collision after 500 candidates (xino=%s)", xino)
	return "", ""
}

// CW1 — root's concern, destructive direction. A recorded dir X parked
// at a committed intent's staged slot must be RESTORED, not deleted.
// With a colliding ino row whose home holds a same-ino dir on ANOTHER
// device, the veto's sameObjectAt misclassifies "still at recorded home"
// and the sealed capture is unlinked — acknowledged content destroyed.
func TestCorrCrossDeviceInoDelete(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	s := newPGStore(t, dsn, dir)
	s.SetReconcileView(authPinned(root, nil))
	collider, xfp := corrFixture(t, s, root, dir)
	xDevIno := devInoOf(t, dir+"/ws/mntB/xd")
	t.Logf("X recorded at mntB/xd dev:ino=%s; collider at %s", xDevIno, collider)

	// A pending remove intent whose authorized-displacement object is X
	// (dstFP = X's fp — X was the object at the intent's declared path).
	// Its own namespace lives on tmpfsB so the parked object can rename in.
	if err := os.MkdirAll(dir+"/ws/mntB/work", 0o755); err != nil {
		t.Fatal(err)
	}
	it := intent{owner: s.owner, scope: "ws", op: "remove",
		path: "mntB/work/q", version: authMint(t, s),
		preFP: "0:0:0:0", dstFP: xfp, at: time.Now()}
	it.id = insertIntent(t, s, it)
	slot := "mntB/work/" + opStagePrefix + strconv.FormatInt(it.id, 10)
	if err := os.Rename(dir+"/ws/mntB/xd", dir+"/ws/"+slot); err != nil {
		t.Fatal(err)
	}
	// The intent's own apply commits between the veto's recordersActive
	// read and its row read — the position discardVetoHook marks.
	discardVetoHook = func() {
		authExec(t, s, `DELETE FROM file_op WHERE id=$1`, it.id)
		authExec(t, s,
			`INSERT INTO file_event (scope, path, op, version) VALUES ($1,$2,$3,$4)`,
			"ws", it.path, "remove", it.version)
	}
	defer func() { discardVetoHook = nil }()

	authSettle(t, s)

	if treeHasDevIno(t, dir, "ws", xDevIno) {
		if _, err := os.Stat(dir + "/ws/mntB/xd"); err == nil {
			t.Log("X restored to its recorded home — correct outcome")
			return
		}
		t.Fatalf("X still parked somewhere instead of restored to mntB/xd")
	}
	_, fp, found := authRow(t, s, "mntB/xd")
	t.Fatalf("DEFECT: recorded dir X destroyed — dev:ino %s absent everywhere; "+
		"row(mntB/xd) still records fp=%q (found=%v). Cross-device ino "+
		"collision at %s satisfied the veto's 'still at recorded home' check",
		xDevIno, fp, found, collider)
}

// CW2 — same collision through the orphan sweep: no delete is involved,
// but the surplus-link verdict "still present at recorded home" leaves
// the recorded dir parked forever — its row claims it, its home is empty.
func TestCorrCrossDeviceInoStrand(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	s := newPGStore(t, dsn, dir)
	s.SetReconcileView(authPinned(root, nil))
	collider, xfp := corrFixture(t, s, root, dir)
	xino, _, _, _ := fpParts(xfp)
	t.Logf("X recorded at mntB/xd ino=%s; collider at %s", xino, collider)

	if err := os.MkdirAll(dir+"/ws/mntB/work", 0o755); err != nil {
		t.Fatal(err)
	}
	it := intent{owner: "dead-inst", scope: "ws", op: "write",
		path: "mntB/work/dead.txt", version: authMint(t, s),
		preFP: "0:0:0:0", at: time.Now().Add(-time.Hour)}
	it.id = insertIntent(t, s, it)
	parked := "mntB/work/" + opStagePrefix + strconv.FormatInt(it.id, 10) + "-p-x"
	if err := os.Rename(dir+"/ws/mntB/xd", dir+"/ws/"+parked); err != nil {
		t.Fatal(err)
	}
	authExec(t, s, `DELETE FROM file_op WHERE id=$1`, it.id)

	authSettle(t, s)
	authSettle(t, s)

	if _, err := os.Stat(dir + "/ws/" + parked); err == nil {
		_, fp, found := authRow(t, s, "mntB/xd")
		t.Fatalf("DEFECT: recorded dir X stranded at %q — row(mntB/xd) records "+
			"fp=%q found=%v but the sweep judged it 'still at home' against the "+
			"cross-device collider at %s", parked, fp, found, collider)
	}
	if _, err := os.Stat(dir + "/ws/mntB/xd"); err != nil {
		where := ""
		if treeHasIno(t, dir, "ws", xino) {
			where = "elsewhere in scope"
		}
		t.Fatalf("X neither parked nor at home — moved to %s", where)
	}
	// Correct outcome: X back at its recorded home.
}

// CW3 — single device, no mounts: a STALE row (its object externally
// deleted) whose ino leg collides with the parked recorded dir wins the
// ORDER BY path LIMIT 1 and misroutes the restore — the dir is installed
// at a name whose row records a different (dead, different-kind) object
// while its real home stays empty forever.
func TestCorrStaleRowInoMisroute(t *testing.T) {
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
	zino, _, _, _ := fpParts(zfp)
	// Stale row at "aa": a file once recorded there, externally deleted —
	// rows are not tombstoned by external divergence. Its ino leg happens
	// to equal the parked dir's (inode reuse — the row records a FILE).
	authExec(t, s,
		`INSERT INTO file_version (scope, path, version, fp) VALUES ('ws','aa',$1,$2)`,
		authMint(t, s), zino+":777:111:222")
	// Park zz's dir object under a dead intent's namespace.
	it := intent{owner: "dead-inst", scope: "ws", op: "write",
		path: "dead.txt", version: authMint(t, s),
		preFP: "0:0:0:0", at: time.Now().Add(-time.Hour)}
	it.id = insertIntent(t, s, it)
	parked := opStagePrefix + strconv.FormatInt(it.id, 10) + "-p-z"
	if err := os.Rename(dir+"/ws/zz", dir+"/ws/"+parked); err != nil {
		t.Fatal(err)
	}
	authExec(t, s, `DELETE FROM file_op WHERE id=$1`, it.id)

	authSettle(t, s)
	authSettle(t, s)

	if st, err := os.Stat(dir + "/ws/zz"); err == nil && st.IsDir() {
		if inoOf(t, root, "ws", "zz") == zino {
			return // correct: recorded object restored to its recorded home
		}
		t.Fatalf("zz restored but holds a different dir object (ino %s)", inoOf(t, root, "ws", "zz"))
	}
	if st, err := os.Stat(dir + "/ws/aa"); err == nil && st.IsDir() {
		if inoOf(t, root, "ws", "aa") == zino {
			_, zrow, _ := authRow(t, s, "zz")
			t.Fatalf("DEFECT: recorded dir misrouted to stale file row's path "+
				"'aa' — its recorded home 'zz' is empty while row(zz)=%q still "+
				"records it; nothing re-judges a public path", zrow)
		}
	}
	t.Fatalf("parked dir vanished: parked=%v aa=%v zz=%v",
		fileExists(dir+"/ws/"+parked), fileExists(dir+"/ws/aa"), fileExists(dir+"/ws/zz"))
}

func fileExists(p string) bool {
	_, err := os.Lstat(p)
	return err == nil
}

// CW4 — wholesale restore of a recorded dir carrying a member whose OWN
// row records a different home: the member rides inside the restored dir
// to a public path and is never re-judged; its recorded home stays empty.
func TestCorrWholesaleRestoreMemberElsewhere(t *testing.T) {
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
	// A committed rename moves m's row to mout; a delayed foreign effect
	// physically returns the object into ddir (external divergence — the
	// row at mout still records it).
	if _, _, err := s.Rename(ctx, "ws", "ddir/m", "mout",
		IfVersion{Mode: "any"}, authProbe(root, "mout"), authProbe(root, "ddir/m"),
		func(it intent) (FileInfo, bool, error) {
			return root.rename("ws", "ddir/m", "mout", false,
				it.dstFP, it.preFP, opStagePrefix+strconv.FormatInt(it.id, 10))
		}); err != nil {
		t.Fatalf("rename ddir/m->mout: %v", err)
	}
	if got, ok := authReadOpt(dir, "ws/mout"); !ok || got != "M" {
		t.Fatalf("rename did not land: mout=%q present=%v", got, ok)
	}
	if err := os.Rename(dir+"/ws/mout", dir+"/ws/ddir/m"); err != nil {
		t.Fatal(err)
	}
	// Park the whole recorded dir under a dead intent's namespace.
	it := intent{owner: "dead-inst", scope: "ws", op: "write",
		path: "dead.txt", version: authMint(t, s),
		preFP: "0:0:0:0", at: time.Now().Add(-time.Hour)}
	it.id = insertIntent(t, s, it)
	parked := opStagePrefix + strconv.FormatInt(it.id, 10) + "-p-d"
	if err := os.Rename(dir+"/ws/ddir", dir+"/ws/"+parked); err != nil {
		t.Fatal(err)
	}
	authExec(t, s, `DELETE FROM file_op WHERE id=$1`, it.id)

	authSettle(t, s)
	authSettle(t, s)

	// The recorded dir must be restored. Where did m end up?
	if st, err := os.Stat(dir + "/ws/ddir"); err != nil || !st.IsDir() {
		t.Fatalf("recorded dir not restored to ddir")
	}
	if _, ok := authReadOpt(dir, "ws/mout"); ok {
		t.Log("member m restored to its own recorded home mout — best outcome")
		return
	}
	got, ok := authReadOpt(dir, "ws/ddir/m")
	_, mrow, mfound := authRow(t, s, "mout")
	t.Logf("after wholesale restore: ddir/m=%q present=%v row(mout)=%q found=%v",
		got, ok, mrow, mfound)
	if ok && mfound {
		t.Logf("RESIDUAL: member m rode inside the restored dir to ddir/m; " +
			"row(mout) still records it and no pass re-judges a public path — " +
			"mout stays empty while ddir/m reports external_change")
	}
}

// CW5a — 194 regression check: committed write whose slot capture grabs a
// foreign object must journal the COMMITTED object's real identity and
// retain the tombstone (the empty-fp defect from the late head).
func TestCorrWriteCommitParkedJournalsRealFP(t *testing.T) {
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
	if _, _, err := s.WithWrite(ctx, "ws", "f.txt", "write",
		IfVersion{Mode: "any"}, sha("OLD"), authProbe(root, "f.txt"),
		authWriteFn(root, "f.txt", "OLD")); err != nil {
		t.Fatalf("write OLD: %v", err)
	}
	var id int64
	root.faultHook = func(tag string) {
		if tag != "write.preSlotDelete" {
			return
		}
		if err := os.WriteFile(dir+"/ws/.filesv-tmp-inj", []byte("FOREIGN-X"), 0o644); err != nil {
			panic(err)
		}
		slot := opStagePrefix + strconv.FormatInt(id, 10)
		if err := os.Rename(dir+"/ws/.filesv-tmp-inj", dir+"/ws/"+slot); err != nil {
			panic(err)
		}
	}
	_, _, werr := s.WithWrite(ctx, "ws", "f.txt", "write",
		IfVersion{Mode: "any"}, sha("W2"), authProbe(root, "f.txt"),
		func(it intent) (FileInfo, bool, error) {
			id = it.id
			return root.atomicWrite("ws", "f.txt", []byte("W2"), false,
				it.dstFP, opStagePrefix+strconv.FormatInt(it.id, 10))
		})
	root.faultHook = nil
	t.Logf("write err: %v", werr)
	if got, _ := authReadOpt(dir, "ws/f.txt"); got != "W2" {
		t.Fatalf("f.txt = %q, want committed W2", got)
	}
	live := durFP(t, root, "ws", "f.txt")
	v, fp, _ := authRow(t, s, "f.txt")
	if fp == "" {
		t.Fatalf("journaled fp=\"\" — empty-identity defect persists")
	}
	if fp3(fp) != fp3(live) {
		t.Fatalf("journaled fp=%q does not name the committed object %q", fp, live)
	}
	t.Logf("row=(%d,%q) live=%q intents=%d", v, fp, live, intentCount(t, s))
}

// CW5b — the LW5b cascade must stay closed: tombstone re-judgment sees
// the parked foreign object, the live acknowledged object, and a row
// that records the live object — no swap, no delete.
func TestCorrWriteCommitParkedCascadeClosed(t *testing.T) {
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
	if _, _, err := s.WithWrite(ctx, "ws", "f.txt", "write",
		IfVersion{Mode: "any"}, sha("OLD"), authProbe(root, "f.txt"),
		authWriteFn(root, "f.txt", "OLD")); err != nil {
		t.Fatalf("write OLD: %v", err)
	}
	var id int64
	root.faultHook = func(tag string) {
		if tag != "write.preSlotDelete" {
			return
		}
		if err := os.WriteFile(dir+"/ws/.filesv-tmp-inj", []byte("FOREIGN-X"), 0o644); err != nil {
			panic(err)
		}
		slot := opStagePrefix + strconv.FormatInt(id, 10)
		if err := os.Rename(dir+"/ws/.filesv-tmp-inj", dir+"/ws/"+slot); err != nil {
			panic(err)
		}
	}
	_, _, _ = s.WithWrite(ctx, "ws", "f.txt", "write",
		IfVersion{Mode: "any"}, sha("W2"), authProbe(root, "f.txt"),
		func(it intent) (FileInfo, bool, error) {
			id = it.id
			return root.atomicWrite("ws", "f.txt", []byte("W2"), false,
				it.dstFP, opStagePrefix+strconv.FormatInt(it.id, 10))
		})
	root.faultHook = nil

	authSettle(t, s)
	authSettle(t, s)

	got, ok := authReadOpt(dir, "ws/f.txt")
	where := scanDirFor(t, dir, "ws", []byte("W2"))
	whereX := scanDirFor(t, dir, "ws", []byte("FOREIGN-X"))
	_, fp, _ := authRow(t, s, "f.txt")
	t.Logf("end state: f.txt=%q present=%v row=%q W2 at %q X at %q",
		got, ok, fp, where, whereX)
	if !ok || got != "W2" || where != "f.txt" {
		t.Fatalf("DEFECT: acknowledged W2 lost: f.txt=%q present=%v W2-bytes at %q",
			got, ok, where)
	}
	if whereX == "" {
		t.Fatalf("foreign capture destroyed — must be preserved")
	}
}

// CW5c — committed write, slot capture grabs a foreign object AND a racer
// displaces the committed object before the post-commit stat: no
// verifiable identity exists, so the op must NOT journal — the pending
// intent settles by observation into an honest record.
func TestCorrWriteCommitParkedUnverifiedIdentity(t *testing.T) {
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
	if _, _, err := s.WithWrite(ctx, "ws", "f.txt", "write",
		IfVersion{Mode: "any"}, sha("OLD"), authProbe(root, "f.txt"),
		authWriteFn(root, "f.txt", "OLD")); err != nil {
		t.Fatalf("write OLD: %v", err)
	}
	var id int64
	var flipped bool
	root.faultHook = func(tag string) {
		if tag != "write.preSlotDelete" {
			return
		}
		if err := os.WriteFile(dir+"/ws/.filesv-tmp-inj", []byte("FOREIGN-X"), 0o644); err != nil {
			panic(err)
		}
		slot := opStagePrefix + strconv.FormatInt(id, 10)
		if err := os.Rename(dir+"/ws/.filesv-tmp-inj", dir+"/ws/"+slot); err != nil {
			panic(err)
		}
		// And a racer replaces the committed object at the path before
		// the post-commit stat can observe it.
		if err := os.WriteFile(dir+"/ws/f.txt", []byte("RACER"), 0o644); err != nil {
			panic(err)
		}
		flipped = true
	}
	_, _, werr := s.WithWrite(ctx, "ws", "f.txt", "write",
		IfVersion{Mode: "any"}, sha("W2"), authProbe(root, "f.txt"),
		func(it intent) (FileInfo, bool, error) {
			id = it.id
			return root.atomicWrite("ws", "f.txt", []byte("W2"), false,
				it.dstFP, opStagePrefix+strconv.FormatInt(it.id, 10))
		})
	root.faultHook = nil
	if !flipped {
		t.Skip("hook never ran")
	}
	t.Logf("write err: %v", werr)
	v, fp, found := authRow(t, s, "f.txt")
	live := durFP(t, root, "ws", "f.txt")
	t.Logf("immediately after op: row=(%d,%q found=%v) live=%q", v, fp, found, live)
	if found && fp3(fp) == fp3(live) && !strings.HasPrefix(fp, "diverged:") {
		t.Fatalf("journaled the RACER's object as this op's commit: fp=%q live=%q", fp, live)
	}
	authSettle(t, s)
	authSettle(t, s)
	got, _ := authReadOpt(dir, "ws/f.txt")
	v2, fp2, _ := authRow(t, s, "f.txt")
	whereX := scanDirFor(t, dir, "ws", []byte("FOREIGN-X"))
	t.Logf("after settle: f.txt=%q row=(%d,%q) X at %q", got, v2, fp2, whereX)
	if whereX == "" {
		t.Fatal("foreign capture destroyed")
	}
}

// CW6 — 195 on real fs: a recorded object parked at parent depth 40
// under a dead namespace is restored (no depth bound).
func TestCorrDeepSweepRestores(t *testing.T) {
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
	deep := "ws"
	for i := 0; i < 40; i++ {
		deep += "/d" + strconv.Itoa(i)
	}
	if err := os.MkdirAll(dir+"/"+deep, 0o755); err != nil {
		t.Fatal(err)
	}
	rel := deep[3:] + "/hdeep.txt"
	if _, _, err := s.WithWrite(ctx, "ws", rel, "write",
		IfVersion{Mode: "any"}, sha("DEEP"), authProbe(root, rel),
		authWriteFn(root, rel, "DEEP")); err != nil {
		t.Fatalf("write: %v", err)
	}
	it := intent{owner: "dead-inst", scope: "ws", op: "write",
		path: deep[3:] + "/dead.txt", version: authMint(t, s),
		preFP: "0:0:0:0", at: time.Now().Add(-time.Hour)}
	it.id = insertIntent(t, s, it)
	parked := deep[3:] + "/" + opStagePrefix + strconv.FormatInt(it.id, 10)
	if err := os.Rename(dir+"/ws/"+rel, dir+"/ws/"+parked); err != nil {
		t.Fatal(err)
	}
	authExec(t, s, `DELETE FROM file_op WHERE id=$1`, it.id)
	authSettle(t, s)
	if got, ok := authReadOpt(dir, "ws/"+rel); !ok || got != "DEEP" {
		where := scanDirFor(t, dir, "ws", []byte("DEEP"))
		t.Fatalf("depth-40 parked recorded object not restored: %q present=%v at %q", got, ok, where)
	}
}
