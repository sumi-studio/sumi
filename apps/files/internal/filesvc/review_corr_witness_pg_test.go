package filesvc

// Independent-review adversarial witnesses for the correction candidate
// 92ccc9d9 (authority-independent-2). Focus: the kind-aware object
// identity introduced for finding 196 — recordedAtObject/recordedMatch/
// sameObjectAt match directories on the inode leg ALONE, and the
// persisted fingerprint carries no device leg. The sweep's traversal
// key (197) does use dev:ino, but the *authority* comparisons —
// including settleDelete's captured-object veto, which gates an actual
// unlink — do not consult DevIno even though both live FileInfos carry
// it.
//
// Cross-device evidence is necessarily SYNTHETIC: the fixture has no
// mount privileges (same bound as the author's own sweepIDView
// witnesses). The injection models exactly what a real stat would
// report for a foreign-device object whose inode number collides with
// a scope-local one: an ino leg that matches, a dev:ino that differs.
// Everything else — PG rows, ext4 objects, the reconcileOne pass — is
// real.
//
// The stale-row and ambiguous-row witnesses need no injection: a ghost
// version row claiming a live object's inode is an ordinary post-reuse
// DB state (ext4 recycles freed inodes; external deletes leave rows).

import (
	"context"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// R-196-root-1 — DESTRUCTIVE: settleDelete's veto classifies the parked
// recorded dir as a "surplus link" because its recorded home currently
// holds a DIFFERENT-object dir on another device that shares the inode
// number. sameObjectAt ignores DevIno, so the impostor satisfies the
// "still at its recorded home" check, and the recorded object is
// unlinked — acknowledged content destroyed by a merely similar object.
//
// Production chain: a write intent was declared on a path holding dir D
// (dstFP=preFP records D — declare performs no kind check); D was
// displaced and late-deposited into the intent's namespace (the
// delayed-drain idiom); the write committed once the path cleared; the
// reconciler's apply journaled it. D is still recorded at its own home,
// which now holds a cross-device dir with a colliding inode number.
func TestReviewCorrPGCrossDevDirVetoDeletesRecorded(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	if err := os.MkdirAll(dir+"/ws", 0o755); err != nil {
		t.Fatal(err)
	}
	// D recorded at its real home through the production mkdir op.
	if _, _, err := s.WithWrite(ctx, "ws", "homeD", "mkdir",
		IfVersion{Mode: "any"}, "dir", authProbe(root, "homeD"),
		func(it intent) (FileInfo, bool, error) {
			return root.mkdir("ws", "homeD")
		}); err != nil {
		t.Fatalf("mkdir homeD: %v", err)
	}
	_, dfp, found := authRow(t, s, "homeD")
	if !found || dfp == "" {
		t.Fatalf("mkdir produced no usable row: fp=%q found=%v", dfp, found)
	}
	dIno, _, _, _ := fpParts(dfp)

	// The write intent whose declared displaced object is D.
	it := intent{owner: "dead-inst", scope: "ws", op: "write", path: "wp",
		version: authMint(t, s), preFP: "0:0:0:0", dstFP: dfp,
		expectSHA: sha("W"), at: time.Now().Add(-time.Hour)}
	it.id = insertIntent(t, s, it)
	parked := opStagePrefix + strconv.FormatInt(it.id, 10) + "-p-dd"
	// Delayed-deposit idiom: D lands in the intent's namespace.
	if err := os.Rename(dir+"/ws/homeD", dir+"/ws/"+parked); err != nil {
		t.Fatal(err)
	}
	// The declared write's bytes reached wp (lost-reply idiom) so the
	// pass's apply journals the commit and deletes the intent row.
	if err := os.WriteFile(dir+"/ws/wp", []byte("W"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A different-device directory now occupies homeD — on ITS
	// filesystem its inode number happens to equal D's inode on this
	// one. Stat reports the collision faithfully: ino leg equal,
	// dev:ino different.
	if err := os.Mkdir(dir+"/ws/homeD", 0o755); err != nil {
		t.Fatal(err)
	}
	vf := authPinned(root, func(v ReconView) ReconView {
		return sweepIDView{ReconView: v,
			fp: map[string]string{"homeD": dIno + ":48:48:48"},
			id: map[string]string{"homeD": "7777:" + dIno}}
	})
	view, err := vf(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	s.reconcileOne(ctx, it, view, false)

	// Required: D (recorded content acknowledged at homeD) is not
	// destroyed by a merely similar object at its home. Wherever it
	// ends up — restored or still parked — its inode must survive.
	alive := scanDirForInode(dir, "ws", dIno)
	if alive == "" {
		_, fp, f := authRow(t, s, "homeD")
		t.Fatalf("recorded directory (ino %s) DESTROYED by the captured-object "+
			"veto: its row (homeD fp=%q found=%v) dangles over a cross-device "+
			"impostor — sameObjectAt treated a same-ino different-dev dir as "+
			"the same object and the 'surplus link' fallthrough unlinked it",
			dIno, fp, f)
	}
	t.Logf("recorded dir preserved at %s", alive)
}

// R-196-root-2 — STRANDING (same false-positive, non-destructive
// surface): the orphan sweep's "still present at its recorded home —
// surplus link" branch leaves the recorded dir parked forever while a
// cross-device impostor occupies the home.
func TestReviewCorrPGCrossDevDirSweepStrandsRecorded(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	if err := os.MkdirAll(dir+"/ws", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.WithWrite(ctx, "ws", "sweepHome", "mkdir",
		IfVersion{Mode: "any"}, "dir", authProbe(root, "sweepHome"),
		func(it intent) (FileInfo, bool, error) {
			return root.mkdir("ws", "sweepHome")
		}); err != nil {
		t.Fatalf("mkdir sweepHome: %v", err)
	}
	_, dfp, _ := authRow(t, s, "sweepHome")
	dIno, _, _, _ := fpParts(dfp)
	parked := opStagePrefix + "424242-p-sd" // intent id never existed
	if err := os.Rename(dir+"/ws/sweepHome", dir+"/ws/"+parked); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir+"/ws/sweepHome", 0o755); err != nil {
		t.Fatal(err)
	}
	s.SetReconcileView(authPinned(root, func(v ReconView) ReconView {
		return sweepIDView{ReconView: v,
			fp: map[string]string{"sweepHome": dIno + ":48:48:48"},
			id: map[string]string{"sweepHome": "7777:" + dIno}}
	}))
	authSettle(t, s)
	authSettle(t, s)
	// Required: the recorded dir is restored home (the impostor is
	// parked aside) — or at minimum not mistaken for already-home.
	if st, serr := root.lstat("ws", "sweepHome"); serr != nil {
		t.Fatalf("sweepHome unverifiable: %v", serr)
	} else if ino, _, _, _ := fpParts(st.Fingerprint); ino != dIno {
		t.Fatalf("recorded dir stranded by false surplus classification: "+
			"sweepHome holds impostor ino %s, recorded ino %s still parked",
			ino, dIno)
	}
}

// R-196-root-3 — AMBIGUOUS HOME (real fs, no injection): a ghost row
// whose inode leg collides with a live parked dir's inode — the state
// inode reuse leaves behind — wins recordedAtObject's ORDER BY path
// LIMIT 1 over the dir's own row. The acknowledged dir is then moved
// to the stale home, leaving its true row dangling.
func TestReviewCorrPGStaleInodeRowMisroutesDir(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	if err := os.MkdirAll(dir+"/ws", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.WithWrite(ctx, "ws", "zHome", "mkdir",
		IfVersion{Mode: "any"}, "dir", authProbe(root, "zHome"),
		func(it intent) (FileInfo, bool, error) {
			return root.mkdir("ws", "zHome")
		}); err != nil {
		t.Fatalf("mkdir zHome: %v", err)
	}
	_, dfp, _ := authRow(t, s, "zHome")
	dIno, _, _, _ := fpParts(dfp)
	parked := opStagePrefix + "434343-p-mr"
	if err := os.Rename(dir+"/ws/zHome", dir+"/ws/"+parked); err != nil {
		t.Fatal(err)
	}
	// Ghost row: a path whose recorded object is gone. Its ino leg
	// equals the parked dir's live inode — exactly the state a recycled
	// inode leaves. No filesystem injection needed.
	authExec(t, s,
		`INSERT INTO file_version (scope, path, version, fp, content_sha)
		 VALUES ('ws','aGhost',$1,$2,'')`, authMint(t, s), dIno+":9:9:9")
	s.SetReconcileView(authPinned(root, nil))
	authSettle(t, s)
	authSettle(t, s)
	// Required: acknowledged dir lands at the row that recorded THIS
	// object — not at a stale row's path.
	if _, serr := root.lstat("ws", "aGhost"); serr == nil {
		if _, serr2 := root.lstat("ws", "zHome"); serr2 != nil {
			t.Fatalf("recorded dir misrouted to stale home aGhost (ino-leg " +
				"collision on a ghost row); its recorded home zHome is absent")
		}
	}
	if st, serr := root.lstat("ws", "zHome"); serr != nil {
		t.Fatalf("recorded dir not at its recorded home zHome: %v", serr)
	} else if ino, _, _, _ := fpParts(st.Fingerprint); ino != dIno {
		t.Fatalf("zHome holds ino %s, want recorded dir ino %s", ino, dIno)
	}
}

// Directory-wholesale restore when a member has its own recorded home
// elsewhere: the member's row is its own authority — the member must
// reach its recorded home (finding 200 / A's F-199), not merely ride
// inside the restored container to an unrecorded public path. The
// member's divergent row must not be laundered into a false-coherent
// record: coherence is honest only because the OBJECT moved to the
// row's home, never because the row was rewritten to wherever bytes
// landed.
//
// (Amended from reviewer B's original, which accepted the member
// riding the container; root's requirement is recovery to the
// member's own recorded home.)
func TestReviewCorrPGRecordedDirMemberRidesWithContainer(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	if err := os.MkdirAll(dir+"/ws", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.WithWrite(ctx, "ws", "recD", "mkdir",
		IfVersion{Mode: "any"}, "dir", authProbe(root, "recD"),
		func(it intent) (FileInfo, bool, error) {
			return root.mkdir("ws", "recD")
		}); err != nil {
		t.Fatalf("mkdir recD: %v", err)
	}
	if _, _, err := s.WithWrite(ctx, "ws", "recD/f.txt", "write",
		IfVersion{Mode: "any"}, sha("FMEM"), authProbe(root, "recD/f.txt"),
		authWriteFn(root, "recD/f.txt", "FMEM")); err != nil {
		t.Fatalf("write recD/f.txt: %v", err)
	}
	// The member's row now claims a different home — the state a rename
	// that never relocated the row leaves.
	authExec(t, s,
		`UPDATE file_version SET path='otherHome/f.txt'
		 WHERE scope='ws' AND path='recD/f.txt'`)
	parked := opStagePrefix + "454545-p-rc"
	if err := os.Mkdir(dir+"/ws/"+parked, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(dir+"/ws/recD", dir+"/ws/"+parked+"/recD"); err != nil {
		t.Fatal(err)
	}
	s.SetReconcileView(authPinned(root, nil))
	authSettle(t, s)
	authSettle(t, s)
	// The container must come home whole, AND the member must reach its
	// own recorded home — the row at otherHome/f.txt claims it, so that
	// is where acknowledged content belongs.
	st, serr := root.lstat("ws", "recD")
	if serr != nil || st.Kind != "dir" {
		t.Fatalf("recorded dir not restored to recD: %v", serr)
	}
	if got, ok := authReadOpt(dir, "ws/otherHome/f.txt"); !ok || got != "FMEM" {
		where := scanDirFor(t, dir, "ws", []byte("FMEM"))
		t.Fatalf("member not recovered to its own recorded home: "+
			"otherHome/f.txt=%q present=%v, FMEM bytes at %q", got, ok, where)
	}
	// The row is now legitimately coherent — the object moved to the
	// row's recorded home. What must never happen is the row being
	// REWRITTEN to wherever bytes landed: if the member had instead
	// stayed at recD/f.txt, the row still pointing at otherHome would be
	// honest divergence; a row rewritten to recD/f.txt would be false
	// coherence. Assert the row still records the original member
	// fingerprint at its recorded home.
	if _, fp, found := authRow(t, s, "otherHome/f.txt"); !found {
		t.Fatal("member row vanished")
	} else {
		live, lerr := root.lstat("ws", "otherHome/f.txt")
		if lerr != nil || fp3(live.Fingerprint) != fp3(fp) {
			t.Fatalf("member row not coherent with the live object: row fp=%q live=%v err=%v",
				fp, live.Fingerprint, lerr)
		}
	}
}

// Finite interference + fresh ops after recovery: an orphaned recorded
// object is restored by the sweep and a normal write then proceeds on
// the same path.
func TestReviewCorrPGFreshWriteAfterOrphanRecovery(t *testing.T) {
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
	if _, _, err := s.WithWrite(ctx, "ws", "fr.txt", "write",
		IfVersion{Mode: "any"}, sha("OLD"), authProbe(root, "fr.txt"),
		authWriteFn(root, "fr.txt", "OLD")); err != nil {
		t.Fatalf("write fr.txt: %v", err)
	}
	parked := opStagePrefix + "464646-p-fw"
	if err := os.Rename(dir+"/ws/fr.txt", dir+"/ws/"+parked); err != nil {
		t.Fatal(err)
	}
	authSettle(t, s)
	if got, ok := authReadOpt(dir, "ws/fr.txt"); !ok || got != "OLD" {
		t.Fatalf("orphaned recorded object not restored: fr.txt=%q present=%v", got, ok)
	}
	if _, _, err := s.WithWrite(ctx, "ws", "fr.txt", "write",
		IfVersion{Mode: "any"}, sha("NEW"), authProbe(root, "fr.txt"),
		authWriteFn(root, "fr.txt", "NEW")); err != nil {
		t.Fatalf("fresh write after recovery: %v", err)
	}
	if got, _ := authReadOpt(dir, "ws/fr.txt"); got != "NEW" {
		t.Fatalf("fr.txt = %q after fresh write", got)
	}
}

// scanDirForInode walks the scope tree looking for the live object
// holding inode ino — used to assert survival regardless of which name
// currently holds it.
func scanDirForInode(root, scope, ino string) string {
	var found string
	var walk func(d string)
	walk = func(d string) {
		if found != "" {
			return
		}
		ents, err := os.ReadDir(d)
		if err != nil {
			return
		}
		for _, en := range ents {
			if found != "" {
				return
			}
			p := d + "/" + en.Name()
			fi, err := os.Stat(p)
			if err != nil {
				continue
			}
			st, ok := fi.Sys().(*syscall.Stat_t)
			if !ok {
				continue
			}
			if strconv.FormatUint(st.Ino, 10) == ino {
				found = p
				return
			}
			if fi.IsDir() {
				walk(p)
			}
		}
	}
	walk(root + "/" + scope)
	return found
}
