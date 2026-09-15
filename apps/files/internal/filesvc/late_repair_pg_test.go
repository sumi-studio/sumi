package filesvc

// Late-settlement repair witnesses for independent-review findings
// 186/187 (candidate head 7657181c, repair branch
// codex/alpha-fabric-cloud-20260914). Real Store, real PG, real ext4
// syscalls through the production pass-pinned view. Composed states use
// the authored tests' own idioms and are labelled where they occur:
// dead-owner intents via insertIntent, parked objects via
// MoveStaged/os.Rename standing in for a retired pass's delayed drain
// or swap, and a gate in the evidence->tx window for the relocate race.
// Assertions are written against the REQUIRED outcome — recovery to the
// recorded home, then a successful fresh operation.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// lateRepairView wraps the pass-pinned production view with a hook that
// runs AFTER the real Hash returns — the gate sits between the pass's
// fs evidence and the fenced DB tx that consumes it, the exact window a
// same-owner writer's commit can land in.
type lateRepairView struct {
	ReconView
	afterHash func(scope, path string)
}

func (v lateRepairView) Hash(scope, path string) (string, error) {
	h, err := v.ReconView.Hash(scope, path)
	if v.afterHash != nil {
		v.afterHash(scope, path)
	}
	return h, err
}

// 186/W1 — under the owned-names protocol the displaced object goes
// straight to the journaled private name (stage-then-exchange); it can
// never sit on a public path. A racer's object landing at the vacated
// source name mid-rename is an ordinary external occupant — recovery
// never moves it — while the displaced D is proven by bound identity and
// discarded only when the committed op was authorized to displace it.
//
// The racer is an executor-side rename at the real fault point — the
// same position a delayed FUSE syscall or an out-of-band move occupies.
// Required: the commit is journaled, the journaled slot keeps its
// enumerator, and a fresh write succeeds after finite interference.
func TestLateRepairRenameCommitParkedRecordedObject(t *testing.T) {
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
	// Acknowledged objects: S at old.txt (rename source), D at new.txt
	// (declared displaced destination), R at rec.txt (the recorded
	// object a racer lands at the vacated source name).
	for _, w := range [][2]string{
		{"old.txt", "S"}, {"new.txt", "D"}, {"rec.txt", "R"},
	} {
		if _, _, err := s.WithWrite(ctx, "ws", w[0], "write",
			IfVersion{Mode: "any"}, sha(w[1]), authProbe(root, w[0]),
			authWriteFn(root, w[0], w[1])); err != nil {
			t.Fatalf("write %s: %v", w[0], err)
		}
	}
	fpR := durFP(t, root, "ws", "rec.txt")

	root.faultHook = func(tag string) {
		if tag != "rename.postVerify" {
			return
		}
		// Between the source capture and the exchange, a racer lands
		// the RECORDED rec.txt object at the vacated source name.
		if err := os.Rename(dir+"/ws/rec.txt", dir+"/ws/old.txt"); err != nil {
			panic(err)
		}
	}
	_, _, rerr := s.Rename(ctx, "ws", "old.txt", "new.txt",
		IfVersion{Mode: "any"}, authProbe(root, "new.txt"), authProbe(root, "old.txt"),
		func(it intent) (FileInfo, bool, error) {
			return root.rename("ws", "old.txt", "new.txt", false, it)
		})
	root.faultHook = nil
	if rerr != nil {
		t.Fatalf("clean rename = %v", rerr)
	}
	// The rename commit is journaled: new.txt's row records S's object.
	liveNew := durFP(t, root, "ws", "new.txt")
	if _, fp, found := authRow(t, s, "new.txt"); !found || fp3(fp) != fp3(liveNew) {
		t.Fatalf("row(new.txt) = %q found=%v, live %s — committed rename not journaled", fp, found, liveNew)
	}
	// R is an ordinary occupant of the vacated source name — it is
	// never moved by recovery and never confused with the displaced D.
	if got, ok := authReadOpt(dir, "ws/old.txt"); !ok || got != "R" {
		t.Fatalf("external occupant at source name disturbed: old.txt=%q present=%v", got, ok)
	}
	// D was authorized for displacement and is gone.
	if where := scanDirFor(t, dir, "ws", []byte("D")); where != "" {
		t.Fatalf("displaced object outlived its authorized discard: %q", where)
	}

	authSettle(t, s)

	// The external move stands — R stays at old.txt; its recorded row
	// remains at rec.txt, honestly diverged until the object is moved
	// back by an ordinary operation.
	if got, ok := authReadOpt(dir, "ws/old.txt"); !ok || got != "R" {
		where := scanDirFor(t, dir, "ws", []byte("R"))
		t.Fatalf("recorded object at old.txt disturbed by recovery: "+
			"old.txt=%q present=%v, R bytes at %q", got, ok, where)
	}
	if _, fp, found := authRow(t, s, "rec.txt"); !found || fp3(fp) != fp3(fpR) {
		t.Fatalf("row(rec.txt) = %q found=%v, want recorded %s", fp, found, fpR)
	}
	// And a fresh operation proceeds after finite interference.
	if _, _, err := s.WithWrite(ctx, "ws", "rec.txt", "write",
		IfVersion{Mode: "any"}, sha("R2"), authProbe(root, "rec.txt"),
		authWriteFn(root, "rec.txt", "R2")); err != nil {
		t.Fatalf("fresh write after recovery: %v", err)
	}
	if got, _ := authReadOpt(dir, "ws/rec.txt"); got != "R2" {
		t.Fatalf("rec.txt = %q after fresh write, want R2", got)
	}
}

// 186/F-1a: a recorded object parked inside a dead intent's namespace
// must be routed to its recorded home — which need not be the intent's
// path — even when a squatter occupies that home (displaced to an
// enumerable parked name, never destroyed).
func TestLateRepairParkedRecordedObjectOrphaned(t *testing.T) {
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
	if _, _, err := s.WithWrite(ctx, "ws", "r.txt", "write", IfVersion{Mode: "any"},
		sha("C-bytes"), authProbe(root, "r.txt"), authWriteFn(root, "r.txt", "C-bytes")); err != nil {
		t.Fatalf("write r.txt: %v", err)
	}
	if _, _, err := s.WithWrite(ctx, "ws", "h.txt", "write", IfVersion{Mode: "any"},
		sha("L-content"), authProbe(root, "h.txt"), authWriteFn(root, "h.txt", "L-content")); err != nil {
		t.Fatalf("write h.txt: %v", err)
	}

	// Dead write intent on r.txt: the path already holds its expected
	// bytes, so the pass rolls it forward and deletes the intent row.
	c := intent{owner: "dead-inst", scope: "ws", op: "write", path: "r.txt",
		version: authMint(t, s), preFP: "0:0:0:0", dstFP: "9:9:9:9",
		expectSHA: sha("C-bytes"), at: time.Now().Add(-time.Hour)}
	c.id = insertIntent(t, s, c)
	parked := opStagePrefix + strconv.FormatInt(c.id, 10) + "-p-aa01"
	// Composed: a retired pass's delayed drain parks h.txt's recorded
	// object into C's namespace (drainSealed idiom); a squatter occupies
	// h.txt.
	if err := os.Rename(dir+"/ws/h.txt", dir+"/ws/"+parked); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/ws/h.txt", []byte("squatter"), 0o644); err != nil {
		t.Fatal(err)
	}

	s.Reconcile(ctx)
	authSettle(t, s)

	// The squatter's public home is never disturbed — recovery does not
	// evict a live occupant. The recorded object surfaces at a visible
	// recovered sibling instead of evicting it.
	if got, _ := authReadOpt(dir, "ws/h.txt"); got != "squatter" {
		t.Fatalf("public squatter evicted: h.txt=%q", got)
	}
	where := scanDirFor(t, dir, "ws", []byte("L-content"))
	if where == "" {
		_, fp, found := authRow(t, s, "h.txt")
		t.Fatalf("recorded content destroyed: h.txt row=%q found=%v "+
			"(intent rows left: %d)", fp, found, intentCount(t, s))
	}
	if strings.Contains(where, opStagePrefix) {
		t.Fatalf("recorded content still parked at a private name %q — must surface visibly", where)
	}
}

// 186/F-1b: same gap, opposite branch — the intent's path is EMPTY and
// holds no row, so the pre-repair restoreStaged fallback moved the
// foreign recorded object onto the intent's path. Required: the object
// returns to its recorded home, not the intent's name.
func TestLateRepairParkedRecordedObjectNotMisplaced(t *testing.T) {
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
	if _, _, err := s.WithWrite(ctx, "ws", "h.txt", "write", IfVersion{Mode: "any"},
		sha("L-content"), authProbe(root, "h.txt"), authWriteFn(root, "h.txt", "L-content")); err != nil {
		t.Fatalf("write h.txt: %v", err)
	}

	// Dead pending write intent on r.txt whose effect never landed:
	// r.txt is absent and carries no row.
	c := intent{owner: "dead-inst", scope: "ws", op: "write", path: "r.txt",
		version: authMint(t, s), preFP: "0:0:0:0", dstFP: "9:9:9:9",
		expectSHA: sha("C-bytes"), at: time.Now().Add(-time.Hour)}
	c.id = insertIntent(t, s, c)
	parked := opStagePrefix + strconv.FormatInt(c.id, 10) + "-p-bb02"
	if err := os.Rename(dir+"/ws/h.txt", dir+"/ws/"+parked); err != nil {
		t.Fatal(err)
	}

	s.Reconcile(ctx)
	authSettle(t, s)

	if got, _ := authReadOpt(dir, "ws/h.txt"); got != "L-content" {
		where := scanDirFor(t, dir, "ws", []byte("L-content"))
		_, fpH, _ := authRow(t, s, "h.txt")
		_, fpR, _ := authRow(t, s, "r.txt")
		t.Fatalf("recorded content misplaced, not restored home: "+
			"L-bytes at %q (want h.txt), row(h.txt)=%q, row(r.txt)=%q",
			where, fpH, fpR)
	}
	if got, ok := authReadOpt(dir, "ws/r.txt"); ok {
		t.Fatalf("r.txt = %q — foreign recorded content moved onto the intent's path", got)
	}
}

// 186/W3: once an intent row is removed, its .filesv-op-<id>* namespace
// is dead-letter unless a pass discovers it another way. A delayed
// settlement effect (decided while the intent lived) can deposit a
// recorded object under the dead namespace at ANY later time — a single
// post-settlement sweep cannot bound that delay. Composed: the intent
// resolves and its row is deleted; the deposit lands after; the orphan
// sweep must still find it.
func TestLateRepairPostRemovalDepositRestored(t *testing.T) {
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
	for _, w := range [][2]string{{"a.txt", "A"}, {"c.txt", "C"}} {
		if _, _, err := s.WithWrite(ctx, "ws", w[0], "write",
			IfVersion{Mode: "any"}, sha(w[1]), authProbe(root, w[0]),
			authWriteFn(root, w[0], w[1])); err != nil {
			t.Fatalf("write %s: %v", w[0], err)
		}
	}
	fpA := durFP(t, root, "ws", "a.txt")
	// Dead pending write intent whose declared bytes already hold c.txt:
	// the pass applies it and deletes the intent row — the last
	// enumeration this namespace gets through the intent.
	cI := intent{owner: "dead-inst", scope: "ws", op: "write", path: "c.txt",
		version: authMint(t, s), preFP: "0:0:0:0", expectSHA: sha("C"),
		at: time.Now().Add(-time.Hour)}
	cI.id = insertIntent(t, s, cI)
	s.Reconcile(ctx)
	if n := intentCount(t, s); n != 0 {
		t.Fatalf("dead intent not settled: %d rows", n)
	}
	// The delayed effect lands now — after the row is gone: recorded
	// object A moves into the dead intent's parked namespace.
	late := opStagePrefix + strconv.FormatInt(cI.id, 10) + "-p-late"
	if err := os.Rename(dir+"/ws/a.txt", dir+"/ws/"+late); err != nil {
		t.Fatalf("late deposit: %v", err)
	}
	authSettle(t, s)
	if got, ok := authReadOpt(dir, "ws/a.txt"); !ok || got != "A" {
		where := scanDirFor(t, dir, "ws", []byte("A"))
		_, fp, found := authRow(t, s, "a.txt")
		t.Fatalf("acknowledged object stranded at dead namespace: a.txt=%q "+
			"(present=%v), A bytes at %q, row(a.txt)=%q found=%v",
			got, ok, where, fp, found)
	}
	if _, fp, found := authRow(t, s, "a.txt"); !found || fp3(fp) != fp3(fpA) {
		t.Fatalf("row(a.txt) = %q found=%v, want %s", fp, found, fpA)
	}
	// A fresh operation on the recovered path succeeds.
	if _, _, err := s.WithWrite(ctx, "ws", "a.txt", "write",
		IfVersion{Mode: "any"}, sha("A2"), authProbe(root, "a.txt"),
		authWriteFn(root, "a.txt", "A2")); err != nil {
		t.Fatalf("fresh write after recovery: %v", err)
	}
}

// 186: same recorded-home routing under a dead REMOVE intent — the
// parked foreign object's home is its recorded path, not the intent's.
func TestLateRepairParkedRecordedUnderRemoveIntent(t *testing.T) {
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
	for _, w := range [][2]string{{"h.txt", "L-content"}, {"victim.txt", "V"}} {
		if _, _, err := s.WithWrite(ctx, "ws", w[0], "write",
			IfVersion{Mode: "any"}, sha(w[1]), authProbe(root, w[0]),
			authWriteFn(root, w[0], w[1])); err != nil {
			t.Fatalf("write %s: %v", w[0], err)
		}
	}
	// Dead pending remove intent on victim.txt; a delayed drain parked
	// h.txt's recorded object under the intent's namespace.
	c := intent{owner: "dead-inst", scope: "ws", op: "remove", path: "victim.txt",
		version: authMint(t, s), preFP: "0:0:0:0", dstFP: "9:9:9:9",
		at: time.Now().Add(-time.Hour)}
	c.id = insertIntent(t, s, c)
	parked := opStagePrefix + strconv.FormatInt(c.id, 10) + "-p-cc03"
	if err := os.Rename(dir+"/ws/h.txt", dir+"/ws/"+parked); err != nil {
		t.Fatal(err)
	}

	authSettle(t, s)

	if got, _ := authReadOpt(dir, "ws/h.txt"); got != "L-content" {
		where := scanDirFor(t, dir, "ws", []byte("L-content"))
		t.Fatalf("recorded content parked under a remove intent not restored: "+
			"h.txt=%q, bytes at %q", got, where)
	}
}

// An orphan at a sealed (-q-) name holds recorded content while a
// squatter occupies the home. The occupant of a public name always
// wins — the recorded object surfaces at a visible sibling rather than
// evicting the squatter.
func TestLateRepairOrphanSealedRecordedRestored(t *testing.T) {
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
	if _, _, err := s.WithWrite(ctx, "ws", "h.txt", "write", IfVersion{Mode: "any"},
		sha("sealed-R"), authProbe(root, "h.txt"), authWriteFn(root, "h.txt", "sealed-R")); err != nil {
		t.Fatalf("write h.txt: %v", err)
	}
	// Compose: the recorded object sits at a dead intent's quarantine
	// name (a RemoveStagedVeto capture whose intent row is gone); a
	// squatter holds the recorded home.
	sealed := opStagePrefix + "7777" + "-q-aa01"
	if err := os.Rename(dir+"/ws/h.txt", dir+"/ws/"+sealed); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/ws/h.txt", []byte("squatter"), 0o644); err != nil {
		t.Fatal(err)
	}
	authSettle(t, s)
	// The squatter's public home is never disturbed.
	if got, _ := authReadOpt(dir, "ws/h.txt"); got != "squatter" {
		t.Fatalf("public squatter evicted: h.txt=%q", got)
	}
	if where := scanDirFor(t, dir, "ws", []byte("sealed-R")); where == "" ||
		strings.Contains(where, opStagePrefix) {
		t.Fatalf("sealed orphan not surfaced visibly: recorded bytes at %q", where)
	}
	if durExists(t, dir, "ws/"+sealed) {
		t.Fatal("sealed name still occupied after the settle")
	}
}

// Unknown unrecorded bytes at a dead namespace are preserved and
// surfaced at a visible recovered name — recovery never collects
// foreign objects it cannot attribute, and never leaves them hidden.
func TestLateRepairUnrecordedOrphanPreserved(t *testing.T) {
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
	if _, _, err := s.WithWrite(ctx, "ws", "k.txt", "write", IfVersion{Mode: "any"},
		sha("K"), authProbe(root, "k.txt"), authWriteFn(root, "k.txt", "K")); err != nil {
		t.Fatalf("write k.txt: %v", err)
	}
	garbage := opStagePrefix + "4242-p-gg07"
	if err := os.WriteFile(dir+"/ws/"+garbage, []byte("unknown-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	authSettle(t, s)
	authSettle(t, s)
	if where := scanDirFor(t, dir, "ws", []byte("unknown-bytes")); where == "" ||
		strings.Contains(where, opStagePrefix) {
		t.Fatalf("unrecorded orphan at %q — unknown bytes must be surfaced visibly, preserved", where)
	}
	if got, _ := authReadOpt(dir, "ws/k.txt"); got != "K" {
		t.Fatalf("k.txt = %q — sweep disturbed a healthy file", got)
	}
}

// 187/F-2a: relocateRows collects member rows and destination evidence
// BEFORE its owner-fenced tx, then — pre-repair — unconditionally
// DELETEs whatever row sits at the destination. A same-owner settler
// whose apply commits in the evidence->tx window loses its
// freshly-acknowledged row; the destination row is replaced by a
// stale-fingerprint member row.
//
// The concurrent writer here is the REAL settler path — declare +
// atomicWrite + apply on production methods — run inside the gate, not
// composed SQL outputs.
func TestLateRepairRelocateKeepsCommittedRow(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	if _, _, err := s.WithWrite(ctx, "ws", "A", "write", IfVersion{Mode: "any"},
		sha("member-M"), authProbe(root, "A"), authWriteFn(root, "A", "member-M")); err != nil {
		t.Fatalf("write member: %v", err)
	}
	memberInfo, err := root.stat("ws", "A")
	if err != nil {
		t.Fatal(err)
	}

	// Dead rename intent A→B, tombstoned past the hot window so fresh
	// ops at B are allowed again. Its fs effect lands late: the member
	// object is now observed at B.
	r := intent{owner: "dead-inst", scope: "ws", op: "rename", path: "A", toPath: "B",
		version: authMint(t, s), preFP: memberInfo.Fingerprint, dstFP: "7:7:7:7",
		expectSHA: sha("member-M"), srcKind: "file", at: time.Now().Add(-time.Hour)}
	r.id = insertIntent(t, s, r)
	authExec(t, s, `UPDATE file_op SET resolved_at = now() - interval '1 minute' WHERE id=$1`, r.id)
	if err := os.Rename(dir+"/ws/A", dir+"/ws/B"); err != nil {
		t.Fatal(err)
	}

	// Gate AFTER relocate's evidence read of B, before its fenced tx.
	g := newAuthGate()
	var wVersion int64
	s.SetReconcileView(authPinned(root, func(v ReconView) ReconView {
		return lateRepairView{ReconView: v, afterHash: func(_, p string) {
			if p != "B" {
				return
			}
			g.hit()
			<-g.release
			// In the window between the pass's evidence and its tx: a
			// fresh write at B completes declare + fs + apply (the
			// timed-out settler shape — runFs goroutines never hold the
			// scope mutex the pass holds).
			wit, derr := s.declare(ctx, "ws", "write", "B", "", IfVersion{Mode: "any"},
				sha("new-W"), authProbe(root, "B"), authProbe(root, "B"))
			if derr != nil {
				t.Errorf("W declare: %v", derr)
				return
			}
			defer s.inflight.Delete(wit.id) // raw declare — clean the mark
			winfo, wcommitted, werr := authWriteFn(root, "B", "new-W")(wit)
			if werr != nil || !wcommitted {
				t.Errorf("W fs: committed=%v err=%v", wcommitted, werr)
				return
			}
			if aerr := s.apply(ctx, wit, winfo, wit.expectSHA, false); aerr != nil {
				t.Errorf("W apply: %v", aerr)
				return
			}
			wVersion = wit.version
		}}
	}))
	s.lastTombScan.Store(0)
	done := make(chan struct{})
	go func() { defer close(done); s.Reconcile(ctx) }()
	g.await(t, "relocate observed member at B")
	close(g.release)
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("reconcile pass did not finish")
	}

	liveB, lerr := root.stat("ws", "B")
	if lerr != nil {
		t.Fatalf("B absent after pass: %v", lerr)
	}
	vB, fpB, foundB := authRow(t, s, "B")
	if !foundB || fp3(fpB) != fp3(liveB.Fingerprint) || vB != wVersion {
		t.Fatalf("relocate erased/replaced the committed row: "+
			"row(B)=(%d,%q,found=%v), live=%q, committed write v%d",
			vB, fpB, foundB, liveB.Fingerprint, wVersion)
	}
	// The member object itself was consumed by the settler's verified
	// displacement (it matched the declared dstFP — an authorized
	// delete), so A's row now records absent content: truthful
	// external_change, not a silent rewrite. A fresh write proceeds.
	if _, _, err := s.WithWrite(ctx, "ws", "B", "write",
		IfVersion{Mode: "any"}, sha("fresh"), authProbe(root, "B"),
		authWriteFn(root, "B", "fresh")); err != nil {
		t.Fatalf("fresh write after recovery: %v", err)
	}
}

// 187/F-2b: the deeper consequence — the member object, parked inside
// the rename intent's own namespace by a delayed drain, previously
// matched the mis-moved row at B, got swapped back onto B, and evicted
// the acknowledged object into the parked namespace: a coherent-looking
// rollback of the actual write. Required: B keeps the acknowledged
// bytes and the member returns to A.
func TestLateRepairRelocateStaleRowDoesNotRevert(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	if _, _, err := s.WithWrite(ctx, "ws", "A", "write", IfVersion{Mode: "any"},
		sha("member-M"), authProbe(root, "A"), authWriteFn(root, "A", "member-M")); err != nil {
		t.Fatalf("write member: %v", err)
	}
	memberInfo, err := root.stat("ws", "A")
	if err != nil {
		t.Fatal(err)
	}
	r := intent{owner: "dead-inst", scope: "ws", op: "rename", path: "A", toPath: "B",
		version: authMint(t, s), preFP: memberInfo.Fingerprint, dstFP: "7:7:7:7",
		expectSHA: sha("member-M"), srcKind: "file", at: time.Now().Add(-time.Hour)}
	r.id = insertIntent(t, s, r)
	authExec(t, s, `UPDATE file_op SET resolved_at = now() - interval '1 minute' WHERE id=$1`, r.id)
	if err := os.Rename(dir+"/ws/A", dir+"/ws/B"); err != nil {
		t.Fatal(err)
	}
	parked := opStagePrefix + strconv.FormatInt(r.id, 10) + "-p-zz02"

	g := newAuthGate()
	s.SetReconcileView(authPinned(root, func(v ReconView) ReconView {
		return lateRepairView{ReconView: v, afterHash: func(_, p string) {
			if p != "B" {
				return
			}
			g.hit()
			<-g.release
			// Composed delayed drain: park B's member object into R's
			// namespace (the drainSealed idiom), then a real fresh write
			// commits fs + row at the now-empty B.
			pv, perr := root.pin(false)
			if perr != nil {
				t.Errorf("pin: %v", perr)
				return
			}
			if merr := pv.MoveStaged("ws", "B", parked); merr != nil {
				t.Errorf("delayed park move: %v", merr)
			}
			pv.Close()
			wit, derr := s.declare(ctx, "ws", "write", "B", "", IfVersion{Mode: "any"},
				sha("new-W"), authProbe(root, "B"), authProbe(root, "B"))
			if derr != nil {
				t.Errorf("W declare: %v", derr)
				return
			}
			defer s.inflight.Delete(wit.id)
			winfo, wcommitted, werr := authWriteFn(root, "B", "new-W")(wit)
			if werr != nil || !wcommitted {
				t.Errorf("W fs: committed=%v err=%v", wcommitted, werr)
				return
			}
			if aerr := s.apply(ctx, wit, winfo, wit.expectSHA, false); aerr != nil {
				t.Errorf("W apply: %v", aerr)
			}
		}}
	}))
	s.lastTombScan.Store(0)
	done := make(chan struct{})
	go func() { defer close(done); s.Reconcile(ctx) }()
	g.await(t, "relocate observed member at B")
	close(g.release)
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("reconcile pass did not finish")
	}
	authSettle(t, s)

	if got, _ := authReadOpt(dir, "ws/B"); got != "new-W" {
		where := scanDirFor(t, dir, "ws", []byte("new-W"))
		vB, fpB, _ := authRow(t, s, "B")
		t.Fatalf("acknowledged write silently reverted: B=%q, "+
			"new-W bytes at %q, row(B)=(%d,%q)", got, where, vB, fpB)
	}
	// And the recorded member returns to its recorded home A.
	if got, ok := authReadOpt(dir, "ws/A"); !ok || got != "member-M" {
		where := scanDirFor(t, dir, "ws", []byte("member-M"))
		t.Fatalf("member not recovered home: A=%q present=%v, bytes at %q",
			got, ok, where)
	}
	// A fresh operation proceeds after finite interference.
	if _, _, err := s.WithWrite(ctx, "ws", "B", "write",
		IfVersion{Mode: "any"}, sha("fresh"), authProbe(root, "B"),
		authWriteFn(root, "B", "fresh")); err != nil {
		t.Fatalf("fresh write after recovery: %v", err)
	}
}

// authWaitRowLock blocks until some session is waiting on a lock — with
// only the two test transactions live, that is the relocate pass stalled
// on the row the holder tx owns. Deterministic within-transaction
// scheduling, no sleep guesses.
func authWaitRowLock(t *testing.T, s *Store) {
	t.Helper()
	authWait(t, "blocked row lock", func() bool {
		var n int
		if err := s.pool.QueryRow(context.Background(),
			`SELECT count(*) FROM pg_locks WHERE NOT granted`).Scan(&n); err != nil {
			return false
		}
		return n > 0
	})
}

// relocateRenameFixture builds the shared 187 within-tx scene through
// production paths: member D/m.txt acknowledged, then a dead tombstoned
// rename intent D→E whose fs effect is composed as landed (os.Rename —
// the same shape a delayed syscall produces), plus a stale destination
// row at E/m.txt recording an object that is not there. Returns the
// intent, the pinned production view, and the member's recorded
// version+fp.
func relocateRenameFixture(t *testing.T, dsn string) (*Store, *posixRoot, string, intent, int64, string) {
	t.Helper()
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	if _, _, err := s.WithWrite(ctx, "ws", "D/m.txt", "write", IfVersion{Mode: "any"},
		sha("member-M"), authProbe(root, "D/m.txt"), authWriteFn(root, "D/m.txt", "member-M")); err != nil {
		t.Fatalf("write member: %v", err)
	}
	srcVer, srcFP, found := authRow(t, s, "D/m.txt")
	if !found {
		t.Fatal("member row missing")
	}
	r := intent{owner: "dead-inst", scope: "ws", op: "rename", path: "D", toPath: "E",
		version: authMint(t, s), srcKind: "dir", at: time.Now().Add(-time.Hour)}
	r.id = insertIntent(t, s, r)
	authExec(t, s, `UPDATE file_op SET resolved_at = now() - interval '1 minute' WHERE id=$1`, r.id)
	if err := os.Rename(dir+"/ws/D", dir+"/ws/E"); err != nil {
		t.Fatal(err)
	}
	// The displaced destination's stale row: records an object that no
	// longer occupies E/m.txt (fp matches nothing live).
	authExec(t, s,
		`INSERT INTO file_version (scope, path, version, fp, content_sha)
		 VALUES ('ws','E/m.txt',$1,'1:2:3:4','stale-sha')`, authMint(t, s))
	return s, root, dir, r, srcVer, srcFP
}

func countRelocateEvents(t *testing.T, s *Store) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM file_event WHERE op='relocate'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// 187/correction-1: a skipped relocation must not mutate either row.
// The submitted repair staged the destination DELETE before the source
// row was locked; a source row that drifted while its FOR UPDATE waited
// made the move `continue` — but the staged delete still committed.
// Here a holder tx owns the member row's lock, lets the pass gather
// evidence and block on that lock, then commits a drift. Required: the
// move is skipped with BOTH rows untouched and no relocate event.
func TestLateRepairRelocateSkippedMoveKeepsRows(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s, root, dir, it, srcVer, srcFP := relocateRenameFixture(t, dsn)
	_ = root
	staleVer, staleFP, _ := authRow(t, s, "E/m.txt")

	hold := authHold(t, dsn)
	if _, err := hold.Exec(context.Background(),
		`SELECT version FROM file_version WHERE scope='ws' AND path='D/m.txt' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	vf := authPinned(root, nil)
	view, err := vf(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	toInfo, err := view.Stat("ws", "E")
	if err != nil {
		t.Fatal(err)
	}
	type res struct {
		n   int
		err error
	}
	done := make(chan res, 1)
	go func() {
		n, rerr := s.relocateRows(context.Background(), it, toInfo, view, true)
		done <- res{n, rerr}
	}()
	authWaitRowLock(t, s) // pass now waits on the member row's lock

	// The drift lands while the pass waits: a concurrent committed
	// change to the member record (composed as a direct row update —
	// the lock holder is the committer).
	if _, err := hold.Exec(context.Background(),
		`UPDATE file_version SET version=version+1, updated=now()
		 WHERE scope='ws' AND path='D/m.txt'`); err != nil {
		t.Fatal(err)
	}
	if err := hold.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("relocateRows: %v", r.err)
		}
		if r.n != 0 {
			t.Fatalf("relocated %d rows despite drifted source", r.n)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("relocateRows did not finish")
	}

	// The skipped move mutated nothing: the stale destination row
	// survives for re-judgment, the member row keeps only the drift
	// the holder committed, and no relocate event was published.
	if v, fp, found := authRow(t, s, "E/m.txt"); !found || v != staleVer || fp != staleFP {
		t.Fatalf("destination row mutated by skipped move: (%d,%q,found=%v), want (%d,%q)",
			v, fp, found, staleVer, staleFP)
	}
	if v, fp, found := authRow(t, s, "D/m.txt"); !found || v != srcVer+1 || fp != srcFP {
		t.Fatalf("source row = (%d,%q,found=%v), want drifted (%d,%q)",
			v, fp, found, srcVer+1, srcFP)
	}
	if n := countRelocateEvents(t, s); n != 0 {
		t.Fatalf("relocate events = %d, want 0 for a skipped move", n)
	}
	// Finite-interference progress: a fresh write on a clean path works.
	if _, _, err := s.WithWrite(context.Background(), "ws", "fresh.txt", "write",
		IfVersion{Mode: "any"}, sha("fresh"), authProbe(root, "fresh.txt"),
		authWriteFn(root, "fresh.txt", "fresh")); err != nil {
		t.Fatalf("fresh write after skipped relocate: %v", err)
	}
	_ = dir
}

// 187/correction-3: the within-transaction evidence-to-lock window. The
// submitted repair re-stat'd the destination BEFORE acquiring the row
// lock, so a commit landing during the lock wait was judged against
// stale bytes — a newly acknowledged row could be misclassified as the
// displaced stale record and deleted. Here a holder tx owns the
// destination row lock, lets the pass gather evidence and block on it,
// then commits a real new object + row (the settler shape). Required:
// the locked row is recognised as changed and nothing is mutated.
func TestLateRepairRelocateLockWaitCommitKeepsRow(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s, root, dir, it, srcVer, srcFP := relocateRenameFixture(t, dsn)
	staleVer, _, _ := authRow(t, s, "E/m.txt")

	hold := authHold(t, dsn)
	if _, err := hold.Exec(context.Background(),
		`SELECT version FROM file_version WHERE scope='ws' AND path='E/m.txt' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	vf := authPinned(root, nil)
	view, err := vf(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	toInfo, err := view.Stat("ws", "E")
	if err != nil {
		t.Fatal(err)
	}
	type res struct {
		n   int
		err error
	}
	done := make(chan res, 1)
	go func() {
		n, rerr := s.relocateRows(context.Background(), it, toInfo, view, true)
		done <- res{n, rerr}
	}()
	authWaitRowLock(t, s) // pass now waits on the destination row's lock

	// While the pass waits: the concurrent settler's commit — new
	// acknowledged bytes at E/m.txt plus their row, applied inside the
	// holder tx (the holder is the committer; composed, not a second
	// live writer).
	if err := os.WriteFile(dir+"/ws/E/m.txt", []byte("new-W"), 0o644); err != nil {
		t.Fatal(err)
	}
	newFP := durFP(t, root, "ws", "E/m.txt")
	if _, err := hold.Exec(context.Background(),
		`UPDATE file_version SET version=version+1, fp=$1, content_sha=$2, updated=now()
		 WHERE scope='ws' AND path='E/m.txt'`, newFP, sha("new-W")); err != nil {
		t.Fatal(err)
	}
	if err := hold.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("relocateRows: %v", r.err)
		}
		if r.n != 0 {
			t.Fatalf("relocated %d rows over a commit that landed in the lock wait", r.n)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("relocateRows did not finish")
	}

	// The row that committed during the lock wait is untouched — it is
	// a different committed decision, not the stale record the
	// decision-time evidence saw.
	if v, fp, found := authRow(t, s, "E/m.txt"); !found || v != staleVer+1 || fp != newFP {
		t.Fatalf("destination row = (%d,%q,found=%v), want committed (%d,%q)",
			v, fp, found, staleVer+1, newFP)
	}
	if got, ok := authReadOpt(dir, "ws/E/m.txt"); !ok || got != "new-W" {
		t.Fatalf("E/m.txt = %q present=%v — acknowledged bytes disturbed", got, ok)
	}
	if v, fp, found := authRow(t, s, "D/m.txt"); !found || v != srcVer || fp != srcFP {
		t.Fatalf("source row = (%d,%q,found=%v), want unchanged (%d,%q)",
			v, fp, found, srcVer, srcFP)
	}
	if n := countRelocateEvents(t, s); n != 0 {
		t.Fatalf("relocate events = %d, want 0 for a skipped move", n)
	}
	if _, _, err := s.WithWrite(context.Background(), "ws", "fresh.txt", "write",
		IfVersion{Mode: "any"}, sha("fresh"), authProbe(root, "fresh.txt"),
		authWriteFn(root, "fresh.txt", "fresh")); err != nil {
		t.Fatalf("fresh write after skipped relocate: %v", err)
	}
}

// ─── Finding 194: committed-but-parked identity ─────────────────────────
//
// The committed+errUndoParked fs paths used to return FileInfo{} and
// applyRetained journaled fp="": the version row could never identify
// the acknowledged object (recordedAt blind), and reads reported
// foreign bytes as clean. Required: a committed row journals only an
// identity verified bound to the object this op published — fp3 of the
// observed object must equal the triple captured before the publish —
// or the intent stays unresolved for the reconciler's hash/fp3-verified
// observation settle. Never fp="", never a late stat that could name a
// different writer's object.

// IW5 — review LW5: a committed write whose slot cleanup captured a
// foreign object (delayed deposit lands at the enumerable slot inside
// the commit window — composed via the write.preSlotDelete seam, the
// position a retired pass's delayed swap occupies). Required: the
// journaled row records the committed object's real fingerprint, the
// intent is retained as a tombstone, and the foreign capture survives.
func TestLateRepairCommitParkedJournalsIdentity(t *testing.T) {
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
	var slot string
	root.faultHook = func(tag string) {
		if tag != "write.preSlotDelete" {
			return
		}
		// A delayed swap (decided by a retired pass while the slot held
		// the displaced object) lands foreign bytes at the enumerable
		// slot between the exchange and the cleanup capture.
		if err := os.WriteFile(dir+"/ws/.filesv-tmp-inj", []byte("FOREIGN-X"), 0o644); err != nil {
			panic(err)
		}
		if err := os.Rename(dir+"/ws/.filesv-tmp-inj", dir+"/ws/"+slot); err != nil {
			panic(err)
		}
	}
	_, _, werr := s.WithWrite(ctx, "ws", "f.txt", "write",
		IfVersion{Mode: "any"}, sha("W2"), authProbe(root, "f.txt"),
		func(it intent) (FileInfo, bool, error) {
			id = it.id
			slot = it.stageBase()
			return root.atomicWrite("ws", "f.txt", []byte("W2"), false, it)
		})
	root.faultHook = nil
	if werr == nil || !errors.Is(werr, ErrExternalChange) {
		t.Fatalf("write with parked capture = %v, want external_change", werr)
	}
	if got, _ := authReadOpt(dir, "ws/f.txt"); got != "W2" {
		t.Fatalf("f.txt = %q, want committed W2", got)
	}
	live := durFP(t, root, "ws", "f.txt")
	_, fp, found := authRow(t, s, "f.txt")
	if !found {
		t.Fatal("no row for committed write")
	}
	if fp == "" || fp3(fp) != fp3(live) {
		t.Fatalf("committed+parked write journaled fp=%q (live object %s) — "+
			"the record must identify the acknowledged object", fp, live)
	}
	var resolved bool
	if err := s.pool.QueryRow(ctx,
		`SELECT resolved_at IS NOT NULL FROM file_op WHERE id=$1`, id).Scan(&resolved); err != nil {
		t.Fatal(err)
	}
	if !resolved {
		t.Fatal("committed+parked intent must be tombstoned, not pending")
	}
	if where := scanDirFor(t, dir, "ws", []byte("FOREIGN-X")); where == "" {
		t.Fatal("foreign capture destroyed — quarantine must park, never unlink")
	}
}

// IW5b — review LW5b: the retained tombstone's re-judgment must not
// exchange the foreign capture onto the public path and delete the
// acknowledged object. The same-sha undo branch may only swap when no
// version row claims the name at all.
func TestLateRepairCommitParkedForeignPreserved(t *testing.T) {
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
	var slot string
	root.faultHook = func(tag string) {
		if tag != "write.preSlotDelete" {
			return
		}
		if err := os.WriteFile(dir+"/ws/.filesv-tmp-inj", []byte("FOREIGN-X"), 0o644); err != nil {
			panic(err)
		}
		if err := os.Rename(dir+"/ws/.filesv-tmp-inj", dir+"/ws/"+slot); err != nil {
			panic(err)
		}
	}
	_, _, werr := s.WithWrite(ctx, "ws", "f.txt", "write",
		IfVersion{Mode: "any"}, sha("W2"), authProbe(root, "f.txt"),
		func(it intent) (FileInfo, bool, error) {
			slot = it.stageBase()
			return root.atomicWrite("ws", "f.txt", []byte("W2"), false, it)
		})
	root.faultHook = nil
	if werr == nil || !errors.Is(werr, ErrExternalChange) {
		t.Fatalf("write with parked capture = %v, want external_change", werr)
	}
	if got, _ := authReadOpt(dir, "ws/f.txt"); got != "W2" {
		t.Fatalf("f.txt = %q, want committed W2", got)
	}
	fpW := durFP(t, root, "ws", "f.txt")

	// Tombstone re-judgment — the ordinary hot scan.
	authSettle(t, s)

	if got, ok := authReadOpt(dir, "ws/f.txt"); !ok || got != "W2" {
		where := scanDirFor(t, dir, "ws", []byte("W2"))
		t.Fatalf("acknowledged W2 displaced: f.txt=%q present=%v, W2 bytes at %q — "+
			"the tombstone settle must not install the foreign capture", got, ok, where)
	}
	if v, fp, found := authRow(t, s, "f.txt"); !found || fp3(fp) != fp3(fpW) {
		t.Fatalf("row(f.txt) = (%d,%q,found=%v), want the acknowledged object's %q",
			v, fp, found, fpW)
	}
	if where := scanDirFor(t, dir, "ws", []byte("FOREIGN-X")); where == "" {
		t.Fatal("foreign capture deleted by the settle pass")
	}
	// Progress: a fresh write still succeeds after the parked residue.
	if _, _, err := s.WithWrite(ctx, "ws", "f.txt", "write",
		IfVersion{Mode: "any"}, sha("W3"), authProbe(root, "f.txt"),
		authWriteFn(root, "f.txt", "W3")); err != nil {
		t.Fatalf("fresh write after parked settle: %v", err)
	}
}

// IW5c — the rename committed+parked site: a foreign object reaching
// the staging slot between the post-exchange verify and the sealed
// capture (rename.preQuarantine) parks at the -q name. The committed
// rename must journal the moved object's real identity, not fp="".
func TestLateRepairRenameSealConflictJournalsIdentity(t *testing.T) {
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
	for _, w := range [][2]string{{"old.txt", "S"}, {"new.txt", "D"}} {
		if _, _, err := s.WithWrite(ctx, "ws", w[0], "write",
			IfVersion{Mode: "any"}, sha(w[1]), authProbe(root, w[0]),
			authWriteFn(root, w[0], w[1])); err != nil {
			t.Fatalf("write %s: %v", w[0], err)
		}
	}
	var id int64
	var slot string
	root.faultHook = func(tag string) {
		if tag != "rename.preSlotDelete" {
			return
		}
		// Foreign bytes claim the captured slot between the identity
		// verify and the sealed capture — replacing the declared-
		// displaced object, whose discard was authorized anyway.
		if err := os.WriteFile(dir+"/ws/.filesv-tmp-inj", []byte("FOREIGN-Q"), 0o644); err != nil {
			panic(err)
		}
		if err := os.Rename(dir+"/ws/.filesv-tmp-inj", dir+"/ws/"+slot); err != nil {
			panic(err)
		}
	}
	_, _, rerr := s.Rename(ctx, "ws", "old.txt", "new.txt",
		IfVersion{Mode: "any"}, authProbe(root, "new.txt"), authProbe(root, "old.txt"),
		func(it intent) (FileInfo, bool, error) {
			id = it.id
			slot = it.stageBase()
			return root.rename("ws", "old.txt", "new.txt", false, it)
		})
	root.faultHook = nil
	if rerr == nil || !errors.Is(rerr, ErrExternalChange) {
		t.Fatalf("rename with parked seal capture = %v, want external_change", rerr)
	}
	if got, _ := authReadOpt(dir, "ws/new.txt"); got != "S" {
		t.Fatalf("new.txt = %q, want committed moved object S", got)
	}
	live := durFP(t, root, "ws", "new.txt")
	v, fp, found := authRow(t, s, "new.txt")
	if !found || fp == "" || fp3(fp) != fp3(live) {
		t.Fatalf("committed+parked rename journaled (%d,%q,found=%v) — "+
			"must record the moved object's real identity %q", v, fp, found, live)
	}
	var resolved bool
	if err := s.pool.QueryRow(ctx,
		`SELECT resolved_at IS NOT NULL FROM file_op WHERE id=$1`, id).Scan(&resolved); err != nil {
		t.Fatal(err)
	}
	if !resolved {
		t.Fatal("committed+parked intent must be tombstoned")
	}
	authSettle(t, s)
	if got, ok := authReadOpt(dir, "ws/new.txt"); !ok || got != "S" {
		t.Fatalf("settle displaced the acknowledged rename: new.txt=%q", got)
	}
	if where := scanDirFor(t, dir, "ws", []byte("FOREIGN-Q")); where == "" {
		t.Fatal("foreign sealed-capture object destroyed")
	}
}

// IW5d — the unverifiable-identity case: the commit landed but a racer
// displaced the committed object before the post-commit stat, AND the
// slot holds a foreign deposit. The op can return no defensible
// identity — it must NOT journal fp="" and must NOT mislabel the
// racer's bytes as its own: the intent stays unresolved and the
// reconciler settles the commit by observation (diverged fingerprint,
// never a clean row over foreign bytes).
func TestLateRepairCommitParkedUnverifiedIdentity(t *testing.T) {
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
	var slot string
	root.faultHook = func(tag string) {
		if tag != "write.preSlotDelete" {
			return
		}
		if err := os.WriteFile(dir+"/ws/.filesv-tmp-inj", []byte("FOREIGN-X"), 0o644); err != nil {
			panic(err)
		}
		if err := os.Rename(dir+"/ws/.filesv-tmp-inj", dir+"/ws/"+slot); err != nil {
			panic(err)
		}
		// A racing writer's object claims the public name before the
		// post-commit observation — the committed W2 object is gone.
		if err := os.WriteFile(dir+"/ws/f.txt", []byte("RACER"), 0o644); err != nil {
			panic(err)
		}
	}
	_, _, werr := s.WithWrite(ctx, "ws", "f.txt", "write",
		IfVersion{Mode: "any"}, sha("W2"), authProbe(root, "f.txt"),
		func(it intent) (FileInfo, bool, error) {
			slot = it.stageBase()
			return root.atomicWrite("ws", "f.txt", []byte("W2"), false, it)
		})
	root.faultHook = nil
	if werr == nil || !errors.Is(werr, ErrExternalChange) {
		t.Fatalf("unverifiable commit = %v, want external_change", werr)
	}
	// No committed row may claim the acknowledged object without a
	// verifiable identity: the intent must not have journaled fp="".
	if _, fp, found := authRow(t, s, "f.txt"); found && fp == "" {
		t.Fatal("journaled fp=\"\" — an unverifiable commit must stay unresolved")
	}
	// The committed object was displaced before observation — the
	// reconciler settles the intent by disk verdict: the racer's
	// content is recorded as divergent, never clean.
	authSettle(t, s)
	v, fp, found := authRow(t, s, "f.txt")
	if !found {
		t.Fatal("settle produced no row for the committed write")
	}
	live := durFP(t, root, "ws", "f.txt")
	if fp3(fp) == fp3(live) && fp != "" {
		// Recording the racer's object under this op's version is only
		// honest if the row marks the divergence — a clean fp here
		// attributes foreign bytes to this version.
		t.Fatalf("row(f.txt) = (%d,%q) cleanly records the racer's object %q",
			v, fp, live)
	}
	if where := scanDirFor(t, dir, "ws", []byte("FOREIGN-X")); where == "" {
		t.Fatal("foreign capture destroyed")
	}
}

// ─── Findings 195/196: orphan-sweep discovery bounds ────────────────────
//
// settleOrphanDir used to stop at depth 32 while relPath accepts any
// depth — a staged namespace deeper than that was enumerable only while
// its intent row lived, and unreachable forever after (195). And an
// orphaned staged DIRECTORY was judged as a leaf by its own
// fingerprint: an auto-created container has no row, so recorded
// members inside it were never inspected (196). Both stranded recorded
// content with bytes preserved — the exact outcome class 186 filed.

// lateDeepOrphanSetup parks a recorded object under a dead intent's
// namespace `parents` components deep, then removes the intent row —
// the post-deletion state the sweep is the only channel for. Ported
// from reviewer B's schedule.
func lateDeepOrphanSetup(t *testing.T, s *Store, root *posixRoot, dir string, parents int) string {
	t.Helper()
	ctx := context.Background()
	if _, _, err := s.WithWrite(ctx, "ws", "hdeep.txt", "write", IfVersion{Mode: "any"},
		sha("DEEP"), authProbe(root, "hdeep.txt"), authWriteFn(root, "hdeep.txt", "DEEP")); err != nil {
		t.Fatalf("write hdeep.txt: %v", err)
	}
	var b strings.Builder
	for i := 1; i <= parents; i++ {
		if i > 1 {
			b.WriteByte('/')
		}
		b.WriteString("d")
		b.WriteString(strconv.Itoa(i))
	}
	deep := b.String()
	if err := os.MkdirAll(dir+"/ws/"+deep, 0o755); err != nil {
		t.Fatal(err)
	}
	it := intent{owner: "dead-inst", scope: "ws", op: "write",
		path: deep + "/r.txt", version: authMint(t, s), preFP: "0:0:0:0",
		dstFP: "9:9:9:9", expectSHA: sha("X"), at: time.Now().Add(-time.Hour)}
	it.id = insertIntent(t, s, it)
	parked := deep + "/" + opStagePrefix + strconv.FormatInt(it.id, 10) + "-p-dd"
	if err := os.Rename(dir+"/ws/hdeep.txt", dir+"/ws/"+parked); err != nil {
		t.Fatal(err)
	}
	// The intent row is gone before any pass sees the deposit.
	authExec(t, s, `DELETE FROM file_op WHERE id=$1`, it.id)
	return parked
}

// 195 control: a parked recorded object beneath a mid-depth parent
// converges — the sweep must reach staged names at ANY real depth.
func TestLateRepairOrphanDeepSweepRestores(t *testing.T) {
	for _, parents := range []int{32, 33, 48} {
		t.Run(strconv.Itoa(parents), func(t *testing.T) {
			dsn := pgDSN(t)
			resetTables(t, dsn)
			dir := t.TempDir()
			root, err := newRoot(dir)
			if err != nil {
				t.Fatal(err)
			}
			s := newPGStore(t, dsn, dir)
			s.SetReconcileView(authPinned(root, nil))
			lateDeepOrphanSetup(t, s, root, dir, parents)
			authSettle(t, s)
			if got, ok := authReadOpt(dir, "ws/hdeep.txt"); !ok || got != "DEEP" {
				where := scanDirFor(t, dir, "ws", []byte("DEEP"))
				t.Fatalf("recorded object parked at parent depth %d not restored: "+
					"hdeep.txt=%q present=%v, bytes at %q", parents, got, ok, where)
			}
			if _, fp, found := authRow(t, s, "hdeep.txt"); !found || fp == "" {
				t.Fatalf("row(hdeep.txt) = %q found=%v", fp, found)
			}
		})
	}
}

// scanDirForFile walks the scope tree looking for a directory entry
// named name — used to assert an unrecorded member survived inside a
// surfaced container regardless of which name holds it.
func scanDirForFile(t *testing.T, dir, scope, name string) string {
	t.Helper()
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
			if en.Name() == name {
				found = p
				return
			}
			if en.IsDir() {
				walk(p)
			}
		}
	}
	walk(filepath.Join(dir, scope))
	if found == "" {
		return ""
	}
	rel, err := filepath.Rel(filepath.Join(dir, scope), found)
	if err != nil {
		t.Fatal(err)
	}
	return rel
}

// 196 — a parked OBJECT that is itself a directory: the container is
// unrecorded (auto-created parents carry no version row), so its own
// fingerprint proves nothing about its contents. The sweep must descend
// and re-home each recorded member; unrecorded residue stays inside.
func TestLateRepairOrphanedDirRestoresRecordedMembers(t *testing.T) {
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
	// dirO is auto-created by the write (no version row); f.txt is
	// recorded; extra.bin is unrecorded foreign content inside.
	if _, _, err := s.WithWrite(ctx, "ws", "dirO/f.txt", "write", IfVersion{Mode: "any"},
		sha("MEMBER"), authProbe(root, "dirO/f.txt"), authWriteFn(root, "dirO/f.txt", "MEMBER")); err != nil {
		t.Fatalf("write dirO/f.txt: %v", err)
	}
	if _, _, found := authRow(t, s, "dirO"); found {
		t.Skip("dirO unexpectedly has a row — premise does not hold")
	}
	if err := os.WriteFile(dir+"/ws/dirO/extra.bin", []byte("EXTRA"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir+"/ws/dirO/sub", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.WithWrite(ctx, "ws", "dirO/sub/g.txt", "write", IfVersion{Mode: "any"},
		sha("NESTED"), authProbe(root, "dirO/sub/g.txt"), authWriteFn(root, "dirO/sub/g.txt", "NESTED")); err != nil {
		t.Fatalf("write dirO/sub/g.txt: %v", err)
	}
	it := intent{owner: "dead-inst", scope: "ws", op: "write", path: "dead.txt",
		version: authMint(t, s), preFP: "0:0:0:0", at: time.Now().Add(-time.Hour)}
	it.id = insertIntent(t, s, it)
	parked := opStagePrefix + strconv.FormatInt(it.id, 10) + "-p-dir9"
	// A delayed drain deposits the whole unrecorded dir under the dead
	// namespace — the drainSealed/SwapStaged idiom.
	if err := os.Rename(dir+"/ws/dirO", dir+"/ws/"+parked); err != nil {
		t.Fatal(err)
	}
	authExec(t, s, `DELETE FROM file_op WHERE id=$1`, it.id)
	authSettle(t, s)
	if got, ok := authReadOpt(dir, "ws/dirO/f.txt"); !ok || got != "MEMBER" {
		pgot, _ := authReadOpt(dir, "ws/"+parked+"/f.txt")
		_, fp, found := authRow(t, s, "dirO/f.txt")
		t.Fatalf("recorded member stranded inside unrecorded parked dir: "+
			"dirO/f.txt=%q present=%v, bytes inside parked dir=%q, row=%q found=%v",
			got, ok, pgot, fp, found)
	}
	if got, ok := authReadOpt(dir, "ws/dirO/sub/g.txt"); !ok || got != "NESTED" {
		t.Fatalf("nested recorded member not restored: dirO/sub/g.txt=%q present=%v", got, ok)
	}
	// Unrecorded residue rides the container to its visible surfaced
	// name — preserved, never installed at the recorded member's names.
	if got, ok := authReadOpt(dir, "ws/"+parked+"/extra.bin"); ok && got == "EXTRA" {
		t.Fatal("container left hidden at its private name")
	}
	if _, ok := authReadOpt(dir, "ws/dirO/extra.bin"); ok {
		t.Fatal("unrecorded member was installed at a public recorded name")
	}
	if where := scanDirForFile(t, dir, "ws", "extra.bin"); where == "" ||
		strings.Contains(where, opStagePrefix) {
		t.Fatalf("unrecorded member destroyed or hidden: extra.bin at %q", where)
	}
}

// mkdir identity: the commit observation must be bound to the created
// object, not the path. A racer that replaces the fresh dir between
// creation and observation must not have its object journaled as this
// op's acknowledgement — the fd of the created dir is the identity.
func TestLateRepairMkdirJournalsBoundIdentity(t *testing.T) {
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
	root.faultHook = func(tag string) {
		if tag != "mkdir.postCreate" {
			return
		}
		// A racer removes the just-created dir and installs a different
		// one at the name — inside the commit→observe window.
		if err := os.Remove(dir + "/ws/mkd"); err != nil {
			panic(err)
		}
		if err := os.Mkdir(dir+"/ws/mkd", 0o755); err != nil {
			panic(err)
		}
	}
	_, _, err = s.WithWrite(ctx, "ws", "mkd", "mkdir",
		IfVersion{Mode: "any"}, "dir", authProbe(root, "mkd"),
		func(it intent) (FileInfo, bool, error) {
			return root.mkdir("ws", "mkd")
		})
	root.faultHook = nil
	if err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	live := durFP(t, root, "ws", "mkd")
	_, fp, found := authRow(t, s, "mkd")
	if !found || fp == "" {
		t.Fatalf("committed mkdir produced row fp=%q found=%v", fp, found)
	}
	if fp3(fp) == fp3(live) {
		t.Fatalf("mkdir journaled the RACER's dir (fp=%q) — the record must "+
			"name the object this op committed, divergent or not", fp)
	}
}

// 196 — a parked directory that is itself RECORDED: judge the member's
// own row before deciding to descend. An empty recorded dir has no leaf
// files to find, and a nonempty one must be restored whole — not have
// its children moved out into a fresh same-named dir while the recorded
// object stays parked.
func TestLateRepairOrphanedDirRestoresRecordedDir(t *testing.T) {
	for _, nonempty := range []bool{false, true} {
		name := "empty"
		if nonempty {
			name = "nonempty"
		}
		t.Run(name, func(t *testing.T) {
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
			// Record a real directory through the production mkdir op.
			if _, _, err := s.WithWrite(ctx, "ws", "recD", "mkdir",
				IfVersion{Mode: "any"}, "dir", authProbe(root, "recD"),
				func(it intent) (FileInfo, bool, error) {
					return root.mkdir("ws", "recD")
				}); err != nil {
				t.Fatalf("mkdir recD: %v", err)
			}
			_, recDFP, found := authRow(t, s, "recD")
			if !found || recDFP == "" {
				t.Fatalf("mkdir produced no usable row: fp=%q found=%v", recDFP, found)
			}
			if nonempty {
				if _, _, err := s.WithWrite(ctx, "ws", "recD/inner.txt", "write",
					IfVersion{Mode: "any"}, sha("IN"), authProbe(root, "recD/inner.txt"),
					authWriteFn(root, "recD/inner.txt", "IN")); err != nil {
					t.Fatalf("write recD/inner.txt: %v", err)
				}
			}
			// A delayed drain deposits the recorded dir inside an
			// unrecorded container under a dead intent's namespace.
			it := intent{owner: "dead-inst", scope: "ws", op: "write", path: "dead.txt",
				version: authMint(t, s), preFP: "0:0:0:0", at: time.Now().Add(-time.Hour)}
			it.id = insertIntent(t, s, it)
			parked := opStagePrefix + strconv.FormatInt(it.id, 10) + "-p-rc"
			if err := os.Mkdir(dir+"/ws/"+parked, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(dir+"/ws/recD", dir+"/ws/"+parked+"/recD"); err != nil {
				t.Fatal(err)
			}
			authExec(t, s, `DELETE FROM file_op WHERE id=$1`, it.id)
			authSettle(t, s)
			// The recorded dir object itself must sit at its recorded
			// home — the same INODE the row acknowledges (fp3's size/mtime
			// legs churn with member writes, so compare object identity).
			live := durFP(t, root, "ws", "recD")
			liveIno, _, _, lok := fpParts(live)
			rowIno, _, _, rok := fpParts(recDFP)
			if !lok || !rok || liveIno != rowIno {
				t.Fatalf("recorded dir not restored to its own home: "+
					"live fp=%q row fp=%q (inodes %q vs %q)",
					live, recDFP, liveIno, rowIno)
			}
			if _, err := os.Stat(dir + "/ws/" + parked + "/recD"); err == nil {
				t.Fatal("recorded dir still parked inside container")
			}
			if nonempty {
				if got, ok := authReadOpt(dir, "ws/recD/inner.txt"); !ok || got != "IN" {
					t.Fatalf("member of restored dir missing: %q present=%v", got, ok)
				}
			}
		})
	}
}

// sweepIDView injects a fabricated traversal identity (and fingerprint)
// at chosen paths — the only way to exercise same-ino-different-dev or
// alias behaviour on a fixture without mount privileges.
type sweepIDView struct {
	ReconView
	fp map[string]string
	id map[string]string // "-" clears the real dev:ino
}

func (v sweepIDView) Stat(scope, path string) (FileInfo, error) {
	st, err := v.ReconView.Stat(scope, path)
	if err != nil {
		return st, err
	}
	if f, ok := v.fp[path]; ok {
		st.Fingerprint = f
	}
	if id, ok := v.id[path]; ok {
		if id == "-" {
			st.DevIno = ""
		} else {
			st.DevIno = id
		}
	}
	return st, nil
}

// 197 — the sweep's loop key must distinguish different objects and
// recognize genuine revisits. Two directories with the SAME inode on
// different devices are distinct objects; both must be walked.
func TestLateRepairSweepKeyCrossDevice(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	// Recorded member in BOTH dirs — readdir order decides which is
	// walked first, so either false-skip must strand something.
	if _, _, err := s.WithWrite(ctx, "ws", "a/g.txt", "write",
		IfVersion{Mode: "any"}, sha("G"), authProbe(root, "a/g.txt"),
		authWriteFn(root, "a/g.txt", "G")); err != nil {
		t.Fatalf("write a/g.txt: %v", err)
	}
	if _, _, err := s.WithWrite(ctx, "ws", "b/f.txt", "write",
		IfVersion{Mode: "any"}, sha("F"), authProbe(root, "b/f.txt"),
		authWriteFn(root, "b/f.txt", "F")); err != nil {
		t.Fatalf("write b/f.txt: %v", err)
	}
	it := intent{owner: "dead-inst", scope: "ws", op: "write", path: "dead.txt",
		version: authMint(t, s), preFP: "0:0:0:0", at: time.Now().Add(-time.Hour)}
	it.id = insertIntent(t, s, it)
	parked := opStagePrefix + strconv.FormatInt(it.id, 10) + "-p-id"
	if err := os.Mkdir(dir+"/ws/"+parked, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(dir+"/ws/a", dir+"/ws/"+parked+"/a"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(dir+"/ws/b", dir+"/ws/"+parked+"/b"); err != nil {
		t.Fatal(err)
	}
	authExec(t, s, `DELETE FROM file_op WHERE id=$1`, it.id)
	// a and b report the same inode on DIFFERENT devices — distinct
	// objects that an inode-only key would collapse.
	s.SetReconcileView(authPinned(root, func(v ReconView) ReconView {
		return sweepIDView{ReconView: v,
			fp: map[string]string{parked + "/a": "5:0:0:0", parked + "/b": "5:0:0:0"},
			id: map[string]string{parked + "/a": "9:5", parked + "/b": "7:5"}}
	}))
	authSettle(t, s)
	if got, ok := authReadOpt(dir, "ws/b/f.txt"); !ok || got != "F" {
		t.Fatalf("recorded member of second same-ino dir stranded: "+
			"b/f.txt=%q present=%v (b was skipped as a false revisit)", got, ok)
	}
	if got, ok := authReadOpt(dir, "ws/a/g.txt"); !ok || got != "G" {
		t.Fatalf("recorded member of first same-ino dir stranded: "+
			"a/g.txt=%q present=%v (a was skipped as a false revisit)", got, ok)
	}
}

// cycleView fabricates a directory that lists a member reporting the
// parent's own dev:ino — a genuine revisit (bind alias / cyclic name
// chain). The loop guard must deduplicate it: one listing of the cycle
// dir per pass, and the fabricated member is never descended. The fake
// keys on a "/x" path suffix so it follows the parked container when the
// reconciler surfaces it under a public name mid-test.
type cycleView struct {
	ReconView
	id    string
	calls map[string]int
}

func (v cycleView) ListStaged(scope, dir, prefix string) ([]string, error) {
	if strings.HasSuffix(dir, "/x") {
		v.calls[dir]++
		return []string{"self"}, nil
	}
	return v.ReconView.ListStaged(scope, dir, prefix)
}

func (v cycleView) Stat(scope, path string) (FileInfo, error) {
	if strings.HasSuffix(path, "/x/self") {
		return FileInfo{Kind: "dir", Fingerprint: "9:0:0:0", DevIno: v.id}, nil
	}
	st, err := v.ReconView.Stat(scope, path)
	if err == nil && strings.HasSuffix(path, "/x") {
		st.DevIno = v.id
	}
	return st, err
}

// 197 converse — a genuine revisit (same dev:ino under a second name,
// e.g. a bind-mounted alias) is still deduplicated: the walk of the
// cycle terminates and the revisited object is not reprocessed.
func TestLateRepairSweepKeyAliasDedup(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	s := newPGStore(t, dsn, dir)
	ctx := context.Background()
	// The sweep enumerates scopes holding a row — anchor "ws" with a
	// real write so the dead namespace is reachable.
	if _, _, err := s.WithWrite(ctx, "ws", "keep.txt", "write",
		IfVersion{Mode: "any"}, sha("K"), authProbe(root, "keep.txt"),
		authWriteFn(root, "keep.txt", "K")); err != nil {
		t.Fatalf("write keep.txt: %v", err)
	}
	it := intent{owner: "dead-inst", scope: "ws", op: "write", path: "dead.txt",
		version: authMint(t, s), preFP: "0:0:0:0", at: time.Now().Add(-time.Hour)}
	it.id = insertIntent(t, s, it)
	parked := opStagePrefix + strconv.FormatInt(it.id, 10) + "-p-al"
	if err := os.MkdirAll(dir+"/ws/"+parked+"/x", 0o755); err != nil {
		t.Fatal(err)
	}
	authExec(t, s, `DELETE FROM file_op WHERE id=$1`, it.id)
	cv := cycleView{id: "9:9", calls: map[string]int{}}
	s.SetReconcileView(authPinned(root, func(v ReconView) ReconView {
		cv.ReconView = v
		return cv
	}))
	authSettle(t, s) // 3 passes — the seen set is per-pass
	// The container may move to a surfaced public name after pass 1, so
	// the cycle dir is listed under different paths across passes — but
	// exactly once per pass, and the fabricated revisit never descends.
	listed := 0
	for d, n := range cv.calls {
		if strings.HasSuffix(d, "/x/self") {
			t.Fatalf("revisited object reprocessed: %q listed %d times", d, n)
		}
		listed += n
	}
	if listed != 3 {
		t.Fatalf("cycle dir listed %d times over 3 passes, want 3", listed)
	}
}

// 197 fallback — an object with NO identifiable traversal key (dev:ino
// unavailable) is keyed by path: distinct paths are never deduplicated,
// which is the safe direction for recorded members.
func TestLateRepairSweepKeyPathFallback(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	if _, _, err := s.WithWrite(ctx, "ws", "a/g.txt", "write",
		IfVersion{Mode: "any"}, sha("G"), authProbe(root, "a/g.txt"),
		authWriteFn(root, "a/g.txt", "G")); err != nil {
		t.Fatalf("write a/g.txt: %v", err)
	}
	if _, _, err := s.WithWrite(ctx, "ws", "b/f.txt", "write",
		IfVersion{Mode: "any"}, sha("F"), authProbe(root, "b/f.txt"),
		authWriteFn(root, "b/f.txt", "F")); err != nil {
		t.Fatalf("write b/f.txt: %v", err)
	}
	it := intent{owner: "dead-inst", scope: "ws", op: "write", path: "dead.txt",
		version: authMint(t, s), preFP: "0:0:0:0", at: time.Now().Add(-time.Hour)}
	it.id = insertIntent(t, s, it)
	parked := opStagePrefix + strconv.FormatInt(it.id, 10) + "-p-fb"
	if err := os.Mkdir(dir+"/ws/"+parked, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(dir+"/ws/a", dir+"/ws/"+parked+"/a"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(dir+"/ws/b", dir+"/ws/"+parked+"/b"); err != nil {
		t.Fatal(err)
	}
	authExec(t, s, `DELETE FROM file_op WHERE id=$1`, it.id)
	// No dev:ino at all — paths must carry the guard, so both dirs walk.
	s.SetReconcileView(authPinned(root, func(v ReconView) ReconView {
		return sweepIDView{ReconView: v,
			fp: map[string]string{parked + "/a": "5:0:0:0", parked + "/b": "5:0:0:0"},
			id: map[string]string{parked + "/a": "-", parked + "/b": "-"}}
	}))
	authSettle(t, s)
	if got, ok := authReadOpt(dir, "ws/b/f.txt"); !ok || got != "F" {
		t.Fatalf("path-fallback sweep stranded recorded member: %q present=%v", got, ok)
	}
	if got, ok := authReadOpt(dir, "ws/a/g.txt"); !ok || got != "G" {
		t.Fatalf("path-fallback sweep stranded recorded member: %q present=%v", got, ok)
	}
}

// Directory-authority variants: multiple inode claims must not be
// resolved lexically — evidence ranks, and a genuinely unresolvable
// claim preserves the object until better evidence arrives.
//
// An EMPTY churned dir is the worst case: its fp3 has diverged from
// its own row (membership churn), it has no members to corroborate,
// and no candidate home holds it. Two rows claim the inode (its own +
// a ghost row left by inode reuse). The sweep must preserve the parked
// dir rather than install it at either home; when the ghost row is
// later removed the same sweep recovers it to the real row's home.
func TestLateRepairCorrDirAmbiguousThenResolves(t *testing.T) {
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
	// Ghost row: a leftover claiming the same object identity. Both rows
	// are seeded to equal strength — the ghost carries the object's own
	// bound oid where the filesystem provides one, so neither durable
	// identity nor the inode leg separates them. Equal strength is
	// injected, not assumed: real inode reuse cannot produce two rows
	// bound to one object.
	var zoid string
	{
		dctx, cancel := s.dbCtx(ctx)
		if err := s.pool.QueryRow(dctx,
			`SELECT oid FROM file_version WHERE scope='ws' AND path='zHome'`).Scan(&zoid); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
	}
	authExec(t, s,
		`UPDATE file_version SET fp=$1 WHERE scope='ws' AND path='zHome'`,
		dIno+":8:8:8")
	authExec(t, s,
		`INSERT INTO file_version (scope, path, version, fp, content_sha, oid)
		 VALUES ('ws','aGhost',$1,$2,'dir',$3)`, authMint(t, s), dIno+":9:9:9", zoid)
	parked := opStagePrefix + "555555-p-amb" // intent id never existed
	if err := os.Rename(dir+"/ws/zHome", dir+"/ws/"+parked); err != nil {
		t.Fatal(err)
	}
	s.SetReconcileView(authPinned(root, nil))
	authSettle(t, s)
	authSettle(t, s)
	// Ambiguous: the dir must be preserved and NOT installed at either
	// claimant's path — ambiguity surfaces visibly, never guesses.
	if _, serr := root.lstat("ws", "aGhost"); serr == nil {
		t.Fatal("ambiguous dir installed at ghost home aGhost")
	}
	if _, serr := root.lstat("ws", "zHome"); serr == nil {
		t.Fatal("ambiguous dir installed at zHome on inode leg alone")
	}
	alive := scanDirForInode(dir, "ws", dIno)
	if alive == "" || strings.Contains(alive, opStagePrefix) {
		t.Fatalf("ambiguous recorded dir LOST or hidden (ino %s): alive=%q", dIno, alive)
	}
	// Once surfaced the object has an ordinary row at its visible name —
	// removing the ghost row does not re-route it (public paths are
	// never re-judged on row evidence).
	authExec(t, s, `DELETE FROM file_version WHERE scope='ws' AND path='aGhost'`)
	authSettle(t, s)
	authSettle(t, s)
	if _, serr := root.lstat("ws", "aGhost"); serr == nil {
		t.Fatal("dir installed at aGhost after ambiguity resolved")
	}
	if alive := scanDirForInode(dir, "ws", dIno); alive == "" ||
		strings.Contains(alive, opStagePrefix) {
		t.Fatalf("recorded dir lost after ambiguity resolved (ino %s)", dIno)
	}
}

// Member corroboration: a churned recorded dir (fp3 no longer matches
// its row) carrying a recorded member resolves a multi-row inode claim
// through the member's own row — the container lands at the home the
// membership evidence supports, not the lexical or ghost claimant.
func TestLateRepairCorrDirMemberCorroboratesHome(t *testing.T) {
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
	// Writing a member churns the dir's size/mtime — its row's fp3 no
	// longer matches the live dir, so only the inode leg + membership
	// identify it.
	if _, _, err := s.WithWrite(ctx, "ws", "zHome/f.txt", "write",
		IfVersion{Mode: "any"}, sha("MC"), authProbe(root, "zHome/f.txt"),
		authWriteFn(root, "zHome/f.txt", "MC")); err != nil {
		t.Fatalf("write zHome/f.txt: %v", err)
	}
	_, dfp, _ := authRow(t, s, "zHome")
	dIno, _, _, _ := fpParts(dfp)
	parked := opStagePrefix + "666666-p-co"
	if err := os.Rename(dir+"/ws/zHome", dir+"/ws/"+parked); err != nil {
		t.Fatal(err)
	}
	authExec(t, s,
		`INSERT INTO file_version (scope, path, version, fp, content_sha)
		 VALUES ('ws','aGhost',$1,$2,'')`, authMint(t, s), dIno+":9:9:9")
	s.SetReconcileView(authPinned(root, nil))
	authSettle(t, s)
	authSettle(t, s)
	if _, serr := root.lstat("ws", "aGhost"); serr == nil {
		t.Fatal("dir installed at ghost home aGhost despite member evidence")
	}
	if got, ok := authReadOpt(dir, "ws/zHome/f.txt"); !ok || got != "MC" {
		t.Fatalf("corroborated dir not restored with member: zHome/f.txt=%q present=%v",
			got, ok)
	}
	st, serr := root.lstat("ws", "zHome")
	if serr != nil {
		t.Fatalf("zHome absent after corroborated restore: %v", serr)
	}
	if ino, _, _, _ := fpParts(st.Fingerprint); ino != dIno {
		t.Fatalf("zHome holds ino %s, want %s", ino, dIno)
	}
}

// Missing live-identity evidence fails closed on the deletion path:
// the impostor at the recorded home presents the recorded dir's exact
// fingerprint but NO dev:ino — the veto must not declare the captured
// object a surplus link on fingerprint alone.
func TestLateRepairCorrVetoMissingDevInoKeeps(t *testing.T) {
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
	if _, _, err := s.WithWrite(ctx, "ws", "homeD", "mkdir",
		IfVersion{Mode: "any"}, "dir", authProbe(root, "homeD"),
		func(it intent) (FileInfo, bool, error) {
			return root.mkdir("ws", "homeD")
		}); err != nil {
		t.Fatalf("mkdir homeD: %v", err)
	}
	_, dfp, _ := authRow(t, s, "homeD")
	dIno, _, _, _ := fpParts(dfp)
	it := intent{owner: "dead-inst", scope: "ws", op: "write", path: "wp",
		version: authMint(t, s), preFP: "0:0:0:0", dstFP: dfp,
		expectSHA: sha("W"), at: time.Now().Add(-time.Hour)}
	it.id = insertIntent(t, s, it)
	parked := opStagePrefix + strconv.FormatInt(it.id, 10) + "-p-mi"
	if err := os.Rename(dir+"/ws/homeD", dir+"/ws/"+parked); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/ws/wp", []byte("W"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir+"/ws/homeD", 0o755); err != nil {
		t.Fatal(err)
	}
	// The occupant presents the row's EXACT fingerprint but no live
	// device identity — the strongest possible row-level forgery.
	vf := authPinned(root, func(v ReconView) ReconView {
		return sweepIDView{ReconView: v,
			fp: map[string]string{"homeD": dfp},
			id: map[string]string{"homeD": "-"}}
	})
	view, err := vf(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	s.reconcileOne(ctx, it, view, false)
	if alive := scanDirForInode(dir, "ws", dIno); alive == "" {
		t.Fatalf("recorded dir (ino %s) destroyed on missing-identity evidence", dIno)
	}
}

// File variant of the cross-device veto: an occupant file reporting
// the recorded file's full fingerprint on a foreign device must not
// satisfy "still at home" — the recorded file survives, and the
// restore swap puts the recorded bytes back at their home while the
// impostor is parked aside.
func TestLateRepairCorrFileCrossDevOccupant(t *testing.T) {
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
	if _, _, err := s.WithWrite(ctx, "ws", "homeF", "write",
		IfVersion{Mode: "any"}, sha("HF"), authProbe(root, "homeF"),
		authWriteFn(root, "homeF", "HF")); err != nil {
		t.Fatalf("write homeF: %v", err)
	}
	_, ffp, _ := authRow(t, s, "homeF")
	fIno, _, _, _ := fpParts(ffp)
	it := intent{owner: "dead-inst", scope: "ws", op: "write", path: "wp",
		version: authMint(t, s), preFP: "0:0:0:0", dstFP: ffp,
		expectSHA: sha("W"), at: time.Now().Add(-time.Hour)}
	it.id = insertIntent(t, s, it)
	parked := opStagePrefix + strconv.FormatInt(it.id, 10) + "-p-fx"
	if err := os.Rename(dir+"/ws/homeF", dir+"/ws/"+parked); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/ws/wp", []byte("W"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A foreign-device file sits at homeF reporting the recorded
	// file's exact fingerprint — fp legs identical, dev:ino differs.
	if err := os.WriteFile(dir+"/ws/homeF", []byte("IMPOSTOR"), 0o644); err != nil {
		t.Fatal(err)
	}
	s.SetReconcileView(authPinned(root, func(v ReconView) ReconView {
		return sweepIDView{ReconView: v,
			fp: map[string]string{"homeF": ffp},
			id: map[string]string{"homeF": "7777:" + fIno}}
	}))
	s.lastStageSweep.Store(0)
	s.Reconcile(ctx)
	// The occupant is a live public object: identical fingerprints or
	// not, recovery never evicts it — the recorded file surfaces at a
	// visible sibling instead.
	if got, ok := authReadOpt(dir, "ws/homeF"); !ok || got != "IMPOSTOR" {
		t.Fatalf("fp-identical cross-dev occupant evicted: homeF=%q present=%v", got, ok)
	}
	if where := scanDirFor(t, dir, "ws", []byte("HF")); where == "" ||
		strings.HasPrefix(where, opStagePrefix) {
		t.Fatalf("recorded file destroyed or left private: %q", where)
	}
}

// Finding 200 / A's CW4 composition: a recorded dir carries a recorded
// member whose row now claims a home outside the container, plus an
// unknown member no row records. Finite interference resolved: the
// container is restored at its own recorded home with its identity and
// unknown member intact, and the recorded member reaches its own
// recorded home — not stranded inside the container at an unrecorded
// public path.
func TestLateRepairMemberExternalHomeAndUnknownRides(t *testing.T) {
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
	if _, _, err := s.WithWrite(ctx, "ws", "ddir", "mkdir",
		IfVersion{Mode: "any"}, "dir", authProbe(root, "ddir"),
		func(it intent) (FileInfo, bool, error) {
			return root.mkdir("ws", "ddir")
		}); err != nil {
		t.Fatalf("mkdir ddir: %v", err)
	}
	if _, _, err := s.WithWrite(ctx, "ws", "ddir/m", "write",
		IfVersion{Mode: "any"}, sha("MM"), authProbe(root, "ddir/m"),
		authWriteFn(root, "ddir/m", "MM")); err != nil {
		t.Fatalf("write ddir/m: %v", err)
	}
	// Unknown member: foreign bytes no row ever recorded.
	if err := os.WriteFile(dir+"/ws/ddir/u", []byte("UNKNOWN"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, dfp, _ := authRow(t, s, "ddir")
	dIno, _, _, _ := fpParts(dfp)
	// The member's committed home is now mout — a rename that moved the
	// row but never moved the bytes.
	authExec(t, s,
		`UPDATE file_version SET path='mout' WHERE scope='ws' AND path='ddir/m'`)
	parked := opStagePrefix + "888888-p-cw"
	if err := os.Mkdir(dir+"/ws/"+parked, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(dir+"/ws/ddir", dir+"/ws/"+parked+"/ddir"); err != nil {
		t.Fatal(err)
	}
	s.SetReconcileView(authPinned(root, nil))
	authSettle(t, s)
	authSettle(t, s)
	// Container restored at its own recorded home with its identity.
	st, serr := root.lstat("ws", "ddir")
	if serr != nil {
		t.Fatalf("container not restored to ddir: %v", serr)
	}
	if got, _, _, _ := fpParts(st.Fingerprint); got != dIno {
		t.Fatalf("container identity changed: ddir holds ino %s, want %s", got, dIno)
	}
	// The recorded member reached its own recorded home.
	if got, ok := authReadOpt(dir, "ws/mout"); !ok || got != "MM" {
		where := scanDirFor(t, dir, "ws", []byte("MM"))
		t.Fatalf("member not at its recorded home: mout=%q present=%v, MM at %q",
			got, ok, where)
	}
	// The unknown member rode the container — preserved, never routed.
	if got, ok := authReadOpt(dir, "ws/ddir/u"); !ok || got != "UNKNOWN" {
		t.Fatalf("unknown member lost or displaced: ddir/u=%q present=%v", got, ok)
	}
	// Fresh ops proceed after recovery.
	if _, _, err := s.WithWrite(ctx, "ws", "fresh.txt", "write",
		IfVersion{Mode: "any"}, sha("FR"), authProbe(root, "fresh.txt"),
		authWriteFn(root, "fresh.txt", "FR")); err != nil {
		t.Fatalf("fresh write after member recovery: %v", err)
	}
}

// Member home occupied by foreign unrecorded bytes: the member's row
// says out/m, and out/m holds newer foreign content no row records. A
// public occupant is never evicted on row evidence — the member rides
// its container home to ddir/m, preserved with its row diverging
// honestly, and the foreign occupant keeps its name untouched.
func TestLateRepairMemberHomeOccupiedPreservesBoth(t *testing.T) {
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
	if _, _, err := s.WithWrite(ctx, "ws", "ddir", "mkdir",
		IfVersion{Mode: "any"}, "dir", authProbe(root, "ddir"),
		func(it intent) (FileInfo, bool, error) {
			return root.mkdir("ws", "ddir")
		}); err != nil {
		t.Fatalf("mkdir ddir: %v", err)
	}
	if _, _, err := s.WithWrite(ctx, "ws", "ddir/m", "write",
		IfVersion{Mode: "any"}, sha("OLD"), authProbe(root, "ddir/m"),
		authWriteFn(root, "ddir/m", "OLD")); err != nil {
		t.Fatalf("write ddir/m: %v", err)
	}
	// Foreign NEWER bytes occupy the member's claimed home — written
	// outside the service, so no row records them.
	if err := os.Mkdir(dir+"/ws/out", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/ws/out/m", []byte("NEWER"), 0o644); err != nil {
		t.Fatal(err)
	}
	authExec(t, s,
		`UPDATE file_version SET path='out/m' WHERE scope='ws' AND path='ddir/m'`)
	parked := opStagePrefix + "999999-p-oc"
	if err := os.Mkdir(dir+"/ws/"+parked, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(dir+"/ws/ddir", dir+"/ws/"+parked+"/ddir"); err != nil {
		t.Fatal(err)
	}
	s.SetReconcileView(authPinned(root, nil))
	authSettle(t, s)
	authSettle(t, s)
	// The foreign occupant's public name is never disturbed on row
	// evidence — out/m keeps NEWER. The recorded member cannot be
	// installed over it, so it rides the restored container to ddir/m:
	// preserved, readable, and honestly diverged from its out/m row.
	if got, ok := authReadOpt(dir, "ws/out/m"); !ok || got != "NEWER" {
		t.Fatalf("foreign occupant evicted by row evidence: out/m=%q present=%v", got, ok)
	}
	if got, ok := authReadOpt(dir, "ws/ddir/m"); !ok || got != "OLD" {
		where := scanDirFor(t, dir, "ws", []byte("OLD"))
		t.Fatalf("recorded member lost: ddir/m=%q present=%v, OLD at %q",
			got, ok, where)
	}
	// The out/m row still records the member object — diverged from the
	// live occupant, which reads report as external_change.
	if _, fp, found := authRow(t, s, "out/m"); !found {
		t.Fatal("member row vanished")
	} else {
		live, lerr := root.lstat("ws", "out/m")
		if lerr != nil || fp3(live.Fingerprint) == fp3(fp) {
			t.Fatalf("member row falsely coherent with foreign occupant: "+
				"row fp=%q err=%v — divergence must stay visible", fp, lerr)
		}
	}
}

// Missing live identity on the member route: when the member's stat
// carries no dev:ino (a filesystem whose Sys() is not Stat_t, or an
// injected view without device evidence), the row is still the member's
// authority — the move revalidation cannot bind identity so it proceeds
// on the row judgment the pass already made. The member reaches its
// recorded home; nothing is deleted on missing evidence anywhere.
func TestLateRepairMemberRoutesWithoutLiveIdentity(t *testing.T) {
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
	if _, _, err := s.WithWrite(ctx, "ws", "ddir", "mkdir",
		IfVersion{Mode: "any"}, "dir", authProbe(root, "ddir"),
		func(it intent) (FileInfo, bool, error) {
			return root.mkdir("ws", "ddir")
		}); err != nil {
		t.Fatalf("mkdir ddir: %v", err)
	}
	if _, _, err := s.WithWrite(ctx, "ws", "ddir/m", "write",
		IfVersion{Mode: "any"}, sha("MM"), authProbe(root, "ddir/m"),
		authWriteFn(root, "ddir/m", "MM")); err != nil {
		t.Fatalf("write ddir/m: %v", err)
	}
	authExec(t, s,
		`UPDATE file_version SET path='mout' WHERE scope='ws' AND path='ddir/m'`)
	parked := opStagePrefix + "121212-p-ni"
	if err := os.Mkdir(dir+"/ws/"+parked, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(dir+"/ws/ddir", dir+"/ws/"+parked+"/ddir"); err != nil {
		t.Fatal(err)
	}
	// Strip dev:ino from the member's stat — the row must still route it.
	s.SetReconcileView(authPinned(root, func(v ReconView) ReconView {
		return sweepIDView{ReconView: v,
			id: map[string]string{parked + "/ddir/m": "-"}}
	}))
	authSettle(t, s)
	authSettle(t, s)
	if got, ok := authReadOpt(dir, "ws/mout"); !ok || got != "MM" {
		t.Fatalf("member not routed without live identity: mout=%q present=%v", got, ok)
	}
	if _, serr := root.lstat("ws", "ddir"); serr != nil {
		t.Fatalf("container not restored: %v", serr)
	}
}

// MW1 deterministic form: a stale row whose object is dead claims a
// foreign dir via a shared inode leg — the inode leg is injected into
// the stale row rather than won by allocation lottery (the defect is in
// claim acceptance, not inode reuse). File-row kind evidence
// (content_sha is a real sha) must exclude it from dir claims entirely.
func TestLateRepairSoleStaleFileRowCannotClaimDir(t *testing.T) {
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
	// External delete: the row stays, never marked diverged.
	if err := os.Remove(dir + "/ws/aa"); err != nil {
		t.Fatal(err)
	}
	// Foreign unrecorded dir; the stale row's fp leg is set to its
	// inode — the same state inode reuse produces organically.
	if err := os.Mkdir(dir+"/ws/forg", 0o755); err != nil {
		t.Fatal(err)
	}
	_, ffp, _ := func() (int64, string, bool) {
		st, serr := root.lstat("ws", "forg")
		if serr != nil {
			t.Fatal(serr)
		}
		return 0, st.Fingerprint, true
	}()
	fino, _, _, _ := fpParts(ffp)
	authExec(t, s,
		`UPDATE file_version SET fp=$1 WHERE scope='ws' AND path='aa'`,
		fino+":1:1:1")
	parked := opStagePrefix + "313131-p-mw"
	if err := os.Rename(dir+"/ws/forg", dir+"/ws/"+parked); err != nil {
		t.Fatal(err)
	}
	authSettle(t, s)
	authSettle(t, s)
	if st, serr := root.lstat("ws", "aa"); serr == nil && st.Kind == "dir" {
		t.Fatal("foreign dir installed at 'aa' on a stale file row's sole ino claim")
	}
	// The unclaimable foreign object is surfaced visibly — preserved and
	// enumerable, never installed on a stale row's word and never left
	// hidden under a private name.
	if alive := scanDirForInode(dir, "ws", fino); alive == "" ||
		strings.Contains(alive, opStagePrefix) {
		t.Fatalf("foreign dir vanished or hidden (ino %s): alive=%q", fino, alive)
	}
}

// Sole stale DIR row variant: kind-matched, ino-leg-rewritten — a stale
// row from inode reuse. Where the filesystem binds durable object
// identity the row's recorded oid contradicts the live object and the
// claim is rejected outright; on unbound filesystems the ino leg is the
// only evidence a real parked recorded dir can offer, so a sole claim
// may restore — never delete.
func TestLateRepairSoleStaleDirRowInoOnlyIsAmbiguous(t *testing.T) {
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
	if _, _, err := s.WithWrite(ctx, "ws", "aa", "mkdir",
		IfVersion{Mode: "any"}, "dir", authProbe(root, "aa"),
		func(it intent) (FileInfo, bool, error) {
			return root.mkdir("ws", "aa")
		}); err != nil {
		t.Fatalf("mkdir aa: %v", err)
	}
	if err := os.Remove(dir + "/ws/aa"); err != nil {
		t.Fatal(err)
	}
	// Foreign unrecorded dir; stale dir row's fp rewritten to its inode
	// leg — same ino, everything else different (a reused inode).
	if err := os.Mkdir(dir+"/ws/forg", 0o755); err != nil {
		t.Fatal(err)
	}
	st0, serr := root.lstat("ws", "forg")
	if serr != nil {
		t.Fatal(serr)
	}
	fino, _, _, _ := fpParts(st0.Fingerprint)
	// Rewrite only the fp leg: the row's bound oid still records the
	// DEAD object — where identity binds, that is affirmative
	// contradiction and the claim dies; where it does not bind, the
	// bare ino leg is indistinguishable from a real recorded dir's.
	var aoid string
	{
		dctx, cancel := s.dbCtx(ctx)
		if err := s.pool.QueryRow(dctx,
			`SELECT oid FROM file_version WHERE scope='ws' AND path='aa'`).Scan(&aoid); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
	}
	authExec(t, s,
		`UPDATE file_version SET fp=$1 WHERE scope='ws' AND path='aa'`,
		fino+":1:1:1")
	parked := opStagePrefix + "323232-p-mw"
	if err := os.Rename(dir+"/ws/forg", dir+"/ws/"+parked); err != nil {
		t.Fatal(err)
	}
	authSettle(t, s)
	authSettle(t, s)
	if aoid != "" {
		// Bound-oid contradiction rejected the claim: the foreign dir
		// surfaces visibly, never at the stale row's path, never hidden.
		if st, serr := root.lstat("ws", "aa"); serr == nil && st.Kind == "dir" {
			t.Fatal("foreign dir installed at 'aa' over a contradicting bound oid")
		}
	}
	if alive := scanDirForInode(dir, "ws", fino); alive == "" ||
		strings.Contains(alive, opStagePrefix) {
		t.Fatalf("foreign dir vanished or hidden (ino %s): alive=%q", fino, alive)
	}
}

// Ordinary external rename must NOT be undone. A person moving a→b on
// the shared filesystem leaves object O at b with row@a still recording
// it — the identical shape a recovery misplacement produces. Without a
// placement journal there is no durable evidence separating them, so
// the sweep must leave b alone: the row stays truthful and divergent,
// the object stays where the user put it.
func TestLateRepairExternalRenameIsNotReverted(t *testing.T) {
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
	if _, _, err := s.WithWrite(ctx, "ws", "a.txt", "write",
		IfVersion{Mode: "any"}, sha("A"), authProbe(root, "a.txt"),
		authWriteFn(root, "a.txt", "A")); err != nil {
		t.Fatalf("write a.txt: %v", err)
	}
	// Ordinary authorized external move — no Sumi intent, no residue.
	if err := os.Rename(dir+"/ws/a.txt", dir+"/ws/b.txt"); err != nil {
		t.Fatal(err)
	}
	authSettle(t, s)
	authSettle(t, s)
	if got, ok := authReadOpt(dir, "ws/b.txt"); !ok || got != "A" {
		t.Fatalf("external rename reverted: ws/b.txt=%q ok=%v", got, ok)
	}
	if fileExists(dir + "/ws/a.txt") {
		t.Fatal("external rename reverted: a.txt resurrected")
	}
	if _, fp, found := authRow(t, s, "a.txt"); !found || fp == "" {
		t.Fatal("row@a.txt rewritten — rows must stay truthful, not follow bytes")
	}
}

// The same protection over a recorded destination: mv a→b destroys b's
// recorded object (the user's own destructive choice) and leaves a's
// object at b. The sweep must not "repair" b back to a and strand the
// user's intent — both stay, rows stay honest.
func TestLateRepairExternalMoveOverRecordedPreserved(t *testing.T) {
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
	for _, p := range []string{"a.txt", "b.txt"} {
		if _, _, err := s.WithWrite(ctx, "ws", p, "write",
			IfVersion{Mode: "any"}, sha(p), authProbe(root, p),
			authWriteFn(root, p, p)); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}
	if err := os.Rename(dir+"/ws/a.txt", dir+"/ws/b.txt"); err != nil {
		t.Fatal(err)
	}
	authSettle(t, s)
	authSettle(t, s)
	got, ok := authReadOpt(dir, "ws/b.txt")
	if !ok || got != "a.txt" {
		t.Fatalf("external move reverted: ws/b.txt=%q ok=%v", got, ok)
	}
	if fileExists(dir + "/ws/a.txt") {
		t.Fatal("external move reverted: a.txt resurrected")
	}
}

// A journaled recovery placement does not freeze the path: the service
// restoring O to its recorded home, then the user moving O on to c, is
// ordinary editing that must stand.
func TestLateRepairPlacedThenUserMovedNotReverted(t *testing.T) {
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
	if _, _, err := s.WithWrite(ctx, "ws", "a.txt", "write",
		IfVersion{Mode: "any"}, sha("A"), authProbe(root, "a.txt"),
		authWriteFn(root, "a.txt", "A")); err != nil {
		t.Fatalf("write a.txt: %v", err)
	}
	// Park then let the sweep restore — a journaled placement at a.txt.
	parked := parkUnderDeadIntent(t, s, dir, "a.txt", "u")
	authSettle(t, s)
	if !fileExists(dir + "/ws/a.txt") {
		t.Fatalf("restore did not return a.txt (parked=%s)", parked)
	}
	// Ordinary user move AFTER the journaled placement.
	if err := os.Rename(dir+"/ws/a.txt", dir+"/ws/c.txt"); err != nil {
		t.Fatal(err)
	}
	authSettle(t, s)
	authSettle(t, s)
	if got, ok := authReadOpt(dir, "ws/c.txt"); !ok || got != "A" {
		t.Fatalf("user move of journaled object reverted: c.txt=%q ok=%v", got, ok)
	}
	if fileExists(dir + "/ws/a.txt") {
		t.Fatal("user move reverted: a.txt resurrected")
	}
}

// flakyStatView returns err for path the first N stat calls, then the
// real result — an unreadable-during-judgment candidate.
type flakyStatView struct {
	ReconView
	path string
	err  error
	left *int
}

func (v flakyStatView) Stat(scope, p string) (FileInfo, error) {
	if p == v.path && *v.left > 0 {
		*v.left--
		return FileInfo{}, v.err
	}
	return v.ReconView.Stat(scope, p)
}

// An unverifiable candidate path is not "open": EIO during claim
// ranking must not let member corroboration elect it over a genuinely
// absent claimant. Old code counted any stat error as absent; a
// corroborated-but-unverified home could then be elected and — once
// the error cleared mid-restore — evict a live foreign occupant on
// member inference alone.
func TestLateRepairUnreadableCandidateNotElected(t *testing.T) {
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
	// Recorded container P with member m; then P parked.
	if _, _, err := s.WithWrite(ctx, "ws", "dd/m.txt", "write",
		IfVersion{Mode: "any"}, sha("M"), authProbe(root, "dd/m.txt"),
		authWriteFn(root, "dd/m.txt", "M")); err != nil {
		t.Fatalf("write member: %v", err)
	}
	st, serr := root.lstat("ws", "dd")
	if serr != nil {
		t.Fatal(serr)
	}
	pino, _, _, _ := fpParts(st.Fingerprint)
	// Two weak ino-leg claimants for P: openH (absent) and blkH
	// (occupied by foreign dir F). m's row claims blkH/m.txt — member
	// corroboration for blkH only.
	authExec(t, s,
		`INSERT INTO file_version (scope,path,version,fp,updated,content_sha)
		 VALUES ('ws','openH',$1,$2,now(),'dir'),('ws','blkH',$1,$2,now(),'dir')`,
		authMint(t, s), pino+":0:0:0")
	authExec(t, s,
		`UPDATE file_version SET path='blkH/m.txt' WHERE scope='ws' AND path='dd/m.txt'`)
	// Foreign unrecorded dir occupies blkH.
	if err := os.Mkdir(dir+"/ws/blkH", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/ws/blkH/foreign.txt", []byte("F"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Park P (rename preserves ino; touch members so its fp3 diverges).
	if err := os.WriteFile(dir+"/ws/dd/churn.txt", []byte("c"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(dir + "/ws/dd/churn.txt"); err != nil {
		t.Fatal(err)
	}
	parked := parkUnderDeadIntent(t, s, dir, "dd", "u")
	pst, serr := root.lstat("ws", parked)
	if serr != nil {
		t.Fatal(serr)
	}
	pv, perr := root.pin(false)
	if perr != nil {
		t.Fatal(perr)
	}
	defer pv.Close()
	left := 1
	fv := flakyStatView{ReconView: pv, path: "blkH",
		err: ErrUnavailable, left: &left}
	// Claim-level discrimination: with blkH unverifiable at judgment
	// time it is not a homeless path — the only electable claimant is
	// openH, which is verifiably absent. Electing blkH is the defect.
	home, free, found := s.recordedHome(ctx, intent{scope: "ws"}, fv, pst, parked)
	if found && home == "blkH" {
		t.Fatal("unverifiable candidate elected — blkH was never proven absent")
	}
	if !found || !free || home != "openH" {
		t.Fatalf("verifiable absent claimant not elected: home=%q free=%v found=%v", home, free, found)
	}
	// End-state: the foreign occupant at blkH survives every pass; the
	// recorded member is extracted to ITS recorded home (blkH/m.txt,
	// verifiably absent — NOREPLACE can never evict the foreign dir's
	// contents), and the container lands at openH.
	s.SetReconcileView(authPinned(root, nil))
	authSettle(t, s)
	authSettle(t, s)
	if got, ok := authReadOpt(dir, "ws/blkH/foreign.txt"); !ok || got != "F" {
		t.Fatalf("foreign dir evicted via unverifiable candidate: %q ok=%v", got, ok)
	}
	if got, ok := authReadOpt(dir, "ws/blkH/m.txt"); !ok || got != "M" {
		t.Fatalf("recorded member not at its recorded home: blkH/m.txt=%q ok=%v",
			got, ok)
	}
	st2, serr := root.lstat("ws", "openH")
	if serr != nil {
		t.Fatalf("container not restored to openH: %v", serr)
	}
	if ino, _, _, _ := fpParts(st2.Fingerprint); ino != pino {
		t.Fatalf("openH holds ino %s, want %s", ino, pino)
	}
}
