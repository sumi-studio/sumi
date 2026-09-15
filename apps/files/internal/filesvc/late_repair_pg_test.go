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
	"strconv"
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

// 186/W1: a rename that commits while a captured foreign object cannot be
// restored (RENAME_NOREPLACE lost to a second racer) previously returned
// success, applied, and deleted the intent row — leaving the recorded
// object at a staging name no pass ever enumerates again.
//
// The racers are executor-side renames at the real fault points — the
// same positions a delayed FUSE syscall or an out-of-band move occupies.
// Required: the commit is journaled AND the intent is retained as a
// tombstone so the parked object keeps an enumerator; a later pass
// restores the recorded object to its recorded home and a fresh write
// succeeds.
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
	// object captured mid-race).
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

	var id int64
	root.faultHook = func(tag string) {
		switch tag {
		case "rename.postVerify":
			// Between the post-exchange verify and the capture, a racer
			// lands the RECORDED rec.txt object at the source name (D
			// survives at dkeep.txt so its fate is not in question).
			if err := os.Link(dir+"/ws/old.txt", dir+"/ws/dkeep.txt"); err != nil {
				panic(err)
			}
			if err := os.Rename(dir+"/ws/rec.txt", dir+"/ws/old.txt"); err != nil {
				panic(err)
			}
		case "rename.preRestore":
			// A second racer re-occupies the source name before the
			// restore — RENAME_NOREPLACE fails EEXIST; the captured R
			// stays parked at the staging slot.
			if err := os.WriteFile(dir+"/ws/old.txt", []byte("Q2"), 0o644); err != nil {
				panic(err)
			}
		}
	}
	_, _, rerr := s.Rename(ctx, "ws", "old.txt", "new.txt",
		IfVersion{Mode: "any"}, authProbe(root, "new.txt"), authProbe(root, "old.txt"),
		func(it intent) (FileInfo, bool, error) {
			id = it.id
			return root.rename("ws", "old.txt", "new.txt", false,
				it.dstFP, it.preFP, opStagePrefix+strconv.FormatInt(it.id, 10))
		})
	root.faultHook = nil
	// The commit stands but the parked undo must be reported — the
	// caller sees the interference, never a silent half-settled success.
	if rerr == nil || !errors.Is(rerr, ErrExternalChange) {
		t.Fatalf("rename with a parked undo = %v, want external_change", rerr)
	}
	slot := opStagePrefix + strconv.FormatInt(id, 10)
	if got, parked := authReadOpt(dir, "ws/"+slot); !parked || got != "R" {
		t.Fatalf("expected R parked at %s, got %q (present=%v)", slot, got, parked)
	}
	// The intent must be retained as a tombstone: deleting its row would
	// orphan the staged namespace (no pass enumerates a removed intent's
	// names).
	if n := intentCount(t, s); n != 1 {
		t.Fatalf("intent rows = %d, want 1 retained tombstone", n)
	}
	var resolved bool
	if err := s.pool.QueryRow(ctx,
		`SELECT resolved_at IS NOT NULL FROM file_op WHERE id=$1`, id).Scan(&resolved); err != nil {
		t.Fatal(err)
	}
	if !resolved {
		t.Fatal("committed+parked intent must be tombstoned, not left pending")
	}
	// The rename commit itself is journaled: new.txt's row records S's
	// object.
	liveNew := durFP(t, root, "ws", "new.txt")
	if _, fp, found := authRow(t, s, "new.txt"); !found || fp3(fp) != fp3(liveNew) {
		t.Fatalf("row(new.txt) = %q found=%v, live %s — committed rename not journaled", fp, found, liveNew)
	}

	authSettle(t, s)

	// Required: recorded content back at its recorded home, row intact.
	if got, ok := authReadOpt(dir, "ws/rec.txt"); !ok || got != "R" {
		where := scanDirFor(t, dir, "ws", []byte("R"))
		_, fp, found := authRow(t, s, "rec.txt")
		t.Fatalf("recorded object not restored to its home: rec.txt=%q "+
			"(present=%v), R bytes at %q, row(rec.txt)=%q found=%v",
			got, ok, where, fp, found)
	}
	if _, fp, found := authRow(t, s, "rec.txt"); !found || fp3(fp) != fp3(fpR) {
		t.Fatalf("row(rec.txt) = %q found=%v, want %s", fp, found, fpR)
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

	if got, _ := authReadOpt(dir, "ws/h.txt"); got != "L-content" {
		where := scanDirFor(t, dir, "ws", []byte("L-content"))
		_, fp, found := authRow(t, s, "h.txt")
		t.Fatalf("recorded content not restored to its home: h.txt=%q, "+
			"recorded bytes found at hidden name %q, row(h.txt)=%q found=%v "+
			"(intent rows left: %d)", got, where, fp, found, intentCount(t, s))
	}
	// The squatter is foreign content — parked at an enumerable staged
	// name, never destroyed.
	if where := scanDirFor(t, dir, "ws", []byte("squatter")); where == "" {
		t.Fatal("squatter bytes destroyed — foreign content must be preserved")
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

// 186: an orphan at a SEALED (-q-) name holds recorded content while a
// squatter occupies the home. The sealed object cannot receive the
// squatter — the sweep drains through a fresh enumerable -p- name.
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
	if got, _ := authReadOpt(dir, "ws/h.txt"); got != "sealed-R" {
		where := scanDirFor(t, dir, "ws", []byte("sealed-R"))
		t.Fatalf("sealed orphan not restored: h.txt=%q, recorded bytes at %q", got, where)
	}
	if where := scanDirFor(t, dir, "ws", []byte("squatter")); where == "" {
		t.Fatal("squatter bytes destroyed by the drain — must be parked")
	}
	if durExists(t, dir, "ws/"+sealed) {
		t.Fatal("sealed name still occupied after the drain")
	}
}

// 186: unknown unrecorded bytes at a dead namespace are preserved —
// recovery prioritizes acknowledged content but never collects foreign
// objects it cannot attribute.
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
	if got, ok := authReadOpt(dir, "ws/"+garbage); !ok || got != "unknown-bytes" {
		t.Fatalf("unrecorded orphan = %q present=%v — unknown bytes must be preserved", got, ok)
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
