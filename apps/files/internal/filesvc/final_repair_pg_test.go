package filesvc

// Regression tests for the independent final reviews (findings
// F236–F244). Several tests seed journal states that production itself
// produces — a file_op.names record written here is byte-identical to
// the row a failed best-effort journal patch leaves behind — and the
// kill-boundary cases run through the real syscall path with a genuine
// SIGKILL (see rename_boundary_pg_test.go).

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// finalStore builds a real Store + posixRoot over a fresh root.
func finalStore(t *testing.T, dsn string) (*Store, *posixRoot, string) {
	t.Helper()
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
	return s, root, dir
}

func putFile(t *testing.T, dir, rel, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(dir+"/"+rel), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/"+rel, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// commitWriteState writes the post-conditions of a committed write
// apply: a product row carrying expectSHA at path plus the file_event —
// exactly what apply/applyRetained commit in one tx.
func commitWriteState(t *testing.T, s *Store, ver int64, path, expectSHA, fp string) {
	t.Helper()
	authExec(t, s,
		`INSERT INTO file_version (scope, path, version, fp, content_sha)
		 VALUES ('ws',$1,$2,$3,$4)
		 ON CONFLICT (scope,path) DO UPDATE SET version=EXCLUDED.version,
		   fp=EXCLUDED.fp, content_sha=EXCLUDED.content_sha`,
		path, ver, fp, expectSHA)
	authExec(t, s,
		`INSERT INTO file_event (scope, path, op, version) VALUES ('ws',$1,'write',$2)`,
		path, ver)
}

// failStatOnceView fails the first Stat beneath a path substring — the
// F237 transient-observation injection.
type failStatOnceView struct {
	ReconView
	prefix string
	once   *bool
}

func (w *failStatOnceView) Stat(scope, path string) (FileInfo, error) {
	if *w.once && w.prefix != "" && strings.Contains(path, w.prefix) {
		*w.once = false
		return FileInfo{}, ErrUnavailable
	}
	return w.ReconView.Stat(scope, path)
}

// failMoveOnceView fails the first MoveStaged — the F236 pass-1
// capture-evasion injection from review A.
type failMoveOnceView struct {
	ReconView
	once *bool
}

func (w *failMoveOnceView) MoveStaged(scope, from, to string) error {
	if *w.once {
		*w.once = false
		return ErrUnavailable
	}
	return w.ReconView.MoveStaged(scope, from, to)
}

// --- F236/F243 (A-F1, B-F1, B-F3): a foreign object at a private name
// is never destroyed on act/event/row facts alone ----------------------

// A committed write's tombstone still reads act=put res=ok (the xch act
// patch failed — a production-identical journal state) while the slot
// holds a FOREIGN object. The reconciler must surface it, never delete.
func TestFinalForeignAtPutActName(t *testing.T) {
	dsn := pgDSN(t)
	s, root, dir := finalStore(t, dsn)
	ctx := context.Background()

	putFile(t, dir, "ws/victim.txt", "new")
	live := durFP(t, root, "ws", "victim.txt")
	commitWriteState(t, s, 50, "victim.txt", sha("new"), live)

	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "victim.txt",
		version: 50, expectSHA: sha("new"),
		names: []nameRec{{Name: ".filesv-op-77-a0-ff01", Act: "put", Res: "ok"}},
		at:    time.Now().Add(-time.Hour),
	})
	authExec(t, s, `UPDATE file_op SET resolved_at=now() WHERE id=$1`, id)

	putFile(t, dir, "ws/.filesv-op-77-a0-ff01", "foreign-user-bytes")

	s.lastTombScan.Store(0)
	s.Reconcile(ctx)

	// The foreign object must survive at a public name — surfaced, with
	// a truthful row and a recover event.
	if where := scanTreeFor(t, dir, "ws", []byte("foreign-user-bytes")); where == "" || containsPrivateSeg(where) {
		t.Fatalf("FOREIGN OBJECT DESTROYED or left hidden: %q", where)
	}
	if got, _ := authReadOpt(dir, "ws/victim.txt"); got != "new" {
		t.Fatalf("victim.txt=%q — committed product disturbed", got)
	}
	if e := scanDirForPrivate(dir, "ws"); e != "" {
		t.Fatalf("private residue left: %q", e)
	}
}

// Same stale-act record WITHOUT commit evidence: still preserve.
func TestFinalForeignAtPutActNameUncommitted(t *testing.T) {
	dsn := pgDSN(t)
	s, _, dir := finalStore(t, dsn)
	ctx := context.Background()

	putFile(t, dir, "ws/victim.txt", "new")
	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "victim.txt",
		version: 50, expectSHA: sha("new"),
		names: []nameRec{{Name: ".filesv-op-78-a0-ff02", Act: "put", Res: "ok"}},
		at:    time.Now().Add(-time.Hour),
	})
	putFile(t, dir, "ws/.filesv-op-78-a0-ff02", "foreign-user-bytes")

	s.Reconcile(ctx)

	if where := scanTreeFor(t, dir, "ws", []byte("foreign-user-bytes")); where == "" || containsPrivateSeg(where) {
		t.Fatalf("foreign object destroyed or left hidden without commit evidence: %q", where)
	}
}

// A committed REMOVE's tombstone with act=cap res=ok and a foreign
// occupant: the captured-object identity is unverified, so it surfaces
// — a committed event is never disposal authority for unverified bytes.
func TestFinalForeignAtRemoveCapName(t *testing.T) {
	dsn := pgDSN(t)
	s, _, dir := finalStore(t, dsn)
	ctx := context.Background()

	authExec(t, s,
		`INSERT INTO file_event (scope,path,op,version) VALUES ('ws','gone.txt','remove',60)`)
	putFile(t, dir, "ws/keep.txt", "keep")

	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "remove", path: "gone.txt",
		version: 60, dstFP: "999:4:111", dstSHA: sha("gone-body"),
		names: []nameRec{{Name: ".filesv-op-93-a0-ff07", Act: "cap", Src: "gone.txt", Res: "ok"}},
		at:    time.Now().Add(-time.Hour),
	})
	authExec(t, s, `UPDATE file_op SET resolved_at=now() WHERE id=$1`, id)

	putFile(t, dir, "ws/.filesv-op-93-a0-ff07", "foreign-at-remove-slot")

	s.lastTombScan.Store(0)
	s.Reconcile(ctx)

	if where := scanTreeFor(t, dir, "ws", []byte("foreign-at-remove-slot")); where == "" || containsPrivateSeg(where) {
		t.Fatalf("foreign occupant at a committed remove's cap name was destroyed or left hidden: %q", where)
	}
}

// A displaced object whose weak identity (fp3+sha, no bound oid) matches
// the declared dst on a tombstone with an unrecorded exchange result:
// the committed product stands at new.txt, so D surfaces visibly.
func TestFinalDisplacedWeakSurfaced(t *testing.T) {
	dsn := pgDSN(t)
	s, root, dir := finalStore(t, dsn)
	ctx := context.Background()

	putFile(t, dir, "ws/new.txt", "S-body")
	liveS := durFP(t, root, "ws", "new.txt")
	authExec(t, s,
		`INSERT INTO file_version (scope,path,version,fp,content_sha) VALUES ('ws','new.txt',70,$1,$2)`,
		liveS, sha("S-body"))
	authExec(t, s,
		`INSERT INTO file_event (scope,path,from_path,op,version) VALUES ('ws','new.txt','old.txt','rename',70)`)

	putFile(t, dir, "ws/.filesv-op-88-a0-ff04", "D-body")
	liveD := durFP(t, root, "ws", ".filesv-op-88-a0-ff04")

	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "rename",
		path: "old.txt", toPath: "new.txt", version: 70,
		expectSHA: sha("S-body"), dstFP: liveD, dstSHA: sha("D-body"),
		names: []nameRec{{Name: ".filesv-op-88-a0-ff04", Act: "xch", Res: ""}},
		at:    time.Now().Add(-time.Hour),
	})
	authExec(t, s, `UPDATE file_op SET resolved_at=now() WHERE id=$1`, id)

	s.lastTombScan.Store(0)
	s.Reconcile(ctx)

	where := scanTreeFor(t, dir, "ws", []byte("D-body"))
	if where == "" || containsPrivateSeg(where) {
		t.Fatalf("displaced D destroyed or left hidden: %q", where)
	}
	if got, _ := authReadOpt(dir, "ws/new.txt"); got != "S-body" {
		t.Fatalf("committed product disturbed: new.txt=%q", got)
	}
}

// The retained-diverged chain: a committed (diverged) write event plus a
// same-content product row is still not disposal authority for the
// foreign object parked at the intent's slot. Seeded journal state —
// the real-SIGKILL version is TestBoundaryKillWritePreUndo.
func TestFinalForeignDisplacedDivergedEvent(t *testing.T) {
	dsn := pgDSN(t)
	s, root, dir := finalStore(t, dsn)
	ctx := context.Background()

	// victim.txt holds the diverged foreign content G; row+event record
	// the diverged applyKeep observation (event journaled, intent kept).
	putFile(t, dir, "ws/victim.txt", "FOREIGN-G")
	liveG := durFP(t, root, "ws", "victim.txt")
	commitWriteState(t, s, 55, "victim.txt", sha("FOREIGN-G"), liveG)

	// The slot holds F — an acknowledged foreign object exchanged out
	// of victim.txt when it raced in after declare declared X there.
	putFile(t, dir, "ws/.filesv-op-96-a0-ff09", "F-ACK")

	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "victim.txt",
		version: 55, expectSHA: sha("BODY"),
		dstFP: "999:4:222", dstSHA: sha("X-declared"),
		names: []nameRec{{Name: ".filesv-op-96-a0-ff09", Act: "xch", Src: "victim.txt", Res: "ok"}},
		at:    time.Now().Add(-time.Hour),
	})
	authExec(t, s, `UPDATE file_op SET resolved_at=now() WHERE id=$1`, id)

	// Pass 1: an injected settle failure keeps the foreign record
	// pending while the tombstone scan judges it — as in A's pause.
	once := true
	s.SetReconcileView(authPinned(root, func(v ReconView) ReconView {
		return &failMoveOnceView{ReconView: v, once: &once}
	}))
	s.lastTombScan.Store(0)
	s.Reconcile(ctx)

	// Same-content retry once the hot tombstone ages: the product row
	// now carries the intent's own expectSHA — the exact condition the
	// old code used as authority — while the parked record is pending.
	authExec(t, s, `UPDATE file_op SET resolved_at=now()-interval '2 hours' WHERE id=$1`, id)
	s.SetReconcileView(authPinned(root, nil))
	if _, _, err := s.WithWrite(ctx, "ws", "victim.txt", "write",
		IfVersion{Mode: "any"}, sha("BODY"),
		authProbe(root, "victim.txt"), authWriteFn(root, "victim.txt", "BODY")); err != nil {
		t.Fatalf("retry write: %v", err)
	}

	s.lastTombScan.Store(0)
	s.lastTombScanCold.Store(0)
	s.Reconcile(ctx)

	where := scanTreeFor(t, dir, "ws", []byte("F-ACK"))
	if where == "" || containsPrivateSeg(where) {
		t.Fatalf("F-ACK destroyed or left hidden: %q", where)
	}
	if !strings.Contains(where, "recovered-o") {
		t.Fatalf("F-ACK surfaced at %q, want a recovered-* name", where)
	}
	if got, _ := authReadOpt(dir, "ws/victim.txt"); got != "BODY" {
		t.Fatalf("victim.txt=%q — retry product disturbed", got)
	}
	if _, _, found := authRow(t, s, where); !found {
		t.Fatalf("surfaced F-ACK at %q has no version row", where)
	}
}

// --- F237 (A-F2): transient settle errors are never terminal ----------

// A transient Stat failure on the surface name must NOT mark the record
// done: the next pass backfills the row and recover event, and a third
// pass mints no duplicates.
func TestFinalSettleTransientStatRetries(t *testing.T) {
	dsn := pgDSN(t)
	s, root, dir := finalStore(t, dsn)
	ctx := context.Background()

	authUndoParkedTombstone(t, s, root, dir)

	once := true
	s.SetReconcileView(authPinned(root, func(v ReconView) ReconView {
		return &failStatOnceView{ReconView: v, prefix: "recovered-o", once: &once}
	}))
	s.lastTombScan.Store(0)
	s.Reconcile(ctx) // pass 1: surface move may land; the stat fails

	// Uninjected passes converge: row + recover event appear — the
	// transient failure was never terminal.
	authSettle(t, s)
	recovered := scanDirFor(t, dir, "ws", []byte("D"))
	if recovered == "" || !strings.Contains(recovered, "recovered-o") {
		t.Fatalf("D not surfaced at a recovered-* name: %q", recovered)
	}
	if _, _, found := authRow(t, s, recovered); !found {
		t.Fatalf("surfaced D at %q has no version row — rowless publication", recovered)
	}
	var ev int
	if err := s.pool.QueryRow(ctx,
		`SELECT 1 FROM file_event WHERE scope='ws' AND path=$1 AND op='recover'`,
		recovered).Scan(&ev); err != nil {
		t.Fatalf("no recover event for %q: %v", recovered, err)
	}

	// Idempotence: further passes mint no duplicate version or event.
	v1, _, _ := authRow(t, s, recovered)
	authSettle(t, s)
	v2, _, _ := authRow(t, s, recovered)
	if v1 != v2 {
		t.Fatalf("re-pass minted a new version at %q: %d -> %d", recovered, v1, v2)
	}
	var nev int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM file_event WHERE scope='ws' AND path=$1 AND op='recover'`,
		recovered).Scan(&nev); err != nil || nev != 1 {
		t.Fatalf("recover events at %q = %d, want exactly 1", recovered, nev)
	}
}

// A record whose row+event committed but whose done patch failed (the
// post-commit/pre-done crash) must converge without duplicating.
func TestFinalSettleEstablishedConverges(t *testing.T) {
	dsn := pgDSN(t)
	s, root, dir := finalStore(t, dsn)
	ctx := context.Background()

	// The settled object already sits at its journaled surface home and
	// its row+event committed — only the done patch was lost.
	putFile(t, dir, "ws/recovered-o91-aaaa", "rescued")
	live := durFP(t, root, "ws", "recovered-o91-aaaa")
	seen := obsIdent(FileInfo{Fingerprint: live})
	authExec(t, s,
		`INSERT INTO file_version (scope,path,version,fp,content_sha) VALUES ('ws','recovered-o91-aaaa',91,$1,$2)`,
		live, sha("rescued"))
	authExec(t, s,
		`INSERT INTO file_event (scope,path,from_path,op,version) VALUES ('ws','recovered-o91-aaaa','.filesv-op-91-a0-ff05','recover',91)`)

	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "victim.txt",
		version: 90, expectSHA: sha("rescued"),
		names: []nameRec{{
			Name: ".filesv-op-91-a0-ff05", Act: "put", Res: "ok",
			Home: "recovered-o91-aaaa", Seen: []string{seen},
		}},
		at: time.Now().Add(-time.Hour),
	})
	s.Reconcile(ctx)
	authSettle(t, s)

	if v, fp, found := authRow(t, s, "recovered-o91-aaaa"); !found || v != 91 || fp != live {
		t.Fatalf("row at recovered-o91-aaaa = (%d,%q,%v), want (91,%q,true)", v, fp, found, live)
	}
	var nev int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM file_event WHERE scope='ws' AND path='recovered-o91-aaaa' AND op='recover'`).Scan(&nev); err != nil || nev != 1 {
		t.Fatalf("recover events = %d, want exactly 1", nev)
	}
}

// Journaled-home bookkeeping after post-move/pre-PG death: the name is
// empty, the destination still holds the settled object — the pass
// writes the row/event without re-moving anything.
func TestFinalJournaledHomeFinishesBookkeeping(t *testing.T) {
	dsn := pgDSN(t)
	s, root, dir := finalStore(t, dsn)
	ctx := context.Background()

	putFile(t, dir, "ws/recovered-o92-bbbb", "rescued2")
	live := durFP(t, root, "ws", "recovered-o92-bbbb")
	seen := obsIdent(FileInfo{Fingerprint: live})

	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "victim.txt",
		version: 92, expectSHA: sha("rescued2"),
		names: []nameRec{{
			Name: ".filesv-op-92-a0-ff10", Act: "put", Res: "ok",
			Home: "recovered-o92-bbbb", Seen: []string{seen},
		}},
		at: time.Now().Add(-time.Hour),
	})
	s.Reconcile(ctx)

	if v, fp, found := authRow(t, s, "recovered-o92-bbbb"); !found || fp != live {
		t.Fatalf("settled row missing/mismatch: found=%v v=%d fp=%q live=%q", found, v, fp, live)
	}
	var ev int
	if err := s.pool.QueryRow(ctx,
		`SELECT 1 FROM file_event WHERE scope='ws' AND path='recovered-o92-bbbb' AND op='recover'`).Scan(&ev); err != nil {
		t.Fatalf("no recover event for settled surface object: %v", err)
	}
}

// --- F244 (B-F4): a late deposit at a done name is re-judged ----------

func TestFinalDoneNameLateDepositSurfaces(t *testing.T) {
	dsn := pgDSN(t)
	s, _, dir := finalStore(t, dsn)
	ctx := context.Background()

	putFile(t, dir, "ws/keep.txt", "keep")
	if _, _, err := s.WithWrite(ctx, "ws", "keep.txt", "write",
		IfVersion{Mode: "any"}, sha("keep2"), nil, func(it intent) (FileInfo, bool, error) {
			it.njDone()
			return FileInfo{Kind: "file", Fingerprint: "fp-keep"}, true, nil
		}); err != nil {
		t.Fatalf("seed write: %v", err)
	}

	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "remove", path: "gone.txt",
		version: 61,
		names:   []nameRec{{Name: ".filesv-op-94-a0-ff08", Act: "cap", Src: "gone.txt", Res: "ok", Done: true}},
		at:      time.Now().Add(-2 * time.Hour),
	})
	authExec(t, s, `UPDATE file_op SET resolved_at=now() - interval '90 minutes' WHERE id=$1`, id)

	// A late filesystem effect / direct deposit lands at the drained
	// name — it must be re-judged and surfaced, never left hidden.
	putFile(t, dir, "ws/.filesv-op-94-a0-ff08", "late-deposited-object")

	s.lastTombScan.Store(0)
	s.lastTombScanCold.Store(0)
	s.lastStageSweep.Store(0)
	s.Reconcile(ctx)

	where := scanTreeFor(t, dir, "ws", []byte("late-deposited-object"))
	if where == "" || containsPrivateSeg(where) {
		t.Fatalf("late deposit at a done name stayed hidden or was destroyed: %q", where)
	}
	if _, _, found := authRow(t, s, where); !found {
		t.Fatalf("surfaced late deposit at %q has no version row", where)
	}
}

// --- F238 (A-F3): xch/und journal scope-relative src paths ------------

// A nested write killed after its exchange must journal the
// scope-relative path ("sub/victim.txt"), not the basename — recovery
// resolves the displaced object beneath its directory.
func TestFinalNestedJournalPath(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	boundaryKill(t, dsn, dir, "write.nestedPostXch")

	s := newPGStore(t, dsn, dir)
	var names []nameRec
	var raw string
	err := s.pool.QueryRow(context.Background(),
		`SELECT names::text FROM file_op WHERE scope='ws' AND path='sub/victim.txt'`).Scan(&raw)
	if err != nil {
		t.Fatalf("no intent row for the nested write: %v", err)
	}
	if err := json.Unmarshal([]byte(raw), &names); err != nil {
		t.Fatalf("names unmarshal: %v", err)
	}
	if len(names) == 0 {
		t.Fatal("no journal")
	}
	if names[0].Src != "sub/victim.txt" {
		t.Fatalf("xch src = %q, want scope-relative %q", names[0].Src, "sub/victim.txt")
	}
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.SetReconcileView(authPinned(root, nil))
	authExec(t, s, `UPDATE file_op SET at = now() - interval '1 hour'`)
	authSettle(t, s)
	// The displaced OLD (nested) is preserved at a public name — the
	// scope-relative journal resolved it correctly.
	if loc := scanTreeFor(t, dir, "ws", []byte("OLD")); loc == "" || containsPrivateSeg(loc) {
		t.Fatalf("displaced nested OLD destroyed or hidden: %q", loc)
	}
}

// --- F239 (A-F4): private names are not user-addressable --------------

func TestFinalPrivateNamesNotAddressable(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(dir+"/ws/sub", 0o755); err != nil {
		t.Fatal(err)
	}
	putFile(t, dir, "ws/.filesv-op-x9", "SLOT")
	putFile(t, dir, "ws/sub/.filesv-op-y1", "NEST")
	putFile(t, dir, "ws/.filesv-tmp-z", "TMP")

	svc, err := NewAt(dir, newFakeStore(), map[string]map[string]bool{"k": {"ws": true}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ op, path string }{
		{"read", ".filesv-op-x9"},
		{"stat", ".filesv-op-x9"},
		{"list", ".filesv-op-x9"},
		{"read", "sub/.filesv-op-y1"},
		{"stat", "sub/.filesv-op-y1"},
		{"read", ".filesv-tmp-z"},
	} {
		w := req(t, svc, "GET", "/v1/files/ws/"+tc.op+"?path="+tc.path, "k", "", nil)
		if w.Code != 400 {
			t.Fatalf("%s %s -> %d %s — want 400 reserved_name", tc.op, tc.path, w.Code, w.Body.String())
		}
	}
}

// --- B-F6: unrecorded-scope deposits are swept within the owned root --

// A private object under a scope dir that has NO DB rows at all is
// still enumerated — the sweep lists the pinned root's immediate
// children, bounded to the owned root.
func TestFinalOrphanUnrecordedScopeSwept(t *testing.T) {
	dsn := pgDSN(t)
	s, _, dir := finalStore(t, dsn)
	ctx := context.Background()

	// Scope "ghost" exists only on disk — no version rows, no intents.
	if err := os.MkdirAll(dir+"/ghost", 0o755); err != nil {
		t.Fatal(err)
	}
	putFile(t, dir, "ghost/.filesv-op-7777-a0-beef", "unrecorded-scope-bytes")

	s.lastStageSweep.Store(0)
	s.Reconcile(ctx)

	where := scanTreeFor(t, dir, "ghost", []byte("unrecorded-scope-bytes"))
	if where == "" || containsPrivateSeg(where) {
		t.Fatalf("unrecorded-scope deposit not surfaced: %q", where)
	}
}

// --- New pub boundary: declared-empty publish is non-destructive ------

// A foreign occupant that lands at a declared-empty destination before
// the publish is never displaced into the private slot: the write
// reports external_change, the foreign object is untouched, and no
// residue remains.
func TestFinalPubForeignOccupantRejected(t *testing.T) {
	dsn := pgDSN(t)
	s, root, dir := finalStore(t, dsn)
	ctx := context.Background()

	putFile(t, dir, "ws/fhome.txt", "F-ACK")
	root.faultHook = func(tag string) {
		if tag == "write.postCreate" {
			// A foreign object claims the declared-empty destination.
			if err := os.Rename(dir+"/ws/fhome.txt", dir+"/ws/victim.txt"); err != nil {
				panic(err)
			}
		}
	}
	_, _, err := s.WithWrite(ctx, "ws", "victim.txt", "write",
		IfVersion{Mode: "any"}, sha("BODY"),
		authProbe(root, "victim.txt"), authWriteFn(root, "victim.txt", "BODY"))
	root.faultHook = nil
	if !errors.Is(err, ErrExternalChange) {
		t.Fatalf("write over foreign-occupied empty dst = %v, want external_change", err)
	}
	if got, _ := authReadOpt(dir, "ws/victim.txt"); got != "F-ACK" {
		t.Fatalf("foreign occupant disturbed: victim.txt=%q", got)
	}
	if e := scanDirForPrivate(dir, "ws"); e != "" {
		t.Fatalf("private residue left: %q", e)
	}
	// Ordinary retry succeeds once interference stops.
	if _, _, err := s.WithWrite(ctx, "ws", "victim.txt", "write",
		IfVersion{Mode: "any"}, sha("BODY"),
		authProbe(root, "victim.txt"), authWriteFn(root, "victim.txt", "BODY")); err != nil {
		t.Fatalf("retry write: %v", err)
	}
	if got, _ := authReadOpt(dir, "ws/victim.txt"); got != "BODY" {
		t.Fatalf("victim.txt=%q after retry", got)
	}
}

// --- B P2 port: late-landing authored body converges ------------------

func TestFinalLateAuthoredBodyConverges(t *testing.T) {
	dsn := pgDSN(t)
	s, root, dir := finalStore(t, dsn)
	ctx := context.Background()

	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "w.txt",
		version: 60, expectSHA: sha("W-body"),
		names: []nameRec{{Name: ".filesv-op-79-a0-ff03", Act: "put", Res: ""}},
		at:    time.Now().Add(-time.Hour),
	})
	// The put syscall landed after the owner died.
	putFile(t, dir, "ws/.filesv-op-79-a0-ff03", "W-body")

	s.Reconcile(ctx)

	where := scanTreeFor(t, dir, "ws", []byte("W-body"))
	if where == "" || containsPrivateSeg(where) {
		t.Fatalf("late-authored body destroyed or left hidden: %q", where)
	}

	// Liveness: a fresh write proceeds once the hot window ages out.
	authExec(t, s, `UPDATE file_op SET resolved_at = now() - interval '1 hour' WHERE resolved_at IS NOT NULL`)
	ver, _, err := s.WithWrite(ctx, "ws", "w.txt", "write", IfVersion{Mode: "any"},
		sha("retry"), authProbe(root, "w.txt"), authWriteFn(root, "w.txt", "retry"))
	if err != nil {
		t.Fatalf("fresh write after settle: %v", err)
	}
	if ver <= 0 {
		t.Fatalf("write version %d", ver)
	}
	if got, _ := authReadOpt(dir, "ws/w.txt"); got != "retry" {
		t.Fatalf("w.txt = %q, want retry", got)
	}
}

// --- B P3 port: orphan private name surfaces through a recover intent -

func TestFinalOrphanNameSurfaces(t *testing.T) {
	dsn := pgDSN(t)
	s, _, dir := finalStore(t, dsn)
	ctx := context.Background()

	if _, _, err := s.WithWrite(ctx, "ws", "keep.txt", "write",
		IfVersion{Mode: "any"}, sha("keep"), nil, func(it intent) (FileInfo, bool, error) {
			it.njDone()
			return FileInfo{Kind: "file", Fingerprint: "fp-keep"}, true, nil
		}); err != nil {
		t.Fatalf("seed write: %v", err)
	}

	putFile(t, dir, "ws/.filesv-op-4242-a0-dead", "orphaned-bytes")

	s.lastStageSweep.Store(0)
	s.Reconcile(ctx)

	where := scanTreeFor(t, dir, "ws", []byte("orphaned-bytes"))
	if where == "" || containsPrivateSeg(where) {
		t.Fatalf("orphaned content destroyed or left hidden: %q", where)
	}
	if _, _, found := authRow(t, s, where); !found {
		t.Fatalf("surfaced orphan %q has no version row", where)
	}
}

// --- B P7 port: surfaced object drops falsified claims, keeps rest ----

func TestFinalSurfaceDropsStaleClaim(t *testing.T) {
	dsn := pgDSN(t)
	s, root, dir := finalStore(t, dsn)
	ctx := context.Background()

	putFile(t, dir, "ws/.filesv-op-95-a0-ff11", "moved-object")
	live := durFP(t, root, "ws", ".filesv-op-95-a0-ff11")
	// A stale row claims this exact object at an absent path.
	authExec(t, s,
		`INSERT INTO file_version (scope,path,version,fp,content_sha) VALUES ('ws','oldpath.txt',95,$1,$2)`,
		live, sha("moved-object"))
	putFile(t, dir, "ws/other.txt", "other")
	liveO := durFP(t, root, "ws", "other.txt")
	authExec(t, s,
		`INSERT INTO file_version (scope,path,version,fp,content_sha) VALUES ('ws','other.txt',96,$1,$2)`,
		liveO, sha("other"))

	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "rename",
		path: "oldpath.txt", toPath: "newpath.txt", version: 97,
		names: []nameRec{{Name: ".filesv-op-95-a0-ff11", Act: "cap", Src: "oldpath.txt", Res: "ok"}},
		at:    time.Now().Add(-time.Hour),
	})
	s.Reconcile(ctx)

	// The captured object returns home only because the row there
	// already claims this exact object — restore makes a recorded
	// claim truthful; otherwise it surfaces.
	where := scanTreeFor(t, dir, "ws", []byte("moved-object"))
	if where == "" || containsPrivateSeg(where) {
		t.Fatalf("moved object destroyed or hidden: %q", where)
	}
	if _, _, found := authRow(t, s, "other.txt"); !found {
		t.Fatal("unrelated row other.txt was dropped")
	}
}
