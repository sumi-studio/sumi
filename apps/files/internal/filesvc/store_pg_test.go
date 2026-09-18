package filesvc

// PG-backed regression coverage for the ownership/intent boundary
// (consistency findings 79–84). Gated on FILESV_TEST_DSN; CI provides a
// postgres service container. Each test truncates the journal tables so
// the suite is order-independent.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func pgDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("FILESV_TEST_DSN")
	if dsn == "" {
		t.Skip("FILESV_TEST_DSN not set — PG-backed store tests skipped")
	}
	return dsn
}

func resetTables(t *testing.T, dsn string) {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())
	_, err = conn.Exec(context.Background(),
		`TRUNCATE file_version, file_event, file_op, file_receipt, file_freeze, file_cut;
		 DELETE FROM store_meta;
		 SELECT setval('file_version_seq', 1, false)`)
	// Tables may not exist before first migrate — that's fine, the
	// test's NewStore call creates them; retry once after a migrate.
	if err != nil {
		conn.Close(context.Background())
		s, serr := NewStore(context.Background(), dsn, t.TempDir())
		if serr != nil {
			t.Fatalf("bootstrap store: %v", serr)
		}
		s.Close()
		conn, err = pgx.Connect(context.Background(), dsn)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		defer conn.Close(context.Background())
		if _, err := conn.Exec(context.Background(),
			`TRUNCATE file_version, file_event, file_op, file_receipt, file_freeze, file_cut;
			 DELETE FROM store_meta;
			 SELECT setval('file_version_seq', 1, false)`); err != nil {
			t.Fatalf("reset: %v", err)
		}
	}
}

func newPGStore(t *testing.T, dsn, root string) *Store {
	t.Helper()
	s, err := NewStore(context.Background(), dsn, root)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

// fakeDisk is an in-memory filesystem view for reconcile tests. Each
// entry carries an inode + mtime so fingerprints use the real
// "ino:size:mtime:ctime" format — rename identity proof in the reconciler
// exercises the same checks production runs.
type fakeDisk struct {
	files   map[string]string // scope/path -> content ("dir" = dir)
	inos    map[string]uint64
	mts     map[string]int64
	statErr map[string]error // injected unobservable errors (EACCES/ENOTCONN/…)
	next    uint64
	clock   int64
}

func newFakeDisk() *fakeDisk {
	return &fakeDisk{files: map[string]string{},
		inos: map[string]uint64{}, mts: map[string]int64{},
		statErr: map[string]error{}}
}

func (d *fakeDisk) put(scope, path, content string) {
	k := scope + "/" + path
	d.next++
	d.clock++
	d.files[k] = content
	d.inos[k] = d.next
	d.mts[k] = d.clock
}

// edit rewrites content in place — same inode, new mtime — like a
// non-atomic executor-side rewrite after a rename.
func (d *fakeDisk) edit(scope, path, content string) {
	k := scope + "/" + path
	d.clock++
	d.files[k] = content
	d.mts[k] = d.clock
}

// mv moves src and its subtree to dst preserving inode and mtime — the
// identity a real rename keeps.
func (d *fakeDisk) mv(scope, src, dst string) {
	pref := scope + "/" + src
	for k, c := range d.files {
		if k == pref || strings.HasPrefix(k, pref+"/") {
			nk := scope + "/" + dst + k[len(pref):]
			d.files[nk] = c
			d.inos[nk] = d.inos[k]
			d.mts[nk] = d.mts[k]
			delete(d.files, k)
			delete(d.inos, k)
			delete(d.mts, k)
		}
	}
}

func (d *fakeDisk) del(scope, path string) {
	k := scope + "/" + path
	delete(d.files, k)
	delete(d.inos, k)
	delete(d.mts, k)
}

// fail injects an unverifiable observation error; heal clears it.
func (d *fakeDisk) fail(scope, path string, err error) {
	d.statErr[scope+"/"+path] = err
}
func (d *fakeDisk) heal(scope, path string) { delete(d.statErr, scope+"/"+path) }

// setIno forges an inode — simulates kernel inode recycling.
func (d *fakeDisk) setIno(scope, path string, ino uint64) {
	d.inos[scope+"/"+path] = ino
}

// ancestorErr reports ErrNotDir when a path is addressed through a
// non-directory ancestor — a definitive current-path fact (f119).
func (d *fakeDisk) ancestorErr(scope, path string) error {
	segs := strings.Split(path, "/")
	for i := 1; i < len(segs); i++ {
		anc := scope + "/" + strings.Join(segs[:i], "/")
		if c, ok := d.files[anc]; ok && c != "dir" {
			return ErrNotDir
		}
	}
	return nil
}

func (d *fakeDisk) stat(scope, path string) (FileInfo, error) {
	k := scope + "/" + path
	if err, ok := d.statErr[k]; ok {
		return FileInfo{}, err
	}
	if err := d.ancestorErr(scope, path); err != nil {
		return FileInfo{}, err
	}
	c, ok := d.files[k]
	if !ok {
		return FileInfo{}, ErrNotFound
	}
	fp := fmt.Sprintf("%d:%d:%d:%d", d.inos[k], int64(len(c)), d.mts[k], d.mts[k])
	if c == "dir" {
		return FileInfo{Kind: "dir", Fingerprint: fp}, nil
	}
	return FileInfo{Kind: "file", Size: int64(len(c)), Fingerprint: fp}, nil
}

func (d *fakeDisk) hash(scope, path string) (string, error) {
	k := scope + "/" + path
	if err, ok := d.statErr[k]; ok {
		return "", err
	}
	if err := d.ancestorErr(scope, path); err != nil {
		return "", err
	}
	c, ok := d.files[k]
	if !ok || c == "dir" {
		return "", ErrNotFound
	}
	sum := sha256.Sum256([]byte(c))
	return hex.EncodeToString(sum[:]), nil
}

func sha(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func versionOf(t *testing.T, s *Store, scope, path string) (int64, string) {
	t.Helper()
	v, fp, _, err := s.ObservedVersion(context.Background(), scope, path)
	if err != nil {
		t.Fatalf("ObservedVersion %s/%s: %v", scope, path, err)
	}
	return v, fp
}

func insertIntent(t *testing.T, s *Store, it intent) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), s.dbTimeout)
	defer cancel()
	var id int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO file_op (root, owner, scope, op, path, to_path, version, pre_fp, dst_fp, expect_sha, src_kind, pre_oid, dst_oid, dst_sha, names, at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) RETURNING id`,
		s.rootID, it.owner, it.scope, it.op, it.path, it.toPath,
		it.version, it.preFP, it.dstFP, it.expectSHA, it.srcKind,
		it.preOid, it.dstOid, it.dstSHA, mustJSON(it.names), it.at).Scan(&id)
	if err != nil {
		t.Fatalf("insert intent: %v", err)
	}
	return id
}

// loadIntentName returns the journaled private name names[i] of an
// intent row — tests use it to find the slot a live declare generated.
func loadIntentName(t *testing.T, s *Store, id int64, i int) string {
	t.Helper()
	var name string
	err := s.pool.QueryRow(context.Background(),
		fmt.Sprintf(`SELECT names->%d->>'name' FROM file_op WHERE id=$1`, i), id).Scan(&name)
	if err != nil {
		t.Fatalf("load names[%d] of intent %d: %v", i, id, err)
	}
	return name
}

func intentCount(t *testing.T, s *Store) int {
	t.Helper()
	var n int
	err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM file_op`).Scan(&n)
	if err != nil {
		t.Fatalf("count intents: %v", err)
	}
	return n
}

// f80: two live stores on the same database+root — the second must refuse.
func TestPGSingleWriterEnforced(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	root := t.TempDir()
	first := newPGStore(t, dsn, root)

	prev := writerAcquireWait
	writerAcquireWait = 1500 * time.Millisecond
	defer func() { writerAcquireWait = prev }()

	_, err := NewStore(context.Background(), dsn, root)
	if err == nil {
		t.Fatal("second store acquired the writer lock — single-writer not enforced")
	}
	_ = first
}

// f80: a different root on the same DB must be refused even after the
// first owner stops — unrelated roots never share version state.
func TestPGRootBindingRefused(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	root1 := t.TempDir()
	s := newPGStore(t, dsn, root1)
	s.Close()

	if _, err := NewStore(context.Background(), dsn, t.TempDir()); err == nil {
		t.Fatal("store bound DB to a different root was allowed to start")
	}
	// Same root after the first owner stops: clean failover, allowed.
	newPGStore(t, dsn, root1)
}

// Healthy write path on real PG: intent declares and settles inside the
// op, leaving a version row + event and no pending intent.
func TestPGWriteSettlesIntent(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)

	probeAbsent := func() (FileInfo, bool, error) { return FileInfo{}, false, nil }
	ver, _, err := s.WithWrite(context.Background(), "ws", "a.txt", "write",
		IfVersion{Mode: "none"}, sha("hello"), probeAbsent,
		func(it intent) (FileInfo, bool, error) {
			disk.put("ws", "a.txt", "hello")
			it.njDone() // a real fs fn drains its journal record
			return FileInfo{Kind: "file", Fingerprint: "fp-a"}, true, nil
		})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if ver < 1 {
		t.Fatalf("version %d", ver)
	}
	if n := intentCount(t, s); n != 0 {
		t.Fatalf("intent left pending: %d", n)
	}
	if v, fp := versionOf(t, s, "ws", "a.txt"); v != ver || fp != "fp-a" {
		t.Fatalf("version row = %d/%q, want %d/fp-a", v, fp, ver)
	}
}

// f79/f82: a dead owner's write intent whose bytes landed converges —
// the version row and event appear exactly once, settled by the live
// owner via reconcile.
func TestPGReconcileDeadOwnerWrite(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	disk.put("ws", "b.txt", "landed")

	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "b.txt",
		version: 42, expectSHA: sha("landed"),
		at: time.Now().Add(-time.Minute),
	})
	if got := s.Reconcile(context.Background()); got != 1 {
		t.Fatalf("reconcile settled %d", got)
	}
	if v, _ := versionOf(t, s, "ws", "b.txt"); v != 42 {
		t.Fatalf("version = %d, want 42", v)
	}
	if n := intentCount(t, s); n != 0 {
		t.Fatalf("intent not settled: %d", n)
	}
}

// f83: dead-owner write intent whose path now holds DIFFERENT content —
// an external edit landed in the gap. The row must record the version
// but mark the content diverged (fingerprint that can never match), so
// stat/CAS report external_change rather than attributing foreign bytes.
func TestPGReconcileDivergedWrite(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	disk.put("ws", "c.txt", "executor-bytes")

	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "c.txt",
		version: 43, expectSHA: sha("service-bytes"),
		at: time.Now().Add(-time.Minute),
	})
	if got := s.Reconcile(context.Background()); got != 1 {
		t.Fatalf("reconcile settled %d", got)
	}
	v, fp := versionOf(t, s, "ws", "c.txt")
	if v != 43 {
		t.Fatalf("version = %d, want 43", v)
	}
	live, _ := disk.stat("ws", "c.txt")
	if fp == "" || fp == live.Fingerprint {
		t.Fatalf("diverged content recorded with clean fp %q", fp)
	}
	if !hasPrefix(fp, "diverged:") {
		t.Fatalf("fp %q should carry the diverged marker", fp)
	}
	// The intent is RETAINED as a tombstone: this op's bytes may still be
	// in flight and could land late — the intent row is the only evidence
	// that can re-attribute them. It is never erased by a timer.
	if n := intentCount(t, s); n != 1 {
		t.Fatalf("diverged write intent erased: total=%d", n)
	}
}

func hasPrefix(s, p string) bool { return len(s) >= len(p) && s[:len(p)] == p }

// f79: an intent owned by the live instance while its fs goroutine is
// still running must never be touched by reconcile — no drop, no apply.
func TestPGReconcileSkipsLiveInflight(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	disk.put("ws", "d.txt", "pending")

	id := insertIntent(t, s, intent{
		owner: s.owner, scope: "ws", op: "write", path: "d.txt",
		version: 44, expectSHA: sha("pending"),
		at: time.Now(),
	})
	s.inflight.Store(id, struct{}{})
	defer s.inflight.Delete(id)
	if got := s.Reconcile(context.Background()); got != 0 {
		t.Fatalf("reconcile touched a live inflight intent (settled %d)", got)
	}
	if n := intentCount(t, s); n != 1 {
		t.Fatalf("intent vanished: %d", n)
	}
	if v, _ := versionOf(t, s, "ws", "d.txt"); v != 0 {
		t.Fatalf("premature version row %d", v)
	}
}

// f79: a dead owner's intent younger than deadGrace is not judged yet —
// its last-issued fs work may still be landing.
func TestPGReconcileDeadGrace(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	disk.put("ws", "e.txt", "x")

	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "e.txt",
		version: 45, expectSHA: sha("x"), at: time.Now(),
	})
	if got := s.Reconcile(context.Background()); got != 0 {
		t.Fatalf("reconcile judged a fresh dead-owner intent (settled %d)", got)
	}
}

// f81: delayed rename settlement must not steal rows created after the
// intent's fs commit. A recreated source file minted a NEWER version and
// must keep its row; declare-time descendants still move.
func TestPGStaleRenameKeepsNewerRows(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)

	ctx := context.Background()
	// Declare-time state: d1/old.txt (v2). The intent minted v4.
	// Post-commit: d1 recreated with new.txt (v5) by a later op.
	disk.put("ws", "d1", "dir")
	d1Info, _ := disk.stat("ws", "d1")
	preFP := d1Info.Fingerprint
	disk.put("ws", "d1/old.txt", "old-bytes")
	oldInfo, _ := disk.stat("ws", "d1/old.txt")
	_, err := s.pool.Exec(ctx,
		`INSERT INTO file_version (scope, path, version, fp) VALUES
		 ('ws','d1/old.txt',2,$1), ('ws','d1/new.txt',5,'fp-new')`,
		oldInfo.Fingerprint)
	if err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	// Disk after: rename ran (d2 is the moved d1 — same inode, and its
	// recorded member d1/old.txt corroborates at d2/old.txt), and the
	// source path was recreated with a new dir (new inode).
	disk.mv("ws", "d1", "d2")
	disk.put("ws", "d1", "dir") // recreated — new inode, differs from preFP

	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "rename", path: "d1", toPath: "d2",
		version: 4, preFP: preFP, srcKind: "dir", at: time.Now().Add(-time.Minute),
	})
	if got := s.Reconcile(context.Background()); got != 1 {
		t.Fatalf("reconcile settled %d", got)
	}
	// The post-commit file keeps its row at the old source path.
	if v, _ := versionOf(t, s, "ws", "d1/new.txt"); v != 5 {
		t.Fatalf("d1/new.txt version = %d — stale rename stole the row", v)
	}
	// The declare-time descendant moved with the rename — relocated,
	// version preserved.
	if v, _ := versionOf(t, s, "ws", "d2/old.txt"); v != 2 {
		t.Fatalf("d2/old.txt version = %d, want 2", v)
	}
	// The directory's own identity is never claimed: no row is minted at
	// the destination dir.
	if v, _ := versionOf(t, s, "ws", "d2"); v != 0 {
		t.Fatalf("d2 version = %d — dir rows are never relocated", v)
	}
}

// f79 idempotency: settling the same intent twice is a no-op — the second
// apply claims nothing. Simulates a reconciler racing an owner settle.
func TestPGApplyIsIdempotent(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	disk.put("ws", "f.txt", "data")

	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "f.txt",
		version: 46, expectSHA: sha("data"), at: time.Now().Add(-time.Minute),
	})
	it := intent{id: id, owner: "dead-inst", scope: "ws", op: "write",
		path: "f.txt", version: 46}
	if err := s.apply(context.Background(), it, FileInfo{Fingerprint: "fp-f"}, "", false, "applied"); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	err := s.apply(context.Background(), it, FileInfo{Fingerprint: "fp-f"}, "", false, "applied")
	if !errors.Is(err, errIntentSettled) {
		t.Fatalf("second apply = %v, want errIntentSettled", err)
	}
	var n int
	if err := s.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM file_event WHERE path='f.txt'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("duplicate apply emitted %d events", n)
	}
}

// f84: restart must never rewind the version allocator below minted or
// pending versions.
func TestPGSequenceNeverRewinds(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	root := t.TempDir()
	s := newPGStore(t, dsn, root)
	// A pending intent holds a minted version; file_version is empty.
	insertIntent(t, s, intent{
		owner: s.owner, scope: "ws", op: "write", path: "g.txt",
		version: 90, at: time.Now(),
	})
	s.Close() // restart — releases the writer lock

	s2 := newPGStore(t, dsn, root)
	var next int64
	if err := s2.pool.QueryRow(context.Background(),
		`SELECT nextval('file_version_seq')`).Scan(&next); err != nil {
		t.Fatal(err)
	}
	if next <= 90 {
		t.Fatalf("sequence rewound to %d below pending version 90", next)
	}
}

// f102: while the filesystem root cannot be verified (canonical mount
// down — a clean unmount leaves a bare directory where every stat reads
// absent), the reconciler must not judge anything. Pending intents and
// the acknowledged rows they cover survive until a pass where absence is
// provable.
func TestPGReconcileMountGate(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	ctx := context.Background()

	// Acknowledged row + pending intent; disk has nothing (as a bare
	// unmounted mountpoint would report).
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO file_version (scope, path, version, fp) VALUES ('ws','pend.txt',7,'fp-p')`); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "pend.txt",
		version: 8, expectSHA: sha("landed"), at: time.Now().Add(-time.Minute),
	})

	unhealthy := true
	s.SetReconcile(disk.stat, disk.hash, func() error {
		if unhealthy {
			return ErrMountUnavailable
		}
		return nil
	})
	if got := s.Reconcile(ctx); got != 0 {
		t.Fatalf("reconcile judged intents while mount unverifiable: settled %d", got)
	}
	if n := intentCount(t, s); n != 1 {
		t.Fatalf("intent lost during unverifiable pass: %d", n)
	}
	if v, _ := versionOf(t, s, "ws", "pend.txt"); v != 7 {
		t.Fatalf("acknowledged row erased during unverifiable pass: %d", v)
	}
	// Gate clears: the same intent is judged on a trustworthy absence —
	// dropped with its ghost row (proves the gate is not a stall).
	unhealthy = false
	if got := s.Reconcile(ctx); got != 1 {
		t.Fatalf("reconcile settled %d after gate cleared", got)
	}
	if v, _ := versionOf(t, s, "ws", "pend.txt"); v != 0 {
		t.Fatalf("ghost row survived trustworthy absence: %d", v)
	}
}

// f103: the destructive half of settlement is owner-fenced. A process
// whose recorded owner changed (watchdog lag or a mid-pass flip) must not
// delete intents or version rows — the live successor rolls them forward.
func TestPGDropFencedForeignOwner(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	ctx := context.Background()

	if _, err := s.pool.Exec(ctx,
		`INSERT INTO file_version (scope, path, version, fp) VALUES ('ws','keep.txt',5,'fp-k')`); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "keep.txt",
		version: 6, at: time.Now().Add(-time.Minute),
	})
	it := intent{id: id, owner: "dead-inst", scope: "ws", op: "write",
		path: "keep.txt", version: 6}

	// A successor took ownership — this process is displaced.
	if _, err := s.pool.Exec(ctx,
		`UPDATE store_meta SET owner='other-inst' WHERE id`); err != nil {
		t.Fatalf("flip owner: %v", err)
	}
	if s.dropIntent(ctx, it) {
		t.Fatal("dropIntent deleted under a foreign owner")
	}
	if s.dropIntentGhosts(ctx, it, true, "") {
		t.Fatal("dropIntentGhosts deleted under a foreign owner")
	}
	if err := s.apply(ctx, it, FileInfo{Fingerprint: "fp-x"}, "", false, "applied"); err == nil {
		t.Fatal("apply ran under a foreign owner")
	}
	if n := intentCount(t, s); n != 1 {
		t.Fatalf("intent deleted under foreign owner: %d", n)
	}
	if v, _ := versionOf(t, s, "ws", "keep.txt"); v != 5 {
		t.Fatalf("acknowledged row deleted under foreign owner: %d", v)
	}
}

// f104: while any intent overlapping the target paths is still pending,
// the outcome of earlier filesystem work is uncertain — declare refuses
// the conflicting op instead of minting rows a delayed settle would
// strand. Non-overlapping paths proceed normally.
func TestPGDeclareBlockedByPendingIntent(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	ctx := context.Background()

	// A rename intent a→b whose fs outcome is not yet settled.
	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "rename", path: "a", toPath: "b",
		version: 30, srcKind: "dir", at: time.Now(),
	})
	probeAbsent := func() (FileInfo, bool, error) { return FileInfo{}, false, nil }
	noop := func(intent) (FileInfo, bool, error) { return FileInfo{Kind: "file"}, true, nil }

	for _, p := range []string{"a", "a/new.txt", "b", "b/x.txt"} {
		_, _, err := s.WithWrite(ctx, "ws", p, "write",
			IfVersion{Mode: "any"}, "", probeAbsent, noop)
		if !errors.Is(err, ErrUnsettled) {
			t.Fatalf("write %s under pending rename: %v, want ErrUnsettled", p, err)
		}
	}
	// An unrelated path is unaffected.
	ver, _, err := s.WithWrite(ctx, "ws", "c.txt", "write",
		IfVersion{Mode: "none"}, sha("c"), probeAbsent,
		func(intent) (FileInfo, bool, error) {
			disk.put("ws", "c.txt", "c")
			return FileInfo{Kind: "file", Fingerprint: "fp-c"}, true, nil
		})
	if err != nil || ver < 1 {
		t.Fatalf("unrelated write blocked: ver=%d err=%v", ver, err)
	}
	// A rename INTO the pending area is refused too.
	_, _, err = s.Rename(ctx, "ws", "c.txt", "b/moved.txt",
		IfVersion{Mode: "none"}, probeAbsent, probeAbsent, noop)
	if !errors.Is(err, ErrUnsettled) {
		t.Fatalf("rename into pending area: %v, want ErrUnsettled", err)
	}
}

// f106: a pending rename whose destination is not the moved source (an
// external create took the name) must not be applied — no clean version,
// no rename event, and the source's rows are preserved rather than moved
// onto foreign paths.
func TestPGRenameReconcileForeignDest(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	ctx := context.Background()

	// Declare-time source e1 with a recorded row; the rename never ran —
	// e1 was deleted externally and e2 created by someone else.
	disk.put("ws", "e1", "orig")
	srcInfo, _ := disk.stat("ws", "e1")
	preFP := srcInfo.Fingerprint
	disk.del("ws", "e1")
	disk.put("ws", "e2", "foreign-bytes") // different inode
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO file_version (scope, path, version, fp) VALUES ('ws','e1',10,$1)`,
		preFP); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "rename", path: "e1", toPath: "e2",
		version: 20, preFP: preFP, srcKind: "file", expectSHA: sha("orig"),
		at: time.Now().Add(-time.Minute),
	})
	if got := s.Reconcile(ctx); got != 1 {
		t.Fatalf("reconcile settled %d", got)
	}
	if v, _ := versionOf(t, s, "ws", "e2"); v != 0 {
		t.Fatalf("foreign destination minted clean version %d", v)
	}
	if v, _ := versionOf(t, s, "ws", "e1"); v != 10 {
		t.Fatalf("source row lost: version %d", v)
	}
	var n int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM file_event WHERE op='rename' AND path='e2'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("rename event journaled for a rename that never ran (%d)", n)
	}
}

// Positive leg: the recorded source object is observed at the
// destination (ino/size/mtime + bytes). The row is RELOCATED keeping its
// version — no rename is claimed, no version is minted.
func TestPGRenameReconcileIdentityMatch(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	ctx := context.Background()

	disk.put("ws", "f1.txt", "payload")
	srcInfo, _ := disk.stat("ws", "f1.txt")
	preFP := srcInfo.Fingerprint
	disk.mv("ws", "f1.txt", "f2.txt") // the rename landed
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO file_version (scope, path, version, fp) VALUES ('ws','f1.txt',10,$1)`,
		preFP); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "rename", path: "f1.txt", toPath: "f2.txt",
		version: 21, preFP: preFP, srcKind: "file", expectSHA: sha("payload"),
		at: time.Now().Add(-time.Minute),
	})
	if got := s.Reconcile(ctx); got != 1 {
		t.Fatalf("reconcile settled %d", got)
	}
	v, fp := versionOf(t, s, "ws", "f2.txt")
	if v != 10 {
		t.Fatalf("destination version = %d — relocation must keep the row's version 10", v)
	}
	live, _ := disk.stat("ws", "f2.txt")
	if fp != live.Fingerprint {
		t.Fatalf("relocated fp %q, want live %q", fp, live.Fingerprint)
	}
	if v, _ := versionOf(t, s, "ws", "f1.txt"); v != 0 {
		t.Fatalf("source row left behind: %d", v)
	}
	// The journal records an observation (relocate), never a rename.
	var nRel, nRen int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM file_event WHERE op='relocate' AND path='f2.txt'`).Scan(&nRel); err != nil {
		t.Fatal(err)
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM file_event WHERE op='rename'`).Scan(&nRen); err != nil {
		t.Fatal(err)
	}
	if nRel != 1 || nRen != 0 {
		t.Fatalf("events: relocate=%d rename=%d, want 1/0", nRel, nRen)
	}
}

// f106 content leg: destination shares the source's inode but its
// content changed (size/mtime differ) — indistinguishable from inode
// recycling after delete+recreate, so the rename is unproven: the intent
// is dropped, no version/event is claimed, and the source row survives.
func TestPGRenameReconcileDivergedFile(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	ctx := context.Background()

	disk.put("ws", "g1.txt", "payload")
	srcInfo, _ := disk.stat("ws", "g1.txt")
	preFP := srcInfo.Fingerprint
	disk.mv("ws", "g1.txt", "g2.txt")    // rename landed
	disk.edit("ws", "g2.txt", "edited!") // then rewritten in place (same inode)
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO file_version (scope, path, version, fp) VALUES ('ws','g1.txt',10,$1)`,
		preFP); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "rename", path: "g1.txt", toPath: "g2.txt",
		version: 22, preFP: preFP, srcKind: "file", expectSHA: sha("payload"),
		at: time.Now().Add(-time.Minute),
	})
	if got := s.Reconcile(ctx); got != 1 {
		t.Fatalf("reconcile settled %d", got)
	}
	if v, _ := versionOf(t, s, "ws", "g2.txt"); v != 0 {
		t.Fatalf("unproven destination minted version %d", v)
	}
	if v, _ := versionOf(t, s, "ws", "g1.txt"); v != 10 {
		t.Fatalf("source row lost: version %d", v)
	}
}

// resolveIntent marks an intent reconciler-resolved (tombstoned), as a
// first settlement pass would — used to exercise late-landing re-judgment.
func resolveIntent(t *testing.T, s *Store, id int64) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(),
		`UPDATE file_op SET resolved_at=now() WHERE id=$1`, id); err != nil {
		t.Fatalf("resolve intent: %v", err)
	}
}

func pendingCount(t *testing.T, s *Store) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM file_op WHERE resolved_at IS NULL`).Scan(&n); err != nil {
		t.Fatalf("count pending: %v", err)
	}
	return n
}

func stalledCause(t *testing.T, s *Store, id int64) string {
	t.Helper()
	var cause string
	var stalledAt any
	if err := s.pool.QueryRow(context.Background(),
		`SELECT coalesce(last_error,''), stalled_at FROM file_op WHERE id=$1`,
		id).Scan(&cause, &stalledAt); err != nil {
		t.Fatalf("read stall: %v", err)
	}
	if stalledAt == nil {
		t.Fatalf("intent %d not marked stalled", id)
	}
	return cause
}

// f118/B-1: a pending non-rename intent (to_path=”) must block only its
// own subtree — before the fix, the ” leg equality made every pending
// write block every other operation.
func TestPGEmptyLegDoesNotBlockDisjoint(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	ctx := context.Background()

	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "w1.txt",
		version: 50, expectSHA: sha("w1"), at: time.Now(),
	})
	probeAbsent := func() (FileInfo, bool, error) { return FileInfo{}, false, nil }

	// The pending path itself is excluded.
	_, _, err := s.WithWrite(ctx, "ws", "w1.txt", "write",
		IfVersion{Mode: "any"}, "", probeAbsent,
		func(intent) (FileInfo, bool, error) { return FileInfo{}, true, nil })
	if !errors.Is(err, ErrUnsettled) {
		t.Fatalf("write over pending path: %v, want ErrUnsettled", err)
	}
	// Disjoint write, mkdir and rename all proceed.
	if _, _, err := s.WithWrite(ctx, "ws", "w2.txt", "write",
		IfVersion{Mode: "none"}, sha("w2"), probeAbsent,
		func(it intent) (FileInfo, bool, error) {
			disk.put("ws", "w2.txt", "w2")
			it.njDone() // a real fs fn drains its journal record
			return FileInfo{Kind: "file", Fingerprint: "fp-w2"}, true, nil
		}); err != nil {
		t.Fatalf("disjoint write blocked by '' to_path: %v", err)
	}
	if _, _, err := s.WithWrite(ctx, "ws", "d1", "mkdir",
		IfVersion{Mode: "any"}, "dir", probeAbsent,
		func(it intent) (FileInfo, bool, error) {
			disk.put("ws", "d1", "dir")
			it.njDone()
			return FileInfo{Kind: "dir", Fingerprint: "fp-d1"}, true, nil
		}); err != nil {
		t.Fatalf("disjoint mkdir blocked by '' to_path: %v", err)
	}
	if _, _, err := s.Rename(ctx, "ws", "w2.txt", "w3.txt",
		IfVersion{Mode: "any"}, probeAbsent, probeAbsent,
		func(it intent) (FileInfo, bool, error) {
			disk.mv("ws", "w2.txt", "w3.txt")
			it.njDone()
			return FileInfo{Kind: "file", Fingerprint: "fp-w3"}, true, nil
		}); err != nil {
		t.Fatalf("disjoint rename blocked by '' to_path: %v", err)
	}
}

// F-RA-5/f120: a mutation that COMMITTED but whose observation failed must
// keep its intent — the error class alone must never erase the record of
// possibly-committed work.
func TestPGCommittedButUnobservedKeepsIntent(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	ctx := context.Background()

	probeAbsent := func() (FileInfo, bool, error) { return FileInfo{}, false, nil }
	_, _, err := s.WithWrite(ctx, "ws", "cx.txt", "write",
		IfVersion{Mode: "none"}, sha("cx"), probeAbsent,
		func(intent) (FileInfo, bool, error) {
			disk.put("ws", "cx.txt", "cx")
			// Publish committed; the trailing stat failed.
			return FileInfo{}, true, ErrUnavailable
		})
	if err == nil {
		t.Fatal("unobserved commit reported clean success")
	}
	if n := pendingCount(t, s); n != 1 {
		t.Fatalf("committed-but-unobserved intent erased: pending=%d", n)
	}
	if got := s.Reconcile(ctx); got != 1 {
		t.Fatalf("reconcile settled %d", got)
	}
	if v, _ := versionOf(t, s, "ws", "cx.txt"); v == 0 {
		t.Fatal("landed write never minted its version")
	}
}

// F-RA-5 converse: a DEFINITIVE pre-commit rejection (permission) drops the
// intent cleanly — no pending residue, no version row.
func TestPGDefinitivePreCommitDropsIntent(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	ctx := context.Background()

	probeAbsent := func() (FileInfo, bool, error) { return FileInfo{}, false, nil }
	_, _, err := s.WithWrite(ctx, "ws", "deny.txt", "write",
		IfVersion{Mode: "none"}, sha("x"), probeAbsent,
		func(intent) (FileInfo, bool, error) {
			return FileInfo{}, false, ErrAccess // rejected before publish
		})
	if !errors.Is(err, ErrAccess) {
		t.Fatalf("write err = %v, want ErrAccess", err)
	}
	if n := intentCount(t, s); n != 0 {
		t.Fatalf("pre-commit rejection left %d intents", n)
	}
	if v, _ := versionOf(t, s, "ws", "deny.txt"); v != 0 {
		t.Fatalf("rejected write minted version %d", v)
	}
}

// F-RA-5 ambiguity leg: a transport-class error WITHOUT a proven commit is
// not a rejection either — the outcome is unknown and the intent survives.
func TestPGAmbiguousErrorKeepsIntent(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	ctx := context.Background()

	probeAbsent := func() (FileInfo, bool, error) { return FileInfo{}, false, nil }
	_, _, err := s.WithWrite(ctx, "ws", "amb.txt", "write",
		IfVersion{Mode: "none"}, sha("amb"), probeAbsent,
		func(intent) (FileInfo, bool, error) {
			return FileInfo{}, false, ErrUnavailable // reply may be lost
		})
	if err == nil {
		t.Fatal("ambiguous fs error reported as success")
	}
	if n := pendingCount(t, s); n != 1 {
		t.Fatalf("ambiguous intent erased: pending=%d", n)
	}
	// Nothing landed: reconcile observes provable absence and resolves it.
	if got := s.Reconcile(ctx); got != 1 {
		t.Fatalf("reconcile settled %d", got)
	}
	if v, _ := versionOf(t, s, "ws", "amb.txt"); v != 0 {
		t.Fatalf("ghost version minted for never-landed write: %d", v)
	}
	if n := intentCount(t, s); n != 1 {
		t.Fatalf("resolved intent not tombstoned: total=%d", n)
	}
}

// f119/B-2: an intent whose path cannot be observed (EACCES) is durable
// uncertainty — it stays pending with its cause recorded, blocks only its
// own subtree, and never resolves on a timer. Restoring observability
// settles it truthfully.
func TestPGUnverifiableStallsDurably(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	ctx := context.Background()

	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "jam.txt",
		version: 60, expectSHA: sha("jam"), at: time.Now().Add(-time.Hour),
	})
	disk.fail("ws", "jam.txt", ErrAccess)

	for i := 0; i < 3; i++ {
		if got := s.Reconcile(ctx); got != 0 {
			t.Fatalf("reconcile resolved an unverifiable intent: %d", got)
		}
	}
	if cause := stalledCause(t, s, id); !strings.Contains(cause, "permission") {
		t.Fatalf("stall cause %q does not record the access failure", cause)
	}
	// Conflicting op sees the cause; disjoint ops proceed.
	probeAbsent := func() (FileInfo, bool, error) { return FileInfo{}, false, nil }
	_, _, err := s.WithWrite(ctx, "ws", "jam.txt", "write",
		IfVersion{Mode: "any"}, "", probeAbsent,
		func(intent) (FileInfo, bool, error) { return FileInfo{}, true, nil })
	if !errors.Is(err, ErrUnsettled) || !strings.Contains(err.Error(), "permission") {
		t.Fatalf("blocked op lacks cause: %v", err)
	}
	if _, _, err := s.WithWrite(ctx, "ws", "free.txt", "write",
		IfVersion{Mode: "none"}, sha("f"), probeAbsent,
		func(intent) (FileInfo, bool, error) {
			disk.put("ws", "free.txt", "f")
			return FileInfo{Kind: "file", Fingerprint: "fp-f"}, true, nil
		}); err != nil {
		t.Fatalf("disjoint write blocked by stalled intent: %v", err)
	}
	// Observability restored: the landed bytes settle truthfully.
	disk.heal("ws", "jam.txt")
	disk.put("ws", "jam.txt", "jam")
	if got := s.Reconcile(ctx); got != 1 {
		t.Fatalf("reconcile settled %d after heal", got)
	}
	if v, _ := versionOf(t, s, "ws", "jam.txt"); v != 60 {
		t.Fatalf("version = %d, want 60", v)
	}
}

// f119/B-2 converse: ENOTDIR is a definitive CURRENT-path fact — a path
// addressed through a non-directory cannot exist as addressed. The intent
// settles by observed absence (no invented causality).
func TestPGNotDirSettlesAbsence(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	ctx := context.Background()

	// A file where the intent expects a directory ancestor.
	disk.put("ws", "f", "a-file-not-a-dir")
	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "f/inner.txt",
		version: 61, expectSHA: sha("inner"), at: time.Now().Add(-time.Hour),
	})
	if got := s.Reconcile(ctx); got != 1 {
		t.Fatalf("reconcile settled %d", got)
	}
	if n := pendingCount(t, s); n != 0 {
		t.Fatalf("ENOTDIR intent still pending: %d", n)
	}
	if v, _ := versionOf(t, s, "ws", "f/inner.txt"); v != 0 {
		t.Fatalf("version minted under a non-directory: %d", v)
	}
}

// Remove settled by observed absence emits NO remove event — we observed
// the path absent; the journal must not claim the service removed it.
func TestPGRemoveAbsentNoEvent(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	ctx := context.Background()

	if _, err := s.pool.Exec(ctx,
		`INSERT INTO file_version (scope, path, version, fp) VALUES ('ws','gone.txt',9,'fp-g')`); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "remove", path: "gone.txt",
		version: 62, at: time.Now().Add(-time.Minute),
	})
	if got := s.Reconcile(ctx); got != 1 {
		t.Fatalf("reconcile settled %d", got)
	}
	if v, _ := versionOf(t, s, "ws", "gone.txt"); v != 0 {
		t.Fatalf("ghost row for absent path survived: %d", v)
	}
	var n int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM file_event WHERE op='remove' AND path='gone.txt'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("remove event invented for unperformed removal (%d)", n)
	}
}

// F-RA-4/f122: the intent is marked inflight BEFORE its row commits, so a
// reconcile pass that observes the row while the fs op still runs can
// never misjudge it as abandoned.
func TestPGInflightMarkedBeforeVisible(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	ctx := context.Background()

	probeAbsent := func() (FileInfo, bool, error) { return FileInfo{}, false, nil }
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, _, err := s.WithWrite(ctx, "ws", "race.txt", "write",
			IfVersion{Mode: "none"}, sha("r"), probeAbsent,
			func(intent) (FileInfo, bool, error) {
				close(started)
				<-release
				disk.put("ws", "race.txt", "r")
				return FileInfo{Kind: "file", Fingerprint: "fp-r"}, true, nil
			})
		done <- err
	}()
	<-started
	// The intent row is committed and visible NOW; a pass must skip it.
	if got := s.Reconcile(ctx); got != 0 {
		t.Fatalf("reconcile judged a live inflight intent: settled %d", got)
	}
	if n := pendingCount(t, s); n != 1 {
		t.Fatalf("inflight intent vanished mid-flight: %d", n)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("write: %v", err)
	}
	if v, _ := versionOf(t, s, "ws", "race.txt"); v == 0 {
		t.Fatal("completed write minted no version")
	}
}

// f106: a directory whose destination recycles the source inode but holds
// FOREIGN members is not the moved tree — no adoption, rows preserved.
func TestPGRenameDirRecycledInodeForeign(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	ctx := context.Background()

	disk.put("ws", "h1", "dir")
	h1, _ := disk.stat("ws", "h1")
	preFP := h1.Fingerprint
	disk.put("ws", "h1/k.txt", "kid")
	kid, _ := disk.stat("ws", "h1/k.txt")
	_, err := s.pool.Exec(ctx,
		`INSERT INTO file_version (scope, path, version, fp) VALUES ('ws','h1/k.txt',12,$1)`,
		kid.Fingerprint)
	if err != nil {
		t.Fatalf("seed row: %v", err)
	}
	// Source tree deleted; a DIFFERENT dir takes the name and is forged to
	// the source's inode (recycling). Its child has different fingerprints.
	disk.del("ws", "h1/k.txt")
	disk.del("ws", "h1")
	disk.put("ws", "h2", "dir")
	disk.put("ws", "h2/k.txt", "stranger") // same name, foreign inode+times
	disk.setIno("ws", "h2", h1Ino(t, preFP))

	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "rename", path: "h1", toPath: "h2",
		version: 23, preFP: preFP, srcKind: "dir", at: time.Now().Add(-time.Minute),
	})
	if got := s.Reconcile(ctx); got != 1 {
		t.Fatalf("reconcile settled %d", got)
	}
	if v, _ := versionOf(t, s, "ws", "h2"); v != 0 {
		t.Fatalf("foreign dir adopted as moved source: version %d", v)
	}
	if v, _ := versionOf(t, s, "ws", "h2/k.txt"); v != 0 {
		t.Fatalf("foreign member laundered: version %d", v)
	}
	if v, _ := versionOf(t, s, "ws", "h1/k.txt"); v != 12 {
		t.Fatalf("source member row lost: %d", v)
	}
}

func h1Ino(t *testing.T, fp string) uint64 {
	t.Helper()
	ino, _, _, ok := fpParts(fp)
	if !ok {
		t.Fatalf("bad fp %q", fp)
	}
	var n uint64
	if _, err := fmt.Sscanf(ino, "%d", &n); err != nil {
		t.Fatalf("ino %q: %v", ino, err)
	}
	return n
}

// f106: a dir with NO recorded members to corroborate stays unproven —
// an inode alone never proves a whole directory moved.
func TestPGRenameDirNoMembersUnproven(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	ctx := context.Background()

	disk.put("ws", "z1", "dir")
	z1, _ := disk.stat("ws", "z1")
	preFP := z1.Fingerprint
	disk.mv("ws", "z1", "z2") // genuinely moved — but no rows recorded members
	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "rename", path: "z1", toPath: "z2",
		version: 24, preFP: preFP, srcKind: "dir", at: time.Now().Add(-time.Minute),
	})
	if got := s.Reconcile(ctx); got != 1 {
		t.Fatalf("reconcile settled %d", got)
	}
	if v, _ := versionOf(t, s, "ws", "z2"); v != 0 {
		t.Fatalf("zero-evidence dir adopted: version %d", v)
	}
}

// f104 tombstone re-judgment: a resolved write intent whose bytes land
// LATE still mints its version — evidence retention enables truthful
// roll-forward instead of stranding.
func TestPGTombstoneLateWriteRollForward(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	ctx := context.Background()

	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "late.txt",
		version: 70, expectSHA: sha("late"), at: time.Now().Add(-time.Hour),
	})
	// First pass: absent — intent tombstoned (resolved, evidence kept).
	if got := s.Reconcile(ctx); got != 1 {
		t.Fatalf("first reconcile settled %d", got)
	}
	_ = id
	if n := intentCount(t, s); n != 1 {
		t.Fatalf("tombstone erased evidence: total=%d", n)
	}
	// The write lands after the drop. Tombstone re-judgment is rate-
	// limited; reset the scan clock to stand in for the interval elapsing.
	disk.put("ws", "late.txt", "late")
	s.lastTombScan.Store(0)
	if got := s.Reconcile(ctx); got != 1 {
		t.Fatalf("tombstone re-judgment settled %d", got)
	}
	if v, _ := versionOf(t, s, "ws", "late.txt"); v != 70 {
		t.Fatalf("late-landed write not rolled forward: %d", v)
	}
}

// f104 tombstone rename: a resolved rename whose fs effect lands late is
// rolled forward, and rows minted post-drop whose recorded fp proves they
// were physically carried by the move are rescued to the destination.
func TestPGTombstoneLateRenameRescue(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	ctx := context.Background()

	disk.put("ws", "s1", "dir")
	s1, _ := disk.stat("ws", "s1")
	disk.put("ws", "s1/k.txt", "kid")
	kid, _ := disk.stat("ws", "s1/k.txt")
	disk.put("ws", "s1/post.txt", "post")
	post, _ := disk.stat("ws", "s1/post.txt")
	// k.txt predates the intent (v2 < 5); post.txt was minted AFTER the
	// intent dropped (v9) — the fs move still carried it physically.
	_, err := s.pool.Exec(ctx,
		`INSERT INTO file_version (scope, path, version, fp) VALUES
		 ('ws','s1/k.txt',2,$1), ('ws','s1/post.txt',9,$2)`,
		kid.Fingerprint, post.Fingerprint)
	if err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	disk.mv("ws", "s1", "s2") // the delayed fs effect lands
	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "rename", path: "s1", toPath: "s2",
		version: 5, preFP: s1.Fingerprint, srcKind: "dir",
		at: time.Now().Add(-time.Hour),
	})
	resolveIntent(t, s, id) // tombstoned — first pass judged it while absent

	s.lastTombScan.Store(0) // stand in for tombstoneScanInterval elapsing
	if got := s.Reconcile(ctx); got != 1 {
		t.Fatalf("tombstone re-judgment settled %d", got)
	}
	if v, _ := versionOf(t, s, "ws", "s2"); v != 0 {
		t.Fatalf("dir body row claimed: %d — dirs are never relocated", v)
	}
	if v, _ := versionOf(t, s, "ws", "s2/k.txt"); v != 2 {
		t.Fatalf("pre-intent member version = %d, want 2", v)
	}
	if v, _ := versionOf(t, s, "ws", "s2/post.txt"); v != 9 {
		t.Fatalf("post-drop member not rescued: %d", v)
	}
	if v, _ := versionOf(t, s, "ws", "s1/post.txt"); v != 0 {
		t.Fatalf("rescued row still at old path: %d", v)
	}
}

// Fable defect: a hardlink (or a symlink that stats as the source) makes
// the destination indistinguishable from the moved object by inode,
// size, mtime AND content. The source is still present with its
// declare-time fingerprint, so the intent is tombstoned — the source
// row is never deleted and no rename/relocate is claimed.
func TestPGDestStatsAsSourceNotClaimed(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	ctx := context.Background()

	disk.put("ws", "a.txt", "payload")
	srcInfo, _ := disk.stat("ws", "a.txt")
	// `ln a.txt b.txt` / `ln -s a.txt b.txt`: stat(b.txt) returns a.txt's
	// exact metadata — inode, times, bytes all identical, no rename ran.
	disk.put("ws", "b.txt", "payload")
	ino, _, _, _ := fpParts(srcInfo.Fingerprint)
	var srcIno uint64
	if _, err := fmt.Sscanf(ino, "%d", &srcIno); err != nil {
		t.Fatal(err)
	}
	disk.setIno("ws", "b.txt", srcIno)
	// forge identical mtime so the whole fingerprint matches
	disk.mts["ws/b.txt"] = disk.mts["ws/a.txt"]

	if _, err := s.pool.Exec(ctx,
		`INSERT INTO file_version (scope, path, version, fp) VALUES ('ws','a.txt',10,$1)`,
		srcInfo.Fingerprint); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "rename", path: "a.txt", toPath: "b.txt",
		version: 30, preFP: srcInfo.Fingerprint, srcKind: "file",
		expectSHA: sha("payload"), at: time.Now().Add(-time.Minute),
	})
	if got := s.Reconcile(ctx); got != 1 {
		t.Fatalf("reconcile settled %d", got)
	}
	if v, _ := versionOf(t, s, "ws", "a.txt"); v != 10 {
		t.Fatalf("still-present source row lost: %d", v)
	}
	if v, _ := versionOf(t, s, "ws", "b.txt"); v != 0 {
		t.Fatalf("foreign destination claimed: %d", v)
	}
	var n int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM file_event WHERE op IN ('rename','relocate')`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("move events journaled for a rename that never ran (%d)", n)
	}
	if n := intentCount(t, s); n != 1 {
		t.Fatalf("tombstone erased: %d", n)
	}
}

// Source recreated after a genuine move: the recorded member relocates
// to the destination on exact fingerprint evidence; a row describing the
// RECREATED source object (different fp) stays at the source path and is
// never stolen into the moved subtree.
func TestPGReconcileSourceRecreatedKeepsRow(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	ctx := context.Background()

	disk.put("ws", "r1", "dir")
	r1, _ := disk.stat("ws", "r1")
	disk.put("ws", "r1/k.txt", "kid")
	kid, _ := disk.stat("ws", "r1/k.txt")
	_, err := s.pool.Exec(ctx,
		`INSERT INTO file_version (scope, path, version, fp) VALUES
		 ('ws','r1/k.txt',3,$1), ('ws','r1/late.txt',8,'fp-recreated')`,
		kid.Fingerprint)
	if err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	disk.mv("ws", "r1", "r2")            // genuine move
	disk.put("ws", "r1", "dir")          // source dir recreated
	disk.put("ws", "r1/late.txt", "new") // and a NEW object at a recorded path

	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "rename", path: "r1", toPath: "r2",
		version: 6, preFP: r1.Fingerprint, srcKind: "dir",
		at: time.Now().Add(-time.Minute),
	})
	if got := s.Reconcile(ctx); got != 1 {
		t.Fatalf("reconcile settled %d", got)
	}
	if v, _ := versionOf(t, s, "ws", "r2/k.txt"); v != 3 {
		t.Fatalf("moved member version = %d, want 3", v)
	}
	if v, _ := versionOf(t, s, "ws", "r1/late.txt"); v != 8 {
		t.Fatalf("recreated-source row stolen: %d", v)
	}
	if v, _ := versionOf(t, s, "ws", "r2/late.txt"); v != 0 {
		t.Fatalf("row relocated despite live source path: %d", v)
	}
	var n int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM file_event WHERE op='rename'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("reconciler emitted a rename event (%d)", n)
	}
}

// A tombstone is never erased by age: a reconciled intent older than any
// horizon still carries its evidence and is still re-judged.
func TestPGTombstoneNeverSwept(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	disk := newFakeDisk()
	s.SetReconcile(disk.stat, disk.hash, nil)
	ctx := context.Background()

	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "old.txt",
		version: 40, expectSHA: sha("old"), at: time.Now().Add(-72 * time.Hour),
	})
	resolveIntent(t, s, id)
	// Age the tombstone past any horizon.
	if _, err := s.pool.Exec(ctx,
		`UPDATE file_op SET resolved_at = now() - interval '72 hours' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	s.lastTombScan.Store(0)
	s.lastTombScanCold.Store(0)
	s.Reconcile(ctx)
	if n := intentCount(t, s); n != 1 {
		t.Fatalf("aged tombstone swept: %d intents remain", n)
	}
	// And it is still re-judged: the effect landing NOW is applied.
	disk.put("ws", "old.txt", "old")
	s.lastTombScanCold.Store(0)
	if got := s.Reconcile(ctx); got != 1 {
		t.Fatalf("aged tombstone not re-judged (settled %d)", got)
	}
	if v, _ := versionOf(t, s, "ws", "old.txt"); v != 40 {
		t.Fatalf("late-landed write on aged tombstone not applied: %d", v)
	}
}

// applyUntilSettled: a live process that observed fs success retries DB
// persistence; a foreign owner (lost writer lock) ends the retry and the
// intent is left for the owner.
func TestPGApplyUntilSettledForeignOwner(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	s := newPGStore(t, dsn, t.TempDir())
	ctx := context.Background()

	id := insertIntent(t, s, intent{
		owner: s.owner, scope: "ws", op: "write", path: "w.txt",
		version: 50, expectSHA: sha("w"), at: time.Now(),
	})
	// Another instance takes the writer lock.
	if _, err := s.pool.Exec(ctx, `UPDATE store_meta SET owner='other-inst'`); err != nil {
		t.Fatal(err)
	}
	err := s.applyUntilSettled(intent{
		id: id, owner: s.owner, scope: "ws", op: "write", path: "w.txt",
		version: 50, expectSHA: sha("w"),
	}, FileInfo{Fingerprint: "fp-w"}, sha("w"))
	if !errors.Is(err, errForeignOwner) {
		t.Fatalf("applyUntilSettled = %v, want foreign owner", err)
	}
	if n := intentCount(t, s); n != 1 {
		t.Fatalf("foreign-owned intent consumed: %d", n)
	}
}

// --- Verified effects: the stale-op durability repair ------------------
//
// The sequence these tests model: an op declares an intent while the old
// owner lives; ownership moves; the intent settles as never-landed; a
// successor acknowledges newer content at the path; THEN the retired
// op's queued filesystem effect lands. The verified effect must undo
// itself — never destroy the successor's acknowledged bytes.

// staleWriteEffect runs the retired op's real fs effect with its
// declare-time evidence — exactly what its paused goroutine does on
// resume.
func staleWriteEffect(p *posixRoot, it intent, body string) (bool, error) {
	_, committed, err := p.atomicWrite(it.scope, it.path, []byte(body), false, it)
	return committed, err
}

// Full ownership-loss sequence on real fs + real PG: the successor's
// acknowledged write survives the late-landing stale effect byte-exact,
// at its normal path, and the stale intent cannot regress the row.
func TestPGStaleWritePreservesSuccessor(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	probeOf := func(p *posixRoot, scope, path string) FPProbe {
		return func() (FileInfo, bool, error) {
			info, err := p.stat(scope, path)
			if errors.Is(err, ErrNotFound) || errors.Is(err, ErrNotDir) {
				return FileInfo{}, false, nil
			}
			return info, err == nil, err
		}
	}
	write := func(s *Store, content string) {
		t.Helper()
		sum := sha256.Sum256([]byte(content))
		_, _, err := s.WithWrite(ctx, "ws", "a.txt", "write",
			IfVersion{Mode: "any"}, hex.EncodeToString(sum[:]),
			probeOf(root, "ws", "a.txt"),
			func(it intent) (FileInfo, bool, error) {
				return root.atomicWrite("ws", "a.txt", []byte(content), false, it)
			})
		if err != nil {
			t.Fatalf("write %q: %v", content, err)
		}
	}

	s1 := newPGStore(t, dsn, dir)
	write(s1, "old")
	fpOld := durFP(t, root, "ws", "a.txt")

	// The stale op declares while "old" is current — then its actor is
	// paused (its fs effect stays queued).
	stale, err := s1.declare(ctx, "ws", "write", "a.txt", "",
		IfVersion{Mode: "any"}, sha("stale"), OpIdentity{}, probeOf(root, "ws", "a.txt"),
		probeOf(root, "ws", "a.txt"))
	if err != nil {
		t.Fatalf("stale declare: %v", err)
	}
	if stale.dstFP != fpOld {
		t.Fatalf("stale dst_fp = %q, want declare-time %q", stale.dstFP, fpOld)
	}
	// Ownership is lost; a successor takes the store.
	s1.Close()
	s2 := newPGStore(t, dsn, dir)
	s2.SetReconcileView(func(context.Context) (ReconView, error) { return root.pin(false) })
	// The dead owner's intent ages past the grace window and settles as
	// never-landed (the path still holds the declare-time object).
	if _, err := s2.pool.Exec(ctx,
		`UPDATE file_op SET at = now() - interval '60 seconds' WHERE id=$1`, stale.id); err != nil {
		t.Fatal(err)
	}
	s2.Reconcile(ctx)
	// Age past the tombstone hot window so the successor is admitted.
	if _, err := s2.pool.Exec(ctx,
		`UPDATE file_op SET resolved_at = now() - interval '60 seconds' WHERE id=$1`, stale.id); err != nil {
		t.Fatal(err)
	}
	// The successor acknowledges newer content.
	write(s2, "new")
	v2, fpV2 := versionOf(t, s2, "ws", "a.txt")
	// THE STALE EFFECT LANDS.
	committed, ferr := staleWriteEffect(root, stale, "stale")
	if !errors.Is(ferr, ErrExternalChange) || committed {
		t.Fatalf("stale effect = (committed=%v, %v), want (false, external_change)", committed, ferr)
	}
	// The successor's acknowledged bytes survive, at the normal path.
	if got := durRead(t, dir, "ws/a.txt"); got != "new" {
		t.Fatalf("a.txt = %q — stale effect destroyed the acknowledged save", got)
	}
	if durExists(t, dir, "ws/"+opStagePrefix+strconv.FormatInt(stale.id, 10)) {
		t.Fatal("stale staged slot left behind")
	}
	// The version row still describes the successor's save.
	if v, fp := versionOf(t, s2, "ws", "a.txt"); v != v2 || fp != fpV2 {
		t.Fatalf("row regressed to (%d,%q), want (%d,%q)", v, fp, v2, fpV2)
	}
}

// The same sequence, but the retired process dies BETWEEN the exchange
// and the verdict.
// A dead write whose authored bytes reached the path (lost reply)
// commits by disk verdict; the displaced occupant it captured is not
// provably the declared displaced object (the journaled dst_fp does not
// match) — it is preserved at a visible sibling, never destroyed and
// never moved over the occupant.
func TestPGReconcileCompletesDeadUndo(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	s.SetReconcileView(func(context.Context) (ReconView, error) { return root.pin(false) })
	if err := os.MkdirAll(dir+"/ws", 0o755); err != nil {
		t.Fatal(err)
	}
	// The dead op's own content at the path; the displaced object sits
	// at a private name its journal does not own (a dead pre-journal
	// protocol's residue — the sweep adopts it).
	if err := os.WriteFile(dir+"/ws/a.txt", []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "a.txt",
		version: 70, preFP: "0:0:0:0", dstFP: "9:3:1:1",
		expectSHA: sha("stale"), at: time.Now().Add(-time.Hour),
	})
	staged := dir + "/ws/" + opStagePrefix + strconv.FormatInt(id, 10)
	if err := os.WriteFile(staged, []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}
	s.Reconcile(ctx)
	// The dead op's row is gone by the time the sweep lists the parked
	// name, so the first sighting only defers; a persistent unowned
	// private name is adopted on the next sweep.
	s.lastStageSweep.Store(0)
	s.Reconcile(ctx)
	// The write's authored bytes are at the path: the effect landed and
	// the intent commits. The private name must be drained.
	if got := durRead(t, dir, "ws/a.txt"); got != "stale" {
		t.Fatalf("a.txt = %q — landed write not committed by disk verdict", got)
	}
	if durExists(t, dir, "ws/"+opStagePrefix+strconv.FormatInt(id, 10)) {
		t.Fatal("private name not settled")
	}
	// The displaced object was never proven ours to destroy — it is
	// preserved at a visible name where people can read and delete it.
	if where := scanDirFor(t, dir, "ws", []byte("new")); where == "" {
		t.Fatal("displaced successor content destroyed — must be preserved visibly")
	}
}

// A parked object that IS recorded content meets an occupied home: the
// squatter is a live public object and is never evicted — the recorded
// object surfaces at a visible sibling name with a readable row, and
// the squatter's bytes are never touched.
func TestPGReconcileRestoresRecordedOverSquatter(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	s.SetReconcileView(func(context.Context) (ReconView, error) { return root.pin(false) })
	probeOf := func(path string) FPProbe {
		return func() (FileInfo, bool, error) {
			info, err := root.stat("ws", path)
			if errors.Is(err, ErrNotFound) || errors.Is(err, ErrNotDir) {
				return FileInfo{}, false, nil
			}
			return info, err == nil, err
		}
	}
	// Acknowledged content at the path — row v1, fp F1.
	_, _, err = s.WithWrite(ctx, "ws", "a.txt", "write",
		IfVersion{Mode: "any"}, sha("v1"), probeOf("a.txt"),
		func(it intent) (FileInfo, bool, error) {
			return root.atomicWrite("ws", "a.txt", []byte("v1"), false, it)
		})
	if err != nil {
		t.Fatalf("write v1: %v", err)
	}
	// The acknowledged object is parked at a stale intent's slot (a dead
	// op's undo captured it); a squatter occupies the name.
	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "a.txt",
		version: 70, preFP: "0:0:0:0", dstFP: "9:9:9",
		expectSHA: sha("stale"), at: time.Now().Add(-time.Hour),
	})
	staged := dir + "/ws/" + opStagePrefix + strconv.FormatInt(id, 10)
	if err := os.Rename(dir+"/ws/a.txt", staged); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/ws/a.txt", []byte("squatter"), 0o644); err != nil {
		t.Fatal(err)
	}
	s.Reconcile(ctx)
	// The squatter is a live public object: recovery never moves it.
	if got := durRead(t, dir, "ws/a.txt"); got != "squatter" {
		t.Fatalf("a.txt = %q — foreign occupant must not be evicted", got)
	}
	// The recorded object surfaces at a visible sibling, preserved and
	// readable — never deleted, never moved over the occupant.
	if where := scanDirFor(t, dir, "ws", []byte("v1")); where == "" ||
		strings.HasPrefix(where, opStagePrefix) {
		t.Fatalf("recorded content lost or left private: %q", where)
	}
}

// A sealed (-q-) name holding recorded content must be drained WITHOUT
// anything ever writing into the sealed name: recovery captures the
// object into a fresh unsealed name and surfaces it visibly — the
// squatter keeps the occupied home. Through real Reconcile + real
// file_version rows — not a DB-free branch.
func TestPGReconcileDrainsSealedObject(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	s.SetReconcileView(func(context.Context) (ReconView, error) { return root.pin(false) })
	probeOf := func(path string) FPProbe {
		return func() (FileInfo, bool, error) {
			info, err := root.stat("ws", path)
			if errors.Is(err, ErrNotFound) || errors.Is(err, ErrNotDir) {
				return FileInfo{}, false, nil
			}
			return info, err == nil, err
		}
	}
	// Acknowledged content at the path — row v1 records its fingerprint.
	if _, _, err = s.WithWrite(ctx, "ws", "a.txt", "write",
		IfVersion{Mode: "any"}, sha("v1"), probeOf("a.txt"),
		func(it intent) (FileInfo, bool, error) {
			return root.atomicWrite("ws", "a.txt", []byte("v1"), false, it)
		}); err != nil {
		t.Fatalf("write v1: %v", err)
	}
	// A dead intent's quarantined object IS the acknowledged file
	// (moved, so the recorded fp3 matches); a squatter holds the name.
	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "a.txt",
		version: 70, preFP: "0:0:0:0", dstFP: "9:9:9",
		expectSHA: sha("stale"), at: time.Now().Add(-time.Hour),
	})
	qrel := opStagePrefix + strconv.FormatInt(id, 10) + "-q-ab12cd"
	if err := os.Rename(dir+"/ws/a.txt", dir+"/ws/"+qrel); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/ws/a.txt", []byte("squatter"), 0o644); err != nil {
		t.Fatal(err)
	}
	s.Reconcile(ctx)
	// The squatter keeps its public name — never evicted, never moved.
	if got := durRead(t, dir, "ws/a.txt"); got != "squatter" {
		t.Fatalf("a.txt = %q — foreign occupant must not be evicted", got)
	}
	if durExists(t, dir, "ws/"+qrel) {
		t.Fatal("sealed name still occupied after the drain")
	}
	// The recorded object was moved OUT of the sealed name and surfaced
	// at a visible name where it stays readable.
	if where := scanDirFor(t, dir, "ws", []byte("v1")); where == "" ||
		strings.HasPrefix(where, opStagePrefix) {
		t.Fatalf("sealed recorded object lost or left private: %q", where)
	}
	// Convergence: a second pass changes nothing and deletes nothing.
	s.Reconcile(ctx)
	if got := durRead(t, dir, "ws/a.txt"); got != "squatter" {
		t.Fatalf("a.txt = %q after second pass", got)
	}
	if where := scanDirFor(t, dir, "ws", []byte("v1")); where == "" ||
		strings.HasPrefix(where, opStagePrefix) {
		t.Fatalf("recorded object lost after second pass: %q", where)
	}
}

// A private base slot occupied by an unattributable foreign object must
// NOT stall the drain forever: every occupied private name is captured
// and surfaced independently, so the recorded sealed object and the
// foreign base occupant each reach a visible name — none is destroyed
// or left stranded.
func TestPGReconcileDrainsSealedBaseSlotOccupied(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	s.SetReconcileView(func(context.Context) (ReconView, error) { return root.pin(false) })
	probeOf := func(path string) FPProbe {
		return func() (FileInfo, bool, error) {
			info, err := root.stat("ws", path)
			if errors.Is(err, ErrNotFound) || errors.Is(err, ErrNotDir) {
				return FileInfo{}, false, nil
			}
			return info, err == nil, err
		}
	}
	if _, _, err = s.WithWrite(ctx, "ws", "a.txt", "write",
		IfVersion{Mode: "any"}, sha("v1"), probeOf("a.txt"),
		func(it intent) (FileInfo, bool, error) {
			return root.atomicWrite("ws", "a.txt", []byte("v1"), false, it)
		}); err != nil {
		t.Fatalf("write v1: %v", err)
	}
	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "a.txt",
		version: 70, preFP: "0:0:0:0", dstFP: "9:9:9",
		expectSHA: sha("stale"), at: time.Now().Add(-time.Hour),
	})
	slot := opStagePrefix + strconv.FormatInt(id, 10)
	qrel := slot + "-q-ab12cd"
	// Recorded content sealed; name squatted; BASE SLOT permanently
	// occupied by an unattributable foreign object.
	if err := os.Rename(dir+"/ws/a.txt", dir+"/ws/"+qrel); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/ws/a.txt", []byte("squatter"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/ws/"+slot, []byte("base-occupant"), 0o644); err != nil {
		t.Fatal(err)
	}
	s.Reconcile(ctx)
	// The squatter keeps its public name — never evicted.
	if got := durRead(t, dir, "ws/a.txt"); got != "squatter" {
		t.Fatalf("a.txt = %q — foreign occupant must not be evicted", got)
	}
	if durExists(t, dir, "ws/"+qrel) {
		t.Fatal("sealed name still occupied after the drain")
	}
	if durExists(t, dir, "ws/"+slot) {
		t.Fatal("base slot still occupied after the drain")
	}
	// Every object survives at a visible name: the recorded v1 and the
	// foreign base occupant are each surfaced, never destroyed.
	if where := scanDirFor(t, dir, "ws", []byte("v1")); where == "" ||
		strings.HasPrefix(where, opStagePrefix) {
		t.Fatalf("recorded object lost or left private: %q", where)
	}
	if where := scanDirFor(t, dir, "ws", []byte("base-occupant")); where == "" ||
		strings.HasPrefix(where, opStagePrefix) {
		t.Fatalf("foreign base occupant lost or left private: %q", where)
	}
	// Convergence: a second pass changes nothing and deletes nothing.
	s.Reconcile(ctx)
	if got := durRead(t, dir, "ws/a.txt"); got != "squatter" {
		t.Fatalf("a.txt = %q after second pass", got)
	}
	if where := scanDirFor(t, dir, "ws", []byte("v1")); where == "" ||
		strings.HasPrefix(where, opStagePrefix) {
		t.Fatalf("recorded object lost after second pass: %q", where)
	}
	if where := scanDirFor(t, dir, "ws", []byte("base-occupant")); where == "" ||
		strings.HasPrefix(where, opStagePrefix) {
		t.Fatalf("base occupant lost after second pass: %q", where)
	}
}

// --- F-3: discard authority -------------------------------------------
//
// A private name holding an object whose fingerprint matches an
// intent's dst_fp is NOT discard authority: content similarity is not
// operation-bound proof. Under the names protocol, discard of a
// displaced object additionally requires a journaled act with an
// observed result plus bound durable identity — and an orphaned name
// carries no provenance at all. Whatever the evidence level, recorded
// content is restored to a free recorded home or surfaced visibly; it
// is never erased on a fingerprint match.

// insertEvent writes the file_event row an intent's apply would have
// journaled — positive commit evidence for composed dead-intent states.
func insertEvent(t *testing.T, s *Store, scope, path, fromPath, op string, version int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), s.dbTimeout)
	defer cancel()
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO file_event (scope, path, from_path, op, version)
		 VALUES ($1,$2,$3,$4,$5)`, scope, path, fromPath, op, version); err != nil {
		t.Fatalf("insert event: %v", err)
	}
}

// The witnessed F-3 shape through real Reconcile + real rows: a dead
// write intent's declared occupant (still the recorded content for its
// name) is parked at a -p- name by a delayed drain, and a squatter
// holds the path. The fingerprint match must never delete the recorded
// object: the squatter keeps its public name and the recorded object
// surfaces at a visible sibling — preserved, readable, deletable.
func TestPGReconcilePreservesRecordedDstFPObject(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	s.SetReconcileView(func(context.Context) (ReconView, error) { return root.pin(false) })
	probeOf := func(path string) FPProbe {
		return func() (FileInfo, bool, error) {
			info, err := root.stat("ws", path)
			if errors.Is(err, ErrNotFound) || errors.Is(err, ErrNotDir) {
				return FileInfo{}, false, nil
			}
			return info, err == nil, err
		}
	}
	// Acknowledged O0 at a.txt — row v records its fingerprint.
	if _, _, err = s.WithWrite(ctx, "ws", "a.txt", "write",
		IfVersion{Mode: "any"}, sha("O0"), probeOf("a.txt"),
		func(it intent) (FileInfo, bool, error) {
			return root.atomicWrite("ws", "a.txt", []byte("O0"), false, it)
		}); err != nil {
		t.Fatalf("write O0: %v", err)
	}
	fp0, err := root.stat("ws", "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	// A dead intent declared when O0 occupied a.txt: dst_fp = O0's fp.
	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "a.txt",
		version: 70, preFP: fp0.Fingerprint, dstFP: fp0.Fingerprint,
		expectSHA: sha("stale"), at: time.Now().Add(-time.Hour),
	})
	slot := opStagePrefix + strconv.FormatInt(id, 10)
	// The transient post-exchange shape plus a delayed drain landing:
	// O0 parks at a -p- name; the path holds foreign content.
	if err := os.Rename(dir+"/ws/a.txt", dir+"/ws/"+slot+"-p-ab12cd"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/ws/a.txt", []byte("squatter"), 0o644); err != nil {
		t.Fatal(err)
	}
	s.Reconcile(ctx)
	// The squatter is a live public object — never evicted by recovery.
	if got := durRead(t, dir, "ws/a.txt"); got != "squatter" {
		t.Fatalf("a.txt = %q — foreign occupant must not be evicted", got)
	}
	// Recorded O0 must surface at a visible name, never deleted.
	if where := scanDirFor(t, dir, "ws", []byte("O0")); where == "" ||
		strings.HasPrefix(where, opStagePrefix) {
		t.Fatalf("recorded content lost to fingerprint-match delete or left private: %q", where)
	}
	// Convergence: further passes keep O0 surfaced and the squatter home.
	s.Reconcile(ctx)
	if got := durRead(t, dir, "ws/a.txt"); got != "squatter" {
		t.Fatalf("a.txt = %q after second pass", got)
	}
	if where := scanDirFor(t, dir, "ws", []byte("O0")); where == "" ||
		strings.HasPrefix(where, opStagePrefix) {
		t.Fatalf("recorded object lost after second pass: %q", where)
	}
}

// The same shape for a tombstoned intent: resolved intents are
// re-judged on the hot tombstone scan, and the recorded object must
// still be surfaced — never deleted on a fingerprint match.
func TestPGReconcilePreservesRecordedDstFPTombstoned(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	s.SetReconcileView(func(context.Context) (ReconView, error) { return root.pin(false) })
	probeOf := func(path string) FPProbe {
		return func() (FileInfo, bool, error) {
			info, err := root.stat("ws", path)
			if errors.Is(err, ErrNotFound) || errors.Is(err, ErrNotDir) {
				return FileInfo{}, false, nil
			}
			return info, err == nil, err
		}
	}
	if _, _, err = s.WithWrite(ctx, "ws", "a.txt", "write",
		IfVersion{Mode: "any"}, sha("O0"), probeOf("a.txt"),
		func(it intent) (FileInfo, bool, error) {
			return root.atomicWrite("ws", "a.txt", []byte("O0"), false, it)
		}); err != nil {
		t.Fatalf("write O0: %v", err)
	}
	fp0, err := root.stat("ws", "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "a.txt",
		version: 71, preFP: fp0.Fingerprint, dstFP: fp0.Fingerprint,
		expectSHA: sha("stale"), at: time.Now().Add(-time.Hour),
	})
	// Tombstone the intent — resolved without its apply (e.g. a parked
	// undo forced it retained as evidence).
	if _, err := s.pool.Exec(ctx,
		`UPDATE file_op SET resolved_at=now() WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	slot := opStagePrefix + strconv.FormatInt(id, 10)
	if err := os.Rename(dir+"/ws/a.txt", dir+"/ws/"+slot+"-p-ab12cd"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/ws/a.txt", []byte("squatter"), 0o644); err != nil {
		t.Fatal(err)
	}
	s.Reconcile(ctx)
	if got := durRead(t, dir, "ws/a.txt"); got != "squatter" {
		t.Fatalf("a.txt = %q — foreign occupant must not be evicted", got)
	}
	if where := scanDirFor(t, dir, "ws", []byte("O0")); where == "" ||
		strings.HasPrefix(where, opStagePrefix) {
		t.Fatalf("tombstoned intent destroyed recorded content: %q", where)
	}
}

// A committed intent's displaced object parked at an unjournaled name
// carries no operation-bound provenance: even with the apply event
// journaled, the orphaned object cannot be proven to be the declared
// displaced object — it surfaces visibly, never deleted on the
// fingerprint match. (A live atomicWrite still discards its displaced
// object in-process; recovery-time discard needs bound identity.)
func TestPGReconcileCommittedDstFPDiscard(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	s.SetReconcileView(func(context.Context) (ReconView, error) { return root.pin(false) })
	probeOf := func(path string) FPProbe {
		return func() (FileInfo, bool, error) {
			info, err := root.stat("ws", path)
			if errors.Is(err, ErrNotFound) || errors.Is(err, ErrNotDir) {
				return FileInfo{}, false, nil
			}
			return info, err == nil, err
		}
	}
	if _, _, err = s.WithWrite(ctx, "ws", "a.txt", "write",
		IfVersion{Mode: "any"}, sha("O0"), probeOf("a.txt"),
		func(it intent) (FileInfo, bool, error) {
			return root.atomicWrite("ws", "a.txt", []byte("O0"), false, it)
		}); err != nil {
		t.Fatalf("write O0: %v", err)
	}
	fp0, err := root.stat("ws", "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "a.txt",
		version: 72, preFP: fp0.Fingerprint, dstFP: fp0.Fingerprint,
		expectSHA: sha("v1"), at: time.Now().Add(-time.Hour),
	})
	slot := opStagePrefix + strconv.FormatInt(id, 10)
	// Compose the committed shape: O0 displaced to the slot, v1 at the
	// path, the row applied to v72 with the journaled apply event.
	if err := os.Rename(dir+"/ws/a.txt", dir+"/ws/"+slot); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/ws/a.txt", []byte("v1"), 0o644); err != nil {
		t.Fatal(err)
	}
	fp1, err := root.stat("ws", "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE file_version SET version=72, fp=$3, updated=now()
		 WHERE scope=$1 AND path=$2`, "ws", "a.txt", fp1.Fingerprint); err != nil {
		t.Fatal(err)
	}
	insertEvent(t, s, "ws", "a.txt", "", "write", 72)
	s.Reconcile(ctx)
	// The settled op's row is gone, so the orphaned displaced object is
	// adopted on the next sweep — one listing sighting only defers.
	s.lastStageSweep.Store(0)
	s.Reconcile(ctx)
	if got := durRead(t, dir, "ws/a.txt"); got != "v1" {
		t.Fatalf("a.txt = %q, want committed v1", got)
	}
	// The orphaned displaced object is preserved at a visible name —
	// without journaled provenance nothing authorizes its destruction.
	if where := scanDirFor(t, dir, "ws", []byte("O0")); where == "" ||
		strings.HasPrefix(where, opStagePrefix) {
		t.Fatalf("displaced O0 destroyed or left private: %q", where)
	}
}

// An uncommitted intent whose declared object is no longer recorded
// anywhere still cannot delete it: without commit proof the object is
// unattributable foreign content — preserved parked. An event while the
// intent row is retained is NOT that proof (a diverged roll-forward
// journals exactly that shape). Once the intent's own effect is
// observed and its roll-forward apply commits (event + intent row
// removed in one tx), the same pass completes the authorized discard.
func TestPGReconcileUncommittedDstFPPreserved(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	s.SetReconcileView(func(context.Context) (ReconView, error) { return root.pin(false) })
	if err := os.MkdirAll(dir+"/ws", 0o755); err != nil {
		t.Fatal(err)
	}
	// A dead write intent; the object it declared is parked at the slot
	// but NO row records it and no event proves the intent committed.
	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "a.txt",
		version: 73, preFP: "0:0:0:0", dstFP: "",
		expectSHA: sha("stale"), at: time.Now().Add(-time.Hour),
	})
	slot := opStagePrefix + strconv.FormatInt(id, 10)
	if err := os.WriteFile(dir+"/ws/"+slot, []byte("unrecorded"), 0o644); err != nil {
		t.Fatal(err)
	}
	fpX, err := root.stat("ws", slot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE file_op SET dst_fp=$2 WHERE id=$1`, id, fpX.Fingerprint); err != nil {
		t.Fatal(err)
	}
	s.Reconcile(ctx)
	// Not recorded + not committed → surfaced visibly, not deleted to
	// "finish" a discard that never happened.
	if where := scanDirFor(t, dir, "ws", []byte("unrecorded")); where == "" ||
		strings.HasPrefix(where, opStagePrefix) {
		t.Fatalf("unrecorded object deleted without commit evidence: %q", where)
	}
	// An event alongside the retained (tombstoned) intent row proves
	// nothing about commit — still preserved.
	insertEvent(t, s, "ws", "a.txt", "", "write", 73)
	s.lastTombScan.Store(0)
	s.Reconcile(ctx)
	if where := scanDirFor(t, dir, "ws", []byte("unrecorded")); where == "" ||
		strings.HasPrefix(where, opStagePrefix) {
		t.Fatalf("object deleted on an event whose intent row was retained: %q", where)
	}
	// The intent's effect lands late: its bytes appear at the path and
	// the tombstone rolls forward — apply removes the intent row with
	// its event. The surfaced object has no operation-bound provenance:
	// it stays visible, preserved forever — the roll-forward never
	// retroactively authorizes destroying it.
	if err := os.WriteFile(dir+"/ws/a.txt", []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	s.lastTombScan.Store(0)
	s.Reconcile(ctx)
	var pending int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM file_op WHERE resolved_at IS NULL`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Fatalf("roll-forward did not settle the intent: %d pending rows", pending)
	}
	if where := scanDirFor(t, dir, "ws", []byte("unrecorded")); where == "" ||
		strings.HasPrefix(where, opStagePrefix) {
		t.Fatalf("object destroyed after the intent's apply committed: %q", where)
	}
}

// Remove intents get the same authority rule: a captured object whose
// capture result was never journaled (the op died mid-flight) is not
// proven to be the declared object — while the row still records it,
// the object is restored to its name, never deleted.
func TestPGReconcileRemoveRecordedDstFP(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	s.SetReconcileView(func(context.Context) (ReconView, error) { return root.pin(false) })
	probeOf := func(path string) FPProbe {
		return func() (FileInfo, bool, error) {
			info, err := root.stat("ws", path)
			if errors.Is(err, ErrNotFound) || errors.Is(err, ErrNotDir) {
				return FileInfo{}, false, nil
			}
			return info, err == nil, err
		}
	}
	if _, _, err = s.WithWrite(ctx, "ws", "a.txt", "write",
		IfVersion{Mode: "any"}, sha("O0"), probeOf("a.txt"),
		func(it intent) (FileInfo, bool, error) {
			return root.atomicWrite("ws", "a.txt", []byte("O0"), false, it)
		}); err != nil {
		t.Fatalf("write O0: %v", err)
	}
	fp0, err := root.stat("ws", "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "remove", path: "a.txt",
		version: 74, preFP: fp0.Fingerprint, dstFP: fp0.Fingerprint,
		at: time.Now().Add(-time.Hour),
	})
	slot := opStagePrefix + strconv.FormatInt(id, 10)
	// The dead remove journaled its capture of a.txt's object into its
	// private name but died before recording the result — the captured
	// object is not proven to be the declared one.
	if _, err := s.pool.Exec(ctx,
		`UPDATE file_op SET names=$2 WHERE id=$1`, id,
		mustJSON([]nameRec{{Name: slot, Act: "cap", Src: "a.txt"}})); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(dir+"/ws/a.txt", dir+"/ws/"+slot); err != nil {
		t.Fatal(err)
	}
	s.Reconcile(ctx)
	if got := durRead(t, dir, "ws/a.txt"); got != "O0" {
		t.Fatalf("a.txt = %q — uncommitted remove destroyed recorded content", got)
	}
}

// Rename: the displaced DESTINATION object is judged against the rows
// too — its recorded home is the destination path (or wherever its row
// now lives), not the source. An uncommitted rename must restore, not
// delete, the recorded destination object.
func TestPGReconcileRenameRecordedDstFP(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	s.SetReconcileView(func(context.Context) (ReconView, error) { return root.pin(false) })
	probeOf := func(path string) FPProbe {
		return func() (FileInfo, bool, error) {
			info, err := root.stat("ws", path)
			if errors.Is(err, ErrNotFound) || errors.Is(err, ErrNotDir) {
				return FileInfo{}, false, nil
			}
			return info, err == nil, err
		}
	}
	if _, _, err = s.WithWrite(ctx, "ws", "new.txt", "write",
		IfVersion{Mode: "any"}, sha("D0"), probeOf("new.txt"),
		func(it intent) (FileInfo, bool, error) {
			return root.atomicWrite("ws", "new.txt", []byte("D0"), false, it)
		}); err != nil {
		t.Fatalf("write D0: %v", err)
	}
	fpD, err := root.stat("ws", "new.txt")
	if err != nil {
		t.Fatal(err)
	}
	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "rename",
		path: "old.txt", toPath: "new.txt",
		version: 75, dstFP: fpD.Fingerprint,
		at: time.Now().Add(-time.Hour),
	})
	slot := opStagePrefix + strconv.FormatInt(id, 10)
	// The dead rename captured the destination object but never
	// committed; the row still records D0 at new.txt.
	if err := os.Rename(dir+"/ws/new.txt", dir+"/ws/"+slot); err != nil {
		t.Fatal(err)
	}
	// The durable contract is bounded convergence, not single-pass
	// atomicity: any transient filesystem or database error inside the
	// attach → capture → judge → settle chain defers the journaled
	// record to a later pass by design (every deferral point retries).
	// What is never acceptable is loss — D0 must remain somewhere under
	// the scope after every pass, and the recorded home must hold it
	// within bounded reconciliation (F253).
	for pass := 0; pass < 4; pass++ {
		s.Reconcile(ctx)
		if durExists(t, dir, "ws/new.txt") && durRead(t, dir, "ws/new.txt") == "D0" {
			// Converged — the row at the recorded home must truthfully
			// record D0's object identity (fp3 — the settle may refresh
			// the mtime leg after the move).
			_, fp, ok := authRow(t, s, "new.txt")
			if !ok || fp3(fp) != fp3(fpD.Fingerprint) {
				t.Fatalf("new.txt row = %q present=%v, want recorded object %s", fp, ok, fpD.Fingerprint)
			}
			return
		}
		if where := scanTreeFor(t, dir, "ws", []byte("D0")); where == "" {
			t.Fatalf("pass %d: D0 destroyed — uncommitted rename must never lose recorded destination content", pass)
		}
	}
	t.Fatal("D0 not restored to its recorded home within bounded reconciliation")
}

// Database-read failure must not authorize deletion: with the store's
// pool closed, the row check cannot answer — the object is preserved.
func TestPGReconcileDBErrorPreservesDstFP(t *testing.T) {
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
	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "a.txt",
		version: 76, preFP: "0:0:0:0", dstFP: "",
		expectSHA: sha("stale"), at: time.Now().Add(-time.Hour),
	})
	slot := opStagePrefix + strconv.FormatInt(id, 10)
	if err := os.WriteFile(dir+"/ws/"+slot, []byte("maybe-recorded"), 0o644); err != nil {
		t.Fatal(err)
	}
	fpX, err := root.stat("ws", slot)
	if err != nil {
		t.Fatal(err)
	}
	it := intent{id: id, owner: "dead-inst", scope: "ws", op: "write",
		path: "a.txt", version: 76, dstFP: fpX.Fingerprint,
		expectSHA: sha("stale"), at: time.Now().Add(-time.Hour),
		names: []nameRec{{Name: slot}}}
	it.journal = &nameJournal{s: s, id: id}
	view, err := root.pin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	s.pool.Close() // the row set is unverifiable — no capture, no judgment
	s.reconcileOne(ctx, it, view, false)
	if where := scanDirFor(t, dir, "ws", []byte("maybe-recorded")); where == "" {
		t.Fatal("object deleted while the version rows were unverifiable")
	}
}
