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
		`TRUNCATE file_version, file_event, file_op;
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
			`TRUNCATE file_version, file_event, file_op;
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
	files map[string]string // scope/path -> content ("dir" = dir)
	inos  map[string]uint64
	mts   map[string]int64
	next  uint64
	clock int64
}

func newFakeDisk() *fakeDisk {
	return &fakeDisk{files: map[string]string{},
		inos: map[string]uint64{}, mts: map[string]int64{}}
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

func (d *fakeDisk) stat(scope, path string) (FileInfo, error) {
	k := scope + "/" + path
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
	v, fp, err := s.ObservedVersion(context.Background(), scope, path)
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
		`INSERT INTO file_op (root, owner, scope, op, path, to_path, version, pre_fp, expect_sha, at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id`,
		s.rootID, it.owner, it.scope, it.op, it.path, it.toPath,
		it.version, it.preFP, it.expectSHA, it.at).Scan(&id)
	if err != nil {
		t.Fatalf("insert intent: %v", err)
	}
	return id
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

	probeAbsent := func() (string, bool, error) { return "", false, nil }
	ver, _, err := s.WithWrite(context.Background(), "ws", "a.txt", "write",
		IfVersion{Mode: "none"}, sha("hello"), probeAbsent,
		func() (FileInfo, error) {
			disk.put("ws", "a.txt", "hello")
			return FileInfo{Kind: "file", Fingerprint: "fp-a"}, nil
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
	_, err := s.pool.Exec(ctx,
		`INSERT INTO file_version (scope, path, version, fp) VALUES
		 ('ws','d1/old.txt',2,'fp-old'), ('ws','d1/new.txt',5,'fp-new')`)
	if err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	// Disk after: rename ran (d2 is the moved d1 — same inode), and the
	// source path was recreated with a new dir (new inode).
	disk.put("ws", "d1", "dir")
	d1Info, _ := disk.stat("ws", "d1")
	preFP := d1Info.Fingerprint
	disk.mv("ws", "d1", "d2")
	disk.put("ws", "d1", "dir") // recreated — new inode, differs from preFP

	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "rename", path: "d1", toPath: "d2",
		version: 4, preFP: preFP, at: time.Now().Add(-time.Minute),
	})
	if got := s.Reconcile(context.Background()); got != 1 {
		t.Fatalf("reconcile settled %d", got)
	}
	// The post-commit file keeps its row at the old source path.
	if v, _ := versionOf(t, s, "ws", "d1/new.txt"); v != 5 {
		t.Fatalf("d1/new.txt version = %d — stale rename stole the row", v)
	}
	// The declare-time descendant moved with the rename.
	if v, _ := versionOf(t, s, "ws", "d2/old.txt"); v != 2 {
		t.Fatalf("d2/old.txt version = %d, want 2", v)
	}
	if v, _ := versionOf(t, s, "ws", "d2"); v != 4 {
		t.Fatalf("d2 version = %d, want 4", v)
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
	if err := s.apply(context.Background(), it, FileInfo{Fingerprint: "fp-f"}); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	err := s.apply(context.Background(), it, FileInfo{Fingerprint: "fp-f"})
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
	if s.dropIntentGhosts(ctx, it, true) {
		t.Fatal("dropIntentGhosts deleted under a foreign owner")
	}
	if err := s.apply(ctx, it, FileInfo{Fingerprint: "fp-x"}); err == nil {
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
		version: 30, at: time.Now(),
	})
	probeAbsent := func() (string, bool, error) { return "", false, nil }
	noop := func() (FileInfo, error) { return FileInfo{Kind: "file"}, nil }

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
		func() (FileInfo, error) {
			disk.put("ws", "c.txt", "c")
			return FileInfo{Kind: "file", Fingerprint: "fp-c"}, nil
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
		version: 20, preFP: preFP, at: time.Now().Add(-time.Minute),
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

// f106 positive leg: the destination IS the moved source (inode
// continuity) — the intent applies, moving the subtree rows.
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
		version: 21, preFP: preFP, at: time.Now().Add(-time.Minute),
	})
	if got := s.Reconcile(ctx); got != 1 {
		t.Fatalf("reconcile settled %d", got)
	}
	v, fp := versionOf(t, s, "ws", "f2.txt")
	if v != 21 {
		t.Fatalf("destination version = %d, want 21", v)
	}
	live, _ := disk.stat("ws", "f2.txt")
	if fp != live.Fingerprint {
		t.Fatalf("clean rename recorded fp %q, want live %q", fp, live.Fingerprint)
	}
	if v, _ := versionOf(t, s, "ws", "f1.txt"); v != 0 {
		t.Fatalf("source row left behind: %d", v)
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
		version: 22, preFP: preFP, at: time.Now().Add(-time.Minute),
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
