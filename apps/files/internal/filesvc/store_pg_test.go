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
	"os"
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

// fakeDisk is an in-memory filesystem view for reconcile tests.
type fakeDisk struct {
	files map[string]string // scope/path -> content ("dir:" prefix = dir)
	fps   map[string]string // scope/path -> fingerprint
}

func newFakeDisk() *fakeDisk {
	return &fakeDisk{files: map[string]string{}, fps: map[string]string{}}
}

func (d *fakeDisk) put(scope, path, content string) {
	k := scope + "/" + path
	d.files[k] = content
	d.fps[k] = "fp-" + hex.EncodeToString([]byte(k + content))[:8]
}

func (d *fakeDisk) del(scope, path string) {
	k := scope + "/" + path
	delete(d.files, k)
	delete(d.fps, k)
}

func (d *fakeDisk) stat(scope, path string) (FileInfo, error) {
	k := scope + "/" + path
	c, ok := d.files[k]
	if !ok {
		return FileInfo{}, ErrNotFound
	}
	if c == "dir" {
		return FileInfo{Kind: "dir", Fingerprint: d.fps[k]}, nil
	}
	return FileInfo{Kind: "file", Size: int64(len(c)), Fingerprint: d.fps[k]}, nil
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
	s.SetReconcile(disk.stat, disk.hash)

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
	s.SetReconcile(disk.stat, disk.hash)
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
	s.SetReconcile(disk.stat, disk.hash)
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
	if fp == "" || fp == disk.fps["ws/c.txt"] {
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
	s.SetReconcile(disk.stat, disk.hash)
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
	s.SetReconcile(disk.stat, disk.hash)
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
	s.SetReconcile(disk.stat, disk.hash)

	ctx := context.Background()
	// Declare-time state: d1/old.txt (v2). The intent minted v4.
	// Post-commit: d1 recreated with new.txt (v5) by a later op.
	_, err := s.pool.Exec(ctx,
		`INSERT INTO file_version (scope, path, version, fp) VALUES
		 ('ws','d1/old.txt',2,'fp-old'), ('ws','d1/new.txt',5,'fp-new')`)
	if err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	// Disk after: rename ran (d2 exists), source dir recreated.
	disk.put("ws", "d1", "dir")
	disk.fps["ws/d1"] = "fp-d1-new" // recreated — differs from preFP
	disk.put("ws", "d2", "dir")

	insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "rename", path: "d1", toPath: "d2",
		version: 4, preFP: "fp-d1-old", at: time.Now().Add(-time.Minute),
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
	s.SetReconcile(disk.stat, disk.hash)
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
