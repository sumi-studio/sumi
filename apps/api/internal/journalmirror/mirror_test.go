package journalmirror

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

func migratedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return migratedPoolTB(t, testdb.Create(t))
}

func migratedPoolB(b *testing.B) *pgxpool.Pool {
	b.Helper()
	return migratedPoolTB(b, testdb.Create(b))
}

func migratedPoolTB(t testing.TB, pool *pgxpool.Pool) *pgxpool.Pool {
	t.Helper()
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

func acquire(t *testing.T, pool *pgxpool.Pool, onFenced func(error)) *Mirror {
	t.Helper()
	m, err := Acquire(context.Background(), pool, t.Name(), onFenced)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func remoteFiles(t *testing.T, pool *pgxpool.Pool, dir string) map[string][]byte {
	t.Helper()
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	files, err := readRemote(context.Background(), tx, dir)
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func openWrapped(t *testing.T, m *Mirror, path string, flag int) File {
	t.Helper()
	f, err := os.OpenFile(path, flag, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	wrapped, err := m.Wrap(path, flag, f)
	if err != nil {
		t.Fatal(err)
	}
	return wrapped
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return content
}

// The first attach adopts the host's existing journals; a later attach on an
// empty directory (a replaced host) writes them back byte for byte. Lock and
// temporary files are not journal state and are not carried.
func TestAttachSeedsThenRestoresIntoEmptyDirectory(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()
	src := t.TempDir()
	big := bytes.Repeat([]byte("0123456789abcdef"), ChunkSize/8) // two chunks
	files := map[string][]byte{
		"events-QUJD.jsonl":                big,
		"events-QUJD.dedup":                []byte("dedup"),
		"browser-session-revocations.json": []byte(`{"v":1}`),
		"empty.jsonl":                      {},
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(src, name), content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"journal-QUJD.lock", ".idempotency.lock", "x.json.123.tmp"} {
		if err := os.WriteFile(filepath.Join(src, name), []byte("not state"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	first := acquire(t, pool, nil)
	report, err := first.Attach(ctx, "browser-events", src)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Seeded || report.Files != len(files) {
		t.Fatalf("seed report = %+v", report)
	}

	second := acquire(t, pool, nil)
	dst := t.TempDir()
	report, err = second.Attach(ctx, "browser-events", dst)
	if err != nil {
		t.Fatal(err)
	}
	if report.Seeded || len(report.Restored) != len(files) {
		t.Fatalf("restore report = %+v", report)
	}
	entries, _ := os.ReadDir(dst)
	if len(entries) != len(files) {
		t.Fatalf("restored %d entries, want %d", len(entries), len(files))
	}
	for name, content := range files {
		if got := mustRead(t, filepath.Join(dst, name)); !bytes.Equal(got, content) {
			t.Fatalf("%s restored %d bytes, want %d", name, len(got), len(content))
		}
		info, _ := os.Stat(filepath.Join(dst, name))
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("%s mode %v", name, info.Mode())
		}
	}
}

// Random positional writes, appends and truncations across chunk boundaries:
// after every Sync PostgreSQL holds exactly the local file.
func TestSyncedWritesMatchLocalFile(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()
	dir := t.TempDir()
	m := acquire(t, pool, nil)
	if _, err := m.Attach(ctx, "commands", dir); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "commands-QQ.jsonl")
	rng := rand.New(rand.NewSource(7))
	for _, flag := range []int{os.O_CREATE | os.O_RDWR, os.O_CREATE | os.O_RDWR | os.O_APPEND} {
		f := openWrapped(t, m, path, flag)
		for step := 0; step < 60; step++ {
			size, _ := f.Seek(0, io.SeekEnd)
			switch k := rng.Intn(10); {
			case k < 6: // append-sized write at the end, sometimes several chunks
				n := rng.Intn(3*ChunkSize/2) + 1
				if _, err := f.Write(bytes.Repeat([]byte{byte('a' + step%26)}, n)); err != nil {
					t.Fatal(err)
				}
			case k < 8 && size > 0 && flag&os.O_APPEND == 0: // overwrite in place
				at := rng.Int63n(size)
				if _, err := f.Seek(at, io.SeekStart); err != nil {
					t.Fatal(err)
				}
				if _, err := f.Write([]byte("OVERWRITE")); err != nil {
					t.Fatal(err)
				}
			default: // rollback-style truncation
				if size > 0 {
					if err := f.Truncate(rng.Int63n(size + 1)); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := f.Sync(); err != nil {
				t.Fatal(err)
			}
			if got, want := remoteFiles(t, pool, "commands")["commands-QQ.jsonl"], mustRead(t, path); !bytes.Equal(got, want) {
				t.Fatalf("flag %x step %d: mirror has %d bytes, local %d", flag, step, len(got), len(want))
			}
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// A write is acknowledged by Sync. Bytes written but never synced are not in
// the mirror; a handle closed with such bytes leaves the file marked, and the
// next Sync of that file (under the journal's lock) carries the whole file.
func TestUnsyncedWritesAreNotMirroredUntilNextSync(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()
	dir := t.TempDir()
	m := acquire(t, pool, nil)
	if _, err := m.Attach(ctx, "commands", dir); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "log.jsonl")
	f := openWrapped(t, m, path, os.O_CREATE|os.O_RDWR|os.O_APPEND)
	if _, err := f.Write([]byte("acked\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("unacked\n")); err != nil {
		t.Fatal(err)
	}
	if got := remoteFiles(t, pool, "commands")["log.jsonl"]; string(got) != "acked\n" {
		t.Fatalf("mirror before sync = %q", got)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if got := remoteFiles(t, pool, "commands")["log.jsonl"]; string(got) != "acked\n" {
		t.Fatalf("close must not commit unsynced bytes: %q", got)
	}

	g := openWrapped(t, m, path, os.O_RDWR|os.O_APPEND)
	defer g.Close()
	if _, err := g.Write([]byte("next\n")); err != nil {
		t.Fatal(err)
	}
	if err := g.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := remoteFiles(t, pool, "commands")["log.jsonl"]; string(got) != "acked\nunacked\nnext\n" {
		t.Fatalf("resync = %q", got)
	}
}

// A newer process fences the older: the older one's Sync fails, changes
// nothing in PostgreSQL, and reports the fence once.
func TestNewerOwnerFencesOlderWrites(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()
	var fenced atomic.Int32
	notified := make(chan struct{}, 1)
	old := acquire(t, pool, func(err error) {
		if errors.Is(err, ErrFenced) {
			fenced.Add(1)
			notified <- struct{}{}
		}
	})
	dir := t.TempDir()
	if _, err := old.Attach(ctx, "commands", dir); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "log.jsonl")
	f := openWrapped(t, old, path, os.O_CREATE|os.O_RDWR|os.O_APPEND)
	defer f.Close()
	if _, err := f.Write([]byte("before\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}

	newer := acquire(t, pool, nil)
	if newer.Epoch() <= old.Epoch() {
		t.Fatalf("epochs old=%d newer=%d", old.Epoch(), newer.Epoch())
	}
	if _, err := f.Write([]byte("after\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); !errors.Is(err, ErrFenced) {
		t.Fatalf("sync after takeover = %v, want ErrFenced", err)
	}
	if err := f.Sync(); !errors.Is(err, ErrFenced) {
		t.Fatalf("later sync = %v, want ErrFenced", err)
	}
	<-notified
	if fenced.Load() != 1 || !old.Fenced() {
		t.Fatalf("fence notifications = %d", fenced.Load())
	}
	if got := remoteFiles(t, pool, "commands")["log.jsonl"]; string(got) != "before\n" {
		t.Fatalf("mirror after fenced write = %q", got)
	}
	if _, err := newer.Attach(ctx, "commands", t.TempDir()); err != nil {
		t.Fatalf("newer owner attach: %v", err)
	}
}

// Restarting on a disk that kept its files (an ordinary host restart): an
// unacknowledged tail is trimmed; any other difference refuses to start and
// changes neither copy.
func TestAttachTrimsUnacknowledgedTailAndRefusesConflicts(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()
	dir := t.TempDir()
	m := acquire(t, pool, nil)
	if _, err := m.Attach(ctx, "commands", dir); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "log.jsonl")
	f := openWrapped(t, m, path, os.O_CREATE|os.O_RDWR|os.O_APPEND)
	if _, err := f.Write([]byte("acked\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	f.Close()
	// Crash after the local fsync, before the mirror commit.
	if err := os.WriteFile(path, []byte("acked\ntorn"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "stray.jsonl"), []byte("never synced"), 0o600); err != nil {
		t.Fatal(err)
	}
	restarted := acquire(t, pool, nil)
	report, err := restarted.Attach(ctx, "commands", dir)
	if err != nil {
		t.Fatal(err)
	}
	if report.Trimmed["log.jsonl"] != 4 || report.Trimmed["stray.jsonl"] != int64(len("never synced")) {
		t.Fatalf("trimmed = %v", report.Trimmed)
	}
	if got := mustRead(t, path); string(got) != "acked\n" {
		t.Fatalf("trimmed local = %q", got)
	}

	if err := os.WriteFile(path, []byte("ACKED\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	conflicted := acquire(t, pool, nil)
	if _, err := conflicted.Attach(ctx, "commands", dir); !errors.Is(err, ErrConflict) {
		t.Fatalf("attach with rewritten history = %v, want ErrConflict", err)
	}
	if got := mustRead(t, path); string(got) != "ACKED\n" {
		t.Fatalf("conflict changed local file: %q", got)
	}
	if got := remoteFiles(t, pool, "commands")["log.jsonl"]; string(got) != "acked\n" {
		t.Fatalf("conflict changed mirror: %q", got)
	}
}

func TestWrapRejectsFilesOutsideAttachedDirectories(t *testing.T) {
	pool := migratedPool(t)
	m := acquire(t, pool, nil)
	dir := t.TempDir()
	if _, err := m.Attach(context.Background(), "commands", dir); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(t.TempDir(), "log.jsonl")
	f, err := os.OpenFile(other, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := m.Wrap(other, os.O_RDWR, f); err == nil {
		t.Fatal("wrap outside attached directory succeeded")
	}
	if wrapped, err := m.Wrap(other, os.O_RDONLY, f); err != nil || wrapped != File(f) {
		t.Fatalf("read-only handle should pass through unchanged: %v", err)
	}
}

func TestVerifyReportsDifferencesWithoutChangingAnything(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()
	dir := t.TempDir()
	for name, content := range map[string]string{"a.jsonl": "one\n", "b.jsonl": "two\n"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	m := acquire(t, pool, nil)
	if _, err := m.Attach(ctx, "commands", dir); err != nil {
		t.Fatal(err)
	}
	if report, err := Verify(ctx, pool, "commands", dir); err != nil || !report.Equal() || report.Files != 2 {
		t.Fatalf("verify identical = %+v, %v", report, err)
	}
	_ = os.WriteFile(filepath.Join(dir, "a.jsonl"), []byte("ONE\n"), 0o600)
	_ = os.Remove(filepath.Join(dir, "b.jsonl"))
	_ = os.WriteFile(filepath.Join(dir, "c.jsonl"), []byte("new\n"), 0o600)
	report, err := Verify(ctx, pool, "commands", dir)
	if err != nil {
		t.Fatal(err)
	}
	if report.Equal() || len(report.Different) != 1 || len(report.Missing) != 1 || len(report.Extra) != 1 {
		t.Fatalf("verify changed dir = %+v", report)
	}
	if got := mustRead(t, filepath.Join(dir, "a.jsonl")); string(got) != "ONE\n" {
		t.Fatalf("verify modified the directory: %q", got)
	}
	if _, err := Verify(ctx, pool, "browser-events", dir); err == nil {
		t.Fatal("verify of an unseeded mirror succeeded")
	}
}
