package journalmirror

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

func migratedPool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	pool := testdb.Create(t)
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

// acquire takes the mirror for a test "host". A test that starts another
// host on the same database closes the previous one first, as a host
// replacement does.
func acquire(t testing.TB, pool *pgxpool.Pool, opts Options) *Mirror {
	t.Helper()
	if opts.Holder == "" {
		opts.Holder = t.Name()
	}
	if opts.Logf == nil {
		opts.Logf = t.Logf
	}
	m, err := Acquire(context.Background(), pool, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

// attachEmpty attaches dir to a mirror initialized empty (a new installation).
func attachEmpty(t testing.TB, m *Mirror, pool *pgxpool.Pool, logical, dir string) {
	t.Helper()
	if _, _, err := InitializeEmpty(context.Background(), pool, logical, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Attach(context.Background(), logical, dir, AttachOptions{}); err != nil {
		t.Fatal(err)
	}
}

func remoteBytes(t testing.TB, pool *pgxpool.Pool, logical, name string) []byte {
	t.Helper()
	ctx := context.Background()
	var size int64
	if err := pool.QueryRow(ctx, `SELECT size FROM api_journal_mirror_files WHERE dir = $1 AND name = $2`, logical, name).Scan(&size); err != nil {
		t.Fatalf("mirror %s/%s: %v", logical, name, err)
	}
	var out []byte
	if err := streamChunks(ctx, pool, logical, name, size, func(_ int64, data []byte) error {
		out = append(out, data...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func remoteNames(t testing.TB, pool *pgxpool.Pool, logical string) []string {
	t.Helper()
	files, err := readRemoteMeta(context.Background(), pool, logical)
	if err != nil {
		t.Fatal(err)
	}
	return sortedKeys(files)
}

func openWrapped(t testing.TB, m *Mirror, path string, flag int) File {
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

func appendSync(t testing.TB, m *Mirror, path, content string) {
	t.Helper()
	f := openWrapped(t, m, path, os.O_CREATE|os.O_RDWR|os.O_APPEND)
	defer f.Close()
	if _, err := f.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
}

func mustRead(t testing.TB, path string) []byte {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return content
}

func writeFiles(t testing.TB, dir string, files map[string][]byte) {
	t.Helper()
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// snapshot returns every regular file under dir (recursively) and its bytes.
func snapshot(t testing.TB, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		out[rel] = string(mustRead(t, path))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func mirrorRows(t testing.TB, pool *pgxpool.Pool) string {
	t.Helper()
	var rows string
	err := pool.QueryRow(context.Background(), `
		SELECT coalesce(string_agg(format('%s/%s:%s:%s:%s', f.dir, f.name, f.size, f.gen, md5(coalesce(c.data, ''))), ',' ORDER BY f.dir, f.name), '')
		FROM api_journal_mirror_files f
		LEFT JOIN LATERAL (SELECT string_agg(data, ''::bytea ORDER BY chunk) AS data FROM api_journal_mirror_chunks WHERE dir = f.dir AND name = f.name) c ON true`).Scan(&rows)
	if err != nil {
		t.Fatal(err)
	}
	var dirs string
	if err := pool.QueryRow(context.Background(), `SELECT coalesce(string_agg(dir || '=' || lineage, ',' ORDER BY dir), '') FROM api_journal_mirror_dirs`).Scan(&dirs); err != nil {
		t.Fatal(err)
	}
	return dirs + "|" + rows
}

// The host that holds the journals adopts them once; a replaced host with an
// empty disk restores them byte for byte. Lock and temporary files are not
// journal state and are not carried.
func TestAdoptThenRestoreIntoEmptyDirectory(t *testing.T) {
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
	writeFiles(t, src, files)
	writeFiles(t, src, map[string][]byte{"journal-QUJD.lock": []byte("x"), ".idempotency.lock": []byte("x"), "x.json.123.tmp": []byte("x")})

	first := acquire(t, pool, Options{Holder: "old-host"})
	report, err := first.Attach(ctx, "browser-events", src, AttachOptions{Mode: AttachAdopt})
	if err != nil {
		t.Fatal(err)
	}
	if !report.Initialized || report.Files != len(files) || report.Lineage == "" {
		t.Fatalf("adopt report = %+v", report)
	}
	if lineage, _ := readMarker(src); lineage != report.Lineage {
		t.Fatalf("source marker lineage %q, want %q", lineage, report.Lineage)
	}
	first.Close()

	second := acquire(t, pool, Options{Holder: "new-host"})
	dst := t.TempDir()
	restored, err := second.Attach(ctx, "browser-events", dst, AttachOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if restored.Initialized || len(restored.Restored) != len(files) || len(restored.Quarantined) != 0 || restored.Lineage != report.Lineage {
		t.Fatalf("restore report = %+v", restored)
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
	for _, name := range []string{"journal-QUJD.lock", "x.json.123.tmp"} {
		if _, err := os.Stat(filepath.Join(dst, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s carried: %v", name, err)
		}
	}
	// An adopt-configured host restarting against the initialized mirror
	// behaves as a restore.
	second.Close()
	third := acquire(t, pool, Options{})
	again, err := third.Attach(ctx, "browser-events", dst, AttachOptions{Mode: AttachAdopt})
	if err != nil || again.Initialized || len(again.Restored)+len(again.Replaced)+len(again.Quarantined) != 0 {
		t.Fatalf("restart attach = %+v, %v", again, err)
	}
}

// F1: a restore-only host that starts before the mirror was initialized
// refuses instead of making "nothing" the journals, and an empty directory
// is never adopted.
func TestUninitializedMirrorIsNeverTreatedAsEmpty(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()
	m := acquire(t, pool, Options{})
	empty := t.TempDir()
	if _, err := m.Attach(ctx, "commands", empty, AttachOptions{Mode: AttachRestore}); !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("restore into an uninitialized mirror = %v, want ErrNotInitialized", err)
	}
	if _, err := m.Attach(ctx, "commands", empty, AttachOptions{Mode: AttachAdopt}); !errors.Is(err, ErrNothingToAdopt) {
		t.Fatalf("adopt of an empty directory = %v, want ErrNothingToAdopt", err)
	}
	populated := t.TempDir()
	writeFiles(t, populated, map[string][]byte{"commands-QQ.jsonl": []byte("{\"seq\":1}\n")})
	if _, err := m.Attach(ctx, "commands", populated, AttachOptions{Mode: AttachRestore}); !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("restore-only start on a populated host = %v, want ErrNotInitialized", err)
	}
	if rows := mirrorRows(t, pool); rows != "|" {
		t.Fatalf("refusals wrote the mirror: %s", rows)
	}
	if got := snapshot(t, populated); len(got) != 1 || got["commands-QQ.jsonl"] != "{\"seq\":1}\n" {
		t.Fatalf("refusal changed the populated directory: %v", got)
	}
}

// F1, the reviewer's order: the empty target is initialized (by mistake)
// before the populated source was adopted, and even accepts a new write.
// The populated source then refuses to reconcile with that mirror and
// neither copy changes; the operator can still adopt it into a fresh
// database.
func TestEmptyTargetBeforePopulatedSourceDestroysNothing(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()
	source := t.TempDir()
	sourceFiles := map[string][]byte{
		"commands-QQ.jsonl": []byte("{\"seq\":1,\"key\":\"idem-1\"}\n{\"seq\":2,\"key\":\"idem-2\"}\n"),
		"events-QQ.jsonl":   bytes.Repeat([]byte("event\n"), 20000),
	}
	writeFiles(t, source, sourceFiles)
	before := snapshot(t, source)

	target := acquire(t, pool, Options{Holder: "container"})
	if _, err := target.Attach(ctx, "commands", t.TempDir(), AttachOptions{}); !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("target before initialization = %v", err)
	}
	targetDir := t.TempDir()
	attachEmpty(t, target, pool, "commands", targetDir)
	appendSync(t, target, filepath.Join(targetDir, "commands-QQ.jsonl"), "{\"seq\":1,\"key\":\"other\"}\n")
	target.Close()
	mirrorBefore := mirrorRows(t, pool)

	for _, mode := range []AttachMode{AttachAdopt, AttachRestore} {
		src := acquire(t, pool, Options{Holder: "source"})
		_, err := src.Attach(ctx, "commands", source, AttachOptions{Mode: mode})
		if !errors.Is(err, ErrWrongMirror) {
			t.Fatalf("mode %d: populated source against the empty-initialized mirror = %v, want ErrWrongMirror", mode, err)
		}
		src.Close()
		if got := snapshot(t, source); len(got) != len(before) {
			t.Fatalf("mode %d: source directory changed: %v", mode, got)
		} else {
			for name, content := range before {
				if got[name] != content {
					t.Fatalf("mode %d: source %s changed", mode, name)
				}
			}
		}
		if got := mirrorRows(t, pool); got != mirrorBefore {
			t.Fatalf("mode %d: refusal changed the mirror\n got %s\nwant %s", mode, got, mirrorBefore)
		}
	}

	// Recovery: the source is adopted into a database whose mirror was not
	// initialized, and a replaced host restores exactly the source.
	fresh := migratedPool(t)
	adopter := acquire(t, fresh, Options{Holder: "source"})
	if _, err := adopter.Attach(ctx, "commands", source, AttachOptions{Mode: AttachAdopt}); err != nil {
		t.Fatal(err)
	}
	adopter.Close()
	restorer := acquire(t, fresh, Options{Holder: "container"})
	dst := t.TempDir()
	if _, err := restorer.Attach(ctx, "commands", dst, AttachOptions{}); err != nil {
		t.Fatal(err)
	}
	for name, content := range sourceFiles {
		if got := mustRead(t, filepath.Join(dst, name)); !bytes.Equal(got, content) {
			t.Fatalf("%s restored wrong", name)
		}
	}
}

// Random positional writes, appends and truncations across chunk boundaries:
// after every Sync PostgreSQL holds exactly the local file.
func TestSyncedWritesMatchLocalFile(t *testing.T) {
	pool := migratedPool(t)
	dir := t.TempDir()
	m := acquire(t, pool, Options{})
	attachEmpty(t, m, pool, "commands", dir)
	path := filepath.Join(dir, "commands-QQ.jsonl")
	rng := rand.New(rand.NewSource(7))
	for _, flag := range []int{os.O_CREATE | os.O_RDWR, os.O_CREATE | os.O_RDWR | os.O_APPEND} {
		f := openWrapped(t, m, path, flag)
		for step := 0; step < 60; step++ {
			size, _ := f.Seek(0, io.SeekEnd)
			switch k := rng.Intn(10); {
			case k < 6:
				n := rng.Intn(3*ChunkSize/2) + 1
				if _, err := f.Write(bytes.Repeat([]byte{byte('a' + step%26)}, n)); err != nil {
					t.Fatal(err)
				}
			case k < 8 && size > 0 && flag&os.O_APPEND == 0:
				at := rng.Int63n(size)
				if _, err := f.Seek(at, io.SeekStart); err != nil {
					t.Fatal(err)
				}
				if _, err := f.Write([]byte("OVERWRITE")); err != nil {
					t.Fatal(err)
				}
			default:
				if size > 0 {
					if err := f.Truncate(rng.Int63n(size + 1)); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := f.Sync(); err != nil {
				t.Fatal(err)
			}
			if got, want := remoteBytes(t, pool, "commands", "commands-QQ.jsonl"), mustRead(t, path); !bytes.Equal(got, want) {
				t.Fatalf("flag %x step %d: mirror has %d bytes, local %d", flag, step, len(got), len(want))
			}
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// A positional write past the end extends with zero bytes, as pwrite does.
	f := openWrapped(t, m, path, os.O_RDWR)
	defer f.Close()
	end, _ := f.Seek(0, io.SeekEnd)
	if _, err := f.Seek(end+ChunkSize+7, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if got, want := remoteBytes(t, pool, "commands", "commands-QQ.jsonl"), mustRead(t, path); !bytes.Equal(got, want) {
		t.Fatalf("sparse write: mirror has %d bytes, local %d", len(got), len(want))
	}
}

// A write is acknowledged by Sync. Bytes written but never synced are not in
// the mirror; a handle closed with such bytes leaves the file marked, and the
// next Sync of that file carries the whole file.
func TestUnsyncedWritesAreNotMirroredUntilNextSync(t *testing.T) {
	pool := migratedPool(t)
	dir := t.TempDir()
	m := acquire(t, pool, Options{})
	attachEmpty(t, m, pool, "commands", dir)
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
	if got := remoteBytes(t, pool, "commands", "log.jsonl"); string(got) != "acked\n" {
		t.Fatalf("mirror before sync = %q", got)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if got := remoteBytes(t, pool, "commands", "log.jsonl"); string(got) != "acked\n" {
		t.Fatalf("close must not commit unsynced bytes: %q", got)
	}
	appendSync(t, m, path, "next\n")
	if got := remoteBytes(t, pool, "commands", "log.jsonl"); string(got) != "acked\nunacked\nnext\n" {
		t.Fatalf("resync = %q", got)
	}
}

// An ordinary restart on a disk that kept its files: an unacknowledged tail
// and a file that was never acknowledged are moved to the quarantine
// directory, and the directory holds exactly the acknowledged state.
func TestRestartQuarantinesUnacknowledgedLocalBytes(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()
	dir := t.TempDir()
	m := acquire(t, pool, Options{})
	attachEmpty(t, m, pool, "commands", dir)
	path := filepath.Join(dir, "log.jsonl")
	appendSync(t, m, path, "acked\n")
	m.Close()
	// Crash after the local fsync, before the mirror commit.
	writeFiles(t, dir, map[string][]byte{"log.jsonl": []byte("acked\ntorn"), "stray.jsonl": []byte("never synced")})

	restarted := acquire(t, pool, Options{})
	report, err := restarted.Attach(ctx, "commands", dir, AttachOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Replaced) != 1 || report.Quarantined["log.jsonl"] != 10 || report.Quarantined["stray.jsonl"] != 12 {
		t.Fatalf("report = %+v", report)
	}
	if got := mustRead(t, path); string(got) != "acked\n" {
		t.Fatalf("local after restart = %q", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "stray.jsonl")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stray file left in the journal directory: %v", err)
	}
	if got := mustRead(t, filepath.Join(report.QuarantineDir, "log.jsonl")); string(got) != "acked\ntorn" {
		t.Fatalf("quarantined log = %q", got)
	}
	if got := mustRead(t, filepath.Join(report.QuarantineDir, "stray.jsonl")); string(got) != "never synced" {
		t.Fatalf("quarantined stray = %q", got)
	}
	if report, err := Verify(ctx, pool, "commands", dir); err != nil || !report.Equal() {
		t.Fatalf("verify after restart = %+v, %v", report, err)
	}
}

// F2: whole-file replacement. The replacement commits to PostgreSQL first;
// a crash before the local rename leaves PostgreSQL ahead, and the restart
// on the same disk restores it and keeps the previous local bytes.
func TestAtomicWriteCrashBeforeLocalReplaceRestartsInPlace(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()
	dir := t.TempDir()
	m := acquire(t, pool, Options{})
	attachEmpty(t, m, pool, "browser-events", dir)
	path := filepath.Join(dir, "browser-session-revocations.json")
	write := m.WrapAtomicWrite(os.WriteFile)
	if err := write(path, []byte(`{"revoked":["a"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	crash := errors.New("crash before rename")
	crashing := m.WrapAtomicWrite(func(string, []byte, os.FileMode) error { return crash })
	if err := crashing(path, []byte(`{"revoked":["a","b"]}`), 0o600); !errors.Is(err, crash) {
		t.Fatalf("crashing write = %v", err)
	}
	m.Close()

	restarted := acquire(t, pool, Options{})
	report, err := restarted.Attach(ctx, "browser-events", dir, AttachOptions{})
	if err != nil {
		t.Fatalf("restart after an interrupted replacement = %v", err)
	}
	if got := mustRead(t, path); string(got) != `{"revoked":["a","b"]}` {
		t.Fatalf("restored revocations = %s", got)
	}
	if got := mustRead(t, filepath.Join(report.QuarantineDir, "browser-session-revocations.json")); string(got) != `{"revoked":["a"]}` {
		t.Fatalf("previous local bytes = %s", got)
	}
}

// F2, the reviewer's case: a whole-file local write that never committed
// (written locally before the remote commit, as the previous design did).
// The restart keeps PostgreSQL's acknowledged copy and the unacknowledged
// local bytes in quarantine instead of refusing to start.
func TestWholeFileLocalWriteBeforeCommitRestartsInPlace(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()
	dir := t.TempDir()
	m := acquire(t, pool, Options{})
	attachEmpty(t, m, pool, "browser-events", dir)
	path := filepath.Join(dir, "browser-session-revocations.json")
	if err := m.WrapAtomicWrite(os.WriteFile)(path, []byte(`{"revoked":["a"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	m.Close()
	writeFiles(t, dir, map[string][]byte{"browser-session-revocations.json": []byte(`{"revoked":["A-rewritten"]}`)})

	restarted := acquire(t, pool, Options{})
	report, err := restarted.Attach(ctx, "browser-events", dir, AttachOptions{})
	if err != nil {
		t.Fatalf("restart after an uncommitted whole-file write = %v", err)
	}
	if got := mustRead(t, path); string(got) != `{"revoked":["a"]}` {
		t.Fatalf("local after restart = %s", got)
	}
	if got := mustRead(t, filepath.Join(report.QuarantineDir, "browser-session-revocations.json")); string(got) != `{"revoked":["A-rewritten"]}` {
		t.Fatalf("quarantined bytes = %s", got)
	}
}

// An ambiguous commit (committed, but the writer saw an error) followed by
// the journal's rollback. Either the next Sync makes PostgreSQL equal the
// rolled-back local file, or, if the host restarts first, PostgreSQL's copy
// (with the record) is restored. Both are one consistent history; the local
// bytes are kept.
func TestAmbiguousCommitConvergesToOneHistory(t *testing.T) {
	for _, restartFirst := range []bool{false, true} {
		pool := migratedPool(t)
		ctx := context.Background()
		dir := t.TempDir()
		m := acquire(t, pool, Options{})
		attachEmpty(t, m, pool, "commands", dir)
		path := filepath.Join(dir, "log.jsonl")
		appendSync(t, m, path, "one\n")

		m.flushHook = func(string, string) error { return errors.New("connection reset after COMMIT") }
		f := openWrapped(t, m, path, os.O_RDWR|os.O_APPEND)
		if _, err := f.Write([]byte("two\n")); err != nil {
			t.Fatal(err)
		}
		err := f.Sync()
		var replication *ReplicationError
		if !errors.As(err, &replication) || !replication.LocalDurable() {
			t.Fatalf("ambiguous commit = %v, want *ReplicationError", err)
		}
		m.flushHook = nil
		// agentevents rolls the append back.
		if err := f.Truncate(4); err != nil {
			t.Fatal(err)
		}
		f.Close()
		if got := remoteBytes(t, pool, "commands", "log.jsonl"); string(got) != "one\ntwo\n" {
			t.Fatalf("mirror after ambiguous commit = %q", got)
		}

		if !restartFirst {
			appendSync(t, m, path, "retry\n")
			if got := remoteBytes(t, pool, "commands", "log.jsonl"); string(got) != "one\nretry\n" {
				t.Fatalf("mirror after the next sync = %q", got)
			}
			continue
		}
		m.Close()
		restarted := acquire(t, pool, Options{})
		report, err := restarted.Attach(ctx, "commands", dir, AttachOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got := mustRead(t, path); string(got) != "one\ntwo\n" {
			t.Fatalf("restored after restart = %q", got)
		}
		if got := mustRead(t, filepath.Join(report.QuarantineDir, "log.jsonl")); string(got) != "one\n" {
			t.Fatalf("quarantined = %q", got)
		}
	}
}

// A database older than what this host acknowledged (restored from an older
// backup) is refused, and so is a mirror of another lineage. Nothing
// changes in either copy.
func TestAttachRefusesOlderOrForeignMirror(t *testing.T) {
	ctx := context.Background()
	pool := migratedPool(t)
	dir := t.TempDir()
	m := acquire(t, pool, Options{})
	attachEmpty(t, m, pool, "commands", dir)
	path := filepath.Join(dir, "log.jsonl")
	appendSync(t, m, path, "one\n")
	appendSync(t, m, path, "two\n")
	m.Close()
	// The database goes back one committed change, as a restore of an older
	// backup would.
	if _, err := pool.Exec(ctx, `SELECT api_journal_mirror_truncate('commands', 'log.jsonl', 4)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE api_journal_mirror_files SET gen = gen - 1 WHERE name = 'log.jsonl'`); err != nil {
		t.Fatal(err)
	}
	local, rows := snapshot(t, dir), mirrorRows(t, pool)
	older := acquire(t, pool, Options{})
	if _, err := older.Attach(ctx, "commands", dir, AttachOptions{}); !errors.Is(err, ErrMirrorBehind) {
		t.Fatalf("attach to an older mirror = %v, want ErrMirrorBehind", err)
	}
	older.Close()
	if got := snapshot(t, dir); len(got) != len(local) || got["log.jsonl"] != "one\ntwo\n" {
		t.Fatalf("refusal changed the directory: %v", got)
	}
	if mirrorRows(t, pool) != rows {
		t.Fatal("refusal changed the mirror")
	}

	other := migratedPool(t)
	foreign := acquire(t, other, Options{})
	if _, _, err := InitializeEmpty(ctx, other, "commands", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := foreign.Attach(ctx, "commands", dir, AttachOptions{Mode: AttachAdopt}); !errors.Is(err, ErrWrongMirror) {
		t.Fatalf("attach to another lineage = %v, want ErrWrongMirror", err)
	}
	fresh := migratedPool(t)
	adopter := acquire(t, fresh, Options{})
	if _, err := adopter.Attach(ctx, "commands", dir, AttachOptions{Mode: AttachAdopt}); !errors.Is(err, ErrWrongMirror) {
		t.Fatalf("adopting a replica of another database = %v, want ErrWrongMirror", err)
	}
	if got := snapshot(t, dir); got["log.jsonl"] != "one\ntwo\n" {
		t.Fatalf("refusals changed the directory: %v", got)
	}
}

func TestInitializeEmptyIsIdempotentAndNeverOverwrites(t *testing.T) {
	ctx := context.Background()
	pool := migratedPool(t)
	created, lineage, err := InitializeEmpty(ctx, pool, "commands", "op")
	if err != nil || !created || lineage == "" {
		t.Fatalf("first = %v %q %v", created, lineage, err)
	}
	created, again, err := InitializeEmpty(ctx, pool, "commands", "op")
	if err != nil || created || again != lineage {
		t.Fatalf("second = %v %q %v", created, again, err)
	}
}

func TestWrapRejectsFilesOutsideAttachedDirectories(t *testing.T) {
	pool := migratedPool(t)
	m := acquire(t, pool, Options{})
	attachEmpty(t, m, pool, "commands", t.TempDir())
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
	writeFiles(t, dir, map[string][]byte{"a.jsonl": []byte("one\n"), "b.jsonl": []byte("two\n")})
	m := acquire(t, pool, Options{})
	if _, err := m.Attach(ctx, "commands", dir, AttachOptions{Mode: AttachAdopt}); err != nil {
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
	if _, err := Verify(ctx, pool, "browser-events", dir); !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("verify of an uninitialized mirror = %v", err)
	}
}
