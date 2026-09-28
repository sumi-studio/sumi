package journalmirror

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// F5: one acknowledged append is one pipelined round trip to PostgreSQL and
// sends about the appended bytes, not the 64 KiB chunk. Latency is injected
// on every server-to-client flight, so time/delay counts round trips.
func TestAppendSyncIsOneRoundTrip(t *testing.T) {
	base := migratedPool(t)
	const delay = 20 * time.Millisecond
	proxy := proxyFor(t, base, delay)
	pool := proxy.pool(t, base)
	ctx := context.Background()
	if _, _, err := InitializeEmpty(ctx, base, "commands", "test"); err != nil {
		t.Fatal(err)
	}
	m := acquire(t, pool, Options{LeaseCheckInterval: time.Hour, LeaseCheckTimeout: time.Minute})
	dir := t.TempDir()
	if _, err := m.Attach(ctx, "commands", dir, AttachOptions{}); err != nil {
		t.Fatal(err)
	}
	f := openWrapped(t, m, filepath.Join(dir, "log.jsonl"), os.O_CREATE|os.O_RDWR|os.O_APPEND)
	defer f.Close()
	// Incompressible 500-byte records, so TOAST compression does not hide a
	// chunk rewrite.
	rnd := make([]byte, 374)
	crand.Read(rnd)
	line := append([]byte(base64.StdEncoding.EncodeToString(rnd))[:499], '\n')
	for i := 0; i < 64; i++ { // warm the statement cache; move into a chunk
		f.Write(line)
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	const n = 20
	// The same appends without injected latency: the local fsyncs and the
	// database's own commit time, subtracted below.
	proxy.delay.Store(0)
	started := time.Now()
	for i := 0; i < n; i++ {
		f.Write(line)
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	local := time.Since(started) / n
	proxy.delay.Store(int64(delay))
	var walBefore string
	if err := base.QueryRow(ctx, `SELECT pg_current_wal_insert_lsn()::text`).Scan(&walBefore); err != nil {
		t.Fatal(err)
	}
	up := proxy.up.Load()
	started = time.Now()
	for i := 0; i < n; i++ {
		f.Write(line)
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	per := time.Since(started) / n
	sent := (proxy.up.Load() - up) / n
	var wal int64
	if err := base.QueryRow(ctx, `SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), $1::pg_lsn)::bigint`, walBefore).Scan(&wal); err != nil {
		t.Fatal(err)
	}
	trips := float64(per-local) / float64(delay)
	t.Logf("%v injected per flight: %v per 500-byte Sync, %v without injection = %.2f round trips; %d bytes sent per append; WAL %d bytes/append (shared cluster upper bound)",
		delay, per.Round(time.Millisecond), local.Round(time.Millisecond), trips, sent, wal/n)
	if trips >= 1.5 {
		t.Fatalf("append took %.2f round trips, want 1", trips)
	}
	if sent > 1500 {
		t.Fatalf("append sent %d bytes for a 500-byte record", sent)
	}
}

// F6: restoring a mirror larger than any allowed in-memory copy streams one
// chunk at a time through a slow link, reports progress, and an interrupted
// restore leaves no partial journal and completes on the next attempt.
func TestRestoreStreamsWithBoundedMemoryAndResumes(t *testing.T) {
	if testing.Short() {
		t.Skip("large fixture")
	}
	base := migratedPool(t)
	ctx := context.Background()
	const files, perFile = 4, 16 << 20 // 64 MiB
	src := t.TempDir()
	line := append(bytes.Repeat([]byte("x"), 1023), '\n')
	for i := 0; i < files; i++ {
		if err := os.WriteFile(filepath.Join(src, fmt.Sprintf("events-%d.jsonl", i)), bytes.Repeat(line, perFile/len(line)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	adopter := acquire(t, base, Options{})
	if _, err := adopter.Attach(ctx, "browser-events", src, AttachOptions{Mode: AttachAdopt}); err != nil {
		t.Fatal(err)
	}
	adopter.Close()

	proxy := proxyFor(t, base, 5*time.Millisecond) // a slow link: 5 ms, 16 MiB/s
	proxy.rate.Store(16 << 20)
	pool := proxy.pool(t, base)
	m := acquire(t, pool, Options{})
	dst := t.TempDir()

	// Interrupted after the first file.
	interrupted, cancel := context.WithCancel(ctx)
	_, err := m.Attach(interrupted, "browser-events", dst, AttachOptions{Progress: func(p AttachProgress) {
		if p.FilesDone == 1 {
			cancel()
		}
	}})
	cancel()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted restore = %v", err)
	}
	for name := range snapshot(t, dst) {
		if name != "events-0.jsonl" && filepath.Ext(name) != ".acked" && name != ".events-1.jsonl.restoring" {
			t.Fatalf("interrupted restore left %s", name)
		}
	}

	var peak atomic.Uint64
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	baseline := ms.HeapInuse
	done := make(chan struct{})
	go func() {
		var s runtime.MemStats
		for {
			select {
			case <-done:
				return
			case <-time.After(5 * time.Millisecond):
			}
			runtime.ReadMemStats(&s)
			if s.HeapInuse > peak.Load() {
				peak.Store(s.HeapInuse)
			}
		}
	}()
	var reports []AttachProgress
	started := time.Now()
	report, err := m.Attach(ctx, "browser-events", dst, AttachOptions{Progress: func(p AttachProgress) { reports = append(reports, p) }})
	elapsed := time.Since(started)
	close(done)
	if err != nil {
		t.Fatal(err)
	}
	growth := int64(peak.Load()) - int64(baseline)
	t.Logf("restored %d MiB (%d files) in %v through a 5 ms, 16 MiB/s link (%.1f MiB/s); heap growth %.1f MiB; %d progress reports; %d bytes received",
		(files*perFile)>>20, report.Files, elapsed.Round(time.Millisecond), float64(files*perFile)/(1<<20)/elapsed.Seconds(),
		float64(growth)/(1<<20), len(reports), proxy.down.Load())
	if growth > 16<<20 {
		t.Fatalf("heap grew %d MiB restoring 64 MiB; restore must stream", growth>>20)
	}
	last := reports[len(reports)-1]
	if last.FilesDone != files || last.BytesDone != last.Bytes || len(reports) < files+4 {
		t.Fatalf("progress = %+v (%d reports)", last, len(reports))
	}
	if verify, err := Verify(ctx, base, "browser-events", dst); err != nil || !verify.Equal() {
		t.Fatalf("verify = %+v %v", verify, err)
	}
	if _, err := os.Stat(filepath.Join(dst, ".events-1.jsonl.restoring")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restoring temp left: %v", err)
	}
}

// After a failed or ambiguous commit, the next Sync repairs PostgreSQL's
// copy from the first uncertain offset: a large log is not sent again, so a
// transient failure does not turn every later append into a whole-file
// upload that could exceed the flush timeout.
func TestResyncAfterFailedCommitSendsOnlyTheUncertainTail(t *testing.T) {
	base := migratedPool(t)
	proxy := proxyFor(t, base, 0)
	pool := proxy.pool(t, base)
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "log.jsonl")
	if err := os.WriteFile(path, bytes.Repeat([]byte("history line\n"), (8<<20)/13), 0o600); err != nil {
		t.Fatal(err)
	}
	m := acquire(t, pool, Options{LeaseCheckInterval: time.Hour, LeaseCheckTimeout: time.Minute})
	if _, err := m.Attach(ctx, "commands", dir, AttachOptions{Mode: AttachAdopt}); err != nil {
		t.Fatal(err)
	}
	f := openWrapped(t, m, path, os.O_RDWR|os.O_APPEND)
	defer f.Close()
	end, _ := f.Seek(0, io.SeekEnd)
	m.flushHook = func(string, string) error { return errors.New("connection reset after COMMIT") }
	f.Write([]byte("ambiguous\n"))
	if err := f.Sync(); err == nil {
		t.Fatal("ambiguous commit reported success")
	}
	m.flushHook = nil
	if err := f.Truncate(end); err != nil { // the journal's rollback
		t.Fatal(err)
	}
	up := proxy.up.Load()
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("next\n"))
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	sent := proxy.up.Load() - up
	t.Logf("repair and next append of an 8 MiB log sent %d bytes", sent)
	if sent > 16<<10 {
		t.Fatalf("repair sent %d bytes", sent)
	}
	if verify, err := Verify(ctx, base, "commands", dir); err != nil || !verify.Equal() {
		t.Fatalf("verify = %+v %v", verify, err)
	}
}
