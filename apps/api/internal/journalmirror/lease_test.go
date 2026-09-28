package journalmirror

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func terminateLease(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(), `
		SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
		WHERE datname = current_database() AND application_name = 'sumi-journal-mirror-lease'`).Scan(&n)
	if err != nil || n != 1 {
		t.Fatalf("terminate lease session: %d sessions, %v", n, err)
	}
}

func lostChannel() (chan error, func(error)) {
	ch := make(chan error, 1)
	return ch, func(err error) { ch <- err }
}

// F4: a second process waits for the lease instead of taking the mirror
// from a running owner, and gets it as soon as the owner closes.
func TestLeaseWaitsForTheRunningOwner(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()
	old := acquire(t, pool, Options{Holder: "old-host"})
	dir := t.TempDir()
	attachEmpty(t, old, pool, "commands", dir)

	short, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	_, err := Acquire(short, pool, Options{Holder: "new-host"})
	cancel()
	if !errors.Is(err, ErrLeaseHeld) || !strings.Contains(err.Error(), "old-host") {
		t.Fatalf("acquire while owned = %v, want ErrLeaseHeld naming the holder", err)
	}
	appendSync(t, old, filepath.Join(dir, "log.jsonl"), "still owner\n")

	got := make(chan *Mirror, 1)
	go func() {
		m, err := Acquire(ctx, pool, Options{Holder: "new-host"})
		if err != nil {
			t.Error(err)
		}
		got <- m
	}()
	select {
	case <-got:
		t.Fatal("new owner acquired while the old one still held the lease")
	case <-time.After(1500 * time.Millisecond):
	}
	closed := time.Now()
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	var newer *Mirror
	select {
	case newer = <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("new owner did not acquire after the old one closed")
	}
	t.Cleanup(func() { newer.Close() })
	t.Logf("lease handed over %v after Close", time.Since(closed).Round(time.Millisecond))
	if newer.Epoch() <= old.Epoch() {
		t.Fatalf("epochs old=%d new=%d", old.Epoch(), newer.Epoch())
	}
	f := openWrapped(t, old, filepath.Join(dir, "log.jsonl"), os.O_RDWR|os.O_APPEND)
	defer f.Close()
	f.Write([]byte("after close\n"))
	if err := f.Sync(); !errors.Is(err, ErrClosed) {
		t.Fatalf("sync after close = %v, want ErrClosed", err)
	}
}

// F4: a lease session that ends (database restart, failover, an operator's
// pg_terminate_backend) is reported at once through OnLost, not at the next
// write, and later writes fail without touching the mirror.
func TestEndedLeaseSessionIsReportedPromptly(t *testing.T) {
	pool := migratedPool(t)
	lost, onLost := lostChannel()
	m := acquire(t, pool, Options{OnLost: onLost, LeaseCheckInterval: time.Minute, LeaseCheckTimeout: time.Minute})
	dir := t.TempDir()
	attachEmpty(t, m, pool, "commands", dir)
	appendSync(t, m, filepath.Join(dir, "log.jsonl"), "before\n")

	started := time.Now()
	terminateLease(t, pool)
	select {
	case err := <-lost:
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("OnLost = %v, want ErrLeaseLost", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ended lease session not reported")
	}
	t.Logf("lease loss reported %v after the session ended (check interval 1 min)", time.Since(started).Round(time.Millisecond))
	f := openWrapped(t, m, filepath.Join(dir, "log.jsonl"), os.O_RDWR|os.O_APPEND)
	defer f.Close()
	f.Write([]byte("after\n"))
	if err := f.Sync(); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("sync after lease loss = %v", err)
	}
	if got := remoteBytes(t, pool, "commands", "log.jsonl"); string(got) != "before\n" {
		t.Fatalf("mirror = %q", got)
	}
	// Another process can take over at once.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	next, err := Acquire(ctx, pool, Options{})
	if err != nil {
		t.Fatal(err)
	}
	next.Close()
}

// F4: a lease connection that silently stops answering (a network partition
// without a reset) is detected within LeaseCheckInterval+LeaseCheckTimeout.
func TestSilentLeasePartitionIsDetectedWithinTheWindow(t *testing.T) {
	base := migratedPool(t)
	proxy := proxyFor(t, base, 0)
	pool := proxy.pool(t, base)
	const interval, timeout = 400 * time.Millisecond, 400 * time.Millisecond
	lost, onLost := lostChannel()
	acquire(t, pool, Options{OnLost: onLost, LeaseCheckInterval: interval, LeaseCheckTimeout: timeout})
	time.Sleep(2 * interval) // healthy checks pass
	select {
	case err := <-lost:
		t.Fatalf("lease reported lost while healthy: %v", err)
	default:
	}
	proxy.stalled.Store(true)
	started := time.Now()
	select {
	case err := <-lost:
		elapsed := time.Since(started)
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("OnLost = %v", err)
		}
		if elapsed > interval+timeout+500*time.Millisecond {
			t.Fatalf("partition detected after %v, window is %v", elapsed, interval+timeout)
		}
		t.Logf("partition detected after %v (window %v)", elapsed.Round(time.Millisecond), interval+timeout)
	case <-time.After(10 * time.Second):
		t.Fatal("partition not detected")
	}
	proxy.stalled.Store(false)
}

// F4 property: writers keep appending while their host loses the lease and
// another host takes over. Every append the old host acknowledged is in the
// new host's restored files, and the mirror does not change after the new
// host acquired.
func TestAcknowledgedWritesSurviveTakeover(t *testing.T) {
	pool := migratedPool(t)
	ctx := context.Background()
	lost, onLost := lostChannel()
	old := acquire(t, pool, Options{Holder: "old-host", OnLost: onLost})
	dir := t.TempDir()
	attachEmpty(t, old, pool, "commands", dir)

	const writers = 4
	var mu sync.Mutex
	acked := map[string]bool{}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			path := filepath.Join(dir, fmt.Sprintf("log-%d.jsonl", w))
			f := openWrapped(t, old, path, os.O_CREATE|os.O_RDWR|os.O_APPEND)
			defer f.Close()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				line := fmt.Sprintf("{\"writer\":%d,\"i\":%d,\"pad\":%q}\n", w, i, strings.Repeat("x", 680))
				if _, err := f.Write([]byte(line)); err != nil {
					return
				}
				if err := f.Sync(); err != nil {
					// The journal would roll back; stop this writer.
					return
				}
				mu.Lock()
				acked[line] = true
				mu.Unlock()
			}
		}(w)
	}
	time.Sleep(300 * time.Millisecond)
	terminateLease(t, pool)
	newer, err := Acquire(ctx, pool, Options{Holder: "new-host"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { newer.Close() })
	atTakeover := mirrorRows(t, pool)
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	select {
	case <-lost:
	case <-time.After(3 * time.Second):
		t.Fatal("old host was not told it lost the lease")
	}
	if mirrorRows(t, pool) != atTakeover {
		t.Fatal("the old host changed the mirror after the new host acquired")
	}
	restored := t.TempDir()
	if _, err := newer.Attach(ctx, "commands", restored, AttachOptions{}); err != nil {
		t.Fatal(err)
	}
	present := map[string]bool{}
	for w := 0; w < writers; w++ {
		content, err := os.ReadFile(filepath.Join(restored, fmt.Sprintf("log-%d.jsonl", w)))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		for _, line := range strings.SplitAfter(string(content), "\n") {
			if line != "" {
				present[line] = true
			}
		}
	}
	for line := range acked {
		if !present[line] {
			t.Fatalf("acknowledged append missing after takeover: %.40s", line)
		}
	}
	if len(acked) == 0 {
		t.Fatal("no appends were acknowledged")
	}
	t.Logf("%d acknowledged appends, all restored", len(acked))
}
