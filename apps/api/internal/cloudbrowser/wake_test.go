package cloudbrowser

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

func (f *fixture) refreshPending(profile string) (string, bool) {
	f.t.Helper()
	var state string
	var pending bool
	if err := f.store.Pool.QueryRow(context.Background(), `SELECT state,refresh_requested_at IS NOT NULL FROM cloud_browser_profiles WHERE profile_id=$1`, profile).Scan(&state, &pending); err != nil {
		f.t.Fatal(err)
	}
	return state, pending
}

func fakeWorker(t *testing.T, answer string) (*httptest.Server, *atomic.Int32) {
	var wakes atomic.Int32
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wakes.Add(1)
		w.Write([]byte(answer))
	}))
	t.Cleanup(worker.Close)
	return worker, &wakes
}

// Review F1 (regression of the reviewer's repro, which saw 4 wakes in 7 s and
// refresh pending forever): a profile left state='live' by a Worker without a
// browser (start refused after begin, DO eviction) gets one refresh wake;
// the Worker's non-hosting answer ends the refresh.
func TestRefreshForAWorkerWithoutBrowserStopsAfterOneWake(t *testing.T) {
	f := setup(t)
	profile := f.profile()
	if status, s := f.begin(profile, true, 0); status != 200 || s.Incarnation != 1 {
		t.Fatalf("begin %d %+v", status, s)
	}
	f.grant(profile, uuid.NewString(), true)
	worker, wakes := fakeWorker(t, `{"accepted":false,"phase":"unavailable"}`)
	f.service.WakeURL = worker.URL
	ctx := context.Background()
	for deadline := time.Now().Add(7 * time.Second); time.Now().Before(deadline); time.Sleep(250 * time.Millisecond) {
		f.service.Sweep(ctx)
	}
	state, pending := f.refreshPending(profile)
	t.Logf("wakes in 7s=%d state=%s refresh_pending=%v", wakes.Load(), state, pending)
	if wakes.Load() != 1 || pending {
		t.Fatalf("expected one wake and no pending refresh; wakes=%d pending=%v", wakes.Load(), pending)
	}
	// A later change is delivered (one wake again), not suppressed.
	f.grant(profile, uuid.NewString(), true)
	f.service.mu.Lock()
	delete(f.service.marks, profile)
	f.service.mu.Unlock()
	if n := f.service.Sweep(ctx); n != 1 || wakes.Load() != 2 {
		t.Fatalf("a new change wakes once: sent=%d wakes=%d", n, wakes.Load())
	}
	// A request newer than the one the wake was sent for stays pending.
	var answered time.Time
	if err := f.store.Pool.QueryRow(ctx, `UPDATE cloud_browser_profiles SET refresh_requested_at=now()-interval '1 second' WHERE profile_id=$1 RETURNING refresh_requested_at`, profile).Scan(&answered); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Pool.Exec(ctx, `UPDATE cloud_browser_profiles SET refresh_requested_at=now() WHERE profile_id=$1`, profile); err != nil {
		t.Fatal(err)
	}
	if err := f.store.DropRefresh(ctx, profile, answered); err != nil {
		t.Fatal(err)
	}
	if _, pending := f.refreshPending(profile); !pending {
		t.Fatal("a refresh requested after the wake was answered was dropped")
	}
}

// A live Worker keeps receiving refresh wakes until it calls /refresh.
func TestRefreshForAHostingWorkerStaysPending(t *testing.T) {
	f := setup(t)
	profile := f.profile()
	f.begin(profile, true, 0)
	f.grant(profile, uuid.NewString(), true)
	worker, wakes := fakeWorker(t, `{"accepted":true,"phase":"live"}`)
	f.service.WakeURL = worker.URL
	if n := f.service.Sweep(context.Background()); n != 1 || wakes.Load() != 1 {
		t.Fatalf("sent=%d", n)
	}
	if _, pending := f.refreshPending(profile); !pending {
		t.Fatal("a live host's refresh was dropped before it fetched it")
	}
}

// Review F1: a Worker that refused a start (Browser Run limit) answers with
// retry_after_ms; queued work does not wake it again before then.
func TestRefusedStartHoldsWorkWakes(t *testing.T) {
	f := setup(t)
	profile := f.profile()
	attachment := f.grant(profile, uuid.NewString(), true)
	f.enqueue(attachment, "observe")
	worker, wakes := fakeWorker(t, `{"accepted":false,"phase":"unavailable","retry_after_ms":60000}`)
	f.service.WakeURL = worker.URL
	ctx := context.Background()
	if n := f.service.Sweep(ctx); n != 1 {
		t.Fatalf("first work wake: %d", n)
	}
	for i := 0; i < 3; i++ {
		f.service.mu.Lock()
		delete(f.service.marks, profile) // past the ordinary 10 s spacing
		f.service.mu.Unlock()
		if n := f.service.Sweep(ctx); n != 0 {
			t.Fatalf("held profile woken again (%d)", i)
		}
	}
	f.service.mu.Lock()
	delete(f.service.marks, profile)
	f.service.holds[profile] = time.Now().Add(-time.Second)
	f.service.mu.Unlock()
	if n := f.service.Sweep(ctx); n != 1 || wakes.Load() != 2 {
		t.Fatalf("after the hold the work wakes again: sent=%d wakes=%d", n, wakes.Load())
	}
	// retry_after_ms is bounded.
	worker2, _ := fakeWorker(t, `{"accepted":false,"phase":"unavailable","retry_after_ms":999999999999}`)
	f.service.WakeURL = worker2.URL
	f.service.mu.Lock()
	delete(f.service.marks, profile)
	delete(f.service.holds, profile)
	f.service.mu.Unlock()
	f.service.Sweep(ctx)
	f.service.mu.Lock()
	until := f.service.holds[profile]
	f.service.mu.Unlock()
	if time.Until(until) > maxHold+time.Second {
		t.Fatalf("hold not bounded: %v", time.Until(until))
	}
}

// Review F6: a rejection reported for an older key never marks a key saved
// since; a rejection of the current key does.
func TestJevRejectionOnlyMarksTheRefusedKey(t *testing.T) {
	f := setup(t)
	profile := f.profile()
	if status, _ := f.call("PUT", "/api/cloud-browser/jev-key", owner, map[string]any{"api_key": "jev-synthetic-old-key"}); status != 200 {
		t.Fatal("set old key")
	}
	_, old := f.begin(profile, true, 0)
	if old.JevKey != "jev-synthetic-old-key" || old.JevKeyVersion == 0 {
		t.Fatalf("begin carries the key and its version: %v %d", old.JevKey != "", old.JevKeyVersion)
	}
	if status, _ := f.call("PUT", "/api/cloud-browser/jev-key", owner, map[string]any{"api_key": "jev-synthetic-new-key"}); status != 200 {
		t.Fatal("set new key")
	}
	jev := func(version int64) (int, any) {
		status, out := f.call("POST", "/api/cloud-browser-host/profiles/"+profile+"/jev-rejected", runtime, map[string]any{"key_version": version})
		return status, out["rejected"]
	}
	if status, rejected := jev(old.JevKeyVersion); status != 200 || rejected != false {
		t.Fatalf("old key's rejection: %d %v", status, rejected)
	}
	var rejected bool
	if err := f.store.Pool.QueryRow(context.Background(), `SELECT rejected FROM cloud_browser_jev_credentials WHERE human_id=$1`, owner).Scan(&rejected); err != nil || rejected {
		t.Fatalf("the new key was marked rejected by the old key's failure: %v", err)
	}
	if status, _ := jev(0); status != 404 {
		t.Fatalf("a rejection without a key version: %d", status)
	}
	_, now := f.begin(profile, true, 0)
	if now.JevKey != "jev-synthetic-new-key" || now.JevKeyVersion == old.JevKeyVersion {
		t.Fatal("new key version")
	}
	if status, rejected := jev(now.JevKeyVersion); status != 200 || rejected != true {
		t.Fatalf("current key's rejection: %d %v", status, rejected)
	}
}
