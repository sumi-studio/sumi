package filesvc

// capture_authority_test.go — private-lineage enforcement on real PG.
//
// Every capture boundary (create, meta, entries, read, release) must
// verify the caller's (owner, epoch) against BOTH the capture row and
// the durable file_freeze barrier. A stale generation gains no grant.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestCaptureNoBarrierRefused: without a durable file_freeze row the
// scope has no authority record — capture creation is refused outright,
// even with a well-formed owner/epoch.
func TestCaptureNoBarrierRefused(t *testing.T) {
	f := capEnv(t)
	root := t.TempDir()
	svc, st, cs := f.newService(t, root)
	defer st.Close()
	defer cs.Close()

	f.wr("x.txt", []byte("x"))
	syncfs(t, f)

	if _, err := cs.Capture(context.Background(), f.scope, f.owner, f.epoch, ""); !errors.Is(err, ErrCaptureStale) {
		t.Fatalf("capture without barrier: %v want ErrCaptureStale", err)
	}
	_ = svc
}

// TestCaptureStaleGeneration: after the barrier advances to a newer
// return lineage, the old generation loses every grant — create, meta,
// entries, read, release — and cannot touch the newer capture either.
func TestCaptureStaleGeneration(t *testing.T) {
	f := capEnv(t)
	root := t.TempDir()
	svc, st, cs := f.newService(t, root)
	defer st.Close()
	defer cs.Close()

	f.wr("a.txt", []byte("alpha"))
	syncfs(t, f)
	old := f.doCapture(t, svc, st, "")

	// Newer return session binds the scope: barrier advances to (sessB, 8).
	if err := st.SetScopeFrozen(context.Background(), f.scope, "sessB", 8,
		"newer-return", true); err != nil {
		t.Fatalf("barrier bump: %v", err)
	}

	// Old generation: every boundary refuses.
	if _, err := cs.Capture(context.Background(), f.scope, "sessA", 7, ""); !errors.Is(err, ErrCaptureStale) {
		t.Fatalf("stale create: %v want ErrCaptureStale", err)
	}
	if _, err := cs.Meta(context.Background(), old.CaptureID, "sessA", 7); !errors.Is(err, ErrCaptureStale) {
		t.Fatalf("stale meta: %v want ErrCaptureStale", err)
	}
	if _, err := cs.Entries(context.Background(), old.CaptureID, "sessA", 7, -1, 10); !errors.Is(err, ErrCaptureStale) {
		t.Fatalf("stale entries: %v want ErrCaptureStale", err)
	}
	if _, err := f.readAll(t, cs, old.CaptureID, 0); !errors.Is(err, ErrCaptureStale) {
		t.Fatalf("stale read: %v want ErrCaptureStale", err)
	}
	if err := cs.Release(context.Background(), old.CaptureID, "sessA", 7); !errors.Is(err, ErrCaptureStale) {
		t.Fatalf("stale release: %v want ErrCaptureStale", err)
	}

	// Newer generation can create — but still cannot address the OLD
	// capture row (it belongs to sessA@7).
	rowB, err := cs.Capture(context.Background(), f.scope, "sessB", 8, "")
	if err != nil {
		t.Fatalf("new-generation create: %v", err)
	}
	if _, err := cs.Meta(context.Background(), old.CaptureID, "sessB", 8); !errors.Is(err, ErrCaptureStale) {
		t.Fatalf("cross-generation read of old capture: %v want ErrCaptureStale", err)
	}
	if _, err := f.entriesByPathOK(cs, rowB.CaptureID, "sessB", 8); err != nil {
		t.Fatalf("new-generation entries: %v", err)
	}
}

// entriesByPathOK is entriesByPath with explicit lineage.
func (f *capFixture) entriesByPathOK(cs *CaptureService, id, owner string, epoch int64) (map[string]captureEntryRow, error) {
	out := map[string]captureEntryRow{}
	var cursor int64 = -1
	for {
		rows, err := cs.Entries(context.Background(), id, owner, epoch, cursor, 500)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			return out, nil
		}
		for _, e := range rows {
			out[string(e.Path)] = e
			cursor = e.Seq
		}
		if len(rows) < 500 {
			return out, nil
		}
	}
}

// TestCaptureExpectedScopeID: retake carries the bound scope_id; it is
// verified against the anchor inside the capture transaction. A
// renamed-out + recreated directory resolves a different inode and the
// retake refuses rather than silently retargeting the replacement.
func TestCaptureExpectedScopeID(t *testing.T) {
	f := capEnv(t)
	root := t.TempDir()
	svc, st, cs := f.newService(t, root)
	defer st.Close()
	defer cs.Close()

	f.wr("orig.txt", []byte("original"))
	syncfs(t, f)
	row1 := f.doCapture(t, svc, st, "")

	// Retake with the correct binding → new capture, same scope_id.
	row2 := f.doCapture(t, svc, st, row1.ScopeID)
	if row2.ScopeID != row1.ScopeID {
		t.Fatal("retake changed scope_id")
	}

	// Retake with a bogus expected id → refused.
	if _, err := cs.Capture(context.Background(), f.scope, f.owner, f.epoch, "si:bogus"); !errors.Is(err, ErrCaptureRefused) {
		t.Fatalf("bogus expected_scope_id: %v want refused", err)
	}

	// Rename scope out, recreate under the same name: replacement dir
	// has a different inode → different scope_id. A retake still bound
	// to the old id must refuse.
	if err := os.Rename(filepath.Join(f.mount, f.scope),
		filepath.Join(f.mount, f.scope+".old")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(f.mount, f.scope), 0o755); err != nil {
		t.Fatal(err)
	}
	syncfs(t, f)
	if _, err := cs.Capture(context.Background(), f.scope, f.owner, f.epoch, row1.ScopeID); !errors.Is(err, ErrCaptureRefused) {
		t.Fatalf("retake after replacement: %v want refused", err)
	}
	// A fresh first-bind (no expectation) on the replacement scope is a
	// NEW scope_id — the API would bind it as a different scope.
	row3 := f.doCapture(t, svc, st, "")
	if row3.ScopeID == row1.ScopeID {
		t.Fatal("replacement scope kept old scope_id")
	}

	// Restore for any later fixture use (scope dir back in place).
	os.RemoveAll(filepath.Join(f.mount, f.scope))
	os.Rename(filepath.Join(f.mount, f.scope+".old"), filepath.Join(f.mount, f.scope))
}

// TestCaptureReadSurvivesScopeContentChange: a fixed old capture keeps
// serving while the lineage stays current — renaming a file INSIDE the
// scope post-capture does not invalidate authority.
func TestCaptureReadSurvivesScopeContentChange(t *testing.T) {
	f := capEnv(t)
	root := t.TempDir()
	svc, st, cs := f.newService(t, root)
	defer st.Close()
	defer cs.Close()

	f.wr("before.txt", []byte("immutable"))
	syncfs(t, f)
	row := f.doCapture(t, svc, st, "")
	byPath := f.entriesByPath(t, cs, row.CaptureID)
	e := byPath["before.txt"]

	// Rename inside the scope — the barrier (keyed by scope name) is
	// untouched, the anchor inode unchanged: authority remains valid.
	if err := os.Rename(filepath.Join(f.mount, f.scope, "before.txt"),
		filepath.Join(f.mount, f.scope, "after.txt")); err != nil {
		t.Fatal(err)
	}
	syncfs(t, f)

	got, err := f.readAll(t, cs, row.CaptureID, e.Seq)
	if err != nil {
		t.Fatalf("read after rename: %v", err)
	}
	if string(got) != "immutable" {
		t.Fatalf("bytes %q", got)
	}
}

// TestCaptureLineageRace: a capture admission racing a barrier advance
// must linearize — whichever commits second sees the winner's
// authority. Post-conditions are deterministic: the newer barrier
// stands, the old generation is refused everywhere, the new one works.
func TestCaptureLineageRace(t *testing.T) {
	f := capEnv(t)
	root := t.TempDir()
	svc, st, cs := f.newService(t, root)
	defer st.Close()
	defer cs.Close()

	f.wr("r.txt", []byte("race"))
	syncfs(t, f)
	_ = svc
	// Standing barrier for the old generation.
	if err := st.SetScopeFrozen(context.Background(), f.scope, "sessA", 7,
		"old-return", true); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var capErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, capErr = cs.Capture(context.Background(), f.scope, "sessA", 7, "")
	}()
	go func() {
		defer wg.Done()
		// Newer generation binds the scope (epoch 8 > 7 always wins).
		_ = st.SetScopeFrozen(context.Background(), f.scope, "sessB", 8, "newer-return", true)
	}()
	wg.Wait()

	// The newer barrier must stand (8 > 7 always wins the CAS).
	if err := st.assertCaptureLineage(context.Background(), f.scope, "sessB", 8); err != nil {
		t.Fatalf("newer barrier not current: %v", err)
	}
	// Whether or not the racing capture was admitted, the old lineage
	// now gets nothing new.
	if _, err := cs.Capture(context.Background(), f.scope, "sessA", 7, ""); !errors.Is(err, ErrCaptureStale) {
		t.Fatalf("post-race stale create: %v want ErrCaptureStale", err)
	}
	if err := st.assertCaptureLineage(context.Background(), f.scope, "sessA", 7); !errors.Is(err, ErrCaptureStale) {
		t.Fatalf("post-race stale lineage check: %v", err)
	}
	// And the new lineage can capture cleanly.
	if _, err := cs.Capture(context.Background(), f.scope, "sessB", 8, ""); err != nil {
		t.Fatalf("new-lineage create after race: %v", err)
	}
	t.Logf("racing capture result: %v (either outcome legal; post-state verified)", capErr)
}
