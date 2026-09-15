package filesvc

// A settler's apply can commit while the orphan sweep is surfacing the
// very object it recorded. The object's row must follow it to the
// surfaced name — never destroyed, never left describing an absent
// path while the object sits unrecorded elsewhere.

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAuthPGApplyCommitsBetweenVetoReads(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	s.SetOpTimeout(400 * time.Millisecond)
	if err := os.MkdirAll(dir+"/ws", 0o755); err != nil {
		t.Fatal(err)
	}
	hold := authHold(t, dsn)
	o, bDone := authHeldWrite(t, s, root, hold, "s.txt", "T")
	select {
	case err := <-bDone:
		if !errors.Is(err, ErrUnavailable) {
			t.Fatalf("held write returned %v, want unavailable", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("held write did not time out")
	}
	c := intent{owner: "dead-inst", scope: "ws", op: "write", path: "r.txt",
		version: authMint(t, s), preFP: "0:0:0:0", expectSHA: sha("T"), at: time.Now().Add(-time.Hour)}
	c.id = insertIntent(t, s, c)
	authExec(t, s, `UPDATE file_op SET resolved_at = now() - interval '1 minute' WHERE id=$1`, c.id)
	v, err := root.pin(false)
	if err != nil {
		t.Fatal(err)
	}
	// Delayed executor-side move parks O under an unjournaled private
	// name — an orphan the sweep surfaces, never deletes.
	parked := opStagePrefix + strconv.FormatInt(c.id, 10) + "-p-aa01"
	if err := v.MoveStaged("ws", "s.txt", parked); err != nil {
		t.Fatalf("delayed park move: %v", err)
	}
	v.Close()

	var once sync.Once
	var hookErr error
	s.SetReconcileView(authPinned(root, func(v ReconView) ReconView {
		return authView{ReconView: v, onMove: func(_, from, _ string) {
			if from != parked {
				return
			}
			once.Do(func() {
				// The in-flight apply commits between the sweep's
				// stat and the surface row mint.
				if err := hold.Commit(ctx); err != nil {
					hookErr = err
					return
				}
				deadline := time.Now().Add(30 * time.Second)
				for time.Now().Before(deadline) {
					var fp string
					if s.pool.QueryRow(ctx, `SELECT fp FROM file_version WHERE scope='ws' AND path='s.txt'`).Scan(&fp) == nil &&
						fp3(fp) == fp3(o.Fingerprint) && authSettlersDone(s) {
						return
					}
					time.Sleep(20 * time.Millisecond)
				}
				hookErr = errors.New("settler apply did not commit inside the surface window")
			})
		}}
	}))
	s.lastTombScan.Store(0)
	s.lastStageSweep.Store(0)
	s.Reconcile(ctx)
	// The sweep created the recover intent during that pass; the next
	// pass settles it.
	s.lastStageSweep.Store(0)
	s.Reconcile(ctx)
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	authSettle(t, s)
	where := scanDirFor(t, dir, "ws", []byte("T"))
	if where == "" {
		t.Fatal("object unlinked while its apply was committing")
	}
	// The row must describe the object wherever it ended up: restored
	// to its recorded home s.txt (the home was free), or re-keyed onto
	// a visible recovered-* surface name.
	if _, fp, found := authRow(t, s, where); !found {
		t.Fatalf("no row records the recovered object at %q", where)
	} else if fp3(fp) != fp3(o.Fingerprint) {
		t.Fatalf("row at %q fp %q does not describe O (%q)", where, fp, o.Fingerprint)
	}
	if where != "s.txt" {
		if !strings.Contains(where, "recovered-o") {
			t.Fatalf("O at %q — want s.txt restore or a visible recovered-* surface", where)
		}
		if _, _, found := authRow(t, s, "s.txt"); found {
			t.Fatal("stale row still claims s.txt while the object sits surfaced")
		}
	}
}
