package filesvc

// The discard veto's two reads — "may another writer still record this
// object?" and "does a row record it?" — are separate statements. An apply
// committing between them removes its evidence from the first and adds
// it to the second, so it is caught only if the recorder check is read
// first. This witness lands exactly that commit between the reads.

import (
	"context"
	"errors"
	"os"
	"strconv"
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
	s.SetReconcileView(authPinned(root, nil))
	s.SetOpTimeout(400 * time.Millisecond)
	if err := os.MkdirAll(dir+"/ws", 0o755); err != nil {
		t.Fatal(err)
	}
	vC := authMint(t, s)
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
		version: vC, preFP: "0:0:0:0", expectSHA: sha("T"), at: time.Now().Add(-time.Hour)}
	c.id = insertIntent(t, s, c)
	authExec(t, s, `UPDATE file_op SET resolved_at = now() - interval '1 minute' WHERE id=$1`, c.id)
	v, err := root.pin(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := v.MoveStaged("ws", "s.txt", opStagePrefix+strconv.FormatInt(c.id, 10)+"-p-aa01"); err != nil {
		t.Fatalf("delayed park move: %v", err)
	}
	v.Close()

	var once sync.Once
	var hookErr error
	discardVetoHook = func() {
		once.Do(func() {
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
			hookErr = errors.New("settler apply did not commit inside the veto window")
		})
	}
	defer func() { discardVetoHook = nil }()
	s.lastTombScan.Store(0)
	s.Reconcile(ctx)
	discardVetoHook = nil
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	if scanDirFor(t, dir, "ws", []byte("T")) == "" {
		t.Fatal("object unlinked after its apply committed between the veto's reads")
	}
	authSettle(t, s)
	authAssertRecovered(t, s, root, dir, "s.txt", "T", o)
}
