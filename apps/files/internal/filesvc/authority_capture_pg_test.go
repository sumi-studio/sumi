package filesvc

// 175: the discard veto must judge the object it actually captured. A
// hash-only (own-bytes) match accepts any inode with the expected bytes,
// so the captured object can differ from the one this pass statted.

import (
	"context"
	"os"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"
)

// authCaptureView lands one exchange of the slot with a.txt immediately
// before the real RemoveStagedVeto captures the slot — the position of
// the intent's own delayed RENAME_EXCHANGE (atomicWrite publishes by
// exchanging its slot with the name).
type authCaptureView struct {
	ReconView
	once *sync.Once
	slot string
}

func (v authCaptureView) RemoveStagedVeto(scope, path, wantFP3, wantSHA string,
	veto func(captured FileInfo) (bool, error)) error {
	if path == v.slot {
		v.once.Do(func() { _ = v.ReconView.SwapStaged(scope, v.slot, "a.txt") })
	}
	return v.ReconView.RemoveStagedVeto(scope, path, wantFP3, wantSHA, veto)
}

func authInodeExists(t *testing.T, dir string, ino uint64) bool {
	t.Helper()
	ents, err := os.ReadDir(dir + "/ws")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		fi, err := os.Lstat(dir + "/ws/" + e.Name())
		if err != nil {
			continue
		}
		if st, ok := fi.Sys().(*syscall.Stat_t); ok && st.Ino == ino {
			return true
		}
	}
	return false
}

// X, an earlier write of "H" at a.txt, is tombstoned with its own staged
// copy S at its slot; C, a later acknowledged write of the same bytes, is
// recorded at a.txt. This pass stats S, then X's delayed exchange lands:
// the slot now holds C and a.txt holds S. The capture takes C (hash
// matches). Judged by S's identity, C looks unrecorded and is unlinked —
// the recorded object is gone and its row can never match again. Judged
// by C's identity, C is recorded elsewhere and is kept, then restored.
func TestAuthPGVetoJudgesCapturedObject(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := newPGStore(t, dsn, dir)
	vX := authMint(t, s)
	if _, _, err := s.WithWrite(ctx, "ws", "a.txt", "write", IfVersion{Mode: "any"},
		sha("H"), authProbe(root, "a.txt"), authWriteFn(root, "a.txt", "H")); err != nil {
		t.Fatalf("write C: %v", err)
	}
	c, err := root.stat("ws", "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	cInoS, _, _, _ := fpParts(c.Fingerprint)
	cIno, _ := strconv.ParseUint(cInoS, 10, 64)

	x := intent{owner: "dead-inst", scope: "ws", op: "write", path: "a.txt",
		version: vX, preFP: "0:0:0:0", expectSHA: sha("H"), at: time.Now().Add(-time.Hour)}
	x.id = insertIntent(t, s, x)
	authExec(t, s, `UPDATE file_op SET resolved_at = now() - interval '1 minute' WHERE id=$1`, x.id)
	slot := opStagePrefix + strconv.FormatInt(x.id, 10)
	if err := os.WriteFile(dir+"/ws/"+slot, []byte("H"), 0o644); err != nil {
		t.Fatal(err)
	}

	var once sync.Once
	s.SetReconcileView(authPinned(root, func(v ReconView) ReconView {
		return authCaptureView{ReconView: v, once: &once, slot: slot}
	}))
	s.lastTombScan.Store(0)
	s.Reconcile(ctx)
	if !authInodeExists(t, dir, cIno) {
		t.Fatalf("recorded object C (ino %d) was unlinked: the veto judged the pre-capture identity", cIno)
	}
	authSettle(t, s)
	authAssertRecovered(t, s, root, dir, "a.txt", "H", c)
}
