package filesvc

// 175: recovery must judge the object it actually finds at the private
// name, not the object it expected. A journaled exchange that lands late
// (after the owning intent died) replaces the slot's staged copy with the
// recorded public object C. Content equality (C's bytes equal the staged
// copy's) is not discard authority: C is a recorded, acknowledged object.

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// authCaptureView lands one exchange of the slot with a.txt immediately
// before the reconciler moves the slot's occupant — the position of the
// dead intent's own delayed RENAME_EXCHANGE (the publish step populates
// the private name with the displaced object).
type authCaptureView struct {
	ReconView
	once *sync.Once
	dir  string
	slot string
}

func (v authCaptureView) MoveStaged(scope, from, to string) error {
	if from == v.slot {
		v.once.Do(func() {
			dfd, err := os.Open(v.dir + "/" + scope)
			if err != nil {
				return
			}
			defer dfd.Close()
			// The delayed exchange: staged S onto a.txt, recorded C
			// into the private name.
			_ = unix.Renameat2(int(dfd.Fd()), v.slot,
				int(dfd.Fd()), "a.txt", unix.RENAME_EXCHANGE)
		})
	}
	return v.ReconView.MoveStaged(scope, from, to)
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

// X, an earlier write of "H" at a.txt, is tombstoned with its staged
// copy S under its journaled name and a declared-but-unresulted xch —
// the crash shape of dying between issuing the exchange and recording
// its result. C, a later acknowledged write of the same bytes, is
// recorded at a.txt. This pass stats S, then X's delayed exchange lands:
// the slot now holds C and a.txt holds S. Judged by content alone C
// would be unlinked as a redundant authored copy — destroying a recorded
// object. The protocol requires C preserved and surfaced with a row.
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
	slot := opStagePrefix + "x" + strconv.FormatInt(x.id, 10)
	authExec(t, s, `UPDATE file_op SET names = $2::jsonb,
		resolved_at = now() - interval '1 minute' WHERE id=$1`,
		x.id, mustJSON([]nameRec{{Name: slot, Act: "xch", Src: "a.txt"}}))
	if err := os.WriteFile(dir+"/ws/"+slot, []byte("H"), 0o644); err != nil {
		t.Fatal(err)
	}

	var once sync.Once
	s.SetReconcileView(authPinned(root, func(v ReconView) ReconView {
		return authCaptureView{ReconView: v, once: &once, dir: dir, slot: slot}
	}))
	s.lastTombScan.Store(0)
	s.Reconcile(ctx)
	if !authInodeExists(t, dir, cIno) {
		t.Fatalf("recorded object C (ino %d) was unlinked: content equality was treated as discard authority", cIno)
	}
	// C surfaced at a visible recovered name with a row; a.txt holds
	// the late-landed authored bytes.
	surfaced := scanDirForInode(dir, "ws", cInoS)
	if surfaced == "" {
		t.Fatal("C is nowhere — destroyed")
	}
	if got, err := os.ReadFile(surfaced); err != nil || string(got) != "H" {
		t.Fatalf("surfaced object %q holds %q err=%v, want C's bytes", surfaced, got, err)
	}
	surfRel := strings.TrimPrefix(surfaced, dir+"/ws/")
	if _, _, found := authRow(t, s, surfRel); !found {
		t.Fatalf("no row records the surfaced object at %q", surfRel)
	}
	if got, _ := authReadOpt(dir, "ws/a.txt"); got != "H" {
		t.Fatalf("a.txt = %q, want the landed authored bytes", got)
	}
}
