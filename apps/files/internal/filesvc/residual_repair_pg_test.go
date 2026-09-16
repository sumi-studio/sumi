package filesvc

// Residual-repair regressions: F253 (CI single-pass deferral),
// F256/B-N1 (interloper swap beneath an owned private name),
// F257/B-N2 (post-commit bookkeeping skipped on early return), and
// F258/B-N3 (age-only .filesv-tmp destruction). Failures injected
// through ReconView wrappers are labelled injected; nothing here
// claims a real after-death filesystem completion witness.

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// failHashWhileArmed fails every Hash beneath a path substring while
// armed — a persistent-through-pass content-unverifiable deferral
// (a single hash failure is re-judged inside the same settleNames loop
// via the done-record restat, so it cannot produce the CI signature).
type failHashWhileArmed struct {
	ReconView
	prefix string
	armed  *bool
}

func (w *failHashWhileArmed) Hash(scope, path string) (string, error) {
	if *w.armed && w.prefix != "" && strings.Contains(path, w.prefix) {
		return "", ErrUnavailable
	}
	return w.ReconView.Hash(scope, path)
}

// failStatNthView fails the n-th Stat beneath a path substring — the
// F253 settleNames observation deferral. The orphan sweep's recursive
// walk already stats the private name once while listing, so the
// settle pass sees the n-th call.
type failStatNthView struct {
	ReconView
	prefix string
	n      int
	count  *int
}

func (w *failStatNthView) Stat(scope, path string) (FileInfo, error) {
	if w.prefix != "" && strings.Contains(path, w.prefix) {
		*w.count++
		if *w.count == w.n {
			return FileInfo{}, ErrUnavailable
		}
	}
	return w.ReconView.Stat(scope, path)
}

// failListOnceView fails the first ListDir beneath a path substring —
// the F257 member-walk deferral injection.
type failListOnceView struct {
	ReconView
	prefix string
	once   *bool
}

func (w *failListOnceView) ListDir(scope, dir string) ([]string, error) {
	if *w.once && w.prefix != "" && strings.Contains(dir, w.prefix) {
		*w.once = false
		return nil, ErrUnavailable
	}
	return w.ReconView.ListDir(scope, dir)
}

// swapOnMoveView replaces the source of a MoveStaged call with foreign
// bytes before the real rename — the F256 interloper deposit swapped
// into an owned private name inside the stat/DB window.
type swapOnMoveView struct {
	ReconView
	dir    string
	match  string
	once   *bool
	bytVal []byte
}

func (w *swapOnMoveView) MoveStaged(scope, from, to string) error {
	if *w.once && strings.Contains(filepath.Base(from), w.match) {
		*w.once = false
		p := filepath.Join(w.dir, scope, from)
		if err := os.Remove(p); err == nil {
			_ = os.WriteFile(p, w.bytVal, 0o644)
		}
	}
	return w.ReconView.MoveStaged(scope, from, to)
}

// intentNames reads back the journaled names array for intent id.
func intentNames(t *testing.T, s *Store, id int64) []nameRec {
	t.Helper()
	var raw []byte
	err := s.pool.QueryRow(context.Background(),
		`SELECT names FROM file_op WHERE id=$1`, id).Scan(&raw)
	if err != nil {
		t.Fatalf("read names journal: %v", err)
	}
	var out []nameRec
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("names journal: %v", err)
	}
	return out
}

// --- F256/B-N1: an interloper swapped into an owned private name is
// never destroyed by a stat-then-unlink -------------------------------

// The recorded object at a.txt is hardlinked into a dead intent's
// private name; the home still holds it. Between the home check and
// the settle move an interloper swaps foreign bytes into the capture
// name — every disposition must preserve them and keep a.txt intact.
func TestResidualInterloperSwapSurfaces(t *testing.T) {
	dsn := pgDSN(t)
	s, root, dir := finalStore(t, dsn)
	ctx := context.Background()

	putFile(t, dir, "ws/a.txt", "D0")
	fpD := durFP(t, root, "ws", "a.txt")
	// Row+event as an ordinary write would record them.
	authExec(t, s,
		`INSERT INTO file_version (scope,path,version,fp,content_sha) VALUES ('ws','a.txt',70,$1,$2)`,
		fpD, sha("D0"))

	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "rename",
		path: "old.txt", toPath: "a.txt",
		version: 75, dstFP: fpD,
		at: time.Now().Add(-time.Hour),
	})
	slot := opStagePrefix + strconv.FormatInt(id, 10)
	// The private name holds a SECOND LINK to the same object — the
	// disposition that once stat-checked then unlinked it.
	if err := os.Link(dir+"/ws/a.txt", dir+"/ws/"+slot); err != nil {
		t.Fatal(err)
	}

	once := true
	s.SetReconcileView(authPinned(root, func(v ReconView) ReconView {
		// The capture move retargets .filesv-op-<id> to
		// .filesv-op-c<id>-<rand>; swap foreign bytes in just before
		// the SETTLE move — the window the old unlink could not see.
		return &swapOnMoveView{ReconView: v, dir: dir, match: ".filesv-op-c", once: &once, bytVal: []byte("INTERLOPER")}
	}))
	s.Reconcile(ctx)

	if got := durRead(t, dir, "ws/a.txt"); got != "D0" {
		t.Fatalf("a.txt = %q — recorded home must be untouched", got)
	}
	where := scanTreeFor(t, dir, "ws", []byte("INTERLOPER"))
	if where == "" {
		t.Fatal("interloper deposit destroyed — the swap inside the stat/unlink window was unlinked")
	}
	if !strings.Contains(where, "recovered-") {
		t.Fatalf("interloper at %q — foreign bytes must surface visibly, not hide under a private name", where)
	}
	// The recorded object's second link surfaced too — nothing is
	// unlinked; an extra visible link is the accepted disposition.
	authSettle(t, s)
	if got := durRead(t, dir, "ws/a.txt"); got != "D0" {
		t.Fatalf("a.txt = %q after settle", got)
	}
}

// A duplicate link at an owned private name (no swap — plain dedup
// postcondition) surfaces as a visible extra link rather than being
// unlinked: the object is safe at home and the private link is
// accounted for.
func TestResidualDuplicateLinkSurfaces(t *testing.T) {
	dsn := pgDSN(t)
	s, root, dir := finalStore(t, dsn)

	putFile(t, dir, "ws/a.txt", "D0")
	fpD := durFP(t, root, "ws", "a.txt")
	authExec(t, s,
		`INSERT INTO file_version (scope,path,version,fp,content_sha) VALUES ('ws','a.txt',70,$1,$2)`,
		fpD, sha("D0"))

	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "rename",
		path: "old.txt", toPath: "a.txt",
		version: 75, dstFP: fpD,
		at: time.Now().Add(-time.Hour),
	})
	slot := opStagePrefix + strconv.FormatInt(id, 10)
	if err := os.Link(dir+"/ws/a.txt", dir+"/ws/"+slot); err != nil {
		t.Fatal(err)
	}

	authSettle(t, s)
	if got := durRead(t, dir, "ws/a.txt"); got != "D0" {
		t.Fatalf("a.txt = %q", got)
	}
	// The duplicate link must surface at a visible recovered-* name —
	// unlinked would leave exactly one name for the inode.
	var extra int
	matches, _ := filepath.Glob(filepath.Join(dir, "ws", "*recovered-*"))
	for _, m := range matches {
		if b, err := os.ReadFile(m); err == nil && string(b) == "D0" {
			extra++
		}
	}
	if extra == 0 {
		t.Fatal("duplicate link unlinked instead of surfaced — a stat-then-unlink window remains")
	}
}

// --- F257/B-N2: post-commit bookkeeping retries after interruption ---

// A prior pass committed the surface row+event then died before
// minting member rows and dropping the stale claim: the existing
// recover event must NOT short-circuit the cleanup — the record only
// drains once bookkeeping converges.
func TestResidualRecoverEventFollowsUp(t *testing.T) {
	dsn := pgDSN(t)
	s, root, dir := finalStore(t, dsn)
	ctx := context.Background()

	if err := os.MkdirAll(dir+"/ws/recovered-o9-aa", 0o755); err != nil {
		t.Fatal(err)
	}
	putFile(t, dir, "ws/recovered-o9-aa/m.txt", "MEMBER")
	dst, err := root.stat("ws", "recovered-o9-aa")
	if err != nil {
		t.Fatal(err)
	}
	// A stale claim provably describing the surfaced dir at an absent
	// path — dropStaleClaims must remove it on the follow-up pass.
	authExec(t, s,
		`INSERT INTO file_version (scope,path,version,fp,content_sha,oid) VALUES ('ws','gone-dir',66,$1,'dir',$2)`,
		dst.Fingerprint, dst.Oid)

	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "gone-dir",
		version: 66,
		names: []nameRec{{
			Name: ".filesv-op-" + strconv.FormatInt(1, 10) + "-zz",
			Dir:  "",
			Home: "recovered-o9-aa",
			Seen: []string{obsIdent(dst)},
		}},
		at: time.Now().Add(-time.Hour),
	})
	priv := ".filesv-op-1-zz"
	// The seeded postcondition: the surface row AND the recover event
	// are already durable — the pre-fix code returned established here
	// and skipped the member mint + claim cleanup forever.
	authExec(t, s,
		`INSERT INTO file_version (scope,path,version,fp,content_sha,oid) VALUES ('ws','recovered-o9-aa',90,$1,'dir',$2)`,
		dst.Fingerprint, dst.Oid)
	authExec(t, s,
		`INSERT INTO file_event (scope,path,from_path,op,version) VALUES ('ws','recovered-o9-aa',$1,'recover',90)`,
		priv)

	s.Reconcile(ctx)

	// The member must have its own truthful row — minted on the
	// follow-up, not skipped by the early return.
	var mv int64
	err = s.pool.QueryRow(ctx,
		`SELECT version FROM file_version WHERE scope='ws' AND path='recovered-o9-aa/m.txt'`).Scan(&mv)
	if errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("member row never minted — existing recover event short-circuited bookkeeping")
	}
	// The stale claim at the absent path must be gone.
	if _, _, ok := authRow(t, s, "gone-dir"); ok {
		t.Fatal("stale claim survived — dropStaleClaims skipped on the early-return path")
	}
	// The record may only drain once bookkeeping is complete.
	for _, rec := range intentNames(t, s, id) {
		if rec.Name == priv && !rec.Done {
			t.Fatal("record drained with bookkeeping still outstanding")
		}
	}
}

// A failure INSIDE the follow-up walk keeps the record live for the
// next pass — directory member convergence retries, it never silently
// marks done.
func TestResidualCleanupFailureRetries(t *testing.T) {
	dsn := pgDSN(t)
	s, root, dir := finalStore(t, dsn)
	ctx := context.Background()

	if err := os.MkdirAll(dir+"/ws/recovered-o9-bb", 0o755); err != nil {
		t.Fatal(err)
	}
	putFile(t, dir, "ws/recovered-o9-bb/m.txt", "MEMBER")
	dst, err := root.stat("ws", "recovered-o9-bb")
	if err != nil {
		t.Fatal(err)
	}
	priv := ".filesv-op-2-zz"
	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "gone-dir2",
		version: 67,
		names: []nameRec{{
			Name: priv,
			Home: "recovered-o9-bb",
			Seen: []string{obsIdent(dst)},
		}},
		at: time.Now().Add(-time.Hour),
	})
	authExec(t, s,
		`INSERT INTO file_version (scope,path,version,fp,content_sha,oid) VALUES ('ws','recovered-o9-bb',91,$1,'dir',$2)`,
		dst.Fingerprint, dst.Oid)
	authExec(t, s,
		`INSERT INTO file_event (scope,path,from_path,op,version) VALUES ('ws','recovered-o9-bb',$1,'recover',91)`,
		priv)

	// Injected: the member-walk ListDir fails once.
	once := true
	s.SetReconcileView(authPinned(root, func(v ReconView) ReconView {
		return &failListOnceView{ReconView: v, prefix: "recovered-o", once: &once}
	}))
	s.Reconcile(ctx)

	// The bookkeeping failed — the record must still be live.
	done := false
	for _, rec := range intentNames(t, s, id) {
		if rec.Name == priv && rec.Done {
			done = true
		}
	}
	if done {
		t.Fatal("record marked done while member mint failed — lost retry")
	}

	// Healed: the next pass finishes the bookkeeping.
	s.SetReconcileView(authPinned(root, nil))
	s.Reconcile(ctx)
	var mv int64
	err = s.pool.QueryRow(ctx,
		`SELECT version FROM file_version WHERE scope='ws' AND path='recovered-o9-bb/m.txt'`).Scan(&mv)
	if errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("member row never minted after retry")
	}
}

// --- F253: discriminating reproduction of the CI signature -----------

// The CI run failed the single-pass assertion while the private name
// provably re-attached. The attach → capture → judge → settle chain
// has deferral points that retry the journaled record on a transient
// error: each produces the identical CI signature (new.txt absent
// after one pass, bytes preserved) and converges next pass. Injected
// once at each candidate point — a one-shot transient is the only
// mechanism consistent with the log and the deterministic local pass.
func TestResidualRenameDeferredSettleConverges(t *testing.T) {
	for _, mode := range []string{"stat", "move", "hash"} {
		t.Run(mode, func(t *testing.T) {
			dsn := pgDSN(t)
			s, root, dir := finalStore(t, dsn)
			ctx := context.Background()

			putFile(t, dir, "ws/new.txt", "D0")
			fpD := durFP(t, root, "ws", "new.txt")
			authExec(t, s,
				`INSERT INTO file_version (scope,path,version,fp,content_sha) VALUES ('ws','new.txt',70,$1,$2)`,
				fpD, sha("D0"))

			id := insertIntent(t, s, intent{
				owner: "dead-inst", scope: "ws", op: "rename",
				path: "old.txt", toPath: "new.txt",
				version: 75, dstFP: fpD,
				at: time.Now().Add(-time.Hour),
			})
			slot := opStagePrefix + strconv.FormatInt(id, 10)
			if err := os.Rename(dir+"/ws/new.txt", dir+"/ws/"+slot); err != nil {
				t.Fatal(err)
			}

			armed := true
			count := 0
			s.SetReconcileView(authPinned(root, func(v ReconView) ReconView {
				switch mode {
				case "stat":
					// The sweep's walk stats the slot once; the
					// settleNames observation is the second call.
					return &failStatNthView{ReconView: v, prefix: slot, n: 2, count: &count}
				case "move":
					once := true
					return &failMoveOnceView{ReconView: v, once: &once}
				default:
					return &failHashWhileArmed{ReconView: v, prefix: "filesv-op", armed: &armed}
				}
			}))
			s.Reconcile(ctx)

			// The exact CI signature: the recorded home is absent after
			// one pass, but the bytes are preserved somewhere beneath
			// the scope and the record remains live to retry.
			if durExists(t, dir, "ws/new.txt") {
				t.Fatalf("%s injection: settled in one pass — the injected deferral was not exercised", mode)
			}
			if where := scanTreeFor(t, dir, "ws", []byte("D0")); where == "" {
				t.Fatalf("%s injection: D0 destroyed", mode)
			}

			armed = false
			s.SetReconcileView(authPinned(root, nil))
			s.lastTombScan.Store(0)
			s.lastStageSweep.Store(0)
			s.Reconcile(ctx)
			if got := durRead(t, dir, "ws/new.txt"); got != "D0" {
				t.Fatalf("%s injection: new.txt = %q after retry — deferred settlement did not converge", mode, got)
			}
		})
	}
}
