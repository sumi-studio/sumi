package filesvc

// Residual-repair regressions: F253 (CI single-pass deferral),
// F256/B-N1 (interloper swap beneath an owned private name),
// F257/B-N2 (post-commit bookkeeping skipped on early return),
// F258/B-N3 (age-only .filesv-tmp destruction), F267 (stale act
// results certifying a later journaled act), and F270 (unknown-outcome
// exchange cleanup). Failures injected through ReconView wrappers are
// labelled injected; nothing here claims a real after-death filesystem
// completion witness.

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
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sys/unix"
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

// fakeClaimRows is the narrow injected boundary for dropStaleClaims:
// a row stream that yields fixed rows, then reports err from Err() —
// modelling a real pgx stream that dies mid-iteration — or fails a
// row's Scan.
type fakeClaimRows struct {
	rows    [][]any
	i       int
	scanErr error
	err     error
}

func (f *fakeClaimRows) Next() bool {
	if f.i < len(f.rows) {
		f.i++
		return true
	}
	return false
}
func (f *fakeClaimRows) Scan(dest ...any) error {
	if f.scanErr != nil {
		return f.scanErr
	}
	for i, v := range f.rows[f.i-1] {
		*(dest[i].(*string)) = v.(string)
	}
	return nil
}
func (f *fakeClaimRows) Err() error { return f.err }
func (f *fakeClaimRows) Close()     {}

// An incomplete claim scan — mid-iteration stream error or a Scan
// failure — must not be treated as successful bookkeeping: the journal
// record stays live for retry (F257). Injected boundary via
// claimRowsFn; the record/settle path underneath is real PG.
func TestResidualClaimStreamErrorRetries(t *testing.T) {
	dsn := pgDSN(t)
	priv := ".filesv-op-3-zz"

	// seed surfaces recovered-o9-cc with its row+event already present,
	// so settleRow takes the existing-event bookkeeping path.
	seed := func(t *testing.T) (*Store, FileInfo, int64) {
		s, root, dir := finalStore(t, dsn)
		if err := os.MkdirAll(dir+"/ws/recovered-o9-cc", 0o755); err != nil {
			t.Fatal(err)
		}
		dst, err := root.stat("ws", "recovered-o9-cc")
		if err != nil {
			t.Fatal(err)
		}
		id := insertIntent(t, s, intent{
			owner: "dead-inst", scope: "ws", op: "write", path: "gone0",
			version: 68,
			names: []nameRec{{
				Name: priv,
				Home: "recovered-o9-cc",
				Seen: []string{obsIdent(dst)},
			}},
			at: time.Now().Add(-time.Hour),
		})
		authExec(t, s,
			`INSERT INTO file_version (scope,path,version,fp,content_sha,oid) VALUES ('ws','recovered-o9-cc',92,$1,'dir',$2)`,
			dst.Fingerprint, dst.Oid)
		authExec(t, s,
			`INSERT INTO file_event (scope,path,from_path,op,version) VALUES ('ws','recovered-o9-cc',$1,'recover',92)`,
			priv)
		return s, dst, id
	}
	isDone := func(t *testing.T, s *Store, id int64) bool {
		for _, rec := range intentNames(t, s, id) {
			if rec.Name == priv {
				return rec.Done
			}
		}
		t.Fatal("record missing")
		return false
	}
	ctx := context.Background()

	// The claim stream dies after yielding one row — unprocessed
	// candidates may remain, so bookkeeping is incomplete.
	t.Run("stream-error", func(t *testing.T) {
		s, dst, id := seed(t)
		s.claimRowsFn = func(context.Context, string, string, string) (claimRows, error) {
			return &fakeClaimRows{
				rows: [][]any{{"gone-a", dst.Fingerprint, dst.Oid}},
				err:  errors.New("stream aborted mid-iteration"),
			}, nil
		}
		s.Reconcile(ctx)
		if isDone(t, s, id) {
			t.Fatal("record marked done while the claim scan died mid-iteration")
		}
		s.claimRowsFn = nil
		authSettle(t, s)
		if !isDone(t, s, id) {
			t.Fatal("record still live after healed retry")
		}
	})

	// A row that fails Scan cannot be certified — same incomplete
	// bookkeeping.
	t.Run("scan-error", func(t *testing.T) {
		s, dst, id := seed(t)
		s.claimRowsFn = func(context.Context, string, string, string) (claimRows, error) {
			return &fakeClaimRows{
				rows:    [][]any{{"gone-a", dst.Fingerprint, dst.Oid}},
				scanErr: errors.New("scan type mismatch"),
			}, nil
		}
		s.Reconcile(ctx)
		if isDone(t, s, id) {
			t.Fatal("record marked done with an unreadable claim row")
		}
		s.claimRowsFn = nil
		authSettle(t, s)
		if !isDone(t, s, id) {
			t.Fatal("record still live after healed retry")
		}
	})
}

// A one-connection pool must not self-wait: dropStaleClaims
// materializes and closes the candidate stream before deleting, so the
// delete never competes with an open result stream for a connection.
// Before the materialization fix this deadlock-for-15s'd every pass
// (the stream held the only conn while Exec waited for one).
func TestResidualStaleClaimSingleConnPool(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	ctx := context.Background()
	dir := t.TempDir()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := &Store{pool: pool, dsn: dsn, opTimeout: 30 * time.Second, dbTimeout: 15 * time.Second,
		owner: "inst-1conn-" + randHex(4), rootID: dir,
		done: make(chan struct{}), reconcile: make(chan struct{}, 1)}
	if err := s.acquireWriter(ctx); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	if err := s.migrate(ctx); err != nil {
		s.Close()
		t.Fatal(err)
	}
	if err := s.bindRoot(ctx); err != nil {
		s.Close()
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	s.SetReconcileView(authPinned(root, nil))
	if err := os.MkdirAll(dir+"/ws", 0o755); err != nil {
		t.Fatal(err)
	}

	priv := ".filesv-op-3-zz"
	deep := "recovered-o9-dd"
	if err := os.MkdirAll(dir+"/ws/"+deep, 0o755); err != nil {
		t.Fatal(err)
	}
	dst, err := root.stat("ws", deep)
	if err != nil {
		t.Fatal(err)
	}
	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "gone4",
		version: 70,
		names: []nameRec{{
			Name: priv,
			Home: deep,
			Seen: []string{obsIdent(dst)},
		}},
		at: time.Now().Add(-time.Hour),
	})
	authExec(t, s,
		`INSERT INTO file_version (scope,path,version,fp,content_sha,oid) VALUES ('ws',$1,94,$2,'dir',$3)`,
		deep, dst.Fingerprint, dst.Oid)
	authExec(t, s,
		`INSERT INTO file_event (scope,path,from_path,op,version) VALUES ('ws',$1,$2,'recover',94)`,
		deep, priv)
	// A stale claim: same object identity, absent path — must be
	// dropped during settleRow's bookkeeping, on one pooled conn.
	authExec(t, s,
		`INSERT INTO file_version (scope,path,version,fp,content_sha) VALUES ('ws','stale-claim',95,$1,'dir')`,
		dst.Fingerprint)

	s.Reconcile(ctx)
	for _, rec := range intentNames(t, s, id) {
		if rec.Name == priv && !rec.Done {
			t.Fatal("record still live on a 1-conn pool — bookkeeping self-waited")
		}
	}
	var gone int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM file_version WHERE scope='ws' AND path='stale-claim'`).
		Scan(&gone); err != nil {
		t.Fatal(err)
	}
	if gone != 0 {
		t.Fatal("stale claim row survived a complete bookkeeping pass")
	}
}

// --- F257: deep-tree member mint progress ----------------------------

// A member mint over a tree deeper than the former 256-level defensive
// bound must now COMPLETE: the API places no depth limit on supported
// trees, and "record stays live but re-walks the same prefix forever"
// is not progress. First a mid-walk ListDir fault proves incomplete
// bookkeeping still keeps the record live; once healed, the deepest
// member's row is minted and the record settles (F257/B-N2).
func TestResidualMemberMintDeepTreeCompletes(t *testing.T) {
	dsn := pgDSN(t)
	s, root, dir := finalStore(t, dsn)
	ctx := context.Background()

	deep := "recovered-o9-dd"
	leafRel := deep + strings.Repeat("/d", 260) + "/leaf.txt"
	cur := dir + "/ws/" + deep + strings.Repeat("/d", 260)
	if err := os.MkdirAll(cur, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cur+"/leaf.txt", []byte("leaf"), 0o644); err != nil {
		t.Fatal(err)
	}
	dst, err := root.stat("ws", deep)
	if err != nil {
		t.Fatal(err)
	}
	priv := ".filesv-op-4-zz"
	id := insertIntent(t, s, intent{
		owner: "dead-inst", scope: "ws", op: "write", path: "gone3",
		version: 69,
		names: []nameRec{{
			Name: priv,
			Home: deep,
			Seen: []string{obsIdent(dst)},
		}},
		at: time.Now().Add(-time.Hour),
	})
	authExec(t, s,
		`INSERT INTO file_version (scope,path,version,fp,content_sha,oid) VALUES ('ws',$1,93,$2,'dir',$3)`,
		deep, dst.Fingerprint, dst.Oid)
	authExec(t, s,
		`INSERT INTO file_event (scope,path,from_path,op,version) VALUES ('ws',$1,$2,'recover',93)`,
		deep, priv)

	// A mid-walk observation failure is still honest: incomplete
	// bookkeeping keeps the record live instead of claiming done.
	once := true
	s.SetReconcileView(authPinned(root, func(v ReconView) ReconView {
		return &failListOnceView{ReconView: v, prefix: deep + "/d", once: &once}
	}))
	s.Reconcile(ctx)
	if !durExists(t, dir, "ws/"+deep) {
		t.Fatal("surfaced dir missing")
	}
	for _, rec := range intentNames(t, s, id) {
		if rec.Name == priv && rec.Done {
			t.Fatal("record done while the member walk was incomplete")
		}
	}

	// Healed: the full depth is walked, the deepest file gets a usable
	// version row, and the record drains.
	s.SetReconcileView(authPinned(root, nil))
	s.Reconcile(ctx)
	for _, rec := range intentNames(t, s, id) {
		if rec.Name == priv && !rec.Done {
			t.Fatal("deep member mint did not complete after faults healed")
		}
	}
	var leafVer int64
	if err := s.pool.QueryRow(ctx,
		`SELECT version FROM file_version WHERE scope='ws' AND path=$1`,
		leafRel).Scan(&leafVer); err != nil {
		t.Fatalf("deepest member has no usable version row: %v", err)
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
			// Bounded in-pass progress: a persistent deferral appends at
			// most one fresh record per pass — the journal must not grow
			// without bound while the condition holds (regression guard
			// for the fresh-record re-entry fix).
			if n := len(intentNames(t, s, id)); n > 6 {
				t.Fatalf("%s injection: names journal grew to %d records in one pass — unbounded capture churn", mode, n)
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

// bindOidView fabricates a bound durable oid from the object's dev:ino
// — the identity ext4/tmpfs actually vend to every stat through
// name_to_handle_at. It reproduces the b117-CI condition (bound live
// oid, intent declared fp/sha only) on any filesystem.
type bindOidView struct{ ReconView }

func (w bindOidView) Stat(scope, path string) (FileInfo, error) {
	st, err := w.ReconView.Stat(scope, path)
	if err == nil && st.DevIno != "" {
		st.Oid = "h:test:" + st.DevIno
	}
	return st, err
}

// The b117 CI failure (F253 reopened): on filesystems where
// name_to_handle_at succeeds — ext4, tmpfs — every stat carries a
// bound oid, while the intent declared the destination by fp/sha only.
// ambiguousMeaning treated "bound live oid, no declared oid" as proof
// of a foreign object and surfaced D0 at recovered-* — permanently,
// silently — instead of restoring it to its recorded home. The fix:
// a bound live oid decides only roles that declared an oid.
func TestResidualBoundOidRestoresHome(t *testing.T) {
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

	s.SetReconcileView(authPinned(root, func(v ReconView) ReconView {
		return bindOidView{v}
	}))
	for pass := 0; pass < 4; pass++ {
		s.Reconcile(ctx)
		if durExists(t, dir, "ws/new.txt") {
			if got := durRead(t, dir, "ws/new.txt"); got != "D0" {
				t.Fatalf("pass %d: new.txt holds %q, not D0", pass, got)
			}
			return
		}
		if where := scanTreeFor(t, dir, "ws", []byte("D0")); where == "" {
			t.Fatalf("pass %d: D0 destroyed", pass)
		}
	}
	t.Fatal("D0 surfaced instead of restored on a bound-oid filesystem")
}

// --- F267/F270: per-action journal results + unknown-outcome exchange ---

// journalActRes reads names[i]'s durable act/res — exactly what a crash
// survivor sees.
func journalActRes(t *testing.T, s *Store, id int64, i int) (act, res string) {
	t.Helper()
	err := s.pool.QueryRow(context.Background(),
		`SELECT coalesce(names->`+strconv.Itoa(i)+`->>'act',''),
		        coalesce(names->`+strconv.Itoa(i)+`->>'res','')
		 FROM file_op WHERE id=$1`, id).Scan(&act, &res)
	if err != nil {
		t.Fatalf("journal read intent %d names[%d]: %v", id, i, err)
	}
	return act, res
}

// F267: at each act-transition crash window the durable journal must
// read the in-flight act with an OPEN result — a previous act's res=ok
// must never certify a later act (such a stale ok once read as
// "exchange applied" and misrouted captured content). The hook only
// reads the durable row mid-op; nothing is injected into the operation.
func TestResidualActResultSeparation(t *testing.T) {
	dsn := pgDSN(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		seed func(t *testing.T, dir string)
		op   func(s *Store, root *posixRoot) error
		hook string
		act  string
	}{
		{
			name: "write-xch",
			seed: func(t *testing.T, dir string) { putFile(t, dir, "ws/victim.txt", "V") },
			op: func(s *Store, root *posixRoot) error {
				_, _, err := s.WithWrite(ctx, "ws", "victim.txt", "write",
					IfVersion{Mode: "any"}, sha("B2"), authProbe(root, "victim.txt"),
					func(it intent) (FileInfo, bool, error) {
						return root.atomicWrite("ws", "victim.txt", []byte("B2"), false, it)
					})
				return err
			},
			hook: "write.postXch", act: "xch",
		},
		{
			name: "write-pub",
			op: func(s *Store, root *posixRoot) error {
				_, _, err := s.WithWrite(ctx, "ws", "fresh.txt", "write",
					IfVersion{Mode: "any"}, sha("B2"), authProbe(root, "fresh.txt"),
					func(it intent) (FileInfo, bool, error) {
						return root.atomicWrite("ws", "fresh.txt", []byte("B2"), false, it)
					})
				return err
			},
			hook: "write.postPub", act: "pub",
		},
		{
			name: "rename-xch",
			seed: func(t *testing.T, dir string) {
				putFile(t, dir, "ws/old.txt", "S")
				putFile(t, dir, "ws/new.txt", "D")
			},
			op: func(s *Store, root *posixRoot) error {
				_, _, err := s.Rename(ctx, "ws", "old.txt", "new.txt",
					IfVersion{Mode: "any"}, authProbe(root, "new.txt"), authProbe(root, "old.txt"),
					func(it intent) (FileInfo, bool, error) {
						return root.rename("ws", "old.txt", "new.txt", false, it)
					})
				return err
			},
			hook: "rename.postXch", act: "xch",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, root, dir := finalStore(t, dsn)
			if tc.seed != nil {
				tc.seed(t, dir)
			}
			saw := false
			root.faultHook = func(tag string) {
				if tag != tc.hook {
					return
				}
				saw = true
				var id int64
				if err := s.pool.QueryRow(ctx,
					`SELECT id FROM file_op ORDER BY id DESC LIMIT 1`).Scan(&id); err != nil {
					t.Errorf("latest intent id: %v", err)
					return
				}
				if act, res := journalActRes(t, s, id, 0); act != tc.act || res != "" {
					t.Errorf("at %s durable journal reads act=%q res=%q — want %q with open result",
						tag, act, res, tc.act)
				}
			}
			if err := tc.op(s, root); err != nil {
				t.Fatalf("op: %v", err)
			}
			if !saw {
				t.Fatalf("fault hook %q never fired", tc.hook)
			}
		})
	}

	// The transition itself: journal.act must clear res in the same
	// write — the earlier act's success cannot outlive it even across a
	// crash between journal updates (the pre-fix two-write shape could).
	t.Run("act-transition-clears-res", func(t *testing.T) {
		s, _, _ := finalStore(t, dsn)
		id := insertIntent(t, s, intent{
			owner: s.owner, scope: "ws", op: "write", path: "u.txt",
			version: 1, at: time.Now(),
			names: []nameRec{{Name: ".filesv-op-u", Act: "cap", Res: "ok"}},
		})
		nj := &nameJournal{s: s, id: id}
		nj.act(0, "xch", "ws/dst.txt")
		if act, res := journalActRes(t, s, id, 0); act != "xch" || res != "" {
			t.Fatalf("after act transition durable journal reads act=%q res=%q — want xch/open", act, res)
		}
	})
}

// F267 interrupted-state recovery: a rename killed between the xch act
// journal and its result now leaves act=xch res="" — the parked
// occupant is judged by provenance and the captured source returns to
// its recorded home. Seeded postcondition over the real reconcile path
// — the durable row exactly as the fixed producer leaves it at the
// kill, not a new crash witness.
func TestResidualInterruptedXchRestoresSource(t *testing.T) {
	dsn := pgDSN(t)
	s, root, dir := finalStore(t, dsn)
	ctx := context.Background()
	// Real writes mint the version rows a live rename would have been
	// declared on — the row at src.txt claiming S is what authorizes
	// the captured occupant's return home.
	for _, w := range [][2]string{{"src.txt", "S"}, {"dst.txt", "D"}} {
		if _, _, err := s.WithWrite(ctx, "ws", w[0], "write",
			IfVersion{Mode: "any"}, sha(w[1]), authProbe(root, w[0]),
			authWriteFn(root, w[0], w[1])); err != nil {
			t.Fatalf("seed write %s: %v", w[0], err)
		}
	}
	fiS, err := root.stat("ws", "src.txt")
	if err != nil {
		t.Fatal(err)
	}
	fiD, err := root.stat("ws", "dst.txt")
	if err != nil {
		t.Fatal(err)
	}
	slot := ".filesv-op-ixch"
	// The cap act MOVED the source's object into the slot — same inode,
	// so its fp3 still matches the declared preFP. (A fresh copy would
	// not: fingerprints are ino:size:mtime.)
	if err := os.Rename(filepath.Join(dir, "ws", "src.txt"), filepath.Join(dir, "ws", slot)); err != nil {
		t.Fatal(err)
	}
	insertIntent(t, s, intent{
		owner: "dead-xch", scope: "ws", op: "rename",
		path: "src.txt", toPath: "dst.txt",
		version: 1, preFP: fiS.Fingerprint, dstFP: fiD.Fingerprint,
		srcKind: "file", expectSHA: sha("S"), dstSHA: sha("D"),
		at:    time.Now().Add(-deadGrace - time.Second),
		names: []nameRec{{Name: slot, Act: "xch", Res: "", Src: "dst.txt"}},
	})

	authSettle(t, s)

	if got := mustRead(filepath.Join(dir, "ws", "src.txt")); got != "S" {
		t.Fatalf("src.txt = %q — captured source not restored to recorded home", got)
	}
	if got := mustRead(filepath.Join(dir, "ws", "dst.txt")); got != "D" {
		t.Fatalf("dst.txt = %q — destination content displaced", got)
	}
	if n := scanDirFor(t, dir, "ws", []byte("S")); n != "src.txt" {
		t.Fatalf("S surfaced at %q instead of its recorded home", n)
	}
	if n := scanDirForPrivate(dir, "ws"); n != "" {
		t.Fatalf("private residue %q", n)
	}
	if _, _, res := boundaryIntent(t, s, "rename", "src.txt"); !res {
		t.Fatal("interrupted intent not resolved")
	}
}

// Legacy-shape witness: a pre-fix producer could leave act=xch res=ok
// with the source still parked. The reader must not give that stale
// success false authority — the occupant is not the declared displaced
// object, so it surfaces preserved at a public name rather than being
// destroyed or restored under the stale claim. The fixed producer can
// no longer emit this shape.
func TestResidualStaleXchOkPreservedNotAuthoritative(t *testing.T) {
	dsn := pgDSN(t)
	s, root, dir := finalStore(t, dsn)
	ctx := context.Background()
	for _, w := range [][2]string{{"src.txt", "S"}, {"dst.txt", "D"}} {
		if _, _, err := s.WithWrite(ctx, "ws", w[0], "write",
			IfVersion{Mode: "any"}, sha(w[1]), authProbe(root, w[0]),
			authWriteFn(root, w[0], w[1])); err != nil {
			t.Fatalf("seed write %s: %v", w[0], err)
		}
	}
	fiS, err := root.stat("ws", "src.txt")
	if err != nil {
		t.Fatal(err)
	}
	fiD, err := root.stat("ws", "dst.txt")
	if err != nil {
		t.Fatal(err)
	}
	slot := ".filesv-op-ixok"
	if err := os.Rename(filepath.Join(dir, "ws", "src.txt"), filepath.Join(dir, "ws", slot)); err != nil {
		t.Fatal(err)
	}
	insertIntent(t, s, intent{
		owner: "dead-xok", scope: "ws", op: "rename",
		path: "src.txt", toPath: "dst.txt",
		version: 1, preFP: fiS.Fingerprint, dstFP: fiD.Fingerprint,
		srcKind: "file", expectSHA: sha("S"), dstSHA: sha("D"),
		at:    time.Now().Add(-deadGrace - time.Second),
		names: []nameRec{{Name: slot, Act: "xch", Res: "ok", Src: "dst.txt"}},
	})

	authSettle(t, s)

	// S preserved at a public recovered name — never destroyed, and
	// never restored to src.txt under the stale res=ok.
	n := scanDirFor(t, dir, "ws", []byte("S"))
	if n == "" || containsPrivateSeg(n) {
		t.Fatalf("stale-ok occupant destroyed or left private: %q", n)
	}
	if n == "src.txt" {
		t.Fatalf("stale res=ok given false home authority — S restored to %q", n)
	}
	if got := mustRead(filepath.Join(dir, "ws", "dst.txt")); got != "D" {
		t.Fatalf("dst.txt = %q — destination content displaced", got)
	}
	if n := scanDirForPrivate(dir, "ws"); n != "" {
		t.Fatalf("private residue %q", n)
	}
}

// --- F270: unknown-outcome exchange -----------------------------------

// A non-ENOENT exchange error can be a lost reply — the syscall may
// have committed. The producer must not stat/unlink the private slot
// while the outstanding exchange can still fill it; the intent is
// tombstoned in place and reconciliation owns the outcome.
func TestResidualWriteXchLostReplyParks(t *testing.T) {
	dsn := pgDSN(t)
	s, root, dir := finalStore(t, dsn)
	putFile(t, dir, "ws/victim.txt", "V")
	ctx := context.Background()

	// The honest errno case: exchange reports failure and never lands.
	root.xchFn = func(int, string, int, string) error { return unix.EIO }
	_, _, err := s.WithWrite(ctx, "ws", "victim.txt", "write",
		IfVersion{Mode: "any"}, sha("B2"), authProbe(root, "victim.txt"),
		func(it intent) (FileInfo, bool, error) {
			return root.atomicWrite("ws", "victim.txt", []byte("B2"), false, it)
		})
	if !errors.Is(err, errUndoParked) {
		t.Fatalf("write err = %v — want errUndoParked", err)
	}
	if got := mustRead(filepath.Join(dir, "ws", "victim.txt")); got != "V" {
		t.Fatalf("victim.txt = %q — untouched content changed", got)
	}
	// The staged body must still occupy the private slot — the old
	// fail() path could stat-then-unlink it here.
	_, _, res := boundaryIntent(t, s, "write", "victim.txt")
	if !res {
		t.Fatal("parked intent not tombstoned")
	}
	id, _, _ := boundaryIntent(t, s, "write", "victim.txt")
	slot := loadIntentName(t, s, id, 0)
	if got := mustRead(filepath.Join(dir, "ws", slot)); got != "B2" {
		t.Fatalf("slot occupant = %q — staged body destroyed before verdict", got)
	}

	authSettle(t, s)

	// Reconciliation drains the parked slot: B2 preserved at a public
	// name, victim.txt untouched, private slot drained.
	if n := scanDirFor(t, dir, "ws", []byte("B2")); n == "" || containsPrivateSeg(n) {
		t.Fatalf("staged body not surfaced at a public name: %q", n)
	}
	if got := mustRead(filepath.Join(dir, "ws", "victim.txt")); got != "V" {
		t.Fatalf("victim.txt = %q after settle", got)
	}
	if n := scanDirForPrivate(dir, "ws"); n != "" {
		t.Fatalf("private residue %q", n)
	}
}

// The reply-lost-after-commit case: the exchange lands, only the
// acknowledgment is lost. Parking keeps both objects; reconciliation
// rolls the landed write forward and surfaces the displaced occupant.
func TestResidualWriteXchLostReplyLanded(t *testing.T) {
	dsn := pgDSN(t)
	s, root, dir := finalStore(t, dsn)
	putFile(t, dir, "ws/victim.txt", "V")
	ctx := context.Background()

	root.xchFn = func(ofd int, on string, nfd int, nn string) error {
		if err := unix.Renameat2(ofd, on, nfd, nn, unix.RENAME_EXCHANGE); err != nil {
			return err
		}
		return unix.EIO // committed, reply lost
	}
	_, _, err := s.WithWrite(ctx, "ws", "victim.txt", "write",
		IfVersion{Mode: "any"}, sha("B2"), authProbe(root, "victim.txt"),
		func(it intent) (FileInfo, bool, error) {
			return root.atomicWrite("ws", "victim.txt", []byte("B2"), false, it)
		})
	if !errors.Is(err, errUndoParked) {
		t.Fatalf("write err = %v — want errUndoParked", err)
	}
	if got := mustRead(filepath.Join(dir, "ws", "victim.txt")); got != "B2" {
		t.Fatalf("victim.txt = %q — exchange did not land", got)
	}
	id, _, _ := boundaryIntent(t, s, "write", "victim.txt")
	slot := loadIntentName(t, s, id, 0)
	if got := mustRead(filepath.Join(dir, "ws", slot)); got != "V" {
		t.Fatalf("slot occupant = %q — displaced content destroyed before verdict", got)
	}

	authSettle(t, s)

	// Landed bytes roll forward at the path; the displaced object is
	// preserved at a public name — never unlinked.
	if got := mustRead(filepath.Join(dir, "ws", "victim.txt")); got != "B2" {
		t.Fatalf("victim.txt = %q after settle — landed content not rolled forward", got)
	}
	if n := scanDirFor(t, dir, "ws", []byte("V")); n == "" || n == "victim.txt" || containsPrivateSeg(n) {
		t.Fatalf("displaced occupant not preserved at a public name: %q", n)
	}
	if n := scanDirForPrivate(dir, "ws"); n != "" {
		t.Fatalf("private residue %q", n)
	}
}

// The rename producer has the same window: an unknown-outcome exchange
// must park rather than restoreSrc on possibly-applied state.
func TestResidualRenameXchLostReplyParks(t *testing.T) {
	dsn := pgDSN(t)
	s, root, dir := finalStore(t, dsn)
	ctx := context.Background()
	// Real writes mint the rows a live rename is declared on — the
	// src-path row claiming S is what authorizes its return home.
	for _, w := range [][2]string{{"old.txt", "S"}, {"new.txt", "D"}} {
		if _, _, err := s.WithWrite(ctx, "ws", w[0], "write",
			IfVersion{Mode: "any"}, sha(w[1]), authProbe(root, w[0]),
			authWriteFn(root, w[0], w[1])); err != nil {
			t.Fatalf("seed write %s: %v", w[0], err)
		}
	}

	root.xchFn = func(int, string, int, string) error { return unix.EIO }
	_, _, err := s.Rename(ctx, "ws", "old.txt", "new.txt",
		IfVersion{Mode: "any"}, authProbe(root, "new.txt"), authProbe(root, "old.txt"),
		func(it intent) (FileInfo, bool, error) {
			return root.rename("ws", "old.txt", "new.txt", false, it)
		})
	if !errors.Is(err, errUndoParked) {
		t.Fatalf("rename err = %v — want errUndoParked", err)
	}
	// restoreSrc must not have run: the source path stays empty, S
	// parked at the private slot for reconciliation.
	if fileExists(filepath.Join(dir, "ws", "old.txt")) {
		t.Fatal("old.txt repopulated — restoreSrc ran on an unknown-outcome exchange")
	}
	id, _, _ := boundaryIntent(t, s, "rename", "old.txt")
	slot := loadIntentName(t, s, id, 0)
	if got := mustRead(filepath.Join(dir, "ws", slot)); got != "S" {
		t.Fatalf("slot occupant = %q — captured source destroyed before verdict", got)
	}

	authSettle(t, s)

	if got := mustRead(filepath.Join(dir, "ws", "old.txt")); got != "S" {
		t.Fatalf("old.txt = %q — captured source not restored", got)
	}
	if got := mustRead(filepath.Join(dir, "ws", "new.txt")); got != "D" {
		t.Fatalf("new.txt = %q — destination content displaced", got)
	}
	if n := scanDirForPrivate(dir, "ws"); n != "" {
		t.Fatalf("private residue %q", n)
	}
}

// Landed rename + lost reply: the exchange committed — new.txt holds S,
// the slot holds displaced D. Parking keeps that state; reconciliation
// rolls the rename forward and surfaces D — under the old code
// restoreSrc would have moved D onto the source path.
func TestResidualRenameXchLostReplyLanded(t *testing.T) {
	dsn := pgDSN(t)
	s, root, dir := finalStore(t, dsn)
	ctx := context.Background()
	for _, w := range [][2]string{{"old.txt", "S"}, {"new.txt", "D"}} {
		if _, _, err := s.WithWrite(ctx, "ws", w[0], "write",
			IfVersion{Mode: "any"}, sha(w[1]), authProbe(root, w[0]),
			authWriteFn(root, w[0], w[1])); err != nil {
			t.Fatalf("seed write %s: %v", w[0], err)
		}
	}

	root.xchFn = func(ofd int, on string, nfd int, nn string) error {
		if err := unix.Renameat2(ofd, on, nfd, nn, unix.RENAME_EXCHANGE); err != nil {
			return err
		}
		return unix.EIO // committed, reply lost
	}
	_, _, err := s.Rename(ctx, "ws", "old.txt", "new.txt",
		IfVersion{Mode: "any"}, authProbe(root, "new.txt"), authProbe(root, "old.txt"),
		func(it intent) (FileInfo, bool, error) {
			return root.rename("ws", "old.txt", "new.txt", false, it)
		})
	if !errors.Is(err, errUndoParked) {
		t.Fatalf("rename err = %v — want errUndoParked", err)
	}
	if got := mustRead(filepath.Join(dir, "ws", "new.txt")); got != "S" {
		t.Fatalf("new.txt = %q — exchange did not land", got)
	}
	id, _, _ := boundaryIntent(t, s, "rename", "old.txt")
	slot := loadIntentName(t, s, id, 0)
	if got := mustRead(filepath.Join(dir, "ws", slot)); got != "D" {
		t.Fatalf("slot occupant = %q — displaced content destroyed before verdict", got)
	}

	authSettle(t, s)

	if got := mustRead(filepath.Join(dir, "ws", "new.txt")); got != "S" {
		t.Fatalf("new.txt = %q after settle — landed rename not kept", got)
	}
	if n := scanDirFor(t, dir, "ws", []byte("D")); n == "" || n == "new.txt" || containsPrivateSeg(n) {
		t.Fatalf("displaced occupant not preserved at a public name: %q", n)
	}
	// restoreSrc must never have run: nothing reappears at the source.
	if fileExists(filepath.Join(dir, "ws", "old.txt")) {
		t.Fatal("old.txt repopulated — displaced occupant restored onto the source path")
	}
	if n := scanDirForPrivate(dir, "ws"); n != "" {
		t.Fatalf("private residue %q", n)
	}
}
