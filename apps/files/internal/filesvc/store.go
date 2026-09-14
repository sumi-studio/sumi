package filesvc

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store mints service-managed versions and records a mutation journal.
// Versions come from a single global sequence (file_version_seq): a minted
// version is unique across paths and time, so a stale If-Version token can
// never collide with a different object that later occupies the same path
// (operation-review F1/ABA). The journal lets clients and reconciliation
// see what the service changed. Executor-direct writes bypass the journal
// entirely; the service detects them by comparing the recorded fingerprint
// to the live one at CAS time (eq/none) or read time (stat/read).
//
// Failure model: SQL cannot atomically cover a filesystem mutation. Every
// mutation runs in three stages —
//
//  1. declare: a tx validates the CAS and commits a file_op intent row
//     carrying the pre-minted version. If the DB cannot take the intent,
//     the filesystem is never touched.
//  2. fn: the filesystem mutation (bounded by opTimeout).
//  3. apply: a tx records version-row effects + event and deletes the
//     intent.
//
// If apply fails or the process dies after the fs commit, the intent row
// survives; the reconciler inspects the disk and either rolls the DB
// effects forward or drops the intent, so disk and DB converge instead of
// leaving stale rows claiming moved-away paths (operation-review F3/P1b).
// All mutations for one scope serialize on a per-scope mutex so a
// concurrent rename cannot interleave with another op's row writes
// (operation-review P2a/f56).
type Store struct {
	pool      *pgxpool.Pool
	opTimeout time.Duration // bounds the fs-mutation phase
	dbTimeout time.Duration // bounds every DB call (f51: wedged PG must not hang handlers)

	scopeMu   sync.Map // scope string -> *sync.Mutex
	inflight  sync.Map // intent id -> struct{} — ops executing in this process
	statFn    StatFn   // set by the service once the fs root exists
	rootID    string   // this service's canonical fs root — owns intents tagged with it
	reconcile chan struct{}
}

// StatFn stats a scope-relative path — injected by the service so the
// reconciler can compare pending intents against the real filesystem.
type StatFn func(scope, path string) (FileInfo, error)

func NewStore(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	s := &Store{
		pool:      pool,
		opTimeout: 30 * time.Second,
		dbTimeout: 15 * time.Second,
		reconcile: make(chan struct{}, 1),
	}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// SetOpTimeout bounds the fs-mutation phase of a versioned write.
func (s *Store) SetOpTimeout(d time.Duration) { s.opTimeout = d }

// SetDBTimeout bounds every DB statement/tx. A blackholed PG fails the
// request at the deadline instead of parking the handler (f51).
func (s *Store) SetDBTimeout(d time.Duration) { s.dbTimeout = d }

// SetReconcile wires the filesystem probe the reconciler uses plus this
// service's canonical root identity. Intents carry the root so that when
// several services share one database, each only reconciles intents whose
// mutations happened on ITS filesystem — a different root's view would
// falsely report "not found" and drop real intents.
func (s *Store) SetReconcile(fn StatFn, rootID string) {
	s.statFn = fn
	s.rootID = rootID
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS file_version (
			scope   text   NOT NULL,
			path    text   NOT NULL,
			version bigint NOT NULL,
			fp      text   NOT NULL DEFAULT '',
			updated timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (scope, path)
		);
		CREATE TABLE IF NOT EXISTS file_event (
			seq       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			scope     text NOT NULL,
			path      text NOT NULL,
			from_path text,
			op        text NOT NULL,
			version   bigint NOT NULL,
			at        timestamptz NOT NULL DEFAULT now()
		);
		ALTER TABLE file_event ADD COLUMN IF NOT EXISTS from_path text;
		CREATE INDEX IF NOT EXISTS file_event_scope_seq ON file_event(scope, seq);
		CREATE SEQUENCE IF NOT EXISTS file_version_seq;
		CREATE TABLE IF NOT EXISTS file_op (
			id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			root     text NOT NULL DEFAULT '',
			scope    text NOT NULL,
			op       text NOT NULL,
			path     text NOT NULL,
			to_path  text NOT NULL DEFAULT '',
			version  bigint NOT NULL,
			pre_fp   text NOT NULL DEFAULT '',
			late     boolean NOT NULL DEFAULT false,
			at       timestamptz NOT NULL DEFAULT now()
		);
		ALTER TABLE file_op ADD COLUMN IF NOT EXISTS root text NOT NULL DEFAULT '';
		CREATE INDEX IF NOT EXISTS file_op_scope ON file_op(scope, path);
		SELECT setval('file_version_seq',
			GREATEST((SELECT COALESCE(MAX(version),0)+1 FROM file_version), 1));
	`)
	return err
}

var (
	ErrConflict       = errors.New("version conflict")
	ErrNoSuchFile     = errors.New("no such file")
	ErrExternalChange = errors.New("path changed outside the service")
	ErrUnavailable    = errors.New("operation timed out")
	ErrNotEmpty       = errors.New("directory not empty")
)

// IfVersion is the caller's declared expectation for the target path.
type IfVersion struct {
	Mode    string // "any" | "none" | "eq"
	Version int64
}

// FPProbe returns the live fingerprint and existence of a path on disk.
type FPProbe func() (fp string, exists bool, err error)

// intent is one committed mutation intent row.
type intent struct {
	id      int64
	scope   string
	op      string // write|mkdir|remove|rename
	path    string // target (rename: source)
	toPath  string // rename destination
	version int64  // pre-minted version this op will record
	preFP   string // fingerprint of path (rename: of source) at declare time
	late    bool   // fs op was still running when the request timed out
	at      time.Time
}

func (s *Store) lockScope(scope string) *sync.Mutex {
	v, _ := s.scopeMu.LoadOrStore(scope, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// dbCtx bounds every DB interaction so a wedged-but-alive PG cannot park a
// handler past the deadline (f51).
func (s *Store) dbCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.dbTimeout)
}

// runBounded executes fn (the filesystem mutation) under opTimeout. On
// timeout the caller returns ErrUnavailable; the abandoned goroutine may
// still complete the fs op — the intent is then marked `late` and the
// reconciler gives it extra time before judging the disk.
func (s *Store) runBounded(fn func() (FileInfo, error)) (FileInfo, error) {
	type res struct {
		i FileInfo
		e error
	}
	ch := make(chan res, 1)
	go func() {
		i, e := fn()
		ch <- res{i, e}
	}()
	select {
	case r := <-ch:
		return r.i, r.e
	case <-time.After(s.opTimeout):
		return FileInfo{}, ErrUnavailable
	}
}

// checkVersion validates the caller's IfVersion against the stored row and
// the live filesystem fingerprint, mutating nothing:
//   - none / eq 0: no version row AND nothing on disk (a create-only write).
//   - eq n>0:      row version must equal n AND the on-disk fingerprint must
//     still equal the recorded one — an executor-side edit since the
//     caller's base version is a 409 external_change, not a silent overwrite.
//   - any:         unconditional; probe is not consulted.
//
// For remove, none/eq mean "a versioned object exists to remove".
func checkVersion(ctx context.Context, tx pgx.Tx, scope, path string, iv IfVersion, probe FPProbe, remove bool) error {
	var ver int64
	var recFP string
	err := tx.QueryRow(ctx,
		`SELECT version, fp FROM file_version WHERE scope=$1 AND path=$2 FOR UPDATE`,
		scope, path).Scan(&ver, &recFP)
	rowMissing := errors.Is(err, pgx.ErrNoRows)
	if err != nil && !rowMissing {
		return err
	}
	if remove {
		switch {
		case rowMissing:
			if iv.Mode == "eq" || iv.Mode == "none" {
				return ErrNoSuchFile
			}
			return nil
		case iv.Mode == "none":
			return ErrConflict
		case iv.Mode == "eq":
			if ver != iv.Version {
				return ErrConflict
			}
			liveFP, exists, perr := probe()
			if perr != nil {
				return perr
			}
			if !exists || (recFP != "" && liveFP != recFP) {
				return ErrExternalChange
			}
		}
		return nil
	}
	switch iv.Mode {
	case "none", "eq":
		if iv.Mode == "none" || iv.Version == 0 {
			if !rowMissing {
				return ErrConflict
			}
			_, exists, perr := probe()
			if perr != nil {
				return perr
			}
			if exists {
				return ErrExternalChange
			}
			return nil
		}
		if rowMissing {
			return ErrNoSuchFile
		}
		if ver != iv.Version {
			return ErrConflict
		}
		liveFP, exists, perr := probe()
		if perr != nil {
			return perr
		}
		if !exists || (recFP != "" && liveFP != recFP) {
			return ErrExternalChange
		}
		return nil
	case "any":
		return nil
	default:
		return fmt.Errorf("unknown if_version mode %q", iv.Mode)
	}
}

// declare validates the CAS and commits the intent row + pre-minted
// version. casProbe guards the CAS path (rename: the destination);
// preProbe fingerprints the op's source path so the reconciler can tell
// "fs ran" from "fs never ran" after a crash.
func (s *Store) declare(ctx context.Context, scope, op, path, toPath string, iv IfVersion, casProbe, preProbe FPProbe) (intent, error) {
	ctx, cancel := s.dbCtx(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return intent{}, err
	}
	defer tx.Rollback(ctx)

	casPath := path
	if op == "rename" {
		casPath = toPath
	}
	if err := checkVersion(ctx, tx, scope, casPath, iv, casProbe, op == "remove"); err != nil {
		return intent{}, err
	}
	it := intent{scope: scope, op: op, path: path, toPath: toPath}
	if preProbe != nil {
		fp, exists, perr := preProbe()
		if perr != nil {
			return intent{}, perr
		}
		if exists {
			it.preFP = fp
		}
	}
	if err := tx.QueryRow(ctx,
		`SELECT nextval('file_version_seq')`).Scan(&it.version); err != nil {
		return intent{}, err
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO file_op (root, scope, op, path, to_path, version, pre_fp)
		 VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id`,
		s.rootID, scope, op, path, toPath, it.version, it.preFP).Scan(&it.id)
	if err != nil {
		return intent{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return intent{}, err
	}
	return it, nil
}

// apply records the version-row effects + event and clears the intent, in
// one tx. It uses only values settled at declare/fs time — no re-checks —
// so it is safe to re-run from the reconciler.
func (s *Store) apply(ctx context.Context, it intent, info FileInfo) error {
	ctx, cancel := s.dbCtx(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	switch it.op {
	case "remove":
		if _, err := tx.Exec(ctx,
			`DELETE FROM file_version WHERE scope=$1 AND (path=$2 OR starts_with(path, $2 || '/'))`,
			it.scope, it.path); err != nil {
			return err
		}
	case "rename":
		// The fs rename replaced the destination subtree wholesale; its
		// rows are dead state. Delete them first so the source-descendant
		// move cannot collide (final-review B F2 ordering, preserved).
		if _, err := tx.Exec(ctx,
			`DELETE FROM file_version WHERE scope=$1 AND starts_with(path, $2 || '/')`,
			it.scope, it.toPath); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE file_version SET path = $3 || substr(path, length($2)+1)
			 WHERE scope=$1 AND starts_with(path, $2 || '/')`,
			it.scope, it.path, it.toPath); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM file_version WHERE scope=$1 AND path=$2`,
			it.scope, it.path); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO file_version (scope, path, version, fp, updated)
			 VALUES ($1,$2,$3,$4,now())
			 ON CONFLICT (scope,path) DO UPDATE
			 SET version=EXCLUDED.version, fp=EXCLUDED.fp, updated=now()
			 WHERE file_version.version < EXCLUDED.version`,
			it.scope, it.toPath, it.version, info.Fingerprint); err != nil {
			return err
		}
	default: // write, mkdir
		// Roll-forward must never regress a committed row: if a later
		// write already applied a newer version (e.g. this intent sat
		// pending while a subsequent op committed), the existing row
		// reflects newer disk state and wins.
		if _, err := tx.Exec(ctx,
			`INSERT INTO file_version (scope, path, version, fp, updated)
			 VALUES ($1,$2,$3,$4,now())
			 ON CONFLICT (scope,path) DO UPDATE
			 SET version=EXCLUDED.version, fp=EXCLUDED.fp, updated=now()
			 WHERE file_version.version < EXCLUDED.version`,
			it.scope, it.path, it.version, info.Fingerprint); err != nil {
			return err
		}
	}
	if it.op == "rename" {
		_, err = tx.Exec(ctx,
			`INSERT INTO file_event (scope, path, from_path, op, version) VALUES ($1,$2,$3,'rename',$4)`,
			it.scope, it.toPath, it.path, it.version)
	} else {
		_, err = tx.Exec(ctx,
			`INSERT INTO file_event (scope, path, op, version) VALUES ($1,$2,$3,$4)`,
			it.scope, it.path, it.op, it.version)
	}
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM file_op WHERE id=$1`, it.id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// dropIntent clears an intent that provably never reached the filesystem.
func (s *Store) dropIntent(ctx context.Context, it intent) {
	ctx, cancel := s.dbCtx(ctx)
	defer cancel()
	s.pool.Exec(ctx, `DELETE FROM file_op WHERE id=$1`, it.id) // best-effort
}

// markLate flags an intent whose fs op outlived the request bound; the
// reconciler gives it extra time before judging the disk.
func (s *Store) markLate(it intent) {
	ctx, cancel := context.WithTimeout(context.Background(), s.dbTimeout)
	defer cancel()
	s.pool.Exec(ctx, `UPDATE file_op SET late=true WHERE id=$1`, it.id)
}

func (s *Store) kickReconcile() {
	select {
	case s.reconcile <- struct{}{}:
	default:
	}
}

// settleAfterFsFailure resolves an intent whose fs call failed: a definite
// fs error means nothing committed (drop); a runBounded timeout means the
// op may still land (mark late).
func (s *Store) settleAfterFsFailure(ctx context.Context, it intent, ferr error) {
	if errors.Is(ferr, ErrUnavailable) {
		s.markLate(it)
		s.kickReconcile()
		return
	}
	s.dropIntent(ctx, it)
}

// WithWrite journals the intent, mutates the filesystem, then applies the
// version row + event. Serialized per scope.
func (s *Store) WithWrite(ctx context.Context, scope, path, op string, iv IfVersion, probe FPProbe, fn func() (FileInfo, error)) (int64, FileInfo, error) {
	mu := s.lockScope(scope)
	mu.Lock()
	defer mu.Unlock()

	it, err := s.declare(ctx, scope, op, path, "", iv, probe, probe)
	if err != nil {
		return 0, FileInfo{}, err
	}
	s.inflight.Store(it.id, struct{}{})
	defer s.inflight.Delete(it.id)

	info, ferr := s.runBounded(fn)
	if ferr != nil {
		s.settleAfterFsFailure(ctx, it, ferr)
		return 0, FileInfo{}, ferr
	}
	if err := s.apply(ctx, it, info); err != nil {
		// fs committed but the DB effects did not — the surviving intent
		// row lets the reconciler converge; the client sees the error,
		// not a silent success.
		s.kickReconcile()
		return 0, FileInfo{}, err
	}
	return it.version, info, nil
}

// Rename CAS-guards the destination, journals intent, performs fn (the fs
// rename — renameat2(RENAME_NOREPLACE) for create-only modes), then moves
// the source subtree's rows to the destination in the apply tx.
func (s *Store) Rename(ctx context.Context, scope, from, to string, iv IfVersion, casProbe, fromProbe FPProbe, fn func() (FileInfo, error)) (int64, FileInfo, error) {
	mu := s.lockScope(scope)
	mu.Lock()
	defer mu.Unlock()

	it, err := s.declare(ctx, scope, "rename", from, to, iv, casProbe, fromProbe)
	if err != nil {
		return 0, FileInfo{}, err
	}
	s.inflight.Store(it.id, struct{}{})
	defer s.inflight.Delete(it.id)

	info, ferr := s.runBounded(fn)
	if ferr != nil {
		s.settleAfterFsFailure(ctx, it, ferr)
		return 0, FileInfo{}, ferr
	}
	if err := s.apply(ctx, it, info); err != nil {
		s.kickReconcile()
		return 0, FileInfo{}, err
	}
	return it.version, info, nil
}

// Remove drops the version rows for the removed path and any descendants
// after the fs removal, under the same intent journal.
func (s *Store) Remove(ctx context.Context, scope, path string, iv IfVersion, probe FPProbe, fn func() error) error {
	mu := s.lockScope(scope)
	mu.Lock()
	defer mu.Unlock()

	it, err := s.declare(ctx, scope, "remove", path, "", iv, probe, probe)
	if err != nil {
		return err
	}
	s.inflight.Store(it.id, struct{}{})
	defer s.inflight.Delete(it.id)

	_, ferr := s.runBounded(func() (FileInfo, error) { return FileInfo{}, fn() })
	if ferr != nil {
		s.settleAfterFsFailure(ctx, it, ferr)
		return ferr
	}
	if err := s.apply(ctx, it, FileInfo{}); err != nil {
		s.kickReconcile()
		return err
	}
	return nil
}

// ObservedVersion returns the service-minted version and recorded fingerprint
// for a path (0,"" if never written through the service).
func (s *Store) ObservedVersion(ctx context.Context, scope, path string) (int64, string, error) {
	ctx, cancel := s.dbCtx(ctx)
	defer cancel()
	var v int64
	var fp string
	err := s.pool.QueryRow(ctx,
		`SELECT version, fp FROM file_version WHERE scope=$1 AND path=$2`,
		scope, path).Scan(&v, &fp)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, "", nil
	}
	return v, fp, err
}

type Event struct {
	Seq     int64  `json:"seq"`
	Path    string `json:"path"`
	From    string `json:"from,omitempty"`
	Op      string `json:"op"`
	Version int64  `json:"version"`
	At      string `json:"at"`
}

func (s *Store) Changes(ctx context.Context, scope string, since int64, limit int) ([]Event, error) {
	ctx, cancel := s.dbCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx,
		`SELECT seq, path, coalesce(from_path,''), op, version, at FROM file_event
		 WHERE scope=$1 AND seq>$2 ORDER BY seq LIMIT $3`,
		scope, since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Event{}
	for rows.Next() {
		var e Event
		var ts time.Time
		if err := rows.Scan(&e.Seq, &e.Path, &e.From, &e.Op, &e.Version, &ts); err != nil {
			return nil, err
		}
		e.At = ts.UTC().Format(time.RFC3339Nano)
		out = append(out, e)
	}
	return out, rows.Err()
}

// --- Reconciler ----------------------------------------------------------
//
// Every surviving intent row means "the fs mutation may have committed
// while its DB effects did not". Reconcile inspects the disk and rolls the
// DB effects forward (fs ran) or drops the intent (fs provably did not
// run), then clears the row. Each filesystem mutation is atomic — rename
// either moved or did not, a write either published or did not — so the
// disk verdict is unambiguous except in the narrow cases documented below.

// ReconcileLoop runs the reconciler at startup, on demand (kickReconcile
// after an apply failure or runBounded timeout), and periodically.
func (s *Store) ReconcileLoop(ctx context.Context) {
	s.Reconcile(ctx)
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.Reconcile(ctx)
		case <-s.reconcile:
			s.Reconcile(ctx)
		}
	}
}

// Reconcile processes all pending intents once. Returns how many it settled.
func (s *Store) Reconcile(ctx context.Context) int {
	if s.statFn == nil {
		return 0
	}
	dctx, cancel := s.dbCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(dctx,
		`SELECT id, scope, op, path, to_path, version, pre_fp, late, at
		 FROM file_op WHERE root = $1 ORDER BY id`,
		s.rootID)
	if err != nil {
		return 0
	}
	var its []intent
	for rows.Next() {
		var it intent
		if err := rows.Scan(&it.id, &it.scope, &it.op, &it.path, &it.toPath,
			&it.version, &it.preFP, &it.late, &it.at); err != nil {
			rows.Close()
			return 0
		}
		its = append(its, it)
	}
	rows.Close()
	settled := 0
	for _, it := range its {
		if s.reconcileOne(ctx, it) {
			settled++
		}
	}
	return settled
}

func (s *Store) reconcileOne(ctx context.Context, it intent) bool {
	// An intent whose fs op outlived the request bound gets extra grace —
	// the abandoned goroutine may still be executing and the disk verdict
	// is only trustworthy once it has had time to land.
	if it.late && time.Since(it.at) < 2*s.opTimeout+30*time.Second {
		return false
	}
	if _, ok := s.inflight.Load(it.id); ok {
		return false // executing in this process right now
	}
	mu := s.lockScope(it.scope)
	mu.Lock()
	defer mu.Unlock()

	switch it.op {
	case "rename":
		toInfo, terr := s.statFn(it.scope, it.toPath)
		frInfo, ferr := s.statFn(it.scope, it.path)
		switch {
		case errors.Is(ferr, ErrNotFound):
			// Source gone. If the destination exists the rename ran;
			// if neither exists, the moved subtree was deleted after —
			// either way the source's stale rows are dead state.
			if terr == nil {
				if err := s.apply(ctx, it, toInfo); err != nil {
					return false
				}
			} else {
				s.dropIntent(ctx, it)
			}
			return true
		case ferr == nil:
			if frInfo.Fingerprint == it.preFP {
				// Source unchanged since declare — the rename never ran.
				s.dropIntent(ctx, it)
				return true
			}
			// Source was recreated: the rename ran iff the destination
			// exists. If it does not, drop — disk wins.
			if terr == nil {
				if err := s.apply(ctx, it, toInfo); err != nil {
					return false
				}
			} else {
				s.dropIntent(ctx, it)
			}
			return true
		default:
			return false // fs unreachable — retry next pass
		}
	default: // write, mkdir, remove
		info, err := s.statFn(it.scope, it.path)
		if err != nil {
			log.Printf("reconcile: stat %s %s/%s -> %v", it.op, it.scope, it.path, err)
		}
		if errors.Is(err, ErrNotFound) {
			if it.op == "remove" {
				// Path absent: the removal landed.
				if err := s.apply(ctx, it, FileInfo{}); err != nil {
					log.Printf("reconcile: apply remove %s/%s: %v", it.scope, it.path, err)
					return false
				}
			} else {
				s.dropIntent(ctx, it)
			}
			return true
		}
		if err != nil {
			return false // fs unreachable — retry next pass
		}
		if it.op == "remove" {
			// Path still exists — removal never committed.
			s.dropIntent(ctx, it)
			return true
		}
		if it.preFP != "" && info.Fingerprint == it.preFP {
			// Byte-identical fingerprint since declare — our mutation
			// never ran (a landed write always mints a new inode/fp).
			s.dropIntent(ctx, it)
			return true
		}
		if err := s.apply(ctx, it, info); err != nil {
			log.Printf("reconcile: apply %s %s/%s: %v", it.op, it.scope, it.path, err)
			return false
		}
		return true
	}
}
