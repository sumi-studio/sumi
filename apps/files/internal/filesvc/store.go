package filesvc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
//
// Ownership/topology (consistency-review f79/f80): one database is bound
// to exactly one canonical filesystem root and at most one live writer.
// NewStore enforces both: a session-scoped pg advisory lock makes this
// process the only writer on the DB (a second instance waits briefly for
// a rolling-restart handoff, then refuses to start), and store_meta.root_id
// binds the DB to one root so two unrelated roots can never share version
// or journal state. Every intent carries its declaring instance's owner
// id; settlement claims the intent atomically inside the apply tx, so a
// dead owner's intents reconcile exactly once and an intent can never be
// double-applied. If the lock connection dies the store is deposed:
// mutations fail fast and the process re-acquires or exits rather than
// risk an unsynchronized second writer.
type Store struct {
	pool      *pgxpool.Pool
	dsn       string
	opTimeout time.Duration // bounds the fs-mutation phase
	dbTimeout time.Duration // bounds every DB call (f51: wedged PG must not hang handlers)

	owner     string      // this instance's identity; intents declare it
	rootID    string      // canonical root this DB is bound to
	lockMu    sync.Mutex  // serializes lockConn Ping/Close/assign (pgx.Conn is not thread-safe)
	lockConn  *pgx.Conn   // dedicated conn holding the writer advisory lock
	deposed   atomic.Bool // writer lock lost — mutations fail fast
	done      chan struct{}
	closeOnce sync.Once

	scopeMu          sync.Map                                 // scope string -> *sync.Mutex
	inflight         sync.Map                                 // intent id -> scope — fs goroutines (settlers) executing in this process
	statFn           StatFn                                   // set by the service once the fs root exists
	hashFn           HashFn                                   // content probe for expected-outcome verification
	fsCheck          func() error                             // when set, verdict that the fs root is trustworthy (canonical mount live)
	viewFn           func(context.Context) (ReconView, error) // pass-pinned fs view; supersedes statFn/hashFn/fsCheck when set
	reconcile        chan struct{}
	lastTombScan     atomic.Int64 // unix nanos of the last hot tombstone re-judgment
	lastTombScanCold atomic.Int64 // unix nanos of the last cold tombstone re-judgment
	lastStageSweep   atomic.Int64 // unix nanos of the last orphan staged sweep
}

// StatFn stats a scope-relative path — injected by the service so the
// reconciler can compare pending intents against the real filesystem.
type StatFn func(scope, path string) (FileInfo, error)

// HashFn returns the sha256 hex of a scope-relative file's content. The
// reconciler compares it against the intent's recorded expectation so
// external bytes are never laundered into a service version.
type HashFn func(scope, path string) (string, error)

// ReconView is one reconcile pass's pinned view of the filesystem. Every
// judgment resolves beneath a descriptor opened at pass start, so a
// mid-pass unmount reports an unreachable filesystem (honest
// "unverifiable") rather than the bare directory it leaves behind
// (F-RA-1). Acquiring the view is itself the mount gate — the service
// runs the mount check before pinning.
//
// The view only ever moves objects between OWNED private names and
// public paths that are verified empty (MoveStaged is NOREPLACE), and
// only ever deletes beneath owned private names (RemoveName). It has no
// operation that can evict or overwrite a public-name occupant.
type ReconView interface {
	Stat(scope, path string) (FileInfo, error)
	Hash(scope, path string) (string, error)
	// MoveStaged moves a recovery object to an empty name beneath the
	// same pinned root (renameat2 NOREPLACE — never overwrites). Used
	// to restore parked content to its home or to a visible surface
	// name.
	MoveStaged(scope, from, to string) error
	// RemoveName deletes the object at an owned private name beneath the
	// pinned root (unlinks in place; a directory is removed with
	// AT_REMOVEDIR; nonempty maps to ErrNotEmpty; ENOENT reports nil).
	// Callers verify identity before invoking it — the name is a
	// single-epoch record, so nothing else may populate it.
	RemoveName(scope, path string) error
	// ListStaged returns base names in dir (a path beneath the scope
	// root) beginning with prefix — used to enumerate private recovery
	// names.
	ListStaged(scope, dir, prefix string) ([]string, error)
	// ListDir returns every entry in dir, unfiltered — the orphan sweep
	// and recorded-member extraction walk ordinary directories, not
	// only staged names. A view that cannot list returns an error and
	// the walk simply does not descend.
	ListDir(scope, dir string) ([]string, error)
	// EnsureDir creates dir and any missing parents beneath the scope
	// (fd-relative mkdir-p). Used to recreate a recorded home's parent
	// chain before moving a member out of a parked container.
	EnsureDir(scope, dir string) error
	Close() error
}

// writerLockKey is the session advisory-lock key for the single-writer
// contract (per database). hashtext is stable across PG versions.
const writerLockKey = int64(0x73756d6966696c65) // "sumifile"

// deadGrace is how long a dead owner's intents age before this instance
// judges their disk outcome. A crashed process cannot start new fs work,
// but a mutation already accepted by a FUSE daemon can complete briefly
// after process death; the grace covers that residual window.
const deadGrace = 20 * time.Second

// tombstoneScanInterval is the cadence at which resolved intents are
// re-judged for late filesystem effects. Pending intents are judged every
// pass; tombstones only at this interval. Without it, a large tombstone
// set makes every pass re-stat every tombstoned path — holding the pinned
// root descriptor almost continuously, which keeps the mount busy and
// breaks ordinary unmount. The interval is the honest detection latency
// for a late-landing effect.
const tombstoneScanInterval = 30 * time.Second

// Tombstone evidence is never erased by a timer: a filesystem effect may
// arrive after any horizon, and the intent row is the only record that can
// re-attribute it. Cost is bounded by cadence, not retention — tombstones
// younger than tombstoneColdAge are re-judged every tombstoneScanInterval;
// older ones are still re-judged, on the slower tombstoneColdScanInterval.
// A late effect lands worst-case within one interval of landing.
const tombstoneColdAge = 24 * time.Hour
const tombstoneColdScanInterval = time.Hour

// stageSweepInterval is the cadence of the orphan staging sweep — the
// backstop for recorded objects parked under .filesv-op-* names whose
// intent row no longer exists. settleStaged discovers a staged
// namespace only through its file_op row, so a delayed
// MoveStaged/SwapStaged deposit — decided while the intent lived — can
// land under a dead namespace at any time; no intent-keyed pass ever
// lists the name again. The sweep runs on this interval against scopes
// that still carry rows; a late deposit is found by the first pass that
// completes after it lands. That is a cadence, not a bound: a pass can
// be skipped or delayed by competing reconcile work or errors, so no
// fixed convergence time is claimed. Per-scope cost is a full
// recursive directory enumeration — every entry, not only staged
// names — bounded by depth 32.
const stageSweepInterval = 30 * time.Second

func NewStore(ctx context.Context, dsn, rootID string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	var b [8]byte
	rand.Read(b[:])
	s := &Store{
		pool:      pool,
		dsn:       dsn,
		opTimeout: 30 * time.Second,
		dbTimeout: 15 * time.Second,
		owner:     "inst-" + hex.EncodeToString(b[:]),
		rootID:    rootID,
		done:      make(chan struct{}),
		reconcile: make(chan struct{}, 1),
	}
	// Single-writer enforcement comes first: migrate and meta binding run
	// under the lock so concurrent startups serialize instead of racing
	// CREATE IF NOT EXISTS (review B migrate race).
	if err := s.acquireWriter(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if err := s.migrate(ctx); err != nil {
		s.releaseWriter()
		pool.Close()
		return nil, err
	}
	if err := s.bindRoot(ctx); err != nil {
		s.releaseWriter()
		pool.Close()
		return nil, err
	}
	go s.watchWriter()
	return s, nil
}

// SetOpTimeout bounds the fs-mutation phase of a versioned write.
func (s *Store) SetOpTimeout(d time.Duration) { s.opTimeout = d }

// SetDBTimeout bounds every DB statement/tx. A blackholed PG fails the
// request at the deadline instead of parking the handler (f51).
func (s *Store) SetDBTimeout(d time.Duration) { s.dbTimeout = d }

// SetReconcile wires the filesystem probes the reconciler uses: stat for
// the disk verdict, hash for expected-content verification, and check —
// when non-nil — a verdict that the filesystem root itself is trustworthy
// right now (canonical mount live and fresh). A failed check means an
// "absent" stat answer proves nothing (a clean unmount leaves a bare,
// empty directory), so no intent is judged while it fails (f102).
func (s *Store) SetReconcile(fn StatFn, hash HashFn, check func() error) {
	s.statFn = fn
	s.hashFn = hash
	s.fsCheck = check
}

// SetReconcileView wires a pass-pinned filesystem view (F-RA-1): each
// reconcile pass acquires it once and judges every intent beneath that
// descriptor. Acquiring the view IS the mount gate — the service runs the
// canonical-mount check inside it before pinning, so a failed acquisition
// means "root not verifiable" and the whole pass is skipped.
func (s *Store) SetReconcileView(fn func(context.Context) (ReconView, error)) {
	s.viewFn = fn
}

// Deposed reports whether the writer lock was lost — the service maps it
// onto /healthz so supervision sees the fenced state instead of a
// healthy-looking zombie (f107).
func (s *Store) Deposed() bool { return s.deposed.Load() }

func (s *Store) Close() {
	s.closeOnce.Do(func() {
		close(s.done)
		s.releaseWriter()
		s.pool.Close()
	})
}

// --- Single-writer ownership ---------------------------------------------
//
// The advisory lock is the storage-authority boundary: the holder is the
// only process allowed to declare/apply/settle intents and version rows on
// this database. Losing the lock connection does NOT prove already-issued
// filesystem work stopped, so deposition is one-way per owner id: on lock
// loss the store stops mutating; on re-acquire, store_meta.owner must still
// be this instance or the process exits — a different owner means a real
// failover happened and this process is a zombie.

// writerAcquireWait bounds how long a second instance waits for the
// writer lock during a rolling-restart handoff before refusing to start.
var writerAcquireWait = 15 * time.Second

func (s *Store) acquireWriter(ctx context.Context) error {
	deadline := time.Now().Add(writerAcquireWait)
	for {
		conn, err := pgx.Connect(ctx, s.dsn)
		if err != nil {
			return fmt.Errorf("writer lock connect: %w", err)
		}
		var got bool
		err = conn.QueryRow(ctx,
			`SELECT pg_try_advisory_lock($1)`, writerLockKey).Scan(&got)
		if err != nil {
			conn.Close(ctx)
			return fmt.Errorf("writer lock: %w", err)
		}
		if got {
			s.lockConn = conn
			log.Printf("store: %s holds the writer lock for root %s", s.owner, s.rootID)
			return nil
		}
		conn.Close(ctx)
		if time.Now().After(deadline) {
			return fmt.Errorf("another live filesvc holds this database's writer lock; refusing to start a second writer")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (s *Store) releaseWriter() {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	if s.lockConn != nil {
		// Closing the session releases the advisory lock.
		s.lockConn.Close(context.Background())
		s.lockConn = nil
	}
}

// bindRoot enforces the one-DB-per-root rule and records this instance as
// the current owner. Runs under the writer lock.
func (s *Store) bindRoot(ctx context.Context) error {
	ctx, cancel := s.dbCtx(ctx)
	defer cancel()
	var rootID, owner string
	err := s.lockConn.QueryRow(ctx,
		`SELECT root_id, owner FROM store_meta WHERE id`).Scan(&rootID, &owner)
	if errors.Is(err, pgx.ErrNoRows) {
		_, err = s.lockConn.Exec(ctx,
			`INSERT INTO store_meta (id, root_id, owner) VALUES (true, $1, $2)`,
			s.rootID, s.owner)
		if err != nil {
			return fmt.Errorf("bind store_meta: %w", err)
		}
		log.Printf("store: bound database to root %s (owner %s)", s.rootID, s.owner)
		return nil
	}
	if err != nil {
		return err
	}
	if rootID != s.rootID {
		return fmt.Errorf("database is bound to a different root %q; refusing to share version state across roots", rootID)
	}
	_, err = s.lockConn.Exec(ctx,
		`UPDATE store_meta SET owner=$1, owner_since=now() WHERE id`, s.owner)
	if err == nil {
		log.Printf("store: %s is now owner of root %s", s.owner, s.rootID)
	}
	return err
}

// watchWriter detects loss of the lock session. While deposed, every
// mutation path returns ErrUnavailable and the reconciler halts. The
// watcher then tries to re-acquire: if the meta owner is still this
// instance (a transient disconnect, no failover) it resumes; if another
// instance took ownership the process exits — it must not continue as a
// zombie writer.
func (s *Store) watchWriter() {
	for {
		select {
		case <-s.done:
			return
		case <-time.After(3 * time.Second):
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		s.lockMu.Lock()
		err := s.lockConn.Ping(ctx)
		s.lockMu.Unlock()
		cancel()
		if err == nil {
			continue
		}
		log.Printf("store: writer lock session lost (%v) — deposing", err)
		s.deposed.Store(true)
		// Close the stale session first — its advisory lock may still be
		// held server-side, which would block our own re-acquire forever.
		s.releaseWriter()
		for {
			select {
			case <-s.done:
				return
			default:
			}
			conn, cerr := pgx.Connect(context.Background(), s.dsn)
			if cerr != nil {
				time.Sleep(2 * time.Second)
				continue
			}
			// Check the recorded owner BEFORE the lock attempt: a live
			// successor holds the lock, so waiting for pg_try_advisory_lock
			// alone can never discover the takeover — the displaced
			// process would linger deposed forever (f107/A-F2). A foreign
			// recorded owner means the failover already happened; exit.
			cctx, ccancel := context.WithTimeout(context.Background(), s.dbTimeout)
			var owner string
			oerr := conn.QueryRow(cctx,
				`SELECT owner FROM store_meta WHERE id`).Scan(&owner)
			ccancel()
			if oerr == nil && owner != s.owner {
				log.Printf("store: another instance (%s) owns this database — exiting", owner)
				os.Exit(1)
			}
			cctx, ccancel = context.WithTimeout(context.Background(), s.dbTimeout)
			var got bool
			cerr = conn.QueryRow(cctx,
				`SELECT pg_try_advisory_lock($1)`, writerLockKey).Scan(&got)
			ccancel()
			if cerr != nil || !got {
				conn.Close(context.Background())
				time.Sleep(2 * time.Second)
				continue
			}
			select {
			case <-s.done:
				conn.Close(context.Background())
				return
			default:
			}
			s.lockMu.Lock()
			s.lockConn = conn
			s.lockMu.Unlock()
			// Re-verify after acquiring: a successor may have bound while
			// we waited on the lock. bindRoot also covers a missing
			// store_meta row (manual DB damage) by rebinding instead of
			// spinning deposed forever (A F-RA minor).
			if berr := s.bindRoot(context.Background()); berr != nil {
				log.Printf("store: rebind after re-acquire failed: %v — exiting", berr)
				os.Exit(1)
			}
			log.Printf("store: %s re-acquired the writer lock", s.owner)
			s.deposed.Store(false)
			s.kickReconcile()
			break
		}
	}
}

func (s *Store) migrate(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS file_version (
			scope   text   NOT NULL,
			path    text   NOT NULL,
			version bigint NOT NULL,
			fp      text   NOT NULL DEFAULT '',
			updated timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (scope, path)
		);
		ALTER TABLE file_version ADD COLUMN IF NOT EXISTS content_sha text NOT NULL DEFAULT '';
		-- birth was the rejected persistent-identity claim — replaced by
		-- the fd-derived durable oid (protocol-v3).
		ALTER TABLE file_version DROP COLUMN IF EXISTS birth;
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
		-- recovery_place was the rejected post-hoc placement journal —
		-- public-path authority never comes from observed fingerprints.
		-- Home authority lives in file_op.names instead (per-intent
		-- private-name records committed before a syscall can populate
		-- them). The table is dropped, not repurposed.
		DROP TABLE IF EXISTS recovery_place;
		CREATE SEQUENCE IF NOT EXISTS file_version_seq;
		CREATE TABLE IF NOT EXISTS store_meta (
			id          boolean PRIMARY KEY DEFAULT true CHECK (id),
			root_id     text NOT NULL,
			owner       text NOT NULL,
			owner_since timestamptz NOT NULL DEFAULT now()
		);
		CREATE TABLE IF NOT EXISTS file_op (
			id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			root       text NOT NULL DEFAULT '',
			owner      text NOT NULL DEFAULT '',
			scope      text NOT NULL,
			op         text NOT NULL,
			path       text NOT NULL,
			to_path    text NOT NULL DEFAULT '',
			version    bigint NOT NULL,
			pre_fp     text NOT NULL DEFAULT '',
			expect_sha text NOT NULL DEFAULT '',
			src_kind   text NOT NULL DEFAULT '',
			at         timestamptz NOT NULL DEFAULT now(),
			resolved_at timestamptz,
			stalled_at  timestamptz,
			last_error  text NOT NULL DEFAULT ''
		);
		ALTER TABLE file_op ADD COLUMN IF NOT EXISTS root text NOT NULL DEFAULT '';
		ALTER TABLE file_op ADD COLUMN IF NOT EXISTS owner text NOT NULL DEFAULT '';
		ALTER TABLE file_op ADD COLUMN IF NOT EXISTS expect_sha text NOT NULL DEFAULT '';
		ALTER TABLE file_op ADD COLUMN IF NOT EXISTS src_kind text NOT NULL DEFAULT '';
		ALTER TABLE file_op ADD COLUMN IF NOT EXISTS resolved_at timestamptz;
		ALTER TABLE file_op ADD COLUMN IF NOT EXISTS stalled_at timestamptz;
		ALTER TABLE file_op ADD COLUMN IF NOT EXISTS last_error text NOT NULL DEFAULT '';
		ALTER TABLE file_op ADD COLUMN IF NOT EXISTS dst_fp text NOT NULL DEFAULT '';
		-- protocol-v3: durable object identities + the private-name
		-- journal. pre_oid/dst_oid are the declared objects' OIDs
		-- (empty when unbound); dst_sha is the destination object's
		-- content hash at declare; names is the JSONB name journal —
		-- every private name the op may populate is recorded here
		-- BEFORE the syscall that can populate it.
		ALTER TABLE file_op ADD COLUMN IF NOT EXISTS pre_oid text NOT NULL DEFAULT '';
		ALTER TABLE file_op ADD COLUMN IF NOT EXISTS dst_oid text NOT NULL DEFAULT '';
		ALTER TABLE file_op ADD COLUMN IF NOT EXISTS dst_sha text NOT NULL DEFAULT '';
		ALTER TABLE file_op ADD COLUMN IF NOT EXISTS names jsonb NOT NULL DEFAULT '[]'::jsonb;
		ALTER TABLE file_version ADD COLUMN IF NOT EXISTS oid text NOT NULL DEFAULT '';
		CREATE INDEX IF NOT EXISTS file_op_scope ON file_op(scope, path);
		SELECT setval('file_version_seq',
			GREATEST(COALESCE((SELECT MAX(version) FROM file_version), 0),
			         COALESCE((SELECT MAX(version) FROM file_op), 0),
			         (SELECT last_value FROM file_version_seq)));
	`)
	return err
}

var (
	ErrConflict       = errors.New("version conflict")
	ErrNoSuchFile     = errors.New("no such file")
	ErrExternalChange = errors.New("path changed outside the service")
	ErrUnavailable    = errors.New("operation timed out")
	ErrNotEmpty       = errors.New("directory not empty")
	// ErrUnsettled: a still-pending intent overlaps this op's paths, so
	// the outcome of earlier filesystem work is not yet known — starting
	// a conflicting mutation now could produce rows the pending
	// settlement would strand (f104). Retry once the intent settles.
	ErrUnsettled = errors.New("a mutation touching this path is still settling")
)

// IfVersion is the caller's declared expectation for the target path.
type IfVersion struct {
	Mode    string // "any" | "none" | "eq"
	Version int64
}

// FPProbe returns the live FileInfo and existence of a path on disk —
// fingerprint AND kind, so rename evidence (src_kind) comes from the same
// observation as pre_fp.
type FPProbe func() (info FileInfo, exists bool, err error)

// intent is one committed mutation intent row.
type intent struct {
	id        int64
	owner     string // instance that declared it — only its fs goroutine or (once dead) the reconciler may settle it
	scope     string
	op        string       // write|mkdir|remove|rename
	path      string       // target (rename: source)
	toPath    string       // rename destination
	version   int64        // pre-minted version this op will record
	preFP     string       // fingerprint of path (rename: of source) at declare time
	preOid    string       // durable object id of path (rename: of source) at declare — "" when unbound
	dstFP     string       // fingerprint of the object the effect may displace (write/remove: path; rename: destination) — the verified effect undoes rather than destroy anything else
	dstOid    string       // durable object id of the declared displaced object — "" when unbound
	dstSHA    string       // sha256 hex of the declared displaced object's content — "" when unobserved
	expectSHA string       // write: sha256 hex of intended bytes; rename of file: sha256 of source; mkdir: "dir"; "": unverified
	srcKind   string       // rename: kind of the source at declare ("" = unknown → unprovable)
	names     []nameRec    // the op's private-name journal; names[0] is the op's own slot
	journal   *nameJournal // bound while this process owns the intent
	at        time.Time
}

// nameRec is one journaled private name. The record is committed before
// any syscall may populate the name — that ordering is the protocol's
// core guarantee: a name's contents are only ever attributable through
// its record, so recovery never has to guess whether an object at a
// private name is service-placed or foreign.
type nameRec struct {
	Name string   `json:"name"`           // base name (carries the reserved .filesv-op- prefix)
	Dir  string   `json:"dir,omitempty"`  // scope-relative parent dir ("" = the op's parent dir)
	Act  string   `json:"act,omitempty"`  // last recorded act: put|xch|cap|mv
	Src  string   `json:"src,omitempty"`  // public path the act sourced from ("" = create)
	Res  string   `json:"res,omitempty"`  // act outcome: ok|noeff ("" = act declared, result unrecorded)
	Obs  []string `json:"obs,omitempty"`  // observed identities (fp3[:oid]) of objects seen at this name
	Done bool     `json:"done,omitempty"` // drained: nothing remains attributable here
	Tomb string   `json:"tomb,omitempty"` // non-empty: unknown-outcome tombstone — evidence kept
}

// nameJournal patches an intent's names JSONB. Every method is
// best-effort — a failed patch degrades a record to "outcome unknown",
// which the reconciler always reads conservatively (preserve, never
// destroy). A nil *nameJournal is a valid no-op target so unintented
// callers share the code paths.
type nameJournal struct {
	s  *Store
	id int64
}

// patch applies a jsonb_set on names[i].cond. Best-effort: errors are
// logged, not returned — the reconciler treats an unpatched record as
// unknown-outcome, which is always the safe reading.
func (nj *nameJournal) patch(i int, cond string, val any) {
	if nj == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), nj.s.dbTimeout)
	defer cancel()
	_, err := nj.s.pool.Exec(ctx,
		`UPDATE file_op SET names = jsonb_set(names, $2::text[], to_jsonb($3::text))
		 WHERE id = $1`, nj.id, []string{strconv.Itoa(i), cond}, fmt.Sprint(val))
	if err != nil {
		log.Printf("filesvc: name journal patch op=%d [%d].%s: %v", nj.id, i, cond, err)
	}
}

// patchBool sets a boolean journal field (done).
func (nj *nameJournal) patchBool(i int, cond string, val bool) {
	if nj == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), nj.s.dbTimeout)
	defer cancel()
	_, err := nj.s.pool.Exec(ctx,
		`UPDATE file_op SET names = jsonb_set(names, $2::text[], to_jsonb($3::boolean))
		 WHERE id = $1`, nj.id, []string{strconv.Itoa(i), cond}, val)
	if err != nil {
		log.Printf("filesvc: name journal patch op=%d [%d].%s: %v", nj.id, i, cond, err)
	}
}

// act records that a named act was initiated on names[i] BEFORE the
// syscall that can populate it — this ordering is what makes an
// unresulted record mean "outcome unknown" rather than "never ran".
func (nj *nameJournal) act(i int, act, src string) {
	if nj == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), nj.s.dbTimeout)
	defer cancel()
	_, err := nj.s.pool.Exec(ctx,
		`UPDATE file_op SET names = jsonb_set(jsonb_set(names,
		    $2::text[], to_jsonb($3::text)),
		    $4::text[], to_jsonb($5::text))
		 WHERE id = $1`, nj.id,
		[]string{strconv.Itoa(i), "act"}, act,
		[]string{strconv.Itoa(i), "src"}, src)
	if err != nil {
		log.Printf("filesvc: name journal act op=%d [%d]: %v", nj.id, i, err)
	}
}

// res records the act's outcome on names[i].
func (nj *nameJournal) res(i int, res string) { nj.patch(i, "res", res) }

// done marks names[i] drained — nothing attributable remains there.
func (nj *nameJournal) done(i int) { nj.patchBool(i, "done", true) }

// tomb marks names[i] tombstoned: the outcome stayed unknown, so the
// record is preserved as late-effect evidence.
func (nj *nameJournal) tomb(i int, cause string) { nj.patch(i, "tomb", cause) }

// obs appends an observed object identity to names[i].obs.
func (nj *nameJournal) obs(i int, ident string) {
	if nj == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), nj.s.dbTimeout)
	defer cancel()
	_, err := nj.s.pool.Exec(ctx,
		`UPDATE file_op SET names = jsonb_set(names, $2::text[],
		    COALESCE(names->$3->'obs', '[]'::jsonb) || to_jsonb($4::text))
		 WHERE id = $1`, nj.id,
		[]string{strconv.Itoa(i), "obs"}, strconv.Itoa(i), ident)
	if err != nil {
		log.Printf("filesvc: name journal obs op=%d [%d]: %v", nj.id, i, err)
	}
}

// stageBase is the op's own private slot — names[0], committed inside
// declare's tx before any syscall can populate it.
func (it intent) stageBase() string {
	if len(it.names) > 0 {
		return it.names[0].Name
	}
	return ""
}

// Intent-side journal shims — nil-safe so unintented calls share paths.
func (it intent) njAct(act, src string) { it.journal.act(0, act, src) }
func (it intent) njRes(res string)      { it.journal.res(0, res) }
func (it intent) njDone()               { it.journal.done(0) }
func (it intent) njTomb(cause string)   { it.journal.tomb(0, cause) }
func (it intent) njObs(ident string)    { it.journal.obs(0, ident) }

func (s *Store) lockScope(scope string) *sync.Mutex {
	v, _ := s.scopeMu.LoadOrStore(scope, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// dbCtx bounds every DB interaction so a wedged-but-alive PG cannot park a
// handler past the deadline (f51).
func (s *Store) dbCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.dbTimeout)
}

// runFs executes the filesystem mutation on a goroutine that OWNS the
// intent's settlement. The intent is already marked inflight at declare
// commit (F-RA-4). On return it applies (success), drops the intent (a
// definitive pre-commit rejection), or leaves it for the reconciler when
// the outcome is unknown: fs ops are composite, and an error after the
// commit point — or a transport-class error where the reply may have
// been lost — must not destroy the only record that can settle the
// possibly-committed work (F-RA-5/f120). The caller waits only up to
// opTimeout; a slow-but-landing fs op still settles itself honestly (f82).
type fsResult struct {
	info      FileInfo
	err       error
	settleErr error // non-nil: fs committed but the journal did not yet
}

// fsErrDefinitive reports whether err is a pre-commit rejection — the
// errno semantics guarantee the mutation did not happen (missing source,
// kind mismatch, permission, conflict, policy). Transport/outcome-class
// errors (ErrMountUnavailable, ErrUnavailable, anything unmapped) are NOT
// definitive: on FUSE a lost reply can follow an applied request.
func fsErrDefinitive(err error) bool {
	switch {
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrNotDir),
		errors.Is(err, ErrIsDir), errors.Is(err, ErrWrongKind),
		errors.Is(err, ErrAccess), errors.Is(err, ErrReserved),
		errors.Is(err, ErrEscape), errors.Is(err, ErrConflict),
		errors.Is(err, ErrExternalChange),
		errors.Is(err, ErrNotEmpty), errors.Is(err, ErrMountPolicy):
		return true
	default:
		return false
	}
}

func (s *Store) runFs(it intent, fn func(intent) (FileInfo, bool, error)) <-chan fsResult {
	ch := make(chan fsResult, 1)
	go func() {
		defer s.inflight.Delete(it.id)
		info, committed, ferr := fn(it)
		var settleErr error
		switch {
		case ferr == nil:
			// success path below
		case it.op == "remove" && errors.Is(ferr, ErrNotFound):
			// Desired absence already holds — the rows are dead state.
			// Clean them WITHOUT an event: we observed absence, we did
			// not cause it (observed-absence vs performed-removal).
			s.dropIntentGhosts(context.Background(), it, true)
		case errors.Is(ferr, errUndoParked):
			if !committed {
				// A verified effect displaced foreign bytes and could not
				// fully restore the pre-effect shape — the parked object is
				// live evidence; retain the intent for the reconciler.
				s.tombstoneIntent(context.Background(), it)
			} else {
				// The effect committed but a foreign object stayed parked
				// at the intent's staging slot: journal the commit AND keep
				// the intent as a tombstone — deleting the row now would
				// leave the parked object under a namespace no pass ever
				// enumerates.
				settleErr = s.applyRetained(it, info, intentSHA(it))
			}
		case !committed && fsErrDefinitive(ferr):
			// Rejected before commit — provably no fs effect.
			s.dropIntent(context.Background(), it)
		default:
			// committed (post-commit observation failure) or ambiguous
			// (lost-reply class): the outcome is unknown — keep the
			// intent for the reconciler's disk verdict.
			log.Printf("store: intent %d (%s %s/%s) fs outcome unknown (%v) — left for reconcile",
				it.id, it.op, it.scope, it.path, ferr)
			s.kickReconcile()
		}
		if ferr == nil {
			settleErr = s.applyUntilSettled(it, info, intentSHA(it))
		}
		ch <- fsResult{info, ferr, settleErr}
	}()
	return ch
}

// intentSHA is the content hash a successful apply commits for this
// intent: the declared payload for content ops, the "dir" kind marker
// for mkdir (recorded so the reconciler can distinguish dir rows when
// locating recorded homes).
func intentSHA(it intent) string {
	if it.op == "mkdir" {
		return "dir"
	}
	return it.expectSHA
}

// applyUntilSettled persists a filesystem commit THIS process observed:
// the syscall returned success, so the outcome is certain here and must
// not be handed to disk inference (the common failure is a transient DB
// outage right after commit — retrying apply writes the journal entry
// with the correct event instead of leaving the reconciler to guess).
// The intent stays inflight throughout, so the reconciler never judges
// it. The loop gives up only when another instance owns the store (the
// successor settles by disk) or the store is closing; either way the
// intent row survives exactly as before. Each attempt is bounded by
// dbTimeout and the backoff caps at 5s, so a permanently unavailable DB
// parks one goroutine — it never spins.
func (s *Store) applyUntilSettled(it intent, info FileInfo, sha string) error {
	backoff := 200 * time.Millisecond
	for attempt := 0; ; attempt++ {
		err := s.apply(context.Background(), it, info, sha, false)
		if err == nil || errors.Is(err, errIntentSettled) {
			return nil
		}
		if errors.Is(err, errForeignOwner) {
			log.Printf("store: apply of intent %d (%s %s/%s): %v — left for the owning instance",
				it.id, it.op, it.scope, it.path, err)
			return err
		}
		if attempt == 0 {
			log.Printf("store: apply of intent %d (%s %s/%s) failed: %v — retrying",
				it.id, it.op, it.scope, it.path, err)
		}
		select {
		case <-s.done:
			return err
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

// applyRetained journals a filesystem commit this process observed AND
// keeps the intent as a tombstone — the fs call reported recovery
// residue parked at the intent's staging slot (errUndoParked with
// committed=true). Deleting the intent row would orphan that namespace:
// settleStaged enumerates staged names through the intent row, so a
// deleted intent leaves parked content undiscoverable. The tombstone
// keeps the slot enumerable until a pass settles it; the orphan sweep
// is the backstop once the row is eventually resolved away.
func (s *Store) applyRetained(it intent, info FileInfo, sha string) error {
	if info.Fingerprint == "" {
		// The commit was reported without a verifiable identity for the
		// acknowledged object. Journaling fp="" would defeat recordedAt
		// (a displaced recorded object could never be routed home) and
		// suppress divergence reporting (recordedFP=="" reads as clean).
		// Keep the intent as an unapplied tombstone instead: re-judgment
		// settles the committed effect by disk observation, which only
		// journals hash/fp3-verified identities, and the staged
		// namespace stays enumerable for the parked residue.
		s.tombstoneIntent(context.Background(), it)
		return nil
	}
	backoff := 200 * time.Millisecond
	for attempt := 0; ; attempt++ {
		err := s.apply(context.Background(), it, info, sha, true)
		if err == nil || errors.Is(err, errIntentSettled) || errors.Is(err, errNotJournaled) {
			return nil
		}
		if errors.Is(err, errForeignOwner) {
			log.Printf("store: retained apply of intent %d (%s %s/%s): %v — left for the owning instance",
				it.id, it.op, it.scope, it.path, err)
			return err
		}
		if attempt == 0 {
			log.Printf("store: retained apply of intent %d (%s %s/%s) failed: %v — retrying",
				it.id, it.op, it.scope, it.path, err)
		}
		select {
		case <-s.done:
			return err
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

// waitFs bounds the caller's wait on the fs mutation; the settling
// goroutine keeps owning the intent regardless.
func (s *Store) waitFs(ch <-chan fsResult) fsResult {
	select {
	case r := <-ch:
		return r
	case <-time.After(s.opTimeout):
		return fsResult{err: ErrUnavailable}
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
			live, exists, perr := probe()
			if perr != nil {
				return perr
			}
			if !exists || (recFP != "" && live.Fingerprint != recFP) {
				return ErrExternalChange
			}
		}
		return nil
	}
	switch iv.Mode {
	case "none", "eq":
		if iv.Mode == "none" || iv.Version == 0 {
			if !rowMissing {
				// A version row with no file behind it is a ghost (an
				// external delete, or a late-landing fs op the journal
				// already covered). Disk is authoritative: allow the
				// create and let apply replace the stale row (f82/F-C —
				// a ghost row must not block create-only forever).
				_, exists, perr := probe()
				if perr != nil {
					return perr
				}
				if !exists {
					return nil
				}
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
		live, exists, perr := probe()
		if perr != nil {
			return perr
		}
		if !exists || (recFP != "" && live.Fingerprint != recFP) {
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
// "fs ran" from "fs never ran" after a crash. expectSHA is the intended
// post-state evidence (sha256 of the write body; "dir" for mkdir) — the
// reconciler compares it against what actually landed so foreign bytes
// are never attributed to this op (f83).
func (s *Store) declare(ctx context.Context, scope, op, path, toPath string, iv IfVersion, expectSHA string, casProbe, preProbe FPProbe) (intent, error) {
	if s.deposed.Load() {
		return intent{}, ErrUnavailable
	}
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
	// Refuse work whose paths overlap a still-pending intent (f104) — or a
	// recently-resolved tombstone still inside its hot window (a dropped
	// rename can still have a filesystem effect in flight; admitting new
	// ops into its area could mint rows that a late landing then strands).
	// Every leg is guarded on a non-empty literal — '' must never act as a
	// leg (B-1/f118: to_path='' = to_path='' used to block ALL non-rename
	// ops). The pending check runs BEFORE the CAS probe so a jammed path
	// reports pending_settlement, not whatever error the probe happens to
	// hit. The blocking intent's recorded error (if it is stalled on an
	// unverifiable path) is surfaced to the caller.
	var blockOp, blockPath, blockErr string
	err = tx.QueryRow(ctx,
		`SELECT op, path, coalesce(last_error,'') FROM file_op WHERE scope=$1 AND
		   (resolved_at IS NULL OR resolved_at > now() - $4::interval) AND (
		     ($2 <> '' AND (
		        path=$2 OR starts_with(path, $2||'/') OR starts_with($2, path||'/')
		     OR (to_path <> '' AND (to_path=$2 OR starts_with(to_path, $2||'/') OR starts_with($2, to_path||'/')))))
		  OR ($3 <> '' AND (
		        path=$3 OR starts_with(path, $3||'/') OR starts_with($3, path||'/')
		     OR (to_path <> '' AND (to_path=$3 OR starts_with(to_path, $3||'/') OR starts_with($3, to_path||'/'))))))
		 LIMIT 1`,
		scope, path, toPath, deadGrace.String()).Scan(&blockOp, &blockPath, &blockErr)
	if err == nil {
		if blockErr != "" {
			return intent{}, fmt.Errorf("%w: %s %s unverifiable: %s", ErrUnsettled, blockOp, blockPath, blockErr)
		}
		return intent{}, ErrUnsettled
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return intent{}, err
	}
	if err := checkVersion(ctx, tx, scope, casPath, iv, casProbe, op == "remove"); err != nil {
		return intent{}, err
	}
	it := intent{owner: s.owner, scope: scope, op: op, path: path, toPath: toPath, expectSHA: expectSHA}
	if preProbe != nil {
		pre, exists, perr := preProbe()
		if perr != nil {
			return intent{}, perr
		}
		if exists {
			it.preFP = pre.Fingerprint
			it.srcKind = pre.Kind
			// Durable identity only ever comes from the opened object —
			// an unbound/transient result records "" (reduced recovery
			// precision), never a path-level guess.
			if pre.oidCls == idBound {
				it.preOid = pre.Oid
			}
		}
	}
	// dstFP/dstOid/dstSHA describe the object a committed effect is
	// allowed to displace — recorded at declare so a verified effect
	// that lands late (after ownership moved and a successor wrote
	// newer content) undoes itself rather than destroy what it never
	// agreed to replace.
	if op == "rename" {
		if casProbe != nil {
			dst, exists, derr := casProbe()
			if derr != nil {
				return intent{}, derr
			}
			if exists {
				it.dstFP = dst.Fingerprint
				if dst.oidCls == idBound {
					it.dstOid = dst.Oid
				}
				if dst.Kind == "file" && s.hashFn != nil {
					if sha, herr := s.hashFn(scope, toPath); herr == nil {
						it.dstSHA = sha
					}
				}
			}
		}
	} else {
		it.dstFP = it.preFP
		it.dstOid = it.preOid
		if it.preFP != "" && it.srcKind == "file" && s.hashFn != nil {
			if sha, herr := s.hashFn(scope, path); herr == nil {
				it.dstSHA = sha
			}
		}
	}
	// Rename of a file: capture content evidence so settlement can tell
	// "the moved source" from "foreign bytes with recycled metadata"
	// (f106/B-4). Prefer the version row's recorded content hash — valid
	// only while its fp still equals the live one — over re-reading the
	// file. A file we cannot hash cannot carry proof, so the declare is
	// refused rather than silently downgraded to metadata-only.
	if op == "rename" && it.srcKind == "file" && it.expectSHA == "" {
		var recFP, recSHA string
		rerr := tx.QueryRow(ctx,
			`SELECT fp, content_sha FROM file_version WHERE scope=$1 AND path=$2`,
			scope, path).Scan(&recFP, &recSHA)
		if rerr == nil && recFP == it.preFP && recSHA != "" {
			it.expectSHA = recSHA
		} else if rerr != nil && !errors.Is(rerr, pgx.ErrNoRows) {
			return intent{}, rerr
		} else if s.hashFn != nil {
			sha, herr := s.hashFn(scope, path)
			if herr != nil {
				return intent{}, herr
			}
			it.expectSHA = sha
		}
	}
	if err := tx.QueryRow(ctx,
		`SELECT nextval('file_version_seq')`).Scan(&it.version); err != nil {
		return intent{}, err
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO file_op (root, owner, scope, op, path, to_path, version, pre_fp, dst_fp, expect_sha, src_kind, pre_oid, dst_oid, dst_sha)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) RETURNING id`,
		s.rootID, s.owner, scope, op, path, toPath, it.version, it.preFP, it.dstFP, it.expectSHA, it.srcKind,
		it.preOid, it.dstOid, it.dstSHA).Scan(&it.id)
	if err != nil {
		return intent{}, err
	}
	// Journal the op's private name BEFORE commit — and therefore before
	// any syscall can populate it. A committed names[0] is the only
	// authority under which the op may use private storage; a name with
	// no journal entry is never touched by anyone.
	if op == "write" || op == "remove" || op == "rename" {
		rec := nameRec{Name: opStagePrefix + fmt.Sprintf("o%d-a0-%s", it.id, randHex(6))}
		if _, err := tx.Exec(ctx,
			`UPDATE file_op SET names = $2 WHERE id = $1`,
			it.id, mustJSON([]nameRec{rec})); err != nil {
			return intent{}, err
		}
		it.names = []nameRec{rec}
	}
	// Mark inflight BEFORE commit: the intent row is only visible to the
	// reconciler after commit, so the mark always lands first — no pass
	// can ever observe a committed own-intent that is not registered as
	// in-flight and misjudge it as abandoned (F-RA-4/f122). If commit
	// fails the intent never existed, so the mark is removed.
	s.inflight.Store(it.id, scope)
	if err := tx.Commit(ctx); err != nil {
		s.inflight.Delete(it.id)
		return intent{}, err
	}
	it.journal = &nameJournal{s: s, id: it.id}
	return it, nil
}

// mustJSON marshals the name journal — it cannot fail on these types.
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// divergedFP marks a settled version row whose on-disk bytes are NOT what
// the op intended: it can never equal a live fingerprint, so stat and CAS
// report external_change instead of laundering foreign content into the
// service's audit trail (f83).
func divergedFP(expect string) string { return "diverged:" + expect }

// errIntentSettled means another settlement path already cleared the
// intent row; the apply is a no-op duplicate.
var errIntentSettled = errors.New("intent already settled")

// errNotJournaled: a diverged apply found nothing new to record — the row
// already carries the observation or a newer version superseded it. The
// intent is resolved (tombstoned); no row or event was written.
var errNotJournaled = errors.New("diverged observation already recorded or superseded")

// apply records the version-row effects + event and clears the intent, in
// one tx. It uses only values settled at declare/fs time — no re-checks —
// so it is safe to re-run from the reconciler.
//
// The intent is CLAIMED first (DELETE … RETURNING): settlement is atomic
// and idempotent — a second apply of the same intent row is a no-op, so a
// stale selected intent can never re-delete or re-move rows a first apply
// already produced (f79/B-F1). The store_meta owner check fences a zombie
// process whose lock session died: its apply aborts and the intent stays
// for the live owner to settle. The owner row is held FOR SHARE until
// commit, so bindRoot's UPDATE waits for every in-flight row writer: no
// tx that read the previous owner can commit after a new owner binds
// (the ordering recordersActive relies on).
// contentSHA is the content hash of the minted state when known (writes:
// the bytes written; reconciled writes: the observed hash; proven file
// renames: the verified source hash). It feeds the declare-time evidence
// cache so later renames need not re-read the file.
// keepIntent retains the intent as a tombstone instead of deleting it —
// used when the reconciler recorded a DIVERGED outcome: the observed
// content is foreign, but the declared effect may still land later. The
// row carries the only evidence that can re-attribute it. Such an apply
// journals an observation, not an effect this op performed: it writes
// the event only when the version row actually changes (otherwise the
// row already carries the observation or a newer version superseded it,
// and errNotJournaled is returned after resolving the intent), and a
// tombstone keeps its original resolved_at so re-judgment never re-arms
// the path's pending_settlement window.
func (s *Store) apply(ctx context.Context, it intent, info FileInfo, contentSHA string, keepIntent bool) error {
	ctx, cancel := s.dbCtx(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var owner string
	if err := tx.QueryRow(ctx,
		`SELECT owner FROM store_meta WHERE id FOR SHARE`).Scan(&owner); err != nil {
		return err
	}
	if owner != s.owner {
		return fmt.Errorf("deposed: writer lock held by %s: %w (%w)", owner, errForeignOwner, ErrUnavailable)
	}
	if keepIntent {
		tag, kerr := tx.Exec(ctx,
			`UPDATE file_op SET resolved_at=COALESCE(resolved_at, now()), stalled_at=NULL, last_error='' WHERE id=$1`, it.id)
		if kerr != nil {
			return kerr
		}
		if tag.RowsAffected() == 0 {
			return errIntentSettled
		}
	} else {
		var claimed int64
		err = tx.QueryRow(ctx,
			`DELETE FROM file_op WHERE id=$1 RETURNING id`, it.id).Scan(&claimed)
		if errors.Is(err, pgx.ErrNoRows) {
			return errIntentSettled
		}
		if err != nil {
			return err
		}
	}

	rowWrites := int64(1)
	switch it.op {
	case "remove":
		// Version-guarded like rename: a row committed after this intent
		// declared (e.g. a recreate that raced a delayed settle) is not
		// dead state and must not be erased (f104).
		if _, err := tx.Exec(ctx,
			`DELETE FROM file_version WHERE scope=$1 AND (path=$2 OR starts_with(path, $2 || '/'))
			 AND version < $3`,
			it.scope, it.path, it.version); err != nil {
			return err
		}
	case "rename":
		// The fs rename replaced the destination subtree wholesale; rows
		// that existed at declare time under the destination are dead
		// state. Every subtree effect is version-guarded at the intent's
		// declare-time version so rows committed by LATER ops (e.g. a
		// file recreated at the old source path after this intent's fs
		// rename ran) are never stolen or erased by delayed settlement
		// (f81/B-F3).
		if _, err := tx.Exec(ctx,
			`DELETE FROM file_version WHERE scope=$1 AND starts_with(path, $2 || '/')
			 AND version < $3`,
			it.scope, it.toPath, it.version); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE file_version SET path = $3 || substr(path, length($2)+1)
			 WHERE scope=$1 AND starts_with(path, $2 || '/')
			   AND version < $4`,
			it.scope, it.path, it.toPath, it.version); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM file_version WHERE scope=$1 AND path=$2 AND version < $3`,
			it.scope, it.path, it.version); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO file_version (scope, path, version, fp, updated, content_sha, oid)
			 VALUES ($1,$2,$3,$4,now(),$5,$6)
			 ON CONFLICT (scope,path) DO UPDATE
			 SET version=EXCLUDED.version, fp=EXCLUDED.fp, updated=now(),
			     content_sha=EXCLUDED.content_sha, oid=EXCLUDED.oid
			 WHERE file_version.version < EXCLUDED.version`,
			it.scope, it.toPath, it.version, info.Fingerprint, contentSHA, info.Oid); err != nil {
			return err
		}
	default: // write, mkdir
		// Roll-forward must never regress a committed row: if a later
		// write already applied a newer version (e.g. this intent sat
		// pending while a subsequent op committed), the existing row
		// reflects newer disk state and wins.
		tag, uerr := tx.Exec(ctx,
			`INSERT INTO file_version (scope, path, version, fp, updated, content_sha, oid)
			 VALUES ($1,$2,$3,$4,now(),$5,$6)
			 ON CONFLICT (scope,path) DO UPDATE
			 SET version=EXCLUDED.version, fp=EXCLUDED.fp, updated=now(),
			     content_sha=EXCLUDED.content_sha, oid=EXCLUDED.oid
			 WHERE file_version.version < EXCLUDED.version`,
			it.scope, it.path, it.version, info.Fingerprint, contentSHA, info.Oid)
		if uerr != nil {
			return uerr
		}
		rowWrites = tag.RowsAffected()
	}
	if rowWrites == 0 {
		// The row already carries this state or a newer version
		// superseded it (a tombstone re-judged after a successor wrote
		// the same bytes, or a repeated diverged observation). An event
		// now would announce a version the row does not hold and forge
		// commit evidence for intentApplied — resolve without one.
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		if keepIntent {
			return errNotJournaled
		}
		return nil
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
	return tx.Commit(ctx)
}

// errForeignOwner: the recorded store owner is another instance — this
// process is fenced and must not settle anything (f103).
var errForeignOwner = errors.New("store owned by another instance")

// checkOwnerTx verifies inside the tx that store_meta still names this
// instance. Every destructive settlement path is fenced by it — a deposed
// process must never delete intents or version rows a live successor
// would roll forward (f103/B-F-B). Like apply, it holds the owner row FOR
// SHARE until the tx ends, so a rebind waits for the fenced writer.
func (s *Store) checkOwnerTx(ctx context.Context, tx pgx.Tx) error {
	var owner string
	if err := tx.QueryRow(ctx,
		`SELECT owner FROM store_meta WHERE id FOR SHARE`).Scan(&owner); err != nil {
		return err
	}
	if owner != s.owner {
		return errForeignOwner
	}
	return nil
}

// dropIntent clears an intent that provably never reached the filesystem.
// Returns true when the intent row is actually gone; a foreign owner or a
// DB failure leaves it for the owning writer's next pass.
func (s *Store) dropIntent(ctx context.Context, it intent) bool {
	ctx, cancel := s.dbCtx(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false
	}
	defer tx.Rollback(ctx)
	if err := s.checkOwnerTx(ctx, tx); err != nil {
		return false
	}
	if _, err := tx.Exec(ctx, `DELETE FROM file_op WHERE id=$1`, it.id); err != nil {
		return false
	}
	return tx.Commit(ctx) == nil
}

// dropIntentGhosts clears an intent plus version rows the disk proves are
// dead state: rows under path (subtree when subtree=true) minted before
// this intent's declare-time version. The version guard preserves rows
// committed by later ops for recreated paths — a stale settlement can
// never steal them (f81). Owner-fenced like dropIntent (f103).
func (s *Store) dropIntentGhosts(ctx context.Context, it intent, subtree bool) bool {
	ctx, cancel := s.dbCtx(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false
	}
	defer tx.Rollback(ctx)
	if err := s.checkOwnerTx(ctx, tx); err != nil {
		return false
	}
	var derr error
	if subtree {
		_, derr = tx.Exec(ctx,
			`DELETE FROM file_version WHERE scope=$1 AND (path=$2 OR starts_with(path, $2 || '/'))
			 AND version < $3`,
			it.scope, it.path, it.version)
	} else {
		_, derr = tx.Exec(ctx,
			`DELETE FROM file_version WHERE scope=$1 AND path=$2 AND version < $3`,
			it.scope, it.path, it.version)
	}
	if derr != nil {
		return false
	}
	if _, err := tx.Exec(ctx, `DELETE FROM file_op WHERE id=$1`, it.id); err != nil {
		return false
	}
	return tx.Commit(ctx) == nil
}

// tombstoneIntent marks an intent resolved WITHOUT deleting its evidence:
// the reconciler keeps re-judging tombstones so a filesystem effect that
// lands after the drop is still settled truthfully (roll-forward), and the
// hot window keeps new ops out of the affected subtree for one grace
// period. Owner-fenced like dropIntent.
func (s *Store) tombstoneIntent(ctx context.Context, it intent) bool {
	ctx, cancel := s.dbCtx(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false
	}
	defer tx.Rollback(ctx)
	if err := s.checkOwnerTx(ctx, tx); err != nil {
		return false
	}
	if _, err := tx.Exec(ctx,
		`UPDATE file_op SET resolved_at=now(), stalled_at=NULL, last_error=''
		 WHERE id=$1 AND resolved_at IS NULL`, it.id); err != nil {
		return false
	}
	return tx.Commit(ctx) == nil
}

// tombstoneIntentGhosts: tombstone + dead-row cleanup in one tx — the
// intent's evidence is retained while rows the disk proves absent are
// deleted (no event: observed absence is not a performed operation).
func (s *Store) tombstoneIntentGhosts(ctx context.Context, it intent, subtree bool) bool {
	ctx, cancel := s.dbCtx(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false
	}
	defer tx.Rollback(ctx)
	if err := s.checkOwnerTx(ctx, tx); err != nil {
		return false
	}
	var derr error
	if subtree {
		_, derr = tx.Exec(ctx,
			`DELETE FROM file_version WHERE scope=$1 AND (path=$2 OR starts_with(path, $2 || '/'))
			 AND version < $3`,
			it.scope, it.path, it.version)
	} else {
		_, derr = tx.Exec(ctx,
			`DELETE FROM file_version WHERE scope=$1 AND path=$2 AND version < $3`,
			it.scope, it.path, it.version)
	}
	if derr != nil {
		return false
	}
	if _, err := tx.Exec(ctx,
		`UPDATE file_op SET resolved_at=now(), stalled_at=NULL, last_error=''
		 WHERE id=$1 AND resolved_at IS NULL`, it.id); err != nil {
		return false
	}
	return tx.Commit(ctx) == nil
}

// markStalled records that an intent cannot be judged right now: the
// filesystem answered with a non-definitive error (unreachable,
// permission-denied). The intent stays pending — the row is the durable
// record of "outcome unknown" and is never dropped on a timer; the cause
// is surfaced to callers the exclusion refuses (f119/B-2).
func (s *Store) markStalled(ctx context.Context, it intent, cause error) {
	ctx, cancel := s.dbCtx(ctx)
	defer cancel()
	if _, err := s.pool.Exec(ctx,
		`UPDATE file_op SET stalled_at=COALESCE(stalled_at, now()), last_error=$2
		 WHERE id=$1 AND resolved_at IS NULL`, it.id, cause.Error()); err != nil {
		log.Printf("reconcile: mark stalled intent %d: %v", it.id, err)
	}
}

// recordedMatch reports whether the row fingerprint records this live
// object — fp3 for files, inode identity for directories.
func recordedMatch(rowFP string, st FileInfo) bool {
	if st.Kind == "dir" {
		ri, _, _, ok1 := fpParts(rowFP)
		si, _, _, ok2 := fpParts(st.Fingerprint)
		return ok1 && ok2 && ri == si
	}
	return fp3(rowFP) == fp3(st.Fingerprint)
}

// sameObjectAt reports whether dst is the same object as st — the
// "still at its recorded home" check. The comparison is the complete
// live object identity dev:ino: an inode number alone is per-filesystem,
// so a foreign-device object with a colliding inode is correctly NOT
// the same. Missing device evidence fails closed — without dev:ino the
// stats cannot prove the same live object — and callers on
// delete-authorizing paths treat "not proven same" as "keep". Restore
// paths still converge: a genuinely-at-home occupant is protected from
// a swap by parkedOwnsRow's fingerprint evidence.
func sameObjectAt(st, dst FileInfo) bool {
	return st.Kind == dst.Kind && st.DevIno != "" &&
		st.DevIno == dst.DevIno
}

// intentApplied reports whether this intent's effect committed and was
// recorded: its apply journaled the file_event row AND removed the intent
// row, in one transaction. The event alone is not that proof — a diverged
// roll-forward journals an event while retaining the intent as a
// tombstone, and it records an observation of foreign content, not an
// effect this op performed. Nor is a matching staged fingerprint: a
// delayed drain or swap can park an object into the intent's namespace
// without its effect ever landing (F-3). Both facts are read in one
// statement, from one snapshot. Rename's event is journaled at the
// destination path.
func (s *Store) intentApplied(ctx context.Context, it intent) (bool, error) {
	if s.pool == nil {
		return false, nil // test-only Store without a pool: no commit proof
	}
	evPath := it.path
	if it.op == "rename" {
		evPath = it.toPath
	}
	dctx, cancel := s.dbCtx(ctx)
	defer cancel()
	var n int
	err := s.pool.QueryRow(dctx,
		`SELECT 1 FROM file_event
		  WHERE scope=$1 AND path=$2 AND op=$3 AND version=$4
		    AND NOT EXISTS (SELECT 1 FROM file_op WHERE id=$5)
		  LIMIT 1`,
		it.scope, evPath, it.op, it.version, it.id).Scan(&n)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// recordersActive reports whether a row writer outside the calling
// reconcile pass may still commit a version row for an object it observed
// at a public path before this call. Every row writer (apply,
// relocateRows) is owner-fenced inside its tx and holds the store_meta
// row FOR SHARE until commit, so bindRoot waits for in-flight writers:
//
//   - store_meta names another instance: a successor may record objects
//     this pass cannot see — true.
//   - store_meta names this instance: every other instance's row write
//     committed before this read, and any later owner binds after it and
//     observes the filesystem only after it. What remains is this
//     instance's own writers. Reconcile passes hold the scope mutex the
//     caller holds, but a settler goroutine outlives its caller's
//     opTimeout and applies without it (f82) — true while a settler of
//     this scope is still running. Its inflight mark is removed only
//     after its apply returns, so a settler absent here has committed
//     already or never will.
//
// Callers read this BEFORE the row set, so an apply committing between
// the two reads is seen by one of them. This holds for a pass that
// entered before its store was deposed: the check is a DB fact, not the
// deposed flag. Objects move only within a scope, so other scopes'
// settlers are irrelevant. A DB error reports (true, err).
func (s *Store) recordersActive(ctx context.Context, scope string) (bool, error) {
	if s.pool == nil {
		return false, nil // test-only Store without a pool: no row writers exist
	}
	dctx, cancel := s.dbCtx(ctx)
	defer cancel()
	var owner string
	if err := s.pool.QueryRow(dctx,
		`SELECT owner FROM store_meta WHERE id`).Scan(&owner); err != nil {
		return true, err
	}
	if owner != s.owner {
		return true, nil
	}
	busy := false
	s.inflight.Range(func(_, v any) bool {
		if sc, ok := v.(string); !ok || sc == scope {
			busy = true
			return false
		}
		return true
	})
	return busy, nil
}

// When the object cannot be identified (no dev:ino — a stat whose
// Sys() is not Stat_t) the path keys the entry: distinct paths are then
// never deduplicated, which is the safe direction — a cyclic structure
// of unidentifiable dirs can lengthen paths, but path resolution fails
// closed at the OS limit, ending the walk.
func markSeen(seen map[string]struct{}, st FileInfo, rel string) bool {
	key := "p:" + rel
	if st.DevIno != "" {
		key = "o:" + st.DevIno
	}
	if _, dup := seen[key]; dup {
		return false
	}
	seen[key] = struct{}{}
	return true
}

// --- Private-name executor (protocol-v3) --------------------------------
//
// Every object the service parks lives under a journaled private name —
// committed in file_op.names BEFORE the syscall that could populate it.
// Recovery never moves a public object on fingerprint evidence; it
// settles only the names its own journal owns. Home authority comes
// from the originating intent's declared identities (pre/dst fp+oid),
// never from an arbitrary older row.

// obsIdent encodes one observed object identity for the journal:
// the fp3 triple plus the durable oid when bound.
func obsIdent(st FileInfo) string {
	if st.Oid != "" {
		return fp3(st.Fingerprint) + "|" + st.Oid
	}
	return fp3(st.Fingerprint)
}

// nameDir resolves the scope-relative directory a name record lives in.
func nameDir(it intent, rec nameRec) string {
	if rec.Dir != "" {
		return rec.Dir
	}
	d, _ := splitRel(it.path)
	return d
}

// nameRel resolves the record's scope-relative path.
func nameRel(it intent, rec nameRec) string {
	if d := nameDir(it, rec); d != "" {
		return d + "/" + rec.Name
	}
	return rec.Name
}

// nameContent classifies the object found at a journaled private name
// against the intent's declared identities. Bound durable identity is
// always judged first — when the filesystem proves the object is (or is
// not) a declared object, content evidence is only a fallback.
type nameContent int

const (
	ncUnknown       nameContent = iota // no declared identity matches — foreign residue
	ncAuthored                         // bytes this op authored (staged body) — sha proof
	ncCaptured                         // the op's declared source object (pre_oid / pre_fp)
	ncDisplaced                        // the declared displaced destination object (dst_oid bound)
	ncDisplacedWeak                    // displaced by weak evidence only (fp3+sha, oid unbound)
)

func (s *Store) classifyName(it intent, view ReconView, rel string, st FileInfo, rec nameRec) nameContent {
	// After a declared exchange the name can hold the displaced object,
	// not our staged body — and while its result is unrecorded a late
	// landing makes that ambiguous. Content equal to what we authored
	// is then NOT authorship proof: it is at best a displaced object
	// with equal bytes, which no content evidence may condemn.
	authoredOK := rec.Act != "xch" || rec.Res == "noeff"
	// Bound identity first — it can PROVE foreign-ness too.
	if st.Oid != "" {
		if it.dstOid != "" && st.Oid == it.dstOid {
			return ncDisplaced
		}
		if it.preOid != "" && st.Oid == it.preOid {
			return ncCaptured
		}
		// A bound identity matching no declared object is definitely
		// not one of them — unless it is our own staged body, which
		// carries a fresh oid: for a write intent the content hash is
		// the authorship proof.
		if authoredOK && it.op == "write" && it.expectSHA != "" && st.Kind == "file" {
			if h, err := view.Hash(it.scope, rel); err == nil && h == it.expectSHA {
				return ncAuthored
			}
		}
		return ncUnknown
	}
	// Unbound object — weaker evidence only.
	if authoredOK && it.op == "write" && it.expectSHA != "" && st.Kind == "file" {
		if h, err := view.Hash(it.scope, rel); err == nil && h == it.expectSHA {
			return ncAuthored
		}
	}
	// Displaced before captured: for write/remove intents declare aliases
	// pre_fp to dst_fp (the occupant IS the displaced object), and the
	// displaced reading is the one with discard semantics.
	if it.dstFP != "" && fp3(st.Fingerprint) == fp3(it.dstFP) {
		if it.dstSHA == "" || st.Kind != "file" {
			return ncDisplacedWeak
		}
		if h, err := view.Hash(it.scope, rel); err == nil && h == it.dstSHA {
			return ncDisplacedWeak
		}
	}
	if it.op == "rename" && it.preFP != "" && fp3(st.Fingerprint) == fp3(it.preFP) {
		// Could be the declared source. For a file, confirm by content
		// when evidence exists; a dir's fp3 is ino:size:mtime — weaker,
		// but a moved dir keeps all three.
		if it.srcKind != "file" || it.expectSHA == "" {
			return ncCaptured
		}
		if h, err := view.Hash(it.scope, rel); err == nil && h == it.expectSHA {
			return ncCaptured
		}
	}
	return ncUnknown
}

// opEffectLanded reports whether the intent's filesystem effect provably
// committed — judged on the public paths, never on the private name.
func (s *Store) opEffectLanded(it intent, view ReconView) bool {
	switch it.op {
	case "write":
		if it.expectSHA == "" {
			return false
		}
		h, err := view.Hash(it.scope, it.path)
		return err == nil && h == it.expectSHA
	case "rename":
		st, err := view.Stat(it.scope, it.toPath)
		if err != nil {
			return false
		}
		if it.srcKind == "file" && it.expectSHA != "" {
			h, herr := view.Hash(it.scope, it.toPath)
			return herr == nil && h == it.expectSHA
		}
		return it.preFP != "" && fp3(st.Fingerprint) == fp3(it.preFP)
	case "remove":
		_, err := view.Stat(it.scope, it.path)
		return absentVerdict(err)
	case "mkdir":
		st, err := view.Stat(it.scope, it.path)
		return err == nil && st.Kind == "dir"
	}
	return false
}

// settleNames executes one intent's private-name journal beneath the
// pass view. For each live record it observes what the name holds,
// records the observation, then CAPTURES the occupant into a freshly
// declared name before judging or moving it:
//
//   - the journal append commits before the capture can populate the
//     new name — the same declare-before-populate ordering the op names
//     follow, so a takeover never meets an unrecorded name.
//   - judging at the fresh name closes the stat→act window: a delayed
//     effect can still swap the occupant of an old name, but it cannot
//     target a name that did not exist when the effect was issued.
//   - the vacated name keeps its record: an act with an unrecorded
//     result stays tombstoned, and every name is re-stat'd each pass so
//     a late-arriving deposit is re-judged rather than skipped forever.
//
// Disposition after classification:
//
//   - authored bytes already acknowledged at the public path: discard
//     (private-name provenance — we created the object under our own
//     journaled name; the content is provably acknowledged elsewhere).
//   - authored/captured content whose op did not land: restore to its
//     origin if free, else surface at a visible sibling name.
//   - the declared displaced object: discard ONLY on bound-oid proof
//     plus a committed effect plus nlink==1 (a hardlinked object is
//     shared — never deleted for one name's reconciliation). Weak
//     evidence (fp3/sha on an unbound object) can restore or surface,
//     never delete — content similarity is not discard authority.
//   - unknown/foreign content: surfaced visibly — preserved, readable,
//     deletable — never destroyed. A recorded row that provably
//     describes it (bound oid, or fp3+sha with the recorded path
//     absent) re-keys onto the surface name or its free recorded home.
func (s *Store) settleNames(ctx context.Context, it intent, view ReconView, tombstoned bool) {
	names := it.names
	for i := 0; i < len(names); i++ {
		rec := names[i]
		if rec.Done {
			continue
		}
		rel := nameRel(it, rec)
		st, serr := view.Stat(it.scope, rel)
		switch {
		case absentVerdict(serr):
			if rec.Act != "" && rec.Res == "" {
				// Declared act, no result, empty name: a late effect
				// can still land — keep the evidence.
				it.journal.tomb(i, "act-unresulted")
			} else if rec.Tomb == "" {
				it.journal.done(i)
			}
			continue
		case serr != nil:
			continue // unverifiable this pass — retry later
		}
		// The name is populated. A tombstone only recorded an empty
		// name at an earlier pass — a late effect arrived and must be
		// judged now.
		it.journal.obs(i, obsIdent(st))
		fi, frel, fst, ok := s.captureName(ctx, &it, &names, view, rec, rel)
		if !ok {
			continue
		}
		s.judgeName(ctx, &it, &names, view, fi, frel, fst, rec)
		// Judged or not, the fresh record is not re-captured inside this
		// pass — a failed disposition retries next pass (the record is
		// still live in the journal).
		names[fi].Done = true
	}
}

// appendName commits a new record to the intent's names journal BEFORE
// the name can be populated — the declare-before-populate ordering is
// what lets a later owner trust that every populated private name has
// an accountable record. The local slice grows in lockstep so journal
// indexes match the DB array.
func (s *Store) appendName(ctx context.Context, it *intent, names *[]nameRec, rec nameRec) (int, bool) {
	dctx, cancel := s.dbCtx(ctx)
	defer cancel()
	if _, err := s.pool.Exec(dctx,
		`UPDATE file_op SET names = names || $2::jsonb WHERE id=$1`,
		it.id, mustJSON([]nameRec{rec})); err != nil {
		log.Printf("reconcile: op %d journal append: %v", it.id, err)
		return 0, false
	}
	*names = append(*names, rec)
	return len(*names) - 1, true
}

// captureName moves the occupant of rel into a freshly declared private
// name of the same intent — atomically, via NOREPLACE — then observes
// it there. Judging and disposition happen on the fresh name: no
// in-flight or delayed syscall could have been aimed at a name that did
// not yet exist, so the verify→act window is closed. The capture's
// result is journaled; an empty rel reports no-capture and lets the
// source record's own empty handling run next pass.
func (s *Store) captureName(ctx context.Context, it *intent, names *[]nameRec, view ReconView, rec nameRec, rel string) (int, string, FileInfo, bool) {
	for try := 0; try < 8; try++ {
		// The fresh name must not carry a "-q-" segment from the record's
		// own base: sealed names refuse all writes, so a capture target
		// derived from one could never be populated.
		fresh := fmt.Sprintf("%sc%d-%s", opStagePrefix, it.id, randHex(5))
		fi, ok := s.appendName(ctx, it, names,
			nameRec{Name: fresh, Dir: rec.Dir, Act: "cap", Src: rel})
		if !ok {
			return 0, "", FileInfo{}, false
		}
		frel := nameRel(*it, (*names)[fi])
		err := view.MoveStaged(it.scope, rel, frel)
		switch {
		case err == nil:
			it.journal.res(fi, "ok")
			st, serr := view.Stat(it.scope, frel)
			if serr != nil {
				return fi, frel, FileInfo{}, false
			}
			it.journal.obs(fi, obsIdent(st))
			return fi, frel, st, true
		case errors.Is(err, ErrNotFound), errors.Is(err, ErrNotDir):
			// The name emptied between stat and capture — nothing to
			// judge; the source record's empty handling applies.
			it.journal.res(fi, "noeff")
			it.journal.done(fi)
			return 0, "", FileInfo{}, false
		case errors.Is(err, ErrConflict):
			continue // fresh-name collision — try another suffix
		default:
			return 0, "", FileInfo{}, false
		}
	}
	return 0, "", FileInfo{}, false
}

// judgeName disposes of the object captured at fresh name rel (record
// index i). prov is the record that originally owned the name the
// object sat at — its declared act and observed result describe what
// the op meant that name to hold, which is what makes a matching
// identity operation-bound rather than a coincidence.
func (s *Store) judgeName(ctx context.Context, it *intent, names *[]nameRec, view ReconView, i int, rel string, st FileInfo, prov nameRec) {
	cls := s.classifyName(*it, view, rel, st, prov)
	if st.Kind == "dir" && cls != ncAuthored {
		// Before the container itself is disposed, extract any member
		// whose recorded home is free and is NOT where riding the
		// container would put it — recorded membership changes survive
		// even when the container object is unknown.
		ride, rideFree, _ := s.recordedHome(ctx, *it, view, st, rel)
		if !rideFree {
			// The container will surface under a name that cannot be
			// known yet — no member can ride it home, so every recorded
			// member is judged for extraction on its own.
			ride = ""
		}
		seen := map[string]struct{}{}
		markSeen(seen, st, rel)
		s.extractRecordedMembers(ctx, it, names, view, rel, ride, 0, seen)
	}
	switch cls {
	case ncAuthored:
		// Our own staged body. If the write's content is already
		// acknowledged at the path, this is a redundant authored copy —
		// discarding it is operation-bound disposal, not ambiguous
		// recovery.
		if s.opEffectLanded(*it, view) {
			if err := view.RemoveName(it.scope, rel); err == nil {
				it.journal.done(i)
			}
			return
		}
		// Not landed: return the authored bytes to their origin or
		// surface them visibly.
		s.restoreOrSurface(ctx, *it, view, i, rel, it.path, st, true, false)
	case ncCaptured:
		// The declared source object, captured but never landed at its
		// destination — its own row (if one provably describes it) is
		// the better home; otherwise restore to the op's source path.
		origin, rowHome := it.path, true
		if home, _, ok := s.recordedHome(ctx, *it, view, st, rel); ok {
			origin = home
		}
		s.restoreOrSurface(ctx, *it, view, i, rel, origin, st, false, rowHome)
	case ncDisplaced:
		// Bound-oid proof: this IS the declared displaced object.
		// Discard requires the whole chain: the act was declared, its
		// result was observed, the op's effect is committed, and the
		// object is not shared or a directory. Content similarity alone
		// never authorizes destroying a displaced object. For a remove
		// the journaled capture IS the committed effect — the object was
		// taken off its path by the act itself.
		landed := it.op == "remove" || s.opEffectLanded(*it, view)
		if prov.Act != "" && prov.Res == "ok" && landed &&
			st.Nlink <= 1 && st.Kind == "file" {
			if err := view.RemoveName(it.scope, rel); err == nil {
				it.journal.done(i)
			}
			return
		}
		// Not committed (or a shared/dir object): its own row is the
		// better home when one provably describes it; otherwise restore
		// to the op's displaced origin — never destroy.
		origin, rowHome := it.path, it.op == "rename"
		if it.op == "rename" {
			origin = it.toPath
		}
		if home, _, ok := s.recordedHome(ctx, *it, view, st, rel); ok {
			origin, rowHome = home, true
		}
		s.restoreOrSurface(ctx, *it, view, i, rel, origin, st, false, rowHome)
	case ncDisplacedWeak:
		origin := it.path
		if it.op == "rename" {
			origin = it.toPath
		}
		if it.op == "remove" && prov.Act != "" && prov.Res == "ok" &&
			st.Nlink <= 1 && st.Kind == "file" {
			// A remove's whole purpose was deleting the declared object:
			// the journaled capture is the operation-bound proof that
			// THIS object was taken from the path the user removed.
			// Weak identity suffices — the alternative is resurrecting
			// a file the user deleted.
			if err := view.RemoveName(it.scope, rel); err == nil {
				it.journal.done(i)
				return
			}
		}
		// Weak displaced evidence on a write/rename can restore or
		// surface, never delete — its own row is the better home when
		// one provably describes it.
		rowHome := it.op == "rename"
		if home, _, ok := s.recordedHome(ctx, *it, view, st, rel); ok {
			origin, rowHome = home, true
		}
		s.restoreOrSurface(ctx, *it, view, i, rel, origin, st, false, rowHome)
	default:
		// Unknown/foreign content under our name — it arrived through a
		// recorded act whose declared identity does not match, or at a
		// name with no act at all (an orphan sweep record). When a
		// version row provably describes this object — bound oid, or
		// fp3+sha with the recorded path absent — it is restored to that
		// recorded home or surfaced with the row re-keyed; otherwise it
		// is surfaced under a fresh row. It is never moved onto a
		// public path on a guess and never destroyed.
		if home, _, ok := s.recordedHome(ctx, *it, view, st, rel); ok {
			s.restoreOrSurface(ctx, *it, view, i, rel, home, st, false, true)
			return
		}
		s.restoreOrSurface(ctx, *it, view, i, rel, "", st, false, false)
	}
}

// recordedHome finds the version row that provably describes the object
// at rel — the object's recorded path. Evidence is ranked: a bound
// durable identity outranks everything; a file needs fp3 and
// content_sha together; a dir matches on fp3 alone (member churn moves
// its size/mtime legs; the inode leg survives a rename). Returns the
// recorded path, whether that path is currently free (observed absent),
// and whether a unique claimant exists at all. A free path is a home
// the object can be restored to; an occupied one is still the right
// sibling base for surfacing — the occupant is never evicted. Any
// ambiguity (two live candidates) yields no path — the object surfaces
// under a fresh name instead.
func (s *Store) recordedHome(ctx context.Context, it intent, view ReconView, st FileInfo, rel string) (string, bool, bool) {
	sha := ""
	if st.Kind == "file" {
		h, err := view.Hash(it.scope, rel)
		if err != nil {
			return "", false, false // content unverifiable — cannot claim a home
		}
		sha = h
	}
	dctx, cancel := s.dbCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(dctx,
		`SELECT path, fp, oid FROM file_version WHERE scope=$1 AND
		 ($2::text <> '' AND oid = $2 OR $3::text <> '' AND content_sha = $3 OR content_sha = 'dir')`,
		it.scope, st.Oid, sha)
	if err != nil {
		return "", false, false
	}
	defer rows.Close()
	type cand struct {
		path, fp, oid string
		strong        bool
	}
	var cands []cand
	for rows.Next() {
		var c cand
		if rows.Scan(&c.path, &c.fp, &c.oid) != nil {
			continue
		}
		if c.oid != "" && st.Oid != "" && c.oid != st.Oid {
			// Both sides carry a bound durable identity and they
			// disagree — affirmative evidence the row recorded a
			// different object (e.g. a stale row over a reused inode),
			// not a weak claim to fall back on.
			continue
		}
		switch {
		case st.Oid != "" && c.oid == st.Oid:
			c.strong = true
		case st.Kind == "file":
			// A file row must agree on content AND fp3.
			if sha == "" || fp3(c.fp) != fp3(st.Fingerprint) {
				continue
			}
		default:
			// Dir/other: oid unbound — the inode leg is the only
			// identity that survives a move.
			ino, _, _, ok := fpParts(st.Fingerprint)
			rino, _, _, rok := fpParts(c.fp)
			if !ok || !rok || ino != rino {
				continue
			}
		}
		cands = append(cands, c)
	}
	// Bound-oid candidates dominate: if any exist only they are homes.
	hasStrong := false
	for _, c := range cands {
		if c.strong {
			hasStrong = true
		}
	}
	free := ""
	freeN := 0
	occupied := ""
	for _, c := range cands {
		if hasStrong && !c.strong {
			continue
		}
		_, serr := view.Stat(it.scope, c.path)
		switch {
		case absentVerdict(serr):
			free, freeN = c.path, freeN+1
		case occupied == "":
			// Occupied — or unverifiable, which conservatively is not
			// free either. Still the right sibling base for surfacing.
			occupied = c.path
		}
	}
	if freeN == 1 {
		return free, true, true
	}
	if freeN == 0 && occupied != "" {
		return occupied, false, true
	}
	return "", false, false // zero claimants, or several free — ambiguous
}

// extractRecordedMembers walks the members of a captured directory
// before the container is disposed: a member whose version row provably
// describes it and whose recorded home is free is moved there directly
// — recorded membership changes survive even when the container object
// itself is unknown or homeless. ride is the path the container will
// occupy ("" when it will surface under a fresh name — then no member
// can reach its recorded home by riding). A member whose recorded home
// equals its ride target is left in place: moving the container already
// returns it. A member dir with its own free recorded home has ITS
// members extracted first (against that home), then moves whole.
// Members never evict a public occupant — MoveStaged is NOREPLACE, and
// recordedHome only reports paths observed absent. Depth-bounded and
// deduplicated by object identity so a cyclic structure terminates.
func (s *Store) extractRecordedMembers(ctx context.Context, it *intent, names *[]nameRec, view ReconView, rel, ride string, depth int, seen map[string]struct{}) {
	if depth > 256 {
		return
	}
	members, err := view.ListDir(it.scope, rel)
	if err != nil {
		return
	}
	for _, m := range members {
		if strings.HasPrefix(m, opStagePrefix) {
			continue // nested private names belong to their own records
		}
		mrel := rel + "/" + m
		st, serr := view.Stat(it.scope, mrel)
		if serr != nil {
			continue // unverifiable — leave it riding the container
		}
		// rideTo is where the member ends up if left riding the
		// container; "" when the container's own destination is unknown
		// (it will surface under a fresh name) — then no member can ride
		// home and every recorded member must be extracted.
		rideTo := ""
		if ride != "" {
			rideTo = ride + "/" + m
		}
		home, free, _ := s.recordedHome(ctx, *it, view, st, mrel)
		if st.Kind == "dir" {
			if !markSeen(seen, st, mrel) {
				continue // cyclic alias — already walked
			}
			if free {
				if rideTo != "" && home == rideTo {
					continue // rides home — nothing to extract
				}
				// Extract inner members that cannot ride to this home,
				// then move the dir whole.
				s.extractRecordedMembers(ctx, it, names, view, mrel, home, depth+1, seen)
			} else {
				s.extractRecordedMembers(ctx, it, names, view, mrel, rideTo, depth+1, seen)
				continue
			}
		}
		if !free || (rideTo != "" && home == rideTo) {
			continue
		}
		mi, mok := s.appendName(ctx, it, names,
			nameRec{Name: mrel, Act: "mv", Src: mrel})
		if !mok {
			continue
		}
		if d, _ := splitRel(home); d != "" {
			_ = view.EnsureDir(it.scope, d)
		}
		if merr := view.MoveStaged(it.scope, mrel, home); merr == nil {
			it.journal.res(mi, "ok")
			it.journal.done(mi)
			s.restoredRow(ctx, *it, view, home, mrel)
		} else {
			// Home filled between check and move, or the move is
			// unverifiable — the member rides the container instead.
			it.journal.res(mi, "noeff")
			it.journal.done(mi)
		}
	}
}

// restoreOrSurface returns the object at private name rel to origin when
// the name is free (NOREPLACE — a live occupant always wins), else moves
// it to a visible sibling name and records a readable row + event so
// people and secretaries can read and delete it. authored=true marks
// content the op authored itself (a write body) — surfaced with a fresh
// version row; captured/displaced content keeps its recorded version row
// when one describes it.
func (s *Store) restoreOrSurface(ctx context.Context, it intent, view ReconView, i int, rel, origin string, st FileInfo, authored, rowHome bool) {
	if origin != "" {
		ost, serr := view.Stat(it.scope, origin)
		switch {
		case absentVerdict(serr):
			// Origin is free — restore home. Recreate a missing parent
			// chain first.
			if d, _ := splitRel(origin); d != "" {
				_ = view.EnsureDir(it.scope, d)
			}
			if err := view.MoveStaged(it.scope, rel, origin); err == nil {
				it.journal.done(i)
				log.Printf("reconcile: restored %s -> %s (op %d)", rel, origin, it.id)
				if rowHome {
					s.restoredRow(ctx, it, view, origin, rel)
				}
				return
			} else if !errors.Is(err, ErrConflict) {
				return // unverifiable move — retry next pass
			}
			// Occupied between stat and move — fall through to surface.
		case serr != nil:
			return // origin unverifiable — retry next pass
		case sameObjectAt(st, ost):
			// The origin's occupant IS this object — it is already home
			// (e.g. another link of a multiply-linked file, or a dir
			// that never left). The private name is a duplicate link;
			// removing it is operation-bound disposal of our own name.
			if err := view.RemoveName(it.scope, rel); err == nil {
				it.journal.done(i)
			}
			return
		}
		// Occupied by a different object — the occupant is never
		// evicted; fall through to surface at a visible sibling.
	}
	// Surface at a visible sibling of the origin (or of the private
	// name's own directory when no origin is known).
	dir, base := splitRel(origin)
	if origin == "" {
		dir, _ = splitRel(rel)
		base = ""
	}
	stem := "recovered-o" + strconv.FormatInt(it.id, 10)
	if base != "" && !strings.HasPrefix(base, opStagePrefix) {
		stem = base + "." + stem
	}
	for k := 0; k < 8; k++ {
		cand := stem
		if k > 0 {
			cand += "-" + strconv.Itoa(k)
		}
		target := cand
		if dir != "" {
			target = dir + "/" + cand
		}
		if err := view.MoveStaged(it.scope, rel, target); err == nil {
			it.journal.done(i)
			s.surfacedRow(ctx, it, view, origin, target, rel, authored)
			return
		} else if !errors.Is(err, ErrConflict) {
			return
		}
	}
	// No surface name available — leave parked under the owned name.
}

// restoredRow refreshes the version row at origin after the object was
// moved back: the move changed its fp (ctime leg) but the row's version
// and history are preserved — the recorded object is home, so the row
// stays truthful and a 'recover' event journals the observation.
// Best-effort: a failed write leaves correct bytes with a stale fp,
// which ordinary reads reconcile via live-fingerprint comparison.
func (s *Store) restoredRow(ctx context.Context, it intent, view ReconView, origin, rel string) {
	dctx, cancel := s.dbCtx(ctx)
	defer cancel()
	live, lerr := view.Stat(it.scope, origin)
	if lerr != nil {
		return
	}
	tx, err := s.pool.BeginTx(dctx, pgx.TxOptions{})
	if err != nil {
		return
	}
	defer tx.Rollback(dctx)
	if err := s.checkOwnerTx(dctx, tx); err != nil {
		return
	}
	var ver int64
	if err := tx.QueryRow(dctx,
		`UPDATE file_version SET fp=$3, oid=$4, updated=now()
		 WHERE scope=$1 AND path=$2 RETURNING version`,
		it.scope, origin, live.Fingerprint, live.Oid).Scan(&ver); err != nil {
		return // no row (or raced) — nothing to refresh
	}
	if _, err := tx.Exec(dctx,
		`INSERT INTO file_event (scope, path, from_path, op, version)
		 VALUES ($1,$2,$3,'recover',$4)`,
		it.scope, origin, rel, ver); err != nil {
		return
	}
	if err := tx.Commit(dctx); err != nil {
		log.Printf("reconcile: restored %s row commit: %v", origin, err)
	}
}

// surfacedRow records a surfaced object so it is readable and deletable
// through the API: when a version row still describes the surfaced
// object at its (now occupied or empty) origin, the row is re-keyed to
// the surface name preserving its version; otherwise a fresh version is
// minted. Either way a 'recover' event is journaled. Best-effort: a
// failure leaves the object visible at the surface name and re-tries
// next pass is not needed — the row write is retried by marking the
// name not-done is WRONG (object is gone); so this does its own tx and
// a failed write only loses the journal entry, never the bytes.
func (s *Store) surfacedRow(ctx context.Context, it intent, view ReconView, origin, target, rel string, authored bool) {
	dctx, cancel := s.dbCtx(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(dctx, pgx.TxOptions{})
	if err != nil {
		log.Printf("reconcile: surfaced %s without row: %v", target, err)
		return
	}
	defer tx.Rollback(dctx)
	if err := s.checkOwnerTx(dctx, tx); err != nil {
		return
	}
	live, lerr := view.Stat(it.scope, target)
	if lerr != nil {
		return // surfaced object vanished before its row — leave it
	}
	sha := ""
	switch live.Kind {
	case "file":
		sha, _ = view.Hash(it.scope, target)
	case "dir":
		sha = "dir"
	}
	// Find the row that already records this object: fp3 and content
	// must both agree (sha where the row carries one), and the row's
	// recorded path must be absent — a row still satisfied by a live
	// path is not this object's home. This is bookkeeping, not object
	// authority: the surfaced object never moves on row evidence.
	if !authored && sha != "" {
		var ver int64
		var oldPath, rowFP string
		rows, qerr := tx.Query(dctx,
			`SELECT path, version, fp FROM file_version
			 WHERE scope=$1 AND content_sha=$2 FOR UPDATE`,
			it.scope, sha)
		type cand struct {
			path, fp string
			ver      int64
		}
		var cands []cand
		if qerr == nil {
			for rows.Next() {
				var c cand
				if rows.Scan(&c.path, &c.ver, &c.fp) == nil {
					cands = append(cands, c)
				}
			}
			rows.Close()
		}
		for _, c := range cands {
			if fp3(c.fp) != fp3(live.Fingerprint) {
				continue
			}
			if _, serr := view.Stat(it.scope, c.path); !absentVerdict(serr) {
				continue // recorded path still occupied — not this row
			}
			ver, oldPath, rowFP = c.ver, c.path, c.fp
			break
		}
		_ = rowFP
		if oldPath != "" {
			if _, err := tx.Exec(dctx,
				`UPDATE file_version SET path=$3, fp=$4, updated=now()
				 WHERE scope=$1 AND path=$2 AND version=$5`,
				it.scope, oldPath, target, live.Fingerprint, ver); err != nil {
				return
			}
			if _, err := tx.Exec(dctx,
				`INSERT INTO file_event (scope, path, from_path, op, version)
				 VALUES ($1,$2,$3,'recover',$4)`,
				it.scope, target, oldPath, ver); err != nil {
				return
			}
			if err := tx.Commit(dctx); err != nil {
				log.Printf("reconcile: surfaced %s row commit: %v", target, err)
			}
			return
		}
	}
	// No row describes this object: mint a fresh version so the surfaced
	// object is an ordinary readable/deletable file.
	var ver int64
	if err := tx.QueryRow(dctx,
		`SELECT nextval('file_version_seq')`).Scan(&ver); err != nil {
		return
	}
	if _, err := tx.Exec(dctx,
		`INSERT INTO file_version (scope, path, version, fp, content_sha, oid)
		 VALUES ($1,$2,$3,$4,$5,$6)
		 ON CONFLICT (scope, path) DO NOTHING`,
		it.scope, target, ver, live.Fingerprint, sha, live.Oid); err != nil {
		return
	}
	if _, err := tx.Exec(dctx,
		`INSERT INTO file_event (scope, path, from_path, op, version)
		 VALUES ($1,$2,$3,'recover',$4)`,
		it.scope, target, rel, ver); err != nil {
		return
	}
	if err := tx.Commit(dctx); err != nil {
		log.Printf("reconcile: surfaced %s row commit: %v", target, err)
	}
}

// sweepOrphanNames discovers .filesv-op-* objects whose owning intent no
// longer records them — a name committed to a journal that was then
// dropped, or residue from a rejected mechanism. Each orphan is attached
// to a surviving intent when one still journals it; otherwise a
// dedicated 'recover' intent is created so the object's settlement is
// itself journaled — the executor surfaces it visibly, never destroys
// it.
func (s *Store) sweepOrphanNames(ctx context.Context, view ReconView) {
	for _, scope := range s.knownScopes(ctx) {
		s.sweepScopeNames(ctx, scope, view)
	}
}

// knownScopes lists every scope with recorded state — version rows or
// intents under this root.
func (s *Store) knownScopes(ctx context.Context) []string {
	dctx, cancel := s.dbCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(dctx,
		`SELECT scope FROM file_version
		 UNION SELECT scope FROM file_op WHERE root=$1`, s.rootID)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var sc string
		if rows.Scan(&sc) == nil {
			out = append(out, sc)
		}
	}
	return out
}

// sweepScopeNames walks one scope's directories for orphan private
// names. Depth-bounded and dedup'd by object identity so a cyclic
// structure cannot spin the walk.
func (s *Store) sweepScopeNames(ctx context.Context, scope string, view ReconView) {
	seen := map[string]struct{}{}
	var walk func(dir string)
	walk = func(dir string) {
		st, err := view.Stat(scope, dir)
		if err != nil || st.Kind != "dir" {
			return
		}
		if !markSeen(seen, st, dir) {
			return
		}
		names, lerr := view.ListStaged(scope, dir, opStagePrefix)
		if lerr != nil {
			return
		}
		for _, name := range names {
			if !strings.HasPrefix(name, opStagePrefix) {
				continue // only private recovery names are owned names
			}
			s.attachOrphanName(ctx, scope, view, dir, name)
		}
		if depth := strings.Count(dir, "/"); depth < 256 {
			// Recurse into subdirectories discovered by listing all
			// entries — ListStaged only returns staged names, so walk
			// the members via a plain listing when available.
			if members, merr := view.ListDir(scope, dir); merr == nil {
				for _, m := range members {
					child := m
					if dir != "" {
						child = dir + "/" + m
					}
					walk(child)
				}
			}
		}
	}
	walk("")
}

// attachOrphanName decides whether a discovered private name is owned:
// an intent journaling it owns it (its settleNames settles it). With no
// owner, a 'recover' intent is created — its own journal records the
// inherited name, so a later takeover can never reissue into a name it
// did not declare.
func (s *Store) attachOrphanName(ctx context.Context, scope string, view ReconView, dir, name string) {
	dctx, cancel := s.dbCtx(ctx)
	defer cancel()
	// An intent that journals this name owns it. Containment matches on
	// the name alone (dir is omitempty, so a dir-less record cannot be
	// matched by a dir key); the candidate rows' records are checked in
	// Go so a same-named record in a different directory does not claim
	// ownership.
	rows, qerr := s.pool.Query(dctx,
		`SELECT id, names FROM file_op
		 WHERE scope=$1 AND names @> $2::jsonb`,
		scope, mustJSON([]map[string]string{{"name": name}}))
	if qerr != nil {
		return
	}
	owned := false
	for rows.Next() {
		var oid int64
		var raw []byte
		if rows.Scan(&oid, &raw) != nil {
			continue
		}
		var recs []nameRec
		if json.Unmarshal(raw, &recs) != nil {
			continue
		}
		for _, r := range recs {
			if r.Name == name && r.Dir == dir {
				owned = true
			}
		}
	}
	rows.Close()
	if owned {
		return // owned — the intent's own executor settles it
	}
	// Orphan: create a recover intent. The journal records the inherited
	// name BEFORE the intent is visible to the executor — the same
	// declare-before-populate ordering as ordinary intents. No act is
	// recorded: the sweep issued no syscall, so an absent name simply
	// resolves done rather than tombstoning forever.
	var id, ver int64
	if err := s.pool.QueryRow(dctx, `SELECT nextval('file_version_seq')`).Scan(&ver); err != nil {
		return
	}
	rec := nameRec{Name: name, Dir: dir}
	err := s.pool.QueryRow(dctx,
		`INSERT INTO file_op (root, owner, scope, op, path, version, names)
		 VALUES ($1,$2,$3,'recover',$4,$5,$6) RETURNING id`,
		s.rootID, s.owner, scope, dir, ver, mustJSON([]nameRec{rec})).Scan(&id)
	if err != nil {
		log.Printf("reconcile: orphan %s/%s recover intent: %v", dir, name, err)
		return
	}
	log.Printf("reconcile: orphan private name %s/%s -> recover intent %d", dir, name, id)
}

func (s *Store) kickReconcile() {
	select {
	case s.reconcile <- struct{}{}:
	default:
	}
}

// WithWrite journals the intent, mutates the filesystem, then lets the
// settling goroutine apply the version row + event. Serialized per scope.
// expectSHA is the sha256 hex of the intended content (or "dir" for
// mkdir); the reconciler uses it to detect external bytes.
func (s *Store) WithWrite(ctx context.Context, scope, path, op string, iv IfVersion, expectSHA string, probe FPProbe, fn func(intent) (FileInfo, bool, error)) (int64, FileInfo, error) {
	mu := s.lockScope(scope)
	mu.Lock()
	defer mu.Unlock()

	it, err := s.declare(ctx, scope, op, path, "", iv, expectSHA, probe, probe)
	if err != nil {
		return 0, FileInfo{}, err
	}
	r := s.waitFs(s.runFs(it, fn))
	if r.err != nil {
		return 0, FileInfo{}, r.err
	}
	if r.settleErr != nil {
		// fs committed but the apply did not — the surviving intent lets
		// the reconciler converge; the client sees the error, not a
		// silent success.
		return 0, FileInfo{}, r.settleErr
	}
	return it.version, r.info, nil
}

// rename — renameat2(RENAME_NOREPLACE) for create-only modes), then moves
// the source subtree's rows to the destination in the apply tx.
func (s *Store) Rename(ctx context.Context, scope, from, to string, iv IfVersion, casProbe, fromProbe FPProbe, fn func(intent) (FileInfo, bool, error)) (int64, FileInfo, error) {
	mu := s.lockScope(scope)
	mu.Lock()
	defer mu.Unlock()

	it, err := s.declare(ctx, scope, "rename", from, to, iv, "", casProbe, fromProbe)
	if err != nil {
		return 0, FileInfo{}, err
	}
	r := s.waitFs(s.runFs(it, fn))
	if r.err != nil {
		return 0, FileInfo{}, r.err
	}
	if r.settleErr != nil {
		return 0, FileInfo{}, r.settleErr
	}
	return it.version, r.info, nil
}

// Remove drops the version rows for the removed path and any descendants
// after the fs removal, under the same intent journal.
func (s *Store) Remove(ctx context.Context, scope, path string, iv IfVersion, probe FPProbe, fn func(intent) (bool, error)) error {
	mu := s.lockScope(scope)
	mu.Lock()
	defer mu.Unlock()

	it, err := s.declare(ctx, scope, "remove", path, "", iv, "", probe, probe)
	if err != nil {
		return err
	}
	r := s.waitFs(s.runFs(it, func(it intent) (FileInfo, bool, error) { _, ferr := fn(it); return FileInfo{}, ferr == nil, ferr }))
	if r.err != nil {
		return r.err
	}
	return r.settleErr
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
// while its DB effects did not". Settlement authority is ownership-based
// (f79): an intent declared by THIS instance is settled by its fs
// goroutine, which keeps ownership until fn() returns — the reconciler
// never touches it. An intent declared by another owner is only judged
// after deadGrace: holding the writer lock proves that owner is dead, and
// the grace covers the residual window where a mutation already accepted
// by a FUSE daemon can complete shortly after process death. An intent of
// this instance that is no longer inflight had a failed settle — its fs
// call already returned, so the disk verdict is final.
//
// The disk verdict itself: present+matching expectation → roll forward;
// provably absent/unchanged → drop; present but NOT the expected content
// → roll forward with a diverged fingerprint so stat/CAS report
// external_change instead of laundering foreign bytes into this op's
// version (f83).

// ReconcileLoop runs the reconciler at startup, on demand (kickReconcile
// after a failed settle or writer re-acquire), and periodically.
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

// funcView adapts injected stat/hash probes to a ReconView — used by
// tests; production wires a descriptor-pinned view via SetReconcileView.
type funcView struct {
	stat StatFn
	hash HashFn
}

func (v funcView) Stat(scope, path string) (FileInfo, error) { return v.stat(scope, path) }
func (v funcView) Hash(scope, path string) (string, error)   { return v.hash(scope, path) }
func (v funcView) MoveStaged(scope, from, to string) error   { return ErrUnavailable }
func (v funcView) RemoveName(scope, path string) error       { return ErrUnavailable }
func (v funcView) ListStaged(scope, dir, prefix string) ([]string, error) {
	return nil, nil
}
func (v funcView) EnsureDir(scope, dir string) error { return ErrUnavailable }
func (v funcView) ListDir(scope, dir string) ([]string, error) {
	return nil, ErrUnavailable
}
func (v funcView) Close() error { return nil }

// Reconcile processes all pending intents once, then re-judges retained
// tombstones. Returns how many it settled. The pass judges only beneath a
// view acquired up front: for the production store this is a pinned root
// descriptor taken after the mount check, so a mid-pass unmount answers
// "unreachable" on the dead mount instead of "absent" on the bare
// directory it leaves behind (f102/F-RA-1). Intents that cannot be
// judged stay pending (or tombstoned) with their evidence intact.
func (s *Store) Reconcile(ctx context.Context) int {
	if s.deposed.Load() {
		return 0
	}
	var view ReconView
	if s.viewFn != nil {
		v, err := s.viewFn(ctx)
		if err != nil {
			log.Printf("reconcile: filesystem root not verifiable (%v) — intents stay pending", err)
			return 0
		}
		view = v
	} else {
		if s.statFn == nil {
			return 0
		}
		if s.fsCheck != nil {
			if err := s.fsCheck(); err != nil {
				log.Printf("reconcile: filesystem root not verifiable (%v) — intents stay pending", err)
				return 0
			}
		}
		view = funcView{s.statFn, s.hashFn}
	}
	defer view.Close()

	dctx, cancel := s.dbCtx(ctx)
	defer cancel()
	const cols = `id, owner, scope, op, path, to_path, version, pre_fp, dst_fp, expect_sha, src_kind, pre_oid, dst_oid, dst_sha, names, at`
	load := func(where string, args ...any) []intent {
		rows, err := s.pool.Query(dctx,
			`SELECT `+cols+` FROM file_op WHERE root=$1 AND `+where+` ORDER BY id`, args...)
		if err != nil {
			return nil
		}
		defer rows.Close()
		var out []intent
		for rows.Next() {
			var it intent
			var names []byte
			if err := rows.Scan(&it.id, &it.owner, &it.scope, &it.op, &it.path, &it.toPath,
				&it.version, &it.preFP, &it.dstFP, &it.expectSHA, &it.srcKind,
				&it.preOid, &it.dstOid, &it.dstSHA, &names, &it.at); err != nil {
				return out
			}
			if len(names) > 0 {
				json.Unmarshal(names, &it.names)
			}
			it.journal = &nameJournal{s: s, id: it.id}
			out = append(out, it)
		}
		return out
	}
	pending := load(`resolved_at IS NULL`, s.rootID)
	// Tombstone re-judgment is rate-limited in two tiers: scanning every
	// resolved intent every pass would pin the root descriptor (and the
	// mount) nearly 100% of the time. Hot tombstones (younger than
	// tombstoneColdAge, where late effects realistically arrive) are
	// re-judged every tombstoneScanInterval; cold ones still carry the
	// only evidence of a potentially delayed effect, so they are never
	// deleted — just re-judged on the slower tombstoneColdScanInterval.
	now := time.Now().UnixNano()
	scanHot := now-s.lastTombScan.Load() >= tombstoneScanInterval.Nanoseconds()
	scanCold := now-s.lastTombScanCold.Load() >= tombstoneColdScanInterval.Nanoseconds()
	var tombs []intent
	if scanHot {
		tombs = load(`resolved_at IS NOT NULL AND resolved_at > now() - $2::interval`,
			s.rootID, tombstoneColdAge.String())
	}
	if scanCold {
		tombs = append(tombs, load(`resolved_at IS NOT NULL AND resolved_at <= now() - $2::interval`,
			s.rootID, tombstoneColdAge.String())...)
	}
	scanTombs := len(tombs) > 0

	settled := 0
	for _, it := range pending {
		if s.deposed.Load() {
			break // fenced mid-pass: the writer lock was lost (f103)
		}
		if s.reconcileOne(ctx, it, view, false) {
			settled++
		}
	}
	if scanTombs {
		now = time.Now().UnixNano()
		if scanHot {
			s.lastTombScan.Store(now)
		}
		if scanCold {
			s.lastTombScanCold.Store(now)
		}
		for _, it := range tombs {
			if s.deposed.Load() {
				break
			}
			// Re-judgment: a tombstoned intent can only be APPLIED (its fs
			// effect provably landed late) — never re-dropped; its evidence
			// is retained for as long as the intent remains unproven.
			if s.reconcileOne(ctx, it, view, true) {
				settled++
			}
		}
	}
	// Orphan staging sweep on its own cadence: a staged deposit decided
	// while its intent lived can land after the intent row resolved — no
	// intent-keyed pass ever lists that namespace again. The sweep is the
	// discovery path for recorded objects parked under dead namespaces;
	// a single post-settlement pass is not enough because the deposit
	// itself may be arbitrarily delayed.
	if now2 := time.Now().UnixNano(); now2-s.lastStageSweep.Load() >= stageSweepInterval.Nanoseconds() {
		s.lastStageSweep.Store(now2)
		s.sweepOrphanNames(ctx, view)
		// Recover intents the sweep just created are owned by us and not
		// inflight — settle them in this pass rather than leaving orphan
		// content parked until the next sweep cadence.
		for _, it := range load(`resolved_at IS NULL`, s.rootID) {
			if s.deposed.Load() {
				break
			}
			if s.reconcileOne(ctx, it, view, false) {
				settled++
			}
		}
	}
	return settled
}

// absentVerdict reports whether a stat error is a definitive current-path
// absence: ENOENT, or ENOTDIR — a path addressed through a non-directory
// provably does not exist AS ADDRESSED (f119/B-2). This is a fact about
// the present, not proof the operation never ran — it settles effect, not
// history. Every other error means "could not observe" — unverifiable.
func absentVerdict(err error) bool {
	return errors.Is(err, ErrNotFound) || errors.Is(err, ErrNotDir)
}

// fpParts splits a live fingerprint "ino:size:mtime_ns:ctime_ns". The
// inode is the object's identity across a same-mount rename; size+mtime
// extend the identity to content for regular files (a rename updates
// ctime, and a dir's mtime moves with its '..' entry, so those legs are
// not compared for identity). Returns ok=false for the 2-field fallback
// fingerprint emitted when Stat_t is unavailable — without an inode the
// destination cannot be proven to be the moved source (f106).
func fpParts(fp string) (ino, size, mtime string, ok bool) {
	a := strings.Split(fp, ":")
	if len(a) != 4 {
		return "", "", "", false
	}
	return a[0], a[1], a[2], true
}

// reconcileOne settles one intent by disk verdict beneath the pass view.
// tombstoned=true re-judges a resolved intent: the ONLY possible outcome
// is a late apply (the fs effect provably landed after the drop) —
// tombstones are never re-dropped, stalled, or ghost-cleaned again.
//
// Error classification (f119/B-2): ErrNotFound and ErrNotDir are
// definitive CURRENT-path absence (a path under a non-directory cannot
// exist as addressed); every other error means the filesystem could not
// be observed — the intent stays pending, marked stalled with the cause,
// and is never resolved on a timer.
func (s *Store) reconcileOne(ctx context.Context, it intent, view ReconView, tombstoned bool) bool {
	if !tombstoned {
		if it.owner == s.owner {
			if _, ok := s.inflight.Load(it.id); ok {
				return false // its fs goroutine still owns settlement
			}
			// Own intent whose settle failed — fn already returned, the
			// disk verdict is final.
		} else {
			// Dead owner's intent. Holding the writer lock proves that
			// owner cannot start new work; deadGrace covers mutations a
			// FUSE daemon may still be completing on its behalf.
			if time.Since(it.at) < deadGrace {
				return false
			}
		}
	}
	mu := s.lockScope(it.scope)
	mu.Lock()
	defer mu.Unlock()

	// Settle the intent's journaled private names before judging the
	// public paths — a process that died mid-effect can leave displaced
	// or staged content parked under owned names, and the judgment must
	// see the settled shape, not the transient one.
	s.settleNames(ctx, it, view, tombstoned)

	switch it.op {
	case "recover":
		// A sweep-created intent whose whole purpose was settling an
		// orphan private name — done above; resolve it.
		if tombstoned {
			return false
		}
		return s.tombstoneIntent(ctx, it)
	case "rename":
		toInfo, terr := view.Stat(it.scope, it.toPath)
		frInfo, ferr := view.Stat(it.scope, it.path)
		if ferr != nil && !absentVerdict(ferr) {
			if !tombstoned {
				s.markStalled(ctx, it, ferr)
			}
			return false
		}
		if terr != nil && !absentVerdict(terr) {
			if !tombstoned {
				s.markStalled(ctx, it, terr)
			}
			return false
		}
		switch {
		case ferr == nil && frInfo.Fingerprint == it.preFP:
			// Source byte-identical to declare — the declared effect is
			// absent; the rows still describe what is at their paths.
			if tombstoned {
				return false
			}
			return s.tombstoneIntent(ctx, it)
		case terr == nil:
			// Something occupies the destination. The reconciler does not
			// decide whether "the rename happened": it relocates each
			// recorded row whose object is observed at the corresponding
			// destination path and absent at its recorded path (current
			// state), journals those observations, and resolves the
			// intent. Whatever is not evidenced stays where it is and is
			// reported through the ordinary live-fingerprint comparison.
			n, rerr := s.relocateRows(ctx, it, toInfo, view, absentVerdict(ferr))
			if rerr != nil {
				if !tombstoned {
					s.markStalled(ctx, it, rerr)
				}
				return false
			}
			if tombstoned {
				return n > 0
			}
			return s.tombstoneIntent(ctx, it)
		case absentVerdict(ferr):
			// Both legs absent — the subtree is gone either way.
			if tombstoned {
				return false
			}
			return s.tombstoneIntentGhosts(ctx, it, true)
		default:
			// Source present but changed, destination absent: nothing is
			// observed anywhere else; rows stay, stat reports the change.
			if tombstoned {
				return false
			}
			return s.tombstoneIntent(ctx, it)
		}
	default: // write, mkdir, remove
		info, serr := view.Stat(it.scope, it.path)
		switch {
		case absentVerdict(serr):
			if tombstoned {
				return false // still absent; tombstone stays for the horizon
			}
			if it.op == "remove" {
				// Desired absence holds — clean dead rows, journal no
				// event: observed absence is not a performed removal.
				return s.tombstoneIntentGhosts(ctx, it, true)
			}
			// Absent + settled owner: the write/mkdir never landed. Any
			// row for the path is a ghost — clean it; keep the intent's
			// evidence for late-landing re-judgment.
			return s.tombstoneIntentGhosts(ctx, it, false)
		case serr != nil:
			// Unverifiable — could not observe. Never resolved by timer.
			if !tombstoned {
				s.markStalled(ctx, it, serr)
			}
			return false
		}
		if it.op == "remove" {
			// Path still exists — the removal never committed.
			if tombstoned {
				return false
			}
			return s.tombstoneIntent(ctx, it)
		}
		if it.preFP != "" && info.Fingerprint == it.preFP {
			// Byte-identical fingerprint since declare — the mutation
			// never ran (a landed write always mints a new inode/fp).
			if tombstoned {
				return false
			}
			return s.tombstoneIntent(ctx, it)
		}
		contentSHA := ""
		if it.op == "mkdir" {
			if info.Kind != "dir" {
				// A non-dir at the path means the mkdir never ran.
				if tombstoned {
					return false
				}
				return s.tombstoneIntent(ctx, it)
			}
			contentSHA = "dir"
		} else if it.expectSHA != "" {
			// Write: verify the landed bytes are the intended ones.
			sha, herr := view.Hash(it.scope, it.path)
			if herr != nil {
				if !tombstoned {
					s.markStalled(ctx, it, herr)
				}
				return false
			}
			contentSHA = sha
			if sha != it.expectSHA {
				// The content at the path is NOT what this op wrote —
				// either our write landed and was then edited, or the
				// write never ran and something else created the file.
				// Record the version with a diverged fingerprint so
				// stat/CAS report external_change (f83) — but KEEP the
				// intent tombstoned: this op's bytes may still land late
				// and overwrite the foreign content, and the intent is
				// the only evidence that can re-attribute them.
				info.Fingerprint = divergedFP(it.expectSHA)
				return s.applyKeep(ctx, it, info, contentSHA, tombstoned)
			}
		}
		if err := s.apply(ctx, it, info, contentSHA, false); err != nil {
			log.Printf("reconcile: apply %s %s/%s: %v", it.op, it.scope, it.path, err)
			return false
		}
		// The apply journaled this intent's commit and removed its row in
		// one tx — the evidence intentApplied requires. Finish draining
		// the journaled private names now: with the row gone, the names
		// journal is the only enumeration of this intent's slots — its
		// records are patched in place (the row is gone, so the journal
		// updates are no-ops; the name was already settled above).
		return true
	}
}

// applyKeep records the observed (diverged) outcome AND retains the
// intent as a tombstone — the declared write may still be in flight.
// Re-judging a tombstone whose observation is already recorded, or whose
// path a newer version superseded, writes nothing (apply's in-tx row
// guard): no stale event, no resolved_at refresh. Returns whether this
// call settled anything.
func (s *Store) applyKeep(ctx context.Context, it intent, info FileInfo, contentSHA string, tombstoned bool) bool {
	err := s.apply(ctx, it, info, contentSHA, true)
	switch {
	case err == nil:
		log.Printf("reconcile: %s %s/%s landed divergent content — marking external", it.op, it.scope, it.path)
		return true
	case errors.Is(err, errNotJournaled):
		return !tombstoned // a pending intent was resolved without a new record
	case !errors.Is(err, errIntentSettled):
		log.Printf("reconcile: apply %s %s/%s: %v", it.op, it.scope, it.path, err)
	}
	return false
}

// relocateRows re-keys version rows from a rename intent's source subtree
// to its destination subtree on PER-ROW evidence about the CURRENT state
// — it never decides whether the rename "happened":
//
//   - a member row (below the source path) moves when its recorded path
//     is definitively absent and the object at the corresponding
//     destination path carries the row's exact recorded fingerprint
//     (ino:size:mtime:ctime). A member of a moved directory keeps all
//     four; copy/restore, link, chmod, or an individual mv change ctime
//     (verified on ext4 and JuiceFS), so the match identifies the same
//     inode in the same recorded state.
//   - the intent's own source object changes its ctime by being moved,
//     so an exact match is impossible for it. A regular file is relocated
//     only when its recorded path is absent, ino/size/mtime equal the
//     declare-time fingerprint and the destination bytes hash to the
//     recorded expect_sha (the row fp is refreshed to the live one — the
//     bytes are proven). A directory's own row is never relocated: an
//     inode number is not identity on ext4 and nothing else survives the
//     move.
//
// Rows keep their versions (no version is minted) and each relocation is
// journaled as a `relocate` event (path=new, from=old) — an observation,
// not a claim that the service performed a rename. Rows without evidence
// stay put and surface through the live-fingerprint comparison (404 at
// the recorded path / version 0 at the destination) like any external
// change. There is no intent-age version cutoff: rows minted after the
// intent are also eligible, provided their decision-time versions still
// match when locked below. This subsumes the earlier "rescue".
//
// srcAbsent short-circuits the per-row source stat when the whole source
// path is already known absent (every descendant path is then absent as
// addressed). A non-nil error means some member could not be observed —
// retry next pass, no verdict.
//
// The evidence above is gathered BEFORE the transaction; a same-scope
// settler can commit between observation and tx start (the scope mutex
// does not cover an in-flight fs goroutine's apply), or while a FOR
// UPDATE inside the tx waits. Every move is therefore serialized on
// row locks and fully revalidated before it writes:
//
//   - the destination row is locked FIRST; the locked (version, fp)
//     must equal the decision-time read — a row that appeared, changed,
//     or disappeared since belongs to a different committed decision
//     and is never deleted;
//   - the destination object is re-stat'd AFTER that lock, so a commit
//     landing during the lock wait is judged against the bytes it
//     actually left;
//   - the source row is locked and must still hold the observed
//     version+fp;
//   - only then do the DELETE/UPDATE/event run. A skipped move writes
//     nothing: no row is mutated and no event is published for it.
//
// The recordersActive screen ahead of the tx also defers judgment while
// a same-scope settler may still be committing rows for in-flight
// evidence — a skipped pass re-judges next sweep.
func (s *Store) relocateRows(ctx context.Context, it intent, toInfo FileInfo, view ReconView, srcAbsent bool) (int, error) {
	dctx, cancel := s.dbCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(dctx,
		`SELECT path, fp, version FROM file_version
		 WHERE scope=$1 AND (path=$2 OR starts_with(path, $2||'/'))
		 ORDER BY path`,
		it.scope, it.path)
	if err != nil {
		return 0, err
	}
	type member struct {
		p, fp string
		ver   int64
	}
	var members []member
	for rows.Next() {
		var m member
		if rows.Scan(&m.p, &m.fp, &m.ver) == nil {
			members = append(members, m)
		}
	}
	rows.Close()

	// liveFP is the destination object's full fingerprint as observed
	// during evidence; the in-tx recheck requires the SAME fingerprint —
	// any change means a different object owns the name now. dstExisted/
	// dstVer/dstFP capture the destination ROW the decision observed (or
	// its absence); the locked row must match exactly before the move may
	// write — a row that appeared, changed, or disappeared since belongs
	// to a different committed decision.
	type move struct {
		from, to, fp, sha, liveFP string
		srcVer                    int64
		srcRowFP                  string
		dstExisted                bool
		dstVer                    int64
		dstFP                     string
	}
	// dstRow reads the destination row's identity at decision time —
	// the baseline the in-tx locked row is compared against.
	dstRow := func(path string) (bool, int64, string, error) {
		var v int64
		var f string
		err := s.pool.QueryRow(dctx,
			`SELECT version, fp FROM file_version WHERE scope=$1 AND path=$2`,
			it.scope, path).Scan(&v, &f)
		if errors.Is(err, pgx.ErrNoRows) {
			return false, 0, "", nil
		}
		if err != nil {
			return false, 0, "", err
		}
		return true, v, f, nil
	}
	var moves []move
	for _, m := range members {
		dest := it.toPath + m.p[len(it.path):]
		if !srcAbsent {
			_, serr := view.Stat(it.scope, m.p)
			if serr == nil {
				continue // still reachable at its recorded path
			}
			if !absentVerdict(serr) {
				return 0, serr
			}
		}
		if m.p == it.path {
			// The moved object itself: files by ino/size/mtime + bytes.
			if it.srcKind != "file" || toInfo.Kind != "file" || it.expectSHA == "" {
				continue
			}
			srcIno, srcSize, srcMt, okS := fpParts(it.preFP)
			toIno, toSize, toMt, okT := fpParts(toInfo.Fingerprint)
			if !okS || !okT || srcIno != toIno || srcSize != toSize || srcMt != toMt {
				continue
			}
			h, herr := view.Hash(it.scope, dest)
			if herr != nil {
				if absentVerdict(herr) {
					continue
				}
				return 0, herr
			}
			if h != it.expectSHA {
				continue
			}
			ex, dv, df, derr := dstRow(dest)
			if derr != nil {
				return 0, derr
			}
			moves = append(moves, move{from: m.p, to: dest, fp: toInfo.Fingerprint,
				sha: it.expectSHA, liveFP: toInfo.Fingerprint, srcVer: m.ver, srcRowFP: m.fp,
				dstExisted: ex, dstVer: dv, dstFP: df})
			continue
		}
		if m.fp == "" || strings.HasPrefix(m.fp, "diverged:") {
			continue // nothing exact to match
		}
		st, serr := view.Stat(it.scope, dest)
		if serr != nil {
			if absentVerdict(serr) {
				continue
			}
			return 0, serr
		}
		if st.Fingerprint != m.fp {
			continue
		}
		ex, dv, df, derr := dstRow(dest)
		if derr != nil {
			return 0, derr
		}
		moves = append(moves, move{from: m.p, to: dest, fp: m.fp,
			liveFP: st.Fingerprint, srcVer: m.ver, srcRowFP: m.fp,
			dstExisted: ex, dstVer: dv, dstFP: df})
	}
	if len(moves) == 0 {
		return 0, nil
	}
	cancel()
	// A same-scope settler mid-apply can still commit rows that the
	// evidence just read — defer rather than act on a half-visible state.
	if busy, berr := s.recordersActive(ctx, it.scope); berr != nil {
		return 0, berr
	} else if busy {
		return 0, nil
	}

	tctx, tcancel := s.dbCtx(ctx)
	defer tcancel()
	tx, err := s.pool.BeginTx(tctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(tctx)
	if err := s.checkOwnerTx(tctx, tx); err != nil {
		return 0, err
	}
	moved := 0
	for _, mv := range moves {
		// Every judgment used pre-transaction evidence; a same-scope
		// settler can commit between it and any point in this tx. The
		// destination row lock is the serialization point: taken FIRST,
		// it freezes the row so every comparison below — against
		// decision-time row evidence and against the post-lock live
		// object — describes one consistent instant. Only after BOTH
		// rows are validated under their locks may this move write: a
		// skipped relocation must not mutate either row or publish an
		// event (a dest DELETE staged before a source check would still
		// commit with the tx).
		var dver int64
		var dfp string
		dExists := true
		derr := tx.QueryRow(tctx,
			`SELECT version, fp FROM file_version WHERE scope=$1 AND path=$2 FOR UPDATE`,
			it.scope, mv.to).Scan(&dver, &dfp)
		switch {
		case errors.Is(derr, pgx.ErrNoRows):
			dExists = false
		case derr != nil:
			return 0, derr
		}
		if dExists != mv.dstExisted || (dExists && (dver != mv.dstVer || dfp != mv.dstFP)) {
			// The destination row appeared, changed, or disappeared
			// between the decision and its lock — it belongs to a
			// different committed decision and is never touched by
			// this stale evidence.
			continue
		}
		// Re-observe the destination object AFTER the row lock, not
		// before: a commit that lands while the FOR UPDATE waits is
		// judged against the bytes it actually left, so new
		// acknowledged content can never be misclassified as the
		// displaced stale object the evidence saw.
		live, lerr := view.Stat(it.scope, mv.to)
		if lerr != nil {
			if absentVerdict(lerr) {
				continue // object gone since evidence — re-judge next pass
			}
			return 0, lerr
		}
		if live.Fingerprint != mv.liveFP {
			continue // a different object owns the name now
		}
		if dExists && fp3(dfp) == fp3(live.Fingerprint) {
			// The row already records the object actually present —
			// deleting it would orphan a correct record.
			continue
		}
		var sver int64
		var sfp string
		serr := tx.QueryRow(tctx,
			`SELECT version, fp FROM file_version WHERE scope=$1 AND path=$2 FOR UPDATE`,
			it.scope, mv.from).Scan(&sver, &sfp)
		switch {
		case errors.Is(serr, pgx.ErrNoRows):
			continue // source row gone — drifted
		case serr != nil:
			return 0, serr
		case sver != mv.srcVer || sfp != mv.srcRowFP:
			continue // source row drifted — no longer the observed member
		}
		// All evidence revalidated under locks — only now may the move
		// write. The locked dest row (if any) records an object provably
		// absent from the path: the displaced one the relocated object
		// replaces. Its delete is keyed to the evidence identity as a
		// final assertion of the comparison already made under the lock.
		if dExists {
			dtag, err := tx.Exec(tctx,
				`DELETE FROM file_version WHERE scope=$1 AND path=$2 AND version=$3 AND fp=$4`,
				it.scope, mv.to, mv.dstVer, mv.dstFP)
			if err != nil {
				return 0, err
			}
			if dtag.RowsAffected() != 1 {
				return 0, fmt.Errorf("relocate: destination row %s/%s changed under lock", it.scope, mv.to)
			}
		}
		tag, err := tx.Exec(tctx,
			`UPDATE file_version SET path=$3, fp=$4,
			        content_sha = CASE WHEN $5 <> '' THEN $5 ELSE content_sha END,
			        updated = now()
			 WHERE scope=$1 AND path=$2 AND version=$6 AND fp=$7`,
			it.scope, mv.from, mv.to, mv.fp, mv.sha, mv.srcVer, mv.srcRowFP)
		if err != nil {
			return 0, err
		}
		if tag.RowsAffected() != 1 {
			// The source row is locked and was just verified — zero
			// rows means a logic violation, not drift. Roll back the
			// whole pass rather than let the dest delete outlive a
			// skipped re-key.
			return 0, fmt.Errorf("relocate: source row %s/%s changed under lock", it.scope, mv.from)
		}
		if _, err := tx.Exec(tctx,
			`INSERT INTO file_event (scope, path, from_path, op, version)
			 SELECT scope, path, $2, 'relocate', version FROM file_version WHERE scope=$1 AND path=$3`,
			it.scope, mv.from, mv.to); err != nil {
			return 0, err
		}
		moved++
	}
	if err := tx.Commit(tctx); err != nil {
		return 0, err
	}
	if moved > 0 {
		log.Printf("reconcile: rename %s/%s -> %s: relocated %d recorded object(s) by fingerprint evidence; no rename claimed",
			it.scope, it.path, it.toPath, moved)
	}
	return moved, nil
}
