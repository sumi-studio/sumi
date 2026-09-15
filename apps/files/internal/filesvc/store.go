package filesvc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
type ReconView interface {
	Stat(scope, path string) (FileInfo, error)
	Hash(scope, path string) (string, error)
	// MoveStaged restores a recovery object to an empty name beneath the
	// same pinned root (renameat2 NOREPLACE — never overwrites).
	MoveStaged(scope, from, to string) error
	// SwapStaged exchanges a recovery object with whatever the name holds
	// (renameat2 RENAME_EXCHANGE) — the second half of a dead op's undo:
	// it puts the displaced object back and parks whatever it evicts at
	// the staging slot for inspection, never unlinking it.
	SwapStaged(scope, staged, name string) error
	// RemoveStaged deletes a recovery object only after re-proving its
	// identity at a name no retired actor can write: it first moves path
	// to a unique quarantine name (path + "-q-" + nonce), re-verifies
	// the captured object against wantFP3 (ino:size:mtime triple) or
	// wantSHA (content hash), and unlinks only on a match. A verified
	// object at path may still be exchanged out by a live retired actor
	// between the caller's check and this call — the move captures
	// whatever is actually there, and a mismatch stays parked at the
	// quarantine name (preserved bytes, re-judged each pass). A path
	// already carrying "-q-" is itself unforgeable, so it is verified
	// in place without another hop. wantFP3 takes precedence; pass "" to
	// use content hash. ENOENT at path reports nil — already gone is the
	// desired end state.
	RemoveStaged(scope, path, wantFP3, wantSHA string) error
	// RemoveStagedVeto is RemoveStaged with a veto evaluated after the
	// object is captured at the sealed name — the last moment a check
	// can still bind to the object about to be unlinked. The veto
	// receives the CAPTURED object's FileInfo (a hash-only match can
	// capture a different inode than the caller statted). While
	// captured, the object cannot be observed at any public path: a row
	// can still come to record it only by committing an observation made
	// before the capture, which the veto must exclude (Store's
	// recordersActive) before consulting the version rows. keep=true parks the
	// object at the sealed name (enumerable for the next pass) and
	// reports ErrConflict; a veto error does the same with that error.
	// A nil veto behaves exactly as RemoveStaged.
	RemoveStagedVeto(scope, path, wantFP3, wantSHA string, veto func(captured FileInfo) (bool, error)) error
	// ListStaged returns base names in dir (a path beneath the scope root)
	// beginning with prefix — used to find crash-orphaned quarantine
	// objects left by a reconciler that died mid-delete.
	ListStaged(scope, dir, prefix string) ([]string, error)
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
		ALTER TABLE file_version ADD COLUMN IF NOT EXISTS birth text NOT NULL DEFAULT '';
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
		-- recovery_place journals which object identity a recovery pass
		-- last placed at a public path. It is the evidence that lets a
		-- later sweep repair a recovery-caused misplacement WITHOUT
		-- mistaking an ordinary external rename for damage: only an
		-- object the service itself put at a public name may be moved
		-- back off it.
		CREATE TABLE IF NOT EXISTS recovery_place (
			scope text NOT NULL,
			path  text NOT NULL,
			fp    text NOT NULL,
			birth text NOT NULL DEFAULT '',
			at    timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (scope, path)
		);
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
	op        string // write|mkdir|remove|rename
	path      string // target (rename: source)
	toPath    string // rename destination
	version   int64  // pre-minted version this op will record
	preFP     string // fingerprint of path (rename: of source) at declare time
	dstFP     string // fingerprint of the object the effect may displace (write/remove: path; rename: destination) — the verified effect undoes rather than destroy anything else
	expectSHA string // write: sha256 hex of intended bytes; rename of file: sha256 of source; mkdir: "dir"; "": unverified
	srcKind   string // rename: kind of the source at declare ("" = unknown → unprovable)
	at        time.Time
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
// intent: the declared payload for content ops, empty for mkdir.
func intentSHA(it intent) string {
	if it.op == "mkdir" {
		return "" // "dir" is a kind marker, not a content hash
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
		}
	}
	// dstFP is the object a committed effect is allowed to displace —
	// recorded at declare so a verified effect that lands late (after
	// ownership moved and a successor wrote newer content) undoes itself
	// rather than destroy what it never agreed to replace.
	if op == "rename" {
		if casProbe != nil {
			dst, exists, derr := casProbe()
			if derr != nil {
				return intent{}, derr
			}
			if exists {
				it.dstFP = dst.Fingerprint
			}
		}
	} else {
		it.dstFP = it.preFP
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
		`INSERT INTO file_op (root, owner, scope, op, path, to_path, version, pre_fp, dst_fp, expect_sha, src_kind)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id`,
		s.rootID, s.owner, scope, op, path, toPath, it.version, it.preFP, it.dstFP, it.expectSHA, it.srcKind).Scan(&it.id)
	if err != nil {
		return intent{}, err
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
	return it, nil
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
			`INSERT INTO file_version (scope, path, version, fp, updated, content_sha, birth)
			 VALUES ($1,$2,$3,$4,now(),$5,$6)
			 ON CONFLICT (scope,path) DO UPDATE
			 SET version=EXCLUDED.version, fp=EXCLUDED.fp, updated=now(),
			     content_sha=EXCLUDED.content_sha, birth=EXCLUDED.birth
			 WHERE file_version.version < EXCLUDED.version`,
			it.scope, it.toPath, it.version, info.Fingerprint, contentSHA, info.Birth); err != nil {
			return err
		}
	default: // write, mkdir
		// Roll-forward must never regress a committed row: if a later
		// write already applied a newer version (e.g. this intent sat
		// pending while a subsequent op committed), the existing row
		// reflects newer disk state and wins.
		tag, uerr := tx.Exec(ctx,
			`INSERT INTO file_version (scope, path, version, fp, updated, content_sha, birth)
			 VALUES ($1,$2,$3,$4,now(),$5,$6)
			 ON CONFLICT (scope,path) DO UPDATE
			 SET version=EXCLUDED.version, fp=EXCLUDED.fp, updated=now(),
			     content_sha=EXCLUDED.content_sha, birth=EXCLUDED.birth
			 WHERE file_version.version < EXCLUDED.version`,
			it.scope, it.path, it.version, info.Fingerprint, contentSHA, info.Birth)
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

// stageRel is the deterministic recovery object slot for an intent —
// where a verified effect's staged content or a parked displaced object
// can be found. It lives in the op's parent directory (rename: the
// source's parent, where a parked displaced destination lands).
func stageRel(it intent) string {
	parent := it.path
	if i := strings.LastIndex(parent, "/"); i >= 0 {
		parent = parent[:i]
	} else {
		parent = ""
	}
	name := opStagePrefix + strconv.FormatInt(it.id, 10)
	if parent == "" {
		return name
	}
	return parent + "/" + name
}

// recClaim is the outcome of resolving which version row records a
// parked object.
type recClaim int

const (
	recNone      recClaim = iota // no row claims this object
	recFound                     // one defensible home
	recAmbiguous                 // multiple claims evidence cannot separate — preserve
)

// recordedAtObject routes a parked object to the home a version row
// records for it. Files match on fp3 — ino:size:mtime is the content
// generation a file row acknowledges. Directories match on the inode
// leg: a dir's size and mtime churn with every member create or
// unlink, so the fp3 recorded at commit diverges from the same live
// dir; the inode is the directory's object identity.
//
// Inode claims are not unique across objects forever: ext4 recycles a
// freed inode and a mounted filesystem brings its own inode space, so
// several rows may claim one live inode — a ghost row for a dead
// object beside the true row. When more than one row matches, rank the
// evidence instead of picking a lexical winner: an exact-fingerprint
// row recorded this object's current generation; an fp3-equal row
// recorded a generation with no membership churn; a home that still
// holds this very object (dev:ino) is already satisfied; and a member
// whose own row resolves beneath a candidate corroborates that
// candidate as the container's home. What remains genuinely
// unresolvable is recAmbiguous — callers preserve rather than install
// content at a home no evidence supports.
func (s *Store) recordedAtObject(ctx context.Context, scope, rel string, st FileInfo, view ReconView) (string, recClaim, error) {
	return s.recordedAtObjectSeen(ctx, scope, rel, st, view, map[string]struct{}{})
}

func (s *Store) recordedAtObjectSeen(ctx context.Context, scope, rel string, st FileInfo, view ReconView, seen map[string]struct{}) (string, recClaim, error) {
	if s.pool == nil {
		return "", recNone, nil
	}
	// Claim collection differs by kind. A file row's match key is fp3 —
	// ino:size:mtime, the content generation it acknowledges — and a
	// file object may never be claimed by a 'dir' row. A directory's
	// fp3 churns with membership, so its claim key is the inode leg,
	// and a real-sha file row may never claim a dir object.
	var rows pgx.Rows
	var qerr error
	dctx, cancel := s.dbCtx(ctx)
	if st.Kind == "dir" {
		ino, _, _, ok := fpParts(st.Fingerprint)
		if !ok {
			cancel()
			return "", recNone, nil // unidentifiable — preserve
		}
		rows, qerr = s.pool.Query(dctx,
			`SELECT path, fp, birth FROM file_version
			  WHERE scope=$1 AND fp LIKE $2 || ':%:%:%'
			    AND (content_sha='dir' OR content_sha='')
			  ORDER BY path`, scope, ino)
	} else {
		rows, qerr = s.pool.Query(dctx,
			`SELECT path, fp, birth FROM file_version
			  WHERE scope=$1 AND (fp = $2 OR fp LIKE $2 || ':%')
			    AND content_sha <> 'dir'
			  ORDER BY path`, scope, fp3(st.Fingerprint))
	}
	if qerr != nil {
		cancel()
		return "", recNone, qerr
	}
	claims, cerr := collectClaims(rows)
	cancel()
	if cerr != nil {
		// An interrupted or partial read must not be treated as the
		// complete claim set — fail closed.
		return "", recNone, cerr
	}
	// A birth contradiction is positive evidence AGAINST a claim: when
	// both the row and the live object carry btime, a mismatch proves
	// the row recorded a different object — it cannot then re-enter
	// through a weaker leg. Missing birth on either side is no
	// evidence at all, never a contradiction.
	kept := claims[:0]
	for _, c := range claims {
		if st.Birth != "" && c.birth != "" && c.birth != st.Birth {
			continue
		}
		kept = append(kept, c)
	}
	claims = kept
	if len(claims) == 0 {
		return "", recNone, nil
	}
	// Evidence tiers, strongest first. ino+birth is durable object
	// identity where the filesystem reports btime. Exact fingerprint
	// and fp3 are generation-level evidence; the bare inode leg is the
	// weakest — needed for pre-birth rows and btime-less filesystems
	// but never sufficient alone for an uncorroborated dir claim.
	var born, exact, triple, rest []string
	st3 := fp3(st.Fingerprint)
	for _, c := range claims {
		switch {
		case st.Birth != "" && c.birth != "" && c.birth == st.Birth:
			born = append(born, c.path)
		case c.fp == st.Fingerprint:
			exact = append(exact, c.path)
		case fp3(c.fp) == st3:
			triple = append(triple, c.path)
		default:
			rest = append(rest, c.path)
		}
	}
	var cands []string
	strong := true // candidates carry more than a bare inode leg
	switch {
	case len(born) > 0:
		cands = born
	case len(exact) > 0:
		cands = exact
	case len(triple) > 0:
		cands = triple
	default:
		cands = rest
		strong = false
	}
	// Presence inside the strongest tier: a candidate path that already
	// holds THIS object (dev:ino) is satisfied — the parked name is a
	// surplus link. Presence never preempts ranking itself: a weaker
	// claimant that happens to hold the object cannot outrank a
	// stronger claim elsewhere.
	if view != nil {
		for _, p := range cands {
			if dst, err := view.Stat(scope, p); err == nil && sameObjectAt(st, dst) {
				return p, recFound, nil
			}
		}
	}
	if len(cands) == 1 {
		if strong {
			return cands[0], recFound, nil
		}
		// A sole inode-leg claimant is a guess, not authority: an
		// externally deleted object's row keeps its fp forever, and
		// inode reuse (ext4 recycles ino; a mount brings its own space)
		// hands that leg to an unrelated object. Require the
		// container's own membership to corroborate the claimant — a
		// recorded member whose row resolves beneath it — else the
		// claim is ambiguous and the object is preserved.
		if view != nil && s.dirMemberCorroborates(ctx, view, scope, rel, cands[0], seen) {
			return cands[0], recFound, nil
		}
		return "", recAmbiguous, nil
	}
	// Multiple strongest-tier claimants. Member corroboration may elect
	// only a candidate whose path is open — verified absent. A
	// candidate that is occupied OR unverifiable (EIO/EACCES/timeout —
	// a stat error is not absence) stays eligible only when NO open
	// candidate exists: member inference can bind a container to a
	// home, but evicting whatever may sit at an unverified path needs
	// object-level evidence, which parkedOwnsRow ranks once the home
	// is chosen — and which cannot even run when the destination
	// cannot be observed.
	if view != nil {
		var open, blocked []string
		for _, p := range cands {
			if _, err := view.Stat(scope, p); absentVerdict(err) {
				open = append(open, p) // verified absent — installable
			} else {
				blocked = append(blocked, p) // occupied or unverifiable
			}
		}
		pool := open
		if len(pool) == 0 {
			pool = blocked
		}
		var corroborated []string
		for _, p := range pool {
			if s.dirMemberCorroborates(ctx, view, scope, rel, p, seen) {
				corroborated = append(corroborated, p)
			}
		}
		if len(corroborated) == 1 {
			return corroborated[0], recFound, nil
		}
	}
	return "", recAmbiguous, nil
}

// dirClaim is one inode-leg claimant row. birth is the persisted
// creation-time identity ("" for rows written before the column or on
// filesystems without btime).
type dirClaim struct{ path, fp, birth string }

// collectClaims drains a claimant query completely. A Scan failure or
// a rows error after partial iteration is returned, not swallowed: an
// incomplete claim set must never masquerade as authority — callers
// treat the error as unverifiable and preserve the object.
func collectClaims(rows pgx.Rows) ([]dirClaim, error) {
	defer rows.Close()
	var out []dirClaim
	for rows.Next() {
		var c dirClaim
		if err := rows.Scan(&c.path, &c.fp, &c.birth); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// dirMemberCorroborates reports whether a member of the parked dir at
// rel has a version row resolving directly beneath home — membership
// evidence that this container is the directory the home's row
// recorded. Each member is judged by its own stat and row, so a
// container swapped at rel mid-pass only yields whichever members the
// current object holds, and corroboration can never authorize
// anything on its own beyond choosing among claimant rows.
// seen bounds the corroboration recursion: a dir member's own claim
// evaluation may itself need corroboration, which walks ITS members.
// Without a visited set a cyclic structure (a bind-mounted alias, a
// dir reachable inside itself) would recurse forever — each container
// is expanded once per top-level judgment, keyed by dev:ino or path
// when the object cannot be identified.
func (s *Store) dirMemberCorroborates(ctx context.Context, view ReconView, scope, rel, home string, seen map[string]struct{}) bool {
	members, lerr := view.ListStaged(scope, rel, "")
	if lerr != nil {
		return false
	}
	for _, m := range members {
		if strings.HasPrefix(m, opStagePrefix) || strings.HasPrefix(m, stagingPrefix) {
			continue
		}
		mrel := rel + "/" + m
		mst, merr := view.Stat(scope, mrel)
		if merr != nil {
			continue
		}
		if mst.Kind == "dir" {
			if !markSeen(seen, mst, mrel) {
				continue // already expanded — cycle or second alias
			}
		}
		// Kind-aware: a dir member's own claim runs the dir-claim path
		// (inode leg + birth), a file member's the fp3 path.
		mh, mclaim, merr := s.recordedAtObjectSeen(ctx, scope, mrel, mst, view, seen)
		if merr != nil || mclaim != recFound {
			continue
		}
		if md, _ := splitRel(mh); md == home {
			return true
		}
	}
	return false
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

// discardVetoHook is a test-only seam between the discard veto's
// recordersActive read and its row read — the position an apply
// committing between the two reads lands in. Production never sets it.
var discardVetoHook func()

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

// recordedFP returns the fingerprint the path's version row records;
// found=false when no row exists. A non-nil error means unverifiable.
func (s *Store) recordedFP(ctx context.Context, scope, path string) (fp string, found bool, err error) {
	if s.pool == nil {
		return "", false, nil
	}
	dctx, cancel := s.dbCtx(ctx)
	defer cancel()
	err = s.pool.QueryRow(dctx,
		`SELECT fp FROM file_version WHERE scope=$1 AND path=$2`,
		scope, path).Scan(&fp)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return fp, true, nil
}

// settleDelete judges whether the staged object at rel may be
// discarded. Two object classes reach here: a declared displacement
// (wantFP3 = fp3 of the object the intent was authorized to destroy —
// authorized only if its effect committed, needCommit=true) and the
// intent's own staged bytes (wantSHA = its expected hash, safe to drop
// whenever unrecorded, needCommit=false).
//
// The rule: never delete an object that a version row records — or may
// still come to record — unless that object is still present at its
// recorded home (a surplus link then loses nothing); a displaced-object
// delete additionally requires intentApplied. The checks that authorize
// the unlink run in the veto, after the object is captured at a sealed
// name. From then on no public path shows it, so the only rows that can
// still come to record it are commits of observations made before the
// capture; recordersActive excludes those writers and is read before the
// row set. The veto judges the CAPTURED object's identity — a hash-only
// match need not be the inode this pass statted. DB uncertainty, active
// recorders and absent commit evidence fail closed: the object stays
// parked at the sealed name, enumerable for the next pass.
func (s *Store) settleDelete(ctx context.Context, it intent, view ReconView,
	rel string, st FileInfo, tombstoned bool, wantFP3, wantSHA string, needCommit bool) {
	// Decision-time screen: still recorded and absent from its recorded
	// home means this object is recorded content parked here by a
	// delayed effect — restore it, never delete.
	home, claim, derr := s.recordedAtObject(ctx, it.scope, rel, st, view)
	if derr != nil || claim == recAmbiguous {
		return // row set unverifiable or ambiguous — preserve
	}
	if claim == recFound {
		dst, serr := view.Stat(it.scope, home)
		if serr != nil || !sameObjectAt(st, dst) {
			s.restoreStaged(ctx, it, view, rel, home, st, tombstoned)
			return
		}
	}
	// Unrecorded (or still present at its recorded home) as of this
	// pass's stat — not yet authority to delete.
	err := view.RemoveStagedVeto(it.scope, rel, wantFP3, wantSHA,
		func(captured FileInfo) (bool, error) {
			busy, derr := s.recordersActive(ctx, it.scope)
			if discardVetoHook != nil {
				discardVetoHook()
			}
			if derr != nil || busy {
				return true, derr
			}
			home, claim, derr := s.recordedAtObject(ctx, it.scope, rel, captured, view)
			if derr != nil {
				return true, derr
			}
			if claim == recAmbiguous {
				// The row evidence cannot separate which home records
				// this object — no proof the captured object is a
				// surplus link, so the unlink is not authorized.
				return true, nil
			}
			if claim == recFound {
				// Deletion is safe only when the recorded home still
				// holds the object — the staged name is then a surplus
				// link (e.g. an exclusive-create linkat leftover). The
				// comparison binds dev:ino when both stats carry it, so
				// a foreign-device object with a colliding inode does
				// not satisfy "still at home". Otherwise it is displaced
				// recorded content: preserve it for the next pass's
				// restore.
				dst, serr := view.Stat(it.scope, home)
				if serr != nil || !sameObjectAt(captured, dst) {
					return true, nil
				}
			}
			if !needCommit {
				return false, nil
			}
			ok, derr := s.intentApplied(ctx, it)
			if derr != nil || !ok {
				return true, derr
			}
			return false, nil
		})
	_ = err // mismatch or veto leaves the object parked at the sealed name
}

// settleStaged finishes what an interrupted verified effect left behind
// at the intent's staging slot, then re-judges any crash-orphaned
// quarantine objects from a prior pass. Every branch is non-destructive
// toward foreign content: the only objects ever deleted are the op's
// own staged bytes (hash-proven) and the exact object the effect was
// allowed to displace (fingerprint-proven) — and even those deletions
// happen under a quarantine name a retired actor cannot write.
// Foreign objects are restored to their home name or left parked.
func (s *Store) settleStaged(ctx context.Context, it intent, view ReconView, tombstoned bool) {
	rel := stageRel(it)
	s.settleStagedOne(ctx, it, view, rel, tombstoned)
	// Crash-orphaned quarantine (-q-) and parked (-p-) objects carry the
	// same identity evidence; re-judge them each pass. The "-"-suffixed
	// prefix covers both classes and -p-…-q- chains from sealed captures
	// of a parked name.
	dir, base := splitRel(rel)
	if names, err := view.ListStaged(it.scope, dir, base+"-"); err == nil {
		for _, n := range names {
			qrel := n
			if dir != "" {
				qrel = dir + "/" + n
			}
			s.settleStagedOne(ctx, it, view, qrel, tombstoned)
		}
	}
}

func (s *Store) settleStagedOne(ctx context.Context, it intent, view ReconView, rel string, tombstoned bool) {
	st, err := view.Stat(it.scope, rel)
	if err != nil {
		return // absent or unobservable — nothing to finish
	}
	st3 := fp3(st.Fingerprint)
	switch it.op {
	case "write", "mkdir":
		if it.expectSHA != "" && it.expectSHA != "dir" && st.Kind == "file" {
			if h, herr := view.Hash(it.scope, rel); herr == nil && h == it.expectSHA {
				// Our own unpublished bytes — safe to discard unless a
				// version row records this very object (a delayed effect
				// can park recorded content whose bytes happen to match).
				s.settleDelete(ctx, it, view, rel, st, tombstoned, "", it.expectSHA, false)
				return
			}
		}
		if it.dstFP != "" && st3 == fp3(it.dstFP) {
			// The object the write was allowed to displace — but a
			// fingerprint match alone cannot prove the exchange
			// committed: a delayed drain or swap can park this recorded
			// object here without the effect ever landing (F-3). Delete
			// only with row-level proof it is unrecorded AND proof the
			// intent's own apply committed (intentApplied).
			s.settleDelete(ctx, it, view, rel, st, tombstoned, st3, "", true)
			return
		}
		// The staged slot holds a foreign object. A version row may
		// record it — the row is authoritative for where recorded
		// content belongs, so route it home (its home may be this
		// intent's own path: restoreStaged swaps only when the row
		// records this very object there, never a bare name).
		home, claim, derr := s.recordedAtObject(ctx, it.scope, rel, st, view)
		if derr != nil || claim == recAmbiguous {
			return // row set unverifiable or ambiguous — preserve, never misplace
		}
		if claim == recFound {
			s.restoreStaged(ctx, it, view, rel, home, st, tombstoned)
			return
		}
		// If the path still carries THIS op's staged bytes, finish the
		// undo the dead process could not: exchange restores the
		// displaced object to its name; whatever comes back is inspected
		// before any discard. A sealed rel can never be an exchange
		// target — drain it instead: park the name's bytes at the
		// unsealed base slot, then move the sealed object onto the name.
		if it.expectSHA != "" && it.expectSHA != "dir" {
			if h, herr := view.Hash(it.scope, it.path); herr == nil && h == it.expectSHA {
				// The path's content hashes to our expected bytes, but
				// the object need not be ours: a successor may have
				// written the same bytes there (174). Swapping would move
				// that object out of its public name toward a discard.
				// Leave the name alone — the slot object stays parked —
				// while another writer may still be committing a row for
				// it, or when a row already records the live object.
				if busy, derr := s.recordersActive(ctx, it.scope); derr != nil || busy {
					return
				}
				// The slot object is unrecorded (recorded objects route
				// home above). The row check asks whether the NAME's
				// record still describes what sits there: a current row
				// (fp3 matches the live object) means a successor owns
				// the name — never displace acknowledged content. A
				// stale row records an object already displaced from the
				// name; the swap then only moves the live object into
				// the slot, where settleDelete's recordedAt screen —
				// and the discard veto for in-flight writers — decide
				// whether it may be discarded. Never a bare hash match.
				if rowFP, found, rerr := s.recordedFP(ctx, it.scope, it.path); rerr != nil {
					return
				} else if found {
					if live, lerr := view.Stat(it.scope, it.path); lerr != nil ||
						fp3(rowFP) == fp3(live.Fingerprint) {
						return
					}
				}
				if _, base := splitRel(rel); isSealedName(base) {
					s.drainSealed(view, it.scope, stageRel(it), rel, it.path)
					s.journalPlacement(ctx, it.scope, view, it.path)
					return
				}
				if view.SwapStaged(it.scope, rel, it.path) == nil {
					s.journalPlacement(ctx, it.scope, view, it.path)
					if h2, herr2 := view.Hash(it.scope, rel); herr2 == nil && h2 == it.expectSHA {
						// Our stale bytes came back — discard them
						// unless a row records this object.
						if st2, serr := view.Stat(it.scope, rel); serr == nil {
							s.settleDelete(ctx, it, view, rel, st2, tombstoned, "", it.expectSHA, false)
						}
					}
					// Otherwise the slot now holds a racing writer's
					// object — leave it parked; the name correctly holds
					// the restored displaced content.
				}
				return
			}
		}
		s.restoreStaged(ctx, it, view, rel, it.path, st, tombstoned)
	case "remove":
		if it.dstFP != "" && st3 == fp3(it.dstFP) {
			// The object the remove was allowed to delete — but a
			// fingerprint match alone cannot prove the removal
			// committed (F-3): require row-level proof it is unrecorded
			// and proof the intent's own apply committed (intentApplied).
			s.settleDelete(ctx, it, view, rel, st, tombstoned, st3, "", true)
			return
		}
		// Recorded content parked under a remove intent belongs at its
		// recorded home — which need not be this intent's path.
		dest := it.path
		home, claim, derr := s.recordedAtObject(ctx, it.scope, rel, st, view)
		if derr != nil || claim == recAmbiguous {
			return // unverifiable or ambiguous — preserve
		}
		if claim == recFound {
			dest = home
		}
		s.restoreStaged(ctx, it, view, rel, dest, st, tombstoned)
	case "rename":
		if it.dstFP != "" && st3 == fp3(it.dstFP) {
			// The displaced destination object — deleting it was part
			// of the rename only if the rename committed (F-3): require
			// row-level proof it is unrecorded and proof the intent's
			// own apply committed (intentApplied).
			s.settleDelete(ctx, it, view, rel, st, tombstoned, st3, "", true)
			return
		}
		// A foreign object parked at the slot came from either the
		// destination name (a raced undo) or the source name (a racer's
		// content captured during commit). Restore it to whichever name
		// the version rows say it belongs to.
		s.restoreStaged(ctx, it, view, rel,
			s.stagedHome(ctx, it, rel, st, view), st, tombstoned)
	}
}

// stagedHome picks the name a parked rename-residual belongs to: a
// version row recording this exact object names its true home — which
// may be a path unrelated to this intent's source and destination.
// Only unrecorded residue falls back to the intent's own names: the
// destination when it matches the destination's recorded fingerprint,
// else the source name — the object was captured there, so restoring
// the source name recovers the pre-effect shape.
func (s *Store) stagedHome(ctx context.Context, it intent, rel string, st FileInfo, view ReconView) string {
	home, claim, err := s.recordedAtObject(ctx, it.scope, rel, st, view)
	if err != nil || claim == recAmbiguous {
		return "" // row set unverifiable or ambiguous — leave parked
	}
	if claim == recFound {
		return home
	}
	dctx, cancel := s.dbCtx(ctx)
	defer cancel()
	var rowFP string
	if it.toPath != "" && s.pool.QueryRow(dctx,
		`SELECT fp FROM file_version WHERE scope=$1 AND path=$2`,
		it.scope, it.toPath).Scan(&rowFP) == nil && recordedMatch(rowFP, st) {
		return it.toPath
	}
	return it.path
}

// restoreStaged returns a parked recovery object to a name it belongs
// to, without ever overwriting: an empty name gets a NOREPLACE move
// when the object is the recorded content for that name (or the intent
// is still unresolved — its outcome never judged); an occupied name
// holding non-recorded content gets the recorded object swapped back
// onto it (the squatter parks at the slot for the next pass). Anything
// else — unverifiable name, recorded content already in place, resolved
// intent with a foreign object — stays parked.
func (s *Store) restoreStaged(ctx context.Context, it intent, view ReconView, rel, destPath string, st FileInfo, tombstoned bool) {
	s.restoreStagedTo(ctx, it.scope, stageRel(it), view, rel, destPath, st, tombstoned)
}

func (s *Store) restoreStagedTo(ctx context.Context, scope, parkBase string, view ReconView, rel, destPath string, st FileInfo, tombstoned bool) {
	if destPath == "" {
		return
	}
	if st.Kind == "dir" {
		// A member's own row is its authority — independent of whether
		// this container's restore proceeds. Members whose recorded
		// homes lie outside the container's target subtree leave now,
		// so a deferred container restore cannot strand them inside.
		s.extractDivergentMembers(ctx, scope, view, rel, destPath)
	}
	dctx, cancel := s.dbCtx(ctx)
	var rowFP, rowSHA, rowBirth string
	rerr := s.pool.QueryRow(dctx,
		`SELECT fp, content_sha, birth FROM file_version WHERE scope=$1 AND path=$2`,
		scope, destPath).Scan(&rowFP, &rowSHA, &rowBirth)
	cancel()
	rowMatch := rerr == nil && recordedMatch(rowFP, st) &&
		birthVerdict(rowBirth, st) >= 0
	dst, derr := view.Stat(scope, destPath)
	switch {
	case derr == nil:
		// Occupied: only a proven recorded object swaps back — never
		// overwrite a name merely because something sits parked. A
		// sealed rel cannot receive the displaced squatter, so drain
		// through the unsealed base slot instead.
		if rowMatch && !sameObjectAt(st, dst) &&
			s.parkedOwnsRow(ctx, view, scope, rel, destPath, rowFP, rowSHA, rowBirth, st, dst) {
			// The occupant may be a live writer's fresh object whose row
			// has not committed yet; moving it now would only churn it
			// into this namespace. Retry once those writers settle.
			if busy, berr := s.recordersActive(ctx, scope); berr != nil || busy {
				return
			}
			if !s.moveJudged(view, scope, rel, st) {
				return // rel changed mid-pass — re-judge next pass
			}
			if _, base := splitRel(rel); isSealedName(base) {
				s.drainSealed(view, scope, parkBase, rel, destPath)
				s.journalPlacement(ctx, scope, view, destPath)
			} else if view.SwapStaged(scope, rel, destPath) == nil {
				s.journalPlacement(ctx, scope, view, destPath)
			}
		}
	case absentVerdict(derr):
		movable := rowMatch
		if !movable && errors.Is(rerr, pgx.ErrNoRows) && !tombstoned {
			// Unresolved intent, empty name, no recorded row —
			// restoring recovers the pre-effect shape.
			movable = true
		}
		if movable && s.moveJudged(view, scope, rel, st) &&
			view.MoveStaged(scope, rel, destPath) == nil {
			s.journalPlacement(ctx, scope, view, destPath)
		}
	default:
		// unverifiable — leave parked
	}
	// A directory that reached its recorded home still owes each member
	// its own judgment: the container's row proves the container's name
	// only — members with divergent rows route to their own homes.
	if st.Kind == "dir" {
		if dst2, serr := view.Stat(scope, destPath); serr == nil && sameObjectAt(st, dst2) {
			s.routeDirMembers(ctx, scope, view, destPath)
		}
	}
}

// journalPlacement records which object a recovery move actually landed
// at a public path — stat'ed AFTER the move, so a swap inside the
// stat→move window journals the true occupant, not the intended one.
// This is the ownership evidence verifyPublicMember needs before it may
// reverse a placement: without it the service cannot distinguish its own
// recovery residue from an ordinary external rename it must not undo.
// Journal failure is fail-safe in the conservative direction: an
// unjournaled placement is simply never auto-reversed.
func (s *Store) journalPlacement(ctx context.Context, scope string, view ReconView, destPath string) {
	if s.pool == nil || view == nil {
		return
	}
	dst, serr := view.Stat(scope, destPath)
	if serr != nil {
		return // nothing verifiably landed — journal nothing
	}
	dctx, cancel := s.dbCtx(ctx)
	_, _ = s.pool.Exec(dctx,
		`INSERT INTO recovery_place (scope, path, fp, birth)
		 VALUES ($1,$2,$3,$4)
		 ON CONFLICT (scope,path) DO UPDATE
		 SET fp=EXCLUDED.fp, birth=EXCLUDED.birth, at=now()`,
		scope, destPath, dst.Fingerprint, dst.Birth)
	cancel()
}

// placedHere reports whether a recovery_place row records THIS object —
// the same contract as row matching (recordedMatch): fp3 generation for
// files, inode leg for directories. A full-fingerprint compare would
// break on every rename, which legitimately updates ctime; birth is a
// positive confirmation only, never required (btime-less filesystems).
func placedHere(fp, birth string, st FileInfo) bool {
	return recordedMatch(fp, st) && birthVerdict(birth, st) >= 0
}

// moveJudged binds the mutation to the object this pass judged: the
// staged name is re-stat'ed and must still hold that same live object
// (dev:ino) before any move or swap. A racer swapping the staged name
// between judgment and mutation is caught here — the pass defers and
// re-judges rather than installing an unexamined object at a recorded
// home. Without device evidence there is nothing to bind; the move
// proceeds on the judgment the pass already made.
func (s *Store) moveJudged(view ReconView, scope, rel string, st FileInfo) bool {
	if st.DevIno == "" {
		return true
	}
	cur, err := view.Stat(scope, rel)
	return err == nil && sameObjectAt(st, cur)
}

// routeDirMembers re-judges each member of a recorded container that
// has just reached (or already occupies) its recorded home. A member
// whose own version row records a different home is routed there
// through the same authority path — restoring the container must not
// absorb members into unrecorded public paths while their rows claim
// them elsewhere. Unrecorded and ambiguous members stay inside the
// container, preserved; staged names inside are handed to the usual
// orphan judgment.
func (s *Store) routeDirMembers(ctx context.Context, scope string, view ReconView, dir string) {
	members, lerr := view.ListStaged(scope, dir, "")
	if lerr != nil {
		return
	}
	for _, m := range members {
		mrel := dir + "/" + m
		if strings.HasPrefix(m, opStagePrefix) {
			s.settleOrphanStaged(ctx, scope, view, mrel, m, map[string]struct{}{})
			continue
		}
		if strings.HasPrefix(m, stagingPrefix) {
			continue
		}
		mst, merr := view.Stat(scope, mrel)
		if merr != nil {
			continue
		}
		mhome, mclaim, mderr := s.recordedAtObject(ctx, scope, mrel, mst, view)
		if mderr != nil || mclaim != recFound || mhome == mrel {
			continue // unrecorded, ambiguous, or already home — rides
		}
		if pdir, _ := splitRel(mhome); pdir != "" {
			if err := view.EnsureDir(scope, pdir); err != nil {
				continue
			}
		}
		s.restoreStagedTo(ctx, scope, orphanParkBase(mrel), view, mrel, mhome, mst, true)
	}
}

// extractDivergentMembers moves members of the container at rel whose
// own version rows record homes OUTSIDE the container's target
// subtree. A member's row is its own authority: restoring the
// container must not carry a member to an unrecorded public path while
// its row names a different home, and a deferred container restore
// must not strand the member inside. Members whose rows point beneath
// destPath are left for routeDirMembers once the container lands.
// Unrecorded and ambiguous members stay inside, preserved.
func (s *Store) extractDivergentMembers(ctx context.Context, scope string, view ReconView, rel, destPath string) {
	members, lerr := view.ListStaged(scope, rel, "")
	if lerr != nil {
		return
	}
	for _, m := range members {
		if strings.HasPrefix(m, opStagePrefix) || strings.HasPrefix(m, stagingPrefix) {
			continue
		}
		mrel := rel + "/" + m
		mst, merr := view.Stat(scope, mrel)
		if merr != nil {
			continue
		}
		mhome, mclaim, mderr := s.recordedAtObject(ctx, scope, mrel, mst, view)
		if mderr != nil || mclaim != recFound {
			continue // unrecorded or ambiguous — rides with the container
		}
		if strings.HasPrefix(mhome, destPath+"/") {
			continue // home inside the container's subtree — post-restore pass
		}
		if pdir, _ := splitRel(mhome); pdir != "" {
			if err := view.EnsureDir(scope, pdir); err != nil {
				continue
			}
		}
		s.restoreStagedTo(ctx, scope, orphanParkBase(mrel), view, mrel, mhome, mst, true)
	}
}

// parkedOwnsRow arbitrates an occupied-home restore. The row at
// destPath matched the parked object's identity evidence — but for a
// directory that evidence is the inode leg alone, and an inode
// collision across devices (or an fp3 coincidence for a file) can make
// the occupant indistinguishable at the row level. The swap evicts the
// occupant to a parked name, so it needs the parked object's claim to
// be strictly stronger — never a heuristic that could evict the row's
// true subject:
//
//  1. If the occupant fails the row's evidence outright it is an
//     undisputed impostor — swap.
//  2. Birth evidence: the row's persisted btime names one creation
//     event. A candidate whose own birth contradicts the row's cannot
//     be its subject; a matching birth beats every weaker leg. Missing
//     birth on either side is no evidence — never a contradiction.
//  3. Recorded generation: exactly one candidate still equals the
//     row's fp3 — the object the row recorded, unchurned since commit.
//  4. Content evidence: a file row's content_sha names the recorded
//     bytes — a hash match beats an fp3-coincident impostor.
//  5. Container corroboration: a member of the parked dir whose own
//     row resolves beneath destPath binds the container to that home.
//  6. Nothing separates them — preserve both: leave the occupant,
//     keep the parked object enumerable for a pass with better
//     evidence. Never evict on a guess.
//
// Notably there is no device-locality inference: a row carries no
// device leg, so "the object's device" is unknowable from the row, and
// the parent's current device does not prove which object the row
// recorded (mount topology can change). Device evidence binds two live
// stats (sameObjectAt); it cannot reconstruct which object a row
// described.
func (s *Store) parkedOwnsRow(ctx context.Context, view ReconView, scope, rel, destPath, rowFP, rowSHA, rowBirth string, st, dst FileInfo) bool {
	if !recordedMatch(rowFP, dst) {
		return true // occupant is not the row's subject at all
	}
	stB, dstB := birthVerdict(rowBirth, st), birthVerdict(rowBirth, dst)
	if stB != dstB {
		return stB > dstB // matching birth > unknown > contradicting
	}
	stGen := fp3(rowFP) == fp3(st.Fingerprint)
	dstGen := fp3(rowFP) == fp3(dst.Fingerprint)
	if stGen != dstGen {
		return stGen
	}
	if rowSHA != "" && rowSHA != "dir" && st.Kind == "file" {
		hs, he := view.Hash(scope, rel)
		hd, de := view.Hash(scope, destPath)
		if he == nil && de == nil && (hs == rowSHA) != (hd == rowSHA) {
			return hs == rowSHA
		}
	}
	if st.Kind == "dir" && s.dirMemberCorroborates(ctx, view, scope, rel, destPath, map[string]struct{}{}) {
		return true
	}
	return false
}

// birthVerdict compares a row's persisted birth with a live object's:
// +1 match, 0 when either side lacks the evidence, -1 contradiction.
func birthVerdict(rowBirth string, st FileInfo) int {
	if rowBirth == "" || st.Birth == "" {
		return 0
	}
	if rowBirth == st.Birth {
		return 1
	}
	return -1
}

// drainSealed moves a sealed quarantine object onto its home name when
// that name holds content that must give way — without ever writing
// into the sealed name. The name's current object is first parked at a
// fresh enumerable -p- name (never the fixed base slot: a permanently
// parked unattributable object there must not stall restores forever),
// then the sealed object moves onto the now-empty name. A racer
// claiming the name between the two moves leaves the sealed object
// parked — bytes are preserved and the next pass retries with a fresh
// park name, so convergence requires only finite interference, never a
// free base slot. Nothing is deleted here.
func (s *Store) drainSealed(view ReconView, scope, parkBase, rel, name string) {
	if view.MoveStaged(scope, name, parkBase+"-p-"+randHex(6)) != nil {
		return
	}
	_ = view.MoveStaged(scope, rel, name)
}

// sweepOrphanStaged re-homes recorded objects parked under staging names
// whose intent row no longer exists. settleStaged enumerates an intent's
// namespace only while its file_op row survives; a delayed
// MoveStaged/SwapStaged/drainSealed deposit — decided while the intent
// lived — can land after the row is gone, and no intent-keyed pass ever
// lists the name again. This sweep walks every scope that still has
// version or intent rows (a scope with no rows can hold only unrecorded
// residue, which is preserved anyway) and restores each orphaned staged
// object that a version row still records to its recorded home. Objects
// nobody records stay parked: unknown foreign bytes are never collected.
func (s *Store) sweepOrphanStaged(ctx context.Context, view ReconView) {
	if s.pool == nil {
		return
	}
	dctx, cancel := s.dbCtx(ctx)
	rows, err := s.pool.Query(dctx,
		`SELECT scope FROM file_version
		 UNION SELECT scope FROM file_op WHERE root=$1`, s.rootID)
	if err != nil {
		cancel()
		return
	}
	var scopes []string
	for rows.Next() {
		var sc string
		if err := rows.Scan(&sc); err == nil {
			scopes = append(scopes, sc)
		}
	}
	rows.Close()
	cancel()
	for _, scope := range scopes {
		if s.deposed.Load() {
			return
		}
		// No scope mutex here: intent ids are never reused, so a
		// namespace whose file_op row is absent stays orphaned for the
		// whole sweep — a live declare cannot resurrect it — and the
		// per-object checks (recordersActive, row match, NOREPLACE
		// moves) already fence against in-flight settlers. Taking the
		// scope lock would let an in-flight op stall a pass that only
		// touches dead namespaces.
		s.settleOrphanDir(ctx, scope, view, "", map[string]struct{}{})
	}
}

// settleOrphanDir walks one directory for orphaned staged objects,
// recursing into subdirectories. It enumerates every entry — staged
// names are found by prefix, directories by stat — so its cost is the
// scope's entry count, not the number of parked objects. There is no
// depth bound: relPath accepts paths of any depth and intents' staging
// namespaces can sit that deep, so a cutoff would strand recorded
// content where no pass ever enumerates it. Loop safety comes from the
// seen set: a directory revisited under a second path (a bind mount or
// cyclic name chain) is identified by dev:ino — or by path when the
// object cannot be identified — and not walked twice.
func (s *Store) settleOrphanDir(ctx context.Context, scope string, view ReconView, dir string, seen map[string]struct{}) {
	names, err := view.ListStaged(scope, dir, "")
	if err != nil {
		return
	}
	for _, n := range names {
		rel := n
		if dir != "" {
			rel = dir + "/" + n
		}
		if strings.HasPrefix(n, opStagePrefix) {
			s.settleOrphanStaged(ctx, scope, view, rel, n, seen)
			continue
		}
		if strings.HasPrefix(n, stagingPrefix) {
			continue // live write-staging name — never parked evidence
		}
		st, serr := view.Stat(scope, rel)
		if serr != nil {
			continue
		}
		// An ordinary public member is re-judged too: interference can
		// leave a recorded object at a public path its row does not
		// describe (e.g. a swap landing inside the stat→move window),
		// and no other pass ever looks at public names. If the row at
		// this path does not record this object but another row does,
		// the object goes home; otherwise it is preserved in place.
		if !s.verifyPublicMember(ctx, scope, view, rel, st) {
			continue // rerouted away — nothing to descend into
		}
		if st.Kind == "dir" {
			if !markSeen(seen, st, rel) {
				continue
			}
			s.settleOrphanDir(ctx, scope, view, rel, seen)
		}
	}
}

// verifyPublicMember re-judges one enumerated public object. It reports
// whether the object still sits at rel when the call returns — false
// means it was rerouted to its recorded home and callers should not
// treat rel as the same entry. An object stays when the row at its
// path records it (coherent), when no row anywhere records it (unknown
// — preserved), or when claims cannot be resolved (ambiguous —
// preserved).
//
// A public path holding a recorded-elsewhere object is not proof the
// service put it there: people and secretaries share this filesystem,
// and an ordinary external `mv a b` produces exactly this shape —
// object at b, row still naming a. Automatically undoing a deliberate
// move is not acceptable, so rerouting requires the recovery_place
// journal to prove THIS object was placed at rel by a recovery pass.
// What the journal cannot distinguish — an externally misplaced
// recorded object at a public path — stays put by design; its row stays
// truthful and the object remains reachable. Recovery repairs only its
// own residue.
func (s *Store) verifyPublicMember(ctx context.Context, scope string, view ReconView, rel string, st FileInfo) bool {
	dctx, cancel := s.dbCtx(ctx)
	var rowFP, rowSHA, rowBirth string
	rerr := s.pool.QueryRow(dctx,
		`SELECT fp, content_sha, birth FROM file_version WHERE scope=$1 AND path=$2`,
		scope, rel).Scan(&rowFP, &rowSHA, &rowBirth)
	cancel()
	if rerr != nil && !errors.Is(rerr, pgx.ErrNoRows) {
		return true // row set unverifiable — preserve, never reroute
	}
	// Coherence needs kind agreement too: a real-sha file row cannot
	// record a dir object, and a 'dir' row cannot record a file. An
	// empty sha carries no kind evidence — coherent for either. Birth
	// likewise only contradicts when both sides carry it.
	kindOK := st.Kind != "dir" || rowSHA == "dir" || rowSHA == ""
	if st.Kind == "file" && rowSHA == "dir" {
		kindOK = false
	}
	if rerr == nil && recordedMatch(rowFP, st) && kindOK &&
		birthVerdict(rowBirth, st) >= 0 {
		return true // the row at this path records this object — coherent
	}
	home, claim, derr := s.recordedAtObject(ctx, scope, rel, st, view)
	if derr != nil || claim != recFound || home == rel {
		return true // unrecorded, unverifiable, or ambiguous — preserved
	}
	// Ownership gate: only an object a recovery pass itself placed at
	// rel may be moved off it — anything else is user-domain content
	// (an ordinary rename, an editor's write), which must not be
	// reverted. A stale or contradicting journal row is pruned.
	dctx, cancel = s.dbCtx(ctx)
	var placeFP, placeBirth string
	perr := s.pool.QueryRow(dctx,
		`SELECT fp, birth FROM recovery_place WHERE scope=$1 AND path=$2`,
		scope, rel).Scan(&placeFP, &placeBirth)
	cancel()
	if perr != nil {
		return true // not journaled (or unreadable) — preserve
	}
	if !placedHere(placeFP, placeBirth, st) {
		dctx, cancel = s.dbCtx(ctx)
		_, _ = s.pool.Exec(dctx,
			`DELETE FROM recovery_place WHERE scope=$1 AND path=$2`, scope, rel)
		cancel()
		return true // journal records a different object — stale; preserve
	}
	// A live writer may be committing to rel or home this instant —
	// defer rather than move an object out from under an in-flight op.
	if busy, berr := s.recordersActive(ctx, scope); berr != nil || busy {
		return true
	}
	if pdir, _ := splitRel(home); pdir != "" {
		if err := view.EnsureDir(scope, pdir); err != nil {
			return true
		}
	}
	s.restoreStagedTo(ctx, scope, orphanParkBase(rel), view, rel, home, st, true)
	if _, serr := view.Stat(scope, rel); serr != nil {
		// The object left rel — the placement journal is consumed.
		dctx, cancel = s.dbCtx(ctx)
		_, _ = s.pool.Exec(dctx,
			`DELETE FROM recovery_place WHERE scope=$1 AND path=$2`, scope, rel)
		cancel()
		return false
	}
	return true
}

// markSeen records a directory identity for the sweep's loop guard.
// dev:ino is the object identity across renames and bind-mounted
// aliases — an inode alone is unique only within one filesystem, so a
// bare ino would collapse distinct directories from different devices.
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

// settleOrphanStaged handles one staged name whose owning intent may be
// gone. A numeric id names a live or tombstoned intent's namespace:
// while its file_op row exists the intent's own settleStaged owns it.
// Only a name with no surviving intent row is an orphan, and only an
// orphan that a version row still records is moved — restored to its
// recorded home. Everything else (unverifiable row set, unrecorded
// bytes) stays parked.
func (s *Store) settleOrphanStaged(ctx context.Context, scope string, view ReconView, rel, name string, seen map[string]struct{}) {
	rest := name[len(opStagePrefix):]
	if idStr, _, _ := strings.Cut(rest, "-"); idStr != "" {
		if id, perr := strconv.ParseInt(idStr, 10, 64); perr == nil {
			dctx, cancel := s.dbCtx(ctx)
			var one int
			qerr := s.pool.QueryRow(dctx,
				`SELECT 1 FROM file_op WHERE id=$1`, id).Scan(&one)
			cancel()
			if qerr == nil {
				return // the intent row still enumerates this namespace
			}
			if !errors.Is(qerr, pgx.ErrNoRows) {
				return // unverifiable — preserve
			}
		}
	}
	st, err := view.Stat(scope, rel)
	if err != nil {
		return
	}
	home, claim, derr := s.recordedAtObject(ctx, scope, rel, st, view)
	if derr != nil || claim == recAmbiguous {
		return // unverifiable or ambiguous — preserve whole
	}
	if claim == recFound {
		if dst, serr := view.Stat(scope, home); serr == nil && sameObjectAt(st, dst) {
			// Still present at its recorded home — surplus link, leave
			// it. The home container's members may still diverge from
			// their own rows; judge them.
			if st.Kind == "dir" {
				s.routeDirMembers(ctx, scope, view, home)
			}
			return
		}
		s.restoreStagedTo(ctx, scope, orphanParkBase(rel), view, rel, home, st, true)
		return
	}
	// The staged object is unrecorded. If it is a directory it can hold
	// recorded members: auto-created parent dirs carry no version row,
	// so the container's fingerprint says nothing about its contents —
	// a delayed drain/swap can deposit an entire unrecorded container
	// under a dead namespace. Descend and judge each member by its own
	// row; the container and unrecorded residue stay parked.
	if st.Kind == "dir" && markSeen(seen, st, rel) {
		s.settleOrphanContents(ctx, scope, view, rel, seen)
	}
}

// settleOrphanContents walks the members of an unrecorded parked
// directory. Every member is judged by its own version row: a recorded
// object is restored to its recorded home (the home's parent chain is
// recreated first — the parked container often IS the missing parent);
// staged names inside are routed through settleOrphanStaged for the
// usual intent-row check; unrecorded members stay inside the container,
// preserved.
func (s *Store) settleOrphanContents(ctx context.Context, scope string, view ReconView, dir string, seen map[string]struct{}) {
	names, err := view.ListStaged(scope, dir, "")
	if err != nil {
		return
	}
	for _, n := range names {
		rel := dir + "/" + n
		if strings.HasPrefix(n, opStagePrefix) {
			s.settleOrphanStaged(ctx, scope, view, rel, n, seen)
			continue
		}
		st, serr := view.Stat(scope, rel)
		if serr != nil {
			continue
		}
		home, claim, derr := s.recordedAtObject(ctx, scope, rel, st, view)
		if derr != nil || claim == recAmbiguous {
			continue // unverifiable or ambiguous — preserve
		}
		if claim == recFound {
			// A recorded object — file OR directory — is restored whole
			// to its recorded home. Judge the container before touching
			// its contents: descending first would move members out of a
			// dir that is itself recorded, leaving the dir's own row
			// stranded (an empty recorded dir has no members to find).
			if dst, serr := view.Stat(scope, home); serr == nil && sameObjectAt(st, dst) {
				if st.Kind == "dir" {
					s.routeDirMembers(ctx, scope, view, home)
				}
				continue // already present at its recorded home — surplus link
			}
			if pdir, _ := splitRel(home); pdir != "" {
				if err := view.EnsureDir(scope, pdir); err != nil {
					continue // parent chain unverifiable — retry next pass
				}
			}
			s.restoreStagedTo(ctx, scope, orphanParkBase(rel), view, rel, home, st, true)
			continue
		}
		// Unrecorded members stay parked — but an unrecorded DIRECTORY
		// can hold recorded members (auto-created parents carry no row),
		// so descend into it and judge each member by its own row.
		if st.Kind == "dir" && markSeen(seen, st, rel) {
			s.settleOrphanContents(ctx, scope, view, rel, seen)
		}
	}
}

// orphanParkBase picks the sibling park base for a squatter displaced
// while restoring an orphan. A numeric intent id keeps the deposit
// attributable to its (dead) namespace; any other staged name parks
// under a generic orphan tag in the same directory. The name stays
// enumerable: every .filesv-op-* name is re-judged by later sweeps.
func orphanParkBase(rel string) string {
	dir, base := splitRel(rel)
	park := opStagePrefix + "orphan"
	if rest, ok := strings.CutPrefix(base, opStagePrefix); ok {
		if idStr, _, _ := strings.Cut(rest, "-"); idStr != "" {
			if _, err := strconv.ParseInt(idStr, 10, 64); err == nil {
				park = opStagePrefix + idStr
			}
		}
	}
	if dir != "" {
		return dir + "/" + park
	}
	return park
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
func (v funcView) SwapStaged(scope, staged, name string) error {
	return ErrUnavailable
}
func (v funcView) RemoveStaged(scope, path, wantFP3, wantSHA string) error {
	return ErrUnavailable
}
func (v funcView) RemoveStagedVeto(scope, path, wantFP3, wantSHA string,
	veto func(captured FileInfo) (bool, error)) error {
	return ErrUnavailable
}
func (v funcView) ListStaged(scope, dir, prefix string) ([]string, error) {
	return nil, nil
}
func (v funcView) EnsureDir(scope, dir string) error { return ErrUnavailable }
func (v funcView) Close() error                      { return nil }

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
	const cols = `id, owner, scope, op, path, to_path, version, pre_fp, dst_fp, expect_sha, src_kind, at`
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
			if err := rows.Scan(&it.id, &it.owner, &it.scope, &it.op, &it.path, &it.toPath,
				&it.version, &it.preFP, &it.dstFP, &it.expectSHA, &it.srcKind, &it.at); err != nil {
				return out
			}
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
		s.sweepOrphanStaged(ctx, view)
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

	// Settle any recovery object the intent's verified effect left at its
	// deterministic staging name before judging the path itself — a
	// process that died between the exchange and the verdict leaves the
	// displaced object there, and the judgment must see the restored
	// shape, not the transient one.
	s.settleStaged(ctx, it, view, tombstoned)

	switch it.op {
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
		// one tx — the evidence intentApplied requires. Finish discarding
		// what the verified effect left behind now: with the row gone, no
		// later pass enumerates this intent's slot.
		s.settleStaged(ctx, it, view, true)
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
