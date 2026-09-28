// Package journalmirror keeps the API's file journals durable in PostgreSQL
// when the host disk may be discarded.
//
// The Direct Chat command logs, the browser event logs and the browser-session
// revocation list are files that the agentevents package appends to, fsyncs and
// sometimes rolls back by truncation. Their correctness is built on POSIX file
// semantics (flock, positional writes, fsync as the acknowledgement point), and
// that code stays unchanged. This package adds one rule underneath it: a
// file's Sync succeeds only after the same bytes are committed to PostgreSQL.
// On start, the mirror reconciles the directory with PostgreSQL's copy before
// the journals open it. A replaced host therefore resumes with every write the
// previous host acknowledged.
//
// Ownership. One API process owns the mirror at a time. Acquire takes a
// session advisory lock (the lease) on a dedicated connection and waits while
// another process holds it, then increments the owner epoch. The lease
// holder watches its lease session and reports the loss through
// Options.OnLost; every write also re-checks the epoch inside its own
// transaction, so a process that lost the lease can no longer acknowledge.
//
// Initialization. A mirror directory exists in PostgreSQL only after an
// explicit initialization: adopting a host's existing files (AttachAdopt) or
// declaring an empty mirror for a new installation (InitializeEmpty). A
// restore-only start (AttachRestore) against a database without that
// initialization fails instead of treating "nothing" as the journals.
//
// Local replica markers. Beside the files, a host keeps the mirror's lineage
// (.journal-mirror) and, per file, the generation of its last acknowledged
// change (.<name>.acked). Attach uses them to tell an ordinary unacknowledged
// local write (PostgreSQL wins; the local bytes are kept in a quarantine
// directory) from a database that is older than what this host acknowledged
// or belongs to another mirror (Attach refuses and changes nothing).
package journalmirror

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ChunkSize is the storage unit for file bytes in PostgreSQL.
const ChunkSize = 64 << 10

// leaseLockKey is the session advisory lock that makes one process the owner.
const leaseLockKey = int64(0x534a4d4c) // "SJML"

var (
	// ErrFenced reports that another API process acquired the mirror after
	// this one. The caller must stop serving: its local files can no longer
	// be made durable.
	ErrFenced = errors.New("journal mirror is owned by a newer API process")
	// ErrLeaseLost reports that this process's lease session ended (the
	// database restarted or failed over, the connection broke, or an operator
	// terminated it). Another process may acquire the mirror now.
	ErrLeaseLost = errors.New("journal mirror lease was lost")
	// ErrLeaseHeld reports that another process kept the lease for the whole
	// acquisition timeout.
	ErrLeaseHeld = errors.New("journal mirror lease is held by another API process")
	// ErrClosed reports use of a mirror after Close.
	ErrClosed = errors.New("journal mirror is closed")
)

// ReplicationError reports a Sync whose local fsync succeeded but whose bytes
// did not commit to PostgreSQL, or whose commit outcome is unknown (the
// connection broke after COMMIT was sent). The local file is durable. The
// file is marked diverged: its next Sync replaces PostgreSQL's copy from the
// first uncertain offset on with the local bytes before it reports success,
// so an ambiguous commit is either overwritten by the local state or, if the
// host is replaced first, restored as PostgreSQL's copy.
type ReplicationError struct{ Err error }

func (e *ReplicationError) Error() string { return "journal mirror: " + e.Err.Error() }
func (e *ReplicationError) Unwrap() error { return e.Err }

// LocalDurable marks the error for callers that must distinguish a failed
// replication (local state intact and durable) from a failed local fsync.
func (e *ReplicationError) LocalDurable() bool { return true }

// File is the handle shape used by the agentevents journals. It is an alias
// of an unnamed interface, identical to agentevents.JournalFile.
type File = interface {
	io.Reader
	io.Writer
	io.Seeker
	Sync() error
	Truncate(size int64) error
	Close() error
	Fd() uintptr
}

var (
	dirNameRe  = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	fileNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._=-]{0,238}$`)
)

// Mirrored reports whether a directory entry carries journal state. Lock
// files only carry flock state, temporary files belong to an atomic write in
// progress, and dot entries are this package's markers and quarantine; the
// journals or this package recreate all of them.
func Mirrored(name string) bool {
	if strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".lock") || strings.HasSuffix(name, ".tmp") {
		return false
	}
	return fileNameRe.MatchString(name)
}

// Options configure Acquire.
type Options struct {
	// Holder is a human-readable identity recorded for operators.
	Holder string
	// OnLost, if not nil, is called once in its own goroutine when this
	// process stops being the owner: the lease session ended or a write found
	// a newer epoch. The process must stop serving and background work.
	OnLost func(error)
	// Logf receives waiting and progress messages. Nil discards them.
	Logf func(format string, args ...any)
	// LeaseCheckInterval and LeaseCheckTimeout bound how long a silently
	// broken lease connection (no reset, no close) goes unnoticed:
	// at most their sum. A closed or terminated session is noticed at once.
	// Defaults: 5 s each.
	LeaseCheckInterval time.Duration
	LeaseCheckTimeout  time.Duration
}

type attachedDir struct {
	logical string
	lineage string
}

// Mirror is one API process's ownership of the PostgreSQL journal mirror.
type Mirror struct {
	pool   *pgxpool.Pool
	holder string
	epoch  int64
	logf   func(string, ...any)
	onLost func(error)

	lease     *pgx.Conn
	stopWatch context.CancelFunc
	watchDone chan struct{}
	closeOnce sync.Once

	mu   sync.RWMutex
	dirs map[string]attachedDir // absolute directory -> attachment

	lost     atomic.Bool
	lostErr  atomic.Pointer[error]
	lostOnce sync.Once

	divergedMu    sync.Mutex
	divergedFiles map[string]int64 // dir/name -> lowest offset changed since the last acknowledgement

	// flushHook, when set by tests in this package, runs after a flush
	// committed and may turn it into an error (an ambiguous commit).
	flushHook func(dir, name string) error
}

// Acquire takes ownership of the mirror for this process. It waits while
// another process holds the lease, logging the holder, until ctx ends.
func Acquire(ctx context.Context, pool *pgxpool.Pool, opts Options) (*Mirror, error) {
	if pool == nil {
		return nil, errors.New("journal mirror requires a database")
	}
	holder := strings.TrimSpace(opts.Holder)
	if holder == "" {
		holder = "unknown"
	}
	logf := opts.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	interval, timeout := opts.LeaseCheckInterval, opts.LeaseCheckTimeout
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	conn, err := pgx.ConnectConfig(ctx, pool.Config().ConnConfig.Copy())
	if err != nil {
		return nil, fmt.Errorf("journal mirror lease connection: %w", err)
	}
	fail := func(err error) (*Mirror, error) {
		_ = conn.Close(context.Background())
		return nil, err
	}
	// If this host vanishes without closing the connection, the server
	// releases the lease once TCP keepalives fail (about 30 s) instead of
	// the operating-system default of hours. Ignored on Unix sockets.
	if _, err := conn.Exec(ctx, `SET tcp_keepalives_idle = 15; SET tcp_keepalives_interval = 5; SET tcp_keepalives_count = 3; SET application_name = 'sumi-journal-mirror-lease'`); err != nil {
		logf("journal mirror: lease connection keepalive settings not applied: %v", err)
	}

	waitStarted := time.Now()
	var lastLog time.Time
	for {
		var got bool
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, leaseLockKey).Scan(&got); err != nil {
			return fail(fmt.Errorf("journal mirror lease: %w", err))
		}
		if got {
			break
		}
		current := describeOwner(ctx, conn)
		if time.Since(lastLog) >= 10*time.Second {
			logf("journal mirror: waiting %s for the lease held by %s", time.Since(waitStarted).Round(time.Second), current)
			lastLog = time.Now()
		}
		select {
		case <-ctx.Done():
			return fail(fmt.Errorf("%w (%s) after %s: %v", ErrLeaseHeld, current, time.Since(waitStarted).Round(time.Second), ctx.Err()))
		case <-time.After(time.Second):
		}
	}

	var epoch int64
	err = conn.QueryRow(ctx, `
		INSERT INTO api_journal_mirror_owner (singleton, epoch, holder, acquired_at)
		VALUES (true, 1, $1, now())
		ON CONFLICT (singleton) DO UPDATE
		SET epoch = api_journal_mirror_owner.epoch + 1, holder = EXCLUDED.holder, acquired_at = now()
		RETURNING epoch`, holder).Scan(&epoch)
	if err != nil {
		return fail(fmt.Errorf("acquire journal mirror: %w", err))
	}
	watchCtx, stop := context.WithCancel(context.Background())
	m := &Mirror{
		pool: pool, holder: holder, epoch: epoch, logf: logf, onLost: opts.OnLost,
		lease: conn, stopWatch: stop, watchDone: make(chan struct{}),
		dirs: map[string]attachedDir{},
	}
	go m.watch(watchCtx, interval, timeout)
	return m, nil
}

func describeOwner(ctx context.Context, conn *pgx.Conn) string {
	var holder string
	var epoch int64
	var since time.Time
	err := conn.QueryRow(ctx, `SELECT holder, epoch, acquired_at FROM api_journal_mirror_owner WHERE singleton`).Scan(&holder, &epoch, &since)
	if err != nil {
		return "an unknown process"
	}
	return fmt.Sprintf("%q (epoch %d, since %s)", holder, epoch, since.UTC().Format(time.RFC3339))
}

// watch keeps a read pending on the lease connection so a closed or
// terminated session is noticed at once, and every interval confirms with a
// read-only query that the session still answers and still owns the epoch.
func (m *Mirror) watch(ctx context.Context, interval, timeout time.Duration) {
	defer close(m.watchDone)
	for {
		waitCtx, cancel := context.WithTimeout(ctx, interval)
		err := m.lease.PgConn().WaitForNotification(waitCtx)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err != nil && !errors.Is(err, context.DeadlineExceeded) && !isTimeout(err) {
			m.lose(fmt.Errorf("%w: %v", ErrLeaseLost, err))
			return
		}
		checkCtx, cancel := context.WithTimeout(ctx, timeout)
		var epoch int64
		err = m.lease.QueryRow(checkCtx, `SELECT epoch FROM api_journal_mirror_owner WHERE singleton`).Scan(&epoch)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			m.lose(fmt.Errorf("%w: lease session did not answer within %s: %v", ErrLeaseLost, timeout, err))
			return
		}
		if epoch != m.epoch {
			m.lose(ErrFenced)
			return
		}
	}
}

func isTimeout(err error) bool {
	var timeout interface{ Timeout() bool }
	return errors.As(err, &timeout) && timeout.Timeout()
}

// lose marks this process as no longer the owner and reports it once.
func (m *Mirror) lose(err error) {
	m.lostOnce.Do(func() {
		m.lostErr.Store(&err)
		m.lost.Store(true)
		m.logf("journal mirror: %v", err)
		if m.onLost != nil {
			go m.onLost(err)
		}
	})
}

// lostError returns why this process is no longer the owner, or nil.
func (m *Mirror) lostError() error {
	if !m.lost.Load() {
		return nil
	}
	if p := m.lostErr.Load(); p != nil {
		return *p
	}
	return ErrLeaseLost
}

// Epoch is this process's owner epoch.
func (m *Mirror) Epoch() int64 { return m.epoch }

// Holder is the identity recorded for this owner.
func (m *Mirror) Holder() string { return m.holder }

// Fenced reports whether this process stopped being the owner (the lease
// was lost, a newer owner was found, or the mirror was closed).
func (m *Mirror) Fenced() bool { return m.lost.Load() }

// Close releases the lease. Writes through the mirror fail afterwards.
// OnLost is not called for a Close.
func (m *Mirror) Close() error {
	var err error
	m.closeOnce.Do(func() {
		m.lostOnce.Do(func() {
			closed := ErrClosed
			m.lostErr.Store(&closed)
			m.lost.Store(true)
		})
		m.stopWatch()
		<-m.watchDone
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = m.lease.Close(ctx)
	})
	return err
}
