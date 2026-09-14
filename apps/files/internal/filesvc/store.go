package filesvc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store mints service-managed per-path versions and records a mutation
// journal. Versions + recorded fingerprints are the conflict-detection
// primitive for API-mediated writers; the journal lets clients and
// reconciliation see what the service changed. Executor-direct writes bypass
// the journal entirely; the service detects them by comparing the recorded
// fingerprint to the live one at CAS time (eq/none) or read time (stat/read).
type Store struct {
	pool      *pgxpool.Pool
	opTimeout time.Duration // bounds how long a mutation may hold its row lock
}

func NewStore(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	s := &Store{pool: pool, opTimeout: 30 * time.Second}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// SetOpTimeout bounds the fs-mutation phase of a versioned write. On expiry
// the transaction rolls back (releasing the row lock) and the caller gets
// ErrUnavailable; the filesystem call may still complete afterwards, in which
// case the file exists without a version row — indistinguishable from, and
// reconciled exactly like, an executor-direct write.
func (s *Store) SetOpTimeout(d time.Duration) { s.opTimeout = d }

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
// The store calls it inside the version transaction, after acquiring the
// row lock, so a conditional write cannot silently overwrite a file that
// changed outside the service.
type FPProbe func() (fp string, exists bool, err error)

// runBounded executes fn (the filesystem mutation) under opTimeout. A slow
// or wedged store can no longer pin a PG row lock indefinitely.
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

// bumpVersion performs the check-and-bump inside an open tx; the row lock is
// held until COMMIT so concurrent writers on the same path serialize.
// probe is consulted for conditional modes:
//   - none / eq 0: no version row AND nothing on disk (a create-only write).
//   - eq n>0:    row version must equal n AND the on-disk fingerprint must
//     still equal the recorded one — an executor-side edit since the
//     caller's base version is a 409 external_change, not a silent overwrite.
//   - any:       unconditional; probe is not consulted.
func bumpVersion(ctx context.Context, tx pgx.Tx, scope, path string, iv IfVersion, probe FPProbe) (int64, error) {
	var newVer int64
	var recFP string
	var err error
	switch iv.Mode {
	case "none":
		err = tx.QueryRow(ctx,
			`INSERT INTO file_version (scope, path, version) VALUES ($1,$2,1)
			 ON CONFLICT (scope,path) DO NOTHING RETURNING version, fp`,
			scope, path).Scan(&newVer, &recFP)
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrConflict
		}
		if err == nil {
			// Row did not exist. Refuse to publish over an executor-created
			// file either — create-only means "must not exist anywhere".
			_, exists, perr := probe()
			if perr != nil {
				return 0, perr
			}
			if exists {
				return 0, ErrExternalChange
			}
		}
	case "eq":
		if iv.Version == 0 {
			// "No service version" must also mean "no file" — otherwise this
			// is a silent overwrite of something the service never saw.
			err = tx.QueryRow(ctx,
				`INSERT INTO file_version (scope, path, version) VALUES ($1,$2,1)
				 ON CONFLICT (scope,path) DO NOTHING RETURNING version, fp`,
				scope, path).Scan(&newVer, &recFP)
			if errors.Is(err, pgx.ErrNoRows) {
				return 0, ErrConflict
			}
			if err == nil {
				_, exists, perr := probe()
				if perr != nil {
					return 0, perr
				}
				if exists {
					return 0, ErrExternalChange
				}
			}
			break
		}
		err = tx.QueryRow(ctx,
			`UPDATE file_version SET version = version + 1, updated = now()
			 WHERE scope=$1 AND path=$2 AND version=$3 RETURNING version, fp`,
			scope, path, iv.Version).Scan(&newVer, &recFP)
		if errors.Is(err, pgx.ErrNoRows) {
			var exists bool
			if qerr := tx.QueryRow(ctx,
				`SELECT EXISTS(SELECT 1 FROM file_version WHERE scope=$1 AND path=$2)`,
				scope, path).Scan(&exists); qerr == nil && !exists {
				return 0, ErrNoSuchFile
			}
			return 0, ErrConflict
		}
		if err == nil {
			liveFP, exists, perr := probe()
			if perr != nil {
				return 0, perr
			}
			if !exists || (recFP != "" && liveFP != recFP) {
				return 0, ErrExternalChange
			}
		}
	case "any":
		err = tx.QueryRow(ctx,
			`INSERT INTO file_version (scope, path, version) VALUES ($1,$2,1)
			 ON CONFLICT (scope,path) DO UPDATE SET version = file_version.version + 1,
			   updated = now() RETURNING version, fp`,
			scope, path).Scan(&newVer, &recFP)
	default:
		return 0, fmt.Errorf("unknown if_version mode %q", iv.Mode)
	}
	if err != nil {
		return 0, err
	}
	return newVer, nil
}

// WithWrite runs fn inside a transaction after checking/bumping the target's
// version. fn performs the actual filesystem mutation (stage+rename/link
// happens inside so a crash cannot publish an unversioned name). On fn error
// the version bump rolls back.
func (s *Store) WithWrite(ctx context.Context, scope, path, op string, iv IfVersion, probe FPProbe, fn func() (FileInfo, error)) (int64, FileInfo, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, FileInfo{}, err
	}
	defer tx.Rollback(ctx)

	newVer, err := bumpVersion(ctx, tx, scope, path, iv, probe)
	if err != nil {
		return 0, FileInfo{}, err
	}

	info, ferr := s.runBounded(fn)
	if ferr != nil {
		return 0, FileInfo{}, ferr
	}
	if _, err := tx.Exec(ctx,
		`UPDATE file_version SET fp=$3 WHERE scope=$1 AND path=$2`,
		scope, path, info.Fingerprint); err != nil {
		return 0, FileInfo{}, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO file_event (scope, path, op, version) VALUES ($1,$2,$3,$4)`,
		scope, path, op, newVer); err != nil {
		return 0, FileInfo{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, FileInfo{}, err
	}
	return newVer, info, nil
}

// Rename applies the CAS to the destination path, performs fn (the FS rename,
// which for create-only modes uses renameat2(RENAME_NOREPLACE)), and moves
// the source subtree's version rows to the destination in the same
// transaction. The event records both from and to.
//
// Ordering matters: all destination-side row reconciliation happens BEFORE
// fn (the irreversible fs move), and the source-descendant move happens
// after it. If fn fails, the tx rollback restores the deleted dst rows.
// If the stale `to/*` rows were left for the descendant UPDATE to collide
// with, the fs move had already committed while the tx rolled back —
// permanent divergence (final-review B F2).
//
// Version lineage: the renamed object keeps its identity, so the source
// row's version carries forward (+1 for this mutation) instead of
// restarting at the destination's counter (review A F5).
func (s *Store) Rename(ctx context.Context, scope, from, to string, iv IfVersion, probe FPProbe, fn func() (FileInfo, error)) (int64, FileInfo, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, FileInfo{}, err
	}
	defer tx.Rollback(ctx)

	var fromVer int64
	var fromFP string
	var haveFrom bool
	err = tx.QueryRow(ctx,
		`SELECT version, fp FROM file_version WHERE scope=$1 AND path=$2`,
		scope, from).Scan(&fromVer, &fromFP)
	switch {
	case err == nil:
		haveFrom = true
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return 0, FileInfo{}, err
	}

	newVer, err := bumpVersion(ctx, tx, scope, to, iv, probe)
	if err != nil {
		return 0, FileInfo{}, err
	}

	// The fs rename replaces the destination subtree wholesale. Drop its
	// stale descendant rows now — inside the tx, so a failed rename rolls
	// them back — so the source-descendant move below cannot collide with
	// rows the disk operation will have erased anyway.
	if _, err := tx.Exec(ctx,
		`DELETE FROM file_version WHERE scope=$1 AND starts_with(path, $2 || '/')`,
		scope, to); err != nil {
		return 0, FileInfo{}, err
	}

	info, ferr := s.runBounded(fn)
	if ferr != nil {
		return 0, FileInfo{}, ferr
	}

	// Move version rows of descendants of the renamed path so a directory
	// rename does not strand children's versions at stale paths. Safe now:
	// nothing under `to/` exists in the table.
	if _, err := tx.Exec(ctx,
		`UPDATE file_version SET path = $3 || substr(path, length($2)+1)
		 WHERE scope=$1 AND starts_with(path, $2 || '/')`,
		scope, from, to); err != nil {
		return 0, FileInfo{}, err
	}
	finalVer := newVer
	if haveFrom {
		finalVer = fromVer + 1
	}
	if _, err := tx.Exec(ctx,
		`UPDATE file_version SET version=$3, fp=$4 WHERE scope=$1 AND path=$2`,
		scope, to, finalVer, info.Fingerprint); err != nil {
		return 0, FileInfo{}, err
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM file_version WHERE scope=$1 AND path=$2`,
		scope, from); err != nil {
		return 0, FileInfo{}, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO file_event (scope, path, from_path, op, version) VALUES ($1,$2,$3,'rename',$4)`,
		scope, to, from, finalVer); err != nil {
		return 0, FileInfo{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, FileInfo{}, err
	}
	return finalVer, info, nil
}

// Remove drops the version row for the removed path and any descendants,
// combining the version CAS + event + row deletes + FS removal in one
// transaction. probe guards conditional removes the same way as writes.
func (s *Store) Remove(ctx context.Context, scope, path string, iv IfVersion, probe FPProbe, fn func() error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var cur int64
	var recFP string
	err = tx.QueryRow(ctx,
		`SELECT version, fp FROM file_version WHERE scope=$1 AND path=$2 FOR UPDATE`,
		scope, path).Scan(&cur, &recFP)
	if errors.Is(err, pgx.ErrNoRows) {
		if iv.Mode == "eq" || iv.Mode == "none" {
			return ErrNoSuchFile
		}
	} else if err != nil {
		return err
	} else if iv.Mode == "none" {
		return ErrConflict
	} else if iv.Mode == "eq" && cur != iv.Version {
		return ErrConflict
	} else if iv.Mode == "eq" {
		// Do not silently remove content that changed outside the service
		// since the caller's base version.
		liveFP, exists, perr := probe()
		if perr != nil {
			return perr
		}
		if !exists || (recFP != "" && liveFP != recFP) {
			return ErrExternalChange
		}
	}
	if _, ferr := s.runBounded(func() (FileInfo, error) { return FileInfo{}, fn() }); ferr != nil {
		return ferr
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM file_version WHERE scope=$1 AND (path=$2 OR starts_with(path, $2 || '/'))`,
		scope, path); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO file_event (scope, path, op, version) VALUES ($1,$2,'remove',$3)`,
		scope, path, cur+1); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ObservedVersion returns the service-minted version and recorded fingerprint
// for a path (0,"" if never written through the service).
func (s *Store) ObservedVersion(ctx context.Context, scope, path string) (int64, string, error) {
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
