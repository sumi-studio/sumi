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
// journal. Versions are the conflict-detection primitive for API-mediated
// writers; the journal lets clients and reconciliation see what the service
// changed (executor-direct writes bypass it and are detected via fingerprint
// comparison instead — that boundary is contractual, not hidden).
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
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
			seq     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			scope   text NOT NULL,
			path    text NOT NULL,
			op      text NOT NULL,
			version bigint NOT NULL,
			at      timestamptz NOT NULL DEFAULT now()
		);
		CREATE INDEX IF NOT EXISTS file_event_scope_seq ON file_event(scope, seq);
	`)
	return err
}

var (
	ErrConflict   = errors.New("version conflict")
	ErrNoSuchFile = errors.New("no such file")
)

// IfVersion is the caller's declared expectation for the target path.
type IfVersion struct {
	Mode    string // "any" | "none" | "eq"
	Version int64
}

// WithWrite runs fn inside a transaction after checking/bumping the target's
// version. fn performs the actual filesystem mutation (stage+rename happens
// inside so a crash cannot publish an unversioned name; see report for the
// residual window between rename and COMMIT). On fn error the version bump
// rolls back.
func (s *Store) WithWrite(ctx context.Context, scope, path, op string, iv IfVersion, fn func() (FileInfo, error)) (int64, FileInfo, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, FileInfo{}, err
	}
	defer tx.Rollback(ctx)

	newVer, err := bumpVersion(ctx, tx, scope, path, iv)
	if err != nil {
		return 0, FileInfo{}, err
	}

	info, ferr := fn()
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

// Rename applies the CAS to the destination path, performs fn (the FS rename),
// and removes the source's version row in the same transaction.
func (s *Store) Rename(ctx context.Context, scope, from, to string, iv IfVersion, fn func() (FileInfo, error)) (int64, FileInfo, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, FileInfo{}, err
	}
	defer tx.Rollback(ctx)

	newVer, err := bumpVersion(ctx, tx, scope, to, iv)
	if err != nil {
		return 0, FileInfo{}, err
	}
	info, ferr := fn()
	if ferr != nil {
		return 0, FileInfo{}, ferr
	}
	if _, err := tx.Exec(ctx,
		`UPDATE file_version SET fp=$3 WHERE scope=$1 AND path=$2`,
		scope, to, info.Fingerprint); err != nil {
		return 0, FileInfo{}, err
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM file_version WHERE scope=$1 AND path=$2`,
		scope, from); err != nil {
		return 0, FileInfo{}, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO file_event (scope, path, op, version) VALUES ($1,$2,'rename',$3)`,
		scope, to, newVer); err != nil {
		return 0, FileInfo{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, FileInfo{}, err
	}
	return newVer, info, nil
}

// bumpVersion performs the check-and-bump inside an open tx; the row lock is
// held until COMMIT so concurrent writers on the same path serialize.
func bumpVersion(ctx context.Context, tx pgx.Tx, scope, path string, iv IfVersion) (int64, error) {
	var newVer int64
	var err error
	switch iv.Mode {
	case "none":
		err = tx.QueryRow(ctx,
			`INSERT INTO file_version (scope, path, version) VALUES ($1,$2,1)
			 ON CONFLICT (scope,path) DO NOTHING RETURNING version`,
			scope, path).Scan(&newVer)
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrConflict
		}
	case "eq":
		if iv.Version == 0 {
			err = tx.QueryRow(ctx,
				`INSERT INTO file_version (scope, path, version) VALUES ($1,$2,1)
				 ON CONFLICT (scope,path) DO NOTHING RETURNING version`,
				scope, path).Scan(&newVer)
			if errors.Is(err, pgx.ErrNoRows) {
				return 0, ErrConflict
			}
			break
		}
		err = tx.QueryRow(ctx,
			`UPDATE file_version SET version = version + 1, updated = now()
			 WHERE scope=$1 AND path=$2 AND version=$3 RETURNING version`,
			scope, path, iv.Version).Scan(&newVer)
		if errors.Is(err, pgx.ErrNoRows) {
			var exists bool
			if qerr := tx.QueryRow(ctx,
				`SELECT EXISTS(SELECT 1 FROM file_version WHERE scope=$1 AND path=$2)`,
				scope, path).Scan(&exists); qerr == nil && !exists {
				return 0, ErrNoSuchFile
			}
			return 0, ErrConflict
		}
	case "any":
		err = tx.QueryRow(ctx,
			`INSERT INTO file_version (scope, path, version) VALUES ($1,$2,1)
			 ON CONFLICT (scope,path) DO UPDATE SET version = file_version.version + 1,
			   updated = now() RETURNING version`,
			scope, path).Scan(&newVer)
	default:
		return 0, fmt.Errorf("unknown if_version mode %q", iv.Mode)
	}
	if err != nil {
		return 0, err
	}
	return newVer, nil
}

// Remove drops the version row for a removed path, combining the version CAS
// + event + row delete + FS removal in one transaction.
func (s *Store) Remove(ctx context.Context, scope, path string, iv IfVersion, fn func() error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var cur int64
	err = tx.QueryRow(ctx,
		`SELECT version FROM file_version WHERE scope=$1 AND path=$2 FOR UPDATE`,
		scope, path).Scan(&cur)
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
	}
	if err := fn(); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM file_version WHERE scope=$1 AND path=$2`, scope, path); err != nil {
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
	Op      string `json:"op"`
	Version int64  `json:"version"`
	At      string `json:"at"`
}

func (s *Store) Changes(ctx context.Context, scope string, since int64, limit int) ([]Event, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT seq, path, op, version, at FROM file_event
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
		if err := rows.Scan(&e.Seq, &e.Path, &e.Op, &e.Version, &ts); err != nil {
			return nil, err
		}
		e.At = ts.UTC().Format(time.RFC3339Nano)
		out = append(out, e)
	}
	return out, rows.Err()
}
