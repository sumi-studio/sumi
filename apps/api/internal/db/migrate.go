package db

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

const migrationsDir = "migrations"

// migrationAdvisoryLockID is a fixed 64-bit key used with pg_advisory_lock so
// concurrent API replicas (or a migrate binary racing the API) cannot apply the
// same migration twice. It is an arbitrary stable constant.
const migrationAdvisoryLockID = int64(0x534d4944) // "SMID"

var upMigrationRe = regexp.MustCompile(`^(\d+)_[^/]+\.up\.sql$`)

// ErrPreCutoverResetRequired marks the one intentional destructive migration
// boundary. Version 0008 was replaced before dogfooding data became durable;
// an old database must be reset instead of guessed at or partially adopted.
var ErrPreCutoverResetRequired = errors.New("pre-cutover Workspace schema replacement requires a database reset")

// migrationBookkeepingSchema is the durable record of applied migrations. The
// runner owns this table; individual migration files must not create it.
const migrationBookkeepingSchema = `
CREATE TABLE IF NOT EXISTS schema_migrations (
	version   bigint      PRIMARY KEY,
	applied_at timestamptz NOT NULL DEFAULT now()
);`

type pendingMigration struct {
	version int
	name    string
	content string
}

// Migrate applies every embedded up-migration that has not yet been recorded in
// schema_migrations. It is idempotent: re-running against an up-to-date database
// is a no-op. A session-level advisory lock serializes concurrent runners.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationAdvisoryLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", migrationAdvisoryLockID)
	}()

	if _, err := pool.Exec(ctx, migrationBookkeepingSchema); err != nil {
		return fmt.Errorf("ensure schema_migrations table: %w", err)
	}
	if err := rejectLegacyWorkspaceMigration(ctx, pool); err != nil {
		return err
	}

	pending, err := pendingMigrations(ctx, pool)
	if err != nil {
		return err
	}
	for _, m := range pending {
		if err := applyMigration(ctx, pool, m); err != nil {
			return err
		}
	}
	return nil
}

// rejectLegacyWorkspaceMigration distinguishes the replaced legacy
// 0008_messaging_schema from the current 0008_workspace_core. Migration
// versions are the durable identity, so a database that already recorded the
// old 0008 would otherwise skip the replacement and fail later in 0009 with an
// ambiguous "places already exists" error. This is deliberately a guard, not
// a compatibility/backfill path.
func rejectLegacyWorkspaceMigration(ctx context.Context, pool *pgxpool.Pool) error {
	var versionApplied, currentFingerprint bool
	err := pool.QueryRow(ctx, `
		SELECT
			EXISTS (SELECT 1 FROM schema_migrations WHERE version = 8),
			EXISTS (
				SELECT 1
				FROM information_schema.columns
				WHERE table_schema = current_schema()
				  AND table_name = 'workspaces'
				  AND column_name = 'owner_workspace_member_id'
			)
			AND to_regclass(current_schema() || '.app_catalog') IS NOT NULL
	`).Scan(&versionApplied, &currentFingerprint)
	if err != nil {
		return fmt.Errorf("inspect pre-cutover Workspace migration boundary: %w", err)
	}
	if versionApplied && !currentFingerprint {
		return fmt.Errorf("%w: recorded migration 0008 is the legacy Messaging schema; reset this pre-cutover database and migrate from empty", ErrPreCutoverResetRequired)
	}
	return nil
}

func pendingMigrations(ctx context.Context, pool *pgxpool.Pool) ([]pendingMigration, error) {
	embedded, err := embeddedUpMigrations()
	if err != nil {
		return nil, err
	}
	if len(embedded) == 0 {
		return nil, nil
	}
	rows, err := pool.Query(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return nil, fmt.Errorf("read applied migrations: %w", err)
	}
	defer rows.Close()
	applied := make(map[int]bool, len(embedded))
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("scan applied version: %w", err)
		}
		applied[version] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate applied migrations: %w", err)
	}
	pending := make([]pendingMigration, 0, len(embedded))
	for _, m := range embedded {
		if applied[m.version] {
			continue
		}
		pending = append(pending, m)
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].version < pending[j].version })
	return pending, nil
}

func applyMigration(ctx context.Context, pool *pgxpool.Pool, m pendingMigration) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration %d: %w", m.version, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, m.content); err != nil {
		return fmt.Errorf("apply migration %d (%s): %w", m.version, m.name, err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", m.version); err != nil {
		return fmt.Errorf("record migration %d: %w", m.version, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %d: %w", m.version, err)
	}
	return nil
}

// embeddedUpMigrations reads the embedded migrations directory and returns the
// up-migrations sorted by version. It is safe to unit-test without a database.
func embeddedUpMigrations() ([]pendingMigration, error) {
	entries, err := fs.ReadDir(migrationFS, migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}
	var out []pendingMigration
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		match := upMigrationRe.FindStringSubmatch(entry.Name())
		if match == nil {
			continue
		}
		version, err := strconv.Atoi(match[1])
		if err != nil {
			return nil, fmt.Errorf("parse migration version %q: %w", entry.Name(), err)
		}
		content, err := migrationFS.ReadFile(migrationsDir + "/" + entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read embedded migration %s: %w", entry.Name(), err)
		}
		out = append(out, pendingMigration{version: version, name: entry.Name(), content: string(content)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

// LatestAppliedVersion returns the highest version recorded in schema_migrations,
// or 0 when no migrations have been applied. It is a convenience for diagnostics.
func LatestAppliedVersion(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	var version int
	err := pool.QueryRow(ctx, "SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&version)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("query latest migration version: %w", err)
	}
	return version, nil
}
