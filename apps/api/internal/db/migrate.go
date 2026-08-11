package db

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

var ErrMigrationChecksumMismatch = errors.New("applied migration checksum does not match embedded history")

// migrationBookkeepingSchema is the durable record of applied migrations. The
// runner owns this table; individual migration files must not create it.
const migrationBookkeepingSchema = `
CREATE TABLE IF NOT EXISTS schema_migrations (
	version   bigint      PRIMARY KEY,
	applied_at timestamptz NOT NULL DEFAULT now(),
	checksum text        NOT NULL
);`

// migrationDB is deliberately satisfied by both pgxpool.Pool and a checked-out
// pgxpool.Conn. Production migration work uses one checked-out connection so
// the session-level advisory lock protects every read and write below it.
type migrationDB interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Begin(context.Context) (pgx.Tx, error)
}

type pendingMigration struct {
	version int
	name    string
	content string
}

// Migrate applies every embedded up-migration that has not yet been recorded in
// schema_migrations. It is idempotent: re-running against an up-to-date database
// is a no-op. A session-level advisory lock serializes concurrent runners.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationAdvisoryLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", migrationAdvisoryLockID)
	}()

	if _, err := conn.Exec(ctx, migrationBookkeepingSchema); err != nil {
		return fmt.Errorf("ensure schema_migrations table: %w", err)
	}
	// Pre-checksum development databases have the table but not the column.
	// Adding it is safe; the intentionally replaced version 0008 is rejected
	// below when its checksum is absent instead of being silently adopted.
	if _, err := conn.Exec(ctx,
		"ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum text"); err != nil {
		return fmt.Errorf("ensure migration checksum column: %w", err)
	}

	pending, err := pendingMigrations(ctx, conn)
	if err != nil {
		return err
	}
	// Once the exact embedded prefix has been proven, make the invariant a DB
	// constraint. A pre-checksum database with any applied row is rejected above
	// rather than retroactively blessing unverifiable history.
	if _, err := conn.Exec(ctx,
		"ALTER TABLE schema_migrations ALTER COLUMN checksum SET NOT NULL"); err != nil {
		return fmt.Errorf("require migration checksums: %w", err)
	}
	if err := rejectLegacyWorkspaceMigration(ctx, conn); err != nil {
		return err
	}
	for _, m := range pending {
		if err := applyMigration(ctx, conn, m); err != nil {
			return err
		}
	}
	return nil
}

// rejectLegacyWorkspaceMigration distinguishes every pre-cutover 0008 shape
// from the current 0008_workspace_core. Migration versions are the durable
// identity, so a database that recorded either the legacy Messaging schema or
// an earlier draft of Workspace core would otherwise skip the replacement.
// This is deliberately a reset guard, not a compatibility/backfill path.
func rejectLegacyWorkspaceMigration(ctx context.Context, db migrationDB) error {
	migrations, err := embeddedUpMigrations()
	if err != nil {
		return err
	}
	var expectedChecksum string
	for _, migration := range migrations {
		if migration.version == 8 {
			expectedChecksum = migrationChecksum(migration.content)
			break
		}
	}
	if expectedChecksum == "" {
		return errors.New("embedded Workspace migration 0008 is missing")
	}
	var versionApplied, currentFingerprint bool
	var recordedChecksum *string
	err = db.QueryRow(ctx, `
		SELECT
			EXISTS (SELECT 1 FROM schema_migrations WHERE version = 8),
			(SELECT checksum FROM schema_migrations WHERE version = 8),
			EXISTS (
				SELECT 1
				FROM information_schema.columns
				WHERE table_schema = current_schema()
				  AND table_name = 'workspaces'
				  AND column_name = 'owner_workspace_member_id'
			)
			AND to_regclass(current_schema() || '.app_catalog') IS NOT NULL
			AND to_regclass(current_schema() || '.app_workspace_role_capabilities') IS NOT NULL
			AND to_regclass(current_schema() || '.workspace_role_app_capability_grants') IS NOT NULL
	`).Scan(&versionApplied, &recordedChecksum, &currentFingerprint)
	if err != nil {
		return fmt.Errorf("inspect pre-cutover Workspace migration boundary: %w", err)
	}
	if versionApplied && (recordedChecksum == nil || *recordedChecksum != expectedChecksum || !currentFingerprint) {
		return fmt.Errorf("%w: recorded migration 0008 does not match the current Workspace foundation; reset this pre-cutover database and migrate from empty", ErrPreCutoverResetRequired)
	}
	return nil
}

func pendingMigrations(ctx context.Context, db migrationDB) ([]pendingMigration, error) {
	embedded, err := embeddedUpMigrations()
	if err != nil {
		return nil, err
	}
	if len(embedded) == 0 {
		return nil, nil
	}
	rows, err := db.Query(ctx, "SELECT version, checksum FROM schema_migrations ORDER BY version")
	if err != nil {
		return nil, fmt.Errorf("read applied migrations: %w", err)
	}
	defer rows.Close()
	appliedCount := 0
	for rows.Next() {
		var version int
		var checksum *string
		if err := rows.Scan(&version, &checksum); err != nil {
			return nil, fmt.Errorf("scan applied version: %w", err)
		}
		if appliedCount >= len(embedded) {
			return nil, fmt.Errorf("%w: applied migration %04d is not present in embedded history; reset this pre-cutover database and migrate from empty", ErrPreCutoverResetRequired, version)
		}
		expected := embedded[appliedCount]
		if version != expected.version {
			return nil, fmt.Errorf("%w: applied history is not the embedded prefix at position %d (found %04d, expected %04d); reset this pre-cutover database and migrate from empty", ErrPreCutoverResetRequired, appliedCount+1, version, expected.version)
		}
		if checksum == nil {
			return nil, fmt.Errorf("%w: migration %04d has no verifiable checksum; reset this pre-cutover database and migrate from empty", ErrPreCutoverResetRequired, version)
		}
		if *checksum != migrationChecksum(expected.content) {
			return nil, fmt.Errorf("%w: %w: version %04d; reset this pre-cutover database and migrate from empty", ErrPreCutoverResetRequired, ErrMigrationChecksumMismatch, version)
		}
		appliedCount++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate applied migrations: %w", err)
	}
	return embedded[appliedCount:], nil
}

func applyMigration(ctx context.Context, db migrationDB, m pendingMigration) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration %d: %w", m.version, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, m.content); err != nil {
		return fmt.Errorf("apply migration %d (%s): %w", m.version, m.name, err)
	}
	if _, err := tx.Exec(ctx,
		"INSERT INTO schema_migrations (version, checksum) VALUES ($1, $2)",
		m.version, migrationChecksum(m.content)); err != nil {
		return fmt.Errorf("record migration %d: %w", m.version, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %d: %w", m.version, err)
	}
	return nil
}

func migrationChecksum(content string) string {
	digest := sha256.Sum256([]byte(content))
	return hex.EncodeToString(digest[:])
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
