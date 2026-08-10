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
	"time"

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
// boundary. Version-only history cannot prove which SQL bytes ran, including
// the replaced version 0008, so a pre-dogfood database must be reset instead
// of guessed at, backfilled, or silently blessed with the current manifest.
var ErrPreCutoverResetRequired = errors.New("pre-cutover database reset required")

// migrationBookkeepingSchema is the durable record of applied migrations. The
// runner owns this table; individual migration files must not create it.
const migrationBookkeepingSchema = `
CREATE TABLE IF NOT EXISTS schema_migrations (
	version    bigint      PRIMARY KEY,
	name       text        NOT NULL,
	sha256     text        NOT NULL,
	applied_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS name text;
ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS sha256 text;`

type pendingMigration struct {
	version int
	name    string
	content string
	sha256  string
}

// migrationStore is implemented by both a pool and one acquired pool
// connection. Migrate uses the latter so its session-scoped advisory lock and
// every operation protected by that lock stay on the same Postgres session.
type migrationStore interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	Begin(context.Context) (pgx.Tx, error)
}

// MigrationManifestEntry is the immutable identity of one embedded migration.
// Version, file name, and the SHA-256 of the exact SQL bytes are all retained:
// a version number alone cannot detect a rewritten migration after dogfood has
// made the database durable product state.
type MigrationManifestEntry struct {
	Version int    `json:"version"`
	Name    string `json:"name"`
	SHA256  string `json:"sha256"`
}

// MigrationStatus describes the exact binary/database migration relationship.
// Ready is true only when every embedded migration is applied and every
// applied row still matches the binary's manifest.
type MigrationStatus struct {
	ManifestSHA256 string                   `json:"manifest_sha256"`
	Expected       []MigrationManifestEntry `json:"expected"`
	Applied        []MigrationManifestEntry `json:"applied"`
	Pending        []MigrationManifestEntry `json:"pending"`
	Ready          bool                     `json:"ready"`
}

// Migrate applies every embedded up-migration that has not yet been recorded in
// schema_migrations. It is idempotent: re-running against an up-to-date database
// is a no-op. A session-level advisory lock serializes concurrent runners.
func Migrate(ctx context.Context, pool *pgxpool.Pool) (returnErr error) {
	connection, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	if _, err := connection.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationAdvisoryLockID); err != nil {
		// Cancellation can race with lock acquisition. Do not return a session
		// whose lock state is uncertain to the pool.
		raw := connection.Hijack()
		closeCtx, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
		closeErr := raw.Close(closeCtx)
		cancelClose()
		return errors.Join(fmt.Errorf("acquire migration lock: %w", err), closeErr)
	}
	defer func() {
		returnErr = errors.Join(returnErr, releaseMigrationConnection(connection))
	}()

	if _, err := connection.Exec(ctx, migrationBookkeepingSchema); err != nil {
		return fmt.Errorf("ensure schema_migrations table: %w", err)
	}
	if err := rejectLegacyManifestRows(ctx, connection); err != nil {
		return err
	}

	pending, err := pendingMigrations(ctx, connection)
	if err != nil {
		return err
	}
	for _, m := range pending {
		if err := applyMigration(ctx, connection, m); err != nil {
			return err
		}
	}
	if err := sealMigrationManifestSchema(ctx, connection); err != nil {
		return err
	}
	return nil
}

func releaseMigrationConnection(connection *pgxpool.Conn) error {
	unlockCtx, cancelUnlock := context.WithTimeout(context.Background(), 5*time.Second)
	var unlocked bool
	err := connection.QueryRow(unlockCtx, "SELECT pg_advisory_unlock($1)", migrationAdvisoryLockID).Scan(&unlocked)
	cancelUnlock()
	if err != nil || !unlocked {
		// Never return a session with an uncertain session-level lock to the pool.
		raw := connection.Hijack()
		closeCtx, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
		closeErr := raw.Close(closeCtx)
		cancelClose()
		if err == nil {
			err = errors.New("Postgres session did not hold the migration lock")
		}
		return errors.Join(
			fmt.Errorf("release migration lock: %w", err),
			closeErr,
		)
	}
	connection.Release()
	return nil
}

func pendingMigrations(ctx context.Context, pool migrationStore) ([]pendingMigration, error) {
	embedded, err := embeddedUpMigrations()
	if err != nil {
		return nil, err
	}
	if len(embedded) == 0 {
		return nil, nil
	}
	rows, err := pool.Query(ctx, "SELECT version, name, sha256 FROM schema_migrations ORDER BY version")
	if err != nil {
		return nil, fmt.Errorf("read applied migrations: %w", err)
	}
	defer rows.Close()
	byVersion := make(map[int]pendingMigration, len(embedded))
	for _, migration := range embedded {
		byVersion[migration.version] = migration
	}
	applied := make(map[int]bool, len(embedded))
	for rows.Next() {
		var version int
		var name, digest string
		if err := rows.Scan(&version, &name, &digest); err != nil {
			return nil, fmt.Errorf("scan applied version: %w", err)
		}
		expected, ok := byVersion[version]
		if !ok {
			return nil, fmt.Errorf("applied migration %d is absent from the embedded manifest", version)
		}
		if name != expected.name || digest != expected.sha256 {
			return nil, fmt.Errorf(
				"applied migration %d manifest mismatch: database has %q sha256=%s, binary has %q sha256=%s",
				version, name, digest, expected.name, expected.sha256,
			)
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

func applyMigration(ctx context.Context, pool migrationStore, m pendingMigration) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration %d: %w", m.version, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, m.content); err != nil {
		return fmt.Errorf("apply migration %d (%s): %w", m.version, m.name, err)
	}
	if _, err := tx.Exec(ctx,
		"INSERT INTO schema_migrations (version, name, sha256) VALUES ($1, $2, $3)",
		m.version, m.name, m.sha256,
	); err != nil {
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
		digest := sha256.Sum256(content)
		out = append(out, pendingMigration{
			version: version,
			name:    entry.Name(),
			content: string(content),
			sha256:  hex.EncodeToString(digest[:]),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	for index := 1; index < len(out); index++ {
		if out[index-1].version == out[index].version {
			return nil, fmt.Errorf(
				"duplicate embedded migration version %d (%s and %s)",
				out[index].version, out[index-1].name, out[index].name,
			)
		}
	}
	return out, nil
}

// EmbeddedMigrationManifest returns the deterministic manifest compiled into
// this binary. It does not need a database and is suitable for release and
// backup manifests.
func EmbeddedMigrationManifest() ([]MigrationManifestEntry, string, error) {
	migrations, err := embeddedUpMigrations()
	if err != nil {
		return nil, "", err
	}
	entries := make([]MigrationManifestEntry, len(migrations))
	manifestHash := sha256.New()
	for index, migration := range migrations {
		entry := MigrationManifestEntry{
			Version: migration.version,
			Name:    migration.name,
			SHA256:  migration.sha256,
		}
		entries[index] = entry
		_, _ = fmt.Fprintf(manifestHash, "%d\x00%s\x00%s\n", entry.Version, entry.Name, entry.SHA256)
	}
	return entries, hex.EncodeToString(manifestHash.Sum(nil)), nil
}

// MigrationManifestStatus verifies the durable bookkeeping rows without
// mutating them. A missing table, pending migration, renamed migration, changed
// SQL body, or database-only historical version makes Ready false and returns
// an error. Deployment readiness uses this after migrate-before-deploy.
func MigrationManifestStatus(ctx context.Context, pool *pgxpool.Pool) (MigrationStatus, error) {
	expected, manifestDigest, err := EmbeddedMigrationManifest()
	if err != nil {
		return MigrationStatus{}, err
	}
	status := MigrationStatus{ManifestSHA256: manifestDigest, Expected: expected}
	rows, err := pool.Query(ctx,
		"SELECT version, name, sha256 FROM schema_migrations ORDER BY version",
	)
	if err != nil {
		return status, fmt.Errorf("read migration manifest: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var entry MigrationManifestEntry
		if err := rows.Scan(&entry.Version, &entry.Name, &entry.SHA256); err != nil {
			return status, fmt.Errorf("scan migration manifest: %w", err)
		}
		status.Applied = append(status.Applied, entry)
	}
	if err := rows.Err(); err != nil {
		return status, fmt.Errorf("iterate migration manifest: %w", err)
	}

	expectedByVersion := make(map[int]MigrationManifestEntry, len(expected))
	for _, entry := range expected {
		expectedByVersion[entry.Version] = entry
	}
	appliedByVersion := make(map[int]MigrationManifestEntry, len(status.Applied))
	for _, entry := range status.Applied {
		expectedEntry, ok := expectedByVersion[entry.Version]
		if !ok {
			return status, fmt.Errorf("applied migration %d is absent from the embedded manifest", entry.Version)
		}
		if entry != expectedEntry {
			return status, fmt.Errorf(
				"applied migration %d manifest mismatch: database has %q sha256=%s, binary has %q sha256=%s",
				entry.Version, entry.Name, entry.SHA256, expectedEntry.Name, expectedEntry.SHA256,
			)
		}
		appliedByVersion[entry.Version] = entry
	}
	for _, entry := range expected {
		if _, ok := appliedByVersion[entry.Version]; !ok {
			status.Pending = append(status.Pending, entry)
		}
	}
	if len(status.Pending) > 0 {
		return status, fmt.Errorf("%d embedded migration(s) are not applied", len(status.Pending))
	}
	status.Ready = true
	return status, nil
}

// VerifyMigrations is the readiness-oriented form of MigrationManifestStatus.
func VerifyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := MigrationManifestStatus(ctx, pool)
	return err
}

// rejectLegacyManifestRows refuses the pre-durability runner's version-only
// records. A current binary cannot prove which historical SQL bytes produced
// such a row, so filling the current name and digest would manufacture evidence
// rather than verify it. Dogfood has not cut over yet: reset the database and
// let the current runner create the manifest from an empty schema.
func rejectLegacyManifestRows(ctx context.Context, pool migrationStore) error {
	rows, err := pool.Query(ctx,
		"SELECT version FROM schema_migrations WHERE name IS NULL OR sha256 IS NULL ORDER BY version",
	)
	if err != nil {
		return fmt.Errorf("inspect migration manifest for legacy rows: %w", err)
	}
	defer rows.Close()
	var versions []int
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return fmt.Errorf("scan legacy migration version: %w", err)
		}
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate legacy migration manifest: %w", err)
	}
	if len(versions) > 0 {
		return fmt.Errorf(
			"%w: legacy version-only schema_migrations rows %v cannot be verified; reset this pre-cutover database before starting this binary",
			ErrPreCutoverResetRequired,
			versions,
		)
	}
	return nil
}

func sealMigrationManifestSchema(ctx context.Context, pool migrationStore) error {
	if _, err := pool.Exec(ctx, `
		ALTER TABLE schema_migrations ALTER COLUMN name SET NOT NULL;
		ALTER TABLE schema_migrations ALTER COLUMN sha256 SET NOT NULL;
		DO $migration_manifest_constraint$
		BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM pg_constraint
				WHERE conrelid = 'schema_migrations'::regclass
				  AND conname = 'schema_migrations_sha256_format'
			) THEN
				ALTER TABLE schema_migrations
					ADD CONSTRAINT schema_migrations_sha256_format
					CHECK (sha256 ~ '^[0-9a-f]{64}$');
			END IF;
		END
		$migration_manifest_constraint$;
	`); err != nil {
		return fmt.Errorf("seal migration manifest columns: %w", err)
	}
	return nil
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
