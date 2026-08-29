package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	schema30Version            = 30
	schema29Version            = 29
	schema30UpName             = "0030_message_revisions.up.sql"
	schema30DownName           = "0030_message_revisions.down.sql"
	sealedSchema30UpChecksum   = "6b6e311be7580df5903c331829f1393e9a7866c394b3e8bc07195518e08c9c13"
	sealedSchema30DownChecksum = "474077eb1f738008a2aaff71e1ab04ffb376e3704658474da746af24fc0938e2"
	schema30CleanupTimeout     = 5 * time.Second
)

var (
	ErrSchema30RollbackWrongHead            = errors.New("schema 30 rollback requires exact migration head 30")
	ErrSchema30RollbackUnsafeData           = errors.New("schema 30 rollback refuses messages with revision other than 1")
	ErrSchema30RollbackCommitOutcomeUnknown = errors.New("schema 30 rollback commit outcome is unknown; keep writers stopped and inspect exact schema state")
)

// PreflightSchema30Rollback proves the database is eligible for the one sealed
// 0030 -> 0029 rollback. It takes the normal migration advisory lock and does
// not mutate the database. Writer quiescence is an external precondition; this
// function cannot prove it.
func PreflightSchema30Rollback(ctx context.Context, pool *pgxpool.Pool) error {
	return withSchema30MigrationLock(ctx, pool, func(conn migrationDB) error {
		_, err := preflightSchema30Rollback(ctx, conn)
		return err
	})
}

// RollbackSchema30To29 performs only the sealed pre-write 0030 -> 0029
// downgrade. The down SQL and deletion of the version-30 bookkeeping row are
// committed atomically, after the same checks as PreflightSchema30Rollback.
// It is intentionally not called by normal API startup.
func RollbackSchema30To29(ctx context.Context, pool *pgxpool.Pool) error {
	return withSchema30MigrationLock(ctx, pool, func(conn migrationDB) error {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin schema 30 rollback: %w", err)
		}
		defer func() {
			cleanupCtx, cleanupCancel := schema30CleanupContext(ctx)
			defer cleanupCancel()
			_ = tx.Rollback(cleanupCtx)
		}()

		// External writer quiescence remains mandatory. This lock closes the
		// database race between the final revision check and DROP COLUMN.
		if _, err := tx.Exec(ctx, "LOCK TABLE messages IN ACCESS EXCLUSIVE MODE"); err != nil {
			return fmt.Errorf("lock messages for schema 30 rollback: %w", err)
		}
		down, err := preflightSchema30Rollback(ctx, tx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, down); err != nil {
			return fmt.Errorf("apply sealed schema 30 down migration: %w", err)
		}
		tag, err := tx.Exec(ctx,
			"DELETE FROM schema_migrations WHERE version = $1 AND checksum = $2",
			schema30Version, sealedSchema30UpChecksum)
		if err != nil {
			return fmt.Errorf("delete schema 30 migration record: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("delete schema 30 migration record: affected %d rows, want 1", tag.RowsAffected())
		}
		var head int
		if err := tx.QueryRow(ctx, "SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&head); err != nil {
			return fmt.Errorf("verify schema 29 rollback head: %w", err)
		}
		if head != schema29Version {
			return fmt.Errorf("verify schema 29 rollback head: found %d, want %d", head, schema29Version)
		}
		if err := tx.Commit(ctx); err != nil {
			// PostgreSQL may have committed even when the client loses the COMMIT
			// response. Do not report a refusal or expose transport details, and do
			// not invite a blind retry.
			return ErrSchema30RollbackCommitOutcomeUnknown
		}
		return nil
	})
}

func withSchema30MigrationLock(ctx context.Context, pool *pgxpool.Pool, run func(migrationDB) error) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire schema 30 rollback connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationAdvisoryLockID); err != nil {
		return fmt.Errorf("acquire migration lock for schema 30 rollback: %w", err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := schema30CleanupContext(ctx)
		defer cleanupCancel()
		_, _ = conn.Exec(cleanupCtx, "SELECT pg_advisory_unlock($1)", migrationAdvisoryLockID)
	}()
	return run(conn)
}

func schema30CleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), schema30CleanupTimeout)
}

func preflightSchema30Rollback(ctx context.Context, conn migrationDB) (string, error) {
	down, err := sealedSchema30RollbackDownSQL()
	if err != nil {
		return "", err
	}

	var head int
	if err := conn.QueryRow(ctx, "SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&head); err != nil {
		return "", fmt.Errorf("read schema 30 rollback head: %w", err)
	}
	if head != schema30Version {
		return "", fmt.Errorf("%w: found %d", ErrSchema30RollbackWrongHead, head)
	}
	pending, err := pendingMigrations(ctx, conn)
	if err != nil {
		return "", fmt.Errorf("verify schema 30 rollback migration history: %w", err)
	}
	if len(pending) != 0 {
		return "", fmt.Errorf("%w: canonical history is incomplete", ErrSchema30RollbackWrongHead)
	}

	var unsafeRevision bool
	if err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM messages WHERE revision IS DISTINCT FROM 1)").Scan(&unsafeRevision); err != nil {
		return "", fmt.Errorf("inspect message revisions for schema 30 rollback: %w", err)
	}
	if unsafeRevision {
		return "", ErrSchema30RollbackUnsafeData
	}
	return down, nil
}

func sealedSchema30RollbackDownSQL() (string, error) {
	migrations, err := embeddedUpMigrations()
	if err != nil {
		return "", err
	}
	if len(migrations) == 0 {
		return "", errors.New("schema 30 rollback has no embedded migration history")
	}
	last := migrations[len(migrations)-1]
	if last.version != schema30Version || last.name != schema30UpName || migrationChecksum(last.content) != sealedSchema30UpChecksum {
		return "", errors.New("schema 30 rollback embedded up migration is not the sealed 0030 artifact")
	}
	down, err := migrationFS.ReadFile(migrationsDir + "/" + schema30DownName)
	if err != nil {
		return "", fmt.Errorf("read sealed schema 30 down migration: %w", err)
	}
	if migrationChecksum(string(down)) != sealedSchema30DownChecksum {
		return "", errors.New("schema 30 rollback embedded down migration is not the sealed 0030 artifact")
	}
	return string(down), nil
}
