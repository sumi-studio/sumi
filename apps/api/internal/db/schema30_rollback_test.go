package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

func TestSchema30RollbackArtifactsAreSealed(t *testing.T) {
	down, err := sealedSchema30RollbackDownSQL()
	if err != nil {
		t.Fatalf("load sealed schema 30 rollback: %v", err)
	}
	if migrationChecksum(down) != sealedSchema30DownChecksum {
		t.Fatal("schema 30 rollback returned an unsealed down migration")
	}
}

func TestPreflightSchema30RollbackDoesNotMutateCanonicalHead(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate to schema 30: %v", err)
	}

	if err := PreflightSchema30Rollback(ctx, pool); err != nil {
		t.Fatalf("preflight schema 30 rollback: %v", err)
	}

	var head int
	var revisionColumnExists bool
	if err := pool.QueryRow(ctx, "SELECT max(version) FROM schema_migrations").Scan(&head); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = current_schema()
			  AND table_name = 'messages'
			  AND column_name = 'revision'
		)`).Scan(&revisionColumnExists); err != nil {
		t.Fatal(err)
	}
	if head != 30 || !revisionColumnExists {
		t.Fatalf("preflight mutated database: head=%d revision_column=%t", head, revisionColumnExists)
	}
}

func TestPreflightSchema30RollbackRejectsWrongHead(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	applyMigrationsThrough(t, ctx, pool, 29)

	err := PreflightSchema30Rollback(ctx, pool)
	if !errors.Is(err, ErrSchema30RollbackWrongHead) {
		t.Fatalf("preflight error = %v, want wrong-head refusal", err)
	}
}

func TestPreflightSchema30RollbackRejectsWrongChecksum(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate to schema 30: %v", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE schema_migrations SET checksum = repeat('0', 64) WHERE version = 29"); err != nil {
		t.Fatal(err)
	}

	err := PreflightSchema30Rollback(ctx, pool)
	if !errors.Is(err, ErrMigrationChecksumMismatch) {
		t.Fatalf("preflight error = %v, want checksum refusal", err)
	}
}

func TestPreflightSchema30RollbackRejectsRevisionAboveOne(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate to schema 30: %v", err)
	}
	insertSchema30RollbackMessage(t, ctx, pool)
	if _, err := pool.Exec(ctx, "UPDATE messages SET revision = 2"); err != nil {
		t.Fatal(err)
	}

	err := PreflightSchema30Rollback(ctx, pool)
	if !errors.Is(err, ErrSchema30RollbackUnsafeData) {
		t.Fatalf("preflight error = %v, want revision refusal", err)
	}
}

func TestPreflightSchema30RollbackRejectsNullRevisionAfterConstraintDrift(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate to schema 30: %v", err)
	}
	insertSchema30RollbackMessage(t, ctx, pool)
	if _, err := pool.Exec(ctx, "ALTER TABLE messages ALTER COLUMN revision DROP NOT NULL"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "UPDATE messages SET revision = NULL"); err != nil {
		t.Fatal(err)
	}

	err := PreflightSchema30Rollback(ctx, pool)
	if !errors.Is(err, ErrSchema30RollbackUnsafeData) {
		t.Fatalf("preflight error = %v, want null-revision refusal", err)
	}
}

func TestRollbackSchema30To29AtomicallyAppliesSealedDownMigration(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate to schema 30: %v", err)
	}
	insertSchema30RollbackMessage(t, ctx, pool)

	if err := RollbackSchema30To29(ctx, pool); err != nil {
		t.Fatalf("rollback schema 30 to 29: %v", err)
	}

	var head, messageCount int
	var revisionColumnExists, version30Exists bool
	if err := pool.QueryRow(ctx, "SELECT max(version) FROM schema_migrations").Scan(&head); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 30)").Scan(&version30Exists); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = current_schema()
			  AND table_name = 'messages'
			  AND column_name = 'revision'
		)`).Scan(&revisionColumnExists); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM messages").Scan(&messageCount); err != nil {
		t.Fatal(err)
	}
	if head != 29 || version30Exists || revisionColumnExists || messageCount != 1 {
		t.Fatalf("rollback result: head=%d version30=%t revision_column=%t messages=%d",
			head, version30Exists, revisionColumnExists, messageCount)
	}
}

func TestPreflightSchema30RollbackWaitsForExistingMigrationLock(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate to schema 30: %v", err)
	}
	lockConn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lockConn.Release()
	if _, err := lockConn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationAdvisoryLockID); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = lockConn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", migrationAdvisoryLockID)
	}()

	blockedCtx, blockedCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer blockedCancel()
	err = PreflightSchema30Rollback(blockedCtx, pool)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("contended preflight error = %v, want deadline exceeded", err)
	}
}

func TestRollbackSchema30To29RefusalLeavesSchema30Intact(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate to schema 30: %v", err)
	}
	insertSchema30RollbackMessage(t, ctx, pool)
	if _, err := pool.Exec(ctx, "UPDATE messages SET revision = 2"); err != nil {
		t.Fatal(err)
	}

	err := RollbackSchema30To29(ctx, pool)
	if !errors.Is(err, ErrSchema30RollbackUnsafeData) {
		t.Fatalf("rollback error = %v, want revision refusal", err)
	}
	var head, revision int
	if err := pool.QueryRow(ctx, "SELECT max(version) FROM schema_migrations").Scan(&head); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT revision FROM messages").Scan(&revision); err != nil {
		t.Fatal(err)
	}
	if head != 30 || revision != 2 {
		t.Fatalf("refused rollback mutated database: head=%d revision=%d", head, revision)
	}
}

func insertSchema30RollbackMessage(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	const (
		humanID      = "0198f0f4-9b72-7000-8000-000000003001"
		workspaceID  = "0198f0f4-9b72-7000-8000-000000003002"
		membershipID = "0198f0f4-9b72-7000-8000-000000003003"
		placeID      = "0198f0f4-9b72-7000-8000-000000003004"
		messageID    = "0198f0f4-9b72-7000-8000-000000003005"
	)
	if _, err := pool.Exec(ctx, "INSERT INTO humans (human_id) VALUES ($1)", humanID); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
		INSERT INTO workspaces (workspace_id, name, owner_workspace_member_id)
		VALUES ($1, 'schema30 rollback', $2)`, workspaceID, membershipID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO workspace_members (workspace_member_id, workspace_id, member_kind, member_id)
		VALUES ($1, $2, 'human', $3)`, membershipID, workspaceID, humanID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO places (place_id, kind, workspace_id, name)
		VALUES ($1, 'channel', $2, 'rollback')`, placeID, workspaceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO messages
			(message_id, workspace_id, place_id, seq, author_kind, author_id, content, client_nonce, request_digest)
		VALUES ($1, $2, $3, 1, 'human', $4, 'before writes', 'schema30-rollback', decode(repeat('ab', 32), 'hex'))`,
		messageID, workspaceID, placeID, humanID); err != nil {
		t.Fatal(err)
	}
}
