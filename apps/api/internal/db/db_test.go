package db

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

func TestEmbeddedUpMigrationsSortedAndUnique(t *testing.T) {
	migrations, err := embeddedUpMigrations()
	if err != nil {
		t.Fatalf("embeddedUpMigrations: %v", err)
	}
	if len(migrations) == 0 {
		t.Fatal("expected at least one embedded migration")
	}
	seen := make(map[int]bool, len(migrations))
	for i, m := range migrations {
		if seen[m.version] {
			t.Fatalf("duplicate migration version %d", m.version)
		}
		seen[m.version] = true
		if i > 0 && migrations[i-1].version >= m.version {
			t.Fatalf("migrations not sorted: %d before %d", migrations[i-1].version, m.version)
		}
		if strings.TrimSpace(m.content) == "" {
			t.Fatalf("migration %d (%s) has empty content", m.version, m.name)
		}
	}
	if migrations[0].version != 1 {
		t.Fatalf("expected first migration version 1, got %d", migrations[0].version)
	}
	if !strings.HasSuffix(migrations[0].name, ".up.sql") {
		t.Fatalf("expected up migration name, got %q", migrations[0].name)
	}
}

func TestSplitSQLStatements(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "strings and identifiers",
			sql:  "SELECT 'it''s; fine', \"semi;colon\"; SELECT 2;",
			want: []string{"SELECT 'it''s; fine', \"semi;colon\"", "SELECT 2"},
		},
		{
			name: "dollar quoted bodies",
			sql:  "DO $$ BEGIN RAISE NOTICE 'one; two'; END $$; SELECT $tag$three; four$tag$;",
			want: []string{"DO $$ BEGIN RAISE NOTICE 'one; two'; END $$", "SELECT $tag$three; four$tag$"},
		},
		{
			name: "comments",
			sql:  "-- keep ; here\nSELECT 1; /* and ; here /* nested ; */ */ SELECT 2; ; \n\t",
			want: []string{"-- keep ; here\nSELECT 1", "/* and ; here /* nested ; */ */ SELECT 2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitSQLStatements(tt.sql)
			if len(got) != len(tt.want) {
				t.Fatalf("splitSQLStatements(%q) = %#v, want %#v", tt.sql, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("statement %d = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestApplyNonTransactionalMigration(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	applyMigrationsThrough(t, ctx, pool, 1)

	// The second statement is rejected when sent alongside the first in one Exec,
	// because PostgreSQL implicitly wraps a multi-statement request in a
	// transaction. Success proves the runner executes non-transactional
	// migrations statement by statement.
	migration := pendingMigration{
		version:       1000,
		name:          "1000_test_concurrent_index.up.sql",
		noTransaction: true,
		content: `DROP INDEX IF EXISTS messages_content_nontransactional_test;
CREATE INDEX CONCURRENTLY messages_content_nontransactional_test
			ON messages (content);`,
	}
	if err := applyMigration(ctx, pool, migration); err != nil {
		t.Fatalf("apply non-transactional migration: %v", err)
	}
	var valid bool
	if err := pool.QueryRow(ctx, `
		SELECT i.indisvalid
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		WHERE c.relname = 'messages_content_nontransactional_test'
	`).Scan(&valid); err != nil {
		t.Fatalf("read non-transactional index: %v", err)
	}
	if !valid {
		t.Fatal("non-transactional index is invalid")
	}
	var applied bool
	if err := pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 1000)",
	).Scan(&applied); err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Fatal("non-transactional migration was not recorded")
	}
}

func TestFailedNonTransactionalMigrationIsNotRecordedAndCanRetry(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	applyMigrationsThrough(t, ctx, pool, 1)

	migration := pendingMigration{
		version:       1001,
		name:          "1001_test_nontransactional_retry.up.sql",
		noTransaction: true,
		content: `CREATE TABLE IF NOT EXISTS nontransactional_migration_retry_test (id integer);
CREATE INDEX CONCURRENTLY nontransactional_migration_retry_test_idx ON messages (missing_column);`,
	}
	if err := applyMigration(ctx, pool, migration); err == nil {
		t.Fatal("failed non-transactional migration unexpectedly succeeded")
	}
	var recorded bool
	if err := pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)", migration.version,
	).Scan(&recorded); err != nil {
		t.Fatal(err)
	}
	if recorded {
		t.Fatal("failed non-transactional migration was recorded")
	}

	migration.content = `CREATE TABLE IF NOT EXISTS nontransactional_migration_retry_test (id integer);
CREATE INDEX CONCURRENTLY nontransactional_migration_retry_test_idx ON messages (content);`
	if err := applyMigration(ctx, pool, migration); err != nil {
		t.Fatalf("retry non-transactional migration: %v", err)
	}
	if err := pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)", migration.version,
	).Scan(&recorded); err != nil {
		t.Fatal(err)
	}
	if !recorded {
		t.Fatal("retried non-transactional migration was not recorded")
	}
}

func applyMigrationsThrough(t *testing.T, ctx context.Context, pool *pgxpool.Pool, maxVersion int) {
	t.Helper()
	if _, err := pool.Exec(ctx, migrationBookkeepingSchema); err != nil {
		t.Fatalf("initialize migration bookkeeping: %v", err)
	}
	migrations, err := embeddedUpMigrations()
	if err != nil {
		t.Fatalf("read migrations: %v", err)
	}
	for _, migration := range migrations {
		if migration.version > maxVersion {
			break
		}
		if err := applyMigration(ctx, pool, migration); err != nil {
			t.Fatalf("apply migration %d: %v", migration.version, err)
		}
	}
}
