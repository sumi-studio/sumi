package db

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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

func TestMigrateIdempotentAgainstEmptyDatabase(t *testing.T) {
	databaseURL := os.Getenv("SUMI_TEST_DB_URL")
	if databaseURL == "" {
		t.Skip("SUMI_TEST_DB_URL not set; skipping Postgres integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	// Clean slate so the test is reproducible against a persistent dev volume.
	if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS schema_migrations"); err != nil {
		t.Fatalf("drop bookkeeping table: %v", err)
	}

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	first, err := LatestAppliedVersion(ctx, pool)
	if err != nil {
		t.Fatalf("latest after first: %v", err)
	}
	if first == 0 {
		t.Fatal("expected non-zero latest version after migrate")
	}
	// Second run must be a no-op and not error.
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	second, err := LatestAppliedVersion(ctx, pool)
	if err != nil {
		t.Fatalf("latest after second: %v", err)
	}
	if first != second {
		t.Fatalf("idempotency broken: first=%d second=%d", first, second)
	}
}
