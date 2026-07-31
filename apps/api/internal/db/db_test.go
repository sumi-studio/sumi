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
	// Reset to a truly empty database so the test is reproducible against a
	// persistent dev volume: drop the public schema and recreate it.
	if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS public CASCADE"); err != nil {
		t.Fatalf("drop public schema: %v", err)
	}
	if _, err := pool.Exec(ctx, "CREATE SCHEMA public"); err != nil {
		t.Fatalf("recreate public schema: %v", err)
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

// TestKosekiSchemaConstraints verifies the 戸籍 invariants from issue #119
// against a migrated database: a credential cannot be rebound to a different
// Human, and an agent has at most one active Employer at a time.
func TestKosekiSchemaConstraints(t *testing.T) {
	databaseURL := os.Getenv("SUMI_TEST_DB_URL")
	if databaseURL == "" {
		t.Skip("SUMI_TEST_DB_URL not set; skipping Postgres integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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

	if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS public CASCADE"); err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	if _, err := pool.Exec(ctx, "CREATE SCHEMA public"); err != nil {
		t.Fatalf("recreate schema: %v", err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	const human1 = "0198f0f4-9b72-7000-8000-000000000001"
	const human2 = "0198f0f4-9b72-7000-8000-000000000002"
	const agentID = "0198f0f4-9b72-7000-8000-00000000000a"
	for _, h := range []string{human1, human2} {
		if _, err := pool.Exec(ctx, "INSERT INTO humans (human_id) VALUES ($1)", h); err != nil {
			t.Fatalf("insert human %s: %v", h, err)
		}
	}
	if _, err := pool.Exec(ctx, "INSERT INTO agents (personality_agent_id, human_id) VALUES ($1, $2)", agentID, human1); err != nil {
		t.Fatalf("insert agent: %v", err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO credentials (provider, external_subject, human_id) VALUES ('firebase', 'uid-aaa', $1)", human1); err != nil {
		t.Fatalf("insert credential: %v", err)
	}

	// Credential rebinding to a different Human must fail.
	_, err = pool.Exec(ctx, "UPDATE credentials SET human_id = $1 WHERE provider='firebase' AND external_subject='uid-aaa'", human2)
	if err == nil {
		t.Fatal("expected credential rebinding to fail, but it succeeded")
	}

	// First active employment succeeds.
	if _, err := pool.Exec(ctx, "INSERT INTO employments (agent_id, employer_type, employer_id) VALUES ($1, 'human', $2)", agentID, human1); err != nil {
		t.Fatalf("insert first employment: %v", err)
	}
	// Second active employment for the same agent must fail.
	_, err = pool.Exec(ctx, "INSERT INTO employments (agent_id, employer_type, employer_id) VALUES ($1, 'human', $2)", agentID, human2)
	if err == nil {
		t.Fatal("expected second active employer to be rejected, but it succeeded")
	}
	// Closing the current employment and opening a new one (異動) must succeed.
	if _, err := pool.Exec(ctx, "UPDATE employments SET ended_at = now() WHERE agent_id=$1 AND ended_at IS NULL", agentID); err != nil {
		t.Fatalf("close employment: %v", err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO employments (agent_id, employer_type, employer_id) VALUES ($1, 'human', $2)", agentID, human2); err != nil {
		t.Fatalf("insert employment after transfer: %v", err)
	}
	// The closed employment does not block a query for the active employer.
	var activeCount int
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM employments WHERE agent_id=$1 AND ended_at IS NULL", agentID).Scan(&activeCount); err != nil {
		t.Fatalf("count active employments: %v", err)
	}
	if activeCount != 1 {
		t.Fatalf("expected exactly 1 active employment after transfer, got %d", activeCount)
	}
}
