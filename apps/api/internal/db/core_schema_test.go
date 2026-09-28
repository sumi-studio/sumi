package db

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

func TestCoreFoundationFromEmptyDatabase(t *testing.T) {
	pool := testdb.Create(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("repeat migration: %v", err)
	}
	var humans, migrations, catalog int
	var retired *string
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM humans), (SELECT count(*) FROM schema_migrations), (SELECT count(*) FROM app_catalog), to_regclass('public.agent_secrets')::text`).Scan(&humans, &migrations, &catalog, &retired); err != nil {
		t.Fatal(err)
	}
	embedded, err := embeddedUpMigrations()
	if err != nil {
		t.Fatal(err)
	}
	if humans != 0 || migrations != len(embedded) || catalog == 0 || retired != nil {
		t.Fatalf("foundation humans=%d migrations=%d catalog=%d retired=%v", humans, migrations, catalog, retired)
	}
	if _, err := pool.Exec(ctx, `UPDATE schema_migrations SET checksum='unrelated-schema'`); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool); !errors.Is(err, ErrMigrationChecksumMismatch) {
		t.Fatalf("changed history accepted: %v", err)
	}
}

// A database already serving users sits at an earlier prefix of the embedded
// history. Later migrations must apply on top of it without touching its rows.
func TestLaterMigrationsKeepExistingFoundationRows(t *testing.T) {
	pool := testdb.Create(t)
	ctx := context.Background()
	applyMigrationsThrough(t, ctx, pool, 1)
	humanID := uuid.Must(uuid.NewV7()).String()
	if _, err := pool.Exec(ctx, `INSERT INTO humans (human_id, display_name) VALUES ($1, 'Existing user')`, humanID); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate an existing foundation: %v", err)
	}
	embedded, err := embeddedUpMigrations()
	if err != nil {
		t.Fatal(err)
	}
	var migrations int
	var name string
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM schema_migrations), (SELECT display_name FROM humans WHERE human_id = $1)`, humanID).Scan(&migrations, &name); err != nil {
		t.Fatal(err)
	}
	if migrations != len(embedded) || name != "Existing user" {
		t.Fatalf("after later migrations: migrations=%d/%d human=%q", migrations, len(embedded), name)
	}
}

func TestCoreFoundationWithDBAOwnedExtension(t *testing.T) {
	// Only this fixture needs DBA privileges to create the extension and a
	// separate schema owner. The migration itself still runs as that owner;
	// other integration tests keep using the constrained test database role.
	adminURL := strings.TrimSpace(os.Getenv("SUMI_TEST_ADMIN_DB_URL"))
	if adminURL == "" {
		t.Skip("SUMI_TEST_ADMIN_DB_URL not set; skipping DBA-owned extension fixture")
	}
	t.Setenv("SUMI_TEST_DB_URL", adminURL)
	pool := testdb.CreateWithMaxConns(t, 1)
	ctx := context.Background()
	role := "core_schema_owner_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	var database string
	if err := pool.QueryRow(ctx, `SELECT current_database()`).Scan(&database); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE EXTENSION pg_trgm`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE ROLE `+role); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(ctx, `RESET ROLE; DROP OWNED BY `+role+` CASCADE; DROP ROLE `+role); err != nil {
			t.Error(err)
		}
	})
	if _, err := pool.Exec(ctx, `GRANT CREATE ON DATABASE "`+database+`" TO `+role+`; GRANT USAGE, CREATE ON SCHEMA public TO `+role+`; SET ROLE `+role); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("non-owner migration with precreated extension: %v", err)
	}
}
