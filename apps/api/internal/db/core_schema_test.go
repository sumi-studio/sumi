package db

import (
	"context"
	"errors"
	"testing"

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
	if humans != 0 || migrations != 1 || catalog == 0 || retired != nil {
		t.Fatalf("foundation humans=%d migrations=%d catalog=%d retired=%v", humans, migrations, catalog, retired)
	}
	if _, err := pool.Exec(ctx, `UPDATE schema_migrations SET checksum='unrelated-schema'`); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool); !errors.Is(err, ErrMigrationChecksumMismatch) {
		t.Fatalf("changed history accepted: %v", err)
	}
}
