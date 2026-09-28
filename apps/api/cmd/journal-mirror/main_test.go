package main

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

// F1: init-empty initializes a new installation once and never changes an
// initialized mirror; it refuses a database with accounts, whose journals
// must be adopted from the host that holds them.
func TestInitEmptyOnlyForNewInstallations(t *testing.T) {
	ctx := context.Background()
	fresh := testdb.Create(t)
	t.Setenv("SUMI_DB_URL", fresh.Config().ConnString())
	if code := run([]string{"init-empty"}); code != 0 {
		t.Fatalf("init-empty on a new installation = %d", code)
	}
	var lineages string
	const query = `SELECT string_agg(dir || '=' || lineage || ':' || initialized_how, ',' ORDER BY dir) FROM api_journal_mirror_dirs`
	if err := fresh.QueryRow(ctx, query).Scan(&lineages); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"init-empty"}); code != 0 {
		t.Fatalf("repeated init-empty = %d", code)
	}
	var again string
	if err := fresh.QueryRow(ctx, query).Scan(&again); err != nil || again != lineages {
		t.Fatalf("repeated init-empty changed the mirror: %q -> %q (%v)", lineages, again, err)
	}

	existing := testdb.Create(t)
	t.Setenv("SUMI_DB_URL", existing.Config().ConnString())
	if code := run([]string{"init-empty"}); code != 0 { // migrates; no accounts yet
		t.Fatalf("init-empty = %d", code)
	}
	if _, err := existing.Exec(ctx, `DELETE FROM api_journal_mirror_dirs`); err != nil {
		t.Fatal(err)
	}
	if _, err := existing.Exec(ctx, `INSERT INTO humans (human_id, display_name) VALUES ($1, 'Existing user')`, uuid.Must(uuid.NewV7()).String()); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"init-empty"}); code != 1 {
		t.Fatalf("init-empty with an existing account = %d, want refusal", code)
	}
	var n int
	if err := existing.QueryRow(ctx, `SELECT count(*) FROM api_journal_mirror_dirs`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("refused init-empty initialized %d directories (%v)", n, err)
	}
}
