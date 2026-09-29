package db

import (
	"context"
	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
	"testing"
)

func TestAgenticMemoryMigrationWithdrawsOnlyUnappliedOneResponseResults(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Create(t)
	if _, e := pool.Exec(ctx, migrationBookkeepingSchema); e != nil {
		t.Fatal(e)
	}
	migrations, e := embeddedUpMigrations()
	if e != nil {
		t.Fatal(e)
	}
	for _, m := range migrations {
		if m.version < 3 {
			if e = applyMigration(ctx, pool, m); e != nil {
				t.Fatal(e)
			}
		}
	}
	id := uuid.Must(uuid.NewV7()).String()
	if _, e = pool.Exec(ctx, `INSERT INTO core_personas(persona_id) VALUES($1)`, id); e != nil {
		t.Fatal(e)
	}
	for i, status := range []string{"sealed", "preparing", "prepared", "kept", "failed", "applied", "superseded"} {
		if _, e = pool.Exec(ctx, `INSERT INTO core_memory_chunks(persona_id,chunk_seq,first_seq,last_seq,est_tokens,status,replacement,replacement_est_tokens) VALUES($1,$2,$2,$2,100,$3,'old candidate',3)`, id, i+1, status); e != nil {
			t.Fatal(e)
		}
	}
	if e = Migrate(ctx, pool); e != nil {
		t.Fatal(e)
	}
	for i, status := range []string{"sealed", "sealed", "sealed", "sealed", "sealed", "applied", "superseded"} {
		var got string
		var replacement *string
		if e = pool.QueryRow(ctx, `SELECT status,replacement FROM core_memory_chunks WHERE persona_id=$1 AND chunk_seq=$2`, id, i+1).Scan(&got, &replacement); e != nil {
			t.Fatal(e)
		}
		if got != status {
			t.Fatalf("chunk %d: %s", i+1, got)
		}
		if i >= 1 && i <= 4 && replacement != nil {
			t.Fatal("unreviewed old result survives")
		}
		if i >= 5 && (replacement == nil || *replacement != "old candidate") {
			t.Fatal("already applied memory changed")
		}
	}
}
