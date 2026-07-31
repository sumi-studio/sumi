package migrations

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed sql/*.sql
var files embed.FS

func Run(ctx context.Context, pool *pgxpool.Pool) error {
	entries, err := fs.ReadDir(files, "sql")
	if err != nil {
		return fmt.Errorf("read embedded migrations: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	return pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
CREATE TABLE IF NOT EXISTS schema_migrations (
  name TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`); err != nil {
			return fmt.Errorf("create schema migrations table: %w", err)
		}
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", int64(0x53554d49544f444f)); err != nil {
			return fmt.Errorf("lock migrations: %w", err)
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			var applied bool
			if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE name = $1)", entry.Name()).Scan(&applied); err != nil {
				return fmt.Errorf("check migration %s: %w", entry.Name(), err)
			}
			if applied {
				continue
			}
			sql, err := files.ReadFile("sql/" + entry.Name())
			if err != nil {
				return fmt.Errorf("read migration %s: %w", entry.Name(), err)
			}
			if _, err := tx.Exec(ctx, string(sql)); err != nil {
				return fmt.Errorf("apply migration %s: %w", entry.Name(), err)
			}
			if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations (name) VALUES ($1)", entry.Name()); err != nil {
				return fmt.Errorf("record migration %s: %w", entry.Name(), err)
			}
		}
		return nil
	})
}
