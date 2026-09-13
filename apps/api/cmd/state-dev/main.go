// Command state-dev serves only the agentstate core-state contract against a
// Postgres database. It exists for local development and isolated tests — the
// same handler is mounted on the API mux when SUMI_CORE_STATE_TOKEN is set —
// and is the honest shape of the Local deployment's state service: local PG,
// same Go service, same schema. The transfer routes (internal/portable) are
// mounted alongside, under the same admin secret.
//
// Env:
//
//	SUMI_DB_URL            postgres connection string (required)
//	SUMI_CORE_STATE_TOKEN  admin/service bearer; also mints persona tokens (required)
//	SUMI_STATE_LISTEN      listen address (default 127.0.0.1:8180)
//	SUMI_DB_CREATE=1       create the database named in SUMI_DB_URL if absent
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/portable"
)

func main() {
	databaseURL := os.Getenv("SUMI_DB_URL")
	if databaseURL == "" {
		log.Fatal("SUMI_DB_URL is required")
	}
	token := os.Getenv("SUMI_CORE_STATE_TOKEN")
	if len(token) < 16 {
		log.Fatal("SUMI_CORE_STATE_TOKEN is required (>=16 chars)")
	}
	listen := os.Getenv("SUMI_STATE_LISTEN")
	if listen == "" {
		listen = "127.0.0.1:8180"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if os.Getenv("SUMI_DB_CREATE") == "1" {
		if err := ensureDatabase(ctx, databaseURL); err != nil {
			log.Fatalf("create database: %v", err)
		}
	}
	pool, err := db.Open(ctx, databaseURL)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool.Pool); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	mux := http.NewServeMux()
	agentstate.NewServer(pool.Pool, token).RegisterRoutes(mux)
	portable.NewServer(pool.Pool, token).RegisterRoutes(mux)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	log.Printf("state-dev listening on http://%s (persona-scoped core state)", listen)
	log.Fatal(http.ListenAndServe(listen, mux))
}

var dbNameRe = regexp.MustCompile(`^[a-z0-9_]{1,63}$`)

// ensureDatabase creates the target database through the server's
// maintenance database, so a fresh Local placement or test run needs no
// separate psql step.
func ensureDatabase(ctx context.Context, databaseURL string) error {
	cfg, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return err
	}
	name := cfg.Database
	if !dbNameRe.MatchString(name) {
		return fmt.Errorf("database name %q must match %s", name, dbNameRe)
	}
	cfg.Database = "postgres"
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, name).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	_, err = conn.Exec(ctx, `CREATE DATABASE "`+name+`"`)
	return err
}
