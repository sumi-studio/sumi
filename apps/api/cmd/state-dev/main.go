// Command state-dev serves only the agentstate core-state contract against a
// Postgres database. It exists for local development and isolated tests — the
// same handler is mounted on the API mux when SUMI_CORE_STATE_TOKEN is set —
// and is the honest shape of the Local deployment's state service: local PG,
// same Go service, same schema.
//
// Env:
//
//	SUMI_DB_URL            postgres connection string (required)
//	SUMI_CORE_STATE_TOKEN  admin/service bearer; also mints persona tokens (required)
//	SUMI_STATE_LISTEN      listen address (default 127.0.0.1:8180)
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
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
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	log.Printf("state-dev listening on http://%s (persona-scoped core state)", listen)
	log.Fatal(http.ListenAndServe(listen, mux))
}
