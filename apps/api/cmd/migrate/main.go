// Command migrate applies embedded control-plane schema migrations to the
// Postgres database named by SUMI_DB_URL. It is idempotent: re-running against
// an up-to-date database is a no-op. The API server runs the same migrations on
// startup; this binary exists for standalone/CI use against an empty database.
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/db"
)

func main() {
	databaseURL := os.Getenv("SUMI_DB_URL")
	if databaseURL == "" {
		log.Fatal("SUMI_DB_URL is required")
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
	version, err := db.LatestAppliedVersion(ctx, pool.Pool)
	if err != nil {
		log.Fatalf("read latest version: %v", err)
	}
	log.Printf("migrations applied; latest version %d", version)
}
