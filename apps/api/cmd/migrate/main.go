// Command migrate applies embedded control-plane schema migrations to the
// Postgres database named by SUMI_DB_URL. It is idempotent: re-running against
// an up-to-date database is a no-op. The API server runs the same migrations on
// startup; this binary exists for standalone/CI use against an empty database.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/db"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	mode := "apply"
	if len(args) > 1 {
		return errors.New("usage: sumi-migrate [apply|verify|status|manifest]")
	}
	if len(args) == 1 {
		mode = strings.TrimSpace(args[0])
	}
	if mode == "manifest" {
		entries, digest, err := db.EmbeddedMigrationManifest()
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(struct {
			ManifestSHA256 string                      `json:"manifest_sha256"`
			Migrations     []db.MigrationManifestEntry `json:"migrations"`
		}{ManifestSHA256: digest, Migrations: entries})
	}
	if mode != "apply" && mode != "verify" && mode != "status" {
		return fmt.Errorf("unknown mode %q; usage: sumi-migrate [apply|verify|status|manifest]", mode)
	}

	databaseURL := strings.TrimSpace(os.Getenv("SUMI_DB_URL"))
	if databaseURL == "" {
		return errors.New("SUMI_DB_URL is required")
	}
	pool, err := db.Open(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer pool.Close()
	if mode == "apply" {
		if err := db.Migrate(ctx, pool.Pool); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	status, verifyErr := db.MigrationManifestStatus(ctx, pool.Pool)
	if mode == "status" || mode == "apply" {
		if err := json.NewEncoder(output).Encode(status); err != nil {
			return fmt.Errorf("write migration status: %w", err)
		}
	}
	if verifyErr != nil {
		return fmt.Errorf("verify migrations: %w", verifyErr)
	}
	version, err := db.LatestAppliedVersion(ctx, pool.Pool)
	if err != nil {
		return fmt.Errorf("read latest version: %w", err)
	}
	log.Printf("migration manifest %s; latest version %d", mode, version)
	return nil
}
