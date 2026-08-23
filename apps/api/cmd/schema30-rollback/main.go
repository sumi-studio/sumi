// Command schema30-rollback is an offline operator tool for the single sealed
// pre-write schema transition 0030 -> 0029. It is not part of API startup.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/db"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Getenv, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "schema30-rollback:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	if len(args) != 1 || (args[0] != "preflight" && args[0] != "apply") {
		return errors.New("operation must be exactly preflight or apply")
	}
	databaseURL := strings.TrimSpace(getenv("SUMI_DB_URL"))
	if databaseURL == "" {
		return errors.New("SUMI_DB_URL is required")
	}
	pool, err := db.Open(ctx, databaseURL)
	if err != nil {
		// Connection errors can contain DSN fields. Keep the operator boundary
		// fail-closed without copying any such detail to stdout/stderr.
		return errors.New("open database failed")
	}
	defer pool.Close()

	switch args[0] {
	case "preflight":
		if err := db.PreflightSchema30Rollback(ctx, pool.Pool); err != nil {
			return fmt.Errorf("preflight refused: %w", err)
		}
		_, _ = fmt.Fprintln(out, "preflight passed: exact schema head 30 is eligible for sealed 0030 -> 0029 rollback")
	case "apply":
		if err := db.RollbackSchema30To29(ctx, pool.Pool); err != nil {
			return classifyApplyError(err)
		}
		_, _ = fmt.Fprintln(out, "rollback committed: exact schema head is 29")
	}
	return nil
}

func classifyApplyError(err error) error {
	if errors.Is(err, db.ErrSchema30RollbackCommitOutcomeUnknown) {
		return err
	}
	return fmt.Errorf("rollback refused: %w", err)
}
