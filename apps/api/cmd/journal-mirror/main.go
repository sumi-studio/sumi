// Command journal-mirror initializes and checks the PostgreSQL journal
// mirror (see internal/journalmirror).
//
//	SUMI_DB_URL=... journal-mirror init-empty [--allow-existing-accounts]
//	SUMI_DB_URL=... journal-mirror verify --commands DIR --browser-events DIR
//
// init-empty declares empty journals for a new installation, so a
// restore-only API (SUMI_API_JOURNAL_MIRROR=postgres) can start. It applies
// the migrations first and never changes an initialized mirror. It refuses a
// database that already has accounts or agents, whose journals are on some
// host and must be adopted from there (SUMI_API_JOURNAL_MIRROR=postgres-adopt).
// If init-empty was run by mistake for such a database, that adoption still
// succeeds as long as the empty mirror has acknowledged no write.
//
// verify is read-only. It prints one JSON report per directory and exits 1
// if any differs. Use it to show that a host move carried every journal
// byte: run it against the old host's directories (after that API stopped)
// and the database the new host uses.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/journalmirror"
)

const usage = "usage: journal-mirror init-empty [--allow-existing-accounts] | verify --commands DIR --browser-events DIR"

var logicalDirs = []string{"commands", "browser-events"}

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, usage)
		return 2
	}
	switch args[0] {
	case "init-empty":
		return initEmpty(args[1:])
	case "verify":
		return verify(args[1:])
	default:
		fmt.Fprintln(os.Stderr, usage)
		return 2
	}
}

func openDatabase(ctx context.Context) (*db.Pool, bool) {
	databaseURL := strings.TrimSpace(os.Getenv("SUMI_DB_URL"))
	if databaseURL == "" {
		fmt.Fprintln(os.Stderr, "SUMI_DB_URL is required")
		return nil, false
	}
	pool, err := db.Open(ctx, databaseURL)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return nil, false
	}
	return pool, true
}

func initEmpty(args []string) int {
	flags := flag.NewFlagSet("init-empty", flag.ContinueOnError)
	allowAccounts := flags.Bool("allow-existing-accounts", false,
		"initialize even though accounts or agents exist; only after confirming that no host holds journals for this database")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, ok := openDatabase(ctx)
	if !ok {
		return 1
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool.Pool); err != nil {
		fmt.Fprintln(os.Stderr, "apply migrations:", err)
		return 1
	}
	var accounts, agents int64
	if err := pool.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM humans), (SELECT count(*) FROM agents)`).Scan(&accounts, &agents); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if (accounts > 0 || agents > 0) && !*allowAccounts {
		fmt.Fprintf(os.Stderr, "this database has %d accounts and %d agents: their journals are on the host that served them. "+
			"Start that host once with SUMI_API_JOURNAL_MIRROR=postgres-adopt instead. Nothing was changed.\n", accounts, agents)
		return 1
	}
	hostname, _ := os.Hostname()
	encoder := json.NewEncoder(os.Stdout)
	for _, logical := range logicalDirs {
		created, lineage, err := journalmirror.InitializeEmpty(ctx, pool.Pool, logical, "journal-mirror init-empty on "+hostname)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", logical, err)
			return 1
		}
		_ = encoder.Encode(map[string]any{"dir": logical, "lineage": lineage, "created": created})
	}
	return 0
}

func verify(args []string) int {
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	commands := flags.String("commands", "", "SUMI_COMMAND_LOG_DIR contents to compare")
	events := flags.String("browser-events", "", "SUMI_BROWSER_EVENT_DIR contents to compare")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *commands == "" && *events == "" {
		fmt.Fprintln(os.Stderr, "at least one directory is required")
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	pool, ok := openDatabase(ctx)
	if !ok {
		return 1
	}
	defer pool.Close()
	status := 0
	encoder := json.NewEncoder(os.Stdout)
	for _, dir := range []struct{ name, path string }{{"commands", *commands}, {"browser-events", *events}} {
		if dir.path == "" {
			continue
		}
		report, err := journalmirror.Verify(ctx, pool.Pool, dir.name, dir.path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", dir.name, err)
			status = 1
			continue
		}
		_ = encoder.Encode(struct {
			journalmirror.VerifyReport
			Equal bool `json:"equal"`
		}{report, report.Equal()})
		if !report.Equal() {
			status = 1
		}
	}
	return status
}
