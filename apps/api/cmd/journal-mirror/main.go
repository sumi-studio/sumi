// Command journal-mirror compares the API's journal directories with the
// PostgreSQL journal mirror (see internal/journalmirror). It is read-only:
//
//	SUMI_DB_URL=... journal-mirror verify --commands DIR --browser-events DIR
//
// It prints one JSON report per directory and exits 1 if any differs. Use
// it to show that a host move carried every journal byte: run it against
// the old host's directories (after that API stopped) and the database the
// new host uses.
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

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 || args[0] != "verify" {
		fmt.Fprintln(os.Stderr, "usage: journal-mirror verify --commands DIR --browser-events DIR")
		return 2
	}
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	commands := flags.String("commands", "", "SUMI_COMMAND_LOG_DIR contents to compare")
	events := flags.String("browser-events", "", "SUMI_BROWSER_EVENT_DIR contents to compare")
	if err := flags.Parse(args[1:]); err != nil {
		return 2
	}
	databaseURL := strings.TrimSpace(os.Getenv("SUMI_DB_URL"))
	if databaseURL == "" || (*commands == "" && *events == "") {
		fmt.Fprintln(os.Stderr, "SUMI_DB_URL and at least one directory are required")
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, err := db.Open(ctx, databaseURL)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
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
