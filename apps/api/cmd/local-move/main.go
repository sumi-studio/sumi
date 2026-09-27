// Command local-move brings this Local secretary into a Sumi Cloud
// registration: it seals the secretary on the Local database, uploads the
// sealed state to the Cloud transfer session, and ends Local authority only
// with the Cloud's proof — transferred after Cloud activated it, or active
// again after Cloud retired the transfer.
//
//	sumi-local-move start          read the move URL from stdin, then continue
//	sumi-local-move resume         continue the move recorded in the state home
//	sumi-local-move cancel         cancel before the Cloud account is created
//	sumi-local-move status         show Local authority and the Cloud session
//	sumi-local-move return         read the return URL from stdin, bring home
//	sumi-local-move return-resume  continue a recorded or pending return
//	sumi-local-move return-status  show where the return stands
//	sumi-local-move return-cancel  give up the recorded return
//
// Env (deploy/local-host/sumi-local-move supplies them from config.env):
//
//	SUMI_LOCAL_HOME    state home; progress is kept in <home>/move (0700)
//	SUMI_DB_URL        this Local placement's database
//	SUMI_PERSONA_ID    the secretary
//	SUMI_LOCAL_CONFIG  this install's config.env — used by `return` to
//	                   retarget an empty slot to the secretary that came
//	                   home, only after its activation committed
//	SUMI_WORKSPACE_ROOT  this install's local file store root — needed
//	                   only when a return carries the Cloud workspace
//	                   to local file storage
//
// The move and return URLs carry the Cloud-issued grant in the fragment. It
// is read from stdin (never argv), stored only in the 0600 state file, and
// sent only as a bearer header. No Local administrator secret is sent to
// Cloud.
//
// Exit status: 0 finished, 3 not finished yet (run resume later), 1 error,
// 2 usage.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/portable"
)

const (
	exitDone    = 0
	exitError   = 1
	exitUsage   = 2
	exitPending = 3
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr, os.Getenv))
}

func usage(w io.Writer) {
	fmt.Fprint(w, `usage: sumi-local-move <command> [flags]

  start          paste the move URL shown during Sumi Cloud registration (stdin)
  resume         continue an interrupted move
  cancel         cancel the move before the Cloud account is created
  status         show where the move stands

  return         paste the return URL shown in Sumi Cloud (stdin), then bring
                 the secretary home to this install
  return-resume  continue a recorded or pending return
  return-status  show where the return stands
  return-cancel  give up the recorded return

  --wait             how long start/resume wait for the Cloud registration to
                     finish before exiting with status 3 (default 30m)
  --use-config-model after the secretary is active here, keep this install's
                     configured provider/model: the carried model selection is
                     cleared explicitly instead of needing a human-bound
                     connection on this install
`)
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string) int {
	if len(args) == 0 {
		usage(stderr)
		return exitUsage
	}
	cmd := args[0]
	fs := flag.NewFlagSet("sumi-local-move", flag.ContinueOnError)
	fs.SetOutput(stderr)
	wait := fs.Duration("wait", 30*time.Minute, "wait for the Cloud registration")
	useConfMdl := fs.Bool("use-config-model", false, "keep this install's configured provider/model for a returned secretary")
	if err := fs.Parse(args[1:]); err != nil || fs.NArg() > 0 {
		usage(stderr)
		return exitUsage
	}
	switch cmd {
	case "start", "resume", "cancel", "status",
		"return", "return-resume", "return-status", "return-cancel":
	case "-h", "--help", "help":
		usage(stdout)
		return exitDone
	default:
		usage(stderr)
		return exitUsage
	}
	home, dbURL, personaID := getenv("SUMI_LOCAL_HOME"), getenv("SUMI_DB_URL"), getenv("SUMI_PERSONA_ID")
	if home == "" || dbURL == "" || personaID == "" {
		fmt.Fprintln(stderr, "sumi-local-move: SUMI_LOCAL_HOME, SUMI_DB_URL and SUMI_PERSONA_ID are required (run it through deploy/local-host/sumi-local-move)")
		return exitUsage
	}
	pool, err := db.Open(ctx, dbURL)
	if err != nil {
		fmt.Fprintf(stderr, "sumi-local-move: open the Local database: %v\n", err)
		return exitError
	}
	defer pool.Close()
	// A fresh install may never have run `start`, leaving its database
	// without the schema — and an unstarted install is exactly the
	// documented fresh target for a return. The schema is this install's
	// own and the migration is idempotent, so apply it rather than fail
	// the slot check on a missing relation.
	if err := db.Migrate(ctx, pool.Pool); err != nil {
		fmt.Fprintf(stderr, "sumi-local-move: migrate the Local database: %v\n", err)
		return exitError
	}
	m := newMover(home, personaID, portable.NewService(pool.Pool), agentstate.NewStore(pool.Pool), stdout)
	m.wait = *wait
	m.wsRoot = getenv("SUMI_WORKSPACE_ROOT")
	config := getenv("SUMI_LOCAL_CONFIG")

	readURL := func(what string) (string, int) {
		if f, ok := stdin.(*os.File); ok {
			if fi, err := f.Stat(); err == nil && fi.Mode()&os.ModeCharDevice != 0 {
				fmt.Fprintf(stderr, "Paste the %s from Sumi Cloud: ", what)
			}
		}
		line, err := bufio.NewReader(stdin).ReadString('\n')
		if err != nil && line == "" {
			fmt.Fprintf(stderr, "sumi-local-move: no %s on stdin\n", what)
			return "", exitUsage
		}
		return strings.TrimSpace(line), 0
	}

	switch cmd {
	case "start":
		raw, code := readURL("move URL")
		if raw == "" {
			return code
		}
		return m.Start(ctx, raw)
	case "resume":
		return m.Resume(ctx)
	case "cancel":
		return m.Cancel(ctx)
	case "status":
		return m.Status(ctx)
	case "return":
		raw, code := readURL("return URL")
		if raw == "" {
			return code
		}
		return m.ReturnStart(ctx, raw, pool.Pool, config, *useConfMdl)
	case "return-resume":
		return m.ReturnResume(ctx, pool.Pool, config, *useConfMdl)
	case "return-status":
		return m.ReturnStatus(ctx, pool.Pool, config)
	default:
		return m.ReturnCancel(ctx, pool.Pool, config)
	}
}
