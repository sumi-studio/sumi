// production-bootstrap admits the first person into a completely empty Sumi
// install. Enrollment is invitation-only and a new install has no Human who
// could issue an invitation, so an operator with database access mints the
// first, email-bound invitation here. Once any Human exists the bootstrap is
// closed for good: later invitations come from an enrollment admin in the
// product.
//
//	production-bootstrap invite -email ADDR -token-out PATH [-ttl 24h] [-origin URL]
//	    Issue the bootstrap invitation. The raw token is written only to
//	    PATH (created new, mode 0600; "-" writes it to stdout instead). With
//	    -origin the file also holds the sign-up link ORIGIN/#invite=TOKEN.
//	    Re-running while no Human exists revokes the earlier unused token.
//	production-bootstrap status
//	    Print JSON: whether the bootstrap is open, the outstanding bootstrap
//	    invitation and, after the first registration, the Human it admitted
//	    (the ID to put in SUMI_ENROLLMENT_ADMIN_HUMAN_IDS).
//
// SUMI_DB_URL names the already-migrated control-plane database. The
// command never migrates, never prints the database URL and never prints a
// token except to the explicit -token-out destination.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "production-bootstrap:", err)
		os.Exit(1)
	}
}

const usage = "usage: production-bootstrap invite -email ADDR -token-out PATH [-ttl 24h] [-origin URL] | status"

func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	switch args[0] {
	case "invite":
		return runInvite(ctx, args[1:], getenv, stdout, stderr)
	case "status":
		if len(args) != 1 {
			return errors.New(usage)
		}
		return runStatus(ctx, getenv, stdout)
	default:
		return errors.New(usage)
	}
}

func openStore(ctx context.Context, getenv func(string) string) (*koseki.Store, func(), error) {
	databaseURL := strings.TrimSpace(getenv("SUMI_DB_URL"))
	if databaseURL == "" {
		return nil, nil, errors.New("SUMI_DB_URL is required")
	}
	pool, err := db.Open(ctx, databaseURL)
	if err != nil {
		// db.Open errors never include the URL itself.
		return nil, nil, err
	}
	return koseki.New(pool.Pool), pool.Close, nil
}

func runInvite(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("invite", flag.ContinueOnError)
	fs.SetOutput(stderr)
	email := fs.String("email", "", "verified email of the first person (required)")
	tokenOut := fs.String("token-out", "", `new file for the raw token, created with mode 0600; "-" for stdout (required)`)
	ttl := fs.Duration("ttl", koseki.DefaultBootstrapInviteTTL, "invitation lifetime (10m to 72h)")
	origin := fs.String("origin", "", "optional web origin, e.g. https://sumi.example; adds the sign-up link to the token output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || strings.TrimSpace(*email) == "" || *tokenOut == "" {
		return errors.New(usage)
	}
	if *ttl < koseki.MinBootstrapInviteTTL || *ttl > koseki.MaxBootstrapInviteTTL {
		return koseki.ErrBootstrapInvite
	}
	if _, err := koseki.NormalizeEmail(*email); err != nil {
		return koseki.ErrBootstrapInvite
	}
	base, err := parseOrigin(*origin)
	if err != nil {
		return err
	}

	// Claim the destination before issuing: a token must never be committed
	// to the database with nowhere to go.
	var out io.Writer = stdout
	var file *os.File
	if *tokenOut != "-" {
		file, err = os.OpenFile(*tokenOut, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return fmt.Errorf("create token file: %w", err)
		}
		out = file
	}
	issued := false
	defer func() {
		if file == nil {
			return
		}
		file.Close()
		if !issued {
			os.Remove(file.Name())
		}
	}()

	store, closeStore, err := openStore(ctx, getenv)
	if err != nil {
		return err
	}
	defer closeStore()
	invite, token, err := store.IssueBootstrapEnrollmentInvite(ctx, *email, *ttl)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23502" && pgErr.ColumnName == "issued_by" {
		return errors.New("the database schema does not accept bootstrap invitations (enrollment_invites.issued_by is NOT NULL)")
	}
	if err != nil {
		return err
	}
	issued = true

	body := token + "\n"
	if base != "" {
		body += base + "/#invite=" + token + "\n"
	}
	if _, err := io.WriteString(out, body); err != nil {
		return fmt.Errorf("write token (invitation %s is issued; re-run to supersede it): %w", invite.ID, err)
	}
	if file != nil {
		if err := file.Sync(); err != nil {
			return fmt.Errorf("sync token file (invitation %s is issued; re-run to supersede it): %w", invite.ID, err)
		}
	}
	summary := fmt.Sprintf("issued bootstrap invitation %s for %s, expires %s", invite.ID, invite.Email, invite.ExpiresAt.UTC().Format(time.RFC3339))
	if invite.Superseded > 0 {
		summary += fmt.Sprintf("; revoked %d earlier unused bootstrap invitation(s)", invite.Superseded)
	}
	// With -token-out - stdout carries the token; keep the summary apart.
	fmt.Fprintln(stderr, summary)
	return nil
}

// parseOrigin accepts an https origin, or http for a loopback host, with no
// path, query or fragment.
func parseOrigin(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	u, err := url.Parse(raw)
	bad := errors.New("-origin must be an origin such as https://sumi.example")
	if err != nil || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return "", bad
	}
	switch u.Scheme {
	case "https":
	case "http":
		host := u.Hostname()
		if host != "localhost" && host != "127.0.0.1" && host != "::1" {
			return "", bad
		}
	default:
		return "", bad
	}
	return u.Scheme + "://" + u.Host, nil
}

func runStatus(ctx context.Context, getenv func(string) string, stdout io.Writer) error {
	store, closeStore, err := openStore(ctx, getenv)
	if err != nil {
		return err
	}
	defer closeStore()
	status, err := store.EnrollmentBootstrapStatus(ctx)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(status)
}
