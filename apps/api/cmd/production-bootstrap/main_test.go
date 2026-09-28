package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

// migratedURL returns the URL of an empty, migrated database. The bootstrap
// DDL is applied here until the consolidated schema carries it (see
// reset-bootstrap/SCHEMA-REQUEST.md); both statements are then no-ops.
func migratedURL(t *testing.T) (string, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	pool := testdb.Create(t)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		ALTER TABLE enrollment_invites ALTER COLUMN issued_by DROP NOT NULL;
		CREATE UNIQUE INDEX IF NOT EXISTS enrollment_invites_one_open_bootstrap
			ON enrollment_invites ((true))
			WHERE issued_by IS NULL AND consumed_at IS NULL AND revoked_at IS NULL`); err != nil {
		t.Fatal(err)
	}
	return pool.Config().ConnString(), ctx
}

func TestInviteWritesTokenOnlyToPrivateFile(t *testing.T) {
	url, ctx := migratedURL(t)
	env := func(k string) string {
		if k == "SUMI_DB_URL" {
			return url
		}
		return ""
	}
	path := filepath.Join(t.TempDir(), "invite")
	var stdout, stderr bytes.Buffer
	if err := run(ctx, []string{"invite", "-email", "First@Example.com", "-token-out", path, "-origin", "https://sumi.example"}, env, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("token file %v %v", info, err)
	}
	raw, _ := os.ReadFile(path)
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 || len(lines[0]) != 43 || lines[1] != "https://sumi.example/#invite="+lines[0] {
		t.Fatalf("token file shape: %d lines", len(lines))
	}
	token := lines[0]
	if strings.Contains(stdout.String()+stderr.String(), token) || stdout.Len() != 0 {
		t.Fatal("token leaked outside the token file")
	}
	if !strings.Contains(stderr.String(), "first@example.com") {
		t.Fatalf("summary: %q", stderr.String())
	}

	// An existing destination is never overwritten, and nothing is issued.
	stderr.Reset()
	if err := run(ctx, []string{"invite", "-email", "first@example.com", "-token-out", path}, env, &stdout, &stderr); err == nil {
		t.Fatal("existing token file overwritten")
	}
	var status koseki.BootstrapStatus
	stdout.Reset()
	if err := run(ctx, []string{"status"}, env, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(stdout.Bytes(), &status); err != nil || !status.Open || status.Outstanding == nil {
		t.Fatalf("status %s %v", stdout.String(), err)
	}
	if strings.Contains(stdout.String(), token) {
		t.Fatal("status printed the token")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(after, raw) {
		t.Fatal("refused run changed the token file")
	}
}

func TestInviteMisuseIssuesNothingAndLeavesNoFile(t *testing.T) {
	url, ctx := migratedURL(t)
	env := func(k string) string {
		if k == "SUMI_DB_URL" {
			return url
		}
		return ""
	}
	dir := t.TempDir()
	for _, args := range [][]string{
		{"invite", "-token-out", filepath.Join(dir, "a")},                                            // no email
		{"invite", "-email", "x@example.com"},                                                        // no destination
		{"invite", "-email", "x@example.com", "-ttl", "100h", "-token-out", filepath.Join(dir, "b")}, // too long
		{"invite", "-email", "x@example.com", "-origin", "http://sumi.example", "-token-out", filepath.Join(dir, "c")},
		{"status", "extra"},
		{"reopen"},
	} {
		var out, errOut bytes.Buffer
		if err := run(ctx, args, env, &out, &errOut); err == nil {
			t.Fatalf("%v accepted", args)
		}
	}
	// A missing database URL fails after claiming the file; the claim is released.
	var out, errOut bytes.Buffer
	if err := run(ctx, []string{"invite", "-email", "x@example.com", "-token-out", filepath.Join(dir, "d")}, func(string) string { return "" }, &out, &errOut); err == nil {
		t.Fatal("ran without SUMI_DB_URL")
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatalf("misuse left %d file(s)", len(entries))
	}
	out.Reset()
	if err := run(ctx, []string{"status"}, env, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	var status koseki.BootstrapStatus
	if err := json.Unmarshal(out.Bytes(), &status); err != nil || status.Outstanding != nil {
		t.Fatalf("misuse issued an invitation: %s", out.String())
	}
}
