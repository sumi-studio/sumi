// Package testdb provides a shared Postgres integration-test helper that
// creates an isolated temporary database per test so parallel test packages do
// not collide on the shared public schema. It is only imported from _test.go
// files. Callers are responsible for running migrations against the returned
// pool.
package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Create returns a pool connected to a freshly created, isolated temporary
// database. It skips the test when SUMI_TEST_DB_URL is unset. The temporary
// database is dropped on test cleanup. The caller must apply migrations.
func Create(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("SUMI_TEST_DB_URL"))
	if databaseURL == "" {
		t.Skip("SUMI_TEST_DB_URL not set; skipping Postgres integration test")
	}
	maintenance, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatalf("connect maintenance pool: %v", err)
	}
	defer maintenance.Close()

	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatalf("generate db suffix: %v", err)
	}
	testDBName := "sumi_test_" + hex.EncodeToString(suffix)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := maintenance.Exec(ctx, fmt.Sprintf(`CREATE DATABASE "%s"`, testDBName)); err != nil {
		t.Fatalf("create test database: %v", err)
	}

	testURL := replaceDatabaseName(databaseURL, testDBName)
	pool, err := pgxpool.New(ctx, testURL)
	if err != nil {
		_, _ = maintenance.Exec(context.Background(), fmt.Sprintf(`DROP DATABASE IF EXISTS "%s" WITH (FORCE)`, testDBName))
		t.Fatalf("connect test pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		_, _ = maintenance.Exec(context.Background(), fmt.Sprintf(`DROP DATABASE IF EXISTS "%s" WITH (FORCE)`, testDBName))
		t.Fatalf("ping test database: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dropCancel()
		if _, err := maintenance.Exec(dropCtx, fmt.Sprintf(`DROP DATABASE IF EXISTS "%s" WITH (FORCE)`, testDBName)); err != nil {
			t.Logf("drop test database %s: %v", testDBName, err)
		}
	})
	return pool
}

// replaceDatabaseName swaps the path component of a Postgres URL to the given
// database name, preserving any query string.
func replaceDatabaseName(databaseURL, name string) string {
	if i := strings.Index(databaseURL, "?"); i >= 0 {
		return swapPathSegment(databaseURL[:i]) + name + databaseURL[i:]
	}
	return swapPathSegment(databaseURL) + name
}

func swapPathSegment(prefix string) string {
	if i := strings.LastIndex(prefix, "/"); i >= 0 {
		return prefix[:i+1]
	}
	return prefix + "/"
}
