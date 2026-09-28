package db

import "testing"

// The released foundation is immutable. Add a new numbered forward migration
// for schema changes; do not edit this checksum to accept an old-file rewrite.
func TestCoreFoundationSeal(t *testing.T) {
	const want = "3c507317ffac5b751698926d2defae979b519e656d4e5f322425952e90c3b791"
	raw, err := migrationFS.ReadFile("migrations/0001_core.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if got := migrationChecksum(string(raw)); got != want {
		t.Fatalf("0001_core.up.sql changed: got %s; add a forward migration instead", got)
	}
}
