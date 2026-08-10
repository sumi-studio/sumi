package testfs

import (
	"os"
	"testing"
)

// PrivateDir returns a temporary directory with permissions suitable for
// security-sensitive runtime state. testing.T.TempDir inherits the process
// umask for its returned child directory and therefore does not itself promise
// owner-only permissions.
func PrivateDir(t testing.TB) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("make temporary directory owner-only: %v", err)
	}
	return dir
}
