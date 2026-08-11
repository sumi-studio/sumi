package db

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Once FROZEN.sha256 is sealed immediately before the first real team
// message, every ordinary Go test run makes edits, deletion, and insertion of
// historical migration files fail. Before that product event there is no
// manifest and the intentionally mutable pre-dogfood history remains explicit.
func TestSealedMigrationHistoryIsImmutable(t *testing.T) {
	const manifestPath = "migrations/FROZEN.sha256"
	manifest, err := os.Open(manifestPath)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		t.Fatalf("open migration freeze manifest: %v", err)
	}
	defer manifest.Close()

	expected := map[string]string{}
	scanner := bufio.NewScanner(manifest)
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())
		if len(parts) != 2 || len(parts[0]) != sha256.Size*2 || filepath.Base(parts[1]) != parts[1] {
			t.Fatalf("invalid migration freeze entry %q", scanner.Text())
		}
		if _, exists := expected[parts[1]]; exists {
			t.Fatalf("duplicate migration freeze entry %q", parts[1])
		}
		expected[parts[1]] = parts[0]
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read migration freeze manifest: %v", err)
	}

	entries, err := os.ReadDir("migrations")
	if err != nil {
		t.Fatalf("read migrations: %v", err)
	}
	actualCount := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		actualCount++
		want, ok := expected[entry.Name()]
		if !ok {
			t.Errorf("migration %s was added outside the sealed forward history", entry.Name())
			continue
		}
		content, err := os.ReadFile(filepath.Join("migrations", entry.Name()))
		if err != nil {
			t.Errorf("read migration %s: %v", entry.Name(), err)
			continue
		}
		digest := sha256.Sum256(content)
		if got := hex.EncodeToString(digest[:]); got != want {
			t.Errorf("migration %s changed after freeze: got %s want %s", entry.Name(), got, want)
		}
		delete(expected, entry.Name())
	}
	if len(expected) != 0 {
		for name := range expected {
			t.Errorf("sealed migration %s was deleted", name)
		}
	}
	if actualCount == 0 {
		t.Fatal("sealed migration history is empty")
	}
}
