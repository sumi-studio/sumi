package db

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

var frozenMigrationEntryRE = regexp.MustCompile(`^([0-9a-f]{64})  (([0-9]{4})_([a-z0-9_]+)\.(down|up)\.sql)$`)

type frozenMigrationEntry struct {
	digest    string
	name      string
	version   int
	stem      string
	direction string
}

// TestSealedMigrationHistoryIsImmutable is the repository trust anchor for
// migrations already applied to durable databases. The manifest is mandatory:
// deleting it must never turn this gate into a skip.
func TestSealedMigrationHistoryIsImmutable(t *testing.T) {
	if err := verifyFrozenMigrationHistory("migrations/FROZEN.sha256", "migrations"); err != nil {
		t.Fatal(err)
	}
}

func verifyFrozenMigrationHistory(manifestPath, migrationDir string) error {
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		return fmt.Errorf("read migration freeze manifest: %w", err)
	}
	if len(manifest) == 0 || manifest[len(manifest)-1] != '\n' {
		return fmt.Errorf("migration freeze manifest must be non-empty and end with a newline")
	}

	lines := strings.Split(strings.TrimSuffix(string(manifest), "\n"), "\n")
	sealed := make([]frozenMigrationEntry, 0, len(lines))
	previousName := ""
	for _, line := range lines {
		match := frozenMigrationEntryRE.FindStringSubmatch(line)
		if match == nil {
			return fmt.Errorf("invalid migration freeze entry %q", line)
		}
		name := match[2]
		if previousName != "" && name <= previousName {
			return fmt.Errorf("migration freeze entries are not in canonical filename order: %q after %q", name, previousName)
		}
		version, err := strconv.Atoi(match[3])
		if err != nil {
			return fmt.Errorf("parse frozen migration version %q: %w", match[3], err)
		}
		sealed = append(sealed, frozenMigrationEntry{
			digest:    match[1],
			name:      name,
			version:   version,
			stem:      match[4],
			direction: match[5],
		})
		previousName = name
	}

	type migrationPair struct {
		stem       string
		directions map[string]bool
	}
	pairs := make(map[int]*migrationPair)
	for _, entry := range sealed {
		pair := pairs[entry.version]
		if pair == nil {
			pair = &migrationPair{stem: entry.stem, directions: make(map[string]bool)}
			pairs[entry.version] = pair
		}
		if pair.stem != entry.stem || pair.directions[entry.direction] {
			return fmt.Errorf("frozen migration version %04d must have one matching up/down pair", entry.version)
		}
		pair.directions[entry.direction] = true
	}
	for version, pair := range pairs {
		if len(pair.directions) != 2 || !pair.directions["up"] || !pair.directions["down"] {
			return fmt.Errorf("frozen migration version %04d must have one matching up/down pair", version)
		}
	}

	dirEntries, err := os.ReadDir(migrationDir)
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	actualNames := make([]string, 0, len(sealed))
	for _, entry := range dirEntries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			actualNames = append(actualNames, entry.Name())
		}
	}
	if len(actualNames) != len(sealed) {
		return fmt.Errorf("sealed migration file count changed: got %d want %d", len(actualNames), len(sealed))
	}
	for i, expected := range sealed {
		if actualNames[i] != expected.name {
			return fmt.Errorf("sealed migration path changed at entry %d: got %q want %q", i+1, actualNames[i], expected.name)
		}
		content, err := os.ReadFile(filepath.Join(migrationDir, expected.name))
		if err != nil {
			return fmt.Errorf("read sealed migration %s: %w", expected.name, err)
		}
		digest := sha256.Sum256(content)
		if got := hex.EncodeToString(digest[:]); got != expected.digest {
			return fmt.Errorf("sealed migration %s changed: got %s want %s", expected.name, got, expected.digest)
		}
	}
	return nil
}

func TestVerifyFrozenMigrationHistoryFailsClosed(t *testing.T) {
	dir := t.TempDir()
	err := verifyFrozenMigrationHistory(filepath.Join(dir, "FROZEN.sha256"), dir)
	if err == nil || !strings.Contains(err.Error(), "read migration freeze manifest") {
		t.Fatalf("missing manifest error = %v, want fail-closed read error", err)
	}
}

func TestVerifyFrozenMigrationHistoryRejectsReorderedEntries(t *testing.T) {
	dir := t.TempDir()
	downName := "0001_baseline.down.sql"
	upName := "0001_baseline.up.sql"
	down := []byte("DROP TABLE baseline;\n")
	up := []byte("CREATE TABLE baseline (id bigint);\n")
	if err := os.WriteFile(filepath.Join(dir, downName), down, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, upName), up, 0o600); err != nil {
		t.Fatal(err)
	}
	downDigest := sha256.Sum256(down)
	upDigest := sha256.Sum256(up)
	manifest := fmt.Sprintf("%x  %s\n%x  %s\n", upDigest, upName, downDigest, downName)
	manifestPath := filepath.Join(dir, "FROZEN.sha256")
	if err := os.WriteFile(manifestPath, []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := verifyFrozenMigrationHistory(manifestPath, dir); err == nil || !strings.Contains(err.Error(), "canonical filename order") {
		t.Fatalf("reordered manifest error = %v, want canonical-order failure", err)
	}
}
