package db

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

func TestEmbeddedUpMigrationsSortedAndUnique(t *testing.T) {
	migrations, err := embeddedUpMigrations()
	if err != nil {
		t.Fatalf("embeddedUpMigrations: %v", err)
	}
	if len(migrations) == 0 {
		t.Fatal("expected at least one embedded migration")
	}
	seen := make(map[int]bool, len(migrations))
	for i, m := range migrations {
		if seen[m.version] {
			t.Fatalf("duplicate migration version %d", m.version)
		}
		seen[m.version] = true
		if i > 0 && migrations[i-1].version >= m.version {
			t.Fatalf("migrations not sorted: %d before %d", migrations[i-1].version, m.version)
		}
		if strings.TrimSpace(m.content) == "" {
			t.Fatalf("migration %d (%s) has empty content", m.version, m.name)
		}
		if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(m.sha256) {
			t.Fatalf("migration %d (%s) has invalid sha256 %q", m.version, m.name, m.sha256)
		}
	}
	if migrations[0].version != 1 {
		t.Fatalf("expected first migration version 1, got %d", migrations[0].version)
	}
	if !strings.HasSuffix(migrations[0].name, ".up.sql") {
		t.Fatalf("expected up migration name, got %q", migrations[0].name)
	}
}

func TestEmbeddedMigrationManifestIsDeterministic(t *testing.T) {
	first, firstDigest, err := EmbeddedMigrationManifest()
	if err != nil {
		t.Fatal(err)
	}
	second, secondDigest, err := EmbeddedMigrationManifest()
	if err != nil {
		t.Fatal(err)
	}
	if len(first) == 0 || len(first) != len(second) {
		t.Fatalf("manifest sizes differ: %d and %d", len(first), len(second))
	}
	if firstDigest != secondDigest || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(firstDigest) {
		t.Fatalf("manifest digest is not deterministic SHA-256: %q vs %q", firstDigest, secondDigest)
	}
	for index := range first {
		if first[index] != second[index] {
			t.Fatalf("manifest entry %d changed: %+v vs %+v", index, first[index], second[index])
		}
	}
}

func TestMigrateIdempotentAgainstEmptyDatabase(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	first, err := LatestAppliedVersion(ctx, pool)
	if err != nil {
		t.Fatalf("latest after first: %v", err)
	}
	if first == 0 {
		t.Fatal("expected non-zero latest version after migrate")
	}
	// Second run must be a no-op and not error.
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	second, err := LatestAppliedVersion(ctx, pool)
	if err != nil {
		t.Fatalf("latest after second: %v", err)
	}
	if first != second {
		t.Fatalf("idempotency broken: first=%d second=%d", first, second)
	}
	status, err := MigrationManifestStatus(ctx, pool)
	if err != nil {
		t.Fatalf("verify manifest: %v", err)
	}
	if !status.Ready || len(status.Pending) != 0 || len(status.Applied) != len(status.Expected) {
		t.Fatalf("unexpected ready manifest status: %+v", status)
	}
}

func TestConcurrentMigrateCallsShareOneSerializedManifestTransition(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const runners = 4
	start := make(chan struct{})
	errorsByRunner := make([]error, runners)
	var wait sync.WaitGroup
	for index := range runners {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			errorsByRunner[index] = Migrate(ctx, pool)
		}()
	}
	close(start)
	wait.Wait()
	for index, err := range errorsByRunner {
		if err != nil {
			t.Fatalf("concurrent migrate runner %d: %v", index, err)
		}
	}
	status, err := MigrationManifestStatus(ctx, pool)
	if err != nil || !status.Ready || len(status.Pending) != 0 {
		t.Fatalf("serialized migration manifest not ready: status=%+v err=%v", status, err)
	}
	connection, err := pgx.Connect(ctx, pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(ctx)
	var lockAvailable bool
	if err := connection.QueryRow(ctx,
		"SELECT pg_try_advisory_lock($1)", migrationAdvisoryLockID,
	).Scan(&lockAvailable); err != nil {
		t.Fatal(err)
	}
	if !lockAvailable {
		t.Fatal("migration returned with its session advisory lock still held")
	}
	if _, err := connection.Exec(ctx,
		"SELECT pg_advisory_unlock($1)", migrationAdvisoryLockID,
	); err != nil {
		t.Fatal(err)
	}
}

func TestMigrateRejectsLegacyVersionOnlyRowsWithoutBlessingThem(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, `
		CREATE TABLE schema_migrations (
			version bigint PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		);
		INSERT INTO schema_migrations(version) VALUES (1)
	`); err != nil {
		t.Fatal(err)
	}
	err := Migrate(ctx, pool)
	if err == nil || !strings.Contains(err.Error(), "reset the pre-cutover database") {
		t.Fatalf("legacy migration history did not require a reset: %v", err)
	}
	var name, digest *string
	if err := pool.QueryRow(ctx,
		"SELECT name, sha256 FROM schema_migrations WHERE version=1",
	).Scan(&name, &digest); err != nil {
		t.Fatal(err)
	}
	if name != nil || digest != nil {
		t.Fatalf("startup blessed unverifiable legacy history: name=%v sha256=%v", name, digest)
	}
}

func TestMigrateRejectsChangedAppliedManifest(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}

	t.Run("content digest", func(t *testing.T) {
		if _, err := pool.Exec(ctx,
			"UPDATE schema_migrations SET sha256=$2 WHERE version=$1",
			1, strings.Repeat("0", 64),
		); err != nil {
			t.Fatal(err)
		}
		if err := VerifyMigrations(ctx, pool); err == nil || !strings.Contains(err.Error(), "manifest mismatch") {
			t.Fatalf("changed digest was not rejected: %v", err)
		}
		if err := Migrate(ctx, pool); err == nil || !strings.Contains(err.Error(), "manifest mismatch") {
			t.Fatalf("migrate accepted changed digest: %v", err)
		}
	})
}

func TestMigrationManifestStatusRejectsDatabaseOnlyVersion(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		"INSERT INTO schema_migrations(version, name, sha256) VALUES ($1, $2, $3)",
		9999, "9999_unknown.up.sql", strings.Repeat("a", 64),
	); err != nil {
		t.Fatal(err)
	}
	if err := VerifyMigrations(ctx, pool); err == nil || !strings.Contains(err.Error(), "absent from the embedded manifest") {
		t.Fatalf("database-only version was not rejected: %v", err)
	}
}

// TestKosekiSchemaConstraints verifies the 戸籍 invariants from issue #119
// against a migrated database: a credential cannot be rebound to a different
// Human, and an agent has at most one active Employer at a time.
func TestKosekiSchemaConstraints(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	const human1 = "0198f0f4-9b72-7000-8000-000000000001"
	const human2 = "0198f0f4-9b72-7000-8000-000000000002"
	const agentID = "0198f0f4-9b72-7000-8000-00000000000a"
	for _, h := range []string{human1, human2} {
		if _, err := pool.Exec(ctx, "INSERT INTO humans (human_id) VALUES ($1)", h); err != nil {
			t.Fatalf("insert human %s: %v", h, err)
		}
	}
	if _, err := pool.Exec(ctx, "INSERT INTO agents (personality_agent_id, human_id) VALUES ($1, $2)", agentID, human1); err != nil {
		t.Fatalf("insert agent: %v", err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO credentials (provider, external_subject, human_id) VALUES ('firebase', 'uid-aaa', $1)", human1); err != nil {
		t.Fatalf("insert credential: %v", err)
	}

	// Credential rebinding to a different Human must fail.
	_, err := pool.Exec(ctx, "UPDATE credentials SET human_id = $1 WHERE provider='firebase' AND external_subject='uid-aaa'", human2)
	if err == nil {
		t.Fatal("expected credential rebinding to fail, but it succeeded")
	}

	// First active employment succeeds.
	if _, err := pool.Exec(ctx, "INSERT INTO employments (agent_id, employer_type, employer_id) VALUES ($1, 'human', $2)", agentID, human1); err != nil {
		t.Fatalf("insert first employment: %v", err)
	}
	// Second active employment for the same agent must fail.
	_, err = pool.Exec(ctx, "INSERT INTO employments (agent_id, employer_type, employer_id) VALUES ($1, 'human', $2)", agentID, human2)
	if err == nil {
		t.Fatal("expected second active employer to be rejected, but it succeeded")
	}
	// Closing the current employment and opening a new one (異動) must succeed.
	if _, err := pool.Exec(ctx, "UPDATE employments SET ended_at = now() WHERE agent_id=$1 AND ended_at IS NULL", agentID); err != nil {
		t.Fatalf("close employment: %v", err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO employments (agent_id, employer_type, employer_id) VALUES ($1, 'human', $2)", agentID, human2); err != nil {
		t.Fatalf("insert employment after transfer: %v", err)
	}
	// The closed employment does not block a query for the active employer.
	var activeCount int
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM employments WHERE agent_id=$1 AND ended_at IS NULL", agentID).Scan(&activeCount); err != nil {
		t.Fatalf("count active employments: %v", err)
	}
	if activeCount != 1 {
		t.Fatalf("expected exactly 1 active employment after transfer, got %d", activeCount)
	}
}

func TestAgentWrappingKeyIdentityMigrationCanonicalizesHistoricalMaterial(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	applyMigrationsThrough(t, ctx, pool, 6)

	const historicalHumanID = "0198f0f4-9b72-7000-8000-000000000011"
	const historicalAgentID = "0198f0f4-9b72-7000-8000-000000000012"
	// Raw URL-safe base64 encoding of bytes 0x00 through 0x1f, exactly as the
	// pre-change Store generated it. It is deliberately not a hex fixture.
	const historicalKey = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"
	const canonicalKey = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	const alreadyHexHumanID = "0198f0f4-9b72-7000-8000-000000000013"
	const alreadyHexAgentID = "0198f0f4-9b72-7000-8000-000000000014"
	const alreadyHexKey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	for _, humanID := range []string{historicalHumanID, alreadyHexHumanID} {
		if _, err := pool.Exec(ctx, "INSERT INTO humans (human_id) VALUES ($1)", humanID); err != nil {
			t.Fatal(err)
		}
	}
	for _, pair := range [][2]string{
		{historicalAgentID, historicalHumanID},
		{alreadyHexAgentID, alreadyHexHumanID},
	} {
		if _, err := pool.Exec(ctx,
			"INSERT INTO agents (personality_agent_id, human_id) VALUES ($1, $2)",
			pair[0], pair[1]); err != nil {
			t.Fatal(err)
		}
	}
	for _, pair := range [][2]string{
		{historicalAgentID, historicalKey},
		{alreadyHexAgentID, alreadyHexKey},
	} {
		if _, err := pool.Exec(ctx,
			"INSERT INTO agent_secrets (personality_agent_id, wrapping_key) VALUES ($1, $2)",
			pair[0], pair[1]); err != nil {
			t.Fatal(err)
		}
	}

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate historical wrapping key: %v", err)
	}

	for _, want := range []struct {
		agentID string
		key     string
	}{
		{historicalAgentID, canonicalKey},
		{alreadyHexAgentID, alreadyHexKey},
	} {
		var key string
		var keyID *string
		if err := pool.QueryRow(ctx,
			"SELECT wrapping_key, wrapping_key_id FROM agent_secrets WHERE personality_agent_id=$1",
			want.agentID).Scan(&key, &keyID); err != nil {
			t.Fatal(err)
		}
		if key != want.key || keyID != nil {
			t.Fatalf("migrated material mismatch for %s: key=%q id=%v", want.agentID, key, keyID)
		}
	}

	if _, err := pool.Exec(ctx,
		"UPDATE agent_secrets SET wrapping_key_id=$2 WHERE personality_agent_id=$1",
		historicalAgentID, " proven-id"); err == nil {
		t.Fatal("invalid wrapping key ID passed the schema constraint")
	}
	if _, err := pool.Exec(ctx,
		"UPDATE agent_secrets SET wrapping_key_id=$2 WHERE personality_agent_id=$1",
		historicalAgentID, "issue75-agent-wrapping/v1"); err != nil {
		t.Fatalf("explicit proven wrapping key ID was rejected: %v", err)
	}
	if _, err := pool.Exec(ctx,
		"UPDATE agent_secrets SET wrapping_key=$2 WHERE personality_agent_id=$1",
		historicalAgentID, historicalKey); err == nil {
		t.Fatal("noncanonical wrapping key passed the schema constraint")
	}
}

func TestAgentWrappingKeyIdentityMigrationRejectsUnknownMaterialAtomically(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	applyMigrationsThrough(t, ctx, pool, 6)

	const humanID = "0198f0f4-9b72-7000-8000-000000000021"
	const agentID = "0198f0f4-9b72-7000-8000-000000000022"
	if _, err := pool.Exec(ctx, "INSERT INTO humans (human_id) VALUES ($1)", humanID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		"INSERT INTO agents (personality_agent_id, human_id) VALUES ($1, $2)",
		agentID, humanID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		"INSERT INTO agent_secrets (personality_agent_id, wrapping_key) VALUES ($1, 'not-a-wrapping-key')",
		agentID); err != nil {
		t.Fatal(err)
	}

	if err := Migrate(ctx, pool); err == nil {
		t.Fatal("migration accepted unknown historical wrapping-key material")
	}
	latest, err := LatestAppliedVersion(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if latest != 6 {
		t.Fatalf("failed migration was recorded: latest=%d", latest)
	}
	var wrappingKeyIDColumnCount int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM information_schema.columns
		WHERE table_schema='public' AND table_name='agent_secrets' AND column_name='wrapping_key_id'
	`).Scan(&wrappingKeyIDColumnCount); err != nil {
		t.Fatal(err)
	}
	if wrappingKeyIDColumnCount != 0 {
		t.Fatal("failed migration did not roll back the wrapping_key_id schema change")
	}
}

func applyMigrationsThrough(t *testing.T, ctx context.Context, pool *pgxpool.Pool, maxVersion int) {
	t.Helper()
	if _, err := pool.Exec(ctx, migrationBookkeepingSchema); err != nil {
		t.Fatalf("initialize migration bookkeeping: %v", err)
	}
	migrations, err := embeddedUpMigrations()
	if err != nil {
		t.Fatalf("read migrations: %v", err)
	}
	for _, migration := range migrations {
		if migration.version > maxVersion {
			break
		}
		if err := applyMigration(ctx, pool, migration); err != nil {
			t.Fatalf("apply migration %d: %v", migration.version, err)
		}
	}
}
