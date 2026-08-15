package db

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
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
	}
	if migrations[0].version != 1 {
		t.Fatalf("expected first migration version 1, got %d", migrations[0].version)
	}
	if !strings.HasSuffix(migrations[0].name, ".up.sql") {
		t.Fatalf("expected up migration name, got %q", migrations[0].name)
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
	var missingChecksums int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM schema_migrations WHERE checksum IS NULL OR length(checksum) <> 64",
	).Scan(&missingChecksums); err != nil {
		t.Fatal(err)
	}
	if missingChecksums != 0 {
		t.Fatalf("fresh migration history has %d missing checksums", missingChecksums)
	}
	var checksumNullable string
	if err := pool.QueryRow(ctx, `
		SELECT is_nullable
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		  AND table_name = 'schema_migrations'
		  AND column_name = 'checksum'
	`).Scan(&checksumNullable); err != nil {
		t.Fatal(err)
	}
	if checksumNullable != "NO" {
		t.Fatalf("schema_migrations.checksum remains nullable: %s", checksumNullable)
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
}

func TestMessagingByteConstraintsReplaceCharacterLimitsAndRollback(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	applyMigrationsThrough(t, ctx, pool, 16)

	const (
		workspaceID       = "0198f0f4-9b72-7000-8000-000000000101"
		placeID           = "0198f0f4-9b72-7000-8000-000000000102"
		authorID          = "0198f0f4-9b72-7000-8000-000000000103"
		workspaceMemberID = "0198f0f4-9b72-7000-8000-000000000110"
	)
	if _, err := pool.Exec(ctx, "INSERT INTO humans (human_id) VALUES ($1)", authorID); err != nil {
		t.Fatalf("insert message author: %v", err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO workspaces (workspace_id, name, owner_workspace_member_id)
		VALUES ($1, 'byte-boundary', $2)`, workspaceID, workspaceMemberID); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("insert canonical workspace: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO workspace_members
			(workspace_member_id, workspace_id, member_kind, member_id)
		VALUES ($1, $2, 'human', $3)`, workspaceMemberID, workspaceID, authorID); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("insert canonical owner membership: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit canonical message workspace: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO places
		(place_id, kind, workspace_id, name, last_seq)
		VALUES ($1, 'channel', $2, 'boundary', 10)`, placeID, workspaceID); err != nil {
		t.Fatalf("insert message place: %v", err)
	}

	const maxContentBytes = 65536
	exactContent := strings.Repeat("界", 21845) + "a"
	overContent := exactContent + "b"
	exactNonce := strings.Repeat("界", 42) + "aa"
	overNonce := strings.Repeat("界", 43)
	if len(exactContent) != maxContentBytes || len(overContent) != maxContentBytes+1 ||
		len(exactNonce) != 128 || len(overNonce) != 129 {
		t.Fatalf("invalid multibyte boundary fixture: content=%d/%d nonce=%d/%d",
			len(exactContent), len(overContent), len(exactNonce), len(overNonce))
	}

	insertMessage := func(messageID string, seq int, content, nonce string) error {
		t.Helper()
		_, err := pool.Exec(ctx, `INSERT INTO messages
			(message_id, workspace_id, place_id, seq, author_kind, author_id, content, client_nonce)
			VALUES ($1, $2, $3, $4, 'human', $5, $6, $7)`,
			messageID, workspaceID, placeID, seq, authorID, content, nonce)
		return err
	}
	assertCheckViolation := func(err error, boundary string) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s unexpectedly accepted", boundary)
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
			t.Fatalf("%s returned %v, want PostgreSQL check violation", boundary, err)
		}
	}

	// 0008 used character counts, so a direct insert could bypass both byte
	// limits. Keeping that invalid row must make the tightening migration fail
	// atomically rather than silently retaining or truncating it.
	if err := insertMessage(
		"0198f0f4-9b72-7000-8000-000000000104", 1, overContent, overNonce,
	); err != nil {
		t.Fatalf("pre-0017 character constraints rejected multibyte fixture: %v", err)
	}
	if err := Migrate(ctx, pool); err == nil {
		t.Fatal("byte-constraint migration accepted a pre-existing oversized row")
	}
	latest, err := LatestAppliedVersion(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if latest != 16 {
		t.Fatalf("failed byte-constraint migration was recorded: latest=%d", latest)
	}
	if _, err := pool.Exec(ctx, "DELETE FROM messages WHERE place_id=$1", placeID); err != nil {
		t.Fatalf("remove pre-migration oversized row: %v", err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("apply byte-constraint migration: %v", err)
	}

	if err := insertMessage(
		"0198f0f4-9b72-7000-8000-000000000105", 2, exactContent, "content-exact",
	); err != nil {
		t.Fatalf("exact 65,536-byte content was rejected: %v", err)
	}
	assertCheckViolation(insertMessage(
		"0198f0f4-9b72-7000-8000-000000000106", 3, overContent, "content-over",
	), "65,537-byte content")
	if err := insertMessage(
		"0198f0f4-9b72-7000-8000-000000000107", 4, "nonce exact", exactNonce,
	); err != nil {
		t.Fatalf("exact 128-byte client nonce was rejected: %v", err)
	}
	assertCheckViolation(insertMessage(
		"0198f0f4-9b72-7000-8000-000000000108", 5, "nonce over", overNonce,
	), "129-byte client nonce")

	down, err := migrationFS.ReadFile("migrations/0017_messaging_byte_constraints.down.sql")
	if err != nil {
		t.Fatalf("read messaging byte-constraint down migration: %v", err)
	}
	if _, err := pool.Exec(ctx, string(down)); err != nil {
		t.Fatalf("apply messaging byte-constraint down migration: %v", err)
	}
	if err := insertMessage(
		"0198f0f4-9b72-7000-8000-000000000109", 6, overContent, overNonce,
	); err != nil {
		t.Fatalf("down migration did not restore character constraints: %v", err)
	}
}

func TestMigrateRejectsChangedAppliedMigrationChecksum(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		"UPDATE schema_migrations SET checksum = repeat('0', 64) WHERE version = 16"); err != nil {
		t.Fatal(err)
	}
	err := Migrate(ctx, pool)
	if !errors.Is(err, ErrMigrationChecksumMismatch) {
		t.Fatalf("changed migration error = %v", err)
	}
	if !errors.Is(err, ErrPreCutoverResetRequired) {
		t.Fatalf("changed migration error is not reset-required: %v", err)
	}
}

func TestMigrateRejectsUnverifiableAppliedMigration(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	applyMigrationsThrough(t, ctx, pool, 7)
	if _, err := pool.Exec(ctx, `
		ALTER TABLE schema_migrations ALTER COLUMN checksum DROP NOT NULL;
		UPDATE schema_migrations SET checksum = NULL WHERE version = 7
	`); err != nil {
		t.Fatal(err)
	}

	err := Migrate(ctx, pool)
	if !errors.Is(err, ErrPreCutoverResetRequired) {
		t.Fatalf("unverifiable history error = %v, want reset-required", err)
	}
	var versionEight bool
	if err := pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 8)",
	).Scan(&versionEight); err != nil {
		t.Fatal(err)
	}
	if versionEight {
		t.Fatal("unverifiable database advanced past its applied history")
	}
}

func TestMigrateRejectsAppliedHistoryThatIsNotEmbeddedPrefix(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "DELETE FROM schema_migrations WHERE version = 8"); err != nil {
		t.Fatal(err)
	}

	err := Migrate(ctx, pool)
	if !errors.Is(err, ErrPreCutoverResetRequired) {
		t.Fatalf("non-prefix history error = %v, want reset-required", err)
	}
}

func TestMigrateUsesOneConnectionForSessionLockAndMigrationWork(t *testing.T) {
	pool := testdb.CreateWithMaxConns(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate with one available connection: %v", err)
	}
	if pool.Stat().TotalConns() != 1 {
		t.Fatalf("migration opened %d connections, want exactly one", pool.Stat().TotalConns())
	}
}

func TestMigrateRejectsLegacyVersionEightWithResetRequiredError(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	applyMigrationsThrough(t, ctx, pool, 7)

	// This is the identifying shape of the replaced 0008 migration: it owns
	// places, but its workspaces table has no distinguished owner membership.
	if _, err := pool.Exec(ctx, `
		ALTER TABLE schema_migrations ALTER COLUMN checksum DROP NOT NULL;
		CREATE TABLE workspaces (
			workspace_id uuidv7 PRIMARY KEY,
			name text NOT NULL,
			created_at timestamptz NOT NULL DEFAULT now()
		);
		CREATE TABLE places (
			place_id uuidv7 PRIMARY KEY,
			workspace_id uuidv7 REFERENCES workspaces(workspace_id)
		);
		INSERT INTO schema_migrations (version) VALUES (8)
	`); err != nil {
		t.Fatalf("create legacy version-eight shape: %v", err)
	}

	err := Migrate(ctx, pool)
	if !errors.Is(err, ErrPreCutoverResetRequired) {
		t.Fatalf("legacy migration error = %v, want reset-required", err)
	}
	if err == nil || !strings.Contains(err.Error(), "reset this pre-cutover database") {
		t.Fatalf("legacy migration error is not actionable: %v", err)
	}
	var versionNine bool
	if err := pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 9)",
	).Scan(&versionNine); err != nil {
		t.Fatal(err)
	}
	if versionNine {
		t.Fatal("legacy database advanced past the fail-fast boundary")
	}
}

func TestWorkspaceCoreDownDropsAppCapabilitySeamInDependencyOrder(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	applyMigrationsThrough(t, ctx, pool, 8)

	down, err := migrationFS.ReadFile("migrations/0008_workspace_core.down.sql")
	if err != nil {
		t.Fatalf("read Workspace down migration: %v", err)
	}
	if _, err := pool.Exec(ctx, string(down)); err != nil {
		t.Fatalf("apply Workspace down migration: %v", err)
	}
	var remaining int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM information_schema.tables
		WHERE table_schema = current_schema()
		  AND table_name IN (
			'app_workspace_role_capabilities',
			'workspace_role_app_capability_grants',
			'app_catalog',
			'workspace_roles'
		  )`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("Workspace down migration left %d capability/role tables", remaining)
	}
	var functionRemains bool
	if err := pool.QueryRow(ctx, `
		SELECT to_regprocedure(
			'prevent_app_workspace_role_capability_identity_mutation()'
		) IS NOT NULL`).Scan(&functionRemains); err != nil {
		t.Fatal(err)
	}
	if functionRemains {
		t.Fatal("Workspace down migration left capability identity trigger function")
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

func TestKosekiIdentityColumnsAreImmutableWithoutFreezingMutableState(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	applyMigrationsThrough(t, ctx, pool, 15)

	const human1 = "0198f0f4-9b72-7000-8000-000000000031"
	const human2 = "0198f0f4-9b72-7000-8000-000000000032"
	const replacementHumanID = "0198f0f4-9b72-7000-8000-000000000033"
	const agentID = "0198f0f4-9b72-7000-8000-000000000034"
	const replacementAgentID = "0198f0f4-9b72-7000-8000-000000000035"
	for _, humanID := range []string{human1, human2} {
		if _, err := pool.Exec(ctx,
			"INSERT INTO humans (human_id, display_name) VALUES ($1, 'Initial')",
			humanID); err != nil {
			t.Fatalf("insert human %s: %v", humanID, err)
		}
	}
	if _, err := pool.Exec(ctx,
		"INSERT INTO agents (personality_agent_id, human_id) VALUES ($1, $2)",
		agentID, human1); err != nil {
		t.Fatalf("insert agent: %v", err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("apply identity-immutability migration over existing identities: %v", err)
	}

	assertIdentityUpdateRejected(t, pool, ctx,
		"UPDATE humans SET human_id=$2 WHERE human_id=$1", human1, replacementHumanID)
	assertIdentityUpdateRejected(t, pool, ctx,
		"UPDATE humans SET created_at=created_at + interval '1 second' WHERE human_id=$1", human1)
	assertIdentityUpdateRejected(t, pool, ctx,
		"UPDATE agents SET personality_agent_id=$2 WHERE personality_agent_id=$1", agentID, replacementAgentID)
	assertIdentityUpdateRejected(t, pool, ctx,
		"UPDATE agents SET created_at=created_at + interval '1 second' WHERE personality_agent_id=$1", agentID)

	if _, err := pool.Exec(ctx, `UPDATE humans
		SET display_name='Chosen', display_name_customized=true, display_name_initialized=true
		WHERE human_id=$1`, human1); err != nil {
		t.Fatalf("update mutable Human attributes: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE agents
		SET display_name='Kuro', warmth='warm', human_id=$2
		WHERE personality_agent_id=$1`, agentID, human2); err != nil {
		t.Fatalf("update mutable PersonalityAgent attributes and current relation: %v", err)
	}

	var humanName string
	var customized, initialized bool
	if err := pool.QueryRow(ctx, `SELECT display_name, display_name_customized, display_name_initialized
		FROM humans WHERE human_id=$1`, human1).Scan(&humanName, &customized, &initialized); err != nil {
		t.Fatalf("read mutable Human attributes: %v", err)
	}
	if humanName != "Chosen" || !customized || !initialized {
		t.Fatalf("mutable Human attributes were not persisted: name=%q customized=%t initialized=%t",
			humanName, customized, initialized)
	}
	var agentName, warmth, ownerID string
	if err := pool.QueryRow(ctx, `SELECT display_name, warmth, human_id
		FROM agents WHERE personality_agent_id=$1`, agentID).Scan(&agentName, &warmth, &ownerID); err != nil {
		t.Fatalf("read mutable PersonalityAgent attributes: %v", err)
	}
	if agentName != "Kuro" || warmth != "warm" || ownerID != human2 {
		t.Fatalf("mutable PersonalityAgent state was not persisted: name=%q warmth=%q human_id=%q",
			agentName, warmth, ownerID)
	}

	// The identity guard deliberately does not decide the still-separate public
	// identity-row retention question. Rows without dependents remain deletable.
	if _, err := pool.Exec(ctx, "DELETE FROM agents WHERE personality_agent_id=$1", agentID); err != nil {
		t.Fatalf("delete agent identity row: %v", err)
	}
	if _, err := pool.Exec(ctx, "DELETE FROM humans WHERE human_id = ANY($1)", []string{human1, human2}); err != nil {
		t.Fatalf("delete Human identity rows: %v", err)
	}
}

func TestKosekiIdentityImmutabilityDownMigrationRemovesOnlyItsGuards(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	down, err := migrationFS.ReadFile("migrations/0016_koseki_identity_immutability.down.sql")
	if err != nil {
		t.Fatalf("read down migration: %v", err)
	}
	if _, err := pool.Exec(ctx, string(down)); err != nil {
		t.Fatalf("apply down migration: %v", err)
	}

	const human1 = "0198f0f4-9b72-7000-8000-000000000041"
	const human2 = "0198f0f4-9b72-7000-8000-000000000042"
	const agentID = "0198f0f4-9b72-7000-8000-000000000043"
	for _, humanID := range []string{human1, human2} {
		if _, err := pool.Exec(ctx, "INSERT INTO humans (human_id) VALUES ($1)", humanID); err != nil {
			t.Fatalf("insert human %s after down migration: %v", humanID, err)
		}
	}
	if _, err := pool.Exec(ctx,
		"INSERT INTO agents (personality_agent_id, human_id) VALUES ($1, $2)", agentID, human2); err != nil {
		t.Fatalf("insert agent after down migration: %v", err)
	}
	if _, err := pool.Exec(ctx,
		"UPDATE humans SET human_id='0198f0f4-9b72-7000-8000-000000000044', created_at=created_at + interval '1 second' WHERE human_id=$1",
		human1); err != nil {
		t.Fatalf("Human identity guard remained after down migration: %v", err)
	}
	if _, err := pool.Exec(ctx,
		"UPDATE agents SET personality_agent_id='0198f0f4-9b72-7000-8000-000000000045', created_at=created_at + interval '1 second' WHERE personality_agent_id=$1",
		agentID); err != nil {
		t.Fatalf("PersonalityAgent identity guard remained after down migration: %v", err)
	}
}

func assertIdentityUpdateRejected(t *testing.T, pool *pgxpool.Pool, ctx context.Context, query string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(ctx, query, args...); err == nil {
		t.Fatal("identity-column update unexpectedly succeeded")
	} else {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) {
			t.Fatalf("identity-column update returned a non-Postgres error: %v", err)
		}
		if pgErr.Code != "23000" {
			t.Fatalf("identity-column update failed outside the identity guard: code=%s error=%v", pgErr.Code, err)
		}
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

func TestWorkspaceCurrentAgentInviteMigrationUpgradeDownAndReupgrade(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	applyMigrationsThrough(t, ctx, pool, 19)

	const (
		humanID            = "0198f0f4-9b72-7000-8000-000000000301"
		agentID            = "0198f0f4-9b72-7000-8000-000000000302"
		workspaceID        = "0198f0f4-9b72-7000-8000-000000000303"
		membershipID       = "0198f0f4-9b72-7000-8000-000000000304"
		shareID            = "0198f0f4-9b72-7000-8000-000000000305"
		targetedID         = "0198f0f4-9b72-7000-8000-000000000306"
		agentBID           = "0198f0f4-9b72-7000-8000-000000000307"
		agentMembershipID  = "0198f0f4-9b72-7000-8000-000000000308"
		agentBMembershipID = "0198f0f4-9b72-7000-8000-000000000309"
		postRedemptionID   = "0198f0f4-9b72-7000-8000-000000000310"
		postRevocationID   = "0198f0f4-9b72-7000-8000-000000000311"
		nonexistentAgentID = "0198f0f4-9b72-7000-8000-000000000399"
	)
	if _, err := pool.Exec(ctx, "INSERT INTO humans (human_id) VALUES ($1)", humanID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO agents (personality_agent_id, human_id)
		VALUES ($1, $3), ($2, $3)`, agentID, agentBID, humanID); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO workspaces (workspace_id, name, owner_workspace_member_id)
		VALUES ($1, 'migration fixture', $2)`, workspaceID, membershipID); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO workspace_members
			(workspace_member_id, workspace_id, member_kind, member_id)
		VALUES ($1, $2, 'human', $3)`, membershipID, workspaceID, humanID); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO workspace_members
			(workspace_member_id, workspace_id, member_kind, member_id)
		VALUES ($1, $3, 'personality_agent', $4),
		       ($2, $3, 'personality_agent', $5)`,
		agentMembershipID, agentBMembershipID, workspaceID, agentID, agentBID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO workspace_invites
			(invite_id, workspace_id, created_by_workspace_member_id, code_hash,
			 redeemed_by_kind, redeemed_by_id, redeemed_workspace_member_id,
			 redeemed_at, expires_at)
		VALUES ($1, $2, $3, decode(repeat('ab', 32), 'hex'),
		        'human', $4, $3, now(), now() + interval '1 hour')`,
		shareID, workspaceID, membershipID, humanID); err != nil {
		t.Fatal(err)
	}

	readMigration := func(name string) string {
		t.Helper()
		content, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		return string(content)
	}
	up := readMigration("0020_workspace_current_agent_invites.up.sql")
	down := readMigration("0020_workspace_current_agent_invites.down.sql")
	if _, err := pool.Exec(ctx, up); err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	var kind string
	var codeHash []byte
	if err := pool.QueryRow(ctx,
		"SELECT invite_kind, code_hash FROM workspace_invites WHERE invite_id=$1",
		shareID).Scan(&kind, &codeHash); err != nil {
		t.Fatal(err)
	}
	if kind != "share_code" || len(codeHash) != 32 {
		t.Fatalf("legacy share invite changed: kind=%q hash=%d bytes", kind, len(codeHash))
	}
	assertRedeemedShare := func(phase string) {
		t.Helper()
		var redeemerKind, redeemerID, redeemedMembershipID string
		var redeemedAt *time.Time
		if err := pool.QueryRow(ctx, `
			SELECT redeemed_by_kind, redeemed_by_id,
			       redeemed_workspace_member_id, redeemed_at
			FROM workspace_invites WHERE invite_id=$1`, shareID).Scan(
			&redeemerKind, &redeemerID, &redeemedMembershipID, &redeemedAt,
		); err != nil {
			t.Fatalf("%s read redeemed share tuple: %v", phase, err)
		}
		if redeemerKind != "human" || redeemerID != humanID ||
			redeemedMembershipID != membershipID || redeemedAt == nil {
			t.Fatalf("%s changed redeemed share tuple: %s %s %s %v",
				phase, redeemerKind, redeemerID, redeemedMembershipID, redeemedAt)
		}
	}
	assertRedeemedShare("upgrade")

	assertRejected := func(name, statement string, arguments ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, statement, arguments...); err == nil {
			t.Fatalf("migration admitted %s", name)
		}
	}
	assertRejected("a share-code variant without a code hash", `
		INSERT INTO workspace_invites
			(invite_id, workspace_id, created_by_workspace_member_id, expires_at)
		VALUES ('0198f0f4-9b72-7000-8000-000000000320', $1, $2,
		        now() + interval '1 hour')`, workspaceID, membershipID)
	assertRejected("a targeted variant without a target", `
		INSERT INTO workspace_invites
			(invite_id, workspace_id, created_by_workspace_member_id,
			 invite_kind, expires_at)
		VALUES ('0198f0f4-9b72-7000-8000-000000000321', $1, $2,
		        'targeted_personality_agent', now() + interval '1 hour')`,
		workspaceID, membershipID)
	assertRejected("an unknown invite kind", `
		INSERT INTO workspace_invites
			(invite_id, workspace_id, created_by_workspace_member_id,
			 code_hash, invite_kind, expires_at)
		VALUES ('0198f0f4-9b72-7000-8000-000000000322', $1, $2,
		        decode(repeat('bc', 32), 'hex'), 'unknown',
		        now() + interval '1 hour')`, workspaceID, membershipID)
	assertRejected("a targeted variant with the wrong target kind", `
		INSERT INTO workspace_invites
			(invite_id, workspace_id, created_by_workspace_member_id,
			 invite_kind, target_kind, target_id, expires_at)
		VALUES ('0198f0f4-9b72-7000-8000-000000000323', $1, $2,
		        'targeted_personality_agent', 'human', $3,
		        now() + interval '1 hour')`, workspaceID, membershipID, agentID)
	assertRejected("a targeted variant for a nonexistent PersonalityAgent", `
		INSERT INTO workspace_invites
			(invite_id, workspace_id, created_by_workspace_member_id,
			 invite_kind, target_kind, target_id, expires_at)
		VALUES ('0198f0f4-9b72-7000-8000-000000000324', $1, $2,
		        'targeted_personality_agent', 'personality_agent', $3,
		        now() + interval '1 hour')`,
		workspaceID, membershipID, nonexistentAgentID)
	if _, err := pool.Exec(ctx, `
		INSERT INTO workspace_invites
			(invite_id, workspace_id, created_by_workspace_member_id,
			 invite_kind, target_kind, target_id, expires_at)
		VALUES ($1, $2, $3, 'targeted_personality_agent', 'personality_agent', $4,
		        now() + interval '1 hour')`, targetedID, workspaceID, membershipID, agentID); err != nil {
		t.Fatalf("insert targeted variant: %v", err)
	}
	assertRejected("a strict variant with both a code hash and PA target", `
		INSERT INTO workspace_invites
			(invite_id, workspace_id, created_by_workspace_member_id,
			 code_hash, invite_kind, target_kind, target_id, expires_at)
		VALUES ('0198f0f4-9b72-7000-8000-000000000325', $1, $2,
		        decode(repeat('cd', 32), 'hex'), 'targeted_personality_agent',
		        'personality_agent', $3, now() + interval '1 hour')`,
		workspaceID, membershipID, agentID)
	assertRejected("a second pending intent for the same Workspace and target", `
		INSERT INTO workspace_invites
			(invite_id, workspace_id, created_by_workspace_member_id,
			 invite_kind, target_kind, target_id, expires_at)
		VALUES ('0198f0f4-9b72-7000-8000-000000000326', $1, $2,
		        'targeted_personality_agent', 'personality_agent', $3,
		        now() + interval '1 hour')`, workspaceID, membershipID, agentID)
	assertRejected("redemption by a different PersonalityAgent", `
		UPDATE workspace_invites
		SET redeemed_by_kind='personality_agent', redeemed_by_id=$2,
		    redeemed_workspace_member_id=$3, redeemed_at=now()
		WHERE invite_id=$1`, targetedID, agentBID, agentBMembershipID)
	if _, err := pool.Exec(ctx, `
		UPDATE workspace_invites
		SET redeemed_by_kind='personality_agent', redeemed_by_id=$2,
		    redeemed_workspace_member_id=$3, redeemed_at=now()
		WHERE invite_id=$1`, targetedID, agentID, agentMembershipID); err != nil {
		t.Fatalf("record exact targeted redemption: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO workspace_invites
			(invite_id, workspace_id, created_by_workspace_member_id,
			 invite_kind, target_kind, target_id, expires_at)
		VALUES ($1, $2, $3, 'targeted_personality_agent',
		        'personality_agent', $4, now() + interval '1 hour')`,
		postRedemptionID, workspaceID, membershipID, agentID); err != nil {
		t.Fatalf("new intent after exact redemption: %v", err)
	}
	if _, err := pool.Exec(ctx,
		"UPDATE workspace_invites SET revoked_at=now() WHERE invite_id=$1",
		postRedemptionID); err != nil {
		t.Fatalf("revoke pending targeted intent: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO workspace_invites
			(invite_id, workspace_id, created_by_workspace_member_id,
			 invite_kind, target_kind, target_id, expires_at)
		VALUES ($1, $2, $3, 'targeted_personality_agent',
		        'personality_agent', $4, now() + interval '1 hour')`,
		postRevocationID, workspaceID, membershipID, agentID); err != nil {
		t.Fatalf("new intent after revocation: %v", err)
	}

	if _, err := pool.Exec(ctx, down); err != nil {
		t.Fatalf("downgrade: %v", err)
	}
	var remaining int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM workspace_invites WHERE invite_id=$1 AND octet_length(code_hash)=32",
		shareID).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 1 {
		t.Fatal("downgrade did not preserve the historical share-code invite")
	}
	assertRedeemedShare("downgrade")
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM workspace_invites WHERE invite_id=$1", targetedID,
	).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatal("downgrade retained a variant the old schema cannot represent")
	}
	if _, err := pool.Exec(ctx, up); err != nil {
		t.Fatalf("re-upgrade: %v", err)
	}
	if err := pool.QueryRow(ctx,
		"SELECT invite_kind FROM workspace_invites WHERE invite_id=$1", shareID,
	).Scan(&kind); err != nil {
		t.Fatal(err)
	}
	if kind != "share_code" {
		t.Fatalf("re-upgraded share invite kind = %q", kind)
	}
	assertRedeemedShare("re-upgrade")
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
