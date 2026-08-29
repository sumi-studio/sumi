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

	requestDigestRequired := false
	insertMessage := func(messageID string, seq int, content, nonce string) error {
		t.Helper()
		if requestDigestRequired {
			_, err := pool.Exec(ctx, `INSERT INTO messages
				(message_id, workspace_id, place_id, seq, author_kind, author_id, content, client_nonce, request_digest)
				VALUES ($1, $2, $3, $4, 'human', $5, $6, $7, decode(repeat('ab', 32), 'hex'))`,
				messageID, workspaceID, placeID, seq, authorID, content, nonce)
			return err
		}
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
	requestDigestRequired = true

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

func TestMessageSearchMigrationUpgradeDownAndReupgrade(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	applyMigrationsThrough(t, ctx, pool, 22)

	readMigration := func(name string) string {
		t.Helper()
		content, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		return string(content)
	}
	up := readMigration("0024_message_search.up.sql")
	down := readMigration("0024_message_search.down.sql")
	execStatements := func(label, sql string) {
		t.Helper()
		for _, stmt := range splitSQLStatements(sql) {
			if _, err := pool.Exec(ctx, stmt); err != nil {
				t.Fatalf("%s: %v", label, err)
			}
		}
	}
	assertIndex := func(want bool) {
		t.Helper()
		var exists bool
		if err := pool.QueryRow(ctx,
			"SELECT to_regclass('public.messages_content_trgm') IS NOT NULL",
		).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists != want {
			t.Fatalf("messages_content_trgm exists=%v, want %v", exists, want)
		}
	}
	execStatements("upgrade", up)
	assertIndex(true)
	execStatements("retry upgrade", up)
	assertIndex(true)
	execStatements("downgrade", down)
	assertIndex(false)
	execStatements("re-upgrade", up)
	assertIndex(true)
}

func TestVoiceChannelMigrationRoundTrip(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	applyMigrationsThrough(t, ctx, pool, 22)

	const (
		humanID      = "0198f0f4-9b72-7000-8000-000000000401"
		workspaceID  = "0198f0f4-9b72-7000-8000-000000000402"
		membershipID = "0198f0f4-9b72-7000-8000-000000000403"
		channelID    = "0198f0f4-9b72-7000-8000-000000000404"
		dmID         = "0198f0f4-9b72-7000-8000-000000000405"
	)
	if _, err := pool.Exec(ctx, "INSERT INTO humans (human_id) VALUES ($1)", humanID); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO workspaces (workspace_id, name, owner_workspace_member_id)
		VALUES ($1, 'voice migration', $2)`, workspaceID, membershipID); err != nil {
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
		INSERT INTO places (place_id, kind, workspace_id, name) VALUES
			($1, 'channel', $3, 'talk'), ($2, 'group_dm', $3, NULL)`,
		channelID, dmID, workspaceID); err != nil {
		t.Fatal(err)
	}

	read := func(name string) string {
		t.Helper()
		content, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		return string(content)
	}
	up := read("0025_voice_channels.up.sql")
	down := read("0025_voice_channels.down.sql")
	assertUp := func(phase string) {
		t.Helper()
		var voice bool
		if err := pool.QueryRow(ctx, "SELECT voice FROM places WHERE place_id=$1", channelID).Scan(&voice); err != nil {
			t.Fatalf("%s read default: %v", phase, err)
		}
		if voice {
			t.Fatalf("%s changed an existing channel into voice", phase)
		}
		if _, err := pool.Exec(ctx, "UPDATE places SET voice=true WHERE place_id=$1", channelID); err != nil {
			t.Fatalf("%s mark channel voice: %v", phase, err)
		}
		if _, err := pool.Exec(ctx, "UPDATE places SET voice=true WHERE place_id=$1", dmID); err == nil {
			t.Fatalf("%s allowed a DM to become a voice channel", phase)
		}
	}
	if _, err := pool.Exec(ctx, up); err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	assertUp("upgrade")
	if _, err := pool.Exec(ctx, down); err != nil {
		t.Fatalf("downgrade: %v", err)
	}
	var columns int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.columns
		WHERE table_schema=current_schema() AND table_name='places' AND column_name='voice'`,
	).Scan(&columns); err != nil {
		t.Fatal(err)
	}
	if columns != 0 {
		t.Fatal("down migration left places.voice behind")
	}
	if _, err := pool.Exec(ctx, up); err != nil {
		t.Fatalf("re-upgrade: %v", err)
	}
	assertUp("re-upgrade")
}

func TestSplitSQLStatements(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "strings and identifiers",
			sql:  "SELECT 'it''s; fine', \"semi;colon\"; SELECT 2;",
			want: []string{"SELECT 'it''s; fine', \"semi;colon\"", "SELECT 2"},
		},
		{
			name: "dollar quoted bodies",
			sql:  "DO $$ BEGIN RAISE NOTICE 'one; two'; END $$; SELECT $tag$three; four$tag$;",
			want: []string{"DO $$ BEGIN RAISE NOTICE 'one; two'; END $$", "SELECT $tag$three; four$tag$"},
		},
		{
			name: "comments",
			sql:  "-- keep ; here\nSELECT 1; /* and ; here /* nested ; */ */ SELECT 2; ; \n\t",
			want: []string{"-- keep ; here\nSELECT 1", "/* and ; here /* nested ; */ */ SELECT 2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitSQLStatements(tt.sql)
			if len(got) != len(tt.want) {
				t.Fatalf("splitSQLStatements(%q) = %#v, want %#v", tt.sql, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("statement %d = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestApplyNonTransactionalMigration(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	applyMigrationsThrough(t, ctx, pool, 22)

	// The second statement is rejected when sent alongside the first in one Exec,
	// because PostgreSQL implicitly wraps a multi-statement request in a
	// transaction. Success proves the runner executes non-transactional
	// migrations statement by statement.
	migration := pendingMigration{
		version:       1000,
		name:          "1000_test_concurrent_index.up.sql",
		noTransaction: true,
		content: `DROP INDEX IF EXISTS messages_content_nontransactional_test;
CREATE INDEX CONCURRENTLY messages_content_nontransactional_test
			ON messages (content);`,
	}
	if err := applyMigration(ctx, pool, migration); err != nil {
		t.Fatalf("apply non-transactional migration: %v", err)
	}
	var valid bool
	if err := pool.QueryRow(ctx, `
		SELECT i.indisvalid
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		WHERE c.relname = 'messages_content_nontransactional_test'
	`).Scan(&valid); err != nil {
		t.Fatalf("read non-transactional index: %v", err)
	}
	if !valid {
		t.Fatal("non-transactional index is invalid")
	}
	var applied bool
	if err := pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 1000)",
	).Scan(&applied); err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Fatal("non-transactional migration was not recorded")
	}
}

func TestFailedNonTransactionalMigrationIsNotRecordedAndCanRetry(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	applyMigrationsThrough(t, ctx, pool, 22)

	migration := pendingMigration{
		version:       1001,
		name:          "1001_test_nontransactional_retry.up.sql",
		noTransaction: true,
		content: `CREATE TABLE IF NOT EXISTS nontransactional_migration_retry_test (id integer);
CREATE INDEX CONCURRENTLY nontransactional_migration_retry_test_idx ON messages (missing_column);`,
	}
	if err := applyMigration(ctx, pool, migration); err == nil {
		t.Fatal("failed non-transactional migration unexpectedly succeeded")
	}
	var recorded bool
	if err := pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)", migration.version,
	).Scan(&recorded); err != nil {
		t.Fatal(err)
	}
	if recorded {
		t.Fatal("failed non-transactional migration was recorded")
	}

	migration.content = `CREATE TABLE IF NOT EXISTS nontransactional_migration_retry_test (id integer);
CREATE INDEX CONCURRENTLY nontransactional_migration_retry_test_idx ON messages (content);`
	if err := applyMigration(ctx, pool, migration); err != nil {
		t.Fatalf("retry non-transactional migration: %v", err)
	}
	if err := pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)", migration.version,
	).Scan(&recorded); err != nil {
		t.Fatal(err)
	}
	if !recorded {
		t.Fatal("retried non-transactional migration was not recorded")
	}
}

func TestPlaceStatusRevisionReceiptsMigrationDownAndReupgrade(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	applyMigrationsThrough(t, ctx, pool, 31)
	if version, err := LatestAppliedVersion(ctx, pool); err != nil || version != 31 {
		t.Fatalf("migrate through 0031: version=%d err=%v", version, err)
	}

	assertShape := func(want bool) {
		t.Helper()
		var receipts, receiptTenure, receiptTenureFK bool
		var placeRevision, placeFunction, statusFunction, statusIndex bool
		var statusColumns int
		if err := pool.QueryRow(ctx, `
			SELECT
				to_regclass('messaging_place_creation_receipts') IS NOT NULL,
				EXISTS (
					SELECT 1 FROM information_schema.columns
					WHERE table_schema = current_schema()
					  AND table_name = 'messaging_place_creation_receipts'
					  AND column_name = 'workspace_member_id'
				),
				EXISTS (
					SELECT 1 FROM pg_constraint
					WHERE conname = 'messaging_place_creation_receipts_workspace_member_identity'
					  AND conrelid = to_regclass('messaging_place_creation_receipts')
					  AND confrelid = to_regclass('workspace_members')
					  AND contype = 'f'
				),
				EXISTS (
					SELECT 1 FROM information_schema.columns
					WHERE table_schema = current_schema()
					  AND table_name = 'places' AND column_name = 'revision'
				),
				to_regprocedure('messaging_increment_place_revision()') IS NOT NULL,
				to_regprocedure('messaging_increment_participant_status_revision()') IS NOT NULL,
				to_regclass('participant_statuses_expiring') IS NOT NULL,
				(
					SELECT count(*) FROM information_schema.columns
					WHERE table_schema = current_schema()
					  AND table_name = 'participant_statuses'
					  AND column_name IN ('base_status', 'base_note', 'revision')
				)
		`).Scan(
			&receipts, &receiptTenure, &receiptTenureFK,
			&placeRevision, &placeFunction, &statusFunction,
			&statusIndex, &statusColumns,
		); err != nil {
			t.Fatal(err)
		}
		if receipts != want || receiptTenure != want || receiptTenureFK != want ||
			placeRevision != want || placeFunction != want ||
			statusFunction != want || statusIndex != want || (statusColumns == 3) != want {
			t.Fatalf("0031 shape = receipts:%t receipt-tenure:%t receipt-tenure-fk:%t place-column:%t place-function:%t status-function:%t status-index:%t status-columns:%d, want present=%t",
				receipts, receiptTenure, receiptTenureFK, placeRevision, placeFunction,
				statusFunction, statusIndex, statusColumns, want)
		}
	}
	assertShape(true)

	down, err := migrationFS.ReadFile(
		"migrations/0031_place_status_revisions_and_creation_receipts.down.sql",
	)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(down)); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("apply 0031 down transaction: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit 0031 down transaction: %v", err)
	}
	assertShape(false)

	if _, err := pool.Exec(ctx, "DELETE FROM schema_migrations WHERE version = 31"); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("reapply 0031: %v", err)
	}
	assertShape(true)
}

func TestParticipantProfilesMigrationUpDownAndReupgrade(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate through 0032: %v", err)
	}

	assertShape := func(want bool) {
		t.Helper()
		var exists bool
		if err := pool.QueryRow(ctx,
			"SELECT to_regclass('participant_profiles') IS NOT NULL",
		).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists != want {
			t.Fatalf("participant_profiles exists=%t, want %t", exists, want)
		}
	}
	assertShape(true)

	const humanID = "0198f0f4-9b72-7000-8000-000000000132"
	if _, err := pool.Exec(ctx, `INSERT INTO participant_profiles
		(member_kind, member_id, tagline) VALUES ('human', $1, '開発')`, humanID); err != nil {
		t.Fatalf("insert valid Participant profile: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO participant_profiles
		(member_kind, member_id, tagline) VALUES ('human', $1, repeat('名', 101))`,
		"0198f0f4-9b72-7000-8000-000000000133"); err == nil {
		t.Fatal("0032 accepted an overlong tagline")
	}

	down, err := migrationFS.ReadFile("migrations/0032_participant_profiles.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(down)); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("apply 0032 down transaction: %v", err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM schema_migrations WHERE version = 32"); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("remove 0032 migration record: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit 0032 down transaction: %v", err)
	}
	assertShape(false)

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("reapply 0032: %v", err)
	}
	assertShape(true)
	var rows int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM participant_profiles").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("recreated Participant profile table retained %d rows", rows)
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

func TestMessageAttachmentsMigrationUpDownReupAndConstraints(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	applyMigrationsThrough(t, ctx, pool, 20)

	readMigration := func(name string) string {
		t.Helper()
		content, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		return string(content)
	}
	up := readMigration("0023_message_attachments.up.sql")
	down := readMigration("0023_message_attachments.down.sql")
	for _, step := range []struct{ name, sql string }{{"up", up}, {"down", down}, {"re-up", up}} {
		if _, err := pool.Exec(ctx, step.sql); err != nil {
			t.Fatalf("%s: %v", step.name, err)
		}
	}
	var nullable string
	if err := pool.QueryRow(ctx, `
		SELECT is_nullable FROM information_schema.columns
		WHERE table_name='messages' AND column_name='request_digest'`).Scan(&nullable); err != nil {
		t.Fatal(err)
	}
	if nullable != "NO" {
		t.Fatalf("request_digest nullable = %q, want NO", nullable)
	}
	// After down the attachment tables and the messages column are gone; after
	// re-up they exist again with the same shape.
	var tables int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.tables
		WHERE table_name IN ('message_attachments','message_attachment_uploads','message_attachment_quotas','message_attachment_store_usage')`).Scan(&tables); err != nil || tables != 4 {
		t.Fatalf("attachment tables after re-up: %d %v", tables, err)
	}
	if _, err := pool.Exec(ctx, down); err != nil {
		t.Fatalf("second down: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.tables
		WHERE table_name IN ('message_attachments','message_attachment_uploads','message_attachment_quotas','message_attachment_store_usage')`).Scan(&tables); err != nil || tables != 0 {
		t.Fatalf("attachment tables after down: %d %v", tables, err)
	}
	var column int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.columns
		WHERE table_name='messages' AND column_name='request_digest'`).Scan(&column); err != nil || column != 0 {
		t.Fatalf("request_digest after down: %d %v", column, err)
	}
	if _, err := pool.Exec(ctx, up); err != nil {
		t.Fatalf("final up: %v", err)
	}

	// Constraint fixture: two Workspaces, one place each, one message each.
	const (
		humanID      = "0198f0f4-9b72-7000-8000-000000000401"
		wsA          = "0198f0f4-9b72-7000-8000-000000000402"
		wsB          = "0198f0f4-9b72-7000-8000-000000000403"
		memberA      = "0198f0f4-9b72-7000-8000-000000000404"
		memberB      = "0198f0f4-9b72-7000-8000-000000000405"
		placeA       = "0198f0f4-9b72-7000-8000-000000000406"
		placeB       = "0198f0f4-9b72-7000-8000-000000000407"
		messageA     = "0198f0f4-9b72-7000-8000-000000000408"
		messageB     = "0198f0f4-9b72-7000-8000-000000000409"
		attachmentID = "0198f0f4-9b72-7000-8000-00000000040a"
		uploadID     = "0198f0f4-9b72-7000-8000-00000000040b"
	)
	if _, err := pool.Exec(ctx, "INSERT INTO humans (human_id) VALUES ($1)", humanID); err != nil {
		t.Fatal(err)
	}
	for _, ws := range [][3]string{{wsA, memberA, placeA}, {wsB, memberB, placeB}} {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO workspaces (workspace_id, name, owner_workspace_member_id) VALUES ($1, 'w', $2)`, ws[0], ws[1]); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO workspace_members (workspace_member_id, workspace_id, member_kind, member_id) VALUES ($1, $2, 'human', $3)`, ws[1], ws[0], humanID); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO places (place_id, kind, workspace_id, name) VALUES ($1, 'channel', $2, 'general')`, ws[2], ws[0]); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, m := range [][3]string{{messageA, wsA, placeA}, {messageB, wsB, placeB}} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO messages (message_id, workspace_id, place_id, seq, author_kind, author_id, content, client_nonce, request_digest)
			VALUES ($1, $2, $3, 1, 'human', $4, 'hello', 'n', decode(repeat('ab', 32), 'hex'))`, m[0], m[1], m[2], humanID); err != nil {
			t.Fatal(err)
		}
	}
	insertAttachment := `
		INSERT INTO message_attachments
			(attachment_id, workspace_id, place_id, uploader_kind, uploader_id, client_nonce,
			 filename, mime, size_bytes, sha256)
		VALUES ($1, $2, $3, 'human', $4, 'nonce', 'f.txt', 'text/plain', $5, decode(repeat('ab', 32), 'hex'))`
	// Size and digest checks.
	if _, err := pool.Exec(ctx, insertAttachment, attachmentID, wsA, placeA, humanID, 20971521); err == nil {
		t.Fatal("size above 20 MiB accepted")
	}
	if _, err := pool.Exec(ctx, insertAttachment, attachmentID, wsA, placeA, humanID, 0); err == nil {
		t.Fatal("zero size accepted")
	}
	if _, err := pool.Exec(ctx, insertAttachment, attachmentID, wsA, placeA, humanID, 5); err != nil {
		t.Fatalf("valid attachment: %v", err)
	}
	// Cross-workspace/place binds are rejected by the composite foreign key.
	if _, err := pool.Exec(ctx, `UPDATE message_attachments SET message_id=$1, bound_at=now() WHERE attachment_id=$2`, messageB, attachmentID); err == nil {
		t.Fatal("cross-workspace bind accepted")
	}
	// bound_at and message_id must move together.
	if _, err := pool.Exec(ctx, `UPDATE message_attachments SET message_id=$1 WHERE attachment_id=$2`, messageA, attachmentID); err == nil {
		t.Fatal("message_id without bound_at accepted")
	}
	if _, err := pool.Exec(ctx, `UPDATE message_attachments SET message_id=$1, bound_at=now() WHERE attachment_id=$2`, messageA, attachmentID); err != nil {
		t.Fatalf("same-workspace bind: %v", err)
	}
	// Position uniqueness per message.
	second := "0198f0f4-9b72-7000-8000-00000000040c"
	if _, err := pool.Exec(ctx, `
		INSERT INTO message_attachments
			(attachment_id, workspace_id, place_id, message_id, bound_at, uploader_kind, uploader_id, client_nonce,
			 filename, mime, size_bytes, sha256, position)
		VALUES ($1, $2, $3, $4, now(), 'human', $5, 'nonce-2', 'g.txt', 'text/plain', 5, decode(repeat('ab', 32), 'hex'), 0)`,
		second, wsA, placeA, messageA, humanID); err == nil {
		t.Fatal("duplicate position accepted")
	}
	// Uploader nonce uniqueness within a place.
	if _, err := pool.Exec(ctx, insertAttachment, second, wsA, placeA, humanID, 5); err == nil {
		t.Fatal("duplicate nonce accepted")
	}
	// Empty content requires a bound attachment at commit; a message that
	// binds one in the same transaction is fine.
	if _, err := pool.Exec(ctx, `
		INSERT INTO messages (message_id, workspace_id, place_id, seq, author_kind, author_id, content, client_nonce, request_digest)
		VALUES ('0198f0f4-9b72-7000-8000-00000000040d', $1, $2, 2, 'human', $3, '', 'empty', decode(repeat('ab', 32), 'hex'))`, wsA, placeA, humanID); err == nil {
		t.Fatal("empty message without attachments accepted")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO messages (message_id, workspace_id, place_id, seq, author_kind, author_id, content, client_nonce, request_digest)
		VALUES ('0198f0f4-9b72-7000-8000-00000000040e', $1, $2, 3, 'human', $3, '', 'empty-ok', decode(repeat('ab', 32), 'hex'))`, wsA, placeA, humanID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO message_attachments
			(attachment_id, workspace_id, place_id, message_id, bound_at, uploader_kind, uploader_id, client_nonce,
			 filename, mime, size_bytes, sha256, position)
		VALUES ($1, $2, $3, '0198f0f4-9b72-7000-8000-00000000040e', now(), 'human', $4, 'nonce-3', 'g.txt', 'text/plain', 5, decode(repeat('ab', 32), 'hex'), 0)`,
		second, wsA, placeA, humanID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("empty message with attachment: %v", err)
	}
	// Reservation checks and quota non-negativity.
	if _, err := pool.Exec(ctx, `
		INSERT INTO message_attachment_uploads
			(upload_id, workspace_id, place_id, uploader_kind, uploader_id, client_nonce, installation_id,
			 authority_epoch, declared_bytes, expires_at)
		VALUES ($1, $2, $3, 'human', $4, 'r', 'inst', 0, 5, now() + interval '1 minute')`, uploadID, wsA, placeA, humanID); err == nil {
		t.Fatal("authority_epoch 0 accepted")
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO message_attachment_uploads
			(upload_id, workspace_id, place_id, uploader_kind, uploader_id, client_nonce, installation_id,
			 authority_epoch, declared_bytes, expires_at)
		VALUES ($1, $2, $3, 'human', $4, 'r', 'inst', 1, 5, now() + interval '1 minute')`, uploadID, wsA, placeA, humanID); err != nil {
		t.Fatalf("valid reservation: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE message_attachment_uploads SET state='finalized' WHERE upload_id=$1`, uploadID); err == nil {
		t.Fatal("finalized without attachment_id accepted")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO message_attachment_quotas (workspace_id, used_bytes) VALUES ($1, -1)`, wsB); err == nil {
		t.Fatal("negative quota accepted")
	}
	// The blob inventory view excludes deleted rows only.
	if _, err := pool.Exec(ctx, `UPDATE message_attachments SET blob_state='deleting' WHERE attachment_id=$1`, attachmentID); err != nil {
		t.Fatal(err)
	}
	var inventory int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM message_attachment_blob_inventory").Scan(&inventory); err != nil || inventory != 2 {
		t.Fatalf("inventory with a deleting row: %d %v", inventory, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE message_attachments SET blob_state='deleted' WHERE attachment_id=$1`, attachmentID); err == nil {
		t.Fatal("deleted without blob_deleted_at accepted")
	}
	if _, err := pool.Exec(ctx, `UPDATE message_attachments SET blob_state='deleted', blob_deleted_at=now() WHERE attachment_id=$1`, attachmentID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM message_attachment_blob_inventory").Scan(&inventory); err != nil || inventory != 1 {
		t.Fatalf("inventory with a deleted row: %d %v", inventory, err)
	}
}
