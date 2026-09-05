package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

func TestMessagePollsMigrationFreshUpDownReupAndEmptyMessageInvariant(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	applyMigrationsThrough(t, ctx, pool, 34)

	readMigration := func(name string) string {
		t.Helper()
		content, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		return string(content)
	}
	functionBody := func() string {
		t.Helper()
		var body string
		if err := pool.QueryRow(ctx, `
			SELECT prosrc FROM pg_proc
			WHERE oid = 'require_attachment_for_empty_message()'::regprocedure`,
		).Scan(&body); err != nil {
			t.Fatal(err)
		}
		return body
	}
	priorEmptyMessageBody := functionBody()
	up := readMigration("0035_message_polls.up.sql")
	down := readMigration("0035_message_polls.down.sql")

	if _, err := pool.Exec(ctx, up); err != nil {
		t.Fatalf("fresh up: %v", err)
	}
	if got := functionBody(); got == priorEmptyMessageBody {
		t.Fatal("up migration did not admit poll-only messages")
	}
	if _, err := pool.Exec(ctx, down); err != nil {
		t.Fatalf("down: %v", err)
	}
	if got := functionBody(); got != priorEmptyMessageBody {
		t.Fatalf("down did not restore the exact prior empty-message trigger body\n--- got ---\n%s\n--- want ---\n%s", got, priorEmptyMessageBody)
	}
	var remainingTables int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.tables
		WHERE table_schema = current_schema()
		  AND table_name IN ('message_polls','message_poll_options','message_poll_votes')`,
	).Scan(&remainingTables); err != nil {
		t.Fatal(err)
	}
	if remainingTables != 0 {
		t.Fatalf("down retained %d poll tables", remainingTables)
	}
	if _, err := pool.Exec(ctx, up); err != nil {
		t.Fatalf("re-up: %v", err)
	}

	var pollTables, pollIndexes int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.tables
		WHERE table_schema = current_schema()
		  AND table_name IN ('message_polls','message_poll_options','message_poll_votes')`,
	).Scan(&pollTables); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_indexes
		WHERE schemaname = current_schema()
		  AND tablename IN ('message_polls','message_poll_options','message_poll_votes')`,
	).Scan(&pollIndexes); err != nil {
		t.Fatal(err)
	}
	// Five indexes are all constraint-owned: three primary keys plus option
	// order and canonical-text uniqueness. There is no redundant query index.
	if pollTables != 3 || pollIndexes != 5 {
		t.Fatalf("poll schema shape tables=%d indexes=%d", pollTables, pollIndexes)
	}

	const (
		humanID          = "0198f0f4-9b72-7000-8000-000000000201"
		workspaceID      = "0198f0f4-9b72-7000-8000-000000000202"
		membershipID     = "0198f0f4-9b72-7000-8000-000000000203"
		placeID          = "0198f0f4-9b72-7000-8000-000000000204"
		messageID        = "0198f0f4-9b72-7000-8000-000000000205"
		optionA          = "0198f0f4-9b72-7000-8000-000000000206"
		optionB          = "0198f0f4-9b72-7000-8000-000000000207"
		emptyWithoutPoll = "0198f0f4-9b72-7000-8000-000000000208"
	)
	if _, err := pool.Exec(ctx, "INSERT INTO humans (human_id) VALUES ($1)", humanID); err != nil {
		t.Fatal(err)
	}
	seed, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seed.Exec(ctx, `
		INSERT INTO workspaces (workspace_id, name, owner_workspace_member_id)
		VALUES ($1, 'poll migration', $2)`, workspaceID, membershipID); err != nil {
		_ = seed.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := seed.Exec(ctx, `
		INSERT INTO workspace_members
			(workspace_member_id, workspace_id, member_kind, member_id)
		VALUES ($1, $2, 'human', $3)`, membershipID, workspaceID, humanID); err != nil {
		_ = seed.Rollback(ctx)
		t.Fatal(err)
	}
	if err := seed.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO places (place_id, kind, workspace_id, name)
		VALUES ($1, 'channel', $2, 'polls')`, placeID, workspaceID); err != nil {
		t.Fatal(err)
	}

	pollOnly, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pollOnly.Exec(ctx, `
		INSERT INTO messages
			(message_id, workspace_id, place_id, seq, author_kind, author_id,
			 content, client_nonce, request_digest)
		VALUES ($1, $2, $3, 1, 'human', $4, '', 'poll-only', decode(repeat('ab', 32), 'hex'))`,
		messageID, workspaceID, placeID, humanID); err != nil {
		_ = pollOnly.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := pollOnly.Exec(ctx, `
		INSERT INTO message_polls (message_id, question, allow_multi)
		VALUES ($1, 'question', false)`, messageID); err != nil {
		_ = pollOnly.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := pollOnly.Exec(ctx, `
		INSERT INTO message_poll_options (option_id, message_id, text, ord)
		VALUES ($1, $3, 'A', 0), ($2, $3, 'B', 1)`, optionA, optionB, messageID); err != nil {
		_ = pollOnly.Rollback(ctx)
		t.Fatal(err)
	}
	if err := pollOnly.Commit(ctx); err != nil {
		t.Fatalf("commit poll-only message: %v", err)
	}

	invalid, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := invalid.Exec(ctx, `
		INSERT INTO messages
			(message_id, workspace_id, place_id, seq, author_kind, author_id,
			 content, client_nonce, request_digest)
		VALUES ($1, $2, $3, 2, 'human', $4, '', 'still-empty', decode(repeat('cd', 32), 'hex'))`,
		emptyWithoutPoll, workspaceID, placeID, humanID); err != nil {
		_ = invalid.Rollback(ctx)
		t.Fatal(err)
	}
	if err := invalid.Commit(ctx); err == nil {
		t.Fatal("deferred trigger accepted an empty message without attachment or poll")
	}

	if _, err := pool.Exec(ctx, "UPDATE message_polls SET revision=-1 WHERE message_id=$1", messageID); !isCheckViolation(err) {
		t.Fatalf("negative poll revision = %v, want check violation", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO message_poll_options (option_id, message_id, text, ord)
		VALUES ('0198f0f4-9b72-7000-8000-000000000209', $1, 'A', 2)`, messageID); err == nil {
		t.Fatal("database accepted a duplicate canonical option")
	}
}

func isCheckViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23514"
}
