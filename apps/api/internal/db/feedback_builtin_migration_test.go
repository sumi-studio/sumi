package db

import (
	"context"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
	"testing"
)

func TestFeedbackBuiltinMigrationRetainsContentAndAccessRecords(t *testing.T) {
	pool := testdb.Create(t)
	ctx := context.Background()
	applyMigrationsThrough(t, ctx, pool, 43)
	exec := func(q string) {
		t.Helper()
		if _, err := pool.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	exec(`INSERT INTO humans(human_id) VALUES('0198f0f4-9b72-7000-8000-000000000001');
 INSERT INTO agents(personality_agent_id,human_id) VALUES('0198f0f4-9b72-7000-8000-000000000002','0198f0f4-9b72-7000-8000-000000000001');
 INSERT INTO app_installations(installation_id,owner_kind,owner_id,app_id) VALUES
 ('0198f0f4-9b72-7000-8000-000000000003','human','0198f0f4-9b72-7000-8000-000000000001','feedback'),
 ('0198f0f4-9b72-7000-8000-000000000004','human','0198f0f4-9b72-7000-8000-000000000001','alarm');
 INSERT INTO feedback_threads(thread_id,author_key,author,title,body) VALUES('0198f0f4-9b72-7000-8000-000000000005','human:0198f0f4-9b72-7000-8000-000000000001','{}','Report','Body');
 INSERT INTO feedback_events(event_id,thread_id,revision,kind,author,body) VALUES('0198f0f4-9b72-7000-8000-000000000006','0198f0f4-9b72-7000-8000-000000000005',2,'message','{}','Reply');
 INSERT INTO feedback_reads(thread_id,reader_key,revision) VALUES('0198f0f4-9b72-7000-8000-000000000005','human:0198f0f4-9b72-7000-8000-000000000001',1);
 INSERT INTO feedback_requests(actor_key,request_id,fingerprint,response) VALUES('human:0198f0f4-9b72-7000-8000-000000000001','76c69e48-63e5-4cd4-b914-37a2b7bb9a44','test','{}');
 INSERT INTO feedback_attention_outbox(event_id,recipient_paid,thread_id,payload) VALUES('0198f0f4-9b72-7000-8000-000000000006','0198f0f4-9b72-7000-8000-000000000002','0198f0f4-9b72-7000-8000-000000000005','{}');
 INSERT INTO feedback_attachments(attachment_id,author_key,thread_id,name,mime_type,content) VALUES('0198f0f4-9b72-7000-8000-000000000007','human:0198f0f4-9b72-7000-8000-000000000001','0198f0f4-9b72-7000-8000-000000000005','image.png','image/png',decode('0102','hex'));`)
	tables := []string{"feedback_threads", "feedback_events", "feedback_reads", "feedback_requests", "feedback_attention_outbox", "feedback_attachments"}
	before := map[string]string{}
	snapshot := func(table string) string {
		t.Helper()
		var v string
		if err := pool.QueryRow(ctx, "SELECT jsonb_agg(to_jsonb(t))::text FROM "+table+" t").Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	for _, table := range tables {
		before[table] = snapshot(table)
	}
	migration, err := migrationFS.ReadFile("migrations/0044_feedback_builtin.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	exec(string(migration))
	for _, table := range tables {
		if snapshot(table) != before[table] {
			t.Fatalf("changed %s", table)
		}
	}
	var old, other int
	if err := pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM app_catalog WHERE app_id='feedback')+(SELECT count(*) FROM app_installations WHERE app_id='feedback'),(SELECT count(*) FROM app_installations WHERE app_id='alarm')").Scan(&old, &other); err != nil || old != 0 || other != 1 {
		t.Fatalf("catalog retirement %d %d %v", old, other, err)
	}
}
