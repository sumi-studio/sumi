package db

import (
	"context"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
	"testing"
)

func TestFeedbackMigrationPopulatedDownReupAndUninstallRetention(t *testing.T) {
	pool := testdb.Create(t)
	ctx := context.Background()
	applyMigrationsThrough(t, ctx, pool, 39)
	migration := func(name string) string {
		t.Helper()
		v, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		return string(v)
	}
	exec := func(sql string) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	up := migration("0040_feedback_inbox.up.sql")
	down := migration("0040_feedback_inbox.down.sql")
	exec(up)
	exec(`INSERT INTO humans(human_id) VALUES('0198f0f4-9b72-7000-8000-000000000001');
 INSERT INTO agents(personality_agent_id,human_id) VALUES('0198f0f4-9b72-7000-8000-000000000002','0198f0f4-9b72-7000-8000-000000000001');
 INSERT INTO app_installations(installation_id,owner_kind,owner_id,app_id) VALUES
 ('0198f0f4-9b72-7000-8000-000000000003','personality_agent','0198f0f4-9b72-7000-8000-000000000002','feedback'),
 ('0198f0f4-9b72-7000-8000-000000000004','human','0198f0f4-9b72-7000-8000-000000000001','alarm');
 INSERT INTO feedback_threads(thread_id,author_key,author,title,body) VALUES('0198f0f4-9b72-7000-8000-000000000005','personality_agent:0198f0f4-9b72-7000-8000-000000000002','{}','Report','Body');
 INSERT INTO feedback_events(event_id,thread_id,revision,kind,author,body) VALUES('0198f0f4-9b72-7000-8000-000000000006','0198f0f4-9b72-7000-8000-000000000005',2,'message','{}','Reply');
 INSERT INTO feedback_reads(thread_id,reader_key,revision) VALUES('0198f0f4-9b72-7000-8000-000000000005','personality_agent:0198f0f4-9b72-7000-8000-000000000002',1);
 INSERT INTO feedback_requests(actor_key,request_id,fingerprint,response) VALUES('personality_agent:0198f0f4-9b72-7000-8000-000000000002','76c69e48-63e5-4cd4-b914-37a2b7bb9a44','test','{}');
 INSERT INTO feedback_attention_outbox(event_id,recipient_paid,thread_id,payload) VALUES('0198f0f4-9b72-7000-8000-000000000006','0198f0f4-9b72-7000-8000-000000000002','0198f0f4-9b72-7000-8000-000000000005','{}');
 DELETE FROM app_installations WHERE app_id='feedback';`)
	var retained bool
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM feedback_threads)=1 AND (SELECT count(*) FROM feedback_events)=1 AND (SELECT count(*) FROM feedback_reads)=1 AND (SELECT count(*) FROM feedback_requests)=1 AND (SELECT count(*) FROM feedback_attention_outbox)=1`).Scan(&retained); err != nil || !retained {
		t.Fatalf("uninstall removed conversation/delivery state: %v %v", retained, err)
	}
	// A schema downgrade is explicitly destructive to this app, unlike uninstall.
	exec(`INSERT INTO app_installations(installation_id,owner_kind,owner_id,app_id) VALUES('0198f0f4-9b72-7000-8000-000000000007','personality_agent','0198f0f4-9b72-7000-8000-000000000002','feedback')`)
	exec(down)
	var otherAppCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM app_installations WHERE app_id='alarm'`).Scan(&otherAppCount); err != nil || otherAppCount != 1 {
		t.Fatalf("downgrade touched another app: %d %v", otherAppCount, err)
	}
	exec(up)
	var allowed bool
	if err := pool.QueryRow(ctx, `SELECT participant_owner_allowed AND NOT workspace_owner_allowed FROM app_catalog WHERE app_id='feedback'`).Scan(&allowed); err != nil || !allowed {
		t.Fatalf("re-up did not restore app: %v %v", allowed, err)
	}
}
