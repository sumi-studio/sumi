package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"sync"
	"testing"
	"time"
)

// The fake models the gateway's fsynced idempotency result, independently of the
// Messaging transaction. This exposes the append/DB-ack crash window.
type attentionDeliveryFixture struct {
	mu         sync.Mutex
	payloads   map[string]string
	receipts   map[string]AgentAttentionReceipt
	events     []AgentAttentionEvent
	calls      int
	holds      int
	prepares   int
	afterAdmit func(context.Context) error
}

func newAttentionDelivery() *attentionDeliveryFixture {
	return &attentionDeliveryFixture{payloads: map[string]string{}, receipts: map[string]AgentAttentionReceipt{}}
}
func (d *attentionDeliveryFixture) Prepare(_ context.Context, _ string) (func(), error) {
	d.mu.Lock()
	d.holds++
	d.prepares++
	d.mu.Unlock()
	return func() { d.mu.Lock(); d.holds--; d.mu.Unlock() }, nil
}
func (d *attentionDeliveryFixture) Lookup(_ context.Context, key string, event AgentAttentionEvent) (AgentAttentionReceipt, bool, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return AgentAttentionReceipt{}, false, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	receipt, found := d.receipts[key]
	if found && d.payloads[key] != string(data) {
		return AgentAttentionReceipt{}, false, errors.New("idempotency payload conflict")
	}
	return receipt, found, nil
}

func (d *attentionDeliveryFixture) Admit(ctx context.Context, key string, event AgentAttentionEvent) (AgentAttentionReceipt, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return AgentAttentionReceipt{}, err
	}
	d.mu.Lock()
	d.calls++
	if d.holds < 1 {
		d.mu.Unlock()
		return AgentAttentionReceipt{}, errors.New("admission hold released before effect")
	}
	receipt, found := d.receipts[key]
	if found && d.payloads[key] != string(data) {
		d.mu.Unlock()
		return AgentAttentionReceipt{}, errors.New("idempotency payload conflict")
	}
	if !found {
		receipt = AgentAttentionReceipt{CommandID: uuid.NewString(), Seq: uint64(len(d.receipts) + 1)}
		d.receipts[key], d.payloads[key] = receipt, string(data)
		d.events = append(d.events, event)
	}
	d.mu.Unlock()
	if d.afterAdmit != nil {
		if err := d.afterAdmit(ctx); err != nil {
			return AgentAttentionReceipt{}, err
		}
	}
	return receipt, nil
}

func TestAgentAttentionMentionFreezesSourceAndOnlyAdmitsOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	ws, ch := w.workspaceWithChannel(t, ctx)
	sender := w.store.mustScope(t, ctx, ws.WorkspaceID, w.humanB)
	msg, _, err := sender.AppendMessage(ctx, AppendInput{PlaceID: ch.PlaceID, Content: "@Kuro 元の相談です", ClientNonce: "attention-once"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := sender.AppendMessage(ctx, AppendInput{PlaceID: ch.PlaceID, Content: "@Kuro 元の相談です", ClientNonce: "attention-once"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sender.EditMessage(ctx, ch.PlaceID, msg.MessageID, "訂正後です", msg.Revision); err != nil {
		t.Fatal(err)
	}
	d := newAttentionDelivery()
	stats, err := w.store.core.DeliverAgentAttention(ctx, d, 10)
	if err != nil || stats.Admitted != 1 {
		t.Fatalf("first delivery: %+v %v", stats, err)
	}
	stats, err = w.store.core.DeliverAgentAttention(ctx, d, 10)
	if err != nil || stats.Admitted != 0 || len(d.events) != 1 || d.holds != 0 {
		t.Fatalf("repeat: %+v %v events=%d holds=%d", stats, err, len(d.events), d.holds)
	}
	event := d.events[0]
	if event.Actor.ID != w.humanB.ID || event.Actor.Kind != string(KindHuman) || event.Actor.DisplayName != "Haru" || event.PersonalityAgentID != w.agent.ID {
		t.Fatalf("wrong source attribution: %+v", event.Actor)
	}
	if event.Content != msg.Content || event.MessageID != msg.MessageID || event.MessageRevision != msg.Revision || !event.OccurredAt.Equal(msg.CreatedAt) || event.Place.ID != ch.PlaceID || event.WorkspaceID != ws.WorkspaceID {
		t.Fatalf("frozen source changed: %+v", event)
	}
	var seq int64
	if err := w.store.pool.QueryRow(ctx, "SELECT admitted_command_seq FROM agent_attention_deliveries WHERE event_id=$1", event.EventID).Scan(&seq); err != nil || seq != 1 {
		t.Fatalf("receipt: %d %v", seq, err)
	}
}

func TestAgentAttentionHonorsChannelNotificationPreferences(t *testing.T) {
	for _, tc := range []struct {
		name, level, content string
		keywords             []string
		want                 bool
	}{
		{"all", NotifyLevelAll, "共有しておきます", nil, true},
		{"mentions only", NotifyLevelMentions, "共有しておきます", nil, false},
		{"keyword", NotifyLevelMentions, "今日のリリース", []string{"リリース"}, true},
		{"keyword absent", NotifyLevelMentions, "共有しておきます", []string{"リリース"}, false},
		{"muted", NotifyLevelMute, "@Kuro 今日のリリース", []string{"リリース"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			w := newWorld(t, ctx)
			ws, ch := w.workspaceWithChannel(t, ctx)
			pa := w.store.mustScope(t, ctx, ws.WorkspaceID, w.agent)
			if _, err := pa.SetNotificationSetting(ctx, tc.level, nil, tc.keywords); err != nil {
				t.Fatal(err)
			}
			msg := w.send(t, ctx, ch.PlaceID, w.humanB, tc.content)
			d := newAttentionDelivery()
			stats, err := w.store.core.DeliverAgentAttention(ctx, d, 10)
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if tc.want {
				want = 1
			}
			if stats.Admitted != want || len(d.events) != want {
				t.Fatalf("delivery: %+v events=%d", stats, len(d.events))
			}
			if tc.want && (d.events[0].Kind != AgentAttentionMessage || d.events[0].MessageID != msg.MessageID || d.events[0].PersonalityAgentID != w.agent.ID) {
				t.Fatalf("wrong channel event: %+v", d.events[0])
			}
		})
	}
}

func TestAgentAttentionResendsIdenticalEventAfterAdmissionAckFailure(t *testing.T) {
	for _, afterFailure := range []string{"restart", "delete", "resolve"} {
		t.Run(afterFailure, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			w := newWorld(t, ctx)
			ws, ch := w.workspaceWithChannel(t, ctx)
			sender := w.store.mustScope(t, ctx, ws.WorkspaceID, w.humanA)
			pa := w.store.mustScope(t, ctx, ws.WorkspaceID, w.agent)
			text := "@Kuro 再開して確認"
			if _, err := pa.SetNotificationSetting(ctx, NotifyLevelMentions, nil, nil); err != nil {
				t.Fatal(err)
			}
			if afterFailure == "resolve" {
				text = "予約の元の話"
			}
			msg := w.send(t, ctx, ch.PlaceID, w.humanA, text)
			var marker ReplyLaterMarker
			if afterFailure == "resolve" {
				var err error
				marker, _, err = pa.CreateReplyLater(ctx, ch.PlaceID, msg.MessageID, "後で読む", time.Now().Add(-time.Hour))
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := w.store.pool.Exec(ctx, `CREATE FUNCTION reject_attention_ack() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN IF NEW.admitted_at IS NOT NULL THEN RAISE EXCEPTION 'synthetic ack failure'; END IF; RETURN NEW; END $$;
                CREATE TRIGGER reject_attention_ack BEFORE UPDATE ON agent_attention_deliveries FOR EACH ROW EXECUTE FUNCTION reject_attention_ack()`); err != nil {
				t.Fatal(err)
			}
			d := newAttentionDelivery()
			stats, err := w.store.core.DeliverAgentAttention(ctx, d, 10)
			if err == nil || stats.Retried != 1 || len(d.events) != 1 {
				t.Fatalf("expected post-admission ack failure: %+v %v", stats, err)
			}
			if _, err := w.store.pool.Exec(ctx, "DROP TRIGGER reject_attention_ack ON agent_attention_deliveries; UPDATE agent_attention_deliveries SET next_attempt_at=now()"); err != nil {
				t.Fatal(err)
			}
			switch afterFailure {
			case "delete":
				if _, err := sender.DeleteMessage(ctx, ch.PlaceID, msg.MessageID); err != nil {
					t.Fatal(err)
				}
			case "resolve":
				if _, err := pa.ResolveReplyLater(ctx, marker.MarkerID); err != nil {
					t.Fatal(err)
				}
			}
			restarted := New(w.store.pool, w.workspaces, w.apps)
			stats, err = restarted.DeliverAgentAttention(ctx, d, 10)
			if err != nil || stats.Admitted != 1 || stats.Suppressed != 0 || len(d.events) != 1 || d.calls != 1 || d.holds != 0 {
				t.Fatalf("reconcile known effect after %s: %+v %v commands=%d calls=%d", afterFailure, stats, err, len(d.events), d.calls)
			}
		})
	}
}

func TestAgentAttentionDueRemindersBelongToPAAndResolveSuppresses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	ws, ch := w.workspaceWithChannel(t, ctx)
	pa := w.store.mustScope(t, ctx, ws.WorkspaceID, w.agent)
	if _, err := pa.SetNotificationSetting(ctx, NotifyLevelMentions, nil, nil); err != nil {
		t.Fatal(err)
	}
	source := w.send(t, ctx, ch.PlaceID, w.humanA, "リマインダーの元の話")
	human := w.store.mustScope(t, ctx, ws.WorkspaceID, w.humanA)
	due := time.Now().Add(-time.Hour).UTC()
	if _, _, err := human.CreateReplyLater(ctx, ch.PlaceID, source.MessageID, "Human個人用", due); err != nil {
		t.Fatal(err)
	}
	marker, created, err := pa.CreateReplyLater(ctx, ch.PlaceID, source.MessageID, "自分で後で読む", due)
	if err != nil || !created {
		t.Fatalf("create: %v", err)
	}
	if _, created, err := pa.CreateReplyLater(ctx, ch.PlaceID, source.MessageID, "再作成", due.Add(time.Hour)); err != nil || created {
		t.Fatalf("repeat: %v", err)
	}
	d := newAttentionDelivery()
	stats, err := New(w.store.pool, w.workspaces, w.apps).DeliverAgentAttention(ctx, d, 10)
	if err != nil || stats.Admitted != 1 || len(d.events) != 1 {
		t.Fatalf("due: %+v %v", stats, err)
	}
	event := d.events[0]
	if event.Kind != AgentAttentionReminder || event.Actor.ID != w.agent.ID || event.Content != marker.Note || event.MarkerID != marker.MarkerID || event.DueAt == nil || !event.DueAt.Equal(due) {
		t.Fatalf("reminder source: %+v", event)
	}
	if _, err := pa.ResolveReplyLater(ctx, marker.MarkerID); err != nil {
		t.Fatal(err)
	}
	future, _, err := pa.CreateReplyLater(ctx, ch.PlaceID, source.MessageID, "取り消す予約", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pa.ResolveReplyLater(ctx, future.MarkerID); err != nil {
		t.Fatal(err)
	}
	// Time passes, but resolution has already canceled this fresh source.
	if _, err := w.store.pool.Exec(ctx, "UPDATE agent_attention_deliveries SET available_at=now() WHERE source_id=$1", future.MarkerID); err != nil {
		t.Fatal(err)
	}
	stats, err = w.store.core.DeliverAgentAttention(ctx, d, 10)
	if err != nil || stats.Admitted != 0 || len(d.events) != 1 {
		t.Fatalf("resolved: %+v %v", stats, err)
	}
}

func TestAgentAttentionRechecksSourceBeforeAdmission(t *testing.T) {
	for _, mutation := range []string{"deleted", "left", "disabled"} {
		t.Run(mutation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			w := newWorld(t, ctx)
			ws, ch := w.workspaceWithChannel(t, ctx)
			sender := w.store.mustScope(t, ctx, ws.WorkspaceID, w.humanA)
			msg := w.send(t, ctx, ch.PlaceID, w.humanA, "@Kuro 届く前に条件が変わる")
			switch mutation {
			case "deleted":
				_, err := sender.DeleteMessage(ctx, ch.PlaceID, msg.MessageID)
				if err != nil {
					t.Fatal(err)
				}
			case "left":
				if err := w.workspaces.Leave(ctx, ws.WorkspaceID, w.agent); err != nil {
					t.Fatal(err)
				}
			case "disabled":
				if _, err := w.apps.SetEnabledByID(ctx, sender.Scope.InstallationID, w.humanA, false); err != nil {
					t.Fatal(err)
				}
			}
			d := newAttentionDelivery()
			stats, err := w.store.core.DeliverAgentAttention(ctx, d, 10)
			if err != nil || stats.Suppressed != 1 || len(d.events) != 0 {
				t.Fatalf("revoked: %+v %v events=%d", stats, err, len(d.events))
			}
		})
	}
}

func TestAgentAttentionSourceLeaseOrdersRevocationAfterAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	ws, ch := w.workspaceWithChannel(t, ctx)
	w.send(t, ctx, ch.PlaceID, w.humanA, "@Kuro 認可中の受信")
	entered, release := make(chan struct{}), make(chan struct{})
	d := newAttentionDelivery()
	d.afterAdmit = func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	deliveryDone := make(chan error, 1)
	go func() { _, err := w.store.core.DeliverAgentAttention(ctx, d, 10); deliveryDone <- err }()
	<-entered
	revoked := make(chan error, 1)
	go func() { revoked <- w.workspaces.Leave(ctx, ws.WorkspaceID, w.agent) }()
	waitForWaitingBackend(t, ctx, w.store.pool)
	close(release)
	if err := <-deliveryDone; err != nil {
		t.Fatal(err)
	}
	if err := <-revoked; err != nil {
		t.Fatal(err)
	}
	if len(d.events) != 1 {
		t.Fatal("lawfully admitted experience was lost")
	}
}

func TestAgentAttentionThreadMutationSharesPlaceThenMessageLockOrder(t *testing.T) {
	for _, mutation := range []string{"edit", "delete"} {
		t.Run(mutation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			w := newWorld(t, ctx)
			ws, channel := w.workspaceWithChannel(t, ctx)
			owner := w.store.mustScope(t, ctx, ws.WorkspaceID, w.humanA)
			thread, _, err := owner.CreateThread(ctx, channel.PlaceID, "同時変更", "", "attention-thread")
			if err != nil {
				t.Fatal(err)
			}
			msg, _, err := owner.AppendMessage(ctx, AppendInput{PlaceID: thread.Place.PlaceID, Content: "@Kuro スレッドの相談", ClientNonce: "attention-thread-message"})
			if err != nil {
				t.Fatal(err)
			}
			blocker, err := w.store.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = blocker.Rollback(context.Background()) }()
			if _, err := blocker.Exec(ctx, "SELECT message_id FROM messages WHERE message_id=$1 FOR UPDATE", msg.MessageID); err != nil {
				t.Fatal(err)
			}
			mutated := make(chan error, 1)
			go func() {
				var err error
				if mutation == "edit" {
					_, err = owner.EditMessage(ctx, thread.Place.PlaceID, msg.MessageID, "修正された相談", msg.Revision)
				} else {
					_, err = owner.DeleteMessage(ctx, thread.Place.PlaceID, msg.MessageID)
				}
				mutated <- err
			}()
			waitForWaitingBackend(t, ctx, w.store.pool)
			delivered := make(chan error, 1)
			d := newAttentionDelivery()
			go func() { _, err := w.store.core.DeliverAgentAttention(ctx, d, 10); delivered <- err }()
			for {
				var waiting int
				if err := w.store.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
                    WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock'`).Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				if waiting >= 2 {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("both operations did not reach lock waits")
				case <-time.After(5 * time.Millisecond):
				}
			}
			if err := blocker.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-mutated; err != nil {
				t.Fatalf("mutation deadlocked/failed: %v", err)
			}
			if err := <-delivered; err != nil {
				t.Fatalf("delivery deadlocked/failed: %v", err)
			}
			expected := 1
			if mutation == "delete" {
				expected = 0
			}
			if len(d.events) != expected {
				t.Fatalf("events=%d want %d", len(d.events), expected)
			}
		})
	}
}

func TestAgentAttentionMessageMutationDoesNotBlockReminderForeignKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	ws, ch := w.workspaceWithChannel(t, ctx)
	owner := w.store.mustScope(t, ctx, ws.WorkspaceID, w.humanA)
	pa := w.store.mustScope(t, ctx, ws.WorkspaceID, w.agent)
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "予約と編集が重なる")
	// Stop actual CreateReplyLater after it locks the message, just before its
	// INSERT acquires the place foreign-key KEY SHARE lock.
	if _, err := w.store.pool.Exec(ctx, `CREATE FUNCTION pause_reminder_insert() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN PERFORM pg_advisory_xact_lock(831427); RETURN NEW; END $$;
        CREATE TRIGGER pause_reminder_insert BEFORE INSERT ON reply_later_markers
        FOR EACH ROW EXECUTE FUNCTION pause_reminder_insert()`); err != nil {
		t.Fatal(err)
	}
	blocker, err := w.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(context.Background()) }()
	if _, err := blocker.Exec(ctx, "SELECT pg_advisory_xact_lock(831427)"); err != nil {
		t.Fatal(err)
	}
	created := make(chan error, 1)
	go func() {
		_, _, err := pa.CreateReplyLater(ctx, ch.PlaceID, msg.MessageID, "後で読む", time.Now().Add(time.Hour))
		created <- err
	}()
	waitForWaitingBackend(t, ctx, w.store.pool)
	edited := make(chan error, 1)
	go func() {
		_, err := owner.EditMessage(ctx, ch.PlaceID, msg.MessageID, "修正した話", msg.Revision)
		edited <- err
	}()
	for {
		var waiting int
		if err := w.store.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
            WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock'`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting >= 2 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("both operations did not reach lock waits")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err := blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-created; err != nil {
		t.Fatalf("reminder insert deadlocked/failed: %v", err)
	}
	if err := <-edited; err != nil {
		t.Fatalf("edit deadlocked/failed: %v", err)
	}
}

func TestAgentAttentionDMRecipientsReceiveWithoutMention(t *testing.T) {
	for _, group := range []bool{false, true} {
		t.Run(map[bool]string{false: "dm", true: "group_dm"}[group], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			w := newWorld(t, ctx)
			ws, _ := w.workspaceWithChannel(t, ctx)
			sender := w.store.mustScope(t, ctx, ws.WorkspaceID, w.humanA)
			var place Place
			var err error
			if group {
				place, err = sender.CreateGroupDM(ctx, []ParticipantRef{w.humanB, w.agent})
			} else {
				place, _, err = sender.EnsureDM(ctx, w.agent)
			}
			if err != nil {
				t.Fatal(err)
			}
			msg, _, err := sender.AppendMessage(ctx, AppendInput{PlaceID: place.PlaceID, Content: "明日の予定を一緒に考えよう", ClientNonce: "ordinary-dm"})
			if err != nil {
				t.Fatal(err)
			}
			d := newAttentionDelivery()
			stats, err := w.store.core.DeliverAgentAttention(ctx, d, 10)
			if err != nil || stats.Admitted != 1 || len(d.events) != 1 {
				t.Fatalf("ordinary DM: %+v %v", stats, err)
			}
			e := d.events[0]
			if e.Kind != AgentAttentionMessage || e.Actor.ID != w.humanA.ID || e.PersonalityAgentID != w.agent.ID || e.MessageID != msg.MessageID || e.Place.ID != place.PlaceID {
				t.Fatalf("wrong source/recipient: %+v", e)
			}
			pa := w.store.mustScope(t, ctx, ws.WorkspaceID, w.agent)
			if _, _, err := pa.AppendMessage(ctx, AppendInput{PlaceID: place.PlaceID, Content: "返信です", ClientNonce: "own-reply"}); err != nil {
				t.Fatal(err)
			}
			stats, err = w.store.core.DeliverAgentAttention(ctx, d, 10)
			if err != nil || stats.Admitted != 0 {
				t.Fatalf("self reply woke author: %+v %v", stats, err)
			}
		})
	}
}

func TestAgentAttentionReplyReturnsToAuthorOnce(t *testing.T) {
	for _, kind := range []string{"channel", "thread", "dm", "mention"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			w := newWorld(t, ctx)
			ws, place := w.workspaceWithChannel(t, ctx)
			pa := w.store.mustScope(t, ctx, ws.WorkspaceID, w.agent)
			sender := w.store.mustScope(t, ctx, ws.WorkspaceID, w.humanB)
			if kind == "thread" {
				thread, _, err := pa.CreateThread(ctx, place.PlaceID, "相談", "", "reply-thread")
				if err != nil {
					t.Fatal(err)
				}
				place = thread.Place
			} else if kind == "dm" {
				var err error
				place, _, err = sender.EnsureDM(ctx, w.agent)
				if err != nil {
					t.Fatal(err)
				}
			}
			// Replies remain direct attention even with mentions-only preferences.
			if _, err := pa.SetNotificationSetting(ctx, NotifyLevelMentions, nil, nil); err != nil {
				t.Fatal(err)
			}
			question, _, err := pa.AppendMessage(ctx, AppendInput{PlaceID: place.PlaceID, Content: "どちらにしますか", ClientNonce: "question"})
			if err != nil {
				t.Fatal(err)
			}
			content := "明日でお願いします"
			if kind == "mention" {
				content = "@Kuro " + content
			}
			input := AppendInput{PlaceID: place.PlaceID, Content: content, ReplyTo: question.MessageID, ClientNonce: "answer"}
			answer, _, err := sender.AppendMessage(ctx, input)
			if err != nil {
				t.Fatal(err)
			}
			if _, created, err := sender.AppendMessage(ctx, input); err != nil || created {
				t.Fatalf("replay: %v %v", created, err)
			}
			d := newAttentionDelivery()
			// Simulate admission acknowledgement loss: retries must reuse frozen metadata.
			d.afterAdmit = func(context.Context) error { return errors.New("lost admission acknowledgement") }
			if stats, err := w.store.core.DeliverAgentAttention(ctx, d, 10); err == nil || stats.Retried != 1 {
				t.Fatalf("expected retry: %+v %v", stats, err)
			}
			d.afterAdmit = nil
			if _, err := w.store.pool.Exec(ctx, "UPDATE agent_attention_deliveries SET next_attempt_at=now()"); err != nil {
				t.Fatal(err)
			}
			stats, err := w.store.core.DeliverAgentAttention(ctx, d, 10)
			if err != nil || stats.Admitted != 1 || len(d.events) != 1 || d.calls != 1 {
				t.Fatalf("delivery: %+v %v events=%d calls=%d", stats, err, len(d.events), d.calls)
			}
			event := d.events[0]
			if event.PersonalityAgentID != w.agent.ID || event.Actor.ID != w.humanB.ID || event.MessageID != answer.MessageID || event.ReplyToMessageID != question.MessageID || event.Content != content || event.ReplyRequired != (kind == "channel" || kind == "thread") {
				t.Fatalf("reply source: %+v", event)
			}
			if kind == "mention" && event.Kind != AgentAttentionMention {
				t.Fatalf("mention reason lost: %+v", event)
			}
			messages, err := pa.MessagesSince(ctx, event.Place.ID, 0, 10)
			if err != nil || len(messages) != 2 || messages[0].MessageID != event.ReplyToMessageID || messages[1].ReplyTo != messages[0].MessageID {
				t.Fatalf("reopen original conversation: %+v %v", messages, err)
			}
		})
	}
}

func TestAgentAttentionReplyRespectsRecipientAndSource(t *testing.T) {
	for _, scenario := range []string{"mute", "self", "unrelated", "deleted_before", "deleted_after", "left", "disabled", "mention_parent_deleted", "dm_parent_deleted", "all_parent_deleted", "keyword_parent_deleted"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			w := newWorld(t, ctx)
			ws, place := w.workspaceWithChannel(t, ctx)
			pa := w.store.mustScope(t, ctx, ws.WorkspaceID, w.agent)
			sender := w.store.mustScope(t, ctx, ws.WorkspaceID, w.humanB)
			level, keywords := NotifyLevelMentions, []string(nil)
			if scenario == "all_parent_deleted" {
				level = NotifyLevelAll
			}
			if scenario == "keyword_parent_deleted" {
				keywords = []string{"回答"}
			}
			if _, err := pa.SetNotificationSetting(ctx, level, nil, keywords); err != nil {
				t.Fatal(err)
			}
			if scenario == "dm_parent_deleted" {
				var err error
				place, _, err = sender.EnsureDM(ctx, w.agent)
				if err != nil {
					t.Fatal(err)
				}
			}
			question, _, err := pa.AppendMessage(ctx, AppendInput{PlaceID: place.PlaceID, Content: "質問", ClientNonce: "question"})
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "mute" {
				if _, err := pa.SetNotificationSetting(ctx, NotifyLevelMute, nil, nil); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "self" {
				sender = pa
			}
			if scenario == "deleted_before" {
				if _, err := pa.DeleteMessage(ctx, place.PlaceID, question.MessageID); err != nil {
					t.Fatal(err)
				}
			}
			replyTo, content := question.MessageID, "回答"
			if scenario == "unrelated" {
				replyTo = ""
			}
			if scenario == "mention_parent_deleted" {
				content = "@Kuro 回答"
			}
			if _, _, err := sender.AppendMessage(ctx, AppendInput{PlaceID: place.PlaceID, Content: content, ReplyTo: replyTo, ClientNonce: "answer"}); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "deleted_after", "mention_parent_deleted", "dm_parent_deleted", "all_parent_deleted", "keyword_parent_deleted":
				if _, err := pa.DeleteMessage(ctx, place.PlaceID, question.MessageID); err != nil {
					t.Fatal(err)
				}
			case "left":
				if err := w.workspaces.Leave(ctx, ws.WorkspaceID, w.agent); err != nil {
					t.Fatal(err)
				}
			case "disabled":
				if _, err := w.apps.SetEnabledByID(ctx, sender.Scope.InstallationID, w.humanA, false); err != nil {
					t.Fatal(err)
				}
			}
			d := newAttentionDelivery()
			stats, err := w.store.core.DeliverAgentAttention(ctx, d, 10)
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if scenario == "mention_parent_deleted" || scenario == "dm_parent_deleted" || scenario == "all_parent_deleted" || scenario == "keyword_parent_deleted" {
				want = 1
			}
			if len(d.events) != want {
				t.Fatalf("events=%+v stats=%+v", d.events, stats)
			}
			if scenario == "deleted_after" || scenario == "left" || scenario == "disabled" {
				if stats.Suppressed != 1 || d.prepares != 0 {
					t.Fatalf("source not suppressed before start: %+v prepares=%d", stats, d.prepares)
				}
			}
		})
	}
}

func TestAgentAttentionReplyAndOtherMentionHaveDistinctRecipients(t *testing.T) {
	for _, mentionOther := range []bool{false, true} {
		t.Run(fmt.Sprint(mentionOther), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			w := newWorld(t, ctx)
			ws, place := w.workspaceWithChannel(t, ctx)
			otherID, err := koseki.New(w.store.pool).MintSecretary(ctx, w.humanB.ID)
			if err != nil {
				t.Fatal(err)
			}
			if err := w.store.AddWorkspaceMember(ctx, ws.WorkspaceID, PersonalityAgent(otherID), RoleMember); err != nil {
				t.Fatal(err)
			}
			if _, err := w.store.pool.Exec(ctx, "UPDATE agents SET display_name='Shiro' WHERE personality_agent_id=$1", otherID); err != nil {
				t.Fatal(err)
			}
			other := w.store.mustScope(t, ctx, ws.WorkspaceID, PersonalityAgent(otherID))
			if _, err := other.SetNotificationSetting(ctx, NotifyLevelMentions, nil, nil); err != nil {
				t.Fatal(err)
			}
			pa := w.store.mustScope(t, ctx, ws.WorkspaceID, w.agent)
			question, _, err := pa.AppendMessage(ctx, AppendInput{PlaceID: place.PlaceID, Content: "質問", ClientNonce: "question"})
			if err != nil {
				t.Fatal(err)
			}
			text := "回答"
			if mentionOther {
				text += " @Shiro"
			}
			sender := w.store.mustScope(t, ctx, ws.WorkspaceID, w.humanB)
			if _, _, err := sender.AppendMessage(ctx, AppendInput{PlaceID: place.PlaceID, Content: text, ReplyTo: question.MessageID, ClientNonce: "answer"}); err != nil {
				t.Fatal(err)
			}
			d := newAttentionDelivery()
			if _, err := w.store.core.DeliverAgentAttention(ctx, d, 10); err != nil {
				t.Fatal(err)
			}
			counts := map[string]int{}
			for _, event := range d.events {
				counts[event.PersonalityAgentID]++
			}
			otherWant := 0
			if mentionOther {
				otherWant = 1
			}
			if counts[w.agent.ID] != 1 || counts[otherID] != otherWant || len(d.events) != 1+otherWant {
				t.Fatalf("recipients: %+v", counts)
			}
		})
	}
}

func TestAgentAttentionReplyDoesNotRejoinFormerThreadParticipant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	ws, channel := w.workspaceWithChannel(t, ctx)
	pa := w.store.mustScope(t, ctx, ws.WorkspaceID, w.agent)
	thread, _, err := pa.CreateThread(ctx, channel.PlaceID, "退出した相談", "", "thread")
	if err != nil {
		t.Fatal(err)
	}
	question, _, err := pa.AppendMessage(ctx, AppendInput{PlaceID: thread.Place.PlaceID, Content: "質問", ClientNonce: "question"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.pool.Exec(ctx, "UPDATE place_members SET left_at=now() WHERE place_id=$1 AND member_id=$2", thread.Place.PlaceID, w.agent.ID); err != nil {
		t.Fatal(err)
	}
	sender := w.store.mustScope(t, ctx, ws.WorkspaceID, w.humanB)
	if _, _, err := sender.AppendMessage(ctx, AppendInput{PlaceID: thread.Place.PlaceID, Content: "回答", ReplyTo: question.MessageID, ClientNonce: "answer"}); err != nil {
		t.Fatal(err)
	}
	d := newAttentionDelivery()
	if stats, err := w.store.core.DeliverAgentAttention(ctx, d, 10); err != nil || stats.Admitted != 0 {
		t.Fatalf("former participant: %+v %v", stats, err)
	}
	var joined int
	if err := w.store.pool.QueryRow(ctx, "SELECT count(*) FROM place_members WHERE place_id=$1 AND member_id=$2 AND left_at IS NULL", thread.Place.PlaceID, w.agent.ID).Scan(&joined); err != nil || joined != 0 {
		t.Fatalf("reply rejoined PA: %d %v", joined, err)
	}
}
