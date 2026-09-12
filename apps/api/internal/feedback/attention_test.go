package feedback

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

type fakeAttention struct {
	events   []AttentionEvent
	accepted map[string]bool
	loseAck  bool
	prepared int
}

func (f *fakeAttention) Prepare(context.Context, string) (func(), error) {
	f.prepared++
	return func() {}, nil
}
func (f *fakeAttention) Lookup(_ context.Context, key string, _ AttentionEvent) (bool, error) {
	return f.accepted[key], nil
}
func (f *fakeAttention) Admit(_ context.Context, key string, e AttentionEvent) error {
	if f.accepted == nil {
		f.accepted = map[string]bool{}
	}
	f.accepted[key] = true
	f.events = append(f.events, e)
	if f.loseAck {
		f.loseAck = false
		return errors.New("reply lost after durable admission")
	}
	return nil
}
func TestFeedbackAttentionReconcilesLostAckWithOriginalSender(t *testing.T) {
	w := fixture(t)
	ctx := context.Background()
	if _, err := w.pool.Exec(ctx, `UPDATE humans SET display_name='Original developer' WHERE human_id=$1`, w.dev.ID); err != nil {
		t.Fatal(err)
	}
	thread, err := w.s.Create(ctx, w.pa, "通知の相談", "Channel does not notify", uuid.NewString(), nil)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := w.s.Reply(ctx, w.dev, thread.ID, "Fixed, please try again", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.pool.Exec(ctx, `UPDATE humans SET display_name='Renamed developer' WHERE human_id=$1`, w.dev.ID); err != nil {
		t.Fatal(err)
	}
	d := &fakeAttention{loseAck: true}
	if err = w.s.DeliverAttention(ctx, d, 10); err == nil {
		t.Fatal("lost acknowledgment did not surface")
	}
	if len(d.events) != 1 {
		t.Fatalf("admissions: %d", len(d.events))
	}
	e := d.events[0]
	if e.Actor.Participant.HumanID != w.dev.ID || e.Actor.DisplayName != "Original developer" || e.PersonalityAgentID != w.pa.ID || e.EventID != reply.ID || e.Kind != "feedback_reply" || e.Revision != reply.Revision {
		t.Fatalf("wrong provenance: %+v", e)
	}
	p, c, err := (&AttentionGateway{TenantID: "test"}).input(e)
	if err != nil {
		t.Fatal(err)
	}
	if p.Actor.PrincipalID != w.dev.ID || p.Actor.PrincipalID == w.human.ID || p.Source.Surface != "feedback" || p.Source.OccurredAt == "" {
		t.Fatalf("sender became recipient: %+v", p)
	}
	var command map[string]any
	_ = json.Unmarshal(c, &command)
	if command["content"] != reply.Body {
		t.Fatal("body changed")
	}
	// Recovery acknowledges the prior admission without a second delivery.
	if _, err = w.pool.Exec(ctx, `UPDATE feedback_attention_outbox SET next_attempt_at=now()`); err != nil {
		t.Fatal(err)
	}
	if err = w.s.DeliverAttention(ctx, d, 10); err != nil {
		t.Fatal(err)
	}
	if len(d.events) != 1 || d.prepared != 1 {
		t.Fatal("recovery duplicated delivery or restarted agent")
	}
	var outcome string
	if err = w.pool.QueryRow(ctx, `SELECT outcome FROM feedback_attention_outbox WHERE event_id=$1 AND recipient_paid=$2`, reply.ID, w.pa.ID).Scan(&outcome); err != nil || outcome != "admitted" {
		t.Fatalf("receipt: %s %v", outcome, err)
	}
}
func TestFeedbackAttentionBuiltinRecipientAndSelfEcho(t *testing.T) {
	w := fixture(t)
	ctx := context.Background()
	w.s.recipients = append(w.s.recipients, w.pa)
	thread, err := w.s.Create(ctx, w.pa, "相談", "Body", uuid.NewString(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err = w.pool.QueryRow(ctx, `SELECT count(*) FROM feedback_attention_outbox`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("self echo queued: %d %v", n, err)
	}
	if _, err = w.s.Reply(ctx, w.dev, thread.ID, "Reply", uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	d := &fakeAttention{}
	if err = w.s.DeliverAttention(ctx, d, 10); err != nil {
		t.Fatal(err)
	}
	if len(d.events) != 1 || d.prepared != 1 {
		t.Fatal("built-in recipient was not notified")
	}
	var outcome string
	if err = w.pool.QueryRow(ctx, `SELECT outcome FROM feedback_attention_outbox`).Scan(&outcome); err != nil || outcome != "admitted" {
		t.Fatalf("suppression: %s %v", outcome, err)
	}
}

type timedOutFirstAttention struct {
	fakeAttention
	failedRecipient string
}

func (d *timedOutFirstAttention) Prepare(ctx context.Context, id string) (func(), error) {
	if d.failedRecipient == "" {
		d.failedRecipient = id
		// Inject the runtime preparation timeout without imposing a short
		// wall-clock deadline on unrelated real database operations.
		return nil, context.DeadlineExceeded
	}
	return d.fakeAttention.Prepare(ctx, id)
}
func TestFeedbackAttentionTimedOutRecipientDoesNotStarveAnother(t *testing.T) {
	w := fixture(t)
	ctx := context.Background()
	paid, err := koseki.New(w.pool).MintSecretary(ctx, w.dev.ID)
	if err != nil {
		t.Fatal(err)
	}
	other := participant.PersonalityAgent(paid)
	w.s.recipients = []participant.Ref{w.pa, other}
	if _, err = w.s.Create(ctx, w.human, "相談", "内容", uuid.NewString(), nil); err != nil {
		t.Fatal(err)
	}
	var deliveryStartedAt time.Time
	if err = w.pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&deliveryStartedAt); err != nil {
		t.Fatal(err)
	}
	d := &timedOutFirstAttention{}
	if err = w.s.DeliverAttention(ctx, d, 10); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout: %v", err)
	}
	if len(d.events) != 1 || d.prepared != 1 {
		t.Fatalf("healthy recipient admissions=%d preparations=%d", len(d.events), d.prepared)
	}
	if d.events[0].PersonalityAgentID == d.failedRecipient {
		t.Fatal("timed-out recipient was admitted instead of the healthy recipient")
	}
	var delayed, admitted int
	if err = w.pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE recipient_paid=$1 AND next_attempt_at >= $2::timestamptz+interval '15 seconds' AND finished_at IS NULL),count(*) FILTER (WHERE recipient_paid=$3 AND outcome='admitted') FROM feedback_attention_outbox`, d.failedRecipient, deliveryStartedAt, d.events[0].PersonalityAgentID).Scan(&delayed, &admitted); err != nil {
		t.Fatal(err)
	}
	if delayed != 1 || admitted != 1 {
		t.Fatalf("bad retry schedule: delayed=%d admitted=%d", delayed, admitted)
	}
}
