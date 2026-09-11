package feedback

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/sumi-studio/sumi/apps/api/internal/apps"
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
	thread, err := w.s.Create(ctx, w.pa, "通知の相談", "Channel does not notify", uuid.NewString())
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
	// Recovery acknowledges the prior admission even after eligibility is lost.
	if _, err = w.pool.Exec(ctx, `UPDATE app_installations SET enabled=false WHERE owner_id=$1 AND app_id='feedback'`, w.pa.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = w.pool.Exec(ctx, `UPDATE feedback_attention_outbox SET next_attempt_at=now()`); err != nil {
		t.Fatal(err)
	}
	if err = w.s.DeliverAttention(ctx, d, 10); err != nil {
		t.Fatal(err)
	}
	if len(d.events) != 1 || d.prepared != 1 {
		t.Fatal("recovery duplicated delivery or restarted disabled agent")
	}
	var outcome string
	if err = w.pool.QueryRow(ctx, `SELECT outcome FROM feedback_attention_outbox WHERE event_id=$1 AND recipient_paid=$2`, reply.ID, w.pa.ID).Scan(&outcome); err != nil || outcome != "admitted" {
		t.Fatalf("receipt: %s %v", outcome, err)
	}
}
func TestFeedbackAttentionSuppressesUnavailableRecipientAndSelfEcho(t *testing.T) {
	w := fixture(t)
	ctx := context.Background()
	w.s.recipients = append(w.s.recipients, w.pa)
	thread, err := w.s.Create(ctx, w.pa, "相談", "Body", uuid.NewString())
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
	if _, err = w.pool.Exec(ctx, `UPDATE app_installations SET enabled=false WHERE owner_kind=$1 AND owner_id=$2 AND app_id='feedback'`, participant.KindPersonalityAgent, w.pa.ID); err != nil {
		t.Fatal(err)
	}
	d := &fakeAttention{}
	if err = w.s.DeliverAttention(ctx, d, 10); err != nil {
		t.Fatal(err)
	}
	if len(d.events) != 0 || d.prepared != 0 {
		t.Fatal("disabled recipient was started or notified")
	}
	var outcome string
	if err = w.pool.QueryRow(ctx, `SELECT outcome FROM feedback_attention_outbox`).Scan(&outcome); err != nil || outcome != "suppressed" {
		t.Fatalf("suppression: %s %v", outcome, err)
	}
}

type blockedFirstAttention struct {
	fakeAttention
	first bool
}

func (d *blockedFirstAttention) Prepare(ctx context.Context, _ string) (func(), error) {
	if !d.first {
		d.first = true
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return func() {}, nil
}
func TestFeedbackAttentionSlowRecipientDoesNotStarveAnother(t *testing.T) {
	w := fixture(t)
	ctx := context.Background()
	paid, err := koseki.New(w.pool).MintSecretary(ctx, w.dev.ID)
	if err != nil {
		t.Fatal(err)
	}
	other := participant.PersonalityAgent(paid)
	if _, err = apps.New(w.pool, nil).InstallAtOperation(ctx, apps.ParticipantOwner(other), other, AppID, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	w.s.recipients = []participant.Ref{w.pa, other}
	if _, err = w.s.Create(ctx, w.human, "相談", "内容", uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	d := &blockedFirstAttention{}
	if err = w.s.deliverAttention(ctx, d, 10, 100*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout: %v", err)
	}
	if len(d.events) != 1 {
		t.Fatalf("healthy recipient did not receive: %d", len(d.events))
	}
	var delayed, admitted int
	if err = w.pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE next_attempt_at>now() AND finished_at IS NULL),count(*) FILTER (WHERE outcome='admitted') FROM feedback_attention_outbox`).Scan(&delayed, &admitted); err != nil {
		t.Fatal(err)
	}
	if delayed != 1 || admitted != 1 {
		t.Fatalf("bad retry schedule: delayed=%d admitted=%d", delayed, admitted)
	}
}
