package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/authemail"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

// scriptedEmailSender returns queued errors in order and records every
// message the delivery worker hands it.
type scriptedEmailSender struct {
	mu       sync.Mutex
	errs     []error
	messages []authemail.Message
}

func (s *scriptedEmailSender) Send(ctx context.Context, message authemail.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, message)
	if len(s.errs) == 0 {
		return nil
	}
	err := s.errs[0]
	s.errs = s.errs[1:]
	return err
}

func (s *scriptedEmailSender) script(errs ...error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errs = append(s.errs, errs...)
}

func (s *scriptedEmailSender) sent() []authemail.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]authemail.Message(nil), s.messages...)
}

func deliveryRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, flowID string) (status, class string, attempts int) {
	t.Helper()
	err := pool.QueryRow(ctx, `SELECT d.status, COALESCE(d.failure_class, ''), d.attempts
		FROM auth_email_deliveries d JOIN auth_email_challenges c USING (challenge_id)
		WHERE c.flow_id=$1 ORDER BY d.created_at DESC LIMIT 1`, flowID).Scan(&status, &class, &attempts)
	if err != nil {
		t.Fatalf("delivery row: %v", err)
	}
	return status, class, attempts
}

// TestEmailDeliveryWorkerMapsSenderOutcomes drives the real durable queue:
// a transient failure keeps the intent pending for retry, a permanent
// rejection fails it, and a clean send records sent. The sender seam is the
// same one SMTPSender implements.
func TestEmailDeliveryWorkerMapsSenderOutcomes(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	store.EmailChallengeKey = &koseki.EmailChallengeKey{
		KeyID: "worker-smtp/v1", Key: []byte("0123456789abcdef0123456789abcdef"),
	}
	sender := &scriptedEmailSender{}
	worker := newEmailDeliveryWorker(store, sender, "https://app.example.test")

	startFlow := func(email string) string {
		t.Helper()
		flow, _, err := store.StartEmailCodeFlow(ctx, koseki.StartAuthFlowRequest{
			Intent: koseki.IntentSignIn, Channel: koseki.ChannelEmailCode,
			ExpectedProvider: koseki.EmailCodeSignInProvider, NormalizedEmail: email,
			Continuation: "/direct-chat", Nonce: controllerNonce(t), TTL: koseki.MaxFlowTTL,
		})
		if err != nil {
			t.Fatalf("start flow: %v", err)
		}
		return flow.FlowID
	}
	claimOne := func() koseki.EmailDelivery {
		t.Helper()
		items, err := store.ClaimEmailDeliveries(ctx, 10, time.Minute)
		if err != nil || len(items) != 1 {
			t.Fatalf("claim: %v %d", err, len(items))
		}
		return items[0]
	}

	// Transient failure: the intent goes back to pending with a future retry.
	sender.script(errors.New("smtp 451 temporary failure"))
	flowID := startFlow("worker-transient@example.test")
	worker.deliver(ctx, claimOne())
	status, class, attempts := deliveryRow(t, ctx, pool, flowID)
	if status != "pending" || class != "" || attempts != 1 {
		t.Fatalf("transient outcome: status=%s class=%s attempts=%d", status, class, attempts)
	}

	// Permanent rejection on the retry: the intent fails with class permanent.
	sender.script(authemail.Permanent(errors.New("smtp 550 mailbox unavailable")))
	if _, err := pool.Exec(ctx, `UPDATE auth_email_deliveries SET next_attempt_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	worker.deliver(ctx, claimOne())
	status, class, attempts = deliveryRow(t, ctx, pool, flowID)
	if status != "failed" || class != "permanent" || attempts != 2 {
		t.Fatalf("permanent outcome: status=%s class=%s attempts=%d", status, class, attempts)
	}

	// Clean send: the rendered challenge reaches the sender and the intent
	// records sent.
	flowID = startFlow("worker-ok@example.test")
	worker.deliver(ctx, claimOne())
	status, class, _ = deliveryRow(t, ctx, pool, flowID)
	if status != "sent" || class != "" {
		t.Fatalf("sent outcome: status=%s class=%s", status, class)
	}
	messages := sender.sent()
	if len(messages) != 3 {
		t.Fatalf("sender calls: %d", len(messages))
	}
	last := messages[2]
	if last.To != "worker-ok@example.test" || !strings.Contains(last.Subject, "確認コード") ||
		!strings.Contains(last.Text, "email-sign-in#challenge=") {
		t.Fatalf("rendered message lost the code-first content: %+v", last)
	}
}
