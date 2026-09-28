package modelconnections

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Promoted from the independent review probes; originals remain in its artifacts.
func TestChatGPTRepairReopenAfterCompletedResponseLost(t *testing.T) {
	s, issuer, clk := chatGPTFixture(t)
	ctx := context.Background()
	first, err := s.BeginChatGPTLogin(ctx, owner, "same-browser", "", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	issuer.mu.Lock()
	issuer.authorized = true
	issuer.mu.Unlock()
	clk.Add(6 * time.Second)
	completed, err := s.PollChatGPTLogin(ctx, owner, "same-browser", first.LoginID)
	if err != nil || completed.Connection == nil {
		t.Fatalf("complete: %+v %v", completed, err)
	}
	// Discard the HTTP response and all component-local state, as on reload.
	reopened, err := s.BeginChatGPTLogin(ctx, owner, "same-browser", "", first.LoginID)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("stored completion=%s; reopened status=%s same_login=%v; statuses=%v", completed.Status, reopened.Status, reopened.LoginID == first.LoginID, loginStatuses(t, s, owner))
	if reopened.Status != "completed" || reopened.LoginID != first.LoginID {
		t.Fatalf("reopen starts a second device login although the first grant is already saved")
	}
	if reopened.Connection == nil || reopened.Connection.ID != completed.Connection.ID || issuer.begins.Load() != 1 || issuer.exchanges.Load() != 1 {
		t.Fatal("recovery must return the saved connection without issuer traffic")
	}
	additional, err := s.BeginChatGPTLogin(ctx, owner, "same-browser", "", uuid.NewString())
	if err != nil || additional.LoginID == first.LoginID || additional.Status != "pending" || issuer.begins.Load() != 2 {
		t.Fatalf("an intentional additional connection must remain possible: %+v %v", additional, err)
	}
}

type reviewTraceKey struct{}
type reviewBeforeHuman struct {
	reached, release chan struct{}
	once             sync.Once
}

func (h *reviewBeforeHuman) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if ctx.Value(reviewTraceKey{}) == true && strings.HasPrefix(data.SQL, "SELECT human_id::text FROM humans") {
		h.once.Do(func() { close(h.reached); <-h.release })
	}
	return ctx
}
func (*reviewBeforeHuman) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func reviewWait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("probe scheduling timed out")
	}
}

func TestChatGPTRepairBeginSerializesWithNewlyPollingLogin(t *testing.T) {
	s, issuer, clk := chatGPTFixture(t)
	bg := context.Background()
	hook := &reviewBeforeHuman{reached: make(chan struct{}), release: make(chan struct{})}
	cfg := s.pool.Config()
	cfg.ConnConfig.Tracer = hook
	pool, err := pgxpool.NewWithConfig(bg, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s.pool = pool
	// Pause a begin just before its human lock. In the broken ordering it
	// had already checked pending rows here. Another begin/poll wins this gap.
	beginDone := make(chan struct{})
	var b LoginView
	var beginErr error
	go func() {
		defer close(beginDone)
		b, beginErr = s.BeginChatGPTLogin(context.WithValue(bg, reviewTraceKey{}, true), owner, "newer-session", "", uuid.NewString())
	}()
	reviewWait(t, hook.reached)
	a, err := s.BeginChatGPTLogin(bg, owner, "older-session", "", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	// The older session starts its regular pending poll while the first
	// begin is descheduled. The real issuer has not authorized anything yet.
	pollEntered, releasePoll := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isDevicePoll(r) {
			close(pollEntered)
			<-releasePoll
		}
		issuer.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	s.oauth.Issuer = server.URL
	clk.Add(6 * time.Second)
	pollDone := make(chan struct{})
	var p LoginView
	var pollErr error
	go func() { defer close(pollDone); p, pollErr = s.PollChatGPTLogin(bg, owner, "older-session", a.LoginID) }()
	reviewWait(t, pollEntered)
	close(hook.release)
	select {
	case <-beginDone:
		close(releasePoll)
		reviewWait(t, pollDone)
		t.Fatal("begin skipped a polling login instead of serializing replacement")
	case <-time.After(100 * time.Millisecond):
	}
	close(releasePoll)
	reviewWait(t, beginDone)
	reviewWait(t, pollDone)
	if beginErr != nil || pollErr != nil {
		t.Fatalf("begin=%v poll=%v", beginErr, pollErr)
	}
	statuses := loginStatuses(t, s, owner)
	t.Logf("new begin=%s; older poll=%s; statuses=%v", b.Status, p.Status, statuses)
	if statuses["pending"] != 1 {
		t.Fatalf("two independent sign-ins remain valid after the newer begin: %v", statuses)
	}
}

func TestChatGPTRepairCancellationSurvivesCallerDeadline(t *testing.T) {
	t.Parallel()
	s, _, clk := slowChatGPTFixture(t, isDevicePoll, 400*time.Millisecond)
	bg := context.Background()
	v, err := s.BeginChatGPTLogin(bg, owner, "same-browser", "", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	clk.Add(6 * time.Second)
	done := make(chan struct{})
	var polled LoginView
	var pollErr error
	go func() { defer close(done); polled, pollErr = s.PollChatGPTLogin(bg, owner, "same-browser", v.LoginID) }()
	// The caller disconnects while cancellation waits on a pending poll.
	// The original review also reproduced this at the old UI's 15s deadline.
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(bg, 100*time.Millisecond)
	defer cancel()
	cancelled, cancelErr := s.CancelChatGPTLogin(ctx, owner, "same-browser", v.LoginID)
	if cancelErr != nil || cancelled.Status != "cancelled" || !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("cancel=%+v err=%v caller=%v", cancelled, cancelErr, ctx.Err())
	}
	<-done
	if pollErr != nil {
		t.Fatal(pollErr)
	}
	reopened, err := s.BeginChatGPTLogin(bg, owner, "same-browser", "", v.LoginID)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("cancel_deadline=%v; poll=%s; reopen=%s same_login=%v", errors.Is(ctx.Err(), context.DeadlineExceeded), polled.Status, reopened.Status, reopened.LoginID == v.LoginID)
	if reopened.LoginID != v.LoginID || reopened.Status != "cancelled" {
		t.Fatal("explicit cancel silently left the same pending login resumable")
	}
}

func TestChatGPTRepairRefreshLockWaitDoesNotSpendIssuerOrSaveBudget(t *testing.T) {
	t.Parallel()
	s, issuer, clk := chatGPTFixture(t)
	c := connect(t, s, issuer, clk, owner, "")
	old := storedGrant(t, s, owner, c.ID)
	var count atomic.Int32
	firstEntered, rotated := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/oauth/token" {
			issuer.ServeHTTP(w, r)
			return
		}
		switch count.Add(1) {
		case 1:
			close(firstEntered)
			time.Sleep(19 * time.Second)
			w.WriteHeader(503)
			_, _ = w.Write([]byte(`{"error":"temporarily_unavailable"}`))
		case 2:
			// The issuer consumes the refresh token, then its response is slow.
			fresh := issuer.tokens("acct-owner")
			time.Sleep(12 * time.Second)
			_ = json.NewEncoder(w).Encode(fresh)
			close(rotated)
		default:
			w.WriteHeader(401)
			_, _ = w.Write([]byte(`{"error":"refresh_token_reused"}`))
		}
	}))
	t.Cleanup(server.Close)
	s.oauth.Issuer = server.URL
	bg := context.Background()
	firstDone := make(chan struct{})
	var firstErr error
	go func() {
		defer close(firstDone)
		_, firstErr = s.ResolveChatGPT(bg, owner, c.ID, TokenDigest(old.AccessToken))
	}()
	<-firstEntered
	_, secondErr := s.ResolveChatGPT(bg, owner, c.ID, TokenDigest(old.AccessToken))
	if secondErr == nil || count.Load() != 1 {
		t.Fatalf("lock waiter must leave without contacting issuer: calls=%d err=%v", count.Load(), secondErr)
	}
	<-firstDone
	_, nextErr := s.ResolveChatGPT(bg, owner, c.ID, TokenDigest(old.AccessToken))
	<-rotated
	saved := storedGrant(t, s, owner, c.ID)
	if nextErr != nil {
		t.Fatal(nextErr)
	}
	t.Logf("first=%v; waiter=%v; rotated_grant_stored=%v; next_reconnect_required=%v", firstErr, secondErr, saved.RefreshToken != old.RefreshToken, errors.Is(nextErr, ErrReconnectRequired))
	if saved.RefreshToken == old.RefreshToken || saved.RefreshToken != "refresh-2" {
		t.Fatal("30-second transaction deadline spent waiting on the first refresh discards the second rotated grant")
	}
}

func TestChatGPTRepairConcurrentSameAttemptAndScope(t *testing.T) {
	s, issuer, _ := chatGPTFixture(t)
	ctx := context.Background()
	id := uuid.NewString()
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func(i int) { defer wg.Done(); _, errs[i] = s.BeginChatGPTLogin(ctx, owner, "session", "", id) }(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if issuer.begins.Load() != 1 || loginStatuses(t, s, owner)["pending"] != 1 {
		t.Fatal("same intent issued multiple codes")
	}
	for _, pair := range [][2]string{{other, "session"}, {owner, "another-session"}} {
		if _, err := s.BeginChatGPTLogin(ctx, pair[0], pair[1], "", id); !errors.Is(err, ErrNotFound) {
			t.Fatalf("rebound login identity: %v", err)
		}
	}
	if issuer.begins.Load() != 1 {
		t.Fatal("cross-session recovery contacted issuer")
	}
}

func TestChatGPTRepairReconnectAcquiresAllLocksBeforeExchange(t *testing.T) {
	for _, resource := range []string{"human", "connection"} {
		t.Run(resource, func(t *testing.T) {
			t.Parallel()
			s, issuer, clk := chatGPTFixture(t)
			ctx := context.Background()
			c := connect(t, s, issuer, clk, owner, "")
			v, err := s.BeginChatGPTLogin(ctx, owner, "session", c.ID, uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			clk.Add(6 * time.Second)
			tx, err := s.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer rollbackChatGPT(tx)
			if resource == "human" {
				err = lockHuman(ctx, tx, owner)
			} else {
				err = lockChatGPTTarget(ctx, tx, owner, c.ID)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.PollChatGPTLogin(ctx, owner, "session", v.LoginID); err == nil {
				t.Fatal("poll should stop at bounded lock wait")
			}
			if issuer.exchanges.Load() != 1 {
				t.Fatal("one-time code consumed before all persistence locks were held")
			}
			rollbackChatGPT(tx)
			got, err := s.PollChatGPTLogin(ctx, owner, "session", v.LoginID)
			if err != nil || got.Status != "completed" || got.Connection == nil || got.Connection.ID != c.ID || issuer.exchanges.Load() != 2 {
				t.Fatalf("retry: %+v %v", got, err)
			}
		})
	}
}
