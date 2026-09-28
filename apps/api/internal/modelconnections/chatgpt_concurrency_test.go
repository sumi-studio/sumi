package modelconnections

import (
	"context"

	"fmt"
	"github.com/google/uuid"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// slowIssuer delays the issuer's answer to matching requests so a caller
// can go away, or another request can run, while it is outstanding.
type slowIssuer struct {
	inner *fakeIssuer
	match func(*http.Request) bool
	delay time.Duration
}

func (h slowIssuer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.match(r) {
		time.Sleep(h.delay)
	}
	h.inner.ServeHTTP(w, r)
}

func isDevicePoll(r *http.Request) bool { return r.URL.Path == "/api/accounts/deviceauth/token" }
func isCodeExchange(r *http.Request) bool {
	return r.URL.Path == "/oauth/token" && strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded")
}

func slowChatGPTFixture(t *testing.T, match func(*http.Request) bool, delay time.Duration) (*Store, *fakeIssuer, *clock) {
	t.Helper()
	s := fixture(t)
	issuer := &fakeIssuer{t: t, account: "acct-owner"}
	srv := httptest.NewServer(slowIssuer{inner: issuer, match: match, delay: delay})
	t.Cleanup(srv.Close)
	clk := &clock{now: time.Now()}
	oc := NewOAuthClient()
	oc.Issuer = srv.URL
	oc.Now = clk.Now
	if err := s.EnableChatGPT(oc); err != nil {
		t.Fatal(err)
	}
	return s, issuer, clk
}

func storedGrant(t *testing.T, s *Store, human, id string) chatGPTPayload {
	t.Helper()
	var ct []byte
	var acct string
	if err := s.pool.QueryRow(context.Background(), `SELECT credential_ciphertext,account_id FROM model_api_connections WHERE human_id=$1 AND connection_id=$2`, human, id).Scan(&ct, &acct); err != nil {
		t.Fatal(err)
	}
	p, err := s.openChatGPT(human, id, acct, ct)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func loginStatuses(t *testing.T, s *Store, human string) map[string]int {
	t.Helper()
	rows, err := s.pool.Query(context.Background(), `SELECT status FROM model_chatgpt_logins WHERE human_id=$1`, human)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var st string
		if err := rows.Scan(&st); err != nil {
			t.Fatal(err)
		}
		out[st]++
	}
	return out
}

// Regression (review F1): the issuer rotates the refresh token as soon as
// it answers. A caller whose request ends mid-refresh (Core's state-call
// deadline, a cancelled Worker request) must not lose the rotated grant;
// before the fix the old token stayed stored and the next refresh was
// answered with refresh_token_reused → reconnect_required.
func TestChatGPTRotatedGrantSurvivesCallerCancellation(t *testing.T) {
	s, issuer, clk := chatGPTFixture(t)
	c := connect(t, s, issuer, clk, owner, "")
	old := storedGrant(t, s, owner, c.ID)
	issuer.mu.Lock()
	issuer.refreshDelay = 400 * time.Millisecond
	issuer.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	// The backend rejected the stored access token; the Core reports it.
	a, err := s.ResolveChatGPT(ctx, owner, c.ID, TokenDigest(old.AccessToken))
	if err != nil {
		t.Fatalf("resolve after the caller left: %v", err)
	}
	if ctx.Err() == nil {
		t.Fatal("the caller's context should have ended during the refresh")
	}
	if issuer.refreshes.Load() != 1 {
		t.Fatalf("refreshes=%d", issuer.refreshes.Load())
	}
	stored := storedGrant(t, s, owner, c.ID)
	if stored.RefreshToken == old.RefreshToken || stored.RefreshToken != "refresh-2" || stored.AccessToken != a.AccessToken {
		t.Fatalf("rotated grant not stored: old=%s stored=%s", old.RefreshToken, stored.RefreshToken)
	}
	// The issuer now treats the old refresh token as spent. The next use
	// needs no refresh; a later refresh presents the rotated token.
	issuer.mu.Lock()
	issuer.refreshDelay = 0
	issuer.mu.Unlock()
	next, err := s.ResolveChatGPT(context.Background(), owner, c.ID, "")
	if err != nil || next.AccessToken != a.AccessToken || issuer.refreshes.Load() != 1 {
		t.Fatalf("next resolve %v refreshes=%d", err, issuer.refreshes.Load())
	}
	if _, err := s.ResolveChatGPT(context.Background(), owner, c.ID, TokenDigest(a.AccessToken)); err != nil {
		t.Fatal(err)
	}
	issuer.mu.Lock()
	presented := issuer.lastRefresh
	issuer.mu.Unlock()
	if presented != "refresh-2" {
		t.Fatalf("second refresh presented %q, want the rotated token", presented)
	}
}

// Regression (review F1, login side): the browser request that triggers
// the completing poll goes away while the one-time code is being
// exchanged. The grant is still saved and the login completes.
func TestChatGPTCodeExchangeSurvivesCallerCancellation(t *testing.T) {
	s, issuer, clk := slowChatGPTFixture(t, isCodeExchange, 400*time.Millisecond)
	bg := context.Background()
	v, err := s.BeginChatGPTLogin(bg, owner, "session-a", "", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	issuer.mu.Lock()
	issuer.authorized = true
	issuer.mu.Unlock()
	clk.Add(6 * time.Second)
	ctx, cancel := context.WithTimeout(bg, 100*time.Millisecond)
	defer cancel()
	got, err := s.PollChatGPTLogin(ctx, owner, "session-a", v.LoginID)
	if err != nil || got.Status != "completed" || got.Connection == nil {
		t.Fatalf("poll after the caller left: %+v %v", got, err)
	}
	if issuer.exchanges.Load() != 1 {
		t.Fatalf("exchanges=%d", issuer.exchanges.Load())
	}
	again, err := s.PollChatGPTLogin(bg, owner, "session-a", v.LoginID)
	if err != nil || again.Status != "completed" || again.Connection == nil || again.Connection.ID != got.Connection.ID {
		t.Fatalf("re-read %+v %v", again, err)
	}
	if storedGrant(t, s, owner, got.Connection.ID).RefreshToken == "" {
		t.Fatal("grant not stored")
	}
}

// Regression (review F2): a new sign-in started while the previous one is
// completing used to deadlock (Postgres aborted one side, either failing
// the new login or losing the exchanged code). Now the new login waits for
// the completing one, which keeps its saved grant.
func TestChatGPTBeginDuringCompletingPoll(t *testing.T) {
	s, issuer, clk := slowChatGPTFixture(t, isDevicePoll, 700*time.Millisecond)
	ctx := context.Background()
	v, err := s.BeginChatGPTLogin(ctx, owner, "session-a", "", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	issuer.mu.Lock()
	issuer.authorized = true
	issuer.mu.Unlock()
	clk.Add(6 * time.Second)
	var wg sync.WaitGroup
	var polled, begun LoginView
	var pollErr, beginErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		polled, pollErr = s.PollChatGPTLogin(ctx, owner, "session-a", v.LoginID)
	}()
	go func() {
		defer wg.Done()
		time.Sleep(200 * time.Millisecond)
		begun, beginErr = s.BeginChatGPTLogin(ctx, owner, "session-b", "", uuid.NewString())
	}()
	wg.Wait()
	if pollErr != nil || polled.Status != "completed" || polled.Connection == nil {
		t.Fatalf("poll %+v %v", polled, pollErr)
	}
	if beginErr != nil || begun.Status != "pending" {
		t.Fatalf("begin %+v %v", begun, beginErr)
	}
	if issuer.exchanges.Load() != 1 {
		t.Fatalf("exchanges=%d", issuer.exchanges.Load())
	}
	if st := loginStatuses(t, s, owner); st["completed"] != 1 || st["pending"] != 1 || st["cancelled"] != 0 {
		t.Fatalf("login statuses %v", st)
	}
	storedGrant(t, s, owner, polled.Connection.ID)
}

// An explicit cancel that arrives while the poll is completing waits for
// it and reports the truth: the login completed and the grant is saved.
func TestChatGPTCancelDuringCompletingPoll(t *testing.T) {
	s, issuer, clk := slowChatGPTFixture(t, isDevicePoll, 700*time.Millisecond)
	ctx := context.Background()
	v, err := s.BeginChatGPTLogin(ctx, owner, "session-a", "", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	issuer.mu.Lock()
	issuer.authorized = true
	issuer.mu.Unlock()
	clk.Add(6 * time.Second)
	var wg sync.WaitGroup
	var polled, cancelled LoginView
	var pollErr, cancelErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		polled, pollErr = s.PollChatGPTLogin(ctx, owner, "session-a", v.LoginID)
	}()
	go func() {
		defer wg.Done()
		time.Sleep(200 * time.Millisecond)
		cancelled, cancelErr = s.CancelChatGPTLogin(ctx, owner, "session-a", v.LoginID)
	}()
	wg.Wait()
	if pollErr != nil || polled.Status != "completed" || cancelErr != nil || cancelled.Status != "completed" {
		t.Fatalf("poll %+v %v cancel %+v %v", polled, pollErr, cancelled, cancelErr)
	}
}

// Concurrent sign-in starts by the same person from several browser
// sessions all succeed and leave exactly one pending login.
func TestChatGPTConcurrentBeginsLeaveOnePending(t *testing.T) {
	s, _, _ := chatGPTFixture(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make([]error, 6)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = s.BeginChatGPTLogin(ctx, owner, fmt.Sprint("session-", i), "", uuid.NewString())
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if st := loginStatuses(t, s, owner); st["pending"] != 1 || st["cancelled"] != len(errs)-1 {
		t.Fatalf("login statuses %v", st)
	}
}
