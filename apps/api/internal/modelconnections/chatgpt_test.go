package modelconnections

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/browseridentity"
)

// fakeIssuer is a synthetic ChatGPT OAuth issuer speaking the wire shapes
// of openai/codex's login client (device code, form-encoded code exchange,
// JSON refresh). Tokens are unsigned JWT-shaped strings carrying the
// claims the client reads.
type fakeIssuer struct {
	t          *testing.T
	mu         sync.Mutex
	authorized bool
	account    string
	// refreshAccount, when set, is the account the next refreshed tokens name.
	refreshAccount string
	refreshStatus  int
	refreshBody    string
	refreshDelay   time.Duration
	refreshes      atomic.Int32
	polls          atomic.Int32
	exchanges      atomic.Int32
	seq            atomic.Int32
	lastRefresh    string
	usercodeStatus int
}

func jwt(account string, exp time.Time, tag string) string {
	enc := func(v any) string {
		b, _ := json.Marshal(v)
		return base64.RawURLEncoding.EncodeToString(b)
	}
	return enc(map[string]string{"alg": "none"}) + "." + enc(map[string]any{
		"exp": exp.Unix(), "tag": tag,
		"https://api.openai.com/auth": map[string]string{"chatgpt_account_id": account},
	}) + ".sig"
}

func (f *fakeIssuer) tokens(account string) map[string]string {
	n := f.seq.Add(1)
	return map[string]string{
		"id_token":      jwt(account, time.Now().Add(time.Hour), fmt.Sprint("id", n)),
		"access_token":  jwt(account, time.Now().Add(time.Hour), fmt.Sprint("access", n)),
		"refresh_token": fmt.Sprint("refresh-", n),
	}
}

func (f *fakeIssuer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/api/accounts/deviceauth/usercode":
		if f.usercodeStatus != 0 {
			w.WriteHeader(f.usercodeStatus)
			return
		}
		var req map[string]string
		if json.Unmarshal(body, &req) != nil || req["client_id"] != chatGPTClientID {
			f.t.Errorf("usercode request %s", body)
		}
		_, _ = w.Write([]byte(`{"device_auth_id":"dev-secret-1","user_code":"ABCD-EFGH","interval":"5"}`))
	case "/api/accounts/deviceauth/token":
		f.polls.Add(1)
		var req map[string]string
		if json.Unmarshal(body, &req) != nil || req["device_auth_id"] != "dev-secret-1" || req["user_code"] != "ABCD-EFGH" {
			f.t.Errorf("poll request %s", body)
		}
		f.mu.Lock()
		ok := f.authorized
		f.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":"authorization_pending"}`))
			return
		}
		_, _ = w.Write([]byte(`{"authorization_code":"code-1","code_challenge":"ch","code_verifier":"ver-1"}`))
	case "/oauth/token":
		if strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
			f.exchanges.Add(1)
			form, _ := url.ParseQuery(string(body))
			if form.Get("grant_type") != "authorization_code" || form.Get("code") != "code-1" || form.Get("code_verifier") != "ver-1" ||
				form.Get("client_id") != chatGPTClientID || !strings.HasSuffix(form.Get("redirect_uri"), "/deviceauth/callback") {
				f.t.Errorf("exchange form %s", body)
			}
			_ = json.NewEncoder(w).Encode(f.tokens(f.account))
			return
		}
		f.refreshes.Add(1)
		var req map[string]string
		if json.Unmarshal(body, &req) != nil || req["grant_type"] != "refresh_token" || req["client_id"] != chatGPTClientID {
			f.t.Errorf("refresh request %s", body)
		}
		f.mu.Lock()
		f.lastRefresh = req["refresh_token"]
		status, failure, delay, account := f.refreshStatus, f.refreshBody, f.refreshDelay, f.account
		if f.refreshAccount != "" {
			account = f.refreshAccount
		}
		f.mu.Unlock()
		time.Sleep(delay)
		if status != 0 {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(failure))
			return
		}
		_ = json.NewEncoder(w).Encode(f.tokens(account))
	default:
		w.WriteHeader(404)
	}
}

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) Add(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func chatGPTFixture(t *testing.T) (*Store, *fakeIssuer, *clock) {
	t.Helper()
	s := fixture(t)
	issuer := &fakeIssuer{t: t, account: "acct-owner"}
	srv := httptest.NewServer(issuer)
	t.Cleanup(srv.Close)
	clk := &clock{now: time.Now()}
	c := NewOAuthClient()
	c.Issuer = srv.URL
	c.Now = clk.Now
	if err := s.EnableChatGPT(c); err != nil {
		t.Fatal(err)
	}
	return s, issuer, clk
}

// connect runs a full device login for human and returns the connection.
func connect(t *testing.T, s *Store, issuer *fakeIssuer, clk *clock, human, target string) Connection {
	t.Helper()
	ctx := context.Background()
	issuer.mu.Lock()
	issuer.authorized = false
	issuer.mu.Unlock()
	v, err := s.BeginChatGPTLogin(ctx, human, "session-"+human, target)
	if err != nil {
		t.Fatal(err)
	}
	clk.Add(6 * time.Second)
	if v, err = s.PollChatGPTLogin(ctx, human, "session-"+human, v.LoginID); err != nil || v.Status != "pending" {
		t.Fatalf("pending poll %+v %v", v, err)
	}
	issuer.mu.Lock()
	issuer.authorized = true
	issuer.mu.Unlock()
	clk.Add(6 * time.Second)
	v, err = s.PollChatGPTLogin(ctx, human, "session-"+human, v.LoginID)
	if err != nil || v.Status != "completed" || v.Connection == nil {
		t.Fatalf("completed poll %+v %v", v, err)
	}
	return *v.Connection
}

func TestChatGPTDeviceLoginStoresSealedSelectedConnection(t *testing.T) {
	s, issuer, clk := chatGPTFixture(t)
	ctx := context.Background()
	v, err := s.BeginChatGPTLogin(ctx, owner, "session-a", "")
	if err != nil {
		t.Fatal(err)
	}
	if v.Status != "pending" || v.UserCode != "ABCD-EFGH" || !strings.HasSuffix(v.VerificationURL, "/codex/device") || v.IntervalMs != 5000 {
		t.Fatalf("begin view %+v", v)
	}
	// Another person, or the same person's other browser session, cannot
	// see or drive the login.
	if _, err := s.PollChatGPTLogin(ctx, other, "session-a", v.LoginID); !errors.Is(err, ErrNotFound) {
		t.Fatal("other human poll", err)
	}
	if _, err := s.PollChatGPTLogin(ctx, owner, "session-b", v.LoginID); !errors.Is(err, ErrNotFound) {
		t.Fatal("other session poll", err)
	}
	// Before the interval elapses a read does not contact the issuer.
	if _, err := s.PollChatGPTLogin(ctx, owner, "session-a", v.LoginID); err != nil || issuer.polls.Load() != 0 {
		t.Fatal("early poll", err, issuer.polls.Load())
	}
	clk.Add(5 * time.Second)
	if v, err = s.PollChatGPTLogin(ctx, owner, "session-a", v.LoginID); err != nil || v.Status != "pending" || issuer.polls.Load() != 1 {
		t.Fatal("pending", v, err)
	}
	issuer.mu.Lock()
	issuer.authorized = true
	issuer.mu.Unlock()
	clk.Add(5 * time.Second)
	// Two concurrent reads (two tabs, or two API processes) exchange the
	// one-time code exactly once.
	var wg sync.WaitGroup
	views := make([]LoginView, 4)
	for i := range views {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			views[i], _ = s.PollChatGPTLogin(ctx, owner, "session-a", v.LoginID)
		}(i)
	}
	wg.Wait()
	if issuer.exchanges.Load() != 1 {
		t.Fatalf("code exchanged %d times", issuer.exchanges.Load())
	}
	for _, view := range views {
		if view.Status != "completed" || view.Connection == nil || view.UserCode != "" {
			t.Fatalf("completed view %+v", view)
		}
	}
	c := views[0].Connection
	if c.Preset != ChatGPTPreset || c.BaseURL != ChatGPTBaseURL || c.Model != DefaultChatGPTModel || c.ReasoningEffort != "medium" {
		t.Fatalf("connection %+v", c)
	}
	sel, ok, err := s.Selected(ctx, owner)
	if err != nil || !ok || sel.Kind != "api" || sel.ConnectionID != c.ID {
		t.Fatal("selection", sel, ok, err)
	}
	// Tokens and the device polling credential are sealed at rest.
	var cipher, device []byte
	if err := s.pool.QueryRow(ctx, `SELECT credential_ciphertext FROM model_api_connections WHERE connection_id=$1`, c.ID).Scan(&cipher); err != nil {
		t.Fatal(err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT device_ciphertext FROM model_chatgpt_logins WHERE login_id=$1`, v.LoginID).Scan(&device); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(cipher, []byte("refresh-")) || bytes.Contains(cipher, []byte("acct-owner")) || bytes.Contains(device, []byte("dev-secret")) {
		t.Fatal("plaintext secret at rest")
	}
	// The API-key paths never expose or overwrite the grant.
	if _, err := s.Resolve(ctx, owner, c.ID); !errors.Is(err, ErrInvalid) {
		t.Fatal("api-key resolve of subscription", err)
	}
	k := "sk-x"
	if _, err := s.Save(ctx, owner, c.ID, Input{Name: "x", Preset: "openai-responses", BaseURL: "https://api.openai.com/v1", Model: "m", APIKey: &k}); !errors.Is(err, ErrInvalid) {
		t.Fatal("api-key overwrite of subscription", err)
	}
	list, err := s.List(ctx, owner)
	if err != nil || len(list) != 1 || list[0].ReasoningEffort != "medium" {
		t.Fatal("list", list, err)
	}
	if others, _ := s.List(ctx, other); len(others) != 0 {
		t.Fatal("cross-user list")
	}
}

func TestChatGPTLoginExpiryCancelAndUnavailable(t *testing.T) {
	s, issuer, clk := chatGPTFixture(t)
	ctx := context.Background()
	v, err := s.BeginChatGPTLogin(ctx, owner, "s", "")
	if err != nil {
		t.Fatal(err)
	}
	clk.Add(16 * time.Minute)
	if v, err = s.PollChatGPTLogin(ctx, owner, "s", v.LoginID); err != nil || v.Status != "expired" || v.UserCode != "" {
		t.Fatal("expired", v, err)
	}
	v, _ = s.BeginChatGPTLogin(ctx, owner, "s", "")
	w, _ := s.BeginChatGPTLogin(ctx, owner, "s", "")
	if old, err := s.PollChatGPTLogin(ctx, owner, "s", v.LoginID); err != nil || old.Status != "cancelled" {
		t.Fatal("superseded login", old, err)
	}
	if c, err := s.CancelChatGPTLogin(ctx, owner, "s", w.LoginID); err != nil || c.Status != "cancelled" {
		t.Fatal("cancel", c, err)
	}
	issuer.usercodeStatus = 404
	if _, err := s.BeginChatGPTLogin(ctx, owner, "s", ""); !errors.Is(err, ErrDeviceLoginUnavailable) {
		t.Fatal("device unavailable", err)
	}
	disabled := fixture(t)
	if _, err := disabled.BeginChatGPTLogin(ctx, owner, "s", ""); !errors.Is(err, ErrChatGPTDisabled) {
		t.Fatal("disabled", err)
	}
}

func TestChatGPTRefreshSingleFlightAcrossConcurrentCallersAndRestart(t *testing.T) {
	s, issuer, clk := chatGPTFixture(t)
	ctx := context.Background()
	c := connect(t, s, issuer, clk, owner, "")
	first, err := s.ResolveChatGPT(ctx, owner, c.ID, "")
	if err != nil || first.AccountID != "acct-owner" || issuer.refreshes.Load() != 0 {
		t.Fatal("fresh token resolve", err, issuer.refreshes.Load())
	}
	// Eight simultaneous calls all saw the same token rejected (401).
	issuer.refreshDelay = 100 * time.Millisecond
	rejected := TokenDigest(first.AccessToken)
	var wg sync.WaitGroup
	got := make([]ChatGPTAccess, 8)
	errs := make([]error, 8)
	for i := range got {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got[i], errs[i] = s.ResolveChatGPT(ctx, owner, c.ID, rejected)
		}(i)
	}
	wg.Wait()
	if issuer.refreshes.Load() != 1 {
		t.Fatalf("refresh token spent %d times", issuer.refreshes.Load())
	}
	for i := range got {
		if errs[i] != nil || got[i].AccessToken == first.AccessToken || got[i].AccessToken != got[0].AccessToken {
			t.Fatalf("caller %d: %v", i, errs[i])
		}
	}
	if got[0].Version != first.Version {
		t.Fatal("a token refresh must not change the connection version")
	}
	// Restart: a new store over the same database and key keeps using the
	// rotated grant (the next refresh sends the rotated refresh token).
	restarted, err := New(s.pool, bytes.Repeat([]byte{8}, 32))
	if err != nil {
		t.Fatal(err)
	}
	oc := NewOAuthClient()
	oc.Issuer, oc.Now = s.oauth.Issuer, clk.Now
	if err := restarted.EnableChatGPT(oc); err != nil {
		t.Fatal(err)
	}
	clk.Add(58 * time.Minute) // inside the refresh margin
	after, err := restarted.ResolveChatGPT(ctx, owner, c.ID, "")
	if err != nil || after.AccessToken == got[0].AccessToken || issuer.refreshes.Load() != 2 || issuer.lastRefresh != "refresh-2" {
		t.Fatal("restart refresh", err, issuer.refreshes.Load(), issuer.lastRefresh)
	}
	// A wrong key cannot open the grant.
	wrong, _ := New(s.pool, bytes.Repeat([]byte{9}, 32))
	_ = wrong.EnableChatGPT(oc)
	if _, err := wrong.ResolveChatGPT(ctx, owner, c.ID, ""); !errors.Is(err, ErrUnavailable) {
		t.Fatal("wrong key", err)
	}
}

func TestChatGPTRevocationIsPerPersonAndNeedsReconnect(t *testing.T) {
	s, issuer, clk := chatGPTFixture(t)
	ctx := context.Background()
	mine := connect(t, s, issuer, clk, owner, "")
	issuer.account = "acct-other"
	theirs := connect(t, s, issuer, clk, other, "")
	issuer.account = "acct-owner"
	// The other person's grant cannot be resolved under my identity.
	if _, err := s.ResolveChatGPT(ctx, owner, theirs.ID, ""); !errors.Is(err, ErrNotFound) {
		t.Fatal("cross-user resolve", err)
	}
	cur, _ := s.ResolveChatGPT(ctx, owner, mine.ID, "")
	issuer.refreshStatus, issuer.refreshBody = 400, `{"error":{"code":"refresh_token_invalidated","message":"secret-bearing text"}}`
	if _, err := s.ResolveChatGPT(ctx, owner, mine.ID, TokenDigest(cur.AccessToken)); !errors.Is(err, ErrReconnectRequired) {
		t.Fatal("revoked", err)
	}
	n := issuer.refreshes.Load()
	if _, err := s.ResolveChatGPT(ctx, owner, mine.ID, ""); !errors.Is(err, ErrReconnectRequired) || issuer.refreshes.Load() != n {
		t.Fatal("reconnect-required must not call the issuer again", err)
	}
	if list, _ := s.List(ctx, owner); !list[0].ReconnectRequired {
		t.Fatal("list does not show reconnect")
	}
	// The other person is unaffected.
	if a, err := s.ResolveChatGPT(ctx, other, theirs.ID, ""); err != nil || a.AccountID != "acct-other" {
		t.Fatal("other person affected", err)
	}
	// Reconnecting the same connection repairs it with a new version.
	issuer.refreshStatus, issuer.refreshBody = 0, ""
	before, _ := s.Describe(ctx, owner, mine.ID)
	again := connect(t, s, issuer, clk, owner, mine.ID)
	if again.ID != mine.ID {
		t.Fatal("reconnect created a new connection")
	}
	after, err := s.ResolveChatGPT(ctx, owner, mine.ID, "")
	if err != nil || after.Version == before.Version || after.Connection.ReconnectRequired {
		t.Fatal("reconnect", err)
	}
}

func TestChatGPTTransientRefreshAndAccountChange(t *testing.T) {
	s, issuer, clk := chatGPTFixture(t)
	ctx := context.Background()
	c := connect(t, s, issuer, clk, owner, "")
	cur, _ := s.ResolveChatGPT(ctx, owner, c.ID, "")
	issuer.refreshStatus = 503
	clk.Add(56 * time.Minute) // inside the margin, still valid
	if a, err := s.ResolveChatGPT(ctx, owner, c.ID, ""); err != nil || a.AccessToken != cur.AccessToken {
		t.Fatal("still-valid token must be used when proactive refresh fails", err)
	}
	if _, err := s.ResolveChatGPT(ctx, owner, c.ID, TokenDigest(cur.AccessToken)); !errors.Is(err, ErrRefreshFailed) {
		t.Fatal("a rejected token is never handed out again", err)
	}
	issuer.refreshStatus = 0
	issuer.refreshAccount = "acct-intruder"
	if _, err := s.ResolveChatGPT(ctx, owner, c.ID, TokenDigest(cur.AccessToken)); !errors.Is(err, ErrReconnectRequired) {
		t.Fatal("account change must require reconnect", err)
	}
}

func TestChatGPTHTTPFlowAndSettings(t *testing.T) {
	s, issuer, clk := chatGPTFixture(t)
	changed := 0
	service := &Service{Store: s, Changed: func(string) { changed++ }, Authenticate: func(*http.Request) (browseridentity.Identity, error) {
		return browseridentity.Identity{HumanID: owner, SessionID: "sess", Authorize: func(ctx context.Context, f func(context.Context) error) error { return f(ctx) }}, nil
	}}
	mux := http.NewServeMux()
	service.RegisterRoutes(mux)
	do := func(method, path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
		return w
	}
	w := do("GET", "/api/model-connections", "")
	if !strings.Contains(w.Body.String(), `"chatgpt":{"available":true}`) {
		t.Fatal(w.Body.String())
	}
	w = do("POST", "/api/model-connections/chatgpt/login", `{}`)
	var v LoginView
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &v) != nil || v.UserCode == "" || strings.Contains(w.Body.String(), "dev-secret") {
		t.Fatal(w.Code, w.Body.String())
	}
	issuer.mu.Lock()
	issuer.authorized = true
	issuer.mu.Unlock()
	clk.Add(6 * time.Second)
	w = do("GET", "/api/model-connections/chatgpt/login/"+v.LoginID, "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"status":"completed"`) || strings.Contains(w.Body.String(), "refresh-") || strings.Contains(w.Body.String(), "access") {
		t.Fatal(w.Code, w.Body.String())
	}
	if changed != 1 {
		t.Fatal("selection change not reported", changed)
	}
	_ = json.Unmarshal(w.Body.Bytes(), &v)
	w = do("PUT", "/api/model-connections/chatgpt/"+v.Connection.ID, `{"model":"gpt-6-sol","reasoningEffort":"high"}`)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"reasoningEffort":"high"`) {
		t.Fatal(w.Code, w.Body.String())
	}
	w = do("PUT", "/api/model-connections/chatgpt/"+v.Connection.ID, `{"model":"gpt-6-sol","reasoningEffort":"ultra-max"}`)
	if w.Code != 400 {
		t.Fatal("invalid effort", w.Code)
	}
	w = do("DELETE", "/api/model-connections/api/"+v.Connection.ID, "")
	if w.Code != 204 {
		t.Fatal("delete", w.Code)
	}
	if _, err := s.ResolveChatGPT(context.Background(), owner, v.Connection.ID, ""); !errors.Is(err, ErrNotFound) {
		t.Fatal("deleted connection still resolves", err)
	}
}
