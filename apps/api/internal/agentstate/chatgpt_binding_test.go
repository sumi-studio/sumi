package agentstate

import (
	"context"

	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/modelconnections"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

func fakeJWT(account string, exp time.Time, tag string) string {
	enc := func(v any) string {
		b, _ := json.Marshal(v)
		return base64.RawURLEncoding.EncodeToString(b)
	}
	return enc(map[string]string{"alg": "none"}) + "." + enc(map[string]any{
		"exp": exp.Unix(), "tag": tag,
		"https://api.openai.com/auth": map[string]string{"chatgpt_account_id": account},
	}) + ".sig"
}

// TestChatGPTBindingKeepsCredentialsOnAPI: the core receives a
// subscription metadata through the persona-scoped binding without the
// access token; Go alone resolves and refreshes the grant — a
// revoked grant becomes a classified reconnect_required binding.
func TestChatGPTBindingKeepsCredentialsOnAPI(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Create(t)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	var refreshes atomic.Int32
	var mu sync.Mutex
	revoked := false
	seq := 0
	issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/api/accounts/deviceauth/usercode":
			_, _ = w.Write([]byte(`{"device_auth_id":"d","user_code":"U-1","interval":"1"}`))
		case "/api/accounts/deviceauth/token":
			_, _ = w.Write([]byte(`{"authorization_code":"c","code_verifier":"v"}`))
		case "/oauth/token":
			if strings.Contains(r.Header.Get("Content-Type"), "json") {
				refreshes.Add(1)
				if revoked {
					w.WriteHeader(401)
					_, _ = w.Write([]byte(`{"error":{"code":"refresh_token_expired"}}`))
					return
				}
			}
			seq++
			_ = json.NewEncoder(w).Encode(map[string]string{
				"access_token":  fakeJWT("acct-1", time.Now().Add(time.Hour), fmt.Sprint(seq)),
				"refresh_token": fmt.Sprint("r", seq),
				"id_token":      fakeJWT("acct-1", time.Now().Add(time.Hour), "id"),
			})
		}
	}))
	defer issuer.Close()
	key := make([]byte, 32)
	conns, err := modelconnections.New(pool, key)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	oauth := modelconnections.NewOAuthClient()
	oauth.Issuer, oauth.Now = issuer.URL, func() time.Time { return now }
	if err := conns.EnableChatGPT(oauth); err != nil {
		t.Fatal(err)
	}
	srv := NewServer(pool, testAdminSecret)
	srv.SetModelConnections(conns)
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)
	human := mustHuman(t, pool)
	pa := pid(t)
	if _, _, err := srv.store.EnsurePersona(ctx, pa, &human, "secretary"); err != nil {
		t.Fatal(err)
	}
	login, err := conns.BeginChatGPTLogin(ctx, human, "s", "", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	done, err := conns.PollChatGPTLogin(ctx, human, "s", login.LoginID)
	if err != nil || done.Connection == nil {
		t.Fatalf("login %+v %v", done, err)
	}
	token := srv.PersonaToken(pa)
	decode := func(rec *httptest.ResponseRecorder) ModelBinding {
		t.Helper()
		if rec.Code != 200 {
			t.Fatalf("binding status %d %s", rec.Code, rec.Body.String())
		}
		var b ModelBinding
		if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil {
			t.Fatal(err)
		}
		return b
	}
	b := decode(do(t, mux, "GET", "/internal/core/personas/"+pa+"/model", token, ""))
	if b.Selection != "api" || b.Connection == nil || b.Connection.Preset != modelconnections.ChatGPTPreset ||
		b.Connection.BaseURL != modelconnections.ChatGPTBaseURL || b.Connection.Model != "gpt-6-astra" ||
		b.Connection.ReasoningEffort != "medium" || b.Connection.AccountID != "acct-1" || b.APIKey != "" || !b.CredentialAvailable {
		t.Fatalf("binding %+v", b)
	}
	first, err := conns.ResolveChatGPT(ctx, human, done.Connection.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conns.ResolveChatGPT(ctx, human, done.Connection.ID, modelconnections.TokenDigest(first.AccessToken)); err != nil {
		t.Fatal(err)
	}
	b = decode(do(t, mux, "GET", "/internal/core/personas/"+pa+"/model", token, ""))
	if b.APIKey != "" || !b.CredentialAvailable || refreshes.Load() != 1 {
		t.Fatal("binding leaked/refreshed credential", b)
	}
	fresh, err := conns.ResolveChatGPT(ctx, human, done.Connection.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	revoked = true
	mu.Unlock()
	_, _ = conns.ResolveChatGPT(ctx, human, done.Connection.ID, modelconnections.TokenDigest(fresh.AccessToken))
	b = decode(do(t, mux, "GET", "/internal/core/personas/"+pa+"/model", token, ""))
	if b.CredentialAvailable || b.APIKey != "" || b.CredentialState != "reconnect_required" {
		t.Fatal("revoked binding", b)
	}
	pb := pid(t)
	if _, _, err := srv.store.EnsurePersona(ctx, pb, &human, "other"); err != nil {
		t.Fatal(err)
	}
	if rec := do(t, mux, "GET", "/internal/core/personas/"+pa+"/model", srv.PersonaToken(pb), ""); rec.Code != 401 {
		t.Fatal("cross-persona binding", rec.Code)
	}
}
