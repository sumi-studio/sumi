package agentstate

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
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

// TestChatGPTBindingAndRejectedTokenRefresh: the core receives a
// subscription connection's access token through the persona-scoped
// binding, and a reported 401 refreshes the grant exactly once — a
// revoked grant becomes a classified reconnect_required binding.
func TestChatGPTBindingAndRejectedTokenRefresh(t *testing.T) {
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
	login, err := conns.BeginChatGPTLogin(ctx, human, "s", "")
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
		b.Connection.ReasoningEffort != "medium" || b.Connection.AccountID != "acct-1" || b.APIKey == "" || !b.CredentialAvailable {
		t.Fatalf("binding %+v", b)
	}
	first := b.APIKey
	body := func(conn, tok string) string {
		return fmt.Sprintf(`{"connection_id":%q,"rejected_token_sha256":%q}`, conn, modelconnections.TokenDigest(tok))
	}
	// A rejection naming another connection does not refresh this one.
	b = decode(do(t, mux, "POST", "/internal/core/personas/"+pa+"/model/credential-refresh", token, body("00000000-0000-4000-8000-000000000000", first)))
	if b.APIKey != first || refreshes.Load() != 0 {
		t.Fatal("unrelated rejection refreshed")
	}
	b = decode(do(t, mux, "POST", "/internal/core/personas/"+pa+"/model/credential-refresh", token, body(b.Connection.ID, first)))
	if b.APIKey == first || !b.CredentialAvailable || refreshes.Load() != 1 {
		t.Fatalf("refresh %+v refreshes=%d", b, refreshes.Load())
	}
	// The same stale rejection again: already replaced, no second rotation.
	b = decode(do(t, mux, "POST", "/internal/core/personas/"+pa+"/model/credential-refresh", token, body(b.Connection.ID, first)))
	if refreshes.Load() != 1 {
		t.Fatal("stale rejection rotated again")
	}
	mu.Lock()
	revoked = true
	mu.Unlock()
	b = decode(do(t, mux, "POST", "/internal/core/personas/"+pa+"/model/credential-refresh", token, body(b.Connection.ID, b.APIKey)))
	if b.CredentialAvailable || b.APIKey != "" || b.CredentialState != "reconnect_required" || b.Connection == nil {
		t.Fatalf("revoked binding %+v", b)
	}
	if rec := do(t, mux, "POST", "/internal/core/personas/"+pa+"/model/credential-refresh", token, `{"connection_id":"x"}`); rec.Code != 400 {
		t.Fatal("malformed refresh accepted", rec.Code)
	}
	pb := pid(t)
	if _, _, err := srv.store.EnsurePersona(ctx, pb, &human, "other"); err != nil {
		t.Fatal(err)
	}
	if rec := do(t, mux, "POST", "/internal/core/personas/"+pa+"/model/credential-refresh", srv.PersonaToken(pb), body(b.Connection.ID, first)); rec.Code != 401 {
		t.Fatal("cross-persona refresh", rec.Code)
	}
}
