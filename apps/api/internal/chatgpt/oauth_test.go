package chatgpt

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func oauthTestToken(account string) string {
	b, _ := json.Marshal(map[string]any{"exp": time.Now().Add(time.Hour).Unix(), "https://api.openai.com/auth": map[string]string{"chatgpt_account_id": account}})
	return "header." + base64.RawURLEncoding.EncodeToString(b) + ".signature"
}

func TestOAuthDeviceLoginProtocol(t *testing.T) {
	polls := 0
	var issuer string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/accounts/deviceauth/usercode":
			var request map[string]string
			if json.NewDecoder(r.Body).Decode(&request) != nil || request["client_id"] != chatGPTClientID {
				t.Error("wrong client request")
			}
			fmt.Fprint(w, `{"device_auth_id":"private-id","user_code":"ABCD-EFGH","interval":"2"}`)
		case "/api/accounts/deviceauth/token":
			var request map[string]string
			_ = json.NewDecoder(r.Body).Decode(&request)
			if request["device_auth_id"] != "private-id" || request["user_code"] != "ABCD-EFGH" {
				t.Error("poll binding lost")
			}
			polls++
			if polls == 1 {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			fmt.Fprint(w, `{"authorization_code":"code & value","code_verifier":"verifier+value"}`)
		case "/oauth/token":
			if err := r.ParseForm(); err != nil {
				t.Error(err)
			}
			if r.Form.Get("code") != "code & value" || r.Form.Get("code_verifier") != "verifier+value" || r.Form.Get("redirect_uri") != issuer+"/deviceauth/callback" {
				t.Error("wrong code exchange")
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"id_token": oauthTestToken("account"), "access_token": oauthTestToken("account"), "refresh_token": "refresh-secret"})
		default:
			t.Errorf("unexpected route %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	issuer = server.URL
	client := NewOAuthClient()
	client.issuer = issuer
	login, err := client.BeginDevice(context.Background())
	if err != nil || login.interval != 2*time.Second {
		t.Fatalf("begin: %+v %v", login, err)
	}
	public, _ := json.Marshal(login)
	if strings.Contains(string(public), "private-id") {
		t.Fatal("private polling credential serialized")
	}
	_, pending, err := client.PollDevice(context.Background(), login)
	if err != nil || !pending {
		t.Fatalf("pending: %v %v", pending, err)
	}
	creds, pending, err := client.PollDevice(context.Background(), login)
	if err != nil || pending || creds.AccountID != "account" || creds.RefreshToken != "refresh-secret" {
		t.Fatalf("completion: pending=%v err=%v", pending, err)
	}
}

func TestOAuthRefreshOmissionsAndErrors(t *testing.T) {
	status, body := 200, `{}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]string
		if json.NewDecoder(r.Body).Decode(&request) != nil || request["refresh_token"] != "secret" || request["grant_type"] != "refresh_token" {
			t.Error("invalid refresh body")
		}
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	}))
	defer server.Close()
	client := NewOAuthClient()
	client.issuer = server.URL
	creds, err := client.Refresh(context.Background(), "secret")
	if err != nil || creds.AccessToken != "" || creds.RefreshToken != "" || !creds.ExpiresAt.IsZero() {
		t.Fatalf("omissions were manufactured: %v", err)
	}
	bodyBytes, _ := json.Marshal(map[string]string{"access_token": oauthTestToken("account"), "refresh_token": "rotated"})
	body = string(bodyBytes)
	creds, err = client.Refresh(context.Background(), "secret")
	if err != nil || creds.AccountID != "account" || creds.RefreshToken != "rotated" {
		t.Fatalf("refresh: %v", err)
	}
	status, body = 400, `{"error":"invalid_grant","detail":"secret"}`
	_, err = client.Refresh(context.Background(), "secret")
	if !errors.Is(err, ErrReconnectRequired) {
		t.Fatalf("wrong permanent error: %v", err)
	}
	status, body = 503, `{"error":{"message":"secret"}}`
	_, err = client.Refresh(context.Background(), "secret")
	if !errors.Is(err, ErrRefreshFailed) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("unsafe transient error: %v", err)
	}
}

func TestOAuthRejectsAccountMismatchAndRedirect(t *testing.T) {
	body, _ := json.Marshal(map[string]string{"id_token": oauthTestToken("a"), "access_token": oauthTestToken("b"), "refresh_token": "secret"})
	if _, err := parseOAuthTokens(body, true); err == nil {
		t.Fatal("account mismatch accepted")
	}
	forwarded := false
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { forwarded = true }))
	defer destination.Close()
	issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, destination.URL, 307) }))
	defer issuer.Close()
	client := NewOAuthClient()
	client.issuer = issuer.URL
	if _, err := client.Refresh(context.Background(), "secret"); err == nil || forwarded {
		t.Fatal("OAuth redirected credentials")
	}
}
