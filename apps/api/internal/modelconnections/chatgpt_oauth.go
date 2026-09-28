package modelconnections

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// Protocol reference: openai/codex 44fe510c (codex-rs/login/src/
// device_code_auth.rs, auth/manager.rs, server.rs). These are the OAuth
// client operations of "Sign in with ChatGPT" only; Sumi keeps its own
// conversation context, tools, memory and agent loop, and never runs a
// Codex agent or app-server.
const (
	ChatGPTPreset        = "chatgpt-codex"
	ChatGPTBaseURL       = "https://chatgpt.com/backend-api/codex"
	DefaultChatGPTModel  = "gpt-6-astra"
	DefaultChatGPTEffort = "medium"
	chatGPTIssuer        = "https://auth.openai.com"
	chatGPTClientID      = "app_EMoamEEZ73f0CkXaXp7hrann"
	// deviceLoginLifetime is the issuer's documented code lifetime.
	deviceLoginLifetime = 15 * time.Minute
)

var (
	// ErrChatGPTDisabled: this server does not offer subscription
	// connections (operator setting), or cannot seal credentials.
	ErrChatGPTDisabled = errors.New("ChatGPT subscription connections are not enabled on this server")
	// ErrReconnectRequired: the stored grant can no longer be refreshed
	// (expired, revoked, reused, or bound to another account); only a new
	// login by the person can repair it.
	ErrReconnectRequired = errors.New("ChatGPT connection needs to be reconnected")
	// ErrRefreshFailed: a transient refresh failure; the next call retries.
	ErrRefreshFailed = errors.New("ChatGPT credential refresh failed")
	// ErrDeviceLoginUnavailable: the issuer does not offer device login for
	// this account/workspace (a 404 on the user-code endpoint).
	ErrDeviceLoginUnavailable = errors.New("ChatGPT device login is unavailable")
	errLoginFailed            = errors.New("ChatGPT login failed")
	errIssuerTransient        = errors.New("ChatGPT issuer unavailable")
)

// OAuthTokens is an internal handoff. Token fields never serialize.
type OAuthTokens struct {
	AccessToken  string    `json:"-"`
	RefreshToken string    `json:"-"`
	AccountID    string    `json:"-"`
	ExpiresAt    time.Time `json:"-"`
}

// DeviceCode is the issuer's user-code grant. DeviceAuthID is the private
// polling credential; only UserCode and VerificationURL are shown.
type DeviceCode struct {
	DeviceAuthID    string
	UserCode        string
	VerificationURL string
	Interval        time.Duration
}

// OAuthClient talks to the ChatGPT OAuth issuer. Issuer and HTTP are
// replaceable for synthetic tests; production uses NewOAuthClient.
type OAuthClient struct {
	HTTP     *http.Client
	Issuer   string
	ClientID string
	Now      func() time.Time
}

func NewOAuthClient() *OAuthClient {
	return &OAuthClient{
		HTTP: &http.Client{
			Timeout: 20 * time.Second,
			// Credentials and one-time codes never follow a redirect.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		Issuer:   chatGPTIssuer,
		ClientID: chatGPTClientID,
	}
}

func (c *OAuthClient) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func (c *OAuthClient) post(ctx context.Context, path, contentType string, body []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.Issuer, "/")+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, errLoginFailed
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Accept", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return 0, nil, ctx.Err()
		}
		return 0, nil, errIssuerTransient
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return 0, nil, errIssuerTransient
	}
	return resp.StatusCode, data, nil
}

// BeginDevice requests a user code. A 404 means device login is not
// available for this issuer/account.
func (c *OAuthClient) BeginDevice(ctx context.Context) (DeviceCode, error) {
	body, _ := json.Marshal(map[string]string{"client_id": c.ClientID})
	status, data, err := c.post(ctx, "/api/accounts/deviceauth/usercode", "application/json", body)
	if err != nil {
		return DeviceCode{}, err
	}
	if status == http.StatusNotFound {
		return DeviceCode{}, ErrDeviceLoginUnavailable
	}
	if status == http.StatusTooManyRequests || status >= 500 {
		return DeviceCode{}, errIssuerTransient
	}
	if status < 200 || status >= 300 {
		return DeviceCode{}, errLoginFailed
	}
	var r struct {
		DeviceAuthID   string          `json:"device_auth_id"`
		UserCode       string          `json:"user_code"`
		LegacyUserCode string          `json:"usercode"`
		Interval       json.RawMessage `json:"interval"`
	}
	if json.Unmarshal(data, &r) != nil {
		return DeviceCode{}, errLoginFailed
	}
	if r.UserCode == "" {
		r.UserCode = r.LegacyUserCode
	}
	if !bounded(r.DeviceAuthID, 4096) || !bounded(r.UserCode, 128) {
		return DeviceCode{}, errLoginFailed
	}
	seconds := int64(5)
	if len(r.Interval) > 0 {
		// The issuer sends the interval as a string ("5"); accept a number too.
		var raw string
		if json.Unmarshal(r.Interval, &raw) != nil {
			raw = string(r.Interval)
		}
		v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			return DeviceCode{}, errLoginFailed
		}
		seconds = v
	}
	// Never busy-poll on an absent or zero interval.
	if seconds < 1 {
		seconds = 5
	}
	if seconds > 900 {
		return DeviceCode{}, errLoginFailed
	}
	return DeviceCode{
		DeviceAuthID:    r.DeviceAuthID,
		UserCode:        r.UserCode,
		VerificationURL: strings.TrimRight(c.Issuer, "/") + "/codex/device",
		Interval:        time.Duration(seconds) * time.Second,
	}, nil
}

// PollDevice performs one poll. pending=true means the person has not
// finished authorizing yet (the issuer answers 403/404 until then). An
// errIssuerTransient error may be retried at the next interval; any other
// error ends the login. The code exchange is never retried: a lost
// response may already have consumed the one-time code.
func (c *OAuthClient) PollDevice(ctx context.Context, deviceAuthID, userCode string) (OAuthTokens, bool, error) {
	body, _ := json.Marshal(map[string]string{"device_auth_id": deviceAuthID, "user_code": userCode})
	status, data, err := c.post(ctx, "/api/accounts/deviceauth/token", "application/json", body)
	if err != nil {
		return OAuthTokens{}, false, err
	}
	if status == http.StatusForbidden || status == http.StatusNotFound {
		return OAuthTokens{}, true, nil
	}
	if status == http.StatusTooManyRequests || status >= 500 {
		return OAuthTokens{}, false, errIssuerTransient
	}
	if status < 200 || status >= 300 {
		return OAuthTokens{}, false, errLoginFailed
	}
	var code struct {
		AuthorizationCode string `json:"authorization_code"`
		CodeVerifier      string `json:"code_verifier"`
	}
	if json.Unmarshal(data, &code) != nil || code.AuthorizationCode == "" || code.CodeVerifier == "" {
		return OAuthTokens{}, false, errLoginFailed
	}
	issuer := strings.TrimRight(c.Issuer, "/")
	form := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code.AuthorizationCode},
		"redirect_uri":  {issuer + "/deviceauth/callback"},
		"client_id":     {c.ClientID},
		"code_verifier": {code.CodeVerifier},
	}
	status, data, err = c.post(ctx, "/oauth/token", "application/x-www-form-urlencoded", []byte(form.Encode()))
	if err != nil {
		return OAuthTokens{}, false, errLoginFailed
	}
	if status < 200 || status >= 300 {
		return OAuthTokens{}, false, errLoginFailed
	}
	tokens, err := c.parseTokens(data, true)
	return tokens, false, err
}

// Refresh exchanges a refresh token. Permanent rejections (the grant is
// expired, revoked, already rotated, or unauthorized) return
// ErrReconnectRequired; everything else is ErrRefreshFailed. Upstream
// bodies are never returned: they can contain credential material.
func (c *OAuthClient) Refresh(ctx context.Context, refreshToken string) (OAuthTokens, error) {
	body, _ := json.Marshal(map[string]string{
		"client_id":     c.ClientID,
		"grant_type":    "refresh_token",
		"refresh_token": refreshToken,
	})
	status, data, err := c.post(ctx, "/oauth/token", "application/json", body)
	if err != nil {
		return OAuthTokens{}, ErrRefreshFailed
	}
	if status >= 200 && status < 300 {
		tokens, err := c.parseTokens(data, false)
		if err != nil {
			return OAuthTokens{}, ErrRefreshFailed
		}
		return tokens, nil
	}
	var failure struct {
		Error json.RawMessage `json:"error"`
	}
	var code string
	if json.Unmarshal(data, &failure) == nil && len(failure.Error) > 0 {
		if json.Unmarshal(failure.Error, &code) != nil {
			var detail struct {
				Code string `json:"code"`
			}
			if json.Unmarshal(failure.Error, &detail) == nil {
				code = detail.Code
			}
		}
	}
	switch strings.ToLower(code) {
	case "refresh_token_expired", "refresh_token_reused", "refresh_token_invalidated":
		return OAuthTokens{}, ErrReconnectRequired
	case "invalid_grant":
		if status == http.StatusBadRequest {
			return OAuthTokens{}, ErrReconnectRequired
		}
	}
	if status == http.StatusUnauthorized {
		return OAuthTokens{}, ErrReconnectRequired
	}
	return OAuthTokens{}, ErrRefreshFailed
}

type tokenClaims struct {
	Exp  int64 `json:"exp"`
	Auth struct {
		AccountID string `json:"chatgpt_account_id"`
	} `json:"https://api.openai.com/auth"`
}

// claims reads JWT claims from a token received directly from the issuer
// over TLS — never from a browser-supplied token — so the signature is not
// re-verified here.
func claims(token string) (tokenClaims, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || len(token) > 65536 {
		return tokenClaims{}, false
	}
	data, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return tokenClaims{}, false
	}
	var c tokenClaims
	if json.Unmarshal(data, &c) != nil {
		return tokenClaims{}, false
	}
	return c, true
}

func validAccount(s string) bool {
	return len(s) > 0 && len(s) <= 256 && strings.IndexFunc(s, unicode.IsControl) < 0
}

// headerSafe: the token travels in an HTTP header; reject anything a
// fetch implementation would refuse or mangle.
func headerSafe(s string) bool {
	return strings.IndexFunc(s, func(r rune) bool { return r < 0x21 || r > 0x7e }) < 0
}

func (c *OAuthClient) parseTokens(data []byte, initial bool) (OAuthTokens, error) {
	var r struct {
		IDToken      string `json:"id_token"`
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	if json.Unmarshal(data, &r) != nil {
		return OAuthTokens{}, errLoginFailed
	}
	t := OAuthTokens{AccessToken: r.AccessToken, RefreshToken: r.RefreshToken}
	if len(t.RefreshToken) > 65536 || !headerSafe(t.RefreshToken) {
		return OAuthTokens{}, errLoginFailed
	}
	if t.AccessToken != "" {
		cl, ok := claims(t.AccessToken)
		if !ok || !headerSafe(t.AccessToken) || cl.Exp <= 0 {
			return OAuthTokens{}, errLoginFailed
		}
		t.ExpiresAt = time.Unix(cl.Exp, 0)
		if !t.ExpiresAt.After(c.now()) {
			return OAuthTokens{}, errLoginFailed
		}
		t.AccountID = cl.Auth.AccountID
	}
	if r.IDToken != "" {
		cl, ok := claims(r.IDToken)
		if !ok || !validAccount(cl.Auth.AccountID) || (t.AccountID != "" && t.AccountID != cl.Auth.AccountID) {
			return OAuthTokens{}, errLoginFailed
		}
		t.AccountID = cl.Auth.AccountID
	}
	if t.AccountID != "" && (!validAccount(t.AccountID) || !headerSafe(t.AccountID)) {
		return OAuthTokens{}, errLoginFailed
	}
	if initial && (t.AccountID == "" || t.AccessToken == "" || t.RefreshToken == "") {
		return OAuthTokens{}, errLoginFailed
	}
	return t, nil
}
