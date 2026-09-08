package chatgpt

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Protocol reference: OpenAI Codex df522cae16b4f4350d6a9e13bc10cbe9650b0f9b,
// login/src/device_code_auth.rs and login/src/auth/manager.rs. These are OAuth
// client operations only; Sumi retains its own agent context and execution loop.
const chatGPTClientID = "app_EMoamEEZ73f0CkXaXp7hrann"

var (
	ErrLoginFailed            = errors.New("ChatGPT login failed; please try again")
	ErrDeviceLoginUnavailable = errors.New("ChatGPT device login is unavailable")
)

type OAuthClient struct {
	client   *http.Client
	issuer   string
	clientID string
}

func NewOAuthClient() *OAuthClient {
	return &OAuthClient{
		client: &http.Client{Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		issuer: "https://auth.openai.com", clientID: chatGPTClientID,
	}
}

// DeviceLogin contains the public ceremony and a private polling credential.
// Only LoginService may expose its public fields to the initiating Human.
type DeviceLogin struct {
	VerificationURL string    `json:"verificationUrl"`
	UserCode        string    `json:"userCode"`
	ExpiresAt       time.Time `json:"expiresAt"`
	deviceAuthID    string
	interval        time.Duration
}

func (c *OAuthClient) request(ctx context.Context, path, contentType string, body []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.issuer+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, ErrLoginFailed
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := c.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return 0, nil, ctx.Err()
		}
		return 0, nil, ErrLoginFailed
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return 0, nil, ErrLoginFailed
	}
	return resp.StatusCode, data, nil
}

func (c *OAuthClient) BeginDevice(ctx context.Context) (DeviceLogin, error) {
	body, _ := json.Marshal(map[string]string{"client_id": c.clientID})
	status, data, err := c.request(ctx, "/api/accounts/deviceauth/usercode", "application/json", body)
	if err != nil {
		return DeviceLogin{}, err
	}
	if status == http.StatusNotFound {
		return DeviceLogin{}, ErrDeviceLoginUnavailable
	}
	if status < 200 || status >= 300 {
		return DeviceLogin{}, ErrLoginFailed
	}
	var result struct {
		DeviceAuthID   string          `json:"device_auth_id"`
		UserCode       string          `json:"user_code"`
		LegacyUserCode string          `json:"usercode"`
		Interval       json.RawMessage `json:"interval"`
	}
	if json.Unmarshal(data, &result) != nil {
		return DeviceLogin{}, ErrLoginFailed
	}
	if result.UserCode == "" {
		result.UserCode = result.LegacyUserCode
	}
	if len(result.DeviceAuthID) == 0 || len(result.DeviceAuthID) > 4096 || len(result.UserCode) == 0 || len(result.UserCode) > 128 {
		return DeviceLogin{}, ErrLoginFailed
	}
	seconds := int64(5)
	if len(result.Interval) > 0 {
		var number json.Number
		if json.Unmarshal(result.Interval, &number) != nil {
			return DeviceLogin{}, ErrLoginFailed
		}
		if value, err := number.Int64(); err == nil {
			seconds = value
		} else {
			return DeviceLogin{}, ErrLoginFailed
		}
	}
	// Never busy-poll if an issuer sends an absent or zero interval.
	if seconds < 1 {
		seconds = 5
	}
	if seconds > 900 {
		return DeviceLogin{}, ErrLoginFailed
	}
	return DeviceLogin{VerificationURL: c.issuer + "/codex/device", UserCode: result.UserCode, ExpiresAt: time.Now().Add(15 * time.Minute), deviceAuthID: result.DeviceAuthID, interval: time.Duration(seconds) * time.Second}, nil
}

// PollDevice performs one poll. Pending authorization is not an error; callers
// observe the returned interval and original deadline between polls.
func (c *OAuthClient) PollDevice(ctx context.Context, login DeviceLogin) (Credentials, bool, error) {
	if !time.Now().Before(login.ExpiresAt) {
		return Credentials{}, false, ErrLoginFailed
	}
	body, _ := json.Marshal(map[string]string{"device_auth_id": login.deviceAuthID, "user_code": login.UserCode})
	status, data, err := c.request(ctx, "/api/accounts/deviceauth/token", "application/json", body)
	if err != nil {
		return Credentials{}, false, err
	}
	if status == http.StatusForbidden || status == http.StatusNotFound {
		return Credentials{}, true, nil
	}
	if status < 200 || status >= 300 {
		return Credentials{}, false, ErrLoginFailed
	}
	var code struct {
		AuthorizationCode string `json:"authorization_code"`
		CodeVerifier      string `json:"code_verifier"`
	}
	if json.Unmarshal(data, &code) != nil || code.AuthorizationCode == "" || code.CodeVerifier == "" {
		return Credentials{}, false, ErrLoginFailed
	}
	form := url.Values{"grant_type": {"authorization_code"}, "code": {code.AuthorizationCode}, "redirect_uri": {c.issuer + "/deviceauth/callback"}, "client_id": {c.clientID}, "code_verifier": {code.CodeVerifier}}
	status, data, err = c.request(ctx, "/oauth/token", "application/x-www-form-urlencoded", []byte(form.Encode()))
	if err != nil {
		return Credentials{}, false, err
	}
	if status < 200 || status >= 300 {
		return Credentials{}, false, ErrLoginFailed
	}
	tokens, err := parseOAuthTokens(data, true)
	return tokens, false, err
}

func (c *OAuthClient) Refresh(ctx context.Context, refreshToken string) (Credentials, error) {
	body, _ := json.Marshal(map[string]string{"client_id": c.clientID, "grant_type": "refresh_token", "refresh_token": refreshToken})
	status, data, err := c.request(ctx, "/oauth/token", "application/json", body)
	if err != nil {
		return Credentials{}, ErrRefreshFailed
	}
	if status >= 200 && status < 300 {
		tokens, err := parseOAuthTokens(data, false)
		if err != nil {
			return Credentials{}, ErrRefreshFailed
		}
		return tokens, nil
	}
	var failure struct {
		Error json.RawMessage `json:"error"`
	}
	var code string
	if json.Unmarshal(data, &failure) == nil {
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
	case "invalid_grant", "refresh_token_expired", "refresh_token_reused", "refresh_token_invalidated":
		return Credentials{}, ErrReconnectRequired
	}
	if status == http.StatusUnauthorized {
		return Credentials{}, ErrReconnectRequired
	}
	// Never return upstream bodies: they can include credentials or request data.
	return Credentials{}, ErrRefreshFailed
}

// These claims are read only from token responses obtained directly over the
// authenticated issuer connection, never from browser-supplied JWTs.
func tokenClaims(token string) (string, time.Time, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || len(token) > 65536 {
		return "", time.Time{}, ErrLoginFailed
	}
	data, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", time.Time{}, ErrLoginFailed
	}
	var claims struct {
		Exp  int64 `json:"exp"`
		Auth struct {
			AccountID string `json:"chatgpt_account_id"`
		} `json:"https://api.openai.com/auth"`
	}
	if json.Unmarshal(data, &claims) != nil {
		return "", time.Time{}, ErrLoginFailed
	}
	var expires time.Time
	if claims.Exp > 0 {
		expires = time.Unix(claims.Exp, 0)
	}
	return claims.Auth.AccountID, expires, nil
}

func parseOAuthTokens(data []byte, initial bool) (Credentials, error) {
	var response struct {
		IDToken      string `json:"id_token"`
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	if json.Unmarshal(data, &response) != nil {
		return Credentials{}, ErrLoginFailed
	}
	c := Credentials{AccessToken: response.AccessToken, RefreshToken: response.RefreshToken}
	if len(c.RefreshToken) > 65536 {
		return Credentials{}, ErrLoginFailed
	}
	if c.AccessToken != "" {
		account, expiry, err := tokenClaims(c.AccessToken)
		if err != nil || !expiry.After(time.Now()) {
			return Credentials{}, ErrLoginFailed
		}
		c.AccountID, c.ExpiresAt = account, expiry
	}
	if response.IDToken != "" {
		account, _, err := tokenClaims(response.IDToken)
		if err != nil || !validAccount(account) || (c.AccountID != "" && c.AccountID != account) {
			return Credentials{}, ErrLoginFailed
		}
		c.AccountID = account
	}
	if initial && (!validAccount(c.AccountID) || c.AccessToken == "" || c.RefreshToken == "") {
		return Credentials{}, ErrLoginFailed
	}
	return c, nil
}
