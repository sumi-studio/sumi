package agentevents

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	defaultBrowserAudience = "sumi:web:conversation"
	// BrowserSessionCookie is the name of the signed HttpOnly session cookie
	// used by browser routes.
	BrowserSessionCookie = "sumi_session"
)

// DefaultBrowserAudience returns the default audience for browser session
// cookies. It is exposed so production wiring and tests can agree on the value
// without hardcoding it in multiple packages.
func DefaultBrowserAudience() string { return defaultBrowserAudience }

// UserSessionClaims are deliberately separate from agent TokenClaims. A
// browser can act only as its user in one conversation; it never learns an
// agent identity, generation, or agent gateway credential.
type UserSessionClaims struct {
	TenantID       string
	UserID         string
	ConversationID string
}

// UserSessionVerifier validates the signed HttpOnly browser session cookie.
// Session issuance/login is a control-plane responsibility outside T28.
type UserSessionVerifier interface {
	VerifySession(ctx context.Context, signedCookie string) (UserSessionClaims, error)
}

type HMACUserSessionVerifier struct {
	secret   []byte
	audience string
}

type userSessionWireClaims struct {
	TenantID       string `json:"tenant_id"`
	UserID         string `json:"user_id"`
	ConversationID string `json:"conversation_id"`
	Exp            int64  `json:"exp"`
	Aud            string `json:"aud"`
}

func NewHMACUserSessionVerifier(secret []byte, audience string) (*HMACUserSessionVerifier, error) {
	if len(secret) < 32 {
		return nil, errors.New("browser session HMAC secret must be at least 32 bytes")
	}
	if audience == "" {
		audience = defaultBrowserAudience
	}
	return &HMACUserSessionVerifier{secret: append([]byte(nil), secret...), audience: audience}, nil
}

// IssueHMACUserSession creates a browser session for trusted control-plane
// callers. Production login owns when this is called; the Compose development
// route uses the same primitive so local browser requests exercise real session
// verification rather than a Todo-specific authentication bypass.
func IssueHMACUserSession(secret []byte, audience string, claims UserSessionClaims, expiresAt time.Time) (string, error) {
	if len(secret) < 32 {
		return "", errors.New("browser session HMAC secret must be at least 32 bytes")
	}
	if audience == "" {
		audience = defaultBrowserAudience
	}
	if claims.TenantID == "" || claims.UserID == "" || claims.ConversationID == "" || !expiresAt.After(time.Now()) {
		return "", errors.New("browser session missing required claims or expiration")
	}
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	body, err := json.Marshal(userSessionWireClaims{
		TenantID: claims.TenantID, UserID: claims.UserID, ConversationID: claims.ConversationID,
		Exp: expiresAt.Unix(), Aud: audience,
	})
	if err != nil {
		return "", fmt.Errorf("marshal browser session claims: %w", err)
	}
	signingInput := header + "." + base64.RawURLEncoding.EncodeToString(body)
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(signingInput))
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (v *HMACUserSessionVerifier) VerifySession(ctx context.Context, signedCookie string) (UserSessionClaims, error) {
	select {
	case <-ctx.Done():
		return UserSessionClaims{}, ctx.Err()
	default:
	}

	if len(signedCookie) > maxSignedTokenBytes {
		return UserSessionClaims{}, errors.New("browser session exceeds maximum allowed size")
	}

	parts := strings.Split(signedCookie, ".")
	if len(parts) != 3 {
		return UserSessionClaims{}, errors.New("invalid browser session format")
	}
	signingInput := parts[0] + "." + parts[1]
	mac := hmac.New(sha256.New, v.secret)
	_, _ = mac.Write([]byte(signingInput))
	expected := mac.Sum(nil)
	signature, err := decodeBase64URL(parts[2])
	if err != nil || len(signature) != len(expected) {
		return UserSessionClaims{}, errors.New("invalid browser session signature")
	}
	if subtle.ConstantTimeCompare(expected, signature) != 1 {
		return UserSessionClaims{}, errors.New("invalid browser session signature")
	}
	headerBytes, err := decodeBase64URL(parts[0])
	if err != nil {
		return UserSessionClaims{}, fmt.Errorf("decode browser session header: %w", err)
	}
	if err := checkDuplicateKeys(headerBytes); err != nil {
		return UserSessionClaims{}, fmt.Errorf("browser session header: %w", err)
	}
	var header struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
	}
	if err := unmarshalStrict(headerBytes, &header); err != nil {
		return UserSessionClaims{}, fmt.Errorf("invalid browser session header: %w", err)
	}
	if header.Alg != "HS256" || header.Typ != "JWT" {
		return UserSessionClaims{}, errors.New("invalid browser session header")
	}
	claimsBytes, err := decodeBase64URL(parts[1])
	if err != nil {
		return UserSessionClaims{}, fmt.Errorf("decode browser session claims: %w", err)
	}
	if err := checkDuplicateKeys(claimsBytes); err != nil {
		return UserSessionClaims{}, fmt.Errorf("browser session claims: %w", err)
	}
	var claims userSessionWireClaims
	if err := unmarshalStrict(claimsBytes, &claims); err != nil {
		return UserSessionClaims{}, fmt.Errorf("parse browser session claims: %w", err)
	}
	if claims.TenantID == "" || claims.UserID == "" || claims.ConversationID == "" || claims.Exp == 0 {
		return UserSessionClaims{}, errors.New("browser session missing required claims")
	}
	if time.Now().Unix() >= claims.Exp || claims.Aud != v.audience {
		return UserSessionClaims{}, errors.New("browser session expired or audience mismatch")
	}
	return UserSessionClaims{TenantID: claims.TenantID, UserID: claims.UserID, ConversationID: claims.ConversationID}, nil
}
