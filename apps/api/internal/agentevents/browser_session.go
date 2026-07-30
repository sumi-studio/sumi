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
	defaultBrowserAudience = "sumi:web:direct-chat"
	// BrowserSessionCookie is the name of the signed HttpOnly session cookie
	// used by browser routes.
	BrowserSessionCookie = "sumi_session"
)

// DefaultBrowserAudience returns the default audience for browser session
// cookies. It is exposed so production wiring and tests can agree on the value
// without hardcoding it in multiple packages.
func DefaultBrowserAudience() string { return defaultBrowserAudience }

// UserSessionClaims are deliberately separate from agent TokenClaims. A
// browser can act only as its authenticated human principal. The signed target
// never comes from a public route or browser-authored command.
type UserSessionClaims struct {
	TenantID           string
	UserID             string
	PersonalityAgentID string
}

// UserSessionVerifier validates the signed HttpOnly browser session cookie.
// The Firebase exchange control-plane issues this format server-side.
type UserSessionVerifier interface {
	VerifySession(ctx context.Context, signedCookie string) (UserSessionClaims, error)
}

type HMACUserSessionVerifier struct {
	secret   []byte
	audience string
}

// BrowserSessionIssuer creates the same short-lived signed session consumed by
// UserSessionVerifier. Issuance remains server-side: browsers never receive
// the tenant or personality-agent binding outside the opaque HttpOnly cookie.
type BrowserSessionIssuer interface {
	IssueSession(ctx context.Context, claims UserSessionClaims, ttl time.Duration) (string, error)
}

type userSessionWireClaims struct {
	TenantID           string `json:"tenant_id"`
	UserID             string `json:"user_id"`
	PersonalityAgentID string `json:"personality_agent_id"`
	Exp                int64  `json:"exp"`
	Aud                string `json:"aud"`
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

// IssueSession signs a bounded-lifetime browser session using the verifier's
// configured audience and key. Callers choose a short TTL appropriate for the
// browser authentication boundary.
func (v *HMACUserSessionVerifier) IssueSession(
	ctx context.Context,
	claims UserSessionClaims,
	ttl time.Duration,
) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}
	if !provenanceIDRegexp.MatchString(claims.TenantID) ||
		!provenanceIDRegexp.MatchString(claims.UserID) {
		return "", errors.New("browser session has invalid tenant or user binding")
	}
	if err := ValidatePersonalityAgentID(claims.PersonalityAgentID); err != nil {
		return "", fmt.Errorf("browser session personality_agent_id: %w", err)
	}
	if ttl < time.Minute || ttl > time.Hour {
		return "", errors.New("browser session TTL must be between one minute and one hour")
	}

	headerPart := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload, err := json.Marshal(userSessionWireClaims{
		TenantID:           claims.TenantID,
		UserID:             claims.UserID,
		PersonalityAgentID: claims.PersonalityAgentID,
		Exp:                time.Now().Add(ttl).Unix(),
		Aud:                v.audience,
	})
	if err != nil {
		return "", fmt.Errorf("marshal browser session claims: %w", err)
	}
	payloadPart := base64.RawURLEncoding.EncodeToString(payload)
	signingInput := headerPart + "." + payloadPart
	mac := hmac.New(sha256.New, v.secret)
	_, _ = mac.Write([]byte(signingInput))
	signed := signingInput + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if len(signed) > maxSignedTokenBytes {
		return "", errors.New("browser session exceeds maximum allowed size")
	}
	return signed, nil
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
	if !provenanceIDRegexp.MatchString(claims.TenantID) ||
		!provenanceIDRegexp.MatchString(claims.UserID) ||
		claims.PersonalityAgentID == "" ||
		claims.Exp == 0 {
		return UserSessionClaims{}, errors.New("browser session missing required claims")
	}
	if err := ValidatePersonalityAgentID(claims.PersonalityAgentID); err != nil {
		return UserSessionClaims{}, fmt.Errorf("browser session personality_agent_id: %w", err)
	}
	if time.Now().Unix() >= claims.Exp || claims.Aud != v.audience {
		return UserSessionClaims{}, errors.New("browser session expired or audience mismatch")
	}
	return UserSessionClaims{
		TenantID:           claims.TenantID,
		UserID:             claims.UserID,
		PersonalityAgentID: claims.PersonalityAgentID,
	}, nil
}
