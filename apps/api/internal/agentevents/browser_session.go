package agentevents

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	defaultBrowserAudience         = "sumi:web:direct-chat"
	maxBrowserSessionTTL           = time.Hour
	maxRevokedSessions             = 4096
	browserSessionIDBytes          = 32
	browserAuthorityBindingIDBytes = sha256.Size
	browserAuthorityBindingDomain  = "sumi:browser-authority-binding:v1"
	// BrowserSessionCookie is the name of the signed HttpOnly session cookie
	// used by browser routes.
	BrowserSessionCookie = "sumi_session"
)

var (
	errBrowserSessionMissing   = errors.New("browser session cookie is missing")
	errBrowserSessionDuplicate = errors.New("duplicate browser session cookies")
	errRevocationCapacity      = errors.New("browser session revocation capacity exhausted")
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
	sessionID          string
	expiresAt          time.Time
	authorityBindingID string
}

// UserSessionVerifier validates the signed HttpOnly browser session cookie.
// The Firebase exchange control-plane issues this format server-side.
type UserSessionVerifier interface {
	VerifySession(ctx context.Context, signedCookie string) (UserSessionClaims, error)
}

type HMACUserSessionVerifier struct {
	secret   []byte
	audience string
	now      func() time.Time
	random   io.Reader

	lifecycleMu sync.RWMutex
	revoked     map[string]int64
	maxRevoked  int
}

// BrowserSessionIssuer creates the same short-lived signed session consumed by
// UserSessionVerifier. Issuance remains server-side: browsers never receive
// the tenant or personality-agent binding outside the opaque HttpOnly cookie.
type BrowserSessionIssuer interface {
	IssueSession(ctx context.Context, claims UserSessionClaims, ttl time.Duration) (string, error)
}

// UserSessionAuthorizer serializes command admission against logout.
type UserSessionAuthorizer interface {
	UserSessionVerifier
	AuthorizeSession(ctx context.Context, claims UserSessionClaims, operation func() error) error
}

// BrowserSessionLifecycle owns issuance and process-local revocation in
// addition to command admission. Revocations are retained until signed expiry
// in a deliberately bounded in-memory set. A deployment with multiple API
// processes must replace this process-local authority with a shared store and
// connection fan-out before claiming cross-process immediate logout.
type BrowserSessionLifecycle interface {
	UserSessionAuthorizer
	BrowserSessionIssuer
	RevokeSession(ctx context.Context, signedCookie string) (UserSessionClaims, error)
}

type userSessionWireClaims struct {
	TenantID           string `json:"tenant_id"`
	UserID             string `json:"user_id"`
	PersonalityAgentID string `json:"personality_agent_id"`
	Iat                int64  `json:"iat"`
	Exp                int64  `json:"exp"`
	Aud                string `json:"aud"`
	SID                string `json:"sid"`
}

func NewHMACUserSessionVerifier(secret []byte, audience string) (*HMACUserSessionVerifier, error) {
	if len(secret) < 32 {
		return nil, errors.New("browser session HMAC secret must be at least 32 bytes")
	}
	if audience == "" {
		audience = defaultBrowserAudience
	}
	return &HMACUserSessionVerifier{
		secret:     append([]byte(nil), secret...),
		audience:   audience,
		now:        time.Now,
		random:     rand.Reader,
		revoked:    make(map[string]int64),
		maxRevoked: maxRevokedSessions,
	}, nil
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
	if ttl < time.Minute || ttl > maxBrowserSessionTTL {
		return "", errors.New("browser session TTL must be between one minute and one hour")
	}
	sessionIDBytes := make([]byte, browserSessionIDBytes)
	if _, err := io.ReadFull(v.random, sessionIDBytes); err != nil {
		return "", fmt.Errorf("generate browser session ID: %w", err)
	}
	now := v.now().UTC().Truncate(time.Second)
	sessionID := base64.RawURLEncoding.EncodeToString(sessionIDBytes)

	headerPart := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload, err := json.Marshal(userSessionWireClaims{
		TenantID:           claims.TenantID,
		UserID:             claims.UserID,
		PersonalityAgentID: claims.PersonalityAgentID,
		Iat:                now.Unix(),
		Exp:                now.Add(ttl).Unix(),
		Aud:                v.audience,
		SID:                sessionID,
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
	claims, err := v.verifySignedSession(ctx, signedCookie)
	if err != nil {
		return UserSessionClaims{}, err
	}
	v.lifecycleMu.RLock()
	defer v.lifecycleMu.RUnlock()
	if _, revoked := v.revoked[claims.sessionID]; revoked {
		return UserSessionClaims{}, errors.New("browser session revoked")
	}
	return claims, nil
}

func (v *HMACUserSessionVerifier) verifySignedSession(ctx context.Context, signedCookie string) (UserSessionClaims, error) {
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
		claims.Iat == 0 ||
		claims.Exp == 0 ||
		!validBrowserSessionID(claims.SID) {
		return UserSessionClaims{}, errors.New("browser session missing required claims")
	}
	if err := ValidatePersonalityAgentID(claims.PersonalityAgentID); err != nil {
		return UserSessionClaims{}, fmt.Errorf("browser session personality_agent_id: %w", err)
	}
	now := v.now().Unix()
	if now >= claims.Exp ||
		claims.Iat > now ||
		claims.Exp <= claims.Iat ||
		claims.Exp-claims.Iat > int64(maxBrowserSessionTTL/time.Second) ||
		claims.Aud != v.audience {
		return UserSessionClaims{}, errors.New("browser session expired or audience mismatch")
	}
	return UserSessionClaims{
		TenantID:           claims.TenantID,
		UserID:             claims.UserID,
		PersonalityAgentID: claims.PersonalityAgentID,
		sessionID:          claims.SID,
		expiresAt:          time.Unix(claims.Exp, 0),
		// Derive only after the cookie signature and claims have been
		// validated. A future verifier key ring must use the exact key that
		// verified this cookie; rotating the signing key then deliberately
		// changes the ID and causes a safe browser-side authority reset.
		authorityBindingID: deriveBrowserAuthorityBindingID(v.secret, claims),
	}, nil
}

func validBrowserSessionID(sessionID string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(sessionID)
	return err == nil &&
		len(decoded) == browserSessionIDBytes &&
		base64.RawURLEncoding.EncodeToString(decoded) == sessionID
}

func deriveBrowserAuthorityBindingID(
	secret []byte,
	claims userSessionWireClaims,
) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(browserAuthorityBindingDomain))
	var length [4]byte
	for _, value := range []string{
		claims.TenantID,
		claims.UserID,
		claims.PersonalityAgentID,
	} {
		binary.BigEndian.PutUint32(length[:], uint32(len(value)))
		_, _ = mac.Write(length[:])
		_, _ = mac.Write([]byte(value))
	}
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func validBrowserAuthorityBindingID(bindingID string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(bindingID)
	return err == nil &&
		len(decoded) == browserAuthorityBindingIDBytes &&
		base64.RawURLEncoding.EncodeToString(decoded) == bindingID
}

func uniqueBrowserSessionCookie(r *http.Request) (*http.Cookie, error) {
	cookies := r.CookiesNamed(BrowserSessionCookie)
	switch len(cookies) {
	case 0:
		return nil, errBrowserSessionMissing
	case 1:
		return cookies[0], nil
	default:
		return nil, errBrowserSessionDuplicate
	}
}

func browserSessionOperationContext(
	parent context.Context,
	claims UserSessionClaims,
) (context.Context, context.CancelFunc) {
	if claims.expiresAt.IsZero() {
		return context.WithCancel(parent)
	}
	return context.WithDeadline(parent, claims.expiresAt)
}

// AuthorizeSession holds a read lease across a security-sensitive operation.
// Logout takes the write lease, so its successful response is a barrier after
// which no command from that session can be appended.
func (v *HMACUserSessionVerifier) AuthorizeSession(
	ctx context.Context,
	claims UserSessionClaims,
	operation func() error,
) error {
	if operation == nil {
		return errors.New("browser session authorization operation is required")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	v.lifecycleMu.RLock()
	defer v.lifecycleMu.RUnlock()
	if claims.sessionID == "" || !validBrowserSessionID(claims.sessionID) {
		return errors.New("browser session has invalid lifecycle claims")
	}
	if _, revoked := v.revoked[claims.sessionID]; revoked {
		return errors.New("browser session revoked")
	}
	if !v.now().Before(claims.expiresAt) {
		return errors.New("browser session expired")
	}
	return operation()
}

// RevokeSession invalidates one signed browser session until its expiration.
// Revocations are process-local and time-bounded; callers must treat a full
// revocation set as an unavailable logout rather than evicting a live entry.
func (v *HMACUserSessionVerifier) RevokeSession(
	ctx context.Context,
	signedCookie string,
) (UserSessionClaims, error) {
	claims, err := v.verifySignedSession(ctx, signedCookie)
	if err != nil {
		return UserSessionClaims{}, err
	}
	v.lifecycleMu.Lock()
	defer v.lifecycleMu.Unlock()
	now := v.now().Unix()
	if now >= claims.expiresAt.Unix() {
		return UserSessionClaims{}, errors.New("browser session expired")
	}
	for sessionID, exp := range v.revoked {
		if now >= exp {
			delete(v.revoked, sessionID)
		}
	}
	if _, exists := v.revoked[claims.sessionID]; exists {
		return claims, nil
	}
	if len(v.revoked) >= v.maxRevoked {
		return UserSessionClaims{}, errRevocationCapacity
	}
	v.revoked[claims.sessionID] = claims.expiresAt.Unix()
	return claims, nil
}
