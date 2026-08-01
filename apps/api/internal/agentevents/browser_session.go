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
	"time"
)

const (
	// Surface-neutral: one browser session serves every app surface. The
	// previous "sumi:web:direct-chat" audience is not accepted; the claim key
	// rename (user_id → human_id) and this cutover are one break, not two.
	defaultBrowserAudience         = "sumi:web"
	maxBrowserSessionTTL           = time.Hour
	maxRevokedSessions             = 4096
	browserSessionIDBytes          = 32
	browserAuthorityBindingIDBytes = sha256.Size
	browserSessionSigningKeyDomain = "sumi:browser-session-signing-key:v2"
	browserAuthorityBindingDomain  = "sumi:browser-authority-binding:v1"
	// BrowserSessionCookie is the name of the signed HttpOnly session cookie
	// used by browser routes.
	BrowserSessionCookie = "sumi_session"
)

var (
	errBrowserSessionMissing   = errors.New("browser session cookie is missing")
	errBrowserSessionDuplicate = errors.New("duplicate browser session cookies")
	errBrowserSessionRevoked   = errors.New("browser session revoked")
	errBrowserSessionRetired   = errors.New("browser session already retired")
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
	HumanID            string
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
	signingKey          []byte
	authorityBindingKey []byte
	audience            string
	now                 func() time.Time
	random              io.Reader
	revocations         BrowserSessionRevocationStore
}

// BrowserSessionRevocationStore is the shared durability and serialization
// boundary for browser-session admission, rotation, and logout. Implementations
// must serialize admission against mutations and make successful revocations
// and rotations visible across API processes and restarts.
type BrowserSessionRevocationStore interface {
	CheckBrowserSession(
		ctx context.Context,
		sessionID string,
		expiresAt time.Time,
		now time.Time,
	) error
	AuthorizeBrowserSession(
		ctx context.Context,
		sessionID string,
		expiresAt time.Time,
		now time.Time,
		operation func() error,
	) error
	RevokeBrowserSession(
		ctx context.Context,
		sessionID string,
		expiresAt time.Time,
		now time.Time,
	) error
	RotateBrowserSession(
		ctx context.Context,
		currentSessionID string,
		currentExpiresAt time.Time,
		successorSessionID string,
		successorExpiresAt time.Time,
		now time.Time,
	) error
}

// BrowserSessionIssuer creates the same short-lived signed session consumed by
// UserSessionVerifier. Issuance remains server-side: browsers never receive
// the tenant or personality-agent binding outside the opaque HttpOnly cookie.
type BrowserSessionIssuer interface {
	IssueSession(ctx context.Context, claims UserSessionClaims, ttl time.Duration) (string, error)
}

type hmacBrowserSessionIssuer struct {
	signer *HMACUserSessionVerifier
}

func (i hmacBrowserSessionIssuer) IssueSession(
	ctx context.Context,
	claims UserSessionClaims,
	ttl time.Duration,
) (string, error) {
	return i.signer.IssueSession(ctx, claims, ttl)
}

// UserSessionAuthorizer serializes command admission against logout.
type UserSessionAuthorizer interface {
	UserSessionVerifier
	AuthorizeSession(ctx context.Context, claims UserSessionClaims, operation func() error) error
}

// BrowserSessionLifecycle owns issuance and shared durable revocation in
// addition to command admission.
type BrowserSessionLifecycle interface {
	UserSessionAuthorizer
	BrowserSessionIssuer
	RevokeSession(ctx context.Context, signedCookie string) (UserSessionClaims, error)
	RevokeSessionForLogout(
		ctx context.Context,
		signedCookie string,
	) (UserSessionClaims, bool, error)
	RotateSession(
		ctx context.Context,
		signedCookie string,
		successorClaims UserSessionClaims,
		ttl time.Duration,
	) (UserSessionClaims, string, bool, error)
}

type userSessionWireClaims struct {
	TenantID           string `json:"tenant_id"`
	HumanID            string `json:"human_id"`
	PersonalityAgentID string `json:"personality_agent_id"`
	Iat                int64  `json:"iat"`
	Exp                int64  `json:"exp"`
	Aud                string `json:"aud"`
	SID                string `json:"sid"`
}

type preparedBrowserSession struct {
	claims       UserSessionClaims
	signingInput string
}

func NewHMACUserSessionVerifier(
	secret []byte,
	audience string,
	revocations BrowserSessionRevocationStore,
) (*HMACUserSessionVerifier, error) {
	if revocations == nil {
		return nil, errors.New("browser session revocation store is required")
	}
	verifier, err := newHMACBrowserSessionSigner(secret, audience)
	if err != nil {
		return nil, err
	}
	verifier.revocations = revocations
	return verifier, nil
}

// NewHMACBrowserSessionIssuer constructs an issuance-only signer for tooling
// that never accepts browser cookies. Verification always requires the shared
// revocation store constructor above.
func NewHMACBrowserSessionIssuer(
	secret []byte,
	audience string,
) (BrowserSessionIssuer, error) {
	signer, err := newHMACBrowserSessionSigner(secret, audience)
	if err != nil {
		return nil, err
	}
	return hmacBrowserSessionIssuer{signer: signer}, nil
}

func newHMACBrowserSessionSigner(
	secret []byte,
	audience string,
) (*HMACUserSessionVerifier, error) {
	if len(secret) < 32 {
		return nil, errors.New("browser session HMAC secret must be at least 32 bytes")
	}
	if audience == "" {
		audience = defaultBrowserAudience
	}
	return &HMACUserSessionVerifier{
		signingKey:          deriveBrowserSessionSigningKey(secret),
		authorityBindingKey: append([]byte(nil), secret...),
		audience:            audience,
		now:                 time.Now,
		random:              rand.Reader,
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
	prepared, err := v.prepareSession(ctx, claims, ttl)
	if err != nil {
		return "", err
	}
	return v.signPreparedSession(prepared), nil
}

func (v *HMACUserSessionVerifier) prepareSession(
	ctx context.Context,
	claims UserSessionClaims,
	ttl time.Duration,
) (preparedBrowserSession, error) {
	select {
	case <-ctx.Done():
		return preparedBrowserSession{}, ctx.Err()
	default:
	}
	if !provenanceIDRegexp.MatchString(claims.TenantID) ||
		ValidateHumanID(claims.HumanID) != nil {
		return preparedBrowserSession{}, errors.New("browser session has invalid tenant or user binding")
	}
	if err := ValidatePersonalityAgentID(claims.PersonalityAgentID); err != nil {
		return preparedBrowserSession{}, fmt.Errorf("browser session personality_agent_id: %w", err)
	}
	if ttl < time.Minute || ttl > maxBrowserSessionTTL {
		return preparedBrowserSession{}, errors.New("browser session TTL must be between one minute and one hour")
	}
	sessionIDBytes := make([]byte, browserSessionIDBytes)
	if _, err := io.ReadFull(v.random, sessionIDBytes); err != nil {
		return preparedBrowserSession{}, fmt.Errorf("generate browser session ID: %w", err)
	}
	now := v.now().UTC().Truncate(time.Second)
	sessionID := base64.RawURLEncoding.EncodeToString(sessionIDBytes)

	headerPart := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	wireClaims := userSessionWireClaims{
		TenantID:           claims.TenantID,
		HumanID:            claims.HumanID,
		PersonalityAgentID: claims.PersonalityAgentID,
		Iat:                now.Unix(),
		Exp:                now.Add(ttl).Unix(),
		Aud:                v.audience,
		SID:                sessionID,
	}
	payload, err := json.Marshal(wireClaims)
	if err != nil {
		return preparedBrowserSession{}, fmt.Errorf("marshal browser session claims: %w", err)
	}
	payloadPart := base64.RawURLEncoding.EncodeToString(payload)
	signingInput := headerPart + "." + payloadPart
	// HS256 always contributes 43 raw-base64url bytes. Validate the final
	// token size before a rotation commits its successor lineage.
	if len(signingInput)+1+base64.RawURLEncoding.EncodedLen(sha256.Size) >
		maxSignedTokenBytes {
		return preparedBrowserSession{}, errors.New("browser session exceeds maximum allowed size")
	}
	return preparedBrowserSession{
		claims: UserSessionClaims{
			TenantID:           wireClaims.TenantID,
			HumanID:            wireClaims.HumanID,
			PersonalityAgentID: wireClaims.PersonalityAgentID,
			sessionID:          wireClaims.SID,
			expiresAt:          time.Unix(wireClaims.Exp, 0),
			authorityBindingID: deriveBrowserAuthorityBindingID(
				v.authorityBindingKey,
				wireClaims,
			),
		},
		signingInput: signingInput,
	}, nil
}

func (v *HMACUserSessionVerifier) signPreparedSession(
	prepared preparedBrowserSession,
) string {
	mac := hmac.New(sha256.New, v.signingKey)
	_, _ = mac.Write([]byte(prepared.signingInput))
	return prepared.signingInput + "." +
		base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (v *HMACUserSessionVerifier) VerifySession(ctx context.Context, signedCookie string) (UserSessionClaims, error) {
	claims, err := v.verifySignedSession(ctx, signedCookie)
	if err != nil {
		return UserSessionClaims{}, err
	}
	if err := v.revocations.CheckBrowserSession(
		ctx,
		claims.sessionID,
		claims.expiresAt,
		v.now(),
	); err != nil {
		return UserSessionClaims{}, fmt.Errorf("check browser session revocation: %w", err)
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
	mac := hmac.New(sha256.New, v.signingKey)
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
		ValidateHumanID(claims.HumanID) != nil ||
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
		HumanID:            claims.HumanID,
		PersonalityAgentID: claims.PersonalityAgentID,
		sessionID:          claims.SID,
		expiresAt:          time.Unix(claims.Exp, 0),
		// The protocol-versioned cookie signing key deliberately differs
		// from this stable authority key. Upgrading the cookie protocol
		// fences old credentials without resetting the same human/target
		// binding; rotating the configured base secret still resets it.
		authorityBindingID: deriveBrowserAuthorityBindingID(
			v.authorityBindingKey,
			claims,
		),
	}, nil
}

func deriveBrowserSessionSigningKey(secret []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(browserSessionSigningKeyDomain))
	return mac.Sum(nil)
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
		claims.HumanID,
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

// AuthorizeSession holds a shared durable admission lease across a
// security-sensitive operation. Logout takes the exclusive lease, so its
// successful response is a cross-process barrier after which no command from
// that session can be appended.
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
	if claims.sessionID == "" || !validBrowserSessionID(claims.sessionID) {
		return errors.New("browser session has invalid lifecycle claims")
	}
	if !v.now().Before(claims.expiresAt) {
		return errors.New("browser session expired")
	}
	if err := v.revocations.AuthorizeBrowserSession(
		ctx,
		claims.sessionID,
		claims.expiresAt,
		v.now(),
		operation,
	); err != nil {
		return fmt.Errorf("authorize browser session: %w", err)
	}
	return nil
}

// RevokeSession invalidates one signed browser session until its expiration.
// Revocations are durable, shared, and time-bounded; callers must treat a full
// or unavailable revocation store as an unavailable logout.
func (v *HMACUserSessionVerifier) RevokeSession(
	ctx context.Context,
	signedCookie string,
) (UserSessionClaims, error) {
	claims, err := v.verifySignedSession(ctx, signedCookie)
	if err != nil {
		return UserSessionClaims{}, err
	}
	now := v.now()
	if !now.Before(claims.expiresAt) {
		return UserSessionClaims{}, errors.New("browser session expired")
	}
	if err := v.revocations.RevokeBrowserSession(
		ctx,
		claims.sessionID,
		claims.expiresAt,
		now,
	); err != nil {
		return UserSessionClaims{}, fmt.Errorf("revoke browser session: %w", err)
	}
	return claims, nil
}

// RevokeSessionForLogout distinguishes a locally invalid or expired cookie
// from a valid signed cookie that requires durable revocation. The boolean is
// true whenever local verification succeeds, including when the durable
// update fails, so callers can retain valid credentials for a logout retry.
func (v *HMACUserSessionVerifier) RevokeSessionForLogout(
	ctx context.Context,
	signedCookie string,
) (UserSessionClaims, bool, error) {
	claims, err := v.verifySignedSession(ctx, signedCookie)
	if err != nil {
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			return UserSessionClaims{}, false, err
		}
		return UserSessionClaims{}, false, nil
	}
	now := v.now()
	if err := v.revocations.RevokeBrowserSession(
		ctx,
		claims.sessionID,
		claims.expiresAt,
		now,
	); err != nil {
		return claims, true, fmt.Errorf(
			"revoke browser session for logout: %w",
			err,
		)
	}
	return claims, true, nil
}

// RotateSession atomically consumes one valid signed session and binds its
// successor in shared durable lineage state before signing or returning the
// replacement token. If logout linearizes after that store commit, it revokes
// the successor even when the token has not yet been delivered.
func (v *HMACUserSessionVerifier) RotateSession(
	ctx context.Context,
	signedCookie string,
	successorClaims UserSessionClaims,
	ttl time.Duration,
) (UserSessionClaims, string, bool, error) {
	current, err := v.verifySignedSession(ctx, signedCookie)
	if err != nil {
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			return UserSessionClaims{}, "", false, err
		}
		return UserSessionClaims{}, "", false, nil
	}
	successor, err := v.prepareSession(ctx, successorClaims, ttl)
	if err != nil {
		return current, "", true, err
	}
	if err := v.revocations.RotateBrowserSession(
		ctx,
		current.sessionID,
		current.expiresAt,
		successor.claims.sessionID,
		successor.claims.expiresAt,
		v.now(),
	); err != nil {
		return current, "", true, fmt.Errorf(
			"rotate browser session: %w",
			err,
		)
	}
	// All potentially failing preparation completed before the store commit.
	// HMAC signing itself is deterministic and cannot strand an untracked live
	// successor after this point.
	return current, v.signPreparedSession(successor), true, nil
}
