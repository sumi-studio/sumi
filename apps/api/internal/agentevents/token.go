package agentevents

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"
)

const defaultAgentAudience = "sumi:agent:events"
const maxSignedTokenBytes = 8 * 1024

// DefaultAgentAudience returns the strict audience used by agent bearer
// credentials when production/local wiring does not override it.
func DefaultAgentAudience() string { return defaultAgentAudience }

// HMACTokenVerifier validates short-lived agent tokens signed with HMAC-SHA256.
// It is a T28 production wiring point for the API-side token verification seam.
type HMACTokenVerifier struct {
	secret   []byte
	audience string
}

type tokenClaims struct {
	TenantID           string `json:"tenant_id"`
	PersonalityAgentID string `json:"personality_agent_id"`
	Generation         uint64 `json:"generation"`
	Exp                int64  `json:"exp"`
	Aud                string `json:"aud"`
}

// NewHMACTokenVerifier returns a verifier that checks HS256 tokens carrying the
// claims described by contracts/agent-events.yaml: tenant_id,
// personality_agent_id, generation, exp and aud. The secret must be at least 32
// bytes. If audience is empty, the default "sumi:agent:events" is required.
func NewHMACTokenVerifier(secret []byte, audience string) (*HMACTokenVerifier, error) {
	if len(secret) < 32 {
		return nil, errors.New("token HMAC secret must be at least 32 bytes")
	}
	if audience == "" {
		audience = defaultAgentAudience
	}
	return &HMACTokenVerifier{secret: append([]byte(nil), secret...), audience: audience}, nil
}

// Verify checks the signature, expiry, audience and required claim shape of an
// Authorization bearer token. It fail-closes on any malformed or mismatched
// input. The signature is verified before any claim is parsed or trusted.
func (v *HMACTokenVerifier) Verify(ctx context.Context, token string) (TokenClaims, error) {
	select {
	case <-ctx.Done():
		return TokenClaims{}, ctx.Err()
	default:
	}

	if len(token) > maxSignedTokenBytes {
		return TokenClaims{}, errors.New("token exceeds maximum allowed size")
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return TokenClaims{}, errors.New("invalid token format")
	}

	signingInput := parts[0] + "." + parts[1]
	mac := hmac.New(sha256.New, v.secret)
	_, _ = mac.Write([]byte(signingInput))
	expected := mac.Sum(nil)
	signature, err := decodeBase64URL(parts[2])
	if err != nil || len(signature) != len(expected) {
		return TokenClaims{}, errors.New("invalid token signature")
	}
	if subtle.ConstantTimeCompare(expected, signature) != 1 {
		return TokenClaims{}, errors.New("invalid token signature")
	}

	headerBytes, err := decodeBase64URL(parts[0])
	if err != nil {
		return TokenClaims{}, fmt.Errorf("decode token header: %w", err)
	}
	if err := checkDuplicateKeys(headerBytes); err != nil {
		return TokenClaims{}, fmt.Errorf("parse token header: %w", err)
	}
	var header struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
	}
	if err := unmarshalStrict(headerBytes, &header); err != nil {
		return TokenClaims{}, fmt.Errorf("parse token header: %w", err)
	}
	if header.Alg != "HS256" {
		return TokenClaims{}, fmt.Errorf("unexpected token alg: %q", header.Alg)
	}
	if header.Typ != "JWT" {
		return TokenClaims{}, fmt.Errorf("unexpected token typ: %q", header.Typ)
	}

	claimsBytes, err := decodeBase64URL(parts[1])
	if err != nil {
		return TokenClaims{}, fmt.Errorf("decode token claims: %w", err)
	}
	if err := checkDuplicateKeys(claimsBytes); err != nil {
		return TokenClaims{}, fmt.Errorf("parse token claims: %w", err)
	}
	var tc tokenClaims
	if err := unmarshalStrict(claimsBytes, &tc); err != nil {
		return TokenClaims{}, fmt.Errorf("parse token claims: %w", err)
	}
	if !provenanceIDRegexp.MatchString(tc.TenantID) || tc.PersonalityAgentID == "" {
		return TokenClaims{}, errors.New("token missing required identity claims")
	}
	if err := ValidatePersonalityAgentID(tc.PersonalityAgentID); err != nil {
		return TokenClaims{}, fmt.Errorf("token personality_agent_id: %w", err)
	}
	if tc.Generation > maxProcessGeneration {
		return TokenClaims{}, fmt.Errorf("token generation %d exceeds process generation range", tc.Generation)
	}
	if tc.Exp == 0 {
		return TokenClaims{}, errors.New("token missing expiry")
	}
	if time.Now().Unix() >= tc.Exp {
		return TokenClaims{}, errors.New("token expired")
	}
	if v.audience != "" && tc.Aud != v.audience {
		return TokenClaims{}, fmt.Errorf("token audience mismatch: %q", tc.Aud)
	}

	return TokenClaims{
		TenantID:           tc.TenantID,
		PersonalityAgentID: tc.PersonalityAgentID,
		Generation:         tc.Generation,
	}, nil
}

func decodeBase64URL(s string) ([]byte, error) {
	s = strings.TrimRight(s, "=")
	return base64.RawURLEncoding.DecodeString(s)
}
