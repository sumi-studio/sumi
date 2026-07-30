package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	firebase "firebase.google.com/go/v4"
	firebaseauth "firebase.google.com/go/v4/auth"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

const (
	defaultAuthSessionTTL = 15 * time.Minute
	minAuthSessionTTL     = time.Minute
	maxAuthSessionTTL     = time.Hour
)

type firebaseAdminIDTokenVerifier struct {
	client firebaseIDTokenClient
}

type firebaseIDTokenClient interface {
	VerifyIDTokenAndCheckRevoked(ctx context.Context, idToken string) (*firebaseauth.Token, error)
}

func (v *firebaseAdminIDTokenVerifier) VerifyIDToken(
	ctx context.Context,
	idToken string,
) (agentevents.FirebaseIdentity, error) {
	if v == nil || v.client == nil {
		return agentevents.FirebaseIdentity{}, errors.New("Firebase verifier is unavailable")
	}
	token, err := v.client.VerifyIDTokenAndCheckRevoked(ctx, idToken)
	if err != nil {
		return agentevents.FirebaseIdentity{}, err
	}
	if token == nil || token.UID == "" {
		return agentevents.FirebaseIdentity{}, errors.New("verified Firebase token has no UID")
	}
	return agentevents.FirebaseIdentity{UID: token.UID, TenantID: token.Firebase.Tenant}, nil
}

// browserAuthServerFromEnv creates the Firebase exchange boundary only when
// SUMI_AUTH_FIREBASE_UID explicitly opts in. Partial opt-in is a startup error;
// no authentication route is registered for an entirely absent configuration.
func browserAuthServerFromEnv(
	ctx context.Context,
	sessions *agentevents.HMACUserSessionVerifier,
	allowedOrigins []string,
) (*agentevents.BrowserAuthServer, bool, error) {
	firebaseUID := strings.TrimSpace(os.Getenv("SUMI_AUTH_FIREBASE_UID"))
	if firebaseUID == "" {
		for _, name := range []string{
			"SUMI_AUTH_FIREBASE_TENANT_ID",
			"SUMI_AUTH_TENANT_ID",
			"SUMI_AUTH_USER_ID",
			"SUMI_AUTH_PERSONALITY_AGENT_ID",
			"SUMI_AUTH_FIREBASE_PROJECT_ID",
			"SUMI_AUTH_ALLOW_INSECURE_COOKIES",
			"SUMI_AUTH_SESSION_TTL",
		} {
			if strings.TrimSpace(os.Getenv(name)) != "" {
				return nil, false, errors.New("SUMI_AUTH_FIREBASE_UID is required when any SUMI_AUTH_* setting is configured")
			}
		}
		return nil, false, nil
	}
	if sessions == nil {
		return nil, false, errors.New("SUMI_BROWSER_SESSION_SECRET is required when Firebase auth is enabled")
	}

	tenantID := strings.TrimSpace(os.Getenv("SUMI_AUTH_TENANT_ID"))
	userID := strings.TrimSpace(os.Getenv("SUMI_AUTH_USER_ID"))
	personalityAgentID := strings.TrimSpace(os.Getenv("SUMI_AUTH_PERSONALITY_AGENT_ID"))
	if tenantID == "" || userID == "" || personalityAgentID == "" {
		return nil, false, errors.New("SUMI_AUTH_TENANT_ID, SUMI_AUTH_USER_ID, and SUMI_AUTH_PERSONALITY_AGENT_ID are required when Firebase auth is enabled")
	}

	projectID := strings.TrimSpace(os.Getenv("SUMI_AUTH_FIREBASE_PROJECT_ID"))
	if projectID == "" {
		projectID = strings.TrimSpace(os.Getenv("GOOGLE_CLOUD_PROJECT"))
	}
	if projectID == "" {
		return nil, false, errors.New("SUMI_AUTH_FIREBASE_PROJECT_ID or GOOGLE_CLOUD_PROJECT is required when Firebase auth is enabled")
	}

	firebaseTenantID := strings.TrimSpace(os.Getenv("SUMI_AUTH_FIREBASE_TENANT_ID"))
	bindings, err := agentevents.NewStaticIdentityBindingResolverForTenant(
		firebaseUID,
		firebaseTenantID,
		agentevents.UserSessionClaims{
			TenantID:           tenantID,
			UserID:             userID,
			PersonalityAgentID: personalityAgentID,
		},
	)
	if err != nil {
		return nil, false, fmt.Errorf("Firebase identity binding: %w", err)
	}

	app, err := firebase.NewApp(ctx, &firebase.Config{ProjectID: projectID})
	if err != nil {
		return nil, false, fmt.Errorf("initialize Firebase Admin SDK: %w", err)
	}
	client, err := app.Auth(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("initialize Firebase Auth client: %w", err)
	}
	var verifierClient firebaseIDTokenClient = client
	if firebaseTenantID != "" {
		tenantClient, err := client.TenantManager.AuthForTenant(firebaseTenantID)
		if err != nil {
			return nil, false, fmt.Errorf("initialize Firebase tenant Auth client: %w", err)
		}
		verifierClient = tenantClient
	}

	secureCookies := true
	if raw := strings.TrimSpace(os.Getenv("SUMI_AUTH_ALLOW_INSECURE_COOKIES")); raw != "" {
		allow, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, false, errors.New("SUMI_AUTH_ALLOW_INSECURE_COOKIES must be a boolean")
		}
		secureCookies = !allow
	}

	server, err := agentevents.NewBrowserAuthServer(
		&firebaseAdminIDTokenVerifier{client: verifierClient},
		bindings,
		sessions,
		allowedOrigins,
		secureCookies,
	)
	if err != nil {
		return nil, false, err
	}
	ttl, err := authSessionTTLFromEnv()
	if err != nil {
		return nil, false, err
	}
	server.SessionTTL = ttl
	return server, true, nil
}

func authSessionTTLFromEnv() (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv("SUMI_AUTH_SESSION_TTL"))
	if raw == "" {
		return defaultAuthSessionTTL, nil
	}
	ttl, err := time.ParseDuration(raw)
	if err != nil || ttl < minAuthSessionTTL || ttl > maxAuthSessionTTL {
		return 0, errors.New("SUMI_AUTH_SESSION_TTL must be between one minute and one hour")
	}
	return ttl, nil
}
