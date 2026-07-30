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
	client *firebaseauth.Client
}

func (v *firebaseAdminIDTokenVerifier) VerifyIDToken(
	ctx context.Context,
	idToken string,
) (agentevents.FirebaseIdentity, error) {
	if v == nil || v.client == nil {
		return agentevents.FirebaseIdentity{}, errors.New("Firebase verifier is unavailable")
	}
	token, err := v.client.VerifyIDToken(ctx, idToken)
	if err != nil {
		return agentevents.FirebaseIdentity{}, err
	}
	if token.UID == "" {
		return agentevents.FirebaseIdentity{}, errors.New("verified Firebase token has no UID")
	}
	return agentevents.FirebaseIdentity{UID: token.UID}, nil
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

	bindings, err := agentevents.NewStaticIdentityBindingResolver(
		firebaseUID,
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

	secureCookies := true
	if raw := strings.TrimSpace(os.Getenv("SUMI_AUTH_ALLOW_INSECURE_COOKIES")); raw != "" {
		allow, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, false, errors.New("SUMI_AUTH_ALLOW_INSECURE_COOKIES must be a boolean")
		}
		secureCookies = !allow
	}

	server, err := agentevents.NewBrowserAuthServer(
		&firebaseAdminIDTokenVerifier{client: client},
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
