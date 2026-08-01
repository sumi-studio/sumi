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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

const (
	defaultAuthSessionTTL = 15 * time.Minute
	minAuthSessionTTL     = time.Minute
	maxAuthSessionTTL     = time.Hour
)

var browserAuthEnvironmentNames = []string{
	"SUMI_AUTH_FIREBASE_UID",
	"SUMI_AUTH_FIREBASE_TENANT_ID",
	"SUMI_AUTH_TENANT_ID",
	"SUMI_AUTH_USER_ID",
	"SUMI_AUTH_PERSONALITY_AGENT_ID",
	"SUMI_AUTH_FIREBASE_PROJECT_ID",
	"SUMI_AUTH_ALLOW_INSECURE_COOKIES",
	"SUMI_AUTH_SESSION_TTL",
}

func browserAuthConfiguredFromEnv() bool {
	for _, name := range browserAuthEnvironmentNames {
		if strings.TrimSpace(os.Getenv(name)) != "" {
			return true
		}
	}
	return false
}

type firebaseAdminIDTokenVerifier struct {
	client firebaseIDTokenClient
}

type firebaseIDTokenClient interface {
	VerifyIDTokenAndCheckRevoked(ctx context.Context, idToken string) (*firebaseauth.Token, error)
}

type firebaseProviderUserClient interface {
	GetUser(ctx context.Context, uid string) (*firebaseauth.UserRecord, error)
	UpdateUser(ctx context.Context, uid string, user *firebaseauth.UserToUpdate) (*firebaseauth.UserRecord, error)
}

type firebaseAdminProviderLifecycle struct {
	client firebaseProviderUserClient
}

func (a *firebaseAdminProviderLifecycle) ProviderAccount(ctx context.Context, firebaseUID string) (firebaseProviderAccount, error) {
	if a == nil || a.client == nil {
		return firebaseProviderAccount{}, errors.New("firebase provider lifecycle is unavailable")
	}
	user, err := a.client.GetUser(ctx, firebaseUID)
	if err != nil {
		return firebaseProviderAccount{}, err
	}
	return firebaseProviderAccountFromUser(user, firebaseUID)
}

func (a *firebaseAdminProviderLifecycle) DeleteProvider(ctx context.Context, firebaseUID, provider string) error {
	if a == nil || a.client == nil {
		return errors.New("firebase provider lifecycle is unavailable")
	}
	if provider != "google.com" && provider != "github.com" {
		return errors.New("unsupported Firebase provider unlink")
	}
	_, err := a.client.UpdateUser(ctx, firebaseUID,
		(&firebaseauth.UserToUpdate{}).ProvidersToDelete([]string{provider}))
	return err
}

func firebaseProviderAccountFromUser(user *firebaseauth.UserRecord, expectedUID string) (firebaseProviderAccount, error) {
	if user == nil || user.UserInfo == nil || user.UID == "" || user.UID != expectedUID {
		return firebaseProviderAccount{}, errors.New("firebase provider account identity mismatch")
	}
	account := firebaseProviderAccount{UID: user.UID, ProviderSubjects: make(map[string]string, 2)}
	for _, provider := range user.ProviderUserInfo {
		if provider == nil {
			continue
		}
		switch provider.ProviderID {
		case "password", "email":
			// This proves only that the live Firebase account still exposes the
			// email/password provider family. Sumi's own completed email-link
			// proof is also required before it counts as a usable method.
			account.EmailProvider = true
		case "google.com", "github.com":
			if provider.UID == "" {
				return firebaseProviderAccount{}, errors.New("firebase provider has no subject")
			}
			if existing, ok := account.ProviderSubjects[provider.ProviderID]; ok && existing != provider.UID {
				return firebaseProviderAccount{}, errors.New("firebase provider has ambiguous subjects")
			}
			account.ProviderSubjects[provider.ProviderID] = provider.UID
		}
	}
	return account, nil
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
	email, _ := token.Claims["email"].(string)
	emailVerified, _ := token.Claims["email_verified"].(bool)
	providerSubjects := make(map[string][]string, len(token.Firebase.Identities))
	for provider, raw := range token.Firebase.Identities {
		switch values := raw.(type) {
		case []interface{}:
			for _, value := range values {
				if subject, ok := value.(string); ok && subject != "" {
					providerSubjects[provider] = append(providerSubjects[provider], subject)
				}
			}
		case []string:
			for _, subject := range values {
				if subject != "" {
					providerSubjects[provider] = append(providerSubjects[provider], subject)
				}
			}
		}
	}
	return agentevents.FirebaseIdentity{
		UID: token.UID, TenantID: token.Firebase.Tenant, Email: email,
		EmailVerified: emailVerified, SignInProvider: token.Firebase.SignInProvider,
		ProviderSubjects: providerSubjects, AuthTime: time.Unix(token.AuthTime, 0).UTC(),
		IssuedAt: time.Unix(token.IssuedAt, 0).UTC(),
	}, nil
}

// browserAuthServerFromEnv creates the Firebase exchange boundary. When a
// control-plane database pool is unavailable it falls back to the legacy
// StaticIdentityBindingResolver (single configured Firebase UID). Partial static
// opt-in is a startup error; no authentication route is registered for an
// entirely absent configuration.
func browserAuthServerFromEnv(
	ctx context.Context,
	sessions *agentevents.HMACUserSessionVerifier,
	allowedOrigins []string,
) (*agentevents.BrowserAuthServer, bool, error) {
	return browserAuthServerFromEnvWithDB(ctx, sessions, allowedOrigins, nil)
}

// browserAuthServerFromEnvWithDB enables the explicit 戸籍 auth-flow boundary
// when pool is non-nil. Production routes then require a persisted sign-in or
// sign-up intent and never invoke the old resolver's silent auto-registration
// exchange. A nil pool is retained only for the isolated static-binding test
// fixture.
func browserAuthServerFromEnvWithDB(
	ctx context.Context,
	sessions *agentevents.HMACUserSessionVerifier,
	allowedOrigins []string,
	pool *pgxpool.Pool,
) (*agentevents.BrowserAuthServer, bool, error) {
	firebaseUID := strings.TrimSpace(os.Getenv("SUMI_AUTH_FIREBASE_UID"))
	kosekiMode := pool != nil
	if !kosekiMode && firebaseUID == "" {
		for _, name := range browserAuthEnvironmentNames {
			if name == "SUMI_AUTH_FIREBASE_UID" {
				continue
			}
			if strings.TrimSpace(os.Getenv(name)) != "" {
				return nil, false, errors.New("SUMI_AUTH_FIREBASE_UID is required when any SUMI_AUTH_* setting is configured")
			}
		}
		return nil, false, nil
	}
	if sessions == nil {
		return nil, false, errors.New("SUMI_BROWSER_SESSION_SECRET is required when Firebase auth is enabled")
	}

	projectID := strings.TrimSpace(os.Getenv("SUMI_AUTH_FIREBASE_PROJECT_ID"))
	if projectID == "" {
		projectID = strings.TrimSpace(os.Getenv("GOOGLE_CLOUD_PROJECT"))
	}
	if projectID == "" {
		return nil, false, errors.New("SUMI_AUTH_FIREBASE_PROJECT_ID or GOOGLE_CLOUD_PROJECT is required when Firebase auth is enabled")
	}

	firebaseTenantID := strings.TrimSpace(os.Getenv("SUMI_AUTH_FIREBASE_TENANT_ID"))
	var bindings agentevents.IdentityBindingResolver
	if kosekiMode {
		tenantID := strings.TrimSpace(os.Getenv("SUMI_AUTH_TENANT_ID"))
		if tenantID == "" {
			return nil, false, errors.New("SUMI_AUTH_TENANT_ID is required for 戸籍-backed authentication")
		}
		store := koseki.New(pool)
		bindings = newKosekiIdentityBindingResolver(store, tenantID, "firebase")
	} else {
		tenantID := strings.TrimSpace(os.Getenv("SUMI_AUTH_TENANT_ID"))
		userID := strings.TrimSpace(os.Getenv("SUMI_AUTH_USER_ID"))
		personalityAgentID := strings.TrimSpace(os.Getenv("SUMI_AUTH_PERSONALITY_AGENT_ID"))
		if tenantID == "" || userID == "" || personalityAgentID == "" {
			return nil, false, errors.New("SUMI_AUTH_TENANT_ID, SUMI_AUTH_USER_ID, and SUMI_AUTH_PERSONALITY_AGENT_ID are required when Firebase auth is enabled")
		}
		resolver, err := agentevents.NewStaticIdentityBindingResolverForTenant(
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
		bindings = resolver
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
	var providerClient firebaseProviderUserClient = client
	if firebaseTenantID != "" {
		tenantClient, err := client.TenantManager.AuthForTenant(firebaseTenantID)
		if err != nil {
			return nil, false, fmt.Errorf("initialize Firebase tenant Auth client: %w", err)
		}
		verifierClient = tenantClient
		providerClient = tenantClient
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
	if kosekiMode {
		server.Flows = newKosekiAuthFlowController(
			koseki.New(pool),
			strings.TrimSpace(os.Getenv("SUMI_AUTH_TENANT_ID")),
			&firebaseAdminProviderLifecycle{client: providerClient},
		)
	}
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
