package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

const sessionTTL = 15 * time.Minute

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, output io.Writer) error {
	secretBase64, err := requiredEnv("SUMI_BROWSER_SESSION_SECRET")
	if err != nil {
		return err
	}
	secret, err := base64.StdEncoding.DecodeString(secretBase64)
	if err != nil {
		return errors.New("SUMI_BROWSER_SESSION_SECRET must be standard base64")
	}
	issuer, err := agentevents.NewHMACBrowserSessionIssuer(
		secret,
		os.Getenv("SUMI_BROWSER_SESSION_AUDIENCE"),
	)
	if err != nil {
		return fmt.Errorf("construct browser session issuer: %w", err)
	}
	tenantID, err := requiredEnv("SUMI_E2E_SESSION_TENANT_ID")
	if err != nil {
		return err
	}
	userID, err := requiredEnv("SUMI_E2E_SESSION_USER_ID")
	if err != nil {
		return err
	}
	if err := provisionE2EHuman(ctx, userID); err != nil {
		return err
	}
	personalityAgentID, err := requiredEnv("SUMI_E2E_SESSION_PERSONALITY_AGENT_ID")
	if err != nil {
		return err
	}
	session, err := issuer.IssueSession(ctx, agentevents.UserSessionClaims{
		TenantID:           tenantID,
		UserID:             userID,
		PersonalityAgentID: personalityAgentID,
	}, sessionTTL)
	if err != nil {
		return fmt.Errorf("issue browser session: %w", err)
	}
	if _, err := fmt.Fprintln(output, session); err != nil {
		return fmt.Errorf("write browser session: %w", err)
	}
	return nil
}

// provisionE2EHuman is an opt-in test fixture seam for browser journeys that
// use the production Workspace and Messaging control plane. Production auth
// creates the Human through Koseki before issuing a browser session; the
// preissued E2E path must establish the same prerequisite explicitly instead
// of weakening participant validation in the application.
func provisionE2EHuman(ctx context.Context, userID string) error {
	databaseURL := strings.TrimSpace(os.Getenv("SUMI_E2E_SESSION_DATABASE_URL"))
	if databaseURL == "" {
		return nil
	}
	displayName := strings.TrimSpace(os.Getenv("SUMI_E2E_SESSION_DISPLAY_NAME"))
	if displayName == "" {
		displayName = "Workspace E2E Human"
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("connect E2E session database: %w", err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `
		INSERT INTO humans (human_id, display_name)
		VALUES ($1, $2)
		ON CONFLICT (human_id) DO NOTHING`, userID, displayName); err != nil {
		return fmt.Errorf("provision E2E Human: %w", err)
	}
	return nil
}

func requiredEnv(name string) (string, error) {
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}
