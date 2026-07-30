package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

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

func requiredEnv(name string) (string, error) {
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}
