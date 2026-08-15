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
	personalityAgentID, err := requiredEnv("SUMI_E2E_SESSION_PERSONALITY_AGENT_ID")
	if err != nil {
		return err
	}
	if err := provisionE2EIdentity(ctx, userID, personalityAgentID); err != nil {
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

// provisionE2EIdentity is an opt-in test fixture seam for browser journeys
// that use the production app control plane. Every database-backed journey
// needs a Human. Direct Chat additionally opts into the Secretary and current
// employment that production Koseki would create before issuing a session.
// App installations remain production UI/API operations exercised by the
// journey.
func provisionE2EIdentity(ctx context.Context, userID, personalityAgentID string) error {
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
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin E2E identity provisioning: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `
			INSERT INTO humans (human_id, display_name)
			VALUES ($1, $2)
			ON CONFLICT (human_id) DO NOTHING`, userID, displayName); err != nil {
		return fmt.Errorf("provision E2E Human: %w", err)
	}
	provisionSecretary := strings.TrimSpace(os.Getenv("SUMI_E2E_SESSION_PROVISION_SECRETARY"))
	if provisionSecretary == "" {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit E2E Human provisioning: %w", err)
		}
		return nil
	}
	if provisionSecretary != "1" {
		return errors.New("SUMI_E2E_SESSION_PROVISION_SECRETARY must be 1 when set")
	}
	if _, err := tx.Exec(ctx, `
				INSERT INTO agents (personality_agent_id, human_id, display_name)
			VALUES ($1, $2, 'Sumi')
			ON CONFLICT (personality_agent_id) DO NOTHING`, personalityAgentID, userID); err != nil {
		return fmt.Errorf("provision E2E PersonalityAgent: %w", err)
	}
	var agentHumanID string
	if err := tx.QueryRow(ctx,
		"SELECT human_id FROM agents WHERE personality_agent_id = $1",
		personalityAgentID,
	).Scan(&agentHumanID); err != nil {
		return fmt.Errorf("verify E2E PersonalityAgent: %w", err)
	}
	if agentHumanID != userID {
		return errors.New("E2E PersonalityAgent belongs to a different Human")
	}
	if _, err := tx.Exec(ctx, `
			INSERT INTO employments (agent_id, employer_type, employer_id)
			SELECT $1::uuidv7, 'human', $2::uuidv7
			WHERE NOT EXISTS (
				SELECT 1 FROM employments
				WHERE agent_id = $1::uuidv7 AND ended_at IS NULL
			)`, personalityAgentID, userID); err != nil {
		return fmt.Errorf("provision E2E employment: %w", err)
	}
	var employerType, employerID string
	if err := tx.QueryRow(ctx, `
			SELECT employer_type, employer_id
			FROM employments
			WHERE agent_id = $1 AND ended_at IS NULL`, personalityAgentID,
	).Scan(&employerType, &employerID); err != nil {
		return fmt.Errorf("verify E2E employment: %w", err)
	}
	if employerType != "human" || employerID != userID {
		return errors.New("E2E PersonalityAgent has a different current Employer")
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit E2E identity provisioning: %w", err)
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
