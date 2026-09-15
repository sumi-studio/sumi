// e2e-seed-member is a test-only provisioning fixture: it inserts a
// PersonalityAgent's workspace_members row directly, standing in for the
// targeted-invitation acceptance that normally happens on the legacy agent's
// local-control lane (which the core-only e2e stack does not run).
// It is not wired into cmd/server.
//
//	SUMI_E2E_SEED_DATABASE_URL  postgres://...
//	SUMI_E2E_SEED_WORKSPACE_ID  uuidv7
//	SUMI_E2E_SEED_MEMBER_KIND   human|personality_agent
//	SUMI_E2E_SEED_MEMBER_ID     uuidv7
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/canonicalid"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	databaseURL := os.Getenv("SUMI_E2E_SEED_DATABASE_URL")
	workspaceID := os.Getenv("SUMI_E2E_SEED_WORKSPACE_ID")
	kind := os.Getenv("SUMI_E2E_SEED_MEMBER_KIND")
	memberID := os.Getenv("SUMI_E2E_SEED_MEMBER_ID")
	if databaseURL == "" || workspaceID == "" || kind == "" || memberID == "" {
		return errors.New("SUMI_E2E_SEED_{DATABASE_URL,WORKSPACE_ID,MEMBER_KIND,MEMBER_ID} are required")
	}
	if kind != "human" && kind != "personality_agent" {
		return errors.New("SUMI_E2E_SEED_MEMBER_KIND must be human or personality_agent")
	}
	if !canonicalid.IsUUIDv7(workspaceID) || !canonicalid.IsUUIDv7(memberID) {
		return errors.New("workspace and member ids must be canonical uuidv7")
	}
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(context.Background())
	_, err = conn.Exec(ctx, `
		INSERT INTO workspace_members
			(workspace_member_id, workspace_id, member_kind, member_id, joined_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING`,
		uuid.Must(uuid.NewV7()).String(), workspaceID, kind, memberID, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("insert membership: %w", err)
	}
	return nil
}
