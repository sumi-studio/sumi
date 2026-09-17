// e2e-registration-seed is a test-only fixture for the secretary-transfer
// registration browser journey in apps/web/e2e. It is never wired into
// cmd/server. It has two modes:
//
//	e2e-registration-seed invite     issue an enrollment invitation on the
//	                                 Cloud control-plane DB; prints the raw token
//	e2e-registration-seed secretary  create a persona with one carried input on
//	                                 the Local placement DB; prints the persona id
//	e2e-registration-seed verify <persona>   assert the carried persona is
//	                                 bound to exactly one Human on the Cloud DB
//	e2e-registration-seed verify-local <persona>  assert Local authority is
//	                                 transferred (not active) on the Local DB
//	e2e-registration-seed resolve-synthetic <flow_id>
//	                                 mark an owned pending sign-up flow as
//	                                 confirmation_required/create_account with a
//	                                 synthetic credential. ONLY the external-IdP
//	                                 verification step is synthesized: the flow's
//	                                 nonce, enrollment invite binding, and browser
//	                                 epoch were created by the real POST /auth/flows,
//	                                 and invite consumption still runs for real at
//	                                 confirm. The synthetic firebase uid is prefixed
//	                                 "synthetic-" and can never collide with a real
//	                                 account credential.
//
//	SUMI_E2E_REG_SEED_DATABASE_URL  postgres://... (already migrated)
//	SUMI_E2E_REG_SEED_EMAIL         invite recipient (invite mode); for
//	                              resolve-synthetic, the verified email when the
//	                              bound invite carries none
//	SUMI_E2E_REG_SEED_DISPLAY_NAME  optional persona label (secretary mode) /
//	                              verified display name (resolve-synthetic)
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) < 1 || len(args) > 2 {
		return errors.New("usage: e2e-registration-seed <invite|secretary|verify|verify-local> [persona]")
	}
	env := func(name string) string { return strings.TrimSpace(os.Getenv(name)) }
	databaseURL := env("SUMI_E2E_REG_SEED_DATABASE_URL")
	if databaseURL == "" {
		return errors.New("SUMI_E2E_REG_SEED_DATABASE_URL is required")
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()

	switch args[0] {
	case "invite":
		email := env("SUMI_E2E_REG_SEED_EMAIL")
		if email == "" {
			return errors.New("SUMI_E2E_REG_SEED_EMAIL is required for invite mode")
		}
		issuer := uuid.Must(uuid.NewV7()).String()
		if _, err := pool.Exec(ctx,
			`INSERT INTO humans (human_id, display_name) VALUES ($1, 'e2e registration operator')`, issuer); err != nil {
			return fmt.Errorf("issuer: %w", err)
		}
		store := koseki.New(pool)
		_, token, err := store.IssueEnrollmentInvite(ctx, issuer, email, 24*time.Hour)
		if err != nil {
			return fmt.Errorf("issue invite: %w", err)
		}
		fmt.Println(token)
		return nil

	case "secretary":
		state := agentstate.NewStore(pool)
		personaID := uuid.Must(uuid.NewV7()).String()
		displayName := env("SUMI_E2E_REG_SEED_DISPLAY_NAME")
		if displayName == "" {
			displayName = "Local secretary"
		}
		if _, _, err := state.EnsurePersona(ctx, personaID, nil, displayName); err != nil {
			return fmt.Errorf("ensure persona: %w", err)
		}
		if _, _, err := state.SubmitInput(ctx, &agentstate.Input{
			PersonaID: personaID, InputID: "e2e-carried-input", Kind: "message",
			Payload: map[string]any{"text": "持ち越される記憶"}, ActorKind: "human", ActorID: "owner",
			SourceSurface: "e2e",
		}); err != nil {
			return fmt.Errorf("carried input: %w", err)
		}
		fmt.Println(personaID)
		return nil

	case "resolve-synthetic":
		if len(args) != 2 {
			return errors.New("resolve-synthetic requires a flow id")
		}
		return resolveSynthetic(ctx, pool, args[1], env("SUMI_E2E_REG_SEED_EMAIL"), env("SUMI_E2E_REG_SEED_DISPLAY_NAME"))

	case "verify":
		if len(args) != 2 {
			return errors.New("verify requires a persona id")
		}
		return verifyCloud(ctx, pool, args[1])

	case "verify-local":
		if len(args) != 2 {
			return errors.New("verify-local requires a persona id")
		}
		return verifyLocal(ctx, pool, args[1])

	default:
		return errors.New("usage: e2e-registration-seed <invite|secretary|resolve-synthetic|verify|verify-local> [id]")
	}
}

// resolveSynthetic turns an owned pending sign-up flow into the state a real
// POST /auth/flows/resolve produces after a verified OAuth token:
// confirmation_required + create_account + a verified credential identity.
// It synthesizes ONLY the external-IdP proof — the flow must already exist,
// be pending, be a provider sign-up, and carry a live enrollment invite whose
// email (when set) becomes the verified identity's email so invite
// consumption at confirm still exercises its real check. Everything else —
// nonce authority, browser epoch binding, confirm, invite consumption,
// transfer claim — runs on the unmodified production path.
func resolveSynthetic(ctx context.Context, pool *pgxpool.Pool, flowID, email, displayName string) error {
	var inviteID string
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(enrollment_invite_id::text,'')
		FROM auth_flows
		WHERE flow_id = $1 AND status = 'pending' AND intent = 'sign_up'
		  AND channel = 'provider' AND expires_at > now()`,
		flowID).Scan(&inviteID); err != nil {
		return fmt.Errorf("flow lookup (must be a live pending provider sign-up): %w", err)
	}
	if inviteID == "" {
		return errors.New("flow carries no enrollment invite — refusing to synthesize an uninvited registration")
	}
	var inviteEmail string
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(email,'') FROM enrollment_invites
		WHERE invite_id = $1 AND revoked_at IS NULL AND consumed_at IS NULL`,
		inviteID).Scan(&inviteEmail)
	if err != nil {
		return fmt.Errorf("invite lookup: %w", err)
	}
	verifiedEmail := inviteEmail
	if verifiedEmail == "" {
		verifiedEmail = email
	}
	if verifiedEmail == "" {
		return errors.New("invite carries no email; pass SUMI_E2E_REG_SEED_EMAIL for the verified address")
	}
	if email != "" && inviteEmail != "" && email != inviteEmail {
		return fmt.Errorf("given email %q does not match the bound invite email %q", email, inviteEmail)
	}
	if displayName == "" {
		displayName = "Synthetic Registrant"
	}
	firebaseUID := "synthetic-" + uuid.Must(uuid.NewV7()).String()
	tag, err := pool.Exec(ctx, `
		UPDATE auth_flows SET status='confirmation_required',
			confirmation_action='create_account', firebase_uid=$2,
			provider_subject=$3, verified_display_name=$4,
			verified_email=$5, email_verified=true, proved_at=now()
		WHERE flow_id=$1 AND status='pending'`,
		flowID, firebaseUID, "synthetic-"+uuid.Must(uuid.NewV7()).String(),
		displayName, verifiedEmail)
	if err != nil {
		return fmt.Errorf("resolve synthetic: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("flow changed underfoot — not pending anymore")
	}
	fmt.Println(firebaseUID)
	return nil
}

// verifyCloud asserts the transfer claimed the carried persona: exactly one
// Human, the persona bound to it and active, the carried input present, and
// the transfer session activated.
func verifyCloud(ctx context.Context, pool *pgxpool.Pool, personaID string) error {
	// The seed issues invites from an operator Human row; only the registrant
	// Human carries a credential.
	var humans int
	if err := pool.QueryRow(ctx,
		`SELECT count(DISTINCT human_id) FROM credentials`).Scan(&humans); err != nil {
		return fmt.Errorf("count credential humans: %w", err)
	}
	if humans != 1 {
		return fmt.Errorf("expected exactly one credentialed human, found %d", humans)
	}
	var authority, humanID string
	err := pool.QueryRow(ctx,
		`SELECT authority, coalesce(human_id::text, '') FROM core_personas WHERE persona_id = $1`,
		personaID).Scan(&authority, &humanID)
	if err != nil {
		return fmt.Errorf("persona lookup: %w", err)
	}
	if authority != "active" {
		return fmt.Errorf("cloud persona authority %q, want active", authority)
	}
	var boundHuman string
	if err := pool.QueryRow(ctx,
		`SELECT DISTINCT human_id::text FROM credentials`).Scan(&boundHuman); err != nil {
		return fmt.Errorf("human lookup: %w", err)
	}
	if humanID != boundHuman {
		return fmt.Errorf("persona bound to %q, want registered human %q", humanID, boundHuman)
	}
	var carried bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM core_inputs WHERE persona_id = $1 AND input_id = 'e2e-carried-input')`,
		personaID).Scan(&carried); err != nil {
		return fmt.Errorf("carried input: %w", err)
	}
	if !carried {
		return errors.New("carried input not present on cloud")
	}
	var activated int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM transfer_sessions WHERE status = 'activated'`).Scan(&activated); err != nil {
		return fmt.Errorf("transfer sessions: %w", err)
	}
	if activated != 1 {
		return fmt.Errorf("expected one activated transfer session, found %d", activated)
	}
	return nil
}

// verifyLocal asserts the source placement gave up authority for the persona.
func verifyLocal(ctx context.Context, pool *pgxpool.Pool, personaID string) error {
	var authority string
	if err := pool.QueryRow(ctx,
		`SELECT authority FROM core_personas WHERE persona_id = $1`,
		personaID).Scan(&authority); err != nil {
		return fmt.Errorf("local persona lookup: %w", err)
	}
	if authority != "transferred" {
		return fmt.Errorf("local persona authority %q, want transferred", authority)
	}
	return nil
}
