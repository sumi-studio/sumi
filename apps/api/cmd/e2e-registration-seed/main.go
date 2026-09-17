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
//	                                 "synthetic-" as a marker; it is not a
//	                                 namespace guarantee against real credentials.
//	                                 Ownership is proven by the flow's nonce —
//	                                 the same authority credential resolve,
//	                                 confirm, and status already require —
//	                                 supplied via SUMI_E2E_REG_SEED_NONCE_FILE
//	                                 and never printed, plus the expected
//	                                 verified email which must match the bound
//	                                 invite's email.
//
//	SUMI_E2E_REG_SEED_DATABASE_URL  postgres://... (already migrated)
//	SUMI_E2E_REG_SEED_EMAIL         invite recipient (invite mode); for
//	                              resolve-synthetic, the required expected
//	                              verified email
//	SUMI_E2E_REG_SEED_NONCE_FILE    resolve-synthetic only: path to a file
//	                              containing the flow's base64url nonce (the
//	                              recovery credential persisted at flow-start)
//	SUMI_E2E_REG_SEED_DISPLAY_NAME  optional persona label (secretary mode) /
//	                              verified display name (resolve-synthetic)
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
		return errors.New("usage: e2e-registration-seed <invite|secretary|resolve-synthetic|verify|verify-local> [id]")
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
		nonce, err := readNonceFile(env("SUMI_E2E_REG_SEED_NONCE_FILE"))
		if err != nil {
			return err
		}
		email := env("SUMI_E2E_REG_SEED_EMAIL")
		if email == "" {
			return errors.New("SUMI_E2E_REG_SEED_EMAIL is required for resolve-synthetic — the expected verified identity must be stated explicitly")
		}
		return resolveSynthetic(ctx, pool, args[1], nonce, email, env("SUMI_E2E_REG_SEED_DISPLAY_NAME"))

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

// readNonceFile loads the flow's ownership nonce from a private file. The
// nonce is a credential — it authorizes resolve/confirm/status on the flow —
// so it is never taken from argv, never echoed, and never included in errors.
func readNonceFile(path string) (string, error) {
	if path == "" {
		return "", errors.New("SUMI_E2E_REG_SEED_NONCE_FILE is required for resolve-synthetic — the flow's nonce proves ownership")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read nonce file: %w", err)
	}
	return strings.TrimSpace(string(raw)), nil
}

// ownershipNonceHash derives the stored nonce identity exactly as production
// does (koseki validateNonce: base64url-decode 32 bytes, SHA-256). The nonce
// itself never touches the database.
func ownershipNonceHash(nonce string) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(nonce)
	if err != nil || len(decoded) != 32 {
		return nil, errors.New("nonce file does not contain a valid flow nonce")
	}
	digest := sha256.Sum256(decoded)
	return digest[:], nil
}

// resolveSynthetic turns an owned pending sign-up flow into the state a real
// POST /auth/flows/resolve produces after a verified OAuth token:
// confirmation_required + create_account + a verified credential identity.
// It synthesizes ONLY the external-IdP proof — and only on the exact flow the
// nonce authorizes. The flow must already exist, be pending, be a provider
// sign-up, and carry a live enrollment invite whose email (when set) must
// equal the expected verified email so invite consumption at confirm still
// exercises its real check. Every precondition is evaluated inside one
// transaction under FOR UPDATE, so a concurrent resolve, confirm, invite
// consumption, or revocation is serialized against this mutation. Everything
// else — nonce authority, browser epoch binding, confirm, invite
// consumption, transfer claim — runs on the unmodified production path.
func resolveSynthetic(ctx context.Context, pool *pgxpool.Pool, flowID, nonce, email, displayName string) error {
	nonceHash, err := ownershipNonceHash(nonce)
	if err != nil {
		return err
	}
	expectedEmail, err := koseki.NormalizeEmail(email)
	if err != nil {
		return fmt.Errorf("expected email: %w", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// The nonce hash is part of the row identity — exactly like the
	// production scanAuthFlowForUpdate — so a wrong nonce selects nothing and
	// no row is locked or mutated.
	var status, intent, channel, inviteID string
	var expiresAt time.Time
	err = tx.QueryRow(ctx, `
		SELECT status, intent, channel,
			COALESCE(enrollment_invite_id::text,''), expires_at
		FROM auth_flows
		WHERE flow_id = $1 AND nonce_hash = $2
		FOR UPDATE`,
		flowID, nonceHash).Scan(&status, &intent, &channel, &inviteID, &expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		var exists bool
		if qerr := tx.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM auth_flows WHERE flow_id = $1)`,
			flowID).Scan(&exists); qerr != nil {
			return fmt.Errorf("flow lookup: %w", qerr)
		}
		if !exists {
			return errors.New("no such auth flow")
		}
		return errors.New("the supplied nonce does not authorize this flow — refusing to mutate it")
	}
	if err != nil {
		return fmt.Errorf("flow lookup: %w", err)
	}
	if status != "pending" {
		return fmt.Errorf("flow status is %q — only pending flows may be resolved", status)
	}
	if intent != "sign_up" || channel != "provider" {
		return fmt.Errorf("flow is %s/%s — only pending provider sign-up flows may be resolved", intent, channel)
	}
	var live bool
	if err := tx.QueryRow(ctx, `SELECT $1::timestamptz > clock_timestamp()`, expiresAt).Scan(&live); err != nil {
		return fmt.Errorf("flow expiry check: %w", err)
	}
	if !live {
		return errors.New("flow has expired")
	}
	if inviteID == "" {
		return errors.New("flow carries no enrollment invite — refusing to synthesize an uninvited registration")
	}

	// The bound invite must still be live, checked under the same row lock
	// production uses so a concurrent consume/revoke cannot slip past.
	var inviteEmail string
	var inviteExpires time.Time
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(email,''), expires_at FROM enrollment_invites
		WHERE invite_id = $1 AND revoked_at IS NULL AND consumed_at IS NULL
		FOR UPDATE`,
		inviteID).Scan(&inviteEmail, &inviteExpires)
	if errors.Is(err, pgx.ErrNoRows) {
		return errors.New("the bound invite is consumed or revoked — refusing")
	}
	if err != nil {
		return fmt.Errorf("invite lookup: %w", err)
	}
	if err := tx.QueryRow(ctx, `SELECT $1::timestamptz > clock_timestamp()`, inviteExpires).Scan(&live); err != nil {
		return fmt.Errorf("invite expiry check: %w", err)
	}
	if !live {
		return errors.New("the bound invite has expired — refusing")
	}
	if inviteEmail != "" && inviteEmail != expectedEmail {
		return errors.New("expected email does not match the bound invite's email — refusing")
	}

	if displayName == "" {
		displayName = "Synthetic Registrant"
	}
	firebaseUID := "synthetic-" + uuid.Must(uuid.NewV7()).String()
	tag, err := tx.Exec(ctx, `
		UPDATE auth_flows SET status='confirmation_required',
			confirmation_action='create_account', firebase_uid=$2,
			provider_subject=$3, verified_display_name=$4,
			verified_email=$5, email_verified=true, proved_at=now()
		WHERE flow_id=$1 AND nonce_hash=$6 AND status='pending'`,
		flowID, firebaseUID, "synthetic-"+uuid.Must(uuid.NewV7()).String(),
		displayName, expectedEmail, nonceHash)
	if err != nil {
		return fmt.Errorf("resolve synthetic: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("flow changed underfoot — not pending anymore")
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
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
