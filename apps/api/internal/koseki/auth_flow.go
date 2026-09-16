package koseki

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sumi-studio/sumi/apps/api/internal/transfersession"
)

type AuthIntent string

const (
	IntentSignIn AuthIntent = "sign_in"
	IntentSignUp AuthIntent = "sign_up"

	ChannelEmailLink = "email_link"
	ChannelProvider  = "provider"

	ActionCreateAccount = "create_account"
	ActionSignIn        = "sign_in"

	OutcomeSignedIn       = "signed_in"
	OutcomeAccountCreated = "account_created"
)

const (
	MinFlowTTL = time.Minute
	MaxFlowTTL = 30 * time.Minute
)

var (
	ErrInvalidAuthFlow          = errors.New("invalid authentication flow")
	ErrAuthFlowExpired          = errors.New("authentication flow expired")
	ErrAuthFlowConsumed         = errors.New("authentication flow already consumed")
	ErrAuthProofMismatch        = errors.New("verified identity does not match authentication flow")
	ErrConfirmation             = errors.New("invalid authentication confirmation")
	ErrCredentialInactive       = errors.New("credential login method is inactive")
	ErrLastLoginMethod          = errors.New("last usable login method cannot be removed")
	ErrRecentReauth             = errors.New("recent reauthentication through another method is required")
	ErrProviderOperationPending = errors.New("another provider operation is pending")
)

type StartAuthFlowRequest struct {
	InviteToken      string
	Intent           AuthIntent
	Channel          string
	ExpectedProvider string
	NormalizedEmail  string
	Continuation     string
	Nonce            string
	// BrowserEpochHash binds the flow to the browser jar that started it, so
	// that jar's logout can cancel its issuance authority.
	BrowserEpochHash string
	TTL              time.Duration
}

type AuthFlow struct {
	EnrollmentInviteID      string
	VerifiedEmail           string
	EmailVerified           bool
	FlowID                  string
	Intent                  AuthIntent
	Channel                 string
	ExpectedProvider        string
	NormalizedEmail         string
	Continuation            string
	Status                  string
	ConfirmationAction      string
	TerminalOutcome         string
	HumanID                 string
	AgentID                 string
	VerifiedProviderSubject string
	VerifiedDisplayName     string
	ExpiresAt               time.Time
	CompletedAt             *time.Time
	EmailProofUID           string
	EmailProofUIDBoundAt    *time.Time
	// BrowserEpochHash is the hashed browser epoch cookie of the jar that may
	// complete this flow. ClosedAt mirrors the durable session store's
	// closed-flow barrier: once set, the flow can never issue a session again.
	BrowserEpochHash string
	ClosedAt         *time.Time
}

type VerifiedIdentity struct {
	FirebaseUID     string
	NormalizedEmail string
	EmailVerified   bool
	SignInProvider  string
	ProviderSubject string
	DisplayName     string
	IssuedAt        time.Time
}

// emailProofTokenSkew tolerates clock difference between Postgres and the
// Firebase token service when checking that a custom sign-in followed the
// recorded mailbox proof.
const emailProofTokenSkew = time.Minute

// NormalizeEmail canonicalizes the email value used only to bind a magic-link
// flow to its proof. It is never used to locate or merge Humans.
func NormalizeEmail(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 3 || len(raw) > 320 || strings.Count(raw, "@") != 1 {
		return "", errors.New("invalid email")
	}
	for _, r := range raw {
		if r <= 0x20 || r >= 0x7f {
			return "", errors.New("email must use printable ASCII")
		}
	}
	parts := strings.SplitN(raw, "@", 2)
	if parts[0] == "" || parts[1] == "" || strings.HasPrefix(parts[1], ".") ||
		strings.HasSuffix(parts[1], ".") || !strings.Contains(parts[1], ".") {
		return "", errors.New("invalid email")
	}
	return strings.ToLower(raw), nil
}

// validBrowserEpochHash matches the agentevents browser-epoch cookie hash:
// base64url SHA-256. Keeping the check local avoids a package cycle.
func validBrowserEpochHash(hash string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(hash)
	return err == nil && len(decoded) == sha256.Size &&
		base64.RawURLEncoding.EncodeToString(decoded) == hash
}

func validateNonce(raw string) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil || len(decoded) != 32 {
		return nil, ErrInvalidAuthFlow
	}
	digest := sha256.Sum256(decoded)
	return digest[:], nil
}

func validateContinuation(raw string) error {
	if len(raw) == 0 || len(raw) > 2048 {
		return ErrInvalidAuthFlow
	}
	u, err := url.Parse(raw)
	if err != nil || u.IsAbs() || u.Host != "" || !strings.HasPrefix(u.Path, "/") || strings.HasPrefix(u.Path, "//") {
		return ErrInvalidAuthFlow
	}
	return nil
}

type preparedAuthFlowStart struct {
	nonceHash []byte
	inviteID  string
}

type authFlowQueryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// StartAuthFlow starts provider and legacy email-link flows. Email-code flows
// must use StartEmailCodeFlow so the challenge and send intent commit together.
func (s *Store) StartAuthFlow(ctx context.Context, request StartAuthFlowRequest) (AuthFlow, error) {
	if request.Channel == ChannelEmailCode {
		return AuthFlow{}, ErrInvalidAuthFlow
	}
	prepared, err := s.prepareAuthFlowStart(ctx, request)
	if err != nil {
		return AuthFlow{}, err
	}
	return insertAuthFlow(ctx, s.pool, request, prepared)
}

func (s *Store) prepareAuthFlowStart(ctx context.Context, request StartAuthFlowRequest) (preparedAuthFlowStart, error) {
	if request.Intent != IntentSignIn && request.Intent != IntentSignUp {
		return preparedAuthFlowStart{}, ErrInvalidAuthFlow
	}
	if request.Channel != ChannelEmailLink && request.Channel != ChannelEmailCode && request.Channel != ChannelProvider {
		return preparedAuthFlowStart{}, ErrInvalidAuthFlow
	}
	if request.TTL < MinFlowTTL || request.TTL > MaxFlowTTL || validateContinuation(request.Continuation) != nil {
		return preparedAuthFlowStart{}, ErrInvalidAuthFlow
	}
	switch request.Channel {
	case ChannelEmailLink:
		if request.ExpectedProvider != "password" || request.NormalizedEmail == "" {
			return preparedAuthFlowStart{}, ErrInvalidAuthFlow
		}
	case ChannelEmailCode:
		if request.ExpectedProvider != EmailCodeSignInProvider || request.NormalizedEmail == "" {
			return preparedAuthFlowStart{}, ErrInvalidAuthFlow
		}
	default:
		if (request.ExpectedProvider != "google.com" && request.ExpectedProvider != "github.com") || request.NormalizedEmail != "" {
			return preparedAuthFlowStart{}, ErrInvalidAuthFlow
		}
	}
	nonceHash, err := validateNonce(request.Nonce)
	if err != nil {
		return preparedAuthFlowStart{}, err
	}
	if request.BrowserEpochHash != "" && !validBrowserEpochHash(request.BrowserEpochHash) {
		return preparedAuthFlowStart{}, ErrInvalidAuthFlow
	}
	inviteID := ""
	if request.InviteToken != "" {
		hash, err := enrollmentTokenHash(request.InviteToken)
		if err != nil {
			return preparedAuthFlowStart{}, err
		}
		// A retry of an existing flow remains recoverable after its invitation was
		// consumed. The nonce and invitation must still identify that exact flow.
		err = s.pool.QueryRow(ctx, `SELECT i.invite_id FROM enrollment_invites i WHERE token_hash=$1 AND (
   (revoked_at IS NULL AND consumed_at IS NULL AND expires_at>now()) OR
   EXISTS(SELECT 1 FROM auth_flows f WHERE f.nonce_hash=$2 AND f.enrollment_invite_id=i.invite_id))`, hash, nonceHash).Scan(&inviteID)
		if errors.Is(err, pgx.ErrNoRows) {
			return preparedAuthFlowStart{}, ErrEnrollmentInvite
		}
		if err != nil {
			return preparedAuthFlowStart{}, err
		}
	} else if request.Intent == IntentSignUp {
		return preparedAuthFlowStart{}, ErrEnrollmentInvite
	}
	return preparedAuthFlowStart{nonceHash: nonceHash, inviteID: inviteID}, nil
}

func insertAuthFlow(ctx context.Context, q authFlowQueryRower, request StartAuthFlowRequest, prepared preparedAuthFlowStart) (AuthFlow, error) {
	nonceHash, inviteID := prepared.nonceHash, prepared.inviteID
	flowID := newUUIDv7()
	expiresAt := time.Now().UTC().Add(request.TTL)
	var result AuthFlow
	err := q.QueryRow(ctx, `
		INSERT INTO auth_flows
			(flow_id, nonce_hash, intent, channel, expected_provider, normalized_email, continuation, expires_at, enrollment_invite_id, browser_epoch_hash)
		VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7, $8, NULLIF($9,'')::uuid, NULLIF($10,''))
		ON CONFLICT (nonce_hash) DO UPDATE SET nonce_hash = auth_flows.nonce_hash
		RETURNING flow_id, intent, channel, expected_provider, COALESCE(normalized_email, ''),
			continuation, status, COALESCE(confirmation_action, ''),
			COALESCE(terminal_outcome, ''), COALESCE(human_id::text, ''),
			COALESCE(personality_agent_id::text, ''), expires_at, COALESCE(enrollment_invite_id::text,''),
			COALESCE(browser_epoch_hash, '')`,
		flowID, nonceHash, request.Intent, request.Channel, request.ExpectedProvider,
		request.NormalizedEmail, request.Continuation, expiresAt, inviteID, request.BrowserEpochHash,
	).Scan(&result.FlowID, &result.Intent, &result.Channel, &result.ExpectedProvider,
		&result.NormalizedEmail, &result.Continuation, &result.Status,
		&result.ConfirmationAction, &result.TerminalOutcome, &result.HumanID,
		&result.AgentID, &result.ExpiresAt, &result.EnrollmentInviteID,
		&result.BrowserEpochHash)
	if err != nil {
		return AuthFlow{}, fmt.Errorf("start auth flow: %w", err)
	}
	// A nonce is the idempotency identity. Reusing it with changed semantics is
	// rejected instead of accidentally continuing a different flow.
	if result.EnrollmentInviteID != inviteID || result.Intent != request.Intent || result.Channel != request.Channel ||
		result.ExpectedProvider != request.ExpectedProvider ||
		result.NormalizedEmail != request.NormalizedEmail || result.Continuation != request.Continuation {
		return AuthFlow{}, ErrInvalidAuthFlow
	}
	return result, nil
}

func (s *Store) ResolveAuthProof(ctx context.Context, flowID, nonce string, identity VerifiedIdentity) (AuthFlow, error) {
	return s.advanceAuthFlow(ctx, flowID, nonce, identity, "")
}

func (s *Store) ConfirmAuthFlow(ctx context.Context, flowID, nonce, action string) (AuthFlow, error) {
	return s.advanceAuthFlow(ctx, flowID, nonce, VerifiedIdentity{}, action)
}

// AuthFlowStatus recovers the semantic state after an ambiguous network
// result. It never advances the flow or issues a session; a consumed proof
// therefore cannot be replayed as another login.
func (s *Store) AuthFlowStatus(ctx context.Context, flowID, nonce string) (AuthFlow, error) {
	nonceHash, err := validateNonce(nonce)
	if err != nil {
		return AuthFlow{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AuthFlow{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	flow, _, err := scanAuthFlowForUpdate(ctx, tx, flowID, nonceHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return AuthFlow{}, unknownAuthFlowAuthority(ctx, tx, flowID, nonceHash)
	}
	if err != nil {
		return AuthFlow{}, err
	}
	if flow.ClosedAt != nil {
		return AuthFlow{}, ErrAuthFlowConsumed
	}
	if flow.Status != "completed" && !time.Now().UTC().Before(flow.ExpiresAt) {
		return AuthFlow{}, ErrAuthFlowExpired
	}
	if err := tx.Commit(ctx); err != nil {
		return AuthFlow{}, err
	}
	return flow, nil
}

// unknownAuthFlowAuthority distinguishes a nonce whose email flow moved to
// another browser from an unknown flow.
func unknownAuthFlowAuthority(ctx context.Context, tx pgx.Tx, flowID string, nonceHash []byte) error {
	var adopted bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM auth_flows WHERE flow_id=$1 AND adopted_from_nonce_hash=$2)`,
		flowID, nonceHash).Scan(&adopted); err != nil {
		return err
	}
	if adopted {
		return ErrEmailFlowContinuedElsewhere
	}
	return ErrInvalidAuthFlow
}

func (s *Store) advanceAuthFlow(ctx context.Context, flowID, nonce string, identity VerifiedIdentity, action string) (AuthFlow, error) {
	nonceHash, err := validateNonce(nonce)
	if err != nil {
		return AuthFlow{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AuthFlow{}, fmt.Errorf("begin auth flow: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	flow, firebaseUID, err := scanAuthFlowForUpdate(ctx, tx, flowID, nonceHash)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return AuthFlow{}, unknownAuthFlowAuthority(ctx, tx, flowID, nonceHash)
		}
		return AuthFlow{}, err
	}
	if flow.ClosedAt != nil {
		// The session store closed this flow's issuance authority (logout,
		// discard, or an account switch); no proof or replay may proceed.
		return AuthFlow{}, ErrAuthFlowConsumed
	}
	if flow.Status == "completed" {
		// A committed outcome is recoverable by the same authority inside the
		// replay window: session admission — not flow completion — owns the
		// current-Human consent check, so a refused or lost issuance response
		// must be able to return the recorded outcome again without repeating
		// provisioning, invite consumption, or credential binding.
		return replayFlowCompletionTx(ctx, tx, flow, firebaseUID, identity, action)
	}
	if !time.Now().UTC().Before(flow.ExpiresAt) {
		return AuthFlow{}, ErrAuthFlowExpired
	}

	if action == "" {
		if identity.FirebaseUID == "" || len(identity.FirebaseUID) > 128 {
			return AuthFlow{}, ErrAuthProofMismatch
		}
		if flow.Status == "confirmation_required" {
			if firebaseUID != identity.FirebaseUID {
				return AuthFlow{}, ErrAuthProofMismatch
			}
			return flow, tx.Commit(ctx)
		}
		if flow.Channel == ChannelEmailLink {
			if !identity.EmailVerified || identity.SignInProvider != "password" ||
				identity.NormalizedEmail != flow.NormalizedEmail {
				return AuthFlow{}, ErrAuthProofMismatch
			}
		} else if flow.Channel == ChannelEmailCode {
			// Sumi proved the mailbox and bound this UID before minting the
			// custom token. Only that principal's later custom sign-in counts.
			if !emailCodeIdentityMatches(flow, identity) {
				return AuthFlow{}, ErrAuthProofMismatch
			}
			identity.NormalizedEmail, identity.EmailVerified = flow.NormalizedEmail, true
		} else if identity.SignInProvider != flow.ExpectedProvider || identity.ProviderSubject == "" || len(identity.ProviderSubject) > 512 {
			return AuthFlow{}, ErrAuthProofMismatch
		}
		firebaseUID = identity.FirebaseUID
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "8:firebase"+firebaseUID); err != nil {
			return AuthFlow{}, fmt.Errorf("lock Firebase credential: %w", err)
		}
		humanID, agentID, exists, err := resolveHumanTx(ctx, tx, firebaseUID)
		if err != nil {
			return AuthFlow{}, err
		}
		switch {
		case flow.Intent == IntentSignIn && exists:
			if flow.Channel == ChannelProvider {
				if err := syncVerifiedProviderTx(ctx, tx, humanID, flow.ExpectedProvider, identity.ProviderSubject, "provider_sign_in"); err != nil {
					return AuthFlow{}, err
				}
			}
			flow, err = completeExistingFlow(ctx, tx, flow, firebaseUID, humanID, agentID, OutcomeSignedIn, identity.ProviderSubject)
		case flow.Intent == IntentSignUp && !exists && s.Transfers == nil:
			flow, err = s.provisionFromFlow(ctx, tx, flow, identity)
		case !exists && (flow.Intent == IntentSignIn || flow.Intent == IntentSignUp):
			// A new credential never provisions at resolve while the secretary
			// move is offered: registration must present "bring my Local
			// secretary" before any account or persona is created. With the
			// transfer surface off there is no choice to make, so sign-up keeps
			// its direct provision above.
			if err := s.checkEnrollmentInviteProof(ctx, tx, flow, identity); err != nil {
				return AuthFlow{}, err
			}
			flow.ConfirmationAction = ActionCreateAccount
			flow.Status = "confirmation_required"
			flow.VerifiedProviderSubject = identity.ProviderSubject
			flow.VerifiedDisplayName = initialHumanDisplayName(identity.DisplayName)
			_, err = tx.Exec(ctx, `UPDATE auth_flows SET status='confirmation_required',
				confirmation_action=$2, firebase_uid=$3, provider_subject=NULLIF($4,''),
				verified_display_name=NULLIF($5,''), verified_email=NULLIF($6,''), email_verified=$7, proved_at=now() WHERE flow_id=$1`,
				flow.FlowID, flow.ConfirmationAction, firebaseUID, identity.ProviderSubject, flow.VerifiedDisplayName, identity.NormalizedEmail, identity.EmailVerified)
		case flow.Intent == IntentSignUp && exists:
			flow.ConfirmationAction = ActionSignIn
			flow.Status = "confirmation_required"
			flow.HumanID, flow.AgentID = humanID, agentID
			flow.VerifiedProviderSubject = identity.ProviderSubject
			flow.VerifiedDisplayName = initialHumanDisplayName(identity.DisplayName)
			_, err = tx.Exec(ctx, `UPDATE auth_flows SET status='confirmation_required',
				confirmation_action=$2, firebase_uid=$3, human_id=$4,
				personality_agent_id=$5, provider_subject=NULLIF($6,''),
				verified_display_name=NULLIF($7,''), proved_at=now() WHERE flow_id=$1`,
				flow.FlowID, flow.ConfirmationAction, firebaseUID, humanID, agentID,
				identity.ProviderSubject, flow.VerifiedDisplayName)
		}
		if err != nil {
			return AuthFlow{}, err
		}
	} else {
		if flow.Status != "confirmation_required" || action != flow.ConfirmationAction || firebaseUID == "" {
			return AuthFlow{}, ErrConfirmation
		}
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "8:firebase"+firebaseUID); err != nil {
			return AuthFlow{}, fmt.Errorf("lock Firebase credential: %w", err)
		}
		humanID, agentID, exists, err := resolveHumanTx(ctx, tx, firebaseUID)
		if err != nil {
			return AuthFlow{}, err
		}
		if action == ActionCreateAccount {
			if exists {
				return AuthFlow{}, ErrCredentialAlreadyBound
			}
			flow, err = s.provisionFromFlow(ctx, tx, flow, VerifiedIdentity{
				FirebaseUID: firebaseUID, SignInProvider: flow.ExpectedProvider,
				ProviderSubject: flow.VerifiedProviderSubject, DisplayName: flow.VerifiedDisplayName,
				NormalizedEmail: flow.VerifiedEmail, EmailVerified: flow.EmailVerified,
			})
		} else {
			if !exists {
				return AuthFlow{}, ErrAuthProofMismatch
			}
			if flow.Channel == ChannelProvider {
				if err := syncVerifiedProviderTx(ctx, tx, humanID, flow.ExpectedProvider, flow.VerifiedProviderSubject, "provider_sign_in"); err != nil {
					return AuthFlow{}, err
				}
			}
			flow, err = completeExistingFlow(ctx, tx, flow, firebaseUID, humanID, agentID, OutcomeSignedIn, flow.VerifiedProviderSubject)
		}
		if err != nil {
			return AuthFlow{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return AuthFlow{}, fmt.Errorf("commit auth flow: %w", err)
	}
	if flow.TerminalOutcome == OutcomeAccountCreated {
		// A claimed transfer session is provisioned by the commit above and
		// owes activation. Close that obligation now, detached from the
		// request's cancellation — a lost response must not leave the Local
		// source sealed and waiting. Sweep covers a crash before this runs.
		s.reconcileProvisionedTransfer(firebaseUID)
	}
	return flow, nil
}

// reconcileProvisionedTransfer activates the just-committed transfer claim
// for this credential. It is deliberately best-effort and detached from the
// caller's context: the account outcome is already committed, and the
// session sweep retakes anything this misses.
func (s *Store) reconcileProvisionedTransfer(firebaseUID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var sessionID string
	err := s.pool.QueryRow(ctx, `SELECT session_id FROM transfer_sessions
		WHERE claim_provider = $1 AND claim_subject = $2 AND status = 'provisioned'`,
		transfersession.ProviderFirebase, firebaseUID).Scan(&sessionID)
	if err != nil {
		return
	}
	_ = s.transferSessions().Reconcile(ctx, sessionID)
}

func emailCodeIdentityMatches(flow AuthFlow, identity VerifiedIdentity) bool {
	return flow.EmailProofUID != "" && flow.EmailProofUIDBoundAt != nil &&
		identity.SignInProvider == EmailCodeSignInProvider &&
		identity.FirebaseUID == flow.EmailProofUID && !identity.IssuedAt.IsZero() &&
		!identity.IssuedAt.Before(flow.EmailProofUIDBoundAt.Add(-emailProofTokenSkew))
}

// completionActionFor maps a recorded terminal outcome back to the
// confirmation action that committed it, so a repeated confirm can be checked
// for action consistency after confirmation_action was cleared.
func completionActionFor(outcome string) string {
	switch outcome {
	case OutcomeAccountCreated:
		return ActionCreateAccount
	case OutcomeSignedIn:
		return ActionSignIn
	}
	return ""
}

// completionIdentityMatches demands that a resolve replay carry a verified
// token for the exact identity the flow completed with: per channel, the
// recorded provider subject or the bound mailbox principal.
func completionIdentityMatches(flow AuthFlow, identity VerifiedIdentity) bool {
	switch flow.Channel {
	case ChannelEmailCode:
		return emailCodeIdentityMatches(flow, identity)
	case ChannelEmailLink:
		return identity.EmailVerified && identity.SignInProvider == "password" &&
			identity.NormalizedEmail == flow.NormalizedEmail
	default:
		return identity.SignInProvider == flow.ExpectedProvider &&
			flow.VerifiedProviderSubject != "" &&
			identity.ProviderSubject == flow.VerifiedProviderSubject
	}
}

// replayFlowCompletionTx repeats a committed terminal outcome whose issuance
// was refused by session admission or whose response was lost. A resolve
// replay must carry the same proof identity the flow completed with; a
// confirm replay must repeat the action implied by the recorded outcome.
// Both are bounded by the replay window and by the recorded principal still
// resolving to the recorded Human. Nothing is written, so no Human,
// invitation, credential, or flow changes, and a mismatched request or a use
// outside the window stays consumed.
func replayFlowCompletionTx(ctx context.Context, tx pgx.Tx, flow AuthFlow, firebaseUID string, identity VerifiedIdentity, action string) (AuthFlow, error) {
	now, err := dbNow(ctx, tx)
	if err != nil {
		return AuthFlow{}, err
	}
	if !flowCompletionReplayable(flow, now) {
		return AuthFlow{}, ErrAuthFlowConsumed
	}
	if action == "" {
		if !completionIdentityMatches(flow, identity) || firebaseUID != identity.FirebaseUID {
			return AuthFlow{}, ErrAuthProofMismatch
		}
	} else {
		if action != completionActionFor(flow.TerminalOutcome) {
			return AuthFlow{}, ErrAuthFlowConsumed
		}
	}
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "8:firebase"+firebaseUID); err != nil {
		return AuthFlow{}, fmt.Errorf("lock Firebase credential: %w", err)
	}
	humanID, agentID, exists, err := resolveHumanTx(ctx, tx, firebaseUID)
	if err != nil {
		return AuthFlow{}, err
	}
	if !exists || humanID != flow.HumanID || agentID != flow.AgentID {
		return AuthFlow{}, ErrAuthProofMismatch
	}
	return flow, tx.Commit(ctx)
}

func scanAuthFlowForUpdate(ctx context.Context, tx pgx.Tx, flowID string, nonceHash []byte) (AuthFlow, string, error) {
	var flow AuthFlow
	var firebaseUID string
	err := tx.QueryRow(ctx, `SELECT flow_id, intent, channel, expected_provider,
		COALESCE(normalized_email, ''), continuation, status,
		COALESCE(confirmation_action, ''), COALESCE(terminal_outcome, ''),
		COALESCE(human_id::text, ''), COALESCE(personality_agent_id::text, ''),
		expires_at, COALESCE(firebase_uid, ''), COALESCE(provider_subject, ''),
		COALESCE(verified_display_name, ''), COALESCE(enrollment_invite_id::text,''), COALESCE(verified_email,''), email_verified,
		COALESCE(email_proof_uid,''), email_proof_uid_bound_at, completed_at,
		COALESCE(browser_epoch_hash,''), closed_at FROM auth_flows
		WHERE flow_id=$1 AND nonce_hash=$2 FOR UPDATE`, flowID, nonceHash).Scan(
		&flow.FlowID, &flow.Intent, &flow.Channel, &flow.ExpectedProvider,
		&flow.NormalizedEmail, &flow.Continuation, &flow.Status,
		&flow.ConfirmationAction, &flow.TerminalOutcome, &flow.HumanID,
		&flow.AgentID, &flow.ExpiresAt, &firebaseUID, &flow.VerifiedProviderSubject,
		&flow.VerifiedDisplayName, &flow.EnrollmentInviteID, &flow.VerifiedEmail, &flow.EmailVerified,
		&flow.EmailProofUID, &flow.EmailProofUIDBoundAt, &flow.CompletedAt,
		&flow.BrowserEpochHash, &flow.ClosedAt)
	return flow, firebaseUID, err
}

func resolveHumanTx(ctx context.Context, tx pgx.Tx, firebaseUID string) (string, string, bool, error) {
	var humanID, agentID string
	err := tx.QueryRow(ctx, `SELECT c.human_id, a.personality_agent_id
		FROM credentials c JOIN agents a ON a.human_id=c.human_id
		WHERE c.provider='firebase' AND c.external_subject=$1 AND c.active`, firebaseUID).Scan(&humanID, &agentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, fmt.Errorf("resolve verified credential: %w", err)
	}
	return humanID, agentID, true, nil
}

func completeExistingFlow(ctx context.Context, tx pgx.Tx, flow AuthFlow, uid, humanID, agentID, outcome, providerSubject string) (AuthFlow, error) {
	// provider_subject is retained on completion so a terminal-outcome replay
	// can still demand the exact recorded provider identity.
	_, err := tx.Exec(ctx, `UPDATE auth_flows SET status='completed', confirmation_action=NULL,
		firebase_uid=$2, human_id=$3, personality_agent_id=$4, terminal_outcome=$5,
		provider_subject=COALESCE(NULLIF($6,''), provider_subject),
		proved_at=COALESCE(proved_at, now()), completed_at=now() WHERE flow_id=$1`,
		flow.FlowID, uid, humanID, agentID, outcome, providerSubject)
	if err != nil {
		return AuthFlow{}, fmt.Errorf("complete auth flow: %w", err)
	}
	flow.Status, flow.ConfirmationAction, flow.TerminalOutcome = "completed", "", outcome
	flow.HumanID, flow.AgentID = humanID, agentID
	if providerSubject != "" {
		flow.VerifiedProviderSubject = providerSubject
	}
	return flow, nil
}

func (s *Store) provisionFromFlow(ctx context.Context, tx pgx.Tx, flow AuthFlow, identity VerifiedIdentity) (AuthFlow, error) {
	wrappingKeyID, err := validateWrappingKeyID(s.wrappingKeyID)
	if err != nil {
		return AuthFlow{}, fmt.Errorf("configured wrapping key ID: %w", err)
	}
	// A verified credential that chose to bring its Local secretary has an
	// open transfer session under its own subject. Claim it here, in the
	// account transaction, so the account is created with the carried
	// persona id instead of a minted one — or fails, never silently falls
	// back to a second secretary. ErrPending (bundle not arrived) and claim
	// conflicts abort the registration for an explicit answer.
	claim, claimed, err := s.transferSessions().ClaimForSubjectInTx(ctx, tx,
		transfersession.Subject{Provider: transfersession.ProviderFirebase, Subject: identity.FirebaseUID})
	if err != nil {
		return AuthFlow{}, err
	}
	humanID, agentID := newUUIDv7(), newUUIDv7()
	if claimed {
		agentID = claim.PersonaID
	}
	wrappingKey, err := generateWrappingKey()
	if err != nil {
		return AuthFlow{}, err
	}
	displayName := initialHumanDisplayName(identity.DisplayName)
	statements := []struct {
		query string
		args  []any
	}{
		{"INSERT INTO humans (human_id, display_name) VALUES ($1, COALESCE(NULLIF($2, ''), 'Sumi'))", []any{humanID, displayName}},
		{"INSERT INTO agents (personality_agent_id, human_id) VALUES ($1, $2)", []any{agentID, humanID}},
		{"INSERT INTO employments (agent_id, employer_type, employer_id) VALUES ($1, $2, $3)", []any{agentID, EmployerHuman, humanID}},
		{"INSERT INTO agent_secrets (personality_agent_id, wrapping_key_id, wrapping_key) VALUES ($1, $2, $3)", []any{agentID, wrappingKeyID, wrappingKey}},
		{"INSERT INTO credentials (provider, external_subject, human_id) VALUES ('firebase', $1, $2)", []any{identity.FirebaseUID, humanID}},
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement.query, statement.args...); err != nil {
			if isUniqueViolation(err) {
				return AuthFlow{}, ErrCredentialAlreadyBound
			}
			return AuthFlow{}, fmt.Errorf("provision confirmed Human and Secretary: %w", err)
		}
	}
	if err := s.consumeEnrollmentInvite(ctx, tx, flow, identity, humanID); err != nil {
		return AuthFlow{}, err
	}
	if flow.Channel == ChannelProvider {
		if identity.ProviderSubject == "" {
			return AuthFlow{}, ErrAuthProofMismatch
		}
		if _, err := tx.Exec(ctx, "INSERT INTO credentials (provider, external_subject, human_id) VALUES ($1,$2,$3)",
			flow.ExpectedProvider, identity.ProviderSubject, humanID); err != nil {
			if isUniqueViolation(err) {
				return AuthFlow{}, ErrCredentialAlreadyBound
			}
			return AuthFlow{}, fmt.Errorf("bind initial provider credential: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO credential_security_events
			(human_id, provider, event_type, decision_path, terminal_outcome)
			VALUES ($1,$2,'provider_linked','new_account_activation','linked')`, humanID, flow.ExpectedProvider); err != nil {
			return AuthFlow{}, fmt.Errorf("record initial provider link: %w", err)
		}
	}
	if _, err := s.directChatApps.InstallDirectChatForNewHumanInTx(ctx, tx, humanID); err != nil {
		return AuthFlow{}, fmt.Errorf("install initial direct chat: %w", err)
	}
	if claimed {
		// The credential row above must commit before this binds the staged
		// persona; the session's activation obligation is recorded here and
		// Reconcile closes it after commit.
		if err := transfersession.ProvisionInTx(ctx, tx, claim, humanID); err != nil {
			return AuthFlow{}, fmt.Errorf("provision carried secretary: %w", err)
		}
	}
	return completeExistingFlow(ctx, tx, flow, identity.FirebaseUID, humanID, agentID, OutcomeAccountCreated, identity.ProviderSubject)
}

func syncVerifiedProviderTx(ctx context.Context, tx pgx.Tx, humanID, provider, subject, decisionPath string) error {
	if subject == "" || len(subject) > 512 {
		return ErrAuthProofMismatch
	}
	var boundHuman string
	var active bool
	err := tx.QueryRow(ctx, "SELECT human_id, active FROM credentials WHERE provider=$1 AND external_subject=$2 FOR UPDATE", provider, subject).Scan(&boundHuman, &active)
	changed := false
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		_, err = tx.Exec(ctx, "INSERT INTO credentials (provider, external_subject, human_id) VALUES ($1,$2,$3)", provider, subject, humanID)
		changed = true
	case err != nil:
		return err
	case boundHuman != humanID:
		return ErrCredentialAlreadyBound
	case !active:
		_, err = tx.Exec(ctx, "UPDATE credentials SET active=true, unlinked_at=NULL WHERE provider=$1 AND external_subject=$2", provider, subject)
		changed = true
	}
	if err != nil {
		return err
	}
	if changed {
		_, err = tx.Exec(ctx, `INSERT INTO credential_security_events
			(human_id, provider, event_type, decision_path, terminal_outcome)
			VALUES ($1,$2,'provider_linked',$3,'linked')`, humanID, provider, decisionPath)
	}
	return err
}

// RegistrantProofSubject authenticates the credential a live new-account
// registration flow proved, for the secretary-move endpoints that act on the
// registrant's behalf. It returns the verified Firebase UID and the browser
// epoch hash the flow was bound to at start; the caller compares that epoch
// to the request's own jar before trusting the subject.
//
// The flow must still carry account-creation authority: a confirmation
// holding "create_account" for a verified UID, or the completed
// account-creation inside its replay window — the same authority a lost
// confirmation answer recovers from. A consumed, closed, expired or
// unrelated flow proves nothing, and a request-body subject is never read.
func (s *Store) RegistrantProofSubject(ctx context.Context, flowID, nonce string) (firebaseUID, epochHash string, err error) {
	nonceHash, err := validateNonce(nonce)
	if err != nil {
		return "", "", err
	}
	var (
		storedNonce        []byte
		status             string
		confirmationAction string
		terminalOutcome    string
		expiresAt          time.Time
		completedAt        *time.Time
		closedAt           *time.Time
	)
	err = s.pool.QueryRow(ctx, `SELECT nonce_hash, status,
		COALESCE(confirmation_action,''), COALESCE(terminal_outcome,''), COALESCE(firebase_uid,''),
		COALESCE(browser_epoch_hash,''), expires_at, completed_at, closed_at
		FROM auth_flows WHERE flow_id=$1`, flowID).Scan(
		&storedNonce, &status, &confirmationAction, &terminalOutcome, &firebaseUID,
		&epochHash, &expiresAt, &completedAt, &closedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", ErrInvalidAuthFlow
	}
	if err != nil {
		return "", "", err
	}
	if subtle.ConstantTimeCompare(storedNonce, nonceHash) != 1 {
		return "", "", ErrAuthProofMismatch
	}
	if closedAt != nil {
		return "", "", ErrAuthFlowConsumed
	}
	now := time.Now().UTC()
	if status == "completed" {
		flow := AuthFlow{Status: status, TerminalOutcome: terminalOutcome, CompletedAt: completedAt}
		if terminalOutcome == OutcomeAccountCreated && firebaseUID != "" && flowCompletionReplayable(flow, now) {
			return firebaseUID, epochHash, nil
		}
		return "", "", ErrAuthFlowConsumed
	}
	if status != "confirmation_required" || confirmationAction != ActionCreateAccount || firebaseUID == "" {
		return "", "", ErrInvalidAuthFlow
	}
	if !now.Before(expiresAt) {
		return "", "", ErrAuthFlowExpired
	}
	return firebaseUID, epochHash, nil
}

// BrowserFlowRef is the revocation-facing view of one flow: enough to fence
// session issuance without exposing the nonce or proof material.
type BrowserFlowRef struct {
	FlowID    string
	HumanID   string
	EpochHash string
	ExpiresAt time.Time
	ClosedAt  *time.Time
}

// BrowserFlowRefForNonce returns the flow only to its nonce authority, so a
// discard request cancels only a flow the calling browser actually owns.
func (s *Store) BrowserFlowRefForNonce(ctx context.Context, flowID, nonce string) (BrowserFlowRef, error) {
	nonceHash, err := validateNonce(nonce)
	if err != nil {
		return BrowserFlowRef{}, err
	}
	var ref BrowserFlowRef
	var storedNonce []byte
	err = s.pool.QueryRow(ctx, `SELECT flow_id, nonce_hash, COALESCE(human_id::text,''),
		COALESCE(browser_epoch_hash,''), expires_at, closed_at FROM auth_flows WHERE flow_id=$1`, flowID).Scan(
		&ref.FlowID, &storedNonce, &ref.HumanID, &ref.EpochHash, &ref.ExpiresAt, &ref.ClosedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return BrowserFlowRef{}, ErrInvalidAuthFlow
	}
	if err != nil {
		return BrowserFlowRef{}, err
	}
	if subtle.ConstantTimeCompare(storedNonce, nonceHash) != 1 {
		return BrowserFlowRef{}, ErrAuthProofMismatch
	}
	return ref, nil
}

// AuthFlowEpochHash reads the flow's recorded browser epoch for admission.
func (s *Store) AuthFlowEpochHash(ctx context.Context, flowID string) (string, error) {
	var epoch string
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(browser_epoch_hash,'') FROM auth_flows WHERE flow_id=$1`, flowID).Scan(&epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrInvalidAuthFlow
	}
	return epoch, err
}

// OpenBrowserFlows lists a browser epoch's flows that could still issue a
// session. Logout closes all of them so a pending or just-completed flow
// cannot sign the jar back in.
func (s *Store) OpenBrowserFlows(ctx context.Context, epochHash string) ([]BrowserFlowRef, error) {
	rows, err := s.pool.Query(ctx, `SELECT flow_id, COALESCE(human_id::text,''), expires_at, closed_at
		FROM auth_flows WHERE browser_epoch_hash=$1 AND closed_at IS NULL`, epochHash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var refs []BrowserFlowRef
	for rows.Next() {
		var ref BrowserFlowRef
		if err := rows.Scan(&ref.FlowID, &ref.HumanID, &ref.ExpiresAt, &ref.ClosedAt); err != nil {
			return nil, err
		}
		ref.EpochHash = epochHash
		refs = append(refs, ref)
	}
	return refs, rows.Err()
}

// CloseAuthFlows mirrors the session store's closed-flow barrier into
// PostgreSQL so status, proof, and replay paths report the closure. Issuance
// never consults this column; the session store decides. The update is
// idempotent and safe to retry after an ambiguous failure.
func (s *Store) CloseAuthFlows(ctx context.Context, flowIDs []string) error {
	for _, flowID := range flowIDs {
		if _, err := s.pool.Exec(ctx, `UPDATE auth_flows SET closed_at=clock_timestamp()
			WHERE flow_id=$1 AND closed_at IS NULL`, flowID); err != nil {
			return fmt.Errorf("close auth flow %s: %w", flowID, err)
		}
	}
	return nil
}
