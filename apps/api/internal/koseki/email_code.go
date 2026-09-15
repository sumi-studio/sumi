package koseki

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Email mailbox proof. One challenge email carries a 6-digit code and a link
// token; both are HMAC-derived from challenge_id with the server key, so the
// database never stores either secret. Proving the mailbox does not sign in:
// the controller then resolves the Firebase principal for the address and the
// browser exchanges a Sumi-minted custom token through ResolveAuthProof.
const (
	ChannelEmailCode = "email_code"
	// EmailCodeSignInProvider is the Firebase sign_in_provider claim of an ID
	// token obtained with signInWithCustomToken (verified with the Auth
	// emulator; see implementation handback).
	EmailCodeSignInProvider = "custom"

	EmailChallengeTTL          = 10 * time.Minute
	EmailCodeMaxAttempts       = 5
	EmailCodeReuseMinRemaining = 3 * time.Minute
	EmailResendMinInterval     = 30 * time.Second
	EmailSendHourlyLimit       = 5
	EmailDeliveryMaxAttempts   = 5
	emailChallengeMinLifetime  = time.Minute
	// EmailCompletionReplayWindow bounds how long the flow authority may
	// repeat a completed email sign-in whose response was lost, for example a
	// session cookie that never reached the browser. The replay returns the
	// same terminal result for the same Firebase principal and Human; it never
	// provisions, consumes an invitation, or moves the flow.
	EmailCompletionReplayWindow = 5 * time.Minute
)

var (
	ErrEmailCodeMismatch            = errors.New("email code does not match")
	ErrEmailCodeLocked              = errors.New("email code attempts exhausted")
	ErrEmailChallengeSuperseded     = errors.New("email challenge was replaced by a newer email")
	ErrEmailChallengeExpired        = errors.New("email challenge expired")
	ErrEmailChallengeConsumed       = errors.New("email challenge already used")
	ErrEmailLinkInvalid             = errors.New("email link is invalid")
	ErrEmailLinkAdoptionRequired    = errors.New("email link belongs to another browser")
	ErrEmailFlowContinuedElsewhere  = errors.New("email flow continued in another browser")
	ErrEmailChallengeUnavailable    = errors.New("email challenge key is unavailable")
	errEmailDeliveryLeaseSuperseded = errors.New("email delivery lease was superseded")
)

// EmailSendLimitedError reports an honest retry time for address or resend
// pacing limits. Already delivered live challenges remain usable.
type EmailSendLimitedError struct {
	RetryAt time.Time
}

func (e *EmailSendLimitedError) Error() string { return "email send limit reached" }

// EmailCodeMismatchError carries the remaining attempts of the live challenge.
type EmailCodeMismatchError struct {
	AttemptsRemaining int
}

func (e *EmailCodeMismatchError) Error() string { return ErrEmailCodeMismatch.Error() }
func (e *EmailCodeMismatchError) Unwrap() error { return ErrEmailCodeMismatch }

// EmailChallengeKey is the server-held derivation key. Rotating KeyID makes
// live challenges under the previous key expire rather than verify.
type EmailChallengeKey struct {
	KeyID string
	Key   []byte
}

func (k *EmailChallengeKey) valid() bool {
	return k != nil && len(k.Key) >= 32 && k.KeyID != "" && len(k.KeyID) <= 64
}

func (k *EmailChallengeKey) mac(label, challengeID string) []byte {
	mac := hmac.New(sha256.New, k.Key)
	mac.Write([]byte("sumi-auth-email/v1/" + label + "/" + k.KeyID + "/" + challengeID))
	return mac.Sum(nil)
}

// EmailChallengeSecrets derives the emailed code and link token.
func (k *EmailChallengeKey) EmailChallengeSecrets(challengeID string) (code, linkToken string, err error) {
	if !k.valid() {
		return "", "", ErrEmailChallengeUnavailable
	}
	codeMAC := k.mac("code", challengeID)
	code = fmt.Sprintf("%06d", binary.BigEndian.Uint64(codeMAC[:8])%1_000_000)
	linkToken = base64.RawURLEncoding.EncodeToString(k.mac("link", challengeID))
	return code, linkToken, nil
}

// NormalizeEmailCode accepts pasted codes with spaces or hyphens and
// full-width digits. Anything else is not a guess and is not counted.
func NormalizeEmailCode(raw string) (string, bool) {
	var b strings.Builder
	for _, r := range raw {
		switch {
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r >= '０' && r <= '９':
			b.WriteRune('0' + (r - '０'))
		case r == ' ' || r == '-' || r == '　' || r == '\t':
		default:
			return "", false
		}
	}
	code := b.String()
	return code, len(code) == 6
}

type EmailChallengeState struct {
	ChallengeID       string
	ChallengeExpires  time.Time
	AttemptsRemaining int
	DeliveryStatus    string
	DeliveryCreatedAt time.Time
	ResendAvailableAt time.Time
}

type EmailProof struct {
	FlowExpiresAt   time.Time
	FlowID          string
	Intent          AuthIntent
	NormalizedEmail string
	Method          string
	ProvedAt        time.Time
	UID             string
	UIDBoundAt      time.Time
	Status          string
}

type EmailLinkInspection struct {
	FlowID          string
	Intent          AuthIntent
	NormalizedEmail string
	// State is usable, proved_here, consumed, superseded, expired, or
	// completed.
	State       string
	SameBrowser bool
	ExpiresAt   time.Time
}

type emailFlowRow struct {
	flow          AuthFlow
	nonceHash     []byte
	provedAt      *time.Time
	proofMethod   string
	proofUID      string
	proofUIDBound *time.Time
}

func scanEmailFlowForUpdate(ctx context.Context, tx pgx.Tx, where string, args ...any) (emailFlowRow, error) {
	var row emailFlowRow
	err := tx.QueryRow(ctx, `SELECT flow_id, intent, channel, COALESCE(normalized_email,''), status,
		COALESCE(terminal_outcome,''), expires_at, completed_at, nonce_hash, email_proved_at, COALESCE(email_proof_method,''),
		COALESCE(email_proof_uid,''), email_proof_uid_bound_at
		FROM auth_flows WHERE `+where+` FOR UPDATE`, args...).Scan(
		&row.flow.FlowID, &row.flow.Intent, &row.flow.Channel, &row.flow.NormalizedEmail,
		&row.flow.Status, &row.flow.TerminalOutcome, &row.flow.ExpiresAt, &row.flow.CompletedAt, &row.nonceHash, &row.provedAt, &row.proofMethod,
		&row.proofUID, &row.proofUIDBound)
	return row, err
}

// emailProofRetryError decides whether a proved flow may hand its proof to
// the same authority again. Before completion that holds only until the flow
// expires, so an abandoned proof cannot keep minting custom tokens. After
// completion it holds only inside the completion replay window.
func emailProofRetryError(flow AuthFlow, now time.Time) error {
	if flow.Status == "completed" {
		if emailCompletionReplayable(flow, now) {
			return nil
		}
		return ErrAuthFlowConsumed
	}
	if !now.Before(flow.ExpiresAt) {
		return ErrAuthFlowExpired
	}
	return nil
}

func emailCompletionReplayable(flow AuthFlow, now time.Time) bool {
	return flow.Channel == ChannelEmailCode && flow.Status == "completed" && flow.CompletedAt != nil &&
		(flow.TerminalOutcome == OutcomeSignedIn || flow.TerminalOutcome == OutcomeAccountCreated) &&
		now.Before(flow.CompletedAt.Add(EmailCompletionReplayWindow))
}

func (r emailFlowRow) proof() EmailProof {
	proof := EmailProof{
		FlowID: r.flow.FlowID, Intent: r.flow.Intent, NormalizedEmail: r.flow.NormalizedEmail,
		Method: r.proofMethod, UID: r.proofUID, Status: r.flow.Status, FlowExpiresAt: r.flow.ExpiresAt,
	}
	if r.provedAt != nil {
		proof.ProvedAt = *r.provedAt
	}
	if r.proofUIDBound != nil {
		proof.UIDBoundAt = *r.proofUIDBound
	}
	return proof
}

// lockEmailFlowByNonce distinguishes an unknown flow from one whose authority
// moved to another browser.
func lockEmailFlowByNonce(ctx context.Context, tx pgx.Tx, flowID string, nonceHash []byte) (emailFlowRow, error) {
	row, err := scanEmailFlowForUpdate(ctx, tx, "flow_id=$1 AND nonce_hash=$2", flowID, nonceHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return emailFlowRow{}, unknownAuthFlowAuthority(ctx, tx, flowID, nonceHash)
	}
	if err != nil {
		return emailFlowRow{}, err
	}
	if row.flow.Channel != ChannelEmailCode {
		return emailFlowRow{}, ErrInvalidAuthFlow
	}
	return row, nil
}

func dbNow(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	var now time.Time
	err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now)
	return now, err
}

func lockEmailAddress(ctx context.Context, tx pgx.Tx, email string) error {
	_, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "9:auth-email:"+email)
	return err
}

// checkEmailSendLimit must run while holding the address lock. It admits at
// most EmailSendHourlyLimit send intents (starts and resends, whether or not
// the mail was delivered) per address in any rolling hour, so the longest
// pause is one hour. Challenges already used for a successful proof do not
// count, so ordinary repeated sign-ins do not consume the budget.
func checkEmailSendLimit(ctx context.Context, tx pgx.Tx, email string, now time.Time) error {
	var admitted int
	var oldest *time.Time
	err := tx.QueryRow(ctx, `SELECT count(*), min(d.created_at)
		FROM auth_email_deliveries d JOIN auth_email_challenges c USING (challenge_id)
		WHERE d.normalized_email=$1 AND d.created_at > $2::timestamptz - interval '1 hour'
			AND c.consumed_at IS NULL`, email, now).Scan(&admitted, &oldest)
	if err != nil {
		return fmt.Errorf("read email send budget: %w", err)
	}
	if admitted >= EmailSendHourlyLimit && oldest != nil {
		return &EmailSendLimitedError{RetryAt: oldest.Add(time.Hour)}
	}
	return nil
}

func insertEmailChallenge(ctx context.Context, tx pgx.Tx, flow AuthFlow, keyID string, now time.Time) (string, time.Time, error) {
	expiresAt := now.Add(EmailChallengeTTL)
	if flow.ExpiresAt.Before(expiresAt) {
		expiresAt = flow.ExpiresAt
	}
	if expiresAt.Sub(now) < emailChallengeMinLifetime {
		return "", time.Time{}, ErrAuthFlowExpired
	}
	challengeID := newUUIDv7()
	if _, err := tx.Exec(ctx, `INSERT INTO auth_email_challenges
		(challenge_id, flow_id, normalized_email, key_id, created_at, expires_at)
		VALUES ($1,$2,$3,$4,$5,$6)`, challengeID, flow.FlowID, flow.NormalizedEmail, keyID, now, expiresAt); err != nil {
		return "", time.Time{}, fmt.Errorf("insert email challenge: %w", err)
	}
	return challengeID, expiresAt, nil
}

func insertEmailDelivery(ctx context.Context, tx pgx.Tx, challengeID, email string, now time.Time) error {
	_, err := tx.Exec(ctx, `INSERT INTO auth_email_deliveries
		(delivery_id, challenge_id, normalized_email, created_at, next_attempt_at)
		VALUES ($1,$2,$3,$4,$4)`, newUUIDv7(), challengeID, email, now)
	if err != nil {
		return fmt.Errorf("insert email delivery: %w", err)
	}
	return nil
}

// StartEmailCodeFlow persists the flow, its first challenge, and the durable
// send intent in one transaction. Retrying the same nonce returns the existing
// flow without another email.
func (s *Store) StartEmailCodeFlow(ctx context.Context, request StartAuthFlowRequest) (AuthFlow, EmailChallengeState, error) {
	if !s.EmailChallengeKey.valid() {
		return AuthFlow{}, EmailChallengeState{}, ErrEmailChallengeUnavailable
	}
	if request.Channel != ChannelEmailCode || request.ExpectedProvider != EmailCodeSignInProvider {
		return AuthFlow{}, EmailChallengeState{}, ErrInvalidAuthFlow
	}
	prepared, err := s.prepareAuthFlowStart(ctx, request)
	if err != nil {
		return AuthFlow{}, EmailChallengeState{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AuthFlow{}, EmailChallengeState{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockEmailAddress(ctx, tx, request.NormalizedEmail); err != nil {
		return AuthFlow{}, EmailChallengeState{}, err
	}
	flow, err := insertAuthFlow(ctx, tx, request, prepared)
	if err != nil {
		return AuthFlow{}, EmailChallengeState{}, err
	}
	var existing bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM auth_email_challenges WHERE flow_id=$1)`, flow.FlowID).Scan(&existing); err != nil {
		return AuthFlow{}, EmailChallengeState{}, err
	}
	if !existing {
		now, err := dbNow(ctx, tx)
		if err != nil {
			return AuthFlow{}, EmailChallengeState{}, err
		}
		if err := checkEmailSendLimit(ctx, tx, flow.NormalizedEmail, now); err != nil {
			return AuthFlow{}, EmailChallengeState{}, err
		}
		challengeID, _, err := insertEmailChallenge(ctx, tx, flow, s.EmailChallengeKey.KeyID, now)
		if err != nil {
			return AuthFlow{}, EmailChallengeState{}, err
		}
		if err := insertEmailDelivery(ctx, tx, challengeID, flow.NormalizedEmail, now); err != nil {
			return AuthFlow{}, EmailChallengeState{}, err
		}
	}
	state, err := emailChallengeStateTx(ctx, tx, flow.FlowID)
	if err != nil {
		return AuthFlow{}, EmailChallengeState{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return AuthFlow{}, EmailChallengeState{}, fmt.Errorf("commit email flow start: %w", err)
	}
	return flow, state, nil
}

func emailChallengeStateTx(ctx context.Context, tx pgx.Tx, flowID string) (EmailChallengeState, error) {
	var state EmailChallengeState
	var attempts int
	var deliveryStatus *string
	var deliveryCreated *time.Time
	err := tx.QueryRow(ctx, `SELECT c.challenge_id, c.expires_at, c.failed_code_attempts, d.status, d.created_at
		FROM auth_email_challenges c
		LEFT JOIN LATERAL (SELECT status, created_at FROM auth_email_deliveries
			WHERE challenge_id=c.challenge_id ORDER BY created_at DESC LIMIT 1) d ON true
		WHERE c.flow_id=$1 ORDER BY c.created_at DESC LIMIT 1`, flowID).Scan(
		&state.ChallengeID, &state.ChallengeExpires, &attempts, &deliveryStatus, &deliveryCreated)
	if errors.Is(err, pgx.ErrNoRows) {
		return EmailChallengeState{}, ErrInvalidAuthFlow
	}
	if err != nil {
		return EmailChallengeState{}, err
	}
	state.AttemptsRemaining = EmailCodeMaxAttempts - attempts
	if deliveryStatus != nil {
		state.DeliveryStatus = *deliveryStatus
		if state.DeliveryStatus == "sending" {
			state.DeliveryStatus = "pending"
		}
	}
	if deliveryCreated != nil {
		state.DeliveryCreatedAt = *deliveryCreated
		state.ResendAvailableAt = deliveryCreated.Add(EmailResendMinInterval)
	}
	return state, nil
}

// EmailFlowStatus is read-only UI state for the flow authority.
func (s *Store) EmailFlowStatus(ctx context.Context, flowID, nonce string) (EmailProof, EmailChallengeState, error) {
	nonceHash, err := validateNonce(nonce)
	if err != nil {
		return EmailProof{}, EmailChallengeState{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return EmailProof{}, EmailChallengeState{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	row, err := lockEmailFlowByNonce(ctx, tx, flowID, nonceHash)
	if err != nil {
		return EmailProof{}, EmailChallengeState{}, err
	}
	state, err := emailChallengeStateTx(ctx, tx, flowID)
	if err != nil {
		return EmailProof{}, EmailChallengeState{}, err
	}
	return row.proof(), state, tx.Commit(ctx)
}

// ResendEmailChallenge queues another email for the flow. The live code is
// reused while it has enough time and attempts left, so out-of-order delivery
// shows the same code; otherwise a new challenge supersedes it.
func (s *Store) ResendEmailChallenge(ctx context.Context, flowID, nonce string) (EmailChallengeState, error) {
	if !s.EmailChallengeKey.valid() {
		return EmailChallengeState{}, ErrEmailChallengeUnavailable
	}
	nonceHash, err := validateNonce(nonce)
	if err != nil {
		return EmailChallengeState{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return EmailChallengeState{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var email string
	if err := tx.QueryRow(ctx, `SELECT COALESCE(normalized_email,'') FROM auth_flows WHERE flow_id=$1`, flowID).Scan(&email); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return EmailChallengeState{}, ErrInvalidAuthFlow
		}
		return EmailChallengeState{}, err
	}
	if err := lockEmailAddress(ctx, tx, email); err != nil {
		return EmailChallengeState{}, err
	}
	row, err := lockEmailFlowByNonce(ctx, tx, flowID, nonceHash)
	if err != nil {
		return EmailChallengeState{}, err
	}
	if row.flow.Status == "completed" {
		return EmailChallengeState{}, ErrAuthFlowConsumed
	}
	if row.provedAt != nil {
		return EmailChallengeState{}, ErrEmailChallengeConsumed
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return EmailChallengeState{}, err
	}
	if !now.Before(row.flow.ExpiresAt) {
		return EmailChallengeState{}, ErrAuthFlowExpired
	}
	var lastDelivery *time.Time
	if err := tx.QueryRow(ctx, `SELECT max(d.created_at) FROM auth_email_deliveries d
		JOIN auth_email_challenges c USING (challenge_id) WHERE c.flow_id=$1`, flowID).Scan(&lastDelivery); err != nil {
		return EmailChallengeState{}, err
	}
	if lastDelivery != nil && now.Before(lastDelivery.Add(EmailResendMinInterval)) {
		return EmailChallengeState{}, &EmailSendLimitedError{RetryAt: lastDelivery.Add(EmailResendMinInterval)}
	}
	if err := checkEmailSendLimit(ctx, tx, email, now); err != nil {
		return EmailChallengeState{}, err
	}
	var liveID, keyID string
	var liveExpires time.Time
	var attempts int
	err = tx.QueryRow(ctx, `SELECT challenge_id, expires_at, failed_code_attempts, key_id FROM auth_email_challenges
		WHERE flow_id=$1 AND superseded_at IS NULL AND consumed_at IS NULL FOR UPDATE`, flowID).Scan(&liveID, &liveExpires, &attempts, &keyID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		liveID = ""
	case err != nil:
		return EmailChallengeState{}, err
	}
	challengeID := liveID
	if liveID == "" || keyID != s.EmailChallengeKey.KeyID || attempts >= EmailCodeMaxAttempts ||
		liveExpires.Sub(now) <= EmailCodeReuseMinRemaining {
		if liveID != "" {
			if _, err := tx.Exec(ctx, `UPDATE auth_email_challenges SET superseded_at=$2 WHERE challenge_id=$1`, liveID, now); err != nil {
				return EmailChallengeState{}, err
			}
			if _, err := tx.Exec(ctx, `UPDATE auth_email_deliveries SET status='cancelled', finished_at=$2
				WHERE challenge_id=$1 AND status='pending'`, liveID, now); err != nil {
				return EmailChallengeState{}, err
			}
		}
		challengeID, _, err = insertEmailChallenge(ctx, tx, row.flow, s.EmailChallengeKey.KeyID, now)
		if err != nil {
			return EmailChallengeState{}, err
		}
	}
	if err := insertEmailDelivery(ctx, tx, challengeID, email, now); err != nil {
		return EmailChallengeState{}, err
	}
	state, err := emailChallengeStateTx(ctx, tx, flowID)
	if err != nil {
		return EmailChallengeState{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return EmailChallengeState{}, fmt.Errorf("commit email resend: %w", err)
	}
	return state, nil
}

// VerifyEmailCode checks a code for the flow authority. A wrong guess is
// committed before the error returns. A flow that already proved its mailbox
// returns that proof again so a lost response or failed Firebase step can be
// retried by the same nonce holder without another code, within the bounds of
// emailProofRetryError.
func (s *Store) VerifyEmailCode(ctx context.Context, flowID, nonce, rawCode string) (EmailProof, error) {
	nonceHash, err := validateNonce(nonce)
	if err != nil {
		return EmailProof{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return EmailProof{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	row, err := lockEmailFlowByNonce(ctx, tx, flowID, nonceHash)
	if err != nil {
		return EmailProof{}, err
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return EmailProof{}, err
	}
	if row.provedAt != nil {
		if err := emailProofRetryError(row.flow, now); err != nil {
			return EmailProof{}, err
		}
		return row.proof(), tx.Commit(ctx)
	}
	if !now.Before(row.flow.ExpiresAt) {
		return EmailProof{}, ErrAuthFlowExpired
	}
	code, ok := NormalizeEmailCode(rawCode)
	if !ok {
		return EmailProof{}, &EmailCodeMismatchError{AttemptsRemaining: -1}
	}
	var challengeID, keyID string
	var expiresAt time.Time
	var attempts int
	err = tx.QueryRow(ctx, `SELECT challenge_id, key_id, expires_at, failed_code_attempts FROM auth_email_challenges
		WHERE flow_id=$1 AND superseded_at IS NULL AND consumed_at IS NULL FOR UPDATE`, flowID).Scan(&challengeID, &keyID, &expiresAt, &attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return EmailProof{}, ErrEmailChallengeExpired
	}
	if err != nil {
		return EmailProof{}, err
	}
	// Row-lock waits may pass expiry; evaluate the clock after the lock.
	if now, err = dbNow(ctx, tx); err != nil {
		return EmailProof{}, err
	}
	if !now.Before(expiresAt) || keyID != s.EmailChallengeKey.KeyID || !s.EmailChallengeKey.valid() {
		return EmailProof{}, ErrEmailChallengeExpired
	}
	if attempts >= EmailCodeMaxAttempts {
		return EmailProof{}, ErrEmailCodeLocked
	}
	expected, _, err := s.EmailChallengeKey.EmailChallengeSecrets(challengeID)
	if err != nil {
		return EmailProof{}, err
	}
	if subtle.ConstantTimeCompare([]byte(expected), []byte(code)) != 1 {
		superseded, err := s.matchesSupersededCode(ctx, tx, flowID, code)
		if err != nil {
			return EmailProof{}, err
		}
		if superseded {
			return EmailProof{}, ErrEmailChallengeSuperseded
		}
		if _, err := tx.Exec(ctx, `UPDATE auth_email_challenges SET failed_code_attempts=failed_code_attempts+1
			WHERE challenge_id=$1`, challengeID); err != nil {
			return EmailProof{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return EmailProof{}, fmt.Errorf("commit failed email code attempt: %w", err)
		}
		return EmailProof{}, &EmailCodeMismatchError{AttemptsRemaining: EmailCodeMaxAttempts - attempts - 1}
	}
	return consumeEmailChallengeTx(ctx, tx, row, challengeID, "code", now)
}

// matchesSupersededCode recognizes a code from an older email of the same
// flow. It is not counted: the old code can no longer sign in, and a random
// guess almost never equals it.
func (s *Store) matchesSupersededCode(ctx context.Context, tx pgx.Tx, flowID, code string) (bool, error) {
	rows, err := tx.Query(ctx, `SELECT challenge_id FROM auth_email_challenges
		WHERE flow_id=$1 AND superseded_at IS NOT NULL AND key_id=$2 ORDER BY created_at DESC LIMIT 10`, flowID, s.EmailChallengeKey.KeyID)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	matched := false
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return false, err
		}
		old, _, err := s.EmailChallengeKey.EmailChallengeSecrets(id)
		if err != nil {
			return false, err
		}
		if subtle.ConstantTimeCompare([]byte(old), []byte(code)) == 1 {
			matched = true
		}
	}
	return matched, rows.Err()
}

func consumeEmailChallengeTx(ctx context.Context, tx pgx.Tx, row emailFlowRow, challengeID, method string, now time.Time) (EmailProof, error) {
	if _, err := tx.Exec(ctx, `UPDATE auth_email_challenges SET consumed_at=$2, consumed_method=$3 WHERE challenge_id=$1`,
		challengeID, now, method); err != nil {
		return EmailProof{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE auth_flows SET email_proved_at=$2, email_proof_method=$3 WHERE flow_id=$1`,
		row.flow.FlowID, now, method); err != nil {
		return EmailProof{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return EmailProof{}, fmt.Errorf("commit email proof: %w", err)
	}
	row.provedAt, row.proofMethod = &now, method
	return row.proof(), nil
}

type emailChallengeRow struct {
	challengeID string
	flowID      string
	keyID       string
	expiresAt   time.Time
	superseded  bool
	consumed    bool
}

func (s *Store) verifiedLinkChallenge(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, challengeID, token, lock string) (emailChallengeRow, error) {
	var row emailChallengeRow
	if !s.EmailChallengeKey.valid() {
		return row, ErrEmailChallengeUnavailable
	}
	if _, err := uuid.Parse(challengeID); err != nil || len(token) != 43 {
		return row, ErrEmailLinkInvalid
	}
	err := q.QueryRow(ctx, `SELECT challenge_id, flow_id, key_id, expires_at, superseded_at IS NOT NULL, consumed_at IS NOT NULL
		FROM auth_email_challenges WHERE challenge_id=$1 `+lock, challengeID).Scan(
		&row.challengeID, &row.flowID, &row.keyID, &row.expiresAt, &row.superseded, &row.consumed)
	if errors.Is(err, pgx.ErrNoRows) {
		return row, ErrEmailLinkInvalid
	}
	if err != nil {
		return row, err
	}
	if row.keyID != s.EmailChallengeKey.KeyID {
		return row, ErrEmailLinkInvalid
	}
	_, expected, err := s.EmailChallengeKey.EmailChallengeSecrets(row.challengeID)
	if err != nil {
		return row, err
	}
	if subtle.ConstantTimeCompare([]byte(expected), []byte(token)) != 1 {
		return row, ErrEmailLinkInvalid
	}
	return row, nil
}

// InspectEmailLink never consumes the challenge. Opening or scanning a link
// therefore leaves the original browser's code usable.
func (s *Store) InspectEmailLink(ctx context.Context, challengeID, token, nonce string) (EmailLinkInspection, error) {
	challenge, err := s.verifiedLinkChallenge(ctx, s.pool, challengeID, token, "")
	if err != nil {
		return EmailLinkInspection{}, err
	}
	var result EmailLinkInspection
	var nonceHash []byte
	var proved bool
	var now time.Time
	err = s.pool.QueryRow(ctx, `SELECT flow_id, intent, normalized_email, status, expires_at, nonce_hash,
		email_proved_at IS NOT NULL, clock_timestamp() FROM auth_flows WHERE flow_id=$1`, challenge.flowID).Scan(
		&result.FlowID, &result.Intent, &result.NormalizedEmail, &result.State, &result.ExpiresAt, &nonceHash, &proved, &now)
	if err != nil {
		return EmailLinkInspection{}, err
	}
	flowStatus := result.State
	if nonce != "" {
		if hash, err := validateNonce(nonce); err == nil && subtle.ConstantTimeCompare(hash, nonceHash) == 1 {
			result.SameBrowser = true
		}
	}
	result.ExpiresAt = challenge.expiresAt
	switch {
	case flowStatus == "completed":
		result.State = "completed"
	case challenge.consumed && proved && result.SameBrowser:
		result.State = "proved_here"
	case challenge.consumed:
		result.State = "consumed"
	case challenge.superseded:
		result.State = "superseded"
	case !now.Before(challenge.expiresAt):
		result.State = "expired"
	default:
		result.State = "usable"
	}
	return result, nil
}

// CompleteEmailLink proves the mailbox with the link token. The flow's own
// nonce completes directly; another browser must explicitly adopt the flow,
// which moves its authority to the new nonce.
func (s *Store) CompleteEmailLink(ctx context.Context, challengeID, token, nonce string, adopt bool) (EmailProof, error) {
	nonceHash, err := validateNonce(nonce)
	if err != nil {
		return EmailProof{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return EmailProof{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	unlocked, err := s.verifiedLinkChallenge(ctx, tx, challengeID, token, "")
	if err != nil {
		return EmailProof{}, err
	}
	row, err := scanEmailFlowForUpdate(ctx, tx, "flow_id=$1", unlocked.flowID)
	if err != nil {
		return EmailProof{}, err
	}
	sameBrowser := subtle.ConstantTimeCompare(row.nonceHash, nonceHash) == 1
	if row.provedAt != nil {
		if !sameBrowser {
			return EmailProof{}, ErrEmailChallengeConsumed
		}
		now, err := dbNow(ctx, tx)
		if err != nil {
			return EmailProof{}, err
		}
		if err := emailProofRetryError(row.flow, now); err != nil {
			return EmailProof{}, err
		}
		return row.proof(), tx.Commit(ctx)
	}
	if !sameBrowser && !adopt {
		return EmailProof{}, ErrEmailLinkAdoptionRequired
	}
	challenge, err := s.verifiedLinkChallenge(ctx, tx, challengeID, token, "FOR UPDATE")
	if err != nil {
		return EmailProof{}, err
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return EmailProof{}, err
	}
	switch {
	case row.flow.Status == "completed" || challenge.consumed:
		return EmailProof{}, ErrEmailChallengeConsumed
	case challenge.superseded:
		return EmailProof{}, ErrEmailChallengeSuperseded
	case !now.Before(challenge.expiresAt) || !now.Before(row.flow.ExpiresAt):
		return EmailProof{}, ErrEmailChallengeExpired
	}
	if !sameBrowser {
		tag, err := tx.Exec(ctx, `UPDATE auth_flows SET adopted_from_nonce_hash=nonce_hash, nonce_hash=$2
			WHERE flow_id=$1 AND adopted_from_nonce_hash IS NULL`, row.flow.FlowID, nonceHash)
		if isUniqueViolation(err) {
			return EmailProof{}, ErrInvalidAuthFlow
		}
		if err != nil {
			return EmailProof{}, err
		}
		// A flow moves to another browser at most once.
		if tag.RowsAffected() != 1 {
			return EmailProof{}, ErrEmailChallengeConsumed
		}
		row.nonceHash = nonceHash
	}
	return consumeEmailChallengeTx(ctx, tx, row, challenge.challengeID, "link", now)
}

// BindEmailProofUID records the Firebase principal resolved for a proved
// mailbox. A retry may only confirm the same UID.
func (s *Store) BindEmailProofUID(ctx context.Context, flowID, nonce, uid string) (EmailProof, error) {
	if uid == "" || len(uid) > 128 {
		return EmailProof{}, ErrAuthProofMismatch
	}
	nonceHash, err := validateNonce(nonce)
	if err != nil {
		return EmailProof{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return EmailProof{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	row, err := lockEmailFlowByNonce(ctx, tx, flowID, nonceHash)
	if err != nil {
		return EmailProof{}, err
	}
	if row.provedAt == nil {
		return EmailProof{}, ErrAuthProofMismatch
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return EmailProof{}, err
	}
	if err := emailProofRetryError(row.flow, now); err != nil {
		return EmailProof{}, err
	}
	if row.proofUID != "" || row.flow.Status == "completed" {
		if row.proofUID != uid {
			return EmailProof{}, ErrAuthProofMismatch
		}
		return row.proof(), tx.Commit(ctx)
	}
	if _, err := tx.Exec(ctx, `UPDATE auth_flows SET email_proof_uid=$2, email_proof_uid_bound_at=$3 WHERE flow_id=$1`,
		flowID, uid, now); err != nil {
		return EmailProof{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return EmailProof{}, err
	}
	row.proofUID, row.proofUIDBound = uid, &now
	return row.proof(), nil
}

// FirebaseUIDBound reports whether a Firebase principal already authorizes a
// Human.
func (s *Store) FirebaseUIDBound(ctx context.Context, uid string) (bool, error) {
	var bound bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM credentials WHERE provider='firebase' AND external_subject=$1)`, uid).Scan(&bound)
	return bound, err
}

// DeleteUnboundFirebasePrincipal runs remove under the per-UID credential lock
// only when no Human has ever bound the principal. Unfinished flows that
// already proved this principal are expired first, so a later confirmation
// cannot provision a Human for a deleted UID. remove runs inside the lock; if
// it fails the transaction rolls back.
func (s *Store) DeleteUnboundFirebasePrincipal(ctx context.Context, uid string, remove func(context.Context) error) error {
	if uid == "" || len(uid) > 128 {
		return ErrAuthProofMismatch
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "8:firebase"+uid); err != nil {
		return fmt.Errorf("lock Firebase credential: %w", err)
	}
	var bound bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM credentials WHERE provider='firebase' AND external_subject=$1)`, uid).Scan(&bound); err != nil {
		return err
	}
	if bound {
		return ErrCredentialAlreadyBound
	}
	if _, err := tx.Exec(ctx, `UPDATE auth_flows SET expires_at=LEAST(expires_at, clock_timestamp())
		WHERE status<>'completed' AND (firebase_uid=$1 OR email_proof_uid=$1)`, uid); err != nil {
		return err
	}
	if err := remove(ctx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// HumanForFirebaseUID returns the active Human bound to a Firebase principal.
func (s *Store) HumanForFirebaseUID(ctx context.Context, uid string) (string, bool, error) {
	var humanID string
	err := s.pool.QueryRow(ctx, `SELECT human_id FROM credentials WHERE provider='firebase' AND external_subject=$1 AND active`, uid).Scan(&humanID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	return humanID, err == nil, err
}

type EmailDelivery struct {
	DeliveryID       string
	ChallengeID      string
	NormalizedEmail  string
	Attempts         int
	CreatedAt        time.Time
	ChallengeExpires time.Time
}

// ClaimEmailDeliveries leases due send intents. Intents whose challenge is no
// longer usable are cancelled instead of sent. An expired lease is reclaimed,
// so delivery is at-least-once.
func (s *Store) ClaimEmailDeliveries(ctx context.Context, limit int, lease time.Duration) ([]EmailDelivery, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT d.delivery_id, d.challenge_id, d.normalized_email, d.attempts, d.created_at,
			c.expires_at, (c.superseded_at IS NOT NULL OR c.consumed_at IS NOT NULL OR c.expires_at <= clock_timestamp()
				OR c.key_id <> $2)
		FROM auth_email_deliveries d JOIN auth_email_challenges c USING (challenge_id)
		WHERE (d.status='pending' AND d.next_attempt_at <= clock_timestamp())
			OR (d.status='sending' AND d.lease_expires_at <= clock_timestamp())
		ORDER BY d.next_attempt_at FOR UPDATE OF d SKIP LOCKED LIMIT $1`, limit, s.EmailChallengeKey.keyIDOrEmpty())
	if err != nil {
		return nil, err
	}
	var claimed []EmailDelivery
	var inactive, exhausted []string
	for rows.Next() {
		var item EmailDelivery
		var dead bool
		if err := rows.Scan(&item.DeliveryID, &item.ChallengeID, &item.NormalizedEmail, &item.Attempts,
			&item.CreatedAt, &item.ChallengeExpires, &dead); err != nil {
			rows.Close()
			return nil, err
		}
		if dead {
			inactive = append(inactive, item.DeliveryID)
			continue
		}
		if item.Attempts >= EmailDeliveryMaxAttempts {
			exhausted = append(exhausted, item.DeliveryID)
			continue
		}
		claimed = append(claimed, item)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, id := range inactive {
		if _, err := tx.Exec(ctx, `UPDATE auth_email_deliveries SET status='cancelled', lease_expires_at=NULL,
			finished_at=clock_timestamp() WHERE delivery_id=$1`, id); err != nil {
			return nil, err
		}
	}
	for _, id := range exhausted {
		// The last attempt's lease expired without a recorded result.
		if _, err := tx.Exec(ctx, `UPDATE auth_email_deliveries SET status='failed', lease_expires_at=NULL,
			finished_at=clock_timestamp(), failure_class='retries_exhausted' WHERE delivery_id=$1`, id); err != nil {
			return nil, err
		}
	}
	for i := range claimed {
		claimed[i].Attempts++
		if _, err := tx.Exec(ctx, `UPDATE auth_email_deliveries SET status='sending', attempts=$2,
			lease_expires_at=clock_timestamp()+$3::bigint*interval '1 microsecond' WHERE delivery_id=$1`,
			claimed[i].DeliveryID, claimed[i].Attempts, lease.Microseconds()); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	if len(claimed) == 0 && len(inactive)+len(exhausted) > 0 {
		return s.ClaimEmailDeliveries(ctx, limit, lease)
	}
	return claimed, nil
}

func (k *EmailChallengeKey) keyIDOrEmpty() string {
	if k == nil {
		return ""
	}
	return k.KeyID
}

// FinishEmailDelivery records a send result for the exact lease attempt.
// retryAt zero with failed=false means sent.
func (s *Store) FinishEmailDelivery(ctx context.Context, deliveryID string, attempt int, sendErr error, permanent bool, retryAt time.Time) error {
	var tag interface{ RowsAffected() int64 }
	var err error
	switch {
	case sendErr == nil:
		tag, err = s.pool.Exec(ctx, `UPDATE auth_email_deliveries SET status='sent', lease_expires_at=NULL,
			finished_at=clock_timestamp() WHERE delivery_id=$1 AND status='sending' AND attempts=$2`, deliveryID, attempt)
	case permanent || retryAt.IsZero():
		class := "retries_exhausted"
		if permanent {
			class = "permanent"
		}
		tag, err = s.pool.Exec(ctx, `UPDATE auth_email_deliveries SET status='failed', lease_expires_at=NULL,
			finished_at=clock_timestamp(), failure_class=$3 WHERE delivery_id=$1 AND status='sending' AND attempts=$2`,
			deliveryID, attempt, class)
	default:
		tag, err = s.pool.Exec(ctx, `UPDATE auth_email_deliveries SET status='pending', lease_expires_at=NULL,
			next_attempt_at=$3 WHERE delivery_id=$1 AND status='sending' AND attempts=$2`, deliveryID, attempt, retryAt)
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errEmailDeliveryLeaseSuperseded
	}
	return nil
}
