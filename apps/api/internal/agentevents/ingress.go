package agentevents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/sumi-studio/sumi/apps/api/internal/directchat"
)

// MaxUserCommandBytes is the wire-size limit for a user_message command before
// command_id/seq allocation. It matches the pre-sequence boundary in
// contracts/agent-events.yaml and §11.1.1.
const MaxUserCommandBytes = 1024 * 1024

// MaxIdempotencyKeyBytes is the largest Idempotency-Key header value that will
// be stored in the durable log. Larger keys are rejected before seq allocation.
const MaxIdempotencyKeyBytes = 1024

var errBodyTooLarge = errors.New("request body exceeds limit")

// RejectReason is a user-visible classification for a pre-sequence rejection.
// It mirrors the command-ack reject_reason enum in the public contract.
type RejectReason string

const (
	RejectUnknownCommand      RejectReason = "unknown_command"
	RejectSchemaViolation     RejectReason = "schema_violation"
	RejectAttachmentsNotEmpty RejectReason = "attachments_not_empty"
	RejectOversized           RejectReason = "oversized"
	RejectNotAllowed          RejectReason = "not_allowed"
	RejectIdempotencyConflict RejectReason = "idempotency_conflict"
	RejectUnavailable         RejectReason = "unavailable"
)

// CommandAppender is the durable command log entry point owned by the T28 API
// production boundary. Its single Append call owns the atomic allocation of a
// canonical command_id and monotonic seq after the ingress validator has
// accepted the payload.
type CommandAppender interface {
	// Append validates (if needed) and atomically allocates the next durable
	// command_id and seq for the personality agent, then returns the persisted
	// CommandEnvelope. Append is only called for payloads that have already
	// passed the pre-sequence size/attachment/shape checks.
	//
	// If idempotencyKey is non-empty, the appender returns the existing
	// CommandEnvelope for that key when the same command bytes are resubmitted;
	// a different body for the same key is a conflict and returns an error.
	Append(ctx context.Context, provenance DirectChatProvenance, idempotencyKey string, command json.RawMessage) (CommandEnvelope, error)
}

// UserCommandIngress is the HTTP handler for web → API user command admission.
// It first requires an exact allow-listed browser Origin, authenticates the
// caller via the signed HttpOnly browser session cookie, derives the target and
// provenance exclusively from that session, then rejects oversized payloads,
// non-empty attachments, and malformed commands before calling
// CommandAppender.Append.
// Rejected requests never allocate a command_id or seq and cannot poison later
// commands.
type UserCommandIngress struct {
	Appender  CommandAppender
	Sessions  UserSessionAuthorizer
	Spawner   DirectChatSpawner
	Readiness interface {
		IsPersonalityAgentReady(context.Context, string) (bool, error)
	}
	SpawnTimeout      time.Duration
	SpawnReadyTimeout time.Duration
	MaxBytes          int64
	AllowedOrigins    []string
	// Authorizer gates direct chat on Current Employer and the exact enabled
	// Human-owned direct-chat AppInstallation. A nil Authorizer fails closed.
	Authorizer DirectChatAuthorizer
	// LifecycleFence must be the same process fence used by app and Employer
	// lifecycle mutations. Nil fails closed.
	LifecycleFence *directchat.LifecycleFence
}

var errCommandAppenderRequired = errors.New("CommandAppender is required")

// NewUserCommandIngress returns an ingress handler wired to the given appender.
// It fail-closes: a nil appender returns an error so cmd/server cannot expose
// the route with an unbacked log. AllowedOrigins defaults to an empty,
// fail-closed allowlist. Once an origin is accepted, a nil Sessions verifier
// causes the request to be rejected with 401 until a production
// UserSessionVerifier is wired.
func NewUserCommandIngress(appender CommandAppender, sessions UserSessionAuthorizer) (*UserCommandIngress, error) {
	if appender == nil {
		return nil, errCommandAppenderRequired
	}
	ingress := &UserCommandIngress{Appender: appender, Sessions: sessions, MaxBytes: MaxUserCommandBytes}
	if readiness, ok := appender.(interface {
		IsPersonalityAgentReady(context.Context, string) (bool, error)
	}); ok {
		ingress.Readiness = readiness
	}
	return ingress, nil
}

func (h *UserCommandIngress) authorizeDirectChat(
	ctx context.Context,
	claims UserSessionClaims,
	scope directChatScope,
) error {
	if h.Authorizer == nil {
		return ErrDirectChatAuthorizationUnavailable
	}
	return h.Authorizer.AuthorizeDirectChat(
		ctx,
		claims.UserID,
		claims.PersonalityAgentID,
		scope.InstallationID,
		scope.AuthorityEpoch,
	)
}

func (h *UserCommandIngress) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !browserOriginAllowed(r, h.AllowedOrigins) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return
	}

	cookie, err := uniqueBrowserSessionCookie(r)
	if err != nil || h.Sessions == nil {
		if errors.Is(err, errBrowserSessionDuplicate) {
			http.Error(w, "duplicate session cookies", http.StatusBadRequest)
			return
		}
		http.Error(w, "missing session", http.StatusUnauthorized)
		return
	}

	claims, err := h.Sessions.VerifySession(r.Context(), cookie.Value)
	if err != nil {
		http.Error(w, "invalid session", http.StatusUnauthorized)
		return
	}
	scope, err := directChatScopeFromRequest(r)
	if err != nil {
		writeDirectChatInvalidScope(w)
		return
	}
	if h.Authorizer == nil || h.LifecycleFence == nil {
		http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
		return
	}
	releaseLifecycle, err := h.LifecycleFence.AcquireOperation(r.Context())
	if err != nil {
		http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
		return
	}
	defer releaseLifecycle()
	if err := h.authorizeDirectChat(r.Context(), claims, scope); err != nil {
		if errors.Is(err, ErrDirectChatAuthorizationUnavailable) {
			http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, "not authorized for this agent", http.StatusForbidden)
		return
	}

	raw, err := readLimitedBody(r.Body, h.MaxBytes)
	if err != nil {
		if errors.Is(err, errBodyTooLarge) {
			writeRejection(w, RejectOversized)
		} else {
			writeRejection(w, RejectSchemaViolation)
		}
		return
	}

	if reason, err := validateUserCommand(raw); err != nil {
		writeRejection(w, reason)
		return
	}

	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		writeRejection(w, RejectSchemaViolation)
		return
	}
	if len(idempotencyKey) > MaxIdempotencyKeyBytes {
		writeRejection(w, RejectOversized)
		return
	}
	if h.Spawner != nil {
		spawnContext, cancelSpawn := context.WithTimeout(r.Context(), h.spawnTimeout())
		err := h.Spawner.EnsureRunning(spawnContext, claims.PersonalityAgentID)
		cancelSpawn()
		if err != nil {
			log.Printf("direct command lazy spawn failed for PAID %s: %v", claims.PersonalityAgentID, err)
			writeUnavailable(w, idempotencyKey)
			return
		}
		if h.Readiness != nil {
			if err := h.awaitSpawnReady(r.Context(), claims.PersonalityAgentID); err != nil {
				log.Printf("direct command runtime readiness failed for PAID %s: %v", claims.PersonalityAgentID, err)
				writeUnavailable(w, idempotencyKey)
				return
			}
		}
	}

	var env CommandEnvelope
	sessionLeaseEntered := false
	appendCalled := false
	operationContext, cancelOperation := browserSessionOperationContext(r.Context(), claims)
	defer cancelOperation()
	err = h.Sessions.AuthorizeSession(r.Context(), claims, func() error {
		sessionLeaseEntered = true
		// Composite PostgreSQL authorization commits before the durable append.
		// The surrounding process-lifetime lifecycle permit and session lease
		// remain held across the effect, so a backend loss after authorization
		// cannot turn an already-appended command into a failed auth commit.
		if err := h.authorizeDirectChat(r.Context(), claims, scope); err != nil {
			return err
		}
		appendCalled = true
		var appendErr error
		env, appendErr = h.Appender.Append(
			operationContext,
			directChatProvenance(claims),
			idempotencyKey,
			raw,
		)
		return appendErr
	})
	if err != nil {
		if !sessionLeaseEntered ||
			(errors.Is(err, context.DeadlineExceeded) && !time.Now().Before(claims.expiresAt)) {
			http.Error(w, "invalid session", http.StatusUnauthorized)
			return
		}
		if !appendCalled {
			if errors.Is(err, ErrDirectChatAuthorizationUnavailable) {
				http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
				return
			}
			http.Error(w, "not authorized for this agent", http.StatusForbidden)
			return
		}
		if errors.Is(err, errBrowserRuntimeUnavailable) {
			writeUnavailable(w, idempotencyKey)
			return
		}
		if errors.Is(err, ErrDirectChatAuthorizationUnavailable) {
			http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
			return
		}
		// Idempotency conflicts are exposed as 409 so callers cannot
		// accidentally mint a second command by retrying with a mutated body.
		if isIdempotencyConflict(err) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(struct {
				Error          string       `json:"error"`
				IdempotencyKey string       `json:"idempotency_key"`
				RejectReason   RejectReason `json:"reject_reason"`
			}{
				Error:          "idempotency_conflict",
				IdempotencyKey: idempotencyKey,
				RejectReason:   RejectIdempotencyConflict,
			})
			return
		}
		http.Error(w, "command append failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(browserCommandReceipt{
		IdempotencyKey: idempotencyKey,
		CommandID:      env.CommandID,
		Seq:            env.Seq,
	})
}

func (h *UserCommandIngress) spawnTimeout() time.Duration {
	if h.SpawnTimeout > 0 {
		return h.SpawnTimeout
	}
	return 30 * time.Second
}

func (h *UserCommandIngress) awaitSpawnReady(ctx context.Context, personalityAgentID string) error {
	timeout := h.SpawnReadyTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	readyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready, err := h.Readiness.IsPersonalityAgentReady(readyCtx, personalityAgentID)
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		select {
		case <-readyCtx.Done():
			return readyCtx.Err()
		case <-ticker.C:
		}
	}
}

func writeUnavailable(w http.ResponseWriter, idempotencyKey string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(struct {
		Error          string       `json:"error"`
		IdempotencyKey string       `json:"idempotency_key"`
		RejectReason   RejectReason `json:"reject_reason"`
	}{
		Error:          "unavailable",
		IdempotencyKey: idempotencyKey,
		RejectReason:   RejectUnavailable,
	})
}

func directChatProvenance(claims UserSessionClaims) DirectChatProvenance {
	return DirectChatProvenance{
		Version:            1,
		TenantID:           claims.TenantID,
		PersonalityAgentID: claims.PersonalityAgentID,
		Actor: ProvenanceActor{
			Kind:        "human",
			PrincipalID: claims.UserID,
		},
		Source: ProvenanceSource{Surface: "direct_chat"},
	}
}

func readLimitedBody(r io.Reader, limit int64) ([]byte, error) {
	// Read one byte past the limit so we can distinguish "exactly at limit"
	// from "over limit" without allocating the full oversized payload.
	limited := io.LimitReader(r, limit+1)
	b, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, errBodyTooLarge
	}
	return b, nil
}

func validateUserCommand(raw []byte) (RejectReason, error) {
	if !utf8.Valid(raw) {
		return RejectSchemaViolation, errors.New("request body is not valid UTF-8")
	}

	if err := checkDuplicateKeys(raw); err != nil {
		return RejectSchemaViolation, fmt.Errorf("invalid JSON: %w", err)
	}

	var cmd userMessageWire
	if err := unmarshalStrict(raw, &cmd); err != nil {
		if errors.Is(err, errAttachmentsNotEmpty) {
			return RejectAttachmentsNotEmpty, err
		}
		return RejectSchemaViolation, fmt.Errorf("invalid command shape: %w", err)
	}
	if cmd.Type != "user_message" {
		return RejectUnknownCommand, fmt.Errorf("unknown command type: %q", cmd.Type)
	}
	if cmd.Text == nil {
		return RejectSchemaViolation, errors.New("missing field: text")
	}
	if cmd.Attachments == nil {
		return RejectSchemaViolation, errors.New("missing field: attachments")
	}

	return "", nil
}

func isIdempotencyConflict(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, errIdempotencyConflict)
}

var errIdempotencyConflict = errors.New("idempotency key conflict")

func writeRejection(w http.ResponseWriter, reason RejectReason) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error":         "invalid_command",
		"reject_reason": string(reason),
	})
}
