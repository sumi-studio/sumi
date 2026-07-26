package agentevents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"unicode/utf8"
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
)

// CommandAppender is the durable command log entry point owned by the T28 API
// production boundary. Its single Append call owns the atomic allocation of a
// canonical command_id and monotonic seq after the ingress validator has
// accepted the payload.
type CommandAppender interface {
	// Append validates (if needed) and atomically allocates the next durable
	// command_id and seq for the conversation, then returns the persisted
	// CommandEnvelope. Append is only called for payloads that have already
	// passed the pre-sequence size/attachment/shape checks.
	//
	// If idempotencyKey is non-empty, the appender returns the existing
	// CommandEnvelope for that key when the same command bytes are resubmitted;
	// a different body for the same key is a conflict and returns an error.
	Append(ctx context.Context, conversationID string, idempotencyKey string, command json.RawMessage) (CommandEnvelope, error)
}

// UserCommandIngress is the HTTP handler for web → API user command admission.
// It authenticates the caller, authorizes the conversation, then rejects
// oversized payloads, non-empty attachments, and malformed commands before
// calling CommandAppender.Append. Rejected requests never allocate a command_id
// or seq and cannot poison later commands.
type UserCommandIngress struct {
	Appender CommandAppender
	Verifier TokenVerifier
	MaxBytes int64
}

// NewUserCommandIngress returns an ingress handler wired to the given appender.
// It fail-closes: a nil appender returns an error so cmd/server cannot expose
// the route with an unbacked log. A nil Verifier causes every request to be
// rejected with 401 until a production TokenVerifier is wired.
func NewUserCommandIngress(appender CommandAppender, verifier TokenVerifier) (*UserCommandIngress, error) {
	if appender == nil {
		return nil, errors.New("CommandAppender is required")
	}
	return &UserCommandIngress{Appender: appender, Verifier: verifier, MaxBytes: MaxUserCommandBytes}, nil
}

func (h *UserCommandIngress) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	conversationID := r.PathValue("conversation_id")
	if conversationID == "" {
		http.Error(w, "missing conversation_id", http.StatusBadRequest)
		return
	}

	token, ok := bearerToken(r.Header.Get("Authorization"))
	if !ok || h.Verifier == nil {
		http.Error(w, "missing authorization", http.StatusUnauthorized)
		return
	}

	claims, err := h.Verifier.Verify(r.Context(), token)
	if err != nil {
		http.Error(w, "invalid authorization", http.StatusUnauthorized)
		return
	}

	if claims.ConversationID != conversationID {
		http.Error(w, "conversation authorization failed", http.StatusForbidden)
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
	if len(idempotencyKey) > MaxIdempotencyKeyBytes {
		writeRejection(w, RejectOversized)
		return
	}

	env, err := h.Appender.Append(r.Context(), conversationID, idempotencyKey, raw)
	if err != nil {
		// Idempotency conflicts are exposed as 409 so callers cannot
		// accidentally mint a second command by retrying with a mutated body.
		if isIdempotencyConflict(err) {
			http.Error(w, "idempotency key conflict", http.StatusConflict)
			return
		}
		http.Error(w, "command append failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(env)
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
