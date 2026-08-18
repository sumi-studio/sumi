package messaging

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

// MaxReactionEmojiChars matches the schema CHECK on message_reactions.emoji.
const MaxReactionEmojiChars = 32

// ReactionSummary aggregates one emoji on one message. Summaries are ordered
// by first reaction, participants within a summary by reaction time — the
// same shape the web model renders as chips.
type ReactionSummary struct {
	Emoji        string
	Participants []ParticipantRef
}

func (s *Server) toggleScopedReaction(ctx context.Context, store *ScopedStore, placeID, messageID, emoji, clientNonce string) (Message, bool, error) {
	s.reactionMu.Lock()
	defer s.reactionMu.Unlock()
	message, reacted, err := store.ToggleReactionIdempotent(ctx, placeID, messageID, emoji, clientNonce)
	if err != nil {
		return Message{}, false, err
	}
	if s.Hub != nil {
		update := reactionUpdateToWire(message)
		_ = s.Hub.PublishScoped(ctx, store, Event{Type: EventReactionUpdated, PlaceID: placeID, Reaction: &update})
	}
	return message, reacted, nil
}

func (s *ScopedStore) ToggleReactionIdempotent(ctx context.Context, placeID, messageID, emoji, clientNonce string) (Message, bool, error) {
	if !clientNonceValid(clientNonce) {
		return Message{}, false, fmt.Errorf("client nonce must be 1..128 bytes")
	}
	if err := validateReactionEmoji(emoji); err != nil {
		return Message{}, false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Message{}, false, fmt.Errorf("begin scoped reaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeMutationInTx(ctx, tx); err != nil {
		return Message{}, false, err
	}
	place, err := s.loadScopedPlace(ctx, tx, placeID)
	if err != nil {
		return Message{}, false, err
	}
	access, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor)
	if err != nil {
		return Message{}, false, err
	}
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)",
		reactionMutationLockKey(s.Scope.Actor, s.Scope.WorkspaceID+":"+clientNonce)); err != nil {
		return Message{}, false, fmt.Errorf("lock scoped reaction mutation: %w", err)
	}
	message, err := lockMessageScoped(ctx, tx, s.Scope.WorkspaceID, placeID, messageID)
	if err != nil {
		return Message{}, false, err
	}
	if message.Seq < access.VisibleFromSeq {
		return Message{}, false, ErrMessageNotFound
	}
	if message.Deleted {
		return Message{}, false, ErrMessageDeleted
	}
	var existingMessageID, existingEmoji string
	var existingReacted bool
	err = tx.QueryRow(ctx, `
		SELECT message_id, emoji, reacted FROM message_reaction_mutations
		WHERE workspace_id = $1 AND member_kind = $2 AND member_id = $3 AND client_nonce = $4`,
		s.Scope.WorkspaceID, s.Scope.Actor.Kind, s.Scope.Actor.ID, clientNonce).Scan(
		&existingMessageID, &existingEmoji, &existingReacted)
	if err == nil {
		if existingMessageID != messageID || existingEmoji != emoji {
			return Message{}, false, ErrIdempotencyConflict
		}
		parts := []Message{message}
		if err := attachMessagePartsWith(ctx, tx, parts); err != nil {
			return Message{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return Message{}, false, err
		}
		return parts[0], existingReacted, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Message{}, false, fmt.Errorf("query scoped reaction mutation: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		DELETE FROM message_reactions
		WHERE message_id = $1 AND member_kind = $2 AND member_id = $3 AND emoji = $4`,
		messageID, s.Scope.Actor.Kind, s.Scope.Actor.ID, emoji)
	if err != nil {
		return Message{}, false, err
	}
	reacted := false
	if tag.RowsAffected() == 0 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO message_reactions (message_id, member_kind, member_id, emoji)
			VALUES ($1, $2, $3, $4)`, messageID, s.Scope.Actor.Kind, s.Scope.Actor.ID, emoji); err != nil {
			return Message{}, false, err
		}
		reacted = true
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO message_reaction_mutations
			(workspace_id, member_kind, member_id, client_nonce, message_id, emoji, reacted)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`, s.Scope.WorkspaceID,
		s.Scope.Actor.Kind, s.Scope.Actor.ID, clientNonce, messageID, emoji, reacted); err != nil {
		return Message{}, false, err
	}
	parts := []Message{message}
	if err := attachMessagePartsWith(ctx, tx, parts); err != nil {
		return Message{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Message{}, false, err
	}
	return parts[0], reacted, nil
}

func reactionMutationLockKey(actor ParticipantRef, clientNonce string) int64 {
	digest := sha256.New()
	for _, value := range []string{string(actor.Kind), actor.ID, clientNonce} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = digest.Write(length[:])
		_, _ = digest.Write([]byte(value))
	}
	return int64(binary.BigEndian.Uint64(digest.Sum(nil)[:8]))
}

// validateReactionEmoji bounds the emoji the same way the schema does. The
// server does not curate a palette (that is presentation); it only rejects
// shapes that could not be an emoji at all.
func validateReactionEmoji(emoji string) error {
	if emoji == "" || utf8.RuneCountInString(emoji) > MaxReactionEmojiChars {
		return fmt.Errorf("emoji must be 1..%d characters", MaxReactionEmojiChars)
	}
	for _, r := range emoji {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return fmt.Errorf("emoji must not contain control or space characters")
		}
	}
	return nil
}
