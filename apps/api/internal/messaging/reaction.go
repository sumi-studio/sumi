package messaging

import (
	"context"
	"fmt"
	"unicode"
	"unicode/utf8"
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

// ToggleReaction flips actor × message × emoji: absent becomes present,
// present becomes absent (人間のUIとagentの道具が同じトグルを同じ経路で使う).
// The message row is locked for the duration, so concurrent toggles serialize
// and the returned state is the committed one. A place the actor cannot see
// is reported as ErrPlaceNotFound; a tombstone rejects new reactions.
// The returned message carries its full reaction and mention state.
func (s *Store) ToggleReaction(ctx context.Context, placeID, messageID string, actor ParticipantRef, emoji string) (Message, bool, error) {
	if err := actor.Validate(); err != nil {
		return Message{}, false, err
	}
	if err := validateReactionEmoji(emoji); err != nil {
		return Message{}, false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Message{}, false, fmt.Errorf("begin toggle reaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	place, err := s.loadPlace(ctx, tx, placeID)
	if err != nil {
		return Message{}, false, err
	}
	visible, err := s.canAccess(ctx, tx, place, actor)
	if err != nil {
		return Message{}, false, err
	}
	if !visible {
		return Message{}, false, ErrPlaceNotFound
	}
	msg, err := lockMessage(ctx, tx, placeID, messageID)
	if err != nil {
		return Message{}, false, err
	}
	if msg.Deleted {
		return Message{}, false, ErrMessageDeleted
	}

	tag, err := tx.Exec(ctx,
		`DELETE FROM message_reactions
		 WHERE message_id = $1 AND member_kind = $2 AND member_id = $3 AND emoji = $4`,
		messageID, actor.Kind, actor.ID, emoji)
	if err != nil {
		return Message{}, false, fmt.Errorf("remove reaction: %w", err)
	}
	reacted := false
	if tag.RowsAffected() == 0 {
		if _, err := tx.Exec(ctx,
			`INSERT INTO message_reactions (message_id, member_kind, member_id, emoji)
			 VALUES ($1, $2, $3, $4)`,
			messageID, actor.Kind, actor.ID, emoji); err != nil {
			return Message{}, false, fmt.Errorf("add reaction: %w", err)
		}
		reacted = true
	}
	if err := tx.Commit(ctx); err != nil {
		return Message{}, false, fmt.Errorf("commit toggle reaction: %w", err)
	}
	messages := []Message{msg}
	if err := s.attachMentions(ctx, messages); err != nil {
		return Message{}, false, err
	}
	if err := s.attachReactions(ctx, messages); err != nil {
		return Message{}, false, err
	}
	return messages[0], reacted, nil
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

// attachReactions loads reaction rows for the given messages in one query and
// aggregates them into per-emoji summaries, mirroring attachMentions.
func (s *Store) attachReactions(ctx context.Context, messages []Message) error {
	if len(messages) == 0 {
		return nil
	}
	ids := make([]string, len(messages))
	index := make(map[string]int, len(messages))
	for i, m := range messages {
		ids[i] = m.MessageID
		index[m.MessageID] = i
	}
	rows, err := s.pool.Query(ctx,
		`SELECT message_id, emoji, member_kind, member_id
		 FROM message_reactions
		 WHERE message_id = ANY($1)
		 ORDER BY message_id, created_at, member_kind, member_id`, ids)
	if err != nil {
		return fmt.Errorf("query reactions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var messageID, emoji, kind, id string
		if err := rows.Scan(&messageID, &emoji, &kind, &id); err != nil {
			return fmt.Errorf("scan reaction: %w", err)
		}
		i := index[messageID]
		participant := ParticipantRef{Kind: ParticipantKind(kind), ID: id}
		found := false
		for j := range messages[i].Reactions {
			if messages[i].Reactions[j].Emoji == emoji {
				messages[i].Reactions[j].Participants = append(messages[i].Reactions[j].Participants, participant)
				found = true
				break
			}
		}
		if !found {
			messages[i].Reactions = append(messages[i].Reactions, ReactionSummary{
				Emoji:        emoji,
				Participants: []ParticipantRef{participant},
			})
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate reactions: %w", err)
	}
	return nil
}
