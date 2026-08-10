package messaging

import (
	"context"
	"fmt"
	"sync"
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

// ReactionMutationResult is the complete projection owned by one successful
// desired-state mutation. It contains no unrelated message fields.
type ReactionMutationResult struct {
	MessageID string
	Reactions []ReactionSummary
	Reacted   bool
}

type reactionSnapshotLoader func(context.Context, pgx.Tx, string) ([]ReactionSummary, error)

// SetReaction states the desired actor × message × emoji membership. Both add
// and remove are idempotent: a retry repeats intent instead of toggling it.
// The authoritative absolute snapshot is read through the same transaction
// before commit. If snapshot construction fails, the mutation rolls back and
// no success can be published without a corresponding projection.
func (s *Store) SetReaction(
	ctx context.Context,
	placeID, messageID string,
	actor ParticipantRef,
	emoji string,
	reacted bool,
) (ReactionMutationResult, error) {
	return s.setReaction(ctx, placeID, messageID, actor, emoji, reacted, loadReactionSnapshot)
}

func (s *Store) setReaction(
	ctx context.Context,
	placeID, messageID string,
	actor ParticipantRef,
	emoji string,
	reacted bool,
	loadSnapshot reactionSnapshotLoader,
) (ReactionMutationResult, error) {
	if err := actor.Validate(); err != nil {
		return ReactionMutationResult{}, err
	}
	if err := validateReactionEmoji(emoji); err != nil {
		return ReactionMutationResult{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ReactionMutationResult{}, fmt.Errorf("begin set reaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	place, err := s.loadPlace(ctx, tx, placeID)
	if err != nil {
		return ReactionMutationResult{}, err
	}
	visible, err := s.canAccess(ctx, tx, place, actor)
	if err != nil {
		return ReactionMutationResult{}, err
	}
	if !visible {
		return ReactionMutationResult{}, ErrPlaceNotFound
	}
	msg, err := lockMessage(ctx, tx, placeID, messageID)
	if err != nil {
		return ReactionMutationResult{}, err
	}
	if msg.Deleted {
		return ReactionMutationResult{}, ErrMessageDeleted
	}

	if reacted {
		if _, err := tx.Exec(ctx,
			`INSERT INTO message_reactions (message_id, member_kind, member_id, emoji)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT (message_id, member_kind, member_id, emoji) DO NOTHING`,
			messageID, actor.Kind, actor.ID, emoji); err != nil {
			return ReactionMutationResult{}, fmt.Errorf("add reaction: %w", err)
		}
	} else if _, err := tx.Exec(ctx,
		`DELETE FROM message_reactions
		 WHERE message_id = $1 AND member_kind = $2 AND member_id = $3 AND emoji = $4`,
		messageID, actor.Kind, actor.ID, emoji); err != nil {
		return ReactionMutationResult{}, fmt.Errorf("remove reaction: %w", err)
	}

	reactions, err := loadSnapshot(ctx, tx, messageID)
	if err != nil {
		return ReactionMutationResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ReactionMutationResult{}, fmt.Errorf("commit set reaction: %w", err)
	}
	return ReactionMutationResult{
		MessageID: messageID,
		Reactions: reactions,
		Reacted:   reacted,
	}, nil
}

type reactionPublishLock struct {
	mu   sync.Mutex
	refs int
}

// lockReactionPublish keeps commit→Hub.Publish ordered for one message while
// leaving unrelated messages fully concurrent.
func (s *Server) lockReactionPublish(messageID string) func() {
	s.reactionLocksMu.Lock()
	if s.reactionLocks == nil {
		s.reactionLocks = make(map[string]*reactionPublishLock)
	}
	entry := s.reactionLocks[messageID]
	if entry == nil {
		entry = &reactionPublishLock{}
		s.reactionLocks[messageID] = entry
	}
	entry.refs++
	s.reactionLocksMu.Unlock()

	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		s.reactionLocksMu.Lock()
		entry.refs--
		if entry.refs == 0 && s.reactionLocks[messageID] == entry {
			delete(s.reactionLocks, messageID)
		}
		s.reactionLocksMu.Unlock()
	}
}

func (s *Server) setReaction(
	ctx context.Context,
	placeID, messageID string,
	actor ParticipantRef,
	emoji string,
	reacted bool,
) (ReactionMutationResult, error) {
	unlock := s.lockReactionPublish(messageID)
	defer unlock()

	result, err := s.Store.SetReaction(ctx, placeID, messageID, actor, emoji, reacted)
	if err != nil {
		return ReactionMutationResult{}, err
	}
	if s.Hub != nil {
		update := reactionUpdateToWire(result.MessageID, result.Reactions)
		s.Hub.Publish(ctx, Event{
			Type: EventReactionUpdated, PlaceID: placeID, Reaction: &update,
		})
	}
	return result, nil
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

func loadReactionSnapshot(
	ctx context.Context,
	tx pgx.Tx,
	messageID string,
) ([]ReactionSummary, error) {
	rows, err := tx.Query(ctx,
		`SELECT emoji, member_kind, member_id
		 FROM message_reactions
		 WHERE message_id = $1
		 ORDER BY created_at, member_kind, member_id`, messageID)
	if err != nil {
		return nil, fmt.Errorf("query reaction snapshot: %w", err)
	}
	defer rows.Close()
	summaries := make([]ReactionSummary, 0)
	indexByEmoji := make(map[string]int)
	for rows.Next() {
		var emoji, kind, id string
		if err := rows.Scan(&emoji, &kind, &id); err != nil {
			return nil, fmt.Errorf("scan reaction snapshot: %w", err)
		}
		participant := ParticipantRef{Kind: ParticipantKind(kind), ID: id}
		index, ok := indexByEmoji[emoji]
		if !ok {
			index = len(summaries)
			indexByEmoji[emoji] = index
			summaries = append(summaries, ReactionSummary{Emoji: emoji})
		}
		summaries[index].Participants = append(summaries[index].Participants, participant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate reaction snapshot: %w", err)
	}
	return summaries, nil
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
