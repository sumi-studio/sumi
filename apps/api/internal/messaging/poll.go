package messaging

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

// Poll bounds, matching the schema CHECKs. Two options is the smallest thing
// that is a question rather than an announcement; ten is where a poll stops
// being readable at a glance.
const (
	MaxPollQuestionChars = 500
	MaxPollOptionChars   = 200
	MinPollOptions       = 2
	MaxPollOptions       = 10
	// MaxPollDuration bounds a closing time, matching the reply-later bound on
	// relative durations the agent lane accepts.
	MaxPollDuration = MaxReplyLaterDelay
)

var (
	// ErrInvalidPoll covers every malformed poll shape: the caller sent a
	// question or option set that could not be a poll at all.
	ErrInvalidPoll = errors.New("invalid poll")
	// ErrPollNotFound is returned when a message carries no poll.
	ErrPollNotFound = errors.New("message has no poll")
	// ErrPollClosed is returned when the deadline has passed. After it, the
	// result stands: a poll that could be edited afterwards is not a record of
	// what people thought at the time.
	ErrPollClosed = errors.New("poll is closed")
	// ErrPollOptionNotFound is returned when a vote names an option that does
	// not belong to this poll.
	ErrPollOptionNotFound = errors.New("poll option not found")
	// ErrPollSingleChoice is returned when several options are named on a poll
	// that allows only one.
	ErrPollSingleChoice = errors.New("poll accepts one choice")
)

// PollOption is one choice with the participants who currently pick it.
// Voters are visible for the same reason reactions are: a shared decision is
// not a secret ballot, and seeing who is missing is how a group closes one.
type PollOption struct {
	OptionID string
	Text     string
	Voters   []ParticipantRef
}

// Poll is the question a message carries.
type Poll struct {
	Question   string
	AllowMulti bool
	ClosesAt   *time.Time
	Options    []PollOption
}

// Closed reports whether the deadline has passed at the given moment.
func (p Poll) Closed(now time.Time) bool {
	return p.ClosesAt != nil && !now.Before(*p.ClosesAt)
}

// PollInput is a poll as the sender states it. Option identity is minted by
// the server so a client can never address an option it did not receive.
type PollInput struct {
	Question   string
	AllowMulti bool
	ClosesAt   *time.Time
	Options    []string
}

// Validate normalizes and bounds the stated poll.
func (in *PollInput) Validate() error {
	in.Question = strings.TrimSpace(in.Question)
	if in.Question == "" || utf8.RuneCountInString(in.Question) > MaxPollQuestionChars {
		return fmt.Errorf("%w: question must be 1..%d characters", ErrInvalidPoll, MaxPollQuestionChars)
	}
	options := make([]string, 0, len(in.Options))
	seen := map[string]bool{}
	for _, option := range in.Options {
		option = strings.TrimSpace(option)
		if option == "" || utf8.RuneCountInString(option) > MaxPollOptionChars {
			return fmt.Errorf("%w: option must be 1..%d characters", ErrInvalidPoll, MaxPollOptionChars)
		}
		// Two identical choices cannot be told apart by a voter, so they are a
		// mistake rather than a preference.
		if seen[option] {
			return fmt.Errorf("%w: options must be distinct", ErrInvalidPoll)
		}
		seen[option] = true
		options = append(options, option)
	}
	if len(options) < MinPollOptions || len(options) > MaxPollOptions {
		return fmt.Errorf("%w: a poll needs %d..%d options", ErrInvalidPoll, MinPollOptions, MaxPollOptions)
	}
	in.Options = options
	if in.ClosesAt != nil {
		if !in.ClosesAt.After(time.Now()) {
			return fmt.Errorf("%w: closing time must be in the future", ErrInvalidPoll)
		}
		if in.ClosesAt.After(time.Now().Add(MaxPollDuration)) {
			return fmt.Errorf("%w: closing time is too far away", ErrInvalidPoll)
		}
	}
	return nil
}

// insertPoll writes the poll a message carries, inside the send transaction so
// a message never exists without the question it was sent to ask.
func insertPoll(ctx context.Context, tx pgx.Tx, messageID string, in PollInput) (*Poll, error) {
	if _, err := tx.Exec(ctx,
		`INSERT INTO message_polls (message_id, question, allow_multi, closes_at)
		 VALUES ($1, $2, $3, $4)`,
		messageID, in.Question, in.AllowMulti, in.ClosesAt); err != nil {
		return nil, fmt.Errorf("insert poll: %w", err)
	}
	poll := &Poll{Question: in.Question, AllowMulti: in.AllowMulti, ClosesAt: in.ClosesAt}
	for index, text := range in.Options {
		optionID := newUUIDv7()
		if _, err := tx.Exec(ctx,
			`INSERT INTO message_poll_options (option_id, message_id, text, ord)
			 VALUES ($1, $2, $3, $4)`,
			optionID, messageID, text, index); err != nil {
			return nil, fmt.Errorf("insert poll option: %w", err)
		}
		poll.Options = append(poll.Options, PollOption{OptionID: optionID, Text: text})
	}
	return poll, nil
}

// VotePoll replaces the voter's choice on one poll. Passing no options is how
// a vote is withdrawn — the same call, so "change your mind" and "take it
// back" are not two different capabilities. A single-choice poll rejects more
// than one option in the server, not only in the UI.
//
// The returned message carries the poll's committed state.
func (s *Store) VotePoll(ctx context.Context, placeID, messageID string, voter ParticipantRef, optionIDs []string) (Message, error) {
	if err := voter.Validate(); err != nil {
		return Message{}, err
	}
	if len(optionIDs) > MaxPollOptions {
		return Message{}, ErrInvalidPoll
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Message{}, fmt.Errorf("begin vote: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	place, err := s.loadPlace(ctx, tx, placeID)
	if err != nil {
		return Message{}, err
	}
	visible, err := s.canAccess(ctx, tx, place, voter)
	if err != nil {
		return Message{}, err
	}
	if !visible {
		return Message{}, ErrPlaceNotFound
	}
	msg, err := lockMessage(ctx, tx, placeID, messageID)
	if err != nil {
		return Message{}, err
	}
	if msg.Deleted {
		return Message{}, ErrMessageDeleted
	}

	var (
		allowMulti bool
		closesAt   *time.Time
	)
	err = tx.QueryRow(ctx,
		"SELECT allow_multi, closes_at FROM message_polls WHERE message_id = $1 FOR UPDATE",
		messageID).Scan(&allowMulti, &closesAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Message{}, ErrPollNotFound
	}
	if err != nil {
		return Message{}, fmt.Errorf("load poll: %w", err)
	}
	if closesAt != nil && !time.Now().Before(*closesAt) {
		return Message{}, ErrPollClosed
	}
	if !allowMulti && len(optionIDs) > 1 {
		return Message{}, ErrPollSingleChoice
	}
	// Every named option must belong to this poll: an id from another message
	// is a mistake, never a vote that silently lands elsewhere.
	for _, optionID := range optionIDs {
		var belongs bool
		if err := tx.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM message_poll_options WHERE option_id = $1 AND message_id = $2)",
			optionID, messageID).Scan(&belongs); err != nil {
			return Message{}, fmt.Errorf("check poll option: %w", err)
		}
		if !belongs {
			return Message{}, ErrPollOptionNotFound
		}
	}
	// Replace rather than merge: the voter's whole choice is restated, so
	// "one vote per poll" holds for single-choice polls by construction.
	if _, err := tx.Exec(ctx,
		`DELETE FROM message_poll_votes
		 WHERE message_id = $1 AND voter_kind = $2 AND voter_id = $3`,
		messageID, voter.Kind, voter.ID); err != nil {
		return Message{}, fmt.Errorf("clear votes: %w", err)
	}
	for _, optionID := range optionIDs {
		if _, err := tx.Exec(ctx,
			`INSERT INTO message_poll_votes (option_id, message_id, voter_kind, voter_id)
			 VALUES ($1, $2, $3, $4)`,
			optionID, messageID, voter.Kind, voter.ID); err != nil {
			return Message{}, fmt.Errorf("insert vote: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Message{}, fmt.Errorf("commit vote: %w", err)
	}
	voted := []Message{msg}
	if err := s.attachMentions(ctx, voted); err != nil {
		return Message{}, err
	}
	if err := s.attachAttachments(ctx, voted); err != nil {
		return Message{}, err
	}
	if err := s.attachReactions(ctx, voted); err != nil {
		return Message{}, err
	}
	if err := s.attachPolls(ctx, voted); err != nil {
		return Message{}, err
	}
	return voted[0], nil
}

// attachPolls loads polls, options and votes for the given messages, mirroring
// attachReactions. Three queries rather than one join keeps the aggregation
// obvious and bounded by the message page, not by the vote count.
func (s *Store) attachPolls(ctx context.Context, messages []Message) error {
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
		`SELECT message_id, question, allow_multi, closes_at
		 FROM message_polls WHERE message_id = ANY($1)`, ids)
	if err != nil {
		return fmt.Errorf("query polls: %w", err)
	}
	polled := map[string]bool{}
	for rows.Next() {
		var (
			messageID string
			poll      Poll
		)
		if err := rows.Scan(&messageID, &poll.Question, &poll.AllowMulti, &poll.ClosesAt); err != nil {
			rows.Close()
			return fmt.Errorf("scan poll: %w", err)
		}
		messages[index[messageID]].Poll = &poll
		polled[messageID] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate polls: %w", err)
	}
	rows.Close()
	if len(polled) == 0 {
		return nil
	}

	optionIndex := map[string]struct{ message, option int }{}
	rows, err = s.pool.Query(ctx,
		`SELECT message_id, option_id, text FROM message_poll_options
		 WHERE message_id = ANY($1) ORDER BY message_id, ord`, ids)
	if err != nil {
		return fmt.Errorf("query poll options: %w", err)
	}
	for rows.Next() {
		var messageID, optionID, text string
		if err := rows.Scan(&messageID, &optionID, &text); err != nil {
			rows.Close()
			return fmt.Errorf("scan poll option: %w", err)
		}
		i := index[messageID]
		poll := messages[i].Poll
		if poll == nil {
			continue
		}
		poll.Options = append(poll.Options, PollOption{OptionID: optionID, Text: text})
		optionIndex[optionID] = struct{ message, option int }{i, len(poll.Options) - 1}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate poll options: %w", err)
	}
	rows.Close()

	rows, err = s.pool.Query(ctx,
		`SELECT option_id, voter_kind, voter_id FROM message_poll_votes
		 WHERE message_id = ANY($1) ORDER BY created_at, voter_kind, voter_id`, ids)
	if err != nil {
		return fmt.Errorf("query poll votes: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var optionID, kind, id string
		if err := rows.Scan(&optionID, &kind, &id); err != nil {
			return fmt.Errorf("scan poll vote: %w", err)
		}
		at, ok := optionIndex[optionID]
		if !ok {
			continue
		}
		option := &messages[at.message].Poll.Options[at.option]
		option.Voters = append(option.Voters, ParticipantRef{Kind: ParticipantKind(kind), ID: id})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate poll votes: %w", err)
	}
	return nil
}
