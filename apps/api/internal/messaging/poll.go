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

const (
	MaxPollQuestionChars = 500
	MaxPollOptionChars   = 200
	MinPollOptions       = 2
	MaxPollOptions       = 10
	MaxPollDuration      = MaxReplyLaterDelay
)

var (
	ErrInvalidPoll        = errors.New("invalid poll")
	ErrPollNotFound       = errors.New("message has no poll")
	ErrPollClosed         = errors.New("poll is closed")
	ErrPollOptionNotFound = errors.New("poll option not found")
	ErrPollSingleChoice   = errors.New("poll accepts one choice")
)

type PollOption struct {
	OptionID string
	Text     string
	Voters   []ParticipantRef
}

type Poll struct {
	Question   string
	AllowMulti bool
	ClosesAt   *time.Time
	Options    []PollOption
}

func (p Poll) Closed(now time.Time) bool {
	return p.ClosesAt != nil && !now.Before(*p.ClosesAt)
}

type PollInput struct {
	Question   string
	AllowMulti bool
	ClosesAt   *time.Time
	Options    []string
}

func pollMatchesInput(poll *Poll, input *PollInput) bool {
	if poll == nil || input == nil {
		return poll == nil && input == nil
	}
	if poll.Question != input.Question || poll.AllowMulti != input.AllowMulti || len(poll.Options) != len(input.Options) {
		return false
	}
	if (poll.ClosesAt == nil) != (input.ClosesAt == nil) ||
		(poll.ClosesAt != nil && !poll.ClosesAt.Equal(*input.ClosesAt)) {
		return false
	}
	for i, option := range poll.Options {
		if option.Text != input.Options[i] {
			return false
		}
	}
	return true
}

func (in *PollInput) Validate(now time.Time) error {
	if err := in.validateFields(); err != nil {
		return err
	}
	return in.validateDeadline(now)
}

// validateFields normalizes and validates the durable poll payload. It is
// deliberately separate from validateDeadline: an idempotent retry must still
// be able to compare its normalized request with the receipt after the poll
// has closed.
func (in *PollInput) validateFields() error {
	if in == nil {
		return ErrInvalidPoll
	}
	in.Question = strings.TrimSpace(in.Question)
	if in.Question == "" || strings.ContainsRune(in.Question, '\x00') || utf8.RuneCountInString(in.Question) > MaxPollQuestionChars {
		return fmt.Errorf("%w: question must be 1..%d characters", ErrInvalidPoll, MaxPollQuestionChars)
	}
	options := make([]string, 0, len(in.Options))
	seen := map[string]bool{}
	for _, option := range in.Options {
		option = strings.TrimSpace(option)
		if option == "" || strings.ContainsRune(option, '\x00') || utf8.RuneCountInString(option) > MaxPollOptionChars {
			return fmt.Errorf("%w: option must be 1..%d characters", ErrInvalidPoll, MaxPollOptionChars)
		}
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
	return nil
}

func (in *PollInput) validateDeadline(now time.Time) error {
	if in.ClosesAt != nil {
		if !in.ClosesAt.After(now) || in.ClosesAt.After(now.Add(MaxPollDuration)) {
			return fmt.Errorf("%w: closing time is outside the accepted range", ErrInvalidPoll)
		}
	}
	return nil
}

func insertPoll(ctx context.Context, tx pgx.Tx, workspaceID, messageID string, in PollInput) (*Poll, error) {
	if _, err := tx.Exec(ctx, `
		INSERT INTO message_polls (workspace_id, message_id, question, allow_multi, closes_at)
		VALUES ($1, $2, $3, $4, $5)`, workspaceID, messageID, in.Question, in.AllowMulti, in.ClosesAt); err != nil {
		return nil, fmt.Errorf("insert poll: %w", err)
	}
	poll := &Poll{Question: in.Question, AllowMulti: in.AllowMulti, ClosesAt: in.ClosesAt}
	for index, text := range in.Options {
		option := PollOption{OptionID: newUUIDv7(), Text: text}
		if _, err := tx.Exec(ctx, `
			INSERT INTO message_poll_options (workspace_id, message_id, option_id, text, ord)
			VALUES ($1, $2, $3, $4, $5)`, workspaceID, messageID, option.OptionID, text, index); err != nil {
			return nil, fmt.Errorf("insert poll option: %w", err)
		}
		poll.Options = append(poll.Options, option)
	}
	return poll, nil
}

// VotePoll replaces the authenticated actor's complete choice. Empty withdraws.
func (s *ScopedStore) VotePoll(ctx context.Context, placeID, messageID string, optionIDs []string) (Message, error) {
	if len(optionIDs) > MaxPollOptions {
		return Message{}, ErrInvalidPoll
	}
	seen := map[string]bool{}
	for _, optionID := range optionIDs {
		if optionID == "" || seen[optionID] {
			return Message{}, ErrInvalidPoll
		}
		seen[optionID] = true
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Message{}, fmt.Errorf("begin scoped poll vote: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeMutationInTx(ctx, tx); err != nil {
		return Message{}, err
	}
	place, err := s.loadScopedPlace(ctx, tx, placeID)
	if err != nil {
		return Message{}, err
	}
	access, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor)
	if err != nil {
		return Message{}, err
	}
	message, err := lockMessageScoped(ctx, tx, s.Scope.WorkspaceID, placeID, messageID)
	if err != nil {
		return Message{}, err
	}
	if message.Seq < access.VisibleFromSeq {
		return Message{}, ErrMessageNotFound
	}
	if message.Deleted {
		return Message{}, ErrMessageDeleted
	}
	var allowMulti bool
	var closesAt *time.Time
	err = tx.QueryRow(ctx, `
		SELECT allow_multi, closes_at FROM message_polls
		WHERE workspace_id=$1 AND message_id=$2 FOR UPDATE`, s.Scope.WorkspaceID, messageID).Scan(&allowMulti, &closesAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Message{}, ErrPollNotFound
	}
	if err != nil {
		return Message{}, fmt.Errorf("load scoped poll: %w", err)
	}
	if closesAt != nil && !time.Now().Before(*closesAt) {
		return Message{}, ErrPollClosed
	}
	if !allowMulti && len(optionIDs) > 1 {
		return Message{}, ErrPollSingleChoice
	}
	for _, optionID := range optionIDs {
		var belongs bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM message_poll_options
			 WHERE workspace_id=$1 AND message_id=$2 AND option_id=$3)`,
			s.Scope.WorkspaceID, messageID, optionID).Scan(&belongs); err != nil {
			return Message{}, fmt.Errorf("check scoped poll option: %w", err)
		}
		if !belongs {
			return Message{}, ErrPollOptionNotFound
		}
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM message_poll_votes
		WHERE workspace_id=$1 AND message_id=$2 AND voter_kind=$3 AND voter_id=$4`,
		s.Scope.WorkspaceID, messageID, s.Scope.Actor.Kind, s.Scope.Actor.ID); err != nil {
		return Message{}, fmt.Errorf("clear scoped poll votes: %w", err)
	}
	for _, optionID := range optionIDs {
		if _, err := tx.Exec(ctx, `
			INSERT INTO message_poll_votes
				(workspace_id, message_id, option_id, voter_kind, voter_id)
			VALUES ($1, $2, $3, $4, $5)`, s.Scope.WorkspaceID, messageID,
			optionID, s.Scope.Actor.Kind, s.Scope.Actor.ID); err != nil {
			return Message{}, fmt.Errorf("insert scoped poll vote: %w", err)
		}
	}
	parts := []Message{message}
	if err := attachMessagePartsWith(ctx, tx, s.Scope.WorkspaceID, parts); err != nil {
		return Message{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Message{}, fmt.Errorf("commit scoped poll vote: %w", err)
	}
	return parts[0], nil
}

func attachPollsWith(ctx context.Context, q querier, workspaceID string, messages []Message) error {
	if len(messages) == 0 {
		return nil
	}
	ids := make([]string, len(messages))
	index := make(map[string]int, len(messages))
	for i, message := range messages {
		ids[i], index[message.MessageID] = message.MessageID, i
	}
	rows, err := q.Query(ctx, `
		SELECT message_id, question, allow_multi, closes_at FROM message_polls
		WHERE workspace_id=$1 AND message_id=ANY($2)`, workspaceID, ids)
	if err != nil {
		return fmt.Errorf("query scoped polls: %w", err)
	}
	for rows.Next() {
		var messageID string
		poll := &Poll{}
		if err := rows.Scan(&messageID, &poll.Question, &poll.AllowMulti, &poll.ClosesAt); err != nil {
			rows.Close()
			return fmt.Errorf("scan scoped poll: %w", err)
		}
		messages[index[messageID]].Poll = poll
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	optionIndex := map[string]struct{ message, option int }{}
	rows, err = q.Query(ctx, `
		SELECT message_id, option_id, text FROM message_poll_options
		WHERE workspace_id=$1 AND message_id=ANY($2) ORDER BY message_id, ord`, workspaceID, ids)
	if err != nil {
		return fmt.Errorf("query scoped poll options: %w", err)
	}
	for rows.Next() {
		var messageID, optionID, text string
		if err := rows.Scan(&messageID, &optionID, &text); err != nil {
			rows.Close()
			return err
		}
		i := index[messageID]
		if messages[i].Poll == nil {
			continue
		}
		messages[i].Poll.Options = append(messages[i].Poll.Options, PollOption{OptionID: optionID, Text: text})
		optionIndex[optionID] = struct{ message, option int }{i, len(messages[i].Poll.Options) - 1}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	rows, err = q.Query(ctx, `
		SELECT option_id, voter_kind, voter_id FROM message_poll_votes
		WHERE workspace_id=$1 AND message_id=ANY($2)
		ORDER BY created_at, voter_kind, voter_id`, workspaceID, ids)
	if err != nil {
		return fmt.Errorf("query scoped poll votes: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var optionID string
		var voter ParticipantRef
		if err := rows.Scan(&optionID, &voter.Kind, &voter.ID); err != nil {
			return err
		}
		at, ok := optionIndex[optionID]
		if ok {
			option := &messages[at.message].Poll.Options[at.option]
			option.Voters = append(option.Voters, voter)
		}
	}
	return rows.Err()
}
