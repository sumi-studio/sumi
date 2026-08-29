package messaging

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
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
	// Revision advances once for every committed whole-choice replacement.
	// Live consumers use it to reject snapshots delivered out of commit order.
	Revision int64
	Options  []PollOption
}

func (p Poll) Closed(now time.Time) bool {
	return p.ClosesAt != nil && !now.Before(*p.ClosesAt)
}

type PollInput struct {
	Question   string
	AllowMulti bool
	ClosesAt   *time.Time
	// RelativeClosesInMinutes preserves the local agent's original request in
	// the nonce digest. ClosesAt still carries the server-clock-derived durable
	// instant and is deliberately omitted from that relative digest projection.
	RelativeClosesInMinutes uint32
	Options                 []string
}

func (in *PollInput) Validate(now time.Time) error {
	if err := in.validateFields(); err != nil {
		return err
	}
	in.resolveRelativeDeadline(now)
	return in.validateDeadline(now)
}

// validateFields canonicalizes the durable text independently of the closing
// time. Nonce replay must be able to compare a normalized request with its
// receipt even after the poll has closed.
func (in *PollInput) validateFields() error {
	if in == nil {
		return ErrInvalidPoll
	}
	if !utf8.ValidString(in.Question) {
		return fmt.Errorf("%w: question is not valid UTF-8", ErrInvalidPoll)
	}
	in.Question = strings.TrimSpace(in.Question)
	if in.Question == "" || strings.ContainsRune(in.Question, '\x00') ||
		utf8.RuneCountInString(in.Question) > MaxPollQuestionChars {
		return fmt.Errorf("%w: question must be 1..%d code points", ErrInvalidPoll, MaxPollQuestionChars)
	}

	options := make([]string, 0, len(in.Options))
	seen := make(map[string]struct{}, len(in.Options))
	for _, raw := range in.Options {
		if !utf8.ValidString(raw) {
			return fmt.Errorf("%w: option is not valid UTF-8", ErrInvalidPoll)
		}
		option := strings.TrimSpace(raw)
		if option == "" || strings.ContainsRune(option, '\x00') ||
			utf8.RuneCountInString(option) > MaxPollOptionChars {
			return fmt.Errorf("%w: option must be 1..%d code points", ErrInvalidPoll, MaxPollOptionChars)
		}
		if _, duplicate := seen[option]; duplicate {
			return fmt.Errorf("%w: options must be distinct after trimming", ErrInvalidPoll)
		}
		seen[option] = struct{}{}
		options = append(options, option)
	}
	if len(options) < MinPollOptions || len(options) > MaxPollOptions {
		return fmt.Errorf("%w: a poll needs %d..%d options", ErrInvalidPoll, MinPollOptions, MaxPollOptions)
	}
	in.Options = options

	if in.ClosesAt != nil {
		// PostgreSQL timestamptz stores microseconds. Normalize the request to the
		// same UTC instant before digesting it so equivalent offsets and discarded
		// sub-microsecond precision do not create false nonce conflicts.
		moment := in.ClosesAt.UTC().Truncate(time.Microsecond)
		in.ClosesAt = &moment
	}
	maxRelative := uint32(MaxPollDuration / time.Minute)
	if in.RelativeClosesInMinutes > maxRelative {
		return fmt.Errorf("%w: relative closing time is outside the accepted range", ErrInvalidPoll)
	}
	return nil
}

func (in *PollInput) resolveRelativeDeadline(now time.Time) {
	if in != nil && in.RelativeClosesInMinutes > 0 {
		moment := now.Add(time.Duration(in.RelativeClosesInMinutes) * time.Minute).
			UTC().Truncate(time.Microsecond)
		in.ClosesAt = &moment
	}
}

func (in *PollInput) validateDeadline(now time.Time) error {
	if in.ClosesAt != nil &&
		(!in.ClosesAt.After(now) || in.ClosesAt.After(now.Add(MaxPollDuration))) {
		return fmt.Errorf("%w: closing time is outside the accepted range", ErrInvalidPoll)
	}
	return nil
}

func insertPoll(ctx context.Context, tx pgx.Tx, messageID string, in PollInput) (*Poll, error) {
	if _, err := tx.Exec(ctx, `
		INSERT INTO message_polls (message_id, question, allow_multi, closes_at)
		VALUES ($1, $2, $3, $4)`, messageID, in.Question, in.AllowMulti, in.ClosesAt); err != nil {
		return nil, fmt.Errorf("insert poll: %w", err)
	}
	poll := &Poll{
		Question: in.Question, AllowMulti: in.AllowMulti, ClosesAt: in.ClosesAt,
		Options: make([]PollOption, 0, len(in.Options)),
	}
	for index, text := range in.Options {
		option := PollOption{OptionID: newUUIDv7(), Text: text, Voters: []ParticipantRef{}}
		if _, err := tx.Exec(ctx, `
			INSERT INTO message_poll_options (option_id, message_id, text, ord)
			VALUES ($1, $2, $3, $4)`, option.OptionID, messageID, text, index); err != nil {
			return nil, fmt.Errorf("insert poll option: %w", err)
		}
		poll.Options = append(poll.Options, option)
	}
	return poll, nil
}

// VotePoll replaces the authenticated actor's complete choice. An empty list
// withdraws every choice and still commits the next authoritative revision.
func (s *ScopedStore) VotePoll(ctx context.Context, placeID, messageID string, optionIDs []string) (Message, error) {
	return s.votePollWithClock(ctx, placeID, messageID, optionIDs, time.Now)
}

// votePollWithClock keeps the boundary condition deterministic in tests. The
// clock is read only after both the carrier message and poll are locked, so a
// vote that waited for another mutation cannot use a stale pre-wait instant.
func (s *ScopedStore) votePollWithClock(
	ctx context.Context,
	placeID, messageID string,
	optionIDs []string,
	now func() time.Time,
) (Message, error) {
	if now == nil || len(optionIDs) > MaxPollOptions {
		return Message{}, ErrInvalidPoll
	}
	seen := make(map[string]struct{}, len(optionIDs))
	for _, optionID := range optionIDs {
		if !validPollOptionID(optionID) {
			return Message{}, ErrPollOptionNotFound
		}
		if _, duplicate := seen[optionID]; duplicate {
			return Message{}, ErrInvalidPoll
		}
		seen[optionID] = struct{}{}
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
		SELECT allow_multi, closes_at
		FROM message_polls WHERE message_id = $1 FOR UPDATE`, messageID,
	).Scan(&allowMulti, &closesAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Message{}, ErrPollNotFound
	}
	if err != nil {
		return Message{}, fmt.Errorf("lock scoped poll: %w", err)
	}
	if closesAt != nil && !now().Before(*closesAt) {
		return Message{}, ErrPollClosed
	}
	if !allowMulti && len(optionIDs) > 1 {
		return Message{}, ErrPollSingleChoice
	}
	if len(optionIDs) > 0 {
		var matched int
		if err := tx.QueryRow(ctx, `
			SELECT count(*) FROM message_poll_options
			WHERE message_id = $1 AND option_id = ANY($2)`, messageID, optionIDs,
		).Scan(&matched); err != nil {
			return Message{}, fmt.Errorf("check scoped poll options: %w", err)
		}
		if matched != len(optionIDs) {
			return Message{}, ErrPollOptionNotFound
		}
	}

	if _, err := tx.Exec(ctx, `
		DELETE FROM message_poll_votes v
		USING message_poll_options o
		WHERE v.option_id = o.option_id AND o.message_id = $1
		  AND v.voter_kind = $2 AND v.voter_id = $3`,
		messageID, s.Scope.Actor.Kind, s.Scope.Actor.ID); err != nil {
		return Message{}, fmt.Errorf("clear scoped poll votes: %w", err)
	}
	for _, optionID := range optionIDs {
		if _, err := tx.Exec(ctx, `
			INSERT INTO message_poll_votes (option_id, voter_kind, voter_id)
			VALUES ($1, $2, $3)`, optionID, s.Scope.Actor.Kind, s.Scope.Actor.ID); err != nil {
			return Message{}, fmt.Errorf("insert scoped poll vote: %w", err)
		}
	}
	var revision int64
	if err := tx.QueryRow(ctx, `
		UPDATE message_polls SET revision = revision + 1
		WHERE message_id = $1 RETURNING revision`, messageID).Scan(&revision); err != nil {
		return Message{}, fmt.Errorf("advance scoped poll revision: %w", err)
	}

	parts := []Message{message}
	if err := attachMessagePartsWith(ctx, tx, parts); err != nil {
		return Message{}, err
	}
	if parts[0].Poll == nil || parts[0].Poll.Revision != revision {
		return Message{}, fmt.Errorf("load scoped poll vote projection at revision %d", revision)
	}
	if err := tx.Commit(ctx); err != nil {
		return Message{}, fmt.Errorf("commit scoped poll vote: %w", err)
	}
	return parts[0], nil
}

func validPollOptionID(id string) bool {
	parsed, err := uuid.Parse(id)
	return err == nil && parsed.Version() == 7 && parsed.String() == id
}

func attachPollsWith(ctx context.Context, q querier, messages []Message) error {
	if len(messages) == 0 {
		return nil
	}
	ids := make([]string, len(messages))
	messageIndex := make(map[string]int, len(messages))
	for i, message := range messages {
		ids[i] = message.MessageID
		messageIndex[message.MessageID] = i
	}

	rows, err := q.Query(ctx, `
		SELECT message_id, question, allow_multi, closes_at, revision
		FROM message_polls WHERE message_id = ANY($1)`, ids)
	if err != nil {
		return fmt.Errorf("query polls: %w", err)
	}
	for rows.Next() {
		var messageID string
		poll := &Poll{Options: []PollOption{}}
		if err := rows.Scan(&messageID, &poll.Question, &poll.AllowMulti, &poll.ClosesAt, &poll.Revision); err != nil {
			rows.Close()
			return fmt.Errorf("scan poll: %w", err)
		}
		messages[messageIndex[messageID]].Poll = poll
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate polls: %w", err)
	}
	rows.Close()

	type optionLocation struct{ message, option int }
	optionIndex := make(map[string]optionLocation)
	rows, err = q.Query(ctx, `
		SELECT message_id, option_id, text
		FROM message_poll_options
		WHERE message_id = ANY($1)
		ORDER BY message_id, ord`, ids)
	if err != nil {
		return fmt.Errorf("query poll options: %w", err)
	}
	for rows.Next() {
		var messageID, optionID, text string
		if err := rows.Scan(&messageID, &optionID, &text); err != nil {
			rows.Close()
			return fmt.Errorf("scan poll option: %w", err)
		}
		message := &messages[messageIndex[messageID]]
		if message.Poll == nil {
			continue
		}
		message.Poll.Options = append(message.Poll.Options, PollOption{
			OptionID: optionID, Text: text, Voters: []ParticipantRef{},
		})
		optionIndex[optionID] = optionLocation{
			message: messageIndex[messageID], option: len(message.Poll.Options) - 1,
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate poll options: %w", err)
	}
	rows.Close()

	rows, err = q.Query(ctx, `
		SELECT v.option_id, v.voter_kind, v.voter_id
		FROM message_poll_options o
		JOIN message_poll_votes v ON v.option_id = o.option_id
		WHERE o.message_id = ANY($1)
		ORDER BY o.message_id, o.ord, v.created_at, v.voter_kind, v.voter_id`, ids)
	if err != nil {
		return fmt.Errorf("query poll votes: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var optionID string
		var voter ParticipantRef
		if err := rows.Scan(&optionID, &voter.Kind, &voter.ID); err != nil {
			return fmt.Errorf("scan poll vote: %w", err)
		}
		location, ok := optionIndex[optionID]
		if ok {
			option := &messages[location.message].Poll.Options[location.option]
			option.Voters = append(option.Voters, voter)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate poll votes: %w", err)
	}
	return nil
}
