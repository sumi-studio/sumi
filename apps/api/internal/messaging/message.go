package messaging

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Urgency values (契約ドラフト: メッセージ単位の緊急度).
const (
	UrgencyUrgent = "urgent"
	UrgencyNormal = "normal"
	UrgencyFYI    = "fyi"
)

// MaxContentBytes matches the schema CHECK on messages.content.
const MaxContentBytes = 65536

// messageContentFitsStorage is the byte-level invariant shared by every
// Messaging transport and PostgreSQL. A NUL byte is valid UTF-8 but cannot be
// stored in a PostgreSQL text column, so reject it before mutation.
func messageContentFitsStorage(content string) bool {
	return len(content) <= MaxContentBytes && !strings.ContainsRune(content, '\x00')
}

// MaxHistoryLimit bounds one History page.
const MaxHistoryLimit = 200

const defaultHistoryLimit = 50

// Message is one durable event of a place. Deleted messages are tombstones:
// content is empty, the fact and the seq remain.
type Message struct {
	MessageID   string
	PlaceID     string
	Seq         int64
	Author      ParticipantRef
	Content     string
	Urgency     string
	Mentions    []ParticipantRef
	Reactions   []ReactionSummary
	ReplyTo     string // empty when not a reply
	ClientNonce string
	CreatedAt   time.Time
	EditedAt    *time.Time
	Deleted     bool
}

// AppendInput is a send request. Mentions are deliberately absent: the server
// resolves them from content and active membership at admission time and never
// accepts them as a client assertion.
type AppendInput struct {
	PlaceID     string
	Author      ParticipantRef
	Content     string
	Urgency     string // empty means normal
	ReplyTo     string // optional message_id in the same place
	ClientNonce string
}

type HistoryOptions struct {
	BeforeSeq int64
	Limit     int
}

type UnreadSummary struct {
	Place        Place
	LastReadSeq  int64
	UnreadCount  int64
	MentionCount int64
}

// AppendMessage commits a message to a place, allocating the next place seq in
// the same transaction. It is idempotent on (place, author, client_nonce):
// retrying a send returns the originally committed message with created=false.
// Authorization is the v0 rule from canAccess; a place the author cannot see
// is reported as ErrPlaceNotFound.
// --- helpers ---

func scanMessages(rows pgx.Rows) ([]Message, error) {
	defer rows.Close()
	var out []Message
	for rows.Next() {
		var (
			m          Message
			authorKind string
			content    *string
			replyTo    *string
			deletedAt  *time.Time
		)
		if err := rows.Scan(&m.MessageID, &m.PlaceID, &m.Seq, &authorKind, &m.Author.ID,
			&content, &m.Urgency, &replyTo, &m.ClientNonce,
			&m.CreatedAt, &m.EditedAt, &deletedAt); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		m.Author.Kind = ParticipantKind(authorKind)
		if content != nil {
			m.Content = *content
		}
		if replyTo != nil {
			m.ReplyTo = *replyTo
		}
		m.Deleted = deletedAt != nil
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate messages: %w", err)
	}
	return out, nil
}

func insertMentions(ctx context.Context, tx pgx.Tx, messageID string, mentions []ParticipantRef) error {
	for _, mention := range mentions {
		if _, err := tx.Exec(ctx,
			"INSERT INTO message_mentions (message_id, member_kind, member_id) VALUES ($1, $2, $3)",
			messageID, mention.Kind, mention.ID); err != nil {
			return fmt.Errorf("insert mention: %w", err)
		}
	}
	return nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
