package messaging

import (
	"context"
	"errors"
	"fmt"
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
	Attachments []Attachment
	ReplyTo     string // empty when not a reply
	ClientNonce string
	CreatedAt   time.Time
	EditedAt    *time.Time
	Deleted     bool
}

// AppendInput is a send request. Mentions are deliberately absent: the server
// resolves them from content and active membership at admission time and never
// accepts them as a client assertion. AttachmentIDs, by contrast, are a client
// assertion the store verifies: only the author's own still-unbound uploads can
// be bound to the message.
type AppendInput struct {
	PlaceID       string
	Author        ParticipantRef
	Content       string
	Urgency       string // empty means normal
	ReplyTo       string // optional message_id in the same place
	ClientNonce   string
	AttachmentIDs []string
}

// AppendMessage commits a message to a place, allocating the next place seq in
// the same transaction. It is idempotent on (place, author, client_nonce):
// retrying a send returns the originally committed message with created=false.
// Authorization is the v0 rule from canAccess; a place the author cannot see
// is reported as ErrPlaceNotFound.
func (s *Store) AppendMessage(ctx context.Context, in AppendInput) (Message, bool, error) {
	if err := in.Author.Validate(); err != nil {
		return Message{}, false, err
	}
	switch in.Urgency {
	case "":
		in.Urgency = UrgencyNormal
	case UrgencyUrgent, UrgencyNormal, UrgencyFYI:
	default:
		return Message{}, false, fmt.Errorf("unknown urgency %q", in.Urgency)
	}
	// A message may be attachments only: sending an image without a caption is
	// an ordinary thing to do. Empty and attachment-less stays refused.
	if in.Content == "" && len(in.AttachmentIDs) == 0 {
		return Message{}, false, fmt.Errorf("content must not be empty")
	}
	if len(in.Content) > MaxContentBytes {
		return Message{}, false, fmt.Errorf("content exceeds %d bytes", MaxContentBytes)
	}
	if len(in.AttachmentIDs) > MaxAttachmentsPerMessage {
		return Message{}, false, ErrTooManyAttachments
	}
	if in.ClientNonce == "" || len(in.ClientNonce) > 128 {
		return Message{}, false, fmt.Errorf("client nonce must be 1..128 bytes")
	}

	msg, created, err := s.appendOnce(ctx, in)
	if err == nil || !isUniqueViolation(err) {
		return msg, created, err
	}
	// Two racing sends with the same nonce: the loser re-reads the winner's
	// commit and returns it as the idempotent result.
	existing, found, err := s.messageByNonce(ctx, s.pool, in)
	if err != nil {
		return Message{}, false, err
	}
	if !found {
		return Message{}, false, fmt.Errorf("idempotent re-read found no message for nonce %q", in.ClientNonce)
	}
	return existing, false, nil
}

func (s *Store) appendOnce(ctx context.Context, in AppendInput) (Message, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Message{}, false, fmt.Errorf("begin append: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	place, err := s.loadPlace(ctx, tx, in.PlaceID)
	if err != nil {
		return Message{}, false, err
	}
	canPost, err := s.canAccess(ctx, tx, place, in.Author)
	if err != nil {
		return Message{}, false, err
	}
	if !canPost {
		return Message{}, false, ErrPlaceNotFound
	}

	if existing, found, err := s.messageByNonce(ctx, tx, in); err != nil {
		return Message{}, false, err
	} else if found {
		if err := tx.Commit(ctx); err != nil {
			return Message{}, false, fmt.Errorf("commit idempotent append: %w", err)
		}
		return existing, false, nil
	}

	if in.ReplyTo != "" {
		var samePlace bool
		err := tx.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM messages WHERE message_id = $1 AND place_id = $2)",
			in.ReplyTo, in.PlaceID).Scan(&samePlace)
		if err != nil {
			return Message{}, false, fmt.Errorf("check reply target: %w", err)
		}
		if !samePlace {
			return Message{}, false, ErrMessageNotFound
		}
	}

	// Allocate the next seq. The row update locks the place for the rest of
	// the transaction, serializing concurrent appends so (place, seq) stays
	// dense and gapless.
	var seq int64
	if err := tx.QueryRow(ctx,
		"UPDATE places SET last_seq = last_seq + 1 WHERE place_id = $1 RETURNING last_seq",
		in.PlaceID).Scan(&seq); err != nil {
		return Message{}, false, fmt.Errorf("allocate seq: %w", err)
	}

	members, err := s.activeMembers(ctx, tx, place)
	if err != nil {
		return Message{}, false, err
	}
	mentions := resolveMentions(in.Content, members)

	msg := Message{
		MessageID:   newUUIDv7(),
		PlaceID:     in.PlaceID,
		Seq:         seq,
		Author:      in.Author,
		Content:     in.Content,
		Urgency:     in.Urgency,
		Mentions:    mentions,
		ReplyTo:     in.ReplyTo,
		ClientNonce: in.ClientNonce,
	}
	var replyTo *string
	if in.ReplyTo != "" {
		replyTo = &in.ReplyTo
	}
	if err := tx.QueryRow(ctx,
		`INSERT INTO messages
		   (message_id, place_id, seq, author_kind, author_id, content, urgency, reply_to, client_nonce)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 RETURNING created_at`,
		msg.MessageID, msg.PlaceID, msg.Seq, in.Author.Kind, in.Author.ID,
		msg.Content, msg.Urgency, replyTo, msg.ClientNonce).Scan(&msg.CreatedAt); err != nil {
		return Message{}, false, fmt.Errorf("insert message: %w", err)
	}
	if err := insertMentions(ctx, tx, msg.MessageID, mentions); err != nil {
		return Message{}, false, err
	}
	attachments, err := bindAttachments(ctx, tx, msg.MessageID, in.Author, in.AttachmentIDs)
	if err != nil {
		return Message{}, false, err
	}
	msg.Attachments = attachments
	if err := tx.Commit(ctx); err != nil {
		return Message{}, false, fmt.Errorf("commit append: %w", err)
	}
	return msg, true, nil
}

// History returns messages ascending by seq. BeforeSeq=0 pages from the
// latest; otherwise strictly older messages are returned, so repeated calls
// with the oldest returned seq walk backwards without overlap.
type HistoryOptions struct {
	BeforeSeq int64
	Limit     int
}

func (s *Store) History(ctx context.Context, placeID string, viewer ParticipantRef, opt HistoryOptions) ([]Message, error) {
	if _, err := s.PlaceFor(ctx, placeID, viewer); err != nil {
		return nil, err
	}
	limit := opt.Limit
	if limit <= 0 {
		limit = defaultHistoryLimit
	}
	if limit > MaxHistoryLimit {
		limit = MaxHistoryLimit
	}
	args := []any{placeID, limit}
	before := ""
	if opt.BeforeSeq > 0 {
		before = "AND seq < $3"
		args = append(args, opt.BeforeSeq)
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`SELECT message_id, place_id, seq, author_kind, author_id, content, urgency,
		        reply_to, client_nonce, created_at, edited_at, deleted_at
		 FROM messages WHERE place_id = $1 %s
		 ORDER BY seq DESC LIMIT $2`, before), args...)
	if err != nil {
		return nil, fmt.Errorf("query history: %w", err)
	}
	messages, err := scanMessages(rows)
	if err != nil {
		return nil, err
	}
	// Reverse to ascending.
	for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 {
		messages[i], messages[j] = messages[j], messages[i]
	}
	if err := s.attachMentions(ctx, messages); err != nil {
		return nil, err
	}
	if err := s.attachAttachments(ctx, messages); err != nil {
		return nil, err
	}
	return messages, nil
}

// MessagesSince returns up to limit messages with seq strictly greater than
// sinceSeq, ascending. It is the catch-up read behind WebSocket reconnect
// cursors (契約ドラフト v0.1: subscribeはcursor catch-upを持つ).
func (s *Store) MessagesSince(ctx context.Context, placeID string, viewer ParticipantRef, sinceSeq int64, limit int) ([]Message, error) {
	if _, err := s.PlaceFor(ctx, placeID, viewer); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > MaxHistoryLimit {
		limit = MaxHistoryLimit
	}
	rows, err := s.pool.Query(ctx,
		`SELECT message_id, place_id, seq, author_kind, author_id, content, urgency,
		        reply_to, client_nonce, created_at, edited_at, deleted_at
		 FROM messages WHERE place_id = $1 AND seq > $2
		 ORDER BY seq ASC LIMIT $3`, placeID, sinceSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("query messages since: %w", err)
	}
	messages, err := scanMessages(rows)
	if err != nil {
		return nil, err
	}
	if err := s.attachMentions(ctx, messages); err != nil {
		return nil, err
	}
	if err := s.attachAttachments(ctx, messages); err != nil {
		return nil, err
	}
	return messages, nil
}

// EditMessage replaces the content of the author's own live message. Mentions
// are re-resolved against active membership at edit time (the edit is a new
// admission).
func (s *Store) EditMessage(ctx context.Context, placeID, messageID string, author ParticipantRef, content string) (Message, error) {
	if err := author.Validate(); err != nil {
		return Message{}, err
	}
	if content == "" {
		return Message{}, fmt.Errorf("content must not be empty")
	}
	if len(content) > MaxContentBytes {
		return Message{}, fmt.Errorf("content exceeds %d bytes", MaxContentBytes)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Message{}, fmt.Errorf("begin edit: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	place, err := s.loadPlace(ctx, tx, placeID)
	if err != nil {
		return Message{}, err
	}
	visible, err := s.canAccess(ctx, tx, place, author)
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
	if msg.Author != author {
		return Message{}, ErrNotAuthor
	}
	if msg.Deleted {
		return Message{}, ErrMessageDeleted
	}

	members, err := s.activeMembers(ctx, tx, place)
	if err != nil {
		return Message{}, err
	}
	mentions := resolveMentions(content, members)
	var editedAt time.Time
	if err := tx.QueryRow(ctx,
		"UPDATE messages SET content = $1, edited_at = now() WHERE message_id = $2 RETURNING edited_at",
		content, messageID).Scan(&editedAt); err != nil {
		return Message{}, fmt.Errorf("update message: %w", err)
	}
	if _, err := tx.Exec(ctx,
		"DELETE FROM message_mentions WHERE message_id = $1", messageID); err != nil {
		return Message{}, fmt.Errorf("clear mentions: %w", err)
	}
	if err := insertMentions(ctx, tx, messageID, mentions); err != nil {
		return Message{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Message{}, fmt.Errorf("commit edit: %w", err)
	}
	msg.Content = content
	msg.Mentions = mentions
	msg.EditedAt = &editedAt
	// An edit rewrites text only; the attachments the message was sent with
	// stay part of it, so the returned message (and its echo) keeps them.
	edited := []Message{msg}
	if err := s.attachAttachments(ctx, edited); err != nil {
		return Message{}, err
	}
	return edited[0], nil
}

// DeleteMessage tombstones a message: content is removed, the fact and the seq
// remain. Allowed for the author, and — in channels — for workspace admins and
// owners (契約ドラフト: メッセージ削除は本人 + admin). Deleting an already
// deleted message is a no-op. The returned message is the tombstone.
func (s *Store) DeleteMessage(ctx context.Context, placeID, messageID string, actor ParticipantRef) (Message, error) {
	if err := actor.Validate(); err != nil {
		return Message{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Message{}, fmt.Errorf("begin delete: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	place, err := s.loadPlace(ctx, tx, placeID)
	if err != nil {
		return Message{}, err
	}
	visible, err := s.canAccess(ctx, tx, place, actor)
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
		return msg, tx.Commit(ctx)
	}
	if msg.Author != actor {
		allowed := false
		if place.Kind == PlaceChannel {
			_, role, err := s.workspaceMembership(ctx, tx, place.WorkspaceID, actor)
			if err != nil {
				return Message{}, err
			}
			allowed = role == RoleAdmin || role == RoleOwner
		}
		if !allowed {
			return Message{}, ErrForbidden
		}
	}
	if _, err := tx.Exec(ctx,
		"UPDATE messages SET content = NULL, deleted_at = now() WHERE message_id = $1",
		messageID); err != nil {
		return Message{}, fmt.Errorf("tombstone message: %w", err)
	}
	// A tombstone no longer addresses anyone; mention-unread must not count it.
	if _, err := tx.Exec(ctx,
		"DELETE FROM message_mentions WHERE message_id = $1", messageID); err != nil {
		return Message{}, fmt.Errorf("clear mentions: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Message{}, fmt.Errorf("commit delete: %w", err)
	}
	msg.Content = ""
	msg.Mentions = nil
	// A tombstone carries nothing: the attachment rows stay as the record of
	// what was sent, but they are no longer delivered or served (see
	// AttachmentForViewer).
	msg.Attachments = nil
	msg.Deleted = true
	return msg, nil
}

// ReadThrough advances the participant's read marker to seq. Idempotent and
// monotonic: a stale or repeated call can never move the marker backwards
// (凍結契約 v1 §3). seq beyond the place's latest is rejected so a marker can
// never claim to have read the future.
func (s *Store) ReadThrough(ctx context.Context, placeID string, p ParticipantRef, seq int64) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if seq < 0 {
		return fmt.Errorf("seq must be non-negative")
	}
	place, err := s.PlaceFor(ctx, placeID, p)
	if err != nil {
		return err
	}
	if seq > place.LastSeq {
		return ErrSeqBeyondLatest
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO read_markers (place_id, member_kind, member_id, last_read_seq)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (place_id, member_kind, member_id)
		 DO UPDATE SET last_read_seq = GREATEST(read_markers.last_read_seq, EXCLUDED.last_read_seq),
		               updated_at = now()`,
		placeID, p.Kind, p.ID, seq)
	if err != nil {
		return fmt.Errorf("advance read marker: %w", err)
	}
	return nil
}

// UnreadSummary is the per-place projection behind the sidebar: latest seq,
// the viewer's read marker, and unread/mention counts. It exists so unvisited
// places still show badges without loading history (契約ドラフト: bootstrap).
// The viewer's own messages never count as unread — writing is reading.
type UnreadSummary struct {
	Place        Place
	LastReadSeq  int64
	UnreadCount  int64
	MentionCount int64
}

// UnreadSummaries lists every place the viewer can see (their workspaces'
// channels plus their dm/group_dm places) with unread projections.
func (s *Store) UnreadSummaries(ctx context.Context, viewer ParticipantRef) ([]UnreadSummary, error) {
	if err := viewer.Validate(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`WITH my_places AS (
		   SELECT p.* FROM places p
		   JOIN workspace_members wm ON wm.workspace_id = p.workspace_id
		    AND wm.member_kind = $1 AND wm.member_id = $2 AND wm.left_at IS NULL
		   WHERE p.kind = 'channel'
		   UNION
		   SELECT p.* FROM places p
		   JOIN place_members pm ON pm.place_id = p.place_id
		    AND pm.member_kind = $1 AND pm.member_id = $2 AND pm.left_at IS NULL
		 )
		 SELECT mp.place_id, mp.kind, mp.workspace_id, mp.name, mp.topic, mp.visibility, mp.last_seq,
		        COALESCE(rm.last_read_seq, 0),
		        (SELECT count(*) FROM messages m
		          WHERE m.place_id = mp.place_id AND m.seq > COALESCE(rm.last_read_seq, 0)
		            AND m.deleted_at IS NULL
		            AND NOT (m.author_kind = $1 AND m.author_id = $2)),
		        (SELECT count(*) FROM messages m
		          JOIN message_mentions mm ON mm.message_id = m.message_id
		          WHERE m.place_id = mp.place_id AND m.seq > COALESCE(rm.last_read_seq, 0)
		            AND m.deleted_at IS NULL
		            AND mm.member_kind = $1 AND mm.member_id = $2
		            AND NOT (m.author_kind = $1 AND m.author_id = $2))
		 FROM my_places mp
		 LEFT JOIN read_markers rm ON rm.place_id = mp.place_id
		  AND rm.member_kind = $1 AND rm.member_id = $2
		 ORDER BY mp.created_at, mp.place_id`,
		viewer.Kind, viewer.ID)
	if err != nil {
		return nil, fmt.Errorf("query unread summaries: %w", err)
	}
	defer rows.Close()
	var out []UnreadSummary
	for rows.Next() {
		var (
			sum         UnreadSummary
			workspaceID *string
			name        *string
		)
		if err := rows.Scan(&sum.Place.PlaceID, &sum.Place.Kind, &workspaceID, &name,
			&sum.Place.Topic, &sum.Place.Visibility, &sum.Place.LastSeq,
			&sum.LastReadSeq, &sum.UnreadCount, &sum.MentionCount); err != nil {
			return nil, fmt.Errorf("scan unread summary: %w", err)
		}
		if workspaceID != nil {
			sum.Place.WorkspaceID = *workspaceID
		}
		if name != nil {
			sum.Place.Name = *name
		}
		out = append(out, sum)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate unread summaries: %w", err)
	}
	return out, nil
}

// --- helpers ---

func (s *Store) messageByNonce(ctx context.Context, q querier, in AppendInput) (Message, bool, error) {
	rows, err := q.Query(ctx,
		`SELECT message_id, place_id, seq, author_kind, author_id, content, urgency,
		        reply_to, client_nonce, created_at, edited_at, deleted_at
		 FROM messages
		 WHERE place_id = $1 AND author_kind = $2 AND author_id = $3 AND client_nonce = $4`,
		in.PlaceID, in.Author.Kind, in.Author.ID, in.ClientNonce)
	if err != nil {
		return Message{}, false, fmt.Errorf("query message by nonce: %w", err)
	}
	messages, err := scanMessages(rows)
	if err != nil {
		return Message{}, false, err
	}
	if len(messages) == 0 {
		return Message{}, false, nil
	}
	if err := s.attachMentions(ctx, messages); err != nil {
		return Message{}, false, err
	}
	if err := s.attachAttachments(ctx, messages); err != nil {
		return Message{}, false, err
	}
	return messages[0], true, nil
}

func lockMessage(ctx context.Context, tx pgx.Tx, placeID, messageID string) (Message, error) {
	rows, err := tx.Query(ctx,
		`SELECT message_id, place_id, seq, author_kind, author_id, content, urgency,
		        reply_to, client_nonce, created_at, edited_at, deleted_at
		 FROM messages WHERE message_id = $1 AND place_id = $2 FOR UPDATE`,
		messageID, placeID)
	if err != nil {
		return Message{}, fmt.Errorf("lock message: %w", err)
	}
	messages, err := scanMessages(rows)
	if err != nil {
		return Message{}, err
	}
	if len(messages) == 0 {
		return Message{}, ErrMessageNotFound
	}
	return messages[0], nil
}

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

// attachMentions loads mention rows for the given messages in one query.
func (s *Store) attachMentions(ctx context.Context, messages []Message) error {
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
		`SELECT mm.message_id, mm.member_kind, mm.member_id
		 FROM message_mentions mm
		 JOIN messages m ON m.message_id = mm.message_id
		 WHERE mm.message_id = ANY($1)
		 ORDER BY mm.message_id, mm.member_kind, mm.member_id`, ids)
	if err != nil {
		return fmt.Errorf("query mentions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var messageID, kind, id string
		if err := rows.Scan(&messageID, &kind, &id); err != nil {
			return fmt.Errorf("scan mention: %w", err)
		}
		i := index[messageID]
		messages[i].Mentions = append(messages[i].Mentions,
			ParticipantRef{Kind: ParticipantKind(kind), ID: id})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate mentions: %w", err)
	}
	return nil
}

func insertMentions(ctx context.Context, tx pgx.Tx, messageID string, mentions []ParticipantRef) error {
	for _, m := range mentions {
		if _, err := tx.Exec(ctx,
			"INSERT INTO message_mentions (message_id, member_kind, member_id) VALUES ($1, $2, $3)",
			messageID, m.Kind, m.ID); err != nil {
			return fmt.Errorf("insert mention: %w", err)
		}
	}
	return nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505" // unique_violation
	}
	return false
}
