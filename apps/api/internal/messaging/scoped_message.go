package messaging

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func (s *ScopedStore) AppendMessage(ctx context.Context, in AppendInput) (Message, bool, error) {
	if in.Author != (ParticipantRef{}) && in.Author != s.Scope.Actor {
		return Message{}, false, errors.New("message author must come from authenticated scope")
	}
	in.Author = s.Scope.Actor
	switch in.Urgency {
	case "":
		in.Urgency = UrgencyNormal
	case UrgencyUrgent, UrgencyNormal, UrgencyFYI:
	default:
		return Message{}, false, fmt.Errorf("unknown urgency %q", in.Urgency)
	}
	// Empty text is valid only when at least one attachment binds; the
	// deferred database trigger enforces the same rule at commit.
	if in.Content == "" && len(in.AttachmentIDs) == 0 {
		return Message{}, false, errors.New("content must not be empty")
	}
	if !messageContentFitsStorage(in.Content) {
		return Message{}, false, fmt.Errorf("content is not storable or exceeds %d bytes", MaxContentBytes)
	}
	if len(in.AttachmentIDs) > MaxAttachmentsPerMessage {
		return Message{}, false, ErrTooManyAttachments
	}
	if in.ClientNonce == "" || len(in.ClientNonce) > 128 {
		return Message{}, false, errors.New("client nonce must be 1..128 bytes")
	}
	message, created, err := s.appendScopedOnce(ctx, in)
	if err == nil || !isUniqueViolation(err) {
		return message, created, err
	}
	existing, found, err := s.authorizedMessageByNonce(ctx, in)
	if err != nil {
		return Message{}, false, err
	}
	if !found {
		return Message{}, false, fmt.Errorf("idempotent re-read found no message for nonce %q", in.ClientNonce)
	}
	return existing, false, nil
}

// requestMatchesReplay compares the incoming request against the durable
// receipt of the message that already owns its nonce. A changed request under
// the same nonce is a conflict, never a silent replay of the first message.
func requestMatchesReplay(in AppendInput, storedDigest []byte) bool {
	incoming := messageRequestDigest(in.Content, in.Urgency, in.ReplyTo, in.AttachmentIDs)
	return bytes.Equal(incoming, storedDigest)
}

func (s *ScopedStore) authorizedMessageByNonce(ctx context.Context, in AppendInput) (Message, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Message{}, false, fmt.Errorf("begin idempotent scoped re-read: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeInTx(ctx, tx); err != nil {
		return Message{}, false, err
	}
	place, err := s.loadScopedPlace(ctx, tx, in.PlaceID)
	if err != nil {
		return Message{}, false, err
	}
	access, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor)
	if err != nil {
		return Message{}, false, err
	}
	message, digest, found, err := s.messageByNonce(ctx, tx, in)
	if err != nil {
		return Message{}, false, err
	}
	if found && message.Seq < access.VisibleFromSeq {
		return Message{}, false, ErrMessageNotFound
	}
	if found && !requestMatchesReplay(in, digest) {
		return Message{}, false, ErrIdempotencyConflict
	}
	if err := tx.Commit(ctx); err != nil {
		return Message{}, false, fmt.Errorf("commit idempotent scoped re-read: %w", err)
	}
	return message, found, nil
}

func (s *ScopedStore) appendScopedOnce(ctx context.Context, in AppendInput) (Message, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Message{}, false, fmt.Errorf("begin scoped append: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeMutationInTx(ctx, tx); err != nil {
		return Message{}, false, err
	}
	place, err := s.loadScopedPlace(ctx, tx, in.PlaceID)
	if err != nil {
		return Message{}, false, err
	}
	access, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor)
	if err != nil {
		return Message{}, false, err
	}
	if existing, digest, found, err := s.messageByNonce(ctx, tx, in); err != nil {
		return Message{}, false, err
	} else if found {
		if existing.Seq < access.VisibleFromSeq {
			return Message{}, false, ErrMessageNotFound
		}
		if !requestMatchesReplay(in, digest) {
			return Message{}, false, ErrIdempotencyConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return Message{}, false, fmt.Errorf("commit idempotent scoped append: %w", err)
		}
		return existing, false, nil
	}
	if in.ReplyTo != "" {
		var samePlace bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM messages
				WHERE workspace_id = $1 AND place_id = $2 AND message_id = $3
				  AND seq >= $4
			)`, s.Scope.WorkspaceID, in.PlaceID, in.ReplyTo, access.VisibleFromSeq).Scan(&samePlace); err != nil {
			return Message{}, false, fmt.Errorf("check scoped reply target: %w", err)
		}
		if !samePlace {
			return Message{}, false, ErrMessageNotFound
		}
	}
	var seq int64
	if err := tx.QueryRow(ctx, `
		UPDATE places SET last_seq = last_seq + 1
		WHERE workspace_id = $1 AND place_id = $2 RETURNING last_seq`,
		s.Scope.WorkspaceID, in.PlaceID).Scan(&seq); err != nil {
		return Message{}, false, fmt.Errorf("allocate scoped seq: %w", err)
	}
	members, err := s.activeMembersScoped(ctx, tx, place)
	if err != nil {
		return Message{}, false, err
	}
	mentions := resolveMentions(in.Content, members)
	message := Message{
		MessageID: newUUIDv7(), PlaceID: in.PlaceID, Seq: seq,
		Author: s.Scope.Actor, Content: in.Content, Urgency: in.Urgency,
		Mentions: mentions, ReplyTo: in.ReplyTo, ClientNonce: in.ClientNonce, Revision: 1,
	}
	var replyTo *string
	if in.ReplyTo != "" {
		replyTo = &in.ReplyTo
	}
	if err := tx.QueryRow(ctx, `
		INSERT INTO messages
			(message_id, workspace_id, place_id, seq, author_kind, author_id,
			 content, urgency, reply_to, client_nonce, request_digest)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		RETURNING created_at`,
		message.MessageID, s.Scope.WorkspaceID, message.PlaceID, message.Seq,
		message.Author.Kind, message.Author.ID, message.Content, message.Urgency,
		replyTo, message.ClientNonce,
		messageRequestDigest(in.Content, in.Urgency, in.ReplyTo, in.AttachmentIDs),
	).Scan(&message.CreatedAt); err != nil {
		return Message{}, false, fmt.Errorf("insert scoped message: %w", err)
	}
	// Attachment binds share the message transaction and snapshot: a miss on
	// any of them rolls back the message, its mentions, its seq, and its
	// notification intents together.
	attachments, err := s.bindAttachmentsInTx(ctx, tx, in.PlaceID, message.MessageID, in.AttachmentIDs)
	if err != nil {
		return Message{}, false, err
	}
	message.Attachments = attachments
	if err := insertMentions(ctx, tx, message.MessageID, mentions); err != nil {
		return Message{}, false, err
	}
	// Notification intent issuance is part of the same commit. Delivery remains
	// post-commit/best-effort, but authoritative recipient intent never is.
	if err := s.issueScopedNotificationIntents(ctx, tx, place, message, members); err != nil {
		return Message{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Message{}, false, fmt.Errorf("commit scoped append: %w", err)
	}
	return message, true, nil
}

func (s *ScopedStore) messageByNonce(ctx context.Context, q querier, in AppendInput) (Message, []byte, bool, error) {
	rows, err := q.Query(ctx, `
		SELECT message_id, place_id, seq, author_kind, author_id, content, urgency,
		       reply_to, client_nonce, created_at, edited_at, revision, deleted_at
		FROM messages
		WHERE workspace_id = $1 AND place_id = $2
		  AND author_kind = $3 AND author_id = $4 AND client_nonce = $5`,
		s.Scope.WorkspaceID, in.PlaceID, s.Scope.Actor.Kind, s.Scope.Actor.ID, in.ClientNonce)
	if err != nil {
		return Message{}, nil, false, fmt.Errorf("query scoped message by nonce: %w", err)
	}
	messages, err := scanMessages(rows)
	if err != nil {
		return Message{}, nil, false, err
	}
	if len(messages) == 0 {
		return Message{}, nil, false, nil
	}
	if err := attachMessagePartsWith(ctx, q, messages); err != nil {
		return Message{}, nil, false, err
	}
	var digest []byte
	if err := q.QueryRow(ctx, `
		SELECT request_digest FROM messages WHERE workspace_id = $1 AND message_id = $2`,
		s.Scope.WorkspaceID, messages[0].MessageID).Scan(&digest); err != nil {
		return Message{}, nil, false, fmt.Errorf("load scoped request digest: %w", err)
	}
	return messages[0], digest, true, nil
}

func (s *ScopedStore) History(ctx context.Context, placeID string, opt HistoryOptions) ([]Message, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin scoped history: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeInTx(ctx, tx); err != nil {
		return nil, err
	}
	place, err := s.loadScopedPlace(ctx, tx, placeID)
	if err != nil {
		return nil, err
	}
	access, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor)
	if err != nil {
		return nil, err
	}
	messages, err := s.historyAfterAuthorization(ctx, tx, place, access, opt)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit scoped history: %w", err)
	}
	return messages, nil
}

// historyAfterAuthorization reads one history page through q after the exact
// installation, Workspace membership, place, and active place tenure have all
// been authorized by the caller. Keeping q explicit lets OpenSnapshot hold
// those decisions and every projected message part in one PostgreSQL snapshot.
func (s *ScopedStore) historyAfterAuthorization(
	ctx context.Context,
	q querier,
	place Place,
	access PlaceAccess,
	opt HistoryOptions,
) ([]Message, error) {
	limit := opt.Limit
	if limit <= 0 {
		limit = defaultHistoryLimit
	}
	if limit > MaxHistoryLimit {
		limit = MaxHistoryLimit
	}
	args := []any{s.Scope.WorkspaceID, place.PlaceID, access.VisibleFromSeq, limit}
	before := ""
	if opt.BeforeSeq > 0 {
		before = "AND seq < $5"
		args = append(args, opt.BeforeSeq)
	}
	rows, err := q.Query(ctx, fmt.Sprintf(`
		SELECT message_id, place_id, seq, author_kind, author_id, content, urgency,
		       reply_to, client_nonce, created_at, edited_at, revision, deleted_at
		FROM messages
		WHERE workspace_id = $1 AND place_id = $2 AND seq >= $3 %s
		ORDER BY seq DESC LIMIT $4`, before), args...)
	if err != nil {
		return nil, fmt.Errorf("query scoped history: %w", err)
	}
	messages, err := scanMessages(rows)
	if err != nil {
		return nil, err
	}
	for left, right := 0, len(messages)-1; left < right; left, right = left+1, right-1 {
		messages[left], messages[right] = messages[right], messages[left]
	}
	if err := attachMessagePartsWith(ctx, q, messages); err != nil {
		return nil, err
	}
	return messages, nil
}

func (s *ScopedStore) MessagesSince(ctx context.Context, placeID string, sinceSeq int64, limit int) ([]Message, error) {
	if limit <= 0 || limit > MaxHistoryLimit {
		limit = MaxHistoryLimit
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin scoped catch-up: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeInTx(ctx, tx); err != nil {
		return nil, err
	}
	place, err := s.loadScopedPlace(ctx, tx, placeID)
	if err != nil {
		return nil, err
	}
	access, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor)
	if err != nil {
		return nil, err
	}
	lower := sinceSeq + 1
	if lower < access.VisibleFromSeq {
		lower = access.VisibleFromSeq
	}
	rows, err := tx.Query(ctx, `
		SELECT message_id, place_id, seq, author_kind, author_id, content, urgency,
		       reply_to, client_nonce, created_at, edited_at, revision, deleted_at
		FROM messages
		WHERE workspace_id = $1 AND place_id = $2 AND seq >= $3
		ORDER BY seq ASC LIMIT $4`, s.Scope.WorkspaceID, placeID, lower, limit)
	if err != nil {
		return nil, fmt.Errorf("query scoped catch-up: %w", err)
	}
	messages, err := scanMessages(rows)
	if err != nil {
		return nil, err
	}
	if err := attachMessagePartsWith(ctx, tx, messages); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit scoped catch-up: %w", err)
	}
	return messages, nil
}

func (s *ScopedStore) EditMessage(ctx context.Context, placeID, messageID, content string, expectedRevision int64) (Message, error) {
	if content == "" {
		return Message{}, errors.New("content must not be empty")
	}
	if !messageContentFitsStorage(content) {
		return Message{}, fmt.Errorf("content is not storable or exceeds %d bytes", MaxContentBytes)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Message{}, fmt.Errorf("begin scoped edit: %w", err)
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
	if message.Author != s.Scope.Actor {
		return Message{}, ErrNotAuthor
	}
	if message.Deleted {
		return Message{}, ErrMessageDeleted
	}
	if expectedRevision <= 0 || message.Revision != expectedRevision {
		return Message{}, currentRevisionConflict(ctx, tx, message)
	}
	members, err := s.activeMembersScoped(ctx, tx, place)
	if err != nil {
		return Message{}, err
	}
	mentions := resolveMentions(content, members)
	var editedAt time.Time
	if err := tx.QueryRow(ctx, `
		UPDATE messages SET content = $1, edited_at = now(), revision = revision + 1
		WHERE workspace_id = $2 AND message_id = $3 AND revision = $4
		RETURNING edited_at, revision`,
		content, s.Scope.WorkspaceID, messageID, expectedRevision).Scan(&editedAt, &message.Revision); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Message{}, currentRevisionConflict(ctx, tx, message)
		}
		return Message{}, fmt.Errorf("update scoped message: %w", err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM message_mentions WHERE message_id = $1", messageID); err != nil {
		return Message{}, fmt.Errorf("clear scoped mentions: %w", err)
	}
	if err := insertMentions(ctx, tx, messageID, mentions); err != nil {
		return Message{}, err
	}
	message.Content, message.Mentions, message.EditedAt = content, mentions, &editedAt
	parts := []Message{message}
	if err := attachMessagePartsWith(ctx, tx, parts); err != nil {
		return Message{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Message{}, fmt.Errorf("commit scoped edit: %w", err)
	}
	return parts[0], nil
}

// currentRevisionConflict completes the locked message before returning it to
// the transport. The caller has already checked exact scope, visibility and
// authorship, so the response cannot disclose a message the editor may not see.
func currentRevisionConflict(ctx context.Context, q querier, message Message) error {
	current := []Message{message}
	if err := attachMessagePartsWith(ctx, q, current); err != nil {
		return fmt.Errorf("load current scoped message for edit conflict: %w", err)
	}
	return &messageRevisionConflictError{Current: current[0]}
}

func (s *ScopedStore) DeleteMessage(ctx context.Context, placeID, messageID string) (Message, error) {
	if err := s.Scope.Validate(); err != nil {
		return Message{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Message{}, fmt.Errorf("begin scoped delete: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	preflight, err := preflightMessageDelete(ctx, tx, s.Scope.WorkspaceID, placeID, messageID)
	if err != nil {
		return Message{}, err
	}
	if !preflight.Deleted && preflight.Author != s.Scope.Actor {
		_, err = s.authorizeManageChannelsInTx(ctx, tx)
	} else {
		_, err = s.authorizeMutationInTx(ctx, tx)
	}
	if err != nil {
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
	if message.Seq < access.VisibleFromSeq || message.Author != preflight.Author {
		return Message{}, ErrMessageNotFound
	}
	if message.Deleted {
		return message, tx.Commit(ctx)
	}
	if message.Author != s.Scope.Actor && place.Kind != PlaceChannel {
		return Message{}, ErrForbidden
	}
	if _, err := tx.Exec(ctx, `
		UPDATE messages SET content = NULL, deleted_at = now()
		WHERE workspace_id = $1 AND message_id = $2`, s.Scope.WorkspaceID, messageID); err != nil {
		return Message{}, fmt.Errorf("tombstone scoped message: %w", err)
	}
	// Bytes leave through the durable deletion outbox after commit; the
	// attachment rows stay as the record of what the message carried.
	if err := enqueueAttachmentDeletionsInTx(ctx, tx, s.Scope.WorkspaceID, messageID); err != nil {
		return Message{}, err
	}
	for _, statement := range []string{
		"DELETE FROM message_mentions WHERE message_id = $1",
		"DELETE FROM message_reactions WHERE message_id = $1",
	} {
		if _, err := tx.Exec(ctx, statement, messageID); err != nil {
			return Message{}, fmt.Errorf("clear tombstone projection: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Message{}, fmt.Errorf("commit scoped delete: %w", err)
	}
	message.Content, message.Mentions, message.Reactions, message.Attachments, message.Deleted = "", nil, nil, nil, true
	return message, nil
}

type messageDeletePreflight struct {
	Author  ParticipantRef
	Deleted bool
}

func preflightMessageDelete(ctx context.Context, tx pgx.Tx, workspaceID, placeID, messageID string) (messageDeletePreflight, error) {
	var preflight messageDeletePreflight
	err := tx.QueryRow(ctx, `
		SELECT author_kind, author_id, deleted_at IS NOT NULL
		FROM messages
		WHERE workspace_id = $1 AND place_id = $2 AND message_id = $3`,
		workspaceID, placeID, messageID).Scan(&preflight.Author.Kind, &preflight.Author.ID, &preflight.Deleted)
	if errors.Is(err, pgx.ErrNoRows) {
		return messageDeletePreflight{}, ErrMessageNotFound
	}
	if err != nil {
		return messageDeletePreflight{}, fmt.Errorf("preflight scoped message delete: %w", err)
	}
	return preflight, nil
}

func (s *ScopedStore) ReadThrough(ctx context.Context, placeID string, seq int64) error {
	if seq < 0 {
		return errors.New("seq must be non-negative")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin scoped read-through: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	membership, err := s.authorizeMutationInTx(ctx, tx)
	if err != nil {
		return err
	}
	place, err := s.loadScopedPlace(ctx, tx, placeID)
	if err != nil {
		return err
	}
	access, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor)
	if err != nil {
		return err
	}
	if seq > place.LastSeq {
		return ErrSeqBeyondLatest
	}
	if access.PlaceMemberID == "" {
		if err := admitPlaceTenure(ctx, tx, placeID, membership, 1); err != nil {
			return err
		}
		access, err = s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor)
		if err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO read_markers (place_id, place_member_id, last_read_seq)
		VALUES ($1, $2, $3)
		ON CONFLICT (place_id, place_member_id)
		DO UPDATE SET last_read_seq = GREATEST(read_markers.last_read_seq, EXCLUDED.last_read_seq),
		              updated_at = now()`, placeID, access.PlaceMemberID, seq); err != nil {
		return fmt.Errorf("advance scoped read marker: %w", err)
	}
	return tx.Commit(ctx)
}

func (s *ScopedStore) ReadMarker(ctx context.Context, placeID string) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin scoped read-marker read: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeInTx(ctx, tx); err != nil {
		return 0, err
	}
	place, err := s.loadScopedPlace(ctx, tx, placeID)
	if err != nil {
		return 0, err
	}
	access, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor)
	if err != nil {
		return 0, err
	}
	seq, err := s.readMarkerAfterAuthorization(ctx, tx, place, access)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return seq, nil
}

// readMarkerAfterAuthorization reads the cursor for the exact active place
// tenure selected by access. It deliberately accepts the caller's querier so
// an OpenSnapshot cannot escape to the pool between history and cursor reads.
func (s *ScopedStore) readMarkerAfterAuthorization(
	ctx context.Context,
	q querier,
	place Place,
	access PlaceAccess,
) (int64, error) {
	if access.PlaceMemberID == "" {
		return 0, nil
	}
	var seq int64
	err := q.QueryRow(ctx, `
		SELECT last_read_seq FROM read_markers
		WHERE place_id = $1 AND place_member_id = $2`, place.PlaceID, access.PlaceMemberID).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		seq = 0
	} else if err != nil {
		return 0, fmt.Errorf("load scoped read marker: %w", err)
	}
	return seq, nil
}

func (s *ScopedStore) UnreadSummaries(ctx context.Context) ([]UnreadSummary, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin scoped unread read: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	membership, err := s.authorizeInTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		WITH visible_places AS (
			SELECT p.*, pm.place_member_id, COALESCE(pm.visible_from_seq, 1) AS visible_from_seq
			FROM places p
			LEFT JOIN place_members pm
			  ON pm.workspace_id = p.workspace_id AND pm.place_id = p.place_id
			 AND pm.workspace_member_id = $2 AND pm.left_at IS NULL
			WHERE p.workspace_id = $1
			  AND (p.kind = 'channel' OR (p.kind IN ('dm', 'group_dm') AND pm.place_member_id IS NOT NULL))
		)
		SELECT vp.place_id, vp.kind, vp.workspace_id, vp.name, vp.topic,
		       vp.visibility, vp.last_seq, vp.voice, COALESCE(rm.last_read_seq, 0),
		       (SELECT count(*) FROM messages m
		        WHERE m.workspace_id = $1 AND m.place_id = vp.place_id
		          AND m.seq >= vp.visible_from_seq AND m.seq > COALESCE(rm.last_read_seq, 0)
		          AND m.deleted_at IS NULL
		          AND NOT (m.author_kind = $3 AND m.author_id = $4)),
		       (SELECT count(*) FROM messages m
		        JOIN message_mentions mm ON mm.message_id = m.message_id
		        WHERE m.workspace_id = $1 AND m.place_id = vp.place_id
		          AND m.seq >= vp.visible_from_seq AND m.seq > COALESCE(rm.last_read_seq, 0)
		          AND m.deleted_at IS NULL
		          AND mm.member_kind = $3 AND mm.member_id = $4
		          AND NOT (m.author_kind = $3 AND m.author_id = $4))
		FROM visible_places vp
		LEFT JOIN read_markers rm
		  ON rm.place_id = vp.place_id AND rm.place_member_id = vp.place_member_id
		ORDER BY vp.created_at, vp.place_id`,
		s.Scope.WorkspaceID, membership.WorkspaceMemberID, s.Scope.Actor.Kind, s.Scope.Actor.ID)
	if err != nil {
		return nil, fmt.Errorf("query scoped unread summaries: %w", err)
	}
	defer rows.Close()
	var summaries []UnreadSummary
	for rows.Next() {
		var summary UnreadSummary
		var name *string
		if err := rows.Scan(&summary.Place.PlaceID, &summary.Place.Kind,
			&summary.Place.WorkspaceID, &name, &summary.Place.Topic,
			&summary.Place.Visibility, &summary.Place.LastSeq, &summary.Place.Voice, &summary.LastReadSeq,
			&summary.UnreadCount, &summary.MentionCount); err != nil {
			return nil, fmt.Errorf("scan scoped unread summary: %w", err)
		}
		if name != nil {
			summary.Place.Name = *name
		}
		summaries = append(summaries, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate scoped unread summaries: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return summaries, nil
}

func attachMessagePartsWith(ctx context.Context, q querier, messages []Message) error {
	if err := attachMentionsWith(ctx, q, messages); err != nil {
		return err
	}
	if err := attachReactionsWith(ctx, q, messages); err != nil {
		return err
	}
	return attachAttachmentsWith(ctx, q, messages)
}

func attachMentionsWith(ctx context.Context, q querier, messages []Message) error {
	if len(messages) == 0 {
		return nil
	}
	ids := make([]string, len(messages))
	index := make(map[string]int, len(messages))
	for i, message := range messages {
		ids[i], index[message.MessageID] = message.MessageID, i
	}
	rows, err := q.Query(ctx, `
		SELECT message_id, member_kind, member_id FROM message_mentions
		WHERE message_id = ANY($1) ORDER BY message_id, member_kind, member_id`, ids)
	if err != nil {
		return fmt.Errorf("query mentions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var messageID string
		var ref ParticipantRef
		if err := rows.Scan(&messageID, &ref.Kind, &ref.ID); err != nil {
			return fmt.Errorf("scan mention: %w", err)
		}
		messages[index[messageID]].Mentions = append(messages[index[messageID]].Mentions, ref)
	}
	return rows.Err()
}

func attachReactionsWith(ctx context.Context, q querier, messages []Message) error {
	if len(messages) == 0 {
		return nil
	}
	ids := make([]string, len(messages))
	index := make(map[string]int, len(messages))
	for i, message := range messages {
		ids[i], index[message.MessageID] = message.MessageID, i
	}
	rows, err := q.Query(ctx, `
		SELECT message_id, emoji, member_kind, member_id FROM message_reactions
		WHERE message_id = ANY($1) ORDER BY message_id, created_at, member_kind, member_id`, ids)
	if err != nil {
		return fmt.Errorf("query reactions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var messageID, emoji string
		var ref ParticipantRef
		if err := rows.Scan(&messageID, &emoji, &ref.Kind, &ref.ID); err != nil {
			return fmt.Errorf("scan reaction: %w", err)
		}
		message := &messages[index[messageID]]
		found := false
		for i := range message.Reactions {
			if message.Reactions[i].Emoji == emoji {
				message.Reactions[i].Participants = append(message.Reactions[i].Participants, ref)
				found = true
				break
			}
		}
		if !found {
			message.Reactions = append(message.Reactions, ReactionSummary{Emoji: emoji, Participants: []ParticipantRef{ref}})
		}
	}
	return rows.Err()
}

func lockMessageScoped(ctx context.Context, tx pgx.Tx, workspaceID, placeID, messageID string) (Message, error) {
	rows, err := tx.Query(ctx, `
		SELECT message_id, place_id, seq, author_kind, author_id, content, urgency,
		       reply_to, client_nonce, created_at, edited_at, revision, deleted_at
		FROM messages WHERE workspace_id = $1 AND place_id = $2 AND message_id = $3
		FOR UPDATE`, workspaceID, placeID, messageID)
	if err != nil {
		return Message{}, fmt.Errorf("lock scoped message: %w", err)
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
