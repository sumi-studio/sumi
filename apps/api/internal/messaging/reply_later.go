package messaging

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

// MaxReplyLaterNoteChars matches the schema CHECK on reply_later_markers.note.
const MaxReplyLaterNoteChars = 500

// DefaultReplyLaterNote is what the one-tap gesture says when the participant
// adds no words of their own (the web UI sends no note; the mock uses the same
// phrase). An explicit note always replaces it.
const DefaultReplyLaterNote = "後で返信します"

// DefaultReplyLaterDelay is how far out the promise reminds when the
// participant names no time. It matches the web UI's first option (30分後).
// MaxReplyLaterDelay bounds a relative request; a promise further out than a
// week is a calendar entry, not a reply-later marker.
const (
	DefaultReplyLaterDelay = 30 * time.Minute
	MaxReplyLaterDelay     = 7 * 24 * time.Hour
)

// ErrMarkerNotFound doubles as the authorization failure: a marker that is not
// the caller's to resolve is reported as missing, so the resolve path never
// confirms marker identifiers across the ownership boundary.
var ErrMarkerNotFound = errors.New("reply-later marker not found")

// ReplyLaterMarker is one durable「後で返信します」promise (合意事項 6). The
// fact and the note are visible to everyone who can see the message; RemindAt
// is the owner's private reminder schedule — the transport layer keeps it off
// every other participant's wire.
type ReplyLaterMarker struct {
	MarkerID    string
	Participant ParticipantRef
	PlaceID     string
	PlaceKind   string
	MessageID   string
	Note        string
	RemindAt    time.Time
	Resolved    bool
}

// CreateReplyLater places the actor's marker on a message they can see.
// Placing the marker is the actor's own declaration — the platform never
// promises on anyone's behalf. Idempotent per (actor, message): repeating the
// tap returns the existing open marker with created=false. The message row is
// locked so concurrent taps serialize, mirroring SetReaction; a place the
// actor cannot see is ErrPlaceNotFound, a tombstone rejects new markers.
func (s *Store) CreateReplyLater(ctx context.Context, placeID, messageID string, actor ParticipantRef, note string, remindAt time.Time) (ReplyLaterMarker, bool, error) {
	if err := actor.Validate(); err != nil {
		return ReplyLaterMarker{}, false, err
	}
	if note == "" {
		note = DefaultReplyLaterNote
	}
	if utf8.RuneCountInString(note) > MaxReplyLaterNoteChars {
		return ReplyLaterMarker{}, false, fmt.Errorf("note exceeds %d characters", MaxReplyLaterNoteChars)
	}
	if remindAt.IsZero() {
		return ReplyLaterMarker{}, false, fmt.Errorf("remind_at must be set")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ReplyLaterMarker{}, false, fmt.Errorf("begin create reply-later: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	place, err := s.loadPlace(ctx, tx, placeID)
	if err != nil {
		return ReplyLaterMarker{}, false, err
	}
	visible, err := s.canAccess(ctx, tx, place, actor)
	if err != nil {
		return ReplyLaterMarker{}, false, err
	}
	if !visible {
		return ReplyLaterMarker{}, false, ErrPlaceNotFound
	}
	msg, err := lockMessage(ctx, tx, placeID, messageID)
	if err != nil {
		return ReplyLaterMarker{}, false, err
	}
	if msg.Deleted {
		return ReplyLaterMarker{}, false, ErrMessageDeleted
	}

	marker := ReplyLaterMarker{
		Participant: actor,
		PlaceID:     placeID,
		PlaceKind:   place.Kind,
		MessageID:   messageID,
	}
	// The locked message row serializes taps, so this read is authoritative.
	err = tx.QueryRow(ctx,
		`SELECT marker_id, note, remind_at FROM reply_later_markers
		 WHERE message_id = $1 AND member_kind = $2 AND member_id = $3 AND resolved_at IS NULL`,
		messageID, actor.Kind, actor.ID).Scan(&marker.MarkerID, &marker.Note, &marker.RemindAt)
	if err == nil {
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return ReplyLaterMarker{}, false, fmt.Errorf("commit idempotent reply-later: %w", commitErr)
		}
		return marker, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ReplyLaterMarker{}, false, fmt.Errorf("query existing reply-later: %w", err)
	}

	marker.MarkerID = newUUIDv7()
	marker.Note = note
	marker.RemindAt = remindAt
	if _, err := tx.Exec(ctx,
		`INSERT INTO reply_later_markers (marker_id, member_kind, member_id, place_id, message_id, note, remind_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		marker.MarkerID, actor.Kind, actor.ID, placeID, messageID, note, remindAt); err != nil {
		return ReplyLaterMarker{}, false, fmt.Errorf("insert reply-later: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ReplyLaterMarker{}, false, fmt.Errorf("commit create reply-later: %w", err)
	}
	return marker, true, nil
}

// ResolveReplyLater marks the actor's own marker as kept. Idempotent: an
// already resolved marker is returned unchanged. Anyone else's marker — or an
// unknown one — is ErrMarkerNotFound (resolve is 本人のみ, and existence is
// not confirmed across that boundary).
func (s *Store) ResolveReplyLater(ctx context.Context, markerID string, actor ParticipantRef) (ReplyLaterMarker, error) {
	if err := actor.Validate(); err != nil {
		return ReplyLaterMarker{}, err
	}
	marker := ReplyLaterMarker{MarkerID: markerID, Participant: actor}
	var resolvedAt *time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT place_id, p.kind, message_id, rl.note, remind_at, resolved_at
		 FROM reply_later_markers rl
		 JOIN places p USING (place_id)
		 WHERE marker_id = $1 AND member_kind = $2 AND member_id = $3`,
		markerID, actor.Kind, actor.ID).
		Scan(&marker.PlaceID, &marker.PlaceKind, &marker.MessageID, &marker.Note, &marker.RemindAt, &resolvedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ReplyLaterMarker{}, ErrMarkerNotFound
	}
	if err != nil {
		return ReplyLaterMarker{}, fmt.Errorf("load reply-later: %w", err)
	}
	marker.Resolved = true
	if resolvedAt != nil {
		return marker, nil
	}
	if _, err := s.pool.Exec(ctx,
		"UPDATE reply_later_markers SET resolved_at = now() WHERE marker_id = $1 AND resolved_at IS NULL",
		markerID); err != nil {
		return ReplyLaterMarker{}, fmt.Errorf("resolve reply-later: %w", err)
	}
	return marker, nil
}

// ReplyLaterMarkersFor lists the open markers of every place the viewer can
// see — their own and everyone else's (the fact is public within the place;
// remind_at secrecy is applied when the marker is put on a wire).
func (s *Store) ReplyLaterMarkersFor(ctx context.Context, viewer ParticipantRef) ([]ReplyLaterMarker, error) {
	if err := viewer.Validate(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, participantVisiblePlacesCTE+`
		 SELECT rl.marker_id, rl.member_kind, rl.member_id, rl.place_id, mp.kind,
		        rl.message_id, rl.note, rl.remind_at
		 FROM reply_later_markers rl
		 JOIN my_places mp ON mp.place_id = rl.place_id
		 WHERE rl.resolved_at IS NULL
		 ORDER BY rl.marker_id`,
		viewer.Kind, viewer.ID)
	if err != nil {
		return nil, fmt.Errorf("query reply-later markers: %w", err)
	}
	defer rows.Close()
	var out []ReplyLaterMarker
	for rows.Next() {
		var (
			marker ReplyLaterMarker
			kind   string
		)
		if err := rows.Scan(&marker.MarkerID, &kind, &marker.Participant.ID, &marker.PlaceID,
			&marker.PlaceKind, &marker.MessageID, &marker.Note, &marker.RemindAt); err != nil {
			return nil, fmt.Errorf("scan reply-later marker: %w", err)
		}
		marker.Participant.Kind = ParticipantKind(kind)
		out = append(out, marker)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate reply-later markers: %w", err)
	}
	return out, nil
}
