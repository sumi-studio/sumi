package messaging

import (
	"errors"
	"time"
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
