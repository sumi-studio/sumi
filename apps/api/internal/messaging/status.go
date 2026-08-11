package messaging

import (
	"time"
)

// Status values (契約ドラフト: 自己申告のStatus。監視による自動表示はしない).
const (
	StatusAvailable = "available"
	StatusBusy      = "busy"
	StatusAway      = "away"
)

// MaxStatusNoteChars matches the schema CHECK on participant_statuses.note.
const MaxStatusNoteChars = 200

// ParticipantStatus is one participant's current self-declared status. A nil
// ExpiresAt holds until replaced; an expired status is filtered at read time
// and never reported, so there is no background sweeper to disagree with.
type ParticipantStatus struct {
	Participant ParticipantRef
	Status      string
	Note        string
	ExpiresAt   *time.Time
}
