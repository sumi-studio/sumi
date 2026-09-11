// Package feedback owns a shared inbox for people and personality agents.
package feedback

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

const AppID = "feedback"
const pageSize = 50

var (
	ErrInvalid      = errors.New("invalid_request")
	ErrNotFound     = errors.New("not_found")
	ErrInstallation = errors.New("installation_required")
	ErrDisabled     = errors.New("app_disabled")
	ErrUnavailable  = errors.New("unavailable")
	ErrRevision     = errors.New("revision_conflict")
	ErrRequest      = errors.New("request_conflict")
)

type Participant struct {
	Kind               participant.Kind `json:"kind"`
	HumanID            string           `json:"human_id,omitempty"`
	PersonalityAgentID string           `json:"personality_agent_id,omitempty"`
}

func wire(ref participant.Ref) Participant {
	p := Participant{Kind: ref.Kind}
	if ref.Kind == participant.KindHuman {
		p.HumanID = ref.ID
	} else {
		p.PersonalityAgentID = ref.ID
	}
	return p
}

type Author struct {
	Participant Participant `json:"participant"`
	DisplayName string      `json:"display_name"`
}
type Thread struct {
	Attachments   []Attachment `json:"attachments"`
	Diagnostics   *Diagnostics `json:"diagnostics,omitempty"`
	IsSummary     bool         `json:"is_summary,omitempty"`
	ID            string       `json:"id"`
	Title         string       `json:"title"`
	Body          string       `json:"body"`
	Status        string       `json:"status"`
	Author        Author       `json:"author"`
	CreatedAt     time.Time    `json:"created_at"`
	UpdatedAt     time.Time    `json:"updated_at"`
	Revision      int64        `json:"revision"`
	LatestMessage *Message     `json:"latest_message"`
	Unread        bool         `json:"unread"`
}
type Message struct {
	ID        string    `json:"id"`
	Author    Author    `json:"author"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
	Revision  int64     `json:"revision"`
}
type Activity struct {
	ID        string    `json:"id"`
	Author    Author    `json:"author"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	Revision  int64     `json:"revision"`
}
type Bootstrap struct {
	RecipientName  string      `json:"recipient_name"`
	Available      bool        `json:"available"`
	Participant    Participant `json:"participant"`
	IsRecipient    bool        `json:"is_recipient"`
	Installed      bool        `json:"installed"`
	Enabled        bool        `json:"enabled"`
	InstallationID string      `json:"installation_id,omitempty"`
}
type ThreadList struct {
	Threads    []Thread `json:"threads"`
	NextCursor *string  `json:"next_cursor"`
}
type Detail struct {
	Thread     Thread     `json:"thread"`
	Messages   []Message  `json:"messages"`
	Activities []Activity `json:"activities"`
	NextCursor *string    `json:"next_cursor"`
}

func ParseRecipients(value string) ([]participant.Ref, error) {
	result := []participant.Ref{}
	for _, key := range strings.Split(value, ",") {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		kind, id, ok := strings.Cut(key, ":")
		ref := participant.Ref{Kind: participant.Kind(kind), ID: id}
		if !ok || ref.Validate() != nil {
			return nil, errors.New("SUMI_FEEDBACK_RECIPIENTS must contain canonical participant keys")
		}
		result = append(result, ref)
	}
	return result, nil
}
func validText(v string, max int) bool {
	return utf8.ValidString(v) && !strings.ContainsRune(v, 0) && strings.TrimSpace(v) != "" && utf8.RuneCountInString(v) <= max
}
func validID(v string, version uuid.Version) bool {
	id, err := uuid.Parse(v)
	return err == nil && id.String() == v && id.Version() == version && id.Variant() == uuid.RFC4122
}
func newID() string { return uuid.Must(uuid.NewV7()).String() }
