package todo

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

type Status string

const (
	StatusOpen Status = "open"
	StatusDone Status = "done"
)

type Priority string

const (
	PriorityNone   Priority = "none"
	PriorityLow    Priority = "low"
	PriorityMedium Priority = "medium"
	PriorityHigh   Priority = "high"
)

type DueKind string

const (
	DueKindDate     DueKind = "date"
	DueKindDatetime DueKind = "datetime"
)

type Due struct {
	Kind     DueKind    `json:"kind"`
	Date     string     `json:"date,omitempty"`
	At       *time.Time `json:"at,omitempty"`
	Timezone string     `json:"timezone"`
}

type DueInput struct {
	Kind     DueKind `json:"kind"`
	Date     string  `json:"date,omitempty"`
	At       string  `json:"at,omitempty"`
	Timezone string  `json:"timezone,omitempty"`
}

type Todo struct {
	ID          string     `json:"id"`
	Title       string     `json:"title"`
	Description string     `json:"description"`
	Status      Status     `json:"status"`
	Priority    Priority   `json:"priority"`
	Due         *Due       `json:"due"`
	Version     int        `json:"version"`
	ViaAgent    bool       `json:"via_agent"`
	CompletedAt *time.Time `json:"completed_at"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

type CreateInput struct {
	Title       string
	Description string
	Status      Status
	Priority    Priority
	Due         *DueInput
}

type UpdateInput struct {
	ExpectedVersion int
	Title           *string
	Description     *string
	Status          *Status
	Priority        *Priority
	DueSet          bool
	Due             *DueInput
}

func (u UpdateInput) HasChanges() bool {
	return u.Title != nil || u.Description != nil || u.Status != nil || u.Priority != nil || u.DueSet
}

type ListFilter struct {
	Status  *Status
	Overdue bool
	Query   string
	Sort    string
	Limit   int
	Offset  int
}

type ListResult struct {
	Items []Todo `json:"items"`
	Total int    `json:"total"`
}

type ValidationError struct{ Message string }

func (e *ValidationError) Error() string { return e.Message }

var ErrNotFound = errors.New("todo not found")

type VersionConflictError struct{ CurrentVersion int }

func (e *VersionConflictError) Error() string { return "todo version conflict" }

var timezonePattern = regexp.MustCompile(`^(?:UTC|[A-Za-z][A-Za-z0-9._+-]*(?:/[A-Za-z0-9._+-]+)+)$`)
var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func IsUUID(value string) bool { return uuidPattern.MatchString(value) }

func validateTitle(value string) error {
	length := utf8.RuneCountInString(value)
	if length < 1 || length > 200 {
		return &ValidationError{Message: "title must contain between 1 and 200 characters"}
	}
	if strings.ContainsRune(value, '\x00') {
		return &ValidationError{Message: "title must not contain NUL"}
	}
	return nil
}

func validateDescription(value string) error {
	if strings.ContainsRune(value, '\x00') {
		return &ValidationError{Message: "description must not contain NUL"}
	}
	return nil
}

func validateStatus(value Status) error {
	if value != StatusOpen && value != StatusDone {
		return &ValidationError{Message: "status must be open or done"}
	}
	return nil
}

func validatePriority(value Priority) error {
	switch value {
	case PriorityNone, PriorityLow, PriorityMedium, PriorityHigh:
		return nil
	default:
		return &ValidationError{Message: "priority must be none, low, medium, or high"}
	}
}

func normalizeDue(input *DueInput, defaultTimezone string) (*Due, error) {
	if input == nil {
		return nil, nil
	}
	timezone := input.Timezone
	if timezone == "" {
		timezone = defaultTimezone
	}
	if !timezonePattern.MatchString(timezone) {
		return nil, &ValidationError{Message: "timezone must be an IANA timezone name"}
	}
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return nil, &ValidationError{Message: "timezone must be an IANA timezone name"}
	}

	switch input.Kind {
	case DueKindDate:
		if input.At != "" || input.Date == "" {
			return nil, &ValidationError{Message: "date due requires date and forbids at"}
		}
		parsed, err := time.Parse("2006-01-02", input.Date)
		if err != nil || parsed.Format("2006-01-02") != input.Date {
			return nil, &ValidationError{Message: "due date must use YYYY-MM-DD"}
		}
		return &Due{Kind: DueKindDate, Date: input.Date, Timezone: timezone}, nil
	case DueKindDatetime:
		if input.Date != "" || input.At == "" {
			return nil, &ValidationError{Message: "datetime due requires at and forbids date"}
		}
		parsed, err := time.Parse(time.RFC3339, input.At)
		if err != nil {
			return nil, &ValidationError{Message: "due at must be RFC3339 with an explicit offset"}
		}
		_, suppliedOffset := parsed.Zone()
		_, timezoneOffset := parsed.In(location).Zone()
		if suppliedOffset != timezoneOffset {
			return nil, &ValidationError{Message: "due at offset does not match timezone"}
		}
		return &Due{Kind: DueKindDatetime, At: &parsed, Timezone: timezone}, nil
	default:
		return nil, &ValidationError{Message: "due kind must be date or datetime"}
	}
}

func Deadline(due *Due) (time.Time, error) {
	if due == nil {
		return time.Time{}, &ValidationError{Message: "due is required"}
	}
	if due.Kind == DueKindDatetime && due.At != nil {
		return *due.At, nil
	}
	if due.Kind != DueKindDate || due.Date == "" {
		return time.Time{}, &ValidationError{Message: "invalid due value"}
	}
	location, err := time.LoadLocation(due.Timezone)
	if err != nil {
		return time.Time{}, &ValidationError{Message: "timezone must be an IANA timezone name"}
	}
	date, err := time.ParseInLocation("2006-01-02", due.Date, location)
	if err != nil {
		return time.Time{}, &ValidationError{Message: "invalid due date"}
	}
	return date.AddDate(0, 0, 1), nil
}

func newUUIDv7(now time.Time) (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate UUIDv7 randomness: %w", err)
	}
	milliseconds := uint64(now.UnixMilli())
	for i := 5; i >= 0; i-- {
		value[i] = byte(milliseconds)
		milliseconds >>= 8
	}
	value[6] = (value[6] & 0x0f) | 0x70
	value[8] = (value[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(value[:])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32], nil
}
