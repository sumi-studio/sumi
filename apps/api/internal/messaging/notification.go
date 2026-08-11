package messaging

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Notification levels (契約ドラフト: NotificationSetting.per_place[].level).
const (
	NotifyLevelAll      = "all"
	NotifyLevelMentions = "mentions"
	NotifyLevelMute     = "mute"
)

// DefaultNotifyLevel applies to a participant who has never said anything about
// their notifications. A missing row is silence, not an error.
const DefaultNotifyLevel = NotifyLevelAll

// Notification reasons, in the priority order the evaluator applies them:
// dm > mention > keyword > all. The reason is the honest answer to "なぜ今
// これで呼ばれたのか" — the recipient sees it, so it must not be a guess.
const (
	NotifyReasonDM      = "dm"
	NotifyReasonMention = "mention"
	NotifyReasonKeyword = "keyword"
	NotifyReasonAll     = "all"
)

// Keyword bounds. A keyword list is a handful of words a person wants to be
// called for, not a search index.
const (
	MaxNotificationKeywords    = 32
	MaxNotificationKeywordRune = 64
)

// PlaceNotifyLevel is one place-scoped override of the owner's default.
type PlaceNotifyLevel struct {
	PlaceID   string
	PlaceKind string
	Level     string
}

// NotificationSetting is one participant's own notification preference
// (契約ドラフト: 本人が所有・本人が変更する). Humans and PersonalityAgents own
// the identical resource; only the owner may read or replace it.
type NotificationSetting struct {
	Owner        ParticipantRef
	DefaultLevel string
	PerPlace     []PlaceNotifyLevel
	Keywords     []string
}

// Default is the level for a place the owner never singled out. An empty
// stored value means "never said anything", not "chose nothing".
func (n NotificationSetting) Default() string {
	if n.DefaultLevel == "" {
		return DefaultNotifyLevel
	}
	return n.DefaultLevel
}

// LevelFor resolves the level that governs one place: the place override when
// the owner set one, otherwise their default.
func (n NotificationSetting) LevelFor(placeID string) string {
	for _, entry := range n.PerPlace {
		if entry.PlaceID == placeID {
			return entry.Level
		}
	}
	return n.Default()
}

// NotificationDecision is the verdict for one recipient of one message: this
// message is worth interrupting them for, and this is why.
type NotificationDecision struct {
	Participant ParticipantRef
	Reason      string
}

// ErrInvalidNotificationSetting marks a setting the caller shaped wrongly (an
// unknown level, an oversized keyword list, the same place twice). It is a
// request error, not an infrastructure one, so the transport layer answers 400
// instead of hiding a bad request behind a 500.
var ErrInvalidNotificationSetting = errors.New("invalid notification setting")

// ValidateNotifyLevel rejects unknown levels fail-closed.
func ValidateNotifyLevel(level string) error {
	switch level {
	case NotifyLevelAll, NotifyLevelMentions, NotifyLevelMute:
		return nil
	default:
		return fmt.Errorf("%w: unknown level %q", ErrInvalidNotificationSetting, level)
	}
}

// --- internals ---

// resolvedSetting is one candidate's effective level for one place plus their
// keyword list, already reduced from the two setting tables.
type resolvedSetting struct {
	level    string
	keywords []string
}

// matchesKeyword reports whether the already-lowercased content contains any of
// the owner's keywords. Matching is case-insensitive substring: Japanese has no
// word boundary to anchor to, and a keyword is a word someone wants to hear,
// not a token in a query language.
func matchesKeyword(loweredContent string, keywords []string) bool {
	for _, keyword := range keywords {
		if keyword == "" {
			continue
		}
		if strings.Contains(loweredContent, strings.ToLower(keyword)) {
			return true
		}
	}
	return false
}

// normalizeKeywords trims, drops empties, and rejects a list that is too long
// or a keyword that is too long. Duplicates collapse: the same word twice is
// the same request.
func normalizeKeywords(keywords []string) ([]string, error) {
	out := make([]string, 0, len(keywords))
	seen := map[string]bool{}
	for _, keyword := range keywords {
		trimmed := strings.TrimSpace(keyword)
		if trimmed == "" {
			continue
		}
		if utf8.RuneCountInString(trimmed) > MaxNotificationKeywordRune {
			return nil, fmt.Errorf("%w: keyword exceeds %d characters",
				ErrInvalidNotificationSetting, MaxNotificationKeywordRune)
		}
		folded := strings.ToLower(trimmed)
		if seen[folded] {
			continue
		}
		seen[folded] = true
		out = append(out, trimmed)
	}
	if len(out) > MaxNotificationKeywords {
		return nil, fmt.Errorf("%w: more than %d keywords",
			ErrInvalidNotificationSetting, MaxNotificationKeywords)
	}
	return out, nil
}
