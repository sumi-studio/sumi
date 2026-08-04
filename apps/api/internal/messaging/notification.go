package messaging

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
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

// NotificationSettingFor loads the owner's own setting. There is no route for
// reading anyone else's: the notification preference is 本人のもの, and its
// absence is a full default, never an error.
func (s *Store) NotificationSettingFor(ctx context.Context, owner ParticipantRef) (NotificationSetting, error) {
	if err := owner.Validate(); err != nil {
		return NotificationSetting{}, err
	}
	setting := NotificationSetting{Owner: owner, DefaultLevel: DefaultNotifyLevel, Keywords: []string{}}
	var keywords []string
	err := s.pool.QueryRow(ctx,
		`SELECT defaults_level, keywords FROM notification_settings
		 WHERE member_kind = $1 AND member_id = $2`,
		owner.Kind, owner.ID).Scan(&setting.DefaultLevel, &keywords)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return NotificationSetting{}, fmt.Errorf("load notification setting: %w", err)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return setting, nil
	}
	if keywords != nil {
		setting.Keywords = keywords
	}
	rows, err := s.pool.Query(ctx,
		`SELECT nsp.place_id, p.kind, nsp.level
		 FROM notification_setting_places nsp
		 JOIN places p USING (place_id)
		 WHERE nsp.member_kind = $1 AND nsp.member_id = $2
		 ORDER BY nsp.place_id`,
		owner.Kind, owner.ID)
	if err != nil {
		return NotificationSetting{}, fmt.Errorf("query notification places: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var entry PlaceNotifyLevel
		if err := rows.Scan(&entry.PlaceID, &entry.PlaceKind, &entry.Level); err != nil {
			return NotificationSetting{}, fmt.Errorf("scan notification place: %w", err)
		}
		setting.PerPlace = append(setting.PerPlace, entry)
	}
	if err := rows.Err(); err != nil {
		return NotificationSetting{}, fmt.Errorf("iterate notification places: %w", err)
	}
	return setting, nil
}

// SetNotificationSetting replaces the owner's whole setting. The owner is the
// authenticated caller, never a request field — nobody configures anyone else's
// attention. A per-place entry for a place the owner cannot see is reported as
// ErrPlaceNotFound, the same answer every other read gives across that
// boundary, so the setting route cannot be used to probe for places.
func (s *Store) SetNotificationSetting(
	ctx context.Context, owner ParticipantRef, defaultLevel string, perPlace []PlaceNotifyLevel, keywords []string,
) (NotificationSetting, error) {
	if err := owner.Validate(); err != nil {
		return NotificationSetting{}, err
	}
	if defaultLevel == "" {
		defaultLevel = DefaultNotifyLevel
	}
	if err := ValidateNotifyLevel(defaultLevel); err != nil {
		return NotificationSetting{}, err
	}
	cleanKeywords, err := normalizeKeywords(keywords)
	if err != nil {
		return NotificationSetting{}, err
	}
	if err := s.participantExists(ctx, owner); err != nil {
		return NotificationSetting{}, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return NotificationSetting{}, fmt.Errorf("begin set notification setting: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		`INSERT INTO notification_settings (member_kind, member_id, defaults_level, keywords)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (member_kind, member_id)
		 DO UPDATE SET defaults_level = EXCLUDED.defaults_level,
		               keywords = EXCLUDED.keywords, updated_at = now()`,
		owner.Kind, owner.ID, defaultLevel, cleanKeywords); err != nil {
		return NotificationSetting{}, fmt.Errorf("upsert notification setting: %w", err)
	}
	if _, err := tx.Exec(ctx,
		"DELETE FROM notification_setting_places WHERE member_kind = $1 AND member_id = $2",
		owner.Kind, owner.ID); err != nil {
		return NotificationSetting{}, fmt.Errorf("clear notification places: %w", err)
	}

	stored := NotificationSetting{Owner: owner, DefaultLevel: defaultLevel, Keywords: cleanKeywords}
	seen := map[string]bool{}
	for _, entry := range perPlace {
		if err := ValidateNotifyLevel(entry.Level); err != nil {
			return NotificationSetting{}, err
		}
		if seen[entry.PlaceID] {
			return NotificationSetting{}, fmt.Errorf(
				"%w: place %s appears twice in per_place", ErrInvalidNotificationSetting, entry.PlaceID)
		}
		seen[entry.PlaceID] = true
		place, err := s.loadPlace(ctx, tx, entry.PlaceID)
		if err != nil {
			return NotificationSetting{}, err
		}
		visible, err := s.canAccess(ctx, tx, place, owner)
		if err != nil {
			return NotificationSetting{}, err
		}
		if !visible {
			return NotificationSetting{}, ErrPlaceNotFound
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO notification_setting_places (member_kind, member_id, place_id, level)
			 VALUES ($1, $2, $3, $4)`,
			owner.Kind, owner.ID, entry.PlaceID, entry.Level); err != nil {
			return NotificationSetting{}, fmt.Errorf("insert notification place: %w", err)
		}
		stored.PerPlace = append(stored.PerPlace, PlaceNotifyLevel{
			PlaceID: entry.PlaceID, PlaceKind: place.Kind, Level: entry.Level,
		})
	}
	if err := tx.Commit(ctx); err != nil {
		return NotificationSetting{}, fmt.Errorf("commit set notification setting: %w", err)
	}
	return stored, nil
}

// NotificationDecisionsFor evaluates, for one committed message, which of the
// place's active members should be interrupted and why. It runs on the server
// because the decision belongs to the receiver: a client-side filter would
// still have carried the muted place's content to that person's device before
// discarding it, which is not 受信側制御 at all.
//
// Priority is dm > mention > keyword > all, and mute suppresses everything —
// silencing a place means silence, not "silence unless someone insists". The
// author is never notified of their own message: writing is not being called.
func (s *Store) NotificationDecisionsFor(ctx context.Context, place Place, msg Message) ([]NotificationDecision, error) {
	members, err := s.activeMembers(ctx, s.pool, place)
	if err != nil {
		return nil, err
	}
	candidates := make([]ParticipantRef, 0, len(members))
	for _, member := range members {
		if member.Participant == msg.Author {
			continue
		}
		candidates = append(candidates, member.Participant)
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	settings, err := s.notificationSettingsFor(ctx, place.PlaceID, candidates)
	if err != nil {
		return nil, err
	}
	mentioned := make(map[string]bool, len(msg.Mentions))
	for _, ref := range msg.Mentions {
		mentioned[ref.Key()] = true
	}
	lowered := strings.ToLower(msg.Content)

	decisions := make([]NotificationDecision, 0, len(candidates))
	for _, candidate := range candidates {
		resolved := settings[candidate.Key()]
		if resolved.level == NotifyLevelMute {
			continue
		}
		reason := ""
		switch {
		case place.Kind != PlaceChannel:
			reason = NotifyReasonDM
		case mentioned[candidate.Key()]:
			reason = NotifyReasonMention
		case matchesKeyword(lowered, resolved.keywords):
			reason = NotifyReasonKeyword
		case resolved.level == NotifyLevelAll:
			reason = NotifyReasonAll
		}
		if reason == "" {
			continue
		}
		decisions = append(decisions, NotificationDecision{Participant: candidate, Reason: reason})
	}
	return decisions, nil
}

// --- internals ---

// resolvedSetting is one candidate's effective level for one place plus their
// keyword list, already reduced from the two setting tables.
type resolvedSetting struct {
	level    string
	keywords []string
}

// notificationSettingsFor loads the effective setting of every candidate for
// one place in a single round trip. Candidates without a row keep the default.
func (s *Store) notificationSettingsFor(
	ctx context.Context, placeID string, candidates []ParticipantRef,
) (map[string]resolvedSetting, error) {
	out := make(map[string]resolvedSetting, len(candidates))
	var humanIDs, agentIDs []string
	for _, candidate := range candidates {
		out[candidate.Key()] = resolvedSetting{level: DefaultNotifyLevel}
		switch candidate.Kind {
		case KindHuman:
			humanIDs = append(humanIDs, candidate.ID)
		case KindPersonalityAgent:
			agentIDs = append(agentIDs, candidate.ID)
		}
	}
	rows, err := s.pool.Query(ctx,
		`SELECT ns.member_kind, ns.member_id, ns.defaults_level, ns.keywords, nsp.level
		 FROM notification_settings ns
		 LEFT JOIN notification_setting_places nsp
		   ON nsp.member_kind = ns.member_kind AND nsp.member_id = ns.member_id
		  AND nsp.place_id = $1
		 WHERE (ns.member_kind = 'human' AND ns.member_id = ANY($2))
		    OR (ns.member_kind = 'personality_agent' AND ns.member_id = ANY($3))`,
		placeID, humanIDs, agentIDs)
	if err != nil {
		return nil, fmt.Errorf("query notification settings: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			kind         string
			id           string
			defaultLevel string
			keywords     []string
			placeLevel   *string
		)
		if err := rows.Scan(&kind, &id, &defaultLevel, &keywords, &placeLevel); err != nil {
			return nil, fmt.Errorf("scan notification setting: %w", err)
		}
		key := ParticipantRef{Kind: ParticipantKind(kind), ID: id}.Key()
		if _, wanted := out[key]; !wanted {
			continue
		}
		level := defaultLevel
		if placeLevel != nil {
			level = *placeLevel
		}
		out[key] = resolvedSetting{level: level, keywords: keywords}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate notification settings: %w", err)
	}
	return out, nil
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
