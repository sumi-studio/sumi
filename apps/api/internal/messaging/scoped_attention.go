package messaging

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

func (s *ScopedStore) NotificationSettingFor(ctx context.Context) (NotificationSetting, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return NotificationSetting{}, fmt.Errorf("begin scoped notification-setting read: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeInTx(ctx, tx); err != nil {
		return NotificationSetting{}, err
	}
	setting := NotificationSetting{Owner: s.Scope.Actor, DefaultLevel: DefaultNotifyLevel, Keywords: []string{}}
	var keywords []string
	err = tx.QueryRow(ctx, `
		SELECT defaults_level, keywords FROM notification_settings
		WHERE workspace_id = $1 AND member_kind = $2 AND member_id = $3`,
		s.Scope.WorkspaceID, s.Scope.Actor.Kind, s.Scope.Actor.ID).Scan(&setting.DefaultLevel, &keywords)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return NotificationSetting{}, fmt.Errorf("load scoped notification setting: %w", err)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return NotificationSetting{}, err
		}
		return setting, nil
	}
	if keywords != nil {
		setting.Keywords = keywords
	}
	rows, err := tx.Query(ctx, `
		SELECT nsp.place_id, p.kind, nsp.level
		FROM notification_setting_places nsp
		JOIN places p ON p.workspace_id = nsp.workspace_id AND p.place_id = nsp.place_id
		WHERE nsp.workspace_id = $1 AND nsp.member_kind = $2 AND nsp.member_id = $3
		ORDER BY nsp.place_id`, s.Scope.WorkspaceID, s.Scope.Actor.Kind, s.Scope.Actor.ID)
	if err != nil {
		return NotificationSetting{}, fmt.Errorf("query scoped notification places: %w", err)
	}
	for rows.Next() {
		var entry PlaceNotifyLevel
		if err := rows.Scan(&entry.PlaceID, &entry.PlaceKind, &entry.Level); err != nil {
			rows.Close()
			return NotificationSetting{}, fmt.Errorf("scan scoped notification place: %w", err)
		}
		setting.PerPlace = append(setting.PerPlace, entry)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return NotificationSetting{}, fmt.Errorf("iterate scoped notification places: %w", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return NotificationSetting{}, err
	}
	return setting, nil
}

func (s *ScopedStore) SetNotificationSetting(ctx context.Context, defaultLevel string, perPlace []PlaceNotifyLevel, keywords []string) (NotificationSetting, error) {
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
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return NotificationSetting{}, fmt.Errorf("begin set scoped notification setting: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeMutationInTx(ctx, tx); err != nil {
		return NotificationSetting{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO notification_settings
			(workspace_id, member_kind, member_id, defaults_level, keywords)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (workspace_id, member_kind, member_id)
		DO UPDATE SET defaults_level = EXCLUDED.defaults_level,
		              keywords = EXCLUDED.keywords, updated_at = now()`,
		s.Scope.WorkspaceID, s.Scope.Actor.Kind, s.Scope.Actor.ID, defaultLevel, cleanKeywords); err != nil {
		return NotificationSetting{}, fmt.Errorf("upsert scoped notification setting: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM notification_setting_places
		WHERE workspace_id = $1 AND member_kind = $2 AND member_id = $3`,
		s.Scope.WorkspaceID, s.Scope.Actor.Kind, s.Scope.Actor.ID); err != nil {
		return NotificationSetting{}, fmt.Errorf("clear scoped notification places: %w", err)
	}
	stored := NotificationSetting{Owner: s.Scope.Actor, DefaultLevel: defaultLevel, Keywords: cleanKeywords}
	seen := map[string]bool{}
	for _, entry := range perPlace {
		if err := ValidateNotifyLevel(entry.Level); err != nil {
			return NotificationSetting{}, err
		}
		if seen[entry.PlaceID] {
			return NotificationSetting{}, fmt.Errorf("%w: place %s appears twice", ErrInvalidNotificationSetting, entry.PlaceID)
		}
		seen[entry.PlaceID] = true
		place, err := s.loadScopedPlace(ctx, tx, entry.PlaceID)
		if err != nil {
			return NotificationSetting{}, err
		}
		if _, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor); err != nil {
			return NotificationSetting{}, err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO notification_setting_places
				(workspace_id, member_kind, member_id, place_id, level)
			VALUES ($1, $2, $3, $4, $5)`,
			s.Scope.WorkspaceID, s.Scope.Actor.Kind, s.Scope.Actor.ID, entry.PlaceID, entry.Level); err != nil {
			return NotificationSetting{}, fmt.Errorf("insert scoped notification place: %w", err)
		}
		stored.PerPlace = append(stored.PerPlace, PlaceNotifyLevel{PlaceID: entry.PlaceID, PlaceKind: place.Kind, Level: entry.Level})
	}
	if err := tx.Commit(ctx); err != nil {
		return NotificationSetting{}, fmt.Errorf("commit scoped notification setting: %w", err)
	}
	return stored, nil
}

func (s *ScopedStore) SetStatus(ctx context.Context, status, note string, expiresAt *time.Time) (ParticipantStatus, error) {
	switch status {
	case StatusAvailable, StatusBusy, StatusAway:
	default:
		return ParticipantStatus{}, fmt.Errorf("unknown status %q", status)
	}
	if utf8.RuneCountInString(note) > MaxStatusNoteChars {
		return ParticipantStatus{}, fmt.Errorf("note exceeds %d characters", MaxStatusNoteChars)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ParticipantStatus{}, fmt.Errorf("begin set scoped status: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeMutationInTx(ctx, tx); err != nil {
		return ParticipantStatus{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO participant_statuses (member_kind, member_id, status, note, expires_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (member_kind, member_id)
		DO UPDATE SET status = EXCLUDED.status, note = EXCLUDED.note,
		              expires_at = EXCLUDED.expires_at, updated_at = now()`,
		s.Scope.Actor.Kind, s.Scope.Actor.ID, status, note, expiresAt); err != nil {
		return ParticipantStatus{}, fmt.Errorf("set scoped status: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ParticipantStatus{}, fmt.Errorf("commit scoped status: %w", err)
	}
	return ParticipantStatus{Participant: s.Scope.Actor, Status: status, Note: note, ExpiresAt: expiresAt}, nil
}

func (s *ScopedStore) StatusesVisibleTo(ctx context.Context) ([]ParticipantStatus, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin scoped statuses read: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeInTx(ctx, tx); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT ps.member_kind, ps.member_id, ps.status, ps.note, ps.expires_at
		FROM participant_statuses ps
		JOIN workspace_members wm
		  ON wm.member_kind = ps.member_kind AND wm.member_id = ps.member_id
		WHERE wm.workspace_id = $1 AND wm.left_at IS NULL
		  AND (ps.expires_at IS NULL OR ps.expires_at > now())
		ORDER BY ps.member_kind, ps.member_id`, s.Scope.WorkspaceID)
	if err != nil {
		return nil, fmt.Errorf("query scoped statuses: %w", err)
	}
	defer rows.Close()
	var statuses []ParticipantStatus
	for rows.Next() {
		var status ParticipantStatus
		if err := rows.Scan(&status.Participant.Kind, &status.Participant.ID, &status.Status, &status.Note, &status.ExpiresAt); err != nil {
			return nil, fmt.Errorf("scan scoped status: %w", err)
		}
		statuses = append(statuses, status)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return statuses, nil
}

func (s *ScopedStore) ParticipantVisible(ctx context.Context, target ParticipantRef) (bool, error) {
	if err := target.Validate(); err != nil {
		return false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeInTx(ctx, tx); err != nil {
		return false, err
	}
	var visible bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM workspace_members
			WHERE workspace_id = $1 AND member_kind = $2 AND member_id = $3 AND left_at IS NULL
		)`, s.Scope.WorkspaceID, target.Kind, target.ID).Scan(&visible); err != nil {
		return false, fmt.Errorf("check scoped participant visibility: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return visible, nil
}

func (s *ScopedStore) CreateReplyLater(ctx context.Context, placeID, messageID, note string, remindAt time.Time) (ReplyLaterMarker, bool, error) {
	if note == "" {
		note = DefaultReplyLaterNote
	}
	if utf8.RuneCountInString(note) > MaxReplyLaterNoteChars || remindAt.IsZero() {
		return ReplyLaterMarker{}, false, errors.New("invalid reply-later marker")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ReplyLaterMarker{}, false, fmt.Errorf("begin scoped reply-later: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeMutationInTx(ctx, tx); err != nil {
		return ReplyLaterMarker{}, false, err
	}
	place, err := s.loadScopedPlace(ctx, tx, placeID)
	if err != nil {
		return ReplyLaterMarker{}, false, err
	}
	access, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor)
	if err != nil {
		return ReplyLaterMarker{}, false, err
	}
	message, err := lockMessageScoped(ctx, tx, s.Scope.WorkspaceID, placeID, messageID)
	if err != nil || message.Seq < access.VisibleFromSeq {
		return ReplyLaterMarker{}, false, ErrMessageNotFound
	}
	if message.Deleted {
		return ReplyLaterMarker{}, false, ErrMessageDeleted
	}
	marker := ReplyLaterMarker{Participant: s.Scope.Actor, PlaceID: placeID, PlaceKind: place.Kind, MessageID: messageID}
	err = tx.QueryRow(ctx, `
		SELECT marker_id, note, remind_at FROM reply_later_markers
		WHERE message_id = $1 AND member_kind = $2 AND member_id = $3 AND resolved_at IS NULL`,
		messageID, s.Scope.Actor.Kind, s.Scope.Actor.ID).Scan(&marker.MarkerID, &marker.Note, &marker.RemindAt)
	if err == nil {
		return marker, false, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ReplyLaterMarker{}, false, err
	}
	marker.MarkerID, marker.Note, marker.RemindAt = newUUIDv7(), note, remindAt
	if _, err := tx.Exec(ctx, `
		INSERT INTO reply_later_markers
			(marker_id, member_kind, member_id, place_id, message_id, note, remind_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		marker.MarkerID, s.Scope.Actor.Kind, s.Scope.Actor.ID, placeID, messageID, note, remindAt); err != nil {
		return ReplyLaterMarker{}, false, fmt.Errorf("insert scoped reply-later: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ReplyLaterMarker{}, false, err
	}
	return marker, true, nil
}

func (s *ScopedStore) ResolveReplyLater(ctx context.Context, markerID string) (ReplyLaterMarker, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ReplyLaterMarker{}, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeMutationInTx(ctx, tx); err != nil {
		return ReplyLaterMarker{}, err
	}
	marker := ReplyLaterMarker{MarkerID: markerID, Participant: s.Scope.Actor}
	var resolvedAt *time.Time
	err = tx.QueryRow(ctx, `
		SELECT rl.place_id, p.kind, rl.message_id, rl.note, rl.remind_at, rl.resolved_at
		FROM reply_later_markers rl
		JOIN places p ON p.place_id = rl.place_id AND p.workspace_id = $1
		JOIN messages m ON m.message_id = rl.message_id AND m.workspace_id = $1
		WHERE rl.marker_id = $2 AND rl.member_kind = $3 AND rl.member_id = $4
		FOR UPDATE OF rl`, s.Scope.WorkspaceID, markerID, s.Scope.Actor.Kind, s.Scope.Actor.ID).Scan(
		&marker.PlaceID, &marker.PlaceKind, &marker.MessageID, &marker.Note, &marker.RemindAt, &resolvedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ReplyLaterMarker{}, ErrMarkerNotFound
	}
	if err != nil {
		return ReplyLaterMarker{}, err
	}
	if _, err := s.placeAccessAfterAuthorization(ctx, tx, Place{PlaceID: marker.PlaceID, Kind: marker.PlaceKind, WorkspaceID: s.Scope.WorkspaceID}, s.Scope.Actor); err != nil {
		return ReplyLaterMarker{}, ErrMarkerNotFound
	}
	marker.Resolved = true
	if resolvedAt == nil {
		if _, err := tx.Exec(ctx, `UPDATE reply_later_markers SET resolved_at = now() WHERE marker_id = $1`, markerID); err != nil {
			return ReplyLaterMarker{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ReplyLaterMarker{}, err
	}
	return marker, nil
}

func (s *ScopedStore) ReplyLaterMarkersFor(ctx context.Context) ([]ReplyLaterMarker, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	membership, err := s.authorizeInTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		WITH visible_places AS (
			SELECT p.place_id, p.kind, pm.place_member_id IS NOT NULL AS participates FROM places p
			LEFT JOIN place_members pm ON pm.workspace_id = p.workspace_id
				 AND pm.place_id = p.place_id AND pm.workspace_member_id = $2 AND pm.left_at IS NULL
			WHERE p.workspace_id = $1
				 AND (p.kind IN ('channel', 'thread') OR (p.kind IN ('dm', 'group_dm') AND pm.place_member_id IS NOT NULL))
		)
		SELECT rl.marker_id, rl.member_kind, rl.member_id, rl.place_id, vp.kind,
		       rl.message_id, rl.note, rl.remind_at
		FROM reply_later_markers rl JOIN visible_places vp ON vp.place_id = rl.place_id
		WHERE rl.resolved_at IS NULL
		  AND (vp.kind <> 'thread' OR vp.participates OR (rl.member_kind = $3 AND rl.member_id = $4))
		ORDER BY rl.marker_id`, s.Scope.WorkspaceID, membership.WorkspaceMemberID, s.Scope.Actor.Kind, s.Scope.Actor.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var markers []ReplyLaterMarker
	for rows.Next() {
		var marker ReplyLaterMarker
		if err := rows.Scan(&marker.MarkerID, &marker.Participant.Kind, &marker.Participant.ID,
			&marker.PlaceID, &marker.PlaceKind, &marker.MessageID, &marker.Note, &marker.RemindAt); err != nil {
			return nil, err
		}
		markers = append(markers, marker)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return markers, nil
}

// scopedNotificationSettingsFor is used only inside the message transaction.
func (s *ScopedStore) scopedNotificationSettingsFor(ctx context.Context, q querier, placeID string, candidates []ParticipantRef) (map[string]resolvedSetting, error) {
	out := make(map[string]resolvedSetting, len(candidates))
	var humanIDs, agentIDs []string
	for _, candidate := range candidates {
		out[candidate.Key()] = resolvedSetting{level: DefaultNotifyLevel}
		if candidate.Kind == KindHuman {
			humanIDs = append(humanIDs, candidate.ID)
		} else {
			agentIDs = append(agentIDs, candidate.ID)
		}
	}
	rows, err := q.Query(ctx, `
		SELECT ns.member_kind, ns.member_id, ns.defaults_level, ns.keywords, nsp.level
		FROM notification_settings ns
		LEFT JOIN notification_setting_places nsp
		 ON nsp.workspace_id = ns.workspace_id AND nsp.member_kind = ns.member_kind
		 AND nsp.member_id = ns.member_id AND nsp.place_id = $2
		WHERE ns.workspace_id = $1 AND (
		 (ns.member_kind = 'human' AND ns.member_id = ANY($3)) OR
		 (ns.member_kind = 'personality_agent' AND ns.member_id = ANY($4)))`,
		s.Scope.WorkspaceID, placeID, humanIDs, agentIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var kind, id, defaultLevel string
		var keywords []string
		var placeLevel *string
		if err := rows.Scan(&kind, &id, &defaultLevel, &keywords, &placeLevel); err != nil {
			return nil, err
		}
		level := defaultLevel
		if placeLevel != nil {
			level = *placeLevel
		}
		out[ParticipantRef{Kind: ParticipantKind(kind), ID: id}.Key()] = resolvedSetting{level: level, keywords: keywords}
	}
	return out, rows.Err()
}

func (s *ScopedStore) issueScopedNotificationIntents(ctx context.Context, tx pgx.Tx, place Place, message Message, members []MemberProfile) error {
	decisions, err := s.notificationDecisionsForMembersScoped(ctx, tx, place, message, members)
	if err != nil {
		return fmt.Errorf("evaluate scoped notification intents: %w", err)
	}
	for _, decision := range decisions {
		if _, err := tx.Exec(ctx, `
			INSERT INTO message_notification_intents
				(message_id, recipient_kind, recipient_id, reason)
			VALUES ($1, $2, $3, $4)`, message.MessageID,
			decision.Participant.Kind, decision.Participant.ID, decision.Reason); err != nil {
			return fmt.Errorf("issue scoped notification intent: %w", err)
		}
	}
	return nil
}

func (s *ScopedStore) notificationDecisionsForMembersScoped(ctx context.Context, q querier, place Place, message Message, members []MemberProfile) ([]NotificationDecision, error) {
	candidates := make([]ParticipantRef, 0, len(members))
	for _, member := range members {
		if member.Participant != message.Author {
			candidates = append(candidates, member.Participant)
		}
	}
	settings, err := s.scopedNotificationSettingsFor(ctx, q, place.PlaceID, candidates)
	if err != nil {
		return nil, err
	}
	mentioned := map[string]bool{}
	for _, ref := range message.Mentions {
		mentioned[ref.Key()] = true
	}
	lowered := strings.ToLower(message.Content)
	decisions := make([]NotificationDecision, 0, len(candidates))
	for _, candidate := range candidates {
		setting := settings[candidate.Key()]
		if setting.level == NotifyLevelMute {
			continue
		}
		reason := ""
		switch {
		case place.Kind == PlaceDM || place.Kind == PlaceGroupDM:
			reason = NotifyReasonDM
		case mentioned[candidate.Key()]:
			reason = NotifyReasonMention
		case matchesKeyword(lowered, setting.keywords):
			reason = NotifyReasonKeyword
		case setting.level == NotifyLevelAll:
			reason = NotifyReasonAll
		}
		if reason != "" {
			decisions = append(decisions, NotificationDecision{Participant: candidate, Reason: reason})
		}
	}
	return decisions, nil
}

func (s *ScopedStore) NotificationDecisionsFor(ctx context.Context, place Place, message Message) ([]NotificationDecision, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeInTx(ctx, tx); err != nil {
		return nil, err
	}
	loaded, err := s.loadScopedPlace(ctx, tx, place.PlaceID)
	if err != nil {
		return nil, err
	}
	if _, err := s.placeAccessAfterAuthorization(ctx, tx, loaded, s.Scope.Actor); err != nil {
		return nil, err
	}
	members, err := s.activeMembersScoped(ctx, tx, loaded)
	if err != nil {
		return nil, err
	}
	if loaded.Kind == PlaceThread {
		members, err = s.threadNotificationMembers(ctx, tx, loaded.PlaceID, members)
		if err != nil {
			return nil, err
		}
	}
	decisions, err := s.notificationDecisionsForMembersScoped(ctx, tx, loaded, message, members)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return decisions, nil
}

func (s *ScopedStore) NotificationIntentsForMessage(ctx context.Context, messageID string) ([]NotificationDecision, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeInTx(ctx, tx); err != nil {
		return nil, err
	}
	var placeID string
	if err := tx.QueryRow(ctx, `
		SELECT place_id FROM messages
		WHERE workspace_id=$1 AND message_id=$2`,
		s.Scope.WorkspaceID, messageID).Scan(&placeID); errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrMessageNotFound
	} else if err != nil {
		return nil, err
	}
	place, err := s.loadScopedPlace(ctx, tx, placeID)
	if err != nil {
		return nil, err
	}
	if _, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor); err != nil {
		return nil, ErrMessageNotFound
	}
	rows, err := tx.Query(ctx, `
		SELECT recipient_kind, recipient_id, reason
		FROM message_notification_intents
		WHERE message_id=$1 ORDER BY recipient_kind, recipient_id`, messageID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []NotificationDecision
	for rows.Next() {
		var item NotificationDecision
		if err := rows.Scan(&item.Participant.Kind, &item.Participant.ID, &item.Reason); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}
