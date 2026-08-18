package messaging

import (
	"context"
	"fmt"
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

// ValidStatus reports whether a value is one of the three self-declared
// states. There is no "offline" or "invisible": Sumi never observes presence,
// so there is nothing automatic to hide (契約ドラフト: Status は自己申告).
func ValidStatus(status string) bool {
	switch status {
	case StatusAvailable, StatusBusy, StatusAway:
		return true
	default:
		return false
	}
}

// ParticipantStatus is one participant's current self-declared status. A nil
// ExpiresAt holds until replaced. A temporary status carries the state it will
// lapse back to, so「1時間だけ取り込み中」returns the participant to what they
// had already said rather than to nothing — the platform never edits a
// self-declaration on someone's behalf, and「何も言っていない」is a different
// answer from「対応可能」.
type ParticipantStatus struct {
	Participant ParticipantRef
	Status      string
	Note        string
	ExpiresAt   *time.Time
	// BaseStatus is what this status lapses back to at ExpiresAt. Empty means
	// there was nothing to return to, and the lapse simply ends the
	// declaration. It is meaningless without ExpiresAt.
	BaseStatus string
	BaseNote   string
}

// storedStatus is the row as written. Readers never see it: everything goes
// through resolve, so no caller can accidentally report a lapsed declaration.
type storedStatus struct {
	status     string
	note       string
	expiresAt  *time.Time
	baseStatus *string
	baseNote   string
}

// resolve turns the stored row into what may be reported at `now`: the
// declared state while it holds, the base it lapses to once it has expired, or
// nothing at all when there was no base.
func (r storedStatus) resolve(participant ParticipantRef, now time.Time) ParticipantStatus {
	if r.expiresAt == nil || r.expiresAt.After(now) {
		out := ParticipantStatus{
			Participant: participant, Status: r.status, Note: r.note, ExpiresAt: r.expiresAt,
			BaseNote: r.baseNote,
		}
		if r.baseStatus != nil {
			out.BaseStatus = *r.baseStatus
		}
		return out
	}
	if r.baseStatus == nil {
		return ParticipantStatus{Participant: participant}
	}
	return ParticipantStatus{Participant: participant, Status: *r.baseStatus, Note: r.baseNote}
}

// lasting is what the participant would be back to once every temporary status
// they have set has lapsed. Replacing one temporary status with another keeps
// that answer, so two short states in a row cannot bury the lasting one the
// participant actually chose to hold.
func (r storedStatus) lasting(participant ParticipantRef, now time.Time) ParticipantStatus {
	if r.expiresAt != nil && r.expiresAt.After(now) {
		// Still holding: what lies underneath is its own base, not itself.
		out := ParticipantStatus{Participant: participant}
		if r.baseStatus != nil {
			out.Status = *r.baseStatus
			out.Note = r.baseNote
		}
		return out
	}
	return r.resolve(participant, now)
}

// StatusExpiry is one lapsed temporary status made durable, together with the
// exact Messaging app addresses that may be told about it. A sweep has no
// actor, so it carries the addresses rather than an actor-scoped store: the
// Hub still re-checks the audience inside each of them.
type StatusExpiry struct {
	Status ParticipantStatus
	Scopes []Scope
}

// ExpireStatuses makes lapsed temporary statuses durable and reports what each
// participant now says. Rows that lapse back to a base keep that base as their
// lasting state; rows with nothing behind them are removed, because a status
// that no longer holds is not a statement about anyone.
//
// Readers already resolve expiry themselves through StatusesVisibleTo, so a
// sweep that never runs costs correctness nothing — only the liveness of the
// announcement on a screen that is already open.
func (s *Store) ExpireStatuses(ctx context.Context) ([]StatusExpiry, error) {
	rows, err := s.pool.Query(ctx, `
		WITH lapsed AS (
		  SELECT member_kind, member_id, base_status, base_note
		  FROM participant_statuses
		  WHERE expires_at IS NOT NULL AND expires_at <= now()
		  FOR UPDATE
		), restored AS (
		  UPDATE participant_statuses ps
		  SET status = lapsed.base_status, note = lapsed.base_note,
		      expires_at = NULL, base_status = NULL, base_note = '', updated_at = now()
		  FROM lapsed
		  WHERE ps.member_kind = lapsed.member_kind AND ps.member_id = lapsed.member_id
		    AND lapsed.base_status IS NOT NULL
		  RETURNING ps.member_id
		), cleared AS (
		  DELETE FROM participant_statuses ps
		  USING lapsed
		  WHERE ps.member_kind = lapsed.member_kind AND ps.member_id = lapsed.member_id
		    AND lapsed.base_status IS NULL
		  RETURNING ps.member_id
		)
		SELECT member_kind, member_id, base_status, base_note FROM lapsed`)
	if err != nil {
		return nil, fmt.Errorf("expire statuses: %w", err)
	}
	var expiries []StatusExpiry
	for rows.Next() {
		var (
			participant ParticipantRef
			baseStatus  *string
			baseNote    string
		)
		if err := rows.Scan(&participant.Kind, &participant.ID, &baseStatus, &baseNote); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan expired status: %w", err)
		}
		status := ParticipantStatus{Participant: participant}
		if baseStatus != nil {
			status.Status = *baseStatus
			status.Note = baseNote
		}
		expiries = append(expiries, StatusExpiry{Status: status})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate expired statuses: %w", err)
	}
	rows.Close()
	for index := range expiries {
		scopes, err := s.messagingScopesOf(ctx, expiries[index].Status.Participant)
		if err != nil {
			return nil, err
		}
		expiries[index].Scopes = scopes
	}
	return expiries, nil
}

// messagingScopesOf lists the enabled Messaging installations of every
// Workspace the participant currently belongs to. Only these addresses can
// carry a statement about them, and each is re-authorized at delivery.
func (s *Store) messagingScopesOf(ctx context.Context, participant ParticipantRef) ([]Scope, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT wm.workspace_id, ai.installation_id, ai.authority_epoch
		FROM workspace_members wm
		JOIN app_installations ai
		  ON ai.owner_kind = 'workspace' AND ai.owner_id = wm.workspace_id
		 AND ai.app_id = $3 AND ai.enabled
		WHERE wm.member_kind = $1 AND wm.member_id = $2 AND wm.left_at IS NULL`,
		participant.Kind, participant.ID, MessagingAppID)
	if err != nil {
		return nil, fmt.Errorf("resolve status delivery scopes: %w", err)
	}
	defer rows.Close()
	var scopes []Scope
	for rows.Next() {
		var scope Scope
		if err := rows.Scan(&scope.WorkspaceID, &scope.InstallationID, &scope.AuthorityEpoch); err != nil {
			return nil, fmt.Errorf("scan status delivery scope: %w", err)
		}
		scopes = append(scopes, scope)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate status delivery scopes: %w", err)
	}
	return scopes, nil
}
