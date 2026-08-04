package messaging

import (
	"context"
	"fmt"
	"time"
	"unicode/utf8"
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

// SetStatus upserts the actor's own status. Only the actor can set it — the
// participant is the authenticated caller, never a request field (自己申告:
// the platform does not speak about anyone's attention on their behalf).
func (s *Store) SetStatus(ctx context.Context, actor ParticipantRef, status, note string, expiresAt *time.Time) (ParticipantStatus, error) {
	if err := actor.Validate(); err != nil {
		return ParticipantStatus{}, err
	}
	switch status {
	case StatusAvailable, StatusBusy, StatusAway:
	default:
		return ParticipantStatus{}, fmt.Errorf("unknown status %q", status)
	}
	if utf8.RuneCountInString(note) > MaxStatusNoteChars {
		return ParticipantStatus{}, fmt.Errorf("note exceeds %d characters", MaxStatusNoteChars)
	}
	if err := s.participantExists(ctx, actor); err != nil {
		return ParticipantStatus{}, err
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO participant_statuses (member_kind, member_id, status, note, expires_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (member_kind, member_id)
		 DO UPDATE SET status = EXCLUDED.status, note = EXCLUDED.note,
		               expires_at = EXCLUDED.expires_at, updated_at = now()`,
		actor.Kind, actor.ID, status, note, expiresAt)
	if err != nil {
		return ParticipantStatus{}, fmt.Errorf("set status: %w", err)
	}
	return ParticipantStatus{Participant: actor, Status: status, Note: note, ExpiresAt: expiresAt}, nil
}

// StatusesVisibleTo lists the current, unexpired statuses of every participant
// the viewer can see — themselves, anyone sharing an active workspace, and
// anyone sharing an active dm/group_dm place. The same basis gates the live
// status_updated fan-out (ParticipantVisible), so bootstrap and the socket
// can never disagree about who may see whose self-declared state.
func (s *Store) StatusesVisibleTo(ctx context.Context, viewer ParticipantRef) ([]ParticipantStatus, error) {
	if err := viewer.Validate(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT ps.member_kind, ps.member_id, ps.status, ps.note, ps.expires_at
		 FROM participant_statuses ps
		 WHERE (ps.expires_at IS NULL OR ps.expires_at > now())
		   AND ((ps.member_kind = $1 AND ps.member_id = $2)
		     OR EXISTS (
		       SELECT 1 FROM workspace_members wa
		       JOIN workspace_members wb ON wa.workspace_id = wb.workspace_id
		       WHERE wa.member_kind = $1 AND wa.member_id = $2 AND wa.left_at IS NULL
		         AND wb.member_kind = ps.member_kind AND wb.member_id = ps.member_id
		         AND wb.left_at IS NULL)
		     OR EXISTS (
		       SELECT 1 FROM place_members pa
		       JOIN place_members pb ON pa.place_id = pb.place_id
		       WHERE pa.member_kind = $1 AND pa.member_id = $2 AND pa.left_at IS NULL
		         AND pb.member_kind = ps.member_kind AND pb.member_id = ps.member_id
		         AND pb.left_at IS NULL))
		 ORDER BY ps.member_kind, ps.member_id`,
		viewer.Kind, viewer.ID)
	if err != nil {
		return nil, fmt.Errorf("query statuses: %w", err)
	}
	defer rows.Close()
	var out []ParticipantStatus
	for rows.Next() {
		var (
			status ParticipantStatus
			kind   string
		)
		if err := rows.Scan(&kind, &status.Participant.ID, &status.Status, &status.Note, &status.ExpiresAt); err != nil {
			return nil, fmt.Errorf("scan status: %w", err)
		}
		status.Participant.Kind = ParticipantKind(kind)
		out = append(out, status)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate statuses: %w", err)
	}
	return out, nil
}

// ParticipantVisible reports whether the viewer may see the target's
// self-declared attention state: themselves, an active shared workspace, or an
// active shared dm/group_dm place. It backs the hub's delivery decision for
// participant-scoped events the way place visibility backs place events.
func (s *Store) ParticipantVisible(ctx context.Context, viewer, target ParticipantRef) (bool, error) {
	if err := viewer.Validate(); err != nil {
		return false, err
	}
	if err := target.Validate(); err != nil {
		return false, err
	}
	if viewer.Key() == target.Key() {
		return true, nil
	}
	shared, err := s.shareActiveWorkspace(ctx, viewer, target)
	if err != nil || shared {
		return shared, err
	}
	var sharedPlace bool
	err = s.pool.QueryRow(ctx,
		`SELECT EXISTS (
		   SELECT 1 FROM place_members pa
		   JOIN place_members pb ON pa.place_id = pb.place_id
		   WHERE pa.member_kind = $1 AND pa.member_id = $2 AND pa.left_at IS NULL
		     AND pb.member_kind = $3 AND pb.member_id = $4 AND pb.left_at IS NULL)`,
		viewer.Kind, viewer.ID, target.Kind, target.ID).Scan(&sharedPlace)
	if err != nil {
		return false, fmt.Errorf("check shared place: %w", err)
	}
	return sharedPlace, nil
}
