package messaging

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
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
// ExpiresAt holds until replaced. A temporary status carries the state it will
// lapse back to, so「1時間だけ取り込み中」returns the participant to what they
// had already said rather than to nothing (the platform never edits a
// self-declaration on someone's behalf).
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

// SetStatus upserts the actor's own status. Only the actor can set it — the
// participant is the authenticated caller, never a request field (自己申告:
// the platform does not speak about anyone's attention on their behalf).
//
// A temporary status (expiresAt != nil) remembers the state it replaces, so it
// lapses back to what the participant had already said. Setting a lasting
// status clears that memory: the new declaration is the whole truth.
func (s *Store) SetStatus(ctx context.Context, actor ParticipantRef, status, note string, expiresAt *time.Time) (ParticipantStatus, error) {
	if err := actor.Validate(); err != nil {
		return ParticipantStatus{}, err
	}
	if !ValidStatus(status) {
		return ParticipantStatus{}, fmt.Errorf("unknown status %q", status)
	}
	if utf8.RuneCountInString(note) > MaxStatusNoteChars {
		return ParticipantStatus{}, fmt.Errorf("note exceeds %d characters", MaxStatusNoteChars)
	}
	if err := s.participantExists(ctx, actor); err != nil {
		return ParticipantStatus{}, err
	}
	next := ParticipantStatus{Participant: actor, Status: status, Note: note, ExpiresAt: expiresAt}
	if expiresAt != nil {
		base, err := s.lastingStatusOf(ctx, actor)
		if err != nil {
			return ParticipantStatus{}, err
		}
		next.BaseStatus = base.Status
		next.BaseNote = base.Note
	}
	var (
		baseStatus *string
		baseNote   = next.BaseNote
	)
	if next.BaseStatus != "" {
		baseStatus = &next.BaseStatus
	} else {
		baseNote = ""
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO participant_statuses
		     (member_kind, member_id, status, note, expires_at, base_status, base_note)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (member_kind, member_id)
		 DO UPDATE SET status = EXCLUDED.status, note = EXCLUDED.note,
		               expires_at = EXCLUDED.expires_at,
		               base_status = EXCLUDED.base_status,
		               base_note = EXCLUDED.base_note,
		               updated_at = now()`,
		actor.Kind, actor.ID, status, note, expiresAt, baseStatus, baseNote)
	if err != nil {
		return ParticipantStatus{}, fmt.Errorf("set status: %w", err)
	}
	next.BaseNote = baseNote
	return next, nil
}

// lastingStatusOf returns what the participant would be back to once every
// temporary status they have set has lapsed. Replacing one temporary status
// with another keeps that answer, so two short states in a row cannot bury the
// lasting one the participant actually chose to hold.
func (s *Store) lastingStatusOf(ctx context.Context, participant ParticipantRef) (ParticipantStatus, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT status, note, expires_at, base_status, base_note
		 FROM participant_statuses
		 WHERE member_kind = $1 AND member_id = $2`,
		participant.Kind, participant.ID)
	var stored storedStatus
	if err := row.Scan(&stored.status, &stored.note, &stored.expiresAt,
		&stored.baseStatus, &stored.baseNote); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ParticipantStatus{Participant: participant}, nil
		}
		return ParticipantStatus{}, fmt.Errorf("read status: %w", err)
	}
	now := time.Now()
	if stored.expiresAt != nil && stored.expiresAt.After(now) {
		// Still holding: what lies underneath is its own base, not itself.
		out := ParticipantStatus{Participant: participant}
		if stored.baseStatus != nil {
			out.Status = *stored.baseStatus
			out.Note = stored.baseNote
		}
		return out, nil
	}
	return stored.resolve(participant, now), nil
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

// StatusesVisibleTo lists the current statuses of every participant the viewer
// can see — themselves, anyone sharing an active workspace, and anyone sharing
// an active dm/group_dm place. The same basis gates the live status_updated
// fan-out (ParticipantVisible), so bootstrap and the socket can never disagree
// about who may see whose self-declared state.
//
// Expiry is resolved here, at read time: a lapsed temporary status reports the
// state it lapses back to, or nothing when there is none. The background
// sweeper only makes that same answer durable and announces it — it can never
// disagree with what a reader would have computed anyway.
func (s *Store) StatusesVisibleTo(ctx context.Context, viewer ParticipantRef) ([]ParticipantStatus, error) {
	if err := viewer.Validate(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT ps.member_kind, ps.member_id, ps.status, ps.note, ps.expires_at,
		        ps.base_status, ps.base_note
		 FROM participant_statuses ps
		 WHERE ((ps.member_kind = $1 AND ps.member_id = $2)
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
	now := time.Now()
	var out []ParticipantStatus
	for rows.Next() {
		var (
			participant ParticipantRef
			kind        string
			stored      storedStatus
		)
		if err := rows.Scan(&kind, &participant.ID, &stored.status, &stored.note,
			&stored.expiresAt, &stored.baseStatus, &stored.baseNote); err != nil {
			return nil, fmt.Errorf("scan status: %w", err)
		}
		participant.Kind = ParticipantKind(kind)
		resolved := stored.resolve(participant, now)
		// A lapsed status with nothing behind it says nothing about the
		// participant, so it is not reported at all.
		if resolved.Status == "" {
			continue
		}
		out = append(out, resolved)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate statuses: %w", err)
	}
	return out, nil
}

// ExpireStatuses makes lapsed temporary statuses durable and reports what each
// participant now says. Rows that lapse back to a base keep that base as their
// lasting state; rows with nothing behind them are removed, because a status
// that no longer holds is not a statement about anyone.
//
// It returns only what actually changed, so the caller announces one
// status_updated per participant whose visible state moved. Readers already saw
// the same answer through StatusesVisibleTo, so a sweep that never runs costs
// correctness nothing — only the liveness of the announcement.
func (s *Store) ExpireStatuses(ctx context.Context) ([]ParticipantStatus, error) {
	rows, err := s.pool.Query(ctx,
		`WITH lapsed AS (
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
	defer rows.Close()
	var out []ParticipantStatus
	for rows.Next() {
		var (
			participant ParticipantRef
			kind        string
			baseStatus  *string
			baseNote    string
		)
		if err := rows.Scan(&kind, &participant.ID, &baseStatus, &baseNote); err != nil {
			return nil, fmt.Errorf("scan expired status: %w", err)
		}
		participant.Kind = ParticipantKind(kind)
		status := ParticipantStatus{Participant: participant}
		if baseStatus != nil {
			status.Status = *baseStatus
			status.Note = baseNote
		}
		out = append(out, status)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate expired statuses: %w", err)
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
