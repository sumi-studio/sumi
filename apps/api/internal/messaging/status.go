package messaging

import (
	"context"
	"fmt"
	"time"

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
	// Revision is allocated only by participant_statuses' database trigger.
	// Every projection, including an empty one after expiry, carries it so a
	// recipient can reject a delayed older state.
	Revision  int64
	Status    string
	Note      string
	ExpiresAt *time.Time
	// BaseStatus is what this status lapses back to at ExpiresAt. Empty means
	// there was nothing to return to, and the lapse simply ends the
	// declaration. It is meaningless without ExpiresAt.
	BaseStatus string
	BaseNote   string
}

// storedStatus is the row as written. Readers never see it: everything goes
// through resolve, so no caller can accidentally report a lapsed declaration.
type storedStatus struct {
	status     *string
	note       string
	expiresAt  *time.Time
	baseStatus *string
	baseNote   string
	revision   int64
}

// resolve turns the stored row into what may be reported at `now`: the
// declared state while it holds, the base it lapses to once it has expired, or
// nothing at all when there was no base.
func (r storedStatus) resolve(participant ParticipantRef, now time.Time) ParticipantStatus {
	if r.status == nil {
		return ParticipantStatus{Participant: participant, Revision: r.revision}
	}
	if r.expiresAt == nil || r.expiresAt.After(now) {
		out := ParticipantStatus{
			Participant: participant, Revision: r.revision, Status: *r.status, Note: r.note, ExpiresAt: r.expiresAt,
			BaseNote: r.baseNote,
		}
		if r.baseStatus != nil {
			out.BaseStatus = *r.baseStatus
		}
		return out
	}
	if r.baseStatus == nil {
		return ParticipantStatus{Participant: participant, Revision: r.revision}
	}
	return ParticipantStatus{Participant: participant, Revision: r.revision, Status: *r.baseStatus, Note: r.baseNote}
}

// StatusExpiry is one lapsed temporary status made durable, together with the
// exact Messaging app addresses that may be told about it. A sweep has no
// actor, so it carries the addresses rather than an actor-scoped store: the
// Hub still re-checks the audience inside each of them.
type StatusExpiry struct {
	Status ParticipantStatus
	Scopes []Scope
}

// ExpireStatuses makes lapsed temporary statuses durable and hands each one to
// `announce` before the transaction commits. Rows that lapse back to a base
// keep that base as their lasting state; rows with nothing behind them are
// removed, because a status that no longer holds is not a statement about
// anyone.
//
// What is announced is only what these statements actually changed, and only
// the values they themselves produced. The eligibility check lives in each
// statement's own WHERE rather than in an earlier read, so a participant who
// declares something new in the meantime simply leaves nothing to lapse: zero
// rows change and nothing is said about them.
//
// `announce` runs inside the transaction, with the affected rows still locked.
// That is what keeps a lapse from arriving after a newer declaration: any
// concurrent SetStatus is waiting at the same row lock and therefore cannot
// publish first. Announcing before the commit is safe here in a way it would
// not be elsewhere — the event states only what every reader already computes
// for itself from the declaration's own expiry and base, so a transaction that
// failed to commit could not leave a screen disagreeing with the durable
// answer.
//
// Readers resolve expiry themselves through StatusesVisibleTo, so a sweep that
// never runs costs correctness nothing.
func (s *Store) ExpireStatuses(
	ctx context.Context,
	announce func(context.Context, StatusExpiry),
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin expire statuses: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var expiries []StatusExpiry
	restored, err := tx.Query(ctx, `
		UPDATE participant_statuses
		SET status = base_status, note = base_note,
		    expires_at = NULL, base_status = NULL, base_note = '', updated_at = now()
		WHERE expires_at IS NOT NULL AND expires_at <= now()
		  AND base_status IS NOT NULL
		RETURNING member_kind, member_id, status, note, revision`)
	if err != nil {
		return fmt.Errorf("restore lapsed statuses: %w", err)
	}
	for restored.Next() {
		var status ParticipantStatus
		if err := restored.Scan(
			&status.Participant.Kind, &status.Participant.ID, &status.Status, &status.Note, &status.Revision,
		); err != nil {
			restored.Close()
			return fmt.Errorf("scan restored status: %w", err)
		}
		expiries = append(expiries, StatusExpiry{Status: status})
	}
	if err := restored.Err(); err != nil {
		restored.Close()
		return fmt.Errorf("iterate restored statuses: %w", err)
	}
	restored.Close()

	cleared, err := tx.Query(ctx, `
		UPDATE participant_statuses
		SET status = NULL, note = '', expires_at = NULL,
		    base_status = NULL, base_note = '', updated_at = now()
		WHERE expires_at IS NOT NULL AND expires_at <= now()
		  AND base_status IS NULL
		RETURNING member_kind, member_id, revision`)
	if err != nil {
		return fmt.Errorf("clear lapsed statuses: %w", err)
	}
	for cleared.Next() {
		// An empty status is how a clear says「もう何も言っていない」.
		var status ParticipantStatus
		if err := cleared.Scan(&status.Participant.Kind, &status.Participant.ID, &status.Revision); err != nil {
			cleared.Close()
			return fmt.Errorf("scan cleared status: %w", err)
		}
		expiries = append(expiries, StatusExpiry{Status: status})
	}
	if err := cleared.Err(); err != nil {
		cleared.Close()
		return fmt.Errorf("iterate cleared statuses: %w", err)
	}
	cleared.Close()

	for index := range expiries {
		scopes, err := s.messagingScopesOfInTx(ctx, tx, expiries[index].Status.Participant)
		if err != nil {
			return err
		}
		expiries[index].Scopes = scopes
		if announce != nil {
			announce(ctx, expiries[index])
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit expire statuses: %w", err)
	}
	return nil
}

// messagingScopesOfInTx lists the enabled Messaging installations of every
// Workspace the participant currently belongs to, read inside the caller's
// transaction. Only these addresses can carry a statement about them, and each
// is re-authorized at delivery.
func (s *Store) messagingScopesOfInTx(
	ctx context.Context,
	tx pgx.Tx,
	participant ParticipantRef,
) ([]Scope, error) {
	rows, err := tx.Query(ctx, `
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
