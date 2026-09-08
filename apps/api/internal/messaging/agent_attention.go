package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
)

const (
	AgentAttentionMessage  = "messaging_message"
	AgentAttentionMention  = "messaging_mention"
	AgentAttentionReminder = "reply_later_due"
)

// AgentAttentionEvent is frozen when the source is issued. It is private input
// to the recipient PA, not a browser command or an instruction from its employer.
// The delivery adapter adds its configured runtime tenant, not a guessed Human.
type AgentAttentionEvent struct {
	EventID            string              `json:"event_id"`
	Kind               string              `json:"kind"`
	PersonalityAgentID string              `json:"personality_agent_id"`
	WorkspaceID        string              `json:"workspace_id"`
	InstallationID     string              `json:"installation_id"`
	AuthorityEpoch     int64               `json:"authority_epoch"`
	Actor              AgentAttentionActor `json:"actor"`
	Place              AgentAttentionPlace `json:"place"`
	MessageID          string              `json:"message_id"`
	MessageRevision    int64               `json:"message_revision"`
	MessageSeq         int64               `json:"message_seq"`
	OccurredAt         time.Time           `json:"occurred_at"`
	Content            string              `json:"content"`
	MarkerID           string              `json:"marker_id,omitempty"`
	DueAt              *time.Time          `json:"due_at,omitempty"`
}

type AgentAttentionActor struct {
	Kind        string `json:"kind"`
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
}

type AgentAttentionPlace struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
	Name string `json:"name"`
}

type AgentAttentionReceipt struct {
	CommandID string
	Seq       uint64
}

// AgentAttentionDelivery is implemented by the internal gateway integration.
// Prepare restores the exact PA and returns its admission hold; it must not run
// a model or admit a command. Admit must fsync/deduplicate the exact event+key
// before returning a receipt. It is called under the source authorization lease.
// A successful Admit can be repeated after a DB commit failure with identical
// bytes/key. Receipt means durable admission, never reply/read/task completion.
type AgentAttentionDelivery interface {
	Prepare(context.Context, string) (release func(), err error)
	// Lookup reads an already-fsynced exact event/key without requiring a running
	// PA, delivering content anew, or allocating a command. An unavailable lookup
	// is an error, never evidence that the event was not admitted.
	Lookup(context.Context, string, AgentAttentionEvent) (AgentAttentionReceipt, bool, error)
	Admit(context.Context, string, AgentAttentionEvent) (AgentAttentionReceipt, error)
}

type AgentAttentionDeliveryStats struct {
	Admitted   int
	Suppressed int
	Retried    int
}

func (s *ScopedStore) attentionEvent(place Place, message Message, actor ParticipantRef, name string) AgentAttentionEvent {
	return AgentAttentionEvent{
		EventID: newUUIDv7(), WorkspaceID: s.Scope.WorkspaceID,
		InstallationID: s.Scope.InstallationID, AuthorityEpoch: s.Scope.AuthorityEpoch,
		Actor:     AgentAttentionActor{Kind: string(actor.Kind), ID: actor.ID, DisplayName: name},
		Place:     AgentAttentionPlace{ID: place.PlaceID, Kind: place.Kind, Name: place.Name},
		MessageID: message.MessageID, MessageRevision: message.Revision,
		MessageSeq: message.Seq, OccurredAt: message.CreatedAt, Content: message.Content,
	}
}

func (s *ScopedStore) issueAgentMessage(ctx context.Context, tx pgx.Tx, place Place, message Message, members []MemberProfile, decision NotificationDecision) error {
	if decision.Participant.Kind != KindPersonalityAgent {
		return nil
	}
	mentioned := false
	for _, ref := range message.Mentions {
		if ref == decision.Participant {
			mentioned = true
			break
		}
	}
	if !mentioned && decision.Reason != NotifyReasonDM {
		return nil
	}
	authorName := ""
	for _, member := range members {
		if member.Participant == message.Author {
			authorName = member.DisplayName
			break
		}
	}
	event := s.attentionEvent(place, message, message.Author, authorName)
	event.Kind, event.PersonalityAgentID = AgentAttentionMention, decision.Participant.ID
	if decision.Reason == NotifyReasonDM {
		event.Kind = AgentAttentionMessage
	}
	// A mention may just have joined its recipient to this thread. The member
	// profiles used to resolve names predate that join; capture the actual
	// post-admission tenure rather than freezing their former empty place ID.
	access, err := s.placeAccessAfterAuthorization(ctx, tx, place, decision.Participant)
	if err != nil {
		return err
	}
	return s.insertAgentAttention(ctx, tx, event, message.MessageID, message.Revision,
		access.WorkspaceMemberID, access.PlaceMemberID, message.CreatedAt)
}

func (s *ScopedStore) issueAgentReminder(ctx context.Context, tx pgx.Tx, place Place, message Message, marker ReplyLaterMarker, access PlaceAccess) error {
	if marker.Participant.Kind != KindPersonalityAgent {
		return nil
	}
	members, err := s.activeMembersScoped(ctx, tx, place)
	if err != nil {
		return err
	}
	name := ""
	for _, member := range members {
		if member.Participant == marker.Participant {
			name = member.DisplayName
			break
		}
	}
	event := s.attentionEvent(place, message, marker.Participant, name)
	event.Kind, event.PersonalityAgentID = AgentAttentionReminder, marker.Participant.ID
	event.MarkerID, event.Content, event.DueAt = marker.MarkerID, marker.Note, &marker.RemindAt
	// The PA created this reminder; its source message time is not creation time.
	if err := tx.QueryRow(ctx, "SELECT created_at FROM reply_later_markers WHERE marker_id=$1", marker.MarkerID).Scan(&event.OccurredAt); err != nil {
		return err
	}
	return s.insertAgentAttention(ctx, tx, event, marker.MarkerID, 1,
		access.WorkspaceMemberID, access.PlaceMemberID, marker.RemindAt)
}

func (s *ScopedStore) insertAgentAttention(ctx context.Context, tx pgx.Tx, event AgentAttentionEvent, sourceID string, revision int64, workspaceMemberID, placeMemberID string, availableAt time.Time) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO agent_attention_deliveries
        (event_id, personality_agent_id, source_kind, source_id, source_revision,
         workspace_id, installation_id, authority_epoch, workspace_member_id,
         place_member_id, place_id, message_id, payload, available_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NULLIF($10,'')::uuidv7,$11,$12,$13,$14)`,
		event.EventID, event.PersonalityAgentID, event.Kind, sourceID, revision,
		s.Scope.WorkspaceID, s.Scope.InstallationID, s.Scope.AuthorityEpoch,
		workspaceMemberID, placeMemberID, event.Place.ID, event.MessageID, payload, availableAt)
	return err
}

type attentionCandidate struct {
	event                            AgentAttentionEvent
	workspaceMemberID, placeMemberID string
}

// DeliverAgentAttention drains one bounded batch. Call on API startup and then
// periodically with the API lifetime context; browser sockets are not its owner.
// A transient failure on one source does not prevent other recipients' delivery.
func (s *Store) DeliverAgentAttention(ctx context.Context, delivery AgentAttentionDelivery, limit int) (AgentAttentionDeliveryStats, error) {
	var stats AgentAttentionDeliveryStats
	if delivery == nil {
		return stats, errors.New("agent attention delivery is unavailable")
	}
	if limit < 1 || limit > 100 {
		return stats, errors.New("agent attention batch must be 1..100")
	}
	rows, err := s.pool.Query(ctx, `SELECT payload, workspace_member_id, COALESCE(place_member_id::text,'')
        FROM agent_attention_deliveries
        WHERE admitted_at IS NULL AND suppressed_at IS NULL
          AND (available_at <= now() OR cancellation_requested_at IS NOT NULL)
          AND next_attempt_at <= now()
        ORDER BY next_attempt_at, available_at, event_id LIMIT $1`, limit)
	if err != nil {
		return stats, err
	}
	var pending []attentionCandidate
	for rows.Next() {
		var item attentionCandidate
		var payload []byte
		if err := rows.Scan(&payload, &item.workspaceMemberID, &item.placeMemberID); err != nil {
			rows.Close()
			return stats, err
		}
		if err := json.Unmarshal(payload, &item.event); err != nil {
			rows.Close()
			return stats, fmt.Errorf("decode stored attention: %w", err)
		}
		pending = append(pending, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return stats, err
	}
	var failures []error
	for _, item := range pending {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		outcome, err := s.deliverAgentAttention(ctx, delivery, item)
		if err == nil {
			switch outcome {
			case "admitted":
				stats.Admitted++
			case "suppressed":
				stats.Suppressed++
			}
			continue
		}
		stats.Retried++
		// Backoff is operational scheduling, not an instruction to the PA.
		_, retryErr := s.pool.Exec(ctx, `UPDATE agent_attention_deliveries
            SET attempt_count=LEAST(attempt_count+1,30),
                next_attempt_at=now()+make_interval(secs => LEAST(60, power(2, LEAST(attempt_count+1,6)))::int)
            WHERE event_id=$1 AND admitted_at IS NULL AND suppressed_at IS NULL`, item.event.EventID)
		failures = append(failures, err, retryErr)
	}
	return stats, errors.Join(failures...)
}

func (s *Store) deliverAgentAttention(ctx context.Context, delivery AgentAttentionDelivery, item attentionCandidate) (string, error) {
	// Reconcile old admissions and suppress lost sources without starting a PA.
	outcome, err := s.attemptAgentAttention(ctx, delivery, item, false)
	if err != nil || outcome != "ready" {
		return outcome, err
	}
	release, err := delivery.Prepare(ctx, item.event.PersonalityAgentID)
	if err != nil {
		return "", err
	}
	if release == nil {
		return "", errors.New("attention runtime admission hold is missing")
	}
	defer release()
	// Startup took place outside source locks. Recheck the source and receipt
	// under those locks before emitting a new command.
	return s.attemptAgentAttention(ctx, delivery, item, true)
}

func (s *Store) attemptAgentAttention(ctx context.Context, delivery AgentAttentionDelivery, item attentionCandidate, mayAdmit bool) (string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	scoped, err := s.Scoped(Scope{WorkspaceID: item.event.WorkspaceID,
		InstallationID: item.event.InstallationID, AuthorityEpoch: item.event.AuthorityEpoch,
		Actor: PersonalityAgent(item.event.PersonalityAgentID)})
	if err != nil {
		return "", err
	}
	// Match ordinary source mutation lock order. Source locks remain held through
	// Admit, so revocation either wins before admission or follows a lawful receipt.
	sourceErr := scoped.authorizeAttentionSource(ctx, tx, item)
	if sourceErr != nil && !attentionSourceUnavailable(sourceErr) {
		return "", sourceErr
	}
	var admitted, suppressed bool
	err = tx.QueryRow(ctx, `SELECT admitted_at IS NOT NULL, suppressed_at IS NOT NULL
        FROM agent_attention_deliveries WHERE event_id=$1 FOR UPDATE`, item.event.EventID).Scan(&admitted, &suppressed)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if admitted || suppressed {
		return "", nil
	}
	key := "attention:" + item.event.PersonalityAgentID + ":" + item.event.EventID
	receipt, found, err := delivery.Lookup(ctx, key, item.event)
	if err != nil {
		return "", err
	}
	// A prior authorized append remains a fact even if its DB acknowledgement
	// failed and the source was revoked or canceled before this retry.
	if found {
		return commitAttentionReceipt(ctx, tx, item.event.EventID, receipt)
	}
	if sourceErr != nil {
		_, err = tx.Exec(ctx, `UPDATE agent_attention_deliveries SET suppressed_at=now(), suppression_reason='source_unavailable' WHERE event_id=$1`, item.event.EventID)
		if err != nil {
			return "", err
		}
		return "suppressed", tx.Commit(ctx)
	}
	if !mayAdmit {
		return "ready", tx.Commit(ctx)
	}
	receipt, err = delivery.Admit(ctx, key, item.event)
	if err != nil {
		return "", err
	}
	return commitAttentionReceipt(ctx, tx, item.event.EventID, receipt)
}

func commitAttentionReceipt(ctx context.Context, tx pgx.Tx, eventID string, receipt AgentAttentionReceipt) (string, error) {
	if receipt.CommandID == "" || receipt.Seq == 0 || receipt.Seq > 9007199254740991 {
		return "", errors.New("invalid attention admission receipt")
	}
	_, err := tx.Exec(ctx, `UPDATE agent_attention_deliveries SET admitted_command_id=$2,
        admitted_command_seq=$3, admitted_at=now() WHERE event_id=$1`, eventID, receipt.CommandID, receipt.Seq)
	if err != nil {
		return "", err
	}
	return "admitted", tx.Commit(ctx)
}

func (s *ScopedStore) authorizeAttentionSource(ctx context.Context, tx pgx.Tx, item attentionCandidate) error {
	_, err := s.authorizeMutationInTx(ctx, tx)
	if err != nil {
		return err
	}
	place, err := s.lockScopedPlace(ctx, tx, item.event.Place.ID)
	if err != nil {
		return err
	}
	access, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor)
	if err != nil {
		return err
	}
	if access.WorkspaceMemberID != item.workspaceMemberID || access.PlaceMemberID != item.placeMemberID {
		return ErrPlaceNotFound
	}
	message, err := lockMessageScoped(ctx, tx, s.Scope.WorkspaceID, place.PlaceID, item.event.MessageID)
	if err != nil {
		return err
	}
	if message.Deleted || message.Seq < access.VisibleFromSeq {
		return ErrMessageNotFound
	}
	if item.event.Kind == AgentAttentionReminder {
		var due bool
		if err := tx.QueryRow(ctx, `SELECT resolved_at IS NULL AND remind_at <= now()
            FROM reply_later_markers WHERE marker_id=$1 AND member_kind='personality_agent'
              AND member_id=$2 AND message_id=$3 FOR UPDATE`, item.event.MarkerID, s.Scope.Actor.ID, message.MessageID).Scan(&due); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrMarkerNotFound
			}
			return err
		}
		if !due {
			return ErrMarkerNotFound
		}
	}
	return nil
}

func attentionSourceUnavailable(err error) bool {
	return errors.Is(err, ErrPlaceNotFound) || errors.Is(err, ErrMessageNotFound) ||
		errors.Is(err, ErrMarkerNotFound) || errors.Is(err, ErrMessageDeleted) ||
		errors.Is(err, applicationapps.ErrInstallationNotFound) ||
		errors.Is(err, applicationapps.ErrAppDisabled) || errors.Is(err, applicationapps.ErrAuthorityEpochStale)
}
