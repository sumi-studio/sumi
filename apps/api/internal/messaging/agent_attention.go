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
	AgentAttentionPollVote = "messaging_poll_vote"
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
	// ReplyRequired is an outbox-only authorization condition. DM/mention
	// delivery does not depend on the continued existence of the parent.
	ReplyRequired    bool                        `json:"reply_required,omitempty"`
	ReplyToMessageID string                      `json:"reply_to_message_id,omitempty"`
	MessageRevision  int64                       `json:"message_revision"`
	MessageSeq       int64                       `json:"message_seq"`
	OccurredAt       time.Time                   `json:"occurred_at"`
	Content          string                      `json:"content"`
	MarkerID         string                      `json:"marker_id,omitempty"`
	DueAt            *time.Time                  `json:"due_at,omitempty"`
	PollVote         *AgentAttentionPollVoteData `json:"poll_vote,omitempty"`
}

// The frozen complete selection at one poll revision, in poll display order.
// Empty SelectedOptions is a withdrawal, not an absent projection.
type AgentAttentionPollVoteData struct {
	PollRevision    int64                      `json:"poll_revision"`
	Question        string                     `json:"question"`
	SelectedOptions []AgentAttentionPollOption `json:"selected_options"`
}

type AgentAttentionPollOption struct {
	OptionID string `json:"option_id"`
	Text     string `json:"text"`
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
	authorName := ""
	for _, member := range members {
		if member.Participant == message.Author {
			authorName = member.DisplayName
			break
		}
	}
	event := s.attentionEvent(place, message, message.Author, authorName)
	event.Kind, event.PersonalityAgentID = AgentAttentionMessage, decision.Participant.ID
	if mentioned && decision.Reason != NotifyReasonDM {
		event.Kind = AgentAttentionMention
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

// A reply names its recipient through the persisted message author, never text
// interpretation. It uses the same outbox as DM/mention attention and remains
// subject to that recipient's notification settings and membership tenure.
func (s *ScopedStore) issueAgentReply(ctx context.Context, tx pgx.Tx, place Place, message Message, members []MemberProfile, decisions []NotificationDecision) (ParticipantRef, error) {
	if message.ReplyTo == "" {
		return ParticipantRef{}, nil
	}
	parent, err := lockMessageScoped(ctx, tx, s.Scope.WorkspaceID, place.PlaceID, message.ReplyTo)
	if err != nil {
		return ParticipantRef{}, err
	}
	if parent.Deleted || parent.Author.Kind != KindPersonalityAgent || parent.Author == message.Author {
		return ParticipantRef{}, nil
	}
	// Do not enroll a former participant merely because their old message is
	// still visible. The recipient must be in this conversation now.
	present, authorName := false, ""
	for _, member := range members {
		present = present || member.Participant == parent.Author
		if member.Participant == message.Author {
			authorName = member.DisplayName
		}
	}
	if !present {
		return ParticipantRef{}, nil
	}
	settings, err := s.scopedNotificationSettingsFor(ctx, tx, place.PlaceID, []ParticipantRef{parent.Author})
	if err != nil {
		return ParticipantRef{}, err
	}
	if settings[parent.Author.Key()].level == NotifyLevelMute {
		return ParticipantRef{}, nil
	}
	access, err := s.placeAccessAfterAuthorization(ctx, tx, place, parent.Author)
	if err != nil {
		return ParticipantRef{}, err
	}
	if parent.Seq < access.VisibleFromSeq {
		return ParticipantRef{}, nil
	}
	event := s.attentionEvent(place, message, message.Author, authorName)
	event.Kind, event.PersonalityAgentID = AgentAttentionMessage, parent.Author.ID
	event.ReplyToMessageID, event.ReplyRequired = parent.MessageID, true
	for _, decision := range decisions {
		if decision.Participant != parent.Author {
			continue
		}
		// Any independently selected notification still belongs to the recipient
		// if the quoted parent disappears before delivery (all/keyword included).
		event.ReplyRequired = false
		if decision.Reason != NotifyReasonDM {
			for _, mention := range message.Mentions {
				if mention == parent.Author {
					event.Kind, event.ReplyRequired = AgentAttentionMention, false
				}
			}
		}
	}
	err = s.insertAgentAttention(ctx, tx, event, message.MessageID, message.Revision,
		access.WorkspaceMemberID, access.PlaceMemberID, message.CreatedAt)
	return parent.Author, err
}

// A vote is an action by the authenticated voter on the author's question.
// It is not a message written by that voter or an instruction from the owner.
func (s *ScopedStore) issueAgentPollVote(ctx context.Context, tx pgx.Tx, place Place, message Message, votedAt time.Time) error {
	if message.Author.Kind != KindPersonalityAgent || message.Author == s.Scope.Actor {
		return nil
	}
	members, err := s.activeMembersScoped(ctx, tx, place)
	if err != nil {
		return err
	}
	if place.Kind == PlaceThread {
		members, err = s.threadNotificationMembers(ctx, tx, place.PlaceID, members)
		if err != nil {
			return err
		}
	}
	present, voterName := false, ""
	for _, member := range members {
		present = present || member.Participant == message.Author
		if member.Participant == s.Scope.Actor {
			voterName = member.DisplayName
		}
	}
	if !present {
		return nil
	}
	settings, err := s.scopedNotificationSettingsFor(ctx, tx, place.PlaceID, []ParticipantRef{message.Author})
	if err != nil {
		return err
	}
	if settings[message.Author.Key()].level == NotifyLevelMute {
		return nil
	}
	access, err := s.placeAccessAfterAuthorization(ctx, tx, place, message.Author)
	if err != nil {
		return err
	}
	if message.Seq < access.VisibleFromSeq {
		return nil
	}
	poll := message.Poll
	if poll == nil || poll.Revision < 1 {
		return errors.New("poll vote attention requires committed projection")
	}
	selected := make([]AgentAttentionPollOption, 0)
	for _, option := range poll.Options {
		for _, voter := range option.Voters {
			if voter == s.Scope.Actor {
				selected = append(selected, AgentAttentionPollOption{OptionID: option.OptionID, Text: option.Text})
				break
			}
		}
	}
	event := s.attentionEvent(place, message, s.Scope.Actor, voterName)
	event.Kind, event.PersonalityAgentID = AgentAttentionPollVote, message.Author.ID
	event.Content, event.OccurredAt = "", votedAt
	event.PollVote = &AgentAttentionPollVoteData{PollRevision: poll.Revision, Question: poll.Question, SelectedOptions: selected}
	return s.insertAgentAttention(ctx, tx, event, message.MessageID, poll.Revision,
		access.WorkspaceMemberID, access.PlaceMemberID, votedAt)
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
	if item.event.ReplyRequired {
		parent, err := lockMessageScoped(ctx, tx, s.Scope.WorkspaceID, place.PlaceID, item.event.ReplyToMessageID)
		if err != nil {
			return err
		}
		if parent.Deleted || parent.Seq < access.VisibleFromSeq || parent.Author != s.Scope.Actor || message.ReplyTo != parent.MessageID {
			return ErrMessageNotFound
		}
	}
	if item.event.Kind == AgentAttentionPollVote {
		if message.Author != s.Scope.Actor {
			return ErrMessageNotFound
		}
		var exists bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM message_polls WHERE message_id=$1)", message.MessageID).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return ErrPollNotFound
		}
		// A vote accepted before closing remains an answer after its deadline.
		// Later votes likewise do not rewrite this event's frozen selection.
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
		errors.Is(err, ErrMarkerNotFound) || errors.Is(err, ErrPollNotFound) || errors.Is(err, ErrMessageDeleted) ||
		errors.Is(err, applicationapps.ErrInstallationNotFound) ||
		errors.Is(err, applicationapps.ErrAppDisabled) || errors.Is(err, applicationapps.ErrAuthorityEpochStale)
}
