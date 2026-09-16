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

const (
	// AttentionChangeEdited marks an update to an already-posted message:
	// the event carries the new current content at the bumped revision.
	AttentionChangeEdited = "edited"
	// AttentionChangeDeleted marks the tombstone event: the original
	// message stays the secretary's experienced past; this event reports
	// that the current view no longer contains it.
	AttentionChangeDeleted = "deleted"
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
	// ReplyRequired is an outbox-only authorization condition on an original
	// reply event, which has no other basis to reach its recipient: delivery
	// suppresses it when the parent stops standing before admission.
	// DM/mention delivery does not depend on the continued existence of the
	// parent. A change event never sets it — the update rides on the
	// recipient's existing view, so delivery re-authorizes its reply_to and
	// retires the designation instead of suppressing the correction (see
	// authorizeAttentionSource).
	ReplyRequired    bool   `json:"reply_required,omitempty"`
	ReplyToMessageID string `json:"reply_to_message_id,omitempty"`
	// Change marks a mutation of an already-posted message (edited/deleted);
	// empty is the original delivery. The deliveries row keeps source_kind
	// 'messaging_message' — the source entity is still the message, and the
	// bumped source_revision already makes each change a distinct deduplicated
	// version without a new source_kind (which the column's CHECK forbids).
	Change string `json:"change,omitempty"`
	// Reason is the notification rule that selected this recipient
	// (dm/mention/keyword/all). It rides along so the delivery adapter can
	// carry the same attention hint into the core input — it is a hint about
	// how strongly the message asks for a response, not a mandate to answer.
	Reason          string    `json:"reason,omitempty"`
	MessageRevision int64     `json:"message_revision"`
	MessageSeq      int64     `json:"message_seq"`
	OccurredAt      time.Time `json:"occurred_at"`
	Content         string    `json:"content"`
	// Attachments is the scoped metadata of the files bound to this message —
	// identity, name, type, and size, never bytes. An attachment-only message
	// must still reach the secretary as a meaningful input, and the ids let it
	// read the bytes afterwards through the authorized open_attachment effect.
	// Tombstones, poll-vote reports, and reminders carry none.
	Attachments []AgentAttentionAttachment  `json:"attachments,omitempty"`
	MarkerID    string                      `json:"marker_id,omitempty"`
	DueAt       *time.Time                  `json:"due_at,omitempty"`
	PollVote    *AgentAttentionPollVoteData `json:"poll_vote,omitempty"`
}

// AgentAttentionAttachment is the delivery-time projection of one bound
// attachment. It names the attachment, never grants access: byte reads
// re-authorize against current tenure and visibility.
type AgentAttentionAttachment struct {
	AttachmentID string `json:"attachment_id"`
	Filename     string `json:"filename"`
	MIME         string `json:"mime"`
	SizeBytes    int64  `json:"size_bytes"`
	SHA256       string `json:"sha256"`
	Position     int    `json:"position"`
	Spoiler      bool   `json:"spoiler"`
	Alt          string `json:"alt,omitempty"`
}

func attentionAttachments(attachments []Attachment) []AgentAttentionAttachment {
	if len(attachments) == 0 {
		return nil
	}
	out := make([]AgentAttentionAttachment, len(attachments))
	for i, a := range attachments {
		out[i] = AgentAttentionAttachment{
			AttachmentID: a.AttachmentID, Filename: a.Filename, MIME: a.MIME,
			SizeBytes: a.SizeBytes, SHA256: a.SHA256Hex(), Position: a.Position,
			Spoiler: a.Spoiler, Alt: a.Alt,
		}
	}
	return out
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
		Attachments: attentionAttachments(message.Attachments),
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
	event.Reason = decision.Reason
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

// issueAgentMessageChange records an edit or a deletion of an already-posted
// message as a new attention event — a current-view update, never a rewrite
// of the event that delivered the original. Recipients are the PAs the
// message actually reached — recorded intents plus every live delivery row
// (reply attention, poll-vote reports, reminders, edit-added mentions) — and,
// when someone else makes the change, the message's own PA author: leaving
// their view stale would let them keep answering a message that no longer
// exists, or never learn their own message was removed. An edit additionally
// reaches members its new content selects for the first time: a mention added
// by an edit is a real call for attention, and a recorded recipient's reason
// is re-derived from the current selection rather than replayed — an edit
// that removed what selected them leaves no reason behind. A deletion has no
// new content to select on.
func (s *ScopedStore) issueAgentMessageChange(ctx context.Context, tx pgx.Tx, place Place, message Message, change string, changedAt time.Time) error {
	recipients := map[string]NotificationDecision{}
	rows, err := tx.Query(ctx, `
		SELECT recipient_kind, recipient_id, reason
		FROM message_notification_intents
		WHERE message_id = $1 AND recipient_kind = 'personality_agent'`, message.MessageID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var decision NotificationDecision
		if err := rows.Scan(&decision.Participant.Kind, &decision.Participant.ID, &decision.Reason); err != nil {
			rows.Close()
			return err
		}
		recipients[decision.Participant.Key()] = decision
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	// Intents are only the recorded selection at append. Reply attention,
	// poll-vote reports, reminders and mentions added by an earlier edit all
	// landed delivery rows without an intent — every secretary the message
	// may have reached through a supported path stays a candidate. Whether a
	// view was actually established is decided at delivery
	// (authorizeAttentionSource): a still-pending row makes the change wait
	// for its outcome, a suppressed one was proven undeliverable here.
	rows, err = tx.Query(ctx, `
		SELECT DISTINCT ON (personality_agent_id)
		       personality_agent_id, COALESCE(payload->>'reason','')
		FROM agent_attention_deliveries
		WHERE message_id = $1 AND suppressed_at IS NULL
		ORDER BY personality_agent_id, available_at DESC, event_id DESC`, message.MessageID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var recipientID, reason string
		if err := rows.Scan(&recipientID, &reason); err != nil {
			rows.Close()
			return err
		}
		ref := PersonalityAgent(recipientID)
		if _, ok := recipients[ref.Key()]; !ok {
			recipients[ref.Key()] = NotificationDecision{Participant: ref, Reason: reason}
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	// A change by someone else is a fact about the author's own message —
	// the author hears it even when no delivery or intent ever named them.
	if message.Author.Kind == KindPersonalityAgent && message.Author != s.Scope.Actor {
		if _, ok := recipients[message.Author.Key()]; !ok {
			recipients[message.Author.Key()] = NotificationDecision{Participant: message.Author}
		}
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
	actorName := ""
	for _, member := range members {
		if member.Participant == s.Scope.Actor {
			actorName = member.DisplayName
			break
		}
	}
	if change == AttentionChangeEdited {
		decisions, err := s.notificationDecisionsForMembersScoped(ctx, tx, place, message, members)
		if err != nil {
			return err
		}
		selected := make(map[string]bool, len(decisions))
		for _, decision := range decisions {
			if decision.Participant.Kind == KindPersonalityAgent {
				recipients[decision.Participant.Key()] = decision
				selected[decision.Participant.Key()] = true
			}
		}
		// A recorded reason names how the original view reached the
		// recipient, not what this update asks of them: an edit that dropped
		// the mention or keyword — or a mute applied since — must not keep
		// demanding attention on the old basis. Only current selection is a
		// live reason on a change event.
		for key, decision := range recipients {
			if !selected[key] && decision.Reason != "" {
				decision.Reason = ""
				recipients[key] = decision
			}
		}
	}
	// reply_to on an event means "this message answers yours". It names only
	// the secretary an ordinary reply appended now would address: an ambient
	// recipient's change event must not inherit it, and new content alone
	// never readdresses a parent that was deleted, precedes the secretary's
	// tenure, or belongs to a secretary no longer in this conversation. Those
	// recipients still get the change through their own reason or record.
	// A tombstone reports the deletion whatever became of the parent since.
	addressee, _, addressed, err := s.replyAddressee(ctx, tx, place, message, members, change == AttentionChangeEdited)
	if err != nil {
		return err
	}
	for _, decision := range recipients {
		if decision.Participant == s.Scope.Actor {
			continue // the actor already knows what it changed
		}
		// Tenure is re-authorized, not replayed from the intent record: a
		// recipient who has since left the place has no view to update.
		access, err := s.placeAccessAfterAuthorization(ctx, tx, place, decision.Participant)
		if errors.Is(err, ErrPlaceNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		event := s.attentionEvent(place, message, s.Scope.Actor, actorName)
		event.Kind, event.PersonalityAgentID = AgentAttentionMessage, decision.Participant.ID
		event.Change, event.Reason = change, decision.Reason
		event.OccurredAt = changedAt
		if addressed && addressee.Author == decision.Participant {
			// The designation is what the parent justifies at issue time;
			// delivery re-checks it against the parent as it stands then
			// (authorizeAttentionSource) — a parent deleted or moved outside
			// the recipient's tenure meanwhile retires the designation
			// without suppressing the correction itself.
			event.ReplyToMessageID = message.ReplyTo
		}
		if change == AttentionChangeDeleted {
			// A tombstone reports that the current view no longer contains the
			// message: the delivered content and its attachment references are
			// cleared the same way.
			event.Content, event.Attachments = "", nil
		} else {
			for _, ref := range message.Mentions {
				if ref == decision.Participant && decision.Reason != NotifyReasonDM {
					event.Kind = AgentAttentionMention
					break
				}
			}
		}
		if err := s.insertAgentAttention(ctx, tx, event, message.MessageID, message.Revision,
			access.WorkspaceMemberID, access.PlaceMemberID, changedAt); err != nil {
			return err
		}
	}
	return nil
}

// A reply names its recipient through the persisted message author, never text
// interpretation. It uses the same outbox as DM/mention attention and remains
// subject to that recipient's notification settings and membership tenure.
func (s *ScopedStore) issueAgentReply(ctx context.Context, tx pgx.Tx, place Place, message Message, members []MemberProfile, decisions []NotificationDecision) (ParticipantRef, error) {
	parent, access, addressed, err := s.replyAddressee(ctx, tx, place, message, members, true)
	if err != nil || !addressed {
		return ParticipantRef{}, err
	}
	authorName := ""
	for _, member := range members {
		if member.Participant == message.Author {
			authorName = member.DisplayName
		}
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
		event.Reason = decision.Reason
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

// replyAddressee is the one rule for which secretary a reply answers: the PA
// author of its parent, when that secretary is in this conversation now (a
// former participant is not re-enrolled because its old message is still
// visible), has not muted the place, and can see the parent in its current
// tenure. Append and edit also require a live parent; a tombstone passes
// requireLiveParent=false because reporting a deletion never depends on it.
func (s *ScopedStore) replyAddressee(ctx context.Context, tx pgx.Tx, place Place, message Message, members []MemberProfile, requireLiveParent bool) (Message, PlaceAccess, bool, error) {
	if message.ReplyTo == "" {
		return Message{}, PlaceAccess{}, false, nil
	}
	parent, err := lockMessageScoped(ctx, tx, s.Scope.WorkspaceID, place.PlaceID, message.ReplyTo)
	if errors.Is(err, ErrMessageNotFound) {
		return Message{}, PlaceAccess{}, false, nil
	}
	if err != nil {
		return Message{}, PlaceAccess{}, false, err
	}
	if (requireLiveParent && parent.Deleted) || parent.Author.Kind != KindPersonalityAgent || parent.Author == message.Author {
		return Message{}, PlaceAccess{}, false, nil
	}
	present := false
	for _, member := range members {
		present = present || member.Participant == parent.Author
	}
	if !present {
		return Message{}, PlaceAccess{}, false, nil
	}
	settings, err := s.scopedNotificationSettingsFor(ctx, tx, place.PlaceID, []ParticipantRef{parent.Author})
	if err != nil {
		return Message{}, PlaceAccess{}, false, err
	}
	if settings[parent.Author.Key()].level == NotifyLevelMute {
		return Message{}, PlaceAccess{}, false, nil
	}
	access, err := s.placeAccessAfterAuthorization(ctx, tx, place, parent.Author)
	if err != nil {
		return Message{}, PlaceAccess{}, false, err
	}
	if parent.Seq < access.VisibleFromSeq {
		return Message{}, PlaceAccess{}, false, nil
	}
	return parent, access, true, nil
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
	event.Content, event.Attachments, event.OccurredAt = "", nil, votedAt
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
	event.Attachments = nil
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
		if reason, terminal := terminalDeliveryReason(err); terminal {
			return s.suppressAttentionNow(ctx, item.event.EventID, reason)
		}
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

// terminalDeliveryReason names the inspectable suppression reason for a
// delivery failure that can never resolve on this placement. A persona that
// permanently transferred away can never accept the input again, and an
// input id already bound to different content can never become this event —
// retrying either forever would only hide the truth. Everything else stays
// retryable: sealed or staged personas can still return to active when a
// transfer aborts, and storage failures are transient.
func terminalDeliveryReason(err error) (string, bool) {
	switch {
	case errors.Is(err, errRecipientTransferred):
		return "recipient_transferred", true
	case errors.Is(err, errAttentionInputConflict):
		return "input_conflict", true
	case errors.Is(err, errUnsupportedAttentionEvent):
		return "unsupported_route", true
	}
	return "", false
}

// suppressAttention marks the pending row terminally undeliverable with the
// truthful reason. The frozen event stays in the row — suppression answers
// the delivery question, it does not erase what was sent.
func suppressAttention(ctx context.Context, tx pgx.Tx, eventID, reason string) (string, error) {
	if _, err := tx.Exec(ctx, `UPDATE agent_attention_deliveries
        SET suppressed_at=now(), suppression_reason=$2
        WHERE event_id=$1 AND admitted_at IS NULL AND suppressed_at IS NULL`,
		eventID, reason); err != nil {
		return "", err
	}
	return "suppressed", tx.Commit(ctx)
}

// suppressAttentionNow suppresses outside an open delivery transaction —
// the path taken when a terminal failure surfaces in Prepare, before the
// second source-locked attempt begins.
func (s *Store) suppressAttentionNow(ctx context.Context, eventID, reason string) (string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	return suppressAttention(ctx, tx, eventID, reason)
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
	event := item.event
	downgraded := false
	if errors.Is(sourceErr, errReplyDesignationGone) {
		// The frozen row keeps what was issued; the admitted input carries
		// the designation the parent still justifies — none. The correction
		// itself stays authorized by the recipient's existing view, and
		// re-deriving the same effective event on every attempt keeps a
		// retry after a lost receipt a deduplication, not a conflict.
		event.ReplyToMessageID, event.ReplyRequired = "", false
		sourceErr = nil
		downgraded = true
	}
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
	receipt, found, err := delivery.Lookup(ctx, key, event)
	if errors.Is(err, errAttentionInputConflict) && downgraded {
		// The stored input may carry the designation held at its own lawful
		// admission — the parent dying afterwards does not unmake it. Both
		// variants are this event's content; only a match on neither is a
		// real conflict.
		receipt, found, err = delivery.Lookup(ctx, key, item.event)
	}
	if err != nil {
		if reason, terminal := terminalDeliveryReason(err); terminal {
			return suppressAttention(ctx, tx, item.event.EventID, reason)
		}
		return "", err
	}
	// A prior authorized append remains a fact even if its DB acknowledgement
	// failed and the source was revoked or canceled before this retry.
	if found {
		return commitAttentionReceipt(ctx, tx, item.event.EventID, receipt)
	}
	if sourceErr != nil {
		reason := "source_unavailable"
		if errors.Is(sourceErr, errNoEstablishedView) {
			reason = "no_prior_view"
		}
		return suppressAttention(ctx, tx, item.event.EventID, reason)
	}
	if !mayAdmit {
		return "ready", tx.Commit(ctx)
	}
	receipt, err = delivery.Admit(ctx, key, event)
	if err != nil {
		if reason, terminal := terminalDeliveryReason(err); terminal {
			return suppressAttention(ctx, tx, item.event.EventID, reason)
		}
		return "", err
	}
	return commitAttentionReceipt(ctx, tx, item.event.EventID, receipt)
}

// errReplyDesignationGone is a delivery-time fact about the event's reply
// designation, not a failure of the delivery's own authorization: the parent
// an edit names was deleted or fell outside the recipient's tenure after the
// event was issued. The update itself still stands on the recipient's
// existing view, so it is delivered without the designation.
var errReplyDesignationGone = errors.New("reply designation no longer stands")

// errNoEstablishedView marks a change event with no basis: the recipient has
// no reason selecting them now, is not the author, and no earlier delivery
// for this message actually reached them. A correction reports against a
// view that does not exist — there is nothing to update.
var errNoEstablishedView = errors.New("recipient has no established view of the message")

// errPriorDeliveryPending leaves the basis undecided: an earlier delivery row
// for this recipient and message is still in flight. The change event retries
// and resolves with that row's outcome — admitted establishes the view,
// suppression retires it.
var errPriorDeliveryPending = errors.New("prior delivery for this message is still pending")

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
	if item.event.Change == AttentionChangeDeleted {
		// The tombstone is the durable fact this event reports; it committed
		// atomically with the event. A live message would mean the stored
		// payload no longer describes the source — retry rather than
		// permanently suppress on the anomaly.
		if !message.Deleted {
			return errors.New("deletion event found a live message")
		}
	} else if message.Deleted {
		// A later deletion supersedes a pending delivery or edit event; the
		// recipient's tombstone event reports the current state.
		return ErrMessageNotFound
	}
	if message.Seq < access.VisibleFromSeq {
		return ErrMessageNotFound
	}
	if item.event.Change != "" && item.event.Reason == "" && message.Author != s.Scope.Actor {
		// A change event updates a view the recipient already holds; it is
		// not a first contact. The basis is a delivery row that actually
		// reached them — admitted, or its durable input when the receipt
		// acknowledgement was lost — or the recipient's own engagement
		// (a reply-later mark or poll vote proves the message was seen).
		// An in-flight earlier delivery leaves the question undecided:
		// retry until that row resolves rather than decide on it. A
		// suppressed or absent original means there is no view to correct.
		var established, pending bool
		err := tx.QueryRow(ctx, `
			SELECT
			  EXISTS (SELECT 1 FROM agent_attention_deliveries d
			          WHERE d.message_id=$1 AND d.personality_agent_id=$2 AND d.event_id<>$3
			            AND (d.admitted_at IS NOT NULL
			                 OR d.source_kind IN ('reply_later_due','messaging_poll_vote')
			                 OR EXISTS (SELECT 1 FROM core_inputs ci
			                            WHERE ci.persona_id=$2 AND ci.input_id='messaging:'||d.event_id::text))),
			  EXISTS (SELECT 1 FROM agent_attention_deliveries d
			          WHERE d.message_id=$1 AND d.personality_agent_id=$2 AND d.event_id<>$3
			            AND d.admitted_at IS NULL AND d.suppressed_at IS NULL
			            AND d.available_at < $4
			            AND d.source_kind IN ('messaging_message','messaging_mention'))`,
			item.event.MessageID, item.event.PersonalityAgentID, item.event.EventID,
			item.event.OccurredAt).Scan(&established, &pending)
		if err != nil {
			return err
		}
		if !established {
			if pending {
				return errPriorDeliveryPending
			}
			return errNoEstablishedView
		}
	}
	if item.event.ReplyRequired ||
		(item.event.Change == AttentionChangeEdited && item.event.ReplyToMessageID != "") {
		// The reply designation is re-authorized against the parent as it
		// stands now, under the same locks a mutation takes. An original
		// reply has no other basis to reach this recipient, so it
		// suppresses when the parent stops standing. An edit already has
		// the recipient's view, so the same failure only retires the
		// designation — the correction still delivers as an ordinary
		// update. A tombstone's reply_to is provenance for the deletion it
		// reports and is never re-checked.
		parentGone := true
		parent, err := lockMessageScoped(ctx, tx, s.Scope.WorkspaceID, place.PlaceID, item.event.ReplyToMessageID)
		if err == nil {
			parentGone = parent.Deleted || parent.Seq < access.VisibleFromSeq ||
				parent.Author != s.Scope.Actor || message.ReplyTo != parent.MessageID
		} else if !errors.Is(err, ErrMessageNotFound) {
			return err
		}
		if parentGone {
			if item.event.Change == AttentionChangeEdited {
				return errReplyDesignationGone
			}
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
		errors.Is(err, errNoEstablishedView) ||
		errors.Is(err, applicationapps.ErrInstallationNotFound) ||
		errors.Is(err, applicationapps.ErrAppDisabled) || errors.Is(err, applicationapps.ErrAuthorityEpochStale)
}
