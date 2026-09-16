package messaging

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
	"github.com/sumi-studio/sumi/apps/api/internal/canonicalid"
)

// MessagingCoreTool is the state-internal tool that posts into a real
// Messaging place as the secretary. Its effect is delegated to Messaging and
// applied inside the state service's operation-claim transaction, so the
// durable operation record and the committed message are atomic — a crash
// cannot leave an unrecorded message or a receipt for a message that never
// landed.
const MessagingCoreTool = "messaging.send"

// CoreAttentionDelivery admits Messaging attention events into the shared
// TypeScript secretary core: one persona per PersonalityAgent, one durable
// input per event. Admission idempotency lives in (persona, input_id), so a
// retried delivery and a lost receipt acknowledgement reconcile to exactly
// one queued input. The core's own lease/claim/recovery contract then
// delivers admitted inputs to a restarted runtime without duplicate effects.
type CoreAttentionDelivery struct {
	Core      *agentstate.Store
	Messaging *Store
	// Hub, when set, receives the live message-created fanout for messages a
	// secretary posts through MessagingCoreTool — the same publish the REST
	// and WS send paths run after their commit. Durable history remains the
	// record; a nil hub only skips the volatile push.
	Hub *Hub
}

var _ AgentAttentionDelivery = (*CoreAttentionDelivery)(nil)

// These delivery failures can never resolve on this placement; the drain
// maps them to an inspectable suppression reason instead of retrying
// forever or fabricating a receipt.
var (
	errRecipientTransferred   = errors.New("recipient persona has permanently transferred from this placement")
	errAttentionInputConflict = errors.New("the durable input under this event id already carries different content")
)

// Prepare ensures the persona exists so an admitted input has a queue to
// land in while no runtime is running. The persona is the PersonalityAgent;
// its owning Human and display name come from the agent record. There is no
// runtime to hold — a cold secretary acquires the writer lease when it next
// runs — so the release is a no-op.
func (d *CoreAttentionDelivery) Prepare(ctx context.Context, paID string) (func(), error) {
	if d == nil || d.Core == nil || d.Messaging == nil {
		return nil, errors.New("core attention delivery is unavailable")
	}
	var humanID *string
	var displayName string
	var hid string
	err := d.Messaging.pool.QueryRow(ctx,
		"SELECT human_id::text, display_name FROM agents WHERE personality_agent_id = $1",
		paID).Scan(&hid, &displayName)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// The delivery row exists only for an admitted member, so the agent
		// record is expected; its absence must not block admission.
	case err != nil:
		return nil, fmt.Errorf("resolve agent for persona: %w", err)
	default:
		humanID = &hid
	}
	if _, _, err := d.Core.EnsurePersona(ctx, paID, humanID, displayName); err != nil {
		return nil, err
	}
	return func() {}, nil
}

func (d *CoreAttentionDelivery) Lookup(ctx context.Context, _ string, event AgentAttentionEvent) (AgentAttentionReceipt, bool, error) {
	if d == nil || d.Core == nil {
		return AgentAttentionReceipt{}, false, errors.New("core attention delivery is unavailable")
	}
	input, _, err := d.Core.GetInput(ctx, event.PersonalityAgentID, coreInputID(event))
	if errors.Is(err, agentstate.ErrInputNotFound) {
		return AgentAttentionReceipt{}, false, nil
	}
	if err != nil {
		return AgentAttentionReceipt{}, false, err
	}
	// A receipt may only stand for an input that actually carries this event.
	// The same input id bound to different content is a conflict, not a
	// delivery — matching bytes are what make a replay a deduplication.
	if !attentionInputMatches(event, input) {
		return AgentAttentionReceipt{}, false,
			fmt.Errorf("%w: %s", errAttentionInputConflict, input.InputID)
	}
	return coreAttentionReceipt(event, input), true, nil
}

func (d *CoreAttentionDelivery) Admit(ctx context.Context, _ string, event AgentAttentionEvent) (AgentAttentionReceipt, error) {
	if d == nil || d.Core == nil {
		return AgentAttentionReceipt{}, errors.New("core attention delivery is unavailable")
	}
	input, _, err := d.Core.SubmitInput(ctx, coreInputFromEvent(event))
	if err != nil {
		switch {
		case errors.Is(err, agentstate.ErrTurnConflict):
			// A row under this input id with different content was admitted
			// between Lookup and Submit. It can never become this event.
			return AgentAttentionReceipt{}, fmt.Errorf("%w: %v", errAttentionInputConflict, err)
		case errors.Is(err, agentstate.ErrPersonaInactive):
			// 'transferred' is terminal: the persona can never accept input
			// on this placement again. 'sealed' and 'staged' can still abort
			// back to active, so those deliveries stay pending and retryable.
			st, stateErr := d.Core.PersonaState(ctx, event.PersonalityAgentID)
			if stateErr == nil && st.Persona.Authority == "transferred" {
				return AgentAttentionReceipt{}, fmt.Errorf("%w: %v", errRecipientTransferred, err)
			}
		}
		return AgentAttentionReceipt{}, err
	}
	return coreAttentionReceipt(event, input), nil
}

func coreInputID(event AgentAttentionEvent) string {
	return "messaging:" + event.EventID
}

// attentionInputMatches decides whether the durable input under this event's
// id really is this event — the contract fields and payload must match what
// SubmitInput would have written. PG stores timestamptz at microsecond
// precision and jsonb numbers without a Go type, so time and payload
// comparisons run at the stored precision and the canonical JSON form.
func attentionInputMatches(event AgentAttentionEvent, input agentstate.Input) bool {
	expected := coreInputFromEvent(event)
	if input.Kind != expected.Kind || input.ActorKind != expected.ActorKind ||
		input.ActorID != expected.ActorID || input.SourceSurface != expected.SourceSurface ||
		input.ThreadID != expected.ThreadID || input.Attention != expected.Attention {
		return false
	}
	if expected.OccurredAt == nil {
		if input.OccurredAt != nil {
			return false
		}
	} else if input.OccurredAt == nil ||
		input.OccurredAt.UnixMicro() != expected.OccurredAt.UnixMicro() {
		return false
	}
	want, err := json.Marshal(expected.Payload)
	if err != nil {
		return false
	}
	got, err := json.Marshal(input.Payload)
	if err != nil {
		return false
	}
	// Marshaled maps are canonical (keys sorted) and numeric values print in
	// their shortest form, so a jsonb-round-tripped int64 equals the original.
	return bytes.Equal(want, got)
}

// coreAttentionReceipt is the admission receipt: the command identity is the
// source event's own UUIDv7 (valid for the deliveries table's uuid column),
// and seq is the admitted input's admission position in milliseconds. Seq is
// a receipt field, not a promise of contiguous ordering.
func coreAttentionReceipt(event AgentAttentionEvent, input agentstate.Input) AgentAttentionReceipt {
	return AgentAttentionReceipt{CommandID: event.EventID, Seq: uint64(input.CreatedAt.UnixMilli())}
}

// coreAttentionFor maps the Messaging notification decision onto the core's
// reply/observe/defer hint: conversation that names or directly answers the
// secretary asks for a response; ambient traffic (notify-level "all",
// keyword matches, poll votes) is delivered for awareness. The hint never
// mandates — the secretary's own judgment decides whether to speak.
func coreAttentionFor(event AgentAttentionEvent) string {
	switch {
	case event.Change == AttentionChangeDeleted:
		return "observe" // a tombstone informs; it never asks for a reply
	case event.Kind == AgentAttentionReminder:
		return "reply" // the secretary asked to be reminded
	case event.Kind == AgentAttentionPollVote:
		return "observe"
	case event.ReplyToMessageID != "" || event.ReplyRequired:
		return "reply" // someone replied to the secretary's own message
	case event.Kind == AgentAttentionMention:
		return "reply"
	case event.Reason == NotifyReasonDM:
		return "reply"
	default:
		return "observe"
	}
}

// coreInputFromEvent freezes the attention event into a durable core input:
// actor, place, message identity, sequence, and the source's own occurrence
// time travel as contract fields and payload provenance so the journal keeps
// who/where/when without presenting it as a generic human message.
func coreInputFromEvent(event AgentAttentionEvent) *agentstate.Input {
	payload := map[string]any{
		"event_id":     event.EventID,
		"event_kind":   event.Kind,
		"workspace_id": event.WorkspaceID,
		"actor": map[string]any{
			"kind":         event.Actor.Kind,
			"id":           event.Actor.ID,
			"display_name": event.Actor.DisplayName,
		},
		"place": map[string]any{
			"id":   event.Place.ID,
			"kind": event.Place.Kind,
			"name": event.Place.Name,
		},
		"message_id":       event.MessageID,
		"message_seq":      event.MessageSeq,
		"message_revision": event.MessageRevision,
	}
	if event.Content != "" {
		payload["text"] = event.Content
	}
	if len(event.Attachments) > 0 {
		// The attachment metadata is part of the delivered view: an
		// attachment-only message must not arrive as an empty input.
		attachments := make([]map[string]any, len(event.Attachments))
		for i, a := range event.Attachments {
			attachments[i] = map[string]any{
				"attachment_id": a.AttachmentID,
				"filename":      a.Filename,
				"mime":          a.MIME,
				"size_bytes":    a.SizeBytes,
				"sha256":        a.SHA256,
				"position":      a.Position,
				"spoiler":       a.Spoiler,
			}
			if a.Alt != "" {
				attachments[i]["alt"] = a.Alt
			}
		}
		payload["attachments"] = attachments
	}
	if event.Reason != "" {
		payload["reason"] = event.Reason
	}
	if event.Change != "" {
		payload["message_change"] = event.Change
	}
	if event.ReplyToMessageID != "" {
		payload["reply_to_message_id"] = event.ReplyToMessageID
	}
	if event.MarkerID != "" {
		payload["marker_id"] = event.MarkerID
	}
	if event.DueAt != nil {
		payload["due_at"] = event.DueAt.UTC().Format(time.RFC3339Nano)
	}
	if event.PollVote != nil {
		options := make([]map[string]any, 0, len(event.PollVote.SelectedOptions))
		for _, option := range event.PollVote.SelectedOptions {
			options = append(options, map[string]any{
				"option_id": option.OptionID,
				"text":      option.Text,
			})
		}
		payload["poll_vote"] = map[string]any{
			"poll_revision":    event.PollVote.PollRevision,
			"question":         event.PollVote.Question,
			"selected_options": options,
		}
	}
	kind := "message"
	switch event.Kind {
	case AgentAttentionReminder:
		kind = "reminder"
	case AgentAttentionPollVote:
		kind = "poll_vote"
	}
	occurredAt := event.OccurredAt
	return &agentstate.Input{
		PersonaID:     event.PersonalityAgentID,
		InputID:       coreInputID(event),
		Kind:          kind,
		Payload:       payload,
		ActorKind:     event.Actor.Kind,
		ActorID:       event.Actor.ID,
		SourceSurface: "messaging",
		ThreadID:      event.Place.ID,
		OccurredAt:    &occurredAt,
		Attention:     coreAttentionFor(event),
	}
}

// SendEffect returns the delegated effect for MessagingCoreTool.
func (d *CoreAttentionDelivery) SendEffect() agentstate.ToolEffect {
	return agentstate.ToolEffect{Apply: d.applySend, AfterCommit: d.afterSendCommit}
}

// applySend resolves the place's current installation inside the claim
// transaction and appends through the ordinary scoped path: the secretary's
// own membership and place tenure authorize the send, the installation's
// current lifecycle epoch fences it, and the operation record and the
// message commit together.
func (d *CoreAttentionDelivery) applySend(ctx context.Context, tx pgx.Tx, personaID, idemKey string, request map[string]any) (map[string]any, error) {
	placeID, _ := request["place_id"].(string)
	content, _ := request["content"].(string)
	replyTo, _ := request["reply_to"].(string)
	urgency, _ := request["urgency"].(string)
	appendIn := AppendInput{
		PlaceID:     placeID,
		Content:     content,
		Urgency:     urgency,
		ReplyTo:     replyTo,
		ClientNonce: coreToolNonce(MessagingCoreTool, idemKey),
	}
	if ids, ok := request["attachments"]; ok {
		list, isList := ids.([]any)
		if !isList {
			return nil, fmt.Errorf("%w: attachments must be a list of attachment ids", agentstate.ErrBadRequest)
		}
		for _, raw := range list {
			id, isString := raw.(string)
			if !isString {
				return nil, fmt.Errorf("%w: attachments must be a list of attachment ids", agentstate.ErrBadRequest)
			}
			appendIn.AttachmentIDs = append(appendIn.AttachmentIDs, id)
		}
	}
	if err := normalizeAppendInput(&appendIn); err != nil {
		// Admission-time request rules are deterministic — replaying the same
		// request can never pass them, so the turn should record a tool error
		// instead of retrying.
		return nil, fmt.Errorf("%w: %v", agentstate.ErrBadRequest, err)
	}
	scoped, err := d.scopeForCorePlace(ctx, tx, personaID, placeID)
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	message, created, err := scoped.appendScopedInTx(ctx, tx, appendIn)
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	response := map[string]any{
		"message_id": message.MessageID,
		"place_id":   message.PlaceID,
		"seq":        message.Seq,
		"created":    created,
		"reply_to":   message.ReplyTo,
		"created_at": message.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
	if len(message.Attachments) > 0 {
		response["attachments"] = wireJSON(attachmentsToWire(message.Attachments))
	}
	return response, nil
}

// scopeForCorePlace resolves the Messaging scope for a place-addressed core
// tool call inside the caller's transaction: place → workspace → the
// workspace's current enabled installation. The effect rides the
// installation's live authority epoch, never a frozen event's — a disabled
// or reinstalled app cannot authorize a new call.
func (d *CoreAttentionDelivery) scopeForCorePlace(ctx context.Context, tx pgx.Tx, personaID, placeID string) (*ScopedStore, error) {
	if !canonicalid.IsUUIDv7(placeID) {
		return nil, ErrPlaceNotFound
	}
	var workspaceID string
	err := tx.QueryRow(ctx,
		"SELECT workspace_id::text FROM places WHERE place_id = $1", placeID).Scan(&workspaceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrPlaceNotFound
	}
	if err != nil {
		return nil, err
	}
	return d.scopeForCoreWorkspace(ctx, tx, personaID, workspaceID)
}

// scopeForCoreWorkspace resolves the Messaging scope for a
// workspace-addressed core tool call — overview, search, DM/channel
// creation, notification settings — where no place names the workspace.
func (d *CoreAttentionDelivery) scopeForCoreWorkspace(ctx context.Context, tx pgx.Tx, personaID, workspaceID string) (*ScopedStore, error) {
	if !canonicalid.IsUUIDv7(workspaceID) {
		return nil, ErrInvalidScope
	}
	var installationID string
	var epoch int64
	err := tx.QueryRow(ctx, `
		SELECT installation_id::text, authority_epoch
		FROM app_installations
		WHERE owner_kind = 'workspace' AND owner_id = $1
		  AND app_id = $2 AND enabled
		ORDER BY installed_at, installation_id LIMIT 1`,
		workspaceID, MessagingAppID).Scan(&installationID, &epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, applicationapps.ErrInstallationNotFound
	}
	if err != nil {
		return nil, err
	}
	return d.Messaging.Scoped(Scope{
		WorkspaceID: workspaceID, InstallationID: installationID,
		AuthorityEpoch: epoch, Actor: PersonalityAgent(personaID),
	})
}

// afterSendCommit fans the committed message out to the live hub exactly as
// the REST and WS send paths do. It re-reads the committed row under a fresh
// authorization rather than trusting the caller's copy; every step is
// best-effort because the durable record is already committed.
func (d *CoreAttentionDelivery) afterSendCommit(ctx context.Context, personaID string, request, response map[string]any) {
	if d.Hub == nil {
		return
	}
	placeID, _ := request["place_id"].(string)
	messageID, _ := response["message_id"].(string)
	if placeID == "" || messageID == "" {
		return
	}
	tx, err := d.Messaging.pool.Begin(ctx)
	if err != nil {
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	scoped, err := d.scopeForCorePlace(ctx, tx, personaID, placeID)
	if err != nil {
		return
	}
	if _, err := scoped.authorizeInTx(ctx, tx); err != nil {
		return
	}
	place, err := scoped.loadScopedPlace(ctx, tx, placeID)
	if err != nil {
		return
	}
	message, err := lockMessageScoped(ctx, tx, scoped.Scope.WorkspaceID, placeID, messageID)
	if err != nil {
		return
	}
	if err := tx.Commit(ctx); err != nil {
		return
	}
	publishMessageCreated(ctx, scoped, d.Hub, place, message)
}

// coreToolNonce derives a delegated Messaging effect's client nonce from the
// operation's server-owned idempotency identity (input + plan position). A
// replayed claim replays the identical request under the identical nonce, so
// the domain's dedup receipts and the operation ledger agree on one
// mutation — one message, one created place, one finalized upload.
func coreToolNonce(tool, idemKey string) string {
	sum := sha256.Sum256([]byte(tool + ":" + idemKey))
	return "core:" + hex.EncodeToString(sum[:20])
}

// coreEffectFailure classifies effect errors for the operation ledger: known
// Messaging/app rejections are deterministic — they surface to the model as
// a tool result (400) instead of leaving the turn to retry the same claim.
// Anything unrecognized stays transient — storage outages must retry.
func coreEffectFailure(err error) error {
	var conflict *messageRevisionConflictError
	switch {
	case errors.Is(err, ErrPlaceNotFound), errors.Is(err, ErrMessageNotFound),
		errors.Is(err, ErrWorkspaceNotFound), errors.Is(err, ErrNotAMember),
		errors.Is(err, ErrParticipantNotFound), errors.Is(err, ErrSeqBeyondLatest),
		errors.Is(err, ErrForbidden), errors.Is(err, ErrNotAuthor),
		errors.Is(err, ErrNotReachable), errors.Is(err, ErrIdempotencyConflict),
		errors.Is(err, ErrInvalidPoll), errors.Is(err, ErrMessageDeleted),
		errors.Is(err, ErrTooManyAttachments), errors.Is(err, ErrInvalidScope),
		errors.Is(err, ErrInvalidChannelName), errors.Is(err, ErrEmptyChannelUpdate),
		errors.Is(err, ErrNotAChannel), errors.Is(err, ErrNotThreadable),
		errors.Is(err, ErrInvalidNotificationSetting),
		errors.Is(err, ErrAttachmentNotFound), errors.Is(err, ErrAttachmentTooLarge),
		errors.Is(err, ErrAttachmentEmpty), errors.Is(err, ErrAttachmentNonce),
		errors.Is(err, ErrAttachmentQuotaExceeded), errors.Is(err, ErrAttachmentDraftLimit),
		errors.Is(err, ErrAttachmentUploadConflict), errors.Is(err, ErrAttachmentUploadExpired),
		errors.Is(err, ErrAttachmentUploadInProgress), errors.Is(err, ErrAttachmentSizeMismatch),
		errors.Is(err, ErrAttachmentsUnavailable), errors.Is(err, ErrAttachmentAlreadySent),
		errors.Is(err, applicationapps.ErrInstallationNotFound),
		errors.Is(err, applicationapps.ErrAppDisabled),
		errors.Is(err, applicationapps.ErrAuthorityEpochStale):
		return fmt.Errorf("%w: %v", agentstate.ErrBadRequest, err)
	case errors.As(err, &conflict):
		return fmt.Errorf("%w: %v", agentstate.ErrBadRequest, err)
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "22") {
		return fmt.Errorf("%w: %v", agentstate.ErrBadRequest, err)
	}
	return err
}
