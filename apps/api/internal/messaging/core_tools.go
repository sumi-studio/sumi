package messaging

// Delegated agentstate effects giving the TypeScript secretary core the
// ordinary shared Messaging surface a Human has: reading places, creating
// channels/DMs/threads, editing and retracting its own messages, owning its
// notification settings, and uploading/reading attachments — all through the
// same scoped domain stores the REST, WS, and local-control lanes use.
//
// Scope always resolves from the persona (the PersonalityAgent) plus the
// request's place or workspace, on the installation's live authority epoch;
// request fields can never name a different actor. Mutations derive their
// client nonce from the server-owned operation identity, so a replayed claim
// returns the domain's stored receipt instead of minting a second place,
// message, or upload. Writes that cannot live inside the claim transaction
// converge through those domain receipts on effect re-run.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"unicode/utf8"
)

const (
	MessagingCoreOverviewTool         = "messaging.overview"
	MessagingCoreOpenTool             = "messaging.open"
	MessagingCoreSearchTool           = "messaging.search"
	MessagingCoreStartDMTool          = "messaging.start_dm"
	MessagingCoreCreateChannelTool    = "messaging.create_channel"
	MessagingCoreUpdateChannelTool    = "messaging.update_channel"
	MessagingCoreDuplicateChannelTool = "messaging.duplicate_channel"
	MessagingCoreCreateThreadTool     = "messaging.create_thread"
	MessagingCoreEditMessageTool      = "messaging.edit_message"
	MessagingCoreDeleteMessageTool    = "messaging.delete_message"
	MessagingCoreNotificationTool     = "messaging.notification_settings"
	MessagingCoreUploadTool           = "messaging.upload_attachment"
	MessagingCoreOpenAttachmentTool   = "messaging.open_attachment"
)

// MaxCoreAttachmentBytes bounds attachment bytes the model uploads inline —
// base64 inside the operation claim request. It matches the local lane's
// fetch bound: larger files still upload through the human REST lane.
// Downloads are not bounded by it — open_attachment pages any size file in
// bounded slices.
const MaxCoreAttachmentBytes int64 = MaxLocalAttachmentFetchBytes

// open_attachment page bounds: default page 64 KiB, hard cap 128 KiB. A
// binary page is ~1.4x as base64 — the largest possible result stays far
// inside the agentstate commit body budget alongside the rest of a turn.
const (
	coreAttachmentReadSliceDefault int64 = 64 << 10
	coreAttachmentReadSliceMax     int64 = 128 << 10
)

// CoreToolEffects is the delegated tool/effect surface the host registers on
// the agentstate store when Messaging and the core share one service.
func (d *CoreAttentionDelivery) CoreToolEffects() map[string]agentstate.ToolEffect {
	return map[string]agentstate.ToolEffect{
		MessagingCoreTool:                 {Apply: d.applySend, AfterCommit: d.afterSendCommit},
		MessagingCoreOverviewTool:         {Apply: d.applyOverview, ReadOnly: alwaysReadOnly},
		MessagingCoreOpenTool:             {Apply: d.applyOpen, ReadOnly: alwaysReadOnly},
		MessagingCoreSearchTool:           {Apply: d.applySearch, ReadOnly: alwaysReadOnly},
		MessagingCoreStartDMTool:          {Apply: d.applyStartDM, AfterCommit: d.afterPlaceCreatedCommit},
		MessagingCoreCreateChannelTool:    {Apply: d.applyCreateChannel, AfterCommit: d.afterPlaceCreatedCommit},
		MessagingCoreDuplicateChannelTool: {Apply: d.applyDuplicateChannel, AfterCommit: d.afterPlaceCreatedCommit},
		MessagingCoreUpdateChannelTool:    {Apply: d.applyUpdateChannel, AfterCommit: d.afterPlaceUpdatedCommit},
		MessagingCoreCreateThreadTool:     {Apply: d.applyCreateThread, AfterCommit: d.afterThreadCreatedCommit},
		MessagingCoreEditMessageTool:      {Apply: d.applyEditMessage, AfterCommit: d.afterMessageEditedCommit},
		MessagingCoreDeleteMessageTool:    {Apply: d.applyDeleteMessage, AfterCommit: d.afterMessageDeletedCommit},
		MessagingCoreNotificationTool:     {Apply: d.applyNotificationSettings, ReadOnly: notificationSettingsReadOnly},
		MessagingCoreUploadTool:           {Apply: d.applyUploadAttachment},
		MessagingCoreOpenAttachmentTool:   {Apply: d.applyOpenAttachment, ReadOnly: alwaysReadOnly},
	}
}

// alwaysReadOnly declares a delegated effect that can never change anything
// outside its own operation record — history, overview, search, and
// attachment reads.
func alwaysReadOnly(map[string]any) bool { return true }

// notificationSettingsMutation reports whether a settings call writes — the
// same request keys applyNotificationSettings treats as a replacement — so
// the notice's effect check and the effect itself can never disagree about
// what a read is.
func notificationSettingsMutation(request map[string]any) bool {
	_, hasDefaults := request["defaults_level"]
	_, hasPerPlace := request["per_place"]
	_, hasKeywords := request["keywords"]
	return hasDefaults || hasPerPlace || hasKeywords
}

func notificationSettingsReadOnly(request map[string]any) bool {
	return !notificationSettingsMutation(request)
}

// --- request decoding helpers ---

func coreRequestString(request map[string]any, key string) string {
	s, _ := request[key].(string)
	return s
}

func coreRequestInt(request map[string]any, key string) (int64, bool) {
	switch v := request[key].(type) {
	case float64:
		return int64(v), true
	case int64:
		return v, true
	case int:
		return int64(v), true
	}
	return 0, false
}

// coreParticipant decodes one request participant through the shared wire
// shape — {kind, human_id} or {kind, personality_agent_id} — so the tool and
// the REST lane accept exactly the same references.
func coreParticipant(raw any) (ParticipantRef, error) {
	data, err := json.Marshal(raw)
	if err != nil {
		return ParticipantRef{}, fmt.Errorf("invalid participant: %w", err)
	}
	var w participantWire
	if err := json.Unmarshal(data, &w); err != nil {
		return ParticipantRef{}, fmt.Errorf("invalid participant")
	}
	return w.ref()
}

// wireJSON projects a wire value into the response map through its JSON
// contract — one projection shared with the browser and local lanes.
func wireJSON(v any) any {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

func wireMap(v any) map[string]any {
	out, _ := wireJSON(v).(map[string]any)
	return out
}

// corePlaceMap is the place detail the model sees — beyond the place ref, the
// name/topic/visibility it needs to decide where it is.
func corePlaceMap(place Place) map[string]any {
	return map[string]any{
		"place_id": place.PlaceID, "kind": place.Kind, "workspace_id": place.WorkspaceID,
		"revision": place.Revision, "name": place.Name, "topic": place.Topic,
		"visibility": place.Visibility, "last_seq": place.LastSeq, "voice": place.Voice,
	}
}

func membersToWire(profiles []MemberProfile) []memberWire {
	out := make([]memberWire, len(profiles))
	for i, p := range profiles {
		out[i] = memberWire{
			Participant: participantToWire(p.Participant),
			DisplayName: p.ProjectedDisplayName(),
			Tagline:     p.Tagline,
		}
	}
	return out
}

func coreBadRequest(format string, args ...any) error {
	return fmt.Errorf("%w: %s", agentstate.ErrBadRequest, fmt.Sprintf(format, args...))
}

// --- reads ---

// applyOverview answers "what does this workspace's Messaging look like to
// me" — the same projection the browser bootstrap and the local lane's
// overview action serve.
func (d *CoreAttentionDelivery) applyOverview(ctx context.Context, tx pgx.Tx, personaID, _ string, request map[string]any) (map[string]any, error) {
	scoped, err := d.scopeForCoreWorkspace(ctx, tx, personaID, coreRequestString(request, "workspace_id"))
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	scoped = scoped.bindTx(tx)
	overview, err := buildOverviewWire(ctx, scoped)
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	return wireMap(overview), nil
}

// applyOpen reads one place the way a client opens it: place detail, members,
// one bounded history page, the viewer's read cursor, and the thread view.
func (d *CoreAttentionDelivery) applyOpen(ctx context.Context, tx pgx.Tx, personaID, _ string, request map[string]any) (map[string]any, error) {
	placeID := coreRequestString(request, "place_id")
	scoped, err := d.scopeForCorePlace(ctx, tx, personaID, placeID)
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	scoped = scoped.bindTx(tx)
	var opt HistoryOptions
	if v, ok := coreRequestInt(request, "before_seq"); ok {
		opt.BeforeSeq = v
	}
	if v, ok := coreRequestInt(request, "limit"); ok {
		opt.Limit = int(v)
	}
	snapshot, err := scoped.OpenSnapshot(ctx, placeID, opt)
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	messages, nextBefore := boundedWirePage(snapshot.Messages, func(m Message) any {
		return messageToWire(snapshot.Place, m)
	})
	response := map[string]any{
		"place":         corePlaceMap(snapshot.Place),
		"members":       wireJSON(membersToWire(snapshot.Members)),
		"messages":      messages,
		"last_read_seq": snapshot.LastReadSeq,
	}
	if nextBefore > 0 {
		// The page hit the response budget before the requested limit: the
		// caller resumes with before_seq = next_before_seq.
		response["truncated"] = true
		response["next_before_seq"] = nextBefore
	}
	if snapshot.Thread != nil {
		response["thread"] = wireMap(threadToWire(*snapshot.Thread))
	}
	if snapshot.Place.Kind == PlaceChannel {
		threads, err := scoped.ThreadsIn(ctx, placeID)
		if err != nil {
			return nil, coreEffectFailure(err)
		}
		response["threads"] = wireJSON(threadsToWire(threads))
	}
	return response, nil
}

// applySearch runs the scoped message search — the same visibility floor and
// place-tenure projection every lane shares.
func (d *CoreAttentionDelivery) applySearch(ctx context.Context, tx pgx.Tx, personaID, _ string, request map[string]any) (map[string]any, error) {
	query := coreRequestString(request, "query")
	placeID := coreRequestString(request, "place_id")
	var scoped *ScopedStore
	var err error
	if placeID != "" {
		scoped, err = d.scopeForCorePlace(ctx, tx, personaID, placeID)
	} else {
		scoped, err = d.scopeForCoreWorkspace(ctx, tx, personaID, coreRequestString(request, "workspace_id"))
	}
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	scoped = scoped.bindTx(tx)
	var opt SearchOptions
	opt.PlaceID = placeID
	if v, ok := coreRequestInt(request, "limit"); ok {
		opt.Limit = int(v)
	}
	results, err := scoped.SearchMessages(ctx, query, opt)
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	items, truncated := boundedWirePageTruncated(results, func(r SearchResult) any {
		return searchResultWire{
			MessageID: r.Message.MessageID, Place: placeToWire(r.Place),
			Seq: r.Message.Seq, Author: participantToWire(r.Message.Author),
			Snippet: r.Snippet, CreatedAt: r.Message.CreatedAt,
		}
	})
	response := map[string]any{"results": items}
	if truncated {
		// No cursor: the caller narrows the query or a place_id.
		response["truncated"] = true
	}
	return response, nil
}

// coreToolReadPageBytes bounds one serialized read page — history, search —
// so a page of maximum-size messages can still live in the operation record,
// the journal event, the commit body, and the model context at once.
const coreToolReadPageBytes = 256 << 10

// boundedWirePage projects an ascending-seq history page bounded by
// coreToolReadPageBytes, keeping the newest rows — the messages an open is
// for. On truncation it returns the seq of the first kept row as the resume
// cursor (exclusive, matching before_seq) so the caller can page older.
func boundedWirePage(rows []Message, project func(Message) any) ([]any, int64) {
	items := make([]any, 0, len(rows))
	used := 0
	var nextBefore int64
	for i := len(rows) - 1; i >= 0; i-- {
		entry := wireJSON(project(rows[i]))
		raw, err := json.Marshal(entry)
		if err == nil && used > 0 && used+len(raw) > coreToolReadPageBytes {
			nextBefore = rows[i+1].Seq
			break
		}
		items = append(items, entry)
		used += len(raw)
	}
	for left, right := 0, len(items)-1; left < right; left, right = left+1, right-1 {
		items[left], items[right] = items[right], items[left]
	}
	return items, nextBefore
}

// boundedWirePageTruncated is boundedWirePage for rows without a seq cursor.
func boundedWirePageTruncated[T any](rows []T, project func(T) any) ([]any, bool) {
	items := make([]any, 0, len(rows))
	used := 0
	for _, r := range rows {
		entry := wireJSON(project(r))
		raw, err := json.Marshal(entry)
		if err == nil && used > 0 && used+len(raw) > coreToolReadPageBytes {
			return items, true
		}
		items = append(items, entry)
		used += len(raw)
	}
	return items, false
}

// --- place creation and channel metadata ---

// applyStartDM opens the ordinary direct places: one other participant is a
// DM (deduplicated by the shared pair key), two or more is a group DM under a
// derived place-creation nonce.
func (d *CoreAttentionDelivery) applyStartDM(ctx context.Context, tx pgx.Tx, personaID, idemKey string, request map[string]any) (map[string]any, error) {
	scoped, err := d.scopeForCoreWorkspace(ctx, tx, personaID, coreRequestString(request, "workspace_id"))
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	scoped = scoped.bindTx(tx)
	rawParticipants, ok := request["participants"].([]any)
	if !ok || len(rawParticipants) == 0 {
		return nil, coreBadRequest("participants must list at least one other member")
	}
	var requested []ParticipantRef
	for _, raw := range rawParticipants {
		ref, err := coreParticipant(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", agentstate.ErrBadRequest, err)
		}
		requested = append(requested, ref)
	}
	others, err := normalizeDMOthers(scoped.Scope.Actor, requested)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", agentstate.ErrBadRequest, err)
	}
	var place Place
	var created bool
	switch len(others) {
	case 0:
		return nil, coreBadRequest("a dm needs at least one participant besides yourself")
	case 1:
		place, created, err = scoped.EnsureDM(ctx, others[0])
	default:
		place, created, err = scoped.CreateGroupDMOnce(ctx, others, coreToolNonce(MessagingCoreStartDMTool, idemKey))
	}
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	members, err := scoped.ActiveMembers(ctx, place.PlaceID)
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	participants := make([]participantWire, len(members))
	for i, m := range members {
		participants[i] = participantToWire(m.Participant)
	}
	return map[string]any{
		"dm":       wireMap(dmWire{DMID: place.PlaceID, Kind: place.Kind, Participants: participants}),
		"place_id": place.PlaceID, "created": created,
	}, nil
}

func (d *CoreAttentionDelivery) applyCreateChannel(ctx context.Context, tx pgx.Tx, personaID, idemKey string, request map[string]any) (map[string]any, error) {
	scoped, err := d.scopeForCoreWorkspace(ctx, tx, personaID, coreRequestString(request, "workspace_id"))
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	scoped = scoped.bindTx(tx)
	voice, _ := request["voice"].(bool)
	place, created, err := scoped.CreateChannelOnce(ctx,
		coreRequestString(request, "name"), coreRequestString(request, "topic"),
		voice, coreToolNonce(MessagingCoreCreateChannelTool, idemKey))
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	return map[string]any{
		"channel":  wireMap(channelToWire(place)),
		"place_id": place.PlaceID, "created": created,
	}, nil
}

// applyUpdateChannel rewrites a channel's name/topic under the existing
// manage-channels capability — the same permission a human editor holds.
func (d *CoreAttentionDelivery) applyUpdateChannel(ctx context.Context, tx pgx.Tx, personaID, _ string, request map[string]any) (map[string]any, error) {
	placeID := coreRequestString(request, "place_id")
	scoped, err := d.scopeForCorePlace(ctx, tx, personaID, placeID)
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	scoped = scoped.bindTx(tx)
	var name, topic *string
	if raw, ok := request["name"]; ok {
		s, isString := raw.(string)
		if !isString {
			return nil, coreBadRequest("name must be a string")
		}
		name = &s
	}
	if raw, ok := request["topic"]; ok {
		s, isString := raw.(string)
		if !isString {
			return nil, coreBadRequest("topic must be a string")
		}
		topic = &s
	}
	place, err := scoped.UpdateChannel(ctx, placeID, name, topic)
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	return map[string]any{"channel": wireMap(channelToWire(place)), "place_id": place.PlaceID}, nil
}

func (d *CoreAttentionDelivery) applyDuplicateChannel(ctx context.Context, tx pgx.Tx, personaID, idemKey string, request map[string]any) (map[string]any, error) {
	placeID := coreRequestString(request, "place_id")
	scoped, err := d.scopeForCorePlace(ctx, tx, personaID, placeID)
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	scoped = scoped.bindTx(tx)
	place, created, err := scoped.DuplicateChannelOnce(ctx, placeID,
		coreRequestString(request, "name"), coreToolNonce(MessagingCoreDuplicateChannelTool, idemKey))
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	return map[string]any{
		"channel":  wireMap(channelToWire(place)),
		"place_id": place.PlaceID, "created": created,
	}, nil
}

// applyCreateThread opens a thread in a channel, optionally anchored to a
// message. The one-thread-per-message race is answered with the existing
// thread rather than an error — the model wanted a thread to exist and it
// does.
func (d *CoreAttentionDelivery) applyCreateThread(ctx context.Context, tx pgx.Tx, personaID, idemKey string, request map[string]any) (map[string]any, error) {
	placeID := coreRequestString(request, "place_id")
	scoped, err := d.scopeForCorePlace(ctx, tx, personaID, placeID)
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	scoped = scoped.bindTx(tx)
	thread, created, err := scoped.CreateThread(ctx, placeID,
		coreRequestString(request, "name"), coreRequestString(request, "message_id"),
		coreToolNonce(MessagingCoreCreateThreadTool, idemKey))
	var exists *ThreadExistsError
	if errors.As(err, &exists) {
		return map[string]any{
			"thread":   wireMap(threadToWire(exists.Thread)),
			"place_id": exists.Thread.Place.PlaceID, "created": false,
		}, nil
	}
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	return map[string]any{
		"thread":   wireMap(threadToWire(thread)),
		"place_id": thread.Place.PlaceID, "created": created,
	}, nil
}

// --- own-message edit and retract ---

// applyEditMessage edits a message the secretary authored — the store
// enforces authorship. expected_revision, when given, is the optimistic
// guard a caller that already opened the place may assert; a conflict that
// finds the requested content already committed is the replay of this exact
// applied effect, answered with the current message instead of a second
// edit.
func (d *CoreAttentionDelivery) applyEditMessage(ctx context.Context, tx pgx.Tx, personaID, _ string, request map[string]any) (map[string]any, error) {
	placeID := coreRequestString(request, "place_id")
	messageID := coreRequestString(request, "message_id")
	content := coreRequestString(request, "content")
	scoped, err := d.scopeForCorePlace(ctx, tx, personaID, placeID)
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	scoped = scoped.bindTx(tx)
	var message Message
	if expected, ok := coreRequestInt(request, "expected_revision"); ok {
		message, err = scoped.EditMessage(ctx, placeID, messageID, content, expected)
	} else {
		message, err = scoped.EditMessageAtCurrent(ctx, placeID, messageID, content)
	}
	var conflict *messageRevisionConflictError
	if errors.As(err, &conflict) &&
		conflict.Current.Author == scoped.Scope.Actor &&
		conflict.Current.Content == content && conflict.Current.EditedAt != nil {
		// Only the author can move a message's content, so the requested
		// state already holding under a bumped revision is this operation's
		// earlier committed run, not someone else's edit.
		message = conflict.Current
		return d.editResponse(ctx, scoped, placeID, message, true)
	}
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	return d.editResponse(ctx, scoped, placeID, message, false)
}

func (d *CoreAttentionDelivery) editResponse(ctx context.Context, scoped *ScopedStore, placeID string, message Message, replayed bool) (map[string]any, error) {
	place, err := scoped.PlaceFor(ctx, placeID)
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	return map[string]any{
		"message":  wireMap(messageToWire(place, message)),
		"place_id": placeID, "message_id": message.MessageID,
		"replayed": replayed,
	}, nil
}

// applyDeleteMessage retracts a message through the shared tombstone rules:
// the secretary may retract its own messages anywhere and, with the
// manage-channels capability, moderate channel messages. Deleting an already
// deleted message returns the tombstone — the replayed effect converges.
func (d *CoreAttentionDelivery) applyDeleteMessage(ctx context.Context, tx pgx.Tx, personaID, _ string, request map[string]any) (map[string]any, error) {
	placeID := coreRequestString(request, "place_id")
	messageID := coreRequestString(request, "message_id")
	scoped, err := d.scopeForCorePlace(ctx, tx, personaID, placeID)
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	scoped = scoped.bindTx(tx)
	message, err := scoped.DeleteMessage(ctx, placeID, messageID)
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	place, err := scoped.PlaceFor(ctx, placeID)
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	return map[string]any{
		"message":  wireMap(messageToWire(place, message)),
		"place_id": placeID, "message_id": message.MessageID,
		"deleted": message.Deleted,
	}, nil
}

// --- notification settings ---

// applyNotificationSettings reads or replaces the secretary's own
// notification setting. Unnamed fields keep their stored values — the store
// replaces the whole setting, so the effect carries the untouched parts
// forward itself.
func (d *CoreAttentionDelivery) applyNotificationSettings(ctx context.Context, tx pgx.Tx, personaID, _ string, request map[string]any) (map[string]any, error) {
	scoped, err := d.scopeForCoreWorkspace(ctx, tx, personaID, coreRequestString(request, "workspace_id"))
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	scoped = scoped.bindTx(tx)
	current, err := scoped.NotificationSettingFor(ctx)
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	if !notificationSettingsMutation(request) {
		return map[string]any{"setting": wireMap(notificationSettingToWire(current))}, nil
	}
	defaultLevel := current.DefaultLevel
	if _, hasDefaults := request["defaults_level"]; hasDefaults {
		defaultLevel = coreRequestString(request, "defaults_level")
		if err := ValidateNotifyLevel(defaultLevel); err != nil {
			return nil, coreEffectFailure(err)
		}
	}
	perPlace := current.PerPlace
	if _, hasPerPlace := request["per_place"]; hasPerPlace {
		var entries []struct {
			Place placeWire `json:"place"`
			Level string    `json:"level"`
		}
		raw, err := json.Marshal(request["per_place"])
		if err != nil {
			return nil, coreBadRequest("invalid per_place")
		}
		if err := json.Unmarshal(raw, &entries); err != nil {
			return nil, coreBadRequest("invalid per_place")
		}
		perPlace = make([]PlaceNotifyLevel, 0, len(entries))
		for _, entry := range entries {
			if err := ValidateNotifyLevel(entry.Level); err != nil {
				return nil, coreEffectFailure(err)
			}
			placeID := entry.Place.placeID()
			if placeID == "" {
				return nil, coreBadRequest("per_place entry needs a place")
			}
			perPlace = append(perPlace, PlaceNotifyLevel{PlaceID: placeID, Level: entry.Level})
		}
	}
	keywords := current.Keywords
	if _, hasKeywords := request["keywords"]; hasKeywords {
		var list []string
		raw, err := json.Marshal(request["keywords"])
		if err != nil || json.Unmarshal(raw, &list) != nil {
			return nil, coreBadRequest("keywords must be a list of strings")
		}
		keywords = list
	}
	setting, err := scoped.SetNotificationSetting(ctx, defaultLevel, perPlace, keywords)
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	return map[string]any{"setting": wireMap(notificationSettingToWire(setting))}, nil
}

// --- attachments ---

// applyUploadAttachment stages and finalizes a file the model supplies inline
// as base64, through the shared upload state machine: the same quota,
// reservation, staging, and receipt rules the browser and local lanes run.
// The byte cap keeps the operation record small; a filename is display
// metadata, never a filesystem path.
func (d *CoreAttentionDelivery) applyUploadAttachment(ctx context.Context, tx pgx.Tx, personaID, idemKey string, request map[string]any) (map[string]any, error) {
	placeID := coreRequestString(request, "place_id")
	filename := strings.TrimSpace(coreRequestString(request, "filename"))
	encoded := coreRequestString(request, "content_base64")
	if filename == "" {
		return nil, coreBadRequest("filename is required")
	}
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, coreBadRequest("content_base64 is not valid base64")
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("%w: %v", agentstate.ErrBadRequest, ErrAttachmentEmpty)
	}
	if int64(len(data)) > MaxCoreAttachmentBytes {
		return nil, fmt.Errorf("%w: %v (core tool boundary is %d bytes)",
			agentstate.ErrBadRequest, ErrAttachmentTooLarge, MaxCoreAttachmentBytes)
	}
	var patch AttachmentDraftPatch
	if raw, ok := request["alt"]; ok {
		s, isString := raw.(string)
		if !isString {
			return nil, coreBadRequest("alt must be a string")
		}
		patch.Alt = &s
	}
	if raw, ok := request["spoiler"]; ok {
		b, isBool := raw.(bool)
		if !isBool {
			return nil, coreBadRequest("spoiler must be a boolean")
		}
		patch.Spoiler = &b
	}
	scoped, err := d.scopeForCorePlace(ctx, tx, personaID, placeID)
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	scoped = scoped.bindTx(tx)
	req := attachmentUploadRequest{
		placeID:      placeID,
		clientNonce:  coreToolNonce(MessagingCoreUploadTool, idemKey),
		filename:     sanitizeAttachmentFilename(filename),
		declaredMIME: coreRequestString(request, "mime"),
		declaredSize: int64(len(data)),
	}
	// The claim's writer-generation fence is this lane's admission boundary;
	// both metadata phases re-authorize the scoped actor on their own.
	inlineAdmit := func(op func() error) (bool, error) { return true, op() }
	att, created, admitted, err := uploadAttachment(ctx, scoped, req, inlineAdmit, nil, bytes.NewReader(data))
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	if !admitted {
		return nil, errors.New("core attachment upload admission refused")
	}
	if !patch.empty() && att.MessageID == "" {
		patched, patchErr := scoped.UpdateDraftAttachment(ctx, att.AttachmentID, patch)
		switch {
		case patchErr == nil:
			att = patched
		case errors.Is(patchErr, ErrAttachmentAlreadySent):
			// A replayed upload whose bytes already went out keeps the
			// sent attachment's metadata — what recipients saw stands.
		default:
			return nil, coreEffectFailure(patchErr)
		}
	}
	return map[string]any{
		"attachment": wireMap(attachmentToWire(att)), "created": created,
	}, nil
}

// applyOpenAttachment reads a bounded slice of attachment bytes the
// secretary can currently see. Like the local lane, the request binds the
// exact place and message the view showed the attachment on — a mismatched
// identity is not-found, and visibility is re-authorized under the live
// scope, so a frozen input can never read what removal or a tombstone took
// away.
//
// Reads are paged (offset/max_bytes) so any size file fits inside the
// operation, journal, commit-body, and model budgets. Textual payloads come
// back decoded as content_text — the representation a text model actually
// reads — with each page cut on a rune boundary so byte-offset paging stays
// exact. Binary payloads return content_base64 for inspection; the tool
// does not pretend a text model comprehends binary content.
func (d *CoreAttentionDelivery) applyOpenAttachment(ctx context.Context, tx pgx.Tx, personaID, _ string, request map[string]any) (map[string]any, error) {
	placeID := coreRequestString(request, "place_id")
	messageID := coreRequestString(request, "message_id")
	attachmentID := coreRequestString(request, "attachment_id")
	if placeID == "" || messageID == "" || !validAttachmentID(attachmentID) {
		return nil, coreBadRequest("place_id, message_id, and attachment_id are required")
	}
	scoped, err := d.scopeForCorePlace(ctx, tx, personaID, placeID)
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	scoped = scoped.bindTx(tx)
	if !scoped.Store.AttachmentsEnabled() {
		return nil, coreEffectFailure(ErrAttachmentsUnavailable)
	}
	att, err := scoped.AttachmentForViewer(ctx, attachmentID)
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	if att.PlaceID != placeID || att.MessageID == "" || att.MessageID != messageID {
		return nil, coreEffectFailure(ErrAttachmentNotFound)
	}
	offset, ok := coreRequestInt(request, "offset")
	if !ok {
		offset = 0
	}
	if offset < 0 || offset > att.SizeBytes {
		return nil, coreBadRequest("offset must be within the attachment's %d bytes", att.SizeBytes)
	}
	maxBytes, ok := coreRequestInt(request, "max_bytes")
	if !ok || maxBytes <= 0 {
		maxBytes = coreAttachmentReadSliceDefault
	}
	if maxBytes > coreAttachmentReadSliceMax {
		maxBytes = coreAttachmentReadSliceMax
	}

	response := map[string]any{
		"attachment": wireMap(attachmentToWire(att)),
		"size_bytes": att.SizeBytes,
		"offset":     offset,
	}
	if offset == att.SizeBytes {
		response["returned_bytes"] = 0
		response["has_more"] = false
		if attachmentTextual(att.MIME, nil) {
			response["encoding"] = "text"
			response["content_text"] = ""
		} else {
			response["encoding"] = "base64"
			response["content_base64"] = ""
		}
		return response, nil
	}

	blob, err := scoped.Store.AttachmentBlobStore().Open(att.AttachmentID)
	if err != nil {
		return nil, coreEffectFailure(err)
	}
	defer func() { _ = blob.Close() }()
	if _, err := blob.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek attachment blob: %w", err)
	}
	data, err := io.ReadAll(io.LimitReader(blob, maxBytes+4))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		data = data[:maxBytes]
	}
	hasMore := offset+int64(len(data)) < att.SizeBytes
	response["has_more"] = hasMore
	if attachmentTextual(att.MIME, data) {
		// Never split a code point across the page edge: returned_bytes
		// then names the exact resume offset.
		data = data[:len(data)-partialRuneTail(data)]
		response["returned_bytes"] = len(data)
		response["encoding"] = "text"
		response["content_text"] = string(data)
		return response, nil
	}
	response["returned_bytes"] = len(data)
	response["encoding"] = "base64"
	response["content_base64"] = base64.StdEncoding.EncodeToString(data)
	return response, nil
}

// attachmentTextual decides whether a payload is decodable text for the
// model: an explicit textual MIME, or unlabeled bytes that are actually
// valid UTF-8 without NULs.
func attachmentTextual(mime string, sample []byte) bool {
	mime = strings.ToLower(strings.TrimSpace(strings.SplitN(mime, ";", 2)[0]))
	if strings.HasPrefix(mime, "text/") {
		return true
	}
	switch mime {
	case "application/json", "application/ld+json", "application/xml",
		"application/javascript", "application/x-javascript",
		"application/yaml", "application/x-yaml", "application/toml",
		"application/sql", "application/csv", "application/x-ndjson",
		"application/x-sh", "image/svg+xml":
		return true
	}
	if mime == "" || mime == "application/octet-stream" {
		// Tolerate a page ending mid-codepoint — the boundary is the
		// reader's, not the file's.
		trimmed := sample[:len(sample)-partialRuneTail(sample)]
		return len(trimmed) > 0 && utf8.Valid(trimmed) && !bytes.ContainsRune(trimmed, 0)
	}
	return false
}

// partialRuneTail returns how many trailing bytes of buf form an incomplete
// UTF-8 encoding — the bytes to leave for the next page. An unfinished
// sequence is at most UTFMax-1 continuation bytes.
func partialRuneTail(buf []byte) int {
	if utf8.Valid(buf) {
		return 0
	}
	for tail := 1; tail <= utf8.UTFMax && tail <= len(buf); tail++ {
		if utf8.Valid(buf[:len(buf)-tail]) {
			return tail
		}
	}
	return 0
}

// --- live fanout after commit ---

// rescopeAfterCommit rebuilds the actor's scope and the committed place
// under a fresh transaction for a best-effort live publish — the same
// re-authorization afterSendCommit performs.
func (d *CoreAttentionDelivery) rescopeAfterCommit(ctx context.Context, personaID, placeID string) (*ScopedStore, Place, error) {
	tx, err := d.Messaging.pool.Begin(ctx)
	if err != nil {
		return nil, Place{}, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	scoped, err := d.scopeForCorePlace(ctx, tx, personaID, placeID)
	if err != nil {
		return nil, Place{}, err
	}
	if _, err := scoped.authorizeInTx(ctx, tx); err != nil {
		return nil, Place{}, err
	}
	place, err := scoped.loadScopedPlace(ctx, tx, placeID)
	if err != nil {
		return nil, Place{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, Place{}, err
	}
	return scoped, place, nil
}

// afterPlaceCreatedCommit publishes the same place_created event the REST and
// local lanes fan out, so humans' clients see the secretary-created place
// live rather than on the next reload.
func (d *CoreAttentionDelivery) afterPlaceCreatedCommit(ctx context.Context, personaID string, _, response map[string]any) {
	if d.Hub == nil {
		return
	}
	// A fresh claim whose derived nonce found the place already existing
	// converged without creating it — don't re-announce a place clients
	// already have.
	if created, _ := response["created"].(bool); !created {
		return
	}
	placeID, _ := response["place_id"].(string)
	if placeID == "" {
		return
	}
	scoped, place, err := d.rescopeAfterCommit(ctx, personaID, placeID)
	if err != nil {
		return
	}
	switch place.Kind {
	case PlaceChannel:
		wire := channelToWire(place)
		_ = d.Hub.PublishScoped(ctx, scoped, Event{Type: EventPlaceCreated, PlaceID: place.PlaceID, Channel: &wire})
	default:
		members, err := scoped.ActiveMembers(ctx, place.PlaceID)
		if err != nil {
			return
		}
		participants := make([]participantWire, len(members))
		for i, m := range members {
			participants[i] = participantToWire(m.Participant)
		}
		wire := dmWire{DMID: place.PlaceID, Kind: place.Kind, Participants: participants}
		_ = d.Hub.PublishScoped(ctx, scoped, Event{Type: EventPlaceCreated, PlaceID: place.PlaceID, DM: &wire})
	}
}

func (d *CoreAttentionDelivery) afterPlaceUpdatedCommit(ctx context.Context, personaID string, _, response map[string]any) {
	if d.Hub == nil {
		return
	}
	placeID, _ := response["place_id"].(string)
	if placeID == "" {
		return
	}
	scoped, place, err := d.rescopeAfterCommit(ctx, personaID, placeID)
	if err != nil || place.Kind != PlaceChannel {
		return
	}
	wire := channelToWire(place)
	_ = d.Hub.PublishScoped(ctx, scoped, Event{Type: EventPlaceUpdated, PlaceID: place.PlaceID, Channel: &wire})
}

// afterThreadCreatedCommit publishes place_created on the parent channel —
// the address every thread event takes on the live lanes.
func (d *CoreAttentionDelivery) afterThreadCreatedCommit(ctx context.Context, personaID string, request, response map[string]any) {
	if d.Hub == nil {
		return
	}
	created, _ := response["created"].(bool)
	if !created {
		return
	}
	parentID, _ := request["place_id"].(string)
	threadID, _ := response["place_id"].(string)
	if parentID == "" || threadID == "" {
		return
	}
	scoped, _, err := d.rescopeAfterCommit(ctx, personaID, parentID)
	if err != nil {
		return
	}
	thread, err := scoped.ThreadFor(ctx, threadID)
	if err != nil {
		return
	}
	wire := threadToWire(thread)
	_ = d.Hub.PublishScoped(ctx, scoped, Event{Type: EventPlaceCreated, PlaceID: parentID, Thread: &wire})
}

// afterMessageEditedCommit / afterMessageDeletedCommit fan the committed
// change out as the message_edited / message_deleted event the REST publish
// carries — re-read under fresh authorization, best-effort on the committed
// durable record.
func (d *CoreAttentionDelivery) afterMessageChangeCommit(ctx context.Context, personaID string, response map[string]any, eventType string) {
	if d.Hub == nil {
		return
	}
	placeID, _ := response["place_id"].(string)
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
	parts := []Message{message}
	if err := attachMessagePartsWith(ctx, tx, parts); err != nil {
		return
	}
	if err := tx.Commit(ctx); err != nil {
		return
	}
	wire := messageToWire(place, parts[0])
	_ = d.Hub.PublishScoped(ctx, scoped, Event{Type: eventType, PlaceID: placeID, Message: &wire})
}

func (d *CoreAttentionDelivery) afterMessageEditedCommit(ctx context.Context, personaID string, _, response map[string]any) {
	d.afterMessageChangeCommit(ctx, personaID, response, EventMessageEdited)
}

func (d *CoreAttentionDelivery) afterMessageDeletedCommit(ctx context.Context, personaID string, _, response map[string]any) {
	d.afterMessageChangeCommit(ctx, personaID, response, EventMessageDeleted)
}
