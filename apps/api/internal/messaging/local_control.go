package messaging

import (
	"context"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

const (
	LocalOverviewPath          = "/local-control/v1/messaging:overview"
	LocalOpenPath              = "/local-control/v1/messaging:open"
	LocalWritePath             = "/local-control/v1/messaging:write"
	LocalReactPath             = "/local-control/v1/messaging:react"
	LocalStatusPath            = "/local-control/v1/messaging:status"
	LocalReplyLaterPath        = "/local-control/v1/messaging:reply-later"
	LocalReplyLaterResolvePath = "/local-control/v1/messaging:reply-later-resolve"
	LocalReadThroughPath       = "/local-control/v1/messaging:read-through"
	// LocalStartDMPath opens the same conversation the human sidebar's
	// 「ダイレクトメッセージを開始」opens: one participant reuses (or mints) the
	// single dm, several mint a group dm. Both go through the identical store
	// calls REST uses, so neither side can reach a place the other cannot.
	LocalStartDMPath = "/local-control/v1/messaging:start-dm"
	// LocalCreateChannelPath / LocalUpdateChannelPath / LocalDuplicateChannelPath
	// are the channel lifecycle the human sidebar's context menu offers,
	// reachable through the same Store calls REST uses.
	LocalCreateChannelPath    = "/local-control/v1/messaging:create-channel"
	LocalUpdateChannelPath    = "/local-control/v1/messaging:update-channel"
	LocalDuplicateChannelPath = "/local-control/v1/messaging:duplicate-channel"
	// LocalNotificationSettingsPath is both the read and the write of the
	// agent's own notification setting. The agent owns the identical resource a
	// Human owns — same contract, different transport (契約ドラフト: 人間はUI、
	// agentはtool). The messaging tool that calls it lands with #209; the口 is
	// here so the capability is not UI-only in the meantime.
	LocalNotificationSettingsPath = "/local-control/v1/messaging:notification-settings"
	// LocalSearchPath is the agent's copy of the human search box. 探すことが
	// UI にしか無いと、agent は「見えているものしか思い出せない」人になる。
	LocalSearchPath = "/local-control/v1/messaging:search"
	// LocalAttentionPath hands the agent its own unconsumed AttentionCandidates
	// and, in the same call, lets it ack what it has taken in.
	LocalAttentionPath = "/local-control/v1/messaging:attention"
)

// maxRelativeMinutes bounds every relative duration the agent lane accepts.
// The agent names durations ("30分後に"), not wall-clock instants, so the
// server's clock decides the moment and a drifting workspace clock cannot
// place a promise in the past or the far future.
const maxRelativeMinutes = uint32(MaxReplyLaterDelay / time.Minute)

// RegisterLocalControlRoutes exposes the same Store capabilities to a
// PersonalityAgent through its PAID-bound Unix control socket. Identity is
// supplied only by the existing generation-fenced authorization lease.
func (s *Server) RegisterLocalControlRoutes(control *agentevents.LocalControlServer) error {
	routes := []struct {
		pattern string
		handler agentevents.LocalAuthorizedHandler
	}{
		{"POST " + LocalOverviewPath, s.localOverview},
		{"POST " + LocalOpenPath, s.localOpen},
		{"POST " + LocalWritePath, s.localWrite},
		{"POST " + LocalReactPath, s.localReact},
		{"POST " + LocalStatusPath, s.localStatus},
		{"POST " + LocalReplyLaterPath, s.localReplyLater},
		{"POST " + LocalReplyLaterResolvePath, s.localReplyLaterResolve},
		{"POST " + LocalReadThroughPath, s.localReadThrough},
		{"POST " + LocalStartDMPath, s.localStartDM},
		{"POST " + LocalCreateChannelPath, s.localCreateChannel},
		{"POST " + LocalUpdateChannelPath, s.localUpdateChannel},
		{"POST " + LocalDuplicateChannelPath, s.localDuplicateChannel},
		{"POST " + LocalNotificationSettingsPath, s.localNotificationSettings},
		{"POST " + LocalSearchPath, s.localSearch},
		{"POST " + LocalAttentionPath, s.localAttention},
	}
	for _, route := range routes {
		if err := control.RegisterAuthorizedRoute(route.pattern, route.handler); err != nil {
			return err
		}
	}
	return nil
}

func localViewer(authorization agentevents.LocalRuntimeAuthorization) ParticipantRef {
	return PersonalityAgent(authorization.PersonalityAgentID)
}

func (s *Server) localOverview(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct{}
	if !decodeJSON(w, r, &request) {
		return
	}
	viewer := localViewer(authorization)
	if err := s.Store.EnsureDefaultWorkspaceMembership(r.Context(), viewer); err != nil {
		writeStoreError(w, err)
		return
	}
	overview, err := s.buildOverview(r.Context(), viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, overview)
}

func (s *Server) localOpen(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		PlaceID   string `json:"place_id"`
		BeforeSeq int64  `json:"before_seq,omitempty"`
		Limit     int    `json:"limit,omitempty"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.PlaceID == "" || request.BeforeSeq < 0 || request.Limit < 0 {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if request.Limit == 0 {
		request.Limit = 20
	}
	if request.Limit > 50 {
		request.Limit = 50
	}
	viewer := localViewer(authorization)
	if err := s.Store.EnsureDefaultWorkspaceMembership(r.Context(), viewer); err != nil {
		writeStoreError(w, err)
		return
	}
	place, err := s.Store.PlaceFor(r.Context(), request.PlaceID, viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	messages, err := s.Store.History(r.Context(), request.PlaceID, viewer, HistoryOptions{BeforeSeq: request.BeforeSeq, Limit: request.Limit})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	profiles, err := s.Store.ActiveMembers(r.Context(), request.PlaceID, viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	lastRead, err := s.Store.ReadMarker(r.Context(), request.PlaceID, viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	wires := make([]messageWire, len(messages))
	for i, message := range messages {
		wires[i] = messageToWire(place, message)
	}
	members := make([]memberWire, len(profiles))
	for i, profile := range profiles {
		members[i] = memberWire{Participant: participantToWire(profile.Participant), DisplayName: profile.ProjectedDisplayName()}
	}
	writeJSON(w, http.StatusOK, struct {
		Place       placeWire     `json:"place"`
		LatestSeq   int64         `json:"latest_seq"`
		LastReadSeq int64         `json:"last_read_seq"`
		Members     []memberWire  `json:"members"`
		Messages    []messageWire `json:"messages"`
	}{placeToWire(place), place.LastSeq, lastRead, members, wires})
}

func (s *Server) localWrite(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		PlaceID     string  `json:"place_id"`
		Content     string  `json:"content"`
		Urgency     string  `json:"urgency"`
		ReplyTo     *string `json:"reply_to,omitempty"`
		ClientNonce string  `json:"client_nonce"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	viewer := localViewer(authorization)
	if err := s.Store.EnsureDefaultWorkspaceMembership(r.Context(), viewer); err != nil {
		writeStoreError(w, err)
		return
	}
	place, err := s.Store.PlaceFor(r.Context(), request.PlaceID, viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	replyTo := ""
	if request.ReplyTo != nil {
		replyTo = *request.ReplyTo
	}
	message, created, err := s.Store.AppendMessage(r.Context(), AppendInput{
		PlaceID: request.PlaceID, Author: viewer, Content: request.Content,
		Urgency: request.Urgency, ReplyTo: replyTo, ClientNonce: request.ClientNonce,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if created {
		publishMessageCreated(r.Context(), s.Store, s.Hub, place, message)
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
	}
	writeJSON(w, status, struct {
		MessageID string      `json:"message_id"`
		Seq       int64       `json:"seq"`
		Message   messageWire `json:"message"`
	}{message.MessageID, message.Seq, messageToWire(place, message)})
}

// localReact toggles the agent's emoji on a message through the identical
// store path the human UI uses. The tool layer scopes it to messages visible
// in the currently open view (ADR 0011 §3: 見えていないものは操作できない);
// the server enforces the shared permission model.
func (s *Server) localReact(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		PlaceID   string `json:"place_id"`
		MessageID string `json:"message_id"`
		Emoji     string `json:"emoji"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.PlaceID == "" || request.MessageID == "" || validateReactionEmoji(request.Emoji) != nil {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	viewer := localViewer(authorization)
	if err := s.Store.EnsureDefaultWorkspaceMembership(r.Context(), viewer); err != nil {
		writeStoreError(w, err)
		return
	}
	place, err := s.Store.PlaceFor(r.Context(), request.PlaceID, viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	message, reacted, err := s.Store.ToggleReaction(r.Context(), request.PlaceID, request.MessageID, viewer, request.Emoji)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	wire := messageToWire(place, message)
	if s.Hub != nil {
		s.Hub.Publish(r.Context(), Event{Type: EventReactionUpdated, PlaceID: request.PlaceID, Message: &wire})
	}
	writeJSON(w, http.StatusOK, struct {
		Message messageWire `json:"message"`
		Reacted bool        `json:"reacted"`
	}{wire, reacted})
}

// localStatus sets the agent's own status through the identical store path the
// human status menu uses. Unlike react and reply-later it is not scoped to an
// open place: a person's attention state is about the person, not a screen.
func (s *Server) localStatus(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		Status string `json:"status"`
		Note   string `json:"note,omitempty"`
		// 0 (or omitted) means the status holds until it is replaced.
		ExpiresInMinutes uint32 `json:"expires_in_minutes,omitempty"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	if !ValidStatus(request.Status) {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if utf8.RuneCountInString(request.Note) > MaxStatusNoteChars ||
		request.ExpiresInMinutes > maxRelativeMinutes {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	var expiresAt *time.Time
	if request.ExpiresInMinutes > 0 {
		moment := time.Now().Add(time.Duration(request.ExpiresInMinutes) * time.Minute)
		expiresAt = &moment
	}
	viewer := localViewer(authorization)
	if err := s.Store.EnsureDefaultWorkspaceMembership(r.Context(), viewer); err != nil {
		writeStoreError(w, err)
		return
	}
	status, err := s.Store.SetStatus(r.Context(), viewer, request.Status, request.Note, expiresAt)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	s.publishStatus(r.Context(), status)
	writeJSON(w, http.StatusOK, struct {
		Status statusWire `json:"status"`
	}{statusToWire(status)})
}

// localReplyLater places the agent's own「後で返信します」marker. The tool
// layer scopes it to messages visible in the currently open view, the same
// rule as react (ADR 0011 §3); the server enforces the shared permission
// model. The marker's own copy carries remind_at because the agent is its
// owner — other participants' wires never do.
func (s *Server) localReplyLater(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		PlaceID   string `json:"place_id"`
		MessageID string `json:"message_id"`
		Note      string `json:"note,omitempty"`
		// Relative so the server's clock, not the workspace's, fixes the moment.
		RemindInMinutes uint32 `json:"remind_in_minutes,omitempty"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.PlaceID == "" || request.MessageID == "" ||
		utf8.RuneCountInString(request.Note) > MaxReplyLaterNoteChars ||
		request.RemindInMinutes > maxRelativeMinutes {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	remindAt := time.Now().Add(DefaultReplyLaterDelay)
	if request.RemindInMinutes > 0 {
		remindAt = time.Now().Add(time.Duration(request.RemindInMinutes) * time.Minute)
	}
	viewer := localViewer(authorization)
	if err := s.Store.EnsureDefaultWorkspaceMembership(r.Context(), viewer); err != nil {
		writeStoreError(w, err)
		return
	}
	marker, created, err := s.Store.CreateReplyLater(
		r.Context(), request.PlaceID, request.MessageID, viewer, request.Note, remindAt)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if created {
		s.publishReplyLaterCreated(r.Context(), marker)
	}
	// TODO(#128): the agent's own reminder rides the「予定された出来事」覚醒
	// トリガ from here once that trigger exists; the marker is already durable.
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
	}
	writeJSON(w, status, struct {
		Marker  replyLaterWire `json:"marker"`
		Created bool           `json:"created"`
	}{replyLaterToWire(marker, viewer), created})
}

// localReplyLaterResolve marks the agent's own promise as kept. Someone else's
// marker is reported as missing, never as forbidden.
func (s *Server) localReplyLaterResolve(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		MarkerID string `json:"marker_id"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.MarkerID == "" {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	viewer := localViewer(authorization)
	if err := s.Store.EnsureDefaultWorkspaceMembership(r.Context(), viewer); err != nil {
		writeStoreError(w, err)
		return
	}
	marker, err := s.Store.ResolveReplyLater(r.Context(), request.MarkerID, viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	s.publishReplyLaterResolved(r.Context(), marker)
	writeJSON(w, http.StatusOK, struct {
		Marker replyLaterWire `json:"marker"`
	}{replyLaterToWire(marker, viewer)})
}

// localNotificationSettings reads or updates the agent's own notification
// setting through the identical store path the human UI uses. A request with
// no field set is a read; any field present is a change to that field only,
// because an agent naming one preference ("この place は mute にして") should not
// silently discard the rest of its setting the way a full PUT would.
func (s *Server) localNotificationSettings(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		DefaultsLevel *string `json:"defaults_level,omitempty"`
		PerPlace      *[]struct {
			PlaceID string `json:"place_id"`
			Level   string `json:"level"`
		} `json:"per_place,omitempty"`
		Keywords *[]string `json:"keywords,omitempty"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	viewer := localViewer(authorization)
	if err := s.Store.EnsureDefaultWorkspaceMembership(r.Context(), viewer); err != nil {
		writeStoreError(w, err)
		return
	}
	current, err := s.Store.NotificationSettingFor(r.Context(), viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if request.DefaultsLevel == nil && request.PerPlace == nil && request.Keywords == nil {
		writeJSON(w, http.StatusOK, struct {
			Setting notificationSettingWire `json:"setting"`
		}{notificationSettingToWire(current)})
		return
	}
	defaultLevel := current.Default()
	if request.DefaultsLevel != nil {
		defaultLevel = *request.DefaultsLevel
	}
	perPlace := current.PerPlace
	if request.PerPlace != nil {
		perPlace = make([]PlaceNotifyLevel, 0, len(*request.PerPlace))
		for _, entry := range *request.PerPlace {
			if entry.PlaceID == "" {
				writeError(w, http.StatusBadRequest, "invalid_request")
				return
			}
			perPlace = append(perPlace, PlaceNotifyLevel{PlaceID: entry.PlaceID, Level: entry.Level})
		}
	}
	keywords := current.Keywords
	if request.Keywords != nil {
		keywords = *request.Keywords
	}
	stored, err := s.Store.SetNotificationSetting(r.Context(), viewer, defaultLevel, perPlace, keywords)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Setting notificationSettingWire `json:"setting"`
	}{notificationSettingToWire(stored)})
}

// localStartDM opens a direct conversation for the agent through the same
// store calls POST /messaging/dms and POST /messaging/group-dms use. One other
// participant means the single dm with them (existing or freshly minted);
// several mean a group dm. The agent is always one of the participants and is
// never named in the body — the credential decides who is acting.
func (s *Server) localStartDM(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		Participants []participantWire `json:"participants"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	if len(request.Participants) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	others := make([]ParticipantRef, 0, len(request.Participants))
	for _, wire := range request.Participants {
		ref, err := wire.ref()
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request")
			return
		}
		others = append(others, ref)
	}
	viewer := localViewer(authorization)
	if err := s.Store.EnsureDefaultWorkspaceMembership(r.Context(), viewer); err != nil {
		writeStoreError(w, err)
		return
	}
	var (
		place   Place
		created = true
		err     error
	)
	if len(others) == 1 {
		place, created, err = s.Store.EnsureDM(r.Context(), viewer, others[0])
	} else {
		place, err = s.Store.CreateGroupDM(r.Context(), viewer, others)
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	wire := dmWire{
		DMID: place.PlaceID, Kind: place.Kind,
		Participants: append([]participantWire{participantToWire(viewer)}, request.Participants...),
	}
	if created && s.Hub != nil {
		s.Hub.Publish(r.Context(), Event{Type: EventPlaceCreated, PlaceID: place.PlaceID, DM: &wire})
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
	}
	writeJSON(w, status, struct {
		DM      dmWire `json:"dm"`
		Created bool   `json:"created"`
	}{wire, created})
}

// localCreateChannel opens a channel in the workspace, the same act as the
// sidebar's「チャンネルを作成」. An omitted workspace_id means the one workspace
// the agent is in — naming it is only required once there is more than one.
func (s *Server) localCreateChannel(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		WorkspaceID string `json:"workspace_id,omitempty"`
		Name        string `json:"name"`
		Topic       string `json:"topic,omitempty"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.Name == "" || utf8.RuneCountInString(request.Name) > MaxChannelNameChars ||
		len(request.Topic) > maxTopicBytes {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	viewer := localViewer(authorization)
	if err := s.Store.EnsureDefaultWorkspaceMembership(r.Context(), viewer); err != nil {
		writeStoreError(w, err)
		return
	}
	workspaceID, err := s.soleWorkspaceID(r.Context(), viewer, request.WorkspaceID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	place, err := s.Store.CreateChannel(r.Context(), workspaceID, request.Name, request.Topic, viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	s.publishChannel(r.Context(), EventPlaceCreated, place)
	writeJSON(w, http.StatusCreated, struct {
		Channel channelWire `json:"channel"`
	}{channelToWire(place)})
}

// localUpdateChannel renames a channel, retopics it, or both. An omitted field
// is left alone: naming one thing must not silently discard the other.
func (s *Server) localUpdateChannel(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		PlaceID string  `json:"place_id"`
		Name    *string `json:"name,omitempty"`
		Topic   *string `json:"topic,omitempty"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.PlaceID == "" || (request.Name == nil && request.Topic == nil) {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if request.Name != nil &&
		(*request.Name == "" || utf8.RuneCountInString(*request.Name) > MaxChannelNameChars) {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if request.Topic != nil && len(*request.Topic) > maxTopicBytes {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	viewer := localViewer(authorization)
	if err := s.Store.EnsureDefaultWorkspaceMembership(r.Context(), viewer); err != nil {
		writeStoreError(w, err)
		return
	}
	place, err := s.Store.UpdateChannel(r.Context(), request.PlaceID, request.Name, request.Topic, viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	s.publishChannel(r.Context(), EventPlaceUpdated, place)
	writeJSON(w, http.StatusOK, struct {
		Channel channelWire `json:"channel"`
	}{channelToWire(place)})
}

// localDuplicateChannel copies a channel's shape (name and topic) into a new,
// empty one. An omitted name takes the same derived default the human menu
// gets, so neither side has its own idea of what a copy is called.
func (s *Server) localDuplicateChannel(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		PlaceID string `json:"place_id"`
		Name    string `json:"name,omitempty"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.PlaceID == "" || utf8.RuneCountInString(request.Name) > MaxChannelNameChars {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	viewer := localViewer(authorization)
	if err := s.Store.EnsureDefaultWorkspaceMembership(r.Context(), viewer); err != nil {
		writeStoreError(w, err)
		return
	}
	place, err := s.Store.DuplicateChannel(r.Context(), request.PlaceID, request.Name, viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	s.publishChannel(r.Context(), EventPlaceCreated, place)
	writeJSON(w, http.StatusCreated, struct {
		Channel channelWire `json:"channel"`
	}{channelToWire(place)})
}

// soleWorkspaceID resolves the workspace a channel act happens in. An explicit
// id is used as given; without one the agent's single workspace is implied, and
// an ambiguous membership is refused rather than guessed.
func (s *Server) soleWorkspaceID(ctx context.Context, viewer ParticipantRef, requested string) (string, error) {
	if requested != "" {
		return requested, nil
	}
	workspaces, err := s.Store.WorkspacesFor(ctx, viewer)
	if err != nil {
		return "", err
	}
	if len(workspaces) != 1 {
		return "", ErrWorkspaceNotFound
	}
	return workspaces[0].WorkspaceID, nil
}

func (s *Server) publishChannel(ctx context.Context, eventType string, place Place) {
	if s.Hub == nil {
		return
	}
	wire := channelToWire(place)
	s.Hub.Publish(ctx, Event{Type: eventType, PlaceID: place.PlaceID, Channel: &wire})
}

// localSearch is the agent's copy of the human search box, through the
// identical store path (SearchMessages)。可視性は store が決めるので、agent が
// 見られない place の発言は結果に現れず、見られない place を名指しした検索は
// 「無い」と答える——人間の UI と同じ答え方である。
func (s *Server) localSearch(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		Query   string `json:"query"`
		PlaceID string `json:"place_id,omitempty"`
		Limit   int    `json:"limit,omitempty"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	query := strings.TrimSpace(request.Query)
	if query == "" || len(query) > MaxSearchQueryBytes || request.Limit < 0 {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	viewer := localViewer(authorization)
	if err := s.Store.EnsureDefaultWorkspaceMembership(r.Context(), viewer); err != nil {
		writeStoreError(w, err)
		return
	}
	results, err := s.Store.SearchMessages(r.Context(), viewer, query,
		SearchOptions{PlaceID: request.PlaceID, Limit: request.Limit})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	wires := make([]searchResultWire, len(results))
	for i, result := range results {
		wires[i] = searchResultWire{
			MessageID: result.Message.MessageID,
			Place:     placeToWire(result.Place),
			Seq:       result.Message.Seq,
			Author:    participantToWire(result.Message.Author),
			Snippet:   result.Snippet,
			CreatedAt: result.Message.CreatedAt,
		}
	}
	writeJSON(w, http.StatusOK, struct {
		Results []searchResultWire `json:"results"`
	}{wires})
}

// attentionCandidateWire is one queued「呼ばれた」. actor / message_id / 本文は
// 載せない：候補は message ref であって本文の注入ではない（凍結契約 v1）。
// 続きは place を open して読む——人間が通知から place を開くのと同じ動きで、
// そこで read cursor も正しく進む。
type attentionCandidateWire struct {
	CandidateID  string    `json:"candidate_id"`
	CandidateSeq int64     `json:"candidate_seq"`
	Place        placeWire `json:"place"`
	MessageSeq   int64     `json:"message_seq"`
	Reason       string    `json:"reason"`
	ArrivalTime  time.Time `json:"arrival_time"`
}

// localAttention hands the agent its own unconsumed AttentionCandidates and, in
// the same call, acks what a previous call already took in.
//
// **暫定配線である（ADR 0010 覚醒トリガ / issue #173）。** 本設計では候補の
// 到着そのものが本人を起こす。ここには自動覚醒が無く、起きている本人が自分で
// 取りに来る形にしてある。それでも「runtime が止まっていた間に呼ばれたこと」は
// shared 側に積まれているので、次に動いたときに必ず見つかる。
//
// consume_through は先に適用する：本人が「ここまで取り込んだ」と言ってから
// 「次は何か」を聞く順である。
func (s *Server) localAttention(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		ConsumeThrough int64 `json:"consume_through,omitempty"`
		Limit          int   `json:"limit,omitempty"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.ConsumeThrough < 0 || request.Limit < 0 {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	viewer := localViewer(authorization)
	if err := s.Store.EnsureDefaultWorkspaceMembership(r.Context(), viewer); err != nil {
		writeStoreError(w, err)
		return
	}
	var consumed int64
	if request.ConsumeThrough > 0 {
		count, err := s.Store.ConsumeAttentionCandidates(r.Context(), viewer, request.ConsumeThrough)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		consumed = count
	}
	candidates, err := s.Store.PendingAttentionCandidates(r.Context(), viewer, request.Limit)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	latestSeq, err := s.Store.LatestAttentionSeq(r.Context(), viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	wires := make([]attentionCandidateWire, len(candidates))
	for i, candidate := range candidates {
		wires[i] = attentionCandidateWire{
			CandidateID:  candidate.CandidateID,
			CandidateSeq: candidate.CandidateSeq,
			Place:        placeToWire(Place{PlaceID: candidate.PlaceID, Kind: candidate.PlaceKind}),
			MessageSeq:   candidate.MessageSeq,
			Reason:       candidate.Reason,
			ArrivalTime:  candidate.CreatedAt,
		}
	}
	writeJSON(w, http.StatusOK, struct {
		Candidates []attentionCandidateWire `json:"candidates"`
		Consumed   int64                    `json:"consumed"`
		LatestSeq  int64                    `json:"latest_seq"`
	}{wires, consumed, latestSeq})
}

func (s *Server) localReadThrough(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		PlaceID string `json:"place_id"`
		Seq     int64  `json:"seq"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	viewer := localViewer(authorization)
	if err := s.Store.EnsureDefaultWorkspaceMembership(r.Context(), viewer); err != nil {
		writeStoreError(w, err)
		return
	}
	if err := s.Store.ReadThrough(r.Context(), request.PlaceID, viewer, request.Seq); err != nil {
		writeStoreError(w, err)
		return
	}
	place, err := s.Store.PlaceFor(r.Context(), request.PlaceID, viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	lastRead, err := s.Store.ReadMarker(r.Context(), request.PlaceID, viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Place       placeWire `json:"place"`
		LastReadSeq int64     `json:"last_read_seq"`
	}{placeToWire(place), lastRead})
}
