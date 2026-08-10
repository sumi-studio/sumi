package messaging

import (
	"net/http"
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
	if created && s.Hub != nil {
		wire := messageToWire(place, message)
		s.Hub.Publish(r.Context(), Event{Type: EventMessageCreated, PlaceID: request.PlaceID, Message: &wire})
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
		PlaceID     string `json:"place_id"`
		MessageID   string `json:"message_id"`
		Emoji       string `json:"emoji"`
		ClientNonce string `json:"client_nonce"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.PlaceID == "" || request.MessageID == "" || request.ClientNonce == "" || len(request.ClientNonce) > 128 || validateReactionEmoji(request.Emoji) != nil {
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
	message, reacted, err := s.toggleReaction(r.Context(), request.PlaceID, request.MessageID, viewer, request.Emoji, request.ClientNonce)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	wire := messageToWire(place, message)
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
	switch request.Status {
	case StatusAvailable, StatusBusy, StatusAway:
	default:
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
