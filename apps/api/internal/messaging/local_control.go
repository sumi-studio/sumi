package messaging

import (
	"net/http"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

const (
	LocalOverviewPath    = "/local-control/v1/messaging:overview"
	LocalOpenPath        = "/local-control/v1/messaging:open"
	LocalWritePath       = "/local-control/v1/messaging:write"
	LocalReadThroughPath = "/local-control/v1/messaging:read-through"
)

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
		members[i] = memberWire{Participant: participantToWire(profile.Participant), DisplayName: profile.DisplayName}
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

func (s *Server) localReadThrough(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		PlaceID string `json:"place_id"`
		Seq     int64  `json:"seq"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	viewer := localViewer(authorization)
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
