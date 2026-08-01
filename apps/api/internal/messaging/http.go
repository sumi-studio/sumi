package messaging

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

// maxRequestBytes bounds any /messaging request body: the largest legal
// message content plus envelope headroom.
const maxRequestBytes = MaxContentBytes + 64*1024

// Server is the /messaging REST surface. It authenticates the browser session
// (exact-origin allowlist + signed HttpOnly cookie, the same policy as the
// direct-chat routes) and acts as the session's Human participant. Every
// permission decision is delegated to the Store, so this layer holds transport
// concerns only (凍結契約 v1 §4: 人間がUIから行うのと同じ経路・同じ権限モデル).
//
// A nil Sessions verifier or an empty origin allowlist fails closed.
type Server struct {
	Store          *Store
	Sessions       agentevents.UserSessionAuthorizer
	AllowedOrigins []string
	// Hub, when set, receives durable events from REST mutations so live
	// WebSocket subscribers see messages regardless of which transport
	// committed them. Nil is fine: durable truth lives in the store.
	Hub *Hub
}

// NewServer returns a messaging REST server backed by the store.
func NewServer(store *Store, sessions agentevents.UserSessionAuthorizer) *Server {
	return &Server{Store: store, Sessions: sessions}
}

// RegisterRoutes mounts the /messaging routes on the public mux.
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /messaging/bootstrap", s.serveBootstrap)
	mux.HandleFunc("POST /messaging/channels", s.serveCreateChannel)
	mux.HandleFunc("POST /messaging/dms", s.serveEnsureDM)
	mux.HandleFunc("POST /messaging/group-dms", s.serveCreateGroupDM)
	mux.HandleFunc("GET /messaging/places/{place_id}", s.servePlace)
	mux.HandleFunc("GET /messaging/places/{place_id}/messages", s.serveHistory)
	mux.HandleFunc("POST /messaging/places/{place_id}/messages", s.serveSend)
	mux.HandleFunc("PATCH /messaging/places/{place_id}/messages/{message_id}", s.serveEdit)
	mux.HandleFunc("DELETE /messaging/places/{place_id}/messages/{message_id}", s.serveDelete)
	mux.HandleFunc("PUT /messaging/places/{place_id}/read-through", s.serveReadThrough)
}

// --- wire shapes (snake_case, ActorRef/PlaceRef-compatible) ---

type participantWire struct {
	Kind               string `json:"kind"`
	HumanID            string `json:"human_id,omitempty"`
	PersonalityAgentID string `json:"personality_agent_id,omitempty"`
}

func participantToWire(p ParticipantRef) participantWire {
	switch p.Kind {
	case KindHuman:
		return participantWire{Kind: string(p.Kind), HumanID: p.ID}
	default:
		return participantWire{Kind: string(p.Kind), PersonalityAgentID: p.ID}
	}
}

// ref converts a client-sent participant to a validated ParticipantRef.
// Unknown kinds fail closed.
func (w participantWire) ref() (ParticipantRef, error) {
	var p ParticipantRef
	switch ParticipantKind(w.Kind) {
	case KindHuman:
		if w.PersonalityAgentID != "" {
			return p, fmt.Errorf("human participant must not carry personality_agent_id")
		}
		p = Human(w.HumanID)
	case KindPersonalityAgent:
		if w.HumanID != "" {
			return p, fmt.Errorf("personality_agent participant must not carry human_id")
		}
		p = PersonalityAgent(w.PersonalityAgentID)
	default:
		return p, fmt.Errorf("unknown participant kind %q", w.Kind)
	}
	if err := p.Validate(); err != nil {
		return p, err
	}
	return p, nil
}

func participantsToWire(refs []ParticipantRef) []participantWire {
	out := make([]participantWire, len(refs))
	for i, r := range refs {
		out[i] = participantToWire(r)
	}
	return out
}

// placeWire matches the frozen PlaceRef shape from contracts/agent-events.yaml.
type placeWire struct {
	Kind      string `json:"kind"`
	ChannelID string `json:"channel_id,omitempty"`
	DMID      string `json:"dm_id,omitempty"`
}

func placeToWire(p Place) placeWire {
	if p.Kind == PlaceChannel {
		return placeWire{Kind: p.Kind, ChannelID: p.PlaceID}
	}
	return placeWire{Kind: p.Kind, DMID: p.PlaceID}
}

type messageWire struct {
	MessageID   string            `json:"message_id"`
	Place       placeWire         `json:"place"`
	Seq         int64             `json:"seq"`
	Author      participantWire   `json:"author"`
	Content     string            `json:"content"`
	Mentions    []participantWire `json:"mentions"`
	Urgency     string            `json:"urgency"`
	ReplyTo     *string           `json:"reply_to"`
	ClientNonce string            `json:"client_nonce"`
	CreatedAt   time.Time         `json:"created_at"`
	EditedAt    *time.Time        `json:"edited_at"`
	Deleted     bool              `json:"deleted"`
}

func messageToWire(place Place, m Message) messageWire {
	w := messageWire{
		MessageID:   m.MessageID,
		Place:       placeToWire(place),
		Seq:         m.Seq,
		Author:      participantToWire(m.Author),
		Content:     m.Content,
		Mentions:    participantsToWire(m.Mentions),
		Urgency:     m.Urgency,
		ClientNonce: m.ClientNonce,
		CreatedAt:   m.CreatedAt,
		EditedAt:    m.EditedAt,
		Deleted:     m.Deleted,
	}
	if m.ReplyTo != "" {
		w.ReplyTo = &m.ReplyTo
	}
	return w
}

type workspaceWire struct {
	WorkspaceID string `json:"workspace_id"`
	Name        string `json:"name"`
}

type channelWire struct {
	ChannelID   string `json:"channel_id"`
	WorkspaceID string `json:"workspace_id"`
	Name        string `json:"name"`
	Topic       string `json:"topic"`
	Visibility  string `json:"visibility"`
}

func channelToWire(p Place) channelWire {
	return channelWire{
		ChannelID:   p.PlaceID,
		WorkspaceID: p.WorkspaceID,
		Name:        p.Name,
		Topic:       p.Topic,
		Visibility:  p.Visibility,
	}
}

type dmWire struct {
	DMID         string            `json:"dm_id"`
	Kind         string            `json:"kind"`
	Participants []participantWire `json:"participants"`
}

type memberWire struct {
	Participant participantWire `json:"participant"`
	DisplayName string          `json:"display_name"`
}

type readMarkerWire struct {
	Place       placeWire `json:"place"`
	LastReadSeq int64     `json:"last_read_seq"`
}

type unreadSummaryWire struct {
	Place        placeWire `json:"place"`
	LatestSeq    int64     `json:"latest_seq"`
	UnreadCount  int64     `json:"unread_count"`
	MentionCount int64     `json:"mention_count"`
}

// --- authentication ---

// viewer authenticates the request and returns the acting participant. The
// browser session lane acts as the session's Human; the agent lane (bearer
// token, acting as the PersonalityAgent) is added alongside when the agent
// tools land — both resolve to a ParticipantRef here and nothing below this
// point distinguishes them.
func (s *Server) viewer(w http.ResponseWriter, r *http.Request) (ParticipantRef, agentevents.UserSessionClaims, bool) {
	var none agentevents.UserSessionClaims
	// Origin is a CSRF boundary for unsafe REST methods. Browsers may omit it
	// from same-origin GET fetches; those reads remain protected by the browser
	// session cookie and same-origin response policy.
	if r.Method != http.MethodGet && !agentevents.BrowserOriginAllowed(r, s.AllowedOrigins) {
		writeError(w, http.StatusForbidden, "origin_not_allowed")
		return ParticipantRef{}, none, false
	}
	cookies := r.CookiesNamed(agentevents.BrowserSessionCookie)
	switch {
	case len(cookies) > 1:
		writeError(w, http.StatusBadRequest, "duplicate_session_cookies")
		return ParticipantRef{}, none, false
	case len(cookies) == 0 || s.Sessions == nil:
		writeError(w, http.StatusUnauthorized, "missing_session")
		return ParticipantRef{}, none, false
	}
	claims, err := s.Sessions.VerifySession(r.Context(), cookies[0].Value)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid_session")
		return ParticipantRef{}, none, false
	}
	viewer := Human(claims.UserID)
	if err := viewer.Validate(); err != nil {
		writeError(w, http.StatusUnauthorized, "invalid_session")
		return ParticipantRef{}, none, false
	}
	return viewer, claims, true
}

// mutate runs op under the session's durable admission lease so a completed
// logout is a barrier: no mutation from that session lands after it (same
// serialization as direct-chat command admission). The returned error is op's
// own error; a lease that was never granted writes 401 and reports done=false.
func (s *Server) mutate(w http.ResponseWriter, r *http.Request, claims agentevents.UserSessionClaims, op func() error) (bool, error) {
	called := false
	err := s.Sessions.AuthorizeSession(r.Context(), claims, func() error {
		called = true
		return op()
	})
	if !called {
		writeError(w, http.StatusUnauthorized, "invalid_session")
		return false, nil
	}
	return true, err
}

// --- handlers ---

func (s *Server) serveBootstrap(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	done, err := s.mutate(w, r, claims, func() error {
		return s.Store.EnsureDefaultWorkspaceMembership(r.Context(), viewer)
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	ctx := r.Context()
	summaries, err := s.Store.UnreadSummaries(ctx, viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	workspaces, err := s.Store.WorkspacesFor(ctx, viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}

	memberSet := map[string]memberWire{}
	var memberOrder []string
	addMembers := func(profiles []MemberProfile) {
		for _, p := range profiles {
			key := p.Participant.Key()
			if _, seen := memberSet[key]; seen {
				continue
			}
			memberSet[key] = memberWire{
				Participant: participantToWire(p.Participant),
				DisplayName: p.ProjectedDisplayName(),
			}
			memberOrder = append(memberOrder, key)
		}
	}

	workspaceWires := make([]workspaceWire, len(workspaces))
	for i, ws := range workspaces {
		workspaceWires[i] = workspaceWire{WorkspaceID: ws.WorkspaceID, Name: ws.Name}
		profiles, err := s.Store.WorkspaceMemberProfiles(ctx, ws.WorkspaceID, viewer)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		addMembers(profiles)
	}

	channels := []channelWire{}
	dms := []dmWire{}
	readMarkers := []readMarkerWire{}
	unread := []unreadSummaryWire{}
	for _, sum := range summaries {
		pw := placeToWire(sum.Place)
		readMarkers = append(readMarkers, readMarkerWire{Place: pw, LastReadSeq: sum.LastReadSeq})
		unread = append(unread, unreadSummaryWire{
			Place: pw, LatestSeq: sum.Place.LastSeq,
			UnreadCount: sum.UnreadCount, MentionCount: sum.MentionCount,
		})
		if sum.Place.Kind == PlaceChannel {
			channels = append(channels, channelToWire(sum.Place))
			continue
		}
		profiles, err := s.Store.ActiveMembers(ctx, sum.Place.PlaceID, viewer)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		addMembers(profiles)
		participants := make([]participantWire, len(profiles))
		for i, p := range profiles {
			participants[i] = participantToWire(p.Participant)
		}
		dms = append(dms, dmWire{DMID: sum.Place.PlaceID, Kind: sum.Place.Kind, Participants: participants})
	}

	members := make([]memberWire, len(memberOrder))
	for i, key := range memberOrder {
		members[i] = memberSet[key]
	}
	writeJSON(w, http.StatusOK, struct {
		Self            participantWire     `json:"self"`
		Workspaces      []workspaceWire     `json:"workspaces"`
		Channels        []channelWire       `json:"channels"`
		DMs             []dmWire            `json:"dms"`
		Members         []memberWire        `json:"members"`
		ReadMarkers     []readMarkerWire    `json:"read_markers"`
		UnreadSummaries []unreadSummaryWire `json:"unread_summaries"`
	}{
		Self:            participantToWire(viewer),
		Workspaces:      workspaceWires,
		Channels:        channels,
		DMs:             dms,
		Members:         members,
		ReadMarkers:     readMarkers,
		UnreadSummaries: unread,
	})
}

func (s *Server) serveCreateChannel(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	var req struct {
		WorkspaceID string `json:"workspace_id"`
		Name        string `json:"name"`
		Topic       string `json:"topic"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Name == "" || len(req.Name) > 200 {
		writeError(w, http.StatusBadRequest, "invalid_name")
		return
	}
	var place Place
	done, err := s.mutate(w, r, claims, func() error {
		var opErr error
		place, opErr = s.Store.CreateChannel(r.Context(), req.WorkspaceID, req.Name, req.Topic, viewer)
		return opErr
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, channelToWire(place))
}

func (s *Server) serveEnsureDM(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	var req struct {
		Participant participantWire `json:"participant"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	other, err := req.Participant.ref()
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_participant")
		return
	}
	var place Place
	done, err := s.mutate(w, r, claims, func() error {
		var opErr error
		place, opErr = s.Store.EnsureDM(r.Context(), viewer, other)
		return opErr
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, dmWire{
		DMID: place.PlaceID, Kind: place.Kind,
		Participants: []participantWire{participantToWire(viewer), participantToWire(other)},
	})
}

func (s *Server) serveCreateGroupDM(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	var req struct {
		Participants []participantWire `json:"participants"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	others := make([]ParticipantRef, 0, len(req.Participants))
	for _, pw := range req.Participants {
		ref, err := pw.ref()
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_participant")
			return
		}
		others = append(others, ref)
	}
	var place Place
	done, err := s.mutate(w, r, claims, func() error {
		var opErr error
		place, opErr = s.Store.CreateGroupDM(r.Context(), viewer, others)
		return opErr
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, dmWire{
		DMID: place.PlaceID, Kind: place.Kind,
		Participants: append([]participantWire{participantToWire(viewer)}, req.Participants...),
	})
}

func (s *Server) servePlace(w http.ResponseWriter, r *http.Request) {
	viewer, _, ok := s.viewer(w, r)
	if !ok {
		return
	}
	place, err := s.Store.PlaceFor(r.Context(), r.PathValue("place_id"), viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	profiles, err := s.Store.ActiveMembers(r.Context(), place.PlaceID, viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	members := make([]memberWire, len(profiles))
	for i, p := range profiles {
		members[i] = memberWire{Participant: participantToWire(p.Participant), DisplayName: p.ProjectedDisplayName()}
	}
	writeJSON(w, http.StatusOK, struct {
		Place     placeWire    `json:"place"`
		LatestSeq int64        `json:"latest_seq"`
		Members   []memberWire `json:"members"`
	}{Place: placeToWire(place), LatestSeq: place.LastSeq, Members: members})
}

func (s *Server) serveHistory(w http.ResponseWriter, r *http.Request) {
	viewer, _, ok := s.viewer(w, r)
	if !ok {
		return
	}
	placeID := r.PathValue("place_id")
	var opt HistoryOptions
	if v := r.URL.Query().Get("before_seq"); v != "" {
		seq, err := strconv.ParseInt(v, 10, 64)
		if err != nil || seq <= 0 {
			writeError(w, http.StatusBadRequest, "invalid_before_seq")
			return
		}
		opt.BeforeSeq = seq
	}
	if v := r.URL.Query().Get("limit"); v != "" {
		limit, err := strconv.Atoi(v)
		if err != nil || limit <= 0 {
			writeError(w, http.StatusBadRequest, "invalid_limit")
			return
		}
		opt.Limit = limit
	}
	place, err := s.Store.PlaceFor(r.Context(), placeID, viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	messages, err := s.Store.History(r.Context(), placeID, viewer, opt)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	wires := make([]messageWire, len(messages))
	for i, m := range messages {
		wires[i] = messageToWire(place, m)
	}
	writeJSON(w, http.StatusOK, struct {
		Messages []messageWire `json:"messages"`
	}{Messages: wires})
}

func (s *Server) serveSend(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	placeID := r.PathValue("place_id")
	var req struct {
		Content     string `json:"content"`
		Urgency     string `json:"urgency"`
		ReplyTo     string `json:"reply_to"`
		ClientNonce string `json:"client_nonce"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	switch req.Urgency {
	case "", UrgencyUrgent, UrgencyNormal, UrgencyFYI:
	default:
		writeError(w, http.StatusBadRequest, "invalid_urgency")
		return
	}
	if req.Content == "" || len(req.Content) > MaxContentBytes {
		writeError(w, http.StatusBadRequest, "invalid_content")
		return
	}
	if req.ClientNonce == "" || len(req.ClientNonce) > 128 {
		writeError(w, http.StatusBadRequest, "invalid_client_nonce")
		return
	}
	place, err := s.Store.PlaceFor(r.Context(), placeID, viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	var (
		msg     Message
		created bool
	)
	done, err := s.mutate(w, r, claims, func() error {
		var opErr error
		msg, created, opErr = s.Store.AppendMessage(r.Context(), AppendInput{
			PlaceID: placeID, Author: viewer, Content: req.Content,
			Urgency: req.Urgency, ReplyTo: req.ReplyTo, ClientNonce: req.ClientNonce,
		})
		return opErr
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	status := http.StatusCreated
	if !created {
		// An idempotent replay: the original receipt, not a new message.
		status = http.StatusOK
	}
	if created {
		wire := messageToWire(place, msg)
		s.Hub.Publish(r.Context(), Event{Type: EventMessageCreated, PlaceID: placeID, Message: &wire})
	}
	writeJSON(w, status, struct {
		MessageID string      `json:"message_id"`
		Seq       int64       `json:"seq"`
		Message   messageWire `json:"message"`
	}{MessageID: msg.MessageID, Seq: msg.Seq, Message: messageToWire(place, msg)})
}

func (s *Server) serveEdit(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	placeID := r.PathValue("place_id")
	var req struct {
		Content string `json:"content"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Content == "" || len(req.Content) > MaxContentBytes {
		writeError(w, http.StatusBadRequest, "invalid_content")
		return
	}
	place, err := s.Store.PlaceFor(r.Context(), placeID, viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	var msg Message
	done, err := s.mutate(w, r, claims, func() error {
		var opErr error
		msg, opErr = s.Store.EditMessage(r.Context(), placeID, r.PathValue("message_id"), viewer, req.Content)
		return opErr
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	wire := messageToWire(place, msg)
	s.Hub.Publish(r.Context(), Event{Type: EventMessageEdited, PlaceID: placeID, Message: &wire})
	writeJSON(w, http.StatusOK, struct {
		Message messageWire `json:"message"`
	}{Message: wire})
}

func (s *Server) serveDelete(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	placeID := r.PathValue("place_id")
	place, err := s.Store.PlaceFor(r.Context(), placeID, viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	var msg Message
	done, err := s.mutate(w, r, claims, func() error {
		var opErr error
		msg, opErr = s.Store.DeleteMessage(r.Context(), placeID, r.PathValue("message_id"), viewer)
		return opErr
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	wire := messageToWire(place, msg)
	s.Hub.Publish(r.Context(), Event{Type: EventMessageDeleted, PlaceID: placeID, Message: &wire})
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) serveReadThrough(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	var req struct {
		Seq int64 `json:"seq"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	done, err := s.mutate(w, r, claims, func() error {
		return s.Store.ReadThrough(r.Context(), r.PathValue("place_id"), viewer, req.Seq)
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- plumbing ---

func decodeJSON(w http.ResponseWriter, r *http.Request, into any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "oversized")
			return false
		}
		writeError(w, http.StatusBadRequest, "invalid_json")
		return false
	}
	if dec.More() {
		writeError(w, http.StatusBadRequest, "invalid_json")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(body)
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]string{"error": code})
}

// writeStoreError maps store sentinels to transport codes. Unknown errors are
// internal: the handlers validate request shape up front, so anything else is
// a bug or an infrastructure failure.
func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrPlaceNotFound), errors.Is(err, ErrMessageNotFound),
		errors.Is(err, ErrWorkspaceNotFound), errors.Is(err, ErrParticipantNotFound):
		writeError(w, http.StatusNotFound, "not_found")
	case errors.Is(err, ErrNotAMember):
		writeError(w, http.StatusForbidden, "not_a_member")
	case errors.Is(err, ErrNotAuthor):
		writeError(w, http.StatusForbidden, "not_author")
	case errors.Is(err, ErrForbidden):
		writeError(w, http.StatusForbidden, "forbidden")
	case errors.Is(err, ErrNotReachable):
		writeError(w, http.StatusForbidden, "not_reachable")
	case errors.Is(err, ErrMessageDeleted):
		writeError(w, http.StatusConflict, "message_deleted")
	case errors.Is(err, ErrSeqBeyondLatest):
		writeError(w, http.StatusBadRequest, "seq_beyond_latest")
	default:
		writeError(w, http.StatusInternalServerError, "internal")
	}
}
