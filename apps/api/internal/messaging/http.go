package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

// maxRequestBytes bounds any /messaging request body: the largest legal
// message content plus envelope headroom.
const maxRequestBytes = MaxContentBytes + 64*1024

// maxTopicBytes bounds a channel topic: one header line, not a document.
const maxTopicBytes = 1000

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
	// reactionMu keeps a reaction commit, its authoritative snapshot and the
	// corresponding live publish in one process-local order. Hub itself is
	// process-local, so this is the ordering boundary clients can observe.
	reactionMu sync.Mutex
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
	mux.HandleFunc("PATCH /messaging/places/{place_id}", s.serveUpdatePlace)
	mux.HandleFunc("GET /messaging/places/{place_id}/messages", s.serveHistory)
	mux.HandleFunc("POST /messaging/places/{place_id}/messages", s.serveSend)
	mux.HandleFunc("PATCH /messaging/places/{place_id}/messages/{message_id}", s.serveEdit)
	mux.HandleFunc("DELETE /messaging/places/{place_id}/messages/{message_id}", s.serveDelete)
	mux.HandleFunc("POST /messaging/places/{place_id}/messages/{message_id}/reactions", s.serveToggleReaction)
	mux.HandleFunc("PUT /messaging/places/{place_id}/read-through", s.serveReadThrough)
	mux.HandleFunc("PUT /messaging/status", s.serveSetStatus)
	mux.HandleFunc("GET /messaging/notification-settings", s.serveNotificationSetting)
	mux.HandleFunc("PUT /messaging/notification-settings", s.serveSetNotificationSetting)
	mux.HandleFunc("POST /messaging/places/{place_id}/messages/{message_id}/reply-later", s.serveCreateReplyLater)
	mux.HandleFunc("POST /messaging/reply-later/{marker_id}/resolve", s.serveResolveReplyLater)
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
	Reactions   []reactionWire    `json:"reactions"`
	ReplyTo     *string           `json:"reply_to"`
	ClientNonce string            `json:"client_nonce"`
	CreatedAt   time.Time         `json:"created_at"`
	EditedAt    *time.Time        `json:"edited_at"`
	Deleted     bool              `json:"deleted"`
}

// reactionWire matches the web model's ReactionSummary.
type reactionWire struct {
	Emoji        string            `json:"emoji"`
	Participants []participantWire `json:"participants"`
}

func reactionsToWire(summaries []ReactionSummary) []reactionWire {
	out := make([]reactionWire, len(summaries))
	for i, summary := range summaries {
		out[i] = reactionWire{
			Emoji:        summary.Emoji,
			Participants: participantsToWire(summary.Participants),
		}
	}
	return out
}

// reactionUpdateWire is the reaction_updated payload: the message identity plus
// its complete reaction set. It deliberately omits content, mentions and
// edited_at. An edit can commit while this event is being assembled; a full
// message here could then arrive late with pre-edit content and roll the edit
// back on every live client. Reaction snapshots are absolute, and
// Server.toggleReaction publishes them in commit order for the local Hub.
type reactionUpdateWire struct {
	MessageID string         `json:"message_id"`
	Reactions []reactionWire `json:"reactions"`
}

func reactionUpdateToWire(m Message) reactionUpdateWire {
	return reactionUpdateWire{
		MessageID: m.MessageID,
		Reactions: reactionsToWire(m.Reactions),
	}
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
		Reactions:   reactionsToWire(m.Reactions),
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

// statusWire matches the web model's ParticipantStatus.
type statusWire struct {
	Participant participantWire `json:"participant"`
	Status      string          `json:"status"`
	Note        string          `json:"note"`
	ExpiresAt   *time.Time      `json:"expires_at"`
}

func statusToWire(status ParticipantStatus) statusWire {
	return statusWire{
		Participant: participantToWire(status.Participant),
		Status:      status.Status,
		Note:        status.Note,
		ExpiresAt:   status.ExpiresAt,
	}
}

// replyLaterWire matches the web model's ReplyLaterMarker. RemindAt rides only
// on the owner's copy (合意事項 6: remind_at は本人が時刻まで約束した場合を
// 除き相手へ公開しない — v1 has no such promise field, so it is never shared);
// everyone else sees the fact and the note.
type replyLaterWire struct {
	MarkerID    string          `json:"marker_id"`
	Participant participantWire `json:"participant"`
	Place       placeWire       `json:"place"`
	MessageID   string          `json:"message_id"`
	Note        string          `json:"note"`
	RemindAt    *time.Time      `json:"remind_at,omitempty"`
	Resolved    bool            `json:"resolved"`
}

// replyLaterToWire projects a marker for one viewer, deciding whether the
// private remind_at may appear. Every path that serializes a marker goes
// through here, so the secrecy rule cannot be skipped by a new endpoint.
func replyLaterToWire(marker ReplyLaterMarker, viewer ParticipantRef) replyLaterWire {
	wire := replyLaterWire{
		MarkerID:    marker.MarkerID,
		Participant: participantToWire(marker.Participant),
		Place:       placeToWire(Place{PlaceID: marker.PlaceID, Kind: marker.PlaceKind}),
		MessageID:   marker.MessageID,
		Note:        marker.Note,
		Resolved:    marker.Resolved,
	}
	if marker.Participant == viewer {
		remindAt := marker.RemindAt
		wire.RemindAt = &remindAt
	}
	return wire
}

// publishReplyLaterCreated fans one durable creation out as two per-audience
// payloads: the owner's copy carries remind_at, everyone else's does not.
func (s *Server) publishReplyLaterCreated(ctx context.Context, marker ReplyLaterMarker) {
	if s.Hub == nil {
		return
	}
	owner := marker.Participant
	ownerWire := replyLaterToWire(marker, owner)
	s.Hub.Publish(ctx, Event{
		Type: EventReplyLaterCreated, PlaceID: marker.PlaceID,
		Marker: &ownerWire, OnlyFor: &owner,
	})
	publicWire := replyLaterToWire(marker, ParticipantRef{})
	s.Hub.Publish(ctx, Event{
		Type: EventReplyLaterCreated, PlaceID: marker.PlaceID,
		Marker: &publicWire, ExceptFor: []ParticipantRef{owner},
	})
}

// publishReplyLaterResolved announces a kept promise. Only the identifier
// travels: everyone who saw the marker appear can retire it, and nothing
// private needs re-stating to do so.
func (s *Server) publishReplyLaterResolved(ctx context.Context, marker ReplyLaterMarker) {
	if s.Hub == nil {
		return
	}
	s.Hub.Publish(ctx, Event{
		Type: EventReplyLaterResolved, PlaceID: marker.PlaceID, MarkerID: marker.MarkerID,
	})
}

// notifyWire is the per-recipient「これで呼びました」marker on message_created.
// Only the reason travels: the recipient already has the message, and the
// server owes them an explanation, not a second copy of the content.
type notifyWire struct {
	Reason string `json:"reason"`
}

// notificationLevelWire matches the contract's `defaults` object, which is an
// object rather than a bare string so later preferences (quiet hours 等) can
// join it without breaking the shape.
type notificationLevelWire struct {
	Level string `json:"level"`
}

type notificationPlaceWire struct {
	Place placeWire `json:"place"`
	Level string    `json:"level"`
}

// notificationSettingWire matches the frozen NotificationSetting shape from
// docs/messaging-contracts-draft.md.
type notificationSettingWire struct {
	Owner    participantWire         `json:"owner"`
	Defaults notificationLevelWire   `json:"defaults"`
	PerPlace []notificationPlaceWire `json:"per_place"`
	Keywords []string                `json:"keywords"`
}

func notificationSettingToWire(setting NotificationSetting) notificationSettingWire {
	perPlace := make([]notificationPlaceWire, len(setting.PerPlace))
	for i, entry := range setting.PerPlace {
		perPlace[i] = notificationPlaceWire{
			Place: placeToWire(Place{PlaceID: entry.PlaceID, Kind: entry.PlaceKind}),
			Level: entry.Level,
		}
	}
	keywords := setting.Keywords
	if keywords == nil {
		keywords = []string{}
	}
	return notificationSettingWire{
		Owner:    participantToWire(setting.Owner),
		Defaults: notificationLevelWire{Level: setting.Default()},
		PerPlace: perPlace,
		Keywords: keywords,
	}
}

// publishMessageCreated fans one committed message out as per-recipient
// payloads: the people the server decided to interrupt get the message with
// `notify`, everyone else gets the same message without it. The evaluation
// happens here — one place, shared by REST, WS, and the agent's control socket
// — so no transport can deliver a message that skipped the receiver's own
// rules. Notification is best-effort on top of durable truth: an evaluation
// failure still delivers the message, silently.
func publishMessageCreated(ctx context.Context, store *Store, hub *Hub, place Place, msg Message) {
	if hub == nil {
		return
	}
	wire := messageToWire(place, msg)
	decisions, err := store.NotificationDecisionsFor(ctx, place, msg)
	if err != nil {
		decisions = nil
	}
	notified := make([]ParticipantRef, 0, len(decisions))
	for _, decision := range decisions {
		recipient := decision.Participant
		notify := notifyWire{Reason: decision.Reason}
		hub.Publish(ctx, Event{
			Type: EventMessageCreated, PlaceID: place.PlaceID,
			Message: &wire, Notify: &notify, OnlyFor: &recipient,
		})
		notified = append(notified, recipient)
	}
	hub.Publish(ctx, Event{
		Type: EventMessageCreated, PlaceID: place.PlaceID,
		Message: &wire, ExceptFor: notified,
	})
}

// publishStatus fans a self-declared status out to everyone who may see the
// participant. It is volatile like typing: the current value is in bootstrap,
// so a missed frame costs nothing.
func (s *Server) publishStatus(ctx context.Context, status ParticipantStatus) {
	if s.Hub == nil {
		return
	}
	subject := status.Participant
	wire := statusToWire(status)
	s.Hub.Publish(ctx, Event{Type: EventStatusUpdated, Subject: &subject, Status: &wire})
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

	// Self-declared attention state: the current status of everyone visible
	// (expired ones are already filtered out) and the open reply-later
	// promises of every visible place. Statuses change over the volatile
	// status_updated event, which never replays — bootstrap is where a fresh
	// client learns the current value.
	statuses, err := s.Store.StatusesVisibleTo(ctx, viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	statusWires := make([]statusWire, len(statuses))
	for i, status := range statuses {
		statusWires[i] = statusToWire(status)
	}
	markers, err := s.Store.ReplyLaterMarkersFor(ctx, viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	markerWires := make([]replyLaterWire, len(markers))
	for i, marker := range markers {
		markerWires[i] = replyLaterToWire(marker, viewer)
	}
	// The viewer's own notification setting: the sidebar dims muted places from
	// the first paint, without a second round trip that could disagree.
	setting, err := s.Store.NotificationSettingFor(ctx, viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, struct {
		Self                participantWire         `json:"self"`
		Workspaces          []workspaceWire         `json:"workspaces"`
		Channels            []channelWire           `json:"channels"`
		DMs                 []dmWire                `json:"dms"`
		Members             []memberWire            `json:"members"`
		Statuses            []statusWire            `json:"statuses"`
		ReadMarkers         []readMarkerWire        `json:"read_markers"`
		UnreadSummaries     []unreadSummaryWire     `json:"unread_summaries"`
		ReplyLaterMarkers   []replyLaterWire        `json:"reply_later_markers"`
		NotificationSetting notificationSettingWire `json:"notification_setting"`
	}{
		Self:                participantToWire(viewer),
		Workspaces:          workspaceWires,
		Channels:            channels,
		DMs:                 dms,
		Members:             members,
		Statuses:            statusWires,
		ReadMarkers:         readMarkers,
		UnreadSummaries:     unread,
		ReplyLaterMarkers:   markerWires,
		NotificationSetting: notificationSettingToWire(setting),
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
	if len(req.Topic) > maxTopicBytes {
		writeError(w, http.StatusBadRequest, "invalid_topic")
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
	wire := channelToWire(place)
	s.Hub.Publish(r.Context(), Event{Type: EventPlaceCreated, PlaceID: place.PlaceID, Channel: &wire})
	writeJSON(w, http.StatusCreated, wire)
}

// serveUpdatePlace edits a channel's mutable fields (v0: topic only).
func (s *Server) serveUpdatePlace(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	var req struct {
		Topic string `json:"topic"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if len(req.Topic) > maxTopicBytes {
		writeError(w, http.StatusBadRequest, "invalid_topic")
		return
	}
	var place Place
	done, err := s.mutate(w, r, claims, func() error {
		var opErr error
		place, opErr = s.Store.UpdateChannelTopic(r.Context(), r.PathValue("place_id"), req.Topic, viewer)
		return opErr
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	wire := channelToWire(place)
	s.Hub.Publish(r.Context(), Event{Type: EventPlaceUpdated, PlaceID: place.PlaceID, Channel: &wire})
	writeJSON(w, http.StatusOK, wire)
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
	var (
		place   Place
		created bool
	)
	done, err := s.mutate(w, r, claims, func() error {
		var opErr error
		place, created, opErr = s.Store.EnsureDM(r.Context(), viewer, other)
		return opErr
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	wire := dmWire{
		DMID: place.PlaceID, Kind: place.Kind,
		Participants: []participantWire{participantToWire(viewer), participantToWire(other)},
	}
	if created {
		s.Hub.Publish(r.Context(), Event{Type: EventPlaceCreated, PlaceID: place.PlaceID, DM: &wire})
	}
	writeJSON(w, http.StatusOK, wire)
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
	wire := dmWire{
		DMID: place.PlaceID, Kind: place.Kind,
		Participants: append([]participantWire{participantToWire(viewer)}, req.Participants...),
	}
	s.Hub.Publish(r.Context(), Event{Type: EventPlaceCreated, PlaceID: place.PlaceID, DM: &wire})
	writeJSON(w, http.StatusCreated, wire)
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
		publishMessageCreated(r.Context(), s.Store, s.Hub, place, msg)
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

// serveToggleReaction toggles the viewer's emoji on a message. The same store
// toggle backs the agent tool path (AX: UIだけにある操作を作らない).
func (s *Server) serveToggleReaction(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	placeID := r.PathValue("place_id")
	var req struct {
		Emoji       string `json:"emoji"`
		ClientNonce string `json:"client_nonce"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if validateReactionEmoji(req.Emoji) != nil {
		writeError(w, http.StatusBadRequest, "invalid_emoji")
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
		reacted bool
	)
	done, err := s.mutate(w, r, claims, func() error {
		var opErr error
		msg, reacted, opErr = s.toggleReaction(
			r.Context(), placeID, r.PathValue("message_id"), viewer, req.Emoji, req.ClientNonce)
		return opErr
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Message messageWire `json:"message"`
		Reacted bool        `json:"reacted"`
	}{Message: messageToWire(place, msg), Reacted: reacted})
}

// serveSetStatus replaces the viewer's own status. There is no route for
// setting anyone else's: the participant is the authenticated session, never a
// request field (自己申告のattention — the platform does not observe or
// announce attention on a person's behalf).
func (s *Server) serveSetStatus(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	var req struct {
		Status string `json:"status"`
		Note   string `json:"note"`
		// Absent or null holds the status until it is replaced.
		ExpiresAt *time.Time `json:"expires_at"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	switch req.Status {
	case StatusAvailable, StatusBusy, StatusAway:
	default:
		writeError(w, http.StatusBadRequest, "invalid_status")
		return
	}
	if utf8.RuneCountInString(req.Note) > MaxStatusNoteChars {
		writeError(w, http.StatusBadRequest, "invalid_note")
		return
	}
	var status ParticipantStatus
	done, err := s.mutate(w, r, claims, func() error {
		var opErr error
		status, opErr = s.Store.SetStatus(r.Context(), viewer, req.Status, req.Note, req.ExpiresAt)
		return opErr
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	s.publishStatus(r.Context(), status)
	writeJSON(w, http.StatusOK, statusToWire(status))
}

// serveNotificationSetting returns the viewer's own notification preference.
// There is no route for reading anyone else's: what interrupts a person is
// theirs to know (契約ドラフト: owner が本人、変更も本人のみ).
func (s *Server) serveNotificationSetting(w http.ResponseWriter, r *http.Request) {
	viewer, _, ok := s.viewer(w, r)
	if !ok {
		return
	}
	setting, err := s.Store.NotificationSettingFor(r.Context(), viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, notificationSettingToWire(setting))
}

// serveSetNotificationSetting replaces the viewer's whole setting. PUT is a
// replacement on purpose: the client always holds the full current setting
// (bootstrap gives it), so a partial merge would only add a way for two tabs to
// disagree about what was removed.
func (s *Server) serveSetNotificationSetting(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	var req struct {
		Defaults notificationLevelWire `json:"defaults"`
		PerPlace []struct {
			Place placeWire `json:"place"`
			Level string    `json:"level"`
		} `json:"per_place"`
		Keywords []string `json:"keywords"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Defaults.Level != "" && ValidateNotifyLevel(req.Defaults.Level) != nil {
		writeError(w, http.StatusBadRequest, "invalid_level")
		return
	}
	perPlace := make([]PlaceNotifyLevel, 0, len(req.PerPlace))
	for _, entry := range req.PerPlace {
		if ValidateNotifyLevel(entry.Level) != nil {
			writeError(w, http.StatusBadRequest, "invalid_level")
			return
		}
		placeID := entry.Place.ChannelID
		if placeID == "" {
			placeID = entry.Place.DMID
		}
		if placeID == "" {
			writeError(w, http.StatusBadRequest, "invalid_place")
			return
		}
		perPlace = append(perPlace, PlaceNotifyLevel{PlaceID: placeID, Level: entry.Level})
	}
	var setting NotificationSetting
	done, err := s.mutate(w, r, claims, func() error {
		var opErr error
		setting, opErr = s.Store.SetNotificationSetting(
			r.Context(), viewer, req.Defaults.Level, perPlace, req.Keywords)
		return opErr
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, notificationSettingToWire(setting))
}

// serveCreateReplyLater places the viewer's own「後で返信します」marker on a
// message they can see. Repeating the tap returns the existing open marker.
func (s *Server) serveCreateReplyLater(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	placeID := r.PathValue("place_id")
	var req struct {
		Note string `json:"note"`
		// The owner's own reminder time. It is stored, echoed back to the
		// owner, and withheld from every other participant's wire.
		RemindAt *time.Time `json:"remind_at"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.RemindAt == nil || req.RemindAt.IsZero() {
		writeError(w, http.StatusBadRequest, "invalid_remind_at")
		return
	}
	if utf8.RuneCountInString(req.Note) > MaxReplyLaterNoteChars {
		writeError(w, http.StatusBadRequest, "invalid_note")
		return
	}
	var (
		marker  ReplyLaterMarker
		created bool
	)
	done, err := s.mutate(w, r, claims, func() error {
		var opErr error
		marker, created, opErr = s.Store.CreateReplyLater(
			r.Context(), placeID, r.PathValue("message_id"), viewer, req.Note, *req.RemindAt)
		return opErr
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if created {
		s.publishReplyLaterCreated(r.Context(), marker)
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
	}
	writeJSON(w, status, struct {
		Marker  replyLaterWire `json:"marker"`
		Created bool           `json:"created"`
	}{Marker: replyLaterToWire(marker, viewer), Created: created})
}

// serveResolveReplyLater marks the viewer's own promise as kept. Someone
// else's marker is reported as missing rather than forbidden, so the route
// never confirms marker identifiers across the ownership boundary.
func (s *Server) serveResolveReplyLater(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	var marker ReplyLaterMarker
	done, err := s.mutate(w, r, claims, func() error {
		var opErr error
		marker, opErr = s.Store.ResolveReplyLater(r.Context(), r.PathValue("marker_id"), viewer)
		return opErr
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	s.publishReplyLaterResolved(r.Context(), marker)
	writeJSON(w, http.StatusOK, struct {
		Marker replyLaterWire `json:"marker"`
	}{Marker: replyLaterToWire(marker, viewer)})
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
		errors.Is(err, ErrWorkspaceNotFound), errors.Is(err, ErrParticipantNotFound),
		errors.Is(err, ErrMarkerNotFound):
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
	case errors.Is(err, ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "idempotency_conflict")
	case errors.Is(err, ErrSeqBeyondLatest):
		writeError(w, http.StatusBadRequest, "seq_beyond_latest")
	case errors.Is(err, ErrNotAChannel):
		writeError(w, http.StatusBadRequest, "not_a_channel")
	case errors.Is(err, ErrInvalidNotificationSetting):
		writeError(w, http.StatusBadRequest, "invalid_notification_setting")
	default:
		writeError(w, http.StatusInternalServerError, "internal")
	}
}
