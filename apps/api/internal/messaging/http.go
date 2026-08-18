package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
)

// maxRequestBytes bounds any /messaging JSON request body. One legal content
// byte can occupy six wire bytes when JSON escapes a control character
// ("\\u0001"), so the cap covers that worst case plus envelope headroom.
const maxRequestBytes = 6*MaxContentBytes + 64*1024

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
	// Calls is nil when this deployment has no configured media transport.
	Calls *CallService
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
	mux.HandleFunc("GET /messaging/search", s.serveSearch)
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
	mux.HandleFunc("GET /messaging/profile", s.serveProfile)
	mux.HandleFunc("PUT /messaging/profile", s.serveSetProfile)
	mux.HandleFunc("PUT /messaging/status", s.serveSetStatus)
	mux.HandleFunc("GET /messaging/notification-settings", s.serveNotificationSetting)
	mux.HandleFunc("PUT /messaging/notification-settings", s.serveSetNotificationSetting)
	mux.HandleFunc("POST /messaging/places/{place_id}/messages/{message_id}/reply-later", s.serveCreateReplyLater)
	mux.HandleFunc("POST /messaging/reply-later/{marker_id}/resolve", s.serveResolveReplyLater)
	mux.HandleFunc("POST /messaging/places/{place_id}/attachments", s.serveUploadAttachment)
	mux.HandleFunc("GET /messaging/attachments/{attachment_id}", s.serveAttachment)
	mux.HandleFunc("PATCH /messaging/attachments/{attachment_id}", s.serveUpdateAttachment)
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
	Attachments []attachmentWire  `json:"attachments"`
	ReplyTo     *string           `json:"reply_to"`
	ClientNonce string            `json:"client_nonce"`
	CreatedAt   time.Time         `json:"created_at"`
	EditedAt    *time.Time        `json:"edited_at"`
	Deleted     bool              `json:"deleted"`
}

// searchResultWire deliberately excludes full message content. A result has
// enough identity to navigate to the durable message plus a bounded snippet.
type searchResultWire struct {
	MessageID string          `json:"message_id"`
	Place     placeWire       `json:"place"`
	Seq       int64           `json:"seq"`
	Author    participantWire `json:"author"`
	Snippet   string          `json:"snippet"`
	CreatedAt time.Time       `json:"created_at"`
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

// messageReceiptWire is the shared mutation acknowledgement for browser REST
// and the agent local-control adapter. It excludes message content so a
// maximum-size committed write always fits the agent's bounded response. The
// durable message itself arrives through timeline/event projection.
type messageReceiptWire struct {
	ClientNonce string `json:"client_nonce"`
	MessageID   string `json:"message_id"`
	Seq         int64  `json:"seq"`
	Created     bool   `json:"created"`
}

func messageReceiptToWire(message Message, created bool) messageReceiptWire {
	return messageReceiptWire{
		ClientNonce: message.ClientNonce,
		MessageID:   message.MessageID,
		Seq:         message.Seq,
		Created:     created,
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
		Attachments: attachmentsToWire(m.Attachments),
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
	Voice       bool   `json:"voice"`
}

func channelToWire(p Place) channelWire {
	return channelWire{
		ChannelID:   p.PlaceID,
		WorkspaceID: p.WorkspaceID,
		Name:        p.Name,
		Topic:       p.Topic,
		Visibility:  p.Visibility,
		Voice:       p.Voice,
	}
}

type dmWire struct {
	DMID         string            `json:"dm_id"`
	Kind         string            `json:"kind"`
	Participants []participantWire `json:"participants"`
}

// memberWire is the presentation of one participant. The tagline rides with
// every member list rather than needing a second round trip: it is what the
// member list, the profile card and the composer all show next to the name.
type memberWire struct {
	Participant participantWire `json:"participant"`
	DisplayName string          `json:"display_name"`
	Tagline     string          `json:"tagline"`
	Revision    int64           `json:"revision"`
}

func memberToWire(profile MemberProfile) memberWire {
	return memberWire{
		Participant: participantToWire(profile.Participant),
		DisplayName: profile.ProjectedDisplayName(),
		Tagline:     profile.Tagline,
		Revision:    profile.Revision,
	}
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
func (s *Server) publishReplyLaterCreated(ctx context.Context, store *ScopedStore, marker ReplyLaterMarker) {
	if s.Hub == nil {
		return
	}
	owner := marker.Participant
	ownerWire := replyLaterToWire(marker, owner)
	ownerEvent := Event{
		Type: EventReplyLaterCreated, PlaceID: marker.PlaceID,
		Marker: &ownerWire, OnlyFor: &owner,
	}
	publicWire := replyLaterToWire(marker, ParticipantRef{})
	publicEvent := Event{
		Type: EventReplyLaterCreated, PlaceID: marker.PlaceID,
		Marker: &publicWire, ExceptFor: []ParticipantRef{owner},
	}
	_ = s.Hub.PublishVariantsScoped(ctx, store, []Event{ownerEvent, publicEvent})
}

// publishReplyLaterResolved announces a kept promise. Only the identifier
// travels: everyone who saw the marker appear can retire it, and nothing
// private needs re-stating to do so.
func (s *Server) publishReplyLaterResolved(ctx context.Context, store *ScopedStore, marker ReplyLaterMarker) {
	if s.Hub == nil {
		return
	}
	_ = s.Hub.PublishScoped(ctx, store, Event{
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
// and intent issuance happen inside AppendMessage — one place shared by REST,
// WS, and the agent's control socket — so no transport can send a message that
// skipped the receiver's own rules. Delivery is best-effort on top of that
// durable truth, so a delivery read failure still fans out the message without
// claiming that anyone was called.
func publishMessageCreated(ctx context.Context, store *ScopedStore, hub *Hub, place Place, msg Message) {
	if hub == nil {
		return
	}
	wire := messageToWire(place, msg)
	decisions, err := store.NotificationIntentsForMessage(ctx, msg.MessageID)
	if err != nil {
		decisions = nil
	}
	notified := make([]ParticipantRef, 0, len(decisions))
	events := make([]Event, 0, len(decisions)+1)
	for _, decision := range decisions {
		recipient := decision.Participant
		notify := notifyWire{Reason: decision.Reason}
		events = append(events, Event{
			Type: EventMessageCreated, PlaceID: place.PlaceID,
			Message: &wire, Notify: &notify, OnlyFor: &recipient,
		})
		notified = append(notified, recipient)
	}
	events = append(events, Event{
		Type: EventMessageCreated, PlaceID: place.PlaceID,
		Message: &wire, ExceptFor: notified,
	})
	_ = hub.PublishVariantsScoped(ctx, store, events)
}

// publishStatus fans a self-declared status out to everyone who may see the
// participant. It is volatile like typing: the current value is in bootstrap,
// so a missed frame costs nothing.
func (s *Server) publishStatus(ctx context.Context, store *ScopedStore, status ParticipantStatus) {
	if s.Hub == nil {
		return
	}
	subject := status.Participant
	wire := statusToWire(status)
	_ = s.Hub.PublishScoped(ctx, store, Event{Type: EventStatusUpdated, Subject: &subject, Status: &wire})
}

// publishProfile fans a replaced profile out to every Workspace where its
// participant is presently visible. Unlike status it is durable: bootstrap
// already carries the current value, so a missed frame is repaired by
// reconnecting rather than by a replay.
func (s *Server) publishProfile(ctx context.Context, scopes []Scope, profile MemberProfile) {
	if s.Hub == nil {
		return
	}
	subject := profile.Participant
	wire := memberToWire(profile)
	for _, scope := range scopes {
		_ = s.Hub.PublishSystemScoped(ctx, scope, Event{Type: EventProfileUpdated, Subject: &subject, Profile: &wire})
	}
}

// setProfile is the scoped transport adapter for Store.setProfile. Receivers
// resolve any delivery reordering through the profile revision.
func (s *Server) setProfile(ctx context.Context, store *ScopedStore, displayName, tagline *string) (MemberProfile, error) {
	return store.SetProfile(ctx, displayName, tagline, s.publishProfile)
}

// SetHumanProfile lets the account settings surface use the exact same
// profile write boundary as Messaging. Session authorization belongs to that
// outer transport; this method owns the durable name/revision write and the
// post-commit fan-out to every Workspace where the Human is visible.
func (s *Server) SetHumanProfile(ctx context.Context, humanID string, displayName string) (MemberProfile, error) {
	return s.Store.setProfile(ctx, ParticipantRef{Kind: KindHuman, ID: humanID}, &displayName, nil, nil, s.publishProfile)
}

type unreadSummaryWire struct {
	Place        placeWire `json:"place"`
	LatestSeq    int64     `json:"latest_seq"`
	UnreadCount  int64     `json:"unread_count"`
	MentionCount int64     `json:"mention_count"`
}

// --- authentication ---

type requestScopeContextKey struct{}

func scopedStoreForRequest(r *http.Request) *ScopedStore {
	store, _ := r.Context().Value(requestScopeContextKey{}).(*ScopedStore)
	return store
}

func exactQueryValue(r *http.Request, key string) (string, bool) {
	values, present := r.URL.Query()[key]
	returnValue := ""
	if present && len(values) == 1 {
		returnValue = values[0]
	}
	return returnValue, present && len(values) == 1 && returnValue != ""
}

// viewer authenticates the request and binds its exact app scope. The
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
	workspaceID, workspaceOK := exactQueryValue(r, "workspace_id")
	installationID, installationOK := exactQueryValue(r, "installation_id")
	authorityEpoch, epochOK := exactAuthorityEpochQuery(r)
	if !workspaceOK || !installationOK || !epochOK || s.Store == nil {
		writeError(w, http.StatusBadRequest, "invalid_scope")
		return ParticipantRef{}, none, false
	}
	scoped, err := s.Store.Scoped(Scope{
		WorkspaceID: workspaceID, InstallationID: installationID,
		AuthorityEpoch: authorityEpoch, Actor: viewer,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_scope")
		return ParticipantRef{}, none, false
	}
	*r = *r.WithContext(context.WithValue(r.Context(), requestScopeContextKey{}, scoped))
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
	viewer, _, ok := s.viewer(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	store := scopedStoreForRequest(r)
	summaries, err := store.UnreadSummaries(ctx)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	workspace, err := store.Workspace(ctx)
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
			memberSet[key] = memberToWire(p)
			memberOrder = append(memberOrder, key)
		}
	}

	workspaceWires := []workspaceWire{{WorkspaceID: workspace.WorkspaceID, Name: workspace.Name}}
	profiles, err := store.WorkspaceMembers(ctx)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	addMembers(profiles)

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
		profiles, err := store.ActiveMembers(ctx, sum.Place.PlaceID)
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
	statuses, err := store.StatusesVisibleTo(ctx)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	statusWires := make([]statusWire, len(statuses))
	for i, status := range statuses {
		statusWires[i] = statusToWire(status)
	}
	markers, err := store.ReplyLaterMarkersFor(ctx)
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
	setting, err := store.NotificationSettingFor(ctx)
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
	_, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	var req struct {
		WorkspaceID string `json:"workspace_id"`
		Name        string `json:"name"`
		Topic       string `json:"topic"`
		Voice       bool   `json:"voice"`
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
		if req.WorkspaceID != "" && req.WorkspaceID != scopedStoreForRequest(r).Scope.WorkspaceID {
			return ErrInvalidScope
		}
		place, opErr = scopedStoreForRequest(r).CreateChannel(r.Context(), req.Name, req.Topic, req.Voice)
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
	_ = s.Hub.PublishScoped(r.Context(), scopedStoreForRequest(r), Event{Type: EventPlaceCreated, PlaceID: place.PlaceID, Channel: &wire})
	writeJSON(w, http.StatusCreated, wire)
}

// serveUpdatePlace edits a channel's mutable fields (v0: topic only).
func (s *Server) serveUpdatePlace(w http.ResponseWriter, r *http.Request) {
	_, claims, ok := s.viewer(w, r)
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
		place, opErr = scopedStoreForRequest(r).UpdateChannelTopic(r.Context(), r.PathValue("place_id"), req.Topic)
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
	_ = s.Hub.PublishScoped(r.Context(), scopedStoreForRequest(r), Event{Type: EventPlaceUpdated, PlaceID: place.PlaceID, Channel: &wire})
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
		place, created, opErr = scopedStoreForRequest(r).EnsureDM(r.Context(), other)
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
		_ = s.Hub.PublishScoped(r.Context(), scopedStoreForRequest(r), Event{Type: EventPlaceCreated, PlaceID: place.PlaceID, DM: &wire})
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
		place, opErr = scopedStoreForRequest(r).CreateGroupDM(r.Context(), others)
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
	_ = s.Hub.PublishScoped(r.Context(), scopedStoreForRequest(r), Event{Type: EventPlaceCreated, PlaceID: place.PlaceID, DM: &wire})
	writeJSON(w, http.StatusCreated, wire)
}

func (s *Server) servePlace(w http.ResponseWriter, r *http.Request) {
	_, _, ok := s.viewer(w, r)
	if !ok {
		return
	}
	store := scopedStoreForRequest(r)
	place, err := store.PlaceFor(r.Context(), r.PathValue("place_id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	profiles, err := store.ActiveMembers(r.Context(), place.PlaceID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	members := make([]memberWire, len(profiles))
	for i, p := range profiles {
		members[i] = memberToWire(p)
	}
	writeJSON(w, http.StatusOK, struct {
		Place     placeWire    `json:"place"`
		LatestSeq int64        `json:"latest_seq"`
		Members   []memberWire `json:"members"`
	}{Place: placeToWire(place), LatestSeq: place.LastSeq, Members: members})
}

func (s *Server) serveHistory(w http.ResponseWriter, r *http.Request) {
	_, _, ok := s.viewer(w, r)
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
	store := scopedStoreForRequest(r)
	place, err := store.PlaceFor(r.Context(), placeID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	messages, err := store.History(r.Context(), placeID, opt)
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

func (s *Server) serveSearch(w http.ResponseWriter, r *http.Request) {
	_, _, ok := s.viewer(w, r)
	if !ok {
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" || len(query) > MaxSearchQueryBytes {
		writeError(w, http.StatusBadRequest, "invalid_query")
		return
	}
	options := SearchOptions{PlaceID: r.URL.Query().Get("place_id")}
	if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
		limit, err := strconv.Atoi(rawLimit)
		if err != nil || limit <= 0 {
			writeError(w, http.StatusBadRequest, "invalid_limit")
			return
		}
		options.Limit = limit
	}
	results, err := scopedStoreForRequest(r).SearchMessages(r.Context(), query, options)
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
	}{Results: wires})
}

func (s *Server) serveSend(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	placeID := r.PathValue("place_id")
	var req struct {
		Content     string   `json:"content"`
		Urgency     string   `json:"urgency"`
		ReplyTo     string   `json:"reply_to"`
		ClientNonce string   `json:"client_nonce"`
		Attachments []string `json:"attachments"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if code := validateSendRequest(req.Content, req.Urgency, req.ClientNonce, req.Attachments); code != "" {
		writeError(w, http.StatusBadRequest, code)
		return
	}
	store := scopedStoreForRequest(r)
	place, err := store.PlaceFor(r.Context(), placeID)
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
		msg, created, opErr = store.AppendMessage(r.Context(), AppendInput{
			PlaceID: placeID, Author: viewer, Content: req.Content,
			Urgency: req.Urgency, ReplyTo: req.ReplyTo, ClientNonce: req.ClientNonce,
			AttachmentIDs: req.Attachments,
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
		publishMessageCreated(r.Context(), store, s.Hub, place, msg)
	}
	writeJSON(w, status, messageReceiptToWire(msg, created))
}

// validateSendRequest is the transport-shape check shared by the browser and
// PA send routes. It returns the error code, or "" when the shape is valid.
// Attachment-only messages are legitimate; empty and attachment-less is not.
func validateSendRequest(content, urgency, clientNonce string, attachments []string) string {
	switch urgency {
	case "", UrgencyUrgent, UrgencyNormal, UrgencyFYI:
	default:
		return "invalid_urgency"
	}
	if (content == "" && len(attachments) == 0) || !messageContentFitsStorage(content) {
		return "invalid_content"
	}
	if len(attachments) > MaxAttachmentsPerMessage {
		return "too_many_attachments"
	}
	for _, id := range attachments {
		if !validAttachmentID(id) {
			return "invalid_attachment"
		}
	}
	if clientNonce == "" || len(clientNonce) > 128 {
		return "invalid_client_nonce"
	}
	return ""
}

func (s *Server) serveEdit(w http.ResponseWriter, r *http.Request) {
	_, claims, ok := s.viewer(w, r)
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
	if req.Content == "" || !messageContentFitsStorage(req.Content) {
		writeError(w, http.StatusBadRequest, "invalid_content")
		return
	}
	store := scopedStoreForRequest(r)
	place, err := store.PlaceFor(r.Context(), placeID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	var msg Message
	done, err := s.mutate(w, r, claims, func() error {
		var opErr error
		msg, opErr = store.EditMessage(r.Context(), placeID, r.PathValue("message_id"), req.Content)
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
	_ = s.Hub.PublishScoped(r.Context(), store, Event{Type: EventMessageEdited, PlaceID: placeID, Message: &wire})
	writeJSON(w, http.StatusOK, struct {
		Message messageWire `json:"message"`
	}{Message: wire})
}

func (s *Server) serveDelete(w http.ResponseWriter, r *http.Request) {
	_, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	placeID := r.PathValue("place_id")
	store := scopedStoreForRequest(r)
	place, err := store.PlaceFor(r.Context(), placeID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	var msg Message
	done, err := s.mutate(w, r, claims, func() error {
		var opErr error
		msg, opErr = store.DeleteMessage(r.Context(), placeID, r.PathValue("message_id"))
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
	_ = s.Hub.PublishScoped(r.Context(), store, Event{Type: EventMessageDeleted, PlaceID: placeID, Message: &wire})
	w.WriteHeader(http.StatusNoContent)
}

// serveToggleReaction toggles the viewer's emoji on a message. The same store
// toggle backs the agent tool path (AX: UIだけにある操作を作らない).
func (s *Server) serveToggleReaction(w http.ResponseWriter, r *http.Request) {
	_, claims, ok := s.viewer(w, r)
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
	store := scopedStoreForRequest(r)
	place, err := store.PlaceFor(r.Context(), placeID)
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
		msg, reacted, opErr = s.toggleScopedReaction(
			r.Context(), store, placeID, r.PathValue("message_id"), req.Emoji, req.ClientNonce)
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

// serveProfile returns the viewer's own canonical profile. Everyone else's is
// already in the member list, so this route exists for the settings screen
// rather than for looking people up.
func (s *Server) serveProfile(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.viewer(w, r); !ok {
		return
	}
	profile, err := scopedStoreForRequest(r).Profile(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, memberToWire(profile))
}

// serveSetProfile replaces the viewer's own profile. Like status there is no
// route for setting anyone else's: the participant is the authenticated
// session, never a request field. An absent JSON field is preserved, so a
// client that only edits the tagline cannot clear the display name.
func (s *Server) serveSetProfile(w http.ResponseWriter, r *http.Request) {
	_, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	var req struct {
		DisplayName *string `json:"display_name"`
		Tagline     *string `json:"tagline"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	var profile MemberProfile
	done, err := s.mutate(w, r, claims, func() error {
		var opErr error
		profile, opErr = s.setProfile(r.Context(), scopedStoreForRequest(r), req.DisplayName, req.Tagline)
		return opErr
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, memberToWire(profile))
}

// serveSetStatus replaces the viewer's own status. There is no route for
// setting anyone else's: the participant is the authenticated session, never a
// request field (自己申告のattention — the platform does not observe or
// announce attention on a person's behalf).
func (s *Server) serveSetStatus(w http.ResponseWriter, r *http.Request) {
	_, claims, ok := s.viewer(w, r)
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
		status, opErr = scopedStoreForRequest(r).SetStatus(r.Context(), req.Status, req.Note, req.ExpiresAt)
		return opErr
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	s.publishStatus(r.Context(), scopedStoreForRequest(r), status)
	writeJSON(w, http.StatusOK, statusToWire(status))
}

// serveNotificationSetting returns the viewer's own notification preference.
// There is no route for reading anyone else's: what interrupts a person is
// theirs to know (契約ドラフト: owner が本人、変更も本人のみ).
func (s *Server) serveNotificationSetting(w http.ResponseWriter, r *http.Request) {
	_, _, ok := s.viewer(w, r)
	if !ok {
		return
	}
	setting, err := scopedStoreForRequest(r).NotificationSettingFor(r.Context())
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
	_, claims, ok := s.viewer(w, r)
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
		setting, opErr = scopedStoreForRequest(r).SetNotificationSetting(
			r.Context(), req.Defaults.Level, perPlace, req.Keywords)
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
		marker, created, opErr = scopedStoreForRequest(r).CreateReplyLater(
			r.Context(), placeID, r.PathValue("message_id"), req.Note, *req.RemindAt)
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
		s.publishReplyLaterCreated(r.Context(), scopedStoreForRequest(r), marker)
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
		marker, opErr = scopedStoreForRequest(r).ResolveReplyLater(r.Context(), r.PathValue("marker_id"))
		return opErr
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	s.publishReplyLaterResolved(r.Context(), scopedStoreForRequest(r), marker)
	writeJSON(w, http.StatusOK, struct {
		Marker replyLaterWire `json:"marker"`
	}{Marker: replyLaterToWire(marker, viewer)})
}

func (s *Server) serveReadThrough(w http.ResponseWriter, r *http.Request) {
	_, claims, ok := s.viewer(w, r)
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
		return scopedStoreForRequest(r).ReadThrough(r.Context(), r.PathValue("place_id"), req.Seq)
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
	// Decoder.More only answers whether another value is available *inside* an
	// array or object. A second Decode is the only strict top-level check: a
	// JSON request has exactly one value followed by EOF.
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
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
	case errors.Is(err, ErrAttachmentNotFound):
		writeError(w, http.StatusNotFound, "not_found")
	case errors.Is(err, ErrAttachmentTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, "attachment_too_large")
	case errors.Is(err, ErrAttachmentSizeMismatch):
		writeError(w, http.StatusBadRequest, "attachment_size_mismatch")
	case errors.Is(err, ErrAttachmentQuotaExceeded):
		writeError(w, http.StatusInsufficientStorage, "attachment_quota_exceeded")
	case errors.Is(err, ErrAttachmentDraftLimit):
		writeError(w, http.StatusConflict, "attachment_draft_limit")
	case errors.Is(err, ErrAttachmentUploadConflict):
		writeError(w, http.StatusConflict, "attachment_upload_conflict")
	case errors.Is(err, ErrAttachmentUploadExpired):
		writeError(w, http.StatusGone, "attachment_upload_expired")
	case errors.Is(err, ErrAttachmentUploadRetired):
		writeError(w, http.StatusGone, "attachment_upload_retired")
	case errors.Is(err, ErrAttachmentAlreadySent):
		writeError(w, http.StatusConflict, "attachment_already_sent")
	case errors.Is(err, ErrTooManyAttachments):
		writeError(w, http.StatusBadRequest, "too_many_attachments")
	case errors.Is(err, ErrAttachmentsUnavailable):
		writeError(w, http.StatusServiceUnavailable, "attachments_unavailable")
	case errors.Is(err, ErrSeqBeyondLatest):
		writeError(w, http.StatusBadRequest, "seq_beyond_latest")
	case errors.Is(err, ErrNotAChannel):
		writeError(w, http.StatusBadRequest, "not_a_channel")
	case errors.Is(err, ErrInvalidNotificationSetting):
		writeError(w, http.StatusBadRequest, "invalid_notification_setting")
	case errors.Is(err, ErrInvalidDisplayName):
		writeError(w, http.StatusBadRequest, "invalid_display_name")
	case errors.Is(err, ErrInvalidTagline):
		writeError(w, http.StatusBadRequest, "invalid_tagline")
	case errors.Is(err, ErrInvalidScope):
		writeError(w, http.StatusBadRequest, "invalid_scope")
	case errors.Is(err, applicationapps.ErrInstallationNotFound):
		writeError(w, http.StatusNotFound, "installation_not_found")
	case errors.Is(err, applicationapps.ErrAppDisabled):
		writeError(w, http.StatusForbidden, "app_disabled")
	default:
		writeError(w, http.StatusInternalServerError, "internal")
	}
}
