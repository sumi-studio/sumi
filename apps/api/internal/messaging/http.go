package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"path"
	"strconv"
	"strings"
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
	// Attachments stores uploaded bytes. Nil disables the attachment routes
	// (503): the deployment has no configured storage root, and failing closed
	// is better than accepting uploads nothing can serve.
	Attachments AttachmentBlobs
	// Hub, when set, receives durable events from REST mutations so live
	// WebSocket subscribers see messages regardless of which transport
	// committed them. Nil is fine: durable truth lives in the store.
	Hub *Hub
	// Push, when set, holds this deployment's VAPID keys and sends Web Push
	// to a Human's registered browsers. Nil keeps the routes mounted and
	// failing closed (503): a subscription nothing can send to is worse than
	// an honest "この deployment に push はまだ無い".
	Push *PushDispatcher
	// Calls, when set, is the RTC surface (ADR 0012). Nil means the deployment
	// configured no media transport, and the call routes are not mounted.
	Calls *CallService
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
	mux.HandleFunc("POST /messaging/places/{place_id}/duplicate", s.serveDuplicatePlace)
	mux.HandleFunc("GET /messaging/places/{place_id}/threads", s.serveThreads)
	mux.HandleFunc("POST /messaging/places/{place_id}/threads", s.serveCreateThread)
	mux.HandleFunc("GET /messaging/places/{place_id}/messages", s.serveHistory)
	mux.HandleFunc("GET /messaging/search", s.serveSearch)
	mux.HandleFunc("POST /messaging/places/{place_id}/messages", s.serveSend)
	mux.HandleFunc("PATCH /messaging/places/{place_id}/messages/{message_id}", s.serveEdit)
	mux.HandleFunc("DELETE /messaging/places/{place_id}/messages/{message_id}", s.serveDelete)
	mux.HandleFunc("POST /messaging/places/{place_id}/messages/{message_id}/reactions", s.serveToggleReaction)
	mux.HandleFunc("POST /messaging/places/{place_id}/messages/{message_id}/poll/vote", s.serveVotePoll)
	mux.HandleFunc("PUT /messaging/places/{place_id}/read-through", s.serveReadThrough)
	mux.HandleFunc("POST /messaging/attachments", s.serveUploadAttachment)
	mux.HandleFunc("GET /messaging/attachments/{attachment_id}", s.serveAttachment)
	mux.HandleFunc("PATCH /messaging/attachments/{attachment_id}", s.serveUpdateAttachment)
	mux.HandleFunc("PUT /messaging/status", s.serveSetStatus)
	mux.HandleFunc("GET /messaging/profile", s.serveProfile)
	mux.HandleFunc("PUT /messaging/profile", s.serveSetProfile)
	mux.HandleFunc("GET /messaging/notification-settings", s.serveNotificationSetting)
	mux.HandleFunc("PUT /messaging/notification-settings", s.serveSetNotificationSetting)
	mux.HandleFunc("POST /messaging/places/{place_id}/messages/{message_id}/reply-later", s.serveCreateReplyLater)
	mux.HandleFunc("POST /messaging/reply-later/{marker_id}/resolve", s.serveResolveReplyLater)
	mux.HandleFunc("GET /messaging/push-key", s.servePushKey)
	mux.HandleFunc("POST /messaging/push-subscriptions", s.serveSavePushSubscription)
	mux.HandleFunc("DELETE /messaging/push-subscriptions", s.serveDeletePushSubscription)
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

// placeWire matches the frozen PlaceRef shape from contracts/agent-events.yaml,
// extended with the thread kind (migration 0018). Consumers that do not know
// `thread` fail closed on the unknown kind, exactly as they must for any future
// kind, so the frozen shape is not reinterpreted.
type placeWire struct {
	Kind      string `json:"kind"`
	ChannelID string `json:"channel_id,omitempty"`
	DMID      string `json:"dm_id,omitempty"`
	ThreadID  string `json:"thread_id,omitempty"`
}

func placeToWire(p Place) placeWire {
	switch p.Kind {
	case PlaceChannel:
		return placeWire{Kind: p.Kind, ChannelID: p.PlaceID}
	case PlaceThread:
		return placeWire{Kind: p.Kind, ThreadID: p.PlaceID}
	default:
		return placeWire{Kind: p.Kind, DMID: p.PlaceID}
	}
}

// placeIDOf reads the identifier out of a client-sent place reference without
// caring which kind field carried it.
func (p placeWire) placeID() string {
	switch {
	case p.ChannelID != "":
		return p.ChannelID
	case p.DMID != "":
		return p.DMID
	default:
		return p.ThreadID
	}
}

// threadWire is a thread place plus what a list of threads renders: how much
// was said, when, and who is in it.
type threadWire struct {
	ThreadID        string            `json:"thread_id"`
	ParentPlace     placeWire         `json:"parent_place"`
	ParentMessageID *string           `json:"parent_message_id"`
	WorkspaceID     string            `json:"workspace_id"`
	Name            string            `json:"name"`
	MessageCount    int64             `json:"message_count"`
	LastMessageAt   *time.Time        `json:"last_message_at"`
	LastMessage     string            `json:"last_message"`
	Participants    []participantWire `json:"participants"`
	LatestSeq       int64             `json:"latest_seq"`
}

func threadToWire(t Thread) threadWire {
	wire := threadWire{
		ThreadID:      t.Place.PlaceID,
		ParentPlace:   placeToWire(Place{PlaceID: t.ParentPlaceID, Kind: PlaceChannel}),
		WorkspaceID:   t.Place.WorkspaceID,
		Name:          t.Place.Name,
		MessageCount:  t.MessageCount,
		LastMessageAt: t.LastMessageAt,
		LastMessage:   t.LastMessagePreview,
		Participants:  participantsToWire(t.Participants),
		LatestSeq:     t.Place.LastSeq,
	}
	if t.ParentMessageID != "" {
		origin := t.ParentMessageID
		wire.ParentMessageID = &origin
	}
	return wire
}

func threadsToWire(threads []Thread) []threadWire {
	out := make([]threadWire, len(threads))
	for i, thread := range threads {
		out[i] = threadToWire(thread)
	}
	return out
}

// attachmentWire is the file a message carries. The bytes are fetched from
// GET /messaging/attachments/{attachment_id}; the wire never inlines them.
type attachmentWire struct {
	AttachmentID string `json:"attachment_id"`
	Filename     string `json:"filename"`
	MIME         string `json:"mime"`
	Size         int64  `json:"size"`
	// Spoiler and Alt are the sender's declarations about the file. They travel
	// with every delivery — REST, WebSocket and the agent's local control lane —
	// so a PersonalityAgent reading a timeline knows「これはネタバレ画像だ」
	// exactly as a human's screen does.
	Spoiler bool   `json:"spoiler"`
	Alt     string `json:"alt"`
}

func attachmentToWire(a Attachment) attachmentWire {
	return attachmentWire{
		AttachmentID: a.AttachmentID,
		Filename:     a.Filename,
		MIME:         a.MIME,
		Size:         a.SizeBytes,
		Spoiler:      a.Spoiler,
		Alt:          a.Alt,
	}
}

func attachmentsToWire(attachments []Attachment) []attachmentWire {
	out := make([]attachmentWire, len(attachments))
	for i, a := range attachments {
		out[i] = attachmentToWire(a)
	}
	return out
}

type messageWire struct {
	MessageID   string            `json:"message_id"`
	Place       placeWire         `json:"place"`
	Seq         int64             `json:"seq"`
	Author      participantWire   `json:"author"`
	Content     string            `json:"content"`
	Mentions    []participantWire `json:"mentions"`
	Attachments []attachmentWire  `json:"attachments"`
	Urgency     string            `json:"urgency"`
	Reactions   []reactionWire    `json:"reactions"`
	// Poll is absent on the wire unless the message asks a question.
	Poll        *pollWire  `json:"poll,omitempty"`
	ReplyTo     *string    `json:"reply_to"`
	ClientNonce string     `json:"client_nonce"`
	CreatedAt   time.Time  `json:"created_at"`
	EditedAt    *time.Time `json:"edited_at"`
	Deleted     bool       `json:"deleted"`
}

// pollOptionWire is one choice and who currently picks it. Voters travel for
// the same reason reaction participants do: a shared decision is not a secret
// ballot, and the counts are derived from the same list the UI highlights.
type pollOptionWire struct {
	OptionID string            `json:"option_id"`
	Text     string            `json:"text"`
	Voters   []participantWire `json:"voters"`
}

type pollWire struct {
	Question   string           `json:"question"`
	AllowMulti bool             `json:"allow_multi"`
	ClosesAt   *time.Time       `json:"closes_at"`
	Options    []pollOptionWire `json:"options"`
}

func pollToWire(poll *Poll) *pollWire {
	if poll == nil {
		return nil
	}
	options := make([]pollOptionWire, len(poll.Options))
	for i, option := range poll.Options {
		options[i] = pollOptionWire{
			OptionID: option.OptionID,
			Text:     option.Text,
			Voters:   participantsToWire(option.Voters),
		}
	}
	return &pollWire{
		Question:   poll.Question,
		AllowMulti: poll.AllowMulti,
		ClosesAt:   poll.ClosesAt,
		Options:    options,
	}
}

// pollRequestWire is a poll as a sender states it. Option identity is minted
// server-side, so this shape carries texts only.
type pollRequestWire struct {
	Question   string     `json:"question"`
	AllowMulti bool       `json:"allow_multi"`
	ClosesAt   *time.Time `json:"closes_at"`
	Options    []string   `json:"options"`
}

func (w *pollRequestWire) input() *PollInput {
	if w == nil {
		return nil
	}
	return &PollInput{
		Question:   w.Question,
		AllowMulti: w.AllowMulti,
		ClosesAt:   w.ClosesAt,
		Options:    w.Options,
	}
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

func messageToWire(place Place, m Message) messageWire {
	w := messageWire{
		MessageID:   m.MessageID,
		Place:       placeToWire(place),
		Seq:         m.Seq,
		Author:      participantToWire(m.Author),
		Content:     m.Content,
		Mentions:    participantsToWire(m.Mentions),
		Attachments: attachmentsToWire(m.Attachments),
		Urgency:     m.Urgency,
		Reactions:   reactionsToWire(m.Reactions),
		Poll:        pollToWire(m.Poll),
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

// searchResultWire is one search hit: the permalink identity (place + seq)
// plus what the result list renders. The full content stays server-side; only
// the snippet crosses the wire.
type searchResultWire struct {
	MessageID string          `json:"message_id"`
	Place     placeWire       `json:"place"`
	Seq       int64           `json:"seq"`
	Author    participantWire `json:"author"`
	Snippet   string          `json:"snippet"`
	CreatedAt time.Time       `json:"created_at"`
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
	// Voice marks a channel people are meant to talk in (ADR 0012).
	Voice bool `json:"voice"`
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

// memberWire is one participant as the UI and the agent tool both see them:
// the 戸籍 name plus whatever the participant said about themselves. Humans and
// PersonalityAgents use the identical shape — there is no bot field.
type memberWire struct {
	Participant        participantWire `json:"participant"`
	DisplayName        string          `json:"display_name"`
	Tagline            string          `json:"tagline"`
	AvatarAttachmentID string          `json:"avatar_attachment_id,omitempty"`
	BannerAttachmentID string          `json:"banner_attachment_id,omitempty"`
}

func memberToWire(p MemberProfile) memberWire {
	return memberWire{
		Participant:        participantToWire(p.Participant),
		DisplayName:        p.ProjectedDisplayName(),
		Tagline:            p.Tagline,
		AvatarAttachmentID: p.AvatarAttachmentID,
		BannerAttachmentID: p.BannerAttachmentID,
	}
}

type readMarkerWire struct {
	Place       placeWire `json:"place"`
	LastReadSeq int64     `json:"last_read_seq"`
}

// statusWire matches the web model's ParticipantStatus. A cleared status (a
// temporary one that lapsed with nothing behind it) travels with an empty
// status: the participant is no longer saying anything about their attention,
// which is different from saying they are available.
type statusWire struct {
	Participant participantWire `json:"participant"`
	Status      string          `json:"status"`
	Note        string          `json:"note"`
	ExpiresAt   *time.Time      `json:"expires_at"`
	// What this temporary status lapses back to. Empty means the lapse ends
	// the declaration instead of restoring an earlier one.
	BaseStatus string `json:"base_status"`
	BaseNote   string `json:"base_note"`
}

func statusToWire(status ParticipantStatus) statusWire {
	return statusWire{
		Participant: participantToWire(status.Participant),
		Status:      status.Status,
		Note:        status.Note,
		ExpiresAt:   status.ExpiresAt,
		BaseStatus:  status.BaseStatus,
		BaseNote:    status.BaseNote,
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
	// 同じひとつの判定から、身体の違う二つの出口へ（凍結契約 v1「Push 通知
	// レイヤーとの対応」）。人間には閉じたタブの向こうへ Web Push、agent には
	// runtime が止まっていても残る AttentionCandidate。どちらも WS が届いた
	// 人を除外したりはしない——「今見ている画面に重ねない」の判断は、その
	// 画面を持っている側（SW / agent 本人）が行う。
	store.deliverPush(ctx, place, msg, decisions)
	store.recordAttentionCandidates(ctx, place, msg, decisions)
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

// DefaultStatusExpiryInterval is how often lapsed temporary statuses are swept.
// Readers already resolve expiry themselves, so this only bounds how late the
// live announcement is — a minute of lag on「1時間だけ取り込み中」is invisible,
// and a tighter loop would buy nothing but wakeups.
const DefaultStatusExpiryInterval = time.Minute

// RunStatusExpiry sweeps lapsed temporary statuses until ctx is done,
// announcing each participant's restored state over the socket so a screen
// left open stops showing「取り込み中」after it stopped being true. Expiry is
// still resolved at read time, so this loop is liveness, not correctness: it
// can be skipped entirely without any reader seeing a stale declaration.
func (s *Server) RunStatusExpiry(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultStatusExpiryInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			statuses, err := s.Store.ExpireStatuses(ctx)
			if err != nil {
				// Best effort: readers still resolve expiry themselves, and
				// the next tick retries.
				continue
			}
			for _, status := range statuses {
				s.publishStatus(ctx, status)
			}
		}
	}
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
			memberSet[key] = memberToWire(p)
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
		// A thread's identity travels in `threads` (it needs its parent), and
		// its members are the parent channel's — already added above.
		if sum.Place.Kind == PlaceThread {
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
	// Threads the viewer has joined. They are places like any other, but the
	// client needs the parent relation to place them under their channel
	// instead of listing them beside it.
	threads, err := s.Store.ThreadsFor(ctx, viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, struct {
		Self                participantWire         `json:"self"`
		Workspaces          []workspaceWire         `json:"workspaces"`
		Channels            []channelWire           `json:"channels"`
		DMs                 []dmWire                `json:"dms"`
		Threads             []threadWire            `json:"threads"`
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
		Threads:             threadsToWire(threads),
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
		// 省略はテキストチャンネル。既定を「話す場所」にしない。
		Voice bool `json:"voice"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Name == "" || utf8.RuneCountInString(req.Name) > MaxChannelNameChars {
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
		place, opErr = s.Store.CreateChannel(r.Context(), req.WorkspaceID, req.Name, req.Topic, viewer, req.Voice)
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

// serveUpdatePlace edits a channel's mutable identity: name, topic, or both.
// An omitted field is left alone, so renaming a channel never clears its topic.
func (s *Server) serveUpdatePlace(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	var req struct {
		Name  *string `json:"name"`
		Topic *string `json:"topic"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Name != nil && (*req.Name == "" || utf8.RuneCountInString(*req.Name) > MaxChannelNameChars) {
		writeError(w, http.StatusBadRequest, "invalid_name")
		return
	}
	if req.Topic != nil && len(*req.Topic) > maxTopicBytes {
		writeError(w, http.StatusBadRequest, "invalid_topic")
		return
	}
	var place Place
	done, err := s.mutate(w, r, claims, func() error {
		var opErr error
		place, opErr = s.Store.UpdateChannel(r.Context(), r.PathValue("place_id"), req.Name, req.Topic, viewer)
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

// serveDuplicatePlace creates a new channel beside an existing one. An omitted
// or empty name takes the server's derived default, so the human menu and the
// agent tool produce the same copy.
func (s *Server) serveDuplicatePlace(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if utf8.RuneCountInString(req.Name) > MaxChannelNameChars {
		writeError(w, http.StatusBadRequest, "invalid_name")
		return
	}
	var place Place
	done, err := s.mutate(w, r, claims, func() error {
		var opErr error
		place, opErr = s.Store.DuplicateChannel(r.Context(), r.PathValue("place_id"), req.Name, viewer)
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

// serveThreads lists the side conversations under a channel. Every member of
// the parent sees all of them: a thread keeps a tangent out of the main flow,
// it does not hide it.
func (s *Server) serveThreads(w http.ResponseWriter, r *http.Request) {
	viewer, _, ok := s.viewer(w, r)
	if !ok {
		return
	}
	threads, err := s.Store.ThreadsIn(r.Context(), r.PathValue("place_id"), viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Threads []threadWire `json:"threads"`
	}{Threads: threadsToWire(threads)})
}

// serveCreateThread opens a thread under a channel, optionally anchored to the
// message it grew out of. The same store call backs the agent tool path.
func (s *Server) serveCreateThread(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	var req struct {
		Name string `json:"name"`
		// Empty means the thread starts from nothing said yet.
		ParentMessageID string `json:"parent_message_id"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.Name) == "" || utf8.RuneCountInString(req.Name) > MaxThreadNameChars {
		writeError(w, http.StatusBadRequest, "invalid_name")
		return
	}
	var thread Thread
	done, err := s.mutate(w, r, claims, func() error {
		var opErr error
		thread, opErr = s.Store.CreateThread(
			r.Context(), r.PathValue("place_id"), req.Name, req.ParentMessageID, viewer)
		return opErr
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	wire := threadToWire(thread)
	s.Hub.Publish(r.Context(), Event{
		Type: EventPlaceCreated, PlaceID: thread.ParentPlaceID, Thread: &wire,
	})
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
		members[i] = memberToWire(p)
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

// serveSearch is GET /messaging/search?q=…&place_id=…&limit=…. Visibility is
// enforced by the store: results only ever come from places the session's
// Human can see, and a place_id the viewer cannot see is 404, not empty.
func (s *Server) serveSearch(w http.ResponseWriter, r *http.Request) {
	viewer, _, ok := s.viewer(w, r)
	if !ok {
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" || len(query) > MaxSearchQueryBytes {
		writeError(w, http.StatusBadRequest, "invalid_query")
		return
	}
	opt := SearchOptions{PlaceID: r.URL.Query().Get("place_id")}
	if v := r.URL.Query().Get("limit"); v != "" {
		limit, err := strconv.Atoi(v)
		if err != nil || limit <= 0 {
			writeError(w, http.StatusBadRequest, "invalid_limit")
			return
		}
		opt.Limit = limit
	}
	results, err := s.Store.SearchMessages(r.Context(), viewer, query, opt)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	wires := make([]searchResultWire, len(results))
	for i, res := range results {
		wires[i] = searchResultWire{
			MessageID: res.Message.MessageID,
			Place:     placeToWire(res.Place),
			Seq:       res.Message.Seq,
			Author:    participantToWire(res.Message.Author),
			Snippet:   res.Snippet,
			CreatedAt: res.Message.CreatedAt,
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
		// A poll rides on the ordinary send: the question and the message that
		// asks it commit together, rather than as two events one of which can
		// be missing (契約: メッセージが投票を運ぶ).
		Poll *pollRequestWire `json:"poll"`
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
	// Attachment-only and poll-only messages are legitimate; empty with
	// nothing attached is not.
	if (req.Content == "" && len(req.Attachments) == 0 && req.Poll == nil) ||
		len(req.Content) > MaxContentBytes {
		writeError(w, http.StatusBadRequest, "invalid_content")
		return
	}
	if len(req.Attachments) > MaxAttachmentsPerMessage {
		writeError(w, http.StatusBadRequest, "too_many_attachments")
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
			AttachmentIDs: req.Attachments, Poll: req.Poll.input(),
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
		Emoji string `json:"emoji"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if validateReactionEmoji(req.Emoji) != nil {
		writeError(w, http.StatusBadRequest, "invalid_emoji")
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
		msg, reacted, opErr = s.Store.ToggleReaction(
			r.Context(), placeID, r.PathValue("message_id"), viewer, req.Emoji)
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
	s.Hub.Publish(r.Context(), Event{Type: EventReactionUpdated, PlaceID: placeID, Message: &wire})
	writeJSON(w, http.StatusOK, struct {
		Message messageWire `json:"message"`
		Reacted bool        `json:"reacted"`
	}{Message: wire, Reacted: reacted})
}

// serveVotePoll restates the viewer's whole choice on one poll. An empty
// option list withdraws the vote — changing your mind and taking it back are
// the same act, not two capabilities. The same store call backs the agent
// tool path (AX: UIだけにある操作を作らない).
func (s *Server) serveVotePoll(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	placeID := r.PathValue("place_id")
	var req struct {
		OptionIDs []string `json:"option_ids"`
	}
	if !decodeJSON(w, r, &req) {
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
		msg, opErr = s.Store.VotePoll(
			r.Context(), placeID, r.PathValue("message_id"), viewer, req.OptionIDs)
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
	s.Hub.Publish(r.Context(), Event{Type: EventPollUpdated, PlaceID: placeID, Message: &wire})
	writeJSON(w, http.StatusOK, struct {
		Message messageWire `json:"message"`
	}{Message: wire})
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
	if !ValidStatus(req.Status) {
		writeError(w, http.StatusBadRequest, "invalid_status")
		return
	}
	if utf8.RuneCountInString(req.Note) > MaxStatusNoteChars {
		writeError(w, http.StatusBadRequest, "invalid_note")
		return
	}
	// An expiry already in the past would be a status nobody ever holds.
	if req.ExpiresAt != nil && !req.ExpiresAt.After(time.Now()) {
		writeError(w, http.StatusBadRequest, "invalid_expires_at")
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

// serveProfile returns the viewer's own profile. Everyone else's profile
// arrives with the member list, so there is no route for reading one by id.
func (s *Server) serveProfile(w http.ResponseWriter, r *http.Request) {
	viewer, _, ok := s.viewer(w, r)
	if !ok {
		return
	}
	profile, err := s.Store.MemberProfileFor(r.Context(), viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, memberToWire(profile))
}

// serveSetProfile replaces the viewer's own profile. PUT is a replacement for
// the same reason the notification setting is: the client already holds the
// current value (bootstrap gives it), so a partial merge would only add a way
// for two tabs to disagree about what was cleared.
//
// There is no route for editing anyone else's profile — 名乗りは本人のもの。
func (s *Server) serveSetProfile(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	var req struct {
		DisplayName string `json:"display_name"`
		Tagline     string `json:"tagline"`
		// 空文字は「画像を外す」。省略も同じ意味になる（PUTは全置換）。
		AvatarAttachmentID string `json:"avatar_attachment_id"`
		BannerAttachmentID string `json:"banner_attachment_id"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	var profile MemberProfile
	done, err := s.mutate(w, r, claims, func() error {
		var opErr error
		profile, opErr = s.Store.SetProfile(r.Context(), viewer,
			req.DisplayName, req.Tagline, req.AvatarAttachmentID, req.BannerAttachmentID)
		return opErr
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	s.publishProfile(r.Context(), profile)
	writeJSON(w, http.StatusOK, memberToWire(profile))
}

// publishProfile tells everyone who can see this participant that the way they
// present themselves changed. Like status it is participant-scoped and never
// replayed: bootstrap carries the current value.
func (s *Server) publishProfile(ctx context.Context, profile MemberProfile) {
	if s.Hub == nil {
		return
	}
	subject := profile.Participant
	wire := memberToWire(profile)
	s.Hub.Publish(ctx, Event{Type: EventProfileUpdated, Subject: &subject, Member: &wire})
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
		placeID := entry.Place.placeID()
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

// --- attachments ---

// maxAttachmentEnvelopeBytes is the multipart framing headroom allowed on top
// of the file itself (part headers, boundaries, other fields).
const maxAttachmentEnvelopeBytes = 64 * 1024

// inlineImageMIMEs are the only types served with `Content-Disposition:
// inline`. Everything else — including image/svg+xml, which is a scriptable
// document — is delivered as a download.
var inlineImageMIMEs = map[string]bool{
	"image/png":  true,
	"image/jpeg": true,
	"image/gif":  true,
	"image/webp": true,
}

// serveUploadAttachment accepts one multipart file and returns its identity.
// The upload is not yet part of any message: it is bound when the uploader
// sends a message listing it.
func (s *Server) serveUploadAttachment(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	if s.Attachments == nil {
		writeError(w, http.StatusServiceUnavailable, "attachments_unavailable")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, MaxAttachmentBytes+maxAttachmentEnvelopeBytes)
	parts, err := r.MultipartReader()
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_multipart")
		return
	}
	var (
		att   Attachment
		found bool
	)
	done, err := s.mutate(w, r, claims, func() error {
		for {
			part, err := parts.NextPart()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return errInvalidMultipart
			}
			if part.FormName() != "file" || part.FileName() == "" {
				part.Close()
				continue
			}
			att, err = s.storeUpload(r.Context(), viewer, part)
			part.Close()
			if err != nil {
				return err
			}
			found = true
			return nil
		}
	})
	if !done {
		return
	}
	switch {
	case errors.Is(err, errInvalidMultipart):
		writeError(w, http.StatusBadRequest, "invalid_multipart")
		return
	case errors.Is(err, ErrAttachmentTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, "attachment_too_large")
		return
	case err != nil:
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "attachment_too_large")
			return
		}
		writeStoreError(w, err)
		return
	}
	if !found {
		writeError(w, http.StatusBadRequest, "missing_file")
		return
	}
	writeJSON(w, http.StatusCreated, attachmentToWire(att))
}

var errInvalidMultipart = errors.New("invalid multipart body")

// storeUpload writes the part's bytes and records the metadata. The blob is
// written first under a freshly minted id, so a metadata row never points at
// missing bytes; a failed insert removes the orphan it just wrote.
func (s *Server) storeUpload(ctx context.Context, uploader ParticipantRef, part *multipart.Part) (Attachment, error) {
	head := make([]byte, 512)
	read, err := io.ReadFull(part, head)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return Attachment{}, ErrAttachmentTooLarge
		}
		return Attachment{}, errInvalidMultipart
	}
	head = head[:read]
	attachmentID := NewAttachmentID()
	size, err := s.Attachments.Put(attachmentID, io.MultiReader(bytes.NewReader(head), part))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return Attachment{}, ErrAttachmentTooLarge
		}
		_ = s.Attachments.Remove(attachmentID)
		return Attachment{}, err
	}
	att, err := s.Store.CreateAttachment(ctx, attachmentID, uploader,
		sanitizeAttachmentFilename(part.FileName()),
		resolveAttachmentMIME(part.Header.Get("Content-Type"), head), size)
	if err != nil {
		_ = s.Attachments.Remove(attachmentID)
		return Attachment{}, err
	}
	return att, nil
}

// serveAttachment delivers the bytes to a viewer the store says may read them.
// Every response is nosniff and sandboxed; only known-safe image types render
// inline, so an uploaded document can never execute in the app's origin.
func (s *Server) serveAttachment(w http.ResponseWriter, r *http.Request) {
	viewer, _, ok := s.viewer(w, r)
	if !ok {
		return
	}
	if s.Attachments == nil {
		writeError(w, http.StatusServiceUnavailable, "attachments_unavailable")
		return
	}
	att, err := s.Store.AttachmentForViewer(r.Context(), r.PathValue("attachment_id"), viewer)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	blob, err := s.Attachments.Open(att.AttachmentID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	defer blob.Close()
	inline := inlineImageMIMEs[att.MIME]
	disposition := "attachment"
	contentType := "application/octet-stream"
	if inline {
		disposition = "inline"
		contentType = att.MIME
	}
	header := w.Header()
	header.Set("Content-Type", contentType)
	header.Set("Content-Disposition",
		mime.FormatMediaType(disposition, map[string]string{"filename": att.Filename}))
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("Content-Security-Policy", "default-src 'none'; sandbox")
	header.Set("Cross-Origin-Resource-Policy", "same-origin")
	header.Set("Referrer-Policy", "no-referrer")
	// Private: the response is authorized per viewer and must never be reused
	// from a shared cache.
	header.Set("Cache-Control", "private, no-store")
	http.ServeContent(w, r, "", att.CreatedAt, blob)
}

// serveUpdateAttachment edits an upload before it is sent: display name,
// description, and the spoiler flag. Only the uploader's own still-unbound
// attachment can be edited — after send, what the recipients saw stands.
//
// An absent field is「触らない」, so a caller naming one preference does not
// silently reset the others (the same shape as the notification settings
// lane).
func (s *Server) serveUpdateAttachment(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	var req struct {
		Filename *string `json:"filename"`
		Alt      *string `json:"alt"`
		Spoiler  *bool   `json:"spoiler"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Filename == nil && req.Alt == nil && req.Spoiler == nil {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if req.Filename != nil {
		name := sanitizeAttachmentFilename(*req.Filename)
		req.Filename = &name
	}
	if req.Alt != nil {
		alt := sanitizeAttachmentAlt(*req.Alt)
		if utf8.RuneCountInString(alt) > MaxAttachmentAltRunes {
			writeError(w, http.StatusBadRequest, "invalid_request")
			return
		}
		req.Alt = &alt
	}
	var att Attachment
	done, err := s.mutate(w, r, claims, func() error {
		var updateErr error
		att, updateErr = s.Store.UpdateDraftAttachment(r.Context(),
			r.PathValue("attachment_id"), viewer,
			AttachmentDraftPatch{Filename: req.Filename, Alt: req.Alt, Spoiler: req.Spoiler})
		return updateErr
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, attachmentToWire(att))
}

// sanitizeAttachmentAlt keeps a one-paragraph description: no control
// characters (newlines included), bounded by the caller's check afterwards.
func sanitizeAttachmentAlt(alt string) string {
	alt = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, alt)
	return strings.TrimSpace(alt)
}

// sanitizeAttachmentFilename keeps a display name only: no directories, no
// control characters, bounded length. It is never used as a storage path.
func sanitizeAttachmentFilename(name string) string {
	name = strings.ReplaceAll(name, "\\", "/")
	name = path.Base(strings.TrimSpace(name))
	name = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, name)
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == ".." || name == "/" {
		return "file"
	}
	for len(name) > MaxAttachmentFilenameBytes {
		_, size := utf8.DecodeLastRuneInString(name)
		name = name[:len(name)-size]
	}
	if name == "" {
		return "file"
	}
	return name
}

// resolveAttachmentMIME decides the stored type from the bytes first and the
// client's claim second. Bytes that sniff as a supported image are that image;
// a claimed image whose bytes disagree is demoted to an opaque download, so a
// document can never be delivered under an inline image type.
func resolveAttachmentMIME(declared string, head []byte) string {
	sniffed := normalizeMediaType(http.DetectContentType(head))
	if inlineImageMIMEs[sniffed] {
		return sniffed
	}
	claimed := normalizeMediaType(declared)
	if claimed == "" || inlineImageMIMEs[claimed] || strings.HasPrefix(claimed, "image/") {
		return "application/octet-stream"
	}
	return claimed
}

func normalizeMediaType(value string) string {
	parsed, _, err := mime.ParseMediaType(strings.TrimSpace(value))
	if err != nil {
		return ""
	}
	parsed = strings.ToLower(parsed)
	if len(parsed) > 255 {
		return ""
	}
	return parsed
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
	case errors.Is(err, ErrAttachmentNotFound):
		writeError(w, http.StatusNotFound, "attachment_not_found")
	case errors.Is(err, ErrAttachmentTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, "attachment_too_large")
	case errors.Is(err, ErrTooManyAttachments):
		writeError(w, http.StatusBadRequest, "too_many_attachments")
	case errors.Is(err, ErrAttachmentEmpty):
		writeError(w, http.StatusBadRequest, "empty_attachment")
	case errors.Is(err, ErrAttachmentAlreadySent):
		writeError(w, http.StatusConflict, "attachment_already_sent")
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
	case errors.Is(err, ErrInvalidNotificationSetting):
		writeError(w, http.StatusBadRequest, "invalid_notification_setting")
	case errors.Is(err, ErrInvalidPushSubscription):
		writeError(w, http.StatusBadRequest, "invalid_push_subscription")
	case errors.Is(err, ErrNotAChannel):
		writeError(w, http.StatusBadRequest, "not_a_channel")
	case errors.Is(err, ErrInvalidChannelName):
		writeError(w, http.StatusBadRequest, "invalid_name")
	case errors.Is(err, ErrNotThreadable):
		writeError(w, http.StatusBadRequest, "not_threadable")
	case errors.Is(err, ErrThreadExists):
		writeError(w, http.StatusConflict, "thread_exists")
	case errors.Is(err, ErrInvalidPoll):
		writeError(w, http.StatusBadRequest, "invalid_poll")
	case errors.Is(err, ErrPollNotFound), errors.Is(err, ErrPollOptionNotFound):
		writeError(w, http.StatusNotFound, "poll_not_found")
	case errors.Is(err, ErrPollClosed):
		writeError(w, http.StatusConflict, "poll_closed")
	case errors.Is(err, ErrPollSingleChoice):
		writeError(w, http.StatusBadRequest, "poll_single_choice")
	case errors.Is(err, ErrInvalidDisplayName):
		writeError(w, http.StatusBadRequest, "invalid_display_name")
	case errors.Is(err, ErrInvalidTagline):
		writeError(w, http.StatusBadRequest, "invalid_tagline")
	case errors.Is(err, ErrInvalidProfileImage):
		writeError(w, http.StatusBadRequest, "invalid_profile_image")
	default:
		writeError(w, http.StatusInternalServerError, "internal")
	}
}
