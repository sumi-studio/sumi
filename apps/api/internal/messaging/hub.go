package messaging

import (
	"context"
	"encoding/json"
	"sync"
)

// Event is one durable or volatile messaging event fanned out to live
// subscribers. Durable events carry the message (with its place seq) or the
// place summary; volatile events (typing, status_updated) are never replayed.
// Place events scope delivery by PlaceID; participant-scoped events
// (status_updated) leave PlaceID empty and set Subject instead. Place
// created/updated events are not replayed either — the durable truth is the
// places table, and a reconnecting client re-reads it via bootstrap.
type Event struct {
	Type    string       `json:"type"`
	PlaceID string       `json:"place_id,omitempty"`
	Message *messageWire `json:"message,omitempty"`
	// Reaction is an absolute reaction-only projection. It deliberately does
	// not carry the rest of a message, which may have been edited concurrently.
	Reaction *reactionUpdateWire `json:"reaction,omitempty"`
	Actor    *participantWire    `json:"actor,omitempty"`
	Status   *statusWire         `json:"status,omitempty"`
	Marker   *replyLaterWire     `json:"marker,omitempty"`
	Channel  *channelWire        `json:"channel,omitempty"`
	DM       *dmWire             `json:"dm,omitempty"`
	// Thread rides on place_created scoped to the parent place, so everyone
	// who can see the parent learns the side conversation exists.
	Thread *threadWire `json:"thread,omitempty"`
	// Member rides on participant-scoped profile changes (表示名・tagline・画像).
	Member *memberWire `json:"member,omitempty"`
	// Notify rides only on the copy addressed to a recipient the server decided
	// to interrupt. Its absence is the answer "this is not worth calling you
	// for", which is why it is per-recipient rather than part of the message.
	Notify   *notifyWire `json:"notify,omitempty"`
	MarkerID string      `json:"marker_id,omitempty"`
	// Call is one place's live call membership (ADR 0012). Volatile like
	// typing: never replayed, re-read from GET /messaging/calls on reconnect.
	Call *callStateWire `json:"call,omitempty"`

	// Delivery controls; never serialized. Subject scopes a place-less event to
	// subscribers who can see that participant. OnlyFor/ExceptFor split one
	// logical event into per-audience payloads (remind_at は本人以外の wire に
	// 載せない; notify は呼ばれた人の wire にしか載らない).
	Subject   *ParticipantRef  `json:"-"`
	OnlyFor   *ParticipantRef  `json:"-"`
	ExceptFor []ParticipantRef `json:"-"`
}

// Durable event types. The wire names match the web model's ServerEvent.
const (
	EventMessageCreated  = "message_created"
	EventMessageEdited   = "message_edited"
	EventMessageDeleted  = "message_deleted"
	EventReactionUpdated = "reaction_updated"
	// EventPollUpdated carries the whole message after a vote. The durable truth
	// is the vote rows; this event is only the fan-out projection.
	EventPollUpdated        = "poll_updated"
	EventTyping             = "typing"
	EventStatusUpdated      = "status_updated"
	EventProfileUpdated     = "profile_updated"
	EventReplyLaterCreated  = "reply_later_created"
	EventReplyLaterResolved = "reply_later_resolved"
	EventPlaceCreated       = "place_created"
	EventPlaceUpdated       = "place_updated"
	EventCallState          = "call_state"
)

// subscriber is one live WebSocket connection's delivery state. visible is a
// positive cache of place visibility: places the subscriber was allowed to see
// at first contact. Unknown places are re-checked against the store (so places
// created mid-connection are delivered); revocations take effect on reconnect,
// like most live-session permission models. The messaging surface's authority
// for what exists is always the store — the hub only decides who is told now.
type subscriber struct {
	viewer ParticipantRef
	send   chan []byte
	// done is closed exactly once on unsubscribe. send is never closed, so
	// concurrent publishers can always select against done without racing a
	// channel close.
	done    chan struct{}
	mu      sync.Mutex
	visible map[string]bool
}

// markVisible records a known visibility verdict.
func (s *subscriber) markVisible(placeID string, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.visible[placeID] = ok
}

func (s *subscriber) visibility(placeID string) (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ok, known := s.visible[placeID]
	return ok, known
}

// Hub fans messaging events out to live subscribers. REST mutations and WS
// mutations publish through the same hub, so a message sent over HTTP still
// reaches every open socket. Delivery is best-effort (凍結契約 v1: 候補の
// 配送はbest-effort、正本はstoreにあり、落ちてもseqから再構成できる) — a
// subscriber whose buffer overflows is dropped and reconnects with cursors.
type Hub struct {
	store *Store

	mu          sync.Mutex
	subscribers map[*subscriber]struct{}
}

// NewHub returns a hub that consults the store for place visibility.
func NewHub(store *Store) *Hub {
	return &Hub{store: store, subscribers: map[*subscriber]struct{}{}}
}

func (h *Hub) subscribe(viewer ParticipantRef) *subscriber {
	sub := &subscriber{
		viewer: viewer,
		// Enough headroom for a busy place; overflow means the reader is not
		// keeping up and replay-on-reconnect is the correct recovery.
		send:    make(chan []byte, 256),
		done:    make(chan struct{}),
		visible: map[string]bool{},
	}
	h.mu.Lock()
	h.subscribers[sub] = struct{}{}
	h.mu.Unlock()
	return sub
}

func (h *Hub) unsubscribe(sub *subscriber) {
	h.mu.Lock()
	if _, ok := h.subscribers[sub]; ok {
		delete(h.subscribers, sub)
		close(sub.done)
	}
	h.mu.Unlock()
}

// Publish delivers the event to every subscriber in its audience who can see
// its scope (place, or subject participant for place-less events). Slow
// subscribers are dropped rather than blocking the publisher.
func (h *Hub) Publish(ctx context.Context, event Event) {
	if h == nil {
		return
	}
	frame, err := json.Marshal(struct {
		Type  string `json:"type"`
		Event Event  `json:"event"`
	}{Type: "event", Event: event})
	if err != nil {
		return
	}
	h.mu.Lock()
	subs := make([]*subscriber, 0, len(h.subscribers))
	for sub := range h.subscribers {
		subs = append(subs, sub)
	}
	h.mu.Unlock()

	var drop []*subscriber
	for _, sub := range subs {
		if event.OnlyFor != nil && sub.viewer != *event.OnlyFor {
			continue
		}
		if excluded(event.ExceptFor, sub.viewer) {
			continue
		}
		if !h.visibleTo(ctx, sub, event) {
			continue
		}
		select {
		case <-sub.done:
		case sub.send <- frame:
		default:
			drop = append(drop, sub)
		}
	}
	for _, sub := range drop {
		h.unsubscribe(sub)
	}
}

// excluded reports whether the viewer is in an event's exclusion list. The
// list is short by construction (one audience split per publish), so a linear
// scan is the honest shape.
func excluded(refs []ParticipantRef, viewer ParticipantRef) bool {
	for _, ref := range refs {
		if ref == viewer {
			return true
		}
	}
	return false
}

// visibleTo answers "may this subscriber be told about this event now",
// caching verdicts per scope key. Place events use place visibility;
// participant-scoped events use ParticipantVisible under a prefixed key so the
// two namespaces cannot collide. An event with neither scope is delivered to
// no one (fail-closed).
func (h *Hub) visibleTo(ctx context.Context, sub *subscriber, event Event) bool {
	scope := event.PlaceID
	if scope == "" {
		if event.Subject == nil {
			return false
		}
		scope = "participant|" + event.Subject.Key()
	}
	ok, known := sub.visibility(scope)
	if known {
		return ok
	}
	if event.PlaceID != "" {
		_, err := h.store.PlaceFor(ctx, event.PlaceID, sub.viewer)
		ok = err == nil
	} else {
		visible, err := h.store.ParticipantVisible(ctx, sub.viewer, *event.Subject)
		ok = err == nil && visible
	}
	sub.markVisible(scope, ok)
	return ok
}
