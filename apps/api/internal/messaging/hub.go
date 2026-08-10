package messaging

import (
	"context"
	"encoding/json"
	"sync"
)

// Event is one durable or volatile messaging event fanned out to live
// subscribers. Message events carry the whole message (with its place seq);
// reaction_updated carries only the partial reaction payload so it can never
// roll back a concurrent edit. Place events carry a place summary but are not
// replayed because reconnecting clients re-read the durable places table via
// bootstrap. Volatile events (typing, status_updated) are never replayed.
// Place events scope delivery by PlaceID; participant-scoped events
// (status_updated) leave PlaceID empty and set Subject instead.
type Event struct {
	Type     string              `json:"type"`
	PlaceID  string              `json:"place_id,omitempty"`
	Message  *messageWire        `json:"message,omitempty"`
	Reaction *reactionUpdateWire `json:"reaction,omitempty"`
	Actor    *participantWire    `json:"actor,omitempty"`
	Channel  *channelWire        `json:"channel,omitempty"`
	DM       *dmWire             `json:"dm,omitempty"`
	Status   *statusWire         `json:"status,omitempty"`
	Marker   *replyLaterWire     `json:"marker,omitempty"`
	// Notify rides only on the copy addressed to a recipient the server decided
	// to interrupt. Its absence is the answer "this is not worth calling you
	// for", which is why it is per-recipient rather than part of the message.
	Notify   *notifyWire `json:"notify,omitempty"`
	MarkerID string      `json:"marker_id,omitempty"`

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
	EventMessageCreated     = "message_created"
	EventMessageEdited      = "message_edited"
	EventMessageDeleted     = "message_deleted"
	EventReactionUpdated    = "reaction_updated"
	EventTyping             = "typing"
	EventStatusUpdated      = "status_updated"
	EventReplyLaterCreated  = "reply_later_created"
	EventReplyLaterResolved = "reply_later_resolved"
	EventPlaceCreated       = "place_created"
	EventPlaceUpdated       = "place_updated"
)

// subscriber is one live WebSocket connection's delivery state. visible keeps
// the most recent observation for catch-up bookkeeping and the store-less
// session-revocation harness, but a live store-backed publish never trusts it:
// membership is re-authorized for every event so revocation fences content
// without requiring reconnect.
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

// visibleTo answers "may this subscriber be told about this event now". Place
// events use place visibility; participant-scoped events use ParticipantVisible
// under a prefixed key so the two namespaces cannot collide. Store-backed live
// delivery always rechecks current authority; the cached observation is never
// an authorization input. An event with neither scope is delivered to no one.
func (h *Hub) visibleTo(ctx context.Context, sub *subscriber, event Event) bool {
	scope := event.PlaceID
	if scope == "" {
		if event.Subject == nil {
			return false
		}
		scope = "participant|" + event.Subject.Key()
	}
	// A nil store exists only in the isolated session-revocation test harness.
	// Its cached scope is still fenced by writePump's session authorization.
	if h.store == nil {
		ok, known := sub.visibility(scope)
		return known && ok
	}
	var ok bool
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
