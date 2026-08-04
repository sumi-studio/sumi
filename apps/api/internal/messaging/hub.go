package messaging

import (
	"context"
	"encoding/json"
	"sync"
)

// Event is one durable or volatile messaging event fanned out to live
// subscribers. Durable events carry the message (with its place seq); volatile
// events (typing) carry only the actor and are never replayed.
type Event struct {
	Type    string           `json:"type"`
	PlaceID string           `json:"place_id"`
	Message *messageWire     `json:"message,omitempty"`
	Actor   *participantWire `json:"actor,omitempty"`
}

// Durable event types. The wire names match the web model's ServerEvent.
const (
	EventMessageCreated  = "message_created"
	EventMessageEdited   = "message_edited"
	EventMessageDeleted  = "message_deleted"
	EventReactionUpdated = "reaction_updated"
	EventTyping          = "typing"
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

// Publish delivers the event to every subscriber who can see its place.
// Slow subscribers are dropped rather than blocking the publisher.
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
		ok, known := sub.visibility(event.PlaceID)
		if !known {
			_, err := h.store.PlaceFor(ctx, event.PlaceID, sub.viewer)
			ok = err == nil
			sub.markVisible(event.PlaceID, ok)
		}
		if !ok {
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
