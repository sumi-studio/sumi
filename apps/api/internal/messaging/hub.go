package messaging

import (
	"context"
	"encoding/json"
	"sync"
)

// Event is one durable or volatile messaging event fanned out to live
// subscribers. Message events carry the whole message (with its place seq);
// reaction_updated and poll_updated carry only their partial projections so
// they can never roll back another concurrently changed message field. Place
// events carry a place summary but are not replayed because reconnecting
// clients re-read the durable places table via
// bootstrap. Volatile events (typing, status_updated) are never replayed.
// Place events scope delivery by PlaceID; participant-scoped events
// (status_updated) leave PlaceID empty and set Subject instead.
type Event struct {
	Type     string              `json:"type"`
	PlaceID  string              `json:"place_id,omitempty"`
	Message  *messageWire        `json:"message,omitempty"`
	Reaction *reactionUpdateWire `json:"reaction,omitempty"`
	Poll     *pollUpdateWire     `json:"poll,omitempty"`
	Actor    *participantWire    `json:"actor,omitempty"`
	Channel  *channelWire        `json:"channel,omitempty"`
	DM       *dmWire             `json:"dm,omitempty"`
	Thread   *threadWire         `json:"thread,omitempty"`
	Status   *statusWire         `json:"status,omitempty"`
	Marker   *replyLaterWire     `json:"marker,omitempty"`
	Call     *callStateWire      `json:"call,omitempty"`
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

// liveBoundary is the non-serialized application address for one live frame.
// Exactly one of placeID and subjectSet is populated. It is comparable so all
// variants of one logical event can be proven to share one audience snapshot.
type liveBoundary struct {
	placeID    string
	subject    ParticipantRef
	subjectSet bool
}

func (b liveBoundary) key() string {
	if b.placeID != "" {
		return b.placeID
	}
	if b.subjectSet {
		return "participant|" + b.subject.Key()
	}
	return ""
}

type outboundFrame struct {
	payload  []byte
	boundary liveBoundary
}

// Durable event types. The wire names match the web model's ServerEvent.
const (
	EventMessageCreated     = "message_created"
	EventMessageEdited      = "message_edited"
	EventMessageDeleted     = "message_deleted"
	EventReactionUpdated    = "reaction_updated"
	EventPollUpdated        = "poll_updated"
	EventTyping             = "typing"
	EventStatusUpdated      = "status_updated"
	EventReplyLaterCreated  = "reply_later_created"
	EventReplyLaterResolved = "reply_later_resolved"
	EventPlaceCreated       = "place_created"
	EventPlaceUpdated       = "place_updated"
	EventCallState          = "call_state"
)

// subscriber is one live WebSocket connection's delivery state. visible keeps
// the most recent observation for catch-up bookkeeping and the store-less
// session-revocation harness, but a live store-backed publish never trusts it:
// membership is re-authorized for every event so revocation fences content
// without requiring reconnect.
type subscriber struct {
	viewer ParticipantRef
	store  *ScopedStore
	send   chan outboundFrame
	// done is closed exactly once on unsubscribe. send is never closed, so
	// concurrent publishers can always select against done without racing a
	// channel close.
	done    chan struct{}
	mu      sync.Mutex
	visible map[string]bool
	// openPlaceID is the one place this connection currently has open. It is a
	// delivery filter, never an authorization: it can only widen delivery to a
	// participant the event's fenced audience already listed as a watcher.
	openPlaceID string
	// deferred holds the handshake cursors for places this connection may read
	// but does not hold. They are not replayed at hello — that would make a
	// thread the viewer merely visited ambient again — and are flushed only if
	// this connection declares that place open.
	deferred map[string]int64
	// replayed is the one durable replay high-water mark for each place on this
	// connection. Both hello and open pass through it: an open immediately
	// following a hello must never replay the same durable frame again.
	replayed map[string]int64
	// caughtUp remembers the latest caught_up boundary already announced for a
	// place. It is intentionally distinct from replayed: catchUpLimit can make
	// the announced head lie beyond the frames put on this socket.
	caughtUp map[string]int64
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

// openPlace declares the one place this connection is looking at. A screen
// shows one place, so a later declaration replaces the earlier one and no
// client can accumulate watched places.
func (s *subscriber) openPlace(placeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.openPlaceID = placeID
}

// closePlace clears the declaration only when it still names the same place,
// so a close for the screen the viewer already left cannot cancel the one
// they moved to.
func (s *subscriber) closePlace(placeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.openPlaceID == placeID {
		s.openPlaceID = ""
	}
}

// deferCursor remembers a handshake cursor that was not replayed. The map is
// bounded by maxHelloCursors because it can only ever hold cursors the
// handshake already carried.
func (s *subscriber) deferCursor(placeID string, since int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deferred == nil {
		s.deferred = map[string]int64{}
	}
	s.deferred[placeID] = since
}

// takeDeferredCursor consumes the cursor for one place. It is one-shot: a
// later close drops the client's own cursor for a place it does not hold, so
// re-opening in the same connection must not replay the same stretch again.
func (s *subscriber) takeDeferredCursor(placeID string) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	since, ok := s.deferred[placeID]
	if ok {
		delete(s.deferred, placeID)
	}
	return since, ok
}

// replaySince returns the shared high-water mark for a replay request. A
// client can advance it with a newer cursor obtained from REST, but neither
// hello nor open can move it backwards and replay frames already sent here.
func (s *subscriber) replaySince(placeID string, since int64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if replayed := s.replayed[placeID]; replayed > since {
		return replayed
	}
	if since > s.replayed[placeID] {
		s.replayed[placeID] = since
	}
	return since
}

// markReplayed advances one connection's place high-water only after that
// message has been accepted for delivery by the subscriber queue.
func (s *subscriber) markReplayed(placeID string, seq int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq > s.replayed[placeID] {
		s.replayed[placeID] = seq
	}
}

// markCaughtUp returns whether this exact/newer durable head still needs its
// caught_up frame. A same-head open after hello is an acknowledgement only,
// never a second client-side resync trigger.
func (s *subscriber) markCaughtUp(placeID string, latestSeq int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	// The zero sequence is a valid durable head for an empty place. A missing
	// map entry is not an already-announced zero boundary: the first replay
	// still owes caught_up{latest_seq:0} to its completion waiter.
	if announced, ok := s.caughtUp[placeID]; ok && latestSeq <= announced {
		return false
	}
	s.caughtUp[placeID] = latestSeq
	return true
}

func (s *subscriber) watching(placeID string) bool {
	if placeID == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.openPlaceID == placeID
}

// Hub fans messaging events out to live subscribers. REST mutations and WS
// mutations publish through the same hub, so a message sent over HTTP still
// reaches every open socket. Delivery is best-effort (凍結契約 v1: 候補の
// 配送はbest-effort、正本はstoreにあり、落ちてもseqから再構成できる) — a
// subscriber whose buffer overflows is dropped and reconnects with cursors.
type Hub struct {
	authorizer hubAuthorizer

	mu          sync.Mutex
	subscribers map[*subscriber]struct{}
}

// hubAuthorizer is the narrow adapter from in-memory fanout to Messaging's
// authorization model. The callback runs while one Workspace/install/place
// shared lease is held, so membership and app lifecycle changes cannot split
// one logical event into subscriber-by-subscriber authority snapshots.
type hubAuthorizer interface {
	withLiveAudience(
		context.Context,
		Scope,
		liveBoundary,
		bool,
		func(liveAudience) error,
	) error
}

// NewHub returns a hub that obtains current authorized participant sets from
// the messaging store before in-memory fanout.
func NewHub(store *Store) *Hub {
	return newHub(store)
}

func newHub(authorizer hubAuthorizer) *Hub {
	return &Hub{authorizer: authorizer, subscribers: map[*subscriber]struct{}{}}
}

func (h *Hub) subscribe(scope any) *subscriber {
	var viewer ParticipantRef
	var store *ScopedStore
	switch value := scope.(type) {
	case ParticipantRef:
		viewer = value // test-only store-less subscriber
	case *ScopedStore:
		store, viewer = value, value.Scope.Actor
	default:
		panic("messaging subscriber requires ParticipantRef or ScopedStore")
	}
	sub := &subscriber{
		viewer: viewer,
		store:  store,
		// Enough headroom for a busy place; overflow means the reader is not
		// keeping up and replay-on-reconnect is the correct recovery.
		send:     make(chan outboundFrame, 256),
		done:     make(chan struct{}),
		visible:  map[string]bool{},
		replayed: map[string]int64{},
		caughtUp: map[string]int64{},
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
func (h *Hub) Publish(ctx context.Context, event Event) error {
	return h.publishVariants(ctx, Scope{}, false, []Event{event})
}

// PublishScoped projects an event from one exact installed Messaging app.
// Production hubs require this form; the unscoped form is retained only for
// store-less fanout harnesses.
func (h *Hub) PublishScoped(ctx context.Context, store *ScopedStore, event Event) error {
	if store == nil {
		return ErrInvalidScope
	}
	return h.publishVariants(ctx, store.Scope, false, []Event{event})
}

// PublishActorScoped publishes a fresh volatile actor operation. Unlike a
// post-commit durable projection, the actor's exact Workspace/place authority
// is checked in the same transaction that resolves and enqueues the immutable
// current audience.
func (h *Hub) PublishActorScoped(ctx context.Context, store *ScopedStore, event Event) error {
	if store == nil {
		return ErrInvalidScope
	}
	return h.publishVariants(ctx, store.Scope, true, []Event{event})
}

// PublishSystemScoped carries trusted server-side projections, such as a
// verified LiveKit webhook, through the current installation/place audience.
func (h *Hub) PublishSystemScoped(ctx context.Context, scope Scope, event Event) error {
	if err := scope.validateAddress(); err != nil {
		return err
	}
	return h.publishVariants(ctx, scope, false, []Event{event})
}

// PublishVariants delivers mutually exclusive recipient variants of one
// logical event. Current authorization is resolved once for the shared scope,
// then OnlyFor/ExceptFor partitions the already-authorized subscribers fully
// in memory. A subscriber receives at most one variant.
func (h *Hub) PublishVariants(ctx context.Context, events []Event) error {
	return h.publishVariants(ctx, Scope{}, false, events)
}

// PublishVariantsScoped is PublishScoped for mutually exclusive payload
// variants. Authorization is still resolved exactly once for the whole set.
func (h *Hub) PublishVariantsScoped(ctx context.Context, store *ScopedStore, events []Event) error {
	if store == nil {
		return ErrInvalidScope
	}
	return h.publishVariants(ctx, store.Scope, false, events)
}

func (h *Hub) publishVariants(
	ctx context.Context,
	scope Scope,
	requireActor bool,
	events []Event,
) error {
	if h == nil {
		return nil
	}
	if len(events) == 0 {
		return nil
	}
	boundary, ok := eventScope(events[0])
	if !ok {
		return ErrInvalidScope
	}
	frames := make([][]byte, len(events))
	onlyFor := make(map[ParticipantRef]int, len(events))
	fallbacks := make([]int, 0, 1)
	excludedByEvent := make([]map[ParticipantRef]struct{}, len(events))
	for i, event := range events {
		candidateScope, valid := eventScope(event)
		if !valid || candidateScope != boundary {
			return ErrInvalidScope
		}
		frame, err := json.Marshal(struct {
			Type  string `json:"type"`
			Event Event  `json:"event"`
		}{Type: "event", Event: event})
		if err != nil {
			return err
		}
		frames[i] = frame
		if event.OnlyFor != nil {
			if _, duplicate := onlyFor[*event.OnlyFor]; duplicate {
				return ErrInvalidScope
			}
			onlyFor[*event.OnlyFor] = i
			continue
		}
		fallbacks = append(fallbacks, i)
		if len(event.ExceptFor) > 0 {
			excluded := make(map[ParticipantRef]struct{}, len(event.ExceptFor))
			for _, participant := range event.ExceptFor {
				excluded[participant] = struct{}{}
			}
			excludedByEvent[i] = excluded
		}
	}

	fanout := func(authorized liveAudience) error {
		h.mu.Lock()
		subs := make([]*subscriber, 0, len(h.subscribers))
		for sub := range h.subscribers {
			subs = append(subs, sub)
		}
		h.mu.Unlock()

		var drop []*subscriber
		for _, sub := range subs {
			// A Participant may have sockets open in several Workspaces (or an
			// old socket sealed to a pre-reinstall installation). Audience
			// membership is meaningful only inside the event's exact app address.
			// This is an in-memory equality check over sealed server state, not a
			// subscriber-by-subscriber authorization query.
			if scope.WorkspaceID != "" && (sub.store == nil ||
				sub.store.Scope.WorkspaceID != scope.WorkspaceID ||
				sub.store.Scope.InstallationID != scope.InstallationID ||
				sub.store.Scope.AuthorityEpoch != scope.AuthorityEpoch) {
				continue
			}
			visible := false
			if h.authorizer == nil {
				visible, _ = sub.visibility(boundary.key())
			} else {
				visible = authorized.admits(sub.viewer, sub.watching(boundary.placeID))
			}
			sub.markVisible(boundary.key(), visible)
			if !visible {
				continue
			}
			variant, found := onlyFor[sub.viewer]
			if !found {
				for _, candidate := range fallbacks {
					if _, excluded := excludedByEvent[candidate][sub.viewer]; excluded {
						continue
					}
					variant, found = candidate, true
					break
				}
			}
			if !found {
				continue
			}
			select {
			case <-sub.done:
			case sub.send <- outboundFrame{payload: frames[variant], boundary: boundary}:
			default:
				drop = append(drop, sub)
			}
		}
		for _, sub := range drop {
			h.unsubscribe(sub)
		}
		return nil
	}
	if h.authorizer != nil {
		return h.authorizer.withLiveAudience(ctx, scope, boundary, requireActor, fanout)
	}
	return fanout(liveAudience{})
}

func eventScope(event Event) (liveBoundary, bool) {
	if event.PlaceID != "" {
		if event.Subject != nil {
			return liveBoundary{}, false
		}
		return liveBoundary{placeID: event.PlaceID}, true
	}
	if event.Subject != nil {
		return liveBoundary{subject: *event.Subject, subjectSet: true}, true
	}
	return liveBoundary{}, false
}
