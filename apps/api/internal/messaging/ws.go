package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

// catchUpLimit bounds how many messages one place replays after hello. A
// client further behind than this sees caught_up.latest_seq beyond its replay
// and backfills over REST history — the socket is a live wire, not the
// archive.
const catchUpLimit = 500

// maxHelloCursors bounds the hello frame so a hostile client cannot turn the
// handshake into an unbounded scan.
const maxHelloCursors = 1024

// WSServer is the /messaging/ws boundary: one socket per session multiplexes
// every place the participant can see (契約ドラフト: WS 1本で全Workspace/place
// をmultiplex)。Authentication is identical to the REST surface. Frames:
//
//	client → server: hello{cursors}, send{...}, typing{place_id}
//	server → client: hello_ack, event{...}, caught_up{place_id, latest_seq},
//	                 receipt{client_nonce, message_id, seq, created},
//	                 error{code, client_nonce?}
//
// Durable truth stays in the store; the socket only carries it. A dropped or
// slow connection loses nothing — reconnect with cursors replays from seq.
type WSServer struct {
	Store          *Store
	Sessions       agentevents.UserSessionAuthorizer
	Hub            *Hub
	AllowedOrigins []string

	HelloTimeout time.Duration
	WriteTimeout time.Duration
	PongWait     time.Duration
	PingInterval time.Duration
	MaxReadLimit int64

	upgrader websocket.Upgrader
}

// NewWSServer returns the messaging WebSocket server.
func NewWSServer(store *Store, sessions agentevents.UserSessionAuthorizer, hub *Hub) *WSServer {
	s := &WSServer{
		Store:        store,
		Sessions:     sessions,
		Hub:          hub,
		HelloTimeout: 10 * time.Second,
		WriteTimeout: 10 * time.Second,
		PongWait:     60 * time.Second,
		PingInterval: 25 * time.Second,
		MaxReadLimit: maxRequestBytes,
	}
	s.upgrader = websocket.Upgrader{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		CheckOrigin: func(r *http.Request) bool {
			return agentevents.BrowserOriginAllowed(r, s.AllowedOrigins)
		},
	}
	return s
}

type wsHello struct {
	Type string `json:"type"`
	// Cursors maps place_id → last seq the client has. Only listed places are
	// replayed; everything else arrives live or over REST.
	Cursors map[string]int64 `json:"cursors"`
}

type wsClientFrame struct {
	Type        string `json:"type"`
	PlaceID     string `json:"place_id"`
	Content     string `json:"content"`
	Urgency     string `json:"urgency"`
	ReplyTo     string `json:"reply_to"`
	ClientNonce string `json:"client_nonce"`
}

func (s *WSServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !agentevents.BrowserOriginAllowed(r, s.AllowedOrigins) {
		writeError(w, http.StatusForbidden, "origin_not_allowed")
		return
	}
	cookies := r.CookiesNamed(agentevents.BrowserSessionCookie)
	if len(cookies) != 1 || s.Sessions == nil {
		writeError(w, http.StatusUnauthorized, "missing_session")
		return
	}
	claims, err := s.Sessions.VerifySession(r.Context(), cookies[0].Value)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid_session")
		return
	}
	viewer := Human(claims.HumanID)
	if err := viewer.Validate(); err != nil {
		writeError(w, http.StatusUnauthorized, "invalid_session")
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.SetReadLimit(s.MaxReadLimit)

	// Hello first, under its own deadline.
	_ = conn.SetReadDeadline(time.Now().Add(s.HelloTimeout))
	var hello wsHello
	if err := conn.ReadJSON(&hello); err != nil || hello.Type != "hello" || len(hello.Cursors) > maxHelloCursors {
		s.writeControlError(conn, "invalid_hello", "")
		return
	}

	sub := s.Hub.subscribe(viewer)
	defer s.Hub.unsubscribe(sub)

	// The writer owns the socket's write side: hub frames, receipts, pings.
	writerDone := make(chan struct{})
	go s.writePump(conn, sub, writerDone)

	if !s.enqueueJSON(sub, map[string]string{"type": "hello_ack"}) {
		return
	}
	if !s.catchUp(r.Context(), sub, hello.Cursors) {
		return
	}

	_ = conn.SetReadDeadline(time.Now().Add(s.PongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(s.PongWait))
	})
	s.readPump(r.Context(), conn, sub, claims)
	<-writerDone
}

// catchUp replays durable messages after each cursor and closes each place
// with caught_up{latest_seq} so the client can detect a replay gap.
func (s *WSServer) catchUp(ctx context.Context, sub *subscriber, cursors map[string]int64) bool {
	for placeID, since := range cursors {
		place, err := s.Store.PlaceFor(ctx, placeID, sub.viewer)
		if err != nil {
			// Not visible (or gone): the cursor is silently dropped. The
			// place's existence is not revealed on this path either.
			sub.markVisible(placeID, false)
			continue
		}
		sub.markVisible(placeID, true)
		messages, err := s.Store.MessagesSince(ctx, placeID, sub.viewer, since, catchUpLimit)
		if err != nil {
			return false
		}
		for _, m := range messages {
			event := Event{Type: EventMessageCreated, PlaceID: placeID}
			wire := messageToWire(place, m)
			event.Message = &wire
			if !s.enqueueJSON(sub, struct {
				Type  string `json:"type"`
				Event Event  `json:"event"`
			}{Type: "event", Event: event}) {
				return false
			}
		}
		if !s.enqueueJSON(sub, struct {
			Type      string `json:"type"`
			PlaceID   string `json:"place_id"`
			LatestSeq int64  `json:"latest_seq"`
		}{Type: "caught_up", PlaceID: placeID, LatestSeq: place.LastSeq}) {
			return false
		}
	}
	return true
}

func (s *WSServer) readPump(ctx context.Context, conn *websocket.Conn, sub *subscriber, claims agentevents.UserSessionClaims) {
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(s.PongWait))
		var frame wsClientFrame
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&frame); err != nil {
			s.enqueueError(sub, "invalid_frame", "")
			return
		}
		switch frame.Type {
		case "send":
			s.handleSend(ctx, sub, claims, frame)
		case "typing":
			s.handleTyping(ctx, sub, frame)
		default:
			s.enqueueError(sub, "unknown_frame", "")
			return
		}
	}
}

func (s *WSServer) handleSend(ctx context.Context, sub *subscriber, claims agentevents.UserSessionClaims, frame wsClientFrame) {
	switch frame.Urgency {
	case "", UrgencyUrgent, UrgencyNormal, UrgencyFYI:
	default:
		s.enqueueError(sub, "invalid_urgency", frame.ClientNonce)
		return
	}
	if frame.Content == "" || len(frame.Content) > MaxContentBytes {
		s.enqueueError(sub, "invalid_content", frame.ClientNonce)
		return
	}
	if frame.ClientNonce == "" || len(frame.ClientNonce) > 128 {
		s.enqueueError(sub, "invalid_client_nonce", frame.ClientNonce)
		return
	}
	place, err := s.Store.PlaceFor(ctx, frame.PlaceID, sub.viewer)
	if err != nil {
		s.enqueueError(sub, storeErrorCode(err), frame.ClientNonce)
		return
	}
	var (
		msg     Message
		created bool
	)
	called := false
	err = s.Sessions.AuthorizeSession(ctx, claims, func() error {
		called = true
		var opErr error
		msg, created, opErr = s.Store.AppendMessage(ctx, AppendInput{
			PlaceID: frame.PlaceID, Author: sub.viewer, Content: frame.Content,
			Urgency: frame.Urgency, ReplyTo: frame.ReplyTo, ClientNonce: frame.ClientNonce,
		})
		return opErr
	})
	if !called {
		s.enqueueError(sub, "invalid_session", frame.ClientNonce)
		return
	}
	if err != nil {
		s.enqueueError(sub, storeErrorCode(err), frame.ClientNonce)
		return
	}
	// Receipt to the sender first, then fan-out (the sender also receives the
	// event and reconciles by client_nonce).
	s.enqueueJSON(sub, struct {
		Type        string `json:"type"`
		ClientNonce string `json:"client_nonce"`
		MessageID   string `json:"message_id"`
		Seq         int64  `json:"seq"`
		Created     bool   `json:"created"`
	}{Type: "receipt", ClientNonce: frame.ClientNonce, MessageID: msg.MessageID, Seq: msg.Seq, Created: created})
	if created {
		wire := messageToWire(place, msg)
		s.Hub.Publish(ctx, Event{Type: EventMessageCreated, PlaceID: frame.PlaceID, Message: &wire})
	}
}

func (s *WSServer) handleTyping(ctx context.Context, sub *subscriber, frame wsClientFrame) {
	// Volatile and best-effort: no receipt, no replay. Visibility is still
	// checked so typing cannot probe places.
	if _, err := s.Store.PlaceFor(ctx, frame.PlaceID, sub.viewer); err != nil {
		return
	}
	actor := participantToWire(sub.viewer)
	s.Hub.Publish(ctx, Event{Type: EventTyping, PlaceID: frame.PlaceID, Actor: &actor})
}

func (s *WSServer) writePump(conn *websocket.Conn, sub *subscriber, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(s.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sub.done:
			_ = conn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseGoingAway, ""), time.Now().Add(s.WriteTimeout))
			_ = conn.Close()
			return
		case frame := <-sub.send:
			_ = conn.SetWriteDeadline(time.Now().Add(s.WriteTimeout))
			if err := conn.WriteMessage(websocket.TextMessage, frame); err != nil {
				_ = conn.Close()
				return
			}
		case <-ticker.C:
			_ = conn.SetWriteDeadline(time.Now().Add(s.WriteTimeout))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				_ = conn.Close()
				return
			}
		}
	}
}

// enqueueJSON queues a frame for the writer. False means the subscriber is
// gone (buffer overflow or unsubscribed) and the caller should stop.
func (s *WSServer) enqueueJSON(sub *subscriber, body any) bool {
	frame, err := json.Marshal(body)
	if err != nil {
		return false
	}
	select {
	case <-sub.done:
		return false
	case sub.send <- frame:
		return true
	default:
		s.Hub.unsubscribe(sub)
		return false
	}
}

func (s *WSServer) enqueueError(sub *subscriber, code, clientNonce string) {
	s.enqueueJSON(sub, struct {
		Type        string `json:"type"`
		Code        string `json:"code"`
		ClientNonce string `json:"client_nonce,omitempty"`
	}{Type: "error", Code: code, ClientNonce: clientNonce})
}

// writeControlError reports a pre-subscription failure directly on the socket.
func (s *WSServer) writeControlError(conn *websocket.Conn, code, clientNonce string) {
	frame, err := json.Marshal(struct {
		Type        string `json:"type"`
		Code        string `json:"code"`
		ClientNonce string `json:"client_nonce,omitempty"`
	}{Type: "error", Code: code, ClientNonce: clientNonce})
	if err != nil {
		return
	}
	_ = conn.SetWriteDeadline(time.Now().Add(s.WriteTimeout))
	_ = conn.WriteMessage(websocket.TextMessage, frame)
}

// storeErrorCode mirrors writeStoreError for the WS error frame.
func storeErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrPlaceNotFound), errors.Is(err, ErrMessageNotFound),
		errors.Is(err, ErrWorkspaceNotFound), errors.Is(err, ErrParticipantNotFound):
		return "not_found"
	case errors.Is(err, ErrNotAMember):
		return "not_a_member"
	case errors.Is(err, ErrNotAuthor):
		return "not_author"
	case errors.Is(err, ErrForbidden):
		return "forbidden"
	case errors.Is(err, ErrNotReachable):
		return "not_reachable"
	case errors.Is(err, ErrMessageDeleted):
		return "message_deleted"
	case errors.Is(err, ErrSeqBeyondLatest):
		return "seq_beyond_latest"
	default:
		return "internal"
	}
}
