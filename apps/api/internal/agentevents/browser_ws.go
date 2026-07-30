package agentevents

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// BrowserServer is the browser-facing direct-chat WebSocket boundary. It never
// accepts an agent bearer token or public target: the signed HttpOnly session
// supplies the target and authenticated provenance.
type BrowserServer struct {
	Sessions UserSessionAuthorizer
	Appender CommandAppender
	Events   *DurableGateway

	AllowedOrigins []string
	HelloTimeout   time.Duration
	WriteTimeout   time.Duration
	PongWait       time.Duration
	PingInterval   time.Duration
	MaxReadLimit   int64

	upgrader      websocket.Upgrader
	connectionsMu sync.Mutex
	connections   map[*websocket.Conn]browserConnection
	accepted      uint64
}

type browserConnection struct {
	sessionID string
	conn      *websocket.Conn
}

// BrowserConnectionStats is a point-in-time view of the browser WebSocket
// lifecycle. Accepted is monotonic for the lifetime of this server instance,
// which lets shutdown and reconnect checks distinguish a new connection from
// a transient UI state.
type BrowserConnectionStats struct {
	Active   int    `json:"active"`
	Accepted uint64 `json:"accepted"`
}

type browserHello struct {
	Type         string `json:"type"`
	LastEventSeq uint64 `json:"last_event_seq"`
}

type browserCommandFrame struct {
	Type           string          `json:"type"`
	IdempotencyKey string          `json:"idempotency_key"`
	Command        json.RawMessage `json:"command"`
}

type browserEventFrame struct {
	Type     string               `json:"type"`
	Envelope browserEventEnvelope `json:"envelope"`
}

type browserCommandAcceptedFrame struct {
	Type           string `json:"type"`
	IdempotencyKey string `json:"idempotency_key"`
	CommandID      string `json:"command_id"`
	Seq            uint64 `json:"seq"`
}

type browserCommandRejectedFrame struct {
	Type           string       `json:"type"`
	IdempotencyKey string       `json:"idempotency_key"`
	RejectReason   RejectReason `json:"reject_reason"`
}

type browserCommandReceipt struct {
	IdempotencyKey string `json:"idempotency_key"`
	CommandID      string `json:"command_id"`
	Seq            uint64 `json:"seq"`
}

type browserEventEnvelope struct {
	Seq   *uint64         `json:"seq,omitempty"`
	Event json.RawMessage `json:"event"`
}

type directChatStatusFrame struct {
	Type   string `json:"type"`
	Status string `json:"status"`
}

func (f *browserEventFrame) UnmarshalJSON(data []byte) error {
	if err := checkDuplicateKeys(data); err != nil {
		return fmt.Errorf("browser event frame: %w", err)
	}
	type wire struct {
		Type     string                `json:"type"`
		Envelope *browserEventEnvelope `json:"envelope"`
	}
	var decoded wire
	if err := unmarshalStrict(data, &decoded); err != nil {
		return err
	}
	if decoded.Type != "event" || decoded.Envelope == nil {
		return errors.New("browser event frame type must be event")
	}
	*f = browserEventFrame{Type: decoded.Type, Envelope: *decoded.Envelope}
	return nil
}

func (e *browserEventEnvelope) UnmarshalJSON(data []byte) error {
	if err := checkDuplicateKeys(data); err != nil {
		return fmt.Errorf("browser event envelope: %w", err)
	}
	type rawEnvelope struct {
		Seq   json.RawMessage `json:"seq"`
		Event json.RawMessage `json:"event"`
	}
	var raw rawEnvelope
	if err := unmarshalStrict(data, &raw); err != nil {
		return err
	}
	if len(raw.Event) == 0 || !json.Valid(raw.Event) {
		return errors.New("browser event envelope requires a valid event")
	}
	if err := validateEvent(raw.Event); err != nil {
		return err
	}
	volatile := volatileEventTypes[eventType(raw.Event)]
	var parsedSeq *uint64
	switch {
	case raw.Seq == nil:
		if !volatile {
			return errors.New("durable browser event requires seq")
		}
	case bytes.Equal(bytes.TrimSpace(raw.Seq), []byte("null")):
		return errors.New("browser event seq must not be null")
	default:
		var seq uint64
		if err := json.Unmarshal(raw.Seq, &seq); err != nil {
			return fmt.Errorf("browser event seq: %w", err)
		}
		if seq > maxJSONSafeInteger {
			return errors.New("browser event seq exceeds JSON-safe integer range")
		}
		if volatile {
			return errors.New("volatile browser event must not have seq")
		}
		parsedSeq = &seq
	}
	*e = browserEventEnvelope{Seq: parsedSeq, Event: raw.Event}
	return nil
}

func (f *browserCommandAcceptedFrame) UnmarshalJSON(data []byte) error {
	if err := checkDuplicateKeys(data); err != nil {
		return fmt.Errorf("browser command accepted frame: %w", err)
	}
	type wire browserCommandAcceptedFrame
	var decoded wire
	if err := unmarshalStrict(data, &decoded); err != nil {
		return err
	}
	value := browserCommandAcceptedFrame(decoded)
	if value.Type != "command_accepted" ||
		value.IdempotencyKey == "" ||
		len(value.IdempotencyKey) > MaxIdempotencyKeyBytes ||
		!canonicalUUIDRegexp.MatchString(value.CommandID) ||
		value.Seq > maxJSONSafeInteger {
		return errors.New("invalid browser command accepted frame")
	}
	*f = value
	return nil
}

func (f *browserCommandRejectedFrame) UnmarshalJSON(data []byte) error {
	if err := checkDuplicateKeys(data); err != nil {
		return fmt.Errorf("browser command rejected frame: %w", err)
	}
	type wire browserCommandRejectedFrame
	var decoded wire
	if err := unmarshalStrict(data, &decoded); err != nil {
		return err
	}
	value := browserCommandRejectedFrame(decoded)
	if value.Type != "command_rejected" ||
		value.IdempotencyKey == "" ||
		len(value.IdempotencyKey) > MaxIdempotencyKeyBytes ||
		!validBrowserRejectReason(value.RejectReason) {
		return errors.New("invalid browser command rejected frame")
	}
	*f = value
	return nil
}

func (f *directChatStatusFrame) UnmarshalJSON(data []byte) error {
	if err := checkDuplicateKeys(data); err != nil {
		return fmt.Errorf("direct chat status frame: %w", err)
	}
	type wire directChatStatusFrame
	var decoded wire
	if err := unmarshalStrict(data, &decoded); err != nil {
		return err
	}
	value := directChatStatusFrame(decoded)
	if value.Type != "direct_chat_status" || (value.Status != "ready" && value.Status != "unavailable") {
		return errors.New("invalid direct chat status frame")
	}
	*f = value
	return nil
}

func validBrowserRejectReason(reason RejectReason) bool {
	switch reason {
	case RejectUnknownCommand,
		RejectSchemaViolation,
		RejectAttachmentsNotEmpty,
		RejectOversized,
		RejectNotAllowed,
		RejectIdempotencyConflict,
		RejectUnavailable:
		return true
	default:
		return false
	}
}

type browserCommandHead struct {
	Type      string `json:"type"`
	RequestID string `json:"request_id"`
}

func NewBrowserServer(sessions UserSessionAuthorizer, appender CommandAppender, events *DurableGateway) *BrowserServer {
	s := &BrowserServer{
		Sessions:     sessions,
		Appender:     appender,
		Events:       events,
		HelloTimeout: 10 * time.Second,
		WriteTimeout: 10 * time.Second,
		MaxReadLimit: MaxUserCommandBytes + 16*1024,
	}
	s.upgrader = websocket.Upgrader{CheckOrigin: s.checkOrigin}
	s.connections = make(map[*websocket.Conn]browserConnection)
	return s
}

func (s *BrowserServer) checkCommandState(personalityAgentID string, head browserCommandHead) (RejectReason, bool) {
	if s.Events == nil {
		return "", false
	}
	switch head.Type {
	case "abort":
		if !s.Events.IsRunInFlight(personalityAgentID) {
			return RejectNotAllowed, true
		}
	case "approval_decision":
		if !s.Events.IsApprovalPending(personalityAgentID, head.RequestID) {
			return RejectNotAllowed, true
		}
	}
	return "", false
}

func (s *BrowserServer) checkOrigin(r *http.Request) bool {
	return browserOriginAllowed(r, s.AllowedOrigins)
}

// ServeHTTP implements targetless GET /direct-chat/ws. Browser
// authentication happens before upgrade so a rejected session cannot consume a
// WebSocket or leak whether an agent connection exists.
func (s *BrowserServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.Sessions == nil || s.Appender == nil || s.Events == nil {
		http.Error(w, "browser websocket not configured", http.StatusServiceUnavailable)
		return
	}
	cookie, err := uniqueBrowserSessionCookie(r)
	if err != nil {
		if errors.Is(err, errBrowserSessionDuplicate) {
			http.Error(w, "duplicate session cookies", http.StatusBadRequest)
			return
		}
		http.Error(w, "missing session", http.StatusUnauthorized)
		return
	}
	claims, err := s.Sessions.VerifySession(r.Context(), cookie.Value)
	if err != nil {
		http.Error(w, "invalid session", http.StatusUnauthorized)
		return
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	if err := s.Sessions.AuthorizeSession(r.Context(), claims, func() error {
		s.addConnection(conn, claims.sessionID)
		return nil
	}); err != nil {
		_ = conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "session unavailable"),
			time.Now().Add(s.writeTimeout()),
		)
		_ = conn.Close()
		return
	}
	defer s.removeConnection(conn)
	defer conn.Close()
	if err := s.run(r.Context(), conn, claims); err != nil && !errors.Is(err, context.Canceled) {
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "browser gateway closed"), time.Now().Add(s.writeTimeout()))
	}
}

// CloseBrowserConnections is a bounded lifecycle operation for server
// shutdown/replacement. Reconnecting browsers retain their durable cursor and
// replay from the authoritative event log; no event is synthesized here.
func (s *BrowserServer) CloseBrowserConnections() {
	s.connectionsMu.Lock()
	connections := make([]*websocket.Conn, 0, len(s.connections))
	for conn := range s.connections {
		connections = append(connections, conn)
	}
	s.connectionsMu.Unlock()
	for _, conn := range connections {
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, "browser gateway reconnect"), time.Now().Add(s.writeTimeout()))
		_ = conn.Close()
	}
}

// CloseBrowserSession terminates only live sockets authorized by the matching
// process-local session. RevokeSession prevents a concurrent post-upgrade
// registration from escaping this close barrier.
func (s *BrowserServer) CloseBrowserSession(sessionID string) {
	if !validBrowserSessionID(sessionID) {
		return
	}
	s.connectionsMu.Lock()
	connections := make([]*websocket.Conn, 0)
	for _, connection := range s.connections {
		if connection.sessionID == sessionID {
			connections = append(connections, connection.conn)
		}
	}
	s.connectionsMu.Unlock()
	for _, conn := range connections {
		_ = conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "session ended"),
			time.Now().Add(s.writeTimeout()),
		)
		_ = conn.Close()
	}
}

func (s *BrowserServer) ConnectionStats() BrowserConnectionStats {
	s.connectionsMu.Lock()
	defer s.connectionsMu.Unlock()
	return BrowserConnectionStats{
		Active:   len(s.connections),
		Accepted: s.accepted,
	}
}

func (s *BrowserServer) addConnection(conn *websocket.Conn, sessionID string) {
	s.connectionsMu.Lock()
	defer s.connectionsMu.Unlock()
	s.connections[conn] = browserConnection{sessionID: sessionID, conn: conn}
	s.accepted++
}

func (s *BrowserServer) removeConnection(conn *websocket.Conn) {
	s.connectionsMu.Lock()
	defer s.connectionsMu.Unlock()
	delete(s.connections, conn)
}

func (s *BrowserServer) run(ctx context.Context, conn *websocket.Conn, claims UserSessionClaims) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if s.MaxReadLimit > 0 {
		conn.SetReadLimit(s.MaxReadLimit)
	}
	if err := conn.SetReadDeadline(s.sessionReadDeadline(claims, s.helloTimeout())); err != nil {
		return err
	}
	_, raw, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("read browser hello: %w", err)
	}
	hello, err := decodeBrowserHello(raw)
	if err != nil {
		return err
	}

	if err := s.Events.EnsureAgentSessionStateRebuilt(ctx, claims.PersonalityAgentID); err != nil {
		return fmt.Errorf("rebuild agent session state: %w", err)
	}

	// Install pong handler before first read and schedule the initial
	// keepalive deadline. Pongs from the browser reset the deadline for the
	// next PongWait interval.
	if s.pongWait() > 0 {
		conn.SetPongHandler(func(string) error {
			_ = conn.SetReadDeadline(s.sessionReadDeadline(claims, s.pongWait()))
			return nil
		})
		if err := conn.SetReadDeadline(s.sessionReadDeadline(claims, s.pongWait())); err != nil {
			return err
		}
	}

	// Send periodic pings so the browser does not time out. The browser
	// WebSocket implementation answers control pings automatically with pongs.
	if s.pingInterval() > 0 {
		ticker := time.NewTicker(s.pingInterval())
		defer ticker.Stop()
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(s.writeTimeout())); err != nil {
						cancel()
						return
					}
				}
			}
		}()
	}

	volatile, unsubscribe := s.Events.SubscribeBrowserVolatile(claims.PersonalityAgentID)
	defer unsubscribe()
	var writeMu sync.Mutex
	write := func(frame any) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		if err := conn.SetWriteDeadline(time.Now().Add(s.writeTimeout())); err != nil {
			return err
		}
		return conn.WriteJSON(frame)
	}
	// Subscribe before replay so volatile traffic produced during catch-up stays
	// queued, then validate and emit the complete durable suffix synchronously.
	// Status is the browser's command-admission barrier, so neither it nor the
	// read pump may start while replay can still fail.
	next, err := s.browserDurableCatchUp(
		ctx,
		claims.PersonalityAgentID,
		hello.LastEventSeq,
		write,
	)
	if err != nil {
		return err
	}
	// Replay may block on the durable log, so sample readiness only after it
	// completes instead of publishing a status captured before the barrier.
	ready, err := s.Events.IsPersonalityAgentReady(ctx, claims.PersonalityAgentID)
	if err != nil {
		return fmt.Errorf("read direct-chat readiness: %w", err)
	}
	if err := write(directChatStatusFrame{Type: "direct_chat_status", Status: readinessStatus(ready)}); err != nil {
		return err
	}

	writerErr := make(chan error, 1)
	go func() {
		err := s.browserEventPump(ctx, claims.PersonalityAgentID, next, ready, volatile, write)
		writerErr <- err
		if err != nil && !errors.Is(err, context.Canceled) {
			cancel()
			// A durable replay or bounded live-queue failure must terminate the
			// blocked reader too; otherwise this browser session would remain
			// half-open until the peer happened to send another frame.
			_ = conn.SetReadDeadline(time.Now())
		}
	}()

	readErr := s.browserReadPump(ctx, conn, claims, write)
	cancel()
	writerResult := <-writerErr
	if readErr != nil && !errors.Is(readErr, context.Canceled) {
		return readErr
	}
	return writerResult
}

func (s *BrowserServer) browserEventPump(ctx context.Context, personalityAgentID string, lastConsumed uint64, ready bool, volatile <-chan Envelope, write func(any) error) error {
	next := lastConsumed
	ticker := time.NewTicker(s.Events.pollInterval())
	defer ticker.Stop()
	for {
		var err error
		next, err = s.browserDurableCatchUp(ctx, personalityAgentID, next, write)
		if err != nil {
			return err
		}
		select {
		case envelope, ok := <-volatile:
			if !ok {
				return errors.New("browser volatile event queue exhausted")
			}
			if envelope.PersonalityAgentID != personalityAgentID {
				return errors.New("browser volatile event target mismatch")
			}
			projected, err := projectBrowserEvent(envelope)
			if err != nil {
				return fmt.Errorf("project volatile browser event: %w", err)
			}
			if err := write(browserEventFrame{Type: "event", Envelope: projected}); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			current, err := s.Events.IsPersonalityAgentReady(ctx, personalityAgentID)
			if err != nil {
				return fmt.Errorf("poll direct-chat readiness: %w", err)
			}
			if current != ready {
				ready = current
				if err := write(directChatStatusFrame{Type: "direct_chat_status", Status: readinessStatus(ready)}); err != nil {
					return err
				}
			}
		}
	}
}

func (s *BrowserServer) browserDurableCatchUp(
	ctx context.Context,
	personalityAgentID string,
	lastConsumed uint64,
	write func(any) error,
) (uint64, error) {
	durable, err := s.Events.EventCatchUp(ctx, personalityAgentID, lastConsumed)
	if err != nil {
		return lastConsumed, fmt.Errorf("browser durable event catch-up: %w", err)
	}
	next := lastConsumed
	for _, envelope := range durable {
		if envelope.Seq == nil {
			return next, errors.New("durable replay returned a volatile event")
		}
		if envelope.PersonalityAgentID != personalityAgentID {
			return next, errors.New("browser event target mismatch")
		}
		projected, err := projectBrowserEvent(envelope)
		if err != nil {
			return next, fmt.Errorf("project durable browser event: %w", err)
		}
		if err := write(browserEventFrame{Type: "event", Envelope: projected}); err != nil {
			return next, err
		}
		next = *envelope.Seq
	}
	return next, nil
}

func (s *BrowserServer) browserReadPump(ctx context.Context, conn *websocket.Conn, claims UserSessionClaims, write func(any) error) error {
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		if s.pongWait() > 0 {
			if err := conn.SetReadDeadline(s.sessionReadDeadline(claims, s.pongWait())); err != nil {
				return err
			}
		}
		frame, err := decodeBrowserCommand(raw)
		if err != nil {
			return err
		}
		reason, err := validateBrowserCommand(frame.Command)
		if err != nil {
			if writeErr := write(browserCommandRejectedFrame{Type: "command_rejected", IdempotencyKey: frame.IdempotencyKey, RejectReason: reason}); writeErr != nil {
				return writeErr
			}
			continue
		}
		var head browserCommandHead
		if err := json.Unmarshal(frame.Command, &head); err != nil {
			if err := write(browserCommandRejectedFrame{Type: "command_rejected", IdempotencyKey: frame.IdempotencyKey, RejectReason: RejectSchemaViolation}); err != nil {
				return err
			}
			continue
		}
		if reason, reject := s.checkCommandState(claims.PersonalityAgentID, head); reject {
			if err := write(browserCommandRejectedFrame{Type: "command_rejected", IdempotencyKey: frame.IdempotencyKey, RejectReason: reason}); err != nil {
				return err
			}
			continue
		}
		var envelope CommandEnvelope
		operationCalled := false
		operationContext, cancelOperation := browserSessionOperationContext(ctx, claims)
		err = s.Sessions.AuthorizeSession(ctx, claims, func() error {
			operationCalled = true
			var appendErr error
			envelope, appendErr = s.Appender.Append(operationContext, directChatProvenance(claims), frame.IdempotencyKey, frame.Command)
			return appendErr
		})
		cancelOperation()
		if err != nil {
			if !operationCalled {
				return errors.New("browser session authority ended")
			}
			if errors.Is(err, errBrowserRuntimeUnavailable) {
				if writeErr := write(browserCommandRejectedFrame{
					Type:           "command_rejected",
					IdempotencyKey: frame.IdempotencyKey,
					RejectReason:   RejectUnavailable,
				}); writeErr != nil {
					return writeErr
				}
				continue
			}
			if isIdempotencyConflict(err) {
				if writeErr := write(browserCommandRejectedFrame{
					Type:           "command_rejected",
					IdempotencyKey: frame.IdempotencyKey,
					RejectReason:   RejectIdempotencyConflict,
				}); writeErr != nil {
					return writeErr
				}
				continue
			}
			return fmt.Errorf("append browser command: %w", err)
		}
		if err := write(browserCommandAcceptedFrame{
			Type:           "command_accepted",
			IdempotencyKey: frame.IdempotencyKey,
			CommandID:      envelope.CommandID,
			Seq:            envelope.Seq,
		}); err != nil {
			return err
		}
	}
}

func projectBrowserEvent(envelope Envelope) (browserEventEnvelope, error) {
	if err := ValidatePersonalityAgentID(envelope.PersonalityAgentID); err != nil {
		return browserEventEnvelope{}, err
	}
	event, err := projectEventArtifactReferences(envelope.Event, envelope.PersonalityAgentID)
	if err != nil {
		return browserEventEnvelope{}, err
	}
	return browserEventEnvelope{Seq: envelope.Seq, Event: event}, nil
}

func readinessStatus(ready bool) string {
	if ready {
		return "ready"
	}
	return "unavailable"
}

func decodeBrowserHello(raw []byte) (browserHello, error) {
	if err := checkDuplicateKeys(raw); err != nil {
		return browserHello{}, fmt.Errorf("browser hello: %w", err)
	}
	var hello browserHello
	if err := unmarshalStrict(raw, &hello); err != nil || hello.Type != "hello" || hello.LastEventSeq > maxJSONSafeInteger {
		return browserHello{}, errors.New("invalid browser hello")
	}
	return hello, nil
}

func decodeBrowserCommand(raw []byte) (browserCommandFrame, error) {
	if err := checkDuplicateKeys(raw); err != nil {
		return browserCommandFrame{}, fmt.Errorf("browser command: %w", err)
	}
	var frame browserCommandFrame
	if err := unmarshalStrict(raw, &frame); err != nil ||
		frame.Type != "command" ||
		frame.IdempotencyKey == "" ||
		len(frame.IdempotencyKey) > MaxIdempotencyKeyBytes ||
		len(frame.Command) == 0 {
		return browserCommandFrame{}, errors.New("invalid browser command frame")
	}
	if len(frame.Command) > MaxUserCommandBytes {
		return browserCommandFrame{}, errors.New("browser command exceeds limit")
	}
	return frame, nil
}

func validateBrowserCommand(raw json.RawMessage) (RejectReason, error) {
	if err := checkDuplicateKeys(raw); err != nil {
		return RejectSchemaViolation, err
	}
	var discriminator struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &discriminator); err != nil {
		return RejectSchemaViolation, err
	}
	if discriminator.Type == "user_message" {
		return validateUserCommand(raw)
	}
	if err := ValidateCommand(raw); err != nil {
		return RejectSchemaViolation, err
	}
	return "", nil
}

func (s *BrowserServer) helloTimeout() time.Duration {
	if s.HelloTimeout > 0 {
		return s.HelloTimeout
	}
	return 10 * time.Second
}

func (s *BrowserServer) writeTimeout() time.Duration {
	if s.WriteTimeout > 0 {
		return s.WriteTimeout
	}
	return 10 * time.Second
}

func (s *BrowserServer) pongWait() time.Duration {
	if s.PongWait > 0 {
		return s.PongWait
	}
	return 60 * time.Second
}

func (s *BrowserServer) pingInterval() time.Duration {
	if s.PingInterval > 0 {
		return s.PingInterval
	}
	return 54 * time.Second
}

func (s *BrowserServer) sessionReadDeadline(claims UserSessionClaims, interval time.Duration) time.Time {
	deadline := time.Now().Add(interval)
	if !claims.expiresAt.IsZero() && claims.expiresAt.Before(deadline) {
		return claims.expiresAt
	}
	return deadline
}
