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
	// Authorizer gates admission and every live private-data boundary on current
	// Employer-ship (私信 Surface, ADR 0009 §5). A nil Authorizer permits any
	// verified session.
	Authorizer DirectChatAuthorizer
	// Spawner optionally lazily starts the target agent runtime on connect
	// (ADR 0010). A nil Spawner assumes the agent is already running.
	Spawner DirectChatSpawner

	AllowedOrigins []string
	HelloTimeout   time.Duration
	WriteTimeout   time.Duration
	PongWait       time.Duration
	PingInterval   time.Duration
	// SpawnTimeout bounds lazy runtime provisioning without making the browser
	// request or socket the owner of the resulting runtime lifetime.
	SpawnTimeout time.Duration
	// AuthorizationPollInterval bounds how long an otherwise-idle socket can
	// retain stale Current-Employer authorization.
	AuthorizationPollInterval time.Duration
	MaxReadLimit              int64

	upgrader       websocket.Upgrader
	connectionsMu  sync.Mutex
	connections    map[*websocket.Conn]browserConnection
	accepted       uint64
	closing        bool
	beforeWrite    func()
	commandIngress *UserCommandIngress
}

// SetSpawner installs one lazy-runtime controller for both direct-chat
// transports. HTTP command admission waits for the newly spawned runtime's
// authenticated Ready publication before allocating a durable sequence.
func (s *BrowserServer) SetSpawner(spawner DirectChatSpawner) {
	s.Spawner = spawner
	if s.commandIngress != nil {
		s.commandIngress.Spawner = spawner
	}
}

type browserConnection struct {
	sessionID string
	conn      *websocket.Conn
}

type idempotencyAwareCommandAppender interface {
	AppendWithIdempotencyStatus(
		ctx context.Context,
		provenance DirectChatProvenance,
		idempotencyKey string,
		command json.RawMessage,
	) (CommandEnvelope, bool, error)
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
	Type           string          `json:"type"`
	IdempotencyKey string          `json:"idempotency_key"`
	CommandID      string          `json:"command_id"`
	Seq            uint64          `json:"seq"`
	Disposition    json.RawMessage `json:"disposition,omitempty"`
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
	if len(value.Disposition) != 0 {
		if err := validateEvent(value.Disposition); err != nil {
			return fmt.Errorf("browser command accepted disposition: %w", err)
		}
		if eventType(value.Disposition) != "command_disposition" {
			return errors.New("browser command accepted disposition must be command_disposition")
		}
		var correlation struct {
			CommandID  string `json:"command_id"`
			CommandSeq uint64 `json:"command_seq"`
		}
		if err := json.Unmarshal(value.Disposition, &correlation); err != nil {
			return fmt.Errorf("decode browser command accepted disposition correlation: %w", err)
		}
		if correlation.CommandID != value.CommandID || correlation.CommandSeq != value.Seq {
			return errors.New("browser command accepted disposition correlation mismatch")
		}
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
		Sessions:                  sessions,
		Appender:                  appender,
		Events:                    events,
		HelloTimeout:              10 * time.Second,
		WriteTimeout:              10 * time.Second,
		SpawnTimeout:              30 * time.Second,
		AuthorizationPollInterval: 5 * time.Second,
		MaxReadLimit:              MaxUserCommandBytes + 16*1024,
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

func (s *BrowserServer) authorizeDirectChat(
	ctx context.Context,
	claims UserSessionClaims,
	operation func() error,
) error {
	if operation == nil {
		return errors.New("browser direct-chat authorization operation is required")
	}
	if s.Authorizer == nil {
		return operation()
	}
	if err := s.Authorizer.AuthorizeDirectChat(
		ctx,
		claims.UserID,
		claims.PersonalityAgentID,
		operation,
	); err != nil {
		return fmt.Errorf("authorize browser direct chat: %w", err)
	}
	return nil
}

func (s *BrowserServer) authorizeBrowserOperation(
	ctx context.Context,
	claims UserSessionClaims,
	operation func() error,
) error {
	if operation == nil {
		return errors.New("browser authorization operation is required")
	}
	return s.Sessions.AuthorizeSession(ctx, claims, func() error {
		return s.authorizeDirectChat(ctx, claims, operation)
	})
}

// ServeHTTP implements targetless GET /direct-chat/ws. Browser
// authentication happens before upgrade so a rejected session cannot consume a
// WebSocket or leak whether an agent connection exists.
func (s *BrowserServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.Sessions == nil || s.Appender == nil || s.Events == nil {
		http.Error(w, "browser websocket not configured", http.StatusServiceUnavailable)
		return
	}
	if !s.checkOrigin(r) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
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
	if s.Spawner != nil {
		// The global browser-session lease authorizes only this bounded,
		// side-effect-free intent. EnsureRunning owns idempotent provisioning and
		// runs after the lease is released so logout cannot wait on a cold start.
		intentLeaseEntered := false
		intentAuthorized := false
		intentContext, cancelIntent := context.WithTimeout(
			r.Context(),
			s.writeTimeout(),
		)
		err = s.Sessions.AuthorizeSession(intentContext, claims, func() error {
			intentLeaseEntered = true
			return s.authorizeDirectChat(intentContext, claims, func() error {
				intentAuthorized = true
				return nil
			})
		})
		cancelIntent()
		if err != nil {
			if !intentLeaseEntered {
				http.Error(w, "invalid session", http.StatusUnauthorized)
			} else if !intentAuthorized {
				http.Error(w, "not authorized for this agent", http.StatusForbidden)
			} else {
				http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
			}
			return
		}

		// Runtime lifecycle belongs to the provisioner and its idle/shutdown
		// policy. The server-owned timeout bounds startup, while the browser
		// request and socket do not become the runtime's lifetime context.
		spawnContext, cancelSpawn := context.WithTimeout(
			context.WithoutCancel(r.Context()),
			s.spawnTimeout(),
		)
		err = s.Spawner.EnsureRunning(spawnContext, claims.PersonalityAgentID)
		cancelSpawn()
		if err != nil {
			http.Error(w, "agent runtime unavailable", http.StatusServiceUnavailable)
			return
		}
	}

	var conn *websocket.Conn
	finalLeaseEntered := false
	finalAuthorized := false
	upgradeAttempted := false
	finalBaseContext, cancelFinalBase := browserSessionOperationContext(
		r.Context(), claims,
	)
	finalContext, cancelFinal := context.WithTimeout(
		finalBaseContext,
		s.writeTimeout(),
	)
	err = s.Sessions.AuthorizeSession(finalContext, claims, func() error {
		finalLeaseEntered = true
		return s.authorizeDirectChat(finalContext, claims, func() error {
			finalAuthorized = true
			if err := finalContext.Err(); err != nil {
				return err
			}
			handshakeTimeout := s.writeTimeout()
			if deadline, ok := finalContext.Deadline(); ok {
				handshakeTimeout = time.Until(deadline)
				if handshakeTimeout <= 0 {
					return context.DeadlineExceeded
				}
			}
			upgrader := s.upgrader
			upgrader.HandshakeTimeout = handshakeTimeout
			upgradeAttempted = true
			var upgradeErr error
			conn, upgradeErr = upgrader.Upgrade(w, r, nil)
			if upgradeErr != nil {
				return upgradeErr
			}
			if !s.addConnection(conn, claims.sessionID) {
				return errors.New("browser gateway is shutting down")
			}
			return nil
		})
	})
	cancelFinal()
	cancelFinalBase()
	if err != nil {
		if conn != nil {
			_ = conn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "session unavailable"),
				time.Now().Add(s.writeTimeout()),
			)
			_ = conn.Close()
			return
		}
		switch {
		case !finalLeaseEntered:
			http.Error(w, "invalid session", http.StatusUnauthorized)
		case !finalAuthorized:
			http.Error(w, "not authorized for this agent", http.StatusForbidden)
		case !upgradeAttempted:
			http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
		}
		return
	}
	defer s.removeConnection(conn)
	defer conn.Close()
	if err := s.run(r.Context(), conn, claims); err != nil && !errors.Is(err, context.Canceled) {
		deadline := s.sessionDeadline(claims, s.writeTimeout())
		if deadline.After(time.Now()) {
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "browser gateway closed"), deadline)
		}
	}
}

// CloseBrowserConnections closes the current socket generation while allowing
// reconnect. Browsers retain their durable cursor and replay from the
// authoritative event log; no event is synthesized here.
func (s *BrowserServer) CloseBrowserConnections() {
	for _, conn := range s.browserConnections(false) {
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, "browser gateway reconnect"), time.Now().Add(s.writeTimeout()))
		_ = conn.Close()
	}
}

// ShutdownBrowserConnections permanently closes admission for this gateway
// instance and waits boundedly for every hijacked handler to drain.
func (s *BrowserServer) ShutdownBrowserConnections(ctx context.Context) error {
	if ctx == nil {
		return errors.New("browser shutdown context is required")
	}
	connections := s.browserConnections(true)
	for _, conn := range connections {
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, "browser gateway shutdown"), time.Now().Add(s.writeTimeout()))
		_ = conn.Close()
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.connectionsMu.Lock()
		active := len(s.connections)
		s.connectionsMu.Unlock()
		if active == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *BrowserServer) browserConnections(shutdown bool) []*websocket.Conn {
	s.connectionsMu.Lock()
	if shutdown {
		s.closing = true
	}
	connections := make([]*websocket.Conn, 0, len(s.connections))
	for conn := range s.connections {
		connections = append(connections, conn)
	}
	s.connectionsMu.Unlock()
	return connections
}

// CloseBrowserSession eagerly terminates matching sockets in this process.
// Shared AuthorizeSession leases around every data write and command append
// provide the cross-process revocation barrier.
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

func (s *BrowserServer) addConnection(conn *websocket.Conn, sessionID string) bool {
	s.connectionsMu.Lock()
	defer s.connectionsMu.Unlock()
	if s.closing {
		return false
	}
	s.connections[conn] = browserConnection{sessionID: sessionID, conn: conn}
	s.accepted++
	return true
}

func (s *BrowserServer) removeConnection(conn *websocket.Conn) {
	s.connectionsMu.Lock()
	defer s.connectionsMu.Unlock()
	delete(s.connections, conn)
}

func (s *BrowserServer) run(ctx context.Context, conn *websocket.Conn, claims UserSessionClaims) error {
	ctx, cancel := browserSessionOperationContext(ctx, claims)
	defer cancel()
	authorize := func(operation func() error) error {
		return s.authorizeBrowserOperation(ctx, claims, operation)
	}
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
			return authorize(func() error {
				if s.Spawner != nil {
					s.Spawner.Touch(claims.PersonalityAgentID)
				}
				return conn.SetReadDeadline(
					s.sessionReadDeadline(claims, s.pongWait()),
				)
			})
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
					deadline := s.sessionDeadline(claims, s.writeTimeout())
					if ctx.Err() != nil || !deadline.After(time.Now()) {
						cancel()
						return
					}
					if err := conn.WriteControl(websocket.PingMessage, nil, deadline); err != nil {
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
	writeSocketUnlocked := func(frame any) error {
		if s.beforeWrite != nil {
			s.beforeWrite()
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		deadline := s.sessionDeadline(claims, s.writeTimeout())
		if !deadline.After(time.Now()) {
			return context.DeadlineExceeded
		}
		if err := conn.SetWriteDeadline(deadline); err != nil {
			return err
		}
		return conn.WriteJSON(frame)
	}
	writeUnlocked := func(frame any) error {
		return authorize(func() error {
			return writeSocketUnlocked(frame)
		})
	}
	withExclusiveWrite := func(operation func(func(any) error) error) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return operation(writeUnlocked)
	}
	write := func(frame any) error {
		return withExclusiveWrite(func(write func(any) error) error {
			return write(frame)
		})
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
		err := s.browserEventPump(
			ctx,
			claims.PersonalityAgentID,
			next,
			ready,
			volatile,
			authorize,
			write,
		)
		writerErr <- err
		if err != nil {
			cancel()
			// A durable replay or bounded live-queue failure must terminate the
			// blocked reader too; otherwise this browser session would remain
			// half-open until the peer happened to send another frame.
			_ = conn.SetReadDeadline(time.Now())
		}
	}()

	readErr := s.browserReadPump(ctx, conn, claims, write, withExclusiveWrite)
	cancel()
	writerResult := <-writerErr
	if readErr != nil && !errors.Is(readErr, context.Canceled) {
		return readErr
	}
	return writerResult
}

func (s *BrowserServer) browserEventPump(
	ctx context.Context,
	personalityAgentID string,
	lastConsumed uint64,
	ready bool,
	volatile <-chan Envelope,
	authorize func(func() error) error,
	write func(any) error,
) error {
	authorizeOperation := func(operation func() error) error {
		if authorize == nil {
			return operation()
		}
		return authorize(operation)
	}
	next := lastConsumed
	ticker := time.NewTicker(s.Events.pollInterval())
	defer ticker.Stop()
	var authorizationTick <-chan time.Time
	if s.Authorizer != nil {
		authorizationTicker := time.NewTicker(s.authorizationPollInterval())
		defer authorizationTicker.Stop()
		authorizationTick = authorizationTicker.C
	}
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
			// A durable commit can land after the catch-up above while its
			// corresponding volatile successor is already queued. Re-establish
			// the durable cursor before emitting that live-only frame so the
			// browser never observes the successor before its durable prefix.
			next, err = s.browserDurableCatchUp(ctx, personalityAgentID, next, write)
			if err != nil {
				return err
			}
			projected, err := projectBrowserEvent(envelope)
			if err != nil {
				return fmt.Errorf("project volatile browser event: %w", err)
			}
			if err := write(browserEventFrame{Type: "event", Envelope: projected}); err != nil {
				return err
			}
			if s.Spawner != nil {
				if err := authorizeOperation(func() error {
					s.Spawner.Touch(personalityAgentID)
					return nil
				}); err != nil {
					return fmt.Errorf("authorize browser event activity: %w", err)
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-authorizationTick:
			if err := authorizeOperation(func() error { return nil }); err != nil {
				return fmt.Errorf("revalidate browser direct chat: %w", err)
			}
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

func (s *BrowserServer) browserReadPump(
	ctx context.Context,
	conn *websocket.Conn,
	claims UserSessionClaims,
	write func(any) error,
	withExclusiveWrite func(func(func(any) error) error) error,
) error {
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		if err := s.authorizeBrowserOperation(ctx, claims, func() error {
			if s.Spawner != nil {
				s.Spawner.Touch(claims.PersonalityAgentID)
			}
			return nil
		}); err != nil {
			return fmt.Errorf("authorize browser inbound frame: %w", err)
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
		existingAcceptance := false
		appendCalled := false
		operationContext, cancelOperation := browserSessionOperationContext(ctx, claims)
		var admissionErr error
		writeErr := withExclusiveWrite(func(writeUnlocked func(any) error) error {
			admissionErr = s.authorizeBrowserOperation(ctx, claims, func() error {
				appendCalled = true
				var appendErr error
				if appender, ok := s.Appender.(idempotencyAwareCommandAppender); ok {
					envelope, existingAcceptance, appendErr = appender.AppendWithIdempotencyStatus(
						operationContext,
						directChatProvenance(claims),
						frame.IdempotencyKey,
						frame.Command,
					)
				} else {
					envelope, appendErr = s.Appender.Append(
						operationContext,
						directChatProvenance(claims),
						frame.IdempotencyKey,
						frame.Command,
					)
				}
				return appendErr
			})
			if admissionErr != nil {
				return nil
			}
			var disposition json.RawMessage
			found := false
			if existingAcceptance {
				var lookupErr error
				disposition, found, lookupErr = s.Events.CommandDispositionFor(operationContext, envelope)
				if lookupErr != nil {
					admissionErr = fmt.Errorf("lookup browser command disposition: %w", lookupErr)
					return nil
				}
			}
			accepted := browserCommandAcceptedFrame{
				Type:           "command_accepted",
				IdempotencyKey: frame.IdempotencyKey,
				CommandID:      envelope.CommandID,
				Seq:            envelope.Seq,
			}
			if found {
				accepted.Disposition = disposition
			}
			return writeUnlocked(accepted)
		})
		cancelOperation()
		if writeErr != nil {
			return writeErr
		}
		err = admissionErr
		if err != nil {
			if !appendCalled {
				return errors.New("browser direct-chat authority ended")
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

func (s *BrowserServer) spawnTimeout() time.Duration {
	if s.SpawnTimeout > 0 {
		return s.SpawnTimeout
	}
	return 30 * time.Second
}

func (s *BrowserServer) authorizationPollInterval() time.Duration {
	if s.AuthorizationPollInterval > 0 {
		return s.AuthorizationPollInterval
	}
	return 5 * time.Second
}

func (s *BrowserServer) sessionReadDeadline(claims UserSessionClaims, interval time.Duration) time.Time {
	return s.sessionDeadline(claims, interval)
}

func (s *BrowserServer) sessionDeadline(claims UserSessionClaims, interval time.Duration) time.Time {
	deadline := time.Now().Add(interval)
	if !claims.expiresAt.IsZero() && claims.expiresAt.Before(deadline) {
		return claims.expiresAt
	}
	return deadline
}
