package agentevents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// BrowserServer is the browser-facing half of the T28 WebSocket boundary. It
// never accepts an agent bearer token: a signed HttpOnly user-session cookie
// authorizes exactly one conversation and all command/event traffic stays on
// that conversation's API-side durable logs.
type BrowserServer struct {
	Sessions UserSessionVerifier
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
	connections   map[*websocket.Conn]struct{}
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
	Type     string   `json:"type"`
	Envelope Envelope `json:"envelope"`
}

type browserCommandAcceptedFrame struct {
	Type     string          `json:"type"`
	Envelope CommandEnvelope `json:"envelope"`
}

type browserCommandRejectedFrame struct {
	Type         string       `json:"type"`
	RejectReason RejectReason `json:"reject_reason"`
}

type browserCommandHead struct {
	Type      string `json:"type"`
	RequestID string `json:"request_id"`
}

func NewBrowserServer(sessions UserSessionVerifier, appender CommandAppender, events *DurableGateway) *BrowserServer {
	s := &BrowserServer{
		Sessions:     sessions,
		Appender:     appender,
		Events:       events,
		HelloTimeout: 10 * time.Second,
		WriteTimeout: 10 * time.Second,
		MaxReadLimit: MaxUserCommandBytes + 16*1024,
	}
	s.upgrader = websocket.Upgrader{CheckOrigin: s.checkOrigin}
	s.connections = make(map[*websocket.Conn]struct{})
	return s
}

func (s *BrowserServer) checkCommandState(conversationID string, head browserCommandHead) (RejectReason, bool) {
	if s.Events == nil {
		return "", false
	}
	switch head.Type {
	case "abort":
		if !s.Events.IsAssistantTurnInFlight(conversationID) {
			return RejectNotAllowed, true
		}
	case "approval_decision":
		if !s.Events.IsApprovalPending(conversationID, head.RequestID) {
			return RejectNotAllowed, true
		}
	}
	return "", false
}

func (s *BrowserServer) checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return false
	}
	for _, allowed := range s.AllowedOrigins {
		if origin == allowed {
			return true
		}
	}
	return false
}

// ServeHTTP implements GET /conversations/{conversation_id}/ws. Browser
// authentication happens before upgrade so a rejected session cannot consume a
// WebSocket or leak whether an agent connection exists.
func (s *BrowserServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.Sessions == nil || s.Appender == nil || s.Events == nil {
		http.Error(w, "browser websocket not configured", http.StatusServiceUnavailable)
		return
	}
	conversationID := r.PathValue("conversation_id")
	if conversationID == "" {
		http.Error(w, "missing conversation_id", http.StatusBadRequest)
		return
	}
	cookie, err := r.Cookie(BrowserSessionCookie)
	if err != nil {
		http.Error(w, "missing session", http.StatusUnauthorized)
		return
	}
	claims, err := s.Sessions.VerifySession(r.Context(), cookie.Value)
	if err != nil {
		http.Error(w, "invalid session", http.StatusUnauthorized)
		return
	}
	if claims.ConversationID != conversationID {
		http.Error(w, "conversation authorization failed", http.StatusForbidden)
		return
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	s.addConnection(conn)
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
	}
}

func (s *BrowserServer) addConnection(conn *websocket.Conn) {
	s.connectionsMu.Lock()
	defer s.connectionsMu.Unlock()
	s.connections[conn] = struct{}{}
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
	if err := conn.SetReadDeadline(time.Now().Add(s.helloTimeout())); err != nil {
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

	if err := s.Events.EnsureConversationStateRebuilt(ctx, claims.ConversationID); err != nil {
		return fmt.Errorf("rebuild conversation state: %w", err)
	}

	// Install pong handler before first read and schedule the initial
	// keepalive deadline. Pongs from the browser reset the deadline for the
	// next PongWait interval.
	if s.pongWait() > 0 {
		conn.SetPongHandler(func(string) error {
			_ = conn.SetReadDeadline(time.Now().Add(s.pongWait()))
			return nil
		})
		if err := conn.SetReadDeadline(time.Now().Add(s.pongWait())); err != nil {
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

	volatile, unsubscribe := s.Events.SubscribeBrowserVolatile(claims.ConversationID)
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

	writerErr := make(chan error, 1)
	go func() {
		err := s.browserEventPump(ctx, claims.ConversationID, hello.LastEventSeq, volatile, write)
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

func (s *BrowserServer) browserEventPump(ctx context.Context, conversationID string, lastConsumed uint64, volatile <-chan Envelope, write func(any) error) error {
	next := lastConsumed
	ticker := time.NewTicker(s.Events.pollInterval())
	defer ticker.Stop()
	for {
		durable, err := s.Events.EventCatchUp(ctx, conversationID, next)
		if err != nil {
			return fmt.Errorf("browser durable event catch-up: %w", err)
		}
		for _, envelope := range durable {
			if err := write(browserEventFrame{Type: "event", Envelope: envelope}); err != nil {
				return err
			}
			if envelope.Seq == nil {
				return errors.New("durable replay returned a volatile event")
			}
			next = *envelope.Seq
		}
		select {
		case envelope, ok := <-volatile:
			if !ok {
				return errors.New("browser volatile event queue exhausted")
			}
			if err := write(browserEventFrame{Type: "event", Envelope: envelope}); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *BrowserServer) browserReadPump(ctx context.Context, conn *websocket.Conn, claims UserSessionClaims, write func(any) error) error {
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		if s.pongWait() > 0 {
			if err := conn.SetReadDeadline(time.Now().Add(s.pongWait())); err != nil {
				return err
			}
		}
		frame, err := decodeBrowserCommand(raw)
		if err != nil {
			return err
		}
		reason, err := validateBrowserCommand(frame.Command)
		if err != nil {
			if writeErr := write(browserCommandRejectedFrame{Type: "command_rejected", RejectReason: reason}); writeErr != nil {
				return writeErr
			}
			continue
		}
		if len(frame.IdempotencyKey) > MaxIdempotencyKeyBytes {
			if err := write(browserCommandRejectedFrame{Type: "command_rejected", RejectReason: RejectOversized}); err != nil {
				return err
			}
			continue
		}
		var head browserCommandHead
		if err := json.Unmarshal(frame.Command, &head); err != nil {
			if err := write(browserCommandRejectedFrame{Type: "command_rejected", RejectReason: RejectSchemaViolation}); err != nil {
				return err
			}
			continue
		}
		if reason, reject := s.checkCommandState(claims.ConversationID, head); reject {
			if err := write(browserCommandRejectedFrame{Type: "command_rejected", RejectReason: reason}); err != nil {
				return err
			}
			continue
		}
		envelope, err := s.Appender.Append(ctx, claims.ConversationID, frame.IdempotencyKey, frame.Command)
		if err != nil {
			if isIdempotencyConflict(err) {
				return err
			}
			return fmt.Errorf("append browser command: %w", err)
		}
		if err := write(browserCommandAcceptedFrame{Type: "command_accepted", Envelope: envelope}); err != nil {
			return err
		}
	}
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
	if err := unmarshalStrict(raw, &frame); err != nil || frame.Type != "command" || len(frame.Command) == 0 {
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
