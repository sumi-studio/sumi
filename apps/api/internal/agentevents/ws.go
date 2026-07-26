// Package agentevents implements the production agent-facing WebSocket gateway.
//
// The handler performs the authenticated hello exchange, waits for the current
// ProcessGeneration's hydration state, sends durable command catch-up, and then
// forwards live commands / inbound frames. T26 (token/identity/lease) and T17
// (hydration/durable source) are represented by compile-safe seams below; the
// zero-value wiring in cmd/server is fail-closed so production bootstrap cannot
// run without them.
package agentevents

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// TokenClaims are the signed claims from the short-lived agent token.
// API derives conversation and expected generation from these claims; the
// agent's hello identifiers are assertions to be verified, not authoritative.
type TokenClaims struct {
	TenantID       string
	AgentID        string
	ConversationID string
	Generation     uint64
}

// TokenVerifier validates the Authorization bearer header. Production wiring
// belongs to T26.
type TokenVerifier interface {
	Verify(ctx context.Context, token string) (TokenClaims, error)
}

// GenerationVerifier confirms that the claimed ProcessGeneration is the
// agent's latest lease and fences stale connections. Production wiring belongs
// to T26.
type GenerationVerifier interface {
	VerifyGeneration(ctx context.Context, agentID string, generation uint64) error
}

// CommandSource is the durable command log. It is authoritative for seq numbers
// and retransmission. Production wiring belongs to T17.
type CommandSource interface {
	// FirstCommandSeq returns the first seq present in the durable command log.
	// It is used as a lower bound so catch-up cannot skip unapplied commands.
	FirstCommandSeq(ctx context.Context, claims TokenClaims) (uint64, error)
	// CatchUp returns commands starting from fromSeq up to the durable tail.
	CatchUp(ctx context.Context, claims TokenClaims, fromSeq uint64) ([]CommandEnvelope, error)
	// Live returns commands from fromSeq onward. The source must bind this cursor
	// before returning so an append between catch-up and subscription cannot be
	// lost.
	// The commands channel is closed when the source becomes invalid. The error
	// channel carries source failures; it is closed after the commands channel.
	Live(ctx context.Context, claims TokenClaims, fromSeq uint64) (<-chan CommandEnvelope, <-chan error, error)
	// ApplyAck records a terminal command acknowledgement.
	ApplyAck(ctx context.Context, claims TokenClaims, ack CommandAck) error
}

// EventSink receives durable outbound event envelopes from the agent.
// Production wiring belongs to T17.
type EventSink interface {
	Receive(ctx context.Context, claims TokenClaims, envelope Envelope) error
	// LastReceivedEventSeq returns the durable consumed prefix for this API
	// identity. It must not be inferred from an agent-provided hello cursor.
	LastReceivedEventSeq(ctx context.Context, claims TokenClaims) (uint64, error)
}

// HydrationLatch waits for the current ProcessGeneration to become Ready.
// Production wiring belongs to T17.
type HydrationLatch interface {
	// WaitFor blocks until the given generation is Ready or the context is done.
	// If the generation is already Ready it returns immediately.
	WaitFor(ctx context.Context, claims TokenClaims, generation uint64) error
}

// Server is the production WebSocket gateway handler.
type Server struct {
	Token      TokenVerifier
	Generation GenerationVerifier
	Commands   CommandSource
	Events     EventSink
	Latch      HydrationLatch

	// HelloTimeout bounds the initial exchange. Catch-up and live reads use
	// context cancellation from the underlying connection.
	HelloTimeout time.Duration

	// WriteTimeout bounds each WebSocket write. Non-positive values use
	// the safe default so a stalled peer cannot leave the writer blocked.
	WriteTimeout time.Duration

	// MaxReadLimit is the largest WebSocket message the server will accept from
	// the hello onward. A value of zero disables the limit.
	MaxReadLimit int64

	// PongWait is the duration the server will wait for a pong after the
	// hello exchange before closing the connection.
	PongWait time.Duration

	// PingInterval is how often the server sends ping control frames from the
	// writer goroutine. It must be shorter than PongWait.
	PingInterval time.Duration

	// AllowedOrigins lists the exact origins allowed to open a WebSocket.
	// An empty list is fail-closed (no origin is accepted). Wildcards are not
	// supported: every accepted origin must be named explicitly.
	AllowedOrigins []string

	upgrader websocket.Upgrader
}

// NewServer returns a Server with the required seams. Missing seams leave the
// handler fail-closed.
func NewServer(tv TokenVerifier, gv GenerationVerifier, cs CommandSource, es EventSink, hl HydrationLatch) *Server {
	s := &Server{
		Token:        tv,
		Generation:   gv,
		Commands:     cs,
		Events:       es,
		Latch:        hl,
		HelloTimeout: 30 * time.Second,
		WriteTimeout: 10 * time.Second,
		MaxReadLimit: 4 * 1024 * 1024,
		PongWait:     60 * time.Second,
		PingInterval: 54 * time.Second,
	}
	s.upgrader = websocket.Upgrader{CheckOrigin: s.checkOrigin}
	return s
}

// checkOrigin implements an explicit browser-origin allow-list. Native agents
// authenticate with a short-lived bearer token and do not send Origin; that
// deliberate non-browser path must remain available without weakening browser
// CSRF protection.
func (s *Server) checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		_, ok := bearerToken(r.Header.Get("Authorization"))
		return ok
	}
	for _, allowed := range s.AllowedOrigins {
		if allowed == origin {
			return true
		}
	}
	return false
}

// NewServerWithTokenVerifier returns a Server that uses the supplied real
// TokenVerifier and fail-closed placeholders for the remaining T17/T26 seams.
// It is the T28 production wiring point for short-lived agent token
// verification; the other production dependencies must still be injected before
// the gateway can carry a real session.
func NewServerWithTokenVerifier(tv TokenVerifier) *Server {
	return NewServer(
		tv,
		failClosedGenerationVerifier{},
		failClosedCommandSource{},
		failClosedEventSink{},
		failClosedHydrationLatch{},
	)
}

// ServeHTTP implements http.Handler for GET /agent/ws.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	token, ok := bearerToken(r.Header.Get("Authorization"))
	if !ok {
		http.Error(w, "missing authorization", http.StatusUnauthorized)
		return
	}

	claims, err := s.Token.Verify(ctx, token)
	if err != nil {
		http.Error(w, "invalid authorization", http.StatusUnauthorized)
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	if err := s.run(ctx, conn, claims); err != nil {
		if !errors.Is(err, context.Canceled) {
			log.Printf("agent websocket closed: %v", err)
		}
		// Close with a generic clean status. The agent treats any close as a
		// reconnect signal.
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "gateway closed"),
			s.writeDeadline())
	}
}

func (s *Server) run(ctx context.Context, conn *websocket.Conn, claims TokenClaims) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if s.MaxReadLimit > 0 {
		conn.SetReadLimit(s.MaxReadLimit)
	}

	helloCtx, helloDone := context.WithTimeout(ctx, s.HelloTimeout)
	defer helloDone()

	if s.HelloTimeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(s.HelloTimeout)); err != nil {
			return fmt.Errorf("set hello read deadline: %w", err)
		}
	}

	var hello AgentHello
	if err := conn.ReadJSON(&hello); err != nil {
		return fmt.Errorf("read hello: %w", err)
	}

	if hello.AgentID != claims.AgentID {
		return fmt.Errorf("agent_id claim mismatch")
	}
	if hello.Generation != claims.Generation {
		return fmt.Errorf("generation claim mismatch")
	}
	if err := s.Generation.VerifyGeneration(helloCtx, claims.AgentID, hello.Generation); err != nil {
		return fmt.Errorf("verify generation: %w", err)
	}

	if err := s.Latch.WaitFor(helloCtx, claims, hello.Generation); err != nil {
		return fmt.Errorf("hydration wait: %w", err)
	}

	firstSeq, err := s.Commands.FirstCommandSeq(helloCtx, claims)
	if err != nil {
		return fmt.Errorf("first command seq: %w", err)
	}

	// The retained log must begin no later than the agent's expected next
	// command; otherwise the server would advertise a guaranteed gap that the
	// agent cannot recover from.
	if firstSeq > hello.LastReceivedCommandSeq+1 {
		return fmt.Errorf("command log gap: first seq %d is beyond agent last received %d", firstSeq, hello.LastReceivedCommandSeq)
	}

	nextSeq := hello.LastAppliedCommandSeq + 1
	if nextSeq < firstSeq {
		nextSeq = firstSeq
	}

	lastReceivedEventSeq, err := s.Events.LastReceivedEventSeq(helloCtx, claims)
	if err != nil {
		return fmt.Errorf("last received event seq: %w", err)
	}
	if lastReceivedEventSeq > hello.LastSentEventSeq {
		return fmt.Errorf(
			"event cursor is ahead of agent: api received %d, agent last sent %d",
			lastReceivedEventSeq,
			hello.LastSentEventSeq,
		)
	}
	apiHello := ApiHello{
		AcceptedGeneration:   claims.Generation,
		LastReceivedEventSeq: lastReceivedEventSeq,
		NextCommandSeq:       nextSeq,
	}
	if err := s.writeJSON(conn, apiHello); err != nil {
		return fmt.Errorf("write api hello: %w", err)
	}

	// Install the pong handler and the first post-hello read deadline before
	// starting the read pump.
	if s.PongWait > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(s.PongWait)); err != nil {
			return fmt.Errorf("set pong read deadline: %w", err)
		}
		conn.SetPongHandler(func(string) error {
			if s.PongWait > 0 {
				_ = conn.SetReadDeadline(time.Now().Add(s.PongWait))
			}
			return nil
		})
	}

	commands, err := s.Commands.CatchUp(ctx, claims, nextSeq)
	if err != nil {
		return fmt.Errorf("command catch-up: %w", err)
	}
	for _, cmd := range commands {
		if err := s.sendCommandEnvelope(conn, cmd); err != nil {
			return fmt.Errorf("send catch-up command: %w", err)
		}
		nextSeq = cmd.Seq + 1
	}

	live, liveErr, err := s.Commands.Live(ctx, claims, nextSeq)
	if err != nil {
		return fmt.Errorf("live commands: %w", err)
	}

	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	var stopOnce sync.Once
	stopPumps := func() {
		stopOnce.Do(func() {
			cancel()
			// Cancellation alone cannot interrupt gorilla's blocking ReadJSON.
			// Poison both halves immediately rather than waiting for PongWait.
			_ = conn.SetReadDeadline(time.Now())
			_ = conn.SetWriteDeadline(time.Now())
		})
	}

	go func() {
		defer wg.Done()
		errCh <- s.readPump(ctx, conn, claims)
		stopPumps()
	}()

	go func() {
		defer wg.Done()
		errCh <- s.writePump(ctx, conn, live, liveErr)
		stopPumps()
	}()

	<-ctx.Done()
	stopPumps()
	var pumpErrs []error
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			pumpErrs = append(pumpErrs, err)
		}
	}
	wg.Wait()
	if len(pumpErrs) > 0 {
		return errors.Join(pumpErrs...)
	}
	return ctx.Err()
}

func (s *Server) readPump(ctx context.Context, conn *websocket.Conn, claims TokenClaims) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var frame OutboundFrame
		if err := conn.ReadJSON(&frame); err != nil {
			return err
		}
		if s.PongWait > 0 {
			if err := conn.SetReadDeadline(time.Now().Add(s.PongWait)); err != nil {
				return err
			}
		}
		if err := frame.Validate(); err != nil {
			return err
		}

		switch frame.FrameType {
		case "event":
			if frame.Envelope.ConversationID != claims.ConversationID {
				return errors.New("event conversation_id claim mismatch")
			}
			if err := s.Events.Receive(ctx, claims, *frame.Envelope); err != nil {
				return err
			}
		case "command_ack":
			if err := s.Commands.ApplyAck(ctx, claims, *frame.Ack); err != nil {
				return err
			}
		}
	}
}

func (s *Server) writePump(ctx context.Context, conn *websocket.Conn, live <-chan CommandEnvelope, liveErr <-chan error) error {
	if s.PingInterval <= 0 {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case cmd, ok := <-live:
				if !ok {
					return sourceCloseError(liveErr)
				}
				if err := s.sendCommandEnvelope(conn, cmd); err != nil {
					return err
				}
			case err, ok := <-liveErr:
				if !ok {
					return errors.New("command source closed")
				}
				if err != nil {
					return err
				}
			}
		}
	}

	ticker := time.NewTicker(s.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case cmd, ok := <-live:
			if !ok {
				return sourceCloseError(liveErr)
			}
			if err := s.sendCommandEnvelope(conn, cmd); err != nil {
				return err
			}
		case err, ok := <-liveErr:
			if !ok {
				return errors.New("command source closed")
			}
			if err != nil {
				return err
			}
		case <-ticker.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, s.writeDeadline()); err != nil {
				return err
			}
		}
	}
}

func sourceCloseError(liveErr <-chan error) error {
	// DurableGateway closes commands before errors. Consume the terminal error
	// so a real source failure is not replaced by a generic closed-channel one.
	if liveErr == nil {
		return errors.New("command source closed")
	}
	if err, ok := <-liveErr; ok && err != nil {
		return err
	}
	return errors.New("command source closed")
}

func (s *Server) sendCommandEnvelope(conn *websocket.Conn, cmd CommandEnvelope) error {
	if cmd.Seq > maxJSONSafeInteger {
		return fmt.Errorf("command envelope seq exceeds JSON-safe integer range")
	}
	if cmd.CommandID == "" {
		return fmt.Errorf("command envelope missing command_id")
	}
	if !canonicalUUIDRegexp.MatchString(cmd.CommandID) {
		return fmt.Errorf("command_id %q is not a canonical lowercase UUID", cmd.CommandID)
	}
	if err := ValidateCommand(cmd.Command); err != nil {
		return err
	}
	return s.writeJSON(conn, cmd)
}

func (s *Server) writeJSON(conn *websocket.Conn, value any) error {
	if err := conn.SetWriteDeadline(s.writeDeadline()); err != nil {
		return fmt.Errorf("set write deadline: %w", err)
	}
	return conn.WriteJSON(value)
}

func (s *Server) writeDeadline() time.Time {
	timeout := s.WriteTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return time.Now().Add(timeout)
}

func bearerToken(header string) (string, bool) {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return "", false
	}
	token := strings.TrimSpace(header[len(prefix):])
	return token, token != ""
}
