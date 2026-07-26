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
	"net/http"
	"strings"
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
	// NextCommandSeq returns the next seq the API expects the agent to apply.
	NextCommandSeq(ctx context.Context, claims TokenClaims) (uint64, error)
	// CatchUp returns commands starting from fromSeq up to the durable tail.
	CatchUp(ctx context.Context, claims TokenClaims, fromSeq uint64) ([]CommandEnvelope, error)
	// Live returns a channel of new commands after catch-up has reached the tail.
	// The channel is closed when the source becomes invalid.
	Live(ctx context.Context, claims TokenClaims) (<-chan CommandEnvelope, error)
	// ApplyAck records a terminal command acknowledgement.
	ApplyAck(ctx context.Context, claims TokenClaims, ack CommandAck) error
}

// EventSink receives durable outbound event envelopes from the agent.
// Production wiring belongs to T17.
type EventSink interface {
	Receive(ctx context.Context, claims TokenClaims, envelope Envelope) error
}

// HydrationLatch waits for the current ProcessGeneration to become Ready.
// Production wiring belongs to T17.
type HydrationLatch interface {
	// WaitFor blocks until the given generation is Ready or the context is done.
	// If the generation is already Ready it returns immediately.
	WaitFor(ctx context.Context, generation uint64) error
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
	}
	s.upgrader = websocket.Upgrader{CheckOrigin: s.checkOrigin}
	return s
}

// checkOrigin implements an explicit allow-list. The zero-value (empty list)
// rejects every origin, including requests that omit the header.
func (s *Server) checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return false
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
		// Close with a generic clean status. Detailed errors are logged by the
		// caller; the agent treats any close as a reconnect signal.
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "gateway closed"),
			time.Now().Add(time.Second))
	}
}

func (s *Server) run(ctx context.Context, conn *websocket.Conn, claims TokenClaims) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	helloCtx, helloDone := context.WithTimeout(ctx, s.HelloTimeout)
	defer helloDone()

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

	if err := s.Latch.WaitFor(helloCtx, hello.Generation); err != nil {
		return fmt.Errorf("hydration wait: %w", err)
	}

	nextSeq, err := s.Commands.NextCommandSeq(helloCtx, claims)
	if err != nil {
		return fmt.Errorf("next command seq: %w", err)
	}
	if nextSeq < hello.LastAppliedCommandSeq+1 {
		nextSeq = hello.LastAppliedCommandSeq + 1
	}

	apiHello := ApiHello{
		AcceptedGeneration:   claims.Generation,
		LastReceivedEventSeq: hello.LastSentEventSeq,
		NextCommandSeq:       nextSeq,
	}
	if err := conn.WriteJSON(apiHello); err != nil {
		return fmt.Errorf("write api hello: %w", err)
	}

	commands, err := s.Commands.CatchUp(ctx, claims, nextSeq)
	if err != nil {
		return fmt.Errorf("command catch-up: %w", err)
	}
	for _, cmd := range commands {
		if err := sendCommandEnvelope(conn, cmd); err != nil {
			return fmt.Errorf("send catch-up command: %w", err)
		}
	}

	live, err := s.Commands.Live(ctx, claims)
	if err != nil {
		return fmt.Errorf("live commands: %w", err)
	}

	errCh := make(chan error, 2)

	go func() {
		errCh <- s.readPump(ctx, conn, claims)
		cancel()
	}()

	go func() {
		errCh <- s.writePump(ctx, conn, live)
		cancel()
	}()

	<-ctx.Done()
	return <-errCh
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

func (s *Server) writePump(ctx context.Context, conn *websocket.Conn, live <-chan CommandEnvelope) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case cmd, ok := <-live:
			if !ok {
				return errors.New("command source closed")
			}
			if err := sendCommandEnvelope(conn, cmd); err != nil {
				return err
			}
		}
	}
}

func sendCommandEnvelope(conn *websocket.Conn, cmd CommandEnvelope) error {
	if err := ValidateCommand(cmd.Command); err != nil {
		return err
	}
	return conn.WriteJSON(cmd)
}

func bearerToken(header string) (string, bool) {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return "", false
	}
	token := strings.TrimSpace(header[len(prefix):])
	return token, token != ""
}
