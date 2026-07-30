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
	"hash/fnv"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// TokenClaims are the signed claims from the short-lived agent token.
// API derives the target and expected generation from these claims; the
// agent's hello identifier is an assertion to be verified, not authoritative.
type TokenClaims struct {
	TenantID           string
	PersonalityAgentID string
	Generation         uint64
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
	VerifyGeneration(ctx context.Context, personalityAgentID string, generation uint64) error
}

// CommandSource is the durable command log. It is authoritative for seq numbers
// and retransmission. Production wiring belongs to T17.
type CommandSource interface {
	// FirstCommandSeq returns the first seq present in the durable command log.
	// It is used as a lower bound so catch-up cannot skip unapplied commands.
	FirstCommandSeq(ctx context.Context, claims TokenClaims) (uint64, error)
	// HasCommands distinguishes an empty log from a retained log starting at 1.
	HasCommands(ctx context.Context, claims TokenClaims) (bool, error)
	// NextCommandSeq returns the first command whose durable ACK state is not
	// terminal, or one past the command tail when every retained command is
	// terminal. The API's own ACK log is authoritative: an agent-reported
	// last_applied cursor cannot prove that ApplyAck completed before a socket
	// was lost.
	NextCommandSeq(ctx context.Context, claims TokenClaims) (uint64, error)
	// CatchUp returns commands starting from fromSeq up to the durable tail.
	CatchUp(ctx context.Context, claims TokenClaims, fromSeq uint64) ([]CommandEnvelope, error)
	// Live returns commands from fromSeq onward. The source must ensure that no
	// command with seq >= fromSeq is dropped, either by binding a cursor before
	// returning or by replaying from fromSeq (so an append between catch-up and
	// the first poll is still delivered). Implementations that poll must start
	// from fromSeq and advance next only after successfully sending each command.
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

	// GenerationPollInterval bounds how long an otherwise-idle connection can
	// remain open after its ProcessGeneration is rolled over.
	GenerationPollInterval time.Duration

	// AllowedOrigins lists the exact origins allowed to open a WebSocket.
	// An empty list is fail-closed (no origin is accepted). Wildcards are not
	// supported: every accepted origin must be named explicitly.
	AllowedOrigins []string

	upgrader websocket.Upgrader

	connectionsMu sync.Mutex
	connections   map[string]*agentConnectionEpoch
	nextEpoch     uint64
	epochGates    [64]sync.Mutex
}

type agentConnectionEpoch struct {
	id                     uint64
	personalityAgentID     string
	claims                 TokenClaims
	conn                   *websocket.Conn
	cancel                 context.CancelFunc
	generationWatchStopped chan struct{}
}

var errConnectionEpochRevoked = errors.New("agent websocket connection epoch revoked")

// NewServer returns a Server with the required seams. Missing seams leave the
// handler fail-closed.
func NewServer(tv TokenVerifier, gv GenerationVerifier, cs CommandSource, es EventSink, hl HydrationLatch) *Server {
	s := &Server{
		Token:                  tv,
		Generation:             gv,
		Commands:               cs,
		Events:                 es,
		Latch:                  hl,
		HelloTimeout:           30 * time.Second,
		WriteTimeout:           10 * time.Second,
		MaxReadLimit:           4 * 1024 * 1024,
		PongWait:               60 * time.Second,
		PingInterval:           54 * time.Second,
		GenerationPollInterval: 250 * time.Millisecond,
		connections:            make(map[string]*agentConnectionEpoch),
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
	if claims.TenantID == "" || ValidatePersonalityAgentID(claims.PersonalityAgentID) != nil {
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

	if hello.PersonalityAgentID != claims.PersonalityAgentID {
		return fmt.Errorf("personality_agent_id claim mismatch")
	}
	if hello.Generation != claims.Generation {
		return fmt.Errorf("generation claim mismatch")
	}
	if hello.LastAppliedCommandSeq > hello.LastReceivedCommandSeq {
		return fmt.Errorf("last applied command seq %d exceeds last received %d", hello.LastAppliedCommandSeq, hello.LastReceivedCommandSeq)
	}
	if err := s.Generation.VerifyGeneration(helloCtx, claims.PersonalityAgentID, hello.Generation); err != nil {
		return fmt.Errorf("verify generation: %w", err)
	}

	epoch := s.installConnectionEpoch(conn, claims, cancel)
	defer s.removeConnectionEpoch(epoch)
	go s.watchGeneration(ctx, epoch)
	defer func() {
		cancel()
		<-epoch.generationWatchStopped
	}()

	if err := s.Latch.WaitFor(helloCtx, claims, hello.Generation); err != nil {
		return fmt.Errorf("hydration wait: %w", err)
	}

	firstSeq, err := s.Commands.FirstCommandSeq(helloCtx, claims)
	if err != nil {
		return fmt.Errorf("first command seq: %w", err)
	}
	hasCommands, err := s.Commands.HasCommands(helloCtx, claims)
	if err != nil {
		return fmt.Errorf("inspect command log: %w", err)
	}
	if !hasCommands && hello.LastAppliedCommandSeq > 0 {
		return fmt.Errorf("command log is empty despite agent last applied seq %d", hello.LastAppliedCommandSeq)
	}

	// The retained log must begin no later than the agent's expected next
	// command; otherwise the server would advertise a guaranteed gap that the
	// agent cannot recover from.
	if hello.LastReceivedCommandSeq == maxJSONSafeInteger || firstSeq > hello.LastReceivedCommandSeq+1 {
		return fmt.Errorf("command log gap: first seq %d is beyond agent last received %d", firstSeq, hello.LastReceivedCommandSeq)
	}
	if firstSeq > maxJSONSafeInteger {
		return fmt.Errorf("first command seq %d exceeds JSON-safe integer range", firstSeq)
	}

	// The API's durable terminal ACK state chooses replay. A wire send followed
	// by disconnect before ApplyAck therefore replays the command even if the
	// agent had applied it locally but did not finish recording the ACK.
	if hello.LastAppliedCommandSeq > maxJSONSafeInteger-1 {
		return fmt.Errorf("next_command_seq would exceed JSON-safe integer range")
	}
	maxNextSeq := hello.LastAppliedCommandSeq + 1
	nextSeq, err := s.Commands.NextCommandSeq(helloCtx, claims)
	if err != nil {
		return fmt.Errorf("next command seq: %w", err)
	}
	if nextSeq < firstSeq {
		return fmt.Errorf("next_command_seq %d precedes retained command log at %d", nextSeq, firstSeq)
	}
	if nextSeq > maxNextSeq {
		return fmt.Errorf("API terminal ACK state is ahead of agent: next_command_seq %d exceeds locally applied bound %d", nextSeq, maxNextSeq)
	}
	if nextSeq > maxJSONSafeInteger {
		return fmt.Errorf("next_command_seq %d exceeds JSON-safe integer range", nextSeq)
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
	if lastReceivedEventSeq > maxJSONSafeInteger {
		return fmt.Errorf("last_received_event_seq %d exceeds JSON-safe integer range", lastReceivedEventSeq)
	}
	apiHello := ApiHello{
		PersonalityAgentID:   claims.PersonalityAgentID,
		AcceptedGeneration:   claims.Generation,
		LastReceivedEventSeq: lastReceivedEventSeq,
		NextCommandSeq:       nextSeq,
	}
	if err := s.writeJSONForEpoch(epoch, apiHello); err != nil {
		return fmt.Errorf("write api hello: %w", err)
	}

	// Install the pong handler before catch-up. The read pump installs its
	// initial deadline immediately before the first read so a long catch-up
	// cannot consume the peer's entire PongWait budget.
	if s.PongWait > 0 {
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
		if err := s.sendCommandEnvelope(epoch, cmd); err != nil {
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
			// Close is allowed concurrently with gorilla's sole reader and writer.
			// Deadline mutation here would race either pump.
			_ = conn.Close()
		})
	}

	go func() {
		defer wg.Done()
		errCh <- s.readPump(ctx, epoch)
		stopPumps()
	}()

	go func() {
		defer wg.Done()
		errCh <- s.writePump(ctx, epoch, live, liveErr)
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

func (s *Server) readPump(ctx context.Context, epoch *agentConnectionEpoch) error {
	conn, claims := epoch.conn, epoch.claims
	if s.PongWait > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(s.PongWait)); err != nil {
			return fmt.Errorf("set initial pong read deadline: %w", err)
		}
	}
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
			if frame.Envelope.PersonalityAgentID != claims.PersonalityAgentID {
				return errors.New("event personality_agent_id claim mismatch")
			}
			if err := s.withCurrentConnectionEpoch(ctx, epoch, func() error {
				return s.Events.Receive(ctx, claims, *frame.Envelope)
			}); err != nil {
				return err
			}
		case "command_ack":
			if err := s.withCurrentConnectionEpoch(ctx, epoch, func() error {
				return s.Commands.ApplyAck(ctx, claims, *frame.Ack)
			}); err != nil {
				return err
			}
		}
	}
}

func (s *Server) writePump(ctx context.Context, epoch *agentConnectionEpoch, live <-chan CommandEnvelope, liveErr <-chan error) error {
	if s.PingInterval <= 0 {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case cmd, ok := <-live:
				if !ok {
					return sourceCloseError(liveErr)
				}
				if err := s.sendCommandEnvelope(epoch, cmd); err != nil {
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
			if err := s.sendCommandEnvelope(epoch, cmd); err != nil {
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
			if err := s.withCurrentConnectionEpoch(ctx, epoch, func() error {
				return epoch.conn.WriteControl(websocket.PingMessage, nil, s.writeDeadline())
			}); err != nil {
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

func (s *Server) sendCommandEnvelope(epoch *agentConnectionEpoch, cmd CommandEnvelope) error {
	if err := cmd.Validate(); err != nil {
		return fmt.Errorf("invalid command envelope: %w", err)
	}
	if cmd.PersonalityAgentID != epoch.claims.PersonalityAgentID {
		return errors.New("command envelope target does not match token claim")
	}
	return s.writeJSONForEpoch(epoch, cmd)
}

func (s *Server) writeJSONForEpoch(epoch *agentConnectionEpoch, value any) error {
	return s.withCurrentConnectionEpoch(context.Background(), epoch, func() error {
		if err := epoch.conn.SetWriteDeadline(s.writeDeadline()); err != nil {
			return fmt.Errorf("set write deadline: %w", err)
		}
		return epoch.conn.WriteJSON(value)
	})
}

func (s *Server) writeDeadline() time.Time {
	timeout := s.WriteTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return time.Now().Add(timeout)
}

func (s *Server) generationPollInterval() time.Duration {
	if s.GenerationPollInterval > 0 {
		return s.GenerationPollInterval
	}
	return 250 * time.Millisecond
}

func (s *Server) epochGate(personalityAgentID string) *sync.Mutex {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(personalityAgentID))
	return &s.epochGates[hash.Sum32()%uint32(len(s.epochGates))]
}

func (s *Server) installConnectionEpoch(
	conn *websocket.Conn,
	claims TokenClaims,
	cancel context.CancelFunc,
) *agentConnectionEpoch {
	epoch := &agentConnectionEpoch{
		personalityAgentID:     claims.PersonalityAgentID,
		claims:                 claims,
		conn:                   conn,
		cancel:                 cancel,
		generationWatchStopped: make(chan struct{}),
	}
	gate := s.epochGate(claims.PersonalityAgentID)
	gate.Lock()
	defer gate.Unlock()

	s.connectionsMu.Lock()
	if s.connections == nil {
		s.connections = make(map[string]*agentConnectionEpoch)
	}
	s.nextEpoch++
	epoch.id = s.nextEpoch
	previous := s.connections[claims.PersonalityAgentID]
	s.connections[claims.PersonalityAgentID] = epoch
	s.connectionsMu.Unlock()

	if previous != nil {
		previous.cancel()
		_ = previous.conn.Close()
	}
	return epoch
}

func (s *Server) removeConnectionEpoch(epoch *agentConnectionEpoch) {
	gate := s.epochGate(epoch.personalityAgentID)
	gate.Lock()
	defer gate.Unlock()

	s.connectionsMu.Lock()
	if s.connections[epoch.personalityAgentID] == epoch {
		delete(s.connections, epoch.personalityAgentID)
	}
	s.connectionsMu.Unlock()
}

func (s *Server) revokeConnectionEpoch(epoch *agentConnectionEpoch) {
	gate := s.epochGate(epoch.personalityAgentID)
	gate.Lock()
	defer gate.Unlock()

	s.connectionsMu.Lock()
	current := s.connections[epoch.personalityAgentID] == epoch
	if current {
		delete(s.connections, epoch.personalityAgentID)
	}
	s.connectionsMu.Unlock()
	if current {
		epoch.cancel()
		_ = epoch.conn.Close()
	}
}

func (s *Server) withCurrentConnectionEpoch(
	ctx context.Context,
	epoch *agentConnectionEpoch,
	call func() error,
) error {
	gate := s.epochGate(epoch.personalityAgentID)
	gate.Lock()
	defer gate.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	s.connectionsMu.Lock()
	current := s.connections[epoch.personalityAgentID] == epoch
	s.connectionsMu.Unlock()
	if !current {
		return errConnectionEpochRevoked
	}
	return call()
}

func (s *Server) watchGeneration(ctx context.Context, epoch *agentConnectionEpoch) {
	defer close(epoch.generationWatchStopped)
	ticker := time.NewTicker(s.generationPollInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.Generation.VerifyGeneration(
				ctx,
				epoch.personalityAgentID,
				epoch.claims.Generation,
			); err != nil {
				s.revokeConnectionEpoch(epoch)
				return
			}
		}
	}
}

func bearerToken(header string) (string, bool) {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return "", false
	}
	token := strings.TrimSpace(header[len(prefix):])
	return token, token != ""
}
