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
	"sync/atomic"
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

// ConnectionLease is an opaque, PAID-global claim to the one active agent
// WebSocket. Sequence is monotonic and lets a delayed local installer prove it
// cannot displace a newer claim.
type ConnectionLease struct {
	Generation uint64
	Sequence   uint64
	ID         string
}

// ConnectionLeaseAuthority atomically claims and fences the single active
// agent connection across Server instances and API processes.
type ConnectionLeaseAuthority interface {
	ClaimConnectionLease(ctx context.Context, claims TokenClaims) (ConnectionLease, error)
	ValidateConnectionLease(ctx context.Context, claims TokenClaims, lease ConnectionLease) error
	WithConnectionLease(
		ctx context.Context,
		claims TokenClaims,
		lease ConnectionLease,
		call func() error,
	) error
	ReleaseConnectionLease(ctx context.Context, claims TokenClaims, lease ConnectionLease) error
}

type connectionLeaseContextKey struct{}

func contextWithConnectionLease(ctx context.Context, lease ConnectionLease) context.Context {
	return context.WithValue(ctx, connectionLeaseContextKey{}, lease)
}

func connectionLeaseFromContext(ctx context.Context) (ConnectionLease, bool) {
	lease, ok := ctx.Value(connectionLeaseContextKey{}).(ConnectionLease)
	return lease, ok
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
	// Implementations must honor ctx cancellation; Server supplies an
	// independent SideEffectTimeout and always invokes this method inside the
	// authoritative lease boundary.
	ApplyAck(ctx context.Context, claims TokenClaims, ack CommandAck) error
}

// EventSink receives durable outbound event envelopes from the agent.
// Production wiring belongs to T17.
type EventSink interface {
	// Receive must honor ctx cancellation; Server supplies an independent
	// SideEffectTimeout and always invokes this method inside the authoritative
	// lease boundary.
	Receive(ctx context.Context, claims TokenClaims, envelope Envelope) error
	// LastReceivedEventSeq returns the durable consumed prefix for this API
	// identity. It must not be inferred from an agent-provided hello cursor.
	LastReceivedEventSeq(ctx context.Context, claims TokenClaims) (uint64, error)
}

// HydrationLatch durably observes the current ProcessGeneration's Ready state.
// The fenced hello exchange may precede Ready, but traffic may not.
// Production wiring belongs to T17.
type HydrationObservation struct {
	Ready            bool
	TerminalNotReady bool
}

type HydrationLatch interface {
	// WaitFor blocks until the given generation is Ready or the context is done.
	// If the generation is already Ready it returns immediately.
	WaitFor(ctx context.Context, claims TokenClaims, generation uint64) error
	// Observe observes the durable state for exactly this generation. A
	// generation change is an error, not readiness for the replacement.
	Observe(ctx context.Context, claims TokenClaims, generation uint64) (HydrationObservation, error)
}

// Server is the production WebSocket gateway handler.
type Server struct {
	Token      TokenVerifier
	Generation GenerationVerifier
	Commands   CommandSource
	Events     EventSink
	Latch      HydrationLatch
	Leases     ConnectionLeaseAuthority

	// HelloTimeout bounds the initial exchange. Catch-up and live reads use
	// context cancellation from the underlying connection.
	HelloTimeout time.Duration

	// WriteTimeout bounds each WebSocket write. Non-positive values use
	// the safe default so a stalled peer cannot leave the writer blocked.
	WriteTimeout time.Duration

	// MaxReadLimit is the largest WebSocket message the server will accept from
	// the hello onward. A value of zero disables the limit.
	MaxReadLimit int64

	// PongWait is the post-hello liveness bound, including while the authenticated
	// current generation is still NotReady.
	PongWait time.Duration

	// PingInterval is how often the dedicated post-hello liveness pump sends a
	// lease-fenced ping. It must be positive and shorter than PongWait.
	PingInterval time.Duration

	// GenerationPollInterval bounds how long an otherwise-idle connection can
	// remain open after its ProcessGeneration is rolled over.
	GenerationPollInterval time.Duration

	// SideEffectTimeout is the independent cancellation deadline supplied to
	// each synchronous ACK/event sink call. Cooperative sinks return by that
	// deadline. A sink that ignores cancellation remains inside the shared
	// lease boundary until it returns; reconnect and rollover fail-stop rather
	// than releasing the fence while a stale callback can still commit.
	SideEffectTimeout time.Duration

	// AllowedOrigins lists the exact origins allowed to open a WebSocket.
	// An empty list is fail-closed (no origin is accepted). Wildcards are not
	// supported: every accepted origin must be named explicitly.
	AllowedOrigins []string

	upgrader websocket.Upgrader

	connectionsMu sync.Mutex
	connections   map[string]*agentConnectionEpoch
	attempts      map[string]uint64
	nextAttempt   uint64
}

type agentConnectionEpoch struct {
	personalityAgentID     string
	claims                 TokenClaims
	lease                  ConnectionLease
	conn                   *websocket.Conn
	cancel                 context.CancelFunc
	generationWatchStopped chan struct{}
	readyObserved          atomic.Bool
}

var errConnectionEpochRevoked = errors.New("agent websocket connection epoch revoked")
var errAgentRuntimeNotReady = errors.New("agent runtime is not Ready")
var errHydrationTerminalNotReady = errors.New("agent runtime entered terminal NotReady")

// ErrSideEffectCancellationContract identifies an ACK/event adapter that
// returned without reporting the cancellation observed by its supplied
// context. The authoritative shared lease remains held until that adapter has
// actually returned.
var ErrSideEffectCancellationContract = errors.New("agent side-effect adapter violated its cancellation contract")

// SideEffectCancellationContractError preserves both the cancellation and any
// unrelated adapter error while supporting errors.Is with
// ErrSideEffectCancellationContract.
type SideEffectCancellationContractError struct {
	ContextErr error
	AdapterErr error
}

func (e *SideEffectCancellationContractError) Error() string {
	if e.AdapterErr != nil {
		return fmt.Sprintf("%v: context=%v adapter=%v", ErrSideEffectCancellationContract, e.ContextErr, e.AdapterErr)
	}
	return fmt.Sprintf("%v: context=%v", ErrSideEffectCancellationContract, e.ContextErr)
}

func (e *SideEffectCancellationContractError) Is(target error) bool {
	return target == ErrSideEffectCancellationContract
}

func (e *SideEffectCancellationContractError) Unwrap() []error {
	if e.AdapterErr == nil {
		return []error{e.ContextErr}
	}
	return []error{e.ContextErr, e.AdapterErr}
}

// NewServer returns a Server with the required seams. Missing seams leave the
// handler fail-closed.
func NewServer(tv TokenVerifier, gv GenerationVerifier, cs CommandSource, es EventSink, hl HydrationLatch) *Server {
	leases, _ := gv.(ConnectionLeaseAuthority)
	s := &Server{
		Token:                  tv,
		Generation:             gv,
		Commands:               cs,
		Events:                 es,
		Latch:                  hl,
		Leases:                 leases,
		HelloTimeout:           30 * time.Second,
		WriteTimeout:           10 * time.Second,
		MaxReadLimit:           4 * 1024 * 1024,
		PongWait:               60 * time.Second,
		PingInterval:           54 * time.Second,
		GenerationPollInterval: 250 * time.Millisecond,
		SideEffectTimeout:      10 * time.Second,
		connections:            make(map[string]*agentConnectionEpoch),
		attempts:               make(map[string]uint64),
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

	if err := s.validateLivenessConfig(); err != nil {
		return err
	}
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
	if s.Leases == nil {
		return errors.New("connection lease authority is not configured")
	}
	attempt := s.newConnectionAttempt()
	defer s.removeConnectionAttempt(claims.PersonalityAgentID, attempt)

	// Claim the authoritative PAID lease before observing any durable cursor.
	// A predecessor remains entitled to commit through its shared lease lock
	// until this exclusive claim succeeds, so snapshots taken before the claim
	// could advertise state that is already stale in the replacement hello.
	if err := s.Generation.VerifyGeneration(
		helloCtx,
		claims.PersonalityAgentID,
		claims.Generation,
	); err != nil {
		return fmt.Errorf("verify generation before lease claim: %w", err)
	}
	if !s.activateConnectionAttempt(claims.PersonalityAgentID, attempt) {
		return errConnectionEpochRevoked
	}
	s.cancelLocalPredecessor(claims)
	lease, err := s.Leases.ClaimConnectionLease(helloCtx, claims)
	if err != nil {
		return fmt.Errorf("claim connection lease: %w", err)
	}
	epoch, err := s.installConnectionEpoch(ctx, conn, claims, lease, cancel)
	if err != nil {
		_ = s.Leases.ReleaseConnectionLease(context.Background(), claims, lease)
		return fmt.Errorf("install connection lease: %w", err)
	}
	go s.watchGeneration(ctx, epoch)
	defer func() {
		cancel()
		<-epoch.generationWatchStopped
		s.removeConnectionEpoch(epoch)
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), s.writeTimeout())
		defer releaseCancel()
		_ = s.Leases.ReleaseConnectionLease(releaseCtx, claims, lease)
	}()

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
	if err := s.writeJSONForEpoch(ctx, epoch, apiHello); err != nil {
		return fmt.Errorf("write api hello: %w", err)
	}
	helloDone()

	// Replace the hello deadline with the explicit post-hello liveness bound.
	// This permits hydration to outlive HelloTimeout while still bounding a
	// silent authenticated peer throughout NotReady, catch-up, and live traffic.
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(s.PongWait))
		return nil
	})
	if err := conn.SetReadDeadline(time.Now().Add(s.PongWait)); err != nil {
		return fmt.Errorf("set post-hello liveness deadline: %w", err)
	}

	errCh := make(chan error, 3)
	var wg sync.WaitGroup
	var stopOnce sync.Once
	stopPumps := func() {
		stopOnce.Do(func() {
			cancel()
			// Close is allowed concurrently with gorilla's sole reader and writer.
			// Deadline mutation here would race either pump.
			_ = conn.Close()
		})
	}
	defer func() {
		stopPumps()
		wg.Wait()
	}()

	wg.Add(2)
	go func() {
		defer wg.Done()
		errCh <- s.readPump(ctx, epoch)
		stopPumps()
	}()
	go func() {
		defer wg.Done()
		errCh <- s.livenessPump(ctx, epoch)
		stopPumps()
	}()

	// The authenticated, current-generation socket and ApiHello are permitted
	// while NotReady to break the bootstrap circular wait. Durable command
	// delivery and inbound side effects remain gated until this exact
	// generation becomes Ready.
	if err := s.Latch.WaitFor(ctx, claims, hello.Generation); err != nil {
		select {
		case pumpErr := <-errCh:
			if pumpErr != nil {
				return pumpErr
			}
		default:
		}
		return fmt.Errorf("hydration wait: %w", err)
	}
	epoch.readyObserved.Store(true)

	commands, err := s.Commands.CatchUp(ctx, claims, nextSeq)
	if err != nil {
		return fmt.Errorf("command catch-up: %w", err)
	}
	for _, cmd := range commands {
		if err := s.sendCommandEnvelope(ctx, epoch, cmd); err != nil {
			return fmt.Errorf("send catch-up command: %w", err)
		}
		nextSeq = cmd.Seq + 1
	}

	live, liveErr, err := s.Commands.Live(ctx, claims, nextSeq)
	if err != nil {
		return fmt.Errorf("live commands: %w", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- s.writePump(ctx, epoch, live, liveErr)
		stopPumps()
	}()

	<-ctx.Done()
	stopPumps()
	var pumpErrs []error
	for i := 0; i < 3; i++ {
		if err := <-errCh; err != nil {
			pumpErrs = append(pumpErrs, err)
		}
	}
	if len(pumpErrs) > 0 {
		return errors.Join(pumpErrs...)
	}
	return ctx.Err()
}

func (s *Server) readPump(ctx context.Context, epoch *agentConnectionEpoch) error {
	conn, claims := epoch.conn, epoch.claims
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
		if err := conn.SetReadDeadline(time.Now().Add(s.PongWait)); err != nil {
			return err
		}
		if err := frame.Validate(); err != nil {
			return err
		}

		switch frame.FrameType {
		case "event":
			if frame.Envelope.PersonalityAgentID != claims.PersonalityAgentID {
				return errors.New("event personality_agent_id claim mismatch")
			}
			if err := s.withSideEffectLease(ctx, epoch, func(effectCtx context.Context) error {
				return s.Events.Receive(
					contextWithConnectionLease(effectCtx, epoch.lease),
					claims,
					*frame.Envelope,
				)
			}); err != nil {
				return err
			}
		case "command_ack":
			if err := s.withSideEffectLease(ctx, epoch, func(effectCtx context.Context) error {
				return s.Commands.ApplyAck(
					contextWithConnectionLease(effectCtx, epoch.lease),
					claims,
					*frame.Ack,
				)
			}); err != nil {
				return err
			}
		}
	}
}

func (s *Server) writePump(ctx context.Context, epoch *agentConnectionEpoch, live <-chan CommandEnvelope, liveErr <-chan error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case cmd, ok := <-live:
			if !ok {
				return sourceCloseError(liveErr)
			}
			if err := s.sendCommandEnvelope(ctx, epoch, cmd); err != nil {
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

// livenessPump starts only after token, hello, generation, and PAID lease
// authentication have succeeded. WriteControl is concurrency-safe with the
// sole data writer, while the shared lease prevents a revoked epoch from
// extending its socket lifetime.
func (s *Server) livenessPump(ctx context.Context, epoch *agentConnectionEpoch) error {
	ping := func() error {
		return s.Leases.WithConnectionLease(ctx, epoch.claims, epoch.lease, func() error {
			return epoch.conn.WriteControl(websocket.PingMessage, nil, s.writeDeadline())
		})
	}
	if err := ping(); err != nil {
		return err
	}
	ticker := time.NewTicker(s.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := ping(); err != nil {
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

func (s *Server) sendCommandEnvelope(
	ctx context.Context,
	epoch *agentConnectionEpoch,
	cmd CommandEnvelope,
) error {
	if err := cmd.Validate(); err != nil {
		return fmt.Errorf("invalid command envelope: %w", err)
	}
	if cmd.PersonalityAgentID != epoch.claims.PersonalityAgentID {
		return errors.New("command envelope target does not match token claim")
	}
	return s.writeJSONForReadyEpoch(ctx, epoch, cmd)
}

func (s *Server) writeJSONForEpoch(
	ctx context.Context,
	epoch *agentConnectionEpoch,
	value any,
) error {
	return s.Leases.WithConnectionLease(ctx, epoch.claims, epoch.lease, func() error {
		return s.writeJSONOnEpoch(epoch, value)
	})
}

func (s *Server) writeJSONForReadyEpoch(
	ctx context.Context,
	epoch *agentConnectionEpoch,
	value any,
) error {
	return s.Leases.WithConnectionLease(ctx, epoch.claims, epoch.lease, func() error {
		observation, err := s.Latch.Observe(ctx, epoch.claims, epoch.claims.Generation)
		if err != nil {
			return err
		}
		if !observation.Ready {
			return errAgentRuntimeNotReady
		}
		return s.writeJSONOnEpoch(epoch, value)
	})
}

func (s *Server) writeJSONOnEpoch(epoch *agentConnectionEpoch, value any) error {
	if err := epoch.conn.SetWriteDeadline(s.writeDeadline()); err != nil {
		return fmt.Errorf("set write deadline: %w", err)
	}
	return epoch.conn.WriteJSON(value)
}

func (s *Server) writeDeadline() time.Time {
	return time.Now().Add(s.writeTimeout())
}

func (s *Server) writeTimeout() time.Duration {
	if s.WriteTimeout > 0 {
		return s.WriteTimeout
	}
	return 10 * time.Second
}

func (s *Server) validateLivenessConfig() error {
	if s.PongWait <= 0 {
		return errors.New("agent websocket PongWait must be positive")
	}
	if s.PingInterval <= 0 {
		return errors.New("agent websocket PingInterval must be positive")
	}
	if s.PingInterval >= s.PongWait {
		return errors.New("agent websocket PingInterval must be shorter than PongWait")
	}
	return nil
}

func (s *Server) generationPollInterval() time.Duration {
	if s.GenerationPollInterval > 0 {
		return s.GenerationPollInterval
	}
	return 250 * time.Millisecond
}

func (s *Server) sideEffectTimeout() time.Duration {
	if s.SideEffectTimeout > 0 {
		return s.SideEffectTimeout
	}
	return 10 * time.Second
}

func (s *Server) withSideEffectLease(
	ctx context.Context,
	epoch *agentConnectionEpoch,
	call func(context.Context) error,
) error {
	effectCtx, cancel := context.WithTimeout(ctx, s.sideEffectTimeout())
	defer cancel()
	return s.Leases.WithConnectionLease(
		effectCtx,
		epoch.claims,
		epoch.lease,
		func() error {
			observation, readyErr := s.Latch.Observe(
				effectCtx,
				epoch.claims,
				epoch.claims.Generation,
			)
			var callErr error
			switch {
			case readyErr != nil:
				callErr = readyErr
			case !observation.Ready:
				callErr = errAgentRuntimeNotReady
			default:
				callErr = call(effectCtx)
			}
			contextErr := effectCtx.Err()
			if contextErr == nil || errors.Is(callErr, contextErr) {
				return callErr
			}
			// This check deliberately runs before WithConnectionLease returns,
			// so a non-cooperative adapter cannot outlive the shared fence.
			return &SideEffectCancellationContractError{
				ContextErr: contextErr,
				AdapterErr: callErr,
			}
		},
	)
}

func (s *Server) cancelLocalPredecessor(claims TokenClaims) {
	s.connectionsMu.Lock()
	previous := s.connections[claims.PersonalityAgentID]
	canCancel := previous != nil && previous.claims.Generation <= claims.Generation
	s.connectionsMu.Unlock()
	if canCancel {
		previous.cancel()
		_ = previous.conn.Close()
	}
}

func (s *Server) newConnectionAttempt() uint64 {
	s.connectionsMu.Lock()
	defer s.connectionsMu.Unlock()
	s.nextAttempt++
	return s.nextAttempt
}

func (s *Server) activateConnectionAttempt(personalityAgentID string, attempt uint64) bool {
	s.connectionsMu.Lock()
	defer s.connectionsMu.Unlock()
	if s.attempts == nil {
		s.attempts = make(map[string]uint64)
	}
	if s.attempts[personalityAgentID] >= attempt {
		return false
	}
	s.attempts[personalityAgentID] = attempt
	return true
}

func (s *Server) removeConnectionAttempt(personalityAgentID string, attempt uint64) {
	s.connectionsMu.Lock()
	defer s.connectionsMu.Unlock()
	if s.attempts[personalityAgentID] == attempt {
		delete(s.attempts, personalityAgentID)
	}
}

func (s *Server) installConnectionEpoch(
	ctx context.Context,
	conn *websocket.Conn,
	claims TokenClaims,
	lease ConnectionLease,
	cancel context.CancelFunc,
) (*agentConnectionEpoch, error) {
	epoch := &agentConnectionEpoch{
		personalityAgentID:     claims.PersonalityAgentID,
		claims:                 claims,
		lease:                  lease,
		conn:                   conn,
		cancel:                 cancel,
		generationWatchStopped: make(chan struct{}),
	}
	if err := s.Leases.ValidateConnectionLease(ctx, claims, lease); err != nil {
		return nil, err
	}

	s.connectionsMu.Lock()
	if s.connections == nil {
		s.connections = make(map[string]*agentConnectionEpoch)
	}
	previous := s.connections[claims.PersonalityAgentID]
	if previous != nil &&
		(previous.lease.Generation > lease.Generation ||
			(previous.lease.Generation == lease.Generation &&
				previous.lease.Sequence >= lease.Sequence)) {
		s.connectionsMu.Unlock()
		return nil, errConnectionEpochRevoked
	}
	s.connections[claims.PersonalityAgentID] = epoch
	s.connectionsMu.Unlock()

	if previous != nil {
		previous.cancel()
		_ = previous.conn.Close()
	}
	return epoch, nil
}

func (s *Server) removeConnectionEpoch(epoch *agentConnectionEpoch) {
	s.connectionsMu.Lock()
	if s.connections[epoch.personalityAgentID] == epoch {
		delete(s.connections, epoch.personalityAgentID)
	}
	s.connectionsMu.Unlock()
}

func (s *Server) revokeConnectionEpoch(epoch *agentConnectionEpoch) {
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

func (s *Server) watchGeneration(ctx context.Context, epoch *agentConnectionEpoch) {
	defer close(epoch.generationWatchStopped)
	ticker := time.NewTicker(s.generationPollInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.Leases.ValidateConnectionLease(
				ctx,
				epoch.claims,
				epoch.lease,
			); err != nil {
				s.revokeConnectionEpoch(epoch)
				return
			}
			observation, err := s.Latch.Observe(
				ctx,
				epoch.claims,
				epoch.claims.Generation,
			)
			if err != nil {
				s.revokeConnectionEpoch(epoch)
				return
			}
			if observation.Ready {
				epoch.readyObserved.Store(true)
				continue
			}
			if observation.TerminalNotReady || epoch.readyObserved.Load() {
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
