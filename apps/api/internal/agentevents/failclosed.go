package agentevents

import (
	"context"
	"errors"
)

// FailClosed seams are used in cmd/server until T26/T17 production identity,
// token verification, durable command source, and hydration are wired. They
// reject every request with a typed error so the binary compiles and the route
// exists, but no production connection can succeed without the real seams.

var errNotWired = errors.New("T26/T17 production seam not wired")

type failClosedTokenVerifier struct{}

func (failClosedTokenVerifier) Verify(ctx context.Context, token string) (TokenClaims, error) {
	return TokenClaims{}, errNotWired
}

type failClosedGenerationVerifier struct{}

func (failClosedGenerationVerifier) VerifyGeneration(ctx context.Context, agentID string, generation uint64) error {
	return errNotWired
}

type failClosedCommandSource struct{}

func (failClosedCommandSource) NextCommandSeq(ctx context.Context, claims TokenClaims) (uint64, error) {
	return 0, errNotWired
}

func (failClosedCommandSource) CatchUp(ctx context.Context, claims TokenClaims, fromSeq uint64) ([]CommandEnvelope, error) {
	return nil, errNotWired
}

func (failClosedCommandSource) Live(ctx context.Context, claims TokenClaims) (<-chan CommandEnvelope, error) {
	return nil, errNotWired
}

func (failClosedCommandSource) ApplyAck(ctx context.Context, claims TokenClaims, ack CommandAck) error {
	return errNotWired
}

type failClosedEventSink struct{}

func (failClosedEventSink) Receive(ctx context.Context, claims TokenClaims, envelope Envelope) error {
	return errNotWired
}

type failClosedHydrationLatch struct{}

func (failClosedHydrationLatch) WaitFor(ctx context.Context, generation uint64) error {
	return errNotWired
}

// NewFailClosedServer returns a Server whose seams all reject. It is suitable
// for cmd/server until T26/T17 inject the real production implementations.
func NewFailClosedServer() *Server {
	return NewServer(
		failClosedTokenVerifier{},
		failClosedGenerationVerifier{},
		failClosedCommandSource{},
		failClosedEventSink{},
		failClosedHydrationLatch{},
	)
}
