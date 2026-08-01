package agentevents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const testPersonalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"

type fakeGenerationVerifier struct {
	mu            sync.Mutex
	latest        uint64
	leaseSequence uint64
	lease         ConnectionLease
}

type gatedClaimLeaseAuthority struct {
	*DurableGateway
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (g *gatedClaimLeaseAuthority) ClaimConnectionLease(
	ctx context.Context,
	claims TokenClaims,
) (ConnectionLease, error) {
	g.once.Do(func() { close(g.entered) })
	select {
	case <-ctx.Done():
		return ConnectionLease{}, ctx.Err()
	case <-g.release:
	}
	return g.DurableGateway.ClaimConnectionLease(ctx, claims)
}

func (f *fakeGenerationVerifier) VerifyGeneration(ctx context.Context, personalityAgentID string, generation uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if generation != f.latest {
		return fmt.Errorf("stale generation: got %d, want %d", generation, f.latest)
	}
	return nil
}

func (f *fakeGenerationVerifier) setLatest(latest uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.latest = latest
}

func (f *fakeGenerationVerifier) ClaimConnectionLease(ctx context.Context, claims TokenClaims) (ConnectionLease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ConnectionLease{}, err
	}
	if claims.Generation != f.latest {
		return ConnectionLease{}, fmt.Errorf("stale generation: got %d, want %d", claims.Generation, f.latest)
	}
	f.leaseSequence++
	f.lease = ConnectionLease{
		Generation: claims.Generation,
		Sequence:   f.leaseSequence,
		ID:         fmt.Sprintf("fake-lease-%d", f.leaseSequence),
	}
	return f.lease, nil
}

func (f *fakeGenerationVerifier) ValidateConnectionLease(
	ctx context.Context,
	claims TokenClaims,
	lease ConnectionLease,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.validateLeaseLocked(ctx, claims, lease)
}

func (f *fakeGenerationVerifier) WithConnectionLease(
	ctx context.Context,
	claims TokenClaims,
	lease ConnectionLease,
	call func() error,
) error {
	f.mu.Lock()
	if err := f.validateLeaseLocked(ctx, claims, lease); err != nil {
		f.mu.Unlock()
		return err
	}
	f.mu.Unlock()
	return call()
}

func (f *fakeGenerationVerifier) ReleaseConnectionLease(
	ctx context.Context,
	claims TokenClaims,
	lease ConnectionLease,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.lease == lease {
		f.lease = ConnectionLease{}
	}
	return nil
}

func (f *fakeGenerationVerifier) validateLeaseLocked(
	ctx context.Context,
	claims TokenClaims,
	lease ConnectionLease,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if claims.Generation != f.latest || f.lease != lease || lease.ID == "" {
		return errConnectionEpochRevoked
	}
	return nil
}

type fakeCommandSource struct {
	mu           sync.Mutex
	commands     []CommandEnvelope
	ackSeq       uint64
	acks         map[uint64]CommandAck
	catchUpCalls uint64
	catchUpDelay time.Duration
	live         chan CommandEnvelope
}

type failingLiveCommandSource struct {
	*fakeCommandSource
	err error
}

func (f *failingLiveCommandSource) Live(ctx context.Context, claims TokenClaims, fromSeq uint64) (<-chan CommandEnvelope, <-chan error, error) {
	commands := make(chan CommandEnvelope)
	errs := make(chan error, 1)
	errs <- f.err
	close(commands)
	close(errs)
	return commands, errs, nil
}

func newFakeCommandSource() *fakeCommandSource {
	return &fakeCommandSource{acks: make(map[uint64]CommandAck), live: make(chan CommandEnvelope, 16)}
}

func (f *fakeCommandSource) FirstCommandSeq(ctx context.Context, claims TokenClaims) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.commands) == 0 {
		return 1, nil
	}
	// Seq values are stored explicitly in each envelope.
	return f.commands[0].Seq, nil
}

func (f *fakeCommandSource) HasCommands(ctx context.Context, claims TokenClaims) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.commands) != 0, nil
}

func (f *fakeCommandSource) NextCommandSeq(ctx context.Context, claims TokenClaims) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.commands) == 0 {
		return 1, nil
	}
	for _, command := range f.commands {
		ack, ok := f.acks[command.Seq]
		if !ok || ack.Status == "received" {
			return command.Seq, nil
		}
	}
	return f.commands[len(f.commands)-1].Seq + 1, nil
}

func (f *fakeCommandSource) CatchUp(ctx context.Context, claims TokenClaims, fromSeq uint64) ([]CommandEnvelope, error) {
	if f.catchUpDelay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(f.catchUpDelay):
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.catchUpCalls++
	var out []CommandEnvelope
	for _, cmd := range f.commands {
		if cmd.Seq >= fromSeq {
			out = append(out, cmd)
		}
	}
	return out, nil
}

func (f *fakeCommandSource) catchUpCount() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.catchUpCalls
}

func (f *fakeCommandSource) Live(ctx context.Context, claims TokenClaims, fromSeq uint64) (<-chan CommandEnvelope, <-chan error, error) {
	return f.live, nil, nil
}

func (f *fakeCommandSource) ApplyAck(ctx context.Context, claims TokenClaims, ack CommandAck) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ackSeq = ack.Seq
	f.acks[ack.Seq] = ack
	return nil
}

func (f *fakeCommandSource) pushCommand(cmd CommandEnvelope) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commands = append(f.commands, cmd)
	select {
	case f.live <- cmd:
	default:
	}
}

type fakeEventSink struct {
	mu        sync.Mutex
	envelopes []Envelope
}

func (f *fakeEventSink) Receive(ctx context.Context, claims TokenClaims, envelope Envelope) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.envelopes = append(f.envelopes, envelope)
	return nil
}

func (f *fakeEventSink) LastReceivedEventSeq(ctx context.Context, claims TokenClaims) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var last uint64
	for _, envelope := range f.envelopes {
		if envelope.Seq != nil && *envelope.Seq > last {
			last = *envelope.Seq
		}
	}
	return last, nil
}

type blockingEventSink struct {
	*fakeEventSink
	entered    chan struct{}
	release    chan struct{}
	deadline   chan time.Duration
	canceled   chan struct{}
	once       sync.Once
	cancelOnce sync.Once
}

func (s *blockingEventSink) Receive(ctx context.Context, claims TokenClaims, envelope Envelope) error {
	if deadline, ok := ctx.Deadline(); ok && s.deadline != nil {
		select {
		case s.deadline <- time.Until(deadline):
		default:
		}
	}
	s.once.Do(func() { close(s.entered) })
	select {
	case <-ctx.Done():
		if s.canceled != nil {
			s.cancelOnce.Do(func() { close(s.canceled) })
		}
		return ctx.Err()
	case <-s.release:
		return s.fakeEventSink.Receive(ctx, claims, envelope)
	}
}

type fakeHydrationLatch struct {
	mu          sync.Mutex
	ready       bool
	terminal    bool
	ch          chan struct{}
	waitStarted chan struct{}
}

func newFakeHydrationLatch() *fakeHydrationLatch {
	return &fakeHydrationLatch{
		ch:          make(chan struct{}),
		waitStarted: make(chan struct{}, 4),
	}
}

func (f *fakeHydrationLatch) WaitFor(ctx context.Context, claims TokenClaims, generation uint64) error {
	f.mu.Lock()
	if f.ready {
		f.mu.Unlock()
		return nil
	}
	ch := f.ch
	f.mu.Unlock()
	select {
	case f.waitStarted <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}

func (f *fakeHydrationLatch) Observe(
	ctx context.Context,
	claims TokenClaims,
	generation uint64,
) (HydrationObservation, error) {
	if err := ctx.Err(); err != nil {
		return HydrationObservation{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return HydrationObservation{
		Ready:            f.ready,
		TerminalNotReady: f.terminal,
	}, nil
}

func (f *fakeHydrationLatch) waitUntilBlocked(t *testing.T) {
	t.Helper()
	select {
	case <-f.waitStarted:
	case <-time.After(time.Second):
		t.Fatal("expected gateway epoch to observe NotReady hydration state")
	}
}

func (f *fakeHydrationLatch) setReady() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.ready {
		f.ready = true
		f.terminal = false
		close(f.ch)
	}
}

func (f *fakeHydrationLatch) setNotReady() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ready {
		f.ready = false
		f.terminal = true
		f.ch = make(chan struct{})
	}
}

func newTestServer(t *testing.T) (*Server, *fakeTokenVerifier, *fakeGenerationVerifier, *fakeCommandSource, *fakeEventSink, *fakeHydrationLatch) {
	tv := &fakeTokenVerifier{}
	gv := &fakeGenerationVerifier{latest: 7}
	cs := newFakeCommandSource()
	es := &fakeEventSink{}
	hl := newFakeHydrationLatch()
	srv := NewServer(tv, gv, cs, es, hl)
	return srv, tv, gv, cs, es, hl
}

func TestWebSocketMissingTokenRejected(t *testing.T) {
	srv, _, _, _, _, _ := newTestServer(t)
	server := startTestServer(t, srv)
	defer server.Close()

	_, resp, err := dialTestWS(t, server, nil)
	if err == nil {
		t.Fatal("expected missing token to be rejected")
	}
	if resp.StatusCode != 401 {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestWebSocketRejectedToken(t *testing.T) {
	srv, tv, _, _, _, _ := newTestServer(t)
	tv.setReject(true)
	server := startTestServer(t, srv)
	defer server.Close()

	header := map[string][]string{"Authorization": {"Bearer bad-token"}}
	_, resp, err := dialTestWS(t, server, header)
	if err == nil {
		t.Fatal("expected rejected token")
	}
	if resp.StatusCode != 401 {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestWebSocketHelloAndCommandCatchUp(t *testing.T) {
	srv, _, _, cs, _, hl := newTestServer(t)
	hl.setReady()

	cmd := testCommandEnvelope(1, "00000000-0000-4000-8000-000000000001", []byte(`{"type":"user_message","text":"hi","attachments":[]}`), "018f47a2-9b3c-7def-8abc-0123456789ab")
	cs.pushCommand(cmd)

	server := startTestServer(t, srv)
	defer server.Close()

	header := map[string][]string{"Authorization": {"Bearer test-token"}}
	conn, resp, err := dialTestWS(t, server, header)
	if err != nil {
		t.Fatalf("dial: %v (status %d)", err, resp.StatusCode)
	}
	defer conn.Close()

	if err := conn.WriteJSON(AgentHello{
		PersonalityAgentID:     "018f47a2-9b3c-7def-8abc-0123456789ab",
		Generation:             7,
		LastSentEventSeq:       0,
		LastReceivedCommandSeq: 0,
		LastAppliedCommandSeq:  0,
	}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	var apiHello ApiHello
	if err := conn.ReadJSON(&apiHello); err != nil {
		t.Fatalf("read api hello: %v", err)
	}
	if apiHello.AcceptedGeneration != 7 || apiHello.NextCommandSeq != 1 {
		t.Fatalf("unexpected api hello: %+v", apiHello)
	}

	var received CommandEnvelope
	if err := conn.ReadJSON(&received); err != nil {
		t.Fatalf("read command: %v", err)
	}
	if received.Seq != 1 {
		t.Fatalf("unexpected command seq: %d", received.Seq)
	}
}

func TestWebSocketHelloUsesDurableEventCursorNotAgentEcho(t *testing.T) {
	srv, _, _, _, es, hl := newTestServer(t)
	hl.setReady()
	seq := uint64(3)
	es.envelopes = []Envelope{{
		Seq:                &seq,
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		Event:              []byte(`{"type":"agent_start"}`),
	}}

	server := startTestServer(t, srv)
	defer server.Close()
	conn, _, err := dialTestWS(t, server, map[string][]string{"Authorization": {"Bearer test-token"}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if err := conn.WriteJSON(AgentHello{
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Generation: 7, LastSentEventSeq: 99,
		LastReceivedCommandSeq: 0, LastAppliedCommandSeq: 0,
	}); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	var hello ApiHello
	if err := conn.ReadJSON(&hello); err != nil {
		t.Fatalf("read api hello: %v", err)
	}
	if hello.LastReceivedEventSeq != 3 {
		t.Fatalf("expected durable event cursor 3, got %d", hello.LastReceivedEventSeq)
	}
}

func TestWebSocketStaleGenerationIsClosed(t *testing.T) {
	srv, _, gv, _, _, hl := newTestServer(t)
	hl.setReady()
	gv.setLatest(99)

	server := startTestServer(t, srv)
	defer server.Close()

	header := map[string][]string{"Authorization": {"Bearer test-token"}}
	conn, _, err := dialTestWS(t, server, header)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(AgentHello{
		PersonalityAgentID:     "018f47a2-9b3c-7def-8abc-0123456789ab",
		Generation:             7,
		LastSentEventSeq:       0,
		LastReceivedCommandSeq: 0,
		LastAppliedCommandSeq:  0,
	}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	var apiHello ApiHello
	if err := conn.ReadJSON(&apiHello); err == nil {
		t.Fatal("expected connection to close on stale generation")
	}
}

func TestWebSocketReadyAfterReconnectHoldsCommands(t *testing.T) {
	srv, _, _, cs, _, hl := newTestServer(t)
	cmd := testCommandEnvelope(1, "00000000-0000-4000-8000-000000000001", []byte(`{"type":"user_message","text":"hi","attachments":[]}`), "018f47a2-9b3c-7def-8abc-0123456789ab")
	cs.pushCommand(cmd)

	server := startTestServer(t, srv)
	defer server.Close()

	header := map[string][]string{"Authorization": {"Bearer test-token"}}

	// First connection: NotReady still permits the fenced hello exchange, but
	// command catch-up remains held. Close it before the latch becomes Ready.
	conn1, _, err := dialTestWS(t, server, header)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := conn1.WriteJSON(AgentHello{
		PersonalityAgentID:     "018f47a2-9b3c-7def-8abc-0123456789ab",
		Generation:             7,
		LastSentEventSeq:       0,
		LastReceivedCommandSeq: 0,
		LastAppliedCommandSeq:  0,
	}); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	hl.waitUntilBlocked(t)
	var apiHello ApiHello
	if err := conn2Wait(conn1, &apiHello, time.Second); err != nil {
		t.Fatalf("NotReady connection did not receive fenced API hello: %v", err)
	}
	if calls := cs.catchUpCount(); calls != 0 {
		t.Fatalf("expected no command catch-up before Ready, got %d calls", calls)
	}
	conn1.Close()

	// Reconnect with the same generation while it is still NotReady. The new
	// epoch must independently observe NotReady and keep command delivery held;
	// it must not inherit a release from the old connection epoch.
	conn2, _, err := dialTestWS(t, server, header)
	if err != nil {
		t.Fatalf("dial reconnect: %v", err)
	}
	defer conn2.Close()
	if err := conn2.WriteJSON(AgentHello{
		PersonalityAgentID:     "018f47a2-9b3c-7def-8abc-0123456789ab",
		Generation:             7,
		LastSentEventSeq:       0,
		LastReceivedCommandSeq: 0,
		LastAppliedCommandSeq:  0,
	}); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	hl.waitUntilBlocked(t)
	if err := conn2Wait(conn2, &apiHello, time.Second); err != nil {
		t.Fatalf("read fenced API hello after reconnect: %v", err)
	}
	if apiHello.NextCommandSeq != 1 {
		t.Fatalf("unexpected next command seq after reconnect: %d", apiHello.NextCommandSeq)
	}
	if calls := cs.catchUpCount(); calls != 0 {
		t.Fatalf("expected no command catch-up before Ready, got %d calls", calls)
	}

	// Only a Ready latch for the same generation may release the new epoch.
	hl.setReady()
	var received CommandEnvelope
	if err := conn2.ReadJSON(&received); err != nil {
		t.Fatalf("read command after reconnect: %v", err)
	}
	if received.Seq != 1 {
		t.Fatalf("unexpected command seq after reconnect: %d", received.Seq)
	}
}

func TestWebSocketNotReadyHelloGatesTrafficUntilReadyAndShutdownFences(t *testing.T) {
	store, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	gateway, err := OpenDurableGateway(t.TempDir(), store)
	if err != nil {
		t.Fatal(err)
	}
	gateway.PollInterval = 5 * time.Millisecond
	authorization := LocalRuntimeAuthorization{
		BearerToken:           "not-ready-control-bearer-32-bytes-minimum",
		TenantID:              "tenant-local",
		PersonalityAgentID:    testPersonalityAgentID,
		Generation:            7,
		RPCBootNonce:          "boot-not-ready",
		Audience:              defaultAgentAudience,
		DeliveryAuthorization: LocalDeliveryRaw,
	}
	control, err := NewLocalControlServer(
		gateway,
		[]byte("not-ready-control-signing-secret-32-bytes-minimum"),
		[]LocalRuntimeAuthorization{authorization},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := control.publishRuntimeState(context.Background(), LocalRuntimeStatePublication{
		PublicationID:      "startup-not-ready",
		PersonalityAgentID: testPersonalityAgentID,
		Generation:         7,
		RPCBootNonce:       "boot-not-ready",
		State:              LocalRuntimeNotReady,
		Reason:             LocalRuntimeStartup,
	}); err != nil {
		t.Fatal(err)
	}
	command, err := store.Append(
		context.Background(),
		testInboundProvenance(testPersonalityAgentID),
		"",
		json.RawMessage(`{"type":"user_message","text":"after-ready","attachments":[]}`),
	)
	if err != nil {
		t.Fatal(err)
	}

	sessions, err := NewHMACUserSessionVerifier(testSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatal(err)
	}
	browserServer := NewBrowserServer(sessions, gateway, gateway)
	browserServer.AllowedOrigins = []string{"https://web.example"}
	browserMux := http.NewServeMux()
	browserMux.Handle("GET /direct-chat/ws", browserServer)
	browserHTTP := httptest.NewServer(browserMux)
	defer browserHTTP.Close()
	browserClaims := userSessionWireClaims{
		TenantID:           "tenant-1",
		HumanID:            "018f47a2-9b3c-7def-8abc-00000000ab01",
		PersonalityAgentID: testPersonalityAgentID,
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                defaultBrowserAudience,
	}
	browser := dialBrowserWS(
		t,
		browserHTTP,
		signBrowserSession(t, testSecret, browserClaims),
		testPersonalityAgentID,
	)
	defer browser.Close()
	if err := browser.WriteJSON(browserHello{Type: "hello", LastEventSeq: 0}); err != nil {
		t.Fatal(err)
	}
	assertDirectChatStatus(t, browser, "unavailable")

	server := NewServer(
		&fakeTokenVerifier{},
		gateway,
		gateway,
		gateway,
		gateway,
	)
	server.GenerationPollInterval = 5 * time.Millisecond
	agentHTTP := startTestServer(t, server)
	defer agentHTTP.Close()
	headers := map[string][]string{"Authorization": {"Bearer test-token"}}
	testDeadline := 5 * time.Second
	connectNotReady := func() *websocket.Conn {
		t.Helper()
		conn, _, err := dialTestWS(t, agentHTTP, headers)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.WriteJSON(AgentHello{
			PersonalityAgentID:     testPersonalityAgentID,
			Generation:             7,
			LastSentEventSeq:       1,
			LastReceivedCommandSeq: 0,
			LastAppliedCommandSeq:  0,
		}); err != nil {
			t.Fatal(err)
		}
		var hello ApiHello
		if err := conn2Wait(conn, &hello, testDeadline); err != nil {
			t.Fatalf("NotReady connection did not receive ApiHello: %v", err)
		}
		if hello.LastReceivedEventSeq != 0 || hello.NextCommandSeq != command.Seq {
			t.Fatalf("unexpected NotReady ApiHello: %+v", hello)
		}
		return conn
	}
	waitInactive := func() {
		t.Helper()
		deadline := time.Now().Add(testDeadline)
		for {
			server.connectionsMu.Lock()
			active := len(server.connections)
			server.connectionsMu.Unlock()
			leaseState, leaseErr := gateway.connectionLeaseState(testPersonalityAgentID)
			if active == 0 && leaseErr == nil && leaseState.present && !leaseState.Active {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("agent connection did not settle: active=%d lease=%+v err=%v", active, leaseState, leaseErr)
			}
			time.Sleep(time.Millisecond)
		}
	}

	eventOffender := connectNotReady()
	seq := uint64(1)
	if err := eventOffender.WriteJSON(OutboundFrame{
		FrameType: "event",
		Envelope: &Envelope{
			Seq:                &seq,
			PersonalityAgentID: testPersonalityAgentID,
			Event:              json.RawMessage(`{"type":"agent_start"}`),
		},
	}); err != nil {
		t.Fatal(err)
	}
	var closedFrame json.RawMessage
	if err := conn2Wait(eventOffender, &closedFrame, testDeadline); err == nil {
		t.Fatal("pre-Ready event did not close the offending connection")
	}
	_ = eventOffender.Close()
	waitInactive()
	if last, err := gateway.LastReceivedEventSeq(
		context.Background(),
		TokenClaims{PersonalityAgentID: testPersonalityAgentID, Generation: 7},
	); err != nil || last != 0 {
		t.Fatalf("pre-Ready event reached durable sink: last=%d err=%v", last, err)
	}

	ackOffender := connectNotReady()
	if err := ackOffender.WriteJSON(OutboundFrame{
		FrameType: "command_ack",
		Ack: &CommandAck{
			PersonalityAgentID: testPersonalityAgentID,
			Seq:                command.Seq,
			CommandID:          command.CommandID,
			Status:             "applied",
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := conn2Wait(ackOffender, &closedFrame, testDeadline); err == nil {
		t.Fatal("pre-Ready ACK did not close the offending connection")
	}
	_ = ackOffender.Close()
	waitInactive()
	if next, err := gateway.NextCommandSeq(
		context.Background(),
		TokenClaims{PersonalityAgentID: testPersonalityAgentID, Generation: 7},
	); err != nil || next != command.Seq {
		t.Fatalf("pre-Ready ACK reached durable sink: next=%d err=%v", next, err)
	}

	agent := connectNotReady()
	defer agent.Close()
	server.connectionsMu.Lock()
	epoch := server.connections[testPersonalityAgentID]
	server.connectionsMu.Unlock()
	if epoch == nil {
		t.Fatal("NotReady connection did not retain its fenced lease")
	}
	commandResult := make(chan error, 1)
	go func() {
		var delivered CommandEnvelope
		err := agent.ReadJSON(&delivered)
		if err == nil && (delivered.Seq != command.Seq || delivered.CommandID != command.CommandID) {
			err = fmt.Errorf("unexpected command after Ready: %+v", delivered)
		}
		commandResult <- err
	}()

	readyReceipt := "ready-receipt-7"
	if _, err := control.publishRuntimeState(context.Background(), LocalRuntimeStatePublication{
		PublicationID:            "ready-exact-generation",
		PersonalityAgentID:       testPersonalityAgentID,
		Generation:               7,
		RPCBootNonce:             "boot-not-ready",
		ExpectedRevision:         revision(1),
		State:                    LocalRuntimeReady,
		HydrationReceiptIdentity: &readyReceipt,
		Reason:                   LocalRuntimeHydrated,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-commandResult:
		if err != nil {
			t.Fatalf("Ready did not unlock exactly one command delivery: %v", err)
		}
	case <-time.After(testDeadline):
		t.Fatal("Ready did not unlock command delivery")
	}
	assertDirectChatStatus(t, browser, "ready")

	if err := agent.WriteJSON(OutboundFrame{
		FrameType: "event",
		Envelope: &Envelope{
			Seq:                &seq,
			PersonalityAgentID: testPersonalityAgentID,
			Event:              json.RawMessage(`{"type":"agent_start"}`),
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := agent.WriteJSON(OutboundFrame{
		FrameType: "command_ack",
		Ack: &CommandAck{
			PersonalityAgentID: testPersonalityAgentID,
			Seq:                command.Seq,
			CommandID:          command.CommandID,
			Status:             "applied",
		},
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(testDeadline)
	for {
		last, eventErr := gateway.LastReceivedEventSeq(
			context.Background(),
			TokenClaims{PersonalityAgentID: testPersonalityAgentID, Generation: 7},
		)
		next, ackErr := gateway.NextCommandSeq(
			context.Background(),
			TokenClaims{PersonalityAgentID: testPersonalityAgentID, Generation: 7},
		)
		if eventErr == nil && ackErr == nil && last == 1 && next == command.Seq+1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("post-Ready traffic did not commit: last=%d eventErr=%v next=%d ackErr=%v", last, eventErr, next, ackErr)
		}
		time.Sleep(time.Millisecond)
	}
	assertBrowserEvent(t, browser, "agent_start", true)

	postCommandRead := make(chan error, 1)
	go func() {
		var extra json.RawMessage
		postCommandRead <- agent.ReadJSON(&extra)
	}()
	if _, err := control.publishRuntimeState(context.Background(), LocalRuntimeStatePublication{
		PublicationID:      "shutdown-not-ready",
		PersonalityAgentID: testPersonalityAgentID,
		Generation:         7,
		RPCBootNonce:       "boot-not-ready",
		ExpectedRevision:   revision(2),
		State:              LocalRuntimeNotReady,
		Reason:             LocalRuntimeShutdown,
	}); err != nil {
		t.Fatal(err)
	}
	assertDirectChatStatus(t, browser, "unavailable")
	if err := browser.WriteJSON(browserCommandFrame{
		Type:           "command",
		IdempotencyKey: "shutdown-command",
		Command:        json.RawMessage(`{"type":"user_message","text":"too late","attachments":[]}`),
	}); err != nil {
		t.Fatal(err)
	}
	var shutdownRejection browserCommandRejectedFrame
	if err := conn2Wait(browser, &shutdownRejection, testDeadline); err != nil {
		t.Fatal(err)
	}
	if shutdownRejection.RejectReason != RejectUnavailable {
		t.Fatalf("shutdown browser command rejection = %+v", shutdownRejection)
	}
	if _, found, err := store.GetCommand(context.Background(), testPersonalityAgentID, command.Seq+1); err != nil || found {
		t.Fatalf("shutdown browser command reached durable log: found=%v err=%v", found, err)
	}
	select {
	case err := <-postCommandRead:
		if err == nil {
			t.Fatal("command was delivered more than once before shutdown fencing")
		}
	case <-time.After(testDeadline):
		t.Fatal("shutdown NotReady did not close the agent connection")
	}
	waitInactive()
	if err := gateway.ValidateConnectionLease(context.Background(), epoch.claims, epoch.lease); err == nil {
		t.Fatal("shutdown NotReady left the old connection lease active")
	}
}

func TestWebSocketAgentSendsAckAndEvent(t *testing.T) {
	srv, _, _, cs, es, hl := newTestServer(t)
	hl.setReady()

	server := startTestServer(t, srv)
	defer server.Close()

	header := map[string][]string{"Authorization": {"Bearer test-token"}}
	conn, _, err := dialTestWS(t, server, header)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(AgentHello{
		PersonalityAgentID:     "018f47a2-9b3c-7def-8abc-0123456789ab",
		Generation:             7,
		LastSentEventSeq:       0,
		LastReceivedCommandSeq: 0,
		LastAppliedCommandSeq:  0,
	}); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	var apiHello ApiHello
	if err := conn2Wait(conn, &apiHello, time.Second); err != nil {
		t.Fatalf("read api hello: %v", err)
	}

	// Send an event and an ack.
	seq1 := uint64(1)
	eventFrame := OutboundFrame{
		FrameType: "event",
		Envelope: &Envelope{
			Seq:                &seq1,
			PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
			Event:              []byte(`{"type":"agent_start"}`),
		},
	}
	if err := conn.WriteJSON(eventFrame); err != nil {
		t.Fatalf("write event: %v", err)
	}
	ackFrame := OutboundFrame{
		FrameType: "command_ack",
		Ack: &CommandAck{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
			Seq:       1,
			CommandID: "00000000-0000-4000-8000-000000000001",
			Status:    "received",
		},
	}
	if err := conn.WriteJSON(ackFrame); err != nil {
		t.Fatalf("write ack: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	es.mu.Lock()
	if len(es.envelopes) != 1 {
		t.Fatalf("expected 1 event, got %d", len(es.envelopes))
	}
	es.mu.Unlock()

	cs.mu.Lock()
	if cs.ackSeq != 1 {
		t.Fatalf("expected ack seq 1, got %d", cs.ackSeq)
	}
	cs.mu.Unlock()
}

func TestWebSocketSameGenerationReconnectRevokesFirstAndNewEpochWorks(t *testing.T) {
	srv, _, _, cs, es, hl := newTestServer(t)
	hl.setReady()
	srv.GenerationPollInterval = 5 * time.Millisecond
	server := startTestServer(t, srv)
	defer server.Close()

	header := map[string][]string{"Authorization": {"Bearer test-token"}}
	first, _, err := dialTestWS(t, server, header)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	writeTestAgentHello(t, first, 7)
	var hello ApiHello
	if err := conn2Wait(first, &hello, time.Second); err != nil {
		t.Fatalf("read first API hello: %v", err)
	}

	second, _, err := dialTestWS(t, server, header)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	writeTestAgentHello(t, second, 7)
	if err := conn2Wait(second, &hello, time.Second); err != nil {
		t.Fatalf("read replacement API hello: %v", err)
	}

	first.SetReadDeadline(time.Now().Add(time.Second))
	if err := first.ReadJSON(&hello); err == nil {
		t.Fatal("first same-generation connection remained active after replacement")
	}

	seq := uint64(1)
	if err := second.WriteJSON(OutboundFrame{
		FrameType: "event",
		Envelope: &Envelope{
			Seq:                &seq,
			PersonalityAgentID: testPersonalityAgentID,
			Event:              []byte(`{"type":"agent_start"}`),
		},
	}); err != nil {
		t.Fatalf("write event on current epoch: %v", err)
	}
	if err := second.WriteJSON(OutboundFrame{
		FrameType: "command_ack",
		Ack: &CommandAck{
			PersonalityAgentID: testPersonalityAgentID,
			Seq:                1,
			CommandID:          "00000000-0000-4000-8000-000000000001",
			Status:             "received",
		},
	}); err != nil {
		t.Fatalf("write ACK on current epoch: %v", err)
	}
	waitForFakeSideEffects(t, es, cs, 1, 1)

	srv.connectionsMu.Lock()
	active := len(srv.connections)
	current := srv.connections[testPersonalityAgentID]
	srv.connectionsMu.Unlock()
	if active != 1 || current == nil || current.lease.Sequence == 0 {
		t.Fatalf("connection registry did not retain exactly the replacement epoch: active=%d", active)
	}
}

func TestWebSocketGenerationRolloverClosesIdleConnection(t *testing.T) {
	srv, _, generation, _, _, hl := newTestServer(t)
	hl.setReady()
	srv.GenerationPollInterval = 5 * time.Millisecond
	server := startTestServer(t, srv)
	defer server.Close()

	conn, _, err := dialTestWS(t, server, map[string][]string{"Authorization": {"Bearer test-token"}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	writeTestAgentHello(t, conn, 7)
	var hello ApiHello
	if err := conn2Wait(conn, &hello, time.Second); err != nil {
		t.Fatalf("read API hello: %v", err)
	}

	generation.setLatest(8)
	conn.SetReadDeadline(time.Now().Add(time.Second))
	if err := conn.ReadJSON(&hello); err == nil {
		t.Fatal("idle stale-generation connection remained open")
	}
	deadline := time.Now().Add(time.Second)
	for {
		srv.connectionsMu.Lock()
		active := len(srv.connections)
		srv.connectionsMu.Unlock()
		if active == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stale epoch remained in registry: active=%d", active)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWebSocketSharedLeaseRevokesConnectionAcrossServerInstances(t *testing.T) {
	store, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runtimeDir := t.TempDir()
	firstGateway, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	secondGateway, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	receipt := "ready-7"
	if err := firstGateway.PublishRuntimeState(testPersonalityAgentID, 7, &receipt); err != nil {
		t.Fatal(err)
	}
	tv := &fakeTokenVerifier{}
	firstServer := NewServer(tv, firstGateway, firstGateway, firstGateway, firstGateway)
	secondServer := NewServer(tv, secondGateway, secondGateway, secondGateway, secondGateway)
	firstServer.GenerationPollInterval = 5 * time.Millisecond
	secondServer.GenerationPollInterval = 5 * time.Millisecond
	firstHTTP := startTestServer(t, firstServer)
	defer firstHTTP.Close()
	secondHTTP := startTestServer(t, secondServer)
	defer secondHTTP.Close()
	headers := map[string][]string{"Authorization": {"Bearer test-token"}}
	testDeadline := 5 * time.Second

	first, _, err := dialTestWS(t, firstHTTP, headers)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	writeTestAgentHello(t, first, 7)
	var hello ApiHello
	if err := conn2Wait(first, &hello, testDeadline); err != nil {
		t.Fatal(err)
	}
	firstServer.connectionsMu.Lock()
	firstEpoch := firstServer.connections[testPersonalityAgentID]
	firstServer.connectionsMu.Unlock()
	if firstEpoch == nil {
		t.Fatal("first Server did not install its shared lease")
	}

	second, _, err := dialTestWS(t, secondHTTP, headers)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	writeTestAgentHello(t, second, 7)
	if err := conn2Wait(second, &hello, testDeadline); err != nil {
		t.Fatalf("second Server did not acquire same-generation lease: %v", err)
	}

	if err := firstGateway.ValidateConnectionLease(
		context.Background(),
		firstEpoch.claims,
		firstEpoch.lease,
	); !errors.Is(err, errConnectionEpochRevoked) {
		t.Fatalf("first Server lease remained authoritative: %v", err)
	}
	seq := uint64(1)
	err = firstGateway.Receive(
		contextWithConnectionLease(context.Background(), firstEpoch.lease),
		firstEpoch.claims,
		Envelope{
			Seq:                &seq,
			PersonalityAgentID: testPersonalityAgentID,
			Event:              json.RawMessage(`{"type":"agent_start"}`),
		},
	)
	if !errors.Is(err, errConnectionEpochRevoked) {
		t.Fatalf("revoked cross-Server event admission was not fenced: %v", err)
	}
	if err := second.WriteJSON(OutboundFrame{
		FrameType: "event",
		Envelope: &Envelope{
			Seq:                &seq,
			PersonalityAgentID: testPersonalityAgentID,
			Event:              json.RawMessage(`{"type":"agent_start"}`),
		},
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(testDeadline)
	for {
		last, err := firstGateway.LastReceivedEventSeq(context.Background(), firstEpoch.claims)
		if err == nil && last == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("current cross-Server epoch did not persist event: last=%d err=%v", last, err)
		}
		time.Sleep(time.Millisecond)
	}

	first.SetReadDeadline(time.Now().Add(testDeadline))
	if err := first.ReadJSON(&hello); err == nil {
		t.Fatal("revoked connection on first Server remained open")
	}
	_ = first.Close()
	_ = second.Close()
	deadline = time.Now().Add(5 * time.Second)
	for {
		firstServer.connectionsMu.Lock()
		firstActive := len(firstServer.connections)
		firstServer.connectionsMu.Unlock()
		secondServer.connectionsMu.Lock()
		secondActive := len(secondServer.connections)
		secondServer.connectionsMu.Unlock()
		leaseState, leaseErr := firstGateway.connectionLeaseState(testPersonalityAgentID)
		if firstActive == 0 && secondActive == 0 &&
			leaseErr == nil && leaseState.present && !leaseState.Active {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"shared lease cleanup did not settle: first=%d second=%d lease=%+v err=%v",
				firstActive,
				secondActive,
				leaseState,
				leaseErr,
			)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWebSocketReplacementClaimsLeaseBeforeSnapshottingDurableCursors(t *testing.T) {
	store, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	command, err := store.Append(
		context.Background(),
		testInboundProvenance(testPersonalityAgentID),
		"",
		json.RawMessage(`{"type":"user_message","text":"lease-race","attachments":[]}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	runtimeDir := t.TempDir()
	firstGateway, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	secondGateway, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	receipt := "ready-7"
	if err := firstGateway.PublishRuntimeState(testPersonalityAgentID, 7, &receipt); err != nil {
		t.Fatal(err)
	}
	tv := &fakeTokenVerifier{}
	firstServer := NewServer(tv, firstGateway, firstGateway, firstGateway, firstGateway)
	firstServer.GenerationPollInterval = 5 * time.Millisecond
	firstHTTP := startTestServer(t, firstServer)
	defer firstHTTP.Close()
	headers := map[string][]string{"Authorization": {"Bearer test-token"}}

	first, _, err := dialTestWS(t, firstHTTP, headers)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	writeTestAgentHello(t, first, 7)
	var hello ApiHello
	if err := conn2Wait(first, &hello, time.Second); err != nil {
		t.Fatal(err)
	}
	var delivered CommandEnvelope
	if err := conn2Wait(first, &delivered, time.Second); err != nil {
		t.Fatalf("first owner did not receive command to acknowledge: %v", err)
	}
	if delivered.Seq != command.Seq || delivered.CommandID != command.CommandID {
		t.Fatalf("first owner received wrong command: %+v", delivered)
	}

	claimEntered := make(chan struct{})
	releaseClaim := make(chan struct{})
	gatedAuthority := &gatedClaimLeaseAuthority{
		DurableGateway: secondGateway,
		entered:        claimEntered,
		release:        releaseClaim,
	}
	secondServer := NewServer(tv, gatedAuthority, secondGateway, secondGateway, secondGateway)
	secondServer.GenerationPollInterval = 5 * time.Millisecond
	secondHTTP := startTestServer(t, secondServer)
	defer secondHTTP.Close()
	second, _, err := dialTestWS(t, secondHTTP, headers)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := second.WriteJSON(AgentHello{
		PersonalityAgentID:     testPersonalityAgentID,
		Generation:             7,
		LastSentEventSeq:       2,
		LastReceivedCommandSeq: command.Seq,
		LastAppliedCommandSeq:  command.Seq,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-claimEntered:
	case <-time.After(time.Second):
		t.Fatal("replacement did not reach authoritative lease claim")
	}

	seq := uint64(1)
	if err := first.WriteJSON(OutboundFrame{
		FrameType: "event",
		Envelope: &Envelope{
			Seq:                &seq,
			PersonalityAgentID: testPersonalityAgentID,
			Event:              json.RawMessage(`{"type":"agent_start"}`),
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := first.WriteJSON(OutboundFrame{
		FrameType: "command_ack",
		Ack: &CommandAck{
			PersonalityAgentID: testPersonalityAgentID,
			Seq:                command.Seq,
			CommandID:          command.CommandID,
			Status:             "applied",
		},
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		last, err := firstGateway.LastReceivedEventSeq(
			context.Background(),
			TokenClaims{PersonalityAgentID: testPersonalityAgentID, Generation: 7},
		)
		next, nextErr := firstGateway.NextCommandSeq(
			context.Background(),
			TokenClaims{PersonalityAgentID: testPersonalityAgentID, Generation: 7},
		)
		if err == nil && nextErr == nil && last == 1 && next == command.Seq+1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"predecessor did not commit cursors N+1 before replacement claim: event=%d eventErr=%v next=%d nextErr=%v",
				last,
				err,
				next,
				nextErr,
			)
		}
		time.Sleep(time.Millisecond)
	}
	close(releaseClaim)

	if err := conn2Wait(second, &hello, time.Second); err != nil {
		t.Fatalf("replacement did not complete hello: %v", err)
	}
	if hello.LastReceivedEventSeq != 1 || hello.NextCommandSeq != command.Seq+1 {
		t.Fatalf("replacement hello used a pre-claim snapshot: %+v", hello)
	}

	seq = 2
	if err := second.WriteJSON(OutboundFrame{
		FrameType: "event",
		Envelope: &Envelope{
			Seq:                &seq,
			PersonalityAgentID: testPersonalityAgentID,
			Event:              json.RawMessage(`{"type":"agent_end"}`),
		},
	}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(time.Second)
	for {
		last, err := secondGateway.LastReceivedEventSeq(
			context.Background(),
			TokenClaims{PersonalityAgentID: testPersonalityAgentID, Generation: 7},
		)
		if err == nil && last == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replacement was torn down or replay cursor was wrong: last=%d err=%v", last, err)
		}
		time.Sleep(time.Millisecond)
	}

	_ = first.Close()
	_ = second.Close()
	deadline = time.Now().Add(5 * time.Second)
	for {
		firstServer.connectionsMu.Lock()
		firstActive := len(firstServer.connections)
		firstServer.connectionsMu.Unlock()
		secondServer.connectionsMu.Lock()
		secondActive := len(secondServer.connections)
		secondServer.connectionsMu.Unlock()
		leaseState, leaseErr := firstGateway.connectionLeaseState(testPersonalityAgentID)
		if firstActive == 0 && secondActive == 0 &&
			leaseErr == nil && leaseState.present && !leaseState.Active {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"claim-before-snapshot cleanup did not settle: first=%d second=%d lease=%+v err=%v",
				firstActive,
				secondActive,
				leaseState,
				leaseErr,
			)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWebSocketSharedLeaseReconnectDrainsContextWaitingSinkWithinBound(t *testing.T) {
	store, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runtimeDir := t.TempDir()
	firstGateway, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	secondGateway, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	receipt := "ready-7"
	if err := firstGateway.PublishRuntimeState(testPersonalityAgentID, 7, &receipt); err != nil {
		t.Fatal(err)
	}
	blockedEvents := &blockingEventSink{
		fakeEventSink: &fakeEventSink{},
		entered:       make(chan struct{}),
		release:       make(chan struct{}),
		deadline:      make(chan time.Duration, 1),
		canceled:      make(chan struct{}),
	}
	tv := &fakeTokenVerifier{}
	firstServer := NewServer(tv, firstGateway, firstGateway, blockedEvents, firstGateway)
	secondServer := NewServer(tv, secondGateway, secondGateway, secondGateway, secondGateway)
	firstServer.SideEffectTimeout = 50 * time.Millisecond
	firstServer.GenerationPollInterval = 5 * time.Millisecond
	secondServer.GenerationPollInterval = 5 * time.Millisecond
	firstHTTP, firstHandlerDone := startTrackedTestServer(t, firstServer)
	secondHTTP, secondHandlerDone := startTrackedTestServer(t, secondServer)
	headers := map[string][]string{"Authorization": {"Bearer test-token"}}

	var first, second *websocket.Conn
	t.Cleanup(func() {
		waiters := make(map[string]<-chan struct{}, 2)
		if first != nil {
			_ = first.Close()
			waiters["first"] = firstHandlerDone
		}
		if second != nil {
			_ = second.Close()
			waiters["second"] = secondHandlerDone
		}
		firstHTTP.Close()
		secondHTTP.Close()
		for name, done := range waiters {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Errorf("%s hijacked websocket handler did not settle", name)
			}
		}
	})

	first, _, err = dialTestWS(t, firstHTTP, headers)
	if err != nil {
		t.Fatal(err)
	}
	writeTestAgentHello(t, first, 7)
	var hello ApiHello
	if err := conn2Wait(first, &hello, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	seq := uint64(1)
	if err := first.WriteJSON(OutboundFrame{
		FrameType: "event",
		Envelope: &Envelope{
			Seq:                &seq,
			PersonalityAgentID: testPersonalityAgentID,
			Event:              json.RawMessage(`{"type":"agent_start"}`),
		},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-blockedEvents.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("old Server sink did not enter the shared lease boundary")
	}
	select {
	case remaining := <-blockedEvents.deadline:
		if remaining > firstServer.SideEffectTimeout+10*time.Millisecond {
			t.Fatalf(
				"sink received a cancellation deadline beyond the product bound: remaining=%v bound=%v",
				remaining,
				firstServer.SideEffectTimeout,
			)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("old Server sink did not receive a bounded context deadline")
	}

	second, _, err = dialTestWS(t, secondHTTP, headers)
	if err != nil {
		t.Fatal(err)
	}
	writeTestAgentHello(t, second, 7)
	if err := conn2Wait(second, &hello, 5*time.Second); err != nil {
		t.Fatalf("cross-Server reconnect did not drain bounded sink: %v", err)
	}
	select {
	case <-blockedEvents.canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("bounded sink did not observe its supplied context cancellation")
	}
	blockedEvents.fakeEventSink.mu.Lock()
	received := len(blockedEvents.fakeEventSink.envelopes)
	blockedEvents.fakeEventSink.mu.Unlock()
	if received != 0 {
		t.Fatalf("timed-out old sink committed %d events", received)
	}
	close(blockedEvents.release)

	_ = first.Close()
	_ = second.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		firstServer.connectionsMu.Lock()
		firstActive := len(firstServer.connections)
		firstServer.connectionsMu.Unlock()
		secondServer.connectionsMu.Lock()
		secondActive := len(secondServer.connections)
		secondServer.connectionsMu.Unlock()
		leaseState, leaseErr := firstGateway.connectionLeaseState(testPersonalityAgentID)
		if firstActive == 0 && secondActive == 0 &&
			leaseErr == nil && leaseState.present && !leaseState.Active {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"bounded sink test cleanup did not settle: first=%d second=%d lease=%+v err=%v",
				firstActive,
				secondActive,
				leaseState,
				leaseErr,
			)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSideEffectCancellationViolationRetainsLeaseUntilCallbackReturns(t *testing.T) {
	store, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runtimeDir := t.TempDir()
	firstGateway, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	secondGateway, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	receipt := "ready-7"
	if err := firstGateway.PublishRuntimeState(testPersonalityAgentID, 7, &receipt); err != nil {
		t.Fatal(err)
	}
	claims := TokenClaims{
		TenantID:           "tenant-a",
		PersonalityAgentID: testPersonalityAgentID,
		Generation:         7,
	}
	lease, err := firstGateway.ClaimConnectionLease(context.Background(), claims)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(&fakeTokenVerifier{}, firstGateway, firstGateway, firstGateway, firstGateway)
	server.SideEffectTimeout = 25 * time.Millisecond
	epoch := &agentConnectionEpoch{claims: claims, lease: lease}

	entered := make(chan struct{})
	expired := make(chan struct{})
	release := make(chan struct{})
	committed := make(chan struct{})
	effectDone := make(chan error, 1)
	go func() {
		effectDone <- server.withSideEffectLease(
			context.Background(),
			epoch,
			func(effectCtx context.Context) error {
				close(entered)
				<-effectCtx.Done()
				close(expired)
				<-release // Deliberately ignore cancellation until the test releases it.
				close(committed)
				return nil
			},
		)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("side-effect callback did not enter the shared lease")
	}
	select {
	case <-expired:
	case <-time.After(time.Second):
		t.Fatal("side-effect callback did not observe its independent deadline")
	}

	claimDone := make(chan error, 1)
	go func() {
		_, err := secondGateway.ClaimConnectionLease(context.Background(), claims)
		claimDone <- err
	}()
	select {
	case err := <-claimDone:
		t.Fatalf("replacement claim crossed a still-running stale callback: %v", err)
	case <-time.After(75 * time.Millisecond):
	}

	close(release)
	err = <-effectDone
	var contractErr *SideEffectCancellationContractError
	if !errors.Is(err, ErrSideEffectCancellationContract) ||
		!errors.Is(err, context.DeadlineExceeded) ||
		!errors.As(err, &contractErr) {
		t.Fatalf("ctx-ignoring callback did not return typed contract violation: %T %v", err, err)
	}
	select {
	case <-committed:
	default:
		t.Fatal("side-effect result returned before the callback ended")
	}
	select {
	case err := <-claimDone:
		if err != nil {
			t.Fatalf("replacement claim failed after stale callback returned: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("replacement claim did not proceed after stale callback returned")
	}
}

func TestConnectionLeaseDelayedOldInstallerCannotEvictNewGeneration(t *testing.T) {
	gateway := openRuntimeGateway(t)
	oldClaims := currentRuntimeClaims(t, gateway, testPersonalityAgentID)
	oldLease, err := gateway.ClaimConnectionLease(context.Background(), oldClaims)
	if err != nil {
		t.Fatal(err)
	}

	newClaims := oldClaims
	newClaims.Generation++
	if err := gateway.PublishRuntimeState(testPersonalityAgentID, newClaims.Generation, nil); err != nil {
		t.Fatal(err)
	}
	newLease, err := gateway.ClaimConnectionLease(context.Background(), newClaims)
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(&fakeTokenVerifier{}, gateway, gateway, gateway, gateway)
	newCtx, newCancel := context.WithCancel(context.Background())
	defer newCancel()
	newEpoch, err := srv.installConnectionEpoch(
		newCtx,
		&websocket.Conn{},
		newClaims,
		newLease,
		newCancel,
	)
	if err != nil {
		t.Fatalf("install new generation: %v", err)
	}

	oldCtx, oldCancel := context.WithCancel(context.Background())
	defer oldCancel()
	if _, err := srv.installConnectionEpoch(
		oldCtx,
		&websocket.Conn{},
		oldClaims,
		oldLease,
		oldCancel,
	); err == nil {
		t.Fatal("delayed old generation installer displaced the new epoch")
	}
	srv.connectionsMu.Lock()
	current := srv.connections[testPersonalityAgentID]
	srv.connectionsMu.Unlock()
	if current != newEpoch {
		t.Fatal("failed old install changed the local current epoch")
	}
	srv.removeConnectionEpoch(newEpoch)
	if err := gateway.ReleaseConnectionLease(context.Background(), newClaims, newLease); err != nil {
		t.Fatal(err)
	}
}

func TestConnectionLeaseLowerGenerationDoesNotCancelLocalCurrent(t *testing.T) {
	srv, _, _, _, _, _ := newTestServer(t)
	currentCtx, currentCancel := context.WithCancel(context.Background())
	defer currentCancel()
	srv.connections[testPersonalityAgentID] = &agentConnectionEpoch{
		personalityAgentID: testPersonalityAgentID,
		claims: TokenClaims{
			PersonalityAgentID: testPersonalityAgentID,
			Generation:         8,
		},
		conn:   &websocket.Conn{},
		cancel: currentCancel,
	}
	srv.cancelLocalPredecessor(TokenClaims{
		PersonalityAgentID: testPersonalityAgentID,
		Generation:         7,
	})
	select {
	case <-currentCtx.Done():
		t.Fatal("lower generation canceled the newer local epoch")
	default:
	}
}

func TestWebSocketReplacementCancelsOldEpochSinkWithoutWaiting(t *testing.T) {
	tv := &fakeTokenVerifier{}
	gv := &fakeGenerationVerifier{latest: 7}
	cs := newFakeCommandSource()
	events := &blockingEventSink{
		fakeEventSink: &fakeEventSink{},
		entered:       make(chan struct{}),
		release:       make(chan struct{}),
	}
	hl := newFakeHydrationLatch()
	hl.setReady()
	srv := NewServer(tv, gv, cs, events, hl)
	server := startTestServer(t, srv)
	defer server.Close()
	header := map[string][]string{"Authorization": {"Bearer test-token"}}

	first, _, err := dialTestWS(t, server, header)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	writeTestAgentHello(t, first, 7)
	var hello ApiHello
	if err := conn2Wait(first, &hello, time.Second); err != nil {
		t.Fatal(err)
	}
	seq := uint64(1)
	if err := first.WriteJSON(OutboundFrame{
		FrameType: "event",
		Envelope: &Envelope{
			Seq:                &seq,
			PersonalityAgentID: testPersonalityAgentID,
			Event:              []byte(`{"type":"agent_start"}`),
		},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-events.entered:
	case <-time.After(time.Second):
		t.Fatal("old epoch side effect did not enter sink")
	}

	second, _, err := dialTestWS(t, server, header)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := second.WriteJSON(AgentHello{
		PersonalityAgentID:     testPersonalityAgentID,
		Generation:             7,
		LastSentEventSeq:       1,
		LastReceivedCommandSeq: 0,
		LastAppliedCommandSeq:  0,
	}); err != nil {
		t.Fatalf("write replacement hello: %v", err)
	}
	secondHello := make(chan error, 1)
	go func() {
		var got ApiHello
		secondHello <- second.ReadJSON(&got)
	}()
	select {
	case err := <-secondHello:
		if err != nil {
			t.Fatalf("replacement API hello while old sink awaited cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement waited on an old sink that was waiting for context cancellation")
	}
	waitForFakeSideEffects(t, events.fakeEventSink, cs, 0, 0)
	close(events.release)

	// Once the replacement hello is visible, the first epoch is no longer
	// current. Frames attempted through it cannot reach either synchronous sink.
	_ = first.WriteJSON(OutboundFrame{
		FrameType: "event",
		Envelope: &Envelope{
			PersonalityAgentID: testPersonalityAgentID,
			Event:              []byte(`{"type":"message_update","message_id":"00000000-0000-4000-8000-000000000001","event":{"type":"text_delta","content_index":0,"delta":"stale"}}`),
		},
	})
	_ = first.WriteJSON(OutboundFrame{
		FrameType: "command_ack",
		Ack: &CommandAck{
			PersonalityAgentID: testPersonalityAgentID,
			Seq:                2,
			CommandID:          "00000000-0000-4000-8000-000000000002",
			Status:             "applied",
		},
	})
	time.Sleep(50 * time.Millisecond)
	waitForFakeSideEffects(t, events.fakeEventSink, cs, 0, 0)
}

func TestWebSocketRejectsEventForAnotherPersonalityAgent(t *testing.T) {
	srv, _, _, _, es, hl := newTestServer(t)
	hl.setReady()

	server := startTestServer(t, srv)
	defer server.Close()

	header := map[string][]string{"Authorization": {"Bearer test-token"}}
	conn, _, err := dialTestWS(t, server, header)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(AgentHello{
		PersonalityAgentID:     "018f47a2-9b3c-7def-8abc-0123456789ab",
		Generation:             7,
		LastSentEventSeq:       0,
		LastReceivedCommandSeq: 0,
		LastAppliedCommandSeq:  0,
	}); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	var apiHello ApiHello
	if err := conn2Wait(conn, &apiHello, time.Second); err != nil {
		t.Fatalf("read api hello: %v", err)
	}

	seq1 := uint64(1)
	if err := conn.WriteJSON(OutboundFrame{
		FrameType: "event",
		Envelope: &Envelope{
			Seq:                &seq1,
			PersonalityAgentID: "018f47a2-9b3c-7def-9abc-0123456789ac",
			Event:              []byte(`{"type":"agent_start"}`),
		},
	}); err != nil {
		t.Fatalf("write mismatched event: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(time.Second))
	defer conn.SetReadDeadline(time.Time{})
	if err := conn.ReadJSON(&apiHello); err == nil {
		t.Fatal("expected personality agent mismatch to close the connection")
	}

	es.mu.Lock()
	defer es.mu.Unlock()
	if len(es.envelopes) != 0 {
		t.Fatalf("expected no event delivery on personality agent mismatch, got %d", len(es.envelopes))
	}
}

func TestWebSocketOriginPolicy(t *testing.T) {
	srv, _, _, _, _, _ := newTestServer(t)
	wildcardRequest := httptest.NewRequest(http.MethodGet, "http://example.test/agent/ws", nil)
	wildcardRequest.Header.Set("Origin", "http://evil.example")
	srv.AllowedOrigins = []string{"*"}
	if srv.checkOrigin(wildcardRequest) {
		t.Fatal("wildcard origin configuration must not disable origin checks")
	}

	server := startTestServer(t, srv)
	defer server.Close()

	wsURL := strings.Replace(server.URL, "http", "ws", 1) + "/agent/ws"
	header := map[string][]string{"Authorization": {"Bearer test-token"}}

	// Native agents authenticate without Origin. Browser requests still need a
	// matching allow-listed origin below.
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("expected authenticated native handshake without Origin: %v", err)
	}
	conn.Close()
	_, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err == nil {
		t.Fatal("expected unauthenticated origin-less handshake to be rejected")
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}

	// Wrong origin is rejected.
	header["Origin"] = []string{"http://evil.example"}
	_, resp, err = websocket.DefaultDialer.Dial(wsURL, header)
	if err == nil {
		t.Fatal("expected mismatched origin to be rejected")
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}

	// Allowed origin upgrades.
	header["Origin"] = []string{server.URL}
	conn, _, err = websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("expected allowed origin to upgrade: %v", err)
	}
	conn.Close()
}

func TestWebSocketCatchUpFromLastAppliedDoesNotSkip(t *testing.T) {
	srv, _, _, cs, _, hl := newTestServer(t)
	hl.setReady()

	for i := 1; i <= 2; i++ {
		cs.pushCommand(testCommandEnvelope(
			uint64(i),
			fmt.Sprintf("00000000-0000-4000-8000-%012d", i),
			[]byte(`{"type":"user_message","text":"hi","attachments":[]}`),
			"018f47a2-9b3c-7def-8abc-0123456789ab",
		))
	}
	if err := cs.ApplyAck(context.Background(), TokenClaims{}, CommandAck{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		Seq:       1,
		CommandID: "00000000-0000-4000-8000-000000000001",
		Status:    "applied",
	}); err != nil {
		t.Fatalf("seed durable terminal ACK: %v", err)
	}

	server := startTestServer(t, srv)
	defer server.Close()

	header := map[string][]string{"Authorization": {"Bearer test-token"}}
	conn, resp, err := dialTestWS(t, server, header)
	if err != nil {
		t.Fatalf("dial: %v (status %d)", err, resp.StatusCode)
	}
	defer conn.Close()

	if err := conn.WriteJSON(AgentHello{
		PersonalityAgentID:     "018f47a2-9b3c-7def-8abc-0123456789ab",
		Generation:             7,
		LastSentEventSeq:       0,
		LastReceivedCommandSeq: 1,
		LastAppliedCommandSeq:  1,
	}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	var apiHello ApiHello
	if err := conn.ReadJSON(&apiHello); err != nil {
		t.Fatalf("read api hello: %v", err)
	}
	if apiHello.NextCommandSeq != 2 {
		t.Fatalf("expected NextCommandSeq 2, got %d", apiHello.NextCommandSeq)
	}

	var received CommandEnvelope
	if err := conn.ReadJSON(&received); err != nil {
		t.Fatalf("read command: %v", err)
	}
	if received.Seq != 2 {
		t.Fatalf("expected catch-up command seq 2, got %d", received.Seq)
	}
}

func TestWebSocketDurableAckGapOverridesAgentLastAppliedCursor(t *testing.T) {
	srv, _, _, cs, _, hl := newTestServer(t)
	hl.setReady()
	cs.pushCommand(testCommandEnvelope(1, "00000000-0000-4000-8000-000000000001", []byte(`{"type":"user_message","text":"hi","attachments":[]}`), "018f47a2-9b3c-7def-8abc-0123456789ab"))
	if err := cs.ApplyAck(context.Background(), TokenClaims{}, CommandAck{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Seq: 1, CommandID: "00000000-0000-4000-8000-000000000001", Status: "received"}); err != nil {
		t.Fatal(err)
	}
	server := startTestServer(t, srv)
	defer server.Close()
	conn, _, err := dialTestWS(t, server, map[string][]string{"Authorization": {"Bearer test-token"}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.WriteJSON(AgentHello{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Generation: 7, LastReceivedCommandSeq: 1, LastAppliedCommandSeq: 1}); err != nil {
		t.Fatal(err)
	}
	var hello ApiHello
	if err := conn.ReadJSON(&hello); err != nil {
		t.Fatal(err)
	}
	if hello.NextCommandSeq != 1 {
		t.Fatalf("nonterminal durable ACK must override agent cursor: got %d", hello.NextCommandSeq)
	}
	var replay CommandEnvelope
	if err := conn.ReadJSON(&replay); err != nil {
		t.Fatal(err)
	}
	if replay.Seq != 1 {
		t.Fatalf("expected replay seq 1, got %d", replay.Seq)
	}
}

func TestWebSocketRejectsEmptyCommandLogAfterDurableProgress(t *testing.T) {
	srv, _, _, _, _, hl := newTestServer(t)
	hl.setReady()
	server := startTestServer(t, srv)
	defer server.Close()

	conn, resp, err := dialTestWS(t, server, map[string][]string{"Authorization": {"Bearer test-token"}})
	if err != nil {
		t.Fatalf("dial: %v (status %d)", err, resp.StatusCode)
	}
	defer conn.Close()
	if err := conn.WriteJSON(AgentHello{
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Generation: 7,
		LastReceivedCommandSeq: 1, LastAppliedCommandSeq: 1,
	}); err != nil {
		t.Fatal(err)
	}
	var apiHello ApiHello
	if err := conn.ReadJSON(&apiHello); err == nil {
		t.Fatal("empty command log must not acknowledge durable agent progress")
	}
}

func TestWebSocketRejectsInvalidCommandID(t *testing.T) {
	srv, _, _, cs, _, hl := newTestServer(t)
	hl.setReady()
	cs.pushCommand(testCommandEnvelope(1, "not-a-canonical-uuid", []byte(`{"type":"user_message","text":"hi","attachments":[]}`), "018f47a2-9b3c-7def-8abc-0123456789ab"))

	server := startTestServer(t, srv)
	defer server.Close()

	header := map[string][]string{"Authorization": {"Bearer test-token"}}
	conn, resp, err := dialTestWS(t, server, header)
	if err != nil {
		t.Fatalf("dial: %v (status %d)", err, resp.StatusCode)
	}
	defer conn.Close()

	if err := conn.WriteJSON(AgentHello{
		PersonalityAgentID:     "018f47a2-9b3c-7def-8abc-0123456789ab",
		Generation:             7,
		LastSentEventSeq:       0,
		LastReceivedCommandSeq: 0,
		LastAppliedCommandSeq:  0,
	}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	var apiHello ApiHello
	if err := conn.ReadJSON(&apiHello); err != nil {
		t.Fatalf("read api hello: %v", err)
	}
	if apiHello.NextCommandSeq != 1 {
		t.Fatalf("expected NextCommandSeq 1, got %d", apiHello.NextCommandSeq)
	}

	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	defer conn.SetReadDeadline(time.Time{})
	var received CommandEnvelope
	if err := conn.ReadJSON(&received); err == nil {
		t.Fatal("expected connection to close on invalid command_id")
	}
}

func TestWebSocketCatchUpGapFromLastReceivedCommandSeq(t *testing.T) {
	srv, _, _, cs, _, hl := newTestServer(t)
	hl.setReady()

	// The retained log begins at seq 5, but the agent has only received up to 3.
	cs.pushCommand(testCommandEnvelope(5, "00000000-0000-4000-8000-000000000005", []byte(`{"type":"user_message","text":"hi","attachments":[]}`), "018f47a2-9b3c-7def-8abc-0123456789ab"))

	server := startTestServer(t, srv)
	defer server.Close()

	header := map[string][]string{"Authorization": {"Bearer test-token"}}
	conn, resp, err := dialTestWS(t, server, header)
	if err != nil {
		t.Fatalf("dial: %v (status %d)", err, resp.StatusCode)
	}
	defer conn.Close()

	if err := conn.WriteJSON(AgentHello{
		PersonalityAgentID:     "018f47a2-9b3c-7def-8abc-0123456789ab",
		Generation:             7,
		LastSentEventSeq:       0,
		LastReceivedCommandSeq: 3,
		LastAppliedCommandSeq:  2,
	}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	var apiHello ApiHello
	if err := conn.ReadJSON(&apiHello); err == nil {
		t.Fatal("expected connection to close when retained log begins beyond agent's last received seq")
	}
}

func TestWebSocketOversizedFrameClosesConnection(t *testing.T) {
	srv, _, _, _, _, hl := newTestServer(t)
	hl.setReady()
	const readLimit = 1024
	srv.MaxReadLimit = readLimit

	server := startTestServer(t, srv)
	defer server.Close()

	header := map[string][]string{"Authorization": {"Bearer test-token"}}
	conn, resp, err := dialTestWS(t, server, header)
	if err != nil {
		t.Fatalf("dial: %v (status %d)", err, resp.StatusCode)
	}
	defer conn.Close()

	if err := conn.WriteJSON(AgentHello{
		PersonalityAgentID:     "018f47a2-9b3c-7def-8abc-0123456789ab",
		Generation:             7,
		LastSentEventSeq:       0,
		LastReceivedCommandSeq: 0,
		LastAppliedCommandSeq:  0,
	}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	var apiHello ApiHello
	if err := conn.ReadJSON(&apiHello); err != nil {
		t.Fatalf("read api hello: %v", err)
	}

	// The limit leaves ample room for AgentHello. This structurally valid frame
	// is deliberately more than twice the post-hello limit.
	largeEvent := `{"frame_type":"event","envelope":{"seq":1,"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"error","message":"` + strings.Repeat("x", 2*readLimit) + `"}}}`
	if err := conn.WriteMessage(websocket.TextMessage, []byte(largeEvent)); err != nil {
		t.Fatalf("write oversized frame: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	defer conn.SetReadDeadline(time.Time{})
	var frame OutboundFrame
	if err := conn.ReadJSON(&frame); err == nil {
		t.Fatal("expected connection to close on oversized read")
	}
}

func TestNewServerConfiguresBoundedWriteTimeout(t *testing.T) {
	srv, _, _, _, _, _ := newTestServer(t)
	if srv.WriteTimeout <= 0 {
		t.Fatalf("WriteTimeout = %v, want a positive bound", srv.WriteTimeout)
	}
}

func TestServerRejectsUnboundedLivenessConfiguration(t *testing.T) {
	tests := []struct {
		name         string
		pongWait     time.Duration
		pingInterval time.Duration
	}{
		{name: "missing pong bound", pongWait: 0, pingInterval: time.Second},
		{name: "missing ping cadence", pongWait: time.Second, pingInterval: 0},
		{name: "ping equals pong bound", pongWait: time.Second, pingInterval: time.Second},
		{name: "ping exceeds pong bound", pongWait: time.Second, pingInterval: 2 * time.Second},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			srv, _, _, _, _, _ := newTestServer(t)
			srv.PongWait = test.pongWait
			srv.PingInterval = test.pingInterval
			if err := srv.validateLivenessConfig(); err == nil {
				t.Fatal("unbounded liveness configuration was accepted")
			}
		})
	}
}

func TestWritePumpClosedErrorChannelDoesNotSpinWithoutPing(t *testing.T) {
	srv, _, _, _, _, _ := newTestServer(t)
	srv.PingInterval = 0
	live := make(chan CommandEnvelope)
	liveErr := make(chan error)
	close(liveErr)

	done := make(chan error, 1)
	go func() { done <- srv.writePump(context.Background(), &agentConnectionEpoch{}, live, liveErr) }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "command source closed") {
			t.Fatalf("expected closed source error, got %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("write pump spun on closed liveErr instead of exiting")
	}
}

func TestWritePumpPreservesSourceErrorAfterCommandsClose(t *testing.T) {
	srv, _, _, _, _, _ := newTestServer(t)
	srv.PingInterval = 0
	live := make(chan CommandEnvelope)
	liveErr := make(chan error, 1)
	close(live)
	liveErr <- errors.New("durable source failed")
	close(liveErr)
	if err := srv.writePump(context.Background(), &agentConnectionEpoch{}, live, liveErr); err == nil || !strings.Contains(err.Error(), "durable source failed") {
		t.Fatalf("expected source error after commands close, got %v", err)
	}
}

func TestWebSocketPumpFailureUnblocksPeerReadWithoutPongWait(t *testing.T) {
	tv := &fakeTokenVerifier{}
	gv := &fakeGenerationVerifier{latest: 7}
	commands := &failingLiveCommandSource{fakeCommandSource: newFakeCommandSource(), err: errors.New("live source failed")}
	es := &fakeEventSink{}
	hl := newFakeHydrationLatch()
	hl.setReady()
	srv := NewServer(tv, gv, commands, es, hl)
	srv.PongWait = 5 * time.Second
	srv.PingInterval = 4 * time.Second

	server := startTestServer(t, srv)
	defer server.Close()
	conn, _, err := dialTestWS(t, server, map[string][]string{"Authorization": {"Bearer test-token"}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.WriteJSON(AgentHello{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Generation: 7}); err != nil {
		t.Fatal(err)
	}
	var hello ApiHello
	if err := conn.ReadJSON(&hello); err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	conn.SetReadDeadline(started.Add(500 * time.Millisecond))
	var frame OutboundFrame
	if err := conn.ReadJSON(&frame); err == nil {
		t.Fatal("expected peer read to unblock after writer failure")
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("peer read waited %v instead of immediate pump teardown", elapsed)
	}
}

func TestWebSocketReadFailureClosesIdleWriterWithoutPongWait(t *testing.T) {
	srv, _, _, _, _, hl := newTestServer(t)
	hl.setReady()
	srv.PongWait = 5 * time.Second
	srv.PingInterval = 4 * time.Second
	server := startTestServer(t, srv)
	defer server.Close()
	conn, _, err := dialTestWS(t, server, map[string][]string{"Authorization": {"Bearer test-token"}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.WriteJSON(AgentHello{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Generation: 7}); err != nil {
		t.Fatal(err)
	}
	var hello ApiHello
	if err := conn.ReadJSON(&hello); err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"frame_type":"unknown"}`)); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	conn.SetReadDeadline(started.Add(500 * time.Millisecond))
	var frame OutboundFrame
	if err := conn.ReadJSON(&frame); err == nil {
		t.Fatal("expected connection close after read-pump validation failure")
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("idle writer survived sibling failure for %v", elapsed)
	}
}

func TestServerWriteDeadlineUsesConfiguredTimeout(t *testing.T) {
	srv, _, _, _, _, _ := newTestServer(t)
	srv.WriteTimeout = time.Second
	before := time.Now()
	deadline := srv.writeDeadline()
	if deadline.Before(before.Add(900*time.Millisecond)) || deadline.After(before.Add(1100*time.Millisecond)) {
		t.Fatalf("write deadline = %v, want approximately one second after %v", deadline, before)
	}
}

func TestWebSocketSilentPeerClosesConnection(t *testing.T) {
	srv, _, _, commands, _, _ := newTestServer(t)
	srv.PongWait = 100 * time.Millisecond
	srv.PingInterval = 20 * time.Millisecond

	server := startTestServer(t, srv)
	defer server.Close()

	header := map[string][]string{"Authorization": {"Bearer test-token"}}
	conn, resp, err := dialTestWS(t, server, header)
	if err != nil {
		t.Fatalf("dial: %v (status %d)", err, resp.StatusCode)
	}
	defer conn.Close()

	if err := conn.WriteJSON(AgentHello{
		PersonalityAgentID:     "018f47a2-9b3c-7def-8abc-0123456789ab",
		Generation:             7,
		LastSentEventSeq:       0,
		LastReceivedCommandSeq: 0,
		LastAppliedCommandSeq:  0,
	}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	var apiHello ApiHello
	if err := conn.ReadJSON(&apiHello); err != nil {
		t.Fatalf("read api hello: %v", err)
	}

	// Disable the client's automatic pong response so the server sees a silent peer.
	conn.SetPingHandler(func(string) error { return nil })

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer conn.SetReadDeadline(time.Time{})
	var received CommandEnvelope
	if err := conn.ReadJSON(&received); err == nil {
		t.Fatal("expected pre-Ready connection to close on silent peer (no pong)")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		srv.connectionsMu.Lock()
		active := len(srv.connections)
		srv.connectionsMu.Unlock()
		if active == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("silent pre-Ready peer remained registered: active=%d", active)
		}
		time.Sleep(time.Millisecond)
	}
	if calls := commands.catchUpCount(); calls != 0 {
		t.Fatalf("silent pre-Ready peer reached command catch-up: calls=%d", calls)
	}
}

func TestWebSocketAuthenticatedHeartbeatOutlivesHelloTimeoutBeforeReady(t *testing.T) {
	srv, _, _, commands, events, hl := newTestServer(t)
	srv.HelloTimeout = 30 * time.Millisecond
	srv.PongWait = 250 * time.Millisecond
	srv.PingInterval = 20 * time.Millisecond

	server := startTestServer(t, srv)
	defer server.Close()
	conn, _, err := dialTestWS(t, server, map[string][]string{"Authorization": {"Bearer test-token"}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.WriteJSON(AgentHello{
		PersonalityAgentID: testPersonalityAgentID,
		Generation:         7,
	}); err != nil {
		t.Fatal(err)
	}
	var hello ApiHello
	if err := conn2Wait(conn, &hello, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	hl.waitUntilBlocked(t)

	pings := make(chan struct{}, 16)
	conn.SetPingHandler(func(data string) error {
		select {
		case pings <- struct{}{}:
		default:
		}
		return conn.WriteControl(
			websocket.PongMessage,
			[]byte(data),
			time.Now().Add(time.Second),
		)
	})
	readDone := make(chan error, 1)
	go func() {
		for {
			var frame json.RawMessage
			if err := conn.ReadJSON(&frame); err != nil {
				readDone <- err
				return
			}
		}
	}()

	started := time.Now()
	observedPings := 0
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for observedPings < 4 || time.Since(started) <= 2*srv.HelloTimeout {
		select {
		case <-pings:
			observedPings++
		case err := <-readDone:
			t.Fatalf("authenticated pre-Ready heartbeat closed early: %v", err)
		case <-deadline.C:
			t.Fatalf(
				"authenticated pre-Ready heartbeat did not remain active: pings=%d elapsed=%v",
				observedPings,
				time.Since(started),
			)
		}
	}
	if calls := commands.catchUpCount(); calls != 0 {
		t.Fatalf("pre-Ready heartbeat unlocked command catch-up: calls=%d", calls)
	}

	hl.setReady()
	seq := uint64(1)
	if err := conn.WriteJSON(OutboundFrame{
		FrameType: "event",
		Envelope: &Envelope{
			Seq:                &seq,
			PersonalityAgentID: testPersonalityAgentID,
			Event:              json.RawMessage(`{"type":"agent_start"}`),
		},
	}); err != nil {
		t.Fatal(err)
	}
	eventDeadline := time.Now().Add(5 * time.Second)
	for {
		events.mu.Lock()
		count := len(events.envelopes)
		events.mu.Unlock()
		if count == 1 {
			break
		}
		if time.Now().After(eventDeadline) {
			t.Fatal("Ready did not unlock traffic after the pre-Ready heartbeat window")
		}
		time.Sleep(time.Millisecond)
	}
	_ = conn.Close()
	select {
	case <-readDone:
	case <-time.After(5 * time.Second):
		t.Fatal("client reader did not stop after heartbeat test connection closed")
	}
}

func TestWebSocketCatchUpDoesNotConsumeInitialPongWait(t *testing.T) {
	srv, _, _, commands, events, hl := newTestServer(t)
	hl.setReady()
	srv.PongWait = 80 * time.Millisecond
	srv.PingInterval = 20 * time.Millisecond
	commands.catchUpDelay = 200 * time.Millisecond

	server := startTestServer(t, srv)
	defer server.Close()
	conn, _, err := dialTestWS(t, server, map[string][]string{"Authorization": {"Bearer test-token"}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.WriteJSON(AgentHello{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Generation: 7}); err != nil {
		t.Fatal(err)
	}
	var hello ApiHello
	if err := conn.ReadJSON(&hello); err != nil {
		t.Fatal(err)
	}
	conn.SetPingHandler(func(data string) error {
		return conn.WriteControl(
			websocket.PongMessage,
			[]byte(data),
			time.Now().Add(time.Second),
		)
	})
	readDone := make(chan error, 1)
	go func() {
		for {
			var frame json.RawMessage
			if err := conn.ReadJSON(&frame); err != nil {
				readDone <- err
				return
			}
		}
	}()

	catchUpDeadline := time.Now().Add(5 * time.Second)
	for commands.catchUpCount() == 0 {
		if time.Now().After(catchUpDeadline) {
			t.Fatal("slow catch-up did not complete while heartbeats were active")
		}
		select {
		case err := <-readDone:
			t.Fatalf("heartbeat did not keep the connection alive through catch-up: %v", err)
		default:
		}
		time.Sleep(time.Millisecond)
	}

	seq := uint64(1)
	if err := conn.WriteJSON(OutboundFrame{
		FrameType: "event",
		Envelope: &Envelope{
			Seq:                &seq,
			PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
			Event:              []byte(`{"type":"agent_start"}`),
		},
	}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		events.mu.Lock()
		count := len(events.envelopes)
		events.mu.Unlock()
		if count == 1 {
			_ = conn.Close()
			select {
			case <-readDone:
			case <-time.After(5 * time.Second):
				t.Fatal("client reader did not stop after catch-up connection closed")
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("event sent after slow catch-up was not received")
}

func TestWebSocketHelloRejectsCheckedAddOverflow(t *testing.T) {
	srv, _, _, _, _, hl := newTestServer(t)
	hl.setReady()

	server := startTestServer(t, srv)
	defer server.Close()

	conn, resp, err := dialTestWS(t, server, map[string][]string{"Authorization": {"Bearer test-token"}})
	if err != nil {
		t.Fatalf("dial: %v (status %d)", err, resp.StatusCode)
	}
	defer conn.Close()

	// last_applied_command_seq at the JSON-safe maximum would require
	// next_command_seq = max + 1. The server must fail closed and not marshal
	// an out-of-range ApiHello.
	if err := conn.WriteJSON(AgentHello{
		PersonalityAgentID:     "018f47a2-9b3c-7def-8abc-0123456789ab",
		Generation:             7,
		LastSentEventSeq:       0,
		LastReceivedCommandSeq: 0,
		LastAppliedCommandSeq:  maxJSONSafeInteger,
	}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	var apiHello ApiHello
	if err := conn.ReadJSON(&apiHello); err == nil {
		t.Fatal("expected connection to close when next_command_seq would overflow JSON-safe range")
	}
}

func startTestServer(t *testing.T, srv *Server) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(srv)
	server.Start()
	srv.AllowedOrigins = []string{server.URL}
	return server
}

func startTrackedTestServer(t *testing.T, srv *Server) (*httptest.Server, <-chan struct{}) {
	t.Helper()
	done := make(chan struct{}, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { done <- struct{}{} }()
		srv.ServeHTTP(w, r)
	}))
	server.Start()
	srv.AllowedOrigins = []string{server.URL}
	return server, done
}

func dialTestWS(t *testing.T, server *httptest.Server, headers map[string][]string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	if headers == nil {
		headers = make(map[string][]string)
	}
	headers["Origin"] = []string{server.URL}
	return websocket.DefaultDialer.Dial(strings.Replace(server.URL, "http", "ws", 1)+"/agent/ws", headers)
}

func conn2Wait(conn *websocket.Conn, v any, timeout time.Duration) error {
	conn.SetReadDeadline(time.Now().Add(timeout))
	defer conn.SetReadDeadline(time.Time{})
	return conn.ReadJSON(v)
}

func writeTestAgentHello(t testing.TB, conn *websocket.Conn, generation uint64) {
	t.Helper()
	if err := conn.WriteJSON(AgentHello{
		PersonalityAgentID:     testPersonalityAgentID,
		Generation:             generation,
		LastReceivedCommandSeq: 0,
		LastAppliedCommandSeq:  0,
	}); err != nil {
		t.Fatalf("write agent hello: %v", err)
	}
}

func waitForFakeSideEffects(
	t testing.TB,
	events *fakeEventSink,
	commands *fakeCommandSource,
	eventCount int,
	ackSeq uint64,
) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		events.mu.Lock()
		gotEvents := len(events.envelopes)
		events.mu.Unlock()
		commands.mu.Lock()
		gotAck := commands.ackSeq
		commands.mu.Unlock()
		if gotEvents == eventCount && gotAck == ackSeq {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"side effects did not reach expected state: events=%d/%d ack=%d/%d",
				gotEvents,
				eventCount,
				gotAck,
				ackSeq,
			)
		}
		time.Sleep(time.Millisecond)
	}
}
