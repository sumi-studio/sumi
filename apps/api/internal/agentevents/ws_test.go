package agentevents

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type fakeTokenVerifier struct {
	reject bool
}

func (f *fakeTokenVerifier) Verify(ctx context.Context, token string) (TokenClaims, error) {
	if f.reject || token == "" {
		return TokenClaims{}, fmt.Errorf("rejected")
	}
	return TokenClaims{
		TenantID:       "tenant-1",
		AgentID:        "agent-1",
		ConversationID: "conversation-1",
		Generation:     7,
	}, nil
}

type fakeGenerationVerifier struct {
	mu     sync.Mutex
	latest uint64
}

func (f *fakeGenerationVerifier) VerifyGeneration(ctx context.Context, agentID string, generation uint64) error {
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

type fakeCommandSource struct {
	mu       sync.Mutex
	commands []CommandEnvelope
	ackSeq   uint64
	live     chan CommandEnvelope
}

func newFakeCommandSource() *fakeCommandSource {
	return &fakeCommandSource{live: make(chan CommandEnvelope, 16)}
}

func (f *fakeCommandSource) NextCommandSeq(ctx context.Context, agentID string, generation uint64) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.commands) == 0 {
		return 1, nil
	}
	// Seq values are stored explicitly in each envelope.
	return f.commands[0].Seq, nil
}

func (f *fakeCommandSource) CatchUp(ctx context.Context, agentID string, generation uint64, fromSeq uint64) ([]CommandEnvelope, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []CommandEnvelope
	for _, cmd := range f.commands {
		if cmd.Seq >= fromSeq {
			out = append(out, cmd)
		}
	}
	return out, nil
}

func (f *fakeCommandSource) Live(ctx context.Context, agentID string, generation uint64) (<-chan CommandEnvelope, error) {
	return f.live, nil
}

func (f *fakeCommandSource) ApplyAck(ctx context.Context, agentID string, generation uint64, ack CommandAck) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ackSeq = ack.Seq
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

func (f *fakeEventSink) Receive(ctx context.Context, agentID string, generation uint64, envelope Envelope) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.envelopes = append(f.envelopes, envelope)
	return nil
}

type fakeHydrationLatch struct {
	mu    sync.Mutex
	ready bool
	ch    chan struct{}
}

func newFakeHydrationLatch() *fakeHydrationLatch {
	return &fakeHydrationLatch{ch: make(chan struct{})}
}

func (f *fakeHydrationLatch) WaitFor(ctx context.Context, generation uint64) error {
	f.mu.Lock()
	if f.ready {
		f.mu.Unlock()
		return nil
	}
	ch := f.ch
	f.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}

func (f *fakeHydrationLatch) setReady() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.ready {
		f.ready = true
		close(f.ch)
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
	server := httptest.NewServer(srv)
	defer server.Close()

	wsURL := strings.Replace(server.URL, "http", "ws", 1) + "/agent/ws"
	_, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err == nil {
		t.Fatal("expected missing token to be rejected")
	}
	if resp.StatusCode != 401 {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestWebSocketRejectedToken(t *testing.T) {
	srv, tv, _, _, _, _ := newTestServer(t)
	tv.reject = true
	server := httptest.NewServer(srv)
	defer server.Close()

	wsURL := strings.Replace(server.URL, "http", "ws", 1) + "/agent/ws"
	header := map[string][]string{"Authorization": {"Bearer bad-token"}}
	_, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
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

	cmd := CommandEnvelope{
		Seq:       1,
		CommandID: "00000000-0000-4000-8000-000000000001",
		Command:   []byte(`{"type":"user_message","text":"hi","attachments":[]}`),
	}
	cs.pushCommand(cmd)

	server := httptest.NewServer(srv)
	defer server.Close()

	wsURL := strings.Replace(server.URL, "http", "ws", 1) + "/agent/ws"
	header := map[string][]string{"Authorization": {"Bearer test-token"}}
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("dial: %v (status %d)", err, resp.StatusCode)
	}
	defer conn.Close()

	if err := conn.WriteJSON(AgentHello{
		AgentID:                "agent-1",
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

func TestWebSocketStaleGenerationIsClosed(t *testing.T) {
	srv, _, gv, _, _, hl := newTestServer(t)
	hl.setReady()
	gv.setLatest(99)

	server := httptest.NewServer(srv)
	defer server.Close()

	wsURL := strings.Replace(server.URL, "http", "ws", 1) + "/agent/ws"
	header := map[string][]string{"Authorization": {"Bearer test-token"}}
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(AgentHello{
		AgentID:                "agent-1",
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
	cmd := CommandEnvelope{
		Seq:       1,
		CommandID: "00000000-0000-4000-8000-000000000001",
		Command:   []byte(`{"type":"user_message","text":"hi","attachments":[]}`),
	}
	cs.pushCommand(cmd)

	server := httptest.NewServer(srv)
	defer server.Close()

	wsURL := strings.Replace(server.URL, "http", "ws", 1) + "/agent/ws"
	header := map[string][]string{"Authorization": {"Bearer test-token"}}

	// First connection: NotReady, so hello succeeds up to latch wait, then
	// blocks. Close it before latch becomes Ready.
	conn1, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := conn1.WriteJSON(AgentHello{
		AgentID:                "agent-1",
		Generation:             7,
		LastSentEventSeq:       0,
		LastReceivedCommandSeq: 0,
		LastAppliedCommandSeq:  0,
	}); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	conn1.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	var apiHello ApiHello
	if err := conn1.ReadJSON(&apiHello); err == nil {
		t.Fatal("expected first connection to wait for hydration")
	}
	conn1.Close()

	// Latch becomes Ready for this generation.
	hl.setReady()

	// Reconnect with the same generation; the new epoch must observe Ready and
	// deliver the pending command.
	conn2, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("dial reconnect: %v", err)
	}
	defer conn2.Close()
	if err := conn2.WriteJSON(AgentHello{
		AgentID:                "agent-1",
		Generation:             7,
		LastSentEventSeq:       0,
		LastReceivedCommandSeq: 0,
		LastAppliedCommandSeq:  0,
	}); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	if err := conn2.ReadJSON(&apiHello); err != nil {
		t.Fatalf("read api hello after reconnect: %v", err)
	}
	if apiHello.NextCommandSeq != 1 {
		t.Fatalf("unexpected next command seq after reconnect: %d", apiHello.NextCommandSeq)
	}

	var received CommandEnvelope
	if err := conn2.ReadJSON(&received); err != nil {
		t.Fatalf("read command after reconnect: %v", err)
	}
	if received.Seq != 1 {
		t.Fatalf("unexpected command seq after reconnect: %d", received.Seq)
	}
}

func TestWebSocketAgentSendsAckAndEvent(t *testing.T) {
	srv, _, _, cs, es, hl := newTestServer(t)
	hl.setReady()

	server := httptest.NewServer(srv)
	defer server.Close()

	wsURL := strings.Replace(server.URL, "http", "ws", 1) + "/agent/ws"
	header := map[string][]string{"Authorization": {"Bearer test-token"}}
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(AgentHello{
		AgentID:                "agent-1",
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
	eventFrame := OutboundFrame{
		FrameType: "event",
		Envelope: &Envelope{
			ConversationID: "conversation-1",
			Event:          []byte(`{"type":"agent_start"}`),
		},
	}
	if err := conn.WriteJSON(eventFrame); err != nil {
		t.Fatalf("write event: %v", err)
	}
	ackFrame := OutboundFrame{
		FrameType: "command_ack",
		Ack: &CommandAck{
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

func conn2Wait(conn *websocket.Conn, v any, timeout time.Duration) error {
	conn.SetReadDeadline(time.Now().Add(timeout))
	defer conn.SetReadDeadline(time.Time{})
	return conn.ReadJSON(v)
}
