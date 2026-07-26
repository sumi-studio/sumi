package agentevents

import (
	"context"
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
	mu           sync.Mutex
	commands     []CommandEnvelope
	ackSeq       uint64
	catchUpCalls uint64
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
	return &fakeCommandSource{live: make(chan CommandEnvelope, 16)}
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

func (f *fakeCommandSource) CatchUp(ctx context.Context, claims TokenClaims, fromSeq uint64) ([]CommandEnvelope, error) {
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

type fakeHydrationLatch struct {
	mu          sync.Mutex
	ready       bool
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

	cmd := CommandEnvelope{
		Seq:       1,
		CommandID: "00000000-0000-4000-8000-000000000001",
		Command:   []byte(`{"type":"user_message","text":"hi","attachments":[]}`),
	}
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

func TestWebSocketHelloUsesDurableEventCursorNotAgentEcho(t *testing.T) {
	srv, _, _, _, es, hl := newTestServer(t)
	hl.setReady()
	seq := uint64(3)
	es.envelopes = []Envelope{{
		Seq:            &seq,
		ConversationID: "conversation-1",
		Event:          []byte(`{"type":"agent_start"}`),
	}}

	server := startTestServer(t, srv)
	defer server.Close()
	conn, _, err := dialTestWS(t, server, map[string][]string{"Authorization": {"Bearer test-token"}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if err := conn.WriteJSON(AgentHello{
		AgentID: "agent-1", Generation: 7, LastSentEventSeq: 99,
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

	server := startTestServer(t, srv)
	defer server.Close()

	header := map[string][]string{"Authorization": {"Bearer test-token"}}

	// First connection: NotReady, so hello succeeds up to latch wait, then
	// blocks. Close it before latch becomes Ready.
	conn1, _, err := dialTestWS(t, server, header)
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
	hl.waitUntilBlocked(t)
	conn1.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	var apiHello ApiHello
	if err := conn1.ReadJSON(&apiHello); err == nil {
		t.Fatal("expected first connection to wait for hydration")
	}
	conn1.Close()

	// Reconnect with the same generation while it is still NotReady. The new
	// epoch must independently observe NotReady and keep its hello/command
	// path held; it must not inherit a release from the old connection epoch.
	conn2, _, err := dialTestWS(t, server, header)
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
	hl.waitUntilBlocked(t)
	if calls := cs.catchUpCount(); calls != 0 {
		t.Fatalf("expected no command catch-up before Ready, got %d calls", calls)
	}

	// Only a Ready latch for the same generation may release the new epoch.
	hl.setReady()
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

	server := startTestServer(t, srv)
	defer server.Close()

	header := map[string][]string{"Authorization": {"Bearer test-token"}}
	conn, _, err := dialTestWS(t, server, header)
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
	seq1 := uint64(1)
	eventFrame := OutboundFrame{
		FrameType: "event",
		Envelope: &Envelope{
			Seq:            &seq1,
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

func TestWebSocketRejectsEventForAnotherConversation(t *testing.T) {
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

	seq1 := uint64(1)
	if err := conn.WriteJSON(OutboundFrame{
		FrameType: "event",
		Envelope: &Envelope{
			Seq:            &seq1,
			ConversationID: "other-conversation",
			Event:          []byte(`{"type":"agent_start"}`),
		},
	}); err != nil {
		t.Fatalf("write mismatched event: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(time.Second))
	defer conn.SetReadDeadline(time.Time{})
	if err := conn.ReadJSON(&apiHello); err == nil {
		t.Fatal("expected conversation mismatch to close the connection")
	}

	es.mu.Lock()
	defer es.mu.Unlock()
	if len(es.envelopes) != 0 {
		t.Fatalf("expected no event delivery on conversation mismatch, got %d", len(es.envelopes))
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
		cs.pushCommand(CommandEnvelope{
			Seq:       uint64(i),
			CommandID: fmt.Sprintf("00000000-0000-4000-8000-%012d", i),
			Command:   []byte(`{"type":"user_message","text":"hi","attachments":[]}`),
		})
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
		AgentID:                "agent-1",
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
		AgentID: "agent-1", Generation: 7,
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
	cs.pushCommand(CommandEnvelope{
		Seq:       1,
		CommandID: "not-a-canonical-uuid",
		Command:   []byte(`{"type":"user_message","text":"hi","attachments":[]}`),
	})

	server := startTestServer(t, srv)
	defer server.Close()

	header := map[string][]string{"Authorization": {"Bearer test-token"}}
	conn, resp, err := dialTestWS(t, server, header)
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
	cs.pushCommand(CommandEnvelope{
		Seq:       5,
		CommandID: "00000000-0000-4000-8000-000000000005",
		Command:   []byte(`{"type":"user_message","text":"hi","attachments":[]}`),
	})

	server := startTestServer(t, srv)
	defer server.Close()

	header := map[string][]string{"Authorization": {"Bearer test-token"}}
	conn, resp, err := dialTestWS(t, server, header)
	if err != nil {
		t.Fatalf("dial: %v (status %d)", err, resp.StatusCode)
	}
	defer conn.Close()

	if err := conn.WriteJSON(AgentHello{
		AgentID:                "agent-1",
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

	// The limit leaves ample room for AgentHello. This structurally valid frame
	// is deliberately more than twice the post-hello limit.
	largeEvent := `{"frame_type":"event","envelope":{"seq":1,"conversation_id":"conversation-1","event":{"type":"error","message":"` + strings.Repeat("x", 2*readLimit) + `"}}}`
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

func TestWritePumpClosedErrorChannelDoesNotSpinWithoutPing(t *testing.T) {
	srv, _, _, _, _, _ := newTestServer(t)
	srv.PingInterval = 0
	live := make(chan CommandEnvelope)
	liveErr := make(chan error)
	close(liveErr)

	done := make(chan error, 1)
	go func() { done <- srv.writePump(context.Background(), nil, live, liveErr) }()
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
	if err := srv.writePump(context.Background(), nil, live, liveErr); err == nil || !strings.Contains(err.Error(), "durable source failed") {
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
	srv.PingInterval = time.Hour

	server := startTestServer(t, srv)
	defer server.Close()
	conn, _, err := dialTestWS(t, server, map[string][]string{"Authorization": {"Bearer test-token"}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.WriteJSON(AgentHello{AgentID: "agent-1", Generation: 7}); err != nil {
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
	srv, _, _, _, _, hl := newTestServer(t)
	hl.setReady()
	srv.PongWait = 100 * time.Millisecond
	srv.PingInterval = 50 * time.Millisecond

	server := startTestServer(t, srv)
	defer server.Close()

	header := map[string][]string{"Authorization": {"Bearer test-token"}}
	conn, resp, err := dialTestWS(t, server, header)
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

	// Disable the client's automatic pong response so the server sees a silent peer.
	conn.SetPingHandler(func(string) error { return nil })

	conn.SetReadDeadline(time.Now().Add(time.Second))
	defer conn.SetReadDeadline(time.Time{})
	var received CommandEnvelope
	if err := conn.ReadJSON(&received); err == nil {
		t.Fatal("expected connection to close on silent peer (no pong)")
	}
}

func startTestServer(t *testing.T, srv *Server) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(srv)
	server.Start()
	srv.AllowedOrigins = []string{server.URL}
	return server
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
