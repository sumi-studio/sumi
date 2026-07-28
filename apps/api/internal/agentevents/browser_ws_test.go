package agentevents

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestBrowserWebSocketAdmitsCommandsAndStreamsDurableAndVolatileEvents(t *testing.T) {
	gateway := openRuntimeGateway(t)
	sessions, err := NewHMACUserSessionVerifier(testSecret, "")
	if err != nil {
		t.Fatal(err)
	}
	server := NewBrowserServer(sessions, gateway.commands, gateway)
	server.AllowedOrigins = []string{"https://web.example"}
	mux := http.NewServeMux()
	mux.Handle("GET /conversations/{conversation_id}/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	claims := userSessionWireClaims{TenantID: "tenant-1", UserID: "user-1", ConversationID: "conversation-1", Exp: time.Now().Add(time.Hour).Unix(), Aud: defaultBrowserAudience}
	conn := dialBrowserWS(t, httpServer, signBrowserSession(t, testSecret, claims), "conversation-1")
	defer conn.Close()
	if err := conn.WriteJSON(browserHello{Type: "hello", LastEventSeq: 0}); err != nil {
		t.Fatal(err)
	}

	seq := uint64(1)
	agentClaims := TokenClaims{TenantID: "tenant-1", AgentID: "agent-1", ConversationID: "conversation-1", Generation: 7}
	// Drive the abort guard from the durable run lifecycle, not internal map
	// mutation.
	if err := gateway.Receive(context.Background(), agentClaims, Envelope{Seq: &seq, ConversationID: "conversation-1", Event: json.RawMessage(`{"type":"agent_start"}`)}); err != nil {
		t.Fatalf("persist durable agent_start: %v", err)
	}
	if replay, err := gateway.EventCatchUp(context.Background(), "conversation-1", 0); err != nil || len(replay) != 1 {
		t.Fatalf("read durable event for browser replay: events=%d err=%v", len(replay), err)
	}
	assertBrowserEvent(t, conn, "agent_start", true)

	seq = 2
	if err := gateway.Receive(context.Background(), agentClaims, Envelope{Seq: &seq, ConversationID: "conversation-1", Event: json.RawMessage(`{"type":"tool_execution_start","tool_call_id":"call-1","tool_name":"read_file","args":{}}`)}); err != nil {
		t.Fatalf("persist durable tool event: %v", err)
	}
	assertBrowserEvent(t, conn, "tool_execution_start", true)
	volatile := Envelope{ConversationID: "conversation-1", Event: json.RawMessage(`{"type":"message_update","message_id":"00000000-0000-4000-8000-000000000001","event":{"type":"text_delta","content_index":0,"delta":"stream"}}`)}
	if err := gateway.Receive(context.Background(), agentClaims, volatile); err != nil {
		t.Fatalf("publish volatile stream event: %v", err)
	}
	assertBrowserEvent(t, conn, "message_update", false)

	seq = 3
	if err := gateway.Receive(context.Background(), agentClaims, Envelope{Seq: &seq, ConversationID: "conversation-1", Event: json.RawMessage(`{"type":"approval_requested","request":{"id":"request-1","tool_call_id":"call-1","tool_name":"read_file","action":{"reviewable":"read"},"args_summary":"read"}}`)}); err != nil {
		t.Fatalf("publish durable approval_requested: %v", err)
	}
	assertBrowserEvent(t, conn, "approval_requested", true)

	// The abort and approval_decision variants are only accepted when a run is
	// in flight and an approval is pending, respectively.
	for index, command := range []json.RawMessage{
		json.RawMessage(`{"type":"user_message","text":"steer me","attachments":[]}`),
		json.RawMessage(`{"type":"abort"}`),
		json.RawMessage(`{"type":"approval_decision","request_id":"request-1","decision":{"type":"approve_once"}}`),
	} {
		if err := conn.WriteJSON(browserCommandFrame{Type: "command", IdempotencyKey: fmt.Sprintf("idempotency-%d", index), Command: command}); err != nil {
			t.Fatal(err)
		}
		var accepted browserCommandAcceptedFrame
		conn.SetReadDeadline(time.Now().Add(time.Second))
		if err := conn.ReadJSON(&accepted); err != nil {
			t.Fatalf("read command admission: %v", err)
		}
		if accepted.Type != "command_accepted" || accepted.Envelope.Seq == 0 || accepted.Envelope.CommandID == "" || len(accepted.Envelope.Command) == 0 {
			t.Fatalf("unexpected command admission: %+v", accepted)
		}
	}
}

func TestBrowserWebSocketRejectsMissingExpiredAndWrongConversationSessions(t *testing.T) {
	gateway := openRuntimeGateway(t)
	sessions, err := NewHMACUserSessionVerifier(testSecret, "")
	if err != nil {
		t.Fatal(err)
	}
	server := NewBrowserServer(sessions, gateway.commands, gateway)
	server.AllowedOrigins = []string{"https://web.example"}
	mux := http.NewServeMux()
	mux.Handle("GET /conversations/{conversation_id}/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	wsURL := strings.Replace(httpServer.URL, "http", "ws", 1) + "/conversations/conversation-1/ws"
	for _, test := range []struct {
		name   string
		cookie string
	}{
		{"missing", ""},
		{"expired", signBrowserSession(t, testSecret, userSessionWireClaims{TenantID: "tenant", UserID: "user", ConversationID: "conversation-1", Exp: time.Now().Add(-time.Hour).Unix(), Aud: defaultBrowserAudience})},
		{"wrong-conversation", signBrowserSession(t, testSecret, userSessionWireClaims{TenantID: "tenant", UserID: "user", ConversationID: "other", Exp: time.Now().Add(time.Hour).Unix(), Aud: defaultBrowserAudience})},
	} {
		t.Run(test.name, func(t *testing.T) {
			header := http.Header{"Origin": {"https://web.example"}}
			if test.cookie != "" {
				header.Set("Cookie", BrowserSessionCookie+"="+test.cookie)
			}
			conn, response, err := websocket.DefaultDialer.Dial(wsURL, header)
			if conn != nil {
				conn.Close()
			}
			if err == nil || response == nil || (response.StatusCode != http.StatusUnauthorized && response.StatusCode != http.StatusForbidden) {
				t.Fatalf("expected session rejection, response=%v err=%v", response, err)
			}
		})
	}
}

func TestBrowserWebSocketReconnectsFromDurableCursor(t *testing.T) {
	gateway := openRuntimeGateway(t)
	sessions, err := NewHMACUserSessionVerifier(testSecret, "")
	if err != nil {
		t.Fatal(err)
	}
	server := NewBrowserServer(sessions, gateway.commands, gateway)
	server.AllowedOrigins = []string{"https://web.example"}
	mux := http.NewServeMux()
	mux.Handle("GET /conversations/{conversation_id}/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()
	cookie := signBrowserSession(t, testSecret, userSessionWireClaims{TenantID: "tenant", UserID: "user", ConversationID: "conversation-1", Exp: time.Now().Add(time.Hour).Unix(), Aud: defaultBrowserAudience})
	claims := TokenClaims{TenantID: "tenant", AgentID: "agent", ConversationID: "conversation-1", Generation: 1}
	seq := uint64(1)
	if err := gateway.Receive(context.Background(), claims, Envelope{Seq: &seq, ConversationID: "conversation-1", Event: json.RawMessage(`{"type":"agent_start"}`)}); err != nil {
		t.Fatal(err)
	}
	first := dialBrowserWS(t, httpServer, cookie, "conversation-1")
	waitForBrowserConnectionStats(t, server, BrowserConnectionStats{Active: 1, Accepted: 1})
	if err := first.WriteJSON(browserHello{Type: "hello", LastEventSeq: 0}); err != nil {
		t.Fatal(err)
	}
	assertBrowserEvent(t, first, "agent_start", true)
	_ = first.Close()
	waitForBrowserConnectionStats(t, server, BrowserConnectionStats{Active: 0, Accepted: 1})
	seq = 2
	if err := gateway.Receive(context.Background(), claims, Envelope{Seq: &seq, ConversationID: "conversation-1", Event: json.RawMessage(`{"type":"agent_end"}`)}); err != nil {
		t.Fatal(err)
	}
	second := dialBrowserWS(t, httpServer, cookie, "conversation-1")
	defer second.Close()
	waitForBrowserConnectionStats(t, server, BrowserConnectionStats{Active: 1, Accepted: 2})
	if err := second.WriteJSON(browserHello{Type: "hello", LastEventSeq: 1}); err != nil {
		t.Fatal(err)
	}
	assertBrowserEvent(t, second, "agent_end", true)
}

func waitForBrowserConnectionStats(t *testing.T, server *BrowserServer, want BrowserConnectionStats) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		got := server.ConnectionStats()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("browser connection stats did not settle: got %+v, want %+v", got, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func dialBrowserWS(t *testing.T, server *httptest.Server, cookie, conversationID string) *websocket.Conn {
	t.Helper()
	wsURL := strings.Replace(server.URL, "http", "ws", 1) + "/conversations/" + conversationID + "/ws"
	header := http.Header{"Origin": {"https://web.example"}, "Cookie": {BrowserSessionCookie + "=" + cookie}}
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("dial browser websocket: %v", err)
	}
	return conn
}

func assertBrowserEvent(t *testing.T, conn *websocket.Conn, eventType string, durable bool) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(time.Second))
	var frame browserEventFrame
	if err := conn.ReadJSON(&frame); err != nil {
		t.Fatalf("read browser event: %v", err)
	}
	if frame.Type != "event" || (frame.Envelope.Seq != nil) != durable {
		t.Fatalf("unexpected browser event frame: %+v", frame)
	}
	var event struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(frame.Envelope.Event, &event); err != nil || event.Type != eventType {
		t.Fatalf("unexpected event: %s (%v)", frame.Envelope.Event, err)
	}
}

func signBrowserSession(t *testing.T, secret []byte, claims userSessionWireClaims) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(header + "." + encoded))
	return header + "." + encoded + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func TestBrowserServerCommandStateGuards(t *testing.T) {
	gateway := openRuntimeGateway(t)
	sessions, err := NewHMACUserSessionVerifier(testSecret, "")
	if err != nil {
		t.Fatal(err)
	}
	server := NewBrowserServer(sessions, gateway.commands, gateway)

	const conversationID = "conversation-1"
	claims := TokenClaims{TenantID: "tenant", AgentID: "agent", ConversationID: conversationID, Generation: 1}

	if reason, reject := server.checkCommandState(conversationID, browserCommandHead{Type: "abort"}); !reject {
		t.Fatal("expected abort to be rejected when no run is in flight")
	} else if reason != RejectNotAllowed {
		t.Fatalf("expected not_allowed, got %q", reason)
	}

	if reason, reject := server.checkCommandState(conversationID, browserCommandHead{Type: "approval_decision", RequestID: "request-1"}); !reject {
		t.Fatal("expected approval_decision to be rejected when no approval is pending")
	} else if reason != RejectNotAllowed {
		t.Fatalf("expected not_allowed, got %q", reason)
	}

	seq := uint64(1)
	if err := gateway.Receive(context.Background(), claims, Envelope{Seq: &seq, ConversationID: conversationID, Event: json.RawMessage(`{"type":"agent_start"}`)}); err != nil {
		t.Fatalf("receive agent_start: %v", err)
	}
	if reason, reject := server.checkCommandState(conversationID, browserCommandHead{Type: "abort"}); reject {
		t.Fatalf("expected abort to be accepted during in-flight run, got %q", reason)
	}

	seq = 2
	if err := gateway.Receive(context.Background(), claims, Envelope{Seq: &seq, ConversationID: conversationID, Event: json.RawMessage(`{"type":"approval_requested","request":{"id":"request-1","tool_call_id":"call-1","tool_name":"read_file","action":{"reviewable":"read"},"args_summary":"read"}}`)}); err != nil {
		t.Fatalf("receive approval_requested: %v", err)
	}
	if reason, reject := server.checkCommandState(conversationID, browserCommandHead{Type: "approval_decision", RequestID: "request-1"}); reject {
		t.Fatalf("expected approval_decision to be accepted for pending request, got %q", reason)
	}
	if reason, reject := server.checkCommandState(conversationID, browserCommandHead{Type: "approval_decision", RequestID: "request-2"}); !reject || reason != RejectNotAllowed {
		t.Fatalf("expected approval_decision to be rejected for unknown request, got reject=%v reason=%q", reject, reason)
	}
}

func TestBrowserWebSocketAdmitsCommandsAfterGatewayRestart(t *testing.T) {
	tmp := t.TempDir()
	storeDir := filepath.Join(tmp, "commands")
	runtimeDir := filepath.Join(tmp, "runtime")

	store, gateway, err := openGatewayAt(t, storeDir, runtimeDir)
	if err != nil {
		t.Fatalf("open first gateway: %v", err)
	}

	const conversationID = "conversation-1"
	claims := TokenClaims{TenantID: "tenant-1", AgentID: "agent-1", ConversationID: conversationID, Generation: 1}

	seq := uint64(1)
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:            &seq,
		ConversationID: conversationID,
		Event:          json.RawMessage(`{"type":"agent_start"}`),
	}); err != nil {
		t.Fatalf("receive agent_start: %v", err)
	}

	seq = 2
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:            &seq,
		ConversationID: conversationID,
		Event:          json.RawMessage(`{"type":"approval_requested","request":{"id":"request-1","tool_call_id":"call-1","tool_name":"read_file","action":{"reviewable":"read"},"args_summary":"read"}}`),
	}); err != nil {
		t.Fatalf("receive approval_requested: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close command store: %v", err)
	}

	store, gateway, err = openGatewayAt(t, storeDir, runtimeDir)
	if err != nil {
		t.Fatalf("reopen gateway: %v", err)
	}
	defer store.Close()

	sessions, err := NewHMACUserSessionVerifier(testSecret, "")
	if err != nil {
		t.Fatal(err)
	}
	server := NewBrowserServer(sessions, gateway.commands, gateway)
	server.AllowedOrigins = []string{"https://web.example"}
	mux := http.NewServeMux()
	mux.Handle("GET /conversations/{conversation_id}/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	cookie := signBrowserSession(t, testSecret, userSessionWireClaims{TenantID: "tenant-1", UserID: "user-1", ConversationID: conversationID, Exp: time.Now().Add(time.Hour).Unix(), Aud: defaultBrowserAudience})
	conn := dialBrowserWS(t, httpServer, cookie, conversationID)
	defer conn.Close()
	if err := conn.WriteJSON(browserHello{Type: "hello", LastEventSeq: 2}); err != nil {
		t.Fatal(err)
	}

	commands := []json.RawMessage{
		json.RawMessage(`{"type":"abort"}`),
		json.RawMessage(`{"type":"approval_decision","request_id":"request-1","decision":{"type":"approve_once"}}`),
		json.RawMessage(`{"type":"approval_decision","request_id":"request-unknown","decision":{"type":"approve_once"}}`),
	}
	for index, command := range commands {
		if err := conn.WriteJSON(browserCommandFrame{Type: "command", IdempotencyKey: fmt.Sprintf("idempotency-%d", index), Command: command}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 2; i++ {
		var accepted browserCommandAcceptedFrame
		conn.SetReadDeadline(time.Now().Add(time.Second))
		if err := conn.ReadJSON(&accepted); err != nil {
			t.Fatalf("read command admission for accepted command %d: %v", i, err)
		}
		if accepted.Type != "command_accepted" || accepted.Envelope.Seq == 0 || accepted.Envelope.CommandID == "" || len(accepted.Envelope.Command) == 0 {
			t.Fatalf("expected command_accepted with allocated seq and command_id, got %+v", accepted)
		}
	}

	var rejected browserCommandRejectedFrame
	conn.SetReadDeadline(time.Now().Add(time.Second))
	if err := conn.ReadJSON(&rejected); err != nil {
		t.Fatalf("read rejected command: %v", err)
	}
	if rejected.Type != "command_rejected" || rejected.RejectReason != RejectNotAllowed {
		t.Fatalf("expected command_rejected with not_allowed, got %+v", rejected)
	}
}

func TestBrowserWebSocketFailsClosedOnCorruptDurableState(t *testing.T) {
	tmp := t.TempDir()
	storeDir := filepath.Join(tmp, "commands")
	runtimeDir := filepath.Join(tmp, "runtime")

	store, gateway, err := openGatewayAt(t, storeDir, runtimeDir)
	if err != nil {
		t.Fatalf("open gateway: %v", err)
	}

	const conversationID = "conversation-1"
	if err := os.WriteFile(
		gateway.eventPath(conversationID),
		[]byte(`{"seq":2,"event":{"seq":2,"conversation_id":"conversation-1","event":{"type":"agent_start"}}}`+"\n"),
		0o600,
	); err != nil {
		t.Fatalf("write corrupt event log: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close command store: %v", err)
	}

	store, gateway, err = openGatewayAt(t, storeDir, runtimeDir)
	if err != nil {
		t.Fatalf("reopen gateway: %v", err)
	}
	defer store.Close()

	sessions, err := NewHMACUserSessionVerifier(testSecret, "")
	if err != nil {
		t.Fatal(err)
	}
	server := NewBrowserServer(sessions, gateway.commands, gateway)
	server.AllowedOrigins = []string{"https://web.example"}
	mux := http.NewServeMux()
	mux.Handle("GET /conversations/{conversation_id}/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	wsURL := strings.Replace(httpServer.URL, "http", "ws", 1) + "/conversations/" + conversationID + "/ws"
	header := http.Header{"Origin": {"https://web.example"}, "Cookie": {BrowserSessionCookie + "=" + signBrowserSession(t, testSecret, userSessionWireClaims{TenantID: "tenant-1", UserID: "user-1", ConversationID: conversationID, Exp: time.Now().Add(time.Hour).Unix(), Aud: defaultBrowserAudience})}}
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("dial browser websocket: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(browserHello{Type: "hello", LastEventSeq: 0}); err != nil {
		t.Fatal(err)
	}

	// After the server attempts to rebuild from the corrupt durable log it must
	// fail closed and close the connection instead of defaulting to an empty
	// "no turn / no approval" state that would admit the next command.
	// The close may win the race with this write. Either outcome is acceptable,
	// but the following read must observe a prompt close rather than a timeout.
	_ = conn.WriteJSON(browserCommandFrame{Type: "command", IdempotencyKey: "ignored", Command: json.RawMessage(`{"type":"abort"}`)})
	conn.SetReadDeadline(time.Now().Add(time.Second))
	var ignored browserCommandAcceptedFrame
	err = conn.ReadJSON(&ignored)
	if err == nil {
		t.Fatal("expected connection to close after corrupt state, got a command acceptance")
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatalf("server hung instead of closing after corrupt state: %v", err)
	}
	var closeErr *websocket.CloseError
	if !errors.As(err, &closeErr) && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("expected websocket close/EOF after corrupt state, got %T: %v", err, err)
	}
}
