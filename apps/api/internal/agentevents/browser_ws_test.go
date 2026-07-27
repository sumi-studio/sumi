package agentevents

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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
	if err := gateway.Receive(context.Background(), agentClaims, Envelope{Seq: &seq, ConversationID: "conversation-1", Event: json.RawMessage(`{"type":"tool_execution_start","tool_call_id":"call-1","tool_name":"read_file","args":{}}`)}); err != nil {
		t.Fatalf("persist durable tool event: %v", err)
	}
	if replay, err := gateway.EventCatchUp(context.Background(), "conversation-1", 0); err != nil || len(replay) != 1 {
		t.Fatalf("read durable event for browser replay: events=%d err=%v", len(replay), err)
	}
	assertBrowserEvent(t, conn, "tool_execution_start", true)
	volatile := Envelope{ConversationID: "conversation-1", Event: json.RawMessage(`{"type":"message_update","message_id":"00000000-0000-4000-8000-000000000001","event":{"type":"text_delta","content_index":0,"delta":"stream"}}`)}
	if err := gateway.Receive(context.Background(), agentClaims, volatile); err != nil {
		t.Fatalf("publish volatile stream event: %v", err)
	}
	assertBrowserEvent(t, conn, "message_update", false)
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
		if accepted.Type != "command_accepted" || len(accepted.Envelope.Command) == 0 {
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
	if err := first.WriteJSON(browserHello{Type: "hello", LastEventSeq: 0}); err != nil {
		t.Fatal(err)
	}
	assertBrowserEvent(t, first, "agent_start", true)
	_ = first.Close()
	seq = 2
	if err := gateway.Receive(context.Background(), claims, Envelope{Seq: &seq, ConversationID: "conversation-1", Event: json.RawMessage(`{"type":"agent_end"}`)}); err != nil {
		t.Fatal(err)
	}
	second := dialBrowserWS(t, httpServer, cookie, "conversation-1")
	defer second.Close()
	if err := second.WriteJSON(browserHello{Type: "hello", LastEventSeq: 1}); err != nil {
		t.Fatal(err)
	}
	assertBrowserEvent(t, second, "agent_end", true)
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
