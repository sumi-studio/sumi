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
	server := NewBrowserServer(sessions, gateway, gateway)
	server.AllowedOrigins = []string{"https://web.example"}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	claims := userSessionWireClaims{TenantID: "tenant-1", UserID: "user-1", PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Exp: time.Now().Add(time.Hour).Unix(), Aud: defaultBrowserAudience}
	conn := dialBrowserWS(t, httpServer, signBrowserSession(t, testSecret, claims), "018f47a2-9b3c-7def-8abc-0123456789ab")
	defer conn.Close()
	if err := conn.WriteJSON(browserHello{Type: "hello", LastEventSeq: 0}); err != nil {
		t.Fatal(err)
	}
	assertDirectChatStatus(t, conn, "unavailable")
	receipt := "hydrated-1"
	if err := gateway.PublishRuntimeState(claims.PersonalityAgentID, 7, &receipt); err != nil {
		t.Fatalf("publish authoritative ready state: %v", err)
	}
	assertDirectChatStatus(t, conn, "ready")

	seq := uint64(1)
	agentClaims := TokenClaims{TenantID: "tenant-1", PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Generation: 7}
	// Drive the abort guard from the durable run lifecycle, not internal map
	// mutation.
	if err := gateway.Receive(context.Background(), agentClaims, Envelope{Seq: &seq, PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Event: json.RawMessage(`{"type":"agent_start"}`)}); err != nil {
		t.Fatalf("persist durable agent_start: %v", err)
	}
	if replay, err := gateway.EventCatchUp(context.Background(), "018f47a2-9b3c-7def-8abc-0123456789ab", 0); err != nil || len(replay) != 1 {
		t.Fatalf("read durable event for browser replay: events=%d err=%v", len(replay), err)
	}
	assertBrowserEvent(t, conn, "agent_start", true)

	seq = 2
	if err := gateway.Receive(context.Background(), agentClaims, Envelope{Seq: &seq, PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Event: json.RawMessage(`{"type":"tool_execution_start","tool_call_id":"call-1","tool_name":"read_file","args":{}}`)}); err != nil {
		t.Fatalf("persist durable tool event: %v", err)
	}
	assertBrowserEvent(t, conn, "tool_execution_start", true)
	volatile := Envelope{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Event: json.RawMessage(`{"type":"message_update","message_id":"00000000-0000-4000-8000-000000000001","event":{"type":"text_delta","content_index":0,"delta":"stream"}}`)}
	if err := gateway.Receive(context.Background(), agentClaims, volatile); err != nil {
		t.Fatalf("publish volatile stream event: %v", err)
	}
	assertBrowserEvent(t, conn, "message_update", false)

	seq = 3
	if err := gateway.Receive(context.Background(), agentClaims, Envelope{Seq: &seq, PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Event: json.RawMessage(`{"type":"approval_requested","request":{"id":"request-1","tool_call_id":"call-1","tool_name":"read_file","action":{"reviewable":"read"},"args_summary":"read"}}`)}); err != nil {
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
		if accepted.Type != "command_accepted" ||
			accepted.IdempotencyKey != fmt.Sprintf("idempotency-%d", index) ||
			accepted.Seq == 0 ||
			accepted.CommandID == "" {
			t.Fatalf("unexpected command admission: %+v", accepted)
		}
	}

	// A changed authenticated record under an existing key is a correlated
	// terminal rejection; it must not close the socket into a retry loop.
	if err := conn.WriteJSON(browserCommandFrame{
		Type:           "command",
		IdempotencyKey: "idempotency-0",
		Command:        json.RawMessage(`{"type":"user_message","text":"changed","attachments":[]}`),
	}); err != nil {
		t.Fatal(err)
	}
	var conflict browserCommandRejectedFrame
	if err := conn.ReadJSON(&conflict); err != nil {
		t.Fatal(err)
	}
	if conflict.Type != "command_rejected" ||
		conflict.IdempotencyKey != "idempotency-0" ||
		conflict.RejectReason != RejectIdempotencyConflict {
		t.Fatalf("unexpected idempotency conflict frame: %+v", conflict)
	}
	if err := conn.WriteJSON(browserCommandFrame{
		Type:           "command",
		IdempotencyKey: "after-conflict",
		Command:        json.RawMessage(`{"type":"user_message","text":"still open","attachments":[]}`),
	}); err != nil {
		t.Fatal(err)
	}
	var accepted browserCommandAcceptedFrame
	if err := conn.ReadJSON(&accepted); err != nil {
		t.Fatal(err)
	}
	if accepted.Type != "command_accepted" || accepted.IdempotencyKey != "after-conflict" {
		t.Fatalf("socket did not continue after conflict: %+v", accepted)
	}
}

func TestBrowserWebSocketRejectsUnavailableWithoutDurableCommand(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	if err := gateway.PublishRuntimeState(personalityAgentID, 7, nil); err != nil {
		t.Fatal(err)
	}
	sessions, err := NewHMACUserSessionVerifier(testSecret, "")
	if err != nil {
		t.Fatal(err)
	}
	server := NewBrowserServer(sessions, gateway, gateway)
	server.AllowedOrigins = []string{"https://web.example"}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	claims := userSessionWireClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: personalityAgentID,
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                defaultBrowserAudience,
	}
	conn := dialBrowserWS(t, httpServer, signBrowserSession(t, testSecret, claims), personalityAgentID)
	defer conn.Close()
	if err := conn.WriteJSON(browserHello{Type: "hello", LastEventSeq: 0}); err != nil {
		t.Fatal(err)
	}
	assertDirectChatStatus(t, conn, "unavailable")

	command := browserCommandFrame{
		Type:           "command",
		IdempotencyKey: "unavailable-command",
		Command:        json.RawMessage(`{"type":"user_message","text":"not yet","attachments":[]}`),
	}
	if err := conn.WriteJSON(command); err != nil {
		t.Fatal(err)
	}
	var rejected browserCommandRejectedFrame
	if err := conn.ReadJSON(&rejected); err != nil {
		t.Fatal(err)
	}
	if rejected.Type != "command_rejected" ||
		rejected.IdempotencyKey != command.IdempotencyKey ||
		rejected.RejectReason != RejectUnavailable {
		t.Fatalf("unexpected unavailable rejection: %+v", rejected)
	}
	if hasCommands, err := gateway.commands.HasCommands(context.Background(), personalityAgentID); err != nil || hasCommands {
		t.Fatalf("NotReady browser command reached durable log: hasCommands=%v err=%v", hasCommands, err)
	}

	receipt := "browser-ready"
	if err := gateway.PublishRuntimeState(personalityAgentID, 7, &receipt); err != nil {
		t.Fatal(err)
	}
	assertDirectChatStatus(t, conn, "ready")
	if err := conn.WriteJSON(command); err != nil {
		t.Fatal(err)
	}
	var accepted browserCommandAcceptedFrame
	if err := conn.ReadJSON(&accepted); err != nil {
		t.Fatal(err)
	}
	if accepted.Type != "command_accepted" || accepted.IdempotencyKey != command.IdempotencyKey {
		t.Fatalf("Ready did not admit previously rejected command: %+v", accepted)
	}
}

func TestBrowserWebSocketRejectsMissingExpiredAndMalformedPersonalityAgentSessions(t *testing.T) {
	gateway := openRuntimeGateway(t)
	sessions, err := NewHMACUserSessionVerifier(testSecret, "")
	if err != nil {
		t.Fatal(err)
	}
	server := NewBrowserServer(sessions, gateway, gateway)
	server.AllowedOrigins = []string{"https://web.example"}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	wsURL := strings.Replace(httpServer.URL, "http", "ws", 1) + "/direct-chat/ws"
	for _, test := range []struct {
		name   string
		cookie string
	}{
		{"missing", ""},
		{"expired", signBrowserSession(t, testSecret, userSessionWireClaims{TenantID: "tenant", UserID: "user", PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Exp: time.Now().Add(-time.Hour).Unix(), Aud: defaultBrowserAudience})},
		{"malformed-personality-agent", signBrowserSession(t, testSecret, userSessionWireClaims{TenantID: "tenant", UserID: "user", PersonalityAgentID: "other", Exp: time.Now().Add(time.Hour).Unix(), Aud: defaultBrowserAudience})},
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
	server := NewBrowserServer(sessions, gateway, gateway)
	server.AllowedOrigins = []string{"https://web.example"}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()
	cookie := signBrowserSession(t, testSecret, userSessionWireClaims{TenantID: "tenant", UserID: "user", PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Exp: time.Now().Add(time.Hour).Unix(), Aud: defaultBrowserAudience})
	claims := TokenClaims{TenantID: "tenant", PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Generation: 1}
	if err := gateway.PublishRuntimeState(claims.PersonalityAgentID, claims.Generation, nil); err != nil {
		t.Fatal(err)
	}
	seq := uint64(1)
	if err := gateway.Receive(context.Background(), claims, Envelope{Seq: &seq, PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Event: json.RawMessage(`{"type":"agent_start"}`)}); err != nil {
		t.Fatal(err)
	}
	first := dialBrowserWS(t, httpServer, cookie, "018f47a2-9b3c-7def-8abc-0123456789ab")
	waitForBrowserConnectionStats(t, server, BrowserConnectionStats{Active: 1, Accepted: 1})
	if err := first.WriteJSON(browserHello{Type: "hello", LastEventSeq: 0}); err != nil {
		t.Fatal(err)
	}
	assertDirectChatStatus(t, first, "unavailable")
	assertBrowserEvent(t, first, "agent_start", true)
	_ = first.Close()
	waitForBrowserConnectionStats(t, server, BrowserConnectionStats{Active: 0, Accepted: 1})
	seq = 2
	if err := gateway.Receive(context.Background(), claims, Envelope{Seq: &seq, PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Event: json.RawMessage(`{"type":"agent_end"}`)}); err != nil {
		t.Fatal(err)
	}
	second := dialBrowserWS(t, httpServer, cookie, "018f47a2-9b3c-7def-8abc-0123456789ab")
	defer second.Close()
	waitForBrowserConnectionStats(t, server, BrowserConnectionStats{Active: 1, Accepted: 2})
	if err := second.WriteJSON(browserHello{Type: "hello", LastEventSeq: 1}); err != nil {
		t.Fatal(err)
	}
	assertDirectChatStatus(t, second, "unavailable")
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

func dialBrowserWS(t *testing.T, server *httptest.Server, cookie, personalityAgentID string) *websocket.Conn {
	t.Helper()
	wsURL := strings.Replace(server.URL, "http", "ws", 1) + "/direct-chat/ws"
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

func assertDirectChatStatus(t *testing.T, conn *websocket.Conn, want string) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(time.Second))
	var frame directChatStatusFrame
	if err := conn.ReadJSON(&frame); err != nil {
		t.Fatalf("read direct-chat status: %v", err)
	}
	if frame.Type != "direct_chat_status" || frame.Status != want {
		t.Fatalf("unexpected direct-chat status: %+v", frame)
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
	server := NewBrowserServer(sessions, gateway, gateway)

	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	claims := TokenClaims{TenantID: "tenant", PersonalityAgentID: personalityAgentID, Generation: 1}
	if err := gateway.PublishRuntimeState(personalityAgentID, claims.Generation, nil); err != nil {
		t.Fatal(err)
	}

	if reason, reject := server.checkCommandState(personalityAgentID, browserCommandHead{Type: "abort"}); !reject {
		t.Fatal("expected abort to be rejected when no run is in flight")
	} else if reason != RejectNotAllowed {
		t.Fatalf("expected not_allowed, got %q", reason)
	}

	if reason, reject := server.checkCommandState(personalityAgentID, browserCommandHead{Type: "approval_decision", RequestID: "request-1"}); !reject {
		t.Fatal("expected approval_decision to be rejected when no approval is pending")
	} else if reason != RejectNotAllowed {
		t.Fatalf("expected not_allowed, got %q", reason)
	}

	seq := uint64(1)
	if err := gateway.Receive(context.Background(), claims, Envelope{Seq: &seq, PersonalityAgentID: personalityAgentID, Event: json.RawMessage(`{"type":"agent_start"}`)}); err != nil {
		t.Fatalf("receive agent_start: %v", err)
	}
	if reason, reject := server.checkCommandState(personalityAgentID, browserCommandHead{Type: "abort"}); reject {
		t.Fatalf("expected abort to be accepted during in-flight run, got %q", reason)
	}

	seq = 2
	if err := gateway.Receive(context.Background(), claims, Envelope{Seq: &seq, PersonalityAgentID: personalityAgentID, Event: json.RawMessage(`{"type":"approval_requested","request":{"id":"request-1","tool_call_id":"call-1","tool_name":"read_file","action":{"reviewable":"read"},"args_summary":"read"}}`)}); err != nil {
		t.Fatalf("receive approval_requested: %v", err)
	}
	if reason, reject := server.checkCommandState(personalityAgentID, browserCommandHead{Type: "approval_decision", RequestID: "request-1"}); reject {
		t.Fatalf("expected approval_decision to be accepted for pending request, got %q", reason)
	}
	if reason, reject := server.checkCommandState(personalityAgentID, browserCommandHead{Type: "approval_decision", RequestID: "request-2"}); !reject || reason != RejectNotAllowed {
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

	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	claims := TokenClaims{TenantID: "tenant-1", PersonalityAgentID: personalityAgentID, Generation: 1}
	if err := gateway.PublishRuntimeState(personalityAgentID, claims.Generation, nil); err != nil {
		t.Fatal(err)
	}

	seq := uint64(1)
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:                &seq,
		PersonalityAgentID: personalityAgentID,
		Event:              json.RawMessage(`{"type":"agent_start"}`),
	}); err != nil {
		t.Fatalf("receive agent_start: %v", err)
	}

	seq = 2
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:                &seq,
		PersonalityAgentID: personalityAgentID,
		Event:              json.RawMessage(`{"type":"approval_requested","request":{"id":"request-1","tool_call_id":"call-1","tool_name":"read_file","action":{"reviewable":"read"},"args_summary":"read"}}`),
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
	server := NewBrowserServer(sessions, gateway, gateway)
	server.AllowedOrigins = []string{"https://web.example"}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	cookie := signBrowserSession(t, testSecret, userSessionWireClaims{TenantID: "tenant-1", UserID: "user-1", PersonalityAgentID: personalityAgentID, Exp: time.Now().Add(time.Hour).Unix(), Aud: defaultBrowserAudience})
	conn := dialBrowserWS(t, httpServer, cookie, personalityAgentID)
	defer conn.Close()
	if err := conn.WriteJSON(browserHello{Type: "hello", LastEventSeq: 2}); err != nil {
		t.Fatal(err)
	}
	assertDirectChatStatus(t, conn, "unavailable")
	receipt := "restart-ready"
	if err := gateway.PublishRuntimeState(personalityAgentID, claims.Generation, &receipt); err != nil {
		t.Fatal(err)
	}
	assertDirectChatStatus(t, conn, "ready")

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
		if accepted.Type != "command_accepted" || accepted.Seq == 0 || accepted.CommandID == "" {
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

	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	if err := os.WriteFile(
		gateway.eventPath(personalityAgentID),
		[]byte(`{"seq":2,"event":{"seq":2,"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"agent_start"}}}`+"\n"),
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
	server := NewBrowserServer(sessions, gateway, gateway)
	server.AllowedOrigins = []string{"https://web.example"}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	wsURL := strings.Replace(httpServer.URL, "http", "ws", 1) + "/direct-chat/ws"
	header := http.Header{"Origin": {"https://web.example"}, "Cookie": {BrowserSessionCookie + "=" + signBrowserSession(t, testSecret, userSessionWireClaims{TenantID: "tenant-1", UserID: "user-1", PersonalityAgentID: personalityAgentID, Exp: time.Now().Add(time.Hour).Unix(), Aud: defaultBrowserAudience})}}
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

func TestDecodeBrowserCommandRequiresContractValidIdempotencyKey(t *testing.T) {
	command := `{"type":"user_message","text":"hi","attachments":[]}`
	for name, key := range map[string]string{
		"empty":     "",
		"oversized": strings.Repeat("k", MaxIdempotencyKeyBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(browserCommandFrame{
				Type:           "command",
				IdempotencyKey: key,
				Command:        json.RawMessage(command),
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeBrowserCommand(raw); err == nil {
				t.Fatalf("accepted invalid idempotency key length %d", len(key))
			}
		})
	}
}

func TestBrowserOutboundFramesRejectMalformedContractShapes(t *testing.T) {
	tests := []struct {
		name   string
		raw    string
		target func() any
	}{
		{
			name:   "event missing envelope",
			raw:    `{"type":"event"}`,
			target: func() any { return &browserEventFrame{} },
		},
		{
			name:   "browser event leaks internal target",
			raw:    `{"type":"event","envelope":{"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"error","message":"x"}}}`,
			target: func() any { return &browserEventFrame{} },
		},
		{
			name:   "browser event has null seq",
			raw:    `{"type":"event","envelope":{"seq":null,"event":{"type":"error","message":"x"}}}`,
			target: func() any { return &browserEventFrame{} },
		},
		{
			name:   "accepted missing correlation key",
			raw:    `{"type":"command_accepted","command_id":"00000000-0000-4000-8000-000000000001","seq":1}`,
			target: func() any { return &browserCommandAcceptedFrame{} },
		},
		{
			name:   "accepted unknown field",
			raw:    `{"type":"command_accepted","idempotency_key":"key","command_id":"00000000-0000-4000-8000-000000000001","seq":1,"extra":true}`,
			target: func() any { return &browserCommandAcceptedFrame{} },
		},
		{
			name:   "rejected missing correlation key",
			raw:    `{"type":"command_rejected","reject_reason":"schema_violation"}`,
			target: func() any { return &browserCommandRejectedFrame{} },
		},
		{
			name:   "rejected unknown reason",
			raw:    `{"type":"command_rejected","idempotency_key":"key","reject_reason":"other"}`,
			target: func() any { return &browserCommandRejectedFrame{} },
		},
		{
			name:   "status unknown value",
			raw:    `{"type":"direct_chat_status","status":"connecting"}`,
			target: func() any { return &directChatStatusFrame{} },
		},
		{
			name:   "status unknown field",
			raw:    `{"type":"direct_chat_status","status":"ready","extra":true}`,
			target: func() any { return &directChatStatusFrame{} },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := json.Unmarshal([]byte(test.raw), test.target()); err == nil {
				t.Fatalf("accepted malformed browser frame: %s", test.raw)
			}
		})
	}

	var volatile browserEventFrame
	if err := json.Unmarshal(
		[]byte(`{"type":"event","envelope":{"event":{"type":"error","message":"x"}}}`),
		&volatile,
	); err != nil {
		t.Fatalf("valid target-free volatile browser event rejected: %v", err)
	}
	if volatile.Envelope.Seq != nil {
		t.Fatalf("volatile browser event gained seq: %+v", volatile)
	}
	var unavailable directChatStatusFrame
	if err := json.Unmarshal(
		[]byte(`{"type":"direct_chat_status","status":"unavailable"}`),
		&unavailable,
	); err != nil {
		t.Fatalf("valid unavailable status rejected: %v", err)
	}
}
