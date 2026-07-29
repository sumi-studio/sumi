package agentevents

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const (
	artifactOwner = "018f47a2-9b3c-7def-8abc-0123456789ab"
	artifactOther = "018f47a2-9b3c-7def-9abc-0123456789ac"
)

func TestProjectBrowserEventRewritesNestedOwnedArtifactHandles(t *testing.T) {
	seq := uint64(7)
	envelope := Envelope{
		Seq:                &seq,
		PersonalityAgentID: artifactOwner,
		Event: json.RawMessage(`{
			"type":"tool_execution_end",
			"tool_call_id":"call-1",
			"result":{
				"primary":"artifact://018f47a2-9b3c-7def-8abc-0123456789ab/tool-output/run-1",
				"note":"first artifact://018f47a2-9b3c-7def-8abc-0123456789ab/attachments/input-1, second artifact://018f47a2-9b3c-7def-8abc-0123456789ab/tool-output/run-2",
				"nested":[{"handle":"artifact://018f47a2-9b3c-7def-8abc-0123456789ab/attachments/input-2"}],
				"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab",
				"tenant_id":"tenant-1",
				"provenance":{"source":"literal-user-shaped-data"},
				"ordinary_uuid":"018f47a2-9b3c-7def-8abc-0123456789ab",
				"malformed":"artifact://018f47a2-9b3c-7def-8abc-0123456789ab/tool-output/run-3/extra",
				"large":9007199254740991,
				"decimal":1.2300e+4
			},
			"is_error":false
		}`),
	}

	projected, err := projectBrowserEvent(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if projected.Seq == nil || *projected.Seq != seq {
		t.Fatalf("projection lost seq: %+v", projected)
	}
	text := string(projected.Event)
	for _, want := range []string{
		`artifact://tool-output/run-1`,
		`artifact://attachments/input-1`,
		`artifact://tool-output/run-2`,
		`artifact://attachments/input-2`,
		`"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab"`,
		`"tenant_id":"tenant-1"`,
		`"provenance":{"source":"literal-user-shaped-data"}`,
		`"ordinary_uuid":"018f47a2-9b3c-7def-8abc-0123456789ab"`,
		`"malformed":"artifact://018f47a2-9b3c-7def-8abc-0123456789ab/tool-output/run-3/extra"`,
		`"large":9007199254740991`,
		`"decimal":1.2300e+4`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("projected event lost %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "artifact://"+artifactOwner+"/attachments/input-1") ||
		strings.Contains(text, "artifact://"+artifactOwner+"/tool-output/run-1") {
		t.Fatalf("owned internal handle leaked through projection: %s", text)
	}

	frameRaw, err := json.Marshal(browserEventFrame{Type: "event", Envelope: projected})
	if err != nil {
		t.Fatal(err)
	}
	var frame map[string]any
	if err := json.Unmarshal(frameRaw, &frame); err != nil {
		t.Fatal(err)
	}
	browserEnvelope, ok := frame["envelope"].(map[string]any)
	if !ok {
		t.Fatalf("browser envelope missing: %s", frameRaw)
	}
	if _, exists := browserEnvelope["personality_agent_id"]; exists {
		t.Fatalf("structural internal target leaked into browser envelope: %s", frameRaw)
	}
	if _, exists := browserEnvelope["provenance"]; exists {
		t.Fatalf("structural provenance leaked into browser envelope: %s", frameRaw)
	}
}

func TestProjectBrowserEventRewritesToolResultMessageButPreservesUserMessage(t *testing.T) {
	toolResult := Envelope{
		PersonalityAgentID: artifactOwner,
		Event: json.RawMessage(`{
			"type":"message_end",
			"message_id":"00000000-0000-4000-8000-000000000001",
			"message":{
				"role":"tool_result",
				"tool_call_id":"call-1",
				"tool_name":"read_file",
				"content":[{"type":"text","text":"full: artifact://018f47a2-9b3c-7def-8abc-0123456789ab/tool-output/run-1"}],
				"details":{"attachment":"artifact://018f47a2-9b3c-7def-8abc-0123456789ab/attachments/input-1"},
				"is_error":false,
				"timestamp":"2026-07-30T00:00:00Z"
			}
		}`),
	}
	seq := uint64(1)
	toolResult.Seq = &seq
	projected, err := projectBrowserEvent(toolResult)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(projected.Event), "artifact://"+artifactOwner+"/") {
		t.Fatalf("tool-result message leaked owner: %s", projected.Event)
	}
	if !strings.Contains(string(projected.Event), "artifact://tool-output/run-1") ||
		!strings.Contains(string(projected.Event), "artifact://attachments/input-1") {
		t.Fatalf("tool-result handles were not projected: %s", projected.Event)
	}

	literal := "literal artifact://" + artifactOwner + "/tool-output/pasted and artifact://tool-output/browser-pasted"
	user := Envelope{
		Seq:                &seq,
		PersonalityAgentID: artifactOwner,
		Event: json.RawMessage(`{
			"type":"message_end",
			"message_id":"00000000-0000-4000-8000-000000000002",
			"message":{
				"role":"user",
				"content":[{"type":"text","text":"` + literal + `"}],
				"timestamp":"2026-07-30T00:00:00Z"
			}
		}`),
	}
	if err := validateEnvelope(user); err != nil {
		t.Fatalf("faithful user-authored handle-like text rejected: %v", err)
	}
	userProjected, err := projectBrowserEvent(user)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(userProjected.Event), literal) {
		t.Fatalf("user-authored message text was rewritten: %s", userProjected.Event)
	}
}

func TestArtifactProjectionRejectsCrossOwnerAndInternalBrowserReferences(t *testing.T) {
	crossOwner := Envelope{
		PersonalityAgentID: artifactOwner,
		Event: json.RawMessage(`{
			"type":"tool_execution_end",
			"tool_call_id":"call-1",
			"result":{"handle":"artifact://018f47a2-9b3c-7def-9abc-0123456789ac/tool-output/run-1"},
			"is_error":false
		}`),
	}
	seq := uint64(1)
	crossOwner.Seq = &seq
	if _, err := projectBrowserEvent(crossOwner); err == nil {
		t.Fatal("cross-owner canonical artifact handle reached browser projection")
	}
	if err := validateEnvelope(crossOwner); err == nil {
		t.Fatal("cross-owner canonical artifact handle passed internal validation")
	}

	browserRef := Envelope{
		Seq:                &seq,
		PersonalityAgentID: artifactOwner,
		Event: json.RawMessage(`{
			"type":"tool_execution_end",
			"tool_call_id":"call-1",
			"result":{"handle":"artifact://tool-output/run-1"},
			"is_error":false
		}`),
	}
	if err := validateEnvelope(browserRef); err == nil {
		t.Fatal("targetless browser artifact reference flowed back into internal RPC")
	}
}

func TestBrowserWebSocketProjectsArtifactHandlesOnDurableAndVolatilePaths(t *testing.T) {
	gateway := openRuntimeGateway(t)
	sessions, err := NewHMACUserSessionVerifier(testSecret, "")
	if err != nil {
		t.Fatal(err)
	}
	server := NewBrowserServer(sessions, gateway.commands, gateway)
	server.AllowedOrigins = []string{"https://web.example"}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	cookie := signBrowserSession(t, testSecret, userSessionWireClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: artifactOwner,
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                defaultBrowserAudience,
	})
	conn := dialBrowserWS(t, httpServer, cookie, artifactOwner)
	defer conn.Close()
	if err := conn.WriteJSON(browserHello{Type: "hello", LastEventSeq: 0}); err != nil {
		t.Fatal(err)
	}
	assertDirectChatStatus(t, conn, "unavailable")

	claims := TokenClaims{TenantID: "tenant-1", PersonalityAgentID: artifactOwner, Generation: 1}
	seq := uint64(1)
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:                &seq,
		PersonalityAgentID: artifactOwner,
		Event: json.RawMessage(`{
			"type":"tool_execution_end",
			"tool_call_id":"call-1",
			"result":{"handle":"artifact://018f47a2-9b3c-7def-8abc-0123456789ab/tool-output/run-1"},
			"is_error":false
		}`),
	}); err != nil {
		t.Fatal(err)
	}
	durable := readBrowserEventFrame(t, conn)
	if durable.Envelope.Seq == nil ||
		!bytes.Contains(durable.Envelope.Event, []byte(`"handle":"artifact://tool-output/run-1"`)) ||
		bytes.Contains(durable.Envelope.Event, []byte("artifact://"+artifactOwner+"/")) {
		t.Fatalf("durable artifact projection mismatch: %+v", durable)
	}

	if err := gateway.Receive(context.Background(), claims, Envelope{
		PersonalityAgentID: artifactOwner,
		Event: json.RawMessage(`{
			"type":"tool_execution_update",
			"tool_call_id":"call-1",
			"partial":{"handles":["artifact://018f47a2-9b3c-7def-8abc-0123456789ab/attachments/input-1","see artifact://018f47a2-9b3c-7def-8abc-0123456789ab/tool-output/run-2"]}
		}`),
	}); err != nil {
		t.Fatal(err)
	}
	volatile := readBrowserEventFrame(t, conn)
	if volatile.Envelope.Seq != nil ||
		!bytes.Contains(volatile.Envelope.Event, []byte(`artifact://attachments/input-1`)) ||
		!bytes.Contains(volatile.Envelope.Event, []byte(`artifact://tool-output/run-2`)) ||
		bytes.Contains(volatile.Envelope.Event, []byte("artifact://"+artifactOwner+"/")) {
		t.Fatalf("volatile artifact projection mismatch: %+v", volatile)
	}
}

func readBrowserEventFrame(t *testing.T, conn interface {
	SetReadDeadline(time.Time) error
	ReadJSON(any) error
}) browserEventFrame {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var frame browserEventFrame
	if err := conn.ReadJSON(&frame); err != nil {
		t.Fatal(err)
	}
	if frame.Type != "event" {
		t.Fatalf("unexpected browser frame: %+v", frame)
	}
	return frame
}
