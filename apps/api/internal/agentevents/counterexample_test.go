package agentevents

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestValidateCommandCounterexamples(t *testing.T) {
	cases := []struct {
		name      string
		raw       string
		shouldErr bool
	}{
		{"approval_decision_missing_decision", `{"type":"approval_decision","request_id":"req-1"}`, true},
		{"approval_decision_non_object_rule", `{"type":"approval_decision","request_id":"req-1","decision":{"type":"approve_always","rule":"notanobject"}}`, true},
		{"approval_decision_unknown_field_in_decision", `{"type":"approval_decision","request_id":"req-1","decision":{"type":"approve_once","extra":1}}`, true},
		{"abort_unknown_field", `{"type":"abort","extra":true}`, true},
		{"user_message_null_attachments", `{"type":"user_message","text":"hi","attachments":null}`, true},
		{"user_message_unknown_field", `{"type":"user_message","text":"hi","attachments":[],"extra":1}`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCommand(json.RawMessage(tc.raw))
			if tc.shouldErr && err == nil {
				t.Fatalf("expected ValidateCommand to reject %q, but it accepted", tc.raw)
			}
			if !tc.shouldErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestOutboundFrameValidateCounterexamples(t *testing.T) {
	cases := []struct {
		name  string
		frame OutboundFrame
	}{
		{"ack_missing_seq", OutboundFrame{FrameType: "command_ack", Ack: &CommandAck{}}},
		{"ack_invalid_command_id", OutboundFrame{FrameType: "command_ack", Ack: &CommandAck{Seq: 1, CommandID: "not-a-uuid", Status: "received"}}},
		{"ack_non_rejected_with_reason", OutboundFrame{FrameType: "command_ack", Ack: &CommandAck{Seq: 1, CommandID: "00000000-0000-4000-8000-000000000001", Status: "received", RejectReason: strPtr("oversized")}}},
		{"ack_unknown_status", OutboundFrame{FrameType: "command_ack", Ack: &CommandAck{Seq: 1, CommandID: "00000000-0000-4000-8000-000000000001", Status: "bogus"}}},
		{"event_missing_fields", OutboundFrame{FrameType: "event", Envelope: &Envelope{}}},
		{"volatile_event_with_seq", OutboundFrame{FrameType: "event", Envelope: &Envelope{Seq: u64Ptr(1), ConversationID: "c", Event: []byte(`{"type":"error","message":"x"}`)}}},
		{"durable_event_without_seq", OutboundFrame{FrameType: "event", Envelope: &Envelope{ConversationID: "c", Event: []byte(`{"type":"message_end","message_id":"00000000-0000-4000-8000-000000000001"}`)}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.frame.Validate(); err == nil {
				t.Fatalf("expected Validate to reject %+v", tc.frame)
			}
		})
	}
}

func TestIngressDuplicateKeysCounterexample(t *testing.T) {
	appender := &fakeCommandAppender{}
	ingress, err := NewUserCommandIngress(appender, &fakeTokenVerifier{conversationID: "conv-1"})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("POST /conversations/{conversation_id}/commands", ingress)
	server := httptest.NewServer(mux)
	defer server.Close()

	body := []byte(`{"type":"user_message","type":"user_message","text":"x","text":"hi","attachments":[]}`)
	resp := postAuthorized(t, server.URL+"/conversations/conv-1/commands", body)
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusCreated {
		t.Fatalf("ingress accepted body with duplicate keys (status %d)", resp.StatusCode)
	}
}

func strPtr(s string) *string { return &s }
func u64Ptr(u uint64) *uint64 { return &u }

func TestValidateCommandAttachmentsRejectsAnyNonEmptyElement(t *testing.T) {
	cases := []string{
		`[1]`,
		`["x"]`,
		`[null]`,
		`[{}]`,
		`[[{}]]`,
	}
	for _, arr := range cases {
		raw := json.RawMessage(`{"type":"user_message","text":"hi","attachments":` + arr + `}`)
		err := ValidateCommand(raw)
		if !errors.Is(err, errAttachmentsNotEmpty) {
			t.Fatalf("expected attachments_not_empty for %s, got %v", arr, err)
		}
	}
}

func TestAgentHelloUnmarshalJSONStrict(t *testing.T) {
	valid := `{
		"agent_id":"agent-1",
		"generation":7,
		"last_sent_event_seq":0,
		"last_received_command_seq":0,
		"last_applied_command_seq":0
	}`
	var h AgentHello
	if err := json.Unmarshal([]byte(valid), &h); err != nil {
		t.Fatalf("valid hello rejected: %v", err)
	}

	unknownField := valid[:len(valid)-1] + `,"extra":true}`
	if err := json.Unmarshal([]byte(unknownField), &h); err == nil {
		t.Fatal("agent hello accepted unknown field")
	}

	missingField := `{"agent_id":"agent-1","generation":7}`
	if err := json.Unmarshal([]byte(missingField), &h); err == nil {
		t.Fatal("agent hello accepted missing fields")
	}

	trailing := valid + ` {}`
	if err := json.Unmarshal([]byte(trailing), &h); err == nil {
		t.Fatal("agent hello accepted trailing data")
	}
}

func TestEnvelopeRejectsMalformedEventBody(t *testing.T) {
	cases := []string{
		`{"seq":1,"conversation_id":"c","event":{"type":"message_end","message_id":"not-a-uuid","message":{"role":"user","content":[{"type":"text","text":"x"}],"timestamp":"2026-07-25T20:00:00Z"}}}`,
		`{"seq":1,"conversation_id":"c","event":{"type":"tool_execution_start","tool_call_id":"call-1","tool_name":"read_file","args":null}}`,
		`{"seq":1,"conversation_id":"c","event":{"type":"approval_resolved","request_id":"req-1","resolution":{"decision":{"type":"approve_once","extra":1}}}}`,
	}
	for _, raw := range cases {
		var env Envelope
		if err := json.Unmarshal([]byte(raw), &env); err == nil {
			t.Fatalf("envelope accepted malformed event: %s", raw)
		}
	}
}

func TestEnvelopeRejectsSeqExceedsJSONSafeInteger(t *testing.T) {
	raw := `{"seq":9007199254740992,"conversation_id":"c","event":{"type":"agent_start"}}`
	var env Envelope
	if err := json.Unmarshal([]byte(raw), &env); err == nil {
		t.Fatal("envelope accepted out-of-range seq")
	}
}

func TestOutboundFrameRejectsCommandAckSeqExceedsJSONSafeInteger(t *testing.T) {
	frame := OutboundFrame{
		FrameType: "command_ack",
		Ack: &CommandAck{
			Seq:        9007199254740992,
			CommandID:  "00000000-0000-4000-8000-000000000001",
			Status:     "received",
		},
	}
	if err := frame.Validate(); err == nil {
		t.Fatal("command_ack accepted out-of-range seq")
	}
}
