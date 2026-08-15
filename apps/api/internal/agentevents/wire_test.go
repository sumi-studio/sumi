package agentevents

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestContractFixturesRoundTrip(t *testing.T) {
	repoRoot, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(repoRoot, "contracts", "agent-events-fixtures.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixtures: %v", err)
	}

	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var fixtures map[string]any
	if err := d.Decode(&fixtures); err != nil {
		t.Fatalf("decode fixtures: %v", err)
	}

	passed := 0
	for name, value := range fixtures {
		fixture, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("fixture %q is not an object", name)
		}
		kind, _ := fixture["kind"].(string)
		wireRaw, err := json.Marshal(fixture["wire"])
		if err != nil {
			t.Fatalf("fixture %q: marshal wire: %v", name, err)
		}

		switch kind {
		case "outbound_frame":
			var frame OutboundFrame
			if err := json.Unmarshal(wireRaw, &frame); err != nil {
				t.Fatalf("fixture %q: unmarshal OutboundFrame: %v", name, err)
			}
			if err := frame.Validate(); err != nil {
				t.Fatalf("fixture %q: validate OutboundFrame: %v", name, err)
			}
			roundTripJSON(t, name, wireRaw, &frame)
		case "command_envelope":
			var env CommandEnvelope
			if err := json.Unmarshal(wireRaw, &env); err != nil {
				t.Fatalf("fixture %q: unmarshal CommandEnvelope: %v", name, err)
			}
			if err := ValidateCommand(env.Command); err != nil {
				t.Fatalf("fixture %q: validate command: %v", name, err)
			}
			roundTripJSON(t, name, wireRaw, &env)
		case "agent_hello":
			var hello AgentHello
			if err := json.Unmarshal(wireRaw, &hello); err != nil {
				t.Fatalf("fixture %q: unmarshal AgentHello: %v", name, err)
			}
			roundTripJSON(t, name, wireRaw, &hello)
		case "api_hello":
			var hello ApiHello
			if err := json.Unmarshal(wireRaw, &hello); err != nil {
				t.Fatalf("fixture %q: unmarshal ApiHello: %v", name, err)
			}
			roundTripJSON(t, name, wireRaw, &hello)
		case "agent_event":
			if err := validateEvent(wireRaw); err != nil {
				t.Fatalf("fixture %q: validate AgentEvent: %v", name, err)
			}
			roundTripGeneric(t, name, wireRaw)
		case "public_message":
			if err := validatePublicMessage(wireRaw); err != nil {
				t.Fatalf("fixture %q: validate PublicMessage: %v", name, err)
			}
			roundTripGeneric(t, name, wireRaw)
		case "browser_hello":
			hello, err := decodeBrowserHello(wireRaw)
			if err != nil {
				t.Fatalf("fixture %q: unmarshal BrowserHello: %v", name, err)
			}
			roundTripJSON(t, name, wireRaw, &hello)
		case "browser_command_frame":
			frame, err := decodeBrowserCommand(wireRaw)
			if err != nil {
				t.Fatalf("fixture %q: unmarshal BrowserCommandFrame: %v", name, err)
			}
			if _, err := validateBrowserCommand(frame.Command); err != nil {
				t.Fatalf("fixture %q: validate BrowserCommandFrame: %v", name, err)
			}
			roundTripJSON(t, name, wireRaw, &frame)
		case "browser_event_frame":
			var frame browserEventFrame
			if err := json.Unmarshal(wireRaw, &frame); err != nil {
				t.Fatalf("fixture %q: unmarshal BrowserEventFrame: %v", name, err)
			}
			roundTripJSON(t, name, wireRaw, &frame)
		case "browser_command_accepted":
			var frame browserCommandAcceptedFrame
			if err := json.Unmarshal(wireRaw, &frame); err != nil {
				t.Fatalf("fixture %q: unmarshal BrowserCommandAcceptedFrame: %v", name, err)
			}
			roundTripJSON(t, name, wireRaw, &frame)
		case "browser_command_rejected":
			var frame browserCommandRejectedFrame
			if err := json.Unmarshal(wireRaw, &frame); err != nil {
				t.Fatalf("fixture %q: unmarshal BrowserCommandRejectedFrame: %v", name, err)
			}
			roundTripJSON(t, name, wireRaw, &frame)
		case "browser_direct_chat_status":
			var frame directChatStatusFrame
			if err := json.Unmarshal(wireRaw, &frame); err != nil {
				t.Fatalf("fixture %q: unmarshal DirectChatStatusFrame: %v", name, err)
			}
			roundTripJSON(t, name, wireRaw, &frame)
		default:
			t.Fatalf("unknown fixture kind %q for %q", kind, name)
		}
		passed++
	}

	if passed < 10 {
		t.Fatalf("expected at least 10 fixtures, got %d", passed)
	}
}

func TestValidateCommandDispositionTerminalRules(t *testing.T) {
	valid := []string{
		`{"type":"command_disposition","command_id":"00000000-0000-4000-8000-000000000001","command_seq":1,"status":"applied"}`,
		`{"type":"command_disposition","command_id":"00000000-0000-4000-8000-000000000002","command_seq":2,"status":"superseded"}`,
		`{"type":"command_disposition","command_id":"00000000-0000-4000-8000-000000000003","command_seq":3,"status":"rejected","reject_reason":"oversized"}`,
	}
	for _, raw := range valid {
		if err := validateEvent([]byte(raw)); err != nil {
			t.Fatalf("valid command disposition rejected: %s: %v", raw, err)
		}
	}

	invalid := []string{
		`{"type":"command_disposition","command_id":"00000000-0000-4000-8000-000000000001","command_seq":1,"status":"received"}`,
		`{"type":"command_disposition","command_id":"00000000-0000-4000-8000-000000000001","command_seq":1,"status":"rejected"}`,
		`{"type":"command_disposition","command_id":"00000000-0000-4000-8000-000000000001","command_seq":1,"status":"applied","reject_reason":"oversized"}`,
		`{"type":"command_disposition","command_id":"00000000-0000-4000-8000-000000000001","command_seq":9007199254740992,"status":"applied"}`,
		`{"type":"command_disposition","command_id":"00000000-0000-4000-8000-000000000001","command_seq":1,"status":"rejected","reject_reason":"unavailable"}`,
	}
	for _, raw := range invalid {
		if err := validateEvent([]byte(raw)); err == nil {
			t.Fatalf("invalid command disposition accepted: %s", raw)
		}
	}
}

func roundTripJSON(t *testing.T, name string, original []byte, v any) {
	t.Helper()
	normalizedOriginal := normalizeJSON(t, original)

	out, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("fixture %q: marshal: %v", name, err)
	}
	normalizedRoundtrip := normalizeJSON(t, out)

	if string(normalizedOriginal) != string(normalizedRoundtrip) {
		t.Fatalf("fixture %q round-trip mismatch\noriginal:  %s\nroundtrip: %s", name, normalizedOriginal, normalizedRoundtrip)
	}
}

func roundTripGeneric(t *testing.T, name string, original []byte) {
	t.Helper()
	normalizedOriginal := normalizeJSON(t, original)

	d := json.NewDecoder(bytes.NewReader(original))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		t.Fatalf("fixture %q: decode: %v", name, err)
	}
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("fixture %q: marshal: %v", name, err)
	}
	normalizedRoundtrip := normalizeJSON(t, out)

	if string(normalizedOriginal) != string(normalizedRoundtrip) {
		t.Fatalf("fixture %q generic round-trip mismatch", name)
	}
}

func normalizeJSON(t *testing.T, data []byte) []byte {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		t.Fatalf("normalize JSON: %v", err)
	}
	out, err := json.Marshal(normalizeValue(v))
	if err != nil {
		t.Fatalf("marshal normalized: %v", err)
	}
	return out
}

func normalizeValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for _, k := range sortedKeys(x) {
			out[k] = normalizeValue(x[k])
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = normalizeValue(e)
		}
		return out
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return n
		}
		f, _ := x.Float64()
		return f
	default:
		return v
	}
}

func TestValidateApprovalDecisionUsesCurrentCallVocabularyOnly(t *testing.T) {
	for _, decision := range []string{"approve_once", "deny_once"} {
		raw := []byte(fmt.Sprintf(`{"type":"approval_decision","request_id":"r-1","decision":{"type":%q}}`, decision))
		if err := ValidateCommand(raw); err != nil {
			t.Fatalf("expected %s to be accepted, got %v", decision, err)
		}
	}

	for _, legacy := range []string{
		`{"type":"approval_decision","request_id":"r-1","decision":{"type":"deny"}}`,
		`{"type":"approval_decision","request_id":"r-1","decision":{"type":"approve_always","rule":{}}}`,
	} {
		if err := ValidateCommand([]byte(legacy)); err == nil {
			t.Fatalf("expected legacy approval decision to be rejected: %s", legacy)
		}
	}
}

func TestOutboundFrameRejectsExplicitNullRejectReason(t *testing.T) {
	for _, status := range []string{"received", "applied", "superseded"} {
		raw := []byte(fmt.Sprintf(`{"frame_type":"command_ack","ack":{"seq":1,"command_id":"00000000-0000-4000-8000-000000000001","status":"%s","reject_reason":null}}`, status))
		var frame OutboundFrame
		if err := json.Unmarshal(raw, &frame); err == nil {
			t.Fatalf("expected reject_reason:null to be rejected for status %s", status)
		}
	}

	// rejected status with null reject_reason must also be rejected.
	raw := []byte(`{"frame_type":"command_ack","ack":{"seq":1,"command_id":"00000000-0000-4000-8000-000000000001","status":"rejected","reject_reason":null}}`)
	var frame OutboundFrame
	if err := json.Unmarshal(raw, &frame); err == nil {
		t.Fatal("expected reject_reason:null to be rejected for rejected status")
	}

	// rejected status with a valid string reject_reason must be accepted.
	raw = []byte(`{"frame_type":"command_ack","ack":{"seq":1,"command_id":"00000000-0000-4000-8000-000000000001","personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","status":"rejected","reject_reason":"unknown_command"}}`)
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("expected valid rejected ack to be accepted, got %v", err)
	}
}

func TestOutboundFrameRejectsExplicitNullWrongBranch(t *testing.T) {
	tests := []string{
		`{"frame_type":"event","envelope":{"seq":1,"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"agent_start"}},"ack":null}`,
		`{"frame_type":"command_ack","envelope":null,"ack":{"seq":1,"command_id":"00000000-0000-4000-8000-000000000001","status":"received"}}`,
	}
	for _, raw := range tests {
		var frame OutboundFrame
		if err := json.Unmarshal([]byte(raw), &frame); err == nil {
			t.Fatalf("expected explicit null wrong branch to be rejected: %s", raw)
		}
	}
}

func TestCommandAckRejectsExplicitNullRejectReason(t *testing.T) {
	for _, status := range []string{"received", "rejected"} {
		raw := fmt.Sprintf(
			`{"seq":1,"command_id":"00000000-0000-4000-8000-000000000001","status":"%s","reject_reason":null}`,
			status,
		)
		var ack CommandAck
		if err := json.Unmarshal([]byte(raw), &ack); err == nil {
			t.Fatalf("expected standalone reject_reason:null to be rejected for status %s", status)
		}
	}
}

func TestValidateApprovalDecisionRequiresRequestID(t *testing.T) {
	for _, raw := range []string{
		`{"type":"approval_decision","decision":{"type":"approve_once"}}`,
		`{"type":"approval_decision","request_id":"","decision":{"type":"approve_once"}}`,
	} {
		if err := ValidateCommand([]byte(raw)); err == nil {
			t.Fatalf("expected missing or empty request_id to be rejected: %s", raw)
		}
	}
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

func TestEnvelopeRejectsExplicitNullSeq(t *testing.T) {
	for _, eventType := range []string{"message_update", "tool_execution_update", "error"} {
		raw := []byte(fmt.Sprintf(`{"seq":null,"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"%s"}}`, eventType))
		var env Envelope
		if err := json.Unmarshal(raw, &env); err == nil {
			t.Fatalf("expected seq:null to be rejected for volatile event %q", eventType)
		}
	}

	// Durable events require a non-null seq; explicit null is not allowed.
	raw := []byte(`{"seq":null,"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"agent_start"}}`)
	var env Envelope
	if err := json.Unmarshal(raw, &env); err == nil {
		t.Fatal("expected seq:null to be rejected for durable event")
	}

	// Missing seq is fine for volatile events.
	raw = []byte(`{"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"error","message":"x"}}`)
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("expected missing seq for volatile event, got %v", err)
	}

	// Missing seq is rejected for durable events.
	raw = []byte(`{"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"agent_start"}}`)
	if err := json.Unmarshal(raw, &env); err == nil {
		t.Fatal("expected missing seq to be rejected for durable event")
	}

	// A valid integer seq is accepted for durable events.
	raw = []byte(`{"seq":7,"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"agent_start"}}`)
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("expected integer seq for durable event, got %v", err)
	}
	if env.Seq == nil || *env.Seq != 7 {
		t.Fatalf("expected seq 7, got %v", env.Seq)
	}

	// A seq is rejected for volatile events even when non-null.
	raw = []byte(`{"seq":7,"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"error","message":"x"}}`)
	if err := json.Unmarshal(raw, &env); err == nil {
		t.Fatal("expected non-null seq to be rejected for volatile event")
	}
}

func TestUnmarshalStrictRejectsTrailingData(t *testing.T) {
	var cmd userMessageWire

	if err := unmarshalStrict([]byte(`{"type":"user_message","text":"hi","attachments":[]} trailing`), &cmd); err == nil {
		t.Fatal("expected trailing data to be rejected")
	}

	if err := unmarshalStrict([]byte(`{"type":"user_message","text":"hi","attachments":[]}`), &cmd); err != nil {
		t.Fatalf("expected clean input to be accepted, got %v", err)
	}

	// Trailing whitespace is acceptable.
	if err := unmarshalStrict([]byte(`{"type":"user_message","text":"hi","attachments":[]}   `+"\n"), &cmd); err != nil {
		t.Fatalf("expected trailing whitespace to be accepted, got %v", err)
	}
}

func TestOutboundFrameRejectsTrailingData(t *testing.T) {
	raw := []byte(`{"frame_type":"event","envelope":{"seq":1,"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"agent_start"}}} trailing`)
	var frame OutboundFrame
	if err := json.Unmarshal(raw, &frame); err == nil {
		t.Fatal("expected trailing data to be rejected for OutboundFrame")
	}
}

func TestAgentHelloRejectsUnknownFields(t *testing.T) {
	raw := []byte(`{"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","generation":"7","last_sent_event_seq":"0","last_received_command_seq":"0","last_applied_command_seq":"0","extra":true}`)
	var hello AgentHello
	if err := json.Unmarshal(raw, &hello); err == nil {
		t.Fatal("expected unknown fields to be rejected for AgentHello")
	}
}

func TestApiHelloRejectsUnknownFields(t *testing.T) {
	raw := []byte(`{"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","accepted_generation":"7","last_received_event_seq":"0","next_command_seq":"1","extra":true}`)
	var hello ApiHello
	if err := json.Unmarshal(raw, &hello); err == nil {
		t.Fatal("expected unknown fields to be rejected for ApiHello")
	}
}

func TestAgentHelloAcceptsFullWidthGenerationAndCursors(t *testing.T) {
	raw := []byte(`{"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","generation":"9223372036854775807","last_sent_event_seq":"18446744073709551615","last_received_command_seq":"18446744073709551615","last_applied_command_seq":"18446744073709551615"}`)
	var hello AgentHello
	if err := json.Unmarshal(raw, &hello); err != nil {
		t.Fatalf("expected full-width hello to be accepted: %v", err)
	}
	if hello.Generation != maxProcessGeneration || hello.LastSentEventSeq != ^uint64(0) {
		t.Fatalf("unexpected decoded full-width hello: %+v", hello)
	}
}

func TestHelloRejectsNonCanonicalDecimalAndOverflow(t *testing.T) {
	tests := []string{
		`{"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","generation":"07","last_sent_event_seq":"0","last_received_command_seq":"0","last_applied_command_seq":"0"}`,
		`{"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","generation":"+7","last_sent_event_seq":"0","last_received_command_seq":"0","last_applied_command_seq":"0"}`,
		`{"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","generation":"7","last_sent_event_seq":"18446744073709551616","last_received_command_seq":"0","last_applied_command_seq":"0"}`,
		`{"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","generation":7,"last_sent_event_seq":"0","last_received_command_seq":"0","last_applied_command_seq":"0"}`,
		`{"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","accepted_generation":"9223372036854775808","last_received_event_seq":"0","next_command_seq":"1"}`,
	}
	for _, raw := range tests {
		var hello AgentHello
		if strings.Contains(raw, "accepted_generation") {
			var apiHello ApiHello
			if err := json.Unmarshal([]byte(raw), &apiHello); err == nil {
				t.Fatalf("accepted noncanonical or overflowing API hello: %s", raw)
			}
			continue
		}
		if err := json.Unmarshal([]byte(raw), &hello); err == nil {
			t.Fatalf("accepted noncanonical or overflowing agent hello: %s", raw)
		}
	}
}

func TestAgentHelloMarshalRejectsOutOfRangeGeneration(t *testing.T) {
	hello := AgentHello{
		PersonalityAgentID:     "018f47a2-9b3c-7def-8abc-0123456789ab",
		Generation:             maxProcessGeneration + 1,
		LastSentEventSeq:       0,
		LastReceivedCommandSeq: 0,
		LastAppliedCommandSeq:  0,
	}
	if _, err := json.Marshal(hello); err == nil {
		t.Fatal("expected out-of-range generation to be rejected when marshaling AgentHello")
	}
}

func TestApiHelloMarshalRejectsOutOfRangeAcceptedGeneration(t *testing.T) {
	hello := ApiHello{
		PersonalityAgentID:   "018f47a2-9b3c-7def-8abc-0123456789ab",
		AcceptedGeneration:   maxProcessGeneration + 1,
		LastReceivedEventSeq: 0,
		NextCommandSeq:       1,
	}
	if _, err := json.Marshal(hello); err == nil {
		t.Fatal("expected out-of-range accepted_generation to be rejected when marshaling ApiHello")
	}
}

func TestHelloMarshalUsesCanonicalDecimalStrings(t *testing.T) {
	hello := ApiHello{
		PersonalityAgentID:   "018f47a2-9b3c-7def-8abc-0123456789ab",
		AcceptedGeneration:   maxProcessGeneration,
		LastReceivedEventSeq: ^uint64(0),
		NextCommandSeq:       ^uint64(0),
	}
	encoded, err := json.Marshal(hello)
	if err != nil {
		t.Fatalf("marshal full-width API hello: %v", err)
	}
	if got, want := string(encoded), `{"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","accepted_generation":"9223372036854775807","last_received_event_seq":"18446744073709551615","next_command_seq":"18446744073709551615"}`; got != want {
		t.Fatalf("hello wire mismatch\n got: %s\nwant: %s", got, want)
	}
}
