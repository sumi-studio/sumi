package agentevents

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
		default:
			t.Fatalf("unknown fixture kind %q for %q", kind, name)
		}
		passed++
	}

	if passed < 10 {
		t.Fatalf("expected at least 10 fixtures, got %d", passed)
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

func TestValidateApprovalDecisionRejectsNullRule(t *testing.T) {
	raw := []byte(`{"type":"approval_decision","request_id":"r-1","decision":{"type":"approve_always","rule":null}}`)
	if err := ValidateCommand(raw); err == nil {
		t.Fatal("expected approve_always with rule:null to be rejected")
	}

	raw = []byte(`{"type":"approval_decision","request_id":"r-1","decision":{"type":"approve_always","rule":{}}}`)
	if err := ValidateCommand(raw); err != nil {
		t.Fatalf("expected empty object rule to be accepted, got %v", err)
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
	raw = []byte(`{"frame_type":"command_ack","ack":{"seq":1,"command_id":"00000000-0000-4000-8000-000000000001","status":"rejected","reject_reason":"unknown_command"}}`)
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("expected valid rejected ack to be accepted, got %v", err)
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
		raw := []byte(fmt.Sprintf(`{"seq":null,"conversation_id":"c","event":{"type":"%s"}}`, eventType))
		var env Envelope
		if err := json.Unmarshal(raw, &env); err == nil {
			t.Fatalf("expected seq:null to be rejected for volatile event %q", eventType)
		}
	}

	// Durable events require a non-null seq; explicit null is not allowed.
	raw := []byte(`{"seq":null,"conversation_id":"c","event":{"type":"agent_start"}}`)
	var env Envelope
	if err := json.Unmarshal(raw, &env); err == nil {
		t.Fatal("expected seq:null to be rejected for durable event")
	}

	// Missing seq is fine for volatile events.
	raw = []byte(`{"conversation_id":"c","event":{"type":"error","message":"x"}}`)
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("expected missing seq for volatile event, got %v", err)
	}

	// Missing seq is rejected for durable events.
	raw = []byte(`{"conversation_id":"c","event":{"type":"agent_start"}}`)
	if err := json.Unmarshal(raw, &env); err == nil {
		t.Fatal("expected missing seq to be rejected for durable event")
	}

	// A valid integer seq is accepted for durable events.
	raw = []byte(`{"seq":7,"conversation_id":"c","event":{"type":"agent_start"}}`)
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("expected integer seq for durable event, got %v", err)
	}
	if env.Seq == nil || *env.Seq != 7 {
		t.Fatalf("expected seq 7, got %v", env.Seq)
	}

	// A seq is rejected for volatile events even when non-null.
	raw = []byte(`{"seq":7,"conversation_id":"c","event":{"type":"error","message":"x"}}`)
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
	raw := []byte(`{"frame_type":"event","envelope":{"seq":1,"conversation_id":"c","event":{"type":"agent_start"}}} trailing`)
	var frame OutboundFrame
	if err := json.Unmarshal(raw, &frame); err == nil {
		t.Fatal("expected trailing data to be rejected for OutboundFrame")
	}
}

func TestAgentHelloRejectsUnknownFields(t *testing.T) {
	raw := []byte(`{"agent_id":"agent-1","generation":7,"last_sent_event_seq":0,"last_received_command_seq":0,"last_applied_command_seq":0,"extra":true}`)
	var hello AgentHello
	if err := json.Unmarshal(raw, &hello); err == nil {
		t.Fatal("expected unknown fields to be rejected for AgentHello")
	}
}

func TestApiHelloRejectsUnknownFields(t *testing.T) {
	raw := []byte(`{"accepted_generation":7,"last_received_event_seq":0,"next_command_seq":1,"extra":true}`)
	var hello ApiHello
	if err := json.Unmarshal(raw, &hello); err == nil {
		t.Fatal("expected unknown fields to be rejected for ApiHello")
	}
}

func TestApiHelloRejectsOutOfRangeSeq(t *testing.T) {
	raw := []byte(fmt.Sprintf(`{"accepted_generation":7,"last_received_event_seq":%d,"next_command_seq":1}`, maxJSONSafeInteger+1))
	var hello ApiHello
	if err := json.Unmarshal(raw, &hello); err == nil {
		t.Fatal("expected out-of-range seq to be rejected for ApiHello")
	}
}

func TestAgentHelloAcceptsMaxSafeGeneration(t *testing.T) {
	raw := []byte(fmt.Sprintf(`{"agent_id":"agent-1","generation":%d,"last_sent_event_seq":0,"last_received_command_seq":0,"last_applied_command_seq":0}`, maxJSONSafeInteger))
	var hello AgentHello
	if err := json.Unmarshal(raw, &hello); err != nil {
		t.Fatalf("expected max-safe generation to be accepted: %v", err)
	}
	if hello.Generation != maxJSONSafeInteger {
		t.Fatalf("expected generation %d, got %d", maxJSONSafeInteger, hello.Generation)
	}
}

func TestAgentHelloRejectsOutOfRangeGeneration(t *testing.T) {
	raw := []byte(fmt.Sprintf(`{"agent_id":"agent-1","generation":%d,"last_sent_event_seq":0,"last_received_command_seq":0,"last_applied_command_seq":0}`, maxJSONSafeInteger+1))
	var hello AgentHello
	if err := json.Unmarshal(raw, &hello); err == nil {
		t.Fatal("expected out-of-range generation to be rejected for AgentHello")
	}
}

func TestApiHelloAcceptsMaxSafeAcceptedGeneration(t *testing.T) {
	raw := []byte(fmt.Sprintf(`{"accepted_generation":%d,"last_received_event_seq":0,"next_command_seq":1}`, maxJSONSafeInteger))
	var hello ApiHello
	if err := json.Unmarshal(raw, &hello); err != nil {
		t.Fatalf("expected max-safe accepted_generation to be accepted: %v", err)
	}
	if hello.AcceptedGeneration != maxJSONSafeInteger {
		t.Fatalf("expected accepted_generation %d, got %d", maxJSONSafeInteger, hello.AcceptedGeneration)
	}
}

func TestApiHelloRejectsOutOfRangeAcceptedGeneration(t *testing.T) {
	raw := []byte(fmt.Sprintf(`{"accepted_generation":%d,"last_received_event_seq":0,"next_command_seq":1}`, maxJSONSafeInteger+1))
	var hello ApiHello
	if err := json.Unmarshal(raw, &hello); err == nil {
		t.Fatal("expected out-of-range accepted_generation to be rejected for ApiHello")
	}
}

func TestAgentHelloMarshalRejectsOutOfRangeGeneration(t *testing.T) {
	hello := AgentHello{
		AgentID:                "agent-1",
		Generation:             maxJSONSafeInteger + 1,
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
		AcceptedGeneration:   maxJSONSafeInteger + 1,
		LastReceivedEventSeq: 0,
		NextCommandSeq:       1,
	}
	if _, err := json.Marshal(hello); err == nil {
		t.Fatal("expected out-of-range accepted_generation to be rejected when marshaling ApiHello")
	}
}

func TestApiHelloMarshalRejectsOutOfRangeLastReceivedEventSeq(t *testing.T) {
	hello := ApiHello{
		AcceptedGeneration:   1,
		LastReceivedEventSeq: maxJSONSafeInteger + 1,
		NextCommandSeq:       1,
	}
	if _, err := json.Marshal(hello); err == nil {
		t.Fatal("expected out-of-range last_received_event_seq to be rejected when marshaling ApiHello")
	}
}

func TestApiHelloMarshalRejectsOutOfRangeNextCommandSeq(t *testing.T) {
	hello := ApiHello{
		AcceptedGeneration:   1,
		LastReceivedEventSeq: 0,
		NextCommandSeq:       maxJSONSafeInteger + 1,
	}
	if _, err := json.Marshal(hello); err == nil {
		t.Fatal("expected out-of-range next_command_seq to be rejected when marshaling ApiHello")
	}
}

func TestApiHelloMarshalAcceptsMaxSafeSeqFields(t *testing.T) {
	hello := ApiHello{
		AcceptedGeneration:   1,
		LastReceivedEventSeq: maxJSONSafeInteger,
		NextCommandSeq:       maxJSONSafeInteger,
	}
	if _, err := json.Marshal(hello); err != nil {
		t.Fatalf("expected max-safe seq fields to marshal: %v", err)
	}
}
