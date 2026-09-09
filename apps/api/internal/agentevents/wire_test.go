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
			if err := env.Validate(); err != nil {
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
		`{"frame_type":"event","envelope":{"audience":"direct_chat","seq":1,"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"agent_start"}},"ack":null}`,
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
		raw := []byte(fmt.Sprintf(`{"audience":"direct_chat","seq":null,"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"%s"}}`, eventType))
		var env Envelope
		if err := json.Unmarshal(raw, &env); err == nil {
			t.Fatalf("expected seq:null to be rejected for volatile event %q", eventType)
		}
	}

	// Durable events require a non-null seq; explicit null is not allowed.
	raw := []byte(`{"audience":"direct_chat","seq":null,"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"agent_start"}}`)
	var env Envelope
	if err := json.Unmarshal(raw, &env); err == nil {
		t.Fatal("expected seq:null to be rejected for durable event")
	}

	// Missing seq is fine for volatile events.
	raw = []byte(`{"audience":"direct_chat","personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"error","message":"x"}}`)
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("expected missing seq for volatile event, got %v", err)
	}

	// Missing seq is rejected for durable events.
	raw = []byte(`{"audience":"direct_chat","personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"agent_start"}}`)
	if err := json.Unmarshal(raw, &env); err == nil {
		t.Fatal("expected missing seq to be rejected for durable event")
	}

	// A valid integer seq is accepted for durable events.
	raw = []byte(`{"audience":"direct_chat","seq":7,"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"agent_start"}}`)
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("expected integer seq for durable event, got %v", err)
	}
	if env.Seq == nil || *env.Seq != 7 {
		t.Fatalf("expected seq 7, got %v", env.Seq)
	}

	// A seq is rejected for volatile events even when non-null.
	raw = []byte(`{"audience":"direct_chat","seq":7,"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"error","message":"x"}}`)
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
	raw := []byte(`{"frame_type":"event","envelope":{"audience":"direct_chat","seq":1,"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"agent_start"}}} trailing`)
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

func TestReplyProvenanceMetadataRejectsMalformedTargets(t *testing.T) {
	raw, err := os.ReadFile("../../../../contracts/agent-events-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures map[string]struct {
		Wire struct {
			Provenance json.RawMessage `json:"provenance"`
		} `json:"wire"`
	}
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"external_reply", "external_mention", "external_reminder"} {
		for _, target := range []any{nil, "", "not-a-uuid", "01992000-0000-7000-8000-000000000008"} {
			var value map[string]any
			if err := json.Unmarshal(fixtures[kind].Wire.Provenance, &value); err != nil {
				t.Fatal(err)
			}
			value["source"].(map[string]any)["reply_to_message_id"] = target
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			var provenance IncomingProvenance
			err = json.Unmarshal(encoded, &provenance)
			valid := target == "01992000-0000-7000-8000-000000000008" && kind != "external_reminder"
			if (err == nil) != valid {
				t.Fatalf("%s target=%v accepted=%v", kind, target, err == nil)
			}
		}
	}
}

func TestPollVoteProvenancePreservesSelectionAndEquality(t *testing.T) {
	const id = "01992000-0000-7000-8000-000000000008"
	p := IncomingProvenance{
		Version: 2, TenantID: "tenant", PersonalityAgentID: id,
		Actor: ProvenanceActor{Kind: "human", PrincipalID: id},
		Source: ProvenanceSource{Surface: "messaging", Kind: "messaging_poll_vote", EventID: id, WorkspaceID: id, InstallationID: id,
			AuthorityEpoch: 1, Place: &ProvenancePlace{ID: id, Kind: "channel", Name: "Shared"}, MessageID: id, MessageRevision: 1, MessageSeq: 1,
			OccurredAt: "2026-09-09T08:00:00Z", PollVote: &ProvenancePollVote{PollRevision: 2, Question: "When?", SelectedOptions: []ProvenancePollOption{{OptionID: id, Text: "Afternoon"}}}},
	}
	for _, selection := range [][]ProvenancePollOption{p.Source.PollVote.SelectedOptions, {}} {
		p.Source.PollVote.SelectedOptions = selection
		encoded, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		var got IncomingProvenance
		if err := json.Unmarshal(encoded, &got); err != nil {
			t.Fatal(err)
		}
		if !p.Equal(got) {
			t.Fatal("round-trip changed poll provenance")
		}
		got.Source.PollVote.PollRevision++
		if p.Equal(got) {
			t.Fatal("different votes compared equal")
		}
	}
}

func TestPollVoteProvenanceRejectsAmbiguousSelections(t *testing.T) {
	fixture, err := os.ReadFile("../../../../contracts/agent-events-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures map[string]struct {
		Wire struct {
			Provenance json.RawMessage `json:"provenance"`
		} `json:"wire"`
	}
	if err := json.Unmarshal(fixture, &fixtures); err != nil {
		t.Fatal(err)
	}
	const id = "01992000-0000-7000-8000-000000000008"
	for _, tc := range []struct {
		name   string
		change func(map[string]any, map[string]any)
	}{
		{"missing vote", func(s, v map[string]any) { delete(s, "poll_vote") }},
		{"null vote", func(s, v map[string]any) { s["poll_vote"] = nil }},
		{"zero revision", func(s, v map[string]any) { v["poll_revision"] = 0 }},
		{"null question", func(s, v map[string]any) { v["question"] = nil }},
		{"missing question", func(s, v map[string]any) { delete(v, "question") }},
		{"null selection", func(s, v map[string]any) { v["selected_options"] = nil }},
		{"duplicate option", func(s, v map[string]any) {
			v["selected_options"] = []any{map[string]any{"option_id": id, "text": "A"}, map[string]any{"option_id": id, "text": "B"}}
		}},
		{"missing option label", func(s, v map[string]any) { v["selected_options"] = []any{map[string]any{"option_id": id}} }},
		{"reply linkage", func(s, v map[string]any) { s["reply_to_message_id"] = id }},
		{"reminder field", func(s, v map[string]any) { s["due_at"] = nil }},
		{"ordinary event", func(s, v map[string]any) { s["kind"] = "messaging_message" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var p map[string]any
			if err := json.Unmarshal(fixtures["external_mention"].Wire.Provenance, &p); err != nil {
				t.Fatal(err)
			}
			s := p["source"].(map[string]any)
			s["kind"] = "messaging_poll_vote"
			v := map[string]any{"poll_revision": 2, "question": "When?", "selected_options": []any{map[string]any{"option_id": id, "text": "Afternoon"}}}
			s["poll_vote"] = v
			tc.change(s, v)
			raw, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			var got IncomingProvenance
			if err := json.Unmarshal(raw, &got); err == nil {
				t.Fatal("accepted ambiguous poll vote")
			}
		})
	}
}

func TestWorkspaceOperationProvenanceRoundTripAndValidation(t *testing.T) {
	raw, err := os.ReadFile("../../../../contracts/agent-events-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures map[string]struct {
		Wire json.RawMessage `json:"wire"`
	}
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"external_process_completed", "external_process_indeterminate"} {
		var envelope CommandEnvelope
		if err := json.Unmarshal(fixtures[key].Wire, &envelope); err != nil {
			t.Fatal(err)
		}
		if err := envelope.Validate(); err != nil {
			t.Fatal(err)
		}
		encoded, _ := json.Marshal(envelope.Provenance)
		var restored IncomingProvenance
		if err := json.Unmarshal(encoded, &restored); err != nil {
			t.Fatal(err)
		}
		if !restored.Equal(envelope.Provenance) {
			t.Fatal("terminal evidence changed")
		}
		restored.Source.Result.StdoutBytes++
		if restored.Equal(envelope.Provenance) {
			t.Fatal("result changes must alter provenance equality")
		}
	}
	var envelope struct {
		Provenance json.RawMessage `json:"provenance"`
	}
	if err := json.Unmarshal(fixtures["external_process_completed"].Wire, &envelope); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(map[string]any){
		func(v map[string]any) { v["actor"].(map[string]any)["kind"] = "human" },
		func(v map[string]any) { v["actor"].(map[string]any)["principal_id"] = "another-pa" },
		func(v map[string]any) {
			v["source"].(map[string]any)["event_id"] = "01992000-0000-4000-8000-000000000021"
		},
		func(v map[string]any) { v["source"].(map[string]any)["operation_id"] = "ABC" },
		func(v map[string]any) { v["source"].(map[string]any)["message_id"] = nil },
		func(v map[string]any) { v["source"].(map[string]any)["surface"] = "messaging" },
		func(v map[string]any) { delete(v["source"].(map[string]any)["result"].(map[string]any), "exit_code") },
		func(v map[string]any) { v["source"].(map[string]any)["result"].(map[string]any)["stdout_bytes"] = -1 },
		func(v map[string]any) { v["source"].(map[string]any)["result"].(map[string]any)["state"] = "running" },
	} {
		var v map[string]any
		if err := json.Unmarshal(envelope.Provenance, &v); err != nil {
			t.Fatal(err)
		}
		mutate(v)
		bad, _ := json.Marshal(v)
		var p IncomingProvenance
		if err := json.Unmarshal(bad, &p); err == nil {
			t.Fatalf("accepted invalid operation: %s", bad)
		}
	}
}
