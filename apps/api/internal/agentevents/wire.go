// Package agentevents contains the Go wire types for contracts/agent-events.yaml.
// It covers the production WebSocket protocol surface (hello, commands, acks,
// outbound frames) used by apps/api and the agent. Durable store semantics and
// identity/hydration seams are owned by T17/T26; this package only validates
// the wire contract.
package agentevents

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// maxJSONSafeInteger is the largest integer representable exactly by JavaScript's
// number type and by the contract's JsonSafeInteger and ProcessGeneration
// definitions.
const maxJSONSafeInteger uint64 = 9_007_199_254_740_991

// maxProcessGeneration is the upper bound shared with the Rust
// ProcessGeneration value type. Hello values are decimal strings so their
// representation is not limited by JavaScript's number range.
const maxProcessGeneration uint64 = 9_223_372_036_854_775_807

// AgentHello is sent by the agent immediately after the WebSocket upgrade.
// Generation and cursors use canonical decimal strings on the wire. This keeps
// the full u64 cursor domain and i64::MAX generation domain lossless for web
// clients while keeping the API's internal representation ergonomic.
type AgentHello struct {
	PersonalityAgentID     string `json:"personality_agent_id"`
	Generation             uint64 `json:"generation"`
	LastSentEventSeq       uint64 `json:"last_sent_event_seq"`
	LastReceivedCommandSeq uint64 `json:"last_received_command_seq"`
	LastAppliedCommandSeq  uint64 `json:"last_applied_command_seq"`
}

// UnmarshalJSON decodes an AgentHello with the same strict discipline used by
// the other production wire DTOs: duplicate keys, unknown fields, and trailing
// bytes are rejected, and every required field must be present.
func (h *AgentHello) UnmarshalJSON(data []byte) error {
	if err := checkDuplicateKeys(data); err != nil {
		return fmt.Errorf("agent hello json: %w", err)
	}
	type rawHello struct {
		PersonalityAgentID     *string `json:"personality_agent_id"`
		Generation             *string `json:"generation"`
		LastSentEventSeq       *string `json:"last_sent_event_seq"`
		LastReceivedCommandSeq *string `json:"last_received_command_seq"`
		LastAppliedCommandSeq  *string `json:"last_applied_command_seq"`
	}
	var raw rawHello
	if err := unmarshalStrict(data, &raw); err != nil {
		return err
	}
	if raw.PersonalityAgentID == nil {
		return fmt.Errorf("personality_agent_id is required")
	}
	if err := ValidatePersonalityAgentID(*raw.PersonalityAgentID); err != nil {
		return err
	}
	if raw.Generation == nil {
		return fmt.Errorf("generation is required")
	}
	if raw.LastSentEventSeq == nil {
		return fmt.Errorf("last_sent_event_seq is required")
	}
	if raw.LastReceivedCommandSeq == nil {
		return fmt.Errorf("last_received_command_seq is required")
	}
	if raw.LastAppliedCommandSeq == nil {
		return fmt.Errorf("last_applied_command_seq is required")
	}
	generation, err := parseCanonicalDecimal(*raw.Generation, maxProcessGeneration)
	if err != nil {
		return fmt.Errorf("generation: %w", err)
	}
	lastSentEventSeq, err := parseCanonicalDecimal(*raw.LastSentEventSeq, ^uint64(0))
	if err != nil {
		return fmt.Errorf("last_sent_event_seq: %w", err)
	}
	lastReceivedCommandSeq, err := parseCanonicalDecimal(*raw.LastReceivedCommandSeq, ^uint64(0))
	if err != nil {
		return fmt.Errorf("last_received_command_seq: %w", err)
	}
	lastAppliedCommandSeq, err := parseCanonicalDecimal(*raw.LastAppliedCommandSeq, ^uint64(0))
	if err != nil {
		return fmt.Errorf("last_applied_command_seq: %w", err)
	}
	*h = AgentHello{
		PersonalityAgentID:     *raw.PersonalityAgentID,
		Generation:             generation,
		LastSentEventSeq:       lastSentEventSeq,
		LastReceivedCommandSeq: lastReceivedCommandSeq,
		LastAppliedCommandSeq:  lastAppliedCommandSeq,
	}
	return nil
}

func (h AgentHello) MarshalJSON() ([]byte, error) {
	if err := ValidatePersonalityAgentID(h.PersonalityAgentID); err != nil {
		return nil, err
	}
	if h.Generation > maxProcessGeneration {
		return nil, fmt.Errorf("generation %d exceeds process generation range", h.Generation)
	}
	return json.Marshal(struct {
		PersonalityAgentID     string `json:"personality_agent_id"`
		Generation             string `json:"generation"`
		LastSentEventSeq       string `json:"last_sent_event_seq"`
		LastReceivedCommandSeq string `json:"last_received_command_seq"`
		LastAppliedCommandSeq  string `json:"last_applied_command_seq"`
	}{h.PersonalityAgentID, strconv.FormatUint(h.Generation, 10), strconv.FormatUint(h.LastSentEventSeq, 10), strconv.FormatUint(h.LastReceivedCommandSeq, 10), strconv.FormatUint(h.LastAppliedCommandSeq, 10)})
}

// ApiHello is returned by the API after verifying the token and generation.
// accepted_generation and cursor values are canonical decimal strings.
type ApiHello struct {
	PersonalityAgentID   string `json:"personality_agent_id"`
	AcceptedGeneration   uint64 `json:"accepted_generation"`
	LastReceivedEventSeq uint64 `json:"last_received_event_seq"`
	NextCommandSeq       uint64 `json:"next_command_seq"`
}

// UnmarshalJSON decodes an ApiHello with strict discipline: duplicate keys,
// unknown fields, and trailing bytes are rejected, and seq values stay within
// the JSON-safe integer range.
func (h *ApiHello) UnmarshalJSON(data []byte) error {
	if err := checkDuplicateKeys(data); err != nil {
		return fmt.Errorf("api hello json: %w", err)
	}
	type rawHello struct {
		PersonalityAgentID   *string `json:"personality_agent_id"`
		AcceptedGeneration   *string `json:"accepted_generation"`
		LastReceivedEventSeq *string `json:"last_received_event_seq"`
		NextCommandSeq       *string `json:"next_command_seq"`
	}
	var raw rawHello
	if err := unmarshalStrict(data, &raw); err != nil {
		return err
	}
	if raw.PersonalityAgentID == nil {
		return fmt.Errorf("personality_agent_id is required")
	}
	if err := ValidatePersonalityAgentID(*raw.PersonalityAgentID); err != nil {
		return err
	}
	if raw.AcceptedGeneration == nil {
		return fmt.Errorf("accepted_generation is required")
	}
	if raw.LastReceivedEventSeq == nil {
		return fmt.Errorf("last_received_event_seq is required")
	}
	if raw.NextCommandSeq == nil {
		return fmt.Errorf("next_command_seq is required")
	}
	acceptedGeneration, err := parseCanonicalDecimal(*raw.AcceptedGeneration, maxProcessGeneration)
	if err != nil {
		return fmt.Errorf("accepted_generation: %w", err)
	}
	lastReceivedEventSeq, err := parseCanonicalDecimal(*raw.LastReceivedEventSeq, ^uint64(0))
	if err != nil {
		return fmt.Errorf("last_received_event_seq: %w", err)
	}
	nextCommandSeq, err := parseCanonicalDecimal(*raw.NextCommandSeq, ^uint64(0))
	if err != nil {
		return fmt.Errorf("next_command_seq: %w", err)
	}
	*h = ApiHello{
		PersonalityAgentID:   *raw.PersonalityAgentID,
		AcceptedGeneration:   acceptedGeneration,
		LastReceivedEventSeq: lastReceivedEventSeq,
		NextCommandSeq:       nextCommandSeq,
	}
	return nil
}

func (h ApiHello) MarshalJSON() ([]byte, error) {
	if err := ValidatePersonalityAgentID(h.PersonalityAgentID); err != nil {
		return nil, err
	}
	if h.AcceptedGeneration > maxProcessGeneration {
		return nil, fmt.Errorf("accepted_generation %d exceeds process generation range", h.AcceptedGeneration)
	}
	return json.Marshal(struct {
		PersonalityAgentID   string `json:"personality_agent_id"`
		AcceptedGeneration   string `json:"accepted_generation"`
		LastReceivedEventSeq string `json:"last_received_event_seq"`
		NextCommandSeq       string `json:"next_command_seq"`
	}{h.PersonalityAgentID, strconv.FormatUint(h.AcceptedGeneration, 10), strconv.FormatUint(h.LastReceivedEventSeq, 10), strconv.FormatUint(h.NextCommandSeq, 10)})
}

// parseCanonicalDecimal accepts only the wire's lossless decimal form: 0 or a
// nonzero ASCII digit followed by ASCII digits. It rejects JSON numbers,
// signs, leading zeros, whitespace, exponents, fractions, and overflow.
func parseCanonicalDecimal(value string, max uint64) (uint64, error) {
	if value == "" {
		return 0, errors.New("empty decimal string")
	}
	if value != "0" && value[0] == '0' {
		return 0, errors.New("non-canonical leading zero")
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			return 0, errors.New("decimal string contains a non-digit")
		}
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, errors.New("decimal string exceeds u64 range")
	}
	if parsed > max {
		return 0, fmt.Errorf("%d exceeds allowed range", parsed)
	}
	return parsed, nil
}

// IncomingProvenance is immutable server-authored admission metadata. It is
// kept separate from caller-authored Command bytes and persists with them.
type IncomingProvenance struct {
	Version            uint8            `json:"version"`
	TenantID           string           `json:"tenant_id"`
	PersonalityAgentID string           `json:"personality_agent_id"`
	Actor              ProvenanceActor  `json:"actor"`
	Source             ProvenanceSource `json:"source"`
}

// DirectChatProvenance names the existing direct-chat call-site type. Both names
// share one validator; external callers must use the authenticated internal lane.
type DirectChatProvenance = IncomingProvenance

type ProvenanceActor struct {
	Kind        string `json:"kind"`
	PrincipalID string `json:"principal_id"`
	DisplayName string `json:"display_name,omitempty"`
}

type ProvenanceSource struct {
	ThreadID              string                     `json:"thread_id,omitempty"`
	Title                 string                     `json:"title,omitempty"`
	Revision              uint64                     `json:"revision,omitempty"`
	OperationID           string                     `json:"operation_id,omitempty"`
	OriginatingToolCallID string                     `json:"originating_tool_call_id,omitempty"`
	Result                *ProvenanceOperationResult `json:"result,omitempty"`
	Surface               string                     `json:"surface"`
	EventID               string                     `json:"event_id,omitempty"`
	Kind                  string                     `json:"kind,omitempty"`
	WorkspaceID           string                     `json:"workspace_id,omitempty"`
	InstallationID        string                     `json:"installation_id,omitempty"`
	AuthorityEpoch        uint64                     `json:"authority_epoch,omitempty"`
	Place                 *ProvenancePlace           `json:"place,omitempty"`
	MessageID             string                     `json:"message_id,omitempty"`
	ReplyToMessageID      string                     `json:"reply_to_message_id,omitempty"`
	PollVote              *ProvenancePollVote        `json:"poll_vote,omitempty"`
	MessageRevision       uint64                     `json:"message_revision,omitempty"`
	MessageSeq            uint64                     `json:"message_seq,omitempty"`
	OccurredAt            string                     `json:"occurred_at,omitempty"`
	MarkerID              string                     `json:"marker_id,omitempty"`
	DueAt                 string                     `json:"due_at,omitempty"`
}

type ProvenanceOperationResult struct {
	State           string `json:"state"`
	ExitCode        *int64 `json:"exit_code"`
	StdoutBytes     uint64 `json:"stdout_bytes"`
	StderrBytes     uint64 `json:"stderr_bytes"`
	OutputTruncated bool   `json:"output_truncated"`
}

func (v *ProvenanceOperationResult) UnmarshalJSON(data []byte) error {
	type wire ProvenanceOperationResult
	var decoded wire
	if err := unmarshalStrict(data, &decoded); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for _, key := range []string{"state", "exit_code", "stdout_bytes", "stderr_bytes", "output_truncated"} {
		raw, ok := fields[key]
		if !ok || (key != "exit_code" && bytes.Equal(bytes.TrimSpace(raw), []byte("null"))) {
			return fmt.Errorf("operation result %s is required", key)
		}
	}
	*v = ProvenanceOperationResult(decoded)
	return nil
}

func equalOperationResult(a, b *ProvenanceOperationResult) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.State == b.State && a.StdoutBytes == b.StdoutBytes && a.StderrBytes == b.StderrBytes && a.OutputTruncated == b.OutputTruncated &&
		((a.ExitCode == nil && b.ExitCode == nil) || (a.ExitCode != nil && b.ExitCode != nil && *a.ExitCode == *b.ExitCode))
}

var operationIDRegexp = regexp.MustCompile(`^[0-9a-f]{64}$`)
var operationEventIDRegexp = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func (p IncomingProvenance) validateWorkspaceOperation() error {
	source := p.Source
	result := source.Result
	if p.Actor.Kind != "personality_agent" || p.Actor.PrincipalID != p.PersonalityAgentID || source.Kind != "process_completed" || !operationEventIDRegexp.MatchString(source.EventID) || !operationIDRegexp.MatchString(source.OperationID) || source.OriginatingToolCallID == "" || result == nil {
		return errors.New("invalid workspace operation source")
	}
	if _, err := time.Parse(time.RFC3339Nano, source.OccurredAt); err != nil {
		return errors.New("operation occurrence must be RFC3339")
	}
	source.Surface, source.Kind, source.EventID, source.OperationID, source.OriginatingToolCallID, source.OccurredAt, source.Result = "", "", "", "", "", "", nil
	if source != (ProvenanceSource{}) {
		return errors.New("workspace operation cannot carry other source fields")
	}
	switch result.State {
	case "succeeded", "failed", "cancelled", "indeterminate":
	default:
		return errors.New("invalid operation terminal state")
	}
	if result.StdoutBytes > maxJSONSafeInteger || result.StderrBytes > maxJSONSafeInteger || (result.ExitCode != nil && (*result.ExitCode > int64(maxJSONSafeInteger) || *result.ExitCode < -int64(maxJSONSafeInteger))) {
		return errors.New("operation result numbers must be JSON-safe")
	}
	return nil
}

var provenanceIDRegexp = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}$`)

type ProvenancePlace struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
	Name string `json:"name"`
}

type ProvenancePollVote struct {
	PollRevision    uint64                 `json:"poll_revision"`
	Question        string                 `json:"question"`
	SelectedOptions []ProvenancePollOption `json:"selected_options"`
}

type ProvenancePollOption struct {
	OptionID string `json:"option_id"`
	Text     string `json:"text"`
}

func (v *ProvenancePollVote) UnmarshalJSON(data []byte) error {
	type wire ProvenancePollVote
	var decoded wire
	if err := unmarshalStrict(data, &decoded); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for _, key := range []string{"poll_revision", "question", "selected_options"} {
		if raw, ok := fields[key]; !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fmt.Errorf("poll vote %s is required", key)
		}
	}
	var options []map[string]json.RawMessage
	if err := json.Unmarshal(fields["selected_options"], &options); err != nil {
		return err
	}
	for _, option := range options {
		for _, key := range []string{"option_id", "text"} {
			if raw, ok := option[key]; !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				return fmt.Errorf("poll option %s is required", key)
			}
		}
	}
	*v = ProvenancePollVote(decoded)
	return nil
}

func (p IncomingProvenance) Equal(other IncomingProvenance) bool {
	a, b := p.Source.Place, other.Source.Place
	av, bv := p.Source.PollVote, other.Source.PollVote
	p.Source.Place, other.Source.Place = nil, nil
	p.Source.PollVote, other.Source.PollVote = nil, nil
	ar, br := p.Source.Result, other.Source.Result
	p.Source.Result, other.Source.Result = nil, nil
	return equalOperationResult(ar, br) && p == other && ((a == nil && b == nil) || (a != nil && b != nil && *a == *b)) &&
		((av == nil && bv == nil) || (av != nil && bv != nil && av.PollRevision == bv.PollRevision && av.Question == bv.Question && slices.Equal(av.SelectedOptions, bv.SelectedOptions)))
}

func (p IncomingProvenance) validateFeedback() error {
	source := p.Source
	if (p.Actor.Kind != "human" && p.Actor.Kind != "personality_agent") ||
		!operationEventIDRegexp.MatchString(source.EventID) || !operationEventIDRegexp.MatchString(source.ThreadID) ||
		strings.TrimSpace(source.Title) == "" || utf8.RuneCountInString(source.Title) > 160 ||
		source.Revision == 0 || source.Revision > maxJSONSafeInteger {
		return errors.New("invalid feedback source")
	}
	switch source.Kind {
	case "feedback_created", "feedback_reply", "feedback_status":
	default:
		return errors.New("invalid feedback event kind")
	}
	if _, err := time.Parse(time.RFC3339Nano, source.OccurredAt); err != nil {
		return errors.New("feedback occurrence must be RFC3339")
	}
	source.Surface, source.Kind, source.EventID, source.ThreadID, source.Title, source.Revision, source.OccurredAt = "", "", "", "", "", 0, ""
	if source != (ProvenanceSource{}) {
		return errors.New("feedback cannot carry other source fields")
	}
	return nil
}

func (p IncomingProvenance) Validate() error {
	if !provenanceIDRegexp.MatchString(p.TenantID) {
		return errors.New("provenance tenant_id must be 1..256 ASCII identifier bytes")
	}
	if err := ValidatePersonalityAgentID(p.PersonalityAgentID); err != nil {
		return err
	}
	if !provenanceIDRegexp.MatchString(p.Actor.PrincipalID) {
		return errors.New("invalid source actor principal")
	}
	if p.Version == 1 {
		if p.Actor.Kind != "human" || p.Actor.DisplayName != "" || p.Source != (ProvenanceSource{Surface: "direct_chat"}) {
			return errors.New("version 1 provenance requires direct-chat authenticated Human")
		}
		return nil
	}
	if p.Version == 2 && p.Source.Surface == "feedback" {
		return p.validateFeedback()
	}
	if p.Source.ThreadID != "" || p.Source.Title != "" || p.Source.Revision != 0 {
		return errors.New("feedback metadata requires feedback source")
	}
	if p.Version == 2 && p.Source.Surface == "workspace_operation" {
		return p.validateWorkspaceOperation()
	}
	if p.Source.OperationID != "" || p.Source.OriginatingToolCallID != "" || p.Source.Result != nil {
		return errors.New("operation metadata requires workspace_operation source")
	}
	if p.Version != 2 || p.Source.Surface != "messaging" {
		return errors.New("external provenance requires version 2 messaging source")
	}
	if p.Actor.Kind != "human" && p.Actor.Kind != "personality_agent" {
		return errors.New("invalid external source actor kind")
	}
	source := p.Source
	for _, id := range []string{source.EventID, source.WorkspaceID, source.InstallationID, source.MessageID} {
		if !canonicalUUIDRegexp.MatchString(id) {
			return errors.New("external source requires canonical UUID identifiers")
		}
	}
	for _, n := range []uint64{source.AuthorityEpoch, source.MessageRevision, source.MessageSeq} {
		if n == 0 || n > maxJSONSafeInteger {
			return errors.New("external source sequence must be positive JSON-safe integer")
		}
	}
	if source.Place == nil || !canonicalUUIDRegexp.MatchString(source.Place.ID) {
		return errors.New("external source place is required")
	}
	switch source.Place.Kind {
	case "channel", "thread", "dm", "group_dm":
	default:
		return errors.New("invalid external source place kind")
	}
	if _, err := time.Parse(time.RFC3339Nano, source.OccurredAt); err != nil {
		return errors.New("external source occurrence must be RFC3339")
	}
	if source.ReplyToMessageID != "" && ((source.Kind != "messaging_message" && source.Kind != "messaging_mention") || !canonicalUUIDRegexp.MatchString(source.ReplyToMessageID)) {
		return errors.New("reply metadata requires a messaging event and canonical target UUID")
	}
	switch source.Kind {
	case "messaging_poll_vote":
		v := source.PollVote
		if v == nil || v.PollRevision == 0 || v.PollRevision > maxJSONSafeInteger || v.SelectedOptions == nil || source.MarkerID != "" || source.DueAt != "" {
			return errors.New("poll vote requires a revision and selection, without reminder fields")
		}
		seen := make(map[string]bool, len(v.SelectedOptions))
		for _, option := range v.SelectedOptions {
			if !canonicalUUIDRegexp.MatchString(option.OptionID) || seen[option.OptionID] {
				return errors.New("poll vote requires distinct canonical option identifiers")
			}
			seen[option.OptionID] = true
		}
	case "messaging_mention", "messaging_message":
		if source.MarkerID != "" || source.DueAt != "" {
			return errors.New("mention cannot carry reminder fields")
		}
	case "reply_later_due":
		if p.Actor.Kind != "personality_agent" || p.Actor.PrincipalID != p.PersonalityAgentID || !canonicalUUIDRegexp.MatchString(source.MarkerID) {
			return errors.New("reminder must originate with recipient PA and marker")
		}
		if _, err := time.Parse(time.RFC3339Nano, source.DueAt); err != nil {
			return errors.New("reminder due_at must be RFC3339")
		}
	default:
		return errors.New("unknown external source kind")
	}
	if source.Kind != "messaging_poll_vote" && source.PollVote != nil {
		return errors.New("poll vote metadata requires a poll vote event")
	}
	return nil
}

func (p *IncomingProvenance) UnmarshalJSON(data []byte) error {
	if err := checkDuplicateKeys(data); err != nil {
		return fmt.Errorf("provenance json: %w", err)
	}
	type wire DirectChatProvenance
	var decoded wire
	if err := unmarshalStrict(data, &decoded); err != nil {
		return err
	}
	value := DirectChatProvenance(decoded)
	var fields struct {
		Actor  map[string]json.RawMessage `json:"actor"`
		Source map[string]json.RawMessage `json:"source"`
	}
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if value.Version == 1 {
		if len(fields.Actor) != 2 || len(fields.Source) != 1 {
			return errors.New("version 1 provenance has external source fields")
		}
	} else if value.Version == 2 && value.Source.Surface == "feedback" {
		if len(fields.Source) != 7 {
			return errors.New("feedback source has missing or extra fields")
		}
		for _, key := range []string{"surface", "kind", "event_id", "thread_id", "title", "revision", "occurred_at"} {
			if raw, ok := fields.Source[key]; !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				return fmt.Errorf("feedback source %s is required", key)
			}
		}
		if raw, ok := fields.Actor["display_name"]; ok && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return errors.New("actor display_name must be a string")
		}
	} else if value.Version == 2 && value.Source.Surface == "workspace_operation" {
		if len(fields.Source) != 7 {
			return errors.New("workspace operation source has missing or extra fields")
		}
		for _, key := range []string{"surface", "kind", "event_id", "operation_id", "originating_tool_call_id", "occurred_at", "result"} {
			if raw, ok := fields.Source[key]; !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				return fmt.Errorf("operation source %s is required", key)
			}
		}
		if raw, ok := fields.Actor["display_name"]; ok && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return errors.New("actor display_name must be a string")
		}
	} else if value.Version == 2 {
		for _, key := range []string{"operation_id", "originating_tool_call_id", "result", "thread_id", "title", "revision"} {
			if _, ok := fields.Source[key]; ok {
				return errors.New("operation metadata requires workspace_operation source")
			}
		}
		if raw, ok := fields.Actor["display_name"]; ok && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return errors.New("actor display_name must be a string")
		}
		for _, key := range []string{"surface", "event_id", "kind", "workspace_id", "installation_id", "authority_epoch", "place", "message_id", "message_revision", "message_seq", "occurred_at"} {
			if raw, ok := fields.Source[key]; !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				return fmt.Errorf("external source %s is required", key)
			}
		}
		if raw, ok := fields.Source["reply_to_message_id"]; ok {
			if (value.Source.Kind != "messaging_message" && value.Source.Kind != "messaging_mention") || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || value.Source.ReplyToMessageID == "" {
				return errors.New("reply_to_message_id requires a messaging event and canonical target UUID")
			}
		}
		if raw, ok := fields.Source["poll_vote"]; ok && (value.Source.Kind != "messaging_poll_vote" || bytes.Equal(bytes.TrimSpace(raw), []byte("null"))) {
			return errors.New("poll_vote requires a poll vote event and object")
		}
		if value.Source.Kind == "messaging_poll_vote" {
			for _, key := range []string{"marker_id", "due_at"} {
				if _, ok := fields.Source[key]; ok {
					return errors.New("poll vote cannot carry reminder fields")
				}
			}
		}
		if value.Source.Kind == "messaging_mention" {
			if _, ok := fields.Source["marker_id"]; ok {
				return errors.New("mention cannot carry marker_id")
			}
			if _, ok := fields.Source["due_at"]; ok {
				return errors.New("mention cannot carry due_at")
			}
		}
		var place map[string]json.RawMessage
		if err := json.Unmarshal(fields.Source["place"], &place); err != nil {
			return err
		}
		for _, key := range []string{"id", "kind", "name"} {
			if raw, ok := place[key]; !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				return fmt.Errorf("external place %s is required", key)
			}
		}
	}
	if err := value.Validate(); err != nil {
		return err
	}
	*p = value
	return nil
}

// CommandEnvelope is the complete authenticated durable command sent from the
// API to the agent. The top-level target is intentionally repeated so internal
// routing can verify it exactly matches authenticated provenance.
type CommandEnvelope struct {
	Seq                uint64               `json:"seq"`
	CommandID          string               `json:"command_id"`
	PersonalityAgentID string               `json:"personality_agent_id"`
	Provenance         DirectChatProvenance `json:"provenance"`
	Command            json.RawMessage      `json:"command"`
}

func (c CommandEnvelope) Validate() error {
	if c.Seq > maxJSONSafeInteger {
		return fmt.Errorf("seq %d exceeds JSON-safe integer range", c.Seq)
	}
	if !canonicalUUIDRegexp.MatchString(c.CommandID) {
		return errors.New("command_id must be a canonical UUID")
	}
	if err := ValidatePersonalityAgentID(c.PersonalityAgentID); err != nil {
		return err
	}
	if err := c.Provenance.Validate(); err != nil {
		return err
	}
	if c.PersonalityAgentID != c.Provenance.PersonalityAgentID {
		return errors.New("command target does not match provenance target")
	}
	if err := validateIncomingCommand(c.Provenance, c.Command); err != nil {
		return fmt.Errorf("invalid command: %w", err)
	}
	return nil
}

// UnmarshalJSON is deliberately strict because command envelopes cross both
// the WebSocket and durable-log trust boundaries.
func (c *CommandEnvelope) UnmarshalJSON(data []byte) error {
	if err := checkDuplicateKeys(data); err != nil {
		return fmt.Errorf("command envelope json: %w", err)
	}
	type rawEnvelope struct {
		Seq                *uint64               `json:"seq"`
		CommandID          *string               `json:"command_id"`
		PersonalityAgentID *string               `json:"personality_agent_id"`
		Provenance         *DirectChatProvenance `json:"provenance"`
		Command            json.RawMessage       `json:"command"`
	}
	var raw rawEnvelope
	if err := unmarshalStrict(data, &raw); err != nil {
		return err
	}
	if raw.Seq == nil || raw.CommandID == nil || raw.PersonalityAgentID == nil || raw.Provenance == nil || len(raw.Command) == 0 {
		return errors.New("seq, command_id, personality_agent_id, provenance, and command are required")
	}
	value := CommandEnvelope{
		Seq:                *raw.Seq,
		CommandID:          *raw.CommandID,
		PersonalityAgentID: *raw.PersonalityAgentID,
		Provenance:         *raw.Provenance,
		Command:            raw.Command,
	}
	if err := value.Validate(); err != nil {
		return err
	}
	*c = value
	return nil
}

// CommandType returns the value of the command object's top-level "type" field.
func (c CommandEnvelope) CommandType() (string, error) {
	type discriminator struct {
		Type string `json:"type"`
	}
	var d discriminator
	if err := json.Unmarshal(c.Command, &d); err != nil {
		return "", err
	}
	return d.Type, nil
}

// UserMessageCommand is the v1 user_message payload. Attachments must be empty.
type UserMessageCommand struct {
	Type        string       `json:"type"`
	Text        string       `json:"text"`
	Attachments []Attachment `json:"attachments"`
}

// AbortCommand is the no-payload abort command.
type AbortCommand struct {
	Type string `json:"type"`
}

// ApprovalDecisionCommand is an approval resolution from the UI.
type ApprovalDecisionCommand struct {
	Type      string          `json:"type"`
	RequestID string          `json:"request_id"`
	Decision  json.RawMessage `json:"decision"`
}

// Attachment is a placeholder; v1 only accepts an empty array.
type Attachment map[string]any

// ValidateCommand returns an error if the command payload violates the public
// contract (e.g. non-empty attachments or unknown variant).
func ValidateCommand(raw json.RawMessage) error {
	if err := checkDuplicateKeys(raw); err != nil {
		return fmt.Errorf("command json: %w", err)
	}
	type discriminator struct {
		Type string `json:"type"`
	}
	var d discriminator
	if err := json.Unmarshal(raw, &d); err != nil {
		return fmt.Errorf("command discriminator: %w", err)
	}
	switch d.Type {
	case "user_message":
		var cmd userMessageWire
		if err := unmarshalStrict(raw, &cmd); err != nil {
			return err
		}
		if cmd.Text == nil {
			return fmt.Errorf("text is required")
		}
		if cmd.Attachments == nil {
			return fmt.Errorf("attachments is required")
		}
	case "abort":
		var cmd AbortCommand
		if err := unmarshalStrict(raw, &cmd); err != nil {
			return err
		}
	case "approval_decision":
		var cmd ApprovalDecisionCommand
		if err := unmarshalStrict(raw, &cmd); err != nil {
			return err
		}
		if cmd.RequestID == "" {
			return fmt.Errorf("approval_decision request_id is required")
		}
		if err := validateApprovalDecision(cmd.Decision); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown command type: %q", d.Type)
	}
	return nil
}

// ExternalEventCommand is server-authored input, never accepted from a browser.
type ExternalEventCommand struct {
	Type    string `json:"type"`
	Content string `json:"content"`
}

func validateIncomingCommand(provenance IncomingProvenance, raw json.RawMessage) error {
	if err := provenance.Validate(); err != nil {
		return err
	}
	if provenance.Version == 1 {
		return ValidateCommand(raw)
	}
	if err := checkDuplicateKeys(raw); err != nil {
		return err
	}
	var command struct {
		Type    string  `json:"type"`
		Content *string `json:"content"`
	}
	if err := unmarshalStrict(raw, &command); err != nil {
		return err
	}
	if command.Type != "external_event" || command.Content == nil {
		return errors.New("external provenance requires external_event content")
	}
	return nil
}

// userMessageWire uses a pointer Text so we can distinguish missing text from
// an empty string, and a custom attachments type so we can distinguish null
// from an empty array and reject non-empty arrays.
type userMessageWire struct {
	Type        string      `json:"type"`
	Text        *string     `json:"text"`
	Attachments *emptyArray `json:"attachments"`
}

// emptyArray unmarshals only the literal empty JSON array. null or a non-empty
// array both return distinct sentinel errors. Elements are decoded independently
// so that a non-empty array of primitives, null, or objects is reported as
// attachments_not_empty rather than a generic JSON unmarshal error.
type emptyArray []struct{}

func (e *emptyArray) UnmarshalJSON(data []byte) error {
	if strings.EqualFold(strings.TrimSpace(string(data)), "null") {
		return errAttachmentsNull
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '[' {
		return fmt.Errorf("attachments must be an array")
	}

	if dec.More() {
		if _, err := dec.Token(); err != nil {
			return err
		}
		return errAttachmentsNotEmpty
	}

	if _, err := dec.Token(); err != nil {
		return err
	}
	*e = emptyArray{}
	return nil
}

func validateApprovalDecision(raw json.RawMessage) error {
	if len(raw) == 0 {
		return fmt.Errorf("approval decision is required")
	}
	type discriminator struct {
		Type string `json:"type"`
	}
	var d discriminator
	if err := json.Unmarshal(raw, &d); err != nil {
		return fmt.Errorf("approval decision discriminator: %w", err)
	}
	switch d.Type {
	case "approve_once", "deny_once":
		var v struct {
			Type string `json:"type"`
		}
		if err := unmarshalStrict(raw, &v); err != nil {
			return fmt.Errorf("approval decision %q: %w", d.Type, err)
		}
	default:
		return fmt.Errorf("unknown approval decision type: %q", d.Type)
	}
	return nil
}

// CommandAck is sent by the agent when a command reaches a terminal state.
type CommandAck struct {
	Seq                uint64  `json:"seq"`
	CommandID          string  `json:"command_id"`
	PersonalityAgentID string  `json:"personality_agent_id"`
	Status             string  `json:"status"`
	RejectReason       *string `json:"reject_reason,omitempty"`
}

// OutboundFrame is the agent -> API frame. Exactly one of Envelope or Ack is
// set, matching the frame_type discriminator.
type OutboundFrame struct {
	FrameType string      `json:"frame_type"`
	Envelope  *Envelope   `json:"envelope,omitempty"`
	Ack       *CommandAck `json:"ack,omitempty"`
}

var (
	commandAckStatuses  = map[string]bool{"received": true, "applied": true, "superseded": true, "rejected": true}
	rejectReasons       = map[string]bool{"unknown_command": true, "schema_violation": true, "attachments_not_empty": true, "oversized": true, "not_allowed": true}
	volatileEventTypes  = map[string]bool{"message_update": true, "tool_execution_update": true, "error": true}
	canonicalUUIDRegexp = regexp.MustCompile("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

	errAttachmentsNotEmpty = errors.New("attachments must be empty")
	errAttachmentsNull     = errors.New("attachments must be an empty array")
)

// UnmarshalJSON validates the raw frame against the wire contract while
// decoding. It rejects duplicate keys, unknown fields, and malformed acks or
// envelopes so readPump cannot receive a partially-valid frame.
func (o *OutboundFrame) UnmarshalJSON(data []byte) error {
	if err := checkDuplicateKeys(data); err != nil {
		return fmt.Errorf("outbound frame json: %w", err)
	}

	type rawFrame struct {
		FrameType string          `json:"frame_type"`
		Envelope  json.RawMessage `json:"envelope,omitempty"`
		Ack       json.RawMessage `json:"ack,omitempty"`
	}
	var raw rawFrame
	if err := unmarshalStrict(data, &raw); err != nil {
		return err
	}

	if raw.FrameType == "event" && len(raw.Ack) != 0 {
		return fmt.Errorf("event frame must not contain ack")
	}
	if raw.FrameType == "command_ack" && len(raw.Envelope) != 0 {
		return fmt.Errorf("command_ack frame must not contain envelope")
	}

	*o = OutboundFrame{FrameType: raw.FrameType}
	if len(raw.Envelope) != 0 {
		var envelope *Envelope
		if err := json.Unmarshal(raw.Envelope, &envelope); err != nil {
			return err
		}
		o.Envelope = envelope
	}
	if len(raw.Ack) != 0 {
		var rawAck rawCommandAck
		if err := unmarshalStrict(raw.Ack, &rawAck); err != nil {
			return err
		}
		ack, err := assembleCommandAck(rawAck)
		if err != nil {
			return err
		}
		o.Ack = ack
	}
	return o.Validate()
}

// assembleCommandAck converts rawCommandAck into CommandAck and enforces the
// schema rule that reject_reason is a string when status is rejected, and is
// absent (not null) for any other status.
func assembleCommandAck(raw rawCommandAck) (*CommandAck, error) {
	if raw.Seq == nil {
		return nil, fmt.Errorf("command_ack seq is required")
	}

	ack := CommandAck{
		Seq:                *raw.Seq,
		CommandID:          raw.CommandID,
		PersonalityAgentID: raw.PersonalityAgentID,
		Status:             raw.Status,
	}

	switch len(raw.RejectReason) {
	case 0:
		// Field is absent.
	case 4:
		if string(raw.RejectReason) == "null" {
			return nil, fmt.Errorf("command_ack reject_reason must not be explicit null")
		}
		fallthrough
	default:
		var reason string
		if err := json.Unmarshal(raw.RejectReason, &reason); err != nil {
			return nil, fmt.Errorf("command_ack reject_reason must be a string: %w", err)
		}
		ack.RejectReason = &reason
	}

	return &ack, nil
}

// rawCommandAck uses a pointer Seq so we can tell "missing" from "zero".
type rawCommandAck struct {
	Seq                *uint64         `json:"seq"`
	CommandID          string          `json:"command_id"`
	PersonalityAgentID string          `json:"personality_agent_id"`
	Status             string          `json:"status"`
	RejectReason       json.RawMessage `json:"reject_reason,omitempty"`
}

// Validate returns an error if the frame does not match the public contract.
func (o OutboundFrame) Validate() error {
	switch o.FrameType {
	case "event":
		if o.Envelope == nil || o.Ack != nil {
			return fmt.Errorf("event frame must have envelope and no ack")
		}
		if err := validateEnvelope(*o.Envelope); err != nil {
			return err
		}
	case "command_ack":
		if o.Ack == nil || o.Envelope != nil {
			return fmt.Errorf("command_ack frame must have ack and no envelope")
		}
		if err := validateCommandAck(*o.Ack); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown frame_type: %q", o.FrameType)
	}
	return nil
}

// UnmarshalJSON makes durable ack-log recovery (and any other CommandAck
// decoding) fail-closed on duplicate keys, unknown fields, trailing data,
// and schema/JSON-safe-integer violations.
func (ack *CommandAck) UnmarshalJSON(data []byte) error {
	if err := checkDuplicateKeys(data); err != nil {
		return fmt.Errorf("command ack json: %w", err)
	}
	var v rawCommandAck
	if err := unmarshalStrict(data, &v); err != nil {
		return err
	}
	parsed, err := assembleCommandAck(v)
	if err != nil {
		return err
	}
	if err := validateCommandAck(*parsed); err != nil {
		return err
	}
	*ack = *parsed
	return nil
}

func validateCommandAck(ack CommandAck) error {
	if ack.Seq > maxJSONSafeInteger {
		return fmt.Errorf("command_ack seq exceeds JSON-safe integer range")
	}
	if ack.CommandID == "" {
		return fmt.Errorf("command_ack command_id is required")
	}
	if !canonicalUUIDRegexp.MatchString(ack.CommandID) {
		return fmt.Errorf("command_ack command_id must be a canonical lowercase UUID")
	}
	if err := ValidatePersonalityAgentID(ack.PersonalityAgentID); err != nil {
		return fmt.Errorf("command_ack: %w", err)
	}
	if !commandAckStatuses[ack.Status] {
		return fmt.Errorf("command_ack status %q is not valid", ack.Status)
	}
	if ack.Status == "rejected" {
		if ack.RejectReason == nil || *ack.RejectReason == "" {
			return fmt.Errorf("rejected command_ack requires reject_reason")
		}
		if !rejectReasons[*ack.RejectReason] {
			return fmt.Errorf("command_ack reject_reason %q is not valid", *ack.RejectReason)
		}
	} else if ack.RejectReason != nil {
		return fmt.Errorf("command_ack reject_reason is only allowed when status is rejected")
	}
	return nil
}

// Envelope wraps a public agent event. The Event body is kept as RawMessage so
// the API can forward it without re-interpreting the event variant vocabulary;
// T17 owns the authoritative event type system.
type OutputAudience string

const (
	AudienceDirectChat OutputAudience = "direct_chat"
	AudienceSecretary  OutputAudience = "secretary"
)

type Envelope struct {
	Audience           OutputAudience  `json:"audience"`
	Seq                *uint64         `json:"seq,omitempty"`
	PersonalityAgentID string          `json:"personality_agent_id"`
	Event              json.RawMessage `json:"event"`
}

func validateEnvelope(e Envelope) error {
	if e.Audience != AudienceDirectChat && e.Audience != AudienceSecretary {
		return errors.New("envelope requires explicit output audience")
	}
	if err := ValidatePersonalityAgentID(e.PersonalityAgentID); err != nil {
		return fmt.Errorf("envelope: %w", err)
	}
	if len(e.Event) == 0 || !json.Valid(e.Event) {
		return fmt.Errorf("envelope event must be valid JSON")
	}
	type discriminator struct {
		Type string `json:"type"`
	}
	var d discriminator
	if err := json.Unmarshal(e.Event, &d); err != nil {
		return fmt.Errorf("envelope event type: %w", err)
	}
	if d.Type == "" {
		return fmt.Errorf("envelope event type is required")
	}
	volatile := volatileEventTypes[d.Type]
	if volatile && e.Seq != nil {
		return fmt.Errorf("volatile event %q must not have seq", d.Type)
	}
	if !volatile && e.Seq == nil {
		return fmt.Errorf("durable event %q requires seq", d.Type)
	}
	if e.Seq != nil && *e.Seq > maxJSONSafeInteger {
		return fmt.Errorf("envelope seq exceeds JSON-safe integer range")
	}
	if err := validateEvent(e.Event); err != nil {
		return err
	}
	// A source-bearing incoming experience can never be projected as a direct
	// chat message, even if a sender accidentally labels its envelope public.
	if d.Type == "message_start" || d.Type == "message_end" || d.Type == "turn_end" {
		var projected struct {
			Message struct {
				IncomingSource *IncomingProvenance `json:"incoming_source"`
			} `json:"message"`
		}
		if err := json.Unmarshal(e.Event, &projected); err != nil {
			return err
		}
		if source := projected.Message.IncomingSource; source != nil {
			if source.Version != 2 || source.PersonalityAgentID != e.PersonalityAgentID || e.Audience != AudienceSecretary {
				return errors.New("incoming source target or output audience mismatch")
			}
		}
	}
	return validateInternalEventArtifactReferences(e.Event, e.PersonalityAgentID)
}

// UnmarshalJSON decodes an Envelope and rejects an explicit JSON null in the
// seq field, matching the contracts/agent-events.yaml rule that volatile events
// must not have seq (even as null) and durable events require a non-null seq.
func (e *Envelope) UnmarshalJSON(data []byte) error {
	if err := checkDuplicateKeys(data); err != nil {
		return fmt.Errorf("envelope json: %w", err)
	}
	type envelopeRaw struct {
		Audience           OutputAudience  `json:"audience"`
		Seq                json.RawMessage `json:"seq"`
		PersonalityAgentID string          `json:"personality_agent_id"`
		Event              json.RawMessage `json:"event"`
	}
	var raw envelopeRaw
	if err := unmarshalStrict(data, &raw); err != nil {
		return err
	}

	if err := ValidatePersonalityAgentID(raw.PersonalityAgentID); err != nil {
		return fmt.Errorf("envelope: %w", err)
	}
	if len(raw.Event) == 0 || !json.Valid(raw.Event) {
		return fmt.Errorf("envelope event must be valid JSON")
	}

	eventType := eventType(raw.Event)
	volatile := volatileEventTypes[eventType]
	switch {
	case raw.Seq == nil:
		if !volatile {
			return fmt.Errorf("durable event %q requires seq", eventType)
		}
	case bytes.Equal(bytes.TrimSpace(raw.Seq), []byte("null")):
		// Explicit null is not allowed for any event: volatile events must
		// not have seq, and durable events require a real integer.
		if volatile {
			return fmt.Errorf("volatile event %q must not have seq", eventType)
		}
		return fmt.Errorf("durable event %q requires seq", eventType)
	default:
		var seq uint64
		if err := json.Unmarshal(raw.Seq, &seq); err != nil {
			return fmt.Errorf("envelope seq: %w", err)
		}
		if seq > maxJSONSafeInteger {
			return fmt.Errorf("envelope seq exceeds JSON-safe integer range")
		}
		if volatile {
			return fmt.Errorf("volatile event %q must not have seq", eventType)
		}
		e.Seq = &seq
	}

	e.Audience = raw.Audience
	e.PersonalityAgentID = raw.PersonalityAgentID
	e.Event = raw.Event
	return validateEnvelope(*e)
}

func eventType(raw json.RawMessage) string {
	type discriminator struct {
		Type string `json:"type"`
	}
	var d discriminator
	if err := json.Unmarshal(raw, &d); err != nil {
		return ""
	}
	return d.Type
}

// unmarshalStrict decodes one top-level JSON value with DisallowUnknownFields
// enabled and rejects any trailing tokens or non-whitespace bytes.
func unmarshalStrict(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	tok, err := dec.Token()
	if err == nil {
		return fmt.Errorf("trailing data after JSON value: %v", tok)
	}
	if !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

// DecodeStrictJSON applies the shared browser/API JSON boundary: duplicate
// object keys, unknown fields, and trailing values are all rejected.
func DecodeStrictJSON(data []byte, value any) error {
	if err := checkDuplicateKeys(data); err != nil {
		return err
	}
	return unmarshalStrict(data, value)
}

// checkDuplicateKeys walks the JSON value and returns an error if any object
// contains duplicate keys. It does not validate values.
func checkDuplicateKeys(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	t, err := dec.Token()
	if err != nil {
		return err
	}
	return checkValueForDuplicates(dec, t)
}

func checkValueForDuplicates(dec *json.Decoder, t json.Token) error {
	switch tok := t.(type) {
	case json.Delim:
		switch tok {
		case '{':
			seen := make(map[string]bool)
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return err
				}
				key, ok := keyTok.(string)
				if !ok {
					return fmt.Errorf("expected string object key")
				}
				if seen[key] {
					return fmt.Errorf("duplicate key %q", key)
				}
				seen[key] = true
				next, err := dec.Token()
				if err != nil {
					return err
				}
				if err := checkValueForDuplicates(dec, next); err != nil {
					return err
				}
			}
			// consume closing '}'
			if _, err := dec.Token(); err != nil {
				return err
			}
		case '[':
			for dec.More() {
				tok, err := dec.Token()
				if err != nil {
					return err
				}
				if err := checkValueForDuplicates(dec, tok); err != nil {
					return err
				}
			}
			// consume closing ']'
			if _, err := dec.Token(); err != nil {
				return err
			}
		}
	}
	return nil
}
