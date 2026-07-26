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
)

// AgentHello is sent by the agent immediately after the WebSocket upgrade.
// Generation is the ProcessGeneration bound to the short-lived credential claim.
type AgentHello struct {
	AgentID                string `json:"agent_id"`
	Generation             uint64 `json:"generation"`
	LastSentEventSeq       uint64 `json:"last_sent_event_seq"`
	LastReceivedCommandSeq uint64 `json:"last_received_command_seq"`
	LastAppliedCommandSeq  uint64 `json:"last_applied_command_seq"`
}

// ApiHello is returned by the API after verifying the token and generation.
type ApiHello struct {
	AcceptedGeneration   uint64 `json:"accepted_generation"`
	LastReceivedEventSeq uint64 `json:"last_received_event_seq"`
	NextCommandSeq       uint64 `json:"next_command_seq"`
}

// CommandEnvelope is a durable command sent from the API to the agent.
type CommandEnvelope struct {
	Seq       uint64          `json:"seq"`
	CommandID string          `json:"command_id"`
	Command   json.RawMessage `json:"command"`
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
		if err := validateApprovalDecision(cmd.Decision); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown command type: %q", d.Type)
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
// array both return distinct sentinel errors.
type emptyArray []struct{}

func (e *emptyArray) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return errAttachmentsNull
	}
	var arr []struct{}
	if err := json.Unmarshal(data, &arr); err != nil {
		return err
	}
	if len(arr) != 0 {
		return errAttachmentsNotEmpty
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
	case "approve_once", "deny":
		var v struct {
			Type string `json:"type"`
		}
		if err := unmarshalStrict(raw, &v); err != nil {
			return fmt.Errorf("approval decision %q: %w", d.Type, err)
		}
	case "approve_always":
		var v struct {
			Type string          `json:"type"`
			Rule json.RawMessage `json:"rule"`
		}
		if err := unmarshalStrict(raw, &v); err != nil {
			return fmt.Errorf("approval decision %q: %w", d.Type, err)
		}
		if len(v.Rule) == 0 {
			return fmt.Errorf("approve_always requires rule")
		}
		if string(v.Rule) == "null" {
			return fmt.Errorf("approve_always rule must be an object")
		}
		// DeferredApprovalRule must be a JSON object with open properties.
		var rule map[string]json.RawMessage
		if err := json.Unmarshal(v.Rule, &rule); err != nil {
			return fmt.Errorf("rule must be an object: %w", err)
		}
		if rule == nil {
			return fmt.Errorf("approve_always rule must be an object")
		}
	default:
		return fmt.Errorf("unknown approval decision type: %q", d.Type)
	}
	return nil
}

// CommandAck is sent by the agent when a command reaches a terminal state.
type CommandAck struct {
	Seq          uint64  `json:"seq"`
	CommandID    string  `json:"command_id"`
	Status       string  `json:"status"`
	RejectReason *string `json:"reject_reason,omitempty"`
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
	rejectReasons       = map[string]bool{"unknown_command": true, "schema_violation": true, "attachments_not_empty": true, "oversized": true}
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
		FrameType string         `json:"frame_type"`
		Envelope  *Envelope      `json:"envelope,omitempty"`
		Ack       *rawCommandAck `json:"ack,omitempty"`
	}
	var raw rawFrame
	if err := unmarshalStrict(data, &raw); err != nil {
		return err
	}

	*o = OutboundFrame{
		FrameType: raw.FrameType,
		Envelope:  raw.Envelope,
	}
	if raw.Ack != nil {
		ack, err := assembleCommandAck(*raw.Ack)
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
		Seq:       *raw.Seq,
		CommandID: raw.CommandID,
		Status:    raw.Status,
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
	Seq          *uint64         `json:"seq"`
	CommandID    string          `json:"command_id"`
	Status       string          `json:"status"`
	RejectReason json.RawMessage `json:"reject_reason,omitempty"`
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

func validateCommandAck(ack CommandAck) error {
	if ack.CommandID == "" {
		return fmt.Errorf("command_ack command_id is required")
	}
	if !canonicalUUIDRegexp.MatchString(ack.CommandID) {
		return fmt.Errorf("command_ack command_id must be a canonical lowercase UUID")
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
type Envelope struct {
	Seq            *uint64         `json:"seq,omitempty"`
	ConversationID string          `json:"conversation_id"`
	Event          json.RawMessage `json:"event"`
}

func validateEnvelope(e Envelope) error {
	if e.ConversationID == "" {
		return fmt.Errorf("envelope conversation_id is required")
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
	return nil
}

// UnmarshalJSON decodes an Envelope and rejects an explicit JSON null in the
// seq field, matching the contracts/agent-events.yaml rule that volatile events
// must not have seq (even as null) and durable events require a non-null seq.
func (e *Envelope) UnmarshalJSON(data []byte) error {
	if err := checkDuplicateKeys(data); err != nil {
		return fmt.Errorf("envelope json: %w", err)
	}
	type envelopeRaw struct {
		Seq            json.RawMessage `json:"seq"`
		ConversationID string          `json:"conversation_id"`
		Event          json.RawMessage `json:"event"`
	}
	var raw envelopeRaw
	if err := unmarshalStrict(data, &raw); err != nil {
		return err
	}

	if raw.ConversationID == "" {
		return fmt.Errorf("envelope conversation_id is required")
	}
	if len(raw.Event) == 0 || !json.Valid(raw.Event) {
		return fmt.Errorf("envelope event must be valid JSON")
	}
	type discriminator struct {
		Type string `json:"type"`
	}
	var d discriminator
	if err := json.Unmarshal(raw.Event, &d); err != nil {
		return fmt.Errorf("envelope event type: %w", err)
	}
	if d.Type == "" {
		return fmt.Errorf("envelope event type is required")
	}

	volatile := volatileEventTypes[d.Type]
	switch {
	case raw.Seq == nil:
		if !volatile {
			return fmt.Errorf("durable event %q requires seq", d.Type)
		}
	case bytes.Equal(bytes.TrimSpace(raw.Seq), []byte("null")):
		// Explicit null is not allowed for any event: volatile events must
		// not have seq, and durable events require a real integer.
		if volatile {
			return fmt.Errorf("volatile event %q must not have seq", d.Type)
		}
		return fmt.Errorf("durable event %q requires seq", d.Type)
	default:
		var seq uint64
		if err := json.Unmarshal(raw.Seq, &seq); err != nil {
			return fmt.Errorf("envelope seq: %w", err)
		}
		if volatile {
			return fmt.Errorf("volatile event %q must not have seq", d.Type)
		}
		e.Seq = &seq
	}

	e.ConversationID = raw.ConversationID
	e.Event = raw.Event
	return nil
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
