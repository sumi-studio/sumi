// Package agentevents contains the Go wire types for contracts/agent-events.yaml.
// It covers the production WebSocket protocol surface (hello, commands, acks,
// outbound frames) used by apps/api and the agent. Durable store semantics and
// identity/hydration seams are owned by T17/T26; this package only validates
// the wire contract.
package agentevents

import (
	"encoding/json"
	"fmt"
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
	type discriminator struct {
		Type string `json:"type"`
	}
	var d discriminator
	if err := json.Unmarshal(raw, &d); err != nil {
		return fmt.Errorf("command discriminator: %w", err)
	}
	switch d.Type {
	case "user_message":
		var cmd UserMessageCommand
		if err := json.Unmarshal(raw, &cmd); err != nil {
			return err
		}
		if len(cmd.Attachments) != 0 {
			return fmt.Errorf("attachments must be empty")
		}
	case "abort":
		var cmd AbortCommand
		if err := json.Unmarshal(raw, &cmd); err != nil {
			return err
		}
	case "approval_decision":
		var cmd ApprovalDecisionCommand
		if err := json.Unmarshal(raw, &cmd); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown command type: %q", d.Type)
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

// Validate returns an error if the frame does not match the public contract.
func (o OutboundFrame) Validate() error {
	switch o.FrameType {
	case "event":
		if o.Envelope == nil || o.Ack != nil {
			return fmt.Errorf("event frame must have envelope and no ack")
		}
	case "command_ack":
		if o.Ack == nil || o.Envelope != nil {
			return fmt.Errorf("command_ack frame must have ack and no envelope")
		}
	default:
		return fmt.Errorf("unknown frame_type: %q", o.FrameType)
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
