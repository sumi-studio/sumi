package agentevents

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
)

// ApprovalOperationOutcome is runtime-authored evidence of a parked operation's
// terminal result. It is not a Human's approval decision or a second tool call.
type ApprovalOperationOutcome struct {
	OperationID string          `json:"operation_id"`
	ToolCallID  string          `json:"tool_call_id"`
	Status      string          `json:"status"`
	Executed    *bool           `json:"executed"`
	Result      json.RawMessage `json:"result"`
}

func decodeApprovalOperation(fields map[string]json.RawMessage, discriminator string) (*ApprovalOperationOutcome, error) {
	keys := []string{discriminator, "operation_id", "tool_call_id", "status", "executed", "result"}
	if err := requireAndAllow(fields, keys, keys); err != nil {
		return nil, err
	}
	for _, key := range keys {
		if key != "executed" && bytes.Equal(bytes.TrimSpace(fields[key]), []byte("null")) {
			return nil, errors.New("approval operation fields cannot be null")
		}
	}
	raw := make(map[string]json.RawMessage, len(fields)-1)
	for key, value := range fields {
		if key != discriminator {
			raw[key] = value
		}
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var value ApprovalOperationOutcome
	if err = unmarshalStrict(data, &value); err != nil {
		return nil, err
	}
	if err = value.Validate(); err != nil {
		return nil, err
	}
	return &value, nil
}
func (v ApprovalOperationOutcome) Validate() error {
	if v.OperationID == "" || v.ToolCallID == "" {
		return errors.New("approval operation identity required")
	}
	switch v.Status {
	case "succeeded", "failed", "denied", "expired", "cancelled", "indeterminate":
	default:
		return errors.New("invalid approval operation status")
	}
	if (v.Status == "indeterminate" && v.Executed != nil) || (v.Status != "indeterminate" && (v.Executed == nil || *v.Executed != (v.Status == "succeeded" || v.Status == "failed"))) {
		return errors.New("approval operation executed/status mismatch")
	}
	if err := validateToolResultPayload(v.Result); err != nil {
		return err
	}
	var result struct {
		ToolCallID string `json:"tool_call_id"`
		IsError    bool   `json:"is_error"`
	}
	if err := json.Unmarshal(v.Result, &result); err != nil {
		return err
	}
	if result.ToolCallID != v.ToolCallID || result.IsError != (v.Status != "succeeded") {
		return errors.New("approval operation result mismatch")
	}
	return nil
}
func equalApprovalOperation(a, b *ApprovalOperationOutcome) bool {
	if a == nil || b == nil {
		return a == b
	}
	var ar, br any
	ad, bd := json.NewDecoder(bytes.NewReader(a.Result)), json.NewDecoder(bytes.NewReader(b.Result))
	ad.UseNumber()
	bd.UseNumber()
	if ad.Decode(&ar) != nil || bd.Decode(&br) != nil {
		return false
	}
	return a.OperationID == b.OperationID && a.ToolCallID == b.ToolCallID && a.Status == b.Status && reflect.DeepEqual(a.Executed, b.Executed) && reflect.DeepEqual(ar, br)
}
func (s *ProvenanceSource) UnmarshalJSON(data []byte) error {
	if err := checkDuplicateKeys(data); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	var surface string
	if err := json.Unmarshal(fields["surface"], &surface); err != nil {
		return err
	}
	if surface == "approval_operation" {
		outcome, err := decodeApprovalOperation(fields, "surface")
		if err != nil {
			return err
		}
		*s = ProvenanceSource{Surface: surface, ApprovalOperation: outcome}
		return nil
	}
	type plain ProvenanceSource
	var value plain
	if err := unmarshalStrict(data, &value); err != nil {
		return err
	}
	*s = ProvenanceSource(value)
	return nil
}
func (s ProvenanceSource) MarshalJSON() ([]byte, error) {
	if s.Surface == "approval_operation" {
		if s.ApprovalOperation == nil {
			return nil, errors.New("approval operation outcome missing")
		}
		return json.Marshal(struct {
			Surface string `json:"surface"`
			*ApprovalOperationOutcome
		}{s.Surface, s.ApprovalOperation})
	}
	type plain ProvenanceSource
	return json.Marshal(plain(s))
}
