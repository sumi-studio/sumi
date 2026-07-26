package agentevents

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// validateEvent validates the body of an AgentEvent against the public
// contracts/agent-events.yaml schema. It is called from validateEnvelope after
// the outer envelope has already been checked for duplicate keys.
func validateEvent(raw json.RawMessage) error {
	if err := checkDuplicateKeys(raw); err != nil {
		return err
	}
	obj, err := asObject(raw, "agent event")
	if err != nil {
		return err
	}
	typ, err := requireString(obj, "type")
	if err != nil {
		return err
	}

	switch typ {
	case "agent_start", "agent_end", "turn_start":
		return requireAndAllow(obj, []string{"type"}, []string{"type"})

	case "turn_end":
		if err := requireAndAllow(obj, []string{"type", "message", "tool_results"}, []string{"type", "message", "tool_results"}); err != nil {
			return err
		}
		if err := validateNullablePublicMessage(obj["message"]); err != nil {
			return fmt.Errorf("turn_end.message: %w", err)
		}
		if err := validateArray(obj["tool_results"], validateToolResultPayload); err != nil {
			return fmt.Errorf("turn_end.tool_results: %w", err)
		}
		return nil

	case "message_start", "message_end":
		if err := requireAndAllow(obj, []string{"type", "message_id", "message"}, []string{"type", "message_id", "message"}); err != nil {
			return err
		}
		if err := validateUUID(obj["message_id"]); err != nil {
			return fmt.Errorf("%s.message_id: %w", typ, err)
		}
		if err := validatePublicMessage(obj["message"]); err != nil {
			return fmt.Errorf("%s.message: %w", typ, err)
		}
		return nil

	case "message_update":
		if err := requireAndAllow(obj, []string{"type", "message_id", "event"}, []string{"type", "message_id", "event"}); err != nil {
			return err
		}
		if err := validateUUID(obj["message_id"]); err != nil {
			return fmt.Errorf("message_update.message_id: %w", err)
		}
		if err := validatePublicStreamEvent(obj["event"]); err != nil {
			return fmt.Errorf("message_update.event: %w", err)
		}
		return nil

	case "tool_execution_start":
		if err := requireAndAllow(obj, []string{"type", "tool_call_id", "tool_name", "args"}, []string{"type", "tool_call_id", "tool_name", "args"}); err != nil {
			return err
		}
		if err := validateString(obj["tool_call_id"]); err != nil {
			return fmt.Errorf("tool_execution_start.tool_call_id: %w", err)
		}
		if err := validateString(obj["tool_name"]); err != nil {
			return fmt.Errorf("tool_execution_start.tool_name: %w", err)
		}
		if err := validateObjectNotNull(obj["args"]); err != nil {
			return fmt.Errorf("tool_execution_start.args: %w", err)
		}
		return nil

	case "tool_execution_update":
		if err := requireAndAllow(obj, []string{"type", "tool_call_id", "partial"}, []string{"type", "tool_call_id", "partial"}); err != nil {
			return err
		}
		if err := validateString(obj["tool_call_id"]); err != nil {
			return fmt.Errorf("tool_execution_update.tool_call_id: %w", err)
		}
		if !json.Valid(obj["partial"]) {
			return fmt.Errorf("tool_execution_update.partial: invalid JSON")
		}
		return nil

	case "tool_execution_end":
		if err := requireAndAllow(obj, []string{"type", "tool_call_id", "result", "is_error"}, []string{"type", "tool_call_id", "result", "is_error"}); err != nil {
			return err
		}
		if err := validateString(obj["tool_call_id"]); err != nil {
			return fmt.Errorf("tool_execution_end.tool_call_id: %w", err)
		}
		if !json.Valid(obj["result"]) {
			return fmt.Errorf("tool_execution_end.result: invalid JSON")
		}
		if err := validateBool(obj["is_error"]); err != nil {
			return fmt.Errorf("tool_execution_end.is_error: %w", err)
		}
		return nil

	case "approval_requested":
		if err := requireAndAllow(obj, []string{"type", "request"}, []string{"type", "request"}); err != nil {
			return err
		}
		if err := validateApprovalRequest(obj["request"]); err != nil {
			return fmt.Errorf("approval_requested.request: %w", err)
		}
		return nil

	case "approval_resolved":
		if err := requireAndAllow(obj, []string{"type", "request_id", "resolution"}, []string{"type", "request_id", "resolution"}); err != nil {
			return err
		}
		if err := validateString(obj["request_id"]); err != nil {
			return fmt.Errorf("approval_resolved.request_id: %w", err)
		}
		if err := validateApprovalResolution(obj["resolution"]); err != nil {
			return fmt.Errorf("approval_resolved.resolution: %w", err)
		}
		return nil

	case "steered":
		if err := requireAndAllow(obj, []string{"type", "mode"}, []string{"type", "mode"}); err != nil {
			return err
		}
		if err := validateEnumString(obj["mode"], steerModes); err != nil {
			return fmt.Errorf("steered.mode: %w", err)
		}
		return nil

	case "memory_maintenance":
		if err := requireAndAllow(obj, []string{"type", "kind"}, []string{"type", "kind"}); err != nil {
			return err
		}
		if err := validateString(obj["kind"]); err != nil {
			return fmt.Errorf("memory_maintenance.kind: %w", err)
		}
		return nil

	case "retry_scheduled":
		if err := requireAndAllow(obj, []string{"type", "attempt", "delay_ms", "retry_at", "error_message"}, []string{"type", "attempt", "delay_ms", "retry_at", "error_message"}); err != nil {
			return err
		}
		if err := validateJSONSafeInteger(obj["attempt"]); err != nil {
			return fmt.Errorf("retry_scheduled.attempt: %w", err)
		}
		if err := validateJSONSafeInteger(obj["delay_ms"]); err != nil {
			return fmt.Errorf("retry_scheduled.delay_ms: %w", err)
		}
		if err := validateString(obj["retry_at"]); err != nil {
			return fmt.Errorf("retry_scheduled.retry_at: %w", err)
		}
		if err := validateString(obj["error_message"]); err != nil {
			return fmt.Errorf("retry_scheduled.error_message: %w", err)
		}
		return nil

	case "error":
		if err := requireAndAllow(obj, []string{"type", "message"}, []string{"type", "message"}); err != nil {
			return err
		}
		if err := validateString(obj["message"]); err != nil {
			return fmt.Errorf("error.message: %w", err)
		}
		return nil

	default:
		return fmt.Errorf("unknown event type: %q", typ)
	}
}

func validatePublicMessage(raw json.RawMessage) error {
	if err := checkDuplicateKeys(raw); err != nil {
		return err
	}
	obj, err := asObject(raw, "public message")
	if err != nil {
		return err
	}
	role, err := requireString(obj, "role")
	if err != nil {
		return err
	}

	switch role {
	case "user":
		if err := requireAndAllow(obj, []string{"role", "content", "timestamp"}, []string{"role", "content", "timestamp"}); err != nil {
			return err
		}
		if err := validateArray(obj["content"], validateUserContent); err != nil {
			return fmt.Errorf("user message content: %w", err)
		}
		if err := validateString(obj["timestamp"]); err != nil {
			return fmt.Errorf("user message timestamp: %w", err)
		}
		return nil

	case "assistant":
		if err := requireAndAllow(obj,
			[]string{"role", "content", "model", "provider", "origin", "usage", "stop_reason", "error_message", "provider_code", "interrupted", "timestamp"},
			[]string{"role", "content", "model", "provider", "origin", "usage", "stop_reason", "error_message", "provider_code", "interrupted", "timestamp"},
		); err != nil {
			return err
		}
		if err := validateArray(obj["content"], validatePublicAssistantContent); err != nil {
			return fmt.Errorf("assistant message content: %w", err)
		}
		if err := validateString(obj["model"]); err != nil {
			return fmt.Errorf("assistant message model: %w", err)
		}
		if err := validateString(obj["provider"]); err != nil {
			return fmt.Errorf("assistant message provider: %w", err)
		}
		if err := validateProviderOrigin(obj["origin"]); err != nil {
			return fmt.Errorf("assistant message origin: %w", err)
		}
		if err := validateUsage(obj["usage"]); err != nil {
			return fmt.Errorf("assistant message usage: %w", err)
		}
		if err := validateEnumString(obj["stop_reason"], stopReasons); err != nil {
			return fmt.Errorf("assistant message stop_reason: %w", err)
		}
		if err := validateStringOrNull(obj["error_message"]); err != nil {
			return fmt.Errorf("assistant message error_message: %w", err)
		}
		if err := validateStringOrNull(obj["provider_code"]); err != nil {
			return fmt.Errorf("assistant message provider_code: %w", err)
		}
		if err := validateBool(obj["interrupted"]); err != nil {
			return fmt.Errorf("assistant message interrupted: %w", err)
		}
		if err := validateString(obj["timestamp"]); err != nil {
			return fmt.Errorf("assistant message timestamp: %w", err)
		}
		return nil

	case "tool_result":
		if err := requireAndAllow(obj, []string{"role", "tool_call_id", "tool_name", "content", "details", "is_error", "timestamp"}, []string{"role", "tool_call_id", "tool_name", "content", "details", "is_error", "timestamp"}); err != nil {
			return err
		}
		if err := validateString(obj["tool_call_id"]); err != nil {
			return fmt.Errorf("tool_result message tool_call_id: %w", err)
		}
		if err := validateString(obj["tool_name"]); err != nil {
			return fmt.Errorf("tool_result message tool_name: %w", err)
		}
		if err := validateArray(obj["content"], validateUserContent); err != nil {
			return fmt.Errorf("tool_result message content: %w", err)
		}
		if !json.Valid(obj["details"]) {
			return fmt.Errorf("tool_result message details: invalid JSON")
		}
		if err := validateBool(obj["is_error"]); err != nil {
			return fmt.Errorf("tool_result message is_error: %w", err)
		}
		if err := validateString(obj["timestamp"]); err != nil {
			return fmt.Errorf("tool_result message timestamp: %w", err)
		}
		return nil

	default:
		return fmt.Errorf("unknown public message role: %q", role)
	}
}

func validatePublicStreamEvent(raw json.RawMessage) error {
	if err := checkDuplicateKeys(raw); err != nil {
		return err
	}
	obj, err := asObject(raw, "public stream event")
	if err != nil {
		return err
	}
	typ, err := requireString(obj, "type")
	if err != nil {
		return err
	}

	base := []string{"type", "content_index"}
	switch typ {
	case "text_start", "thinking_start", "tool_call_start", "reasoning_summary_start":
		if err := requireAndAllow(obj, base, base); err != nil {
			return err
		}
		if err := validateJSONSafeInteger(obj["content_index"]); err != nil {
			return fmt.Errorf("%s.content_index: %w", typ, err)
		}
		return nil

	case "text_delta", "thinking_delta", "tool_call_delta", "reasoning_summary_delta":
		if err := requireAndAllow(obj, []string{"type", "content_index", "delta"}, []string{"type", "content_index", "delta"}); err != nil {
			return err
		}
		if err := validateJSONSafeInteger(obj["content_index"]); err != nil {
			return fmt.Errorf("%s.content_index: %w", typ, err)
		}
		if err := validateString(obj["delta"]); err != nil {
			return fmt.Errorf("%s.delta: %w", typ, err)
		}
		return nil

	case "text_end", "thinking_end", "reasoning_summary_end":
		if err := requireAndAllow(obj, []string{"type", "content_index", "content"}, []string{"type", "content_index", "content"}); err != nil {
			return err
		}
		if err := validateJSONSafeInteger(obj["content_index"]); err != nil {
			return fmt.Errorf("%s.content_index: %w", typ, err)
		}
		if err := validateString(obj["content"]); err != nil {
			return fmt.Errorf("%s.content: %w", typ, err)
		}
		return nil

	case "tool_call_preview":
		if err := requireAndAllow(obj, []string{"type", "content_index", "preview"}, []string{"type", "content_index", "preview"}); err != nil {
			return err
		}
		if err := validateJSONSafeInteger(obj["content_index"]); err != nil {
			return fmt.Errorf("tool_call_preview.content_index: %w", err)
		}
		if !json.Valid(obj["preview"]) {
			return fmt.Errorf("tool_call_preview.preview: invalid JSON")
		}
		return nil

	case "tool_call_end":
		if err := requireAndAllow(obj, []string{"type", "content_index", "tool_call"}, []string{"type", "content_index", "tool_call"}); err != nil {
			return err
		}
		if err := validateJSONSafeInteger(obj["content_index"]); err != nil {
			return fmt.Errorf("tool_call_end.content_index: %w", err)
		}
		if err := validateToolCall(obj["tool_call"]); err != nil {
			return fmt.Errorf("tool_call_end.tool_call: %w", err)
		}
		return nil

	case "tool_call_rejected":
		if err := requireAndAllow(obj, []string{"type", "content_index", "rejected"}, []string{"type", "content_index", "rejected"}); err != nil {
			return err
		}
		if err := validateJSONSafeInteger(obj["content_index"]); err != nil {
			return fmt.Errorf("tool_call_rejected.content_index: %w", err)
		}
		if err := validateRejectedToolCall(obj["rejected"]); err != nil {
			return fmt.Errorf("tool_call_rejected.rejected: %w", err)
		}
		return nil

	default:
		return fmt.Errorf("unknown public stream event type: %q", typ)
	}
}

func validateUserContent(raw json.RawMessage) error {
	if err := checkDuplicateKeys(raw); err != nil {
		return err
	}
	obj, err := asObject(raw, "user content block")
	if err != nil {
		return err
	}
	typ, err := requireString(obj, "type")
	if err != nil {
		return err
	}
	switch typ {
	case "text":
		if err := requireAndAllow(obj, []string{"type", "text"}, []string{"type", "text"}); err != nil {
			return err
		}
		return validateString(obj["text"])
	case "image":
		if err := requireAndAllow(obj, []string{"type", "data", "mime_type"}, []string{"type", "data", "mime_type"}); err != nil {
			return err
		}
		if err := validateString(obj["data"]); err != nil {
			return fmt.Errorf("image data: %w", err)
		}
		if err := validateString(obj["mime_type"]); err != nil {
			return fmt.Errorf("image mime_type: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unknown user content type: %q", typ)
	}
}

func validatePublicAssistantContent(raw json.RawMessage) error {
	if err := checkDuplicateKeys(raw); err != nil {
		return err
	}
	obj, err := asObject(raw, "assistant content block")
	if err != nil {
		return err
	}
	typ, err := requireString(obj, "type")
	if err != nil {
		return err
	}
	switch typ {
	case "text":
		if err := requireAndAllow(obj, []string{"type", "text", "wire_item_index"}, []string{"type", "text", "wire_item_index"}); err != nil {
			return err
		}
		if err := validateString(obj["text"]); err != nil {
			return fmt.Errorf("text content: %w", err)
		}
		if err := validateJSONSafeInteger(obj["wire_item_index"]); err != nil {
			return fmt.Errorf("text wire_item_index: %w", err)
		}
		return nil
	case "thinking":
		if err := requireAndAllow(obj, []string{"type", "thinking", "signature_field", "wire_item_index"}, []string{"type", "thinking", "signature_field", "wire_item_index"}); err != nil {
			return err
		}
		if err := validateString(obj["thinking"]); err != nil {
			return fmt.Errorf("thinking content: %w", err)
		}
		if err := validateString(obj["signature_field"]); err != nil {
			return fmt.Errorf("thinking signature_field: %w", err)
		}
		if err := validateJSONSafeInteger(obj["wire_item_index"]); err != nil {
			return fmt.Errorf("thinking wire_item_index: %w", err)
		}
		return nil
	case "tool_call":
		if err := requireAndAllow(obj, []string{"type", "tool_call", "wire_item_index"}, []string{"type", "tool_call", "wire_item_index"}); err != nil {
			return err
		}
		if err := validateToolCall(obj["tool_call"]); err != nil {
			return fmt.Errorf("tool_call content: %w", err)
		}
		if err := validateJSONSafeInteger(obj["wire_item_index"]); err != nil {
			return fmt.Errorf("tool_call wire_item_index: %w", err)
		}
		return nil
	case "rejected_tool_call":
		if err := requireAndAllow(obj, []string{"type", "rejected", "wire_item_index"}, []string{"type", "rejected", "wire_item_index"}); err != nil {
			return err
		}
		if err := validateRejectedToolCall(obj["rejected"]); err != nil {
			return fmt.Errorf("rejected_tool_call content: %w", err)
		}
		if err := validateJSONSafeInteger(obj["wire_item_index"]); err != nil {
			return fmt.Errorf("rejected_tool_call wire_item_index: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unknown assistant content type: %q", typ)
	}
}

func validateToolCall(raw json.RawMessage) error {
	if err := checkDuplicateKeys(raw); err != nil {
		return err
	}
	obj, err := asObject(raw, "tool call")
	if err != nil {
		return err
	}
	if err := requireAndAllow(obj, []string{"id", "name", "arguments"}, []string{"id", "name", "arguments"}); err != nil {
		return err
	}
	if err := validateString(obj["id"]); err != nil {
		return fmt.Errorf("tool call id: %w", err)
	}
	if err := validateString(obj["name"]); err != nil {
		return fmt.Errorf("tool call name: %w", err)
	}
	if err := validateObjectNotNull(obj["arguments"]); err != nil {
		return fmt.Errorf("tool call arguments: %w", err)
	}
	return nil
}

func validateRejectedToolCall(raw json.RawMessage) error {
	if err := checkDuplicateKeys(raw); err != nil {
		return err
	}
	obj, err := asObject(raw, "rejected tool call")
	if err != nil {
		return err
	}
	if err := requireAndAllow(obj, []string{"id", "name", "error"}, []string{"id", "name", "error"}); err != nil {
		return err
	}
	if err := validateString(obj["id"]); err != nil {
		return fmt.Errorf("rejected tool call id: %w", err)
	}
	if err := validateString(obj["name"]); err != nil {
		return fmt.Errorf("rejected tool call name: %w", err)
	}
	if err := validateEnumString(obj["error"], toolArgumentErrors); err != nil {
		return fmt.Errorf("rejected tool call error: %w", err)
	}
	return nil
}

func validateToolResultPayload(raw json.RawMessage) error {
	if err := checkDuplicateKeys(raw); err != nil {
		return err
	}
	obj, err := asObject(raw, "tool result payload")
	if err != nil {
		return err
	}
	if err := requireAndAllow(obj, []string{"tool_call_id", "tool_name", "content", "details", "is_error", "timestamp"}, []string{"tool_call_id", "tool_name", "content", "details", "is_error", "timestamp"}); err != nil {
		return err
	}
	if err := validateString(obj["tool_call_id"]); err != nil {
		return fmt.Errorf("tool result tool_call_id: %w", err)
	}
	if err := validateString(obj["tool_name"]); err != nil {
		return fmt.Errorf("tool result tool_name: %w", err)
	}
	if err := validateArray(obj["content"], validateUserContent); err != nil {
		return fmt.Errorf("tool result content: %w", err)
	}
	if !json.Valid(obj["details"]) {
		return fmt.Errorf("tool result details: invalid JSON")
	}
	if err := validateBool(obj["is_error"]); err != nil {
		return fmt.Errorf("tool result is_error: %w", err)
	}
	if err := validateString(obj["timestamp"]); err != nil {
		return fmt.Errorf("tool result timestamp: %w", err)
	}
	return nil
}

func validateApprovalRequest(raw json.RawMessage) error {
	if err := checkDuplicateKeys(raw); err != nil {
		return err
	}
	obj, err := asObject(raw, "approval request")
	if err != nil {
		return err
	}
	if err := requireAndAllow(obj, []string{"id", "tool_call_id", "tool_name", "action", "args_summary"}, []string{"id", "tool_call_id", "tool_name", "action", "args_summary", "reason", "audit"}); err != nil {
		return err
	}
	if err := validateString(obj["id"]); err != nil {
		return fmt.Errorf("approval request id: %w", err)
	}
	if err := validateString(obj["tool_call_id"]); err != nil {
		return fmt.Errorf("approval request tool_call_id: %w", err)
	}
	if err := validateString(obj["tool_name"]); err != nil {
		return fmt.Errorf("approval request tool_name: %w", err)
	}
	if err := validateReviewProjection(obj["action"]); err != nil {
		return fmt.Errorf("approval request action: %w", err)
	}
	if !json.Valid(obj["args_summary"]) {
		return fmt.Errorf("approval request args_summary: invalid JSON")
	}
	if reason, ok := obj["reason"]; ok {
		if err := validateStringOrNull(reason); err != nil {
			return fmt.Errorf("approval request reason: %w", err)
		}
	}
	if audit, ok := obj["audit"]; ok {
		if err := validateNullableAuditDecision(audit); err != nil {
			return fmt.Errorf("approval request audit: %w", err)
		}
	}
	return nil
}

func validateReviewProjection(raw json.RawMessage) error {
	if err := checkDuplicateKeys(raw); err != nil {
		return err
	}
	obj, err := asObject(raw, "review projection")
	if err != nil {
		return err
	}
	if err := requireAndAllow(obj, nil, []string{"reviewable", "insufficient_evidence"}); err != nil {
		return err
	}
	hasReviewable := obj["reviewable"] != nil && !bytes.Equal(bytes.TrimSpace(obj["reviewable"]), []byte("null"))
	hasInsufficient := obj["insufficient_evidence"] != nil && !bytes.Equal(bytes.TrimSpace(obj["insufficient_evidence"]), []byte("null"))
	if hasReviewable && hasInsufficient {
		return fmt.Errorf("review projection must contain exactly one of reviewable or insufficient_evidence")
	}
	if !hasReviewable && !hasInsufficient {
		return fmt.Errorf("review projection must contain reviewable or insufficient_evidence")
	}
	if hasInsufficient {
		if err := validateInsufficientEvidence(obj["insufficient_evidence"]); err != nil {
			return err
		}
	}
	if hasReviewable && !json.Valid(obj["reviewable"]) {
		return fmt.Errorf("reviewable: invalid JSON")
	}
	return nil
}

func validateInsufficientEvidence(raw json.RawMessage) error {
	if err := checkDuplicateKeys(raw); err != nil {
		return err
	}
	obj, err := asObject(raw, "insufficient evidence")
	if err != nil {
		return err
	}
	if err := requireAndAllow(obj, []string{"reason"}, []string{"reason"}); err != nil {
		return err
	}
	return validateString(obj["reason"])
}

func validateNullableAuditDecision(raw json.RawMessage) error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}
	return validateAuditDecision(raw)
}

func validateAuditDecision(raw json.RawMessage) error {
	if err := checkDuplicateKeys(raw); err != nil {
		return err
	}
	obj, err := asObject(raw, "audit decision")
	if err != nil {
		return err
	}
	if err := requireAndAllow(obj, []string{"outcome", "risk", "authorization", "rationale"}, []string{"outcome", "risk", "authorization", "rationale"}); err != nil {
		return err
	}
	if err := validateEnumString(obj["outcome"], auditOutcomes); err != nil {
		return fmt.Errorf("audit outcome: %w", err)
	}
	if err := validateEnumString(obj["risk"], riskLevels); err != nil {
		return fmt.Errorf("audit risk: %w", err)
	}
	if err := validateEnumString(obj["authorization"], userAuthorizations); err != nil {
		return fmt.Errorf("audit authorization: %w", err)
	}
	if err := validateString(obj["rationale"]); err != nil {
		return fmt.Errorf("audit rationale: %w", err)
	}
	return nil
}

func validateApprovalResolution(raw json.RawMessage) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) > 0 && trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return err
		}
		if s == "cancelled" {
			return nil
		}
		return fmt.Errorf("approval resolution string must be 'cancelled', got %q", s)
	}
	if err := checkDuplicateKeys(raw); err != nil {
		return err
	}
	obj, err := asObject(raw, "approval resolution")
	if err != nil {
		return err
	}
	if err := requireAndAllow(obj, []string{"decision"}, []string{"decision"}); err != nil {
		return err
	}
	return validateApprovalDecision(obj["decision"])
}

func validateUsage(raw json.RawMessage) error {
	if err := checkDuplicateKeys(raw); err != nil {
		return err
	}
	obj, err := asObject(raw, "usage")
	if err != nil {
		return err
	}
	if err := requireAndAllow(obj, []string{"input", "output", "cache_read", "cache_write", "reasoning", "total_tokens"}, []string{"input", "output", "cache_read", "cache_write", "reasoning", "total_tokens"}); err != nil {
		return err
	}
	for _, key := range []string{"input", "output", "cache_read", "cache_write", "reasoning", "total_tokens"} {
		if err := validateJSONSafeInteger(obj[key]); err != nil {
			return fmt.Errorf("usage.%s: %w", key, err)
		}
	}
	return nil
}

func validateProviderOrigin(raw json.RawMessage) error {
	if err := checkDuplicateKeys(raw); err != nil {
		return err
	}
	obj, err := asObject(raw, "provider origin")
	if err != nil {
		return err
	}
	if err := requireAndAllow(obj, []string{"provider_instance_id", "protocol", "model"}, []string{"provider_instance_id", "protocol", "model"}); err != nil {
		return err
	}
	if err := validateString(obj["provider_instance_id"]); err != nil {
		return fmt.Errorf("provider_instance_id: %w", err)
	}
	if err := validateEnumString(obj["protocol"], apiProtocols); err != nil {
		return fmt.Errorf("protocol: %w", err)
	}
	if err := validateString(obj["model"]); err != nil {
		return fmt.Errorf("model: %w", err)
	}
	return nil
}

func validateNullablePublicMessage(raw json.RawMessage) error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}
	return validatePublicMessage(raw)
}

func validateObjectNotNull(raw json.RawMessage) error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("value must be an object, got null")
	}
	obj, err := asObject(raw, "object")
	if err != nil {
		return err
	}
	if obj == nil {
		return fmt.Errorf("value must be an object")
	}
	return nil
}

func validateArray(raw json.RawMessage, fn func(json.RawMessage) error) error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("array must not be null")
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil {
		return err
	}
	for i, elem := range arr {
		if err := fn(elem); err != nil {
			return fmt.Errorf("element %d: %w", i, err)
		}
	}
	return nil
}

func validateUUID(raw json.RawMessage) error {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return err
	}
	if !canonicalUUIDRegexp.MatchString(s) {
		return fmt.Errorf("%q is not a canonical lower-case UUID", s)
	}
	return nil
}

func validateString(raw json.RawMessage) error {
	var s string
	return json.Unmarshal(raw, &s)
}

func validateStringOrNull(raw json.RawMessage) error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}
	return validateString(raw)
}

func validateBool(raw json.RawMessage) error {
	var b bool
	return json.Unmarshal(raw, &b)
}

func validateJSONSafeInteger(raw json.RawMessage) error {
	var n uint64
	if err := json.Unmarshal(raw, &n); err != nil {
		return err
	}
	if n > maxJSONSafeInteger {
		return fmt.Errorf("integer exceeds JSON-safe range")
	}
	return nil
}

func validateEnumString(raw json.RawMessage, allowed map[string]bool) error {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return err
	}
	if !allowed[s] {
		return fmt.Errorf("%q is not a valid value", s)
	}
	return nil
}

func asObject(raw json.RawMessage, what string) (map[string]json.RawMessage, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, fmt.Errorf("%s must be an object", what)
	}
	return obj, nil
}

func requireString(obj map[string]json.RawMessage, key string) (string, error) {
	raw, ok := obj[key]
	if !ok {
		return "", fmt.Errorf("missing field %q", key)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", fmt.Errorf("field %q: %w", key, err)
	}
	return s, nil
}

func requireAndAllow(obj map[string]json.RawMessage, required, allowed []string) error {
	allowedSet := make(map[string]bool, len(allowed))
	for _, k := range allowed {
		allowedSet[k] = true
	}
	for k := range obj {
		if !allowedSet[k] {
			return fmt.Errorf("unknown field %q", k)
		}
	}
	for _, k := range required {
		if _, ok := obj[k]; !ok {
			return fmt.Errorf("missing field %q", k)
		}
	}
	return nil
}

var (
	steerModes        = map[string]bool{"hard": true, "soft": true}
	stopReasons       = map[string]bool{"stop": true, "length": true, "tool_use": true, "error": true, "aborted": true}
	auditOutcomes     = map[string]bool{"allow": true, "deny": true}
	riskLevels        = map[string]bool{"low": true, "medium": true, "high": true, "critical": true}
	userAuthorizations = map[string]bool{"unknown": true, "low": true, "medium": true, "high": true}
	toolArgumentErrors = map[string]bool{"invalid_json": true, "non_object": true, "schema_violation": true, "incomplete_response": true, "too_large": true}
	apiProtocols      = map[string]bool{"open_ai_chat_completions": true, "open_ai_responses": true, "anthropic_messages": true}
)
