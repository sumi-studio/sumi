package agentevents

import (
	"encoding/json"
	"fmt"
	"testing"
)

func approvalOutcomeFixture(status string) map[string]any {
	return map[string]any{"type": "approval_operation_outcome", "operation_id": "op-1", "tool_call_id": "call-1", "status": status, "executed": status == "succeeded" || status == "failed", "result": map[string]any{"tool_call_id": "call-1", "tool_name": "bash", "provider_call_id": "provider-1", "content": []any{map[string]any{"type": "text", "text": "original result"}}, "details": map[string]any{"exit_code": 0}, "is_error": status != "succeeded", "timestamp": "2026-09-12T00:00:00Z"}}
}
func approvalJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestApprovalOperationOutcomeTerminalStatuses(t *testing.T) {
	for _, status := range []string{"succeeded", "failed", "denied", "expired", "cancelled", "indeterminate"} {
		t.Run(status, func(t *testing.T) {
			event := approvalOutcomeFixture(status)
			if status == "indeterminate" {
				event["executed"] = nil
			}
			if err := validateEvent(approvalJSON(t, event)); err != nil {
				t.Fatal(err)
			}
			if err := ValidateCommand(approvalJSON(t, event)); err == nil {
				t.Fatal("runtime outcome admitted as browser command")
			}
		})
	}
}
func TestApprovalOperationOutcomeRejectsMalformedEvidence(t *testing.T) {
	cases := map[string]func(map[string]any){
		"missing result":           func(v map[string]any) { delete(v, "result") },
		"null executed":            func(v map[string]any) { v["executed"] = nil },
		"unknown field":            func(v map[string]any) { v["decision"] = "approve_once" },
		"unknown status":           func(v map[string]any) { v["status"] = "pending" },
		"executed mismatch":        func(v map[string]any) { v["executed"] = false },
		"empty identity":           func(v map[string]any) { v["operation_id"] = "" },
		"result identity mismatch": func(v map[string]any) { v["result"].(map[string]any)["tool_call_id"] = "other" },
		"result status mismatch":   func(v map[string]any) { v["result"].(map[string]any)["is_error"] = true },
		"extra result role":        func(v map[string]any) { v["result"].(map[string]any)["role"] = "toolResult" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			v := approvalOutcomeFixture("succeeded")
			mutate(v)
			if err := validateEvent(approvalJSON(t, v)); err == nil {
				t.Fatal("malformed evidence accepted")
			}
		})
	}
}
func TestApprovalOperationProvenanceRoundTripAndAuthority(t *testing.T) {
	const pa = "018f47a2-9b3c-7def-8abc-0123456789ab"
	source := approvalOutcomeFixture("denied")
	delete(source, "type")
	source["surface"] = "approval_operation"
	raw := approvalJSON(t, map[string]any{"version": 2, "tenant_id": "tenant-1", "personality_agent_id": pa, "actor": map[string]any{"kind": "personality_agent", "principal_id": pa}, "source": source})
	var value IncomingProvenance
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	if err := value.Validate(); err != nil {
		t.Fatal(err)
	}
	var restored IncomingProvenance
	if err := json.Unmarshal(approvalJSON(t, value), &restored); err != nil {
		t.Fatal(err)
	}
	if !value.Equal(restored) {
		t.Fatal("result lost through provenance roundtrip")
	}
	for _, command := range []string{`{"type":"external_event","content":"done"}`, `{"type":"approval_decision","request_id":"req","decision":{"type":"approve_once"}}`} {
		if err := validateIncomingCommand(value, json.RawMessage(command)); err == nil {
			t.Fatalf("runtime source admitted command %s", command)
		}
	}
	for _, actor := range []string{"human", "foreign"} {
		t.Run(actor, func(t *testing.T) {
			bad := value
			if actor == "human" {
				bad.Actor.Kind = "human"
			} else {
				bad.Actor.PrincipalID = "018f47a2-9b3c-7def-8abc-0123456789ac"
			}
			if bad.Validate() == nil {
				t.Fatal("foreign authority accepted")
			}
		})
	}
	source["event_id"] = "unexpected"
	bad := fmt.Sprintf(`{"version":2,"tenant_id":"tenant-1","personality_agent_id":%q,"actor":{"kind":"personality_agent","principal_id":%q},"source":%s}`, pa, pa, approvalJSON(t, source))
	if json.Unmarshal([]byte(bad), &restored) == nil {
		t.Fatal("extra source field accepted")
	}
}

func TestApprovalOperationEqualityKeepsExactNumbers(t *testing.T) {
	a := &ApprovalOperationOutcome{Result: json.RawMessage(`{"receipt_version":9007199254740992}`)}
	b := &ApprovalOperationOutcome{Result: json.RawMessage(`{"receipt_version":9007199254740993}`)}
	if equalApprovalOperation(a, b) {
		t.Fatal("different receipt numbers compare equal")
	}
	a.Result = json.RawMessage(`{"a":1,"b":2}`)
	b.Result = json.RawMessage(`{"b":2,"a":1}`)
	if !equalApprovalOperation(a, b) {
		t.Fatal("object key order changes identity")
	}
}

func TestApprovalOperationHistoryProjectsOnlyRuntimeResult(t *testing.T) {
	result := approvalOutcomeFixture("succeeded")["result"].(map[string]any)
	result["details"] = map[string]any{"artifact": "artifact://" + artifactOwner + "/tool-output/run-1"}
	source := approvalOutcomeFixture("succeeded")
	delete(source, "type")
	source["surface"], source["result"] = "approval_operation", result
	humanText := "Do not rewrite artifact://" + artifactOther + "/tool-output/user-pasted"
	message := map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": humanText}}, "timestamp": "2026-09-12T00:00:00Z", "incoming_source": map[string]any{"version": 2, "tenant_id": "tenant-1", "personality_agent_id": artifactOwner, "actor": map[string]any{"kind": "personality_agent", "principal_id": artifactOwner}, "source": source}}
	event := map[string]any{"type": "message_end", "message_id": "018f47a2-9b3c-7def-8abc-0123456789ac", "message": message}
	raw := approvalJSON(t, event)
	if err := validateInternalEventArtifactReferences(raw, artifactOwner); err != nil {
		t.Fatal(err)
	}
	projected, err := projectEventArtifactReferences(raw, artifactOwner)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err = json.Unmarshal(projected, &decoded); err != nil {
		t.Fatal(err)
	}
	got := decoded["message"].(map[string]any)
	if got["content"].([]any)[0].(map[string]any)["text"] != humanText {
		t.Fatal("user text rewritten")
	}
	details := got["incoming_source"].(map[string]any)["source"].(map[string]any)["result"].(map[string]any)["details"].(map[string]any)
	if details["artifact"] != "artifact://tool-output/run-1" {
		t.Fatalf("runtime result not projected: %v", details)
	}
	result["details"] = map[string]any{"artifact": "artifact://" + artifactOther + "/tool-output/run-1"}
	if err = validateInternalEventArtifactReferences(approvalJSON(t, event), artifactOwner); err == nil {
		t.Fatal("foreign runtime artifact accepted")
	}
}
