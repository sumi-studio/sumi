package agentstate

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestSkillReadBundledGuides(t *testing.T) {
	if !slices.Contains(NewStore(nil).ClaimableTools(), "skill.read") {
		t.Fatal("skill reader is not discoverable")
	}
	for _, name := range []string{"messaging", "calls", "terminal-files"} {
		t.Run(name, func(t *testing.T) {
			guide, err := readSkill(map[string]any{"name": name})
			if err != nil {
				t.Fatal(err)
			}
			body, ok := guide["content"].(string)
			if !ok || !strings.HasPrefix(body, "# ") || len(body) > 16*1024 {
				t.Fatalf("missing or unbounded guide: %+v", guide)
			}
			if guide["name"] != name || guide["media_type"] != "text/markdown" ||
				guide["revision"] != fmt.Sprintf("%x", sha256.Sum256([]byte(body))) {
				t.Fatalf("wrong guide identity: %+v", guide)
			}
		})
	}
	for _, request := range []map[string]any{
		{}, {"name": 1}, {"name": "unknown"}, {"name": "../store.go"},
		{"name": "/etc/passwd"}, {"name": "messaging", "path": "/etc/passwd"},
	} {
		if _, err := readSkill(request); !errors.Is(err, ErrBadRequest) {
			t.Fatalf("accepted non-catalog read %#v: %v", request, err)
		}
	}
}

func TestSkillReadReceiptSurvivesServiceRestart(t *testing.T) {
	call := PlanCall{CallID: "read-guide", Tool: "skill.read", Route: "normal", Request: map[string]any{"name": "messaging"}}
	f := newApprovalFixture(t, call)
	op, approval, fresh := f.claim(t, "t-1", 0, call)
	if !fresh || approval != nil || op.Status != "done" || op.Response["name"] != "messaging" {
		t.Fatalf("read did not complete: op=%+v approval=%+v fresh=%v", op, approval, fresh)
	}
	if !f.s.operationReadOnly(call.Tool, call.Request) {
		t.Fatal("reading a guide counted as an outward effect")
	}
	// A new service instance only has the database; the saved receipt must
	// be returned instead of performing another read for the same plan call.
	f.s = NewStore(f.pool)
	replayed, approval, fresh := f.claim(t, "t-1", 0, call)
	if fresh || approval != nil || replayed.OperationID != op.OperationID || !reflect.DeepEqual(replayed.Response, op.Response) {
		t.Fatalf("read was not replayed: %+v fresh=%v", replayed, fresh)
	}
	var count int
	if err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM core_events
		WHERE persona_id=$1 AND kind='tool_result' AND payload->>'tool'='skill.read'`, f.pa).Scan(&count); err != nil || count != 1 {
		t.Fatalf("guide read not journaled exactly once: count=%d err=%v", count, err)
	}
}

func TestUnknownSkillCannotCreateAnApproval(t *testing.T) {
	call := PlanCall{Tool: "skill.read", Route: "elevated", Request: map[string]any{"name": "not-installed"}}
	f := newApprovalFixture(t, call)
	op, approval, fresh := f.claim(t, "t-1", 0, call)
	if !fresh || approval != nil || op.Status != "failed" || !strings.Contains(fmt.Sprint(op.Response["error"]), "unknown skill") {
		t.Fatalf("unknown guide did not produce a tool error: op=%+v approval=%+v", op, approval)
	}
	if f.outboxCount(t, "approval_requested") != 0 {
		t.Fatal("an unexecutable read requested human approval")
	}
}
