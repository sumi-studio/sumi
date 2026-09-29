package agentevents

import (
	"context"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"testing"
)

func TestMemoryMechanismInputAndItsTurnAreNotDirectChatPosts(t *testing.T) {
	c := &CoreDirectChat{}
	st := newCoreDirectChatPersona()
	st.turnInput["memory-turn"] = "memory:1:4:occurred"
	events := []agentstate.Event{{Kind: "input_received", TurnID: "memory-turn", Payload: map[string]any{"input_id": "memory:1:4:occurred", "actor_kind": "memory", "source_surface": "core_memory", "text": "checkpoint failed"}}, {Kind: "assistant_message", TurnID: "memory-turn", Payload: map[string]any{"text": "I know compaction is paused"}}, {Kind: "tool_call", TurnID: "memory-turn", Payload: map[string]any{"tool": "journal.note"}}}
	for _, ev := range events {
		out, e := c.translate(context.Background(), "persona", ev, nil, st)
		if e != nil || len(out) != 0 {
			t.Fatalf("memory mechanism leaked into Direct as %s: %s %v", ev.Kind, out, e)
		}
	}
}
