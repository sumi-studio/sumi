package portable

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"strings"
	"testing"
	"time"
)

func TestTransferResumesExactPrivateMemoryBranch(t *testing.T) {
	ctx := context.Background()
	src, dst := newPlacement(t), newPlacement(t)
	id := newID(t)
	must(drop(src.state.EnsurePersona(ctx, id, nil, "continuing secretary")))
	gen := must(src.state.AcquireWriter(ctx, id, "source", time.Minute)).Generation
	for i := 0; i < 2; i++ {
		in, turn := fmt.Sprintf("in-%d", i), fmt.Sprintf("t-%d", i)
		submit(t, src, id, in, "hello")
		must(src.state.LoadTurn(ctx, id, gen, turn, 100))
		must(src.state.CommitTurn(ctx, id, turn, gen, agentstate.CommitRequest{Outcome: "complete", Events: []agentstate.EventInput{{Kind: "input_received", Payload: map[string]any{"input_id": in, "text": "hello", "actor_kind": "human"}}, {Kind: "assistant_message", Payload: map[string]any{"text": strings.Repeat("past ", 9000)}}}, Output: map[string]any{"text": "done"}}))
	}
	must(src.state.MemoryMaintain(ctx, id, gen))
	one, two := 1, 2
	snap := agentstate.BranchSnapshot{Messages: []json.RawMessage{json.RawMessage(`{"role":"system","content":"same"}`), json.RawMessage(`{"role":"user","content":"hello"}`), json.RawMessage(`{"role":"assistant","content":"past"}`)}, Tools: []json.RawMessage{json.RawMessage(`{"name":"file.read","parameters":{"properties":{"z":{"type":"string"},"a":{"type":"string"}}}}`)}, Ranges: []agentstate.BranchSourceRange{{FirstSeq: 1, LastSeq: 1, MessageIndex: &one}, {FirstSeq: 2, LastSeq: 2, MessageIndex: &two}}}
	branch := must(src.state.ClaimMemoryBranch(ctx, id, gen, &snap))
	progress := json.RawMessage(`{"status":"running","messages":[{"role":"assistant","content":"unfinished thinking"}],"rounds":3,"tokens":120,"source_read":[[0,20]],"candidate_read":[],"candidate":null,"review":{"kind":"keep","version":0,"sha256":"source-hash","token":"opened-before-move","opened_round":3},"policy":{"maxRounds":32,"maxTokens":0,"maxConsecutiveFailures":3}}`)
	saved := must(src.state.SaveMemoryBranch(ctx, id, gen, branch.Chunk.ChunkSeq, branch.Revision, progress))
	must(src.svc.Seal(ctx, id, "move-branch", placementID(t, dst)))
	bundle, _ := exportBytes(t, src, id, "move-branch")
	if _, _, e := dst.svc.Import(ctx, bytes.NewReader(bundle), nil, false); e != nil {
		t.Fatal(e)
	}
	must(dst.svc.Activate(ctx, id, "move-branch"))
	dgen := must(dst.state.AcquireWriter(ctx, id, "destination", time.Minute)).Generation
	must(dst.state.Recover(ctx, id, dgen))
	resumed := must(dst.state.ClaimMemoryBranch(ctx, id, dgen, nil))
	if resumed == nil || resumed.Revision != saved.Revision || string(resumed.State) != string(progress) || string(resumed.Snapshot.Tools[0]) != string(snap.Tools[0]) {
		t.Fatalf("private branch changed across transfer: %+v", resumed)
	}
	var done map[string]any
	if e := json.Unmarshal(progress, &done); e != nil {
		t.Fatal(e)
	}
	done["status"] = "kept"
	done["rounds"] = 4
	done["final"] = done["review"]
	raw, _ := json.Marshal(done)
	result := must(dst.state.SaveMemoryBranch(ctx, id, dgen, resumed.Chunk.ChunkSeq, resumed.Revision, raw))
	if result.Chunk.Status != "kept" {
		t.Fatal("review could not finish after transfer")
	}
}
