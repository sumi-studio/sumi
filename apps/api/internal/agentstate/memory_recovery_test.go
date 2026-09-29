package agentstate

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func memoryResumeOperation(t *testing.T, s *Store, p string, g int64, mode string) {
	t.Helper()
	ctx := context.Background()
	if _, _, e := s.SubmitInput(ctx, &Input{PersonaID: p, InputID: "resume", Kind: "message", Payload: map[string]any{"text": "resume"}}); e != nil {
		t.Fatal(e)
	}
	if _, e := s.LoadTurn(ctx, p, g, "resume-turn", 5000); e != nil {
		t.Fatal(e)
	}
	req := map[string]any{"chunk_seq": 1, "mode": mode, "additional_rounds": 5}
	mustPlan(t, s, p, "resume-turn", g, PlanCall{CallID: "resume-native", Tool: "memory.resume", Route: "normal", Request: req})
	op, _, fresh, e := s.ClaimOperation(ctx, p, "resume-turn", g, "resume-op", "memory.resume", 0, req)
	if e != nil || !fresh || op.Status != "done" {
		t.Fatalf("resume op %+v %v", op, e)
	}
	before, e := s.readMemoryBranch(ctx, s.pool, p, 1)
	if e != nil {
		t.Fatal(e)
	}
	replay, _, fresh, e := s.ClaimOperation(ctx, p, "resume-turn", g, "resume-replay", "memory.resume", 0, req)
	if e != nil || fresh || replay.OperationID != op.OperationID {
		t.Fatalf("replay %+v %v", replay, e)
	}
	after, e := s.readMemoryBranch(ctx, s.pool, p, 1)
	if e != nil || before.Revision != after.Revision {
		t.Fatal("replay granted work twice", e)
	}
	var calls, results int
	if e = s.pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE kind='tool_call'),count(*) FILTER (WHERE kind='tool_result') FROM core_events WHERE persona_id=$1 AND payload->>'tool'='memory.resume'`, p).Scan(&calls, &results); e != nil || calls != 1 || results != 1 {
		t.Fatalf("atomic experience %d %d %v", calls, results, e)
	}
}
func TestMemoryResumeOperationRetainsPrefixDraftAndIdempotentBudget(t *testing.T) {
	s, p, g, b := branchFixture(t)
	ctx := context.Background()
	n := branchState("draft unchanged")
	n["status"] = "paused"
	n["pause_reason"] = "configured_budget"
	n["retry_at"] = nil
	n["model_calls"] = 32
	n["review"] = map[string]any{"kind": "replace", "version": 1, "sha256": branchHash([]byte("draft unchanged")), "token": "stale", "opened_round": 1}
	if _, e := s.SaveMemoryBranch(ctx, p, g, 1, b.Revision, checkpointRaw(t, n)); e != nil {
		t.Fatal(e)
	}
	memoryResumeOperation(t, s, p, g, "resume")
	resumed, e := s.ClaimMemoryBranch(ctx, p, g, nil)
	if e != nil || resumed == nil {
		t.Fatal("not claimable", e)
	}
	if string(resumed.Snapshot.Tools[0]) != string(b.Snapshot.Tools[0]) {
		t.Fatal("prefix changed")
	}
	var state map[string]any
	if e = json.Unmarshal(resumed.State, &state); e != nil {
		t.Fatal(e)
	}
	if state["review"] != nil || state["candidate"].(map[string]any)["text"] != "draft unchanged" || state["budget_extension"].(map[string]any)["rounds"] != float64(5) {
		t.Fatal(state)
	}
	if !strings.Contains(string(resumed.State), "recovery selected") {
		t.Fatal("no recorded recovery decision")
	}
}
func TestMemoryExplicitRebranchRequiresCurrentMatchingSnapshotAndArchivesDraft(t *testing.T) {
	s, p, g, b := branchFixture(t)
	ctx := context.Background()
	n := branchState("valuable unreviewed draft")
	n["policy"] = map[string]any{"maxRounds": 32, "maxTokens": 100}
	n["status"] = "paused"
	n["pause_reason"] = "private_tools_missing"
	n["retry_at"] = nil
	saved, e := s.SaveMemoryBranch(ctx, p, g, 1, b.Revision, checkpointRaw(t, n))
	if e != nil {
		t.Fatal(e)
	}
	memoryResumeOperation(t, s, p, g, "rebranch")
	if got, e := s.ClaimMemoryBranch(ctx, p, g, nil); e != nil || got != nil {
		t.Fatal("alarm must wait for real parent boundary", e)
	}
	if got, e := s.ClaimMemoryBranch(ctx, p, g, &b.Snapshot); e != nil || got != nil {
		t.Fatal("still missing file.read", e)
	}
	snap := b.Snapshot
	snap.Tools = append(append([]json.RawMessage(nil), snap.Tools...), json.RawMessage(`{"name":"file.read","parameters":{"type":"object"}}`))
	wrong := snap
	wrong.Ranges = append([]BranchSourceRange(nil), snap.Ranges...)
	layer, source := 1, int64(99)
	wrong.Ranges[0].Layer = &layer
	wrong.Ranges[0].ChunkSeq = &source
	if got, e := s.ClaimMemoryBranch(ctx, p, g, &wrong); e != nil || got != nil {
		t.Fatal("wrong source identity accepted", e)
	}
	reboot := NewStore(s.pool)
	got, e := reboot.ClaimMemoryBranch(ctx, p, g, &snap)
	if e != nil || got == nil {
		t.Fatal("matching snapshot did not restart", e)
	}
	if len(got.State) != 0 || len(got.Snapshot.Tools) != 2 || got.Revision <= saved.Revision {
		t.Fatal(got)
	}
	var prior map[string]any
	if e = json.Unmarshal(got.PreviousAttempt, &prior); e != nil {
		t.Fatal(e)
	}
	if prior["candidate"].(map[string]any)["text"] != "valuable unreviewed draft" || prior["restart_budget"].(map[string]any)["rounds"] != float64(5) || prior["restart_budget"].(map[string]any)["tokens"] != float64(90) {
		t.Fatal(prior)
	}
	var history string
	if e = s.pool.QueryRow(ctx, `SELECT previous_attempts FROM core_memory_branches WHERE persona_id=$1 AND chunk_seq=1`, p).Scan(&history); e != nil {
		t.Fatal(e)
	}
	var attempts []MemoryBranch
	if e = json.Unmarshal([]byte(history), &attempts); e != nil || len(attempts) != 1 || string(attempts[0].Snapshot.Tools[0]) != string(b.Snapshot.Tools[0]) {
		t.Fatal(history, e)
	}
	if _, e = s.AcquireWriter(ctx, p, "recovery", time.Minute); e == nil {
		t.Fatal("another live writer unexpectedly acquired")
	}
}

func TestMemoryResumeCannotSilentlyRemoveAnExhaustedTokenCap(t *testing.T) {
	s, p, g, b := branchFixture(t)
	ctx := context.Background()
	n := branchState("saved")
	n["status"] = "paused"
	n["pause_reason"] = "configured_budget"
	n["policy"] = map[string]any{"maxRounds": 32, "maxTokens": 10}
	n["tokens"] = 10
	if _, e := s.SaveMemoryBranch(ctx, p, g, 1, b.Revision, checkpointRaw(t, n)); e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"resume", "rebranch"} {
		tx, e := s.pool.Begin(ctx)
		if e != nil {
			t.Fatal(e)
		}
		_, e = s.resumeMemory(ctx, tx, p, map[string]any{"chunk_seq": 1, "mode": mode, "additional_rounds": 5})
		_ = tx.Rollback(ctx)
		if e == nil || !strings.Contains(e.Error(), "additional_tokens") {
			t.Fatalf("%s erased the token cap: %v", mode, e)
		}
	}
}

// The first tool claim journals its input before Core commits the turn. That
// receipt must retain exactly the recovery references from the delivered event.
func TestMemoryNoticeRecoveryJournalsReferencesAtFirstToolClaim(t *testing.T) {
	s, p, g, b := branchFixture(t)
	ctx := context.Background()
	n := branchState("saved draft")
	n["status"] = "paused"
	n["pause_reason"] = "configured_budget"
	n["model_calls"] = 32
	n["issue"] = map[string]string{"code": "memory_budget_exhausted", "message": "Preparation reached its configured execution budget."}
	if _, err := s.SaveMemoryBranch(ctx, p, g, b.Chunk.ChunkSeq, b.Revision, checkpointRaw(t, n)); err != nil {
		t.Fatal(err)
	}
	load, err := s.LoadTurn(ctx, p, g, "notice-recovery", 5000)
	if err != nil || load.Input == nil || load.Input.Kind != "memory_status" {
		t.Fatalf("notice did not reach intake: %+v %v", load, err)
	}
	// The caller learns the target from the delivered input, not fixture state.
	req := map[string]any{"chunk_seq": load.Input.Payload["chunk_seq"], "mode": "resume", "additional_rounds": 4}
	c := PlanCall{CallID: "resume-from-notice", Tool: "memory.resume", Route: "normal", Request: req}
	mustPlan(t, s, p, load.Turn.TurnID, g, c)
	op, _, fresh, err := s.ClaimOperation(ctx, p, load.Turn.TurnID, g, "notice-op", c.Tool, 0, req)
	if err != nil || !fresh || op.Status != "done" {
		t.Fatalf("recovery from delivered notice: %+v %v", op, err)
	}
	var received map[string]any
	if err := s.pool.QueryRow(ctx, `SELECT payload FROM core_events WHERE persona_id=$1 AND kind='input_received' AND payload->>'input_id'=$2`, p, load.Input.InputID).Scan(&received); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"chunk_seq", "transition", "code", "text"} {
		if received[key] != load.Input.Payload[key] {
			t.Fatalf("first-claim receipt lost %s: %v vs %v", key, received[key], load.Input.Payload[key])
		}
	}
	if received["actor_kind"] != "memory" || received["source_surface"] != "core_memory" || received["attention"] != "observe" {
		t.Fatal("notice became an external message", received)
	}

	// A new state-service instance receives a retry after the receipt was lost.
	// It replays the result and cannot grant the same execution budget twice.
	restarted := NewStore(s.pool)
	replay, _, fresh, err := restarted.ClaimOperation(ctx, p, load.Turn.TurnID, g, "notice-op-replay", c.Tool, 0, req)
	if err != nil || fresh || replay.OperationID != op.OperationID {
		t.Fatalf("lost receipt replay: %+v %v", replay, err)
	}
	resumed, err := restarted.ClaimMemoryBranch(ctx, p, g, nil)
	if err != nil || resumed == nil {
		t.Fatalf("resumed branch unavailable: %+v %v", resumed, err)
	}
	var st map[string]any
	if err := json.Unmarshal(resumed.State, &st); err != nil {
		t.Fatal(err)
	}
	if st["budget_extension"].(map[string]any)["rounds"] != float64(4) {
		t.Fatal("replayed recovery granted duplicate budget", st)
	}
	var notices, posts int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM core_inputs WHERE persona_id=$1 AND kind='memory_status'`, p).Scan(&notices); err != nil {
		t.Fatal(err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM core_outbox WHERE persona_id=$1`, p).Scan(&posts); err != nil {
		t.Fatal(err)
	}
	if notices != 1 || posts != 0 {
		t.Fatalf("recovery duplicated or published a notice: notices=%d posts=%d", notices, posts)
	}
}
