package agentstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// bigText returns a reply body whose stored-JSON estimate is ~10.3k tokens
// (~4 bytes/token over ~41KB), comfortably clearing the 10k chunk minimum.
func bigText() string { return strings.Repeat("x", 41_000) }

// commitEvents appends journal rows the way a committed turn does.
func commitEvents(t *testing.T, s *Store, persona, turnID string, events []EventInput) {
	t.Helper()
	ctx := context.Background()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.appendEventsTx(ctx, tx, persona, turnID, events); err != nil {
		t.Fatalf("append events: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit events: %v", err)
	}
}

// exchange is one conversation exchange: a small input plus a large reply.
func exchange(label, reply string) []EventInput {
	return []EventInput{
		{Kind: "input_received", Payload: map[string]any{
			"input_id": "in-" + label, "kind": "message",
			"payload": map[string]any{"text": "msg " + label}, "actor_kind": "human"}},
		{Kind: "assistant_message", Payload: map[string]any{"text": reply}},
	}
}

// acquireWriter claims the writer lease and returns the live generation.
func acquireWriter(t *testing.T, s *Store, persona string, ttl time.Duration) int64 {
	t.Helper()
	w, err := s.AcquireWriter(context.Background(), persona, "test-writer", ttl)
	if err != nil {
		t.Fatalf("acquire writer: %v", err)
	}
	return w.Generation
}

// toolTurn is one committed multi-round turn: an input, then per round an
// assistant message; every round but the last ends with one tool_call +
// tool_result pair, the way runTurn commits them.
func toolTurn(label string, replies ...string) []EventInput {
	evs := []EventInput{
		{Kind: "input_received", Payload: map[string]any{
			"input_id": "in-" + label, "kind": "message",
			"payload": map[string]any{"text": "msg " + label}, "actor_kind": "human"}},
	}
	for i, reply := range replies {
		evs = append(evs, EventInput{
			Kind: "assistant_message", Payload: map[string]any{"text": reply}})
		if i < len(replies)-1 {
			id := fmt.Sprintf("%s-c%d", label, i)
			evs = append(evs,
				EventInput{Kind: "tool_call", Payload: map[string]any{
					"call_id": id, "tool": "conversation_history",
					"request": map[string]any{"operation": "read"}}},
				EventInput{Kind: "tool_result", Payload: map[string]any{
					"call_id": id, "tool": "conversation_history",
					"response": map[string]any{"ok": true}}})
		}
	}
	return evs
}

// chunkKinds returns the journal kind at seq for boundary assertions.
func chunkKinds(t *testing.T, s *Store, persona string) map[int64]string {
	t.Helper()
	evs, err := s.Events(context.Background(), persona, 0, 1000)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	kinds := map[int64]string{}
	for _, e := range evs {
		kinds[e.Seq] = e.Kind
	}
	return kinds
}

// A committed stretch whose window passes the forced limit seals at a real
// unit boundary: before an assistant_message that does not continue a tool
// flow — here the first reply after an oversized input. The huge record
// seals alone; the deciding text and the call/result flow it starts stay
// one unit.
func TestMemoryForcedSealBeforeLeadingAssistant(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)

	// seq: 1 input(~21k), 2 assistant(~10.3k), 3 call, 4 result,
	// 5 assistant. The only safe interior boundary is before seq 2; every
	// later assistant directly continues a tool flow.
	commitEvents(t, s, pa, "t1", []EventInput{
		{Kind: "input_received", Payload: map[string]any{
			"input_id": "in-a", "kind": "message",
			"payload":    map[string]any{"text": strings.Repeat("z", 85_000)},
			"actor_kind": "human"}},
		{Kind: "assistant_message", Payload: map[string]any{"text": bigText()}},
		{Kind: "tool_call", Payload: map[string]any{
			"call_id": "a-c0", "tool": "conversation_history",
			"request": map[string]any{"operation": "read"}}},
		{Kind: "tool_result", Payload: map[string]any{
			"call_id": "a-c0", "tool": "conversation_history",
			"response": map[string]any{"ok": true}}},
		{Kind: "assistant_message", Payload: map[string]any{"text": "done"}},
	})
	commitEvents(t, s, pa, "t2", exchange("b", "short"))
	if _, err := s.MemoryMaintain(ctx, pa, gen); err != nil {
		t.Fatalf("maintain: %v", err)
	}
	c1, err := s.chunk(ctx, s.pool, pa, 1)
	if err != nil {
		t.Fatalf("chunk 1: %v", err)
	}
	if c1.FirstSeq != 1 || c1.LastSeq != 1 || c1.EstTokens <= L0ForcedSealLimitTokens {
		t.Fatalf("oversized input should seal alone, got %+v", c1)
	}
	c2, err := s.chunk(ctx, s.pool, pa, 2)
	if err != nil {
		t.Fatalf("chunk 2: %v", err)
	}
	if c2.FirstSeq != 2 || c2.LastSeq != 5 {
		t.Fatalf("the tool flow should seal whole, got [%d,%d]",
			c2.FirstSeq, c2.LastSeq)
	}
	kinds := chunkKinds(t, s, pa)
	if kinds[c2.LastSeq+1] != "input_received" {
		t.Fatalf("cut misplaced: next=%s", kinds[c2.LastSeq+1])
	}
}

// A committed turn with tool flow has no safe interior boundary once its
// window passes the forced limit — every continuation assistant follows a
// tool_result, and a cut before a tool_call would split the deciding text
// from its effects. The whole turn seals at the next input boundary.
func TestMemoryForcedSealNeverSplitsToolFlow(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)

	// seq: 1 input, 2 assistant(~10.3k), 3 call, 4 result, 5 assistant(~10.3k),
	// 6 call, 7 result, 8 assistant. No interior boundary exists.
	commitEvents(t, s, pa, "t1", toolTurn("a", bigText(), bigText(), "done"))
	commitEvents(t, s, pa, "t2", exchange("b", "short"))
	if _, err := s.MemoryMaintain(ctx, pa, gen); err != nil {
		t.Fatalf("maintain: %v", err)
	}
	c1, err := s.chunk(ctx, s.pool, pa, 1)
	if err != nil {
		t.Fatalf("chunk 1: %v", err)
	}
	if c1.FirstSeq != 1 || c1.LastSeq != 8 || c1.EstTokens <= L0ForcedSealLimitTokens {
		t.Fatalf("the whole oversized turn should seal at the input boundary, got %+v", c1)
	}
	kinds := chunkKinds(t, s, pa)
	if kinds[c1.LastSeq+1] != "input_received" {
		t.Fatalf("cut misplaced: next=%s", kinds[c1.LastSeq+1])
	}
	// The remainder is below the minimum and stays the live tail.
	if _, err := s.chunk(ctx, s.pool, pa, 2); !errors.Is(err, ErrChunkNotFound) {
		t.Fatalf("unexpected second chunk: %v", err)
	}
}

// When every interior boundary is unsafe — before a tool_result, or before
// an assistant message directly continuing a tool flow — the turn seals
// whole at the next input. The forced limit is a target, not a promise:
// an indivisible flow can exceed it and is never split to satisfy it.
func TestMemoryForcedSealKeepsToolFlowWhole(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)

	// seq: 1 input, 2 assistant(~10.3k), 3 call, 4 result, 5 assistant(~10.3k).
	commitEvents(t, s, pa, "t1", toolTurn("a", bigText(), bigText()))
	commitEvents(t, s, pa, "t2", exchange("b", "short"))
	if _, err := s.MemoryMaintain(ctx, pa, gen); err != nil {
		t.Fatalf("maintain: %v", err)
	}
	c1, err := s.chunk(ctx, s.pool, pa, 1)
	if err != nil {
		t.Fatalf("chunk 1: %v", err)
	}
	if c1.FirstSeq != 1 || c1.LastSeq != 5 || c1.EstTokens <= L0ForcedSealLimitTokens {
		t.Fatalf("whole tool flow should seal past the limit, got %+v", c1)
	}
}

// A single record can itself exceed the forced limit: the chunk seals whole
// at the next input boundary and the record is never split or truncated.
func TestMemoryForcedSealSingleOversizedRecord(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)

	commitEvents(t, s, pa, "t1", []EventInput{
		{Kind: "input_received", Payload: map[string]any{
			"input_id": "in-a", "kind": "message",
			"payload": map[string]any{"text": "msg a"}, "actor_kind": "human"}},
		{Kind: "assistant_message", Payload: map[string]any{
			"text": strings.Repeat("y", 120_000)}},
	})
	commitEvents(t, s, pa, "t2", exchange("b", "short"))
	if _, err := s.MemoryMaintain(ctx, pa, gen); err != nil {
		t.Fatalf("maintain: %v", err)
	}
	c1, err := s.chunk(ctx, s.pool, pa, 1)
	if err != nil {
		t.Fatalf("chunk 1: %v", err)
	}
	if c1.FirstSeq != 1 || c1.LastSeq != 2 || c1.EstTokens <= L0ForcedSealLimitTokens {
		t.Fatalf("oversized record seals whole past the limit, got %+v", c1)
	}
	// The original record is still complete and readable.
	res := loadContext(t, s, pa, gen)
	if !contextSeqs(res)[2] {
		t.Fatal("oversized original missing from context")
	}
}

// seedSealed writes n exchanges then runs maintenance, returning chunk 1.
// Each exchange opens with an input boundary, so a ≥10k window seals as
// soon as the following input lands; the last exchange stays the live tail.
func seedSealed(t *testing.T, s *Store, persona string, gen int64, exchanges int) MemoryChunk {
	t.Helper()
	for i := 0; i < exchanges; i++ {
		commitEvents(t, s, persona, "turn", exchange(fmt.Sprint(i), bigText()))
	}
	st, err := s.MemoryMaintain(context.Background(), persona, gen)
	if err != nil {
		t.Fatalf("maintain: %v", err)
	}
	if st.Sealed < 1 {
		t.Fatalf("expected at least one sealed chunk, got %+v", st)
	}
	c, err := s.chunk(context.Background(), s.pool, persona, 1)
	if err != nil {
		t.Fatalf("chunk 1: %v", err)
	}
	return *c
}

// loadContext returns the rendered context view via LoadTurn with an empty
// input queue — no turn is claimed, only the journal is rendered.
func loadContext(t *testing.T, s *Store, persona string, gen int64) LoadResult {
	t.Helper()
	res, err := s.LoadTurn(context.Background(), persona, gen, "", 100)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return res
}

func contextSeqs(res LoadResult) map[int64]bool {
	seqs := map[int64]bool{}
	for _, e := range res.Context {
		seqs[e.Seq] = true
	}
	return seqs
}

func TestMemorySealClaimComplete(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)

	c := seedSealed(t, s, pa, gen, 2)
	if c.Status != "sealed" || c.FirstSeq != 1 {
		t.Fatalf("chunk 1: %+v", c)
	}
	if c.EstTokens < L0ChunkMinTokens {
		t.Fatalf("chunk est %d below minimum", c.EstTokens)
	}
	// The second exchange's input boundary ended the window: the sealed
	// range stops before that input and the tail stays raw.
	if c.LastSeq != 2 {
		t.Fatalf("expected sealed range [1,2], got [%d,%d]", c.FirstSeq, c.LastSeq)
	}

	claimed, err := s.ClaimMemoryChunk(ctx, pa, gen, 50)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.Chunk == nil {
		t.Fatal("expected a claimed chunk")
	}
	if claimed.Chunk.ChunkSeq != 1 || claimed.Chunk.Status != "preparing" {
		t.Fatalf("claimed chunk: %+v", claimed.Chunk)
	}
	if len(claimed.TargetEvents) != 2 {
		t.Fatalf("target events: %d", len(claimed.TargetEvents))
	}
	if len(claimed.Context.Events) != 4 {
		t.Fatalf("parent context events: %d", len(claimed.Context.Events))
	}
	// A claim spends nothing: attempts count recorded failures only.
	if claimed.Chunk.Attempts != 0 || claimed.Chunk.Interruptions != 0 || claimed.Chunk.ClaimedAt == nil {
		t.Fatalf("fresh claim accounting: %+v", claimed.Chunk)
	}

	// One preparation branch at a time: a repeated claim (the first
	// response lost) counts the orphaned claim as an interruption, paces it,
	// and never opens a second branch or spends an attempt.
	again, err := s.ClaimMemoryChunk(ctx, pa, gen, 50)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if again.Chunk != nil {
		t.Fatalf("orphaned claim must be paced, not re-claimed at once: %+v", again.Chunk)
	}
	c1, err := s.chunk(ctx, s.pool, pa, 1)
	if err != nil || c1.Status != "sealed" || c1.Interruptions != 1 || c1.Attempts != 0 ||
		c1.NotBefore == nil || !c1.NotBefore.After(time.Now()) {
		t.Fatalf("interrupted claim: %+v %v", c1, err)
	}
	if st, err := s.MemoryStatus(ctx, pa); err != nil || st.Claimable != 0 || st.NextClaimableAt == nil {
		t.Fatalf("status while paced: %+v %v", st, err)
	}
	// The recorded deadline is wall-clock; a host clock step can regress
	// now() below it (observed on this WSL2 host), which would be a clock
	// artifact rather than the eligibility contract under test. Push the
	// deadline into the past directly instead of sleeping.
	if _, err := s.pool.Exec(ctx,
		`UPDATE core_memory_chunks SET not_before = '2000-01-01'::timestamptz
		 WHERE persona_id = $1 AND chunk_seq = 1`, pa); err != nil {
		t.Fatal(err)
	}
	again, err = s.ClaimMemoryChunk(ctx, pa, gen, 50)
	if err != nil || again.Chunk == nil || again.Chunk.ChunkSeq != 1 || again.Chunk.Attempts != 0 {
		t.Fatalf("claim after pacing: %+v %v", again.Chunk, err)
	}
	if st, err := s.MemoryStatus(ctx, pa); err != nil || st.Preparing != 1 || st.Sealed != 0 {
		t.Fatalf("status after re-claim: %+v %v", st, err)
	}

	done, err := s.CompleteMemoryChunk(ctx, pa, gen, 1, "L1 replacement text", false)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if done.Status != "prepared" || done.Replacement == nil || *done.Replacement != "L1 replacement text" {
		t.Fatalf("completed chunk: %+v", done)
	}

	// Completion does not apply: originals still render, no block yet.
	res := loadContext(t, s, pa, gen)
	if len(res.Memory) != 0 {
		t.Fatalf("unexpected applied memory: %+v", res.Memory)
	}
	if len(res.Context) != 4 {
		t.Fatalf("context should still hold all raw events, got %d", len(res.Context))
	}
}

func TestMemoryApplyOnlyAboveLiveLimit(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)

	// Three exchanges: chunks [1,2] and [3,4] seal, tail {5,6} stays raw —
	// live ≈ 3×10.3k ≈ 31k, under the 40k limit.
	seedSealed(t, s, pa, gen, 3)
	for seq := int64(1); seq <= 2; seq++ {
		claimed, err := s.ClaimMemoryChunk(ctx, pa, gen, 50)
		if err != nil || claimed.Chunk == nil || claimed.Chunk.ChunkSeq != seq {
			t.Fatalf("claim %d: %v %+v", seq, err, claimed.Chunk)
		}
		if _, err := s.CompleteMemoryChunk(ctx, pa, gen, seq, "short L1", false); err != nil {
			t.Fatalf("complete %d: %v", seq, err)
		}
	}
	st, err := s.MemoryMaintain(ctx, pa, gen)
	if err != nil {
		t.Fatalf("maintain: %v", err)
	}
	if st.LiveRawTokens > L0LiveLimitTokens {
		t.Fatalf("fixture should stay under the limit, live=%d", st.LiveRawTokens)
	}
	// The candidates wait: prepared, not applied, while live ≤ 40k.
	if st.Applied != 0 || st.Prepared != 2 {
		t.Fatalf("candidates must wait below the limit: %+v", st)
	}

	// Two more exchanges push live raw over 40k → both candidates apply
	// (one alone would leave live ≈ 41k, still over).
	commitEvents(t, s, pa, "turn", exchange("x", bigText()))
	commitEvents(t, s, pa, "turn", exchange("y", bigText()))
	st, err = s.MemoryMaintain(ctx, pa, gen)
	if err != nil {
		t.Fatalf("maintain 2: %v", err)
	}
	if st.Applied != 2 {
		t.Fatalf("expected the prepared chunks applied, got %+v", st)
	}
	if st.LiveRawTokens > L0LiveLimitTokens {
		t.Fatalf("maintain should bring live under the limit, live=%d", st.LiveRawTokens)
	}

	res := loadContext(t, s, pa, gen)
	if len(res.Memory) != 2 {
		t.Fatalf("applied blocks: %+v", res.Memory)
	}
	if res.Memory[0].FirstSeq != 1 || res.Memory[0].Text != "short L1" {
		t.Fatalf("block: %+v", res.Memory[0])
	}
	seqs := contextSeqs(res)
	if seqs[1] || seqs[2] || seqs[3] || seqs[4] {
		t.Fatalf("applied originals still render: %v", seqs)
	}
	for seq := int64(5); seq <= 10; seq++ {
		if !seqs[seq] {
			t.Fatalf("unapplied seq %d missing from context: %v", seq, seqs)
		}
	}
	// The canonical journal is untouched — every seq still stored.
	all, err := s.Events(ctx, pa, 0, 500)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if len(all) != 10 {
		t.Fatalf("journal rows: %d", len(all))
	}
}

func TestMemoryEventsDuringPreparationSurvive(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)

	seedSealed(t, s, pa, gen, 2) // chunk 1 covers [1,2]
	claimed, err := s.ClaimMemoryChunk(ctx, pa, gen, 50)
	if err != nil || claimed.Chunk == nil {
		t.Fatalf("claim: %v %+v", err, claimed.Chunk)
	}
	// A correction lands while preparation runs — outside [1,2].
	commitEvents(t, s, pa, "turn", exchange("corr",
		"correction arrived mid-preparation "+bigText()))
	commitEvents(t, s, pa, "turn", exchange("d1", bigText()))
	commitEvents(t, s, pa, "turn", exchange("d2", bigText()))
	commitEvents(t, s, pa, "turn", exchange("d3", bigText()))
	if _, err := s.CompleteMemoryChunk(ctx, pa, gen, 1, "condensed L1", false); err != nil {
		t.Fatalf("complete: %v", err)
	}
	st, err := s.MemoryMaintain(ctx, pa, gen)
	if err != nil {
		t.Fatalf("maintain: %v", err)
	}
	if st.Applied < 1 {
		t.Fatalf("expected chunk 1 applied: %+v", st)
	}
	res := loadContext(t, s, pa, gen)
	found := false
	for _, e := range res.Context {
		if e.Seq == 1 || e.Seq == 2 {
			t.Fatalf("applied originals still render: seq %d", e.Seq)
		}
		if e.Kind == "assistant_message" &&
			strings.Contains(e.Payload["text"].(string), "correction arrived") {
			found = true
		}
	}
	if !found {
		t.Fatal("correction committed during preparation is missing from context")
	}
}

func TestMemoryRecoverResealsStalePreparing(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen1 := acquireWriter(t, s, pa, 50*time.Millisecond)

	seedSealed(t, s, pa, gen1, 2)
	claimed, err := s.ClaimMemoryChunk(ctx, pa, gen1, 50)
	if err != nil || claimed.Chunk == nil {
		t.Fatalf("claim: %v", err)
	}
	// Writer crashes mid-preparation: its lease expires, a new generation
	// acquires, and recovery returns the orphaned claim to the shelf.
	time.Sleep(100 * time.Millisecond)
	gen2 := acquireWriter(t, s, pa, time.Minute)
	if gen2 == gen1 {
		t.Fatal("expected a new generation")
	}
	if _, err := s.Recover(ctx, pa, gen2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	c, err := s.chunk(ctx, s.pool, pa, 1)
	if err != nil {
		t.Fatalf("chunk: %v", err)
	}
	// The lost claim is an interruption, not a failed attempt.
	if c.Status != "sealed" || c.Interruptions != 1 || c.Attempts != 0 || c.ClaimedGeneration != nil {
		t.Fatalf("stale preparing chunk not resealed as an interruption: %+v", c)
	}
	// The dead generation's outcome can no longer land.
	if _, err := s.CompleteMemoryChunk(ctx, pa, gen1, 1, "late", false); !errors.Is(err, ErrGenerationFence) {
		t.Fatalf("fenced complete: %v", err)
	}
	// The new generation prepares it instead, once the short pacing passes.
	// The recorded deadline is wall-clock; a host clock step can regress
	// now() below it (observed on this WSL2 host), so push it into the past
	// directly rather than sleeping on a margin.
	if c.NotBefore == nil {
		t.Fatalf("interrupted chunk lost its pacing deadline: %+v", c)
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE core_memory_chunks SET not_before = '2000-01-01'::timestamptz
		 WHERE persona_id = $1 AND chunk_seq = 1`, pa); err != nil {
		t.Fatal(err)
	}
	re, err := s.ClaimMemoryChunk(ctx, pa, gen2, 50)
	if err != nil || re.Chunk == nil {
		t.Fatalf("reclaim: %v", err)
	}
	if _, err := s.CompleteMemoryChunk(ctx, pa, gen2, 1, "gen2 replacement", false); err != nil {
		t.Fatalf("gen2 complete: %v", err)
	}
}

func TestMemoryFailTerminalKeepsOriginals(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)

	seedSealed(t, s, pa, gen, 2)
	claimed, _ := s.ClaimMemoryChunk(ctx, pa, gen, 50)
	if claimed.Chunk == nil {
		t.Fatal("claim")
	}
	c, err := s.FailMemoryChunk(ctx, pa, gen, 1, "provider rejected", false)
	if err != nil {
		t.Fatalf("fail: %v", err)
	}
	if c.Status != "failed" {
		t.Fatalf("status: %s", c.Status)
	}
	if n, err := s.ClaimMemoryChunk(ctx, pa, gen, 50); err != nil || n.Chunk != nil {
		t.Fatalf("failed chunk must not reprepare: %+v", n.Chunk)
	}
	// Originals still render.
	res := loadContext(t, s, pa, gen)
	if len(res.Context) != 4 {
		t.Fatalf("failed chunk originals missing: %d events", len(res.Context))
	}
}

func TestMemoryRetryBackoffThenExhaust(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)

	seedSealed(t, s, pa, gen, 2)
	claimed, _ := s.ClaimMemoryChunk(ctx, pa, gen, 50)
	if claimed.Chunk == nil {
		t.Fatal("claim")
	}
	c, err := s.FailMemoryChunk(ctx, pa, gen, 1, "transient", true)
	if err != nil {
		t.Fatalf("fail: %v", err)
	}
	if c.Status != "sealed" || c.NotBefore == nil || !c.NotBefore.After(time.Now()) {
		t.Fatalf("retryable failure should reseal with backoff: %+v", c)
	}
	// While backed off it cannot be claimed.
	if n, err := s.ClaimMemoryChunk(ctx, pa, gen, 50); err != nil || n.Chunk != nil {
		t.Fatalf("backed-off chunk claimed: %+v", n.Chunk)
	}
	clearBackoff := func() {
		if _, err := s.pool.Exec(ctx,
			`UPDATE core_memory_chunks SET not_before = NULL WHERE persona_id = $1`, pa); err != nil {
			t.Fatalf("clear backoff: %v", err)
		}
	}
	clearBackoff()
	// Attempts 2 and 3: the third retryable failure exhausts the budget.
	for i := 0; i < memoryChunkMaxAttempts-1; i++ {
		claimed, err := s.ClaimMemoryChunk(ctx, pa, gen, 50)
		if err != nil || claimed.Chunk == nil {
			t.Fatalf("reclaim %d: %v", i, err)
		}
		c, err := s.FailMemoryChunk(ctx, pa, gen, 1, "transient", true)
		if err != nil {
			t.Fatalf("fail %d: %v", i, err)
		}
		if i < memoryChunkMaxAttempts-2 {
			if c.Status != "sealed" {
				t.Fatalf("failed early at attempt %d", c.Attempts)
			}
			clearBackoff()
		}
	}
	c, _ = s.chunk(ctx, s.pool, pa, 1)
	if c.Status != "failed" || c.Attempts != memoryChunkMaxAttempts {
		t.Fatalf("exhausted chunk: %+v", c)
	}
}

// A model layer that cannot produce a request — an unbound selection, a
// missing credential, a binding-lookup outage — is a placement condition,
// not a preparation outcome: the claim returns to the shelf without a
// verdict and without spending attempts or interruptions, so the same
// chunk proceeds once a usable binding exists.
func TestMemoryReshelveKeepsWork(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)

	seedSealed(t, s, pa, gen, 2)
	claimed, _ := s.ClaimMemoryChunk(ctx, pa, gen, 50)
	if claimed.Chunk == nil {
		t.Fatal("claim")
	}
	c, err := s.ReshelveMemoryChunk(ctx, pa, gen, 1, "model: selection needs a destination binding")
	if err != nil {
		t.Fatalf("reshelve: %v", err)
	}
	if c.Status != "sealed" || c.Attempts != 0 || c.Interruptions != 0 {
		t.Fatalf("reshelve spent verdict budget: %+v", c)
	}
	if c.ClaimedGeneration != nil || c.ClaimedAt != nil {
		t.Fatalf("reshelved chunk still claimed: %+v", c)
	}
	if c.LastError == nil || *c.LastError == "" {
		t.Fatal("reshelve should record the reason for visibility")
	}
	if c.NotBefore == nil || !c.NotBefore.After(time.Now()) {
		t.Fatalf("reshelved chunk should be paced: %+v", c)
	}
	// While paced it is not claimable — a persistent unbound window does
	// not spin claim/reshelve inside one tick.
	if n, err := s.ClaimMemoryChunk(ctx, pa, gen, 50); err != nil || n.Chunk != nil {
		t.Fatalf("paced chunk claimed: %+v", n.Chunk)
	}
	// The same work proceeds once a usable binding exists — no verdict,
	// no spent attempt, nothing lost.
	if _, err := s.pool.Exec(ctx,
		`UPDATE core_memory_chunks SET not_before = NULL WHERE persona_id = $1`, pa); err != nil {
		t.Fatalf("clear pacing: %v", err)
	}
	again, err := s.ClaimMemoryChunk(ctx, pa, gen, 50)
	if err != nil || again.Chunk == nil || again.Chunk.ChunkSeq != 1 {
		t.Fatalf("reclaim after reshelve: %+v", again.Chunk)
	}
	if _, err := s.CompleteMemoryChunk(ctx, pa, gen, 1, "prepared text", false); err != nil {
		t.Fatalf("complete after reshelve: %v", err)
	}
	// A reshelve against a chunk that is not this generation's live claim
	// is a conflict — the shelf is never rewritten under the wrong claim.
	if _, err := s.ReshelveMemoryChunk(ctx, pa, gen, 1, "not claimed"); !errors.Is(err, ErrMemoryConflict) {
		t.Fatalf("reshelve of unclaimed chunk: %v, want ErrMemoryConflict", err)
	}
}

func TestMemoryKeepUnchanged(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)

	seedSealed(t, s, pa, gen, 2)
	claimed, _ := s.ClaimMemoryChunk(ctx, pa, gen, 50)
	if claimed.Chunk == nil {
		t.Fatal("claim")
	}
	c, err := s.CompleteMemoryChunk(ctx, pa, gen, 1, "", true)
	if err != nil {
		t.Fatalf("keep_unchanged: %v", err)
	}
	if c.Status != "kept" {
		t.Fatalf("status: %s", c.Status)
	}
	// A kept chunk is never reprepared and never applied, even when the
	// live estimate crosses the limit.
	if n, err := s.ClaimMemoryChunk(ctx, pa, gen, 50); err != nil || n.Chunk != nil {
		t.Fatalf("kept chunk reclaimed: %+v", n.Chunk)
	}
	// Three more exchanges: live raw ≈ 51k crosses the 40k limit while the
	// whole journal still fits the 60k send cap.
	for i := 0; i < 3; i++ {
		commitEvents(t, s, pa, "turn", exchange(fmt.Sprint("k", i), bigText()))
	}
	st, err := s.MemoryMaintain(ctx, pa, gen)
	if err != nil {
		t.Fatalf("maintain: %v", err)
	}
	if st.Applied != 0 {
		t.Fatalf("kept chunk applied: %+v", st)
	}
	res := loadContext(t, s, pa, gen)
	if !contextSeqs(res)[1] {
		t.Fatal("kept originals missing from context")
	}
}

func TestMemoryCompleteReplay(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)

	seedSealed(t, s, pa, gen, 2)
	claimed, _ := s.ClaimMemoryChunk(ctx, pa, gen, 50)
	if claimed.Chunk == nil {
		t.Fatal("claim")
	}
	if _, err := s.CompleteMemoryChunk(ctx, pa, gen, 1, "stable text", false); err != nil {
		t.Fatalf("complete: %v", err)
	}
	// A lost response retried with identical content returns the stored row.
	c, err := s.CompleteMemoryChunk(ctx, pa, gen, 1, "stable text", false)
	if err != nil {
		t.Fatalf("replay complete: %v", err)
	}
	if c.Status != "prepared" {
		t.Fatalf("replay status: %s", c.Status)
	}
	// A different answer for a resolved chunk conflicts.
	if _, err := s.CompleteMemoryChunk(ctx, pa, gen, 1, "different text", false); !errors.Is(err, ErrMemoryConflict) {
		t.Fatalf("conflicting replay: %v", err)
	}
}

func TestConversationHistory(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)

	commitEvents(t, s, pa, "turn", []EventInput{
		{Kind: "input_received", Payload: map[string]any{
			"input_id": "in-a", "kind": "message",
			"payload":    map[string]any{"text": "do you remember the green tea order?"},
			"actor_kind": "human"}},
		{Kind: "assistant_message", Payload: map[string]any{"text": "the order was sencha, two tins"}},
		{Kind: "tool_call", Payload: map[string]any{
			"call_id": "c1", "tool": "schedule.set", "arguments": map[string]any{"note": "green tea reminder"}}},
		{Kind: "tool_result", Payload: map[string]any{
			"call_id": "c1", "tool": "schedule.set", "output": map[string]any{"ok": true}}},
		{Kind: "input_received", Payload: map[string]any{
			"input_id": "in-b", "kind": "message",
			"payload": map[string]any{"text": "next"}, "actor_kind": "human"}},
	})
	// Seal a chunk over the exchange directly — the fixture events are too
	// small to pass the seal minimum, and the test targets the reader, not
	// the sealing walk.
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO core_memory_chunks
			(persona_id, chunk_seq, first_seq, last_seq, est_tokens, status)
		VALUES ($1, 1, 1, 4, 400, 'sealed')`, pa); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Search finds the literal text.
	res, err := s.conversationHistory(ctx, tx, pa, map[string]any{
		"operation": "search", "query": "sencha"})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	msgs := res["messages"].([]map[string]any)
	if len(msgs) != 1 || msgs[0]["source"].(map[string]any)["seq"] != int64(2) {
		t.Fatalf("search hits: %+v", msgs)
	}
	// Search also reaches serialized tool records.
	res, err = s.conversationHistory(ctx, tx, pa, map[string]any{
		"operation": "search", "query": "schedule.set"})
	if err != nil {
		t.Fatalf("search tool: %v", err)
	}
	if len(res["messages"].([]map[string]any)) < 1 {
		t.Fatal("tool record not searchable")
	}
	// Case-sensitive literal: no false positive.
	res, _ = s.conversationHistory(ctx, tx, pa, map[string]any{
		"operation": "search", "query": "SENCHA"})
	if len(res["messages"].([]map[string]any)) != 0 {
		t.Fatal("case-insensitive match — expected literal")
	}

	// Exact read by seq returns the stable serialization.
	res, err = s.conversationHistory(ctx, tx, pa, map[string]any{
		"operation": "read", "seq": float64(2)})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	msgs = res["messages"].([]map[string]any)
	if len(msgs) != 1 || msgs[0]["content_complete"] != true {
		t.Fatalf("read: %+v", msgs)
	}
	if !strings.Contains(string(msgs[0]["event"].(json.RawMessage)), `"kind":"assistant_message"`) {
		t.Fatalf("event json: %v", msgs[0]["event"])
	}

	// Range read pages by limit with after_seq continuation.
	res, err = s.conversationHistory(ctx, tx, pa, map[string]any{
		"operation": "read", "limit": float64(2)})
	if err != nil {
		t.Fatalf("range read: %v", err)
	}
	msgs = res["messages"].([]map[string]any)
	if len(msgs) != 2 || res["has_more"] != true {
		t.Fatalf("page 1: %+v", res)
	}
	res, err = s.conversationHistory(ctx, tx, pa, map[string]any{
		"operation": "read",
		"after_seq": float64(res["next_after_seq"].(int64)),
		"limit":     float64(10)})
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	msgs = res["messages"].([]map[string]any)
	if len(msgs) != 3 || res["has_more"] == true {
		t.Fatalf("page 2: %+v", res)
	}

	// chunk_seq reads the chunk's original records — they stay readable
	// even after application because core_events is never rewritten.
	res, err = s.conversationHistory(ctx, tx, pa, map[string]any{
		"operation": "read", "chunk_seq": float64(1)})
	if err != nil {
		t.Fatalf("chunk read: %v", err)
	}
	if len(res["messages"].([]map[string]any)) != 4 {
		t.Fatalf("chunk read: %+v", res["messages"])
	}
	// A chunk page continues with the same chunk_seq plus after_seq, and the
	// continuation stays inside the chunk's range (seq 5 lies outside it).
	res, err = s.conversationHistory(ctx, tx, pa, map[string]any{
		"operation": "read", "chunk_seq": float64(1), "limit": float64(3)})
	if err != nil {
		t.Fatalf("chunk page 1: %v", err)
	}
	if len(res["messages"].([]map[string]any)) != 3 || res["next_after_seq"] != int64(3) {
		t.Fatalf("chunk page 1: %+v", res)
	}
	res, err = s.conversationHistory(ctx, tx, pa, map[string]any{
		"operation": "read", "chunk_seq": float64(1), "after_seq": float64(3)})
	if err != nil {
		t.Fatalf("chunk page 2: %v", err)
	}
	msgs = res["messages"].([]map[string]any)
	if len(msgs) != 1 || msgs[0]["source"].(map[string]any)["seq"] != int64(4) || res["has_more"] != false {
		t.Fatalf("chunk page 2: %+v", res)
	}
	// A wrong chunk locator is the model's bad request (recorded as the tool
	// result), not a 404 the core would treat as a state outage.
	if _, err := s.conversationHistory(ctx, tx, pa, map[string]any{
		"operation": "read", "chunk_seq": float64(99)}); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("missing chunk: %v", err)
	}

	// Locator validation.
	if _, err := s.conversationHistory(ctx, tx, pa, map[string]any{
		"operation": "read", "seq": float64(1), "chunk_seq": float64(1)}); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("two locators: %v", err)
	}
	if _, err := s.conversationHistory(ctx, tx, pa, map[string]any{
		"operation": "search"}); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("search without query: %v", err)
	}
	if _, err := s.conversationHistory(ctx, tx, pa, map[string]any{
		"operation": "read", "seq": float64(999)}); err == nil {
		t.Fatal("missing seq should error")
	}
}

func TestConversationHistoryOversizedRecord(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)

	commitEvents(t, s, pa, "turn", []EventInput{
		{Kind: "assistant_message", Payload: map[string]any{"text": strings.Repeat("y", 40_000)}},
	})
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// The ~40k-character record exceeds one page: fragments chain through
	// next_read, and following the chain to its end reassembles the stable
	// serialization exactly — no dropped or duplicated characters.
	res, err := s.conversationHistory(ctx, tx, pa, map[string]any{
		"operation": "read", "seq": float64(1)})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var assembled strings.Builder
	pages := 0
	for {
		pages++
		msgs := res["messages"].([]map[string]any)
		if len(msgs) != 1 || msgs[0]["content_complete"] != false {
			t.Fatalf("fragment page %d: %+v", pages, msgs)
		}
		assembled.WriteString(msgs[0]["event_json_fragment"].(string))
		next, ok := res["next_read"].(map[string]any)
		if !ok {
			if msgs[0]["event_json_range"].(map[string]any)["ends_event"] != true {
				t.Fatalf("chain ended before the event: %+v", msgs[0]["event_json_range"])
			}
			break
		}
		if pages > 10 {
			t.Fatal("fragment chain does not terminate")
		}
		res, err = s.conversationHistory(ctx, tx, pa, map[string]any{
			"operation": "read", "seq": float64(1),
			"content_offset": float64(next["content_offset"].(int))})
		if err != nil {
			t.Fatalf("continuation %d: %v", pages, err)
		}
	}
	stored, err := s.Events(ctx, pa, 0, 10)
	if err != nil || len(stored) != 1 {
		t.Fatalf("events: %v %d", err, len(stored))
	}
	want, err := journalEventJSON(stored[0])
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	if pages != 3 || assembled.String() != want {
		t.Fatalf("reassembled %d pages, identical=%v", pages, assembled.String() == want)
	}
	// Out-of-range offset errors.
	if _, err := s.conversationHistory(ctx, tx, pa, map[string]any{
		"operation": "read", "seq": float64(1),
		"content_offset": float64(10_000_000)}); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("bad offset: %v", err)
	}
}

// Raw records stay in the sent context by capacity, not by a record count:
// a long run of small messages renders whole, and only past the send cap do
// the oldest raw records leave — reported as an omitted range that excludes
// applied memory, while every row stays in the journal.
func TestMemoryRenderedContextByCapacity(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)

	for i := 0; i < 50; i++ {
		commitEvents(t, s, pa, "turn", exchange(fmt.Sprint("s", i), "short reply"))
	}
	res, err := s.LoadTurn(ctx, pa, gen, "", 2000)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(res.Context) != 100 || res.Omitted != nil {
		t.Fatalf("small history should render whole: %d events, omitted %+v", len(res.Context), res.Omitted)
	}

	for i := 0; i < 7; i++ {
		commitEvents(t, s, pa, "turn", exchange(fmt.Sprint("b", i), bigText()))
	}
	res, err = s.LoadTurn(ctx, pa, gen, "", 2000)
	if err != nil {
		t.Fatalf("load 2: %v", err)
	}
	var est int64
	for _, e := range res.Context {
		est += estPayloadTokens(e.Kind, e.Payload)
	}
	first, last := res.Context[0].Seq, res.Context[len(res.Context)-1].Seq
	if est > L0SendCapTokens || last != 114 {
		t.Fatalf("raw window est=%d last=%d", est, last)
	}
	if res.Omitted == nil || res.Omitted.FirstSeq != 1 || res.Omitted.LastSeq != first-1 ||
		res.Omitted.Count != first-1 {
		t.Fatalf("omitted range: %+v (first rendered %d)", res.Omitted, first)
	}

	// Applied memory is not "omitted": its block renders instead.
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO core_memory_chunks
			(persona_id, chunk_seq, first_seq, last_seq, est_tokens, status, replacement, replacement_est_tokens)
		VALUES ($1, 1, 1, 10, 100, 'applied', 'earlier small talk', 5)`, pa); err != nil {
		t.Fatalf("insert applied chunk: %v", err)
	}
	res, err = s.LoadTurn(ctx, pa, gen, "", 2000)
	if err != nil {
		t.Fatalf("load 3: %v", err)
	}
	if len(res.Memory) != 1 || res.Omitted == nil || res.Omitted.FirstSeq != 11 ||
		res.Omitted.Count != res.Context[0].Seq-11 {
		t.Fatalf("omitted with applied memory: %+v memory=%d", res.Omitted, len(res.Memory))
	}
}

// A claim whose response was lost, or a branch stopped mid-preparation,
// leaves the chunk 'preparing' under the live generation. The next claim
// counts it as an interruption and paces it instead of stalling preparation
// until a restart. Interruptions never spend the failure budget; their own
// bound ends a host that dies on every claim visibly as 'failed'.
func TestMemoryClaimReclaimsOrphanedPreparing(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)

	seedSealed(t, s, pa, gen, 2)
	clearBackoff := func() {
		if _, err := s.pool.Exec(ctx,
			`UPDATE core_memory_chunks SET not_before = NULL WHERE persona_id = $1`, pa); err != nil {
			t.Fatalf("clear backoff: %v", err)
		}
	}
	claimed, err := s.ClaimMemoryChunk(ctx, pa, gen, 50)
	if err != nil || claimed.Chunk == nil || len(claimed.TargetEvents) != 2 {
		t.Fatalf("first claim: %v %+v", err, claimed)
	}
	for i := 1; i <= memoryChunkMaxInterruptions; i++ {
		// The previous claim never recorded an outcome: the next claim
		// interrupts it and paces the chunk.
		n, err := s.ClaimMemoryChunk(ctx, pa, gen, 50)
		if err != nil || n.Chunk != nil {
			t.Fatalf("claim over orphan %d: %v %+v", i, err, n.Chunk)
		}
		c, err := s.chunk(ctx, s.pool, pa, 1)
		if err != nil || c.Interruptions != i || c.Attempts != 0 {
			t.Fatalf("interruption %d: %+v %v", i, c, err)
		}
		if i == memoryChunkMaxInterruptions {
			break
		}
		if c.Status != "sealed" || c.NotBefore == nil {
			t.Fatalf("interruption %d must reseal with pacing: %+v", i, c)
		}
		clearBackoff()
		re, err := s.ClaimMemoryChunk(ctx, pa, gen, 50)
		if err != nil || re.Chunk == nil || re.Chunk.ChunkSeq != 1 || re.Chunk.Attempts != 0 {
			t.Fatalf("re-claim %d: %v %+v", i, err, re.Chunk)
		}
	}
	c, err := s.chunk(ctx, s.pool, pa, 1)
	if err != nil {
		t.Fatalf("chunk: %v", err)
	}
	if c.Status != "failed" || c.Attempts != 0 || c.LastError == nil ||
		!strings.Contains(*c.LastError, "interrupted") {
		t.Fatalf("exhausted chunk: %+v", c)
	}
	if !contextSeqs(loadContext(t, s, pa, gen))[1] {
		t.Fatal("failed chunk originals missing from context")
	}
}

// --- Upper layers: L1→L2 consolidation and L2-internal reintegration ----
// (docs/agent/memory.md steps 4–5, memory-boundaries-2026-09-08). Chunk rows
// under test are produced by the real seal/claim/complete/apply pipeline on
// PostgreSQL — nothing here writes chunk rows by hand.

// prepareChunk claims chunk seq and shelves replacement text on it.
func prepareChunk(t *testing.T, s *Store, pa string, gen, seq int64, text string) {
	t.Helper()
	cl, err := s.ClaimMemoryChunk(context.Background(), pa, gen, 50)
	if err != nil || cl.Chunk == nil || cl.Chunk.ChunkSeq != seq {
		t.Fatalf("claim for %d: %v %+v", seq, err, cl.Chunk)
	}
	if _, err := s.CompleteMemoryChunk(context.Background(), pa, gen, seq, text, false); err != nil {
		t.Fatalf("complete %d: %v", seq, err)
	}
}

// l1Replacement is ~3.5k estimated tokens — five applied copies (17.5k) sit
// above the 15k L1 limit and below nothing else.
func l1Replacement() string { return strings.Repeat("a", 14_000) }

// An L1 overflow creates a sealed layer-2 target over the oldest contiguous
// applied run; its claim carries the sources' accepted texts (never raw
// events); applying it supersedes the sources in place; and a later L2
// overflow reintegrates the contiguous applied L2 run the same way. Every
// original stays durable and readable through chunk_seq.
func TestMemoryUpperConsolidatesAndReintegrates(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)

	// 8 exchanges → 7 sealed L1 chunks [1,2]..[13,14] + tail [15,16].
	seedSealed(t, s, pa, gen, 8)
	for seq := int64(1); seq <= 7; seq++ {
		prepareChunk(t, s, pa, gen, seq, l1Replacement())
	}
	// Live raw ≈ 82k > 40k: maintain applies prepared chunks while the
	// estimate stays over — c1..c5 (removing ~51.5k) — then, with applied
	// L1 at ~17.5k > 15k, creates the first L2 target.
	st, err := s.MemoryMaintain(ctx, pa, gen)
	if err != nil {
		t.Fatalf("maintain: %v", err)
	}
	if st.Applied != 5 || st.Prepared != 2 || st.Sealed != 1 {
		t.Fatalf("after apply+target: %+v", st)
	}
	target, err := s.chunk(ctx, s.pool, pa, 8)
	if err != nil {
		t.Fatalf("upper target: %v", err)
	}
	// Oldest contiguous run, oldest-first until the remainder fits 11k:
	// c1+c2 consumed (7k) leaves 10.5k applied.
	if target.Layer != 2 || target.Status != "sealed" ||
		target.FirstSeq != 1 || target.LastSeq != 4 ||
		len(target.Sources) != 2 || target.Sources[0] != 1 || target.Sources[1] != 2 {
		t.Fatalf("L2 target shape: %+v", target)
	}
	if target.EstTokens != 2*3_500 {
		t.Fatalf("target est = %d, want the consumed sources' 7000", target.EstTokens)
	}

	// The claim carries the selected fragments' accepted texts with their
	// locators — no raw events.
	cl, err := s.ClaimMemoryChunk(ctx, pa, gen, 50)
	if err != nil || cl.Chunk == nil || cl.Chunk.ChunkSeq != 8 {
		t.Fatalf("upper claim: %v %+v", err, cl.Chunk)
	}
	if len(cl.TargetEvents) != 0 || len(cl.TargetFragments) != 2 {
		t.Fatalf("upper claim input: events=%d fragments=%d",
			len(cl.TargetEvents), len(cl.TargetFragments))
	}
	for i, f := range cl.TargetFragments {
		if f.Layer != 1 || f.ChunkSeq != int64(i+1) || f.Text != l1Replacement() {
			t.Fatalf("fragment %d: %+v", i, f)
		}
	}
	if len(cl.Context.Events) == 0 {
		t.Fatal("upper claim carries the rendered parent context")
	}

	l2text := strings.Repeat("b", 20_000) // ~5k < the 7k consumed
	if _, err := s.CompleteMemoryChunk(ctx, pa, gen, 8, l2text, false); err != nil {
		t.Fatalf("complete L2: %v", err)
	}
	st, err = s.MemoryMaintain(ctx, pa, gen)
	if err != nil {
		t.Fatalf("apply maintain: %v", err)
	}
	// The layer was still over its limit: sources superseded, target applied.
	if st.Applied != 4 || st.Superseded != 2 {
		t.Fatalf("after L2 apply: %+v", st)
	}
	for _, seq := range []int64{1, 2} {
		c, err := s.chunk(ctx, s.pool, pa, seq)
		if err != nil || c.Status != "superseded" {
			t.Fatalf("source %d not superseded: %+v %v", seq, c, err)
		}
	}
	if target, err = s.chunk(ctx, s.pool, pa, 8); err != nil ||
		target.Status != "applied" || target.AppliedAt == nil {
		t.Fatalf("target not applied: %+v %v", target, err)
	}

	// The L2 block renders at its earliest source's position — before the
	// surviving L1 blocks — and the superseded coverage no longer renders raw.
	res := loadContext(t, s, pa, gen)
	if len(res.Memory) != 4 ||
		res.Memory[0].ChunkSeq != 8 || res.Memory[0].Layer != 2 ||
		res.Memory[0].FirstSeq != 1 || res.Memory[0].Text != l2text {
		t.Fatalf("rendered blocks: %+v", res.Memory)
	}
	seqs := contextSeqs(res)
	for seq := int64(1); seq <= 10; seq++ {
		if seqs[seq] {
			t.Fatalf("covered seq %d still renders raw", seq)
		}
	}
	// Prepared c6/c7 and the live tail still render raw.
	for seq := int64(11); seq <= 16; seq++ {
		if !seqs[seq] {
			t.Fatalf("uncovered seq %d missing from context", seq)
		}
	}
	// Source reread: a superseded chunk_seq still opens its raw originals —
	// big records page at one per read, so follow after_seq to the range end.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if got := readChunkSeqs(t, s, tx, pa, 1); fmt.Sprint(got) != "[1 2]" {
		t.Fatalf("superseded chunk 1 originals unreadable: %v", got)
	}

	// --- L2-internal reintegration -------------------------------------
	// Push live raw back over 40k so the shelved L1 candidates apply and a
	// second L2 target forms over the next contiguous run.
	for i := 0; i < 3; i++ {
		commitEvents(t, s, pa, "turn", exchange(fmt.Sprint("u", i), bigText()))
	}
	if _, err := s.MemoryMaintain(ctx, pa, gen); err != nil {
		t.Fatalf("maintain u: %v", err)
	}
	// New chunks 9,10,11 sealed; c6,c7 applied; target 12 over sources 3,4.
	target12, err := s.chunk(ctx, s.pool, pa, 12)
	if err != nil {
		t.Fatalf("second target: %v", err)
	}
	if target12.Layer != 2 || target12.FirstSeq != 5 || target12.LastSeq != 8 ||
		len(target12.Sources) != 2 || target12.Sources[0] != 3 || target12.Sources[1] != 4 {
		t.Fatalf("second L2 target: %+v", target12)
	}
	l2text2 := strings.Repeat("c", 22_000) // ~5.5k < 7k consumed
	prepareChunk(t, s, pa, gen, 9, l1Replacement())
	prepareChunk(t, s, pa, gen, 10, l1Replacement())
	prepareChunk(t, s, pa, gen, 11, l1Replacement())
	prepareChunk(t, s, pa, gen, 12, l2text2)
	if _, err := s.MemoryMaintain(ctx, pa, gen); err != nil {
		t.Fatalf("apply 12: %v", err)
	}
	// Applied L2 = 5k + 5.5k = 10.5k > 10k: the same pass creates the
	// reintegration target over the whole contiguous applied L2 run —
	// chunk 8 [1,4] + chunk 12 [5,8] tile [1,8].
	target13, err := s.chunk(ctx, s.pool, pa, 13)
	if err != nil {
		t.Fatalf("reintegration target: %v", err)
	}
	if target13.Layer != 2 || target13.Status != "sealed" ||
		target13.FirstSeq != 1 || target13.LastSeq != 8 ||
		len(target13.Sources) != 2 || target13.Sources[0] != 8 || target13.Sources[1] != 12 {
		t.Fatalf("reintegration target: %+v", target13)
	}
	// Its claim carries the L2 fragments' texts — reintegration may
	// rearrange within them but imports nothing else.
	cl, err = s.ClaimMemoryChunk(ctx, pa, gen, 50)
	if err != nil || cl.Chunk == nil || cl.Chunk.ChunkSeq != 13 {
		t.Fatalf("reintegration claim: %v %+v", err, cl.Chunk)
	}
	if len(cl.TargetFragments) != 2 || len(cl.TargetEvents) != 0 {
		t.Fatalf("reintegration input: %+v", cl.TargetFragments)
	}
	if cl.TargetFragments[0].Layer != 2 || cl.TargetFragments[0].Text != l2text ||
		cl.TargetFragments[1].Text != l2text2 {
		t.Fatalf("reintegration fragments: %+v", cl.TargetFragments)
	}
	if _, err := s.CompleteMemoryChunk(ctx, pa, gen, 13, "integrated memory", false); err != nil {
		t.Fatalf("complete 13: %v", err)
	}
	st, err = s.MemoryMaintain(ctx, pa, gen)
	if err != nil {
		t.Fatalf("apply 13: %v", err)
	}
	if st.Superseded != 6 || st.Applied != 5 {
		t.Fatalf("after reintegration: %+v", st)
	}
	for _, seq := range []int64{8, 12} {
		c, err := s.chunk(ctx, s.pool, pa, seq)
		if err != nil || c.Status != "superseded" {
			t.Fatalf("L2 source %d not superseded: %+v %v", seq, c, err)
		}
	}
	res = loadContext(t, s, pa, gen)
	if len(res.Memory) == 0 || res.Memory[0].ChunkSeq != 13 ||
		res.Memory[0].FirstSeq != 1 || res.Memory[0].Text != "integrated memory" {
		t.Fatalf("reintegrated block: %+v", res.Memory)
	}
	for seq := int64(1); seq <= 8; seq++ {
		if seqs = contextSeqs(res); seqs[seq] {
			t.Fatalf("reintegrated coverage seq %d renders raw", seq)
		}
	}
	// The whole chain stayed durable: 13 chunk rows, originals readable —
	// the reintegrated L2 source rereads its full four-record coverage.
	if got := readChunkSeqs(t, s, tx, pa, 8); fmt.Sprint(got) != "[1 2 3 4]" {
		t.Fatalf("superseded L2 originals unreadable: %v", got)
	}
}

// readChunkSeqs pages a chunk_seq read to the end of the chunk's range and
// returns the distinct record seqs it returned, in order. Each record is a
// bigText event (~41KB serialized): the 16KB page budget fragments a record
// through next_read (seq + content_offset) and resumes the chunk query at
// resume_after_seq, so a record's seq can appear in consecutive messages.
func readChunkSeqs(t *testing.T, s *Store, tx pgx.Tx, persona string, chunkSeq int64) []int64 {
	t.Helper()
	var seqs []int64
	appendSeq := func(m map[string]any) {
		seq := m["source"].(map[string]any)["seq"].(int64)
		if len(seqs) == 0 || seqs[len(seqs)-1] != seq {
			seqs = append(seqs, seq)
		}
	}
	asInt := func(v any) int64 {
		switch n := v.(type) {
		case int64:
			return n
		case int:
			return int64(n)
		default:
			t.Fatalf("reread cursor %v (%T) is not an integer", v, v)
			return 0
		}
	}
	req := map[string]any{"operation": "read", "chunk_seq": float64(chunkSeq), "limit": float64(20)}
	for i := 0; i < 40; i++ {
		res, err := s.conversationHistory(context.Background(), tx, persona, req)
		if err != nil {
			t.Fatalf("chunk %d reread: %v", chunkSeq, err)
		}
		var resumeAfter int64
		for _, m := range res["messages"].([]map[string]any) {
			appendSeq(m)
			if r, ok := m["resume_after_seq"].(int64); ok {
				resumeAfter = r
			}
		}
		if next, ok := res["next_read"].(map[string]any); ok {
			// Mid-record fragment: continue inside the same record.
			req = map[string]any{"operation": "read",
				"seq":            float64(asInt(next["seq"])),
				"content_offset": float64(asInt(next["content_offset"]))}
			continue
		}
		if res["next_after_seq"] != nil {
			req = map[string]any{"operation": "read",
				"chunk_seq": float64(chunkSeq),
				"after_seq": float64(asInt(res["next_after_seq"])), "limit": float64(20)}
			continue
		}
		if resumeAfter != 0 {
			// The fragmented record ended; resume the chunk query past it.
			req = map[string]any{"operation": "read",
				"chunk_seq": float64(chunkSeq),
				"after_seq": float64(resumeAfter), "limit": float64(20)}
			continue
		}
		return seqs
	}
	t.Fatalf("chunk %d reread did not terminate", chunkSeq)
	return nil
}

// A KEEP_UNCHANGED (or exhausted failure) verdict settles its exact source
// tuple: the same selection is never resealed, while a different grouping
// of the remaining shelf still proceeds.
func TestMemoryUpperSettledTupleNotResealed(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)

	seedSealed(t, s, pa, gen, 8)
	for seq := int64(1); seq <= 7; seq++ {
		prepareChunk(t, s, pa, gen, seq, l1Replacement())
	}
	if _, err := s.MemoryMaintain(ctx, pa, gen); err != nil {
		t.Fatalf("maintain: %v", err)
	}
	// Target 8 covers sources [1,2]; the model keeps them.
	cl, err := s.ClaimMemoryChunk(ctx, pa, gen, 50)
	if err != nil || cl.Chunk == nil || cl.Chunk.ChunkSeq != 8 {
		t.Fatalf("claim 8: %v %+v", err, cl.Chunk)
	}
	if _, err := s.CompleteMemoryChunk(ctx, pa, gen, 8, "", true); err != nil {
		t.Fatalf("keep: %v", err)
	}
	st, err := s.MemoryMaintain(ctx, pa, gen)
	if err != nil {
		t.Fatalf("maintain after keep: %v", err)
	}
	// The kept tuple is not resealed — but the next contiguous group is a
	// different tuple and proceeds: target 9 over sources [3,4].
	c8, err := s.chunk(ctx, s.pool, pa, 8)
	if err != nil || c8.Status != "kept" {
		t.Fatalf("kept target: %+v %v", c8, err)
	}
	c9, err := s.chunk(ctx, s.pool, pa, 9)
	if err != nil {
		t.Fatalf("next target: %v", err)
	}
	if c9.Layer != 2 || c9.Status != "sealed" ||
		len(c9.Sources) != 2 || c9.Sources[0] != 3 || c9.Sources[1] != 4 ||
		c9.FirstSeq != 5 || c9.LastSeq != 8 {
		t.Fatalf("regrouped target: %+v", c9)
	}
	if st.Sealed != 1 {
		t.Fatalf("one in-flight upper target: %+v", st)
	}

	// A terminally failed target settles the same way: the identical tuple
	// is never resealed (a fresh attempt budget on the same sources would
	// loop forever), while originals stay applied and rendering.
	cl, err = s.ClaimMemoryChunk(ctx, pa, gen, 50)
	if err != nil || cl.Chunk == nil || cl.Chunk.ChunkSeq != 9 {
		t.Fatalf("claim 9: %v %+v", err, cl.Chunk)
	}
	if _, err := s.FailMemoryChunk(ctx, pa, gen, 9, "provider refused", false); err != nil {
		t.Fatalf("fail 9: %v", err)
	}
	if _, err := s.MemoryMaintain(ctx, pa, gen); err != nil {
		t.Fatalf("maintain after fail: %v", err)
	}
	c9, _ = s.chunk(ctx, s.pool, pa, 9)
	if c9.Status != "failed" {
		t.Fatalf("failed target: %+v", c9)
	}
	// No new target reselects [1,2] or [3,4]; the remainder of that applied
	// run is chunk 5 alone (6,7 are still shelved candidates). The reference
	// sets no minimum group size, so the single-source tuple [5] proceeds —
	// it is a different grouping, not a relitigated verdict.
	var next MemoryChunk
	found := false
	rows, err := s.pool.Query(ctx, `
		SELECT `+chunkCols+` FROM core_memory_chunks
		WHERE persona_id = $1 AND layer = 2 AND status = 'sealed'`, pa)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	for rows.Next() {
		c, err := scanChunk(rows)
		if err != nil {
			rows.Close()
			t.Fatalf("scan: %v", err)
		}
		if found {
			t.Fatal("more than one sealed upper target")
		}
		next, found = c, true
	}
	rows.Close()
	if !found || len(next.Sources) != 1 || next.Sources[0] != 5 ||
		next.FirstSeq != 9 || next.LastSeq != 10 {
		t.Fatalf("next grouping: %+v", next)
	}
	for _, seq := range []int64{1, 2, 3, 4} {
		c, err := s.chunk(ctx, s.pool, pa, seq)
		if err != nil || c.Status != "applied" {
			t.Fatalf("settled source %d lost its originals' replacement: %+v %v", seq, c, err)
		}
	}
}

// A prepared upper target applies only while its layer is still over the
// limit: if the layer has settled under it, the candidate waits shelved and
// the sources are never superseded.
func TestMemoryUpperApplyGateShelves(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)

	seedSealed(t, s, pa, gen, 8)
	for seq := int64(1); seq <= 7; seq++ {
		prepareChunk(t, s, pa, gen, seq, l1Replacement())
	}
	if _, err := s.MemoryMaintain(ctx, pa, gen); err != nil {
		t.Fatalf("maintain: %v", err)
	}
	prepareChunk(t, s, pa, gen, 8, strings.Repeat("b", 20_000))
	// The layer settles under its limit before the candidate can apply:
	// shrink the unselected applied L1 rows' estimates so applied L1 totals
	// ~10k < 15k (a direct fixture of the gate condition, as a correction
	// pass could leave it).
	if _, err := s.pool.Exec(ctx, `
		UPDATE core_memory_chunks SET replacement_est_tokens = 2000
		WHERE persona_id = $1 AND chunk_seq IN (3, 4, 5)`, pa); err != nil {
		t.Fatalf("settle layer: %v", err)
	}
	st, err := s.MemoryMaintain(ctx, pa, gen)
	if err != nil {
		t.Fatalf("maintain shelved: %v", err)
	}
	c8, err := s.chunk(ctx, s.pool, pa, 8)
	if err != nil || c8.Status != "prepared" {
		t.Fatalf("shelved target: %+v %v", c8, err)
	}
	if st.Superseded != 0 {
		t.Fatalf("nothing may be superseded while shelved: %+v", st)
	}
	for _, seq := range []int64{1, 2} {
		c, _ := s.chunk(ctx, s.pool, pa, seq)
		if c.Status != "applied" {
			t.Fatalf("source %d touched while shelved: %+v", seq, c)
		}
	}
	// When the layer is over the limit again the same candidate applies.
	if _, err := s.pool.Exec(ctx, `
		UPDATE core_memory_chunks SET replacement_est_tokens = 3500
		WHERE persona_id = $1 AND chunk_seq IN (3, 4, 5)`, pa); err != nil {
		t.Fatalf("restore layer: %v", err)
	}
	if _, err := s.MemoryMaintain(ctx, pa, gen); err != nil {
		t.Fatalf("maintain apply: %v", err)
	}
	c8, _ = s.chunk(ctx, s.pool, pa, 8)
	if c8.Status != "applied" {
		t.Fatalf("target not applied once the gate holds: %+v", c8)
	}
}

// An upper target whose sources are no longer all applied is stale — a
// state the one-in-flight rule cannot produce but a transferred or crafted
// row can. The claim fails it honestly: no attempts spent, sources
// untouched, no replacement prepared from a different source set.
func TestMemoryUpperStaleTargetFailsHonestly(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)

	seedSealed(t, s, pa, gen, 8)
	for seq := int64(1); seq <= 7; seq++ {
		prepareChunk(t, s, pa, gen, seq, l1Replacement())
	}
	if _, err := s.MemoryMaintain(ctx, pa, gen); err != nil {
		t.Fatalf("maintain: %v", err)
	}
	// Simulate a carried/tampered row: source 1 is no longer applied.
	if _, err := s.pool.Exec(ctx, `
		UPDATE core_memory_chunks SET status = 'superseded'
		WHERE persona_id = $1 AND chunk_seq = 1`, pa); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	cl, err := s.ClaimMemoryChunk(ctx, pa, gen, 50)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if cl.Chunk != nil {
		t.Fatalf("stale target claimed for preparation: %+v", cl.Chunk)
	}
	c8, err := s.chunk(ctx, s.pool, pa, 8)
	if err != nil || c8.Status != "failed" || c8.Attempts != 0 ||
		c8.LastError == nil || !strings.Contains(*c8.LastError, "stale") {
		t.Fatalf("stale target: %+v %v", c8, err)
	}
	c1, _ := s.chunk(ctx, s.pool, pa, 1)
	if c1.Status != "superseded" {
		t.Fatalf("source row touched by the honest failure: %+v", c1)
	}
}

// Upper-layer claims fence and interrupt like layer-1 ones: a dead
// generation's preparing target returns to the shelf as an interruption,
// its outcome can never land, and the live generation reprepares it.
func TestMemoryUpperClaimFencingAndInterruption(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen1 := acquireWriter(t, s, pa, 50*time.Millisecond)

	seedSealed(t, s, pa, gen1, 8)
	for seq := int64(1); seq <= 7; seq++ {
		prepareChunk(t, s, pa, gen1, seq, l1Replacement())
	}
	if _, err := s.MemoryMaintain(ctx, pa, gen1); err != nil {
		t.Fatalf("maintain: %v", err)
	}
	cl, err := s.ClaimMemoryChunk(ctx, pa, gen1, 50)
	if err != nil || cl.Chunk == nil || cl.Chunk.ChunkSeq != 8 {
		t.Fatalf("claim 8: %v %+v", err, cl.Chunk)
	}
	// The writer dies mid-preparation; a new generation recovers.
	time.Sleep(100 * time.Millisecond)
	gen2 := acquireWriter(t, s, pa, time.Minute)
	if _, err := s.Recover(ctx, pa, gen2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	c8, err := s.chunk(ctx, s.pool, pa, 8)
	if err != nil || c8.Status != "sealed" || c8.Interruptions != 1 || c8.Attempts != 0 {
		t.Fatalf("interrupted upper claim: %+v %v", c8, err)
	}
	if _, err := s.CompleteMemoryChunk(ctx, pa, gen1, 8, "late answer", false); !errors.Is(err, ErrGenerationFence) {
		t.Fatalf("fenced upper complete: %v", err)
	}
	// The interruption's deadline is wall-clock; a host clock step can
	// regress now() below it (observed on this WSL2 host), so push it into
	// the past directly rather than sleeping on a margin.
	if c8.NotBefore == nil {
		t.Fatalf("interrupted upper chunk lost its pacing deadline: %+v", c8)
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE core_memory_chunks SET not_before = '2000-01-01'::timestamptz
		 WHERE persona_id = $1 AND chunk_seq = 8`, pa); err != nil {
		t.Fatal(err)
	}
	cl, err = s.ClaimMemoryChunk(ctx, pa, gen2, 50)
	if err != nil || cl.Chunk == nil || cl.Chunk.ChunkSeq != 8 {
		t.Fatalf("reclaim 8: %v %+v", err, cl.Chunk)
	}
	if len(cl.TargetFragments) != 2 {
		t.Fatalf("reclaimed fragments: %+v", cl.TargetFragments)
	}
	if _, err := s.CompleteMemoryChunk(ctx, pa, gen2, 8, "gen2 L2 text", false); err != nil {
		t.Fatalf("gen2 complete: %v", err)
	}
}
