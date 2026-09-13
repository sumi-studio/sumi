package agentstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
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
	if claimed.Chunk.Attempts != 1 {
		t.Fatalf("attempts: %d", claimed.Chunk.Attempts)
	}

	// One preparation branch at a time: a repeated claim (the first
	// response lost) re-claims the same chunk rather than opening a second.
	again, err := s.ClaimMemoryChunk(ctx, pa, gen, 50)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if again.Chunk == nil || again.Chunk.ChunkSeq != 1 || again.Chunk.Attempts != 2 {
		t.Fatalf("second claim should re-claim chunk 1: %+v", again.Chunk)
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
	if c.Status != "sealed" {
		t.Fatalf("stale preparing chunk not resealed: %+v", c)
	}
	// The dead generation's outcome can no longer land.
	if _, err := s.CompleteMemoryChunk(ctx, pa, gen1, 1, "late", false); !errors.Is(err, ErrGenerationFence) {
		t.Fatalf("fenced complete: %v", err)
	}
	// The new generation prepares it instead.
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
// re-claims it instead of stalling preparation until a restart; interrupted
// attempts still spend the budget and end visibly as 'failed'.
func TestMemoryClaimReclaimsOrphanedPreparing(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)

	seedSealed(t, s, pa, gen, 2)
	for attempt := 1; attempt <= memoryChunkMaxAttempts; attempt++ {
		claimed, err := s.ClaimMemoryChunk(ctx, pa, gen, 50)
		if err != nil || claimed.Chunk == nil {
			t.Fatalf("claim %d: %v %+v", attempt, err, claimed)
		}
		if claimed.Chunk.ChunkSeq != 1 || claimed.Chunk.Attempts != attempt ||
			len(claimed.TargetEvents) != 2 {
			t.Fatalf("claim %d: %+v", attempt, claimed.Chunk)
		}
	}
	// The re-claimed chunk can still complete under the same generation.
	st, err := s.MemoryStatus(ctx, pa)
	if err != nil || st.Preparing != 1 {
		t.Fatalf("status: %+v %v", st, err)
	}
	// One more interrupted claim: budget spent → failed, nothing claimable.
	claimed, err := s.ClaimMemoryChunk(ctx, pa, gen, 50)
	if err != nil || claimed.Chunk != nil {
		t.Fatalf("exhausted claim: %v %+v", err, claimed.Chunk)
	}
	c, err := s.chunk(ctx, s.pool, pa, 1)
	if err != nil {
		t.Fatalf("chunk: %v", err)
	}
	if c.Status != "failed" || c.LastError == nil || !strings.Contains(*c.LastError, "did not finish") {
		t.Fatalf("exhausted chunk: %+v", c)
	}
	if !contextSeqs(loadContext(t, s, pa, gen))[1] {
		t.Fatal("failed chunk originals missing from context")
	}
}
