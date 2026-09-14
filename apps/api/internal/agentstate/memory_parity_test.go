package agentstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// A replacement that is not smaller than its range is kept visible but never
// applied: the originals stay the sent context.
func TestMemoryNonShrinkingReplacementKept(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)
	c := seedSealed(t, s, pa, gen, 2)
	claimed, err := s.ClaimMemoryChunk(ctx, pa, gen, 100)
	if err != nil || claimed.Chunk == nil || claimed.Chunk.ChunkSeq != c.ChunkSeq {
		t.Fatalf("claim: %+v err=%v", claimed, err)
	}
	big := strings.Repeat("z", int(c.EstTokens)*4+400)
	got, err := s.CompleteMemoryChunk(ctx, pa, gen, c.ChunkSeq, big, false)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if got.Status != "kept" || got.Replacement == nil || *got.Replacement != big ||
		got.LastError == nil || !strings.Contains(*got.LastError, "did not shrink") {
		t.Fatalf("non-shrinking replacement: status=%s err=%v", got.Status, got.LastError)
	}
	if _, err := s.CompleteMemoryChunk(ctx, pa, gen, c.ChunkSeq, big, false); err != nil {
		t.Fatalf("identical replay: %v", err)
	}
	if _, err := s.CompleteMemoryChunk(ctx, pa, gen, c.ChunkSeq, "short", false); !errors.Is(err, ErrMemoryConflict) {
		t.Fatalf("different replay: %v", err)
	}
	if _, err := s.CompleteMemoryChunk(ctx, pa, gen, c.ChunkSeq, "", true); !errors.Is(err, ErrMemoryConflict) {
		t.Fatalf("keep replay over a stored replacement: %v", err)
	}
	for i := 0; i < 4; i++ {
		commitEvents(t, s, pa, "turn", exchange(fmt.Sprint("more", i), bigText()))
	}
	st, err := s.MemoryMaintain(ctx, pa, gen)
	if err != nil || st.Applied != 0 || st.LiveRawTokens <= L0LiveLimitTokens {
		t.Fatalf("maintain over the limit: %+v err=%v", st, err)
	}
	after, err := s.chunk(ctx, s.pool, pa, c.ChunkSeq)
	if err != nil || after.Status != "kept" {
		t.Fatalf("chunk after maintain: %+v err=%v", after, err)
	}
	// Only the raw send cap may leave the originals out — never the kept
	// replacement.
	if res := loadContext(t, s, pa, gen); len(res.Memory) != 0 || res.MemoryOmitted != nil {
		t.Fatalf("kept replacement reached the context: %+v %+v", res.Memory, res.MemoryOmitted)
	}
}

// prepareOldest claims the oldest claimable chunk and completes it with a
// replacement of the given estimated size.
func prepareOldest(t *testing.T, s *Store, pa string, gen int64, estTokens int) MemoryChunk {
	t.Helper()
	ctx := context.Background()
	claimed, err := s.ClaimMemoryChunk(ctx, pa, gen, 100)
	if err != nil || claimed.Chunk == nil {
		t.Fatalf("claim: %+v err=%v", claimed, err)
	}
	c, err := s.CompleteMemoryChunk(ctx, pa, gen, claimed.Chunk.ChunkSeq,
		strings.Repeat("r", estTokens*4), false)
	if err != nil || c.Status != "prepared" {
		t.Fatalf("complete chunk %d: %+v err=%v", claimed.Chunk.ChunkSeq, c, err)
	}
	return *c
}

// Applied fragments are admitted newest-first under the memory cap; the
// older ones leave the context as one explicit, time-anchored range.
func TestMemoryAppliedAdmissionCap(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)
	for i := 0; i < 8; i++ {
		commitEvents(t, s, pa, "turn", exchange(fmt.Sprint(i), bigText()))
	}
	if st, err := s.MemoryMaintain(ctx, pa, gen); err != nil || st.Sealed != 7 {
		t.Fatalf("seal: %+v err=%v", st, err)
	}
	for i := 0; i < 4; i++ {
		prepareOldest(t, s, pa, gen, 10_000)
	}
	st, err := s.MemoryMaintain(ctx, pa, gen)
	if err != nil || st.Applied != 4 || st.AppliedOmitted != 2 || st.MemorySendCapTokens != MemorySendCapTokens {
		t.Fatalf("apply: %+v err=%v", st, err)
	}
	res := loadContext(t, s, pa, gen)
	if len(res.Memory) != 2 || res.Memory[0].ChunkSeq != 3 || res.Memory[1].ChunkSeq != 4 {
		t.Fatalf("admitted blocks: %+v", res.Memory)
	}
	evs, err := s.Events(ctx, pa, 0, 100)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	at := map[int64]time.Time{}
	for _, e := range evs {
		at[e.Seq] = e.CreatedAt
	}
	if b := res.Memory[0]; !b.FirstTime.Equal(at[b.FirstSeq]) || !b.LastTime.Equal(at[b.LastSeq]) {
		t.Fatalf("block times %v..%v, events %v..%v", b.FirstTime, b.LastTime, at[b.FirstSeq], at[b.LastSeq])
	}
	om := res.MemoryOmitted
	if om == nil || om.Count != 2 || om.FirstChunkSeq != 1 || om.LastChunkSeq != 2 ||
		om.FirstSeq != 1 || om.LastSeq != 4 || om.EstTokens != 20_000 ||
		!om.FirstTime.Equal(at[1]) || !om.LastTime.Equal(at[4]) {
		t.Fatalf("memory omitted: %+v", om)
	}
	// Originals of the left-out fragments stay readable.
	chunk1, err := s.chunk(ctx, s.pool, pa, 1)
	if err != nil || chunk1.Status != "applied" || chunk1.Replacement == nil {
		t.Fatalf("chunk 1: %+v err=%v", chunk1, err)
	}
}

// One enormous newest fragment is left out whole with its locator, never
// sent over the cap.
func TestMemoryOversizedFragmentIsOmittedExplicitly(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)
	commitEvents(t, s, pa, "turn", exchange("huge", strings.Repeat("h", 120_000)))
	commitEvents(t, s, pa, "turn", exchange("b", bigText()))
	commitEvents(t, s, pa, "turn", exchange("c", bigText()))
	if _, err := s.MemoryMaintain(ctx, pa, gen); err != nil {
		t.Fatalf("seal: %v", err)
	}
	prepareOldest(t, s, pa, gen, 26_000)
	st, err := s.MemoryMaintain(ctx, pa, gen)
	if err != nil || st.Applied != 1 || st.AppliedOmitted != 1 {
		t.Fatalf("apply: %+v err=%v", st, err)
	}
	res := loadContext(t, s, pa, gen)
	if len(res.Memory) != 0 || res.MemoryOmitted == nil || res.MemoryOmitted.Count != 1 ||
		res.MemoryOmitted.EstTokens != 26_000 || res.MemoryOmitted.FirstSeq != 1 || res.MemoryOmitted.LastSeq != 2 {
		t.Fatalf("oversized fragment: memory=%+v omitted=%+v", res.Memory, res.MemoryOmitted)
	}
}

// A note written mid-turn follows the input it responds to; a crash and
// retry never journal that input twice nor show its partial records back to
// the retried turn; and the note seals with its own exchange.
func TestNoteFollowsInputExactlyOnceAcrossRetry(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	l1, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	received := func(inputID string, attempt int) EventInput {
		return EventInput{Kind: "input_received", Payload: map[string]any{
			"input_id": inputID, "kind": "message", "text": "text of " + inputID,
			"actor_kind": "human", "source_surface": "test", "attempt": attempt}}
	}
	submit := func(id string) {
		if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: id, Kind: "message",
			Payload: map[string]any{"text": "text of " + id}}); err != nil {
			t.Fatalf("submit %s: %v", id, err)
		}
	}
	// A prior, large exchange.
	submit("in-0")
	if _, err := s.LoadTurn(ctx, pa, l1.Generation, "t-0", 100); err != nil {
		t.Fatalf("load t-0: %v", err)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-0", l1.Generation, CommitRequest{Outcome: "complete",
		Events: []EventInput{received("in-0", 1),
			{Kind: "assistant_message", Payload: map[string]any{"text": bigText()}}},
		Output: map[string]any{"text": "ok"}}); err != nil {
		t.Fatalf("commit t-0: %v", err)
	}

	submit("in-1")
	if _, err := s.LoadTurn(ctx, pa, l1.Generation, "t-1", 100); err != nil {
		t.Fatalf("load t-1: %v", err)
	}
	noteReq := map[string]any{"text": "remember this"}
	mustPlan(t, s, pa, "t-1", l1.Generation, PlanCall{CallID: "c1", Tool: "journal.note", Route: "normal", Request: noteReq})
	op, _, fresh, err := s.ClaimOperation(ctx, pa, "t-1", l1.Generation, "op-1", "journal.note", 0, noteReq)
	if err != nil || !fresh {
		t.Fatalf("note claim: %+v fresh=%v err=%v", op, fresh, err)
	}

	// The process dies before committing; a new generation recovers.
	l2, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil || l2.Generation == l1.Generation {
		t.Fatalf("re-acquire: %+v err=%v", l2, err)
	}
	if _, err := s.Recover(ctx, pa, l2.Generation); err != nil {
		t.Fatalf("recover: %v", err)
	}
	res, err := s.LoadTurn(ctx, pa, l2.Generation, "t-1b", 100)
	if err != nil || res.Input == nil || res.Input.InputID != "in-1" || res.Turn.Attempt != 2 || res.Plan == nil {
		t.Fatalf("retry load: %+v err=%v", res, err)
	}
	sawPrior := false
	for _, e := range res.Context {
		if e.TurnID == "t-1" {
			t.Fatalf("retried turn sees its own partial record %+v", e)
		}
		if e.TurnID == "t-0" && e.Kind == "input_received" {
			sawPrior = true
		}
	}
	if !sawPrior {
		t.Fatal("prior exchange missing from the retried turn's context")
	}
	op2, _, fresh2, err := s.ClaimOperation(ctx, pa, "t-1b", l2.Generation, "op-2", "journal.note", 0, noteReq)
	if err != nil || fresh2 || fmt.Sprint(op2.Response["seq"]) != fmt.Sprint(op.Response["seq"]) {
		t.Fatalf("note replay: %+v fresh=%v err=%v", op2, fresh2, err)
	}
	commit := CommitRequest{Outcome: "complete",
		Events: []EventInput{received("in-1", 2),
			{Kind: "tool_call", Payload: map[string]any{"call_id": "c1", "tool": "journal.note", "request": noteReq}},
			{Kind: "tool_result", Payload: map[string]any{"call_id": "c1", "tool": "journal.note", "response": op2.Response}},
			{Kind: "assistant_message", Payload: map[string]any{"text": "noted"}}},
		Output: map[string]any{"text": "noted"}}
	if _, err := s.CommitTurn(ctx, pa, "t-1b", l2.Generation, commit); err != nil {
		t.Fatalf("commit t-1b: %v", err)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-1b", l2.Generation, commit); err != nil {
		t.Fatalf("identical commit replay: %v", err)
	}

	evs, err := s.Events(ctx, pa, 0, 100)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	var kinds []string
	var receivedSeq, noteSeq int64
	count := 0
	for _, e := range evs {
		if e.TurnID == "t-0" {
			continue
		}
		kinds = append(kinds, e.Kind)
		if e.Kind == "input_received" && e.Payload["input_id"] == "in-1" {
			count++
			receivedSeq = e.Seq
		}
		if e.Kind == "note" {
			noteSeq = e.Seq
		}
	}
	want := "input_received,note,tool_call,tool_result,assistant_message"
	if count != 1 || strings.Join(kinds, ",") != want || receivedSeq >= noteSeq {
		t.Fatalf("in-1 records: %v (input_received x%d)", kinds, count)
	}
	for _, e := range evs {
		if e.TurnID == "t-0" && e.Seq > receivedSeq {
			t.Fatalf("prior exchange record %d after in-1's input", e.Seq)
		}
	}
	var stored *int64
	if err := pool.QueryRow(ctx, `SELECT received_seq FROM core_inputs WHERE persona_id = $1 AND input_id = 'in-1'`,
		pa).Scan(&stored); err != nil || stored == nil || *stored != receivedSeq {
		t.Fatalf("received_seq %v, want %d (err=%v)", stored, receivedSeq, err)
	}

	// The next input seals the prior exchange; the note stays with its own.
	commitEvents(t, s, pa, "turn", exchange("next", "small"))
	if _, err := s.MemoryMaintain(ctx, pa, l2.Generation); err != nil {
		t.Fatalf("maintain: %v", err)
	}
	c1, err := s.chunk(ctx, s.pool, pa, 1)
	if err != nil || c1.LastSeq >= receivedSeq {
		t.Fatalf("chunk 1 %+v must end before in-1 (seq %d), err=%v", c1, receivedSeq, err)
	}
}

// historyCall runs conversation_history the way a claim does and returns the
// result as the model receives it: through JSON, without HTML escaping.
func historyCall(t *testing.T, s *Store, pa string, request map[string]any) map[string]any {
	t.Helper()
	ctx := context.Background()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	res, err := s.conversationHistory(ctx, tx, pa, request)
	if err != nil {
		t.Fatalf("history %v: %v", request, err)
	}
	raw, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}

func encodeAsModelSees(t *testing.T, v any) string {
	t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		t.Fatalf("encode: %v", err)
	}
	return strings.TrimSuffix(buf.String(), "\n")
}

func historyMessages(out map[string]any) []map[string]any {
	raw, _ := out["messages"].([]any)
	msgs := make([]map[string]any, 0, len(raw))
	for _, m := range raw {
		msgs = append(msgs, m.(map[string]any))
	}
	return msgs
}

// Text copied from a read result finds its record again through search.
func TestHistorySearchFindsTextCopiedFromRead(t *testing.T) {
	s, _ := newStore(t)
	pa := pid(t)
	mustPersona(t, s, pa)
	commitEvents(t, s, pa, "turn", []EventInput{
		{Kind: "tool_result", Payload: map[string]any{"call_id": "c9", "tool": "web.fetch",
			"output": map[string]any{"html": `<b>"quoted"</b> & more`}}},
		{Kind: "assistant_message", Payload: map[string]any{"text": "plain words here"}},
	})
	read := historyCall(t, s, pa, map[string]any{"operation": "read", "from_seq": float64(1)})
	msgs := historyMessages(read)
	if len(msgs) != 2 {
		t.Fatalf("read: %+v", read)
	}
	seen := encodeAsModelSees(t, msgs[0]["event"])
	start := strings.Index(seen, `"call_id"`)
	end := strings.Index(seen, `& more`)
	if start < 0 || end < 0 {
		t.Fatalf("read event as seen: %s", seen)
	}
	copied := seen[start : end+len(`& more`)]
	if !strings.Contains(copied, `<b>\"quoted\"</b>`) {
		t.Fatalf("copied substring is not the model's view: %s", copied)
	}
	search := func(q string) []map[string]any {
		return historyMessages(historyCall(t, s, pa, map[string]any{"operation": "search", "query": q}))
	}
	if hits := search(copied); len(hits) != 1 || hits[0]["snippet_source"] != journalEventFormat ||
		fmt.Sprint(hits[0]["source"].(map[string]any)["seq"]) != "1" {
		t.Fatalf("copied tool record: %+v", hits)
	}
	if hits := search("plain words"); len(hits) != 1 || hits[0]["snippet_source"] != "text" {
		t.Fatalf("text match: %+v", hits)
	}
	textJSON := encodeAsModelSees(t, msgs[1]["event"])
	i := strings.Index(textJSON, `"payload"`)
	if hits := search(textJSON[i:]); len(hits) != 1 || hits[0]["snippet_source"] != journalEventFormat {
		t.Fatalf("copied text record JSON %q: %+v", textJSON[i:], hits)
	}
	if hits := search(`"call_id": "c9"`); len(hits) != 0 {
		t.Fatalf("spaced JSON is not the stored form: %+v", hits)
	}
}

// One search call scans a bounded number of records and says where to go on.
func TestHistorySearchScanBudget(t *testing.T) {
	s, _ := newStore(t)
	pa := pid(t)
	mustPersona(t, s, pa)
	events := make([]EventInput, 0, 2100)
	for i := 1; i <= 2100; i++ {
		text := fmt.Sprintf("record %d", i)
		if i == 2051 {
			text = "NEEDLE in record 2051"
		}
		events = append(events, EventInput{Kind: "assistant_message", Payload: map[string]any{"text": text}})
	}
	commitEvents(t, s, pa, "turn", events)
	first := historyCall(t, s, pa, map[string]any{"operation": "search", "query": "NEEDLE"})
	if len(historyMessages(first)) != 0 || first["scan_budget_reached"] != true ||
		fmt.Sprint(first["next_after_seq"]) != "2000" || fmt.Sprint(first["scanned_records"]) != "2000" ||
		first["has_more"] != true {
		t.Fatalf("first page: %+v", first)
	}
	next := historyCall(t, s, pa, map[string]any{"operation": "search", "query": "NEEDLE", "after_seq": float64(2000)})
	hits := historyMessages(next)
	if len(hits) != 1 || fmt.Sprint(hits[0]["source"].(map[string]any)["seq"]) != "2051" ||
		next["scan_budget_reached"] != false || next["next_after_seq"] != nil {
		t.Fatalf("continuation: %+v", next)
	}
}
