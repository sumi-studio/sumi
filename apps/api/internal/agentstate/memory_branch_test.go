package agentstate

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func branchFixture(t *testing.T) (*Store, string, int64, *MemoryBranch) {
	t.Helper()
	s, _ := newStore(t)
	p := pid(t)
	mustPersona(t, s, p)
	g := acquireWriter(t, s, p, time.Minute)
	commitEvents(t, s, p, "seed", exchange("seed", bigText()))
	commitEvents(t, s, p, "next", exchange("next", "later tail"))
	if _, e := s.MemoryMaintain(context.Background(), p, g); e != nil {
		t.Fatal(e)
	}
	one, two := 1, 2
	snap := BranchSnapshot{Messages: []json.RawMessage{json.RawMessage(`{"role":"system","content":"system"}`), json.RawMessage(`{"role":"user","content":"old question"}`), json.RawMessage(`{"role":"assistant","content":"` + bigText() + `"}`)}, Tools: []json.RawMessage{json.RawMessage(`{"name":"file.write","description":"write","parameters":{"type":"object","properties":{"z":{"type":"string"},"a":{"type":"integer"}},"required":["z","a"]}}`)}, Ranges: []BranchSourceRange{{FirstSeq: 1, LastSeq: 1, MessageIndex: &one}, {FirstSeq: 2, LastSeq: 2, MessageIndex: &two}}}
	b, e := s.ClaimMemoryBranch(context.Background(), p, g, &snap)
	if e != nil || b == nil {
		t.Fatalf("claim %v %v", b, e)
	}
	return s, p, g, b
}
func checkpointRaw(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func branchState(text string) map[string]any {
	return map[string]any{"status": "running", "messages": []any{map[string]any{"role": "user", "content": "organize"}}, "rounds": 1, "tokens": 10, "policy": map[string]any{"maxRounds": 32}, "candidate": map[string]any{"text": text, "version": 1, "sha256": branchHash([]byte(text))}}
}
func TestMemoryBranchSnapshotRoundTripAndFencing(t *testing.T) {
	s, p, g, b := branchFixture(t)
	ctx := context.Background()
	before := string(b.Snapshot.Tools[0])
	want := `"z":{"type":"string"},"a":{"type":"integer"}`
	if !strings.Contains(before, want) {
		t.Fatal("schema key order changed on initial PG roundtrip", before)
	}
	next := branchState("private draft")
	saved, e := s.SaveMemoryBranch(ctx, p, g, b.Chunk.ChunkSeq, b.Revision, checkpointRaw(t, next))
	if e != nil {
		t.Fatal(e)
	}
	reboot := NewStore(s.pool)
	resumed, e := reboot.ClaimMemoryBranch(ctx, p, g, nil)
	if e != nil || resumed.Revision != saved.Revision {
		t.Fatalf("resume %v %v", resumed, e)
	}
	if output := os.Getenv("SUMI_MEMORY_PREFIX_PROOF"); output != "" {
		if err := os.WriteFile(output, checkpointRaw(t, map[string]any{"before": b.Snapshot, "after": resumed.Snapshot}), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if string(resumed.Snapshot.Tools[0]) != before {
		t.Fatal("PG did not preserve exact schema JSON")
	}
	if _, e = s.pool.Exec(ctx, `UPDATE core_writer_leases SET expires_at=now()-interval '1 second' WHERE persona_id=$1`, p); e != nil {
		t.Fatal(e)
	}
	if _, e = s.SaveMemoryBranch(ctx, p, g, b.Chunk.ChunkSeq, saved.Revision, checkpointRaw(t, next)); !errors.Is(e, ErrGenerationFence) {
		t.Fatalf("expired lease accepted: %v", e)
	}
	newGen := acquireWriter(t, s, p, time.Minute)
	if _, e = s.Recover(ctx, p, newGen); e != nil {
		t.Fatal(e)
	}
	continued, e := s.ClaimMemoryBranch(ctx, p, newGen, nil)
	if e != nil || continued == nil || continued.Revision != saved.Revision || string(continued.State) != string(saved.State) {
		t.Fatalf("new generation did not retain branch: %v %v", continued, e)
	}
	if _, e = s.SaveMemoryBranch(ctx, p, g, b.Chunk.ChunkSeq, saved.Revision, checkpointRaw(t, next)); !errors.Is(e, ErrGenerationFence) {
		t.Fatalf("old generation was not fenced: %v", e)
	}
}
func TestMemoryBranchConfirmationRevisionAndLostReceipt(t *testing.T) {
	s, p, g, b := branchFixture(t)
	ctx := context.Background()
	n := branchState("A shorter memory")
	review := map[string]any{"kind": "replace", "version": 1, "sha256": branchHash([]byte("A shorter memory")), "token": "modal", "opened_round": 1}
	n["review"] = review
	saved, e := s.SaveMemoryBranch(ctx, p, g, b.Chunk.ChunkSeq, b.Revision, checkpointRaw(t, n))
	if e != nil {
		t.Fatal(e)
	}
	n["rounds"] = 2
	n["status"] = "prepared"
	n["final"] = review
	original := n["candidate"]
	n["candidate"] = map[string]any{"text": "another", "version": 2, "sha256": branchHash([]byte("another"))}
	if _, e = s.SaveMemoryBranch(ctx, p, g, b.Chunk.ChunkSeq, saved.Revision, checkpointRaw(t, n)); !errors.Is(e, ErrMemoryConflict) {
		t.Fatalf("edited draft used old modal: %v", e)
	}
	n["candidate"] = original
	raw := checkpointRaw(t, n)
	done, e := s.SaveMemoryBranch(ctx, p, g, b.Chunk.ChunkSeq, saved.Revision, raw)
	if e != nil {
		t.Fatal(e)
	}
	replay, e := s.SaveMemoryBranch(ctx, p, g, b.Chunk.ChunkSeq, saved.Revision, raw)
	if e != nil || replay.Revision != done.Revision {
		t.Fatalf("lost receipt replay %v %v", replay, e)
	}
	commitEvents(t, s, p, "later", exchange("later", strings.Repeat("later ", 30000)))
	if _, e = s.MemoryMaintain(ctx, p, g); e != nil {
		t.Fatal(e)
	}
	rc, e := s.renderedContext(ctx, s.pool, p, 1, "")
	if e != nil {
		t.Fatal(e)
	}
	if len(rc.Memory) != 1 || len(rc.Events) < 2 || !strings.Contains(string(checkpointRaw(t, rc.Events)), "later") {
		t.Fatalf("application lost later original context")
	}
}
func TestMemoryBranchFaultTransitionsAndPausedScheduling(t *testing.T) {
	s, p, g, b := branchFixture(t)
	ctx := context.Background()
	n := branchState("saved draft")
	n["status"] = "paused"
	n["pause_reason"] = "context_capacity"
	n["issue"] = map[string]string{"code": "memory_context_capacity", "message": "capacity rejected"}
	saved, e := s.SaveMemoryBranch(ctx, p, g, b.Chunk.ChunkSeq, b.Revision, checkpointRaw(t, n))
	if e != nil {
		t.Fatal(e)
	}
	again, e := s.SaveMemoryBranch(ctx, p, g, b.Chunk.ChunkSeq, saved.Revision, checkpointRaw(t, n))
	if e != nil {
		t.Fatal(e)
	}
	st, e := s.MemoryStatus(ctx, p)
	if e != nil || st.Claimable != 0 || len(st.Branches) != 1 || st.Branches[0].Issue == nil {
		t.Fatalf("status %v %v", st, e)
	}
	resumed, e := s.ClaimMemoryBranch(ctx, p, g, nil)
	if e != nil || resumed != nil {
		t.Fatalf("indefinite pause was automatically retried: %v %v", resumed, e)
	}
	n["status"] = "running"
	n["issue"] = nil
	n["pause_reason"] = nil
	if _, e = s.SaveMemoryBranch(ctx, p, g, b.Chunk.ChunkSeq, again.Revision, checkpointRaw(t, n)); e != nil {
		t.Fatal(e)
	}
	var notices, posts int
	if e = s.pool.QueryRow(ctx, `SELECT count(*) FROM core_inputs WHERE persona_id=$1 AND actor_kind='memory' AND source_surface='core_memory' AND attention='observe'`, p).Scan(&notices); e != nil {
		t.Fatal(e)
	}
	if e = s.pool.QueryRow(ctx, `SELECT count(*) FROM core_outbox WHERE persona_id=$1`, p).Scan(&posts); e != nil {
		t.Fatal(e)
	}
	if notices != 2 || posts != 0 {
		t.Fatalf("want occurrence/recovery only and no publication, notices=%d posts=%d", notices, posts)
	}
}

func TestMemoryBranchKeepReconsiderationNeedsNewLiveMaterial(t *testing.T) {
	s, p, g, b := branchFixture(t)
	ctx := context.Background()
	n := branchState("unfinished candidate")
	review := map[string]any{"kind": "keep", "version": 0, "sha256": "source-hash", "token": "keep-modal", "opened_round": 1}
	n["review"] = review
	saved, e := s.SaveMemoryBranch(ctx, p, g, b.Chunk.ChunkSeq, b.Revision, checkpointRaw(t, n))
	if e != nil {
		t.Fatal(e)
	}
	n["rounds"] = 2
	n["status"] = "kept"
	n["final"] = review
	done, e := s.SaveMemoryBranch(ctx, p, g, b.Chunk.ChunkSeq, saved.Revision, checkpointRaw(t, n))
	if e != nil {
		t.Fatal(e)
	}
	if again, e := s.ClaimMemoryBranch(ctx, p, g, &b.Snapshot); e != nil || again != nil {
		t.Fatalf("identical KEEP was retried %v %v", again, e)
	}
	snap := b.Snapshot
	idx := len(snap.Messages)
	snap.Messages = append(snap.Messages, json.RawMessage(`{"role":"user","content":"`+strings.Repeat("new material ", 16000)+`"}`))
	snap.Ranges = append(snap.Ranges, BranchSourceRange{FirstSeq: 3, LastSeq: 3, MessageIndex: &idx})
	again, e := s.ClaimMemoryBranch(ctx, p, g, &snap)
	if e != nil || again == nil {
		t.Fatalf("new material did not reconsider KEEP %v %v", again, e)
	}
	if again.State != nil || again.Revision <= done.Revision {
		t.Fatalf("new attempt reused old progress: %v", again)
	}
	var history string
	if e = s.pool.QueryRow(ctx, `SELECT previous_attempts FROM core_memory_branches WHERE persona_id=$1 AND chunk_seq=$2`, p, b.Chunk.ChunkSeq).Scan(&history); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(history, "unfinished candidate") || !strings.Contains(history, "keep-modal") {
		t.Fatal("prior attempt not preserved")
	}
}

func TestMemoryBranchJapaneseEmojiShrinkUsesUTF8Bytes(t *testing.T) {
	for _, text := range []string{strings.Repeat("雨", 12), strings.Repeat("😀", 9)} {
		t.Run(text[:3], func(t *testing.T) {
			s, p, g, b := branchFixture(t)
			ctx := context.Background()
			if _, e := s.pool.Exec(ctx, `UPDATE core_memory_chunks SET est_tokens=9 WHERE persona_id=$1 AND chunk_seq=$2`, p, b.Chunk.ChunkSeq); e != nil {
				t.Fatal(e)
			}
			n := branchState(text)
			review := map[string]any{"kind": "replace", "version": 1, "sha256": branchHash([]byte(text)), "token": "utf8-modal", "opened_round": 1}
			n["review"] = review
			saved, e := s.SaveMemoryBranch(ctx, p, g, b.Chunk.ChunkSeq, b.Revision, checkpointRaw(t, n))
			if e != nil {
				t.Fatal(e)
			}
			n["rounds"] = 2
			n["status"] = "prepared"
			n["final"] = review
			if _, e = s.SaveMemoryBranch(ctx, p, g, b.Chunk.ChunkSeq, saved.Revision, checkpointRaw(t, n)); !errors.Is(e, ErrBadRequest) {
				t.Fatalf("nonshrinking UTF8 candidate accepted: %v", e)
			}
			smaller := string([]rune(text)[:len([]rune(text))-4])
			n["status"] = "running"
			delete(n, "final")
			n["candidate"] = map[string]any{"text": smaller, "version": 2, "sha256": branchHash([]byte(smaller))}
			n["review"] = nil
			saved, e = s.SaveMemoryBranch(ctx, p, g, b.Chunk.ChunkSeq, saved.Revision, checkpointRaw(t, n))
			if e != nil {
				t.Fatal(e)
			}
			review = map[string]any{"kind": "replace", "version": 2, "sha256": branchHash([]byte(smaller)), "token": "smaller-modal", "opened_round": 3}
			n["rounds"] = 3
			n["review"] = review
			saved, e = s.SaveMemoryBranch(ctx, p, g, b.Chunk.ChunkSeq, saved.Revision, checkpointRaw(t, n))
			if e != nil {
				t.Fatal(e)
			}
			n["rounds"] = 4
			n["status"] = "prepared"
			n["final"] = review
			done, e := s.SaveMemoryBranch(ctx, p, g, b.Chunk.ChunkSeq, saved.Revision, checkpointRaw(t, n))
			if e != nil {
				t.Fatal(e)
			}
			if *done.Chunk.ReplacementEstTokens != int64((len(smaller)+3)/4) {
				t.Fatal("UTF8 estimator mismatch")
			}
		})
	}
}

// The Go fixture supplies the already rendered parent boundary. Claim itself
// never loads history; the separate Core/host tests exercise live rendering.
func liveBranchSnapshotFixture(t *testing.T, s *Store, p string) BranchSnapshot {
	t.Helper()
	rc, e := s.renderedContext(context.Background(), s.pool, p, 1, "")
	if e != nil {
		t.Fatal(e)
	}
	snap := BranchSnapshot{Messages: []json.RawMessage{json.RawMessage(`{"role":"system","content":"fixed parent instructions"}`)}, Tools: []json.RawMessage{json.RawMessage(`{"name":"file.read","parameters":{"type":"object"}}`), json.RawMessage(`{"name":"file.write","parameters":{"type":"object"}}`)}}
	for _, event := range rc.Events {
		idx := len(snap.Messages)
		snap.Messages = append(snap.Messages, checkpointRaw(t, map[string]any{"role": "user", "content": string(checkpointRaw(t, event.Payload))}))
		snap.Ranges = append(snap.Ranges, BranchSourceRange{FirstSeq: event.Seq, LastSeq: event.Seq, MessageIndex: &idx})
	}
	for _, block := range rc.Memory {
		idx := len(snap.Messages)
		layer := block.Layer
		chunkSeq := block.ChunkSeq
		snap.Messages = append(snap.Messages, checkpointRaw(t, map[string]any{"role": "user", "content": block.Text}))
		snap.Ranges = append(snap.Ranges, BranchSourceRange{FirstSeq: block.FirstSeq, LastSeq: block.LastSeq, MessageIndex: &idx, Layer: &layer, ChunkSeq: &chunkSeq})
	}
	return snap
}
func reviewedBranchFixture(t *testing.T, s *Store, p string, g, seq int64, text string) *MemoryBranch {
	t.Helper()
	ctx := context.Background()
	snap := liveBranchSnapshotFixture(t, s, p)
	b, e := s.ClaimMemoryBranch(ctx, p, g, &snap)
	if e != nil || b == nil || b.Chunk.ChunkSeq != seq {
		t.Fatalf("claim %d: %+v %v", seq, b, e)
	}
	n := branchState(text)
	review := map[string]any{"kind": "replace", "version": 1, "sha256": branchHash([]byte(text)), "token": "reviewed", "opened_round": 1}
	n["review"] = review
	saved, e := s.SaveMemoryBranch(ctx, p, g, seq, b.Revision, checkpointRaw(t, n))
	if e != nil {
		t.Fatal(e)
	}
	reboot := NewStore(s.pool)
	resumed, e := reboot.ClaimMemoryBranch(ctx, p, g, nil)
	if e != nil || resumed == nil || resumed.Revision != saved.Revision {
		t.Fatalf("restart %d: %v %v", seq, resumed, e)
	}
	n["rounds"] = 2
	n["status"] = "prepared"
	n["final"] = review
	done, e := reboot.SaveMemoryBranch(ctx, p, g, seq, resumed.Revision, checkpointRaw(t, n))
	if e != nil {
		t.Fatal(e)
	}
	return done
}
func TestMemoryBranchAllLayersSealReviewRestartApply(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	p := pid(t)
	mustPersona(t, s, p)
	g := acquireWriter(t, s, p, time.Minute)
	seedSealed(t, s, p, g, 8)
	for seq := int64(1); seq <= 7; seq++ {
		reviewedBranchFixture(t, s, p, g, seq, l1Replacement())
	}
	beforeApply := liveBranchSnapshotFixture(t, s, p)
	st, e := s.MemoryMaintain(ctx, p, g)
	if e != nil || st.Applied != 5 || st.Sealed != 1 {
		t.Fatalf("L1 apply %v %v", st, e)
	}
	if wrong, e := s.ClaimMemoryBranch(ctx, p, g, &beforeApply); e != nil || wrong != nil {
		t.Fatalf("L2 accepted the raw pre-apply snapshot: %v %v", wrong, e)
	}
	stale := liveBranchSnapshotFixture(t, s, p)
	for i := range stale.Ranges {
		if stale.Ranges[i].ChunkSeq != nil {
			n := *stale.Ranges[i].ChunkSeq + 1000
			stale.Ranges[i].ChunkSeq = &n
		}
	}
	if wrong, e := s.ClaimMemoryBranch(ctx, p, g, &stale); e != nil || wrong != nil {
		t.Fatalf("L2 accepted same range/layer but different fragment identities: %v %v", wrong, e)
	}
	l2a := strings.Repeat("b", 20000)
	b := reviewedBranchFixture(t, s, p, g, 8, l2a)
	if b.Chunk.Layer != 2 || len(b.Chunk.Sources) != 2 {
		t.Fatal("missing L1 consolidation target")
	}
	var sourceLayers []int
	for _, r := range b.Snapshot.Ranges {
		if r.FirstSeq >= b.Chunk.FirstSeq && r.LastSeq <= b.Chunk.LastSeq && r.Layer != nil {
			sourceLayers = append(sourceLayers, *r.Layer)
		}
	}
	if len(sourceLayers) != 2 || sourceLayers[0] != 1 || sourceLayers[1] != 1 {
		t.Fatal("upper source is not the applied L1 context")
	}
	if _, e = s.MemoryMaintain(ctx, p, g); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 3; i++ {
		commitEvents(t, s, p, "later", exchange("later", bigText()))
	}
	if _, e = s.MemoryMaintain(ctx, p, g); e != nil {
		t.Fatal(e)
	}
	for seq := int64(9); seq <= 11; seq++ {
		reviewedBranchFixture(t, s, p, g, seq, l1Replacement())
	}
	reviewedBranchFixture(t, s, p, g, 12, strings.Repeat("c", 22000))
	if _, e = s.MemoryMaintain(ctx, p, g); e != nil {
		t.Fatal(e)
	}
	b = reviewedBranchFixture(t, s, p, g, 13, "integrated memory")
	for _, r := range b.Snapshot.Ranges {
		if r.FirstSeq >= b.Chunk.FirstSeq && r.LastSeq <= b.Chunk.LastSeq && (r.Layer == nil || *r.Layer != 2) {
			t.Fatal("reintegration escaped L2 sources")
		}
	}
	st, e = s.MemoryMaintain(ctx, p, g)
	if e != nil || st.Superseded != 6 || st.Applied != 5 {
		t.Fatalf("L2 reintegration %v %v", st, e)
	}
	rc, e := s.renderedContext(ctx, s.pool, p, 1, "")
	if e != nil || len(rc.Memory) == 0 || rc.Memory[0].ChunkSeq != 13 || rc.Memory[0].Text != "integrated memory" {
		t.Fatalf("render %v %v", rc, e)
	}
	for _, ev := range rc.Events {
		if ev.Seq <= 8 {
			t.Fatal("superseded original rendered twice")
		}
	}
	if len(rc.Events) == 0 {
		t.Fatal("later raw tail disappeared")
	}
}
