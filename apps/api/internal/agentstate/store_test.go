package agentstate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

func pid(t *testing.T) string {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuidv7: %v", err)
	}
	return id.String()
}

func newStore(t *testing.T) (*Store, *pgxpool.Pool) {
	t.Helper()
	pool := testdb.Create(t)
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return NewStore(pool), pool
}

func mustPersona(t *testing.T, s *Store, id string) {
	t.Helper()
	if _, _, err := s.EnsurePersona(context.Background(), id, nil, ""); err != nil {
		t.Fatalf("ensure persona %s: %v", id, err)
	}
}

func TestWriterFencingAndRecovery(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)

	// First writer acquires.
	l1, err := s.AcquireWriter(ctx, pa, "holder-1", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if l1.Generation != 1 {
		t.Fatalf("generation = %d, want 1", l1.Generation)
	}

	// A concurrent writer is refused while the lease is live.
	if _, err := s.AcquireWriter(ctx, pa, "holder-2", time.Minute); err != ErrWriterHeld {
		t.Fatalf("concurrent acquire err = %v, want ErrWriterHeld", err)
	}

	// Same holder re-acquiring (e.g. idempotent reconnect) keeps working.
	l1b, err := s.AcquireWriter(ctx, pa, "holder-1", 50*time.Millisecond)
	if err != nil || l1b.Generation != 2 {
		t.Fatalf("same-holder re-acquire: %+v err=%v", l1b, err)
	}

	// After expiry a different holder wins and the old generation is fenced.
	time.Sleep(60 * time.Millisecond)
	l2, err := s.AcquireWriter(ctx, pa, "holder-2", time.Minute)
	if err != nil {
		t.Fatalf("post-expiry acquire: %v", err)
	}
	if l2.Generation != 3 {
		t.Fatalf("generation = %d, want 3", l2.Generation)
	}

	// Old generation cannot mutate: commit attempt under gen 2 is fenced.
	_, err = s.CommitTurn(ctx, pa, "any-turn", 2, CommitRequest{Outcome: "complete"})
	if err != ErrGenerationFence {
		t.Fatalf("old-generation commit err = %v, want ErrGenerationFence", err)
	}

	// Persona isolation: holder-2's lease on persona-a gives no rights on b.
	pb := pid(t)
	mustPersona(t, s, pb)
	lb, err := s.AcquireWriter(ctx, pb, "holder-b", time.Minute)
	if err != nil || lb.Generation != 1 {
		t.Fatalf("persona-b acquire: %+v err=%v", lb, err)
	}
	if _, err := s.CommitTurn(ctx, pb, "t-x", 3, CommitRequest{Outcome: "complete"}); err != ErrGenerationFence {
		t.Fatalf("persona-a generation must not write persona-b: err=%v", err)
	}
}

func TestInputTurnCommitReplay(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	in := &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "hello"}, ActorKind: "human", ActorID: "h-1",
		SourceSurface: "dev", Attention: "reply"}
	stored, created, err := s.SubmitInput(ctx, in)
	if err != nil || !created || stored.Status != "queued" {
		t.Fatalf("submit: %+v created=%v err=%v", stored, created, err)
	}
	// Lost submit response → resubmit replays, not duplicates.
	again, created2, err := s.SubmitInput(ctx, in)
	if err != nil || created2 || again.Status != "queued" {
		t.Fatalf("resubmit: created=%v status=%s err=%v", created2, again.Status, err)
	}

	load, err := s.LoadTurn(ctx, pa, lease.Generation, "turn-1", 10)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if load.Turn == nil || load.Turn.Status != "running" || load.Input == nil || load.Input.InputID != "in-1" {
		t.Fatalf("load: %+v", load)
	}

	// Lost load response → second load replays the same running turn.
	load2, err := s.LoadTurn(ctx, pa, lease.Generation, "turn-other", 10)
	if err != nil {
		t.Fatalf("load replay: %v", err)
	}
	if load2.Turn == nil || load2.Turn.TurnID != "turn-1" {
		t.Fatalf("load replay returned %+v, want running turn-1", load2.Turn)
	}

	commit := CommitRequest{
		Outcome: "complete",
		Events: []EventInput{
			{Kind: "input_received", Payload: map[string]any{"input_id": "in-1"}},
			{Kind: "assistant_message", Payload: map[string]any{"text": "hi"}},
		},
		Output: map[string]any{"text": "hi"},
		Usage:  map[string]any{"input_tokens": 3, "output_tokens": 1},
	}
	turn, err := s.CommitTurn(ctx, pa, "turn-1", lease.Generation, commit)
	if err != nil || turn.Status != "done" {
		t.Fatalf("commit: %+v err=%v", turn, err)
	}
	// Lost commit response → re-commit replays stored result.
	turn2, err := s.CommitTurn(ctx, pa, "turn-1", lease.Generation, commit)
	if err != nil || turn2.Status != "done" {
		t.Fatalf("commit replay: %+v err=%v", turn2, err)
	}

	in2, linked, err := s.GetInput(ctx, pa, "in-1")
	if err != nil || in2.Status != "done" || linked == nil || linked.Status != "done" {
		t.Fatalf("input after commit: %+v turn=%+v err=%v", in2, linked, err)
	}

	evs, err := s.Events(ctx, pa, 0, 100)
	if err != nil || len(evs) != 2 {
		t.Fatalf("events: %d err=%v", len(evs), err)
	}
	if evs[0].Seq != 1 || evs[1].Seq != 2 {
		t.Fatalf("event seqs: %d,%d", evs[0].Seq, evs[1].Seq)
	}
	out, err := s.Outbox(ctx, pa, 0, 10)
	if err != nil || len(out) != 1 || out[0].Kind != "turn_completed" {
		t.Fatalf("outbox: %+v err=%v", out, err)
	}
}

func TestCrashMidTurnRecovery(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)

	l1, err := s.AcquireWriter(ctx, pa, "gen1", 30*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire gen1: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-x", Kind: "message",
		Payload: map[string]any{"text": "work"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	load, err := s.LoadTurn(ctx, pa, l1.Generation, "t-1", 10)
	if err != nil || load.Turn == nil {
		t.Fatalf("load gen1: %v", err)
	}
	// Writer dies mid-turn (lease expires; no commit).
	time.Sleep(40 * time.Millisecond)

	l2 := acquireAfterExpiry(t, s, pa, "gen2")
	rec, err := s.Recover(ctx, pa, l2.Generation)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(rec.InterruptedTurns) != 1 || rec.InterruptedTurns[0] != "t-1" {
		t.Fatalf("interrupted: %+v", rec)
	}
	if len(rec.RequeuedInputs) != 1 || rec.RequeuedInputs[0] != "in-x" {
		t.Fatalf("requeued: %+v", rec)
	}

	// The old generation is fenced even for reads-after-write paths.
	if _, err := s.CommitTurn(ctx, pa, "t-1", l1.Generation,
		CommitRequest{Outcome: "complete"}); err == nil {
		t.Fatalf("dead generation committed")
	}

	// New generation re-runs the same input as attempt 2.
	load2, err := s.LoadTurn(ctx, pa, l2.Generation, "t-2", 10)
	if err != nil || load2.Turn == nil || load2.Turn.Attempt != 2 {
		t.Fatalf("recovery load: %+v err=%v", load2, err)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-2", l2.Generation, CommitRequest{
		Outcome: "complete", Output: map[string]any{"text": "done"},
	}); err != nil {
		t.Fatalf("recovery commit: %v", err)
	}
	in, turn, err := s.GetInput(ctx, pa, "in-x")
	if err != nil || in.Status != "done" || turn == nil || turn.TurnID != "t-2" {
		t.Fatalf("final input: %+v turn=%+v err=%v", in, turn, err)
	}
}

// f74: the store API accepted a commit carrying the same input_received
// twice, journaled both, and linked the marker to the first — the second
// copy was then unlinked and the cut's reverse-link check refused every
// later Seal, permanently. The write boundary now dedups: one receipt per
// input, per journal. A receipt naming a still-queued input is journaled
// history and gets its marker linked, so that input's own commit does not
// repeat it.
func TestCommitDeduplicatesInputReceived(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	l, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	for _, id := range []string{"in-1", "in-2"} {
		if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: id, Kind: "message",
			Payload: map[string]any{"text": id}}); err != nil {
			t.Fatalf("submit %s: %v", id, err)
		}
	}
	if _, err := s.LoadTurn(ctx, pa, l.Generation, "t-1", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-1", l.Generation, CommitRequest{
		Outcome: "complete",
		Events: []EventInput{
			{Kind: "input_received", Payload: map[string]any{"input_id": "in-1"}},
			{Kind: "note", Payload: map[string]any{"text": "between"}},
			{Kind: "input_received", Payload: map[string]any{"input_id": "in-1"}}, // duplicate copy
			{Kind: "input_received", Payload: map[string]any{"input_id": "in-2"}}, // other input's receipt
		},
		Output: map[string]any{"text": "ok"},
	}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	var receipts, seq1, seq2 int64
	var marker1, marker2 *int64
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM core_events WHERE persona_id = $1 AND kind = 'input_received'`,
		pa).Scan(&receipts); err != nil || receipts != 2 {
		t.Fatalf("journaled receipts = %d err=%v, want exactly 2", receipts, err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT min(seq) FILTER (WHERE payload->>'input_id' = 'in-1'),
				min(seq) FILTER (WHERE payload->>'input_id' = 'in-2')
		 FROM core_events WHERE persona_id = $1 AND kind = 'input_received'`,
		pa).Scan(&seq1, &seq2); err != nil || seq1 != 1 || seq2 != 3 {
		t.Fatalf("receipt seqs = %d,%d err=%v, want 1,3", seq1, seq2, err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT max(received_seq) FILTER (WHERE input_id = 'in-1'),
				max(received_seq) FILTER (WHERE input_id = 'in-2')
		 FROM core_inputs WHERE persona_id = $1`,
		pa).Scan(&marker1, &marker2); err != nil {
		t.Fatalf("markers: %v", err)
	}
	if marker1 == nil || *marker1 != seq1 || marker2 == nil || *marker2 != seq2 {
		t.Fatalf("markers = %v,%v want %d,%d", marker1, marker2, seq1, seq2)
	}
	// in-2's own commit must not re-journal its receipt.
	load2, err := s.LoadTurn(ctx, pa, l.Generation, "t-2", 10)
	if err != nil || load2.Input == nil || load2.Input.InputID != "in-2" {
		t.Fatalf("load in-2: %+v err=%v", load2, err)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-2", l.Generation, CommitRequest{
		Outcome: "complete",
		Events: []EventInput{
			{Kind: "input_received", Payload: map[string]any{"input_id": "in-2"}},
			{Kind: "assistant_message", Payload: map[string]any{"text": "done"}},
		},
		Output: map[string]any{"text": "done"},
	}); err != nil {
		t.Fatalf("in-2 commit: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM core_events WHERE persona_id = $1 AND kind = 'input_received'`,
		pa).Scan(&receipts); err != nil || receipts != 2 {
		t.Fatalf("receipts after in-2 commit = %d err=%v, want 2", receipts, err)
	}
}

// unlinkedReceipts is the cut verifier's reverse link: every journaled
// input_received that names an existing input must be the one its marker
// points at.
func unlinkedReceipts(t *testing.T, pool *pgxpool.Pool, pa string) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM core_events e
		JOIN core_inputs i ON i.persona_id = e.persona_id
			AND i.input_id = e.payload->>'input_id'
		WHERE e.persona_id = $1 AND e.kind = 'input_received'
			AND (i.received_seq IS NULL OR i.received_seq <> e.seq)`,
		pa).Scan(&n); err != nil {
		t.Fatalf("unlinked receipts: %v", err)
	}
	return n
}

// f-memory-86/87: a receipt must carry a non-empty string input_id naming
// an existing input — the commit is refused before anything lands, so a
// retry once the input exists journals exactly one receipt and the persona
// stays sealable.
func TestCommitRejectsReceiptForAbsentInput(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	l, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "one"}}); err != nil {
		t.Fatalf("submit in-1: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, l.Generation, "t-1", 10); err != nil {
		t.Fatalf("load t-1: %v", err)
	}
	ghostCommit := CommitRequest{
		Outcome: "complete",
		Events: []EventInput{
			{Kind: "input_received", Payload: map[string]any{"input_id": "in-1"}},
			{Kind: "input_received", Payload: map[string]any{"input_id": "ghost-1"}},
			{Kind: "assistant_message", Payload: map[string]any{"text": "hi"}},
		},
		Output: map[string]any{"text": "hi"},
	}
	if _, err := s.CommitTurn(ctx, pa, "t-1", l.Generation, ghostCommit); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("ghost-receipt commit err = %v, want ErrBadRequest", err)
	}
	// The refusal rolled back: nothing journaled, the turn still runs, and
	// no input or marker state was touched.
	var events int64
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM core_events WHERE persona_id = $1`, pa).Scan(&events); err != nil || events != 0 {
		t.Fatalf("events after refused commit = %d err=%v, want 0", events, err)
	}
	tm, err := s.turn(ctx, pool, pa, "t-1")
	if err != nil || tm.Status != "running" {
		t.Fatalf("turn after refused commit = %+v err=%v, want running", tm, err)
	}
	// Once the input exists the same commit succeeds.
	if _, created, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "ghost-1",
		Kind: "message", Payload: map[string]any{"text": "arrived late"}}); err != nil || !created {
		t.Fatalf("submit ghost-1: created=%v err=%v", created, err)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-1", l.Generation, ghostCommit); err != nil {
		t.Fatalf("retry after input creation: %v", err)
	}
	var receipts int64
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM core_events WHERE persona_id = $1 AND kind = 'input_received'`,
		pa).Scan(&receipts); err != nil || receipts != 2 {
		t.Fatalf("receipts = %d err=%v, want 2", receipts, err)
	}
	if n := unlinkedReceipts(t, pool, pa); n != 0 {
		t.Fatalf("unlinked input_received rows = %d, want 0", n)
	}
}

// A receipt's input_id is a string — the documented input-ID type. Any
// other JSON shape refuses the commit before mutation; the same commit is
// legal once corrected.
func TestCommitRejectsMalformedReceiptIDs(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	l, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "5", Kind: "message",
		Payload: map[string]any{"text": "five"}}); err != nil {
		t.Fatalf("submit 5: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, l.Generation, "t-1", 10); err != nil {
		t.Fatalf("load t-1: %v", err)
	}
	// Every malformed identity shape refuses — including inside a commit
	// whose other receipts are valid, and a copy dedup would have dropped.
	for i, bad := range []any{float64(5), nil, map[string]any{"a": 1}, true, ""} {
		_, err := s.CommitTurn(ctx, pa, "t-1", l.Generation, CommitRequest{
			Outcome: "complete",
			Events: []EventInput{
				{Kind: "input_received", Payload: map[string]any{"input_id": "5"}},
				{Kind: "input_received", Payload: map[string]any{"input_id": bad}},
			},
			Output: map[string]any{"text": "hi"},
		})
		if !errors.Is(err, ErrBadRequest) {
			t.Fatalf("malformed input_id %v (%d): err = %v, want ErrBadRequest", bad, i, err)
		}
	}
	var events int64
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM core_events WHERE persona_id = $1`, pa).Scan(&events); err != nil || events != 0 {
		t.Fatalf("events after refused commits = %d err=%v, want 0", events, err)
	}
	// A valid commit still works: one receipt for "5", marker linked.
	if _, err := s.CommitTurn(ctx, pa, "t-1", l.Generation, CommitRequest{
		Outcome: "complete",
		Events: []EventInput{
			{Kind: "input_received", Payload: map[string]any{"input_id": "5"}},
			{Kind: "assistant_message", Payload: map[string]any{"text": "hi"}},
		},
		Output: map[string]any{"text": "hi"},
	}); err != nil {
		t.Fatalf("valid commit after refusals: %v", err)
	}
	var receipts int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM core_events
		WHERE persona_id = $1 AND kind = 'input_received' AND payload->>'input_id' = '5'`,
		pa).Scan(&receipts); err != nil || receipts != 1 {
		t.Fatalf("receipts = %d err=%v, want 1", receipts, err)
	}
	if n := unlinkedReceipts(t, pool, pa); n != 0 {
		t.Fatalf("unlinked input_received rows = %d, want 0", n)
	}
}

// The lost-adoption race from the reviews is closed by refusal, not by
// locking: a commit carrying a receipt for an input being submitted
// concurrently either sees the committed row (journals and links it) or is
// refused cleanly — it can never produce a journaled receipt beside a
// NULL-marker input.
func TestConcurrentSubmitVsReceiptCommit(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	l, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "one"}}); err != nil {
		t.Fatalf("submit in-1: %v", err)
	}
	for i := 0; i < 12; i++ {
		turn := fmt.Sprintf("t-race-%d", i)
		input := fmt.Sprintf("race-%d", i)
		load, err := s.LoadTurn(ctx, pa, l.Generation, turn, 10)
		if err != nil {
			t.Fatalf("load %s: %v", turn, err)
		}
		var wg sync.WaitGroup
		var commitErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, commitErr = s.CommitTurn(ctx, pa, turn, l.Generation, CommitRequest{
				Outcome: "complete",
				Events: []EventInput{
					{Kind: "input_received", Payload: map[string]any{"input_id": load.Input.InputID}},
					{Kind: "input_received", Payload: map[string]any{"input_id": input}},
				},
				Output: map[string]any{"text": "ok"},
			})
		}()
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(i%4) * time.Millisecond)
			if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa,
				InputID: input, Kind: "message",
				Payload: map[string]any{"text": "late"}}); err != nil {
				t.Errorf("submit %s: %v", input, err)
			}
		}()
		wg.Wait()
		if commitErr != nil {
			// Refused while the input was still absent: the turn stays
			// running and a retry once the input exists succeeds.
			if !errors.Is(commitErr, ErrBadRequest) {
				t.Fatalf("commit %s: %v, want ErrBadRequest", turn, commitErr)
			}
			if _, err := s.CommitTurn(ctx, pa, turn, l.Generation, CommitRequest{
				Outcome: "complete",
				Events: []EventInput{
					{Kind: "input_received", Payload: map[string]any{"input_id": load.Input.InputID}},
					{Kind: "input_received", Payload: map[string]any{"input_id": input}},
				},
				Output: map[string]any{"text": "ok"},
			}); err != nil {
				t.Fatalf("retry commit %s after input landed: %v", turn, err)
			}
		}
	}
	// Whatever the interleaving, every journaled receipt is linked.
	if n := unlinkedReceipts(t, pool, pa); n != 0 {
		t.Fatalf("unlinked input_received rows = %d, want 0", n)
	}
	var dangling int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM core_inputs
		WHERE persona_id = $1 AND received_seq IS NOT NULL AND NOT EXISTS (
			SELECT 1 FROM core_events e
			WHERE e.persona_id = core_inputs.persona_id AND e.seq = core_inputs.received_seq
				AND e.kind = 'input_received'
				AND e.payload->>'input_id' = core_inputs.input_id)`,
		pa).Scan(&dangling); err != nil || dangling != 0 {
		t.Fatalf("mismatched markers = %d err=%v, want 0", dangling, err)
	}
}

// The same race against a job-terminal notification: the notification
// inserts the 'job:<id>' input inside the job's terminal transition, and a
// concurrent commit carrying a receipt for that id either sees the
// committed row or is refused — never a journaled receipt beside a
// NULL-marker input.
func TestConcurrentJobNotifyVsReceiptCommit(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	l, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "one"}}); err != nil {
		t.Fatalf("submit in-1: %v", err)
	}
	for i := 0; i < 8; i++ {
		jobID := fmt.Sprintf("j-race-%d", i)
		input := "job:" + jobID
		turn := fmt.Sprintf("t-job-%d", i)
		if _, _, err := s.SubmitJob(ctx, pa, jobID, "subprocess",
			map[string]any{"command": []any{"echo", "hi"}}, "api"); err != nil {
			t.Fatalf("submit %s: %v", jobID, err)
		}
		claimed, _, err := s.ClaimJobs(ctx, pa, "runner-1", []string{"subprocess"}, time.Minute, 1)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("claim %s: claimed=%+v err=%v", jobID, claimed, err)
		}
		load, err := s.LoadTurn(ctx, pa, l.Generation, turn, 10)
		if err != nil {
			t.Fatalf("load %s: %v", turn, err)
		}
		commit := func() error {
			_, err := s.CommitTurn(ctx, pa, turn, l.Generation, CommitRequest{
				Outcome: "complete",
				Events: []EventInput{
					{Kind: "input_received", Payload: map[string]any{"input_id": load.Input.InputID}},
					{Kind: "input_received", Payload: map[string]any{"input_id": input}},
				},
				Output: map[string]any{"text": "ok"},
			})
			return err
		}
		var wg sync.WaitGroup
		var commitErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			commitErr = commit()
		}()
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(i%4) * time.Millisecond)
			if _, err := s.CompleteJob(ctx, pa, jobID, "runner-1", "done",
				map[string]any{"exit_code": 0.0}, ""); err != nil {
				t.Errorf("complete %s: %v", jobID, err)
			}
		}()
		wg.Wait()
		if commitErr != nil {
			if !errors.Is(commitErr, ErrBadRequest) {
				t.Fatalf("commit %s: %v, want ErrBadRequest", turn, commitErr)
			}
			if err := commit(); err != nil {
				t.Fatalf("retry commit %s after notification landed: %v", turn, err)
			}
		}
	}
	if n := unlinkedReceipts(t, pool, pa); n != 0 {
		t.Fatalf("unlinked input_received rows = %d, want 0", n)
	}
	var dangling int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM core_inputs
		WHERE persona_id = $1 AND received_seq IS NOT NULL AND NOT EXISTS (
			SELECT 1 FROM core_events e
			WHERE e.persona_id = core_inputs.persona_id AND e.seq = core_inputs.received_seq
				AND e.kind = 'input_received'
				AND e.payload->>'input_id' = core_inputs.input_id)`,
		pa).Scan(&dangling); err != nil || dangling != 0 {
		t.Fatalf("mismatched markers = %d err=%v, want 0", dangling, err)
	}
}

// mustPlan records a durable round-0 decision for the turn's input.
func mustPlan(t *testing.T, s *Store, pa, turnID string, gen int64, calls ...PlanCall) TurnPlan {
	t.Helper()
	p, created, err := s.SavePlan(context.Background(), pa, turnID, gen, 0,
		Decision{Text: "reply", Calls: calls})
	if err != nil || !created {
		t.Fatalf("save plan: %+v created=%v err=%v", p, created, err)
	}
	return p
}

func TestOperationIdempotency(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "remind"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	load, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10)
	if err != nil || load.Turn == nil {
		t.Fatalf("load: %v", err)
	}

	wake := time.Now().Add(-time.Second).UTC()
	req := map[string]any{"schedule_id": "s-1", "wake_at": wake.Format(time.RFC3339Nano),
		"payload": map[string]any{"note": "ping"}}
	mustPlan(t, s, pa, "t-1", lease.Generation, PlanCall{Tool: "schedule.set", Route: "normal", Request: req})

	op, _, fresh, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"op-1", "schedule.set", 0, req)
	if err != nil || !fresh || op.Status != "done" {
		t.Fatalf("claim: %+v fresh=%v err=%v", op, fresh, err)
	}
	// Replay after lost response: same plan position and the identical
	// request returns the stored op — even under a new caller operation_id.
	op2, _, fresh2, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"op-2", "schedule.set", 0, req)
	if err != nil || fresh2 || op2.OperationID != "op-1" || op2.Status != "done" {
		t.Fatalf("replay claim: %+v fresh=%v err=%v", op2, fresh2, err)
	}
	// A replayed position carrying a different request is a contract
	// violation — the stored receipt must not be returned for an effect
	// that never ran (and it is off-plan besides).
	_, _, _, err = s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"op-3", "schedule.set", 0,
		map[string]any{"schedule_id": "s-1", "wake_at": wake.Format(time.RFC3339Nano)})
	if !errors.Is(err, ErrTurnConflict) {
		t.Fatalf("divergent replay err = %v, want ErrTurnConflict", err)
	}

	// The internal effect was atomic with the claim: the schedule exists and
	// dispatch fires exactly one wake input.
	fired, err := s.DispatchDueSchedules(ctx, pa, lease.Generation, time.Now(), 10)
	if err != nil || len(fired) != 1 || fired[0].ScheduleID != "s-1" {
		t.Fatalf("dispatch: %+v err=%v", fired, err)
	}
	fired2, err := s.DispatchDueSchedules(ctx, pa, lease.Generation, time.Now(), 10)
	if err != nil || len(fired2) != 0 {
		t.Fatalf("re-dispatch: %+v", fired2)
	}
	in, _, err := s.GetInput(ctx, pa, "sched:s-1")
	if err != nil || in.Status != "queued" || in.Kind != "wake" {
		t.Fatalf("wake input: %+v err=%v", in, err)
	}
}

func TestJournalNoteTool(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	mustPlan(t, s, pa, "t-1", lease.Generation,
		PlanCall{Tool: "journal.note", Route: "normal", Request: map[string]any{"text": "remember this"}})
	op, _, fresh, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"op-n", "journal.note", 0, map[string]any{"text": "remember this"})
	if err != nil || !fresh || op.Status != "done" || op.Response["seq"] == nil {
		t.Fatalf("note claim: %+v fresh=%v err=%v", op, fresh, err)
	}
	// The note follows the input that caused it: the effect journals its
	// input first, so seq order is causal.
	evs, err := s.Events(ctx, pa, 0, 10)
	if err != nil || len(evs) != 2 || evs[0].Kind != "input_received" ||
		evs[0].Payload["input_id"] != "in-1" || evs[1].Kind != "note" {
		t.Fatalf("events: %+v err=%v", evs, err)
	}
}

func TestFailTurnRequeues(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	load, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10)
	if err != nil || load.Turn == nil {
		t.Fatalf("load: %v", err)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-1", lease.Generation, CommitRequest{
		Outcome: "fail", Error: "provider timeout", Retryable: true,
	}); err != nil {
		t.Fatalf("fail commit: %v", err)
	}
	in, _, err := s.GetInput(ctx, pa, "in-1")
	if err != nil || in.Status != "queued" {
		t.Fatalf("requeued input: %+v err=%v", in, err)
	}
	// The retryable requeue parks the input behind its backoff; expire it
	// to reach the retry immediately.
	if _, err := pool.Exec(ctx,
		`UPDATE core_inputs SET not_before = NULL WHERE persona_id = $1`, pa); err != nil {
		t.Fatalf("expire backoff: %v", err)
	}
	load2, err := s.LoadTurn(ctx, pa, lease.Generation, "t-2", 10)
	if err != nil || load2.Turn == nil || load2.Turn.Attempt != 2 {
		t.Fatalf("retry load: %+v err=%v", load2, err)
	}
}

// B1 regression: a released lease must keep its generation reserved. The
// next acquire continues the monotonic sequence, so a mutation in flight
// under the released generation can never be admitted under the new epoch.
func TestGenerationMonotonicAcrossRelease(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)

	l1, err := s.AcquireWriter(ctx, pa, "holder-a", time.Minute)
	if err != nil || l1.Generation != 1 {
		t.Fatalf("acquire: %+v err=%v", l1, err)
	}
	if err := s.ReleaseWriter(ctx, pa, "holder-a", l1.Generation); err != nil {
		t.Fatalf("release: %v", err)
	}
	l2, err := s.AcquireWriter(ctx, pa, "holder-b", time.Minute)
	if err != nil {
		t.Fatalf("reacquire: %v", err)
	}
	if l2.Generation != 2 {
		t.Fatalf("generation recycled: got %d, want 2", l2.Generation)
	}

	// Reproduce the exact reported sequence: B owns a running turn; stale
	// A commits under the recycled generation value.
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	load, err := s.LoadTurn(ctx, pa, l2.Generation, "tB", 10)
	if err != nil || load.Turn == nil {
		t.Fatalf("load: %+v err=%v", load, err)
	}
	_, err = s.CommitTurn(ctx, pa, "tB", l1.Generation, CommitRequest{Outcome: "fail", Error: "stale"})
	if !errors.Is(err, ErrGenerationFence) {
		t.Fatalf("stale-generation commit err = %v, want ErrGenerationFence", err)
	}
	// The live turn is untouched.
	if _, err := s.CommitTurn(ctx, pa, "tB", l2.Generation, CommitRequest{
		Outcome: "complete", Output: map[string]any{"text": "ok"},
	}); err != nil {
		t.Fatalf("live commit: %v", err)
	}
	in, turn, err := s.GetInput(ctx, pa, "in-1")
	if err != nil || in.Status != "done" || turn.Status != "done" {
		t.Fatalf("final state: %+v / %+v err=%v", in, turn, err)
	}
}

// B2 regression: replays return stored results only for identical requests.
func TestConflictingReplaysRejected(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	in := &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "hello"}, ActorKind: "human", ActorID: "h-1",
		SourceSurface: "dev", Attention: "reply"}
	if _, _, err := s.SubmitInput(ctx, in); err != nil {
		t.Fatalf("submit: %v", err)
	}
	// Identical resubmit still replays.
	if _, created, err := s.SubmitInput(ctx, in); err != nil || created {
		t.Fatalf("identical resubmit: created=%v err=%v", created, err)
	}
	// Divergent resubmit is rejected, not absorbed.
	diverged := *in
	diverged.Payload = map[string]any{"text": "different"}
	if _, _, err := s.SubmitInput(ctx, &diverged); !errors.Is(err, ErrTurnConflict) {
		t.Fatalf("divergent input replay err = %v, want ErrTurnConflict", err)
	}
	diverged2 := *in
	diverged2.ActorID = "other"
	if _, _, err := s.SubmitInput(ctx, &diverged2); !errors.Is(err, ErrTurnConflict) {
		t.Fatalf("divergent actor replay err = %v, want ErrTurnConflict", err)
	}

	load, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10)
	if err != nil || load.Turn == nil {
		t.Fatalf("load: %v", err)
	}
	commit := CommitRequest{
		Outcome: "complete",
		Events: []EventInput{
			{Kind: "input_received", Payload: map[string]any{"input_id": "in-1"}},
			{Kind: "assistant_message", Payload: map[string]any{"text": "hi"}},
		},
		Output: map[string]any{"text": "hi"},
	}
	if _, err := s.CommitTurn(ctx, pa, "t-1", lease.Generation, commit); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// Identical re-commit replays the stored turn.
	if _, err := s.CommitTurn(ctx, pa, "t-1", lease.Generation, commit); err != nil {
		t.Fatalf("identical commit replay: %v", err)
	}
	// Divergent re-commit (different output, different events) is rejected.
	divergedCommit := commit
	divergedCommit.Output = map[string]any{"text": "changed"}
	if _, err := s.CommitTurn(ctx, pa, "t-1", lease.Generation, divergedCommit); !errors.Is(err, ErrTurnConflict) {
		t.Fatalf("divergent commit output err = %v, want ErrTurnConflict", err)
	}
	divergedCommit2 := commit
	divergedCommit2.Events = append(append([]EventInput{}, commit.Events...),
		EventInput{Kind: "extra", Payload: map[string]any{}})
	if _, err := s.CommitTurn(ctx, pa, "t-1", lease.Generation, divergedCommit2); !errors.Is(err, ErrTurnConflict) {
		t.Fatalf("divergent commit events err = %v, want ErrTurnConflict", err)
	}
	divergedCommit3 := commit
	divergedCommit3.Outcome = "fail"
	if _, err := s.CommitTurn(ctx, pa, "t-1", lease.Generation, divergedCommit3); !errors.Is(err, ErrTurnConflict) {
		t.Fatalf("divergent commit outcome err = %v, want ErrTurnConflict", err)
	}
	// Dropping the event list entirely is also divergence — the committed
	// request is compared exactly, not inferred from the journaled tail.
	divergedCommit4 := commit
	divergedCommit4.Events = nil
	if _, err := s.CommitTurn(ctx, pa, "t-1", lease.Generation, divergedCommit4); !errors.Is(err, ErrTurnConflict) {
		t.Fatalf("empty-events commit replay err = %v, want ErrTurnConflict", err)
	}
}

// B3 regression: the sched: input namespace is reserved, and dispatch never
// marks a schedule fired when its wake slot is occupied by a foreign row.
func TestSchedNamespaceReserved(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	// Callers cannot occupy the reserved namespace.
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "sched:col", Kind: "message",
		Payload: map[string]any{"text": "squat"}}); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("sched: submit err = %v, want ErrBadRequest", err)
	}

	// Defense in depth: a foreign row (e.g. written before the reservation
	// or by a buggy path) must abort dispatch, not drop the wake silently.
	if _, err := pool.Exec(ctx, `
		INSERT INTO core_inputs (persona_id, input_id, kind, payload, actor_kind, actor_id, source_surface, attention, status)
		VALUES ($1, 'sched:col', 'message', '{}'::jsonb, 'human', 'x', 'dev', 'reply', 'queued')`, pa); err != nil {
		t.Fatalf("seed foreign input: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO core_schedules (persona_id, schedule_id, wake_at, payload, status)
		VALUES ($1, 'col', now() - interval '1 second', '{"text":"wake"}'::jsonb, 'pending')`, pa); err != nil {
		t.Fatalf("seed schedule: %v", err)
	}
	if _, err := s.DispatchDueSchedules(ctx, pa, lease.Generation, time.Now(), 10); !errors.Is(err, ErrTurnConflict) {
		t.Fatalf("dispatch over foreign row err = %v, want ErrTurnConflict", err)
	}
	var status string
	if err := pool.QueryRow(ctx,
		`SELECT status FROM core_schedules WHERE persona_id = $1 AND schedule_id = 'col'`, pa).Scan(&status); err != nil {
		t.Fatalf("schedule status: %v", err)
	}
	if status != "pending" {
		t.Fatalf("schedule marked %q despite dropped wake", status)
	}
	// Clean the foreign row → the same schedule fires normally.
	if _, err := pool.Exec(ctx,
		`DELETE FROM core_inputs WHERE persona_id = $1 AND input_id = 'sched:col'`, pa); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	fired, err := s.DispatchDueSchedules(ctx, pa, lease.Generation, time.Now(), 10)
	if err != nil || len(fired) != 1 {
		t.Fatalf("dispatch after cleanup: %+v err=%v", fired, err)
	}
	in, _, err := s.GetInput(ctx, pa, "sched:col")
	if err != nil || in.Kind != "wake" || in.ActorID != "col" {
		t.Fatalf("wake input: %+v err=%v", in, err)
	}
}

// Operations must be recorded against the live running turn, not a
// nonexistent or finished one, and only against the recorded plan.
func TestClaimOperationTurnBinding(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	// No turn at all.
	if _, _, _, err := s.ClaimOperation(ctx, pa, "ghost", lease.Generation,
		"op-1", "journal.note", 0, map[string]any{"text": "x"}); !errors.Is(err, ErrTurnNotFound) {
		t.Fatalf("claim on missing turn err = %v, want ErrTurnNotFound", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	// Running turn but no recorded plan: claims are not allowed before the
	// decision is durable.
	if _, _, _, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"op-2", "journal.note", 0, map[string]any{"text": "x"}); !errors.Is(err, ErrTurnConflict) {
		t.Fatalf("claim without plan err = %v, want ErrTurnConflict", err)
	}
	mustPlan(t, s, pa, "t-1", lease.Generation,
		PlanCall{Tool: "journal.note", Route: "normal", Request: map[string]any{"text": "x"}})
	if _, err := s.CommitTurn(ctx, pa, "t-1", lease.Generation, CommitRequest{Outcome: "complete"}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// Finished turn.
	if _, _, _, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"op-3", "journal.note", 0, map[string]any{"text": "x"}); !errors.Is(err, ErrTurnConflict) {
		t.Fatalf("claim on finished turn err = %v, want ErrTurnConflict", err)
	}
}

// Only the two state-internal tools are claimable in this slice.
func TestUnknownToolRejected(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	mustPlan(t, s, pa, "t-1", lease.Generation,
		PlanCall{Tool: "http.post", Route: "normal", Request: map[string]any{"url": "https://x"}})
	if _, _, _, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"op-1", "http.post", 0, map[string]any{"url": "https://x"}); !errors.Is(err, ErrUnknownTool) {
		t.Fatalf("unknown tool err = %v, want ErrUnknownTool", err)
	}
}

// A caller-supplied future `now` is a hint, not an override: schedules are
// due only against the service clock.
func TestDispatchClampsCallerNow(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	future := time.Now().Add(365 * 24 * time.Hour)
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	mustPlan(t, s, pa, "t-1", lease.Generation, PlanCall{Tool: "schedule.set", Route: "normal",
		Request: map[string]any{"schedule_id": "far", "wake_at": future.Format(time.RFC3339Nano)}})
	if _, _, _, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation, "op-1", "schedule.set", 0,
		map[string]any{"schedule_id": "far", "wake_at": future.Format(time.RFC3339Nano)}); err != nil {
		t.Fatalf("schedule.set: %v", err)
	}
	// now=+10y must not fire the 1-year-out schedule.
	fired, err := s.DispatchDueSchedules(ctx, pa, lease.Generation, time.Now().Add(10*365*24*time.Hour), 10)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if len(fired) != 0 {
		t.Fatalf("caller-supplied future now fired %+v", fired)
	}
}

func TestListLimitsClamped(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	// Oversized context_limit must not error or scan unboundedly.
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 1<<30); err != nil {
		t.Fatalf("load with huge context limit: %v", err)
	}
	if _, err := s.Events(ctx, pa, 0, 1<<30); err != nil {
		t.Fatalf("events with huge limit: %v", err)
	}
	if _, err := s.Outbox(ctx, pa, 0, 1<<30); err != nil {
		t.Fatalf("outbox with huge limit: %v", err)
	}
}

// F1: the durable plan is written by the live running turn, replayed
// identically, and never superseded by a conflicting decision.
func TestPlanSaveReplayConflict(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	// No turn → no plan authorship.
	if _, _, err := s.SavePlan(ctx, pa, "ghost", lease.Generation, 0,
		Decision{Text: "x", Calls: []PlanCall{}}); !errors.Is(err, ErrTurnNotFound) {
		t.Fatalf("plan on missing turn err = %v, want ErrTurnNotFound", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	dec := Decision{Text: "hi there", Calls: []PlanCall{
		{Tool: "journal.note", Route: "normal", Request: map[string]any{"text": "n1"}},
		{Tool: "schedule.set", Route: "normal", Request: map[string]any{"schedule_id": "s-9",
			"wake_at": time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)}},
	}, Usage: map[string]any{"input_tokens": 7}}
	p, created, err := s.SavePlan(ctx, pa, "t-1", lease.Generation, 0, dec)
	if err != nil || !created || p.InputID != "in-1" || p.TurnID != "t-1" {
		t.Fatalf("save: %+v created=%v err=%v", p, created, err)
	}
	if len(p.Plan) != 1 || len(p.Plan[0].Calls) != 2 || p.Plan[0].Text != "hi there" {
		t.Fatalf("stored plan: %+v", p.Plan)
	}
	// Identical resave replays the stored row (lost-response retry).
	p2, created2, err := s.SavePlan(ctx, pa, "t-1", lease.Generation, 0, dec)
	if err != nil || created2 || p2.TurnID != "t-1" {
		t.Fatalf("identical resave: %+v created=%v err=%v", p2, created2, err)
	}
	// Conflicting decision under the same input is rejected.
	diverged := dec
	diverged.Text = "changed"
	if _, _, err := s.SavePlan(ctx, pa, "t-1", lease.Generation, 0, diverged); !errors.Is(err, ErrTurnConflict) {
		t.Fatalf("divergent plan err = %v, want ErrTurnConflict", err)
	}
	// loadTurn surfaces the recorded plan.
	load, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10)
	if err != nil || load.Plan == nil || load.Plan.Plan[0].Text != "hi there" {
		t.Fatalf("load plan: %+v err=%v", load.Plan, err)
	}
	// A finished turn cannot author a plan.
	if _, err := s.CommitTurn(ctx, pa, "t-1", lease.Generation, CommitRequest{Outcome: "complete"}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, _, err := s.SavePlan(ctx, pa, "t-1", lease.Generation, 0, dec); !errors.Is(err, ErrTurnConflict) {
		t.Fatalf("plan on finished turn err = %v, want ErrTurnConflict", err)
	}
}

// F1 core guarantee: a plan recorded before a crash is continued by the
// next attempt — earlier positions replay receipts, later positions
// execute once; the replacement model output is never consulted.
func TestPlanContinuesAcrossRecovery(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	l1, err := s.AcquireWriter(ctx, pa, "gen1", 30*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire gen1: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, l1.Generation, "t-1", 10); err != nil {
		t.Fatalf("load gen1: %v", err)
	}
	mustPlan(t, s, pa, "t-1", l1.Generation,
		PlanCall{Tool: "journal.note", Route: "normal", Request: map[string]any{"text": "first"}},
		PlanCall{Tool: "journal.note", Route: "normal", Request: map[string]any{"text": "second"}})
	// First effect executes, then the writer dies mid-turn.
	if _, _, _, err := s.ClaimOperation(ctx, pa, "t-1", l1.Generation,
		"op-a", "journal.note", 0, map[string]any{"text": "first"}); err != nil {
		t.Fatalf("claim 0 gen1: %v", err)
	}
	time.Sleep(40 * time.Millisecond)

	l2 := acquireAfterExpiry(t, s, pa, "gen2")
	if _, err := s.Recover(ctx, pa, l2.Generation); err != nil {
		t.Fatalf("recover: %v", err)
	}
	load, err := s.LoadTurn(ctx, pa, l2.Generation, "t-2", 10)
	if err != nil || load.Turn == nil || load.Turn.Attempt != 2 {
		t.Fatalf("load gen2: %+v err=%v", load, err)
	}
	// Attempt 2 sees the recorded plan — not asked to re-plan.
	if load.Plan == nil || len(load.Plan.Plan) != 1 || len(load.Plan.Plan[0].Calls) != 2 || load.Plan.TurnID != "t-1" {
		t.Fatalf("plan on attempt 2: %+v", load.Plan)
	}
	// Position 0 replays attempt 1's receipt under a fresh caller
	// operation_id — server-derived identity means no second effect.
	op, _, fresh, err := s.ClaimOperation(ctx, pa, "t-2", l2.Generation,
		"op-b", "journal.note", 0, map[string]any{"text": "first"})
	if err != nil || fresh || op.OperationID != "op-a" {
		t.Fatalf("replay claim gen2: %+v fresh=%v err=%v", op, fresh, err)
	}
	// Position 1 executes exactly once.
	if _, _, fresh, err := s.ClaimOperation(ctx, pa, "t-2", l2.Generation,
		"op-c", "journal.note", 1, map[string]any{"text": "second"}); err != nil || !fresh {
		t.Fatalf("claim 1 gen2: fresh=%v err=%v", fresh, err)
	}
	// Exactly two note events — one per planned call, no duplicates.
	evs, err := s.Events(ctx, pa, 0, 50)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	notes := 0
	for _, e := range evs {
		if e.Kind == "note" {
			notes++
		}
	}
	if notes != 2 {
		t.Fatalf("note events = %d, want 2", notes)
	}
}

// F1: claims must be entries of the recorded plan — off-plan tool, wrong
// request, out-of-range, or negative index all reject before any effect.
func TestClaimPlanBinding(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	mustPlan(t, s, pa, "t-1", lease.Generation,
		PlanCall{Tool: "journal.note", Route: "normal", Request: map[string]any{"text": "planned"}})
	for _, tc := range []struct {
		name    string
		idx     int
		tool    string
		req     map[string]any
		wantErr error
	}{
		{"off-plan tool", 0, "schedule.set", map[string]any{"text": "planned"}, ErrTurnConflict},
		{"wrong request", 0, "journal.note", map[string]any{"text": "other"}, ErrTurnConflict},
		{"index out of range", 1, "journal.note", map[string]any{"text": "planned"}, ErrTurnConflict},
		{"negative index", -1, "journal.note", map[string]any{"text": "planned"}, ErrBadRequest},
	} {
		if _, _, _, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
			"op-"+tc.name, tc.tool, tc.idx, tc.req); !errors.Is(err, tc.wantErr) {
			t.Fatalf("%s: err = %v, want %v", tc.name, err, tc.wantErr)
		}
	}
	// On-plan call executes.
	op, _, fresh, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"op-ok", "journal.note", 0, map[string]any{"text": "planned"})
	if err != nil || !fresh || op.Status != "done" {
		t.Fatalf("on-plan claim: %+v fresh=%v err=%v", op, fresh, err)
	}
}

// F1 server-owned identity: caller operation IDs are per-attempt labels;
// the durable effect key is (input, call_index) derived server-side.
func TestServerDerivedClaimIdentity(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	mustPlan(t, s, pa, "t-1", lease.Generation,
		PlanCall{Tool: "journal.note", Route: "normal", Request: map[string]any{"text": "once"}})
	if _, _, _, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"op-first", "journal.note", 0, map[string]any{"text": "once"}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	// Same planned call, different caller operation_id: replays the stored
	// receipt — never a second effect.
	op, _, fresh, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"op-second", "journal.note", 0, map[string]any{"text": "once"})
	if err != nil || fresh || op.OperationID != "op-first" {
		t.Fatalf("identity replay: %+v fresh=%v err=%v", op, fresh, err)
	}
	evs, _ := s.Events(ctx, pa, 0, 50)
	notes := 0
	for _, e := range evs {
		if e.Kind == "note" {
			notes++
		}
	}
	if notes != 1 {
		t.Fatalf("note events = %d, want 1 — effect must not duplicate", notes)
	}
	// The stored durable key is server-derived, not caller-chosen.
	if op.IdempotencyKey != "in-1:tool:0" {
		t.Fatalf("idempotency_key = %q, want server-derived in-1:tool:0", op.IdempotencyKey)
	}
}

// A retryable-failed turn does not discard the recorded plan: the requeued
// input's next attempt continues the same decision.
func TestPlanSurvivesRetryableFail(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	mustPlan(t, s, pa, "t-1", lease.Generation,
		PlanCall{Tool: "journal.note", Route: "normal", Request: map[string]any{"text": "keep"}})
	if _, _, _, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"op-1", "journal.note", 0, map[string]any{"text": "keep"}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-1", lease.Generation, CommitRequest{
		Outcome: "fail", Error: "transient", Retryable: true}); err != nil {
		t.Fatalf("fail commit: %v", err)
	}
	// Expire the retry backoff so the next attempt can claim immediately.
	if _, err := pool.Exec(ctx,
		`UPDATE core_inputs SET not_before = NULL WHERE persona_id = $1`, pa); err != nil {
		t.Fatalf("expire backoff: %v", err)
	}
	load, err := s.LoadTurn(ctx, pa, lease.Generation, "t-2", 10)
	if err != nil || load.Turn == nil || load.Turn.Attempt != 2 {
		t.Fatalf("retry load: %+v err=%v", load, err)
	}
	if load.Plan == nil || load.Plan.Plan[0].Calls[0].Tool != "journal.note" {
		t.Fatalf("plan lost across retryable fail: %+v", load.Plan)
	}
	// The already-executed call replays its receipt on the new attempt.
	op, _, fresh, err := s.ClaimOperation(ctx, pa, "t-2", lease.Generation,
		"op-2", "journal.note", 0, map[string]any{"text": "keep"})
	if err != nil || fresh || op.OperationID != "op-1" {
		t.Fatalf("retry replay: %+v fresh=%v err=%v", op, fresh, err)
	}
}

// acquireAfterExpiry waits out a short-lived lease and acquires under the
// new holder. Lease expiry is judged on the database clock — which can step
// under a loaded host — so a fixed client-side sleep is not proof the lease
// is dead; poll until the store agrees.
func acquireAfterExpiry(t *testing.T, s *Store, pa, holder string) WriterLease {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		l, err := s.AcquireWriter(context.Background(), pa, holder, time.Minute)
		if err == nil {
			return l
		}
		if !errors.Is(err, ErrWriterHeld) || time.Now().After(deadline) {
			t.Fatalf("acquire after lease expiry: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Multi-round plan: a later round is appended after earlier rounds' effects
// committed — including by a new attempt after recovery — and claims address
// flat positions across all rounds.
func TestPlanRoundsAppendAcrossAttempts(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	l1, err := s.AcquireWriter(ctx, pa, "gen1", 30*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire gen1: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, l1.Generation, "t-1", 10); err != nil {
		t.Fatalf("load gen1: %v", err)
	}
	mustPlan(t, s, pa, "t-1", l1.Generation,
		PlanCall{Tool: "journal.note", Route: "normal", Request: map[string]any{"text": "r0-note"}})
	// The round-0 effect commits; the writer dies before consulting round 1.
	if _, _, fresh, err := s.ClaimOperation(ctx, pa, "t-1", l1.Generation,
		"op-a", "journal.note", 0, map[string]any{"text": "r0-note"}); err != nil || !fresh {
		t.Fatalf("claim gen1: fresh=%v err=%v", fresh, err)
	}
	time.Sleep(40 * time.Millisecond)

	l2 := acquireAfterExpiry(t, s, pa, "gen2")
	if _, err := s.Recover(ctx, pa, l2.Generation); err != nil {
		t.Fatalf("recover: %v", err)
	}
	load, err := s.LoadTurn(ctx, pa, l2.Generation, "t-2", 10)
	if err != nil || load.Plan == nil || len(load.Plan.Plan) != 1 {
		t.Fatalf("load gen2: %+v err=%v", load, err)
	}
	// Position 0 replays under the new attempt.
	if op, _, fresh, err := s.ClaimOperation(ctx, pa, "t-2", l2.Generation,
		"op-b", "journal.note", 0, map[string]any{"text": "r0-note"}); err != nil || fresh || op.OperationID != "op-a" {
		t.Fatalf("replay gen2: fresh=%v err=%v", fresh, err)
	}
	// The new attempt appends round 1 to the same durable record.
	p, created, err := s.SavePlan(ctx, pa, "t-2", l2.Generation, 1,
		Decision{Text: "final reply", Calls: []PlanCall{
			{Tool: "journal.note", Route: "normal", Request: map[string]any{"text": "r1-note"}},
		}})
	if err != nil || !created || len(p.Plan) != 2 {
		t.Fatalf("append round 1: %+v created=%v err=%v", p, created, err)
	}
	// Round 1's call is flat position 1 — it claims and executes once.
	if _, _, fresh, err := s.ClaimOperation(ctx, pa, "t-2", l2.Generation,
		"op-c", "journal.note", 1, map[string]any{"text": "r1-note"}); err != nil || !fresh {
		t.Fatalf("claim flat pos 1: fresh=%v err=%v", fresh, err)
	}
	// Skipping a round and rewriting a recorded round both conflict.
	if _, _, err := s.SavePlan(ctx, pa, "t-2", l2.Generation, 3,
		Decision{Text: "gap"}); !errors.Is(err, ErrTurnConflict) {
		t.Fatalf("gap round err = %v, want ErrTurnConflict", err)
	}
	if _, _, err := s.SavePlan(ctx, pa, "t-2", l2.Generation, 0,
		Decision{Text: "rewrite", Calls: []PlanCall{}}); !errors.Is(err, ErrTurnConflict) {
		t.Fatalf("rewrite round err = %v, want ErrTurnConflict", err)
	}
	// Identical resend of round 1 replays.
	if _, created, err := s.SavePlan(ctx, pa, "t-2", l2.Generation, 1,
		Decision{Text: "final reply", Calls: []PlanCall{
			{Tool: "journal.note", Route: "normal", Request: map[string]any{"text": "r1-note"}},
		}}); err != nil || created {
		t.Fatalf("resend round 1: created=%v err=%v", created, err)
	}
}

// CR3-B1: deterministically invalid tool data is a 400-class rejection at
// the plan/claim boundaries, never a retryable 500 — the input resolves
// with a recorded error instead of blocking every later input forever.
func TestDeterministicToolDataRejected(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10); err != nil {
		t.Fatalf("load: %v", err)
	}

	// A decision containing NUL cannot be persisted — rejected at SavePlan,
	// the first persistence boundary, before any effect is reached.
	if _, _, err := s.SavePlan(ctx, pa, "t-1", lease.Generation, 0,
		Decision{Text: "reply\x00stop"}); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("NUL text savePlan err = %v, want ErrBadRequest", err)
	}
	if _, _, err := s.SavePlan(ctx, pa, "t-1", lease.Generation, 0,
		Decision{Text: "ok", Calls: []PlanCall{
			{Tool: "journal.note", Route: "normal", Request: map[string]any{"text": "a\x00b"}},
		}}); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("NUL request savePlan err = %v, want ErrBadRequest", err)
	}
	// The rejected saves recorded nothing: the same turn can still store a
	// clean decision.
	badPolicyReq := map[string]any{
		"schedule_id": "rem",
		"wake_at":     time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano),
		"miss_policy": "bogus",
	}
	mustPlan(t, s, pa, "t-1", lease.Generation,
		PlanCall{Tool: "schedule.set", Route: "normal", Request: badPolicyReq})

	// A plan-valid but semantically invalid argument (miss_policy not in
	// the enum) is rejected as a bad request, not a constraint 500.
	if _, _, _, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"op-bad-policy", "schedule.set", 0, badPolicyReq); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("bogus miss_policy claim err = %v, want ErrBadRequest", err)
	}
	// A NUL in the claim request is a 400 before plan binding — it can
	// never match or execute.
	if _, _, _, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"op-nul", "journal.note", 0,
		map[string]any{"text": "a\u0000b"}); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("NUL claim err = %v, want ErrBadRequest", err)
	}
}

// CR3-B2: reusing a schedule_id must not silently return the old row as a
// fresh success. Identical contents over a still-pending row replay;
// different contents or a dead schedule are explicit tool errors.
func TestScheduleSetIDReuse(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	wake1 := time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond)
	req1 := map[string]any{
		"schedule_id": "rem",
		"wake_at":     wake1.Format(time.RFC3339Nano),
		"payload":     map[string]any{"text": "hi"},
		"miss_policy": "coalesce",
	}
	// in-1 sets the schedule.
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10); err != nil {
		t.Fatalf("load t-1: %v", err)
	}
	mustPlan(t, s, pa, "t-1", lease.Generation, PlanCall{Tool: "schedule.set", Route: "normal", Request: req1})
	op, _, fresh, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"op-1", "schedule.set", 0, req1)
	if err != nil || !fresh || op.Status != "done" {
		t.Fatalf("initial schedule.set: %+v fresh=%v err=%v", op, fresh, err)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-1", lease.Generation,
		CommitRequest{Outcome: "complete"}); err != nil {
		t.Fatalf("commit t-1: %v", err)
	}

	// in-2 reuses the id with different contents → bad request, row kept.
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-2", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit in-2: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-2", 10); err != nil {
		t.Fatalf("load t-2: %v", err)
	}
	req2 := map[string]any{
		"schedule_id": "rem",
		"wake_at":     wake1.Add(24 * time.Hour).Format(time.RFC3339Nano),
		"payload":     map[string]any{"text": "hi"},
		"miss_policy": "coalesce",
	}
	mustPlan(t, s, pa, "t-2", lease.Generation, PlanCall{Tool: "schedule.set", Route: "normal", Request: req2})
	if _, _, _, err := s.ClaimOperation(ctx, pa, "t-2", lease.Generation,
		"op-2", "schedule.set", 0, req2); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("conflicting reuse err = %v, want ErrBadRequest", err)
	}
	var storedWake time.Time
	var status, mp string
	if err := pool.QueryRow(ctx,
		`SELECT wake_at, status, miss_policy FROM core_schedules WHERE persona_id = $1 AND schedule_id = 'rem'`,
		pa).Scan(&storedWake, &status, &mp); err != nil {
		t.Fatalf("read schedule: %v", err)
	}
	if !storedWake.Equal(wake1) || status != "pending" || mp != "coalesce" {
		t.Fatalf("existing schedule mutated: wake=%v status=%s miss_policy=%s", storedWake, status, mp)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-2", lease.Generation,
		CommitRequest{Outcome: "fail", Error: "tool error recorded"}); err != nil {
		t.Fatalf("commit t-2: %v", err)
	}

	// in-3 reuses the id with identical contents while pending → true
	// idempotent set: the existing row is returned, no duplicate.
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-3", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit in-3: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-3", 10); err != nil {
		t.Fatalf("load t-3: %v", err)
	}
	mustPlan(t, s, pa, "t-3", lease.Generation, PlanCall{Tool: "schedule.set", Route: "normal", Request: req1})
	op, _, fresh, err = s.ClaimOperation(ctx, pa, "t-3", lease.Generation,
		"op-3", "schedule.set", 0, req1)
	if err != nil || !fresh || op.Status != "done" {
		t.Fatalf("identical pending reuse: %+v fresh=%v err=%v", op, fresh, err)
	}
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM core_schedules WHERE persona_id = $1 AND schedule_id = 'rem'`, pa).
		Scan(&count); err != nil || count != 1 {
		t.Fatalf("schedule rows = %d err=%v, want 1", count, err)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-3", lease.Generation,
		CommitRequest{Outcome: "complete"}); err != nil {
		t.Fatalf("commit t-3: %v", err)
	}

	// Fire it, then an identical reuse must not report a stale success.
	fired, err := s.DispatchDueSchedules(ctx, pa, lease.Generation, time.Now(), 10)
	if err != nil || len(fired) != 1 {
		t.Fatalf("dispatch: %+v err=%v", fired, err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-4", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit in-4: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-4", 10); err != nil {
		t.Fatalf("load t-4: %v", err)
	}
	mustPlan(t, s, pa, "t-4", lease.Generation, PlanCall{Tool: "schedule.set", Route: "normal", Request: req1})
	if _, _, _, err := s.ClaimOperation(ctx, pa, "t-4", lease.Generation,
		"op-4", "schedule.set", 0, req1); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("fired schedule reuse err = %v, want ErrBadRequest", err)
	}
}

// Fresh-review F1: a commit whose payload PostgreSQL can never store is
// a deterministic 400 at the store boundary — so the core records an
// honest failure instead of leaving the input claimed forever.
// (Adapted from foundation repair b4cdc722: on this branch the stored
// error text is diagnostic and NUL-stripped before marshalling, so a
// NUL-bearing error commits successfully rather than 400ing — the 400 is
// reserved for record content: events, output, usage.)
func TestCommitUnstorablePayloadRejected(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	// NUL in committed record content — an event payload or the output —
	// cannot persist; each variant is a deterministic 400.
	if _, err := s.CommitTurn(ctx, pa, "t-1", lease.Generation, CommitRequest{
		Outcome: "complete", Events: []EventInput{
			{Kind: "assistant_message", Payload: map[string]any{"text": "a\u0000b"}},
		}, Output: map[string]any{"text": "a\u0000b"},
	}); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("NUL output commit err = %v, want ErrBadRequest", err)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-1", lease.Generation, CommitRequest{
		Outcome: "fail", Retryable: true, Events: []EventInput{
			{Kind: "assistant_message", Payload: map[string]any{"text": "a\u0000b"}},
		},
	}); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("NUL event commit err = %v, want ErrBadRequest", err)
	}
	// A NUL-bearing error string is diagnostic, not record content: it is
	// stripped and the retryable failure commits — the input requeues
	// with backoff rather than dying at the boundary.
	tr, err := s.CommitTurn(ctx, pa, "t-1", lease.Generation, CommitRequest{
		Outcome: "fail", Retryable: true, Error: "model: \u0000 exploded",
	})
	if err != nil || tr.Status != "failed" {
		t.Fatalf("NUL error commit should succeed stripped: %+v err=%v", tr, err)
	}
	if tr.Error == nil || strings.Contains(*tr.Error, "\u0000") {
		t.Fatalf("error not stripped: %v", tr.Error)
	}
	// Nothing from the rejected commits landed: the turn requeued
	// retryably, parked behind not_before.
	in, _, err := s.GetInput(ctx, pa, "in-1")
	if err != nil || in.Status != "queued" || in.NotBefore == nil {
		t.Fatalf("input after NUL-error commit: %+v err=%v", in, err)
	}
}

// Fresh-review F2: a retryable failure requeues with backoff (not_before)
// instead of instantly reclaiming — the queue stays fair and the retry
// rate is bounded.
func TestRetryableFailureRequeuesWithBackoff(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	submit := func(id string) {
		t.Helper()
		if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: id, Kind: "message",
			Payload: map[string]any{"text": "x"}}); err != nil {
			t.Fatalf("submit %s: %v", id, err)
		}
	}
	submit("in-bad")
	submit("in-good")
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10); err != nil {
		t.Fatalf("load t-1: %v", err)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-1", lease.Generation, CommitRequest{
		Outcome: "fail", Retryable: true, Error: "provider down",
	}); err != nil {
		t.Fatalf("retryable commit: %v", err)
	}
	// The failed input is queued but parked behind its backoff — and a
	// later queued input claims first instead of starving behind it.
	var nb time.Time
	if err := pool.QueryRow(ctx,
		`SELECT not_before FROM core_inputs WHERE persona_id = $1 AND input_id = 'in-bad'`,
		pa).Scan(&nb); err != nil {
		t.Fatalf("read not_before: %v", err)
	}
	if !nb.After(time.Now()) {
		t.Fatalf("not_before = %v, want a future backoff", nb)
	}
	res, err := s.LoadTurn(ctx, pa, lease.Generation, "t-2", 10)
	if err != nil {
		t.Fatalf("load t-2: %v", err)
	}
	if res.Input == nil || res.Input.InputID != "in-good" {
		t.Fatalf("claim during backoff took %+v, want in-good", res.Input)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-2", lease.Generation, CommitRequest{
		Outcome: "complete",
	}); err != nil {
		t.Fatalf("complete in-good: %v", err)
	}
	// Backoff grows with the input's attempt count: failing in-bad a
	// second time parks it for longer than the first.
	expire := func() {
		t.Helper()
		if _, err := pool.Exec(ctx,
			`UPDATE core_inputs SET not_before = now() - interval '1 second'
			 WHERE persona_id = $1 AND status = 'queued'`, pa); err != nil {
			t.Fatalf("expire not_before: %v", err)
		}
	}
	expire()
	res, err = s.LoadTurn(ctx, pa, lease.Generation, "t-3", 10)
	if err != nil {
		t.Fatalf("load t-3: %v", err)
	}
	if res.Input == nil || res.Input.InputID != "in-bad" || res.Turn.Attempt != 2 {
		t.Fatalf("post-backoff claim took %+v, want in-bad attempt 2", res.Input)
	}
	before := time.Now()
	if _, err := s.CommitTurn(ctx, pa, "t-3", lease.Generation, CommitRequest{
		Outcome: "fail", Retryable: true, Error: "provider down",
	}); err != nil {
		t.Fatalf("second retryable commit: %v", err)
	}
	var nb2 time.Time
	if err := pool.QueryRow(ctx,
		`SELECT not_before FROM core_inputs WHERE persona_id = $1 AND input_id = 'in-bad'`,
		pa).Scan(&nb2); err != nil {
		t.Fatalf("read not_before 2: %v", err)
	}
	if nb2.Sub(before) <= nb.Sub(before) {
		t.Fatalf("backoff did not grow: attempt1=%v attempt2=%v", nb, nb2)
	}
}

// Fresh-review F1: provider-supplied pacing (Retry-After) flows through
// CommitRequest.retry_after_ms into the requeue's not_before — honored on
// top of the per-attempt backoff, and clamped so a hint can never silence
// a request.
func TestRetryAfterMsPacesRequeue(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	lease, err := s.AcquireWriter(ctx, pa, "h", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-ra", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-ra", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	before := time.Now()
	if _, err := s.CommitTurn(ctx, pa, "t-ra", lease.Generation, CommitRequest{
		Outcome: "fail", Retryable: true, Error: "rate limited", RetryAfterMs: 5_000,
	}); err != nil {
		t.Fatalf("retryable commit: %v", err)
	}
	var nb time.Time
	if err := pool.QueryRow(ctx,
		`SELECT not_before FROM core_inputs WHERE persona_id = $1 AND input_id = 'in-ra'`,
		pa).Scan(&nb); err != nil {
		t.Fatalf("read not_before: %v", err)
	}
	if d := nb.Sub(before); d < 4*time.Second || d > 6*time.Second {
		t.Fatalf("retry_after_ms must dominate the backoff (~5s), got %v", d)
	}
	// An absurd hint is clamped — it can slow a retry, never silence it.
	if _, err := pool.Exec(ctx,
		`UPDATE core_inputs SET not_before = now() - interval '1 second'
		 WHERE persona_id = $1 AND input_id = 'in-ra'`, pa); err != nil {
		t.Fatalf("expire not_before: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-ra2", 10); err != nil {
		t.Fatalf("reload: %v", err)
	}
	before = time.Now()
	if _, err := s.CommitTurn(ctx, pa, "t-ra2", lease.Generation, CommitRequest{
		Outcome: "fail", Retryable: true, Error: "rate limited",
		RetryAfterMs: 3_600_000, // provider asks for an hour
	}); err != nil {
		t.Fatalf("second retryable commit: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT not_before FROM core_inputs WHERE persona_id = $1 AND input_id = 'in-ra'`,
		pa).Scan(&nb); err != nil {
		t.Fatalf("read not_before: %v", err)
	}
	if d := nb.Sub(before); d > 3*time.Minute {
		t.Fatalf("an absurd Retry-After must be clamped to 2min, got %v", d)
	}
}
