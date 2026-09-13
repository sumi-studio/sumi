package agentstate

import (
	"context"
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

	l2, err := s.AcquireWriter(ctx, pa, "gen2", time.Minute)
	if err != nil {
		t.Fatalf("acquire gen2: %v", err)
	}
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
	op, fresh, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"op-1", "schedule.set", "t-1:schedule.set:0",
		map[string]any{"schedule_id": "s-1", "wake_at": wake.Format(time.RFC3339Nano),
			"payload": map[string]any{"note": "ping"}})
	if err != nil || !fresh || op.Status != "done" {
		t.Fatalf("claim: %+v fresh=%v err=%v", op, fresh, err)
	}
	// Replay after lost response: same idempotency key returns stored op.
	op2, fresh2, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"op-2", "schedule.set", "t-1:schedule.set:0",
		map[string]any{"schedule_id": "s-1", "wake_at": wake.Format(time.RFC3339Nano)})
	if err != nil || fresh2 || op2.OperationID != "op-1" || op2.Status != "done" {
		t.Fatalf("replay claim: %+v fresh=%v err=%v", op2, fresh2, err)
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
	op, fresh, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"op-n", "journal.note", "k1", map[string]any{"text": "remember this"})
	if err != nil || !fresh || op.Status != "done" || op.Response["seq"] == nil {
		t.Fatalf("note claim: %+v fresh=%v err=%v", op, fresh, err)
	}
	evs, err := s.Events(ctx, pa, 0, 10)
	if err != nil || len(evs) != 1 || evs[0].Kind != "note" {
		t.Fatalf("events: %+v err=%v", evs, err)
	}
}

func TestFailTurnRequeues(t *testing.T) {
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
	load2, err := s.LoadTurn(ctx, pa, lease.Generation, "t-2", 10)
	if err != nil || load2.Turn == nil || load2.Turn.Attempt != 2 {
		t.Fatalf("retry load: %+v err=%v", load2, err)
	}
}
