package agentstate

import (
	"context"
	"errors"
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
	// Replay after lost response: same idempotency key AND the identical
	// request returns the stored op.
	req := map[string]any{"schedule_id": "s-1", "wake_at": wake.Format(time.RFC3339Nano),
		"payload": map[string]any{"note": "ping"}}
	op2, fresh2, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"op-2", "schedule.set", "t-1:schedule.set:0", req)
	if err != nil || fresh2 || op2.OperationID != "op-1" || op2.Status != "done" {
		t.Fatalf("replay claim: %+v fresh=%v err=%v", op2, fresh2, err)
	}
	// A replayed key carrying a different request is a contract violation —
	// the stored receipt must not be returned for an effect that never ran.
	_, _, err = s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"op-3", "schedule.set", "t-1:schedule.set:0",
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
// nonexistent or finished one.
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
	if _, _, err := s.ClaimOperation(ctx, pa, "ghost", lease.Generation,
		"op-1", "journal.note", "k1", map[string]any{"text": "x"}); !errors.Is(err, ErrTurnNotFound) {
		t.Fatalf("claim on missing turn err = %v, want ErrTurnNotFound", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-1", Kind: "message",
		Payload: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, lease.Generation, "t-1", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-1", lease.Generation, CommitRequest{Outcome: "complete"}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// Finished turn.
	if _, _, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"op-2", "journal.note", "k2", map[string]any{"text": "x"}); !errors.Is(err, ErrTurnConflict) {
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
	if _, _, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation,
		"op-1", "http.post", "k1", map[string]any{"url": "https://x"}); !errors.Is(err, ErrUnknownTool) {
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
	if _, _, err := s.ClaimOperation(ctx, pa, "t-1", lease.Generation, "op-1", "schedule.set", "k1",
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
