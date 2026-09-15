package agentstate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// --- fixtures --------------------------------------------------------------
// mustHuman comes from approvals_test.go (same package).

func mustConnection(t *testing.T, pool *pgxpool.Pool, humanID string) string {
	t.Helper()
	connID := pid(t)
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO model_api_connections
			(human_id, connection_id, name, preset, base_url, model,
			 credential_ciphertext, version)
		VALUES ($1, $2::uuid, 'test conn', 'openai-chat',
			'https://provider.example/v1', 'fixture-model',
			'key', $3::uuid)`,
		humanID, connID, pid(t)); err != nil {
		t.Fatalf("insert connection: %v", err)
	}
	return connID
}

// boundPersona creates a persona bound to a fresh human and returns both.
func boundPersona(t *testing.T, s *Store, pool *pgxpool.Pool) (persona, human string) {
	t.Helper()
	human = mustHuman(t, pool)
	persona = pid(t)
	if _, _, err := s.EnsurePersona(context.Background(), persona, &human, ""); err != nil {
		t.Fatalf("ensure persona: %v", err)
	}
	return persona, human
}

func connFunding(id string) FundingRef {
	return FundingRef{Kind: "connection", ID: id, Model: "fixture-model"}
}

// fixtureBudget: 1 minor unit per million input tokens, 2 per million
// output — labelled fixture rates, not a provider price list.
func fixtureBudget(limit int64) UsageBudget {
	return UsageBudget{
		LimitMinor:        limit,
		Currency:          "USD",
		RateInputPerMTok:  1_000_000, // 1 unit per token → easy arithmetic
		RateOutputPerMTok: 2_000_000,
		PricingRevision:   "fixture-rates-v1",
	}
}

func admitReq(factID string, gen int64, funding FundingRef, inTok, outBound int64) UsageAdmitRequest {
	req := UsageAdmitRequest{
		Generation: gen,
		FactID:     factID,
		Kind:       "model_call",
		Phase:      "turn",
		Funding:    funding,
		Estimate:   UsageEstimate{InputTokens: inTok},
	}
	if outBound > 0 {
		req.Estimate.OutputTokensBound = &outBound
	}
	return req
}

// --- tests -----------------------------------------------------------------

// A recorded call is inspectable with its funding identity; redelivery of
// the same fact replays the stored row instead of double counting, and a
// conflicting payload under a known fact_id is a contract violation.
func TestUsageRecordIdempotentAndConflicting(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	conn := mustConnection(t, pool, human)
	gen := acquireWriter(t, s, pa, time.Minute)

	res, err := s.AdmitUsage(ctx, pa, admitReq("f-1", gen, connFunding(conn), 1000, 500))
	if err != nil || !res.Admitted || res.Reservation == nil {
		t.Fatalf("admit: %+v err=%v", res, err)
	}
	in, out, cached := int64(1000), int64(400), int64(200)
	rec := UsageRecordRequest{
		FactID: "f-1", Kind: "model_call", Phase: "turn", TurnID: "turn-1",
		InputID: "in-1", Round: 0, Funding: connFunding(conn),
		Status: "reported", InputTokens: &in, OutputTokens: &out,
		CachedTokens: &cached, Quantities: map[string]any{"prompt_tokens": 1000},
	}
	fact, created, err := s.RecordUsage(ctx, pa, rec)
	if err != nil || !created {
		t.Fatalf("record: %+v created=%v err=%v", fact, created, err)
	}
	if fact.Funding.Kind != "connection" || fact.Funding.ID != conn ||
		fact.Funding.Model != "fixture-model" {
		t.Fatalf("funding attribution: %+v", fact.Funding)
	}
	// Redelivery of the identical record replays, does not duplicate.
	again, created2, err := s.RecordUsage(ctx, pa, rec)
	if err != nil || created2 || again.FactID != "f-1" {
		t.Fatalf("redelivery: created=%v err=%v", created2, err)
	}
	// A conflicting payload under the same fact id is a contract violation.
	bad := rec
	badOut := int64(999)
	bad.OutputTokens = &badOut
	if _, _, err := s.RecordUsage(ctx, pa, bad); !errors.Is(err, ErrUsageFactConflict) {
		t.Fatalf("conflicting record err=%v, want ErrUsageFactConflict", err)
	}
	facts, err := s.ListUsageFacts(ctx, pa, 10)
	if err != nil || len(facts) != 1 {
		t.Fatalf("ledger after redelivery+conflict: %d facts err=%v", len(facts), err)
	}
	// The reservation settled when its fact landed.
	var status string
	if err := pool.QueryRow(ctx,
		`SELECT status FROM usage_reservations WHERE persona_id=$1 AND fact_id='f-1'`,
		pa).Scan(&status); err != nil || status != "settled" {
		t.Fatalf("reservation status=%q err=%v, want settled", status, err)
	}
}

// A call whose provider usage never resolved records 'unknown' — visible
// in the ledger, never silently zero — and keeps its admission estimate
// as explicitly estimated spend: an attempted call may have consumed
// money, so its hold must not silently restore the allowance.
func TestUsageUnknownFact(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	conn := mustConnection(t, pool, human)
	gen := acquireWriter(t, s, pa, time.Minute)
	if _, err := s.SetBudget(ctx, human, "connection", conn, fixtureBudget(1_000_000)); err != nil {
		t.Fatalf("set budget: %v", err)
	}
	// 500*1 + 500*2 = 1500 estimated.
	if _, err := s.AdmitUsage(ctx, pa, admitReq("f-lost", gen, connFunding(conn), 500, 500)); err != nil {
		t.Fatalf("admit: %v", err)
	}
	fact, created, err := s.RecordUsage(ctx, pa, UsageRecordRequest{
		FactID: "f-lost", Kind: "model_call", Phase: "turn", TurnID: "t",
		Funding: connFunding(conn), Status: "unknown",
		Quantities: map[string]any{},
	})
	if err != nil || !created || fact.Status != "unknown" {
		t.Fatalf("record unknown: %+v created=%v err=%v", fact, created, err)
	}
	if fact.InputTokens != nil {
		t.Fatalf("unknown fact carries no reported tokens: %+v", fact)
	}
	if fact.CostMinor == nil || *fact.CostMinor != 1500 ||
		fact.CostBasis == nil || *fact.CostBasis != "admission_estimate" {
		t.Fatalf("unknown fact must retain the estimate as uncertain spend: %+v", fact)
	}
	// The estimate counts against the cap — repeated unknown calls cannot
	// cycle the same allowance.
	spent, held, _ := s.fundingSpend(ctx, s.pool, "connection", conn, "USD")
	if spent != 1500 || held != 0 {
		t.Fatalf("spent=%d held=%d, want 1500/0", spent, held)
	}
	facts, _ := s.ListUsageFacts(ctx, pa, 10)
	if len(facts) != 1 || facts[0].Status != "unknown" {
		t.Fatalf("unknown fact inspectable: %+v", facts)
	}
}

// A call the core knows was never produced releases its reservation —
// 'not_sent' is the only path that frees held spend without a report.
func TestUsageNotSentReleases(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	conn := mustConnection(t, pool, human)
	gen := acquireWriter(t, s, pa, time.Minute)
	if _, err := s.SetBudget(ctx, human, "connection", conn, fixtureBudget(1_000_000)); err != nil {
		t.Fatalf("set budget: %v", err)
	}
	if _, err := s.AdmitUsage(ctx, pa, admitReq("f-ns", gen, connFunding(conn), 500, 500)); err != nil {
		t.Fatalf("admit: %v", err)
	}
	fact, created, err := s.RecordUsage(ctx, pa, UsageRecordRequest{
		FactID: "f-ns", Kind: "model_call", Phase: "turn",
		Funding: connFunding(conn), Status: "not_sent",
		Quantities: map[string]any{},
	})
	if err != nil || !created || fact.Status != "not_sent" || fact.CostMinor != nil {
		t.Fatalf("not_sent record: %+v created=%v err=%v", fact, created, err)
	}
	var status string
	if err := pool.QueryRow(ctx,
		`SELECT status FROM usage_reservations WHERE persona_id=$1 AND fact_id='f-ns'`,
		pa).Scan(&status); err != nil || status != "released" {
		t.Fatalf("reservation=%q err=%v, want released", status, err)
	}
	spent, held, _ := s.fundingSpend(ctx, s.pool, "connection", conn, "USD")
	if spent != 0 || held != 0 {
		t.Fatalf("spent=%d held=%d after not_sent, want 0/0", spent, held)
	}
}

// Facts recorded under one connection keep that attribution forever —
// creating/selecting a different connection later never rewrites them.
func TestUsageAttributionSurvivesConnectionChange(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	connA := mustConnection(t, pool, human)
	gen := acquireWriter(t, s, pa, time.Minute)

	if _, err := s.AdmitUsage(ctx, pa, admitReq("f-a", gen, connFunding(connA), 10, 10)); err != nil {
		t.Fatalf("admit A: %v", err)
	}
	in, out := int64(10), int64(5)
	if _, _, err := s.RecordUsage(ctx, pa, UsageRecordRequest{
		FactID: "f-a", Kind: "model_call", Phase: "turn",
		Funding: connFunding(connA), Status: "reported",
		InputTokens: &in, OutputTokens: &out, Quantities: map[string]any{},
	}); err != nil {
		t.Fatalf("record A: %v", err)
	}
	// The human adds a second connection — prior facts stay on A.
	mustConnection(t, pool, human)
	facts, _ := s.ListUsageFacts(ctx, pa, 10)
	if len(facts) != 1 || facts[0].Funding.ID != connA {
		t.Fatalf("reattribution: %+v", facts)
	}
}

// A persona cannot admit or attribute spend to another human's connection.
func TestUsageCrossHumanFundingDenied(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, _ := boundPersona(t, s, pool)
	_, otherHuman := boundPersona(t, s, pool)
	otherConn := mustConnection(t, pool, otherHuman)
	gen := acquireWriter(t, s, pa, time.Minute)

	_, err := s.AdmitUsage(ctx, pa, admitReq("f-x", gen, connFunding(otherConn), 10, 10))
	if !errors.Is(err, ErrFundingNotFound) {
		t.Fatalf("cross-human admit err=%v, want ErrFundingNotFound", err)
	}
	// Recording fabricated spend against another human's funding is denied
	// the same way — no reservation exists to prove prior admission.
	in, out := int64(1), int64(1)
	_, _, err = s.RecordUsage(ctx, pa, UsageRecordRequest{
		FactID: "f-x", Kind: "model_call", Phase: "turn",
		Funding: connFunding(otherConn), Status: "reported",
		InputTokens: &in, OutputTokens: &out, Quantities: map[string]any{},
	})
	if !errors.Is(err, ErrFundingNotFound) {
		t.Fatalf("cross-human record err=%v, want ErrFundingNotFound", err)
	}
}

// Concurrent admits racing for one configured cap are serialized on the
// budget row: exactly the calls that fit are admitted, the rest denied.
func TestUsageConcurrentAdmitsCannotOverspend(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	conn := mustConnection(t, pool, human)
	gen := acquireWriter(t, s, pa, time.Minute)

	// Cap fits exactly 3 reservations of 10 needed units each.
	if _, err := s.SetBudget(ctx, human, "connection", conn, fixtureBudget(30)); err != nil {
		t.Fatalf("set budget: %v", err)
	}
	// input=6 + output bound=2: 6*1 + 2*2 = 10 units per call.
	const contenders = 8
	results := make(chan UsageAdmitResult, contenders)
	errs := make(chan error, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := s.AdmitUsage(ctx, pa,
				admitReq(fmt.Sprintf("f-c%d", i), gen, connFunding(conn), 6, 2))
			if err != nil {
				errs <- err
				return
			}
			results <- res
		}(i)
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent admit err=%v", err)
	}
	admitted, denied := 0, 0
	for res := range results {
		if res.Admitted {
			admitted++
		} else {
			denied++
			if res.Wait == nil || res.Wait.RemainingMinor < 10 {
				// remaining < needed is the honest reason
				if res.Wait == nil {
					t.Fatal("denial must carry a wait")
				}
			}
		}
	}
	if admitted != 3 || denied != contenders-3 {
		t.Fatalf("admitted=%d denied=%d, want 3/%d", admitted, denied, contenders-3)
	}
	// Held reservations account for exactly the admitted spend.
	_, held, err := s.fundingSpend(ctx, s.pool, "connection", conn, "USD")
	if err != nil || held != 30 {
		t.Fatalf("held=%d err=%v, want 30", held, err)
	}
}

// A denied admission commits 'await' with the budget wait: the input
// parks durably, the wait is inspectable, and a budget increase resumes
// it without a duplicate turn.
func TestUsageBudgetWaitCommitAndResume(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	conn := mustConnection(t, pool, human)
	selectConnection(t, pool, human, conn)
	gen := acquireWriter(t, s, pa, time.Minute)

	if _, err := s.SetBudget(ctx, human, "connection", conn, fixtureBudget(5)); err != nil {
		t.Fatalf("set budget: %v", err)
	}
	in := &Input{PersonaID: pa, InputID: "in-w", Kind: "message",
		Payload: map[string]any{"text": "hi"}, ActorKind: "human", ActorID: human,
		SourceSurface: "test", Attention: "reply"}
	if _, _, err := s.SubmitInput(ctx, in); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, gen, "t-w", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	// Denied: 10 needed > 5 limit.
	res, err := s.AdmitUsage(ctx, pa, admitReq("f-w", gen, connFunding(conn), 6, 2))
	if err != nil || res.Admitted || res.Wait == nil {
		t.Fatalf("admit should deny: %+v err=%v", res, err)
	}
	wait := res.Wait
	turn, err := s.CommitTurn(ctx, pa, "t-w", gen, CommitRequest{
		Outcome: "await",
		Wait: &CommitWait{
			Kind: "budget", Funding: wait.Funding, Estimate: &wait.Estimate,
		},
	})
	if err != nil || turn.Status != "awaiting" {
		t.Fatalf("await commit: %+v err=%v", turn, err)
	}
	got, _, err := s.GetInput(ctx, pa, "in-w")
	if err != nil || got.Status != "waiting" {
		t.Fatalf("input status=%q err=%v, want waiting", got.Status, err)
	}
	waits, err := s.BudgetWaitsForHuman(ctx, human)
	if err != nil || len(waits) != 1 || waits[0].FundingID != conn {
		t.Fatalf("budget waits: %+v err=%v", waits, err)
	}
	// Raising the cap resumes the parked input.
	if _, err := s.SetBudget(ctx, human, "connection", conn, fixtureBudget(100)); err != nil {
		t.Fatalf("raise budget: %v", err)
	}
	got, _, err = s.GetInput(ctx, pa, "in-w")
	if err != nil || got.Status != "queued" {
		t.Fatalf("input after raise=%q err=%v, want queued", got.Status, err)
	}
	waits, _ = s.BudgetWaitsForHuman(ctx, human)
	if len(waits) != 0 {
		t.Fatalf("wait row should be gone: %+v", waits)
	}
	// The resumed input runs a fresh turn — no duplicate side effects.
	if _, err := s.LoadTurn(ctx, pa, gen, "t-w2", 10); err != nil {
		t.Fatalf("reload: %v", err)
	}
	res2, err := s.AdmitUsage(ctx, pa, admitReq("f-w2", gen, connFunding(conn), 6, 2))
	if err != nil || !res2.Admitted {
		t.Fatalf("admit after raise: %+v err=%v", res2, err)
	}
}

// If the cap changed between the denial and the commit, the input
// requeues immediately instead of waiting on a blocker that no longer
// exists — the same race the approval-await path serializes.
func TestUsageBudgetWaitFitsAtCommit(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	conn := mustConnection(t, pool, human)
	selectConnection(t, pool, human, conn)
	gen := acquireWriter(t, s, pa, time.Minute)

	if _, err := s.SetBudget(ctx, human, "connection", conn, fixtureBudget(5)); err != nil {
		t.Fatalf("set budget: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-f",
		Kind: "message", Payload: map[string]any{"text": "hi"},
		ActorKind: "human", ActorID: human, SourceSurface: "test"}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, gen, "t-f", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	res, err := s.AdmitUsage(ctx, pa, admitReq("f-f", gen, connFunding(conn), 6, 2))
	if err != nil || res.Admitted {
		t.Fatalf("expected denial: %+v err=%v", res, err)
	}
	// The human raises the cap before the commit lands.
	if _, err := s.SetBudget(ctx, human, "connection", conn, fixtureBudget(100)); err != nil {
		t.Fatalf("raise: %v", err)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-f", gen, CommitRequest{
		Outcome: "await",
		Wait: &CommitWait{Kind: "budget", Funding: res.Wait.Funding,
			Estimate: &res.Wait.Estimate},
	}); err != nil {
		t.Fatalf("await commit: %v", err)
	}
	got, _, _ := s.GetInput(ctx, pa, "in-f")
	if got.Status != "queued" {
		t.Fatalf("input=%q, want queued (blocker already gone)", got.Status)
	}
	waits, _ := s.BudgetWaitsForHuman(ctx, human)
	if len(waits) != 0 {
		t.Fatalf("no wait row expected: %+v", waits)
	}
}

// Removing the cap entirely resumes waits too — uncapped is explicit.
func TestUsageBudgetClearResumes(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	conn := mustConnection(t, pool, human)
	selectConnection(t, pool, human, conn)
	gen := acquireWriter(t, s, pa, time.Minute)

	if _, err := s.SetBudget(ctx, human, "connection", conn, fixtureBudget(1)); err != nil {
		t.Fatalf("set budget: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-c",
		Kind: "message", Payload: map[string]any{"text": "hi"},
		ActorKind: "human", ActorID: human, SourceSurface: "test"}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, gen, "t-c", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	res, _ := s.AdmitUsage(ctx, pa, admitReq("f-c", gen, connFunding(conn), 6, 2))
	if _, err := s.CommitTurn(ctx, pa, "t-c", gen, CommitRequest{
		Outcome: "await",
		Wait: &CommitWait{Kind: "budget", Funding: res.Wait.Funding,
			Estimate: &res.Wait.Estimate},
	}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := s.ClearBudget(ctx, human, "connection", conn); err != nil {
		t.Fatalf("clear: %v", err)
	}
	got, _, _ := s.GetInput(ctx, pa, "in-c")
	if got.Status != "queued" {
		t.Fatalf("input=%q after clear, want queued", got.Status)
	}
	// Clearing again is not found, and another human cannot clear it.
	if err := s.ClearBudget(ctx, human, "connection", conn); !errors.Is(err, ErrFundingNotFound) {
		t.Fatalf("second clear err=%v", err)
	}
}

// Recording is deliberately not generation-fenced: a replaced writer can
// still report spend that already happened under the dead generation.
func TestUsageRecordSurvivesFenceLoss(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	conn := mustConnection(t, pool, human)
	gen1 := acquireWriter(t, s, pa, 50*time.Millisecond)

	if _, err := s.AdmitUsage(ctx, pa, admitReq("f-fence", gen1, connFunding(conn), 10, 10)); err != nil {
		t.Fatalf("admit: %v", err)
	}
	// The writer dies; a new holder takes over after the lease lapses.
	time.Sleep(60 * time.Millisecond)
	gen2 := acquireWriter(t, s, pa, time.Minute)
	if gen2 <= gen1 {
		t.Fatalf("gen2=%d must exceed gen1=%d", gen2, gen1)
	}
	// The old generation cannot admit new spend…
	if _, err := s.AdmitUsage(ctx, pa, admitReq("f-fence2", gen1, connFunding(conn), 10, 10)); !errors.Is(err, ErrGenerationFence) {
		t.Fatalf("old-gen admit err=%v, want ErrGenerationFence", err)
	}
	// …but it can still record the spend that already happened. Recovery
	// already reconciled the lost record into an 'unrecorded' fact; this
	// late report upgrades it to the provider's actual quantities.
	in, out := int64(10), int64(4)
	fact, _, err := s.RecordUsage(ctx, pa, UsageRecordRequest{
		FactID: "f-fence", Kind: "model_call", Phase: "turn",
		Funding: connFunding(conn), Status: "reported",
		InputTokens: &in, OutputTokens: &out, Quantities: map[string]any{},
	})
	if err != nil || fact.Status != "reported" {
		t.Fatalf("unfenced late record: %+v err=%v", fact, err)
	}
	var status string
	if err := pool.QueryRow(ctx,
		`SELECT status FROM usage_reservations WHERE persona_id=$1 AND fact_id='f-fence'`,
		pa).Scan(&status); err != nil || status != "settled" {
		t.Fatalf("reservation=%q err=%v, want settled", status, err)
	}
}

// An admitted call whose record never lands is not released back to the
// allowance — a lost response does not prove the request never left. Turn
// commit and generation recovery reconcile the hold into an inspectable
// 'unrecorded' fact carrying the admission estimate as uncertain spend.
func TestUsageOrphanedReservationReconciled(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	conn := mustConnection(t, pool, human)
	gen := acquireWriter(t, s, pa, 200*time.Millisecond)
	if _, err := s.SetBudget(ctx, human, "connection", conn, fixtureBudget(1_000_000)); err != nil {
		t.Fatalf("set budget: %v", err)
	}

	// Commit-turn reconcile: admitted under turn t-orphan, never recorded.
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-o",
		Kind: "message", Payload: map[string]any{"text": "hi"},
		ActorKind: "human", ActorID: human, SourceSurface: "test"}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, gen, "t-orphan", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	req := admitReq("f-orphan", gen, connFunding(conn), 6, 2)
	req.TurnID = "t-orphan"
	if _, err := s.AdmitUsage(ctx, pa, req); err != nil {
		t.Fatalf("admit: %v", err)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-orphan", gen,
		CommitRequest{Outcome: "complete"}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	var status string
	if err := pool.QueryRow(ctx,
		`SELECT status FROM usage_reservations WHERE persona_id=$1 AND fact_id='f-orphan'`,
		pa).Scan(&status); err != nil || status != "settled" {
		t.Fatalf("orphan reservation=%q err=%v, want settled", status, err)
	}
	// The hold became an inspectable 'unrecorded' fact (6*1 + 2*2 = 10)
	// rather than fresh allowance.
	facts, _ := s.ListUsageFacts(ctx, pa, 10)
	if len(facts) != 1 || facts[0].Status != "unrecorded" ||
		facts[0].CostMinor == nil || *facts[0].CostMinor != 10 ||
		facts[0].CostBasis == nil || *facts[0].CostBasis != "admission_estimate" {
		t.Fatalf("orphan should be an unrecorded estimated fact: %+v", facts)
	}
	spent, held, _ := s.fundingSpend(ctx, s.pool, "connection", conn, "USD")
	if spent != 10 || held != 0 {
		t.Fatalf("spent=%d held=%d after reconcile, want 10/0", spent, held)
	}

	// A late real report upgrades the unrecorded fact to actual spend.
	in, out := int64(6), int64(1)
	fact, _, err := s.RecordUsage(ctx, pa, UsageRecordRequest{
		FactID: "f-orphan", Kind: "model_call", Phase: "turn", TurnID: "t-orphan",
		Funding: connFunding(conn), Status: "reported",
		InputTokens: &in, OutputTokens: &out, Quantities: map[string]any{},
	})
	if err != nil || fact.Status != "reported" {
		t.Fatalf("late report upgrade: %+v err=%v", fact, err)
	}
	if fact.CostMinor == nil || *fact.CostMinor != 8 { // 6*1 + 1*2
		t.Fatalf("upgraded cost=%v, want 8 under the admission card", fact.CostMinor)
	}
	spent, _, _ = s.fundingSpend(ctx, s.pool, "connection", conn, "USD")
	if spent != 8 {
		t.Fatalf("spent=%d after late report, want 8", spent)
	}

	// Recovery reconcile: a dead generation's held reservation becomes an
	// 'unrecorded' fact too — never silently released.
	if _, err := s.AdmitUsage(ctx, pa, admitReq("f-dead", gen, connFunding(conn), 6, 2)); err != nil {
		t.Fatalf("admit f-dead: %v", err)
	}
	time.Sleep(220 * time.Millisecond) // let the short lease lapse
	gen2 := acquireWriter(t, s, pa, time.Minute)
	if _, err := s.Recover(ctx, pa, gen2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	var dead string
	if err := pool.QueryRow(ctx,
		`SELECT status FROM usage_reservations WHERE persona_id=$1 AND fact_id='f-dead'`,
		pa).Scan(&dead); err != nil || dead != "settled" {
		t.Fatalf("dead-gen reservation=%q err=%v, want settled", dead, err)
	}
	facts, _ = s.ListUsageFacts(ctx, pa, 10)
	found := false
	for _, f := range facts {
		if f.FactID == "f-dead" {
			found = true
			if f.Status != "unrecorded" || f.CostMinor == nil || *f.CostMinor != 10 {
				t.Fatalf("dead-gen fact: %+v, want unrecorded estimate 10", f)
			}
		}
	}
	if !found {
		t.Fatal("dead-gen call must be inspectable as an unrecorded fact")
	}
}

// A rate or currency change mid-call cannot rewrite the cost basis of a
// call already admitted — the reservation snapshots the card that priced
// it — and spend sums never cross currencies.
func TestUsageSnapshotPricingAndCurrency(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	conn := mustConnection(t, pool, human)
	gen := acquireWriter(t, s, pa, time.Minute)
	if _, err := s.SetBudget(ctx, human, "connection", conn, fixtureBudget(1_000_000)); err != nil {
		t.Fatalf("set budget: %v", err)
	}
	if _, err := s.AdmitUsage(ctx, pa, admitReq("f-snap", gen, connFunding(conn), 100, 100)); err != nil {
		t.Fatalf("admit: %v", err)
	}
	// The owner edits the card mid-call: new currency, new rates.
	jpy := fixtureBudget(5_000)
	jpy.Currency = "JPY"
	jpy.RateInputPerMTok = 9_000_000
	jpy.PricingRevision = "fixture-rates-v2"
	if _, err := s.SetBudget(ctx, human, "connection", conn, jpy); err != nil {
		t.Fatalf("edit budget: %v", err)
	}
	in, out := int64(100), int64(50)
	fact, created, err := s.RecordUsage(ctx, pa, UsageRecordRequest{
		FactID: "f-snap", Kind: "model_call", Phase: "turn",
		Funding: connFunding(conn), Status: "reported",
		InputTokens: &in, OutputTokens: &out, Quantities: map[string]any{},
	})
	if err != nil || !created {
		t.Fatalf("record: %+v err=%v", fact, err)
	}
	// Priced under the admission card (USD fixture v1): 100*1 + 50*2 = 200.
	if fact.CostMinor == nil || *fact.CostMinor != 200 ||
		fact.Currency == nil || *fact.Currency != "USD" ||
		fact.PricingRevision == nil || *fact.PricingRevision != "fixture-rates-v1" {
		t.Fatalf("snapshot pricing broken: %+v", fact)
	}
	// The JPY budget counts only JPY spend — the USD fact is not
	// reinterpreted as yen.
	view, err := s.BudgetView(ctx, "connection", conn)
	if err != nil || view == nil {
		t.Fatalf("budget view: %+v err=%v", view, err)
	}
	if view.SpentMinor != 0 || view.Currency != "JPY" {
		t.Fatalf("JPY budget must not count USD spend: %+v", view)
	}
	// Totals keep both currencies dimensioned, never one integer.
	views, err := s.UsageForHuman(ctx, human, 10)
	if err != nil || len(views) != 1 {
		t.Fatalf("usage view: %d err=%v", len(views), err)
	}
	if views[0].Totals.Costs["USD"] != 200 {
		t.Fatalf("totals costs: %+v", views[0].Totals.Costs)
	}
}

// A Sumi-provided allocation is a distinct funding kind: the grantee's
// personas may spend it while the grant is live, never after revocation.
func TestUsageSumiGrantFunding(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	_, otherHuman := boundPersona(t, s, pool)
	gen := acquireWriter(t, s, pa, time.Minute)

	if _, err := pool.Exec(ctx, `
		INSERT INTO usage_funding_grants (funding_id, human_id, label)
		VALUES ('sumi-grant-1', $1, 'alpha allocation')`, human); err != nil {
		t.Fatalf("grant: %v", err)
	}
	sumi := FundingRef{Kind: "sumi", ID: "sumi-grant-1"}
	res, err := s.AdmitUsage(ctx, pa, admitReq("f-sumi", gen, sumi, 10, 10))
	if err != nil || !res.Admitted {
		t.Fatalf("sumi admit: %+v err=%v", res, err)
	}
	// A grant to another human does not fund this persona.
	if _, err := pool.Exec(ctx, `
		INSERT INTO usage_funding_grants (funding_id, human_id, label)
		VALUES ('sumi-grant-2', $1, 'not yours')`, otherHuman); err != nil {
		t.Fatalf("grant2: %v", err)
	}
	if _, err := s.AdmitUsage(ctx, pa,
		admitReq("f-sumi2", gen, FundingRef{Kind: "sumi", ID: "sumi-grant-2"}, 10, 10)); !errors.Is(err, ErrFundingNotFound) {
		t.Fatalf("other's grant err=%v, want ErrFundingNotFound", err)
	}
	// Revocation ends spend at the next admission boundary.
	if _, err := pool.Exec(ctx,
		`UPDATE usage_funding_grants SET revoked_at = now() WHERE funding_id='sumi-grant-1'`); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if _, err := s.AdmitUsage(ctx, pa,
		admitReq("f-sumi3", gen, sumi, 10, 10)); !errors.Is(err, ErrFundingNotFound) {
		t.Fatalf("revoked grant err=%v, want ErrFundingNotFound", err)
	}
	// The already-recorded grant spend stays attributed after revocation.
	in, out := int64(10), int64(5)
	if _, _, err := s.RecordUsage(ctx, pa, UsageRecordRequest{
		FactID: "f-sumi", Kind: "model_call", Phase: "turn",
		Funding: sumi, Status: "reported",
		InputTokens: &in, OutputTokens: &out, Quantities: map[string]any{},
	}); err != nil {
		t.Fatalf("record after revoke: %v", err)
	}
}

// A memory chunk reshelved on a budget denial spends no attempt and no
// interruption; a funding change clears its pacing early.
func TestReshelveMemoryChunkAndBudgetUnpark(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	conn := mustConnection(t, pool, human)
	gen := acquireWriter(t, s, pa, time.Minute)

	c := seedSealed(t, s, pa, gen, 2)
	claimed, err := s.ClaimMemoryChunk(ctx, pa, gen, 50)
	if err != nil || claimed.Chunk == nil {
		t.Fatalf("claim: %+v err=%v", claimed, err)
	}
	// Budget-denied: reshelve with the 'budget-wait:' reason and a slow
	// pace — the claim reached no model.
	back, err := s.ReshelveMemoryChunk(ctx, pa, gen, claimed.Chunk.ChunkSeq,
		"budget-wait: denied for connection:"+conn, 30_000)
	if err != nil {
		t.Fatalf("reshelve: %v", err)
	}
	if back.Status != "sealed" || back.Attempts != 0 || back.Interruptions != 0 {
		t.Fatalf("reshelve must not spend budgets: %+v", back)
	}
	if back.NotBefore == nil || !back.NotBefore.After(time.Now().Add(10*time.Second)) {
		t.Fatalf("budget pacing expected, got %v", back.NotBefore)
	}
	// A funding change unparks the chunk — the wait is over.
	if _, err := s.ResumeWaitsForHuman(ctx, human); err != nil {
		t.Fatalf("resume: %v", err)
	}
	after, err := s.chunk(ctx, s.pool, pa, c.ChunkSeq)
	if err != nil || after.NotBefore != nil {
		t.Fatalf("funding change should clear pacing: %+v err=%v", after, err)
	}
}

// Admitting the same fact id twice replays the held reservation rather
// than double-reserving — a lost admit response is the same call.
func TestUsageAdmitReplay(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	conn := mustConnection(t, pool, human)
	gen := acquireWriter(t, s, pa, time.Minute)
	if _, err := s.SetBudget(ctx, human, "connection", conn, fixtureBudget(100)); err != nil {
		t.Fatalf("set budget: %v", err)
	}
	req := admitReq("f-replay", gen, connFunding(conn), 6, 2)
	r1, err := s.AdmitUsage(ctx, pa, req)
	if err != nil || !r1.Admitted {
		t.Fatalf("first admit: %+v err=%v", r1, err)
	}
	r2, err := s.AdmitUsage(ctx, pa, req)
	if err != nil || !r2.Admitted || r2.Reservation.ReservedMinor != r1.Reservation.ReservedMinor {
		t.Fatalf("replayed admit must return the same hold: %+v err=%v", r2, err)
	}
	_, held, _ := s.fundingSpend(ctx, s.pool, "connection", conn, "USD")
	if held != r1.Reservation.ReservedMinor {
		t.Fatalf("held=%d, want single reservation %d", held, r1.Reservation.ReservedMinor)
	}
	// Same fact id under different funding conflicts.
	bad := req
	bad.Funding = FundingRef{Kind: "operator", ID: "env"}
	if _, err := s.AdmitUsage(ctx, pa, bad); !errors.Is(err, ErrUsageFactConflict) {
		t.Fatalf("conflicting admit err=%v, want ErrUsageFactConflict", err)
	}
}

// A partial provider report never becomes final priced usage: the state
// service refuses 'reported' without both input and output, an 'unknown'
// fact keeps the supplied categories and the admission estimate, and the
// complete report for the same fact supersedes the estimate once — spend
// moves from estimate to actual, never counting both.
func TestUsagePartialReportSupersededByCompleteReport(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	conn := mustConnection(t, pool, human)
	gen := acquireWriter(t, s, pa, time.Minute)
	if _, err := s.SetBudget(ctx, human, "connection", conn, fixtureBudget(1_000_000)); err != nil {
		t.Fatalf("set budget: %v", err)
	}
	// 500*1 + 500*2 = 1500 estimated.
	if _, err := s.AdmitUsage(ctx, pa, admitReq("f-part", gen, connFunding(conn), 500, 500)); err != nil {
		t.Fatalf("admit: %v", err)
	}
	i64 := func(v int64) *int64 { return &v }
	spend := func(wantSpent, wantHeld int64) {
		t.Helper()
		spent, held, err := s.fundingSpend(ctx, s.pool, "connection", conn, "USD")
		if err != nil || spent != wantSpent || held != wantHeld {
			t.Fatalf("spent=%d held=%d err=%v, want %d/%d", spent, held, err, wantSpent, wantHeld)
		}
	}
	base := UsageRecordRequest{
		FactID: "f-part", Kind: "model_call", Phase: "turn", TurnID: "t",
		Funding: connFunding(conn), Quantities: map[string]any{"prompt_tokens": 600},
	}

	partial := base
	partial.Status = "reported"
	partial.InputTokens = i64(600)
	if _, _, err := s.RecordUsage(ctx, pa, partial); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("partial 'reported' err=%v, want ErrBadRequest", err)
	}
	spend(0, 1500)

	partial.Status = "unknown"
	fact, created, err := s.RecordUsage(ctx, pa, partial)
	if err != nil || !created {
		t.Fatalf("record partial unknown: created=%v err=%v", created, err)
	}
	if fact.Status != "unknown" || fact.InputTokens == nil || *fact.InputTokens != 600 ||
		fact.OutputTokens != nil || fact.Quantities["prompt_tokens"] == nil ||
		fact.CostMinor == nil || *fact.CostMinor != 1500 ||
		fact.CostBasis == nil || *fact.CostBasis != "admission_estimate" {
		t.Fatalf("partial fact must keep supplied input, null output and the estimate: %+v", fact)
	}
	spend(1500, 0)
	if _, created, err := s.RecordUsage(ctx, pa, partial); err != nil || created {
		t.Fatalf("partial redelivery: created=%v err=%v", created, err)
	}
	// An explicit zero output is a different report from an absent one, and
	// a call reported as attempted cannot turn into never-sent.
	zeroOut := partial
	zeroOut.OutputTokens = i64(0)
	notSent := base
	notSent.Status = "not_sent"
	notSent.Quantities = nil
	for name, r := range map[string]UsageRecordRequest{"explicit zero": zeroOut, "not_sent": notSent} {
		if _, _, err := s.RecordUsage(ctx, pa, r); !errors.Is(err, ErrUsageFactConflict) {
			t.Fatalf("%s after partial unknown: err=%v, want conflict", name, err)
		}
	}
	spend(1500, 0)

	// The complete report — explicit zero output is valid — supersedes.
	full := base
	full.Status = "reported"
	full.InputTokens, full.OutputTokens = i64(600), i64(0)
	fact, created, err = s.RecordUsage(ctx, pa, full)
	if err != nil || created || fact.Status != "reported" ||
		fact.OutputTokens == nil || *fact.OutputTokens != 0 ||
		fact.CostMinor == nil || *fact.CostMinor != 600 ||
		fact.CostBasis == nil || *fact.CostBasis != "configured_rates" {
		t.Fatalf("complete report: %+v created=%v err=%v", fact, created, err)
	}
	spend(600, 0)
	// Redelivery of either report replays the final fact; a contradicting
	// report conflicts. Nothing moves spend again.
	for _, r := range []UsageRecordRequest{full, partial} {
		got, created, err := s.RecordUsage(ctx, pa, r)
		if err != nil || created || got.Status != "reported" || *got.CostMinor != 600 {
			t.Fatalf("redelivery %s: %+v created=%v err=%v", r.Status, got, created, err)
		}
	}
	contradict := partial
	contradict.InputTokens = i64(601)
	if _, _, err := s.RecordUsage(ctx, pa, contradict); !errors.Is(err, ErrUsageFactConflict) {
		t.Fatalf("contradicting partial err=%v, want conflict", err)
	}
	spend(600, 0)
	if facts, _ := s.ListUsageFacts(ctx, pa, 10); len(facts) != 1 {
		t.Fatalf("%d facts, want exactly one for the call", len(facts))
	}

	// Malformed claims are refused before any state is touched.
	bad := base
	bad.FactID, bad.Status = "f-bad", "not_sent"
	bad.InputTokens = i64(1)
	if _, _, err := s.RecordUsage(ctx, pa, bad); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("not_sent with tokens err=%v, want ErrBadRequest", err)
	}
	bad.Status, bad.InputTokens, bad.OutputTokens = "reported", i64(-1), i64(1)
	if _, _, err := s.RecordUsage(ctx, pa, bad); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("negative tokens err=%v, want ErrBadRequest", err)
	}
}

// A record must carry the funding its admission reserved under. Another
// source the persona may also spend — a second connection of the same
// human — cannot take the call over: the fact would attribute spend to B
// while A's hold settles, silently moving cost and budget authority
// between funding sources.
func TestUsageRecordFundingMustMatchAdmission(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	connA := mustConnection(t, pool, human)
	connB := mustConnection(t, pool, human)
	gen := acquireWriter(t, s, pa, time.Minute)
	for _, c := range []string{connA, connB} {
		if _, err := s.SetBudget(ctx, human, "connection", c, fixtureBudget(1_000_000)); err != nil {
			t.Fatalf("set budget: %v", err)
		}
	}
	// 500*1 + 500*2 = 1500 held on A.
	if _, err := s.AdmitUsage(ctx, pa, admitReq("f-ab", gen, connFunding(connA), 500, 500)); err != nil {
		t.Fatalf("admit A: %v", err)
	}
	in, out := int64(400), int64(100) // 400*1 + 100*2 = 600
	report := func(factID string, f FundingRef) UsageRecordRequest {
		return UsageRecordRequest{FactID: factID, Kind: "model_call", Phase: "turn",
			Funding: f, Status: "reported", InputTokens: &in, OutputTokens: &out,
			Quantities: map[string]any{}}
	}
	spend := func(conn string) (int64, int64) {
		t.Helper()
		spent, held, err := s.fundingSpend(ctx, s.pool, "connection", conn, "USD")
		if err != nil {
			t.Fatalf("funding spend: %v", err)
		}
		return spent, held
	}

	fact, _, err := s.RecordUsage(ctx, pa, report("f-ab", connFunding(connB)))
	if !errors.Is(err, ErrUsageFactConflict) {
		t.Errorf("record under B for A's admission: fact=%+v err=%v, want ErrUsageFactConflict", fact, err)
	}
	if facts, _ := s.ListUsageFacts(ctx, pa, 10); len(facts) != 0 {
		t.Errorf("mismatched record left a fact: %+v", facts)
	}
	if spent, held := spend(connA); spent != 0 || held != 1500 {
		t.Errorf("A spent=%d held=%d, want 0/1500 — A's hold must stay A's", spent, held)
	}
	if spent, held := spend(connB); spent != 0 || held != 0 {
		t.Errorf("B spent=%d held=%d, want 0/0 — B never admitted this call", spent, held)
	}
	if t.Failed() {
		return
	}

	// The admitted funding records and settles as usual.
	fact, created, err := s.RecordUsage(ctx, pa, report("f-ab", connFunding(connA)))
	if err != nil || !created || fact.Funding.ID != connA {
		t.Fatalf("record under A: %+v created=%v err=%v", fact, created, err)
	}
	if spent, held := spend(connA); spent != 600 || held != 0 {
		t.Fatalf("A spent=%d held=%d after its record, want 600/0", spent, held)
	}
	// A settled reservation still pins the call to A.
	if _, _, err := s.RecordUsage(ctx, pa, report("f-ab", connFunding(connB))); !errors.Is(err, ErrUsageFactConflict) {
		t.Fatalf("record under B after settlement err=%v, want ErrUsageFactConflict", err)
	}
	// A record with no admission at all is still accepted for funding the
	// persona may spend now, priced under that source's current card.
	fact, created, err = s.RecordUsage(ctx, pa, report("f-free", connFunding(connB)))
	if err != nil || !created || fact.Funding.ID != connB || fact.CostMinor == nil ||
		*fact.CostMinor != 600 || *fact.CostBasis != "configured_rates" {
		t.Fatalf("unreserved record under B: %+v created=%v err=%v", fact, created, err)
	}
	if spent, held := spend(connB); spent != 600 || held != 0 {
		t.Fatalf("B spent=%d held=%d, want 600/0", spent, held)
	}
	if spent, _ := spend(connA); spent != 600 {
		t.Fatalf("A spent=%d, want 600 (unchanged)", spent)
	}
}

// A's hold stays A's through every later stage: transfer seal reconciles
// it into A's 'unrecorded' estimate, A's connection is deleted, and the
// fenced core's late partial and then complete reports still land on A —
// never on another source the persona can spend.
func TestUsageLateRecordAfterSealKeepsAdmissionFunding(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	connA := mustConnection(t, pool, human)
	connB := mustConnection(t, pool, human)
	gen := acquireWriter(t, s, pa, time.Minute)
	for _, c := range []string{connA, connB} {
		if _, err := s.SetBudget(ctx, human, "connection", c, fixtureBudget(1_000_000)); err != nil {
			t.Fatalf("set budget: %v", err)
		}
	}
	if _, err := s.AdmitUsage(ctx, pa, admitReq("f-late", gen, connFunding(connA), 500, 500)); err != nil {
		t.Fatalf("admit A: %v", err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := ReconcileRetiredReservations(ctx, tx, pa); err != nil {
		t.Fatalf("reconcile at seal: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	facts, _ := s.ListUsageFacts(ctx, pa, 10)
	if len(facts) != 1 || facts[0].Status != "unrecorded" || facts[0].Funding.ID != connA ||
		facts[0].CostMinor == nil || *facts[0].CostMinor != 1500 {
		t.Fatalf("seal reconciliation: %+v", facts)
	}
	if _, err := pool.Exec(ctx,
		`DELETE FROM model_api_connections WHERE connection_id = $1::uuid`, connA); err != nil {
		t.Fatalf("delete connection A: %v", err)
	}

	partialIn := int64(400)
	partial := func(f FundingRef) UsageRecordRequest {
		return UsageRecordRequest{FactID: "f-late", Kind: "model_call", Phase: "turn",
			Funding: f, Status: "unknown", InputTokens: &partialIn,
			Quantities: map[string]any{}}
	}
	in, out := int64(400), int64(100)
	complete := func(factID string, f FundingRef) UsageRecordRequest {
		return UsageRecordRequest{FactID: factID, Kind: "model_call", Phase: "turn",
			Funding: f, Status: "reported", InputTokens: &in, OutputTokens: &out,
			Quantities: map[string]any{}}
	}
	if _, _, err := s.RecordUsage(ctx, pa, partial(connFunding(connB))); !errors.Is(err, ErrUsageFactConflict) {
		t.Fatalf("late partial under B err=%v, want ErrUsageFactConflict", err)
	}
	fact, created, err := s.RecordUsage(ctx, pa, partial(connFunding(connA)))
	if err != nil || created || fact.Status != "unknown" || fact.Funding.ID != connA ||
		fact.CostMinor == nil || *fact.CostMinor != 1500 || *fact.CostBasis != "admission_estimate" {
		t.Fatalf("late partial under deleted A: %+v created=%v err=%v", fact, created, err)
	}
	if _, _, err := s.RecordUsage(ctx, pa, complete("f-late", connFunding(connB))); !errors.Is(err, ErrUsageFactConflict) {
		t.Fatalf("late complete under B err=%v, want ErrUsageFactConflict", err)
	}
	fact, created, err = s.RecordUsage(ctx, pa, complete("f-late", connFunding(connA)))
	if err != nil || created || fact.Status != "reported" || fact.Funding.ID != connA ||
		fact.CostMinor == nil || *fact.CostMinor != 600 {
		t.Fatalf("late complete under deleted A: %+v created=%v err=%v", fact, created, err)
	}
	for conn, want := range map[string]int64{connA: 600, connB: 0} {
		spent, held, err := s.fundingSpend(ctx, s.pool, "connection", conn, "USD")
		if err != nil || spent != want || held != 0 {
			t.Fatalf("%s spent=%d held=%d err=%v, want %d/0", conn, spent, held, err, want)
		}
	}
	// With no admission to prove it, a deleted connection authorizes nothing.
	if _, _, err := s.RecordUsage(ctx, pa, complete("f-orphan", connFunding(connA))); !errors.Is(err, ErrFundingNotFound) {
		t.Fatalf("unreserved record under deleted A err=%v, want ErrFundingNotFound", err)
	}
}

// parkOnBudget submits an input, claims it, has admission deny it, and
// commits the turn 'await' on the returned wait.
func parkOnBudget(t *testing.T, s *Store, pa, human string, gen int64, conn, inputID, turnID, factID string) *BudgetWait {
	t.Helper()
	ctx := context.Background()
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: inputID,
		Kind: "message", Payload: map[string]any{"text": "hi"},
		ActorKind: "human", ActorID: human, SourceSurface: "test"}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, gen, turnID, 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	res, err := s.AdmitUsage(ctx, pa, admitReq(factID, gen, connFunding(conn), 6, 2))
	if err != nil || res.Admitted || res.Wait == nil {
		t.Fatalf("admit should deny: %+v err=%v", res, err)
	}
	if _, err := s.CommitTurn(ctx, pa, turnID, gen, CommitRequest{
		Outcome: "await",
		Wait: &CommitWait{Kind: "budget", Funding: res.Wait.Funding,
			Estimate: &res.Wait.Estimate},
	}); err != nil {
		t.Fatalf("await commit: %v", err)
	}
	return res.Wait
}

// Lowering the rate card under an unchanged limit is a configuration
// change that can make a parked call fit: the wait is priced again under
// the new card, so the input resumes without an unrelated change or a
// manual nudge. A lower card that still does not fit keeps it parked.
func TestUsageBudgetWaitResumesOnRateDecrease(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	conn := mustConnection(t, pool, human)
	selectConnection(t, pool, human, conn)
	gen := acquireWriter(t, s, pa, time.Minute)

	// 6 input * 1 + 2 output * 2 = 10 > 8.
	if _, err := s.SetBudget(ctx, human, "connection", conn, fixtureBudget(8)); err != nil {
		t.Fatalf("set budget: %v", err)
	}
	if w := parkOnBudget(t, s, pa, human, gen, conn, "in-r", "t-r", "f-r"); w.NeededMinor != 10 {
		t.Fatalf("denied need=%d, want 10", w.NeededMinor)
	}
	status := func() string {
		t.Helper()
		got, _, err := s.GetInput(ctx, pa, "in-r")
		if err != nil {
			t.Fatalf("get input: %v", err)
		}
		return got.Status
	}
	if st := status(); st != "waiting" {
		t.Fatalf("input=%q, want waiting", st)
	}
	// Cheaper, but still over: 6*1 + 2*1.5 = 9 > 8.
	b := fixtureBudget(8)
	b.RateOutputPerMTok = 1_500_000
	if _, err := s.SetBudget(ctx, human, "connection", conn, b); err != nil {
		t.Fatalf("lower output rate: %v", err)
	}
	if st := status(); st != "waiting" {
		t.Fatalf("input=%q after a card that still does not fit, want waiting", st)
	}
	waits, err := s.BudgetWaitsForHuman(ctx, human)
	if err != nil || len(waits) != 1 || waits[0].NeededMinor != 9 || waits[0].Currency != "USD" {
		t.Errorf("still-parked wait: %+v err=%v, want needed 9 USD under the current card", waits, err)
	}
	// Fits under the same limit: 6*0.5 + 2*1 = 5 <= 8.
	b.RateInputPerMTok, b.RateOutputPerMTok = 500_000, 1_000_000
	if _, err := s.SetBudget(ctx, human, "connection", conn, b); err != nil {
		t.Fatalf("lower both rates: %v", err)
	}
	if st := status(); st != "queued" {
		t.Fatalf("input=%q after a rate decrease that fits, want queued", st)
	}
	if waits, _ := s.BudgetWaitsForHuman(ctx, human); len(waits) != 0 {
		t.Fatalf("wait row should be gone: %+v", waits)
	}
	if _, err := s.LoadTurn(ctx, pa, gen, "t-r2", 10); err != nil {
		t.Fatalf("reload: %v", err)
	}
	res, err := s.AdmitUsage(ctx, pa, admitReq("f-r2", gen, connFunding(conn), 6, 2))
	if err != nil || !res.Admitted || res.Reservation.ReservedMinor != 5 {
		t.Fatalf("admit after rate decrease: %+v err=%v", res, err)
	}
}

// A rate decrease landing between the denial and the await commit is the
// same race as a limit raise there: the commit prices the wait under the
// card now in force and requeues instead of parking on a stale amount.
func TestUsageBudgetWaitRepricedAtCommit(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	conn := mustConnection(t, pool, human)
	selectConnection(t, pool, human, conn)
	gen := acquireWriter(t, s, pa, time.Minute)

	if _, err := s.SetBudget(ctx, human, "connection", conn, fixtureBudget(8)); err != nil {
		t.Fatalf("set budget: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-rc",
		Kind: "message", Payload: map[string]any{"text": "hi"},
		ActorKind: "human", ActorID: human, SourceSurface: "test"}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, gen, "t-rc", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	res, err := s.AdmitUsage(ctx, pa, admitReq("f-rc", gen, connFunding(conn), 6, 2))
	if err != nil || res.Admitted {
		t.Fatalf("expected denial: %+v err=%v", res, err)
	}
	b := fixtureBudget(8)
	b.RateInputPerMTok, b.RateOutputPerMTok = 500_000, 1_000_000
	if _, err := s.SetBudget(ctx, human, "connection", conn, b); err != nil {
		t.Fatalf("lower rates: %v", err)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-rc", gen, CommitRequest{
		Outcome: "await",
		Wait: &CommitWait{Kind: "budget", Funding: res.Wait.Funding,
			Estimate: &res.Wait.Estimate},
	}); err != nil {
		t.Fatalf("await commit: %v", err)
	}
	got, _, _ := s.GetInput(ctx, pa, "in-rc")
	if got.Status != "queued" {
		t.Fatalf("input=%q, want queued (the new card fits)", got.Status)
	}
	if waits, _ := s.BudgetWaitsForHuman(ctx, human); len(waits) != 0 {
		t.Fatalf("no wait row expected: %+v", waits)
	}
}

// --- budget progress: park/change serialization and headroom restores -----

// lockWaiters counts sessions blocked on a row lock — a tuple-lock wait or
// a wait on the holder's transactionid, depending on the statement — proof
// that a transaction is blocked mid-statement, never a sleep used as proof.
func lockWaiters(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(DISTINCT pid) FROM pg_locks
		WHERE locktype IN ('tuple', 'transactionid') AND NOT granted`).Scan(&n); err != nil {
		t.Fatalf("pg_locks: %v", err)
	}
	return n
}

// waitForLockWaiter blocks until at least `want` sessions wait on row
// locks — deterministic evidence a transaction is parked mid-statement.
func waitForLockWaiter(t *testing.T, pool *pgxpool.Pool, want int, who string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if lockWaiters(t, pool) >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never blocked", who)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// selectConnection seeds the human's model selection — the state the
// production flow implies when a call was admitted on a connection.
func selectConnection(t *testing.T, pool *pgxpool.Pool, humanID, connID string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO model_connection_selections (human_id, kind, connection_id)
		VALUES ($1, 'api', $2::uuid)
		ON CONFLICT (human_id) DO UPDATE SET kind = 'api', connection_id = $2::uuid`,
		humanID, connID); err != nil {
		t.Fatalf("select connection: %v", err)
	}
}

// A park commit that has already read the funding's headroom must not land
// its wait row after the concurrent condition change's resume pass already
// finished — the affordable input would stay parked until an unrelated
// future change (a lost wake). For every change kind the park and the
// resume serialize: either the change's resume observes the committed wait
// row, or the park observes the changed condition.
//
// The interleaving is real, not slept: the controller transaction holds
// the input row so CommitTurn stalls just before marking the input
// 'waiting' — past its headroom read — while the change plus its resume
// pass run. Then the input row releases and the park lands.
func TestUsageBudgetWaitParkSerializedAgainstChanges(t *testing.T) {
	ctx := context.Background()
	changes := []struct {
		name string
		run  func(s *Store, pool *pgxpool.Pool, human, connA, connB string) error
	}{
		{"limit increase", func(s *Store, pool *pgxpool.Pool, human, a, _ string) error {
			_, err := s.SetBudget(ctx, human, "connection", a, fixtureBudget(100))
			return err
		}},
		{"rate decrease", func(s *Store, pool *pgxpool.Pool, human, a, _ string) error {
			low := fixtureBudget(8)
			low.RateInputPerMTok, low.RateOutputPerMTok = 500_000, 1_000_000
			_, err := s.SetBudget(ctx, human, "connection", a, low)
			return err
		}},
		{"clear budget", func(s *Store, pool *pgxpool.Pool, human, a, _ string) error {
			return s.ClearBudget(ctx, human, "connection", a)
		}},
		{"funding change", func(s *Store, pool *pgxpool.Pool, human, _, b string) error {
			if _, err := pool.Exec(ctx, `
				INSERT INTO model_connection_selections (human_id, kind, connection_id)
				VALUES ($1, 'api', $2::uuid)
				ON CONFLICT (human_id) DO UPDATE SET kind = 'api', connection_id = $2::uuid`,
				human, b); err != nil {
				return err
			}
			_, err := s.ResumeWaitsForHuman(ctx, human)
			return err
		}},
	}
	for _, tc := range changes {
		t.Run(tc.name, func(t *testing.T) {
			s, pool := newStore(t)
			pa, human := boundPersona(t, s, pool)
			connA := mustConnection(t, pool, human)
			connB := mustConnection(t, pool, human)
			selectConnection(t, pool, human, connA)
			gen := acquireWriter(t, s, pa, time.Minute)

			// 6 input + 2 output bound = 10 needed > 8 limit.
			if _, err := s.SetBudget(ctx, human, "connection", connA, fixtureBudget(8)); err != nil {
				t.Fatalf("set budget: %v", err)
			}
			if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-race",
				Kind: "message", Payload: map[string]any{"text": "hi"},
				ActorKind: "human", ActorID: human, SourceSurface: "test"}); err != nil {
				t.Fatalf("submit: %v", err)
			}
			if _, err := s.LoadTurn(ctx, pa, gen, "t-race", 10); err != nil {
				t.Fatalf("load: %v", err)
			}
			res, err := s.AdmitUsage(ctx, pa, admitReq("f-race", gen, connFunding(connA), 6, 2))
			if err != nil || res.Admitted || res.Wait == nil {
				t.Fatalf("admit should deny: %+v err=%v", res, err)
			}

			// Hold the input row so the park commit stalls at its input
			// UPDATE — past the point where it evaluated headroom.
			ctl, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = ctl.Rollback(ctx) }()
			if _, err := ctl.Exec(ctx, `
				SELECT 1 FROM core_inputs
				WHERE persona_id = $1 AND input_id = 'in-race' FOR UPDATE`, pa); err != nil {
				t.Fatalf("controller lock: %v", err)
			}

			commitErr := make(chan error, 1)
			go func() {
				_, err := s.CommitTurn(ctx, pa, "t-race", gen, CommitRequest{
					Outcome: "await",
					Wait: &CommitWait{Kind: "budget", Funding: res.Wait.Funding,
						Estimate: &res.Wait.Estimate},
				})
				commitErr <- err
			}()
			waitForLockWaiter(t, pool, 1, "park commit")

			changeErr := make(chan error, 1)
			go func() { changeErr <- tc.run(s, pool, human, connA, connB) }()

			// Either the whole change completed before the park could land
			// (the old lost-wake interleaving), or it serialized behind the
			// in-flight commit — a second session waiting on a row lock.
			changeEarly := false
			deadline := time.Now().Add(15 * time.Second)
			for {
				select {
				case err := <-changeErr:
					if err != nil {
						t.Fatalf("change: %v", err)
					}
					changeEarly = true
				default:
				}
				if changeEarly || lockWaiters(t, pool) >= 2 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("change neither completed nor serialized with the park")
				}
				time.Sleep(5 * time.Millisecond)
			}
			t.Logf("change ran fully before the park landed: %v", changeEarly)

			if err := ctl.Commit(ctx); err != nil {
				t.Fatalf("release input row: %v", err)
			}
			if err := <-commitErr; err != nil {
				t.Fatalf("park commit: %v", err)
			}
			if !changeEarly {
				if err := <-changeErr; err != nil {
					t.Fatalf("change: %v", err)
				}
			}

			got, _, err := s.GetInput(ctx, pa, "in-race")
			if err != nil {
				t.Fatalf("get input: %v", err)
			}
			if got.Status != "queued" {
				t.Fatalf("input=%q after the change raced the park, want queued (lost wake)", got.Status)
			}
			waits, err := s.BudgetWaitsForHuman(ctx, human)
			if err != nil || len(waits) != 0 {
				t.Fatalf("a wait row must not outlive its resume: %+v err=%v", waits, err)
			}
			// The resumed input re-admits: the wake never authorized the
			// call — admission decides again.
			if _, err := s.LoadTurn(ctx, pa, gen, "t-race2", 10); err != nil {
				t.Fatalf("reload: %v", err)
			}
			res2, err := s.AdmitUsage(ctx, pa, admitReq("f-race2", gen, connFunding(connA), 6, 2))
			if err != nil {
				t.Fatalf("re-admit: %v", err)
			}
			switch tc.name {
			case "rate decrease":
				if !res2.Admitted || res2.Reservation.ReservedMinor != 5 {
					t.Fatalf("re-admit after rate decrease: %+v, want reserved 5", res2)
				}
			case "limit increase", "clear budget":
				if !res2.Admitted {
					t.Fatalf("re-admit after %s: %+v", tc.name, res2)
				}
			case "funding change":
				// The selection moved to connB; the wait's funding is
				// stale — a connA admit must not be replayed as if still
				// denied/parked; a fresh attempt resolves connB instead.
				if !res2.Admitted {
					// connA has no budget headroom contract now — a denial
					// here is also honest; what must not happen is a wait
					// row on the stale funding.
					if res2.Wait == nil || res2.Wait.Funding.ID != connA {
						t.Fatalf("unexpected deny shape after funding change: %+v", res2)
					}
				}
			}
		})
	}
}

// When the funding itself changed between the denial and the commit and
// the human-scope resume already ran, the wait's funding is stale: the
// commit requeues instead of parking on a source the next attempt would
// not resolve.
func TestUsageBudgetWaitStaleFundingAtCommit(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	connA := mustConnection(t, pool, human)
	connB := mustConnection(t, pool, human)
	selectConnection(t, pool, human, connA)
	gen := acquireWriter(t, s, pa, time.Minute)

	if _, err := s.SetBudget(ctx, human, "connection", connA, fixtureBudget(8)); err != nil {
		t.Fatalf("set budget: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: "in-sf",
		Kind: "message", Payload: map[string]any{"text": "hi"},
		ActorKind: "human", ActorID: human, SourceSurface: "test"}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, gen, "t-sf", 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	res, err := s.AdmitUsage(ctx, pa, admitReq("f-sf", gen, connFunding(connA), 6, 2))
	if err != nil || res.Admitted {
		t.Fatalf("expected denial: %+v err=%v", res, err)
	}
	// The human switches connections — the resume pass runs before the
	// park commit lands.
	selectConnection(t, pool, human, connB)
	if _, err := s.ResumeWaitsForHuman(ctx, human); err != nil {
		t.Fatalf("human resume: %v", err)
	}
	if _, err := s.CommitTurn(ctx, pa, "t-sf", gen, CommitRequest{
		Outcome: "await",
		Wait: &CommitWait{Kind: "budget", Funding: res.Wait.Funding,
			Estimate: &res.Wait.Estimate},
	}); err != nil {
		t.Fatalf("await commit: %v", err)
	}
	got, _, _ := s.GetInput(ctx, pa, "in-sf")
	if got.Status != "queued" {
		t.Fatalf("input=%q, want queued — the wait's funding is stale", got.Status)
	}
	if waits, _ := s.BudgetWaitsForHuman(ctx, human); len(waits) != 0 {
		t.Fatalf("no wait row expected on the stale funding: %+v", waits)
	}
}

// parkEstimate is parkOnBudget with a caller-chosen estimate, so a parked
// call can be smaller than a concurrent hold.
func parkEstimate(t *testing.T, s *Store, pa, human string, gen int64, conn, inputID, turnID, factID string, inTok, outBound int64) {
	t.Helper()
	ctx := context.Background()
	if _, _, err := s.SubmitInput(ctx, &Input{PersonaID: pa, InputID: inputID,
		Kind: "message", Payload: map[string]any{"text": "hi"},
		ActorKind: "human", ActorID: human, SourceSurface: "test"}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, pa, gen, turnID, 10); err != nil {
		t.Fatalf("load: %v", err)
	}
	res, err := s.AdmitUsage(ctx, pa, admitReq(factID, gen, connFunding(conn), inTok, outBound))
	if err != nil || res.Admitted || res.Wait == nil {
		t.Fatalf("admit should deny: %+v err=%v", res, err)
	}
	if _, err := s.CommitTurn(ctx, pa, turnID, gen, CommitRequest{
		Outcome: "await",
		Wait: &CommitWait{Kind: "budget", Funding: res.Wait.Funding,
			Estimate: &res.Wait.Estimate},
	}); err != nil {
		t.Fatalf("await commit: %v", err)
	}
}

// A concurrent call that settles below its reservation frees headroom —
// the input parked on that headroom resumes from the record alone, with
// no human budget change. The wake only requeues: admission decides again
// on the next attempt.
func TestUsageSettleBelowBoundResumesBudgetWait(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	conn := mustConnection(t, pool, human)
	selectConnection(t, pool, human, conn)
	gen := acquireWriter(t, s, pa, time.Minute)

	if _, err := s.SetBudget(ctx, human, "connection", conn, fixtureBudget(10)); err != nil {
		t.Fatalf("set budget: %v", err)
	}
	// Call A holds the whole cap (6*1 + 2*2 = 10); B needs 3*1 + 1*2 = 5.
	if _, err := s.AdmitUsage(ctx, pa, admitReq("f-a", gen, connFunding(conn), 6, 2)); err != nil {
		t.Fatalf("admit A: %v", err)
	}
	parkEstimate(t, s, pa, human, gen, conn, "in-b", "t-b", "f-b", 3, 1)
	if got, _, _ := s.GetInput(ctx, pa, "in-b"); got.Status != "waiting" {
		t.Fatalf("in-b=%q, want waiting", got.Status)
	}
	// A lands below its bound: actual 3*1 + 1*2 = 5 of the reserved 10.
	in, out := int64(3), int64(1)
	fact, created, err := s.RecordUsage(ctx, pa, UsageRecordRequest{
		FactID: "f-a", Kind: "model_call", Phase: "turn",
		Funding: connFunding(conn), Status: "reported",
		InputTokens: &in, OutputTokens: &out, Quantities: map[string]any{},
	})
	if err != nil || !created || fact.CostMinor == nil || *fact.CostMinor != 5 {
		t.Fatalf("record A: %+v created=%v err=%v", fact, created, err)
	}
	if got, _, _ := s.GetInput(ctx, pa, "in-b"); got.Status != "queued" {
		t.Fatalf("in-b=%q after A settled below bound, want queued (headroom restored)", got.Status)
	}
	if waits, _ := s.BudgetWaitsForHuman(ctx, human); len(waits) != 0 {
		t.Fatalf("wait row must be gone after headroom returned: %+v", waits)
	}
	// Re-admission still decides — the wake authorized nothing.
	if _, err := s.LoadTurn(ctx, pa, gen, "t-b2", 10); err != nil {
		t.Fatalf("reload: %v", err)
	}
	res, err := s.AdmitUsage(ctx, pa, admitReq("f-b2", gen, connFunding(conn), 3, 1))
	if err != nil || !res.Admitted || res.Reservation.ReservedMinor != 5 {
		t.Fatalf("re-admit: %+v err=%v, want admitted reserved 5", res, err)
	}
	// Close t-b2 so the next park's LoadTurn starts a fresh turn. Its held
	// reservation reconciles to an 'unrecorded' fact at the estimate —
	// the hold becomes spend (10), so the arithmetic below is unchanged.
	if _, err := s.CommitTurn(ctx, pa, "t-b2", gen, CommitRequest{Outcome: "complete"}); err != nil {
		t.Fatalf("complete t-b2: %v", err)
	}
	// The real report for f-b2 arrives late and upgrades the 'unrecorded'
	// fact to its actual zero usage — spend drops back to 5.
	zero := int64(0)
	if _, _, err := s.RecordUsage(ctx, pa, UsageRecordRequest{
		FactID: "f-b2", Kind: "model_call", Phase: "turn",
		Funding: connFunding(conn), Status: "reported",
		InputTokens: &zero, OutputTokens: &zero, Quantities: map[string]any{},
	}); err != nil {
		t.Fatalf("late record f-b2: %v", err)
	}

	// A settle that frees too little leaves the wait parked.
	if _, err := s.AdmitUsage(ctx, pa, admitReq("f-c", gen, connFunding(conn), 3, 1)); err != nil {
		t.Fatalf("admit C: %v", err)
	}
	// spent 5 (f-a) + held 5 (f-c) = 10 fills the cap — park D needing 5.
	parkEstimate(t, s, pa, human, gen, conn, "in-d", "t-d", "f-d", 3, 1)
	if got, _, _ := s.GetInput(ctx, pa, "in-d"); got.Status != "waiting" {
		t.Fatalf("in-d=%q, want waiting", got.Status)
	}
	// C reports 4 of the reserved 5 — freeing 1 leaves no room for 5.
	in2, out1 := int64(2), int64(1)
	if _, _, err := s.RecordUsage(ctx, pa, UsageRecordRequest{
		FactID: "f-c", Kind: "model_call", Phase: "turn",
		Funding: connFunding(conn), Status: "reported",
		InputTokens: &in2, OutputTokens: &out1, Quantities: map[string]any{},
	}); err != nil {
		t.Fatalf("record C: %v", err)
	}
	// spent 5+4=9 > limit-needed — the freed headroom cannot fit 5.
	if got, _, _ := s.GetInput(ctx, pa, "in-d"); got.Status != "waiting" {
		t.Fatalf("in-d=%q, want still waiting — not enough headroom returned", got.Status)
	}
	if waits, _ := s.BudgetWaitsForHuman(ctx, human); len(waits) != 1 {
		t.Fatalf("wait row should persist: %+v", waits)
	}
}

// A 'not_sent' report releases the whole hold — the parked input resumes
// from the record alone.
func TestUsageNotSentReleaseResumesBudgetWait(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	conn := mustConnection(t, pool, human)
	selectConnection(t, pool, human, conn)
	gen := acquireWriter(t, s, pa, time.Minute)

	if _, err := s.SetBudget(ctx, human, "connection", conn, fixtureBudget(10)); err != nil {
		t.Fatalf("set budget: %v", err)
	}
	if _, err := s.AdmitUsage(ctx, pa, admitReq("f-ns", gen, connFunding(conn), 6, 2)); err != nil {
		t.Fatalf("admit: %v", err)
	}
	parkEstimate(t, s, pa, human, gen, conn, "in-ns", "t-ns", "f-nsp", 3, 1)
	if _, _, err := s.RecordUsage(ctx, pa, UsageRecordRequest{
		FactID: "f-ns", Kind: "model_call", Phase: "turn",
		Funding: connFunding(conn), Status: "not_sent", Quantities: map[string]any{},
	}); err != nil {
		t.Fatalf("not_sent record: %v", err)
	}
	if got, _, _ := s.GetInput(ctx, pa, "in-ns"); got.Status != "queued" {
		t.Fatalf("in-ns=%q after the hold released, want queued", got.Status)
	}
	if waits, _ := s.BudgetWaitsForHuman(ctx, human); len(waits) != 0 {
		t.Fatalf("wait row must be gone: %+v", waits)
	}
}

// The same invariant covers a hold already reconciled to an 'unrecorded'
// estimate: a late 'not_sent' report retires the estimate, restores the
// spend, and resumes the parked input. A late 'reported' below the
// estimate restores only the difference.
func TestUsageReconciledHoldLateRecordResumesBudgetWait(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	conn := mustConnection(t, pool, human)
	selectConnection(t, pool, human, conn)
	gen := acquireWriter(t, s, pa, time.Minute)

	if _, err := s.SetBudget(ctx, human, "connection", conn, fixtureBudget(10)); err != nil {
		t.Fatalf("set budget: %v", err)
	}
	if _, err := s.AdmitUsage(ctx, pa, admitReq("f-r", gen, connFunding(conn), 6, 2)); err != nil {
		t.Fatalf("admit: %v", err)
	}
	// Recovery/seal-style reconciliation turns the hold into an
	// 'unrecorded' fact — 10 estimated spent — the parked wait stays.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := ReconcileRetiredReservations(ctx, tx, pa); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	parkEstimate(t, s, pa, human, gen, conn, "in-r", "t-r2", "f-rp", 3, 1)
	if got, _, _ := s.GetInput(ctx, pa, "in-r"); got.Status != "waiting" {
		t.Fatalf("in-r=%q, want waiting", got.Status)
	}
	// The report proves the request never left: the estimate comes back.
	if _, _, err := s.RecordUsage(ctx, pa, UsageRecordRequest{
		FactID: "f-r", Kind: "model_call", Phase: "turn",
		Funding: connFunding(conn), Status: "not_sent", Quantities: map[string]any{},
	}); err != nil {
		t.Fatalf("late not_sent: %v", err)
	}
	if got, _, _ := s.GetInput(ctx, pa, "in-r"); got.Status != "queued" {
		t.Fatalf("in-r=%q after the estimate was retired, want queued", got.Status)
	}
	if waits, _ := s.BudgetWaitsForHuman(ctx, human); len(waits) != 0 {
		t.Fatalf("wait row must be gone: %+v", waits)
	}
}

// A pending approval is a second, independent wait: restoring headroom
// deletes the budget wait row but must not run the input — the human's
// decision still gates it.
func TestUsageSettleResumeHonorsPendingApproval(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	conn := mustConnection(t, pool, human)
	selectConnection(t, pool, human, conn)
	gen := acquireWriter(t, s, pa, time.Minute)

	if _, err := s.SetBudget(ctx, human, "connection", conn, fixtureBudget(10)); err != nil {
		t.Fatalf("set budget: %v", err)
	}
	if _, err := s.AdmitUsage(ctx, pa, admitReq("f-h", gen, connFunding(conn), 6, 2)); err != nil {
		t.Fatalf("admit: %v", err)
	}
	parkEstimate(t, s, pa, human, gen, conn, "in-h", "t-h", "f-hp", 3, 1)
	// A pending approval on the same input — the human has not decided.
	if _, err := pool.Exec(ctx, `
		INSERT INTO core_tool_approvals
			(approval_id, persona_id, input_id, call_index, operation_id,
			 turn_id, tool, route, required_by, request, action_digest, status)
		VALUES ('ap-h', $1, 'in-h', 0, 'op-h', 't-h', 'journal.note',
			'normal', 'intrinsic', '{}'::jsonb, 'digest-h', 'pending')`, pa); err != nil {
		t.Fatalf("seed approval: %v", err)
	}
	if _, _, err := s.RecordUsage(ctx, pa, UsageRecordRequest{
		FactID: "f-h", Kind: "model_call", Phase: "turn",
		Funding: connFunding(conn), Status: "not_sent", Quantities: map[string]any{},
	}); err != nil {
		t.Fatalf("record: %v", err)
	}
	// The budget wait resolved but the approval still holds the input —
	// nothing re-ran it.
	got, _, _ := s.GetInput(ctx, pa, "in-h")
	if got.Status != "waiting" {
		t.Fatalf("in-h=%q, want still waiting on the human's decision", got.Status)
	}
	if waits, _ := s.BudgetWaitsForHuman(ctx, human); len(waits) != 0 {
		t.Fatalf("budget wait row should be gone: %+v", waits)
	}
	var apStatus string
	if err := pool.QueryRow(ctx,
		`SELECT status FROM core_tool_approvals WHERE persona_id=$1 AND approval_id='ap-h'`,
		pa).Scan(&apStatus); err != nil || apStatus != "pending" {
		t.Fatalf("approval=%q err=%v, want untouched pending", apStatus, err)
	}
}

// A call admitted while no rate card existed reserved nothing. A budget
// added mid-call must not rewrite that record's cost basis: an 'unknown'
// report stays honestly unpriced — never a fabricated zero-cost
// 'admission_estimate' denominated in the new card's currency. A complete
// 'reported' for the same call still prices its actual tokens under the
// card in force at record time.
func TestUsageUnpricedAdmitUnknownStaysUnpriced(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	conn := mustConnection(t, pool, human)
	gen := acquireWriter(t, s, pa, time.Minute)

	// No budget at admission — unpriced holds, reserved 0.
	for _, fid := range []string{"f-u", "f-u2"} {
		res, err := s.AdmitUsage(ctx, pa, admitReq(fid, gen, connFunding(conn), 500, 500))
		if err != nil || !res.Admitted || res.Reservation.ReservedMinor != 0 {
			t.Fatalf("unpriced admit %s: %+v err=%v", fid, res, err)
		}
	}
	// The cap and rate card arrive while the calls are in flight.
	if _, err := s.SetBudget(ctx, human, "connection", conn, fixtureBudget(1_000_000)); err != nil {
		t.Fatalf("set budget mid-call: %v", err)
	}
	fact, created, err := s.RecordUsage(ctx, pa, UsageRecordRequest{
		FactID: "f-u", Kind: "model_call", Phase: "turn",
		Funding: connFunding(conn), Status: "unknown", Quantities: map[string]any{},
	})
	if err != nil || !created {
		t.Fatalf("record unknown: %+v created=%v err=%v", fact, created, err)
	}
	if fact.CostMinor != nil || fact.Currency != nil || fact.CostBasis != nil {
		t.Fatalf("unpriced admission must stay unpriced on unknown, got cost=%v ccy=%v basis=%v",
			fact.CostMinor, fact.Currency, fact.CostBasis)
	}
	// The complete report prices the actual tokens under the card in
	// force now — real usage, not a fabricated estimate.
	in, out := int64(100), int64(50)
	fact2, _, err := s.RecordUsage(ctx, pa, UsageRecordRequest{
		FactID: "f-u2", Kind: "model_call", Phase: "turn",
		Funding: connFunding(conn), Status: "reported",
		InputTokens: &in, OutputTokens: &out, Quantities: map[string]any{},
	})
	if err != nil || fact2.CostMinor == nil || *fact2.CostMinor != 200 ||
		fact2.CostBasis == nil || *fact2.CostBasis != "configured_rates" {
		t.Fatalf("late 'reported' under the current card: %+v err=%v", fact2, err)
	}
}
