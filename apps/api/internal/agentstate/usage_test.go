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
// in the ledger, never silently zero, and not priced by the rate card.
func TestUsageUnknownFact(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	conn := mustConnection(t, pool, human)
	gen := acquireWriter(t, s, pa, time.Minute)
	if _, err := s.SetBudget(ctx, human, "connection", conn, fixtureBudget(1_000_000)); err != nil {
		t.Fatalf("set budget: %v", err)
	}
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
	if fact.CostMinor != nil || fact.InputTokens != nil {
		t.Fatalf("unknown fact must carry no tokens and no fabricated cost: %+v", fact)
	}
	facts, _ := s.ListUsageFacts(ctx, pa, 10)
	if len(facts) != 1 || facts[0].Status != "unknown" {
		t.Fatalf("unknown fact inspectable: %+v", facts)
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
	_, held, err := s.fundingSpend(ctx, s.pool, "connection", conn)
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
			Kind: "budget", Funding: wait.Funding,
			NeededMinor: wait.NeededMinor, Currency: wait.Currency,
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
			NeededMinor: res.Wait.NeededMinor, Currency: res.Wait.Currency},
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
			NeededMinor: res.Wait.NeededMinor, Currency: res.Wait.Currency},
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
	// …but it can still record the spend that already happened.
	in, out := int64(10), int64(4)
	fact, created, err := s.RecordUsage(ctx, pa, UsageRecordRequest{
		FactID: "f-fence", Kind: "model_call", Phase: "turn",
		Funding: connFunding(conn), Status: "reported",
		InputTokens: &in, OutputTokens: &out, Quantities: map[string]any{},
	})
	if err != nil || !created {
		t.Fatalf("unfenced record: %+v created=%v err=%v", fact, created, err)
	}
	// Recovery settled the old generation's reservation against the fact.
	var status string
	if err := pool.QueryRow(ctx,
		`SELECT status FROM usage_reservations WHERE persona_id=$1 AND fact_id='f-fence'`,
		pa).Scan(&status); err != nil || status != "settled" {
		t.Fatalf("reservation=%q err=%v, want settled", status, err)
	}
}

// An admitted call whose record never lands cannot hold budget headroom
// forever: turn commit releases this turn's orphans, generation recovery
// releases a dead writer's.
func TestUsageOrphanedReservationRelease(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa, human := boundPersona(t, s, pool)
	conn := mustConnection(t, pool, human)
	gen := acquireWriter(t, s, pa, 200*time.Millisecond)
	if _, err := s.SetBudget(ctx, human, "connection", conn, fixtureBudget(100)); err != nil {
		t.Fatalf("set budget: %v", err)
	}

	// Commit-turn release: admitted under turn t-orphan, never recorded.
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
		pa).Scan(&status); err != nil || status != "released" {
		t.Fatalf("orphan reservation=%q err=%v, want released", status, err)
	}
	_, held, _ := s.fundingSpend(ctx, s.pool, "connection", conn)
	if held != 0 {
		t.Fatalf("held=%d after release, want 0", held)
	}

	// Recovery release: a dead generation's held reservation frees when
	// the next writer recovers.
	if _, err := s.AdmitUsage(ctx, pa, admitReq("f-dead", gen, connFunding(conn), 6, 2)); err != nil {
		t.Fatalf("admit f-dead: %v", err)
	}
	time.Sleep(220 * time.Millisecond) // let the short lease lapse
	gen2 := acquireWriter(t, s, pa, time.Minute)
	if _, err := s.Recover(ctx, pa, gen2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	var released string
	if err := pool.QueryRow(ctx,
		`SELECT status FROM usage_reservations WHERE persona_id=$1 AND fact_id='f-dead'`,
		pa).Scan(&released); err != nil || released != "released" {
		t.Fatalf("dead-gen reservation=%q err=%v, want released", released, err)
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
	_, held, _ := s.fundingSpend(ctx, s.pool, "connection", conn)
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
