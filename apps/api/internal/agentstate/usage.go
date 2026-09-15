// Usage accounting and explicit budgets (migration 0055).
//
// Every provider call the core makes passes two boundaries:
//
//	admit   — under the writer generation, before any request is sent.
//	          Reserves the priced estimate against the funding source's
//	          configured budget; denial means no request was made and the
//	          caller parks the turn instead of burning attempts.
//	record  — after the call resolves, persona-scoped (not fenced: the
//	          spend already happened and a replaced writer must still be
//	          able to report it). One row per logical call; redelivery of
//	          the same fact_id replays the stored fact, a conflicting
//	          payload under a known fact_id is a contract violation.
//
// The funding identity recorded on the fact is the one selected at call
// time — switching connections never reattributes earlier calls. Token
// categories stay distinct and non-overlapping; a call whose usage never
// arrived is recorded 'unknown', not silently zero, and retains its
// admission estimate as explicitly estimated spend so uncertain spend
// cannot restore the budget it may have consumed. Money is integer minor
// units dimensioned by currency — spend sums never cross currencies;
// configured rates are the owner's declared estimate (pricing_revision
// carries provenance), snapshotted at admission so a mid-call rate or
// currency change cannot rewrite that call's cost basis, and never
// presented as a provider bill.
package agentstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	// ErrUsageFactConflict: a fact_id already recorded with a different
	// payload — the caller is confusing a genuinely additional call with a
	// redelivery (HTTP 409).
	ErrUsageFactConflict = errors.New("conflicting usage fact")
	// ErrFundingNotFound: the funding source does not exist or is not
	// usable by this persona/human (HTTP 404).
	ErrFundingNotFound = errors.New("funding source not found")
	// ErrFundingForbidden: the funding source exists but the authenticated
	// human does not own or manage it (HTTP 403).
	ErrFundingForbidden = errors.New("funding source is not yours")
)

// FundingRef identifies the funding principal selected for a call.
// kind 'connection' is a Human-owned model_api_connections row (id is the
// connection uuid, version/model/preset snapshot the identity at call
// time). 'operator' is the host's environment default (id 'env'), used
// only when the persona has no explicit selection. 'sumi' is a
// Sumi-provided allocation (usage_funding_grants) granted to the persona's
// bound human.
type FundingRef struct {
	Kind     string `json:"kind"`
	ID       string `json:"id"`
	Version  string `json:"version,omitempty"`
	Model    string `json:"model,omitempty"`
	Provider string `json:"provider,omitempty"`
}

// UsageEstimate is the caller's pre-call size estimate, priced by the
// configured rate card at admission. OutputTokensBound is the configured
// maximum output when one exists; nil means the call's spend is not
// bounded at admission and the reservation only bounds further admits.
type UsageEstimate struct {
	InputTokens       int64  `json:"input_tokens"`
	OutputTokensBound *int64 `json:"output_tokens_bound"`
}

// UsageAdmitRequest asks for admission of one provider call.
type UsageAdmitRequest struct {
	Generation int64         `json:"generation"`
	FactID     string        `json:"fact_id"`
	Kind       string        `json:"kind"`
	Phase      string        `json:"phase"`
	TurnID     string        `json:"turn_id,omitempty"`
	InputID    string        `json:"input_id,omitempty"`
	Round      int           `json:"round"`
	Funding    FundingRef    `json:"funding"`
	Estimate   UsageEstimate `json:"estimate"`
}

// BudgetWait describes a denied admission: what the call would have
// needed against the configured cap. The caller commits the turn 'await'
// carrying this so the input parks until budget or funding changes.
type BudgetWait struct {
	Funding         FundingRef `json:"funding"`
	NeededMinor     int64      `json:"needed_minor"`
	LimitMinor      int64      `json:"limit_minor"`
	SpentMinor      int64      `json:"spent_minor"`
	HeldMinor       int64      `json:"held_minor"`
	RemainingMinor  int64      `json:"remaining_minor"`
	Currency        string     `json:"currency"`
	PricingRevision string     `json:"pricing_revision"`
	// Bounded is false when the call's output was not limited at
	// admission — the cap bounds further admits, not that call's bill.
	Bounded bool `json:"bounded"`
}

// UsageReservation is the held spend for an admitted call.
type UsageReservation struct {
	FactID        string `json:"fact_id"`
	ReservedMinor int64  `json:"reserved_minor"`
	Currency      string `json:"currency,omitempty"`
	Bounded       bool   `json:"bounded"`
	Status        string `json:"status"`
}

// UsageAdmitResult is the admission verdict.
type UsageAdmitResult struct {
	Admitted    bool              `json:"admitted"`
	Reservation *UsageReservation `json:"reservation,omitempty"`
	Wait        *BudgetWait       `json:"wait,omitempty"`
}

// UsageRecordRequest reports one call's resolved usage.
type UsageRecordRequest struct {
	FactID  string     `json:"fact_id"`
	Kind    string     `json:"kind"`
	Phase   string     `json:"phase"`
	TurnID  string     `json:"turn_id,omitempty"`
	InputID string     `json:"input_id,omitempty"`
	Round   int        `json:"round"`
	Funding FundingRef `json:"funding"`
	// Status 'reported' carries the provider's own usage fields;
	// 'unknown' means the call was attempted but no usage report
	// resolved — recorded, never silently zero; 'not_sent' asserts no
	// request was produced after admission (the reservation releases —
	// nothing was or can be owed).
	Status       string         `json:"status"`
	InputTokens  *int64         `json:"input_tokens"`
	OutputTokens *int64         `json:"output_tokens"`
	CachedTokens *int64         `json:"cached_tokens"`
	Quantities   map[string]any `json:"quantities"`
}

// UsageFact is one ledger row: one logical provider call.
type UsageFact struct {
	PersonaID       string         `json:"persona_id"`
	FactID          string         `json:"fact_id"`
	Kind            string         `json:"kind"`
	Phase           string         `json:"phase"`
	TurnID          *string        `json:"turn_id,omitempty"`
	InputID         *string        `json:"input_id,omitempty"`
	Round           *int           `json:"round,omitempty"`
	Funding         FundingRef     `json:"funding"`
	Status          string         `json:"status"`
	InputTokens     *int64         `json:"input_tokens"`
	OutputTokens    *int64         `json:"output_tokens"`
	CachedTokens    *int64         `json:"cached_tokens"`
	Quantities      map[string]any `json:"quantities"`
	CostMinor       *int64         `json:"cost_minor"`
	Currency        *string        `json:"currency,omitempty"`
	CostBasis       *string        `json:"cost_basis,omitempty"`
	PricingRevision *string        `json:"pricing_revision,omitempty"`
	RecordedAt      time.Time      `json:"recorded_at"`
}

// UsageBudget is the owner's configured cap on one funding source.
type UsageBudget struct {
	FundingKind       string    `json:"funding_kind"`
	FundingID         string    `json:"funding_id"`
	LimitMinor        int64     `json:"limit_minor"`
	Currency          string    `json:"currency"`
	RateInputPerMTok  int64     `json:"rate_input_per_mtok"`
	RateOutputPerMTok int64     `json:"rate_output_per_mtok"`
	RateCachedPerMTok *int64    `json:"rate_cached_per_mtok"`
	PricingRevision   string    `json:"pricing_revision"`
	UpdatedAt         time.Time `json:"updated_at"`
	SpentMinor        int64     `json:"spent_minor"`
	HeldMinor         int64     `json:"held_minor"`
	RemainingMinor    int64     `json:"remaining_minor"`
}

// UsageTotals aggregates a funding source's ledger for display. Costs are
// per-currency — facts recorded under different rate-card currencies are
// never summed into one integer.
type UsageTotals struct {
	Calls         int64            `json:"calls"`
	UnknownCalls  int64            `json:"unknown_calls"`
	UnpricedCalls int64            `json:"unpriced_calls"`
	InputTokens   int64            `json:"input_tokens"`
	OutputTokens  int64            `json:"output_tokens"`
	CachedTokens  int64            `json:"cached_tokens"`
	Costs         map[string]int64 `json:"costs"`
}

// BudgetWaitRow is a parked input's budget-wait record.
type BudgetWaitRow struct {
	PersonaID   string    `json:"persona_id"`
	InputID     string    `json:"input_id"`
	TurnID      string    `json:"turn_id"`
	FundingKind string    `json:"funding_kind"`
	FundingID   string    `json:"funding_id"`
	NeededMinor int64     `json:"needed_minor"`
	Currency    string    `json:"currency"`
	CreatedAt   time.Time `json:"created_at"`
}

const tokensPerMTok = int64(1_000_000)

// rateSnapshot is the rate card that priced a call — snapshotted onto the
// reservation at admission so a mid-call rate/currency/budget change
// cannot rewrite that call's cost basis.
type rateSnapshot struct {
	currency      string
	inputPerMTok  int64
	outputPerMTok int64
	cachedPerMTok *int64
	revision      string
}

func (b *UsageBudget) snapshot() rateSnapshot {
	return rateSnapshot{
		currency:      b.Currency,
		inputPerMTok:  b.RateInputPerMTok,
		outputPerMTok: b.RateOutputPerMTok,
		cachedPerMTok: b.RateCachedPerMTok,
		revision:      b.PricingRevision,
	}
}

// priceTokens prices one token category at minor units per million tokens,
// rounding up so a nonzero cost is never rounded to zero. Arithmetic is
// overflow-checked — an absurd quantity is an error, never a wrapped sum.
func priceTokens(tokens, ratePerMTok int64) (int64, error) {
	if tokens < 0 || ratePerMTok < 0 {
		return 0, fmt.Errorf("%w: negative token quantity or rate", ErrBadRequest)
	}
	if tokens == 0 || ratePerMTok == 0 {
		return 0, nil
	}
	if tokens > (math.MaxInt64-(tokensPerMTok-1))/ratePerMTok {
		return 0, fmt.Errorf("%w: token/rate product overflows minor units", ErrBadRequest)
	}
	return (tokens*ratePerMTok + tokensPerMTok - 1) / tokensPerMTok, nil
}

// addMinor sums minor units with an overflow check.
func addMinor(a, b int64) (int64, error) {
	if a > math.MaxInt64-b {
		return 0, fmt.Errorf("%w: cost sum overflows minor units", ErrBadRequest)
	}
	return a + b, nil
}

// priceEstimate prices an admission estimate against the rate card.
func priceEstimate(est UsageEstimate, card rateSnapshot) (int64, bool, error) {
	needed, err := priceTokens(est.InputTokens, card.inputPerMTok)
	if err != nil {
		return 0, false, err
	}
	bounded := est.OutputTokensBound != nil
	if bounded {
		out, err := priceTokens(*est.OutputTokensBound, card.outputPerMTok)
		if err != nil {
			return 0, false, err
		}
		if needed, err = addMinor(needed, out); err != nil {
			return 0, false, err
		}
	}
	return needed, bounded, nil
}

// priceReported prices reported usage additively over the normalized
// (non-overlapping) categories: input_tokens excludes cached_tokens, so
// nothing is subtracted and cache tokens are never double-counted. Cached
// input bills at the cached rate when one is configured, else the input
// rate. Returns ok=false when the fact is not priceable — never a
// fabricated zero-cost bill.
func priceReported(req UsageRecordRequest, card rateSnapshot) (int64, bool, error) {
	if req.Status != "reported" || req.InputTokens == nil || req.OutputTokens == nil {
		return 0, false, nil
	}
	cached := int64(0)
	if req.CachedTokens != nil {
		cached = *req.CachedTokens
	}
	cachedRate := card.inputPerMTok
	if card.cachedPerMTok != nil {
		cachedRate = *card.cachedPerMTok
	}
	cost, err := priceTokens(*req.InputTokens, card.inputPerMTok)
	if err != nil {
		return 0, false, err
	}
	c, err := priceTokens(cached, cachedRate)
	if err != nil {
		return 0, false, err
	}
	if cost, err = addMinor(cost, c); err != nil {
		return 0, false, err
	}
	o, err := priceTokens(*req.OutputTokens, card.outputPerMTok)
	if err != nil {
		return 0, false, err
	}
	if cost, err = addMinor(cost, o); err != nil {
		return 0, false, err
	}
	return cost, true, nil
}

// budgetForFunding reads the configured cap, if any, for one funding source.
func (s *Store) budgetForFunding(ctx context.Context, db queryRower, kind, id string) (*UsageBudget, error) {
	var b UsageBudget
	err := db.QueryRow(ctx, `
		SELECT funding_kind, funding_id, limit_minor, currency,
			rate_input_per_mtok, rate_output_per_mtok, rate_cached_per_mtok,
			pricing_revision, updated_at
		FROM usage_budgets WHERE funding_kind = $1 AND funding_id = $2`,
		kind, id).Scan(&b.FundingKind, &b.FundingID, &b.LimitMinor, &b.Currency,
		&b.RateInputPerMTok, &b.RateOutputPerMTok, &b.RateCachedPerMTok,
		&b.PricingRevision, &b.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &b, nil
}

// budgetForFundingLocked is budgetForFunding under a row lock — the
// serialization point for concurrent admits on one funding source.
func (s *Store) budgetForFundingLocked(ctx context.Context, tx pgx.Tx, kind, id string) (*UsageBudget, error) {
	var b UsageBudget
	err := tx.QueryRow(ctx, `
		SELECT funding_kind, funding_id, limit_minor, currency,
			rate_input_per_mtok, rate_output_per_mtok, rate_cached_per_mtok,
			pricing_revision, updated_at
		FROM usage_budgets WHERE funding_kind = $1 AND funding_id = $2 FOR UPDATE`,
		kind, id).Scan(&b.FundingKind, &b.FundingID, &b.LimitMinor, &b.Currency,
		&b.RateInputPerMTok, &b.RateOutputPerMTok, &b.RateCachedPerMTok,
		&b.PricingRevision, &b.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &b, nil
}

// fundingSpend returns (spent, held) minor units against one funding
// source in one currency: settled facts' recorded cost plus in-flight
// reservations. Spend is currency-dimensioned — a rate-card currency
// change never reinterprets earlier facts as the new unit. Both sums are
// read in one statement so a settlement committing between them cannot be
// missed (read-committed statement snapshot).
func (s *Store) fundingSpend(ctx context.Context, db queryRower, kind, id, currency string) (int64, int64, error) {
	var spent, held int64
	err := db.QueryRow(ctx, `
		SELECT
			(SELECT COALESCE(SUM(cost_minor), 0) FROM usage_facts
				WHERE funding_kind = $1 AND funding_id = $2 AND currency = $3),
			(SELECT COALESCE(SUM(reserved_minor), 0) FROM usage_reservations
				WHERE funding_kind = $1 AND funding_id = $2 AND status = 'held'
					AND currency = $3)`,
		kind, id, currency).Scan(&spent, &held)
	return spent, held, err
}

// fundingAllowed verifies the persona may spend this funding source:
// 'connection' must belong to the persona's bound human, 'sumi' must be
// a live grant to that human, 'operator' is the host env default (id 'env').
func (s *Store) fundingAllowed(ctx context.Context, db queryRower, personaID string, f FundingRef) error {
	switch f.Kind {
	case "operator":
		if f.ID != "env" {
			return fmt.Errorf("%w: operator funding id must be 'env'", ErrBadRequest)
		}
		return nil
	case "connection":
		var exists bool
		err := db.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM model_api_connections c
				JOIN core_personas p ON p.human_id = c.human_id
				WHERE p.persona_id = $1 AND c.connection_id = $2::uuid
			)`, personaID, f.ID).Scan(&exists)
		if err != nil {
			return dataErr(err)
		}
		if !exists {
			return ErrFundingNotFound
		}
		return nil
	case "sumi":
		var exists bool
		err := db.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM usage_funding_grants g
				JOIN core_personas p ON p.human_id = g.human_id
				WHERE p.persona_id = $1 AND g.funding_id = $2 AND g.revoked_at IS NULL
			)`, personaID, f.ID).Scan(&exists)
		if err != nil {
			return err
		}
		if !exists {
			return ErrFundingNotFound
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown funding kind %q", ErrBadRequest, f.Kind)
	}
}

// AdmitUsage reserves priced estimate spend for one provider call under
// the writer's generation. Replaying the same admit (lost response)
// returns the held reservation rather than double-reserving. A denial
// creates nothing durable — the caller commits the wait at turn commit.
func (s *Store) AdmitUsage(ctx context.Context, personaID string, req UsageAdmitRequest) (UsageAdmitResult, error) {
	var res UsageAdmitResult
	if req.FactID == "" || req.Kind == "" || req.Phase == "" {
		return res, fmt.Errorf("%w: fact_id, kind and phase are required", ErrBadRequest)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return res, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := requireGeneration(ctx, tx, personaID, req.Generation); err != nil {
		return res, err
	}
	if err := s.fundingAllowed(ctx, tx, personaID, req.Funding); err != nil {
		return res, err
	}

	// Idempotent replay: the same fact_id admitted already (its response
	// was lost) returns the held reservation — it is the same call.
	var existing UsageReservation
	var existingKind, existingID string
	err = tx.QueryRow(ctx, `
		SELECT fact_id, funding_kind, funding_id, reserved_minor,
			COALESCE(currency, ''), bounded, status
		FROM usage_reservations WHERE persona_id = $1 AND fact_id = $2`,
		personaID, req.FactID).
		Scan(&existing.FactID, &existingKind, &existingID, &existing.ReservedMinor,
			&existing.Currency, &existing.Bounded, &existing.Status)
	switch {
	case err == nil:
		if existingKind != req.Funding.Kind || existingID != req.Funding.ID {
			return res, fmt.Errorf("%w: fact_id %s already admitted under different funding", ErrUsageFactConflict, req.FactID)
		}
		if existing.Status != "held" {
			return res, fmt.Errorf("%w: fact_id %s already %s", ErrUsageFactConflict, req.FactID, existing.Status)
		}
		res.Admitted = true
		res.Reservation = &existing
		return res, tx.Commit(ctx)
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return res, err
	}

	budget, err := s.budgetForFundingLocked(ctx, tx, req.Funding.Kind, req.Funding.ID)
	if err != nil {
		return res, err
	}
	var needed, reserved int64
	var card *rateSnapshot
	bounded := req.Estimate.OutputTokensBound != nil
	if budget != nil {
		snap := budget.snapshot()
		card = &snap
		var b bool
		needed, b, err = priceEstimate(req.Estimate, *card)
		if err != nil {
			return res, err
		}
		bounded = b
		reserved = needed
		spent, held, err := s.fundingSpend(ctx, tx, req.Funding.Kind, req.Funding.ID, budget.Currency)
		if err != nil {
			return res, err
		}
		if needed > budget.LimitMinor || spent+held > budget.LimitMinor-needed {
			res.Admitted = false
			res.Wait = &BudgetWait{
				Funding:         req.Funding,
				NeededMinor:     needed,
				LimitMinor:      budget.LimitMinor,
				SpentMinor:      spent,
				HeldMinor:       held,
				RemainingMinor:  budget.LimitMinor - spent - held,
				Currency:        budget.Currency,
				PricingRevision: budget.PricingRevision,
				Bounded:         bounded,
			}
			if err := tx.Commit(ctx); err != nil {
				return res, err
			}
			return res, nil
		}
	}
	var cardInput, cardOutput, cardCached, estBound, estInput any
	var cardRevision string
	if card != nil {
		cardInput, cardOutput, cardRevision = card.inputPerMTok, card.outputPerMTok, card.revision
		if card.cachedPerMTok != nil {
			cardCached = *card.cachedPerMTok
		}
	}
	if req.Estimate.OutputTokensBound != nil {
		estBound = *req.Estimate.OutputTokensBound
	}
	estInput = req.Estimate.InputTokens
	if _, err := tx.Exec(ctx, `
		INSERT INTO usage_reservations
			(persona_id, fact_id, kind, phase, turn_id, input_id, round,
			 funding_kind, funding_id, funding, reserved_minor, currency,
			 bounded, est_input_tokens, est_output_bound,
			 rate_input_per_mtok, rate_output_per_mtok, rate_cached_per_mtok,
			 pricing_revision, generation, status)
		VALUES ($1, $2, $3, $4, NULLIF($5, ''), NULLIF($6, ''), $7,
			$8, $9, $10, $11, NULLIF($12, ''), $13, $14, $15,
			$16, $17, $18, NULLIF($19, ''), $20, 'held')`,
		personaID, req.FactID, req.Kind, req.Phase, req.TurnID, req.InputID, req.Round,
		req.Funding.Kind, req.Funding.ID, fundingSnapshot(req.Funding), reserved,
		cardCurrency(card), bounded, estInput, estBound,
		cardInput, cardOutput, cardCached, cardRevision, req.Generation); err != nil {
		return res, dataErr(err)
	}
	res.Admitted = true
	res.Reservation = &UsageReservation{
		FactID:        req.FactID,
		ReservedMinor: reserved,
		Currency:      cardCurrency(card),
		Bounded:       bounded,
		Status:        "held",
	}
	if err := tx.Commit(ctx); err != nil {
		return res, err
	}
	return res, nil
}

func cardCurrency(card *rateSnapshot) string {
	if card == nil {
		return ""
	}
	return card.currency
}

// budgetFits reports whether needed minor units fit the funding source's
// configured cap right now. No configured cap fits everything — absence
// is not an implicit limit.
func (s *Store) budgetFits(ctx context.Context, db queryRower, kind, id string, needed int64) (bool, error) {
	budget, err := s.budgetForFunding(ctx, db, kind, id)
	if err != nil {
		return false, err
	}
	if budget == nil {
		return true, nil
	}
	spent, held, err := s.fundingSpend(ctx, db, kind, id, budget.Currency)
	if err != nil {
		return false, err
	}
	return needed <= budget.LimitMinor && spent+held <= budget.LimitMinor-needed, nil
}

var factCols = `persona_id, fact_id, kind, phase, turn_id, input_id, round,
	funding_kind, funding_id, funding, status,
	input_tokens, output_tokens, cached_tokens, quantities,
	cost_minor, currency, cost_basis, pricing_revision, recorded_at`

func scanFact(row interface{ Scan(...any) error }) (UsageFact, error) {
	var f UsageFact
	var funding []byte
	err := row.Scan(&f.PersonaID, &f.FactID, &f.Kind, &f.Phase, &f.TurnID,
		&f.InputID, &f.Round, &f.Funding.Kind, &f.Funding.ID, &funding,
		&f.Status, &f.InputTokens, &f.OutputTokens, &f.CachedTokens,
		&f.Quantities, &f.CostMinor, &f.Currency, &f.CostBasis,
		&f.PricingRevision, &f.RecordedAt)
	if err != nil {
		return f, err
	}
	if len(funding) > 0 {
		var snap map[string]any
		if err := json.Unmarshal(funding, &snap); err != nil {
			return f, fmt.Errorf("decode funding snapshot: %w", err)
		}
		f.Funding.Version, _ = snap["version"].(string)
		f.Funding.Model, _ = snap["model"].(string)
		f.Funding.Provider, _ = snap["provider"].(string)
	}
	return f, nil
}

// fundingSnapshot preserves the non-secret funding identity captured at
// call time — what a later connection switch cannot rewrite.
func fundingSnapshot(f FundingRef) map[string]any {
	snap := map[string]any{"kind": f.Kind, "id": f.ID}
	if f.Version != "" {
		snap["version"] = f.Version
	}
	if f.Model != "" {
		snap["model"] = f.Model
	}
	if f.Provider != "" {
		snap["provider"] = f.Provider
	}
	return snap
}

// RecordUsage persists one call's resolved usage. Idempotent on
// (persona_id, fact_id): an identical redelivery replays the stored fact;
// a different payload under the same id conflicts — the caller generated
// a new fact id for a genuinely additional call. Deliberately not
// generation-fenced: the spend already happened.
func (s *Store) RecordUsage(ctx context.Context, personaID string, req UsageRecordRequest) (UsageFact, bool, error) {
	var fact UsageFact
	if req.FactID == "" || req.Kind == "" || req.Phase == "" {
		return fact, false, fmt.Errorf("%w: fact_id, kind and phase are required", ErrBadRequest)
	}
	if req.Status != "reported" && req.Status != "unknown" && req.Status != "not_sent" {
		return fact, false, fmt.Errorf("%w: status must be reported, unknown or not_sent", ErrBadRequest)
	}
	if hasNUL(req.Quantities) {
		return fact, false, fmt.Errorf("%w: quantities cannot contain NUL", ErrBadRequest)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fact, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// The reservation row carries the admission-time pricing snapshot —
	// lock it so settlement and a concurrent record of the same fact_id
	// serialize, and so the fact is priced under the card that admitted
	// the call even if the budget was edited or removed mid-call.
	var res struct {
		fundingKind, fundingID string
		reserved               int64
		currency               *string
		rateIn, rateOut        *int64
		rateCached             *int64
		revision               *string
	}
	var hasRes bool
	err = tx.QueryRow(ctx, `
		SELECT funding_kind, funding_id, reserved_minor, currency,
			rate_input_per_mtok, rate_output_per_mtok, rate_cached_per_mtok,
			pricing_revision
		FROM usage_reservations
		WHERE persona_id = $1 AND fact_id = $2 FOR UPDATE`,
		personaID, req.FactID).Scan(
		&res.fundingKind, &res.fundingID, &res.reserved, &res.currency,
		&res.rateIn, &res.rateOut, &res.rateCached, &res.revision)
	switch {
	case err == nil:
		hasRes = true
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return fact, false, err
	}

	// Funding authority: a matching reservation proves the call was
	// admitted under this funding when it was selected — it stays
	// attributable even if the connection was since deleted. With no
	// reservation the funding must validate as the persona's own now, so
	// a persona cannot attribute fabricated spend to another human's
	// funding source.
	reserved := hasRes && res.fundingKind == req.Funding.Kind && res.fundingID == req.Funding.ID
	if !reserved {
		if err := s.fundingAllowed(ctx, tx, personaID, req.Funding); err != nil {
			return fact, false, err
		}
	}

	// Rate card for this record: the admission snapshot when the call was
	// admitted under a configured budget; else the current budget (an
	// unadmitted record path); else unpriced.
	var card *rateSnapshot
	if hasRes && res.currency != nil && res.rateIn != nil && res.rateOut != nil {
		card = &rateSnapshot{
			currency:      *res.currency,
			inputPerMTok:  *res.rateIn,
			outputPerMTok: *res.rateOut,
			cachedPerMTok: res.rateCached,
		}
		if res.revision != nil {
			card.revision = *res.revision
		}
	} else {
		budget, err := s.budgetForFunding(ctx, tx, req.Funding.Kind, req.Funding.ID)
		if err != nil {
			return fact, false, err
		}
		if budget != nil {
			snap := budget.snapshot()
			card = &snap
		}
	}
	costMinor, currency, basis, revision, err := priceRecord(req, card, hasRes, res.reserved)
	if err != nil {
		return fact, false, err
	}
	if req.Quantities == nil {
		req.Quantities = map[string]any{}
	}
	fact, err = scanFact(tx.QueryRow(ctx, `
		INSERT INTO usage_facts
			(persona_id, fact_id, kind, phase, turn_id, input_id, round,
			 funding_kind, funding_id, funding, status,
			 input_tokens, output_tokens, cached_tokens, quantities,
			 cost_minor, currency, cost_basis, pricing_revision)
		VALUES ($1, $2, $3, $4, NULLIF($5, ''), NULLIF($6, ''), $7,
			$8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
		ON CONFLICT (persona_id, fact_id) DO NOTHING
		RETURNING `+factCols,
		personaID, req.FactID, req.Kind, req.Phase, req.TurnID, req.InputID, req.Round,
		req.Funding.Kind, req.Funding.ID, fundingSnapshot(req.Funding), req.Status,
		req.InputTokens, req.OutputTokens, req.CachedTokens, req.Quantities,
		costMinor, currency, basis, revision))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// Redelivery or a genuinely conflicting reuse of the fact id.
		existing, err := s.usageFact(ctx, tx, personaID, req.FactID)
		if err != nil {
			return fact, false, err
		}
		if !sameCallIdentity(*existing, req) {
			return fact, false, fmt.Errorf("%w: fact_id %s recorded with different content", ErrUsageFactConflict, req.FactID)
		}
		// Upgrade lattice: 'unrecorded' and 'unknown' facts can still be
		// superseded by the call's real report — the admission estimate is
		// replaced by actual priced spend, or cleared when the reporter
		// asserts the request never left. 'reported' and 'not_sent' are
		// terminal: only an identical payload replays.
		switch existing.Status {
		case "reported", "not_sent":
			if !factMatches(*existing, req) {
				return fact, false, fmt.Errorf("%w: fact_id %s recorded with different content", ErrUsageFactConflict, req.FactID)
			}
			fact = *existing
		case "unknown":
			if factMatches(*existing, req) {
				fact = *existing
				break
			}
			fallthrough
		case "unrecorded":
			if req.Status != "reported" && req.Status != "unknown" && req.Status != "not_sent" {
				return fact, false, fmt.Errorf("%w: fact_id %s recorded with different content", ErrUsageFactConflict, req.FactID)
			}
			fact, err = scanFact(tx.QueryRow(ctx, `
				UPDATE usage_facts SET
					status = $3, input_tokens = $4, output_tokens = $5,
					cached_tokens = $6, quantities = $7,
					cost_minor = $8, currency = $9, cost_basis = $10,
					pricing_revision = $11
				WHERE persona_id = $1 AND fact_id = $2
				RETURNING `+factCols,
				personaID, req.FactID, req.Status,
				req.InputTokens, req.OutputTokens, req.CachedTokens, req.Quantities,
				costMinor, currency, basis, revision))
			if err != nil {
				return fact, false, err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return fact, false, err
		}
		return fact, false, nil
	case err != nil:
		return fact, false, dataErr(err)
	}
	// The reservation for this call settles — its hold becomes the fact's
	// recorded cost. A 'not_sent' report releases the hold entirely: the
	// core asserts no request was produced. An already-reconciled
	// reservation (its 'unrecorded' fact just got upgraded by this late
	// report) stays settled — the fact now carries the actual spend.
	newStatus := "settled"
	if req.Status == "not_sent" {
		newStatus = "released"
	}
	if _, err := tx.Exec(ctx, `
		UPDATE usage_reservations SET status = $3, settled_at = now()
		WHERE persona_id = $1 AND fact_id = $2 AND status = 'held'`,
		personaID, req.FactID, newStatus); err != nil {
		return fact, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return fact, false, err
	}
	return fact, true, nil
}

// priceRecord derives the fact's cost fields from the rate card and the
// call's admission state:
//
//	reported   — priced under the card ('configured_rates').
//	unknown    — the admission estimate is retained as uncertain spend
//	             ('admission_estimate') when a priced reservation exists:
//	             an attempted call whose usage never resolved does not
//	             restore budget it may already have consumed.
//	not_sent   — unpriced; the reservation releases.
//	no card    — unpriced; there is nothing honest to charge against.
func priceRecord(req UsageRecordRequest, card *rateSnapshot, hasRes bool, reserved int64) (costMinor *int64, currency, basis, revision *string, err error) {
	switch req.Status {
	case "reported":
		if card == nil {
			return nil, nil, nil, nil, nil
		}
		priced, ok, err := priceReported(req, *card)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		if !ok {
			return nil, nil, nil, nil, nil
		}
		return &priced, &card.currency, ptrString("configured_rates"), &card.revision, nil
	case "unknown":
		if hasRes && card != nil && card.currency != "" {
			return &reserved, &card.currency, ptrString("admission_estimate"), &card.revision, nil
		}
		return nil, nil, nil, nil, nil
	case "not_sent":
		return nil, nil, nil, nil, nil
	}
	return nil, nil, nil, nil, nil
}

func ptrString(s string) *string { return &s }

// sameCallIdentity reports whether the stored fact and the request
// describe the same logical call — the identity fields a reporter may
// never change (status/tokens can still upgrade under the lattice).
func sameCallIdentity(f UsageFact, req UsageRecordRequest) bool {
	var turnID, inputID string
	var round int
	if f.TurnID != nil {
		turnID = *f.TurnID
	}
	if f.InputID != nil {
		inputID = *f.InputID
	}
	if f.Round != nil {
		round = *f.Round
	}
	return f.Kind == req.Kind && f.Phase == req.Phase &&
		turnID == req.TurnID && inputID == req.InputID && round == req.Round &&
		f.Funding.Kind == req.Funding.Kind && f.Funding.ID == req.Funding.ID
}

// factMatches compares a redelivery to the stored fact on the fields a
// caller could not legitimately change — identical means same fact.
func factMatches(f UsageFact, req UsageRecordRequest) bool {
	var turnID, inputID string
	var round int
	if f.TurnID != nil {
		turnID = *f.TurnID
	}
	if f.InputID != nil {
		inputID = *f.InputID
	}
	if f.Round != nil {
		round = *f.Round
	}
	var inTok, outTok, cachedTok int64
	if f.InputTokens != nil {
		inTok = *f.InputTokens
	}
	if f.OutputTokens != nil {
		outTok = *f.OutputTokens
	}
	if f.CachedTokens != nil {
		cachedTok = *f.CachedTokens
	}
	reqIn, reqOut, reqCached := int64(0), int64(0), int64(0)
	if req.InputTokens != nil {
		reqIn = *req.InputTokens
	}
	if req.OutputTokens != nil {
		reqOut = *req.OutputTokens
	}
	if req.CachedTokens != nil {
		reqCached = *req.CachedTokens
	}
	return f.Kind == req.Kind && f.Phase == req.Phase &&
		turnID == req.TurnID && inputID == req.InputID && round == req.Round &&
		f.Funding.Kind == req.Funding.Kind && f.Funding.ID == req.Funding.ID &&
		f.Status == req.Status &&
		inTok == reqIn && outTok == reqOut && cachedTok == reqCached
}

func (s *Store) usageFact(ctx context.Context, db queryRower, personaID, factID string) (*UsageFact, error) {
	f, err := scanFact(db.QueryRow(ctx,
		`SELECT `+factCols+` FROM usage_facts WHERE persona_id = $1 AND fact_id = $2`,
		personaID, factID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: usage fact", ErrTurnNotFound)
	}
	if err != nil {
		return nil, err
	}
	return &f, nil
}

// ListUsageFacts is the persona-scoped ledger view, oldest first.
func (s *Store) ListUsageFacts(ctx context.Context, personaID string, limit int) ([]UsageFact, error) {
	limit = clampLimit(limit, 100, 500)
	rows, err := s.pool.Query(ctx, `
		SELECT `+factCols+` FROM usage_facts
		WHERE persona_id = $1 ORDER BY recorded_at, fact_id LIMIT $2`,
		personaID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []UsageFact{}
	for rows.Next() {
		f, err := scanFact(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// ownsFunding verifies the human may inspect or configure this funding
// source: a 'connection' they own, or a 'sumi' grant to them.
func (s *Store) ownsFunding(ctx context.Context, db queryRower, humanID, kind, id string) error {
	switch kind {
	case "connection":
		var exists bool
		err := db.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM model_api_connections
				WHERE human_id = $1 AND connection_id = $2::uuid
			)`, humanID, id).Scan(&exists)
		if err != nil {
			return dataErr(err)
		}
		if !exists {
			// A real connection belonging to someone else is forbidden;
			// absence anywhere is not found — the caller cannot tell which.
			var anywhere bool
			if err := db.QueryRow(ctx,
				`SELECT EXISTS (SELECT 1 FROM model_api_connections WHERE connection_id = $1::uuid)`,
				id).Scan(&anywhere); err != nil {
				return err
			}
			if anywhere {
				return ErrFundingForbidden
			}
			return ErrFundingNotFound
		}
		return nil
	case "sumi":
		var exists bool
		err := db.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM usage_funding_grants
				WHERE funding_id = $1 AND human_id = $2 AND revoked_at IS NULL
			)`, id, humanID).Scan(&exists)
		if err != nil {
			return err
		}
		if !exists {
			var anywhere bool
			if err := db.QueryRow(ctx,
				`SELECT EXISTS (SELECT 1 FROM usage_funding_grants WHERE funding_id = $1)`,
				id).Scan(&anywhere); err != nil {
				return err
			}
			if anywhere {
				return ErrFundingForbidden
			}
			return ErrFundingNotFound
		}
		return nil
	default:
		return fmt.Errorf("%w: funding kind %q is not user-managed", ErrBadRequest, kind)
	}
}

// SetBudget installs or replaces the owner's cap on one funding source,
// then resumes waits the new cap can now admit. A rate card is required:
// a limit without rates cannot bound spend, so refusing the pair is more
// honest than silently admitting everything.
func (s *Store) SetBudget(ctx context.Context, humanID, kind, id string, b UsageBudget) (UsageBudget, error) {
	if err := s.ownsFunding(ctx, s.pool, humanID, kind, id); err != nil {
		return b, err
	}
	if b.LimitMinor < 0 || len(b.Currency) != 3 ||
		b.RateInputPerMTok < 0 || b.RateOutputPerMTok < 0 ||
		(b.RateCachedPerMTok != nil && *b.RateCachedPerMTok < 0) {
		return b, fmt.Errorf("%w: limit, 3-letter currency and non-negative rates are required", ErrBadRequest)
	}
	err := s.pool.QueryRow(ctx, `
		INSERT INTO usage_budgets
			(funding_kind, funding_id, limit_minor, currency,
			 rate_input_per_mtok, rate_output_per_mtok, rate_cached_per_mtok,
			 pricing_revision, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
		ON CONFLICT (funding_kind, funding_id) DO UPDATE SET
			limit_minor = $3, currency = $4,
			rate_input_per_mtok = $5, rate_output_per_mtok = $6,
			rate_cached_per_mtok = $7, pricing_revision = $8, updated_at = now()
		RETURNING updated_at`,
		kind, id, b.LimitMinor, b.Currency,
		b.RateInputPerMTok, b.RateOutputPerMTok, b.RateCachedPerMTok,
		b.PricingRevision).Scan(&b.UpdatedAt)
	if err != nil {
		return b, dataErr(err)
	}
	if _, err := s.ResumeWaitsForFunding(ctx, kind, id); err != nil {
		return b, err
	}
	b.FundingKind, b.FundingID = kind, id
	return b, nil
}

// SetBudgetAdmin installs a cap without human ownership — dev/test and
// placement administration for sources with no self-service surface
// ('sumi' grants, whose budget the funder manages; 'operator'/'env', the
// host's own default spend).
func (s *Store) SetBudgetAdmin(ctx context.Context, kind, id string, b UsageBudget) (UsageBudget, error) {
	if kind != "connection" && kind != "sumi" && kind != "operator" {
		return b, fmt.Errorf("%w: funding kind must be connection, sumi or operator", ErrBadRequest)
	}
	if b.LimitMinor < 0 || len(b.Currency) != 3 ||
		b.RateInputPerMTok < 0 || b.RateOutputPerMTok < 0 ||
		(b.RateCachedPerMTok != nil && *b.RateCachedPerMTok < 0) {
		return b, fmt.Errorf("%w: limit, 3-letter currency and non-negative rates are required", ErrBadRequest)
	}
	err := s.pool.QueryRow(ctx, `
		INSERT INTO usage_budgets
			(funding_kind, funding_id, limit_minor, currency,
			 rate_input_per_mtok, rate_output_per_mtok, rate_cached_per_mtok,
			 pricing_revision, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
		ON CONFLICT (funding_kind, funding_id) DO UPDATE SET
			limit_minor = $3, currency = $4,
			rate_input_per_mtok = $5, rate_output_per_mtok = $6,
			rate_cached_per_mtok = $7, pricing_revision = $8, updated_at = now()
		RETURNING updated_at`,
		kind, id, b.LimitMinor, b.Currency,
		b.RateInputPerMTok, b.RateOutputPerMTok, b.RateCachedPerMTok,
		b.PricingRevision).Scan(&b.UpdatedAt)
	if err != nil {
		return b, dataErr(err)
	}
	if _, err := s.ResumeWaitsForFunding(ctx, kind, id); err != nil {
		return b, err
	}
	b.FundingKind, b.FundingID = kind, id
	return b, nil
}

// ClearBudget removes the cap entirely — uncapped is an explicit state,
// not a default — and resumes every input waiting on that funding.
func (s *Store) ClearBudget(ctx context.Context, humanID, kind, id string) error {
	if err := s.ownsFunding(ctx, s.pool, humanID, kind, id); err != nil {
		return err
	}
	return s.clearBudget(ctx, kind, id)
}

// ClearBudgetAdmin removes a cap without human ownership (dev/admin).
func (s *Store) ClearBudgetAdmin(ctx context.Context, kind, id string) error {
	if kind != "connection" && kind != "sumi" && kind != "operator" {
		return fmt.Errorf("%w: funding kind must be connection, sumi or operator", ErrBadRequest)
	}
	return s.clearBudget(ctx, kind, id)
}

func (s *Store) clearBudget(ctx context.Context, kind, id string) error {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM usage_budgets WHERE funding_kind = $1 AND funding_id = $2`,
		kind, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrFundingNotFound
	}
	_, err = s.ResumeWaitsForFunding(ctx, kind, id)
	return err
}

// BudgetView returns the configured budget plus live spend for display.
func (s *Store) BudgetView(ctx context.Context, kind, id string) (*UsageBudget, error) {
	b, err := s.budgetForFunding(ctx, s.pool, kind, id)
	if err != nil || b == nil {
		return b, err
	}
	spent, held, err := s.fundingSpend(ctx, s.pool, kind, id, b.Currency)
	if err != nil {
		return nil, err
	}
	b.SpentMinor, b.HeldMinor = spent, held
	b.RemainingMinor = b.LimitMinor - spent - held
	return b, nil
}

// ResumeWaitsForFunding requeues inputs parked on this funding source —
// called after a budget increase or removal. Waits whose needed amount
// still does not fit stay parked; the resumed ones accumulate their
// parked time into waited_ms like an approval wait.
func (s *Store) ResumeWaitsForFunding(ctx context.Context, kind, id string) (int, error) {
	fitsNeeded := int64(-1) // -1: resume everything — the cap is gone
	budget, err := s.budgetForFunding(ctx, s.pool, kind, id)
	if err != nil {
		return 0, err
	}
	if budget != nil {
		spent, held, err := s.fundingSpend(ctx, s.pool, kind, id, budget.Currency)
		if err != nil {
			return 0, err
		}
		fitsNeeded = budget.LimitMinor - spent - held
	}
	// Memory chunks reshelved on a budget wait belong to the funding
	// owner's personas — resolve the owner so they unpark too.
	var humans []string
	switch kind {
	case "connection":
		var h string
		if err := s.pool.QueryRow(ctx,
			`SELECT human_id FROM model_api_connections WHERE connection_id = $1::uuid`,
			id).Scan(&h); err == nil {
			humans = []string{h}
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return 0, err
		}
	case "sumi":
		var h string
		if err := s.pool.QueryRow(ctx,
			`SELECT human_id FROM usage_funding_grants WHERE funding_id = $1 AND revoked_at IS NULL`,
			id).Scan(&h); err == nil {
			humans = []string{h}
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return 0, err
		}
	}
	return s.resumeWaits(ctx, `
		DELETE FROM core_budget_waits w
		WHERE w.funding_kind = $1 AND w.funding_id = $2
			AND ($3 < 0 OR w.needed_minor <= $3)
		RETURNING w.persona_id, w.input_id`, humans, kind, id, fitsNeeded)
}

// ResumeWaitsForHuman requeues every budget-parked input of this human's
// personas — called when the human's model selection or a connection
// changed, because the next attempt re-resolves funding and may spend a
// different source entirely.
func (s *Store) ResumeWaitsForHuman(ctx context.Context, humanID string) (int, error) {
	return s.resumeWaits(ctx, `
		DELETE FROM core_budget_waits w
		USING core_personas p
		WHERE w.persona_id = p.persona_id AND p.human_id = $1
		RETURNING w.persona_id, w.input_id`, []string{humanID}, humanID)
}

// resumeWaits deletes the matching wait rows, requeues their inputs, and
// unparks memory chunks those humans' personas reshelved on a budget wait
// (marked by the budget-wait reason prefix) so preparation resumes on the
// next maintenance pass instead of sleeping out its pacing.
func (s *Store) resumeWaits(ctx context.Context, deleteQuery string, humans []string, args ...any) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, deleteQuery, args...)
	if err != nil {
		return 0, err
	}
	type parked struct{ persona, input string }
	var resuming []parked
	for rows.Next() {
		var p parked
		if err := rows.Scan(&p.persona, &p.input); err != nil {
			rows.Close()
			return 0, err
		}
		resuming = append(resuming, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	n := 0
	for _, p := range resuming {
		// Requeue only an input still parked, and only when nothing else
		// holds it — a pending approval is a second, independent wait.
		tag, err := tx.Exec(ctx, `
			UPDATE core_inputs i SET status = 'queued', claimed_generation = NULL,
				turn_id = NULL, not_before = NULL,
				waited_ms = i.waited_ms + COALESCE(EXTRACT(EPOCH FROM (now() - i.waiting_since)) * 1000, 0)::bigint,
				waiting_since = NULL
			WHERE i.persona_id = $1 AND i.input_id = $2 AND i.status = 'waiting'
				AND NOT EXISTS (
					SELECT 1 FROM core_tool_approvals a
					WHERE a.persona_id = i.persona_id AND a.input_id = i.input_id
						AND a.status = 'pending'
				)`,
			p.persona, p.input)
		if err != nil {
			return 0, err
		}
		if tag.RowsAffected() > 0 {
			n++
		}
	}
	if len(humans) > 0 {
		if _, err := tx.Exec(ctx, `
			UPDATE core_memory_chunks mc SET not_before = NULL
			FROM core_personas p
			WHERE mc.persona_id = p.persona_id AND p.human_id = ANY($1::uuidv7[])
				AND mc.status = 'sealed' AND mc.not_before IS NOT NULL
				AND mc.last_error LIKE 'budget-wait:%'`, humans); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return n, nil
}

// BudgetWaitsForHuman lists the human's personas' parked budget waits —
// what is waiting on which funding, for the settings surface.
func (s *Store) BudgetWaitsForHuman(ctx context.Context, humanID string) ([]BudgetWaitRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT w.persona_id, w.input_id, w.turn_id, w.funding_kind, w.funding_id,
			w.needed_minor, w.currency, w.created_at
		FROM core_budget_waits w
		JOIN core_personas p ON p.persona_id = w.persona_id
		WHERE p.human_id = $1 ORDER BY w.created_at`, humanID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []BudgetWaitRow{}
	for rows.Next() {
		var w BudgetWaitRow
		if err := rows.Scan(&w.PersonaID, &w.InputID, &w.TurnID,
			&w.FundingKind, &w.FundingID, &w.NeededMinor, &w.Currency, &w.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// UsageSourceView is one funding source's summary for its owner.
type UsageSourceView struct {
	Kind     string       `json:"kind"`
	ID       string       `json:"id"`
	Name     string       `json:"name,omitempty"`
	Model    string       `json:"model,omitempty"`
	Preset   string       `json:"preset,omitempty"`
	Selected bool         `json:"selected"`
	Grant    bool         `json:"grant,omitempty"`
	Budget   *UsageBudget `json:"budget,omitempty"`
	Totals   UsageTotals  `json:"totals"`
	Recent   []UsageFact  `json:"recent"`
}

// UsageForHuman lists the funding sources the human may inspect — their
// own API connections and live Sumi grants — each with totals, the
// configured budget, and recent facts.
func (s *Store) UsageForHuman(ctx context.Context, humanID string, recentLimit int) ([]UsageSourceView, error) {
	recentLimit = clampLimit(recentLimit, 20, 100)
	var views []UsageSourceView

	connRows, err := s.pool.Query(ctx, `
		SELECT c.connection_id, c.name, c.preset, c.model,
			EXISTS (
				SELECT 1 FROM model_connection_selections sel
				WHERE sel.human_id = c.human_id AND sel.kind = 'api'
					AND sel.connection_id = c.connection_id
			) AS selected
		FROM model_api_connections c WHERE c.human_id = $1
		ORDER BY c.name`, humanID)
	if err != nil {
		return nil, err
	}
	type connRow struct {
		id, name, preset, model string
		selected                bool
	}
	var conns []connRow
	for connRows.Next() {
		var c connRow
		if err := connRows.Scan(&c.id, &c.name, &c.preset, &c.model, &c.selected); err != nil {
			connRows.Close()
			return nil, err
		}
		conns = append(conns, c)
	}
	connRows.Close()
	if err := connRows.Err(); err != nil {
		return nil, err
	}
	for _, c := range conns {
		v := UsageSourceView{
			Kind: "connection", ID: c.id, Name: c.name,
			Preset: c.preset, Model: c.model, Selected: c.selected,
			Recent: []UsageFact{},
		}
		views = append(views, v)
	}

	grantRows, err := s.pool.Query(ctx, `
		SELECT funding_id, label FROM usage_funding_grants
		WHERE human_id = $1 AND revoked_at IS NULL ORDER BY created_at`, humanID)
	if err != nil {
		return nil, err
	}
	for grantRows.Next() {
		var v UsageSourceView
		v.Kind = "sumi"
		v.Grant = true
		v.Recent = []UsageFact{}
		if err := grantRows.Scan(&v.ID, &v.Name); err != nil {
			grantRows.Close()
			return nil, err
		}
		views = append(views, v)
	}
	grantRows.Close()
	if err := grantRows.Err(); err != nil {
		return nil, err
	}

	for i := range views {
		v := &views[i]
		v.Budget, err = s.BudgetView(ctx, v.Kind, v.ID)
		if err != nil {
			return nil, err
		}
		v.Totals, err = s.usageTotals(ctx, v.Kind, v.ID)
		if err != nil {
			return nil, err
		}
		v.Recent, err = s.recentFactsForFunding(ctx, v.Kind, v.ID, recentLimit)
		if err != nil {
			return nil, err
		}
	}
	if views == nil {
		views = []UsageSourceView{}
	}
	return views, nil
}

func (s *Store) usageTotals(ctx context.Context, kind, id string) (UsageTotals, error) {
	var t UsageTotals
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*),
			COUNT(*) FILTER (WHERE status IN ('unknown','unrecorded')),
			COUNT(*) FILTER (WHERE cost_minor IS NULL),
			COALESCE(SUM(input_tokens), 0),
			COALESCE(SUM(output_tokens), 0),
			COALESCE(SUM(cached_tokens), 0)
		FROM usage_facts WHERE funding_kind = $1 AND funding_id = $2`,
		kind, id).Scan(&t.Calls, &t.UnknownCalls, &t.UnpricedCalls,
		&t.InputTokens, &t.OutputTokens, &t.CachedTokens)
	if err != nil {
		return t, err
	}
	// Spend is currency-dimensioned: facts recorded under different
	// rate-card currencies are reported per currency, never summed into
	// one integer.
	rows, err := s.pool.Query(ctx, `
		SELECT currency, SUM(cost_minor) FROM usage_facts
		WHERE funding_kind = $1 AND funding_id = $2
			AND cost_minor IS NOT NULL AND currency IS NOT NULL
		GROUP BY currency`, kind, id)
	if err != nil {
		return t, err
	}
	defer rows.Close()
	t.Costs = map[string]int64{}
	for rows.Next() {
		var ccy string
		var sum int64
		if err := rows.Scan(&ccy, &sum); err != nil {
			return t, err
		}
		t.Costs[ccy] = sum
	}
	return t, rows.Err()
}

// recentFactsForFunding lists recent facts for one owned funding source.
func (s *Store) recentFactsForFunding(ctx context.Context, kind, id string, limit int) ([]UsageFact, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+factCols+` FROM usage_facts
		WHERE funding_kind = $1 AND funding_id = $2
		ORDER BY recorded_at DESC, fact_id DESC LIMIT $3`, kind, id, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []UsageFact{}
	for rows.Next() {
		f, err := scanFact(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// FactsForFunding is the owned-funding fact list for the inspection route.
func (s *Store) FactsForFunding(ctx context.Context, humanID, kind, id string, limit int) ([]UsageFact, error) {
	if err := s.ownsFunding(ctx, s.pool, humanID, kind, id); err != nil {
		return nil, err
	}
	return s.recentFactsForFunding(ctx, kind, id, clampLimit(limit, 50, 200))
}

// reconcileHeldReservations resolves holds whose facts never landed. A
// held reservation with no usage_facts row is an admit whose provider call
// may already have consumed money or still be running remotely — a lost
// response does not prove the request never left. It is NOT released back
// to the allowance: reconciliation writes an inspectable 'unrecorded' fact
// carrying the admission estimate as 'admission_estimate' spend (no cost
// when the call was admitted under no configured budget), then settles the
// hold. A later real report for the same fact_id upgrades the row under
// the record lattice. A call the core knows was never sent reports
// 'not_sent' and releases instead — this path cannot make that claim.
// Called inside the turn-commit transaction for the committing turn, and
// at recovery for dead generations.
func reconcileHeldReservations(ctx context.Context, tx pgx.Tx, personaID string, turnID string, belowGeneration *int64) error {
	where := `r.persona_id = $1 AND r.status = 'held'`
	args := []any{personaID}
	if turnID != "" {
		args = append(args, turnID)
		where += fmt.Sprintf(" AND r.turn_id = $%d", len(args))
	}
	if belowGeneration != nil {
		args = append(args, *belowGeneration)
		where += fmt.Sprintf(" AND r.generation < $%d", len(args))
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO usage_facts
			(persona_id, fact_id, kind, phase, turn_id, input_id, round,
			 funding_kind, funding_id, funding, status,
			 cost_minor, currency, cost_basis, pricing_revision)
		SELECT r.persona_id, r.fact_id, r.kind, r.phase, r.turn_id,
			r.input_id, r.round, r.funding_kind, r.funding_id, r.funding,
			'unrecorded',
			CASE WHEN r.currency IS NOT NULL THEN r.reserved_minor END,
			r.currency,
			CASE WHEN r.currency IS NOT NULL THEN 'admission_estimate' END,
			r.pricing_revision
		FROM usage_reservations r
		WHERE `+where+`
			AND NOT EXISTS (
				SELECT 1 FROM usage_facts f
				WHERE f.persona_id = r.persona_id AND f.fact_id = r.fact_id
			)
		ON CONFLICT (persona_id, fact_id) DO NOTHING`, args...); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		UPDATE usage_reservations r SET status = 'settled', settled_at = now()
		WHERE `+where, args...)
	return err
}
