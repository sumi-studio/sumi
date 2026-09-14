// Package agentstate implements the persona-scoped canonical state service
// for the TypeScript secretary core (engineering plan §3, M02). PostgreSQL is
// the canonical record: durable inputs, turns, journal events, operations,
// schedules, and the writer lease that enforces single-writer exclusion.
//
// Writer exclusion is DB-enforced, not host-assumed: every mutation presents
// the caller's writer generation and the store re-checks it against
// core_writer_leases inside the same transaction, so a fenced-out generation
// cannot commit. Lease expiry is a liveness hint; correctness does not depend
// on the clock.
//
// The turn protocol is deliberately coarse — LoadTurn and CommitTurn — so a
// remote core pays one round trip per boundary instead of many.
package agentstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrPersonaNotFound = errors.New("persona not found")
	ErrWriterHeld      = errors.New("writer lease held by another live holder")
	ErrGenerationFence = errors.New("writer generation fenced")
	ErrInputNotFound   = errors.New("input not found")
	ErrTurnNotFound    = errors.New("turn not found")
	ErrTurnConflict    = errors.New("conflicting turn state")
	ErrOpNotFound      = errors.New("operation not found")
	ErrUnknownTool     = errors.New("unknown tool")
	ErrBadRequest      = errors.New("bad request")
	// ErrPersonaInactive: the persona is sealed for, staged by, or already
	// moved by a transfer (internal/portable), so this placement may not run
	// it or accept new inputs for it.
	ErrPersonaInactive = errors.New("persona is not active in this placement")
)

// dataErr maps deterministic PostgreSQL data errors — class 22 data
// exceptions (e.g. 22P05 unsupported Unicode escape) and 23514 check
// violations — to ErrBadRequest. They are caused by the submitted
// content, are never transient, and must surface as 400 so the caller
// records a tool/decision error instead of retrying the same write
// forever.
func dataErr(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) &&
		(strings.HasPrefix(pgErr.Code, "22") || pgErr.Code == "23514") {
		return fmt.Errorf("%w: %s", ErrBadRequest, pgErr.Message)
	}
	return err
}

// hasNUL reports whether any string in v (after JSON normalization)
// contains NUL — PostgreSQL jsonb cannot store it (22P05). Checked
// explicitly so the failure is a clean 400 at the first persistence
// boundary rather than a wrapped driver error at a later one.
func hasNUL(v any) bool {
	switch t := v.(type) {
	case string:
		return strings.ContainsRune(t, 0)
	case map[string]any:
		for _, e := range t {
			if hasNUL(e) {
				return true
			}
		}
	case []any:
		for _, e := range t {
			if hasNUL(e) {
				return true
			}
		}
	}
	return false
}

type Persona struct {
	PersonaID   string    `json:"persona_id"`
	HumanID     *string   `json:"human_id"`
	DisplayName string    `json:"display_name"`
	CreatedAt   time.Time `json:"created_at"`
	// Authority is active, sealed, staged or transferred (migration 0049).
	Authority  string  `json:"authority"`
	TransferID *string `json:"transfer_id"`
}

type WriterLease struct {
	PersonaID  string    `json:"persona_id"`
	Generation int64     `json:"generation"`
	HolderID   string    `json:"holder_id"`
	AcquiredAt time.Time `json:"acquired_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}

// Input is a durable inbox entry. Provenance and attention are contract-level
// fields so shared-channel routing ("who this concerns / who should notice")
// does not need a later retrofit.
type Input struct {
	PersonaID         string         `json:"persona_id"`
	InputID           string         `json:"input_id"`
	Kind              string         `json:"kind"`
	Payload           map[string]any `json:"payload"`
	ActorKind         string         `json:"actor_kind"`
	ActorID           string         `json:"actor_id"`
	SourceSurface     string         `json:"source_surface"`
	ThreadID          string         `json:"thread_id"`
	OccurredAt        *time.Time     `json:"occurred_at"`
	Attention         string         `json:"attention"`
	Status            string         `json:"status"`
	ClaimedGeneration *int64         `json:"claimed_generation"`
	TurnID            *string        `json:"turn_id"`
	CreatedAt         time.Time      `json:"created_at"`
	DoneAt            *time.Time     `json:"done_at"`
	// NotBefore delays a retryable-failed input's next claim — the
	// durable bound that keeps one failing input from hot-looping and
	// starving every later queued input.
	NotBefore *time.Time `json:"not_before"`
}

type Turn struct {
	PersonaID  string         `json:"persona_id"`
	TurnID     string         `json:"turn_id"`
	InputID    string         `json:"input_id"`
	Generation int64          `json:"generation"`
	Attempt    int            `json:"attempt"`
	Status     string         `json:"status"`
	StartedAt  time.Time      `json:"started_at"`
	FinishedAt *time.Time     `json:"finished_at"`
	Output     map[string]any `json:"output"`
	Usage      map[string]any `json:"usage"`
	Error      *string        `json:"error"`
}

type Event struct {
	PersonaID string         `json:"persona_id"`
	Seq       int64          `json:"seq"`
	TurnID    string         `json:"turn_id"`
	Kind      string         `json:"kind"`
	Payload   map[string]any `json:"payload"`
	CreatedAt time.Time      `json:"created_at"`
}

// Operation is the side-effect ledger row. response carries the effect
// receipt (for external effects, e.g. a file version token) so a crash
// between an effect and its recording reconciles here instead of leaving an
// unrecorded effect or a dangling record.
type Operation struct {
	PersonaID         string         `json:"persona_id"`
	OperationID       string         `json:"operation_id"`
	TurnID            string         `json:"turn_id"`
	Tool              string         `json:"tool"`
	IdempotencyKey    string         `json:"idempotency_key"`
	Request           map[string]any `json:"request"`
	Status            string         `json:"status"`
	Response          map[string]any `json:"response"`
	ClaimedGeneration int64          `json:"claimed_generation"`
	CreatedAt         time.Time      `json:"created_at"`
	CompletedAt       *time.Time     `json:"completed_at"`
}

type Schedule struct {
	PersonaID         string         `json:"persona_id"`
	ScheduleID        string         `json:"schedule_id"`
	WakeAt            time.Time      `json:"wake_at"`
	Payload           map[string]any `json:"payload"`
	MissPolicy        string         `json:"miss_policy"`
	Status            string         `json:"status"`
	ClaimedGeneration *int64         `json:"claimed_generation"`
	CreatedAt         time.Time      `json:"created_at"`
	FiredAt           *time.Time     `json:"fired_at"`
}

type OutboxEntry struct {
	PersonaID   string         `json:"persona_id"`
	Seq         int64          `json:"seq"`
	Kind        string         `json:"kind"`
	Payload     map[string]any `json:"payload"`
	CreatedAt   time.Time      `json:"created_at"`
	DeliveredAt *time.Time     `json:"delivered_at"`
}

type PersonaState struct {
	Persona          Persona      `json:"persona"`
	Lease            *WriterLease `json:"lease"`
	QueuedInputs     int          `json:"queued_inputs"`
	RunningTurn      *string      `json:"running_turn"`
	PendingSchedules int          `json:"pending_schedules"`
	LatestEventSeq   int64        `json:"latest_event_seq"`
}

type RecoverResult struct {
	InterruptedTurns []string `json:"interrupted_turns"`
	RequeuedInputs   []string `json:"requeued_inputs"`
	ReleasedClaims   []string `json:"released_schedule_claims"`
}

// EventInput is one journal entry committed with a turn or recorded by a
// state-internal tool.
type EventInput struct {
	Kind    string         `json:"kind"`
	Payload map[string]any `json:"payload"`
}

// PlanCall is one decided tool call inside a durable plan. call_id is the
// model's own identifier (kept verbatim for later provider tool_calls
// reconstruction); the call's position in calls is the durable identity.
type PlanCall struct {
	CallID  string         `json:"call_id,omitempty"`
	Tool    string         `json:"tool"`
	Request map[string]any `json:"request"`
}

// Decision is what the model decided in one round of a turn: the text it
// produced, the ordered tool calls to execute, and reported usage. Each
// round is persisted before any of its effects run; recovery continues it
// instead of re-planning.
type Decision struct {
	Text  string         `json:"text"`
	Calls []PlanCall     `json:"calls"`
	Usage map[string]any `json:"usage"`
}

// TurnPlan is the durable record of one input's decisions. One row per
// input; Plan is the append-only list of rounds — a recorded round never
// changes, a new round may only be appended by the live turn. A replayed
// identical save returns the stored row; a conflicting save is rejected.
// A round with zero calls is the turn's final decision — its text is the
// reply, informed by the committed tool results of earlier rounds.
type TurnPlan struct {
	PersonaID  string     `json:"persona_id"`
	InputID    string     `json:"input_id"`
	TurnID     string     `json:"turn_id"`
	Generation int64      `json:"generation"`
	Plan       []Decision `json:"plan"`
	CreatedAt  time.Time  `json:"created_at"`
}

// LoadResult is one coarse read: the running or freshly begun turn, its
// claimed input, and the journal tail the core assembles context from.
type LoadResult struct {
	Turn    *Turn   `json:"turn"`
	Input   *Input  `json:"input"`
	Context []Event `json:"context"`
	// Memory holds the applied L1 replacement blocks. Each renders at the
	// journal position where its events were — the core interleaves them
	// with the raw tail by sequence position.
	Memory []MemoryBlock `json:"memory"`
	// Omitted is the extent of older raw records outside the send cap —
	// still stored and readable through conversation_history; nil if none.
	Omitted *OmittedRange `json:"omitted"`
	// MemoryOmitted is the extent of older applied memory blocks outside
	// the memory cap — stored, originals readable; nil if none.
	MemoryOmitted *OmittedMemory `json:"memory_omitted"`
	// Plan is the input's recorded decision, if one exists — returned on
	// both the fresh-claim and running-turn replay paths so a retried
	// attempt continues the recorded plan rather than re-planning.
	Plan *TurnPlan `json:"plan"`
}

// CommitRequest is the single end-of-turn write: journal entries plus the
// turn outcome. Complete marks the input done and appends the outbox record;
// a failed retryable outcome requeues the input.
type CommitRequest struct {
	Outcome   string         `json:"outcome"` // "complete" | "fail"
	Events    []EventInput   `json:"events"`
	Output    map[string]any `json:"output"`
	Usage     map[string]any `json:"usage"`
	Error     string         `json:"error"`
	Retryable bool           `json:"retryable"`
	// RetryAfterMs is provider-supplied pacing (HTTP Retry-After) for a
	// retryable failure: the requeue's not_before is at least
	// now()+RetryAfterMs on top of the per-attempt backoff. Clamped to
	// [0, 2min] — a hint can slow the next retry, never silence it.
	RetryAfterMs int64 `json:"retry_after_ms,omitempty"`
}

// NewTurnID is supplied by the caller so LoadTurn retries can be linked; the
// service generates one per attempt internally when needed.
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// requireGeneration locks the writer lease row and verifies the presented
// generation. Holding the row lock for the rest of the transaction also
// serializes mutations from callers sharing one generation.
//
// The persona's placement authority is part of the fence: a sealed, staged or
// transferred persona admits no mutation even under a matching generation, so
// a staged import can never be driven by a caller that guesses its epoch.
func requireGeneration(ctx context.Context, tx pgx.Tx, personaID string, generation int64) error {
	var current int64
	var authority string
	err := tx.QueryRow(ctx,
		`SELECT l.generation, p.authority
		 FROM core_writer_leases l JOIN core_personas p ON p.persona_id = l.persona_id
		 WHERE l.persona_id = $1 FOR UPDATE OF l`,
		personaID).Scan(&current, &authority)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrGenerationFence
	}
	if err != nil {
		return fmt.Errorf("read writer lease: %w", err)
	}
	if current != generation {
		return ErrGenerationFence
	}
	if authority != "active" {
		return fmt.Errorf("%w: persona authority is %s", ErrGenerationFence, authority)
	}
	return nil
}

func (s *Store) EnsurePersona(ctx context.Context, personaID string, humanID *string, displayName string) (Persona, bool, error) {
	var p Persona
	err := s.pool.QueryRow(ctx, `
		INSERT INTO core_personas (persona_id, human_id, display_name)
		VALUES ($1, $2, $3)
		ON CONFLICT (persona_id) DO NOTHING
		RETURNING persona_id, human_id, display_name, created_at, authority, transfer_id`,
		personaID, humanID, displayName).
		Scan(&p.PersonaID, &p.HumanID, &p.DisplayName, &p.CreatedAt, &p.Authority, &p.TransferID)
	if errors.Is(err, pgx.ErrNoRows) {
		err = s.pool.QueryRow(ctx,
			`SELECT persona_id, human_id, display_name, created_at, authority, transfer_id FROM core_personas WHERE persona_id = $1`,
			personaID).Scan(&p.PersonaID, &p.HumanID, &p.DisplayName, &p.CreatedAt, &p.Authority, &p.TransferID)
		if errors.Is(err, pgx.ErrNoRows) {
			return p, false, ErrPersonaNotFound
		}
		return p, false, err
	}
	if err != nil {
		return Persona{}, false, fmt.Errorf("ensure persona: %w", err)
	}
	return p, true, nil
}

func (s *Store) PersonaState(ctx context.Context, personaID string) (PersonaState, error) {
	var st PersonaState
	err := s.pool.QueryRow(ctx,
		`SELECT persona_id, human_id, display_name, created_at, authority, transfer_id FROM core_personas WHERE persona_id = $1`,
		personaID).Scan(&st.Persona.PersonaID, &st.Persona.HumanID, &st.Persona.DisplayName, &st.Persona.CreatedAt,
		&st.Persona.Authority, &st.Persona.TransferID)
	if errors.Is(err, pgx.ErrNoRows) {
		return st, ErrPersonaNotFound
	}
	if err != nil {
		return st, err
	}
	var lease WriterLease
	err = s.pool.QueryRow(ctx,
		`SELECT persona_id, generation, holder_id, acquired_at, expires_at FROM core_writer_leases WHERE persona_id = $1`,
		personaID).Scan(&lease.PersonaID, &lease.Generation, &lease.HolderID, &lease.AcquiredAt, &lease.ExpiresAt)
	switch {
	case err == nil:
		st.Lease = &lease
	case !errors.Is(err, pgx.ErrNoRows):
		return st, err
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM core_inputs WHERE persona_id = $1 AND status = 'queued'`,
		personaID).Scan(&st.QueuedInputs); err != nil {
		return st, err
	}
	var running *string
	if err := s.pool.QueryRow(ctx,
		`SELECT turn_id FROM core_turns WHERE persona_id = $1 AND status = 'running'`,
		personaID).Scan(&running); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return st, err
	}
	st.RunningTurn = running
	if err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM core_schedules WHERE persona_id = $1 AND status = 'pending'`,
		personaID).Scan(&st.PendingSchedules); err != nil {
		return st, err
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(seq), 0) FROM core_events WHERE persona_id = $1`,
		personaID).Scan(&st.LatestEventSeq); err != nil {
		return st, err
	}
	return st, nil
}

// AcquireWriter takes the persona writer lease when free, expired, or already
// held by the same holder, returning the new fencing generation. Only an
// active persona can be acquired; a transfer seal also parks the lease on a
// far-future expiry, so a concurrent acquire cannot slip past the seal.
func (s *Store) AcquireWriter(ctx context.Context, personaID, holderID string, ttl time.Duration) (WriterLease, error) {
	var lease WriterLease
	err := s.pool.QueryRow(ctx, `
		INSERT INTO core_writer_leases (persona_id, generation, holder_id, expires_at)
		SELECT $1::uuidv7, 1, $2, now() + $3::interval FROM core_personas
		WHERE persona_id = $1::uuidv7 AND authority = 'active'
		ON CONFLICT (persona_id) DO UPDATE SET
			generation  = core_writer_leases.generation + 1,
			holder_id   = EXCLUDED.holder_id,
			acquired_at = now(),
			expires_at  = EXCLUDED.expires_at
		WHERE core_writer_leases.expires_at < now()
		   OR core_writer_leases.holder_id = EXCLUDED.holder_id
		RETURNING persona_id, generation, holder_id, acquired_at, expires_at`,
		personaID, holderID, ttl).
		Scan(&lease.PersonaID, &lease.Generation, &lease.HolderID, &lease.AcquiredAt, &lease.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		var authority string
		err := s.pool.QueryRow(ctx,
			`SELECT authority FROM core_personas WHERE persona_id = $1`,
			personaID).Scan(&authority)
		if errors.Is(err, pgx.ErrNoRows) {
			return lease, ErrPersonaNotFound
		}
		if err != nil {
			return lease, err
		}
		if authority != "active" {
			return lease, fmt.Errorf("%w: authority is %s", ErrPersonaInactive, authority)
		}
		return lease, ErrWriterHeld
	}
	if err != nil {
		return lease, fmt.Errorf("acquire writer: %w", err)
	}
	return lease, nil
}

func (s *Store) RenewWriter(ctx context.Context, personaID, holderID string, generation int64, ttl time.Duration) (WriterLease, error) {
	var lease WriterLease
	err := s.pool.QueryRow(ctx, `
		UPDATE core_writer_leases SET expires_at = now() + $4::interval
		WHERE persona_id = $1 AND generation = $2 AND holder_id = $3
		RETURNING persona_id, generation, holder_id, acquired_at, expires_at`,
		personaID, generation, holderID, ttl).
		Scan(&lease.PersonaID, &lease.Generation, &lease.HolderID, &lease.AcquiredAt, &lease.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return lease, ErrGenerationFence
	}
	if err != nil {
		return lease, fmt.Errorf("renew writer: %w", err)
	}
	return lease, nil
}

// ReleaseWriter expires the lease row in place rather than deleting it:
// the generation must be monotonic per persona, so the next acquire goes
// through the ON CONFLICT path and returns generation+1. A deleted row
// would restart generation at 1 and admit a stale holder's in-flight
// mutation under the recycled fencing token. The expiry is a fixed past
// instant, not now(): a now()-written "dead" marker can look live to a
// later transaction after the host clock steps backward.
func (s *Store) ReleaseWriter(ctx context.Context, personaID, holderID string, generation int64) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE core_writer_leases SET expires_at = 'epoch'::timestamptz
		 WHERE persona_id = $1 AND generation = $2 AND holder_id = $3`,
		personaID, generation, holderID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrGenerationFence
	}
	return nil
}

const inputCols = `persona_id, input_id, kind, payload, actor_kind, actor_id,
	source_surface, thread_id, occurred_at, attention, status,
	claimed_generation, turn_id, created_at, done_at, not_before`

type inputScanner interface {
	Scan(dest ...any) error
}

func scanInput(row inputScanner) (Input, error) {
	var in Input
	err := row.Scan(&in.PersonaID, &in.InputID, &in.Kind, &in.Payload,
		&in.ActorKind, &in.ActorID, &in.SourceSurface, &in.ThreadID,
		&in.OccurredAt, &in.Attention, &in.Status, &in.ClaimedGeneration,
		&in.TurnID, &in.CreatedAt, &in.DoneAt, &in.NotBefore)
	if errors.Is(err, pgx.ErrNoRows) {
		return in, ErrInputNotFound
	}
	return in, err
}

// schedInputPrefix is reserved for wake inputs enqueued by schedule
// dispatch; a caller-supplied input_id in that namespace could otherwise
// pre-occupy a schedule's wake slot and silently drop the wake.
const schedInputPrefix = "sched:"

// SubmitInput durably records an input. Re-submitting the same input_id
// replays the stored row — this is the replay-after-lost-response path.
// A replay whose fields differ from the stored request is a contract
// violation, not idempotency: it is rejected rather than answered with a
// receipt for a different request.
func (s *Store) SubmitInput(ctx context.Context, in *Input) (Input, bool, error) {
	if strings.HasPrefix(in.InputID, schedInputPrefix) || strings.HasPrefix(in.InputID, jobInputPrefix) {
		return Input{}, false, fmt.Errorf("%w: input_id prefix %q is reserved", ErrBadRequest, strings.SplitN(in.InputID, ":", 2)[0]+":")
	}
	if in.Attention == "" {
		in.Attention = "reply"
	}
	if in.Payload == nil {
		in.Payload = map[string]any{}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Input{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Share-lock the persona row: a transfer seal updates it, so a new input
	// either commits before the seal (and is inside the export cut) or sees
	// the new authority and is refused. Refusal is explicit — the ingress
	// still holds the input — never a silent drop. Replays of an accepted
	// input stay answerable in every authority.
	var authority string
	err = tx.QueryRow(ctx,
		`SELECT authority FROM core_personas WHERE persona_id = $1 FOR SHARE`,
		in.PersonaID).Scan(&authority)
	if errors.Is(err, pgx.ErrNoRows) {
		return Input{}, false, ErrPersonaNotFound
	}
	if err != nil {
		return Input{}, false, dataErr(err)
	}
	var stored Input
	err = pgx.ErrNoRows
	if authority == "active" {
		// A receipt for this input_id may already be in the journal — a
		// commit can journal input_received ahead of the input's creation.
		// The new row adopts the earliest such receipt as its marker so
		// materializing the id claims existing history instead of letting
		// a second receipt land unlinked.
		err = tx.QueryRow(ctx, `
		INSERT INTO core_inputs (persona_id, input_id, kind, payload, actor_kind, actor_id,
			source_surface, thread_id, occurred_at, attention, status, received_seq)
		SELECT $1::uuidv7, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'queued',
			(SELECT MIN(seq) FROM core_events
			 WHERE persona_id = $1::uuidv7 AND kind = 'input_received'
				AND payload->>'input_id' = $2)
		FROM core_personas WHERE persona_id = $1::uuidv7
		ON CONFLICT (persona_id, input_id) DO NOTHING
		RETURNING `+inputCols,
			in.PersonaID, in.InputID, in.Kind, in.Payload, in.ActorKind, in.ActorID,
			in.SourceSurface, in.ThreadID, in.OccurredAt, in.Attention).
			Scan(&stored.PersonaID, &stored.InputID, &stored.Kind, &stored.Payload,
				&stored.ActorKind, &stored.ActorID, &stored.SourceSurface, &stored.ThreadID,
				&stored.OccurredAt, &stored.Attention, &stored.Status, &stored.ClaimedGeneration,
				&stored.TurnID, &stored.CreatedAt, &stored.DoneAt, &stored.NotBefore)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		// Replay of an existing input_id is only valid when every caller-
		// supplied field matches what was stored — an idempotent retry, not
		// a different input claiming the same id.
		var same bool
		err = tx.QueryRow(ctx, `
			SELECT kind = $3 AND payload = $4::jsonb AND actor_kind = $5
				AND actor_id = $6 AND source_surface = $7 AND thread_id = $8
				AND occurred_at IS NOT DISTINCT FROM $9 AND attention = $10
			FROM core_inputs WHERE persona_id = $1 AND input_id = $2`,
			in.PersonaID, in.InputID, in.Kind, in.Payload, in.ActorKind, in.ActorID,
			in.SourceSurface, in.ThreadID, in.OccurredAt, in.Attention).Scan(&same)
		if errors.Is(err, pgx.ErrNoRows) && authority != "active" {
			return Input{}, false, fmt.Errorf("%w: authority is %s", ErrPersonaInactive, authority)
		}
		if err != nil {
			return Input{}, false, err
		}
		if !same {
			return Input{}, false, fmt.Errorf("%w: input_id replay carries a different request", ErrTurnConflict)
		}
		stored, err = scanInput(tx.QueryRow(ctx,
			`SELECT `+inputCols+` FROM core_inputs WHERE persona_id = $1 AND input_id = $2`,
			in.PersonaID, in.InputID))
		if err != nil {
			return Input{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return Input{}, false, err
		}
		return stored, false, nil
	}
	if err != nil {
		return Input{}, false, fmt.Errorf("submit input: %w", dataErr(err))
	}
	if err := tx.Commit(ctx); err != nil {
		return Input{}, false, err
	}
	return stored, true, nil
}

type queryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (s *Store) turn(ctx context.Context, db queryRower, personaID, turnID string) (*Turn, error) {
	var t Turn
	err := db.QueryRow(ctx, `
		SELECT persona_id, turn_id, input_id, generation, attempt, status, started_at, finished_at, output, usage, error
		FROM core_turns WHERE persona_id = $1 AND turn_id = $2`, personaID, turnID).
		Scan(&t.PersonaID, &t.TurnID, &t.InputID, &t.Generation, &t.Attempt, &t.Status,
			&t.StartedAt, &t.FinishedAt, &t.Output, &t.Usage, &t.Error)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrTurnNotFound
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (s *Store) GetInput(ctx context.Context, personaID, inputID string) (Input, *Turn, error) {
	in, err := scanInput(s.pool.QueryRow(ctx,
		`SELECT `+inputCols+` FROM core_inputs WHERE persona_id = $1 AND input_id = $2`,
		personaID, inputID))
	if err != nil {
		return in, nil, err
	}
	if in.TurnID == nil {
		return in, nil, nil
	}
	turn, err := s.turn(ctx, s.pool, personaID, *in.TurnID)
	if errors.Is(err, ErrTurnNotFound) {
		return in, nil, nil
	}
	return in, turn, err
}

// SavePlan durably records one round of the model's decisions for the input
// a running turn is resolving — the "decision before effects" boundary (F1),
// extended so tool results can be fed back for a truthful final reply: the
// model may be consulted again after a round's effects commit, and each new
// round is appended to the same durable record before its own effects run.
//
// The caller names the turn and the round index; the input is derived
// server-side from the turn row so it cannot be mis-asserted. The stored
// plan is append-only: re-saving an existing round is idempotent only when
// identical (a lost-response resend), a different decision at a recorded
// position or a gap in the round sequence is a contract violation (409).
// Rounds may be appended by a later attempt of the same input — the plan
// belongs to the input's resolution lineage, not to one attempt.
func (s *Store) SavePlan(ctx context.Context, personaID, turnID string, generation int64, round int64, decision Decision) (TurnPlan, bool, error) {
	if round < 0 {
		return TurnPlan{}, false, fmt.Errorf("%w: round must be >= 0", ErrBadRequest)
	}
	if decision.Calls == nil {
		decision.Calls = []PlanCall{}
	}
	if decision.Usage == nil {
		decision.Usage = map[string]any{}
	}
	for i := range decision.Calls {
		if decision.Calls[i].Tool == "" {
			return TurnPlan{}, false, fmt.Errorf("%w: plan call %d missing tool", ErrBadRequest, i)
		}
		if decision.Calls[i].Request == nil {
			decision.Calls[i].Request = map[string]any{}
		}
	}
	decJSON, err := json.Marshal(decision)
	if err != nil {
		return TurnPlan{}, false, err
	}
	// The plan is the first persistence boundary for model output: a NUL
	// anywhere in it cannot be stored as jsonb, and retrying the save can
	// never succeed. Reject it as a deterministic decision error here —
	// before any effect boundary is reached.
	var genericDecision any
	if err := json.Unmarshal(decJSON, &genericDecision); err != nil {
		return TurnPlan{}, false, err
	}
	if hasNUL(genericDecision) {
		return TurnPlan{}, false, fmt.Errorf("%w: decision contains a NUL byte jsonb cannot store", ErrBadRequest)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return TurnPlan{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := requireGeneration(ctx, tx, personaID, generation); err != nil {
		return TurnPlan{}, false, err
	}
	// The plan may only be authored by the live attempt: the named turn must
	// be running under this generation.
	var inputID string
	var turnGen int64
	var turnStatus string
	err = tx.QueryRow(ctx,
		`SELECT input_id, generation, status FROM core_turns WHERE persona_id = $1 AND turn_id = $2`,
		personaID, turnID).Scan(&inputID, &turnGen, &turnStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return TurnPlan{}, false, ErrTurnNotFound
	}
	if err != nil {
		return TurnPlan{}, false, err
	}
	if turnGen != generation || turnStatus != "running" {
		return TurnPlan{}, false, ErrTurnConflict
	}
	// Serialize appends against other writers on this row.
	var storedRounds int64
	var haveRow bool
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(jsonb_array_length(plan), 0) FROM core_turn_plans
		WHERE persona_id = $1 AND input_id = $2 FOR UPDATE`,
		personaID, inputID).Scan(&storedRounds)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		if round != 0 {
			return TurnPlan{}, false, fmt.Errorf("%w: first plan round must be round 0", ErrTurnConflict)
		}
	case err != nil:
		return TurnPlan{}, false, err
	default:
		haveRow = true
	}
	var p TurnPlan
	if !haveRow {
		err = tx.QueryRow(ctx, `
			INSERT INTO core_turn_plans (persona_id, input_id, turn_id, generation, plan)
			VALUES ($1, $2, $3, $4, jsonb_build_array($5::jsonb))
			RETURNING persona_id, input_id, turn_id, generation, plan, created_at`,
			personaID, inputID, turnID, generation, decJSON).
			Scan(&p.PersonaID, &p.InputID, &p.TurnID, &p.Generation, &p.Plan, &p.CreatedAt)
		if err != nil {
			return TurnPlan{}, false, fmt.Errorf("save plan: %w", dataErr(err))
		}
		if err := tx.Commit(ctx); err != nil {
			return TurnPlan{}, false, err
		}
		return p, true, nil
	}
	switch {
	case round < storedRounds:
		// Re-saving a recorded round is idempotent only when identical —
		// a different decision at a recorded position is a contract
		// violation, never a supersession.
		var same bool
		if err := tx.QueryRow(ctx, `
			SELECT plan->($3::int) = $4::jsonb FROM core_turn_plans
			WHERE persona_id = $1 AND input_id = $2`,
			personaID, inputID, round, decJSON).Scan(&same); err != nil {
			return TurnPlan{}, false, err
		}
		if !same {
			return TurnPlan{}, false, fmt.Errorf("%w: round %d already recorded with a different decision", ErrTurnConflict, round)
		}
	case round == storedRounds:
		err = tx.QueryRow(ctx, `
			UPDATE core_turn_plans SET plan = plan || $3::jsonb
			WHERE persona_id = $1 AND input_id = $2
			RETURNING persona_id, input_id, turn_id, generation, plan, created_at`,
			personaID, inputID, json.RawMessage("["+string(decJSON)+"]")).
			Scan(&p.PersonaID, &p.InputID, &p.TurnID, &p.Generation, &p.Plan, &p.CreatedAt)
		if err != nil {
			return TurnPlan{}, false, fmt.Errorf("append plan round: %w", dataErr(err))
		}
		if err := tx.Commit(ctx); err != nil {
			return TurnPlan{}, false, err
		}
		return p, true, nil
	default:
		return TurnPlan{}, false, fmt.Errorf("%w: plan round %d skips recorded rounds (have %d)", ErrTurnConflict, round, storedRounds)
	}
	stored, err := s.planForInput(ctx, tx, personaID, inputID)
	if err != nil {
		return TurnPlan{}, false, fmt.Errorf("save plan: %w", dataErr(err))
	}
	if stored == nil {
		return TurnPlan{}, false, fmt.Errorf("plan vanished mid-transaction")
	}
	if err := tx.Commit(ctx); err != nil {
		return TurnPlan{}, false, err
	}
	return *stored, false, nil
}

// planForInput returns the recorded decision for an input, or nil.
func (s *Store) planForInput(ctx context.Context, db queryRower, personaID, inputID string) (*TurnPlan, error) {
	var p TurnPlan
	err := db.QueryRow(ctx, `
		SELECT persona_id, input_id, turn_id, generation, plan, created_at
		FROM core_turn_plans WHERE persona_id = $1 AND input_id = $2`,
		personaID, inputID).
		Scan(&p.PersonaID, &p.InputID, &p.TurnID, &p.Generation, &p.Plan, &p.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// clampLimit bounds caller-supplied list sizes: non-positive picks the
// default, oversized values are capped server-side.
func clampLimit(v, def, max int) int {
	if v <= 0 {
		return def
	}
	if v > max {
		return max
	}
	return v
}

// LoadTurn is the coarse turn-start read under the writer's generation. If a
// turn is already running under this generation (a lost load response), it is
// replayed. Otherwise the oldest queued input is claimed and its turn begun
// in the same transaction. The journal tail is returned with either result so
// the core needs exactly one round trip to assemble a turn.
func (s *Store) LoadTurn(ctx context.Context, personaID string, generation int64, turnID string, contextLimit int) (LoadResult, error) {
	var res LoadResult
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return res, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := requireGeneration(ctx, tx, personaID, generation); err != nil {
		return res, err
	}
	var t *Turn
	err = tx.QueryRow(ctx,
		`SELECT turn_id FROM core_turns WHERE persona_id = $1 AND status = 'running'`,
		personaID).Scan(&turnID)
	switch {
	case err == nil:
		t, err = s.turn(ctx, tx, personaID, turnID)
		if err != nil {
			return res, err
		}
		if t.Generation != generation {
			return res, ErrTurnConflict
		}
	case errors.Is(err, pgx.ErrNoRows):
		var in Input
		err = tx.QueryRow(ctx, `
			UPDATE core_inputs SET status = 'claimed', claimed_generation = $2, not_before = NULL
			WHERE (persona_id, input_id) = (
				SELECT persona_id, input_id FROM core_inputs
				WHERE persona_id = $1 AND status = 'queued'
					AND (not_before IS NULL OR not_before <= now())
				ORDER BY created_at, input_id LIMIT 1 FOR UPDATE SKIP LOCKED
			)
			RETURNING `+inputCols,
			personaID, generation).
			Scan(&in.PersonaID, &in.InputID, &in.Kind, &in.Payload,
				&in.ActorKind, &in.ActorID, &in.SourceSurface, &in.ThreadID,
				&in.OccurredAt, &in.Attention, &in.Status, &in.ClaimedGeneration,
				&in.TurnID, &in.CreatedAt, &in.DoneAt, &in.NotBefore)
		if errors.Is(err, pgx.ErrNoRows) {
			rc, err := s.renderedContext(ctx, tx, personaID, contextLimit, "")
			if err != nil {
				return res, err
			}
			res.Context, res.Memory, res.Omitted, res.MemoryOmitted = rc.Events, rc.Memory, rc.Omitted, rc.MemoryOmitted
			if err := tx.Commit(ctx); err != nil {
				return res, err
			}
			return res, nil
		}
		if err != nil {
			return res, fmt.Errorf("claim input: %w", err)
		}
		var attempt int
		if err := tx.QueryRow(ctx,
			`SELECT COUNT(*) + 1 FROM core_turns WHERE persona_id = $1 AND input_id = $2`,
			personaID, in.InputID).Scan(&attempt); err != nil {
			return res, err
		}
		if turnID == "" {
			turnID = uuid.NewString()
		}
		var nt Turn
		err = tx.QueryRow(ctx, `
			INSERT INTO core_turns (persona_id, turn_id, input_id, generation, attempt, status)
			VALUES ($1, $2, $3, $4, $5, 'running')
			RETURNING persona_id, turn_id, input_id, generation, attempt, status, started_at, finished_at, output, usage, error`,
			personaID, turnID, in.InputID, generation, attempt).
			Scan(&nt.PersonaID, &nt.TurnID, &nt.InputID, &nt.Generation, &nt.Attempt, &nt.Status,
				&nt.StartedAt, &nt.FinishedAt, &nt.Output, &nt.Usage, &nt.Error)
		if err != nil {
			return res, fmt.Errorf("begin turn: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE core_inputs SET turn_id = $3 WHERE persona_id = $1 AND input_id = $2`,
			personaID, in.InputID, turnID); err != nil {
			return res, err
		}
		t = &nt
		res.Input = &in
	default:
		return res, err
	}
	if res.Input == nil && t != nil {
		in, err := scanInput(tx.QueryRow(ctx,
			`SELECT `+inputCols+` FROM core_inputs WHERE persona_id = $1 AND input_id = $2`,
			personaID, t.InputID))
		if err != nil {
			return res, err
		}
		res.Input = &in
	}
	res.Turn = t
	if res.Input != nil {
		// A recorded plan belongs to the input's resolution lineage, so it
		// surfaces on whichever attempt is running now — fresh claim or
		// replayed running turn alike.
		res.Plan, err = s.planForInput(ctx, tx, personaID, res.Input.InputID)
		if err != nil {
			return res, err
		}
	}
	// The turn presents its own input (and its recorded plan re-presents any
	// mid-turn effects), so records an earlier attempt already journaled for
	// this input are left out of the rendered history.
	exclude := ""
	if res.Input != nil {
		exclude = res.Input.InputID
	}
	rc, err := s.renderedContext(ctx, tx, personaID, contextLimit, exclude)
	if err != nil {
		return res, err
	}
	res.Context, res.Memory, res.Omitted, res.MemoryOmitted = rc.Events, rc.Memory, rc.Omitted, rc.MemoryOmitted
	if err := tx.Commit(ctx); err != nil {
		return res, err
	}
	return res, nil
}

// CommitTurn is the coarse end-of-turn write: journal events, turn outcome,
// input disposition, and the outbox record land in one transaction.
// Re-committing an already-finished turn replays the stored result, so a
// lost commit response is safe.
func (s *Store) CommitTurn(ctx context.Context, personaID, turnID string, generation int64, req CommitRequest) (*Turn, error) {
	if req.Outcome != "complete" && req.Outcome != "fail" {
		return nil, fmt.Errorf("%w: outcome must be complete or fail", ErrBadRequest)
	}
	// Error text is diagnostic, not authoritative content: strip bytes PG
	// text/jsonb cannot hold so a poisoned provider message cannot make the
	// failure itself unpersistable and loop attempts forever. Normalized
	// before the commit_request marshals so replays compare identically.
	req.Error = strings.ReplaceAll(req.Error, "\x00", "")
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := requireGeneration(ctx, tx, personaID, generation); err != nil {
		return nil, err
	}
	t, err := s.turn(ctx, tx, personaID, turnID)
	if err != nil {
		return nil, err
	}
	if t.Status != "running" {
		if t.Generation != generation {
			return nil, ErrTurnConflict
		}
		// Already finished by an earlier commit whose response was lost.
		// Replaying is only valid when the request is byte-identical to
		// what was committed — a different outcome, output, error, or event
		// list under the same turn_id is a contract violation, not
		// idempotency. commit_request was stored with the commit, so the
		// comparison is exact rather than inferred from side effects.
		reqJSON, err := json.Marshal(req)
		if err != nil {
			return nil, err
		}
		var same bool
		if err := tx.QueryRow(ctx, `
			SELECT commit_request IS NOT DISTINCT FROM $3::jsonb
			FROM core_turns WHERE persona_id = $1 AND turn_id = $2`,
			personaID, turnID, reqJSON).Scan(&same); err != nil {
			return nil, err
		}
		if !same {
			return nil, fmt.Errorf("%w: turn replay carries a different commit", ErrTurnConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return t, nil
	}
	if t.Generation != generation {
		return nil, ErrTurnConflict
	}
	if req.Outcome != "complete" && req.Outcome != "fail" {
		return nil, fmt.Errorf("%w: outcome must be complete or fail", ErrBadRequest)
	}
	reqJSON, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	// Exactly one input_received per input identity ever lands in the
	// journal: a copy whose identity is already journaled — the input's
	// marker is set, or a receipt carrying that payload->>'input_id' exists
	// (see withoutJournaledInput) — is dropped, and so is a second copy
	// inside the request itself. A duplicate receipt is the same fact
	// twice, not new history. commit_request keeps the request as sent, so
	// replays still compare.
	events, err := withoutJournaledInput(ctx, tx, personaID, req.Events)
	if err != nil {
		return nil, err
	}
	var base int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(seq), 0) FROM core_events WHERE persona_id = $1`,
		personaID).Scan(&base); err != nil {
		return nil, err
	}
	if err := s.appendEventsTx(ctx, tx, personaID, turnID, events); err != nil {
		return nil, dataErr(err)
	}
	// Link every input_received this commit journaled to its input's
	// marker — not only this turn's input: a receipt for another input is
	// unusual but journaled history, and leaving it unlinked would make the
	// persona permanently unsealable under the cut's reverse-link check.
	if _, err := tx.Exec(ctx, `
		UPDATE core_inputs i SET received_seq = s.seq
		FROM (
			SELECT payload->>'input_id' AS input_id, MIN(seq) AS seq
			FROM core_events
			WHERE persona_id = $1 AND seq > $2 AND kind = 'input_received'
			GROUP BY 1
		) s
		WHERE i.persona_id = $1 AND i.input_id = s.input_id
			AND i.received_seq IS NULL`,
		personaID, base); err != nil {
		return nil, err
	}
	switch req.Outcome {
	case "complete":
		if err := tx.QueryRow(ctx, `
			UPDATE core_turns SET status = 'done', finished_at = now(), output = $3, usage = $4, commit_request = $5
			WHERE persona_id = $1 AND turn_id = $2
			RETURNING status, finished_at, output, usage`,
			personaID, turnID, req.Output, req.Usage, reqJSON).
			Scan(&t.Status, &t.FinishedAt, &t.Output, &t.Usage); err != nil {
			return nil, fmt.Errorf("complete turn: %w", dataErr(err))
		}
		if _, err := tx.Exec(ctx,
			`UPDATE core_inputs SET status = 'done', done_at = now() WHERE persona_id = $1 AND input_id = $2`,
			personaID, t.InputID); err != nil {
			return nil, dataErr(err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO core_outbox (persona_id, seq, kind, payload)
			SELECT $1::uuidv7, COALESCE(MAX(seq), 0) + 1, 'turn_completed', $2
			FROM core_outbox WHERE persona_id = $1::uuidv7`,
			personaID, map[string]any{
				"turn_id":  turnID,
				"input_id": t.InputID,
				"output":   req.Output,
			}); err != nil {
			return nil, fmt.Errorf("append outbox: %w", dataErr(err))
		}
	case "fail":
		if err := tx.QueryRow(ctx, `
			UPDATE core_turns SET status = 'failed', finished_at = now(), error = $3, commit_request = $4
			WHERE persona_id = $1 AND turn_id = $2 RETURNING status, finished_at, error`,
			personaID, turnID, req.Error, reqJSON).
			Scan(&t.Status, &t.FinishedAt, &t.Error); err != nil {
			return nil, dataErr(err)
		}
		if req.Retryable {
			// The requeue carries a per-attempt backoff: without it a
			// deterministically failing input reclaims instantly every
			// pass (its original created_at wins the ordering), grows
			// the journal and turns table without bound, and starves
			// every later queued input. not_before keeps the retry
			// honest and lets other work proceed. Provider-supplied
			// pacing (Retry-After) is honored on top, clamped to 2min —
			// a hint can slow a retry, never silence it.
			delay := retryBackoff(t.Attempt)
			if after := time.Duration(req.RetryAfterMs) * time.Millisecond; after > delay {
				if after > 2*time.Minute {
					after = 2 * time.Minute
				}
				delay = after
			}
			if _, err := tx.Exec(ctx, `
				UPDATE core_inputs SET status = 'queued', claimed_generation = NULL,
					turn_id = NULL, not_before = now() + $3 * interval '1 millisecond'
				WHERE persona_id = $1 AND input_id = $2`,
				personaID, t.InputID, delay.Milliseconds()); err != nil {
				return nil, err
			}
		} else {
			if _, err := tx.Exec(ctx, `
				UPDATE core_inputs SET status = 'done', done_at = now()
				WHERE persona_id = $1 AND input_id = $2`, personaID, t.InputID); err != nil {
				return nil, err
			}
			// A terminal failure resolves the input — the requester must see
			// the request ended, not wait silently. The failure is the reply.
			if _, err := tx.Exec(ctx, `
				INSERT INTO core_outbox (persona_id, seq, kind, payload)
				SELECT $1::uuidv7, COALESCE(MAX(seq), 0) + 1, 'turn_failed', $2
				FROM core_outbox WHERE persona_id = $1::uuidv7`,
				personaID, map[string]any{
					"turn_id":  turnID,
					"input_id": t.InputID,
					"error":    req.Error,
				}); err != nil {
				return nil, fmt.Errorf("append outbox: %w", err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return t, nil
}

// retryBackoff bounds how soon a retryable-failed input may be claimed
// again: 200ms doubling per attempt, capped at 30s. The attempt number
// of the turn that just failed drives it, so a permanently failing
// input decays to a slow poll instead of a hot loop — while later queued
// inputs remain claimable during the delay.
func retryBackoff(attempt int) time.Duration {
	shift := attempt - 1
	if shift < 0 {
		shift = 0
	}
	if shift > 8 {
		shift = 8
	}
	d := 200 * time.Millisecond << shift
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

// jsonbEqual compares two JSON payloads semantically: both sides pass
// through a canonical encode/decode so key order and numeric type
// (Go int vs JSON float64) do not matter. nil and {} are intentionally
// different — a caller that committed no output is not the same request
// as one that committed {}.
func jsonbEqual(a, b map[string]any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	an, bn := map[string]any{}, map[string]any{}
	if err := roundTrip(a, &an); err != nil {
		return false
	}
	if err := roundTrip(b, &bn); err != nil {
		return false
	}
	return reflect.DeepEqual(an, bn)
}

func roundTrip(v map[string]any, out *map[string]any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

func (s *Store) appendEventsTx(ctx context.Context, tx pgx.Tx, personaID, turnID string, events []EventInput) error {
	if len(events) == 0 {
		return nil
	}
	kinds := make([]string, len(events))
	payloads := make([]map[string]any, len(events))
	for i, e := range events {
		if e.Kind == "" {
			return fmt.Errorf("%w: event kind required", ErrBadRequest)
		}
		kinds[i] = e.Kind
		payloads[i] = e.Payload
	}
	// WITH ORDINALITY makes the request's event order the journal order —
	// unspecified input ordering would silently reorder the life log.
	_, err := tx.Exec(ctx, `
		INSERT INTO core_events (persona_id, seq, turn_id, kind, payload)
		SELECT $1, base.base + e.ord, $2, e.kind, e.payload
		FROM unnest($3::text[], $4::jsonb[]) WITH ORDINALITY AS e(kind, payload, ord)
		CROSS JOIN (SELECT COALESCE(MAX(seq), 0) AS base FROM core_events WHERE persona_id = $1) base`,
		personaID, turnID, kinds, payloads)
	if err != nil {
		return fmt.Errorf("append events: %w", err)
	}
	return nil
}

// Recover runs under the caller's fresh generation: turns still running under
// older generations are interrupted and their inputs requeued, claimed-but-
// unbegun inputs are requeued, and stale schedule claims are released.
func (s *Store) Recover(ctx context.Context, personaID string, generation int64) (RecoverResult, error) {
	res := RecoverResult{
		InterruptedTurns: []string{},
		RequeuedInputs:   []string{},
		ReleasedClaims:   []string{},
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return res, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := requireGeneration(ctx, tx, personaID, generation); err != nil {
		return res, err
	}
	rows, err := tx.Query(ctx, `
		UPDATE core_turns SET status = 'interrupted', finished_at = now()
		WHERE persona_id = $1 AND status = 'running' AND generation <> $2
		RETURNING turn_id, input_id`, personaID, generation)
	if err != nil {
		return res, err
	}
	var interruptedInputs []string
	for rows.Next() {
		var tid, iid string
		if err := rows.Scan(&tid, &iid); err != nil {
			rows.Close()
			return res, err
		}
		res.InterruptedTurns = append(res.InterruptedTurns, tid)
		interruptedInputs = append(interruptedInputs, iid)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return res, err
	}
	inRows, err := tx.Query(ctx, `
		UPDATE core_inputs SET status = 'queued', claimed_generation = NULL, turn_id = NULL
		WHERE persona_id = $1 AND status = 'claimed' AND (
			(claimed_generation IS NOT NULL AND claimed_generation <> $2)
			OR input_id = ANY($3::text[])
		)
		RETURNING input_id`, personaID, generation, interruptedInputs)
	if err != nil {
		return res, err
	}
	for inRows.Next() {
		var id string
		if err := inRows.Scan(&id); err != nil {
			inRows.Close()
			return res, err
		}
		res.RequeuedInputs = append(res.RequeuedInputs, id)
	}
	inRows.Close()
	if err := inRows.Err(); err != nil {
		return res, err
	}
	sRows, err := tx.Query(ctx, `
		UPDATE core_schedules SET status = 'pending', claimed_generation = NULL
		WHERE persona_id = $1 AND status = 'claimed' AND (claimed_generation IS NULL OR claimed_generation <> $2)
		RETURNING schedule_id`, personaID, generation)
	if err != nil {
		return res, err
	}
	for sRows.Next() {
		var id string
		if err := sRows.Scan(&id); err != nil {
			sRows.Close()
			return res, err
		}
		res.ReleasedClaims = append(res.ReleasedClaims, id)
	}
	sRows.Close()
	if err := sRows.Err(); err != nil {
		return res, err
	}
	// Return memory chunks a fenced generation was preparing to the shelf
	// so the live generation can reprepare them — the originals never left
	// the context while preparation ran. The lost claim counts as an
	// interruption, not a failed attempt.
	if err := interruptPreparing(ctx, tx, personaID, &generation); err != nil {
		return res, err
	}
	if err := tx.Commit(ctx); err != nil {
		return res, err
	}
	return res, nil
}

func (s *Store) Events(ctx context.Context, personaID string, afterSeq int64, limit int) ([]Event, error) {
	limit = clampLimit(limit, 200, 1000)
	rows, err := s.pool.Query(ctx, `
		SELECT persona_id, seq, turn_id, kind, payload, created_at
		FROM core_events WHERE persona_id = $1 AND seq > $2
		ORDER BY seq LIMIT $3`, personaID, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Event{}
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.PersonaID, &e.Seq, &e.TurnID, &e.Kind, &e.Payload, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// isInternalTool lists the tools whose effects this slice can apply
// atomically inside the claim transaction. Anything else has no execution
// path yet and must not be claimable.
func isInternalTool(tool string) bool {
	switch tool {
	case "schedule.set", "journal.note", "conversation_history",
		"job.start", "job.status", "job.cancel":
		return true
	}
	return false
}

// ensureInputReceived journals the input a turn is serving before a
// mid-turn effect lands in the journal, so the effect follows its cause in
// seq order (and in the memory chunk the seal walk cuts at that input). The
// payload is the one the core commits for the same input. received_seq makes
// it happen once per input: later effects, retried attempts and the turn's
// own commit all see the input as already journaled.
func ensureInputReceived(ctx context.Context, tx pgx.Tx, personaID, inputID, turnID string) error {
	var received *int64
	if err := tx.QueryRow(ctx,
		`SELECT received_seq FROM core_inputs WHERE persona_id = $1 AND input_id = $2 FOR UPDATE`,
		personaID, inputID).Scan(&received); err != nil {
		return fmt.Errorf("input for journal: %w", err)
	}
	if received != nil {
		return nil
	}
	// The journal may already carry a receipt naming this input (journaled
	// before the row's marker was visible to this write). Adopt it rather
	// than journaling a second, unlinked copy.
	var journaled *int64
	if err := tx.QueryRow(ctx,
		`SELECT MIN(seq) FROM core_events
		 WHERE persona_id = $1 AND kind = 'input_received' AND payload->>'input_id' = $2`,
		personaID, inputID).Scan(&journaled); err != nil {
		return fmt.Errorf("input for journal: %w", err)
	}
	if journaled != nil {
		_, err := tx.Exec(ctx,
			`UPDATE core_inputs SET received_seq = $3 WHERE persona_id = $1 AND input_id = $2`,
			personaID, inputID, *journaled)
		return err
	}
	in, err := scanInput(tx.QueryRow(ctx,
		`SELECT `+inputCols+` FROM core_inputs WHERE persona_id = $1 AND input_id = $2`,
		personaID, inputID))
	if err != nil {
		return err
	}
	var attempt int
	if err := tx.QueryRow(ctx,
		`SELECT attempt FROM core_turns WHERE persona_id = $1 AND turn_id = $2`,
		personaID, turnID).Scan(&attempt); err != nil {
		return err
	}
	var text any
	if t, ok := in.Payload["text"].(string); ok {
		text = t
	}
	var seq int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO core_events (persona_id, seq, turn_id, kind, payload)
		SELECT $1::uuidv7, COALESCE(MAX(seq), 0) + 1, $2, 'input_received', $3
		FROM core_events WHERE persona_id = $1::uuidv7
		RETURNING seq`,
		personaID, turnID, map[string]any{
			"input_id":       in.InputID,
			"kind":           in.Kind,
			"text":           text,
			"actor_kind":     in.ActorKind,
			"source_surface": in.SourceSurface,
			"attempt":        attempt,
		}).Scan(&seq); err != nil {
		return fmt.Errorf("journal input: %w", dataErr(err))
	}
	_, err = tx.Exec(ctx,
		`UPDATE core_inputs SET received_seq = $3 WHERE persona_id = $1 AND input_id = $2`,
		personaID, inputID, seq)
	return err
}

// withoutJournaledInput enforces one input_received per input identity in
// the journal. A receipt's identity is the journal's own key —
// payload->>'input_id' — resolved in PostgreSQL so dedup, the marker
// back-fill and the cut's link checks share one namespace: numeric 5 and
// string "5" name one input, not two. A copy whose identity is already
// journaled (the input's marker is set, or a receipt carrying that key
// exists — including a ghost receipt for an input not yet created) is
// dropped, and so is a second copy inside the request itself: a duplicate
// receipt is the same fact twice, not new history. A receipt with a
// missing or null input_id names no input — it is journaled ghost content
// that can never join a row, so it is neither deduplicated nor linked.
// The marker check runs FOR UPDATE so a commit cannot dedup against a
// marker another in-flight write has not recorded yet.
func withoutJournaledInput(ctx context.Context, tx pgx.Tx, personaID string, events []EventInput) ([]EventInput, error) {
	var payloads []string
	var idx []int
	for i, e := range events {
		if e.Kind != "input_received" {
			continue
		}
		raw, err := json.Marshal(e.Payload)
		if err != nil {
			return nil, err
		}
		payloads = append(payloads, string(raw))
		idx = append(idx, i)
	}
	if len(payloads) == 0 {
		return events, nil
	}
	keys := make(map[int]string, len(idx))
	var named []string
	seen := map[string]bool{}
	rows, err := tx.Query(ctx, `
		SELECT (p::jsonb)->>'input_id'
		FROM unnest($1::text[]) WITH ORDINALITY AS t(p, i)
		ORDER BY t.i`, payloads)
	if err != nil {
		return nil, err
	}
	k := 0
	for rows.Next() {
		var id *string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		if id != nil {
			keys[idx[k]] = *id
			if !seen[*id] {
				seen[*id] = true
				named = append(named, *id)
			}
		}
		k++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	journaled := map[string]bool{}
	if len(named) > 0 {
		rows, err = tx.Query(ctx,
			`SELECT input_id FROM core_inputs
			 WHERE persona_id = $1 AND input_id = ANY($2) AND received_seq IS NOT NULL
			 FOR UPDATE`,
			personaID, named)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			journaled[id] = true
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		rows.Close()
		// A receipt already in the journal names its input forever —
		// including a ghost whose input row does not exist yet. Dropping
		// the second copy is what keeps a later SubmitInput adoption from
		// joining an unlinked receipt.
		rows, err = tx.Query(ctx, `
			SELECT DISTINCT payload->>'input_id' FROM core_events
			WHERE persona_id = $1 AND kind = 'input_received'
				AND payload->>'input_id' = ANY($2)`,
			personaID, named)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			journaled[id] = true
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		rows.Close()
	}
	out := make([]EventInput, 0, len(events))
	emitted := map[string]bool{}
	for i, e := range events {
		if id, ok := keys[i]; ok {
			if journaled[id] || emitted[id] {
				continue
			}
			emitted[id] = true
		}
		out = append(out, e)
	}
	return out, nil
}

// internalToolResponse applies a state-internal tool's effect inside the
// claim transaction: the operation record and its effect are atomic, so a
// crash cannot leave an unrecorded effect or a dangling record. inputID and
// callIndex are the claim's plan position — job.start derives its job_id
// from them so a replayed claim can never mint a second job.
func (s *Store) internalToolResponse(ctx context.Context, tx pgx.Tx, personaID, turnID, inputID, tool string, callIndex int, request map[string]any) (map[string]any, bool, error) {
	if strings.HasPrefix(tool, "job.") {
		resp, err := s.internalJobTool(ctx, tx, personaID, turnID, inputID, tool, callIndex, request)
		return resp, resp != nil, err
	}
	switch tool {
	case "schedule.set":
		scheduleID, _ := request["schedule_id"].(string)
		if scheduleID == "" {
			scheduleID = fmt.Sprintf("sch-%d", time.Now().UnixNano())
		}
		wakeAtStr, _ := request["wake_at"].(string)
		wakeAt, err := time.Parse(time.RFC3339Nano, wakeAtStr)
		if err != nil {
			return nil, false, fmt.Errorf("%w: schedule.set requires RFC3339 wake_at", ErrBadRequest)
		}
		payload, _ := request["payload"].(map[string]any)
		if payload == nil {
			payload = map[string]any{}
		}
		missPolicy, _ := request["miss_policy"].(string)
		if missPolicy == "" {
			missPolicy = "fire_late"
		}
		switch missPolicy {
		case "fire_late", "coalesce", "expire", "report_missed":
		default:
			return nil, false, fmt.Errorf("%w: schedule.set miss_policy must be fire_late, coalesce, expire, or report_missed", ErrBadRequest)
		}
		var sch Schedule
		err = tx.QueryRow(ctx, `
			INSERT INTO core_schedules (persona_id, schedule_id, wake_at, payload, miss_policy, status)
			VALUES ($1, $2, $3, $4, $5, 'pending')
			ON CONFLICT (persona_id, schedule_id) DO NOTHING
			RETURNING persona_id, schedule_id, wake_at, payload, miss_policy, status, created_at`,
			personaID, scheduleID, wakeAt, payload, missPolicy).
			Scan(&sch.PersonaID, &sch.ScheduleID, &sch.WakeAt, &sch.Payload, &sch.MissPolicy, &sch.Status, &sch.CreatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			// The id is taken. Crash replay never reaches here — the
			// operation receipt is keyed by plan position — so a conflict
			// is a fresh claim reusing the id. An identical request over a
			// still-pending row is a true idempotent set; anything else
			// (different contents, or a fired/cancelled/expired row) must
			// not report a wake that was not created.
			err = tx.QueryRow(ctx, `
				SELECT persona_id, schedule_id, wake_at, payload, miss_policy, status, created_at
				FROM core_schedules WHERE persona_id = $1 AND schedule_id = $2`,
				personaID, scheduleID).
				Scan(&sch.PersonaID, &sch.ScheduleID, &sch.WakeAt, &sch.Payload, &sch.MissPolicy, &sch.Status, &sch.CreatedAt)
			if err != nil {
				return nil, false, fmt.Errorf("schedule.set: %w", dataErr(err))
			}
			identical := sch.WakeAt.Equal(wakeAt) &&
				sch.MissPolicy == missPolicy &&
				jsonbEqual(sch.Payload, payload)
			if !identical {
				return nil, false, fmt.Errorf("%w: schedule.set schedule_id %q already exists with different contents", ErrBadRequest, scheduleID)
			}
			if sch.Status != "pending" {
				return nil, false, fmt.Errorf("%w: schedule.set schedule_id %q is already %s", ErrBadRequest, scheduleID, sch.Status)
			}
			return map[string]any{"schedule": sch}, true, nil
		}
		if err != nil {
			return nil, false, fmt.Errorf("schedule.set: %w", dataErr(err))
		}
		return map[string]any{"schedule": sch}, true, nil
	case "journal.note":
		text, _ := request["text"].(string)
		if text == "" {
			return nil, false, fmt.Errorf("%w: journal.note requires text", ErrBadRequest)
		}
		if err := ensureInputReceived(ctx, tx, personaID, inputID, turnID); err != nil {
			return nil, false, err
		}
		var seq int64
		err := tx.QueryRow(ctx, `
			INSERT INTO core_events (persona_id, seq, turn_id, kind, payload)
			SELECT $1::uuidv7, COALESCE(MAX(seq), 0) + 1, $2, 'note', $3
			FROM core_events WHERE persona_id = $1::uuidv7
			RETURNING seq`,
			personaID, turnID, map[string]any{"text": text}).Scan(&seq)
		if err != nil {
			return nil, false, fmt.Errorf("journal.note: %w", dataErr(err))
		}
		return map[string]any{"seq": seq, "kind": "note"}, true, nil
	case "conversation_history":
		resp, err := s.conversationHistory(ctx, tx, personaID, request)
		if err != nil {
			return nil, false, err
		}
		return resp, true, nil
	default:
		return nil, false, nil
	}
}

// ClaimOperation records an authorized side effect before it executes.
// Idempotent by (persona, tool, idempotency_key): a replayed claim returns the
// stored operation including its response, so effects are not re-run.
//
// State-internal tools (schedule.set, journal.note) apply their effect in the
// claim transaction and return done immediately. External tools are claimed
// 'running' and must be completed via CompleteOperation; a crash between the
// external effect and its completion leaves a 'running' record that the next
// generation reclaims — reconciliation by querying the external system is the
// caller's duty, the ledger alone cannot prove an ambiguous external effect.
func (s *Store) ClaimOperation(ctx context.Context, personaID, turnID string, generation int64, operationID, tool string, callIndex int, request map[string]any) (Operation, bool, error) {
	if !isInternalTool(tool) {
		// This slice has no external executor; claiming an unregistered tool
		// would record a permanently dangling 'running' operation. Reject at
		// the boundary — the authorized external-tool contract (M08) adds its
		// own claim path.
		return Operation{}, false, fmt.Errorf("%w: %s", ErrUnknownTool, tool)
	}
	if request == nil {
		request = map[string]any{}
	}
	// The request is about to be bound against the recorded plan as jsonb:
	// a NUL in it is a deterministic data error, never a transient one.
	if hasNUL(request) {
		return Operation{}, false, fmt.Errorf("%w: %s request contains a NUL byte jsonb cannot store", ErrBadRequest, tool)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Operation{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := requireGeneration(ctx, tx, personaID, generation); err != nil {
		return Operation{}, false, err
	}
	// Operations are attributed to a turn; that turn must be the live one —
	// running under this generation — so an effect cannot be recorded
	// against a nonexistent or other-epoch turn.
	var inputID string
	var turnGen int64
	var turnStatus string
	err = tx.QueryRow(ctx,
		`SELECT input_id, generation, status FROM core_turns WHERE persona_id = $1 AND turn_id = $2`,
		personaID, turnID).Scan(&inputID, &turnGen, &turnStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return Operation{}, false, ErrTurnNotFound
	}
	if err != nil {
		return Operation{}, false, err
	}
	if turnGen != generation || turnStatus != "running" {
		return Operation{}, false, ErrTurnConflict
	}
	// The claim must be an entry of the input's recorded plan: the decision
	// is durable before effects, so an off-plan call — absent plan, index
	// out of range, or a different tool/request at that position — is a
	// contract violation, never a fresh effect. call_index addresses a flat
	// position across every recorded round's calls in order.
	if callIndex < 0 {
		return Operation{}, false, fmt.Errorf("%w: call_index must be >= 0", ErrBadRequest)
	}
	plan, err := s.planForInput(ctx, tx, personaID, inputID)
	if err != nil {
		return Operation{}, false, dataErr(err)
	}
	var flat []PlanCall
	if plan != nil {
		for _, round := range plan.Plan {
			flat = append(flat, round.Calls...)
		}
	}
	planned := callIndex < len(flat) && flat[callIndex].Tool == tool &&
		jsonbEqual(flat[callIndex].Request, request)
	if !planned {
		return Operation{}, false, fmt.Errorf("%w: claim is not call %d of the recorded plan", ErrTurnConflict, callIndex)
	}
	// Effect identity is server-owned: derived from the turn's input and the
	// plan position. A caller-chosen key could otherwise mint a second
	// effect for the same planned call.
	idemKey := inputID + ":tool:" + strconv.Itoa(callIndex)
	// Claim first: the idempotency insert decides whether this call owns the
	// effect. The internal effect is applied only on a fresh claim, then the
	// operation is finalized in the same transaction — record and effect are
	// atomic.
	var op Operation
	err = tx.QueryRow(ctx, `
		INSERT INTO core_operations (persona_id, operation_id, turn_id, tool, idempotency_key, request, status, claimed_generation)
		VALUES ($1, $2, $3, $4, $5, $6, 'running', $7)
		ON CONFLICT (persona_id, tool, idempotency_key) DO NOTHING
		RETURNING persona_id, operation_id, turn_id, tool, idempotency_key, request, status, response, claimed_generation, created_at, completed_at`,
		personaID, operationID, turnID, tool, idemKey, request, generation).
		Scan(&op.PersonaID, &op.OperationID, &op.TurnID, &op.Tool, &op.IdempotencyKey, &op.Request,
			&op.Status, &op.Response, &op.ClaimedGeneration, &op.CreatedAt, &op.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		op, err = s.operationByKey(ctx, tx, personaID, tool, idemKey)
		if err != nil {
			return Operation{}, false, err
		}
		// A replayed key is idempotent only for the identical request —
		// returning the stored receipt for a different request would record
		// an effect that never ran.
		if !jsonbEqual(op.Request, request) {
			return Operation{}, false, fmt.Errorf("%w: idempotency_key replay carries a different request", ErrTurnConflict)
		}
		if op.Status == "running" && op.ClaimedGeneration != generation {
			// The claiming generation is fenced; reclaim for re-execution.
			if _, err := tx.Exec(ctx, `
				UPDATE core_operations SET claimed_generation = $4, turn_id = $5
				WHERE persona_id = $1 AND tool = $2 AND idempotency_key = $3 AND status = 'running'`,
				personaID, tool, idemKey, generation, turnID); err != nil {
				return Operation{}, false, err
			}
			op.ClaimedGeneration = generation
			op.TurnID = turnID
			if err := tx.Commit(ctx); err != nil {
				return Operation{}, false, err
			}
			return op, true, nil
		}
		if strings.HasPrefix(tool, "job.") && op.Status == "done" {
			if op.Response, err = withCurrentJobTx(ctx, tx, personaID, op.Response); err != nil {
				return Operation{}, false, err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return Operation{}, false, err
		}
		return op, false, nil
	}
	if err != nil {
		return Operation{}, false, fmt.Errorf("claim operation: %w", dataErr(err))
	}
	// Fresh claim: apply the state-internal effect and finish the record in
	// the same transaction.
	response, internal, err := s.internalToolResponse(ctx, tx, personaID, turnID, inputID, tool, callIndex, request)
	if err != nil {
		return Operation{}, false, err
	}
	if internal {
		if err := tx.QueryRow(ctx, `
			UPDATE core_operations SET status = 'done', response = $4, completed_at = now()
			WHERE persona_id = $1 AND operation_id = $2 AND claimed_generation = $3
			RETURNING status, response, completed_at`,
			personaID, operationID, generation, response).
			Scan(&op.Status, &op.Response, &op.CompletedAt); err != nil {
			return Operation{}, false, fmt.Errorf("finish internal operation: %w", dataErr(err))
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Operation{}, false, err
	}
	return op, true, nil
}

func (s *Store) operationByKey(ctx context.Context, db queryRower, personaID, tool, idemKey string) (Operation, error) {
	var op Operation
	err := db.QueryRow(ctx, `
		SELECT persona_id, operation_id, turn_id, tool, idempotency_key, request, status, response, claimed_generation, created_at, completed_at
		FROM core_operations WHERE persona_id = $1 AND tool = $2 AND idempotency_key = $3`,
		personaID, tool, idemKey).
		Scan(&op.PersonaID, &op.OperationID, &op.TurnID, &op.Tool, &op.IdempotencyKey, &op.Request,
			&op.Status, &op.Response, &op.ClaimedGeneration, &op.CreatedAt, &op.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return op, ErrOpNotFound
	}
	return op, err
}

func (s *Store) CompleteOperation(ctx context.Context, personaID, operationID string, generation int64, response map[string]any, failed bool) (Operation, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Operation{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := requireGeneration(ctx, tx, personaID, generation); err != nil {
		return Operation{}, err
	}
	status := "done"
	if failed {
		status = "failed"
	}
	var op Operation
	err = tx.QueryRow(ctx, `
		UPDATE core_operations SET status = $4, response = $5, completed_at = now()
		WHERE persona_id = $1 AND operation_id = $2 AND claimed_generation = $3 AND status = 'running'
		RETURNING persona_id, operation_id, turn_id, tool, idempotency_key, request, status, response, claimed_generation, created_at, completed_at`,
		personaID, operationID, generation, status, response).
		Scan(&op.PersonaID, &op.OperationID, &op.TurnID, &op.Tool, &op.IdempotencyKey, &op.Request,
			&op.Status, &op.Response, &op.ClaimedGeneration, &op.CreatedAt, &op.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		var existing Operation
		err2 := tx.QueryRow(ctx, `
			SELECT persona_id, operation_id, turn_id, tool, idempotency_key, request, status, response, claimed_generation, created_at, completed_at
			FROM core_operations WHERE persona_id = $1 AND operation_id = $2`, personaID, operationID).
			Scan(&existing.PersonaID, &existing.OperationID, &existing.TurnID, &existing.Tool, &existing.IdempotencyKey,
				&existing.Request, &existing.Status, &existing.Response, &existing.ClaimedGeneration, &existing.CreatedAt, &existing.CompletedAt)
		if errors.Is(err2, pgx.ErrNoRows) {
			return Operation{}, ErrOpNotFound
		}
		if err2 != nil {
			return Operation{}, err2
		}
		if existing.Status != "running" {
			if err := tx.Commit(ctx); err != nil {
				return Operation{}, err
			}
			return existing, nil
		}
		return Operation{}, ErrGenerationFence
	}
	if err != nil {
		return Operation{}, fmt.Errorf("complete operation: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Operation{}, err
	}
	return op, nil
}

// DispatchDueSchedules atomically fires due schedules into durable wake
// inputs. Input id `sched:<schedule_id>` makes re-dispatch idempotent, so a
// schedule can never produce two wake inputs and is never lost between claim
// and enqueue. All pending due schedules fire late — the per-entry
// miss_policy is recorded contract for the scheduler milestones; the
// product-visible policy for missed wakes is deliberately not decided here.
func (s *Store) DispatchDueSchedules(ctx context.Context, personaID string, generation int64, now time.Time, limit int) ([]Schedule, error) {
	limit = clampLimit(limit, 16, 256)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := requireGeneration(ctx, tx, personaID, generation); err != nil {
		return nil, err
	}
	// LEAST(now(), $2): the caller's clock is a scheduling hint only — a
	// client-supplied future `now` must not pull future schedules into the
	// present. A stale `now` merely under-fires; the next dispatch catches up.
	rows, err := tx.Query(ctx, `
		SELECT schedule_id FROM core_schedules
		WHERE persona_id = $1 AND status = 'pending' AND wake_at <= LEAST(now(), $2)
		ORDER BY wake_at, schedule_id LIMIT $3 FOR UPDATE SKIP LOCKED`,
		personaID, now, limit)
	if err != nil {
		return nil, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := []Schedule{}
	for _, id := range ids {
		tag, err := tx.Exec(ctx, `
			INSERT INTO core_inputs (persona_id, input_id, kind, payload, actor_kind, actor_id, source_surface, attention, status, received_seq)
			SELECT persona_id, 'sched:' || schedule_id, 'wake', payload, 'schedule', schedule_id, 'core_schedules', 'reply', 'queued',
				(SELECT MIN(seq) FROM core_events
				 WHERE persona_id = core_schedules.persona_id AND kind = 'input_received'
					AND payload->>'input_id' = 'sched:' || core_schedules.schedule_id)
			FROM core_schedules
			WHERE persona_id = $1 AND schedule_id = $2
			ON CONFLICT (persona_id, input_id) DO NOTHING`,
			personaID, id)
		if err != nil {
			return nil, err
		}
		if tag.RowsAffected() == 0 {
			// Defense in depth: the sched: namespace is reserved at
			// SubmitInput, so a conflicting row can only be this schedule's
			// own wake input. Anything else means the wake would be dropped
			// while the schedule reports fired — fail loudly instead.
			var kind, actorID string
			if err := tx.QueryRow(ctx, `
				SELECT kind, actor_id FROM core_inputs
				WHERE persona_id = $1 AND input_id = $2`,
				personaID, schedInputPrefix+id).Scan(&kind, &actorID); err != nil {
				return nil, err
			}
			if kind != "wake" || actorID != id {
				return nil, fmt.Errorf("%w: wake input %s%s occupied by a foreign row", ErrTurnConflict, schedInputPrefix, id)
			}
		}
		var sch Schedule
		err = tx.QueryRow(ctx, `
			UPDATE core_schedules SET status = 'fired', fired_at = now(), claimed_generation = $3
			WHERE persona_id = $1 AND schedule_id = $2
			RETURNING persona_id, schedule_id, wake_at, payload, miss_policy, status, claimed_generation, created_at, fired_at`,
			personaID, id, generation).
			Scan(&sch.PersonaID, &sch.ScheduleID, &sch.WakeAt, &sch.Payload, &sch.MissPolicy, &sch.Status, &sch.ClaimedGeneration, &sch.CreatedAt, &sch.FiredAt)
		if err != nil {
			return nil, err
		}
		out = append(out, sch)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) Outbox(ctx context.Context, personaID string, afterSeq int64, limit int) ([]OutboxEntry, error) {
	limit = clampLimit(limit, 200, 1000)
	rows, err := s.pool.Query(ctx, `
		SELECT persona_id, seq, kind, payload, created_at, delivered_at
		FROM core_outbox WHERE persona_id = $1 AND seq > $2
		ORDER BY seq LIMIT $3`, personaID, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []OutboxEntry{}
	for rows.Next() {
		var e OutboxEntry
		if err := rows.Scan(&e.PersonaID, &e.Seq, &e.Kind, &e.Payload, &e.CreatedAt, &e.DeliveredAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
