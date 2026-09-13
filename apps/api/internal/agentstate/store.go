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
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
)

type Persona struct {
	PersonaID   string    `json:"persona_id"`
	HumanID     *string   `json:"human_id"`
	DisplayName string    `json:"display_name"`
	CreatedAt   time.Time `json:"created_at"`
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

// LoadResult is one coarse read: the running or freshly begun turn, its
// claimed input, and the journal tail the core assembles context from.
type LoadResult struct {
	Turn    *Turn   `json:"turn"`
	Input   *Input  `json:"input"`
	Context []Event `json:"context"`
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
func requireGeneration(ctx context.Context, tx pgx.Tx, personaID string, generation int64) error {
	var current int64
	err := tx.QueryRow(ctx,
		`SELECT generation FROM core_writer_leases WHERE persona_id = $1 FOR UPDATE`,
		personaID).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrGenerationFence
	}
	if err != nil {
		return fmt.Errorf("read writer lease: %w", err)
	}
	if current != generation {
		return ErrGenerationFence
	}
	return nil
}

func (s *Store) EnsurePersona(ctx context.Context, personaID string, humanID *string, displayName string) (Persona, bool, error) {
	var p Persona
	err := s.pool.QueryRow(ctx, `
		INSERT INTO core_personas (persona_id, human_id, display_name)
		VALUES ($1, $2, $3)
		ON CONFLICT (persona_id) DO NOTHING
		RETURNING persona_id, human_id, display_name, created_at`,
		personaID, humanID, displayName).
		Scan(&p.PersonaID, &p.HumanID, &p.DisplayName, &p.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		err = s.pool.QueryRow(ctx,
			`SELECT persona_id, human_id, display_name, created_at FROM core_personas WHERE persona_id = $1`,
			personaID).Scan(&p.PersonaID, &p.HumanID, &p.DisplayName, &p.CreatedAt)
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
		`SELECT persona_id, human_id, display_name, created_at FROM core_personas WHERE persona_id = $1`,
		personaID).Scan(&st.Persona.PersonaID, &st.Persona.HumanID, &st.Persona.DisplayName, &st.Persona.CreatedAt)
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
// held by the same holder, returning the new fencing generation.
func (s *Store) AcquireWriter(ctx context.Context, personaID, holderID string, ttl time.Duration) (WriterLease, error) {
	var lease WriterLease
	err := s.pool.QueryRow(ctx, `
		INSERT INTO core_writer_leases (persona_id, generation, holder_id, expires_at)
		SELECT $1::uuidv7, 1, $2, now() + $3::interval FROM core_personas WHERE persona_id = $1::uuidv7
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
		var exists bool
		if err := s.pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM core_personas WHERE persona_id = $1)`,
			personaID).Scan(&exists); err != nil {
			return lease, err
		}
		if !exists {
			return lease, ErrPersonaNotFound
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

func (s *Store) ReleaseWriter(ctx context.Context, personaID, holderID string, generation int64) error {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM core_writer_leases WHERE persona_id = $1 AND generation = $2 AND holder_id = $3`,
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
	claimed_generation, turn_id, created_at, done_at`

type inputScanner interface {
	Scan(dest ...any) error
}

func scanInput(row inputScanner) (Input, error) {
	var in Input
	err := row.Scan(&in.PersonaID, &in.InputID, &in.Kind, &in.Payload,
		&in.ActorKind, &in.ActorID, &in.SourceSurface, &in.ThreadID,
		&in.OccurredAt, &in.Attention, &in.Status, &in.ClaimedGeneration,
		&in.TurnID, &in.CreatedAt, &in.DoneAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return in, ErrInputNotFound
	}
	return in, err
}

// SubmitInput durably records an input. Re-submitting the same input_id
// replays the stored row — this is the replay-after-lost-response path.
func (s *Store) SubmitInput(ctx context.Context, in *Input) (Input, bool, error) {
	if in.Attention == "" {
		in.Attention = "reply"
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Input{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var stored Input
	err = tx.QueryRow(ctx, `
		INSERT INTO core_inputs (persona_id, input_id, kind, payload, actor_kind, actor_id,
			source_surface, thread_id, occurred_at, attention, status)
		SELECT $1::uuidv7, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'queued'
		FROM core_personas WHERE persona_id = $1::uuidv7
		ON CONFLICT (persona_id, input_id) DO NOTHING
		RETURNING `+inputCols,
		in.PersonaID, in.InputID, in.Kind, in.Payload, in.ActorKind, in.ActorID,
		in.SourceSurface, in.ThreadID, in.OccurredAt, in.Attention).
		Scan(&stored.PersonaID, &stored.InputID, &stored.Kind, &stored.Payload,
			&stored.ActorKind, &stored.ActorID, &stored.SourceSurface, &stored.ThreadID,
			&stored.OccurredAt, &stored.Attention, &stored.Status, &stored.ClaimedGeneration,
			&stored.TurnID, &stored.CreatedAt, &stored.DoneAt)
	if errors.Is(err, pgx.ErrNoRows) {
		var exists bool
		err = tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM core_personas WHERE persona_id = $1)`, in.PersonaID).Scan(&exists)
		if err != nil {
			return Input{}, false, err
		}
		if !exists {
			return Input{}, false, ErrPersonaNotFound
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
		return Input{}, false, fmt.Errorf("submit input: %w", err)
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

func (s *Store) journalTail(ctx context.Context, db interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, personaID string, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := db.Query(ctx, `
		SELECT persona_id, seq, turn_id, kind, payload, created_at
		FROM (SELECT * FROM core_events WHERE persona_id = $1 ORDER BY seq DESC LIMIT $2) recent
		ORDER BY seq`, personaID, limit)
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
			UPDATE core_inputs SET status = 'claimed', claimed_generation = $2
			WHERE (persona_id, input_id) = (
				SELECT persona_id, input_id FROM core_inputs
				WHERE persona_id = $1 AND status = 'queued'
				ORDER BY created_at, input_id LIMIT 1 FOR UPDATE SKIP LOCKED
			)
			RETURNING `+inputCols,
			personaID, generation).
			Scan(&in.PersonaID, &in.InputID, &in.Kind, &in.Payload,
				&in.ActorKind, &in.ActorID, &in.SourceSurface, &in.ThreadID,
				&in.OccurredAt, &in.Attention, &in.Status, &in.ClaimedGeneration,
				&in.TurnID, &in.CreatedAt, &in.DoneAt)
		if errors.Is(err, pgx.ErrNoRows) {
			res.Context, err = s.journalTail(ctx, tx, personaID, contextLimit)
			if err != nil {
				return res, err
			}
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
	res.Context, err = s.journalTail(ctx, tx, personaID, contextLimit)
	if err != nil {
		return res, err
	}
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
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return t, nil
	}
	if t.Generation != generation {
		return nil, ErrTurnConflict
	}
	if err := s.appendEventsTx(ctx, tx, personaID, turnID, req.Events); err != nil {
		return nil, err
	}
	switch req.Outcome {
	case "complete":
		if err := tx.QueryRow(ctx, `
			UPDATE core_turns SET status = 'done', finished_at = now(), output = $3, usage = $4
			WHERE persona_id = $1 AND turn_id = $2
			RETURNING status, finished_at, output, usage`,
			personaID, turnID, req.Output, req.Usage).
			Scan(&t.Status, &t.FinishedAt, &t.Output, &t.Usage); err != nil {
			return nil, fmt.Errorf("complete turn: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE core_inputs SET status = 'done', done_at = now() WHERE persona_id = $1 AND input_id = $2`,
			personaID, t.InputID); err != nil {
			return nil, err
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
			return nil, fmt.Errorf("append outbox: %w", err)
		}
	case "fail":
		if err := tx.QueryRow(ctx, `
			UPDATE core_turns SET status = 'failed', finished_at = now(), error = $3
			WHERE persona_id = $1 AND turn_id = $2 RETURNING status, finished_at, error`,
			personaID, turnID, req.Error).
			Scan(&t.Status, &t.FinishedAt, &t.Error); err != nil {
			return nil, err
		}
		if req.Retryable {
			if _, err := tx.Exec(ctx, `
				UPDATE core_inputs SET status = 'queued', claimed_generation = NULL, turn_id = NULL
				WHERE persona_id = $1 AND input_id = $2`, personaID, t.InputID); err != nil {
				return nil, err
			}
		} else {
			if _, err := tx.Exec(ctx, `
				UPDATE core_inputs SET status = 'done', done_at = now()
				WHERE persona_id = $1 AND input_id = $2`, personaID, t.InputID); err != nil {
				return nil, err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return t, nil
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
	_, err := tx.Exec(ctx, `
		INSERT INTO core_events (persona_id, seq, turn_id, kind, payload)
		SELECT $1, base.base + e.rn, $2, e.kind, e.payload
		FROM (
			SELECT u.kind, u.payload, ROW_NUMBER() OVER () AS rn
			FROM unnest($3::text[], $4::jsonb[]) AS u(kind, payload)
		) e
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
	if err := tx.Commit(ctx); err != nil {
		return res, err
	}
	return res, nil
}

func (s *Store) Events(ctx context.Context, personaID string, afterSeq int64, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 200
	}
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

// internalToolResponse applies a state-internal tool's effect inside the
// claim transaction: the operation record and its effect are atomic, so a
// crash cannot leave an unrecorded effect or a dangling record.
func (s *Store) internalToolResponse(ctx context.Context, tx pgx.Tx, personaID, turnID, tool string, request map[string]any) (map[string]any, bool, error) {
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
		var sch Schedule
		err = tx.QueryRow(ctx, `
			INSERT INTO core_schedules (persona_id, schedule_id, wake_at, payload, miss_policy, status)
			VALUES ($1, $2, $3, $4, $5, 'pending')
			ON CONFLICT (persona_id, schedule_id) DO NOTHING
			RETURNING persona_id, schedule_id, wake_at, payload, miss_policy, status, created_at`,
			personaID, scheduleID, wakeAt, payload, missPolicy).
			Scan(&sch.PersonaID, &sch.ScheduleID, &sch.WakeAt, &sch.Payload, &sch.MissPolicy, &sch.Status, &sch.CreatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			// Schedule already exists under this id — replay it.
			err = tx.QueryRow(ctx, `
				SELECT persona_id, schedule_id, wake_at, payload, miss_policy, status, created_at
				FROM core_schedules WHERE persona_id = $1 AND schedule_id = $2`,
				personaID, scheduleID).
				Scan(&sch.PersonaID, &sch.ScheduleID, &sch.WakeAt, &sch.Payload, &sch.MissPolicy, &sch.Status, &sch.CreatedAt)
		}
		if err != nil {
			return nil, false, fmt.Errorf("schedule.set: %w", err)
		}
		return map[string]any{"schedule": sch}, true, nil
	case "journal.note":
		text, _ := request["text"].(string)
		if text == "" {
			return nil, false, fmt.Errorf("%w: journal.note requires text", ErrBadRequest)
		}
		var seq int64
		err := tx.QueryRow(ctx, `
			INSERT INTO core_events (persona_id, seq, turn_id, kind, payload)
			SELECT $1::uuidv7, COALESCE(MAX(seq), 0) + 1, $2, 'note', $3
			FROM core_events WHERE persona_id = $1::uuidv7
			RETURNING seq`,
			personaID, turnID, map[string]any{"text": text}).Scan(&seq)
		if err != nil {
			return nil, false, fmt.Errorf("journal.note: %w", err)
		}
		return map[string]any{"seq": seq, "kind": "note"}, true, nil
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
func (s *Store) ClaimOperation(ctx context.Context, personaID, turnID string, generation int64, operationID, tool, idemKey string, request map[string]any) (Operation, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Operation{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := requireGeneration(ctx, tx, personaID, generation); err != nil {
		return Operation{}, false, err
	}
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
		if err := tx.Commit(ctx); err != nil {
			return Operation{}, false, err
		}
		return op, false, nil
	}
	if err != nil {
		return Operation{}, false, fmt.Errorf("claim operation: %w", err)
	}
	// Fresh claim: apply the state-internal effect and finish the record in
	// the same transaction.
	response, internal, err := s.internalToolResponse(ctx, tx, personaID, turnID, tool, request)
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
			return Operation{}, false, fmt.Errorf("finish internal operation: %w", err)
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
	if limit <= 0 {
		limit = 16
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := requireGeneration(ctx, tx, personaID, generation); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT schedule_id FROM core_schedules
		WHERE persona_id = $1 AND status = 'pending' AND wake_at <= $2
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
		if _, err := tx.Exec(ctx, `
			INSERT INTO core_inputs (persona_id, input_id, kind, payload, actor_kind, actor_id, source_surface, attention, status)
			SELECT persona_id, 'sched:' || schedule_id, 'wake', payload, 'schedule', schedule_id, 'core_schedules', 'reply', 'queued'
			FROM core_schedules
			WHERE persona_id = $1 AND schedule_id = $2
			ON CONFLICT (persona_id, input_id) DO NOTHING`,
			personaID, id); err != nil {
			return nil, err
		}
		var sch Schedule
		err := tx.QueryRow(ctx, `
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
	if limit <= 0 {
		limit = 200
	}
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
