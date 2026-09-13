package portable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrPersonaNotFound      = errors.New("persona not found")
	ErrTransferNotFound     = errors.New("transfer not found")
	ErrTransferConflict     = errors.New("transfer conflict")
	ErrPersonaExists        = errors.New("persona already present in this placement")
	ErrUnresolvedOperations = errors.New("persona has operations with unresolved effects")
	ErrNotPortable          = errors.New("state cannot be represented in this bundle format")
	ErrBadBundle            = errors.New("bundle rejected")
	ErrIntegrity            = errors.New("reference integrity check failed")
	ErrBadRequest           = errors.New("bad request")
)

var (
	transferIDRe = regexp.MustCompile(`^[A-Za-z0-9._-]{8,128}$`)
	uuidv7Re     = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
)

// sealHolder is the lease holder recorded while a transfer holds the cut.
// No core runs under it; it only explains the parked lease to an operator.
func sealHolder(transferID string) string { return "transfer:" + transferID }

// Service runs transfer steps against one placement's database.
type Service struct {
	pool *pgxpool.Pool
}

func NewService(pool *pgxpool.Pool) *Service {
	return &Service{pool: pool}
}

type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func validateIDs(personaID, transferID string) error {
	if !uuidv7Re.MatchString(personaID) {
		return fmt.Errorf("%w: persona_id must be a uuidv7", ErrBadRequest)
	}
	if !transferIDRe.MatchString(transferID) {
		return fmt.Errorf("%w: transfer_id must be 8-128 characters of [A-Za-z0-9._-]", ErrBadRequest)
	}
	return nil
}

// lockPersona takes FOR NO KEY UPDATE on the persona row. It conflicts with
// SubmitInput's share lock, so inputs serialize with authority changes, but
// not with the key-share locks that foreign-key checks take while a writer
// commits under its lease lock — which would otherwise deadlock with a seal.
func lockPersona(ctx context.Context, tx pgx.Tx, personaID string) (string, *string, error) {
	var authority string
	var transferID *string
	err := tx.QueryRow(ctx,
		`SELECT authority, transfer_id FROM core_personas WHERE persona_id = $1 FOR NO KEY UPDATE`,
		personaID).Scan(&authority, &transferID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil, ErrPersonaNotFound
	}
	return authority, transferID, err
}

func heldBy(transferID *string, id string) bool {
	return transferID != nil && *transferID == id
}

// ledger reads one side of a transfer. Status, digest and update time come
// from the ledger columns; the rest is the receipt recorded when the step
// that created the row verified it.
func ledger(ctx context.Context, q querier, direction, transferID string, lock bool) (Receipt, error) {
	sql := `SELECT persona_id::text, status, COALESCE(content_sha256, ''), receipt, updated_at
		FROM core_transfers WHERE direction = $1 AND transfer_id = $2`
	if lock {
		sql += ` FOR UPDATE`
	}
	var rec Receipt
	var personaID, status, digest string
	var raw []byte
	var updated time.Time
	err := q.QueryRow(ctx, sql, direction, transferID).Scan(&personaID, &status, &digest, &raw, &updated)
	if errors.Is(err, pgx.ErrNoRows) {
		return rec, ErrTransferNotFound
	}
	if err != nil {
		return rec, err
	}
	if err := json.Unmarshal(raw, &rec); err != nil {
		return rec, fmt.Errorf("decode transfer receipt: %w", err)
	}
	rec.Direction = direction
	rec.TransferID = transferID
	rec.PersonaID = personaID
	rec.Status = status
	rec.ContentSHA256 = digest
	rec.UpdatedAt = updated.UTC()
	return rec, nil
}

func setStatus(ctx context.Context, tx pgx.Tx, direction, transferID, status string) (time.Time, error) {
	var updated time.Time
	err := tx.QueryRow(ctx,
		`UPDATE core_transfers SET status = $3, updated_at = now()
		 WHERE direction = $1 AND transfer_id = $2 RETURNING updated_at`,
		direction, transferID, status).Scan(&updated)
	return updated.UTC(), err
}

// Status returns the recorded state of one side of a transfer — the answer
// to a step whose response was lost.
func (s *Service) Status(ctx context.Context, direction, transferID string) (Receipt, error) {
	if direction != "export" && direction != "import" {
		return Receipt{}, fmt.Errorf("%w: direction must be export or import", ErrBadRequest)
	}
	return ledger(ctx, s.pool, direction, transferID, false)
}

// Seal fixes the export cut on the source. In one transaction it bumps the
// writer generation past every holder and parks the lease, marks the persona
// sealed, verifies the state, and records the cut. From commit on, the
// running secretary is fenced at its next state call, no new writer can be
// acquired, and new inputs are refused, so nothing the export reads can
// change. Work in flight is not lost: an uncommitted turn stays running and
// is recovered by whichever placement runs the secretary next.
//
// Sealing refuses state that could duplicate or lose an effect: an operation
// still running has an unknown external result and must be resolved first.
func (s *Service) Seal(ctx context.Context, personaID, transferID string) (Receipt, error) {
	if err := validateIDs(personaID, transferID); err != nil {
		return Receipt{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Receipt{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	authority, held, err := lockPersona(ctx, tx, personaID)
	if err != nil {
		return Receipt{}, err
	}
	if heldBy(held, transferID) && (authority == "sealed" || authority == "transferred") {
		rec, err := ledger(ctx, tx, "export", transferID, false)
		if err != nil {
			return Receipt{}, err
		}
		return rec, tx.Commit(ctx)
	}
	if authority != "active" {
		return Receipt{}, fmt.Errorf("%w: persona authority is %s", ErrTransferConflict, authority)
	}
	if _, err := ledger(ctx, tx, "export", transferID, false); err == nil {
		return Receipt{}, fmt.Errorf("%w: transfer_id %s was already used", ErrTransferConflict, transferID)
	} else if !errors.Is(err, ErrTransferNotFound) {
		return Receipt{}, err
	}

	// Taking the lease row waits for an in-flight mutation of the current
	// writer to commit, so it lands inside the cut; every later mutation of
	// that writer presents a stale generation and is fenced.
	var epoch int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO core_writer_leases (persona_id, generation, holder_id, expires_at)
		VALUES ($1, 1, $2, now() + interval '100 years')
		ON CONFLICT (persona_id) DO UPDATE SET
			generation  = core_writer_leases.generation + 1,
			holder_id   = EXCLUDED.holder_id,
			acquired_at = now(),
			expires_at  = EXCLUDED.expires_at
		RETURNING generation`,
		personaID, sealHolder(transferID)).Scan(&epoch); err != nil {
		return Receipt{}, fmt.Errorf("seal writer lease: %w", err)
	}

	running, err := strings_(ctx, tx, `
		SELECT operation_id || ' (' || tool || ')' FROM core_operations
		WHERE persona_id = $1 AND status = 'running' ORDER BY operation_id COLLATE "C"`, personaID)
	if err != nil {
		return Receipt{}, err
	}
	if len(running) > 0 {
		return Receipt{}, fmt.Errorf("%w: %s; reconcile them before sealing",
			ErrUnresolvedOperations, strings.Join(running, ", "))
	}
	var literalNulls int64
	if err := tx.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM core_turns WHERE persona_id = $1 AND (
		            jsonb_typeof(output) = 'null' OR jsonb_typeof(usage) = 'null'
		            OR jsonb_typeof(commit_request) = 'null'))
		     + (SELECT count(*) FROM core_operations WHERE persona_id = $1
		            AND jsonb_typeof(response) = 'null')`,
		personaID).Scan(&literalNulls); err != nil {
		return Receipt{}, err
	}
	if literalNulls > 0 {
		return Receipt{}, fmt.Errorf("%w: %d nullable jsonb values hold the JSON null literal", ErrNotPortable, literalNulls)
	}

	if _, err := tx.Exec(ctx,
		`UPDATE core_personas SET authority = 'sealed', transfer_id = $2 WHERE persona_id = $1`,
		personaID, transferID); err != nil {
		return Receipt{}, err
	}
	violations, err := verifyCut(ctx, tx, personaID)
	if err != nil {
		return Receipt{}, err
	}
	if len(violations) > 0 {
		return Receipt{}, fmt.Errorf("%w on the source: %s", ErrIntegrity, describe(violations))
	}
	rows, cont, cut, err := summarize(ctx, tx, personaID)
	if err != nil {
		return Receipt{}, err
	}
	cut.GenerationHighWater = epoch
	var sealedAt time.Time
	if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&sealedAt); err != nil {
		return Receipt{}, err
	}
	rec := Receipt{
		Direction:     "export",
		TransferID:    transferID,
		PersonaID:     personaID,
		Status:        "sealed",
		FormatVersion: FormatVersion,
		SealedAt:      sealedAt.UTC(),
		Cut:           cut,
		Rows:          rows,
		Continuity:    cont,
		NotIncluded:   NotIncluded,
		UpdatedAt:     sealedAt.UTC(),
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return Receipt{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO core_transfers (direction, transfer_id, persona_id, status, format_version, receipt, created_at, updated_at)
		VALUES ('export', $1, $2, 'sealed', $3, $4, $5, $5)`,
		transferID, personaID, FormatVersion, raw, sealedAt); err != nil {
		return Receipt{}, fmt.Errorf("record transfer: %w", err)
	}
	return rec, tx.Commit(ctx)
}

// Complete marks the source transferred once the destination has verified
// the same bundle. destinationSHA256 is the content digest from the
// destination's import receipt; it must equal the digest this source
// exported, so the source only records a hand-off of exactly what it sent.
// The source rows stay as history; deleting them is a separate decision.
func (s *Service) Complete(ctx context.Context, personaID, transferID, destinationSHA256 string) (Receipt, error) {
	if err := validateIDs(personaID, transferID); err != nil {
		return Receipt{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Receipt{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	authority, held, err := lockPersona(ctx, tx, personaID)
	if err != nil {
		return Receipt{}, err
	}
	rec, err := ledger(ctx, tx, "export", transferID, true)
	if err != nil {
		return Receipt{}, err
	}
	if rec.PersonaID != personaID || !heldBy(held, transferID) {
		return Receipt{}, fmt.Errorf("%w: persona is not held by transfer %s", ErrTransferConflict, transferID)
	}
	if rec.ContentSHA256 == "" {
		return Receipt{}, fmt.Errorf("%w: transfer %s has not been exported", ErrTransferConflict, transferID)
	}
	if rec.ContentSHA256 != destinationSHA256 {
		return Receipt{}, fmt.Errorf("%w: destination digest %s does not match exported digest %s",
			ErrTransferConflict, destinationSHA256, rec.ContentSHA256)
	}
	switch authority {
	case "transferred":
		return rec, tx.Commit(ctx)
	case "sealed":
	default:
		return Receipt{}, fmt.Errorf("%w: persona authority is %s", ErrTransferConflict, authority)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE core_personas SET authority = 'transferred' WHERE persona_id = $1`, personaID); err != nil {
		return Receipt{}, err
	}
	if rec.UpdatedAt, err = setStatus(ctx, tx, "export", transferID, "completed"); err != nil {
		return Receipt{}, err
	}
	rec.Status = "completed"
	return rec, tx.Commit(ctx)
}

// Abort returns a sealed source to active so the same secretary continues
// where it was. The caller must first establish that the destination did not
// activate this transfer (Status on the destination, then Discard there);
// the source alone cannot know, and aborting after activation would leave
// two placements able to run the same secretary.
func (s *Service) Abort(ctx context.Context, personaID, transferID string) (Receipt, error) {
	if err := validateIDs(personaID, transferID); err != nil {
		return Receipt{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Receipt{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	authority, held, err := lockPersona(ctx, tx, personaID)
	if err != nil {
		return Receipt{}, err
	}
	rec, err := ledger(ctx, tx, "export", transferID, true)
	if err != nil {
		return Receipt{}, err
	}
	if rec.PersonaID != personaID {
		return Receipt{}, fmt.Errorf("%w: transfer %s belongs to another persona", ErrTransferConflict, transferID)
	}
	if rec.Status == "aborted" {
		return rec, tx.Commit(ctx)
	}
	if authority != "sealed" || !heldBy(held, transferID) {
		return Receipt{}, fmt.Errorf("%w: persona authority is %s", ErrTransferConflict, authority)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE core_personas SET authority = 'active', transfer_id = NULL WHERE persona_id = $1`, personaID); err != nil {
		return Receipt{}, err
	}
	// Expire the parked lease so the next writer acquires generation+1.
	if _, err := tx.Exec(ctx,
		`UPDATE core_writer_leases SET expires_at = now() WHERE persona_id = $1`, personaID); err != nil {
		return Receipt{}, err
	}
	if rec.UpdatedAt, err = setStatus(ctx, tx, "export", transferID, "aborted"); err != nil {
		return Receipt{}, err
	}
	rec.Status = "aborted"
	return rec, tx.Commit(ctx)
}

// Activate makes a staged import the authoritative placement. From commit
// on, a writer can be acquired and inputs accepted here. The first writer
// acquires a generation above the source's epoch and runs ordinary recovery,
// which interrupts carried running turns and requeues their inputs.
func (s *Service) Activate(ctx context.Context, personaID, transferID string) (Receipt, error) {
	if err := validateIDs(personaID, transferID); err != nil {
		return Receipt{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Receipt{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	authority, held, err := lockPersona(ctx, tx, personaID)
	if err != nil {
		return Receipt{}, err
	}
	rec, err := ledger(ctx, tx, "import", transferID, true)
	if err != nil {
		return Receipt{}, err
	}
	if rec.PersonaID != personaID {
		return Receipt{}, fmt.Errorf("%w: transfer %s belongs to another persona", ErrTransferConflict, transferID)
	}
	if rec.Status == "activated" {
		return rec, tx.Commit(ctx)
	}
	if rec.Status != "staged" || authority != "staged" || !heldBy(held, transferID) {
		return Receipt{}, fmt.Errorf("%w: transfer is %s and persona authority is %s", ErrTransferConflict, rec.Status, authority)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE core_personas SET authority = 'active' WHERE persona_id = $1`, personaID); err != nil {
		return Receipt{}, err
	}
	if rec.UpdatedAt, err = setStatus(ctx, tx, "import", transferID, "activated"); err != nil {
		return Receipt{}, err
	}
	rec.Status = "activated"
	return rec, tx.Commit(ctx)
}

// Discard removes a staged, never-activated import so the transfer can be
// retried or abandoned. The ledger row remains as a record.
func (s *Service) Discard(ctx context.Context, personaID, transferID string) (Receipt, error) {
	if err := validateIDs(personaID, transferID); err != nil {
		return Receipt{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Receipt{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rec, err := ledger(ctx, tx, "import", transferID, true)
	if err != nil {
		return Receipt{}, err
	}
	if rec.PersonaID != personaID {
		return Receipt{}, fmt.Errorf("%w: transfer %s belongs to another persona", ErrTransferConflict, transferID)
	}
	if rec.Status == "discarded" {
		return rec, tx.Commit(ctx)
	}
	if rec.Status != "staged" {
		return Receipt{}, fmt.Errorf("%w: an %s transfer cannot be discarded", ErrTransferConflict, rec.Status)
	}
	authority, held, err := lockPersona(ctx, tx, personaID)
	if err != nil {
		return Receipt{}, err
	}
	if authority != "staged" || !heldBy(held, transferID) {
		return Receipt{}, fmt.Errorf("%w: persona authority is %s", ErrTransferConflict, authority)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM core_personas WHERE persona_id = $1`, personaID); err != nil {
		return Receipt{}, fmt.Errorf("discard staged persona: %w", err)
	}
	if rec.UpdatedAt, err = setStatus(ctx, tx, "import", transferID, "discarded"); err != nil {
		return Receipt{}, err
	}
	rec.Status = "discarded"
	return rec, tx.Commit(ctx)
}

// summarize counts carried rows and what continues at the cut.
func summarize(ctx context.Context, q querier, personaID string) (map[string]int64, Continuity, Cut, error) {
	rows := map[string]int64{}
	var cont Continuity
	var cut Cut
	for _, t := range append([]table{personaTable}, coreTables...) {
		var n int64
		if err := q.QueryRow(ctx, `SELECT count(*) FROM `+t.name+` WHERE persona_id = $1`, personaID).Scan(&n); err != nil {
			return nil, cont, cut, fmt.Errorf("count %s: %w", t.name, err)
		}
		rows[t.name] = n
	}
	err := q.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM core_events WHERE persona_id = $1),
			(SELECT count(*) FROM core_events WHERE persona_id = $1 AND kind = 'note'),
			(SELECT count(*) FROM core_inputs WHERE persona_id = $1 AND status = 'queued'),
			(SELECT count(*) FROM core_inputs WHERE persona_id = $1 AND status = 'claimed'),
			(SELECT count(*) FROM core_turns WHERE persona_id = $1 AND status = 'running'),
			(SELECT count(*) FROM core_turn_plans p JOIN core_inputs i
				ON i.persona_id = p.persona_id AND i.input_id = p.input_id
				WHERE p.persona_id = $1 AND i.status <> 'done'),
			(SELECT count(*) FROM core_schedules WHERE persona_id = $1 AND status IN ('pending', 'claimed')),
			(SELECT count(*) FROM core_outbox WHERE persona_id = $1 AND delivered_at IS NULL),
			(SELECT COALESCE(max(seq), 0) FROM core_events WHERE persona_id = $1),
			(SELECT COALESCE(max(seq), 0) FROM core_outbox WHERE persona_id = $1)`,
		personaID).Scan(&cont.JournalEvents, &cont.Notes, &cont.QueuedInputs, &cont.ClaimedInputs,
		&cont.RunningTurns, &cont.UnfinishedPlans, &cont.PendingSchedules, &cont.UndeliveredOut,
		&cut.LatestEventSeq, &cut.LatestOutboxSeq)
	return rows, cont, cut, err
}

func strings_(ctx context.Context, q querier, sql string, args ...any) ([]string, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
