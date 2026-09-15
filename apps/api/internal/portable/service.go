package portable

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
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
	ErrMissingProof         = errors.New("destination proof required")
)

var (
	transferIDRe = regexp.MustCompile(`^[A-Za-z0-9._-]{8,128}$`)
	uuidv7Re     = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	proofRe      = regexp.MustCompile(`^[0-9a-f]{64}$`)
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

// PlacementID returns this placement's stable identity, minting it on first
// use. A seal addresses its bundle to exactly one placement id.
func (s *Service) PlacementID(ctx context.Context) (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO core_placement (placement_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		id.String()); err != nil {
		return "", err
	}
	var got string
	err = s.pool.QueryRow(ctx, `SELECT placement_id FROM core_placement`).Scan(&got)
	return got, err
}

// newTransferKey mints the per-transfer key that travels inside the bundle.
func newTransferKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// transferProof is the evidence a destination produces when it commits a
// transition: HMAC-SHA256 of "action:transfer_id:persona_id:destination_id"
// under the transfer's key. The destination computes it over its own
// placement id; the source verifies it against the destination it recorded
// at seal, so a proof minted by any other placement — for example a retire
// dispatched to the wrong service — can never satisfy the source. The source
// verifies it before ending its own authority, so completing or aborting
// requires a value the destination only publishes when that step actually
// committed — readable again from its transfer Status after a lost response.
// A party holding the bundle can mint any proof; it already holds the whole
// life, and deliberate forgery is out of scope. The gates exist so that an
// honest caller cannot end authority by accident.
func transferProof(key, action, transferID, personaID, destinationID string) string {
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(action + ":" + transferID + ":" + personaID + ":" + destinationID))
	return hex.EncodeToString(mac.Sum(nil))
}

func proofMatches(key, action, transferID, personaID, destinationID, presented string) bool {
	return subtle.ConstantTimeCompare(
		[]byte(transferProof(key, action, transferID, personaID, destinationID)), []byte(presented)) == 1
}

// transferLock serializes an import with a retire of the same transfer on
// this placement: without it a retire's tombstone and a slow import could
// commit in either order and leave a staged copy after cancellation.
func transferLock(ctx context.Context, tx pgx.Tx, transferID string) error {
	_, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		"portable.transfer:"+transferID)
	return err
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

// ledger reads one side of a transfer. Status, digest, destination and update
// time come from the ledger columns; the rest is the receipt recorded when
// the step that created the row verified it. The proof key is loaded into the
// receipt's unexported key field for verification; it is never serialized.
func ledger(ctx context.Context, q querier, direction, transferID string, lock bool) (Receipt, error) {
	sql := `SELECT persona_id::text, status, COALESCE(content_sha256, ''),
			COALESCE(proof_key, ''), COALESCE(destination_id::text, ''), receipt, updated_at
		FROM core_transfers WHERE direction = $1 AND transfer_id = $2`
	if lock {
		sql += ` FOR UPDATE`
	}
	var rec Receipt
	var personaID, status, digest, key, destination string
	var raw []byte
	var updated time.Time
	err := q.QueryRow(ctx, sql, direction, transferID).
		Scan(&personaID, &status, &digest, &key, &destination, &raw, &updated)
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
	rec.DestinationID = destination
	rec.key = key
	rec.UpdatedAt = updated.UTC()
	return rec, nil
}

// commit records a transition: the new status plus the receipt as augmented
// by that step (the proofs it produced), so Status returns them after a lost
// response.
func commit(ctx context.Context, tx pgx.Tx, direction, transferID, status string, rec *Receipt) error {
	rec.Status = status
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	var updated time.Time
	err = tx.QueryRow(ctx,
		`UPDATE core_transfers SET status = $3, receipt = $4, updated_at = now()
		 WHERE direction = $1 AND transfer_id = $2 RETURNING updated_at`,
		direction, transferID, status, raw).Scan(&updated)
	rec.UpdatedAt = updated.UTC()
	return err
}

// Status returns the recorded state of one side of a transfer — the answer
// to a step whose response was lost, including the proofs the destination
// produced.
func (s *Service) Status(ctx context.Context, direction, transferID string) (Receipt, error) {
	if direction != "export" && direction != "import" {
		return Receipt{}, fmt.Errorf("%w: direction must be export or import", ErrBadRequest)
	}
	return ledger(ctx, s.pool, direction, transferID, false)
}

// Seal fixes the export cut on the source and addresses it to one
// destination. In one transaction it bumps the writer generation past every
// holder and parks the lease, marks the persona sealed, verifies the state,
// and records the cut, the destination and a fresh transfer key. From commit
// on, the running secretary is fenced at its next state call, no new writer
// can be acquired, and new inputs are refused, so nothing the export reads
// can change. Work in flight is not lost: an uncommitted turn stays running
// and is recovered by whichever placement runs the secretary next.
//
// Sealing refuses state that could duplicate or lose an effect: an operation
// still running has an unknown external result and must be resolved first.
// destinationID must name another placement (read it from the destination's
// /internal/core/placement); retargeting means sealing a new transfer.
func (s *Service) Seal(ctx context.Context, personaID, transferID, destinationID string) (Receipt, error) {
	if err := validateIDs(personaID, transferID); err != nil {
		return Receipt{}, err
	}
	if !uuidv7Re.MatchString(destinationID) {
		return Receipt{}, fmt.Errorf("%w: destination_id must be the destination's placement uuidv7", ErrBadRequest)
	}
	own, err := s.PlacementID(ctx)
	if err != nil {
		return Receipt{}, err
	}
	if destinationID == own {
		return Receipt{}, fmt.Errorf("%w: destination_id %s is this placement", ErrBadRequest, destinationID)
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
		if rec.DestinationID != destinationID {
			return Receipt{}, fmt.Errorf("%w: transfer %s was sealed for destination %s",
				ErrTransferConflict, transferID, rec.DestinationID)
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
	// A non-terminal job is an unresolved external effect of the same kind:
	// its runner claim belongs to this placement, so moving now would leave
	// work running detached — finishing detached on a sealed source whose
	// notification lands in a dead inbox — while the destination knows
	// nothing of it. Finish or cancel the jobs, then seal. The persona row
	// lock held here serializes this check against submitJobTx's share-lock.
	inflight, err := strings_(ctx, tx, `
		SELECT job_id || ' (' || status || ')' FROM core_jobs
		WHERE persona_id = $1 AND status IN ('queued','running','cancel_requested')
		ORDER BY job_id COLLATE "C"`, personaID)
	if err != nil {
		return Receipt{}, err
	}
	if len(inflight) > 0 {
		return Receipt{}, fmt.Errorf("%w: jobs %s; wait for them to finish or cancel them before sealing",
			ErrUnresolvedOperations, strings.Join(inflight, ", "))
	}
	// A live call session is a runner claim of the same detachable kind as
	// a job: its media actor keeps minting tickets and speaking on this
	// placement while the seal claims authority ended. Call sessions are
	// messaging-local state — never carried in the bundle — so seal ends
	// the participation here with an explicit reason rather than refusing:
	// the secretary's transfer must not be blocked by a call it cannot
	// leave post-seal. Non-terminal speech is recorded honestly: never
	// started expires; mid-flight becomes unknown. The persona row lock
	// held above serializes this sweep against concurrent call mutations —
	// the bridge's share-locked authority check sees the retired authority
	// after commit and refuses every later call operation.
	if _, err := tx.Exec(ctx, `
		UPDATE call_utterances u
		SET status = CASE WHEN u.status = 'intended' THEN 'expired' ELSE 'unknown' END,
		    detail = CASE WHEN jsonb_typeof(u.detail) = 'object'
		                  THEN u.detail ELSE '{}'::jsonb END || '{"reason":"transfer_sealed"}'::jsonb,
		    updated_at = now()
		FROM call_sessions s
		WHERE u.session_id = s.session_id AND s.personality_agent_id = $1
		  AND u.status IN ('intended','dequeued','emitting')`, personaID); err != nil {
		return Receipt{}, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE call_sessions
		SET status='revoked', ended_at=now(), end_reason='transfer_sealed',
		    claimed_by=NULL, claim_expires_at=NULL, updated_at=now()
		WHERE personality_agent_id = $1
		  AND status IN ('requested','claimed','active','ending','interrupted')`, personaID); err != nil {
		return Receipt{}, err
	}
	// The model selection is human-scoped account state: it binds the
	// persona's human, and the destination binds a different account whose
	// connection rows cannot be assumed to exist. Snapshot it onto the
	// persona as non-secret intent — an explicit 'none', or the selected
	// connection's kind and metadata without any credential — so the
	// bundle carries what the user chose. A persona that itself arrived by
	// transfer may still carry an unresolved intent: when there is no
	// selection to snapshot, that intent is what the persona owes, so it
	// is kept rather than silently erased. The value this snapshot
	// replaces is recorded on the export receipt so Abort can restore the
	// pre-seal semantics exactly.
	// At the destination the intent is enforced, not silently substituted:
	// modelBinding reports needs_rebinding until the destination human
	// selects a connection of the same kind (or the intent is explicitly
	// cleared), and the core refuses to run a model rather than falling
	// back to an environment default. A persona with no selection and no
	// carried intent snapshots NULL and keeps the destination's ordinary
	// unset semantics.
	var priorIntent json.RawMessage
	if err := tx.QueryRow(ctx,
		`SELECT model_intent FROM core_personas WHERE persona_id = $1`, personaID).Scan(&priorIntent); err != nil {
		return Receipt{}, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE core_personas p SET model_intent = COALESCE((
			SELECT jsonb_build_object('kind', m.kind, 'connection', CASE
				WHEN m.kind = 'api' THEN (
					SELECT jsonb_build_object(
						'connection_id', c.connection_id::text, 'name', c.name,
						'preset', c.preset, 'base_url', c.base_url,
						'model', c.model, 'version', c.version::text)
					FROM model_api_connections c
					WHERE c.human_id = m.human_id AND c.connection_id = m.connection_id)
				WHEN m.kind = 'chatgpt' THEN (
					SELECT jsonb_build_object('model', g.model, 'effort', g.effort)
					FROM chatgpt_connections g WHERE g.human_id = m.human_id)
				ELSE NULL END)
			FROM model_connection_selections m
			WHERE m.human_id = p.human_id), p.model_intent)
		WHERE p.persona_id = $1`, personaID); err != nil {
		return Receipt{}, err
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
	// A 'preparing' chunk is a claim by the writer generation the seal just
	// fenced: the branch can never record an outcome, so its claim is dead
	// placement-local execution state. The cut carries the durable memory
	// — verdicts, replacement text, ranges, attempt history — but never a
	// live claim: the row returns to 'sealed' so the destination can claim
	// it under its own writer. Attempts and interruptions are judgments and
	// history, not claims; they carry unchanged.
	if _, err := tx.Exec(ctx, `
		UPDATE core_memory_chunks
		SET status = 'sealed', claimed_generation = NULL, claimed_at = NULL, not_before = NULL
		WHERE persona_id = $1 AND status = 'preparing'`, personaID); err != nil {
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
	key, err := newTransferKey()
	if err != nil {
		return Receipt{}, err
	}
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
		DestinationID: destinationID,
		SealedAt:      sealedAt.UTC(),
		Cut:           cut,
		Rows:          rows,
		Continuity:    cont,
		NotIncluded:   NotIncluded,
		UpdatedAt:     sealedAt.UTC(),
		// The intent this seal's snapshot replaced — Abort restores it so a
		// cancelled transfer returns the source to exactly its pre-transfer
		// model semantics.
		PriorModelIntent: priorIntent,
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return Receipt{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO core_transfers (direction, transfer_id, persona_id, status, format_version, destination_id, proof_key, receipt, created_at, updated_at)
		VALUES ('export', $1, $2, 'sealed', $3, $4, $5, $6, $7, $7)`,
		transferID, personaID, FormatVersion, destinationID, key, raw, sealedAt); err != nil {
		return Receipt{}, fmt.Errorf("record transfer: %w", err)
	}
	return rec, tx.Commit(ctx)
}

// Complete marks the source transferred once the destination committed
// activation. activateProof is the activate_proof from the destination's
// activate response or its import Status — a value that only exists because
// the destination committed. Without it the source stays sealed: a complete
// issued against a transfer the destination never ran can no longer strand
// the secretary on no placement. The source rows stay as history; deleting
// them is a separate decision.
func (s *Service) Complete(ctx context.Context, personaID, transferID, activateProof string) (Receipt, error) {
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
	if rec.Status == "completed" {
		return rec, tx.Commit(ctx)
	}
	if authority != "sealed" {
		return Receipt{}, fmt.Errorf("%w: persona authority is %s", ErrTransferConflict, authority)
	}
	if !proofRe.MatchString(activateProof) {
		return Receipt{}, fmt.Errorf("%w: pass the destination's activate_proof "+
			"(POST activate on the destination, or GET /transfers/import/%s there)", ErrMissingProof, transferID)
	}
	if !proofMatches(rec.key, "activate", transferID, personaID, rec.DestinationID, activateProof) {
		return Receipt{}, fmt.Errorf("%w: %s is not the activation proof for transfer %s on destination %s; "+
			"the addressed placement has not activated it (check its import status)",
			ErrTransferConflict, activateProof, transferID, rec.DestinationID)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE core_personas SET authority = 'transferred' WHERE persona_id = $1`, personaID); err != nil {
		return Receipt{}, err
	}
	rec.ActivateProof = activateProof
	if err := commit(ctx, tx, "export", transferID, "completed", &rec); err != nil {
		return Receipt{}, err
	}
	return rec, tx.Commit(ctx)
}

// Abort returns a sealed source to active so the same secretary continues
// where it was. It requires the destination's retire_proof: evidence that the
// recorded destination committed "this transfer will never run here" — the
// staged copy deleted or a tombstone recorded before the bundle ever
// arrived. A destination that activated can never produce one, and a proof
// minted by any other placement names the wrong destination, so no lost
// response or mistaken retry can leave two placements able to run the
// secretary.
//
// If the destination cannot be asked at all (unreachable, retired service),
// the source stays sealed: paused and visible, but singular. There is no
// force path — a placement that is gone for good is a deliberate product
// decision with explicit identity consequences, to be designed separately.
func (s *Service) Abort(ctx context.Context, personaID, transferID, retireProof string) (Receipt, error) {
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
	if !proofRe.MatchString(retireProof) {
		return Receipt{}, fmt.Errorf("%w: pass the destination's retire_proof "+
			"(POST retire on the destination, or GET /transfers/import/%s there)", ErrMissingProof, transferID)
	}
	if !proofMatches(rec.key, "retire", transferID, personaID, rec.DestinationID, retireProof) {
		return Receipt{}, fmt.Errorf("%w: %s is not the retire proof for transfer %s on destination %s; "+
			"the addressed placement has not retired it (check its import status)",
			ErrTransferConflict, retireProof, transferID, rec.DestinationID)
	}
	// The seal-time intent snapshot is a transfer artifact and dies with the
	// transfer: restore the intent the seal recorded as replaced. For a
	// persona that never moved that is NULL — the live selection drives the
	// binding again, so an ordinary model-kind change after the abort is not
	// stranded in needs_rebinding. For a persona that itself arrived by
	// transfer, a still-unresolved incoming intent is restored rather than
	// silently erased.
	var intentRestore *string
	if len(rec.PriorModelIntent) > 0 {
		s := string(rec.PriorModelIntent)
		intentRestore = &s
	}
	if _, err := tx.Exec(ctx,
		`UPDATE core_personas SET authority = 'active', transfer_id = NULL, model_intent = $2::jsonb WHERE persona_id = $1`,
		personaID, intentRestore); err != nil {
		return Receipt{}, err
	}
	// Expire the parked lease so the next writer acquires generation+1.
	// A fixed past instant stays dead under any clock — a now()-written
	// expiry can look live to a later transaction after a backward
	// host-clock step.
	if _, err := tx.Exec(ctx,
		`UPDATE core_writer_leases SET expires_at = 'epoch'::timestamptz WHERE persona_id = $1`, personaID); err != nil {
		return Receipt{}, err
	}
	rec.RetireProof = retireProof
	if err := commit(ctx, tx, "export", transferID, "aborted", &rec); err != nil {
		return Receipt{}, err
	}
	return rec, tx.Commit(ctx)
}

// Activate makes a staged import the authoritative placement and produces the
// activation proof the source's Complete requires. From commit on, a writer
// can be acquired and inputs accepted here. The first writer acquires a
// generation above the source's epoch and runs ordinary recovery, which
// interrupts carried running turns and requeues their inputs.
func (s *Service) Activate(ctx context.Context, personaID, transferID string) (Receipt, error) {
	if err := validateIDs(personaID, transferID); err != nil {
		return Receipt{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Receipt{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := transferLock(ctx, tx, transferID); err != nil {
		return Receipt{}, err
	}
	// The ledger decides before the persona row does: after a retire the
	// persona is gone, and the tombstone — not a 404 — must answer a late
	// activate so the source can tell "retired" from "never arrived".
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
	if rec.Status != "staged" {
		return Receipt{}, fmt.Errorf("%w: transfer %s is %s; it cannot be activated", ErrTransferConflict, transferID, rec.Status)
	}
	authority, held, err := lockPersona(ctx, tx, personaID)
	if err != nil {
		return Receipt{}, err
	}
	if authority != "staged" || !heldBy(held, transferID) {
		return Receipt{}, fmt.Errorf("%w: persona authority is %s", ErrTransferConflict, authority)
	}
	// An unbound persona may activate only while nothing parked needs a
	// human decider: an approval is an identity-scoped act and no one may
	// decide it while human_id is NULL, so activating with a pending
	// approval would strand its waiting input forever. Binding stays
	// reachable — POST /internal/core/personas/{id}/bind works on a staged
	// persona — and retiring the transfer is always allowed.
	var humanID *string
	if err := tx.QueryRow(ctx,
		`SELECT human_id FROM core_personas WHERE persona_id = $1`, personaID).Scan(&humanID); err != nil {
		return Receipt{}, err
	}
	if humanID == nil {
		var pending int64
		if err := tx.QueryRow(ctx, `
			SELECT count(*) FROM core_tool_approvals
			WHERE persona_id = $1 AND status = 'pending'`, personaID).Scan(&pending); err != nil {
			return Receipt{}, err
		}
		if pending > 0 {
			return Receipt{}, fmt.Errorf("%w: persona is not bound to a human and %d pending approval(s) require a decision only a bound human can make; bind a human (POST /internal/core/personas/%s/bind) before activating, or retire the transfer",
				ErrTransferConflict, pending, personaID)
		}
	}
	// The proof names this placement's own id — which is the ledger's
	// destination_id for every import — so it can only ever verify as the
	// addressed destination's evidence.
	own, err := s.PlacementID(ctx)
	if err != nil {
		return Receipt{}, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE core_personas SET authority = 'active' WHERE persona_id = $1`, personaID); err != nil {
		return Receipt{}, err
	}
	rec.ActivateProof = transferProof(rec.key, "activate", transferID, personaID, own)
	if err := commit(ctx, tx, "import", transferID, "activated", &rec); err != nil {
		return Receipt{}, err
	}
	return rec, tx.Commit(ctx)
}

// Retire records on the destination that this transfer will never run here:
// a staged import's persona is deleted, or — when the bundle never arrived —
// a tombstone is written so a late import is refused. Either way it produces
// the retire_proof the source's Abort requires, computed over this
// placement's own id — so a retire dispatched to the wrong service is both
// refused here (destination_id must be this placement) and useless there
// (its proof names the wrong placement). Retire is refused once the transfer
// has activated; the ledger row lock makes retire and activate commit in
// some order, never both.
//
// destinationID is always required — the caller knows it from the bundle
// header — and must equal this placement's id. transferKey is required only
// for the tombstone case: a placement that never imported the bundle has not
// seen the key, so the caller passes it from the bundle header.
//
// A tombstone is the one ledger record a caller writes from unverifiable
// parameters, so it stays correctable: while no bundle was imported
// (content_sha256 is empty), a re-retire with a different persona_id or
// transfer_key rewrites the record and re-mints the proof. Correction can
// never lift the foreclosure — status stays retired, so no late import or
// activate can ever proceed — and only a record naming the real key, persona
// and destination produces a proof the source accepts. An imported-then-
// retired row is never rewritten: its parameters were verified at import.
func (s *Service) Retire(ctx context.Context, personaID, transferID, destinationID, transferKey string) (Receipt, error) {
	if err := validateIDs(personaID, transferID); err != nil {
		return Receipt{}, err
	}
	if !uuidv7Re.MatchString(destinationID) {
		return Receipt{}, fmt.Errorf("%w: destination_id must be this placement's uuidv7", ErrBadRequest)
	}
	if transferKey != "" && !proofRe.MatchString(transferKey) {
		return Receipt{}, fmt.Errorf("%w: transfer_key must be the 64-hex key from the bundle header", ErrBadRequest)
	}
	own, err := s.PlacementID(ctx)
	if err != nil {
		return Receipt{}, err
	}
	if destinationID != own {
		return Receipt{}, fmt.Errorf("%w: retire was addressed to placement %s; this placement is %s",
			ErrBadRequest, destinationID, own)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Receipt{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := transferLock(ctx, tx, transferID); err != nil {
		return Receipt{}, err
	}
	rec, err := ledger(ctx, tx, "import", transferID, true)
	switch {
	case err == nil && rec.Status == "retired" && rec.ContentSHA256 == "":
		// A bare tombstone: nothing was ever imported, so its persona_id and
		// key are caller-supplied and may be wrong. Every touch must present
		// the key; replay on identical parameters, rewrite — never
		// un-retire — on different ones.
		if transferKey == "" {
			return Receipt{}, fmt.Errorf("%w: the tombstone for transfer %s was recorded under a "+
				"transfer_key; pass transfer_key from the bundle header", ErrMissingProof, transferID)
		}
		if rec.PersonaID == personaID && transferKey == rec.key {
			return rec, tx.Commit(ctx)
		}
		if err := s.refuseOwnSource(ctx, tx, personaID, transferID); err != nil {
			return Receipt{}, err
		}
		rec.PersonaID = personaID
		rec.key = transferKey
		rec.RetireProof = transferProof(transferKey, "retire", transferID, personaID, own)
		raw, merr := json.Marshal(rec)
		if merr != nil {
			return Receipt{}, merr
		}
		var updated time.Time
		if err := tx.QueryRow(ctx, `
			UPDATE core_transfers SET persona_id = $2, proof_key = $3, receipt = $4, updated_at = now()
			WHERE direction = 'import' AND transfer_id = $1 AND status = 'retired' AND content_sha256 IS NULL
			RETURNING updated_at`, transferID, personaID, transferKey, raw).
			Scan(&updated); err != nil {
			return Receipt{}, fmt.Errorf("correct transfer tombstone: %w", err)
		}
		rec.UpdatedAt = updated.UTC()
		return rec, tx.Commit(ctx)
	case err == nil:
		if rec.PersonaID != personaID {
			return Receipt{}, fmt.Errorf("%w: transfer %s belongs to another persona", ErrTransferConflict, transferID)
		}
		if rec.Status == "retired" {
			// Imported-then-retired: its parameters were verified at import.
			// Replay on them; a different key is a mismatch, not a rewrite.
			if transferKey != "" && transferKey != rec.key {
				return Receipt{}, fmt.Errorf("%w: transfer %s was retired under a different transfer_key",
					ErrTransferConflict, transferID)
			}
			return rec, tx.Commit(ctx)
		}
		if rec.Status != "staged" {
			return Receipt{}, fmt.Errorf("%w: an %s transfer cannot be retired", ErrTransferConflict, rec.Status)
		}
		authority, held, lerr := lockPersona(ctx, tx, personaID)
		if lerr != nil {
			return Receipt{}, lerr
		}
		if authority != "staged" || !heldBy(held, transferID) {
			return Receipt{}, fmt.Errorf("%w: persona authority is %s", ErrTransferConflict, authority)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM core_personas WHERE persona_id = $1`, personaID); err != nil {
			return Receipt{}, fmt.Errorf("retire staged persona: %w", err)
		}
		rec.RetireProof = transferProof(rec.key, "retire", transferID, personaID, own)
		if err := commit(ctx, tx, "import", transferID, "retired", &rec); err != nil {
			return Receipt{}, err
		}
		return rec, tx.Commit(ctx)
	case !errors.Is(err, ErrTransferNotFound):
		return Receipt{}, err
	}

	// Tombstone: the bundle never arrived here. Refuse to retire the
	// transfer's own source — that would let a mistaken call to this
	// service produce a proof the source itself accepts while a copy
	// elsewhere stays activatable.
	if err := s.refuseOwnSource(ctx, tx, personaID, transferID); err != nil {
		return Receipt{}, err
	}
	if transferKey == "" {
		return Receipt{}, fmt.Errorf("%w: transfer %s was never imported here; "+
			"pass transfer_key from the bundle header", ErrMissingProof, transferID)
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
		return Receipt{}, err
	}
	rec = Receipt{
		Direction:     "import",
		TransferID:    transferID,
		PersonaID:     personaID,
		Status:        "retired",
		FormatVersion: FormatVersion,
		DestinationID: own,
		RetireProof:   transferProof(transferKey, "retire", transferID, personaID, own),
		NotIncluded:   NotIncluded,
		UpdatedAt:     now.UTC(),
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return Receipt{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO core_transfers (direction, transfer_id, persona_id, status, format_version, destination_id, proof_key, receipt, created_at, updated_at)
		VALUES ('import', $1, $2, 'retired', $3, $4, $5, $6, $7, $7)`,
		transferID, personaID, FormatVersion, own, transferKey, raw, now); err != nil {
		return Receipt{}, fmt.Errorf("record transfer tombstone: %w", err)
	}
	return rec, tx.Commit(ctx)
}

// refuseOwnSource refuses to tombstone a transfer whose source is this
// placement: the persona here holds the transfer in sealed or transferred
// authority, and a local tombstone could mint a proof the source accepts
// while a copy elsewhere stays activatable.
func (s *Service) refuseOwnSource(ctx context.Context, tx pgx.Tx, personaID, transferID string) error {
	var authority string
	var held *string
	err := tx.QueryRow(ctx,
		`SELECT authority, transfer_id FROM core_personas WHERE persona_id = $1 FOR NO KEY UPDATE`,
		personaID).Scan(&authority, &held)
	switch {
	case err == nil:
		if heldBy(held, transferID) && (authority == "sealed" || authority == "transferred") {
			return fmt.Errorf("%w: persona %s is this placement's own %s transfer %s",
				ErrTransferConflict, personaID, authority, transferID)
		}
		return nil
	case errors.Is(err, pgx.ErrNoRows):
		return nil
	default:
		return err
	}
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
			(SELECT count(*) FROM core_inputs WHERE persona_id = $1 AND status = 'waiting'),
			(SELECT count(*) FROM core_turns WHERE persona_id = $1 AND status = 'running'),
			(SELECT count(*) FROM core_turn_plans p JOIN core_inputs i
				ON i.persona_id = p.persona_id AND i.input_id = p.input_id
				WHERE p.persona_id = $1 AND i.status <> 'done'),
			(SELECT count(*) FROM core_tool_approvals WHERE persona_id = $1 AND status = 'pending'),
			(SELECT count(*) FROM core_schedules WHERE persona_id = $1 AND status IN ('pending', 'claimed')),
			(SELECT count(*) FROM core_outbox WHERE persona_id = $1 AND delivered_at IS NULL),
			(SELECT COALESCE(max(seq), 0) FROM core_events WHERE persona_id = $1),
			(SELECT COALESCE(max(seq), 0) FROM core_outbox WHERE persona_id = $1),
			(SELECT count(*) FROM core_memory_chunks WHERE persona_id = $1 AND status = 'applied'),
			(SELECT count(*) FROM core_memory_chunks WHERE persona_id = $1 AND status = 'prepared'),
			(SELECT count(*) FROM core_memory_chunks WHERE persona_id = $1 AND status = 'sealed'),
			(SELECT count(*) FROM core_memory_chunks WHERE persona_id = $1 AND status = 'kept'),
			(SELECT count(*) FROM core_memory_chunks WHERE persona_id = $1 AND status = 'failed')`,
		personaID).Scan(&cont.JournalEvents, &cont.Notes, &cont.QueuedInputs, &cont.ClaimedInputs,
		&cont.WaitingInputs, &cont.RunningTurns, &cont.UnfinishedPlans, &cont.PendingApprovals,
		&cont.PendingSchedules, &cont.UndeliveredOut,
		&cut.LatestEventSeq, &cut.LatestOutboxSeq,
		&cont.MemoryApplied, &cont.MemoryPrepared, &cont.MemorySealed, &cont.MemoryKept, &cont.MemoryFailed)
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
