package agentstate

// Secretary-independent background jobs (M09). A job is a persona-scoped
// execution record whose lifecycle is owned by a *runner claim*, not by the
// writer lease: the writer fence must not revoke a legitimate running job's
// completion authority (engineering plan §5), so none of these calls take a
// writer generation. Authorization is the persona capability token plus, for
// claim/heartbeat/complete, the recorded runner identity.
//
// Lifecycle: queued → running → done|failed|cancelled, with cancel_requested
// as the running→cancelled transit state and 'lost' for an expired claim
// (indeterminate outcome — never silently re-executed). Every terminal
// transition enqueues exactly one 'job:<job_id>' notification input in the
// same transaction, so the secretary learns the outcome through its ordinary
// input stream after any restart, exactly once.

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	ErrJobNotFound   = errors.New("job not found")
	ErrJobConflict   = errors.New("conflicting job state")
	ErrJobNotClaimed = errors.New("job is not claimed by this runner")
)

// jobInputPrefix is reserved for job terminal notifications; a caller-
// supplied input_id in that namespace could pre-occupy a job's notification
// slot and silently drop the result. (Same rule as schedInputPrefix.)
const jobInputPrefix = "job:"

// jobToolPrefix is reserved for server-derived job ids authored by the
// job.start tool effect; a caller-supplied job_id in that namespace could
// collide with a plan-position-derived id.
const jobToolPrefix = "op:"

type Job struct {
	PersonaID         string         `json:"persona_id"`
	JobID             string         `json:"job_id"`
	Kind              string         `json:"kind"`
	Request           map[string]any `json:"request"`
	Status            string         `json:"status"`
	ClaimedBy         *string        `json:"claimed_by"`
	ClaimExpiresAt    *time.Time     `json:"claim_expires_at"`
	CreatedBy         string         `json:"created_by"`
	CreatedAt         time.Time      `json:"created_at"`
	StartedAt         *time.Time     `json:"started_at"`
	FinishedAt        *time.Time     `json:"finished_at"`
	CancelRequestedAt *time.Time     `json:"cancel_requested_at"`
	Result            map[string]any `json:"result"`
	Error             *string        `json:"error"`
	NotifiedAt        *time.Time     `json:"notified_at"`
}

// jobTerminal is the set of statuses a job cannot leave once reached.
func jobTerminal(status string) bool {
	switch status {
	case "done", "failed", "cancelled", "lost":
		return true
	}
	return false
}

const jobCols = `persona_id, job_id, kind, request, status, claimed_by,
	claim_expires_at, created_by, created_at, started_at, finished_at,
	cancel_requested_at, result, error, notified_at`

func scanJob(row inputScanner) (Job, error) {
	var j Job
	err := row.Scan(&j.PersonaID, &j.JobID, &j.Kind, &j.Request, &j.Status,
		&j.ClaimedBy, &j.ClaimExpiresAt, &j.CreatedBy, &j.CreatedAt,
		&j.StartedAt, &j.FinishedAt, &j.CancelRequestedAt, &j.Result,
		&j.Error, &j.NotifiedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return j, ErrJobNotFound
	}
	return j, err
}

// validateJobRequest enforces the per-kind request shape at the persistence
// boundary, so a malformed spec is a deterministic 400 — never a queued job
// no runner can execute.
func validateJobRequest(kind string, request map[string]any) error {
	switch kind {
	case "subprocess":
		rawCmd, ok := request["command"].([]any)
		if !ok || len(rawCmd) == 0 {
			return fmt.Errorf("%w: subprocess job requires a non-empty command array", ErrBadRequest)
		}
		for i, arg := range rawCmd {
			s, ok := arg.(string)
			if !ok || s == "" {
				return fmt.Errorf("%w: subprocess command[%d] must be a non-empty string", ErrBadRequest, i)
			}
		}
		if cwd, ok := request["cwd"]; ok {
			if _, ok := cwd.(string); !ok {
				return fmt.Errorf("%w: subprocess cwd must be a string", ErrBadRequest)
			}
		}
		if t, ok := request["timeout_ms"]; ok {
			ms, ok := t.(float64)
			if !ok || ms <= 0 || ms > 3_600_000 || ms != float64(int64(ms)) {
				return fmt.Errorf("%w: subprocess timeout_ms must be an integer in (0, 3600000]", ErrBadRequest)
			}
		}
		if rawEnv, ok := request["env"]; ok {
			env, ok := rawEnv.(map[string]any)
			if !ok {
				return fmt.Errorf("%w: subprocess env must be an object of strings", ErrBadRequest)
			}
			for k, v := range env {
				if _, ok := v.(string); !ok {
					return fmt.Errorf("%w: subprocess env[%q] must be a string", ErrBadRequest, k)
				}
			}
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown job kind %q", ErrBadRequest, kind)
	}
}

func validJobID(jobID string) error {
	if jobID == "" || len(jobID) > 256 {
		return fmt.Errorf("%w: job_id must be 1-256 characters", ErrBadRequest)
	}
	if strings.HasPrefix(jobID, jobToolPrefix) {
		return fmt.Errorf("%w: job_id prefix %q is reserved", ErrBadRequest, jobToolPrefix)
	}
	if jobID == "claim" {
		// POST .../jobs/claim is the claim route; a job literally named
		// "claim" could never be addressed by the job-scoped routes.
		return fmt.Errorf("%w: job_id %q is reserved", ErrBadRequest, "claim")
	}
	return nil
}

// SubmitJob durably records a job for later claiming. Re-submitting the same
// job_id replays the stored row — the lost-response path. A replay whose
// kind or request differs is a contract violation (409), not idempotency.
func (s *Store) SubmitJob(ctx context.Context, personaID, jobID, kind string, request map[string]any, createdBy string) (Job, bool, error) {
	if err := validJobID(jobID); err != nil {
		return Job{}, false, err
	}
	if request == nil {
		request = map[string]any{}
	}
	if err := validateJobRequest(kind, request); err != nil {
		return Job{}, false, err
	}
	if hasNUL(request) {
		return Job{}, false, fmt.Errorf("%w: job request contains a NUL byte jsonb cannot store", ErrBadRequest)
	}
	if createdBy == "" {
		createdBy = "api"
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Job{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	j, created, err := s.submitJobTx(ctx, tx, personaID, jobID, kind, request, createdBy)
	if err != nil {
		return Job{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, false, err
	}
	return j, created, nil
}

// submitJobTx is the insert-or-replay core shared by the API route and the
// job.start tool effect (which runs inside the operation claim transaction).
func (s *Store) submitJobTx(ctx context.Context, tx pgx.Tx, personaID, jobID, kind string, request map[string]any, createdBy string) (Job, bool, error) {
	var j Job
	err := tx.QueryRow(ctx, `
		INSERT INTO core_jobs (persona_id, job_id, kind, request, status, created_by)
		SELECT $1::uuidv7, $2, $3, $4, 'queued', $5
		FROM core_personas WHERE persona_id = $1::uuidv7
		ON CONFLICT (persona_id, job_id) DO NOTHING
		RETURNING `+jobCols,
		personaID, jobID, kind, request, createdBy).
		Scan(&j.PersonaID, &j.JobID, &j.Kind, &j.Request, &j.Status,
			&j.ClaimedBy, &j.ClaimExpiresAt, &j.CreatedBy, &j.CreatedAt,
			&j.StartedAt, &j.FinishedAt, &j.CancelRequestedAt, &j.Result,
			&j.Error, &j.NotifiedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		var exists bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM core_personas WHERE persona_id = $1)`,
			personaID).Scan(&exists); err != nil {
			return Job{}, false, err
		}
		if !exists {
			return Job{}, false, ErrPersonaNotFound
		}
		// Replay is only valid when every caller-supplied field matches the
		// stored request — an idempotent retry, not a different job.
		var same bool
		if err := tx.QueryRow(ctx, `
			SELECT kind = $3 AND request = $4::jsonb
			FROM core_jobs WHERE persona_id = $1 AND job_id = $2`,
			personaID, jobID, kind, request).Scan(&same); err != nil {
			return Job{}, false, err
		}
		if !same {
			return Job{}, false, fmt.Errorf("%w: job_id replay carries a different request", ErrJobConflict)
		}
		stored, err := scanJob(tx.QueryRow(ctx,
			`SELECT `+jobCols+` FROM core_jobs WHERE persona_id = $1 AND job_id = $2`,
			personaID, jobID))
		if err != nil {
			return Job{}, false, err
		}
		return stored, false, nil
	}
	if err != nil {
		return Job{}, false, fmt.Errorf("submit job: %w", dataErr(err))
	}
	return j, true, nil
}

func (s *Store) GetJob(ctx context.Context, personaID, jobID string) (Job, error) {
	return scanJob(s.pool.QueryRow(ctx,
		`SELECT `+jobCols+` FROM core_jobs WHERE persona_id = $1 AND job_id = $2`,
		personaID, jobID))
}

// ListJobs returns jobs newest-first. An empty status filter lists all;
// otherwise only jobs in one of the listed statuses are returned.
func (s *Store) ListJobs(ctx context.Context, personaID string, statuses []string, limit int) ([]Job, error) {
	limit = clampLimit(limit, 50, 500)
	rows, err := s.pool.Query(ctx, `
		SELECT `+jobCols+` FROM core_jobs
		WHERE persona_id = $1 AND (COALESCE(cardinality($2::text[]), 0) = 0 OR status = ANY($2::text[]))
		ORDER BY created_at DESC, job_id DESC LIMIT $3`,
		personaID, statuses, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// jobNotificationText is the one-line, model-readable summary carried in the
// notification input's payload.text (the journal/context assembly reads it).
func jobNotificationText(j *Job) string {
	var b strings.Builder
	b.WriteString("job " + j.JobID + " (" + j.Kind + ") " + j.Status)
	if j.Result != nil {
		if code, ok := j.Result["exit_code"]; ok {
			b.WriteString(", exit " + fmt.Sprint(code))
		}
	}
	if j.Error != nil && *j.Error != "" {
		b.WriteString(": " + *j.Error)
	}
	return b.String()
}

// notifyJobTerminalTx enqueues the job's terminal notification input exactly
// once (id 'job:<job_id>', ON CONFLICT DO NOTHING) and stamps notified_at.
// Callers hold the job's row lock inside the terminal transition's tx.
func (s *Store) notifyJobTerminalTx(ctx context.Context, tx pgx.Tx, j *Job) error {
	payload := map[string]any{
		"job_id": j.JobID,
		"kind":   j.Kind,
		"status": j.Status,
		"text":   jobNotificationText(j),
	}
	if j.Error != nil && *j.Error != "" {
		payload["error"] = *j.Error
	}
	if j.Result != nil {
		if code, ok := j.Result["exit_code"]; ok {
			payload["exit_code"] = code
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO core_inputs (persona_id, input_id, kind, payload, actor_kind, actor_id, source_surface, attention, status)
		VALUES ($1::uuidv7, $2, 'job_completed', $3, 'job', $4, 'core_jobs', 'reply', 'queued')
		ON CONFLICT (persona_id, input_id) DO NOTHING`,
		j.PersonaID, jobInputPrefix+j.JobID, payload, j.JobID); err != nil {
		return fmt.Errorf("notify job: %w", dataErr(err))
	}
	if _, err := tx.Exec(ctx,
		`UPDATE core_jobs SET notified_at = now() WHERE persona_id = $1 AND job_id = $2`,
		j.PersonaID, j.JobID); err != nil {
		return err
	}
	now := time.Now()
	j.NotifiedAt = &now
	return nil
}

// CancelJob: queued → cancelled (never ran, notification queued); running →
// cancel_requested (the owning runner sees it via heartbeat/poll and stops the
// execution); terminal or already-cancel-requested → idempotent replay of the
// stored row. Cancel is deliberately not writer-gated: stopping a job must
// work while the secretary is down.
func (s *Store) CancelJob(ctx context.Context, personaID, jobID string) (Job, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Job{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	j, err := s.cancelJobTx(ctx, tx, personaID, jobID)
	if err != nil {
		return Job{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, err
	}
	return j, nil
}

func (s *Store) cancelJobTx(ctx context.Context, tx pgx.Tx, personaID, jobID string) (Job, error) {
	j, err := scanJob(tx.QueryRow(ctx,
		`SELECT `+jobCols+` FROM core_jobs WHERE persona_id = $1 AND job_id = $2 FOR UPDATE`,
		personaID, jobID))
	if err != nil {
		return j, err
	}
	switch j.Status {
	case "queued":
		// Never claimed: terminal cancel, honestly recorded as not-run.
		j.Status = "cancelled"
		now := time.Now()
		j.FinishedAt = &now
		j.CancelRequestedAt = &now
		if _, err := tx.Exec(ctx, `
			UPDATE core_jobs SET status = 'cancelled', cancel_requested_at = now(), finished_at = now()
			WHERE persona_id = $1 AND job_id = $2`, personaID, jobID); err != nil {
			return Job{}, err
		}
		if err := s.notifyJobTerminalTx(ctx, tx, &j); err != nil {
			return Job{}, err
		}
		return j, nil
	case "running":
		if _, err := tx.Exec(ctx, `
			UPDATE core_jobs SET status = 'cancel_requested', cancel_requested_at = now()
			WHERE persona_id = $1 AND job_id = $2`, personaID, jobID); err != nil {
			return Job{}, err
		}
		j.Status = "cancel_requested"
		now := time.Now()
		j.CancelRequestedAt = &now
		return j, nil
	default:
		// cancel_requested or terminal: replay the stored row.
		return j, nil
	}
}

// ClaimJobs is the runner's one periodic call: it first sweeps live jobs whose
// claim expired (→ 'lost' + notification — indeterminate, never re-run), then
// claims up to limit queued jobs of the requested kinds for this runner.
func (s *Store) ClaimJobs(ctx context.Context, personaID, runnerID string, kinds []string, lease time.Duration, limit int) ([]Job, []Job, error) {
	if runnerID == "" || len(runnerID) > 256 {
		return nil, nil, fmt.Errorf("%w: runner_id must be 1-256 characters", ErrBadRequest)
	}
	if len(kinds) == 0 {
		return nil, nil, fmt.Errorf("%w: kinds required", ErrBadRequest)
	}
	if lease <= 0 {
		return nil, nil, fmt.Errorf("%w: positive lease_ms required", ErrBadRequest)
	}
	limit = clampLimit(limit, 1, 32)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Sweep: a running job whose claim expired has an indeterminate outcome —
	// the process may still be running orphaned. Mark it 'lost' and notify;
	// it is never re-queued for re-execution.
	rows, err := tx.Query(ctx, `
		UPDATE core_jobs SET status = 'lost', finished_at = now(),
			error = 'runner claim expired; outcome is indeterminate',
			result = COALESCE(result, '{}'::jsonb) || '{"reason":"claim_expired"}'::jsonb
		WHERE persona_id = $1 AND status IN ('running','cancel_requested')
			AND claim_expires_at < now()
		RETURNING `+jobCols, personaID)
	if err != nil {
		return nil, nil, err
	}
	swept := []Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			rows.Close()
			return nil, nil, err
		}
		swept = append(swept, j)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	for i := range swept {
		if err := s.notifyJobTerminalTx(ctx, tx, &swept[i]); err != nil {
			return nil, nil, err
		}
	}

	rows, err = tx.Query(ctx, `
		UPDATE core_jobs SET status = 'running', claimed_by = $2,
			claim_expires_at = now() + $4::interval,
			started_at = COALESCE(started_at, now())
		WHERE (persona_id, job_id) IN (
			SELECT persona_id, job_id FROM core_jobs
			WHERE persona_id = $1 AND status = 'queued' AND kind = ANY($3::text[])
			ORDER BY created_at, job_id LIMIT $5 FOR UPDATE SKIP LOCKED
		)
		RETURNING `+jobCols,
		personaID, runnerID, kinds, lease, limit)
	if err != nil {
		return nil, nil, err
	}
	claimed := []Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			rows.Close()
			return nil, nil, err
		}
		claimed = append(claimed, j)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, err
	}
	return claimed, swept, nil
}

// HeartbeatJob extends the runner's claim and returns the current row so the
// runner observes cancel_requested. If the job is no longer owned by this
// runner (terminal, lost, or claimed away), the stored row is returned with
// ErrJobNotClaimed so the caller can stop the execution it still holds.
func (s *Store) HeartbeatJob(ctx context.Context, personaID, jobID, runnerID string, lease time.Duration) (Job, error) {
	if lease <= 0 {
		return Job{}, fmt.Errorf("%w: positive lease_ms required", ErrBadRequest)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Job{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	j, err := scanJob(tx.QueryRow(ctx,
		`SELECT `+jobCols+` FROM core_jobs WHERE persona_id = $1 AND job_id = $2 FOR UPDATE`,
		personaID, jobID))
	if err != nil {
		return j, err
	}
	if j.ClaimedBy == nil || *j.ClaimedBy != runnerID || jobTerminal(j.Status) {
		return j, ErrJobNotClaimed
	}
	if _, err := tx.Exec(ctx, `
		UPDATE core_jobs SET claim_expires_at = now() + $4::interval
		WHERE persona_id = $1 AND job_id = $2 AND claimed_by = $3`,
		personaID, jobID, runnerID, lease); err != nil {
		return Job{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, err
	}
	return j, nil
}

// CompleteJob records the runner-observed terminal outcome and enqueues the
// notification input in one transaction — the result is durable before it is
// observable. Only the claiming runner may complete; a replay with an
// identical outcome returns the stored row (lost response), a divergent one
// conflicts.
func (s *Store) CompleteJob(ctx context.Context, personaID, jobID, runnerID, status string, result map[string]any, jobError string) (Job, error) {
	switch status {
	case "done", "failed", "cancelled":
	default:
		return Job{}, fmt.Errorf("%w: complete status must be done, failed, or cancelled", ErrBadRequest)
	}
	if result == nil {
		result = map[string]any{}
	}
	if hasNUL(result) {
		return Job{}, fmt.Errorf("%w: job result contains a NUL byte jsonb cannot store", ErrBadRequest)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Job{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	j, err := scanJob(tx.QueryRow(ctx,
		`SELECT `+jobCols+` FROM core_jobs WHERE persona_id = $1 AND job_id = $2 FOR UPDATE`,
		personaID, jobID))
	if err != nil {
		return j, err
	}
	if jobTerminal(j.Status) {
		// Idempotent replay after a lost response: the stored record is the
		// answer. A divergent terminal report is a contract violation.
		same := j.Status == status && jsonbEqual(j.Result, result)
		storedErr := ""
		if j.Error != nil {
			storedErr = *j.Error
		}
		same = same && storedErr == jobError
		if !same {
			return j, fmt.Errorf("%w: job already finished as %s", ErrJobConflict, j.Status)
		}
		if err := tx.Commit(ctx); err != nil {
			return Job{}, err
		}
		return j, nil
	}
	if j.ClaimedBy == nil || *j.ClaimedBy != runnerID {
		return j, ErrJobNotClaimed
	}
	// running or cancel_requested: the claiming runner reports what it
	// actually observed. A job that exited before the cancel landed records
	// its real outcome while keeping cancel_requested_at as evidence.
	var errCol *string
	if jobError != "" {
		errCol = &jobError
	}
	err = tx.QueryRow(ctx, `
		UPDATE core_jobs SET status = $4, result = $5, error = $6, finished_at = now()
		WHERE persona_id = $1 AND job_id = $2 AND claimed_by = $3
		RETURNING `+jobCols,
		personaID, jobID, runnerID, status, result, errCol).
		Scan(&j.PersonaID, &j.JobID, &j.Kind, &j.Request, &j.Status,
			&j.ClaimedBy, &j.ClaimExpiresAt, &j.CreatedBy, &j.CreatedAt,
			&j.StartedAt, &j.FinishedAt, &j.CancelRequestedAt, &j.Result,
			&j.Error, &j.NotifiedAt)
	if err != nil {
		return Job{}, fmt.Errorf("complete job: %w", dataErr(err))
	}
	if err := s.notifyJobTerminalTx(ctx, tx, &j); err != nil {
		return Job{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, err
	}
	return j, nil
}

// internalJobTool runs the job tools' state-internal effects inside the
// operation claim transaction. job.start's job_id is derived from the plan
// position (op:<input_id>:<call_index>) so a replayed claim can never mint a
// second job and the model can never choose an id that collides with another
// input's job.
func (s *Store) internalJobTool(ctx context.Context, tx pgx.Tx, personaID, turnID, inputID, tool string, callIndex int, request map[string]any) (map[string]any, error) {
	switch tool {
	case "job.start":
		// The tool's request IS the subprocess spec; kind is fixed so the
		// model cannot assert a job family this slice has no runner for.
		if err := validateJobRequest("subprocess", request); err != nil {
			return nil, err
		}
		if hasNUL(request) {
			return nil, fmt.Errorf("%w: job.start request contains a NUL byte jsonb cannot store", ErrBadRequest)
		}
		jobID := jobToolPrefix + inputID + ":" + strconv.Itoa(callIndex)
		j, _, err := s.submitJobTx(ctx, tx, personaID, jobID, "subprocess", request,
			"tool:"+turnID+":"+strconv.Itoa(callIndex))
		if err != nil {
			return nil, err
		}
		return map[string]any{"job": j}, nil
	case "job.status":
		jobID, _ := request["job_id"].(string)
		if jobID == "" {
			return nil, fmt.Errorf("%w: job.status requires job_id", ErrBadRequest)
		}
		j, err := scanJob(tx.QueryRow(ctx,
			`SELECT `+jobCols+` FROM core_jobs WHERE persona_id = $1 AND job_id = $2`,
			personaID, jobID))
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadRequest, err)
		}
		return map[string]any{"job": j}, nil
	case "job.cancel":
		jobID, _ := request["job_id"].(string)
		if jobID == "" {
			return nil, fmt.Errorf("%w: job.cancel requires job_id", ErrBadRequest)
		}
		j, err := s.cancelJobTx(ctx, tx, personaID, jobID)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadRequest, err)
		}
		return map[string]any{"job": j}, nil
	}
	return nil, nil
}
