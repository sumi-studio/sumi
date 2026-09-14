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
	if strings.ContainsRune(jobID, 0) {
		return fmt.Errorf("%w: job_id contains a NUL byte text cannot store", ErrBadRequest)
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
	// Share-lock the persona row, the same rule SubmitInput follows: a
	// transfer seal holds it FOR NO KEY UPDATE while it checks for in-flight
	// jobs, so a submit either lands before the seal's check (and the seal
	// then refuses) or sees the sealed authority and is refused — a queued
	// job can never slip between the check and the commit. Replays of an
	// existing job stay answerable in every authority, like input receipts.
	var authority string
	err := tx.QueryRow(ctx,
		`SELECT authority FROM core_personas WHERE persona_id = $1 FOR SHARE`,
		personaID).Scan(&authority)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, ErrPersonaNotFound
	}
	if err != nil {
		return Job{}, false, dataErr(err)
	}
	var j Job
	err = pgx.ErrNoRows
	if authority == "active" {
		err = tx.QueryRow(ctx, `
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
	}
	if errors.Is(err, pgx.ErrNoRows) {
		// Replay is only valid when every caller-supplied field matches the
		// stored request — an idempotent retry, not a different job.
		var same bool
		if err := tx.QueryRow(ctx, `
			SELECT kind = $3 AND request = $4::jsonb
			FROM core_jobs WHERE persona_id = $1 AND job_id = $2`,
			personaID, jobID, kind, request).Scan(&same); err == nil {
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
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return Job{}, false, err
		}
		return Job{}, false, fmt.Errorf("%w: authority is %s", ErrPersonaInactive, authority)
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

// Notification context bounds, in code points: enough to recognise the job
// and its request, never the full request (job.status has the command).
const (
	jobNoteCommandRunes = 120
	jobNoteRequestRunes = 200
)

// boundRunes cuts s to at most n code points — never inside a UTF-8
// sequence — and marks the cut.
func boundRunes(s string, n int) string {
	i := 0
	for pos := range s {
		if i == n {
			return s[:pos] + "…"
		}
		i++
	}
	return s
}

// jobCommandSummary renders the job's command as one bounded line.
func jobCommandSummary(j *Job) string {
	cmd, _ := j.Request["command"].([]any)
	if len(cmd) == 0 {
		return ""
	}
	parts := make([]string, 0, len(cmd))
	for _, c := range cmd {
		parts = append(parts, fmt.Sprint(c))
	}
	return boundRunes(strings.Join(parts, " "), jobNoteCommandRunes)
}

// jobOrigin is the request a tool-minted job was started for.
type jobOrigin struct {
	InputID string
	Request string
	// InProgress: the origin request had not finished when the job ended.
	InProgress bool
}

// jobOriginTx resolves a tool-minted job (job_id op:<input_id>:<call_index>)
// to its origin request. The notification can be handled before that
// request's journal exists — a later model round of the same turn may still
// be backing off — so it must explain itself. API-submitted jobs have no
// origin input and return a zero value.
func jobOriginTx(ctx context.Context, tx pgx.Tx, j *Job) (jobOrigin, error) {
	if !strings.HasPrefix(j.CreatedBy, "tool:") {
		return jobOrigin{}, nil
	}
	rest, _ := strings.CutPrefix(j.JobID, jobToolPrefix)
	i := strings.LastIndex(rest, ":")
	if i <= 0 {
		return jobOrigin{}, nil
	}
	o := jobOrigin{InputID: rest[:i]}
	var text, status string
	err := tx.QueryRow(ctx,
		`SELECT coalesce(payload->>'text', ''), status FROM core_inputs WHERE persona_id = $1 AND input_id = $2`,
		j.PersonaID, o.InputID).Scan(&text, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobOrigin{}, nil
	}
	if err != nil {
		return jobOrigin{}, err
	}
	o.Request = boundRunes(text, jobNoteRequestRunes)
	o.InProgress = status != "done"
	return o, nil
}

// jobNotificationText is the one-line, model-readable summary carried in the
// notification input's payload.text (the journal/context assembly reads it).
// It names the command and the request the job was started for, so the turn
// is self-explanatory even when processed before that request's journal.
func jobNotificationText(j *Job, origin jobOrigin) string {
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
	if cmd := jobCommandSummary(j); cmd != "" {
		b.WriteString(" — command: " + cmd)
	}
	if origin.InputID != "" {
		b.WriteString(" — started by you for request " + origin.InputID)
		if origin.Request != "" {
			b.WriteString(": " + strconv.Quote(origin.Request))
		}
		if origin.InProgress {
			b.WriteString(" (that request was not finished yet when this job ended)")
		}
	}
	return b.String()
}

// notifyJobTerminalTx enqueues the job's terminal notification input exactly
// once (id 'job:<job_id>', ON CONFLICT DO NOTHING) and stamps notified_at.
// Callers hold the job's row lock inside the terminal transition's tx.
func (s *Store) notifyJobTerminalTx(ctx context.Context, tx pgx.Tx, j *Job) error {
	origin, err := jobOriginTx(ctx, tx, j)
	if err != nil {
		return err
	}
	payload := map[string]any{
		"job_id": j.JobID,
		"kind":   j.Kind,
		"status": j.Status,
		"text":   jobNotificationText(j, origin),
	}
	if cmd := jobCommandSummary(j); cmd != "" {
		payload["command"] = cmd
	}
	if origin.InputID != "" {
		payload["origin_input_id"] = origin.InputID
		payload["origin_request"] = origin.Request
		payload["origin_in_progress"] = origin.InProgress
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
		INSERT INTO core_inputs (persona_id, input_id, kind, payload, actor_kind, actor_id, source_surface, attention, status, received_seq)
		SELECT $1::uuidv7, $2, 'job_completed', $3, 'job', $4, 'core_jobs', 'reply', 'queued',
			(SELECT MIN(seq) FROM core_events
			 WHERE persona_id = $1::uuidv7 AND kind = 'input_received'
				AND payload->>'input_id' = $2)
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
	if strings.ContainsRune(runnerID, 0) {
		return nil, nil, fmt.Errorf("%w: runner_id contains a NUL byte text cannot store", ErrBadRequest)
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

	// The claim is gated on the persona being active: submission and the
	// transfer seal are serialized so no claimable job can exist on a
	// non-active persona, and this predicate keeps that true even if one
	// ever does — a sealed or transferred placement must not start work.
	rows, err = tx.Query(ctx, `
		UPDATE core_jobs SET status = 'running', claimed_by = $2,
			claim_expires_at = now() + $4::interval,
			started_at = COALESCE(started_at, now())
		WHERE (persona_id, job_id) IN (
			SELECT j.persona_id, j.job_id FROM core_jobs j
			WHERE j.persona_id = $1 AND j.status = 'queued' AND j.kind = ANY($3::text[])
				AND EXISTS (SELECT 1 FROM core_personas p
				            WHERE p.persona_id = j.persona_id AND p.authority = 'active')
			ORDER BY j.created_at, j.job_id LIMIT $5 FOR UPDATE SKIP LOCKED
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
	if strings.ContainsRune(jobError, 0) {
		return Job{}, fmt.Errorf("%w: job error contains a NUL byte text cannot store", ErrBadRequest)
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
		if hasNUL(jobID) {
			return nil, fmt.Errorf("%w: job_id contains a NUL byte", ErrBadRequest)
		}
		j, err := scanJob(tx.QueryRow(ctx,
			`SELECT `+jobCols+` FROM core_jobs WHERE persona_id = $1 AND job_id = $2`,
			personaID, jobID))
		if err != nil {
			return nil, jobToolErr(err)
		}
		return map[string]any{"job": j}, nil
	case "job.cancel":
		jobID, _ := request["job_id"].(string)
		if jobID == "" {
			return nil, fmt.Errorf("%w: job.cancel requires job_id", ErrBadRequest)
		}
		if hasNUL(jobID) {
			return nil, fmt.Errorf("%w: job_id contains a NUL byte", ErrBadRequest)
		}
		j, err := s.cancelJobTx(ctx, tx, personaID, jobID)
		if err != nil {
			return nil, jobToolErr(err)
		}
		return map[string]any{"job": j}, nil
	}
	return nil, nil
}

// jobToolErr keeps definite rejections (unknown job, deterministic data
// errors) as recorded 400 tool results but lets transient store failures
// propagate, so the claim — and thus the call — is retried rather than
// committed as a bad request.
func jobToolErr(err error) error {
	if errors.Is(err, ErrJobNotFound) {
		return fmt.Errorf("%w: %v", ErrBadRequest, err)
	}
	return dataErr(err)
}

// withCurrentJobTx returns a replayed job.* receipt with the job's state now
// alongside it. The receipt is the call's original result — job.start's
// always says 'queued' — while a resumed turn is typically reasoning after
// the job moved on (a later round backed off, the job finished, and its
// notification may already have been handled). The stored receipt is never
// rewritten; current_job exists only in this replay's response.
func withCurrentJobTx(ctx context.Context, tx pgx.Tx, personaID string, receipt map[string]any) (map[string]any, error) {
	jm, _ := receipt["job"].(map[string]any)
	jobID, _ := jm["job_id"].(string)
	if jobID == "" {
		return receipt, nil
	}
	j, err := scanJob(tx.QueryRow(ctx,
		`SELECT `+jobCols+` FROM core_jobs WHERE persona_id = $1 AND job_id = $2`,
		personaID, jobID))
	if errors.Is(err, ErrJobNotFound) {
		return receipt, nil
	}
	if err != nil {
		return nil, err
	}
	current := map[string]any{"status": j.Status}
	if code, ok := j.Result["exit_code"]; ok {
		current["exit_code"] = code
	}
	if j.Error != nil && *j.Error != "" {
		current["error"] = *j.Error
	}
	if j.FinishedAt != nil {
		current["finished_at"] = j.FinishedAt
	}
	out := make(map[string]any, len(receipt)+2)
	for k, v := range receipt {
		out[k] = v
	}
	out["current_job"] = current
	out["receipt_note"] = "job is this call's original result; current_job is the job's state when this turn resumed"
	return out, nil
}
