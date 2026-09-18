package agentstate

// Kind-filtered job discovery (M09 follow-up): the shared seam every job
// backend consumes. A backend asks "which personas have work for my kinds"
// and "which personas still carry my claims" instead of polling every
// persona or duplicating discovery SQL. Both queries take a rotation cursor
// like PersonasAwaitingRuntime so a bounded page walks the whole set over
// consecutive calls instead of always returning the same oldest prefix.

import (
	"context"
	"fmt"
)

// PersonasWithRunnableJobs returns active personas holding at least one
// queued job of the requested kinds — the set a runner should offer claims
// to. Sealed or transferred personas are excluded even if a queued row
// somehow exists: a claim on one would be refused anyway, and re-listing
// it every sweep would starve real work.
//
// after is the rotation cursor: rows with persona_id > after sort first,
// then the result wraps. The caller advances the cursor to the last
// returned id ("" restarts from the beginning).
func (s *Store) PersonasWithRunnableJobs(ctx context.Context, kinds []string, limit int, after string) ([]string, error) {
	if len(kinds) == 0 {
		return nil, fmt.Errorf("%w: kinds required", ErrBadRequest)
	}
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	rows, err := s.pool.Query(ctx, `
		SELECT persona_id FROM (
			SELECT DISTINCT j.persona_id FROM core_jobs j
			JOIN core_personas p ON p.persona_id = j.persona_id AND p.authority = 'active'
			WHERE j.status = 'queued' AND j.kind = ANY($1::text[])
		) t
		ORDER BY (persona_id::text > $2) DESC, persona_id
		LIMIT $3`, kinds, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// PersonasWithClaimedJobs returns personas where runnerID still holds a
// claim record on a job of the requested kinds in a state that may need
// reconciliation: running or cancel_requested (live claims the restarted
// runner must resume or lose), and lost (a verdict already committed whose
// backend execution may still be running and must be reconciled, not
// assumed stopped). Terminal done/failed/cancelled rows carry no live
// backend work by construction and are excluded.
func (s *Store) PersonasWithClaimedJobs(ctx context.Context, runnerID string, kinds []string, limit int, after string) ([]string, error) {
	if runnerID == "" {
		return nil, fmt.Errorf("%w: runner_id required", ErrBadRequest)
	}
	if len(kinds) == 0 {
		return nil, fmt.Errorf("%w: kinds required", ErrBadRequest)
	}
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	rows, err := s.pool.Query(ctx, `
		SELECT persona_id FROM (
			SELECT DISTINCT j.persona_id FROM core_jobs j
			WHERE j.claimed_by = $1 AND j.kind = ANY($2::text[])
				AND j.status IN ('running','cancel_requested','lost')
		) t
		ORDER BY (persona_id::text > $3) DESC, persona_id
		LIMIT $4`, runnerID, kinds, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// Job-level discovery — the seam the reconcile loop actually drives on.
// Persona-level pages cannot express "this runner's unresolved work": a
// per-persona ListJobs(statuses, N) window is newest-first and unfiltered,
// so a persona with more than N newer resolved or foreign-kind rows hides
// the older unresolved one forever. These queries filter in SQL and page
// over the unresolved set itself, so the bound applies to the work, not to
// the persona's history.
//
// Both take a (persona_id, job_id) cursor: rows strictly after the cursor
// sort next. The caller advances it to the last returned row and resets to
// ("","") when a page comes back short, so consecutive calls walk the whole
// unresolved set fairly instead of re-reading the same prefix.

// ClaimJobsNeedingAttention returns this runner's unresolved claimed jobs —
// running/cancel_requested claims plus 'lost' rows that still lack an
// observed_outcome. A lost row that already carries its outcome is fully
// resolved: it is excluded here so it stops generating process-backend
// reads. Rows are filtered by kind and owner in SQL; the caller never sees
// foreign-kind or foreign-runner records.
func (s *Store) ClaimJobsNeedingAttention(ctx context.Context, runnerID string, kinds []string, limit int, afterPersona, afterJob string) ([]Job, error) {
	if runnerID == "" {
		return nil, fmt.Errorf("%w: runner_id required", ErrBadRequest)
	}
	if len(kinds) == 0 {
		return nil, fmt.Errorf("%w: kinds required", ErrBadRequest)
	}
	if limit <= 0 || limit > 500 {
		limit = 64
	}
	rows, err := s.pool.Query(ctx, `
		SELECT `+jobCols+` FROM core_jobs j
		WHERE j.claimed_by = $1 AND j.kind = ANY($2::text[])
			AND (j.status IN ('running','cancel_requested')
			     OR (j.status = 'lost'
			         AND NOT (COALESCE(j.result, '{}'::jsonb) ? 'observed_outcome')))
			AND (j.persona_id::text, j.job_id) > ($3::text, $4::text)
		ORDER BY j.persona_id, j.job_id
		LIMIT $5`, runnerID, kinds, afterPersona, afterJob, limit)
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

// RunnableJobs returns queued jobs of the requested kinds on active
// personas, at job granularity with the same fair cursor. Sealed or
// transferred personas are excluded in SQL — a claim on one would be
// refused anyway.
func (s *Store) RunnableJobs(ctx context.Context, kinds []string, limit int, afterPersona, afterJob string) ([]Job, error) {
	if len(kinds) == 0 {
		return nil, fmt.Errorf("%w: kinds required", ErrBadRequest)
	}
	if limit <= 0 || limit > 500 {
		limit = 64
	}
	rows, err := s.pool.Query(ctx, `
		SELECT `+jobColsJ+` FROM core_jobs j
		JOIN core_personas p ON p.persona_id = j.persona_id AND p.authority = 'active'
		WHERE j.status = 'queued' AND j.kind = ANY($1::text[])
			AND (j.persona_id::text, j.job_id) > ($2::text, $3::text)
		ORDER BY j.persona_id, j.job_id
		LIMIT $4`, kinds, afterPersona, afterJob, limit)
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
