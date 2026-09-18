package agentstate

// Job-scoped file capability for script jobs (M10 lightweight scripts).
//
// A script never holds a file credential. The runner's worker asks this
// service to perform one bounded, allowlisted filesvc operation; authority
// is the job's live runner claim (status 'running', claimed_by = caller,
// claim not expired) — cancel_requested already denies NEW operations,
// which is the strongest statement the lifecycle supports: a cancel that
// landed before the call means the effect was never authorized.
//
// Mutating operations are admitted in two committed steps so the fact of
// admission survives a dead connection or process:
//  1. AdmitJobFileOp locks the job row, verifies the claim, and commits an
//     'admitted' ledger row carrying a server-minted op_id — the exact
//     X-Idempotency-Key sent to filesvc.
//  2. RecordJobFileOp marks the row settled/refused after a determinate
//     upstream answer, or 'unknown' when the response was lost.
//
// A row stuck at admitted/unknown is reconciled by ResolveJobFileOp: the
// stored canonical request is resent under the same key, and filesvc's
// durable receipt answers "did this commit" truthfully — replayed means it
// committed, a fresh execution means it now does, and a diverged receipt is
// preserved as 'diverged' rather than silently retried. A terminal job
// record therefore never has to claim admitted effects settled: the ledger
// keeps the pending truth until resolution.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	// ErrJobFileOpDenied is the claim-authority refusal: the job is not a
	// live 'running' claim of this runner (queued, cancel_requested,
	// terminal, expired, or claimed by someone else) — no new effect.
	ErrJobFileOpDenied = errors.New("job claim does not authorize file operations")
	ErrJobFileOpNotFound = errors.New("job file operation not found")
)

// Mutating ops the capability may run. Read ops (stat/list/read) are
// authority-checked per call and never enter the ledger — they have no
// effect to settle.
var jobFileMutatingOps = map[string]bool{"write": true, "mkdir": true, "remove": true}
var jobFileReadOps = map[string]bool{"stat": true, "list": true, "read": true}

func jobFileOpAllowed(op string) (mutating bool, ok bool) {
	if jobFileMutatingOps[op] {
		return true, true
	}
	return false, jobFileReadOps[op]
}

// Bounds. Per-op bodies and paths stay small; the per-job call/byte budget
// is the runner's configured limit enforced alongside, not schema-shaped.
const (
	jobFilePathMaxBytes = 1024
	jobFileBodyMaxBytes = 256 << 10
)

var jobIfVersionRe = regexp.MustCompile(`^(any|none|[0-9]+)$`)

// JobFileOp is the durable ledger record for one mutating file operation.
type JobFileOp struct {
	PersonaID  string         `json:"persona_id"`
	JobID      string         `json:"job_id"`
	OpSeq      int64          `json:"op_seq"`
	OpID       string         `json:"op_id"`
	Op         string         `json:"op"`
	Scope      string         `json:"scope"`
	Path       string         `json:"path"`
	Request    map[string]any `json:"request"`
	Body       []byte         `json:"-"`
	Status     string         `json:"status"`
	Result     map[string]any `json:"result"`
	Error      *string        `json:"error"`
	CreatedAt  time.Time      `json:"created_at"`
	ResolvedAt *time.Time     `json:"resolved_at"`
}

const jobFileOpCols = `persona_id, job_id, op_seq, op_id, op, scope, path,
	request, body, status, result, error, created_at, resolved_at`

func scanJobFileOp(row inputScanner) (JobFileOp, error) {
	var o JobFileOp
	err := row.Scan(&o.PersonaID, &o.JobID, &o.OpSeq, &o.OpID, &o.Op, &o.Scope,
		&o.Path, &o.Request, &o.Body, &o.Status, &o.Result, &o.Error,
		&o.CreatedAt, &o.ResolvedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return o, ErrJobFileOpNotFound
	}
	return o, err
}

// JobFileUnresolved reports whether the status still needs reconciliation.
func JobFileUnresolved(status string) bool {
	return status == "admitted" || status == "unknown"
}

func validJobFilePath(path string) error {
	if path == "" || len(path) > jobFilePathMaxBytes {
		return fmt.Errorf("%w: path must be 1-%d bytes", ErrBadRequest, jobFilePathMaxBytes)
	}
	if path[0] == '/' {
		return fmt.Errorf("%w: path must be scope-relative, not absolute", ErrBadRequest)
	}
	for _, seg := range strings.Split(path, "/") {
		if seg == ".." {
			return fmt.Errorf("%w: path must not contain '..'", ErrBadRequest)
		}
	}
	return nil
}

// checkJobFileAuthorityTx verifies the caller's live runner claim on the
// job row (locked FOR UPDATE so the check serializes against cancel,
// completion, and claim-expiry transitions). A cancel that committed first
// is a denial here, and this admission commits before the check's lock is
// released — an admitted op is one the claim authorized, by construction.
func checkJobFileAuthorityTx(ctx context.Context, tx pgx.Tx, personaID, jobID, runnerID string) error {
	var status, claimedBy *string
	var expires *time.Time
	err := tx.QueryRow(ctx,
		`SELECT status, claimed_by, claim_expires_at FROM core_jobs
			WHERE persona_id = $1 AND job_id = $2 FOR UPDATE`,
		personaID, jobID).Scan(&status, &claimedBy, &expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrJobNotFound
	}
	if err != nil {
		return err
	}
	if status == nil || *status != "running" || claimedBy == nil || *claimedBy != runnerID ||
		expires == nil || !expires.After(time.Now()) {
		return ErrJobFileOpDenied
	}
	return nil
}

// CheckJobFileAuthority authorizes a read (non-mutating) file operation:
// the same live-claim rule, with no ledger row because there is no effect
// to reconcile.
func (s *Store) CheckJobFileAuthority(ctx context.Context, personaID, jobID, runnerID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := checkJobFileAuthorityTx(ctx, tx, personaID, jobID, runnerID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// canonicalFileRequest renders the operation's durable request record: the
// fields the keyed resend must reproduce exactly.
func canonicalFileRequest(op, path, ifVersion string, body []byte) map[string]any {
	req := map[string]any{"op": op, "path": path}
	if ifVersion != "" {
		req["if_version"] = ifVersion
	}
	if body != nil {
		sum := sha256.Sum256(body)
		req["body_sha256"] = hex.EncodeToString(sum[:])
		req["body_bytes"] = len(body)
	}
	return req
}

// AdmitJobFileOp validates and durably records a mutating operation while
// the claim check holds — the ledger row commits before the upstream call,
// so the operation's existence survives the caller, the connection, and
// the service process.
func (s *Store) AdmitJobFileOp(ctx context.Context, personaID, jobID, runnerID, op, scope, path, ifVersion string, body []byte) (JobFileOp, error) {
	if !jobFileMutatingOps[op] {
		return JobFileOp{}, fmt.Errorf("%w: op %q is not a mutating file operation", ErrBadRequest, op)
	}
	if err := validJobFilePath(path); err != nil {
		return JobFileOp{}, err
	}
	if ifVersion != "" && !jobIfVersionRe.MatchString(ifVersion) {
		return JobFileOp{}, fmt.Errorf("%w: if_version must be any|none|<n>", ErrBadRequest)
	}
	if len(body) > jobFileBodyMaxBytes {
		return JobFileOp{}, fmt.Errorf("%w: body exceeds %d bytes", ErrBadRequest, jobFileBodyMaxBytes)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return JobFileOp{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := checkJobFileAuthorityTx(ctx, tx, personaID, jobID, runnerID); err != nil {
		return JobFileOp{}, err
	}
	opID := "jfo:" + uuid.NewString()
	req := canonicalFileRequest(op, path, ifVersion, body)
	o, err := scanJobFileOp(tx.QueryRow(ctx, `
		INSERT INTO core_job_file_ops
			(persona_id, job_id, op_seq, op_id, op, scope, path, request, body, status)
		SELECT $1::uuidv7, $2, COALESCE(MAX(op_seq), 0) + 1, $3, $4, $5, $6, $7, $8, 'admitted'
		FROM core_job_file_ops WHERE persona_id = $1::uuidv7 AND job_id = $2
		RETURNING `+jobFileOpCols,
		personaID, jobID, opID, op, scope, path, req, body))
	if err != nil {
		return JobFileOp{}, fmt.Errorf("admit job file op: %w", dataErr(err))
	}
	if err := tx.Commit(ctx); err != nil {
		return JobFileOp{}, err
	}
	return o, nil
}

// RecordJobFileOp settles the ledger row with the upstream outcome. Only
// unresolved rows move; a row already resolved by a racing reconciler is
// returned unchanged — identical settlements are idempotent by the filesvc
// key's own determinism.
func (s *Store) RecordJobFileOp(ctx context.Context, personaID, jobID, opID, status string, result map[string]any, opErr string) (JobFileOp, error) {
	switch status {
	case "settled", "refused", "unknown", "diverged":
	default:
		return JobFileOp{}, fmt.Errorf("%w: file op status must be settled, refused, unknown, or diverged", ErrBadRequest)
	}
	var errCol *string
	if opErr != "" {
		errCol = &opErr
	}
	resolved := status != "unknown"
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return JobFileOp{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	o, err := scanJobFileOp(tx.QueryRow(ctx,
		`SELECT `+jobFileOpCols+` FROM core_job_file_ops
			WHERE persona_id = $1 AND job_id = $2 AND op_id = $3 FOR UPDATE`,
		personaID, jobID, opID))
	if err != nil {
		return JobFileOp{}, err
	}
	if JobFileUnresolved(o.Status) {
		o, err = scanJobFileOp(tx.QueryRow(ctx, `
			UPDATE core_job_file_ops
			SET status = $4, result = $5, error = $6,
				resolved_at = CASE WHEN $7 THEN now() ELSE resolved_at END
			WHERE persona_id = $1 AND job_id = $2 AND op_id = $3
			RETURNING `+jobFileOpCols,
			personaID, jobID, opID, status, result, errCol, resolved))
		if err != nil {
			return JobFileOp{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return JobFileOp{}, err
	}
	return o, nil
}

// JobFileOpForResolve returns the unresolved op's durable record under a
// row lock the caller renews per resolve attempt. The second return reports
// whether a resend is still needed (false = already resolved; return the
// stored row).
func (s *Store) JobFileOpForResolve(ctx context.Context, personaID, jobID, opID string) (JobFileOp, bool, error) {
	o, err := scanJobFileOp(s.pool.QueryRow(ctx,
		`SELECT `+jobFileOpCols+` FROM core_job_file_ops
			WHERE persona_id = $1 AND job_id = $2 AND op_id = $3`,
		personaID, jobID, opID))
	if err != nil {
		return JobFileOp{}, false, err
	}
	return o, JobFileUnresolved(o.Status), nil
}

// ListJobFileOps returns the job's mutating-operation ledger, oldest first.
// pendingOnly limits to admitted/unknown rows — the reconciler's view.
func (s *Store) ListJobFileOps(ctx context.Context, personaID, jobID string, pendingOnly bool, limit int) ([]JobFileOp, error) {
	limit = clampLimit(limit, 50, 500)
	q := `SELECT ` + jobFileOpCols + ` FROM core_job_file_ops
		WHERE persona_id = $1 AND job_id = $2`
	if pendingOnly {
		q += ` AND status IN ('admitted','unknown')`
	}
	q += ` ORDER BY op_seq LIMIT $3`
	rows, err := s.pool.Query(ctx, q, personaID, jobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []JobFileOp{}
	for rows.Next() {
		o, err := scanJobFileOp(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// PendingJobFileOps counts a job's unresolved ops; the terminal report
// carries the count so a done record can say honestly that admitted
// effects were still unsettled when it committed.
func (s *Store) PendingJobFileOps(ctx context.Context, personaID, jobID string) (int, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM core_job_file_ops
		WHERE persona_id = $1 AND job_id = $2 AND status IN ('admitted','unknown')`,
		personaID, jobID).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}
