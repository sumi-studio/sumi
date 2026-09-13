// Durable tool-call approval (ADR 0013, M08 slice).
//
// A recorded planned call that requires human approval parks in the
// operation ledger before any effect runs: the operation row waits in
// 'awaiting_approval', bound to its input+plan position, and a
// core_tool_approvals row carries the exact action — tool, immutable
// invocation route, request, and a domain-separated action digest — plus
// the authority provenance a grant resolves to.
//
// Decisions use only the CurrentCallDecision vocabulary (approve_once /
// deny_once); standing policy mutation is a separate concern and is not
// stored here. Approving consumes the grant exactly once inside the claim
// transaction that applies the effect; denying finalizes the operation
// failed with the denial recorded — a replay can return the stored
// outcome but can never re-run or silently bypass it.
//
// The approval decision path is deliberately NOT writer-fenced: the human's
// decision is an authority act, not a core mutation. It serializes against
// the parking commit through the approval row lock.
package agentstate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	ErrApprovalNotFound  = errors.New("approval not found")
	ErrApprovalConflict  = errors.New("conflicting approval decision")
	ErrApprovalDecidedBy = errors.New("approval decision must identify the deciding human")
	ErrApprovalForbidden = errors.New("approval decision not permitted for this human")
)

// ToolApproval is the durable record of one planned call's human decision.
type ToolApproval struct {
	ApprovalID    string         `json:"approval_id"`
	PersonaID     string         `json:"persona_id"`
	InputID       string         `json:"input_id"`
	CallIndex     int            `json:"call_index"`
	OperationID   string         `json:"operation_id"`
	TurnID        string         `json:"turn_id"`
	Tool          string         `json:"tool"`
	Route         string         `json:"route"`
	RequiredBy    string         `json:"required_by"`
	Request       map[string]any `json:"request"`
	ActionDigest  string         `json:"action_digest"`
	Status        string         `json:"status"` // pending | approved | denied
	Decision      *string        `json:"decision"`
	DecisionID    *string        `json:"decision_id"`
	DecidedByKind *string        `json:"decided_by_kind"`
	DecidedByID   *string        `json:"decided_by_id"`
	Provenance    *string        `json:"provenance"`
	DecidedAt     *time.Time     `json:"decided_at"`
	ConsumedAt    *time.Time     `json:"consumed_at"`
	CreatedAt     time.Time      `json:"created_at"`
}

// ApprovalDecision is the authenticated one-shot human decision on a
// pending approval. DecisionID is the deciding command's identity: an
// identical replay returns the stored decision, a different decision on an
// already-resolved approval is a conflict — never an overwrite.
type ApprovalDecision struct {
	Decision      string `json:"decision"` // approve_once | deny_once
	DecisionID    string `json:"decision_id"`
	DecidedByKind string `json:"decided_by_kind"`
	DecidedByID   string `json:"decided_by_id"`
}

// toolAuthority is the foundation-owned registry of which state-internal
// tools require a human decision before their effect may run. The model may
// only ever *raise* a call's requirements (elevated route); it can never
// lower an intrinsic requirement by proposing route "normal".
var toolAuthority = map[string]struct {
	internal         bool
	requiresApproval bool
}{
	"schedule.set": {internal: true},
	"journal.note": {internal: true},
	// Speaking to the shared channel on the human's behalf is an
	// outward-facing act: it always waits for an explicit human decision.
	"message.send": {internal: true, requiresApproval: true},
}

func isInternalTool(tool string) bool {
	info, ok := toolAuthority[tool]
	return ok && info.internal
}

// approvalRequirement returns why a recorded call must wait for a human
// decision: "intrinsic" for a tool that always requires approval, "route"
// when the model proposed the call on the elevated route, "" otherwise.
func approvalRequirement(tool, route string) string {
	if info, ok := toolAuthority[tool]; ok && info.requiresApproval {
		return "intrinsic"
	}
	if route == "elevated" {
		return "route"
	}
	return ""
}

// actionDigest is the canonical, domain-separated identity of the exact
// action a human decides on: tool + route + request. Two calls differing in
// any of these are different decisions.
func actionDigest(tool, route string, request map[string]any) string {
	// json.Marshal sorts map keys — canonical for this map shape.
	reqJSON, _ := json.Marshal(request)
	sum := sha256.Sum256([]byte("sumi.core.tool-action.v1\x00" + tool + "\x00" + route + "\x00" + string(reqJSON)))
	return hex.EncodeToString(sum[:])
}

// approvalID is deterministic per (persona, input, call position) so the
// same planned call always surfaces under one durable identity — replayed
// claims and restarts re-find the same pending approval instead of minting
// a new one.
func approvalID(personaID, inputID string, callIndex int) string {
	sum := sha256.Sum256([]byte("sumi.core.approval.v1\x00" + personaID + "\x00" + inputID + "\x00" + strconv.Itoa(callIndex)))
	return "appr-" + hex.EncodeToString(sum[:16])
}

func scanApproval(row pgx.Row) (ToolApproval, error) {
	var a ToolApproval
	err := row.Scan(&a.ApprovalID, &a.PersonaID, &a.InputID, &a.CallIndex,
		&a.OperationID, &a.TurnID, &a.Tool, &a.Route, &a.RequiredBy,
		&a.Request, &a.ActionDigest, &a.Status, &a.Decision, &a.DecisionID,
		&a.DecidedByKind, &a.DecidedByID, &a.Provenance, &a.DecidedAt,
		&a.ConsumedAt, &a.CreatedAt)
	return a, err
}

const approvalCols = `approval_id, persona_id, input_id, call_index, operation_id, turn_id,
	tool, route, required_by, request, action_digest, status, decision, decision_id,
	decided_by_kind, decided_by_id, provenance, decided_at, consumed_at, created_at`

func (s *Store) approvalForCall(ctx context.Context, db queryRower, personaID, inputID string, callIndex int, forUpdate bool) (*ToolApproval, error) {
	q := `SELECT ` + approvalCols + ` FROM core_tool_approvals
		WHERE persona_id = $1 AND input_id = $2 AND call_index = $3`
	if forUpdate {
		q += ` FOR UPDATE`
	}
	a, err := scanApproval(db.QueryRow(ctx, q, personaID, inputID, callIndex))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

// ListApprovals returns a persona's approval records, optionally filtered by
// status (pending | approved | denied). Pending first is not assumed —
// order is creation order so the list reads as a queue.
func (s *Store) ListApprovals(ctx context.Context, personaID, status string) ([]ToolApproval, error) {
	if status != "" && status != "pending" && status != "approved" && status != "denied" {
		return nil, fmt.Errorf("%w: unknown approval status %q", ErrBadRequest, status)
	}
	q := `SELECT ` + approvalCols + ` FROM core_tool_approvals WHERE persona_id = $1`
	args := []any{personaID}
	if status != "" {
		q += ` AND status = $2`
		args = append(args, status)
	}
	q += ` ORDER BY created_at, approval_id`
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ToolApproval{}
	for rows.Next() {
		a, err := scanApproval(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// GetApproval returns one approval record.
func (s *Store) GetApproval(ctx context.Context, personaID, apprID string) (*ToolApproval, error) {
	a, err := scanApproval(s.pool.QueryRow(ctx,
		`SELECT `+approvalCols+` FROM core_tool_approvals WHERE persona_id = $1 AND approval_id = $2`,
		personaID, apprID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrApprovalNotFound
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

// ResolveApproval records an authenticated human's one-shot decision on a
// pending approval. ApproveOnce marks the grant approved with provenance
// agent_own_with_human_consent and requeues the parked input — the next
// attempt's claim consumes the grant and applies the effect exactly once.
// DenyOnce finalizes the operation failed with the denial as its durable
// response, so a replayed claim returns the denial instead of running or
// silently re-prompting. Both paths journal approval_decided and requeue
// the input — the decision survives restart and is never silently bypassed.
func (s *Store) ResolveApproval(ctx context.Context, personaID, apprID string, req ApprovalDecision) (*ToolApproval, error) {
	if req.Decision != "approve_once" && req.Decision != "deny_once" {
		return nil, fmt.Errorf("%w: decision must be approve_once or deny_once", ErrBadRequest)
	}
	if req.DecidedByKind != "human" || req.DecidedByID == "" {
		return nil, fmt.Errorf("%w: decided_by_kind 'human' and decided_by_id required", ErrApprovalDecidedBy)
	}
	// The decision route is admin-authenticated, but the named human is
	// still checked against the persona's owner: a host that relays some
	// other human's click cannot consent on this secretary's behalf.
	p, err := s.persona(ctx, personaID)
	if err != nil {
		return nil, err
	}
	if p.HumanID != nil && *p.HumanID != req.DecidedByID {
		return nil, fmt.Errorf("%w: only the persona's human may decide", ErrApprovalForbidden)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	a, err := scanApproval(tx.QueryRow(ctx,
		`SELECT `+approvalCols+` FROM core_tool_approvals
		 WHERE persona_id = $1 AND approval_id = $2 FOR UPDATE`,
		personaID, apprID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrApprovalNotFound
	}
	if err != nil {
		return nil, err
	}
	if a.Status != "pending" {
		// Idempotent replay of the same authenticated command returns the
		// stored decision; any divergent decision on a resolved approval is
		// a conflict — a denial is never overwritten into an approval.
		same := a.Decision != nil && *a.Decision == req.Decision &&
			a.DecisionID != nil && *a.DecisionID == req.DecisionID &&
			a.DecidedByKind != nil && *a.DecidedByKind == req.DecidedByKind &&
			a.DecidedByID != nil && *a.DecidedByID == req.DecidedByID
		if req.DecisionID == "" || !same {
			return nil, fmt.Errorf("%w: approval %s already %s", ErrApprovalConflict, apprID, a.Status)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return &a, nil
	}
	provenance := ""
	newStatus := "denied"
	if req.Decision == "approve_once" {
		provenance = "agent_own_with_human_consent"
		newStatus = "approved"
	}
	err = tx.QueryRow(ctx, `
		UPDATE core_tool_approvals
		SET status = $3, decision = $4, decision_id = $5,
			decided_by_kind = $6, decided_by_id = $7, decided_at = now(),
			provenance = NULLIF($8, '')
		WHERE persona_id = $1 AND approval_id = $2 AND status = 'pending'
		RETURNING `+approvalCols,
		personaID, apprID, newStatus,
		req.Decision, req.DecisionID, req.DecidedByKind, req.DecidedByID, provenance).
		Scan(&a.ApprovalID, &a.PersonaID, &a.InputID, &a.CallIndex,
			&a.OperationID, &a.TurnID, &a.Tool, &a.Route, &a.RequiredBy,
			&a.Request, &a.ActionDigest, &a.Status, &a.Decision, &a.DecisionID,
			&a.DecidedByKind, &a.DecidedByID, &a.Provenance, &a.DecidedAt,
			&a.ConsumedAt, &a.CreatedAt)
	if err != nil {
		return nil, err
	}
	if req.Decision == "deny_once" {
		// Finalize the parked operation failed with the denial as its
		// durable response — the next claim replays this outcome rather
		// than executing or waiting again.
		if _, err := tx.Exec(ctx, `
			UPDATE core_operations
			SET status = 'failed', completed_at = now(), response = $3
			WHERE persona_id = $1 AND operation_id = $2 AND status = 'awaiting_approval'`,
			personaID, a.OperationID, map[string]any{
				"error": "denied",
				"denial": map[string]any{
					"approval_id":     a.ApprovalID,
					"decision":        req.Decision,
					"decided_by_kind": req.DecidedByKind,
					"decided_by_id":   req.DecidedByID,
				},
			}); err != nil {
			return nil, fmt.Errorf("finalize denied operation: %w", err)
		}
	}
	// The decision itself is journaled on the parked turn's record — the
	// durable log shows who decided what even if the input never resumes.
	if err := s.appendEventsTx(ctx, tx, personaID, a.TurnID, []EventInput{{
		Kind: "approval_decided",
		Payload: map[string]any{
			"approval_id":     a.ApprovalID,
			"input_id":        a.InputID,
			"call_index":      a.CallIndex,
			"tool":            a.Tool,
			"route":           a.Route,
			"decision":        req.Decision,
			"decided_by_kind": req.DecidedByKind,
			"decided_by_id":   req.DecidedByID,
		},
	}}); err != nil {
		return nil, err
	}
	// Requeue the parked input for resumption. When the parking commit has
	// not landed yet (input still 'claimed'), the await commit itself sees
	// no pending approval and queues the input — the decision can never be
	// stranded between the two.
	if _, err := tx.Exec(ctx, `
		UPDATE core_inputs SET status = 'queued', claimed_generation = NULL,
			turn_id = NULL, not_before = NULL
		WHERE persona_id = $1 AND input_id = $2 AND status = 'waiting'`,
		personaID, a.InputID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &a, nil
}

// deniedIdenticalCall finds a denied approval on the same input for exactly
// this tool and request, whatever route it was proposed on.
func (s *Store) deniedIdenticalCall(ctx context.Context, db queryRower, personaID, inputID, tool string, request map[string]any) (*ToolApproval, error) {
	a, err := scanApproval(db.QueryRow(ctx, `
		SELECT `+approvalCols+` FROM core_tool_approvals
		WHERE persona_id = $1 AND input_id = $2 AND tool = $3 AND request = $4
			AND status = 'denied'
		ORDER BY call_index LIMIT 1`,
		personaID, inputID, tool, request))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

// denialResponse is the durable failed-operation response for a call the
// human refused.
func denialResponse(a *ToolApproval) map[string]any {
	denial := map[string]any{"approval_id": a.ApprovalID}
	if a.Decision != nil {
		denial["decision"] = *a.Decision
	}
	if a.DecidedByKind != nil {
		denial["decided_by_kind"] = *a.DecidedByKind
	}
	if a.DecidedByID != nil {
		denial["decided_by_id"] = *a.DecidedByID
	}
	return map[string]any{"error": "denied", "denial": denial}
}

// pendingApprovalsForInput locks the input's pending approval rows so the
// await commit serializes against a concurrent ResolveApproval.
func (s *Store) pendingApprovalsForInput(ctx context.Context, tx pgx.Tx, personaID, inputID string) ([]ToolApproval, error) {
	rows, err := tx.Query(ctx, `
		SELECT `+approvalCols+` FROM core_tool_approvals
		WHERE persona_id = $1 AND input_id = $2 AND status = 'pending'
		ORDER BY call_index FOR UPDATE`, personaID, inputID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ToolApproval{}
	for rows.Next() {
		a, err := scanApproval(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
