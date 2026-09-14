package portable

import (
	"context"
	"fmt"
	"strings"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

// Violation is one failed reference-integrity check and how many rows fail it.
type Violation struct {
	Check string `json:"check"`
	Rows  int64  `json:"rows"`
}

// cutChecks are the references core state relies on that foreign keys do not
// enforce. They run on the source at seal and on the destination before a
// bundle is staged. Each query returns the number of offending rows.
//
// The epoch check is only meaningful at a cut (sealed source or staged
// destination), where no row may carry the lease's current generation.
var cutChecks = []struct{ name, sql string }{
	{"input_turn_missing", `
		SELECT count(*) FROM core_inputs i
		WHERE i.persona_id = $1 AND i.turn_id IS NOT NULL AND NOT EXISTS (
			SELECT 1 FROM core_turns t WHERE t.persona_id = i.persona_id AND t.turn_id = i.turn_id)`},
	{"event_turn_missing", `
		SELECT count(*) FROM core_events e
		WHERE e.persona_id = $1 AND NOT EXISTS (
			SELECT 1 FROM core_turns t WHERE t.persona_id = e.persona_id AND t.turn_id = e.turn_id)`},
	{"operation_turn_missing", `
		SELECT count(*) FROM core_operations o
		WHERE o.persona_id = $1 AND NOT EXISTS (
			SELECT 1 FROM core_turns t WHERE t.persona_id = o.persona_id AND t.turn_id = o.turn_id)`},
	// An operation's identity is its position in the input's recorded plan;
	// without that plan entry a destination could re-execute it as new. The
	// key's call index addresses a flat position across every round's calls
	// in order — the same indexing ClaimOperation applies.
	{"operation_outside_recorded_plan", `
		SELECT count(*) FROM core_operations o
		WHERE o.persona_id = $1 AND NOT EXISTS (
			SELECT 1 FROM core_turn_plans p
			CROSS JOIN LATERAL (
				SELECT row_number() OVER (ORDER BY r.ri, c.ci) - 1 AS flat_idx,
				       c.call->>'tool' AS tool
				FROM jsonb_array_elements(p.plan) WITH ORDINALITY AS r(round, ri)
				CROSS JOIN LATERAL jsonb_array_elements(r.round->'calls') WITH ORDINALITY AS c(call, ci)
			) AS f
			WHERE p.persona_id = o.persona_id
			  AND o.idempotency_key = p.input_id || ':tool:' || f.flat_idx::text
			  AND f.tool = o.tool)`},
	// Recovery requeues a running turn's input only while that input is
	// claimed by the turn; anything else would strand the input.
	{"running_turn_input_not_claimed", `
		SELECT count(*) FROM core_turns t
		JOIN core_inputs i ON i.persona_id = t.persona_id AND i.input_id = t.input_id
		WHERE t.persona_id = $1 AND t.status = 'running'
		  AND (i.status <> 'claimed' OR i.turn_id IS DISTINCT FROM t.turn_id)`},
	{"done_input_turn_unfinished", `
		SELECT count(*) FROM core_inputs i
		JOIN core_turns t ON t.persona_id = i.persona_id AND t.turn_id = i.turn_id
		WHERE i.persona_id = $1 AND i.status = 'done' AND t.status NOT IN ('done', 'failed')`},
	{"wake_input_schedule_missing", `
		SELECT count(*) FROM core_inputs i
		WHERE i.persona_id = $1 AND i.input_id LIKE 'sched:%' AND NOT EXISTS (
			SELECT 1 FROM core_schedules s
			WHERE s.persona_id = i.persona_id AND 'sched:' || s.schedule_id = i.input_id)`},
	{"fired_schedule_wake_missing", `
		SELECT count(*) FROM core_schedules s
		WHERE s.persona_id = $1 AND s.status = 'fired' AND NOT EXISTS (
			SELECT 1 FROM core_inputs i
			WHERE i.persona_id = s.persona_id AND i.input_id = 'sched:' || s.schedule_id)`},
	// An approval's identity is the operation it parks plus that
	// operation's position in the input's recorded plan. A dangling or
	// mismatched link would let the destination re-execute a decided call
	// or strand a pending one.
	{"approval_operation_missing", `
		SELECT count(*) FROM core_tool_approvals a
		WHERE a.persona_id = $1 AND NOT EXISTS (
			SELECT 1 FROM core_operations o
			WHERE o.persona_id = a.persona_id AND o.operation_id = a.operation_id
			  AND o.turn_id = a.turn_id AND o.tool = a.tool
			  AND o.idempotency_key = a.input_id || ':tool:' || a.call_index::text)`},
	// A waiting input is parked on a human decision; without its pending
	// approval the destination could never resume it.
	{"waiting_input_approval_missing", `
		SELECT count(*) FROM core_inputs i
		WHERE i.persona_id = $1 AND i.status = 'waiting' AND NOT EXISTS (
			SELECT 1 FROM core_tool_approvals a
			WHERE a.persona_id = i.persona_id AND a.input_id = i.input_id AND a.status = 'pending')`},
	{"waiting_since_without_waiting", `
		SELECT count(*) FROM core_inputs i
		WHERE i.persona_id = $1 AND i.waiting_since IS NOT NULL AND i.status <> 'waiting'`},
	{"pending_approval_operation_resolved", `
		SELECT count(*) FROM core_tool_approvals a
		JOIN core_operations o ON o.persona_id = a.persona_id AND o.operation_id = a.operation_id
		WHERE a.persona_id = $1 AND a.status = 'pending' AND o.status <> 'awaiting_approval'`},
	// The approval must describe the exact action the operation would run:
	// same request payload and a recomputably-correct action digest (the
	// digest itself is checked in Go below). A request that disagrees with
	// its operation means the human decided on one thing while the ledger
	// would execute another.
	{"approval_operation_request_mismatch", `
		SELECT count(*) FROM core_tool_approvals a
		JOIN core_operations o ON o.persona_id = a.persona_id AND o.operation_id = a.operation_id
		WHERE a.persona_id = $1 AND a.request <> o.request`},
	// A pending approval carries no decision and is never consumed.
	{"pending_approval_decided_fields", `
		SELECT count(*) FROM core_tool_approvals a
		WHERE a.persona_id = $1 AND a.status = 'pending' AND (
			a.decision IS NOT NULL OR a.decision_id IS NOT NULL
			OR a.decided_by_id IS NOT NULL OR a.decided_at IS NOT NULL
			OR a.consumed_at IS NOT NULL)`},
	// A decided approval carries its whole decision record.
	{"decided_approval_incomplete", `
		SELECT count(*) FROM core_tool_approvals a
		WHERE a.persona_id = $1 AND a.status IN ('approved','denied') AND (
			a.decision IS NULL OR a.decision_id IS NULL
			OR a.decided_by_id IS NULL OR a.decided_at IS NULL)`},
	{"approval_decision_status_mismatch", `
		SELECT count(*) FROM core_tool_approvals a
		WHERE a.persona_id = $1 AND (
			(a.status = 'approved' AND a.decision IS DISTINCT FROM 'approve_once')
			OR (a.status = 'denied' AND a.decision IS DISTINCT FROM 'deny_once'))`},
	{"approved_approval_without_provenance", `
		SELECT count(*) FROM core_tool_approvals a
		WHERE a.persona_id = $1 AND a.status = 'approved' AND a.provenance IS NULL`},
	// The lifecycle matrix, status by status:
	//   pending                → operation awaiting_approval (checked above)
	//   approved, unconsumed   → operation awaiting_approval — the carried
	//                            grant awaiting its one execution
	//   approved, consumed     → operation done or failed — the receipt
	//   denied                 → operation failed, never consumed
	{"denied_approval_operation_not_failed", `
		SELECT count(*) FROM core_tool_approvals a
		JOIN core_operations o ON o.persona_id = a.persona_id AND o.operation_id = a.operation_id
		WHERE a.persona_id = $1 AND a.status = 'denied' AND o.status <> 'failed'`},
	{"denied_approval_consumed", `
		SELECT count(*) FROM core_tool_approvals a
		WHERE a.persona_id = $1 AND a.status = 'denied' AND a.consumed_at IS NOT NULL`},
	{"unconsumed_grant_operation_settled", `
		SELECT count(*) FROM core_tool_approvals a
		JOIN core_operations o ON o.persona_id = a.persona_id AND o.operation_id = a.operation_id
		WHERE a.persona_id = $1 AND a.status = 'approved' AND a.consumed_at IS NULL
			AND o.status <> 'awaiting_approval'`},
	{"consumed_grant_operation_unsettled", `
		SELECT count(*) FROM core_tool_approvals a
		JOIN core_operations o ON o.persona_id = a.persona_id AND o.operation_id = a.operation_id
		WHERE a.persona_id = $1 AND a.consumed_at IS NOT NULL
			AND o.status NOT IN ('done','failed')`},
	// An awaiting operation must hold a live grant to resolve with: a
	// pending approval or an approved-unconsumed one. Without one the
	// parked call could never settle — the B F2 loop — and without this
	// check a denial attached to an awaiting operation would re-park
	// forever.
	{"awaiting_operation_without_live_approval", `
		SELECT count(*) FROM core_operations o
		WHERE o.persona_id = $1 AND o.status = 'awaiting_approval' AND NOT EXISTS (
			SELECT 1 FROM core_tool_approvals a
			WHERE a.persona_id = o.persona_id AND a.operation_id = o.operation_id
			  AND (a.status = 'pending'
			       OR (a.status = 'approved' AND a.consumed_at IS NULL)))`},
	{"outbox_reference_missing", `
		SELECT count(*) FROM core_outbox o
		WHERE o.persona_id = $1 AND o.kind = 'turn_completed' AND (
			NOT EXISTS (SELECT 1 FROM core_turns t
				WHERE t.persona_id = o.persona_id AND t.turn_id = o.payload->>'turn_id')
			OR NOT EXISTS (SELECT 1 FROM core_inputs i
				WHERE i.persona_id = o.persona_id AND i.input_id = o.payload->>'input_id'))`},
	{"journal_seq_not_contiguous", `
		SELECT CASE WHEN count(*) = COALESCE(max(seq), 0) AND COALESCE(min(seq), 1) >= 1 THEN 0 ELSE 1 END
		FROM core_events WHERE persona_id = $1`},
	{"outbox_seq_not_contiguous", `
		SELECT CASE WHEN count(*) = COALESCE(max(seq), 0) AND COALESCE(min(seq), 1) >= 1 THEN 0 ELSE 1 END
		FROM core_outbox WHERE persona_id = $1`},
	{"generation_not_below_epoch", `
		SELECT count(*) FROM (
			SELECT generation AS g FROM core_turns WHERE persona_id = $1
			UNION ALL SELECT generation FROM core_turn_plans WHERE persona_id = $1
			UNION ALL SELECT claimed_generation FROM core_operations WHERE persona_id = $1
			UNION ALL SELECT claimed_generation FROM core_inputs WHERE persona_id = $1 AND claimed_generation IS NOT NULL
			UNION ALL SELECT claimed_generation FROM core_schedules WHERE persona_id = $1 AND claimed_generation IS NOT NULL
		) x
		WHERE x.g >= COALESCE((SELECT generation FROM core_writer_leases WHERE persona_id = $1), 0)`},
}

func verifyCut(ctx context.Context, q querier, personaID string) ([]Violation, error) {
	out := []Violation{}
	for _, c := range cutChecks {
		var n int64
		if err := q.QueryRow(ctx, c.sql, personaID).Scan(&n); err != nil {
			return nil, fmt.Errorf("integrity check %s: %w", c.name, err)
		}
		if n > 0 {
			out = append(out, Violation{Check: c.name, Rows: n})
		}
	}
	// The action digest binds the approval to the exact action the human
	// decided on; recompute it rather than trusting the stored value. This
	// is semantic coherence — corruption detection — not authenticity: an
	// attacker who edits the bundle can recompute digests too, so a clean
	// check proves consistency, never consent (the bundle's unkeyed
	// sha256 checksum has the same boundary).
	rows, err := q.Query(ctx, `
		SELECT approval_id, tool, route, request, action_digest
		FROM core_tool_approvals WHERE persona_id = $1`, personaID)
	if err != nil {
		return nil, fmt.Errorf("integrity check approval_digest_mismatch: %w", err)
	}
	defer rows.Close()
	var badDigests int64
	for rows.Next() {
		var id, tool, route, digest string
		var request map[string]any
		if err := rows.Scan(&id, &tool, &route, &request, &digest); err != nil {
			return nil, fmt.Errorf("integrity check approval_digest_mismatch: %w", err)
		}
		if digest != agentstate.ActionDigest(tool, route, request) {
			badDigests++
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("integrity check approval_digest_mismatch: %w", err)
	}
	if badDigests > 0 {
		out = append(out, Violation{Check: "approval_digest_mismatch", Rows: badDigests})
	}
	return out, nil
}

func describe(vs []Violation) string {
	parts := make([]string, len(vs))
	for i, v := range vs {
		parts[i] = fmt.Sprintf("%s (%d)", v.Check, v.Rows)
	}
	return strings.Join(parts, ", ")
}
