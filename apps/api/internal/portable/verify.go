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
	// A pending approval carries no current decision record — no debris
	// fields that only a decided row may hold. prior_* is deliberately NOT
	// checked here: a cross-authority import legitimately re-pends a grant
	// while preserving the source's decision as prior provenance.
	{"pending_approval_decided_fields", `
		SELECT count(*) FROM core_tool_approvals a
		WHERE a.persona_id = $1 AND a.status = 'pending' AND (
			a.decision IS NOT NULL OR a.decision_id IS NOT NULL
			OR a.decided_by_kind IS NOT NULL OR a.decided_by_id IS NOT NULL
			OR a.provenance IS NOT NULL
			OR a.decided_at IS NOT NULL OR a.consumed_at IS NOT NULL)`},
	// A decided approval carries its whole decision record.
	{"decided_approval_incomplete", `
		SELECT count(*) FROM core_tool_approvals a
		WHERE a.persona_id = $1 AND a.status IN ('approved','denied') AND (
			a.decision IS NULL OR a.decision_id IS NULL
			OR a.decided_by_kind IS NULL OR a.decided_by_id IS NULL
			OR a.decided_at IS NULL)`},
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
	// A memory chunk's claim is execution authority of a placement-bound
	// writer; the seal normalizes live 'preparing' rows back to 'sealed'
	// before the cut, so a bundle carrying a claim is malformed, not a
	// transfer of in-flight work.
	{"memory_chunk_claim_carried", `
		SELECT count(*) FROM core_memory_chunks c
		WHERE c.persona_id = $1 AND (
			c.status = 'preparing' OR c.claimed_generation IS NOT NULL OR c.claimed_at IS NOT NULL)`},
	// Upper-layer targets carry their provenance: every source must resolve
	// to a distinct carried same-persona chunk, all in one layer, and the
	// target's journal range must be exactly its sources' span — anchored at
	// both ends AND tiled inside, since bounds alone leave interior gaps and
	// overlaps unchecked. The array itself must be in first_seq order: the
	// selection emits it canonically and the settled-tuple dedup compares
	// arrays order-sensitively, so a reordered carried tuple would silently
	// relitigate a verdict it was meant to settle. A source is only ever
	// selected while 'applied'; afterwards it is either still applied or
	// superseded by the target that consumed it, so any other lifecycle
	// status is a state the pipeline cannot emit. And applying a target
	// supersedes its sources in the same transaction: an 'applied' target
	// over still-live sources double-renders the range — only a crafted row
	// holds that. Settled or in-flight targets legitimately keep 'applied'
	// sources, and a failed or stale one's sources may already be
	// superseded by a different applied target, so the superseded
	// requirement binds 'applied' targets only.
	{"memory_chunk_sources_invalid", `
		SELECT count(*) FROM core_memory_chunks c
		WHERE c.persona_id = $1 AND (
			(c.layer = 1 AND c.sources IS NOT NULL)
			OR (c.layer >= 2 AND (
				c.sources IS NULL OR cardinality(c.sources) = 0
				OR (SELECT count(*) FROM core_memory_chunks s2
					WHERE s2.persona_id = c.persona_id
					AND s2.chunk_seq = ANY(c.sources)) <> cardinality(c.sources)
				OR (SELECT count(DISTINCT s2.layer) FROM core_memory_chunks s2
					WHERE s2.persona_id = c.persona_id
					AND s2.chunk_seq = ANY(c.sources)) <> 1
				OR (SELECT min(s2.first_seq) FROM core_memory_chunks s2
					WHERE s2.persona_id = c.persona_id
					AND s2.chunk_seq = ANY(c.sources)) <> c.first_seq
				OR (SELECT max(s2.last_seq) FROM core_memory_chunks s2
					WHERE s2.persona_id = c.persona_id
					AND s2.chunk_seq = ANY(c.sources)) <> c.last_seq
				OR c.sources <> (SELECT array_agg(s2.chunk_seq ORDER BY s2.first_seq)
					FROM core_memory_chunks s2
					WHERE s2.persona_id = c.persona_id
					AND s2.chunk_seq = ANY(c.sources))
				OR EXISTS (
					SELECT 1 FROM (
						SELECT s2.first_seq,
							lag(s2.last_seq) OVER (ORDER BY s2.first_seq) AS prev_last
						FROM core_memory_chunks s2
						WHERE s2.persona_id = c.persona_id
						AND s2.chunk_seq = ANY(c.sources)) tile
					WHERE tile.first_seq <> tile.prev_last + 1)
				OR EXISTS (SELECT 1 FROM core_memory_chunks s2
					WHERE s2.persona_id = c.persona_id
					AND s2.chunk_seq = ANY(c.sources)
					AND s2.status NOT IN ('applied', 'superseded'))
				OR (c.status = 'applied' AND EXISTS (
					SELECT 1 FROM core_memory_chunks s2
					WHERE s2.persona_id = c.persona_id
					AND s2.chunk_seq = ANY(c.sources)
					AND s2.status <> 'superseded'))
			)))`},
	// 'superseded' means an applied upper target rendered the row's range —
	// the store only ever marks a source superseded inside the same
	// transaction that applies its consumer. So every superseded row must be
	// reachable as a source of an applied target, transitively through
	// superseded intermediate targets (a valid L2 block can itself be
	// reintegrated into a later applied ancestor). A superseded row with no
	// applied ancestor leaves journal coverage rendered by nothing — the
	// pipeline cannot produce that, so the bundle is crafted.
	{"memory_chunk_superseded_uncovered", `
		WITH RECURSIVE covered AS (
			SELECT s2.chunk_seq AS seq FROM core_memory_chunks c
			JOIN core_memory_chunks s2 ON s2.persona_id = c.persona_id
				AND s2.chunk_seq = ANY(c.sources)
			WHERE c.persona_id = $1 AND c.status = 'applied'
			UNION
			SELECT s2.chunk_seq FROM core_memory_chunks c
			JOIN covered cov ON cov.seq = c.chunk_seq
			JOIN core_memory_chunks s2 ON s2.persona_id = c.persona_id
				AND s2.chunk_seq = ANY(c.sources)
			WHERE c.persona_id = $1
		)
		SELECT count(*) FROM core_memory_chunks c
		WHERE c.persona_id = $1 AND c.status = 'superseded'
			AND NOT EXISTS (SELECT 1 FROM covered cov WHERE cov.seq = c.chunk_seq)`},
	// Chunk ranges are locators into the carried journal; a range that
	// reaches past it would render a fragment for records that do not
	// exist.
	{"memory_chunk_range_outside_journal", `
		SELECT count(*) FROM core_memory_chunks c
		WHERE c.persona_id = $1 AND (
			c.first_seq < 1
			OR c.last_seq > (SELECT COALESCE(max(seq), 0) FROM core_events WHERE persona_id = $1))`},
	// The lifecycle's own payload invariants: a prepared/applied row only
	// ever exists with replacement text and its estimate — the renderer scans
	// both into non-nullable fields, so a NULL there would fail every context
	// load on the destination. 'kept' allows either shape the store writes —
	// keep-unchanged (both NULL) or a non-shrinking text (both set) — but not
	// one without the other. Missing timestamps are not corruption.
	{"memory_chunk_missing_payload", `
		SELECT count(*) FROM core_memory_chunks c
		WHERE c.persona_id = $1 AND (
			(c.status IN ('prepared','applied')
				AND (c.replacement IS NULL OR c.replacement_est_tokens IS NULL))
			OR (c.status = 'kept'
				AND (c.replacement IS NULL) <> (c.replacement_est_tokens IS NULL)))`},
	// Sequence numbers, token estimates and counters are semantic values the
	// store only ever produces positive or zero: chunk_seq/layer start at 1,
	// estimates and attempt/interruption counts are >= 0. A negative value is
	// a crafted row that corrupts ordering and the live-raw accounting; a
	// layer above 1 is a consolidation/reintegration target whose sources'
	// provenance is checked separately above.
	{"memory_chunk_negative_values", `
		SELECT count(*) FROM core_memory_chunks c
		WHERE c.persona_id = $1 AND (
			c.chunk_seq < 1 OR c.layer < 1 OR c.est_tokens < 0
			OR c.replacement_est_tokens < 0 OR c.attempts < 0 OR c.interruptions < 0)`},
	// received_seq is the causal-dedup pointer: it must name this input's own
	// carried input_received event. A marker that dangles, points at another
	// kind, or names another input's event makes the destination skip
	// journaling the real input_received — the input's original record would
	// be silently absent from the journal it moves with.
	{"input_received_seq_mismatch", `
		SELECT count(*) FROM core_inputs i
		WHERE i.persona_id = $1 AND i.received_seq IS NOT NULL AND NOT EXISTS (
			SELECT 1 FROM core_events e
			WHERE e.persona_id = i.persona_id AND e.seq = i.received_seq
				AND e.kind = 'input_received'
				AND e.payload->>'input_id' = i.input_id)`},
	// The reverse link: an input_received already in the journal must be the
	// one its input points at. A missing or mismatched marker would let the
	// destination journal the same input a second time. A queued or claimed
	// input with no journaled event legitimately keeps NULL.
	{"input_received_seq_not_linked", `
		SELECT count(*) FROM core_events e
		JOIN core_inputs i ON i.persona_id = e.persona_id
			AND i.input_id = e.payload->>'input_id'
		WHERE e.persona_id = $1 AND e.kind = 'input_received'
			AND (i.received_seq IS NULL OR i.received_seq <> e.seq)`},
	// admission_seq is the claim queue's order. The import regenerates it
	// from the destination's identity sequence in carried order (the bundle
	// requires the source values positive and strictly increasing), so
	// staged values are destination-allocated — these checks are the
	// postcondition on what actually landed: positive and unique per
	// persona. A non-positive or duplicated one is a crafted row that
	// corrupts or makes that order ambiguous.
	{"input_admission_seq_invalid", `
		SELECT count(*) FROM core_inputs i
		WHERE i.persona_id = $1 AND i.admission_seq < 1`},
	{"input_admission_seq_duplicate", `
		SELECT count(*) FROM (
			SELECT 1 FROM core_inputs WHERE persona_id = $1
			GROUP BY admission_seq HAVING count(*) > 1) d`},
	{"journal_seq_not_contiguous", `
		SELECT CASE WHEN count(*) = COALESCE(max(seq), 0) AND COALESCE(min(seq), 1) >= 1 THEN 0 ELSE 1 END
		FROM core_events WHERE persona_id = $1`},
	{"outbox_seq_not_contiguous", `
		SELECT CASE WHEN count(*) = COALESCE(max(seq), 0) AND COALESCE(min(seq), 1) >= 1 THEN 0 ELSE 1 END
		FROM core_outbox WHERE persona_id = $1`},
	// A carried model intent is either absent or a supported shape: an
	// object whose kind names a known selection kind. A malformed intent —
	// non-object, missing kind, unknown kind — would silently degrade the
	// needs_rebinding gate to unset and let the environment default run.
	// Corruption detection, not authenticity: like every check here, this
	// proves the cut is coherent, never that the bundle is genuine.
	{"model_intent_malformed", `
		SELECT count(*) FROM core_personas p
		WHERE p.persona_id = $1 AND p.model_intent IS NOT NULL AND (
			jsonb_typeof(p.model_intent) IS DISTINCT FROM 'object'
			OR jsonb_typeof(p.model_intent->'kind') IS DISTINCT FROM 'string'
			OR p.model_intent->>'kind' NOT IN ('none','api','chatgpt'))`},
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
