package portable

import (
	"context"
	"fmt"
	"strings"
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
	// to a carried same-persona chunk, all in one layer, and the target's
	// journal range must be exactly its sources' span. A crafted bundle that
	// dangles a source reference, mixes layers, or claims a range beyond its
	// sources would activate a fragment that cannot be read back or checked.
	{"memory_chunk_sources_invalid", `
		SELECT count(*) FROM core_memory_chunks c
		WHERE c.persona_id = $1 AND (
			(c.layer = 1 AND c.sources IS NOT NULL)
			OR (c.layer >= 2 AND (
				c.sources IS NULL OR cardinality(c.sources) = 0
				OR EXISTS (SELECT 1 FROM unnest(c.sources) AS s(seq)
					WHERE NOT EXISTS (SELECT 1 FROM core_memory_chunks s2
						WHERE s2.persona_id = c.persona_id AND s2.chunk_seq = s.seq))
				OR (SELECT count(DISTINCT s2.layer) FROM core_memory_chunks s2
					WHERE s2.persona_id = c.persona_id
					AND s2.chunk_seq = ANY(c.sources)) <> 1
				OR (SELECT min(s2.first_seq) FROM core_memory_chunks s2
					WHERE s2.persona_id = c.persona_id
					AND s2.chunk_seq = ANY(c.sources)) <> c.first_seq
				OR (SELECT max(s2.last_seq) FROM core_memory_chunks s2
					WHERE s2.persona_id = c.persona_id
					AND s2.chunk_seq = ANY(c.sources)) <> c.last_seq
			)))`},
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
	return out, nil
}

func describe(vs []Violation) string {
	parts := make([]string, len(vs))
	for i, v := range vs {
		parts[i] = fmt.Sprintf("%s (%d)", v.Check, v.Rows)
	}
	return strings.Join(parts, ", ")
}
