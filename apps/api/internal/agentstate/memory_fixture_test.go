package agentstate

// Fixture drivers for stored chunk layouts in seal/apply/portability tests.
// Runtime preparation uses only the revisioned MemoryBranch API.
import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"strings"
	"time"
)

const memoryChunkMaxAttempts = 3
const memoryChunkMaxInterruptions = 8
const memoryReshelvePacing = 200 * time.Millisecond

// ClaimedMemoryChunk is a chunk plus everything the preparation branch needs:
// the covered events verbatim (layer-1 target) or the selected source
// fragments verbatim (upper-layer target), and the rendered parent context
// at claim time.
type ClaimedMemoryChunk struct {
	Chunk *MemoryChunk `json:"chunk"`
	// TargetEvents is the sealed journal range's stored events — set for a
	// layer-1 target, empty for an upper-layer one.
	TargetEvents []Event `json:"target_events"`
	// TargetFragments is the selected source fragments' accepted texts with
	// their locators — set for an upper-layer target, empty for layer 1.
	TargetFragments []MemoryBlock   `json:"target_fragments"`
	Context         RenderedContext `json:"context"`
}

// interruptPreparing returns 'preparing' chunks whose claim ended without a
// recorded outcome to the shelf. The originals never left the context while
// preparation ran, so nothing is lost; the interruption is counted apart
// from attempts and paced by backoff (200ms doubling, capped at 30s). Once
// memoryChunkMaxInterruptions is reached the chunk becomes 'failed' —
// visible, originals kept. exceptGeneration, when set, leaves that
// generation's claims alone (the live writer's own branch).
func interruptPreparing(ctx context.Context, tx pgx.Tx, personaID string, exceptGeneration *int64) error {
	args := []any{personaID, memoryChunkMaxInterruptions,
		fmt.Sprintf("preparation was interrupted %d times without a recorded outcome", memoryChunkMaxInterruptions)}
	scope := ""
	if exceptGeneration != nil {
		scope = " AND claimed_generation <> $4"
		args = append(args, *exceptGeneration)
	}
	_, err := tx.Exec(ctx, `
		UPDATE core_memory_chunks SET
			interruptions = interruptions + 1,
			claimed_generation = NULL,
			claimed_at = NULL,
			status = CASE WHEN interruptions + 1 >= $2 THEN 'failed' ELSE 'sealed' END,
			not_before = CASE WHEN interruptions + 1 >= $2 THEN NULL
				ELSE now() + LEAST(200 * power(2, LEAST(interruptions, 8)), 30000) * interval '1 millisecond' END,
			last_error = CASE WHEN interruptions + 1 >= $2
				THEN concat_ws('; ', last_error, $3::text) ELSE last_error END
		WHERE persona_id = $1 AND status = 'preparing' AND NOT EXISTS (SELECT 1 FROM core_memory_branches b WHERE b.persona_id=core_memory_chunks.persona_id AND b.chunk_seq=core_memory_chunks.chunk_seq)`+scope, args...)
	return err
}

// ClaimMemoryChunk claims the oldest sealable chunk for preparation — one
// branch at a time. The returned context is the rendered parent context at
// claim time (the same view a turn would see), so the branch inherits the
// parent's context rather than a target-only summary input. A claim spends
// nothing: attempts count recorded failures only.
func (s *Store) ClaimMemoryChunk(ctx context.Context, personaID string, generation int64, contextLimit int) (*ClaimedMemoryChunk, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := requireGeneration(ctx, tx, personaID, generation); err != nil {
		return nil, err
	}
	// The caller holds the live generation and claims only while it runs no
	// branch of its own, so any 'preparing' row is orphaned: a dead
	// generation's claim, a lost claim response, or a branch stopped before
	// it recorded an outcome. It is counted as an interruption and paced —
	// never as a failed attempt. This is convergence, not serialization:
	// the scan and the claim are separate statements, so concurrent
	// same-generation callers can both see an empty shelf and both claim —
	// transiently two 'preparing' rows, never the same target. The pacing
	// and generation fencing, not single-flight, are what the lifecycle
	// relies on.
	if err := interruptPreparing(ctx, tx, personaID, nil); err != nil {
		return nil, err
	}
	c, err := scanChunk(tx.QueryRow(ctx, `
		UPDATE core_memory_chunks
		SET status = 'preparing', claimed_generation = $2, claimed_at = now(), not_before = NULL
		WHERE (persona_id, chunk_seq) = (
			SELECT persona_id, chunk_seq FROM core_memory_chunks
			WHERE persona_id = $1 AND status = 'sealed'
				AND (not_before IS NULL OR not_before <= statement_timestamp())
			ORDER BY chunk_seq LIMIT 1 FOR UPDATE)
		RETURNING `+chunkCols, personaID, generation))
	if errors.Is(err, ErrChunkNotFound) {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return &ClaimedMemoryChunk{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim memory chunk: %w", dataErr(err))
	}
	claimed := &ClaimedMemoryChunk{Chunk: &c}
	if c.Layer >= 2 {
		// An upper-layer target's input is its selected sources' accepted
		// texts with their locators — not raw events. The sources must all
		// still be applied: a stale target — a crafted or carried row, or a
		// target whose sources a different applied target consumed while it
		// waited — is marked failed without spending attempts rather than
		// prepared from a different source set.
		frows, err := tx.Query(ctx, `
			SELECT s.chunk_seq, s.layer, s.first_seq, s.last_seq,
				f.created_at, l.created_at, s.replacement, s.replacement_est_tokens, s.status
			FROM core_memory_chunks s
			JOIN core_events f ON f.persona_id = s.persona_id AND f.seq = s.first_seq
			JOIN core_events l ON l.persona_id = s.persona_id AND l.seq = s.last_seq
			WHERE s.persona_id = $1 AND s.chunk_seq = ANY($2)
			ORDER BY s.first_seq`, personaID, c.Sources)
		if err != nil {
			return nil, err
		}
		stale := false
		var frags []MemoryBlock
		for frows.Next() {
			var b MemoryBlock
			var status string
			if err := frows.Scan(&b.ChunkSeq, &b.Layer, &b.FirstSeq, &b.LastSeq,
				&b.FirstTime, &b.LastTime, &b.Text, &b.EstTokens, &status); err != nil {
				frows.Close()
				return nil, err
			}
			if status != "applied" {
				stale = true
			}
			frags = append(frags, b)
		}
		frows.Close()
		if err := frows.Err(); err != nil {
			return nil, err
		}
		if stale || len(frags) != len(c.Sources) {
			if _, err := tx.Exec(ctx, `
				UPDATE core_memory_chunks SET status = 'failed', claimed_generation = NULL,
					claimed_at = NULL,
					last_error = 'upper-layer target is stale: its selected sources are no longer applied'
				WHERE persona_id = $1 AND chunk_seq = $2`,
				personaID, c.ChunkSeq); err != nil {
				return nil, err
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, err
			}
			return &ClaimedMemoryChunk{}, nil
		}
		claimed.TargetFragments = frags
	} else {
		events, err := s.eventsInRange(ctx, tx, personaID, c.FirstSeq, c.LastSeq)
		if err != nil {
			return nil, err
		}
		claimed.TargetEvents = events
	}
	rc, err := s.renderedContext(ctx, tx, personaID, contextLimit, "")
	if err != nil {
		return nil, err
	}
	claimed.Context = rc
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return claimed, nil
}

// CompleteMemoryChunk shelves the finished replacement candidate. The result
// is stored and waits — completion alone never inserts it into the sent
// context or removes the originals. keep_unchanged is the model's
// KEEP_UNCHANGED decision: the originals are kept and the chunk is never
// reprepared. A replacement that does not shrink the range is kept the same
// way (design: KEEP_UNCHANGED, a non-shrinking result, or failure keep the
// originals), with the text stored for inspection. A replayed identical
// complete returns the stored row.
func (s *Store) CompleteMemoryChunk(ctx context.Context, personaID string, generation int64, chunkSeq int64, replacement string, keepUnchanged bool) (*MemoryChunk, error) {
	if keepUnchanged == (replacement != "") {
		return nil, fmt.Errorf("%w: exactly one of replacement text or keep_unchanged is required", ErrBadRequest)
	}
	if hasNUL(replacement) {
		return nil, fmt.Errorf("%w: replacement contains a NUL byte the store cannot hold", ErrBadRequest)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := requireGeneration(ctx, tx, personaID, generation); err != nil {
		return nil, err
	}
	c, err := scanChunk(tx.QueryRow(ctx,
		`SELECT `+chunkCols+` FROM core_memory_chunks WHERE persona_id = $1 AND chunk_seq = $2 FOR UPDATE`,
		personaID, chunkSeq))
	if err != nil {
		return nil, err
	}
	switch {
	case c.Status == "preparing":
		if c.ClaimedGeneration == nil || *c.ClaimedGeneration != generation {
			return nil, ErrGenerationFence
		}
		rest := estTextTokens(replacement)
		switch {
		case keepUnchanged:
			err = tx.QueryRow(ctx, `
				UPDATE core_memory_chunks SET status = 'kept', claimed_generation = NULL,
					claimed_at = NULL, prepared_at = now()
				WHERE persona_id = $1 AND chunk_seq = $2 RETURNING chunk_seq`,
				personaID, chunkSeq).Scan(&c.ChunkSeq)
		case rest >= c.EstTokens:
			err = tx.QueryRow(ctx, `
				UPDATE core_memory_chunks SET status = 'kept', replacement = $3,
					replacement_est_tokens = $4, claimed_generation = NULL, claimed_at = NULL,
					prepared_at = now(), last_error = $5
				WHERE persona_id = $1 AND chunk_seq = $2 RETURNING chunk_seq`,
				personaID, chunkSeq, replacement, rest,
				fmt.Sprintf("replacement did not shrink the range (%d >= %d estimated tokens); originals kept", rest, c.EstTokens)).
				Scan(&c.ChunkSeq)
		default:
			err = tx.QueryRow(ctx, `
				UPDATE core_memory_chunks SET status = 'prepared', replacement = $3,
					replacement_est_tokens = $4, claimed_generation = NULL, claimed_at = NULL,
					prepared_at = now()
				WHERE persona_id = $1 AND chunk_seq = $2 RETURNING chunk_seq`,
				personaID, chunkSeq, replacement, rest).Scan(&c.ChunkSeq)
		}
		if err != nil {
			return nil, fmt.Errorf("complete memory chunk: %w", dataErr(err))
		}
	case c.Status == "prepared", c.Status == "kept", c.Status == "applied":
		// Lost-response replay: identical content returns the stored row; a
		// different answer for an already-resolved chunk conflicts. A kept
		// chunk that stored a non-shrinking replacement replays that text.
		same := (keepUnchanged && c.Status == "kept" && c.Replacement == nil) ||
			(!keepUnchanged && c.Replacement != nil && *c.Replacement == replacement)
		if !same {
			return nil, fmt.Errorf("%w: chunk %d already completed with different content", ErrMemoryConflict, chunkSeq)
		}
	default:
		return nil, fmt.Errorf("%w: chunk %d is %s, not preparing", ErrMemoryConflict, chunkSeq, c.Status)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return s.chunk(ctx, s.pool, personaID, chunkSeq)
}

// FailMemoryChunk records a failed preparation attempt — the only thing that
// spends attempts. A retryable failure returns the chunk to 'sealed' with a
// backoff while attempts remain; a non-retryable failure or an exhausted
// budget marks it 'failed' — visible, originals kept — rather than silently
// skipped.
func (s *Store) FailMemoryChunk(ctx context.Context, personaID string, generation int64, chunkSeq int64, errText string, retryable bool) (*MemoryChunk, error) {
	errText = strings.ReplaceAll(errText, "\x00", "")
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := requireGeneration(ctx, tx, personaID, generation); err != nil {
		return nil, err
	}
	c, err := scanChunk(tx.QueryRow(ctx,
		`SELECT `+chunkCols+` FROM core_memory_chunks WHERE persona_id = $1 AND chunk_seq = $2 FOR UPDATE`,
		personaID, chunkSeq))
	if err != nil {
		return nil, err
	}
	if c.Status != "preparing" || c.ClaimedGeneration == nil || *c.ClaimedGeneration != generation {
		return nil, fmt.Errorf("%w: chunk %d is not preparing under this generation", ErrMemoryConflict, chunkSeq)
	}
	attempts := c.Attempts + 1
	if retryable && attempts < memoryChunkMaxAttempts {
		delay := retryBackoff(attempts)
		if _, err := tx.Exec(ctx, `
			UPDATE core_memory_chunks SET status = 'sealed', attempts = $5,
				claimed_generation = NULL, claimed_at = NULL,
				last_error = $3, not_before = now() + $4 * interval '1 millisecond'
			WHERE persona_id = $1 AND chunk_seq = $2`,
			personaID, chunkSeq, errText, delay.Milliseconds(), attempts); err != nil {
			return nil, err
		}
	} else {
		if _, err := tx.Exec(ctx, `
			UPDATE core_memory_chunks SET status = 'failed', attempts = $4,
				claimed_generation = NULL, claimed_at = NULL,
				last_error = $3, not_before = NULL
			WHERE persona_id = $1 AND chunk_seq = $2`,
			personaID, chunkSeq, errText, attempts); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return s.chunk(ctx, s.pool, personaID, chunkSeq)
}

// ReshelveMemoryChunk returns a claimed chunk to the shelf when no model
// request could be evaluated — an unbound or deleted selection, a missing
// credential, a binding-lookup outage, or a budget-denied admission. No
// verdict was evaluated, so the claim spends neither an attempt nor an
// interruption; the reason is kept on last_error for visibility. delayMs
// paces the next claim beyond the default tick — a budget wait uses a
// slower cadence because only a funding change can unblock it.
func (s *Store) ReshelveMemoryChunk(ctx context.Context, personaID string, generation int64, chunkSeq int64, reason string, delayMs int64) (*MemoryChunk, error) {
	reason = strings.ReplaceAll(reason, "\x00", "")
	delay := time.Duration(delayMs) * time.Millisecond
	if delay <= 0 {
		delay = memoryReshelvePacing
	}
	if delay > 10*time.Minute {
		delay = 10 * time.Minute
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := requireGeneration(ctx, tx, personaID, generation); err != nil {
		return nil, err
	}
	c, err := scanChunk(tx.QueryRow(ctx,
		`SELECT `+chunkCols+` FROM core_memory_chunks WHERE persona_id = $1 AND chunk_seq = $2 FOR UPDATE`,
		personaID, chunkSeq))
	if err != nil {
		return nil, err
	}
	if c.Status != "preparing" || c.ClaimedGeneration == nil || *c.ClaimedGeneration != generation {
		return nil, fmt.Errorf("%w: chunk %d is not preparing under this generation", ErrMemoryConflict, chunkSeq)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE core_memory_chunks SET status = 'sealed',
			claimed_generation = NULL, claimed_at = NULL,
			last_error = $3, not_before = now() + $4 * interval '1 millisecond'
		WHERE persona_id = $1 AND chunk_seq = $2`,
		personaID, chunkSeq, reason, delay.Milliseconds()); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return s.chunk(ctx, s.pool, personaID, chunkSeq)
}
