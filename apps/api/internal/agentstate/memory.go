// Memory layer for the shared secretary core: sealed journal chunks,
// asynchronous L1 preparation, and in-place application.
//
// The canonical life log is core_events — journal rows are never deleted or
// rewritten by memory maintenance. A chunk seals a contiguous journal prefix
// range [first_seq, last_seq] at a safe boundary (just before a later
// input_received event, never inside an unresolved tool flow). Preparation is
// a separate durable state: a claimed chunk is 'preparing' under one writer
// generation while the model produces a replacement candidate on a branch
// with the parent's context. The candidate is shelved ('prepared') and only
// rendered once the live raw estimate exceeds L0LiveLimitTokens — at which
// point the oldest prepared chunks are 'applied' in order until the estimate
// drops back under the limit. Events appended after a chunk's last_seq —
// corrections and new experiences that arrived during preparation — are
// outside the sealed range and are never touched by application.
//
// Thresholds follow docs/agent/memory-preparation-and-replacement-2026-09-08:
// seal at >=10k estimated tokens, replace when live raw exceeds 40k. The
// estimate is an internal capacity heuristic, not provider billing.
package agentstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	// L0ChunkMinTokens: a normal L0 chunk seals once the open accumulation
	// reaches this internal estimate at a safe boundary.
	L0ChunkMinTokens int64 = 10_000
	// L0LiveLimitTokens: prepared replacements apply only while the live raw
	// estimate (unapplied chunks + unsealed tail) exceeds this.
	L0LiveLimitTokens int64 = 40_000
	// L0SendCapTokens bounds the raw records rendered into one sent context.
	// Raw records stay in the context by capacity, not by record count: with
	// nothing prepared, originals keep rendering past L0LiveLimitTokens (the
	// conversation never waits for preparation), and only beyond this cap do
	// the oldest raw records leave the sent context — reported as an omitted
	// range, never deleted or summarized. The headroom over the live limit
	// absorbs preparation lag; with applied blocks and the system prompt it
	// keeps a normal context near the ~80k design target.
	L0SendCapTokens int64 = 60_000
	// contextMaxEvents is a defensive row bound on the rendered raw window;
	// the token cap is the normal bound.
	contextMaxEvents = 5_000
	// memoryChunkMaxAttempts bounds automatic preparation attempts per chunk.
	// Every claim is an attempt, including one interrupted by a stop or a
	// lost claim response. A chunk that exhausts them becomes 'failed' —
	// visible in status, its originals still live — rather than silently
	// skipped or retried forever.
	memoryChunkMaxAttempts = 3
)

// ErrMemoryConflict marks a memory-state contract violation (HTTP 409).
var ErrMemoryConflict = errors.New("conflicting memory state")

// ErrChunkNotFound marks a missing memory chunk (HTTP 404).
var ErrChunkNotFound = errors.New("memory chunk not found")

// estPayloadTokens is the internal capacity estimate for one journal record:
// ~4 bytes per token over the stored JSON plus a small per-record cost. It is
// deliberately not provider billing — thresholds and hysteresis are tuned
// against it, and calibration stays a separate decision (plan D6).
func estPayloadTokens(kind string, payload map[string]any) int64 {
	n := int64(len(kind) + 16)
	if raw, err := json.Marshal(payload); err == nil {
		n += int64(len(raw))
	}
	return (n + 3) / 4
}

// estTextTokens estimates a stored replacement text for the same internal
// capacity accounting as estPayloadTokens.
func estTextTokens(text string) int64 {
	return (int64(len(text)) + 3) / 4
}

// MemoryChunk is one sealed journal range and its L1 replacement lifecycle.
type MemoryChunk struct {
	PersonaID            string     `json:"persona_id"`
	ChunkSeq             int64      `json:"chunk_seq"`
	Layer                int        `json:"layer"`
	FirstSeq             int64      `json:"first_seq"`
	LastSeq              int64      `json:"last_seq"`
	EstTokens            int64      `json:"est_tokens"`
	Status               string     `json:"status"`
	Replacement          *string    `json:"replacement"`
	ReplacementEstTokens *int64     `json:"replacement_est_tokens"`
	Attempts             int        `json:"attempts"`
	LastError            *string    `json:"last_error"`
	ClaimedGeneration    *int64     `json:"claimed_generation"`
	NotBefore            *time.Time `json:"not_before"`
	CreatedAt            time.Time  `json:"created_at"`
	PreparedAt           *time.Time `json:"prepared_at"`
	AppliedAt            *time.Time `json:"applied_at"`
}

// MemoryBlock is an applied chunk as it appears in the sent context: the
// replacement text rendered at the position where its events were.
type MemoryBlock struct {
	ChunkSeq  int64  `json:"chunk_seq"`
	Layer     int    `json:"layer"`
	FirstSeq  int64  `json:"first_seq"`
	LastSeq   int64  `json:"last_seq"`
	Text      string `json:"text"`
	EstTokens int64  `json:"est_tokens"`
}

// RenderedContext is the journal as the model sees it: events not covered by
// an applied chunk, plus every applied block. The caller interleaves blocks
// at their original positions.
type RenderedContext struct {
	Events []Event       `json:"events"`
	Memory []MemoryBlock `json:"memory"`
	// Omitted describes older raw records that are neither applied memory
	// nor inside the send cap. They remain in the journal and readable
	// through conversation_history; nil when nothing was left out.
	Omitted *OmittedRange `json:"omitted"`
}

// OmittedRange is the extent of raw records outside the sent context.
type OmittedRange struct {
	Count     int64     `json:"count"`
	FirstSeq  int64     `json:"first_seq"`
	LastSeq   int64     `json:"last_seq"`
	FirstTime time.Time `json:"first_time"`
	LastTime  time.Time `json:"last_time"`
}

// MemoryStatus reports the memory layer's current shape for observability
// and for the writer's preparation scheduling.
type MemoryStatus struct {
	// LiveRawTokens estimates the raw records still in the sent context:
	// unapplied chunks (sealed through failed/kept) plus the unsealed tail.
	LiveRawTokens int64 `json:"live_raw_tokens"`
	// AppliedTokens estimates the replacement texts currently rendered.
	AppliedTokens   int64 `json:"applied_tokens"`
	Sealed          int   `json:"sealed"`
	Preparing       int   `json:"preparing"`
	Prepared        int   `json:"prepared"`
	Applied         int   `json:"applied"`
	Kept            int   `json:"kept"`
	Failed          int   `json:"failed"`
	CoveredSeq      int64 `json:"covered_seq"`
	LatestSeq       int64 `json:"latest_seq"`
	ChunkMinTokens  int64 `json:"chunk_min_tokens"`
	LiveLimitTokens int64 `json:"live_limit_tokens"`
}

// ClaimedMemoryChunk is a chunk plus everything the preparation branch needs:
// the covered events verbatim and the rendered parent context at claim time.
type ClaimedMemoryChunk struct {
	Chunk        *MemoryChunk    `json:"chunk"`
	TargetEvents []Event         `json:"target_events"`
	Context      RenderedContext `json:"context"`
}

var chunkCols = `persona_id, chunk_seq, layer, first_seq, last_seq, est_tokens,
	status, replacement, replacement_est_tokens, attempts, last_error,
	claimed_generation, not_before, created_at, prepared_at, applied_at`

func scanChunk(row interface{ Scan(...any) error }) (MemoryChunk, error) {
	var c MemoryChunk
	err := row.Scan(&c.PersonaID, &c.ChunkSeq, &c.Layer, &c.FirstSeq, &c.LastSeq,
		&c.EstTokens, &c.Status, &c.Replacement, &c.ReplacementEstTokens,
		&c.Attempts, &c.LastError, &c.ClaimedGeneration, &c.NotBefore,
		&c.CreatedAt, &c.PreparedAt, &c.AppliedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, ErrChunkNotFound
	}
	return c, err
}

func (s *Store) chunk(ctx context.Context, db queryRower, personaID string, chunkSeq int64) (*MemoryChunk, error) {
	c, err := scanChunk(db.QueryRow(ctx,
		`SELECT `+chunkCols+` FROM core_memory_chunks WHERE persona_id = $1 AND chunk_seq = $2`,
		personaID, chunkSeq))
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// eventsInRange returns journal rows in [fromSeq, toSeq] ascending.
func (s *Store) eventsInRange(ctx context.Context, db interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, personaID string, fromSeq, toSeq int64) ([]Event, error) {
	rows, err := db.Query(ctx, `
		SELECT persona_id, seq, turn_id, kind, payload, created_at
		FROM core_events WHERE persona_id = $1 AND seq >= $2 AND seq <= $3
		ORDER BY seq`, personaID, fromSeq, toSeq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Event{}
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.PersonaID, &e.Seq, &e.TurnID, &e.Kind, &e.Payload, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// renderedContext returns the journal as the model sees it: the newest raw
// events not covered by an applied chunk, up to L0SendCapTokens (and the
// caller's row bound), plus every applied block. Applied blocks are returned
// regardless of the raw window — compacted memory stays in the context even
// when its original range is older than the raw window. The newest record is
// always included, even alone over the cap.
func (s *Store) renderedContext(ctx context.Context, db interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}, personaID string, limit int) (RenderedContext, error) {
	var rc RenderedContext
	limit = clampLimit(limit, contextMaxEvents, contextMaxEvents)
	rows, err := db.Query(ctx, `
		SELECT persona_id, seq, turn_id, kind, payload, created_at
		FROM core_events e
		WHERE e.persona_id = $1
			AND NOT EXISTS (
				SELECT 1 FROM core_memory_chunks c
				WHERE c.persona_id = e.persona_id AND c.status = 'applied'
					AND e.seq BETWEEN c.first_seq AND c.last_seq)
		ORDER BY seq DESC LIMIT $2`, personaID, limit+1)
	if err != nil {
		return rc, err
	}
	var newestFirst []Event
	var budget int64
	truncated := false
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.PersonaID, &e.Seq, &e.TurnID, &e.Kind, &e.Payload, &e.CreatedAt); err != nil {
			rows.Close()
			return rc, err
		}
		est := estPayloadTokens(e.Kind, e.Payload)
		if len(newestFirst) >= limit || (len(newestFirst) > 0 && budget+est > L0SendCapTokens) {
			truncated = true
			break
		}
		budget += est
		newestFirst = append(newestFirst, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return rc, err
	}
	rc.Events = make([]Event, len(newestFirst))
	for i, e := range newestFirst {
		rc.Events[len(newestFirst)-1-i] = e
	}
	if truncated && len(rc.Events) > 0 {
		var om OmittedRange
		if err := db.QueryRow(ctx, `
			SELECT COUNT(*), MIN(seq), MAX(seq), MIN(created_at), MAX(created_at)
			FROM core_events e
			WHERE e.persona_id = $1 AND e.seq < $2
				AND NOT EXISTS (
					SELECT 1 FROM core_memory_chunks c
					WHERE c.persona_id = e.persona_id AND c.status = 'applied'
						AND e.seq BETWEEN c.first_seq AND c.last_seq)`,
			personaID, rc.Events[0].Seq).Scan(&om.Count, &om.FirstSeq, &om.LastSeq,
			&om.FirstTime, &om.LastTime); err != nil {
			return rc, err
		}
		rc.Omitted = &om
	}
	rc.Memory, err = s.appliedBlocks(ctx, db, personaID)
	return rc, err
}

func (s *Store) appliedBlocks(ctx context.Context, db interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, personaID string) ([]MemoryBlock, error) {
	rows, err := db.Query(ctx, `
		SELECT chunk_seq, layer, first_seq, last_seq, replacement, replacement_est_tokens
		FROM core_memory_chunks
		WHERE persona_id = $1 AND status = 'applied'
		ORDER BY chunk_seq`, personaID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []MemoryBlock{}
	for rows.Next() {
		var b MemoryBlock
		if err := rows.Scan(&b.ChunkSeq, &b.Layer, &b.FirstSeq, &b.LastSeq, &b.Text, &b.EstTokens); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// MemoryMaintain is the writer's memory housekeeping step: seal newly safe
// journal ranges, then apply prepared replacements while the live raw
// estimate exceeds the limit. It runs between turns under the writer
// generation so application never interleaves with an in-flight model call.
func (s *Store) MemoryMaintain(ctx context.Context, personaID string, generation int64) (MemoryStatus, error) {
	var st MemoryStatus
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return st, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := requireGeneration(ctx, tx, personaID, generation); err != nil {
		return st, err
	}

	// A 'preparing' chunk claimed by a fenced generation belonged to a dead
	// writer — return it to the shelf so the live generation can reprepare.
	if _, err := tx.Exec(ctx, `
		UPDATE core_memory_chunks SET status = 'sealed', claimed_generation = NULL
		WHERE persona_id = $1 AND status = 'preparing' AND claimed_generation <> $2`,
		personaID, generation); err != nil {
		return st, err
	}

	var covered int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(last_seq), 0) FROM core_memory_chunks WHERE persona_id = $1`,
		personaID).Scan(&covered); err != nil {
		return st, err
	}
	var chunkSeq int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(chunk_seq), 0) FROM core_memory_chunks WHERE persona_id = $1`,
		personaID).Scan(&chunkSeq); err != nil {
		return st, err
	}

	// Seal walk: accumulate the unsealed tail; cut a chunk just before each
	// input_received once the accumulation reaches the minimum and no tool
	// call in the window is still waiting for its result. Tool calls and
	// results commit inside one turn transaction, so a dangling call can
	// never sit at a turn boundary — the pending set is defensive depth.
	rows, err := tx.Query(ctx, `
		SELECT seq, kind, payload FROM core_events
		WHERE persona_id = $1 AND seq > $2 ORDER BY seq`, personaID, covered)
	if err != nil {
		return st, err
	}
	type evRow struct {
		seq     int64
		kind    string
		payload map[string]any
	}
	var tail []evRow
	for rows.Next() {
		var e evRow
		if err := rows.Scan(&e.seq, &e.kind, &e.payload); err != nil {
			rows.Close()
			return st, err
		}
		tail = append(tail, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return st, err
	}
	var window []evRow
	pending := map[string]bool{}
	seal := func(first, lastSeq, est int64) error {
		chunkSeq++
		_, err := tx.Exec(ctx, `
			INSERT INTO core_memory_chunks
				(persona_id, chunk_seq, first_seq, last_seq, est_tokens, status)
			VALUES ($1, $2, $3, $4, $5, 'sealed')`,
			personaID, chunkSeq, first, lastSeq, est)
		return err
	}
	var windowEst int64
	var windowStart int64 = -1
	for _, e := range tail {
		// Boundary check happens BEFORE the event joins the window: the
		// input itself opens the next chunk.
		if e.kind == "input_received" && windowStart >= 0 && len(pending) == 0 &&
			windowEst >= L0ChunkMinTokens {
			last := window[len(window)-1].seq
			if err := seal(windowStart, last, windowEst); err != nil {
				rows.Close()
				return st, err
			}
			window = window[:0]
			windowEst = 0
			windowStart = -1
		}
		if windowStart < 0 {
			windowStart = e.seq
		}
		window = append(window, e)
		windowEst += estPayloadTokens(e.kind, e.payload)
		switch e.kind {
		case "tool_call":
			if id, ok := e.payload["call_id"].(string); ok && id != "" {
				pending[id] = true
			}
		case "tool_result":
			if id, ok := e.payload["call_id"].(string); ok {
				delete(pending, id)
			}
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return st, err
	}
	// The unsealed remainder is the live tail: it is never sealed without a
	// following input boundary, and it contributes to the raw estimate.
	tailEst := windowEst

	// Live raw = every not-yet-applied chunk (its originals still render)
	// plus the unsealed tail. Applied chunks contribute their replacement
	// estimate to the compacted portion instead.
	var chunkRaw int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(SUM(est_tokens), 0) FROM core_memory_chunks
		WHERE persona_id = $1 AND status <> 'applied'`, personaID).Scan(&chunkRaw); err != nil {
		return st, err
	}
	live := chunkRaw + tailEst
	if live > L0LiveLimitTokens {
		// Apply the oldest prepared chunks until the estimate is back under
		// the limit or the shelf is empty. Applying a later prepared chunk
		// while an older one is still unfinished is allowed — the
		// replacement renders at its own original position and unfinished
		// originals stay.
		prows, err := tx.Query(ctx, `
			SELECT chunk_seq, est_tokens, COALESCE(replacement_est_tokens, 0)
			FROM core_memory_chunks
			WHERE persona_id = $1 AND status = 'prepared'
			ORDER BY chunk_seq FOR UPDATE`, personaID)
		if err != nil {
			return st, err
		}
		type cand struct {
			seq, est, rest int64
		}
		var cands []cand
		for prows.Next() {
			var c cand
			if err := prows.Scan(&c.seq, &c.est, &c.rest); err != nil {
				prows.Close()
				return st, err
			}
			cands = append(cands, c)
		}
		prows.Close()
		if err := prows.Err(); err != nil {
			return st, err
		}
		for _, c := range cands {
			if live <= L0LiveLimitTokens {
				break
			}
			if _, err := tx.Exec(ctx, `
				UPDATE core_memory_chunks SET status = 'applied', applied_at = now()
				WHERE persona_id = $1 AND chunk_seq = $2 AND status = 'prepared'`,
				personaID, c.seq); err != nil {
				return st, err
			}
			live -= c.est
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return st, err
	}
	return s.MemoryStatus(ctx, personaID)
}

// MemoryStatus reads the current memory shape without mutating it.
func (s *Store) MemoryStatus(ctx context.Context, personaID string) (MemoryStatus, error) {
	st := MemoryStatus{
		ChunkMinTokens:  L0ChunkMinTokens,
		LiveLimitTokens: L0LiveLimitTokens,
	}
	err := s.pool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(est_tokens) FILTER (WHERE status <> 'applied'), 0),
			COALESCE(SUM(replacement_est_tokens) FILTER (WHERE status = 'applied'), 0),
			COUNT(*) FILTER (WHERE status = 'sealed'),
			COUNT(*) FILTER (WHERE status = 'preparing'),
			COUNT(*) FILTER (WHERE status = 'prepared'),
			COUNT(*) FILTER (WHERE status = 'applied'),
			COUNT(*) FILTER (WHERE status = 'kept'),
			COUNT(*) FILTER (WHERE status = 'failed'),
			COALESCE(MAX(last_seq), 0)
		FROM core_memory_chunks WHERE persona_id = $1`, personaID).
		Scan(&st.LiveRawTokens, &st.AppliedTokens, &st.Sealed, &st.Preparing,
			&st.Prepared, &st.Applied, &st.Kept, &st.Failed, &st.CoveredSeq)
	if err != nil {
		return st, err
	}
	var tail int64
	if err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(seq), 0) FROM core_events WHERE persona_id = $1`,
		personaID).Scan(&st.LatestSeq); err != nil {
		return st, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT kind, payload FROM core_events
		WHERE persona_id = $1 AND seq > $2 ORDER BY seq`, personaID, st.CoveredSeq)
	if err != nil {
		return st, err
	}
	for rows.Next() {
		var kind string
		var payload map[string]any
		if err := rows.Scan(&kind, &payload); err != nil {
			rows.Close()
			return st, err
		}
		tail += estPayloadTokens(kind, payload)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return st, err
	}
	st.LiveRawTokens += tail
	return st, nil
}

// ClaimMemoryChunk claims the oldest sealable chunk for preparation — one
// branch at a time. The returned context is the rendered parent context at
// claim time (the same view a turn would see), so the branch inherits the
// parent's context rather than a target-only summary input.
func (s *Store) ClaimMemoryChunk(ctx context.Context, personaID string, generation int64, contextLimit int) (*ClaimedMemoryChunk, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := requireGeneration(ctx, tx, personaID, generation); err != nil {
		return nil, err
	}
	var c MemoryChunk
	for {
		// A 'preparing' chunk is re-claimed before any sealed one. The caller
		// holds the live generation, so no other writer runs a branch, and
		// the core claims only while it has no branch of its own — any
		// 'preparing' row is therefore orphaned: a dead generation's claim, a
		// lost claim response, or a branch stopped mid-preparation. At most
		// one row is ever 'preparing', so one branch at a time still holds.
		cand, err := scanChunk(tx.QueryRow(ctx, `
			SELECT `+chunkCols+` FROM core_memory_chunks
			WHERE persona_id = $1 AND (status = 'preparing'
				OR (status = 'sealed' AND (not_before IS NULL OR not_before <= now())))
			ORDER BY status = 'preparing' DESC, chunk_seq
			LIMIT 1 FOR UPDATE`, personaID))
		if errors.Is(err, ErrChunkNotFound) {
			if err := tx.Commit(ctx); err != nil {
				return nil, err
			}
			return &ClaimedMemoryChunk{}, nil
		}
		if err != nil {
			return nil, err
		}
		if cand.Attempts >= memoryChunkMaxAttempts {
			// Interrupted attempts spent the budget without a recorded
			// outcome: terminal and visible, originals kept.
			if _, err := tx.Exec(ctx, `
				UPDATE core_memory_chunks SET status = 'failed', claimed_generation = NULL,
					not_before = NULL, last_error = concat_ws('; ', last_error, $3::text)
				WHERE persona_id = $1 AND chunk_seq = $2`,
				personaID, cand.ChunkSeq,
				fmt.Sprintf("preparation did not finish within %d attempts", memoryChunkMaxAttempts)); err != nil {
				return nil, err
			}
			continue
		}
		c, err = scanChunk(tx.QueryRow(ctx, `
			UPDATE core_memory_chunks
			SET status = 'preparing', claimed_generation = $3, attempts = attempts + 1,
				not_before = NULL
			WHERE persona_id = $1 AND chunk_seq = $2
			RETURNING `+chunkCols, personaID, cand.ChunkSeq, generation))
		if err != nil {
			return nil, fmt.Errorf("claim memory chunk: %w", dataErr(err))
		}
		break
	}
	events, err := s.eventsInRange(ctx, tx, personaID, c.FirstSeq, c.LastSeq)
	if err != nil {
		return nil, err
	}
	rc, err := s.renderedContext(ctx, tx, personaID, contextLimit)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &ClaimedMemoryChunk{Chunk: &c, TargetEvents: events, Context: rc}, nil
}

// CompleteMemoryChunk shelves the finished replacement candidate. The result
// is stored and waits — completion alone never inserts it into the sent
// context or removes the originals. keep_unchanged is the model's
// KEEP_UNCHANGED decision: the originals are kept and the chunk is never
// reprepared. A replayed identical complete returns the stored row.
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
		if keepUnchanged {
			err = tx.QueryRow(ctx, `
				UPDATE core_memory_chunks SET status = 'kept', claimed_generation = NULL,
					prepared_at = now()
				WHERE persona_id = $1 AND chunk_seq = $2 RETURNING chunk_seq`,
				personaID, chunkSeq).Scan(&c.ChunkSeq)
		} else {
			rest := estTextTokens(replacement)
			err = tx.QueryRow(ctx, `
				UPDATE core_memory_chunks SET status = 'prepared', replacement = $3,
					replacement_est_tokens = $4, claimed_generation = NULL, prepared_at = now()
				WHERE persona_id = $1 AND chunk_seq = $2 RETURNING chunk_seq`,
				personaID, chunkSeq, replacement, rest).Scan(&c.ChunkSeq)
		}
		if err != nil {
			return nil, fmt.Errorf("complete memory chunk: %w", dataErr(err))
		}
	case c.Status == "prepared", c.Status == "kept", c.Status == "applied":
		// Lost-response replay: identical content returns the stored row; a
		// different answer for an already-resolved chunk conflicts.
		same := keepUnchanged == (c.Status == "kept") &&
			((c.Replacement == nil && replacement == "") ||
				(c.Replacement != nil && *c.Replacement == replacement))
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

// FailMemoryChunk records a failed preparation attempt. A retryable failure
// returns the chunk to 'sealed' with a backoff while attempts remain; a
// non-retryable failure or an exhausted budget marks it 'failed' — visible,
// originals kept — rather than silently skipped.
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
	if retryable && c.Attempts < memoryChunkMaxAttempts {
		delay := retryBackoff(c.Attempts)
		if _, err := tx.Exec(ctx, `
			UPDATE core_memory_chunks SET status = 'sealed', claimed_generation = NULL,
				last_error = $3, not_before = now() + $4 * interval '1 millisecond'
			WHERE persona_id = $1 AND chunk_seq = $2`,
			personaID, chunkSeq, errText, delay.Milliseconds()); err != nil {
			return nil, err
		}
	} else {
		if _, err := tx.Exec(ctx, `
			UPDATE core_memory_chunks SET status = 'failed', claimed_generation = NULL,
				last_error = $3, not_before = NULL
			WHERE persona_id = $1 AND chunk_seq = $2`,
			personaID, chunkSeq, errText); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return s.chunk(ctx, s.pool, personaID, chunkSeq)
}
