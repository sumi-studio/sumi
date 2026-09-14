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
//
// Upper layers share the same pipeline (docs/agent/memory.md steps 4–5,
// memory-boundaries-2026-09-08). When applied L1 replacements exceed
// L1LimitTokens, the writer creates a sealed layer-2 target over the oldest
// contiguous applied L1 fragments; a branch replaces their combined text
// with one smaller L2 fragment, and the sources become 'superseded' — the
// accepted decisions and their texts stay durable, their ranges are
// represented by the applied target. Applied L2 beyond L2LimitTokens is
// reintegrated the same way: a target whose sources are L2 fragments may
// rearrange material within them, importing nothing from other layers.
// Selected sources alone supply a replacement — later corrections and
// fragments outside the selection are never folded in.
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
	// L0ForcedSealLimitTokens bounds one sealed chunk's target size: while
	// the open window exceeds it, the walk may cut at any safe boundary, not
	// only before a new input, so a single oversized committed turn still
	// becomes bounded preparation targets. It is a target, not a promise —
	// a single huge record or a tool group with no safe interior boundary
	// pushes a chunk past it, and no record is ever split to satisfy it.
	// The 2× ratio over the minimum keeps the accepted design's relation
	// between ordinary and evacuation boundaries.
	L0ForcedSealLimitTokens int64 = L0ChunkMinTokens * 2
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
	// MemorySendCapTokens bounds the applied replacement text admitted into
	// one sent context: the design's L1 (15k) + L2 (10k) allotment, held by
	// L1 alone until L1→L2 consolidation exists. The newest applied blocks
	// are admitted; older ones beyond the cap are reported as an omitted
	// memory range — stored, not summarized, originals readable — instead
	// of silently growing (or one huge replacement poisoning) every turn.
	MemorySendCapTokens int64 = 25_000
	// L1LimitTokens: applied L1 replacement text beyond this triggers an
	// L1→L2 consolidation target (docs/agent/memory.md: L1 holds ~15k).
	L1LimitTokens int64 = 15_000
	// L1DropToTokens: one L1→L2 target consumes the oldest contiguous
	// applied L1 fragments until at most this much applied L1 remains
	// unselected — the design's drop-to hysteresis.
	L1DropToTokens int64 = 11_000
	// L2LimitTokens: applied L2 beyond this triggers L2-internal
	// reintegration over the whole contiguous applied L2 run.
	L2LimitTokens int64 = 10_000
	// contextMaxEvents is a defensive row bound on the rendered raw window;
	// the token cap is the normal bound.
	contextMaxEvents = 5_000
	// memoryChunkMaxAttempts bounds recorded preparation failures per chunk
	// (provider errors, incomplete or truncated output, timeouts). A chunk
	// that exhausts them becomes 'failed' — visible in status, its originals
	// still live — rather than silently skipped or retried forever.
	memoryChunkMaxAttempts = 3
	// memoryChunkMaxInterruptions bounds claims that ended with no recorded
	// outcome (host stopped or evicted, writer fenced, claim response lost).
	// Interruptions are not failures of the preparation itself, so they do
	// not spend attempts; they are paced by backoff and bounded separately
	// so a host that dies on every claim cannot loop model calls forever.
	memoryChunkMaxInterruptions = 8
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

// MemoryChunk is one sealed journal range and its replacement lifecycle.
// Layer 1 chunks seal raw journal events; layer 2 chunks are consolidation
// targets whose Sources name the accepted fragments they consume.
type MemoryChunk struct {
	PersonaID string `json:"persona_id"`
	ChunkSeq  int64  `json:"chunk_seq"`
	Layer     int    `json:"layer"`
	// Sources is the ordered chunk_seqs an upper-layer target consumes —
	// the replacement's provenance. NULL for ordinary L0→L1 chunks. A
	// 'kept' target's source tuple is what later selection dedups against.
	Sources              []int64    `json:"sources"`
	FirstSeq             int64      `json:"first_seq"`
	LastSeq              int64      `json:"last_seq"`
	EstTokens            int64      `json:"est_tokens"`
	Status               string     `json:"status"`
	Replacement          *string    `json:"replacement"`
	ReplacementEstTokens *int64     `json:"replacement_est_tokens"`
	Attempts             int        `json:"attempts"`
	Interruptions        int        `json:"interruptions"`
	LastError            *string    `json:"last_error"`
	ClaimedGeneration    *int64     `json:"claimed_generation"`
	ClaimedAt            *time.Time `json:"claimed_at"`
	NotBefore            *time.Time `json:"not_before"`
	CreatedAt            time.Time  `json:"created_at"`
	PreparedAt           *time.Time `json:"prepared_at"`
	AppliedAt            *time.Time `json:"applied_at"`
}

// MemoryBlock is an applied chunk as it appears in the sent context: the
// replacement text rendered at the position where its events were, with the
// time range those events were recorded in.
type MemoryBlock struct {
	ChunkSeq  int64     `json:"chunk_seq"`
	Layer     int       `json:"layer"`
	FirstSeq  int64     `json:"first_seq"`
	LastSeq   int64     `json:"last_seq"`
	FirstTime time.Time `json:"first_time"`
	LastTime  time.Time `json:"last_time"`
	Text      string    `json:"text"`
	EstTokens int64     `json:"est_tokens"`
}

// RenderedContext is the journal as the model sees it: events not covered by
// an applied chunk, plus the applied blocks admitted under the memory cap.
// The caller interleaves blocks and notices at their original positions.
type RenderedContext struct {
	Events []Event       `json:"events"`
	Memory []MemoryBlock `json:"memory"`
	// Omitted describes older raw records that are neither applied memory
	// nor inside the send cap. They remain in the journal and readable
	// through conversation_history; nil when nothing was left out.
	Omitted *OmittedRange `json:"omitted"`
	// MemoryOmitted describes older applied blocks outside
	// MemorySendCapTokens; nil when every applied block was admitted.
	MemoryOmitted *OmittedMemory `json:"memory_omitted"`
}

// OmittedRange is the extent of raw records outside the sent context.
type OmittedRange struct {
	Count     int64     `json:"count"`
	FirstSeq  int64     `json:"first_seq"`
	LastSeq   int64     `json:"last_seq"`
	FirstTime time.Time `json:"first_time"`
	LastTime  time.Time `json:"last_time"`
}

// OmittedMemory is the extent of applied memory blocks outside the sent
// context: the oldest applied chunks beyond MemorySendCapTokens.
type OmittedMemory struct {
	Count         int       `json:"count"`
	FirstChunkSeq int64     `json:"first_chunk_seq"`
	LastChunkSeq  int64     `json:"last_chunk_seq"`
	FirstSeq      int64     `json:"first_seq"`
	LastSeq       int64     `json:"last_seq"`
	FirstTime     time.Time `json:"first_time"`
	LastTime      time.Time `json:"last_time"`
	EstTokens     int64     `json:"est_tokens"`
}

// MemoryStatus reports the memory layer's current shape for observability
// and for the writer's preparation scheduling.
type MemoryStatus struct {
	// LiveRawTokens estimates the raw records still in the sent context:
	// unapplied chunks (sealed through failed/kept) plus the unsealed tail.
	LiveRawTokens int64 `json:"live_raw_tokens"`
	// AppliedTokens estimates the replacement texts of every applied chunk,
	// including any left outside the memory cap.
	AppliedTokens int64 `json:"applied_tokens"`
	Sealed        int   `json:"sealed"`
	Preparing     int   `json:"preparing"`
	Prepared      int   `json:"prepared"`
	Applied       int   `json:"applied"`
	Kept          int   `json:"kept"`
	Failed        int   `json:"failed"`
	// Superseded counts sources replaced by an applied upper-layer block —
	// their accepted rows stay durable but no longer render raw.
	Superseded int `json:"superseded"`
	// Claimable counts chunks a claim could take now: sealed past their
	// backoff, or 'preparing' rows the live writer is not running.
	Claimable int `json:"claimable"`
	// NextClaimableAt is the earliest time any chunk becomes claimable; nil
	// when nothing waits for preparation. Hosts use it to schedule a wake.
	NextClaimableAt *time.Time `json:"next_claimable_at"`
	// AppliedOmitted counts applied blocks outside MemorySendCapTokens.
	AppliedOmitted      int   `json:"applied_omitted"`
	CoveredSeq          int64 `json:"covered_seq"`
	LatestSeq           int64 `json:"latest_seq"`
	ChunkMinTokens      int64 `json:"chunk_min_tokens"`
	LiveLimitTokens     int64 `json:"live_limit_tokens"`
	MemorySendCapTokens int64 `json:"memory_send_cap_tokens"`
}

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

var chunkCols = `persona_id, chunk_seq, layer, sources, first_seq, last_seq, est_tokens,
	status, replacement, replacement_est_tokens, attempts, interruptions, last_error,
	claimed_generation, claimed_at, not_before, created_at, prepared_at, applied_at`

func scanChunk(row interface{ Scan(...any) error }) (MemoryChunk, error) {
	var c MemoryChunk
	err := row.Scan(&c.PersonaID, &c.ChunkSeq, &c.Layer, &c.Sources, &c.FirstSeq, &c.LastSeq,
		&c.EstTokens, &c.Status, &c.Replacement, &c.ReplacementEstTokens,
		&c.Attempts, &c.Interruptions, &c.LastError, &c.ClaimedGeneration, &c.ClaimedAt,
		&c.NotBefore, &c.CreatedAt, &c.PreparedAt, &c.AppliedAt)
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

// contextQuerier is the read surface renderedContext needs: a pool or a tx.
type contextQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
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
// events not covered by an applied chunk (or by a superseded source — its
// records are represented by the applied upper-layer block that consumed
// it), up to L0SendCapTokens (and the caller's row bound), plus the newest
// applied blocks up to MemorySendCapTokens. Applied blocks are admitted
// regardless of the raw window — compacted memory stays in the context
// even when its original range is older than the raw window. The newest
// record is always included, even alone over the cap.
//
// excludeInputID names the input a turn is about to present itself: records
// its earlier attempts already journaled mid-turn (its input_received and a
// journal.note effect) are re-presented by the turn and its recorded plan,
// so they are left out here instead of appearing twice. Empty excludes none.
func (s *Store) renderedContext(ctx context.Context, db contextQuerier, personaID string, limit int, excludeInputID string) (RenderedContext, error) {
	var rc RenderedContext
	limit = clampLimit(limit, contextMaxEvents, contextMaxEvents)
	rows, err := db.Query(ctx, `
		SELECT persona_id, seq, turn_id, kind, payload, created_at
		FROM core_events e
		WHERE e.persona_id = $1
			AND NOT EXISTS (
				SELECT 1 FROM core_memory_chunks c
				WHERE c.persona_id = e.persona_id AND c.status IN ('applied','superseded')
					AND e.seq BETWEEN c.first_seq AND c.last_seq)
			AND NOT EXISTS (
				SELECT 1 FROM core_turns t
				WHERE t.persona_id = e.persona_id AND t.turn_id = e.turn_id
					AND t.input_id = $3)
		ORDER BY seq DESC LIMIT $2`, personaID, limit+1, excludeInputID)
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
					WHERE c.persona_id = e.persona_id AND c.status IN ('applied','superseded')
						AND e.seq BETWEEN c.first_seq AND c.last_seq)
				AND NOT EXISTS (
					SELECT 1 FROM core_turns t
					WHERE t.persona_id = e.persona_id AND t.turn_id = e.turn_id
						AND t.input_id = $3)`,
			personaID, rc.Events[0].Seq, excludeInputID).Scan(&om.Count, &om.FirstSeq, &om.LastSeq,
			&om.FirstTime, &om.LastTime); err != nil {
			return rc, err
		}
		rc.Omitted = &om
	}
	blocks, err := s.appliedBlocks(ctx, db, personaID)
	if err != nil {
		return rc, err
	}
	rc.Memory, rc.MemoryOmitted = admitApplied(blocks)
	return rc, nil
}

// appliedBlocks returns every applied chunk in journal position order —
// first_seq, not chunk_seq, so an upper-layer block renders at its
// earliest source's position — with the recorded time of its first and
// last covered event.
func (s *Store) appliedBlocks(ctx context.Context, db interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, personaID string) ([]MemoryBlock, error) {
	rows, err := db.Query(ctx, `
		SELECT c.chunk_seq, c.layer, c.first_seq, c.last_seq, f.created_at, l.created_at,
			c.replacement, c.replacement_est_tokens
		FROM core_memory_chunks c
		JOIN core_events f ON f.persona_id = c.persona_id AND f.seq = c.first_seq
		JOIN core_events l ON l.persona_id = c.persona_id AND l.seq = c.last_seq
		WHERE c.persona_id = $1 AND c.status = 'applied'
		ORDER BY c.first_seq`, personaID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []MemoryBlock{}
	for rows.Next() {
		var b MemoryBlock
		if err := rows.Scan(&b.ChunkSeq, &b.Layer, &b.FirstSeq, &b.LastSeq, &b.FirstTime, &b.LastTime,
			&b.Text, &b.EstTokens); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// admitApplied admits the newest applied blocks whose estimates fit
// MemorySendCapTokens, stopping at the first block that does not fit; that
// block and every older one form the omitted memory range. The cut is
// contiguous, so the notice names one journal range, and nothing is dropped
// silently or summarized in its place.
func admitApplied(blocks []MemoryBlock) ([]MemoryBlock, *OmittedMemory) {
	var used int64
	cut := len(blocks)
	for i := len(blocks) - 1; i >= 0; i-- {
		if used+blocks[i].EstTokens > MemorySendCapTokens {
			break
		}
		used += blocks[i].EstTokens
		cut = i
	}
	if cut == 0 {
		return blocks, nil
	}
	older := blocks[:cut]
	first, last := older[0], older[len(older)-1]
	om := &OmittedMemory{
		Count:         len(older),
		FirstChunkSeq: first.ChunkSeq,
		LastChunkSeq:  last.ChunkSeq,
		FirstSeq:      first.FirstSeq,
		LastSeq:       last.LastSeq,
		FirstTime:     first.FirstTime,
		LastTime:      last.LastTime,
	}
	for _, b := range older {
		om.EstTokens += b.EstTokens
	}
	return blocks[cut:], om
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
		WHERE persona_id = $1 AND status = 'preparing'`+scope, args...)
	return err
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
	if err := interruptPreparing(ctx, tx, personaID, &generation); err != nil {
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
	// call in the window is still waiting for its result. While the window
	// exceeds L0ForcedSealLimitTokens, one further boundary kind opens —
	// before an assistant_message that does not directly continue a tool
	// flow — so an oversized committed stretch still becomes bounded
	// preparation targets where a meaningful unit boundary exists. A turn's
	// deciding text and the calls/results it started are one unit: a cut
	// before a tool_call would separate the rationale from its effects, and
	// a turn with no interior boundary seals whole past the limit. Tool
	// calls and results commit inside one turn transaction, so a dangling
	// call can never sit at a turn boundary — the pending set is defensive
	// depth.
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
	prevKind := ""
	for _, e := range tail {
		// Boundary check happens BEFORE the event joins the window: the
		// boundary event itself opens the next chunk. Every boundary
		// requires a started window and no tool call still waiting for its
		// result. Never cut before a tool_call or tool_result — the deciding
		// assistant text and the effects it started are one unit — and never
		// before an assistant_message directly continuing a tool flow (it
		// follows a tool_result): the flow's results and its continuation
		// stay together.
		if windowStart >= 0 && len(pending) == 0 {
			cut := false
			switch e.kind {
			case "input_received":
				cut = windowEst >= L0ChunkMinTokens
			case "assistant_message":
				cut = windowEst > L0ForcedSealLimitTokens &&
					prevKind != "tool_result"
			}
			if cut {
				last := window[len(window)-1].seq
				if err := seal(windowStart, last, windowEst); err != nil {
					return st, err
				}
				window = window[:0]
				windowEst = 0
				windowStart = -1
			}
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
		prevKind = e.kind
	}
	// The unsealed remainder is the live tail: it is never sealed while no
	// boundary follows it, and it contributes to the raw estimate.
	tailEst := windowEst

	// Live raw = every not-yet-applied layer-1 chunk (its originals still
	// render) plus the unsealed tail. An upper-layer row never counts: its
	// range is already represented by its sources' layer-1 accounting while
	// they live, and by its applied replacement afterwards. 'superseded'
	// sources leave the count because their events no longer render raw —
	// the applied upper block represents them.
	var chunkRaw int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(SUM(est_tokens), 0) FROM core_memory_chunks
		WHERE persona_id = $1 AND layer = 1 AND status NOT IN ('applied','superseded')`,
		personaID).Scan(&chunkRaw); err != nil {
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
			WHERE persona_id = $1 AND layer = 1 AND status = 'prepared'
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

	// Upper layer: apply a prepared layer-2 target once the layer it
	// consumes is still over its own limit — the same gate that created it,
	// so a shelved candidate never lands on a layer that has since fallen
	// under the threshold. Sources and target swap in this transaction:
	// sources become 'superseded' (their accepted rows stay, their events
	// stop rendering raw because the applied target now represents them)
	// and the target becomes 'applied'.
	urows, err := tx.Query(ctx, `
		SELECT `+chunkCols+` FROM core_memory_chunks
		WHERE persona_id = $1 AND layer >= 2 AND status = 'prepared'
		ORDER BY chunk_seq FOR UPDATE`, personaID)
	if err != nil {
		return st, err
	}
	var preparedUpper []MemoryChunk
	for urows.Next() {
		c, err := scanChunk(urows)
		if err != nil {
			urows.Close()
			return st, err
		}
		preparedUpper = append(preparedUpper, c)
	}
	urows.Close()
	if err := urows.Err(); err != nil {
		return st, err
	}
	for _, target := range preparedUpper {
		if err := applyUpperTarget(ctx, tx, personaID, target); err != nil {
			return st, err
		}
	}

	// Then create the next upper-layer target when a layer is over its
	// limit. One target at a time: a sealed/preparing/prepared upper row
	// owns its source group until it is applied or kept.
	if err := prepareUpperTarget(ctx, tx, personaID, &chunkSeq); err != nil {
		return st, err
	}
	if err := tx.Commit(ctx); err != nil {
		return st, err
	}
	return s.MemoryStatus(ctx, personaID)
}

// appliedLayerTokens is the replacement-text estimate of one layer's
// applied fragments — the layer's load as the send context carries it.
func appliedLayerTokens(ctx context.Context, db interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, personaID string, layer int) (int64, error) {
	var n int64
	err := db.QueryRow(ctx, `
		SELECT COALESCE(SUM(replacement_est_tokens), 0) FROM core_memory_chunks
		WHERE persona_id = $1 AND layer = $2 AND status = 'applied'`,
		personaID, layer).Scan(&n)
	return n, err
}

// applyUpperTarget applies one prepared layer-2 target if its layer gate
// still holds. A target whose sources are no longer all applied cannot be
// reconstructed (unreachable while the one-in-flight rule holds, but the
// check is the honest answer if a bundle or another path produced one):
// it is marked failed, its sources untouched, without spending attempts.
func applyUpperTarget(ctx context.Context, tx pgx.Tx, personaID string, target MemoryChunk) error {
	type src struct {
		seq, layer int64
		status     string
	}
	srows, err := tx.Query(ctx, `
		SELECT chunk_seq, layer, status FROM core_memory_chunks
		WHERE persona_id = $1 AND chunk_seq = ANY($2) FOR UPDATE`,
		personaID, target.Sources)
	if err != nil {
		return err
	}
	var srcs []src
	for srows.Next() {
		var s src
		if err := srows.Scan(&s.seq, &s.layer, &s.status); err != nil {
			srows.Close()
			return err
		}
		srcs = append(srcs, s)
	}
	srows.Close()
	if err := srows.Err(); err != nil {
		return err
	}
	srcLayer := int64(0)
	stale := len(srcs) != len(target.Sources)
	for _, s := range srcs {
		if s.status != "applied" {
			stale = true
		}
		if srcLayer == 0 {
			srcLayer = s.layer
		} else if s.layer != srcLayer {
			stale = true
		}
	}
	if srcLayer != 1 && srcLayer != 2 {
		stale = true
	}
	if stale {
		_, err := tx.Exec(ctx, `
			UPDATE core_memory_chunks SET status = 'failed',
				last_error = 'upper-layer target is stale: its selected sources are no longer applied'
			WHERE persona_id = $1 AND chunk_seq = $2 AND status = 'prepared'`,
			personaID, target.ChunkSeq)
		return err
	}
	var limit int64
	if srcLayer == 1 {
		limit = L1LimitTokens
	} else {
		limit = L2LimitTokens
	}
	total, err := appliedLayerTokens(ctx, tx, personaID, int(srcLayer))
	if err != nil {
		return err
	}
	if total <= limit {
		return nil // the layer settled under its limit; the target stays shelved
	}
	tag, err := tx.Exec(ctx, `
		UPDATE core_memory_chunks SET status = 'superseded'
		WHERE persona_id = $1 AND chunk_seq = ANY($2) AND status = 'applied'`,
		personaID, target.Sources)
	if err != nil {
		return err
	}
	if int(tag.RowsAffected()) != len(target.Sources) {
		return fmt.Errorf("%w: upper target %d lost sources mid-apply", ErrMemoryConflict, target.ChunkSeq)
	}
	_, err = tx.Exec(ctx, `
		UPDATE core_memory_chunks SET status = 'applied', applied_at = now()
		WHERE persona_id = $1 AND chunk_seq = $2 AND status = 'prepared'`,
		personaID, target.ChunkSeq)
	return err
}

// prepareUpperTarget creates at most one sealed layer-2 target per pass,
// from the oldest contiguous run of applied fragments in the layer that is
// over its limit. L1→L2 consumes oldest-first until the unselected applied
// remainder drops to L1DropToTokens; L2 reintegration takes the whole
// contiguous applied L2 run. Contiguity is tile-adjacency in the journal:
// chunks seal contiguous ranges, so source b follows source a only when
// b.first_seq = a.last_seq + 1 — a kept chunk, a still-raw range or an
// unrelated fragment between them ends the run, and a later correction
// outside the span is never folded into an earlier replacement.
// A selection identical to a settled ('kept' or 'failed') target's source
// tuple is skipped: the verdict already holds for exactly those sources.
func prepareUpperTarget(ctx context.Context, tx pgx.Tx, personaID string, chunkSeq *int64) error {
	var busy bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM core_memory_chunks
			WHERE persona_id = $1 AND layer >= 2
				AND status IN ('sealed','preparing','prepared'))`,
		personaID).Scan(&busy); err != nil {
		return err
	}
	if busy {
		return nil
	}
	for _, sel := range []struct {
		srcLayer int
		limit    int64
		dropTo   int64
		// whole consumes the entire contiguous run (L2 reintegration);
		// otherwise the run stops once the applied remainder fits dropTo.
		whole bool
	}{
		{srcLayer: 1, limit: L1LimitTokens, dropTo: L1DropToTokens},
		{srcLayer: 2, limit: L2LimitTokens, dropTo: L2LimitTokens, whole: true},
	} {
		total, err := appliedLayerTokens(ctx, tx, personaID, sel.srcLayer)
		if err != nil {
			return err
		}
		if total <= sel.limit {
			continue
		}
		rows, err := tx.Query(ctx, `
			SELECT chunk_seq, first_seq, last_seq, replacement_est_tokens
			FROM core_memory_chunks
			WHERE persona_id = $1 AND layer = $2 AND status = 'applied'
			ORDER BY first_seq`, personaID, sel.srcLayer)
		if err != nil {
			return err
		}
		type frag struct {
			seq, first, last, rest int64
		}
		var frags []frag
		for rows.Next() {
			var f frag
			if err := rows.Scan(&f.seq, &f.first, &f.last, &f.rest); err != nil {
				rows.Close()
				return err
			}
			frags = append(frags, f)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for i := 0; i < len(frags); {
			group := []frag{frags[i]}
			consumed := frags[i].rest
			i++
			for i < len(frags) && frags[i].first == group[len(group)-1].last+1 &&
				(sel.whole || consumed < total-sel.dropTo) {
				group = append(group, frags[i])
				consumed += frags[i].rest
				i++
			}
			var srcSeqs []int64
			for _, f := range group {
				srcSeqs = append(srcSeqs, f.seq)
			}
			// A prior verdict applies only to this exact source tuple: a
			// 'kept' row is the model's KEEP_UNCHANGED for those sources,
			// and a 'failed' row exhausted its attempts — resealing the
			// identical set would either relitigate the answer or burn a
			// fresh attempt budget forever. A different grouping of the same
			// shelf may still run.
			var settled bool
			if err := tx.QueryRow(ctx, `
				SELECT EXISTS(SELECT 1 FROM core_memory_chunks
					WHERE persona_id = $1 AND layer >= 2 AND status IN ('kept','failed')
						AND sources = $2::bigint[])`,
				personaID, srcSeqs).Scan(&settled); err != nil {
				return err
			}
			if settled {
				continue
			}
			*chunkSeq++
			_, err := tx.Exec(ctx, `
				INSERT INTO core_memory_chunks
					(persona_id, chunk_seq, layer, sources, first_seq, last_seq, est_tokens, status)
				VALUES ($1, $2, 2, $3, $4, $5, $6, 'sealed')`,
				personaID, *chunkSeq, srcSeqs, group[0].first, group[len(group)-1].last, consumed)
			return err
		}
	}
	return nil
}

// MemoryStatus reads the current memory shape without mutating it.
func (s *Store) MemoryStatus(ctx context.Context, personaID string) (MemoryStatus, error) {
	st := MemoryStatus{
		ChunkMinTokens:      L0ChunkMinTokens,
		LiveLimitTokens:     L0LiveLimitTokens,
		MemorySendCapTokens: MemorySendCapTokens,
	}
	err := s.pool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(est_tokens) FILTER (WHERE layer = 1
				AND status NOT IN ('applied','superseded')), 0),
			COALESCE(SUM(replacement_est_tokens) FILTER (WHERE status = 'applied'), 0),
			COUNT(*) FILTER (WHERE status = 'sealed'),
			COUNT(*) FILTER (WHERE status = 'preparing'),
			COUNT(*) FILTER (WHERE status = 'prepared'),
			COUNT(*) FILTER (WHERE status = 'applied'),
			COUNT(*) FILTER (WHERE status = 'kept'),
			COUNT(*) FILTER (WHERE status = 'failed'),
			COUNT(*) FILTER (WHERE status = 'superseded'),
			COUNT(*) FILTER (WHERE status = 'preparing'
				OR (status = 'sealed' AND (not_before IS NULL OR not_before <= now()))),
			MIN(CASE WHEN status = 'preparing' THEN now()
				WHEN status = 'sealed' THEN GREATEST(COALESCE(not_before, now()), now()) END),
			COALESCE(MAX(last_seq), 0)
		FROM core_memory_chunks WHERE persona_id = $1`, personaID).
		Scan(&st.LiveRawTokens, &st.AppliedTokens, &st.Sealed, &st.Preparing,
			&st.Prepared, &st.Applied, &st.Kept, &st.Failed, &st.Superseded,
			&st.Claimable, &st.NextClaimableAt, &st.CoveredSeq)
	if err != nil {
		return st, err
	}
	if st.Applied > 0 {
		rows, err := s.pool.Query(ctx, `
			SELECT COALESCE(replacement_est_tokens, 0) FROM core_memory_chunks
			WHERE persona_id = $1 AND status = 'applied' ORDER BY first_seq`, personaID)
		if err != nil {
			return st, err
		}
		var blocks []MemoryBlock
		for rows.Next() {
			var b MemoryBlock
			if err := rows.Scan(&b.EstTokens); err != nil {
				rows.Close()
				return st, err
			}
			blocks = append(blocks, b)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return st, err
		}
		if _, om := admitApplied(blocks); om != nil {
			st.AppliedOmitted = om.Count
		}
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
	// never as a failed attempt — which keeps one branch at a time.
	if err := interruptPreparing(ctx, tx, personaID, nil); err != nil {
		return nil, err
	}
	c, err := scanChunk(tx.QueryRow(ctx, `
		UPDATE core_memory_chunks
		SET status = 'preparing', claimed_generation = $2, claimed_at = now(), not_before = NULL
		WHERE (persona_id, chunk_seq) = (
			SELECT persona_id, chunk_seq FROM core_memory_chunks
			WHERE persona_id = $1 AND status = 'sealed'
				AND (not_before IS NULL OR not_before <= now())
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
		// still be applied: a stale target (unreachable while the
		// one-in-flight rule holds; possible only through a crafted or
		// carried row) is marked failed without spending attempts rather
		// than prepared from a different source set.
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
