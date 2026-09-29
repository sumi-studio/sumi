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
// Current preparation thresholds:
// seal at >=10k estimated tokens, replace when live raw exceeds 40k. The
// estimate is an internal capacity heuristic, not provider billing.
//
// Upper layers share the same pipeline. When applied L1 replacements exceed
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
	// L1LimitTokens: applied L1 replacement text beyond this triggers an
	// L1→L2 consolidation target.
	L1LimitTokens int64 = 15_000
	// L1DropToTokens: one L1→L2 target consumes the oldest contiguous
	// applied L1 fragments until at most this much applied L1 remains
	// unselected — the design's drop-to hysteresis.
	L1DropToTokens int64 = 11_000
	// L2LimitTokens: applied L2 beyond this triggers L2-internal
	// reintegration over the whole contiguous applied L2 run.
	L2LimitTokens int64 = 10_000
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
	Branches []MemoryBranchStatus `json:"branches"`
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

// renderedContext returns every unrepresented event and every applied block.
// Only an explicitly confirmed and applied replacement can remove an original
// from this view. Row/token budgets do not silently discard kept/failed work.
func (s *Store) renderedContext(ctx context.Context, db contextQuerier, personaID string, _ int, excludeInputID string) (RenderedContext, error) {
	var rc RenderedContext
	rows, err := db.Query(ctx, `
 SELECT persona_id,seq,turn_id,kind,payload,created_at FROM core_events e
 WHERE e.persona_id=$1 AND NOT EXISTS (
  SELECT 1 FROM core_memory_chunks c WHERE c.persona_id=e.persona_id
   AND c.status IN ('applied','superseded') AND e.seq BETWEEN c.first_seq AND c.last_seq)
 AND NOT EXISTS (SELECT 1 FROM core_turns t WHERE t.persona_id=e.persona_id
  AND t.turn_id=e.turn_id AND t.input_id=$2)
 ORDER BY seq`, personaID, excludeInputID)
	if err != nil {
		return rc, err
	}
	rc.Events = []Event{}
	for rows.Next() {
		var e Event
		if err = rows.Scan(&e.PersonaID, &e.Seq, &e.TurnID, &e.Kind, &e.Payload, &e.CreatedAt); err != nil {
			rows.Close()
			return rc, err
		}
		rc.Events = append(rc.Events, e)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return rc, err
	}
	rc.Memory, err = s.appliedBlocks(ctx, db, personaID)
	return rc, err
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
	// a turn with no interior boundary seals whole past the limit. Native
	// calls from one decision may be journaled across recovery, with another
	// input in between. Keep that whole known round together; the pending
	// set also prevents sealing a call whose result has not been recorded.
	rows, err := tx.Query(ctx, `
		SELECT seq, turn_id, kind, payload FROM core_events
		WHERE persona_id = $1 AND seq > $2 ORDER BY seq`, personaID, covered)
	if err != nil {
		return st, err
	}
	type evRow struct {
		seq     int64
		kind    string
		turnID  string
		payload map[string]any
	}
	var tail []evRow
	for rows.Next() {
		var e evRow
		if err := rows.Scan(&e.seq, &e.turnID, &e.kind, &e.payload); err != nil {
			rows.Close()
			return st, err
		}
		tail = append(tail, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return st, err
	}
	roundKey := func(e evRow) string {
		r, ok := e.payload["round"]
		if !ok || (e.kind != "assistant_message" && e.kind != "tool_call" && e.kind != "tool_result") {
			return ""
		}
		return fmt.Sprintf("%s:%v", e.turnID, r)
	}
	roundEnds := map[string]int64{}
	for _, e := range tail {
		if key := roundKey(e); key != "" {
			roundEnds[key] = e.seq
		}
	}
	var activeRoundEnd int64 = -1
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
		if windowStart >= 0 && len(pending) == 0 && e.seq > activeRoundEnd {
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
		if key := roundKey(e); key != "" && roundEnds[key] > activeRoundEnd {
			activeRoundEnd = roundEnds[key]
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
// reconstructed — a crafted or carried row, or a stale target whose sources
// a different applied target consumed while it waited — so it is marked
// failed, its sources untouched, without spending attempts.
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
		MemorySendCapTokens: 0,
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
			COUNT(*) FILTER (WHERE (status = 'preparing' AND EXISTS(SELECT 1 FROM core_memory_branches b WHERE b.persona_id=core_memory_chunks.persona_id AND b.chunk_seq=core_memory_chunks.chunk_seq AND (b.status='running' OR b.retry_at<=statement_timestamp())))),
			MIN(CASE WHEN status = 'preparing' THEN (SELECT CASE WHEN b.status='running' THEN statement_timestamp() ELSE b.retry_at END FROM core_memory_branches b WHERE b.persona_id=core_memory_chunks.persona_id AND b.chunk_seq=core_memory_chunks.chunk_seq) END),
			COALESCE(MAX(last_seq), 0)
		FROM core_memory_chunks WHERE persona_id = $1`, personaID).
		Scan(&st.LiveRawTokens, &st.AppliedTokens, &st.Sealed, &st.Preparing,
			&st.Prepared, &st.Applied, &st.Kept, &st.Failed, &st.Superseded,
			&st.Claimable, &st.NextClaimableAt, &st.CoveredSeq)
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
	rows, err = s.pool.Query(ctx, `SELECT b.chunk_seq,b.status,b.revision,b.retry_at,b.issue,b.state::jsonb->>'pause_reason' FROM core_memory_branches b JOIN core_memory_chunks c USING(persona_id,chunk_seq) WHERE b.persona_id=$1 AND c.status='preparing' ORDER BY b.chunk_seq`, personaID)
	if err != nil {
		return st, err
	}
	st.Branches = []MemoryBranchStatus{}
	for rows.Next() {
		var b MemoryBranchStatus
		if err = rows.Scan(&b.ChunkSeq, &b.Status, &b.Revision, &b.RetryAt, &b.Issue, &b.PauseReason); err != nil {
			rows.Close()
			return st, err
		}
		st.Branches = append(st.Branches, b)
	}
	rows.Close()
	return st, rows.Err()
}
