package agentstate

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"time"
)

type memoryResumeRequest struct {
	Chunk          int64
	Mode           string
	Rounds, Tokens int64
}

func parseMemoryResumeRequest(request map[string]any) (memoryResumeRequest, error) {
	var r memoryResumeRequest
	number := func(key string, max int64, required bool) (int64, error) {
		v, ok := request[key]
		if !ok && !required {
			return 0, nil
		}
		raw, e := json.Marshal(v)
		if e != nil {
			return 0, e
		}
		var n int64
		if json.Unmarshal(raw, &n) != nil || n < 0 || n > max || (required && n == 0) {
			return 0, fmt.Errorf("%w: memory.resume invalid %s", ErrBadRequest, key)
		}
		return n, nil
	}
	var e error
	if r.Chunk, e = number("chunk_seq", 1<<53-1, true); e != nil {
		return r, e
	}
	if r.Rounds, e = number("additional_rounds", 128, true); e != nil {
		return r, e
	}
	if r.Tokens, e = number("additional_tokens", 100000000, false); e != nil {
		return r, e
	}
	r.Mode, _ = request["mode"].(string)
	if r.Mode != "resume" && r.Mode != "rebranch" {
		return r, fmt.Errorf("%w: memory.resume requires resume or rebranch", ErrBadRequest)
	}
	for k := range request {
		if k != "chunk_seq" && k != "mode" && k != "additional_rounds" && k != "additional_tokens" {
			return r, fmt.Errorf("%w: memory.resume unexpected field %s", ErrBadRequest, k)
		}
	}
	return r, nil
}

// Called inside the ordinary operation effect+receipt+experience transaction.
// No draft text or historical content is accepted from the caller.
func (s *Store) resumeMemory(ctx context.Context, tx pgx.Tx, persona string, request map[string]any) (map[string]any, error) {
	r, e := parseMemoryResumeRequest(request)
	if e != nil {
		return nil, e
	}
	b, e := s.readMemoryBranch(ctx, tx, persona, r.Chunk)
	if e != nil {
		return nil, fmt.Errorf("%w: memory branch not found", ErrBadRequest)
	}
	var st map[string]json.RawMessage
	if json.Unmarshal(b.State, &st) != nil || string(st["status"]) != "\"paused\"" || b.Chunk.Status != "preparing" {
		return nil, fmt.Errorf("%w: memory.resume requires a paused unfinished branch", ErrBadRequest)
	}
	put := func(k string, v any) { st[k], _ = json.Marshal(v) }
	put("review", nil)
	put("final", nil)
	put("source_read", []any{})
	put("candidate_read", []any{})
	put("in_flight", nil)
	put("failures", 0)
	put("interruptions", 0)
	var retry any
	if r.Mode == "rebranch" {
		var policy struct {
			MaxTokens int64 `json:"maxTokens"`
		}
		_ = json.Unmarshal(st["policy"], &policy)
		var execution struct {
			Tokens int64 `json:"tokens"`
		}
		var extension struct {
			Tokens int64 `json:"tokens"`
		}
		limit := policy.MaxTokens
		if v, ok := st["execution_budget"]; ok && string(v) != "null" {
			_ = json.Unmarshal(v, &execution)
			limit = execution.Tokens
		}
		_ = json.Unmarshal(st["budget_extension"], &extension)
		var used int64
		_ = json.Unmarshal(st["tokens"], &used)
		restartTokens := r.Tokens
		if limit > 0 {
			remaining := limit + extension.Tokens - used
			if remaining > 0 {
				restartTokens += remaining
			}
			if restartTokens == 0 {
				return nil, fmt.Errorf("%w: exhausted token cap requires additional_tokens", ErrBadRequest)
			}
		}
		put("rebranch_requested", true)
		put("restart_budget", map[string]any{"rounds": r.Rounds, "tokens": restartTokens})
		put("pause_reason", "rebranch_requested")
		put("retry_at", nil)
	} else {
		if string(st["pause_reason"]) == "\"private_tools_missing\"" {
			return nil, fmt.Errorf("%w: frozen tools cannot change; use rebranch", ErrBadRequest)
		}
		var ext struct {
			Rounds int64 `json:"rounds"`
			Tokens int64 `json:"tokens"`
		}
		_ = json.Unmarshal(st["budget_extension"], &ext)
		var policy struct {
			MaxTokens int64 `json:"maxTokens"`
		}
		_ = json.Unmarshal(st["policy"], &policy)
		var execution struct {
			Tokens int64 `json:"tokens"`
		}
		limit := policy.MaxTokens
		if v, ok := st["execution_budget"]; ok && string(v) != "null" {
			_ = json.Unmarshal(v, &execution)
			limit = execution.Tokens
		}
		var used int64
		_ = json.Unmarshal(st["tokens"], &used)
		if limit > 0 && limit+ext.Tokens+r.Tokens <= used {
			return nil, fmt.Errorf("%w: exhausted token cap requires additional_tokens", ErrBadRequest)
		}
		ext.Rounds += r.Rounds
		ext.Tokens += r.Tokens
		put("budget_extension", ext)
		put("rebranch_requested", false)
		put("pause_reason", "resume_requested")
		retry = time.Now().UTC()
		put("retry_at", retry)
	}
	var messages []json.RawMessage
	_ = json.Unmarshal(st["messages"], &messages)
	note, _ := json.Marshal(map[string]any{"role": "user", "content": fmt.Sprintf("[Memory recovery selected by the main secretary] mode=%s, additional_rounds=%d, additional_tokens=%d. This is a finite execution grant, not a change to your draft. Read source and candidate again and open a new confirmation.", r.Mode, r.Rounds, r.Tokens)})
	messages = append(messages, note)
	put("messages", messages)
	raw, _ := json.Marshal(st)
	_, e = tx.Exec(ctx, `UPDATE core_memory_branches SET state=$3,revision=revision+1,checkpoint_hash='',retry_at=$4,updated_at=now() WHERE persona_id=$1 AND chunk_seq=$2`, persona, r.Chunk, string(raw), retry)
	return map[string]any{"chunk_seq": r.Chunk, "mode": r.Mode, "additional_rounds": r.Rounds, "additional_tokens": r.Tokens, "waiting_for_parent_snapshot": r.Mode == "rebranch"}, e
}

func snapshotHasMemoryTools(snap *BranchSnapshot) bool {
	found := map[string]bool{}
	for _, raw := range snap.Tools {
		var t struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(raw, &t) == nil {
			found[t.Name] = true
		}
	}
	return found["file.read"] && found["file.write"]
}

// A deliberately requested new attempt only takes a real, current parent
// boundary that still contains this exact target. Nothing is re-rendered here.
func (s *Store) restartRequestedMemory(ctx context.Context, tx pgx.Tx, persona string, snap *BranchSnapshot) error {
	if snap == nil || !snapshotHasMemoryTools(snap) {
		return nil
	}
	var seq int64
	e := tx.QueryRow(ctx, `SELECT b.chunk_seq FROM core_memory_branches b JOIN core_memory_chunks c USING(persona_id,chunk_seq) WHERE b.persona_id=$1 AND c.status='preparing' AND b.state::jsonb->>'rebranch_requested'='true' ORDER BY b.chunk_seq LIMIT 1`, persona).Scan(&seq)
	if e == pgx.ErrNoRows {
		return nil
	}
	if e != nil {
		return e
	}
	if e = validateBranchSnapshot(snap); e != nil {
		return e
	}
	b, e := s.readMemoryBranch(ctx, tx, persona, seq)
	if e != nil {
		return e
	}
	matches, e := s.snapshotMatchesChunk(ctx, tx, persona, b.Chunk, *snap)
	if e != nil || !matches {
		return e
	}
	if e = s.archiveMemoryAttempt(ctx, tx, persona, b); e != nil {
		return e
	}
	raw, e := json.Marshal(snap)
	if e != nil {
		return e
	}
	_, e = tx.Exec(ctx, `UPDATE core_memory_branches SET snapshot=$3,state=NULL,revision=revision+1,checkpoint_hash='',status='running',retry_at=NULL,updated_at=now() WHERE persona_id=$1 AND chunk_seq=$2`, persona, seq, string(raw))
	return e
}
func (s *Store) archiveMemoryAttempt(ctx context.Context, tx pgx.Tx, persona string, b *MemoryBranch) error {
	var historyText string
	if e := tx.QueryRow(ctx, `SELECT previous_attempts FROM core_memory_branches WHERE persona_id=$1 AND chunk_seq=$2`, persona, b.Chunk.ChunkSeq).Scan(&historyText); e != nil {
		return e
	}
	var history []json.RawMessage
	if e := json.Unmarshal([]byte(historyText), &history); e != nil {
		return e
	}
	copy := *b
	copy.PreviousAttempt = nil
	raw, e := json.Marshal(copy)
	if e != nil {
		return e
	}
	history = append(history, raw)
	raw, e = json.Marshal(history)
	if e != nil {
		return e
	}
	_, e = tx.Exec(ctx, `UPDATE core_memory_branches SET previous_attempts=$3 WHERE persona_id=$1 AND chunk_seq=$2`, persona, b.Chunk.ChunkSeq, string(raw))
	return e
}
