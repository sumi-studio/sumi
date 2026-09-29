package agentstate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

// BranchSnapshot is captured at the live model boundary. The state service
// never builds it from journal records or resolves fresh historical content.
type BranchSnapshot struct {
	Messages []json.RawMessage   `json:"messages"`
	Tools    []json.RawMessage   `json:"tools"`
	Ranges   []BranchSourceRange `json:"ranges"`
	Binding  json.RawMessage     `json:"binding,omitempty"`
}
type BranchSourceRange struct {
	FirstSeq     int64  `json:"first_seq"`
	LastSeq      int64  `json:"last_seq"`
	MessageIndex *int   `json:"message_index,omitempty"`
	Layer        *int   `json:"layer,omitempty"`
	ChunkSeq     *int64 `json:"chunk_seq,omitempty"`
}
type MemoryBranch struct {
	Chunk           *MemoryChunk    `json:"chunk"`
	Snapshot        BranchSnapshot  `json:"snapshot"`
	State           json.RawMessage `json:"state"`
	Revision        int64           `json:"revision"`
	PreviousAttempt json.RawMessage `json:"previous_attempt,omitempty"`
}
type branchCandidate struct {
	Text    string `json:"text"`
	Version int64  `json:"version"`
	SHA256  string `json:"sha256"`
}
type branchReview struct {
	Kind        string `json:"kind"`
	Version     int64  `json:"version"`
	SHA256      string `json:"sha256"`
	Token       string `json:"token"`
	OpenedRound int64  `json:"opened_round"`
}
type branchIssue struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
type branchCheckpoint struct {
	Status      string            `json:"status"`
	Messages    []json.RawMessage `json:"messages"`
	Candidate   *branchCandidate  `json:"candidate"`
	Review      *branchReview     `json:"review"`
	Final       *branchReview     `json:"final"`
	Rounds      int64             `json:"rounds"`
	ModelCalls  int64             `json:"model_calls"`
	Tokens      int64             `json:"tokens"`
	RetryAt     *time.Time        `json:"retry_at"`
	PauseReason *string           `json:"pause_reason"`
	Issue       *branchIssue      `json:"issue"`
}

func requireMemoryGeneration(ctx context.Context, tx pgx.Tx, persona string, generation int64) error {
	if err := requireGeneration(ctx, tx, persona, generation); err != nil {
		return err
	}
	var live bool
	if err := tx.QueryRow(ctx, `SELECT expires_at > statement_timestamp() FROM core_writer_leases WHERE persona_id=$1`, persona).Scan(&live); err != nil {
		return err
	}
	if !live {
		return ErrGenerationFence
	}
	return nil
}
func branchHash(raw []byte) string { h := sha256.Sum256(raw); return hex.EncodeToString(h[:]) }
func canonicalBranchJSON(raw json.RawMessage) ([]byte, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}
func validateBranchSnapshot(snap *BranchSnapshot) error {
	if snap == nil || len(snap.Messages) == 0 || len(snap.Ranges) == 0 {
		return fmt.Errorf("%w: a live context snapshot is required", ErrBadRequest)
	}
	for _, r := range snap.Ranges {
		if r.FirstSeq < 1 || r.LastSeq < r.FirstSeq || (r.MessageIndex != nil && (*r.MessageIndex < 0 || *r.MessageIndex >= len(snap.Messages))) {
			return fmt.Errorf("%w: invalid snapshot source range", ErrBadRequest)
		}
	}
	for _, raw := range snap.Messages {
		var m struct {
			Role    string  `json:"role"`
			Content *string `json:"content"`
		}
		if json.Unmarshal(raw, &m) != nil || m.Content == nil || (m.Role != "system" && m.Role != "user" && m.Role != "assistant" && m.Role != "tool") {
			return fmt.Errorf("%w: invalid snapshot message", ErrBadRequest)
		}
	}
	return nil
}
func snapshotCovers(snap BranchSnapshot, first, last int64) bool {
	// A grouped native message cannot be partly attributed to a target.
	selected := map[int]bool{}
	for _, r := range snap.Ranges {
		if r.MessageIndex != nil && r.FirstSeq >= first && r.LastSeq <= last {
			selected[*r.MessageIndex] = true
		}
	}
	for _, r := range snap.Ranges {
		if r.MessageIndex != nil && selected[*r.MessageIndex] && (r.FirstSeq < first || r.LastSeq > last) {
			return false
		}
	}
	ranges := append([]BranchSourceRange(nil), snap.Ranges...)
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].FirstSeq < ranges[j].FirstSeq })
	next := first
	for _, r := range ranges {
		if r.LastSeq < first || r.FirstSeq > last {
			continue
		}
		// A prepared block may not be partially consumed by another target.
		if r.FirstSeq < first || r.LastSeq > last || r.FirstSeq > next {
			return false
		}
		if r.LastSeq >= next {
			next = r.LastSeq + 1
		}
	}
	return next == last+1
}

// Equal sequence coverage is insufficient after a layer application: an
// upper target must have its actual selected fragments in the live snapshot.
func (s *Store) snapshotMatchesChunk(ctx context.Context, db contextQuerier, persona string, c *MemoryChunk, snap BranchSnapshot) (bool, error) {
	if !snapshotCovers(snap, c.FirstSeq, c.LastSeq) {
		return false, nil
	}
	if c.Layer == 1 {
		for _, r := range snap.Ranges {
			if r.FirstSeq >= c.FirstSeq && r.LastSeq <= c.LastSeq && (r.Layer != nil || r.ChunkSeq != nil) {
				return false, nil
			}
		}
		return true, nil
	}
	for _, seq := range c.Sources {
		source, e := s.chunk(ctx, db, persona, seq)
		if e != nil {
			return false, e
		}
		if source.Status != "applied" {
			return false, nil
		}
		found := false
		for _, r := range snap.Ranges {
			if r.MessageIndex != nil && r.ChunkSeq != nil && *r.ChunkSeq == seq && r.Layer != nil && *r.Layer == source.Layer && r.FirstSeq == source.FirstSeq && r.LastSeq == source.LastSeq {
				found = true
				break
			}
		}
		if !found {
			return false, nil
		}
	}
	return true, nil
}

func (s *Store) readMemoryBranch(ctx context.Context, db contextQuerier, persona string, seq int64) (*MemoryBranch, error) {
	b := &MemoryBranch{}
	var snapshot []byte
	var previous string
	if err := db.QueryRow(ctx, `SELECT snapshot, state, revision, previous_attempts FROM core_memory_branches WHERE persona_id=$1 AND chunk_seq=$2`, persona, seq).Scan(&snapshot, &b.State, &b.Revision, &previous); err != nil {
		return nil, err
	}
	if len(b.State) == 0 {
		var attempts []struct {
			State json.RawMessage `json:"state"`
		}
		if json.Unmarshal([]byte(previous), &attempts) == nil && len(attempts) > 0 {
			b.PreviousAttempt = attempts[len(attempts)-1].State
		}
	}
	if err := json.Unmarshal(snapshot, &b.Snapshot); err != nil {
		return nil, err
	}
	c, err := s.chunk(ctx, db, persona, seq)
	if err != nil {
		return nil, err
	}
	b.Chunk = c
	return b, nil
}

// ClaimMemoryBranch resumes an existing branch before taking any new work.
// A new branch is possible only with a frozen snapshot that actually covers
// its whole target. A restart without a snapshot may resume, never reconstruct.
func (s *Store) ClaimMemoryBranch(ctx context.Context, persona string, generation int64, snapshot *BranchSnapshot, policies ...json.RawMessage) (*MemoryBranch, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err = requireMemoryGeneration(ctx, tx, persona, generation); err != nil {
		return nil, err
	}
	if err = s.restartRequestedMemory(ctx, tx, persona, snapshot); err != nil {
		return nil, err
	}
	var policy any
	if len(policies) > 0 && len(policies[0]) > 0 {
		policy = policies[0]
	}
	var selectedFingerprint any
	if snapshot != nil && len(snapshot.Binding) > 0 {
		var pin struct {
			Fingerprint string `json:"fingerprint"`
		}
		if json.Unmarshal(snapshot.Binding, &pin) == nil && pin.Fingerprint != "" {
			selectedFingerprint = pin.Fingerprint
		}
	}
	var seq int64
	err = tx.QueryRow(ctx, `SELECT b.chunk_seq FROM core_memory_branches b JOIN core_memory_chunks c USING(persona_id,chunk_seq) WHERE b.persona_id=$1 AND c.status='preparing' AND COALESCE(b.state::jsonb->>'rebranch_requested','false')<>'true' AND (b.status='running' OR b.retry_at<=statement_timestamp() OR ($2::jsonb IS NOT NULL AND b.state::jsonb->'policy'<>$2::jsonb AND b.state::jsonb->>'pause_reason'='configured_budget') OR ($3::text IS NOT NULL AND $3<>COALESCE(b.state::jsonb->'effective_binding'->>'fingerprint',b.snapshot::jsonb->'binding'->>'fingerprint'))) ORDER BY b.chunk_seq LIMIT 1`, persona, policy, selectedFingerprint).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		var waiting bool
		if e := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM core_memory_chunks WHERE persona_id=$1 AND status='preparing')`, persona).Scan(&waiting); e != nil {
			return nil, e
		}
		if waiting {
			return nil, nil
		} // one durable branch, including an explicit pause
		if snapshot == nil {
			return nil, nil
		}
		if err = validateBranchSnapshot(snapshot); err != nil {
			return nil, err
		}
		// A kept verdict can be reconsidered only against substantially new context.
		if err = s.reconsiderKeptMemory(ctx, tx, persona, *snapshot); err != nil {
			return nil, err
		}
		rows, e := tx.Query(ctx, `SELECT chunk_seq,first_seq,last_seq FROM core_memory_chunks WHERE persona_id=$1 AND status='sealed' AND NOT EXISTS(SELECT 1 FROM unnest(core_memory_chunks.sources) src JOIN core_memory_chunks c ON c.persona_id=$1 AND c.chunk_seq=src WHERE c.status<>'applied') AND (not_before IS NULL OR not_before<=statement_timestamp()) ORDER BY chunk_seq`, persona)
		if e != nil {
			return nil, e
		}
		var candidates []int64
		for rows.Next() {
			var n, a, z int64
			if e = rows.Scan(&n, &a, &z); e != nil {
				rows.Close()
				return nil, e
			}
			if snapshotCovers(*snapshot, a, z) {
				candidates = append(candidates, n)
			}
		}
		rows.Close()
		if e = rows.Err(); e != nil {
			return nil, e
		}
		for _, n := range candidates {
			c, e := s.chunk(ctx, tx, persona, n)
			if e != nil {
				return nil, e
			}
			matches, e := s.snapshotMatchesChunk(ctx, tx, persona, c, *snapshot)
			if e != nil {
				return nil, e
			}
			if matches {
				seq = n
				break
			}
		}
		if seq == 0 {
			return nil, nil
		}
		raw, e := json.Marshal(snapshot)
		if e != nil {
			return nil, e
		}
		_, err = tx.Exec(ctx, `INSERT INTO core_memory_branches(persona_id,chunk_seq,snapshot) VALUES($1,$2,$3)
   ON CONFLICT(persona_id,chunk_seq) DO UPDATE SET snapshot=EXCLUDED.snapshot,state=NULL,
    revision=core_memory_branches.revision+1,checkpoint_hash='',status='running',retry_at=NULL,issue=NULL`, persona, seq, string(raw))
	} else if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `UPDATE core_memory_chunks SET status='preparing',claimed_generation=$3,claimed_at=now(),not_before=NULL WHERE persona_id=$1 AND chunk_seq=$2`, persona, seq, generation); err != nil {
		return nil, err
	}
	b, err := s.readMemoryBranch(ctx, tx, persona, seq)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return b, nil
}

// SaveMemoryBranch checkpoints a full model round, private-file effects and
// confirmation state atomically. A repeated identical revision replays its
// receipt. No messages are sent into Messaging by this path.
func (s *Store) SaveMemoryBranch(ctx context.Context, persona string, generation, seq, expected int64, raw json.RawMessage) (*MemoryBranch, error) {
	canonical, err := canonicalBranchJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: checkpoint must be JSON", ErrBadRequest)
	}
	var next branchCheckpoint
	if err = json.Unmarshal(raw, &next); err != nil {
		return nil, fmt.Errorf("%w: malformed checkpoint", ErrBadRequest)
	}
	if next.Status != "running" && next.Status != "paused" && next.Status != "prepared" && next.Status != "kept" {
		return nil, fmt.Errorf("%w: invalid branch status", ErrBadRequest)
	}
	if len(next.Messages) == 0 || next.Rounds < 0 || next.ModelCalls < 0 || next.Tokens < 0 {
		return nil, fmt.Errorf("%w: invalid branch progress", ErrBadRequest)
	}
	if next.Candidate != nil && (next.Candidate.Version < 1 || next.Candidate.SHA256 != branchHash([]byte(next.Candidate.Text))) {
		return nil, fmt.Errorf("%w: candidate hash mismatch", ErrBadRequest)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err = requireMemoryGeneration(ctx, tx, persona, generation); err != nil {
		return nil, err
	}
	b, err := s.readMemoryBranch(ctx, tx, persona, seq)
	if err != nil {
		return nil, err
	}
	digest := branchHash(canonical)
	var priorDigest string
	if err = tx.QueryRow(ctx, `SELECT checkpoint_hash FROM core_memory_branches WHERE persona_id=$1 AND chunk_seq=$2`, persona, seq).Scan(&priorDigest); err != nil {
		return nil, err
	}
	if b.Revision == expected+1 && digest == priorDigest {
		return b, nil
	}
	if b.Revision != expected {
		return nil, fmt.Errorf("%w: stale branch revision", ErrMemoryConflict)
	}
	if b.Chunk.Status != "preparing" {
		return nil, fmt.Errorf("%w: branch already settled", ErrMemoryConflict)
	}
	var old branchCheckpoint
	if len(b.State) > 0 {
		if err = json.Unmarshal(b.State, &old); err != nil {
			return nil, err
		}
	}
	if next.Rounds < old.Rounds || next.ModelCalls < old.ModelCalls || next.Tokens < old.Tokens || len(next.Messages) < len(old.Messages) {
		return nil, fmt.Errorf("%w: progress cannot move backwards", ErrMemoryConflict)
	}
	for i := range old.Messages {
		var a, z any
		_ = json.Unmarshal(old.Messages[i], &a)
		_ = json.Unmarshal(next.Messages[i], &z)
		if !reflect.DeepEqual(a, z) {
			return nil, fmt.Errorf("%w: branch transcript is append-only", ErrMemoryConflict)
		}
	}
	if next.Candidate != nil && old.Candidate != nil {
		if next.Candidate.Version < old.Candidate.Version || (next.Candidate.Version == old.Candidate.Version && next.Candidate.SHA256 != old.Candidate.SHA256) {
			return nil, fmt.Errorf("%w: stale candidate version", ErrMemoryConflict)
		}
	}
	if next.Status == "prepared" || next.Status == "kept" {
		if next.Final == nil || old.Review == nil || !reflect.DeepEqual(next.Final, old.Review) || next.Rounds <= old.Review.OpenedRound {
			return nil, fmt.Errorf("%w: completion requires the previously opened confirmation", ErrMemoryConflict)
		}
		if next.Status == "prepared" {
			if next.Final.Kind != "replace" || next.Candidate == nil || next.Candidate.Version != next.Final.Version || next.Candidate.SHA256 != next.Final.SHA256 || old.Candidate == nil || !reflect.DeepEqual(old.Candidate, next.Candidate) {
				return nil, fmt.Errorf("%w: confirmed candidate changed", ErrMemoryConflict)
			}
			if estTextTokens(next.Candidate.Text) >= b.Chunk.EstTokens {
				return nil, fmt.Errorf("%w: candidate does not reduce the target", ErrBadRequest)
			}
			_, err = tx.Exec(ctx, `UPDATE core_memory_chunks SET status='prepared',replacement=$3,replacement_est_tokens=$4,prepared_at=now(),claimed_generation=NULL,claimed_at=NULL,last_error=NULL WHERE persona_id=$1 AND chunk_seq=$2`, persona, seq, next.Candidate.Text, estTextTokens(next.Candidate.Text))
		} else {
			if next.Final.Kind != "keep" {
				return nil, fmt.Errorf("%w: keep confirmation required", ErrMemoryConflict)
			}
			_, err = tx.Exec(ctx, `UPDATE core_memory_chunks SET status='kept',prepared_at=now(),claimed_generation=NULL,claimed_at=NULL,last_error=NULL WHERE persona_id=$1 AND chunk_seq=$2`, persona, seq)
		}
		if err != nil {
			return nil, err
		}
	}
	if len(b.State) == 0 && len(b.PreviousAttempt) > 0 {
		var prior branchCheckpoint
		if json.Unmarshal(b.PreviousAttempt, &prior) == nil {
			old.Issue = prior.Issue
		}
	}
	if !reflect.DeepEqual(old.Issue, next.Issue) {
		if old.Issue != nil && next.Issue == nil {
			err = s.memoryBranchNotice(ctx, tx, persona, seq, expected+1, "recovered", old.Issue)
		}
		if next.Issue != nil {
			err = s.memoryBranchNotice(ctx, tx, persona, seq, expected+1, "occurred", next.Issue)
		}
		if err != nil {
			return nil, err
		}
	}
	_, err = tx.Exec(ctx, `UPDATE core_memory_branches SET state=$3,revision=revision+1,checkpoint_hash=$4,status=$5,retry_at=$6,issue=$7,updated_at=now() WHERE persona_id=$1 AND chunk_seq=$2`, persona, seq, string(raw), digest, next.Status, next.RetryAt, next.Issue)
	if err != nil {
		return nil, err
	}
	result, err := s.readMemoryBranch(ctx, tx, persona, seq)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}
func (s *Store) memoryBranchNotice(ctx context.Context, tx pgx.Tx, persona string, seq, revision int64, transition string, issue *branchIssue) error {
	text := fmt.Sprintf("[Memory mechanism occurred] %s Draft and branch progress are saved.", issue.Message)
	if transition == "recovered" {
		text = fmt.Sprintf("[Memory mechanism recovered] Memory preparation has progressed past the previously reported %s problem.", issue.Code)
	}
	payload := map[string]any{"text": text, "chunk_seq": seq, "transition": transition, "code": issue.Code}
	_, err := tx.Exec(ctx, `INSERT INTO core_inputs(persona_id,input_id,kind,payload,actor_kind,source_surface,attention,status) VALUES($1,$2,'memory_status',$3,'memory','core_memory','observe','queued') ON CONFLICT(persona_id,input_id) DO NOTHING`, persona, fmt.Sprintf("memory:%d:%d:%s", seq, revision, transition), payload)
	return err
}

// Reconsider only a kept range after genuinely new live material; neither a
// wake nor a new generation is grounds for rerunning an unchanged decision.
func (s *Store) reconsiderKeptMemory(ctx context.Context, tx pgx.Tx, persona string, snap BranchSnapshot) error {
	var pressure [3]int64
	counted := map[int]bool{}
	for _, r := range snap.Ranges {
		if r.MessageIndex != nil && !counted[*r.MessageIndex] {
			counted[*r.MessageIndex] = true
			layer := 0
			if r.Layer != nil {
				layer = *r.Layer
			}
			if layer >= 0 && layer < len(pressure) {
				pressure[layer] += estTextTokens(string(snap.Messages[*r.MessageIndex]))
			}
		}
	}
	if pressure[0] <= L0LiveLimitTokens && pressure[1] <= L1LimitTokens && pressure[2] <= L2LimitTokens {
		return nil
	}
	rows, err := tx.Query(ctx, `SELECT b.chunk_seq FROM core_memory_branches b JOIN core_memory_chunks c USING(persona_id,chunk_seq) WHERE b.persona_id=$1 AND b.status='kept' AND c.status='kept' ORDER BY b.chunk_seq`, persona)
	if err != nil {
		return err
	}
	var candidates []int64
	for rows.Next() {
		var seq int64
		if err = rows.Scan(&seq); err != nil {
			rows.Close()
			return err
		}
		candidates = append(candidates, seq)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	for _, seq := range candidates {
		b, err := s.readMemoryBranch(ctx, tx, persona, seq)
		if err != nil {
			return err
		}
		matches, err := s.snapshotMatchesChunk(ctx, tx, persona, b.Chunk, snap)
		if err != nil {
			return err
		}
		if !matches {
			continue
		}
		var last int64
		for _, r := range b.Snapshot.Ranges {
			if r.LastSeq > last {
				last = r.LastSeq
			}
		}
		var added int64
		seen := map[int]bool{}
		for _, r := range snap.Ranges {
			if r.FirstSeq > last && r.MessageIndex != nil && !seen[*r.MessageIndex] {
				seen[*r.MessageIndex] = true
				added += estTextTokens(string(snap.Messages[*r.MessageIndex]))
			}
		}
		if added < L0ChunkMinTokens {
			continue
		}
		var historyText string
		if err = tx.QueryRow(ctx, `SELECT previous_attempts FROM core_memory_branches WHERE persona_id=$1 AND chunk_seq=$2`, persona, seq).Scan(&historyText); err != nil {
			return err
		}
		var history []json.RawMessage
		if err = json.Unmarshal([]byte(historyText), &history); err != nil {
			return err
		}
		raw, err := json.Marshal(b)
		if err != nil {
			return err
		}
		history = append(history, raw)
		raw, err = json.Marshal(history)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE core_memory_branches SET previous_attempts=$3 WHERE persona_id=$1 AND chunk_seq=$2`, persona, seq, string(raw)); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE core_memory_chunks SET status='sealed',prepared_at=NULL WHERE persona_id=$1 AND chunk_seq=$2`, persona, seq)
		return err
	}
	return nil
}

type MemoryBranchStatus struct {
	ChunkSeq    int64        `json:"chunk_seq"`
	Status      string       `json:"status"`
	Revision    int64        `json:"revision"`
	RetryAt     *time.Time   `json:"retry_at"`
	Issue       *branchIssue `json:"issue"`
	PauseReason *string      `json:"pause_reason"`
}
