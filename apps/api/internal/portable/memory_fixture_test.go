package portable

// Storage-layout fixtures. The removed one-response compaction API is not
// available in production; revisioned branch execution is tested separately.
import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"testing"
)

type claimedMemoryFixture struct {
	Chunk           *agentstate.MemoryChunk
	TargetEvents    []agentstate.Event
	TargetFragments []agentstate.MemoryBlock
}

func memoryFixtureFence(p placement, ctx context.Context, id string, g int64) error {
	var current int64
	var authority string
	e := p.pool.QueryRow(ctx, `SELECT l.generation,p.authority FROM core_personas p JOIN core_writer_leases l USING (persona_id) WHERE p.persona_id=$1`, id).Scan(&current, &authority)
	if e != nil {
		return e
	}
	if current != g || authority != "active" {
		return agentstate.ErrGenerationFence
	}
	return nil
}
func claimMemoryFixture(t *testing.T, p placement, ctx context.Context, id string, g int64, _ int) (*claimedMemoryFixture, error) {
	if e := memoryFixtureFence(p, ctx, id, g); e != nil {
		return nil, e
	}
	_, e := p.pool.Exec(ctx, `UPDATE core_memory_chunks SET status='sealed',interruptions=interruptions+1,claimed_generation=NULL,claimed_at=NULL,not_before=now()+interval '200 milliseconds' WHERE persona_id=$1 AND status='preparing'`, id)
	if e != nil {
		return nil, e
	}
	var n int64
	e = p.pool.QueryRow(ctx, `SELECT chunk_seq FROM core_memory_chunks WHERE persona_id=$1 AND status='sealed' AND (not_before IS NULL OR not_before<=now()) ORDER BY chunk_seq LIMIT 1`, id).Scan(&n)
	if e == pgx.ErrNoRows {
		return &claimedMemoryFixture{}, nil
	}
	if e != nil {
		return nil, e
	}
	_, e = p.pool.Exec(ctx, `UPDATE core_memory_chunks SET status='preparing',claimed_generation=$3,claimed_at=now() WHERE persona_id=$1 AND chunk_seq=$2`, id, n, g)
	if e != nil {
		return nil, e
	}
	c := chunkRow(t, p, id, n)
	b := &claimedMemoryFixture{Chunk: &c}
	for _, src := range c.Sources {
		v := chunkRow(t, p, id, src)
		b.TargetFragments = append(b.TargetFragments, agentstate.MemoryBlock{ChunkSeq: v.ChunkSeq, Layer: v.Layer, FirstSeq: v.FirstSeq, LastSeq: v.LastSeq, Text: *v.Replacement, EstTokens: *v.ReplacementEstTokens})
	}
	return b, nil
}
func completeMemoryFixture(t *testing.T, p placement, ctx context.Context, id string, g, n int64, text string, keep bool) (int, error) {
	if e := memoryFixtureFence(p, ctx, id, g); e != nil {
		return 0, e
	}
	status := "prepared"
	var repl any = text
	var est any = (len(text) + 3) / 4
	if keep {
		status = "kept"
		repl = nil
		est = nil
	}
	_, e := p.pool.Exec(ctx, `UPDATE core_memory_chunks SET status=$3,replacement=$4,replacement_est_tokens=$5,prepared_at=now(),claimed_generation=NULL,claimed_at=NULL WHERE persona_id=$1 AND chunk_seq=$2`, id, n, status, repl, est)
	return 1, e
}
func failMemoryFixture(t *testing.T, p placement, ctx context.Context, id string, g, n int64, text string, retry bool) (int, error) {
	if e := memoryFixtureFence(p, ctx, id, g); e != nil {
		return 0, e
	}
	status := "failed"
	if retry {
		status = "sealed"
	}
	_, e := p.pool.Exec(ctx, `UPDATE core_memory_chunks SET status=$3,last_error=$4,attempts=attempts+1,claimed_generation=NULL,claimed_at=NULL,not_before=CASE WHEN $5 THEN now()+interval '200 milliseconds' ELSE NULL END WHERE persona_id=$1 AND chunk_seq=$2`, id, n, status, text, retry)
	return 1, e
}
