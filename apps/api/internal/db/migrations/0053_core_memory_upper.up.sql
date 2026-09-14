-- Upper memory layers for the shared secretary core (docs/agent/memory.md
-- steps 4–5, memory-boundaries-2026-09-08): L1→L2 consolidation and
-- L2-internal reintegration on the same chunk pipeline.
--
-- A layer-2 row is a consolidation target created by the writer's maintain
-- step from selected applied fragments. sources records the ordered
-- chunk_seqs it consumes — the accepted provenance of the replacement:
-- the target's [first_seq, last_seq] is exactly the sources' contiguous
-- span, selected sources alone supply the replacement text, and a 'kept'
-- target's source tuple is what re-selection dedups against. Ordinary
-- (L0→L1) chunks keep sources NULL.
--
-- One new status closes the lifecycle: 'superseded' marks a source whose
-- accepted fragment was replaced by an applied upper-layer block. The row
-- stays — the accepted decision and its text remain durable and auditable —
-- but its covered events are no longer rendered raw (the applied target
-- represents them) and it never counts toward the live raw estimate.
-- 'superseded' is only ever written together with the covering target's
-- 'applied' transition, so every superseded range is represented by an
-- applied row.
--
-- (persona_id, first_seq) uniqueness stays an L1 invariant, now expressed
-- as a partial index. It cannot extend across layers or statuses: an
-- upper target's first_seq is its first source's first_seq by design, and
-- an L2-internal reintegration target shares it with its L2 source while
-- that source is still applied. Within layer 2, overlap safety comes from
-- the pipeline itself — one in-flight upper target, atomic
-- source-supersede/target-apply in one transaction — not from the index.
ALTER TABLE core_memory_chunks
    DROP CONSTRAINT core_memory_chunks_status_check,
    DROP CONSTRAINT core_memory_chunks_persona_id_first_seq_key,
    ADD COLUMN sources bigint[],
    ADD CONSTRAINT core_memory_chunks_status_check
        CHECK (status IN ('sealed','preparing','prepared','applied','kept','failed','superseded')),
    -- Sources are the layer-2 target's own provenance: they resolve to
    -- same-persona chunks whose union is the target's range, all in the
    -- layer the target consumes. Enforced by the writer's single
    -- transaction at creation and by the portable verify checks; the
    -- CHECK here only rules out the two nonsensical shapes.
    ADD CONSTRAINT core_memory_chunks_layer_check CHECK (layer >= 1),
    ADD CONSTRAINT core_memory_chunks_sources_check
        CHECK (CASE WHEN layer = 1 THEN sources IS NULL
                    ELSE sources IS NOT NULL AND cardinality(sources) >= 1 END);

-- One chunk can claim a journal position per persona within layer 1; upper
-- layers share positions with their sources and their own reintegrations.
CREATE UNIQUE INDEX core_memory_chunks_l1_first_seq
    ON core_memory_chunks(persona_id, first_seq) WHERE layer = 1;
