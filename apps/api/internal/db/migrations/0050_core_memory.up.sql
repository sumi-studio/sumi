-- Memory layer for the shared TypeScript secretary core (engineering plan
-- M06; docs/agent/memory.md and memory-preparation-and-replacement-2026-09-08).
--
-- The canonical life log stays in core_events — journal rows are never
-- deleted or rewritten by memory maintenance. A chunk is a sealed contiguous
-- journal range whose raw events may later be replaced in the sent context by
-- a prepared L1 replacement text. Sealing, preparation, and application are
-- separate durable states so a crash or a correction arriving mid-preparation
-- can never lose or reorder the record.
--
-- Status machine (writer-generation fenced):
--   sealed    — range cut at a safe boundary, waiting for preparation
--   preparing — claimed by a writer generation for the L1 branch (one at a
--               time); reverts to sealed when a new generation recovers
--   prepared  — replacement candidate on the shelf; NOT in the sent context
--   applied   — replacement text renders at the chunk's original position
--   kept      — the model answered KEEP_UNCHANGED, or its replacement did not
--               shrink the range; originals stay, never reprepared
--   failed    — retry budget exhausted; originals stay and the failure is
--               visible in memory status rather than silently skipped
--
-- attempts counts recorded preparation failures only. interruptions counts
-- claims that ended without any recorded outcome (host stopped, writer
-- fenced, claim response lost); they are paced and bounded separately so a
-- slow-but-finite preparation is never failed by its host's lifecycle.
CREATE TABLE core_memory_chunks (
    persona_id             uuidv7      NOT NULL REFERENCES core_personas(persona_id) ON DELETE CASCADE,
    chunk_seq              bigint      NOT NULL,
    layer                  smallint    NOT NULL DEFAULT 1,
    first_seq              bigint      NOT NULL,
    last_seq               bigint      NOT NULL,
    est_tokens             bigint      NOT NULL,
    status                 text        NOT NULL
        CHECK (status IN ('sealed','preparing','prepared','applied','kept','failed')),
    replacement            text,
    replacement_est_tokens bigint,
    attempts               int         NOT NULL DEFAULT 0,
    interruptions          int         NOT NULL DEFAULT 0,
    last_error             text,
    claimed_generation     bigint,
    claimed_at             timestamptz,
    not_before             timestamptz,
    created_at             timestamptz NOT NULL DEFAULT now(),
    prepared_at            timestamptz,
    applied_at             timestamptz,
    PRIMARY KEY (persona_id, chunk_seq),
    UNIQUE (persona_id, first_seq),
    CHECK (first_seq <= last_seq)
);
CREATE INDEX core_memory_chunks_cover
    ON core_memory_chunks(persona_id, last_seq);

-- The journal seq of an input's one input_received event. A turn journals
-- its input at commit, but a state-internal effect (journal.note) lands in
-- the journal mid-turn; that effect first journals the input that caused it,
-- so the note follows its input in seq order and falls in the same memory
-- chunk. Commit then skips the already-journaled input, keeping exactly one
-- input_received per input across crashes and retries.
ALTER TABLE core_inputs ADD COLUMN received_seq bigint;
