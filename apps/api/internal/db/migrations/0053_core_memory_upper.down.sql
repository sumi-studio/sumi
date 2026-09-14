-- Forward-only once upper-layer data exists: this rollback predates any
-- layer-2 rows. The restored UNIQUE(persona_id, first_seq) cannot build
-- while upper targets share first_seq with their sources, and the restored
-- status CHECK rejects 'superseded' — on such a database the migration
-- fails its constraint step rather than silently deleting upper memory.
-- That is intentional: upper-layer rows carry accepted memory decisions,
-- so there is no supported downgrade that discards them.
DROP INDEX core_memory_chunks_l1_first_seq;
ALTER TABLE core_memory_chunks
    DROP CONSTRAINT core_memory_chunks_status_check,
    DROP CONSTRAINT core_memory_chunks_layer_check,
    DROP CONSTRAINT core_memory_chunks_sources_check,
    DROP COLUMN sources,
    ADD CONSTRAINT core_memory_chunks_status_check
        CHECK (status IN ('sealed','preparing','prepared','applied','kept','failed')),
    ADD CONSTRAINT core_memory_chunks_persona_id_first_seq_key
        UNIQUE (persona_id, first_seq);
