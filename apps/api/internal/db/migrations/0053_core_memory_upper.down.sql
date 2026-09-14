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
