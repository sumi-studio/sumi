-- Durable private memory branches; no one-shot API compatibility.
CREATE TABLE public.core_memory_branches (
 persona_id public.uuidv7 NOT NULL,
 chunk_seq bigint NOT NULL,
 snapshot text NOT NULL,
  previous_attempts text NOT NULL DEFAULT '[]',
 state text,
 revision bigint NOT NULL DEFAULT 0,
 checkpoint_hash text NOT NULL DEFAULT '',
 status text NOT NULL DEFAULT 'running' CHECK (status IN ('running','paused','prepared','kept')),
 retry_at timestamptz,
 issue jsonb,
 updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(persona_id,chunk_seq),
 FOREIGN KEY(persona_id,chunk_seq) REFERENCES public.core_memory_chunks(persona_id,chunk_seq) ON DELETE CASCADE
);

-- Unapplied one-response work has no frozen parent context or versioned
-- confirmation. Withdraw it once at deployment; the next actual parent
-- consultation can prepare the range under the new protocol. Applied and
-- superseded memories, and their journal originals, remain untouched.
UPDATE public.core_memory_chunks
SET status = 'sealed', replacement = NULL, replacement_est_tokens = NULL,
    claimed_generation = NULL, claimed_at = NULL, prepared_at = NULL,
    not_before = NULL, attempts = 0, interruptions = 0, last_error = NULL
WHERE status IN ('preparing','prepared','kept','failed');

-- An already superseded source set cannot be prepared again.
UPDATE public.core_memory_chunks c
SET status = 'failed', last_error = 'source range already superseded before agentic preparation'
WHERE c.status = 'sealed' AND c.layer >= 2 AND EXISTS (
 SELECT 1 FROM public.core_memory_chunks s
 WHERE s.persona_id = c.persona_id AND s.chunk_seq = ANY(c.sources) AND s.status <> 'applied'
);
