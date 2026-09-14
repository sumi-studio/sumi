-- Admission order for core inputs. A secretary resolves queued inputs in the
-- order they were accepted: two messages in one conversation must not swap.
-- created_at is the wall-clock received time and can step backward (observed
-- on the WSL2 development host), so it records when an input arrived but no
-- longer decides which input runs first. The identity sequence is assigned at
-- insert and only moves forward.
ALTER TABLE core_inputs ADD COLUMN admission_seq bigint GENERATED ALWAYS AS IDENTITY;

DROP INDEX core_inputs_pending;
CREATE INDEX core_inputs_pending ON core_inputs(persona_id, admission_seq)
    WHERE status = 'queued';
