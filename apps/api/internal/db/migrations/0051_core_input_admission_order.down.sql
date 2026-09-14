DROP INDEX core_inputs_pending;
CREATE INDEX core_inputs_pending ON core_inputs(persona_id, created_at)
    WHERE status = 'queued';

ALTER TABLE core_inputs DROP COLUMN admission_seq;
