DROP TABLE IF EXISTS core_tool_approvals;

ALTER TABLE core_operations DROP CONSTRAINT core_operations_status_check;
ALTER TABLE core_operations ADD CONSTRAINT core_operations_status_check
    CHECK (status IN ('running','done','failed'));

ALTER TABLE core_turns DROP CONSTRAINT core_turns_status_check;
ALTER TABLE core_turns ADD CONSTRAINT core_turns_status_check
    CHECK (status IN ('running','done','interrupted','failed'));

ALTER TABLE core_inputs DROP CONSTRAINT core_inputs_status_check;
ALTER TABLE core_inputs ADD CONSTRAINT core_inputs_status_check
    CHECK (status IN ('queued','claimed','done'));
