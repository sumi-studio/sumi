
-- Current-state lookups must not scan the person's inactive history at boot.
CREATE INDEX inbound_status_run ON inbound_commands(status, run_id);
CREATE INDEX tools_state_run ON tool_executions(state, run_id);
CREATE INDEX approvals_state_run ON approval_log(state, run_id);
CREATE INDEX event_run_seq ON agent_events(json_extract(internal_metadata, '$.run_id'), seq);
CREATE INDEX event_current_turn ON agent_events(json_extract(internal_metadata, '$.run_id'), seq DESC)
WHERE event_type = 'turn_start';
CREATE INDEX event_owner_evidence ON agent_events(event_type,
  json_extract(internal_metadata, '$.run_id'), json_extract(internal_metadata, '$.turn_id'), seq);
CREATE INDEX event_tool_start ON agent_events(json_extract(envelope, '$.tool_call_id'))
WHERE event_type = 'tool_execution_start';

CREATE INDEX tools_run ON tool_executions(run_id);
CREATE INDEX approvals_run ON approval_log(run_id);
CREATE INDEX event_approval_request ON agent_events(json_extract(envelope, '$.request.id'))
WHERE event_type = 'approval_requested';

CREATE INDEX prepared_provider_mutations ON provider_context_mutations(prepared_at, mutation_id)
WHERE state = 'prepared';
CREATE INDEX current_memory_batches ON memory_batches(layer, state);
CREATE INDEX current_memory_jobs ON memory_jobs(status, kind);

CREATE INDEX memory_batches_state ON memory_batches(state, layer);

CREATE INDEX memory_jobs_first_source ON memory_jobs(kind, json_extract(source_ids, '$[0]'), status);
