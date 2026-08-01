-- D6: the control plane is the authority for persistent approval rules.
-- `approval_rules` remains the durable audit/proposal log written by
-- ApproveAlways decisions; only a verified bundle in this singleton cache can
-- authorize rules after restart.
CREATE TABLE approval_policy_cache (
  singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
  tenant_id TEXT NOT NULL,
  personality_agent_id TEXT NOT NULL
    CHECK (sumi_is_canonical_uuid_v7(personality_agent_id) = 1),
  version INTEGER NOT NULL CHECK (version >= 0),
  issued_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  key_id TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  signature BLOB NOT NULL CHECK (length(signature) = 64),
  installed_at TEXT NOT NULL,
  CHECK (expires_at > issued_at)
);
