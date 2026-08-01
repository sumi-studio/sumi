-- 0003_agent_secrets: per-agent cryptographic material generated at
-- registration time (issue #121, ADR 0009 §3). The wrapping key is minted by
-- the trusted provisioning boundary when a Secretary is hired and stored here
-- so the control plane can provision it to the agent runtime on lazy spawn
-- (issue #123). It is distinct from the agent's identity, which is continuous
-- across restarts (ADR 0008).
CREATE TABLE agent_secrets (
    personality_agent_id uuidv7      PRIMARY KEY REFERENCES agents(personality_agent_id),
    wrapping_key         text        NOT NULL,
    created_at           timestamptz NOT NULL DEFAULT now()
);
