-- Sumi-owned Cloud browser profiles. One enabled profile per continuing
-- secretary, owned by its human. The browser itself runs in a Cloudflare
-- Browser Run session; this row is the canonical record of its incarnations
-- and of the last semantic checkpoint (cookies, per-origin storage, tabs),
-- sealed with a key derived from SUMI_MODEL_CONNECTION_KEY. Frames and
-- heartbeats are never stored.
CREATE TABLE cloud_browser_profiles (
 profile_id uuid PRIMARY KEY,
 human_id uuidv7 NOT NULL REFERENCES humans(human_id),
 persona_id uuidv7 NOT NULL REFERENCES core_personas(persona_id),
 enabled boolean NOT NULL DEFAULT true,
 -- Increments whenever a new remote browser is created for the profile.
 -- Observations, tickets and admitted work from an older incarnation are refused.
 incarnation bigint NOT NULL DEFAULT 0,
 state text NOT NULL DEFAULT 'sleeping' CHECK (state IN ('sleeping','live','lost')),
 state_at timestamptz NOT NULL DEFAULT now(),
 -- Stable tab slot ids of the latest checkpoint (no URLs or titles).
 tab_ids jsonb NOT NULL DEFAULT '[]',
 snapshot_seq bigint NOT NULL DEFAULT 0,
 snapshot_version int,
 snapshot_incarnation bigint,
 snapshot bytea,
 snapshot_bytes int,
 snapshot_at timestamptz,
 -- Grants or credentials changed while the host may be live: deliver them.
 refresh_requested_at timestamptz,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX cloud_browser_profile_persona ON cloud_browser_profiles(persona_id) WHERE enabled;
CREATE INDEX cloud_browser_profile_human ON cloud_browser_profiles(human_id) WHERE enabled;

-- Optional user-scoped Jev operation-layer key for Cloud goals (BYOK). The
-- direct browser.observe/act path never needs it.
CREATE TABLE cloud_browser_jev_credentials (
 human_id uuidv7 PRIMARY KEY REFERENCES humans(human_id),
 sealed bytea NOT NULL,
 rejected boolean NOT NULL DEFAULT false,
 updated_at timestamptz NOT NULL DEFAULT now()
);
