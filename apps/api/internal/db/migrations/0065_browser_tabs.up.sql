-- One explicit standing grant to one live, browser-owned tab incarnation.
-- Credentials authorize only that tab's host poll/result routes, never Core.
CREATE TABLE browser_tab_attachments (
 attachment_id uuid PRIMARY KEY,
 human_id uuidv7 NOT NULL REFERENCES humans(human_id),
 persona_id uuidv7 NOT NULL REFERENCES core_personas(persona_id),
 name text NOT NULL,
 tab jsonb NOT NULL,
 host_token_hash bytea NOT NULL,
 allow_actions boolean NOT NULL DEFAULT false,
 enabled boolean NOT NULL DEFAULT true,
 last_seen_at timestamptz NOT NULL DEFAULT 'epoch',
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX browser_tabs_persona ON browser_tab_attachments(persona_id) WHERE enabled;
CREATE UNIQUE INDEX browser_tab_identity ON browser_tab_attachments(human_id, (tab->>'runtimeId'), (tab->>'tabId')) WHERE enabled;
