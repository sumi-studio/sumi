-- Host-declared: the attached desktop host can run delegated Jev browser goals.
-- Refreshed on every authenticated host poll; no Jev credential is stored here.
ALTER TABLE browser_tab_attachments ADD COLUMN jev_available boolean NOT NULL DEFAULT false;
