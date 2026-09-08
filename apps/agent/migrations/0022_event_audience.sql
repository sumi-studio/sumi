-- Populated only by an explicitly authorized offline pre-external cutover.
-- Fresh databases and later events never acquire an implicit public audience.
CREATE TABLE legacy_event_audience (
  personality_agent_id TEXT PRIMARY KEY NOT NULL,
  through_seq INTEGER NOT NULL CHECK (through_seq >= 0)
);
