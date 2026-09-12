-- A derived disposition never replaces the authenticated incoming command.
CREATE TABLE notification_reflex (
  command_id TEXT PRIMARY KEY REFERENCES inbound_commands(command_id),
  decision_key_ref TEXT NOT NULL REFERENCES data_keys(key_ref),
  decision_ciphertext BLOB NOT NULL,
  ready_at_ms INTEGER NOT NULL
);
CREATE INDEX notification_reflex_ready ON notification_reflex(ready_at_ms);
