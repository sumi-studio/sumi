-- Durable delivery of newly issued PA mentions and PA-owned reminders.
-- Source IDs deliberately survive deletion so pending delivery can be suppressed.
-- Existing reminders are not backfilled with guessed membership/installation tenure.
CREATE TABLE agent_attention_deliveries (
    event_id uuidv7 PRIMARY KEY,
    personality_agent_id uuidv7 NOT NULL,
    source_kind text NOT NULL CHECK (source_kind IN ('messaging_message', 'messaging_mention', 'reply_later_due')),
    source_id uuidv7 NOT NULL,
    source_revision bigint NOT NULL CHECK (source_revision > 0),
    workspace_id uuidv7 NOT NULL,
    installation_id uuidv7 NOT NULL,
    authority_epoch bigint NOT NULL CHECK (authority_epoch > 0),
    workspace_member_id uuidv7 NOT NULL,
    place_member_id uuidv7,
    place_id uuidv7 NOT NULL,
    message_id uuidv7 NOT NULL,
    payload jsonb NOT NULL,
    available_at timestamptz NOT NULL,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    admitted_command_id uuidv7,
    admitted_command_seq bigint,
    admitted_at timestamptz,
    cancellation_requested_at timestamptz,
    suppressed_at timestamptz,
    suppression_reason text,
    UNIQUE (personality_agent_id, source_kind, source_id, source_revision),
    CHECK ((admitted_at IS NULL) = (admitted_command_id IS NULL)),
    CHECK ((admitted_at IS NULL) = (admitted_command_seq IS NULL)),
    CHECK (admitted_command_seq IS NULL OR admitted_command_seq > 0),
    CHECK ((suppressed_at IS NULL) = (suppression_reason IS NULL)),
    CHECK (admitted_at IS NULL OR suppressed_at IS NULL)
);
CREATE INDEX agent_attention_ready ON agent_attention_deliveries
    (next_attempt_at, available_at, event_id)
    WHERE admitted_at IS NULL AND suppressed_at IS NULL;
