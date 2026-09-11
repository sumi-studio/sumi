INSERT INTO app_catalog (app_id, display_name, workspace_owner_allowed, participant_owner_allowed)
VALUES ('feedback', 'Feedback', false, true);

-- Feedback is addressed to people, independent of source workspace membership.
-- App uninstall removes an installation, never the conversation.
CREATE TABLE feedback_threads (
    thread_id uuidv7 PRIMARY KEY,
    author_key text NOT NULL,
    author jsonb NOT NULL,
    title text NOT NULL CHECK (char_length(title) BETWEEN 1 AND 160),
    body text NOT NULL CHECK (char_length(body) BETWEEN 1 AND 20000),
    status text NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'resolved')),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX feedback_threads_updated ON feedback_threads (updated_at DESC, thread_id DESC);
CREATE INDEX feedback_threads_author ON feedback_threads (author_key, updated_at DESC, thread_id DESC);
CREATE TABLE feedback_events (
    event_id uuidv7 PRIMARY KEY,
    thread_id uuidv7 NOT NULL REFERENCES feedback_threads(thread_id),
    revision bigint NOT NULL CHECK (revision > 1),
    kind text NOT NULL CHECK (kind IN ('message', 'status')),
    author jsonb NOT NULL,
    body text,
    status text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((kind = 'message' AND body IS NOT NULL AND char_length(body) BETWEEN 1 AND 20000 AND status IS NULL)
        OR (kind = 'status' AND body IS NULL AND status IN ('open', 'resolved'))),
    UNIQUE (thread_id, revision)
);
CREATE TABLE feedback_reads (
    thread_id uuidv7 NOT NULL REFERENCES feedback_threads(thread_id),
    reader_key text NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0),
    PRIMARY KEY (thread_id, reader_key)
);
-- Receipts retain the actual response, so an ambiguous retry cannot add a second
-- conversation or message, even after the thread has changed or been resolved.
CREATE TABLE feedback_requests (
    actor_key text NOT NULL,
    request_id uuid NOT NULL,
    fingerprint text NOT NULL,
    response jsonb NOT NULL,
    PRIMARY KEY (actor_key, request_id)
);

CREATE TABLE feedback_attention_outbox (
    event_id uuidv7 NOT NULL,
    recipient_paid uuidv7 NOT NULL REFERENCES agents(personality_agent_id),
    thread_id uuidv7 NOT NULL REFERENCES feedback_threads(thread_id),
    payload jsonb NOT NULL,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    outcome text CHECK (outcome IN ('admitted','suppressed')),
    PRIMARY KEY(event_id,recipient_paid)
);
CREATE INDEX feedback_attention_pending ON feedback_attention_outbox(next_attempt_at,event_id) WHERE finished_at IS NULL;
