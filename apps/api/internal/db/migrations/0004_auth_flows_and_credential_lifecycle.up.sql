-- 0004_auth_flows_and_credential_lifecycle: explicit proof/intent state and
-- durable login-method history for issue #135.

ALTER TABLE credentials
    ADD COLUMN active boolean NOT NULL DEFAULT true,
    ADD COLUMN unlinked_at timestamptz;

ALTER TABLE credentials
    ADD CONSTRAINT credentials_unlink_state
    CHECK ((active AND unlinked_at IS NULL) OR (NOT active AND unlinked_at IS NOT NULL));

-- Historical credential ownership is immutable. A disabled login method may
-- be re-enabled only for the same Human, never deleted or reassigned.
CREATE OR REPLACE FUNCTION protect_credential_history() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.provider IS DISTINCT FROM NEW.provider
       OR OLD.external_subject IS DISTINCT FROM NEW.external_subject
       OR OLD.human_id IS DISTINCT FROM NEW.human_id
       OR OLD.bound_at IS DISTINCT FROM NEW.bound_at THEN
        RAISE EXCEPTION 'credential identity and historical binding are immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER credential_history_immutable
    BEFORE UPDATE ON credentials
    FOR EACH ROW EXECUTE FUNCTION protect_credential_history();

CREATE OR REPLACE FUNCTION prevent_credential_delete() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'credential history cannot be deleted';
END;
$$;

CREATE TRIGGER credential_no_delete
    BEFORE DELETE ON credentials
    FOR EACH ROW EXECUTE FUNCTION prevent_credential_delete();

CREATE TABLE auth_flows (
    flow_id             uuidv7      PRIMARY KEY,
    nonce_hash          bytea       NOT NULL UNIQUE CHECK (octet_length(nonce_hash) = 32),
    intent              text        NOT NULL CHECK (intent IN ('sign_in', 'sign_up')),
    channel             text        NOT NULL CHECK (channel IN ('email_link', 'provider')),
    expected_provider   text        NOT NULL,
    normalized_email    text,
    continuation        text        NOT NULL,
    status              text        NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'confirmation_required', 'completed')),
    confirmation_action text CHECK (confirmation_action IN ('create_account', 'sign_in')),
    firebase_uid        text,
    provider_subject    text,
    human_id            uuidv7 REFERENCES humans(human_id),
    personality_agent_id uuidv7 REFERENCES agents(personality_agent_id),
    terminal_outcome    text CHECK (terminal_outcome IN ('signed_in', 'account_created')),
    created_at          timestamptz NOT NULL DEFAULT now(),
    proved_at           timestamptz,
    completed_at        timestamptz,
    expires_at          timestamptz NOT NULL,
    CHECK ((channel = 'email_link' AND normalized_email IS NOT NULL)
        OR (channel = 'provider' AND normalized_email IS NULL)),
    CHECK ((status = 'confirmation_required' AND confirmation_action IS NOT NULL
            AND firebase_uid IS NOT NULL AND proved_at IS NOT NULL)
        OR (status <> 'confirmation_required' AND confirmation_action IS NULL)),
    CHECK ((status = 'completed' AND terminal_outcome IS NOT NULL
            AND human_id IS NOT NULL AND personality_agent_id IS NOT NULL
            AND completed_at IS NOT NULL)
        OR (status <> 'completed' AND terminal_outcome IS NULL
            AND completed_at IS NULL))
);

CREATE INDEX auth_flows_expiry ON auth_flows (expires_at) WHERE status <> 'completed';

CREATE TABLE provider_operations (
    operation_id        uuidv7      PRIMARY KEY,
    nonce_hash          bytea       NOT NULL UNIQUE CHECK (octet_length(nonce_hash) = 32),
    human_id            uuidv7      NOT NULL REFERENCES humans(human_id),
    firebase_uid        text        NOT NULL,
    provider            text        NOT NULL,
    operation           text        NOT NULL CHECK (operation IN ('link', 'unlink')),
    status              text        NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'completed', 'failed')),
    decision_path       text        NOT NULL,
    terminal_outcome    text,
    created_at          timestamptz NOT NULL DEFAULT now(),
    expires_at          timestamptz NOT NULL,
    completed_at        timestamptz,
    CHECK ((status = 'pending' AND terminal_outcome IS NULL AND completed_at IS NULL)
        OR (status <> 'pending' AND terminal_outcome IS NOT NULL AND completed_at IS NOT NULL))
);

CREATE TABLE credential_security_events (
    event_id            bigserial    PRIMARY KEY,
    operation_id        uuidv7       REFERENCES provider_operations(operation_id),
    human_id            uuidv7       NOT NULL REFERENCES humans(human_id),
    provider            text         NOT NULL,
    event_type          text         NOT NULL CHECK (event_type IN ('provider_linked', 'provider_unlinked', 'provider_link_failed', 'provider_unlink_failed')),
    decision_path       text         NOT NULL,
    terminal_outcome    text         NOT NULL,
    occurred_at         timestamptz  NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION prevent_security_event_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'credential security events are append-only';
END;
$$;

CREATE TRIGGER credential_security_events_append_only
    BEFORE UPDATE OR DELETE ON credential_security_events
    FOR EACH ROW EXECUTE FUNCTION prevent_security_event_mutation();
