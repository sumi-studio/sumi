-- 0057_email_code_auth: Sumi-owned mailbox proof for email sign-in. One
-- emailed challenge carries both a 6-digit code (primary) and a link token.
-- Secrets are derived with a server-held HMAC key from challenge_id, so no
-- raw code, token, or ciphertext is stored. Firebase UID remains the Human key.

ALTER TABLE auth_flows DROP CONSTRAINT auth_flows_channel_check;
ALTER TABLE auth_flows ADD CONSTRAINT auth_flows_channel_check
    CHECK (channel IN ('email_link', 'email_code', 'provider'));

ALTER TABLE auth_flows DROP CONSTRAINT auth_flows_check;
ALTER TABLE auth_flows ADD CONSTRAINT auth_flows_check
    CHECK ((channel IN ('email_link', 'email_code') AND normalized_email IS NOT NULL)
        OR (channel = 'provider' AND normalized_email IS NULL));

ALTER TABLE auth_flows
    ADD COLUMN email_proved_at timestamptz,
    ADD COLUMN email_proof_method text CHECK (email_proof_method IN ('code', 'link')),
    ADD COLUMN email_proof_uid text
        CHECK (email_proof_uid IS NULL OR char_length(email_proof_uid) BETWEEN 1 AND 128),
    ADD COLUMN email_proof_uid_bound_at timestamptz,
    -- A link finished in another browser rebinds the flow authority to that
    -- browser's nonce. The original nonce may then only learn that fact.
    ADD COLUMN adopted_from_nonce_hash bytea UNIQUE
        CHECK (adopted_from_nonce_hash IS NULL OR octet_length(adopted_from_nonce_hash) = 32);

ALTER TABLE auth_flows ADD CONSTRAINT auth_flows_email_proof_state CHECK (
    (channel = 'email_code' OR (email_proved_at IS NULL AND email_proof_uid IS NULL
        AND adopted_from_nonce_hash IS NULL))
    AND ((email_proved_at IS NULL) = (email_proof_method IS NULL))
    AND ((email_proof_uid IS NULL) = (email_proof_uid_bound_at IS NULL))
    AND (email_proof_uid IS NULL OR email_proved_at IS NOT NULL)
    AND (adopted_from_nonce_hash IS NULL OR email_proof_method = 'link')
    AND (channel <> 'email_code' OR status = 'pending' OR email_proof_uid IS NOT NULL)
    AND (channel <> 'email_code' OR firebase_uid IS NULL OR firebase_uid = email_proof_uid)
);

CREATE TABLE auth_email_challenges (
    challenge_id          uuidv7      PRIMARY KEY,
    flow_id               uuidv7      NOT NULL REFERENCES auth_flows(flow_id),
    normalized_email      text        NOT NULL,
    key_id                text        NOT NULL CHECK (char_length(key_id) BETWEEN 1 AND 64),
    failed_code_attempts  integer     NOT NULL DEFAULT 0 CHECK (failed_code_attempts BETWEEN 0 AND 5),
    created_at            timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at            timestamptz NOT NULL,
    superseded_at         timestamptz,
    consumed_at           timestamptz,
    consumed_method       text CHECK (consumed_method IN ('code', 'link')),
    CHECK (expires_at > created_at),
    CHECK ((consumed_at IS NULL) = (consumed_method IS NULL)),
    CHECK (consumed_at IS NULL OR superseded_at IS NULL)
);

-- A resend either reuses the live challenge or supersedes it.
CREATE UNIQUE INDEX auth_email_challenges_one_live
    ON auth_email_challenges (flow_id) WHERE superseded_at IS NULL AND consumed_at IS NULL;

-- Durable send intent. It commits with its challenge; delivery runs outside
-- that transaction and is at-least-once.
CREATE TABLE auth_email_deliveries (
    delivery_id       uuidv7      PRIMARY KEY,
    challenge_id      uuidv7      NOT NULL REFERENCES auth_email_challenges(challenge_id),
    normalized_email  text        NOT NULL,
    status            text        NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'sending', 'sent', 'failed', 'cancelled')),
    attempts          integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    created_at        timestamptz NOT NULL DEFAULT clock_timestamp(),
    next_attempt_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_expires_at  timestamptz,
    finished_at       timestamptz,
    failure_class     text CHECK (failure_class IN ('permanent', 'retries_exhausted')),
    CHECK ((status = 'sending') = (lease_expires_at IS NOT NULL)),
    CHECK ((status IN ('sent', 'failed', 'cancelled')) = (finished_at IS NOT NULL)),
    CHECK ((status = 'failed') = (failure_class IS NOT NULL))
);

CREATE INDEX auth_email_deliveries_due
    ON auth_email_deliveries (next_attempt_at) WHERE status IN ('pending', 'sending');
CREATE INDEX auth_email_deliveries_address_window
    ON auth_email_deliveries (normalized_email, created_at);
CREATE INDEX auth_email_deliveries_challenge
    ON auth_email_deliveries (challenge_id, created_at);

CREATE INDEX auth_flows_completed_email_code_proof
    ON auth_flows (human_id, firebase_uid)
    WHERE channel = 'email_code' AND status = 'completed';
