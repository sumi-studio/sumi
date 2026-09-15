DROP INDEX auth_flows_completed_email_code_proof;
DROP TABLE auth_email_deliveries;
DROP TABLE auth_email_challenges;
DELETE FROM auth_flows WHERE channel = 'email_code';

ALTER TABLE auth_flows DROP CONSTRAINT auth_flows_email_proof_state;
ALTER TABLE auth_flows
    DROP COLUMN adopted_from_nonce_hash,
    DROP COLUMN email_proof_uid_bound_at,
    DROP COLUMN email_proof_uid,
    DROP COLUMN email_proof_method,
    DROP COLUMN email_proved_at;

ALTER TABLE auth_flows DROP CONSTRAINT auth_flows_check;
ALTER TABLE auth_flows ADD CONSTRAINT auth_flows_check
    CHECK ((channel = 'email_link' AND normalized_email IS NOT NULL)
        OR (channel = 'provider' AND normalized_email IS NULL));

ALTER TABLE auth_flows DROP CONSTRAINT auth_flows_channel_check;
ALTER TABLE auth_flows ADD CONSTRAINT auth_flows_channel_check
    CHECK (channel IN ('email_link', 'provider'));
