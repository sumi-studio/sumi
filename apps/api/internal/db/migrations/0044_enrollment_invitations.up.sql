-- Sumi enrollment is separate from membership in any Workspace.
CREATE TABLE enrollment_invites (
    invite_id uuidv7 PRIMARY KEY,
    token_hash bytea NOT NULL UNIQUE CHECK (octet_length(token_hash)=32),
    issued_by uuidv7 NOT NULL REFERENCES humans(human_id),
    email text,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    consumed_at timestamptz,
    consumed_by uuidv7 REFERENCES humans(human_id),
    CHECK ((consumed_at IS NULL) = (consumed_by IS NULL))
);
ALTER TABLE auth_flows ADD COLUMN enrollment_invite_id uuidv7 REFERENCES enrollment_invites(invite_id);
ALTER TABLE auth_flows ADD COLUMN verified_email text;
ALTER TABLE auth_flows ADD COLUMN email_verified boolean NOT NULL DEFAULT false;
