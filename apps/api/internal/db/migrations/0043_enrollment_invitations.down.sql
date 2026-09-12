ALTER TABLE workspace_invites DROP COLUMN reserved_human_id;
ALTER TABLE enrollment_invites DROP COLUMN workspace_invite_id;
ALTER TABLE auth_flows DROP COLUMN email_verified, DROP COLUMN verified_email, DROP COLUMN enrollment_invite_id;
DROP TABLE enrollment_invites;
