ALTER TABLE enrollment_invites ADD COLUMN workspace_invite_id uuidv7 UNIQUE REFERENCES workspace_invites(invite_id);
ALTER TABLE workspace_invites ADD COLUMN reserved_human_id uuidv7 REFERENCES humans(human_id);
