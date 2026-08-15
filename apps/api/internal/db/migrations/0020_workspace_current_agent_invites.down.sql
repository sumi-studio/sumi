DROP INDEX IF EXISTS workspace_invites_pending_targeted_pa_by_invite;
DROP INDEX IF EXISTS workspace_invites_one_pending_targeted_pa;

-- The old schema has no representation for a non-secret targeted invitation.
-- Remove only that new variant before restoring code_hash to NOT NULL; all
-- historical opaque-code rows and their redemption records remain byte-for-
-- byte intact.
DELETE FROM workspace_invites WHERE invite_kind = 'targeted_personality_agent';

ALTER TABLE workspace_invites
    DROP CONSTRAINT IF EXISTS workspace_invites_target_redeemer,
    DROP CONSTRAINT IF EXISTS workspace_invites_strict_variant,
    DROP COLUMN IF EXISTS target_id,
    DROP COLUMN IF EXISTS target_kind,
    DROP COLUMN IF EXISTS invite_kind,
    ALTER COLUMN code_hash SET NOT NULL;
