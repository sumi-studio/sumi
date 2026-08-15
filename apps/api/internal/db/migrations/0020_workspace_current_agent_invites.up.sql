-- Workspace invitations are a strict sum type.  Existing opaque-code invites
-- remain unchanged; the new variant targets the exact PersonalityAgent bound
-- into the issuing Human's signed browser session without storing a second
-- secret or allowing the browser to choose a PAID.
ALTER TABLE workspace_invites
    ALTER COLUMN code_hash DROP NOT NULL,
    ADD COLUMN invite_kind text NOT NULL DEFAULT 'share_code'
        CHECK (invite_kind IN ('share_code', 'targeted_personality_agent')),
    ADD COLUMN target_kind text
        CHECK (target_kind = 'personality_agent'),
    ADD COLUMN target_id uuidv7 REFERENCES agents(personality_agent_id);

ALTER TABLE workspace_invites
    ADD CONSTRAINT workspace_invites_strict_variant CHECK (
        (
            invite_kind = 'share_code'
            AND code_hash IS NOT NULL
            AND target_kind IS NULL
            AND target_id IS NULL
        )
        OR
        (
            invite_kind = 'targeted_personality_agent'
            AND code_hash IS NULL
            AND target_kind = 'personality_agent'
            AND target_id IS NOT NULL
        )
    ),
    ADD CONSTRAINT workspace_invites_target_redeemer CHECK (
        invite_kind <> 'targeted_personality_agent'
        OR redeemed_by_kind IS NULL
        OR (
            redeemed_by_kind = target_kind
            AND redeemed_by_id = target_id
        )
    );

-- Expired targeted invites are explicitly closed before replacement.  This
-- partial uniqueness constraint therefore makes concurrent issuance converge
-- on one active pending intent without using wall-clock time in an index.
CREATE UNIQUE INDEX workspace_invites_one_pending_targeted_pa
    ON workspace_invites (workspace_id, target_id)
    WHERE invite_kind = 'targeted_personality_agent'
      AND revoked_at IS NULL
      AND redeemed_at IS NULL;

-- The later PA-owned local-control list pages by the durable UUIDv7
-- invitation identity after binding the bearer to one exact target.  Keep the
-- index order identical to that keyset contract; no wall-clock ordering or
-- Workspace inference belongs in the PA lookup seam.
CREATE INDEX workspace_invites_pending_targeted_pa_by_invite
    ON workspace_invites (target_id, invite_id)
    WHERE invite_kind = 'targeted_personality_agent'
      AND revoked_at IS NULL
      AND redeemed_at IS NULL;
