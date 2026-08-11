-- Direct Chat transport authority must not revive when an installation is
-- disabled and later re-enabled under the same installation_id.  The epoch is
-- transport control metadata: clients seal the exact value observed at bind
-- time and app admission revalidates it before every effect.
ALTER TABLE app_installations
    ADD COLUMN authority_epoch bigint NOT NULL DEFAULT 1
        CHECK (authority_epoch >= 1);
