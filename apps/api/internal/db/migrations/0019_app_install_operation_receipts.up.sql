-- Browser Participant install takeover uses a UUIDv4 operation identity. The
-- receipt is lifecycle history, not an installation child: it intentionally
-- survives uninstall so a delayed request cannot recreate a removed binding.
CREATE TABLE app_install_operation_receipts (
    owner_kind       text        NOT NULL
        CHECK (owner_kind IN ('workspace', 'human', 'personality_agent')),
    owner_id         uuidv7      NOT NULL,
    operation_id     uuid        NOT NULL,
    app_id           text        NOT NULL
        CHECK (app_id ~ '^[a-z][a-z0-9-]{0,63}$'),
    status           text        NOT NULL
        CHECK (status IN ('pending', 'installed', 'already_installed')),
    installation_id  uuidv7,
    enabled          boolean,
    authority_epoch  bigint,
    installed_at     timestamptz,
    updated_at       timestamptz,
    created_at       timestamptz NOT NULL,
    completed_at     timestamptz,
    PRIMARY KEY (owner_kind, owner_id, operation_id),
    CHECK (
        (
            status = 'pending'
            AND installation_id IS NULL
            AND enabled IS NULL
            AND authority_epoch IS NULL
            AND installed_at IS NULL
            AND updated_at IS NULL
            AND completed_at IS NULL
        )
        OR (
            status = 'already_installed'
            AND installation_id IS NULL
            AND enabled IS NULL
            AND authority_epoch IS NULL
            AND installed_at IS NULL
            AND updated_at IS NULL
            AND completed_at IS NOT NULL
        )
        OR (
            status = 'installed'
            AND installation_id IS NOT NULL
            AND enabled IS TRUE
            AND authority_epoch = 1
            AND installed_at IS NOT NULL
            AND updated_at = installed_at
            AND completed_at IS NOT NULL
        )
    ),
    CHECK (completed_at IS NULL OR completed_at >= created_at)
);

-- The operation UUID is v4 (journal intent identity), while owner and
-- installation identities remain UUIDv7. Keep the canonical lowercase check
-- at the application boundary; this constraint prevents non-v4 receipts if a
-- future caller bypasses it.
ALTER TABLE app_install_operation_receipts
    ADD CONSTRAINT app_install_operation_receipts_uuidv4
    CHECK (
        operation_id::text ~
        '^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
    );
