-- 0008_workspace_core: application-wide Workspace, membership, role, invite,
-- and app-installation control plane. This file deliberately replaces the old
-- pre-cutover 0008_messaging_schema migration. Only a fresh/reset database may
-- cross this boundary; the migration runner rejects a database that recorded
-- the legacy version instead of attempting compatibility or adoption.
-- Messaging is a consumer of this schema; it does not own or seed Workspace
-- identity.

-- A Workspace has exactly one distinguished owner *membership*. The
-- owner_workspace_member_id foreign key is installed after workspace_members
-- because the two rows are created together with deferred constraint checking.
CREATE TABLE workspaces (
    workspace_id              uuidv7      PRIMARY KEY,
    name                      text        NOT NULL CHECK (char_length(name) BETWEEN 1 AND 200),
    owner_workspace_member_id uuidv7      NOT NULL UNIQUE,
    created_at                timestamptz NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, owner_workspace_member_id)
);

-- Leaving closes a tenure instead of reusing it. Every role assignment points
-- at workspace_member_id, so leaving and later rejoining can never resurrect
-- authority from an earlier tenure.
CREATE TABLE workspace_members (
    workspace_member_id uuidv7      PRIMARY KEY,
    workspace_id        uuidv7      NOT NULL REFERENCES workspaces(workspace_id),
    member_kind         text        NOT NULL
        CHECK (member_kind IN ('human', 'personality_agent')),
    member_id           uuidv7      NOT NULL,
    joined_at           timestamptz NOT NULL DEFAULT now(),
    left_at             timestamptz,
    CHECK (left_at IS NULL OR left_at >= joined_at),
    UNIQUE (workspace_id, workspace_member_id),
    -- Polymorphic consumers that retain the stable ParticipantRef alongside
    -- a tenure can bind all four values without a trigger.
    UNIQUE (workspace_id, workspace_member_id, member_kind, member_id)
);

ALTER TABLE workspaces
    ADD CONSTRAINT workspace_owner_is_own_membership
    FOREIGN KEY (workspace_id, owner_workspace_member_id)
    REFERENCES workspace_members (workspace_id, workspace_member_id)
    DEFERRABLE INITIALLY DEFERRED;

-- Ownership may move only to an active membership tenure in the same
-- Workspace. The application operation adds the current-owner authorization;
-- this trigger keeps direct SQL and future callers inside the structural
-- invariant as well.
CREATE OR REPLACE FUNCTION validate_workspace_owner_change()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.owner_workspace_member_id IS DISTINCT FROM OLD.owner_workspace_member_id
       AND NOT EXISTS (
           SELECT 1
           FROM workspace_members wm
           WHERE wm.workspace_id = NEW.workspace_id
             AND wm.workspace_member_id = NEW.owner_workspace_member_id
             AND wm.left_at IS NULL
       ) THEN
        RAISE EXCEPTION 'workspace owner must be an active membership tenure';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER workspace_owner_valid
    BEFORE UPDATE OF owner_workspace_member_id ON workspaces
    FOR EACH ROW
    EXECUTE FUNCTION validate_workspace_owner_change();

CREATE UNIQUE INDEX workspace_members_one_active_tenure
    ON workspace_members (workspace_id, member_kind, member_id)
    WHERE left_at IS NULL;

CREATE INDEX workspace_memberships_by_participant
    ON workspace_members (member_kind, member_id, workspace_id)
    WHERE left_at IS NULL;

-- PostgreSQL cannot express a polymorphic Human|PersonalityAgent foreign key.
-- Reject orphan memberships at the database boundary so every authorization
-- subject remains a real application participant even under direct SQL.
CREATE OR REPLACE FUNCTION validate_workspace_member_participant()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.member_kind = 'human' AND NOT EXISTS (
        SELECT 1 FROM humans WHERE human_id = NEW.member_id
    ) THEN
        RAISE EXCEPTION 'unknown Human workspace member';
    ELSIF NEW.member_kind = 'personality_agent' AND NOT EXISTS (
        SELECT 1 FROM agents WHERE personality_agent_id = NEW.member_id
    ) THEN
        RAISE EXCEPTION 'unknown PersonalityAgent workspace member';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER workspace_member_participant_exists
    BEFORE INSERT OR UPDATE OF member_kind, member_id ON workspace_members
    FOR EACH ROW
    EXECUTE FUNCTION validate_workspace_member_participant();

-- The distinguished owner membership and the participant it names are
-- immutable while that exact tenure owns the Workspace. After a transfer the
-- former owner becomes an ordinary active membership and may leave or be
-- removed through the normal operations.
CREATE OR REPLACE FUNCTION prevent_workspace_owner_membership_mutation()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' AND EXISTS (
        SELECT 1 FROM workspaces w
        WHERE w.owner_workspace_member_id = OLD.workspace_member_id
    ) THEN
        RAISE EXCEPTION 'workspace owner membership is immutable';
    END IF;
    IF TG_OP = 'UPDATE' AND EXISTS (
        SELECT 1 FROM workspaces w
        WHERE w.owner_workspace_member_id = OLD.workspace_member_id
    ) AND (
        NEW.workspace_id IS DISTINCT FROM OLD.workspace_id
        OR NEW.member_kind IS DISTINCT FROM OLD.member_kind
        OR NEW.member_id IS DISTINCT FROM OLD.member_id
        OR NEW.left_at IS DISTINCT FROM OLD.left_at
    ) THEN
        RAISE EXCEPTION 'workspace owner membership is immutable';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER workspace_owner_membership_immutable
    BEFORE UPDATE OR DELETE ON workspace_members
    FOR EACH ROW
    EXECUTE FUNCTION prevent_workspace_owner_membership_mutation();

-- Non-owner authority comes only from explicit custom roles. Owner authority
-- is derived from workspaces.owner_workspace_member_id, not from a magic role.
CREATE TABLE workspace_roles (
    role_id      uuidv7      PRIMARY KEY,
    workspace_id uuidv7      NOT NULL REFERENCES workspaces(workspace_id),
    name         text        NOT NULL CHECK (char_length(name) BETWEEN 1 AND 60),
    color        text        CHECK (color IS NULL OR color ~ '^#[0-9a-f]{6}$'),
    -- Ordering hint only: higher roles render first; duplicate positions use
    -- the stable name/role_id tie-breakers in application queries.
    position     integer     NOT NULL DEFAULT 0
        CHECK (position BETWEEN 0 AND 1000000),
    permissions  jsonb       NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(permissions) = 'object'),
    created_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, name),
    UNIQUE (workspace_id, role_id)
);

CREATE TABLE workspace_role_assignments (
    workspace_id        uuidv7      NOT NULL,
    role_id             uuidv7      NOT NULL,
    workspace_member_id uuidv7      NOT NULL,
    granted_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (role_id, workspace_member_id),
    FOREIGN KEY (workspace_id, role_id)
        REFERENCES workspace_roles (workspace_id, role_id) ON DELETE CASCADE,
    FOREIGN KEY (workspace_id, workspace_member_id)
        REFERENCES workspace_members (workspace_id, workspace_member_id) ON DELETE CASCADE
);

CREATE INDEX workspace_role_assignments_by_member
    ON workspace_role_assignments (workspace_member_id, role_id);

-- Invite codes are returned once and never stored in plaintext. Every invite
-- is single-use. Redemption keeps the consuming actor and membership so an
-- exact same-actor retry returns the original result without a second admit.
CREATE TABLE workspace_invites (
    invite_id                    uuidv7      PRIMARY KEY,
    workspace_id                 uuidv7      NOT NULL REFERENCES workspaces(workspace_id),
    created_by_workspace_member_id uuidv7    NOT NULL,
    code_hash                    bytea       NOT NULL UNIQUE CHECK (octet_length(code_hash) = 32),
    expires_at                   timestamptz NOT NULL,
    redeemed_by_kind             text CHECK (redeemed_by_kind IN ('human', 'personality_agent')),
    redeemed_by_id               uuidv7,
    redeemed_workspace_member_id uuidv7,
    redeemed_at                  timestamptz,
    revoked_at                   timestamptz,
    created_at                   timestamptz NOT NULL DEFAULT now(),
    CHECK (
        (redeemed_by_kind IS NULL AND redeemed_by_id IS NULL
         AND redeemed_workspace_member_id IS NULL AND redeemed_at IS NULL)
        OR
        (redeemed_by_kind IS NOT NULL AND redeemed_by_id IS NOT NULL
         AND redeemed_workspace_member_id IS NOT NULL AND redeemed_at IS NOT NULL)
    ),
    FOREIGN KEY (workspace_id, created_by_workspace_member_id)
        REFERENCES workspace_members (workspace_id, workspace_member_id),
    FOREIGN KEY (workspace_id, redeemed_workspace_member_id,
                 redeemed_by_kind, redeemed_by_id)
        REFERENCES workspace_members
            (workspace_id, workspace_member_id, member_kind, member_id)
);

CREATE INDEX workspace_invites_by_workspace
    ON workspace_invites (workspace_id, created_at DESC);

-- The server catalog is descriptor/availability truth; app_installations below
-- is lifecycle truth. Renderer-local metadata may select an icon/component but
-- cannot invent an app or its installed/enabled state.
CREATE TABLE app_catalog (
    app_id                    text        PRIMARY KEY
        CHECK (app_id ~ '^[a-z][a-z0-9-]{0,63}$'),
    display_name              text        NOT NULL CHECK (char_length(display_name) BETWEEN 1 AND 100),
    workspace_owner_allowed   boolean     NOT NULL,
    participant_owner_allowed boolean     NOT NULL,
    created_at                timestamptz NOT NULL DEFAULT now(),
    CHECK (workspace_owner_allowed OR participant_owner_allowed)
);

INSERT INTO app_catalog
    (app_id, display_name, workspace_owner_allowed, participant_owner_allowed)
VALUES
    ('messaging', 'Messaging', true, false),
    ('alarm', 'Alarm', false, true),
    ('direct-chat', 'Direct Chat', false, true),
    ('life-log', 'Life Log', false, true);

-- Apps own the vocabulary and labels for capabilities that may be attached to
-- Workspace roles. Workspace only stores and evaluates those catalog-backed
-- refs. A capability identity is immutable: retiring a ref and later adding a
-- new capability with the same spelling must not resurrect historical grants.
CREATE TABLE app_workspace_role_capabilities (
    capability_id  uuidv7      PRIMARY KEY,
    app_id          text        NOT NULL REFERENCES app_catalog(app_id),
    capability_ref text        NOT NULL
        CHECK (capability_ref ~ '^app\.[a-z][a-z0-9-]{0,63}\.[a-z][a-z0-9_]{0,63}$'),
    label           text        NOT NULL CHECK (char_length(label) BETWEEN 1 AND 100),
    created_at      timestamptz NOT NULL DEFAULT now(),
    retired_at      timestamptz,
    CHECK (capability_ref LIKE ('app.' || app_id || '.%')),
    CHECK (retired_at IS NULL OR retired_at >= created_at),
    UNIQUE (capability_id, capability_ref)
);

CREATE UNIQUE INDEX app_workspace_role_capabilities_active_ref
    ON app_workspace_role_capabilities (capability_ref)
    WHERE retired_at IS NULL;

CREATE OR REPLACE FUNCTION prevent_app_workspace_role_capability_identity_mutation()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.capability_id IS DISTINCT FROM OLD.capability_id
       OR NEW.app_id IS DISTINCT FROM OLD.app_id
       OR NEW.capability_ref IS DISTINCT FROM OLD.capability_ref THEN
        RAISE EXCEPTION 'app Workspace-role capability identity is immutable';
    END IF;
    IF OLD.retired_at IS NOT NULL
       AND NEW.retired_at IS DISTINCT FROM OLD.retired_at THEN
        RAISE EXCEPTION 'retired app Workspace-role capability cannot be reactivated or rewritten';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER app_workspace_role_capability_identity_immutable
    BEFORE UPDATE OF capability_id, app_id, capability_ref, retired_at
    ON app_workspace_role_capabilities
    FOR EACH ROW
    EXECUTE FUNCTION prevent_app_workspace_role_capability_identity_mutation();

INSERT INTO app_workspace_role_capabilities
    (capability_id, app_id, capability_ref, label)
VALUES
    ('0198f0f4-9b72-7000-8000-0000000008c1', 'messaging',
     'app.messaging.manage_channels', 'Manage channels');

-- Platform permissions remain in workspace_roles.permissions. App capability
-- grants are separate so a role can preserve the ref it was shown with while
-- effective authorization binds to the exact catalog identity that was
-- recognized when the grant was made.
CREATE TABLE workspace_role_app_capability_grants (
    workspace_id           uuidv7      NOT NULL,
    role_id                uuidv7      NOT NULL,
    capability_id          uuidv7      NOT NULL,
    capability_ref_snapshot text       NOT NULL
        CHECK (capability_ref_snapshot ~ '^app\.[a-z][a-z0-9-]{0,63}\.[a-z][a-z0-9_]{0,63}$'),
    granted_at             timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (role_id, capability_id),
    UNIQUE (role_id, capability_ref_snapshot),
    FOREIGN KEY (capability_id, capability_ref_snapshot)
        REFERENCES app_workspace_role_capabilities
            (capability_id, capability_ref),
    FOREIGN KEY (workspace_id, role_id)
        REFERENCES workspace_roles (workspace_id, role_id) ON DELETE CASCADE
);

CREATE INDEX workspace_role_app_capability_grants_by_role
    ON workspace_role_app_capability_grants (workspace_id, role_id);

-- Uninstall removes this binding only. App-owned data deliberately has no FK
-- to installation_id, so lifecycle changes cannot cascade into app data.
CREATE TABLE app_installations (
    installation_id uuidv7      PRIMARY KEY,
    -- Private storage encoding only: human/personality_agent are the two
    -- ParticipantRef variants inside the canonical Workspace|Participant
    -- owner sum; they are not application-level owner kinds.
    owner_kind       text        NOT NULL
        CHECK (owner_kind IN ('workspace', 'human', 'personality_agent')),
    owner_id         uuidv7      NOT NULL,
    app_id           text        NOT NULL REFERENCES app_catalog(app_id),
    enabled          boolean     NOT NULL DEFAULT true,
    installed_at     timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (owner_kind, owner_id, app_id)
);

CREATE INDEX app_installations_by_owner
    ON app_installations (owner_kind, owner_id, app_id);

-- installation_id is the mutation address. Moving that identity between
-- owners or apps would let an authorization decision made for the old binding
-- affect a different resource, so address fields are immutable.
CREATE OR REPLACE FUNCTION prevent_app_installation_address_mutation()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.owner_kind IS DISTINCT FROM OLD.owner_kind
       OR NEW.owner_id IS DISTINCT FROM OLD.owner_id
       OR NEW.app_id IS DISTINCT FROM OLD.app_id THEN
        RAISE EXCEPTION 'app installation address is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER app_installation_address_immutable
    BEFORE UPDATE OF owner_kind, owner_id, app_id ON app_installations
    FOR EACH ROW
    EXECUTE FUNCTION prevent_app_installation_address_mutation();

-- PostgreSQL has no polymorphic FK. Keep orphan prevention at the database
-- boundary with a fail-closed trigger while application authorization remains
-- the owner relation's domain rule.
CREATE OR REPLACE FUNCTION validate_app_installation_owner()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.owner_kind = 'workspace' AND NOT EXISTS (
        SELECT 1 FROM workspaces WHERE workspace_id = NEW.owner_id
    ) THEN
        RAISE EXCEPTION 'unknown workspace installation owner';
    ELSIF NEW.owner_kind = 'human' AND NOT EXISTS (
        SELECT 1 FROM humans WHERE human_id = NEW.owner_id
    ) THEN
        RAISE EXCEPTION 'unknown Human installation owner';
    ELSIF NEW.owner_kind = 'personality_agent' AND NOT EXISTS (
        SELECT 1 FROM agents WHERE personality_agent_id = NEW.owner_id
    ) THEN
        RAISE EXCEPTION 'unknown PersonalityAgent installation owner';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER app_installation_owner_exists
    BEFORE INSERT OR UPDATE OF owner_kind, owner_id ON app_installations
    FOR EACH ROW
    EXECUTE FUNCTION validate_app_installation_owner();
