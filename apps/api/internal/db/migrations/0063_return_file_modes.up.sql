-- 0063_return_file_modes: selectable file handling for Cloud-to-Local
-- returns.
--
-- file_mode records the owner's explicit choice for where the returning
-- secretary's working file store lives after the move:
--   'local'  the Cloud workspace is copied into the Local file store
--            before activation; the Cloud copy is retained read-only and
--            is never deleted automatically.
--   'cloud'  the Cloud store stays the working store; the destination's
--            file tools and person-facing file surface keep reading and
--            writing it through a scoped storage credential.
-- NULL is a session created before the choice existed: it moves records
-- only and serves no file routes. There is no records-only product path
-- for new sessions — Create requires an explicit mode.
ALTER TABLE return_sessions
    ADD COLUMN file_mode text
    CHECK (file_mode IN ('local','cloud'));

-- file_epoch is the ONE durable generation order for file-store
-- authority: the filesvc mutation barrier (file_freeze.owner_epoch),
-- the storage credential's lineage and every "is a newer bound return
-- in charge" check all compare this same value. A sequence — not
-- created_at — is the source so ordering never depends on timestamp
-- precision or clock skew between minting and comparison sites.
-- Minted at session create; create order is the only possible lineage
-- order because a persona can hold at most one open return session.
CREATE SEQUENCE return_file_epoch_seq;
ALTER TABLE return_sessions
    ADD COLUMN file_epoch bigint NOT NULL
        DEFAULT nextval('return_file_epoch_seq');

-- persona_file_tokens: durable scoped storage credentials minted for a
-- return destination under 'cloud' file mode. A token authorizes file
-- operations on exactly the persona's derived filesvc scope — nothing
-- else: it revives no secretary authority, authorizes no core state, and
-- never carries or exposes the API's internal wildcard credential.
--
--   token_hash                 SHA-256 of the minted token; the token
--                              itself is returned exactly once and never
--                              stored.
--   session_id                 the return session whose completed
--                              authority transfer the token was issued
--                              under — the credential's lineage, used so
--                              a cancelled or superseded session can
--                              only retire its own tokens.
--   destination_placement_id   the install the token was minted for.
--                              Minting supersedes earlier active tokens
--                              for the same persona+destination; a stale
--                              session for a different destination can
--                              never rotate a later destination's
--                              credential.
--   status                     active → superseded (a newer token for
--                              the same destination answered a re-mint)
--                              or revoked (session cancelled/aborted,
--                              owner revocation, or the working store
--                              moved to Local). resolved_at set iff
--                              status is not active.
CREATE TABLE persona_file_tokens (
    token_hash               bytea       PRIMARY KEY
        CHECK (octet_length(token_hash) = 32),
    persona_id               uuidv7      NOT NULL,
    session_id               uuidv7      NOT NULL
        REFERENCES return_sessions(session_id),
    destination_placement_id uuidv7      NOT NULL,
    file_epoch               bigint      NOT NULL DEFAULT 0,
    scope                    text        NOT NULL,
    status                   text        NOT NULL
        CHECK (status IN ('active','superseded','revoked')),
    issued_at                timestamptz NOT NULL DEFAULT now(),
    resolved_at              timestamptz,
    CHECK ((status = 'active') = (resolved_at IS NULL))
);

-- Token lookup is by primary key; the active index serves "which tokens
-- are live for this persona" (revocation, working-store checks) without
-- scanning history.
CREATE INDEX persona_file_tokens_active
    ON persona_file_tokens (persona_id, destination_placement_id)
    WHERE status = 'active';
