-- 0059_transfer_sessions: a Cloud registration that brings an existing Local
-- secretary (internal/transfersession). The session is bookkeeping around
-- the portable transfer ledger, never a second copy of it: persona id,
-- receipts and destination proofs stay in core_transfers under
-- transfer_id = session_id::text.
--
-- claim_provider/claim_subject is the typed credential identity a trusted
-- authentication flow proved when the session was created; only a live flow
-- proving the same credential may claim the staged secretary.
-- grant_hash is SHA-256 of the scoped grant handed to the Local source; the
-- grant itself is never stored.
--
-- source_placement_id/source_persona_id is the one Local source this session
-- accepts a bundle from. The Local command records it before it seals, so a
-- move URL pasted into a second Local placement is refused while that
-- secretary is still active there instead of sealing a secretary whose bundle
-- could never be admitted. The binding is also the admission gate: an upload
-- whose bundle header names another persona is refused before it is read.
--   awaiting_bundle  admission open until admit_until
--   staged           the import committed; claimable until claim_until
--   provisioned      the account transaction bound the persona to human_id;
--                    activation is owed (this status is the obligation)
--   activated        the destination is authoritative
--   cancelled/expired  terminal; a staged import is retired afterwards
CREATE TABLE transfer_sessions (
    session_id     uuidv7      PRIMARY KEY,
    claim_provider text        NOT NULL CHECK (claim_provider IN ('firebase')),
    claim_subject  text        NOT NULL CHECK (char_length(claim_subject) BETWEEN 1 AND 128),
    grant_hash     bytea       NOT NULL UNIQUE CHECK (octet_length(grant_hash) = 32),
    status         text        NOT NULL
        CHECK (status IN ('awaiting_bundle','staged','provisioned','activated','cancelled','expired')),
    human_id       uuidv7      REFERENCES humans(human_id),
    source_placement_id uuidv7,
    source_persona_id   uuidv7,
    source_bound_at     timestamptz,
    admit_until    timestamptz NOT NULL,
    claim_until    timestamptz,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    CHECK ((status IN ('provisioned','activated')) = (human_id IS NOT NULL)),
    CHECK (status <> 'staged' OR claim_until IS NOT NULL),
    -- The three source columns are written once, together.
    CHECK ((source_placement_id IS NULL) = (source_persona_id IS NULL)),
    CHECK ((source_placement_id IS NULL) = (source_bound_at IS NULL))
);

-- One open registration transfer per credential: the staged receipt a fresh
-- proof recovers is unambiguous.
CREATE UNIQUE INDEX transfer_sessions_open_subject
    ON transfer_sessions (claim_provider, claim_subject)
    WHERE status IN ('awaiting_bundle','staged','provisioned');
