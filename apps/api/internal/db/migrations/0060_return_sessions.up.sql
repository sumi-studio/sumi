-- 0060_return_sessions: a signed-in owner's request to bring their Cloud
-- secretary back to a Sumi Local placement (internal/returnsession). The
-- session is bookkeeping around the portable transfer ledger, never a second
-- copy of it: persona id, receipts and proofs stay in core_transfers under
-- transfer_id = session_id::text.
--
-- human_id + persona_id name the owner and the secretary the session moves;
-- both were proved by the owner's live browser session at create. grant_hash
-- is SHA-256 of the scoped grant handed to the Local command; the grant
-- itself is never stored.
--
-- destination_* is the one Local placement this session serves, recorded by
-- the Local command before this placement seals — a return URL pasted into a
-- second Local placement is refused there while this secretary stays active
-- here, instead of sealing a secretary whose bundle would land on a receiver
-- that was never checked. destination_slot_state is what that receiver
-- declared about its persona slot ('absent' or 'surrendered'); it is
-- evidence, not authority — the destination's own import admission is the
-- enforcement, this record is what makes "another Local took the URL" a
-- decidable refusal before the seal.
--
-- prior_transfer_id is the persona's transfer hold just before this
-- session's seal — the forward move that brought the secretary here, when
-- one exists. The destination's surrendered copy is held by the same
-- transfer id, so the receiver can cross-check the reclaim lineage it
-- asserts (portable import's supersedes) against what the source recorded.
--   awaiting_destination  admission open until admit_until; the seal has not
--                         happened, so cancelling here moves no authority
--   sealed                the export cut is held; the bundle is downloadable
--   cancelling            a cancel was requested after sealing; the source
--                         stays sealed until the destination's retire_proof
--                         arrives (an activate_proof can still win — a
--                         committed activation is never un-done by cancel)
--   completed             the source completed: this placement is
--                         transferred, the destination is authoritative
--   aborted               the destination retired; the source is active again
--   cancelled/expired     terminal before any seal; nothing moved
CREATE TABLE return_sessions (
    session_id     uuidv7      PRIMARY KEY,
    human_id       uuidv7      NOT NULL REFERENCES humans(human_id),
    persona_id     uuidv7      NOT NULL,
    grant_hash     bytea       NOT NULL UNIQUE CHECK (octet_length(grant_hash) = 32),
    status         text        NOT NULL
        CHECK (status IN ('awaiting_destination','sealed','cancelling','completed','aborted','cancelled','expired')),
    destination_placement_id uuidv7,
    destination_persona_id   uuidv7,
    destination_slot_state   text CHECK (destination_slot_state IN ('absent','surrendered')),
    destination_bound_at     timestamptz,
    prior_transfer_id        text,
    admit_until    timestamptz NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    -- The four destination columns are written once, together.
    CHECK ((destination_placement_id IS NULL) = (destination_persona_id IS NULL)),
    CHECK ((destination_placement_id IS NULL) = (destination_slot_state IS NULL)),
    CHECK ((destination_placement_id IS NULL) = (destination_bound_at IS NULL)),
    -- Every status past the seal carries a destination binding. The
    -- binding itself lands while the session is still
    -- awaiting_destination — the seal is a separate step, so the
    -- implication runs one way only.
    CHECK (status NOT IN ('sealed','cancelling','completed','aborted')
           OR destination_placement_id IS NOT NULL)
);

-- One open return per secretary: the sealed source a fresh proof recovers is
-- unambiguous.
CREATE UNIQUE INDEX return_sessions_open_persona
    ON return_sessions (persona_id)
    WHERE status IN ('awaiting_destination','sealed','cancelling');
