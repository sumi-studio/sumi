-- Portable secretary state (engineering plan §4.2/§5, M19): which placement
-- is authoritative for a persona, and the ledger of transfers that moved it.
--
-- authority is checked by the state service inside the same transactions
-- that check the writer generation, so it is a database fence rather than a
-- display flag:
--   active       this placement may acquire a writer and accept new inputs
--   sealed       an export cut is held here; no writer, no new inputs
--   staged       imported here but not activated; no writer, no new inputs
--   transferred  another placement is authoritative; this copy is history
-- transfer_id names the transfer that last changed authority.
ALTER TABLE core_personas
    ADD COLUMN authority text NOT NULL DEFAULT 'active'
        CHECK (authority IN ('active','sealed','staged','transferred')),
    ADD COLUMN transfer_id text;

-- One row per placement, minted on first use. A bundle is addressed to one
-- placement id, so an ordinary retry cannot stage the same transfer on two
-- placements; retargeting means sealing a new transfer.
CREATE TABLE core_placement (
    singleton    boolean     PRIMARY KEY DEFAULT true CHECK (singleton),
    placement_id uuidv7      NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- One row per transfer side. No foreign key to core_personas: retiring a
-- staged import deletes the persona, and the ledger still records that it
-- happened. receipt is the verified summary (row counts, cut, what continues,
-- what the bundle does not carry, and destination-produced proofs) returned
-- to the caller on every replay. proof_key is the transfer's HMAC key —
-- present on every ledger row that has seen the bundle — and is never copied
-- into a receipt: the source uses it to verify the destination's
-- activate/retire proofs, so ending a placement's authority requires evidence
-- only the destination could have produced by committing that step.
CREATE TABLE core_transfers (
    direction      text        NOT NULL CHECK (direction IN ('export','import')),
    transfer_id    text        NOT NULL,
    persona_id     uuidv7      NOT NULL,
    status         text        NOT NULL
        CHECK (status IN ('sealed','completed','aborted','staged','activated','retired')),
    format_version int         NOT NULL,
    destination_id uuidv7,
    content_sha256 text,
    proof_key      text,
    receipt        jsonb       NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (direction, transfer_id)
);
CREATE INDEX core_transfers_by_persona ON core_transfers(persona_id, created_at);
