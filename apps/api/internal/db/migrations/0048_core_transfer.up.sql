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

-- One row per transfer side. No foreign key to core_personas: discarding a
-- staged import deletes the persona, and the ledger still records that it
-- happened. receipt is the verified summary (row counts, cut, what continues,
-- what the bundle does not carry) returned to the caller on every replay.
CREATE TABLE core_transfers (
    direction      text        NOT NULL CHECK (direction IN ('export','import')),
    transfer_id    text        NOT NULL,
    persona_id     uuidv7      NOT NULL,
    status         text        NOT NULL
        CHECK (status IN ('sealed','completed','aborted','staged','activated','discarded')),
    format_version int         NOT NULL,
    content_sha256 text,
    receipt        jsonb       NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (direction, transfer_id)
);
CREATE INDEX core_transfers_by_persona ON core_transfers(persona_id, created_at);
