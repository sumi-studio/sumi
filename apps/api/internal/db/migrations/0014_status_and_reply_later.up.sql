-- 0012_status_and_reply_later: self-declared attention state
-- (docs/messaging-contracts-draft.md「Status と ReplyLater — 自己申告の
-- attention」). No read receipts, no automatic presence: everything in these
-- tables is something the participant said about themselves, so both carry the
-- shared participant (kind, id) shape — Humans and PersonalityAgents press the
-- same button.

-- 1. participant_statuses — one current status per participant. Setting a new
--    status replaces the row; expiry is enforced at read time (an expired row
--    is simply not reported), so no background job is needed.
CREATE TABLE participant_statuses (
    member_kind text        NOT NULL
        CHECK (member_kind IN ('human', 'personality_agent')),
    member_id   uuidv7      NOT NULL,
    status      text        NOT NULL CHECK (status IN ('available', 'busy', 'away')),
    note        text        NOT NULL DEFAULT '' CHECK (length(note) <= 200),
    -- NULL means the status holds until replaced.
    expires_at  timestamptz,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (member_kind, member_id)
);

-- 2. reply_later_markers — the durable「後で返信します」marker (合意事項 6).
--    The marker (fact + note) is visible to everyone who can see the message;
--    remind_at belongs to the owner's private reminder schedule and is never
--    put on another participant's wire — that secrecy lives at the transport
--    layer, the row stores the whole truth. Resolution keeps the row (the
--    promise was kept) instead of deleting it.
CREATE TABLE reply_later_markers (
    marker_id   uuidv7      PRIMARY KEY,
    member_kind text        NOT NULL
        CHECK (member_kind IN ('human', 'personality_agent')),
    member_id   uuidv7      NOT NULL,
    place_id    uuidv7      NOT NULL REFERENCES places(place_id),
    message_id  uuidv7      NOT NULL REFERENCES messages(message_id) ON DELETE CASCADE,
    note        text        NOT NULL DEFAULT '' CHECK (length(note) <= 500),
    remind_at   timestamptz NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz,
    CHECK (resolved_at IS NULL OR resolved_at >= created_at)
);

-- One open promise per participant per message: repeating the tap is
-- idempotent by database guarantee, and a resolved marker frees the slot.
CREATE UNIQUE INDEX reply_later_one_active_per_message
    ON reply_later_markers (message_id, member_kind, member_id)
    WHERE resolved_at IS NULL;

-- Bootstrap lists the open markers of every visible place.
CREATE INDEX reply_later_markers_active_by_place
    ON reply_later_markers (place_id)
    WHERE resolved_at IS NULL;
