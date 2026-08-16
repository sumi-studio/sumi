-- Voice channels remain ordinary channels so text, unread state, mentions,
-- and notification settings continue to use one place (ADR 0012).
ALTER TABLE places ADD COLUMN voice boolean NOT NULL DEFAULT false;

-- DMs can host an ad-hoc call, but only channels are persistent voice places.
ALTER TABLE places ADD CONSTRAINT places_voice_is_channel_only
    CHECK (NOT voice OR kind = 'channel');
