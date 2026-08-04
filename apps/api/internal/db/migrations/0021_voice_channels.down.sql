ALTER TABLE places DROP CONSTRAINT IF EXISTS places_voice_is_channel_only;
ALTER TABLE places DROP COLUMN IF EXISTS voice;
