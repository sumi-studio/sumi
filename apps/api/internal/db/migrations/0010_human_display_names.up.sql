-- 0010_human_display_names: distinguish a provider-seeded initial label from
-- a name the Human explicitly chose in Sumi, and retain a verified initial
-- name while an explicit auth flow waits for Human confirmation.

ALTER TABLE humans
    ADD COLUMN display_name_customized boolean NOT NULL DEFAULT false;

-- Existing rows with the historical Sumi sentinel have not consumed a real
-- initial label. Every Human created after this migration is initialized at
-- creation, even when Firebase offers no usable name.
ALTER TABLE humans ADD COLUMN display_name_initialized boolean;
UPDATE humans SET display_name_initialized = (display_name <> 'Sumi');
ALTER TABLE humans ALTER COLUMN display_name_initialized SET NOT NULL;
ALTER TABLE humans ALTER COLUMN display_name_initialized SET DEFAULT true;

ALTER TABLE humans
    ADD CONSTRAINT humans_display_name_length
    CHECK (char_length(display_name) BETWEEN 1 AND 80);

ALTER TABLE auth_flows
    ADD COLUMN verified_display_name text
        CHECK (verified_display_name IS NULL OR
               char_length(verified_display_name) BETWEEN 1 AND 80);
