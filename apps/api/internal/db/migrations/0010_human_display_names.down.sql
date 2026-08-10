ALTER TABLE auth_flows DROP COLUMN verified_display_name;
ALTER TABLE humans DROP CONSTRAINT humans_display_name_length;
ALTER TABLE humans DROP COLUMN display_name_initialized;
ALTER TABLE humans DROP COLUMN display_name_customized;
