-- 0021_finalize_human_display_name_initialization: the historical Sumi
-- sentinel is no longer upgraded at sign-in. Preserve the stored label and
-- finalize all remaining rows so provider metadata cannot mutate it later.
UPDATE humans
SET display_name_initialized = true
WHERE NOT display_name_initialized;
