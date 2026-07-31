-- 0001_baseline: migration framework baseline.
-- The 戸籍 schema lands in subsequent migrations (issue #119). This migration
-- exists so the runner proves it can apply statements to an empty database and
-- record the version; the schema_migrations bookkeeping table is owned by the
-- runner itself.
SELECT 1;
