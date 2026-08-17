-- 0024_message_search rollback.
-- +no-transaction
DROP INDEX CONCURRENTLY IF EXISTS messages_content_trgm;
-- pg_trgm is database-wide state and may be used independently by later
-- migrations, so rollback only removes this migration's index.
