-- 0024_message_search rollback.
DROP INDEX messages_content_trgm;
-- pg_trgm is database-wide state and may be used independently by later
-- migrations, so rollback only removes this migration's index.
