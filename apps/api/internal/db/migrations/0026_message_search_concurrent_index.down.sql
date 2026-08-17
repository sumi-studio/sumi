-- 0026_message_search_concurrent_index rollback.
-- +no-transaction
DROP INDEX CONCURRENTLY IF EXISTS messages_content_trgm;
