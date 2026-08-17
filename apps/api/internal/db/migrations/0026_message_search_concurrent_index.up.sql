-- 0026_message_search_concurrent_index: 0024 created messages_content_trgm
-- with a blocking CREATE INDEX. Rebuild it concurrently so a populated messages
-- table keeps accepting writes; drop first so retrying a failed INVALID build heals it.
-- +no-transaction
DROP INDEX CONCURRENTLY IF EXISTS messages_content_trgm;
CREATE INDEX CONCURRENTLY messages_content_trgm
    ON messages USING GIN (content gin_trgm_ops)
    WHERE deleted_at IS NULL;
