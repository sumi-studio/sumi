-- 0026_message_search_concurrent_index rollback: 0026 only rebuilt the index that
-- 0024 owns, so rolling it back must leave that index in place (built concurrently
-- here as well) — otherwise 0024's own rollback (plain DROP INDEX) fails and message
-- search silently disappears.
-- +no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS messages_content_trgm
    ON messages USING GIN (content gin_trgm_ops)
    WHERE deleted_at IS NULL;
