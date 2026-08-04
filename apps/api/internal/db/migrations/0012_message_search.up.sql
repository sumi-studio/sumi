-- 0012_message_search: substring search over live message content (issue #198).
-- Japanese text has no lexeme boundaries for the built-in FTS dictionaries, so
-- search is case-insensitive ILIKE substring matching ranked by pg_trgm
-- similarity. The trigram GIN index accelerates ILIKE for queries of three or
-- more characters; shorter queries scan only the viewer-visible places, which
-- is acceptable at the current scale. Tombstones (deleted_at IS NOT NULL) hold
-- NULL content and are excluded by the partial index and by the query.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX messages_content_trgm
    ON messages USING GIN (content gin_trgm_ops)
    WHERE deleted_at IS NULL;
