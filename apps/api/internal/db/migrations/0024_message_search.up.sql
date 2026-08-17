-- 0024_message_search: substring search over live message content.
--
-- Japanese text has no lexeme boundaries for the built-in FTS dictionaries,
-- so search uses case-insensitive ILIKE substring matching ranked by pg_trgm
-- similarity. The trigram index helps queries of three or more characters;
-- shorter queries still have exact semantics over the caller's visible places.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX messages_content_trgm
    ON messages USING GIN (content gin_trgm_ops)
    WHERE deleted_at IS NULL;
