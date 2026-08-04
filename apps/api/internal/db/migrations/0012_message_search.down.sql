-- 0012_message_search rollback.
DROP INDEX messages_content_trgm;
-- pg_trgm is left installed: extensions are shared database state and later
-- objects may depend on it independently of this migration.
