CREATE OR REPLACE FUNCTION require_attachment_for_empty_message()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.deleted_at IS NULL AND NEW.content = '' AND NOT EXISTS (
        SELECT 1 FROM message_attachments a
        WHERE a.workspace_id = NEW.workspace_id
          AND a.place_id = NEW.place_id
          AND a.message_id = NEW.message_id
    ) THEN
        RAISE EXCEPTION 'a message with empty content must bind at least one attachment';
    END IF;
    RETURN NULL;
END;
$$;

DROP TABLE message_poll_votes;
DROP TABLE message_poll_options;
DROP TABLE message_polls;
