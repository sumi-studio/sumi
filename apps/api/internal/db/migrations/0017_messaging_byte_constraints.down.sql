-- 0017_messaging_byte_constraints rollback: restore the character-oriented
-- constraints created by 0008.
ALTER TABLE messages
    DROP CONSTRAINT messages_content_check,
    DROP CONSTRAINT messages_client_nonce_check,
    ADD CONSTRAINT messages_content_check
        CHECK (length(content) <= 65536),
    ADD CONSTRAINT messages_client_nonce_check
        CHECK (length(client_nonce) BETWEEN 1 AND 128);
