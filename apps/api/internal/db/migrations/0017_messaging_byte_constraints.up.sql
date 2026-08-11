-- 0017_messaging_byte_constraints: align the durable schema with Messaging's
-- byte-oriented wire and Store limits. PostgreSQL length(text) counts
-- characters, so multibyte content and idempotency keys could previously
-- exceed the application limits when inserted outside the Store.
ALTER TABLE messages
    DROP CONSTRAINT messages_content_check,
    DROP CONSTRAINT messages_client_nonce_check,
    ADD CONSTRAINT messages_content_check
        CHECK (octet_length(content) <= 65536),
    ADD CONSTRAINT messages_client_nonce_check
        CHECK (octet_length(client_nonce) BETWEEN 1 AND 128);
