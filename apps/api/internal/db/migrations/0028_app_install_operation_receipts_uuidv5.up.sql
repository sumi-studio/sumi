-- Install receipts accept random client UUIDv4 intents and declared UUIDv5
-- identities derived by trusted server provisioning. Both remain canonical
-- lowercase RFC 4122 UUIDs; UUIDv5 is never accepted at client boundaries.
ALTER TABLE app_install_operation_receipts
    DROP CONSTRAINT app_install_operation_receipts_uuidv4;

ALTER TABLE app_install_operation_receipts
    ADD CONSTRAINT app_install_operation_receipts_uuidv4_or_uuidv5
    CHECK (
        operation_id::text ~
        '^[0-9a-f]{8}-[0-9a-f]{4}-[45][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
    );
