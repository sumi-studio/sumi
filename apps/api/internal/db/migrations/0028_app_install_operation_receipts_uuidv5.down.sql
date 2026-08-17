ALTER TABLE app_install_operation_receipts
    DROP CONSTRAINT app_install_operation_receipts_uuidv4_or_uuidv5;

ALTER TABLE app_install_operation_receipts
    ADD CONSTRAINT app_install_operation_receipts_uuidv4
    CHECK (
        operation_id::text ~
        '^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
    );
