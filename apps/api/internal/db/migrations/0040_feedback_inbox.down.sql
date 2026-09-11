DROP TABLE feedback_attention_outbox;
DROP TABLE feedback_requests, feedback_reads, feedback_events, feedback_threads;
DELETE FROM app_install_operation_receipts WHERE app_id = 'feedback';
DELETE FROM app_installations WHERE app_id = 'feedback';
DELETE FROM app_catalog WHERE app_id = 'feedback';
