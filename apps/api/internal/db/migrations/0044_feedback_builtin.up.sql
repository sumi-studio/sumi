-- Feedback is a built-in Sumi service, not an installable participant app.
-- Conversation, attachment and per-participant access records remain untouched.
DELETE FROM app_install_operation_receipts WHERE app_id='feedback';
DELETE FROM app_installations WHERE app_id='feedback';
DELETE FROM app_catalog WHERE app_id='feedback';
