-- A pending receipt completes the originating tool-call protocol, not the effect.
ALTER TABLE approval_log ADD COLUMN receipt_message_id TEXT REFERENCES messages(id);
CREATE UNIQUE INDEX approval_receipt_message ON approval_log(receipt_message_id)
WHERE receipt_message_id IS NOT NULL;
