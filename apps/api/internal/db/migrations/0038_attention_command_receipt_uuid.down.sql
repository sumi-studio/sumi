-- Downgrade is intentionally rejected by the domain when UUIDv4 receipts
-- exist; never delete admitted experiences to make an older schema fit.
ALTER TABLE agent_attention_deliveries
    ALTER COLUMN admitted_command_id TYPE uuidv7 USING admitted_command_id::uuidv7;
