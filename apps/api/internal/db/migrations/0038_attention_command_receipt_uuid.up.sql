-- CommandStore owns command identities and generates UUIDv4, independently of
-- the UUIDv7 Messaging source/event identity. Preserve every existing receipt.
ALTER TABLE agent_attention_deliveries
    ALTER COLUMN admitted_command_id TYPE uuid USING admitted_command_id::uuid;
