-- Refuse downgrade while poll-answer history exists rather than discard it.
ALTER TABLE agent_attention_deliveries
    DROP CONSTRAINT agent_attention_deliveries_source_kind_check;
ALTER TABLE agent_attention_deliveries
    ADD CONSTRAINT agent_attention_deliveries_source_kind_check
    CHECK (source_kind IN ('messaging_message', 'messaging_mention', 'reply_later_due'));
