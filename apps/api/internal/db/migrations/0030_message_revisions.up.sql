-- PATCH編集を無通知で上書きしないための compare-and-swap 版。
ALTER TABLE messages
    ADD COLUMN revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0);
