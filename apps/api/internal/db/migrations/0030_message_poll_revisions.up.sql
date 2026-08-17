-- poll snapshots are published after a vote transaction commits. A revision
-- lets receivers reject a delayed snapshot from an earlier commit.
ALTER TABLE message_polls
    ADD COLUMN revision bigint NOT NULL DEFAULT 0;
