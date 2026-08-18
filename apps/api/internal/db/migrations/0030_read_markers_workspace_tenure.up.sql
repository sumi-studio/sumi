-- 0030_read_markers_workspace_tenure: thread viewers need a cursor without
-- becoming thread participants. The Workspace membership tenure scopes that
-- cursor so leaving and rejoining cannot revive it.
DROP TABLE read_markers;

CREATE TABLE read_markers (
    place_id            uuidv7      NOT NULL REFERENCES places (place_id),
    workspace_member_id uuidv7      NOT NULL REFERENCES workspace_members (workspace_member_id),
    last_read_seq       bigint      NOT NULL
        CHECK (last_read_seq >= 0 AND last_read_seq <= 9007199254740991),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (place_id, workspace_member_id)
);
