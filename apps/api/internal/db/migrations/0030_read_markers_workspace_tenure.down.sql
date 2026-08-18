DROP TABLE read_markers;

CREATE TABLE read_markers (
    place_id        uuidv7      NOT NULL,
    place_member_id uuidv7      NOT NULL,
    last_read_seq   bigint      NOT NULL
        CHECK (last_read_seq >= 0 AND last_read_seq <= 9007199254740991),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (place_id, place_member_id),
    FOREIGN KEY (place_id, place_member_id)
        REFERENCES place_members (place_id, place_member_id)
);
