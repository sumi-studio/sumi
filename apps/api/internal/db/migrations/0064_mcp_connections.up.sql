-- Remote MCP grants are human-owned, write-only credentials. A version pins
-- queued work to the exact connection/grant under which it was admitted.
CREATE TABLE mcp_connections (
 human_id uuidv7 NOT NULL REFERENCES humans(human_id),
 connection_id uuid NOT NULL,
 name text NOT NULL,
 endpoint text NOT NULL,
 enabled boolean NOT NULL DEFAULT false,
 credential_ciphertext bytea NOT NULL,
 version uuid NOT NULL,
 PRIMARY KEY(human_id, connection_id)
);
