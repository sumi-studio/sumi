-- Account-free Local grants belong to one install and one continuing persona.
-- Host-bound secrets/configuration are not included in portable Core state.
CREATE TABLE local_mcp_connections (
 host_id uuid NOT NULL,
 persona_id uuidv7 NOT NULL REFERENCES core_personas(persona_id),
 connection_id uuid NOT NULL,
 name text NOT NULL,
 transport text NOT NULL CHECK (transport IN ('https','stdio')),
 endpoint text NOT NULL DEFAULT '',
 enabled boolean NOT NULL DEFAULT false,
 configuration_ciphertext bytea NOT NULL,
 version uuid NOT NULL,
 PRIMARY KEY(host_id, persona_id, connection_id)
);
