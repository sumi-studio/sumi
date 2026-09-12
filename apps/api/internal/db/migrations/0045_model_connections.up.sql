CREATE TABLE model_api_connections (
 human_id uuidv7 NOT NULL REFERENCES humans(human_id),
 connection_id uuid NOT NULL,
 name text NOT NULL,
 preset text NOT NULL,
 base_url text NOT NULL,
 model text NOT NULL,
 credential_ciphertext bytea NOT NULL,
 version uuid NOT NULL,
 PRIMARY KEY(human_id, connection_id)
);
CREATE TABLE model_connection_selections (
 human_id uuidv7 PRIMARY KEY REFERENCES humans(human_id),
 kind text NOT NULL CHECK(kind IN ('api','chatgpt','none')),
 connection_id uuid,
 CHECK ((kind='api') = (connection_id IS NOT NULL)),
 FOREIGN KEY(human_id, connection_id) REFERENCES model_api_connections(human_id,connection_id)
);
