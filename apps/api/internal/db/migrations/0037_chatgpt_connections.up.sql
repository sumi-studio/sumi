-- ChatGPT inference subscriptions belong to a Human, independently of Sumi login.
CREATE TABLE chatgpt_connections (
    human_id uuidv7 PRIMARY KEY REFERENCES humans(human_id),
    connection_id uuid NOT NULL,
    account_id text NOT NULL,
    credential_ciphertext bytea NOT NULL,
    expires_at timestamptz NOT NULL,
    model text NOT NULL,
    effort text NOT NULL,
    reconnect_required boolean NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT now()
);
