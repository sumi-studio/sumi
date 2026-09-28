-- ChatGPT subscription connections: a model connection whose credential is a
-- ChatGPT OAuth grant (access + refresh token) instead of an API key. The
-- sealed tokens live in credential_ciphertext like an API key; the columns
-- below are non-secret metadata the refresh path and settings UI need.
ALTER TABLE public.model_api_connections
    ADD COLUMN account_id text,
    ADD COLUMN access_expires_at timestamp with time zone,
    ADD COLUMN reconnect_required boolean DEFAULT false NOT NULL,
    ADD COLUMN reasoning_effort text,
    ADD CONSTRAINT model_api_connections_chatgpt_account_check
        CHECK (((preset = 'chatgpt-codex'::text) = (account_id IS NOT NULL))),
    ADD CONSTRAINT model_api_connections_reasoning_effort_check
        CHECK (((reasoning_effort IS NULL) OR (reasoning_effort = ANY (ARRAY['low'::text, 'medium'::text, 'high'::text, 'xhigh'::text, 'max'::text]))));

-- A pending device-code login. It is durable so any API process can serve
-- the browser's status polls and a restart does not lose it; the poll to
-- the issuer happens on those reads, not in a background worker.
CREATE TABLE public.model_chatgpt_logins (
    login_id uuid NOT NULL,
    human_id public.uuidv7 NOT NULL,
    session_id text NOT NULL,
    -- Reconnect target; NULL creates a new connection.
    connection_id uuid,
    -- Sealed issuer device_auth_id (the polling credential).
    device_ciphertext bytea NOT NULL,
    user_code text NOT NULL,
    verification_url text NOT NULL,
    interval_seconds integer NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    next_poll_at timestamp with time zone NOT NULL,
    status text NOT NULL,
    error_code text,
    result_connection_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT model_chatgpt_logins_pkey PRIMARY KEY (login_id),
    CONSTRAINT model_chatgpt_logins_human_id_fkey FOREIGN KEY (human_id) REFERENCES public.humans(human_id) ON DELETE CASCADE,
    CONSTRAINT model_chatgpt_logins_interval_check CHECK (((interval_seconds >= 1) AND (interval_seconds <= 900))),
    CONSTRAINT model_chatgpt_logins_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'completed'::text, 'failed'::text, 'expired'::text, 'cancelled'::text])))
);

CREATE INDEX model_chatgpt_logins_human_idx ON public.model_chatgpt_logins USING btree (human_id, created_at);
